from __future__ import annotations

import math
from collections import defaultdict, deque
from datetime import date, datetime, timedelta, timezone
from statistics import median
from typing import Any, Iterable
from uuid import UUID
from zoneinfo import ZoneInfo

from psycopg.types.json import Jsonb

from app.config import get_settings
from app.db import db_connection, fetch_all, fetch_one


NY = ZoneInfo("America/New_York")
RULE_VERSION = "acquisition-v1"


def _f(value: Any, default: float = 0.0) -> float:
    try:
        return float(value) if value is not None else default
    except (TypeError, ValueError):
        return default


def _rolling_return(rows: list[dict[str, Any]], index: int, minutes: int) -> float | None:
    if index <= 0:
        return None
    target = rows[index]["ts"] - timedelta(minutes=minutes)
    prior = None
    for candidate in reversed(rows[:index]):
        if candidate["ts"] <= target:
            prior = candidate
            break
    if not prior:
        return None
    base = _f(prior.get("close"))
    current = _f(rows[index].get("close"))
    return current / base - 1.0 if base and current else None


def _relative_volume(rows: list[dict[str, Any]], index: int, window: int = 5) -> float | None:
    start = max(0, index - window + 1)
    recent = sum(_f(row.get("volume")) for row in rows[start : index + 1])
    history = [_f(row.get("volume")) for row in rows[max(0, start - 30) : start] if _f(row.get("volume")) > 0]
    if len(history) < 5:
        return None
    expected = median(history) * max(1, index - start + 1)
    return recent / expected if expected else None


def _dollar_volume(rows: list[dict[str, Any]], index: int, window: int = 5) -> float:
    start = max(0, index - window + 1)
    return sum(_f(row.get("close")) * _f(row.get("volume")) for row in rows[start : index + 1])


def detect_equity_capture_windows(
    rows: list[dict[str, Any]],
    *,
    run_start: datetime,
    run_end: datetime,
    move_pct: float,
    move_5m_pct: float,
    relative_volume_threshold: float,
    min_price: float,
    min_dollar_volume: float,
    cooldown_minutes: int,
    before_minutes: int,
    after_minutes: int,
) -> list[dict[str, Any]]:
    """Return neutral acquisition windows for unusual equity activity.

    This intentionally does not calculate future outcomes or label winners.
    """
    sessions: dict[date, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        local = row["ts"].astimezone(NY)
        if local.weekday() >= 5:
            continue
        minute = local.hour * 60 + local.minute
        if 570 <= minute < 960:
            sessions[local.date()].append(row)

    events: list[dict[str, Any]] = []
    cooldown = timedelta(minutes=cooldown_minutes)
    for session_date, session in sorted(sessions.items()):
        session.sort(key=lambda item: item["ts"])
        if not session:
            continue
        open_price = _f(session[0].get("open") or session[0].get("close"))
        last_trigger: datetime | None = None
        for index, row in enumerate(session):
            price = _f(row.get("close"))
            if price < min_price or not open_price:
                continue
            from_open = price / open_price - 1.0
            ret5 = _rolling_return(session, index, 5)
            rel_vol = _relative_volume(session, index)
            dollar_volume = _dollar_volume(session, index)
            trigger_kind = None
            trigger_value = None
            if from_open >= move_pct:
                trigger_kind, trigger_value = "session_return", from_open
            elif ret5 is not None and ret5 >= move_5m_pct:
                trigger_kind, trigger_value = "return_5m", ret5
            elif (
                rel_vol is not None
                and rel_vol >= relative_volume_threshold
                and ret5 is not None
                and ret5 > 0
            ):
                trigger_kind, trigger_value = "relative_volume", rel_vol
            if trigger_kind is None or dollar_volume < min_dollar_volume:
                continue
            if last_trigger and row["ts"] - last_trigger < cooldown:
                continue
            last_trigger = row["ts"]
            events.append(
                {
                    "trigger_ts": row["ts"],
                    "window_start": max(run_start, row["ts"] - timedelta(minutes=before_minutes)),
                    "window_end": min(run_end, row["ts"] + timedelta(minutes=after_minutes)),
                    "trigger_kind": trigger_kind,
                    "trigger_value": trigger_value,
                    "reason": {
                        "session_date": session_date.isoformat(),
                        "return_from_open": from_open,
                        "return_5m": ret5,
                        "relative_volume_5m": rel_vol,
                        "dollar_volume_5m": dollar_volume,
                        "price": price,
                    },
                }
            )
    return events


def detect_crypto_capture_windows(
    rows: list[dict[str, Any]],
    *,
    run_start: datetime,
    run_end: datetime,
    move_5m_pct: float,
    move_15m_pct: float,
    relative_volume_threshold: float,
    min_price: float,
    min_dollar_volume: float,
    cooldown_minutes: int,
    before_minutes: int,
    after_minutes: int,
) -> list[dict[str, Any]]:
    """Return neutral 24/7 crypto acquisition windows."""
    rows = sorted(rows, key=lambda item: item["ts"])
    events: list[dict[str, Any]] = []
    last_trigger: datetime | None = None
    cooldown = timedelta(minutes=cooldown_minutes)
    for index, row in enumerate(rows):
        price = _f(row.get("close"))
        if price < min_price:
            continue
        ret5 = _rolling_return(rows, index, 5)
        ret15 = _rolling_return(rows, index, 15)
        rel_vol = _relative_volume(rows, index)
        dollar_volume = _dollar_volume(rows, index)
        trigger_kind = None
        trigger_value = None
        if ret5 is not None and ret5 >= move_5m_pct:
            trigger_kind, trigger_value = "return_5m", ret5
        elif ret15 is not None and ret15 >= move_15m_pct:
            trigger_kind, trigger_value = "return_15m", ret15
        elif (
            rel_vol is not None
            and rel_vol >= relative_volume_threshold
            and ret5 is not None
            and ret5 > 0
        ):
            trigger_kind, trigger_value = "relative_volume", rel_vol
        if trigger_kind is None or dollar_volume < min_dollar_volume:
            continue
        if last_trigger and row["ts"] - last_trigger < cooldown:
            continue
        last_trigger = row["ts"]
        events.append(
            {
                "trigger_ts": row["ts"],
                "window_start": max(run_start, row["ts"] - timedelta(minutes=before_minutes)),
                "window_end": min(run_end, row["ts"] + timedelta(minutes=after_minutes)),
                "trigger_kind": trigger_kind,
                "trigger_value": trigger_value,
                "reason": {
                    "return_5m": ret5,
                    "return_15m": ret15,
                    "relative_volume_5m": rel_vol,
                    "dollar_volume_5m": dollar_volume,
                    "price": price,
                },
            }
        )
    return events


def scan_capture_partition(partition: dict[str, Any]) -> int:
    settings = get_settings()
    instrument = fetch_one(
        "select id,provider,provider_symbol,canonical_symbol,asset_class from instruments where id=%s",
        (partition["instrument_id"],),
    )
    if not instrument:
        return 0
    rows = fetch_all(
        """
        select ts,open,high,low,close,volume,quote_volume,trade_count,vwap,
               taker_buy_base_volume,taker_buy_quote_volume
          from market_bars_1m
         where provider=%s and instrument_id=%s and ts >= %s and ts < %s
         order by ts
        """,
        (instrument["provider"], instrument["id"], partition["start_ts"], partition["end_ts"]),
    )
    if instrument["asset_class"] == "us_equity":
        events = detect_equity_capture_windows(
            rows,
            run_start=partition["start_ts"],
            run_end=partition["end_ts"],
            move_pct=settings.equity_capture_move_pct / 100.0,
            move_5m_pct=settings.equity_capture_5m_move_pct / 100.0,
            relative_volume_threshold=settings.capture_relative_volume,
            min_price=settings.capture_min_price,
            min_dollar_volume=settings.capture_min_dollar_volume,
            cooldown_minutes=settings.capture_cooldown_minutes,
            before_minutes=settings.capture_window_before_minutes,
            after_minutes=settings.capture_window_after_minutes,
        )
    else:
        events = detect_crypto_capture_windows(
            rows,
            run_start=partition["start_ts"],
            run_end=partition["end_ts"],
            move_5m_pct=settings.crypto_capture_5m_move_pct / 100.0,
            move_15m_pct=settings.crypto_capture_15m_move_pct / 100.0,
            relative_volume_threshold=settings.capture_relative_volume,
            min_price=settings.capture_min_price,
            min_dollar_volume=settings.capture_min_dollar_volume,
            cooldown_minutes=settings.capture_cooldown_minutes,
            before_minutes=settings.capture_window_before_minutes,
            after_minutes=settings.capture_window_after_minutes,
        )
    events = events[:100]
    with db_connection() as conn, conn.cursor() as cur:
        for event in events:
            cur.execute(
                """
                insert into capture_windows(
                    run_id,provider,asset_class,instrument_id,provider_symbol,canonical_symbol,
                    trigger_ts,window_start,window_end,trigger_kind,trigger_value,rule_version,reason
                ) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                on conflict do nothing
                """,
                (
                    partition["run_id"], instrument["provider"], instrument["asset_class"],
                    instrument["id"], instrument["provider_symbol"], instrument["canonical_symbol"],
                    event["trigger_ts"], event["window_start"], event["window_end"],
                    event["trigger_kind"], event["trigger_value"], RULE_VERSION, Jsonb(event["reason"]),
                ),
            )
        cur.execute(
            "update collection_partitions set cursor=%s,row_count=%s,heartbeat_at=now(),updated_at=now() where id=%s",
            (Jsonb({"finished": True, "events": len(events), "rule_version": RULE_VERSION}), len(events), partition["id"]),
        )
        conn.commit()
    return len(events)


def _chunks(start: datetime, end: datetime, minutes: int) -> Iterable[tuple[datetime, datetime]]:
    cursor = start
    step = timedelta(minutes=minutes)
    while cursor < end:
        next_end = min(end, cursor + step)
        yield cursor, next_end
        cursor = next_end


def _merged_windows(rows: list[dict[str, Any]]) -> list[tuple[datetime, datetime]]:
    if not rows:
        return []
    windows = sorted((row["window_start"], row["window_end"]) for row in rows)
    merged: list[list[datetime]] = [[windows[0][0], windows[0][1]]]
    for start, end in windows[1:]:
        if start <= merged[-1][1]:
            merged[-1][1] = max(merged[-1][1], end)
        else:
            merged.append([start, end])
    return [(row[0], row[1]) for row in merged]


def plan_capture_scan(run_id: UUID) -> int:
    settings = get_settings()
    run = fetch_one("select * from collection_runs where id=%s", (run_id,))
    if not run:
        return 0
    instruments = fetch_all(
        """
        select i.id,i.provider,i.provider_symbol,i.priority
          from instruments i
         where i.provider = any(%s)
           and i.provider in ('alpaca','coinbase','binance')
           and exists (
               select 1 from market_bars_1m b
                where b.provider=i.provider and b.instrument_id=i.id
                  and b.ts >= %s and b.ts < %s
           )
         order by i.priority desc
        """,
        (run["providers"], run["start_ts"], run["end_ts"]),
    )
    inserted = 0
    with db_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            update collection_runs
               set stage='capture_scan',status='running',completed_at=null,
                   enhancement_requested=true,
                   enhancement_started_at=coalesce(enhancement_started_at,now()),updated_at=now()
             where id=%s
            """,
            (run_id,),
        )
        for instrument in instruments:
            cur.execute(
                """
                insert into collection_partitions(
                    run_id,provider,instrument_id,provider_symbol,data_type,start_ts,end_ts,
                    status,priority,max_attempts
                ) values (%s,'miner',%s,%s,'capture_scan',%s,%s,'queued',%s,%s)
                on conflict do nothing
                """,
                (
                    run_id, instrument["id"], instrument["provider_symbol"],
                    run["start_ts"], run["end_ts"], int(instrument["priority"] or 0),
                    settings.max_partition_attempts,
                ),
            )
            inserted += cur.rowcount
        if not instruments:
            cur.execute("update collection_runs set stage='enrichment',updated_at=now() where id=%s", (run_id,))
        cur.execute("select refresh_collection_run_counts(%s)", (run_id,))
        conn.commit()
    return inserted


def plan_enrichment(run_id: UUID) -> int:
    settings = get_settings()
    run = fetch_one("select * from collection_runs where id=%s", (run_id,))
    if not run:
        return 0
    windows = fetch_all(
        """
        select cw.*,i.priority
          from capture_windows cw join instruments i on i.id=cw.instrument_id
         where cw.run_id=%s
         order by cw.provider,cw.instrument_id,cw.window_start
         limit %s
        """,
        (run_id, settings.max_capture_windows_per_run),
    )
    by_instrument: dict[tuple[str, UUID, str], list[dict[str, Any]]] = defaultdict(list)
    for row in windows:
        by_instrument[(row["provider"], row["instrument_id"], row["provider_symbol"])].append(row)

    inserted = 0
    with db_connection() as conn, conn.cursor() as cur:
        cur.execute("update collection_runs set stage='enrichment',status='running',updated_at=now() where id=%s", (run_id,))

        # Targeted historical trade/quote data. These windows include false positives;
        # the downstream integration layer decides what they mean.
        for (provider, instrument_id, symbol), event_rows in by_instrument.items():
            for start, end in _merged_windows(event_rows):
                if provider == "alpaca":
                    data_types = ("trades", "quotes")
                elif provider == "binance":
                    data_types = ("trades",)
                else:
                    data_types = ()
                for data_type in data_types:
                    for chunk_start, chunk_end in _chunks(start, end, settings.microstructure_partition_minutes):
                        cur.execute(
                            """
                            insert into collection_partitions(
                                run_id,provider,instrument_id,provider_symbol,data_type,start_ts,end_ts,
                                status,priority,max_attempts,cursor
                            ) values (%s,%s,%s,%s,%s,%s,%s,'queued',5000,%s,%s)
                            on conflict do nothing
                            """,
                            (
                                run_id, provider, instrument_id, symbol, data_type, chunk_start, chunk_end,
                                settings.max_partition_attempts,
                                Jsonb({"capture_window_ids": [str(row["id"]) for row in event_rows]}),
                            ),
                        )
                        inserted += cur.rowcount

        # Massive reference/float/short interest. Default is all Alpaca symbols.
        if settings.equity_enrichment_enabled and "alpaca" in run["providers"]:
            if settings.equity_context_scope == "all":
                equity_rows = fetch_all(
                    "select id,provider_symbol,priority from instruments where provider='alpaca' and preferred=true order by priority desc"
                )
            else:
                equity_rows = fetch_all(
                    """
                    select distinct i.id,i.provider_symbol,i.priority
                      from capture_windows cw join instruments i on i.id=cw.instrument_id
                     where cw.run_id=%s and cw.provider='alpaca'
                    """,
                    (run_id,),
                )
            captured_equities = {
                (row["instrument_id"], row["provider_symbol"])
                for row in windows if row["provider"] == "alpaca"
            }
            for equity in equity_rows:
                cur.execute(
                    """
                    insert into collection_partitions(
                        run_id,provider,instrument_id,provider_symbol,data_type,start_ts,end_ts,
                        status,priority,max_attempts
                    ) values (%s,'massive',%s,%s,'massive_context',%s,%s,'queued',3500,%s)
                    on conflict do nothing
                    """,
                    (
                        run_id, equity["id"], equity["provider_symbol"], run["start_ts"], run["end_ts"],
                        settings.max_partition_attempts,
                    ),
                )
                inserted += cur.rowcount
            for instrument_id, symbol in captured_equities:
                for provider, data_type, priority in (
                    ("sec", "sec_filings", 3200),
                    ("alpaca", "news", 3000),
                ):
                    cur.execute(
                        """
                        insert into collection_partitions(
                            run_id,provider,instrument_id,provider_symbol,data_type,start_ts,end_ts,
                            status,priority,max_attempts
                        ) values (%s,%s,%s,%s,%s,%s,%s,'queued',%s,%s)
                        on conflict do nothing
                        """,
                        (
                            run_id, provider, instrument_id, symbol, data_type,
                            run["start_ts"], run["end_ts"], priority, settings.max_partition_attempts,
                        ),
                    )
                    inserted += cur.rowcount

            day = run["start_ts"].date()
            while day <= run["end_ts"].date():
                if day.weekday() < 5:
                    day_start = datetime.combine(day, datetime.min.time(), tzinfo=timezone.utc)
                    cur.execute(
                        """
                        insert into collection_partitions(
                            run_id,provider,data_type,start_ts,end_ts,status,priority,max_attempts
                        ) values (%s,'finra','finra_short_volume',%s,%s,'queued',2500,%s)
                        on conflict do nothing
                        """,
                        (run_id, day_start, day_start + timedelta(days=1), settings.max_partition_attempts),
                    )
                    inserted += cur.rowcount
                day += timedelta(days=1)

        if settings.crypto_enrichment_enabled and any(p in run["providers"] for p in ("coinbase", "binance")):
            for provider, data_type, priority in (
                ("crypto", "crypto_catalogues", 2800),
                ("coingecko", "coingecko_supply", 2400),
            ):
                cur.execute(
                    """
                    insert into collection_partitions(
                        run_id,provider,data_type,start_ts,end_ts,status,priority,max_attempts
                    ) values (%s,%s,%s,%s,%s,'queued',%s,%s)
                    on conflict do nothing
                    """,
                    (
                        run_id, provider, data_type, run["start_ts"], run["end_ts"],
                        priority, settings.max_partition_attempts,
                    ),
                )
                inserted += cur.rowcount

            # One derivatives backfill per preferred Binance base, capped by liquidity priority.
            crypto_rows = fetch_all(
                """
                select distinct on (canonical_symbol) canonical_symbol,provider_symbol,priority
                  from instruments
                 where provider='binance' and asset_class='crypto_spot' and preferred=true
                 order by canonical_symbol,priority desc
                 limit %s
                """,
                (settings.crypto_derivatives_symbol_cap,),
            )
            for item in crypto_rows:
                cur.execute(
                    """
                    insert into collection_partitions(
                        run_id,provider,provider_symbol,data_type,start_ts,end_ts,status,priority,max_attempts,cursor
                    ) values (%s,'binance_futures',%s,'crypto_derivatives',%s,%s,'queued',2200,%s,%s)
                    on conflict do nothing
                    """,
                    (
                        run_id, item["canonical_symbol"], run["start_ts"], run["end_ts"],
                        settings.max_partition_attempts,
                        Jsonb({"canonical_symbol": item["canonical_symbol"]}),
                    ),
                )
                inserted += cur.rowcount

        cur.execute("update capture_windows set planned=true,updated_at=now() where run_id=%s", (run_id,))
        if inserted == 0:
            cur.execute(
                """
                update collection_runs
                   set stage='ready',enhancement_completed_at=now(),updated_at=now()
                 where id=%s
                """,
                (run_id,),
            )
        cur.execute("select refresh_collection_run_counts(%s)", (run_id,))
        conn.commit()
    return inserted


def request_enhancement(run_id: UUID) -> None:
    """Reuse an existing v1 bar run without recollecting it."""
    with db_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            update collection_runs
               set enhancement_requested=true,stage='collecting',status='running',
                   completed_at=null,error=null,updated_at=now()
             where id=%s and status not in ('cancelled','failed')
            """,
            (run_id,),
        )
        cur.execute(
            """
            update collection_partitions
               set status='retry_wait',not_before=now(),locked_by=null,locked_at=null,updated_at=now()
             where run_id=%s and status='running'
            """,
            (run_id,),
        )
        cur.execute("select refresh_collection_run_counts(%s)", (run_id,))
        conn.commit()


def _stage_is_terminal(run_id: UUID, data_types: tuple[str, ...]) -> bool:
    row = fetch_one(
        """
        select count(*) filter (where status in ('queued','retry_wait','running')) as active,
               count(*) as total
          from collection_partitions
         where run_id=%s and data_type = any(%s)
        """,
        (run_id, list(data_types)),
    )
    return bool(row) and int(row["active"] or 0) == 0


def advance_mining_runs() -> int:
    """Advance durable mining stages once all partitions in the current stage are terminal."""
    candidates = fetch_all(
        """
        select id,stage,status,providers,enhancement_requested
          from collection_runs
         where status in ('queued','running','completed','completed_with_errors')
           and stage in ('collecting','capture_scan','enrichment')
         order by created_at
        """
    )
    advanced = 0
    for candidate in candidates:
        lock_key = f"mining-stage:{candidate['id']}"
        with db_connection() as lock_conn, lock_conn.cursor() as lock_cur:
            lock_cur.execute("select pg_try_advisory_lock(hashtext(%s)) as acquired", (lock_key,))
            acquired = bool(lock_cur.fetchone()["acquired"])
            lock_conn.commit()
            if not acquired:
                continue
            try:
                run = fetch_one("select * from collection_runs where id=%s", (candidate["id"],))
                if not run or run["status"] in {"paused", "cancelled", "failed"}:
                    continue
                if run["stage"] == "collecting":
                    if not _stage_is_terminal(run["id"], ("catalogue", "bars_1m")):
                        continue
                    if get_settings().capture_scan_enabled:
                        plan_capture_scan(run["id"])
                    else:
                        plan_enrichment(run["id"])
                    advanced += 1
                elif run["stage"] == "capture_scan":
                    if not _stage_is_terminal(run["id"], ("capture_scan",)):
                        continue
                    plan_enrichment(run["id"])
                    advanced += 1
                elif run["stage"] == "enrichment":
                    row = fetch_one(
                        """
                        select count(*) filter (where status in ('queued','retry_wait','running')) as active
                          from collection_partitions where run_id=%s
                        """,
                        (run["id"],),
                    )
                    if int(row["active"] or 0) > 0:
                        continue
                    with db_connection() as conn, conn.cursor() as cur:
                        cur.execute(
                            """
                            update collection_runs
                               set stage='ready',enhancement_completed_at=now(),updated_at=now()
                             where id=%s
                            """,
                            (run["id"],),
                        )
                        cur.execute("select refresh_collection_run_counts(%s)", (run["id"],))
                        conn.commit()
                    advanced += 1
            finally:
                lock_cur.execute("select pg_advisory_unlock(hashtext(%s))", (lock_key,))
                lock_conn.commit()
    return advanced
