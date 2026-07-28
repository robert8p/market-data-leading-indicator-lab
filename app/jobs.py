from __future__ import annotations

import hashlib
import logging
import math
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable, Iterator
from uuid import UUID
from zoneinfo import ZoneInfo

from psycopg.types.json import Jsonb

from app.config import get_settings
from app.db import db_connection, fetch_all, fetch_one
from app.exceptions import CancelRequested, PauseRequested


logger = logging.getLogger(__name__)
PRIMARY_PROVIDERS = ("alpaca", "coinbase", "binance")
ALL_PROVIDERS = (*PRIMARY_PROVIDERS, "twelvedata")


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def floor_minute(value: datetime) -> datetime:
    return value.astimezone(timezone.utc).replace(second=0, microsecond=0)


def create_collection_run(name: str, providers: list[str], days: int = 30) -> UUID:
    settings = get_settings()
    end_ts = floor_minute(utc_now() - timedelta(minutes=settings.collection_end_lag_minutes))
    start_ts = end_ts - timedelta(days=days)
    providers = [provider for provider in providers if provider in ALL_PROVIDERS]
    if not providers:
        raise ValueError("At least one valid provider is required")

    config = {
        "days": days,
        "collector_version": "3.3.0",
        "collector_only": True,
        "alpaca_feed": settings.alpaca_feed,
        "alpaca_otc_enabled": settings.alpaca_otc_enabled,
        "alpaca_adjustment": "split",
        "collection_end_lag_minutes": settings.collection_end_lag_minutes,
        "binance_pair_mode": settings.binance_pair_mode,
        "crypto_full_pair_universe": settings.crypto_full_pair_universe,
        "crypto_broad_observation_seconds": settings.crypto_broad_observation_seconds,
        "equity_baseline_sample_rate": settings.equity_baseline_sample_rate,
        "twelvedata_symbol_cap": settings.twelvedata_symbol_cap,
        "capture_scan_enabled": settings.capture_scan_enabled,
        "equity_enrichment_enabled": settings.equity_enrichment_enabled,
        "crypto_enrichment_enabled": settings.crypto_enrichment_enabled,
        "integration_layer_exports": False,
    }

    with db_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            insert into collection_runs(
                name,status,stage,start_ts,end_ts,providers,config,enhancement_requested
            )
            values (%s,'queued','catalogue',%s,%s,%s,%s,true)
            returning id
            """,
            (name, start_ts, end_ts, providers, Jsonb(config)),
        )
        run_id = cur.fetchone()["id"]
        for provider in PRIMARY_PROVIDERS:
            if provider not in providers:
                continue
            cur.execute(
                """
                insert into collection_partitions(
                    run_id,provider,data_type,status,priority,max_attempts
                ) values (%s,%s,'catalogue','queued',1000,%s)
                on conflict do nothing
                """,
                (run_id, provider, settings.max_partition_attempts),
            )
        conn.commit()
    return run_id

def upsert_instruments(items: list[dict[str, Any]], replace_provider: str | None = None) -> int:
    sql = """
        insert into instruments(
            provider, provider_symbol, canonical_symbol, display_name, asset_class,
            base_asset, quote_asset, exchange, status, tradable, preferred,
            source_feed, priority, metadata, last_seen_at
        ) values (
            %(provider)s, %(provider_symbol)s, %(canonical_symbol)s, %(display_name)s, %(asset_class)s,
            %(base_asset)s, %(quote_asset)s, %(exchange)s, %(status)s, %(tradable)s, %(preferred)s,
            %(source_feed)s, %(priority)s, %(metadata)s, now()
        )
        on conflict(provider, provider_symbol) do update set
            canonical_symbol = excluded.canonical_symbol,
            display_name = excluded.display_name,
            asset_class = excluded.asset_class,
            base_asset = excluded.base_asset,
            quote_asset = excluded.quote_asset,
            exchange = excluded.exchange,
            status = excluded.status,
            tradable = excluded.tradable,
            preferred = excluded.preferred,
            source_feed = excluded.source_feed,
            priority = excluded.priority,
            metadata = excluded.metadata,
            last_seen_at = now()
    """
    prepared = [{**item, "metadata": Jsonb(item.get("metadata") or {})} for item in items]
    with db_connection() as conn, conn.cursor() as cur:
        if replace_provider:
            cur.execute(
                "update instruments set tradable=false, preferred=false, last_seen_at=now() where provider=%s",
                (replace_provider,),
            )
        if prepared:
            cur.executemany(sql, prepared)
        if replace_provider and items:
            snapshot_ts = utc_now().replace(second=0, microsecond=0)
            by_class: dict[str, list[dict[str, Any]]] = {}
            for item in items:
                by_class.setdefault(item.get("asset_class") or "unknown", []).append(item)
            for asset_class, class_items in by_class.items():
                cur.execute(
                    """
                    insert into market_universe_snapshots(
                        provider,snapshot_ts,asset_class,tradable_count,preferred_count,metadata
                    ) values (%s,%s,%s,%s,%s,%s)
                    on conflict(provider,snapshot_ts,asset_class) do update set
                        tradable_count=excluded.tradable_count,preferred_count=excluded.preferred_count,
                        metadata=excluded.metadata
                    """,
                    (
                        replace_provider,snapshot_ts,asset_class,
                        sum(1 for item in class_items if item.get("tradable")),
                        sum(1 for item in class_items if item.get("preferred")),
                        Jsonb({"catalogue_rows": len(class_items)}),
                    ),
                )
        conn.commit()
    return len(items)



SYSTEM_GROUPS: dict[str, tuple[str, str]] = {
    "all_alpaca": ("All Alpaca instruments", "All preferred Alpaca US equity instruments in the current catalogue."),
    "all_coinbase": ("All Coinbase spot pairs", "All preferred Coinbase Exchange spot products."),
    "all_binance": ("All Binance spot pairs", "All preferred Binance.com spot products."),
    "all_twelvedata": ("All Twelve Data indicators", "All quota-controlled Twelve Data validation and indicator instruments."),
    "all_crypto_spot": ("All crypto spot", "Preferred Coinbase and Binance spot instruments."),
    "crypto_majors": ("Crypto majors", "BTC, ETH, SOL, XRP, BNB, ADA and DOGE across configured crypto providers."),
    "us_broad_market": ("US broad-market proxies", "SPY, QQQ, IWM and DIA where available."),
    "us_sector_proxies": ("US sector proxies", "SPDR sector ETFs used for cross-sector and breadth analysis."),
    "rates_credit_proxies": ("Rates and credit proxies", "Treasury-duration and credit ETFs such as TLT, IEF, SHY, HYG and LQD."),
    "commodity_proxies": ("Commodity proxies", "Gold, silver, oil and diversified commodity proxies where available."),
    "volatility_currency_proxies": ("Volatility and currency proxies", "VIX/DXY and listed volatility or currency proxy instruments where available."),
}


def refresh_instrument_groups() -> int:
    """Rebuild reusable system instrument groups after provider catalogues are refreshed."""
    with db_connection() as conn, conn.cursor() as cur:
        for group_id, (display_name, description) in SYSTEM_GROUPS.items():
            cur.execute(
                """
                insert into instrument_groups(id, display_name, description, is_system, updated_at)
                values (%s,%s,%s,true,now())
                on conflict(id) do update set
                    display_name=excluded.display_name, description=excluded.description,
                    is_system=true, updated_at=now()
                """,
                (group_id, display_name, description),
            )
        cur.execute(
            "delete from instrument_group_members where group_id in (select id from instrument_groups where is_system=true)"
        )
        membership_sql = {
            "all_alpaca": "provider='alpaca' and preferred=true",
            "all_coinbase": "provider='coinbase' and preferred=true",
            "all_binance": "provider='binance' and preferred=true",
            "all_twelvedata": "provider='twelvedata' and preferred=true",
            "all_crypto_spot": "provider in ('coinbase','binance') and asset_class='crypto_spot' and preferred=true",
            "crypto_majors": "canonical_symbol in ('BTC','ETH','SOL','XRP','BNB','ADA','DOGE') and provider in ('coinbase','binance','twelvedata') and preferred=true",
            "us_broad_market": "upper(provider_symbol) in ('SPY','QQQ','IWM','DIA') and preferred=true",
            "us_sector_proxies": "upper(provider_symbol) in ('XLB','XLC','XLE','XLF','XLI','XLK','XLP','XLRE','XLU','XLV','XLY') and preferred=true",
            "rates_credit_proxies": "upper(provider_symbol) in ('TLT','IEF','SHY','HYG','LQD') and preferred=true",
            "commodity_proxies": "upper(provider_symbol) in ('GLD','SLV','USO','DBA','XAU/USD','XAG/USD') and preferred=true",
            "volatility_currency_proxies": "upper(provider_symbol) in ('VIX','DXY','VXX','UUP','FXE','FXY','EUR/USD','GBP/USD','USD/JPY','USD/CHF','AUD/USD') and preferred=true",
        }
        for group_id, condition in membership_sql.items():
            cur.execute(
                f"""
                insert into instrument_group_members(group_id, instrument_id)
                select %s, id from instruments where {condition}
                on conflict do nothing
                """,
                (group_id,),
            )
        cur.execute("select count(*) as count from instrument_group_members")
        count = int(cur.fetchone()["count"])
        conn.commit()
        return count


def _infer_indicator_class(symbol: str) -> tuple[str, str | None, str | None]:
    symbol = symbol.upper()
    if "/" in symbol:
        base, quote = symbol.split("/", 1)
        if base in {"BTC", "ETH", "SOL", "XRP", "BNB", "ADA", "DOGE"}:
            return "crypto_indicator", base, quote
        if base in {"XAU", "XAG", "WTI", "BRENT"}:
            return "commodity_indicator", base, quote
        return "forex", base, quote
    return "indicator_proxy", symbol, "USD"


def create_twelvedata_mappings(run_id: UUID) -> int:
    settings = get_settings()
    run = fetch_one("select providers from collection_runs where id = %s", (run_id,))
    if not run or "twelvedata" not in run["providers"]:
        return 0

    selected: list[dict[str, Any]] = []
    seen: set[str] = set()

    for index, symbol in enumerate(settings.twelvedata_indicators):
        symbol = symbol.strip().upper()
        if not symbol or symbol in seen:
            continue
        asset_class, base, quote = _infer_indicator_class(symbol)
        selected.append(
            {
                "provider": "twelvedata",
                "provider_symbol": symbol,
                "canonical_symbol": base or symbol,
                "display_name": symbol,
                "asset_class": asset_class,
                "base_asset": base,
                "quote_asset": quote,
                "exchange": "Twelve Data",
                "status": "candidate",
                "tradable": False,
                "preferred": True,
                "source_feed": "twelvedata_basic",
                "priority": 10_000 - index,
                "metadata": {"mapping_type": "curated_indicator"},
            }
        )
        seen.add(symbol)

    remaining = max(0, settings.twelvedata_symbol_cap - len(selected))
    primary_sources = [provider for provider in PRIMARY_PROVIDERS if provider in run["providers"]]
    if remaining and primary_sources:
        per_provider_limit = max(20, math.ceil(remaining / len(primary_sources)) * 4)
        buckets: dict[str, list[dict[str, Any]]] = {}
        for provider in primary_sources:
            buckets[provider] = fetch_all(
                """
                select provider, provider_symbol, canonical_symbol, display_name, asset_class,
                       base_asset, quote_asset, exchange, priority
                  from instruments
                 where provider = %s and tradable = true and preferred = true
                 order by priority desc, canonical_symbol, provider_symbol
                 limit %s
                """,
                (provider, per_provider_limit),
            )

        offsets = {provider: 0 for provider in primary_sources}
        while len(selected) < settings.twelvedata_symbol_cap:
            made_progress = False
            for provider in primary_sources:
                bucket = buckets[provider]
                offset = offsets[provider]
                if offset >= len(bucket):
                    continue
                candidate = bucket[offset]
                offsets[provider] += 1
                made_progress = True
                if provider == "alpaca":
                    td_symbol = candidate["provider_symbol"].upper()
                    base_asset = candidate["base_asset"] or td_symbol
                    quote_asset = "USD"
                    asset_class = "us_equity_validation"
                else:
                    base_asset = (candidate["base_asset"] or candidate["canonical_symbol"]).upper()
                    quote_asset = "USD"
                    td_symbol = f"{base_asset}/USD"
                    asset_class = "crypto_validation"
                if td_symbol in seen:
                    continue
                selected.append(
                    {
                        "provider": "twelvedata",
                        "provider_symbol": td_symbol,
                        "canonical_symbol": base_asset,
                        "display_name": f"{td_symbol} validation",
                        "asset_class": asset_class,
                        "base_asset": base_asset,
                        "quote_asset": quote_asset,
                        "exchange": "Twelve Data",
                        "status": "candidate",
                        "tradable": False,
                        "preferred": True,
                        "source_feed": "twelvedata_basic",
                        "priority": int(candidate["priority"] or 0),
                        "metadata": {
                            "mapping_type": "primary_provider_validation",
                            "source_provider": provider,
                            "source_symbol": candidate["provider_symbol"],
                        },
                    }
                )
                seen.add(td_symbol)
                if len(selected) >= settings.twelvedata_symbol_cap:
                    break
            if not made_progress:
                break

    return upsert_instruments(selected, replace_provider="twelvedata")


def _iter_windows(start: datetime, end: datetime, step: timedelta) -> Iterator[tuple[datetime, datetime]]:
    cursor = start
    while cursor < end:
        next_cursor = min(cursor + step, end)
        yield cursor, next_cursor
        cursor = next_cursor


def _alpaca_windows(start: datetime, end: datetime) -> Iterator[tuple[datetime, datetime]]:
    # Partition by New York trading date, not UTC date. This preserves the final
    # post-market hour during US standard time while still skipping weekends.
    market_tz = ZoneInfo("America/New_York")
    local_date = start.astimezone(market_tz).date()
    final_date = (end - timedelta(microseconds=1)).astimezone(market_tz).date()
    while local_date <= final_date:
        if local_date.weekday() < 5:
            local_start = datetime.combine(local_date, datetime.min.time(), tzinfo=market_tz)
            local_end = local_start + timedelta(days=1)
            window_start = max(start, local_start.astimezone(timezone.utc))
            window_end = min(end, local_end.astimezone(timezone.utc))
            if window_start < window_end:
                yield window_start, window_end
        local_date += timedelta(days=1)


def _provider_windows(provider: str, start: datetime, end: datetime) -> Iterator[tuple[datetime, datetime]]:
    if provider == "alpaca":
        yield from _alpaca_windows(start, end)
    elif provider == "coinbase":
        yield from _iter_windows(start, end, timedelta(minutes=300))
    elif provider == "binance":
        yield from _iter_windows(start, end, timedelta(minutes=999))
    elif provider == "twelvedata":
        yield from _iter_windows(start, end, timedelta(days=3))
    else:
        raise ValueError(f"Unknown provider: {provider}")



def _planning_is_active(cur, run_id: UUID) -> bool:
    cur.execute("select status from collection_runs where id=%s", (run_id,))
    row = cur.fetchone()
    return bool(row and row["status"] in {"queued", "running", "completed"})


def _plan_data_partitions_unlocked(run_id: UUID) -> int:
    settings = get_settings()
    with db_connection() as conn, conn.cursor() as cur:
        cur.execute("select * from collection_runs where id = %s for update", (run_id,))
        run = cur.fetchone()
        if not run:
            raise ValueError(f"Collection run not found: {run_id}")
        if run["stage"] == "collecting":
            conn.commit()
            return 0
        if run["status"] in {"paused", "cancelled", "failed", "completed_with_errors"}:
            conn.commit()
            return 0

        selected_primary = [provider for provider in PRIMARY_PROVIDERS if provider in run["providers"]]
        cur.execute(
            """
            select provider, status
              from collection_partitions
             where run_id = %s and data_type = 'catalogue'
            """,
            (run_id,),
        )
        catalogue_status = {row["provider"]: row["status"] for row in cur.fetchall()}
        terminal = {"completed", "completed_empty", "skipped", "failed"}
        if any(catalogue_status.get(provider) not in terminal for provider in selected_primary):
            conn.commit()
            return 0
        if any(catalogue_status.get(provider) == "failed" for provider in selected_primary):
            cur.execute(
                "update collection_runs set status='completed_with_errors', error='One or more provider catalogues failed', completed_at=now(), updated_at=now() where id=%s",
                (run_id,),
            )
            conn.commit()
            return 0
        cur.execute("update collection_runs set stage='planning', status='running', started_at=coalesce(started_at,now()), updated_at=now() where id=%s", (run_id,))
        conn.commit()

    create_twelvedata_mappings(run_id)
    refresh_instrument_groups()

    run = fetch_one("select * from collection_runs where id = %s", (run_id,))
    if not run or run["status"] in {"paused", "cancelled", "failed", "completed_with_errors"}:
        return 0
    total_inserted = 0
    with db_connection() as conn, conn.cursor() as cur:
        for provider in run["providers"]:
            if not _planning_is_active(cur, run_id):
                conn.commit()
                return total_inserted
            instrument_rows = cur.execute(
                """
                select id, provider_symbol, priority, source_feed
                  from instruments
                 where provider = %s and tradable = %s and preferred = true
                 order by priority desc, provider_symbol
                """,
                (provider, provider != "twelvedata"),
            ).fetchall()
            # Twelve Data instruments are validation/indicator instruments and deliberately have tradable=false.
            if provider == "twelvedata":
                instrument_rows = cur.execute(
                    """
                    select id, provider_symbol, priority
                      from instruments
                     where provider = 'twelvedata' and preferred = true
                     order by priority desc, provider_symbol
                     limit %s
                    """,
                    (settings.twelvedata_symbol_cap,),
                ).fetchall()

            batch: list[tuple[Any, ...]] = []
            for instrument in instrument_rows:
                for window_start, window_end in _provider_windows(provider, run["start_ts"], run["end_ts"]):
                    batch.append(
                        (
                            run_id,
                            provider,
                            instrument["id"],
                            instrument["provider_symbol"],
                            window_start,
                            window_end,
                            int(instrument["priority"] or 0),
                            settings.max_partition_attempts,
                            Jsonb({"feed": instrument.get("source_feed")}),
                        )
                    )
                    if len(batch) >= 5000:
                        if not _planning_is_active(cur, run_id):
                            conn.commit()
                            return total_inserted
                        cur.executemany(
                            """
                            insert into collection_partitions(
                                run_id, provider, instrument_id, provider_symbol, data_type,
                                start_ts, end_ts, status, priority, max_attempts, cursor
                            ) values (%s,%s,%s,%s,'bars_1m',%s,%s,'queued',%s,%s,%s)
                            on conflict do nothing
                            """,
                            batch,
                        )
                        total_inserted += len(batch)
                        conn.commit()
                        batch.clear()
            if batch:
                if not _planning_is_active(cur, run_id):
                    conn.commit()
                    return total_inserted
                cur.executemany(
                    """
                    insert into collection_partitions(
                        run_id, provider, instrument_id, provider_symbol, data_type,
                        start_ts, end_ts, status, priority, max_attempts, cursor
                    ) values (%s,%s,%s,%s,'bars_1m',%s,%s,'queued',%s,%s,%s)
                    on conflict do nothing
                    """,
                    batch,
                )
                total_inserted += len(batch)
                conn.commit()

        cur.execute(
            "update collection_runs set stage='collecting', status='running', updated_at=now() where id=%s and status in ('queued','running','completed')",
            (run_id,),
        )
        cur.execute("select refresh_collection_run_counts(%s)", (run_id,))
        conn.commit()
    return total_inserted


def plan_data_partitions(run_id: UUID) -> int:
    """Plan one run under a session advisory lock so multiple workers cannot plan it concurrently."""
    with db_connection() as lock_conn, lock_conn.cursor() as lock_cur:
        lock_cur.execute("select pg_try_advisory_lock(hashtext(%s)) as acquired", (str(run_id),))
        acquired = bool(lock_cur.fetchone()["acquired"])
        lock_conn.commit()
        if not acquired:
            return 0
        try:
            return _plan_data_partitions_unlocked(run_id)
        finally:
            lock_cur.execute("select pg_advisory_unlock(hashtext(%s))", (str(run_id),))
            lock_conn.commit()


def claim_collection_partition(worker_id: str) -> dict[str, Any] | None:
    return fetch_one("select * from claim_collection_partition(%s)", (worker_id,))


def collection_control_state(run_id: UUID) -> str:
    row = fetch_one("select status from collection_runs where id = %s", (run_id,))
    return row["status"] if row else "cancelled"


def assert_collection_active(run_id: UUID) -> None:
    state = collection_control_state(run_id)
    if state == "paused":
        raise PauseRequested()
    if state in {"cancelled", "failed", "completed", "completed_with_errors"}:
        raise CancelRequested()


def save_bar_page(partition_id: UUID, rows: list[dict[str, Any]], cursor: dict[str, Any]) -> int:
    columns = [
        "provider", "instrument_id", "ts", "open", "high", "low", "close", "volume",
        "quote_volume", "trade_count", "vwap", "taker_buy_base_volume",
        "taker_buy_quote_volume", "source_feed",
    ]
    conflict_sql = """
        on conflict(provider, instrument_id, ts) do update set
            open=excluded.open, high=excluded.high, low=excluded.low, close=excluded.close,
            volume=excluded.volume, quote_volume=excluded.quote_volume,
            trade_count=excluded.trade_count, vwap=excluded.vwap,
            taker_buy_base_volume=excluded.taker_buy_base_volume,
            taker_buy_quote_volume=excluded.taker_buy_quote_volume,
            source_feed=excluded.source_feed
    """
    with db_connection() as conn, conn.cursor() as cur:
        for offset in range(0, len(rows), 1000):
            chunk = rows[offset : offset + 1000]
            row_placeholder = "(" + ",".join(["%s"] * len(columns)) + ")"
            values_sql = ",".join([row_placeholder] * len(chunk))
            params = [row.get(column) for row in chunk for column in columns]
            cur.execute(
                f"insert into market_bars_1m({','.join(columns)}) values {values_sql} {conflict_sql}",
                params,
            )
        cur.execute(
            """
            update collection_partitions
               set cursor=%s,
                   row_count=row_count + %s,
                   heartbeat_at=now(),
                   updated_at=now()
             where id=%s
            """,
            (Jsonb(cursor), len(rows), partition_id),
        )
        conn.commit()
    return len(rows)



def _save_tick_page(
    partition_id: UUID,
    table: str,
    columns: list[str],
    rows: list[dict[str, Any]],
    cursor: dict[str, Any],
) -> int:
    allowed = {"market_trades", "market_quotes_l1"}
    if table not in allowed:
        raise ValueError(f"Unsupported tick table: {table}")
    with db_connection() as conn, conn.cursor() as cur:
        if rows:
            prepared = []
            for row in rows:
                item = dict(row)
                for key in ("conditions", "metadata"):
                    item[key] = Jsonb(item.get(key) or ([] if key == "conditions" else {}))
                prepared.append(item)
            placeholders = ",".join(["%s"] * len(columns))
            sql = (
                f"insert into {table}({','.join(columns)}) values ({placeholders}) "
                "on conflict do nothing"
            )
            cur.executemany(sql, [[row.get(column) for column in columns] for row in prepared])
        cur.execute(
            """
            update collection_partitions
               set cursor=%s,row_count=row_count+%s,heartbeat_at=now(),updated_at=now()
             where id=%s
            """,
            (Jsonb(cursor), len(rows), partition_id),
        )
        conn.commit()
    return len(rows)


def save_trade_page(partition_id: UUID, rows: list[dict[str, Any]], cursor: dict[str, Any]) -> int:
    return _save_tick_page(
        partition_id,
        "market_trades",
        [
            "provider","instrument_id","message_key","ts","price","size","quote_size",
            "aggressor_side","exchange","trade_id","conditions","source_feed","metadata",
        ],
        rows,
        cursor,
    )


def save_quote_page(partition_id: UUID, rows: list[dict[str, Any]], cursor: dict[str, Any]) -> int:
    return _save_tick_page(
        partition_id,
        "market_quotes_l1",
        [
            "provider","instrument_id","message_key","ts","bid_exchange","bid_price","bid_size",
            "ask_exchange","ask_price","ask_size","conditions","source_feed","metadata",
        ],
        rows,
        cursor,
    )

def complete_partition(partition_id: UUID, *, empty: bool = False, checksum: str | None = None) -> None:
    with db_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            update collection_partitions
               set status=%s, checksum=%s, locked_by=null, heartbeat_at=now(), updated_at=now(), last_error=null
             where id=%s
            returning run_id
            """,
            ("completed_empty" if empty else "completed", checksum, partition_id),
        )
        row = cur.fetchone()
        if row:
            cur.execute("select refresh_collection_run_counts(%s)", (row["run_id"],))
        conn.commit()


def skip_partition(partition_id: UUID, message: str, code: str | None = None) -> None:
    with db_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            update collection_partitions
               set status='skipped', last_error=%s, error_code=%s, locked_by=null, heartbeat_at=now(), updated_at=now()
             where id=%s returning run_id
            """,
            (message[:4000], code, partition_id),
        )
        row = cur.fetchone()
        if row:
            cur.execute("select refresh_collection_run_counts(%s)", (row["run_id"],))
        conn.commit()


def retry_or_fail_partition(
    partition: dict[str, Any], message: str, code: str | None, retry_at: datetime | None, retryable: bool
) -> None:
    attempts = int(partition["attempts"] or 0)
    max_attempts = int(partition["max_attempts"] or get_settings().max_partition_attempts)
    should_retry = retryable and attempts < max_attempts
    if should_retry:
        delay_seconds = min(3600, 15 * (2 ** max(0, attempts - 1)))
        not_before = retry_at or (utc_now() + timedelta(seconds=delay_seconds))
        new_status = "retry_wait"
    else:
        not_before = None
        new_status = "failed"

    with db_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            update collection_partitions
               set status=%s, not_before=%s, last_error=%s, error_code=%s,
                   locked_by=null, heartbeat_at=now(), updated_at=now()
             where id=%s returning run_id
            """,
            (new_status, not_before, message[:4000], code, partition["id"]),
        )
        row = cur.fetchone()
        if row and partition.get("provider") == "twelvedata" and code == "rate_limit" and not_before:
            # A Twelve Data quota/rate-limit response applies to the API key, not just one symbol.
            # Defer the remaining Twelve Data queue together rather than burning requests on
            # hundreds of partitions that are guaranteed to receive the same response.
            cur.execute(
                """
                update collection_partitions
                   set not_before = case
                           when not_before is null or not_before < %s then %s
                           else not_before
                       end,
                       status = case when status='queued' then 'retry_wait' else status end,
                       updated_at = now()
                 where run_id=%s and provider='twelvedata'
                   and status in ('queued','retry_wait')
                """,
                (not_before, not_before, row["run_id"]),
            )
        if row:
            cur.execute("select refresh_collection_run_counts(%s)", (row["run_id"],))
        conn.commit()


def release_partition_for_pause(partition_id: UUID) -> None:
    with db_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            update collection_partitions
               set status='queued', locked_by=null, locked_at=null, heartbeat_at=now(), updated_at=now()
             where id=%s
            """,
            (partition_id,),
        )
        conn.commit()


def cancel_running_partition(partition_id: UUID) -> None:
    with db_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            update collection_partitions
               set status='cancelled', locked_by=null, heartbeat_at=now(), updated_at=now()
             where id=%s
            """,
            (partition_id,),
        )
        conn.commit()


def checksum_rows(rows_seen: int, first_ts: datetime | None, last_ts: datetime | None) -> str:
    raw = f"{rows_seen}|{first_ts.isoformat() if first_ts else ''}|{last_ts.isoformat() if last_ts else ''}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def reclaim_stale_work() -> dict[str, int]:
    settings = get_settings()
    interval = timedelta(minutes=settings.stale_partition_minutes)
    with db_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            update collection_partitions cp
               set status='retry_wait',not_before=now(),locked_by=null,locked_at=null,
                   last_error=coalesce(last_error,'') || E'\nRecovered stale running partition',
                   updated_at=now()
              from collection_runs cr
             where cp.run_id=cr.id
               and cp.status='running'
               and cp.heartbeat_at < now() - %s
               and cr.status in ('queued','running')
            """,
            (interval,),
        )
        collection_count = cur.rowcount
        conn.commit()
    return {"collection": collection_count}

def find_runs_ready_for_planning() -> list[UUID]:
    rows = fetch_all(
        """
        select cr.id
          from collection_runs cr
         where cr.stage in ('catalogue','planning')
           and cr.status in ('queued','running','completed')
         order by cr.created_at
        """
    )
    return [row["id"] for row in rows]
