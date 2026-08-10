from __future__ import annotations

import csv
import hashlib
import io
import logging
import math
import tempfile
import zipfile
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from uuid import UUID

import httpx
from psycopg.types.json import Jsonb

from app.cint001_contract import (
    CROSS_SECTIONAL_BUCKETS,
    ENTRY_OFFSET_MINUTES,
    EXECUTION_SPEC,
    HOLD_MINUTES,
    RETURN_LOOKBACK_BARS,
    RULE_VERSION,
    SELECT_BUCKET,
    UNIVERSE,
    VALIDATION_END,
    VALIDATION_SIGNAL_END,
    VALIDATION_START,
    FINAL_HOLDOUT_START,
    FINAL_HOLDOUT_END,
)
from app.db import db_connection, fetch_one

logger = logging.getLogger(__name__)
DATA_BASE = "https://data.binance.vision"


def create_execution_run(name: str = "C-INT-001 execution validation") -> UUID:
    with db_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            insert into cint001_execution_runs(
                name,status,stage,rule_version,validation_start,validation_end,
                execution_spec,final_holdout_start,final_holdout_end,holdout_opened
            ) values (%s,'queued','archive_backfill',%s,%s,%s,%s,%s,%s,false)
            returning id
            """,
            (
                name,
                RULE_VERSION,
                VALIDATION_START,
                VALIDATION_END,
                Jsonb(EXECUTION_SPEC),
                FINAL_HOLDOUT_START,
                FINAL_HOLDOUT_END,
            ),
        )
        run_id = cur.fetchone()["id"]
        cursor = VALIDATION_START.replace(day=1)
        while cursor < VALIDATION_END:
            next_month = (cursor + timedelta(days=32)).replace(day=1)
            for symbol in UNIVERSE:
                key = f"{symbol}:{cursor:%Y-%m}"
                cur.execute(
                    """
                    insert into cint001_execution_work_items(run_id,stage,partition_key,payload)
                    values (%s,'month',%s,%s) on conflict do nothing
                    """,
                    (
                        run_id,
                        key,
                        Jsonb({"spot_symbol": symbol, "month": cursor.date().isoformat()}),
                    ),
                )
            cursor = next_month
        conn.commit()
    return run_id


def claim_execution_work(worker_id: str) -> dict[str, Any] | None:
    with db_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            with candidate as (
                select w.id
                from cint001_execution_work_items w
                join cint001_execution_runs r on r.id=w.run_id
                where w.status in ('queued','retry_wait') and w.not_before <= now()
                  and r.status in ('queued','running')
                order by case w.stage when 'month' then 1 when 'analysis' then 2 else 50 end,w.id
                for update skip locked limit 1
            )
            update cint001_execution_work_items w
               set status='running',attempts=attempts+1,locked_by=%s,locked_at=now(),updated_at=now()
              from candidate c where w.id=c.id returning w.*
            """,
            (worker_id,),
        )
        row = cur.fetchone()
        if row:
            cur.execute(
                "update cint001_execution_runs set status='running',started_at=coalesce(started_at,now()),updated_at=now() where id=%s",
                (row["run_id"],),
            )
        conn.commit()
        return row


def reclaim_stale_execution_work(minutes: int = 30) -> int:
    with db_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            update cint001_execution_work_items
               set status='retry_wait',locked_by=null,locked_at=null,not_before=now(),updated_at=now(),
                   last_error=coalesce(last_error,'stale execution worker lock reclaimed')
             where status='running' and locked_at < now()-(%s*interval '1 minute')
            """,
            (minutes,),
        )
        count = cur.rowcount
        conn.commit()
        return count


def _complete(
    item_id: int,
    rows: int = 0,
    progress: dict[str, Any] | None = None,
    status: str = "completed",
) -> None:
    with db_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            update cint001_execution_work_items
               set status=%s,row_count=%s,progress=%s,locked_by=null,locked_at=null,updated_at=now()
             where id=%s
            """,
            (status, rows, Jsonb(progress or {}), item_id),
        )
        conn.commit()


def _fail(item: dict[str, Any], exc: BaseException) -> None:
    attempts = int(item.get("attempts") or 1)
    max_attempts = int(item.get("max_attempts") or 8)
    status = "failed" if attempts >= max_attempts else "retry_wait"
    delay = min(900, 15 * (2 ** min(attempts, 6)))
    with db_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            update cint001_execution_work_items
               set status=%s,last_error=%s,locked_by=null,locked_at=null,
                   not_before=now()+(%s*interval '1 second'),updated_at=now()
             where id=%s
            """,
            (status, f"{type(exc).__name__}: {exc}", delay, item["id"]),
        )
        if status == "failed":
            cur.execute(
                "update cint001_execution_runs set error=coalesce(error,%s),updated_at=now() where id=%s",
                (f"Execution work failed {item['partition_key']}: {exc}", item["run_id"]),
            )
        conn.commit()


def _futures_candidates(spot_symbol: str) -> list[str]:
    symbol = spot_symbol.upper()
    candidates = [symbol]
    if symbol.endswith("USDT"):
        base = symbol[:-4]
        if base and not base.startswith("1000"):
            candidates.append(f"1000{base}USDT")
    return candidates


def _download(client: httpx.Client, url: str, path: Path) -> tuple[bool, str | None]:
    digest = hashlib.sha256()
    with client.stream("GET", url) as response:
        if response.status_code == 404:
            return False, None
        response.raise_for_status()
        with path.open("wb") as handle:
            for chunk in response.iter_bytes(chunk_size=1024 * 1024):
                if chunk:
                    digest.update(chunk)
                    handle.write(chunk)
    checksum_url = url + ".CHECKSUM"
    checksum = client.get(checksum_url)
    source = (
        checksum.text.strip().split()[0].lower()
        if checksum.status_code == 200 and checksum.text.strip()
        else None
    )
    computed = digest.hexdigest()
    if source and len(source) == 64 and source != computed:
        raise ValueError(f"Checksum mismatch for {url}")
    return True, computed


def _archive_urls(futures_symbol: str, month: date) -> dict[str, str]:
    ym = f"{month:%Y-%m}"
    return {
        "kline": f"{DATA_BASE}/data/futures/um/monthly/klines/{futures_symbol}/15m/{futures_symbol}-15m-{ym}.zip",
        "mark": f"{DATA_BASE}/data/futures/um/monthly/markPriceKlines/{futures_symbol}/15m/{futures_symbol}-15m-{ym}.zip",
        "funding": f"{DATA_BASE}/data/futures/um/monthly/fundingRate/{futures_symbol}/{futures_symbol}-fundingRate-{ym}.zip",
    }


def _timestamp(value: str) -> datetime:
    raw = int(float(value))
    divisor = 1_000_000 if abs(raw) >= 1_000_000_000_000_000 else 1_000
    return datetime.fromtimestamp(raw / divisor, tz=timezone.utc)


def _csv_rows(path: Path) -> list[list[str]]:
    with zipfile.ZipFile(path) as archive:
        names = [name for name in archive.namelist() if name.lower().endswith(".csv")]
        if not names:
            raise ValueError(f"Archive {path.name} contains no CSV")
        with archive.open(names[0]) as raw, io.TextIOWrapper(
            raw, encoding="utf-8", newline=""
        ) as text:
            return list(csv.reader(text))


def _parse_klines(path: Path) -> list[tuple]:
    output: list[tuple] = []
    for row in _csv_rows(path):
        if len(row) < 9:
            continue
        try:
            ts = _timestamp(row[0])
            open_px, high, low, close = map(float, row[1:5])
            volume = float(row[5]) if len(row) > 5 else 0.0
            quote_volume = float(row[7]) if len(row) > 7 else 0.0
            trades = int(float(row[8])) if len(row) > 8 and row[8] else 0
        except (ValueError, OverflowError):
            continue
        output.append((ts, open_px, high, low, close, volume, quote_volume, trades))
    return output


def _parse_mark_klines(path: Path) -> list[tuple]:
    output: list[tuple] = []
    for row in _csv_rows(path):
        if len(row) < 5:
            continue
        try:
            ts = _timestamp(row[0])
            open_px, high, low, close = map(float, row[1:5])
        except (ValueError, OverflowError):
            continue
        output.append((ts, open_px, high, low, close))
    return output


def _normalise_header(value: str) -> str:
    return value.strip().lower().replace(" ", "_").replace("-", "_")


def _parse_funding(path: Path) -> list[tuple]:
    rows = _csv_rows(path)
    if not rows:
        return []
    header = [_normalise_header(v) for v in rows[0]]
    has_header = not _looks_numeric(rows[0][0])
    data = rows[1:] if has_header else rows
    time_idx = (
        _find_col(header, ("funding_time", "calc_time", "time", "timestamp"))
        if has_header
        else 0
    )
    rate_idx = (
        _find_col(header, ("funding_rate", "last_funding_rate", "rate"))
        if has_header
        else (2 if len(rows[0]) >= 3 else 1)
    )
    mark_idx = (
        _find_col(header, ("mark_price", "markprice"), required=False)
        if has_header
        else None
    )
    output: list[tuple] = []
    for row in data:
        try:
            ts = _timestamp(row[time_idx])
            rate = float(row[rate_idx])
            mark = (
                float(row[mark_idx])
                if mark_idx is not None and mark_idx < len(row) and row[mark_idx]
                else None
            )
        except (ValueError, OverflowError, IndexError):
            continue
        output.append((ts, rate, mark))
    return output


def _looks_numeric(value: str) -> bool:
    try:
        float(value)
        return True
    except (TypeError, ValueError):
        return False


def _find_col(
    header: list[str], names: tuple[str, ...], required: bool = True
) -> int | None:
    for name in names:
        if name in header:
            return header.index(name)
    if required:
        raise ValueError(f"Funding archive header missing expected columns: {header}")
    return None


def _canonical_symbol(spot_symbol: str) -> str:
    return spot_symbol[:-4] if spot_symbol.endswith("USDT") else spot_symbol


def _process_month(item: dict[str, Any]) -> None:
    spot_symbol = str(item["payload"]["spot_symbol"]).upper()
    month = date.fromisoformat(item["payload"]["month"])
    with tempfile.TemporaryDirectory(prefix="cint001-") as temp_dir, httpx.Client(
        timeout=180, follow_redirects=True
    ) as client:
        root = Path(temp_dir)
        chosen: str | None = None
        files: dict[str, Path] = {}
        checksums: dict[str, str | None] = {}
        availability = {"kline": False, "mark": False, "funding": False}
        sources: dict[str, str] = {}
        for candidate in _futures_candidates(spot_symbol):
            urls = _archive_urls(candidate, month)
            kline_path = root / f"{candidate}-kline.zip"
            exists, checksum = _download(client, urls["kline"], kline_path)
            if not exists:
                continue
            chosen = candidate
            availability["kline"] = True
            files["kline"] = kline_path
            checksums["kline"] = checksum
            sources["kline"] = urls["kline"]
            for kind in ("mark", "funding"):
                path = root / f"{candidate}-{kind}.zip"
                present, csum = _download(client, urls[kind], path)
                availability[kind] = present
                if present:
                    files[kind] = path
                    checksums[kind] = csum
                    sources[kind] = urls[kind]
            break

        kline_rows = _parse_klines(files["kline"]) if availability["kline"] else []
        mark_rows = _parse_mark_klines(files["mark"]) if availability["mark"] else []
        funding_rows = _parse_funding(files["funding"]) if availability["funding"] else []
        canonical = _canonical_symbol(spot_symbol)
        with db_connection() as conn, conn.cursor() as cur:
            cur.execute(
                """
                insert into cint001_contract_months(
                    run_id,spot_symbol,futures_symbol,period_start,kline_available,mark_available,funding_available,
                    source_urls,checksums,updated_at
                ) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,now())
                on conflict (run_id,spot_symbol,period_start) do update set
                    futures_symbol=excluded.futures_symbol,kline_available=excluded.kline_available,
                    mark_available=excluded.mark_available,funding_available=excluded.funding_available,
                    source_urls=excluded.source_urls,checksums=excluded.checksums,updated_at=now()
                """,
                (
                    item["run_id"],
                    spot_symbol,
                    chosen,
                    month,
                    availability["kline"],
                    availability["mark"],
                    availability["funding"],
                    Jsonb(sources),
                    Jsonb(checksums),
                ),
            )
            conn.commit()

        # Monthly 15-minute archives are small enough for batched executemany.
        if chosen and kline_rows:
            with db_connection() as conn, conn.cursor() as cur:
                cur.executemany(
                    """
                    insert into crypto_futures_15m_binance(
                        venue_symbol,canonical_symbol,bucket_start,open,high,low,close,volume,quote_volume,trade_count,source
                    ) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'binance_data_vision_monthly_15m')
                    on conflict (venue_symbol,bucket_start) do update set
                        open=excluded.open,high=excluded.high,low=excluded.low,close=excluded.close,
                        volume=excluded.volume,quote_volume=excluded.quote_volume,trade_count=excluded.trade_count,
                        source=excluded.source,updated_at=now()
                    """,
                    [(chosen, canonical, *row) for row in kline_rows],
                )
                conn.commit()
        if chosen and mark_rows:
            with db_connection() as conn, conn.cursor() as cur:
                cur.executemany(
                    """
                    insert into crypto_futures_mark_15m_binance(
                        venue_symbol,canonical_symbol,bucket_start,open,high,low,close,source
                    ) values (%s,%s,%s,%s,%s,%s,%s,'binance_data_vision_monthly_mark_15m')
                    on conflict (venue_symbol,bucket_start) do update set
                        open=excluded.open,high=excluded.high,low=excluded.low,close=excluded.close,
                        source=excluded.source,updated_at=now()
                    """,
                    [(chosen, canonical, *row) for row in mark_rows],
                )
                cur.executemany(
                    """
                    insert into crypto_derivatives_metrics(
                        provider,venue_symbol,canonical_symbol,ts,interval,mark_price,metadata
                    ) values ('binance_futures',%s,%s,%s,'15m',%s,%s)
                    on conflict (provider,venue_symbol,ts,interval) do update set
                        mark_price=excluded.mark_price,
                        metadata=crypto_derivatives_metrics.metadata || excluded.metadata
                    """,
                    [
                        (
                            chosen,
                            canonical,
                            row[0],
                            row[4],
                            Jsonb(
                                {
                                    "source": "binance_data_vision",
                                    "mark_open": row[1],
                                    "mark_high": row[2],
                                    "mark_low": row[3],
                                }
                            ),
                        )
                        for row in mark_rows
                    ],
                )
                conn.commit()
        if chosen and funding_rows:
            with db_connection() as conn, conn.cursor() as cur:
                cur.executemany(
                    """
                    insert into crypto_futures_funding_binance(
                        venue_symbol,canonical_symbol,funding_ts,funding_rate,mark_price,source
                    ) values (%s,%s,%s,%s,%s,'binance_data_vision_monthly_funding')
                    on conflict (venue_symbol,funding_ts) do update set
                        funding_rate=excluded.funding_rate,
                        mark_price=coalesce(excluded.mark_price,crypto_futures_funding_binance.mark_price),
                        source=excluded.source,updated_at=now()
                    """,
                    [(chosen, canonical, *row) for row in funding_rows],
                )
                cur.executemany(
                    """
                    insert into crypto_derivatives_metrics(
                        provider,venue_symbol,canonical_symbol,ts,interval,mark_price,funding_rate,metadata
                    ) values ('binance_futures',%s,%s,%s,'funding',%s,%s,%s)
                    on conflict (provider,venue_symbol,ts,interval) do update set
                        mark_price=coalesce(excluded.mark_price,crypto_derivatives_metrics.mark_price),
                        funding_rate=excluded.funding_rate,
                        metadata=crypto_derivatives_metrics.metadata || excluded.metadata
                    """,
                    [
                        (
                            chosen,
                            canonical,
                            row[0],
                            row[2],
                            row[1],
                            Jsonb({"source": "binance_data_vision"}),
                        )
                        for row in funding_rows
                    ],
                )
                conn.commit()

    _complete(
        item["id"],
        len(kline_rows) + len(mark_rows) + len(funding_rows),
        {
            "spot_symbol": spot_symbol,
            "futures_symbol": chosen,
            "availability": availability,
            "kline_rows": len(kline_rows),
            "mark_rows": len(mark_rows),
            "funding_rows": len(funding_rows),
        },
        status="completed" if availability["kline"] else "missing",
    )


def _queue_analysis_if_ready(run_id: UUID) -> None:
    row = fetch_one(
        """
        select count(*) filter(where stage='month' and status in ('queued','retry_wait','running')) active,
               count(*) filter(where stage='month' and status='failed') failed,
               count(*) filter(where stage='analysis') analysis_items
        from cint001_execution_work_items where run_id=%s
        """,
        (run_id,),
    ) or {}
    if int(row.get("active") or 0) or int(row.get("analysis_items") or 0):
        return
    if int(row.get("failed") or 0):
        with db_connection() as conn, conn.cursor() as cur:
            cur.execute(
                """
                update cint001_execution_runs
                   set status='completed_with_errors',stage='archive_backfill_failed',
                       completed_at=now(),updated_at=now()
                 where id=%s
                """,
                (run_id,),
            )
            conn.commit()
        return
    with db_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            insert into cint001_execution_work_items(run_id,stage,partition_key,payload)
            values (%s,'analysis','validation',%s) on conflict do nothing
            """,
            (run_id, Jsonb({})),
        )
        cur.execute(
            "update cint001_execution_runs set stage='execution_analysis',updated_at=now() where id=%s",
            (run_id,),
        )
        conn.commit()


def _process_analysis(item: dict[str, Any]) -> None:
    run_id = UUID(str(item["run_id"]))
    universe = list(UNIVERSE)
    entry_offset = ENTRY_OFFSET_MINUTES
    exit_offset = ENTRY_OFFSET_MINUTES + HOLD_MINUTES
    with db_connection() as conn, conn.cursor() as cur:
        cur.execute("delete from cint001_execution_trades where run_id=%s", (run_id,))
        cur.execute(
            """
            insert into cint001_execution_trades(
                run_id,signal_bucket,signal_ts,entry_ts,exit_ts,phase,spot_symbol,futures_symbol,
                r1h,range15,q_r1h,q_range,selected_count,executable_count,panel_member_count,
                futures_short_return,funding_return,panel_long_return,gross_relative_return,
                spot_entry,futures_entry,futures_exit,entry_basis_bps,exit_basis_bps
            )
            with raw as (
                select symbol,bucket_start,signal_ts,open,high,low,close,
                       close/nullif(lag(close,%s) over(partition by symbol order by bucket_start),0)-1 r1h,
                       high/nullif(low,0)-1 range15
                from crypto_research_15m_binance
                where symbol=any(%s) and bucket_start >= %s-interval '2 hours' and bucket_start < %s
            ), ranked as (
                select *,
                       ntile(%s) over(partition by bucket_start order by r1h) q_r1h,
                       ntile(%s) over(partition by bucket_start order by range15) q_range
                from raw
                where r1h is not null and bucket_start >= %s and bucket_start < %s
            ), selected as (
                select * from ranked where q_r1h=%s and q_range=%s
            ), counts as (
                select bucket_start,count(*) selected_count from selected group by 1
            ), panel as (
                select s.bucket_start,
                       avg(x.open/nullif(e.open,0)-1) panel_long_return,
                       count(*) panel_members
                from (select distinct bucket_start from selected) s
                join crypto_research_15m_binance e
                  on e.symbol=any(%s)
                 and e.bucket_start=s.bucket_start+(%s*interval '1 minute')
                join crypto_research_15m_binance x
                  on x.symbol=e.symbol
                 and x.bucket_start=s.bucket_start+(%s*interval '1 minute')
                group by 1
            ), mapped as (
                select s.*,c.selected_count,cm.futures_symbol,
                       s.bucket_start+(%s*interval '1 minute') entry_ts,
                       s.bucket_start+(%s*interval '1 minute') exit_ts
                from selected s
                join counts c using(bucket_start)
                left join cint001_contract_months cm
                  on cm.run_id=%s
                 and cm.spot_symbol=s.symbol
                 and cm.period_start=date_trunc('month',s.bucket_start+(%s*interval '1 minute'))::date
                 and cm.kline_available and cm.funding_available
            ), outcomes as (
                select m.*,p.panel_long_return,p.panel_members,
                       fe.open futures_entry,fx.open futures_exit,se.open spot_entry,
                       1-fx.open/nullif(fe.open,0) futures_short_return,
                       coalesce((
                           select sum(f.funding_rate*coalesce(f.mark_price,fe.open)/nullif(fe.open,0))
                           from crypto_futures_funding_binance f
                           where f.venue_symbol=m.futures_symbol
                             and f.funding_ts>m.entry_ts and f.funding_ts<=m.exit_ts
                       ),0) funding_return
                from mapped m
                join panel p using(bucket_start)
                left join crypto_futures_15m_binance fe
                  on fe.venue_symbol=m.futures_symbol and fe.bucket_start=m.entry_ts
                left join crypto_futures_15m_binance fx
                  on fx.venue_symbol=m.futures_symbol and fx.bucket_start=m.exit_ts
                left join crypto_research_15m_binance se
                  on se.symbol=m.symbol and se.bucket_start=m.entry_ts
            ), exec_counts as (
                select bucket_start,
                       count(*) filter(where futures_entry is not null and futures_exit is not null) executable_count
                from outcomes group by 1
            )
            select %s,o.bucket_start,o.signal_ts,o.entry_ts,o.exit_ts,
                   (extract(hour from o.signal_ts)::int*4+
                    floor(extract(minute from o.signal_ts)/15)::int)::smallint,
                   o.symbol,o.futures_symbol,o.r1h,o.range15,o.q_r1h,o.q_range,
                   o.selected_count,ec.executable_count,o.panel_members,
                   o.futures_short_return,o.funding_return,o.panel_long_return,
                   case when o.futures_short_return is not null
                        then o.futures_short_return+o.funding_return+o.panel_long_return end,
                   o.spot_entry,o.futures_entry,o.futures_exit,
                   case when o.futures_entry is not null and o.spot_entry is not null
                        then (o.futures_entry/o.spot_entry-1)*10000 end,
                   null
            from outcomes o join exec_counts ec using(bucket_start)
            """,
            (
                RETURN_LOOKBACK_BARS,
                universe,
                VALIDATION_START,
                VALIDATION_SIGNAL_END,
                CROSS_SECTIONAL_BUCKETS,
                CROSS_SECTIONAL_BUCKETS,
                VALIDATION_START,
                VALIDATION_SIGNAL_END,
                SELECT_BUCKET,
                SELECT_BUCKET,
                universe,
                entry_offset,
                exit_offset,
                entry_offset,
                exit_offset,
                run_id,
                entry_offset,
                run_id,
            ),
        )
        conn.commit()

    with db_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            update cint001_execution_trades t
               set exit_basis_bps=(t.futures_exit/s.open-1)*10000
              from crypto_research_15m_binance s
             where t.run_id=%s and s.symbol=t.spot_symbol and s.bucket_start=t.exit_ts
               and t.futures_exit is not null and s.open is not null
            """,
            (run_id,),
        )
        conn.commit()

    metrics = _execution_metrics(run_id)
    with db_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            insert into cint001_execution_results(run_id,result_scope,metrics)
            values (%s,'validation_60m_delay_24h',%s)
            on conflict (run_id,result_scope)
            do update set metrics=excluded.metrics,created_at=now()
            """,
            (run_id, Jsonb(metrics)),
        )
        cur.execute(
            """
            update cint001_execution_runs
               set status='completed',stage='completed',completed_at=now(),updated_at=now(),
                   result_summary=%s
             where id=%s
            """,
            (Jsonb(metrics), run_id),
        )
        conn.commit()
    _complete(item["id"], int(metrics.get("selected_asset_observations") or 0), metrics)


def _execution_metrics(run_id: UUID) -> dict[str, Any]:
    coverage = fetch_one(
        """
        select count(*) selected_asset_observations,
               count(*) filter(where futures_short_return is not null) executable_asset_observations,
               count(distinct spot_symbol) selected_symbols,
               count(distinct spot_symbol) filter(where futures_short_return is not null) executable_symbols,
               count(distinct signal_bucket) signal_timestamps,
               count(distinct signal_bucket)
                   filter(where executable_count=selected_count and panel_member_count=30) strict_timestamps
        from cint001_execution_trades where run_id=%s
        """,
        (run_id,),
    ) or {}
    phase = fetch_one(
        """
        with timestamp_returns as (
            select signal_bucket,phase,avg(gross_relative_return) gross_relative,
                   bool_and(executable_count=selected_count and panel_member_count=30) strict
            from cint001_execution_trades
            where run_id=%s and gross_relative_return is not null
            group by signal_bucket,phase
        ), strict as (
            select * from timestamp_returns where strict
        ), phase_stats as (
            select phase,count(*) days,avg(gross_relative) mean_ret,
                   percentile_cont(.5) within group(order by gross_relative) median_ret,
                   avg((gross_relative>0)::int) hit
            from strict group by phase
        )
        select count(*) phases,avg(days) avg_days,avg(mean_ret) avg_phase_mean,
               percentile_cont(.5) within group(order by mean_ret) median_phase_mean,
               min(mean_ret) worst_phase,max(mean_ret) best_phase,
               avg((mean_ret>0)::int) positive_phase_fraction,
               avg((median_ret>0)::int) positive_median_fraction,avg(hit) avg_hit
        from phase_stats
        """,
        (run_id,),
    ) or {}
    daily = fetch_one(
        """
        with timestamp_returns as (
            select signal_bucket,avg(gross_relative_return) gross_relative,
                   bool_and(executable_count=selected_count and panel_member_count=30) strict
            from cint001_execution_trades
            where run_id=%s and gross_relative_return is not null
            group by signal_bucket
        ), d as (
            select signal_bucket::date d,avg(gross_relative) ret
            from timestamp_returns where strict group by 1
        )
        select count(*) days,avg(ret) mean_daily,
               percentile_cont(.5) within group(order by ret) median_daily,
               stddev_samp(ret) sd_daily,avg((ret>0)::int) positive_day_fraction,
               min(ret) worst_day,max(ret) best_day
        from d
        """,
        (run_id,),
    ) or {}
    economics = fetch_one(
        """
        select avg(futures_short_return) avg_short_price_return,
               avg(funding_return) avg_short_funding_return,
               avg(panel_long_return) avg_panel_long_return,
               avg(gross_relative_return) avg_asset_level_gross_relative,
               percentile_cont(.5) within group(order by entry_basis_bps) median_entry_basis_bps,
               percentile_cont(.95) within group(order by abs(entry_basis_bps)) p95_abs_entry_basis_bps,
               percentile_cont(.5) within group(order by exit_basis_bps) median_exit_basis_bps
        from cint001_execution_trades
        where run_id=%s and gross_relative_return is not null
        """,
        (run_id,),
    ) or {}
    selected = int(coverage.get("selected_asset_observations") or 0)
    executable = int(coverage.get("executable_asset_observations") or 0)
    metrics = {**coverage, **phase, **daily, **economics}
    metrics["asset_execution_coverage"] = executable / selected if selected else None
    sd = daily.get("sd_daily")
    n = int(daily.get("days") or 0)
    mean = daily.get("mean_daily")
    metrics["daily_naive_t"] = (
        float(mean) / (float(sd) / math.sqrt(n))
        if mean is not None and sd not in (None, 0) and n > 1
        else None
    )
    metrics["fees_spread_slippage_status"] = (
        "NOT_YET_INCLUDED: requires historical book ticker / fee tier; holdout remains sealed"
    )
    metrics["holdout_opened"] = False
    return metrics


def advance_execution_run(run_id: UUID) -> None:
    _queue_analysis_if_ready(run_id)


def process_execution_work(item: dict[str, Any]) -> None:
    try:
        if item["stage"] == "month":
            _process_month(item)
        elif item["stage"] == "analysis":
            _process_analysis(item)
        else:
            raise ValueError(f"Unknown C-INT-001 execution stage: {item['stage']}")
        advance_execution_run(UUID(str(item["run_id"])))
    except Exception as exc:
        logger.exception(
            "C-INT-001 execution work failed stage=%s key=%s",
            item.get("stage"),
            item.get("partition_key"),
        )
        _fail(item, exc)
