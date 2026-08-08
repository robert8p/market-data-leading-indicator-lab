from __future__ import annotations

import csv
import hashlib
import io
import logging
import re
import socket
import tempfile
import xml.etree.ElementTree as ET
import zipfile
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable
from uuid import UUID

import httpx
from dateutil.relativedelta import relativedelta
from psycopg.types.json import Jsonb

from app.b001_contract import (
    CLOSE_VS_VWAP_MAX,
    COST_SPEC,
    DISCOVERY_START,
    DISPERSION_MAX,
    EXACT_THRESHOLDS,
    EXECUTION_SPEC,
    EXTREME_PERCENTILE,
    FINAL_5M_MAX,
    HIGH_TO_CLOSE_MIN,
    LIQUIDITY_LOOKBACK_DAYS,
    REPLICATION_END,
    RULE_VERSION,
)
from app.db import db_connection, fetch_all, fetch_one
from app.storage import SupabaseStorage

logger = logging.getLogger(__name__)
S3_INDEX = "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision"
DATA_BASE = "https://data.binance.vision"
LEVERAGED_TOKEN_RE = re.compile(r"(?:UP|DOWN|BULL|BEAR)USDT$")
MONTH_FILE_RE = re.compile(r"-(\d{4})-(\d{2})\.zip$")


def _month_floor(value: datetime) -> datetime:
    return value.astimezone(timezone.utc).replace(day=1, hour=0, minute=0, second=0, microsecond=0)


def _months(start: datetime, end: datetime) -> Iterable[tuple[datetime, datetime]]:
    cursor = _month_floor(start)
    while cursor < end:
        nxt = cursor + relativedelta(months=1)
        yield cursor, nxt
        cursor = nxt


def create_b001_run(name: str = "B-001 locked historical replication", target_months: int = 24) -> UUID:
    if target_months < 12 or target_months > 60:
        raise ValueError("B-001 historical replication requires 12 to 60 months")
    requested_end = REPLICATION_END
    requested_start = requested_end - relativedelta(months=target_months)
    with db_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            insert into crypto_b001_replication_runs(
                name,status,stage,rule_version,requested_start,requested_end,target_months,minimum_months,
                exact_thresholds,execution_spec,cost_spec
            ) values (%s,'queued','archive_discovery',%s,%s,%s,%s,12,%s,%s,%s)
            returning id
            """,
            (
                name.strip() or "B-001 locked historical replication",
                RULE_VERSION,
                requested_start,
                requested_end,
                target_months,
                Jsonb(EXACT_THRESHOLDS),
                Jsonb(EXECUTION_SPEC),
                Jsonb(COST_SPEC),
            ),
        )
        run_id = cur.fetchone()["id"]
        cur.execute(
            """
            insert into crypto_b001_replication_work_items(run_id,stage,partition_key,payload)
            values (%s,'discover_archives','root',%s)
            on conflict do nothing
            """,
            (run_id, Jsonb({})),
        )
        conn.commit()
    return run_id


def claim_b001_work(worker_id: str) -> dict[str, Any] | None:
    with db_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            with candidate as (
                select w.id
                from crypto_b001_replication_work_items w
                join crypto_b001_replication_runs r on r.id=w.run_id
                where w.status in ('queued','retry_wait') and w.not_before<=now()
                  and r.status in ('queued','running')
                order by case w.stage
                    when 'discover_archives' then 1 when 'discover_symbol' then 2 when 'spot_month' then 3
                    when 'derive_features' then 4 when 'market_state' then 5 when 'signals' then 6
                    when 'shortability' then 7 when 'analysis' then 8 else 50 end,
                    w.id
                for update skip locked
                limit 1
            )
            update crypto_b001_replication_work_items w
               set status='running',attempts=attempts+1,locked_by=%s,locked_at=now(),updated_at=now()
              from candidate c
             where w.id=c.id
            returning w.*
            """,
            (worker_id,),
        )
        row = cur.fetchone()
        if row:
            cur.execute(
                "update crypto_b001_replication_runs set status='running',started_at=coalesce(started_at,now()),updated_at=now() where id=%s",
                (row["run_id"],),
            )
        conn.commit()
        return row


def reclaim_stale_b001_work(minutes: int = 45) -> int:
    with db_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            update crypto_b001_replication_work_items
               set status='retry_wait',locked_by=null,locked_at=null,not_before=now(),
                   last_error=coalesce(last_error,'stale worker lock reclaimed'),updated_at=now()
             where status='running' and locked_at < now() - (%s * interval '1 minute')
            """,
            (minutes,),
        )
        count = cur.rowcount
        conn.commit()
        return count


def _complete(item_id: int, row_count: int = 0, progress: dict | None = None, status: str = "completed") -> None:
    with db_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            update crypto_b001_replication_work_items
               set status=%s,row_count=%s,progress=%s,locked_by=null,locked_at=null,updated_at=now()
             where id=%s
            """,
            (status, row_count, Jsonb(progress or {}), item_id),
        )
        conn.commit()


def _fail(item: dict[str, Any], exc: Exception, code: str = "replication_error") -> None:
    attempts = int(item.get("attempts") or 1)
    max_attempts = int(item.get("max_attempts") or 8)
    status = "failed" if attempts >= max_attempts else "retry_wait"
    delay = min(900, 15 * (2 ** min(attempts, 6)))
    with db_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            update crypto_b001_replication_work_items
               set status=%s,last_error=%s,error_code=%s,locked_by=null,locked_at=null,
                   not_before=now()+(%s * interval '1 second'),updated_at=now()
             where id=%s
            """,
            (status, f"{type(exc).__name__}: {exc}", code, delay, item["id"]),
        )
        if status == "failed":
            cur.execute(
                "update crypto_b001_replication_runs set status='completed_with_errors',error=coalesce(error,%s),updated_at=now() where id=%s",
                (f"Work item failed: {item['stage']} {item['partition_key']}: {exc}", item["run_id"]),
            )
        conn.commit()


def _s3_list(prefix: str, delimiter: str | None = None) -> tuple[list[str], list[str]]:
    keys: list[str] = []
    prefixes: list[str] = []
    token: str | None = None
    with httpx.Client(timeout=60, follow_redirects=True) as client:
        while True:
            params: dict[str, str] = {"list-type": "2", "prefix": prefix, "max-keys": "1000"}
            if delimiter:
                params["delimiter"] = delimiter
            if token:
                params["continuation-token"] = token
            response = client.get(S3_INDEX, params=params)
            response.raise_for_status()
            root = ET.fromstring(response.content)
            for elem in root.iter():
                tag = elem.tag.rsplit("}", 1)[-1]
                if tag == "Key" and elem.text:
                    keys.append(elem.text)
                elif tag == "Prefix" and elem.text and elem.text != prefix:
                    prefixes.append(elem.text)
            truncated = next((e.text for e in root.iter() if e.tag.rsplit("}", 1)[-1] == "IsTruncated"), "false")
            if str(truncated).lower() != "true":
                break
            token = next((e.text for e in root.iter() if e.tag.rsplit("}", 1)[-1] == "NextContinuationToken"), None)
            if not token:
                break
    return keys, prefixes


def _discover_archives(item: dict[str, Any]) -> None:
    _keys, prefixes = _s3_list("data/spot/monthly/klines/", delimiter="/")
    symbols = sorted({prefix.rstrip("/").split("/")[-1].upper() for prefix in prefixes})
    symbols = [symbol for symbol in symbols if symbol.endswith("USDT") and not LEVERAGED_TOKEN_RE.search(symbol)]
    with db_connection() as conn, conn.cursor() as cur:
        for symbol in symbols:
            cur.execute(
                """
                insert into crypto_b001_replication_work_items(run_id,stage,partition_key,payload)
                values (%s,'discover_symbol',%s,%s) on conflict do nothing
                """,
                (item["run_id"], symbol, Jsonb({"symbol": symbol})),
            )
        conn.commit()
    _complete(item["id"], len(symbols), {"historical_usdt_symbol_prefixes": len(symbols)})


def _discover_symbol(item: dict[str, Any]) -> None:
    symbol = str(item["payload"]["symbol"]).upper()
    run = fetch_one("select * from crypto_b001_replication_runs where id=%s", (item["run_id"],))
    if not run:
        raise RuntimeError("Replication run disappeared")
    collection_start = run["requested_start"] - timedelta(days=LIQUIDITY_LOOKBACK_DAYS + 1)
    keys, _prefixes = _s3_list(f"data/spot/monthly/klines/{symbol}/1m/")
    planned = 0
    with db_connection() as conn, conn.cursor() as cur:
        for key in keys:
            if not key.endswith(".zip") or key.endswith(".zip.CHECKSUM"):
                continue
            match = MONTH_FILE_RE.search(key)
            if not match:
                continue
            period_start = datetime(int(match.group(1)), int(match.group(2)), 1, tzinfo=timezone.utc)
            period_end = period_start + relativedelta(months=1)
            if period_end <= collection_start or period_start >= run["requested_end"]:
                continue
            source_url = f"{DATA_BASE}/{key}"
            payload = {
                "symbol": symbol,
                "period_start": period_start.date().isoformat(),
                "period_end": period_end.date().isoformat(),
                "source_url": source_url,
                "checksum_url": source_url + ".CHECKSUM",
            }
            cur.execute(
                """
                insert into crypto_b001_replication_archive_files(
                    run_id,symbol,period_start,period_end,source_url,checksum_url
                ) values (%s,%s,%s,%s,%s,%s)
                on conflict (run_id,symbol,market_type,interval,period_start) do nothing
                """,
                (item["run_id"], symbol, period_start.date(), period_end.date(), source_url, source_url + ".CHECKSUM"),
            )
            cur.execute(
                """
                insert into crypto_b001_replication_work_items(run_id,stage,partition_key,payload)
                values (%s,'spot_month',%s,%s) on conflict do nothing
                """,
                (item["run_id"], f"{symbol}:{period_start:%Y-%m}", Jsonb(payload)),
            )
            planned += 1
        conn.commit()
    _complete(item["id"], planned, {"monthly_archives": planned})


def _timestamp_from_binance(value: str) -> datetime:
    raw = int(value)
    divisor = 1_000_000 if abs(raw) >= 1_000_000_000_000_000 else 1_000
    return datetime.fromtimestamp(raw / divisor, tz=timezone.utc)


def _parse_checksum(text: str) -> str | None:
    token = text.strip().split()[0] if text.strip() else ""
    return token.lower() if re.fullmatch(r"[0-9a-fA-F]{64}", token) else None


def _aggregate_archive(path: Path, collection_start: datetime, requested_end: datetime) -> tuple[list[tuple], dict[str, Any]]:
    minutes: dict[datetime, tuple] = {}
    total_rows = 0
    rows_window = 0
    first_ts: datetime | None = None
    last_ts: datetime | None = None
    with zipfile.ZipFile(path) as archive:
        members = [name for name in archive.namelist() if name.lower().endswith(".csv")]
        if not members:
            raise ValueError("Binance archive contains no CSV")
        with archive.open(members[0]) as raw, io.TextIOWrapper(raw, encoding="utf-8", newline="") as text:
            reader = csv.reader(text)
            for row in reader:
                if len(row) < 11:
                    continue
                try:
                    ts = _timestamp_from_binance(row[0])
                    values = (
                        float(row[1]), float(row[2]), float(row[3]), float(row[4]), float(row[5]),
                        float(row[7]), int(float(row[8])), float(row[10]),
                    )
                except (ValueError, OverflowError):
                    continue
                total_rows += 1
                first_ts = min(first_ts, ts) if first_ts else ts
                last_ts = max(last_ts, ts) if last_ts else ts
                if not (collection_start <= ts < requested_end):
                    continue
                rows_window += 1
                minutes[ts] = values
    buckets: dict[datetime, list[tuple[datetime, tuple]]] = {}
    for ts, values in minutes.items():
        bucket = ts.replace(minute=(ts.minute // 15) * 15, second=0, microsecond=0)
        buckets.setdefault(bucket, []).append((ts, values))
    complete_rows: list[tuple] = []
    incomplete = 0
    missing_minutes = 0
    for bucket, observations in sorted(buckets.items()):
        by_ts = {ts: values for ts, values in observations}
        expected = [bucket + timedelta(minutes=i) for i in range(15)]
        missing = [ts for ts in expected if ts not in by_ts]
        if missing:
            incomplete += 1
            missing_minutes += len(missing)
            continue
        ordered = [(ts, by_ts[ts]) for ts in expected]
        opens = [row[1][0] for row in ordered]
        highs = [row[1][1] for row in ordered]
        lows = [row[1][2] for row in ordered]
        closes = [row[1][3] for row in ordered]
        volumes = [row[1][4] for row in ordered]
        qvs = [row[1][5] for row in ordered]
        trades = [row[1][6] for row in ordered]
        taker_qvs = [row[1][7] for row in ordered]
        high = max(highs)
        close = closes[-1]
        base_volume = sum(volumes)
        quote_volume = sum(qvs)
        # Five elapsed minutes ending at the final minute close: C[t] / C[t-5] - 1.
        final_5m_return = close / closes[-6] - 1.0 if closes[-6] else None
        intrabar_vwap = quote_volume / base_volume if base_volume else None
        close_vs_vwap = close / intrabar_vwap - 1.0 if intrabar_vwap else None
        high_to_close = (high - close) / high if high else None
        complete_rows.append((
            bucket, bucket + timedelta(minutes=15), 15, opens[0], high, min(lows), close,
            base_volume, quote_volume, sum(trades), sum(taker_qvs), final_5m_return,
            intrabar_vwap, close_vs_vwap, high_to_close, expected[0], expected[-1],
        ))
    return complete_rows, {
        "total_rows": total_rows,
        "rows_in_window": rows_window,
        "first_ts": first_ts,
        "last_ts": last_ts,
        "complete_15m": len(complete_rows),
        "incomplete_15m": incomplete,
        "missing_minutes": missing_minutes,
    }


def _process_spot_month(item: dict[str, Any]) -> None:
    payload = item["payload"]
    symbol = payload["symbol"]
    period_start = date.fromisoformat(payload["period_start"])
    run = fetch_one("select * from crypto_b001_replication_runs where id=%s", (item["run_id"],))
    if not run:
        raise RuntimeError("Replication run disappeared")
    collection_start = run["requested_start"] - timedelta(days=LIQUIDITY_LOOKBACK_DAYS + 1)
    source_url = payload["source_url"]
    checksum_url = payload["checksum_url"]
    with tempfile.TemporaryDirectory(prefix="b001-") as temp_dir:
        path = Path(temp_dir) / source_url.rsplit("/", 1)[-1]
        with httpx.Client(timeout=180, follow_redirects=True) as client:
            response = client.get(source_url)
            if response.status_code == 404:
                with db_connection() as conn, conn.cursor() as cur:
                    cur.execute(
                        "update crypto_b001_replication_archive_files set source_status='missing',updated_at=now() where run_id=%s and symbol=%s and period_start=%s",
                        (item["run_id"], symbol, period_start),
                    )
                    conn.commit()
                _complete(item["id"], 0, {"http_status": 404}, status="missing")
                return
            response.raise_for_status()
            path.write_bytes(response.content)
            checksum_response = client.get(checksum_url)
        computed = hashlib.sha256(path.read_bytes()).hexdigest()
        source_checksum = _parse_checksum(checksum_response.text) if checksum_response.status_code == 200 else None
        verified = source_checksum == computed if source_checksum else None
        if verified is False:
            raise ValueError(f"Checksum mismatch for {source_url}")
        rows, stats = _aggregate_archive(path, collection_start, run["requested_end"])
        object_path = f"b001/{item['run_id']}/binance/spot/monthly/klines/{symbol}/1m/{path.name}"
        size, storage_checksum = SupabaseStorage().upload_file(path, object_path, "application/zip")
        if storage_checksum != computed:
            raise ValueError("Storage upload checksum differs from downloaded archive")
        with db_connection() as conn, conn.cursor() as cur:
            cur.execute(
                """
                create temporary table b001_stage_15m(
                    bucket_start timestamptz,signal_ts timestamptz,minute_count integer,open double precision,
                    high double precision,low double precision,close double precision,volume double precision,
                    quote_volume double precision,trade_count bigint,taker_buy_quote_volume double precision,
                    final_5m_return double precision,intrabar_vwap double precision,close_vs_vwap double precision,
                    high_to_close_rejection double precision,first_minute_ts timestamptz,last_minute_ts timestamptz
                ) on commit drop
                """
            )
            if rows:
                with cur.copy("copy b001_stage_15m from stdin") as copy:
                    for row in rows:
                        copy.write_row(row)
                cur.execute(
                    """
                    insert into crypto_b001_replication_15m(
                        run_id,symbol,bucket_start,signal_ts,minute_count,open,high,low,close,volume,quote_volume,
                        trade_count,taker_buy_quote_volume,final_5m_return,intrabar_vwap,close_vs_vwap,
                        high_to_close_rejection,first_minute_ts,last_minute_ts,source_period_start
                    )
                    select %s,%s,bucket_start,signal_ts,minute_count,open,high,low,close,volume,quote_volume,
                           trade_count,taker_buy_quote_volume,final_5m_return,intrabar_vwap,close_vs_vwap,
                           high_to_close_rejection,first_minute_ts,last_minute_ts,%s
                    from b001_stage_15m
                    on conflict (run_id,symbol,bucket_start) do update set
                        signal_ts=excluded.signal_ts,minute_count=excluded.minute_count,open=excluded.open,high=excluded.high,
                        low=excluded.low,close=excluded.close,volume=excluded.volume,quote_volume=excluded.quote_volume,
                        trade_count=excluded.trade_count,taker_buy_quote_volume=excluded.taker_buy_quote_volume,
                        final_5m_return=excluded.final_5m_return,intrabar_vwap=excluded.intrabar_vwap,
                        close_vs_vwap=excluded.close_vs_vwap,high_to_close_rejection=excluded.high_to_close_rejection,
                        first_minute_ts=excluded.first_minute_ts,last_minute_ts=excluded.last_minute_ts,
                        source_period_start=excluded.source_period_start
                    """,
                    (item["run_id"], symbol, period_start),
                )
            cur.execute(
                """
                update crypto_b001_replication_archive_files set
                    source_checksum=%s,computed_checksum=%s,checksum_verified=%s,storage_object_path=%s,
                    storage_size_bytes=%s,source_status='loaded',row_count=%s,rows_in_replication_window=%s,
                    first_ts=%s,last_ts=%s,complete_15m_count=%s,incomplete_15m_count=%s,missing_minute_count=%s,
                    updated_at=now()
                where run_id=%s and symbol=%s and period_start=%s
                """,
                (
                    source_checksum,computed,verified,object_path,size,stats["total_rows"],stats["rows_in_window"],
                    stats["first_ts"],stats["last_ts"],stats["complete_15m"],stats["incomplete_15m"],
                    stats["missing_minutes"],item["run_id"],symbol,period_start,
                ),
            )
            conn.commit()
    _complete(item["id"], stats["rows_in_window"], stats)


def _derive_features(item: dict[str, Any]) -> None:
    symbol = item["payload"]["symbol"]
    with db_connection() as conn, conn.cursor() as cur:
        cur.execute("delete from crypto_b001_replication_features where run_id=%s and symbol=%s", (item["run_id"], symbol))
        cur.execute(
            """
            insert into crypto_b001_replication_features(
                run_id,symbol,bucket_start,signal_ts,open,high,low,close,quote_volume,trade_count,taker_buy_quote_volume,
                ret15,ret30,ret60,ret240,qv_accel1,qv_ratio4,qv_ratio16,trade_accel1,trade_ratio4,trade_ratio16,
                buy_share15,range15,pos_vs_high4h,pos_vs_low4h,pos_vs_high1h,pos_vs_low1h,body_efficiency,
                upper_wick_share,lower_wick_share,ret_accel15,rv1h,rv4h,trailing_liquidity_avg_qv,
                final_5m_return,intrabar_vwap,close_vs_vwap,high_to_close_rejection
            )
            with base as (
                select b.*,
                    lag(close,1) over w as c1,lag(close,2) over w as c2,lag(close,4) over w as c4,lag(close,16) over w as c16,
                    lag(quote_volume,1) over w as qv1,lag(trade_count,1) over w as tc1,
                    avg(quote_volume) over (partition by run_id,symbol order by bucket_start rows between 3 preceding and current row) qv4,
                    avg(quote_volume) over (partition by run_id,symbol order by bucket_start rows between 15 preceding and current row) qv16,
                    avg(trade_count::double precision) over (partition by run_id,symbol order by bucket_start rows between 3 preceding and current row) tc4,
                    avg(trade_count::double precision) over (partition by run_id,symbol order by bucket_start rows between 15 preceding and current row) tc16,
                    max(high) over (partition by run_id,symbol order by bucket_start rows between 15 preceding and current row) h16,
                    min(low) over (partition by run_id,symbol order by bucket_start rows between 15 preceding and current row) l16,
                    max(high) over (partition by run_id,symbol order by bucket_start rows between 3 preceding and current row) h4,
                    min(low) over (partition by run_id,symbol order by bucket_start rows between 3 preceding and current row) l4,
                    avg(quote_volume) over (
                        partition by run_id,symbol order by bucket_start
                        range between interval '18 days' preceding and interval '15 minutes' preceding
                    ) trailing_qv
                from crypto_b001_replication_15m b
                where run_id=%s and symbol=%s
                window w as (partition by run_id,symbol order by bucket_start)
            ), returns as (
                select base.*,
                    close/nullif(c1,0)-1 ret15_x,close/nullif(c2,0)-1 ret30_x,
                    close/nullif(c4,0)-1 ret60_x,close/nullif(c16,0)-1 ret240_x
                from base
            ), final as (
                select returns.*,
                    lag(ret15_x,1) over (partition by run_id,symbol order by bucket_start) prior_ret15,
                    stddev_samp(ret15_x) over (partition by run_id,symbol order by bucket_start rows between 3 preceding and current row) rv1h_x,
                    stddev_samp(ret15_x) over (partition by run_id,symbol order by bucket_start rows between 15 preceding and current row) rv4h_x
                from returns
            )
            select run_id,symbol,bucket_start,signal_ts,open,high,low,close,quote_volume,trade_count,taker_buy_quote_volume,
                ret15_x,ret30_x,ret60_x,ret240_x,
                quote_volume/nullif(qv1,0),quote_volume/nullif(qv4,0),quote_volume/nullif(qv16,0),
                trade_count/nullif(tc1,0),trade_count/nullif(tc4,0),trade_count/nullif(tc16,0),
                taker_buy_quote_volume/nullif(quote_volume,0),high/nullif(low,0)-1,
                close/nullif(h16,0),close/nullif(l16,0)-1,close/nullif(h4,0),close/nullif(l4,0)-1,
                abs(close-open)/nullif(high-low,0),(high-greatest(open,close))/nullif(high-low,0),
                (least(open,close)-low)/nullif(high-low,0),ret15_x-prior_ret15,rv1h_x,rv4h_x,trailing_qv,
                final_5m_return,intrabar_vwap,close_vs_vwap,high_to_close_rejection
            from final
            """,
            (item["run_id"], symbol),
        )
        count = cur.rowcount
        conn.commit()
    _complete(item["id"], count)


def _materialize_market_state(item: dict[str, Any]) -> None:
    start = datetime.fromisoformat(item["payload"]["start"])
    end = datetime.fromisoformat(item["payload"]["end"])
    with db_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            with ranked as (
                select run_id,symbol,bucket_start,
                       percent_rank() over(partition by run_id,bucket_start order by trailing_liquidity_avg_qv) as pct
                from crypto_b001_replication_features
                where run_id=%s and bucket_start >= %s and bucket_start < %s
                  and trailing_liquidity_avg_qv is not null
            )
            update crypto_b001_replication_features f
               set liquidity_pct=r.pct,liquidity_eligible=(r.pct>=0.50)
              from ranked r
             where f.run_id=r.run_id and f.symbol=r.symbol and f.bucket_start=r.bucket_start
            """,
            (item["run_id"], start, end),
        )
        cur.execute(
            "delete from crypto_b001_replication_market_state where run_id=%s and bucket_start >= %s and bucket_start < %s",
            (item["run_id"], start, end),
        )
        cur.execute(
            """
            insert into crypto_b001_replication_market_state(
                run_id,bucket_start,n_symbols,breadth_up,mean_ret15,median_ret15,dispersion15,p10_ret15,p90_ret15,
                mean_range15,btc_ret15,btc_ret60,eth_ret15,eth_ret60
            )
            select run_id,bucket_start,count(*) filter(where ret15 is not null),
                avg((ret15>0)::int) filter(where ret15 is not null),avg(ret15),
                percentile_cont(.5) within group(order by ret15),stddev_samp(ret15),
                percentile_cont(.10) within group(order by ret15),percentile_cont(.90) within group(order by ret15),
                avg(range15),max(ret15) filter(where symbol='BTCUSDT'),max(ret60) filter(where symbol='BTCUSDT'),
                max(ret15) filter(where symbol='ETHUSDT'),max(ret60) filter(where symbol='ETHUSDT')
            from crypto_b001_replication_features
            where run_id=%s and bucket_start >= %s and bucket_start < %s
            group by run_id,bucket_start
            """,
            (item["run_id"], start, end),
        )
        count = cur.rowcount
        conn.commit()
    _complete(item["id"], count)


def _generate_signals(item: dict[str, Any]) -> None:
    start = datetime.fromisoformat(item["payload"]["start"])
    end = datetime.fromisoformat(item["payload"]["end"])
    run = fetch_one("select requested_start,requested_end from crypto_b001_replication_runs where id=%s", (item["run_id"],))
    if not run:
        raise RuntimeError("Replication run disappeared")
    span = run["requested_end"] - run["requested_start"]
    b1 = run["requested_start"] + span / 3
    b2 = run["requested_start"] + span * 2 / 3
    rank_start = start - timedelta(minutes=75)
    with db_connection() as conn, conn.cursor() as cur:
        cur.execute(
            "delete from crypto_b001_replication_signals where run_id=%s and bucket_start >= %s and bucket_start < %s",
            (item["run_id"], start, end),
        )
        cur.execute(
            """
            insert into crypto_b001_replication_signals(
                run_id,symbol,bucket_start,signal_ts,chronological_block,range15,pos_vs_low4h,qv_ratio16,
                range15_pct,pos_vs_low4h_pct,qv_ratio16_pct,extreme_t,extreme_t15,extreme_t30,extreme_t75,
                ret15,previous_range15,final_5m_return,high_to_close_rejection,close_vs_vwap,
                minute_rejection_a,minute_rejection_b,minute_rejection_c,minute_rejection_count,dispersion15,
                trailing_liquidity_avg_qv,liquidity_pct
            )
            with ranked as (
                select f.*,
                    percent_rank() over(partition by bucket_start order by range15) range_pct,
                    percent_rank() over(partition by bucket_start order by pos_vs_low4h) low_pct,
                    percent_rank() over(partition by bucket_start order by qv_ratio16) qv_pct
                from crypto_b001_replication_features f
                where f.run_id=%s and f.bucket_start >= %s and f.bucket_start < %s
                  and f.liquidity_eligible and f.range15 is not null and f.pos_vs_low4h is not null and f.qv_ratio16 is not null
            ), state as (
                select ranked.*,(range_pct >= %s and low_pct >= %s and qv_pct >= %s) extreme
                from ranked
            ), candidates as (
                select c.*,p.range15 previous_range,
                    p.extreme extreme_15,p2.extreme extreme_30,p5.extreme extreme_75,
                    m.dispersion15
                from state c
                join state p on p.symbol=c.symbol and p.bucket_start=c.bucket_start-interval '15 minutes'
                join state p2 on p2.symbol=c.symbol and p2.bucket_start=c.bucket_start-interval '30 minutes'
                join state p5 on p5.symbol=c.symbol and p5.bucket_start=c.bucket_start-interval '75 minutes'
                join crypto_b001_replication_market_state m on m.run_id=c.run_id and m.bucket_start=c.bucket_start
                where c.bucket_start >= %s and c.bucket_start < %s
            )
            select run_id,symbol,bucket_start,signal_ts,
                case when bucket_start < %s then 1 when bucket_start < %s then 2 else 3 end,
                range15,pos_vs_low4h,qv_ratio16,range_pct,low_pct,qv_pct,true,true,true,false,
                ret15,previous_range,final_5m_return,high_to_close_rejection,close_vs_vwap,
                final_5m_return <= %s,
                high_to_close_rejection >= %s,
                close_vs_vwap <= %s,
                (final_5m_return <= %s)::int+(high_to_close_rejection >= %s)::int+(close_vs_vwap <= %s)::int,
                dispersion15,trailing_liquidity_avg_qv,liquidity_pct
            from candidates
            where extreme and extreme_15 and extreme_30 and not extreme_75
              and ret15 <= 0 and range15 < previous_range
              and ((final_5m_return <= %s)::int+(high_to_close_rejection >= %s)::int+(close_vs_vwap <= %s)::int) >= 2
              and dispersion15 <= %s
            """,
            (
                item["run_id"],rank_start,end,EXTREME_PERCENTILE,EXTREME_PERCENTILE,EXTREME_PERCENTILE,
                max(start, run["requested_start"]),min(end, run["requested_end"]),b1,b2,
                FINAL_5M_MAX,HIGH_TO_CLOSE_MIN,CLOSE_VS_VWAP_MAX,
                FINAL_5M_MAX,HIGH_TO_CLOSE_MIN,CLOSE_VS_VWAP_MAX,
                FINAL_5M_MAX,HIGH_TO_CLOSE_MIN,CLOSE_VS_VWAP_MAX,DISPERSION_MAX,
            ),
        )
        count = cur.rowcount
        conn.commit()
    _complete(item["id"], count)


def _check_shortability(item: dict[str, Any]) -> None:
    symbol = item["payload"]["symbol"]
    period_start = date.fromisoformat(item["payload"]["period_start"])
    candidates = [symbol]
    base = symbol[:-4] if symbol.endswith("USDT") else symbol
    if base and not base.startswith("1000"):
        candidates.append(f"1000{base}USDT")
    available = False
    evidence = None
    with httpx.Client(timeout=30, follow_redirects=True) as client:
        for futures_symbol in candidates:
            url = f"{DATA_BASE}/data/futures/um/monthly/klines/{futures_symbol}/1m/{futures_symbol}-1m-{period_start:%Y-%m}.zip"
            response = client.head(url)
            if response.status_code == 200:
                available = True
                evidence = url
                break
    with db_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            insert into crypto_b001_replication_shortability(
                run_id,symbol,period_start,spot_data_present,spot_trading_status,margin_enabled,margin_evidence_source,
                perpetual_available,perpetual_evidence_source,spread_bps,spread_evidence_source
            ) values (%s,%s,%s,true,'TRADING_OBSERVED',null,'historical_margin_metadata_unavailable',%s,%s,null,'1m_kline_archive_has_no_spread')
            on conflict (run_id,symbol,period_start) do update set
                spot_data_present=true,spot_trading_status='TRADING_OBSERVED',perpetual_available=excluded.perpetual_available,
                perpetual_evidence_source=excluded.perpetual_evidence_source,checked_at=now()
            """,
            (item["run_id"], symbol, period_start, available, evidence),
        )
        cur.execute(
            """
            update crypto_b001_replication_signals s set
                spot_trading_status='TRADING_OBSERVED',margin_enabled=null,perpetual_available=%s,spread_bps=null,
                historically_executable=%s,
                shortability_reason=%s
            where s.run_id=%s and s.symbol=%s and date_trunc('month',s.bucket_start)::date=%s
            """,
            (
                available,available,
                "Binance USD-M perpetual archive present" if available else "No direct/1000x USD-M perpetual archive found; historical margin status unavailable",
                item["run_id"],symbol,period_start,
            ),
        )
        conn.commit()
    _complete(item["id"], 1, {"perpetual_available": available, "evidence": evidence})


def _refresh_run_stats(run_id: UUID) -> None:
    with db_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            with a as (
                select count(*) planned,
                       count(*) filter(where source_status='loaded') completed,
                       count(*) filter(where source_status='missing') missing,
                       coalesce(sum(rows_in_replication_window) filter(where source_status='loaded'),0) minute_rows,
                       coalesce(sum(complete_15m_count) filter(where source_status='loaded'),0) complete15,
                       coalesce(sum(incomplete_15m_count) filter(where source_status='loaded'),0) incomplete15,
                       count(distinct symbol) filter(where source_status='loaded') symbols,
                       min(first_ts) filter(where source_status='loaded') first_ts,
                       max(last_ts) filter(where source_status='loaded') last_ts
                from crypto_b001_replication_archive_files where run_id=%s
            )
            update crypto_b001_replication_runs r set
                archive_files_planned=a.planned,archive_files_completed=a.completed,archive_files_missing=a.missing,
                one_minute_rows=a.minute_rows,complete_15m_rows=a.complete15,incomplete_15m_buckets=a.incomplete15,
                symbols_loaded=a.symbols,effective_start=greatest(r.requested_start,a.first_ts),
                effective_end=least(r.requested_end,a.last_ts + interval '1 minute'),
                completeness_pct=case when a.planned>0 then 100.0*a.completed/a.planned else 0 end,updated_at=now()
            from a where r.id=%s
            """,
            (run_id, run_id),
        )
        conn.commit()


def advance_b001_run(run_id: UUID) -> None:
    _refresh_run_stats(run_id)
    run = fetch_one("select * from crypto_b001_replication_runs where id=%s", (run_id,))
    if not run or run["status"] in {"paused","cancelled","failed","completed"}:
        return
    counts = {row["stage"]: row for row in fetch_all(
        """
        select stage,count(*) total,
               count(*) filter(where status in ('queued','retry_wait','running')) active,
               count(*) filter(where status='failed') failed
        from crypto_b001_replication_work_items where run_id=%s group by stage
        """,
        (run_id,),
    )}
    discovery_active = sum(int(counts.get(stage,{}).get("active") or 0) for stage in ("discover_archives","discover_symbol"))
    spot_active = int(counts.get("spot_month",{}).get("active") or 0)
    if discovery_active or spot_active:
        stage = "archive_discovery" if discovery_active else "historical_1m_backfill"
        with db_connection() as conn, conn.cursor() as cur:
            cur.execute("update crypto_b001_replication_runs set stage=%s,updated_at=now() where id=%s", (stage,run_id))
            conn.commit()
        return
    if "spot_month" not in counts:
        return
    if "derive_features" not in counts:
        symbols = fetch_all("select distinct symbol from crypto_b001_replication_15m where run_id=%s order by symbol", (run_id,))
        with db_connection() as conn, conn.cursor() as cur:
            for row in symbols:
                cur.execute(
                    "insert into crypto_b001_replication_work_items(run_id,stage,partition_key,payload) values (%s,'derive_features',%s,%s) on conflict do nothing",
                    (run_id,row["symbol"],Jsonb({"symbol":row["symbol"]})),
                )
            cur.execute("update crypto_b001_replication_runs set stage='feature_reconstruction',updated_at=now() where id=%s", (run_id,))
            conn.commit()
        return
    if int(counts["derive_features"].get("active") or 0):
        return
    if "market_state" not in counts:
        collection_start = run["requested_start"] - timedelta(days=LIQUIDITY_LOOKBACK_DAYS + 1)
        with db_connection() as conn, conn.cursor() as cur:
            for start,end in _months(collection_start,run["requested_end"]):
                cur.execute(
                    "insert into crypto_b001_replication_work_items(run_id,stage,partition_key,payload) values (%s,'market_state',%s,%s) on conflict do nothing",
                    (run_id,f"{start:%Y-%m}",Jsonb({"start":start.isoformat(),"end":end.isoformat()})),
                )
            cur.execute("update crypto_b001_replication_runs set stage='market_state',updated_at=now() where id=%s", (run_id,))
            conn.commit()
        return
    if int(counts["market_state"].get("active") or 0):
        return
    if "signals" not in counts:
        with db_connection() as conn, conn.cursor() as cur:
            for start,end in _months(run["requested_start"],run["requested_end"]):
                cur.execute(
                    "insert into crypto_b001_replication_work_items(run_id,stage,partition_key,payload) values (%s,'signals',%s,%s) on conflict do nothing",
                    (run_id,f"{start:%Y-%m}",Jsonb({"start":start.isoformat(),"end":end.isoformat()})),
                )
            cur.execute("update crypto_b001_replication_runs set stage='frozen_signal_generation',updated_at=now() where id=%s", (run_id,))
            conn.commit()
        return
    if int(counts["signals"].get("active") or 0):
        return
    if "shortability" not in counts:
        pairs = fetch_all(
            "select distinct symbol,date_trunc('month',bucket_start)::date period_start from crypto_b001_replication_signals where run_id=%s order by 2,1",
            (run_id,),
        )
        with db_connection() as conn, conn.cursor() as cur:
            for row in pairs:
                key = f"{row['symbol']}:{row['period_start']:%Y-%m}"
                cur.execute(
                    "insert into crypto_b001_replication_work_items(run_id,stage,partition_key,payload) values (%s,'shortability',%s,%s) on conflict do nothing",
                    (run_id,key,Jsonb({"symbol":row["symbol"],"period_start":row["period_start"].isoformat()})),
                )
            cur.execute("update crypto_b001_replication_runs set primary_signal_count=(select count(*) from crypto_b001_replication_signals where run_id=%s),stage='shortability',updated_at=now() where id=%s", (run_id,run_id))
            conn.commit()
        return
    if int(counts["shortability"].get("active") or 0):
        return
    if "analysis" not in counts:
        with db_connection() as conn, conn.cursor() as cur:
            cur.execute(
                "insert into crypto_b001_replication_work_items(run_id,stage,partition_key,payload) values (%s,'analysis','full',%s) on conflict do nothing",
                (run_id,Jsonb({})),
            )
            cur.execute("update crypto_b001_replication_runs set stage='replication_analysis',updated_at=now() where id=%s", (run_id,))
            conn.commit()


def process_b001_work(item: dict[str, Any]) -> None:
    try:
        stage = item["stage"]
        if stage == "discover_archives":
            _discover_archives(item)
        elif stage == "discover_symbol":
            _discover_symbol(item)
        elif stage == "spot_month":
            _process_spot_month(item)
        elif stage == "derive_features":
            _derive_features(item)
        elif stage == "market_state":
            _materialize_market_state(item)
        elif stage == "signals":
            _generate_signals(item)
        elif stage == "shortability":
            _check_shortability(item)
        elif stage == "analysis":
            from app.b001_analysis import run_full_analysis
            run_full_analysis(UUID(str(item["run_id"])))
            _complete(item["id"], 1)
        else:
            raise ValueError(f"Unknown B-001 work stage {stage}")
        advance_b001_run(UUID(str(item["run_id"])))
    except Exception as exc:
        logger.exception("B-001 work failed stage=%s key=%s", item.get("stage"), item.get("partition_key"))
        _fail(item, exc)


def worker_identity() -> str:
    return f"b001:{socket.gethostname()}"
