from __future__ import annotations

"""Canonical-history hardening layer for the locked B-001 runtime.

Python prefers this package over the legacy sibling module ``app/b001_runtime.py``.
We deliberately load that legacy runtime under a private module name so all of its
pre-outcome methodology hardening remains active, then replace only the monthly
Binance archive ingestion step.

The replacement fixes the storage boundary required by the historical replication
programme:

* every downloaded Binance 1-minute bar is bulk-upserted into the existing
  ``market_bars_1m_binance`` canonical miner partition;
* the same staged rows are aggregated into the B-001 15-minute research layer,
  so the archive is parsed only once;
* monthly source ZIPs are temporary ingestion artefacts and are not duplicated in
  Supabase Storage;
* retries remain idempotent via the existing canonical unique key and B-001 keys.

No B-001 signal threshold, execution rule, cost, chronology, or analysis logic is
changed here.
"""

import csv
import hashlib
import importlib.util
import io
import sys
import tempfile
import zipfile
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import httpx
from psycopg.types.json import Jsonb


_LEGACY_PATH = Path(__file__).resolve().parents[1] / "b001_runtime.py"
_SPEC = importlib.util.spec_from_file_location("app._b001_runtime_legacy", _LEGACY_PATH)
if _SPEC is None or _SPEC.loader is None:  # pragma: no cover
    raise ImportError(f"Unable to load legacy B-001 runtime from {_LEGACY_PATH}")
_LEGACY = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = _LEGACY
_SPEC.loader.exec_module(_LEGACY)

replication = _LEGACY.replication
db_connection = _LEGACY.db_connection
fetch_one = _LEGACY.fetch_one


def _archive_timestamp(value: str) -> datetime:
    raw = int(value)
    divisor = 1_000_000 if abs(raw) >= 1_000_000_000_000_000 else 1_000
    return datetime.fromtimestamp(raw / divisor, tz=timezone.utc)


def _historical_instrument_id(
    cur: Any,
    symbol: str,
    first_ts: datetime | None,
    last_ts: datetime | None,
) -> Any:
    cur.execute(
        "select id from instruments where provider='binance' and provider_symbol=%s",
        (symbol,),
    )
    row = cur.fetchone()
    if row:
        return row["id"]

    if not symbol.endswith("USDT") or len(symbol) <= 4:
        raise ValueError(f"B-001 archive symbol is not a canonical USDT pair: {symbol}")
    base_asset = symbol[:-4]
    seen_start = first_ts or datetime.now(timezone.utc)
    seen_end = last_ts or seen_start
    cur.execute(
        """
        insert into instruments(
            provider,provider_symbol,canonical_symbol,display_name,asset_class,
            base_asset,quote_asset,exchange,status,tradable,preferred,source_feed,
            priority,metadata,first_seen_at,last_seen_at
        ) values (
            'binance',%s,%s,%s,'crypto_spot',%s,'USDT','Binance.com',
            'HISTORICAL',false,false,'binance_data_vision',0,%s,%s,%s
        )
        on conflict (provider,provider_symbol) do nothing
        returning id
        """,
        (
            symbol,
            base_asset,
            f"{base_asset}/USDT",
            base_asset,
            Jsonb({
                "historical_archive": True,
                "source": "data.binance.vision",
                "quoteAsset": "USDT",
                "symbol": symbol,
            }),
            seen_start,
            seen_end,
        ),
    )
    inserted = cur.fetchone()
    if inserted:
        return inserted["id"]
    cur.execute(
        "select id from instruments where provider='binance' and provider_symbol=%s",
        (symbol,),
    )
    existing = cur.fetchone()
    if not existing:
        raise RuntimeError(f"Unable to resolve canonical instrument for {symbol}")
    return existing["id"]


def _process_spot_month_canonical(item: dict[str, Any]) -> None:
    payload = item["payload"]
    symbol = str(payload["symbol"]).upper()
    period_start = date.fromisoformat(payload["period_start"])
    run = fetch_one(
        "select * from crypto_b001_replication_runs where id=%s",
        (item["run_id"],),
    )
    if not run:
        raise RuntimeError("Replication run disappeared")

    collection_start = run["requested_start"] - timedelta(
        days=replication.LIQUIDITY_LOOKBACK_DAYS + 1
    )
    requested_end = run["requested_end"]
    source_url = payload["source_url"]
    checksum_url = payload["checksum_url"]

    with tempfile.TemporaryDirectory(prefix="b001-") as temp_dir:
        path = Path(temp_dir) / source_url.rsplit("/", 1)[-1]
        digest = hashlib.sha256()

        with httpx.Client(timeout=180, follow_redirects=True) as client:
            with client.stream("GET", source_url) as response:
                if response.status_code == 404:
                    with db_connection() as conn, conn.cursor() as cur:
                        cur.execute(
                            """
                            update crypto_b001_replication_archive_files
                               set source_status='missing',updated_at=now()
                             where run_id=%s and symbol=%s and period_start=%s
                            """,
                            (item["run_id"], symbol, period_start),
                        )
                        conn.commit()
                    replication._complete(
                        item["id"], 0, {"http_status": 404}, status="missing"
                    )
                    return
                response.raise_for_status()
                with path.open("wb") as handle:
                    for chunk in response.iter_bytes(chunk_size=1024 * 1024):
                        if not chunk:
                            continue
                        digest.update(chunk)
                        handle.write(chunk)
            checksum_response = client.get(checksum_url)

        computed = digest.hexdigest()
        source_checksum = (
            replication._parse_checksum(checksum_response.text)
            if checksum_response.status_code == 200
            else None
        )
        verified = source_checksum == computed if source_checksum else None
        if verified is False:
            raise ValueError(f"Checksum mismatch for {source_url}")

        total_rows = 0
        rows_in_window = 0
        first_ts: datetime | None = None
        last_ts: datetime | None = None

        with db_connection() as conn, conn.cursor() as cur:
            cur.execute(
                """
                create temporary table b001_stage_1m(
                    ts timestamptz primary key,
                    open double precision,
                    high double precision,
                    low double precision,
                    close double precision,
                    volume double precision,
                    quote_volume double precision,
                    trade_count bigint,
                    vwap double precision,
                    taker_buy_base_volume double precision,
                    taker_buy_quote_volume double precision
                ) on commit drop
                """
            )

            with zipfile.ZipFile(path) as archive:
                members = [
                    name for name in archive.namelist() if name.lower().endswith(".csv")
                ]
                if not members:
                    raise ValueError("Binance archive contains no CSV")
                with archive.open(members[0]) as raw, io.TextIOWrapper(
                    raw, encoding="utf-8", newline=""
                ) as text, cur.copy(
                    """
                    copy b001_stage_1m(
                        ts,open,high,low,close,volume,quote_volume,trade_count,vwap,
                        taker_buy_base_volume,taker_buy_quote_volume
                    ) from stdin
                    """
                ) as copy:
                    reader = csv.reader(text)
                    for row in reader:
                        if len(row) < 11:
                            continue
                        try:
                            ts = _archive_timestamp(row[0])
                            open_px = float(row[1])
                            high = float(row[2])
                            low = float(row[3])
                            close = float(row[4])
                            volume = float(row[5])
                            quote_volume = float(row[7])
                            trade_count = int(float(row[8]))
                            taker_buy_base_volume = float(row[9])
                            taker_buy_quote_volume = float(row[10])
                        except (ValueError, OverflowError):
                            continue
                        vwap = quote_volume / volume if volume else None
                        copy.write_row(
                            (
                                ts,
                                open_px,
                                high,
                                low,
                                close,
                                volume,
                                quote_volume,
                                trade_count,
                                vwap,
                                taker_buy_base_volume,
                                taker_buy_quote_volume,
                            )
                        )
                        total_rows += 1
                        if collection_start <= ts < requested_end:
                            rows_in_window += 1
                        first_ts = min(first_ts, ts) if first_ts else ts
                        last_ts = max(last_ts, ts) if last_ts else ts

            instrument_id = _historical_instrument_id(cur, symbol, first_ts, last_ts)

            cur.execute(
                """
                insert into market_bars_1m_binance(
                    provider,instrument_id,ts,open,high,low,close,volume,quote_volume,
                    trade_count,vwap,taker_buy_base_volume,taker_buy_quote_volume,source_feed
                )
                select 'binance',%s,ts,open,high,low,close,volume,quote_volume,
                       trade_count,vwap,taker_buy_base_volume,taker_buy_quote_volume,
                       'binance_data_vision_monthly_1m'
                  from b001_stage_1m
                on conflict (provider,instrument_id,ts) do nothing
                """,
                (instrument_id,),
            )
            canonical_inserted = cur.rowcount

            cur.execute(
                """
                with bucketed as (
                    select
                        date_bin(interval '15 minutes',ts,timestamptz '1970-01-01') bucket_start,
                        count(*) minute_count,
                        min(ts) first_minute_ts,
                        max(ts) last_minute_ts,
                        (array_agg(open order by ts))[1] open,
                        max(high) high,
                        min(low) low,
                        (array_agg(close order by ts))[15] close,
                        array_agg(close order by ts) closes,
                        sum(volume) volume,
                        sum(quote_volume) quote_volume,
                        sum(trade_count)::bigint trade_count,
                        sum(taker_buy_quote_volume) taker_buy_quote_volume
                    from b001_stage_1m
                    where ts >= %s and ts < %s
                    group by 1
                ), complete as (
                    select *,
                        close/nullif(closes[10],0)-1 final_5m_return,
                        quote_volume/nullif(volume,0) intrabar_vwap
                    from bucketed
                    where minute_count=15
                      and last_minute_ts=first_minute_ts+interval '14 minutes'
                ), prepared as (
                    select *,
                        close/nullif(intrabar_vwap,0)-1 close_vs_vwap,
                        (high-close)/nullif(high,0) high_to_close_rejection
                    from complete
                )
                insert into crypto_b001_replication_15m(
                    run_id,symbol,bucket_start,signal_ts,minute_count,open,high,low,close,
                    volume,quote_volume,trade_count,taker_buy_quote_volume,final_5m_return,
                    intrabar_vwap,close_vs_vwap,high_to_close_rejection,first_minute_ts,
                    last_minute_ts,source_period_start
                )
                select %s,%s,bucket_start,bucket_start+interval '15 minutes',minute_count,
                       open,high,low,close,volume,quote_volume,trade_count,
                       taker_buy_quote_volume,final_5m_return,intrabar_vwap,close_vs_vwap,
                       high_to_close_rejection,first_minute_ts,last_minute_ts,%s
                  from prepared
                on conflict (run_id,symbol,bucket_start) do update set
                    signal_ts=excluded.signal_ts,
                    minute_count=excluded.minute_count,
                    open=excluded.open,
                    high=excluded.high,
                    low=excluded.low,
                    close=excluded.close,
                    volume=excluded.volume,
                    quote_volume=excluded.quote_volume,
                    trade_count=excluded.trade_count,
                    taker_buy_quote_volume=excluded.taker_buy_quote_volume,
                    final_5m_return=excluded.final_5m_return,
                    intrabar_vwap=excluded.intrabar_vwap,
                    close_vs_vwap=excluded.close_vs_vwap,
                    high_to_close_rejection=excluded.high_to_close_rejection,
                    first_minute_ts=excluded.first_minute_ts,
                    last_minute_ts=excluded.last_minute_ts,
                    source_period_start=excluded.source_period_start
                """,
                (
                    collection_start,
                    requested_end,
                    item["run_id"],
                    symbol,
                    period_start,
                ),
            )
            complete_15m = cur.rowcount

            cur.execute(
                """
                with bucketed as (
                    select
                        date_bin(interval '15 minutes',ts,timestamptz '1970-01-01') bucket_start,
                        count(*) minute_count,
                        min(ts) first_minute_ts,
                        max(ts) last_minute_ts
                    from b001_stage_1m
                    where ts >= %s and ts < %s
                    group by 1
                )
                select
                    count(*) filter(
                        where minute_count<>15
                           or last_minute_ts<>first_minute_ts+interval '14 minutes'
                    )::bigint incomplete_15m,
                    coalesce(sum(
                        greatest(0,15-minute_count)
                    ) filter(
                        where minute_count<>15
                           or last_minute_ts<>first_minute_ts+interval '14 minutes'
                    ),0)::bigint missing_minutes
                from bucketed
                """,
                (collection_start, requested_end),
            )
            quality = cur.fetchone() or {}
            incomplete_15m = int(quality.get("incomplete_15m") or 0)
            missing_minutes = int(quality.get("missing_minutes") or 0)

            cur.execute(
                """
                update crypto_b001_replication_archive_files set
                    source_checksum=%s,
                    computed_checksum=%s,
                    checksum_verified=%s,
                    storage_object_path=null,
                    storage_size_bytes=null,
                    source_status='loaded',
                    row_count=%s,
                    rows_in_replication_window=%s,
                    first_ts=%s,
                    last_ts=%s,
                    complete_15m_count=%s,
                    incomplete_15m_count=%s,
                    missing_minute_count=%s,
                    metadata=coalesce(metadata,'{}'::jsonb) || %s,
                    updated_at=now()
                where run_id=%s and symbol=%s and period_start=%s
                """,
                (
                    source_checksum,
                    computed,
                    verified,
                    total_rows,
                    rows_in_window,
                    first_ts,
                    last_ts,
                    complete_15m,
                    incomplete_15m,
                    missing_minutes,
                    Jsonb({
                        "canonical_store": "market_bars_1m_binance",
                        "canonical_inserted_rows": canonical_inserted,
                        "archive_retained": False,
                        "source_feed": "binance_data_vision_monthly_1m",
                    }),
                    item["run_id"],
                    symbol,
                    period_start,
                ),
            )
            conn.commit()

        stats = {
            "total_rows": total_rows,
            "rows_in_window": rows_in_window,
            "first_ts": first_ts,
            "last_ts": last_ts,
            "canonical_inserted_rows": canonical_inserted,
            "complete_15m": complete_15m,
            "incomplete_15m": incomplete_15m,
            "missing_minutes": missing_minutes,
        }
        replication._complete(item["id"], rows_in_window, stats)


replication._process_spot_month = _process_spot_month_canonical

claim_b001_work = replication.claim_b001_work
process_b001_work = replication.process_b001_work
reclaim_stale_b001_work = replication.reclaim_stale_b001_work
create_b001_run = replication.create_b001_run
