from __future__ import annotations

"""Fast, lineage-preserving ingestion for explicit B-001 forward holdouts.

Historical replication keeps the canonical 1-minute upsert unchanged. A
prospective holdout already has its frozen universe/rule and only needs a
cryptographically pinned raw source plus the identical run-specific 15-minute
research bars. For runs tagged ``execution_spec.purpose=forward_holdout`` this
module therefore:

* downloads the exact Binance archive;
* verifies Binance's published SHA-256 checksum;
* pins the immutable Binance source URL by source/computed SHA-256;
* applies the existing locked ``_aggregate_archive`` implementation;
* writes the same ``crypto_b001_replication_15m`` rows and archive QA metadata;
* omits redundant canonical 1-minute upserts and duplicate raw-archive mirroring.

The completion verifier requires checksum identity, an immutable Binance URL
pinned by SHA-256, and exact persisted 15-minute counts. No signal, threshold,
chronology, cost, shortability, or outcome logic changes.
"""

import hashlib
import tempfile
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import httpx
from psycopg.types.json import Jsonb

import app.b001_operational_hardening as operational
import app.b001_replication as replication
from app.db import db_connection, fetch_one


_ORIGINAL_PROCESS_SPOT = replication._process_spot_month
_ORIGINAL_VERIFY_ARCHIVE = operational._verify_archive_before_complete
_HTTP = httpx.Client(
    timeout=180,
    follow_redirects=True,
    limits=httpx.Limits(max_connections=64, max_keepalive_connections=32, keepalive_expiry=60.0),
)


def _is_forward_holdout(run: dict[str, Any] | None) -> bool:
    return bool(run and (run.get("execution_spec") or {}).get("purpose") == "forward_holdout")


def _process_forward_archive(item: dict[str, Any]) -> None:
    payload = dict(item["payload"] or {})
    symbol = str(payload["symbol"]).upper()
    period_start = date.fromisoformat(str(payload["period_start"]))
    run = fetch_one("select * from crypto_b001_replication_runs where id=%s", (item["run_id"],))
    if not _is_forward_holdout(run):
        return _ORIGINAL_PROCESS_SPOT(item)

    collection_start = run["requested_start"] - timedelta(days=replication.LIQUIDITY_LOOKBACK_DAYS + 1)
    source_url = str(payload["source_url"])
    checksum_url = str(payload["checksum_url"])
    granularity = str(payload.get("archive_granularity") or "monthly")

    with tempfile.TemporaryDirectory(prefix="b001-forward-") as temp_dir:
        path = Path(temp_dir) / source_url.rsplit("/", 1)[-1]
        digest = hashlib.sha256()
        with _HTTP.stream("GET", source_url) as response:
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
                replication._complete(item["id"], 0, {"http_status": 404}, status="missing")
                return
            response.raise_for_status()
            with path.open("wb") as handle:
                for chunk in response.iter_bytes(chunk_size=1024 * 1024):
                    if not chunk:
                        continue
                    digest.update(chunk)
                    handle.write(chunk)
        checksum_response = _HTTP.get(checksum_url)

        computed = digest.hexdigest()
        source_checksum = (
            replication._parse_checksum(checksum_response.text)
            if checksum_response.status_code == 200
            else None
        )
        if not source_checksum:
            raise ValueError(f"Forward holdout requires a published checksum: {checksum_url}")
        if source_checksum != computed:
            raise ValueError(f"Checksum mismatch for {source_url}")

        rows, stats = replication._aggregate_archive(path, collection_start, run["requested_end"])
        with db_connection() as conn, conn.cursor() as cur:
            cur.execute(
                """
                create temporary table b001_forward_stage_15m(
                    bucket_start timestamptz,signal_ts timestamptz,minute_count integer,
                    open double precision,high double precision,low double precision,
                    close double precision,volume double precision,quote_volume double precision,
                    trade_count bigint,taker_buy_quote_volume double precision,
                    final_5m_return double precision,intrabar_vwap double precision,
                    close_vs_vwap double precision,high_to_close_rejection double precision,
                    first_minute_ts timestamptz,last_minute_ts timestamptz
                ) on commit drop
                """
            )
            if rows:
                with cur.copy("copy b001_forward_stage_15m from stdin") as copy:
                    for row in rows:
                        copy.write_row(row)
                cur.execute(
                    """
                    insert into crypto_b001_replication_15m(
                        run_id,symbol,bucket_start,signal_ts,minute_count,open,high,low,close,
                        volume,quote_volume,trade_count,taker_buy_quote_volume,final_5m_return,
                        intrabar_vwap,close_vs_vwap,high_to_close_rejection,first_minute_ts,
                        last_minute_ts,source_period_start
                    )
                    select %s,%s,bucket_start,signal_ts,minute_count,open,high,low,close,
                           volume,quote_volume,trade_count,taker_buy_quote_volume,final_5m_return,
                           intrabar_vwap,close_vs_vwap,high_to_close_rejection,first_minute_ts,
                           last_minute_ts,%s
                      from b001_forward_stage_15m
                    on conflict (run_id,symbol,bucket_start) do update set
                        signal_ts=excluded.signal_ts,minute_count=excluded.minute_count,
                        open=excluded.open,high=excluded.high,low=excluded.low,close=excluded.close,
                        volume=excluded.volume,quote_volume=excluded.quote_volume,
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
                    (item["run_id"], symbol, period_start),
                )

            cur.execute(
                """
                update crypto_b001_replication_archive_files set
                    source_checksum=%s,computed_checksum=%s,checksum_verified=true,
                    storage_object_path=null,storage_size_bytes=null,source_status='loaded',
                    row_count=%s,rows_in_replication_window=%s,first_ts=%s,last_ts=%s,
                    complete_15m_count=%s,incomplete_15m_count=%s,missing_minute_count=%s,
                    metadata=coalesce(metadata,'{}'::jsonb) || %s,updated_at=now()
                where run_id=%s and symbol=%s and period_start=%s
                """,
                (
                    source_checksum,computed,
                    stats["total_rows"],stats["rows_in_window"],stats["first_ts"],stats["last_ts"],
                    stats["complete_15m"],stats["incomplete_15m"],stats["missing_minutes"],
                    Jsonb({
                        "forward_holdout_fast_path": True,
                        "raw_archive_retained": False,
                        "source_archive_persistence": "remote_checksum_only",
                        "source_archive_provider": "Binance data.binance.vision",
                        "canonical_1m_upsert_skipped": True,
                        "duplicate_source_archive_upload_skipped": True,
                        "reason": "sealed prospective holdout; immutable Binance URL pinned by published SHA-256; identical locked 15m aggregation",
                        "archive_granularity": granularity,
                    }),
                    item["run_id"],symbol,period_start,
                ),
            )
            conn.commit()

    replication._complete(
        item["id"],
        stats["rows_in_window"],
        {
            **stats,
            "forward_holdout_fast_path": True,
            "source_archive_persistence": "remote_checksum_only",
            "checksum_verified": True,
        },
    )


def _verify_archive_before_complete(item_id: int) -> dict[str, Any]:
    item = fetch_one(
        "select id,run_id,stage,partition_key,payload from crypto_b001_replication_work_items where id=%s",
        (item_id,),
    )
    if not item or item.get("stage") != "spot_month":
        return {}
    run = fetch_one(
        "select execution_spec from crypto_b001_replication_runs where id=%s",
        (item["run_id"],),
    )
    if not _is_forward_holdout(run):
        return _ORIGINAL_VERIFY_ARCHIVE(item_id)

    payload = dict(item.get("payload") or {})
    symbol = str(payload.get("symbol") or "").upper()
    period_start = payload.get("period_start")
    archive = fetch_one(
        """
        select source_status,source_url,source_checksum,computed_checksum,checksum_verified,
               row_count,first_ts,last_ts,complete_15m_count,
               storage_object_path,storage_size_bytes,metadata
          from crypto_b001_replication_archive_files
         where run_id=%s and symbol=%s and period_start=%s::date
        """,
        (item["run_id"],symbol,period_start),
    )
    if not archive:
        raise operational.ArchiveVerificationError(
            f"forward archive ledger row missing for {symbol}:{period_start}"
        )
    if archive.get("source_status") != "loaded":
        raise operational.ArchiveVerificationError(
            f"forward archive ledger not loaded for {symbol}:{period_start}"
        )
    if archive.get("checksum_verified") is not True:
        raise operational.ArchiveVerificationError(
            f"forward archive checksum not verified for {symbol}:{period_start}"
        )
    if not archive.get("source_checksum") or archive.get("source_checksum") != archive.get("computed_checksum"):
        raise operational.ArchiveVerificationError(
            f"forward source/computed checksum identity failed for {symbol}:{period_start}"
        )

    metadata = dict(archive.get("metadata") or {})
    if not bool(metadata.get("forward_holdout_fast_path")):
        return _ORIGINAL_VERIFY_ARCHIVE(item_id)

    remote_pinned = (
        metadata.get("source_archive_persistence") == "remote_checksum_only"
        and str(archive.get("source_url") or "").startswith("https://data.binance.vision/")
        and bool(archive.get("source_checksum"))
    )
    if not remote_pinned:
        raise operational.ArchiveVerificationError(
            f"forward raw source is not checksum-pinned for {symbol}:{period_start}"
        )

    persisted = fetch_one(
        """
        select count(*)::bigint n,min(first_minute_ts) first_ts,max(last_minute_ts) last_ts
          from crypto_b001_replication_15m
         where run_id=%s and symbol=%s and source_period_start=%s::date
        """,
        (item["run_id"],symbol,period_start),
    ) or {}
    persisted_15m = int(persisted.get("n") or 0)
    expected_15m = int(archive.get("complete_15m_count") or 0)
    if persisted_15m != expected_15m:
        raise operational.ArchiveVerificationError(
            f"forward 15m verification failed for {symbol}:{period_start}: expected={expected_15m} found={persisted_15m}"
        )

    return {
        "verified": True,
        "verification_mode": "immutable_binance_url_plus_sha256_plus_exact_15m_count",
        "raw_archive_mirrored": False,
        "source_archive_persistence": "remote_checksum_only",
        "source_url": archive.get("source_url"),
        "source_checksum": archive.get("source_checksum"),
        "archive_rows": int(archive.get("row_count") or 0),
        "persisted_15m_rows": persisted_15m,
    }


replication._process_spot_month = _process_forward_archive
operational._verify_archive_before_complete = _verify_archive_before_complete
