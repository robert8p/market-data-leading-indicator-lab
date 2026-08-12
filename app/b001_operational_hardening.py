from __future__ import annotations

"""Operational resilience for the long-running locked B-001 replication.

This module changes no signal, feature, universe, execution, cost, or validation
rule. It provides durable work semantics around the frozen research pipeline:

* transient infrastructure failures retry with exponential backoff;
* transient DB failures do not consume the finite research/data retry budget;
* long-running work heartbeats its lock so an active job is never reclaimed as stale;
* completed archive work is verified against canonical DB inserts before the work
  item is marked complete;
* permanent archive failures are recorded for later reprocessing but do not
  terminate the rest of the 24-month run;
* operational progress/throughput/failure metrics are checkpointed on the run.
"""

import logging
import threading
import time
import zipfile
from datetime import datetime, timezone
from typing import Any
from uuid import UUID

import httpx
from psycopg import InterfaceError, OperationalError
from psycopg.types.json import Jsonb
from psycopg_pool import PoolTimeout

import app.b001_replication as replication
# Importing methodology hardening applies all pre-outcome replication patches
# before the operational wrapper below is installed.
import app.b001_methodology_hardening as methodology  # noqa: F401
# Replace the release facade's O(N) progress scan only after all methodology
# patches are installed. This changes counters/operational cost, not research.
import app.b001_progress_hardening as progress_hardening  # noqa: F401
from app.db import check_pool, db_connection, fetch_one


logger = logging.getLogger(__name__)
TRANSIENT_DB_EXCEPTIONS = (PoolTimeout, OperationalError, InterfaceError)
HEARTBEAT_SECONDS = 30.0
_PROGRESS_LOG_SECONDS = 60.0
_progress_lock = threading.Lock()
_last_progress_log = 0.0


class ArchiveVerificationError(RuntimeError):
    """Raised when durable inserts do not reconcile with the archive ledger."""


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def is_transient_db_error(exc: BaseException) -> bool:
    """Return True only for connection/pool infrastructure failures."""
    current: BaseException | None = exc
    seen: set[int] = set()
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        if isinstance(current, TRANSIENT_DB_EXCEPTIONS):
            return True
        message = str(current).lower()
        if any(
            marker in message
            for marker in (
                "couldn't get a connection",
                "could not get a connection",
                "connection timeout",
                "connection timed out",
                "connection is closed",
                "server closed the connection unexpectedly",
                "max clients reached",
                "emaxconnsession",
                "terminating connection due to administrator command",
            )
        ):
            return True
        current = current.__cause__ or current.__context__
    return False


def classify_failure(exc: BaseException) -> tuple[bool, str]:
    """Classify a failure as retryable or permanent without hiding code/data bugs."""
    if is_transient_db_error(exc):
        return True, "db_transient"
    if isinstance(exc, (httpx.TimeoutException, httpx.NetworkError, TimeoutError, ConnectionError)):
        return True, "network_transient"
    if isinstance(exc, httpx.HTTPStatusError):
        status = exc.response.status_code
        if status in {408, 425, 429} or status >= 500:
            return True, f"http_{status}_transient"
        return False, f"http_{status}_permanent"
    if isinstance(exc, zipfile.BadZipFile):
        return True, "archive_download_corrupt"

    message = str(exc).lower()
    if "checksum mismatch" in message:
        return True, "checksum_mismatch"
    if isinstance(exc, ArchiveVerificationError):
        return False, "archive_verification_failed"
    if isinstance(exc, (TypeError, KeyError, AssertionError)):
        return False, "implementation_error"
    if isinstance(exc, ValueError):
        return False, "data_validation_error"
    if isinstance(exc, OSError):
        return True, "io_transient"
    return True, "replication_retryable"


def _retry_delay_seconds(retry_number: int, item_id: int, *, ceiling: int = 900) -> int:
    retry_number = max(1, retry_number)
    base = min(ceiling, 15 * (2 ** min(retry_number - 1, 6)))
    return min(ceiling, base + (item_id % 31))


def _checkpoint_with_db_retry(action, *, label: str) -> None:
    last_exc: BaseException | None = None
    for checkpoint_attempt in range(5):
        try:
            action()
            return
        except Exception as exc:
            last_exc = exc
            if not is_transient_db_error(exc):
                raise
            try:
                check_pool()
            except Exception:
                pass
            if checkpoint_attempt < 4:
                time.sleep(min(1.0 * (2**checkpoint_attempt), 8.0))
    assert last_exc is not None
    logger.error("Unable to persist B-001 checkpoint after retries label=%s error=%s", label, last_exc)
    raise last_exc


def _record_transient_db_retry(item: dict[str, Any], exc: BaseException) -> None:
    progress = dict(item.get("progress") or {})
    infra_retries = int(progress.get("infra_retries") or 0) + 1
    delay = _retry_delay_seconds(infra_retries, int(item["id"]))
    patch = {
        "infra_retries": infra_retries,
        "last_transient_error": f"{type(exc).__name__}: {exc}",
        "last_transient_error_at": _utc_now_iso(),
        "last_retry_delay_seconds": delay,
    }

    def persist() -> None:
        with db_connection() as conn, conn.cursor() as cur:
            cur.execute(
                """
                update crypto_b001_replication_work_items
                   set status='retry_wait', attempts=greatest(attempts-1,0),
                       last_error=%s, error_code='db_transient',
                       progress=coalesce(progress,'{}'::jsonb) || %s,
                       locked_by=null, locked_at=null,
                       not_before=now()+(%s * interval '1 second'), updated_at=now()
                 where id=%s
                """,
                (f"{type(exc).__name__}: {exc}", Jsonb(patch), delay, item["id"]),
            )
            conn.commit()

    _checkpoint_with_db_retry(persist, label="transient-db-retry")
    logger.warning(
        "B-001 transient DB failure deferred without consuming retry budget stage=%s key=%s infra_retry=%s delay=%ss",
        item.get("stage"), item.get("partition_key"), infra_retries, delay,
    )


def _record_retryable_failure(item: dict[str, Any], exc: BaseException, code: str) -> None:
    attempts = int(item.get("attempts") or 1)
    max_attempts = int(item.get("max_attempts") or 8)
    if attempts >= max_attempts:
        _record_permanent_failure(item, exc, f"{code}_exhausted")
        return
    delay = _retry_delay_seconds(attempts, int(item["id"]))
    progress = dict(item.get("progress") or {})
    retries = int(progress.get("retries") or 0) + 1
    patch = {
        "retries": retries,
        "last_retry_error": f"{type(exc).__name__}: {exc}",
        "last_retry_at": _utc_now_iso(),
        "last_retry_delay_seconds": delay,
    }

    def persist() -> None:
        with db_connection() as conn, conn.cursor() as cur:
            cur.execute(
                """
                update crypto_b001_replication_work_items
                   set status='retry_wait',last_error=%s,error_code=%s,
                       progress=coalesce(progress,'{}'::jsonb) || %s,
                       locked_by=null,locked_at=null,
                       not_before=now()+(%s * interval '1 second'),updated_at=now()
                 where id=%s
                """,
                (f"{type(exc).__name__}: {exc}", code, Jsonb(patch), delay, item["id"]),
            )
            conn.commit()

    _checkpoint_with_db_retry(persist, label="retryable-failure")
    logger.warning(
        "B-001 retryable failure stage=%s key=%s attempt=%s/%s code=%s delay=%ss",
        item.get("stage"), item.get("partition_key"), attempts, max_attempts, code, delay,
    )


def _record_permanent_failure(item: dict[str, Any], exc: BaseException, code: str) -> None:
    failure = {
        "permanent": True,
        "error_code": code,
        "error": f"{type(exc).__name__}: {exc}",
        "failed_at": _utc_now_iso(),
        "reprocessable": True,
    }
    is_archive = item.get("stage") == "spot_month"

    def persist() -> None:
        with db_connection() as conn, conn.cursor() as cur:
            cur.execute(
                """
                update crypto_b001_replication_work_items
                   set status='failed',last_error=%s,error_code=%s,
                       progress=coalesce(progress,'{}'::jsonb) || %s,
                       locked_by=null,locked_at=null,updated_at=now()
                 where id=%s
                """,
                (failure["error"], code, Jsonb({"permanent_failure": failure}), item["id"]),
            )
            if is_archive:
                payload = dict(item.get("payload") or {})
                cur.execute(
                    """
                    update crypto_b001_replication_archive_files
                       set source_status='failed', metadata=coalesce(metadata,'{}'::jsonb) || %s, updated_at=now()
                     where run_id=%s and symbol=%s and period_start=%s::date
                    """,
                    (Jsonb({"permanent_failure": failure}), item["run_id"], str(payload.get("symbol") or "").upper(), payload.get("period_start")),
                )
                cur.execute(
                    "update crypto_b001_replication_runs set status='running',updated_at=now() where id=%s and status in ('queued','running','completed_with_errors')",
                    (item["run_id"],),
                )
            else:
                cur.execute(
                    """
                    update crypto_b001_replication_runs
                       set status='completed_with_errors',error=coalesce(error,%s),updated_at=now()
                     where id=%s
                    """,
                    (f"Work item failed permanently: {item.get('stage')} {item.get('partition_key')}: {exc}", item["run_id"]),
                )
            conn.commit()

    _checkpoint_with_db_retry(persist, label="permanent-failure")
    logger.error(
        "B-001 permanent failure recorded stage=%s key=%s code=%s; archive_run_continues=%s",
        item.get("stage"), item.get("partition_key"), code, is_archive,
    )
    if is_archive:
        for advance_attempt in range(5):
            try:
                replication.advance_b001_run(UUID(str(item["run_id"])))
                break
            except Exception as advance_exc:
                if not is_transient_db_error(advance_exc):
                    logger.exception("B-001 failed to advance after permanent archive failure")
                    break
                time.sleep(min(2 ** advance_attempt, 8))


def _verify_archive_before_complete(item_id: int) -> dict[str, Any]:
    item = fetch_one(
        "select id,run_id,stage,partition_key,payload from crypto_b001_replication_work_items where id=%s",
        (item_id,),
    )
    if not item or item.get("stage") != "spot_month":
        return {}
    payload = dict(item.get("payload") or {})
    symbol = str(payload.get("symbol") or "").upper()
    period_start = payload.get("period_start")
    archive = fetch_one(
        """
        select source_status,checksum_verified,row_count,first_ts,last_ts,complete_15m_count
          from crypto_b001_replication_archive_files
         where run_id=%s and symbol=%s and period_start=%s::date
        """,
        (item["run_id"], symbol, period_start),
    )
    if not archive:
        raise ArchiveVerificationError(f"archive ledger row missing for {symbol}:{period_start}")
    if archive.get("source_status") != "loaded":
        raise ArchiveVerificationError(f"archive ledger not loaded for {symbol}:{period_start}: {archive.get('source_status')}")
    if archive.get("checksum_verified") is not True:
        raise ArchiveVerificationError(f"checksum not verified for {symbol}:{period_start}")

    row_count = int(archive.get("row_count") or 0)
    canonical_count = 0
    if archive.get("first_ts") is not None and archive.get("last_ts") is not None:
        canonical = fetch_one(
            """
            select count(*)::bigint n
              from market_bars_1m_binance b
              join instruments i on i.id=b.instrument_id
             where b.provider='binance' and i.provider='binance' and i.provider_symbol=%s
               and b.ts between %s and %s
            """,
            (symbol, archive["first_ts"], archive["last_ts"]),
        )
        canonical_count = int((canonical or {}).get("n") or 0)
    if canonical_count < row_count:
        raise ArchiveVerificationError(
            f"canonical insert verification failed for {symbol}:{period_start}: expected_at_least={row_count} found={canonical_count}"
        )

    expected_15m = int(archive.get("complete_15m_count") or 0)
    persisted = fetch_one(
        """
        select count(*)::bigint n
          from crypto_b001_replication_15m
         where run_id=%s and symbol=%s and source_period_start=%s::date
        """,
        (item["run_id"], symbol, period_start),
    )
    persisted_15m = int((persisted or {}).get("n") or 0)
    if persisted_15m != expected_15m:
        raise ArchiveVerificationError(
            f"15m insert verification failed for {symbol}:{period_start}: expected={expected_15m} found={persisted_15m}"
        )
    return {
        "verified": True,
        "verified_at": _utc_now_iso(),
        "canonical_rows_present": canonical_count,
        "archive_rows": row_count,
        "persisted_15m_rows": persisted_15m,
    }


_ORIGINAL_COMPLETE = replication._complete
_ORIGINAL_FAIL = replication._fail
_ORIGINAL_PROCESS = replication.process_b001_work


def _complete(item_id: int, row_count: int = 0, progress: dict | None = None, status: str = "completed") -> None:
    merged = dict(progress or {})
    if status == "completed":
        verification = _verify_archive_before_complete(item_id)
        if verification:
            merged["insert_verification"] = verification
    _ORIGINAL_COMPLETE(item_id, row_count, merged, status)


def _fail(item: dict[str, Any], exc: Exception, code: str = "replication_error") -> None:
    retryable, classified_code = classify_failure(exc)
    if classified_code == "db_transient":
        _record_transient_db_retry(item, exc)
        return
    if retryable:
        _record_retryable_failure(item, exc, classified_code or code)
        return
    _record_permanent_failure(item, exc, classified_code or code)


def _heartbeat_once(item: dict[str, Any]) -> bool:
    locked_by = item.get("locked_by")
    if not locked_by:
        return False
    try:
        with db_connection() as conn, conn.cursor() as cur:
            cur.execute(
                """
                update crypto_b001_replication_work_items
                   set locked_at=now(),updated_at=now(), progress=coalesce(progress,'{}'::jsonb) || %s
                 where id=%s and status='running' and locked_by=%s
                """,
                (Jsonb({"heartbeat_at": _utc_now_iso()}), item["id"], locked_by),
            )
            alive = cur.rowcount == 1
            conn.commit()
            return alive
    except Exception as exc:
        if is_transient_db_error(exc):
            logger.warning("B-001 heartbeat deferred by transient DB failure item=%s: %s", item.get("id"), exc)
            return True
        logger.exception("B-001 heartbeat failed item=%s", item.get("id"))
        return True


def _heartbeat_loop(item: dict[str, Any], stop: threading.Event) -> None:
    while not stop.wait(HEARTBEAT_SECONDS):
        if not _heartbeat_once(item):
            return


def get_operational_metrics(run_id: UUID) -> dict[str, Any]:
    row = fetch_one(
        """
        with w as (
            select
                count(*) filter(where status='completed')::bigint completed,
                count(*) filter(where status='failed')::bigint failed,
                count(*) filter(where status='queued')::bigint queued,
                count(*) filter(where status='retry_wait')::bigint retry_wait,
                count(*) filter(where status='running')::bigint running,
                count(*) filter(where attempts>1)::bigint retried_items,
                coalesce(sum(greatest(attempts-1,0)),0)::bigint consumed_retries,
                coalesce(sum(case when (progress->>'infra_retries') ~ '^[0-9]+$' then (progress->>'infra_retries')::bigint else 0 end),0)::bigint infra_retries,
                count(*) filter(where status='completed' and updated_at>=now()-interval '15 minutes')::bigint completed_15m,
                count(*) filter(where stage='spot_month' and status='completed' and updated_at>=now()-interval '15 minutes')::bigint archives_15m,
                max(updated_at) latest_work_update
            from crypto_b001_replication_work_items where run_id=%s
        ), a as (
            select count(*) filter(where source_status='failed')::bigint permanent_archive_failures
            from crypto_b001_replication_archive_files where run_id=%s
        )
        select w.*,a.permanent_archive_failures from w cross join a
        """,
        (run_id, run_id),
    ) or {}
    completed = int(row.get("completed") or 0)
    failed = int(row.get("failed") or 0)
    attempted_terminal = completed + failed
    latest_work_update = row.get("latest_work_update")
    if isinstance(latest_work_update, datetime):
        latest_work_update = latest_work_update.isoformat()
    return {
        **row,
        "latest_work_update": latest_work_update,
        "items_per_hour_recent": int(row.get("completed_15m") or 0) * 4,
        "archives_per_hour_recent": int(row.get("archives_15m") or 0) * 4,
        "permanent_failure_rate_pct": (100.0 * failed / attempted_terminal) if attempted_terminal else 0.0,
        "retry_item_rate_pct": (100.0 * int(row.get("retried_items") or 0) / attempted_terminal) if attempted_terminal else 0.0,
        "measured_at": _utc_now_iso(),
    }


def _maybe_checkpoint_progress(run_id: UUID) -> None:
    global _last_progress_log
    now = time.monotonic()
    with _progress_lock:
        if now - _last_progress_log < _PROGRESS_LOG_SECONDS:
            return
        _last_progress_log = now
    try:
        metrics = get_operational_metrics(run_id)
        with db_connection() as conn, conn.cursor() as cur:
            cur.execute(
                """
                update crypto_b001_replication_runs
                   set execution_spec=coalesce(execution_spec,'{}'::jsonb) || %s, updated_at=now()
                 where id=%s
                """,
                (Jsonb({"operational_metrics": metrics}), run_id),
            )
            conn.commit()
        logger.info(
            "B-001 progress run=%s completed=%s failed=%s queued=%s retry_wait=%s running=%s items_per_hour=%s archives_per_hour=%s failure_rate=%.4f%% infra_retries=%s",
            run_id, metrics.get("completed"), metrics.get("failed"), metrics.get("queued"), metrics.get("retry_wait"),
            metrics.get("running"), metrics.get("items_per_hour_recent"), metrics.get("archives_per_hour_recent"),
            metrics.get("permanent_failure_rate_pct", 0.0), metrics.get("infra_retries"),
        )
    except Exception:
        logger.exception("Unable to checkpoint B-001 operational metrics run=%s", run_id)


def process_b001_work(item: dict[str, Any]) -> None:
    """Process a durable work item while refreshing its ownership heartbeat."""
    stop = threading.Event()
    heartbeat = threading.Thread(
        target=_heartbeat_loop,
        args=(item, stop),
        name=f"b001-heartbeat-{item.get('id')}",
        daemon=True,
    )
    heartbeat.start()
    try:
        _ORIGINAL_PROCESS(item)
    finally:
        stop.set()
        heartbeat.join(timeout=1.0)
        try:
            _maybe_checkpoint_progress(UUID(str(item["run_id"])))
        except Exception:
            logger.exception("Unable to emit B-001 progress metrics after item=%s", item.get("id"))


def requeue_failed_archives(run_id: UUID) -> int:
    """Explicit later-reprocessing path for permanently failed archive items."""
    with db_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            update crypto_b001_replication_work_items
               set status='retry_wait',attempts=0,not_before=now(),last_error=null,error_code=null,
                   locked_by=null,locked_at=null, progress=coalesce(progress,'{}'::jsonb) || %s, updated_at=now()
             where run_id=%s and stage='spot_month' and status='failed'
            """,
            (Jsonb({"reprocessing_requested_at": _utc_now_iso()}), run_id),
        )
        count = cur.rowcount
        cur.execute(
            """
            update crypto_b001_replication_archive_files set source_status='planned',updated_at=now()
             where run_id=%s and source_status='failed'
            """,
            (run_id,),
        )
        if count:
            cur.execute(
                "update crypto_b001_replication_runs set status='running',error=null,completed_at=null,updated_at=now() where id=%s",
                (run_id,),
            )
        conn.commit()
        return count


replication._complete = _complete
replication._fail = _fail

claim_b001_work = replication.claim_b001_work
reclaim_stale_b001_work = replication.reclaim_stale_b001_work
