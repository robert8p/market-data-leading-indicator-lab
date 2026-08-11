from __future__ import annotations

"""Operational resilience for long-running collection partitions.

This module changes no market-data selection, research rule, signal, feature,
validation, or cost assumption. It hardens only the durable execution layer:

- transient database/network/provider infrastructure failures use an independent
  retry counter and do not exhaust the normal data/logic failure budget;
- retries use exponential backoff with deterministic jitter;
- claimed checkpoints are structurally validated before work resumes;
- invalid checkpoints reset only the affected partition, relying on idempotent
  inserts/upserts to replay that partition safely;
- operational failures are written to a separate research failure ledger.

Completed partitions are never reopened by this layer.
"""

import logging
from datetime import datetime, timedelta
from typing import Any

from app import jobs
from app.db import db_connection


logger = logging.getLogger(__name__)

_TRANSIENT_CODES = {
    "timeout",
    "network",
    "rate_limit",
    "http_408",
    "http_409",
    "http_425",
    "http_500",
    "http_502",
    "http_503",
    "http_504",
}

_TRANSIENT_MARKERS = (
    "statement timeout",
    "canceling statement due to statement timeout",
    "couldn't get a connection",
    "could not get a connection",
    "connection timeout",
    "connection timed out",
    "connection is closed",
    "connection reset",
    "connection refused",
    "server closed the connection unexpectedly",
    "pool timeout",
    "deadlock detected",
    "could not serialize access",
    "timeout calling",
    "network error calling",
    "temporary failure in name resolution",
    "name or service not known",
)

_ORIGINAL_CLAIM = jobs.claim_collection_partition
_ORIGINAL_RETRY_OR_FAIL = jobs.retry_or_fail_partition


def _failure_class(code: str | None, message: str, retryable: bool) -> str:
    if retryable and _is_transient_failure(code, message):
        return "infrastructure"
    if code and (
        code.startswith("http_")
        or code in {"rate_limit", "invalid_json", "provider_error"}
    ):
        return "provider"
    return "data_or_logic"


def _is_transient_failure(code: str | None, message: str) -> bool:
    if (code or "").lower() in _TRANSIENT_CODES:
        return True
    lowered = (message or "").lower()
    return any(marker in lowered for marker in _TRANSIENT_MARKERS)


def _jitter_seconds(partition_id: Any) -> int:
    token = str(partition_id).replace("-", "")
    try:
        return int(token[-6:], 16) % 31
    except Exception:
        return sum(ord(ch) for ch in token) % 31


def _record_failure(
    cur: Any,
    partition: dict[str, Any],
    *,
    failure_class: str,
    error_code: str | None,
    message: str,
    retryable: bool,
    retry_scheduled: bool,
) -> None:
    cur.execute(
        """
        insert into research.collection_failure_ledger(
            partition_id,run_id,provider,data_type,provider_symbol,failure_class,
            error_code,error_message,retryable,retry_scheduled
        ) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """,
        (
            partition["id"],
            partition["run_id"],
            partition.get("provider") or "unknown",
            partition.get("data_type") or "unknown",
            partition.get("provider_symbol"),
            failure_class,
            error_code,
            (message or "")[:4000],
            retryable,
            retry_scheduled,
        ),
    )


def _checkpoint_times(cursor: dict[str, Any]) -> tuple[datetime | None, datetime | None]:
    first = datetime.fromisoformat(cursor["first_ts"]) if cursor.get("first_ts") else None
    last = datetime.fromisoformat(cursor["last_ts"]) if cursor.get("last_ts") else None
    return first, last


def _checkpoint_problem(partition: dict[str, Any]) -> str | None:
    """Return a reason only when a persisted checkpoint cannot safely resume."""
    cursor = dict(partition.get("cursor") or {})
    row_count = int(partition.get("row_count") or 0)
    if row_count < 0:
        return "negative row_count"

    # A newly planned partition may legitimately carry capture metadata/feed in
    # cursor before its first page. That is configuration, not a progress cursor.
    if row_count == 0 and not cursor.get("first_ts") and not cursor.get("last_ts"):
        return None

    try:
        first_ts, last_ts = _checkpoint_times(cursor)
    except (TypeError, ValueError) as exc:
        return f"invalid checkpoint timestamp: {exc}"

    if row_count > 0 and (first_ts is None or last_ts is None):
        return "row_count>0 without first_ts/last_ts"
    if first_ts and last_ts and first_ts > last_ts:
        return "first_ts is after last_ts"

    start_ts = partition.get("start_ts")
    end_ts = partition.get("end_ts")
    tolerance = timedelta(minutes=1)
    if first_ts and start_ts and first_ts < start_ts - tolerance:
        return "first_ts precedes partition window"
    if last_ts and end_ts and last_ts > end_ts + tolerance:
        return "last_ts exceeds partition window"

    if row_count > 0 and not cursor.get("finished") and not cursor.get("next_page_token"):
        return "partial checkpoint has no continuation token"

    if row_count > 0 and partition.get("data_type") in {"quotes", "trades", "bars_1m"}:
        table = {
            "quotes": "market_quotes_l1",
            "trades": "market_trades",
            "bars_1m": "market_bars_1m",
        }[partition["data_type"]]
        with db_connection() as conn, conn.cursor() as cur:
            cur.execute(
                f"""
                select count(*) as n,min(ts) as min_ts,max(ts) as max_ts
                  from {table}
                 where provider=%s and instrument_id=%s
                   and ts >= %s and ts < %s
                """,
                (
                    partition["provider"],
                    partition["instrument_id"],
                    partition["start_ts"],
                    partition["end_ts"],
                ),
            )
            observed = cur.fetchone()
            conn.commit()
        if not observed or int(observed["n"] or 0) == 0:
            return "checkpoint records rows but durable data is absent"

    return None


def _base_cursor(partition: dict[str, Any]) -> dict[str, Any]:
    existing = dict(partition.get("cursor") or {})
    return {
        key: existing[key]
        for key in ("feed", "capture_window_ids")
        if key in existing
    }


def validate_claimed_checkpoint(partition: dict[str, Any]) -> dict[str, Any]:
    """Validate a claimed item; reset only that item if its checkpoint is invalid."""
    problem = _checkpoint_problem(partition)
    if problem is None:
        with db_connection() as conn, conn.cursor() as cur:
            cur.execute(
                """
                update collection_partitions
                   set last_checkpoint_validated_at=now(),
                       last_checkpoint_validation='valid',updated_at=now()
                 where id=%s
                """,
                (partition["id"],),
            )
            conn.commit()
        return partition

    base_cursor = _base_cursor(partition)
    with db_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            update collection_partitions
               set cursor=%s,row_count=0,checksum=null,
                   checkpoint_reset_count=checkpoint_reset_count+1,
                   last_checkpoint_validated_at=now(),
                   last_checkpoint_validation='reset',
                   last_error=%s,error_code='checkpoint_reset',updated_at=now()
             where id=%s
            """,
            (jobs.Jsonb(base_cursor), problem[:4000], partition["id"]),
        )
        _record_failure(
            cur,
            partition,
            failure_class="checkpoint",
            error_code="checkpoint_reset",
            message=problem,
            retryable=True,
            retry_scheduled=False,
        )
        conn.commit()

    partition["cursor"] = base_cursor
    partition["row_count"] = 0
    partition["checksum"] = None
    partition["checkpoint_reset_count"] = int(partition.get("checkpoint_reset_count") or 0) + 1
    logger.warning(
        "Reset invalid collection checkpoint partition=%s provider=%s type=%s symbol=%s reason=%s",
        partition["id"],
        partition.get("provider"),
        partition.get("data_type"),
        partition.get("provider_symbol"),
        problem,
    )
    return partition


def claim_collection_partition(worker_id: str) -> dict[str, Any] | None:
    partition = _ORIGINAL_CLAIM(worker_id)
    if not partition:
        return None
    return validate_claimed_checkpoint(dict(partition))


def retry_or_fail_partition(
    partition: dict[str, Any],
    message: str,
    code: str | None,
    retry_at: datetime | None,
    retryable: bool,
) -> None:
    """Keep infrastructure failures separate from research/data failure budget."""
    if retryable and _is_transient_failure(code, message):
        retry_number = int(partition.get("infrastructure_retry_count") or 0) + 1
        delay = min(3600, 15 * (2 ** min(retry_number - 1, 8)))
        delay += _jitter_seconds(partition["id"])
        not_before = jobs.utc_now() + timedelta(seconds=delay)
        if retry_at and retry_at > not_before:
            not_before = retry_at

        with db_connection() as conn, conn.cursor() as cur:
            cur.execute(
                """
                update collection_partitions
                   set status='retry_wait',not_before=%s,
                       attempts=greatest(attempts-1,0),
                       infrastructure_retry_count=infrastructure_retry_count+1,
                       last_error=%s,error_code=%s,
                       locked_by=null,locked_at=null,heartbeat_at=now(),updated_at=now()
                 where id=%s
                 returning run_id
                """,
                (
                    not_before,
                    (message or "")[:4000],
                    f"infra_transient:{code or 'unknown'}"[:255],
                    partition["id"],
                ),
            )
            row = cur.fetchone()
            _record_failure(
                cur,
                partition,
                failure_class="infrastructure",
                error_code=code,
                message=message,
                retryable=True,
                retry_scheduled=True,
            )
            if row:
                cur.execute("select refresh_collection_run_counts(%s)", (row["run_id"],))
            conn.commit()
        logger.warning(
            "Transient collection infrastructure failure deferred without consuming failure budget "
            "partition=%s provider=%s type=%s symbol=%s retry=%s delay=%ss code=%s",
            partition["id"],
            partition.get("provider"),
            partition.get("data_type"),
            partition.get("provider_symbol"),
            retry_number,
            delay,
            code,
        )
        return

    failure_class = _failure_class(code, message, retryable)
    _ORIGINAL_RETRY_OR_FAIL(partition, message, code, retry_at, retryable)
    try:
        with db_connection() as conn, conn.cursor() as cur:
            _record_failure(
                cur,
                partition,
                failure_class=failure_class,
                error_code=code,
                message=message,
                retryable=retryable,
                retry_scheduled=retryable,
            )
            conn.commit()
    except Exception:
        # Failure-ledger recording must never turn a recoverable item failure into
        # a worker-wide failure. The partition state above remains authoritative.
        logger.exception("Could not append collection failure ledger entry partition=%s", partition.get("id"))


jobs.claim_collection_partition = claim_collection_partition
jobs.retry_or_fail_partition = retry_or_fail_partition
