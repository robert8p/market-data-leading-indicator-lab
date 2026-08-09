from __future__ import annotations

"""Operational resilience for the long-running locked B-001 replication.

This module changes no signal, feature, universe, execution, cost, or validation
rule.  It only changes how infrastructure-level database connection failures are
handled by the durable worker.

A claimed work item increments its normal attempt counter.  A transient database
connection failure is not a research/data failure, so this layer returns the item
to ``retry_wait`` and reverses that claim increment.  Therefore a temporary pool
or connection outage cannot exhaust ``max_attempts`` and terminate the 24-month
replication.
"""

import logging
import time
from typing import Any

from psycopg import InterfaceError, OperationalError
from psycopg_pool import PoolTimeout

import app.b001_replication as replication
# Importing methodology hardening applies all pre-outcome replication patches
# before the operational wrapper below is installed.
import app.b001_methodology_hardening as methodology  # noqa: F401
from app.db import db_connection


logger = logging.getLogger(__name__)
TRANSIENT_DB_EXCEPTIONS = (PoolTimeout, OperationalError, InterfaceError)


def is_transient_db_error(exc: BaseException) -> bool:
    """Return True only for connection/pool infrastructure failures.

    Walk the exception chain because psycopg may wrap the original connection
    error.  Message matching is deliberately narrow so SQL/data errors continue
    to use the normal finite retry budget.
    """
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
            )
        ):
            return True
        current = current.__cause__ or current.__context__
    return False


def _transient_retry_delay_seconds(item: dict[str, Any]) -> int:
    # Use the already-incremented claim count to spread repeated infrastructure
    # retries, plus deterministic per-item jitter to avoid a thundering herd.
    attempts = max(1, int(item.get("attempts") or 1))
    base = min(240, 15 * (2 ** min(attempts - 1, 4)))
    jitter = int(item.get("id") or 0) % 31
    return base + jitter


def _record_transient_db_retry(item: dict[str, Any], exc: BaseException) -> None:
    delay = _transient_retry_delay_seconds(item)
    last_exc: BaseException | None = None

    # The database may still be recovering when we try to record the retry.
    # Retry the checkpoint write locally; if all writes fail, the work item stays
    # running and the existing stale-lock reclaimer will safely recover it later.
    for checkpoint_attempt in range(5):
        try:
            with db_connection() as conn, conn.cursor() as cur:
                cur.execute(
                    """
                    update crypto_b001_replication_work_items
                       set status='retry_wait',
                           attempts=greatest(attempts-1,0),
                           last_error=%s,
                           error_code='db_transient',
                           locked_by=null,
                           locked_at=null,
                           not_before=now()+(%s * interval '1 second'),
                           updated_at=now()
                     where id=%s
                    """,
                    (f"{type(exc).__name__}: {exc}", delay, item["id"]),
                )
                conn.commit()
            logger.warning(
                "B-001 transient DB failure deferred without consuming retry budget "
                "stage=%s key=%s delay=%ss",
                item.get("stage"),
                item.get("partition_key"),
                delay,
            )
            return
        except Exception as checkpoint_exc:  # pragma: no branch - bounded loop
            last_exc = checkpoint_exc
            if not is_transient_db_error(checkpoint_exc):
                raise
            if checkpoint_attempt < 4:
                time.sleep(min(1.0 * (2**checkpoint_attempt), 8.0))

    assert last_exc is not None
    raise last_exc


_ORIGINAL_FAIL = replication._fail


def _fail(item: dict[str, Any], exc: Exception, code: str = "replication_error") -> None:
    if is_transient_db_error(exc):
        _record_transient_db_retry(item, exc)
        return
    _ORIGINAL_FAIL(item, exc, code)


# process_b001_work resolves the module-global _fail at runtime, so replacing it
# here hardens every B-001 stage without changing replication methodology.
replication._fail = _fail

claim_b001_work = replication.claim_b001_work
process_b001_work = replication.process_b001_work
reclaim_stale_b001_work = replication.reclaim_stale_b001_work
