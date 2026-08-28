from __future__ import annotations

import logging
import os
import threading
from typing import Any

from app.db import db_connection


logger = logging.getLogger(__name__)


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int, minimum: int, maximum: int) -> int:
    try:
        value = int(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        value = default
    return min(maximum, max(minimum, value))


def _run_cycle(worker_id: str) -> dict[str, Any] | None:
    """Run one factory cycle outside the Data API/pg_cron timeout envelope.

    PostgreSQL starts the statement timeout before entering a called function, so
    changing it from inside the function cannot reliably save a long discovery
    statement. The worker therefore sets the transaction-local timeout first and
    invokes the factory in a second statement on the same direct DB session.
    """
    statement_timeout_ms = _env_int(
        "STRATEGY_FACTORY_STATEMENT_TIMEOUT_MS",
        2_700_000,
        60_000,
        7_200_000,
    )
    lock_timeout_ms = _env_int(
        "STRATEGY_FACTORY_LOCK_TIMEOUT_MS",
        30_000,
        1_000,
        300_000,
    )

    with db_connection() as conn, conn.cursor() as cur:
        cur.execute("select to_regprocedure('research_hub.run_strategy_factory_automation_v1()') as function")
        available = cur.fetchone()
        if not available or not available.get("function"):
            conn.rollback()
            return None

        cur.execute("set local statement_timeout = %s", (statement_timeout_ms,))
        cur.execute("set local lock_timeout = %s", (lock_timeout_ms,))
        cur.execute(
            "select research_hub.run_strategy_factory_automation_v1() as result, %s::text as worker_id",
            (worker_id,),
        )
        row = cur.fetchone()
        conn.commit()
        return row.get("result") if row else None


def run_strategy_factory_loop(shutdown_event: threading.Event, worker_id: str) -> None:
    """Continuously advance the zero-capital strategy factory on the Render worker."""
    enabled = _env_bool("STRATEGY_FACTORY_AUTOMATION_ENABLED", True)
    idle_seconds = _env_int("STRATEGY_FACTORY_IDLE_SECONDS", 30, 5, 600)
    active_seconds = _env_int("STRATEGY_FACTORY_ACTIVE_SECONDS", 5, 1, 120)
    error_seconds = _env_int("STRATEGY_FACTORY_ERROR_SECONDS", 60, 5, 900)

    logger.info(
        "Strategy-factory worker lane starting id=%s enabled=%s idle_seconds=%s active_seconds=%s",
        worker_id,
        enabled,
        idle_seconds,
        active_seconds,
    )
    if not enabled:
        return

    while not shutdown_event.is_set():
        try:
            result = _run_cycle(worker_id)
            if result is None:
                logger.info("Strategy-factory schema is not yet available; retrying")
                shutdown_event.wait(error_seconds)
                continue

            first = result.get("first_advance") or {}
            second = result.get("second_advance") or {}
            approvals = result.get("automatic_approvals") or {}
            notifications = result.get("notifications") or {}
            did_work = bool(
                first.get("claimed")
                or second.get("claimed")
                or int(approvals.get("approved") or 0) > 0
                or int(notifications.get("inserted") or 0) > 0
            )
            if did_work:
                logger.info("Strategy-factory cycle completed result=%s", result)
                shutdown_event.wait(active_seconds)
            else:
                shutdown_event.wait(idle_seconds)
        except Exception:
            logger.exception("Strategy-factory worker lane failed a cycle; durable state will be retried")
            shutdown_event.wait(error_seconds)

    logger.info("Strategy-factory worker lane stopping id=%s", worker_id)
