from __future__ import annotations

import json
import logging
import os
import signal
import socket
import threading
import time
from typing import Any

from app.db import db_connection, fetch_one, get_pool


logging.basicConfig(
    level=getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger(__name__)
shutdown_event = threading.Event()


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default


def _worker_id() -> str:
    return f"research:{socket.gethostname()}:{os.getpid()}"


def _handle_signal(signum: int, _frame: Any) -> None:
    logger.warning("Received signal %s; research worker will stop after the current database task", signum)
    shutdown_event.set()


def should_finalize(status_counts: dict[str, int]) -> bool:
    """Return true only when a chunked run has at least one completed task and no incomplete/failing tasks."""
    completed = int(status_counts.get("completed", 0))
    blockers = sum(int(status_counts.get(key, 0)) for key in ("queued", "running", "failed"))
    return completed > 0 and blockers == 0


def _claim_task(worker_id: str) -> dict[str, Any] | None:
    row = fetch_one(
        "select * from research_hub.claim_dispatchable_experiment_task_v1(%s)",
        (worker_id,),
    )
    if not row or row.get("task_id") is None:
        return None
    return row


def _run_task(task: dict[str, Any], worker_id: str) -> dict[str, Any]:
    """Run one atomic database-owned screen with a bounded long statement timeout.

    The claim is committed before this function starts. If the client connection
    disappears mid-query, PostgreSQL rolls back the task execution transaction and
    the durable stale-task reclaimer can return the claimed task to the queue.
    """
    timeout_minutes = max(5, _env_int("RESEARCH_TASK_TIMEOUT_MINUTES", 45))
    with db_connection() as conn, conn.cursor() as cur:
        cur.execute("set local statement_timeout = %s", (f"{timeout_minutes}min",))
        cur.execute(
            "select research_hub.run_feature_screen_task(%s,%s) as result",
            (task["task_id"], worker_id),
        )
        row = cur.fetchone()
        conn.commit()
    result = dict((row or {}).get("result") or {})
    logger.info(
        "Research task finished id=%s run=%s key=%s status=%s result=%s",
        task.get("task_id"), task.get("run_id"), task.get("task_key"),
        result.get("status"), json.dumps(result, sort_keys=True),
    )
    return result


def _status_counts(run_id: str) -> dict[str, int]:
    rows = fetch_one(
        """
        select
          count(*) filter(where status='completed')::int as completed,
          count(*) filter(where status='queued')::int as queued,
          count(*) filter(where status='running')::int as running,
          count(*) filter(where status='failed')::int as failed
        from research_hub.experiment_tasks where run_id=%s
        """,
        (run_id,),
    ) or {}
    return {key: int(rows.get(key) or 0) for key in ("completed", "queued", "running", "failed")}


def _maybe_finalize(run_id: str) -> dict[str, Any] | None:
    counts = _status_counts(run_id)
    if not should_finalize(counts):
        return None
    row = fetch_one(
        "select research_hub.finalize_chunked_screen(%s) as result",
        (run_id,),
    )
    result = dict((row or {}).get("result") or {})
    logger.info("Finalized chunked research run=%s result=%s", run_id, json.dumps(result, sort_keys=True))
    return result


def _reclaim_stale() -> int:
    stale_minutes = max(10, _env_int("RESEARCH_STALE_TASK_MINUTES", 60))
    row = fetch_one(
        "select research_hub.reclaim_stale_experiment_tasks(make_interval(mins=>%s)) as reclaimed",
        (stale_minutes,),
    ) or {}
    return int(row.get("reclaimed") or 0)


def main() -> None:
    if os.getenv("RESEARCH_WORKER_ENABLED", "false").strip().lower() not in {"1", "true", "yes", "on"}:
        raise RuntimeError("RESEARCH_WORKER_ENABLED is not true; refusing to start a research-only worker")

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)
    worker_id = _worker_id()
    poll_seconds = max(1.0, _env_float("RESEARCH_WORKER_POLL_SECONDS", 10.0))
    reclaim_seconds = max(60.0, _env_float("RESEARCH_RECLAIM_SECONDS", 300.0))
    last_reclaim = 0.0
    logger.info("Research worker started id=%s", worker_id)

    try:
        while not shutdown_event.is_set():
            now_mono = time.monotonic()
            if now_mono - last_reclaim >= reclaim_seconds:
                try:
                    reclaimed = _reclaim_stale()
                    if reclaimed:
                        logger.warning("Reclaimed %s stale research task(s)", reclaimed)
                except Exception:
                    logger.exception("Research stale-task reclaim failed; worker will continue")
                last_reclaim = now_mono

            try:
                task = _claim_task(worker_id)
            except Exception:
                logger.exception("Research task claim failed; retrying after poll interval")
                shutdown_event.wait(poll_seconds)
                continue

            if not task:
                shutdown_event.wait(poll_seconds)
                continue

            try:
                result = _run_task(task, worker_id)
                if result.get("status") in {"completed", "already_completed"}:
                    _maybe_finalize(str(task["run_id"]))
                elif result.get("status") == "failed":
                    logger.error("Research task %s failed inside the deterministic engine: %s", task["task_id"], result)
            except Exception:
                # Do not mutate the research result after an uncertain client-side
                # failure. The committed claim remains durable and stale reclaim
                # will safely retry if the task did not commit server-side.
                logger.exception("Research task %s escaped due to client/infrastructure failure", task.get("task_id"))
    finally:
        logger.info("Research worker stopping id=%s", worker_id)
        try:
            get_pool().close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
