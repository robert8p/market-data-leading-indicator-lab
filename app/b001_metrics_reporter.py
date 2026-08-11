from __future__ import annotations

import logging
import threading
import time
from typing import Any

from psycopg.types.json import Jsonb

from app.db import db_connection, fetch_all, fetch_one


logger = logging.getLogger(__name__)
_INTERVAL_SECONDS = 60.0
_started = False
_start_lock = threading.Lock()


def _utc_measurement_sql() -> str:
    return "to_char(clock_timestamp() at time zone 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"')"


def collect_metrics(run_id: Any) -> dict[str, Any]:
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
                coalesce(sum(case
                    when (progress->>'infra_retries') ~ '^[0-9]+$'
                    then (progress->>'infra_retries')::bigint else 0 end),0)::bigint infra_retries,
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
    terminal = completed + failed
    retried_items = int(row.get("retried_items") or 0)
    return {
        **row,
        "items_per_hour_recent": int(row.get("completed_15m") or 0) * 4,
        "archives_per_hour_recent": int(row.get("archives_15m") or 0) * 4,
        "permanent_failure_rate_pct": 100.0 * failed / terminal if terminal else 0.0,
        "retry_item_rate_pct": 100.0 * retried_items / terminal if terminal else 0.0,
    }


def persist_metrics(run_id: Any) -> dict[str, Any]:
    metrics = collect_metrics(run_id)
    with db_connection() as conn, conn.cursor() as cur:
        cur.execute(
            f"select {_utc_measurement_sql()} measured_at"
        )
        measured_at = cur.fetchone()["measured_at"]
        metrics["measured_at"] = measured_at
        cur.execute(
            """
            update crypto_b001_replication_runs
               set execution_spec=coalesce(execution_spec,'{}'::jsonb) || %s,
                   updated_at=now()
             where id=%s
            """,
            (Jsonb({"operational_metrics": metrics}), run_id),
        )
        conn.commit()
    logger.info(
        "B-001 live progress run=%s completed=%s failed=%s queued=%s retry_wait=%s running=%s "
        "items_per_hour=%s archives_per_hour=%s failure_rate=%.4f%% retry_rate=%.4f%% infra_retries=%s",
        run_id,
        metrics.get("completed"), metrics.get("failed"), metrics.get("queued"),
        metrics.get("retry_wait"), metrics.get("running"), metrics.get("items_per_hour_recent"),
        metrics.get("archives_per_hour_recent"), metrics.get("permanent_failure_rate_pct", 0.0),
        metrics.get("retry_item_rate_pct", 0.0), metrics.get("infra_retries"),
    )
    return metrics


def _report_loop(stop: threading.Event) -> None:
    while not stop.is_set():
        try:
            runs = fetch_all(
                """
                select id from crypto_b001_replication_runs
                 where status in ('queued','running')
                 order by created_at
                """
            )
            for run in runs:
                persist_metrics(run["id"])
        except Exception:
            logger.exception("B-001 live metrics reporter cycle failed; next cycle will retry")
        stop.wait(_INTERVAL_SECONDS)


def start_background() -> None:
    global _started
    with _start_lock:
        if _started:
            return
        _started = True
        stop = threading.Event()
        thread = threading.Thread(
            target=_report_loop,
            args=(stop,),
            name="b001-live-metrics",
            daemon=True,
        )
        thread.start()
