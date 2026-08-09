from __future__ import annotations

"""Lightweight, durable data-quality/readiness checks for completed collection runs.

The original schema already included ``data_quality_results`` but nothing populated it.
This module completes that design without scanning the very large raw market tables.
It uses durable collection-partition metadata so a readiness check is cheap enough to
run automatically when a collection reaches the ``ready`` stage.
"""

from collections import Counter
from datetime import datetime, timezone
from typing import Iterable
from uuid import UUID

from psycopg.types.json import Jsonb

from app.db import db_connection, fetch_all, fetch_one


QUALITY_VERSION = "run-readiness-v1"
PRIMARY_BAR_PROVIDERS = {"alpaca", "coinbase", "binance"}


def summarize_severities(severities: Iterable[str]) -> dict[str, object]:
    counts = Counter(str(value).lower() for value in severities)
    errors = int(counts.get("error", 0))
    warnings = int(counts.get("warning", 0))
    infos = int(counts.get("info", 0))
    status = "fail" if errors else ("review" if warnings else "pass")
    return {
        "status": status,
        "analysis_ready": status == "pass",
        "errors": errors,
        "warnings": warnings,
        "infos": infos,
    }


def _check(provider: str, name: str, severity: str, **details: object) -> dict[str, object]:
    return {
        "provider": provider,
        "check_name": name,
        "severity": severity,
        "details": details,
    }


def run_data_quality_checks(run_id: UUID) -> dict[str, object]:
    """Populate the existing quality table and persist an analysis-readiness gate.

    The checks deliberately avoid full-table scans of bars/trades/quotes. Primary-key
    constraints already prevent duplicate minute bars; this gate instead validates
    durable workload completion, provider coverage and the presence of usable rows.
    """
    run = fetch_one("select * from collection_runs where id=%s", (run_id,))
    if not run:
        raise ValueError(f"Collection run {run_id} does not exist")

    partitions = fetch_all(
        """
        select provider,data_type,
               count(*) as total,
               count(*) filter (where status='completed') as completed,
               count(*) filter (where status='completed_empty') as completed_empty,
               count(*) filter (where status='failed') as failed,
               count(*) filter (where status='skipped') as skipped,
               count(*) filter (where status in ('queued','retry_wait','running')) as active,
               coalesce(sum(row_count),0) as rows,
               min(start_ts) filter (where row_count > 0) as first_partition_start,
               max(end_ts) filter (where row_count > 0) as last_partition_end
          from collection_partitions
         where run_id=%s
         group by provider,data_type
         order by provider,data_type
        """,
        (run_id,),
    )

    checks: list[dict[str, object]] = []
    total_active = sum(int(row["active"] or 0) for row in partitions)
    total_failed = sum(int(row["failed"] or 0) for row in partitions)
    total_skipped = sum(int(row["skipped"] or 0) for row in partitions)
    total_rows = sum(int(row["rows"] or 0) for row in partitions)

    checks.append(
        _check(
            "system",
            "all_partitions_terminal",
            "error" if total_active else "info",
            active_partitions=total_active,
            expected=0,
        )
    )
    checks.append(
        _check(
            "system",
            "failed_partitions",
            "error" if total_failed else "info",
            failed_partitions=total_failed,
            expected=0,
        )
    )
    checks.append(
        _check(
            "system",
            "skipped_partitions",
            "warning" if total_skipped else "info",
            skipped_partitions=total_skipped,
            expected=0,
        )
    )
    checks.append(
        _check(
            "system",
            "committed_rows_present",
            "error" if total_rows <= 0 else "info",
            rows=total_rows,
        )
    )

    by_key = {(str(row["provider"]), str(row["data_type"])): row for row in partitions}
    for provider in list(run.get("providers") or []):
        provider = str(provider)
        bars = by_key.get((provider, "bars_1m"))
        if not bars:
            severity = "error" if provider in PRIMARY_BAR_PROVIDERS else "warning"
            checks.append(
                _check(provider, "one_minute_bar_coverage", severity, reason="no bars_1m partitions found")
            )
            continue

        bar_rows = int(bars["rows"] or 0)
        active = int(bars["active"] or 0)
        failed = int(bars["failed"] or 0)
        skipped = int(bars["skipped"] or 0)
        total = int(bars["total"] or 0)
        completed = int(bars["completed"] or 0)
        completed_empty = int(bars["completed_empty"] or 0)
        severity = "info"
        if active or failed:
            severity = "error"
        elif bar_rows <= 0:
            severity = "error" if provider in PRIMARY_BAR_PROVIDERS else "warning"
        elif skipped:
            severity = "warning"

        checks.append(
            _check(
                provider,
                "one_minute_bar_coverage",
                severity,
                partitions=total,
                completed=completed,
                completed_empty=completed_empty,
                active=active,
                failed=failed,
                skipped=skipped,
                rows=bar_rows,
                first_partition_start=(
                    bars["first_partition_start"].isoformat() if bars.get("first_partition_start") else None
                ),
                last_partition_end=(
                    bars["last_partition_end"].isoformat() if bars.get("last_partition_end") else None
                ),
            )
        )

    capture = fetch_one(
        """
        select count(*) as windows,
               count(*) filter (where planned=true) as admitted,
               count(*) filter (where planned=false) as excluded
          from capture_windows where run_id=%s
        """,
        (run_id,),
    ) or {"windows": 0, "admitted": 0, "excluded": 0}
    checks.append(
        _check(
            "miner",
            "capture_window_audit",
            "info",
            windows=int(capture["windows"] or 0),
            admitted=int(capture["admitted"] or 0),
            excluded=int(capture["excluded"] or 0),
        )
    )

    aggregate = by_key.get(("miner", "equity_microstructure_aggregate"))
    if aggregate:
        checks.append(
            _check(
                "miner",
                "microstructure_aggregation",
                "error" if int(aggregate["active"] or 0) or int(aggregate["failed"] or 0) else "info",
                partitions=int(aggregate["total"] or 0),
                completed=int(aggregate["completed"] or 0),
                completed_empty=int(aggregate["completed_empty"] or 0),
                rows=int(aggregate["rows"] or 0),
                active=int(aggregate["active"] or 0),
                failed=int(aggregate["failed"] or 0),
            )
        )

    summary = summarize_severities(check["severity"] for check in checks)
    checked_at = datetime.now(timezone.utc).isoformat()
    gate = {
        **summary,
        "version": QUALITY_VERSION,
        "checked_at": checked_at,
        "run_id": str(run_id),
        "stage": str(run.get("stage") or ""),
        "collection_status": str(run.get("status") or ""),
        "rows_assessed": total_rows,
        "checks": len(checks),
    }

    with db_connection() as conn, conn.cursor() as cur:
        cur.execute("delete from data_quality_results where run_id=%s", (run_id,))
        for check in checks:
            cur.execute(
                """
                insert into data_quality_results(run_id,provider,check_name,severity,details)
                values (%s,%s,%s,%s,%s)
                """,
                (
                    run_id,
                    check["provider"],
                    check["check_name"],
                    check["severity"],
                    Jsonb(check["details"]),
                ),
            )
        cur.execute(
            """
            update collection_runs
               set config=jsonb_set(coalesce(config,'{}'::jsonb),'{quality_gate}',%s,true)
             where id=%s
            """,
            (Jsonb(gate), run_id),
        )
        conn.commit()
    return gate


def run_ready_quality_checks(limit: int = 5) -> int:
    """Quality-check newly completed ready runs once per completion cycle."""
    rows = fetch_all(
        """
        select cr.id
          from collection_runs cr
          left join lateral (
              select max(created_at) as last_quality_check
                from data_quality_results dqr
               where dqr.run_id=cr.id
          ) q on true
         where cr.stage='ready'
           and cr.status in ('completed','completed_with_errors')
           and (
               q.last_quality_check is null
               or q.last_quality_check < coalesce(cr.enhancement_completed_at,cr.completed_at,cr.updated_at)
           )
         order by cr.completed_at nulls last,cr.created_at
         limit %s
        """,
        (limit,),
    )
    for row in rows:
        run_data_quality_checks(row["id"])
    return len(rows)
