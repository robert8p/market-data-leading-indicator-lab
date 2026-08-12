from __future__ import annotations

"""Forward-holdout semantics for the chronology-corrected B-001 rule.

Historical-replication QA expects roughly a year of older unseen history and
checks that the replication ends before the original discovery window.  A
prospective holdout has the opposite chronology: it must begin only after the
methodology freeze.  This patch changes only those audit semantics when a run
is explicitly tagged `purpose=forward_holdout` in execution_spec.
"""

from datetime import timedelta
from typing import Any
from uuid import UUID

from psycopg.types.json import Jsonb

import app.b001_analysis as analysis
import app.b001_replication as replication
from app.b001_contract import HOLD_HOURS
from app.db import db_connection, fetch_one


_ORIGINAL_GENERATE_SIGNALS = replication._generate_signals
_ORIGINAL_QA_CHECKS = analysis._qa_checks


def _is_forward_holdout(run: dict[str, Any] | None) -> bool:
    return bool(run and (run.get("execution_spec") or {}).get("purpose") == "forward_holdout")


def _generate_signals(item: dict[str, Any]) -> None:
    _ORIGINAL_GENERATE_SIGNALS(item)
    run = fetch_one(
        "select requested_end,execution_spec from crypto_b001_replication_runs where id=%s",
        (item["run_id"],),
    )
    if not _is_forward_holdout(run):
        return

    # Signal bar closes 15m after bucket_start; execution is another 15m later.
    cutoff = run["requested_end"] - timedelta(hours=HOLD_HOURS, minutes=30)
    with db_connection() as conn, conn.cursor() as cur:
        cur.execute(
            "delete from crypto_b001_replication_signals where run_id=%s and bucket_start >= %s",
            (item["run_id"], cutoff),
        )
        removed = cur.rowcount
        if removed:
            cur.execute(
                """
                update crypto_b001_replication_work_items
                   set row_count=greatest(0,row_count-%s),
                       progress=progress || %s::jsonb,
                       updated_at=now()
                 where id=%s
                """,
                (
                    removed,
                    Jsonb({
                        "forward_holdout_terminal_horizon_removed": removed,
                        "rule": "signal bucket must be < requested_end - 8h30m for 15m post-signal delay plus frozen 8h hold",
                    }),
                    item["id"],
                ),
            )
        conn.commit()


def _qa_checks(run_id: UUID):
    checks = _ORIGINAL_QA_CHECKS(run_id)
    run = fetch_one(
        "select requested_start,requested_end,effective_start,effective_end,discovery_end,execution_spec from crypto_b001_replication_runs where id=%s",
        (run_id,),
    )
    if not _is_forward_holdout(run):
        return checks

    spec = run.get("execution_spec") or {}
    freeze_text = spec.get("methodology_freeze_end")
    freeze_end = run["discovery_end"]
    if freeze_text:
        from datetime import datetime
        freeze_end = datetime.fromisoformat(str(freeze_text).replace("Z", "+00:00"))

    chronology_pass = run["requested_start"] >= freeze_end
    effective_start = max(run.get("effective_start") or run["requested_start"], run["requested_start"])
    effective_end = min(run.get("effective_end") or run["requested_end"], run["requested_end"])
    coverage_days = max(0.0, (effective_end - effective_start).total_seconds() / 86400.0)
    minimum_days = float(spec.get("minimum_forward_holdout_days") or 10)
    coverage_pass = coverage_days >= minimum_days

    replacements = {
        1: {
            "name": "Forward holdout begins after methodology freeze",
            "passed": chronology_pass,
            "details": {
                "requested_start": run["requested_start"].isoformat(),
                "methodology_freeze_end": freeze_end.isoformat(),
                "purpose": "forward_holdout",
            },
        },
        13: {
            "name": "Minimum forward holdout coverage",
            "passed": coverage_pass,
            "details": {
                "coverage_days": coverage_days,
                "minimum_forward_holdout_days": minimum_days,
                "requested_end": run["requested_end"].isoformat(),
            },
        },
    }
    with db_connection() as conn, conn.cursor() as cur:
        for number, replacement in replacements.items():
            cur.execute(
                """
                update crypto_b001_replication_qa
                   set check_name=%s,passed=%s,details=%s,checked_at=now()
                 where run_id=%s and check_number=%s
                """,
                (
                    replacement["name"], replacement["passed"],
                    Jsonb(replacement["details"]), run_id, number,
                ),
            )
        conn.commit()

    for check in checks:
        replacement = replacements.get(check["number"])
        if replacement:
            check.update(replacement)
    return checks


replication._generate_signals = _generate_signals
analysis._qa_checks = _qa_checks
