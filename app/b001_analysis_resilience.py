from __future__ import annotations

"""Durable phase checkpoints for the locked B-001 final analysis.

The statistical functions remain the existing frozen functions. This wrapper only
avoids repeating already completed phases after a deploy, worker restart, or
transient infrastructure failure.
"""

from typing import Any
from uuid import UUID

from psycopg.types.json import Jsonb

import app.b001_analysis as analysis
from app.db import db_connection, fetch_all, fetch_one


_ORIGINAL_RUN_FULL_ANALYSIS = analysis.run_full_analysis


def _analysis_item(run_id: UUID) -> dict[str, Any]:
    row = fetch_one(
        """
        select id,progress from crypto_b001_replication_work_items
         where run_id=%s and stage='analysis' and partition_key='full'
         order by id desc limit 1
        """,
        (run_id,),
    )
    if not row:
        raise RuntimeError("B-001 analysis work item is missing")
    return row


def _checkpoint(item_id: int, phase: str, **details: Any) -> None:
    patch = {
        f"analysis_{phase}_complete": True,
        "analysis_last_completed_phase": phase,
        **details,
    }
    with db_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            update crypto_b001_replication_work_items
               set progress=coalesce(progress,'{}'::jsonb) || %s,updated_at=now()
             where id=%s
            """,
            (Jsonb(patch), item_id),
        )
        conn.commit()


def _set_stage(run_id: UUID, stage: str) -> None:
    with db_connection() as conn, conn.cursor() as cur:
        cur.execute(
            "update crypto_b001_replication_runs set stage=%s,updated_at=now() where id=%s",
            (stage, run_id),
        )
        conn.commit()


def _load_robustness(run_id: UUID) -> dict[str, dict]:
    rows = fetch_all(
        "select robustness_type,variant,metrics from crypto_b001_replication_robustness where run_id=%s",
        (run_id,),
    )
    return {f"{row['robustness_type']}:{row['variant']}": row["metrics"] for row in rows}


def _load_qa(run_id: UUID) -> list[dict[str, Any]]:
    rows = fetch_all(
        "select check_number,check_name,passed,details from crypto_b001_replication_qa where run_id=%s order by check_number",
        (run_id,),
    )
    return [
        {
            "number": int(row["check_number"]),
            "name": row["check_name"],
            "passed": bool(row["passed"]),
            "details": row.get("details") or {},
        }
        for row in rows
    ]


def run_full_analysis_resumable(run_id: UUID) -> None:
    item = _analysis_item(run_id)
    item_id = int(item["id"])
    progress = dict(item.get("progress") or {})

    if not progress.get("analysis_trade_simulation_complete"):
        _set_stage(run_id, "trade_simulation")
        analysis._build_primary_trades(run_id)
        analysis._persist_primary_metrics(run_id)
        _checkpoint(item_id, "trade_simulation")
        progress["analysis_trade_simulation_complete"] = True

    signals = fetch_all(
        "select * from crypto_b001_replication_signals where run_id=%s order by bucket_start,symbol",
        (run_id,),
    )

    concentration_pass: bool
    concentration_details: dict[str, Any]
    if not progress.get("analysis_falsification_complete"):
        _set_stage(run_id, "falsification")
        # Placebos are a deterministic output of this phase. Clear only when
        # entering an incomplete falsification phase, never on every retry.
        with db_connection() as conn, conn.cursor() as cur:
            cur.execute("delete from crypto_b001_replication_placebos where run_id=%s", (run_id,))
            conn.commit()
        analysis._timestamp_placebos(run_id, signals)
        analysis._symbol_placebo(run_id, signals)
        analysis._low_dispersion_placebo(run_id)
        analysis._component_ablations(run_id)
        concentration_pass, concentration_details = analysis._leave_out_tests(run_id)
        _checkpoint(
            item_id,
            "falsification",
            analysis_concentration_pass=concentration_pass,
            analysis_concentration_details=concentration_details,
        )
        progress["analysis_falsification_complete"] = True
        progress["analysis_concentration_pass"] = concentration_pass
        progress["analysis_concentration_details"] = concentration_details
    else:
        concentration_pass = bool(progress.get("analysis_concentration_pass"))
        concentration_details = dict(progress.get("analysis_concentration_details") or {})

    if not progress.get("analysis_robustness_complete"):
        _set_stage(run_id, "post_replication_robustness")
        robustness = analysis._robustness(run_id, signals)
        _checkpoint(item_id, "robustness")
        progress["analysis_robustness_complete"] = True
    else:
        robustness = _load_robustness(run_id)

    if not progress.get("analysis_qa_complete"):
        qa = analysis._qa_checks(run_id)
        _checkpoint(item_id, "qa")
        progress["analysis_qa_complete"] = True
    else:
        qa = _load_qa(run_id)

    classification, reason, score = analysis._classification(
        run_id, robustness, concentration_pass, concentration_details
    )
    _checkpoint(
        item_id,
        "classification",
        analysis_classification=classification,
        analysis_classification_reason=reason,
    )

    export_row = fetch_one(
        "select storage_object_path,size_bytes,sha256 from crypto_b001_replication_exports where run_id=%s and export_type='full_zip' order by created_at desc limit 1",
        (run_id,),
    )
    if export_row:
        export = dict(export_row)
    else:
        export = analysis._export(run_id)
    _checkpoint(item_id, "export", analysis_export=export)

    mandatory_qa = all(check["passed"] for check in qa if check["number"] <= 12)
    coverage_qa = next((check["passed"] for check in qa if check["number"] == 13), False)
    final_status = "completed" if mandatory_qa and coverage_qa else "completed_with_errors"
    with db_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            update crypto_b001_replication_runs set
                status=%s,stage='completed',classification=%s,classification_reason=%s,
                completed_at=now(),updated_at=now(),
                execution_spec=coalesce(execution_spec,'{}'::jsonb) || %s
            where id=%s
            """,
            (
                final_status,
                classification,
                reason,
                Jsonb({"hard_rule_scorecard": score, "export": export}),
                run_id,
            ),
        )
        conn.commit()
    _checkpoint(item_id, "finalized")


# process_b001_work imports run_full_analysis from this module at execution time,
# so replacing the module binding is sufficient without changing the frozen
# replication state machine.
analysis.run_full_analysis = run_full_analysis_resumable
