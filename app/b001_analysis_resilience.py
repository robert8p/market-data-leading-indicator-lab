from __future__ import annotations

"""Durable phase checkpoints for the locked B-001 final analysis.

The statistical functions remain the existing frozen functions. This wrapper only
avoids repeating already completed phases after a deploy, worker restart, or
transient infrastructure failure. Long full-history placebo scans are split into
deterministic calendar-month statements so the database statement timeout cannot
erase an otherwise valid long-running falsification campaign.
"""

from datetime import timedelta
from typing import Any
from uuid import UUID

from psycopg.types.json import Jsonb

import app.b001_analysis as analysis
from app.b001_contract import DISPERSION_MAX, PRIMARY_COMBINED_COST_BP
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


def _checkpoint_substep(item_id: int, name: str, **details: Any) -> None:
    patch = {
        f"analysis_falsification_{name}_complete": True,
        "analysis_falsification_last_completed_substep": name,
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


def _mark_falsification_initialized(item_id: int) -> None:
    with db_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            update crypto_b001_replication_work_items
               set progress=coalesce(progress,'{}'::jsonb) || %s,updated_at=now()
             where id=%s
            """,
            (Jsonb({"analysis_falsification_initialized": True}), item_id),
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


def _low_dispersion_placebo_chunked(run_id: UUID) -> None:
    """Exact low-dispersion placebo, evaluated in bounded monthly SQL chunks.

    The original query selects one deterministic non-signal liquid symbol per
    low-dispersion timestamp using a row_number partitioned by bucket_start. A
    calendar-month split is mathematically identical because every partition is
    contained inside a single timestamp/month; only the database execution unit
    changes.
    """
    run = fetch_one(
        "select requested_start,requested_end from crypto_b001_replication_runs where id=%s",
        (run_id,),
    )
    if not run:
        raise RuntimeError("B-001 replication run disappeared during falsification")

    metric_rows: list[Any] = []
    cursor = run["requested_start"].replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    while cursor < run["requested_end"]:
        month_end = (cursor + timedelta(days=32)).replace(day=1)
        start = max(cursor, run["requested_start"])
        end = min(month_end, run["requested_end"])
        rows = fetch_all(
            """
            with candidates as (
                select f.symbol,f.bucket_start,
                       row_number() over(
                           partition by f.bucket_start
                           order by md5(f.symbol || f.bucket_start::text || 'B001_LOW_DISP_V1')
                       ) rn
                from crypto_b001_replication_features f
                join crypto_b001_replication_market_state m
                  on m.run_id=f.run_id and m.bucket_start=f.bucket_start
                where f.run_id=%s and f.bucket_start >= %s and f.bucket_start < %s
                  and f.liquidity_eligible and m.dispersion15 <= %s
                  and not exists (
                      select 1 from crypto_b001_replication_signals s
                       where s.run_id=f.run_id and s.symbol=f.symbol
                         and s.bucket_start=f.bucket_start
                  )
            ), picked as (
                select * from candidates where rn=1
            ), outcomes as (
                select p.symbol,p.bucket_start,te.open te,tx.open tx,be.open be,bx.open bx
                from picked p
                join crypto_b001_replication_15m te
                  on te.run_id=%s and te.symbol=p.symbol
                 and te.bucket_start=p.bucket_start+interval '15 minutes'
                join crypto_b001_replication_15m tx
                  on tx.run_id=te.run_id and tx.symbol=te.symbol
                 and tx.bucket_start=p.bucket_start+interval '8 hours 15 minutes'
                join crypto_b001_replication_15m be
                  on be.run_id=te.run_id and be.symbol='BTCUSDT'
                 and be.bucket_start=p.bucket_start+interval '15 minutes'
                join crypto_b001_replication_15m bx
                  on bx.run_id=te.run_id and bx.symbol='BTCUSDT'
                 and bx.bucket_start=p.bucket_start+interval '8 hours 15 minutes'
            )
            select symbol,bucket_start,
                   1-tx/te+0.75*(bx/be-1)-%s net_return
              from outcomes
             order by bucket_start
            """,
            (
                run_id,
                start,
                end,
                DISPERSION_MAX,
                run_id,
                analysis._cost(PRIMARY_COMBINED_COST_BP),
            ),
        )
        metric_rows.extend(
            analysis.MetricInput(
                row["symbol"],
                row["bucket_start"] + timedelta(minutes=15),
                float(row["net_return"]),
            )
            for row in rows
        )
        cursor = month_end

    analysis._persist_placebo(
        run_id,
        "low_dispersion",
        "one_deterministic_ordinary_liquid_short_per_low_dispersion_timestamp",
        metric_rows,
        {"seed": "B001_LOW_DISP_V1", "execution": "calendar_month_chunks_exact_equivalence"},
    )


def _run_falsification_resumable(
    run_id: UUID,
    item_id: int,
    progress: dict[str, Any],
    signals: list[dict[str, Any]],
) -> tuple[bool, dict[str, Any]]:
    _set_stage(run_id, "falsification")

    if not progress.get("analysis_falsification_initialized"):
        # Clear legacy/partial outputs exactly once when entering the resumable
        # falsification workflow. Subsequent retries retain completed substeps.
        with db_connection() as conn, conn.cursor() as cur:
            cur.execute("delete from crypto_b001_replication_placebos where run_id=%s", (run_id,))
            conn.commit()
        _mark_falsification_initialized(item_id)
        progress["analysis_falsification_initialized"] = True

    if not progress.get("analysis_falsification_timestamp_placebos_complete"):
        analysis._timestamp_placebos(run_id, signals)
        _checkpoint_substep(item_id, "timestamp_placebos")
        progress["analysis_falsification_timestamp_placebos_complete"] = True

    if not progress.get("analysis_falsification_symbol_placebo_complete"):
        analysis._symbol_placebo(run_id, signals)
        _checkpoint_substep(item_id, "symbol_placebo")
        progress["analysis_falsification_symbol_placebo_complete"] = True

    if not progress.get("analysis_falsification_low_dispersion_placebo_complete"):
        _low_dispersion_placebo_chunked(run_id)
        _checkpoint_substep(item_id, "low_dispersion_placebo")
        progress["analysis_falsification_low_dispersion_placebo_complete"] = True

    if not progress.get("analysis_falsification_component_ablations_complete"):
        analysis._component_ablations(run_id)
        _checkpoint_substep(item_id, "component_ablations")
        progress["analysis_falsification_component_ablations_complete"] = True

    if not progress.get("analysis_falsification_leave_out_tests_complete"):
        concentration_pass, concentration_details = analysis._leave_out_tests(run_id)
        _checkpoint_substep(
            item_id,
            "leave_out_tests",
            analysis_concentration_pass=concentration_pass,
            analysis_concentration_details=concentration_details,
        )
        progress["analysis_falsification_leave_out_tests_complete"] = True
        progress["analysis_concentration_pass"] = concentration_pass
        progress["analysis_concentration_details"] = concentration_details
    else:
        concentration_pass = bool(progress.get("analysis_concentration_pass"))
        concentration_details = dict(progress.get("analysis_concentration_details") or {})

    _checkpoint(
        item_id,
        "falsification",
        analysis_concentration_pass=concentration_pass,
        analysis_concentration_details=concentration_details,
    )
    return concentration_pass, concentration_details


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
        concentration_pass, concentration_details = _run_falsification_resumable(
            run_id, item_id, progress, signals
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


analysis.run_full_analysis = run_full_analysis_resumable
