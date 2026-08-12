from __future__ import annotations

"""Forward-holdout semantics for the chronology-corrected B-001 rule.

Historical-replication QA expects roughly a year of older unseen history and
checks that the replication ends before the original discovery window. A
prospective holdout has the opposite chronology: it must begin only after the
methodology freeze. This patch applies only to runs explicitly tagged
`purpose=forward_holdout` in execution_spec.
"""

from datetime import timedelta
from typing import Any
from uuid import UUID

from psycopg.types.json import Jsonb

import app.b001_analysis as analysis
import app.b001_replication as replication
from app.b001_contract import HOLD_HOURS, PRIMARY_COMBINED_COST_BP
from app.db import db_connection, fetch_one


_ORIGINAL_GENERATE_SIGNALS = replication._generate_signals
_ORIGINAL_QA_CHECKS = analysis._qa_checks
_ORIGINAL_CLASSIFICATION = analysis._classification


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


def _classification(
    run_id: UUID,
    robustness: dict[str, dict],
    concentration_pass: bool,
    concentration_details: dict,
):
    run = fetch_one(
        "select execution_spec from crypto_b001_replication_runs where id=%s",
        (run_id,),
    )
    if not _is_forward_holdout(run):
        return _ORIGINAL_CLASSIFICATION(run_id, robustness, concentration_pass, concentration_details)

    spec = run.get("execution_spec") or {}
    rule = spec.get("forward_holdout_decision_rule") or {}
    min_n = int(rule.get("minimum_executable_nonoverlap_trades") or 10)
    hit_floor = float(rule.get("primary_hit_rate_gt") or 0.5)
    mean_floor = float(rule.get("primary_mean_net_return_gt") or 0.0)
    pf_floor = float(rule.get("primary_profit_factor_gt") or 1.0)
    stress_bp = float(rule.get("cost_stress_bp") or 100.0)
    stress_mean_floor = float(rule.get("cost_stress_mean_net_return_gt") or 0.0)

    row = fetch_one(
        """
        select metrics from crypto_b001_replication_metrics
        where run_id=%s and structure='B-001a' and position_mode='portfolio'
          and execution_subset='historically_executable' and cost_bp=%s and block='aggregate'
        """,
        (run_id, PRIMARY_COMBINED_COST_BP),
    )
    metrics = (row or {}).get("metrics") or {}
    n = int(metrics.get("n_trades") or 0)
    hit = float(metrics.get("hit_rate") or 0.0)
    mean = float(metrics.get("mean_net_return") or 0.0)
    pf = float(metrics.get("profit_factor") or 0.0)

    stressed = fetch_one(
        """
        with r as (
            select gross_return - %s::double precision / 10000.0 net_return
            from crypto_b001_replication_trades
            where run_id=%s and structure='B-001a' and position_mode='portfolio'
              and execution_subset='historically_executable' and cost_bp=%s
              and not ignored_overlap
        )
        select count(*)::bigint n,
               avg(net_return) mean_net_return,
               avg((net_return>0)::int) hit_rate,
               sum(greatest(net_return,0))/nullif(abs(sum(least(net_return,0))),0) profit_factor
        from r
        """,
        (stress_bp, run_id, PRIMARY_COMBINED_COST_BP),
    ) or {}
    stress_mean = float(stressed.get("mean_net_return") or 0.0)

    qa = fetch_one(
        """
        select count(*)::int total,
               count(*) filter(where passed)::int passed,
               count(*) filter(where not passed)::int failed
        from crypto_b001_replication_qa where run_id=%s
        """,
        (run_id,),
    ) or {}
    qa_pass = int(qa.get("failed") or 0) == 0 and int(qa.get("total") or 0) >= 13

    score = {
        "purpose": "forward_holdout",
        "decision_rule_frozen_before_outcomes": bool(rule) and rule.get("rule_changes_after_outcomes") is False,
        "minimum_executable_nonoverlap_trades": min_n,
        "executable_nonoverlap_n": n,
        "sample_size_pass": n >= min_n,
        "primary_hit_rate": hit,
        "primary_hit_rate_pass": hit > hit_floor,
        "primary_mean_net_return": mean,
        "primary_mean_net_return_pass": mean > mean_floor,
        "primary_profit_factor": pf,
        "primary_profit_factor_pass": pf > pf_floor,
        "stress_cost_bp": stress_bp,
        "stress_mean_net_return": stress_mean,
        "stress_mean_net_return_pass": stress_mean > stress_mean_floor,
        "all_qa_pass": qa_pass,
        "qa_total": int(qa.get("total") or 0),
        "qa_failed": int(qa.get("failed") or 0),
        "concentration_pass": concentration_pass,
        "concentration_details": concentration_details,
        "loss_shape_constraint_removed": True,
        "rule_changes_after_outcomes": False,
    }

    if n < min_n:
        return (
            "B",
            f"INCONCLUSIVE forward holdout: {n} executable non-overlapping trades, below the pre-frozen minimum of {min_n}. No rule change is permitted from this result.",
            score,
        )

    economic_pass = hit > hit_floor and mean > mean_floor and pf > pf_floor
    all_pass = economic_pass and stress_mean > stress_mean_floor and qa_pass and concentration_pass
    if all_pass:
        return (
            "A",
            "The sealed chronology-corrected forward holdout passes the pre-frozen executable sample-size, hit-rate, net-expectancy, profit-factor, 100bp cost-stress, QA and concentration gates. The former loss-shape constraint is not a promotion criterion.",
            score,
        )

    hard_fail = (hit <= hit_floor) or (mean <= mean_floor) or (pf <= pf_floor) or (stress_mean <= stress_mean_floor)
    if hard_fail:
        return (
            "D",
            "The sealed forward holdout fails one or more pre-frozen economic gates. The rule remains frozen and is not retuned from holdout outcomes.",
            score,
        )

    return (
        "C",
        "The sealed forward holdout is economically positive but fails a non-economic pre-frozen QA or concentration gate; it is not promoted.",
        score,
    )


replication._generate_signals = _generate_signals
analysis._qa_checks = _qa_checks
analysis._classification = _classification
