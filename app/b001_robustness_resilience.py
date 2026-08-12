from __future__ import annotations

"""Durable, exact orchestration for B-001 post-replication robustness.

This module changes no robustness parameter, signal rule, execution rule, hedge,
holding period or cost assumption. It only changes execution shape:

* persist each robustness variant immediately instead of holding the entire phase
  in memory until the end;
* skip already-persisted variants after a retry or deploy;
* calculate candidate outcomes in bounded set-based batches rather than one SQL
  round trip per candidate;
* compute the shared cross-sectional rank state once per month for all threshold
  perturbations instead of recomputing identical percentile ranks 16 times;
* preserve the methodology-hardening cost stress definition on the exact accepted
  non-overlapping primary B-001a research portfolio trades.
"""

import re
from datetime import timedelta
from typing import Any
from uuid import UUID

from psycopg.types.json import Jsonb

import app.b001_analysis as analysis
from app.b001_contract import (
    BTC_HEDGE_WEIGHT,
    CLOSE_VS_VWAP_MAX,
    DISPERSION_MAX,
    FINAL_5M_MAX,
    HIGH_TO_CLOSE_MIN,
    HOLD_HOURS,
    PRIMARY_COMBINED_COST_BP,
    STRESS_COSTS_BP,
    TOKEN_COST_BP,
    MetricInput,
    calculate_metrics,
)
from app.db import db_connection, fetch_all, fetch_one


_BATCH_SIZE = 2000


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
        raise RuntimeError("B-001 analysis work item is missing during robustness")
    return row


def _robustness_progress_key(rtype: str, variant: str) -> str:
    safe = re.sub(r"[^a-zA-Z0-9]+", "_", f"{rtype}_{variant}").strip("_").lower()
    return f"analysis_robustness_{safe}_complete"


def _checkpoint_patch(item_id: int, patch: dict[str, Any]) -> None:
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


def _persist_variant(
    run_id: UUID,
    item_id: int,
    rtype: str,
    variant: str,
    metrics: dict[str, Any],
    parameters: dict[str, Any],
) -> None:
    with db_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            insert into crypto_b001_replication_robustness(
                run_id,robustness_type,variant,metrics,parameters
            ) values (%s,%s,%s,%s,%s)
            on conflict (run_id,robustness_type,variant) do update set
                metrics=excluded.metrics,
                parameters=excluded.parameters,
                created_at=now()
            """,
            (run_id, rtype, variant, Jsonb(metrics), Jsonb(parameters)),
        )
        cur.execute(
            """
            update crypto_b001_replication_work_items
               set progress=coalesce(progress,'{}'::jsonb) || %s,updated_at=now()
             where id=%s
            """,
            (
                Jsonb(
                    {
                        _robustness_progress_key(rtype, variant): True,
                        "analysis_robustness_last_completed_variant": f"{rtype}:{variant}",
                    }
                ),
                item_id,
            ),
        )
        conn.commit()


def _existing_variants(run_id: UUID) -> dict[str, dict[str, Any]]:
    rows = fetch_all(
        """
        select robustness_type,variant,metrics
          from crypto_b001_replication_robustness
         where run_id=%s
        """,
        (run_id,),
    )
    return {
        f"{row['robustness_type']}:{row['variant']}": row.get("metrics") or {}
        for row in rows
    }


def _portfolio_cost_metrics(run_id: UUID, cost_bp: float) -> dict[str, Any]:
    """Exact methodology-hardening cost stress on accepted primary trades."""
    rows = fetch_all(
        """
        select t.symbol,s.signal_ts,t.gross_return,t.mae,t.mfe,t.concurrency
          from crypto_b001_replication_trades t
          join crypto_b001_replication_signals s on s.id=t.signal_id
         where t.run_id=%s
           and t.structure='B-001a'
           and t.position_mode='portfolio'
           and t.execution_subset='research'
           and t.cost_bp=%s
           and not t.ignored_overlap
         order by s.signal_ts,t.symbol
        """,
        (run_id, PRIMARY_COMBINED_COST_BP),
    )
    return calculate_metrics(
        MetricInput(
            symbol=row["symbol"],
            signal_ts=row["signal_ts"],
            net_return=float(row["gross_return"]) - float(cost_bp) / 10_000.0,
            mae=float(row["mae"]) if row.get("mae") is not None else None,
            mfe=float(row["mfe"]) if row.get("mfe") is not None else None,
            concurrency=int(row["concurrency"]) if row.get("concurrency") is not None else None,
        )
        for row in rows
    )


def _variant_metrics_set_based(
    run_id: UUID | str,
    candidates: list[tuple[str, Any]],
    *,
    hold_hours: int = HOLD_HOURS,
    hedge_weight: float = BTC_HEDGE_WEIGHT,
    cost_bp: float | None = None,
    batch_size: int = _BATCH_SIZE,
) -> dict[str, Any]:
    if cost_bp is None:
        cost_bp = TOKEN_COST_BP + hedge_weight * TOKEN_COST_BP
    metric_rows: list[MetricInput] = []
    for offset in range(0, len(candidates), max(1, batch_size)):
        batch = candidates[offset : offset + max(1, batch_size)]
        if not batch:
            continue
        symbols = [symbol for symbol, _bucket in batch]
        buckets = [bucket for _symbol, bucket in batch]
        rows = fetch_all(
            """
            with c as (
                select *
                  from unnest(%s::text[],%s::timestamptz[]) as x(symbol,bucket_start)
            )
            select
                c.symbol,
                c.bucket_start + interval '15 minutes' entry_ts,
                1.0 - tx.open/nullif(te.open,0)
                  + %s * (bx.open/nullif(be.open,0)-1.0)
                  - %s as net_return
              from c
              join crypto_b001_replication_15m te
                on te.run_id=%s and te.symbol=c.symbol
               and te.bucket_start=c.bucket_start+interval '15 minutes'
              join crypto_b001_replication_15m tx
                on tx.run_id=te.run_id and tx.symbol=te.symbol
               and tx.bucket_start=c.bucket_start+interval '15 minutes'+(%s * interval '1 hour')
              join crypto_b001_replication_15m be
                on be.run_id=te.run_id and be.symbol='BTCUSDT'
               and be.bucket_start=c.bucket_start+interval '15 minutes'
              join crypto_b001_replication_15m bx
                on bx.run_id=te.run_id and bx.symbol='BTCUSDT'
               and bx.bucket_start=c.bucket_start+interval '15 minutes'+(%s * interval '1 hour')
             order by c.bucket_start,c.symbol
            """,
            (
                symbols,
                buckets,
                hedge_weight,
                float(cost_bp) / 10_000.0,
                run_id,
                hold_hours,
                hold_hours,
            ),
        )
        metric_rows.extend(
            MetricInput(
                symbol=row["symbol"],
                signal_ts=row["entry_ts"],
                net_return=float(row["net_return"]),
            )
            for row in rows
        )
    return calculate_metrics(metric_rows)


def _threshold_variant_specs() -> list[tuple[str, str, dict[str, float]]]:
    specs: list[tuple[str, str, dict[str, float]]] = []
    for mult in (0.8, 0.9, 1.1, 1.2):
        specs.extend(
            [
                ("threshold_dispersion", f"x{mult:.1f}", {"dispersion_max": DISPERSION_MAX * mult}),
                ("threshold_final_5m", f"x{mult:.1f}", {"final_5m_max": FINAL_5M_MAX * mult}),
                ("threshold_high_to_close", f"x{mult:.1f}", {"high_to_close_min": HIGH_TO_CLOSE_MIN * mult}),
                ("threshold_close_vs_vwap", f"x{mult:.1f}", {"close_vs_vwap_max": CLOSE_VS_VWAP_MAX * mult}),
            ]
        )
    return specs


def _threshold_candidate_sets(
    run_id: UUID,
    specs: list[tuple[str, str, dict[str, float]]],
) -> dict[str, list[tuple[str, Any]]]:
    """Compute the common ranked state once per month, then apply exact variants.

    All threshold perturbations share the extreme-state, persistence/recency and
    exhaustion rules. Only dispersion and the 2-of-3 rejection thresholds vary.
    Returning that common state once and evaluating the exact comparisons in
    Python is mathematically equivalent to issuing 16 copies of the rank query.
    """
    run = fetch_one(
        "select requested_start,requested_end from crypto_b001_replication_runs where id=%s",
        (run_id,),
    )
    if not run:
        raise RuntimeError("B-001 replication run disappeared during robustness")

    result = {f"{rtype}:{variant}": [] for rtype, variant, _params in specs}
    cursor = run["requested_start"].replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    while cursor < run["requested_end"]:
        month_end = (cursor + timedelta(days=32)).replace(day=1)
        start = max(cursor, run["requested_start"])
        end = min(month_end, run["requested_end"])
        rank_start = start - timedelta(minutes=75)
        rows = fetch_all(
            """
            with ranked as (
                select f.*,
                    percent_rank() over(partition by bucket_start order by range15) rp,
                    percent_rank() over(partition by bucket_start order by pos_vs_low4h) lp,
                    percent_rank() over(partition by bucket_start order by qv_ratio16) qp
                from crypto_b001_replication_features f
                where f.run_id=%s and f.bucket_start >= %s and f.bucket_start < %s
                  and f.liquidity_eligible and f.range15 is not null
                  and f.pos_vs_low4h is not null and f.qv_ratio16 is not null
            ), state as (
                select ranked.*,(rp>=0.90 and lp>=0.90 and qp>=0.90) extreme
                  from ranked
            )
            select
                c.symbol,c.bucket_start,c.final_5m_return,c.high_to_close_rejection,
                c.close_vs_vwap,m.dispersion15
              from state c
              join state p
                on p.symbol=c.symbol and p.bucket_start=c.bucket_start-interval '15 minutes'
              join state p2
                on p2.symbol=c.symbol and p2.bucket_start=c.bucket_start-interval '30 minutes'
              join state p5
                on p5.symbol=c.symbol and p5.bucket_start=c.bucket_start-interval '75 minutes'
              join crypto_b001_replication_market_state m
                on m.run_id=c.run_id and m.bucket_start=c.bucket_start
             where c.bucket_start >= %s and c.bucket_start < %s
               and c.extreme and p.extreme and p2.extreme and not p5.extreme
               and c.ret15 <= 0 and c.range15 < p.range15
             order by c.bucket_start,c.symbol
            """,
            (run_id, rank_start, end, start, end),
        )

        for row in rows:
            f5 = row.get("final_5m_return")
            hc = row.get("high_to_close_rejection")
            cv = row.get("close_vs_vwap")
            disp = row.get("dispersion15")
            for rtype, variant, params in specs:
                dispersion_max = float(params.get("dispersion_max", DISPERSION_MAX))
                final_5m_max = float(params.get("final_5m_max", FINAL_5M_MAX))
                high_to_close_min = float(params.get("high_to_close_min", HIGH_TO_CLOSE_MIN))
                close_vs_vwap_max = float(params.get("close_vs_vwap_max", CLOSE_VS_VWAP_MAX))
                rejection_count = (
                    int(f5 is not None and float(f5) <= final_5m_max)
                    + int(hc is not None and float(hc) >= high_to_close_min)
                    + int(cv is not None and float(cv) <= close_vs_vwap_max)
                )
                if disp is not None and float(disp) <= dispersion_max and rejection_count >= 2:
                    result[f"{rtype}:{variant}"].append((row["symbol"], row["bucket_start"]))
        cursor = month_end
    return result


def _resumable_robustness(run_id: UUID, signals: list[dict[str, Any]]) -> dict[str, dict]:
    item = _analysis_item(run_id)
    item_id = int(item["id"])
    progress = dict(item.get("progress") or {})

    if not progress.get("analysis_robustness_initialized"):
        with db_connection() as conn, conn.cursor() as cur:
            cur.execute("delete from crypto_b001_replication_robustness where run_id=%s", (run_id,))
            conn.commit()
        _checkpoint_patch(item_id, {"analysis_robustness_initialized": True})

    outputs = _existing_variants(run_id)
    primary_candidates = [(s["symbol"], s["bucket_start"]) for s in signals]

    def ensure(
        rtype: str,
        variant: str,
        parameters: dict[str, Any],
        calculator,
    ) -> None:
        key = f"{rtype}:{variant}"
        if key in outputs:
            return
        metrics = calculator()
        _persist_variant(run_id, item_id, rtype, variant, metrics, parameters)
        outputs[key] = metrics

    for bp in STRESS_COSTS_BP:
        ensure(
            "cost_stress",
            f"{bp:g}bp",
            {
                "cost_bp": bp,
                "basis": "same accepted non-overlapping B-001a research portfolio trades as primary",
                "label": "POST-REPLICATION ROBUSTNESS — NOT PRIMARY TEST",
            },
            lambda bp=bp: _portfolio_cost_metrics(run_id, bp),
        )

    for hours in (6, 10, 12):
        ensure(
            "holding_period",
            f"{hours}h",
            {"hold_hours": hours},
            lambda hours=hours: _variant_metrics_set_based(
                run_id,
                primary_candidates,
                hold_hours=hours,
                cost_bp=PRIMARY_COMBINED_COST_BP,
            ),
        )

    for weight in (0.50, 0.60, 0.90):
        variant_cost = TOKEN_COST_BP + weight * TOKEN_COST_BP
        ensure(
            "btc_hedge_weight",
            f"{weight:.2f}",
            {"btc_hedge_weight": weight, "cost_bp": variant_cost},
            lambda weight=weight, variant_cost=variant_cost: _variant_metrics_set_based(
                run_id,
                primary_candidates,
                hedge_weight=weight,
                cost_bp=variant_cost,
            ),
        )

    specs = _threshold_variant_specs()
    missing_specs = [
        spec for spec in specs if f"{spec[0]}:{spec[1]}" not in outputs
    ]
    if missing_specs:
        candidate_sets = _threshold_candidate_sets(run_id, missing_specs)
        _checkpoint_patch(
            item_id,
            {
                "analysis_robustness_threshold_shared_scan_complete": True,
                "analysis_robustness_threshold_shared_scan_variants": len(missing_specs),
            },
        )
        for rtype, variant, params in missing_specs:
            key = f"{rtype}:{variant}"
            metrics = _variant_metrics_set_based(
                run_id,
                candidate_sets[key],
                cost_bp=PRIMARY_COMBINED_COST_BP,
            )
            _persist_variant(run_id, item_id, rtype, variant, metrics, params)
            outputs[key] = metrics

    _checkpoint_patch(
        item_id,
        {
            "analysis_robustness_variant_count": len(outputs),
            "analysis_robustness_resumable_complete": True,
        },
    )
    return outputs


analysis._robustness = _resumable_robustness
