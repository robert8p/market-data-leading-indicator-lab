from __future__ import annotations

"""Durable, exact orchestration for B-001 post-replication robustness.

This module changes no robustness parameter, signal rule, execution rule, hedge,
holding period or cost assumption. It only changes execution shape:

* persist each robustness variant immediately instead of holding the entire phase
  in memory until the end;
* skip already-persisted variants after a retry or deploy;
* calculate candidate outcomes in bounded set-based batches rather than one SQL
  round trip per candidate;
* preserve the methodology-hardening cost stress definition on the exact accepted
  non-overlapping primary B-001a research portfolio trades.
"""

import re
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

    # Effective methodology-hardening cost stress definition.
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

    threshold_variants: list[tuple[str, float, dict[str, float]]] = []
    for mult in (0.8, 0.9, 1.1, 1.2):
        threshold_variants.extend(
            [
                ("dispersion", mult, {"dispersion_max": DISPERSION_MAX * mult}),
                ("final_5m", mult, {"final_5m_max": FINAL_5M_MAX * mult}),
                ("high_to_close", mult, {"high_to_close_min": HIGH_TO_CLOSE_MIN * mult}),
                ("close_vs_vwap", mult, {"close_vs_vwap_max": CLOSE_VS_VWAP_MAX * mult}),
            ]
        )

    for kind, mult, params in threshold_variants:
        rtype = f"threshold_{kind}"
        variant = f"x{mult:.1f}"
        key = f"{rtype}:{variant}"
        if key in outputs:
            continue
        kwargs = {
            "dispersion_max": DISPERSION_MAX,
            "final_5m_max": FINAL_5M_MAX,
            "high_to_close_min": HIGH_TO_CLOSE_MIN,
            "close_vs_vwap_max": CLOSE_VS_VWAP_MAX,
        }
        kwargs.update(params)
        candidates = analysis._candidate_rows_for_variant(run_id, **kwargs)
        metrics = _variant_metrics_set_based(
            run_id,
            candidates,
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


# Imported after methodology hardening, so this becomes the effective robustness
# implementation used by the resumable analysis facade.
analysis._robustness = _resumable_robustness
