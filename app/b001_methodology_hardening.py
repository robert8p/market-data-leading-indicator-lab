from __future__ import annotations

"""Pre-outcome methodological hardening for the locked B-001 replication.

This module changes no B-001 threshold, feature, hedge, holding period or cost.
It only makes two boundary/reporting rules explicit before historical outcomes
are available:

1. A primary signal is retained only when its next-bar entry and full frozen
   eight-hour exit both lie inside the unseen replication window.
2. Class-A transaction-cost stress is measured on the exact same accepted,
   non-overlapping B-001a portfolio trades as the primary result.

The original robustness function still produces all other post-replication
neighbourhood reports. Cost-stress rows are then deterministically replaced
with the portfolio-consistent values below.
"""

from datetime import timedelta
from typing import Any
from uuid import UUID

from psycopg.types.json import Jsonb

import app.b001_analysis as analysis
import app.b001_replication as replication
import app.b001_runtime as runtime
from app.b001_contract import (
    PRIMARY_COMBINED_COST_BP,
    STRESS_COSTS_BP,
    MetricInput,
    calculate_metrics,
)
from app.db import db_connection, fetch_all, fetch_one


_ORIGINAL_SIGNAL_GENERATOR = runtime._generate_signals
_ORIGINAL_CANDIDATE_VARIANTS = runtime._candidate_rows_for_variant
_ORIGINAL_ROBUSTNESS = analysis._robustness


def _primary_signal_cutoff(run_id: UUID):
    row = fetch_one(
        "select requested_end from crypto_b001_replication_runs where id=%s",
        (run_id,),
    )
    if not row:
        raise RuntimeError("B-001 replication run disappeared")
    # Signal bucket T -> entry T+15m -> frozen exit T+8h15m.
    # requested_end is exclusive, so the exit bucket must be strictly earlier.
    return row["requested_end"] - timedelta(hours=8, minutes=15)


def _generate_signals(item: dict[str, Any]) -> None:
    """Generate the frozen signal, then remove terminally censored observations."""
    _ORIGINAL_SIGNAL_GENERATOR(item)
    cutoff = _primary_signal_cutoff(UUID(str(item["run_id"])))
    with db_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            delete from crypto_b001_replication_signals
            where run_id=%s and bucket_start >= %s
            """,
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
                        "terminal_horizon_removed": removed,
                        "rule": "signal bucket must be < requested_end - 8h15m so next-bar entry plus frozen 8h exit stays inside unseen history",
                    }),
                    item["id"],
                ),
            )
        conn.commit()


def _candidate_rows_for_variant(
    run_id: UUID,
    removed: str | None = None,
    dispersion_max: float = runtime.DISPERSION_MAX,
    final_5m_max: float = runtime.FINAL_5M_MAX,
    high_to_close_min: float = runtime.HIGH_TO_CLOSE_MIN,
    close_vs_vwap_max: float = runtime.CLOSE_VS_VWAP_MAX,
):
    rows = _ORIGINAL_CANDIDATE_VARIANTS(
        run_id,
        removed=removed,
        dispersion_max=dispersion_max,
        final_5m_max=final_5m_max,
        high_to_close_min=high_to_close_min,
        close_vs_vwap_max=close_vs_vwap_max,
    )
    cutoff = _primary_signal_cutoff(run_id)
    return [(symbol, bucket) for symbol, bucket in rows if bucket < cutoff]


def _portfolio_cost_metrics(run_id: UUID, cost_bp: float) -> dict:
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


def _robustness(run_id: UUID) -> dict[str, dict]:
    outputs = _ORIGINAL_ROBUSTNESS(run_id)
    for bp in STRESS_COSTS_BP:
        metrics = _portfolio_cost_metrics(run_id, bp)
        key = f"cost_stress:{bp:g}bp"
        outputs[key] = metrics
        with db_connection() as conn, conn.cursor() as cur:
            cur.execute(
                """
                insert into crypto_b001_replication_robustness(
                    run_id,robustness_type,variant,metrics,parameters
                ) values (%s,'cost_stress',%s,%s,%s)
                on conflict (run_id,robustness_type,variant) do update set
                    metrics=excluded.metrics,
                    parameters=excluded.parameters,
                    created_at=now()
                """,
                (
                    run_id,
                    f"{bp:g}bp",
                    Jsonb(metrics),
                    Jsonb({
                        "cost_bp": bp,
                        "basis": "same accepted non-overlapping B-001a research portfolio trades as primary",
                        "label": "POST-REPLICATION ROBUSTNESS — NOT PRIMARY TEST",
                    }),
                ),
            )
            conn.commit()
    return outputs


# Patch the live globals used by the durable worker/analysis functions.
runtime._generate_signals = _generate_signals
replication._generate_signals = _generate_signals
runtime._candidate_rows_for_variant = _candidate_rows_for_variant
analysis._candidate_rows_for_variant = _candidate_rows_for_variant
analysis._robustness = _robustness
