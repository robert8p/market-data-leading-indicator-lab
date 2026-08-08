from __future__ import annotations

"""Pre-outcome methodological hardening for the locked B-001 replication.

This module changes no B-001 threshold, feature, hedge, holding period or cost.
It makes boundary/data-quality rules explicit before historical outcomes exist:

1. Durable work progress is JSON-safe so retries remain operationally idempotent.
2. Lagged and rolling features are valid only across genuinely contiguous
   15-minute observations; missing bars are never silently bridged by row lag.
3. A primary signal is retained only when its next-bar entry and full frozen
   eight-hour exit both lie inside the unseen replication window.
4. Class-A transaction-cost stress is measured on the exact same accepted,
   non-overlapping B-001a portfolio trades as the primary result.

Importing app.b001_runtime first applies its release-time corrections to the
public replication/analysis callables. This module then wraps those patched
public callables only; it deliberately does not depend on b001_runtime's private
helper names.
"""

import json
from datetime import timedelta
from typing import Any
from uuid import UUID

from psycopg.types.json import Jsonb

import app.b001_analysis as analysis
import app.b001_replication as replication
import app.b001_runtime  # noqa: F401  # apply release-time patches first
from app.b001_contract import (
    CLOSE_VS_VWAP_MAX,
    DISPERSION_MAX,
    FINAL_5M_MAX,
    HIGH_TO_CLOSE_MIN,
    PRIMARY_COMBINED_COST_BP,
    STRESS_COSTS_BP,
    MetricInput,
    calculate_metrics,
)
from app.db import db_connection, fetch_all, fetch_one


_ORIGINAL_COMPLETE = replication._complete
_ORIGINAL_DERIVE_FEATURES = replication._derive_features
_ORIGINAL_SIGNAL_GENERATOR = replication._generate_signals
_ORIGINAL_CANDIDATE_VARIANTS = analysis._candidate_rows_for_variant
_ORIGINAL_ROBUSTNESS = analysis._robustness


def _json_safe(value: Any) -> Any:
    return json.loads(json.dumps(value, default=str))


def _complete(
    item_id: int,
    row_count: int = 0,
    progress: dict | None = None,
    status: str = "completed",
) -> None:
    _ORIGINAL_COMPLETE(item_id, row_count, _json_safe(progress or {}), status)


def _derive_features(item: dict[str, Any]) -> None:
    """Run exact discovery formulas, then invalidate any window that crosses a gap."""
    _ORIGINAL_DERIVE_FEATURES(item)
    symbol = str(item["payload"]["symbol"])
    with db_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            with continuity as (
                select run_id,symbol,bucket_start,
                    lag(bucket_start,1) over w as l1,
                    lag(bucket_start,2) over w as l2,
                    lag(bucket_start,3) over w as l3,
                    lag(bucket_start,4) over w as l4,
                    lag(bucket_start,15) over w as l15,
                    lag(bucket_start,16) over w as l16
                from crypto_b001_replication_features
                where run_id=%s and symbol=%s
                window w as (partition by run_id,symbol order by bucket_start)
            )
            update crypto_b001_replication_features f set
                ret15 = case when c.l1=f.bucket_start-interval '15 minutes' then f.ret15 end,
                ret30 = case when c.l2=f.bucket_start-interval '30 minutes' then f.ret30 end,
                ret60 = case when c.l4=f.bucket_start-interval '60 minutes' then f.ret60 end,
                ret240 = case when c.l16=f.bucket_start-interval '240 minutes' then f.ret240 end,
                qv_accel1 = case when c.l1=f.bucket_start-interval '15 minutes' then f.qv_accel1 end,
                qv_ratio4 = case when c.l3=f.bucket_start-interval '45 minutes' then f.qv_ratio4 end,
                qv_ratio16 = case when c.l15=f.bucket_start-interval '225 minutes' then f.qv_ratio16 end,
                trade_accel1 = case when c.l1=f.bucket_start-interval '15 minutes' then f.trade_accel1 end,
                trade_ratio4 = case when c.l3=f.bucket_start-interval '45 minutes' then f.trade_ratio4 end,
                trade_ratio16 = case when c.l15=f.bucket_start-interval '225 minutes' then f.trade_ratio16 end,
                pos_vs_high1h = case when c.l3=f.bucket_start-interval '45 minutes' then f.pos_vs_high1h end,
                pos_vs_low1h = case when c.l3=f.bucket_start-interval '45 minutes' then f.pos_vs_low1h end,
                pos_vs_high4h = case when c.l15=f.bucket_start-interval '225 minutes' then f.pos_vs_high4h end,
                pos_vs_low4h = case when c.l15=f.bucket_start-interval '225 minutes' then f.pos_vs_low4h end,
                ret_accel15 = case when c.l2=f.bucket_start-interval '30 minutes' then f.ret_accel15 end,
                rv1h = case when c.l4=f.bucket_start-interval '60 minutes' then f.rv1h end,
                rv4h = case when c.l16=f.bucket_start-interval '240 minutes' then f.rv4h end
            from continuity c
            where f.run_id=c.run_id and f.symbol=c.symbol and f.bucket_start=c.bucket_start
            """,
            (item["run_id"], symbol),
        )
        cur.execute(
            """
            update crypto_b001_replication_work_items
               set progress=progress || %s::jsonb,updated_at=now()
             where id=%s
            """,
            (
                Jsonb({
                    "continuity_hardened": True,
                    "rule": "lagged/rolling features are NULL unless all required 15-minute observations are contiguous",
                }),
                item["id"],
            ),
        )
        conn.commit()


def _primary_signal_cutoff(run_id: UUID):
    row = fetch_one(
        "select requested_end from crypto_b001_replication_runs where id=%s",
        (run_id,),
    )
    if not row:
        raise RuntimeError("B-001 replication run disappeared")
    return row["requested_end"] - timedelta(hours=8, minutes=15)


def _generate_signals(item: dict[str, Any]) -> None:
    """Generate the frozen signal, then remove terminally censored observations."""
    _ORIGINAL_SIGNAL_GENERATOR(item)
    cutoff = _primary_signal_cutoff(UUID(str(item["run_id"])))
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
    dispersion_max: float = DISPERSION_MAX,
    final_5m_max: float = FINAL_5M_MAX,
    high_to_close_min: float = HIGH_TO_CLOSE_MIN,
    close_vs_vwap_max: float = CLOSE_VS_VWAP_MAX,
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


replication._complete = _complete
replication._derive_features = _derive_features
replication._generate_signals = _generate_signals
analysis._candidate_rows_for_variant = _candidate_rows_for_variant
analysis._robustness = _robustness

claim_b001_work = replication.claim_b001_work
process_b001_work = replication.process_b001_work
reclaim_stale_b001_work = replication.reclaim_stale_b001_work
