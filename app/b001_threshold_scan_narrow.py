from __future__ import annotations

"""Reduce B-001 threshold robustness sort width without changing semantics.

The shared threshold scan needs only the fields below plus the three percentile
ranks. Carrying the entire feature row through three window sorts materially
increases temp-file I/O. This replacement uses the same rows, partitions,
percent_rank definitions, common conditions, thresholds and ordering.
"""

from datetime import timedelta
from typing import Any
from uuid import UUID

import app.b001_robustness_resilience as robustness
from app.b001_contract import (
    CLOSE_VS_VWAP_MAX,
    DISPERSION_MAX,
    FINAL_5M_MAX,
    HIGH_TO_CLOSE_MIN,
)


def _threshold_candidate_sets_narrow(
    run_id: UUID,
    specs: list[tuple[str, str, dict[str, float]]],
) -> dict[str, list[tuple[str, Any]]]:
    run = robustness.fetch_one(
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
        rows = robustness.fetch_all(
            """
            with ranked as (
                select
                    f.run_id,f.symbol,f.bucket_start,f.range15,f.ret15,
                    f.final_5m_return,f.high_to_close_rejection,f.close_vs_vwap,
                    percent_rank() over(partition by f.bucket_start order by f.range15) rp,
                    percent_rank() over(partition by f.bucket_start order by f.pos_vs_low4h) lp,
                    percent_rank() over(partition by f.bucket_start order by f.qv_ratio16) qp
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


robustness._threshold_candidate_sets = _threshold_candidate_sets_narrow
