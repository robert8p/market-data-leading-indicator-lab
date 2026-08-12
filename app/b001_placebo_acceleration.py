from __future__ import annotations

"""Set-based execution for two frozen B-001 falsification placebos.

This module changes only query execution shape. It preserves exactly the existing
placebo definitions, deterministic control selection, entry/exit timestamps,
BTC hedge, holding period and transaction cost. The purpose is to replace
thousands of small database round trips with bounded set queries so the durable
falsification phase remains practical under shared database load.
"""

from datetime import timedelta
from typing import Any
from uuid import UUID

import app.b001_analysis as analysis
from app.b001_contract import BTC_HEDGE_WEIGHT, PRIMARY_COMBINED_COST_BP, MetricInput
from app.db import fetch_all, fetch_one


_TIMESTAMP_SHIFTS = (-60, -30, -15, 15, 30, 60)
_SYMBOL_SEED_PREFIX = "B001_SYMBOL_PLACEBO_V1:"


def _primary_cost_fraction() -> float:
    return float(PRIMARY_COMBINED_COST_BP) / 10_000.0


def timestamp_placebos_set_based(run_id: UUID, _signals: list[dict[str, Any]]) -> None:
    """Exact replacement for analysis._timestamp_placebos using six set queries."""
    for shift in _TIMESTAMP_SHIFTS:
        rows = fetch_all(
            """
            select
                s.symbol,
                s.bucket_start + (%s * interval '1 minute') + interval '15 minutes' as entry_ts,
                1.0 - tx.open/nullif(te.open,0)
                  + %s * (bx.open/nullif(be.open,0)-1.0)
                  - %s as net_return
            from crypto_b001_replication_signals s
            join crypto_b001_replication_15m te
              on te.run_id=s.run_id
             and te.symbol=s.symbol
             and te.bucket_start=s.bucket_start + (%s * interval '1 minute') + interval '15 minutes'
            join crypto_b001_replication_15m tx
              on tx.run_id=te.run_id
             and tx.symbol=te.symbol
             and tx.bucket_start=s.bucket_start + (%s * interval '1 minute') + interval '8 hours 15 minutes'
            join crypto_b001_replication_15m be
              on be.run_id=s.run_id
             and be.symbol='BTCUSDT'
             and be.bucket_start=s.bucket_start + (%s * interval '1 minute') + interval '15 minutes'
            join crypto_b001_replication_15m bx
              on bx.run_id=s.run_id
             and bx.symbol='BTCUSDT'
             and bx.bucket_start=s.bucket_start + (%s * interval '1 minute') + interval '8 hours 15 minutes'
            where s.run_id=%s
            order by s.bucket_start,s.symbol
            """,
            (
                shift,
                BTC_HEDGE_WEIGHT,
                _primary_cost_fraction(),
                shift,
                shift,
                shift,
                shift,
                run_id,
            ),
        )
        metric_rows = [
            MetricInput(
                symbol=row["symbol"],
                signal_ts=row["entry_ts"],
                net_return=float(row["net_return"]),
            )
            for row in rows
        ]
        analysis._persist_placebo(
            run_id,
            "timestamp",
            f"shift_{shift:+d}m",
            metric_rows,
            {
                "shift_minutes": shift,
                "execution": "set_based_exact_equivalence",
            },
        )


def symbol_placebo_set_based(run_id: UUID, _signals: list[dict[str, Any]]) -> None:
    """Exact deterministic five-control symbol placebo in calendar-month chunks."""
    run = fetch_one(
        "select requested_start,requested_end from crypto_b001_replication_runs where id=%s",
        (run_id,),
    )
    if not run:
        raise RuntimeError("B-001 replication run disappeared during symbol placebo")

    metric_rows: list[MetricInput] = []
    cursor = run["requested_start"].replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    while cursor < run["requested_end"]:
        month_end = (cursor + timedelta(days=32)).replace(day=1)
        start = max(cursor, run["requested_start"])
        end = min(month_end, run["requested_end"])
        rows = fetch_all(
            """
            with sig as (
                select id,run_id,symbol,bucket_start
                from crypto_b001_replication_signals
                where run_id=%s and bucket_start >= %s and bucket_start < %s
            ), chosen as (
                select
                    s.id as signal_id,
                    s.symbol as signal_symbol,
                    s.bucket_start,
                    c.symbol,
                    c.control_order
                from sig s
                cross join lateral (
                    select
                        f.symbol,
                        md5(f.symbol || (%s || s.id::text)) as control_order
                    from crypto_b001_replication_features f
                    where f.run_id=s.run_id
                      and f.bucket_start=s.bucket_start
                      and f.liquidity_eligible
                      and f.symbol<>s.symbol
                      and not exists (
                          select 1
                          from crypto_b001_replication_signals x
                          where x.run_id=f.run_id
                            and x.bucket_start=f.bucket_start
                            and x.symbol=f.symbol
                      )
                    order by md5(f.symbol || (%s || s.id::text))
                    limit 5
                ) c
            )
            select
                c.symbol,
                c.bucket_start + interval '15 minutes' as entry_ts,
                1.0 - tx.open/nullif(te.open,0)
                  + %s * (bx.open/nullif(be.open,0)-1.0)
                  - %s as net_return
            from chosen c
            join crypto_b001_replication_15m te
              on te.run_id=%s
             and te.symbol=c.symbol
             and te.bucket_start=c.bucket_start+interval '15 minutes'
            join crypto_b001_replication_15m tx
              on tx.run_id=te.run_id
             and tx.symbol=te.symbol
             and tx.bucket_start=c.bucket_start+interval '8 hours 15 minutes'
            join crypto_b001_replication_15m be
              on be.run_id=te.run_id
             and be.symbol='BTCUSDT'
             and be.bucket_start=c.bucket_start+interval '15 minutes'
            join crypto_b001_replication_15m bx
              on bx.run_id=te.run_id
             and bx.symbol='BTCUSDT'
             and bx.bucket_start=c.bucket_start+interval '8 hours 15 minutes'
            order by c.bucket_start,c.signal_symbol,c.signal_id,c.control_order
            """,
            (
                run_id,
                start,
                end,
                _SYMBOL_SEED_PREFIX,
                _SYMBOL_SEED_PREFIX,
                BTC_HEDGE_WEIGHT,
                _primary_cost_fraction(),
                run_id,
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
        cursor = month_end

    analysis._persist_placebo(
        run_id,
        "symbol",
        "five_deterministic_non_signal_controls",
        metric_rows,
        {
            "controls_per_signal": 5,
            "seed": "B001_SYMBOL_PLACEBO_V1",
            "execution": "calendar_month_set_based_exact_equivalence",
        },
    )


def install() -> None:
    analysis._timestamp_placebos = timestamp_placebos_set_based
    analysis._symbol_placebo = symbol_placebo_set_based


install()
