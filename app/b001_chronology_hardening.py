from __future__ import annotations

from datetime import timedelta
from typing import Any
from uuid import UUID

import app.b001_analysis as analysis
from app.b001_contract import (
    BTC_HEDGE_WEIGHT,
    HOLD_HOURS,
    PRIMARY_COMBINED_COST_BP,
)
from app.db import fetch_one

# A completed 15-minute signal bar is only known at signal_ts.  The original
# implementation priced entry at that exact timestamp, which is the next
# bar's open but assumes zero compute/order latency.  Until a finer-grained
# executable-price model is available, use one full additional 15-minute bar
# as a conservative chronology guard.
ENTRY_DELAY_MINUTES = 15


def _cost(bp: float) -> float:
    return float(bp) / 10_000.0


def _signal_price_outcome(
    run_id: UUID,
    signal: dict[str, Any],
    hold_hours: int = HOLD_HOURS,
) -> dict[str, Any] | None:
    signal_ts = signal.get("signal_ts") or (signal["bucket_start"] + timedelta(minutes=15))
    entry_ts = signal_ts + timedelta(minutes=ENTRY_DELAY_MINUTES)
    exit_ts = entry_ts + timedelta(hours=hold_hours)
    row = fetch_one(
        """
        select te.open token_entry,tx.open token_exit,be.open btc_entry,bx.open btc_exit
        from crypto_b001_replication_15m te
        join crypto_b001_replication_15m tx
          on tx.run_id=te.run_id and tx.symbol=te.symbol and tx.bucket_start=%s
        join crypto_b001_replication_15m be
          on be.run_id=te.run_id and be.symbol='BTCUSDT' and be.bucket_start=%s
        join crypto_b001_replication_15m bx
          on bx.run_id=te.run_id and bx.symbol='BTCUSDT' and bx.bucket_start=%s
        where te.run_id=%s and te.symbol=%s and te.bucket_start=%s
        """,
        (exit_ts, entry_ts, exit_ts, run_id, signal["symbol"], entry_ts),
    )
    if not row:
        return None
    token_gross = 1.0 - float(row["token_exit"]) / float(row["token_entry"])
    btc_return = float(row["btc_exit"]) / float(row["btc_entry"]) - 1.0
    basket = fetch_one(
        """
        with members as (
            select symbol
            from crypto_b001_replication_features
            where run_id=%s and bucket_start=%s and liquidity_eligible and symbol<>%s
        ), outcomes as (
            select m.symbol,x.open/e.open-1.0 ret
            from members m
            join crypto_b001_replication_15m e
              on e.run_id=%s and e.symbol=m.symbol and e.bucket_start=%s
            join crypto_b001_replication_15m x
              on x.run_id=e.run_id and x.symbol=e.symbol and x.bucket_start=%s
        )
        select count(*) n,avg(ret) basket_return from outcomes
        """,
        (
            run_id,
            signal["bucket_start"],
            signal["symbol"],
            run_id,
            entry_ts,
            exit_ts,
        ),
    )
    return {
        "entry_ts": entry_ts,
        "exit_ts": exit_ts,
        "token_entry": float(row["token_entry"]),
        "token_exit": float(row["token_exit"]),
        "btc_entry": float(row["btc_entry"]),
        "btc_exit": float(row["btc_exit"]),
        "token_gross": token_gross,
        "btc_return": btc_return,
        "basket_return": (
            float(basket["basket_return"])
            if basket and basket.get("basket_return") is not None
            else None
        ),
        "basket_members": int(basket["n"]) if basket else 0,
    }


def _simple_b001a_outcome(
    run_id: UUID,
    symbol: str,
    signal_bucket,
    hold_hours: int = HOLD_HOURS,
    hedge_weight: float = BTC_HEDGE_WEIGHT,
    cost_bp: float = PRIMARY_COMBINED_COST_BP,
):
    entry = signal_bucket + timedelta(minutes=15 + ENTRY_DELAY_MINUTES)
    exit_ts = entry + timedelta(hours=hold_hours)
    row = fetch_one(
        """
        select te.open te,tx.open tx,be.open be,bx.open bx
        from crypto_b001_replication_15m te
        join crypto_b001_replication_15m tx
          on tx.run_id=te.run_id and tx.symbol=te.symbol and tx.bucket_start=%s
        join crypto_b001_replication_15m be
          on be.run_id=te.run_id and be.symbol='BTCUSDT' and be.bucket_start=%s
        join crypto_b001_replication_15m bx
          on bx.run_id=te.run_id and bx.symbol='BTCUSDT' and bx.bucket_start=%s
        where te.run_id=%s and te.symbol=%s and te.bucket_start=%s
        """,
        (exit_ts, entry, exit_ts, run_id, symbol, entry),
    )
    if not row:
        return None
    token = 1.0 - float(row["tx"]) / float(row["te"])
    btc = float(row["bx"]) / float(row["be"]) - 1.0
    return entry, token + hedge_weight * btc - _cost(cost_bp)


def _classification(
    run_id: UUID,
    robustness: dict[str, dict],
    concentration_pass: bool,
    concentration_details: dict,
):
    primary_row = fetch_one(
        """
        select metrics from crypto_b001_replication_metrics
        where run_id=%s and structure='B-001a' and position_mode='portfolio'
          and execution_subset='research' and cost_bp=%s and block='aggregate'
        """,
        (run_id, PRIMARY_COMBINED_COST_BP),
    )
    primary = (primary_row or {}).get("metrics") or {}
    blocks = []
    for block in ("1", "2", "3"):
        row = fetch_one(
            """
            select metrics from crypto_b001_replication_metrics
            where run_id=%s and structure='B-001a' and position_mode='portfolio'
              and execution_subset='research' and cost_bp=%s and block=%s
            """,
            (run_id, PRIMARY_COMBINED_COST_BP, block),
        )
        blocks.append((row or {}).get("metrics") or {})
    exec_row = fetch_one(
        """
        select metrics from crypto_b001_replication_metrics
        where run_id=%s and structure='B-001a' and position_mode='portfolio'
          and execution_subset='historically_executable' and cost_bp=%s and block='aggregate'
        """,
        (run_id, PRIMARY_COMBINED_COST_BP),
    )
    executable = (exec_row or {}).get("metrics") or {}

    n = int(primary.get("n_trades") or 0)
    hit = float(primary.get("hit_rate") or 0)
    mean = float(primary.get("mean_net_return") or 0)
    positive_blocks = sum(
        1
        for m in blocks
        if (m.get("hit_rate") or 0) > 0.5 and (m.get("mean_net_return") or 0) > 0
    )
    cost_gate = []
    for bp in (50.0, 75.0, 100.0):
        m = robustness.get(f"cost_stress:{bp:g}bp") or {}
        cost_gate.append((m.get("hit_rate") or 0) > 0.5 and (m.get("mean_net_return") or 0) > 0)
    cost_pass = all(cost_gate)
    exec_n = int(executable.get("n_trades") or 0)
    short_pass = (
        exec_n > 0
        and (executable.get("hit_rate") or 0) > 0.5
        and (executable.get("mean_net_return") or 0) > 0
    )

    score = {
        "hit_rate_gt_50": hit > 0.5,
        "positive_mean_net_return": mean > 0,
        "sufficient_independent_n": n >= 30,
        "multiple_chronological_blocks_positive": positive_blocks >= 2,
        "cost_stress": cost_pass,
        "shortability": short_pass,
        "no_one_or_two_symbol_dependence": concentration_pass,
        "loss_shape_constraint_removed": True,
        "entry_delay_minutes_after_signal_ts": ENTRY_DELAY_MINUTES,
        "primary_n": n,
        "positive_blocks": positive_blocks,
        "historically_executable_n": exec_n,
        "concentration_details": concentration_details,
    }
    core = score["hit_rate_gt_50"] and score["positive_mean_net_return"]
    all_a = (
        core
        and score["sufficient_independent_n"]
        and score["multiple_chronological_blocks_positive"]
        and cost_pass
        and short_pass
        and concentration_pass
    )
    if all_a:
        return (
            "A",
            "Chronology-corrected unseen-history replication satisfies the remaining economic, sample-size, chronological-consistency, cost, shortability and concentration gates. The former loss-shape constraint is not a promotion criterion.",
            score,
        )
    if n < 30 and core:
        return (
            "B",
            f"Primary economics remain promising, but the chronology-corrected replication produced only {n} independent portfolio trades (<30 required).",
            score,
        )
    if mean <= 0 and hit <= 0.5:
        return (
            "D",
            f"Chronology-corrected older unseen history materially falsifies B-001: aggregate primary mean net return is {mean:.6f} and hit rate is {hit:.2%}.",
            score,
        )
    failed = [key for key, value in score.items() if isinstance(value, bool) and not value]
    return (
        "C",
        "Predictive/economic structure remains visible but one or more remaining replication conditions fail: "
        + ", ".join(failed)
        + ".",
        score,
    )


def install() -> None:
    analysis._signal_price_outcome = _signal_price_outcome
    analysis._simple_b001a_outcome = _simple_b001a_outcome
    analysis._classification = _classification


install()
