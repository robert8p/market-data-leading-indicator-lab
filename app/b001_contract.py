from __future__ import annotations

import math
import statistics
from dataclasses import dataclass
from datetime import datetime
from typing import Iterable, Sequence

RULE_VERSION = "B-001-frozen-2026-08-08"
DISCOVERY_START = datetime.fromisoformat("2026-06-28T00:00:00+00:00")
DISCOVERY_END = datetime.fromisoformat("2026-07-28T16:30:00+00:00")
REPLICATION_END = datetime.fromisoformat("2026-06-28T00:00:00+00:00")

DISPERSION_MAX = 0.00573317406149824
FINAL_5M_MAX = -0.0102328142205443
HIGH_TO_CLOSE_MIN = 0.0487464571796793
CLOSE_VS_VWAP_MAX = -0.0151343875084486
EXTREME_PERCENTILE = 0.90
BTC_HEDGE_WEIGHT = 0.75
HOLD_HOURS = 8
TOKEN_COST_BP = 22.0
BTC_HEDGE_COST_BP = 16.5
PRIMARY_COMBINED_COST_BP = 38.5
STRESS_COSTS_BP = (50.0, 75.0, 100.0, 150.0, 200.0)
LIQUIDITY_LOOKBACK_DAYS = 18
LIQUIDITY_PERCENTILE_MIN = 0.50

EXACT_THRESHOLDS = {
    "dispersion_max": DISPERSION_MAX,
    "final_5m_return_max": FINAL_5M_MAX,
    "high_to_close_rejection_min": HIGH_TO_CLOSE_MIN,
    "close_vs_vwap_max": CLOSE_VS_VWAP_MAX,
    "cross_sectional_extreme_percentile": EXTREME_PERCENTILE,
    "sequence": {"extreme_at_lags_15m": [0, 1, 2], "must_be_false_at_lag_15m": 5},
    "ret15_max": 0.0,
    "range_contracts_vs_previous": True,
    "minute_rejection_min_conditions": 2,
}

EXECUTION_SPEC = {
    "entry": "open_of_next_15m_bar_after_signal_completion",
    "exit": "open_at_entry_plus_8_hours",
    "hold_hours": HOLD_HOURS,
    "B-001a": {"token": "short_1.00", "btc": "long_0.75"},
    "B-001b": {"token": "short_1.00", "hedge": "none"},
    "B-001c": {
        "token": "short_1.00",
        "basket": "long_0.75_equal_weight_entry_time_liquid_half_excluding_signal_token",
        "provenance_note": "The original B-001c artefact was not available in the live repo/File Library. This comparison-only basket convention is frozen before historical outcomes are read; B-001a remains the primary replication test.",
    },
    "portfolio_overlap": "one_active_position_per_token; different-token positions are not suppressed",
    "classification_cost_gate": "positive mean and >50% hit rate at 50, 75 and 100 bp; 150 and 200 bp are reported stress only",
}

COST_SPEC = {
    "B-001a_primary_bp": PRIMARY_COMBINED_COST_BP,
    "B-001b_primary_bp": TOKEN_COST_BP,
    "B-001c_primary_bp": PRIMARY_COMBINED_COST_BP,
    "B-001a_stress_bp": list(STRESS_COSTS_BP),
}


@dataclass(frozen=True)
class MetricInput:
    symbol: str
    signal_ts: datetime
    net_return: float
    mae: float | None = None
    mfe: float | None = None
    concurrency: int | None = None


def percentile(values: Sequence[float], q: float) -> float | None:
    if not values:
        return None
    ordered = sorted(float(v) for v in values)
    if len(ordered) == 1:
        return ordered[0]
    position = (len(ordered) - 1) * q
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    weight = position - lower
    return ordered[lower] * (1.0 - weight) + ordered[upper] * weight


def wilson_interval(wins: int, n: int, z: float = 1.959963984540054) -> tuple[float | None, float | None]:
    if n <= 0:
        return None, None
    p = wins / n
    denom = 1.0 + (z * z) / n
    centre = (p + (z * z) / (2.0 * n)) / denom
    half = z * math.sqrt((p * (1.0 - p) / n) + (z * z) / (4.0 * n * n)) / denom
    return centre - half, centre + half


def max_drawdown_fixed_nominal(returns: Sequence[float]) -> float:
    cumulative = 0.0
    peak = 0.0
    worst = 0.0
    for value in returns:
        cumulative += float(value)
        peak = max(peak, cumulative)
        worst = min(worst, cumulative - peak)
    return worst


def longest_losing_streak(returns: Sequence[float]) -> int:
    longest = 0
    current = 0
    for value in returns:
        if value < 0:
            current += 1
            longest = max(longest, current)
        else:
            current = 0
    return longest


def calculate_metrics(rows: Iterable[MetricInput]) -> dict:
    ordered = sorted(list(rows), key=lambda row: (row.signal_ts, row.symbol))
    returns = [float(row.net_return) for row in ordered]
    winners = [value for value in returns if value > 0]
    losers = [value for value in returns if value < 0]
    n = len(returns)
    wins = len(winners)
    losses = len(losers)
    hit_rate = wins / n if n else None
    ci_low, ci_high = wilson_interval(wins, n)
    avg_winner = statistics.fmean(winners) if winners else None
    avg_loser = statistics.fmean(losers) if losers else None
    worst_loss = min(losers) if losers else 0.0 if n else None
    worst_loss_ratio = (
        abs(worst_loss) / avg_winner
        if worst_loss is not None and avg_winner is not None and avg_winner > 0
        else None
    )
    bounded_losses = (
        sum(1 for value in losers if abs(value) <= 0.10 * avg_winner) / losses
        if losses and avg_winner is not None and avg_winner > 0
        else 1.0 if not losses and n else None
    )
    profit_factor = (
        sum(winners) / abs(sum(losers))
        if losers and abs(sum(losers)) > 0
        else math.inf if winners else None
    )
    symbols: dict[str, int] = {}
    dates = set()
    for row in ordered:
        symbols[row.symbol] = symbols.get(row.symbol, 0) + 1
        dates.add(row.signal_ts.date().isoformat())
    gaps = [
        (ordered[index].signal_ts - ordered[index - 1].signal_ts).total_seconds()
        for index in range(1, len(ordered))
    ]
    maes = [float(row.mae) for row in ordered if row.mae is not None]
    mfes = [float(row.mfe) for row in ordered if row.mfe is not None]
    concurrencies = [int(row.concurrency) for row in ordered if row.concurrency is not None]
    return {
        "n_trades": n,
        "winning_trades": wins,
        "losing_trades": losses,
        "flat_trades": n - wins - losses,
        "n_symbols": len(symbols),
        "n_signal_dates": len(dates),
        "max_signals_one_symbol": max(symbols.values()) if symbols else 0,
        "hit_rate": hit_rate,
        "wilson_95_low": ci_low,
        "wilson_95_high": ci_high,
        "mean_net_return": statistics.fmean(returns) if returns else None,
        "median_net_return": statistics.median(returns) if returns else None,
        "average_winner": avg_winner,
        "median_winner": statistics.median(winners) if winners else None,
        "average_loser": avg_loser,
        "median_loser": statistics.median(losers) if losers else None,
        "profit_factor": None if profit_factor is None else ("Infinity" if math.isinf(profit_factor) else profit_factor),
        "worst_individual_net_loss": worst_loss,
        "p05_net_return": percentile(returns, 0.05),
        "p01_net_return": percentile(returns, 0.01),
        "maximum_adverse_excursion": min(maes) if maes else None,
        "maximum_favourable_excursion": max(mfes) if mfes else None,
        "worst_loss_ratio": worst_loss_ratio,
        "losers_within_10pct_of_avg_winner_pct": bounded_losses,
        "cumulative_return_fixed_nominal": sum(returns) if returns else 0.0,
        "maximum_drawdown_fixed_nominal": max_drawdown_fixed_nominal(returns),
        "longest_losing_streak": longest_losing_streak(returns),
        "longest_seconds_between_signals": max(gaps) if gaps else None,
        "max_concurrency": max(concurrencies) if concurrencies else None,
        "average_concurrency": statistics.fmean(concurrencies) if concurrencies else None,
    }
