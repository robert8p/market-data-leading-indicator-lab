from __future__ import annotations

import random
from collections import defaultdict
from statistics import median
from typing import Any, Iterable


BOOTSTRAP_SEED = 20260811
BOOTSTRAP_RESAMPLES = 10_000
FDR_Q = 0.05


def _mean(values: list[float]) -> float | None:
    return sum(values) / len(values) if values else None


def bh_fdr(pvalues: dict[str, float], q: float = FDR_Q) -> dict[str, dict[str, float | bool]]:
    """Benjamini-Hochberg decisions and monotone adjusted p-values."""
    ordered = sorted(pvalues.items(), key=lambda item: (item[1], item[0]))
    m = len(ordered)
    if not m:
        return {}
    raw_adjusted = [min(1.0, p * m / rank) for rank, (_, p) in enumerate(ordered, start=1)]
    adjusted = raw_adjusted[:]
    for index in range(m - 2, -1, -1):
        adjusted[index] = min(adjusted[index], adjusted[index + 1])
    return {
        name: {"p": p, "p_adjusted": adjusted[index], "reject": adjusted[index] <= q}
        for index, (name, p) in enumerate(ordered)
    }


def cluster_bootstrap_high_minus_control(
    rows: Iterable[dict[str, Any]],
    *,
    resamples: int = BOOTSTRAP_RESAMPLES,
    seed: int = BOOTSTRAP_SEED,
) -> dict[str, float | int | None]:
    """Bootstrap US trading-session clusters, preserving all within-day dependence."""
    by_day: dict[Any, list[dict[str, Any]]] = defaultdict(list)
    materialized = []
    for row in rows:
        if row.get("gross_return") is None:
            continue
        materialized.append(row)
        by_day[row["session_date"]].append(row)
    days = sorted(by_day, key=str)
    high = [float(row["gross_return"]) for row in materialized if row["sample_class"] == "high"]
    control = [float(row["gross_return"]) for row in materialized if row["sample_class"] == "control"]
    if not days or not high or not control:
        return {
            "sessions": len(days), "high_n": len(high), "control_n": len(control),
            "mean_difference": None, "median_difference": None,
            "ci_low": None, "ci_high": None, "p_two_sided": None,
        }
    observed = sum(high) / len(high) - sum(control) / len(control)
    median_difference = median(high) - median(control)
    rng = random.Random(seed)
    draws: list[float] = []
    for _ in range(resamples):
        sampled_days = [rng.choice(days) for _ in days]
        sampled_high: list[float] = []
        sampled_control: list[float] = []
        for day in sampled_days:
            for row in by_day[day]:
                if row["sample_class"] == "high":
                    sampled_high.append(float(row["gross_return"]))
                elif row["sample_class"] == "control":
                    sampled_control.append(float(row["gross_return"]))
        if sampled_high and sampled_control:
            draws.append(sum(sampled_high) / len(sampled_high) - sum(sampled_control) / len(sampled_control))
    if not draws:
        return {
            "sessions": len(days), "high_n": len(high), "control_n": len(control),
            "mean_difference": observed, "median_difference": median_difference,
            "ci_low": None, "ci_high": None, "p_two_sided": None,
        }
    draws.sort()
    lo_index = max(0, int(0.025 * (len(draws) - 1)))
    hi_index = min(len(draws) - 1, int(0.975 * (len(draws) - 1)))
    non_positive = sum(value <= 0 for value in draws) / len(draws)
    non_negative = sum(value >= 0 for value in draws) / len(draws)
    p_two_sided = min(1.0, 2.0 * min(non_positive, non_negative))
    return {
        "sessions": len(days),
        "high_n": len(high),
        "control_n": len(control),
        "mean_difference": observed,
        "median_difference": median_difference,
        "ci_low": draws[lo_index],
        "ci_high": draws[hi_index],
        "p_two_sided": p_two_sided,
    }


def destructive_tail_checks(rows: Iterable[dict[str, Any]]) -> dict[str, float | None]:
    """Compare high-minus-control mean after removing favorable high-state tails."""
    materialized = [row for row in rows if row.get("gross_return") is not None]
    controls = [float(row["gross_return"]) for row in materialized if row["sample_class"] == "control"]
    highs = sorted(
        [float(row["gross_return"]) for row in materialized if row["sample_class"] == "high"],
        reverse=True,
    )
    control_mean = _mean(controls)
    if control_mean is None or not highs:
        return {"full": None, "remove_best_1": None, "remove_best_5": None}

    def difference(values: list[float]) -> float | None:
        value = _mean(values)
        return None if value is None else value - control_mean

    return {
        "full": difference(highs),
        "remove_best_1": difference(highs[1:]),
        "remove_best_5": difference(highs[5:]),
    }
