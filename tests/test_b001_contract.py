from datetime import datetime, timezone

from app.b001_contract import (
    BTC_HEDGE_WEIGHT,
    CLOSE_VS_VWAP_MAX,
    DISPERSION_MAX,
    EXTREME_PERCENTILE,
    FINAL_5M_MAX,
    HIGH_TO_CLOSE_MIN,
    HOLD_HOURS,
    PRIMARY_COMBINED_COST_BP,
    RULE_VERSION,
    MetricInput,
    calculate_metrics,
    wilson_interval,
)


def test_frozen_b001_constants_are_exact():
    assert RULE_VERSION == "B-001-frozen-2026-08-08"
    assert DISPERSION_MAX == 0.00573317406149824
    assert FINAL_5M_MAX == -0.0102328142205443
    assert HIGH_TO_CLOSE_MIN == 0.0487464571796793
    assert CLOSE_VS_VWAP_MAX == -0.0151343875084486
    assert EXTREME_PERCENTILE == 0.90
    assert BTC_HEDGE_WEIGHT == 0.75
    assert HOLD_HOURS == 8
    assert PRIMARY_COMBINED_COST_BP == 38.5


def test_wilson_interval_is_bounded():
    low, high = wilson_interval(7, 10)
    assert 0 < low < 0.7 < high < 1


def test_worst_loss_ratio_and_loss_bounding_are_exact():
    ts = datetime(2025, 1, 1, tzinfo=timezone.utc)
    rows = [
        MetricInput("AUSDT", ts, 0.10),
        MetricInput("BUSDT", ts, 0.12),
        MetricInput("CUSDT", ts, -0.01),
    ]
    metrics = calculate_metrics(rows)
    assert metrics["average_winner"] == 0.11
    assert abs(metrics["worst_loss_ratio"] - (0.01 / 0.11)) < 1e-12
    assert metrics["losers_within_10pct_of_avg_winner_pct"] == 1.0


def test_loss_bounding_fails_when_any_loss_exceeds_ten_percent_of_average_winner():
    ts = datetime(2025, 1, 1, tzinfo=timezone.utc)
    rows = [
        MetricInput("AUSDT", ts, 0.10),
        MetricInput("BUSDT", ts, 0.10),
        MetricInput("CUSDT", ts, -0.011),
    ]
    metrics = calculate_metrics(rows)
    assert metrics["worst_loss_ratio"] > 0.10
    assert metrics["losers_within_10pct_of_avg_winner_pct"] == 0.0


def test_no_loser_case_has_zero_worst_loss_ratio():
    ts = datetime(2025, 1, 1, tzinfo=timezone.utc)
    metrics = calculate_metrics([MetricInput("AUSDT", ts, 0.02)])
    assert metrics["worst_individual_net_loss"] == 0.0
    assert metrics["worst_loss_ratio"] == 0.0
    assert metrics["losers_within_10pct_of_avg_winner_pct"] == 1.0
