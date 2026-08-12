from app.option_vol_analysis import bh_fdr, cluster_bootstrap_high_minus_control, destructive_tail_checks


def test_bh_fdr_adjusts_three_buckets_monotonically():
    result = bh_fdr({"0dte": 0.01, "1_2dte": 0.02, "5_9dte": 0.20})
    assert result["0dte"]["p_adjusted"] <= result["1_2dte"]["p_adjusted"]
    assert result["0dte"]["reject"] is True
    assert result["5_9dte"]["reject"] is False


def test_cluster_bootstrap_preserves_session_clusters_and_positive_difference():
    rows = [
        {"session_date": "2026-07-01", "sample_class": "high", "gross_return": 0.10},
        {"session_date": "2026-07-01", "sample_class": "control", "gross_return": 0.00},
        {"session_date": "2026-07-02", "sample_class": "high", "gross_return": 0.08},
        {"session_date": "2026-07-02", "sample_class": "control", "gross_return": -0.01},
        {"session_date": "2026-07-03", "sample_class": "high", "gross_return": 0.06},
        {"session_date": "2026-07-03", "sample_class": "control", "gross_return": 0.01},
    ]
    result = cluster_bootstrap_high_minus_control(rows, resamples=1000, seed=7)
    assert result["sessions"] == 3
    assert result["high_n"] == 3
    assert result["control_n"] == 3
    assert result["mean_difference"] > 0
    assert result["ci_low"] > 0


def test_destructive_tail_checks_remove_favorable_high_tail():
    rows = [
        {"sample_class": "high", "gross_return": 1.0},
        {"sample_class": "high", "gross_return": 0.2},
        {"sample_class": "high", "gross_return": 0.1},
        {"sample_class": "control", "gross_return": 0.0},
        {"sample_class": "control", "gross_return": 0.0},
    ]
    result = destructive_tail_checks(rows)
    assert result["full"] > result["remove_best_1"]
    assert result["remove_best_5"] is None
