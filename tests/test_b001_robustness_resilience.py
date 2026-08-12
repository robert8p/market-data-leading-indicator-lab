from __future__ import annotations

from datetime import datetime, timezone

import app.b001_robustness_resilience as resilience


def test_variant_metrics_set_based_batches_candidates(monkeypatch):
    calls = []

    def fake_fetch_all(sql, params):
        calls.append((sql, params))
        symbols, buckets = params[0], params[1]
        return [
            {"symbol": symbol, "entry_ts": bucket, "net_return": 0.01}
            for symbol, bucket in zip(symbols, buckets)
        ]

    monkeypatch.setattr(resilience, "fetch_all", fake_fetch_all)
    monkeypatch.setattr(
        resilience,
        "calculate_metrics",
        lambda rows: {"n_trades": len(list(rows))},
    )
    candidates = [
        (f"S{i}USDT", datetime(2026, 1, 1, tzinfo=timezone.utc))
        for i in range(5)
    ]
    result = resilience._variant_metrics_set_based(
        "00000000-0000-0000-0000-000000000001",
        candidates,
        batch_size=2,
    )
    assert result["n_trades"] == 5
    assert len(calls) == 3
    assert "unnest" in calls[0][0].lower()


def test_robustness_variant_key_is_stable():
    assert (
        resilience._robustness_progress_key("threshold_final_5m", "x0.8")
        == "analysis_robustness_threshold_final_5m_x0_8_complete"
    )
