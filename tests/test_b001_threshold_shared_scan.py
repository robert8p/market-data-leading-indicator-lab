from __future__ import annotations

from datetime import datetime, timezone

import app.b001_robustness_resilience as resilience
import app.b001_threshold_scan_narrow as narrow


def test_threshold_shared_scan_applies_exact_variant_comparisons(monkeypatch):
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    end = datetime(2026, 1, 20, tzinfo=timezone.utc)
    monkeypatch.setattr(
        resilience,
        "fetch_one",
        lambda *args, **kwargs: {"requested_start": start, "requested_end": end},
    )
    calls = []

    def fake_fetch(sql, params):
        calls.append((sql, params))
        return [
            {
                "symbol": "TESTUSDT",
                "bucket_start": start,
                "final_5m_return": resilience.FINAL_5M_MAX,
                "high_to_close_rejection": resilience.HIGH_TO_CLOSE_MIN,
                "close_vs_vwap": resilience.CLOSE_VS_VWAP_MAX,
                "dispersion15": resilience.DISPERSION_MAX,
            }
        ]

    monkeypatch.setattr(narrow, "_fetch_rank_rows", fake_fetch)
    specs = [
        ("threshold_dispersion", "x1.1", {"dispersion_max": resilience.DISPERSION_MAX * 1.1}),
        ("threshold_dispersion", "x0.8", {"dispersion_max": resilience.DISPERSION_MAX * 0.8}),
    ]
    result = resilience._threshold_candidate_sets(
        "00000000-0000-0000-0000-000000000001", specs
    )
    assert len(calls) == 1
    assert result["threshold_dispersion:x1.1"] == [("TESTUSDT", start)]
    assert result["threshold_dispersion:x0.8"] == []
    assert "percent_rank()" in calls[0][0]
