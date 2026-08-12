from __future__ import annotations

from datetime import datetime, timezone

import app.b001_threshold_scan_narrow as narrow


def test_threshold_scan_carries_only_required_feature_columns(monkeypatch):
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    monkeypatch.setattr(
        narrow.robustness,
        "fetch_one",
        lambda *args, **kwargs: {
            "requested_start": start,
            "requested_end": datetime(2026, 1, 20, tzinfo=timezone.utc),
        },
    )
    seen = []

    def fake_fetch_all(sql, params):
        seen.append(sql)
        return []

    monkeypatch.setattr(narrow.robustness, "fetch_all", fake_fetch_all)
    narrow._threshold_candidate_sets_narrow(
        "00000000-0000-0000-0000-000000000001",
        [("threshold_dispersion", "x1.1", {"dispersion_max": 1.0})],
    )
    sql = seen[0].lower()
    assert "select f.*" not in sql
    assert "f.final_5m_return" in sql
    assert "f.high_to_close_rejection" in sql
    assert "f.close_vs_vwap" in sql
    assert sql.count("percent_rank()") == 3
