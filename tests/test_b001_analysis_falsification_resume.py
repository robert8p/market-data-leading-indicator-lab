from datetime import datetime, timezone

import app.b001_analysis_resilience as resilience


def test_falsification_skips_completed_substeps(monkeypatch):
    calls = []
    monkeypatch.setattr(resilience, "_set_stage", lambda *args: calls.append("stage"))
    monkeypatch.setattr(resilience, "_checkpoint", lambda *args, **kwargs: calls.append("phase_checkpoint"))
    monkeypatch.setattr(resilience, "_checkpoint_substep", lambda *args, **kwargs: calls.append(args[1]))
    monkeypatch.setattr(resilience, "_low_dispersion_placebo_chunked", lambda *args: calls.append("low"))
    monkeypatch.setattr(resilience.analysis, "_timestamp_placebos", lambda *args: calls.append("timestamp"))
    monkeypatch.setattr(resilience.analysis, "_symbol_placebo", lambda *args: calls.append("symbol"))
    monkeypatch.setattr(resilience.analysis, "_component_ablations", lambda *args: calls.append("ablations"))
    monkeypatch.setattr(
        resilience.analysis,
        "_leave_out_tests",
        lambda *args: (True, {"ok": True}),
    )

    progress = {
        "analysis_falsification_initialized": True,
        "analysis_falsification_timestamp_placebos_complete": True,
        "analysis_falsification_symbol_placebo_complete": True,
    }
    passed, details = resilience._run_falsification_resumable(
        "run", 1, progress, []
    )

    assert "timestamp" not in calls
    assert "symbol" not in calls
    assert "low" in calls
    assert "ablations" in calls
    assert passed is True
    assert details == {"ok": True}


def test_low_dispersion_placebo_is_split_by_calendar_month(monkeypatch):
    monkeypatch.setattr(
        resilience,
        "fetch_one",
        lambda *args, **kwargs: {
            "requested_start": datetime(2025, 1, 15, tzinfo=timezone.utc),
            "requested_end": datetime(2025, 3, 2, tzinfo=timezone.utc),
        },
    )
    query_windows = []

    def fake_fetch_all(sql, params):
        query_windows.append((params[1], params[2]))
        start = params[1]
        return [{"symbol": "TESTUSDT", "bucket_start": start, "net_return": 0.01}]

    monkeypatch.setattr(resilience, "fetch_all", fake_fetch_all)
    persisted = []
    monkeypatch.setattr(
        resilience.analysis,
        "_persist_placebo",
        lambda run_id, placebo_type, variant, rows, details: persisted.append(
            (placebo_type, variant, list(rows), details)
        ),
    )

    resilience._low_dispersion_placebo_chunked("run")

    assert len(query_windows) == 3
    assert query_windows[0][0] == datetime(2025, 1, 15, tzinfo=timezone.utc)
    assert query_windows[0][1] == datetime(2025, 2, 1, tzinfo=timezone.utc)
    assert query_windows[-1][1] == datetime(2025, 3, 2, tzinfo=timezone.utc)
    assert len(persisted) == 1
    assert len(persisted[0][2]) == 3
    assert persisted[0][3]["execution"] == "calendar_month_chunks_exact_equivalence"
