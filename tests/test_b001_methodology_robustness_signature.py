from __future__ import annotations

import app.b001_methodology_hardening as hardening


def test_robustness_wrapper_forwards_signals(monkeypatch):
    seen = {}

    def fake_original(run_id, signals):
        seen["run_id"] = run_id
        seen["signals"] = signals
        return {"base": {"sample_size": len(signals)}}

    monkeypatch.setattr(hardening, "_ORIGINAL_ROBUSTNESS", fake_original)
    monkeypatch.setattr(hardening, "STRESS_COSTS_BP", ())

    signals = [{"symbol": "TESTUSDT"}]
    result = hardening._robustness("run-1", signals)

    assert seen == {"run_id": "run-1", "signals": signals}
    assert result == {"base": {"sample_size": 1}}
