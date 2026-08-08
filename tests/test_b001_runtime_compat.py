from __future__ import annotations


def test_canonical_runtime_preserves_legacy_hardening_interface() -> None:
    import app.b001_runtime as runtime

    assert callable(runtime._generate_signals)
    assert callable(runtime._candidate_rows_for_variant)
    assert runtime.DISPERSION_MAX > 0

    # This is the exact production import that previously crash-looped the worker.
    import app.b001_methodology_hardening as hardening

    assert callable(hardening.claim_b001_work)
    assert callable(hardening.process_b001_work)
    assert runtime.replication._process_spot_month.__name__ == "_process_spot_month_canonical"
