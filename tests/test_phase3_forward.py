from __future__ import annotations

import math

import pytest

from app.phase3_forward import _valid_quote, eqmdef_f30, first_30m_return, opening_return


def test_opening_return() -> None:
    assert opening_return(99.0, 100.0) == pytest.approx(-0.01)
    with pytest.raises(ValueError):
        opening_return(0.0, 100.0)


def test_first_30m_return() -> None:
    assert first_30m_return(100.0, 101.0) == pytest.approx(0.01)
    with pytest.raises(ValueError):
        first_30m_return(100.0, -1.0)


def test_eqmdef_matches_frozen_formula() -> None:
    values = {
        "SPY": 0.010,
        "QQQ": 0.012,
        "IWM": 0.008,
        "GLD": -0.002,
        "TLT": -0.004,
    }
    expected = (0.010 + 0.012 + 0.008) / 3.0 - (-0.002 - 0.004) / 2.0
    assert eqmdef_f30(values) == pytest.approx(expected)
    assert math.isfinite(eqmdef_f30(values))


def test_eqmdef_requires_all_five_frozen_legs() -> None:
    with pytest.raises(ValueError, match="missing state legs"):
        eqmdef_f30({"SPY": 0.0, "QQQ": 0.0, "IWM": 0.0, "GLD": 0.0})


def test_quote_validation_rejects_crossed_or_nonpositive_quotes() -> None:
    assert _valid_quote({"bp": 100.0, "ap": 100.01, "t": "2026-08-25T14:01:00Z"})
    assert not _valid_quote({"bp": 100.02, "ap": 100.01, "t": "2026-08-25T14:01:00Z"})
    assert not _valid_quote({"bp": 0.0, "ap": 100.01, "t": "2026-08-25T14:01:00Z"})
    assert not _valid_quote({"bp": None, "ap": None, "t": "2026-08-25T14:01:00Z"})
