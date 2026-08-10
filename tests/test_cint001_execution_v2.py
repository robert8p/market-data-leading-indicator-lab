from __future__ import annotations

from app.cint001_contract import (
    EXPECTED_VALIDATION_MEMBERS,
    MASTER_UNIVERSE,
    VALIDATION_ACTIVE_UNIVERSE,
)
from app.cint001_execution_v2 import _adjusted_futures_price_sql


def test_validation_active_universe_is_frozen_from_raw_availability_only():
    assert len(MASTER_UNIVERSE) == 30
    assert len(VALIDATION_ACTIVE_UNIVERSE) == EXPECTED_VALIDATION_MEMBERS == 26
    assert set(MASTER_UNIVERSE) - set(VALIDATION_ACTIVE_UNIVERSE) == {
        "BETAUSDT",
        "BNXUSDT",
        "FTMUSDT",
        "RNDRUSDT",
    }


def test_basis_adjustment_handles_1000_contract_units_without_changing_return_math():
    sql = _adjusted_futures_price_sql(
        "o.futures_entry", "o.spot_symbol", "o.futures_symbol"
    )
    assert "o.futures_symbol" in sql
    assert "o.spot_symbol" in sql
    assert "o.futures_entry/1000.0" in sql
