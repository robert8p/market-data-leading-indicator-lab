from datetime import date, datetime, timezone

from app.option_vol_research import exact_straddle_result, select_contract_pairs


def _contract(symbol: str, expiry: str, strike: float, option_type: str, oi: int = 100):
    return {
        "symbol": symbol,
        "expiration_date": expiry,
        "strike_price": str(strike),
        "type": option_type,
        "open_interest": str(oi),
    }


def test_select_contract_pairs_uses_earliest_expiry_then_nearest_atm():
    contracts = [
        _contract("C0_499", "2026-07-10", 499, "call"),
        _contract("P0_499", "2026-07-10", 499, "put"),
        _contract("C0_500", "2026-07-10", 500, "call"),
        _contract("P0_500", "2026-07-10", 500, "put"),
        _contract("C1_500", "2026-07-11", 500, "call"),
        _contract("P1_500", "2026-07-11", 500, "put"),
        _contract("C2_500", "2026-07-12", 500, "call"),
        _contract("P2_500", "2026-07-12", 500, "put"),
        _contract("C5_500", "2026-07-15", 500, "call"),
        _contract("P5_500", "2026-07-15", 500, "put"),
        _contract("C7_500", "2026-07-17", 500, "call"),
        _contract("P7_500", "2026-07-17", 500, "put"),
    ]
    buckets = {
        "0dte": {"min": 0, "max": 0},
        "1_2dte": {"min": 1, "max": 2},
        "5_9dte": {"min": 5, "max": 9},
    }
    selected = select_contract_pairs(
        contracts, entry_date=date(2026, 7, 10), spot=499.6, dte_buckets=buckets
    )
    by_bucket = {item["dte_bucket"]: item for item in selected}
    assert by_bucket["0dte"]["strike"] == 500
    assert by_bucket["1_2dte"]["expiration_date"] == date(2026, 7, 11)
    assert by_bucket["5_9dte"]["expiration_date"] == date(2026, 7, 15)
    assert all(item["metadata"]["open_interest_used_for_selection"] is False for item in selected)


def test_select_contract_pairs_requires_both_call_and_put():
    contracts = [
        _contract("C0_500", "2026-07-10", 500, "call"),
        _contract("C0_501", "2026-07-10", 501, "call"),
        _contract("P0_501", "2026-07-10", 501, "put"),
    ]
    selected = select_contract_pairs(
        contracts,
        entry_date=date(2026, 7, 10),
        spot=500.1,
        dte_buckets={"0dte": {"min": 0, "max": 0}},
    )
    assert selected[0]["strike"] == 501


def test_exact_straddle_result_uses_exact_entry_open_and_exit_close():
    entry = datetime(2026, 7, 10, 14, 1, tzinfo=timezone.utc)
    exit_ts = datetime(2026, 7, 10, 14, 30, tzinfo=timezone.utc)
    rows = [
        {"contract_symbol": "CALL", "ts": entry, "open": 2.0, "close": 2.1},
        {"contract_symbol": "PUT", "ts": entry, "open": 1.5, "close": 1.4},
        {"contract_symbol": "CALL", "ts": exit_ts, "open": 2.4, "close": 2.5},
        {"contract_symbol": "PUT", "ts": exit_ts, "open": 1.2, "close": 1.0},
    ]
    result = exact_straddle_result(
        rows,
        call_symbol="CALL",
        put_symbol="PUT",
        entry_ts=entry,
        exit_ts=exit_ts,
        spy_open=500.0,
    )
    assert result["complete"] is True
    assert result["entry_straddle"] == 3.5
    assert result["exit_straddle"] == 3.5
    assert result["gross_return"] == 0.0
    assert result["premium_to_spy"] == 0.007
    assert result["notes"]["spread_slippage_commission_included"] is False


def test_exact_straddle_result_rejects_stale_or_missing_exact_minute():
    entry = datetime(2026, 7, 10, 14, 1, tzinfo=timezone.utc)
    exit_ts = datetime(2026, 7, 10, 14, 30, tzinfo=timezone.utc)
    rows = [
        {"contract_symbol": "CALL", "ts": entry, "open": 2.0, "close": 2.1},
        {"contract_symbol": "PUT", "ts": entry, "open": 1.5, "close": 1.4},
        {"contract_symbol": "CALL", "ts": exit_ts, "open": 2.4, "close": 2.5},
    ]
    result = exact_straddle_result(
        rows,
        call_symbol="CALL",
        put_symbol="PUT",
        entry_ts=entry,
        exit_ts=exit_ts,
        spy_open=500.0,
    )
    assert result["complete"] is False
    assert "put_exit" in result["notes"]["missing_exact_bars"]
