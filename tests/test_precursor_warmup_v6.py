from datetime import datetime, timezone

from app.precursor_warmup_v6 import (
    derive_rows,
    normalize_bars,
    reference_to_api_symbol,
)


def test_preferred_share_translation_only_changes_lowercase_p_marker():
    assert reference_to_api_symbol("WFCpC") == "WFC.PRC"
    assert reference_to_api_symbol("VOYApB") == "VOYA.PRB"
    assert reference_to_api_symbol("BRK.A") == "BRK.A"
    assert reference_to_api_symbol("BF.B") == "BF.B"
    assert reference_to_api_symbol("AAPL") == "AAPL"


def test_normalize_and_derive_regular_session_rows():
    start = datetime(2025, 8, 29, 13, 30, tzinfo=timezone.utc)
    close = datetime(2025, 8, 29, 20, 0, tzinfo=timezone.utc)
    raw = [
        {"t": "2025-08-29T13:30:00Z", "o": 10, "h": 11, "l": 9.5, "c": 10.5, "v": 100, "n": 5, "vw": 10.2},
        {"t": "2025-08-29T19:30:00Z", "o": 10.6, "h": 12, "l": 10.4, "c": 11.8, "v": 200, "n": 8, "vw": 11.5},
    ]
    bars = normalize_bars("TEST", raw, start, close)
    rows = derive_rows(
        "TEST", "TEST", bars, "2025-08-29", close, "a" * 64, 1,
        start, close, datetime(2026, 10, 1, 0, 0, tzinfo=timezone.utc),
    )
    full = next(r for r in rows if r["source_mode"] == "RTH_30MIN_AGGREGATED")
    closing = next(r for r in rows if r["source_mode"] == "CLOSING_30MIN")

    assert full["open"] == 10
    assert full["high"] == 12
    assert full["low"] == 9.5
    assert full["close"] == 11.8
    assert full["volume"] == 300
    assert full["trade_count"] == 13
    assert closing["close"] == 11.8
    assert closing["source_period_volume"] == 200
    assert closing["open"] is None
    assert closing["available_at"] == "2025-08-29T20:01:00+00:00"


def test_early_close_closing_bar_is_derived_from_calendar_close():
    start = datetime(2025, 7, 3, 13, 30, tzinfo=timezone.utc)
    close = datetime(2025, 7, 3, 17, 0, tzinfo=timezone.utc)
    raw = [
        {"t": "2025-07-03T13:30:00Z", "o": 10, "h": 10.5, "l": 9.8, "c": 10.2, "v": 100, "n": 4},
        {"t": "2025-07-03T16:30:00Z", "o": 10.2, "h": 10.8, "l": 10.1, "c": 10.7, "v": 150, "n": 6},
    ]
    bars = normalize_bars("TEST", raw, start, close)
    rows = derive_rows(
        "TEST", "TEST", bars, "2025-07-03", close, "b" * 64, 2,
        start, close, datetime(2026, 10, 1, 0, 0, tzinfo=timezone.utc),
    )
    closing = next(r for r in rows if r["source_mode"] == "CLOSING_30MIN")
    assert closing["close"] == 10.7
    assert closing["event_time"] == "2025-07-03T17:00:00+00:00"
    assert closing["available_at"] == "2025-07-03T17:01:00+00:00"
