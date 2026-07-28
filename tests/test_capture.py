from datetime import datetime, timedelta, timezone

from app.capture import detect_crypto_capture_windows, detect_equity_capture_windows


def _rows(start, prices, volumes):
    return [
        {
            "ts": start + timedelta(minutes=index),
            "open": price,
            "high": price,
            "low": price,
            "close": price,
            "volume": volumes[index],
        }
        for index, price in enumerate(prices)
    ]


def test_crypto_capture_detects_early_move_without_future_label():
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    prices = [100.0] * 30 + [100.2, 100.5, 101.0, 101.7, 102.0, 102.2]
    volumes = [100.0] * 30 + [200, 250, 300, 350, 400, 450]
    events = detect_crypto_capture_windows(
        _rows(start, prices, volumes),
        run_start=start,
        run_end=start + timedelta(hours=2),
        move_5m_pct=0.015,
        move_15m_pct=0.025,
        relative_volume_threshold=5.0,
        min_price=0.05,
        min_dollar_volume=10_000,
        cooldown_minutes=120,
        before_minutes=60,
        after_minutes=120,
    )
    assert events
    assert events[0]["trigger_kind"] in {"return_5m", "return_15m"}
    assert "future_return" not in events[0]["reason"]


def test_equity_capture_uses_regular_session():
    start = datetime(2026, 1, 2, 14, 30, tzinfo=timezone.utc)  # 09:30 New York in winter
    prices = [10.0] * 10 + [10.05, 10.1, 10.2, 10.25]
    volumes = [10_000.0] * len(prices)
    events = detect_equity_capture_windows(
        _rows(start, prices, volumes),
        run_start=start,
        run_end=start + timedelta(hours=7),
        move_pct=0.02,
        move_5m_pct=0.015,
        relative_volume_threshold=5.0,
        min_price=0.5,
        min_dollar_volume=10_000,
        cooldown_minutes=120,
        before_minutes=60,
        after_minutes=120,
    )
    assert events
    assert events[0]["reason"]["return_from_open"] >= 0.02


def test_equity_baseline_windows_are_deterministic_and_outcome_independent():
    from app.capture import detect_equity_baseline_windows

    start = datetime(2026, 1, 5, 15, 0, tzinfo=timezone.utc)  # 10:00 New York
    rows = _rows(start, [10.0] * 60, [1000.0] * 60)
    kwargs = dict(
        provider_symbol="ABC",
        run_start=start - timedelta(hours=1),
        run_end=start + timedelta(hours=8),
        sample_rate=1.0,
        seed="test-seed",
        before_minutes=120,
        after_minutes=120,
    )
    first = detect_equity_baseline_windows(rows, **kwargs)
    second = detect_equity_baseline_windows(rows, **kwargs)
    assert first == second
    assert len(first) == 1
    assert first[0]["selection_class"] == "baseline"
    assert first[0]["trigger_kind"] == "deterministic_baseline"
    assert "future_return" not in first[0]["reason"]
    assert first[0]["window_start"] <= first[0]["trigger_ts"] - timedelta(minutes=59)


def test_capture_cap_admission_is_not_alphabetical():
    from pathlib import Path

    source = (Path(__file__).parents[1] / "app" / "capture.py").read_text(encoding="utf-8")
    assert "def admission_order" in source
    assert "hashlib.sha256(raw.encode()).digest()" in source
    assert "key=admission_order" in source
