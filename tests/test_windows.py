from datetime import datetime, timedelta, timezone

from app.jobs import _provider_windows


def test_coinbase_windows_never_exceed_300_minutes():
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    end = start + timedelta(days=2)
    windows = list(_provider_windows("coinbase", start, end))
    assert windows
    assert all((right - left) <= timedelta(minutes=300) for left, right in windows)
    assert windows[0][0] == start
    assert windows[-1][1] == end


def test_binance_windows_never_exceed_999_minutes():
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    end = start + timedelta(days=3)
    windows = list(_provider_windows("binance", start, end))
    assert all((right - left) <= timedelta(minutes=999) for left, right in windows)
    assert windows[-1][1] == end


def test_alpaca_skips_weekends():
    # Friday through Monday end: Friday and Monday windows only.
    start = datetime(2026, 1, 2, 5, tzinfo=timezone.utc)
    end = datetime(2026, 1, 6, 5, tzinfo=timezone.utc)
    windows = list(_provider_windows("alpaca", start, end))
    weekdays = [left.weekday() for left, _ in windows]
    assert weekdays == [4, 0]


def test_twelvedata_three_day_windows():
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    end = start + timedelta(days=8)
    windows = list(_provider_windows("twelvedata", start, end))
    assert len(windows) == 3
    assert all((right - left) <= timedelta(days=3) for left, right in windows)


def test_alpaca_standard_time_window_keeps_friday_postmarket_hour():
    # A New York Friday ends at 05:00 UTC on Saturday during standard time.
    start = datetime(2026, 1, 9, 5, tzinfo=timezone.utc)
    end = datetime(2026, 1, 10, 6, tzinfo=timezone.utc)
    windows = list(_provider_windows("alpaca", start, end))
    assert len(windows) == 1
    assert windows[0][1] == datetime(2026, 1, 10, 5, tzinfo=timezone.utc)
