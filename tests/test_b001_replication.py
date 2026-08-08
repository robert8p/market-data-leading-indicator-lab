import csv
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

from app.b001_replication import _aggregate_archive, _timestamp_from_binance


def _write_archive(path: Path, missing_minute: int | None = None) -> None:
    csv_path = path.with_suffix(".csv")
    start = datetime(2025, 1, 1, tzinfo=timezone.utc)
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        for minute in range(30):
            if minute == missing_minute:
                continue
            ts = start + timedelta(minutes=minute)
            open_price = 100 + minute
            close_price = open_price + 0.5
            base_volume = 2.0
            quote_volume = close_price * base_volume
            writer.writerow([
                int(ts.timestamp() * 1_000), open_price, open_price + 1, open_price - 1, close_price,
                base_volume, int((ts + timedelta(minutes=1)).timestamp() * 1_000) - 1,
                quote_volume, 10, 1.0, quote_volume / 2, 0,
            ])
    with zipfile.ZipFile(path, "w") as archive:
        archive.write(csv_path, arcname="TESTUSDT-1m-2025-01.csv")


def test_binance_timestamp_supports_milliseconds_and_microseconds():
    expected = datetime(2025, 1, 1, tzinfo=timezone.utc)
    assert _timestamp_from_binance(str(int(expected.timestamp() * 1_000))) == expected
    assert _timestamp_from_binance(str(int(expected.timestamp() * 1_000_000))) == expected


def test_complete_archive_creates_only_complete_fifteen_minute_bars(tmp_path):
    archive = tmp_path / "complete.zip"
    _write_archive(archive)
    start = datetime(2025, 1, 1, tzinfo=timezone.utc)
    rows, stats = _aggregate_archive(archive, start, start + timedelta(minutes=30))
    assert len(rows) == 2
    assert stats["complete_15m"] == 2
    assert stats["incomplete_15m"] == 0
    assert all(row[2] == 15 for row in rows)
    assert rows[0][0] == start
    assert rows[0][1] == start + timedelta(minutes=15)


def test_missing_minute_drops_entire_fifteen_minute_bucket_without_forward_fill(tmp_path):
    archive = tmp_path / "missing.zip"
    _write_archive(archive, missing_minute=7)
    start = datetime(2025, 1, 1, tzinfo=timezone.utc)
    rows, stats = _aggregate_archive(archive, start, start + timedelta(minutes=30))
    assert len(rows) == 1
    assert rows[0][0] == start + timedelta(minutes=15)
    assert stats["complete_15m"] == 1
    assert stats["incomplete_15m"] == 1
    assert stats["missing_minutes"] == 1
