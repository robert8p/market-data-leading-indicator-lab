from __future__ import annotations

import csv
import io
import zipfile
from datetime import datetime, timezone

from app.cint001_bookticker import _extract, _parse_timestamp, _row_parser


def _us(dt: datetime) -> str:
    return str(int(dt.timestamp() * 1_000_000))


def test_parse_timestamp_supports_milliseconds_microseconds_and_nanoseconds() -> None:
    expected = datetime(2026, 2, 1, 12, 0, tzinfo=timezone.utc)
    assert _parse_timestamp(str(int(expected.timestamp() * 1_000))) == expected
    assert _parse_timestamp(str(int(expected.timestamp() * 1_000_000))) == expected
    assert _parse_timestamp(str(int(expected.timestamp() * 1_000_000_000))) == expected


def test_row_parser_accepts_header_and_compact_seven_column_layout() -> None:
    header = [
        "update_id",
        "best_bid_price",
        "best_bid_qty",
        "best_ask_price",
        "best_ask_qty",
        "transaction_time",
        "event_time",
    ]
    indices, has_header, schema = _row_parser(header)
    assert has_header is True
    assert indices["bid_price"] == 1
    assert indices["ask_price"] == 3
    assert indices["time"] == 5
    assert schema["mode"] == "header"

    row = ["1", "100", "2", "101", "3", "1770000000000000", "1770000000000000"]
    indices, has_header, schema = _row_parser(row)
    assert has_header is False
    assert indices == {
        "bid_price": 1,
        "bid_qty": 2,
        "ask_price": 3,
        "ask_qty": 4,
        "time": 5,
    }
    assert schema["mode"] == "validated_inferred_7_column"


def test_extract_uses_first_quote_at_or_after_each_target(tmp_path) -> None:
    base = datetime(2026, 2, 1, 12, 0, tzinfo=timezone.utc)
    targets = [base, base.replace(minute=15)]
    rows = [
        [
            "update_id",
            "best_bid_price",
            "best_bid_qty",
            "best_ask_price",
            "best_ask_qty",
            "transaction_time",
            "event_time",
        ],
        ["1", "99.0", "10", "100.0", "11", _us(base.replace(second=1)), _us(base.replace(second=1))],
        ["2", "101.0", "12", "102.0", "13", _us(base.replace(minute=15, second=2)), _us(base.replace(minute=15, second=2))],
    ]

    archive_path = tmp_path / "sample.zip"
    buffer = io.StringIO()
    writer = csv.writer(buffer)
    writer.writerows(rows)
    with zipfile.ZipFile(archive_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("sample.csv", buffer.getvalue())

    snapshots, schema, scanned = _extract(archive_path, targets, base.date())
    assert scanned == 2
    assert len(snapshots) == 2

    first = snapshots[0]
    assert first[0] == targets[0]
    assert first[1] == base.replace(second=1)
    assert first[2] == 99.0
    assert first[4] == 100.0
    assert first[6] == 1000.0
    assert first[7] == "at_or_after"

    second = snapshots[1]
    assert second[0] == targets[1]
    assert second[1] == base.replace(minute=15, second=2)
    assert second[6] == 2000.0
    assert schema["timing_policy"] == "first_valid_quote_at_or_after_target"
