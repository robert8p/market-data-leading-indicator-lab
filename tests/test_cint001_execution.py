from __future__ import annotations

import csv
import io
import tempfile
import zipfile
from pathlib import Path

from app.cint001_contract import ENTRY_OFFSET_MINUTES, HOLD_MINUTES, VALIDATION_SIGNAL_END
from app.cint001_execution import (
    _futures_candidates,
    _parse_funding,
    _parse_klines,
    _parse_mark_klines,
)


def _zip_csv(rows: list[list[object]]) -> Path:
    handle = tempfile.NamedTemporaryFile(suffix=".zip", delete=False)
    handle.close()
    path = Path(handle.name)
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as archive:
        with archive.open("data.csv", "w") as raw:
            text = io.TextIOWrapper(raw, encoding="utf-8", newline="", write_through=True)
            writer = csv.writer(text)
            writer.writerows(rows)
    return path


def test_futures_candidates_include_1000_contract_without_changing_signal_symbol():
    assert _futures_candidates("BONKUSDT") == ["BONKUSDT", "1000BONKUSDT"]
    assert _futures_candidates("1000SATSUSDT") == ["1000SATSUSDT"]


def test_kline_and_mark_parsers_accept_headered_archives():
    rows = [
        ["open_time", "open", "high", "low", "close", "volume", "close_time", "quote_volume", "count", "tbv", "tbq", "ignore"],
        [1759276800000, "10", "11", "9", "10.5", "100", "1759277699999", "1000", "12", "50", "500", "0"],
    ]
    path = _zip_csv(rows)
    try:
        kline = _parse_klines(path)
        mark = _parse_mark_klines(path)
        assert len(kline) == 1 and kline[0][1:5] == (10.0, 11.0, 9.0, 10.5)
        assert len(mark) == 1 and mark[0][1:5] == (10.0, 11.0, 9.0, 10.5)
    finally:
        path.unlink(missing_ok=True)


def test_funding_parser_accepts_data_vision_style_header():
    path = _zip_csv(
        [
            ["calc_time", "funding_interval_hours", "last_funding_rate"],
            [1759276800000, "8", "0.0001"],
        ]
    )
    try:
        rows = _parse_funding(path)
        assert len(rows) == 1
        assert rows[0][1] == 0.0001
    finally:
        path.unlink(missing_ok=True)


def test_validation_signal_end_purges_holdout_crossing_trades():
    assert ENTRY_OFFSET_MINUTES == 75
    assert HOLD_MINUTES == 1440
    assert VALIDATION_SIGNAL_END.isoformat() == "2026-02-27T22:45:00+00:00"
