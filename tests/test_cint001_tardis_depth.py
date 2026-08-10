from __future__ import annotations

import csv
import gzip
from datetime import datetime, timezone

from app.cint001_tardis_depth import _extract, _levels, _parse_timestamp, _url


def _us(dt: datetime) -> str:
    return str(int(dt.timestamp() * 1_000_000))


def test_tardis_depth_url_and_timestamp() -> None:
    dt = datetime(2026, 2, 1, 12, 0, tzinfo=timezone.utc)
    assert _parse_timestamp(_us(dt)) == dt
    assert _url("ZKUSDT", dt.date()) == (
        "https://datasets.tardis.dev/v1/binance-futures/book_snapshot_25/"
        "2026/02/01/ZKUSDT.csv.gz"
    )


def test_levels_parse_top_25_shape() -> None:
    row = {
        "bids[0].price": "100",
        "bids[0].amount": "2",
        "bids[1].price": "99",
        "bids[1].amount": "3",
        "asks[0].price": "101",
        "asks[0].amount": "4",
    }
    assert _levels(row, "bid") == [
        {"price": 100.0, "amount": 2.0},
        {"price": 99.0, "amount": 3.0},
    ]
    assert _levels(row, "ask") == [{"price": 101.0, "amount": 4.0}]


def test_extract_uses_first_snapshot_at_or_after_target(tmp_path) -> None:
    base = datetime(2026, 2, 1, 12, 0, tzinfo=timezone.utc)
    targets = [base, base.replace(minute=15)]
    path = tmp_path / "ZKUSDT.csv.gz"

    fields = ["exchange", "symbol", "timestamp", "local_timestamp"]
    for i in range(25):
        fields += [
            f"asks[{i}].price",
            f"asks[{i}].amount",
            f"bids[{i}].price",
            f"bids[{i}].amount",
        ]

    def make_row(ts: datetime, bid: float, ask: float) -> dict[str, str]:
        row = {name: "" for name in fields}
        row.update(
            {
                "exchange": "binance-futures",
                "symbol": "ZKUSDT",
                "timestamp": _us(ts),
                "local_timestamp": _us(ts),
                "asks[0].price": str(ask),
                "asks[0].amount": "10",
                "bids[0].price": str(bid),
                "bids[0].amount": "12",
                "asks[1].price": str(ask + 0.1),
                "asks[1].amount": "20",
                "bids[1].price": str(bid - 0.1),
                "bids[1].amount": "25",
            }
        )
        return row

    rows = [
        make_row(base.replace(second=1), 99.0, 100.0),
        make_row(base.replace(minute=15, second=2), 98.0, 99.0),
    ]
    with gzip.open(path, "wt", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)

    snapshots, schema, scanned = _extract(path, targets)
    assert scanned == 2
    assert len(snapshots) == 2
    assert snapshots[0][0] == targets[0]
    assert snapshots[0][1] == base.replace(second=1)
    assert snapshots[0][3] == 1000.0
    assert snapshots[0][4][0] == {"price": 99.0, "amount": 12.0}
    assert snapshots[0][5][0] == {"price": 100.0, "amount": 10.0}
    assert len(snapshots[0][4]) == 2
    assert schema["dataset"] == "book_snapshot_25"
