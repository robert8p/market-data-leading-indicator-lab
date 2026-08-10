from __future__ import annotations

import csv
import gzip
from datetime import datetime, timezone

from app.cint001_tardis_quotes import _extract, _parse_timestamp, _url


def _us(dt: datetime) -> str:
    return str(int(dt.timestamp() * 1_000_000))


def test_tardis_url_and_microsecond_timestamp() -> None:
    dt = datetime(2026, 2, 1, 12, 0, tzinfo=timezone.utc)
    assert _parse_timestamp(_us(dt)) == dt
    assert _url("ZKUSDT", dt.date()) == (
        "https://datasets.tardis.dev/v1/binance-futures/quotes/2026/02/01/ZKUSDT.csv.gz"
    )


def test_extract_uses_first_exchange_quote_at_or_after_target(tmp_path) -> None:
    base = datetime(2026, 2, 1, 12, 0, tzinfo=timezone.utc)
    targets = [base, base.replace(minute=15)]
    path = tmp_path / "ZKUSDT.csv.gz"

    rows = [
        {
            "exchange": "binance-futures",
            "symbol": "ZKUSDT",
            "timestamp": _us(base.replace(second=1)),
            "local_timestamp": _us(base.replace(second=1, microsecond=5000)),
            "ask_amount": "12",
            "ask_price": "0.0301",
            "bid_price": "0.0300",
            "bid_amount": "15",
        },
        {
            "exchange": "binance-futures",
            "symbol": "ZKUSDT",
            "timestamp": _us(base.replace(minute=15, second=2)),
            "local_timestamp": _us(base.replace(minute=15, second=2, microsecond=7000)),
            "ask_amount": "10",
            "ask_price": "0.0299",
            "bid_price": "0.0298",
            "bid_amount": "18",
        },
    ]
    fields = [
        "exchange",
        "symbol",
        "timestamp",
        "local_timestamp",
        "ask_amount",
        "ask_price",
        "bid_price",
        "bid_amount",
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
    assert snapshots[0][2] == 0.0300
    assert snapshots[0][4] == 0.0301
    assert snapshots[0][6] == 1000.0
    assert snapshots[0][7] == "at_or_after"
    assert snapshots[1][6] == 2000.0
    assert schema["provider"] == "tardis.dev"
    assert schema["quote_construction"] == "reconstructed_from_exchange_l2"
