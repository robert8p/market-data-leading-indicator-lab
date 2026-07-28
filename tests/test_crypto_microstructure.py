from app.crypto_stream import BookState


def test_book_metrics_calculate_depth_imbalance_and_microprice():
    state = BookState(depth=2)
    state.snapshot([["99", "5"], ["98", "3"]], [["101", "2"], ["102", "4"]])
    metrics = state.metrics()
    assert metrics["bid_price"] == 99
    assert metrics["ask_price"] == 101
    assert metrics["bid_depth"] == 8
    assert metrics["ask_depth"] == 6
    assert metrics["depth_imbalance"] > 0
    assert 99 < metrics["microprice"] < 101


def test_book_updates_remove_zero_quantity_level():
    state = BookState(depth=2)
    state.snapshot([["99", "5"]], [["101", "2"]])
    state.update("buy", "99", "0")
    assert state.metrics() == {}


def test_failed_raw_upload_is_retained_for_retry(monkeypatch, tmp_path):
    import asyncio
    from datetime import datetime, timedelta, timezone
    from app.crypto_stream import RawSegmentWriter

    writer = RawSegmentWriter()
    writer.root = tmp_path
    ts = datetime.now(timezone.utc) - timedelta(hours=1)
    writer.write("test", "spot", "BTC-USD", "BTC", "trades", ts, {"x": 1})

    attempts = {"count": 0}

    def flaky_upload(_segment):
        attempts["count"] += 1
        if attempts["count"] == 1:
            raise RuntimeError("temporary upload failure")

    monkeypatch.setattr(writer, "_upload", flaky_upload)
    uploaded = asyncio.run(writer.close_expired(datetime.now(timezone.utc)))
    assert uploaded == 0
    assert len(writer.pending_uploads) == 1
    pending_path = next(iter(writer.pending_uploads))
    assert pending_path.exists()

    uploaded = asyncio.run(writer.close_expired(datetime.now(timezone.utc)))
    assert uploaded == 1
    assert not writer.pending_uploads
    assert not pending_path.exists()


def test_failed_database_flush_rebuffers_rows(monkeypatch):
    import asyncio
    from datetime import datetime, timedelta, timezone
    from app.crypto_stream import Bucket, CryptoCollector

    collector = CryptoCollector()
    ts = datetime.now(timezone.utc) - timedelta(minutes=1)
    row = Bucket("test", "spot", "BTC-USD", "BTC", ts, trade_count=1, buy_count=1)
    collector.buckets[(row.provider, row.market_type, row.venue_symbol, row.ts)] = row

    def fail_flush(_rows):
        raise RuntimeError("temporary database failure")

    monkeypatch.setattr(collector, "_flush_rows", fail_flush)
    try:
        asyncio.run(collector.flush())
    except RuntimeError:
        pass
    else:
        raise AssertionError("Expected the simulated database failure")

    restored = collector.buckets[(row.provider, row.market_type, row.venue_symbol, row.ts)]
    assert restored.trade_count == 1
    assert restored.buy_count == 1
