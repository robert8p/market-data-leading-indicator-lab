from app.crypto_stream_bookfix import (
    _crossed_book_guard,
    apply_partial_depth_snapshot,
)


class FakeBookState:
    def __init__(self):
        self.snapshots = []

    def snapshot(self, bids, asks):
        self.snapshots.append((bids, asks))


def test_binance_partial_depth_replaces_book_from_short_keys():
    state = FakeBookState()
    payload = {
        "e": "depthUpdate",
        "b": [["100", "2"], ["99", "3"]],
        "a": [["101", "4"], ["102", "5"]],
    }

    apply_partial_depth_snapshot(state, payload)

    assert state.snapshots == [
        (
            [["100", "2"], ["99", "3"]],
            [["101", "4"], ["102", "5"]],
        )
    ]


def test_binance_partial_depth_accepts_long_keys():
    state = FakeBookState()
    payload = {"bids": [["100", "2"]], "asks": [["101", "4"]]}

    apply_partial_depth_snapshot(state, payload)

    assert state.snapshots == [([["100", "2"]], [["101", "4"]])]


def test_crossed_book_guard_rejects_negative_spread():
    class State:
        def metrics(self):
            return {"bid_price": 101.0, "ask_price": 100.0, "spread_bps": -99.5}

    guarded = _crossed_book_guard(State.metrics)
    assert guarded(State()) == {}


def test_crossed_book_guard_preserves_valid_and_locked_books():
    class State:
        def __init__(self, bid, ask):
            self.bid = bid
            self.ask = ask

        def metrics(self):
            return {"bid_price": self.bid, "ask_price": self.ask}

    guarded = _crossed_book_guard(State.metrics)
    assert guarded(State(100.0, 101.0)) == {"bid_price": 100.0, "ask_price": 101.0}
    assert guarded(State(100.0, 100.0)) == {"bid_price": 100.0, "ask_price": 100.0}
