from datetime import datetime, timezone

import app.b001_placebo_acceleration as accel


def test_timestamp_placebos_use_six_set_queries(monkeypatch):
    queries = []
    persisted = []

    def fake_fetch_all(sql, params):
        queries.append((sql, params))
        shift = params[0]
        return [
            {
                "symbol": "TESTUSDT",
                "entry_ts": datetime(2025, 1, 1, 0, 15, tzinfo=timezone.utc),
                "net_return": 0.01 + shift * 0.0,
            }
        ]

    monkeypatch.setattr(accel, "fetch_all", fake_fetch_all)
    monkeypatch.setattr(
        accel.analysis,
        "_persist_placebo",
        lambda run_id, placebo_type, variant, rows, details: persisted.append(
            (placebo_type, variant, list(rows), details)
        ),
    )

    accel.timestamp_placebos_set_based("run", [])

    assert [q[1][0] for q in queries] == [-60, -30, -15, 15, 30, 60]
    assert len(persisted) == 6
    assert [p[1] for p in persisted] == [
        "shift_-60m",
        "shift_-30m",
        "shift_-15m",
        "shift_+15m",
        "shift_+30m",
        "shift_+60m",
    ]
    assert all(p[3]["execution"] == "set_based_exact_equivalence" for p in persisted)
    assert all(len(p[2]) == 1 for p in persisted)


def test_symbol_placebo_preserves_seed_and_chunks_calendar_months(monkeypatch):
    monkeypatch.setattr(
        accel,
        "fetch_one",
        lambda *args, **kwargs: {
            "requested_start": datetime(2025, 1, 15, tzinfo=timezone.utc),
            "requested_end": datetime(2025, 3, 2, tzinfo=timezone.utc),
        },
    )
    queries = []
    persisted = []

    def fake_fetch_all(sql, params):
        queries.append((sql, params))
        return [
            {
                "symbol": "CTRLUSDT",
                "entry_ts": params[1] + __import__("datetime").timedelta(minutes=15),
                "net_return": 0.02,
            }
        ]

    monkeypatch.setattr(accel, "fetch_all", fake_fetch_all)
    monkeypatch.setattr(
        accel.analysis,
        "_persist_placebo",
        lambda run_id, placebo_type, variant, rows, details: persisted.append(
            (placebo_type, variant, list(rows), details)
        ),
    )

    accel.symbol_placebo_set_based("run", [])

    assert len(queries) == 3
    assert queries[0][1][1] == datetime(2025, 1, 15, tzinfo=timezone.utc)
    assert queries[0][1][2] == datetime(2025, 2, 1, tzinfo=timezone.utc)
    assert queries[-1][1][2] == datetime(2025, 3, 2, tzinfo=timezone.utc)
    assert all(q[1][3] == "B001_SYMBOL_PLACEBO_V1:" for q in queries)
    assert all(q[1][4] == "B001_SYMBOL_PLACEBO_V1:" for q in queries)
    assert len(persisted) == 1
    assert persisted[0][0] == "symbol"
    assert persisted[0][1] == "five_deterministic_non_signal_controls"
    assert persisted[0][3]["controls_per_signal"] == 5
    assert persisted[0][3]["seed"] == "B001_SYMBOL_PLACEBO_V1"
    assert persisted[0][3]["execution"] == "calendar_month_set_based_exact_equivalence"
    assert len(persisted[0][2]) == 3
