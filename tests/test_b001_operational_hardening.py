from __future__ import annotations

from contextlib import contextmanager

from psycopg_pool import PoolTimeout

import app.b001_operational_hardening as hardening
import app.worker as worker


def test_pool_timeout_is_transient_but_data_error_is_not():
    assert hardening.is_transient_db_error(PoolTimeout("couldn't get a connection after 30.00 sec"))
    assert not hardening.is_transient_db_error(ValueError("bad historical row"))


def test_worker_reserves_database_pool_headroom():
    assert worker.B001_PARALLELISM <= worker.B001_REQUESTED_PARALLELISM
    assert worker.B001_PARALLELISM <= max(1, worker.settings.db_pool_size - 1)


def test_db_guard_keeps_worker_alive_on_pool_timeout():
    def raises_timeout():
        raise PoolTimeout("couldn't get a connection after 30.00 sec")

    assert worker._db_call("test", raises_timeout, "retry") == "retry"


def test_transient_checkpoint_reverses_claim_attempt(monkeypatch):
    executed: list[tuple[str, tuple | None]] = []

    class FakeCursor:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, sql, params=None):
            executed.append((sql, params))

    class FakeConnection:
        def cursor(self):
            return FakeCursor()

        def commit(self):
            return None

    @contextmanager
    def fake_connection():
        yield FakeConnection()

    monkeypatch.setattr(hardening, "db_connection", fake_connection)
    item = {
        "id": 1423,
        "attempts": 8,
        "stage": "spot_month",
        "partition_key": "ARBUSDT:2024-12",
    }
    hardening._record_transient_db_retry(
        item,
        PoolTimeout("couldn't get a connection after 30.00 sec"),
    )

    assert len(executed) == 1
    sql, params = executed[0]
    normalized = " ".join(sql.split())
    assert "status='retry_wait'" in normalized
    assert "attempts=greatest(attempts-1,0)" in normalized
    assert "error_code='db_transient'" in normalized
    assert params is not None
    assert params[-1] == 1423


def test_transient_failure_does_not_call_normal_terminal_handler(monkeypatch):
    recorded: list[tuple[dict, BaseException]] = []
    terminal_calls: list[tuple] = []

    monkeypatch.setattr(
        hardening,
        "_record_transient_db_retry",
        lambda item, exc: recorded.append((item, exc)),
    )
    monkeypatch.setattr(
        hardening,
        "_ORIGINAL_FAIL",
        lambda *args, **kwargs: terminal_calls.append((args, kwargs)),
    )

    item = {"id": 1, "attempts": 8, "stage": "spot_month", "partition_key": "TEST:2025-01"}
    exc = PoolTimeout("couldn't get a connection after 30.00 sec")
    hardening._fail(item, exc)

    assert recorded == [(item, exc)]
    assert terminal_calls == []
