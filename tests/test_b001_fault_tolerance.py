from __future__ import annotations

from contextlib import contextmanager

import httpx

import app.b001_operational_hardening as hardening


def test_failure_classifier_distinguishes_retryable_and_permanent_errors():
    retryable, code = hardening.classify_failure(httpx.ReadTimeout("temporary timeout"))
    assert retryable is True
    assert code == "network_transient"

    retryable, code = hardening.classify_failure(TypeError("bad function signature"))
    assert retryable is False
    assert code == "implementation_error"

    retryable, code = hardening.classify_failure(
        hardening.ArchiveVerificationError("canonical rows missing")
    )
    assert retryable is False
    assert code == "archive_verification_failed"


def test_retry_backoff_increases_and_is_bounded():
    first = hardening._retry_delay_seconds(1, 100)
    second = hardening._retry_delay_seconds(2, 100)
    sixth = hardening._retry_delay_seconds(6, 100)
    huge = hardening._retry_delay_seconds(100, 100)
    assert first < second < sixth
    assert huge <= 900


def test_archive_completion_verifies_before_marking_complete(monkeypatch):
    calls: list[tuple] = []

    monkeypatch.setattr(
        hardening,
        "_verify_archive_before_complete",
        lambda item_id: {"verified": True, "canonical_rows_present": 100},
    )
    monkeypatch.setattr(
        hardening,
        "_ORIGINAL_COMPLETE",
        lambda item_id, row_count, progress, status: calls.append(
            (item_id, row_count, progress, status)
        ),
    )

    hardening._complete(7, 100, {"source_rows": 100}, "completed")
    assert len(calls) == 1
    item_id, row_count, progress, status = calls[0]
    assert item_id == 7
    assert row_count == 100
    assert status == "completed"
    assert progress["insert_verification"]["verified"] is True


def test_permanent_archive_failure_is_recorded_without_terminating_run(monkeypatch):
    statements: list[str] = []

    class FakeCursor:
        rowcount = 1

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, sql, params=None):
            statements.append(" ".join(sql.split()))

    class FakeConnection:
        def cursor(self):
            return FakeCursor()

        def commit(self):
            return None

    @contextmanager
    def fake_connection():
        yield FakeConnection()

    monkeypatch.setattr(hardening, "db_connection", fake_connection)
    monkeypatch.setattr(hardening.replication, "advance_b001_run", lambda run_id: None)

    item = {
        "id": 11,
        "run_id": "7d9ba848-87ef-42a7-a25a-6971d44aee9d",
        "stage": "spot_month",
        "partition_key": "TESTUSDT:2025-01",
        "payload": {"symbol": "TESTUSDT", "period_start": "2025-01-01"},
    }
    hardening._record_permanent_failure(item, ValueError("malformed archive"), "data_validation_error")

    joined = "\n".join(statements)
    assert "set status='failed'" in joined
    assert "set source_status='failed'" in joined
    assert "set status='running'" in joined
    assert "set status='completed_with_errors'" not in joined


def test_transient_db_retry_tracks_infra_retry_without_consuming_attempt(monkeypatch):
    executed: list[tuple[str, tuple | None]] = []

    class FakeCursor:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, sql, params=None):
            executed.append((" ".join(sql.split()), params))

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
        "id": 12,
        "attempts": 8,
        "stage": "spot_month",
        "partition_key": "TESTUSDT:2025-02",
        "progress": {"infra_retries": 2},
    }
    hardening._record_transient_db_retry(item, hardening.PoolTimeout("pool busy"))

    sql, params = executed[0]
    assert "attempts=greatest(attempts-1,0)" in sql
    assert "error_code='db_transient'" in sql
    assert params is not None
    progress_patch = params[1].obj
    assert progress_patch["infra_retries"] == 3
