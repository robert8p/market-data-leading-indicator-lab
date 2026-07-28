from datetime import datetime, timezone

from fastapi.testclient import TestClient

import app.main as main
from app.main import app


def test_login_page_renders() -> None:
    with TestClient(app, base_url="https://testserver") as client:
        response = client.get("/login")
    assert response.status_code == 200
    assert "Market Data Miner" in response.text
    assert "Welcome back" in response.text


def test_invalid_login_renders_error() -> None:
    with TestClient(app, base_url="https://testserver") as client:
        response = client.post(
            "/login",
            data={"username": "incorrect", "password": "incorrect"},
        )
    assert response.status_code == 401
    assert "Incorrect username or password" in response.text


def test_dashboard_and_run_detail_render_progress(monkeypatch) -> None:
    now = datetime(2026, 7, 28, 14, 30, tzinfo=timezone.utc)
    run = {
        "id": "11111111-1111-1111-1111-111111111111",
        "name": "30-day test run",
        "status": "running",
        "stage": "enrichment",
        "start_ts": now,
        "end_ts": now,
        "created_at": now,
        "completed_partitions": 75,
        "planned_partitions": 100,
        "failed_partitions": 2,
        "skipped_partitions": 1,
        "rows_written": 123456,
        "enhancement_requested": True,
    }

    def fake_all(sql: str, params=None):
        if "from collection_runs" in sql and "limit 25" in sql:
            return [run]
        if "from instruments group by provider" in sql:
            return [{"provider": "alpaca", "instruments": 100, "preferred_instruments": 50}]
        if "from provider_health" in sql:
            return [{"provider": "alpaca", "service": "historical", "status": "healthy", "last_message_at": now}]
        if "from collection_partitions" in sql and "group by" in sql:
            return [{"provider": "alpaca", "data_type": "bars_1m", "status": "completed", "partitions": 10, "rows": 1000}]
        if "from collection_partitions" in sql and "status in ('failed','retry_wait')" in sql:
            return []
        if "from capture_windows" in sql and "group by" in sql:
            return []
        return []

    def fake_one(sql: str, params=None):
        if "pg_database_size" in sql:
            return {"bytes": 123456789}
        if "from crypto_stream_sessions" in sql:
            return {
                "status": "running",
                "last_heartbeat_at": now,
                "started_at": now,
                "message_count": 5000,
                "flush_count": 120,
            }
        if "select count(*) from capture_windows" in sql:
            return {
                "capture_windows": 10,
                "market_trades": 20,
                "market_quotes": 30,
                "crypto_seconds": 40,
                "derivative_rows": 50,
                "supply_rows": 60,
                "raw_objects": 70,
            }
        if "from collection_runs where id" in sql:
            return run
        return None

    monkeypatch.setattr(main, "fetch_all", fake_all)
    monkeypatch.setattr(main, "fetch_one", fake_one)

    with TestClient(app, base_url="https://testserver") as client:
        client.post("/login", data={"username": "rob", "password": "test-password"})
        dashboard = client.get("/")
        detail = client.get("/runs/11111111-1111-1111-1111-111111111111")

    assert dashboard.status_code == 200
    assert 'aria-valuenow="75"' in dashboard.text
    assert "Collection control centre" in dashboard.text
    assert detail.status_code == 200
    assert "Overall progress" in detail.text
    assert 'data-auto-refresh-seconds="20"' in detail.text
    assert "75%" in detail.text
