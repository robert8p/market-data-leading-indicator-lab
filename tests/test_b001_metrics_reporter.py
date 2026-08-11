from __future__ import annotations

import app.b001_metrics_reporter as reporter


def test_metrics_reporter_derives_throughput_and_rates(monkeypatch):
    monkeypatch.setattr(
        reporter,
        "fetch_one",
        lambda *args, **kwargs: {
            "completed": 90,
            "failed": 10,
            "queued": 2,
            "retry_wait": 3,
            "running": 1,
            "retried_items": 20,
            "consumed_retries": 25,
            "infra_retries": 4,
            "completed_15m": 8,
            "archives_15m": 3,
            "permanent_archive_failures": 2,
        },
    )
    metrics = reporter.collect_metrics("run")
    assert metrics["items_per_hour_recent"] == 32
    assert metrics["archives_per_hour_recent"] == 12
    assert metrics["permanent_failure_rate_pct"] == 10.0
    assert metrics["retry_item_rate_pct"] == 20.0
