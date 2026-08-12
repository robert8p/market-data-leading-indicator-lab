from __future__ import annotations

from pathlib import Path

from app.research_worker import should_finalize


def test_should_finalize_only_when_all_tasks_completed():
    assert should_finalize({"completed": 28, "queued": 0, "running": 0, "failed": 0}) is True
    assert should_finalize({"completed": 27, "queued": 1, "running": 0, "failed": 0}) is False
    assert should_finalize({"completed": 27, "queued": 0, "running": 1, "failed": 0}) is False
    assert should_finalize({"completed": 27, "queued": 0, "running": 0, "failed": 1}) is False
    assert should_finalize({"completed": 0, "queued": 0, "running": 0, "failed": 0}) is False


def test_research_worker_uses_dispatch_gated_claim_and_no_provider_validation():
    source = (Path(__file__).resolve().parents[1] / "app" / "research_worker.py").read_text(encoding="utf-8")
    assert "claim_dispatchable_experiment_task_v1" in source
    assert "claim_experiment_task(" not in source
    assert "validate_worker" not in source
    assert "RESEARCH_WORKER_ENABLED" in source
    assert "RESEARCH_TASK_TIMEOUT_MINUTES" in source
    assert "reclaim_stale_experiment_tasks" in source


def test_research_worker_does_not_retry_uncertain_results_in_client_code():
    source = (Path(__file__).resolve().parents[1] / "app" / "research_worker.py").read_text(encoding="utf-8")
    assert "stale reclaim" in source
    assert "Do not mutate the research result after an uncertain client-side" in source
