from __future__ import annotations

from pathlib import Path

import pytest

from app.research_worker import should_finalize, task_runner_function


def test_should_finalize_only_when_all_tasks_completed():
    assert should_finalize({"completed": 34, "queued": 0, "running": 0, "failed": 0}) is True
    assert should_finalize({"completed": 33, "queued": 1, "running": 0, "failed": 0}) is False
    assert should_finalize({"completed": 33, "queued": 0, "running": 1, "failed": 0}) is False
    assert should_finalize({"completed": 33, "queued": 0, "running": 0, "failed": 1}) is False
    assert should_finalize({"completed": 0, "queued": 0, "running": 0, "failed": 0}) is False


def test_task_runner_function_supports_complete_frozen_family():
    assert task_runner_function("feature_screen") == "research_hub.run_feature_screen_task"
    assert task_runner_function("event_screen") == "research_hub.run_event_screen_task"
    assert task_runner_function("crypto_spot_futures_feature_screen") == "research_hub.run_crypto_spot_futures_feature_screen_task_v1"
    with pytest.raises(ValueError, match="Unsupported research task type"):
        task_runner_function("unknown")


def test_research_worker_uses_dispatch_gated_claim_and_no_provider_validation():
    source = (Path(__file__).resolve().parents[1] / "app" / "research_worker.py").read_text(encoding="utf-8")
    assert "claim_dispatchable_experiment_task_v1" in source
    assert "claim_experiment_task(" not in source
    assert "validate_worker" not in source
    assert "RESEARCH_WORKER_ENABLED" in source
    assert "RESEARCH_TASK_TIMEOUT_MINUTES" in source
    assert "reclaim_stale_experiment_tasks" in source
    assert "run_feature_screen_task" in source
    assert "run_event_screen_task" in source
    assert "run_crypto_spot_futures_feature_screen_task_v1" in source


def test_research_worker_does_not_retry_uncertain_results_in_client_code():
    source = (Path(__file__).resolve().parents[1] / "app" / "research_worker.py").read_text(encoding="utf-8")
    assert "stale reclaim" in source
    assert "Do not mutate the research result after an uncertain client-side" in source
