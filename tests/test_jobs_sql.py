from uuid import uuid4

import app.jobs as jobs


def test_find_runs_ready_for_planning_uses_valid_postgres_ordering(monkeypatch):
    run_id = uuid4()
    captured: dict[str, str] = {}

    def fake_fetch_all(sql: str, params=None):
        captured["sql"] = sql
        return [{"id": run_id}]

    monkeypatch.setattr(jobs, "fetch_all", fake_fetch_all)

    assert jobs.find_runs_ready_for_planning() == [run_id]
    normalised = " ".join(captured["sql"].lower().split())
    assert "select cr.id from collection_runs cr" in normalised
    assert "select distinct" not in normalised
    assert "order by cr.created_at" in normalised
