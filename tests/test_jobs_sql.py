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


def test_equity_aggregation_uses_quote_based_trade_classification():
    from pathlib import Path

    source = (Path(__file__).parents[1] / "app" / "aggregation.py").read_text(encoding="utf-8")
    assert "left join lateral" in source
    assert "t.price >= q.ask_price then 'buy'" in source
    assert "t.price <= q.bid_price then 'sell'" in source
    assert "quote_test_5s_v1" in source


def test_workers_wait_for_latest_miner_schema():
    from pathlib import Path

    root = Path(__file__).parents[1] / "app"
    worker = (root / "worker.py").read_text(encoding="utf-8")
    stream = (root / "crypto_stream.py").read_text(encoding="utf-8")
    assert "public.capture_decisions" in worker
    assert "public.crypto_market_observations_1m" in stream
    assert "await asyncio.to_thread(wait_for_stream_schema)" in stream


def test_bar_partition_planning_persists_source_feed_in_both_insert_paths():
    from pathlib import Path

    source = (Path(__file__).parents[1] / "app" / "jobs.py").read_text(encoding="utf-8")
    assert source.count("start_ts, end_ts, status, priority, max_attempts, cursor") >= 2
    assert source.count("'bars_1m',%s,%s,'queued',%s,%s,%s") >= 2
