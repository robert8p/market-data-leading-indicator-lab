from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone

import app.b001_threshold_scan_narrow as narrow


def test_threshold_scan_carries_only_required_feature_columns(monkeypatch):
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    monkeypatch.setattr(
        narrow.robustness,
        "fetch_one",
        lambda *args, **kwargs: {
            "requested_start": start,
            "requested_end": datetime(2026, 1, 20, tzinfo=timezone.utc),
        },
    )
    seen = []

    def fake_fetch(sql, params):
        seen.append(sql)
        return []

    monkeypatch.setattr(narrow, "_fetch_rank_rows", fake_fetch)
    narrow._threshold_candidate_sets_narrow(
        "00000000-0000-0000-0000-000000000001",
        [("threshold_dispersion", "x1.1", {"dispersion_max": 1.0})],
    )
    sql = seen[0].lower()
    assert "select f.*" not in sql
    assert "f.final_5m_return" in sql
    assert "f.high_to_close_rejection" in sql
    assert "f.close_vs_vwap" in sql
    assert sql.count("percent_rank()") == 3


def test_rank_fetch_sets_work_mem_locally(monkeypatch):
    executed = []

    class Cursor:
        def __enter__(self): return self
        def __exit__(self, *args): return False
        def execute(self, sql, params=None): executed.append((sql, params))
        def fetchall(self): return []

    class Conn:
        def cursor(self): return Cursor()
        def commit(self): executed.append(("COMMIT", None))

    @contextmanager
    def fake_connection():
        yield Conn()

    monkeypatch.setattr(narrow.robustness, "db_connection", fake_connection)
    narrow._fetch_rank_rows("select 1 where 1=%s", (1,))
    assert executed[0] == ("set local work_mem=%s", (narrow._THRESHOLD_WORK_MEM,))
    assert executed[1] == ("select 1 where 1=%s", (1,))
    assert executed[-1][0] == "COMMIT"
