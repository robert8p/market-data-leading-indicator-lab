import app.b001_mainloop_isolation as isolation


def test_yield_wrapper_skips_original_while_b001_active(monkeypatch):
    called = []
    monkeypatch.setattr(isolation, "b001_exclusive_active", lambda: True)
    wrapped = isolation._wrap_yield(
        lambda *args, **kwargs: called.append((args, kwargs)) or "worked",
        None,
        "test claim",
    )

    assert wrapped("x") is None
    assert called == []


def test_yield_wrapper_restores_original_after_b001_terminal(monkeypatch):
    called = []
    monkeypatch.setattr(isolation, "b001_exclusive_active", lambda: False)
    wrapped = isolation._wrap_yield(
        lambda value: called.append(value) or "worked",
        None,
        "test claim",
    )

    assert wrapped("x") == "worked"
    assert called == ["x"]


def test_exclusive_state_fails_closed_on_db_error(monkeypatch):
    monkeypatch.setenv("B001_EXCLUSIVE", "true")
    monkeypatch.setattr(isolation, "_cache_checked_at", 0.0)

    def broken(*args, **kwargs):
        raise RuntimeError("database unavailable")

    monkeypatch.setattr(isolation, "fetch_one", broken)
    assert isolation.b001_exclusive_active() is True


def test_exclusive_state_uses_live_run_flag(monkeypatch):
    monkeypatch.setenv("B001_EXCLUSIVE", "true")
    monkeypatch.setattr(isolation, "_cache_checked_at", 0.0)
    monkeypatch.setattr(isolation, "fetch_one", lambda *args, **kwargs: {"active": True})
    assert isolation.b001_exclusive_active() is True

    monkeypatch.setattr(isolation, "_cache_checked_at", 0.0)
    monkeypatch.setattr(isolation, "fetch_one", lambda *args, **kwargs: {"active": False})
    assert isolation.b001_exclusive_active() is False
