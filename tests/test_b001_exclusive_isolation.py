import app
import app.db


def test_b001_exclusive_active_uses_live_run_state(monkeypatch):
    monkeypatch.setenv("B001_EXCLUSIVE", "true")
    monkeypatch.setattr(app.db, "fetch_one", lambda *args, **kwargs: {"active": True})
    assert app._b001_exclusive_active() is True

    monkeypatch.setattr(app.db, "fetch_one", lambda *args, **kwargs: {"active": False})
    assert app._b001_exclusive_active() is False


def test_b001_exclusive_state_fails_closed_on_database_error(monkeypatch):
    monkeypatch.setenv("B001_EXCLUSIVE", "true")

    def broken(*args, **kwargs):
        raise RuntimeError("database unavailable")

    monkeypatch.setattr(app.db, "fetch_one", broken)
    assert app._b001_exclusive_active() is True


def test_no_exclusive_flag_never_defers(monkeypatch):
    monkeypatch.delenv("B001_EXCLUSIVE", raising=False)
    assert app._b001_exclusive_active() is False
