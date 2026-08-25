from __future__ import annotations

import importlib.util
import os
import runpy
import sys
import types
from pathlib import Path

import pytest


_BOOTSTRAP = Path(__file__).resolve().parents[1] / "phase3_bootstrap" / "sitecustomize.py"


class _Secret:
    def __init__(self, value: str) -> None:
        self._value = value

    def get_secret_value(self) -> str:
        return self._value


def _execute_bootstrap(
    monkeypatch: pytest.MonkeyPatch,
    *,
    phase3_enabled: bool,
    settings: object | None,
) -> list[tuple[str, str]]:
    calls: list[tuple[str, str]] = []
    fake_app = types.ModuleType("app")
    fake_config = types.ModuleType("app.config")
    if settings is not None:
        fake_config.settings = settings
    fake_app.config = fake_config
    monkeypatch.setitem(sys.modules, "app", fake_app)
    monkeypatch.setitem(sys.modules, "app.config", fake_config)
    monkeypatch.setenv(
        "PHASE3_FORWARD_MONITOR_ENABLED",
        "true" if phase3_enabled else "false",
    )
    monkeypatch.delenv("SUPABASE_SERVICE_ROLE_KEY", raising=False)
    monkeypatch.setattr(
        runpy,
        "run_path",
        lambda path, run_name: calls.append((str(path), str(run_name))),
    )

    module_name = f"phase3_bootstrap_test_{id(settings)}_{phase3_enabled}"
    spec = importlib.util.spec_from_file_location(module_name, _BOOTSTRAP)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return calls


def test_bootstrap_loads_service_role_from_validated_settings(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = types.SimpleNamespace(
        supabase_service_role_key=_Secret("validated-service-role-secret")
    )

    calls = _execute_bootstrap(
        monkeypatch,
        phase3_enabled=True,
        settings=settings,
    )

    assert os.environ["SUPABASE_SERVICE_ROLE_KEY"] == "validated-service-role-secret"
    assert os.environ["PHASE3_FORWARD_MONITOR_ENABLED"] == "true"
    assert calls and calls[0][1] == "_phase3_root_sitecustomize"


def test_bootstrap_is_inert_when_phase3_is_disabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = _execute_bootstrap(
        monkeypatch,
        phase3_enabled=False,
        settings=types.SimpleNamespace(
            supabase_service_role_key=_Secret("must-not-be-loaded")
        ),
    )

    assert "SUPABASE_SERVICE_ROLE_KEY" not in os.environ
    assert calls and calls[0][1] == "_phase3_root_sitecustomize"
