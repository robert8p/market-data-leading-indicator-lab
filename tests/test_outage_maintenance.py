from __future__ import annotations

import pytest

import app.migrate as migrate
from app.outage_maintenance import database_outage_maintenance_enabled


@pytest.mark.parametrize("value", ["1", "true", "TRUE", "yes", "on"])
def test_outage_maintenance_truthy_values(monkeypatch, value: str) -> None:
    monkeypatch.setenv("DATABASE_OUTAGE_MAINTENANCE_MODE", value)
    assert database_outage_maintenance_enabled() is True


def test_outage_maintenance_disabled_by_default(monkeypatch) -> None:
    monkeypatch.delenv("DATABASE_OUTAGE_MAINTENANCE_MODE", raising=False)
    assert database_outage_maintenance_enabled() is False


def test_migration_skips_database_in_outage_maintenance(monkeypatch) -> None:
    monkeypatch.setenv("DATABASE_OUTAGE_MAINTENANCE_MODE", "true")

    def unexpected_settings_call():
        raise AssertionError("maintenance pre-deploy must not load database settings")

    monkeypatch.setattr(migrate, "get_settings", unexpected_settings_call)
    migrate.main()
