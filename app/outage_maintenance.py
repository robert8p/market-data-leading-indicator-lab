from __future__ import annotations

import os


ENVIRONMENT_VARIABLE = "DATABASE_OUTAGE_MAINTENANCE_MODE"
SKIP_MIGRATIONS_ENVIRONMENT_VARIABLE = "SKIP_DATABASE_MIGRATIONS"
_TRUTHY = {"1", "true", "yes", "on"}


def database_outage_maintenance_enabled() -> bool:
    """Return whether database-free outage maintenance mode is enabled."""
    return os.getenv(ENVIRONMENT_VARIABLE, "").strip().lower() in _TRUTHY


def database_migrations_disabled() -> bool:
    """Return whether pre-deploy migrations must avoid database access.

    Least-privileged runtime services can opt out independently of outage mode;
    schema migrations are then applied through the privileged deployment path.
    """
    return (
        database_outage_maintenance_enabled()
        or os.getenv(SKIP_MIGRATIONS_ENVIRONMENT_VARIABLE, "").strip().lower() in _TRUTHY
    )
