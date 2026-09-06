from __future__ import annotations

import os


ENVIRONMENT_VARIABLE = "DATABASE_OUTAGE_MAINTENANCE_MODE"
_TRUTHY = {"1", "true", "yes", "on"}


def database_outage_maintenance_enabled() -> bool:
    """Return whether database-free outage maintenance mode is enabled."""
    return os.getenv(ENVIRONMENT_VARIABLE, "").strip().lower() in _TRUTHY
