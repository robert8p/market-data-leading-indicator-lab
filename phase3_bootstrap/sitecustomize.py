"""Higher-priority startup hook for the Phase 3 collector service.

Render's application settings may be loaded by Pydantic from its configured
settings sources rather than exposed as raw process environment variables at
Python's earliest ``sitecustomize`` stage.  This hook temporarily suppresses
Phase 3 startup, imports the already-validated application settings, exposes
only the service-role value to the current process, and then executes the
repository's normal root startup hook.

The hook is activated only when its directory is placed first on ``PYTHONPATH``.
It does not alter signal formulas, worker SQL, market-data capture, trading, or
holdout access.
"""

from __future__ import annotations

import logging
import os
import runpy
from pathlib import Path
from typing import Any

_TRUTHY = {"1", "true", "yes", "on"}
_logger = logging.getLogger(__name__)


def _plain_secret(value: Any) -> str:
    if value is None:
        return ""
    get_secret_value = getattr(value, "get_secret_value", None)
    if callable(get_secret_value):
        value = get_secret_value()
    return str(value).strip()


def _validated_settings() -> Any:
    from app import config as config_module

    existing = getattr(config_module, "settings", None)
    if existing is not None:
        return existing

    get_settings = getattr(config_module, "get_settings", None)
    if callable(get_settings):
        return get_settings()

    settings_type = getattr(config_module, "Settings", None)
    if settings_type is not None:
        return settings_type()

    raise RuntimeError("Application settings object is unavailable")


def _preload_gateway_credential() -> bool:
    if os.getenv("SUPABASE_SERVICE_ROLE_KEY", "").strip():
        return True

    settings = _validated_settings()
    for attribute in (
        "supabase_service_role_key",
        "service_role_key",
        "supabase_service_key",
    ):
        value = _plain_secret(getattr(settings, attribute, None))
        if value:
            os.environ["SUPABASE_SERVICE_ROLE_KEY"] = value
            return True
    return False


_phase3_requested = (
    os.getenv("PHASE3_FORWARD_MONITOR_ENABLED", "").strip().lower() in _TRUTHY
)
_original_phase3_value = os.environ.get("PHASE3_FORWARD_MONITOR_ENABLED")

if _phase3_requested:
    # Prevent app/__init__.py from starting the direct-DB collector while the
    # settings module is imported.  The root hook starts exactly one patched
    # collector after the credential is available.
    os.environ["PHASE3_FORWARD_MONITOR_ENABLED"] = "false"
    try:
        if _preload_gateway_credential():
            _logger.info(
                "Loaded Phase 3 gateway authentication from validated application settings"
            )
        else:
            _logger.error(
                "Validated application settings contain no Phase 3 gateway credential"
            )
    except Exception:
        _logger.exception("Failed to preload Phase 3 gateway authentication")
    finally:
        if _original_phase3_value is None:
            os.environ["PHASE3_FORWARD_MONITOR_ENABLED"] = "true"
        else:
            os.environ["PHASE3_FORWARD_MONITOR_ENABLED"] = _original_phase3_value

_root_hook = Path(__file__).resolve().parents[1] / "sitecustomize.py"
if not _root_hook.is_file():
    raise RuntimeError(f"Root startup hook is missing: {_root_hook}")
runpy.run_path(str(_root_hook), run_name="_phase3_root_sitecustomize")
