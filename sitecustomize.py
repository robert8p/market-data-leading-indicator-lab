"""Process-level opt-in hooks for isolated research backfills.

Python imports ``sitecustomize`` automatically when it is present on sys.path.
The hooks are inert unless a service-specific environment flag is explicitly
set. Phase 3 uses this early hook to install its restricted HTTP transport
before the :mod:`app` package can start the collector with the normal database
transport.
"""

from __future__ import annotations

import logging
import os
from urllib.parse import unquote, urlsplit

_TRUTHY = {"1", "true", "yes", "on"}
_logger = logging.getLogger(__name__)

# Index-futures historical ingestion is an explicit one-shot, resumable lane.
# The application bootstrap and database RPCs independently enforce the fixed
# [2025-09-01, 2026-09-01) evidence window.
if os.getenv("INDEX_FUTURES_INGEST_ENABLED", "").strip().lower() in _TRUTHY:
    try:
        from app.index_futures_fixed_window_bootstrap import (
            start_fixed_window_index_futures_ingestion,
        )

        start_fixed_window_index_futures_ingestion()
        _logger.info("Started fixed-window index-futures ingestion lane")
    except Exception:
        _logger.exception("Failed to start fixed-window index-futures ingestion lane")

# The PID5 historical option lane is intentionally started before importing any
# app.* module. It uses only Supabase's HTTPS Data API plus Alpaca's HTTP APIs,
# so it remains available if the normal Postgres pooler route is unhealthy.
# The run-name guard is frozen to the dedicated PID5CONV queue family and does
# not open validation or holdout data on its own.
if os.getenv("OPTION_CONVEXITY_HTTP_ENABLED", "").strip().lower() in _TRUTHY:
    try:
        import option_convexity_http_worker as _pid5_option_http

        _pid5_option_http._RUN_PREFIX = "PID5CONV "
        _pid5_option_http.start_background()
        _logger.info("Started isolated PID5 option HTTP backfill lane")
    except Exception:
        _logger.exception("Failed to start isolated PID5 option HTTP backfill lane")


def _phase3_gateway_auth_available() -> bool:
    if os.getenv("PHASE3_FORWARD_GATEWAY_TOKEN", "").strip():
        return True
    if os.getenv("SUPABASE_SERVICE_ROLE_KEY", "").strip():
        return True
    database_url = os.getenv("DATABASE_URL", "").strip()
    if not database_url:
        return False
    try:
        return bool(unquote(urlsplit(database_url).password or ""))
    except ValueError:
        return False


_phase3_requested = os.getenv("PHASE3_FORWARD_MONITOR_ENABLED", "").strip().lower() in _TRUTHY
_phase3_gateway_configured = bool(
    os.getenv(
        "PHASE3_FORWARD_GATEWAY_URL",
        "https://oxzabweahkoimtevbbny.supabase.co/functions/v1/phase3-forward-gateway",
    ).strip()
    and _phase3_gateway_auth_available()
)
_phase3_original_value = os.environ.get("PHASE3_FORWARD_MONITOR_ENABLED")
_phase3_patch_ready = False

# Importing any app.* module first executes app/__init__.py. Temporarily suppress
# its normal Phase 3 startup so the HTTP transport can be installed before the
# collector thread exists. Other application startup hooks still run once.
if _phase3_requested and _phase3_gateway_configured:
    os.environ["PHASE3_FORWARD_MONITOR_ENABLED"] = "false"

if os.getenv("CINT001_BOOKTICKER_ENABLED", "").strip().lower() in _TRUTHY:
    try:
        from app.cint001_bookticker import start_background

        start_background()
    except Exception:
        _logger.exception(
            "Failed to start opt-in C-INT-001 bookTicker background backfill"
        )

if _phase3_requested:
    if not _phase3_gateway_configured:
        _logger.error(
            "Phase 3 collector requested without gateway authentication; "
            "the collector remains disabled"
        )
        os.environ["PHASE3_FORWARD_MONITOR_ENABLED"] = "false"
    else:
        try:
            from app.phase3_gateway_patch import install_gateway_patch

            install_gateway_patch()
            _phase3_patch_ready = True
        except Exception:
            _logger.exception("Failed to install the Phase 3 restricted gateway transport")
            os.environ["PHASE3_FORWARD_MONITOR_ENABLED"] = "false"

if _phase3_patch_ready:
    if _phase3_original_value is None:
        os.environ["PHASE3_FORWARD_MONITOR_ENABLED"] = "true"
    else:
        os.environ["PHASE3_FORWARD_MONITOR_ENABLED"] = _phase3_original_value
    try:
        from app.phase3_forward import start_background as start_phase3_background

        start_phase3_background()
        _logger.info("Started Phase 3 collector with the restricted HTTP gateway transport")
    except Exception:
        os.environ["PHASE3_FORWARD_MONITOR_ENABLED"] = "false"
        _logger.exception("Failed to start Phase 3 collector after installing its gateway transport")
