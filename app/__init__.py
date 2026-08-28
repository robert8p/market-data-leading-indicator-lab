from __future__ import annotations

import logging
import os
import threading

from app.database_url import normalise_custom_supabase_pooler_route

__version__ = "3.5.2"

_TRUTHY = {"1", "true", "yes", "on"}
_logger = logging.getLogger(__name__)

# Versioned custom PostgreSQL logins are supported by the Supabase session
# pooler, not the transaction-pooler endpoint. Rewrite only the route; preserve
# the existing credential and never log the URL.
_raw_database_url = os.getenv("DATABASE_URL", "")
if _raw_database_url:
    _normalised_database_url = normalise_custom_supabase_pooler_route(_raw_database_url)
    if _normalised_database_url != _raw_database_url:
        os.environ["DATABASE_URL"] = _normalised_database_url
        _logger.info("Normalised the custom database identity onto the Supabase session pooler")

# Install durable collection resilience before worker/side-lane modules import
# functions from app.jobs. This is operational hardening only: it validates
# checkpoints, separates infrastructure retries from research/data failures, and
# preserves completed work.
try:
    import app.collection_operational_hardening  # noqa: F401
except Exception:
    _logger.exception("Failed to install collection operational hardening")


def _b001_exclusive_active() -> bool:
    """Return True while this worker must reserve its shared DB pool for B-001.

    On a database connectivity failure, fail closed and keep optional side lanes
    deferred. This prevents an outage from allowing background work to consume
    the last available pool connection before B-001 can recover.
    """
    if os.getenv("B001_EXCLUSIVE", "").strip().lower() not in _TRUTHY:
        return False
    try:
        from app.db import fetch_one

        row = fetch_one(
            """
            select exists(
                select 1 from crypto_b001_replication_runs
                 where status in ('queued','running')
            ) active
            """
        )
        return bool(row and row.get("active"))
    except Exception:
        _logger.warning(
            "Unable to confirm B-001 exclusive state; optional worker side lanes remain deferred",
            exc_info=True,
        )
        return True


def _defer_background_start(start_callable, label: str) -> None:
    def runner() -> None:
        stop = threading.Event()
        announced = False
        while _b001_exclusive_active():
            if not announced:
                _logger.info("Deferring %s while exclusive B-001 work is active", label)
                announced = True
            stop.wait(15.0)
        try:
            start_callable()
        except Exception:
            _logger.exception("Failed to start deferred %s", label)

    threading.Thread(
        target=runner,
        name=f"deferred-{label}",
        daemon=True,
    ).start()


def _run_loop_after_b001(loop_callable, stop: threading.Event, label: str) -> None:
    announced = False
    while not stop.is_set() and _b001_exclusive_active():
        if not announced:
            _logger.info("Deferring %s while exclusive B-001 work is active", label)
            announced = True
        stop.wait(15.0)
    if not stop.is_set():
        loop_callable(stop)


# B-001's exclusive flag exists only on the long-running worker. Install both
# main-loop isolation and set-based placebo execution before app.worker imports
# its function bindings. Frozen research definitions/economics are unchanged.
if os.getenv("B001_EXCLUSIVE", "").strip().lower() in _TRUTHY:
    try:
        import app.b001_mainloop_isolation  # noqa: F401
    except Exception:
        _logger.exception("Failed to install exclusive B-001 main-loop isolation")
    try:
        import app.b001_placebo_acceleration  # noqa: F401
    except Exception:
        _logger.exception("Failed to install B-001 set-based placebo acceleration")
    try:
        from app.b001_metrics_reporter import start_background as start_b001_metrics_background

        start_b001_metrics_background()
    except Exception:
        _logger.exception("Failed to start B-001 live operational metrics reporter")

# Opt-in research backfills are deferred while an exclusive B-001 run is active.
# They start automatically once B-001 reaches a terminal state, so no manual
# environment-variable restoration is required.
if os.getenv("CINT001_BOOKTICKER_ENABLED", "").strip().lower() in _TRUTHY:
    try:
        from app.cint001_bookticker import start_background as start_bookticker_background

        _defer_background_start(start_bookticker_background, "C-INT-001 bookTicker")
    except Exception:
        _logger.exception("Failed to prepare opt-in C-INT-001 Binance bookTicker backfill")

if os.getenv("CINT001_TARDIS_QUOTES_ENABLED", "").strip().lower() in _TRUTHY:
    try:
        from app.cint001_tardis_quotes import start_background as start_tardis_quotes_background

        _defer_background_start(start_tardis_quotes_background, "C-INT-001 Tardis quotes")
    except Exception:
        _logger.exception("Failed to prepare opt-in C-INT-001 Tardis quote sample backfill")

if os.getenv("CINT001_TARDIS_DEPTH_ENABLED", "").strip().lower() in _TRUTHY:
    try:
        from app.cint001_tardis_depth import start_background as start_tardis_depth_background

        _defer_background_start(start_tardis_depth_background, "C-INT-001 Tardis depth sample backfill")
    except Exception:
        _logger.exception("Failed to prepare opt-in C-INT-001 Tardis depth sample backfill")

if os.getenv("CYCLICAL_LIVE_MONITOR_ENABLED", "").strip().lower() in _TRUTHY:
    try:
        from app.cyclical_live_monitor import run_cyclical_monitor_loop

        _cyclical_monitor_stop = threading.Event()
        _cyclical_monitor_thread = threading.Thread(
            target=_run_loop_after_b001,
            args=(run_cyclical_monitor_loop, _cyclical_monitor_stop, "cyclical live monitor"),
            name="cyclical-live-monitor",
            daemon=True,
        )
        _cyclical_monitor_thread.start()
    except Exception:
        _logger.exception("Failed to prepare cyclical leadership live monitor")

if os.getenv("URGENT_COLLECTION_ENABLED", "").strip().lower() in _TRUTHY:
    try:
        from app.urgent_collection import run_urgent_collection_loop

        _urgent_collection_stop = threading.Event()
        _urgent_collection_thread = threading.Thread(
            target=_run_loop_after_b001,
            args=(run_urgent_collection_loop, _urgent_collection_stop, "urgent collection"),
            name="urgent-collection",
            daemon=True,
        )
        _urgent_collection_thread.start()
    except Exception:
        _logger.exception("Failed to prepare urgent collection lane")

# Phase 3 evidence capture is time-critical and deliberately not deferred by the
# B-001 backfill. It uses a single bounded DB lease and short API/DB operations,
# has no trading path, and cannot read accumulated sealed outcomes.
if os.getenv("PHASE3_FORWARD_MONITOR_ENABLED", "").strip().lower() in _TRUTHY:
    try:
        from app.phase3_forward import start_background as start_phase3_forward_background

        start_phase3_forward_background()
    except Exception:
        _logger.exception("Failed to start the sealed Phase 3 forward monitor")

# The strategy factory performs long-running, zero-capital research stages that
# cannot complete inside the database scheduler's short statement timeout. It is
# opt-in on the dedicated Render worker, and is deferred while exclusive B-001
# work is active so the two heavy lanes do not compete for the shared DB pool.
if os.getenv("STRATEGY_FACTORY_AUTOMATION_ENABLED", "").strip().lower() in _TRUTHY:
    try:
        from app.strategy_factory_automation import start_background as start_strategy_factory_background

        _defer_background_start(start_strategy_factory_background, "strategy factory automation")
    except Exception:
        _logger.exception("Failed to prepare the strategy-factory automation lane")
