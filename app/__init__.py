from __future__ import annotations

import logging
import os
import threading

__version__ = "3.4.0"

_TRUTHY = {"1", "true", "yes", "on"}
_logger = logging.getLogger(__name__)

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


# B-001's exclusive flag exists only on the long-running worker. Install the
# set-based placebo execution before the worker imports its analysis facade.
# This changes query shape only; frozen placebo definitions and economics stay
# identical. Keep the independent metrics reporter active during long phases.
if os.getenv("B001_EXCLUSIVE", "").strip().lower() in _TRUTHY:
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

        _defer_background_start(start_tardis_depth_background, "C-INT-001 Tardis depth")
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
