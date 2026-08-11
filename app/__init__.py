from __future__ import annotations

import logging
import os
import threading

__version__ = "3.4.0"

_TRUTHY = {"1", "true", "yes", "on"}

# Install durable collection resilience before worker/side-lane modules import
# functions from app.jobs. This is operational hardening only: it validates
# checkpoints, separates infrastructure retries from research/data failures, and
# preserves completed work.
try:
    import app.collection_operational_hardening  # noqa: F401
except Exception:
    logging.getLogger(__name__).exception(
        "Failed to install collection operational hardening"
    )

# B-001's exclusive flag exists only on the long-running worker. Keep a tiny
# independent reporter on that worker so queue depth, recent throughput, retries
# and permanent failure rate remain visible even while one research phase runs
# for a long time.
if os.getenv("B001_EXCLUSIVE", "").strip().lower() in _TRUTHY:
    try:
        from app.b001_metrics_reporter import start_background as start_b001_metrics_background

        start_b001_metrics_background()
    except Exception:
        logging.getLogger(__name__).exception(
            "Failed to start B-001 live operational metrics reporter"
        )

# Opt-in research backfills must be explicitly enabled on the intended service.
# `python -m app.worker` always imports this package before the worker module,
# making this a reliable bootstrap point without changing the production worker
# command. All other services remain inert because the flags are absent.
if os.getenv("CINT001_BOOKTICKER_ENABLED", "").strip().lower() in _TRUTHY:
    try:
        from app.cint001_bookticker import start_background as start_bookticker_background

        start_bookticker_background()
    except Exception:
        logging.getLogger(__name__).exception(
            "Failed to start opt-in C-INT-001 Binance bookTicker backfill"
        )

if os.getenv("CINT001_TARDIS_QUOTES_ENABLED", "").strip().lower() in _TRUTHY:
    try:
        from app.cint001_tardis_quotes import start_background as start_tardis_quotes_background

        start_tardis_quotes_background()
    except Exception:
        logging.getLogger(__name__).exception(
            "Failed to start opt-in C-INT-001 Tardis quote sample backfill"
        )

if os.getenv("CINT001_TARDIS_DEPTH_ENABLED", "").strip().lower() in _TRUTHY:
    try:
        from app.cint001_tardis_depth import start_background as start_tardis_depth_background

        start_tardis_depth_background()
    except Exception:
        logging.getLogger(__name__).exception(
            "Failed to start opt-in C-INT-001 Tardis depth sample backfill"
        )

# The cyclical monitor is also opt-in and is enabled only on the collection
# worker service. It runs in a daemon thread so long-running research jobs cannot
# delay the once-per-minute signal check. It writes alerts only; it never places
# orders.
if os.getenv("CYCLICAL_LIVE_MONITOR_ENABLED", "").strip().lower() in _TRUTHY:
    try:
        from app.cyclical_live_monitor import run_cyclical_monitor_loop

        _cyclical_monitor_stop = threading.Event()
        _cyclical_monitor_thread = threading.Thread(
            target=run_cyclical_monitor_loop,
            args=(_cyclical_monitor_stop,),
            name="cyclical-live-monitor",
            daemon=True,
        )
        _cyclical_monitor_thread.start()
    except Exception:
        logging.getLogger(__name__).exception(
            "Failed to start cyclical leadership live monitor"
        )

# A separate opt-in urgent lane claims designated high-priority bars, trades and
# quotes. This lets targeted collection continue alongside long B-001 work
# without weakening B-001's own execution boundary.
if os.getenv("URGENT_COLLECTION_ENABLED", "").strip().lower() in _TRUTHY:
    try:
        from app.urgent_collection import run_urgent_collection_loop

        _urgent_collection_stop = threading.Event()
        _urgent_collection_thread = threading.Thread(
            target=run_urgent_collection_loop,
            args=(_urgent_collection_stop,),
            name="urgent-collection",
            daemon=True,
        )
        _urgent_collection_thread.start()
    except Exception:
        logging.getLogger(__name__).exception(
            "Failed to start urgent collection lane"
        )
