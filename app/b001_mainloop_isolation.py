from __future__ import annotations

"""Yield non-B-001 main-loop work while an exclusive B-001 run is active.

The background side lanes are already deferred by app.__init__. This module also
wraps the shared worker's ordinary collection/option maintenance entry points
*before* app.worker imports them, so a queued/running B-001 job cannot be starved
behind unrelated collection work on the same two-connection pool.

No collection state is deleted or cancelled. Deferred functions immediately
resume their original behavior once B-001 is no longer queued/running.
"""

import logging
import os
import threading
import time
from typing import Any, Callable

import app.capture as capture
import app.cint001_execution_v2 as cint001
import app.jobs as jobs
import app.option_vol as option_vol
import app.quality as quality
from app.db import fetch_one


logger = logging.getLogger(__name__)
_TRUTHY = {"1", "true", "yes", "on"}
_CACHE_SECONDS = 3.0
_cache_lock = threading.Lock()
_cache_checked_at = 0.0
_cache_active = True
_installed = False


def b001_exclusive_active() -> bool:
    global _cache_checked_at, _cache_active
    if os.getenv("B001_EXCLUSIVE", "").strip().lower() not in _TRUTHY:
        return False

    now = time.monotonic()
    with _cache_lock:
        if now - _cache_checked_at < _CACHE_SECONDS:
            return _cache_active
        try:
            row = fetch_one(
                """
                select exists(
                    select 1
                    from crypto_b001_replication_runs
                    where status in ('queued','running')
                ) active
                """
            )
            _cache_active = bool(row and row.get("active"))
        except Exception as exc:
            # Fail closed: during DB pressure/outage, preserve scarce capacity for
            # B-001 recovery instead of letting optional work race it.
            _cache_active = True
            logger.warning(
                "Unable to confirm exclusive B-001 state; main-loop side work remains deferred: %s",
                exc,
            )
        _cache_checked_at = now
        return _cache_active


def _wrap_yield(
    original: Callable[..., Any],
    deferred_value: Any,
    label: str,
) -> Callable[..., Any]:
    last_log = [0.0]

    def wrapped(*args: Any, **kwargs: Any) -> Any:
        if b001_exclusive_active():
            now = time.monotonic()
            if now - last_log[0] >= 60.0:
                logger.info("Deferring main-loop %s while exclusive B-001 work is active", label)
                last_log[0] = now
            return deferred_value
        return original(*args, **kwargs)

    return wrapped


def install() -> None:
    global _installed
    if _installed:
        return
    _installed = True

    # Startup/periodic maintenance that can be expensive on large collection tables.
    jobs.reclaim_stale_work = _wrap_yield(jobs.reclaim_stale_work, {}, "collection stale reclaim")
    cint001.reclaim_stale_execution_work = _wrap_yield(
        cint001.reclaim_stale_execution_work, 0, "C-INT-001 stale reclaim"
    )
    option_vol.reclaim_stale_option_events = _wrap_yield(
        option_vol.reclaim_stale_option_events, 0, "option stale reclaim"
    )

    # The option lane runs before the B-001 claim in the current main loop.
    option_vol.claim_option_event = _wrap_yield(
        option_vol.claim_option_event, None, "option claim"
    )

    # Ordinary miner work after the B-001 claim path. These wrappers also keep
    # the pool quiet when B-001 is already running on another/draining instance.
    jobs.claim_collection_partition = _wrap_yield(
        jobs.claim_collection_partition, None, "collection claim"
    )
    capture.advance_mining_runs = _wrap_yield(
        capture.advance_mining_runs, False, "collection stage advance"
    )
    quality.run_ready_quality_checks = _wrap_yield(
        quality.run_ready_quality_checks, 0, "collection readiness QA"
    )
    jobs.find_runs_ready_for_planning = _wrap_yield(
        jobs.find_runs_ready_for_planning, [], "collection planning scan"
    )

    logger.info("Installed exclusive B-001 main-loop isolation")


install()
