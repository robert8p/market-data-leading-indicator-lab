from __future__ import annotations

import logging
import os
import threading
import time

import app.equity_reference_backfill_http as base

logger = logging.getLogger(__name__)
_start_lock = threading.Lock()
_started = False
_rate_lock = threading.Lock()
_next_massive_slot = 0.0


def _global_massive_throttle(self) -> None:
    """Allocate provider request starts globally across all backfill threads.

    This keeps the aggregate request-start rate at the existing MASSIVE_RPM
    setting while allowing response latency and Supabase RPC work to overlap.
    """
    global _next_massive_slot
    with _rate_lock:
        now = time.monotonic()
        target = max(now, _next_massive_slot)
        _next_massive_slot = target + self.interval
    delay = target - now
    if delay > 0:
        time.sleep(delay)


def _ordered_stage_massive_day(
    rpc: base.SupabaseRPC,
    massive: base.MassiveClient,
    run_id: str,
    observation_date: str,
) -> None:
    next_url: str | None = None
    page_no = 0
    provider_rows = 0
    while page_no == 0 or next_url:
        params = None if next_url else {
            "date": observation_date,
            "market": "stocks",
            "active": "true",
            "order": "asc",
            "sort": "ticker",
            "limit": 1000,
        }
        payload = massive.get(next_url or "/v3/reference/tickers", params)
        rows = payload.get("results") or []
        if not isinstance(rows, list):
            raise RuntimeError("Massive reference results were not a list")
        page_no += 1
        provider_rows += len(rows)
        shaped = []
        for item in rows:
            if not isinstance(item, dict):
                continue
            shaped.append({
                "ticker": item.get("ticker"),
                "name": item.get("name"),
                "type": item.get("type"),
                "active": item.get("active"),
                "primary_exchange": item.get("primary_exchange"),
                "market": item.get("market"),
                "locale": item.get("locale"),
                "currency_name": item.get("currency_name"),
                "cik": item.get("cik"),
                "composite_figi": item.get("composite_figi"),
                "share_class_figi": item.get("share_class_figi"),
                "last_updated_utc": item.get("last_updated_utc"),
                "payload": item,
            })
        rpc.call("erbv1_stage_reference_page", {
            "p_run_id": run_id,
            "p_observation_date": observation_date,
            "p_page_no": page_no,
            "p_page_checksum": base._json_hash(rows),
            "p_rows": shaped,
        })
        next_url = base._clean_next_url(str(payload.get("next_url"))) if payload.get("next_url") else None

    if page_no < 1 or provider_rows < 1:
        raise RuntimeError(f"Massive returned no historical reference rows for {observation_date}")

    while True:
        result = rpc.call("erbv1_finalize_reference_day", {
            "p_run_id": run_id,
            "p_observation_date": observation_date,
            "p_page_count": page_no,
            "p_provider_rows": provider_rows,
        }) or {}
        if result.get("completed"):
            break
        if result.get("waiting_prior_date"):
            time.sleep(0.75)
            continue
        raise RuntimeError(f"Unexpected ordered-finalize response for {observation_date}: {result}")

    logger.info(
        "Equity reference date completed run=%s date=%s pages=%s provider_rows=%s",
        run_id,
        observation_date,
        page_no,
        provider_rows,
    )


def _runner(worker_no: int) -> None:
    worker_id = (
        f"equity-reference-rest-{worker_no}:"
        f"{os.getenv('RENDER_INSTANCE_ID') or os.uname().nodename}:{os.getpid()}"
    )
    while True:
        try:
            if not base.process_once(worker_id):
                time.sleep(3.0)
        except Exception:
            logger.exception("Equity reference parallel lane escaped top-level iteration worker=%s", worker_no)
            time.sleep(10.0)


def start_background() -> None:
    global _started
    if not base._truthy("EQUITY_REFERENCE_BACKFILL_ENABLED"):
        return
    with _start_lock:
        if _started:
            return
        _started = True
        try:
            threads = int(os.getenv("EQUITY_REFERENCE_BACKFILL_THREADS", "3"))
        except ValueError:
            threads = 3
        threads = min(4, max(1, threads))
        base.MassiveClient._throttle = _global_massive_throttle
        base._stage_massive_day = _ordered_stage_massive_day
        for worker_no in range(1, threads + 1):
            threading.Thread(
                target=_runner,
                args=(worker_no,),
                name=f"equity-reference-rest-{worker_no}",
                daemon=True,
            ).start()
        logger.info(
            "Started governed equity reference backfill with %s fetch lanes under one global Massive rate schedule",
            threads,
        )
