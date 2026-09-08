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
    """Allocate provider request starts globally across all backfill threads."""
    global _next_massive_slot
    with _rate_lock:
        now = time.monotonic()
        target = max(now, _next_massive_slot)
        _next_massive_slot = target + self.interval
    delay = target - now
    if delay > 0:
        time.sleep(delay)


def _stage_massive_day(
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

    rpc.call("erbv1_mark_reference_day_staged", {
        "p_run_id": run_id,
        "p_observation_date": observation_date,
        "p_page_count": page_no,
        "p_provider_rows": provider_rows,
    })
    logger.info(
        "Equity reference date fetched and staged run=%s date=%s pages=%s provider_rows=%s",
        run_id,
        observation_date,
        page_no,
        provider_rows,
    )


def _process_once(worker_id: str) -> bool:
    rpc = base.SupabaseRPC()
    claim = rpc.call("erbv1_claim_day", {"p_worker_id": worker_id}) or {}
    state = str(claim.get("state") or "none")
    if state in {"none", "busy"}:
        return False
    run_id = str(claim.get("run_id"))
    try:
        if state == "finalize_day":
            result = rpc.call("erbv1_finalize_reference_day", {
                "p_run_id": run_id,
                "p_observation_date": claim["observation_date"],
                "p_page_count": claim["page_count"],
                "p_provider_rows": claim["provider_rows"],
            })
            logger.info(
                "Equity reference date finalized in order run=%s date=%s result=%s",
                run_id,
                claim["observation_date"],
                result,
            )
            return True
        if state == "day":
            _stage_massive_day(rpc, base.MassiveClient(), run_id, str(claim["observation_date"]))
            return True
        if state == "corporate_actions":
            base._acquire_actions(
                rpc,
                base.MassiveClient(),
                run_id,
                str(claim["window_start"]),
                str(claim["window_end"]),
            )
            return True
        if state == "finalize":
            result = rpc.call("erbv1_finalize_run", {"p_run_id": run_id})
            logger.info("Equity reference staged backfill finalized run=%s result=%s", run_id, result)
            return True
        return False
    except Exception as exc:
        text = f"{type(exc).__name__}: {exc}"[:1800]
        status = (
            "blocked_provider_access"
            if "HTTP 401" in text or "HTTP 403" in text or "credentials absent" in text.lower()
            else "retry"
        )
        try:
            rpc.call("erbv1_mark_error", {
                "p_run_id": run_id,
                "p_observation_date": claim.get("observation_date"),
                "p_status": status,
                "p_error": text,
            })
        except Exception:
            logger.exception("Failed to persist equity reference backfill error")
        logger.exception("Equity reference backfill iteration failed run=%s state=%s", run_id, state)
        time.sleep(5.0)
        return True


def _runner(worker_no: int) -> None:
    worker_id = (
        f"equity-reference-rest-{worker_no}:"
        f"{os.getenv('RENDER_INSTANCE_ID') or os.uname().nodename}:{os.getpid()}"
    )
    while True:
        try:
            if not _process_once(worker_id):
                time.sleep(1.0)
        except Exception:
            logger.exception("Equity reference parallel lane escaped top-level iteration worker=%s", worker_no)
            time.sleep(5.0)


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
        for worker_no in range(1, threads + 1):
            threading.Thread(
                target=_runner,
                args=(worker_no,),
                name=f"equity-reference-rest-{worker_no}",
                daemon=True,
            ).start()
        logger.info(
            "Started governed equity reference backfill with %s fetch/finalize lanes under one global Massive rate schedule",
            threads,
        )
