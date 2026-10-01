from __future__ import annotations

import hashlib
import logging
import os
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx

from app.config import get_settings

logger = logging.getLogger(__name__)

BRIDGE_PATH = "/functions/v1/astra-precursor-warmup-v6-20261001"
_PREF = re.compile(r"^(.+)p([A-Z])$")


class WarmupSourceError(RuntimeError):
    def __init__(self, message: str, *, status_code: int | None = None, split_safe: bool = False):
        super().__init__(message)
        self.status_code = status_code
        self.split_safe = split_safe


def reference_to_api_symbol(symbol: str) -> str:
    match = _PREF.fullmatch(symbol)
    if not match:
        return symbol
    return f"{match.group(1)}.PR{match.group(2)}"


def _parse_ts(value: str) -> datetime:
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def normalize_bars(api_symbol: str, raw: list[dict[str, Any]], start: datetime, end: datetime) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    last_ts: datetime | None = None
    for item in raw:
        ts_raw = item.get("t", item.get("timestamp"))
        if not ts_raw:
            raise WarmupSourceError(f"missing timestamp for {api_symbol}")
        ts = _parse_ts(str(ts_raw))
        if ts < start or ts >= end:
            raise WarmupSourceError(f"bar outside requested RTH for {api_symbol}: {ts.isoformat()}")
        if last_ts is not None and ts <= last_ts:
            raise WarmupSourceError(f"non-increasing bar timestamps for {api_symbol}")
        last_ts = ts

        def f(short: str, long: str) -> float:
            value = item.get(short, item.get(long))
            if value is None:
                raise WarmupSourceError(f"missing {long} for {api_symbol}")
            return float(value)

        o, h, l, c = f("o", "open"), f("h", "high"), f("l", "low"), f("c", "close")
        v = float(item.get("v", item.get("volume", 0)) or 0)
        n_raw = item.get("n", item.get("trade_count"))
        n = None if n_raw is None else float(n_raw)
        vw_raw = item.get("vw", item.get("vwap"))
        vw = None if vw_raw is None else float(vw_raw)
        if min(o, h, l, c) <= 0 or v < 0 or h < max(o, l, c) or l > min(o, h, c):
            raise WarmupSourceError(f"invalid OHLCV for {api_symbol} at {ts.isoformat()}")
        result.append(
            {
                "symbol": api_symbol,
                "timestamp": ts.isoformat(),
                "open": o,
                "high": h,
                "low": l,
                "close": c,
                "volume": v,
                "trade_count": n,
                "vwap": vw,
            }
        )
    if len(result) > 13:
        raise WarmupSourceError(f"unexpected >13 30Min RTH bars for {api_symbol}")
    return result


def derive_rows(
    reference_symbol: str,
    api_symbol: str,
    bars: list[dict[str, Any]],
    session_date: str,
    regular_close: datetime,
    response_sha256: str,
    source_key: str,
    requested_start: datetime,
    requested_end: datetime,
    source_observed_at: datetime,
) -> list[dict[str, Any]]:
    if not bars:
        return []
    event_time = regular_close.astimezone(timezone.utc)
    available_at = event_time + timedelta(minutes=1)
    total_volume = sum(float(x["volume"]) for x in bars)
    counts = [x["trade_count"] for x in bars if x["trade_count"] is not None]
    total_trades = sum(float(x) for x in counts) if counts else None
    base_provenance = {
        "source": "ALPACA_SIP_PREWINDOW_30MIN_V6_ACCELERATED",
        "feed": "sip",
        "timeframe": "30Min",
        "asof": "-",
        "timezone": "America/New_York",
        "requested_start": requested_start.isoformat(),
        "requested_end": requested_end.isoformat(),
        "source_api_symbol": api_symbol,
        "original_reference_symbol": reference_symbol,
        "source_file": source_key,
        "sha256": response_sha256,
        "symbol_notation_version": "CQS_PREFERRED_TO_CMS_PR_V1",
        "availability_definition": "complete_regular_close_plus_one_minute_pending_historical_identity_reconciliation",
        "vwap": "not_used_30Min_VWAP_differs_from_minute_aggregation",
    }
    rows = [
        {
            "source_symbol": reference_symbol,
            "session_date": session_date,
            "source_mode": "RTH_30MIN_AGGREGATED",
            "open": bars[0]["open"],
            "high": max(x["high"] for x in bars),
            "low": min(x["low"] for x in bars),
            "close": bars[-1]["close"],
            "volume": total_volume,
            "trade_count": total_trades,
            "source_period_volume": total_volume,
            "raw_payload": bars,
            "event_time": event_time.isoformat(),
            "available_at": available_at.isoformat(),
            "source_observed_at": source_observed_at.isoformat(),
            "identity_state": "PENDING_EXACT_HISTORICAL_TICKER_INTERVAL",
            "provenance": {
                **base_provenance,
                "source_mode": "RTH_30MIN_AGGREGATED",
                "observed_bars": len(bars),
                "volume_definition": "sum_native_SIP_RTH_30Min_volume",
            },
        }
    ]
    close_start = event_time - timedelta(minutes=30)
    close_bars = [x for x in bars if close_start <= _parse_ts(x["timestamp"]) < event_time]
    if close_bars:
        last = close_bars[-1]
        rows.append(
            {
                "source_symbol": reference_symbol,
                "session_date": session_date,
                "source_mode": "CLOSING_30MIN",
                "open": None,
                "high": None,
                "low": None,
                "close": last["close"],
                "volume": None,
                "trade_count": None,
                "source_period_volume": last["volume"],
                "raw_payload": [last],
                "event_time": event_time.isoformat(),
                "available_at": available_at.isoformat(),
                "source_observed_at": source_observed_at.isoformat(),
                "identity_state": "PENDING_EXACT_HISTORICAL_TICKER_INTERVAL",
                "provenance": {
                    **base_provenance,
                    "source_mode": "CLOSING_30MIN",
                    "observed_bars": 1,
                    "volume_definition": "volume_NULL_period_volume_only",
                },
            }
        )
    return rows


class RestWarmupRunner:
    def __init__(self, worker_id: str):
        self.worker_id = f"{worker_id}:precursor_warmup_v6"
        self.settings = get_settings()
        token = os.getenv("SCENARIO10_BACKFILL_TOKEN", "").strip()
        if not token:
            raise RuntimeError("SCENARIO10_BACKFILL_TOKEN missing for warmup REST bridge")
        self.bridge = self.settings.supabase_url.rstrip("/") + BRIDGE_PATH
        self.bridge_client = httpx.Client(
            timeout=httpx.Timeout(120.0),
            follow_redirects=True,
            headers={"Authorization": f"Bearer {token}"},
        )
        self.alpaca_client = httpx.Client(
            timeout=httpx.Timeout(45.0),
            follow_redirects=True,
            headers={
                "APCA-API-KEY-ID": self.settings.alpaca_api_key,
                "APCA-API-SECRET-KEY": self.settings.alpaca_api_secret,
            },
        )
        self.last_request_monotonic = 0.0
        self.min_interval_seconds = 2.2

    def close(self) -> None:
        self.bridge_client.close()
        self.alpaca_client.close()

    def bridge_post(self, payload: dict[str, Any]) -> dict[str, Any]:
        response = self.bridge_client.post(self.bridge, json=payload)
        response.raise_for_status()
        body = response.json()
        if body.get("error"):
            raise RuntimeError(f"warmup bridge error: {body['error']}")
        return body

    def claim(self) -> dict[str, Any] | None:
        body = self.bridge_post({"op": "claim", "worker_id": self.worker_id})
        job = body.get("job")
        if job and job.get("min_interval_ms") is not None:
            self.min_interval_seconds = max(2.2, float(job["min_interval_ms"]) / 1000.0)
        return job

    def _pace(self) -> None:
        elapsed = time.monotonic() - self.last_request_monotonic
        if elapsed < self.min_interval_seconds:
            time.sleep(self.min_interval_seconds - elapsed)

    def fetch_subset(
        self,
        job: dict[str, Any],
        reference_symbols: list[str],
        start: datetime,
        close: datetime,
        subpart: int,
    ) -> tuple[dict[str, Any], list[dict[str, Any]], int]:
        api_symbols = [reference_to_api_symbol(s) for s in reference_symbols]
        if len(set(api_symbols)) != len(api_symbols):
            raise WarmupSourceError("reference->API symbol translation collision")
        end = close - timedelta(milliseconds=1)
        params = {
            "symbols": ",".join(api_symbols),
            "timeframe": "30Min",
            "start": start.isoformat().replace("+00:00", "Z"),
            "end": end.isoformat().replace("+00:00", "Z"),
            "feed": "sip",
            "sort": "asc",
            "limit": 10000,
            "asof": "-",
        }
        started_at = datetime.now(timezone.utc)
        self._pace()
        self.last_request_monotonic = time.monotonic()
        observed_at = datetime.now(timezone.utc)
        response: httpx.Response | None = None
        try:
            response = self.alpaca_client.get("https://data.alpaca.markets/v2/stocks/bars", params=params)
            raw_text = response.text
            response_sha = hashlib.sha256(raw_text.encode("utf-8")).hexdigest()
            try:
                body = response.json()
            except Exception as exc:
                raise WarmupSourceError("Alpaca non-JSON response", status_code=response.status_code) from exc
            if response.status_code != 200:
                raise WarmupSourceError(
                    f"Alpaca HTTP {response.status_code}: {str(body)[:300]}",
                    status_code=response.status_code,
                    split_safe=response.status_code in {400, 413, 414},
                )
            if body.get("next_page_token"):
                raise WarmupSourceError("unexpected Alpaca pagination: request would truncate source")
            raw_bars = body.get("bars")
            if not isinstance(raw_bars, dict):
                raise WarmupSourceError("Alpaca bars payload is not a symbol map")
            unexpected = set(raw_bars) - set(api_symbols)
            if unexpected:
                raise WarmupSourceError(f"unexpected Alpaca symbols: {sorted(unexpected)[:5]}")
            api_to_ref = dict(zip(api_symbols, reference_symbols))
            normalized_map: dict[str, list[dict[str, Any]]] = {}
            source_rows: list[dict[str, Any]] = []
            total_bars = 0
            source_key = (
                "supabase://market_factors_20250901_20260731_v1/precursor_warmup_fast_request_v6/"
                f"{job['session_date']}:{job['batch']}:{job['attempt_number']}:{subpart}"
            )
            for api_symbol, values in raw_bars.items():
                if not isinstance(values, list):
                    raise WarmupSourceError(f"Alpaca bars list invalid for {api_symbol}")
                bars = normalize_bars(api_symbol, values, start, close)
                normalized_map[api_symbol] = bars
                total_bars += len(bars)
                ref = api_to_ref[api_symbol]
                source_rows.extend(
                    derive_rows(
                        ref,
                        api_symbol,
                        bars,
                        str(job["session_date"]),
                        close,
                        response_sha,
                        source_key,
                        start,
                        end,
                        observed_at,
                    )
                )
            attempt = {
                "subpart": subpart,
                "reference_symbols": reference_symbols,
                "api_symbols": api_symbols,
                "request_params": params,
                "status": "SOURCE_RESPONSE_VALIDATED",
                "http_status": 200,
                "returned_symbols": len(normalized_map),
                "returned_bars": total_bars,
                "response_sha256": response_sha,
                "error_detail": None,
                "source_observed_at": observed_at.isoformat(),
                "started_at": started_at.isoformat(),
                "completed_at": datetime.now(timezone.utc).isoformat(),
                "bars": normalized_map,
            }
            return attempt, source_rows, total_bars
        except Exception as exc:
            status_code = response.status_code if response is not None else getattr(exc, "status_code", None)
            response_sha = hashlib.sha256(response.text.encode("utf-8")).hexdigest() if response is not None else None
            attempt = {
                "subpart": subpart,
                "reference_symbols": reference_symbols,
                "api_symbols": api_symbols,
                "request_params": params,
                "status": "SOURCE_ERROR",
                "http_status": status_code,
                "returned_symbols": None,
                "returned_bars": None,
                "response_sha256": response_sha,
                "error_detail": f"{type(exc).__name__}: {exc}"[:1000],
                "source_observed_at": observed_at.isoformat(),
                "started_at": started_at.isoformat(),
                "completed_at": datetime.now(timezone.utc).isoformat(),
                "bars": {},
            }
            setattr(exc, "_warmup_attempt", attempt)
            raise

    def complete(self, job: dict[str, Any], attempts: list[dict[str, Any]], rows: list[dict[str, Any]], total_bars: int) -> None:
        observed_symbols = len({r["source_symbol"] for r in rows if r["source_mode"] == "RTH_30MIN_AGGREGATED"})
        self.bridge_post(
            {
                "op": "complete",
                "session_date": job["session_date"],
                "batch": int(job["batch"]),
                "attempt_number": int(job["attempt_number"]),
                "worker_id": job["worker_id"],
                "responses": attempts,
                "source_rows": rows,
                "observed_symbols": observed_symbols,
                "observed_bars": total_bars,
            }
        )

    def fail(self, job: dict[str, Any], attempts: list[dict[str, Any]], error: str) -> None:
        if not attempts:
            attempts = [
                {
                    "subpart": 0,
                    "reference_symbols": list(job["requested_symbols"]),
                    "api_symbols": [reference_to_api_symbol(s) for s in job["requested_symbols"]],
                    "request_params": {},
                    "status": "SOURCE_ERROR",
                    "http_status": None,
                    "returned_symbols": None,
                    "returned_bars": None,
                    "response_sha256": None,
                    "error_detail": error[:1000],
                    "source_observed_at": datetime.now(timezone.utc).isoformat(),
                    "bars": {},
                }
            ]
        self.bridge_post(
            {
                "op": "fail",
                "session_date": job["session_date"],
                "batch": int(job["batch"]),
                "attempt_number": int(job["attempt_number"]),
                "worker_id": job["worker_id"],
                "responses": attempts,
                "error": error[:1000],
            }
        )

    def process(self, job: dict[str, Any]) -> None:
        start = _parse_ts(job["regular_open"])
        close = _parse_ts(job["regular_close"])
        symbols = list(job["requested_symbols"])
        attempts: list[dict[str, Any]] = []
        try:
            attempt, rows, total_bars = self.fetch_subset(job, symbols, start, close, 0)
            attempts.append(attempt)
            self.complete(job, attempts, rows, total_bars)
            return
        except WarmupSourceError as exc:
            att = getattr(exc, "_warmup_attempt", None)
            if att:
                attempts.append(att)
            if not exc.split_safe or len(symbols) <= 125:
                self.fail(job, attempts, f"{type(exc).__name__}: {exc}")
                if exc.status_code == 429:
                    time.sleep(60)
                elif exc.status_code and exc.status_code >= 500:
                    time.sleep(5)
                return
        except Exception as exc:
            att = getattr(exc, "_warmup_attempt", None)
            if att:
                attempts.append(att)
            self.fail(job, attempts, f"{type(exc).__name__}: {exc}")
            time.sleep(5)
            return

        midpoint = (len(symbols) + 1) // 2
        attempts = []
        rows: list[dict[str, Any]] = []
        total_bars = 0
        try:
            for index, subset in enumerate((symbols[:midpoint], symbols[midpoint:]), start=1):
                attempt, new_rows, bars = self.fetch_subset(job, subset, start, close, index)
                attempts.append(attempt)
                rows.extend(new_rows)
                total_bars += bars
            self.complete(job, attempts, rows, total_bars)
        except Exception as exc:
            att = getattr(exc, "_warmup_attempt", None)
            if att:
                attempts.append(att)
            self.fail(job, attempts, f"{type(exc).__name__}: {exc}")
            if getattr(exc, "status_code", None) == 429:
                time.sleep(60)
            else:
                time.sleep(5)


def run_precursor_warmup_v6(worker_id: str, shutdown_event: Any) -> None:
    logger.warning("ASTRA precursor warmup v6 REST lane starting worker_id=%s", worker_id)
    try:
        runner = RestWarmupRunner(worker_id)
    except Exception:
        logger.exception("ASTRA precursor warmup v6 REST lane could not initialize")
        return
    try:
        while not shutdown_event.is_set():
            job = None
            try:
                job = runner.claim()
                if not job:
                    shutdown_event.wait(5)
                    continue
                runner.process(job)
            except Exception as exc:
                logger.exception("ASTRA precursor warmup v6 REST lane error job=%s", job and (job.get("session_date"), job.get("batch")))
                if job:
                    try:
                        runner.fail(job, [], f"{type(exc).__name__}: {exc}")
                    except Exception:
                        logger.exception("Warmup v6 REST lane could not persist failure")
                shutdown_event.wait(5)
    finally:
        runner.close()
        logger.warning("ASTRA precursor warmup v6 REST lane stopping worker_id=%s", worker_id)
