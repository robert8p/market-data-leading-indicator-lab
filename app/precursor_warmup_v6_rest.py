from __future__ import annotations

import hashlib
import json
import logging
import os
import time
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx

from app.config import get_settings

logger = logging.getLogger(__name__)

EDGE_SLUG = "astra-precursor-warmup-v6-20261001"
TOKEN_ENV = "SCENARIO10_BACKFILL_TOKEN"
SOURCE_NAME = "ALPACA_SIP_PREWINDOW_30MIN_V6_ACCELERATED"


def _api_symbol(symbol: str) -> str:
    if len(symbol) >= 3 and symbol[-2] == "p" and symbol[-1].isalpha() and symbol[-1].isupper():
        return f"{symbol[:-2]}.PR{symbol[-1]}"
    return symbol


def _iso(value: str | datetime) -> str:
    if isinstance(value, datetime):
        dt = value
    else:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat()


def _parse_dt(value: str | datetime) -> datetime:
    return datetime.fromisoformat(_iso(value))


def _normalize_one(api_symbol: str, item: dict[str, Any], start: datetime, close: datetime) -> dict[str, Any]:
    ts_raw = item.get("t", item.get("timestamp"))
    if not ts_raw:
        raise ValueError(f"missing_timestamp:{api_symbol}")
    ts = _parse_dt(str(ts_raw))
    if ts < start or ts >= close:
        raise ValueError(f"bar_outside_rth:{api_symbol}:{ts.isoformat()}")

    def required(short: str, long: str) -> float:
        value = item.get(short, item.get(long))
        if value is None:
            raise ValueError(f"missing_{long}:{api_symbol}")
        out = float(value)
        if not (out == out and abs(out) != float("inf")):
            raise ValueError(f"nonfinite_{long}:{api_symbol}")
        return out

    o = required("o", "open")
    h = required("h", "high")
    l = required("l", "low")
    c = required("c", "close")
    v = float(item.get("v", item.get("volume", 0)) or 0)
    n_raw = item.get("n", item.get("trade_count"))
    vw_raw = item.get("vw", item.get("vwap"))
    n = None if n_raw is None else float(n_raw)
    vw = None if vw_raw is None else float(vw_raw)
    if min(o, h, l, c) <= 0 or v < 0 or h < max(o, l, c) or l > min(o, h, c):
        raise ValueError(f"invalid_ohlcv:{api_symbol}:{ts.isoformat()}")
    return {
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


def _normalize_map(
    raw_bars: dict[str, Any],
    api_symbols: list[str],
    start: datetime,
    close: datetime,
) -> dict[str, list[dict[str, Any]]]:
    unexpected = set(raw_bars) - set(api_symbols)
    if unexpected:
        raise ValueError(f"unexpected_symbols:{sorted(unexpected)[:5]}")
    out: dict[str, list[dict[str, Any]]] = {}
    for api in api_symbols:
        raw = raw_bars.get(api, [])
        if not isinstance(raw, list):
            raise ValueError(f"bars_not_array:{api}")
        rows = [_normalize_one(api, x, start, close) for x in raw]
        if len(rows) > 13:
            raise ValueError(f"too_many_30min_bars:{api}:{len(rows)}")
        stamps = [x["timestamp"] for x in rows]
        if stamps != sorted(set(stamps)):
            raise ValueError(f"non_increasing_bars:{api}")
        if rows:
            out[api] = rows
    return out


def _source_rows(
    *,
    session_date: str,
    start: datetime,
    close: datetime,
    reference_symbols: list[str],
    api_symbols: list[str],
    bars_by_api: dict[str, list[dict[str, Any]]],
    response_sha256: str,
    source_observed_at: str,
    subpart: int,
    attempt: int,
    batch: int,
) -> list[dict[str, Any]]:
    api_to_ref = dict(zip(api_symbols, reference_symbols))
    rows: list[dict[str, Any]] = []
    event_time = close.astimezone(timezone.utc)
    available_at = event_time + timedelta(minutes=1)
    requested_end = close - timedelta(milliseconds=1)

    for api, bars in bars_by_api.items():
        if not bars:
            continue
        ref = api_to_ref[api]
        total_volume = sum(float(x["volume"]) for x in bars)
        trade_counts = [float(x["trade_count"]) for x in bars if x["trade_count"] is not None]
        total_trades = sum(trade_counts) if trade_counts else None
        base = {
            "source": SOURCE_NAME,
            "feed": "sip",
            "timeframe": "30Min",
            "asof": "-",
            "timezone": "America/New_York",
            "requested_start": start.isoformat(),
            "requested_end": requested_end.isoformat(),
            "source_api_symbol": api,
            "original_reference_symbol": ref,
            "source_file": (
                f"supabase://market_factors_20250901_20260731_v1/"
                f"precursor_warmup_fast_request_v6/{session_date}:{batch}:{attempt}:{subpart}"
            ),
            "sha256": response_sha256,
            "symbol_notation_version": "CQS_PREFERRED_TO_CMS_PR_V1",
            "availability_definition": (
                "complete_regular_close_plus_one_minute_pending_historical_identity_reconciliation"
            ),
            "vwap": "not_used_30Min_VWAP_differs_from_minute_aggregation",
        }
        rows.append(
            {
                "source_symbol": ref,
                "session_date": session_date,
                "source_mode": "RTH_30MIN_AGGREGATED",
                "close": bars[-1]["close"],
                "open": bars[0]["open"],
                "high": max(x["high"] for x in bars),
                "low": min(x["low"] for x in bars),
                "volume": total_volume,
                "trade_count": total_trades,
                "source_period_volume": total_volume,
                "raw_payload": bars,
                "event_time": event_time.isoformat(),
                "available_at": available_at.isoformat(),
                "source_observed_at": source_observed_at,
                "identity_state": "PENDING_EXACT_HISTORICAL_TICKER_INTERVAL",
                "provenance": {
                    **base,
                    "source_mode": "RTH_30MIN_AGGREGATED",
                    "observed_bars": len(bars),
                    "volume_definition": "sum_native_SIP_RTH_30Min_volume",
                },
            }
        )
        close_start = event_time - timedelta(minutes=30)
        closing = [x for x in bars if close_start <= _parse_dt(x["timestamp"]) < event_time]
        if closing:
            last = closing[-1]
            rows.append(
                {
                    "source_symbol": ref,
                    "session_date": session_date,
                    "source_mode": "CLOSING_30MIN",
                    "close": last["close"],
                    "open": None,
                    "high": None,
                    "low": None,
                    "volume": None,
                    "trade_count": None,
                    "source_period_volume": last["volume"],
                    "raw_payload": [last],
                    "event_time": event_time.isoformat(),
                    "available_at": available_at.isoformat(),
                    "source_observed_at": source_observed_at,
                    "identity_state": "PENDING_EXACT_HISTORICAL_TICKER_INTERVAL",
                    "provenance": {
                        **base,
                        "source_mode": "CLOSING_30MIN",
                        "observed_bars": 1,
                        "volume_definition": "volume_NULL_period_volume_only",
                    },
                }
            )
    return rows


class WarmupRestLane:
    def __init__(self, worker_id: str):
        self.worker_id = f"{worker_id}:precursor_warmup_v6_rest"
        self.settings = get_settings()
        token = os.getenv(TOKEN_ENV, "").strip()
        if not token:
            raise RuntimeError(f"{TOKEN_ENV} is required when PRECURSOR_WARMUP_V6_ENABLED=true")
        self.bridge = (
            self.settings.supabase_url.rstrip("/")
            + f"/functions/v1/{EDGE_SLUG}"
        )
        self.control = httpx.Client(
            timeout=httpx.Timeout(60.0),
            follow_redirects=True,
            headers={"Authorization": f"Bearer {token}"},
        )
        self.market = httpx.Client(
            timeout=httpx.Timeout(45.0),
            follow_redirects=True,
            headers={
                "APCA-API-KEY-ID": self.settings.alpaca_api_key,
                "APCA-API-SECRET-KEY": self.settings.alpaca_api_secret,
            },
        )
        self.last_market_call = 0.0

    def close(self) -> None:
        self.control.close()
        self.market.close()

    def _post(self, payload: dict[str, Any]) -> Any:
        response = self.control.post(self.bridge, json=payload)
        response.raise_for_status()
        return response.json()

    def _pace(self, min_interval_ms: int) -> None:
        interval = max(2.2, float(min_interval_ms) / 1000.0)
        remaining = interval - (time.monotonic() - self.last_market_call)
        if remaining > 0:
            time.sleep(remaining)

    def _fetch_part(
        self,
        job: dict[str, Any],
        reference_symbols: list[str],
        subpart: int,
    ) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        api_symbols = [_api_symbol(s) for s in reference_symbols]
        if len(set(api_symbols)) != len(api_symbols):
            raise ValueError("reference_to_api_symbol_collision")

        start = _parse_dt(job["regular_open"])
        close = _parse_dt(job["regular_close"])
        end = close - timedelta(milliseconds=1)
        params = {
            "symbols": ",".join(api_symbols),
            "timeframe": "30Min",
            "start": start.isoformat().replace("+00:00", "Z"),
            "end": end.isoformat().replace("+00:00", "Z"),
            "feed": "sip",
            "sort": "asc",
            "limit": "10000",
            "asof": "-",
        }
        started = datetime.now(timezone.utc)
        self._pace(int(job.get("min_interval_ms") or 2200))
        self.last_market_call = time.monotonic()
        response: httpx.Response | None = None
        body: Any = {}
        try:
            response = self.market.get(
                "https://data.alpaca.markets/v2/stocks/bars",
                params=params,
            )
            observed = datetime.now(timezone.utc)
            raw_text = response.text
            response_sha256 = hashlib.sha256(raw_text.encode("utf-8")).hexdigest()
            try:
                body = response.json()
            except Exception as exc:
                raise RuntimeError("alpaca_non_json_response") from exc
            if response.status_code != 200:
                error = f"alpaca_http_{response.status_code}:{str(body)[:300]}"
                return (
                    {
                        "subpart": subpart,
                        "reference_symbols": reference_symbols,
                        "api_symbols": api_symbols,
                        "request_params": params,
                        "status": "SOURCE_ERROR",
                        "http_status": response.status_code,
                        "returned_symbols": None,
                        "returned_bars": None,
                        "response_sha256": response_sha256,
                        "error_detail": error,
                        "started_at": started.isoformat(),
                        "completed_at": observed.isoformat(),
                        "source_observed_at": observed.isoformat(),
                        "bars": {},
                    },
                    [],
                )
            if body.get("next_page_token"):
                raise RuntimeError("unexpected_alpaca_pagination")
            raw_bars = body.get("bars")
            if not isinstance(raw_bars, dict):
                raise RuntimeError("alpaca_bars_payload_not_symbol_map")
            normalized = _normalize_map(raw_bars, api_symbols, start, close)
            total_bars = sum(len(x) for x in normalized.values())
            record = {
                "subpart": subpart,
                "reference_symbols": reference_symbols,
                "api_symbols": api_symbols,
                "request_params": params,
                "status": "SOURCE_RESPONSE_VALIDATED",
                "http_status": 200,
                "returned_symbols": len(normalized),
                "returned_bars": total_bars,
                "response_sha256": response_sha256,
                "error_detail": None,
                "started_at": started.isoformat(),
                "completed_at": observed.isoformat(),
                "source_observed_at": observed.isoformat(),
                "bars": normalized,
            }
            rows = _source_rows(
                session_date=str(job["session_date"]),
                start=start,
                close=close,
                reference_symbols=reference_symbols,
                api_symbols=api_symbols,
                bars_by_api=normalized,
                response_sha256=response_sha256,
                source_observed_at=observed.isoformat(),
                subpart=subpart,
                attempt=int(job["attempt_number"]),
                batch=int(job["batch"]),
            )
            return record, rows
        except Exception as exc:
            observed = datetime.now(timezone.utc)
            status = response.status_code if response is not None else None
            sha256 = None
            if response is not None:
                sha256 = hashlib.sha256(response.text.encode("utf-8")).hexdigest()
            return (
                {
                    "subpart": subpart,
                    "reference_symbols": reference_symbols,
                    "api_symbols": api_symbols,
                    "request_params": params,
                    "status": "SOURCE_ERROR",
                    "http_status": status,
                    "returned_symbols": None,
                    "returned_bars": None,
                    "response_sha256": sha256,
                    "error_detail": f"{type(exc).__name__}:{exc}"[:1000],
                    "started_at": started.isoformat(),
                    "completed_at": observed.isoformat(),
                    "source_observed_at": observed.isoformat(),
                    "bars": {},
                },
                [],
            )

    def process(self, job: dict[str, Any]) -> None:
        refs = [str(x) for x in job["requested_symbols"]]
        responses: list[dict[str, Any]] = []
        source_rows: list[dict[str, Any]] = []

        primary, primary_rows = self._fetch_part(job, refs, 0)
        responses.append(primary)
        source_rows.extend(primary_rows)

        split_safe = primary["status"] != "SOURCE_RESPONSE_VALIDATED" and primary.get("http_status") in {400, 413, 414}
        if split_safe and len(refs) > 125:
            responses = []
            source_rows = []
            midpoint = (len(refs) + 1) // 2
            for index, part in enumerate((refs[:midpoint], refs[midpoint:]), start=1):
                result, rows = self._fetch_part(job, part, index)
                responses.append(result)
                source_rows.extend(rows)
                if result["status"] != "SOURCE_RESPONSE_VALIDATED":
                    break

        success = all(x["status"] == "SOURCE_RESPONSE_VALIDATED" for x in responses)
        base = {
            "session_date": job["session_date"],
            "batch": int(job["batch"]),
            "attempt_number": int(job["attempt_number"]),
            "worker_id": self.worker_id,
            "responses": responses,
        }
        if success:
            observed_symbols = len(
                {
                    row["source_symbol"]
                    for row in source_rows
                    if row["source_mode"] == "RTH_30MIN_AGGREGATED"
                }
            )
            observed_bars = sum(
                len(row["raw_payload"])
                for row in source_rows
                if row["source_mode"] == "RTH_30MIN_AGGREGATED"
            )
            payload = {
                **base,
                "op": "complete",
                "source_rows": source_rows,
                "observed_symbols": observed_symbols,
                "observed_bars": observed_bars,
            }
            self._post(payload)
            return

        error = next(
            (x.get("error_detail") for x in responses if x["status"] != "SOURCE_RESPONSE_VALIDATED"),
            "source_request_failed",
        )
        self._post({**base, "op": "fail", "error": error})
        failed_status = next(
            (x.get("http_status") for x in responses if x["status"] != "SOURCE_RESPONSE_VALIDATED"),
            None,
        )
        if failed_status == 429:
            time.sleep(60)
        elif failed_status and int(failed_status) >= 500:
            time.sleep(5)

    def run(self, shutdown_event: Any) -> None:
        logger.warning("ASTRA precursor warmup v6 REST lane starting worker_id=%s", self.worker_id)
        try:
            while not shutdown_event.is_set():
                try:
                    data = self._post({"op": "claim", "worker_id": self.worker_id})
                    job = data.get("job")
                    if not job:
                        shutdown_event.wait(2)
                        continue
                    self.process(job)
                except httpx.HTTPStatusError as exc:
                    logger.warning(
                        "Warmup v6 bridge HTTP error status=%s error=%s",
                        exc.response.status_code,
                        exc,
                    )
                    shutdown_event.wait(5 if exc.response.status_code >= 500 else 30)
                except Exception:
                    logger.exception("Warmup v6 REST lane escaped tick protection")
                    shutdown_event.wait(5)
        finally:
            self.close()
            logger.warning("ASTRA precursor warmup v6 REST lane stopping worker_id=%s", self.worker_id)


def run_precursor_warmup_v6_rest(worker_id: str, shutdown_event: Any) -> None:
    WarmupRestLane(worker_id).run(shutdown_event)
