from __future__ import annotations

"""HTTP-only historical option backfill for the frozen PID5 convexity study.

This module deliberately does not import ``app``. It exists so the historical
single-stock option lane can continue while the main worker's Postgres pooler
route is unavailable. Queue state and results are read/written through the
Supabase Data API with the service-role credential already held by the Render
worker. Alpaca credentials are read from the same worker environment.

The module only consumes runs whose names start with ``PID5-CONVEXITY-``.
It does not create signals, tune rules, or open validation/holdout data.
"""

import json
import logging
import os
import threading
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any

import httpx

logger = logging.getLogger(__name__)

_TRUTHY = {"1", "true", "yes", "on"}
_RUN_PREFIX = "PID5-CONVEXITY-"
_PENDING = {"queued", "retry_wait"}
_TERMINAL_BAD = {"failed", "skipped"}


def _utc(value: str | datetime) -> datetime:
    if isinstance(value, datetime):
        dt = value
    else:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _as_float(value: Any) -> float | None:
    try:
        return None if value is None else float(value)
    except (TypeError, ValueError):
        return None


def _as_int(value: Any) -> int | None:
    try:
        return None if value is None else int(value)
    except (TypeError, ValueError):
        return None


def _jsonable(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    raise TypeError(f"Unsupported JSON type: {type(value)!r}")


@dataclass(frozen=True)
class Env:
    supabase_url: str
    service_key: str
    alpaca_key: str
    alpaca_secret: str
    poll_seconds: float

    @classmethod
    def load(cls) -> "Env":
        values = {
            "SUPABASE_URL": os.getenv("SUPABASE_URL", "").strip(),
            "SUPABASE_SERVICE_ROLE_KEY": os.getenv("SUPABASE_SERVICE_ROLE_KEY", "").strip(),
            "ALPACA_API_KEY": os.getenv("ALPACA_API_KEY", "").strip(),
            "ALPACA_API_SECRET": os.getenv("ALPACA_API_SECRET", "").strip(),
        }
        missing = [name for name, value in values.items() if not value]
        if missing:
            raise RuntimeError(f"Missing HTTP option worker settings: {', '.join(missing)}")
        return cls(
            supabase_url=values["SUPABASE_URL"].rstrip("/"),
            service_key=values["SUPABASE_SERVICE_ROLE_KEY"],
            alpaca_key=values["ALPACA_API_KEY"],
            alpaca_secret=values["ALPACA_API_SECRET"],
            poll_seconds=max(1.0, float(os.getenv("OPTION_CONVEXITY_HTTP_POLL_SECONDS", "5"))),
        )


class Rest:
    def __init__(self, env: Env) -> None:
        self.base = f"{env.supabase_url}/rest/v1"
        self.client = httpx.Client(
            timeout=httpx.Timeout(60.0),
            headers={
                "apikey": env.service_key,
                "authorization": f"Bearer {env.service_key}",
                "accept": "application/json",
                "content-type": "application/json",
            },
        )

    def get(self, table: str, params: dict[str, Any]) -> list[dict[str, Any]]:
        response = self.client.get(f"{self.base}/{table}", params=params)
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, list):
            raise RuntimeError(f"Unexpected Data API response for {table}")
        return payload

    def patch(self, table: str, filters: dict[str, str], payload: dict[str, Any]) -> None:
        response = self.client.patch(
            f"{self.base}/{table}",
            params=filters,
            json=payload,
            headers={"prefer": "return=minimal"},
        )
        response.raise_for_status()

    def upsert(self, table: str, rows: list[dict[str, Any]] | dict[str, Any], conflict: str) -> None:
        payload = rows if isinstance(rows, list) else [rows]
        if not payload:
            return
        response = self.client.post(
            f"{self.base}/{table}",
            params={"on_conflict": conflict},
            json=payload,
            headers={"prefer": "resolution=merge-duplicates,return=minimal"},
        )
        response.raise_for_status()


class AlpacaOptions:
    def __init__(self, env: Env) -> None:
        self.client = httpx.Client(
            timeout=httpx.Timeout(60.0),
            headers={
                "APCA-API-KEY-ID": env.alpaca_key,
                "APCA-API-SECRET-KEY": env.alpaca_secret,
                "accept": "application/json",
            },
        )

    def contracts(self, underlying: str, expiration_gte: date, expiration_lte: date) -> list[dict[str, Any]]:
        found: dict[str, dict[str, Any]] = {}
        for status in ("inactive", "active"):
            token: str | None = None
            while True:
                params: dict[str, Any] = {
                    "underlying_symbols": underlying,
                    "expiration_date_gte": expiration_gte.isoformat(),
                    "expiration_date_lte": expiration_lte.isoformat(),
                    "status": status,
                    "limit": 10000,
                }
                if token:
                    params["page_token"] = token
                response = self.client.get(
                    "https://paper-api.alpaca.markets/v2/options/contracts",
                    params=params,
                )
                response.raise_for_status()
                payload = response.json()
                for item in payload.get("option_contracts") or []:
                    symbol = str(item.get("symbol") or "")
                    if symbol:
                        found[symbol] = item
                token = payload.get("next_page_token") or payload.get("page_token")
                if not token:
                    break
        return list(found.values())

    def bars(self, symbols: list[str], start: datetime, end: datetime) -> dict[str, list[dict[str, Any]]]:
        result: dict[str, list[dict[str, Any]]] = {symbol: [] for symbol in symbols}
        token: str | None = None
        while True:
            params: dict[str, Any] = {
                "symbols": ",".join(symbols),
                "timeframe": "1Min",
                "start": _iso(start),
                "end": _iso(end),
                "limit": 10000,
                "sort": "asc",
            }
            if token:
                params["page_token"] = token
            response = self.client.get(
                "https://data.alpaca.markets/v1beta1/options/bars",
                params=params,
            )
            response.raise_for_status()
            payload = response.json()
            raw = payload.get("bars") or {}
            if isinstance(raw, dict):
                for symbol, rows in raw.items():
                    if isinstance(rows, list):
                        result.setdefault(symbol, []).extend(rows)
            elif isinstance(raw, list):
                for row in raw:
                    symbol = str(row.get("S") or row.get("symbol") or "")
                    if symbol:
                        result.setdefault(symbol, []).append(row)
            token = payload.get("next_page_token")
            if not token:
                break
        return result


def _choose_pair(
    contracts: list[dict[str, Any]], event_date: date, min_dte: int, max_dte: int, underlying_open: float
) -> dict[str, Any] | None:
    paired: dict[tuple[date, float], dict[str, dict[str, Any]]] = {}
    for item in contracts:
        try:
            expiration = date.fromisoformat(str(item["expiration_date"]))
            strike = float(item["strike_price"])
        except (KeyError, TypeError, ValueError):
            continue
        dte = (expiration - event_date).days
        option_type = str(item.get("type") or "").lower()
        if not (min_dte <= dte <= max_dte) or option_type not in {"call", "put"}:
            continue
        paired.setdefault((expiration, strike), {})[option_type] = item

    choices: list[tuple[date, float, float, dict[str, dict[str, Any]]]] = []
    for (expiration, strike), legs in paired.items():
        if "call" in legs and "put" in legs:
            choices.append((expiration, abs(strike - underlying_open), strike, legs))
    if not choices:
        return None
    expiration, _distance, strike, legs = min(choices, key=lambda item: (item[0], item[1], item[2]))
    return {
        "expiration_date": expiration,
        "strike": strike,
        "call": legs["call"],
        "put": legs["put"],
    }


def _parse_bar(symbol: str, raw: dict[str, Any]) -> dict[str, Any] | None:
    raw_ts = raw.get("t") or raw.get("timestamp")
    if not raw_ts:
        return None
    return {
        "contract_symbol": symbol,
        "ts": _iso(_utc(str(raw_ts))),
        "open": _as_float(raw.get("o")),
        "high": _as_float(raw.get("h")),
        "low": _as_float(raw.get("l")),
        "close": _as_float(raw.get("c")),
        "volume": _as_float(raw.get("v")),
        "trade_count": _as_int(raw.get("n")),
        "vwap": _as_float(raw.get("vw")),
    }


class OptionConvexityHttpWorker:
    def __init__(self, stop: threading.Event | None = None) -> None:
        self.env = Env.load()
        self.rest = Rest(self.env)
        self.alpaca = AlpacaOptions(self.env)
        self.stop = stop or threading.Event()
        self.worker_id = f"option-http:{os.getpid()}"
        self.completed_this_process = 0

    def _runs(self) -> dict[str, dict[str, Any]]:
        rows = self.rest.get(
            "option_vol_research_runs",
            {
                "select": "id,name,status,underlying_symbol,dte_buckets,execution_spec",
                "name": f"like.{_RUN_PREFIX}*",
                "order": "created_at.asc",
            },
        )
        return {str(row["id"]): row for row in rows if str(row.get("name") or "").startswith(_RUN_PREFIX)}

    def _next_event(self, runs: dict[str, dict[str, Any]]) -> dict[str, Any] | None:
        if not runs:
            return None
        rows = self.rest.get(
            "option_vol_research_events",
            {
                "select": "*",
                "status": "in.(queued,retry_wait)",
                "order": "entry_ts.asc,id.asc",
                "limit": 500,
            },
        )
        now = datetime.now(timezone.utc)
        for row in rows:
            if str(row.get("run_id")) not in runs:
                continue
            not_before = row.get("not_before")
            if not_before and _utc(str(not_before)) > now:
                continue
            return row
        return None

    def _claim(self, event: dict[str, Any]) -> dict[str, Any]:
        attempts = int(event.get("attempts") or 0) + 1
        now = _iso(datetime.now(timezone.utc))
        self.rest.patch(
            "option_vol_research_events",
            {"id": f"eq.{event['id']}"},
            {
                "status": "running",
                "attempts": attempts,
                "locked_by": self.worker_id,
                "locked_at": now,
                "last_error": None,
                "updated_at": now,
            },
        )
        claimed = dict(event)
        claimed["attempts"] = attempts
        claimed["status"] = "running"
        return claimed

    def _refresh_run(self, run_id: str) -> None:
        rows = self.rest.get(
            "option_vol_research_events",
            {"select": "status", "run_id": f"eq.{run_id}", "limit": 1000},
        )
        completed = sum(1 for row in rows if row.get("status") == "completed")
        failed = sum(1 for row in rows if row.get("status") in _TERMINAL_BAD)
        pending = sum(1 for row in rows if row.get("status") in {"queued", "retry_wait", "running"})
        now = _iso(datetime.now(timezone.utc))
        payload: dict[str, Any] = {
            "status": "running" if pending else ("completed_with_errors" if failed else "completed"),
            "stage": "event_backfill" if pending else "analysis_ready",
            "events_completed": completed,
            "events_failed": failed,
            "updated_at": now,
        }
        current = self.rest.get(
            "option_vol_research_runs",
            {"select": "started_at,completed_at", "id": f"eq.{run_id}", "limit": 1},
        )
        if current and not current[0].get("started_at"):
            payload["started_at"] = now
        if not pending and current and not current[0].get("completed_at"):
            payload["completed_at"] = now
        self.rest.patch("option_vol_research_runs", {"id": f"eq.{run_id}"}, payload)

    def _finish(self, event: dict[str, Any], status: str, error: str | None = None) -> None:
        now = _iso(datetime.now(timezone.utc))
        self.rest.patch(
            "option_vol_research_events",
            {"id": f"eq.{event['id']}"},
            {
                "status": status,
                "locked_by": None,
                "locked_at": None,
                "last_error": error[:2000] if error else None,
                "updated_at": now,
            },
        )
        self._refresh_run(str(event["run_id"]))

    def _retry(self, event: dict[str, Any], exc: Exception) -> None:
        attempts = int(event.get("attempts") or 0)
        max_attempts = int(event.get("max_attempts") or 5)
        terminal = attempts >= max_attempts
        now = datetime.now(timezone.utc)
        delay = min(900, 30 * (2 ** max(0, attempts - 1)))
        payload = {
            "status": "failed" if terminal else "retry_wait",
            "not_before": _iso(now if terminal else now + timedelta(seconds=delay)),
            "locked_by": None,
            "locked_at": None,
            "last_error": f"{type(exc).__name__}: {exc}"[:2000],
            "updated_at": _iso(now),
        }
        self.rest.patch("option_vol_research_events", {"id": f"eq.{event['id']}"}, payload)
        self._refresh_run(str(event["run_id"]))

    def _store_bucket(
        self,
        event: dict[str, Any],
        bucket: str,
        pair: dict[str, Any] | None,
        bars_by_symbol: dict[str, list[dict[str, Any]]] | None,
        reason: str | None = None,
    ) -> bool:
        if pair is None:
            self.rest.upsert(
                "option_vol_research_results",
                {
                    "event_id": event["id"],
                    "dte_bucket": bucket,
                    "complete": False,
                    "notes": {"reason": reason or "no_contract_pair", "transport": "supabase_data_api"},
                    "updated_at": _iso(datetime.now(timezone.utc)),
                },
                "event_id,dte_bucket",
            )
            return False

        call, put = pair["call"], pair["put"]
        call_symbol, put_symbol = str(call["symbol"]), str(put["symbol"])
        self.rest.upsert(
            "option_vol_research_contracts",
            {
                "event_id": event["id"],
                "dte_bucket": bucket,
                "expiration_date": pair["expiration_date"].isoformat(),
                "strike": pair["strike"],
                "call_symbol": call_symbol,
                "put_symbol": put_symbol,
                "call_open_interest": _as_int(call.get("open_interest")),
                "put_open_interest": _as_int(put.get("open_interest")),
                "metadata": {
                    "selection": "earliest_expiry_nearest_ATM_same_strike_lower_strike_tiebreak",
                    "transport": "alpaca_option_bars_plus_supabase_data_api",
                    "call": call,
                    "put": put,
                },
            },
            "event_id,dte_bucket",
        )

        rows: list[dict[str, Any]] = []
        for symbol in (call_symbol, put_symbol):
            for raw in (bars_by_symbol or {}).get(symbol, []):
                parsed = _parse_bar(symbol, raw)
                if parsed:
                    rows.append({"event_id": event["id"], **parsed})
        if rows:
            for offset in range(0, len(rows), 500):
                self.rest.upsert(
                    "option_vol_research_bars",
                    rows[offset : offset + 500],
                    "event_id,contract_symbol,ts",
                )

        entry_ts = _iso(_utc(event["entry_ts"]))
        exit_ts = _iso(_utc(event["exit_ts"]))
        idx = {(row["contract_symbol"], row["ts"]): row for row in rows}
        ce = idx.get((call_symbol, entry_ts))
        pe = idx.get((put_symbol, entry_ts))
        cx = idx.get((call_symbol, exit_ts))
        px = idx.get((put_symbol, exit_ts))
        entry_call = ce.get("open") if ce else None
        entry_put = pe.get("open") if pe else None
        exit_call = cx.get("close") if cx else None
        exit_put = px.get("close") if px else None
        entry_straddle = (
            float(entry_call) + float(entry_put)
            if entry_call is not None and entry_put is not None
            else None
        )
        exit_straddle = (
            float(exit_call) + float(exit_put)
            if exit_call is not None and exit_put is not None
            else None
        )
        gross = (
            exit_straddle / entry_straddle - 1.0
            if entry_straddle not in (None, 0) and exit_straddle is not None
            else None
        )
        underlying_open = _as_float(event.get("spy_open"))
        premium_to_underlying = (
            entry_straddle / underlying_open
            if entry_straddle is not None and underlying_open not in (None, 0)
            else None
        )
        complete = all(v is not None for v in (entry_call, entry_put, exit_call, exit_put))
        missing = [
            name
            for name, value in (
                ("entry_call", entry_call),
                ("entry_put", entry_put),
                ("exit_call", exit_call),
                ("exit_put", exit_put),
            )
            if value is None
        ]
        self.rest.upsert(
            "option_vol_research_results",
            {
                "event_id": event["id"],
                "dte_bucket": bucket,
                "entry_call": entry_call,
                "entry_put": entry_put,
                "entry_straddle": entry_straddle,
                "exit_call": exit_call,
                "exit_put": exit_put,
                "exit_straddle": exit_straddle,
                "gross_return": gross,
                # Legacy column name retained for schema compatibility. For this
                # lane the denominator is the selected single-stock underlying.
                "premium_to_spy": premium_to_underlying,
                "complete": complete,
                "notes": {
                    "missing": missing,
                    "exact_entry_ts": entry_ts,
                    "exact_exit_ts": exit_ts,
                    "bar_proxy_only": True,
                    "no_stale_fills": True,
                    "transport": "supabase_data_api",
                    "premium_denominator": "selected_single_stock_underlying",
                },
                "updated_at": _iso(datetime.now(timezone.utc)),
            },
            "event_id,dte_bucket",
        )
        return complete

    def process(self, event: dict[str, Any], run: dict[str, Any]) -> None:
        entry_ts = _utc(event["entry_ts"])
        exit_ts = _utc(event["exit_ts"])
        event_date = entry_ts.date()
        underlying_open = _as_float(event.get("spy_open"))
        if underlying_open in (None, 0):
            self._finish(event, "failed", "missing underlying entry open for contract selection")
            return
        buckets = dict(run.get("dte_buckets") or {})
        complete_count = 0
        for bucket, spec in buckets.items():
            min_dte, max_dte = int(spec["min"]), int(spec["max"])
            contracts = self.alpaca.contracts(
                str(run.get("underlying_symbol") or ""),
                event_date + timedelta(days=min_dte),
                event_date + timedelta(days=max_dte),
            )
            pair = _choose_pair(contracts, event_date, min_dte, max_dte, float(underlying_open))
            if pair is None:
                self._store_bucket(
                    event,
                    bucket,
                    None,
                    None,
                    "no_same_strike_call_put_pair_in_frozen_dte_bucket",
                )
                continue
            symbols = [str(pair["call"]["symbol"]), str(pair["put"]["symbol"])]
            bars = self.alpaca.bars(symbols, entry_ts, exit_ts)
            if self._store_bucket(event, bucket, pair, bars):
                complete_count += 1
        self._finish(
            event,
            "completed",
            None if complete_count else "no frozen DTE bucket had exact entry+exit option bars",
        )
        self.completed_this_process += 1
        logger.info(
            "PID5 option HTTP event completed id=%s underlying=%s complete_buckets=%s/%s total=%s",
            event["id"],
            run.get("underlying_symbol"),
            complete_count,
            len(buckets),
            self.completed_this_process,
        )

    def run(self) -> None:
        logger.info("PID5 option convexity HTTP worker started; Postgres transport is not used")
        while not self.stop.is_set():
            try:
                runs = self._runs()
                event = self._next_event(runs)
                if event is None:
                    self.stop.wait(self.env.poll_seconds)
                    continue
                run = runs.get(str(event["run_id"]))
                if run is None:
                    self.stop.wait(self.env.poll_seconds)
                    continue
                claimed = self._claim(event)
                try:
                    self.process(claimed, run)
                except Exception as exc:
                    logger.exception("PID5 option HTTP event failed id=%s", claimed.get("id"))
                    try:
                        self._retry(claimed, exc)
                    except Exception:
                        logger.exception("Failed to persist PID5 option retry state id=%s", claimed.get("id"))
                        self.stop.wait(min(30.0, self.env.poll_seconds))
            except Exception:
                logger.exception("PID5 option HTTP worker loop failed; retrying without changing research state")
                self.stop.wait(min(30.0, max(5.0, self.env.poll_seconds)))


def start_background() -> threading.Event:
    stop = threading.Event()
    thread = threading.Thread(
        target=lambda: OptionConvexityHttpWorker(stop).run(),
        name="pid5-option-convexity-http",
        daemon=True,
    )
    thread.start()
    return stop


def enabled() -> bool:
    return os.getenv("OPTION_CONVEXITY_HTTP_ENABLED", "").strip().lower() in _TRUTHY
