from __future__ import annotations

import hashlib
import json
import logging
import os
import socket
import threading
import time
import uuid
from datetime import UTC, date, datetime, time as dt_time, timedelta
from typing import Any, Iterable
from urllib.parse import quote
from zoneinfo import ZoneInfo

from app.db import fetch_one
from app.providers.alpaca import AlpacaProvider


logger = logging.getLogger(__name__)
ET = ZoneInfo("America/New_York")
_TRUTHY = {"1", "true", "yes", "on"}
_BC_SYMBOLS = ("SPY", "QQQ", "IWM", "GLD", "TLT")
_BC_THRESHOLD = 0.00606789668829825
_XAL_THRESHOLD = -0.00386614774767422
_STOP_PCT = 4.5
_stop_event = threading.Event()
_thread: threading.Thread | None = None


def _truthy(name: str, default: str = "false") -> bool:
    return os.getenv(name, default).strip().lower() in _TRUTHY


def _parse_ts(value: str | datetime) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _et_at(day: date, hour: int, minute: int) -> datetime:
    return datetime.combine(day, dt_time(hour, minute), tzinfo=ET)


def _hash_payload(payload: Any) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def opening_return(open_price: float, prior_close: float) -> float:
    if open_price <= 0 or prior_close <= 0:
        raise ValueError("prices must be positive")
    return open_price / prior_close - 1.0


def first_30m_return(open_0930: float, close_0959: float) -> float:
    if open_0930 <= 0 or close_0959 <= 0:
        raise ValueError("prices must be positive")
    return close_0959 / open_0930 - 1.0


def eqmdef_f30(values: dict[str, float]) -> float:
    missing = [symbol for symbol in _BC_SYMBOLS if symbol not in values]
    if missing:
        raise ValueError(f"missing state legs: {','.join(missing)}")
    return (values["SPY"] + values["QQQ"] + values["IWM"]) / 3.0 - (
        values["GLD"] + values["TLT"]
    ) / 2.0


def _valid_quote(item: dict[str, Any]) -> bool:
    try:
        bid = float(item["bp"])
        ask = float(item["ap"])
    except (KeyError, TypeError, ValueError):
        return False
    return bid > 0 and ask >= bid


def _quote_tuple(item: dict[str, Any]) -> tuple[datetime, float, float]:
    if not _valid_quote(item):
        raise ValueError("invalid quote")
    return _parse_ts(item["t"]), float(item["bp"]), float(item["ap"])


class Phase3ForwardMonitor:
    """Write-only evidence collector for the frozen Phase 3 BC-LI and XAL lanes.

    The process receives no direct SELECT access to either sealed outcome table.
    It may read only same-day operational state through bounded SECURITY DEFINER
    functions, and those functions never return historical or aggregate returns.
    """

    def __init__(self) -> None:
        self.enabled = _truthy("PHASE3_FORWARD_MONITOR_ENABLED")
        self.poll_seconds = max(15.0, float(os.getenv("PHASE3_FORWARD_POLL_SECONDS", "30")))
        self.lease_ttl_seconds = max(60, min(600, int(os.getenv("PHASE3_FORWARD_LEASE_TTL_SECONDS", "180"))))
        self.service_id = os.getenv("RENDER_SERVICE_ID", "srv-d9jm320u01pc73fhinbg")
        self.deployment_id = os.getenv("RENDER_DEPLOY_ID", os.getenv("RENDER_GIT_COMMIT", "unknown"))
        self.instance_id = os.getenv("RENDER_INSTANCE_ID", f"{socket.gethostname()}:{os.getpid()}")
        self.owner_id = uuid.uuid5(
            uuid.NAMESPACE_URL,
            f"phase3-forward|{self.service_id}|{self.deployment_id}|{self.instance_id}",
        )
        self.api = AlpacaProvider()
        self._last_catchup_day: date | None = None
        self._last_phase = "STARTING"

    def _call(self, function_name: str, params: tuple[Any, ...]) -> dict[str, Any]:
        placeholders = ",".join(["%s"] * len(params))
        row = fetch_one(f"select research_v3.{function_name}({placeholders}) as result", params)
        value = (row or {}).get("result")
        return value if isinstance(value, dict) else {"result": value}

    def _lease(self) -> bool:
        result = self._call(
            "acquire_forward_collector_lease",
            (
                self.owner_id,
                self.service_id,
                self.deployment_id,
                self.instance_id,
                self.lease_ttl_seconds,
            ),
        )
        return bool(result.get("acquired"))

    def _heartbeat(self, phase: str, details: dict[str, Any] | None = None, error: str | None = None) -> None:
        self._last_phase = phase
        self._call(
            "record_forward_collector_heartbeat",
            (
                self.owner_id,
                self.service_id,
                self.deployment_id,
                self.instance_id,
                phase,
                details or {},
                error,
            ),
        )

    def _calendar(self, start: date, end: date) -> list[dict[str, Any]]:
        payload = self.api.http.get(
            "https://paper-api.alpaca.markets/v2/calendar",
            params={"start": start.isoformat(), "end": end.isoformat()},
        )
        return list(payload or [])

    def _is_market_day(self, day: date) -> bool:
        return any(str(item.get("date")) == day.isoformat() for item in self._calendar(day, day))

    def _previous_market_day(self, day: date) -> date:
        items = self._calendar(day - timedelta(days=10), day - timedelta(days=1))
        dates = sorted(date.fromisoformat(str(item["date"])) for item in items if item.get("date"))
        if not dates:
            raise RuntimeError(f"no previous market day found before {day}")
        return dates[-1]

    def _bars(self, symbol: str, start: datetime, end: datetime) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        token: str | None = None
        while True:
            params: dict[str, Any] = {
                "timeframe": "1Min",
                "start": start.astimezone(UTC).isoformat(),
                "end": end.astimezone(UTC).isoformat(),
                "limit": 10000,
                "adjustment": "split",
                "feed": "sip",
                "sort": "asc",
            }
            if token:
                params["page_token"] = token
            payload = self.api.http.get(
                f"https://data.alpaca.markets/v2/stocks/{quote(symbol, safe='')}/bars",
                params=params,
            )
            rows.extend(payload.get("bars") or [])
            token = payload.get("next_page_token")
            if not token:
                return rows

    def _quotes(self, symbol: str, start: datetime, end: datetime) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        token: str | None = None
        while True:
            params: dict[str, Any] = {
                "start": start.astimezone(UTC).isoformat(),
                "end": end.astimezone(UTC).isoformat(),
                "limit": 10000,
                "feed": "sip",
                "sort": "asc",
            }
            if token:
                params["page_token"] = token
            payload = self.api.http.get(
                f"https://data.alpaca.markets/v2/stocks/{quote(symbol, safe='')}/quotes",
                params=params,
            )
            rows.extend(item for item in (payload.get("quotes") or []) if _valid_quote(item))
            token = payload.get("next_page_token")
            if not token:
                return rows

    def _first_quote(self, symbol: str, start: datetime, seconds: int = 180) -> tuple[datetime, float, float] | None:
        quotes = self._quotes(symbol, start, start + timedelta(seconds=seconds))
        return _quote_tuple(quotes[0]) if quotes else None

    def _latest_quote(self, symbol: str) -> tuple[datetime, float, float] | None:
        payload = self.api.http.get(
            "https://data.alpaca.markets/v2/stocks/quotes/latest",
            params={"symbols": symbol, "feed": "sip"},
        )
        item = (payload.get("quotes") or {}).get(symbol)
        return _quote_tuple(item) if item and _valid_quote(item) else None

    def _bar_at(self, symbol: str, day: date, hour: int, minute: int) -> dict[str, Any] | None:
        start = _et_at(day, hour, minute)
        rows = self._bars(symbol, start, start + timedelta(minutes=2))
        for row in rows:
            local = _parse_ts(row["t"]).astimezone(ET)
            if local.date() == day and local.hour == hour and local.minute == minute:
                return row
        return None

    def _f30_state(self, day: date) -> tuple[dict[str, float], str]:
        values: dict[str, float] = {}
        evidence: dict[str, Any] = {"trade_date": day.isoformat(), "symbols": {}}
        for symbol in _BC_SYMBOLS:
            rows = self._bars(symbol, _et_at(day, 9, 30), _et_at(day, 10, 1))
            by_time = {
                (_parse_ts(row["t"]).astimezone(ET).hour, _parse_ts(row["t"]).astimezone(ET).minute): row
                for row in rows
                if _parse_ts(row["t"]).astimezone(ET).date() == day
            }
            opening = by_time.get((9, 30))
            ending = by_time.get((9, 59))
            if not opening or not ending:
                raise RuntimeError(f"incomplete {symbol} first-30-minute bars")
            values[symbol] = first_30m_return(float(opening["o"]), float(ending["c"]))
            evidence["symbols"][symbol] = {
                "open_ts": opening["t"],
                "close_ts": ending["t"],
                "open": opening["o"],
                "close": ending["c"],
            }
        evidence["eqmdef_f30"] = eqmdef_f30(values)
        return values, _hash_payload(evidence)

    def _xal_state(self, day: date) -> tuple[float, float, str]:
        previous = self._previous_market_day(day)
        current_open = self._bar_at("SPY", day, 9, 30)
        prior_close = self._bar_at("SPY", previous, 15, 59)
        if not current_open or not prior_close:
            raise RuntimeError("incomplete SPY opening-gap bars")
        open_price = float(current_open["o"])
        close_price = float(prior_close["c"])
        payload = {
            "trade_date": day.isoformat(),
            "previous_market_date": previous.isoformat(),
            "current_open": current_open,
            "prior_close": prior_close,
            "opening_gap": opening_return(open_price, close_price),
        }
        return open_price, close_price, _hash_payload(payload)

    def _bc_state(self, day: date) -> dict[str, Any]:
        return self._call("bc_li_runtime_state", (day,))

    def _xal_runtime_state(self, day: date) -> dict[str, Any]:
        return self._call("xal007_runtime_state", (day,))

    def _process_bc(self, day: date, now_et: datetime, *, historical: bool) -> None:
        state = self._bc_state(day)
        if state.get("finalized"):
            return
        decision_at = _et_at(day, 10, 0)
        if not historical and now_et < _et_at(day, 10, 1):
            return
        if not state.get("exists"):
            try:
                values, source_hash = self._f30_state(day)
            except Exception as exc:
                if historical or now_et >= _et_at(day, 10, 15):
                    self._call(
                        "bc_li_record_exclusion",
                        (day, decision_at, f"source_state_unavailable:{type(exc).__name__}", _hash_payload({"day": day, "error": str(exc)})),
                    )
                return
            self._call(
                "bc_li_record_signal",
                (
                    day,
                    decision_at,
                    values["SPY"],
                    values["QQQ"],
                    values["IWM"],
                    values["GLD"],
                    values["TLT"],
                    source_hash,
                ),
            )
            state = self._bc_state(day)
        if not state.get("triggered") or state.get("finalized"):
            return

        entry_at = _et_at(day, 10, 1)
        entry: tuple[datetime, float, float] | None = None
        if not state.get("entry_recorded"):
            entry = self._first_quote("QQQ", entry_at)
            if entry is None:
                if historical or now_et >= _et_at(day, 10, 10):
                    self._call(
                        "bc_li_finalize_exclusion",
                        (day, "entry_quote_unavailable", _hash_payload({"day": day, "phase": "entry"})),
                    )
                return
            self._call("bc_li_record_entry", (day, entry[0], entry[1], entry[2], _hash_payload(entry)))
            state = self._bc_state(day)
        if state.get("finalized"):
            return

        exit_at = _et_at(day, 15, 59)
        if historical or now_et >= exit_at:
            quotes = self._quotes("QQQ", entry_at, _et_at(day, 16, 1))
            if not quotes:
                self._call("bc_li_finalize_exclusion", (day, "intraday_quotes_unavailable", _hash_payload({"day": day, "phase": "path"})))
                return
            if entry is None:
                entry = _quote_tuple(quotes[0])
            entry_ask = entry[2]
            path = [_quote_tuple(item) for item in quotes]
            path = [item for item in path if item[0] >= entry_at.astimezone(UTC)]
            before_exit = [item for item in path if item[0] < exit_at.astimezone(UTC)]
            stop_item = next((item for item in before_exit if item[1] <= entry_ask * (1.0 - _STOP_PCT / 100.0)), None)
            considered = [item for item in before_exit if stop_item is None or item[0] <= stop_item[0]]
            if considered:
                best = max(considered, key=lambda item: item[1])
                worst = min(considered, key=lambda item: item[1])
                self._call("bc_li_record_quote", (day, best[0], best[1], best[2], _hash_payload(best)))
                if not self._bc_state(day).get("finalized"):
                    self._call("bc_li_record_quote", (day, worst[0], worst[1], worst[2], _hash_payload(worst)))
            if self._bc_state(day).get("finalized"):
                return
            exit_quote = next((item for item in path if item[0] >= exit_at.astimezone(UTC)), None)
            if exit_quote is None:
                self._call("bc_li_finalize_exclusion", (day, "exit_quote_unavailable", _hash_payload({"day": day, "phase": "exit"})))
                return
            self._call("bc_li_record_exit", (day, exit_quote[0], exit_quote[1], exit_quote[2], _hash_payload(exit_quote)))
            return

        latest = self._latest_quote("QQQ")
        if latest and latest[0].astimezone(ET).date() == day:
            self._call("bc_li_record_quote", (day, latest[0], latest[1], latest[2], _hash_payload(latest)))

    def _process_xal(self, day: date, now_et: datetime, *, historical: bool) -> None:
        state = self._xal_runtime_state(day)
        if state.get("finalized"):
            return
        signal_at = _et_at(day, 9, 30)
        if not historical and now_et < _et_at(day, 9, 31):
            return
        if not state.get("exists"):
            try:
                open_price, prior_close, source_hash = self._xal_state(day)
            except Exception as exc:
                if historical or now_et >= _et_at(day, 9, 45):
                    self._call(
                        "xal007_record_exclusion",
                        (day, signal_at, f"source_state_unavailable:{type(exc).__name__}", _hash_payload({"day": day, "error": str(exc)})),
                    )
                return
            self._call("xal007_record_signal", (day, signal_at, open_price, prior_close, source_hash))
            state = self._xal_runtime_state(day)
        if not state.get("triggered") or state.get("finalized"):
            return

        entry_at = _et_at(day, 15, 30)
        if not historical and now_et < entry_at:
            return
        if not state.get("entry_recorded"):
            entry = self._first_quote("GLD", entry_at)
            if entry is None:
                if historical or now_et >= _et_at(day, 15, 40):
                    self._call("xal007_finalize_exclusion", (day, "entry_quote_unavailable", _hash_payload({"day": day, "phase": "entry"})))
                return
            self._call("xal007_record_entry", (day, entry[0], entry[1], entry[2], _hash_payload(entry)))
            state = self._xal_runtime_state(day)
        if state.get("finalized"):
            return

        exit_at = _et_at(day, 16, 0)
        if historical or now_et >= exit_at:
            exit_quote = self._first_quote("GLD", exit_at)
            if exit_quote is None:
                self._call("xal007_finalize_exclusion", (day, "exit_quote_unavailable", _hash_payload({"day": day, "phase": "exit"})))
                return
            self._call("xal007_record_exit", (day, exit_quote[0], exit_quote[1], exit_quote[2], _hash_payload(exit_quote)))

    def _catch_up(self, today_et: date, now_et: datetime) -> dict[str, int]:
        if self._last_catchup_day == today_et:
            return {"bc_days": 0, "xal_days": 0}
        self._last_catchup_day = today_et
        bc_start = date.fromisoformat(os.getenv("PHASE3_BC_LI_CATCHUP_START", "2026-08-21"))
        xal_start = date.fromisoformat(os.getenv("PHASE3_XAL_CATCHUP_START", "2026-08-25"))
        end = today_et if now_et >= _et_at(today_et, 16, 1) else today_et - timedelta(days=1)
        if end < min(bc_start, xal_start):
            return {"bc_days": 0, "xal_days": 0}
        market_days = [date.fromisoformat(str(item["date"])) for item in self._calendar(min(bc_start, xal_start), end)]
        bc_count = 0
        xal_count = 0
        for day in market_days:
            if day >= bc_start:
                self._process_bc(day, _et_at(day, 16, 2), historical=True)
                bc_count += 1
            if day >= xal_start:
                self._process_xal(day, _et_at(day, 16, 2), historical=True)
                xal_count += 1
        return {"bc_days": bc_count, "xal_days": xal_count}

    def tick(self) -> None:
        if not self.enabled:
            return
        if not self._lease():
            logger.warning("Phase 3 forward collector lease is owned by another instance")
            return
        now_et = datetime.now(UTC).astimezone(ET)
        catchup = self._catch_up(now_et.date(), now_et)
        if not self._is_market_day(now_et.date()):
            self._heartbeat("WAITING_NON_MARKET_DAY", {"market_date_et": now_et.date().isoformat(), **catchup})
            return
        self._process_bc(now_et.date(), now_et, historical=False)
        self._process_xal(now_et.date(), now_et, historical=False)
        self._heartbeat(
            "MONITORING",
            {
                "market_date_et": now_et.date().isoformat(),
                "local_time_et": now_et.isoformat(),
                "bc_state": self._bc_state(now_et.date()),
                "xal_state": self._xal_runtime_state(now_et.date()),
                **catchup,
            },
        )


def run_phase3_forward_loop(stop: threading.Event | None = None) -> None:
    monitor = Phase3ForwardMonitor()
    if not monitor.enabled:
        logger.info("Phase 3 forward monitor disabled")
        return
    stop = stop or _stop_event
    logger.info(
        "Starting Phase 3 write-only forward monitor service=%s instance=%s poll_seconds=%s",
        monitor.service_id,
        monitor.instance_id,
        monitor.poll_seconds,
    )
    while not stop.is_set():
        try:
            monitor.tick()
        except Exception as exc:  # noqa: BLE001
            logger.exception("Phase 3 forward monitor tick failed")
            try:
                if monitor._lease():
                    monitor._heartbeat("ERROR_RETRYING", {"last_phase": monitor._last_phase}, f"{type(exc).__name__}: {exc}")
            except Exception:  # noqa: BLE001
                logger.exception("Phase 3 forward monitor could not persist its error heartbeat")
        stop.wait(monitor.poll_seconds)


def start_background() -> threading.Thread | None:
    global _thread
    if not _truthy("PHASE3_FORWARD_MONITOR_ENABLED"):
        return None
    if _thread and _thread.is_alive():
        return _thread
    _thread = threading.Thread(
        target=run_phase3_forward_loop,
        args=(_stop_event,),
        name="phase3-forward-monitor",
        daemon=True,
    )
    _thread.start()
    return _thread
