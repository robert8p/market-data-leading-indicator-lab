from __future__ import annotations

import json
import logging
import os
import time
import uuid
from datetime import date, datetime, time as dt_time, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

from app.db import execute, fetch_all, fetch_one
from app.providers.alpaca import AlpacaProvider
from app.providers.base import as_utc
from app.providers.binance import BinanceProvider


logger = logging.getLogger(__name__)
UTC = timezone.utc
NY = ZoneInfo("America/New_York")
CANDIDATE_ID = "XAL-006"
IWM_SYMBOL = "IWM"
DOGS_SYMBOL = "DOGSUSDT"
FROZEN_THRESHOLD = -0.0036892077395876
LEAD_MINUTES = 15
HOLD_MINUTES = 90
EXIT_FROM_SOURCE_MINUTES = LEAD_MINUTES + HOLD_MINUTES
RESEARCH_COST_20 = 0.002
RESEARCH_COST_30 = 0.003
CAPACITY_NOTIONALS = (25.0, 50.0, 100.0, 250.0, 500.0)
PROSPECTIVE_TOLERANCE_SECONDS = 90


def _enabled() -> bool:
    return os.getenv("XAL006_LIVE_ENABLED", "true").strip().lower() in {"1", "true", "yes", "on"}


def _safe_float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _spread_bps(bid: float | None, ask: float | None) -> float | None:
    if bid is None or ask is None or bid <= 0 or ask <= 0:
        return None
    mid = (bid + ask) / 2.0
    return ((ask - bid) / mid) * 10_000.0 if mid > 0 else None


def _book_levels(raw: Any) -> list[tuple[float, float]]:
    levels: list[tuple[float, float]] = []
    for level in raw or []:
        if not isinstance(level, (list, tuple)) or len(level) < 2:
            continue
        price = _safe_float(level[0])
        qty = _safe_float(level[1])
        if price and qty and price > 0 and qty > 0:
            levels.append((price, qty))
    return levels


def _buy_capacity(asks: list[tuple[float, float]], notionals: tuple[float, ...]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for notional in notionals:
        remaining_quote = notional
        quote_spent = 0.0
        qty_bought = 0.0
        for price, qty in asks:
            level_quote = price * qty
            take_quote = min(remaining_quote, level_quote)
            if take_quote <= 0:
                continue
            take_qty = take_quote / price
            quote_spent += take_quote
            qty_bought += take_qty
            remaining_quote -= take_quote
            if remaining_quote <= 1e-9:
                break
        key = str(int(notional))
        depth_ok = remaining_quote <= 1e-9
        result[key] = {
            "notional": notional,
            "depth_ok": depth_ok,
            "unfilled_quote": max(0.0, remaining_quote),
            "qty": qty_bought if depth_ok else None,
            "vwap": (quote_spent / qty_bought) if depth_ok and qty_bought > 0 else None,
        }
    return result


def _sell_proceeds(bids: list[tuple[float, float]], qty: float) -> tuple[float | None, float | None]:
    remaining_qty = qty
    proceeds = 0.0
    sold = 0.0
    for price, level_qty in bids:
        take_qty = min(remaining_qty, level_qty)
        if take_qty <= 0:
            continue
        proceeds += price * take_qty
        sold += take_qty
        remaining_qty -= take_qty
        if remaining_qty <= 1e-12:
            break
    if remaining_qty > 1e-12 or sold <= 0:
        return None, None
    return proceeds, proceeds / sold


def _capacity_returns(entry_capacity: dict[str, Any], exit_bids: list[tuple[float, float]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, item in (entry_capacity or {}).items():
        qty = _safe_float((item or {}).get("qty"))
        notional = _safe_float((item or {}).get("notional"))
        if qty is None or notional is None or qty <= 0 or notional <= 0:
            result[str(key)] = {"depth_ok": False, "return": None}
            continue
        proceeds, sell_vwap = _sell_proceeds(exit_bids, qty)
        if proceeds is None:
            result[str(key)] = {"depth_ok": False, "return": None}
            continue
        result[str(key)] = {
            "depth_ok": True,
            "entry_notional": notional,
            "qty": qty,
            "exit_proceeds": proceeds,
            "exit_vwap": sell_vwap,
            "return": proceeds / notional - 1.0,
        }
    return result


class XAL006LiveMonitor:
    """Prospective evidence recorder for the frozen XAL-006 rule.

    It never places orders or changes the candidate definition. Source timing mirrors
    research.xal_prepare_equity_source_events exactly: each 15-minute return uses the
    open at T-15m and close at T-1m, and the LOW-state lag resets at the start of each
    New York trading day, making 09:45 eligible without consulting pre-market state.
    """

    def __init__(self, worker_id: str):
        self.worker_id = worker_id
        self.enabled = _enabled()
        self.alpaca = AlpacaProvider() if self.enabled else None
        self.binance = BinanceProvider() if self.enabled else None
        self._last_tick = 0.0
        self._schema_ready = False
        self._last_schema_check = 0.0

    def _check_schema(self) -> bool:
        now = time.monotonic()
        if self._schema_ready:
            return True
        if now - self._last_schema_check < 30:
            return False
        self._last_schema_check = now
        row = fetch_one(
            "select to_regclass('research.xal_live_signals') as signals, "
            "to_regclass('research.xal_live_source_evaluations') as evaluations"
        )
        self._schema_ready = bool(row and row.get("signals") and row.get("evaluations"))
        return self._schema_ready

    @staticmethod
    def _latest_source_boundary(now_utc: datetime) -> datetime | None:
        ny_now = now_utc.astimezone(NY)
        if ny_now.weekday() >= 5:
            return None
        session_start = datetime.combine(ny_now.date(), dt_time(9, 45), tzinfo=NY)
        session_end = datetime.combine(ny_now.date(), dt_time(16, 0), tzinfo=NY)
        eligible_now = ny_now - timedelta(seconds=5)
        if eligible_now < session_start:
            return None
        if eligible_now > session_end:
            eligible_now = session_end
        minute_of_day = eligible_now.hour * 60 + eligible_now.minute
        floored = minute_of_day - (minute_of_day % 15)
        boundary = eligible_now.replace(
            hour=floored // 60,
            minute=floored % 60,
            second=0,
            microsecond=0,
        )
        if boundary < session_start:
            return None
        return boundary.astimezone(UTC)

    @staticmethod
    def _boundaries_for_day(trade_date: date, latest_utc: datetime) -> list[datetime]:
        start = datetime.combine(trade_date, dt_time(9, 45), tzinfo=NY)
        end = datetime.combine(trade_date, dt_time(16, 0), tzinfo=NY)
        latest_ny = min(latest_utc.astimezone(NY), end)
        boundaries: list[datetime] = []
        cursor = start
        while cursor <= latest_ny:
            boundaries.append(cursor.astimezone(UTC))
            cursor += timedelta(minutes=15)
        return boundaries

    @staticmethod
    def _is_first_session_boundary(boundary_utc: datetime) -> bool:
        local = boundary_utc.astimezone(NY)
        return local.time().replace(tzinfo=None) == dt_time(9, 45)

    def _fetch_iwm_returns(self, boundary_utc: datetime) -> tuple[float, float | None, dict[str, Any]]:
        """Return frozen source value and prior eligible state using exact bar endpoints."""
        assert self.alpaca is not None
        first_session_boundary = self._is_first_session_boundary(boundary_utc)
        current_start = boundary_utc - timedelta(minutes=15)
        current_end = boundary_utc - timedelta(minutes=1)
        previous_start = boundary_utc - timedelta(minutes=30)
        previous_end = boundary_utc - timedelta(minutes=16)
        request_start = current_start if first_session_boundary else previous_start

        payload = self.alpaca.http.get(
            f"https://data.alpaca.markets/v2/stocks/{IWM_SYMBOL}/bars",
            params={
                "timeframe": "1Min",
                "start": request_start.isoformat(),
                "end": boundary_utc.isoformat(),
                "limit": 100,
                "adjustment": "split",
                "feed": self.alpaca.settings.alpaca_feed,
                "sort": "asc",
            },
        )
        bars: dict[datetime, tuple[float | None, float | None]] = {}
        for bar in payload.get("bars") or []:
            ts = as_utc(bar["t"])
            if request_start <= ts < boundary_utc:
                bars[ts] = (_safe_float(bar.get("o")), _safe_float(bar.get("c")))

        current_open = (bars.get(current_start) or (None, None))[0]
        current_close = (bars.get(current_end) or (None, None))[1]
        if current_open is None or current_open <= 0 or current_close is None:
            raise RuntimeError(
                f"Missing exact IWM source endpoints at {boundary_utc.isoformat()}: "
                f"open@{current_start.isoformat()}={current_open} close@{current_end.isoformat()}={current_close}"
            )
        current_ret = current_close / current_open - 1.0

        previous_ret: float | None = None
        previous_open: float | None = None
        previous_close: float | None = None
        if not first_session_boundary:
            previous_open = (bars.get(previous_start) or (None, None))[0]
            previous_close = (bars.get(previous_end) or (None, None))[1]
            if previous_open is None or previous_open <= 0 or previous_close is None:
                raise RuntimeError(
                    f"Missing exact prior IWM state endpoints at {boundary_utc.isoformat()}: "
                    f"open@{previous_start.isoformat()}={previous_open} close@{previous_end.isoformat()}={previous_close}"
                )
            previous_ret = previous_close / previous_open - 1.0

        audit = {
            "source_semantics": "frozen_xal_prepare_equity_source_events",
            "current_open_ts": current_start.isoformat(),
            "current_close_ts": current_end.isoformat(),
            "current_open": current_open,
            "current_close": current_close,
            "previous_open_ts": None if first_session_boundary else previous_start.isoformat(),
            "previous_close_ts": None if first_session_boundary else previous_end.isoformat(),
            "previous_open": previous_open,
            "previous_close": previous_close,
            "session_state_reset": first_session_boundary,
            "previous_ok_default_false": first_session_boundary,
            "bars_returned": len(bars),
            "feed": self.alpaca.settings.alpaca_feed,
        }
        return current_ret, previous_ret, audit

    def _record_source(self, boundary_utc: datetime, now_utc: datetime) -> bool:
        trade_date = boundary_utc.astimezone(NY).date()
        existing = fetch_one(
            "select event_triggered from research.xal_live_source_evaluations "
            "where candidate_id=%s and source_bar_end_ts=%s",
            (CANDIDATE_ID, boundary_utc),
        )
        if existing:
            return False

        current_ret, previous_ret, audit = self._fetch_iwm_returns(boundary_utc)
        previous_ok = previous_ret is not None and previous_ret <= FROZEN_THRESHOLD
        tail_entry = current_ret <= FROZEN_THRESHOLD and not previous_ok
        prior_event = fetch_one(
            "select source_bar_end_ts from research.xal_live_source_evaluations "
            "where candidate_id=%s and trade_date=%s and event_triggered=true "
            "order by source_bar_end_ts limit 1",
            (CANDIDATE_ID, trade_date),
        )
        event_triggered = bool(tail_entry and not prior_event)
        delay_seconds = max(0.0, (now_utc - boundary_utc).total_seconds())
        prospective = delay_seconds <= PROSPECTIVE_TOLERANCE_SECONDS
        source_payload = {
            **audit,
            "tail_entry": tail_entry,
            "previous_ok": previous_ok,
            "first_daily_tail_entry": event_triggered,
            "capture_delay_seconds": delay_seconds,
            "prospective": prospective,
            "frozen_threshold": FROZEN_THRESHOLD,
        }
        execute(
            "insert into research.xal_live_source_evaluations "
            "(candidate_id,source_bar_end_ts,trade_date,source_return,previous_source_return,threshold_value,event_triggered,source_payload) "
            "values (%s,%s,%s,%s,%s,%s,%s,%s::jsonb) on conflict do nothing",
            (
                CANDIDATE_ID,
                boundary_utc,
                trade_date,
                current_ret,
                previous_ret,
                FROZEN_THRESHOLD,
                event_triggered,
                json.dumps(source_payload),
            ),
        )

        if event_triggered:
            signal_id = uuid.uuid4()
            entry_ts = boundary_utc + timedelta(minutes=LEAD_MINUTES)
            exit_ts = boundary_utc + timedelta(minutes=EXIT_FROM_SOURCE_MINUTES)
            status = "TRIGGERED" if prospective else "MISSED_SOURCE_CAPTURE"
            execute(
                "insert into research.xal_live_signals "
                "(signal_id,candidate_id,trade_date,source_bar_end_ts,source_return,previous_source_return,threshold_value,scheduled_entry_ts,scheduled_exit_ts,status,data_quality) "
                "values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb) "
                "on conflict (candidate_id,trade_date) do nothing",
                (
                    signal_id,
                    CANDIDATE_ID,
                    trade_date,
                    boundary_utc,
                    current_ret,
                    previous_ret,
                    FROZEN_THRESHOLD,
                    entry_ts,
                    exit_ts,
                    status,
                    json.dumps(
                        {
                            "source_prospective": prospective,
                            "source_capture_delay_seconds": delay_seconds,
                            "source_semantics": "frozen_xal_prepare_equity_source_events",
                            "session_state_reset": audit["session_state_reset"],
                        }
                    ),
                ),
            )
            logger.warning(
                "XAL-006 source event trade_date=%s source_ts=%s return=%.6f prospective=%s",
                trade_date,
                boundary_utc.isoformat(),
                current_ret,
                prospective,
            )
        return True

    def _binance_book(self) -> dict[str, Any]:
        assert self.binance is not None
        ticker = self.binance.http.get(
            "https://api.binance.com/api/v3/ticker/bookTicker",
            params={"symbol": DOGS_SYMBOL},
        )
        depth = self.binance.http.get(
            "https://api.binance.com/api/v3/depth",
            params={"symbol": DOGS_SYMBOL, "limit": 100},
        )
        bid = _safe_float(ticker.get("bidPrice"))
        ask = _safe_float(ticker.get("askPrice"))
        bids = _book_levels(depth.get("bids"))
        asks = _book_levels(depth.get("asks"))
        return {
            "bid": bid,
            "ask": ask,
            "mid": ((bid + ask) / 2.0) if bid is not None and ask is not None else None,
            "spread_bps": _spread_bps(bid, ask),
            "bids": bids,
            "asks": asks,
            "last_update_id": depth.get("lastUpdateId"),
        }

    def _prior_minute_close(self, ts_utc: datetime) -> float | None:
        assert self.binance is not None
        start = ts_utc - timedelta(minutes=1)
        payload = self.binance.http.get(
            "https://api.binance.com/api/v3/klines",
            params={
                "symbol": DOGS_SYMBOL,
                "interval": "1m",
                "startTime": int(start.timestamp() * 1000),
                "endTime": int(ts_utc.timestamp() * 1000) - 1,
                "limit": 1,
            },
        )
        if not payload:
            return None
        return _safe_float(payload[-1][4])

    def _capture_entry(self, signal: dict[str, Any], now_utc: datetime) -> bool:
        scheduled = signal["scheduled_entry_ts"]
        if now_utc < scheduled:
            return False
        delay = (now_utc - scheduled).total_seconds()
        if delay > PROSPECTIVE_TOLERANCE_SECONDS:
            execute(
                "update research.xal_live_signals set status='MISSED_ENTRY_CAPTURE', error=%s, attempts=attempts+1, updated_at=now() where signal_id=%s",
                (f"entry capture delay {delay:.1f}s exceeded prospective tolerance", signal["signal_id"]),
            )
            return True

        book = self._binance_book()
        passive_limit = self._prior_minute_close(scheduled)
        capacity = _buy_capacity(book["asks"], CAPACITY_NOTIONALS)
        depth_json = {
            "last_update_id": book["last_update_id"],
            "bids": book["bids"],
            "asks": book["asks"],
        }
        quality = dict(signal.get("data_quality") or {})
        quality.update(
            {
                "entry_prospective": True,
                "entry_capture_delay_seconds": delay,
                "passive_fill_model": "touch_proxy_no_queue_position",
                "auto_trade": False,
            }
        )
        execute(
            "update research.xal_live_signals set status='ENTRY_CAPTURED', entry_captured_at=%s, entry_bid=%s, entry_ask=%s, entry_mid=%s, entry_spread_bps=%s, "
            "entry_depth=%s::jsonb, entry_capacity=%s::jsonb, passive_limit_price=%s, data_quality=%s::jsonb, attempts=attempts+1, error=null, updated_at=now() "
            "where signal_id=%s",
            (
                now_utc,
                book["bid"],
                book["ask"],
                book["mid"],
                book["spread_bps"],
                json.dumps(depth_json),
                json.dumps(capacity),
                passive_limit,
                json.dumps(quality),
                signal["signal_id"],
            ),
        )
        logger.warning("XAL-006 prospective entry captured signal=%s delay=%.1fs", signal["signal_id"], delay)
        return True

    def _evaluate_passive_fill(self, signal: dict[str, Any], now_utc: datetime) -> bool:
        if signal.get("passive_evaluated_at") is not None:
            return False
        entry_ts = signal["scheduled_entry_ts"]
        if now_utc < entry_ts + timedelta(minutes=5):
            return False
        limit_price = _safe_float(signal.get("passive_limit_price"))
        if limit_price is None:
            execute(
                "update research.xal_live_signals set passive_evaluated_at=now(), passive_filled=false, updated_at=now() where signal_id=%s",
                (signal["signal_id"],),
            )
            return True

        assert self.binance is not None
        end_ts = entry_ts + timedelta(minutes=5)
        payload = self.binance.http.get(
            "https://api.binance.com/api/v3/klines",
            params={
                "symbol": DOGS_SYMBOL,
                "interval": "1m",
                "startTime": int(entry_ts.timestamp() * 1000),
                "endTime": int(end_ts.timestamp() * 1000) - 1,
                "limit": 5,
            },
        )
        fill_ts = None
        for candle in payload or []:
            low = _safe_float(candle[3])
            if low is not None and low <= limit_price:
                fill_ts = datetime.fromtimestamp(int(candle[0]) / 1000, tz=UTC)
                break
        execute(
            "update research.xal_live_signals set passive_evaluated_at=now(), passive_filled=%s, passive_fill_ts=%s, updated_at=now() where signal_id=%s",
            (fill_ts is not None, fill_ts, signal["signal_id"]),
        )
        return True

    def _capture_exit(self, signal: dict[str, Any], now_utc: datetime) -> bool:
        scheduled = signal["scheduled_exit_ts"]
        if now_utc < scheduled:
            return False
        delay = (now_utc - scheduled).total_seconds()
        if delay > PROSPECTIVE_TOLERANCE_SECONDS:
            execute(
                "update research.xal_live_signals set status='MISSED_EXIT_CAPTURE', error=%s, attempts=attempts+1, updated_at=now() where signal_id=%s",
                (f"exit capture delay {delay:.1f}s exceeded prospective tolerance", signal["signal_id"]),
            )
            return True

        book = self._binance_book()
        entry_ask = _safe_float(signal.get("entry_ask"))
        entry_mid = _safe_float(signal.get("entry_mid"))
        quote_cross = (book["bid"] / entry_ask - 1.0) if entry_ask and book["bid"] else None
        mid_return = (book["mid"] / entry_mid - 1.0) if entry_mid and book["mid"] else None
        net20 = mid_return - RESEARCH_COST_20 if mid_return is not None else None
        net30 = mid_return - RESEARCH_COST_30 if mid_return is not None else None
        passive_limit = _safe_float(signal.get("passive_limit_price"))
        passive_cross = None
        if signal.get("passive_filled") and passive_limit and book["bid"]:
            passive_cross = book["bid"] / passive_limit - 1.0
        capacity_returns = _capacity_returns(signal.get("entry_capacity") or {}, book["bids"])
        depth_json = {
            "last_update_id": book["last_update_id"],
            "bids": book["bids"],
            "asks": book["asks"],
        }
        quality = dict(signal.get("data_quality") or {})
        quality.update({"exit_prospective": True, "exit_capture_delay_seconds": delay})
        execute(
            "update research.xal_live_signals set status='COMPLETE', exit_captured_at=%s, exit_bid=%s, exit_ask=%s, exit_mid=%s, exit_spread_bps=%s, "
            "exit_depth=%s::jsonb, exit_capacity=%s::jsonb, quote_cross_return=%s, mid_return=%s, research_net_20bps=%s, research_net_30bps=%s, "
            "passive_quote_cross_return=%s, capacity_returns=%s::jsonb, data_quality=%s::jsonb, attempts=attempts+1, error=null, updated_at=now() where signal_id=%s",
            (
                now_utc,
                book["bid"],
                book["ask"],
                book["mid"],
                book["spread_bps"],
                json.dumps(depth_json),
                json.dumps({"bids": book["bids"][:20], "asks": book["asks"][:20]}),
                quote_cross,
                mid_return,
                net20,
                net30,
                passive_cross,
                json.dumps(capacity_returns),
                json.dumps(quality),
                signal["signal_id"],
            ),
        )
        logger.warning(
            "XAL-006 prospective exit captured signal=%s quote_cross=%s net20=%s",
            signal["signal_id"],
            quote_cross,
            net20,
        )
        return True

    def _process_pending_signals(self, now_utc: datetime) -> bool:
        rows = fetch_all(
            "select * from research.xal_live_signals where candidate_id=%s and status in ('TRIGGERED','ENTRY_CAPTURED') order by source_bar_end_ts",
            (CANDIDATE_ID,),
        )
        did_work = False
        for row in rows:
            status = row.get("status")
            if status == "TRIGGERED":
                if self._capture_entry(row, now_utc):
                    did_work = True
                row = fetch_one(
                    "select * from research.xal_live_signals where signal_id=%s",
                    (row["signal_id"],),
                ) or row
                status = row.get("status")
            if status == "ENTRY_CAPTURED":
                if self._evaluate_passive_fill(row, now_utc):
                    did_work = True
                    row = fetch_one(
                        "select * from research.xal_live_signals where signal_id=%s",
                        (row["signal_id"],),
                    ) or row
                if self._capture_exit(row, now_utc):
                    did_work = True
        return did_work

    def _heartbeat(
        self,
        now_utc: datetime,
        *,
        status: str,
        last_boundary: datetime | None = None,
        error: str | None = None,
    ) -> None:
        execute(
            "insert into research.xal_live_monitor_state(candidate_id,enabled,worker_id,monitor_status,last_checked_at,last_source_boundary,last_success_at,last_error_at,last_error,metadata,updated_at) "
            "values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,now()) "
            "on conflict (candidate_id) do update set enabled=excluded.enabled, worker_id=excluded.worker_id, monitor_status=excluded.monitor_status, "
            "last_checked_at=excluded.last_checked_at, last_source_boundary=coalesce(excluded.last_source_boundary,research.xal_live_monitor_state.last_source_boundary), "
            "last_success_at=case when excluded.last_error is null then excluded.last_success_at else research.xal_live_monitor_state.last_success_at end, "
            "last_error_at=case when excluded.last_error is not null then excluded.last_error_at else research.xal_live_monitor_state.last_error_at end, "
            "last_error=excluded.last_error, metadata=research.xal_live_monitor_state.metadata || excluded.metadata, updated_at=now()",
            (
                CANDIDATE_ID,
                self.enabled,
                self.worker_id,
                status,
                now_utc,
                last_boundary,
                now_utc if error is None else None,
                now_utc if error is not None else None,
                error,
                json.dumps(
                    {
                        "mode": "evidence_only_no_autotrade",
                        "prospective_tolerance_seconds": PROSPECTIVE_TOLERANCE_SECONDS,
                        "source_semantics": "frozen_xal_prepare_equity_source_events",
                        "session_state_reset": "09:45_America/New_York",
                    }
                ),
            ),
        )

    def tick(self) -> bool:
        if not self.enabled:
            return False
        now_mono = time.monotonic()
        if now_mono - self._last_tick < 5:
            return False
        self._last_tick = now_mono
        if not self._check_schema():
            return False

        now_utc = datetime.now(UTC)
        did_work = False
        last_boundary = None
        try:
            if self._process_pending_signals(now_utc):
                did_work = True
            latest = self._latest_source_boundary(now_utc)
            if latest is not None:
                last_boundary = latest
                trade_date = latest.astimezone(NY).date()
                existing_rows = fetch_all(
                    "select source_bar_end_ts from research.xal_live_source_evaluations where candidate_id=%s and trade_date=%s",
                    (CANDIDATE_ID, trade_date),
                )
                existing = {row["source_bar_end_ts"] for row in existing_rows}
                for boundary in self._boundaries_for_day(trade_date, latest):
                    if boundary in existing:
                        continue
                    if self._record_source(boundary, now_utc):
                        did_work = True
            self._heartbeat(now_utc, status="RUNNING", last_boundary=last_boundary)
        except Exception as exc:
            logger.exception("XAL-006 live monitor tick failed; next poll will retry")
            try:
                self._heartbeat(
                    now_utc,
                    status="ERROR_RETRYING",
                    last_boundary=last_boundary,
                    error=f"{type(exc).__name__}: {exc}",
                )
            except Exception:
                logger.exception("XAL-006 live monitor heartbeat update also failed")
        return did_work
