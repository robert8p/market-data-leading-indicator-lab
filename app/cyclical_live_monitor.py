from __future__ import annotations

import logging
import os
import threading
from datetime import datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

from psycopg.types.json import Jsonb

from app.config import get_settings
from app.db import db_connection, fetch_all, fetch_one
from app.exceptions import EmptyData
from app.providers.alpaca import AlpacaProvider


logger = logging.getLogger(__name__)
NY = ZoneInfo("America/New_York")
IMPLEMENTATION_ID = "LI-CYCLICAL-INTEGRATED-ENERGY-ANY-01"
TARGET_SYMBOL = "QQQ"
CONFIRM_SYMBOL = "XLB"
THRESHOLDS: dict[str, float] = {
    "VDE": 0.00424107860034517,
    "IYE": 0.00409204162257359,
    "FENY": 0.00443190975020147,
}
SYMBOLS = (*THRESHOLDS.keys(), CONFIRM_SYMBOL, TARGET_SYMBOL)
HOLD_MINUTES = 60
POLL_SECONDS = max(15, int(os.getenv("CYCLICAL_LIVE_MONITOR_POLL_SECONDS", "60")))
ENABLED = os.getenv("CYCLICAL_LIVE_MONITOR_ENABLED", "true").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}


def _floor_minute(value: datetime) -> datetime:
    return value.astimezone(timezone.utc).replace(second=0, microsecond=0)


def _in_monitor_window(now_utc: datetime) -> bool:
    local = now_utc.astimezone(NY)
    if local.weekday() >= 5:
        return False
    minute = local.hour * 60 + local.minute
    # 10:30-15:00 ET is the frozen signal window. Keep the poller alive for a
    # few minutes after 15:00 so the final 15:00 bar can be evaluated once it
    # is complete.
    return 630 <= minute <= 905


def _upsert_bars(rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    sql = """
        insert into market_bars_1m(
            provider,instrument_id,ts,open,high,low,close,volume,quote_volume,
            trade_count,vwap,taker_buy_base_volume,taker_buy_quote_volume,source_feed
        ) values (
            %(provider)s,%(instrument_id)s,%(ts)s,%(open)s,%(high)s,%(low)s,%(close)s,
            %(volume)s,%(quote_volume)s,%(trade_count)s,%(vwap)s,%(taker_buy_base_volume)s,
            %(taker_buy_quote_volume)s,%(source_feed)s
        )
        on conflict(provider,instrument_id,ts) do update set
            open=excluded.open,high=excluded.high,low=excluded.low,close=excluded.close,
            volume=excluded.volume,quote_volume=excluded.quote_volume,
            trade_count=excluded.trade_count,vwap=excluded.vwap,
            taker_buy_base_volume=excluded.taker_buy_base_volume,
            taker_buy_quote_volume=excluded.taker_buy_quote_volume,
            source_feed=excluded.source_feed
    """
    with db_connection() as conn, conn.cursor() as cur:
        cur.executemany(sql, rows)
        conn.commit()


def _fetch_recent_bars(provider: AlpacaProvider, now_utc: datetime) -> dict[str, dict[datetime, dict[str, Any]]]:
    instruments = fetch_all(
        """
        select id,provider_symbol
          from instruments
         where provider='alpaca' and provider_symbol = any(%s)
        """,
        (list(SYMBOLS),),
    )
    by_symbol = {row["provider_symbol"]: row for row in instruments}
    missing = [symbol for symbol in SYMBOLS if symbol not in by_symbol]
    if missing:
        raise RuntimeError(f"Missing Alpaca instruments for live monitor: {missing}")

    # Alpaca's historical bars endpoint returns completed minute bars. Asking
    # through the current minute and retaining only bars strictly before the
    # floor-minute avoids using an in-progress candle.
    end_ts = _floor_minute(now_utc)
    start_ts = end_ts - timedelta(minutes=35)
    result: dict[str, dict[datetime, dict[str, Any]]] = {}
    all_rows: list[dict[str, Any]] = []
    for symbol in SYMBOLS:
        instrument = by_symbol[symbol]
        partition = {
            "provider_symbol": symbol,
            "instrument_id": instrument["id"],
            "start_ts": start_ts,
            "end_ts": end_ts,
            "cursor": {"feed": provider.settings.alpaca_feed},
        }
        rows: list[dict[str, Any]] = []
        try:
            for page in provider.iter_bar_pages(partition):
                rows.extend(row for row in page.rows if row["ts"] < end_ts)
                if page.done:
                    break
        except EmptyData:
            rows = []
        result[symbol] = {row["ts"]: row for row in rows}
        all_rows.extend(rows)
    _upsert_bars(all_rows)
    return result


def _r15(rows: dict[str, dict[datetime, dict[str, Any]]], symbol: str, ts: datetime) -> float | None:
    current = rows.get(symbol, {}).get(ts)
    prior = rows.get(symbol, {}).get(ts - timedelta(minutes=15))
    if not current or not prior:
        return None
    current_close = float(current.get("close") or 0.0)
    prior_close = float(prior.get("close") or 0.0)
    if current_close <= 0 or prior_close <= 0:
        return None
    return current_close / prior_close - 1.0


def _condition_at(
    rows: dict[str, dict[datetime, dict[str, Any]]], ts: datetime
) -> tuple[bool, dict[str, Any]]:
    local = ts.astimezone(NY)
    minute = local.hour * 60 + local.minute
    if local.weekday() >= 5 or minute < 630 or minute > 900:
        return False, {}

    xlb_r15 = _r15(rows, CONFIRM_SYMBOL, ts)
    if xlb_r15 is None:
        return False, {}

    energy_returns = {symbol: _r15(rows, symbol, ts) for symbol in THRESHOLDS}
    triggered = [
        symbol
        for symbol, threshold in THRESHOLDS.items()
        if energy_returns[symbol] is not None and energy_returns[symbol] >= threshold
    ]
    condition = xlb_r15 > 0 and bool(triggered)
    return condition, {
        "xlb_r15": xlb_r15,
        "energy_r15": energy_returns,
        "triggered_predictors": triggered,
    }


def _latest_evaluable_ts(rows: dict[str, dict[datetime, dict[str, Any]]]) -> datetime | None:
    xlb_times = set(rows.get(CONFIRM_SYMBOL, {}))
    energy_times: set[datetime] = set()
    for symbol in THRESHOLDS:
        energy_times.update(rows.get(symbol, {}))
    candidates = sorted(xlb_times & energy_times, reverse=True)
    for ts in candidates:
        local = ts.astimezone(NY)
        minute = local.hour * 60 + local.minute
        if local.weekday() < 5 and 630 <= minute <= 900:
            # XLB needs an exact 15-minute base and at least one energy ETF must
            # have an exact 15-minute return at this timestamp.
            if _r15(rows, CONFIRM_SYMBOL, ts) is None:
                continue
            if any(_r15(rows, symbol, ts) is not None for symbol in THRESHOLDS):
                return ts
    return None


def _existing_alert(signal_ts: datetime) -> bool:
    row = fetch_one(
        """
        select id
          from application_events
         where event_type='live_signal_alert'
           and details->>'implementation_id'=%s
           and details->>'signal_ts'=%s
         limit 1
        """,
        (IMPLEMENTATION_ID, signal_ts.isoformat()),
    )
    return bool(row)


def _overlap_blocked(signal_ts: datetime) -> bool:
    row = fetch_one(
        """
        select details->>'signal_ts' as signal_ts
          from application_events
         where event_type='live_signal_alert'
           and details->>'implementation_id'=%s
         order by created_at desc
         limit 1
        """,
        (IMPLEMENTATION_ID,),
    )
    if not row or not row.get("signal_ts"):
        return False
    try:
        previous = datetime.fromisoformat(row["signal_ts"])
    except (TypeError, ValueError):
        return False
    return signal_ts < previous + timedelta(minutes=HOLD_MINUTES)


def _insert_alert(signal_ts: datetime, details: dict[str, Any], rows: dict[str, dict[datetime, dict[str, Any]]]) -> None:
    qqq_bar = rows.get(TARGET_SYMBOL, {}).get(signal_ts)
    qqq_close = float(qqq_bar.get("close")) if qqq_bar and qqq_bar.get("close") is not None else None
    payload = {
        "implementation_id": IMPLEMENTATION_ID,
        "signal_ts": signal_ts.isoformat(),
        "signal_time_ny": signal_ts.astimezone(NY).isoformat(),
        "target": TARGET_SYMBOL,
        "entry": "next available minute after alert; research rule uses next-minute open",
        "expected_hold_minutes": HOLD_MINUTES,
        "session": "10:30-15:00 America/New_York",
        "confirmation": "XLB 15m return > 0",
        "thresholds": THRESHOLDS,
        "triggered_predictors": details["triggered_predictors"],
        "energy_r15": details["energy_r15"],
        "xlb_r15": details["xlb_r15"],
        "qqq_close_at_signal": qqq_close,
        "mode": "MONITOR_ONLY_NO_AUTO_TRADING",
        "research_note": "Frozen integrated-energy cyclical leadership implementation; 2024-2026 engineering robustness tested.",
    }
    message = (
        "LIVE CYCLICAL SIGNAL: "
        + "/".join(details["triggered_predictors"])
        + " extreme 15m impulse with XLB positive confirmation -> monitor long QQQ for 60m"
    )
    with db_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            insert into application_events(level,event_type,message,details)
            values ('info','live_signal_alert',%s,%s)
            """,
            (message, Jsonb(payload)),
        )
        conn.commit()
    logger.warning("%s signal_ts=%s", message, signal_ts.isoformat())


def scan_cyclical_live_signal(provider: AlpacaProvider | None = None, now_utc: datetime | None = None) -> bool:
    if not ENABLED:
        return False
    now_utc = now_utc or datetime.now(timezone.utc)
    if not _in_monitor_window(now_utc):
        return False
    provider = provider or AlpacaProvider()
    rows = _fetch_recent_bars(provider, now_utc)
    signal_ts = _latest_evaluable_ts(rows)
    if signal_ts is None:
        return False
    current, details = _condition_at(rows, signal_ts)
    if not current:
        return False
    previous_ts = signal_ts - timedelta(minutes=1)
    previous, _ = _condition_at(rows, previous_ts)
    if previous:
        return False
    if _existing_alert(signal_ts) or _overlap_blocked(signal_ts):
        return False
    _insert_alert(signal_ts, details, rows)
    return True


def run_cyclical_monitor_loop(stop_event: threading.Event) -> None:
    if not ENABLED:
        logger.info("Cyclical live monitor disabled")
        return
    provider = AlpacaProvider()
    logger.info(
        "Cyclical live monitor started implementation=%s poll_seconds=%s",
        IMPLEMENTATION_ID,
        POLL_SECONDS,
    )
    while not stop_event.is_set():
        try:
            scan_cyclical_live_signal(provider=provider)
        except Exception:
            # Monitoring must never terminate the collection/research worker.
            logger.exception("Cyclical live monitor iteration failed; it will retry")
        stop_event.wait(POLL_SECONDS)
    logger.info("Cyclical live monitor stopping")
