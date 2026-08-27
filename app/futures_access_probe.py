from __future__ import annotations

import json
import logging
import os
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

logger = logging.getLogger(__name__)

_BASE = "https://api.massive.com"


def _safe_json(raw: bytes) -> dict:
    try:
        parsed = json.loads(raw.decode("utf-8", errors="replace"))
        return parsed if isinstance(parsed, dict) else {"payload_type": type(parsed).__name__}
    except Exception:
        return {}


def _get(path: str, params: dict[str, object], api_key: str) -> tuple[int, dict]:
    query = urlencode({k: v for k, v in params.items() if v is not None})
    url = f"{_BASE}{path}" + (f"?{query}" if query else "")
    request = Request(
        url,
        headers={
            "Authorization": f"Bearer {api_key}",
            "Accept": "application/json",
            "User-Agent": "market-data-leading-indicator-lab/futures-entitlement-probe",
        },
        method="GET",
    )
    try:
        with urlopen(request, timeout=30) as response:
            return int(response.status), _safe_json(response.read())
    except HTTPError as exc:
        return int(exc.code), _safe_json(exc.read())


def _message(payload: dict) -> str:
    value = payload.get("message") or payload.get("error") or payload.get("status") or ""
    return str(value)[:240].replace("\n", " ")


def _results(payload: dict) -> list[dict]:
    value = payload.get("results")
    if isinstance(value, list):
        return [row for row in value if isinstance(row, dict)]
    return []


def _probe_contract(api_key: str, ticker: str, asof: str) -> None:
    status, payload = _get(
        "/futures/v1/contracts",
        {"ticker": ticker, "date": asof, "limit": 5},
        api_key,
    )
    rows = _results(payload)
    first = rows[0] if rows else {}
    logger.warning(
        "INDEX_FUTURES_ACCESS_PROBE provider=massive stage=contract ticker=%s asof=%s http=%s api_status=%s rows=%s "
        "product_code=%s venue=%s first_trade_date=%s last_trade_date=%s settlement_date=%s message=%s",
        ticker,
        asof,
        status,
        payload.get("status"),
        len(rows),
        first.get("product_code"),
        first.get("trading_venue"),
        first.get("first_trade_date"),
        first.get("last_trade_date"),
        first.get("settlement_date"),
        _message(payload),
    )


def _probe_bars(api_key: str, ticker: str, day: str, label: str) -> None:
    status, payload = _get(
        f"/futures/v1/aggs/{ticker}",
        {
            "resolution": "1min",
            "window_start.gte": day,
            "window_start.lt": _next_day(day),
            "limit": 5,
            "sort": "window_start.asc",
        },
        api_key,
    )
    rows = _results(payload)
    first = rows[0] if rows else {}
    logger.warning(
        "INDEX_FUTURES_ACCESS_PROBE provider=massive stage=aggs label=%s ticker=%s day=%s http=%s api_status=%s rows=%s "
        "window_start=%s session_end_date=%s has_ohlc=%s has_volume=%s has_transactions=%s message=%s",
        label,
        ticker,
        day,
        status,
        payload.get("status"),
        len(rows),
        first.get("window_start"),
        first.get("session_end_date"),
        all(first.get(k) is not None for k in ("open", "high", "low", "close")),
        first.get("volume") is not None,
        first.get("transactions") is not None,
        _message(payload),
    )


def _next_day(day: str) -> str:
    from datetime import date, timedelta

    return (date.fromisoformat(day) + timedelta(days=1)).isoformat()


def run_massive_futures_access_probe() -> None:
    """Probe Massive futures reference + minute aggregates without ever logging credentials."""
    api_key = os.getenv("MASSIVE_API_KEY", "").strip()
    if not api_key:
        logger.warning("INDEX_FUTURES_ACCESS_PROBE provider=massive key_present=false")
        return

    try:
        current = {
            "ESU6": "2026-08-26",
            "MESU6": "2026-08-26",
            "NQU6": "2026-08-26",
            "MNQU6": "2026-08-26",
            "YMU6": "2026-08-26",
            "MYMU6": "2026-08-26",
            "RTYU6": "2026-08-26",
            "M2KU6": "2026-08-26",
            "VXU6": "2026-08-26",
        }
        for ticker, asof in current.items():
            _probe_contract(api_key, ticker, asof)
            _probe_bars(api_key, ticker, asof, "current")

        # Historical-depth probes deliberately target known September ES contracts.
        # Successful minute bars indicate the connected plan's practical history depth.
        for ticker, day, label in (
            ("ESU4", "2024-08-27", "2y"),
            ("ESU3", "2023-08-25", "3y"),
            ("ESU2", "2022-08-26", "4y"),
            ("ESU1", "2021-08-26", "5y"),
        ):
            _probe_contract(api_key, ticker, day)
            _probe_bars(api_key, ticker, day, label)
    except (URLError, TimeoutError) as exc:
        logger.warning(
            "INDEX_FUTURES_ACCESS_PROBE provider=massive stage=network error_type=%s",
            type(exc).__name__,
        )
    except Exception as exc:
        logger.warning(
            "INDEX_FUTURES_ACCESS_PROBE provider=massive stage=unexpected error_type=%s",
            type(exc).__name__,
        )
