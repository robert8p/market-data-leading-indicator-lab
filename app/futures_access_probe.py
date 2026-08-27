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


def run_massive_futures_access_probe() -> None:
    """Probe Massive futures reference + minute aggregates without ever logging credentials."""
    api_key = os.getenv("MASSIVE_API_KEY", "").strip()
    if not api_key:
        logger.warning("INDEX_FUTURES_ACCESS_PROBE provider=massive key_present=false")
        return

    try:
        status, payload = _get(
            "/futures/v1/contracts",
            {"product_code": "ES", "date": "2026-08-26", "active": "true", "limit": 5},
            api_key,
        )
        rows = _results(payload)
        first = rows[0] if rows else {}
        ticker = str(first.get("ticker") or first.get("symbol") or "")
        logger.warning(
            "INDEX_FUTURES_ACCESS_PROBE provider=massive stage=contracts http=%s api_status=%s rows=%s "
            "ticker=%s product_code=%s first_trade_date=%s last_trade_date=%s settlement_date=%s message=%s",
            status,
            payload.get("status"),
            len(rows),
            ticker or None,
            first.get("product_code"),
            first.get("first_trade_date"),
            first.get("last_trade_date"),
            first.get("settlement_date"),
            _message(payload),
        )
        if status >= 400 or not ticker:
            return

        agg_status, agg_payload = _get(
            f"/futures/v1/aggs/{ticker}",
            {"resolution": "1min", "limit": 5, "sort": "window_start.desc"},
            api_key,
        )
        agg_rows = _results(agg_payload)
        first_bar = agg_rows[0] if agg_rows else {}
        logger.warning(
            "INDEX_FUTURES_ACCESS_PROBE provider=massive stage=aggs http=%s api_status=%s rows=%s "
            "ticker=%s window_start=%s session_end_date=%s has_volume=%s has_transactions=%s message=%s",
            agg_status,
            agg_payload.get("status"),
            len(agg_rows),
            ticker,
            first_bar.get("window_start"),
            first_bar.get("session_end_date"),
            first_bar.get("volume") is not None,
            first_bar.get("transactions") is not None,
            _message(agg_payload),
        )
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
