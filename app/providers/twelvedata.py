from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Iterable

from app.config import get_settings
from app.exceptions import EmptyData, ProviderError
from app.http import JsonHttpClient
from app.providers.base import BaseProvider, Page, as_float, as_utc


class TwelveDataProvider(BaseProvider):
    name = "twelvedata"

    def __init__(self):
        self.settings = get_settings()
        self.http = JsonHttpClient(self.settings.twelvedata_requests_per_minute)

    def catalogue(self) -> list[dict[str, Any]]:
        # Twelve Data mappings are deliberately created from the primary-provider
        # catalogue so the free quota is spent on data, not catalogue lookups.
        return []

    def iter_bar_pages(self, partition: dict[str, Any]) -> Iterable[Page]:
        symbol = partition["provider_symbol"]
        cursor = dict(partition.get("cursor") or {})
        if cursor.get("finished"):
            yield Page(rows=[], cursor=cursor, done=True)
            return
        payload = self.http.get(
            "https://api.twelvedata.com/time_series",
            params={
                "symbol": symbol,
                "interval": "1min",
                "start_date": partition["start_ts"].strftime("%Y-%m-%d %H:%M:%S"),
                "end_date": (partition["end_ts"] - timedelta(seconds=1)).strftime("%Y-%m-%d %H:%M:%S"),
                "timezone": "UTC",
                "order": "ASC",
                "outputsize": 5000,
                "apikey": self.settings.twelvedata_api_key,
            },
            allow_error_json=True,
        )
        if isinstance(payload, dict) and payload.get("status") == "error":
            code = str(payload.get("code") or "")
            message = str(payload.get("message") or "Twelve Data error")
            lowered = message.lower()
            if code == "429" or "credit" in lowered or "rate limit" in lowered:
                now = datetime.now(timezone.utc)
                retry_at = (now + timedelta(days=1)).replace(hour=0, minute=1, second=0, microsecond=0)
                raise ProviderError(message, retryable=True, retry_at=retry_at, code="rate_limit")
            if "no data" in lowered or "not available" in lowered or "symbol" in lowered:
                raise EmptyData(message)
            raise ProviderError(message, retryable=False, code=code or "provider_error")

        values = payload.get("values") if isinstance(payload, dict) else None
        if not values:
            raise EmptyData(f"No Twelve Data values for {symbol} in this partition")
        rows = []
        for value in values:
            rows.append(
                {
                    "provider": self.name,
                    "instrument_id": partition["instrument_id"],
                    "ts": as_utc(value["datetime"]),
                    "open": as_float(value.get("open")),
                    "high": as_float(value.get("high")),
                    "low": as_float(value.get("low")),
                    "close": as_float(value.get("close")),
                    "volume": as_float(value.get("volume")),
                    "quote_volume": None,
                    "trade_count": None,
                    "vwap": None,
                    "taker_buy_base_volume": None,
                    "taker_buy_quote_volume": None,
                    "source_feed": "twelvedata_basic",
                }
            )
        rows.sort(key=lambda row: row["ts"])
        yield Page(rows=rows, cursor={"finished": True}, done=True)
