from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Iterable

from app.config import get_settings
from app.exceptions import EmptyData, ProviderError
from app.http import JsonHttpClient
from app.providers.base import BaseProvider, Page, as_float, as_utc
from app.remediation_sources_ohlc_v1 import parse_records, build_requests


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
        task = {"source_type":"twelvedata_candles", "symbol":symbol, "interval_seconds":60,
                "start_ts":as_utc(partition["start_ts"]).isoformat(), "end_ts":as_utc(partition["end_ts"]).isoformat()}
        try:
            build_requests(task)  # Validate bounded UTC interval before spending a source credit.
        except (ValueError, TypeError, KeyError) as exc:
            raise ProviderError(str(exc), retryable=False, code="source_request_contract") from exc
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
            if code == "429" or "rate limit" in lowered:
                now = datetime.now(timezone.utc)
                retry_at = (now + timedelta(days=1)).replace(hour=0, minute=1, second=0, microsecond=0)
                raise ProviderError(message, retryable=True, retry_at=retry_at, code="rate_limit")
            # Access, entitlement and invalid-symbol responses do not establish
            # a genuine historical absence and must not complete/skip a partition.
            raise ProviderError(message, retryable=False, code="source_access_denied" if code in {"401","403"} else code or "provider_error")

        try:
            records, validation = parse_records(task, payload)
        except (ValueError, TypeError, KeyError) as exc:
            raise ProviderError(str(exc), retryable=False, code="source_payload_contract") from exc
        if not validation["normalization_passed"]:
            raise ProviderError(
                "Twelve Data source failed normalization: "
                f"{validation['invalid_count']} invalid rows and {validation['duplicate_conflict_count']} conflicting duplicates; no synthetic correction applied",
                retryable=False, code="source_data_invalid")
        if not records:
            raise EmptyData(f"No Twelve Data values for {symbol} in this partition")
        rows = []
        for record in records:
            value = record["values"]
            rows.append(
                {
                    "provider": self.name,
                    "instrument_id": partition["instrument_id"],
                    "ts": as_utc(record["observed_at"]),
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
        yield Page(rows=rows, cursor={"finished": True, "source_validation": validation}, done=True)

