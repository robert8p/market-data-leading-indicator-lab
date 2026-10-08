"""Pure bounded public Coinbase candle helpers; no network or database writes."""
from __future__ import annotations

import json
import re
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from urllib.parse import urlencode, quote

from .remediation_sources_binance_v1 import _dt, _iso, _decimal

UTC = timezone.utc
VERSION = "coinbase_candle_source_rules_20261008_v1"
ADVANCED_INTERVALS = {60: "ONE_MINUTE", 300: "FIVE_MINUTE", 900: "FIFTEEN_MINUTE",
                      1800: "THIRTY_MINUTE", 3600: "ONE_HOUR", 7200: "TWO_HOUR",
                      14400: "FOUR_HOUR", 21600: "SIX_HOUR", 86400: "ONE_DAY"}
EXCHANGE_INTERVALS = {60, 300, 900, 3600, 21600, 86400}


def _settings(task):
    req = task.get("request_json") or {}
    if isinstance(req, str):
        req = json.loads(req)
    if task["source_type"] != "coinbase_candles":
        raise ValueError("Unsupported Coinbase source_type")
    api = req.get("api", "exchange")
    if api not in ("exchange", "advanced"):
        raise ValueError("Unsupported Coinbase public API")
    symbol = str(task["symbol"]).upper()
    if not re.fullmatch(r"[A-Z0-9]+-[A-Z0-9]+", symbol):
        raise ValueError("A full Coinbase spot product pair is required")
    seconds = int(task["interval_seconds"])
    if seconds not in (EXCHANGE_INTERVALS if api == "exchange" else ADVANCED_INTERVALS):
        raise ValueError("Unsupported native Coinbase granularity")
    start, end = _dt(task["start_ts"]), _dt(task["end_ts"])
    duration = (end - start).total_seconds()
    if duration <= 0 or duration / seconds > 300:
        raise ValueError("Split Coinbase requests into at most300 nominal buckets")
    if any(d.microsecond or int(d.timestamp()) % seconds for d in (start, end)):
        raise ValueError("Coinbase task boundaries must align to native bucket grid")
    return api, symbol, seconds, start, end


def build_requests(task):
    api, symbol, seconds, start, end = _settings(task)
    # Provider may return candles before start or at inclusive end; parser filters.
    request_end = end - timedelta(seconds=1)
    if api == "exchange":
        endpoint = "https://api.exchange.coinbase.com/products/" + quote(symbol, safe="") + "/candles"
        params = {"granularity": seconds, "start": _iso(start), "end": _iso(request_end)}
    else:
        endpoint = "https://api.coinbase.com/api/v3/brokerage/market/products/" + quote(symbol, safe="") + "/candles"
        params = {"granularity": ADVANCED_INTERVALS[seconds], "start": int(start.timestamp()),
                  "end": int(request_end.timestamp()), "limit": 350}
    return [{"url": endpoint + "?" + urlencode(params), "role": "candles"}]


def parse_records(task, raw_json):
    api, symbol, seconds, start, end = _settings(task)
    payload = json.loads(raw_json) if isinstance(raw_json, (bytes, bytearray, str)) else raw_json
    source_rows = payload if api == "exchange" else payload.get("candles") if isinstance(payload, dict) else None
    if not isinstance(source_rows, list):
        raise ValueError("Coinbase did not return a candle array")
    validation = {"parser_version": VERSION, "raw_count": 0, "valid_count": 0,
                  "invalid_count": 0, "duplicate_count": 0, "outside_count": 0,
                  "duplicate_conflict_count": 0, "errors": [], "strict_replay_certified": False,
                  "nominal_grid_slots": int((end - start).total_seconds() // seconds)}
    records_by_key = {}
    for i, raw in enumerate(source_rows):
        validation["raw_count"] += 1
        try:
            if api == "exchange":
                if not isinstance(raw, list) or len(raw) < 6:
                    raise ValueError("Exchange candle requires timestamp,low,high,open,close,volume")
                stamp, low, high, opn, close, volume = raw[:6]
            else:
                if not isinstance(raw, dict):
                    raise ValueError("Advanced candle must be an object")
                stamp, low, high, opn, close, volume = [raw[k] for k in ("start", "low", "high", "open", "close", "volume")]
            n = _decimal(stamp, "bucket_start", nonnegative=True)
            if n != n.to_integral_value():
                raise ValueError("Nonintegral Coinbase epoch seconds")
            label = datetime(1970, 1, 1, tzinfo=UTC) + timedelta(seconds=int(n))
            if int(n) % seconds:
                raise ValueError("Coinbase candle off interval grid")
            bar_end = label + timedelta(seconds=seconds)
            o, h, l, c = [_decimal(v, name, positive=True) for v, name in zip([opn, high, low, close], ["open", "high", "low", "close"])]
            v = _decimal(volume, "volume", nonnegative=True)
            if h < max(o, l, c) or l > min(o, h, c):
                raise ValueError("Invalid source OHLC; no synthetic correction applied")
            if not start <= label < end or bar_end > end:
                validation["outside_count"] += 1
                continue
            values = dict(zip(["open", "high", "low", "close", "volume"], map(str, [o, h, l, c, v])))
            values.update({"quote_volume": None, "trade_count": None, "vwap": None,
                           "taker_buy_base_volume": None, "taker_buy_quote_volume": None,
                           "provider_symbol": symbol, "base_asset": symbol.split("-")[0],
                           "quote_asset": symbol.split("-")[1], "_source_api": api,
                           "_source_row_number": i, "_source_label_ts": _iso(label),
                           "_source_timestamp_unit": "s", "_parser_version": VERSION,
                           "_not_before": _iso(bar_end),
                           "_not_before_basis": "BAR_COMPLETION_LOWER_BOUND_NOT_PUBLICATION",
                           "_historical_publication_at": None, "_historical_first_receipt_at": None,
                           "_original_version_history_complete": False,
                           "_strict_historical_replay_eligible": False,
                           "_unit_basis": "QUOTE_CURRENCY_PRICES_BASE_ASSET_VOLUME"})
            key = f"coinbase|spot|{symbol}|{seconds}|{_iso(label)}"
            record = {"record_key": key, "observed_at": _iso(label), "bar_end": _iso(bar_end), "values": values}
            if key in records_by_key:
                validation["duplicate_count"] += 1
                old = {k: v for k, v in records_by_key[key]["values"].items() if k != "_source_row_number"}
                new = {k: v for k, v in values.items() if k != "_source_row_number"}
                if old != new:
                    validation["duplicate_conflict_count"] += 1
                    if len(validation["errors"]) < 20:
                        validation["errors"].append({"row": i, "reason": "conflicting_duplicate", "record_key": key})
                continue
            records_by_key[key] = record
        except (ValueError, TypeError, KeyError, OverflowError) as exc:
            validation["invalid_count"] += 1
            if len(validation["errors"]) < 20:
                validation["errors"].append({"row": i, "reason": str(exc)})
    records = sorted(records_by_key.values(), key=lambda r: r["record_key"])
    validation["valid_count"] = len(records)
    validation["absent_nominal_slots"] = validation["nominal_grid_slots"] - len(records)
    validation["normalization_passed"] = validation["invalid_count"] == 0 and validation["duplicate_conflict_count"] == 0
    validation["coverage_assertion"] = "SOURCE_ROWS_ONLY_ABSENT_BUCKETS_REQUIRE_NO_TRADE_OR_COVERAGE_ADJUDICATION"
    return records, validation
