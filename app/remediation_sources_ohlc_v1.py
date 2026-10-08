"""Pure source rules for bounded Twelve Data and Massive replacement requests.

Credentials and HTTP are owned by the isolated worker. No candle repair, filling,
price adjustment or claim of historical publication is performed here.
"""
from __future__ import annotations

import json
import re
from datetime import datetime, timedelta, timezone
from urllib.parse import urlencode, quote

from .remediation_sources_binance_v1 import _dt, _iso, _decimal

VERSION = "ohlc_source_rules_20261008_v4_explicit_crypto_source"
UTC = timezone.utc


def _settings(task):
    kind = task["source_type"]
    if kind not in ("twelvedata_candles", "massive_candles"):
        raise ValueError("Unsupported OHLC source type")
    # Massive native symbols are case-sensitive: BCpC, TpC, MRPw and NVRIw
    # identify different trading instruments from their uppercase spellings.
    # Twelve Data's existing complete-pair normalization remains provider-scoped.
    symbol = str(task["symbol"])
    if kind == "twelvedata_candles":
        symbol = symbol.upper()
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9./:_-]{0,99}", symbol):
        raise ValueError("Invalid full provider symbol")
    seconds = int(task["interval_seconds"])
    start, end = _dt(task["start_ts"]), _dt(task["end_ts"])
    if not start < end or seconds < 60 or seconds % 60:
        raise ValueError("Invalid OHLC task interval")
    if any(d.microsecond or int(d.timestamp()) % seconds for d in (start, end)):
        raise ValueError("Task must align to requested minute grid")
    if kind == "twelvedata_candles" and (seconds != 60 or end-start > timedelta(days=3)):
        raise ValueError("Twelve Data uses bounded one-minute requests of at most three days")
    if kind == "massive_candles" and end-start > timedelta(days=31):
        raise ValueError("Massive request exceeds one bounded month")
    return kind, symbol, seconds, start, end


def build_requests(task):
    kind, symbol, seconds, start, end = _settings(task)
    if kind == "twelvedata_candles":
        params = {"symbol": symbol, "interval": "1min", "start_date": start.strftime("%Y-%m-%d %H:%M:%S"),
                  "end_date": (end-timedelta(seconds=1)).strftime("%Y-%m-%d %H:%M:%S"),
                  "timezone": "UTC", "order": "ASC", "outputsize": 5000}
        request = task.get("request_json") or {}
        exchange = request.get("expected_exchange")
        if exchange is not None:
            if not isinstance(exchange,str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9 ._-]{0,79}",exchange):
                raise ValueError("Exact provider exchange identity required")
            params["exchange"] = exchange
        if request.get("asset_class") == "crypto" and not exchange:
            raise ValueError("Crypto requests require evidenced explicit exchange selection")
        url = "https://api.twelvedata.com/time_series?" + urlencode(params)
    else:
        url = ("https://api.massive.com/v2/aggs/ticker/" + quote(symbol, safe="") + "/range/" + str(seconds//60)
               + "/minute/" + str(int(start.timestamp()*1000)) + "/" + str(int(end.timestamp()*1000)-1)
               + "?adjusted=false&sort=asc&limit=50000")
    return [{"url": url, "role": "candles"}]


def parse_records(task, raw_json):
    kind, symbol, seconds, start, end = _settings(task)
    payload = json.loads(raw_json) if isinstance(raw_json, (str, bytes, bytearray)) else raw_json
    if not isinstance(payload, dict):
        raise ValueError("Source did not return an object")
    if payload.get("status") in ("error", "ERROR", "NOT_AUTHORIZED") or payload.get("error"):
        raise ValueError("Source returned an error; absence was not established")
    if payload.get("next_url"):
        raise ValueError("Provider truncated bounded response; split the queued request")
    if kind == "twelvedata_candles":
        meta = payload.get("meta") or {}
        if meta.get("symbol") and str(meta["symbol"]).upper() != symbol:
            raise ValueError("Source symbol differs from requested full symbol")
        if meta.get("interval") and meta["interval"] != "1min":
            raise ValueError("Source interval differs from requested minute interval")
        tz = meta.get("timezone") or meta.get("exchange_timezone")
        # exchange_timezone describes the venue, not necessarily response labels.
        if meta.get("timezone") and tz not in ("UTC", "Etc/UTC", "GMT"):
            raise ValueError("Source label timezone contradicts explicit UTC request")
        request = task.get("request_json") or {}
        if meta.get("type") == "Digital Currency":
            if not request.get("expected_exchange") or not request.get("expected_currency_quote"):
                raise ValueError("Crypto source contract requires explicit exchange and quote currency")
        for expected, native in (("expected_exchange","exchange"),("expected_currency_quote","currency_quote")):
            if request.get(expected) is not None and meta.get(native) != request[expected]:
                raise ValueError("Provider source identity differs from authorized exchange/quote contract")
        source_rows = payload.get("values")
    else:
        if payload.get("ticker") and str(payload["ticker"]) != symbol:
            raise ValueError("Source ticker differs from requested full symbol")
        if payload.get("adjusted") is True:
            raise ValueError("Adjusted response contradicts raw price request")
        source_rows = payload.get("results", []) if payload.get("resultsCount") == 0 else payload.get("results")
    if not isinstance(source_rows, list):
        raise ValueError("Source did not return a candle array")
    validation = {"parser_version": VERSION, "raw_count": 0, "valid_count": 0, "invalid_count": 0,
                  "duplicate_count": 0, "outside_count": 0, "duplicate_conflict_count": 0, "errors": [],
                  "strict_replay_certified": False, "nominal_grid_slots": int((end-start).total_seconds()//seconds),
                  "adjustment_basis": "RAW_SOURCE_RESPONSE_NO_ACTION_ADJUSTMENT",
                  "coverage_assertion": "SOURCE_ROWS_ONLY_NO_GRID_FILLING"}
    if kind == "twelvedata_candles":
        validation["provider_metadata"] = {k:meta.get(k) for k in ("symbol","interval","exchange","currency_base","currency_quote","type")}
        validation["source_selection"] = {"requested_exchange":(task.get("request_json") or {}).get("expected_exchange"),
          "requested_quote_currency":(task.get("request_json") or {}).get("expected_currency_quote"),
          "basis":"EXPLICIT_PROVIDER_EXCHANGE_AND_QUOTE" if (task.get("request_json") or {}).get("expected_exchange") else "NON_CRYPTO_PROVIDER_DEFAULT",
          "historical_selection_knowledge_recovered":False,
          "USD_USDT_equivalence_asserted":False}
    is_quoted_pair = kind == "massive_candles" and symbol.startswith("C:")
    if is_quoted_pair:
        validation["source_contract"] = "PROVIDER_BID_ASK_QUOTE_AGGREGATES_NOT_EXECUTED_TRADES"
        validation["activity_contract"] = "v_n_vw_RETAINED_IN_RAW_NOT_PROMOTED_AS_TRADE_VOLUME_COUNT_OR_VWAP"
        validation["source_contract_evidence"] = "https://massive.com/docs/rest/forex/aggregates/custom-bars"
    records_by_time = {}
    for i, value in enumerate(source_rows):
        validation["raw_count"] += 1
        try:
            if not isinstance(value, dict):
                raise ValueError("Candle must be an object")
            if kind == "twelvedata_candles":
                label = datetime.fromisoformat(str(value["datetime"]).replace("Z", "+00:00"))
                label = label.replace(tzinfo=UTC) if label.tzinfo is None else label.astimezone(UTC)
                source_values = [value[k] for k in ("open", "high", "low", "close")]
                volume = value.get("volume")
                vwap = None
                trades = None
            else:
                millis = _decimal(value["t"], "timestamp", nonnegative=True)
                if millis != millis.to_integral_value():
                    raise ValueError("Nonintegral source timestamp")
                label = datetime(1970,1,1,tzinfo=UTC)+timedelta(milliseconds=int(millis))
                source_values = [value[k] for k in ("o", "h", "l", "c")]
                volume, vwap, trades = value.get("v"), value.get("vw"), value.get("n")
                if is_quoted_pair:
                    # C: bars derive from quoted bid/ask updates. Preserve source
                    # activity fields in immutable raw only; they are not trades.
                    for field in ("v", "vw", "n"):
                        if value.get(field) is not None:
                            _decimal(value[field], "quoted_source_" + field, nonnegative=True)
                    volume = vwap = trades = None
            if label.microsecond or int(label.timestamp()) % seconds:
                raise ValueError("Source candle is off requested interval grid")
            o,h,l,c = [_decimal(v,k,positive=True) for v,k in zip(source_values,("open","high","low","close"))]
            if h < max(o,l,c) or l > min(o,h,c):
                raise ValueError("Invalid source OHLC; no synthetic correction applied")
            v = None if volume is None else str(_decimal(volume,"volume",nonnegative=True))
            vw = None if vwap is None else str(_decimal(vwap,"vwap",positive=True))
            nt = None if trades is None else _decimal(trades,"trade_count",nonnegative=True)
            if nt is not None and nt != nt.to_integral_value():
                raise ValueError("Nonintegral trade count")
            bar_end = label+timedelta(seconds=seconds)
            if not start <= label < end or bar_end > end:
                validation["outside_count"] += 1
                continue
            values = dict(zip(("open","high","low","close"),map(str,(o,h,l,c))))
            values.update({"volume":v,"vwap":vw,"trade_count":None if nt is None else int(nt),
                           "quote_volume":None,"taker_buy_base_volume":None,"taker_buy_quote_volume":None,
                           "_not_before":_iso(bar_end),"_not_before_basis":"BAR_COMPLETION_LOWER_BOUND_NOT_PUBLICATION",
                           "_unit_basis":("PROVIDER_NATIVE_QUOTED_PRICE_REQUIRES_FX_OR_METAL_UNIT_CONTRACT" if is_quoted_pair else "PROVIDER_NATIVE_PRICE_AND_VOLUME_UNITS_REQUIRES_INSTRUMENT_CONTRACT"),
                           "_price_basis":("PROVIDER_BID_ASK_QUOTE_AGGREGATE" if is_quoted_pair else "PROVIDER_NATIVE_TRADE_OR_UNSPECIFIED_PRICE"),
                           "_historical_publication_at":None,"_historical_first_receipt_at":None,
                           "_strict_historical_replay_eligible":False,"_source_row_number":i})
            stamp = _iso(label)
            if stamp in records_by_time:
                validation["duplicate_count"] += 1
                old = {k:v for k,v in records_by_time[stamp]["values"].items() if k != "_source_row_number"}
                new = {k:v for k,v in values.items() if k != "_source_row_number"}
                if old != new:
                    validation["duplicate_conflict_count"] += 1
                    validation["errors"].append({"row":i,"reason":"conflicting_duplicate"})
                continue
            records_by_time[stamp] = {"record_key":f"{kind}|{symbol}|{seconds}|{stamp}","observed_at":stamp,
                                      "bar_end":_iso(bar_end),"values":values}
        except (KeyError,ValueError,TypeError,OverflowError) as exc:
            validation["invalid_count"] += 1
            if len(validation["errors"])<20:
                validation["errors"].append({"row":i,"reason":str(exc)})
    records = [records_by_time[k] for k in sorted(records_by_time)]
    validation["valid_count"] = len(records)
    validation["normalization_passed"] = validation["invalid_count"] == 0 and validation["duplicate_conflict_count"] == 0
    validation["absent_nominal_slots"] = validation["nominal_grid_slots"] - len(records)
    return records, validation
