"""Pure Binance archive request/normalization helpers; no network or database writes.

Worker must retain original ZIP, HTTP receipt/Last-Modified and published checksum
before calling this module. Completion bounds below are not publication evidence.
"""
from __future__ import annotations

import csv
import io
import json
import re
import zipfile
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation

UTC = timezone.utc
VERSION = "binance_archive_source_rules_20261008_v1"
INTERVALS = {1: "1s", 60: "1m", 180: "3m", 300: "5m", 900: "15m", 1800: "30m",
             3600: "1h", 7200: "2h", 14400: "4h", 21600: "6h", 28800: "8h",
             43200: "12h", 86400: "1d"}
KINDS = {"binance_klines": "klines", "binance_mark": "markPriceKlines",
         "binance_index": "indexPriceKlines", "binance_metrics": "metrics",
         "binance_funding": "fundingRate"}
METRICS = {"sum_open_interest": "open_interest", "sum_open_interest_value": "open_interest_value",
           "count_toptrader_long_short_ratio": "top_account_long_short_ratio",
           "sum_toptrader_long_short_ratio": "top_position_long_short_ratio",
           "count_long_short_ratio": "global_long_short_ratio",
           "sum_taker_long_short_vol_ratio": "taker_buy_sell_ratio"}


def _dt(value):
    d = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if d.tzinfo is None:
        raise ValueError("Task timestamps must have explicit timezone")
    return d.astimezone(UTC)


def _iso(value):
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _decimal(value, name, nonnegative=False, positive=False):
    try:
        n = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        raise ValueError("invalid numeric " + name)
    if not n.is_finite() or (nonnegative and n < 0) or (positive and n <= 0):
        raise ValueError("invalid finite/range " + name)
    return n


def _epoch(raw, unit):
    n = _decimal(raw, "timestamp", nonnegative=True)
    if n != n.to_integral_value():
        raise ValueError("nonintegral source timestamp")
    # Units are source-specific, never guessed from numeric magnitude.
    micros = int(n) * {"ms": 1000, "us": 1}[unit]
    return datetime(1970, 1, 1, tzinfo=UTC) + timedelta(microseconds=micros)


def _settings(task):
    req = task.get("request_json") or {}
    if isinstance(req, str):
        req = json.loads(req)
    kind = task["source_type"]
    if kind not in KINDS:
        raise ValueError("Unsupported Binance source_type")
    symbol = str(task["symbol"]).upper()
    if not re.fullmatch(r"[A-Z0-9_]{1,64}", symbol):
        raise ValueError("Invalid venue symbol")
    market = req.get("market", "um")
    if market not in ("um", "spot") or (market == "spot" and kind != "binance_klines"):
        raise ValueError("Unsupported market/source combination")
    freq = req.get("archive_frequency", "daily")
    if freq not in ("daily", "monthly"):
        raise ValueError("Unsupported archive frequency")
    if kind == "binance_metrics" and freq != "daily":
        raise ValueError("Verified metrics source uses daily archives")
    if kind == "binance_funding" and freq != "monthly":
        raise ValueError("Verified funding source uses monthly archives")
    start, end = _dt(task["start_ts"]), _dt(task["end_ts"])
    if not start < end:
        raise ValueError("Empty task interval")
    seconds = int(task.get("interval_seconds") or (300 if kind == "binance_metrics" else 0))
    if kind == "binance_metrics" and seconds != 300:
        raise ValueError("Metrics are native five-minute observations; no implicit resampling")
    if kind not in ("binance_metrics", "binance_funding") and seconds not in INTERVALS:
        raise ValueError("Unsupported fixed bar interval")
    anchor = _dt(req["archive_start_ts"]) if req.get("archive_start_ts") else start
    # The task is bounded to exactly one archive; parent queues additional files.
    archive_start = anchor.replace(hour=0, minute=0, second=0, microsecond=0)
    if freq == "monthly":
        archive_start = archive_start.replace(day=1)
        archive_end = (archive_start.replace(year=archive_start.year + 1, month=1)
                       if archive_start.month == 12 else archive_start.replace(month=archive_start.month + 1))
    else:
        archive_end = archive_start + timedelta(days=1)
    if start < archive_start or end > archive_end:
        raise ValueError("Task crosses archive boundary; split task before acquisition")
    return req, kind, symbol, market, freq, seconds, start, end, archive_start, archive_end


def build_requests(task):
    _, kind, symbol, market, freq, seconds, _, _, anchor, _ = _settings(task)
    date_part = anchor.strftime("%Y-%m" if freq == "monthly" else "%Y-%m-%d")
    prefix = "data/spot" if market == "spot" else "data/futures/um"
    dataset = KINDS[kind]
    if kind in ("binance_metrics", "binance_funding"):
        filename = f"{symbol}-{dataset}-{date_part}.zip"
        path = f"{prefix}/{freq}/{dataset}/{symbol}/{filename}"
    else:
        interval = INTERVALS[seconds]
        filename = f"{symbol}-{interval}-{date_part}.zip"
        path = f"{prefix}/{freq}/{dataset}/{symbol}/{interval}/{filename}"
    url = "https://data.binance.vision/" + path
    return [{"url": url, "role": "archive"}, {"url": url + ".CHECKSUM", "role": "checksum"}]


def parse_records(task, raw_zip):
    req, kind, symbol, market, freq, seconds, start, end, archive_start, archive_end = _settings(task)
    validation = {"parser_version": VERSION, "raw_count": 0, "valid_count": 0,
                  "invalid_count": 0, "duplicate_count": 0, "outside_count": 0,
                  "duplicate_conflict_count": 0, "errors": [], "strict_replay_certified": False}
    with zipfile.ZipFile(io.BytesIO(raw_zip)) as z:
        members = [m for m in z.infolist() if not m.is_dir() and m.filename.lower().endswith(".csv")]
        if len(members) != 1:
            raise ValueError("Expected one CSV member in archive")
        member = members[0]
        if member.file_size > int(req.get("max_uncompressed_bytes", 128 * 1024 * 1024)):
            raise ValueError("Archive exceeds bounded uncompressed byte budget")
        text = z.read(member).decode("utf-8-sig")
    validation["member_name"] = member.filename
    raw_rows = [r for r in csv.reader(io.StringIO(text)) if r and any(x.strip() for x in r)]
    header = None
    if kind in ("binance_metrics", "binance_funding"):
        if not raw_rows:
            raise ValueError("Missing required source header")
        header = raw_rows.pop(0)
        required = ["create_time", "symbol", *METRICS] if kind == "binance_metrics" else ["calc_time", "funding_interval_hours", "last_funding_rate"]
        if not all(name in header for name in required):
            raise ValueError("Unexpected source header: " + ",".join(header))
    elif raw_rows and raw_rows[0][0].strip().lower() in ("open_time", "open time"):
        header = raw_rows.pop(0)
    validation["source_header"] = header
    records_by_key = {}
    labels = []
    for row_number, row in enumerate(raw_rows, 2 if header else 1):
        validation["raw_count"] += 1
        try:
            values = {"_parser_version": VERSION, "_source_row_number": row_number,
                      "_source_label_timezone": "UTC", "_historical_publication_at": None,
                      "_historical_first_receipt_at": None, "_original_version_history_complete": False,
                      "_strict_historical_replay_eligible": False, "venue_symbol": symbol,
                      "market": market, "source_type": kind}
            if kind == "binance_metrics":
                if len(row) != len(header):
                    raise ValueError("Metrics field count mismatch")
                item = dict(zip(header, row))
                if item["symbol"] != symbol:
                    raise ValueError("Source symbol differs from manifest")
                label = datetime.fromisoformat(item["create_time"]).replace(tzinfo=UTC)
                bar_end = label + timedelta(minutes=5)
                for source, target in METRICS.items():
                    values[target] = str(_decimal(item[source], source, nonnegative=True))
                values.update({"_source_label_ts": _iso(label), "_not_before": _iso(bar_end),
                               "_not_before_basis": "MODELED_FIVE_MINUTE_COMPLETION_BOUND_CURRENT_ARCHIVE",
                               "_stock_snapshot_time_verified": False,
                               "_taker_interval_basis": "CURRENT_ARCHIVE_START_LABEL_PROBE_SUPPORTED",
                               "_unit_basis": "PROVIDER_NATIVE_CONTRACT_AND_QUOTE_UNITS_MAPPING_REQUIRED"})
                # Loaded versions must remain unambiguously same-day start-labelled.
                if not archive_start <= label < archive_end:
                    raise ValueError("Ambiguous metrics label outside archive date; preserve for adjudication")
            elif kind == "binance_funding":
                if len(row) != len(header):
                    raise ValueError("Funding field count mismatch")
                item = dict(zip(header, row))
                label = _epoch(item["calc_time"], "ms")
                bar_end = None
                hours = _decimal(item["funding_interval_hours"], "funding_interval_hours", positive=True)
                values.update({"funding_rate": str(_decimal(item["last_funding_rate"], "funding_rate")),
                               "funding_interval_hours": str(hours), "mark_price": None,
                               "_source_label_ts": _iso(label), "_not_before": _iso(label),
                               "_not_before_basis": "PROVIDER_FUNDING_EVENT_TIME_NOT_PUBLICATION",
                               "_unit_basis": "DECIMAL_RATE_PER_PROVIDER_FUNDING_INTERVAL"})
            else:
                if len(row) < 11:
                    raise ValueError("Kline missing required source fields")
                unit = "us" if market == "spot" and archive_start >= datetime(2025, 1, 1, tzinfo=UTC) else "ms"
                label = _epoch(row[0], unit)
                bar_end = label + timedelta(seconds=seconds)
                close_label = _epoch(row[6], unit)
                if close_label != bar_end - timedelta(microseconds=1 if unit == "us" else 1000):
                    raise ValueError("Kline close timestamp disagrees with fixed interval")
                o, h, l, c = [_decimal(row[i], n, positive=True) for i, n in zip(range(1, 5), ["open", "high", "low", "close"])]
                if h < max(o, l, c) or l > min(o, h, c):
                    raise ValueError("Invalid source OHLC invariant")
                values.update(dict(zip(["open", "high", "low", "close"], map(str, [o, h, l, c]))))
                values.update({"_source_label_ts": _iso(label), "_source_close_label_ts": _iso(close_label),
                               "_source_timestamp_unit": unit, "_not_before": _iso(bar_end),
                               "_not_before_basis": "BAR_COMPLETION_LOWER_BOUND_NOT_PUBLICATION"})
                if kind == "binance_klines":
                    vol = _decimal(row[5], "volume", nonnegative=True)
                    quote_vol = _decimal(row[7], "quote_volume", nonnegative=True)
                    count = _decimal(row[8], "trade_count", nonnegative=True)
                    buy = _decimal(row[9], "taker_buy_base_volume", nonnegative=True)
                    buy_quote = _decimal(row[10], "taker_buy_quote_volume", nonnegative=True)
                    if count != count.to_integral_value() or buy > vol or buy_quote > quote_vol:
                        raise ValueError("Invalid trade activity invariant")
                    values.update({"volume": str(vol), "quote_volume": str(quote_vol), "trade_count": int(count),
                                   "taker_buy_base_volume": str(buy), "taker_buy_quote_volume": str(buy_quote),
                                   "_unit_basis": "SOURCE_NATIVE_BASE_ASSET_AND_QUOTE_ASSET"})
                else:
                    # Mark/index count is sample count; zero volumes are placeholders.
                    values.update({"volume": None, "quote_volume": None, "trade_count": None,
                                   "taker_buy_base_volume": None, "taker_buy_quote_volume": None,
                                   "source_sample_count": str(_decimal(row[8], "sample_count", nonnegative=True)),
                                   "_unit_basis": "MARK_OR_INDEX_PRICE_NO_TRADE_ACTIVITY"})
            if kind != "binance_funding":
                epoch_seconds = int((label - datetime(1970, 1, 1, tzinfo=UTC)).total_seconds())
                if label.microsecond or epoch_seconds % seconds:
                    raise ValueError("Source label is off interval grid")
            if not start <= label < end or (bar_end is not None and bar_end > end):
                validation["outside_count"] += 1
                continue
            labels.append(_iso(label))
            key = f"{market}|{kind}|{symbol}|{seconds}|{_iso(label)}"
            record = {"record_key": key, "observed_at": _iso(label),
                      "bar_end": _iso(bar_end) if bar_end else None, "values": values}
            if key in records_by_key:
                validation["duplicate_count"] += 1
                old = {k: v for k, v in records_by_key[key]["values"].items() if k != "_source_row_number"}
                new = {k: v for k, v in values.items() if k != "_source_row_number"}
                if old != new:
                    validation["duplicate_conflict_count"] += 1
                    if len(validation["errors"]) < 20:
                        validation["errors"].append({"row": row_number, "reason": "conflicting_duplicate", "record_key": key})
                continue
            records_by_key[key] = record
        except (ValueError, TypeError, KeyError, OverflowError) as exc:
            validation["invalid_count"] += 1
            if len(validation["errors"]) < 20:
                validation["errors"].append({"row": row_number, "reason": str(exc)})
    records = sorted(records_by_key.values(), key=lambda r: r["record_key"])
    validation["valid_count"] = len(records)
    validation["first_source_label"] = min(labels) if labels else None
    validation["last_source_label"] = max(labels) if labels else None
    validation["normalization_passed"] = validation["invalid_count"] == 0 and validation["duplicate_conflict_count"] == 0
    validation["coverage_assertion"] = "RETURNED_SOURCE_ROWS_ONLY_NOT_HISTORICAL_UNIVERSE_OR_PROVIDER_GRID_COMPLETENESS"
    return records, validation
