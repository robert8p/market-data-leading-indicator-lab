"""Binance historical percentage-band depth profiles, not full order books.

One normalized record contains every source level at one native timestamp. Raw
CSV-band rows and normalized timestamp profiles have separately named counts.
No bid/ask quotes, spread, exact first receipt, or regular sampling grid is made up.
"""
from __future__ import annotations

import csv
import io
import json
import re
import zipfile
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from decimal import Decimal, InvalidOperation

PARSER_VERSION = "binance_depth_profile_remediation_v2"
VERSION = PARSER_VERSION
LEVELS = tuple(Decimal(x) for x in tuple(range(-5, 0)) + tuple(range(1, 6)))
INNER_LEVELS = tuple(sorted(LEVELS + (Decimal("-0.2"), Decimal("0.2"))))
MAX_ZIP_BYTES = 8 * 1024 * 1024
MAX_CSV_BYTES = 64 * 1024 * 1024


def _dt(value):
    d = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if d.tzinfo is None:
        raise ValueError("task_timestamp_requires_timezone")
    return d.astimezone(timezone.utc)


def _iso(d):
    return d.isoformat().replace("+00:00", "Z")


def _task(task):
    if task.get("source_type") != "binance_book_depth":
        raise ValueError("unsupported_depth_source_type")
    symbol = task["symbol"]
    if not re.fullmatch(r"[A-Z0-9_]{2,40}", symbol):
        raise ValueError("invalid_depth_symbol")
    start, end = _dt(task["start_ts"]), _dt(task["end_ts"])
    r = task.get("request_json") or {}
    if r.get("market", "um") != "um" or r.get("archive_frequency", "daily") != "daily":
        raise ValueError("depth_requires_um_daily")
    if int(task.get("interval_seconds", -1)) != 0:
        raise ValueError("depth_is_irregular_snapshot_not_fixed_interval")
    if start >= end or (end - timedelta(microseconds=1)).date() != start.date():
        raise ValueError("depth_task_must_fit_one_utc_archive_day")
    return symbol, start, end


def build_requests(task):
    symbol, start, _ = _task(task)
    filename = f"{symbol}-bookDepth-{start:%Y-%m-%d}.zip"
    url = f"https://data.binance.vision/data/futures/um/daily/bookDepth/{symbol}/{filename}"
    return [{"url": url, "role": "archive"}, {"url": url + ".CHECKSUM", "role": "checksum"}]


def _number(value):
    try:
        n = Decimal(str(value))
    except InvalidOperation as exc:
        raise ValueError("invalid_depth_number") from exc
    if not n.is_finite() or n < 0:
        raise ValueError("negative_or_nonfinite_depth")
    return n


def parse_records(task, raw_zip):
    symbol, start, end = _task(task)
    if len(raw_zip) > MAX_ZIP_BYTES:
        raise ValueError("depth_zip_limit_exceeded")
    with zipfile.ZipFile(io.BytesIO(raw_zip)) as z:
        files = [x for x in z.infolist() if not x.is_dir()]
        if len(files) != 1 or files[0].file_size > MAX_CSV_BYTES or files[0].flag_bits & 1:
            raise ValueError("unsupported_depth_archive_structure")
        raw_csv = z.read(files[0])
    reader = csv.DictReader(io.StringIO(raw_csv.decode("utf-8-sig")))
    if reader.fieldnames != ["timestamp", "percentage", "depth", "notional"]:
        raise ValueError("depth_header_contract_changed")
    profiles = defaultdict(list)
    errors = []
    unparseable_rows = 0
    raw_source_rows = 0
    for row in reader:
        raw_source_rows += 1
        try:
            label = datetime.strptime(row["timestamp"], "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
            profiles[label].append(row)
        except (ValueError, TypeError):
            unparseable_rows += 1
            if len(errors) < 20:
                errors.append({"source_row": raw_source_rows, "error": "invalid_source_timestamp"})

    records = []
    level_counts = defaultdict(int)
    invalid_count = unparseable_rows
    outside_count = 0
    duplicate_band_rows = 0
    conflicting_profiles = 0
    for label, rows in sorted(profiles.items()):
        if not start <= label < end:
            outside_count += 1
            continue
        try:
            levels = {}
            for r in rows:
                level = Decimal(r["percentage"])
                if not level.is_finite() or level not in INNER_LEVELS:
                    raise ValueError("unsupported_percentage_band")
                values = (_number(r["depth"]), _number(r["notional"]))
                if (values[0] == 0) != (values[1] == 0):
                    raise ValueError("zero_depth_notional_inconsistent")
                if level in levels:
                    duplicate_band_rows += 1
                    if levels[level] != values:
                        conflicting_profiles += 1
                        raise ValueError("conflicting_duplicate_band")
                levels[level] = values
            if set(levels) not in (set(LEVELS), set(INNER_LEVELS)):
                raise ValueError("incomplete_depth_profile")
            for sign in (-1, 1):
                side = sorted((k for k in levels if k * sign > 0), key=abs)
                for near, far in zip(side, side[1:]):
                    if any(levels[far][j] < levels[near][j] for j in (0, 1)):
                        raise ValueError("nonmonotonic_cumulative_band")
            lower, upper = [], []
            for level, (quantity, notional) in levels.items():
                if quantity == 0:
                    continue
                weighted = notional / quantity
                edge = weighted / (1 + Decimal(level) / 100)
                lower.append(weighted if level < 0 else edge)
                upper.append(edge if level < 0 else weighted)
            if lower and max(lower) > min(upper):
                raise ValueError("no_common_feasible_midpoint_for_bands")
            ordered_levels = sorted(levels)
            bands = [[str(level.normalize()), str(levels[level][0]), str(levels[level][1])] for level in ordered_levels]
            level_counts[len(bands)] += 1
            records.append({
                "record_key": f"binance_um:bookDepth:{symbol}:{_iso(label)}",
                "observed_at": _iso(label), "bar_end": None,
                "values": {
                    "depth_bands": bands,
                    "_depth_band_columns": ["signed_percent_from_midpoint", "source_native_quantity", "quote_notional"],
                    "_not_before": _iso(label),
                    "_not_before_basis": "SOURCE_SNAPSHOT_LABEL_NOT_PUBLICATION_OR_RECEIPT",
                    "_unit_basis": "UM_SOURCE_NATIVE_QUANTITY_QUOTE_NOTIONAL_SCALED_TOKEN_MAPPING_REQUIRED",
                    "_timestamp_basis": "BINANCE_DAILY_ARCHIVE_UTC_LABEL_SECONDS",
                    "_source_band_rows": len(rows),
                    "_exact_sampling_grid_claimed": False,
                    "_best_bid_ask_available": False,
                },
            })
        except (ValueError, TypeError, KeyError, InvalidOperation) as exc:
            invalid_count += 1
            if len(errors) < 20:
                errors.append({"observed_at": _iso(label), "error": str(exc)})

    validation = {
        "parser_version": PARSER_VERSION,
        "raw_count": len(profiles) + unparseable_rows,
        "valid_count": len(records), "invalid_count": invalid_count,
        "outside_count": outside_count, "duplicate_count": 0,
        "duplicate_conflict_count": conflicting_profiles,
        "normalization_passed": invalid_count == 0,
        "count_basis": "DISTINCT_NATIVE_TIMESTAMP_PROFILES_PLUS_UNPARSEABLE_SOURCE_ROWS",
        "raw_source_csv_band_rows": raw_source_rows,
        "duplicate_source_band_rows": duplicate_band_rows,
        "source_levels_per_complete_profile": sorted(level_counts),
        "source_profile_band_count_populations": dict(level_counts),
        "source_percentage_storage": "EXACT_DECIMAL_TEXT_NO_TRUNCATION",
        "errors": errors,
        "source_contract": "IRREGULAR_PERCENTAGE_BAND_DEPTH_PROFILES_NOT_BEST_QUOTES_OR_FULL_L2_BOOK",
        "external_quote_price_reconciliation": False,
    }
    return records, validation
