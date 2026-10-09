"""One immutable, native-case Massive historical ticker page per durable task.

The queue's reviewed completion trigger alone schedules an exact returned cursor.
This parser returns no price observations. Native rows stay in the retained raw
artifact for separately reviewed identity import, including every unmatched row.
"""
from __future__ import annotations

import hashlib
import json
import re
from collections import Counter
from datetime import datetime, timedelta, timezone
from urllib.parse import parse_qsl, urlencode, urlsplit

VERSION = "massive_native_warmup_reference_page_20261008_v1"
WARMUP_CONTRACT = "RTH_DAILY_LOOKBACK61_PREWINDOW_2025-06-04_2025-08-29_V1"
SOURCE_TYPE = "massive_reference_tickers"
ORIGIN = "https://api.massive.com"
PATH = "/v3/reference/tickers"
MAX_PAGES = 16
MAX_ROWS = 1000
SYMBOL_PATTERN = r"[A-Za-z0-9][A-Za-z0-9._/-]{0,63}"
CURSOR_PATTERN = r"[A-Za-z0-9_+/=-]{1,8192}"
SHA_PATTERN = r"[a-f0-9]{64}"
SCOPE = "MASSIVE_STOCKS_ACTIVE_ON_REFERENCE_DATE_ALL_NATIVE_TICKERS"
UTC = timezone.utc


def sha(text):
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _dt(value):
    value = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if value.tzinfo is None:
        raise ValueError("Explicit UTC offset required")
    return value.astimezone(UTC)


def settings(task):
    req = task.get("request_json") or {}
    if isinstance(req, str):
        req = json.loads(req)
    if (task.get("provider") != "massive" or task.get("source_type") != SOURCE_TYPE
            or task.get("interval_seconds") != 0 or task.get("symbol") != "US_STOCKS"):
        raise ValueError("Native reference task scope invalid")
    if req.get("required_parser_version") != VERSION or req.get("reference_scope") != SCOPE:
        raise ValueError("Explicit native reference contract required")
    start, end = _dt(task["start_ts"]), _dt(task["end_ts"])
    if (end != start + timedelta(days=1) or start.time().isoformat() != "00:00:00"
            or start < _dt("2025-06-04T00:00:00Z") or end > _dt("2025-08-30T00:00:00Z")
            or start.weekday() >= 5 or start.date().isoformat() in {"2025-06-19", "2025-07-04"}
            or req.get("warmup_contract") != WARMUP_CONTRACT
            or req.get("reference_date") != start.date().isoformat()):
        raise ValueError("Native reference date window invalid")
    page = req.get("page_number")
    if isinstance(page, bool) or not isinstance(page, int) or not 1 <= page <= MAX_PAGES:
        raise ValueError("Native reference page bound invalid")
    cursor = req.get("cursor")
    ancestors = req.get("ancestor_cursor_sha256", [])
    if not isinstance(ancestors, list) or len(ancestors) != max(0, page-2):
        raise ValueError("Native cursor lineage length invalid")
    if any(not isinstance(x, str) or not re.fullmatch(SHA_PATTERN, x) for x in ancestors):
        raise ValueError("Native cursor lineage hash invalid")
    if len(ancestors) != len(set(ancestors)):
        raise ValueError("Native cursor lineage contains a cycle")
    if page == 1:
        if cursor is not None or req.get("parent_batch_key") or req.get("parent_source_sha256") or req.get("previous_last_native_ticker"):
            raise ValueError("First page must have no inherited cursor")
    else:
        if not isinstance(cursor, str) or not re.fullmatch(CURSOR_PATTERN, cursor):
            raise ValueError("Exact provider cursor required")
        if sha(cursor) in ancestors:
            raise ValueError("Native cursor repeats an ancestor")
        if not req.get("parent_batch_key") or not re.fullmatch(SHA_PATTERN, str(req.get("parent_source_sha256", ""))):
            raise ValueError("Committed parent source proof required")
        previous = req.get("previous_last_native_ticker")
        if not isinstance(previous, str) or not re.fullmatch(SYMBOL_PATTERN, previous):
            raise ValueError("Previous native boundary required")
    return req, page, cursor, ancestors


def validate_reference_url(url):
    try:
        p = urlsplit(url)
        if (p.scheme != "https" or p.hostname != "api.massive.com" or p.path != PATH
                or p.username or p.password or p.port not in (None, 443) or p.fragment):
            return False
        pairs = parse_qsl(p.query, keep_blank_values=True, strict_parsing=True)
        if len(pairs) != len(dict(pairs)):
            return False
        q = dict(pairs)
        if set(q) == {"cursor"}:
            return bool(re.fullmatch(CURSOR_PATTERN, q["cursor"]))
        if set(q) != {"market", "date", "active", "sort", "order", "limit"}:
            return False
        return (q["market"] == "stocks" and q["active"] == "true" and q["sort"] == "ticker"
                and q["order"] == "asc" and q["limit"] == "1000"
                and bool(re.fullmatch(r"202[56]-[0-9]{2}-[0-9]{2}", q["date"])))
    except (ValueError, TypeError):
        return False


def build_requests(task):
    req, page, cursor, _ = settings(task)
    params = ({"market": "stocks", "date": req["reference_date"], "active": "true",
               "sort": "ticker", "order": "asc", "limit": MAX_ROWS} if page == 1 else {"cursor": cursor})
    url = ORIGIN + PATH + "?" + urlencode(params)
    if not validate_reference_url(url):
        raise ValueError("Native reference URL rejected")
    return [{"role": "candles", "url": url}]  # Existing transport primary JSON role.


def parse_records(task, raw):
    req, page, cursor, ancestors = settings(task)
    payload = json.loads(raw) if isinstance(raw, (bytes, str)) else raw
    if not isinstance(payload, dict) or payload.get("status") != "OK":
        raise ValueError("Native reference source status invalid")
    native = payload.get("results", [])
    count = payload.get("count")
    if (not isinstance(native, list) or isinstance(count, bool) or not isinstance(count, int)
            or count != len(native) or not 0 <= count <= MAX_ROWS):
        raise ValueError("Native page source population mismatch")
    symbols, kinds, exchanges, locales = [], Counter(), Counter(), Counter()
    for row in native:
        if not isinstance(row, dict):
            raise ValueError("Native ticker row must be an object")
        ticker = row.get("ticker")
        if not isinstance(ticker, str) or not re.fullmatch(SYMBOL_PATTERN, ticker) or ".." in ticker:
            raise ValueError("Native ticker symbol invalid")
        if row.get("market") != "stocks" or row.get("active") is not True:
            raise ValueError("Native dated market/active scope mismatch")
        symbols.append(ticker)
        kinds[str(row.get("type"))] += 1
        exchanges[str(row.get("primary_exchange"))] += 1
        locales[str(row.get("locale"))] += 1
    if len(set(symbols)) != count or symbols != sorted(symbols):
        raise ValueError("Native ticker order or exact duplicate invalid")
    if symbols and page > 1 and symbols[0] <= req["previous_last_native_ticker"]:
        raise ValueError("Native page boundary overlaps previous source")
    next_url = payload.get("next_url") or None
    next_cursor = None
    if next_url is not None:
        if not isinstance(next_url, str) or not validate_reference_url(next_url):
            raise ValueError("Provider next URL rejected")
        query = dict(parse_qsl(urlsplit(next_url).query, keep_blank_values=True))
        if set(query) != {"cursor"}:
            raise ValueError("Next page must contain exact opaque cursor only")
        next_cursor = query["cursor"]
        if (page >= MAX_PAGES or not symbols or sha(next_cursor) in ancestors
                or cursor is not None and sha(next_cursor) == sha(cursor)):
            raise ValueError("Native cursor cycle, empty continuation, or page ceiling")
    validation = {
        "raw_count": 0, "valid_count": 0, "invalid_count": 0, "duplicate_count": 0, "outside_count": 0,
        "duplicate_conflict_count": 0, "normalization_passed": True, "parser_version": VERSION,
        "source_record_grain": "RAW_ONLY_DATED_NATIVE_REFERENCE_ROWS_NO_PRICE_OBSERVATIONS",
        "source_native_reference_rows": count, "source_unique_native_tickers": count,
        "source_symbol_case_preserved": True, "source_native_order_verified": True,
        "source_reference_date": req["reference_date"], "source_reference_scope": SCOPE,
        "source_page_number": page, "source_page_complete": True,
        "source_cursor_sha256": sha(cursor) if cursor is not None else None,
        "first_native_ticker": symbols[0] if symbols else None,
        "last_native_ticker": symbols[-1] if symbols else None,
        "next_cursor": next_cursor, "next_cursor_sha256": sha(next_cursor) if next_cursor else None,
        "next_url": next_url, "source_terminal_page": next_cursor is None,
        "source_request_id": payload.get("request_id"),
        "native_type_counts": dict(kinds), "native_exchange_counts": dict(exchanges), "native_locale_counts": dict(locales),
        "source_coverage_complete": False,  # Requires all committed pages in the date-level view.
        "historical_effective_date_semantics": "PROVIDER_DATED_LISTING_SNAPSHOT",
        "historical_knowledge_semantics": "ACTUAL_LATE_ACQUISITION_RETAINED_SEPARATELY",
        "historical_first_receipt_recovered": False, "historical_classification_first_receipt_recovered": False,
        "unmatched_native_rows_preserved": True,
        "warmup_contract": WARMUP_CONTRACT, "anchor_filter_applied": False,
        "original_main_window_denominator_changed": False,
    }
    return [], validation

