"""Source-only minute collector core; not a price normalizer or deployment.

The integration adapter must obtain a live, fenced native source reservation for
ONE page before calling acquire_page. This module never marks prices, instruments,
or data families accepted. It performs no retries after an uncertain HTTP/commit
outcome. Credentials are injected only into the outgoing request, never receipts.
"""
from __future__ import annotations
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Callable, Mapping
from urllib.parse import urlsplit, urlunsplit, parse_qsl, urlencode, quote
import hashlib
import json
import math
import re
import time

ORIGIN = "https://api.massive.com"
COHORT = "2dc0f766befed6de6b052221c3c87482d507ba1bad931cbb98654eb3af899bec"
WINDOWS = {(1785556800000, 1788235200000), (1788235200000, 1790827200000)}
MAX_BYTES = 8_388_608
MAX_PAGE_RECORDS = 50_000
QUALIFICATION = "RAW_PROVIDER_SOURCE_ONLY_IDENTITY_LIFECYCLE_NOT_CERTIFIED"
_FIELDS = {"schema", "cohort_sha256", "candidate_company_id", "symbol", "start_ms",
           "end_exclusive_ms", "multiplier", "timespan", "adjusted", "sort", "limit",
           "max_pages", "max_response_bytes", "qualification", "normalized_price_activation",
           "request_sha256"}
_CRED_NAMES = {"apiKey", "api_key", "apikey"}

class SourceError(Exception):
    """Only a constant error code; never a raw URL, key, response or exception."""

class UnknownOutcome(SourceError):
    """A request or commit may have happened: reconcile before another attempt."""

def canonical(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
                      allow_nan=False).encode("utf-8")

def digest(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()

def validate_manifest(m: Mapping[str, Any]) -> None:
    if not isinstance(m, dict) or set(m) != _FIELDS:
        raise SourceError("MANIFEST_SHAPE")
    if m["schema"] != "RAW_MINUTE_MONTH_V1" or m["cohort_sha256"] != COHORT:
        raise SourceError("MANIFEST_COHORT")
    if not isinstance(m["candidate_company_id"], str) or not re.fullmatch(r"CIK:\d{10}", m["candidate_company_id"]):
        raise SourceError("MANIFEST_COMPANY")
    if not isinstance(m["symbol"], str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9.\^-]{0,31}", m["symbol"]):
        raise SourceError("MANIFEST_SYMBOL")
    if type(m["start_ms"]) is not int or type(m["end_exclusive_ms"]) is not int or (m["start_ms"], m["end_exclusive_ms"]) not in WINDOWS:
        raise SourceError("MANIFEST_WINDOW")
    if (type(m["multiplier"]) is not int or m["multiplier"] != 1 or m["timespan"] != "minute"
            or m["adjusted"] is not False or m["sort"] != "asc" or type(m["limit"]) is not int
            or m["limit"] != 50_000):
        raise SourceError("MANIFEST_PRICE_BASIS")
    if (type(m["max_pages"]) is not int or not 1 <= m["max_pages"] <= 4
            or type(m["max_response_bytes"]) is not int or not 1 <= m["max_response_bytes"] <= MAX_BYTES):
        raise SourceError("MANIFEST_BOUNDS")
    if m["qualification"] != QUALIFICATION or m["normalized_price_activation"] is not False:
        raise SourceError("MANIFEST_QUALIFICATION")
    if m["request_sha256"] != digest(canonical({k: v for k, v in m.items() if k != "request_sha256"})):
        raise SourceError("MANIFEST_HASH")

def source_path(m: Mapping[str, Any]) -> str:
    return f'/v2/aggs/ticker/{quote(m["symbol"], safe="")}/range/1/minute/{m["start_ms"]}/{m["end_exclusive_ms"] - 1}'

def first_url(m: Mapping[str, Any]) -> str:
    validate_manifest(m)
    return ORIGIN + source_path(m) + "?adjusted=false&sort=asc&limit=50000"

def clean_next(value: Any, m: Mapping[str, Any]) -> str | None:
    if value is None or value == "":
        return None
    if not isinstance(value, str) or len(value) > 16_384 or any(ord(c) < 32 or ord(c) == 127 for c in value):
        raise SourceError("NEXT_URL_SHAPE")
    try:
        u = urlsplit(value)
        if u.scheme != "https" or u.netloc != "api.massive.com" or u.path != source_path(m) or u.fragment:
            raise SourceError("NEXT_URL_SCOPE")
        pairs = parse_qsl(u.query, keep_blank_values=True, strict_parsing=True)
    except SourceError:
        raise
    except Exception:
        raise SourceError("NEXT_URL_SHAPE") from None
    seen: set[str] = set()
    cursor = None
    allowed = {"adjusted": "false", "sort": "asc", "limit": "50000"}
    for key, val in pairs:
        if key in seen:
            raise SourceError("NEXT_URL_DUPLICATE_PARAMETER")
        seen.add(key)
        if key in _CRED_NAMES:
            continue
        if key == "cursor":
            cursor = val
        elif key not in allowed or val != allowed[key]:
            raise SourceError("NEXT_URL_PARAMETER")
    if not cursor or len(cursor) > 8192 or any(ord(c) <= 32 or ord(c) == 127 for c in cursor):
        raise SourceError("NEXT_URL_CURSOR")
    return ORIGIN + source_path(m) + "?" + urlencode({"cursor": cursor})

def validate_page_url(url: str, m: Mapping[str, Any], page: int) -> None:
    if type(page) is not int or not 1 <= page <= m["max_pages"]:
        raise SourceError("PAGE_LIMIT")
    if page == 1:
        if url != first_url(m):
            raise SourceError("FIRST_URL_MISMATCH")
    elif clean_next(url, m) != url:
        raise SourceError("CURSOR_URL_NOT_CANONICAL")

def _pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    d = {}
    for key, value in pairs:
        if key in d:
            raise SourceError("DUPLICATE_JSON_KEY")
        d[key] = value
    return d

def _constant(_: str) -> Any:
    raise SourceError("NONFINITE_JSON")

_DECODER = json.JSONDecoder(parse_float=Decimal, object_pairs_hook=_pairs, parse_constant=_constant)

def _depth_and_secret(v: Any, key: str, depth: int = 0) -> None:
    if depth > 64:
        raise SourceError("JSON_DEPTH")
    if isinstance(v, str):
        if key in v or quote(key, safe="") in v:
            raise SourceError("CREDENTIAL_ECHO")
    elif isinstance(v, list):
        for item in v:
            _depth_and_secret(item, key, depth + 1)
    elif isinstance(v, dict):
        for k, item in v.items():
            _depth_and_secret(k, key, depth + 1)
            _depth_and_secret(item, key, depth + 1)

def _top_level_span(text: str, target: str) -> tuple[int, int] | None:
    """Find a top-level value using the same strict decoder; keep result lexemes."""
    i = 0
    def ws(j: int) -> int:
        while j < len(text) and text[j] in " \t\r\n":
            j += 1
        return j
    i = ws(i)
    if i >= len(text) or text[i] != "{":
        raise SourceError("JSON_OBJECT_REQUIRED")
    i += 1
    found = None
    while True:
        i = ws(i)
        if i < len(text) and text[i] == "}":
            return found
        k, i = _DECODER.raw_decode(text, i)
        i = ws(i)
        if not isinstance(k, str) or text[i] != ":":
            raise SourceError("INVALID_JSON")
        start = ws(i + 1)
        _, end = _DECODER.raw_decode(text, start)
        if k == target:
            found = start, end
        i = ws(end)
        if text[i] == ",":
            i += 1
        elif text[i] == "}":
            return found
        else:
            raise SourceError("INVALID_JSON")

@dataclass(frozen=True)
class RetainedPage:
    stored_bytes: bytes
    source_sha256: str
    wire_sha256: str
    wire_bytes: int
    result_count: int
    results_presence: str
    next_url: str | None
    terminal: bool
    observation_timestamp_errors: int
    duplicate_timestamps: int
    source_rows_in_timestamp_order: bool

    def safe_metadata(self) -> dict[str, Any]:
        return {"source_sha256": self.source_sha256, "wire_sha256": self.wire_sha256,
                "wire_bytes": self.wire_bytes, "stored_bytes": len(self.stored_bytes),
                "result_count": self.result_count, "results_presence": self.results_presence,
                "next_url": self.next_url, "terminal": self.terminal,
                "observation_timestamp_errors": self.observation_timestamp_errors,
                "duplicate_timestamps": self.duplicate_timestamps,
                "source_rows_in_timestamp_order": self.source_rows_in_timestamp_order,
                "qualification": QUALIFICATION, "original_historical_receipt_verified": False,
                "identity_lifecycle_qualified": False, "no_trade_inferred": False,
                "family_accepted": False, "normalized_values_created": 0}

def retain_envelope(raw: bytes, m: Mapping[str, Any], key: str, current_url: str) -> RetainedPage:
    validate_manifest(m)
    if not isinstance(key, str) or len(key) < 8 or any(ord(c) <= 32 or ord(c) == 127 for c in key):
        raise SourceError("CREDENTIAL_UNAVAILABLE")
    if not isinstance(raw, bytes) or not 0 < len(raw) <= m["max_response_bytes"]:
        raise SourceError("RESPONSE_SIZE")
    try:
        text = raw.decode("utf-8", errors="strict")
        obj = _DECODER.decode(text)
    except SourceError:
        raise
    except (ValueError, UnicodeError, RecursionError):
        raise SourceError("INVALID_JSON") from None
    if not isinstance(obj, dict) or obj.get("status") != "OK" or obj.get("ticker") != m["symbol"] or obj.get("adjusted") is not False:
        raise SourceError("SOURCE_ENVELOPE")
    if "results" in obj:
        rows = obj["results"]
        if not isinstance(rows, list):
            raise SourceError("SOURCE_RESULTS")
        presence = "ARRAY"
    elif type(obj.get("resultsCount")) is int and obj["resultsCount"] == 0:
        rows, presence = [], "OMITTED_WITH_EXPLICIT_ZERO"
    else:
        raise SourceError("SOURCE_RESULTS")
    if len(rows) > MAX_PAGE_RECORDS or any(not isinstance(r, dict) for r in rows):
        raise SourceError("SOURCE_RECORDS")
    for field in ("resultsCount", "queryCount"):
        if field in obj and (type(obj[field]) is not int or obj[field] < 0):
            raise SourceError("SOURCE_COUNTS")
    if "resultsCount" in obj and obj["resultsCount"] != len(rows):
        raise SourceError("SOURCE_COUNTS")
    nxt = clean_next(obj.get("next_url"), m)
    if nxt is not None and nxt == current_url:
        raise SourceError("REPEATED_CURSOR")
    if "next_url" in obj:
        span = _top_level_span(text, "next_url")
        if span is None:
            raise SourceError("NEXT_SPAN")
        text = text[:span[0]] + json.dumps(nxt, separators=(",", ":")) + text[span[1]:]
    sanitized = _DECODER.decode(text)
    _depth_and_secret(sanitized, key)
    retained = text.encode("utf-8")
    if len(retained) > m["max_response_bytes"]:
        raise SourceError("RETAINED_SIZE")
    ts = [r.get("t") for r in rows]
    good = [t for t in ts if type(t) is int and m["start_ms"] <= t < m["end_exclusive_ms"] and t % 60000 == 0]
    return RetainedPage(retained, digest(retained), digest(raw), len(raw), len(rows), presence,
                        nxt, nxt is None, len(ts) - len(good), len(good) - len(set(good)),
                        len(good) == len(ts) and good == sorted(good))

@dataclass(frozen=True)
class Reservation:
    manifest_sha256: str
    request_url: str
    page_number: int
    attempt_id: str
    reserved_bytes: int
    expiry_epoch: float
    reserved: bool
    owner_fence: int
    stop_generation: int

@dataclass(frozen=True)
class HttpResponse:
    status: int
    body: bytes
    redirected: bool = False

def acquire_page(m: Mapping[str, Any], reservation: Reservation, credential: str,
                 transport: Callable[[str, Mapping[str, str], int, float], HttpResponse],
                 commit: Callable[[RetainedPage, Reservation], Any],
                 failure: Callable[[str, Reservation], Any], *, now: Callable[[], float] = time.time) -> Any:
    """One admitted GET; the adapter must atomically claim and enforce live stops.

    transport MUST enforce the supplied byte bound while streaming, disable
    redirects/retries and respect the deadline. Its URL must never be logged.
    commit MUST check current owner/stop/lease/capacity and source replay equality.
    Unknown transfer or commit outcomes deliberately retain the reservation.
    """
    validate_manifest(m)
    if (not isinstance(reservation, Reservation) or reservation.reserved is not True
            or reservation.manifest_sha256 != m["request_sha256"]
            or type(reservation.owner_fence) is not int or reservation.owner_fence != 3
            or type(reservation.stop_generation) is not int or reservation.stop_generation != 1
            or type(reservation.reserved_bytes) is not int or reservation.reserved_bytes != m["max_response_bytes"]
            or type(reservation.expiry_epoch) not in (int, float) or not math.isfinite(reservation.expiry_epoch)
            or reservation.expiry_epoch <= now() + 2 or not isinstance(reservation.attempt_id, str)
            or not re.fullmatch(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", reservation.attempt_id)):
        raise SourceError("CURRENT_RESERVATION_REQUIRED")
    validate_page_url(reservation.request_url, m, reservation.page_number)
    if not isinstance(credential, str) or len(credential) < 8 or any(ord(c) <= 32 or ord(c) == 127 for c in credential):
        raise SourceError("CREDENTIAL_UNAVAILABLE")
    target = reservation.request_url + "&" + urlencode({"apiKey": credential})
    try:
        response = transport(target, {"Accept": "application/json"}, m["max_response_bytes"], min(reservation.expiry_epoch - 1, now() + 25))
    except Exception:
        raise UnknownOutcome("TRANSPORT_OUTCOME_REQUIRES_RECONCILIATION") from None
    if (not isinstance(response, HttpResponse) or type(response.status) is not int
            or not 100 <= response.status <= 599 or type(response.redirected) is not bool):
        raise UnknownOutcome("INVALID_TRANSPORT_RECEIPT_REQUIRES_RECONCILIATION")
    if response.redirected:
        failure("REDIRECT_REJECTED", reservation)
        raise SourceError("REDIRECT_REJECTED")
    if response.status != 200:
        code = "HTTP_" + str(response.status) if type(response.status) is int and 100 <= response.status <= 599 else "INVALID_HTTP_STATUS"
        failure(code, reservation)
        raise SourceError(code)
    try:
        page = retain_envelope(response.body, m, credential, reservation.request_url)
    except SourceError as exc:
        # A known, fully returned invalid response is classified but never retried.
        failure(str(exc), reservation)
        raise
    try:
        if now() >= reservation.expiry_epoch:
            raise UnknownOutcome("LEASE_EXPIRED_BEFORE_COMMIT")
        return commit(page, reservation)
    except Exception:
        # A possibly committed response MUST NOT be released/refetched blindly.
        raise UnknownOutcome("COMMIT_OUTCOME_REQUIRES_RECONCILIATION") from None
