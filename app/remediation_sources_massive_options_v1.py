"""Finite intended-cohort native options reference and minute trade-bar pilot.

Every raw response is retained by the existing worker before this parser runs.
Reference pages remain raw-only; selected typed contract metadata is a locator
into those bytes, not an original HTTP reconstruction. Minute bars retain the
provider's O: identifier. No quotes, OI, IV, Greeks, or historical receipt claim.
"""
from __future__ import annotations

import hashlib
import json
import re
from collections import Counter
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from urllib.parse import parse_qsl, urlencode, urlsplit
from zoneinfo import ZoneInfo

VERSION = "massive_native_options_pilot_20261008_v1"
PILOT_ID = "intended_cohort_options_native_pilot_20261008_v1"
RUN_ID = "market_data_remediation_20261008_v1"
FROZEN_UNIVERSE_RUN_ID = "8ad3d16b-854e-443d-bb65-a6aa3c7a1719"
REFERENCE = "massive_option_reference"
MINUTES = "massive_option_minutes"
KINDS = {REFERENCE, MINUTES}
ORIGIN = "https://api.massive.com"
REFERENCE_PATH = "/v3/reference/options/contracts"
MAX_PAGES = 4
MAX_REFERENCE_ROWS = 1000
MAX_MINUTE_ROWS = 1440
MAX_SOURCE_BODY_BYTES = 1024 * 1024
MAX_ATTEMPTS = 1
UTC = timezone.utc
NY = ZoneInfo("America/New_York")
SHA_RE = r"[a-f0-9]{64}"
CURSOR_RE = r"[A-Za-z0-9_+/=-]{1,8192}"
NATIVE_RE = r"O:[A-Z][A-Z0-9._-]{0,19}[0-9]{6}[CP][0-9]{8}"
STANDARD_RE = re.compile(r"O:(?P<root>[A-Z][A-Z0-9]{0,5})(?P<expiry>[0-9]{6})(?P<right>[CP])(?P<strike>[0-9]{8})")
MINUTE_BASIS = "MINUTE_COMPLETION_LOWER_BOUND_NOT_PUBLICATION"
IDENTITY_BASIS = "FROZEN_STRONG_DATED_STATE_AND_PROVIDER_ASOF_UNDERLYING_MATCH_PENDING_NATIVE_EQUITY_RELEASE_VERIFICATION"
PILOTS = [
  {
    "candidate_id": "392f9bffcacad7b7a58ec03c84ef8140e94e90b356d76d787704afb10176e6c3",
    "instrument_key": 12047,
    "historical_state_id": 30508,
    "session_date": "2025-09-29",
    "historical_state_symbol": "UPST"
  },
  {
    "candidate_id": "b7ccb7abe93de09546baa666b03355479551bc67e2c2d74a7defd50280f81686",
    "instrument_key": 6166,
    "historical_state_id": 23477,
    "session_date": "2025-10-08",
    "historical_state_symbol": "IRD"
  },
  {
    "candidate_id": "afe0c4f3e0ae4dbfd426858a503aa6dfee9071bef405c91947e6d8e49359da21",
    "instrument_key": 13218,
    "historical_state_id": 32003,
    "session_date": "2025-11-28",
    "historical_state_symbol": "ZYME"
  },
  {
    "candidate_id": "e1f2a07f9abcf7768b1c61fba66a6f5ebf58d3673cd282f3d7a9d02d79716699",
    "instrument_key": 1334,
    "historical_state_id": 17542,
    "session_date": "2025-12-31",
    "historical_state_symbol": "BFH"
  },
  {
    "candidate_id": "9d88f2b9eaa18f2902ebdc7e792c3f85326a6021ed5f8c82122475510f27ddad",
    "instrument_key": 12267,
    "historical_state_id": 30769,
    "session_date": "2026-01-16",
    "historical_state_symbol": "VERI"
  },
  {
    "candidate_id": "1d009a6da92d33f39a6a8723b2621461514da0e0deb83b45d717c28683376e7c",
    "instrument_key": 10228,
    "historical_state_id": 28290,
    "session_date": "2026-02-02",
    "historical_state_symbol": "SAFT"
  },
  {
    "candidate_id": "6820e2108a87f03e78e2860c89be43acbcaa87638d2c75c2ca58f881f59ba5cf",
    "instrument_key": 7694,
    "historical_state_id": 25262,
    "session_date": "2026-03-18",
    "historical_state_symbol": "MRAM"
  },
  {
    "candidate_id": "cdcd01811ce03ccbf260a87192b1a6b3be3cd6516b3f910cbd084dce74b1e910",
    "instrument_key": 13117,
    "historical_state_id": 31876,
    "session_date": "2026-04-21",
    "historical_state_symbol": "YSS"
  },
  {
    "candidate_id": "7bf02e5baa2bfa2fbfd1d4e347c749838113ce19ae61d555cefa828241abdaad",
    "instrument_key": 13118,
    "historical_state_id": 31877,
    "session_date": "2026-05-22",
    "historical_state_symbol": "YSWY"
  },
  {
    "candidate_id": "0722102376181b693c71f52e0801dfb68d2e1025d6c0664ae1195fe3f66e7d13",
    "instrument_key": 12629,
    "historical_state_id": 31216,
    "session_date": "2026-06-03",
    "historical_state_symbol": "WFCF"
  },
  {
    "candidate_id": "163f0b4c72cd3b29c99fe099cc36ea4db8261f5fb1664c14eb37d76514bf78ef",
    "instrument_key": 6660,
    "historical_state_id": 24057,
    "session_date": "2026-07-07",
    "historical_state_symbol": "KGS"
  }
]
BY_ID = {item["candidate_id"]:item for item in PILOTS}
EXPECTED_HISTORICAL_FILTER_PROBES = {"392f9bffcacad7b7a58ec03c84ef8140e94e90b356d76d787704afb10176e6c3":"O:UPST251010C00058000"}


def sha(value):
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def stamp(value):
    result = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if result.tzinfo is None:
        raise ValueError("Explicit timezone required")
    return result.astimezone(UTC)


def iso(value):
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def day_bounds(day):
    start = datetime.combine(date.fromisoformat(day), datetime.min.time(), NY)
    return start.astimezone(UTC), (start + timedelta(days=1)).astimezone(UTC)


def reference_key(candidate_id, page):
    return f"massive:options:pilot-v1:{candidate_id}:reference:p{page:02d}"


def minute_key(candidate_id, symbol):
    return f"massive:options:pilot-v1:{candidate_id}:minutes"


RECOVERY_VERSION = "massive_options_429_recovery_20261008_v1"
RECOVERY_CAPABILITY = "massive_option_minutes_429_recovery"
RECOVERY_CONTRACTS = {
    "1d009a6da92d33f39a6a8723b2621461514da0e0deb83b45d717c28683376e7c": {
        "recovery_id": "massive_options_429_recovery_20261008_v1",
        "required_recovery_version": "massive_options_429_recovery_20261008_v1",
        "parent_batch_key": "massive:options:pilot-v1:1d009a6da92d33f39a6a8723b2621461514da0e0deb83b45d717c28683376e7c:minutes",
        "parent_batch_id": 837311,
        "parent_error_source_id": "594d1d6a-8caf-443d-9896-ed6efb679ac9",
        "parent_error_source_sha256": "456268359390ff35773c2ddd1ea119a3ac3d58a7e9ac6ba5371522cd4d0a5a64",
        "parent_http_status": 429,
        "authorized_additional_http_attempts": 1
    },
    "6820e2108a87f03e78e2860c89be43acbcaa87638d2c75c2ca58f881f59ba5cf": {
        "recovery_id": "massive_options_429_recovery_20261008_v1",
        "required_recovery_version": "massive_options_429_recovery_20261008_v1",
        "parent_batch_key": "massive:options:pilot-v1:6820e2108a87f03e78e2860c89be43acbcaa87638d2c75c2ca58f881f59ba5cf:minutes",
        "parent_batch_id": 837312,
        "parent_error_source_id": "27e89246-376e-4239-a4e0-0e785cb136e3",
        "parent_error_source_sha256": "7fd04f9c2a4b9ed87da30c6da9084073d9177cb212504988983fff5c7d255a83",
        "parent_http_status": 429,
        "authorized_additional_http_attempts": 1
    },
    "cdcd01811ce03ccbf260a87192b1a6b3be3cd6516b3f910cbd084dce74b1e910": {
        "recovery_id": "massive_options_429_recovery_20261008_v1",
        "required_recovery_version": "massive_options_429_recovery_20261008_v1",
        "parent_batch_key": "massive:options:pilot-v1:cdcd01811ce03ccbf260a87192b1a6b3be3cd6516b3f910cbd084dce74b1e910:minutes",
        "parent_batch_id": 837313,
        "parent_error_source_id": "adccba5f-a331-41e2-afbb-eb107d06051e",
        "parent_error_source_sha256": "44cf97d614b66ac6fcd8354019fa7884b96efb94ca5f29fd2d4a6e628a9c274b",
        "parent_http_status": 429,
        "authorized_additional_http_attempts": 1
    }
}
# Additional attempts have distinct keys; the original one-attempt pilot is immutable.
def recovery_request_valid(task, request):
    recovery = request.get("recovery")
    if recovery is None:
        return request.get("minimum_source_request_spacing_seconds", 0) == 0
    expected = RECOVERY_CONTRACTS.get(request.get("candidate_id"))
    return (task.get("source_type") == MINUTES and expected is not None
            and isinstance(recovery, dict) and recovery == expected
            and all(type(recovery.get(k)) is type(v) for k,v in expected.items())
            and type(request.get("minimum_source_request_spacing_seconds")) is int
            and request["minimum_source_request_spacing_seconds"] == 20)

def reference_params(pilot):
    day = date.fromisoformat(pilot["session_date"])
    return {"underlying_ticker":pilot["historical_state_symbol"], "as_of":str(day), "expired":"false",
            "expiration_date.gte":str(day+timedelta(days=7)), "expiration_date.lte":str(day+timedelta(days=90)),
            "sort":"ticker", "order":"asc", "limit":1000}


def _number(value, nonnegative=False, positive=False, integral=False):
    if isinstance(value, bool) or not isinstance(value, (str, int, float, Decimal)):
        raise ValueError("Native finite number required")
    if isinstance(value, str) and not re.fullmatch(r"[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?", value):
        raise ValueError("Native numeric spelling rejected")
    try:
        result = Decimal(str(value))
    except (ValueError, InvalidOperation):
        raise ValueError("Native numeric value rejected") from None
    if not result.is_finite() or (nonnegative and result < 0) or (positive and result <= 0):
        raise ValueError("Native number out of bounds")
    if abs(result) > Decimal("1e18") or result.as_tuple().exponent < -18:
        raise ValueError("Native number exceeds finite staging precision bound")
    if integral and result != result.to_integral_value():
        raise ValueError("Native integer quantity required")
    return result


def _unique_object(pairs):
    result = {}
    for key,value in pairs:
        if key in result:
            raise ValueError("Duplicate source object key")
        result[key] = value
    return result


def _payload(raw):
    if isinstance(raw, (bytes,str)):
        if len(raw.encode("utf-8") if isinstance(raw,str) else raw) > MAX_SOURCE_BODY_BYTES:
            raise ValueError("Bounded native response was exceeded")
        return json.loads(raw, parse_float=Decimal, object_pairs_hook=_unique_object)
    return raw


def settings(task):
    request = task.get("request_json") or {}
    if isinstance(request,str):
        request = json.loads(request)
    pilot = BY_ID.get(request.get("candidate_id"))
    kind = task.get("source_type")
    if not pilot or task.get("run_id") != RUN_ID or task.get("provider") != "massive" or kind not in KINDS:
        raise ValueError("Exact intended-cohort pilot required")
    required = {"pilot_id":PILOT_ID, "required_parser_version":VERSION, "max_attempts":MAX_ATTEMPTS,
                "max_source_body_bytes":MAX_SOURCE_BODY_BYTES, "max_reference_pages":MAX_PAGES,
                "source_price_usd":0, "price_status":"INCLUDED_NO_INCREMENTAL_CHARGE",
                "acquisition_mode":"FETCH", "historical_first_receipt_recovered":False,
                "strict_historical_replay_eligible":False, "source_coverage_complete":False,
                "underlying_identity_basis":IDENTITY_BASIS, "frozen_universe_run_id":FROZEN_UNIVERSE_RUN_ID}
    for key,expected in required.items():
        if type(request.get(key)) is not type(expected) or request[key] != expected:
            raise ValueError("Exact bounded source and timing contract required")
    if not isinstance(request.get("price_evidence"),str) or not request["price_evidence"]:
        raise ValueError("Existing entitlement evidence required")
    if request.get("cohort_generation_id") is not None or request.get("warmup_contract") is not None:
        raise ValueError("Options pilot cannot change cohort or warmup scope")
    if task.get("instrument_id") is not None:
        raise ValueError("Underlying context is an integer key, not an invented UUID")
    if not recovery_request_valid(task,request):
        raise ValueError("Exact separately capped native 429 recovery required")
    start,end = stamp(task["start_ts"]),stamp(task["end_ts"])
    if kind == REFERENCE:
        expected_start = stamp(pilot["session_date"]+"T00:00:00Z")
        page = request.get("page_number")
        if type(page) is not int or not 1 <= page <= MAX_PAGES:
            raise ValueError("Finite reference page required")
        if (start,end,task.get("interval_seconds"),task.get("symbol"),task.get("batch_key")) != (
            expected_start,expected_start+timedelta(days=1),0,pilot["historical_state_symbol"],
            reference_key(pilot["candidate_id"],page)):
            raise ValueError("Dated reference scope changed")
        cursor = request.get("cursor")
        ancestors = request.get("ancestor_cursor_sha256",[])
        if not isinstance(ancestors,list) or len(ancestors) != max(0,page-2) or len(ancestors) != len(set(ancestors)):
            raise ValueError("Cursor ancestry required")
        if any(not isinstance(value,str) or not re.fullmatch(SHA_RE,value) for value in ancestors):
            raise ValueError("Cursor ancestry hash rejected")
        if page == 1:
            if cursor is not None or any(request.get(key) is not None for key in (
                "parent_batch_key","parent_source_sha256","previous_last_native_ticker")):
                raise ValueError("Initial reference page must have no parent")
        else:
            if not isinstance(cursor,str) or not re.fullmatch(CURSOR_RE,cursor) or sha(cursor) in ancestors:
                raise ValueError("Exact acyclic source cursor required")
            if request.get("parent_batch_key") != reference_key(pilot["candidate_id"],page-1):
                raise ValueError("Exact preceding page required")
            if not re.fullmatch(SHA_RE,str(request.get("parent_source_sha256",""))):
                raise ValueError("Parent retained source hash required")
            if not re.fullmatch(NATIVE_RE,str(request.get("previous_last_native_ticker",""))):
                raise ValueError("Native page boundary required")
    else:
        selected = request.get("selected_contract")
        if not isinstance(selected,dict) or not selected_contract_valid(selected,pilot):
            raise ValueError("Exact standard native contract reference required")
        symbol = selected["native_symbol"]
        if (start,end) != day_bounds(pilot["session_date"]) or task.get("interval_seconds") != 60:
            raise ValueError("One native Eastern-date minute request required")
        expected_key = minute_key(pilot["candidate_id"],symbol)
        if request.get("recovery") is not None:
            expected_key += ":recovery:429-v1"
        if task.get("symbol") != symbol or task.get("batch_key") != expected_key:
            raise ValueError("Native contract request mismatch")
        if request.get("reference_chain_complete") is not True or request.get("selection_rule") != "LOWEST_SHA256_NATIVE_TICKER_ACROSS_COMPLETE_REFERENCE_CHAIN":
            raise ValueError("Complete source-independent selection required")
        for key in ("reference_source_sha256","reference_chain_sha256"):
            if not re.fullmatch(SHA_RE,str(request.get(key,""))):
                raise ValueError("Retained reference chain hash required")
        if not re.fullmatch(r"[a-f0-9-]{36}",str(request.get("reference_source_id",""))):
            raise ValueError("Retained reference source identity required")
    return kind,request,pilot,start,end


def validate_url(url):
    try:
        parsed = urlsplit(url)
        if parsed.scheme != "https" or parsed.netloc != "api.massive.com" or parsed.fragment:
            return False
        pairs = parse_qsl(parsed.query,keep_blank_values=True,strict_parsing=True)
        if len(pairs) != len(dict(pairs)):
            return False
        params = dict(pairs)
        if parsed.path == REFERENCE_PATH:
            if set(params) == {"cursor"}:
                return bool(re.fullmatch(CURSOR_RE,params["cursor"]))
            return any(params == {key:str(value) for key,value in reference_params(pilot).items()} for pilot in PILOTS)
        match = re.fullmatch(r"/v2/aggs/ticker/(O:[A-Z][A-Z0-9._-]{0,19}[0-9]{6}[CP][0-9]{8})/range/1/minute/([0-9]{4}-[0-9]{2}-[0-9]{2})/\2",parsed.path)
        return bool(match and match[2] in {pilot["session_date"] for pilot in PILOTS} and
                    params == {"adjusted":"false","sort":"asc","limit":"50000"})
    except (ValueError,TypeError):
        return False


def build_requests(task):
    kind,request,pilot,_,_ = settings(task)
    if kind == REFERENCE:
        params = reference_params(pilot) if request["page_number"] == 1 else {"cursor":request["cursor"]}
        url = ORIGIN + REFERENCE_PATH + "?" + urlencode(params)
    else:
        url = f'{ORIGIN}/v2/aggs/ticker/{task["symbol"]}/range/1/minute/{pilot["session_date"]}/{pilot["session_date"]}?adjusted=false&sort=asc&limit=50000'
    if not validate_url(url):
        raise ValueError("Exact pilot URL rejected")
    return [{"role":"candles","url":url}]


def selected_contract_valid(selected,pilot):
    try:
        symbol = selected["native_symbol"]
        match = STANDARD_RE.fullmatch(symbol)
        expiry = date.fromisoformat(selected["expiry"])
        day = date.fromisoformat(pilot["session_date"])
        return bool(match and match["root"] == pilot["historical_state_symbol"] and
                    selected["underlying_native_symbol"] == pilot["historical_state_symbol"] and
                    expiry.strftime("%y%m%d") == match["expiry"] and
                    selected["option_right"] == match["right"] and
                    _number(selected["strike"],nonnegative=True) == Decimal(match["strike"])/1000 and
                    7 <= (expiry-day).days <= 90 and selected["shares_per_contract"] == 100 and
                    selected["additional_underlyings"] == [] and selected["standard_contract_eligible"] is True and
                    type(selected["source_row_ordinal"]) is int and 0 <= selected["source_row_ordinal"] < MAX_REFERENCE_ROWS and
                    selected["native_ticker_sha256"] == sha(symbol))
    except (KeyError,TypeError,ValueError,InvalidOperation):
        return False


def _selected_reference(row,ordinal,pilot):
    reasons = []
    symbol = row["ticker"]
    match = STANDARD_RE.fullmatch(symbol)
    expiry = None
    try:
        expiry = date.fromisoformat(row["expiration_date"])
        if not 7 <= (expiry-date.fromisoformat(pilot["session_date"])).days <= 90:
            reasons.append("OUTSIDE_DTE_7_90")
    except (ValueError,KeyError,TypeError):
        reasons.append("EXPIRY_UNRESOLVED")
    right = {"call":"C","put":"P"}.get(row.get("contract_type"))
    if right is None:
        reasons.append("UNSUPPORTED_CONTRACT_TYPE")
    try:
        strike = _number(row["strike_price"],nonnegative=True)
    except (ValueError,KeyError,TypeError):
        strike = None
        reasons.append("STRIKE_UNRESOLVED")
    if not match or match["root"] != pilot["historical_state_symbol"]:
        reasons.append("NONSTANDARD_OR_UNRESOLVED_OCC_ROOT")
    if match and expiry and expiry.strftime("%y%m%d") != match["expiry"]:
        reasons.append("OCC_EXPIRY_MISMATCH")
    if match and right and right != match["right"]:
        reasons.append("OCC_RIGHT_MISMATCH")
    if match and strike is not None and strike != Decimal(match["strike"])/1000:
        reasons.append("OCC_STRIKE_MISMATCH")
    if isinstance(row.get("shares_per_contract"),bool) or row.get("shares_per_contract") != 100:
        reasons.append("NONSTANDARD_OR_UNRESOLVED_SHARES")
    additional = row.get("additional_underlyings",[])
    if additional != []:
        reasons.append("ADDITIONAL_OR_UNRESOLVED_DELIVERABLES")
    if reasons:
        return None,reasons
    selected = {
        "native_symbol":symbol,"underlying_native_symbol":row["underlying_ticker"],"expiry":str(expiry),
        "option_right":right,"strike":str(strike),"shares_per_contract":100,"additional_underlyings":[],
        "standard_contract_eligible":True,"source_row_ordinal":ordinal,"native_ticker_sha256":sha(symbol),
        "primary_exchange":row.get("primary_exchange"),"exercise_style":row.get("exercise_style"),
        "native_correction_number":row.get("correction"),"reference_date":pilot["session_date"],
    }
    # Optional metadata is a locator/context, not a raw-payload reconstruction.
    for key in ("primary_exchange","exercise_style"):
        if selected[key] is not None and (not isinstance(selected[key],str) or len(selected[key]) > 80):
            raise ValueError("Native contract metadata label rejected")
    correction = selected["native_correction_number"]
    if correction is not None and (isinstance(correction,bool) or not isinstance(correction,int) or correction < 0):
        raise ValueError("Native contract correction rejected")
    if not selected_contract_valid(selected,pilot):
        raise ValueError("Selected native contract inconsistent")
    return selected,[]


def _base_validation():
    return {"raw_count":0,"valid_count":0,"invalid_count":0,"duplicate_count":0,"outside_count":0,
            "duplicate_conflict_count":0,"identity_ineligible_count":0,"normalization_passed":True,
            "parser_version":VERSION,"historical_publication_recovered":False,
            "historical_first_receipt_recovered":False,"strict_historical_replay_eligible":False,
            "source_coverage_complete":False,"retained_original_revision_history_complete":False,
            "underlying_identity_basis":IDENTITY_BASIS,"native_equity_identity_release_verified":False,
            "quotes_obtained":False,"open_interest_obtained":False,"implied_volatility_obtained":False,"greeks_obtained":False}


def parse_reference(task,payload,request,pilot):
    native = payload.get("results",[])
    if not isinstance(native,list) or len(native) > MAX_REFERENCE_ROWS:
        raise ValueError("Native reference page population rejected")
    if "count" in payload and (type(payload["count"]) is not int or payload["count"] != len(native)):
        raise ValueError("Native reference count disagrees with retained rows")
    symbols,eligible,reasons = [],[],Counter()
    for ordinal,row in enumerate(native):
        if not isinstance(row,dict) or not isinstance(row.get("ticker"),str) or not re.fullmatch(NATIVE_RE,row["ticker"]):
            raise ValueError("Native reference identity rejected")
        if row.get("underlying_ticker") != pilot["historical_state_symbol"]:
            raise ValueError("Source returned an unrequested native underlying")
        symbols.append(row["ticker"])
        selected,exclusions = _selected_reference(row,ordinal,pilot)
        if selected is not None:
            eligible.append(selected)
        reasons.update(exclusions)
    if symbols != sorted(symbols) or len(symbols) != len(set(symbols)):
        raise ValueError("Native reference order or duplicate identity rejected")
    page = request["page_number"]
    if symbols and page > 1 and symbols[0] <= request["previous_last_native_ticker"]:
        raise ValueError("Native reference pages overlap")
    next_url = payload.get("next_url") or None
    next_cursor = None
    if next_url is not None:
        if not isinstance(next_url,str) or not validate_url(next_url) or urlsplit(next_url).path != REFERENCE_PATH:
            raise ValueError("Exact native reference continuation rejected")
        params = dict(parse_qsl(urlsplit(next_url).query,keep_blank_values=True))
        if set(params) != {"cursor"} or not symbols:
            raise ValueError("Opaque native cursor and nonempty page required")
        next_cursor = params["cursor"]
        if sha(next_cursor) in request.get("ancestor_cursor_sha256",[]) or next_cursor == request.get("cursor"):
            raise ValueError("Native reference cursor cycle")
    chosen = min(eligible,key=lambda item:item["native_ticker_sha256"]) if eligible else None
    validation = {
        **_base_validation(),"source_record_grain":"RAW_ONLY_DATED_NATIVE_OPTION_REFERENCE",
        "source_native_reference_rows":len(native),"source_unique_native_tickers":len(symbols),
        "source_standard_contracts":len(eligible),"source_nonstandard_or_unresolved_contracts":len(native)-len(eligible),
        "source_exclusion_reason_counts":dict(reasons),"selected_contract":chosen,
        "selection_rule":"LOWEST_SHA256_NATIVE_TICKER_WITHIN_NATIVE_REFERENCE_PAGE",
        "source_reference_date":pilot["session_date"],"candidate_id":pilot["candidate_id"],
        "source_page_number":page,"source_page_complete":True,"source_terminal_page":next_cursor is None,
        "source_page_limit_reached":page == MAX_PAGES and next_cursor is not None,
        "source_native_order_verified":True,"first_native_ticker":symbols[0] if symbols else None,
        "last_native_ticker":symbols[-1] if symbols else None,
        "source_cursor_sha256":sha(request["cursor"]) if request.get("cursor") else None,
        "next_url":next_url,"next_cursor":next_cursor,"next_cursor_sha256":sha(next_cursor) if next_cursor else None,
        "source_expired_filter":False,
        "source_expired_filter_probe_expected_ticker":EXPECTED_HISTORICAL_FILTER_PROBES.get(pilot["candidate_id"]),
        "source_expired_filter_expected_ticker_seen":(
            EXPECTED_HISTORICAL_FILTER_PROBES[pilot["candidate_id"]] in symbols
            if pilot["candidate_id"] in EXPECTED_HISTORICAL_FILTER_PROBES else None),
        "historical_no_options_absence_certified":False,
        "source_request_id":payload.get("request_id"),
        "raw_reference_count_basis":"NATIVE_ROWS_RETAINED_IN_SOURCE_BYTES_NOT_PRICE_OBSERVATIONS",
    }
    return [],validation


def parse_minutes(task,payload,request,pilot,start,end):
    if payload.get("ticker") != task["symbol"] or payload.get("adjusted") is not False:
        raise ValueError("Exact native unadjusted option minute source required")
    native = payload.get("results",[])
    if not isinstance(native,list) or len(native) > MAX_MINUTE_ROWS or payload.get("next_url"):
        raise ValueError("One complete bounded native minute page required")
    if "resultsCount" in payload and (type(payload["resultsCount"]) is not int or payload["resultsCount"] != len(native)):
        raise ValueError("Native minute result count mismatch")
    validation = {**_base_validation(),"source_record_grain":"NATIVE_OPTION_CONTRACT_MINUTE",
                  "raw_count":len(native),"source_reference_date":pilot["session_date"],
                  "source_native_symbol":task["symbol"],"source_adjusted":False,
                  "source_request_complete":True,"eligible_contract_days":1,
                  "missing_minutes_are_not_imputed":True,"source_volume_unit":"NATIVE_OPTION_CONTRACTS",
                  "source_price_unit":"NATIVE_UNADJUSTED_OPTION_TRADE_PRICE",
                  "source_reported_query_count":payload.get("queryCount"),
                  "source_reported_result_count":payload.get("resultsCount")}
    if request.get("recovery") is not None:
        validation["native_http_recovery"] = dict(request["recovery"])
        validation["minimum_source_request_spacing_seconds"] = 20
    records = {}
    ref = request["selected_contract"]
    start_ms,end_ms = int(start.timestamp())*1000,int(end.timestamp())*1000
    for row in native:
        try:
            if not isinstance(row,dict) or type(row.get("t")) is not int or row["t"] % 60000:
                raise ValueError("Native minute epoch alignment rejected")
            millis = row["t"]
            if not start_ms <= millis < end_ms:
                validation["outside_count"] += 1
                continue
            observed = datetime.fromtimestamp(millis//1000,UTC)
            values = {name:str(_number(row[key],positive=True)) for name,key in (
                ("open","o"),("high","h"),("low","l"),("close","c"))}
            prices = {key:Decimal(value) for key,value in values.items()}
            if prices["high"] < max(prices.values()) or prices["low"] > min(prices.values()):
                raise ValueError("Native option OHLC contradiction")
            values["volume_contracts"] = str(_number(row["v"],nonnegative=True,integral=True))
            values["trade_count"] = int(_number(row["n"],nonnegative=True,integral=True)) if row.get("n") is not None else None
            if values["trade_count"] is not None and values["trade_count"] > 9223372036854775807:
                raise ValueError("Native trade count exceeds typed storage")
            values["vwap"] = str(_number(row["vw"],nonnegative=True)) if row.get("vw") is not None else None
            values["availability_not_before"] = iso(observed+timedelta(minutes=1))
            values["availability_basis"] = MINUTE_BASIS
            record = {
                "record_key":task["symbol"]+":"+str(millis),"native_symbol":task["symbol"],
                "observed_at":iso(observed),"bar_end":iso(observed+timedelta(minutes=1)),
                "session_date":pilot["session_date"],"underlying_instrument_key":str(pilot["instrument_key"]),
                "underlying_native_symbol":pilot["historical_state_symbol"],"expiry":ref["expiry"],
                "option_right":ref["option_right"],"strike":ref["strike"],"shares_per_contract":100,
                "reference_evidence_id":request["reference_source_id"]+"#native_row:"+str(ref["source_row_ordinal"]),
                "publication_at":None,"available_at":None,"open_interest":None,"implied_volatility":None,"greeks":None,
                "values":values,
            }
            if millis in records:
                validation["duplicate_count"] += 1
                if record != records[millis]:
                    validation["duplicate_conflict_count"] += 1
            else:
                records[millis] = record
        except (ValueError,KeyError,TypeError,OverflowError,InvalidOperation):
            validation["invalid_count"] += 1
    result = [records[key] for key in sorted(records)]
    validation["valid_count"] = len(result)
    validation["normalization_passed"] = not (validation["invalid_count"] or validation["duplicate_conflict_count"] or validation["outside_count"])
    if len(native) != len(result)+validation["invalid_count"]+validation["duplicate_count"]+validation["outside_count"]:
        raise ValueError("Native minute population reconciliation failed")
    return result,validation


def parse_records(task,raw):
    kind,request,pilot,start,end = settings(task)
    payload = _payload(raw)
    if not isinstance(payload,dict) or payload.get("status") != "OK":
        raise ValueError("Native options source status rejected")
    if payload.get("request_id") is not None and (not isinstance(payload["request_id"],str) or len(payload["request_id"]) > 256):
        raise ValueError("Native request identifier rejected")
    for key in ("queryCount","resultsCount"):
        if payload.get(key) is not None and (type(payload[key]) is not int or not 0 <= payload[key] <= 1000000):
            raise ValueError("Native source population metadata rejected")
    return parse_reference(task,payload,request,pilot) if kind == REFERENCE else parse_minutes(task,payload,request,pilot,start,end)


def compact_records(records,validation):
    # Both source-specific envelopes already match the guarded typed/raw commit.
    return records,dict(validation)

