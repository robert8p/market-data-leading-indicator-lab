"""One fixed raw earnings probe using the existing TwelveData entitlement.

Only the worker performs authenticated GETs. No key, upgrade, paging, retry,
normalized financial fact, forecast vintage, or announcement time is created.
The quota body must be retained and its decision committed before earnings.
"""
from __future__ import annotations

import datetime as dt
import json
import re
import time
from decimal import Decimal, InvalidOperation
from urllib.parse import parse_qsl, urlencode, urlsplit

VERSION = "twelvedata_earnings_included_quota_probe_20261008_v1"
SOURCE_TYPE = "twelvedata_earnings_probe"
RUN_ID = "market_data_remediation_20261008_v1"
BATCH_KEY = "twelvedata:earnings-included-quota-probe:AAPL:20250901:20260801:v1"
ORIGIN = "https://api.twelvedata.com"
PARAMS = {"symbol":"AAPL", "start_date":"2025-09-01", "end_date":"2026-07-31", "outputsize":1000, "format":"JSON"}
QUOTA_URL = ORIGIN + "/api_usage?format=JSON"
EARNINGS_URL = ORIGIN + "/earnings?" + urlencode(PARAMS)
QUOTA_WEIGHT = 1
EARNINGS_WEIGHT = 20
MAX_CREDITS = QUOTA_WEIGHT + EARNINGS_WEIGHT
MAX_QUOTA_AGE_SECONDS = 30
PREMIUM_WITHOUT_DAILY_LIMIT = {"grow", "pro", "enterprise"}
START = dt.datetime(2025, 9, 1, tzinfo=dt.timezone.utc)
END = dt.datetime(2026, 8, 1, tzinfo=dt.timezone.utc)
FALSE_CLAIMS = (
    "historical_forecast_vintage_recovered", "historical_announcement_timestamp_recovered",
    "historical_first_receipt_recovered", "strict_historical_surprise_eligible",
    "source_coverage_complete",
)


def _timestamp(value):
    if not isinstance(value, str):
        raise ValueError("Timestamp string required")
    parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("Timezone required")
    return parsed.astimezone(dt.timezone.utc)


def settings(task):
    request = task.get("request_json") or {}
    if not isinstance(request, dict):
        raise ValueError("Fixed earnings request object required")
    scope = (task.get("run_id"), task.get("batch_key"), task.get("provider"),
             task.get("source_type"), task.get("symbol"), task.get("interval_seconds"))
    if scope != (RUN_ID, BATCH_KEY, "twelvedata", SOURCE_TYPE, "AAPL", 0):
        raise ValueError("Fixed earnings entitlement probe scope required")
    if _timestamp(task["start_ts"]) != START or _timestamp(task["end_ts"]) != END:
        raise ValueError("Exact historical probe window required")
    required = {"required_parser_version":VERSION, "max_weighted_credits_per_attempt":MAX_CREDITS,
                "max_physical_requests":2, "max_attempts":1, "source_price_usd":0,
                "quota_preflight_required":True, "raw_only":True,
                "price_status":"INCLUDED_NO_INCREMENTAL_CHARGE", "acquisition_mode":"FETCH"}
    for key, expected in required.items():
        if type(request.get(key)) is not type(expected) or request[key] != expected:
            raise ValueError("Bounded included-credit contract required")
    if not isinstance(request.get("price_evidence"), str) or not request["price_evidence"].strip():
        raise ValueError("Existing entitlement evidence required")
    if any(request.get(key) is not False for key in FALSE_CLAIMS):
        raise ValueError("Unsupported historical or completeness claim")
    if request.get("cohort_generation_id") is not None or request.get("warmup_contract") is not None:
        raise ValueError("Probe cannot use cohort or warmup scope")
    return request


def validate_url(url):
    try:
        parsed = urlsplit(url)
    except (TypeError, ValueError):
        return False
    if parsed.scheme != "https" or parsed.netloc != "api.twelvedata.com" or parsed.fragment:
        return False
    pairs = parse_qsl(parsed.query, keep_blank_values=True)
    return ((parsed.path == "/api_usage" and pairs == [("format", "JSON")]) or
            (parsed.path == "/earnings" and len(pairs) == len(PARAMS) and
             dict(pairs) == {key:str(value) for key, value in PARAMS.items()}))


def build_requests(task):
    settings(task)
    return [{"role":"quota", "url":QUOTA_URL}, {"role":"earnings", "url":EARNINGS_URL}]


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("Duplicate source object key")
        result[key] = value
    return result


def _json(raw):
    return json.loads(raw, parse_float=Decimal, object_pairs_hook=_unique_object) if isinstance(raw, (bytes, str)) else raw


def included_credit_gate(raw):
    """Fail closed; returned fields contain only whitelisted quota evidence.

    Require 21 remaining credits conservatively, because a quota response may
    or may not include its own one-credit charge in current_usage. This probe
    never renews a balance or reserves credits used by unrelated callers.
    """
    result = {"allowed":False, "code":"quota_evidence_missing", "quota_request_weight":QUOTA_WEIGHT,
              "earnings_request_weight":EARNINGS_WEIGHT, "max_total_weighted_credits":MAX_CREDITS,
              "conservative_minimum_remaining_credits":MAX_CREDITS, "incremental_price_usd":0}
    try:
        payload = _json(raw)
    except (ValueError, TypeError, UnicodeError):
        result["code"] = "quota_json_rejected"
        return result
    if not isinstance(payload, dict) or payload.get("status") in ("error", "ERROR") or payload.get("error"):
        result["code"] = "quota_endpoint_rejected"
        return result

    def count(key):
        value = payload.get(key)
        return value if type(value) is int and 0 <= value <= 1_000_000_000 else None

    category = payload.get("plan_category")
    if not isinstance(category, str) or category.lower() not in {"basic", *PREMIUM_WITHOUT_DAILY_LIMIT}:
        result["code"] = "quota_plan_category_unrecognized"
        return result
    result["plan_category"] = category
    used, limit = count("current_usage"), count("plan_limit")
    if used is None or limit is None or used > limit:
        return result
    result.update(current_usage=used, plan_limit=limit, observed_minutely_credits_left=limit-used)
    if limit-used < MAX_CREDITS:
        result["code"] = "included_minutely_credit_balance_below21"
        return result
    if "daily_usage" in payload or "plan_daily_limit" in payload:
        daily, daily_limit = count("daily_usage"), count("plan_daily_limit")
        if daily is None or daily_limit is None or daily > daily_limit:
            result["code"] = "daily_quota_evidence_incomplete"
            return result
        result.update(daily_usage=daily, plan_daily_limit=daily_limit, daily_limit_basis="OBSERVED_NUMERIC_LIMIT")
        if daily_limit-daily < MAX_CREDITS:
            result["code"] = "included_daily_credit_balance_below21"
            return result
    elif category.lower() in PREMIUM_WITHOUT_DAILY_LIMIT:
        result["daily_limit_basis"] = "PUBLISHED_PREMIUM_NO_DAILY_LIMIT"
    else:
        result["code"] = "daily_quota_evidence_missing"
        return result
    result.update(allowed=True, code="within_observed_existing_included_quota")
    return result


def _numeric_or_null(value):
    if value is None:
        return
    if isinstance(value, bool) or not isinstance(value, (str, int, float, Decimal)):
        raise ValueError("Invalid native earnings number")
    if isinstance(value, str) and not re.fullmatch(r"[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?", value):
        raise ValueError("Invalid native earnings number")
    try:
        number = Decimal(str(value))
    except (InvalidOperation, ValueError):
        raise ValueError("Invalid native earnings number") from None
    if not number.is_finite():
        raise ValueError("Nonfinite native earnings number")


def _zero_validation():
    return {"raw_count":0, "valid_count":0, "invalid_count":0, "duplicate_count":0,
            "outside_count":0, "duplicate_conflict_count":0, "normalization_passed":True,
            "parser_version":VERSION, "normalized_rows_created":0,
            "normalized_count_basis":"NO_NORMALIZATION_FOR_RAW_NATIVE_EARNINGS_PROBE",
            **{key:False for key in FALSE_CLAIMS}}


def parse_records(task, raw):
    settings(task)
    payload = _json(raw)
    if not isinstance(payload, dict) or payload.get("status") != "ok" or not isinstance(payload.get("earnings"), list):
        raise ValueError("Native earnings response invalid")
    meta = payload.get("meta")
    if not isinstance(meta, dict) or meta.get("symbol") != "AAPL" or meta.get("currency") != "USD":
        raise ValueError("Native earnings identity or currency mismatch")
    rows = payload["earnings"]
    if len(rows) > PARAMS["outputsize"]:
        raise ValueError("Native earnings response exceeds fixed bound")
    dates, timing = [], {}
    actuals = estimates = matched = 0
    for row in rows:
        if not isinstance(row, dict) or not isinstance(row.get("date"), str) or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", row["date"]):
            raise ValueError("Invalid earnings source row")
        date = dt.date.fromisoformat(row["date"])
        if not START.date() <= date < END.date():
            raise ValueError("Native earnings outside exact window")
        dates.append(str(date))
        actual, estimate = row.get("eps_actual"), row.get("eps_estimate")
        for value in (actual, estimate, row.get("difference"), row.get("surprise_prc")):
            _numeric_or_null(value)
        actuals += actual is not None
        estimates += estimate is not None
        matched += actual is not None and estimate is not None
        label = row.get("time")
        if label is not None and (not isinstance(label, str) or len(label) > 64):
            raise ValueError("Invalid earnings time label")
        label = "<null>" if label is None else label
        timing[label] = timing.get(label, 0) + 1
    validation = {**_zero_validation(), "source_record_grain":"RAW_ONLY_NATIVE_EARNINGS_ENTITLEMENT_PROBE",
                  "source_native_earnings_rows":len(rows), "source_actual_eps_present":actuals,
                  "source_estimate_eps_present":estimates, "source_actual_and_estimate_present":matched,
                  "source_release_dates":sorted(dates), "source_release_time_labels":timing,
                  "source_duplicate_date_rows_retained":len(dates)-len(set(dates)),
                  "source_response_reached_outputsize_limit":len(rows) == PARAMS["outputsize"],
                  "source_pagination_hint_present":any(payload.get(key) for key in ("next_url", "next_page", "next_page_token", "cursor")),
                  "source_currency":"USD", "source_symbol":"AAPL", "raw_native_earnings_retained":True,
                  "source_contract":"Retrospective native reported EPS/estimate observations; no accounting-basis/forecast-vintage matching or ex-ante surprise claim"}
    return [], validation


def execute_raw_probe(task, requests, fetch, commit, stopped, fault, monotonic=time.monotonic):
    """Run at most two source requests; caller owns the existing lease heartbeat."""
    settings(task)
    if type(task.get("attempts")) is not int or task["attempts"] != 1 or requests != build_requests(task):
        raise fault("earnings_single_attempt_contract_required")

    def retained_request(request):
        if stopped():
            raise fault("earnings_probe_stopped_without_retry")
        body, artifact = fetch(task, request)
        result = commit({"source":artifact, "artifact_only":True})
        if result.get("stored") is not True:
            raise fault("earnings_raw_retention_unconfirmed")
        if artifact["http_status"] != 200:
            code = artifact["http_status"]
            raise fault("source_http_" + str(code), blocked_external=400 <= code < 500)
        return body, artifact

    quota_clock = monotonic()
    quota_body, quota_source = retained_request(requests[0])
    gate = included_credit_gate(quota_body)
    quota_evidence = {"quota_preflight":gate, "quota_source_id":quota_source["source_id"],
                      "quota_source_sha256":quota_source["source_sha256"],
                      "quota_received_at":quota_source["received_at"], "max_attempts":1,
                      "max_physical_requests":2, "max_weighted_credits_per_attempt":MAX_CREDITS,
                      "incremental_price_usd":0}
    quota_validation = {**_zero_validation(), **quota_evidence,
                        "source_record_grain":"RAW_ONLY_QUOTA_PREFLIGHT", "physical_requests_attempted":1,
                        "weighted_credits_upper_bound":QUOTA_WEIGHT, "earnings_request_sent":False}
    ack = commit({"source_id":quota_source["source_id"], "records":[], "validation":quota_validation})
    if ack.get("stored") is not True:
        raise fault("earnings_quota_decision_retention_unconfirmed")
    if gate["allowed"] is not True:
        raise fault(gate["code"], blocked_external=True)
    if monotonic()-quota_clock > MAX_QUOTA_AGE_SECONDS:
        raise fault("earnings_quota_evidence_expired_without_retry", blocked_external=True)
    raw, primary = retained_request(requests[1])
    try:
        payload = _json(raw)
        if isinstance(payload, dict) and (payload.get("status") in ("error", "ERROR", "NOT_AUTHORIZED") or payload.get("error")):
            code = str(payload.get("code") or "")
            raise fault("source_application_error_" + (code if code.isdigit() else "unspecified"),
                        blocked_external=code in {"401", "403", "404", "429"})
        records, validation = parse_records(task, payload)
    except (ValueError, TypeError, KeyError, OverflowError, UnicodeError):
        raise fault("source_parse_rejected", source_invalid=True) from None
    if records:
        raise fault("earnings_raw_only_contract_required", source_invalid=True)
    validation.update(quota_evidence)
    validation.update(physical_requests_attempted=2, weighted_credits_upper_bound=MAX_CREDITS,
                      earnings_request_sent=True, historical_publication_recovered=False,
                      retained_original_revision_history_complete=False)
    result = commit({"source_id":primary["source_id"], "records":[], "validation":validation, "final":True})
    if result.get("stored") is not True:
        raise fault("earnings_final_commit_unconfirmed")
    return result, validation
