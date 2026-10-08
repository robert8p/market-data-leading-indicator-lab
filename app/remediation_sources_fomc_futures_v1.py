"""Exactly two native Fed-funds futures candle requests; no modeled availability."""
from __future__ import annotations
import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from urllib.parse import urlsplit, parse_qsl, urlencode

VERSION = "massive_fomc_two_windows_20261008_v1"
PROBE_ID = "fm03_native_two_windows_20261008_v1"
DOCS = "https://massive.com/docs/rest/futures/aggregates"
SPECS = {
 "massive:fomc:ZQF6:2026-01-28:1845-1900": ("ZQF6","2026-01-28T18:45:00Z","2026-01-28T19:00:00Z",1769625900000000000,1769626800000000000),
 "massive:fomc:ZQJ6:2026-04-29:1745-1800": ("ZQJ6","2026-04-29T17:45:00Z","2026-04-29T18:00:00Z",1777484700000000000,1777485600000000000),
}
UTC = timezone.utc
EPOCH = datetime(1970,1,1,tzinfo=UTC)
MINUTE_NS = 60_000_000_000

def dt(value):
    d = datetime.fromisoformat(str(value).replace("Z","+00:00"))
    if d.tzinfo is None:
        raise ValueError("timezone_required")
    return d.astimezone(UTC)

def iso(value):
    return value.isoformat().replace("+00:00","Z")

def number(value, field, integer=False, positive=False):
    if isinstance(value, bool) or not isinstance(value,(str,int,float,Decimal)):
        raise ValueError("invalid_" + field)
    try:
        n = Decimal(str(value))
    except InvalidOperation:
        raise ValueError("invalid_" + field) from None
    if not n.is_finite() or n < 0 or (positive and n <= 0) or (integer and n != n.to_integral_value()):
        raise ValueError("invalid_" + field)
    return n

def request_url(spec):
    ticker,_,_,start_ns,end_ns=spec
    return "https://api.massive.com/futures/v1/aggs/"+ticker+"?"+urlencode({
        "resolution":"1min","window_start.gte":str(start_ns),"window_start.lt":str(end_ns),
        "sort":"window_start.asc","limit":"1000"})

def validate_url(url):
    try:
        p=urlsplit(url)
        if p.scheme!="https" or p.netloc!="api.massive.com" or p.fragment or p.username or p.password:
            return False
        pairs=parse_qsl(p.query,keep_blank_values=True,strict_parsing=True)
        if len(pairs)!=5 or len(dict(pairs))!=5:
            return False
        return any(p.path==urlsplit(request_url(s)).path and dict(pairs)==dict(parse_qsl(urlsplit(request_url(s)).query)) for s in SPECS.values())
    except (ValueError,TypeError):
        return False

def settings(task):
    spec=SPECS.get(task.get("batch_key"))
    req=task.get("request_json") or {}
    if spec is None or task.get("run_id")!="market_data_remediation_20261008_v1" or task.get("provider")!="massive" or task.get("source_type")!="massive_candles":
        raise ValueError("exact_finite_fomc_task_required")
    if req.get("probe_id")!=PROBE_ID or req.get("required_futures_version")!=VERSION or req.get("required_parser_version")!=VERSION:
        raise ValueError("exact_futures_parser_contract_required")
    if req.get("provider_calls_cap")!=1 or req.get("max_attempts")!=1 or req.get("price_status")!="INCLUDED_NO_INCREMENTAL_CHARGE" or req.get("maximum_incremental_cost_usd")!=0 or not req.get("price_evidence"):
        raise ValueError("one_call_existing_no_cost_contract_required")
    if task.get("symbol")!=spec[0] or task.get("interval_seconds")!=60 or dt(task.get("start_ts"))!=dt(spec[1]) or dt(task.get("end_ts"))!=dt(spec[2]):
        raise ValueError("exact_native_ticker_window_required")
    if task.get("attempts",0) not in (0,1):
        raise ValueError("no_source_retry_allowed")
    return spec

def build_requests(task):
    return [{"url":request_url(settings(task)),"role":"candles"}]

def reject_duplicate_json_keys(pairs):
    out={}
    for key,value in pairs:
        if key in out:
            raise ValueError("duplicate_native_json_key")
        out[key]=value
    return out

def parse_records(task, raw):
    spec=settings(task)
    p=json.loads(raw,parse_float=Decimal,object_pairs_hook=reject_duplicate_json_keys) if isinstance(raw,(str,bytes,bytearray)) else raw
    if not isinstance(p,dict) or p.get("status")!="OK" or p.get("error") or p.get("next_url"):
        raise ValueError("complete_successful_native_response_required")
    source=p.get("results")
    if not isinstance(source,list) or len(source)>15:
        raise ValueError("bounded_native_results_required")
    output={}
    duplicates=0
    for i,row in enumerate(source):
        if not isinstance(row,dict) or row.get("ticker")!=spec[0]:
            raise ValueError("native_contract_mismatch")
        n=number(row.get("window_start"),"window_start",integer=True)
        ns=int(n)
        if ns % MINUTE_NS or not spec[3] <= ns < spec[4]:
            raise ValueError("native_candle_outside_exact_window_or_off_grid")
        stamp=EPOCH+timedelta(seconds=ns//1_000_000_000)
        o,h,l,c=[number(row.get(k),k,positive=True) for k in ("open","high","low","close")]
        if h < max(o,l,c) or l > min(o,h,c):
            raise ValueError("native_ohlc_inconsistent")
        volume=number(row.get("volume"),"volume",integer=True)
        count=number(row.get("transactions"),"transactions",integer=True)
        number(row.get("dollar_volume"),"dollar_volume")
        session=row.get("session_end_date")
        if not isinstance(session,str) or len(session)!=10 or datetime.strptime(session,"%Y-%m-%d").date()!=stamp.date():
            raise ValueError("native_session_end_date_mismatch")
        if row.get("settlement_price") is not None:
            raise ValueError("intraday_settlement_not_expected")
        values={k:str(v) for k,v in zip(("open","high","low","close"),(o,h,l,c))}
        values.update({"volume":str(volume),"trade_count":int(count),"vwap":None,"quote_volume":None,
          "_not_before":iso(stamp+timedelta(minutes=1)),
          "_not_before_basis":"BAR_COMPLETION_LOWER_BOUND_NOT_PUBLICATION",
          "_unit_basis":"ZQ_QUOTED_PRICE_100_MINUS_RATE_PERCENT_VOLUME_CONTRACTS",
          "_price_basis":"NATIVE_FUTURES_TRADE_AGGREGATE",
          "_historical_publication_at":None,"_historical_first_receipt_at":None,
          "_strict_historical_replay_eligible":False})
        key=iso(stamp)
        record={"record_key":"massive_futures|"+spec[0]+"|60|"+key,"observed_at":key,
                "bar_end":iso(stamp+timedelta(minutes=1)),"values":values}
        if key in output:
            duplicates+=1
            if output[key]!=record:
                raise ValueError("conflicting_native_duplicate")
        else:
            output[key]=record
    records=[output[k] for k in sorted(output)]
    return records,{"parser_version":VERSION,"raw_count":len(source),"valid_count":len(records),
        "invalid_count":0,"outside_count":0,"duplicate_count":duplicates,"duplicate_conflict_count":0,
        "normalization_passed":True,"nominal_grid_slots":15,"absent_nominal_slots":15-len(records),
        "coverage_assertion":"SOURCE_ROWS_ONLY_NO_GRID_FILLING",
        "native_empty_response":len(source)==0,"no_trade_absence_certified":False,
        "price_unit_contract":"ZQ_PRICE_POINTS_100_MINUS_RATE_PERCENT",
        "volume_unit_contract":"TRADED_CONTRACTS",
        "dollar_volume_multiplier_applied":False,"settlement_or_notional_dollars_asserted":False,
        "strict_replay_certified":False,"historical_publication_recovered":False,
        "historical_first_receipt_recovered":False,"source_contract_evidence":DOCS,
        "dollar_volume_contract":"RETAINED_RAW_PRICE_TIMES_CONTRACTS_NOT_USD_NOTIONAL",
        "adjustment_basis":"RAW_NATIVE_FUTURES_UNADJUSTED",
        "modeled_delay_applied_by_parser":False}

def self_test():
    key=next(iter(SPECS));sp=SPECS[key]
    task={"run_id":"market_data_remediation_20261008_v1","batch_key":key,"provider":"massive",
          "source_type":"massive_candles","symbol":sp[0],"interval_seconds":60,
          "start_ts":sp[1],"end_ts":sp[2],"attempts":1,"request_json":{
          "probe_id":PROBE_ID,"required_futures_version":VERSION,"required_parser_version":VERSION,
          "provider_calls_cap":1,"max_attempts":1,"price_status":"INCLUDED_NO_INCREMENTAL_CHARGE",
          "maximum_incremental_cost_usd":0,"price_evidence":DOCS}}
    row={"window_start":sp[3],"ticker":sp[0],"open":96.3,"high":96.4,"low":96.2,"close":96.35,
         "volume":10,"transactions":3,"dollar_volume":963.5,"session_end_date":"2026-01-28"}
    records,v=parse_records(task,json.dumps({"status":"OK","results":[row]}))
    assert len(records)==1 and records[0]["values"]["close"]=="96.35"
    assert records[0]["values"]["vwap"] is None and not v["historical_first_receipt_recovered"]
    assert records[0]["bar_end"]=="2026-01-28T18:46:00Z"
    assert parse_records(task,{"status":"OK","results":[]})[1]["native_empty_response"]
    assert parse_records(task,{"status":"OK","results":[row,row]})[1]["duplicate_count"]==1
    failures=0
    bads=[{"ticker":"ZQJ6"},{"window_start":sp[4]},{"window_start":sp[3]+1},
          {"close":97},{"volume":1.5},{"transactions":-1},{"dollar_volume":"NaN"},
          {"session_end_date":"2026-01-29"},{"settlement_price":96},{"open":float("inf")}]
    for bad in bads:
        try: parse_records(task,{"status":"OK","results":[{**row,**bad}]})
        except (ValueError,OverflowError): failures+=1
    for payload in ({"status":"ERROR","results":[]},{"status":"OK","results":[],"next_url":"https://example.com"},
                    {"status":"OK","results":[row]*16},{"status":"OK","results":[row,{**row,"close":96.3}]}):
        try: parse_records(task,payload)
        except ValueError: failures+=1
    assert failures==14
    u=build_requests(task)[0]["url"];assert validate_url(u)
    assert not any(validate_url(bad) for bad in (u+"&limit=1000",u+"&apiKey=test",
        u.replace("api.massive.com","evil.example"),u+"#fragment",u.replace(str(sp[3]),str(sp[3]+MINUTE_NS))))
    assert settings({**task,"start_ts":"2026-01-28 18:45:00+00:00"})==sp
    for bad in ({"symbol":"ZQJ6"},{"attempts":2},{"interval_seconds":300}):
        try: settings({**task,**bad})
        except ValueError: pass
        else: raise AssertionError("unauthorized_task_accepted")
    try: parse_records(task,'{"status":"OK","status":"OK","results":[]}')
    except ValueError: pass
    else: raise AssertionError("duplicate_native_json_key_accepted")
    return {"cases":28,"failures":0}
