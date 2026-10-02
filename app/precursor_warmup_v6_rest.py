"""ASTRA V41: fixed historical-source transport only; no trades or research.

Uses the existing worker, credentials and private bridge. PostgreSQL owns
immutable plans, reservations, raw evidence, validation and idempotency.
"""
from __future__ import annotations
import hashlib
import json
import logging
import math
import os
from pathlib import Path
import time
from datetime import datetime, timezone, timedelta
from typing import Any
import httpx
from app.config import get_settings

logger = logging.getLogger(__name__)
VERSION = "NATIVE_REST_V41_20261002"
SLUG = "astra-precursor-warmup-v6-20261001"
PROJECT = "https://oxzabweahkoimtevbbny.supabase.co"
PROVIDER = "https://data.alpaca.markets/v2/stocks/bars"

class FatalPersistenceError(RuntimeError):
    pass

def dt(value: str) -> datetime:
    ans = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if ans.tzinfo is None:
        raise ValueError("Timezone missing")
    return ans.astimezone(timezone.utc)

def normalize(body: dict, job: dict) -> dict:
    if not isinstance(body, dict) or "bars" not in body:
        raise ValueError("Provider bars field missing")
    if body.get("next_page_token") or body.get("next_url"):
        raise ValueError("Pagination remains; no completeness claim")
    raw = body["bars"]
    if raw is None:
        raw = {}
    if not isinstance(raw, dict):
        raise ValueError("Provider bars must be a map")
    apis = job["api_symbols"]
    if len(apis) != len(set(apis)) or set(raw) - set(apis):
        raise ValueError("Duplicate or unexpected symbols")
    opening, closing = dt(job["regular_open"]), dt(job["regular_close"])
    step = 60 if job["lane"] == "MINUTE" else 1800
    out = {}
    for symbol, bars in raw.items():
        if not isinstance(bars, list):
            raise ValueError("Provider bars must be arrays")
        if len(bars) > int((closing-opening).total_seconds()/step):
            raise ValueError("Excess bar count")
        seen, normalized = set(), []
        for bar in bars:
            stamp = dt(bar["t"])
            if not opening <= stamp < closing or stamp.timestamp() % step:
                raise ValueError("Outside official RTH or off grid")
            if stamp in seen:
                raise ValueError("Duplicate UTC clock")
            seen.add(stamp)
            row = {"symbol": symbol, "timestamp": stamp.isoformat()}
            for short, long in [("o","open"),("h","high"),("l","low"),("c","close"),("v","volume"),("n","trade_count")]:
                val = bar.get(short)
                if isinstance(val, bool) or not isinstance(val, (int,float)) or not math.isfinite(val):
                    raise ValueError(f"Missing or nonfinite {long}")
                if short in ("v","n"):
                    if val < 0 or int(val) != val or val > 2**53-1:
                        raise ValueError("Invalid volume/trade count")
                    row[long] = int(val)
                else:
                    if val <= 0:
                        raise ValueError("Nonpositive price")
                    row[long] = val
            if row["high"] < max(row["open"],row["low"],row["close"]) or row["low"] > min(row["open"],row["close"]):
                raise ValueError("Impossible OHLC")
            vw = bar.get("vw")
            if vw is not None and (isinstance(vw,bool) or not isinstance(vw,(int,float)) or not math.isfinite(vw) or vw <= 0):
                raise ValueError("Invalid VWAP")
            if step == 60 and vw is None:
                raise ValueError("Minute VWAP missing")
            row["vwap"] = vw
            normalized.append(row)
        normalized.sort(key=lambda x:x["timestamp"])
        out[symbol] = normalized
    if sum(map(len,out.values())) > int(job["request_params"]["limit"]):
        raise ValueError("Page size exceeded")
    return out

def aggregate_payload(job: dict, bars: dict, observed: str, sha: str) -> dict:
    regular_close = dt(job["regular_close"])
    sources, broker = [], []
    for ref, api in zip(job["reference_symbols"], job["api_symbols"], strict=True):
        series = bars.get(api, [])
        row = {"source_symbol":ref,"raw_bars":series,"native_bar_count":len(series),
               "native_open":series[0]["open"] if series else None,
               "native_high":max(x["high"] for x in series) if series else None,
               "native_low":min(x["low"] for x in series) if series else None,
               "native_close":series[-1]["close"] if series else None,
               "native_volume":sum(x["volume"] for x in series) if series else None,
               "native_trade_count":sum(x["trade_count"] for x in series) if series else None}
        broker.append(row)
        if job["lane"] != "WARMUP" or not series:
            continue
        provenance = {"source":"Alpaca REST V40","feed":"sip","timeframe":"30Min","asof":"-","adjustment":"raw","currency":"USD","api_symbol":api,"reference_symbol":ref,"assembled_page_union_sha256":sha,"source_file":f"supabase://oxzabweahkoimtevbbny/market_factors_20250901_20260731_v1/native_rest_task_v40/{job['task_id']}","original_publication_replay_certified":False,"vwap":"not_substituted_for_minute_grain","transform_version":VERSION}
        common = {"source_symbol":ref,"session_date":job["session_date"],"event_time":regular_close.isoformat(),"available_at":(regular_close+timedelta(minutes=1)).isoformat(),"source_observed_at":observed,"identity_state":"PENDING_EXACT_HISTORICAL_TICKER_INTERVAL","provenance":provenance}
        sources.append({**common,"source_mode":"RTH_30MIN_AGGREGATED","open":row["native_open"],"high":row["native_high"],"low":row["native_low"],"close":row["native_close"],"volume":row["native_volume"],"trade_count":row["native_trade_count"],"source_period_volume":row["native_volume"],"raw_payload":series})
        last = [b for b in series if dt(b["timestamp"]) == regular_close-timedelta(minutes=30)]
        if last:
            sources.append({**common,"source_mode":"CLOSING_30MIN","open":None,"high":None,"low":None,"close":last[0]["close"],"volume":None,"trade_count":None,"source_period_volume":last[0]["volume"],"raw_payload":last})
    return {"source_rows":sources,"broker_rows":broker}

class WarmupRestLane:
    def __init__(self, worker_id: str):
        self.worker = worker_id + ":source-v40"
        self.settings = get_settings()
        if self.settings.supabase_url.rstrip("/") != PROJECT:
            raise RuntimeError("Fixed project does not match configured project")
        token = self.settings.supabase_service_role_key.strip()
        if not token:
            raise RuntimeError("Existing private-bridge credential missing")
        self.bridge = PROJECT + "/functions/v1/" + SLUG
        self.control = httpx.Client(timeout=60,follow_redirects=False,headers={"Authorization":"Bearer "+token})
        self.market = httpx.Client(timeout=45,follow_redirects=False,headers={"APCA-API-KEY-ID":self.settings.alpaca_api_key,"APCA-API-SECRET-KEY":self.settings.alpaca_api_secret})
        self.last_provider = 0.0

    def post(self, payload: dict) -> dict:
        for attempt in range(3):
            try:
                r = self.control.post(self.bridge,json=payload)
                r.raise_for_status()
                return r.json()
            except (httpx.TransportError,httpx.HTTPStatusError) as exc:
                code = exc.response.status_code if isinstance(exc,httpx.HTTPStatusError) else None
                if code in (400,401,403,404) or attempt == 2:
                    raise
                time.sleep(20)
        raise RuntimeError("Bridge backoff exhausted")

    def capture(self, job: dict, stop: Any) -> None:
        if job.get("url") != PROVIDER:
            raise ValueError("Nonallowlisted provider URL")
        params = job["request_params"]
        for key, val in [("feed","sip"),("asof","-"),("adjustment","raw"),("sort","asc"),("currency","USD")]:
            if params.get(key) != val:
                raise ValueError("Immutable provider definition mismatch")
        if params.get("timeframe") not in ("1Min","30Min"):
            raise ValueError("Unexpected source timeframe")
        payload = {"op":"submit_v40","task_id":job["task_id"],"worker_id":self.worker,"started_at":datetime.now(timezone.utc).isoformat(),"http_status":None,"raw_text":"","error":None,"normalized_bars":{},"source_rows":[],"broker_rows":[]}
        try:
            terminal = False
            for page in range(1,17):
                reservation = self.post({"op":"page_reserve_v41","worker_id":self.worker,"task_id":job["task_id"],"page_number":page})
                page_params=reservation["request_params"]
                if {k:v for k,v in page_params.items() if k!='page_token'} != params:
                    raise ValueError("Continuation changed immutable request")
                if reservation.get("already_saved"):
                    code=reservation.get("http_status")
                    token=reservation.get("next_page_token")
                    payload["http_status"]=code
                    payload["source_observed_at"]=reservation["source_observed_at"]
                    payload["raw_text"]=json.dumps(reservation["raw_body"],allow_nan=False)
                else:
                    delay=max(float(reservation.get("wait_seconds",0)),6-(time.monotonic()-self.last_provider),0)
                    if stop.wait(delay):
                        return
                    self.last_provider=time.monotonic()
                    error=None
                    response=None
                    try:
                        response=self.market.get(PROVIDER,params=page_params)
                        text=response.text
                        code=response.status_code
                        if len(response.content)>6*1024*1024:
                            error="Source page exceeded bounded envelope"
                        elif code!=200:
                            error=f"provider_http_{code}"
                    except Exception as exc:
                        text=""
                        code=None
                        error=f"{type(exc).__name__}:{str(exc)[:400]}"
                    observed=datetime.now(timezone.utc).isoformat()
                    payload.update(http_status=code,raw_text=text,source_observed_at=observed)
                    saved=self.post({"op":"page_save_v41","task_id":job["task_id"],"worker_id":self.worker,"page_number":page,"http_status":code,"raw_text":text,"source_observed_at":observed,"error":error})
                    if saved.get("error") or code!=200:
                        raise ValueError(saved.get("error") or f"provider_http_{code}")
                    token=saved.get("next_page_token")
                if code!=200:
                    raise ValueError(f"Preserved provider_http_{code}")
                if not token:
                    terminal=True
                    break
            if not terminal:
                raise ValueError("Finite pagination guard reached; preserve all pages")
            bundle=self.post({"op":"page_bundle_v41","worker_id":self.worker,"task_id":job["task_id"]})
            body=bundle["body"]
            payload["raw_text"]=json.dumps(body,separators=(',',':'),allow_nan=False)
            payload["source_observed_at"]=bundle["source_observed_at"]
            payload["http_status"]=200
            bars=normalize(body,job)
            payload["normalized_bars"]=bars
            payload.update(aggregate_payload(job,bars,payload["source_observed_at"],hashlib.sha256(payload["raw_text"].encode()).hexdigest()))
        except Exception as exc:
            payload["error"]=f"{type(exc).__name__}:{str(exc)[:500]}"
        payload.setdefault("source_observed_at",datetime.now(timezone.utc).isoformat())
        # Retain the identical captured response through finite persistence retries.
        # A failed SQL transaction must never cause a repeat provider request.
        for persistence_attempt in range(3):
            if stop.is_set():
                break
            try:
                result=self.post(payload)
                logger.warning("ASTRA V41 persisted lane=%s date=%s batch=%s task=%s state=%s",job["lane"],job["session_date"],job["batch"],job["task_id"],result.get("status","SOURCE_ADJUDICATED"))
                return
            except Exception as exc:
                logger.error("ASTRA V41 retained response awaiting persistence task=%s type=%s",job["task_id"],type(exc).__name__)
                if isinstance(exc,httpx.HTTPStatusError) and exc.response.status_code in (401,403):
                    break
                stop.wait(30)
        try:
            name=Path('/tmp') / ('astra-v41-unsent-'+str(job['task_id'])+'.json')
            fd=os.open(name,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
            with os.fdopen(fd,'w') as f:
                json.dump(payload,f,allow_nan=False)
        except Exception:
            pass
        raise FatalPersistenceError('Raw response persistence needs intervention; no repeat provider request')

    def run(self, stop: Any) -> None:
        try:
            has_keys=bool(self.settings.alpaca_api_key.strip() and self.settings.alpaca_api_secret.strip())
            self.post({"op":"hello_v40","worker_id":self.worker,"version":VERSION,"alpaca_configured":has_keys,"project_matches":True})
            if not has_keys:
                logger.error("ASTRA V40 missing existing Alpaca credentials; no provider calls")
                return
            logger.warning("ASTRA V40 historical-source-only lane started")
            while not stop.is_set():
                try:
                    job=self.post({"op":"claim_v40","worker_id":self.worker}).get("job")
                    if job:
                        self.capture(job,stop)
                    else:
                        stop.wait(15)
                except FatalPersistenceError:
                    logger.critical("ASTRA V40 halted with an unacknowledged source response; no repeat provider call")
                    return
                except Exception as exc:
                    logger.error("ASTRA V40 claim/transport unavailable type=%s",type(exc).__name__)
                    stop.wait(30)
        finally:
            self.control.close()
            self.market.close()

def run_precursor_warmup_v6_rest(worker_id: str, shutdown_event: Any) -> None:
    WarmupRestLane(worker_id).run(shutdown_event)
