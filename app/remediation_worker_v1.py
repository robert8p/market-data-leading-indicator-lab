"""Finite, resumable source acquisition for the authorized 2026-10-08 remediation.

Run as ``python -m app.remediation_worker_v1`` on the existing worker. This module
does not import the generic worker, provider registry, discovery or research jobs.
The coordinator alone inserts the finite source queue and promotes checked data.
"""
from __future__ import annotations

import base64
import gzip
import hashlib
import json
import os
import re
import signal
import socket
import threading
import time
import uuid
import zipfile
from datetime import datetime, timezone
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
from urllib.request import Request, HTTPRedirectHandler, build_opener

from . import remediation_sources_binance_v1 as binance
from . import remediation_sources_binance_depth_v1 as depth
from . import remediation_sources_coinbase_v1 as coinbase
from . import remediation_sources_ohlc_v1 as ohlc
from . import remediation_sources_massive_compat_v1 as massive_compat
from . import remediation_sources_alpaca_v1 as alpaca
from . import remediation_sources_alpaca_assets_v1 as alpaca_assets
from . import remediation_sources_alpaca_panel_v1 as alpaca_panel
from . import remediation_sources_alpaca_tick_chain_v1 as alpaca_tick_chain
from . import remediation_sources_massive_reference_v1 as massive_reference
from . import remediation_sources_massive_warmup_reference_v1 as warmup_reference
from . import remediation_sources_twelvedata_earnings_v1 as td_earnings
from . import remediation_sources_massive_options_v1 as massive_options
from . import remediation_sources_fomc_futures_v1 as fomc_futures

RUN_ID = "market_data_remediation_20261008_v1"
PROJECT_REF = "oxzabweahkoimtevbbny"
SUPABASE_ORIGIN = "https://" + PROJECT_REF + ".supabase.co"
VERSION = "market_data_remediation_worker_20261008_v1"
UTC = timezone.utc
MAX_BODY_BYTES = 64*1024*1024
MAX_PACKED_BYTES = 8*1024*1024
MAX_RPC_BODY_BYTES = 16*1024*1024
RPC_NAMES = {"claim", "heartbeat", "commit", "fail", "budget_policy", "pipeline", "noncrypto_claim"}
PROVIDERS = {"binance_archive":binance,"coinbase":coinbase,"twelvedata":ohlc,"massive":massive_compat,"alpaca":alpaca}
FIELDS = {"open","high","low","close","volume","quote_volume","trade_count","vwap",
          "taker_buy_base_volume","taker_buy_quote_volume","open_interest_quantity","open_interest_quote",
          "global_account_long_short_ratio","top_account_long_short_ratio","top_position_long_short_ratio",
          "taker_long_short_ratio","funding_rate","funding_interval_hours","availability_not_before","availability_basis","depth_bands"}
ALIASES = {"open_interest":"open_interest_quantity","open_interest_value":"open_interest_quote",
           "global_long_short_ratio":"global_account_long_short_ratio","taker_buy_sell_ratio":"taker_long_short_ratio",
           "_not_before":"availability_not_before","_not_before_basis":"availability_basis"}
SOURCE_CAPABILITIES = {
    "massive_fomc_futures":fomc_futures.VERSION,
    "alpaca_tick_continuation":alpaca_tick_chain.CONTINUATION_VERSION,
    **{name:alpaca_panel.VERSION for name in alpaca_panel.KINDS},
    "coinbase_candles":coinbase.VERSION,
    **{name:binance.VERSION for name in ("binance_klines","binance_mark","binance_index","binance_metrics","binance_funding")},
    "binance_book_depth":depth.VERSION,
    "twelvedata_candles":ohlc.VERSION,"massive_candles":massive_compat.VERSION,
    **{name:alpaca.VERSION for name in ("alpaca_option_bars","alpaca_equity_quotes_probe","alpaca_equity_trades_probe")},
    "alpaca_assets_snapshot":alpaca_assets.VERSION,
    "massive_reference_tickers":massive_reference.VERSION,
    "massive_warmup_reference":warmup_reference.VERSION,
    td_earnings.SOURCE_TYPE:td_earnings.VERSION,
    **{name:massive_options.VERSION for name in massive_options.KINDS},
    massive_options.RECOVERY_CAPABILITY:massive_options.RECOVERY_VERSION,
}
ALPACA_KEY_NAMES = ("ALPACA_API_KEY","APCA_API_KEY_ID","ALPACA_KEY_ID","ALPACA_API_KEY_ID")
ALPACA_SECRET_NAMES = ("ALPACA_API_SECRET","APCA_API_SECRET_KEY","ALPACA_SECRET_KEY")

def alpaca_credentials(environ):
    """Resolve once per Worker; never emit or return credentials outside runtime."""
    return (next((environ[n] for n in ALPACA_KEY_NAMES if environ.get(n)), ""),
            next((environ[n] for n in ALPACA_SECRET_NAMES if environ.get(n)), ""))

SAFE_HEADERS = {"date","etag","last-modified","content-type","content-length","retry-after"}


def utcnow():
    return datetime.now(UTC).isoformat().replace("+00:00","Z")


def emit(event, **fields):
    # Callers only supply known codes/counts/identities, never exception strings,
    # response bodies, auth headers, environment values or credential-bearing URLs.
    print(json.dumps({"at":utcnow(),"event":event,"version":VERSION,**fields},sort_keys=True),flush=True)


class WorkerFault(Exception):
    def __init__(self, code, retryable=False, source_invalid=False, blocked_external=False):
        super().__init__(code)
        self.code, self.retryable, self.source_invalid = code, retryable, source_invalid
        self.blocked_external = blocked_external


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise WorkerFault("source_redirect_blocked")


def compact_records(records, validation):
    """Keep typed values; retain shared unit/timing contracts once per batch."""
    meta = {}
    compact = []
    for record in records:
        values = record["values"]
        for key in ("_unit_basis","_price_basis","_not_before_basis","_source_api","_taker_interval_basis","_source_timestamp_unit","_depth_band_columns","_timestamp_basis"):
            if values.get(key) is not None:
                existing = meta.setdefault(key,[])
                if values[key] not in existing:
                    if len(existing)>=20:
                        raise WorkerFault("source_contract_variation_requires_partition",source_invalid=True)
                    existing.append(values[key])
        out = {ALIASES.get(k,k):v for k,v in values.items() if ALIASES.get(k,k) in FIELDS}
        compact.append({"record_key":record["record_key"],"observed_at":record["observed_at"],
                        "bar_end":record.get("bar_end"),"values":out})
    validation = dict(validation)
    validation["source_record_contract"] = meta
    validation.setdefault("source_contract",meta)
    validation["historical_publication_recovered"] = False
    validation["historical_first_receipt_recovered"] = False
    validation["retained_original_revision_history_complete"] = False
    return compact, validation


def source_artifact(url, status, headers, body, role, parser_version, secrets=()):
    """Retain actual receipt and response hashes; redact only credential echoes."""
    safe_url = sanitize_url(url)
    original_hash = hashlib.sha256(body).hexdigest()
    stored = body
    count = 0
    for secret in sorted(set(s for s in secrets if s),key=len,reverse=True):
        needle = secret.encode("utf-8")
        if len(needle)>=8:
            count += stored.count(needle)
            stored = stored.replace(needle,b"[REDACTED_CREDENTIAL]")
    packed = gzip.compress(stored,compresslevel=6,mtime=0)
    if len(body)>MAX_BODY_BYTES or len(packed)>MAX_PACKED_BYTES:
        raise WorkerFault("source_exceeds_bounded_storage_object")
    return {"source_id":str(uuid.uuid4()),"role":role,"source_url":safe_url,"http_status":status,
            "headers":{str(k).lower():str(v) for k,v in headers.items() if str(k).lower() in SAFE_HEADERS},
            "received_at":utcnow(),"revision_at":None,"source_sha256":original_hash,
            "stored_body_sha256":hashlib.sha256(stored).hexdigest(),"compressed_sha256":hashlib.sha256(packed).hexdigest(),
            "original_bytes":len(body),"compressed_base64":base64.b64encode(packed).decode("ascii"),
            "parser_version":parser_version,"credential_redactions":count,
            "provenance":{"worker_version":VERSION,"receipt_semantics":"ACTUAL_ACQUISITION_RECEIPT",
                          "last_modified_semantics":"HTTP_ARTIFACT_METADATA_NOT_MARKET_REVISION",
                          "source_payload_intact":count==0,"strict_historical_replay_eligible":False}}


def sanitize_url(url):
    p = urlsplit(url)
    forbidden = {"apikey","api_key","api-key","token","access_token","secret","password"}
    query = [(k,v) for k,v in parse_qsl(p.query,keep_blank_values=True) if k.lower() not in forbidden]
    return urlunsplit((p.scheme,p.netloc,p.path,urlencode(query),""))


def validate_url(provider,url):
    p = urlsplit(url)
    if p.scheme!="https" or p.username or p.password or p.port not in (None,443) or p.fragment:
        raise WorkerFault("source_url_not_allowed")
    if provider=="coinbase":
        allowed = ((p.hostname=="api.exchange.coinbase.com" and re.fullmatch(r"/products/[A-Z0-9]+-[A-Z0-9]+/candles",p.path)) or
                   (p.hostname=="api.coinbase.com" and re.fullmatch(r"/api/v3/brokerage/market/products/[A-Z0-9]+-[A-Z0-9]+/candles",p.path)))
    elif provider=="binance_archive":
        allowed = p.hostname=="data.binance.vision" and re.fullmatch(r"/data/(futures/um|spot)/(daily|monthly)/(klines|markPriceKlines|indexPriceKlines|metrics|fundingRate|bookDepth)/[A-Za-z0-9_./-]+\.zip(?:\.CHECKSUM)?",p.path) and ".." not in p.path
    elif provider=="twelvedata":
        allowed = (p.hostname=="api.twelvedata.com" and p.path=="/time_series") or td_earnings.validate_url(url)
    elif provider=="massive":
        allowed = (p.hostname=="api.massive.com" and re.fullmatch(r"/v2/aggs/ticker/[A-Za-z0-9%._:-]+/range/[0-9]+/minute/[0-9]+/[0-9]+",p.path)) or massive_reference.validate_reference_url(url) or fomc_futures.validate_url(url) or massive_options.validate_url(url)
    elif provider=="alpaca":
        allowed = (p.hostname=="data.alpaca.markets" and p.path in ("/v1beta1/options/bars","/v2/stocks/quotes","/v2/stocks/trades")) or alpaca_assets.validate_metadata_url(url)
    else:
        allowed = False
    if not allowed:
        raise WorkerFault("source_url_not_allowed")


class RpcClient:
    def __init__(self,environ=None):
        env = os.environ if environ is None else environ
        self.key = env.get("SUPABASE_SERVICE_ROLE_KEY","")
        origin = env.get("SUPABASE_URL",SUPABASE_ORIGIN).rstrip("/")
        if origin!=SUPABASE_ORIGIN:
            raise WorkerFault("supabase_target_mismatch")
        self.origin = origin
        self.db_url = env.get("SUPABASE_DB_URL") or env.get("DATABASE_URL") or ""
        self.opener = build_opener(NoRedirect)
        self.lock = threading.Lock()
        self.mode = "rest" if self.key else "postgres" if self.db_url else None
        if not self.mode:
            raise WorkerFault("existing_database_credentials_required")
        if self.mode=="postgres":
            try:
                import psycopg
                from psycopg.conninfo import conninfo_to_dict
                self.psycopg = psycopg
                info = conninfo_to_dict(self.db_url)
                host,user = info.get("host",""),info.get("user","")
                if not (host=="db."+PROJECT_REF+".supabase.co" or
                        (host.endswith(".pooler.supabase.com") and user.endswith("."+PROJECT_REF))):
                    raise WorkerFault("direct_database_target_unverified")
            except ImportError:
                raise WorkerFault("existing_postgres_client_required")

    def call(self,operation,payload):
        if operation not in RPC_NAMES:
            raise WorkerFault("rpc_not_allowed")
        name = "market_data_remediation_"+operation+"_v1"
        packed = json.dumps({"p_request":payload},separators=(",",":"),allow_nan=False).encode()
        if len(packed)>MAX_RPC_BODY_BYTES:
            raise WorkerFault("rpc_payload_too_large")
        # A failed claim is not transparently repeated: its lease may have committed.
        # Source/row commits carry immutable IDs and are safe to retry once.
        retries = 2 if operation in ("commit","heartbeat") else 1
        for attempt in range(retries):
            try:
                if self.mode=="rest":
                    req = Request(self.origin+"/rest/v1/rpc/"+name,data=packed,method="POST",headers={
                        "Authorization":"Bearer "+self.key,"apikey":self.key,"Content-Type":"application/json"})
                    with self.opener.open(req,timeout=45) as response:
                        raw = response.read(1024*1024+1)
                    if len(raw)>1024*1024:
                        raise WorkerFault("rpc_response_too_large")
                    result = json.loads(raw)
                else:
                    from psycopg.types.json import Jsonb
                    with self.lock,self.psycopg.connect(self.db_url,connect_timeout=15,application_name=VERSION) as conn:
                        conn.execute("set local statement_timeout='30s'")
                        result = conn.execute("select public."+name+"(%s::jsonb)",(Jsonb(payload),)).fetchone()[0]
                if not isinstance(result,dict):
                    raise WorkerFault("rpc_response_invalid")
                return result
            except HTTPError as exc:
                if exc.code>=500 and attempt+1<retries:
                    time.sleep(1)
                    continue
                suffix = ""
                try:
                    info = json.loads(exc.read(8192))
                    state = str(info.get("code", ""))
                    if re.fullmatch(r"[A-Z0-9]{5,12}",state):
                        suffix += "_"+state
                    message = str(info.get("message",""))
                    match = re.match(r"^((?:remediation|source|record|final|candle|mark_index|normalized|within_commit|nonfinite|worker)_[a-z0-9_]{1,100})(?:$|[ :])",message)
                    if match:
                        suffix += "_"+match.group(1)
                except (ValueError,UnicodeError,AttributeError,TypeError):
                    pass
                raise WorkerFault("rpc_http_"+str(exc.code)+suffix,retryable=exc.code>=500 or exc.code==429)
            except (URLError,TimeoutError,socket.timeout):
                if attempt+1<retries:
                    time.sleep(1)
                    continue
                raise WorkerFault("rpc_transport_error",retryable=True)
            except WorkerFault:
                raise
            except Exception as exc:
                # Database exceptions can contain connection strings or SQL values.
                state = getattr(exc,"sqlstate","") or ""
                suffix = "_"+state if re.fullmatch(r"[A-Z0-9]{5}",state) else ""
                retryable = not state or state.startswith("08") or state in {"40001","40P01","53300","53400","57P03","57014"}
                raise WorkerFault("rpc_database_or_decode_error"+suffix,retryable=retryable)


class Worker:
    # Legacy synthetic fixtures construct with __new__; default to finite policy.
    budgets_removed = False

    def __init__(self,rpc,environ=None):
        self.env = os.environ if environ is None else environ
        self.rpc = rpc
        self.budgets_removed = getattr(rpc, "budgets_removed", False) is True
        self.alpaca_key, self.alpaca_secret = alpaca_credentials(self.env)
        self.worker_id = "remediation:"+self.env.get("RENDER_INSTANCE_ID",str(uuid.uuid4()))[:110]
        self.stop = threading.Event()
        self.opener = build_opener(NoRedirect)
        self.request_count = 0
        self.batch_count = 0
        self.ceiling_reached = False
        self.max_requests = min(20000,max(1,int(self.env.get("MARKET_DATA_REMEDIATION_MAX_REQUESTS","100"))))
        self.max_batches = min(10000,max(1,int(self.env.get("MARKET_DATA_REMEDIATION_MAX_BATCHES","50"))))
        self.fomc_mode = self.env.get("MARKET_DATA_REMEDIATION_FOMC_PROBE_VERSION", "")
        if self.fomc_mode:
            if self.fomc_mode != fomc_futures.VERSION:
                raise WorkerFault("finite_fomc_version_invalid")
            self.max_requests = min(self.max_requests, 2)
            self.max_batches = min(self.max_batches, 2)
        self.last_request = {}
        self.secrets = [self.env.get(k,"") for k in ("SUPABASE_SERVICE_ROLE_KEY","TWELVEDATA_API_KEY","MASSIVE_API_KEY","POLYGON_API_KEY",*ALPACA_KEY_NAMES,*ALPACA_SECRET_NAMES)]

    def identity(self,batch):
        return {"run_id":RUN_ID,"worker_id":self.worker_id,"batch_id":batch["batch_id"],"lease_token":batch["lease_token"]}

    def source_module(self,batch):
        if (batch.get("request_json") or {}).get("required_futures_version") is not None:
            if (batch.get("request_json") or {}).get("required_futures_version") != fomc_futures.VERSION:
                raise WorkerFault("finite_futures_parser_version_invalid")
            return fomc_futures
        if batch["provider"]=="massive" and batch["source_type"] in massive_options.KINDS:
            return massive_options
        if batch["provider"]=="twelvedata" and batch["source_type"]==td_earnings.SOURCE_TYPE:
            return td_earnings
        if batch["provider"]=="alpaca" and batch["source_type"] in alpaca_tick_chain.KINDS and batch.get("request_json",{}).get("required_continuation_version"):
            return alpaca_tick_chain
        if batch["provider"]=="alpaca" and batch["source_type"] in alpaca_panel.KINDS:
            return alpaca_panel
        if batch["provider"]=="massive" and batch["source_type"]=="massive_reference_tickers":
            return warmup_reference if batch.get("request_json", {}).get("required_parser_version") == warmup_reference.VERSION else massive_reference
        if batch["provider"]=="alpaca" and batch["source_type"]=="alpaca_assets_snapshot":
            return alpaca_assets
        if batch["provider"]=="binance_archive" and batch["source_type"]=="binance_book_depth":
            return depth
        return PROVIDERS[batch["provider"]]

    def preflight(self,batch):
        if batch.get("run_id")!=RUN_ID or batch.get("provider") not in PROVIDERS:
            raise WorkerFault("task_scope_mismatch")
        start,end = binance._dt(batch["start_ts"]),binance._dt(batch["end_ts"])
        req = batch.get("request_json") or {}
        if isinstance(req,str):
            req = json.loads(req)
            batch["request_json"] = req
        if end>binance._dt("2026-08-01T00:00:00Z") or end<=start:
            raise WorkerFault("task_window_mismatch")
        if start<binance._dt("2025-09-01T00:00:00Z") and not req.get("warmup_contract"):
            raise WorkerFault("documented_warmup_contract_required")
        if batch["provider"] in ("twelvedata","massive"):
            if not self.budgets_removed and (req.get("price_status")!="INCLUDED_NO_INCREMENTAL_CHARGE" or not req.get("price_evidence")):
                raise WorkerFault("verified_existing_entitlement_price_required",blocked_external=True)
            key = (self.env.get("TWELVEDATA_API_KEY") if batch["provider"]=="twelvedata" else
                   self.env.get("MASSIVE_API_KEY") or self.env.get("POLYGON_API_KEY"))
            if not key:
                raise WorkerFault("existing_"+batch["provider"]+"_credential_required",blocked_external=True)
        if batch["provider"]=="alpaca":
            if not self.budgets_removed and (req.get("price_status")!="INCLUDED_NO_INCREMENTAL_CHARGE" or not req.get("price_evidence")):
                raise WorkerFault("verified_existing_entitlement_price_required",blocked_external=True)
            if not self.alpaca_key or not self.alpaca_secret:
                raise WorkerFault("existing_alpaca_credentials_required",blocked_external=True)
        module = self.source_module(batch)
        if (module is fomc_futures) != bool(self.fomc_mode):
            raise WorkerFault("finite_fomc_mode_task_mismatch")
        if module is massive_options and batch.get("attempts")!=1:
            raise WorkerFault("options_single_attempt_required")
        if req.get("required_parser_version") and req["required_parser_version"] != module.VERSION:
            raise WorkerFault("required_source_parser_version_mismatch")
        minimum_spacing = req.get("minimum_source_request_spacing_seconds", 0)
        if not isinstance(minimum_spacing, (int, float)) or not 0 <= minimum_spacing <= 300:
            raise WorkerFault("source_request_spacing_invalid")
        requests = module.build_requests(batch)
        for request in requests:
            validate_url(batch["provider"],request["url"])
        if not self.budgets_removed and self.request_count+len(requests)>self.max_requests:
            self.ceiling_reached = True
            raise WorkerFault("process_request_ceiling_reached",retryable=True)
        return module,requests

    def fetch(self,batch,request):
        provider,url = batch["provider"],request["url"]
        validate_url(provider,url)
        if not self.budgets_removed and self.request_count>=self.max_requests:
            self.ceiling_reached = True
            raise WorkerFault("process_request_ceiling_reached",retryable=True)
        seconds = {"coinbase":0.4,"binance_archive":1.0,"twelvedata":8.0,"massive":1.2,"alpaca":0.4}[provider]
        seconds = max(seconds, (batch.get("request_json") or {}).get("minimum_source_request_spacing_seconds", 0))
        remaining = seconds-(time.monotonic()-self.last_request.get(provider,0))
        if remaining>0 and self.stop.wait(remaining):
            raise WorkerFault("worker_stopping",retryable=True)
        headers = {"User-Agent":VERSION,"Accept-Encoding":"identity"}
        if provider=="twelvedata":
            url += "&"+urlencode({"apikey":self.env["TWELVEDATA_API_KEY"]})
        elif provider=="massive":
            headers["Authorization"] = "Bearer "+(self.env.get("MASSIVE_API_KEY") or self.env["POLYGON_API_KEY"])
        elif provider=="alpaca":
            headers["APCA-API-KEY-ID"] = self.alpaca_key
            headers["APCA-API-SECRET-KEY"] = self.alpaca_secret
        self.request_count += 1
        self.last_request[provider] = time.monotonic()
        req = Request(url,headers=headers,method="GET")
        try:
            response = self.opener.open(req,timeout=30)
        except HTTPError as error:
            response = error
        except (URLError,TimeoutError,socket.timeout):
            raise WorkerFault("source_transport_error",retryable=True)
        with response:
            body_limit = massive_options.MAX_SOURCE_BODY_BYTES if batch["source_type"] in massive_options.KINDS else MAX_BODY_BYTES
            body = response.read(body_limit+1)
            status = response.status
            response_headers = dict(response.headers)
        role = "checksum" if request["role"]=="checksum" else "primary"
        artifact = source_artifact(request["url"],status,response_headers,body,role if status==200 else "error",
                                   self.source_module(batch).VERSION,self.secrets)
        artifact["provenance"]["attempt"] = batch.get("attempts")
        if batch["source_type"] in massive_options.KINDS:
            artifact["provenance"]["response_body_complete"] = len(body)<=body_limit
            artifact["provenance"]["source_payload_intact"] = len(body)<=body_limit and artifact["credential_redactions"]==0
            artifact["provenance"]["source_hash_basis"] = "RETAINED_RESPONSE_BYTES_WITH_EXPLICIT_COMPLETENESS_FLAG"
            artifact["provenance"]["native_pilot_id"] = massive_options.PILOT_ID
        license_evidence = (batch.get("request_json") or {}).get("license_evidence")
        if license_evidence:
            artifact["provenance"]["license_evidence"] = license_evidence
        return body,artifact

    def process(self,batch,prefetched=None):
        identity = self.identity(batch)
        heartbeat_stop = threading.Event()
        lost_lease = threading.Event()

        def heartbeat():
            while not heartbeat_stop.wait(25):
                try:
                    result = self.rpc.call("heartbeat",identity)
                    if not result.get("renewed"):
                        lost_lease.set()
                        return
                except WorkerFault:
                    lost_lease.set()
                    return

        heart = threading.Thread(target=heartbeat,name="remediation-lease",daemon=True)
        heart.start()
        try:
            if batch.get("source_type") in ({td_earnings.SOURCE_TYPE}|massive_options.KINDS) and prefetched is not None:
                raise WorkerFault("earnings_probe_prefetch_not_allowed")
            if prefetched is not None and isinstance(prefetched["result"],WorkerFault):
                raise prefetched["result"]
            module,requests = self.preflight(batch) if prefetched is None else (prefetched["module"],prefetched["requests"])
            if module is td_earnings:
                result,validation = td_earnings.execute_raw_probe(
                    batch, requests, self.fetch,
                    lambda payload:self.rpc.call("commit",{**identity,**payload}),
                    lambda:lost_lease.is_set() or self.stop.is_set(), WorkerFault)
                emit("batch_finished",batch_id=batch["batch_id"],provider=batch["provider"],source_type=batch["source_type"],
                     valid_count=result["valid_count"],invalid_count=validation["invalid_count"],
                     duplicate_conflict_count=0,normalization_passed=validation["normalization_passed"])
                return
            bodies = {}
            primary = None
            for request in requests:
                if lost_lease.is_set() or (self.stop.is_set() and prefetched is None):
                    raise WorkerFault("worker_lease_or_stop",retryable=True)
                body,artifact = self.fetch(batch,request) if prefetched is None else prefetched["result"]
                self.rpc.call("commit",{**identity,"source":artifact,"artifact_only":True})
                if prefetched is not None and artifact["provenance"].get("response_body_complete") is False:
                    raise WorkerFault("source_cohort_response_byte_limit",source_invalid=True)
                if module is massive_options and artifact["provenance"].get("response_body_complete") is False:
                    raise WorkerFault("options_native_response_byte_limit",source_invalid=True)
                if artifact["http_status"]!=200:
                    code = artifact["http_status"]
                    raise WorkerFault("source_http_"+str(code),retryable=code==429 or code>=500,blocked_external=400<=code<500 and code!=429)
                bodies[request["role"]] = body
                if artifact["role"]=="primary":
                    primary = artifact
            if batch["provider"]=="binance_archive":
                checksum_text = bodies["checksum"].decode("ascii","strict").strip()
                expected = checksum_text.split()[0] if checksum_text else ""
                if not re.fullmatch(r"[a-fA-F0-9]{64}",expected) or expected.lower()!=hashlib.sha256(bodies["archive"]).hexdigest():
                    raise WorkerFault("publisher_checksum_mismatch",source_invalid=True)
                raw = bodies["archive"]
            else:
                raw = bodies["candles"]
                try:
                    payload = json.loads(raw)
                except (ValueError,UnicodeError):
                    raise WorkerFault("source_json_invalid",source_invalid=True)
                if isinstance(payload,dict) and (payload.get("status") in ("error","ERROR","NOT_AUTHORIZED") or payload.get("error")):
                    code = str(payload.get("code") or "")
                    raise WorkerFault("source_application_error_"+(code if code.isdigit() else "unspecified"),retryable=code=="429",blocked_external=code in {"401","403","404"})
            try:
                records,validation = module.parse_records(batch,raw)
                if module is massive_options:
                    records,validation = massive_options.compact_records(records,validation)
                else:
                    records,validation = alpaca_panel.compact_records(records,validation) if module in (alpaca_panel,alpaca_tick_chain) else compact_records(records,validation)
            except (ValueError,KeyError,TypeError,OverflowError,UnicodeError,zipfile.BadZipFile,RuntimeError):
                raise WorkerFault("source_parse_rejected",source_invalid=True)
            if batch["provider"]=="binance_archive":
                validation["published_archive_checksum_verified"] = True
            if len(records)>50000:
                raise WorkerFault("source_record_ceiling_exceeded",source_invalid=True)
            # Preserve the raw receipt transaction; finalize the final typed
            # chunk atomically through the existing database reconciliation.
            combined_final = batch["provider"] in {"coinbase", "binance_archive"}
            result = None
            for offset in range(0,len(records),500):
                if lost_lease.is_set() or (self.stop.is_set() and prefetched is None):
                    raise WorkerFault("worker_lease_or_stop",retryable=True)
                chunk = {**identity,"source_id":primary["source_id"],"records":records[offset:offset+500]}
                is_last = offset + 500 >= len(records)
                if offset==0 or (combined_final and is_last):
                    chunk["validation"] = validation
                if combined_final and is_last:
                    chunk["final"] = True
                result = self.rpc.call("commit",chunk)
            if not combined_final or not records:
                result = self.rpc.call("commit",{**identity,"source_id":primary["source_id"],"records":[],"validation":validation,"final":True})
            emit("batch_finished",batch_id=batch["batch_id"],provider=batch["provider"],source_type=batch["source_type"],
                 valid_count=result["valid_count"],invalid_count=validation["invalid_count"],
                 duplicate_conflict_count=validation.get("duplicate_conflict_count",0),normalization_passed=validation["normalization_passed"])
        except WorkerFault as exc:
            if batch.get("source_type") in ({td_earnings.SOURCE_TYPE}|massive_options.KINDS) or (batch.get("request_json") or {}).get("required_futures_version") is not None:
                exc.retryable = False
            try:
                self.rpc.call("fail",{**identity,"retryable":exc.retryable,"source_invalid":exc.source_invalid,"blocked_external":exc.blocked_external,
                                      "retry_after_seconds":min(3600,60*2**max(0,batch.get("attempts",1)-1)),
                                      "error":{"code":exc.code,"worker_version":VERSION}})
            except WorkerFault:
                emit("failure_record_pending_lease_recovery",batch_id=batch["batch_id"],code=exc.code)
            emit("batch_not_completed",batch_id=batch["batch_id"],code=exc.code,retryable=exc.retryable)
        except Exception as exc:
            # Class names are safe operational identifiers; raw exception text may
            # contain a credential-bearing URL or response and is never logged.
            code = "worker_unexpected_"+type(exc).__name__
            try:
                self.rpc.call("fail",{**identity,"retryable":False,"source_invalid":False,"blocked_external":False,
                                      "error":{"code":code,"worker_version":VERSION}})
            except WorkerFault:
                emit("failure_record_pending_lease_recovery",batch_id=batch["batch_id"],code=code)
            emit("batch_not_completed",batch_id=batch["batch_id"],code=code,retryable=False)
        finally:
            heartbeat_stop.set()
            heart.join(timeout=2)

    def run(self):
        idle_report_at = 0
        while not self.stop.is_set():
            if self.ceiling_reached or (not self.budgets_removed and (self.batch_count>=self.max_batches or self.request_count>=self.max_requests)):
                if time.monotonic()-idle_report_at>60:
                    emit("bounded_process_idle",batch_count=self.batch_count,request_count=self.request_count)
                    idle_report_at = time.monotonic()
                self.stop.wait(30)
                continue
            try:
                claim = self.rpc.call("claim",{"run_id":RUN_ID,"worker_id":self.worker_id,"source_capabilities":SOURCE_CAPABILITIES, **({"fomc_probe_version":self.fomc_mode} if self.fomc_mode else {})})
                if claim.get("status")!="claimed":
                    if time.monotonic()-idle_report_at>60:
                        emit("queue_idle",reason=claim.get("reason","no_claim"),batch_count=self.batch_count,
                             request_count=self.request_count,used_bytes=claim.get("used_bytes"),max_bytes=claim.get("max_bytes"))
                        idle_report_at = time.monotonic()
                    self.stop.wait(30)
                    continue
                self.batch_count += 1
                emit("batch_claimed",batch_id=claim["batch"]["batch_id"],attempt=claim["batch"]["attempts"])
                self.process(claim["batch"])
            except WorkerFault as exc:
                emit("worker_waiting",code=exc.code)
                self.stop.wait(30)


def main():
    if os.environ.get("MARKET_DATA_REMEDIATION_ENABLED","").lower()!="true" or os.environ.get("MARKET_DATA_REMEDIATION_RUN_ID")!=RUN_ID:
        emit("remediation_not_enabled")
        return
    emit("credential_presence",supabase_service_role=bool(os.environ.get("SUPABASE_SERVICE_ROLE_KEY")),
         database_url=bool(os.environ.get("SUPABASE_DB_URL") or os.environ.get("DATABASE_URL")),
         twelvedata=bool(os.environ.get("TWELVEDATA_API_KEY")),
         massive=bool(os.environ.get("MASSIVE_API_KEY") or os.environ.get("POLYGON_API_KEY")),
         alpaca_key=any(bool(os.environ.get(n)) for n in ALPACA_KEY_NAMES),
         alpaca_secret=any(bool(os.environ.get(n)) for n in ALPACA_SECRET_NAMES),
         massive_s3_access_key=any(bool(os.environ.get(n)) for n in ("MASSIVE_S3_ACCESS_KEY_ID","POLYGON_S3_ACCESS_KEY_ID")),
         massive_s3_secret_key=any(bool(os.environ.get(n)) for n in ("MASSIVE_S3_SECRET_ACCESS_KEY","POLYGON_S3_SECRET_ACCESS_KEY")),
         aws_access_key_id=bool(os.environ.get("AWS_ACCESS_KEY_ID")),
         aws_secret_access_key=bool(os.environ.get("AWS_SECRET_ACCESS_KEY")),
         aws_session_token=bool(os.environ.get("AWS_SESSION_TOKEN")),
         matching_massive_s3_endpoint=any(os.environ.get(n, "").rstrip("/") in ("https://files.massive.com", "https://files.polygon.io")
             for n in ("AWS_ENDPOINT_URL_S3", "AWS_ENDPOINT_URL", "S3_ENDPOINT_URL", "MASSIVE_S3_ENDPOINT", "POLYGON_S3_ENDPOINT")),
         tardis=bool(os.environ.get("TARDIS_API_KEY")),
         coinapi=bool(os.environ.get("COINAPI_API_KEY") or os.environ.get("COINAPI_KEY")))
    try:
        from . import remediation_options_selftest_v1 as options_selftest
        import sys
        if not options_selftest.run_tests(sys.modules[__name__]):
            raise WorkerFault("native_options_targeted_selftests_failed")
        emit("finite_fomc_parser_selftests", version=fomc_futures.VERSION, **fomc_futures.self_test())
        rpc = RpcClient()
        policy = rpc.call("budget_policy", {"run_id": RUN_ID})
        if policy.get("run_id") != RUN_ID or policy.get("project_ref") != PROJECT_REF:
            raise WorkerFault("budget_policy_target_mismatch")
        rpc.budgets_removed = policy.get("budgets_removed") is True
        sys.modules[__name__].BUDGETS_REMOVED = rpc.budgets_removed
        emit("remediation_budget_policy_loaded", budgets_removed=rpc.budgets_removed,
             authorization_change_id=policy.get("authorization_change_id"))
        capacity_monitor = None
        if os.environ.get("MARKET_DATA_REMEDIATION_CAPACITY_OBSERVER_VERSION", ""):
            from . import remediation_capacity_observer_v2 as capacity
            if os.environ["MARKET_DATA_REMEDIATION_CAPACITY_OBSERVER_VERSION"] != capacity.VERSION:
                raise WorkerFault("capacity_observer_version_invalid")
            import sys
            capacity_monitor = capacity.CapacityMonitor(sys.modules[__name__], rpc)
            capacity_monitor.start()
        metrics_probe_version = os.environ.get("MARKET_DATA_REMEDIATION_METRICS_PROBE_VERSION", "")
        if metrics_probe_version:
            from . import remediation_filesystem_probe_v1 as metrics_probe
            if metrics_probe_version != metrics_probe.VERSION:
                raise WorkerFault("readonly_metrics_probe_version_invalid")
            import sys
            if not metrics_probe.run_probe(sys.modules[__name__], rpc):
                raise WorkerFault("readonly_metrics_probe_did_not_produce_evidence")
        feature_lane = os.environ.get("MARKET_DATA_REMEDIATION_EXECUTION_LANE", "source")
        if feature_lane not in ("source", "compact_features", "native_listings"):
            raise WorkerFault("remediation_execution_lane_invalid")
        if feature_lane == "native_listings":
            if os.environ.get("MARKET_DATA_REMEDIATION_COINBASE_COHORT", "").lower()=="true":
                raise WorkerFault("remediation_conflicting_execution_lanes")
            from . import remediation_native_listing_v1 as native_listings
            import sys
            worker = native_listings.create_worker(sys.modules[__name__],rpc)
        elif feature_lane == "compact_features":
            if os.environ.get("MARKET_DATA_REMEDIATION_COINBASE_COHORT", "").lower()=="true":
                raise WorkerFault("remediation_conflicting_execution_lanes")
            import sys
            if os.environ.get("MARKET_DATA_REMEDIATION_NATIVE_INTERLEAVE", "").lower()=="true":
                from . import remediation_pipeline_v1 as pipeline
                worker = pipeline.create_worker(sys.modules[__name__],rpc)
            else:
                from . import remediation_compact_features_v1 as compact_features
                worker = compact_features.create_worker(sys.modules[__name__],rpc)
        elif os.environ.get("MARKET_DATA_REMEDIATION_COINBASE_COHORT", "").lower()=="true":
            from . import remediation_coinbase_cohort_v1 as cohort
            import sys
            worker = cohort.create_worker(sys.modules[__name__],rpc)
        else:
            worker = Worker(rpc)
    except WorkerFault as exc:
        emit("startup_blocked",code=exc.code)
        return
    for sig in (signal.SIGTERM,signal.SIGINT):
        signal.signal(sig,lambda *_:worker.stop.set())
    emit("isolated_worker_started",run_id=RUN_ID,project_ref=PROJECT_REF,rpc_mode=rpc.mode,
         max_requests=None if rpc.budgets_removed else worker.max_requests,max_batches=None if rpc.budgets_removed else worker.max_batches,budgets_removed=rpc.budgets_removed)
    try:
        worker.run()
    finally:
        if capacity_monitor is not None:
            capacity_monitor.stop.set()


if __name__=="__main__":
    main()


