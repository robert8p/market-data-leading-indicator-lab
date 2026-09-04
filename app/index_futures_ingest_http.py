from __future__ import annotations

import json
import logging
import os
import re
import threading
import time
from datetime import date, datetime, timedelta, timezone
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

logger = logging.getLogger(__name__)

_MASSIVE_BASE = "https://api.massive.com"
_ROOTS = ("ES", "MES", "NQ", "MNQ", "YM", "MYM", "RTY", "M2K")
_EXPECTED_VENUE = {"ES":"XCME","MES":"XCME","NQ":"XCME","MNQ":"XCME","YM":"XCBT","MYM":"XCBT","RTY":"XCME","M2K":"XCME"}
_START = date(2025, 9, 1)
_END_EXCLUSIVE = date(2026, 9, 1)
_started = False
_start_lock = threading.Lock()


def _enabled() -> bool:
    return os.getenv("INDEX_FUTURES_INGEST_ENABLED", "false").strip().lower() in {"1", "true", "yes", "on"}


def _request_interval() -> float:
    try:
        return max(12.25, float(os.getenv("INDEX_FUTURES_REQUEST_INTERVAL_SECONDS", "12.25")))
    except ValueError:
        return 12.25


class SupabaseRPC:
    def __init__(self) -> None:
        self.base = os.getenv("SUPABASE_URL", "").strip().rstrip("/")
        self.key = os.getenv("SUPABASE_SERVICE_ROLE_KEY", "").strip()
        if not self.base or not self.key:
            raise RuntimeError("SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY absent")

    def call(self, function: str, payload: dict | None = None):
        url = f"{self.base}/rest/v1/rpc/{function}"
        body = json.dumps(payload or {}, separators=(",", ":"), default=str).encode("utf-8")
        request = Request(
            url,
            data=body,
            headers={
                "Authorization": f"Bearer {self.key}",
                "apikey": self.key,
                "Content-Type": "application/json",
                "Accept": "application/json",
                "User-Agent": "market-data-leading-indicator-lab/index-futures-v1",
            },
            method="POST",
        )
        try:
            with urlopen(request, timeout=240) as response:
                raw = response.read()
                if not raw:
                    return None
                return json.loads(raw.decode("utf-8", errors="replace"))
        except HTTPError as exc:
            raw = exc.read()
            try:
                parsed = json.loads(raw.decode("utf-8", errors="replace"))
                message = str(parsed.get("message") or parsed.get("details") or parsed.get("hint") or "")[:500]
            except Exception:
                message = ""
            raise RuntimeError(f"Supabase RPC {function} HTTP {exc.code}: {message}") from exc


class MassiveClient:
    def __init__(self, api_key: str, rpc: SupabaseRPC, run_id: str) -> None:
        self.api_key = api_key
        self.rpc = rpc
        self.run_id = run_id
        self.interval = _request_interval()
        self.last_request = 0.0

    def _throttle(self) -> None:
        elapsed = time.monotonic() - self.last_request
        if self.last_request and elapsed < self.interval:
            time.sleep(self.interval - elapsed)

    def _count(self) -> None:
        self.last_request = time.monotonic()
        self.rpc.call("ifv1_update_run", {"p_run_id": self.run_id, "p_requests_delta": 1})

    def get(self, path_or_url: str, params: dict | None = None) -> dict:
        retry = 0
        while True:
            self._throttle()
            if path_or_url.startswith("http"):
                url = path_or_url
                if params:
                    url += ("&" if "?" in url else "?") + urlencode(params)
            else:
                query = urlencode({k: v for k, v in (params or {}).items() if v is not None})
                url = f"{_MASSIVE_BASE}{path_or_url}" + (f"?{query}" if query else "")
            req = Request(
                url,
                headers={
                    "Authorization": f"Bearer {self.api_key}",
                    "Accept": "application/json",
                    "User-Agent": "market-data-leading-indicator-lab/index-futures-v1",
                },
                method="GET",
            )
            try:
                with urlopen(req, timeout=60) as response:
                    raw = response.read()
                self._count()
                result = json.loads(raw.decode("utf-8", errors="replace"))
                if not isinstance(result, dict):
                    raise RuntimeError("Massive response was not an object")
                return result
            except HTTPError as exc:
                self._count()
                raw = exc.read()
                try:
                    parsed = json.loads(raw.decode("utf-8", errors="replace"))
                except Exception:
                    parsed = {}
                if exc.code == 429 and retry < 12:
                    retry += 1
                    retry_after = exc.headers.get("Retry-After") if exc.headers else None
                    try:
                        pause = max(13.0, float(retry_after)) if retry_after else 65.0
                    except ValueError:
                        pause = 65.0
                    logger.warning("INDEX_FUTURES_INGEST provider rate limit retry=%s", retry)
                    time.sleep(pause)
                    continue
                message = str(parsed.get("message") or parsed.get("error") or "")[:300]
                raise RuntimeError(f"Massive HTTP {exc.code}: {message}") from exc
            except URLError as exc:
                if retry < 5:
                    retry += 1
                    time.sleep(min(60.0, 5.0 * retry))
                    continue
                raise RuntimeError(f"Massive network error {type(exc).__name__}") from exc

    def pages(self, path: str, params: dict):
        next_url = None
        first = True
        while first or next_url:
            first = False
            payload = self.get(next_url or path, params if not next_url else None)
            rows = payload.get("results")
            yield [r for r in rows if isinstance(r, dict)] if isinstance(rows, list) else []
            next_url = str(payload.get("next_url")) if payload.get("next_url") else None


def _as_date(value) -> date | None:
    if not value:
        return None
    try:
        return date.fromisoformat(str(value)[:10])
    except ValueError:
        return None


def _ns_iso(value) -> str:
    return datetime.fromtimestamp(int(value) / 1_000_000_000, tz=timezone.utc).isoformat()


def _chunks(items: list[dict], size: int = 1000):
    for i in range(0, len(items), size):
        yield items[i:i+size]


def _update(rpc: SupabaseRPC, run_id: str, **kwargs) -> None:
    payload = {"p_run_id": run_id}
    mapping = {
        "requests_delta":"p_requests_delta",
        "bars_delta":"p_bars_delta",
        "sessions_delta":"p_sessions_delta",
        "schedules_delta":"p_schedules_delta",
        "contracts_count":"p_contracts_count",
        "checkpoint":"p_checkpoint",
        "status":"p_status",
        "error":"p_error",
    }
    for key, value in kwargs.items():
        if value is not None:
            payload[mapping[key]] = value
    rpc.call("ifv1_update_run", payload)


def _product(client: MassiveClient, rpc: SupabaseRPC, root: str) -> None:
    payload = client.get("/futures/v1/products", {"product_code":root,"date":(_END_EXCLUSIVE-timedelta(days=1)).isoformat(),"limit":10})
    for row in payload.get("results", []):
        if isinstance(row, dict) and row.get("product_code") == root:
            rpc.call("ifv1_upsert_product", {"p_root":root,"p_asof":(_END_EXCLUSIVE-timedelta(days=1)).isoformat(),"p_payload":row})
            return


def _discover(client: MassiveClient, rpc: SupabaseRPC, root: str) -> list[dict]:
    upper = _END_EXCLUSIVE + timedelta(days=130)
    payload = client.get("/futures/v1/contracts", {
        "product_code":root,"type":"single","last_trade_date.gte":_START.isoformat(),
        "last_trade_date.lte":upper.isoformat(),"first_trade_date.lt":_END_EXCLUSIVE.isoformat(),
        "limit":1000,"sort":"last_trade_date.asc",
    })
    pattern = re.compile(rf"^{re.escape(root)}[HMUZ][0-9]{{1,2}}$")
    result = []
    for row in payload.get("results", []):
        if not isinstance(row, dict):
            continue
        ticker = str(row.get("ticker") or "")
        last_trade = _as_date(row.get("last_trade_date"))
        if row.get("product_code") != root or row.get("trading_venue") != _EXPECTED_VENUE[root] or not pattern.match(ticker) or not last_trade:
            continue
        contract_id = rpc.call("ifv1_upsert_contract", {"p_root":root,"p_asof":(_END_EXCLUSIVE-timedelta(days=1)).isoformat(),"p_payload":row})
        result.append({
            "contract_id":str(contract_id),"root":root,"ticker":ticker,"last_trade_date":last_trade,
            "first_trade_date":_as_date(row.get("first_trade_date")),"settlement_date":_as_date(row.get("settlement_date")),
        })
    return result


def _schedules(client: MassiveClient, rpc: SupabaseRPC, run_id: str, root: str) -> None:
    params = {"product_code":root,"session_end_date.gte":_START.isoformat(),"session_end_date.lt":_END_EXCLUSIVE.isoformat(),"limit":1000,"sort":"session_end_date.asc"}
    total = 0
    for page in client.pages("/futures/v1/schedules", params):
        rows = []
        for row in page:
            if row.get("product_code") != root or row.get("trading_venue") != _EXPECTED_VENUE[root]:
                continue
            if not row.get("timestamp") or not row.get("session_end_date"):
                continue
            rows.append({"root":root,"product_code":root,"exchange_mic":_EXPECTED_VENUE[root],"session_end_date":row["session_end_date"],"event":str(row.get("event") or "unknown"),"event_ts_utc":row["timestamp"]})
        if rows:
            n = rpc.call("ifv1_insert_schedules", {"p_rows":rows}) or 0
            total += int(n)
            _update(rpc,run_id,schedules_delta=int(n),checkpoint={"stage":"schedules","root":root,"rows":total})


def _bars(client: MassiveClient, rpc: SupabaseRPC, run_id: str, c: dict) -> None:
    start = max(_START, c["last_trade_date"] - timedelta(days=98))
    end = min(_END_EXCLUSIVE, c["last_trade_date"] + timedelta(days=1))
    if start >= end:
        return
    max_ns = rpc.call("ifv1_max_bar_ns", {"p_contract_id":c["contract_id"]})
    params = {"resolution":"1min","window_start.lt":end.isoformat(),"limit":50000,"sort":"window_start.asc"}
    if max_ns:
        params["window_start.gt"] = int(max_ns)
    else:
        params["window_start.gte"] = start.isoformat()
    total = 0
    lower_dt = datetime.combine(_START, datetime.min.time(), tzinfo=timezone.utc)
    upper_dt = datetime.combine(_END_EXCLUSIVE, datetime.min.time(), tzinfo=timezone.utc)
    for page in client.pages(f"/futures/v1/aggs/{c['ticker']}", params):
        prepared = []
        for row in page:
            ns = row.get("window_start")
            if ns is None:
                continue
            try:
                dt = datetime.fromtimestamp(int(ns)/1_000_000_000,tz=timezone.utc)
                if dt < lower_dt or dt >= upper_dt:
                    continue
                o=float(row["open"]); h=float(row["high"]); l=float(row["low"]); cl=float(row["close"])
                if h < l or not (l <= o <= h) or not (l <= cl <= h):
                    continue
            except (KeyError,TypeError,ValueError,OverflowError):
                continue
            vol=row.get("volume"); tx=row.get("transactions"); dv=row.get("dollar_volume")
            try: vwap=float(dv)/int(vol) if dv is not None and vol not in (None,0) else None
            except (TypeError,ValueError,ZeroDivisionError): vwap=None
            prepared.append({
                "contract_id":c["contract_id"],"root":c["root"],"ticker":c["ticker"],"ts_utc":dt.isoformat(),"ts_ns":int(ns),
                "session_end_date":row.get("session_end_date"),"open":o,"high":h,"low":l,"close":cl,
                "volume":int(vol) if vol is not None else None,"trade_count":int(tx) if tx is not None else None,
                "dollar_volume":float(dv) if dv is not None else None,"vwap":vwap,
                "settlement_price":float(row["settlement_price"]) if row.get("settlement_price") is not None else None,
            })
        for chunk in _chunks(prepared):
            n = rpc.call("ifv1_insert_bars", {"p_rows":chunk}) or 0
            total += int(n)
            _update(rpc,run_id,bars_delta=int(n),checkpoint={"stage":"bars_1m","root":c["root"],"contract":c["ticker"],"rows":total})
        logger.info("INDEX_FUTURES_INGEST stage=bars root=%s ticker=%s source_rows=%s upserted=%s",c["root"],c["ticker"],len(page),total)


def _sessions(client: MassiveClient, rpc: SupabaseRPC, run_id: str, c: dict) -> None:
    start=max(_START,c["last_trade_date"]-timedelta(days=98)); end=min(_END_EXCLUSIVE,c["last_trade_date"]+timedelta(days=1))
    if start>=end: return
    existing=rpc.call("ifv1_session_count", {"p_contract_id":c["contract_id"],"p_start":start.isoformat(),"p_end":end.isoformat()}) or 0
    if int(existing)>=40: return
    params={"resolution":"1session","window_start.gte":(start-timedelta(days=1)).isoformat(),"window_start.lt":end.isoformat(),"limit":1000,"sort":"window_start.asc"}
    for page in client.pages(f"/futures/v1/aggs/{c['ticker']}",params):
        rows=[]
        for row in page:
            sd=_as_date(row.get("session_end_date")); ns=row.get("window_start")
            if not sd or sd<_START or sd>=_END_EXCLUSIVE: continue
            vol=row.get("volume"); tx=row.get("transactions"); dv=row.get("dollar_volume")
            try: vwap=float(dv)/int(vol) if dv is not None and vol not in (None,0) else None
            except (TypeError,ValueError,ZeroDivisionError): vwap=None
            rows.append({
                "contract_id":c["contract_id"],"root":c["root"],"ticker":c["ticker"],"session_end_date":sd.isoformat(),
                "session_start_ts_utc":_ns_iso(ns) if ns is not None else None,"ts_ns":int(ns) if ns is not None else None,
                "open":row.get("open"),"high":row.get("high"),"low":row.get("low"),"close":row.get("close"),
                "volume":int(vol) if vol is not None else None,"trade_count":int(tx) if tx is not None else None,
                "dollar_volume":float(dv) if dv is not None else None,"vwap":vwap,
                "settlement_price":float(row["settlement_price"]) if row.get("settlement_price") is not None else None,
            })
        if rows:
            n=rpc.call("ifv1_insert_sessions", {"p_rows":rows}) or 0
            _update(rpc,run_id,sessions_delta=int(n),checkpoint={"stage":"sessions","root":c["root"],"contract":c["ticker"],"rows":int(n)})


def run_index_futures_ingestion() -> None:
    try:
        massive_key=os.getenv("MASSIVE_API_KEY","").strip()
        if not massive_key: raise RuntimeError("MASSIVE_API_KEY absent")
        rpc=SupabaseRPC()
        run_id=str(rpc.call("ifv1_ensure_run",{
            "p_start":_START.isoformat(),"p_end_exclusive":_END_EXCLUSIVE.isoformat(),"p_roots":list(_ROOTS),
            "p_config":{"version":"index_futures_ingest_http_v1_fixed_window","contract_window_days_before_expiry":98,"minute_resolution":"1min","minute_page_limit":50000,"transport":"service-role RPC to private schema"},
        }))
        client=MassiveClient(massive_key,rpc,run_id)
        logger.warning("INDEX_FUTURES_INGEST started run_id=%s transport=service_role_rpc",run_id)
        count=0
        for root in _ROOTS:
            _update(rpc,run_id,checkpoint={"stage":"reference","root":root})
            _product(client,rpc,root)
            contracts=_discover(client,rpc,root)
            count+=len(contracts)
            _update(rpc,run_id,contracts_count=count,checkpoint={"stage":"contracts","root":root,"contracts":len(contracts)})
            _schedules(client,rpc,run_id,root)
            for c in contracts:
                _bars(client,rpc,run_id,c)
                _sessions(client,rpc,run_id,c)
        rpc.call("ifv1_insert_source_manifest",{"p_payload":{
            "provider":"massive","source_family":"Massive Futures REST","endpoint_or_family":"/futures/v1/contracts + /products + /schedules + /aggs/{ticker}",
            "access_mechanism":"Existing authorised Render MASSIVE_API_KEY; bearer authentication; credential never persisted in database",
            "requested_start_date":_START.isoformat(),"requested_end_date":(_END_EXCLUSIVE-timedelta(days=1)).isoformat(),
            "available_start_date":_START.isoformat(),"available_end_date":(_END_EXCLUSIVE-timedelta(days=1)).isoformat(),"frequency":"1 minute",
            "fields_available":{"bars":["timestamp","open","high","low","close","volume","transactions","session_end_date"],"session":["settlement_price","dollar_volume"],"reference":["full contract ticker","product_code","venue","first/last trade dates","settlement date","tick sizes"],"schedule":["UTC session events"]},
            "known_limitations":{"history":"Futures Basic: 2 years","rate_limit":"5 API calls/minute","open_interest":"not exposed by current Massive Futures REST","quotes_trades":"not included on Futures Basic","vx":"CFE not covered","settlement_observability":"publication timestamp unavailable; unsafe intraday"},
            "transformation_lineage":"index_futures_ingest_http_v1","contract_mapping_version":"provider_ticker_contract_id_v1","roll_methodology_version":"pending_post_ingest","adjustment_methodology":"pending_post_ingest",
        }})
        _update(rpc,run_id,status="completed_with_limitations",checkpoint={"stage":"raw_complete"})
        logger.warning("INDEX_FUTURES_INGEST completed run_id=%s",run_id)
    except Exception as exc:
        logger.exception("INDEX_FUTURES_INGEST failed")
        try:
            if 'rpc' in locals() and 'run_id' in locals():
                _update(rpc,run_id,status="failed",error=f"{type(exc).__name__}: {str(exc)[:500]}")
        except Exception:
            pass


def start_index_futures_ingestion_if_enabled() -> None:
    global _started
    if not _enabled(): return
    with _start_lock:
        if _started: return
        _started=True
    threading.Thread(target=run_index_futures_ingestion,name="index-futures-ingest",daemon=True).start()
