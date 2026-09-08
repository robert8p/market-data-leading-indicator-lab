from __future__ import annotations

import hashlib
import json
import logging
import os
import threading
import time
from datetime import datetime, timezone
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
from urllib.request import Request, urlopen

logger = logging.getLogger(__name__)
_started = False
_start_lock = threading.Lock()


def _truthy(name: str) -> bool:
    return os.getenv(name, "").strip().lower() in {"1", "true", "yes", "on"}


def _json_hash(value) -> str:
    raw = json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _clean_next_url(url: str | None) -> str | None:
    if not url:
        return None
    parts = urlsplit(url)
    query = [(k, v) for k, v in parse_qsl(parts.query, keep_blank_values=True) if k.lower() not in {"apikey", "api_key"}]
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query), parts.fragment))


class SupabaseRPC:
    def __init__(self) -> None:
        self.base = os.getenv("SUPABASE_URL", "").strip().rstrip("/")
        self.key = os.getenv("SUPABASE_SERVICE_ROLE_KEY", "").strip()
        if not self.base or not self.key:
            raise RuntimeError("SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY absent")

    def call(self, function: str, payload: dict | None = None):
        body = json.dumps(payload or {}, separators=(",", ":"), default=str).encode("utf-8")
        request = Request(
            f"{self.base}/rest/v1/rpc/{function}",
            data=body,
            headers={
                "Authorization": f"Bearer {self.key}",
                "apikey": self.key,
                "Content-Type": "application/json",
                "Accept": "application/json",
                "User-Agent": "market-data-leading-indicator-lab/equity-reference-backfill-v1",
            },
            method="POST",
        )
        for attempt in range(20):
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
                    message = str(parsed.get("message") or parsed.get("details") or parsed.get("hint") or "")[:800]
                except Exception:
                    message = ""
                if exc.code in {429, 502, 503, 504} and attempt < 19:
                    retry_after = exc.headers.get("Retry-After") if exc.headers else None
                    try:
                        pause = max(float(retry_after), min(60.0, 2.0 ** attempt)) if retry_after else min(60.0, 2.0 ** attempt)
                    except ValueError:
                        pause = min(60.0, 2.0 ** attempt)
                    logger.warning("Equity reference Supabase RPC transient function=%s status=%s attempt=%s", function, exc.code, attempt + 1)
                    time.sleep(max(1.0, pause))
                    continue
                raise RuntimeError(f"Supabase RPC {function} HTTP {exc.code}: {message}") from exc
            except URLError as exc:
                if attempt < 19:
                    logger.warning("Equity reference Supabase RPC network retry function=%s attempt=%s", function, attempt + 1)
                    time.sleep(min(60.0, 2.0 ** attempt))
                    continue
                raise RuntimeError(f"Supabase RPC {function} network error {type(exc).__name__}") from exc
        raise RuntimeError(f"Supabase RPC {function} exhausted retries")


class MassiveClient:
    def __init__(self) -> None:
        self.api_key = os.getenv("MASSIVE_API_KEY", "").strip()
        if not self.api_key:
            raise RuntimeError("MASSIVE_API_KEY absent")
        try:
            rpm = max(1.0, float(os.getenv("MASSIVE_RPM", "60")))
        except ValueError:
            rpm = 60.0
        self.interval = max(0.1, 60.0 / rpm)
        self.last_request = 0.0

    def _throttle(self) -> None:
        elapsed = time.monotonic() - self.last_request
        if self.last_request and elapsed < self.interval:
            time.sleep(self.interval - elapsed)

    def get(self, path_or_url: str, params: dict | None = None) -> dict:
        retry = 0
        while True:
            self._throttle()
            if path_or_url.startswith("http"):
                url = _clean_next_url(path_or_url) or path_or_url
                if params:
                    url += ("&" if "?" in url else "?") + urlencode({k: v for k, v in params.items() if v is not None})
            else:
                query = urlencode({k: v for k, v in (params or {}).items() if v is not None})
                url = f"https://api.massive.com{path_or_url}" + (f"?{query}" if query else "")
            request = Request(
                url,
                headers={
                    "Authorization": f"Bearer {self.api_key}",
                    "Accept": "application/json",
                    "User-Agent": "market-data-leading-indicator-lab/equity-reference-backfill-v1",
                },
                method="GET",
            )
            try:
                with urlopen(request, timeout=90) as response:
                    raw = response.read()
                self.last_request = time.monotonic()
                payload = json.loads(raw.decode("utf-8", errors="replace"))
                if not isinstance(payload, dict):
                    raise RuntimeError("Massive response was not an object")
                return payload
            except HTTPError as exc:
                self.last_request = time.monotonic()
                raw = exc.read()
                try:
                    parsed = json.loads(raw.decode("utf-8", errors="replace"))
                    message = str(parsed.get("message") or parsed.get("error") or "")[:500]
                except Exception:
                    message = ""
                if exc.code == 429 and retry < 12:
                    retry += 1
                    retry_after = exc.headers.get("Retry-After") if exc.headers else None
                    try:
                        pause = max(2.0, float(retry_after)) if retry_after else 65.0
                    except ValueError:
                        pause = 65.0
                    logger.warning("Equity reference Massive rate limit retry=%s", retry)
                    time.sleep(pause)
                    continue
                raise RuntimeError(f"Massive HTTP {exc.code}: {message}") from exc
            except URLError as exc:
                if retry < 5:
                    retry += 1
                    time.sleep(min(60.0, 5.0 * retry))
                    continue
                raise RuntimeError(f"Massive network error {type(exc).__name__}") from exc


class AlpacaClient:
    def __init__(self) -> None:
        key = os.getenv("ALPACA_API_KEY", "").strip()
        secret = (os.getenv("ALPACA_API_SECRET", "") or os.getenv("ALPACA_SECRET_KEY", "")).strip()
        if not key or not secret:
            raise RuntimeError("Alpaca corporate-action credentials absent")
        self.headers = {
            "APCA-API-KEY-ID": key,
            "APCA-API-SECRET-KEY": secret,
            "Accept": "application/json",
            "User-Agent": "market-data-leading-indicator-lab/equity-reference-backfill-v1",
        }

    def get(self, params: dict) -> dict:
        url = "https://data.alpaca.markets/v1/corporate-actions?" + urlencode({k: v for k, v in params.items() if v is not None})
        request = Request(url, headers=self.headers, method="GET")
        retry = 0
        while True:
            try:
                with urlopen(request, timeout=90) as response:
                    raw = response.read()
                payload = json.loads(raw.decode("utf-8", errors="replace"))
                if not isinstance(payload, dict):
                    raise RuntimeError("Alpaca corporate-actions response was not an object")
                return payload
            except HTTPError as exc:
                raw = exc.read()
                try:
                    parsed = json.loads(raw.decode("utf-8", errors="replace"))
                    message = str(parsed.get("message") or parsed.get("error") or "")[:500]
                except Exception:
                    message = ""
                if exc.code == 429 and retry < 12:
                    retry += 1
                    time.sleep(min(60.0, 2.0 ** retry))
                    continue
                raise RuntimeError(f"Alpaca corporate-actions HTTP {exc.code}: {message}") from exc
            except URLError as exc:
                if retry < 5:
                    retry += 1
                    time.sleep(min(60.0, 5.0 * retry))
                    continue
                raise RuntimeError(f"Alpaca corporate-actions network error {type(exc).__name__}") from exc


ALPACA_ACTION_KEYS = {
    "reverse_splits": "REVERSE_SPLIT",
    "forward_splits": "FORWARD_SPLIT",
    "unit_splits": "UNIT_SPLIT",
    "cash_dividends": "CASH_DIVIDEND",
    "stock_dividends": "STOCK_DIVIDEND",
    "spin_offs": "SPIN_OFF",
    "cash_mergers": "CASH_MERGER",
    "stock_mergers": "STOCK_MERGER",
    "stock_and_cash_mergers": "STOCK_AND_CASH_MERGER",
    "redemptions": "REDEMPTION",
    "name_changes": "NAME_CHANGE",
    "worthless_removals": "WORTHLESS_REMOVAL",
    "rights_distributions": "RIGHTS_DISTRIBUTION",
    "partial_calls": "PARTIAL_CALL",
    "reorganizations": "REORGANIZATION",
    "capital_gains_distributions": "CAPITAL_GAINS_DISTRIBUTION",
}


def _stage_massive_day(rpc: SupabaseRPC, massive: MassiveClient, run_id: str, observation_date: str) -> None:
    next_url: str | None = None
    page_no = 0
    provider_rows = 0
    while page_no == 0 or next_url:
        params = None if next_url else {
            "date": observation_date,
            "market": "stocks",
            "active": "true",
            "order": "asc",
            "sort": "ticker",
            "limit": 1000,
        }
        payload = massive.get(next_url or "/v3/reference/tickers", params)
        rows = payload.get("results") or []
        if not isinstance(rows, list):
            raise RuntimeError("Massive reference results were not a list")
        page_no += 1
        provider_rows += len(rows)
        shaped = []
        for item in rows:
            if not isinstance(item, dict):
                continue
            shaped.append({
                "ticker": item.get("ticker"),
                "name": item.get("name"),
                "type": item.get("type"),
                "active": item.get("active"),
                "primary_exchange": item.get("primary_exchange"),
                "market": item.get("market"),
                "locale": item.get("locale"),
                "currency_name": item.get("currency_name"),
                "cik": item.get("cik"),
                "composite_figi": item.get("composite_figi"),
                "share_class_figi": item.get("share_class_figi"),
                "last_updated_utc": item.get("last_updated_utc"),
                "payload": item,
            })
        rpc.call("erbv1_stage_reference_page", {
            "p_run_id": run_id,
            "p_observation_date": observation_date,
            "p_page_no": page_no,
            "p_page_checksum": _json_hash(rows),
            "p_rows": shaped,
        })
        next_url = _clean_next_url(str(payload.get("next_url"))) if payload.get("next_url") else None
    if page_no < 1 or provider_rows < 1:
        raise RuntimeError(f"Massive returned no historical reference rows for {observation_date}")
    rpc.call("erbv1_finalize_reference_day", {
        "p_run_id": run_id,
        "p_observation_date": observation_date,
        "p_page_count": page_no,
        "p_provider_rows": provider_rows,
    })
    logger.info("Equity reference date completed run=%s date=%s pages=%s provider_rows=%s", run_id, observation_date, page_no, provider_rows)


def _pick_symbol(item: dict) -> str | None:
    for key in ("symbol", "old_symbol", "initiating_symbol", "target_symbol", "from_symbol", "source_symbol"):
        value = item.get(key)
        if value:
            return str(value).upper()
    return None


def _pick_new_symbol(item: dict) -> str | None:
    for key in ("new_symbol", "acquirer_symbol", "to_symbol", "new_ticker"):
        value = item.get(key)
        if value:
            return str(value).upper()
    return None


def _num(value):
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _normalise_action(provider: str, action_type: str, item: dict) -> dict:
    split_from = _num(item.get("split_from") if "split_from" in item else item.get("old_rate"))
    split_to = _num(item.get("split_to") if "split_to" in item else item.get("new_rate"))
    ratio = split_to / split_from if split_from not in (None, 0) and split_to is not None else None
    source_symbol = _pick_symbol(item)
    provider_key = str(item.get("id") or item.get("corporate_action_id") or _json_hash([provider, action_type, item]))
    row = {
        "provider": provider,
        "provider_action_key": provider_key,
        "source_symbol": source_symbol,
        "old_symbol": item.get("old_symbol") or source_symbol,
        "new_symbol": _pick_new_symbol(item),
        "action_type": action_type,
        "announced_at": item.get("announced_at") or item.get("announcement_at"),
        "declaration_date": item.get("declaration_date"),
        "record_date": item.get("record_date"),
        "ex_date": item.get("ex_date"),
        "payable_date": item.get("payable_date"),
        "process_date": item.get("process_date"),
        "execution_date": item.get("execution_date"),
        "ratio_new_per_old": ratio,
        "split_from": split_from,
        "split_to": split_to,
        "provider_created_at": item.get("created_at"),
        "provider_updated_at": item.get("updated_at"),
        "raw_payload": item,
        "provenance": {
            "provider": provider,
            "later_loaded_reference_fact": True,
            "predictor_use_permitted": False,
            "acquired_at": datetime.now(timezone.utc).isoformat(),
        },
    }
    row["row_fingerprint"] = _json_hash(row)
    return row


def _upsert_action_batch(rpc: SupabaseRPC, run_id: str, rows: list[dict]) -> None:
    for start in range(0, len(rows), 500):
        rpc.call("erbv1_upsert_actions", {"p_run_id": run_id, "p_rows": rows[start:start + 500]})


def _acquire_actions(rpc: SupabaseRPC, massive: MassiveClient, run_id: str, window_start: str, window_end: str) -> None:
    massive_count = 0
    next_url: str | None = None
    first = True
    while first or next_url:
        first = False
        params = None if next_url else {
            "execution_date.gte": window_start,
            "execution_date.lte": window_end,
            "sort": "execution_date.asc",
            "limit": 5000,
        }
        payload = massive.get(next_url or "/stocks/v1/splits", params)
        raw_rows = payload.get("results") or []
        if not isinstance(raw_rows, list):
            raise RuntimeError("Massive split results were not a list")
        shaped = []
        for item in raw_rows:
            if not isinstance(item, dict):
                continue
            split_from = _num(item.get("split_from"))
            split_to = _num(item.get("split_to"))
            ratio = split_to / split_from if split_from not in (None, 0) and split_to is not None else None
            action_type = "FORWARD_SPLIT" if ratio is not None and ratio > 1 else "REVERSE_SPLIT" if ratio is not None and ratio < 1 else "SPLIT"
            shaped.append(_normalise_action("massive", action_type, item))
        _upsert_action_batch(rpc, run_id, shaped)
        massive_count += len(shaped)
        next_url = _clean_next_url(str(payload.get("next_url"))) if payload.get("next_url") else None

    alpaca = AlpacaClient()
    page_token: str | None = None
    alpaca_count = 0
    while True:
        params = {
            "start": window_start,
            "end": window_end,
            "region": "us",
            "data_quality": "complete",
            "limit": 1000,
            "sort": "asc",
            "page_token": page_token,
        }
        payload = alpaca.get(params)
        shaped = []
        for key, action_type in ALPACA_ACTION_KEYS.items():
            values = payload.get(key) or []
            if not isinstance(values, list):
                continue
            for item in values:
                if isinstance(item, dict):
                    shaped.append(_normalise_action("alpaca", action_type, item))
        _upsert_action_batch(rpc, run_id, shaped)
        alpaca_count += len(shaped)
        page_token = payload.get("next_page_token")
        if not page_token:
            break

    rpc.call("erbv1_finish_action_acquisition", {
        "p_run_id": run_id,
        "p_massive_count": massive_count,
        "p_alpaca_count": alpaca_count,
    })
    logger.info("Equity reference corporate actions acquired run=%s massive=%s alpaca=%s", run_id, massive_count, alpaca_count)


def process_once(worker_id: str) -> bool:
    rpc = SupabaseRPC()
    claim = rpc.call("erbv1_claim_day", {"p_worker_id": worker_id}) or {}
    state = str(claim.get("state") or "none")
    if state in {"none", "busy"}:
        return False
    run_id = str(claim.get("run_id"))
    try:
        massive = MassiveClient()
        if state == "day":
            _stage_massive_day(rpc, massive, run_id, str(claim["observation_date"]))
            return True
        if state == "corporate_actions":
            _acquire_actions(rpc, massive, run_id, str(claim["window_start"]), str(claim["window_end"]))
            return True
        if state == "finalize":
            result = rpc.call("erbv1_finalize_run", {"p_run_id": run_id})
            logger.info("Equity reference staged backfill finalized run=%s result=%s", run_id, result)
            return True
        return False
    except Exception as exc:
        text = f"{type(exc).__name__}: {exc}"[:1800]
        status = "blocked_provider_access" if "HTTP 401" in text or "HTTP 403" in text or "credentials absent" in text.lower() else "retry"
        try:
            rpc.call("erbv1_mark_error", {
                "p_run_id": run_id,
                "p_observation_date": claim.get("observation_date"),
                "p_status": status,
                "p_error": text,
            })
        except Exception:
            logger.exception("Failed to persist equity reference backfill error")
        logger.exception("Equity reference backfill iteration failed run=%s state=%s", run_id, state)
        time.sleep(10.0)
        return True


def _loop() -> None:
    worker_id = f"equity-reference-rest:{os.getenv('RENDER_INSTANCE_ID') or os.uname().nodename}:{os.getpid()}"
    while True:
        try:
            did_work = process_once(worker_id)
            if not did_work:
                time.sleep(5.0)
        except Exception:
            logger.exception("Equity reference REST lane escaped top-level iteration")
            time.sleep(10.0)


def start_background() -> None:
    global _started
    if not _truthy("EQUITY_REFERENCE_BACKFILL_ENABLED"):
        return
    with _start_lock:
        if _started:
            return
        _started = True
        threading.Thread(target=_loop, name="equity-reference-rest-backfill", daemon=True).start()
        logger.info("Started governed REST-only equity reference backfill lane")
