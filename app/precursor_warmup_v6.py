from __future__ import annotations

import hashlib
import json
import logging
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx

from app.config import get_settings
from app.db import db_connection

logger = logging.getLogger(__name__)

SCHEMA = "market_factors_20250901_20260731_v1"
RUN_KEY = "full_window_20260930_v2"
PLAN = f"{SCHEMA}.precursor_warmup_fast_plan_v6"
CONTROL = f"{SCHEMA}.precursor_warmup_fast_control_v6"
REQUEST = f"{SCHEMA}.precursor_warmup_fast_request_v6"
CALENDAR = f"{SCHEMA}.precursor_warmup_calendar_v6"
SOURCE = f"{SCHEMA}.precursor_warmup_source_v5"
CHECKPOINT = f"{SCHEMA}.precursor_checkpoint_v2"

_PREF = re.compile(r"^(.+)p([A-Z])$")


class WarmupSourceError(RuntimeError):
    def __init__(self, message: str, *, status_code: int | None = None, split_safe: bool = False):
        super().__init__(message)
        self.status_code = status_code
        self.split_safe = split_safe


def reference_to_api_symbol(symbol: str) -> str:
    """Translate ASTRA's CQS preferred-share notation to Alpaca CMS notation."""
    match = _PREF.fullmatch(symbol)
    if not match:
        return symbol
    return f"{match.group(1)}.PR{match.group(2)}"


def _parse_ts(value: str) -> datetime:
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def normalize_bars(api_symbol: str, raw: list[dict[str, Any]], start: datetime, end: datetime) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    last_ts: datetime | None = None
    for item in raw:
        ts_raw = item.get("t", item.get("timestamp"))
        if not ts_raw:
            raise WarmupSourceError(f"missing timestamp for {api_symbol}")
        ts = _parse_ts(str(ts_raw))
        if ts < start or ts >= end:
            raise WarmupSourceError(f"bar outside requested RTH for {api_symbol}: {ts.isoformat()}")
        if last_ts is not None and ts <= last_ts:
            raise WarmupSourceError(f"non-increasing bar timestamps for {api_symbol}")
        last_ts = ts

        def f(short: str, long: str) -> float:
            value = item.get(short, item.get(long))
            if value is None:
                raise WarmupSourceError(f"missing {long} for {api_symbol}")
            return float(value)

        o, h, l, c = f("o", "open"), f("h", "high"), f("l", "low"), f("c", "close")
        v = float(item.get("v", item.get("volume", 0)) or 0)
        n_raw = item.get("n", item.get("trade_count"))
        n = None if n_raw is None else float(n_raw)
        vw_raw = item.get("vw", item.get("vwap"))
        vw = None if vw_raw is None else float(vw_raw)
        if min(o, h, l, c) <= 0 or v < 0 or h < max(o, l, c) or l > min(o, h, c):
            raise WarmupSourceError(f"invalid OHLCV for {api_symbol} at {ts.isoformat()}")
        result.append(
            {
                "symbol": api_symbol,
                "timestamp": ts.isoformat(),
                "open": o,
                "high": h,
                "low": l,
                "close": c,
                "volume": v,
                "trade_count": n,
                "vwap": vw,
            }
        )
    if len(result) > 13:
        raise WarmupSourceError(f"unexpected >13 30Min RTH bars for {api_symbol}")
    return result


def derive_rows(
    reference_symbol: str,
    api_symbol: str,
    bars: list[dict[str, Any]],
    session_date: str,
    regular_close: datetime,
    response_sha256: str,
    request_id: int,
    requested_start: datetime,
    requested_end: datetime,
) -> list[dict[str, Any]]:
    if not bars:
        return []

    event_time = regular_close.astimezone(timezone.utc)
    available_at = event_time + timedelta(minutes=1)
    total_volume = sum(float(x["volume"]) for x in bars)
    counts = [x["trade_count"] for x in bars if x["trade_count"] is not None]
    total_trades = sum(float(x) for x in counts) if counts else None
    source_file = f"postgres://{SCHEMA}/precursor_warmup_fast_request_v6/{request_id}"

    base_provenance = {
        "source": "ALPACA_SIP_PREWINDOW_30MIN_V6_ACCELERATED",
        "feed": "sip",
        "timeframe": "30Min",
        "asof": "-",
        "timezone": "America/New_York",
        "requested_start": requested_start.isoformat(),
        "requested_end": requested_end.isoformat(),
        "source_api_symbol": api_symbol,
        "original_reference_symbol": reference_symbol,
        "source_file": source_file,
        "sha256": response_sha256,
        "symbol_notation_version": "CQS_PREFERRED_TO_CMS_PR_V1",
        "availability_definition": "complete_regular_close_plus_one_minute_pending_historical_identity_reconciliation",
        "vwap": "not_used_30Min_VWAP_differs_from_minute_aggregation",
    }

    full = {
        "source_symbol": reference_symbol,
        "session_date": session_date,
        "source_mode": "RTH_30MIN_AGGREGATED",
        "open": bars[0]["open"],
        "high": max(x["high"] for x in bars),
        "low": min(x["low"] for x in bars),
        "close": bars[-1]["close"],
        "volume": total_volume,
        "trade_count": total_trades,
        "source_period_volume": total_volume,
        "raw_payload": bars,
        "event_time": event_time,
        "available_at": available_at,
        "provenance": {
            **base_provenance,
            "source_mode": "RTH_30MIN_AGGREGATED",
            "observed_bars": len(bars),
            "volume_definition": "sum_native_SIP_RTH_30Min_volume",
        },
    }

    close_start = event_time - timedelta(minutes=30)
    close_bars = [x for x in bars if close_start <= _parse_ts(x["timestamp"]) < event_time]
    rows = [full]
    if close_bars:
        last = close_bars[-1]
        rows.append(
            {
                "source_symbol": reference_symbol,
                "session_date": session_date,
                "source_mode": "CLOSING_30MIN",
                "open": None,
                "high": None,
                "low": None,
                "close": last["close"],
                "volume": None,
                "trade_count": None,
                "source_period_volume": last["volume"],
                "raw_payload": [last],
                "event_time": event_time,
                "available_at": available_at,
                "provenance": {
                    **base_provenance,
                    "source_mode": "CLOSING_30MIN",
                    "observed_bars": 1,
                    "volume_definition": "volume_NULL_period_volume_only",
                },
            }
        )
    return rows


def _control() -> dict[str, Any] | None:
    with db_connection() as conn, conn.cursor() as cur:
        cur.execute(f"select * from {CONTROL} where run_key=%s", (RUN_KEY,))
        row = cur.fetchone()
        conn.commit()
        return row


def _claim(worker_id: str) -> dict[str, Any] | None:
    with db_connection() as conn, conn.cursor() as cur:
        cur.execute(
            f"""
            with cfg as (
              select max_attempts
              from {CONTROL}
              where run_key=%s and enabled=true
            ), picked as (
              select p.session_date,p.batch
              from {PLAN} p,cfg
              where p.status in ('READY','SOURCE_ERROR')
                and p.attempt_count < cfg.max_attempts
                and (p.lease_until is null or p.lease_until < clock_timestamp())
              order by p.session_date,p.batch
              for update skip locked
              limit 1
            )
            update {PLAN} p
            set status='RUNNING',
                attempt_count=p.attempt_count+1,
                owner_identity=%s,
                last_heartbeat=clock_timestamp(),
                lease_until=clock_timestamp()+interval '10 minutes',
                error_summary=null,
                updated_at=clock_timestamp()
            from picked
            where p.session_date=picked.session_date and p.batch=picked.batch
            returning p.*
            """,
            (RUN_KEY, worker_id),
        )
        row = cur.fetchone()
        conn.commit()
        return row


def _calendar(session_date: Any) -> tuple[datetime, datetime]:
    with db_connection() as conn, conn.cursor() as cur:
        cur.execute(
            f"select regular_open,regular_close from {CALENDAR} where session_date=%s",
            (session_date,),
        )
        row = cur.fetchone()
        conn.commit()
    if not row:
        raise WarmupSourceError(f"missing certified warmup calendar for {session_date}")
    return row["regular_open"].astimezone(timezone.utc), row["regular_close"].astimezone(timezone.utc)


def _record_request_start(
    session_date: Any,
    batch: int,
    attempt: int,
    subpart: int,
    reference_symbols: list[str],
    api_symbols: list[str],
    params: dict[str, Any],
    worker_id: str,
) -> int:
    with db_connection() as conn, conn.cursor() as cur:
        cur.execute(
            f"""
            insert into {REQUEST}(
              session_date,batch,attempt_number,subpart,
              requested_reference_symbols,requested_api_symbols,request_params,
              status,worker_identity
            ) values(%s,%s,%s,%s,%s::jsonb,%s::jsonb,%s::jsonb,'IN_FLIGHT',%s)
            returning request_id
            """,
            (
                session_date,
                batch,
                attempt,
                subpart,
                json.dumps(reference_symbols),
                json.dumps(api_symbols),
                json.dumps(params, sort_keys=True, separators=(",", ":")),
                worker_id,
            ),
        )
        request_id = int(cur.fetchone()["request_id"])
        cur.execute(
            f"""
            update {CONTROL}
            set request_count=request_count+1,last_request_at=clock_timestamp(),updated_at=clock_timestamp()
            where run_key=%s
            """,
            (RUN_KEY,),
        )
        conn.commit()
        return request_id


def _record_request_finish(
    request_id: int,
    *,
    status: str,
    http_status: int | None,
    returned_symbols: int | None,
    returned_bars: int | None,
    response_sha256: str | None,
    error_detail: str | None,
) -> None:
    with db_connection() as conn, conn.cursor() as cur:
        cur.execute(
            f"""
            update {REQUEST}
            set status=%s,http_status=%s,returned_symbols=%s,returned_bars=%s,
                response_sha256=%s,error_detail=%s,completed_at=clock_timestamp()
            where request_id=%s
            """,
            (status, http_status, returned_symbols, returned_bars, response_sha256, error_detail, request_id),
        )
        conn.commit()


def _insert_source_rows(rows: list[dict[str, Any]], response_observed_at: datetime) -> None:
    if not rows:
        return
    with db_connection() as conn, conn.cursor() as cur:
        for row in rows:
            cur.execute(
                f"""
                insert into {SOURCE}(
                  source_symbol,session_date,source_mode,close,open,high,low,
                  volume,trade_count,source_period_volume,raw_payload,event_time,
                  available_at,source_observed_at,ingested_at,provenance,identity_state
                ) values(
                  %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s,%s,%s,clock_timestamp(),%s::jsonb,
                  'PENDING_EXACT_HISTORICAL_TICKER_INTERVAL'
                )
                on conflict(source_mode,session_date,source_symbol) do nothing
                """,
                (
                    row["source_symbol"], row["session_date"], row["source_mode"], row["close"],
                    row["open"], row["high"], row["low"], row["volume"], row["trade_count"],
                    row["source_period_volume"], json.dumps(row["raw_payload"], separators=(",", ":")),
                    row["event_time"], row["available_at"], response_observed_at,
                    json.dumps(row["provenance"], sort_keys=True, separators=(",", ":")),
                ),
            )
        conn.commit()


def _finish_partition(job: dict[str, Any], *, status: str, error: str | None, observed_symbols: int, observed_bars: int) -> None:
    with db_connection() as conn, conn.cursor() as cur:
        cur.execute(
            f"""
            update {PLAN}
            set status=%s,owner_identity=null,last_heartbeat=clock_timestamp(),lease_until=null,
                error_summary=%s,updated_at=clock_timestamp()
            where session_date=%s and batch=%s and owner_identity=%s
            """,
            (status, error, job["session_date"], job["batch"], job["owner_identity"]),
        )
        cur.execute(
            f"""
            update {CONTROL}
            set last_progress_at=clock_timestamp(),
                state=case when %s='SOURCE_COMPLETE_PENDING_IDENTITY'
                           then 'RUNNING_SOURCE_RECOVERY'
                           else 'RUNNING_WITH_SOURCE_ERRORS' end,
                blocker=%s,updated_at=clock_timestamp()
            where run_key=%s
            """,
            (status, error, RUN_KEY),
        )
        cur.execute(
            f"""
            insert into {CHECKPOINT}(run_key,family,partition_key,status,rows_written,evidence,updated_at)
            values(%s,'warmup_fast_source_v6',%s,%s,%s,%s::jsonb,clock_timestamp())
            on conflict(run_key,family,partition_key) do update
            set status=excluded.status,rows_written=excluded.rows_written,evidence=excluded.evidence,updated_at=excluded.updated_at
            """,
            (
                RUN_KEY,
                f"{job['session_date']}:{job['batch']}",
                status,
                observed_bars,
                json.dumps(
                    {
                        "observed_symbols": observed_symbols,
                        "observed_bars": observed_bars,
                        "attempt_count": job["attempt_count"],
                        "source": "Alpaca SIP 30Min",
                        "global_completion": False,
                        "error": error,
                    },
                    sort_keys=True,
                    separators=(",", ":"),
                ),
            ),
        )
        conn.commit()


class WarmupFetcher:
    def __init__(self, worker_id: str):
        self.worker_id = worker_id
        self.settings = get_settings()
        self.headers = {
            "APCA-API-KEY-ID": self.settings.alpaca_api_key,
            "APCA-API-SECRET-KEY": self.settings.alpaca_api_secret,
        }
        self.client = httpx.Client(timeout=httpx.Timeout(45.0), follow_redirects=True, headers=self.headers)
        self.last_request_monotonic = 0.0

    def close(self) -> None:
        self.client.close()

    def _pace(self) -> None:
        cfg = _control()
        interval = max(2.2, float((cfg or {}).get("min_interval_ms") or 2200) / 1000.0)
        elapsed = time.monotonic() - self.last_request_monotonic
        if elapsed < interval:
            time.sleep(interval - elapsed)

    def fetch(
        self,
        job: dict[str, Any],
        reference_symbols: list[str],
        start: datetime,
        close: datetime,
        subpart: int,
    ) -> tuple[dict[str, list[dict[str, Any]]], int]:
        api_symbols = [reference_to_api_symbol(s) for s in reference_symbols]
        if len(set(api_symbols)) != len(api_symbols):
            raise WarmupSourceError("reference->API symbol translation collision")

        end = close - timedelta(milliseconds=1)
        params: dict[str, Any] = {
            "symbols": ",".join(api_symbols),
            "timeframe": "30Min",
            "start": start.isoformat().replace("+00:00", "Z"),
            "end": end.isoformat().replace("+00:00", "Z"),
            "feed": "sip",
            "sort": "asc",
            "limit": 10000,
            "asof": "-",
        }
        request_id = _record_request_start(
            job["session_date"], int(job["batch"]), int(job["attempt_count"]),
            subpart, reference_symbols, api_symbols, params, self.worker_id,
        )
        self._pace()
        observed_at = datetime.now(timezone.utc)
        self.last_request_monotonic = time.monotonic()
        response: httpx.Response | None = None
        body: Any = None
        try:
            response = self.client.get("https://data.alpaca.markets/v2/stocks/bars", params=params)
            raw_text = response.text
            sha = hashlib.sha256(raw_text.encode("utf-8")).hexdigest()
            try:
                body = response.json()
            except Exception as exc:
                raise WarmupSourceError("Alpaca non-JSON response", status_code=response.status_code) from exc

            if response.status_code != 200:
                split_safe = response.status_code in {400, 413, 414}
                raise WarmupSourceError(
                    f"Alpaca HTTP {response.status_code}: {str(body)[:300]}",
                    status_code=response.status_code,
                    split_safe=split_safe,
                )
            if body.get("next_page_token"):
                raise WarmupSourceError("unexpected Alpaca pagination: request would truncate source")
            raw_bars = body.get("bars")
            if not isinstance(raw_bars, dict):
                raise WarmupSourceError("Alpaca bars payload is not a symbol map")
            unexpected = set(raw_bars) - set(api_symbols)
            if unexpected:
                raise WarmupSourceError(f"unexpected Alpaca symbols: {sorted(unexpected)[:5]}")

            normalized: dict[str, list[dict[str, Any]]] = {}
            api_to_reference = dict(zip(api_symbols, reference_symbols))
            source_rows: list[dict[str, Any]] = []
            total_bars = 0
            for api_symbol, values in raw_bars.items():
                if not isinstance(values, list):
                    raise WarmupSourceError(f"Alpaca bars list invalid for {api_symbol}")
                bars = normalize_bars(api_symbol, values, start, close)
                normalized[api_to_reference[api_symbol]] = bars
                total_bars += len(bars)
                source_rows.extend(
                    derive_rows(
                        api_to_reference[api_symbol], api_symbol, bars,
                        str(job["session_date"]), close, sha, request_id, start, end,
                    )
                )

            _insert_source_rows(source_rows, observed_at)
            _record_request_finish(
                request_id, status="SOURCE_RESPONSE_VALIDATED", http_status=200,
                returned_symbols=len(normalized), returned_bars=total_bars,
                response_sha256=sha, error_detail=None,
            )
            return normalized, total_bars
        except Exception as exc:
            status_code = response.status_code if response is not None else getattr(exc, "status_code", None)
            error = f"{type(exc).__name__}: {exc}"[:1000]
            sha = None
            if response is not None:
                sha = hashlib.sha256(response.text.encode("utf-8")).hexdigest()
            _record_request_finish(
                request_id, status="SOURCE_ERROR", http_status=status_code,
                returned_symbols=None, returned_bars=None, response_sha256=sha, error_detail=error,
            )
            raise

    def process(self, job: dict[str, Any]) -> None:
        start, close = _calendar(job["session_date"])
        symbols = list(job["requested_symbols"])
        try:
            first, bars = self.fetch(job, symbols, start, close, 0)
            _finish_partition(
                job, status="SOURCE_COMPLETE_PENDING_IDENTITY", error=None,
                observed_symbols=len(first), observed_bars=bars,
            )
            return
        except WarmupSourceError as exc:
            if not exc.split_safe or len(symbols) <= 125:
                retry_status = "SOURCE_ERROR"
                _finish_partition(job, status=retry_status, error=f"{type(exc).__name__}: {exc}"[:1000], observed_symbols=0, observed_bars=0)
                if exc.status_code == 429:
                    time.sleep(60)
                elif exc.status_code and exc.status_code >= 500:
                    time.sleep(5)
                return
        except (httpx.TimeoutException, httpx.NetworkError) as exc:
            _finish_partition(job, status="SOURCE_ERROR", error=f"{type(exc).__name__}: {exc}"[:1000], observed_symbols=0, observed_bars=0)
            time.sleep(5)
            return

        midpoint = (len(symbols) + 1) // 2
        parts = [symbols[:midpoint], symbols[midpoint:]]
        observed: set[str] = set()
        total_bars = 0
        try:
            for index, part in enumerate(parts, start=1):
                got, count = self.fetch(job, part, start, close, index)
                observed.update(got)
                total_bars += count
            _finish_partition(
                job, status="SOURCE_COMPLETE_PENDING_IDENTITY", error=None,
                observed_symbols=len(observed), observed_bars=total_bars,
            )
        except Exception as exc:
            _finish_partition(
                job, status="SOURCE_ERROR", error=f"{type(exc).__name__}: {exc}"[:1000],
                observed_symbols=len(observed), observed_bars=total_bars,
            )
            time.sleep(5)


def run_precursor_warmup_v6(worker_id: str, shutdown_event: Any) -> None:
    logger.warning("ASTRA precursor warmup v6 lane starting worker_id=%s", worker_id)
    fetcher = WarmupFetcher(f"{worker_id}:precursor_warmup_v6")
    try:
        while not shutdown_event.is_set():
            cfg = _control()
            if not cfg or not cfg.get("enabled"):
                shutdown_event.wait(5)
                continue
            if int(cfg.get("request_count") or 0) >= int(cfg.get("request_limit") or 0):
                with db_connection() as conn, conn.cursor() as cur:
                    cur.execute(
                        f"""
                        update {CONTROL}
                        set enabled=false,state='RESOURCE_GUARD_REVIEW_REQUIRED',
                            blocker='Warmup request guard reached; review existing entitlement/headroom only.',
                            updated_at=clock_timestamp()
                        where run_key=%s
                        """,
                        (RUN_KEY,),
                    )
                    conn.commit()
                continue

            job = _claim(fetcher.worker_id)
            if not job:
                with db_connection() as conn, conn.cursor() as cur:
                    cur.execute(
                        f"""
                        select
                          count(*) filter(where status in ('READY','RUNNING')) as pending,
                          count(*) filter(where status='SOURCE_ERROR') as errors
                        from {PLAN}
                        """
                    )
                    counts = cur.fetchone()
                    if int(counts["pending"] or 0) == 0:
                        state = "SOURCE_QUEUE_DRAINED_RECONCILIATION_REQUIRED" if int(counts["errors"] or 0) == 0 else "SOURCE_QUEUE_DRAINED_WITH_ERRORS"
                        cur.execute(
                            f"""
                            update {CONTROL}
                            set enabled=false,state=%s,
                                blocker=case when %s>0 then %s else null end,
                                updated_at=clock_timestamp()
                            where run_key=%s
                            """,
                            (state, int(counts["errors"] or 0), f"{int(counts['errors'] or 0)} warmup partitions remain SOURCE_ERROR", RUN_KEY),
                        )
                    conn.commit()
                shutdown_event.wait(5)
                continue

            fetcher.process(job)
    finally:
        fetcher.close()
        logger.warning("ASTRA precursor warmup v6 lane stopping worker_id=%s", worker_id)
