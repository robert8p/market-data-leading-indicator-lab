from __future__ import annotations

import json
import logging
from datetime import date, datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

from app.config import get_settings
from app.db import db_connection
from app.exceptions import ProviderError
from app.http import JsonHttpClient
from app.providers.base import as_float, as_int, as_utc

logger = logging.getLogger(__name__)
ET = ZoneInfo("America/New_York")


class AlpacaOptionResearchClient:
    """Minimal Alpaca client for the frozen SPY option-volatility research queue."""

    def __init__(self) -> None:
        settings = get_settings()
        self.http = JsonHttpClient(
            settings.alpaca_requests_per_minute,
            headers={
                "APCA-API-KEY-ID": settings.alpaca_api_key,
                "APCA-API-SECRET-KEY": settings.alpaca_api_secret,
            },
        )

    def contracts(self, underlying: str, expiration_gte: date, expiration_lte: date) -> list[dict[str, Any]]:
        found: dict[str, dict[str, Any]] = {}
        # Past expiries are inactive. Query active too so the same worker also
        # works for later near-live pilots without changing selection logic.
        for status in ("inactive", "active"):
            token: str | None = None
            while True:
                params: dict[str, Any] = {
                    "underlying_symbols": underlying,
                    "expiration_date_gte": expiration_gte.isoformat(),
                    "expiration_date_lte": expiration_lte.isoformat(),
                    "status": status,
                    "limit": 10000,
                }
                if token:
                    params["page_token"] = token
                payload = self.http.get(
                    "https://paper-api.alpaca.markets/v2/options/contracts",
                    params=params,
                )
                for item in payload.get("option_contracts") or []:
                    symbol = str(item.get("symbol") or "")
                    if symbol:
                        found[symbol] = item
                token = payload.get("next_page_token") or payload.get("page_token")
                if not token:
                    break
        return list(found.values())

    def bars(self, symbols: list[str], start: datetime, end: datetime) -> dict[str, list[dict[str, Any]]]:
        result: dict[str, list[dict[str, Any]]] = {symbol: [] for symbol in symbols}
        token: str | None = None
        while True:
            params: dict[str, Any] = {
                "symbols": ",".join(symbols),
                "timeframe": "1Min",
                "start": start.astimezone(timezone.utc).isoformat(),
                "end": end.astimezone(timezone.utc).isoformat(),
                "limit": 10000,
                "sort": "asc",
            }
            if token:
                params["page_token"] = token
            payload = self.http.get(
                "https://data.alpaca.markets/v1beta1/options/bars",
                params=params,
            )
            raw = payload.get("bars") or {}
            if isinstance(raw, dict):
                for symbol, rows in raw.items():
                    if isinstance(rows, list):
                        result.setdefault(symbol, []).extend(rows)
            elif isinstance(raw, list):
                for row in raw:
                    symbol = str(row.get("S") or row.get("symbol") or "")
                    if symbol:
                        result.setdefault(symbol, []).append(row)
            token = payload.get("next_page_token")
            if not token:
                break
        return result


def _refresh_run(conn, run_id: Any) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            select
                count(*) filter (where status = 'completed') as completed,
                count(*) filter (where status in ('failed','skipped')) as failed,
                count(*) filter (where status in ('queued','retry_wait','running')) as pending
            from public.option_vol_research_events
            where run_id = %s
            """,
            (run_id,),
        )
        counts = cur.fetchone()
        completed = int(counts["completed"] or 0)
        failed = int(counts["failed"] or 0)
        pending = int(counts["pending"] or 0)
        if pending == 0:
            status = "completed_with_errors" if failed else "completed"
            stage = "analysis_ready"
        else:
            status = "running"
            stage = "event_backfill"
        cur.execute(
            """
            update public.option_vol_research_runs
            set status = %s,
                stage = %s,
                events_completed = %s,
                events_failed = %s,
                started_at = coalesce(started_at, now()),
                completed_at = case when %s = 0 then coalesce(completed_at, now()) else completed_at end,
                updated_at = now()
            where id = %s
            """,
            (status, stage, completed, failed, pending, run_id),
        )


def claim_option_event(worker_id: str) -> dict[str, Any] | None:
    with db_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            with candidate as (
                select e.id
                from public.option_vol_research_events e
                join public.option_vol_research_runs r on r.id = e.run_id
                where r.status in ('queued','running')
                  and e.status in ('queued','retry_wait')
                  and e.not_before <= now()
                  and e.attempts < e.max_attempts
                order by e.entry_ts, e.sample_class, e.id
                for update of e skip locked
                limit 1
            ), claimed as (
                update public.option_vol_research_events e
                set status = 'running', attempts = attempts + 1,
                    locked_by = %s, locked_at = now(), last_error = null, updated_at = now()
                from candidate c
                where e.id = c.id
                returning e.*
            )
            select c.*, r.underlying_symbol, r.dte_buckets, r.execution_spec
            from claimed c
            join public.option_vol_research_runs r on r.id = c.run_id
            """,
            (worker_id,),
        )
        row = cur.fetchone()
        if row:
            cur.execute(
                """
                update public.option_vol_research_runs
                set status='running', started_at=coalesce(started_at,now()), updated_at=now()
                where id=%s and status='queued'
                """,
                (row["run_id"],),
            )
        conn.commit()
        return row


def reclaim_stale_option_events() -> int:
    with db_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            update public.option_vol_research_events
            set status = case when attempts >= max_attempts then 'failed' else 'retry_wait' end,
                not_before = case when attempts >= max_attempts then not_before else now() end,
                locked_by = null, locked_at = null,
                last_error = coalesce(last_error,'stale option-event lock reclaimed'), updated_at=now()
            where status='running' and locked_at < now() - interval '15 minutes'
            returning run_id
            """
        )
        rows = cur.fetchall()
        for run_id in {row["run_id"] for row in rows}:
            _refresh_run(conn, run_id)
        conn.commit()
        return len(rows)


def _choose_pair(
    contracts: list[dict[str, Any]], event_date: date, min_dte: int, max_dte: int, spy_open: float
) -> dict[str, Any] | None:
    paired: dict[tuple[date, float], dict[str, dict[str, Any]]] = {}
    for item in contracts:
        try:
            expiration = date.fromisoformat(str(item["expiration_date"]))
            strike = float(item["strike_price"])
        except (KeyError, TypeError, ValueError):
            continue
        dte = (expiration - event_date).days
        option_type = str(item.get("type") or "").lower()
        if not (min_dte <= dte <= max_dte) or option_type not in {"call", "put"}:
            continue
        paired.setdefault((expiration, strike), {})[option_type] = item

    choices: list[tuple[date, float, float, dict[str, dict[str, Any]]]] = []
    for (expiration, strike), legs in paired.items():
        if "call" in legs and "put" in legs:
            choices.append((expiration, abs(strike-spy_open), strike, legs))
    if not choices:
        return None
    expiration, _distance, strike, legs = min(choices, key=lambda item: (item[0], item[1], item[2]))
    return {"expiration_date": expiration, "strike": strike, "call": legs["call"], "put": legs["put"]}


def _bar(symbol: str, raw: dict[str, Any]) -> dict[str, Any] | None:
    raw_ts = raw.get("t") or raw.get("timestamp")
    if not raw_ts:
        return None
    return {
        "contract_symbol": symbol,
        "ts": as_utc(raw_ts),
        "open": as_float(raw.get("o")), "high": as_float(raw.get("h")),
        "low": as_float(raw.get("l")), "close": as_float(raw.get("c")),
        "volume": as_float(raw.get("v")), "trade_count": as_int(raw.get("n")),
        "vwap": as_float(raw.get("vw")),
    }


def _spy_range(conn, entry_ts: datetime, exit_ts: datetime) -> float | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            select max(high) hi, min(low) lo
            from public.market_bars_1m_alpaca
            where provider='alpaca'
              and instrument_id='238e66b7-86f3-45eb-8769-a1ab64234540'::uuid
              and ts >= %s and ts <= %s
            """,
            (entry_ts, exit_ts),
        )
        row = cur.fetchone()
    if not row or row["hi"] is None or row["lo"] in (None,0):
        return None
    return float(row["hi"])/float(row["lo"])-1.0


def _store_bucket(
    conn, event: dict[str, Any], bucket: str, pair: dict[str, Any] | None,
    bars_by_symbol: dict[str, list[dict[str, Any]]] | None, reason: str | None = None,
) -> bool:
    if pair is None:
        with conn.cursor() as cur:
            cur.execute(
                """
                insert into public.option_vol_research_results(event_id,dte_bucket,complete,notes,updated_at)
                values(%s,%s,false,%s::jsonb,now())
                on conflict(event_id,dte_bucket) do update
                set complete=false, notes=excluded.notes, updated_at=now()
                """,
                (event["id"], bucket, json.dumps({"reason": reason or "no_contract_pair"})),
            )
        return False

    call, put = pair["call"], pair["put"]
    call_symbol, put_symbol = str(call["symbol"]), str(put["symbol"])
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into public.option_vol_research_contracts
                (event_id,dte_bucket,expiration_date,strike,call_symbol,put_symbol,
                 call_open_interest,put_open_interest,metadata)
            values(%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb)
            on conflict(event_id,dte_bucket) do update
            set expiration_date=excluded.expiration_date,strike=excluded.strike,
                call_symbol=excluded.call_symbol,put_symbol=excluded.put_symbol,
                call_open_interest=excluded.call_open_interest,put_open_interest=excluded.put_open_interest,
                metadata=excluded.metadata
            """,
            (event["id"], bucket, pair["expiration_date"], pair["strike"], call_symbol, put_symbol,
             as_int(call.get("open_interest")), as_int(put.get("open_interest")),
             json.dumps({"selection":"earliest_expiry_nearest_ATM_same_strike_lower_strike_tiebreak",
                         "call":call,"put":put}, default=str)),
        )

    rows: list[dict[str, Any]] = []
    for symbol in (call_symbol, put_symbol):
        for raw in (bars_by_symbol or {}).get(symbol, []):
            parsed = _bar(symbol, raw)
            if parsed:
                rows.append(parsed)
    if rows:
        with conn.cursor() as cur:
            cur.executemany(
                """
                insert into public.option_vol_research_bars
                    (event_id,contract_symbol,ts,open,high,low,close,volume,trade_count,vwap)
                values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                on conflict(event_id,contract_symbol,ts) do update
                set open=excluded.open,high=excluded.high,low=excluded.low,close=excluded.close,
                    volume=excluded.volume,trade_count=excluded.trade_count,vwap=excluded.vwap
                """,
                [(event["id"],r["contract_symbol"],r["ts"],r["open"],r["high"],r["low"],r["close"],
                  r["volume"],r["trade_count"],r["vwap"]) for r in rows],
            )

    idx = {(r["contract_symbol"],r["ts"]):r for r in rows}
    ce, pe = idx.get((call_symbol,event["entry_ts"])), idx.get((put_symbol,event["entry_ts"]))
    cx, px = idx.get((call_symbol,event["exit_ts"])), idx.get((put_symbol,event["exit_ts"]))
    entry_call = ce["open"] if ce else None
    entry_put = pe["open"] if pe else None
    exit_call = cx["close"] if cx else None
    exit_put = px["close"] if px else None
    entry_straddle = float(entry_call)+float(entry_put) if entry_call is not None and entry_put is not None else None
    exit_straddle = float(exit_call)+float(exit_put) if exit_call is not None and exit_put is not None else None
    gross = exit_straddle/entry_straddle-1.0 if entry_straddle not in (None,0) and exit_straddle is not None else None
    spy_open = as_float(event.get("spy_open"))
    premium_to_spy = entry_straddle/spy_open if entry_straddle is not None and spy_open not in (None,0) else None
    complete = all(v is not None for v in (entry_call,entry_put,exit_call,exit_put))
    missing = [name for name,value in (("entry_call",entry_call),("entry_put",entry_put),("exit_call",exit_call),("exit_put",exit_put)) if value is None]
    notes = {"missing":missing,"exact_entry_ts":event["entry_ts"].isoformat(),
             "exact_exit_ts":event["exit_ts"].isoformat(),"bar_proxy_only":True,"no_stale_fills":True}
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into public.option_vol_research_results
                (event_id,dte_bucket,entry_call,entry_put,entry_straddle,exit_call,exit_put,exit_straddle,
                 gross_return,premium_to_spy,spy_range_30m,complete,notes,updated_at)
            values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,now())
            on conflict(event_id,dte_bucket) do update
            set entry_call=excluded.entry_call,entry_put=excluded.entry_put,entry_straddle=excluded.entry_straddle,
                exit_call=excluded.exit_call,exit_put=excluded.exit_put,exit_straddle=excluded.exit_straddle,
                gross_return=excluded.gross_return,premium_to_spy=excluded.premium_to_spy,
                spy_range_30m=excluded.spy_range_30m,complete=excluded.complete,notes=excluded.notes,updated_at=now()
            """,
            (event["id"],bucket,entry_call,entry_put,entry_straddle,exit_call,exit_put,exit_straddle,
             gross,premium_to_spy,_spy_range(conn,event["entry_ts"],event["exit_ts"]),complete,json.dumps(notes)),
        )
    return complete


def _finish(event: dict[str, Any], status: str, error: str | None = None) -> None:
    with db_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            update public.option_vol_research_events
            set status=%s,locked_by=null,locked_at=null,last_error=%s,updated_at=now()
            where id=%s
            """,
            (status,error,event["id"]),
        )
        _refresh_run(conn,event["run_id"])
        conn.commit()


def _retry(event: dict[str, Any], error: str, retryable: bool) -> None:
    attempts = int(event.get("attempts") or 0)
    terminal = (not retryable) or attempts >= int(event.get("max_attempts") or 5)
    status = "failed" if terminal else "retry_wait"
    delay = min(900,30*(2**max(0,attempts-1)))
    with db_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            update public.option_vol_research_events
            set status=%s,
                not_before=case when %s='retry_wait' then now()+make_interval(secs=>%s) else not_before end,
                locked_by=null,locked_at=null,last_error=%s,updated_at=now()
            where id=%s
            """,
            (status,status,delay,error[:2000],event["id"]),
        )
        _refresh_run(conn,event["run_id"])
        conn.commit()


def process_option_event(event: dict[str, Any]) -> None:
    client = AlpacaOptionResearchClient()
    entry_ts: datetime = event["entry_ts"]
    event_date = entry_ts.astimezone(ET).date()
    spy_open = as_float(event.get("spy_open"))
    if spy_open in (None,0):
        _retry(event,"missing SPY entry open for option contract selection",False)
        return
    buckets = dict(event.get("dte_buckets") or {})
    try:
        complete_count = 0
        for bucket,spec in buckets.items():
            min_dte,max_dte = int(spec["min"]),int(spec["max"])
            contracts = client.contracts(
                str(event.get("underlying_symbol") or "SPY"),
                event_date+timedelta(days=min_dte),event_date+timedelta(days=max_dte),
            )
            pair = _choose_pair(contracts,event_date,min_dte,max_dte,float(spy_open))
            if pair is None:
                with db_connection() as conn:
                    _store_bucket(conn,event,bucket,None,None,"no_same_strike_call_put_pair_in_frozen_dte_bucket")
                    conn.commit()
                continue
            symbols = [str(pair["call"]["symbol"]),str(pair["put"]["symbol"])]
            bars = client.bars(symbols,event["entry_ts"],event["exit_ts"])
            with db_connection() as conn:
                if _store_bucket(conn,event,bucket,pair,bars):
                    complete_count += 1
                conn.commit()
        _finish(event,"completed",None if complete_count else "no frozen DTE bucket had exact entry+exit option bars")
        logger.info("Completed option-vol event id=%s class=%s complete_buckets=%s/%s",
                    event["id"],event.get("sample_class"),complete_count,len(buckets))
    except ProviderError as exc:
        _retry(event,f"{exc.code}: {exc}",exc.retryable)
        logger.warning("Option provider error event=%s code=%s retryable=%s message=%s",
                       event["id"],exc.code,exc.retryable,exc)
    except Exception as exc:
        _retry(event,f"{type(exc).__name__}: {exc}",True)
        logger.exception("Unexpected option-vol event failure id=%s",event["id"])
