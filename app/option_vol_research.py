from __future__ import annotations

import logging
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from functools import lru_cache
from typing import Any
from zoneinfo import ZoneInfo

from psycopg.types.json import Jsonb

from app.config import get_settings
from app.db import db_connection
from app.exceptions import ProviderError
from app.http import JsonHttpClient


logger = logging.getLogger(__name__)
ET = ZoneInfo("America/New_York")


def _as_utc(value: str | datetime) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _as_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _as_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


@lru_cache(maxsize=1)
def _alpaca_http() -> JsonHttpClient:
    settings = get_settings()
    headers = {
        "APCA-API-KEY-ID": settings.alpaca_api_key,
        "APCA-API-SECRET-KEY": settings.alpaca_api_secret,
    }
    return JsonHttpClient(settings.alpaca_requests_per_minute, headers=headers)


def claim_option_vol_event(worker_id: str) -> dict[str, Any] | None:
    """Durably claim one low-priority option-research event."""
    with db_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            with candidate as (
                select e.id
                  from option_vol_research_events e
                  join option_vol_research_runs r on r.id=e.run_id
                 where e.status in ('queued','retry_wait')
                   and e.not_before <= now()
                   and r.status in ('queued','running')
                 order by e.created_at,e.bucket_start,e.sample_class
                 for update of e skip locked
                 limit 1
            )
            update option_vol_research_events e
               set status='running',attempts=e.attempts+1,locked_by=%s,locked_at=now(),updated_at=now()
              from candidate c
             where e.id=c.id
            returning e.*
            """,
            (worker_id,),
        )
        row = cur.fetchone()
        if not row:
            conn.commit()
            return None
        cur.execute(
            """
            update option_vol_research_runs
               set status='running',started_at=coalesce(started_at,now()),updated_at=now()
             where id=%s and status='queued'
            """,
            (row["run_id"],),
        )
        cur.execute(
            """
            select underlying_symbol,dte_buckets,control_spec,execution_spec
              from option_vol_research_runs where id=%s
            """,
            (row["run_id"],),
        )
        run = cur.fetchone()
        conn.commit()
        return {**row, "run": run}


def reclaim_stale_option_vol_events(stale_minutes: int = 20) -> int:
    with db_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            update option_vol_research_events
               set status='retry_wait',locked_by=null,locked_at=null,not_before=now(),updated_at=now(),
                   last_error=coalesce(last_error,'stale running event reclaimed')
             where status='running'
               and locked_at < now() - (%s * interval '1 minute')
               and attempts < max_attempts
            returning id
            """,
            (stale_minutes,),
        )
        rows = cur.fetchall()
        conn.commit()
        return len(rows)


def _fetch_contracts(underlying: str, entry_date: date, max_dte: int, spot: float) -> list[dict[str, Any]]:
    """Fetch expired historical contracts only; selection never uses option outcomes or OI."""
    client = _alpaca_http()
    page_token: str | None = None
    contracts: list[dict[str, Any]] = []
    upper = entry_date + timedelta(days=max_dte)
    # The +/-10% strike bounds are transport-only and cannot affect ATM selection
    # unless an implausibly large gap leaves no near-ATM contracts, in which case
    # the event is skipped rather than widened after outcomes are observed.
    while True:
        params: dict[str, Any] = {
            "underlying_symbols": underlying,
            "status": "inactive",
            "expiration_date_gte": entry_date.isoformat(),
            "expiration_date_lte": upper.isoformat(),
            "strike_price_gte": f"{spot * 0.90:.4f}",
            "strike_price_lte": f"{spot * 1.10:.4f}",
            "limit": 10000,
        }
        if page_token:
            params["page_token"] = page_token
        payload = client.get("https://paper-api.alpaca.markets/v2/options/contracts", params=params)
        contracts.extend(payload.get("option_contracts") or [])
        page_token = payload.get("next_page_token") or payload.get("page_token")
        if not page_token:
            break
    return contracts


def select_contract_pairs(
    contracts: list[dict[str, Any]],
    *,
    entry_date: date,
    spot: float,
    dte_buckets: dict[str, Any],
) -> list[dict[str, Any]]:
    """Choose earliest expiry in each frozen DTE bucket, then nearest ATM strike.

    Contract open interest is intentionally ignored because historical point-in-time
    OI is not guaranteed by the contracts endpoint.
    """
    pairs: dict[tuple[date, float], dict[str, dict[str, Any]]] = defaultdict(dict)
    for contract in contracts:
        symbol = str(contract.get("symbol") or "")
        option_type = str(contract.get("type") or "").lower()
        expiry_raw = contract.get("expiration_date")
        strike = _as_float(contract.get("strike_price"))
        if not symbol or option_type not in {"call", "put"} or not expiry_raw or strike is None:
            continue
        expiry = date.fromisoformat(str(expiry_raw))
        pairs[(expiry, strike)][option_type] = contract

    complete = [
        (expiry, strike, sides)
        for (expiry, strike), sides in pairs.items()
        if "call" in sides and "put" in sides
    ]
    selected: list[dict[str, Any]] = []
    for bucket_name, spec in sorted(
        dte_buckets.items(), key=lambda kv: (int(kv[1]["min"]), int(kv[1]["max"]), kv[0])
    ):
        lo, hi = int(spec["min"]), int(spec["max"])
        eligible = [row for row in complete if lo <= (row[0] - entry_date).days <= hi]
        if not eligible:
            continue
        earliest_dte = min((row[0] - entry_date).days for row in eligible)
        eligible = [row for row in eligible if (row[0] - entry_date).days == earliest_dte]
        expiry, strike, sides = min(eligible, key=lambda row: (abs(row[1] - spot), row[1]))
        selected.append(
            {
                "dte_bucket": bucket_name,
                "expiration_date": expiry,
                "strike": strike,
                "call_symbol": sides["call"]["symbol"],
                "put_symbol": sides["put"]["symbol"],
                "call_open_interest": _as_int(sides["call"].get("open_interest")),
                "put_open_interest": _as_int(sides["put"].get("open_interest")),
                "metadata": {
                    "selection": "earliest_expiry_then_nearest_atm_lower_strike_tiebreak",
                    "dte": earliest_dte,
                    "open_interest_used_for_selection": False,
                },
            }
        )
    return selected


def _fetch_option_bars(symbols: list[str], start: datetime, end: datetime) -> list[dict[str, Any]]:
    client = _alpaca_http()
    page_token: str | None = None
    rows: list[dict[str, Any]] = []
    while True:
        params: dict[str, Any] = {
            "symbols": ",".join(symbols),
            "timeframe": "1Min",
            "start": start.isoformat(),
            "end": end.isoformat(),
            "limit": 10000,
            "sort": "asc",
        }
        if page_token:
            params["page_token"] = page_token
        payload = client.get("https://data.alpaca.markets/v1beta1/options/bars", params=params)
        raw = payload.get("bars") or {}
        if isinstance(raw, dict):
            for symbol, bars in raw.items():
                for bar in bars or []:
                    rows.append(_normalise_bar(str(symbol), bar))
        elif isinstance(raw, list):
            for bar in raw:
                symbol = str(bar.get("S") or bar.get("symbol") or "")
                if symbol:
                    rows.append(_normalise_bar(symbol, bar))
        page_token = payload.get("next_page_token")
        if not page_token:
            break
    return rows


def _normalise_bar(symbol: str, bar: dict[str, Any]) -> dict[str, Any]:
    return {
        "contract_symbol": symbol,
        "ts": _as_utc(bar["t"]),
        "open": _as_float(bar.get("o")),
        "high": _as_float(bar.get("h")),
        "low": _as_float(bar.get("l")),
        "close": _as_float(bar.get("c")),
        "volume": _as_float(bar.get("v")),
        "trade_count": _as_int(bar.get("n")),
        "vwap": _as_float(bar.get("vw")),
    }


def exact_straddle_result(
    rows: list[dict[str, Any]],
    *,
    call_symbol: str,
    put_symbol: str,
    entry_ts: datetime,
    exit_ts: datetime,
    spy_open: float,
) -> dict[str, Any]:
    by_key = {(row["contract_symbol"], row["ts"]): row for row in rows}
    call_entry = by_key.get((call_symbol, entry_ts))
    put_entry = by_key.get((put_symbol, entry_ts))
    call_exit = by_key.get((call_symbol, exit_ts))
    put_exit = by_key.get((put_symbol, exit_ts))
    if not all([call_entry, put_entry, call_exit, put_exit]):
        return {
            "complete": False,
            "notes": {
                "missing_exact_bars": [
                    label
                    for label, value in [
                        ("call_entry", call_entry), ("put_entry", put_entry),
                        ("call_exit", call_exit), ("put_exit", put_exit),
                    ]
                    if value is None
                ],
                "execution_layer": "trade_bar_proxy_not_bid_ask",
            },
        }
    entry_call = call_entry["open"]
    entry_put = put_entry["open"]
    exit_call = call_exit["close"]
    exit_put = put_exit["close"]
    if None in {entry_call, entry_put, exit_call, exit_put}:
        return {"complete": False, "notes": {"missing_prices": True}}
    entry_straddle = float(entry_call) + float(entry_put)
    exit_straddle = float(exit_call) + float(exit_put)
    if entry_straddle <= 0 or spy_open <= 0:
        return {"complete": False, "notes": {"invalid_entry_premium": entry_straddle}}
    return {
        "entry_call": entry_call,
        "entry_put": entry_put,
        "entry_straddle": entry_straddle,
        "exit_call": exit_call,
        "exit_put": exit_put,
        "exit_straddle": exit_straddle,
        "gross_return": exit_straddle / entry_straddle - 1.0,
        "premium_to_spy": entry_straddle / spy_open,
        "complete": True,
        "notes": {
            "execution_layer": "trade_bar_proxy_not_bid_ask",
            "spread_slippage_commission_included": False,
            "exact_entry_exit_minutes_required": True,
        },
    }


def _spy_range(entry_ts: datetime, exit_ts: datetime) -> float | None:
    with db_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            select max(high)/nullif(min(low),0)-1 as range30
              from market_bars_1m_alpaca
             where provider='alpaca'
               and instrument_id='238e66b7-86f3-45eb-8769-a1ab64234540'
               and ts between %s and %s
            """,
            (entry_ts, exit_ts),
        )
        row = cur.fetchone()
        conn.commit()
        return _as_float(row["range30"]) if row else None


def _store_event_data(event: dict[str, Any], pairs: list[dict[str, Any]], rows: list[dict[str, Any]]) -> int:
    spy_range = _spy_range(event["entry_ts"], event["exit_ts"])
    complete_count = 0
    with db_connection() as conn, conn.cursor() as cur:
        for pair in pairs:
            cur.execute(
                """
                insert into option_vol_research_contracts(
                    event_id,dte_bucket,expiration_date,strike,call_symbol,put_symbol,
                    call_open_interest,put_open_interest,metadata
                ) values (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                on conflict(event_id,dte_bucket) do update set
                    expiration_date=excluded.expiration_date,strike=excluded.strike,
                    call_symbol=excluded.call_symbol,put_symbol=excluded.put_symbol,
                    call_open_interest=excluded.call_open_interest,put_open_interest=excluded.put_open_interest,
                    metadata=excluded.metadata
                """,
                (
                    event["id"], pair["dte_bucket"], pair["expiration_date"], pair["strike"],
                    pair["call_symbol"], pair["put_symbol"], pair["call_open_interest"], pair["put_open_interest"],
                    Jsonb(pair["metadata"]),
                ),
            )
        for row in rows:
            cur.execute(
                """
                insert into option_vol_research_bars(
                    event_id,contract_symbol,ts,open,high,low,close,volume,trade_count,vwap
                ) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                on conflict(event_id,contract_symbol,ts) do update set
                    open=excluded.open,high=excluded.high,low=excluded.low,close=excluded.close,
                    volume=excluded.volume,trade_count=excluded.trade_count,vwap=excluded.vwap
                """,
                (
                    event["id"], row["contract_symbol"], row["ts"], row["open"], row["high"], row["low"],
                    row["close"], row["volume"], row["trade_count"], row["vwap"],
                ),
            )
        for pair in pairs:
            result = exact_straddle_result(
                rows,
                call_symbol=pair["call_symbol"],put_symbol=pair["put_symbol"],
                entry_ts=event["entry_ts"],exit_ts=event["exit_ts"],spy_open=float(event["spy_open"]),
            )
            if result.get("complete"):
                complete_count += 1
            cur.execute(
                """
                insert into option_vol_research_results(
                    event_id,dte_bucket,entry_call,entry_put,entry_straddle,exit_call,exit_put,exit_straddle,
                    gross_return,premium_to_spy,spy_range_30m,complete,notes,updated_at
                ) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,now())
                on conflict(event_id,dte_bucket) do update set
                    entry_call=excluded.entry_call,entry_put=excluded.entry_put,entry_straddle=excluded.entry_straddle,
                    exit_call=excluded.exit_call,exit_put=excluded.exit_put,exit_straddle=excluded.exit_straddle,
                    gross_return=excluded.gross_return,premium_to_spy=excluded.premium_to_spy,
                    spy_range_30m=excluded.spy_range_30m,complete=excluded.complete,notes=excluded.notes,updated_at=now()
                """,
                (
                    event["id"],pair["dte_bucket"],result.get("entry_call"),result.get("entry_put"),
                    result.get("entry_straddle"),result.get("exit_call"),result.get("exit_put"),
                    result.get("exit_straddle"),result.get("gross_return"),result.get("premium_to_spy"),
                    spy_range,bool(result.get("complete")),Jsonb(result.get("notes") or {}),
                ),
            )
        conn.commit()
    return complete_count


def _refresh_run(run_id: Any) -> None:
    with db_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            select count(*) filter(where status in ('completed','skipped')) as done,
                   count(*) filter(where status='failed') as failed,
                   count(*) filter(where status in ('queued','retry_wait','running')) as pending
              from option_vol_research_events where run_id=%s
            """,
            (run_id,),
        )
        counts = cur.fetchone()
        pending = int(counts["pending"] or 0)
        failed = int(counts["failed"] or 0)
        status = "running" if pending else ("completed_with_errors" if failed else "completed")
        cur.execute(
            """
            update option_vol_research_runs
               set events_completed=%s,events_failed=%s,status=%s,
                   completed_at=case when %s=0 then coalesce(completed_at,now()) else null end,
                   updated_at=now()
             where id=%s
            """,
            (int(counts["done"] or 0),failed,status,pending,run_id),
        )
        conn.commit()


def _skip_event(event: dict[str, Any], reason: str) -> None:
    with db_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            update option_vol_research_events
               set status='skipped',last_error=%s,locked_by=null,locked_at=null,updated_at=now()
             where id=%s
            """,
            (reason,event["id"]),
        )
        conn.commit()
    _refresh_run(event["run_id"])


def _fail_event(event: dict[str, Any], exc: Exception) -> None:
    retryable = not isinstance(exc, ProviderError) or exc.retryable
    attempts = int(event.get("attempts") or 1)
    max_attempts = int(event.get("max_attempts") or 5)
    will_retry = retryable and attempts < max_attempts
    delay_minutes = min(30, 2 ** min(attempts, 4))
    with db_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            update option_vol_research_events
               set status=%s,last_error=%s,locked_by=null,locked_at=null,
                   not_before=case when %s then now()+(%s*interval '1 minute') else not_before end,
                   updated_at=now()
             where id=%s
            """,
            ("retry_wait" if will_retry else "failed",f"{type(exc).__name__}: {exc}",will_retry,delay_minutes,event["id"]),
        )
        conn.commit()
    _refresh_run(event["run_id"])


def process_option_vol_event(event: dict[str, Any]) -> None:
    """Fetch only the frozen ATM straddle ladder for one matched research event."""
    try:
        underlying = str(event["run"]["underlying_symbol"])
        entry_ts = _as_utc(event["entry_ts"])
        exit_ts = _as_utc(event["exit_ts"])
        entry_date = entry_ts.astimezone(ET).date()
        dte_buckets = dict(event["run"]["dte_buckets"] or {})
        if not dte_buckets:
            _skip_event(event,"no DTE buckets configured")
            return
        max_dte = max(int(spec["max"]) for spec in dte_buckets.values())
        contracts = _fetch_contracts(underlying,entry_date,max_dte,float(event["spy_open"]))
        pairs = select_contract_pairs(contracts,entry_date=entry_date,spot=float(event["spy_open"]),dte_buckets=dte_buckets)
        if not pairs:
            _skip_event(event,"no complete ATM call/put pair in frozen DTE buckets")
            return
        symbols = sorted({symbol for pair in pairs for symbol in (pair["call_symbol"],pair["put_symbol"])})
        rows = _fetch_option_bars(symbols,entry_ts,exit_ts)
        complete_count = _store_event_data(event,pairs,rows)
        with db_connection() as conn, conn.cursor() as cur:
            cur.execute(
                """
                update option_vol_research_events
                   set status='completed',last_error=%s,locked_by=null,locked_at=null,updated_at=now()
                 where id=%s
                """,
                (None if complete_count else "no DTE bucket had exact entry+exit bars",event["id"]),
            )
            conn.commit()
        _refresh_run(event["run_id"])
    except Exception as exc:
        logger.exception("Option-vol event failed id=%s",event.get("id"))
        _fail_event(event,exc)
