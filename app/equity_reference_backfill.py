from __future__ import annotations

import hashlib
import json
import logging
from collections import Counter
from datetime import date, datetime, timezone
from functools import lru_cache
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from psycopg.types.json import Jsonb

from app.config import get_settings
from app.db import db_connection, fetch_all, fetch_one
from app.exceptions import ProviderError
from app.http import JsonHttpClient

logger = logging.getLogger(__name__)
RUN_TABLE = "reference.equity_reference_backfill_run_v1"
DAY_TABLE = "reference.equity_reference_backfill_day_v1"
SCOPE_TABLE = "reference.equity_reference_backfill_scope_v1"
EVENT_TABLE = "reference.equity_reference_state_event_v1"
ACTION_TABLE = "reference.equity_corporate_action_backfill_v1"
STATE_TABLE = "reference.equity_security_state_v1"
ALIAS_TABLE = "reference.equity_symbol_alias_v1"
AUDIT_TABLE = "reference.equity_reference_backfill_audit_v1"

TYPE_MAP = {
    "CS": "ORDINARY_COMMON_EQUITY",
    "ETF": "ETF",
    "ETN": "ETP",
    "ETV": "ETP",
    "ETS": "ETP",
    "FUND": "FUND_OTHER",
    "ADRC": "ADR",
    "ADRP": "ADR",
    "PFD": "PREFERRED_SHARE",
    "WARRANT": "WARRANT",
    "UNIT": "UNIT",
    "RIGHT": "RIGHTS",
    "SP": "OTHER_LISTED_SECURITY",
}

ALPACA_ACTION_KEYS = {
    "reverse_splits": "reverse_split",
    "forward_splits": "forward_split",
    "unit_splits": "unit_split",
    "cash_dividends": "cash_dividend",
    "stock_dividends": "stock_dividend",
    "spin_offs": "spin_off",
    "cash_mergers": "cash_merger",
    "stock_mergers": "stock_merger",
    "stock_and_cash_mergers": "stock_and_cash_merger",
    "redemptions": "redemption",
    "name_changes": "name_change",
    "worthless_removals": "worthless_removal",
    "rights_distributions": "rights_distribution",
    "partial_calls": "partial_call",
    "reorganizations": "reorganization",
    "capital_gains_distributions": "capital_gains_distribution",
}


def _json_hash(value: Any) -> str:
    raw = json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _as_date(value: Any) -> date | None:
    if not value:
        return None
    try:
        return date.fromisoformat(str(value)[:10])
    except (TypeError, ValueError):
        return None


def _as_ts(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    except (TypeError, ValueError):
        return None


def _as_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _normalise_type(raw_type: Any) -> str:
    raw = str(raw_type or "").strip().upper()
    if not raw:
        return "UNKNOWN"
    return TYPE_MAP.get(raw, "OTHER_LISTED_SECURITY")


def _safe_next_url(url: str | None) -> str | None:
    if not url:
        return None
    parts = urlsplit(url)
    query = [(k, v) for k, v in parse_qsl(parts.query, keep_blank_values=True) if k.lower() != "apikey"]
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query), parts.fragment))


@lru_cache(maxsize=1)
def _massive_client() -> JsonHttpClient:
    settings = get_settings()
    return JsonHttpClient(settings.massive_requests_per_minute)


@lru_cache(maxsize=1)
def _alpaca_client() -> JsonHttpClient:
    settings = get_settings()
    return JsonHttpClient(
        settings.alpaca_requests_per_minute,
        headers={
            "APCA-API-KEY-ID": settings.alpaca_api_key,
            "APCA-API-SECRET-KEY": settings.alpaca_api_secret,
        },
    )


def _massive_get(url: str, params: dict[str, Any] | None = None) -> Any:
    settings = get_settings()
    return _massive_client().get(url, params={**(params or {}), "apiKey": settings.massive_api_key})


def _schema_available() -> bool:
    row = fetch_one("select to_regclass(%s) is not null as ok", (RUN_TABLE,))
    return bool(row and row.get("ok"))


def _active_run() -> dict[str, Any] | None:
    return fetch_one(
        f"""
        select * from {RUN_TABLE}
         where status in ('queued','running','retry')
         order by created_at
         limit 1
        """
    )


def _claim_day(run_id: str, worker_id: str) -> dict[str, Any] | None:
    with db_connection() as conn, conn.cursor() as cur:
        cur.execute(
            f"""
            with target as (
                select run_id,observation_date
                  from {DAY_TABLE}
                 where run_id=%s
                   and (
                     status in ('queued','retry')
                     or (status='running' and coalesce(heartbeat_at,started_at) < now()-interval '20 minutes')
                   )
                 order by observation_date
                 for update skip locked
                 limit 1
            )
            update {DAY_TABLE} d
               set status='running',
                   started_at=coalesce(d.started_at,now()),
                   heartbeat_at=now(),
                   error=null
              from target t
             where d.run_id=t.run_id and d.observation_date=t.observation_date
            returning d.*
            """,
            (run_id,),
        )
        row = cur.fetchone()
        if row:
            cur.execute(
                f"""
                update {RUN_TABLE}
                   set status='running',worker_id=%s,started_at=coalesce(started_at,now()),heartbeat_at=now(),updated_at=now(),error=null
                 where run_id=%s
                """,
                (worker_id, run_id),
            )
        conn.commit()
        return row


def _scope_rows(run_id: str) -> list[dict[str, Any]]:
    return fetch_all(
        f"""
        select instrument_key,current_symbol,current_share_class_figi,current_composite_figi,
               first_seen_date,last_seen_date,currently_present,last_state_hash,last_state
          from {SCOPE_TABLE}
         where run_id=%s
        """,
        (run_id,),
    )


def _unique_index(rows: list[dict[str, Any]], key: str) -> dict[str, int]:
    values = [str(row[key]).upper() for row in rows if row.get(key)]
    counts = Counter(values)
    return {
        str(row[key]).upper(): int(row["instrument_key"])
        for row in rows
        if row.get(key) and counts[str(row[key]).upper()] == 1
    }


def _scope_maps(rows: list[dict[str, Any]]) -> tuple[dict[str, int], dict[str, int], dict[str, int]]:
    return (
        _unique_index(rows, "current_share_class_figi"),
        _unique_index(rows, "current_composite_figi"),
        _unique_index(rows, "current_symbol"),
    )


def _map_ticker_row(
    item: dict[str, Any],
    share_map: dict[str, int],
    composite_map: dict[str, int],
    symbol_map: dict[str, int],
) -> tuple[int | None, str | None, str | None, bool]:
    share = str(item.get("share_class_figi") or "").upper()
    composite = str(item.get("composite_figi") or "").upper()
    ticker = str(item.get("ticker") or "").upper()
    weak_candidates = {value for value in (composite_map.get(composite), symbol_map.get(ticker)) if value is not None}
    if share and share in share_map:
        chosen = share_map[share]
        conflict = bool(weak_candidates - {chosen})
        return chosen, "unique_share_class_figi", "STRONG", conflict
    if composite and composite in composite_map:
        chosen = composite_map[composite]
        conflict = bool({value for value in (symbol_map.get(ticker),) if value is not None} - {chosen})
        return chosen, "unique_composite_figi", "STRONG", conflict
    if ticker and ticker in symbol_map:
        return symbol_map[ticker], "exact_date_scoped_ticker", "DATE_SCOPED_SYMBOL", False
    return None, None, None, False


def _fetch_massive_ticker_snapshot(observation_date: date) -> tuple[list[dict[str, Any]], int]:
    url = "https://api.massive.com/v3/reference/tickers"
    params: dict[str, Any] | None = {
        "date": observation_date.isoformat(),
        "market": "stocks",
        "active": "true",
        "order": "asc",
        "sort": "ticker",
        "limit": 1000,
    }
    results: list[dict[str, Any]] = []
    pages = 0
    while url:
        payload = _massive_get(url, params)
        pages += 1
        if not isinstance(payload, dict):
            raise RuntimeError("Massive ticker snapshot response was not an object")
        page_rows = payload.get("results") or []
        if not isinstance(page_rows, list):
            raise RuntimeError("Massive ticker snapshot results were not a list")
        results.extend(row for row in page_rows if isinstance(row, dict))
        url = _safe_next_url(payload.get("next_url"))
        params = None
    return results, pages


def _write_reference_day(run: dict[str, Any], day_row: dict[str, Any], provider_rows: list[dict[str, Any]], pages: int) -> None:
    run_id = str(run["run_id"])
    observation_date = day_row["observation_date"]
    scope = _scope_rows(run_id)
    share_map, composite_map, symbol_map = _scope_maps(scope)
    mapped: dict[int, dict[str, Any]] = {}
    ambiguous = 0
    precedence_conflicts = 0
    for item in provider_rows:
        instrument_key, method, strength, conflict = _map_ticker_row(item, share_map, composite_map, symbol_map)
        if instrument_key is None:
            continue
        if conflict:
            precedence_conflicts += 1
        core = {
            "ticker": item.get("ticker"),
            "name": item.get("name"),
            "raw_type": item.get("type"),
            "normalized_type": _normalise_type(item.get("type")),
            "active": item.get("active"),
            "primary_exchange": item.get("primary_exchange"),
            "market": item.get("market"),
            "locale": item.get("locale"),
            "currency_name": item.get("currency_name"),
            "cik": item.get("cik"),
            "composite_figi": item.get("composite_figi"),
            "share_class_figi": item.get("share_class_figi"),
        }
        row = {
            "instrument_key": instrument_key,
            **core,
            "state_hash": _json_hash(core),
            "provider_last_updated_utc": _as_ts(item.get("last_updated_utc")),
            "mapping_method": method,
            "mapping_strength": strength,
            "payload": item,
            "mapping_conflict": conflict,
        }
        existing = mapped.get(instrument_key)
        if existing is not None:
            if existing["state_hash"] != row["state_hash"]:
                ambiguous += 1
                mapped[instrument_key] = {"ambiguous": True}
            continue
        mapped[instrument_key] = row
    mapped = {key: value for key, value in mapped.items() if not value.get("ambiguous")}
    now = datetime.now(timezone.utc)

    with db_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            create temp table tmp_equity_reference_day(
                instrument_key integer primary key,
                state_hash text not null,
                ticker text,
                name text,
                raw_type text,
                normalized_type text,
                active boolean,
                primary_exchange text,
                market text,
                locale text,
                currency_name text,
                cik text,
                composite_figi text,
                share_class_figi text,
                provider_last_updated_utc timestamptz,
                mapping_method text,
                mapping_strength text,
                payload jsonb,
                mapping_conflict boolean not null
            ) on commit drop
            """
        )
        if mapped:
            with cur.copy(
                "copy tmp_equity_reference_day(instrument_key,state_hash,ticker,name,raw_type,normalized_type,active,primary_exchange,market,locale,currency_name,cik,composite_figi,share_class_figi,provider_last_updated_utc,mapping_method,mapping_strength,payload,mapping_conflict) from stdin"
            ) as copy:
                for row in mapped.values():
                    copy.write_row((
                        row["instrument_key"],row["state_hash"],row["ticker"],row["name"],row["raw_type"],row["normalized_type"],row["active"],
                        row["primary_exchange"],row["market"],row["locale"],row["currency_name"],row["cik"],row["composite_figi"],row["share_class_figi"],
                        row["provider_last_updated_utc"],row["mapping_method"],row["mapping_strength"],Jsonb(row["payload"]),row["mapping_conflict"],
                    ))

        cur.execute(
            f"""
            insert into {EVENT_TABLE}(
                run_id,instrument_key,observation_date,event_kind,state_hash,ticker,name,raw_type,normalized_type,active,
                primary_exchange,market,locale,currency_name,cik,composite_figi,share_class_figi,provider_last_updated_utc,
                mapping_method,mapping_strength,provider_payload,source_observed_at,loaded_at,research_available_at,provenance
            )
            select %s,t.instrument_key,%s,
                   case when s.first_seen_date is null then 'STATE_INITIAL'
                        when not s.currently_present then 'REAPPEAR'
                        else 'STATE_CHANGE' end,
                   t.state_hash,t.ticker,t.name,t.raw_type,t.normalized_type,t.active,t.primary_exchange,t.market,t.locale,t.currency_name,
                   t.cik,t.composite_figi,t.share_class_figi,t.provider_last_updated_utc,t.mapping_method,t.mapping_strength,t.payload,
                   %s,%s,%s,
                   jsonb_build_object('provider_snapshot_date',%s::date,'date_scoped',true,'mapping_precedence_conflict',t.mapping_conflict)
              from tmp_equity_reference_day t
              join {SCOPE_TABLE} s on s.run_id=%s and s.instrument_key=t.instrument_key
             where s.first_seen_date is null or not s.currently_present or s.last_state_hash is distinct from t.state_hash
            on conflict (run_id,instrument_key,observation_date,event_kind) do nothing
            """,
            (run_id, observation_date, now, now, now, observation_date, run_id),
        )
        cur.execute(
            f"""
            insert into {EVENT_TABLE}(
                run_id,instrument_key,observation_date,event_kind,state_hash,ticker,name,raw_type,normalized_type,active,
                primary_exchange,market,locale,currency_name,cik,composite_figi,share_class_figi,mapping_method,mapping_strength,
                source_observed_at,loaded_at,research_available_at,provenance
            )
            select %s,s.instrument_key,%s,'ABSENT_START',null,
                   s.last_state->>'ticker',s.last_state->>'name',s.last_state->>'raw_type',s.last_state->>'normalized_type',false,
                   s.last_state->>'primary_exchange',s.last_state->>'market',s.last_state->>'locale',s.last_state->>'currency_name',
                   s.last_state->>'cik',s.last_state->>'composite_figi',s.last_state->>'share_class_figi',
                   'complete_date_snapshot_absence','NEGATIVE_MEMBERSHIP_EVIDENCE',%s,%s,%s,
                   jsonb_build_object('provider_snapshot_date',%s::date,'date_scoped',true,'absence_not_automatically_delisting',true)
              from {SCOPE_TABLE} s
             where s.run_id=%s and s.currently_present
               and not exists (select 1 from tmp_equity_reference_day t where t.instrument_key=s.instrument_key)
            on conflict (run_id,instrument_key,observation_date,event_kind) do nothing
            """,
            (run_id, observation_date, now, now, now, observation_date, run_id),
        )
        cur.execute(
            f"""
            update {SCOPE_TABLE} s
               set currently_present=false
             where s.run_id=%s and s.currently_present
               and not exists (select 1 from tmp_equity_reference_day t where t.instrument_key=s.instrument_key)
            """,
            (run_id,),
        )
        cur.execute(
            f"""
            update {SCOPE_TABLE} s
               set first_seen_date=coalesce(s.first_seen_date,%s),
                   last_seen_date=%s,
                   currently_present=true,
                   last_state_hash=t.state_hash,
                   last_state=jsonb_build_object(
                       'ticker',t.ticker,'name',t.name,'raw_type',t.raw_type,'normalized_type',t.normalized_type,'active',t.active,
                       'primary_exchange',t.primary_exchange,'market',t.market,'locale',t.locale,'currency_name',t.currency_name,
                       'cik',t.cik,'composite_figi',t.composite_figi,'share_class_figi',t.share_class_figi
                   ),
                   days_seen=s.days_seen+1,
                   historical_type_observation_days=s.historical_type_observation_days + case when t.raw_type is not null then 1 else 0 end,
                   mapping_status='HISTORICAL_REFERENCE_OBSERVED'
              from tmp_equity_reference_day t
             where s.run_id=%s and s.instrument_key=t.instrument_key
            """,
            (observation_date, observation_date, run_id),
        )
        mapped_count = len(mapped)
        provider_count = len(provider_rows)
        cur.execute(
            f"""
            update {DAY_TABLE}
               set status='completed',next_url=null,page_count=%s,provider_rows=%s,mapped_rows=%s,mapped_instruments=%s,
                   ambiguous_mapping_rows=%s,unmatched_provider_rows=%s,heartbeat_at=%s,completed_at=%s,error=null,
                   metadata=jsonb_build_object('mapping_precedence_conflicts',%s,'atomic_full_date_write',true)
             where run_id=%s and observation_date=%s
            """,
            (pages, provider_count, mapped_count, mapped_count, ambiguous, max(0, provider_count-mapped_count), now, now, precedence_conflicts, run_id, observation_date),
        )
        cur.execute(
            f"""
            update {RUN_TABLE}
               set dates_completed=(select count(*) from {DAY_TABLE} where run_id=%s and status='completed'),
                   provider_rows_seen=provider_rows_seen+%s,
                   mapped_rows_seen=mapped_rows_seen+%s,
                   ambiguous_rows_seen=ambiguous_rows_seen+%s,
                   heartbeat_at=%s,updated_at=%s,error=null
             where run_id=%s
            """,
            (run_id, provider_count, mapped_count, ambiguous, now, now, run_id),
        )
        conn.commit()


def _process_reference_day(run: dict[str, Any], worker_id: str) -> bool:
    day_row = _claim_day(str(run["run_id"]), worker_id)
    if not day_row:
        with db_connection() as conn, conn.cursor() as cur:
            cur.execute(
                f"""
                update {RUN_TABLE}
                   set phase='corporate_actions',heartbeat_at=now(),updated_at=now()
                 where run_id=%s and not exists (
                     select 1 from {DAY_TABLE} where run_id=%s and status<>'completed'
                 )
                """,
                (run["run_id"], run["run_id"]),
            )
            conn.commit()
        return True
    try:
        rows, pages = _fetch_massive_ticker_snapshot(day_row["observation_date"])
        _write_reference_day(run, day_row, rows, pages)
        logger.info("Equity reference date complete run=%s date=%s provider_rows=%s pages=%s", run["run_id"], day_row["observation_date"], len(rows), pages)
        return True
    except ProviderError as exc:
        permanent = not exc.retryable
        status = "blocked_provider_access" if permanent else "retry"
        with db_connection() as conn, conn.cursor() as cur:
            cur.execute(
                f"update {DAY_TABLE} set status=%s,error=%s,heartbeat_at=now() where run_id=%s and observation_date=%s",
                (status, f"{exc.code}: {exc}", run["run_id"], day_row["observation_date"]),
            )
            cur.execute(
                f"update {RUN_TABLE} set status=%s,error=%s,heartbeat_at=now(),updated_at=now() where run_id=%s",
                ("blocked_provider_access" if permanent else "retry", f"Massive historical reference: {exc.code}: {exc}", run["run_id"]),
            )
            conn.commit()
        logger.warning("Equity reference provider error run=%s date=%s error=%s", run["run_id"], day_row["observation_date"], exc)
        return True


def _fetch_massive_splits(window_start: date, window_end: date) -> list[dict[str, Any]]:
    url = "https://api.massive.com/stocks/v1/splits"
    params: dict[str, Any] | None = {
        "execution_date.gte": window_start.isoformat(),
        "execution_date.lte": window_end.isoformat(),
        "limit": 5000,
        "sort": "execution_date.asc",
    }
    rows: list[dict[str, Any]] = []
    while url:
        payload = _massive_get(url, params)
        if not isinstance(payload, dict):
            raise RuntimeError("Massive splits response was not an object")
        rows.extend(row for row in (payload.get("results") or []) if isinstance(row, dict))
        url = _safe_next_url(payload.get("next_url"))
        params = None
    return rows


def _fetch_alpaca_actions(window_start: date, window_end: date) -> list[tuple[str, dict[str, Any]]]:
    page_token: str | None = None
    results: list[tuple[str, dict[str, Any]]] = []
    while True:
        params: dict[str, Any] = {
            "start": window_start.isoformat(),
            "end": window_end.isoformat(),
            "region": "us",
            "data_quality": "complete",
            "limit": 1000,
            "sort": "asc",
        }
        if page_token:
            params["page_token"] = page_token
        payload = _alpaca_client().get("https://data.alpaca.markets/v1/corporate-actions", params=params)
        if not isinstance(payload, dict):
            raise RuntimeError("Alpaca corporate-actions response was not an object")
        for key, action_type in ALPACA_ACTION_KEYS.items():
            for row in payload.get(key) or []:
                if isinstance(row, dict):
                    results.append((action_type, row))
        page_token = payload.get("next_page_token")
        if not page_token:
            break
    return results


def _action_symbol(row: dict[str, Any]) -> str | None:
    for key in ("symbol", "old_symbol", "initiating_symbol", "target_symbol", "from_symbol", "source_symbol"):
        value = row.get(key)
        if value:
            return str(value).upper()
    return None


def _action_new_symbol(row: dict[str, Any]) -> str | None:
    for key in ("new_symbol", "acquirer_symbol", "to_symbol", "new_ticker"):
        value = row.get(key)
        if value:
            return str(value).upper()
    return None


def _insert_action(
    cur,
    run_id: str,
    provider: str,
    provider_action_key: str,
    action_type: str,
    row: dict[str, Any],
    *,
    source_symbol: str | None = None,
    ratio_new_per_old: float | None = None,
    split_from: float | None = None,
    split_to: float | None = None,
    execution_date: date | None = None,
) -> None:
    old_symbol = str(row.get("old_symbol") or source_symbol or "").upper() or None
    new_symbol = _action_new_symbol(row)
    fingerprint = _json_hash([provider, provider_action_key, action_type, row])
    cur.execute(
        f"""
        insert into {ACTION_TABLE}(
            run_id,provider,provider_action_key,source_symbol,old_symbol,new_symbol,action_type,announced_at,declaration_date,
            record_date,ex_date,payable_date,process_date,execution_date,ratio_new_per_old,split_from,split_to,
            provider_created_at,provider_updated_at,source_observed_at,raw_payload,provenance,row_fingerprint
        ) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,now(),%s,%s,%s)
        on conflict (run_id,provider,provider_action_key) do update set
            source_symbol=excluded.source_symbol,old_symbol=excluded.old_symbol,new_symbol=excluded.new_symbol,
            action_type=excluded.action_type,announced_at=excluded.announced_at,declaration_date=excluded.declaration_date,
            record_date=excluded.record_date,ex_date=excluded.ex_date,payable_date=excluded.payable_date,
            process_date=excluded.process_date,execution_date=excluded.execution_date,ratio_new_per_old=excluded.ratio_new_per_old,
            split_from=excluded.split_from,split_to=excluded.split_to,provider_created_at=excluded.provider_created_at,
            provider_updated_at=excluded.provider_updated_at,raw_payload=excluded.raw_payload,provenance=excluded.provenance,row_fingerprint=excluded.row_fingerprint
        """,
        (
            run_id,provider,provider_action_key,source_symbol,old_symbol,new_symbol,action_type,
            _as_ts(row.get("announced_at") or row.get("announcement_at")),_as_date(row.get("declaration_date")),
            _as_date(row.get("record_date")),_as_date(row.get("ex_date")),_as_date(row.get("payable_date")),
            _as_date(row.get("process_date")),execution_date or _as_date(row.get("execution_date")),ratio_new_per_old,
            split_from,split_to,_as_ts(row.get("created_at")),_as_ts(row.get("updated_at")),Jsonb(row),
            Jsonb({"later_loaded_reference_fact": True, "predictor_use_permitted": False, "provider": provider}),fingerprint,
        ),
    )


def _process_actions(run: dict[str, Any]) -> bool:
    run_id = str(run["run_id"])
    window_start = run["window_start"]
    window_end = run["window_end"]
    try:
        massive = _fetch_massive_splits(window_start, window_end)
        alpaca = _fetch_alpaca_actions(window_start, window_end)
        with db_connection() as conn, conn.cursor() as cur:
            for row in massive:
                split_from = _as_float(row.get("split_from"))
                split_to = _as_float(row.get("split_to"))
                ratio = split_to / split_from if split_from and split_to else None
                key = str(row.get("id") or _json_hash(row))
                _insert_action(
                    cur,run_id,"massive",key,str(row.get("adjustment_type") or "split"),row,
                    source_symbol=str(row.get("ticker") or "").upper() or None,
                    ratio_new_per_old=ratio,split_from=split_from,split_to=split_to,
                    execution_date=_as_date(row.get("execution_date")),
                )
            for action_type, row in alpaca:
                key = str(row.get("id") or _json_hash([action_type,row]))
                old_rate = _as_float(row.get("old_rate") or row.get("split_from"))
                new_rate = _as_float(row.get("new_rate") or row.get("split_to"))
                ratio = new_rate / old_rate if old_rate and new_rate else None
                _insert_action(
                    cur,run_id,"alpaca",key,action_type,row,
                    source_symbol=_action_symbol(row),ratio_new_per_old=ratio,
                    split_from=old_rate,split_to=new_rate,execution_date=_as_date(row.get("execution_date")),
                )
            cur.execute(f"select count(*) as n from {ACTION_TABLE} where run_id=%s", (run_id,))
            action_count = int(cur.fetchone()["n"])
            cur.execute(
                f"update {RUN_TABLE} set phase='finalize',action_rows_seen=%s,heartbeat_at=now(),updated_at=now(),error=null where run_id=%s",
                (action_count,run_id),
            )
            conn.commit()
        logger.info("Equity corporate-action acquisition complete run=%s massive=%s alpaca=%s",run_id,len(massive),len(alpaca))
        return True
    except ProviderError as exc:
        permanent = not exc.retryable
        with db_connection() as conn, conn.cursor() as cur:
            cur.execute(
                f"update {RUN_TABLE} set status=%s,error=%s,heartbeat_at=now(),updated_at=now() where run_id=%s",
                ("blocked_provider_access" if permanent else "retry",f"Corporate-action provider: {exc.code}: {exc}",run_id),
            )
            conn.commit()
        return True


def _finalize(run: dict[str, Any]) -> bool:
    run_id = str(run["run_id"])
    window_end_plus_one = run["window_end"].toordinal() + 1
    end_exclusive = date.fromordinal(window_end_plus_one)
    with db_connection() as conn, conn.cursor() as cur:
        cur.execute(f"delete from {STATE_TABLE} where run_id=%s", (run_id,))
        cur.execute(
            f"""
            with ordered as (
                select e.*,
                       lead(e.observation_date) over(partition by e.instrument_key order by e.observation_date,e.event_id) as next_event_date
                  from {EVENT_TABLE} e
                 where e.run_id=%s
            ), positive as (
                select o.*,
                       coalesce(
                           (select max(c.session_date) from market_governance.us_equity_fixed_session_calendar c where c.session_date < o.next_event_date),
                           s.last_seen_date,
                           o.observation_date
                       ) as last_confirmed_date
                  from ordered o
                  join {SCOPE_TABLE} s on s.run_id=o.run_id and s.instrument_key=o.instrument_key
                 where o.event_kind in ('STATE_INITIAL','STATE_CHANGE','REAPPEAR')
            )
            insert into {STATE_TABLE}(
                run_id,instrument_key,valid_from,valid_to,ticker,name,raw_type,normalized_type,active,primary_exchange,market,locale,
                currency_name,cik,composite_figi,share_class_figi,mapping_method,mapping_strength,certification_state,source_system_key,
                source_first_observation_date,source_last_observation_date,provenance,row_fingerprint
            )
            select run_id,instrument_key,observation_date,coalesce(next_event_date,%s::date),ticker,name,raw_type,normalized_type,active,
                   primary_exchange,market,locale,currency_name,cik,composite_figi,share_class_figi,mapping_method,mapping_strength,
                   case when normalized_type='UNKNOWN' then 'UNCERTIFIED_SECURITY_TYPE'
                        when mapping_strength='STRONG' then 'CERTIFIED_HISTORICAL_SECURITY_STATE_STRONG_IDENTITY'
                        else 'CERTIFIED_HISTORICAL_SECURITY_TYPE_SOURCE_SYMBOL_IDENTITY_ONLY' end,
                   source_system_key,observation_date,last_confirmed_date,
                   provenance || jsonb_build_object('provider_last_updated_utc',provider_last_updated_utc,'historical_snapshot_date',observation_date),
                   encode(digest(concat_ws('|',run_id::text,instrument_key::text,observation_date::text,coalesce(next_event_date,%s::date)::text,
                         coalesce(ticker,''),coalesce(raw_type,''),coalesce(composite_figi,''),coalesce(share_class_figi,'')),'sha256'),'hex')
              from positive
            """,
            (run_id,end_exclusive,end_exclusive),
        )
        cur.execute(f"delete from {ALIAS_TABLE} where run_id=%s", (run_id,))
        cur.execute(
            f"""
            insert into {ALIAS_TABLE}(run_id,instrument_key,symbol,valid_from,valid_to,source_system_key,mapping_method,mapping_strength,certification_state,provenance,row_fingerprint)
            select run_id,instrument_key,ticker,valid_from,valid_to,source_system_key,mapping_method,mapping_strength,certification_state,
                   provenance,
                   encode(digest(concat_ws('|',run_id::text,instrument_key::text,ticker,valid_from::text,coalesce(valid_to::text,'')),'sha256'),'hex')
              from {STATE_TABLE}
             where run_id=%s and ticker is not null
            """,
            (run_id,),
        )
        cur.execute(
            f"""
            update {ACTION_TABLE} a
               set instrument_key=s.instrument_key,
                   mapping_status='MAPPED_BY_EFFECTIVE_SYMBOL',
                   mapping_method='equity_security_state_symbol_asof'
              from lateral (
                   select st.instrument_key
                     from {STATE_TABLE} st
                    where st.run_id=a.run_id
                      and st.ticker=coalesce(a.source_symbol,a.old_symbol)
                      and coalesce(a.execution_date,a.ex_date,a.process_date) >= st.valid_from
                      and (st.valid_to is null or coalesce(a.execution_date,a.ex_date,a.process_date) < st.valid_to)
                    order by case when st.mapping_strength='STRONG' then 0 else 1 end, st.valid_from desc
                    limit 1
              ) s
             where a.run_id=%s and a.instrument_key is null
            """,
            (run_id,),
        )
        cur.execute(
            f"""
            update {ACTION_TABLE} a
               set instrument_key=sc.instrument_key,
                   mapping_status='MAPPED_BY_FIXED_SYMBOL_FALLBACK',
                   mapping_method='unique_fixed_symbol_fallback'
              from {SCOPE_TABLE} sc
             where a.run_id=%s and sc.run_id=a.run_id and a.instrument_key is null
               and sc.current_symbol=coalesce(a.source_symbol,a.old_symbol)
               and (select count(*) from {SCOPE_TABLE} x where x.run_id=a.run_id and x.current_symbol=sc.current_symbol)=1
            """,
            (run_id,),
        )
        checks = {
            "calendar_complete": f"select count(*) filter(where status='completed') n,count(*) total from {DAY_TABLE} where run_id=%s",
            "scope_seen": f"select count(*) filter(where first_seen_date is not null) n,count(*) total from {SCOPE_TABLE} where run_id=%s",
            "strong_identity_scope": f"select count(distinct instrument_key) n,(select count(*) from {SCOPE_TABLE} where run_id=%s) total from {STATE_TABLE} where run_id=%s and mapping_strength='STRONG'",
            "historical_type_scope": f"select count(*) filter(where historical_type_observation_days>0) n,count(*) total from {SCOPE_TABLE} where run_id=%s",
            "actions_mapped": f"select count(*) filter(where instrument_key is not null) n,count(*) total from {ACTION_TABLE} where run_id=%s",
        }
        for key, sql in checks.items():
            params = (run_id,run_id) if key == "strong_identity_scope" else (run_id,)
            cur.execute(sql, params)
            row = cur.fetchone()
            n = int(row["n"] or 0)
            total = int(row["total"] or 0)
            passed = n == total if key == "calendar_complete" else None
            cur.execute(
                f"""
                insert into {AUDIT_TABLE}(run_id,check_key,checked_count,mismatch_count,passed,details)
                values (%s,%s,%s,%s,%s,%s)
                on conflict (run_id,check_key) do update set checked_count=excluded.checked_count,mismatch_count=excluded.mismatch_count,
                    passed=excluded.passed,details=excluded.details,checked_at=now()
                """,
                (run_id,key,total,max(0,total-n),passed,Jsonb({"matched_or_certified":n,"total":total})),
            )
        cur.execute(f"select count(*) as n from {STATE_TABLE} where run_id=%s", (run_id,))
        states = int(cur.fetchone()["n"])
        cur.execute(f"select count(*) as n from {EVENT_TABLE} where run_id=%s", (run_id,))
        events = int(cur.fetchone()["n"])
        cur.execute(f"select count(*) as n from {ACTION_TABLE} where run_id=%s and instrument_key is not null", (run_id,))
        actions_mapped = int(cur.fetchone()["n"])
        cur.execute(
            f"""
            update {RUN_TABLE}
               set status='completed_staged_pending_canonical_merge',phase='complete',state_event_rows=%s,security_state_rows=%s,
                   actions_mapped=%s,heartbeat_at=now(),completed_at=now(),updated_at=now(),error=null,
                   metadata=metadata || jsonb_build_object('daily_reference_scan_complete',true,'canonical_merge_pending_independent_verification',true)
             where run_id=%s
            """,
            (events,states,actions_mapped,run_id),
        )
        conn.commit()
    logger.info("Equity reference backfill staged run=%s states=%s events=%s mapped_actions=%s",run_id,states,events,actions_mapped)
    return True


def process_equity_reference_backfill_once(worker_id: str) -> bool:
    if not _schema_available():
        return False
    run = _active_run()
    if not run:
        return False
    phase = str(run.get("phase") or "reference_snapshots")
    if phase == "reference_snapshots":
        return _process_reference_day(run, worker_id)
    if phase == "corporate_actions":
        return _process_actions(run)
    if phase == "finalize":
        return _finalize(run)
    return False
