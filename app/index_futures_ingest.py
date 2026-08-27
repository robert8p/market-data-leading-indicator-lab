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
from uuid import UUID

from app.db import db_connection, fetch_one

logger = logging.getLogger(__name__)

_BASE = "https://api.massive.com"
_ROOTS = ("ES", "MES", "NQ", "MNQ", "YM", "MYM", "RTY", "M2K")
_QUARTER_MONTH_CODES = "HMUZ"
_DEFAULT_START = date(2024, 8, 27)
_DEFAULT_END_EXCLUSIVE = date(2026, 8, 27)
_thread_lock = threading.Lock()
_started = False


def _enabled() -> bool:
    return os.getenv("INDEX_FUTURES_INGEST_ENABLED", "false").strip().lower() in {"1", "true", "yes", "on"}


def _request_interval() -> float:
    # Futures Basic is 5 requests/minute. Stay below the documented ceiling.
    try:
        return max(12.25, float(os.getenv("INDEX_FUTURES_REQUEST_INTERVAL_SECONDS", "12.25")))
    except ValueError:
        return 12.25


class MassiveFuturesClient:
    def __init__(self, api_key: str, run_id: UUID):
        self.api_key = api_key
        self.run_id = run_id
        self.min_interval = _request_interval()
        self.last_request_monotonic = 0.0

    def _throttle(self) -> None:
        elapsed = time.monotonic() - self.last_request_monotonic
        if self.last_request_monotonic and elapsed < self.min_interval:
            time.sleep(self.min_interval - elapsed)

    def _record_request(self) -> None:
        self.last_request_monotonic = time.monotonic()
        with db_connection() as conn, conn.cursor() as cur:
            cur.execute(
                "update index_futures_v1.ingestion_runs set requests_made=requests_made+1,updated_at=now() where id=%s",
                (self.run_id,),
            )
            conn.commit()

    def get(self, path_or_url: str, params: dict[str, object] | None = None) -> dict:
        retries = 0
        while True:
            self._throttle()
            if path_or_url.startswith("http"):
                url = path_or_url
                if params:
                    url += ("&" if "?" in url else "?") + urlencode(params)
            else:
                query = urlencode({k: v for k, v in (params or {}).items() if v is not None})
                url = f"{_BASE}{path_or_url}" + (f"?{query}" if query else "")
            request = Request(
                url,
                headers={
                    "Authorization": f"Bearer {self.api_key}",
                    "Accept": "application/json",
                    "User-Agent": "market-data-leading-indicator-lab/index-futures-v1",
                },
                method="GET",
            )
            try:
                with urlopen(request, timeout=60) as response:
                    raw = response.read()
                    self._record_request()
                    payload = json.loads(raw.decode("utf-8", errors="replace"))
                    if not isinstance(payload, dict):
                        raise RuntimeError("Massive response was not a JSON object")
                    return payload
            except HTTPError as exc:
                self._record_request()
                raw = exc.read()
                try:
                    payload = json.loads(raw.decode("utf-8", errors="replace"))
                except Exception:
                    payload = {}
                if exc.code == 429 and retries < 12:
                    retries += 1
                    retry_after = exc.headers.get("Retry-After") if exc.headers else None
                    try:
                        sleep_for = max(13.0, float(retry_after)) if retry_after else 65.0
                    except ValueError:
                        sleep_for = 65.0
                    logger.warning("INDEX_FUTURES_INGEST provider rate limit; retry=%s", retries)
                    time.sleep(sleep_for)
                    continue
                message = str(payload.get("message") or payload.get("error") or "")[:300]
                raise RuntimeError(f"Massive HTTP {exc.code}: {message}") from exc
            except URLError as exc:
                if retries < 5:
                    retries += 1
                    time.sleep(min(60.0, 5.0 * retries))
                    continue
                raise RuntimeError(f"Massive network error: {type(exc).__name__}") from exc

    def pages(self, path: str, params: dict[str, object]) -> list[dict]:
        next_url: str | None = None
        first = True
        while first or next_url:
            first = False
            payload = self.get(next_url or path, params if not next_url else None)
            rows = payload.get("results")
            if isinstance(rows, list):
                yield [row for row in rows if isinstance(row, dict)]
            else:
                yield []
            value = payload.get("next_url")
            next_url = str(value) if value else None


def _parse_date(value: object) -> date | None:
    if not value:
        return None
    try:
        return date.fromisoformat(str(value)[:10])
    except ValueError:
        return None


def _ns_to_dt(value: object) -> datetime:
    return datetime.fromtimestamp(int(value) / 1_000_000_000, tz=timezone.utc)


def _ensure_run(start: date, end_exclusive: date) -> UUID:
    existing = fetch_one(
        """
        select id from index_futures_v1.ingestion_runs
         where provider='massive'
           and coverage_start=%s::date::timestamptz
           and coverage_end_exclusive=%s::date::timestamptz
           and roots=%s::text[]
           and status in ('queued','running')
         order by created_at desc limit 1
        """,
        (start.isoformat(), end_exclusive.isoformat(), list(_ROOTS)),
    )
    if existing:
        run_id = existing["id"]
        with db_connection() as conn, conn.cursor() as cur:
            cur.execute(
                "update index_futures_v1.ingestion_runs set status='running',started_at=coalesce(started_at,now()),updated_at=now() where id=%s",
                (run_id,),
            )
            conn.commit()
        return run_id

    with db_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            insert into index_futures_v1.ingestion_runs(
                provider,status,coverage_start,coverage_end_exclusive,roots,config,started_at
            ) values ('massive','running',%s::date::timestamptz,%s::date::timestamptz,%s::text[],%s::jsonb,now())
            returning id
            """,
            (
                start.isoformat(),
                end_exclusive.isoformat(),
                list(_ROOTS),
                json.dumps({
                    "version": "index_futures_ingest_v1",
                    "contract_window_days_before_expiry": 98,
                    "minute_resolution": "1min",
                    "minute_page_limit": 50000,
                    "source": "Massive Futures REST",
                    "point_in_time_policy": "contract-identified raw bars; no synthetic continuous input",
                }),
            ),
        )
        run_id = cur.fetchone()["id"]
        conn.commit()
        return run_id


def _checkpoint(run_id: UUID, **values: object) -> None:
    payload = {k: v for k, v in values.items() if v is not None}
    with db_connection() as conn, conn.cursor() as cur:
        cur.execute(
            "update index_futures_v1.ingestion_runs set last_checkpoint=%s::jsonb,updated_at=now() where id=%s",
            (json.dumps(payload, default=str), run_id),
        )
        conn.commit()


def _root_spec(root: str) -> dict:
    row = fetch_one("select * from index_futures_v1.root_specs where root=%s", (root,))
    if not row:
        raise RuntimeError(f"Missing root spec for {root}")
    return row


def _upsert_product_snapshot(client: MassiveFuturesClient, root: str, asof: date) -> None:
    payload = client.get(
        "/futures/v1/products",
        {"product_code": root, "date": asof.isoformat(), "limit": 10},
    )
    rows = [r for r in payload.get("results", []) if isinstance(r, dict) and r.get("product_code") == root]
    for row in rows[:1]:
        with db_connection() as conn, conn.cursor() as cur:
            cur.execute(
                """
                insert into index_futures_v1.product_metadata_history(
                    provider,product_code,asof_date,root,name,trading_venue,asset_class,asset_sub_class,
                    sector,sub_sector,settlement_currency_code,trade_currency_code,settlement_method,
                    settlement_type,price_quotation,unit_of_measure,unit_of_measure_qty,contract_type,
                    raw_provider_payload,observability_class
                ) values (
                    'massive',%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,
                    'retrospective_reconstructed_pit_reproducible'
                )
                on conflict(provider,product_code,asof_date) do update set
                    name=excluded.name,trading_venue=excluded.trading_venue,asset_class=excluded.asset_class,
                    asset_sub_class=excluded.asset_sub_class,sector=excluded.sector,sub_sector=excluded.sub_sector,
                    settlement_currency_code=excluded.settlement_currency_code,trade_currency_code=excluded.trade_currency_code,
                    settlement_method=excluded.settlement_method,settlement_type=excluded.settlement_type,
                    price_quotation=excluded.price_quotation,unit_of_measure=excluded.unit_of_measure,
                    unit_of_measure_qty=excluded.unit_of_measure_qty,contract_type=excluded.contract_type,
                    raw_provider_payload=excluded.raw_provider_payload,ingested_at=now()
                """,
                (
                    root, asof, root, row.get("name"), row.get("trading_venue"), row.get("asset_class"),
                    row.get("asset_sub_class"), row.get("sector"), row.get("sub_sector"),
                    row.get("settlement_currency_code"), row.get("trade_currency_code"),
                    row.get("settlement_method"), row.get("settlement_type"), row.get("price_quotation"),
                    row.get("unit_of_measure"), row.get("unit_of_measure_qty"), row.get("type"), json.dumps(row),
                ),
            )
            conn.commit()


def _discover_contracts(client: MassiveFuturesClient, root: str, start: date, end_exclusive: date) -> list[dict]:
    spec = _root_spec(root)
    upper_expiry = end_exclusive + timedelta(days=130)
    payload = client.get(
        "/futures/v1/contracts",
        {
            "product_code": root,
            "type": "single",
            "last_trade_date.gte": start.isoformat(),
            "last_trade_date.lte": upper_expiry.isoformat(),
            "first_trade_date.lt": end_exclusive.isoformat(),
            "limit": 1000,
            "sort": "last_trade_date.asc",
        },
    )
    pattern = re.compile(rf"^{re.escape(root)}[{_QUARTER_MONTH_CODES}][0-9]{{1,2}}$")
    accepted: list[dict] = []
    for row in payload.get("results", []):
        if not isinstance(row, dict):
            continue
        ticker = str(row.get("ticker") or "")
        if row.get("product_code") != root or not pattern.match(ticker):
            continue
        if row.get("trading_venue") != spec["exchange_mic"]:
            logger.warning("INDEX_FUTURES_INGEST identity exclusion root=%s ticker=%s venue=%s", root, ticker, row.get("trading_venue"))
            continue
        last_trade = _parse_date(row.get("last_trade_date"))
        if not last_trade or last_trade < start or last_trade > upper_expiry:
            continue
        accepted.append(row)

    for row in accepted:
        ticker = str(row["ticker"])
        first_trade = _parse_date(row.get("first_trade_date"))
        last_trade = _parse_date(row.get("last_trade_date"))
        settlement = _parse_date(row.get("settlement_date"))
        expiry_basis = settlement or last_trade
        with db_connection() as conn, conn.cursor() as cur:
            cur.execute(
                """
                insert into index_futures_v1.contracts(
                    provider,provider_contract_identifier,root,full_contract_symbol,product_code,underlying_index,
                    contract_class,asset_class,exchange_mic,currency,expiry_month,expiry_year,first_trade_date,
                    last_trade_date,settlement_date,contract_multiplier,tick_size,tick_value,contract_type,last_observed_at
                ) values ('massive',%s,%s,%s,%s,%s,%s,'futures',%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,now())
                on conflict(provider,provider_contract_identifier) do update set
                    last_observed_at=now(),first_trade_date=excluded.first_trade_date,last_trade_date=excluded.last_trade_date,
                    settlement_date=excluded.settlement_date,contract_type=excluded.contract_type
                returning contract_id
                """,
                (
                    ticker, root, ticker, root, spec["underlying_index"], spec["contract_class"], spec["exchange_mic"],
                    spec["currency"], expiry_basis.month if expiry_basis else None, expiry_basis.year if expiry_basis else None,
                    first_trade, last_trade, settlement, spec["contract_multiplier"], spec["tick_size"], spec["tick_value"], row.get("type"),
                ),
            )
            contract_id = cur.fetchone()["contract_id"]
            asof = _parse_date(row.get("date")) or (end_exclusive - timedelta(days=1))
            cur.execute(
                """
                insert into index_futures_v1.contract_metadata_history(
                    provider,provider_contract_identifier,asof_date,contract_id,active,days_to_maturity,name,product_code,
                    exchange_mic,first_trade_date,last_trade_date,settlement_date,trade_tick_size,settlement_tick_size,
                    spread_tick_size,contract_type,raw_provider_payload,observability_class
                ) values ('massive',%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,
                          'retrospective_reconstructed_pit_reproducible')
                on conflict(provider,provider_contract_identifier,asof_date) do update set
                    active=excluded.active,days_to_maturity=excluded.days_to_maturity,name=excluded.name,
                    exchange_mic=excluded.exchange_mic,first_trade_date=excluded.first_trade_date,
                    last_trade_date=excluded.last_trade_date,settlement_date=excluded.settlement_date,
                    trade_tick_size=excluded.trade_tick_size,settlement_tick_size=excluded.settlement_tick_size,
                    spread_tick_size=excluded.spread_tick_size,contract_type=excluded.contract_type,
                    raw_provider_payload=excluded.raw_provider_payload,ingested_at=now()
                """,
                (
                    ticker, asof, contract_id, row.get("active"), row.get("days_to_maturity"), row.get("name"), root,
                    spec["exchange_mic"], first_trade, last_trade, settlement, row.get("trade_tick_size"),
                    row.get("settlement_tick_size"), row.get("spread_tick_size"), row.get("type"), json.dumps(row),
                ),
            )
            conn.commit()
        row = dict(row)
        row["contract_id"] = contract_id
    return accepted


def _contract_rows(root: str) -> list[dict]:
    start = _DEFAULT_START
    upper = _DEFAULT_END_EXCLUSIVE + timedelta(days=130)
    with db_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            select contract_id,root,full_contract_symbol,first_trade_date,last_trade_date,settlement_date
              from index_futures_v1.contracts
             where provider='massive' and root=%s and last_trade_date between %s and %s
             order by last_trade_date,full_contract_symbol
            """,
            (root, start, upper),
        )
        rows = list(cur.fetchall())
        conn.commit()
        return rows


def _insert_bar_page(contract: dict, rows: list[dict]) -> int:
    prepared = []
    for row in rows:
        ts_ns = row.get("window_start")
        if ts_ns is None:
            continue
        try:
            ts = _ns_to_dt(ts_ns)
            o = float(row["open"]); h = float(row["high"]); l = float(row["low"]); c = float(row["close"])
        except (KeyError, TypeError, ValueError, OverflowError):
            continue
        if ts < datetime.combine(_DEFAULT_START, datetime.min.time(), tzinfo=timezone.utc) or ts >= datetime.combine(_DEFAULT_END_EXCLUSIVE, datetime.min.time(), tzinfo=timezone.utc):
            continue
        volume = row.get("volume")
        transactions = row.get("transactions")
        dollar_volume = row.get("dollar_volume")
        try:
            vwap = float(dollar_volume) / int(volume) if dollar_volume is not None and volume not in (None, 0) else None
        except (TypeError, ValueError, ZeroDivisionError):
            vwap = None
        prepared.append((
            contract["contract_id"], contract["root"], contract["full_contract_symbol"], ts, int(ts_ns),
            _parse_date(row.get("session_end_date")), o, h, l, c,
            int(volume) if volume is not None else None,
            int(transactions) if transactions is not None else None,
            float(dollar_volume) if dollar_volume is not None else None,
            vwap,
            float(row["settlement_price"]) if row.get("settlement_price") is not None else None,
        ))
    if not prepared:
        return 0

    with db_connection() as conn, conn.cursor() as cur:
        cur.execute("""
            create temporary table if not exists _if_bar_stage (
                contract_id uuid,root text,full_contract_symbol text,ts_utc timestamptz,source_timestamp_ns bigint,
                session_end_date date,open double precision,high double precision,low double precision,close double precision,
                volume bigint,trade_count bigint,dollar_volume double precision,vwap double precision,settlement_price double precision
            ) on commit delete rows
        """)
        with cur.copy("copy _if_bar_stage(contract_id,root,full_contract_symbol,ts_utc,source_timestamp_ns,session_end_date,open,high,low,close,volume,trade_count,dollar_volume,vwap,settlement_price) from stdin") as copy:
            for item in prepared:
                copy.write_row(item)
        cur.execute("""
            insert into index_futures_v1.bars_1m(
                contract_id,root,full_contract_symbol,ts_utc,source_timestamp_ns,session_end_date,open,high,low,close,
                volume,trade_count,dollar_volume,vwap,settlement_price
            )
            select contract_id,root,full_contract_symbol,ts_utc,source_timestamp_ns,session_end_date,open,high,low,close,
                   volume,trade_count,dollar_volume,vwap,settlement_price
              from _if_bar_stage
            on conflict(contract_id,ts_utc) do update set
                session_end_date=excluded.session_end_date,open=excluded.open,high=excluded.high,low=excluded.low,
                close=excluded.close,volume=excluded.volume,trade_count=excluded.trade_count,
                dollar_volume=excluded.dollar_volume,vwap=excluded.vwap,settlement_price=excluded.settlement_price,
                ingested_at=now()
        """)
        affected = cur.rowcount
        conn.commit()
        return max(0, int(affected or 0))


def _ingest_contract_bars(client: MassiveFuturesClient, run_id: UUID, contract: dict) -> int:
    last_trade: date = contract["last_trade_date"]
    window_start = max(_DEFAULT_START, last_trade - timedelta(days=98))
    window_end = min(_DEFAULT_END_EXCLUSIVE, last_trade + timedelta(days=1))
    if window_start >= window_end:
        return 0

    existing = fetch_one(
        "select max(source_timestamp_ns) as max_ns from index_futures_v1.bars_1m where contract_id=%s",
        (contract["contract_id"],),
    )
    params: dict[str, object] = {
        "resolution": "1min",
        "window_start.lt": window_end.isoformat(),
        "limit": 50000,
        "sort": "window_start.asc",
    }
    max_ns = existing.get("max_ns") if existing else None
    if max_ns:
        params["window_start.gt"] = int(max_ns)
    else:
        params["window_start.gte"] = window_start.isoformat()

    total = 0
    for page in client.pages(f"/futures/v1/aggs/{contract['full_contract_symbol']}", params):
        written = _insert_bar_page(contract, page)
        total += written
        with db_connection() as conn, conn.cursor() as cur:
            cur.execute(
                "update index_futures_v1.ingestion_runs set bars_written=bars_written+%s,updated_at=now() where id=%s",
                (written, run_id),
            )
            conn.commit()
        _checkpoint(run_id, stage="bars_1m", root=contract["root"], contract=contract["full_contract_symbol"], rows=total)
        logger.info("INDEX_FUTURES_INGEST stage=bars root=%s ticker=%s page_rows=%s total=%s", contract["root"], contract["full_contract_symbol"], len(page), total)
    return total


def _ingest_session_aggregates(client: MassiveFuturesClient, run_id: UUID, contract: dict) -> int:
    last_trade: date = contract["last_trade_date"]
    window_start = max(_DEFAULT_START, last_trade - timedelta(days=98))
    window_end = min(_DEFAULT_END_EXCLUSIVE, last_trade + timedelta(days=1))
    if window_start >= window_end:
        return 0
    existing = fetch_one(
        "select count(*) as n from index_futures_v1.session_aggregates where contract_id=%s and session_end_date >= %s and session_end_date < %s",
        (contract["contract_id"], window_start, window_end),
    )
    if existing and int(existing["n"] or 0) >= 40:
        return 0

    total = 0
    params = {
        "resolution": "1session",
        "window_start.gte": (window_start - timedelta(days=1)).isoformat(),
        "window_start.lt": window_end.isoformat(),
        "limit": 1000,
        "sort": "window_start.asc",
    }
    for page in client.pages(f"/futures/v1/aggs/{contract['full_contract_symbol']}", params):
        prepared = []
        for row in page:
            session_date = _parse_date(row.get("session_end_date"))
            if not session_date or session_date < _DEFAULT_START or session_date >= _DEFAULT_END_EXCLUSIVE:
                continue
            ts_ns = row.get("window_start")
            volume = row.get("volume")
            dollar_volume = row.get("dollar_volume")
            try:
                vwap = float(dollar_volume) / int(volume) if dollar_volume is not None and volume not in (None, 0) else None
            except (TypeError, ValueError, ZeroDivisionError):
                vwap = None
            prepared.append((
                contract["contract_id"], contract["root"], contract["full_contract_symbol"], session_date,
                _ns_to_dt(ts_ns) if ts_ns is not None else None, int(ts_ns) if ts_ns is not None else None,
                row.get("open"), row.get("high"), row.get("low"), row.get("close"),
                int(volume) if volume is not None else None,
                int(row["transactions"]) if row.get("transactions") is not None else None,
                float(dollar_volume) if dollar_volume is not None else None,
                vwap,
                float(row["settlement_price"]) if row.get("settlement_price") is not None else None,
            ))
        if prepared:
            with db_connection() as conn, conn.cursor() as cur:
                cur.executemany("""
                    insert into index_futures_v1.session_aggregates(
                        contract_id,root,full_contract_symbol,session_end_date,session_start_ts_utc,source_timestamp_ns,
                        open,high,low,close,volume,trade_count,dollar_volume,vwap,settlement_price
                    ) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    on conflict(contract_id,session_end_date) do update set
                        session_start_ts_utc=excluded.session_start_ts_utc,source_timestamp_ns=excluded.source_timestamp_ns,
                        open=excluded.open,high=excluded.high,low=excluded.low,close=excluded.close,volume=excluded.volume,
                        trade_count=excluded.trade_count,dollar_volume=excluded.dollar_volume,vwap=excluded.vwap,
                        settlement_price=excluded.settlement_price,ingested_at=now()
                """, prepared)
                cur.execute(
                    "update index_futures_v1.ingestion_runs set session_rows_written=session_rows_written+%s,updated_at=now() where id=%s",
                    (len(prepared), run_id),
                )
                conn.commit()
            total += len(prepared)
    return total


def _ingest_schedules(client: MassiveFuturesClient, run_id: UUID, root: str) -> int:
    total = 0
    params = {
        "product_code": root,
        "session_end_date.gte": _DEFAULT_START.isoformat(),
        "session_end_date.lt": _DEFAULT_END_EXCLUSIVE.isoformat(),
        "limit": 1000,
        "sort": "session_end_date.asc",
    }
    for page in client.pages("/futures/v1/schedules", params):
        prepared = []
        spec = _root_spec(root)
        for row in page:
            if row.get("product_code") != root or row.get("trading_venue") != spec["exchange_mic"]:
                continue
            session_date = _parse_date(row.get("session_end_date"))
            try:
                event_ts = datetime.fromisoformat(str(row.get("timestamp")).replace("Z", "+00:00"))
            except (TypeError, ValueError):
                continue
            if not session_date:
                continue
            prepared.append((root, root, spec["exchange_mic"], session_date, str(row.get("event") or "unknown"), event_ts))
        if prepared:
            with db_connection() as conn, conn.cursor() as cur:
                cur.executemany("""
                    insert into index_futures_v1.session_calendar(root,product_code,exchange_mic,session_end_date,event,event_ts_utc)
                    values (%s,%s,%s,%s,%s,%s)
                    on conflict(root,session_end_date,event,event_ts_utc) do nothing
                """, prepared)
                cur.execute(
                    "update index_futures_v1.ingestion_runs set schedule_rows_written=schedule_rows_written+%s,updated_at=now() where id=%s",
                    (len(prepared), run_id),
                )
                conn.commit()
            total += len(prepared)
    return total


def run_index_futures_ingestion() -> None:
    api_key = os.getenv("MASSIVE_API_KEY", "").strip()
    if not api_key:
        logger.error("INDEX_FUTURES_INGEST cannot start: MASSIVE_API_KEY absent")
        return
    start = _DEFAULT_START
    end_exclusive = _DEFAULT_END_EXCLUSIVE
    run_id = _ensure_run(start, end_exclusive)
    client = MassiveFuturesClient(api_key, run_id)
    logger.warning("INDEX_FUTURES_INGEST started run_id=%s roots=%s", run_id, ",".join(_ROOTS))
    try:
        contract_total = 0
        for root in _ROOTS:
            _checkpoint(run_id, stage="reference", root=root)
            _upsert_product_snapshot(client, root, end_exclusive - timedelta(days=1))
            contracts = _discover_contracts(client, root, start, end_exclusive)
            contract_total += len(contracts)
            _ingest_schedules(client, run_id, root)
            with db_connection() as conn, conn.cursor() as cur:
                cur.execute("update index_futures_v1.ingestion_runs set contracts_discovered=%s,updated_at=now() where id=%s", (contract_total, run_id))
                conn.commit()

            for contract in _contract_rows(root):
                _ingest_contract_bars(client, run_id, contract)
                _ingest_session_aggregates(client, run_id, contract)

        with db_connection() as conn, conn.cursor() as cur:
            cur.execute("""
                insert into index_futures_v1.source_manifest(
                    provider,source_family,endpoint_or_family,access_mechanism,access_verified_at,
                    requested_start_date,requested_end_date,available_start_date,available_end_date,frequency,
                    fields_available,known_limitations,transformation_lineage,contract_mapping_version,
                    roll_methodology_version,adjustment_methodology
                ) values (
                    'massive','Massive Futures REST','/futures/v1/contracts + /products + /schedules + /aggs/{ticker}',
                    'Existing authorised Render MASSIVE_API_KEY; bearer authentication; credential never persisted in database',now(),
                    %s,%s,%s,%s,'1 minute',%s::jsonb,%s::jsonb,
                    'index_futures_ingest_v1','contract_id_provider_ticker_v1','pending_post_ingest','pending_post_ingest'
                )
            """, (
                start,end_exclusive-timedelta(days=1),start,end_exclusive-timedelta(days=1),
                json.dumps({"bars":["timestamp","open","high","low","close","volume","transactions","session_end_date"],"session":["settlement_price","dollar_volume"],"reference":["contract ticker","product_code","venue","first/last trade dates","settlement date","tick sizes"],"schedule":["open/close/break/holiday events in UTC"]}),
                json.dumps({"history":"Existing Futures Basic access is limited to 2 years","rate_limit":"5 API calls/minute","open_interest":"Not exposed by current Massive Futures REST endpoints","quotes_trades":"Not included on Futures Basic","vx":"CFE/VIX futures not covered; Massive Futures covers CME/CBOT/NYMEX/COMEX","settlement_observability":"Publication timestamp not supplied; settlement is unsafe intraday until separately established"}),
            ))
            cur.execute("update index_futures_v1.ingestion_runs set status='completed_with_limitations',completed_at=now(),updated_at=now(),last_checkpoint=%s::jsonb where id=%s", (json.dumps({"stage":"raw_complete"}),run_id))
            conn.commit()
        logger.warning("INDEX_FUTURES_INGEST completed run_id=%s", run_id)
    except Exception as exc:
        logger.exception("INDEX_FUTURES_INGEST failed run_id=%s", run_id)
        with db_connection() as conn, conn.cursor() as cur:
            cur.execute(
                "update index_futures_v1.ingestion_runs set status='failed',error=%s,updated_at=now() where id=%s",
                (f"{type(exc).__name__}: {str(exc)[:500]}", run_id),
            )
            conn.commit()


def start_index_futures_ingestion_if_enabled() -> None:
    global _started
    if not _enabled():
        return
    with _thread_lock:
        if _started:
            return
        _started = True
    thread = threading.Thread(target=run_index_futures_ingestion, name="index-futures-ingest", daemon=True)
    thread.start()
