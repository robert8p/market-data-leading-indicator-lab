from __future__ import annotations

import csv
import io
import logging
import re
from functools import lru_cache
from html.parser import HTMLParser
from datetime import date, datetime, timedelta, timezone
from typing import Any
from urllib.parse import quote

from psycopg.types.json import Jsonb

from app.config import get_settings
from app.db import db_connection, fetch_all, fetch_one
from app.exceptions import EmptyData, ProviderError
from app.http import JsonHttpClient
from app.providers.base import as_float, as_utc

logger = logging.getLogger(__name__)


def _result_rows(payload: Any) -> list[dict[str, Any]]:
    if not isinstance(payload, dict):
        return []
    result = payload.get("results")
    if isinstance(result, list):
        return [row for row in result if isinstance(row, dict)]
    if isinstance(result, dict):
        return [result]
    return [payload] if any(key in payload for key in ("ticker", "cik", "free_float")) else []


def _as_date(value: Any, fallback: date) -> date:
    if not value:
        return fallback
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).date()
    except (TypeError, ValueError):
        return fallback


def _first_result(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return {}
    result = payload.get("results")
    if isinstance(result, list):
        return result[0] if result else {}
    if isinstance(result, dict):
        return result
    return payload if any(key in payload for key in ("ticker", "cik", "free_float")) else {}


@lru_cache(maxsize=1)
def _massive_client() -> JsonHttpClient:
    settings = get_settings()
    return JsonHttpClient(settings.massive_requests_per_minute)


def _massive_get(client: JsonHttpClient, path: str, params: dict[str, Any]) -> Any:
    settings = get_settings()
    return client.get(f"https://api.massive.com{path}", params={**params, "apiKey": settings.massive_api_key})


def _ticker_overview(symbol: str, client: JsonHttpClient | None = None) -> dict[str, Any]:
    client = client or _massive_client()
    payload = _massive_get(client, f"/v3/reference/tickers/{quote(symbol, safe='')}", {})
    return _first_result(payload)


def collect_massive_context(partition: dict[str, Any]) -> int:
    client = _massive_client()
    symbol = partition["provider_symbol"]
    overview_payload = _massive_get(client, f"/v3/reference/tickers/{quote(symbol, safe='')}", {})
    overview = _first_result(overview_payload)
    float_payload = _massive_get(client, "/stocks/vX/float", {"ticker": symbol, "limit": 10})
    float_row = _first_result(float_payload)
    short_payload = _massive_get(
        client,
        "/stocks/v1/short-interest",
        {
            "ticker": symbol,
            "limit": 30,
            "sort": "settlement_date.desc",
            "settlement_date.lte": partition["end_ts"].date().isoformat(),
        },
    )
    short_rows = _result_rows(short_payload)
    fallback_date = partition["end_ts"].date()
    cik = str(overview.get("cik") or "") or None
    snapshots: list[dict[str, Any]] = [
        {
            "source": "massive_reference",
            "asof_date": _as_date(overview.get("last_updated_utc"), fallback_date),
            "cik": cik,
            "market_cap": as_float(overview.get("market_cap")),
            "free_float": None,
            "free_float_percent": None,
            "share_class_shares_outstanding": as_float(overview.get("share_class_shares_outstanding")),
            "weighted_shares_outstanding": as_float(overview.get("weighted_shares_outstanding")),
            "short_interest": None,
            "avg_daily_volume": None,
            "days_to_cover": None,
            "metadata": {"point_in_time_warning": "Reference fields may be current rather than historical.", "payload": overview_payload},
        }
    ]
    if float_row:
        snapshots.append({
            "source": "massive_float",
            "asof_date": _as_date(float_row.get("effective_date"), fallback_date),
            "cik": cik,
            "market_cap": None,
            "free_float": as_float(float_row.get("free_float")),
            "free_float_percent": as_float(float_row.get("free_float_percent")),
            "share_class_shares_outstanding": None,
            "weighted_shares_outstanding": None,
            "short_interest": None,
            "avg_daily_volume": None,
            "days_to_cover": None,
            "metadata": {"point_in_time_warning": "Use only when effective_date is not after the event.", "payload": float_payload},
        })
    for short_row in short_rows:
        snapshots.append({
            "source": "massive_short_interest",
            "asof_date": _as_date(short_row.get("settlement_date"), fallback_date),
            "cik": cik,
            "market_cap": None,
            "free_float": None,
            "free_float_percent": None,
            "share_class_shares_outstanding": None,
            "weighted_shares_outstanding": None,
            "short_interest": as_float(short_row.get("short_interest")),
            "avg_daily_volume": as_float(short_row.get("avg_daily_volume")),
            "days_to_cover": as_float(short_row.get("days_to_cover")),
            "metadata": {"payload": short_row},
        })

    with db_connection() as conn, conn.cursor() as cur:
        for snapshot in snapshots:
            cur.execute(
                """
                insert into equity_context_snapshots(
                    instrument_id,source,asof_date,ticker,cik,market_cap,free_float,free_float_percent,
                    share_class_shares_outstanding,weighted_shares_outstanding,short_interest,
                    avg_daily_volume,days_to_cover,metadata,updated_at
                ) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,now())
                on conflict(instrument_id,source,asof_date) do update set
                    cik=excluded.cik,market_cap=excluded.market_cap,free_float=excluded.free_float,
                    free_float_percent=excluded.free_float_percent,
                    share_class_shares_outstanding=excluded.share_class_shares_outstanding,
                    weighted_shares_outstanding=excluded.weighted_shares_outstanding,
                    short_interest=excluded.short_interest,avg_daily_volume=excluded.avg_daily_volume,
                    days_to_cover=excluded.days_to_cover,metadata=excluded.metadata,updated_at=now()
                """,
                (
                    partition["instrument_id"],snapshot["source"],snapshot["asof_date"],symbol,snapshot["cik"],
                    snapshot["market_cap"],snapshot["free_float"],snapshot["free_float_percent"],
                    snapshot["share_class_shares_outstanding"],snapshot["weighted_shares_outstanding"],
                    snapshot["short_interest"],snapshot["avg_daily_volume"],snapshot["days_to_cover"],
                    Jsonb(snapshot["metadata"]),
                ),
            )
        cur.execute(
            "update collection_partitions set row_count=%s,cursor=%s,heartbeat_at=now(),updated_at=now() where id=%s",
            (len(snapshots), Jsonb({"finished": True, "snapshot_count": len(snapshots)}), partition["id"]),
        )
        conn.commit()
    return len(snapshots)


class _SECTextExtractor(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.parts: list[str] = []
        self._ignored = 0

    def handle_starttag(self, tag: str, attrs) -> None:
        if tag.lower() in {"script", "style"}:
            self._ignored += 1

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() in {"script", "style"} and self._ignored:
            self._ignored -= 1

    def handle_data(self, data: str) -> None:
        if not self._ignored:
            self.parts.append(data)


DILUTION_PHRASES = [
    "at-the-market", "at the market offering", "sales agreement", "shelf registration",
    "prospectus supplement", "common stock offering", "registered direct offering",
    "private placement", "warrant", "convertible note", "convertible preferred",
    "shares of common stock", "resale prospectus", "equity line", "purchase agreement",
]


def _sec_plain_text(raw: str) -> str:
    parser = _SECTextExtractor()
    try:
        parser.feed(raw)
        text = " ".join(parser.parts)
    except Exception:
        text = re.sub(r"<[^>]+>", " ", raw)
    return re.sub(r"\s+", " ", text).strip()


def _dilution_signals(text: str) -> dict[str, Any]:
    lowered = text.lower()
    found: list[str] = []
    snippets: list[str] = []
    for phrase in DILUTION_PHRASES:
        start = 0
        while len(snippets) < 8:
            index = lowered.find(phrase, start)
            if index < 0:
                break
            found.append(phrase)
            left = max(0, index - 180)
            right = min(len(text), index + len(phrase) + 260)
            snippet = text[left:right].strip()
            if snippet and snippet not in snippets:
                snippets.append(snippet)
            start = index + len(phrase)
        if len(snippets) >= 8:
            break
    money = list(dict.fromkeys(re.findall(r"\$\s?\d[\d,]*(?:\.\d+)?\s?(?:million|billion)?", text, flags=re.I)))[:10]
    shares = list(dict.fromkeys(re.findall(r"\b\d[\d,]*(?:\.\d+)?\s+(?:shares|units)\b", text, flags=re.I)))[:10]
    return {"keywords": sorted(set(found)), "snippets": snippets, "amount_mentions": money, "share_mentions": shares}


DILUTION_FORMS = {
    "S-1", "S-1/A", "S-3", "S-3/A", "F-1", "F-1/A", "F-3", "F-3/A",
    "424B1", "424B2", "424B3", "424B4", "424B5", "424B7", "424B8", "EFFECT",
}


@lru_cache(maxsize=1)
def _sec_client() -> JsonHttpClient:
    settings = get_settings()
    return JsonHttpClient(
        settings.sec_requests_per_minute,
        headers={"User-Agent": settings.sec_user_agent, "Accept-Encoding": "gzip, deflate"},
    )


def collect_sec_filings(partition: dict[str, Any]) -> int:
    settings = get_settings()
    context = fetch_one(
        "select cik from equity_context_snapshots where instrument_id=%s and cik is not null order by asof_date desc limit 1",
        (partition["instrument_id"],),
    )
    cik = str(context["cik"]).lstrip("0") if context and context.get("cik") else ""
    if not cik:
        overview = _ticker_overview(partition["provider_symbol"])
        cik = str(overview.get("cik") or "").lstrip("0")
    if not cik:
        raise EmptyData(f"No SEC CIK found for {partition['provider_symbol']}")
    padded = cik.zfill(10)
    payload = _sec_client().get(f"https://data.sec.gov/submissions/CIK{padded}.json")
    recent = ((payload.get("filings") or {}).get("recent") or {}) if isinstance(payload, dict) else {}
    fields = ["accessionNumber", "filingDate", "reportDate", "acceptanceDateTime", "act", "form", "fileNumber", "filmNumber", "items", "size", "isXBRL", "isInlineXBRL", "primaryDocument", "primaryDocDescription"]
    count = max((len(recent.get(field) or []) for field in fields), default=0)
    cutoff = partition["start_ts"].date() - timedelta(days=365)
    rows: list[dict[str, Any]] = []
    for index in range(count):
        item = {field: (recent.get(field) or [None] * count)[index] if index < len(recent.get(field) or []) else None for field in fields}
        if not item.get("accessionNumber") or not item.get("form"):
            continue
        filing_date = datetime.fromisoformat(item["filingDate"]).date() if item.get("filingDate") else None
        if filing_date and filing_date < cutoff:
            continue
        accession_clean = item["accessionNumber"].replace("-", "")
        primary_doc = item.get("primaryDocument")
        url = f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{accession_clean}/{primary_doc}" if primary_doc else None
        rows.append({
            "accession": item["accessionNumber"], "filing_date": filing_date,
            "report_date": datetime.fromisoformat(item["reportDate"]).date() if item.get("reportDate") else None,
            "accepted_at": as_utc(item["acceptanceDateTime"]) if item.get("acceptanceDateTime") else None,
            "form": item["form"], "primary_document": primary_doc,
            "description": item.get("primaryDocDescription"), "url": url,
            "dilution": item["form"].upper() in DILUTION_FORMS,
            "signals": {},
            "metadata": item,
        })

    if settings.sec_document_scan_enabled and settings.sec_max_documents_per_symbol > 0:
        scan_candidates = sorted(
            [row for row in rows if row.get("url") and (row["form"].upper() in DILUTION_FORMS or row["form"].upper() == "8-K")],
            key=lambda row: (
                row["form"].upper() in DILUTION_FORMS,
                row.get("filing_date") or datetime.min.date(),
            ),
            reverse=True,
        )[: settings.sec_max_documents_per_symbol]
        client = _sec_client()
        for row in scan_candidates:
            try:
                raw = client.get_text(row["url"])
                signals = _dilution_signals(_sec_plain_text(raw))
                row["signals"] = signals
                if signals["keywords"]:
                    row["dilution"] = True
            except ProviderError as exc:
                logger.warning("SEC document scan failed symbol=%s accession=%s: %s", partition["provider_symbol"], row["accession"], exc)

    with db_connection() as conn, conn.cursor() as cur:
        for row in rows:
            cur.execute(
                """
                insert into sec_filings(instrument_id,accession_number,cik,filing_date,report_date,accepted_at,form,
                    primary_document,primary_doc_description,filing_url,is_dilution_relevant,dilution_signals,metadata)
                values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                on conflict(instrument_id,accession_number) do update set
                    filing_date=excluded.filing_date,report_date=excluded.report_date,accepted_at=excluded.accepted_at,
                    form=excluded.form,primary_document=excluded.primary_document,
                    primary_doc_description=excluded.primary_doc_description,filing_url=excluded.filing_url,
                    is_dilution_relevant=excluded.is_dilution_relevant,dilution_signals=excluded.dilution_signals,metadata=excluded.metadata
                """,
                (partition["instrument_id"],row["accession"],cik,row["filing_date"],row["report_date"],row["accepted_at"],
                 row["form"],row["primary_document"],row["description"],row["url"],row["dilution"],Jsonb(row["signals"]),Jsonb(row["metadata"])),
            )
        cur.execute(
            "update collection_partitions set row_count=%s,cursor=%s,heartbeat_at=now(),updated_at=now() where id=%s",
            (len(rows), Jsonb({"finished": True, "cik": padded}), partition["id"]),
        )
        conn.commit()
    return len(rows)



@lru_cache(maxsize=1)
def _finra_client() -> JsonHttpClient:
    return JsonHttpClient(get_settings().finra_requests_per_minute)


@lru_cache(maxsize=1)
def _alpaca_news_client() -> JsonHttpClient:
    settings = get_settings()
    return JsonHttpClient(
        settings.alpaca_requests_per_minute,
        headers={"APCA-API-KEY-ID": settings.alpaca_api_key, "APCA-API-SECRET-KEY": settings.alpaca_api_secret},
    )

def collect_finra_short_volume(partition: dict[str, Any]) -> int:
    trade_date = partition["start_ts"].date()
    source_file = f"CNMSshvol{trade_date:%Y%m%d}.txt"
    url = f"https://cdn.finra.org/equity/regsho/daily/{source_file}"
    try:
        text = _finra_client().get_text(url)
    except ProviderError as exc:
        if exc.code == "http_404":
            raise EmptyData(f"No FINRA short-volume file for {trade_date}") from exc
        raise
    selected = fetch_all(
        """
        select id,provider_symbol
          from instruments
         where provider='alpaca' and preferred=true
        """
    )
    by_symbol = {row["provider_symbol"].upper(): row["id"] for row in selected}
    rows = []
    reader = csv.DictReader(io.StringIO(text), delimiter="|")
    for row in reader:
        symbol = str(row.get("Symbol") or "").upper()
        instrument_id = by_symbol.get(symbol)
        if not instrument_id:
            continue
        rows.append((instrument_id, symbol, row))
    with db_connection() as conn, conn.cursor() as cur:
        for instrument_id, symbol, row in rows:
            cur.execute(
                """
                insert into finra_short_volume(instrument_id,trade_date,symbol,short_volume,short_exempt_volume,total_volume,market,source_file,metadata)
                values (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                on conflict(instrument_id,trade_date,market) do update set short_volume=excluded.short_volume,short_exempt_volume=excluded.short_exempt_volume,total_volume=excluded.total_volume,source_file=excluded.source_file,metadata=excluded.metadata
                """,
                (instrument_id,trade_date,symbol,as_float(row.get("ShortVolume")),as_float(row.get("ShortExemptVolume")),
                 as_float(row.get("TotalVolume")),row.get("Market") or '',source_file,Jsonb(row)),
            )
        cur.execute(
            "update collection_partitions set row_count=%s,cursor=%s,heartbeat_at=now(),updated_at=now() where id=%s",
            (len(rows), Jsonb({"finished": True, "source_file": source_file}), partition["id"]),
        )
        conn.commit()
    return len(rows)


def collect_alpaca_news(partition: dict[str, Any]) -> int:
    client = _alpaca_news_client()
    cursor = dict(partition.get("cursor") or {})
    if cursor.get("finished"):
        return int(partition.get("row_count") or 0)
    page_token = cursor.get("next_page_token")
    total = int(partition.get("row_count") or 0)
    while True:
        params: dict[str, Any] = {
            "symbols": partition["provider_symbol"], "start": partition["start_ts"].isoformat(),
            "end": partition["end_ts"].isoformat(), "limit": 50, "sort": "asc",
        }
        if page_token:
            params["page_token"] = page_token
        payload = client.get("https://data.alpaca.markets/v1beta1/news", params=params)
        articles = payload.get("news") or []
        with db_connection() as conn, conn.cursor() as cur:
            for item in articles:
                cur.execute(
                    """
                    insert into market_news(provider,news_id,headline,summary,author,source,published_at,updated_at,symbols,url,content,metadata)
                    values ('alpaca',%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    on conflict(provider,news_id) do update set headline=excluded.headline,summary=excluded.summary,
                        updated_at=excluded.updated_at,symbols=excluded.symbols,url=excluded.url,content=excluded.content,metadata=excluded.metadata
                    """,
                    (item["id"],item.get("headline") or "",item.get("summary"),item.get("author"),item.get("source"),
                     as_utc(item["created_at"]),as_utc(item["updated_at"]) if item.get("updated_at") else None,
                     item.get("symbols") or [],item.get("url"),item.get("content"),Jsonb(item)),
                )
            page_token = payload.get("next_page_token")
            total += len(articles)
            cur.execute(
                "update collection_partitions set row_count=%s,cursor=%s,heartbeat_at=now(),updated_at=now() where id=%s",
                (total, Jsonb({"next_page_token": page_token, "finished": not bool(page_token)}), partition["id"]),
            )
            conn.commit()
        if not page_token:
            break
    if total == 0:
        raise EmptyData(f"No Alpaca news for {partition['provider_symbol']} in the run window")
    return total



@lru_cache(maxsize=1)
def _crypto_catalogue_client() -> JsonHttpClient:
    return JsonHttpClient(get_settings().binance_requests_per_minute)


def _normalise_crypto_asset(value: str | None) -> str:
    value = (value or "").upper()
    aliases = {"XBT": "BTC", "XXBT": "BTC", "XETH": "ETH", "ZUSD": "USD", "ZEUR": "EUR", "ZGBP": "GBP"}
    return aliases.get(value, value.lstrip("XZ") if len(value) > 3 and value[0] in {"X", "Z"} else value)


def collect_crypto_catalogues(partition: dict[str, Any]) -> int:
    """Populate cross-venue symbol mappings used by the prospective stream worker."""
    settings = get_settings()
    rows: list[dict[str, Any]] = []

    existing = fetch_all(
        """
        select provider,provider_symbol,canonical_symbol,base_asset,quote_asset,status,tradable,priority,metadata
          from instruments
         where provider in ('coinbase','binance') and asset_class='crypto_spot'
        """
    )
    for item in existing:
        rows.append(
            {
                "provider": "binance_spot" if item["provider"] == "binance" else "coinbase",
                "market_type": "spot",
                "venue_symbol": item["provider_symbol"],
                "canonical_symbol": item["canonical_symbol"],
                "base_asset": item["base_asset"],
                "quote_asset": item["quote_asset"],
                "status": item["status"],
                "tradable": item["tradable"],
                "priority": int(item["priority"] or 0),
                "metadata": item["metadata"] or {},
            }
        )

    client = _crypto_catalogue_client()
    futures = client.get("https://fapi.binance.com/fapi/v1/exchangeInfo")
    for item in futures.get("symbols") or []:
        if item.get("contractType") != "PERPETUAL" or item.get("status") != "TRADING":
            continue
        base = _normalise_crypto_asset(item.get("baseAsset"))
        quote_asset = _normalise_crypto_asset(item.get("quoteAsset"))
        rows.append(
            {
                "provider": "binance_futures",
                "market_type": "perpetual",
                "venue_symbol": item["symbol"],
                "canonical_symbol": base,
                "base_asset": base,
                "quote_asset": quote_asset,
                "status": item.get("status"),
                "tradable": True,
                "priority": 1000 if quote_asset == "USDT" else 0,
                "metadata": item,
            }
        )

    kraken = JsonHttpClient(settings.kraken_requests_per_minute).get("https://api.kraken.com/0/public/AssetPairs")
    for key, item in (kraken.get("result") or {}).items():
        wsname = item.get("wsname") or ""
        if "/" not in wsname:
            continue
        base, quote_asset = (_normalise_crypto_asset(part) for part in wsname.split("/", 1))
        if quote_asset not in settings.kraken_quote_priority:
            continue
        rows.append(
            {
                "provider": "kraken",
                "market_type": "spot",
                "venue_symbol": wsname,
                "canonical_symbol": base,
                "base_asset": base,
                "quote_asset": quote_asset,
                "status": "online",
                "tradable": True,
                "priority": (len(settings.kraken_quote_priority) - settings.kraken_quote_priority.index(quote_asset)) * 1000,
                "metadata": {"rest_key": key, **item},
            }
        )

    bybit_client = JsonHttpClient(settings.bybit_requests_per_minute)
    cursor = None
    while True:
        params = {"category": "linear", "limit": 1000}
        if cursor:
            params["cursor"] = cursor
        payload = bybit_client.get("https://api.bybit.com/v5/market/instruments-info", params=params)
        result = payload.get("result") or {}
        for item in result.get("list") or []:
            if item.get("contractType") not in {"LinearPerpetual", "InversePerpetual"}:
                continue
            if item.get("status") != "Trading":
                continue
            base = _normalise_crypto_asset(item.get("baseCoin"))
            quote_asset = _normalise_crypto_asset(item.get("quoteCoin"))
            rows.append(
                {
                    "provider": "bybit",
                    "market_type": "perpetual",
                    "venue_symbol": item["symbol"],
                    "canonical_symbol": base,
                    "base_asset": base,
                    "quote_asset": quote_asset,
                    "status": item.get("status"),
                    "tradable": True,
                    "priority": 1000 if quote_asset in {"USDT", "USDC"} else 0,
                    "metadata": item,
                }
            )
        cursor = result.get("nextPageCursor")
        if not cursor:
            break

    with db_connection() as conn, conn.cursor() as cur:
        for row in rows:
            cur.execute(
                """
                insert into crypto_venue_symbols(
                    provider,market_type,venue_symbol,canonical_symbol,base_asset,quote_asset,
                    status,tradable,priority,metadata,last_seen_at
                ) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,now())
                on conflict(provider,market_type,venue_symbol) do update set
                    canonical_symbol=excluded.canonical_symbol,base_asset=excluded.base_asset,
                    quote_asset=excluded.quote_asset,status=excluded.status,tradable=excluded.tradable,
                    priority=excluded.priority,metadata=excluded.metadata,last_seen_at=now()
                """,
                (
                    row["provider"], row["market_type"], row["venue_symbol"], row["canonical_symbol"],
                    row["base_asset"], row["quote_asset"], row["status"], row["tradable"],
                    row["priority"], Jsonb(row["metadata"]),
                ),
            )
        cur.execute(
            "update collection_partitions set row_count=%s,cursor=%s,heartbeat_at=now(),updated_at=now() where id=%s",
            (len(rows), Jsonb({"finished": True}), partition["id"]),
        )
        conn.commit()
    return len(rows)


@lru_cache(maxsize=1)
def _coingecko_client() -> JsonHttpClient:
    settings = get_settings()
    headers = {"x-cg-demo-api-key": settings.coingecko_demo_api_key} if settings.coingecko_demo_api_key else None
    return JsonHttpClient(settings.coingecko_requests_per_minute, headers=headers)


def collect_coingecko_supply(partition: dict[str, Any]) -> int:
    client = _coingecko_client()
    captured_at = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    count = 0
    page = int((partition.get("cursor") or {}).get("page") or 1)
    while page <= 20:
        payload = client.get(
            "https://api.coingecko.com/api/v3/coins/markets",
            params={
                "vs_currency": "usd",
                "order": "market_cap_desc",
                "per_page": 250,
                "page": page,
                "sparkline": "false",
                "price_change_percentage": "1h,24h,7d",
            },
        )
        if not payload:
            break
        with db_connection() as conn, conn.cursor() as cur:
            for item in payload:
                cur.execute(
                    """
                    insert into crypto_supply_snapshots(
                        source,source_id,canonical_symbol,name,asof_ts,current_price,market_cap,
                        fully_diluted_valuation,total_volume_24h,circulating_supply,total_supply,
                        max_supply,ath,atl,metadata
                    ) values ('coingecko',%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    on conflict(source,source_id,asof_ts) do update set
                        current_price=excluded.current_price,market_cap=excluded.market_cap,
                        fully_diluted_valuation=excluded.fully_diluted_valuation,
                        total_volume_24h=excluded.total_volume_24h,
                        circulating_supply=excluded.circulating_supply,total_supply=excluded.total_supply,
                        max_supply=excluded.max_supply,metadata=excluded.metadata
                    """,
                    (
                        item["id"], str(item.get("symbol") or "").upper(), item.get("name"), captured_at,
                        as_float(item.get("current_price")), as_float(item.get("market_cap")),
                        as_float(item.get("fully_diluted_valuation")), as_float(item.get("total_volume")),
                        as_float(item.get("circulating_supply")), as_float(item.get("total_supply")),
                        as_float(item.get("max_supply")), as_float(item.get("ath")), as_float(item.get("atl")),
                        Jsonb(item),
                    ),
                )
            count += len(payload)
            page += 1
            cur.execute(
                "update collection_partitions set row_count=%s,cursor=%s,heartbeat_at=now(),updated_at=now() where id=%s",
                (count, Jsonb({"page": page, "finished": len(payload) < 250}), partition["id"]),
            )
            conn.commit()
        if len(payload) < 250:
            break
    return count


@lru_cache(maxsize=1)
def _binance_futures_client() -> JsonHttpClient:
    return JsonHttpClient(get_settings().binance_requests_per_minute)


def _fetch_binance_series(path: str, symbol: str, start_ms: int, end_ms: int, *, period: str = "5m") -> list[dict[str, Any]]:
    client = _binance_futures_client()
    rows: list[dict[str, Any]] = []
    cursor = start_ms
    while cursor <= end_ms:
        params: dict[str, Any] = {"symbol": symbol, "startTime": cursor, "endTime": end_ms, "limit": 500}
        if path.startswith("/futures/data/"):
            params["period"] = period
        payload = client.get(f"https://fapi.binance.com{path}", params=params)
        if not payload:
            break
        rows.extend(payload)
        timestamps = [
            int(item.get("timestamp") or item.get("fundingTime") or 0)
            for item in payload
            if item.get("timestamp") is not None or item.get("fundingTime") is not None
        ]
        if not timestamps:
            break
        next_cursor = max(timestamps) + 1
        if next_cursor <= cursor or len(payload) < 500:
            break
        cursor = next_cursor
    return rows


def collect_crypto_derivatives(partition: dict[str, Any]) -> int:
    canonical = str((partition.get("cursor") or {}).get("canonical_symbol") or partition["provider_symbol"]).upper()
    mapping = fetch_one(
        """
        select venue_symbol from crypto_venue_symbols
         where provider='binance_futures' and market_type='perpetual'
           and canonical_symbol=%s and tradable=true
         order by priority desc limit 1
        """,
        (canonical,),
    )
    if mapping:
        symbol = mapping["venue_symbol"]
    else:
        # Catalogue and derivative partitions can be claimed by separate workers at
        # nearly the same time. Resolve the venue symbol directly instead of
        # guessing that every asset has a USDT perpetual contract.
        exchange_info = _binance_futures_client().get("https://fapi.binance.com/fapi/v1/exchangeInfo")
        candidates = [
            item for item in exchange_info.get("symbols", [])
            if str(item.get("baseAsset") or "").upper() == canonical
            and item.get("contractType") == "PERPETUAL"
            and item.get("status") == "TRADING"
        ]
        candidates.sort(key=lambda item: (str(item.get("quoteAsset") or "").upper() != "USDT", item.get("symbol") or ""))
        if not candidates:
            raise EmptyData(f"No active Binance perpetual contract for {canonical}")
        symbol = str(candidates[0]["symbol"])
    start_ms = int(partition["start_ts"].timestamp() * 1000)
    end_ms = int(partition["end_ts"].timestamp() * 1000)

    series = {
        "funding": _fetch_binance_series("/fapi/v1/fundingRate", symbol, start_ms, end_ms),
        "open_interest": _fetch_binance_series("/futures/data/openInterestHist", symbol, start_ms, end_ms),
        "global_ratio": _fetch_binance_series("/futures/data/globalLongShortAccountRatio", symbol, start_ms, end_ms),
        "top_account": _fetch_binance_series("/futures/data/topLongShortAccountRatio", symbol, start_ms, end_ms),
        "top_position": _fetch_binance_series("/futures/data/topLongShortPositionRatio", symbol, start_ms, end_ms),
        "taker_ratio": _fetch_binance_series("/futures/data/takerlongshortRatio", symbol, start_ms, end_ms),
    }
    merged: dict[int, dict[str, Any]] = {}
    for item in series["open_interest"]:
        ts = int(item.get("timestamp") or 0)
        merged.setdefault(ts, {}).update(
            {
                "open_interest": as_float(item.get("sumOpenInterest")),
                "open_interest_value": as_float(item.get("sumOpenInterestValue")),
                "open_interest_payload": item,
            }
        )
    for name, field in (
        ("global_ratio", "global_long_short_ratio"),
        ("top_account", "top_account_long_short_ratio"),
        ("top_position", "top_position_long_short_ratio"),
        ("taker_ratio", "taker_buy_sell_ratio"),
    ):
        for item in series[name]:
            ts = int(item.get("timestamp") or 0)
            value = item.get("longShortRatio") if name != "taker_ratio" else item.get("buySellRatio")
            merged.setdefault(ts, {})[field] = as_float(value)
            merged[ts][f"{name}_payload"] = item
    for item in series["funding"]:
        ts = int(item.get("fundingTime") or 0)
        merged.setdefault(ts, {}).update(
            {
                "funding_rate": as_float(item.get("fundingRate")),
                "mark_price": as_float(item.get("markPrice")),
                "funding_payload": item,
            }
        )

    count = 0
    with db_connection() as conn, conn.cursor() as cur:
        for ts_ms, item in sorted(merged.items()):
            if not ts_ms:
                continue
            ts = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
            cur.execute(
                """
                insert into crypto_derivatives_metrics(
                    provider,venue_symbol,canonical_symbol,ts,interval,mark_price,index_price,
                    funding_rate,open_interest,open_interest_value,global_long_short_ratio,
                    top_account_long_short_ratio,top_position_long_short_ratio,taker_buy_sell_ratio,metadata
                ) values ('binance_futures',%s,%s,%s,'5m',%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                on conflict(provider,venue_symbol,ts,interval) do update set
                    mark_price=excluded.mark_price,funding_rate=excluded.funding_rate,
                    open_interest=excluded.open_interest,open_interest_value=excluded.open_interest_value,
                    global_long_short_ratio=excluded.global_long_short_ratio,
                    top_account_long_short_ratio=excluded.top_account_long_short_ratio,
                    top_position_long_short_ratio=excluded.top_position_long_short_ratio,
                    taker_buy_sell_ratio=excluded.taker_buy_sell_ratio,metadata=excluded.metadata
                """,
                (
                    symbol, canonical, ts, item.get("mark_price"), item.get("index_price"),
                    item.get("funding_rate"), item.get("open_interest"), item.get("open_interest_value"),
                    item.get("global_long_short_ratio"), item.get("top_account_long_short_ratio"),
                    item.get("top_position_long_short_ratio"), item.get("taker_buy_sell_ratio"),
                    Jsonb(item),
                ),
            )
            count += 1
        cur.execute(
            "update collection_partitions set row_count=%s,cursor=%s,heartbeat_at=now(),updated_at=now() where id=%s",
            (count, Jsonb({"finished": True, "canonical_symbol": canonical, "venue_symbol": symbol}), partition["id"]),
        )
        conn.commit()
    return count


def process_enrichment_partition(partition: dict[str, Any]) -> int:
    handlers = {
        "massive_context": collect_massive_context,
        "sec_filings": collect_sec_filings,
        "finra_short_volume": collect_finra_short_volume,
        "news": collect_alpaca_news,
        "crypto_catalogues": collect_crypto_catalogues,
        "coingecko_supply": collect_coingecko_supply,
        "crypto_derivatives": collect_crypto_derivatives,
    }
    try:
        return handlers[partition["data_type"]](partition)
    except KeyError as exc:
        raise ValueError(f"Unsupported enrichment partition type: {partition['data_type']}") from exc
