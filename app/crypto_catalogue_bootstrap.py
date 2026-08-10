from __future__ import annotations

import json
from typing import Any

from app.config import get_settings
from app.db import db_connection, fetch_all
from app.enrichment import _normalise_crypto_asset
from app.http import JsonHttpClient


def _row_key(row: dict[str, Any]) -> tuple[str, str, str]:
    return row["provider"], row["market_type"], row["venue_symbol"]


def refresh_crypto_venue_catalogue() -> int:
    """Refresh all stream venue mappings with one bulk database transaction.

    This is intentionally separate from the normal collection-partition handler so
    the always-on stream can recover from an empty catalogue without depending on
    the batch worker. Venue discovery semantics match collect_crypto_catalogues().
    """
    settings = get_settings()
    rows: list[dict[str, Any]] = []

    existing = fetch_all(
        """
        select provider,provider_symbol,canonical_symbol,base_asset,quote_asset,status,
               tradable,priority,metadata
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
                "tradable": bool(item["tradable"]),
                "priority": int(item["priority"] or 0),
                "metadata": item["metadata"] or {},
            }
        )

    binance_client = JsonHttpClient(settings.binance_requests_per_minute)
    futures = binance_client.get("https://fapi.binance.com/fapi/v1/exchangeInfo")
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

    kraken = JsonHttpClient(settings.kraken_requests_per_minute).get(
        "https://api.kraken.com/0/public/AssetPairs"
    )
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
                "priority": (
                    len(settings.kraken_quote_priority)
                    - settings.kraken_quote_priority.index(quote_asset)
                )
                * 1000,
                "metadata": {"rest_key": key, **item},
            }
        )

    bybit_client = JsonHttpClient(settings.bybit_requests_per_minute)
    cursor: str | None = None
    seen_cursors: set[str] = set()
    while True:
        params: dict[str, Any] = {"category": "linear", "limit": 1000}
        if cursor:
            params["cursor"] = cursor
        payload = bybit_client.get(
            "https://api.bybit.com/v5/market/instruments-info", params=params
        )
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
        next_cursor = str(result.get("nextPageCursor") or "")
        if not next_cursor or next_cursor in seen_cursors:
            break
        seen_cursors.add(next_cursor)
        cursor = next_cursor

    # Deterministically collapse any provider payload duplication before COPY.
    unique = {_row_key(row): row for row in rows}
    rows = [unique[key] for key in sorted(unique)]

    with db_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            create temporary table crypto_venue_catalogue_stage(
                provider text not null,
                market_type text not null,
                venue_symbol text not null,
                canonical_symbol text not null,
                base_asset text,
                quote_asset text,
                status text,
                tradable boolean not null,
                priority integer not null,
                metadata_text text not null,
                primary key(provider,market_type,venue_symbol)
            ) on commit drop
            """
        )
        with cur.copy(
            """
            copy crypto_venue_catalogue_stage(
                provider,market_type,venue_symbol,canonical_symbol,base_asset,quote_asset,
                status,tradable,priority,metadata_text
            ) from stdin
            """
        ) as copy:
            for row in rows:
                copy.write_row(
                    (
                        row["provider"],
                        row["market_type"],
                        row["venue_symbol"],
                        row["canonical_symbol"],
                        row["base_asset"],
                        row["quote_asset"],
                        row["status"],
                        row["tradable"],
                        row["priority"],
                        json.dumps(row["metadata"], separators=(",", ":"), default=str),
                    )
                )

        cur.execute(
            """
            update crypto_venue_symbols
               set tradable=false,
                   status='not_seen_in_latest_catalogue',
                   last_seen_at=now()
             where provider in ('coinbase','binance_spot','binance_futures','kraken','bybit')
            """
        )
        cur.execute(
            """
            insert into crypto_venue_symbols(
                provider,market_type,venue_symbol,canonical_symbol,base_asset,quote_asset,
                status,tradable,priority,metadata,last_seen_at
            )
            select provider,market_type,venue_symbol,canonical_symbol,base_asset,quote_asset,
                   status,tradable,priority,metadata_text::jsonb,now()
              from crypto_venue_catalogue_stage
            on conflict(provider,market_type,venue_symbol) do update set
                canonical_symbol=excluded.canonical_symbol,
                base_asset=excluded.base_asset,
                quote_asset=excluded.quote_asset,
                status=excluded.status,
                tradable=excluded.tradable,
                priority=excluded.priority,
                metadata=excluded.metadata,
                last_seen_at=now()
            """
        )
        conn.commit()

    return len(rows)
