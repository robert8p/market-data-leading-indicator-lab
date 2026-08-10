from __future__ import annotations

import asyncio
import json
import time
from collections.abc import Iterator
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

import httpx
import websockets

from app.config import get_settings
from app.db import db_connection, fetch_all


_ORIGINAL_WEBSOCKET_CONNECT = websockets.connect


class _CanonicalSymbolLookup:
    """Provider-aware compatibility lookup for legacy stream variable names.

    Kraken only calls .get(); Bybit both iterates the mapping and calls .get().
    Iteration therefore exposes Bybit symbols only, while .get() resolves either
    Kraken or Bybit symbols. This preserves the intended behaviour of both handlers.
    """

    def __init__(self, lookup: dict[str, str], bybit_symbols: list[str]):
        self._lookup = lookup
        self._bybit_symbols = tuple(bybit_symbols)

    def __iter__(self) -> Iterator[str]:
        return iter(self._bybit_symbols)

    def __len__(self) -> int:
        return len(self._bybit_symbols)

    def get(self, key: str, default: Any = None) -> Any:
        return self._lookup.get(key, default)


def _connect_with_longer_open_timeout(*args: Any, **kwargs: Any) -> Any:
    """Give venue handshakes enough time during a simultaneous worker restart."""
    kwargs.setdefault("open_timeout", 30)
    return _ORIGINAL_WEBSOCKET_CONNECT(*args, **kwargs)


def _health(provider: str, service: str, *, status: str, error: str | None = None, messages: int = 0) -> None:
    """Health upsert with fully typed values and recovered-error clearing."""
    now = datetime.now(timezone.utc)
    last_message_at = now if messages > 0 else None
    last_success_at = now if status == "connected" else None
    last_error_at = now if error is not None else None
    reconnect_delta = 1 if status == "reconnecting" else 0

    with db_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            insert into provider_health(
                provider,service,status,last_message_at,last_success_at,last_error_at,
                message_count,reconnect_count,last_error,updated_at
            ) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,now())
            on conflict(provider,service) do update set
                status=excluded.status,
                last_message_at=coalesce(excluded.last_message_at,provider_health.last_message_at),
                last_success_at=coalesce(excluded.last_success_at,provider_health.last_success_at),
                last_error_at=coalesce(excluded.last_error_at,provider_health.last_error_at),
                message_count=provider_health.message_count+excluded.message_count,
                reconnect_count=provider_health.reconnect_count+excluded.reconnect_count,
                last_error=case
                    when excluded.status='connected' then null
                    else coalesce(excluded.last_error,provider_health.last_error)
                end,
                updated_at=now()
            """,
            (
                provider,
                service,
                status,
                last_message_at,
                last_success_at,
                last_error_at,
                int(messages),
                reconnect_delta,
                error,
            ),
        )
        conn.commit()


def _active_targets() -> set[str]:
    settings = get_settings()
    targets = set(settings.crypto_stream_core_symbols)
    rows = fetch_all(
        """
        select canonical_symbol
          from crypto_capture_targets
         where expires_at > now()
         order by priority_score desc nulls last, last_observed_at desc nulls last, updated_at desc
         limit %s
        """,
        (settings.crypto_stream_max_dynamic_targets,),
    )
    targets.update(str(row["canonical_symbol"]).upper() for row in rows)
    return targets


async def _bybit_heartbeat(ws: Any, shutdown: asyncio.Event) -> None:
    """Send Bybit's documented application-level heartbeat every 20 seconds."""
    while not shutdown.is_set():
        try:
            await asyncio.wait_for(shutdown.wait(), timeout=20)
            return
        except asyncio.TimeoutError:
            await ws.send(json.dumps({"op": "ping"}))


def _stable_bybit_stream(stream_module: Any):
    async def stream_bybit(collector: Any, mappings: list[dict[str, Any]], active: set[str]) -> None:
        if not mappings:
            return
        mapping_by_symbol = {row["venue_symbol"]: row["canonical_symbol"] for row in mappings}
        args: list[str] = []
        for symbol in mapping_by_symbol:
            args.extend(
                [
                    f"orderbook.50.{symbol}",
                    f"publicTrade.{symbol}",
                    f"tickers.{symbol}",
                    f"allLiquidation.{symbol}",
                ]
            )
        url = "wss://stream.bybit.com/v5/public/linear"
        while not stream_module.shutdown.is_set():
            heartbeat: asyncio.Task[Any] | None = None
            try:
                _health("bybit", "websocket", status="connecting")
                # Bybit documents an application-level {"op":"ping"} heartbeat.
                # Disable generic protocol pings and send the documented heartbeat.
                async with _connect_with_longer_open_timeout(
                    url,
                    ping_interval=None,
                    max_size=16 * 1024 * 1024,
                ) as ws:
                    await ws.send(json.dumps({"op": "subscribe", "args": args}))
                    heartbeat = asyncio.create_task(_bybit_heartbeat(ws, stream_module.shutdown))
                    _health("bybit", "websocket", status="connected")
                    async for raw_message in ws:
                        message = json.loads(raw_message)
                        topic = str(message.get("topic") or "")
                        if not topic:
                            continue
                        data = message.get("data")
                        symbol = topic.split(".")[-1] if "." in topic else ""
                        canonical = mapping_by_symbol.get(symbol)
                        if not canonical:
                            continue
                        raw_enabled = stream_module.is_raw(canonical, active)
                        ts = stream_module.parse_ts(message.get("ts") or time.time())
                        if topic.startswith("orderbook"):
                            book_data = data or {}
                            state = collector.book_state("bybit", "perpetual", symbol)
                            if message.get("type") == "snapshot":
                                state.snapshot(book_data.get("b") or [], book_data.get("a") or [])
                            else:
                                for price, size in book_data.get("b") or []:
                                    state.update("buy", price, size)
                                for price, size in book_data.get("a") or []:
                                    state.update("sell", price, size)
                            await collector.book(
                                "bybit", "perpetual", symbol, canonical, ts, message, raw_enabled
                            )
                        elif topic.startswith("publicTrade"):
                            for trade in data or []:
                                price = stream_module.f(trade.get("p"))
                                size = stream_module.f(trade.get("v"))
                                if price is not None and size is not None:
                                    await collector.trade(
                                        "bybit",
                                        "perpetual",
                                        symbol,
                                        canonical,
                                        stream_module.parse_ts(trade.get("T") or message.get("ts")),
                                        price,
                                        size,
                                        str(trade.get("S") or "Sell").lower(),
                                        trade,
                                        raw_enabled,
                                    )
                        elif topic.startswith("tickers"):
                            ticker = data or {}
                            await collector.derivative(
                                "bybit",
                                "perpetual",
                                symbol,
                                canonical,
                                ts,
                                {
                                    "mark_price": stream_module.f(ticker.get("markPrice")),
                                    "index_price": stream_module.f(ticker.get("indexPrice")),
                                    "funding_rate": stream_module.f(ticker.get("fundingRate")),
                                    "open_interest": stream_module.f(ticker.get("openInterest")),
                                    "open_interest_value": stream_module.f(ticker.get("openInterestValue")),
                                },
                                message,
                                raw_enabled,
                            )
                        elif topic.startswith("allLiquidation"):
                            liquidation_items = (
                                data
                                if isinstance(data, list)
                                else ([data] if isinstance(data, dict) else [])
                            )
                            for item in liquidation_items:
                                price = stream_module.f(item.get("p"))
                                size = stream_module.f(item.get("v"))
                                if price is not None and size is not None:
                                    await collector.liquidation(
                                        "bybit",
                                        "perpetual",
                                        symbol,
                                        canonical,
                                        stream_module.parse_ts(item.get("T") or message.get("ts")),
                                        str(item.get("T") or uuid4()),
                                        str(item.get("S") or "Sell").lower(),
                                        price,
                                        size,
                                        item,
                                        raw_enabled,
                                    )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                _health("bybit", "websocket", status="reconnecting", error=str(exc))
                stream_module.logger.warning("Bybit stream reconnecting: %s", exc)
                await asyncio.sleep(5)
            finally:
                if heartbeat is not None:
                    heartbeat.cancel()
                    try:
                        await heartbeat
                    except asyncio.CancelledError:
                        pass

    return stream_bybit


def _stable_binance_open_interest(stream_module: Any):
    async def poll_binance_open_interest(
        collector: Any, mappings: list[dict[str, Any]], active: set[str]
    ) -> None:
        async with httpx.AsyncClient(timeout=20) as client:
            while not stream_module.shutdown.is_set():
                rate_limited = False
                for row in mappings:
                    if stream_module.shutdown.is_set():
                        return
                    try:
                        symbol = row["venue_symbol"]
                        response = await client.get(
                            "https://fapi.binance.com/fapi/v1/openInterest",
                            params={"symbol": symbol},
                        )
                        if response.status_code == 429:
                            retry_after = response.headers.get("Retry-After")
                            try:
                                delay = max(5.0, min(float(retry_after or 10), 120.0))
                            except ValueError:
                                delay = 10.0
                            stream_module.logger.warning(
                                "Binance open-interest rate limited; backing off %.1fs", delay
                            )
                            rate_limited = True
                            try:
                                await asyncio.wait_for(stream_module.shutdown.wait(), timeout=delay)
                                return
                            except asyncio.TimeoutError:
                                break
                        response.raise_for_status()
                        payload = response.json()
                        oi = stream_module.f(payload.get("openInterest"))
                        canonical = row["canonical_symbol"]
                        await collector.derivative(
                            "binance_futures",
                            "perpetual",
                            symbol,
                            canonical,
                            stream_module.utc_now(),
                            {"open_interest": oi},
                            payload,
                            stream_module.is_raw(canonical, active),
                        )
                        # Keep this secondary poll comfortably below request-weight ceilings.
                        await asyncio.sleep(0.1)
                    except asyncio.CancelledError:
                        raise
                    except Exception as exc:
                        stream_module.logger.debug(
                            "Open-interest poll failed %s: %s", row.get("venue_symbol"), exc
                        )
                if rate_limited:
                    continue
                try:
                    await asyncio.wait_for(stream_module.shutdown.wait(), timeout=300)
                    return
                except asyncio.TimeoutError:
                    pass

    return poll_binance_open_interest


def install_crypto_stream_runtime_fixes(stream_module: Any) -> None:
    """Install narrow fixes before the stream event loop creates any tasks."""
    stream_module._health = _health
    stream_module.websockets.connect = _connect_with_longer_open_timeout
    stream_module.poll_binance_open_interest = _stable_binance_open_interest(stream_module)

    targets = sorted(_active_targets())
    rows = fetch_all(
        """
        select provider,venue_symbol,canonical_symbol
          from crypto_venue_symbols
         where provider in ('coinbase','kraken','bybit')
           and tradable=true
           and canonical_symbol = any(%s)
        """,
        (targets,),
    )
    coinbase: dict[str, str] = {}
    kraken_and_bybit: dict[str, str] = {}
    bybit_symbols: list[str] = []
    for row in rows:
        provider = row["provider"]
        venue_symbol = row["venue_symbol"]
        canonical_symbol = row["canonical_symbol"]
        if provider == "coinbase":
            coinbase[venue_symbol] = canonical_symbol
        elif provider in {"kraken", "bybit"}:
            kraken_and_bybit[venue_symbol] = canonical_symbol
            if provider == "bybit":
                bybit_symbols.append(venue_symbol)

    # Compatibility aliases for the Coinbase and Kraken handlers whose local
    # mapping variable names differ from the names referenced in their bodies.
    stream_module.canonical_by_product = coinbase
    stream_module.canonical_by_symbol = _CanonicalSymbolLookup(
        kraken_and_bybit,
        sorted(bybit_symbols),
    )
    stream_module.stream_bybit = _stable_bybit_stream(stream_module)
