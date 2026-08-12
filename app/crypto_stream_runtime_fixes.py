from __future__ import annotations

import asyncio
import json
import logging
import time
from collections.abc import Iterator
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

import httpx
import websockets
from psycopg.types.json import Jsonb

from app.config import get_settings
from app.db import db_connection, fetch_all, get_pool


_ORIGINAL_WEBSOCKET_CONNECT = websockets.connect
logger = logging.getLogger(__name__)


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
    """Best-effort health telemetry that can never take down a market-data stream.

    Health writes run inside websocket coroutines in the legacy stream code. A
    normal pool checkout can therefore block the entire asyncio event loop for the
    global acquire timeout. Use a deliberately tiny checkout deadline plus a local
    statement timeout, and swallow telemetry failures so observability degradation
    cannot become feed degradation.
    """
    now = datetime.now(timezone.utc)
    last_message_at = now if messages > 0 else None
    last_success_at = now if status == "connected" else None
    last_error_at = now if error is not None else None
    reconnect_delta = 1 if status == "reconnecting" else 0

    try:
        with get_pool().connection(timeout=0.25) as conn, conn.cursor() as cur:
            cur.execute("set local statement_timeout = '1000ms'")
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
    except Exception as exc:
        logger.warning(
            "Provider health update skipped provider=%s service=%s status=%s error=%s",
            provider,
            service,
            status,
            exc,
        )


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


def _bucket_params(row: Any) -> tuple[Any, ...]:
    book, der = row.book, row.derivative
    return (
        row.provider,
        row.market_type,
        row.venue_symbol,
        row.canonical_symbol,
        row.ts,
        row.trade_count,
        row.buy_count,
        row.sell_count,
        row.buy_base_volume,
        row.sell_base_volume,
        row.buy_quote_volume,
        row.sell_quote_volume,
        row.last_trade_price,
        book.get("bid_price"),
        book.get("bid_size"),
        book.get("ask_price"),
        book.get("ask_size"),
        book.get("spread"),
        book.get("spread_bps"),
        book.get("mid_price"),
        book.get("microprice"),
        book.get("bid_depth"),
        book.get("ask_depth"),
        book.get("depth_imbalance"),
        book.get("weighted_bid_price"),
        book.get("weighted_ask_price"),
        row.book_update_count,
        der.get("mark_price"),
        der.get("index_price"),
        der.get("funding_rate"),
        der.get("next_funding_at"),
        der.get("open_interest"),
        der.get("open_interest_value"),
        row.liquidation_buy_notional,
        row.liquidation_sell_notional,
        Jsonb(row.metadata),
    )


def _bulk_flush_rows(rows: list[Any]) -> None:
    """Bulk the 1-second aggregate upserts instead of one network round trip per row."""
    if not rows:
        return
    sql = """
        insert into crypto_microstructure_1s(
            provider,market_type,venue_symbol,canonical_symbol,ts,
            trade_count,buy_count,sell_count,buy_base_volume,sell_base_volume,
            buy_quote_volume,sell_quote_volume,last_trade_price,
            bid_price,bid_size,ask_price,ask_size,spread,spread_bps,mid_price,microprice,
            bid_depth,ask_depth,depth_imbalance,weighted_bid_price,weighted_ask_price,
            book_update_count,mark_price,index_price,funding_rate,next_funding_at,
            open_interest,open_interest_value,liquidation_buy_notional,
            liquidation_sell_notional,metadata
        ) values (
            %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
            %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
            %s,%s,%s,%s,%s,%s,%s,%s,%s,%s
        )
        on conflict(provider,market_type,venue_symbol,ts) do update set
            trade_count=crypto_microstructure_1s.trade_count+excluded.trade_count,
            buy_count=crypto_microstructure_1s.buy_count+excluded.buy_count,
            sell_count=crypto_microstructure_1s.sell_count+excluded.sell_count,
            buy_base_volume=crypto_microstructure_1s.buy_base_volume+excluded.buy_base_volume,
            sell_base_volume=crypto_microstructure_1s.sell_base_volume+excluded.sell_base_volume,
            buy_quote_volume=crypto_microstructure_1s.buy_quote_volume+excluded.buy_quote_volume,
            sell_quote_volume=crypto_microstructure_1s.sell_quote_volume+excluded.sell_quote_volume,
            last_trade_price=coalesce(excluded.last_trade_price,crypto_microstructure_1s.last_trade_price),
            bid_price=coalesce(excluded.bid_price,crypto_microstructure_1s.bid_price),
            bid_size=coalesce(excluded.bid_size,crypto_microstructure_1s.bid_size),
            ask_price=coalesce(excluded.ask_price,crypto_microstructure_1s.ask_price),
            ask_size=coalesce(excluded.ask_size,crypto_microstructure_1s.ask_size),
            spread=coalesce(excluded.spread,crypto_microstructure_1s.spread),
            spread_bps=coalesce(excluded.spread_bps,crypto_microstructure_1s.spread_bps),
            mid_price=coalesce(excluded.mid_price,crypto_microstructure_1s.mid_price),
            microprice=coalesce(excluded.microprice,crypto_microstructure_1s.microprice),
            bid_depth=coalesce(excluded.bid_depth,crypto_microstructure_1s.bid_depth),
            ask_depth=coalesce(excluded.ask_depth,crypto_microstructure_1s.ask_depth),
            depth_imbalance=coalesce(excluded.depth_imbalance,crypto_microstructure_1s.depth_imbalance),
            weighted_bid_price=coalesce(excluded.weighted_bid_price,crypto_microstructure_1s.weighted_bid_price),
            weighted_ask_price=coalesce(excluded.weighted_ask_price,crypto_microstructure_1s.weighted_ask_price),
            book_update_count=crypto_microstructure_1s.book_update_count+excluded.book_update_count,
            mark_price=coalesce(excluded.mark_price,crypto_microstructure_1s.mark_price),
            index_price=coalesce(excluded.index_price,crypto_microstructure_1s.index_price),
            funding_rate=coalesce(excluded.funding_rate,crypto_microstructure_1s.funding_rate),
            next_funding_at=coalesce(excluded.next_funding_at,crypto_microstructure_1s.next_funding_at),
            open_interest=coalesce(excluded.open_interest,crypto_microstructure_1s.open_interest),
            open_interest_value=coalesce(excluded.open_interest_value,crypto_microstructure_1s.open_interest_value),
            liquidation_buy_notional=crypto_microstructure_1s.liquidation_buy_notional+excluded.liquidation_buy_notional,
            liquidation_sell_notional=crypto_microstructure_1s.liquidation_sell_notional+excluded.liquidation_sell_notional,
            metadata=crypto_microstructure_1s.metadata || excluded.metadata
    """
    with db_connection() as conn, conn.cursor() as cur:
        for start in range(0, len(rows), 2000):
            batch = rows[start : start + 2000]
            cur.executemany(sql, (_bucket_params(row) for row in batch))
        conn.commit()


async def _raw_upload_one(writer: Any, path: Path, segment: Any) -> bool:
    try:
        await asyncio.to_thread(writer._upload, segment)
    except Exception as exc:
        logger.warning("Raw segment upload will be retried path=%s error=%s", path, exc)
        return False
    writer.pending_uploads.pop(path, None)
    path.unlink(missing_ok=True)
    return True


def _raw_upload_concurrency() -> int:
    """Reserve DB-pool capacity for aggregate flushes and stream control writes."""
    return max(1, min(2, get_settings().db_pool_size - 1))


async def _nonblocking_close_expired(self: Any, now: datetime, force: bool = False) -> int:
    """Close expired raw files immediately and drain uploads without blocking aggregation."""
    expired = [key for key, segment in self.segments.items() if force or segment.end_ts <= now]
    for key in expired:
        segment = self.segments.pop(key)
        try:
            segment.handle.close()
        finally:
            self.pending_uploads[segment.path] = segment

    if not hasattr(self, "_uploading_paths"):
        self._uploading_paths: set[Path] = set()
    if not hasattr(self, "_upload_tasks"):
        self._upload_tasks: set[asyncio.Task[Any]] = set()

    finished = {task for task in self._upload_tasks if task.done()}
    self._upload_tasks.difference_update(finished)

    async def launch(path: Path, segment: Any) -> None:
        try:
            await _raw_upload_one(self, path, segment)
        finally:
            self._uploading_paths.discard(path)

    max_background_uploads = _raw_upload_concurrency()
    if force:
        if self._upload_tasks:
            await asyncio.gather(*list(self._upload_tasks), return_exceptions=True)
            self._upload_tasks.clear()
        uploaded = 0
        while self.pending_uploads:
            batch = list(self.pending_uploads.items())[:max_background_uploads]
            results = await asyncio.gather(
                *(_raw_upload_one(self, path, segment) for path, segment in batch),
                return_exceptions=False,
            )
            uploaded += sum(1 for result in results if result)
            if not any(results):
                break
        return uploaded

    capacity = max(0, max_background_uploads - len(self._uploading_paths))
    if capacity <= 0:
        return 0
    candidates = [
        (path, segment)
        for path, segment in self.pending_uploads.items()
        if path not in self._uploading_paths
    ][:capacity]
    for path, segment in candidates:
        self._uploading_paths.add(path)
        task = asyncio.create_task(launch(path, segment), name=f"raw-upload-{path.name}")
        self._upload_tasks.add(task)
    return 0


def _write_session_heartbeat(session_id: Any, message_count: int, flush_count: int) -> bool:
    """Persist the heartbeat off-loop and fail open under database pressure."""
    try:
        with get_pool().connection(timeout=1.0) as conn, conn.cursor() as cur:
            cur.execute("set local statement_timeout = '2000ms'")
            cur.execute(
                """
                update crypto_stream_sessions
                   set last_heartbeat_at=now(),message_count=%s,flush_count=%s
                 where id=%s
                """,
                (message_count, flush_count, session_id),
            )
            conn.commit()
        return True
    except Exception as exc:
        logger.warning("Crypto stream heartbeat write skipped: %s", exc)
        return False


def _efficient_flush_loop(stream_module: Any):
    async def flush_loop(collector: Any, session_id: Any) -> None:
        heartbeat_interval = max(10.0, float(stream_module.settings.crypto_aggregation_seconds) * 10.0)
        last_heartbeat = 0.0
        while not stream_module.shutdown.is_set():
            try:
                await collector.flush()
                await stream_module.broad_observations.flush()
                now_monotonic = time.monotonic()
                if now_monotonic - last_heartbeat >= heartbeat_interval:
                    written = await asyncio.to_thread(
                        _write_session_heartbeat,
                        session_id,
                        collector.message_count,
                        collector.flush_count,
                    )
                    if written:
                        last_heartbeat = now_monotonic
            except Exception:
                stream_module.logger.exception(
                    "Crypto aggregate flush failed; buffered data will be retried"
                )
            try:
                await asyncio.wait_for(
                    stream_module.shutdown.wait(),
                    timeout=max(1, stream_module.settings.crypto_aggregation_seconds),
                )
            except asyncio.TimeoutError:
                pass

    return flush_loop


def install_crypto_stream_runtime_fixes(stream_module: Any) -> None:
    """Install narrow fixes before the stream event loop creates any tasks."""
    stream_module._health = _health
    stream_module.websockets.connect = _connect_with_longer_open_timeout
    stream_module.poll_binance_open_interest = _stable_binance_open_interest(stream_module)
    stream_module.CryptoCollector._flush_rows = staticmethod(_bulk_flush_rows)
    stream_module.RawSegmentWriter.close_expired = _nonblocking_close_expired
    stream_module.flush_loop = _efficient_flush_loop(stream_module)

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

    stream_module.canonical_by_product = coinbase
    stream_module.canonical_by_symbol = _CanonicalSymbolLookup(
        kraken_and_bybit,
        sorted(bybit_symbols),
    )
    stream_module.stream_bybit = _stable_bybit_stream(stream_module)
