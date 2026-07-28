from __future__ import annotations

import asyncio
import gzip
import json
import logging
import math
import os
import signal
import sqlite3
import socket
import tempfile
import threading
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import httpx
import websockets
from psycopg.types.json import Jsonb

from app.config import get_settings
from app.db import db_connection, fetch_all, fetch_one, get_pool
from app.dynamic_detection import (
    DetectionDecision,
    DetectorConfig,
    MarketObservation,
    MultiVenueDetector,
)
from app.storage import SupabaseStorage


settings = get_settings()
logging.basicConfig(
    level=getattr(logging, settings.log_level.upper(), logging.INFO),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger(__name__)
shutdown = asyncio.Event()


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def floor_bucket(ts: datetime, seconds: int) -> datetime:
    epoch = int(ts.timestamp())
    return datetime.fromtimestamp(epoch - epoch % seconds, tz=timezone.utc)


def parse_ts(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc)
    if isinstance(value, (int, float)):
        scale = 1000 if value > 10_000_000_000 else 1
        return datetime.fromtimestamp(value / scale, tz=timezone.utc)
    text = str(value or "").replace("Z", "+00:00")
    return datetime.fromisoformat(text).astimezone(timezone.utc)


def f(value: Any, default: float | None = None) -> float | None:
    try:
        return float(value) if value not in (None, "") else default
    except (TypeError, ValueError):
        return default


class BookState:
    def __init__(self, depth: int):
        self.depth = depth
        self.bids: dict[float, float] = {}
        self.asks: dict[float, float] = {}

    def snapshot(self, bids: list[Any], asks: list[Any]) -> None:
        self.bids = self._to_levels(bids)
        self.asks = self._to_levels(asks)
        self._trim()

    def update(self, side: str, price: Any, size: Any) -> None:
        p, q = f(price), f(size)
        if p is None or q is None:
            return
        levels = self.bids if side.lower() in {"buy", "bid", "b"} else self.asks
        if q <= 0:
            levels.pop(p, None)
        else:
            levels[p] = q
        self._trim()

    @staticmethod
    def _to_levels(levels: list[Any]) -> dict[float, float]:
        result: dict[float, float] = {}
        for level in levels or []:
            if isinstance(level, dict):
                price = level.get("price") or level.get("price_level")
                size = level.get("qty") or level.get("quantity") or level.get("new_quantity")
            else:
                price = level[0] if len(level) > 0 else None
                size = level[1] if len(level) > 1 else None
            p, q = f(price), f(size)
            if p is not None and q is not None and q > 0:
                result[p] = q
        return result

    def _trim(self) -> None:
        if len(self.bids) > self.depth * 3:
            keep = sorted(self.bids, reverse=True)[: self.depth * 2]
            self.bids = {p: self.bids[p] for p in keep}
        if len(self.asks) > self.depth * 3:
            keep = sorted(self.asks)[: self.depth * 2]
            self.asks = {p: self.asks[p] for p in keep}

    def metrics(self) -> dict[str, float | None]:
        bids = sorted(self.bids.items(), reverse=True)[: self.depth]
        asks = sorted(self.asks.items())[: self.depth]
        if not bids or not asks:
            return {}
        bid_price, bid_size = bids[0]
        ask_price, ask_size = asks[0]
        mid = (bid_price + ask_price) / 2
        spread = ask_price - bid_price
        bid_depth = sum(size for _, size in bids)
        ask_depth = sum(size for _, size in asks)
        total = bid_depth + ask_depth
        microprice = (
            (ask_price * bid_size + bid_price * ask_size) / (bid_size + ask_size)
            if bid_size + ask_size
            else mid
        )
        weighted_bid = sum(price * size for price, size in bids) / bid_depth if bid_depth else None
        weighted_ask = sum(price * size for price, size in asks) / ask_depth if ask_depth else None
        return {
            "bid_price": bid_price,
            "bid_size": bid_size,
            "ask_price": ask_price,
            "ask_size": ask_size,
            "spread": spread,
            "spread_bps": spread / mid * 10000 if mid else None,
            "mid_price": mid,
            "microprice": microprice,
            "bid_depth": bid_depth,
            "ask_depth": ask_depth,
            "depth_imbalance": (bid_depth - ask_depth) / total if total else None,
            "weighted_bid_price": weighted_bid,
            "weighted_ask_price": weighted_ask,
        }


@dataclass
class Bucket:
    provider: str
    market_type: str
    venue_symbol: str
    canonical_symbol: str
    ts: datetime
    trade_count: int = 0
    buy_count: int = 0
    sell_count: int = 0
    buy_base_volume: float = 0.0
    sell_base_volume: float = 0.0
    buy_quote_volume: float = 0.0
    sell_quote_volume: float = 0.0
    last_trade_price: float | None = None
    book_update_count: int = 0
    book: dict[str, Any] = field(default_factory=dict)
    derivative: dict[str, Any] = field(default_factory=dict)
    liquidation_buy_notional: float = 0.0
    liquidation_sell_notional: float = 0.0
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class RawSegment:
    provider: str
    market_type: str
    venue_symbol: str
    canonical_symbol: str
    channel: str
    start_ts: datetime
    end_ts: datetime
    path: Path
    handle: Any
    message_count: int = 0


class RawSegmentWriter:
    def __init__(self):
        self.storage = SupabaseStorage(settings.raw_bucket)
        self.root = Path(tempfile.gettempdir()) / "market-data-raw-v3"
        self.root.mkdir(parents=True, exist_ok=True)
        self.segments: dict[tuple[str, str, str, str, datetime], RawSegment] = {}
        self.pending_uploads: dict[Path, RawSegment] = {}

    def _segment_start(self, ts: datetime) -> datetime:
        minutes = settings.crypto_raw_segment_minutes
        return ts.replace(minute=(ts.minute // minutes) * minutes, second=0, microsecond=0)

    def write(
        self,
        provider: str,
        market_type: str,
        venue_symbol: str,
        canonical_symbol: str,
        channel: str,
        ts: datetime,
        payload: Any,
    ) -> None:
        if not settings.crypto_raw_capture_enabled:
            return
        start = self._segment_start(ts)
        key = (provider, market_type, venue_symbol, channel, start)
        segment = self.segments.get(key)
        if segment is None:
            safe = venue_symbol.replace("/", "_").replace("-", "_")
            directory = self.root / provider / market_type / safe / channel
            directory.mkdir(parents=True, exist_ok=True)
            path = directory / f"{start:%Y%m%dT%H%M%SZ}.jsonl.gz"
            handle = gzip.open(path, "at", encoding="utf-8")
            segment = RawSegment(
                provider=provider,
                market_type=market_type,
                venue_symbol=venue_symbol,
                canonical_symbol=canonical_symbol,
                channel=channel,
                start_ts=start,
                end_ts=start + timedelta(minutes=settings.crypto_raw_segment_minutes),
                path=path,
                handle=handle,
            )
            self.segments[key] = segment
        segment.handle.write(json.dumps(payload, separators=(",", ":"), default=str) + "\n")
        segment.message_count += 1

    async def close_expired(self, now: datetime, force: bool = False) -> int:
        expired = [key for key, segment in self.segments.items() if force or segment.end_ts <= now]
        for key in expired:
            segment = self.segments.pop(key)
            segment.handle.close()
            self.pending_uploads[segment.path] = segment

        uploaded = 0
        # Failed uploads remain on local disk and are retried on every flush. The
        # object path is deterministic, so an upload that succeeded before a DB
        # failure can safely be repeated.
        for path, segment in list(self.pending_uploads.items()):
            try:
                await asyncio.to_thread(self._upload, segment)
            except Exception as exc:
                logger.warning("Raw segment upload will be retried path=%s error=%s", path, exc)
                continue
            self.pending_uploads.pop(path, None)
            path.unlink(missing_ok=True)
            uploaded += 1
        return uploaded

    def _upload(self, segment: RawSegment) -> None:
        object_path = (
            f"crypto/{segment.provider}/{segment.market_type}/{segment.canonical_symbol}/"
            f"{segment.venue_symbol.replace('/', '_')}/{segment.channel}/"
            f"{segment.start_ts:%Y/%m/%d/%H%M}.jsonl.gz"
        )
        size, checksum = self.storage.upload_file(segment.path, object_path, "application/gzip")
        with db_connection() as conn, conn.cursor() as cur:
            cur.execute(
                """
                insert into crypto_raw_objects(
                    provider,market_type,venue_symbol,canonical_symbol,channel,start_ts,end_ts,
                    object_path,content_type,compression,message_count,size_bytes,checksum,status
                ) values (%s,%s,%s,%s,%s,%s,%s,%s,'application/gzip','gzip',%s,%s,%s,'uploaded')
                on conflict(object_path) do update set
                    message_count=excluded.message_count,size_bytes=excluded.size_bytes,
                    checksum=excluded.checksum,status='uploaded'
                """,
                (
                    segment.provider, segment.market_type, segment.venue_symbol,
                    segment.canonical_symbol, segment.channel, segment.start_ts, segment.end_ts,
                    object_path, segment.message_count, size, checksum,
                ),
            )
            conn.commit()


class CryptoCollector:
    def __init__(self):
        self.buckets: dict[tuple[str, str, str, datetime], Bucket] = {}
        self.books: dict[tuple[str, str, str], BookState] = {}
        self.lock = asyncio.Lock()
        self.raw = RawSegmentWriter()
        self.flush_count = 0
        self.message_count = 0

    def book_state(self, provider: str, market_type: str, symbol: str) -> BookState:
        return self.books.setdefault((provider, market_type, symbol), BookState(settings.crypto_order_book_depth))

    def _bucket(self, provider: str, market_type: str, symbol: str, canonical: str, ts: datetime) -> Bucket:
        bucket_ts = floor_bucket(ts, settings.crypto_aggregation_seconds)
        key = (provider, market_type, symbol, bucket_ts)
        return self.buckets.setdefault(key, Bucket(provider, market_type, symbol, canonical, bucket_ts))

    async def trade(
        self, provider: str, market_type: str, symbol: str, canonical: str,
        ts: datetime, price: float, size: float, side: str, payload: Any, raw: bool,
    ) -> None:
        async with self.lock:
            bucket = self._bucket(provider, market_type, symbol, canonical, ts)
            quote = price * size
            bucket.trade_count += 1
            bucket.last_trade_price = price
            if side.lower() == "buy":
                bucket.buy_count += 1
                bucket.buy_base_volume += size
                bucket.buy_quote_volume += quote
            else:
                bucket.sell_count += 1
                bucket.sell_base_volume += size
                bucket.sell_quote_volume += quote
            self.message_count += 1
            if raw:
                self.raw.write(provider, market_type, symbol, canonical, "trades", ts, payload)

    async def book(
        self, provider: str, market_type: str, symbol: str, canonical: str,
        ts: datetime, payload: Any, raw: bool,
    ) -> None:
        state = self.book_state(provider, market_type, symbol)
        async with self.lock:
            bucket = self._bucket(provider, market_type, symbol, canonical, ts)
            bucket.book = state.metrics()
            bucket.book_update_count += 1
            self.message_count += 1
            if raw:
                self.raw.write(provider, market_type, symbol, canonical, "order_book", ts, payload)

    async def derivative(
        self, provider: str, market_type: str, symbol: str, canonical: str,
        ts: datetime, fields: dict[str, Any], payload: Any, raw: bool,
    ) -> None:
        async with self.lock:
            bucket = self._bucket(provider, market_type, symbol, canonical, ts)
            bucket.derivative.update({k: v for k, v in fields.items() if v is not None})
            self.message_count += 1
            if raw:
                self.raw.write(provider, market_type, symbol, canonical, "derivatives", ts, payload)

    async def liquidation(
        self, provider: str, market_type: str, symbol: str, canonical: str,
        ts: datetime, event_id: str, side: str, price: float, size: float, payload: Any, raw: bool,
    ) -> None:
        notional = price * size
        async with self.lock:
            bucket = self._bucket(provider, market_type, symbol, canonical, ts)
            if side.lower() == "buy":
                bucket.liquidation_buy_notional += notional
            else:
                bucket.liquidation_sell_notional += notional
            if raw:
                self.raw.write(provider, market_type, symbol, canonical, "liquidations", ts, payload)
        await asyncio.to_thread(
            self._save_liquidation, provider, symbol, canonical, event_id, ts, side, price, size, notional, payload
        )

    @staticmethod
    def _save_liquidation(provider, symbol, canonical, event_id, ts, side, price, size, notional, payload) -> None:
        with db_connection() as conn, conn.cursor() as cur:
            cur.execute(
                """
                insert into crypto_liquidations(
                    provider,venue_symbol,canonical_symbol,event_id,ts,side,price,quantity,notional,metadata
                ) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                on conflict do nothing
                """,
                (provider, symbol, canonical, event_id, ts, side, price, size, notional, Jsonb(payload)),
            )
            conn.commit()

    async def flush(self, force: bool = False) -> int:
        cutoff = floor_bucket(utc_now(), settings.crypto_aggregation_seconds)
        async with self.lock:
            keys = [key for key, bucket in self.buckets.items() if force or bucket.ts < cutoff]
            rows = [self.buckets.pop(key) for key in keys]
        if rows:
            try:
                await asyncio.to_thread(self._flush_rows, rows)
            except Exception:
                # Never claim that popped rows are buffered unless they really are.
                # Merge them back so the next heartbeat retries the same seconds.
                async with self.lock:
                    for row in rows:
                        key = (row.provider, row.market_type, row.venue_symbol, row.ts)
                        existing = self.buckets.get(key)
                        if existing is None:
                            self.buckets[key] = row
                        else:
                            existing.trade_count += row.trade_count
                            existing.buy_count += row.buy_count
                            existing.sell_count += row.sell_count
                            existing.buy_base_volume += row.buy_base_volume
                            existing.sell_base_volume += row.sell_base_volume
                            existing.buy_quote_volume += row.buy_quote_volume
                            existing.sell_quote_volume += row.sell_quote_volume
                            existing.last_trade_price = existing.last_trade_price or row.last_trade_price
                            existing.book_update_count += row.book_update_count
                            if not existing.book:
                                existing.book = row.book
                            existing.derivative = {**row.derivative, **existing.derivative}
                            existing.liquidation_buy_notional += row.liquidation_buy_notional
                            existing.liquidation_sell_notional += row.liquidation_sell_notional
                            existing.metadata = {**row.metadata, **existing.metadata}
                raise
            self.flush_count += 1
        await self.raw.close_expired(utc_now(), force=force)
        return len(rows)

    @staticmethod
    def _flush_rows(rows: list[Bucket]) -> None:
        with db_connection() as conn, conn.cursor() as cur:
            for row in rows:
                book, der = row.book, row.derivative
                cur.execute(
                    """
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
                    """,
                    (
                        row.provider, row.market_type, row.venue_symbol, row.canonical_symbol, row.ts,
                        row.trade_count, row.buy_count, row.sell_count, row.buy_base_volume, row.sell_base_volume,
                        row.buy_quote_volume, row.sell_quote_volume, row.last_trade_price,
                        book.get("bid_price"), book.get("bid_size"), book.get("ask_price"), book.get("ask_size"),
                        book.get("spread"), book.get("spread_bps"), book.get("mid_price"), book.get("microprice"),
                        book.get("bid_depth"), book.get("ask_depth"), book.get("depth_imbalance"),
                        book.get("weighted_bid_price"), book.get("weighted_ask_price"), row.book_update_count,
                        der.get("mark_price"), der.get("index_price"), der.get("funding_rate"),
                        der.get("next_funding_at"), der.get("open_interest"), der.get("open_interest_value"),
                        row.liquidation_buy_notional, row.liquidation_sell_notional, Jsonb(row.metadata),
                    ),
                )
            conn.commit()


def _health(provider: str, service: str, *, status: str, error: str | None = None, messages: int = 0) -> None:
    with db_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            insert into provider_health(
                provider,service,status,last_message_at,last_success_at,last_error_at,
                message_count,reconnect_count,last_error,updated_at
            ) values (%s,%s,%s,case when %s>0 then now() end,
                     case when %s='connected' then now() end,
                     case when %s is not null then now() end,%s,0,%s,now())
            on conflict(provider,service) do update set
                status=excluded.status,
                last_message_at=case when %s>0 then now() else provider_health.last_message_at end,
                last_success_at=case when %s='connected' then now() else provider_health.last_success_at end,
                last_error_at=case when %s is not null then now() else provider_health.last_error_at end,
                message_count=provider_health.message_count+%s,
                reconnect_count=provider_health.reconnect_count+
                    case when excluded.status='reconnecting' then 1 else 0 end,
                last_error=coalesce(%s,provider_health.last_error),updated_at=now()
            """,
            (
                provider, service, status, messages, status, error, messages, error,
                messages, status, error, messages, error,
            ),
        )
        conn.commit()


def load_targets() -> tuple[set[str], dict[str, list[dict[str, Any]]], set[str]]:
    now = utc_now()
    active_rows = fetch_all(
        """
        select canonical_symbol
          from crypto_capture_targets
         where expires_at > now()
         order by priority_score desc nulls last, last_observed_at desc nulls last, updated_at desc
         limit %s
        """,
        (settings.crypto_stream_max_dynamic_targets,),
    )
    active = {row["canonical_symbol"].upper() for row in active_rows}
    targets = set(settings.crypto_stream_core_symbols) | active
    mappings: dict[str, list[dict[str, Any]]] = defaultdict(list)
    if targets:
        rows = fetch_all(
            """
            select distinct on (provider,canonical_symbol)
                   provider,market_type,venue_symbol,canonical_symbol,quote_asset,priority
              from crypto_venue_symbols
             where canonical_symbol = any(%s) and tradable=true
             order by provider,canonical_symbol,priority desc
            """,
            (list(targets),),
        )
        for row in rows:
            mappings[row["provider"]].append(row)
    return targets, mappings, active


def is_raw(canonical: str, active: set[str]) -> bool:
    return settings.crypto_raw_capture_enabled and (
        canonical in active or settings.crypto_raw_core_enabled
    )


async def stream_binance_spot(collector: CryptoCollector, mappings: list[dict[str, Any]], active: set[str]) -> None:
    if not mappings:
        return
    by_symbol = {row["venue_symbol"].lower(): row["canonical_symbol"] for row in mappings}
    streams = []
    for symbol in by_symbol:
        streams.extend([f"{symbol}@aggTrade", f"{symbol}@depth20@100ms"])
    url = "wss://stream.binance.com:9443/stream?streams=" + "/".join(streams)
    while not shutdown.is_set():
        try:
            _health("binance_spot", "websocket", status="connecting")
            async with websockets.connect(url, ping_interval=20, ping_timeout=20, max_size=16 * 1024 * 1024) as ws:
                _health("binance_spot", "websocket", status="connected")
                async for raw_message in ws:
                    message = json.loads(raw_message)
                    data = message.get("data") or {}
                    stream = message.get("stream") or ""
                    symbol = str(data.get("s") or stream.split("@")[0]).lower()
                    canonical = by_symbol.get(symbol)
                    if not canonical:
                        continue
                    ts = parse_ts(data.get("E") or data.get("T") or time.time())
                    raw_enabled = is_raw(canonical, active)
                    if data.get("e") == "aggTrade":
                        price, size = f(data.get("p")), f(data.get("q"))
                        if price is not None and size is not None:
                            await collector.trade(
                                "binance_spot", "spot", symbol.upper(), canonical, ts,
                                price, size, "sell" if data.get("m") else "buy", data, raw_enabled,
                            )
                    elif "bids" in data or "asks" in data:
                        state = collector.book_state("binance_spot", "spot", symbol.upper())
                        state.snapshot(data.get("bids") or [], data.get("asks") or [])
                        await collector.book("binance_spot", "spot", symbol.upper(), canonical, ts, data, raw_enabled)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            _health("binance_spot", "websocket", status="reconnecting", error=str(exc))
            logger.warning("Binance spot stream reconnecting: %s", exc)
            await asyncio.sleep(5)


async def stream_binance_futures(collector: CryptoCollector, mappings: list[dict[str, Any]], active: set[str]) -> None:
    if not mappings:
        return
    by_symbol = {row["venue_symbol"].lower(): row["canonical_symbol"] for row in mappings}
    streams = []
    for symbol in by_symbol:
        streams.extend([
            f"{symbol}@aggTrade", f"{symbol}@depth20@500ms",
            f"{symbol}@markPrice@1s", f"{symbol}@forceOrder",
        ])
    url = "wss://fstream.binance.com/stream?streams=" + "/".join(streams)
    while not shutdown.is_set():
        try:
            _health("binance_futures", "websocket", status="connecting")
            async with websockets.connect(url, ping_interval=20, ping_timeout=20, max_size=16 * 1024 * 1024) as ws:
                _health("binance_futures", "websocket", status="connected")
                async for raw_message in ws:
                    message = json.loads(raw_message)
                    data = message.get("data") or {}
                    event = data.get("e")
                    order = data.get("o") or {}
                    stream_name = str(message.get("stream") or "")
                    symbol = str(data.get("s") or order.get("s") or stream_name.split("@")[0]).lower()
                    canonical = by_symbol.get(symbol)
                    if not canonical:
                        continue
                    ts = parse_ts(data.get("E") or data.get("T") or order.get("T") or time.time())
                    raw_enabled = is_raw(canonical, active)
                    if event == "aggTrade":
                        price, size = f(data.get("p")), f(data.get("q"))
                        if price is not None and size is not None:
                            await collector.trade(
                                "binance_futures", "perpetual", symbol.upper(), canonical, ts,
                                price, size, "sell" if data.get("m") else "buy", data, raw_enabled,
                            )
                    elif event == "depthUpdate" or "bids" in data:
                        state = collector.book_state("binance_futures", "perpetual", symbol.upper())
                        if data.get("bids") or data.get("asks"):
                            state.snapshot(data.get("bids") or [], data.get("asks") or [])
                        else:
                            for price, size in data.get("b") or []:
                                state.update("buy", price, size)
                            for price, size in data.get("a") or []:
                                state.update("sell", price, size)
                        await collector.book("binance_futures", "perpetual", symbol.upper(), canonical, ts, data, raw_enabled)
                    elif event == "markPriceUpdate":
                        await collector.derivative(
                            "binance_futures", "perpetual", symbol.upper(), canonical, ts,
                            {
                                "mark_price": f(data.get("p")),
                                "index_price": f(data.get("i")),
                                "funding_rate": f(data.get("r")),
                                "next_funding_at": parse_ts(data["T"]) if data.get("T") else None,
                            },
                            data, raw_enabled,
                        )
                    elif event == "forceOrder":
                        price, size = f(order.get("ap") or order.get("p")), f(order.get("q"))
                        if price is not None and size is not None:
                            await collector.liquidation(
                                "binance_futures", "perpetual", symbol.upper(), canonical, ts,
                                str(order.get("T") or data.get("E")), str(order.get("S") or "").lower(),
                                price, size, data, raw_enabled,
                            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            _health("binance_futures", "websocket", status="reconnecting", error=str(exc))
            logger.warning("Binance futures stream reconnecting: %s", exc)
            await asyncio.sleep(5)


async def poll_binance_open_interest(collector: CryptoCollector, mappings: list[dict[str, Any]], active: set[str]) -> None:
    import httpx
    async with httpx.AsyncClient(timeout=20) as client:
        while not shutdown.is_set():
            for row in mappings:
                try:
                    symbol = row["venue_symbol"]
                    response = await client.get("https://fapi.binance.com/fapi/v1/openInterest", params={"symbol": symbol})
                    response.raise_for_status()
                    payload = response.json()
                    oi = f(payload.get("openInterest"))
                    canonical = row["canonical_symbol"]
                    await collector.derivative(
                        "binance_futures", "perpetual", symbol, canonical, utc_now(),
                        {"open_interest": oi}, payload, is_raw(canonical, active),
                    )
                except Exception as exc:
                    logger.debug("Open-interest poll failed %s: %s", row.get("venue_symbol"), exc)
            try:
                await asyncio.wait_for(shutdown.wait(), timeout=300)
            except asyncio.TimeoutError:
                pass


async def stream_coinbase(collector: CryptoCollector, mappings: list[dict[str, Any]], active: set[str]) -> None:
    if not mappings:
        return
    products = [row["venue_symbol"] for row in mappings]
    mapping_by_product = {row["venue_symbol"]: row for row in mappings}
    url = "wss://ws-feed.exchange.coinbase.com"
    subscribe = {"type": "subscribe", "product_ids": products, "channels": ["matches", "level2_batch"]}
    while not shutdown.is_set():
        try:
            _health("coinbase", "websocket", status="connecting")
            async with websockets.connect(url, ping_interval=20, ping_timeout=20, max_size=16 * 1024 * 1024) as ws:
                await ws.send(json.dumps(subscribe))
                _health("coinbase", "websocket", status="connected")
                async for raw_message in ws:
                    data = json.loads(raw_message)
                    product = data.get("product_id")
                    canonical = canonical_by_product.get(product)
                    if not canonical:
                        continue
                    ts = parse_ts(data.get("time") or time.time())
                    raw_enabled = is_raw(canonical, active)
                    msg_type = data.get("type")
                    if msg_type in {"match", "last_match"}:
                        price, size = f(data.get("price")), f(data.get("size"))
                        if price is not None and size is not None:
                            taker_side = "buy" if data.get("side") == "sell" else "sell"
                            await collector.trade("coinbase", "spot", product, canonical, ts, price, size, taker_side, data, raw_enabled)
                    elif msg_type == "snapshot":
                        state = collector.book_state("coinbase", "spot", product)
                        state.snapshot(data.get("bids") or [], data.get("asks") or [])
                        await collector.book("coinbase", "spot", product, canonical, ts, data, raw_enabled)
                    elif msg_type == "l2update":
                        state = collector.book_state("coinbase", "spot", product)
                        for side, price, size in data.get("changes") or []:
                            state.update(side, price, size)
                        await collector.book("coinbase", "spot", product, canonical, ts, data, raw_enabled)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            _health("coinbase", "websocket", status="reconnecting", error=str(exc))
            logger.warning("Coinbase stream reconnecting: %s", exc)
            await asyncio.sleep(5)


async def stream_kraken(collector: CryptoCollector, mappings: list[dict[str, Any]], active: set[str]) -> None:
    if not mappings:
        return
    symbols = [row["venue_symbol"] for row in mappings]
    mapping_by_symbol = {row["venue_symbol"]: row for row in mappings}
    url = "wss://ws.kraken.com/v2"
    while not shutdown.is_set():
        try:
            _health("kraken", "websocket", status="connecting")
            async with websockets.connect(url, ping_interval=20, ping_timeout=20, max_size=16 * 1024 * 1024) as ws:
                await ws.send(json.dumps({"method": "subscribe", "params": {"channel": "book", "symbol": symbols, "depth": min(settings.crypto_order_book_depth, 100), "snapshot": True}}))
                await ws.send(json.dumps({"method": "subscribe", "params": {"channel": "trade", "symbol": symbols, "snapshot": False}}))
                _health("kraken", "websocket", status="connected")
                async for raw_message in ws:
                    message = json.loads(raw_message)
                    channel = message.get("channel")
                    for data in message.get("data") or []:
                        symbol = data.get("symbol")
                        canonical = canonical_by_symbol.get(symbol)
                        if not canonical:
                            continue
                        raw_enabled = is_raw(canonical, active)
                        if channel == "book":
                            ts = parse_ts(data.get("timestamp") or time.time())
                            state = collector.book_state("kraken", "spot", symbol)
                            if message.get("type") == "snapshot":
                                state.snapshot(data.get("bids") or [], data.get("asks") or [])
                            else:
                                for level in data.get("bids") or []:
                                    state.update("buy", level.get("price"), level.get("qty"))
                                for level in data.get("asks") or []:
                                    state.update("sell", level.get("price"), level.get("qty"))
                            await collector.book("kraken", "spot", symbol, canonical, ts, data, raw_enabled)
                        elif channel == "trade":
                            ts = parse_ts(data.get("timestamp") or time.time())
                            price, size = f(data.get("price")), f(data.get("qty"))
                            if price is not None and size is not None:
                                await collector.trade(
                                    "kraken", "spot", symbol, canonical, ts, price, size,
                                    str(data.get("side") or "sell"), data, raw_enabled,
                                )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            _health("kraken", "websocket", status="reconnecting", error=str(exc))
            logger.warning("Kraken stream reconnecting: %s", exc)
            await asyncio.sleep(5)


async def stream_bybit(collector: CryptoCollector, mappings: list[dict[str, Any]], active: set[str]) -> None:
    if not mappings:
        return
    mapping_by_symbol = {row["venue_symbol"]: row for row in mappings}
    args = []
    for symbol in canonical_by_symbol:
        args.extend([f"orderbook.50.{symbol}", f"publicTrade.{symbol}", f"tickers.{symbol}", f"allLiquidation.{symbol}"])
    url = "wss://stream.bybit.com/v5/public/linear"
    while not shutdown.is_set():
        try:
            _health("bybit", "websocket", status="connecting")
            async with websockets.connect(url, ping_interval=20, ping_timeout=20, max_size=16 * 1024 * 1024) as ws:
                await ws.send(json.dumps({"op": "subscribe", "args": args}))
                _health("bybit", "websocket", status="connected")
                async for raw_message in ws:
                    message = json.loads(raw_message)
                    topic = str(message.get("topic") or "")
                    data = message.get("data")
                    symbol = topic.split(".")[-1] if "." in topic else ""
                    canonical = canonical_by_symbol.get(symbol)
                    if not canonical:
                        continue
                    raw_enabled = is_raw(canonical, active)
                    ts = parse_ts(message.get("ts") or time.time())
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
                        await collector.book("bybit", "perpetual", symbol, canonical, ts, message, raw_enabled)
                    elif topic.startswith("publicTrade"):
                        for trade in data or []:
                            price, size = f(trade.get("p")), f(trade.get("v"))
                            if price is not None and size is not None:
                                await collector.trade(
                                    "bybit", "perpetual", symbol, canonical,
                                    parse_ts(trade.get("T") or message.get("ts")),
                                    price, size, str(trade.get("S") or "Sell").lower(),
                                    trade, raw_enabled,
                                )
                    elif topic.startswith("tickers"):
                        ticker = data or {}
                        await collector.derivative(
                            "bybit", "perpetual", symbol, canonical, ts,
                            {
                                "mark_price": f(ticker.get("markPrice")),
                                "index_price": f(ticker.get("indexPrice")),
                                "funding_rate": f(ticker.get("fundingRate")),
                                "open_interest": f(ticker.get("openInterest")),
                                "open_interest_value": f(ticker.get("openInterestValue")),
                            },
                            message, raw_enabled,
                        )
                    elif topic.startswith("allLiquidation"):
                        liquidation_items = data if isinstance(data, list) else ([data] if isinstance(data, dict) else [])
                        for item in liquidation_items:
                            price, size = f(item.get("p")), f(item.get("v"))
                            if price is not None and size is not None:
                                await collector.liquidation(
                                    "bybit", "perpetual", symbol, canonical,
                                    parse_ts(item.get("T") or message.get("ts")),
                                    str(item.get("T") or uuid4()), str(item.get("S") or "Sell").lower(),
                                    price, size, item, raw_enabled,
                                )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            _health("bybit", "websocket", status="reconnecting", error=str(exc))
            logger.warning("Bybit stream reconnecting: %s", exc)
            await asyncio.sleep(5)


class BroadObservationStore:
    """Permanent one-minute all-pair facts plus a bounded higher-frequency ring buffer."""

    def __init__(self):
        self.pending_permanent: dict[tuple[str, str, str, datetime], dict[str, Any]] = {}
        self.pending_buffer: dict[tuple[str, str, str, datetime], dict[str, Any]] = {}
        self.lock = asyncio.Lock()
        self.storage = SupabaseStorage(settings.raw_bucket)
        self.root = Path(tempfile.gettempdir()) / "market-data-broad-v33"
        self.root.mkdir(parents=True, exist_ok=True)
        self.ring_path = self.root / "broad_ring.sqlite3"
        self.last_prune_at = datetime.min.replace(tzinfo=timezone.utc)
        self.pending_preservations: dict[Path, tuple[DetectionDecision, datetime, datetime, int]] = {}
        self.preserve_lock = threading.Lock()
        self._init_ring()

    @property
    def pending(self) -> dict[tuple[str, str, str, datetime], dict[str, Any]]:
        """Compatibility/readiness view used by tests and operational introspection."""
        return {**self.pending_permanent, **self.pending_buffer}

    def _connect_ring(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.ring_path, timeout=30)
        connection.execute("pragma journal_mode=WAL")
        connection.execute("pragma synchronous=NORMAL")
        return connection

    def _init_ring(self) -> None:
        with self._connect_ring() as conn:
            conn.execute(
                """
                create table if not exists broad_buffer(
                    provider text not null,
                    market_type text not null,
                    venue_symbol text not null,
                    canonical_symbol text not null,
                    quote_asset text,
                    ts integer not null,
                    last_price real not null,
                    quote_volume_24h real,
                    bid_price real,
                    ask_price real,
                    metadata text not null default '{}',
                    primary key(provider,market_type,venue_symbol,ts)
                )
                """
            )
            conn.execute(
                "create index if not exists broad_buffer_symbol_ts_idx on broad_buffer(canonical_symbol,ts)"
            )
            conn.commit()

    @staticmethod
    def _row(
        *, provider: str, market_type: str, venue_symbol: str, canonical_symbol: str,
        quote_asset: str | None, ts: datetime, price: float, quote_volume_24h: float | None,
        bid_price: float | None, ask_price: float | None, metadata: dict[str, Any] | None,
    ) -> dict[str, Any]:
        return {
            "provider": provider,
            "market_type": market_type,
            "venue_symbol": venue_symbol,
            "canonical_symbol": canonical_symbol,
            "quote_asset": quote_asset,
            "ts": ts,
            "last_price": price,
            "quote_volume_24h": quote_volume_24h,
            "bid_price": bid_price,
            "ask_price": ask_price,
            "metadata": metadata or {},
        }

    async def record(
        self, *, provider: str, market_type: str, venue_symbol: str, canonical_symbol: str,
        quote_asset: str | None, ts: datetime, price: float, quote_volume_24h: float | None,
        bid_price: float | None = None, ask_price: float | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        if not settings.crypto_broad_observation_enabled:
            return
        permanent_ts = floor_bucket(ts, settings.crypto_broad_observation_seconds)
        buffer_ts = floor_bucket(ts, settings.crypto_broad_buffer_seconds)
        permanent = self._row(
            provider=provider,market_type=market_type,venue_symbol=venue_symbol,
            canonical_symbol=canonical_symbol,quote_asset=quote_asset,ts=permanent_ts,
            price=price,quote_volume_24h=quote_volume_24h,bid_price=bid_price,
            ask_price=ask_price,metadata=metadata,
        )
        buffered = {**permanent, "ts": buffer_ts}
        async with self.lock:
            self.pending_permanent[(provider, market_type, venue_symbol, permanent_ts)] = permanent
            self.pending_buffer[(provider, market_type, venue_symbol, buffer_ts)] = buffered

    async def flush(self, force: bool = False) -> int:
        if not settings.crypto_broad_observation_enabled:
            return 0
        now = utc_now()
        permanent_cutoff = datetime.max.replace(tzinfo=timezone.utc) if force else floor_bucket(
            now, settings.crypto_broad_observation_seconds
        )
        buffer_cutoff = datetime.max.replace(tzinfo=timezone.utc) if force else floor_bucket(
            now, settings.crypto_broad_buffer_seconds
        )
        async with self.lock:
            permanent_keys = [key for key, row in self.pending_permanent.items() if row["ts"] < permanent_cutoff]
            buffer_keys = [key for key, row in self.pending_buffer.items() if row["ts"] < buffer_cutoff]
            permanent_rows = [self.pending_permanent.pop(key) for key in permanent_keys]
            buffer_rows = [self.pending_buffer.pop(key) for key in buffer_keys]

        if permanent_rows:
            try:
                await asyncio.to_thread(self._flush_rows, permanent_rows)
            except Exception:
                async with self.lock:
                    for row in permanent_rows:
                        key = (row["provider"], row["market_type"], row["venue_symbol"], row["ts"])
                        self.pending_permanent[key] = row
                    for row in buffer_rows:
                        key = (row["provider"], row["market_type"], row["venue_symbol"], row["ts"])
                        self.pending_buffer[key] = row
                raise

        if buffer_rows:
            prune_before = None
            if force or now - self.last_prune_at >= timedelta(minutes=5):
                prune_before = now - timedelta(minutes=settings.crypto_broad_buffer_minutes)
                self.last_prune_at = now
            try:
                await asyncio.to_thread(self._write_ring, buffer_rows, prune_before)
            except Exception:
                async with self.lock:
                    for row in buffer_rows:
                        key = (row["provider"], row["market_type"], row["venue_symbol"], row["ts"])
                        self.pending_buffer[key] = row
                raise
        await asyncio.to_thread(self._retry_preservations_sync)
        return len(permanent_rows) + len(buffer_rows)

    @staticmethod
    def _flush_rows(rows: list[dict[str, Any]]) -> None:
        with db_connection() as conn, conn.cursor() as cur:
            cur.executemany(
                """
                insert into crypto_market_observations_1m(
                    provider,market_type,venue_symbol,canonical_symbol,quote_asset,ts,last_price,
                    quote_volume_24h,bid_price,ask_price,metadata
                ) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                on conflict(provider,market_type,venue_symbol,ts) do update set
                    canonical_symbol=excluded.canonical_symbol,quote_asset=excluded.quote_asset,
                    last_price=excluded.last_price,quote_volume_24h=excluded.quote_volume_24h,
                    bid_price=coalesce(excluded.bid_price,crypto_market_observations_1m.bid_price),
                    ask_price=coalesce(excluded.ask_price,crypto_market_observations_1m.ask_price),
                    metadata=crypto_market_observations_1m.metadata || excluded.metadata
                """,
                [
                    (
                        row["provider"],row["market_type"],row["venue_symbol"],row["canonical_symbol"],
                        row["quote_asset"],row["ts"],row["last_price"],row["quote_volume_24h"],
                        row["bid_price"],row["ask_price"],Jsonb(row["metadata"]),
                    )
                    for row in rows
                ],
            )
            conn.commit()

    def _write_ring(self, rows: list[dict[str, Any]], prune_before: datetime | None) -> None:
        with self._connect_ring() as conn:
            conn.executemany(
                """
                insert into broad_buffer(
                    provider,market_type,venue_symbol,canonical_symbol,quote_asset,ts,last_price,
                    quote_volume_24h,bid_price,ask_price,metadata
                ) values (?,?,?,?,?,?,?,?,?,?,?)
                on conflict(provider,market_type,venue_symbol,ts) do update set
                    canonical_symbol=excluded.canonical_symbol,quote_asset=excluded.quote_asset,
                    last_price=excluded.last_price,quote_volume_24h=excluded.quote_volume_24h,
                    bid_price=coalesce(excluded.bid_price,broad_buffer.bid_price),
                    ask_price=coalesce(excluded.ask_price,broad_buffer.ask_price),metadata=excluded.metadata
                """,
                [
                    (
                        row["provider"],row["market_type"],row["venue_symbol"],row["canonical_symbol"],
                        row["quote_asset"],int(row["ts"].timestamp()),row["last_price"],
                        row["quote_volume_24h"],row["bid_price"],row["ask_price"],
                        json.dumps(row["metadata"],separators=(",", ":"),default=str),
                    )
                    for row in rows
                ],
            )
            if prune_before is not None:
                conn.execute("delete from broad_buffer where ts < ?", (int(prune_before.timestamp()),))
            conn.commit()

    async def preserve(self, decision: DetectionDecision) -> None:
        if not settings.crypto_preserve_pretrigger_buffer:
            return
        await self.flush(force=True)
        try:
            await asyncio.to_thread(self._preserve_sync, decision)
        except Exception as exc:
            # Preservation is valuable, but it must never interrupt broad market collection.
            # The compressed local file remains queued and is retried by the flush loop.
            logger.warning(
                "Pre-trigger buffer upload queued for retry symbol=%s detected_at=%s error=%s",
                decision.canonical_symbol, decision.detected_at.isoformat(), exc,
            )

    def _preserve_sync(self, decision: DetectionDecision) -> None:
        start = decision.detected_at - timedelta(minutes=settings.crypto_broad_buffer_minutes)
        with self._connect_ring() as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                """
                select provider,market_type,venue_symbol,canonical_symbol,quote_asset,ts,last_price,
                       quote_volume_24h,bid_price,ask_price,metadata
                  from broad_buffer
                 where canonical_symbol=? and ts >= ? and ts <= ?
                 order by ts,provider,market_type,venue_symbol
                """,
                (
                    decision.canonical_symbol,
                    int(start.timestamp()),
                    int(decision.detected_at.timestamp()),
                ),
            ).fetchall()
        if not rows:
            return
        safe_ts = decision.detected_at.strftime("%Y%m%dT%H%M%SZ")
        path = self.root / f"{decision.canonical_symbol}_{safe_ts}.jsonl.gz"
        if not path.exists():
            with gzip.open(path, "wt", encoding="utf-8") as handle:
                for row in rows:
                    payload = dict(row)
                    payload["ts"] = datetime.fromtimestamp(payload["ts"], tz=timezone.utc).isoformat()
                    try:
                        payload["metadata"] = json.loads(payload.get("metadata") or "{}")
                    except json.JSONDecodeError:
                        pass
                    handle.write(json.dumps(payload, separators=(",", ":"), default=str) + "\n")
        with self.preserve_lock:
            self.pending_preservations[path] = (decision, start, decision.detected_at, len(rows))
        self._retry_preservations_sync()

    def _retry_preservations_sync(self) -> int:
        with self.preserve_lock:
            pending = list(self.pending_preservations.items())
        uploaded = 0
        for path, (decision, start, end, message_count) in pending:
            if not path.exists():
                with self.preserve_lock:
                    self.pending_preservations.pop(path, None)
                continue
            safe_ts = decision.detected_at.strftime("%Y%m%dT%H%M%SZ")
            object_path = (
                f"crypto/pretrigger/{decision.canonical_symbol}/{decision.detected_at:%Y/%m/%d}/"
                f"{safe_ts}.jsonl.gz"
            )
            try:
                size, checksum = self.storage.upload_file(path, object_path, "application/gzip")
                with db_connection() as conn, conn.cursor() as cur:
                    cur.execute(
                        """
                        insert into crypto_raw_objects(
                            provider,market_type,venue_symbol,canonical_symbol,channel,start_ts,end_ts,
                            object_path,content_type,compression,message_count,size_bytes,checksum,status,metadata
                        ) values ('multi_venue','broad',%s,%s,'broad_pretrigger',%s,%s,%s,
                                  'application/gzip','gzip',%s,%s,%s,'uploaded',%s)
                        on conflict(object_path) do update set message_count=excluded.message_count,
                            size_bytes=excluded.size_bytes,checksum=excluded.checksum,status='uploaded',
                            metadata=excluded.metadata
                        """,
                        (
                            decision.canonical_symbol,decision.canonical_symbol,start,end,
                            object_path,message_count,size,checksum,Jsonb(decision.as_reason()),
                        ),
                    )
                    conn.commit()
            except Exception:
                # Keep the file and in-memory queue for a later flush retry.
                continue
            path.unlink(missing_ok=True)
            with self.preserve_lock:
                self.pending_preservations.pop(path, None)
            uploaded += 1
        return uploaded



broad_observations = BroadObservationStore()


def _detector_config() -> DetectorConfig:
    return DetectorConfig(
        fast_window_seconds=settings.broad_crypto_fast_window_minutes * 60,
        fast_move_pct=settings.broad_crypto_fast_move_pct,
        base_window_seconds=settings.broad_crypto_trigger_window_minutes * 60,
        single_venue_move_pct=settings.broad_crypto_trigger_move_pct,
        slow_window_seconds=settings.broad_crypto_slow_window_minutes * 60,
        slow_move_pct=settings.broad_crypto_slow_move_pct,
        confirmation_count=settings.broad_crypto_confirmation_count,
        confirmed_move_pct=settings.broad_crypto_confirmed_move_pct,
        derivative_move_pct=settings.broad_crypto_derivative_move_pct,
        spot_confirmation_pct=settings.broad_crypto_spot_confirmation_pct,
        volume_move_pct=settings.broad_crypto_volume_move_pct,
        volume_acceleration=settings.broad_crypto_volume_acceleration,
        confirmation_seconds=settings.broad_crypto_confirmation_seconds,
        cooldown_seconds=settings.broad_crypto_trigger_cooldown_seconds,
    )


def _chunks(items: list[dict[str, Any]], size: int) -> list[list[dict[str, Any]]]:
    return [items[index:index + size] for index in range(0, len(items), size)]


def _load_broad_mappings(provider: str) -> list[dict[str, Any]]:
    rows = fetch_all(
        """
        select provider,market_type,venue_symbol,canonical_symbol,quote_asset,priority
          from crypto_venue_symbols
         where provider=%s and tradable=true
         order by priority desc,venue_symbol
        """,
        (provider,),
    )
    if rows or provider != "binance_spot":
        return rows
    return fetch_all(
        """
        select 'binance_spot' as provider,'spot' as market_type,
               provider_symbol as venue_symbol,canonical_symbol,quote_asset,priority
          from instruments
         where provider='binance' and preferred=true and tradable=true
        """
    )


async def _observe(
    detector: MultiVenueDetector,
    *,
    provider: str,
    market_type: str,
    venue_symbol: str,
    canonical_symbol: str,
    quote_asset: str | None,
    ts: datetime,
    price: float | None,
    quote_volume_24h: float | None,
    bid_price: float | None = None,
    ask_price: float | None = None,
) -> None:
    if price is None or price <= 0:
        return
    await broad_observations.record(
        provider=provider,market_type=market_type,venue_symbol=venue_symbol,
        canonical_symbol=canonical_symbol,quote_asset=quote_asset,ts=ts,price=price,
        quote_volume_24h=quote_volume_24h,bid_price=bid_price,ask_price=ask_price,
    )
    decision = detector.ingest(
        MarketObservation(
            provider=provider,
            market_type=market_type,
            venue_symbol=venue_symbol,
            canonical_symbol=canonical_symbol,
            ts=ts,
            price=price,
            quote_volume_24h=quote_volume_24h,
        )
    )
    if decision is not None:
        await broad_observations.preserve(decision)
        await asyncio.to_thread(_activate_target, decision)


async def broad_binance_scanner(
    detector: MultiVenueDetector,
    *,
    provider: str,
    market_type: str,
    url: str,
) -> None:
    """Scan all mapped Binance spot or perpetual tickers without deep subscriptions."""
    while not shutdown.is_set():
        try:
            mappings = await asyncio.to_thread(_load_broad_mappings, provider)
            mapping_cache = {row["venue_symbol"]: row for row in mappings}
            if not mapping_cache:
                await asyncio.sleep(30)
                continue
            _health(provider, "broad_scanner", status="connecting")
            async with websockets.connect(url, ping_interval=20, ping_timeout=20, max_size=32 * 1024 * 1024) as ws:
                _health(provider, "broad_scanner", status="connected")
                async for raw_message in ws:
                    now = utc_now()
                    payload = json.loads(raw_message)
                    items = payload if isinstance(payload, list) else [payload]
                    for item in items:
                        venue_symbol = str(item.get("s") or "")
                        mapping = mapping_cache.get(venue_symbol)
                        price = f(item.get("c"))
                        quote_volume = f(item.get("q"))
                        if not mapping:
                            continue
                        await _observe(
                            detector,
                            provider=provider,
                            market_type=market_type,
                            venue_symbol=venue_symbol,
                            canonical_symbol=mapping["canonical_symbol"],
                            quote_asset=mapping.get("quote_asset"),
                            ts=parse_ts(item.get("E") or now),
                            price=price,
                            quote_volume_24h=quote_volume,
                        )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            _health(provider, "broad_scanner", status="reconnecting", error=str(exc))
            logger.warning("%s broad scanner reconnecting: %s", provider, exc)
            await asyncio.sleep(5)


async def _coinbase_ticker_connection(
    detector: MultiVenueDetector,
    mappings: list[dict[str, Any]],
    connection_number: int,
) -> None:
    products = [row["venue_symbol"] for row in mappings]
    mapping_by_product = {row["venue_symbol"]: row for row in mappings}
    subscribe = {"type": "subscribe", "product_ids": products, "channels": ["ticker_batch"]}
    while not shutdown.is_set():
        try:
            _health("coinbase", f"broad_scanner_{connection_number}", status="connecting")
            async with websockets.connect(
                "wss://ws-feed.exchange.coinbase.com",
                ping_interval=20,
                ping_timeout=20,
                max_size=16 * 1024 * 1024,
            ) as ws:
                await ws.send(json.dumps(subscribe))
                _health("coinbase", f"broad_scanner_{connection_number}", status="connected")
                async for raw_message in ws:
                    item = json.loads(raw_message)
                    product = str(item.get("product_id") or "")
                    mapping = mapping_by_product.get(product)
                    price = f(item.get("price"))
                    base_volume = f(item.get("volume_24h"))
                    if not mapping:
                        continue
                    await _observe(
                        detector,
                        provider="coinbase",
                        market_type="spot",
                        venue_symbol=product,
                        canonical_symbol=mapping["canonical_symbol"],
                        quote_asset=mapping.get("quote_asset"),
                        ts=parse_ts(item.get("time") or time.time()),
                        price=price,
                        quote_volume_24h=(price * base_volume if price is not None and base_volume is not None else None),
                        bid_price=f(item.get("best_bid")),
                        ask_price=f(item.get("best_ask")),
                    )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            _health("coinbase", f"broad_scanner_{connection_number}", status="reconnecting", error=str(exc))
            logger.warning("Coinbase broad scanner %s reconnecting: %s", connection_number, exc)
            await asyncio.sleep(5)


async def broad_coinbase_scanner(detector: MultiVenueDetector) -> None:
    while not shutdown.is_set():
        mappings = await asyncio.to_thread(_load_broad_mappings, "coinbase")
        if not mappings:
            await asyncio.sleep(30)
            continue
        tasks = [
            asyncio.create_task(_coinbase_ticker_connection(detector, chunk, index + 1))
            for index, chunk in enumerate(_chunks(mappings, 150))
        ]
        try:
            await asyncio.gather(*tasks)
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)


async def _kraken_ticker_connection(
    detector: MultiVenueDetector,
    mappings: list[dict[str, Any]],
    connection_number: int,
) -> None:
    symbols = [row["venue_symbol"] for row in mappings]
    mapping_by_symbol = {row["venue_symbol"]: row for row in mappings}
    subscribe = {"method": "subscribe", "params": {"channel": "ticker", "symbol": symbols, "snapshot": True}}
    while not shutdown.is_set():
        try:
            _health("kraken", f"broad_scanner_{connection_number}", status="connecting")
            async with websockets.connect(
                "wss://ws.kraken.com/v2",
                ping_interval=20,
                ping_timeout=20,
                max_size=16 * 1024 * 1024,
            ) as ws:
                await ws.send(json.dumps(subscribe))
                _health("kraken", f"broad_scanner_{connection_number}", status="connected")
                async for raw_message in ws:
                    message = json.loads(raw_message)
                    if message.get("channel") != "ticker":
                        continue
                    for item in message.get("data") or []:
                        symbol = str(item.get("symbol") or "")
                        mapping = mapping_by_symbol.get(symbol)
                        price = f(item.get("last"))
                        base_volume = f(item.get("volume") or item.get("volume_24h"))
                        if not mapping:
                            continue
                        await _observe(
                            detector,
                            provider="kraken",
                            market_type="spot",
                            venue_symbol=symbol,
                            canonical_symbol=mapping["canonical_symbol"],
                            quote_asset=mapping.get("quote_asset"),
                            ts=parse_ts(item.get("timestamp") or time.time()),
                            price=price,
                            quote_volume_24h=(price * base_volume if price is not None and base_volume is not None else None),
                            bid_price=f(item.get("bid")),
                            ask_price=f(item.get("ask")),
                        )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            _health("kraken", f"broad_scanner_{connection_number}", status="reconnecting", error=str(exc))
            logger.warning("Kraken broad scanner %s reconnecting: %s", connection_number, exc)
            await asyncio.sleep(5)


async def broad_kraken_scanner(detector: MultiVenueDetector) -> None:
    while not shutdown.is_set():
        mappings = await asyncio.to_thread(_load_broad_mappings, "kraken")
        if not mappings:
            await asyncio.sleep(30)
            continue
        tasks = [
            asyncio.create_task(_kraken_ticker_connection(detector, chunk, index + 1))
            for index, chunk in enumerate(_chunks(mappings, 150))
        ]
        try:
            await asyncio.gather(*tasks)
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)


async def broad_bybit_scanner(detector: MultiVenueDetector) -> None:
    mappings: dict[str, str] = {}
    last_mapping_refresh = datetime.min.replace(tzinfo=timezone.utc)
    async with httpx.AsyncClient(timeout=20) as client:
        while not shutdown.is_set():
            try:
                now = utc_now()
                if not mappings or now - last_mapping_refresh > timedelta(minutes=15):
                    rows = await asyncio.to_thread(_load_broad_mappings, "bybit")
                    mappings = {row["venue_symbol"]: row for row in rows}
                    last_mapping_refresh = now
                if not mappings:
                    await asyncio.sleep(30)
                    continue
                _health("bybit", "broad_scanner", status="connecting")
                response = await client.get("https://api.bybit.com/v5/market/tickers", params={"category": "linear"})
                response.raise_for_status()
                payload = response.json()
                if str(payload.get("retCode", 0)) != "0":
                    raise RuntimeError(f"Bybit ticker response error: {payload.get('retMsg')}")
                for item in ((payload.get("result") or {}).get("list") or []):
                    symbol = str(item.get("symbol") or "")
                    mapping = mappings.get(symbol)
                    if not mapping:
                        continue
                    await _observe(
                        detector,
                        provider="bybit",
                        market_type="perpetual",
                        venue_symbol=symbol,
                        canonical_symbol=mapping["canonical_symbol"],
                        quote_asset=mapping.get("quote_asset"),
                        ts=now,
                        price=f(item.get("lastPrice")),
                        quote_volume_24h=f(item.get("turnover24h")),
                        bid_price=f(item.get("bid1Price")),
                        ask_price=f(item.get("ask1Price")),
                    )
                _health("bybit", "broad_scanner", status="connected")
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                _health("bybit", "broad_scanner", status="reconnecting", error=str(exc))
                logger.warning("Bybit broad scanner retrying: %s", exc)
            try:
                await asyncio.wait_for(shutdown.wait(), timeout=settings.broad_crypto_scanner_poll_seconds)
            except asyncio.TimeoutError:
                pass


def _activate_target(decision: DetectionDecision) -> None:
    expires = decision.detected_at + timedelta(minutes=settings.crypto_stream_target_ttl_minutes)
    reason = decision.as_reason()
    with db_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            insert into crypto_capture_targets(
                canonical_symbol,source,reason,expires_at,updated_at,
                priority_score,last_observed_at,confirmation_count,trigger_type
            ) values (%s,'multi_venue_detector',%s,%s,now(),%s,%s,%s,%s)
            on conflict(canonical_symbol) do update set
                source=excluded.source,reason=excluded.reason,
                expires_at=greatest(crypto_capture_targets.expires_at,excluded.expires_at),
                priority_score=excluded.priority_score,last_observed_at=excluded.last_observed_at,
                confirmation_count=excluded.confirmation_count,trigger_type=excluded.trigger_type,
                updated_at=now()
            """,
            (
                decision.canonical_symbol,
                Jsonb(reason),
                expires,
                decision.score,
                decision.detected_at,
                decision.provider_count,
                decision.trigger_type,
            ),
        )
        cur.execute(
            """
            select rank_position
              from (
                    select canonical_symbol,
                           row_number() over (
                               order by priority_score desc,
                                        last_observed_at desc nulls last,
                                        updated_at desc,
                                        canonical_symbol
                           ) as rank_position
                      from crypto_capture_targets
                     where expires_at > now()
                   ) ranked
             where canonical_symbol=%s
            """,
            (decision.canonical_symbol,),
        )
        rank_row = cur.fetchone()
        admitted = bool(rank_row and int(rank_row["rank_position"]) <= settings.crypto_stream_max_dynamic_targets)
        cur.execute(
            """
            insert into crypto_dynamic_detections(
                canonical_symbol,detected_at,score,trigger_type,admitted,
                provider_count,spot_provider_count,derivatives_provider_count,reason
            ) values (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            (
                decision.canonical_symbol,
                decision.detected_at,
                decision.score,
                decision.trigger_type,
                admitted,
                decision.provider_count,
                decision.spot_provider_count,
                decision.derivatives_provider_count,
                Jsonb(reason),
            ),
        )
        conn.commit()


async def flush_loop(collector: CryptoCollector, session_id: UUID) -> None:
    while not shutdown.is_set():
        try:
            await collector.flush()
            await broad_observations.flush()
            with db_connection() as conn, conn.cursor() as cur:
                cur.execute(
                    """
                    update crypto_stream_sessions
                       set last_heartbeat_at=now(),message_count=%s,flush_count=%s
                     where id=%s
                    """,
                    (collector.message_count, collector.flush_count, session_id),
                )
                conn.commit()
        except Exception:
            logger.exception("Crypto aggregate flush failed; buffered data will be retried")
        try:
            await asyncio.wait_for(shutdown.wait(), timeout=max(1, settings.crypto_aggregation_seconds))
        except asyncio.TimeoutError:
            pass




def wait_for_stream_schema() -> None:
    for _attempt in range(120):
        try:
            row = fetch_one("select to_regclass('public.crypto_market_observations_1m') as table_name")
            if row and row["table_name"]:
                return
        except Exception:
            pass
        time.sleep(2)
    raise RuntimeError("Database schema through migration 005 was not ready after four minutes")

def _worker_id() -> str:
    return f"{socket.gethostname()}:{os.getpid()}"


async def run() -> None:
    settings.validate_crypto_stream()
    await asyncio.to_thread(wait_for_stream_schema)
    collector = CryptoCollector()
    worker_id = _worker_id()
    session_id = uuid4()
    with db_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            insert into crypto_stream_sessions(id,worker_id,config)
            values (%s,%s,%s)
            """,
            (
                session_id, worker_id,
                Jsonb({
                    "version": "3.3.0",
                    "venues": settings.crypto_stream_venues,
                    "core_symbols": settings.crypto_stream_core_symbols,
                    "aggregation_seconds": settings.crypto_aggregation_seconds,
                    "raw_capture_enabled": settings.crypto_raw_capture_enabled,
                    "full_pair_universe": settings.crypto_full_pair_universe,
                    "broad_observation_seconds": settings.crypto_broad_observation_seconds,
                }),
            ),
        )
        conn.commit()

    detector = MultiVenueDetector(_detector_config())
    venues = set(settings.crypto_stream_venues)
    scanner_tasks: list[asyncio.Task] = []
    if settings.broad_crypto_trigger_enabled:
        if "binance_spot" in venues:
            scanner_tasks.append(asyncio.create_task(
                broad_binance_scanner(
                    detector,
                    provider="binance_spot",
                    market_type="spot",
                    url="wss://stream.binance.com:9443/ws/!miniTicker@arr",
                ),
                name="broad-binance-spot",
            ))
        if "binance_futures" in venues:
            scanner_tasks.append(asyncio.create_task(
                broad_binance_scanner(
                    detector,
                    provider="binance_futures",
                    market_type="perpetual",
                    url="wss://fstream.binance.com/ws/!miniTicker@arr",
                ),
                name="broad-binance-futures",
            ))
        if "coinbase" in venues:
            scanner_tasks.append(asyncio.create_task(broad_coinbase_scanner(detector), name="broad-coinbase"))
        if "kraken" in venues:
            scanner_tasks.append(asyncio.create_task(broad_kraken_scanner(detector), name="broad-kraken"))
        if "bybit" in venues:
            scanner_tasks.append(asyncio.create_task(broad_bybit_scanner(detector), name="broad-bybit"))
    flush_task = asyncio.create_task(flush_loop(collector, session_id), name="flush")
    deep_tasks: list[asyncio.Task] = []
    last_signature = None
    try:
        while not shutdown.is_set():
            targets, mappings, active = await asyncio.to_thread(load_targets)
            signature = (
                tuple(sorted((provider, row["venue_symbol"]) for provider, rows in mappings.items() for row in rows)),
                tuple(sorted(active)),
            )
            if signature != last_signature:
                for task in deep_tasks:
                    task.cancel()
                if deep_tasks:
                    await asyncio.gather(*deep_tasks, return_exceptions=True)
                deep_tasks = []
                if "binance_spot" in venues:
                    for index, chunk in enumerate(_chunks(mappings.get("binance_spot", []), 60), start=1):
                        deep_tasks.append(asyncio.create_task(
                            stream_binance_spot(collector, chunk, active),
                            name=f"deep-binance-spot-{index}",
                        ))
                if "binance_futures" in venues:
                    futures_mappings = mappings.get("binance_futures", [])
                    for index, chunk in enumerate(_chunks(futures_mappings, 30), start=1):
                        deep_tasks.append(asyncio.create_task(
                            stream_binance_futures(collector, chunk, active),
                            name=f"deep-binance-futures-{index}",
                        ))
                    if futures_mappings:
                        deep_tasks.append(asyncio.create_task(
                            poll_binance_open_interest(collector, futures_mappings, active),
                            name="deep-binance-open-interest",
                        ))
                if "coinbase" in venues:
                    for index, chunk in enumerate(_chunks(mappings.get("coinbase", []), 100), start=1):
                        deep_tasks.append(asyncio.create_task(
                            stream_coinbase(collector, chunk, active),
                            name=f"deep-coinbase-{index}",
                        ))
                if "kraken" in venues:
                    for index, chunk in enumerate(_chunks(mappings.get("kraken", []), 100), start=1):
                        deep_tasks.append(asyncio.create_task(
                            stream_kraken(collector, chunk, active),
                            name=f"deep-kraken-{index}",
                        ))
                if "bybit" in venues:
                    for index, chunk in enumerate(_chunks(mappings.get("bybit", []), 20), start=1):
                        deep_tasks.append(asyncio.create_task(
                            stream_bybit(collector, chunk, active),
                            name=f"deep-bybit-{index}",
                        ))
                last_signature = signature
                mapping_count = sum(len(rows) for rows in mappings.values())
                logger.info(
                    "Crypto stream targets refreshed: %s total symbols, %s dynamic, %s venue mappings, %s deep tasks",
                    len(targets), len(active), mapping_count, len(deep_tasks),
                )
            try:
                await asyncio.wait_for(shutdown.wait(), timeout=settings.crypto_stream_refresh_seconds)
            except asyncio.TimeoutError:
                pass
    finally:
        for task in deep_tasks + scanner_tasks + [flush_task]:
            task.cancel()
        await asyncio.gather(*deep_tasks, *scanner_tasks, flush_task, return_exceptions=True)
        await collector.flush(force=True)
        await broad_observations.flush(force=True)
        with db_connection() as conn, conn.cursor() as cur:
            cur.execute(
                """
                update crypto_stream_sessions
                   set status='stopped',stopped_at=now(),last_heartbeat_at=now(),
                       message_count=%s,flush_count=%s
                 where id=%s
                """,
                (collector.message_count, collector.flush_count, session_id),
            )
            conn.commit()


def _signal_handler() -> None:
    shutdown.set()


def main() -> None:
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    for signame in ("SIGTERM", "SIGINT"):
        signum = getattr(signal, signame, None)
        if signum is not None:
            try:
                loop.add_signal_handler(signum, _signal_handler)
            except NotImplementedError:
                signal.signal(signum, lambda *_: loop.call_soon_threadsafe(_signal_handler))
    try:
        loop.run_until_complete(run())
    finally:
        try:
            get_pool().close()
        except Exception:
            pass
        loop.close()


if __name__ == "__main__":
    main()
