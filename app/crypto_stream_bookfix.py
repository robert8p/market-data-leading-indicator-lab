from __future__ import annotations

import asyncio
import json
import time
from typing import Any


def _partial_depth_levels(data: dict[str, Any]) -> tuple[list[Any], list[Any]]:
    """Return the complete top-N bid/ask lists carried by a partial-depth message."""
    bids = data.get("bids")
    asks = data.get("asks")
    if bids is None:
        bids = data.get("b")
    if asks is None:
        asks = data.get("a")
    return list(bids or []), list(asks or [])


def apply_partial_depth_snapshot(state: Any, data: dict[str, Any]) -> None:
    """Replace, rather than incrementally mutate, state for Binance partial depth."""
    bids, asks = _partial_depth_levels(data)
    state.snapshot(bids, asks)


def _crossed_book_guard(original_metrics):
    def metrics(self):
        values = original_metrics(self)
        if not values:
            return values
        bid = values.get("bid_price")
        ask = values.get("ask_price")
        if bid is not None and ask is not None and bid > ask:
            return {}
        return values

    return metrics


def _fixed_binance_futures_stream(stream_module: Any):
    async def stream_binance_futures(
        collector: Any,
        mappings: list[dict[str, Any]],
        active: set[str],
    ) -> None:
        if not mappings:
            return

        by_symbol = {
            row["venue_symbol"].lower(): row["canonical_symbol"]
            for row in mappings
        }
        streams: list[str] = []
        for symbol in by_symbol:
            streams.extend(
                [
                    f"{symbol}@aggTrade",
                    f"{symbol}@depth20@500ms",
                    f"{symbol}@markPrice@1s",
                    f"{symbol}@forceOrder",
                ]
            )

        url = "wss://fstream.binance.com/stream?streams=" + "/".join(streams)
        while not stream_module.shutdown.is_set():
            try:
                stream_module._health(
                    "binance_futures", "websocket", status="connecting"
                )
                async with stream_module.websockets.connect(
                    url,
                    ping_interval=20,
                    ping_timeout=20,
                    max_size=16 * 1024 * 1024,
                ) as ws:
                    stream_module._health(
                        "binance_futures", "websocket", status="connected"
                    )
                    async for raw_message in ws:
                        message = json.loads(raw_message)
                        data = message.get("data") or {}
                        event = data.get("e")
                        order = data.get("o") or {}
                        stream_name = str(message.get("stream") or "")
                        symbol = str(
                            data.get("s")
                            or order.get("s")
                            or stream_name.split("@")[0]
                        ).lower()
                        canonical = by_symbol.get(symbol)
                        if not canonical:
                            continue

                        ts = stream_module.parse_ts(
                            data.get("E")
                            or data.get("T")
                            or order.get("T")
                            or time.time()
                        )
                        raw_enabled = stream_module.is_raw(canonical, active)

                        if event == "aggTrade":
                            price = stream_module.f(data.get("p"))
                            size = stream_module.f(data.get("q"))
                            if price is not None and size is not None:
                                await collector.trade(
                                    "binance_futures",
                                    "perpetual",
                                    symbol.upper(),
                                    canonical,
                                    ts,
                                    price,
                                    size,
                                    "sell" if data.get("m") else "buy",
                                    data,
                                    raw_enabled,
                                )
                        elif event == "depthUpdate" or "bids" in data or "asks" in data:
                            state = collector.book_state(
                                "binance_futures", "perpetual", symbol.upper()
                            )
                            # @depth20 is a partial top-N book stream. Each message is
                            # the current partial book, not a delta to accumulate.
                            apply_partial_depth_snapshot(state, data)
                            await collector.book(
                                "binance_futures",
                                "perpetual",
                                symbol.upper(),
                                canonical,
                                ts,
                                data,
                                raw_enabled,
                            )
                        elif event == "markPriceUpdate":
                            await collector.derivative(
                                "binance_futures",
                                "perpetual",
                                symbol.upper(),
                                canonical,
                                ts,
                                {
                                    "mark_price": stream_module.f(data.get("p")),
                                    "index_price": stream_module.f(data.get("i")),
                                    "funding_rate": stream_module.f(data.get("r")),
                                    "next_funding_at": (
                                        stream_module.parse_ts(data["T"])
                                        if data.get("T")
                                        else None
                                    ),
                                },
                                data,
                                raw_enabled,
                            )
                        elif event == "forceOrder":
                            price = stream_module.f(order.get("ap") or order.get("p"))
                            size = stream_module.f(order.get("q"))
                            if price is not None and size is not None:
                                await collector.liquidation(
                                    "binance_futures",
                                    "perpetual",
                                    symbol.upper(),
                                    canonical,
                                    ts,
                                    str(order.get("T") or data.get("E")),
                                    str(order.get("S") or "").lower(),
                                    price,
                                    size,
                                    data,
                                    raw_enabled,
                                )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                stream_module._health(
                    "binance_futures",
                    "websocket",
                    status="reconnecting",
                    error=str(exc),
                )
                stream_module.logger.warning(
                    "Binance futures stream reconnecting: %s", exc
                )
                await asyncio.sleep(5)

    return stream_binance_futures


def install_binance_futures_book_fix(stream_module: Any) -> None:
    """Install the partial-depth fix before deep-stream tasks are created."""
    book_state = stream_module.BookState
    if not getattr(book_state, "_crossed_book_guard_installed", False):
        book_state.metrics = _crossed_book_guard(book_state.metrics)
        book_state._crossed_book_guard_installed = True

    stream_module.stream_binance_futures = _fixed_binance_futures_stream(stream_module)
