from __future__ import annotations

import hashlib
import json
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable

from app.config import get_settings
from app.exceptions import EmptyData
from app.http import JsonHttpClient
from app.providers.base import BaseProvider, Page, as_float, as_int


class BinanceProvider(BaseProvider):
    name = "binance"

    def __init__(self):
        self.settings = get_settings()
        self.http = JsonHttpClient(self.settings.binance_requests_per_minute)

    def catalogue(self) -> list[dict[str, Any]]:
        exchange_info = self.http.get("https://api.binance.com/api/v3/exchangeInfo")
        ticker_payload = self.http.get("https://api.binance.com/api/v3/ticker/24hr")
        ticker_map = {item.get("symbol"): item for item in ticker_payload if item.get("symbol")}
        quote_priority = {quote: len(self.settings.binance_quote_priority) - index for index, quote in enumerate(self.settings.binance_quote_priority)}

        candidates: list[dict[str, Any]] = []
        for symbol_info in exchange_info.get("symbols", []):
            if symbol_info.get("status") != "TRADING" or not symbol_info.get("isSpotTradingAllowed", True):
                continue
            quote_asset = (symbol_info.get("quoteAsset") or "").upper()
            base_asset = (symbol_info.get("baseAsset") or "").upper()
            if not self.settings.crypto_full_pair_universe and quote_asset not in quote_priority:
                continue
            symbol = symbol_info["symbol"]
            ticker = ticker_map.get(symbol, {})
            quote_volume = float(ticker.get("quoteVolume") or 0.0)
            candidates.append(
                {
                    "provider": self.name,
                    "provider_symbol": symbol,
                    "canonical_symbol": base_asset,
                    "display_name": f"{base_asset}/{quote_asset}",
                    "asset_class": "crypto_spot",
                    "base_asset": base_asset,
                    "quote_asset": quote_asset,
                    "exchange": "Binance.com",
                    "status": "TRADING",
                    "tradable": True,
                    "preferred": True,
                    "source_feed": "binance_spot",
                    "priority": quote_priority.get(quote_asset, 0) * 1_000_000 + min(int(quote_volume), 999_999),
                    "metadata": {**symbol_info, "ticker24h": ticker},
                }
            )

        if self.settings.binance_pair_mode == "all":
            return candidates

        best_by_base: dict[str, dict[str, Any]] = {}
        for item in candidates:
            current = best_by_base.get(item["base_asset"])
            if current is None or item["priority"] > current["priority"]:
                best_by_base[item["base_asset"]] = item
        return list(best_by_base.values())

    def iter_bar_pages(self, partition: dict[str, Any]) -> Iterable[Page]:
        symbol = partition["provider_symbol"]
        cursor = dict(partition.get("cursor") or {})
        if cursor.get("finished"):
            yield Page(rows=[], cursor=cursor, done=True)
            return
        start_ms = int(partition["start_ts"].timestamp() * 1000)
        end_ms = int((partition["end_ts"] - timedelta(milliseconds=1)).timestamp() * 1000)
        payload = self.http.get(
            "https://api.binance.com/api/v3/klines",
            params={
                "symbol": symbol,
                "interval": "1m",
                "startTime": start_ms,
                "endTime": end_ms,
                "limit": 1000,
            },
        )
        rows = []
        dt_module = __import__("datetime")
        for candle in payload:
            if len(candle) < 11:
                continue
            rows.append(
                {
                    "provider": self.name,
                    "instrument_id": partition["instrument_id"],
                    "ts": dt_module.datetime.fromtimestamp(int(candle[0]) / 1000, tz=dt_module.timezone.utc),
                    "open": as_float(candle[1]),
                    "high": as_float(candle[2]),
                    "low": as_float(candle[3]),
                    "close": as_float(candle[4]),
                    "volume": as_float(candle[5]),
                    "quote_volume": as_float(candle[7]),
                    "trade_count": as_int(candle[8]),
                    "vwap": None,
                    "taker_buy_base_volume": as_float(candle[9]),
                    "taker_buy_quote_volume": as_float(candle[10]),
                    "source_feed": "binance_spot",
                }
            )
        if not rows:
            raise EmptyData(f"No Binance klines for {symbol} in this partition")
        yield Page(rows=rows, cursor={"finished": True}, done=True)


    @staticmethod
    def _trade_key(symbol: str, item: dict[str, Any]) -> str:
        trade_id = item.get("a")
        if trade_id is not None:
            return f"agg:{symbol}:{trade_id}"
        raw = json.dumps(item, sort_keys=True, separators=(",", ":"), default=str)
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    def iter_trade_pages(self, partition: dict[str, Any]) -> Iterable[Page]:
        """Backfill aggregate trades for a bounded acquisition window.

        Binance aggregate trades are grouped by a single taker order. The maker flag
        allows a deterministic aggressor-side classification.
        """
        symbol = partition["provider_symbol"]
        cursor = dict(partition.get("cursor") or {})
        if cursor.get("finished"):
            yield Page(rows=[], cursor=cursor, done=True)
            return

        start_ms = int(partition["start_ts"].timestamp() * 1000)
        end_ms = int((partition["end_ts"] - timedelta(milliseconds=1)).timestamp() * 1000)
        from_id = cursor.get("next_from_id")
        yielded = False

        while True:
            params: dict[str, Any] = {"symbol": symbol, "limit": 1000}
            if from_id is not None:
                params["fromId"] = int(from_id)
            else:
                params["startTime"] = start_ms
                params["endTime"] = end_ms
            payload = self.http.get("https://api.binance.com/api/v3/aggTrades", params=params)
            raw = [item for item in payload if int(item.get("T") or 0) <= end_ms]
            rows: list[dict[str, Any]] = []
            for item in raw:
                price = as_float(item.get("p"))
                size = as_float(item.get("q"))
                if price is None or size is None:
                    continue
                buyer_is_maker = bool(item.get("m"))
                rows.append(
                    {
                        "provider": self.name,
                        "instrument_id": partition["instrument_id"],
                        "message_key": self._trade_key(symbol, item),
                        "ts": datetime.fromtimestamp(int(item["T"]) / 1000, tz=timezone.utc),
                        "price": price,
                        "size": size,
                        "quote_size": price * size,
                        "aggressor_side": "sell" if buyer_is_maker else "buy",
                        "exchange": "Binance.com",
                        "trade_id": str(item.get("a")) if item.get("a") is not None else None,
                        "conditions": [],
                        "source_feed": "binance_aggTrades",
                        "metadata": {
                            "first_trade_id": item.get("f"),
                            "last_trade_id": item.get("l"),
                            "buyer_is_maker": buyer_is_maker,
                            "best_price_match": item.get("M"),
                        },
                    }
                )
            yielded = yielded or bool(rows)
            if not raw or len(raw) < 1000:
                next_id = None
                done = True
            else:
                last_id = int(raw[-1]["a"])
                last_time = int(raw[-1]["T"])
                next_id = last_id + 1 if last_time < end_ms else None
                done = next_id is None
            yield Page(
                rows=rows,
                cursor={"next_from_id": next_id, "finished": done},
                done=done,
            )
            if done:
                break
            from_id = next_id

        if not yielded:
            raise EmptyData(f"No Binance aggregate trades for {symbol} in this partition")
