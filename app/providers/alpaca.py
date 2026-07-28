from __future__ import annotations

import hashlib
import json
from typing import Any, Iterable
from urllib.parse import quote

from app.config import get_settings
from app.exceptions import EmptyData
from app.http import JsonHttpClient
from app.providers.base import BaseProvider, Page, as_float, as_int, as_utc


VALIDATION_PRIORITY = [
    "SPY", "QQQ", "IWM", "DIA", "AAPL", "MSFT", "NVDA", "AMZN", "META", "GOOGL",
    "GOOG", "TSLA", "AMD", "AVGO", "NFLX", "JPM", "XOM", "UNH", "LLY", "V",
    "MA", "COST", "WMT", "HD", "KO", "PEP", "GLD", "SLV", "TLT", "HYG", "LQD",
]
VALIDATION_RANK = {symbol: len(VALIDATION_PRIORITY) - index for index, symbol in enumerate(VALIDATION_PRIORITY)}


class AlpacaProvider(BaseProvider):
    name = "alpaca"

    def __init__(self):
        self.settings = get_settings()
        self.headers = {
            "APCA-API-KEY-ID": self.settings.alpaca_api_key,
            "APCA-API-SECRET-KEY": self.settings.alpaca_api_secret,
        }
        self.http = JsonHttpClient(self.settings.alpaca_requests_per_minute, headers=self.headers)

    def catalogue(self) -> list[dict[str, Any]]:
        payload = self.http.get(
            "https://paper-api.alpaca.markets/v2/assets",
            params={"status": "active", "asset_class": "us_equity"},
        )
        result: list[dict[str, Any]] = []
        for asset in payload:
            if not asset.get("tradable", False):
                continue
            symbol = asset["symbol"]
            result.append(
                {
                    "provider": self.name,
                    "provider_symbol": symbol,
                    "canonical_symbol": symbol.upper(),
                    "display_name": asset.get("name") or symbol,
                    "asset_class": "us_equity",
                    "base_asset": symbol.upper(),
                    "quote_asset": "USD",
                    "exchange": asset.get("exchange"),
                    "status": asset.get("status", "active"),
                    "tradable": True,
                    "preferred": True,
                    "source_feed": self.settings.alpaca_feed,
                    "priority": VALIDATION_RANK.get(symbol.upper(), 0) * 10_000 + (10 if asset.get("easy_to_borrow") else 0),
                    "metadata": asset,
                }
            )
        return result

    def iter_bar_pages(self, partition: dict[str, Any]) -> Iterable[Page]:
        symbol = partition["provider_symbol"]
        cursor = dict(partition.get("cursor") or {})
        if cursor.get("finished"):
            yield Page(rows=[], cursor=cursor, done=True)
            return
        page_token = cursor.get("next_page_token")
        yielded = False
        while True:
            params: dict[str, Any] = {
                "timeframe": "1Min",
                "start": partition["start_ts"].isoformat(),
                "end": partition["end_ts"].isoformat(),
                "limit": 10000,
                "adjustment": "split",
                "feed": self.settings.alpaca_feed,
                "sort": "asc",
            }
            if page_token:
                params["page_token"] = page_token
            payload = self.http.get(
                f"https://data.alpaca.markets/v2/stocks/{quote(symbol, safe='')}/bars",
                params=params,
            )
            raw_bars = payload.get("bars") or []
            rows = [
                {
                    "provider": self.name,
                    "instrument_id": partition["instrument_id"],
                    "ts": as_utc(bar["t"]),
                    "open": as_float(bar.get("o")),
                    "high": as_float(bar.get("h")),
                    "low": as_float(bar.get("l")),
                    "close": as_float(bar.get("c")),
                    "volume": as_float(bar.get("v")),
                    "quote_volume": None,
                    "trade_count": as_int(bar.get("n")),
                    "vwap": as_float(bar.get("vw")),
                    "taker_buy_base_volume": None,
                    "taker_buy_quote_volume": None,
                    "source_feed": self.settings.alpaca_feed,
                }
                for bar in raw_bars
            ]
            next_page_token = payload.get("next_page_token")
            yielded = yielded or bool(rows)
            yield Page(
                rows=rows,
                cursor={"next_page_token": next_page_token, "finished": not bool(next_page_token)},
                done=not bool(next_page_token),
            )
            if not next_page_token:
                break
            page_token = next_page_token
        if not yielded:
            raise EmptyData(f"No Alpaca {self.settings.alpaca_feed.upper()} bars for {symbol} in this partition")


    @staticmethod
    def _message_key(kind: str, symbol: str, item: dict[str, Any]) -> str:
        raw = json.dumps([kind, symbol, item], sort_keys=True, separators=(",", ":"), default=str)
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    def iter_trade_pages(self, partition: dict[str, Any]) -> Iterable[Page]:
        symbol = partition["provider_symbol"]
        cursor = dict(partition.get("cursor") or {})
        if cursor.get("finished"):
            yield Page(rows=[], cursor=cursor, done=True)
            return
        page_token = cursor.get("next_page_token")
        yielded = False
        while True:
            params: dict[str, Any] = {
                "start": partition["start_ts"].isoformat(),
                "end": partition["end_ts"].isoformat(),
                "limit": 10000,
                "feed": self.settings.alpaca_feed,
                "sort": "asc",
            }
            if page_token:
                params["page_token"] = page_token
            payload = self.http.get(
                f"https://data.alpaca.markets/v2/stocks/{quote(symbol, safe='')}/trades",
                params=params,
            )
            raw = payload.get("trades") or []
            rows = [{
                "instrument_id": partition["instrument_id"],
                "message_key": self._message_key("trade", symbol, item),
                "ts": as_utc(item["t"]),
                "price": as_float(item.get("p")),
                "size": as_float(item.get("s")),
                "provider": self.name,
                "quote_size": (as_float(item.get("p")) or 0.0) * (as_float(item.get("s")) or 0.0),
                "aggressor_side": None,
                "exchange": item.get("x"),
                "trade_id": str(item.get("i")) if item.get("i") is not None else None,
                "conditions": item.get("c") or [],
                "source_feed": self.settings.alpaca_feed,
                "metadata": {"tape": item.get("z")},
            } for item in raw if item.get("p") is not None and item.get("s") is not None]
            next_page_token = payload.get("next_page_token")
            yielded = yielded or bool(rows)
            yield Page(rows=rows, cursor={"next_page_token": next_page_token, "finished": not bool(next_page_token)}, done=not bool(next_page_token))
            if not next_page_token:
                break
            page_token = next_page_token
        if not yielded:
            raise EmptyData(f"No Alpaca trades for {symbol} in this partition")

    def iter_quote_pages(self, partition: dict[str, Any]) -> Iterable[Page]:
        symbol = partition["provider_symbol"]
        cursor = dict(partition.get("cursor") or {})
        if cursor.get("finished"):
            yield Page(rows=[], cursor=cursor, done=True)
            return
        page_token = cursor.get("next_page_token")
        yielded = False
        while True:
            params: dict[str, Any] = {
                "start": partition["start_ts"].isoformat(),
                "end": partition["end_ts"].isoformat(),
                "limit": 10000,
                "feed": self.settings.alpaca_feed,
                "sort": "asc",
            }
            if page_token:
                params["page_token"] = page_token
            payload = self.http.get(
                f"https://data.alpaca.markets/v2/stocks/{quote(symbol, safe='')}/quotes",
                params=params,
            )
            raw = payload.get("quotes") or []
            rows = [{
                "instrument_id": partition["instrument_id"],
                "message_key": self._message_key("quote", symbol, item),
                "ts": as_utc(item["t"]),
                "provider": self.name,
                "bid_exchange": item.get("bx"),
                "bid_price": as_float(item.get("bp")),
                "bid_size": as_float(item.get("bs")),
                "ask_exchange": item.get("ax"),
                "ask_price": as_float(item.get("ap")),
                "ask_size": as_float(item.get("as")),
                "conditions": item.get("c") or [],
                "source_feed": self.settings.alpaca_feed,
                "metadata": {"tape": item.get("z")},
            } for item in raw]
            next_page_token = payload.get("next_page_token")
            yielded = yielded or bool(rows)
            yield Page(rows=rows, cursor={"next_page_token": next_page_token, "finished": not bool(next_page_token)}, done=not bool(next_page_token))
            if not next_page_token:
                break
            page_token = next_page_token
        if not yielded:
            raise EmptyData(f"No Alpaca quotes for {symbol} in this partition")
