from __future__ import annotations

from datetime import timedelta
from typing import Any, Iterable
from urllib.parse import quote

from app.config import get_settings
from app.exceptions import EmptyData
from app.http import JsonHttpClient
from app.providers.base import BaseProvider, Page, as_float


CRYPTO_PRIORITY = ["BTC", "ETH", "SOL", "XRP", "BNB", "ADA", "DOGE", "AVAX", "LINK", "DOT", "LTC", "BCH", "UNI", "AAVE", "ATOM"]
CRYPTO_RANK = {symbol: len(CRYPTO_PRIORITY) - index for index, symbol in enumerate(CRYPTO_PRIORITY)}


class CoinbaseProvider(BaseProvider):
    name = "coinbase"

    def __init__(self):
        self.settings = get_settings()
        self.http = JsonHttpClient(300)

    def catalogue(self) -> list[dict[str, Any]]:
        payload = self.http.get("https://api.exchange.coinbase.com/products")
        allowed_quotes = {value.upper() for value in self.settings.coinbase_allowed_quotes}
        result: list[dict[str, Any]] = []
        for product in payload:
            quote_asset = (product.get("quote_currency") or "").upper()
            base_asset = (product.get("base_currency") or "").upper()
            if not self.settings.crypto_full_pair_universe and quote_asset not in allowed_quotes:
                continue
            if product.get("status") not in {"online", None} or product.get("trading_disabled", False):
                continue
            product_id = product["id"]
            result.append(
                {
                    "provider": self.name,
                    "provider_symbol": product_id,
                    "canonical_symbol": base_asset,
                    "display_name": product.get("display_name") or product_id,
                    "asset_class": "crypto_spot",
                    "base_asset": base_asset,
                    "quote_asset": quote_asset,
                    "exchange": "Coinbase Exchange",
                    "status": product.get("status", "online"),
                    "tradable": True,
                    "preferred": True,
                    "source_feed": "coinbase_exchange",
                    "priority": (
                        (100 - self.settings.coinbase_allowed_quotes.index(quote_asset)) * 10_000
                        + CRYPTO_RANK.get(base_asset, 0)
                    ) if quote_asset in self.settings.coinbase_allowed_quotes else CRYPTO_RANK.get(base_asset, 0),
                    "metadata": product,
                }
            )
        return result

    def iter_bar_pages(self, partition: dict[str, Any]) -> Iterable[Page]:
        product_id = partition["provider_symbol"]
        cursor = dict(partition.get("cursor") or {})
        if cursor.get("finished"):
            yield Page(rows=[], cursor=cursor, done=True)
            return
        # Coinbase treats start/end as inclusive and rejects ranges exceeding 300 candles.
        end_inclusive = partition["end_ts"] - timedelta(milliseconds=1)
        payload = self.http.get(
            f"https://api.exchange.coinbase.com/products/{quote(product_id, safe='')}/candles",
            params={
                "granularity": 60,
                "start": partition["start_ts"].isoformat(),
                "end": end_inclusive.isoformat(),
            },
        )
        rows = []
        for candle in payload:
            if len(candle) < 6:
                continue
            ts = int(candle[0])
            rows.append(
                {
                    "provider": self.name,
                    "instrument_id": partition["instrument_id"],
                    "ts": __import__("datetime").datetime.fromtimestamp(ts, tz=__import__("datetime").timezone.utc),
                    "open": as_float(candle[3]),
                    "high": as_float(candle[2]),
                    "low": as_float(candle[1]),
                    "close": as_float(candle[4]),
                    "volume": as_float(candle[5]),
                    "quote_volume": None,
                    "trade_count": None,
                    "vwap": None,
                    "taker_buy_base_volume": None,
                    "taker_buy_quote_volume": None,
                    "source_feed": "coinbase_exchange",
                }
            )
        rows.sort(key=lambda row: row["ts"])
        if not rows:
            raise EmptyData(f"No Coinbase candles for {product_id} in this partition")
        yield Page(rows=rows, cursor={"finished": True}, done=True)
