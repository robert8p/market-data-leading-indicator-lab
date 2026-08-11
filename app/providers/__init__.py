from app.providers.alpaca import AlpacaProvider
from app.providers.binance_paginated import BinanceProvider
from app.providers.coinbase import CoinbaseProvider
from app.providers.twelvedata import TwelveDataProvider

PROVIDER_CLASSES = {
    "alpaca": AlpacaProvider,
    "coinbase": CoinbaseProvider,
    "binance": BinanceProvider,
    "twelvedata": TwelveDataProvider,
}
