import os

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

if os.getenv("INDEX_FUTURES_ACCESS_PROBE", "false").strip().lower() in {"1", "true", "yes", "on"}:
    from app.futures_access_probe import run_massive_futures_access_probe

    run_massive_futures_access_probe()
