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

# The futures lane is isolated, opt-in, resumable and runs in its own low-rate thread.
from app.index_futures_ingest import start_index_futures_ingestion_if_enabled

start_index_futures_ingestion_if_enabled()
