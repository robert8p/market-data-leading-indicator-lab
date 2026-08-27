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

# The futures lane is isolated, opt-in, resumable and uses service-role RPCs so
# it does not depend on the worker's legacy direct Postgres credential.
from app.index_futures_ingest_http_v2 import start_index_futures_ingestion_if_enabled

start_index_futures_ingestion_if_enabled()
