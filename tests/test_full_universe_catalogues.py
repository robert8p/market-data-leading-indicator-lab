from app.providers.binance import BinanceProvider
from app.providers.coinbase import CoinbaseProvider


def test_coinbase_full_pair_universe_keeps_non_priority_quote(monkeypatch):
    provider = CoinbaseProvider()
    provider.settings.crypto_full_pair_universe = True
    monkeypatch.setattr(
        provider.http,
        "get",
        lambda *_args, **_kwargs: [
            {
                "id": "ABC-TRY",
                "base_currency": "ABC",
                "quote_currency": "TRY",
                "display_name": "ABC/TRY",
                "status": "online",
                "trading_disabled": False,
            }
        ],
    )
    rows = provider.catalogue()
    assert [row["provider_symbol"] for row in rows] == ["ABC-TRY"]
    assert rows[0]["quote_asset"] == "TRY"
    assert rows[0]["preferred"] is True


def test_binance_all_pair_mode_keeps_non_priority_quote(monkeypatch):
    provider = BinanceProvider()
    provider.settings.crypto_full_pair_universe = True
    provider.settings.binance_pair_mode = "all"

    def fake_get(url, **_kwargs):
        if url.endswith("exchangeInfo"):
            return {
                "symbols": [
                    {
                        "symbol": "ABCTRY",
                        "status": "TRADING",
                        "isSpotTradingAllowed": True,
                        "baseAsset": "ABC",
                        "quoteAsset": "TRY",
                    }
                ]
            }
        return [{"symbol": "ABCTRY", "quoteVolume": "12345"}]

    monkeypatch.setattr(provider.http, "get", fake_get)
    rows = provider.catalogue()
    assert [row["provider_symbol"] for row in rows] == ["ABCTRY"]
    assert rows[0]["quote_asset"] == "TRY"
    assert rows[0]["preferred"] is True
