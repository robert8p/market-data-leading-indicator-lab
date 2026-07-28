from app.config import Settings


def test_csv_environment_values_parse_to_lists(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "postgresql://user:password@localhost/postgres")
    monkeypatch.setenv("SUPABASE_URL", "https://example.supabase.co")
    monkeypatch.setenv("SUPABASE_SERVICE_ROLE_KEY", "key")
    monkeypatch.setenv("BINANCE_QUOTE_PRIORITY", "USDT, USDC, BTC")
    monkeypatch.setenv("CRYPTO_STREAM_CORE_SYMBOLS", "BTC, ETH, SOL")
    settings = Settings()
    assert settings.binance_quote_priority == ["USDT", "USDC", "BTC"]
    assert settings.crypto_stream_core_symbols == ["BTC", "ETH", "SOL"]
    assert settings.alpaca_feed == "sip"


def test_equity_context_scope_is_validated(monkeypatch):
    monkeypatch.setenv("EQUITY_CONTEXT_SCOPE", "captured")
    settings = Settings()
    assert settings.equity_context_scope == "captured"
