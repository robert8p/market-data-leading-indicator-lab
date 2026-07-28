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
    assert settings.crypto_stream_max_dynamic_targets == 75
    assert settings.broad_crypto_confirmation_count == 2
    assert settings.alpaca_feed == "sip"


def test_equity_context_scope_is_validated(monkeypatch):
    monkeypatch.setenv("EQUITY_CONTEXT_SCOPE", "captured")
    settings = Settings()
    assert settings.equity_context_scope == "captured"


def test_miner_first_defaults_cover_full_crypto_pairs_and_equity_baselines(monkeypatch):
    settings = Settings()
    assert settings.crypto_full_pair_universe is True
    assert settings.binance_pair_mode == "all"
    assert settings.crypto_broad_observation_seconds == 60
    assert settings.capture_window_before_minutes == 120
    assert settings.equity_baseline_sample_enabled is True
