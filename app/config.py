from __future__ import annotations

from functools import lru_cache

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Runtime settings for the collection-only market-data miner."""

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    database_url: str = Field(alias="DATABASE_URL")
    supabase_url: str = Field(alias="SUPABASE_URL")
    supabase_service_role_key: str = Field(alias="SUPABASE_SERVICE_ROLE_KEY")
    raw_bucket: str = Field(default="market-data-raw", alias="RAW_BUCKET")

    app_username: str = Field(default="rob", alias="APP_USERNAME")
    app_password: str = Field(default="", alias="APP_PASSWORD")
    session_secret: str = Field(default="change-me", alias="SESSION_SECRET")

    # US equities
    alpaca_api_key: str = Field(default="", alias="ALPACA_API_KEY")
    alpaca_api_secret: str = Field(default="", alias="ALPACA_API_SECRET")
    alpaca_feed: str = Field(default="sip", alias="ALPACA_FEED")
    alpaca_otc_enabled: bool = Field(default=False, alias="ALPACA_OTC_ENABLED")
    alpaca_requests_per_minute: int = Field(default=9000, ge=1, alias="ALPACA_REQUESTS_PER_MINUTE")

    massive_api_key: str = Field(default="", alias="MASSIVE_API_KEY")
    massive_requests_per_minute: int = Field(default=60, ge=1, alias="MASSIVE_REQUESTS_PER_MINUTE")
    equity_enrichment_enabled: bool = Field(default=True, alias="EQUITY_ENRICHMENT_ENABLED")
    equity_context_scope: str = Field(default="all", alias="EQUITY_CONTEXT_SCOPE")

    sec_user_agent: str = Field(default="", alias="SEC_USER_AGENT")
    sec_requests_per_minute: int = Field(default=300, ge=1, alias="SEC_REQUESTS_PER_MINUTE")
    sec_document_scan_enabled: bool = Field(default=True, alias="SEC_DOCUMENT_SCAN_ENABLED")
    sec_max_documents_per_symbol: int = Field(default=8, ge=0, le=25, alias="SEC_MAX_DOCUMENTS_PER_SYMBOL")
    finra_requests_per_minute: int = Field(default=30, ge=1, alias="FINRA_REQUESTS_PER_MINUTE")

    # Neutral acquisition triggers. These decide what extra raw data to collect,
    # not whether a pattern is predictive.
    capture_scan_enabled: bool = Field(default=True, alias="CAPTURE_SCAN_ENABLED")
    equity_capture_move_pct: float = Field(default=2.0, ge=0.1, alias="EQUITY_CAPTURE_MOVE_PCT")
    equity_capture_5m_move_pct: float = Field(default=1.5, ge=0.1, alias="EQUITY_CAPTURE_5M_MOVE_PCT")
    crypto_capture_5m_move_pct: float = Field(default=1.5, ge=0.1, alias="CRYPTO_CAPTURE_5M_MOVE_PCT")
    crypto_capture_15m_move_pct: float = Field(default=2.5, ge=0.1, alias="CRYPTO_CAPTURE_15M_MOVE_PCT")
    capture_relative_volume: float = Field(default=5.0, ge=1.0, alias="CAPTURE_RELATIVE_VOLUME")
    capture_min_price: float = Field(default=0.05, ge=0.0, alias="CAPTURE_MIN_PRICE")
    capture_min_dollar_volume: float = Field(default=50_000.0, ge=0.0, alias="CAPTURE_MIN_DOLLAR_VOLUME")
    capture_cooldown_minutes: int = Field(default=120, ge=15, le=1440, alias="CAPTURE_COOLDOWN_MINUTES")
    capture_window_before_minutes: int = Field(default=120, ge=5, le=360, alias="CAPTURE_WINDOW_BEFORE_MINUTES")
    equity_baseline_sample_enabled: bool = Field(default=True, alias="EQUITY_BASELINE_SAMPLE_ENABLED")
    equity_baseline_sample_rate: float = Field(default=0.005, ge=0.0, le=1.0, alias="EQUITY_BASELINE_SAMPLE_RATE")
    equity_baseline_max_windows_per_run: int = Field(default=1000, ge=0, le=10000, alias="EQUITY_BASELINE_MAX_WINDOWS_PER_RUN")
    equity_baseline_seed: str = Field(default="miner-baseline-v1", alias="EQUITY_BASELINE_SEED")
    max_capture_windows_per_instrument: int = Field(default=1000, ge=10, le=5000, alias="MAX_CAPTURE_WINDOWS_PER_INSTRUMENT")
    capture_window_after_minutes: int = Field(default=120, ge=15, le=720, alias="CAPTURE_WINDOW_AFTER_MINUTES")
    microstructure_partition_minutes: int = Field(default=15, ge=5, le=60, alias="MICROSTRUCTURE_PARTITION_MINUTES")
    max_capture_windows_per_run: int = Field(default=5000, ge=100, le=50000, alias="MAX_CAPTURE_WINDOWS_PER_RUN")

    # Crypto historical/context enrichment
    crypto_enrichment_enabled: bool = Field(default=True, alias="CRYPTO_ENRICHMENT_ENABLED")
    coingecko_demo_api_key: str = Field(default="", alias="COINGECKO_DEMO_API_KEY")
    coingecko_requests_per_minute: int = Field(default=25, ge=1, alias="COINGECKO_REQUESTS_PER_MINUTE")
    crypto_derivatives_symbol_cap: int = Field(default=300, ge=10, le=1000, alias="CRYPTO_DERIVATIVES_SYMBOL_CAP")
    binance_requests_per_minute: int = Field(default=900, ge=1, alias="BINANCE_REQUESTS_PER_MINUTE")
    bybit_requests_per_minute: int = Field(default=300, ge=1, alias="BYBIT_REQUESTS_PER_MINUTE")
    kraken_requests_per_minute: int = Field(default=60, ge=1, alias="KRAKEN_REQUESTS_PER_MINUTE")

    # Prospective crypto microstructure stream
    crypto_stream_enabled: bool = Field(default=True, alias="CRYPTO_STREAM_ENABLED")
    crypto_stream_core_symbols_csv: str = Field(
        default="BTC,ETH,SOL,XRP,BNB,DOGE,ADA,AVAX,LINK,LTC,BCH,DOT,UNI,AAVE,ATOM",
        alias="CRYPTO_STREAM_CORE_SYMBOLS",
    )
    crypto_stream_venues_csv: str = Field(
        default="coinbase,binance_spot,binance_futures,kraken,bybit",
        alias="CRYPTO_STREAM_VENUES",
    )
    crypto_stream_target_ttl_minutes: int = Field(default=180, ge=30, le=1440, alias="CRYPTO_STREAM_TARGET_TTL_MINUTES")
    crypto_stream_max_dynamic_targets: int = Field(default=75, ge=1, le=200, alias="CRYPTO_STREAM_MAX_DYNAMIC_TARGETS")
    crypto_stream_refresh_seconds: int = Field(default=60, ge=15, le=600, alias="CRYPTO_STREAM_REFRESH_SECONDS")
    crypto_aggregation_seconds: int = Field(default=1, ge=1, le=60, alias="CRYPTO_AGGREGATION_SECONDS")
    crypto_order_book_depth: int = Field(default=20, ge=5, le=100, alias="CRYPTO_ORDER_BOOK_DEPTH")
    crypto_raw_capture_enabled: bool = Field(default=True, alias="CRYPTO_RAW_CAPTURE_ENABLED")
    crypto_raw_segment_minutes: int = Field(default=15, ge=5, le=60, alias="CRYPTO_RAW_SEGMENT_MINUTES")
    crypto_raw_core_enabled: bool = Field(default=False, alias="CRYPTO_RAW_CORE_ENABLED")
    crypto_full_pair_universe: bool = Field(default=True, alias="CRYPTO_FULL_PAIR_UNIVERSE")
    crypto_broad_observation_enabled: bool = Field(default=True, alias="CRYPTO_BROAD_OBSERVATION_ENABLED")
    crypto_broad_observation_seconds: int = Field(default=60, ge=5, le=300, alias="CRYPTO_BROAD_OBSERVATION_SECONDS")
    crypto_broad_buffer_seconds: int = Field(default=15, ge=5, le=60, alias="CRYPTO_BROAD_BUFFER_SECONDS")
    crypto_broad_buffer_minutes: int = Field(default=120, ge=15, le=360, alias="CRYPTO_BROAD_BUFFER_MINUTES")
    crypto_preserve_pretrigger_buffer: bool = Field(default=True, alias="CRYPTO_PRESERVE_PRETRIGGER_BUFFER")
    broad_crypto_trigger_enabled: bool = Field(default=True, alias="BROAD_CRYPTO_TRIGGER_ENABLED")
    # Backwards-compatible 5-minute single-venue threshold.
    broad_crypto_trigger_move_pct: float = Field(default=1.5, ge=0.1, alias="BROAD_CRYPTO_TRIGGER_MOVE_PCT")
    broad_crypto_trigger_window_minutes: int = Field(default=5, ge=1, le=60, alias="BROAD_CRYPTO_TRIGGER_WINDOW_MINUTES")
    broad_crypto_fast_window_minutes: int = Field(default=1, ge=1, le=15, alias="BROAD_CRYPTO_FAST_WINDOW_MINUTES")
    broad_crypto_fast_move_pct: float = Field(default=0.75, ge=0.1, alias="BROAD_CRYPTO_FAST_MOVE_PCT")
    broad_crypto_slow_window_minutes: int = Field(default=15, ge=5, le=120, alias="BROAD_CRYPTO_SLOW_WINDOW_MINUTES")
    broad_crypto_slow_move_pct: float = Field(default=2.5, ge=0.1, alias="BROAD_CRYPTO_SLOW_MOVE_PCT")
    broad_crypto_confirmation_count: int = Field(default=2, ge=2, le=5, alias="BROAD_CRYPTO_CONFIRMATION_COUNT")
    broad_crypto_confirmed_move_pct: float = Field(default=0.75, ge=0.1, alias="BROAD_CRYPTO_CONFIRMED_MOVE_PCT")
    broad_crypto_derivative_move_pct: float = Field(default=1.0, ge=0.1, alias="BROAD_CRYPTO_DERIVATIVE_MOVE_PCT")
    broad_crypto_spot_confirmation_pct: float = Field(default=0.35, ge=0.0, alias="BROAD_CRYPTO_SPOT_CONFIRMATION_PCT")
    broad_crypto_volume_move_pct: float = Field(default=0.5, ge=0.0, alias="BROAD_CRYPTO_VOLUME_MOVE_PCT")
    broad_crypto_volume_acceleration: float = Field(default=3.0, ge=1.0, alias="BROAD_CRYPTO_VOLUME_ACCELERATION")
    broad_crypto_confirmation_seconds: int = Field(default=45, ge=5, le=300, alias="BROAD_CRYPTO_CONFIRMATION_SECONDS")
    broad_crypto_trigger_cooldown_seconds: int = Field(default=300, ge=30, le=3600, alias="BROAD_CRYPTO_TRIGGER_COOLDOWN_SECONDS")
    broad_crypto_scanner_poll_seconds: int = Field(default=5, ge=1, le=60, alias="BROAD_CRYPTO_SCANNER_POLL_SECONDS")

    # Existing providers
    twelvedata_api_key: str = Field(default="", alias="TWELVEDATA_API_KEY")
    twelvedata_symbol_cap: int = Field(default=200, ge=1, alias="TWELVEDATA_SYMBOL_CAP")
    twelvedata_requests_per_minute: int = Field(default=8, ge=1, alias="TWELVEDATA_REQUESTS_PER_MINUTE")
    twelvedata_indicators_csv: str = Field(
        default=(
            "SPY,QQQ,IWM,DIA,VIX,DXY,GLD,SLV,USO,TLT,HYG,LQD,UUP,FXE,EWJ,EEM,"
            "BTC/USD,ETH/USD,EUR/USD,GBP/USD,USD/JPY,USD/CHF,AUD/USD,XAU/USD,XAG/USD"
        ),
        alias="TWELVEDATA_INDICATORS",
    )
    binance_pair_mode: str = Field(default="all", alias="BINANCE_PAIR_MODE")
    binance_quote_priority_csv: str = Field(default="USDT,USDC,FDUSD,BTC,ETH", alias="BINANCE_QUOTE_PRIORITY")
    coinbase_allowed_quotes_csv: str = Field(default="USD,USDC,GBP,EUR,BTC,ETH", alias="COINBASE_ALLOWED_QUOTES")
    kraken_quote_priority_csv: str = Field(default="USD,USDT,USDC,GBP,EUR", alias="KRAKEN_QUOTE_PRIORITY")

    # Worker/reliability
    db_pool_size: int = Field(default=4, ge=2, le=20, alias="DB_POOL_SIZE")
    worker_poll_seconds: float = Field(default=2.0, ge=0.2, alias="WORKER_POLL_SECONDS")
    stale_partition_minutes: int = Field(default=15, ge=2, alias="STALE_PARTITION_MINUTES")
    collection_end_lag_minutes: int = Field(default=5, ge=0, alias="COLLECTION_END_LAG_MINUTES")
    max_partition_attempts: int = Field(default=8, ge=1, le=30, alias="MAX_PARTITION_ATTEMPTS")
    http_timeout_seconds: float = Field(default=60, ge=5, alias="HTTP_TIMEOUT_SECONDS")
    storage_upload_chunk_mb: int = Field(default=6, ge=1, le=50, alias="STORAGE_UPLOAD_CHUNK_MB")
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")

    @staticmethod
    def _csv(value: str) -> list[str]:
        return [item.strip() for item in value.split(",") if item.strip()]

    @property
    def twelvedata_indicators(self) -> list[str]:
        return self._csv(self.twelvedata_indicators_csv)

    @property
    def binance_quote_priority(self) -> list[str]:
        return [item.upper() for item in self._csv(self.binance_quote_priority_csv)]

    @property
    def coinbase_allowed_quotes(self) -> list[str]:
        return [item.upper() for item in self._csv(self.coinbase_allowed_quotes_csv)]

    @property
    def kraken_quote_priority(self) -> list[str]:
        return [item.upper() for item in self._csv(self.kraken_quote_priority_csv)]

    @property
    def crypto_stream_core_symbols(self) -> list[str]:
        return [item.upper() for item in self._csv(self.crypto_stream_core_symbols_csv)]

    @property
    def crypto_stream_venues(self) -> list[str]:
        return [item.lower() for item in self._csv(self.crypto_stream_venues_csv)]

    @field_validator("database_url")
    @classmethod
    def normalise_database_url(cls, value: str) -> str:
        if value.startswith("postgres://"):
            value = "postgresql://" + value[len("postgres://"):]
        return value

    @field_validator("alpaca_feed")
    @classmethod
    def validate_alpaca_feed(cls, value: str) -> str:
        value = value.lower().strip()
        if value not in {"sip", "iex", "otc", "boats"}:
            raise ValueError("ALPACA_FEED must be sip, iex, otc or boats")
        return value

    @field_validator("equity_context_scope")
    @classmethod
    def validate_equity_context_scope(cls, value: str) -> str:
        value = value.lower().strip()
        if value not in {"all", "captured"}:
            raise ValueError("EQUITY_CONTEXT_SCOPE must be all or captured")
        return value

    def validate_web(self) -> None:
        missing: list[str] = []
        if not self.app_password:
            missing.append("APP_PASSWORD")
        if self.session_secret == "change-me":
            missing.append("SESSION_SECRET")
        if missing:
            raise RuntimeError(f"Missing or insecure web settings: {', '.join(missing)}")

    def validate_worker(self) -> None:
        missing: list[str] = []
        for name, value in [
            ("ALPACA_API_KEY", self.alpaca_api_key),
            ("ALPACA_API_SECRET", self.alpaca_api_secret),
            ("TWELVEDATA_API_KEY", self.twelvedata_api_key),
        ]:
            if not value:
                missing.append(name)
        if self.equity_enrichment_enabled:
            if not self.massive_api_key:
                missing.append("MASSIVE_API_KEY")
            if not self.sec_user_agent:
                missing.append("SEC_USER_AGENT")
        if missing:
            raise RuntimeError(f"Missing worker settings: {', '.join(missing)}")

    def validate_crypto_stream(self) -> None:
        if not self.crypto_stream_enabled:
            raise RuntimeError("CRYPTO_STREAM_ENABLED=false")
        if not self.supabase_url or not self.supabase_service_role_key:
            raise RuntimeError("Supabase credentials are required for raw crypto capture")


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
