-- v3.0.1: prospective crypto microstructure, derivatives and supply mining.

create table if not exists crypto_venue_symbols (
    provider text not null,
    market_type text not null,
    venue_symbol text not null,
    canonical_symbol text not null,
    base_asset text,
    quote_asset text,
    status text,
    tradable boolean not null default true,
    priority integer not null default 0,
    metadata jsonb not null default '{}'::jsonb,
    first_seen_at timestamptz not null default now(),
    last_seen_at timestamptz not null default now(),
    primary key(provider, market_type, venue_symbol)
);
create index if not exists crypto_venue_symbols_canonical_idx
    on crypto_venue_symbols(canonical_symbol, provider, market_type, priority desc);

create table if not exists crypto_capture_targets (
    canonical_symbol text primary key,
    source text not null,
    reason jsonb not null default '{}'::jsonb,
    activated_at timestamptz not null default now(),
    expires_at timestamptz not null,
    updated_at timestamptz not null default now()
);
create index if not exists crypto_capture_targets_expiry_idx on crypto_capture_targets(expires_at);

create table if not exists crypto_microstructure_1s (
    provider text not null,
    market_type text not null,
    venue_symbol text not null,
    canonical_symbol text not null,
    ts timestamptz not null,
    trade_count bigint not null default 0,
    buy_count bigint not null default 0,
    sell_count bigint not null default 0,
    buy_base_volume double precision not null default 0,
    sell_base_volume double precision not null default 0,
    buy_quote_volume double precision not null default 0,
    sell_quote_volume double precision not null default 0,
    last_trade_price double precision,
    bid_price double precision,
    bid_size double precision,
    ask_price double precision,
    ask_size double precision,
    spread double precision,
    spread_bps double precision,
    mid_price double precision,
    microprice double precision,
    bid_depth double precision,
    ask_depth double precision,
    depth_imbalance double precision,
    weighted_bid_price double precision,
    weighted_ask_price double precision,
    book_update_count bigint not null default 0,
    mark_price double precision,
    index_price double precision,
    funding_rate double precision,
    next_funding_at timestamptz,
    open_interest double precision,
    open_interest_value double precision,
    liquidation_buy_notional double precision not null default 0,
    liquidation_sell_notional double precision not null default 0,
    metadata jsonb not null default '{}'::jsonb,
    inserted_at timestamptz not null default now(),
    primary key(provider, market_type, venue_symbol, ts)
);
create index if not exists crypto_microstructure_symbol_ts_idx
    on crypto_microstructure_1s(canonical_symbol, ts);
create index if not exists crypto_microstructure_ts_brin
    on crypto_microstructure_1s using brin(ts);

create table if not exists crypto_derivatives_metrics (
    provider text not null,
    venue_symbol text not null,
    canonical_symbol text not null,
    ts timestamptz not null,
    interval text not null,
    mark_price double precision,
    index_price double precision,
    funding_rate double precision,
    next_funding_at timestamptz,
    open_interest double precision,
    open_interest_value double precision,
    global_long_short_ratio double precision,
    top_account_long_short_ratio double precision,
    top_position_long_short_ratio double precision,
    taker_buy_sell_ratio double precision,
    basis double precision,
    metadata jsonb not null default '{}'::jsonb,
    inserted_at timestamptz not null default now(),
    primary key(provider, venue_symbol, ts, interval)
);
create index if not exists crypto_derivatives_canonical_ts_idx
    on crypto_derivatives_metrics(canonical_symbol, ts);
create index if not exists crypto_derivatives_ts_brin
    on crypto_derivatives_metrics using brin(ts);

create table if not exists crypto_liquidations (
    provider text not null,
    venue_symbol text not null,
    canonical_symbol text not null,
    event_id text not null,
    ts timestamptz not null,
    side text,
    price double precision,
    quantity double precision,
    notional double precision,
    metadata jsonb not null default '{}'::jsonb,
    inserted_at timestamptz not null default now(),
    primary key(provider, venue_symbol, event_id)
);
create index if not exists crypto_liquidations_symbol_ts_idx
    on crypto_liquidations(canonical_symbol, ts);

create table if not exists crypto_supply_snapshots (
    source text not null,
    source_id text not null,
    canonical_symbol text not null,
    name text,
    asof_ts timestamptz not null,
    current_price double precision,
    market_cap double precision,
    fully_diluted_valuation double precision,
    total_volume_24h double precision,
    circulating_supply double precision,
    total_supply double precision,
    max_supply double precision,
    ath double precision,
    atl double precision,
    metadata jsonb not null default '{}'::jsonb,
    inserted_at timestamptz not null default now(),
    primary key(source, source_id, asof_ts)
);
create index if not exists crypto_supply_symbol_asof_idx
    on crypto_supply_snapshots(canonical_symbol, asof_ts desc);

create table if not exists crypto_raw_objects (
    id uuid primary key default gen_random_uuid(),
    provider text not null,
    market_type text not null,
    venue_symbol text not null,
    canonical_symbol text not null,
    channel text not null,
    start_ts timestamptz not null,
    end_ts timestamptz not null,
    object_path text not null unique,
    content_type text not null,
    compression text,
    message_count bigint not null default 0,
    size_bytes bigint,
    checksum text,
    status text not null default 'uploaded',
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now()
);
create index if not exists crypto_raw_objects_symbol_ts_idx
    on crypto_raw_objects(canonical_symbol, start_ts);

create table if not exists crypto_stream_sessions (
    id uuid primary key default gen_random_uuid(),
    worker_id text not null,
    started_at timestamptz not null default now(),
    stopped_at timestamptz,
    status text not null default 'running',
    config jsonb not null default '{}'::jsonb,
    last_heartbeat_at timestamptz not null default now(),
    message_count bigint not null default 0,
    flush_count bigint not null default 0,
    reconnect_count bigint not null default 0,
    last_error text
);
create index if not exists crypto_stream_sessions_heartbeat_idx
    on crypto_stream_sessions(last_heartbeat_at desc);

create table if not exists crypto_stream_gaps (
    id bigserial primary key,
    provider text not null,
    service text not null,
    venue_symbol text,
    detected_at timestamptz not null default now(),
    gap_start timestamptz,
    gap_end timestamptz,
    reason text not null,
    metadata jsonb not null default '{}'::jsonb
);
create index if not exists crypto_stream_gaps_detected_idx on crypto_stream_gaps(detected_at desc);
