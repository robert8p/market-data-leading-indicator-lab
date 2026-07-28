-- v3.3.0: miner-first, full-universe coverage and unbiased acquisition support.

alter table capture_windows
    add column if not exists selection_class text not null default 'anomaly',
    add column if not exists admission_status text not null default 'pending',
    add column if not exists admission_reason text;

create index if not exists capture_windows_selection_idx
    on capture_windows(run_id, selection_class, admission_status, trigger_ts);

create table if not exists capture_decisions (
    id bigserial primary key,
    run_id uuid not null references collection_runs(id) on delete cascade,
    provider text not null,
    asset_class text not null,
    instrument_id uuid not null references instruments(id) on delete cascade,
    provider_symbol text not null,
    canonical_symbol text not null,
    observed_at timestamptz not null,
    window_start timestamptz not null,
    window_end timestamptz not null,
    selection_class text not null,
    trigger_kind text not null,
    trigger_value double precision,
    admitted boolean not null,
    admission_reason text not null,
    rule_version text not null,
    reason jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    unique(run_id, provider, instrument_id, observed_at, trigger_kind, rule_version)
);
create index if not exists capture_decisions_run_admitted_idx
    on capture_decisions(run_id, admitted, selection_class, observed_at);
create index if not exists capture_decisions_instrument_ts_idx
    on capture_decisions(instrument_id, observed_at);

create table if not exists equity_microstructure_1m (
    provider text not null default 'alpaca',
    instrument_id uuid not null references instruments(id) on delete cascade,
    ts timestamptz not null,
    trade_count bigint not null default 0,
    buy_trade_count bigint not null default 0,
    sell_trade_count bigint not null default 0,
    unknown_trade_count bigint not null default 0,
    buy_volume double precision not null default 0,
    sell_volume double precision not null default 0,
    unknown_volume double precision not null default 0,
    total_volume double precision not null default 0,
    total_notional double precision not null default 0,
    vwap double precision,
    first_trade_price double precision,
    last_trade_price double precision,
    high_trade_price double precision,
    low_trade_price double precision,
    quote_count bigint not null default 0,
    avg_bid_price double precision,
    avg_ask_price double precision,
    avg_bid_size double precision,
    avg_ask_size double precision,
    avg_spread double precision,
    avg_spread_bps double precision,
    min_spread_bps double precision,
    max_spread_bps double precision,
    last_bid_price double precision,
    last_ask_price double precision,
    last_bid_size double precision,
    last_ask_size double precision,
    metadata jsonb not null default '{}'::jsonb,
    inserted_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    primary key(provider, instrument_id, ts)
);
create index if not exists equity_microstructure_instrument_ts_idx
    on equity_microstructure_1m(instrument_id, ts);
create index if not exists equity_microstructure_ts_brin
    on equity_microstructure_1m using brin(ts);

create table if not exists crypto_market_observations_1m (
    provider text not null,
    market_type text not null,
    venue_symbol text not null,
    canonical_symbol text not null,
    quote_asset text,
    ts timestamptz not null,
    last_price double precision not null,
    quote_volume_24h double precision,
    bid_price double precision,
    ask_price double precision,
    metadata jsonb not null default '{}'::jsonb,
    inserted_at timestamptz not null default now(),
    primary key(provider, market_type, venue_symbol, ts)
);
create index if not exists crypto_market_observations_symbol_ts_idx
    on crypto_market_observations_1m(canonical_symbol, ts);
create index if not exists crypto_market_observations_ts_brin
    on crypto_market_observations_1m using brin(ts);

create table if not exists market_universe_snapshots (
    provider text not null,
    snapshot_ts timestamptz not null,
    asset_class text not null,
    tradable_count integer not null,
    preferred_count integer not null,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    primary key(provider, snapshot_ts, asset_class)
);
create index if not exists market_universe_snapshots_provider_ts_idx
    on market_universe_snapshots(provider, snapshot_ts desc);
