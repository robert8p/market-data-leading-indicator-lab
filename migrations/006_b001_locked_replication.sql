-- v3.4.0: locked historical replication infrastructure for B-001.
-- Additive only. Existing collector and June/July 2026 discovery tables are untouched.

create table if not exists crypto_b001_replication_runs (
    id uuid primary key default gen_random_uuid(),
    name text not null,
    status text not null default 'queued' check (status in ('queued','running','paused','completed','completed_with_errors','failed','cancelled')),
    stage text not null default 'queued',
    rule_version text not null default 'B-001-frozen-2026-08-08',
    code_version text,
    requested_start timestamptz not null,
    requested_end timestamptz not null,
    effective_start timestamptz,
    effective_end timestamptz,
    discovery_start timestamptz not null default '2026-06-28 00:00:00+00',
    discovery_end timestamptz not null default '2026-07-28 16:30:00+00',
    target_months integer not null default 24,
    minimum_months integer not null default 12,
    liquidity_method text not null default 'trailing_18d_pre_signal_avg_quote_volume_percent_rank_top_half',
    liquidity_difference_note text not null default 'Discovery used a fixed average quote-volume ranking over 2026-06-28 to before 2026-07-16. Historical replication uses the mechanically closest no-look-ahead equivalent: trailing 18 calendar days of completed 15-minute bars strictly before T, then the same cross-sectional percent_rank and >=0.50 cutoff.',
    exact_thresholds jsonb not null,
    execution_spec jsonb not null,
    cost_spec jsonb not null,
    archive_files_planned bigint not null default 0,
    archive_files_completed bigint not null default 0,
    archive_files_missing bigint not null default 0,
    one_minute_rows bigint not null default 0,
    complete_15m_rows bigint not null default 0,
    incomplete_15m_buckets bigint not null default 0,
    symbols_loaded integer not null default 0,
    completeness_pct double precision,
    primary_signal_count bigint,
    classification text check (classification is null or classification in ('A','B','C','D')),
    classification_reason text,
    error text,
    created_at timestamptz not null default now(),
    started_at timestamptz,
    completed_at timestamptz,
    updated_at timestamptz not null default now(),
    check (requested_start < requested_end),
    check (requested_end < discovery_start)
);
create index if not exists crypto_b001_runs_created_idx on crypto_b001_replication_runs(created_at desc);
create index if not exists crypto_b001_runs_status_idx on crypto_b001_replication_runs(status,stage,created_at);

create table if not exists crypto_b001_replication_work_items (
    id bigserial primary key,
    run_id uuid not null references crypto_b001_replication_runs(id) on delete cascade,
    stage text not null,
    partition_key text not null,
    payload jsonb not null default '{}'::jsonb,
    status text not null default 'queued' check (status in ('queued','running','retry_wait','completed','missing','failed','cancelled')),
    attempts integer not null default 0,
    max_attempts integer not null default 8,
    not_before timestamptz not null default now(),
    locked_by text,
    locked_at timestamptz,
    row_count bigint not null default 0,
    progress jsonb not null default '{}'::jsonb,
    last_error text,
    error_code text,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    unique(run_id,stage,partition_key)
);
create index if not exists crypto_b001_work_claim_idx on crypto_b001_replication_work_items(status,not_before,id);
create index if not exists crypto_b001_work_run_stage_idx on crypto_b001_replication_work_items(run_id,stage,status);

create table if not exists crypto_b001_replication_archive_files (
    run_id uuid not null references crypto_b001_replication_runs(id) on delete cascade,
    symbol text not null,
    market_type text not null default 'spot',
    interval text not null default '1m',
    period_start date not null,
    period_end date not null,
    source_url text not null,
    checksum_url text,
    source_checksum text,
    computed_checksum text,
    checksum_verified boolean,
    storage_object_path text,
    storage_size_bytes bigint,
    source_status text not null default 'planned',
    row_count bigint not null default 0,
    rows_in_replication_window bigint not null default 0,
    first_ts timestamptz,
    last_ts timestamptz,
    complete_15m_count bigint not null default 0,
    incomplete_15m_count bigint not null default 0,
    missing_minute_count bigint not null default 0,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    primary key(run_id,symbol,market_type,interval,period_start)
);
create index if not exists crypto_b001_archive_symbol_idx on crypto_b001_replication_archive_files(run_id,symbol,period_start);

create table if not exists crypto_b001_replication_15m (
    run_id uuid not null references crypto_b001_replication_runs(id) on delete cascade,
    symbol text not null,
    bucket_start timestamptz not null,
    signal_ts timestamptz not null,
    minute_count integer not null,
    open double precision not null,
    high double precision not null,
    low double precision not null,
    close double precision not null,
    volume double precision not null,
    quote_volume double precision not null,
    trade_count bigint not null,
    taker_buy_quote_volume double precision not null,
    final_5m_return double precision,
    intrabar_vwap double precision,
    close_vs_vwap double precision,
    high_to_close_rejection double precision,
    first_minute_ts timestamptz not null,
    last_minute_ts timestamptz not null,
    source_period_start date not null,
    created_at timestamptz not null default now(),
    primary key(run_id,symbol,bucket_start),
    check (minute_count = 15),
    check (signal_ts = bucket_start + interval '15 minutes')
);
create index if not exists crypto_b001_15m_ts_idx on crypto_b001_replication_15m(run_id,bucket_start,symbol);
create index if not exists crypto_b001_15m_symbol_ts_idx on crypto_b001_replication_15m(run_id,symbol,bucket_start);

create table if not exists crypto_b001_replication_features (
    run_id uuid not null references crypto_b001_replication_runs(id) on delete cascade,
    symbol text not null,
    bucket_start timestamptz not null,
    signal_ts timestamptz not null,
    open double precision not null,
    high double precision not null,
    low double precision not null,
    close double precision not null,
    quote_volume double precision not null,
    trade_count bigint not null,
    taker_buy_quote_volume double precision not null,
    ret15 double precision,
    ret30 double precision,
    ret60 double precision,
    ret240 double precision,
    qv_accel1 double precision,
    qv_ratio4 double precision,
    qv_ratio16 double precision,
    trade_accel1 double precision,
    trade_ratio4 double precision,
    trade_ratio16 double precision,
    buy_share15 double precision,
    range15 double precision,
    pos_vs_high4h double precision,
    pos_vs_low4h double precision,
    pos_vs_high1h double precision,
    pos_vs_low1h double precision,
    body_efficiency double precision,
    upper_wick_share double precision,
    lower_wick_share double precision,
    ret_accel15 double precision,
    rv1h double precision,
    rv4h double precision,
    trailing_liquidity_avg_qv double precision,
    liquidity_pct double precision,
    liquidity_eligible boolean not null default false,
    final_5m_return double precision,
    intrabar_vwap double precision,
    close_vs_vwap double precision,
    high_to_close_rejection double precision,
    created_at timestamptz not null default now(),
    primary key(run_id,symbol,bucket_start)
);
create index if not exists crypto_b001_features_ts_idx on crypto_b001_replication_features(run_id,bucket_start,symbol);
create index if not exists crypto_b001_features_eligible_idx on crypto_b001_replication_features(run_id,bucket_start) where liquidity_eligible;

create table if not exists crypto_b001_replication_market_state (
    run_id uuid not null references crypto_b001_replication_runs(id) on delete cascade,
    bucket_start timestamptz not null,
    n_symbols integer not null,
    breadth_up double precision,
    mean_ret15 double precision,
    median_ret15 double precision,
    dispersion15 double precision,
    p10_ret15 double precision,
    p90_ret15 double precision,
    mean_range15 double precision,
    btc_ret15 double precision,
    btc_ret60 double precision,
    eth_ret15 double precision,
    eth_ret60 double precision,
    created_at timestamptz not null default now(),
    primary key(run_id,bucket_start)
);

create table if not exists crypto_b001_replication_shortability (
    run_id uuid not null references crypto_b001_replication_runs(id) on delete cascade,
    symbol text not null,
    period_start date not null,
    spot_data_present boolean not null default false,
    spot_trading_status text,
    margin_enabled boolean,
    margin_evidence_source text,
    perpetual_available boolean,
    perpetual_evidence_source text,
    spread_bps double precision,
    spread_evidence_source text,
    metadata jsonb not null default '{}'::jsonb,
    checked_at timestamptz not null default now(),
    primary key(run_id,symbol,period_start)
);

create table if not exists crypto_b001_replication_signals (
    id bigserial primary key,
    run_id uuid not null references crypto_b001_replication_runs(id) on delete cascade,
    symbol text not null,
    bucket_start timestamptz not null,
    signal_ts timestamptz not null,
    chronological_block smallint not null check (chronological_block between 1 and 3),
    range15 double precision not null,
    pos_vs_low4h double precision not null,
    qv_ratio16 double precision not null,
    range15_pct double precision not null,
    pos_vs_low4h_pct double precision not null,
    qv_ratio16_pct double precision not null,
    extreme_t boolean not null,
    extreme_t15 boolean not null,
    extreme_t30 boolean not null,
    extreme_t75 boolean not null,
    ret15 double precision not null,
    previous_range15 double precision not null,
    final_5m_return double precision,
    high_to_close_rejection double precision,
    close_vs_vwap double precision,
    minute_rejection_a boolean not null,
    minute_rejection_b boolean not null,
    minute_rejection_c boolean not null,
    minute_rejection_count smallint not null,
    dispersion15 double precision not null,
    trailing_liquidity_avg_qv double precision,
    liquidity_pct double precision not null,
    research_eligible boolean not null default true,
    spot_trading_status text,
    margin_enabled boolean,
    perpetual_available boolean,
    spread_bps double precision,
    historically_executable boolean,
    shortability_reason text,
    created_at timestamptz not null default now(),
    unique(run_id,symbol,bucket_start)
);
create index if not exists crypto_b001_signals_ts_idx on crypto_b001_replication_signals(run_id,bucket_start,symbol);
create index if not exists crypto_b001_signals_exec_idx on crypto_b001_replication_signals(run_id,historically_executable,bucket_start);

create table if not exists crypto_b001_replication_trades (
    id bigserial primary key,
    run_id uuid not null references crypto_b001_replication_runs(id) on delete cascade,
    signal_id bigint not null references crypto_b001_replication_signals(id) on delete cascade,
    symbol text not null,
    chronological_block smallint not null,
    structure text not null check (structure in ('B-001a','B-001b','B-001c')),
    position_mode text not null check (position_mode in ('signal_level','portfolio')),
    execution_subset text not null check (execution_subset in ('research','historically_executable')),
    cost_bp double precision not null,
    entry_ts timestamptz not null,
    exit_ts timestamptz not null,
    token_entry double precision not null,
    token_exit double precision not null,
    btc_entry double precision,
    btc_exit double precision,
    basket_entry_value double precision,
    basket_exit_value double precision,
    token_gross_return double precision not null,
    hedge_gross_return double precision not null default 0,
    gross_return double precision not null,
    transaction_cost double precision not null,
    net_return double precision not null,
    mae double precision,
    mfe double precision,
    concurrency integer,
    ignored_overlap boolean not null default false,
    created_at timestamptz not null default now(),
    unique(run_id,signal_id,structure,position_mode,execution_subset,cost_bp)
);
create index if not exists crypto_b001_trades_metrics_idx on crypto_b001_replication_trades(run_id,structure,position_mode,execution_subset,cost_bp,chronological_block);

create table if not exists crypto_b001_replication_metrics (
    id bigserial primary key,
    run_id uuid not null references crypto_b001_replication_runs(id) on delete cascade,
    structure text not null,
    position_mode text not null,
    execution_subset text not null,
    cost_bp double precision not null,
    block text not null,
    metrics jsonb not null,
    created_at timestamptz not null default now(),
    unique(run_id,structure,position_mode,execution_subset,cost_bp,block)
);

create table if not exists crypto_b001_replication_placebos (
    id bigserial primary key,
    run_id uuid not null references crypto_b001_replication_runs(id) on delete cascade,
    placebo_type text not null,
    variant text not null,
    block text not null default 'aggregate',
    metrics jsonb not null,
    details jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    unique(run_id,placebo_type,variant,block)
);

create table if not exists crypto_b001_replication_robustness (
    id bigserial primary key,
    run_id uuid not null references crypto_b001_replication_runs(id) on delete cascade,
    robustness_type text not null,
    variant text not null,
    label text not null default 'POST-REPLICATION ROBUSTNESS — NOT PRIMARY TEST',
    metrics jsonb not null,
    parameters jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    unique(run_id,robustness_type,variant)
);

create table if not exists crypto_b001_replication_qa (
    id bigserial primary key,
    run_id uuid not null references crypto_b001_replication_runs(id) on delete cascade,
    check_number integer not null,
    check_name text not null,
    passed boolean not null,
    details jsonb not null default '{}'::jsonb,
    checked_at timestamptz not null default now(),
    unique(run_id,check_number)
);

create table if not exists crypto_b001_replication_exports (
    id bigserial primary key,
    run_id uuid not null references crypto_b001_replication_runs(id) on delete cascade,
    export_type text not null,
    storage_object_path text not null,
    size_bytes bigint,
    sha256 text,
    created_at timestamptz not null default now(),
    unique(run_id,export_type,storage_object_path)
);

-- These tables are server-side research surfaces. Keep the Data API closed unless a deliberate policy is added later.
alter table crypto_b001_replication_runs enable row level security;
alter table crypto_b001_replication_work_items enable row level security;
alter table crypto_b001_replication_archive_files enable row level security;
alter table crypto_b001_replication_15m enable row level security;
alter table crypto_b001_replication_features enable row level security;
alter table crypto_b001_replication_market_state enable row level security;
alter table crypto_b001_replication_shortability enable row level security;
alter table crypto_b001_replication_signals enable row level security;
alter table crypto_b001_replication_trades enable row level security;
alter table crypto_b001_replication_metrics enable row level security;
alter table crypto_b001_replication_placebos enable row level security;
alter table crypto_b001_replication_robustness enable row level security;
alter table crypto_b001_replication_qa enable row level security;
alter table crypto_b001_replication_exports enable row level security;
