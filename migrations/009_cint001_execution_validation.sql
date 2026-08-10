-- C-INT-001 execution validation layer.
-- This is isolated from earlier strategy research. It stores only the frozen
-- blank-canvas candidate's execution-validation contract, raw USD-M execution
-- evidence, and audit outputs. The final holdout is explicitly sealed in run state.

create table if not exists crypto_futures_15m_binance (
    venue_symbol text not null,
    canonical_symbol text not null,
    bucket_start timestamptz not null,
    open double precision not null,
    high double precision not null,
    low double precision not null,
    close double precision not null,
    volume double precision not null default 0,
    quote_volume double precision not null default 0,
    trade_count bigint not null default 0,
    source text not null,
    inserted_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    primary key(venue_symbol,bucket_start)
);
create index if not exists crypto_futures_15m_canonical_ts_idx
    on crypto_futures_15m_binance(canonical_symbol,bucket_start);
create index if not exists crypto_futures_15m_ts_brin
    on crypto_futures_15m_binance using brin(bucket_start);

create table if not exists crypto_futures_mark_15m_binance (
    venue_symbol text not null,
    canonical_symbol text not null,
    bucket_start timestamptz not null,
    open double precision not null,
    high double precision not null,
    low double precision not null,
    close double precision not null,
    source text not null,
    inserted_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    primary key(venue_symbol,bucket_start)
);
create index if not exists crypto_futures_mark_15m_canonical_ts_idx
    on crypto_futures_mark_15m_binance(canonical_symbol,bucket_start);

create table if not exists crypto_futures_funding_binance (
    venue_symbol text not null,
    canonical_symbol text not null,
    funding_ts timestamptz not null,
    funding_rate double precision not null,
    mark_price double precision,
    source text not null,
    inserted_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    primary key(venue_symbol,funding_ts)
);
create index if not exists crypto_futures_funding_canonical_ts_idx
    on crypto_futures_funding_binance(canonical_symbol,funding_ts);

create table if not exists cint001_execution_runs (
    id uuid primary key default gen_random_uuid(),
    name text not null,
    status text not null default 'queued' check(status in ('queued','running','completed','completed_with_errors','paused','cancelled')),
    stage text not null default 'archive_backfill',
    rule_version text not null,
    validation_start timestamptz not null,
    validation_end timestamptz not null,
    execution_spec jsonb not null,
    final_holdout_start timestamptz not null,
    final_holdout_end timestamptz not null,
    holdout_opened boolean not null default false,
    result_summary jsonb not null default '{}'::jsonb,
    error text,
    created_at timestamptz not null default now(),
    started_at timestamptz,
    completed_at timestamptz,
    updated_at timestamptz not null default now(),
    check(validation_start < validation_end),
    check(final_holdout_start < final_holdout_end)
);
create index if not exists cint001_execution_runs_status_idx on cint001_execution_runs(status,stage,created_at);

create table if not exists cint001_execution_work_items (
    id bigserial primary key,
    run_id uuid not null references cint001_execution_runs(id) on delete cascade,
    stage text not null check(stage in ('month','analysis')),
    partition_key text not null,
    payload jsonb not null default '{}'::jsonb,
    status text not null default 'queued' check(status in ('queued','running','retry_wait','completed','missing','failed','cancelled')),
    attempts integer not null default 0,
    max_attempts integer not null default 8,
    not_before timestamptz not null default now(),
    locked_by text,
    locked_at timestamptz,
    row_count bigint not null default 0,
    progress jsonb not null default '{}'::jsonb,
    last_error text,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    unique(run_id,stage,partition_key)
);
create index if not exists cint001_execution_work_claim_idx on cint001_execution_work_items(status,not_before,id);
create index if not exists cint001_execution_work_run_idx on cint001_execution_work_items(run_id,stage,status);

create table if not exists cint001_contract_months (
    run_id uuid not null references cint001_execution_runs(id) on delete cascade,
    spot_symbol text not null,
    futures_symbol text,
    period_start date not null,
    kline_available boolean not null default false,
    mark_available boolean not null default false,
    funding_available boolean not null default false,
    source_urls jsonb not null default '{}'::jsonb,
    checksums jsonb not null default '{}'::jsonb,
    updated_at timestamptz not null default now(),
    primary key(run_id,spot_symbol,period_start)
);
create index if not exists cint001_contract_months_futures_idx on cint001_contract_months(run_id,futures_symbol,period_start);

create table if not exists cint001_execution_trades (
    id bigserial primary key,
    run_id uuid not null references cint001_execution_runs(id) on delete cascade,
    signal_bucket timestamptz not null,
    signal_ts timestamptz not null,
    entry_ts timestamptz not null,
    exit_ts timestamptz not null,
    phase smallint not null check(phase between 0 and 95),
    spot_symbol text not null,
    futures_symbol text,
    r1h double precision not null,
    range15 double precision not null,
    q_r1h smallint not null,
    q_range smallint not null,
    selected_count integer not null,
    executable_count integer not null,
    panel_member_count integer not null,
    futures_short_return double precision,
    funding_return double precision,
    panel_long_return double precision,
    gross_relative_return double precision,
    spot_entry double precision,
    futures_entry double precision,
    futures_exit double precision,
    entry_basis_bps double precision,
    exit_basis_bps double precision,
    created_at timestamptz not null default now(),
    unique(run_id,signal_bucket,spot_symbol)
);
create index if not exists cint001_execution_trades_ts_idx on cint001_execution_trades(run_id,signal_bucket,phase);
create index if not exists cint001_execution_trades_symbol_idx on cint001_execution_trades(run_id,spot_symbol,signal_bucket);

create table if not exists cint001_execution_results (
    run_id uuid not null references cint001_execution_runs(id) on delete cascade,
    result_scope text not null,
    metrics jsonb not null,
    created_at timestamptz not null default now(),
    primary key(run_id,result_scope)
);

alter table crypto_futures_15m_binance enable row level security;
alter table crypto_futures_mark_15m_binance enable row level security;
alter table crypto_futures_funding_binance enable row level security;
alter table cint001_execution_runs enable row level security;
alter table cint001_execution_work_items enable row level security;
alter table cint001_contract_months enable row level security;
alter table cint001_execution_trades enable row level security;
alter table cint001_execution_results enable row level security;
