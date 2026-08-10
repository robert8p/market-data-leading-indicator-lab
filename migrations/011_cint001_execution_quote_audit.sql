-- C-INT-001 causally strict execution audit materialisations and historical top-of-book evidence.
-- This migration is additive and idempotent. It does not open or populate the final holdout.

create table if not exists cint001_signal_selection (
    run_id uuid not null references cint001_execution_runs(id) on delete cascade,
    signal_bucket timestamptz not null,
    signal_ts timestamptz not null,
    spot_symbol text not null,
    r1h double precision not null,
    range15 double precision not null,
    q_r1h smallint not null,
    q_range smallint not null,
    selected_count integer not null,
    entry_ts timestamptz not null,
    exit_ts timestamptz not null,
    futures_symbol text,
    created_at timestamptz not null default now(),
    primary key(run_id,signal_bucket,spot_symbol)
);
create index if not exists cint001_signal_selection_ts_idx
    on cint001_signal_selection(run_id,signal_bucket);

create table if not exists cint001_panel_returns (
    run_id uuid not null references cint001_execution_runs(id) on delete cascade,
    signal_bucket timestamptz not null,
    entry_ts timestamptz not null,
    exit_ts timestamptz not null,
    member_count integer not null,
    panel_long_return double precision not null,
    created_at timestamptz not null default now(),
    primary key(run_id,signal_bucket)
);
create index if not exists cint001_panel_returns_ts_idx
    on cint001_panel_returns(run_id,signal_bucket);

create table if not exists cint001_bookticker_days (
    id bigserial primary key,
    run_id uuid not null references cint001_execution_runs(id) on delete cascade,
    futures_symbol text not null,
    trade_date date not null,
    priority integer not null default 10,
    target_count integer not null default 0,
    status text not null default 'queued'
        check(status in ('queued','running','completed','missing','retry_wait','failed')),
    attempts integer not null default 0,
    max_attempts integer not null default 5,
    not_before timestamptz not null default now(),
    locked_by text,
    locked_at timestamptz,
    row_count bigint not null default 0,
    checksum text,
    schema_info jsonb not null default '{}'::jsonb,
    last_error text,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    unique(run_id,futures_symbol,trade_date)
);
create index if not exists cint001_bookticker_days_claim_idx
    on cint001_bookticker_days(status,not_before,priority,id);

create table if not exists cint001_bookticker_snapshots (
    run_id uuid not null references cint001_execution_runs(id) on delete cascade,
    futures_symbol text not null,
    target_ts timestamptz not null,
    quote_ts timestamptz not null,
    bid_price double precision not null,
    bid_qty double precision,
    ask_price double precision not null,
    ask_qty double precision,
    age_ms double precision not null,
    timing_relation text not null,
    source_date date not null,
    source text not null default 'binance_data_vision_bookTicker',
    inserted_at timestamptz not null default now(),
    primary key(run_id,futures_symbol,target_ts),
    check(bid_price>0 and ask_price>=bid_price)
);
create index if not exists cint001_bookticker_snapshots_target_idx
    on cint001_bookticker_snapshots(run_id,target_ts,futures_symbol);

alter table cint001_bookticker_snapshots
    drop constraint if exists cint001_bookticker_snapshots_timing_relation_check;
alter table cint001_bookticker_snapshots
    add constraint cint001_bookticker_snapshots_timing_relation_check
    check(timing_relation in ('at_or_after'));

alter table cint001_signal_selection enable row level security;
alter table cint001_panel_returns enable row level security;
alter table cint001_bookticker_days enable row level security;
alter table cint001_bookticker_snapshots enable row level security;
