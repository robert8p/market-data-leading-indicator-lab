-- Tardis free first-of-month top-25 order-book samples for C-INT-001 execution capacity red-team.
-- Additive only; does not alter the signal or open the final holdout.

create table if not exists cint001_depth_days (
    id bigserial primary key,
    run_id uuid not null references cint001_execution_runs(id) on delete cascade,
    futures_symbol text not null,
    trade_date date not null,
    priority integer not null default 10,
    target_count integer not null default 0,
    status text not null default 'queued'
        check(status in ('queued','running','completed','missing','retry_wait','failed')),
    attempts integer not null default 0,
    max_attempts integer not null default 4,
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
create index if not exists cint001_depth_days_claim_idx
    on cint001_depth_days(status,not_before,priority,id);

create table if not exists cint001_depth_snapshots (
    run_id uuid not null references cint001_execution_runs(id) on delete cascade,
    futures_symbol text not null,
    target_ts timestamptz not null,
    snapshot_ts timestamptz not null,
    local_ts timestamptz,
    age_ms double precision not null,
    bids jsonb not null,
    asks jsonb not null,
    source_date date not null,
    source text not null default 'tardis_book_snapshot_25_free_sample',
    inserted_at timestamptz not null default now(),
    primary key(run_id,futures_symbol,target_ts)
);
create index if not exists cint001_depth_snapshots_target_idx
    on cint001_depth_snapshots(run_id,target_ts,futures_symbol);

alter table cint001_depth_days enable row level security;
alter table cint001_depth_snapshots enable row level security;
