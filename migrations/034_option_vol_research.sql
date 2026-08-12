create table if not exists public.option_vol_research_runs (
    id uuid primary key default gen_random_uuid(),
    name text not null,
    status text not null default 'queued' check (status in ('queued','running','paused','completed','completed_with_errors','failed','cancelled')),
    stage text not null default 'event_backfill',
    underlying_symbol text not null default 'SPY',
    signal_threshold double precision not null,
    requested_start timestamptz not null,
    requested_end timestamptz not null,
    dte_buckets jsonb not null,
    control_spec jsonb not null,
    execution_spec jsonb not null,
    events_planned integer not null default 0,
    events_completed integer not null default 0,
    events_failed integer not null default 0,
    error text,
    created_at timestamptz not null default now(),
    started_at timestamptz,
    completed_at timestamptz,
    updated_at timestamptz not null default now()
);

create table if not exists public.option_vol_research_events (
    id uuid primary key default gen_random_uuid(),
    run_id uuid not null references public.option_vol_research_runs(id) on delete cascade,
    bucket_start timestamptz not null,
    entry_ts timestamptz not null,
    exit_ts timestamptz not null,
    sample_class text not null check (sample_class in ('high','control')),
    slot_et text not null,
    mean_crypto_range double precision not null,
    spy_open double precision,
    status text not null default 'queued' check (status in ('queued','running','completed','failed','skipped','retry_wait')),
    attempts integer not null default 0,
    max_attempts integer not null default 5,
    not_before timestamptz not null default now(),
    locked_by text,
    locked_at timestamptz,
    last_error text,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    unique(run_id,bucket_start,sample_class)
);

create index if not exists option_vol_events_claim_idx
    on public.option_vol_research_events(status,not_before,created_at)
    where status in ('queued','retry_wait');
create index if not exists option_vol_events_run_idx
    on public.option_vol_research_events(run_id,sample_class,bucket_start);

create table if not exists public.option_vol_research_contracts (
    event_id uuid not null references public.option_vol_research_events(id) on delete cascade,
    dte_bucket text not null,
    expiration_date date not null,
    strike double precision not null,
    call_symbol text not null,
    put_symbol text not null,
    call_open_interest bigint,
    put_open_interest bigint,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    primary key(event_id,dte_bucket)
);

create table if not exists public.option_vol_research_bars (
    event_id uuid not null references public.option_vol_research_events(id) on delete cascade,
    contract_symbol text not null,
    ts timestamptz not null,
    open double precision,
    high double precision,
    low double precision,
    close double precision,
    volume double precision,
    trade_count bigint,
    vwap double precision,
    created_at timestamptz not null default now(),
    primary key(event_id,contract_symbol,ts)
);

create table if not exists public.option_vol_research_results (
    event_id uuid not null references public.option_vol_research_events(id) on delete cascade,
    dte_bucket text not null,
    entry_call double precision,
    entry_put double precision,
    entry_straddle double precision,
    exit_call double precision,
    exit_put double precision,
    exit_straddle double precision,
    gross_return double precision,
    premium_to_spy double precision,
    spy_range_30m double precision,
    complete boolean not null default false,
    notes jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    primary key(event_id,dte_bucket)
);

-- These research tables are server-side only. RLS is enabled by default so
-- anon/authenticated Data API roles cannot read or mutate them without an
-- explicit future policy. The backend's database/service role performs the work.
alter table public.option_vol_research_runs enable row level security;
alter table public.option_vol_research_events enable row level security;
alter table public.option_vol_research_contracts enable row level security;
alter table public.option_vol_research_bars enable row level security;
alter table public.option_vol_research_results enable row level security;
