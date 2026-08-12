create table if not exists research.xal_live_source_evaluations (
    candidate_id text not null references research.xal_candidates(candidate_id),
    source_bar_end_ts timestamptz not null,
    trade_date date not null,
    source_return double precision,
    previous_source_return double precision,
    threshold_value double precision not null,
    event_triggered boolean not null default false,
    source_feed text not null default 'alpaca_sip',
    source_payload jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    primary key (candidate_id, source_bar_end_ts)
);

create index if not exists xal_live_source_evaluations_date_idx
    on research.xal_live_source_evaluations(candidate_id, trade_date, source_bar_end_ts);

create table if not exists research.xal_live_signals (
    signal_id uuid primary key,
    candidate_id text not null references research.xal_candidates(candidate_id),
    trade_date date not null,
    source_bar_end_ts timestamptz not null,
    source_return double precision not null,
    previous_source_return double precision,
    threshold_value double precision not null,
    scheduled_entry_ts timestamptz not null,
    scheduled_exit_ts timestamptz not null,
    status text not null default 'TRIGGERED',
    entry_captured_at timestamptz,
    entry_bid double precision,
    entry_ask double precision,
    entry_mid double precision,
    entry_spread_bps double precision,
    entry_depth jsonb not null default '{}'::jsonb,
    entry_capacity jsonb not null default '{}'::jsonb,
    passive_limit_price double precision,
    passive_evaluated_at timestamptz,
    passive_filled boolean,
    passive_fill_ts timestamptz,
    exit_captured_at timestamptz,
    exit_bid double precision,
    exit_ask double precision,
    exit_mid double precision,
    exit_spread_bps double precision,
    exit_depth jsonb not null default '{}'::jsonb,
    exit_capacity jsonb not null default '{}'::jsonb,
    quote_cross_return double precision,
    mid_return double precision,
    research_net_20bps double precision,
    research_net_30bps double precision,
    passive_quote_cross_return double precision,
    capacity_returns jsonb not null default '{}'::jsonb,
    data_quality jsonb not null default '{}'::jsonb,
    error text,
    attempts integer not null default 0,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    unique(candidate_id, trade_date)
);

create index if not exists xal_live_signals_status_idx
    on research.xal_live_signals(candidate_id, status, scheduled_entry_ts, scheduled_exit_ts);

create table if not exists research.xal_live_monitor_state (
    candidate_id text primary key references research.xal_candidates(candidate_id),
    enabled boolean not null default true,
    worker_id text,
    monitor_status text not null default 'INITIALISING',
    last_checked_at timestamptz,
    last_source_boundary timestamptz,
    last_success_at timestamptz,
    last_error_at timestamptz,
    last_error text,
    metadata jsonb not null default '{}'::jsonb,
    updated_at timestamptz not null default now()
);

insert into research.xal_live_monitor_state(candidate_id, monitor_status, metadata)
values (
    'XAL-006',
    'READY',
    jsonb_build_object(
        'frozen_threshold', -0.0036892077395876,
        'lead_minutes', 15,
        'hold_minutes', 90,
        'target', 'DOGSUSDT',
        'mode', 'evidence_only_no_autotrade'
    )
)
on conflict (candidate_id) do update
set metadata = research.xal_live_monitor_state.metadata || excluded.metadata,
    updated_at = now();

alter table research.xal_live_source_evaluations enable row level security;
alter table research.xal_live_signals enable row level security;
alter table research.xal_live_monitor_state enable row level security;

create or replace view research.xal006_live_summary
with (security_invoker = true)
as
select
    count(*) filter (where status = 'COMPLETE')::bigint as completed_signals,
    max(source_bar_end_ts) as latest_signal_ts,
    avg(research_net_20bps) filter (where status = 'COMPLETE') as mean_net_20bps,
    avg(research_net_30bps) filter (where status = 'COMPLETE') as mean_net_30bps,
    avg((research_net_20bps > 0)::int) filter (where status = 'COMPLETE') as hit_rate_20bps,
    avg(quote_cross_return) filter (where status = 'COMPLETE') as mean_quote_cross_return,
    avg((passive_filled)::int) filter (where passive_filled is not null) as passive_fill_rate,
    max(updated_at) as last_updated_at
from research.xal_live_signals
where candidate_id = 'XAL-006';