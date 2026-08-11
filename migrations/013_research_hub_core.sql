create schema if not exists research_hub;

revoke all on schema research_hub from public;
revoke all on schema research_hub from anon;
revoke all on schema research_hub from authenticated;

create table if not exists research_hub.data_stores (
    store_key text primary key,
    platform text not null,
    project_ref text,
    region text,
    connection_env_var text,
    read_mode text not null default 'direct_postgres',
    is_primary boolean not null default false,
    enabled boolean not null default true,
    description text,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists research_hub.datasets (
    dataset_key text primary key,
    store_key text not null references research_hub.data_stores(store_key),
    schema_name text not null,
    relation_name text not null,
    asset_class text,
    provider text,
    frequency text,
    grain text,
    ts_column text,
    instrument_column text,
    observable_at_column text,
    is_raw boolean not null default false,
    point_in_time_safe boolean,
    coverage_start timestamptz,
    coverage_end timestamptz,
    row_estimate bigint,
    status text not null default 'registered',
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    unique (store_key, schema_name, relation_name)
);

create table if not exists research_hub.feature_definitions (
    feature_key text primary key,
    dataset_key text references research_hub.datasets(dataset_key),
    feature_name text not null,
    feature_family text,
    value_type text not null default 'double precision',
    source_expression text,
    decision_time_rule text not null,
    observable_at_rule text not null,
    lookback_seconds integer check (lookback_seconds is null or lookback_seconds >= 0),
    uses_future_data boolean not null default false check (uses_future_data = false),
    enabled boolean not null default true,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists research_hub.outcome_definitions (
    outcome_key text primary key,
    outcome_name text not null,
    target_asset_class text,
    horizon_seconds integer not null check (horizon_seconds > 0),
    entry_rule text not null,
    exit_rule text not null,
    cost_model_key text,
    enabled boolean not null default true,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists research_hub.point_in_time_universe (
    universe_key text not null,
    instrument_key text not null,
    effective_from timestamptz not null,
    effective_to timestamptz,
    tradable boolean,
    active boolean,
    exchange text,
    source_dataset_key text references research_hub.datasets(dataset_key),
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    primary key (universe_key, instrument_key, effective_from),
    check (effective_to is null or effective_to > effective_from)
);
create index if not exists idx_research_hub_pit_universe_lookup on research_hub.point_in_time_universe (universe_key, instrument_key, effective_from, effective_to);

create table if not exists research_hub.feature_sets (
    feature_set_key text primary key,
    description text,
    decision_grain text not null,
    source_dataset_keys text[] not null default '{}'::text[],
    feature_keys text[] not null default '{}'::text[],
    materialization_schema text,
    materialization_relation text,
    point_in_time_verified boolean not null default false,
    verification_notes text,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists research_hub.feature_rows (
    feature_set_key text not null references research_hub.feature_sets(feature_set_key) on delete cascade,
    instrument_key text not null,
    decision_ts timestamptz not null,
    observable_at timestamptz not null,
    features jsonb not null,
    source_dataset_key text references research_hub.datasets(dataset_key),
    source_row_hash text,
    quality jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    primary key (feature_set_key, instrument_key, decision_ts),
    check (observable_at <= decision_ts)
);
create index if not exists idx_research_hub_feature_rows_ts on research_hub.feature_rows (feature_set_key, decision_ts);
create index if not exists idx_research_hub_feature_rows_brin on research_hub.feature_rows using brin (decision_ts);

create table if not exists research_hub.outcome_sets (
    outcome_set_key text primary key,
    description text,
    outcome_keys text[] not null default '{}'::text[],
    materialization_schema text,
    materialization_relation text,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists research_hub.outcome_rows (
    outcome_set_key text not null references research_hub.outcome_sets(outcome_set_key) on delete cascade,
    instrument_key text not null,
    decision_ts timestamptz not null,
    horizon_seconds integer not null check (horizon_seconds > 0),
    entry_ts timestamptz,
    exit_ts timestamptz,
    gross_return double precision,
    net_return double precision,
    max_favourable_excursion double precision,
    max_adverse_excursion double precision,
    realised_volatility double precision,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    primary key (outcome_set_key, instrument_key, decision_ts, horizon_seconds),
    check (exit_ts is null or exit_ts > decision_ts)
);
create index if not exists idx_research_hub_outcome_rows_ts on research_hub.outcome_rows (outcome_set_key, decision_ts, horizon_seconds);
create index if not exists idx_research_hub_outcome_rows_brin on research_hub.outcome_rows using brin (decision_ts);

create table if not exists research_hub.experiment_runs (
    run_id uuid primary key default gen_random_uuid(),
    run_key text unique not null,
    name text not null,
    status text not null default 'planned',
    feature_set_key text references research_hub.feature_sets(feature_set_key),
    outcome_set_key text references research_hub.outcome_sets(outcome_set_key),
    discovery_start timestamptz,
    discovery_end timestamptz,
    validation_start timestamptz,
    validation_end timestamptz,
    holdout_start timestamptz,
    holdout_end timestamptz,
    config jsonb not null default '{}'::jsonb,
    search_space_tests bigint,
    code_version text,
    created_at timestamptz not null default now(),
    started_at timestamptz,
    completed_at timestamptz,
    updated_at timestamptz not null default now()
);

create table if not exists research_hub.experiment_tests (
    test_id bigserial primary key,
    run_id uuid not null references research_hub.experiment_runs(run_id) on delete cascade,
    feature_key text,
    outcome_key text,
    source_instrument text,
    target_instrument text,
    slice_key text,
    horizon_seconds integer,
    n bigint,
    mean_gross double precision,
    mean_net double precision,
    median_net double precision,
    hit_rate_net double precision,
    profit_factor_net double precision,
    worst_net double precision,
    p_value double precision,
    q_value double precision,
    effect_size double precision,
    adjacent_horizon_positive boolean,
    validation_positive boolean,
    holdout_positive boolean,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now()
);
create index if not exists idx_research_hub_tests_run_q on research_hub.experiment_tests (run_id, q_value nulls last, mean_net desc);

create table if not exists research_hub.candidate_ledger (
    candidate_id text primary key,
    run_id uuid references research_hub.experiment_runs(run_id),
    status text not null,
    descriptive_name text not null,
    frozen_definition jsonb not null,
    metrics jsonb not null default '{}'::jsonb,
    confidence text,
    failure_conditions jsonb not null default '{}'::jsonb,
    next_test text,
    frozen_at timestamptz,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists research_hub.sync_checkpoints (
    dataset_key text primary key references research_hub.datasets(dataset_key) on delete cascade,
    last_source_ts timestamptz,
    last_source_key text,
    last_row_count bigint,
    status text not null default 'never_synced',
    last_error text,
    metadata jsonb not null default '{}'::jsonb,
    updated_at timestamptz not null default now()
);

create table if not exists research_hub.data_quality_issues (
    issue_id bigserial primary key,
    dataset_key text references research_hub.datasets(dataset_key),
    detected_at timestamptz not null default now(),
    severity text not null,
    issue_type text not null,
    range_start timestamptz,
    range_end timestamptz,
    instrument_key text,
    details jsonb not null default '{}'::jsonb,
    resolved_at timestamptz
);

insert into research_hub.data_stores(store_key,platform,project_ref,region,connection_env_var,read_mode,is_primary,description) values
('market_data_primary','supabase','oxzabweahkoimtevbbny','eu-west-1','DATABASE_URL','direct_postgres',true,'Primary market-data-leading-indicators fact and research store'),
('alpaca_rapid_discovery','supabase','mnmkxjirpwbptdnvjmpw','eu-west-1','ALPACA_RAPID_DATABASE_URL','direct_postgres',false,'Deep/full-universe Alpaca rapid-discovery store'),
('alpaca_138_research','supabase','ztoxsojoimljfoijkrvf','eu-west-1','ALPACA_138_DATABASE_URL','direct_postgres',false,'Frozen 13.8 execution research store')
on conflict (store_key) do update set platform=excluded.platform,project_ref=excluded.project_ref,region=excluded.region,connection_env_var=excluded.connection_env_var,read_mode=excluded.read_mode,is_primary=excluded.is_primary,description=excluded.description,updated_at=now();

insert into research_hub.outcome_definitions(outcome_key,outcome_name,horizon_seconds,entry_rule,exit_rule,cost_model_key,metadata) values
('fwd_return_1m','Forward return, 1 minute',60,'first executable price at or after decision_ts','first executable price at or after decision_ts + 60 seconds','asset_specific','{"standard":true}'),
('fwd_return_5m','Forward return, 5 minutes',300,'first executable price at or after decision_ts','first executable price at or after decision_ts + 300 seconds','asset_specific','{"standard":true}'),
('fwd_return_15m','Forward return, 15 minutes',900,'first executable price at or after decision_ts','first executable price at or after decision_ts + 900 seconds','asset_specific','{"standard":true}'),
('fwd_return_30m','Forward return, 30 minutes',1800,'first executable price at or after decision_ts','first executable price at or after decision_ts + 1800 seconds','asset_specific','{"standard":true}'),
('fwd_return_60m','Forward return, 60 minutes',3600,'first executable price at or after decision_ts','first executable price at or after decision_ts + 3600 seconds','asset_specific','{"standard":true}'),
('fwd_return_120m','Forward return, 120 minutes',7200,'first executable price at or after decision_ts','first executable price at or after decision_ts + 7200 seconds','asset_specific','{"standard":true}'),
('fwd_return_240m','Forward return, 240 minutes',14400,'first executable price at or after decision_ts','first executable price at or after decision_ts + 14400 seconds','asset_specific','{"standard":true}')
on conflict (outcome_key) do nothing;

create or replace view research_hub.dataset_inventory with (security_invoker=true) as
select d.dataset_key,d.store_key,s.project_ref,s.region,d.schema_name,d.relation_name,d.asset_class,d.provider,d.frequency,d.grain,d.is_raw,d.point_in_time_safe,d.coverage_start,d.coverage_end,d.row_estimate,d.status,d.metadata
from research_hub.datasets d join research_hub.data_stores s using(store_key);

comment on schema research_hub is 'Private semantic and experiment-control layer for ChatGPT-directed, point-in-time market leading-indicator research.';
comment on table research_hub.feature_rows is 'Compact research-ready point-in-time feature vectors. observable_at must never exceed decision_ts.';
comment on table research_hub.outcome_rows is 'Future outcomes kept physically separate from predictor features to reduce leakage risk.';
comment on table research_hub.point_in_time_universe is 'Effective-dated instrument membership/tradability history for survivorship-bias control.';
