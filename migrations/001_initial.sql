create extension if not exists pgcrypto;

create table if not exists schema_migrations (
    version text primary key,
    applied_at timestamptz not null default now()
);

create table if not exists collection_runs (
    id uuid primary key default gen_random_uuid(),
    name text not null,
    status text not null default 'queued' check (status in ('queued','running','paused','completed','completed_with_errors','cancelled','failed')),
    stage text not null default 'catalogue',
    start_ts timestamptz not null,
    end_ts timestamptz not null,
    providers text[] not null default array['alpaca','coinbase','binance','twelvedata']::text[],
    config jsonb not null default '{}'::jsonb,
    planned_partitions bigint not null default 0,
    completed_partitions bigint not null default 0,
    failed_partitions bigint not null default 0,
    skipped_partitions bigint not null default 0,
    rows_written bigint not null default 0,
    error text,
    created_at timestamptz not null default now(),
    started_at timestamptz,
    completed_at timestamptz,
    updated_at timestamptz not null default now()
);

create table if not exists instruments (
    id uuid primary key default gen_random_uuid(),
    provider text not null check (provider in ('alpaca','coinbase','binance','twelvedata')),
    provider_symbol text not null,
    canonical_symbol text not null,
    display_name text,
    asset_class text not null,
    base_asset text,
    quote_asset text,
    exchange text,
    status text,
    tradable boolean not null default true,
    preferred boolean not null default true,
    source_feed text,
    priority integer not null default 0,
    metadata jsonb not null default '{}'::jsonb,
    first_seen_at timestamptz not null default now(),
    last_seen_at timestamptz not null default now(),
    unique(provider, provider_symbol)
);

create index if not exists instruments_provider_class_idx on instruments(provider, asset_class, preferred);
create index if not exists instruments_canonical_idx on instruments(canonical_symbol);
create index if not exists instruments_quote_idx on instruments(quote_asset);

create table if not exists instrument_groups (
    id text primary key,
    display_name text not null,
    description text,
    is_system boolean not null default false,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists instrument_group_members (
    group_id text not null references instrument_groups(id) on delete cascade,
    instrument_id uuid not null references instruments(id) on delete cascade,
    created_at timestamptz not null default now(),
    primary key(group_id, instrument_id)
);
create index if not exists instrument_group_members_instrument_idx on instrument_group_members(instrument_id);

create table if not exists collection_partitions (
    id uuid primary key default gen_random_uuid(),
    run_id uuid not null references collection_runs(id) on delete cascade,
    provider text not null,
    instrument_id uuid references instruments(id) on delete cascade,
    provider_symbol text,
    data_type text not null default 'bars_1m',
    start_ts timestamptz,
    end_ts timestamptz,
    status text not null default 'queued' check (status in ('queued','running','completed','completed_empty','retry_wait','failed','skipped','cancelled')),
    priority integer not null default 0,
    attempts integer not null default 0,
    max_attempts integer not null default 8,
    cursor jsonb not null default '{}'::jsonb,
    row_count bigint not null default 0,
    checksum text,
    locked_by text,
    locked_at timestamptz,
    heartbeat_at timestamptz,
    not_before timestamptz,
    last_error text,
    error_code text,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create unique index if not exists collection_partition_unique_idx
on collection_partitions(
    run_id,
    provider,
    coalesce(instrument_id, '00000000-0000-0000-0000-000000000000'::uuid),
    data_type,
    coalesce(start_ts, '-infinity'::timestamptz),
    coalesce(end_ts, 'infinity'::timestamptz)
);
create index if not exists collection_partition_claim_idx on collection_partitions(status, not_before, priority desc, created_at);
create index if not exists collection_partition_run_status_idx on collection_partitions(run_id, status);
create index if not exists collection_partition_heartbeat_idx on collection_partitions(status, heartbeat_at);

create table if not exists market_bars_1m (
    provider text not null,
    instrument_id uuid not null references instruments(id) on delete cascade,
    ts timestamptz not null,
    open double precision,
    high double precision,
    low double precision,
    close double precision,
    volume double precision,
    quote_volume double precision,
    trade_count bigint,
    vwap double precision,
    taker_buy_base_volume double precision,
    taker_buy_quote_volume double precision,
    source_feed text,
    inserted_at timestamptz not null default now(),
    primary key(provider, instrument_id, ts)
) partition by list(provider);

create table if not exists market_bars_1m_alpaca partition of market_bars_1m for values in ('alpaca');
create table if not exists market_bars_1m_coinbase partition of market_bars_1m for values in ('coinbase');
create table if not exists market_bars_1m_binance partition of market_bars_1m for values in ('binance');
create table if not exists market_bars_1m_twelvedata partition of market_bars_1m for values in ('twelvedata');

create index if not exists market_bars_alpaca_ts_brin on market_bars_1m_alpaca using brin(ts);
create index if not exists market_bars_coinbase_ts_brin on market_bars_1m_coinbase using brin(ts);
create index if not exists market_bars_binance_ts_brin on market_bars_1m_binance using brin(ts);
create index if not exists market_bars_twelvedata_ts_brin on market_bars_1m_twelvedata using brin(ts);

create table if not exists data_quality_results (
    id uuid primary key default gen_random_uuid(),
    run_id uuid references collection_runs(id) on delete cascade,
    instrument_id uuid references instruments(id) on delete cascade,
    provider text not null,
    check_name text not null,
    severity text not null check (severity in ('info','warning','error')),
    details jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now()
);

create table if not exists export_jobs (
    id uuid primary key default gen_random_uuid(),
    name text not null,
    status text not null default 'queued' check (status in ('queued','planning','running','completed','completed_with_errors','cancelled','failed')),
    filters jsonb not null default '{}'::jsonb,
    total_parts integer not null default 0,
    completed_parts integer not null default 0,
    failed_parts integer not null default 0,
    total_bytes bigint not null default 0,
    locked_by text,
    heartbeat_at timestamptz,
    error text,
    created_at timestamptz not null default now(),
    started_at timestamptz,
    completed_at timestamptz,
    updated_at timestamptz not null default now()
);

create table if not exists export_parts (
    id uuid primary key default gen_random_uuid(),
    export_job_id uuid not null references export_jobs(id) on delete cascade,
    part_number integer not null,
    part_type text not null default 'data' check (part_type in ('data','matrix')),
    status text not null default 'queued' check (status in ('queued','running','completed','retry_wait','failed','cancelled')),
    instrument_ids jsonb not null default '[]'::jsonb,
    object_path text,
    filename text,
    size_bytes bigint,
    checksum text,
    attempts integer not null default 0,
    max_attempts integer not null default 5,
    locked_by text,
    locked_at timestamptz,
    heartbeat_at timestamptz,
    not_before timestamptz,
    last_error text,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    unique(export_job_id, part_number)
);

create index if not exists export_parts_claim_idx on export_parts(status, not_before, created_at);

create table if not exists application_events (
    id bigserial primary key,
    level text not null,
    event_type text not null,
    run_id uuid,
    export_job_id uuid,
    message text not null,
    details jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now()
);
create index if not exists application_events_created_idx on application_events(created_at desc);

create or replace function claim_collection_partition(p_worker_id text)
returns setof collection_partitions
language plpgsql
as $$
begin
    return query
    with candidate as (
        select cp.id
        from collection_partitions cp
        join collection_runs cr on cr.id = cp.run_id
        where cp.status in ('queued','retry_wait')
          and (cp.not_before is null or cp.not_before <= now())
          and cr.status in ('queued','running')
        order by cp.priority desc, cp.created_at, cp.id
        for update of cp skip locked
        limit 1
    )
    update collection_partitions cp
       set status = 'running',
           locked_by = p_worker_id,
           locked_at = now(),
           heartbeat_at = now(),
           attempts = attempts + 1,
           updated_at = now()
      from candidate
     where cp.id = candidate.id
    returning cp.*;
end;
$$;

create or replace function claim_export_job_for_planning(p_worker_id text)
returns setof export_jobs
language plpgsql
as $$
begin
    return query
    with candidate as (
        select ej.id
        from export_jobs ej
        where ej.status = 'queued'
        order by ej.created_at, ej.id
        for update skip locked
        limit 1
    )
    update export_jobs ej
       set status = 'planning',
           locked_by = p_worker_id,
           heartbeat_at = now(),
           started_at = coalesce(started_at, now()),
           updated_at = now()
      from candidate
     where ej.id = candidate.id
    returning ej.*;
end;
$$;

create or replace function claim_export_part(p_worker_id text)
returns setof export_parts
language plpgsql
as $$
begin
    return query
    with candidate as (
        select ep.id
        from export_parts ep
        join export_jobs ej on ej.id = ep.export_job_id
        where ep.status in ('queued','retry_wait')
          and (ep.not_before is null or ep.not_before <= now())
          and ej.status = 'running'
        order by ep.part_number, ep.created_at
        for update of ep skip locked
        limit 1
    )
    update export_parts ep
       set status = 'running',
           locked_by = p_worker_id,
           locked_at = now(),
           heartbeat_at = now(),
           attempts = attempts + 1,
           updated_at = now()
      from candidate
     where ep.id = candidate.id
    returning ep.*;
end;
$$;

create or replace function refresh_collection_run_counts(p_run_id uuid)
returns void
language plpgsql
as $$
declare
    v_total bigint;
    v_done bigint;
    v_failed bigint;
    v_skipped bigint;
    v_rows bigint;
    v_active bigint;
begin
    select count(*),
           count(*) filter (where status in ('completed','completed_empty')),
           count(*) filter (where status = 'failed'),
           count(*) filter (where status = 'skipped'),
           coalesce(sum(row_count),0),
           count(*) filter (where status in ('queued','retry_wait','running'))
      into v_total, v_done, v_failed, v_skipped, v_rows, v_active
      from collection_partitions
     where run_id = p_run_id;

    update collection_runs
       set planned_partitions = v_total,
           completed_partitions = v_done,
           failed_partitions = v_failed,
           skipped_partitions = v_skipped,
           rows_written = v_rows,
           updated_at = now(),
           status = case
               when status in ('paused','cancelled','failed') then status
               when stage in ('catalogue','planning') then 'running'
               when v_active = 0 and v_failed > 0 then 'completed_with_errors'
               when v_active = 0 and v_total > 0 then 'completed'
               else 'running'
           end,
           completed_at = case
               when stage = 'collecting' and v_active = 0 and v_total > 0 then coalesce(completed_at, now())
               else completed_at
           end
     where id = p_run_id;
end;
$$;

create or replace function refresh_export_job_counts(p_export_job_id uuid)
returns void
language plpgsql
as $$
declare
    v_total integer;
    v_done integer;
    v_failed integer;
    v_bytes bigint;
    v_active integer;
begin
    select count(*),
           count(*) filter (where status = 'completed'),
           count(*) filter (where status = 'failed'),
           coalesce(sum(size_bytes),0),
           count(*) filter (where status in ('queued','retry_wait','running'))
      into v_total, v_done, v_failed, v_bytes, v_active
      from export_parts
     where export_job_id = p_export_job_id;

    update export_jobs
       set total_parts = v_total,
           completed_parts = v_done,
           failed_parts = v_failed,
           total_bytes = v_bytes,
           updated_at = now(),
           status = case
               when status in ('cancelled','failed','planning') then status
               when v_active = 0 and v_failed > 0 then 'completed_with_errors'
               when v_active = 0 and v_total > 0 then 'completed'
               else 'running'
           end,
           completed_at = case
               when v_active = 0 and v_total > 0 then coalesce(completed_at, now())
               else completed_at
           end
     where id = p_export_job_id;
end;
$$;


-- Keep application tables inaccessible through Supabase's public anon/authenticated API roles.
do $$
begin
    if exists (select 1 from pg_roles where rolname='anon')
       and exists (select 1 from pg_roles where rolname='authenticated') then
        revoke all privileges on all tables in schema public from anon, authenticated;
        revoke all privileges on all sequences in schema public from anon, authenticated;
        alter default privileges in schema public revoke all on tables from anon, authenticated;
        alter default privileges in schema public revoke all on sequences from anon, authenticated;
    end if;
end;
$$;
