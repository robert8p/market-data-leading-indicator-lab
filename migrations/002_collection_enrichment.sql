-- v3.0.1: collection-only enrichment and restartable acquisition windows.
-- Additive migration from the original v1.0.2 schema. No existing market bars are removed.

alter table collection_runs
    add column if not exists enhancement_requested boolean not null default false,
    add column if not exists enhancement_started_at timestamptz,
    add column if not exists enhancement_completed_at timestamptz;

create table if not exists capture_windows (
    id uuid primary key default gen_random_uuid(),
    run_id uuid not null references collection_runs(id) on delete cascade,
    provider text not null,
    asset_class text not null,
    instrument_id uuid not null references instruments(id) on delete cascade,
    provider_symbol text not null,
    canonical_symbol text not null,
    trigger_ts timestamptz not null,
    window_start timestamptz not null,
    window_end timestamptz not null,
    trigger_kind text not null,
    trigger_value double precision,
    rule_version text not null default 'acquisition-v1',
    reason jsonb not null default '{}'::jsonb,
    planned boolean not null default false,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    unique(run_id, provider, instrument_id, trigger_ts, trigger_kind)
);
create index if not exists capture_windows_run_planned_idx
    on capture_windows(run_id, planned, provider, trigger_ts);
create index if not exists capture_windows_instrument_ts_idx
    on capture_windows(instrument_id, trigger_ts);

create table if not exists market_trades (
    provider text not null,
    instrument_id uuid not null references instruments(id) on delete cascade,
    message_key text not null,
    ts timestamptz not null,
    price double precision not null,
    size double precision not null,
    quote_size double precision,
    aggressor_side text,
    exchange text,
    trade_id text,
    conditions jsonb not null default '[]'::jsonb,
    source_feed text,
    metadata jsonb not null default '{}'::jsonb,
    inserted_at timestamptz not null default now(),
    primary key(provider, instrument_id, message_key)
);
create index if not exists market_trades_instrument_ts_idx on market_trades(instrument_id, ts);
create index if not exists market_trades_ts_brin on market_trades using brin(ts);

create table if not exists market_quotes_l1 (
    provider text not null,
    instrument_id uuid not null references instruments(id) on delete cascade,
    message_key text not null,
    ts timestamptz not null,
    bid_exchange text,
    bid_price double precision,
    bid_size double precision,
    ask_exchange text,
    ask_price double precision,
    ask_size double precision,
    conditions jsonb not null default '[]'::jsonb,
    source_feed text,
    metadata jsonb not null default '{}'::jsonb,
    inserted_at timestamptz not null default now(),
    primary key(provider, instrument_id, message_key)
);
create index if not exists market_quotes_l1_instrument_ts_idx on market_quotes_l1(instrument_id, ts);
create index if not exists market_quotes_l1_ts_brin on market_quotes_l1 using brin(ts);

create table if not exists equity_context_snapshots (
    id uuid primary key default gen_random_uuid(),
    instrument_id uuid not null references instruments(id) on delete cascade,
    source text not null,
    asof_date date not null,
    ticker text not null,
    cik text,
    market_cap double precision,
    free_float double precision,
    free_float_percent double precision,
    share_class_shares_outstanding double precision,
    weighted_shares_outstanding double precision,
    short_interest double precision,
    avg_daily_volume double precision,
    days_to_cover double precision,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    unique(instrument_id, source, asof_date)
);
create index if not exists equity_context_instrument_asof_idx
    on equity_context_snapshots(instrument_id, asof_date desc);

create table if not exists sec_filings (
    instrument_id uuid not null references instruments(id) on delete cascade,
    accession_number text not null,
    cik text not null,
    filing_date date,
    report_date date,
    accepted_at timestamptz,
    form text not null,
    primary_document text,
    primary_doc_description text,
    filing_url text,
    is_dilution_relevant boolean not null default false,
    dilution_signals jsonb not null default '{}'::jsonb,
    metadata jsonb not null default '{}'::jsonb,
    inserted_at timestamptz not null default now(),
    primary key(instrument_id, accession_number)
);
create index if not exists sec_filings_instrument_date_idx
    on sec_filings(instrument_id, filing_date desc);
create index if not exists sec_filings_dilution_idx
    on sec_filings(instrument_id, is_dilution_relevant, filing_date desc);

create table if not exists finra_short_volume (
    instrument_id uuid not null references instruments(id) on delete cascade,
    trade_date date not null,
    symbol text not null,
    short_volume double precision,
    short_exempt_volume double precision,
    total_volume double precision,
    market text not null default '',
    source_file text,
    metadata jsonb not null default '{}'::jsonb,
    inserted_at timestamptz not null default now(),
    primary key(instrument_id, trade_date, market)
);
create index if not exists finra_short_volume_date_idx on finra_short_volume(trade_date, symbol);

create table if not exists market_news (
    provider text not null,
    news_id text not null,
    headline text not null,
    summary text,
    author text,
    source text,
    published_at timestamptz not null,
    updated_at timestamptz,
    symbols text[] not null default '{}'::text[],
    url text,
    content text,
    metadata jsonb not null default '{}'::jsonb,
    inserted_at timestamptz not null default now(),
    primary key(provider, news_id)
);
create index if not exists market_news_published_idx on market_news(published_at);
create index if not exists market_news_symbols_gin on market_news using gin(symbols);

create table if not exists provider_health (
    provider text not null,
    service text not null,
    status text not null,
    last_message_at timestamptz,
    last_success_at timestamptz,
    last_error_at timestamptz,
    message_count bigint not null default 0,
    reconnect_count bigint not null default 0,
    last_error text,
    metadata jsonb not null default '{}'::jsonb,
    updated_at timestamptz not null default now(),
    primary key(provider, service)
);

-- Runs must pass through the mining/enrichment stages before being finalised.
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
    v_stage text;
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

    select stage into v_stage from collection_runs where id = p_run_id;

    update collection_runs
       set planned_partitions = v_total,
           completed_partitions = v_done,
           failed_partitions = v_failed,
           skipped_partitions = v_skipped,
           rows_written = v_rows,
           updated_at = now(),
           status = case
               when status in ('paused','cancelled','failed') then status
               when v_active = 0 and v_stage = 'ready' and v_failed > 0 then 'completed_with_errors'
               when v_active = 0 and v_stage = 'ready' then 'completed'
               else 'running'
           end,
           completed_at = case
               when v_active = 0 and v_stage = 'ready' then coalesce(completed_at, now())
               else completed_at
           end
     where id = p_run_id;
end;
$$;
