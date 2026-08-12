insert into research_hub.datasets(
    dataset_key, store_key, schema_name, relation_name, asset_class, provider,
    frequency, grain, ts_column, instrument_column, observable_at_column,
    is_raw, point_in_time_safe, coverage_start, coverage_end, row_estimate,
    status, metadata, created_at, updated_at
)
values
(
    'primary.xal006_live_source_evaluations', 'market_data_primary', 'research', 'xal_live_source_evaluations',
    'multi', 'alpaca', '15m', 'prospective-source-evaluation', 'source_bar_end_ts', null, 'created_at',
    false, true, null, null, 0, 'available',
    jsonb_build_object(
        'candidate_id','XAL-006',
        'role','prospective_point_in_time_source_evidence',
        'frozen_rule',true,
        'auto_trade',false
    ), now(), now()
),
(
    'primary.xal006_live_execution_signals', 'market_data_primary', 'research', 'xal_live_signals',
    'multi', 'multi', 'event', 'prospective-execution-audit', 'source_bar_end_ts', null, 'updated_at',
    false, false, null, null, 0, 'available',
    jsonb_build_object(
        'candidate_id','XAL-006',
        'role','prospective_execution_outcomes',
        'contains_future_outcomes',true,
        'eligible_for_predictor_search',false,
        'frozen_rule',true,
        'auto_trade',false
    ), now(), now()
)
on conflict (dataset_key) do update
set store_key=excluded.store_key,
    schema_name=excluded.schema_name,
    relation_name=excluded.relation_name,
    asset_class=excluded.asset_class,
    provider=excluded.provider,
    frequency=excluded.frequency,
    grain=excluded.grain,
    ts_column=excluded.ts_column,
    instrument_column=excluded.instrument_column,
    observable_at_column=excluded.observable_at_column,
    is_raw=excluded.is_raw,
    point_in_time_safe=excluded.point_in_time_safe,
    status=excluded.status,
    metadata=research_hub.datasets.metadata || excluded.metadata,
    updated_at=now();

create or replace function research_hub.sync_xal_candidate_to_ledger()
returns trigger
language plpgsql
security definer
set search_path = research_hub, research, public, pg_temp
as $$
begin
    insert into research_hub.candidate_ledger(
        candidate_id, candidate_version, descriptive_name, status, confidence,
        frozen_definition, definition_hash, metrics, failure_conditions, next_test,
        frozen_at, first_discovered_at, last_tested_at,
        source_store_key, source_schema, source_table, source_record_key,
        provenance, created_at, updated_at
    )
    values(
        new.candidate_id,
        1,
        new.descriptive_name,
        new.status,
        new.confidence,
        coalesce(new.frozen_definition,'{}'::jsonb),
        encode(digest(convert_to(coalesce(new.frozen_definition,'{}'::jsonb)::text,'UTF8'),'sha256'),'hex'),
        coalesce(new.metrics,'{}'::jsonb),
        coalesce(new.failure_conditions,'{}'::jsonb),
        new.next_test,
        new.frozen_at,
        new.created_at,
        new.updated_at,
        'market_data_primary',
        'research',
        'xal_candidates',
        new.candidate_id,
        jsonb_build_object(
            'authoritative_source','market_data_primary.research.xal_candidates',
            'canonical_sync','triggered',
            'frozen_definition_preserved',true
        ),
        coalesce(new.created_at,now()),
        now()
    )
    on conflict (candidate_id) do update
    set descriptive_name=excluded.descriptive_name,
        status=excluded.status,
        confidence=excluded.confidence,
        frozen_definition=excluded.frozen_definition,
        definition_hash=excluded.definition_hash,
        metrics=excluded.metrics,
        failure_conditions=excluded.failure_conditions,
        next_test=excluded.next_test,
        frozen_at=excluded.frozen_at,
        first_discovered_at=coalesce(research_hub.candidate_ledger.first_discovered_at, excluded.first_discovered_at),
        last_tested_at=excluded.last_tested_at,
        source_store_key=excluded.source_store_key,
        source_schema=excluded.source_schema,
        source_table=excluded.source_table,
        source_record_key=excluded.source_record_key,
        provenance=coalesce(research_hub.candidate_ledger.provenance,'{}'::jsonb) || excluded.provenance,
        updated_at=now();
    return new;
end;
$$;

drop trigger if exists trg_sync_xal_candidate_to_ledger on research.xal_candidates;
create trigger trg_sync_xal_candidate_to_ledger
after insert or update on research.xal_candidates
for each row execute function research_hub.sync_xal_candidate_to_ledger();

insert into research_hub.candidate_ledger(
    candidate_id, candidate_version, descriptive_name, status, confidence,
    frozen_definition, definition_hash, metrics, failure_conditions, next_test,
    frozen_at, first_discovered_at, last_tested_at,
    source_store_key, source_schema, source_table, source_record_key,
    provenance, created_at, updated_at
)
select
    c.candidate_id,
    1,
    c.descriptive_name,
    c.status,
    c.confidence,
    coalesce(c.frozen_definition,'{}'::jsonb),
    encode(digest(convert_to(coalesce(c.frozen_definition,'{}'::jsonb)::text,'UTF8'),'sha256'),'hex'),
    coalesce(c.metrics,'{}'::jsonb),
    coalesce(c.failure_conditions,'{}'::jsonb),
    c.next_test,
    c.frozen_at,
    c.created_at,
    c.updated_at,
    'market_data_primary',
    'research',
    'xal_candidates',
    c.candidate_id,
    jsonb_build_object(
        'authoritative_source','market_data_primary.research.xal_candidates',
        'canonical_sync','initial_backfill',
        'frozen_definition_preserved',true
    ),
    coalesce(c.created_at,now()),
    now()
from research.xal_candidates c
on conflict (candidate_id) do update
set descriptive_name=excluded.descriptive_name,
    status=excluded.status,
    confidence=excluded.confidence,
    frozen_definition=excluded.frozen_definition,
    definition_hash=excluded.definition_hash,
    metrics=excluded.metrics,
    failure_conditions=excluded.failure_conditions,
    next_test=excluded.next_test,
    frozen_at=excluded.frozen_at,
    first_discovered_at=coalesce(research_hub.candidate_ledger.first_discovered_at, excluded.first_discovered_at),
    last_tested_at=excluded.last_tested_at,
    source_store_key=excluded.source_store_key,
    source_schema=excluded.source_schema,
    source_table=excluded.source_table,
    source_record_key=excluded.source_record_key,
    provenance=coalesce(research_hub.candidate_ledger.provenance,'{}'::jsonb) || excluded.provenance,
    updated_at=now();

create or replace function research_hub.sync_xal006_live_candidate_metrics()
returns trigger
language plpgsql
security definer
set search_path = research_hub, research, public, pg_temp
as $$
declare
    summary jsonb;
begin
    if new.candidate_id <> 'XAL-006' or new.status <> 'COMPLETE' then
        return new;
    end if;

    select jsonb_build_object(
        'n', count(*),
        'latest_signal_ts', max(source_bar_end_ts),
        'mean_quote_cross_return', avg(quote_cross_return),
        'mean_mid_return', avg(mid_return),
        'mean_net_20bps', avg(research_net_20bps),
        'mean_net_30bps', avg(research_net_30bps),
        'hit_rate_20bps', avg((research_net_20bps > 0)::int),
        'passive_fill_rate', avg((passive_filled)::int) filter (where passive_filled is not null),
        'mean_entry_spread_bps', avg(entry_spread_bps),
        'mean_exit_spread_bps', avg(exit_spread_bps),
        'evidence_type','prospective_point_in_time_execution',
        'auto_trade',false,
        'frozen_rule_unchanged',true,
        'last_completed_at', max(exit_captured_at)
    )
    into summary
    from research.xal_live_signals
    where candidate_id='XAL-006' and status='COMPLETE';

    update research.xal_candidates
    set metrics = coalesce(metrics,'{}'::jsonb) || jsonb_build_object('prospective_live_execution', summary),
        updated_at = now()
    where candidate_id='XAL-006';

    return new;
end;
$$;

drop trigger if exists trg_sync_xal006_live_candidate_metrics on research.xal_live_signals;
create trigger trg_sync_xal006_live_candidate_metrics
after insert or update of status, quote_cross_return, mid_return, research_net_20bps, research_net_30bps, passive_filled, entry_spread_bps, exit_spread_bps
on research.xal_live_signals
for each row execute function research_hub.sync_xal006_live_candidate_metrics();

create or replace function research_hub.sync_xal006_live_job()
returns trigger
language plpgsql
security definer
set search_path = research_hub, research, public, pg_temp
as $$
declare
    s research.xal_live_monitor_state%rowtype;
    completed_count bigint;
    triggered_count bigint;
    source_eval_count bigint;
    summary jsonb;
begin
    select * into s
    from research.xal_live_monitor_state
    where candidate_id='XAL-006';

    if not found then
        return new;
    end if;

    select
        count(*) filter (where status='COMPLETE'),
        count(*)
    into completed_count, triggered_count
    from research.xal_live_signals
    where candidate_id='XAL-006';

    select count(*) into source_eval_count
    from research.xal_live_source_evaluations
    where candidate_id='XAL-006';

    select jsonb_build_object(
        'completed_signals', count(*) filter (where status='COMPLETE'),
        'triggered_signals', count(*),
        'latest_signal_ts', max(source_bar_end_ts),
        'mean_net_20bps', avg(research_net_20bps) filter (where status='COMPLETE'),
        'mean_net_30bps', avg(research_net_30bps) filter (where status='COMPLETE'),
        'hit_rate_20bps', avg((research_net_20bps > 0)::int) filter (where status='COMPLETE'),
        'mean_quote_cross_return', avg(quote_cross_return) filter (where status='COMPLETE'),
        'passive_fill_rate', avg((passive_filled)::int) filter (where passive_filled is not null),
        'source_evaluations', source_eval_count,
        'monitor_status', s.monitor_status,
        'monitor_worker_id', s.worker_id,
        'last_checked_at', s.last_checked_at,
        'last_source_boundary', s.last_source_boundary,
        'frozen_rule_unchanged', true,
        'auto_trade', false
    ) into summary
    from research.xal_live_signals
    where candidate_id='XAL-006';

    insert into research_hub.program_jobs(
        job_key, exact_name, purpose, store_key,
        source_schema, source_table, source_id, job_kind, current_state,
        started_at, latest_successful_checkpoint,
        progress_current, progress_total, completion_pct,
        latest_result, current_error, retry_state, next_automatic_action,
        intervention_required, exact_intervention, frozen_rule, holdout_sensitive,
        metadata, created_at, updated_at
    )
    values(
        'XAL006-LIVE-EXECUTION',
        'XAL-006 prospective execution validation',
        'Collect point-in-time IWM trigger evidence and DOGSUSDT entry/exit quotes, depth, passive-fill proxies and small-notional capacity without changing the frozen XAL-006 rule.',
        'market_data_primary',
        'research',
        'xal_live_monitor_state',
        'XAL-006',
        'prospective_validation',
        lower(s.monitor_status),
        coalesce(s.last_success_at, now()),
        s.last_success_at,
        completed_count,
        null,
        null,
        summary,
        s.last_error,
        case when s.monitor_status='ERROR_RETRYING' then 'automatic retry active' else 'continuous prospective monitoring' end,
        'Keep XAL-006 frozen; capture the next genuinely new trigger, entry and exit prospectively and append execution evidence. Do not retune from forward outcomes.',
        false,
        null,
        true,
        false,
        jsonb_build_object(
            'candidate_id','XAL-006',
            'evidence_standard','prospective_point_in_time_execution',
            'research_cost_bps',jsonb_build_array(20,30),
            'capacity_notionals_usd',jsonb_build_array(25,50,100,250,500),
            'passive_fill_model','touch_proxy_no_queue_position',
            'auto_trade',false,
            'promotion_sensitive',true
        ),
        now(),
        now()
    )
    on conflict (job_key) do update
    set current_state=excluded.current_state,
        latest_successful_checkpoint=excluded.latest_successful_checkpoint,
        progress_current=excluded.progress_current,
        latest_result=excluded.latest_result,
        current_error=excluded.current_error,
        retry_state=excluded.retry_state,
        next_automatic_action=excluded.next_automatic_action,
        intervention_required=excluded.intervention_required,
        exact_intervention=excluded.exact_intervention,
        frozen_rule=excluded.frozen_rule,
        holdout_sensitive=excluded.holdout_sensitive,
        metadata=coalesce(research_hub.program_jobs.metadata,'{}'::jsonb) || excluded.metadata,
        updated_at=now();

    return new;
end;
$$;

drop trigger if exists trg_sync_xal006_live_job_state on research.xal_live_monitor_state;
create trigger trg_sync_xal006_live_job_state
after insert or update on research.xal_live_monitor_state
for each row when (new.candidate_id='XAL-006')
execute function research_hub.sync_xal006_live_job();

drop trigger if exists trg_sync_xal006_live_job_signal on research.xal_live_signals;
create trigger trg_sync_xal006_live_job_signal
after insert or update on research.xal_live_signals
for each row when (new.candidate_id='XAL-006')
execute function research_hub.sync_xal006_live_job();

update research.xal_live_monitor_state
set updated_at=now()
where candidate_id='XAL-006';