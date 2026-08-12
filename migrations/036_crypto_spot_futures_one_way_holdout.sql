create table if not exists research_hub.crypto_spot_futures_holdout_registry_v1 (
    programme_key text primary key,
    holdout_start timestamptz not null,
    holdout_end timestamptz not null,
    max_horizon_seconds integer not null,
    expected_instruments integer not null,
    expected_feature_rows bigint not null,
    expected_outcome_rows bigint not null,
    status text not null check(status in ('SEALED_UNMATERIALIZED','MATERIALIZED_SEALED','ACCESSED_LOCKED')),
    materialized_run_id uuid references research_hub.experiment_runs(run_id),
    materialized_at timestamptz,
    accessed_at timestamptz,
    coverage_audit jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

insert into research_hub.crypto_spot_futures_holdout_registry_v1(
    programme_key,holdout_start,holdout_end,max_horizon_seconds,expected_instruments,
    expected_feature_rows,expected_outcome_rows,status,coverage_audit
) values(
    'CRYPTO-SPOT-FUTURES-V1',
    '2026-06-01 00:00:00+00','2026-06-27 00:00:00+00',86400,26,
    64896,259584,'SEALED_UNMATERIALIZED',
    jsonb_build_object(
        'defined_before_candidate_results',true,
        'selection_basis','metadata coverage only; no holdout returns or candidate outcomes inspected',
        'spot_source_min_signal_ts','2026-06-01T00:00:00Z',
        'spot_source_max_signal_ts','2026-06-28T00:00:00Z',
        'spot_rows_per_symbol_in_june_audit',2593,
        'futures_mark_max_ts','2026-06-30T23:45:00Z',
        'safe_end_reason','end-exclusive 2026-06-27 keeps the maximum 24h exit inside exact spot coverage for every decision and all 26 symbols'
    )
)
on conflict(programme_key) do update set
    holdout_start=excluded.holdout_start,
    holdout_end=excluded.holdout_end,
    max_horizon_seconds=excluded.max_horizon_seconds,
    expected_instruments=excluded.expected_instruments,
    expected_feature_rows=excluded.expected_feature_rows,
    expected_outcome_rows=excluded.expected_outcome_rows,
    coverage_audit=case
        when research_hub.crypto_spot_futures_holdout_registry_v1.status='SEALED_UNMATERIALIZED'
        then excluded.coverage_audit
        else research_hub.crypto_spot_futures_holdout_registry_v1.coverage_audit
    end,
    updated_at=now()
where research_hub.crypto_spot_futures_holdout_registry_v1.status='SEALED_UNMATERIALIZED';

create table if not exists research_hub.crypto_spot_futures15m_holdout_features_v1 (
    run_id uuid not null references research_hub.experiment_runs(run_id) on delete restrict,
    instrument_key text not null,
    decision_ts timestamptz not null,
    source_bucket_start timestamptz not null,
    funding_observed_at timestamptz,
    feature_payload jsonb not null,
    source_hash text not null,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    primary key(run_id,instrument_key,decision_ts),
    check(funding_observed_at is null or funding_observed_at<=decision_ts)
);
create index if not exists crypto_sf_holdout_features_run_ts_idx
    on research_hub.crypto_spot_futures15m_holdout_features_v1(run_id,decision_ts,instrument_key);

create table if not exists research_hub.crypto_spot_futures15m_holdout_outcomes_v1 (
    run_id uuid not null references research_hub.experiment_runs(run_id) on delete restrict,
    instrument_key text not null,
    decision_ts timestamptz not null,
    horizon_seconds integer not null,
    entry_ts timestamptz not null,
    exit_ts timestamptz not null,
    gross_return double precision not null,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    primary key(run_id,instrument_key,decision_ts,horizon_seconds)
);
create index if not exists crypto_sf_holdout_outcomes_run_ts_idx
    on research_hub.crypto_spot_futures15m_holdout_outcomes_v1(run_id,decision_ts,instrument_key,horizon_seconds);

alter table research_hub.crypto_spot_futures_holdout_registry_v1 enable row level security;
alter table research_hub.crypto_spot_futures15m_holdout_features_v1 enable row level security;
alter table research_hub.crypto_spot_futures15m_holdout_outcomes_v1 enable row level security;
revoke all on research_hub.crypto_spot_futures_holdout_registry_v1 from public,anon,authenticated;
revoke all on research_hub.crypto_spot_futures15m_holdout_features_v1 from public,anon,authenticated;
revoke all on research_hub.crypto_spot_futures15m_holdout_outcomes_v1 from public,anon,authenticated;

create or replace function research_hub.materialize_crypto_spot_futures_holdout_v1(p_run_id uuid)
returns jsonb
language plpgsql
security invoker
set search_path=pg_catalog,public,research_hub,pg_temp
as $$
declare
    r research_hub.experiment_runs%rowtype;
    h research_hub.crypto_spot_futures_holdout_registry_v1%rowtype;
    v_spot_run_id uuid;
    v_result jsonb;
    v_eligible bigint:=0;
    v_features bigint:=0;
    v_outcomes bigint:=0;
    v_instruments bigint:=0;
begin
    if not pg_try_advisory_xact_lock(hashtext('rh-crypto-spot-futures-one-way-holdout-v1')::bigint) then
        return jsonb_build_object('status','busy','holdout_accessed',false);
    end if;

    select * into r
    from research_hub.experiment_runs
    where run_id=p_run_id
    for update;
    if not found then raise exception 'Unknown experiment run %',p_run_id; end if;
    if r.run_key<>'RH-CRYPTO-SPOT-FUTURES-V1-20260812'
       or r.feature_set_key<>'crypto.spot_futures15m.v1' then
        raise exception 'Run % is not the registered crypto spot/futures holdout family',r.run_key;
    end if;
    if coalesce((r.config->>'holdout_accessed')::boolean,false) then
        raise exception 'Holdout already accessed for run %',r.run_key;
    end if;
    if r.status<>'holdout_materialization_eligible' then
        raise exception 'Run % is not holdout-materialization eligible (status=%)',r.run_key,r.status;
    end if;

    select count(*) into v_eligible
    from research_hub.candidate_ledger
    where run_id=p_run_id and status='HOLDOUT_MATERIALIZATION_ELIGIBLE';
    if v_eligible=0 then raise exception 'Run % has no eligible frozen candidates',r.run_key; end if;

    select * into h
    from research_hub.crypto_spot_futures_holdout_registry_v1
    where programme_key='CRYPTO-SPOT-FUTURES-V1'
    for update;
    if h.status='ACCESSED_LOCKED' then
        raise exception 'Registered holdout has already been accessed and is permanently locked';
    end if;
    if h.status='MATERIALIZED_SEALED' then
        if h.materialized_run_id=p_run_id then
            return jsonb_build_object('status','already_materialized','run_id',p_run_id,'holdout_accessed',false);
        end if;
        raise exception 'Registered holdout already materialized for a different run';
    end if;

    if exists(
        select 1 from research_hub.crypto_spot_futures15m_features_v1
        where decision_ts>=h.holdout_start and decision_ts<h.holdout_end
    ) or exists(
        select 1 from research_hub.crypto_spot_futures15m_outcomes_v1
        where decision_ts>=h.holdout_start and decision_ts<h.holdout_end
    ) then
        raise exception 'Holdout contamination guard: June rows already exist in discovery/validation typed caches';
    end if;

    select id into v_spot_run_id
    from public.crypto_b001_replication_runs
    where completeness_pct=100
      and complete_15m_rows>0
      and effective_start<=h.holdout_start-interval '7 days'
      and effective_end>=h.holdout_end+make_interval(secs=>h.max_horizon_seconds)
    order by complete_15m_rows desc,created_at desc
    limit 1;
    if v_spot_run_id is null then
        raise exception 'No complete spot source run covers the sealed holdout plus maximum horizon';
    end if;

    v_result:=research_hub.refresh_crypto_spot_futures15m_typed_v1(
        v_spot_run_id,h.holdout_start,h.holdout_end
    );
    if coalesce(v_result->>'status','')='busy' then
        return jsonb_build_object('status','busy_source_materializer','holdout_accessed',false);
    end if;
    if coalesce(v_result->>'status','')<>'completed' then
        raise exception 'Unexpected source materializer result: %',v_result;
    end if;

    select count(*),count(distinct instrument_key)
    into v_features,v_instruments
    from research_hub.crypto_spot_futures15m_features_v1
    where decision_ts>=h.holdout_start and decision_ts<h.holdout_end;

    select count(*) into v_outcomes
    from research_hub.crypto_spot_futures15m_outcomes_v1
    where decision_ts>=h.holdout_start and decision_ts<h.holdout_end;

    if v_features<>h.expected_feature_rows
       or v_outcomes<>h.expected_outcome_rows
       or v_instruments<>h.expected_instruments then
        raise exception 'Sealed holdout completeness failure: features=% expected=% outcomes=% expected=% instruments=% expected=%',
            v_features,h.expected_feature_rows,v_outcomes,h.expected_outcome_rows,v_instruments,h.expected_instruments;
    end if;

    if exists(
        select 1
        from research_hub.crypto_spot_futures15m_features_v1 f
        left join lateral (
            select count(*) n
            from research_hub.crypto_spot_futures15m_outcomes_v1 o
            where o.instrument_key=f.instrument_key
              and o.decision_ts=f.decision_ts
        ) q on true
        where f.decision_ts>=h.holdout_start
          and f.decision_ts<h.holdout_end
          and q.n<>4
    ) then
        raise exception 'Sealed holdout exact-horizon completeness failure';
    end if;

    insert into research_hub.crypto_spot_futures15m_holdout_features_v1(
        run_id,instrument_key,decision_ts,source_bucket_start,funding_observed_at,
        feature_payload,source_hash,metadata
    )
    select
        p_run_id,f.instrument_key,f.decision_ts,f.source_bucket_start,f.funding_observed_at,
        to_jsonb(f),f.source_hash,
        coalesce(f.metadata,'{}'::jsonb)||jsonb_build_object(
            'sealed_holdout',true,'programme_key',h.programme_key
        )
    from research_hub.crypto_spot_futures15m_features_v1 f
    where f.decision_ts>=h.holdout_start and f.decision_ts<h.holdout_end
    on conflict(run_id,instrument_key,decision_ts) do nothing;

    insert into research_hub.crypto_spot_futures15m_holdout_outcomes_v1(
        run_id,instrument_key,decision_ts,horizon_seconds,entry_ts,exit_ts,gross_return,metadata
    )
    select
        p_run_id,o.instrument_key,o.decision_ts,o.horizon_seconds,o.entry_ts,o.exit_ts,o.gross_return,
        coalesce(o.metadata,'{}'::jsonb)||jsonb_build_object(
            'sealed_holdout',true,'programme_key',h.programme_key
        )
    from research_hub.crypto_spot_futures15m_outcomes_v1 o
    where o.decision_ts>=h.holdout_start and o.decision_ts<h.holdout_end
    on conflict(run_id,instrument_key,decision_ts,horizon_seconds) do nothing;

    delete from research_hub.crypto_spot_futures15m_outcomes_v1
    where decision_ts>=h.holdout_start and decision_ts<h.holdout_end;
    delete from research_hub.crypto_spot_futures15m_features_v1
    where decision_ts>=h.holdout_start and decision_ts<h.holdout_end;

    if exists(
        select 1 from research_hub.crypto_spot_futures15m_features_v1
        where decision_ts>=h.holdout_start and decision_ts<h.holdout_end
    ) or exists(
        select 1 from research_hub.crypto_spot_futures15m_outcomes_v1
        where decision_ts>=h.holdout_start and decision_ts<h.holdout_end
    ) then
        raise exception 'Holdout isolation failure: temporary June rows remain in discovery cache';
    end if;

    update research_hub.crypto_spot_futures_holdout_registry_v1
    set status='MATERIALIZED_SEALED',
        materialized_run_id=p_run_id,
        materialized_at=now(),
        updated_at=now(),
        coverage_audit=coverage_audit||jsonb_build_object(
            'materialized_feature_rows',v_features,
            'materialized_outcome_rows',v_outcomes,
            'materialized_instruments',v_instruments,
            'source_run_id',v_spot_run_id
        )
    where programme_key=h.programme_key;

    update research_hub.experiment_runs
    set holdout_start=h.holdout_start,
        holdout_end=h.holdout_end,
        holdout_sealed=true,
        status='holdout_materialized_sealed',
        updated_at=now(),
        config=coalesce(config,'{}'::jsonb)||jsonb_build_object(
            'holdout_materialized',true,
            'holdout_accessed',false,
            'holdout_store','research_hub.crypto_spot_futures15m_holdout_*_v1',
            'holdout_interval_end_exclusive',true
        )
    where run_id=p_run_id;

    update research_hub.candidate_ledger
    set status='HOLDOUT_SEALED_READY',
        next_test='Evaluate the frozen definition exactly once on the run-scoped sealed holdout. No retuning.',
        updated_at=now()
    where run_id=p_run_id and status='HOLDOUT_MATERIALIZATION_ELIGIBLE';

    return jsonb_build_object(
        'status','holdout_materialized_sealed',
        'run_id',p_run_id,
        'holdout_start',h.holdout_start,
        'holdout_end',h.holdout_end,
        'feature_rows',v_features,
        'outcome_rows',v_outcomes,
        'instruments',v_instruments,
        'eligible_candidates',v_eligible,
        'holdout_accessed',false
    );
end;
$$;

create or replace function research_hub.evaluate_crypto_spot_futures_holdout_v1(p_run_id uuid)
returns jsonb
language plpgsql
security invoker
set search_path=pg_catalog,research_hub,pg_temp
as $$
declare
    r research_hub.experiment_runs%rowtype;
    h research_hub.crypto_spot_futures_holdout_registry_v1%rowtype;
    rec record;
    v_column text;
    v_cost double precision;
    v_n bigint;
    v_mean double precision;
    v_median double precision;
    v_mean50 double precision;
    v_days bigint;
    v_day_mean double precision;
    v_day_sd double precision;
    v_day_p double precision;
    v_weeks bigint;
    v_week_mean double precision;
    v_pass boolean;
    v_eval bigint:=0;
    v_passed bigint:=0;
    v_min_events integer:=30;
    v_min_days integer:=10;
    v_min_weeks integer:=3;
    v_p_limit double precision:=0.10;
begin
    if not pg_try_advisory_xact_lock(hashtext('rh-crypto-spot-futures-holdout-eval-v1')::bigint) then
        return jsonb_build_object('status','busy','holdout_accessed',false);
    end if;

    select * into r
    from research_hub.experiment_runs
    where run_id=p_run_id
    for update;
    if not found then raise exception 'Unknown experiment run %',p_run_id; end if;
    if r.status<>'holdout_materialized_sealed'
       or r.holdout_start is null
       or r.holdout_end is null
       or not r.holdout_sealed then
        raise exception 'Run % is not in sealed materialized holdout state',r.run_key;
    end if;
    if coalesce((r.config->>'holdout_accessed')::boolean,false) then
        raise exception 'Holdout already accessed for run %',r.run_key;
    end if;

    select * into h
    from research_hub.crypto_spot_futures_holdout_registry_v1
    where programme_key='CRYPTO-SPOT-FUTURES-V1'
    for update;
    if h.status<>'MATERIALIZED_SEALED' or h.materialized_run_id<>p_run_id then
        raise exception 'Run-scoped holdout is not materialized and sealed for this run';
    end if;

    v_min_events:=coalesce((r.config->>'minimum_holdout_events')::integer,30);
    v_min_days:=coalesce((r.config->>'minimum_holdout_days')::integer,10);
    v_min_weeks:=coalesce((r.config->>'minimum_holdout_weeks')::integer,3);
    v_p_limit:=coalesce((r.config->>'holdout_cluster_p_limit')::double precision,0.10);

    for rec in
        select *
        from research_hub.candidate_ledger
        where run_id=p_run_id and status='HOLDOUT_SEALED_READY'
        order by candidate_id
    loop
        v_column:=substr(rec.frozen_definition->>'feature_key',4);
        v_cost:=coalesce((rec.frozen_definition->>'round_trip_cost_bps')::double precision,20)/10000.0;

        execute format($sql$
            with events as materialized (
                select
                    f.decision_ts,
                    (($8)::integer)*o.gross_return-$9 net_return,
                    (($8)::integer)*o.gross_return-0.005 net50
                from research_hub.crypto_spot_futures15m_holdout_features_v1 f
                join research_hub.crypto_spot_futures15m_holdout_outcomes_v1 o
                  on o.run_id=f.run_id
                 and o.instrument_key=$5
                 and o.decision_ts=f.decision_ts
                 and o.horizon_seconds=$6
                where f.run_id=$1
                  and f.instrument_key=$4
                  and f.decision_ts>=$2
                  and f.decision_ts<$3
                  and jsonb_typeof(f.feature_payload->%L)='number'
                  and (($7='LOW' and (f.feature_payload->>%L)::double precision<=$10)
                    or ($7='HIGH' and (f.feature_payload->>%L)::double precision>=$10))
            ), daily as(
                select decision_ts::date d,avg(net_return) m
                from events group by decision_ts::date
            ), weekly as(
                select date_trunc('week',decision_ts) w,avg(net_return) m
                from events group by date_trunc('week',decision_ts)
            )
            select
                (select count(*) from events),
                (select avg(net_return) from events),
                (select percentile_cont(.5) within group(order by net_return) from events),
                (select avg(net50) from events),
                (select count(*) from daily),
                (select avg(m) from daily),
                (select stddev_samp(m) from daily),
                (select count(*) from weekly),
                (select avg(m) from weekly)
        $sql$,v_column,v_column,v_column)
        into v_n,v_mean,v_median,v_mean50,v_days,v_day_mean,v_day_sd,v_weeks,v_week_mean
        using
            p_run_id,h.holdout_start,h.holdout_end,
            rec.frozen_definition->>'source_instrument',
            rec.frozen_definition->>'target_instrument',
            (rec.frozen_definition->>'horizon_seconds')::integer,
            rec.frozen_definition->>'tail',
            (rec.frozen_definition->>'trade_direction')::integer,
            v_cost,
            (rec.frozen_definition->>'threshold')::double precision;

        v_day_p:=case
            when v_days>1 and v_day_sd>0
            then research_hub.positive_edge_pvalue(v_day_mean,v_day_sd,v_days)
        end;

        v_pass:=coalesce(v_n,0)>=v_min_events
                and coalesce(v_mean,-1e100)>0
                and coalesce(v_mean50,-1e100)>0
                and coalesce(v_days,0)>=v_min_days
                and coalesce(v_day_mean,-1e100)>0
                and coalesce(v_day_p,1.0)<=v_p_limit
                and coalesce(v_weeks,0)>=v_min_weeks
                and coalesce(v_week_mean,-1e100)>0;

        update research_hub.candidate_ledger
        set metrics=coalesce(metrics,'{}'::jsonb)||jsonb_build_object(
                'holdout',jsonb_build_object(
                    'interval_start',h.holdout_start,
                    'interval_end_exclusive',h.holdout_end,
                    'n',v_n,
                    'mean_net',v_mean,
                    'median_net',v_median,
                    'mean_net_50bps',v_mean50,
                    'utc_days',v_days,
                    'daily_cluster_mean_net',v_day_mean,
                    'daily_cluster_sd_net',v_day_sd,
                    'daily_cluster_one_sided_p',v_day_p,
                    'weeks',v_weeks,
                    'weekly_mean_net',v_week_mean,
                    'passed',v_pass,
                    'minimum_events',v_min_events,
                    'minimum_days',v_min_days,
                    'minimum_weeks',v_min_weeks,
                    'p_limit',v_p_limit
                )
            ),
            frozen_definition=frozen_definition||jsonb_build_object('holdout_accessed',true),
            status=case
                when v_pass then 'HOLDOUT_PASSED_EXECUTION_PENDING'
                else 'REJECTED_HOLDOUT'
            end,
            confidence=case
                when v_pass then 'Holdout passed; execution replication pending'
                else 'Rejected'
            end,
            next_test=case
                when v_pass then 'Replicate the frozen mechanism and executable economics on independent 1-second multi-venue microstructure before any deployment.'
                else 'Reject this frozen candidate. Any redesign must be a new experiment family.'
            end,
            updated_at=now()
        where candidate_id=rec.candidate_id;

        v_eval:=v_eval+1;
        if v_pass then v_passed:=v_passed+1; end if;
    end loop;

    if v_eval=0 then raise exception 'No HOLDOUT_SEALED_READY candidates found for run %',r.run_key; end if;

    update research_hub.crypto_spot_futures_holdout_registry_v1
    set status='ACCESSED_LOCKED',
        accessed_at=now(),
        updated_at=now(),
        coverage_audit=coverage_audit||jsonb_build_object(
            'evaluated_candidates',v_eval,
            'passed_candidates',v_passed
        )
    where programme_key=h.programme_key;

    update research_hub.experiment_runs
    set status=case
            when v_passed>0 then 'holdout_evaluated_execution_pending'
            else 'rejected_holdout'
        end,
        updated_at=now(),
        config=coalesce(config,'{}'::jsonb)||jsonb_build_object(
            'holdout_accessed',true,
            'holdout_accessed_at',now(),
            'holdout_evaluated_candidates',v_eval,
            'holdout_passed_candidates',v_passed
        )
    where run_id=p_run_id;

    update research_hub.program_jobs
    set current_state=case
            when v_passed>0 then 'holdout_passed_execution_replication_required'
            else 'rejected_holdout'
        end,
        progress_current=v_eval,
        progress_total=v_eval,
        completion_pct=100,
        latest_successful_checkpoint=now(),
        current_error=null,
        retry_state=case
            when v_passed>0 then 'sealed holdout evaluated exactly once; execution replication required'
            else 'terminal holdout rejection; no retuning'
        end,
        next_automatic_action=case
            when v_passed>0 then 'Use the independent 1-second multi-venue microstructure store to test executable fills/mechanism for the frozen holdout survivors. Do not modify their definitions.'
            else 'Continue to the next independent registered family.'
        end,
        latest_result=jsonb_build_object(
            'run_id',p_run_id,
            'holdout_evaluated',v_eval,
            'holdout_passed',v_passed,
            'holdout_accessed',true
        ),
        updated_at=now()
    where job_key='EXPERIMENT-CRYPTO-SPOT-FUTURES-V1';

    return jsonb_build_object(
        'status',case
            when v_passed>0 then 'holdout_evaluated_execution_pending'
            else 'rejected_holdout'
        end,
        'run_id',p_run_id,
        'evaluated',v_eval,
        'passed',v_passed,
        'holdout_accessed',true
    );
end;
$$;

create or replace function research_hub.advance_crypto_spot_futures_holdout_v1()
returns jsonb
language plpgsql
security invoker
set search_path=pg_catalog,research_hub,pg_temp
as $$
declare
    v_run_id uuid;
    v_status text;
begin
    select run_id,status into v_run_id,v_status
    from research_hub.experiment_runs
    where run_key='RH-CRYPTO-SPOT-FUTURES-V1-20260812';

    if v_run_id is null then
        return jsonb_build_object('status','waiting_for_run','holdout_accessed',false);
    end if;
    if v_status='holdout_materialization_eligible' then
        return research_hub.materialize_crypto_spot_futures_holdout_v1(v_run_id);
    end if;
    if v_status='holdout_materialized_sealed' then
        return research_hub.evaluate_crypto_spot_futures_holdout_v1(v_run_id);
    end if;
    if v_status in ('holdout_evaluated_execution_pending','rejected_holdout') then
        return jsonb_build_object('status',v_status,'run_id',v_run_id,'holdout_accessed',true);
    end if;

    return jsonb_build_object(
        'status','waiting','run_id',v_run_id,'run_status',v_status,'holdout_accessed',false
    );
end;
$$;

revoke all on function research_hub.materialize_crypto_spot_futures_holdout_v1(uuid) from public,anon,authenticated;
revoke all on function research_hub.evaluate_crypto_spot_futures_holdout_v1(uuid) from public,anon,authenticated;
revoke all on function research_hub.advance_crypto_spot_futures_holdout_v1() from public,anon,authenticated;

comment on table research_hub.crypto_spot_futures_holdout_registry_v1 is
'Predeclared one-way sealed holdout registry. June interval was chosen only from source-coverage metadata before candidate results existed.';
comment on function research_hub.materialize_crypto_spot_futures_holdout_v1(uuid) is
'One-way run-scoped holdout materializer. Builds the predeclared June interval inside one transaction, copies it to private sealed tables, and removes temporary rows from discovery/validation caches before commit.';
comment on function research_hub.evaluate_crypto_spot_futures_holdout_v1(uuid) is
'Evaluates only frozen holdout-eligible cross-asset candidates exactly once. Requires positive base and 50bps economics, positive daily/weekly blocks and a predeclared daily-cluster significance gate. No obsolete hit-rate/worst-loss criterion.';

do $$
begin
    if exists(select 1 from cron.job where jobname='research_hub_crypto_spot_futures_holdout_v1') then
        perform cron.unschedule((select jobid from cron.job where jobname='research_hub_crypto_spot_futures_holdout_v1' limit 1));
    end if;
    perform cron.schedule(
        'research_hub_crypto_spot_futures_holdout_v1',
        '17,37,57 * * * *',
        'set work_mem=''96MB''; set statement_timeout=''30min''; select research_hub.advance_crypto_spot_futures_holdout_v1();'
    );
end $$;