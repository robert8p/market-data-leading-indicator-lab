-- Materialise each audited source partition as soon as it is quality-ready while
-- independent public recovery continues. Legitimate later-listed/no-history symbols
-- are excluded rather than creating a permanent all-symbol completion deadlock.

create or replace function research_hub.refresh_crypto_positioning15m_work_v1()
returns jsonb
language plpgsql
security invoker
set search_path=pg_catalog,public,research_hub,pg_temp
as $$
declare
    v_deriv_pass bigint;
    v_spot_total bigint;
    v_spot_terminal bigint;
    v_spot_audited bigint;
    v_spot_pass bigint;
    v_spot_fail bigint;
    v_work_total bigint;
    v_work_done bigint;
    v_work_failed bigint;
    v_rank_total bigint;
    v_rank_done bigint;
    v_feature_rows bigint;
    v_outcome_rows bigint;
    v_state text;
begin
    select count(*) into v_deriv_pass
    from research_hub.binance_deriv_recovery_quality_v1
    where run_id='1d57032e-20fa-4d23-b066-14cc659b13e2'::uuid and data_quality_pass=true;
    select count(*),count(*) filter(where status in ('completed','completed_empty','failed'))
    into v_spot_total,v_spot_terminal
    from research_hub.binance_spot15m_positioning_work_v1;
    select count(*),count(*) filter(where quality_pass),count(*) filter(where not quality_pass)
    into v_spot_audited,v_spot_pass,v_spot_fail
    from research_hub.binance_spot15m_positioning_quality_v1;

    insert into research_hub.crypto_positioning15m_work_v1(canonical_symbol,status,metadata)
    select d.canonical_symbol,'queued',jsonb_build_object(
        'derivatives_quality_pass',true,'spot_quality_pass',true,
        'historical_window_role','discovery_validation_only','future_replication_required',true
    )
    from research_hub.binance_deriv_recovery_quality_v1 d
    join research_hub.binance_spot15m_positioning_quality_v1 s using(canonical_symbol)
    where d.run_id='1d57032e-20fa-4d23-b066-14cc659b13e2'::uuid
      and d.data_quality_pass=true and s.quality_pass=true
    on conflict(canonical_symbol) do update set
        metadata=research_hub.crypto_positioning15m_work_v1.metadata||excluded.metadata,
        status=case when research_hub.crypto_positioning15m_work_v1.status='waiting_source_quality'
                    then 'queued' else research_hub.crypto_positioning15m_work_v1.status end,
        updated_at=now();

    select count(*),count(*) filter(where status='completed'),count(*) filter(where status='failed')
    into v_work_total,v_work_done,v_work_failed
    from research_hub.crypto_positioning15m_work_v1;
    select coalesce(sum(feature_rows),0),coalesce(sum(outcome_rows),0)
    into v_feature_rows,v_outcome_rows
    from research_hub.crypto_positioning15m_work_v1;
    select count(*),count(*) filter(where status='completed')
    into v_rank_total,v_rank_done
    from research_hub.crypto_positioning15m_rank_work_v1;

    v_state:=case
        when v_work_done+v_work_failed<v_work_total then 'materializing_audited_positioning_partitions'
        when v_spot_terminal<v_spot_total then 'waiting_for_remaining_spot_recovery'
        when v_spot_audited<v_spot_terminal then 'waiting_for_remaining_spot_quality_audits'
        when v_rank_total=0 then 'ready_to_plan_cross_sectional_rank_work'
        when v_rank_done<v_rank_total then 'finalizing_cross_sectional_ranks'
        else 'ready_for_frozen_experiment_manifest' end;

    update research_hub.program_jobs
    set current_state=v_state,
        progress_current=v_work_done+v_work_failed+v_rank_done,
        progress_total=greatest(v_work_total,1)+case when v_rank_total>0 then v_rank_total else 30 end,
        completion_pct=100.0*(v_work_done+v_work_failed+v_rank_done)/
            greatest(v_work_total+greatest(v_rank_total,30),1),
        latest_result=jsonb_build_object(
            'derivatives_quality_pass_symbols',v_deriv_pass,
            'spot_source_symbols_total',v_spot_total,
            'spot_recovery_terminal_symbols',v_spot_terminal,
            'spot_quality_audited_symbols',v_spot_audited,
            'spot_quality_pass_symbols',v_spot_pass,
            'spot_quality_excluded_symbols',v_spot_fail,
            'materialization_symbols_total',v_work_total,
            'materialization_symbols_completed',v_work_done,
            'materialization_symbols_failed',v_work_failed,
            'feature_rows',v_feature_rows,
            'outcome_rows',v_outcome_rows,
            'rank_days_total',v_rank_total,
            'rank_days_completed',v_rank_done,
            'historical_holdout_available',false,
            'future_replication_required',true,
            'incremental_materialization_enabled',true
        ),
        retry_state='automatic bounded point-in-time materialization concurrent with public source recovery',
        next_automatic_action=case
            when v_state='materializing_audited_positioning_partitions'
                then 'Materialise one already audited quality-passing symbol per clean compute slot while independent source recovery continues.'
            when v_state='waiting_for_remaining_spot_recovery'
                then 'Continue public spot recovery. Legitimate later-listed/no-history paths remain excluded rather than blocking the programme.'
            when v_state='waiting_for_remaining_spot_quality_audits'
                then 'Complete remaining source-quality audits; only quality-passing symbols enter the typed panel.'
            when v_state='ready_to_plan_cross_sectional_rank_work'
                then 'Create frozen UTC-date rank tasks for the complete quality-passing typed panel.'
            when v_state='finalizing_cross_sectional_ranks'
                then 'Finalize one cross-sectional date per clean compute slot.'
            else 'Execute only the already frozen 31,122-hypothesis manifest.' end,
        intervention_required=false,
        exact_intervention=null,
        updated_at=now()
    where job_key='FEATURE-CRYPTO-POSITIONING-V1';

    return jsonb_build_object(
        'state',v_state,
        'derivatives_quality_pass',v_deriv_pass,
        'spot_total',v_spot_total,
        'spot_terminal',v_spot_terminal,
        'spot_audited',v_spot_audited,
        'spot_quality_pass',v_spot_pass,
        'spot_quality_fail',v_spot_fail,
        'work_total',v_work_total,
        'work_done',v_work_done,
        'work_failed',v_work_failed,
        'rank_total',v_rank_total,
        'rank_done',v_rank_done,
        'feature_rows',v_feature_rows,
        'outcome_rows',v_outcome_rows
    );
end;
$$;
revoke all on function research_hub.refresh_crypto_positioning15m_work_v1() from public,anon,authenticated;

create or replace function research_hub.advance_crypto_positioning15m_v1()
returns jsonb
language plpgsql
security invoker
set search_path=pg_catalog,public,research_hub,pg_temp
as $$
declare
    v_state jsonb;
    v_pressure jsonb;
    v_symbol text;
    v_date date;
    v_result jsonb;
    v_rows bigint;
    v_work_total bigint;
    v_work_done bigint;
    v_work_failed bigint;
    v_spot_total bigint;
    v_spot_terminal bigint;
    v_spot_audited bigint;
begin
    if not pg_try_advisory_xact_lock(hashtext('research_hub.advance_crypto_positioning15m_v1')) then
        return jsonb_build_object('status','busy_advisory_lock');
    end if;
    v_state:=research_hub.refresh_crypto_positioning15m_work_v1();

    select canonical_symbol into v_symbol
    from research_hub.crypto_positioning15m_work_v1
    where status in ('queued','retry_wait') and attempts<max_attempts
    order by priority desc,canonical_symbol
    for update skip locked
    limit 1;

    if v_symbol is not null then
        v_pressure:=research_hub.crypto_positioning_compute_pressure_v1();
        if coalesce((v_pressure->>'busy')::boolean,false) then
            return jsonb_build_object('status','waiting_for_clean_compute_slot','pressure',v_pressure,'state',v_state);
        end if;
        update research_hub.crypto_positioning15m_work_v1
        set status='running',attempts=attempts+1,started_at=now(),last_error=null,updated_at=now()
        where canonical_symbol=v_symbol;
        begin
            v_result:=research_hub.materialize_crypto_positioning15m_symbol_v1(v_symbol);
            update research_hub.crypto_positioning15m_work_v1
            set status='completed',feature_rows=(v_result->>'feature_rows')::bigint,
                outcome_rows=(v_result->>'outcome_rows')::bigint,
                completed_at=now(),
                metadata=metadata||jsonb_build_object('partition_hash',v_result->>'partition_hash'),
                updated_at=now()
            where canonical_symbol=v_symbol;
            perform research_hub.refresh_crypto_positioning15m_work_v1();
            return jsonb_build_object('status','symbol_completed','canonical_symbol',v_symbol,'result',v_result);
        exception when others then
            update research_hub.crypto_positioning15m_work_v1
            set status=case when attempts>=max_attempts then 'failed' else 'retry_wait' end,
                last_error=left(sqlerrm,4000),updated_at=now()
            where canonical_symbol=v_symbol;
            return jsonb_build_object('status','symbol_failed','canonical_symbol',v_symbol,'error',sqlerrm);
        end;
    end if;

    select count(*),count(*) filter(where status='completed'),count(*) filter(where status='failed')
    into v_work_total,v_work_done,v_work_failed
    from research_hub.crypto_positioning15m_work_v1;
    select count(*),count(*) filter(where status in ('completed','completed_empty','failed'))
    into v_spot_total,v_spot_terminal
    from research_hub.binance_spot15m_positioning_work_v1;
    select count(*) into v_spot_audited
    from research_hub.binance_spot15m_positioning_quality_v1;

    if v_spot_terminal<v_spot_total or v_spot_audited<v_spot_terminal then
        return jsonb_build_object('status',v_state->>'state','state',v_state);
    end if;
    if v_work_total=0 or v_work_done+v_work_failed<v_work_total then
        return jsonb_build_object('status','waiting_for_materialization_terminal','state',v_state);
    end if;

    v_pressure:=research_hub.crypto_positioning_compute_pressure_v1();
    if coalesce((v_pressure->>'busy')::boolean,false) then
        return jsonb_build_object('status','waiting_for_clean_compute_slot','pressure',v_pressure,'state',v_state);
    end if;

    if not exists(select 1 from research_hub.crypto_positioning15m_rank_work_v1) then
        insert into research_hub.crypto_positioning15m_rank_work_v1(decision_date)
        select d::date
        from generate_series('2026-07-14'::date,'2026-08-12'::date,interval '1 day') d
        on conflict do nothing;
    end if;

    select decision_date into v_date
    from research_hub.crypto_positioning15m_rank_work_v1
    where status in ('queued','retry_wait') and attempts<max_attempts
    order by decision_date
    for update skip locked
    limit 1;
    if v_date is not null then
        update research_hub.crypto_positioning15m_rank_work_v1
        set status='running',attempts=attempts+1,started_at=now(),last_error=null,updated_at=now()
        where decision_date=v_date;
        begin
            v_rows:=research_hub.finalize_crypto_positioning15m_rank_day_v1(v_date);
            update research_hub.crypto_positioning15m_rank_work_v1
            set status='completed',rows_updated=v_rows,completed_at=now(),updated_at=now()
            where decision_date=v_date;
            perform research_hub.refresh_crypto_positioning15m_work_v1();
            return jsonb_build_object('status','rank_day_completed','decision_date',v_date,'rows_updated',v_rows);
        exception when others then
            update research_hub.crypto_positioning15m_rank_work_v1
            set status=case when attempts>=max_attempts then 'failed' else 'retry_wait' end,
                last_error=left(sqlerrm,4000),updated_at=now()
            where decision_date=v_date;
            return jsonb_build_object('status','rank_day_failed','decision_date',v_date,'error',sqlerrm);
        end;
    end if;

    update research_hub.crypto_positioning15m_control_v1
    set cross_sectional_ranks_finalized=true,updated_at=now()
    where singleton=true
      and not exists(select 1 from research_hub.crypto_positioning15m_rank_work_v1 where status<>'completed');
    v_state:=research_hub.refresh_crypto_positioning15m_work_v1();
    return jsonb_build_object('status',v_state->>'state','state',v_state);
end;
$$;
revoke all on function research_hub.advance_crypto_positioning15m_v1() from public,anon,authenticated;

select research_hub.refresh_crypto_positioning15m_work_v1();
