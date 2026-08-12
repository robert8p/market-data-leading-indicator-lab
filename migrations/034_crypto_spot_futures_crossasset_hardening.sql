create or replace function research_hub.recompute_crypto_spot_futures_cluster_inference_v1(
    p_run_id uuid,
    p_feature_key text
)
returns jsonb
language plpgsql
security invoker
set search_path=pg_catalog,research_hub,pg_temp
as $$
declare
    r research_hub.experiment_runs%rowtype;
    v_column text;
    v_min_discovery_clusters integer;
    v_min_validation_clusters integer;
    v_min_validation_events integer;
    v_updated bigint:=0;
begin
    select * into r from research_hub.experiment_runs where run_id=p_run_id;
    if not found then raise exception 'Unknown experiment run %',p_run_id; end if;
    if r.feature_set_key<>'crypto.spot_futures15m.v1' or r.outcome_set_key<>'crypto.binance_spot15m_returns.v1' then
        raise exception 'Run % is not typed crypto spot/futures v1',r.run_key;
    end if;
    if r.holdout_start is not null or r.holdout_end is not null or coalesce((r.config->>'holdout_accessed')::boolean,false) then
        raise exception 'Cluster inference may not access an assigned or opened holdout';
    end if;
    if p_feature_key is null or p_feature_key not like 'cf.%' then raise exception 'Invalid feature key %',p_feature_key; end if;
    v_column:=substr(p_feature_key,4);
    if not exists(
        select 1 from information_schema.columns
        where table_schema='research_hub' and table_name='crypto_spot_futures15m_features_v1'
          and column_name=v_column and data_type='double precision'
    ) then raise exception 'Feature % does not map to a typed double-precision column',p_feature_key; end if;

    v_min_discovery_clusters:=coalesce((r.config->>'minimum_discovery_clusters')::integer,30);
    v_min_validation_clusters:=coalesce((r.config->>'minimum_validation_clusters')::integer,15);
    v_min_validation_events:=coalesce((r.config->>'minimum_validation_events')::integer,100);

    create temporary table tmp_sf_cluster_stats(
        test_id bigint not null,
        phase text not null,
        cluster_n bigint not null,
        cluster_mean_net double precision,
        cluster_sd_net double precision,
        cluster_p_value double precision,
        primary key(test_id,phase)
    ) on commit drop;

    execute format($sql$
        insert into tmp_sf_cluster_stats(test_id,phase,cluster_n,cluster_mean_net,cluster_sd_net,cluster_p_value)
        with defs as materialized (
            select t.test_id,t.source_instrument,t.target_instrument,t.horizon_seconds,
                   case when t.slice_key like 'LOW_%%' then 'LOW' else 'HIGH' end tail,
                   (t.metadata->>'threshold')::double precision threshold,
                   (t.metadata->>'trade_direction')::integer trade_direction,
                   coalesce((t.metadata->>'round_trip_cost_bps')::double precision,20)/10000.0 cost
            from research_hub.experiment_tests t
            where t.run_id=$1 and t.feature_key=$2
        ), scored as materialized (
            select d.test_id,
                   case when f.decision_ts>=$3 and f.decision_ts<$4 then 'discovery'
                        when f.decision_ts>=$5 and f.decision_ts<$6 then 'validation' end phase,
                   f.decision_ts::date cluster_date,
                   d.trade_direction*o.gross_return-d.cost net_return
            from defs d
            join research_hub.crypto_spot_futures15m_features_v1 f
              on f.instrument_key=d.source_instrument
             and f.decision_ts>=$3 and f.decision_ts<$6
             and f.%I is not null
            join research_hub.crypto_spot_futures15m_outcomes_v1 o
              on o.instrument_key=d.target_instrument
             and o.decision_ts=f.decision_ts
             and o.horizon_seconds=d.horizon_seconds
             and o.gross_return is not null
            where (d.tail='LOW' and f.%I<=d.threshold)
               or (d.tail='HIGH' and f.%I>=d.threshold)
        ), daily as (
            select test_id,phase,cluster_date,avg(net_return) cluster_return
            from scored where phase is not null
            group by test_id,phase,cluster_date
        ), stats as (
            select test_id,phase,count(*)::bigint cluster_n,
                   avg(cluster_return) cluster_mean_net,
                   stddev_samp(cluster_return) cluster_sd_net
            from daily group by test_id,phase
        )
        select test_id,phase,cluster_n,cluster_mean_net,cluster_sd_net,
               case when cluster_n>1 and cluster_sd_net>0
                    then research_hub.direction_selected_pvalue_from_effect(cluster_mean_net/cluster_sd_net,cluster_n)
               end cluster_p_value
        from stats
    $sql$,v_column,v_column,v_column)
    using p_run_id,p_feature_key,r.discovery_start,r.discovery_end,r.validation_start,r.validation_end;

    update research_hub.experiment_tests t
    set effect_size=null,
        p_value=case when d.cluster_n>=v_min_discovery_clusters then d.cluster_p_value else null end,
        validation_positive=(
            coalesce(t.validation_n,0)>=v_min_validation_events
            and coalesce(t.validation_mean_net,-1e100)>0
            and coalesce(v.cluster_n,0)>=v_min_validation_clusters
            and coalesce(v.cluster_mean_net,-1e100)>0
        ),
        metadata=coalesce(t.metadata,'{}'::jsonb)||jsonb_build_object(
            'inference_unit','source_symbol_utc_date',
            'inference_method','direction_selected_two_sided_cluster_mean',
            'event_effect_size_precluster',t.effect_size,
            'cluster_inference',jsonb_build_object(
                'discovery_clusters',d.cluster_n,
                'discovery_cluster_mean_net',d.cluster_mean_net,
                'discovery_cluster_sd_net',d.cluster_sd_net,
                'discovery_cluster_p_value',case when d.cluster_n>=v_min_discovery_clusters then d.cluster_p_value else null end,
                'minimum_discovery_clusters',v_min_discovery_clusters,
                'validation_clusters',v.cluster_n,
                'validation_cluster_mean_net',v.cluster_mean_net,
                'minimum_validation_clusters',v_min_validation_clusters
            ),
            'obsolete_hit_rate_promotion_gate_applied',false,
            'holdout_accessed',false
        )
    from tmp_sf_cluster_stats d
    left join tmp_sf_cluster_stats v on v.test_id=d.test_id and v.phase='validation'
    where t.test_id=d.test_id and d.phase='discovery';
    get diagnostics v_updated=row_count;

    return jsonb_build_object('run_id',p_run_id,'feature_key',p_feature_key,'tests_updated',v_updated,
                              'minimum_discovery_clusters',v_min_discovery_clusters,
                              'minimum_validation_clusters',v_min_validation_clusters,
                              'holdout_accessed',false);
end;
$$;

create or replace function research_hub.run_crypto_spot_futures_feature_screen_task_v2(
    p_task_id bigint,
    p_worker_id text
)
returns jsonb
language plpgsql
security invoker
set search_path=pg_catalog,research_hub,pg_temp
as $$
declare
    v_task research_hub.experiment_tasks%rowtype;
    v_feature text;
    v_base jsonb;
    v_cluster jsonb;
begin
    select * into v_task from research_hub.experiment_tasks where task_id=p_task_id;
    if not found then raise exception 'Unknown experiment task %',p_task_id; end if;
    if v_task.task_type<>'crypto_spot_futures_feature_screen' then
        raise exception 'Task % has unsupported type %',p_task_id,v_task.task_type;
    end if;
    v_feature:=v_task.payload->>'feature_key';

    begin
        v_base:=research_hub.run_crypto_spot_futures_feature_screen_task_v1(p_task_id,p_worker_id);
        if coalesce(v_base->>'status','') not in ('completed','already_completed') then
            return v_base;
        end if;
        v_cluster:=research_hub.recompute_crypto_spot_futures_cluster_inference_v1(v_task.run_id,v_feature);
        update research_hub.experiment_tasks
        set status='completed',
            result_summary=coalesce(result_summary,'{}'::jsonb)||jsonb_build_object(
                'cluster_inference',v_cluster,
                'executor_version','v2_cluster_corrected',
                'holdout_accessed',false
            ),
            completed_at=now(),heartbeat_at=now(),last_error=null,updated_at=now()
        where task_id=p_task_id;
        return jsonb_build_object('task_id',p_task_id,'status','completed','feature_key',v_feature,
                                  'cluster_inference',v_cluster,'holdout_accessed',false);
    exception when others then
        delete from research_hub.experiment_tests where run_id=v_task.run_id and feature_key=v_feature;
        update research_hub.experiment_tasks
        set status='failed',last_error=left(sqlerrm,4000),completed_at=now(),updated_at=now()
        where task_id=p_task_id;
        return jsonb_build_object('task_id',p_task_id,'status','failed','feature_key',v_feature,
                                  'error',sqlerrm,'holdout_accessed',false);
    end;
end;
$$;

create or replace function research_hub.process_next_crypto_spot_futures_crossasset_task_v2()
returns jsonb
language plpgsql
security invoker
set search_path=pg_catalog,research_hub,pg_temp
as $$
declare v_task_id bigint;
begin
    if not pg_try_advisory_xact_lock(hashtext('rh-crypto-spot-futures-crossasset-task-v2')::bigint) then
        return jsonb_build_object('status','busy','holdout_accessed',false);
    end if;
    select t.task_id into v_task_id
    from research_hub.experiment_tasks t
    left join research_hub.experiment_dispatch_controls c on c.run_id=t.run_id
    where t.task_type='crypto_spot_futures_feature_screen'
      and (t.status='queued' or (t.status='failed' and t.attempts<4))
      and coalesce(c.dispatch_enabled,true)
    order by t.priority,t.task_id
    for update of t skip locked
    limit 1;
    if v_task_id is null then return jsonb_build_object('status','idle','holdout_accessed',false); end if;
    return research_hub.run_crypto_spot_futures_feature_screen_task_v2(v_task_id,'pg-cron-crypto-crossasset-v2');
end;
$$;

create or replace function research_hub.finalize_crypto_spot_futures_crossasset_v2(p_run_id uuid)
returns jsonb
language plpgsql
security invoker
set search_path=pg_catalog,research_hub,pg_temp
as $$
declare
    r research_hub.experiment_runs%rowtype;
    v_total_tasks bigint;
    v_completed_tasks bigint;
    v_failed_tasks bigint;
    v_tests bigint;
    v_candidates bigint;
    v_fdr double precision;
begin
    select * into r from research_hub.experiment_runs where run_id=p_run_id for update;
    if not found then raise exception 'Unknown experiment run %',p_run_id; end if;
    if r.feature_set_key<>'crypto.spot_futures15m.v1' or r.outcome_set_key<>'crypto.binance_spot15m_returns.v1' then
        raise exception 'Run % is not typed crypto spot/futures v1',r.run_key;
    end if;
    if r.holdout_start is not null or r.holdout_end is not null or coalesce((r.config->>'holdout_accessed')::boolean,false) then
        raise exception 'Cross-asset finalizer may not access or assign holdout';
    end if;

    select count(*),count(*) filter(where status='completed'),count(*) filter(where status='failed')
      into v_total_tasks,v_completed_tasks,v_failed_tasks
    from research_hub.experiment_tasks
    where run_id=p_run_id and task_type='crypto_spot_futures_feature_screen';
    if v_total_tasks=0 or v_completed_tasks<>v_total_tasks or v_failed_tasks>0 then
        return jsonb_build_object('status','not_ready','run_id',p_run_id,'total_tasks',v_total_tasks,
                                  'completed_tasks',v_completed_tasks,'failed_tasks',v_failed_tasks,
                                  'holdout_accessed',false);
    end if;

    v_fdr:=coalesce((r.config->>'fdr_q')::double precision,0.05);

    with ranked as(
        select test_id,p_value,row_number() over(order by p_value,test_id) rn,count(*) over() m
        from research_hub.experiment_tests
        where run_id=p_run_id and p_value is not null
    ), raw_q as(
        select test_id,rn,least(1.0,p_value*m::double precision/rn::double precision) raw_q from ranked
    ), adjusted as(
        select test_id,least(1.0,min(raw_q) over(order by rn desc rows between unbounded preceding and current row)) q_value
        from raw_q
    )
    update research_hub.experiment_tests t set q_value=a.q_value
    from adjusted a where t.run_id=p_run_id and t.test_id=a.test_id;

    with ordered as(
        select test_id,
               lag(mean_net) over(partition by run_id,feature_key,source_instrument,target_instrument,slice_key order by horizon_seconds) prev_mean,
               lag(validation_mean_net) over(partition by run_id,feature_key,source_instrument,target_instrument,slice_key order by horizon_seconds) prev_val,
               lead(mean_net) over(partition by run_id,feature_key,source_instrument,target_instrument,slice_key order by horizon_seconds) next_mean,
               lead(validation_mean_net) over(partition by run_id,feature_key,source_instrument,target_instrument,slice_key order by horizon_seconds) next_val
        from research_hub.experiment_tests where run_id=p_run_id
    )
    update research_hub.experiment_tests t
    set adjacent_horizon_positive=(
        (coalesce(o.prev_mean,-1e100)>0 and coalesce(o.prev_val,-1e100)>0)
        or (coalesce(o.next_mean,-1e100)>0 and coalesce(o.next_val,-1e100)>0)
    )
    from ordered o where t.test_id=o.test_id;

    delete from research_hub.candidate_ledger where run_id=p_run_id;
    insert into research_hub.candidate_ledger(
        candidate_id,run_id,status,descriptive_name,frozen_definition,metrics,confidence,next_test,frozen_at
    )
    select
        'RH-CF-'||upper(substr(md5(r.run_key||'|'||t.feature_key||'|'||t.source_instrument||'|'||t.target_instrument||'|'||t.slice_key||'|'||t.horizon_seconds),1,16)),
        p_run_id,
        'PREHOLDOUT_ROBUSTNESS_REQUIRED',
        t.feature_key||' '||t.source_instrument||' -> '||t.target_instrument||' '||t.slice_key||' @ '||t.horizon_seconds||'s',
        jsonb_build_object(
            'engine','research_hub_crypto_spot_futures_crossasset_v2',
            'feature_set_key',r.feature_set_key,'outcome_set_key',r.outcome_set_key,
            'feature_key',t.feature_key,'source_instrument',t.source_instrument,'target_instrument',t.target_instrument,
            'tail',case when t.slice_key like 'LOW_%' then 'LOW' else 'HIGH' end,
            'tail_quantile',t.metadata->'tail_quantile','threshold',t.metadata->'threshold',
            'trade_direction',t.metadata->'trade_direction','horizon_seconds',t.horizon_seconds,
            'round_trip_cost_bps',t.metadata->'round_trip_cost_bps',
            'discovery_period',jsonb_build_array(r.discovery_start,r.discovery_end),
            'validation_period',jsonb_build_array(r.validation_start,r.validation_end),
            'inference_unit','source_symbol_utc_date','holdout_accessed',false
        ),
        jsonb_build_object(
            'discovery',t.metadata->'discovery','validation',t.metadata->'validation',
            'cluster_inference',t.metadata->'cluster_inference','q_value',t.q_value,
            'adjacent_horizon_positive',t.adjacent_horizon_positive
        ),
        case when t.q_value<=v_fdr/10.0 then 'Strong pre-holdout' else 'Candidate pre-holdout' end,
        'Run time/symbol placebos, moving-block dependence tests and 20/50/100 bps cost stress. Do not materialize or access June 2026 holdout.',
        now()
    from research_hub.experiment_tests t
    where t.run_id=p_run_id
      and t.q_value is not null and t.q_value<=v_fdr
      and t.mean_net>0
      and t.validation_positive is true
      and t.adjacent_horizon_positive is true
    on conflict(candidate_id) do update set
        status=excluded.status,frozen_definition=excluded.frozen_definition,metrics=excluded.metrics,
        confidence=excluded.confidence,next_test=excluded.next_test,frozen_at=excluded.frozen_at,updated_at=now();

    select count(*) into v_tests from research_hub.experiment_tests where run_id=p_run_id;
    select count(*) into v_candidates from research_hub.candidate_ledger where run_id=p_run_id;
    update research_hub.experiment_runs
    set status=case when v_candidates>0 then 'preholdout_candidates_frozen' else 'rejected_discovery_validation' end,
        search_space_tests=v_tests,completed_at=now(),updated_at=now(),
        config=coalesce(config,'{}'::jsonb)||jsonb_build_object(
            'global_fdr_applied',true,'global_fdr_family_size',v_tests,
            'inference_unit','source_symbol_utc_date','obsolete_hit_rate_promotion_gate_applied',false,
            'preholdout_candidate_count',v_candidates,'holdout_accessed',false
        )
    where run_id=p_run_id;

    update research_hub.program_jobs
    set current_state=case when v_candidates>0 then 'preholdout_robustness_required' else 'rejected_discovery_validation' end,
        progress_current=v_tests,progress_total=v_tests,completion_pct=100,
        latest_successful_checkpoint=now(),current_error=null,
        retry_state=case when v_candidates>0 then 'global FDR complete; candidate definitions frozen before robustness' else 'terminal for this registered first-pass family; no holdout access' end,
        next_automatic_action=case when v_candidates>0
            then 'Run pre-holdout placebo, moving-block dependence and cost-stress suite on frozen candidates. Do not open June 2026 holdout.'
            else 'Record rejection and continue to the next registered independent family without retuning this run.' end,
        latest_result=jsonb_build_object('run_id',p_run_id,'tests',v_tests,'preholdout_candidates',v_candidates,'holdout_accessed',false),
        updated_at=now()
    where job_key='EXPERIMENT-CRYPTO-SPOT-FUTURES-V1';

    return jsonb_build_object('status',case when v_candidates>0 then 'preholdout_robustness_required' else 'rejected_discovery_validation' end,
                              'run_id',p_run_id,'tests',v_tests,'preholdout_candidates',v_candidates,
                              'holdout_accessed',false);
end;
$$;

create or replace function research_hub.advance_crypto_spot_futures_crossasset_finalize_v2()
returns jsonb
language plpgsql
security invoker
set search_path=pg_catalog,research_hub,pg_temp
as $$
declare v_run_id uuid; v_status text;
begin
    select run_id,status into v_run_id,v_status
    from research_hub.experiment_runs
    where run_key='RH-CRYPTO-SPOT-FUTURES-V1-20260812';
    if v_run_id is null then return jsonb_build_object('status','waiting_for_run','holdout_accessed',false); end if;
    if v_status in ('preholdout_candidates_frozen','rejected_discovery_validation') then
        return jsonb_build_object('status',v_status,'run_id',v_run_id,'holdout_accessed',false);
    end if;
    return research_hub.finalize_crypto_spot_futures_crossasset_v2(v_run_id);
end;
$$;

revoke all on function research_hub.recompute_crypto_spot_futures_cluster_inference_v1(uuid,text) from public,anon,authenticated;
revoke all on function research_hub.run_crypto_spot_futures_feature_screen_task_v2(bigint,text) from public,anon,authenticated;
revoke all on function research_hub.process_next_crypto_spot_futures_crossasset_task_v2() from public,anon,authenticated;
revoke all on function research_hub.finalize_crypto_spot_futures_crossasset_v2(uuid) from public,anon,authenticated;
revoke all on function research_hub.advance_crypto_spot_futures_crossasset_finalize_v2() from public,anon,authenticated;

comment on function research_hub.recompute_crypto_spot_futures_cluster_inference_v1(uuid,text) is
'Replaces raw-event inference for one frozen typed cross-asset feature family with source-symbol UTC-date cluster inference. Event economics remain unchanged; June 2026 is never read.';
comment on function research_hub.finalize_crypto_spot_futures_crossasset_v2(uuid) is
'Applies one global BH-FDR across the full typed cross-asset run and freezes only discovery+validation survivors for pre-holdout robustness. No obsolete hit-rate/worst-loss promotion gate and no holdout access.';

do $$
begin
    if exists(select 1 from cron.job where jobname='research_hub_crypto_spot_futures15m_screen_v1') then
        perform cron.unschedule((select jobid from cron.job where jobname='research_hub_crypto_spot_futures15m_screen_v1' limit 1));
    end if;
    if exists(select 1 from cron.job where jobname='research_hub_crypto_spot_futures_crossasset_task_v2') then
        perform cron.unschedule((select jobid from cron.job where jobname='research_hub_crypto_spot_futures_crossasset_task_v2' limit 1));
    end if;
    perform cron.schedule(
        'research_hub_crypto_spot_futures_crossasset_task_v2',
        '4,14,24,34,44,54 * * * *',
        'set work_mem=''96MB''; set statement_timeout=''45min''; select research_hub.process_next_crypto_spot_futures_crossasset_task_v2();'
    );
    if exists(select 1 from cron.job where jobname='research_hub_crypto_spot_futures_crossasset_finalize_v2') then
        perform cron.unschedule((select jobid from cron.job where jobname='research_hub_crypto_spot_futures_crossasset_finalize_v2' limit 1));
    end if;
    perform cron.schedule(
        'research_hub_crypto_spot_futures_crossasset_finalize_v2',
        '9,29,49 * * * *',
        'select research_hub.advance_crypto_spot_futures_crossasset_finalize_v2();'
    );
    if exists(select 1 from cron.job where jobname='research_hub_crypto_spot_futures15m_v1') then
        perform cron.unschedule((select jobid from cron.job where jobname='research_hub_crypto_spot_futures15m_v1' limit 1));
    end if;
    perform cron.schedule(
        'research_hub_crypto_spot_futures15m_v1',
        '2,9,12,19,22,29,32,39,42,49,52,59 * * * *',
        'set work_mem=''64MB''; set statement_timeout=''5min''; select research_hub.process_next_crypto_spot_futures15m_partition_v1();'
    );
end $$;