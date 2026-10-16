create or replace function research_hub.plan_crypto_spot_futures_screen_tasks_v1(p_run_id uuid)
returns bigint
language plpgsql
security invoker
set search_path=pg_catalog,research_hub,pg_temp
as $$
declare
    r research_hub.experiment_runs%rowtype;
    v_inserted bigint:=0;
begin
    select * into r from research_hub.experiment_runs where run_id=p_run_id;
    if not found then raise exception 'Unknown experiment run %',p_run_id; end if;
    if r.feature_set_key<>'crypto.spot_futures15m.v1' or r.outcome_set_key<>'crypto.binance_spot15m_returns.v1' then
        raise exception 'Run % is not the typed crypto spot/futures v1 family',r.run_key;
    end if;
    if exists(select 1 from research_hub.feature_sets where feature_set_key=r.feature_set_key and point_in_time_verified is distinct from true) then
        raise exception 'Feature set % is not point-in-time verified',r.feature_set_key;
    end if;

    insert into research_hub.experiment_tasks(run_id,task_key,task_type,payload)
    select p_run_id,
           'typed_feature:'||f.feature_key,
           'crypto_spot_futures_feature_screen',
           jsonb_build_object(
               'feature_key',f.feature_key,
               'feature_set_key',r.feature_set_key,
               'outcome_set_key',r.outcome_set_key,
               'target_scope','all_materialized_instruments',
               'tail_quantiles',coalesce(r.config->'tail_quantiles','[0.02,0.05,0.10,0.20]'::jsonb),
               'round_trip_cost_bps',coalesce(r.config->'round_trip_cost_bps','20'::jsonb),
               'holdout_accessed',false
           )
    from research_hub.feature_sets fs
    cross join lateral unnest(fs.feature_keys) f(feature_key)
    where fs.feature_set_key=r.feature_set_key
    on conflict(run_id,task_key) do nothing;
    get diagnostics v_inserted=row_count;

    update research_hub.experiment_runs
    set status=case when status='planned' then 'tasks_planned' else status end,
        config=coalesce(config,'{}'::jsonb)||jsonb_build_object(
            'execution_mode','typed_atomic_tasks',
            'typed_task_type','crypto_spot_futures_feature_screen',
            'target_scope','all_26_materialized_spot_targets',
            'holdout_accessed',false
        ),
        updated_at=now()
    where run_id=p_run_id;
    return v_inserted;
end;
$$;

revoke all on function research_hub.plan_crypto_spot_futures_screen_tasks_v1(uuid) from public,anon,authenticated;

create or replace function research_hub.run_crypto_spot_futures_feature_screen_task_v1(p_task_id bigint,p_worker_id text)
returns jsonb
language plpgsql
security invoker
set search_path=pg_catalog,research_hub,pg_temp
as $$
declare
    v_task research_hub.experiment_tasks%rowtype;
    v_run research_hub.experiment_runs%rowtype;
    v_feature text;
    v_column text;
    v_cost double precision;
    v_min_events integer;
    v_min_validation integer;
    v_min_hit double precision;
    v_tests bigint:=0;
begin
    select * into v_task from research_hub.experiment_tasks where task_id=p_task_id for update;
    if not found then raise exception 'Unknown experiment task %',p_task_id; end if;
    if v_task.task_type<>'crypto_spot_futures_feature_screen' then
        raise exception 'Task % has unsupported type % for typed spot/futures executor',p_task_id,v_task.task_type;
    end if;
    if v_task.status='completed' then return jsonb_build_object('task_id',p_task_id,'status','already_completed'); end if;
    if v_task.status in ('queued','failed') then
        update research_hub.experiment_tasks
        set status='running',claimed_by=p_worker_id,attempts=attempts+1,started_at=coalesce(started_at,now()),heartbeat_at=now(),completed_at=null,last_error=null,updated_at=now()
        where task_id=p_task_id;
        v_task.status:='running';
        v_task.claimed_by:=p_worker_id;
    end if;
    if v_task.status<>'running' or v_task.claimed_by is distinct from p_worker_id then
        raise exception 'Task % is not claimed by worker %',p_task_id,p_worker_id;
    end if;

    select * into v_run from research_hub.experiment_runs where run_id=v_task.run_id;
    if not found then raise exception 'Missing experiment run for task %',p_task_id; end if;
    if v_run.feature_set_key<>'crypto.spot_futures15m.v1' or v_run.outcome_set_key<>'crypto.binance_spot15m_returns.v1' then
        raise exception 'Task % run is not typed crypto spot/futures v1',p_task_id;
    end if;
    if v_run.holdout_start is not null or v_run.holdout_end is not null or coalesce((v_run.config->>'holdout_accessed')::boolean,false) then
        raise exception 'Typed spot/futures screening task may not access holdout';
    end if;

    v_feature:=v_task.payload->>'feature_key';
    if v_feature is null or v_feature not like 'cf.%' then raise exception 'Task % has invalid feature key %',p_task_id,v_feature; end if;
    v_column:=substr(v_feature,4);
    if not exists(
        select 1 from information_schema.columns
        where table_schema='research_hub' and table_name='crypto_spot_futures15m_features_v1'
          and column_name=v_column and data_type='double precision'
    ) then raise exception 'Feature % does not map to a typed double-precision column',v_feature; end if;

    v_cost:=coalesce((v_run.config->>'round_trip_cost_bps')::double precision,20)/10000.0;
    v_min_events:=coalesce((v_run.config->>'minimum_discovery_events')::integer,200);
    v_min_validation:=coalesce((v_run.config->>'minimum_validation_events')::integer,100);
    v_min_hit:=coalesce((v_run.config->>'minimum_hit_rate')::double precision,0.0);

    delete from research_hub.experiment_tests where run_id=v_task.run_id and feature_key=v_feature;

    create temporary table tmp_sf_metrics(
        phase text not null,
        source_instrument text not null,
        tail text not null,
        tail_q double precision not null,
        threshold double precision not null,
        target_instrument text not null,
        horizon_seconds integer not null,
        trade_direction integer not null,
        n bigint not null,
        mean_gross double precision,
        mean_net double precision,
        median_net double precision,
        hit_rate_net double precision,
        profit_factor_net double precision,
        worst_net double precision,
        avg_winner_net double precision,
        avg_loser_net double precision,
        worst_loss_ratio double precision,
        sd_net double precision,
        primary key(phase,source_instrument,tail,tail_q,target_instrument,horizon_seconds)
    ) on commit drop;

    execute format($sql$
        insert into tmp_sf_metrics
        with base as materialized (
            select f.instrument_key source_instrument,f.decision_ts,f.%I::double precision feature_value,
                   case when f.decision_ts>=$1 and f.decision_ts<$2 then 'discovery'
                        when f.decision_ts>=$3 and f.decision_ts<$4 then 'validation' end phase
            from research_hub.crypto_spot_futures15m_features_v1 f
            where f.%I is not null
              and f.decision_ts>=$1 and f.decision_ts<$4
        ), quantiles as (
            select distinct x::double precision tail_q
            from jsonb_array_elements_text($5) q(x)
        ), thresholds as materialized (
            select b.source_instrument,q.tail_q,
                   percentile_cont(q.tail_q) within group(order by b.feature_value) low_cut,
                   percentile_cont(1.0-q.tail_q) within group(order by b.feature_value) high_cut
            from base b cross join quantiles q
            where b.phase='discovery'
            group by b.source_instrument,q.tail_q
        ), events as materialized (
            select b.source_instrument,b.decision_ts,b.phase,x.tail,th.tail_q,x.threshold
            from base b
            join thresholds th on th.source_instrument=b.source_instrument
            cross join lateral(values('LOW'::text,th.low_cut),('HIGH'::text,th.high_cut)) x(tail,threshold)
            where b.phase is not null
              and ((x.tail='LOW' and b.feature_value<=x.threshold) or (x.tail='HIGH' and b.feature_value>=x.threshold))
        ), event_outcomes as materialized (
            select e.source_instrument,e.decision_ts,e.phase,e.tail,e.tail_q,e.threshold,
                   o.instrument_key target_instrument,o.horizon_seconds,o.gross_return
            from events e
            join research_hub.crypto_spot_futures15m_outcomes_v1 o
              on o.decision_ts=e.decision_ts and o.gross_return is not null
        ), directions as materialized (
            select eo.source_instrument,eo.tail,eo.tail_q,eo.threshold,eo.target_instrument,eo.horizon_seconds,
                   case when avg(eo.gross_return)>=0 then 1 else -1 end trade_direction,
                   count(*) discovery_n
            from event_outcomes eo
            where eo.phase='discovery'
            group by eo.source_instrument,eo.tail,eo.tail_q,eo.threshold,eo.target_instrument,eo.horizon_seconds
            having count(*) >= $6
        ), scored as (
            select eo.phase,eo.source_instrument,eo.tail,eo.tail_q,d.threshold,eo.target_instrument,eo.horizon_seconds,d.trade_direction,
                   d.trade_direction*eo.gross_return directed_gross,
                   d.trade_direction*eo.gross_return-$7 net_return
            from event_outcomes eo
            join directions d
              on d.source_instrument=eo.source_instrument and d.tail=eo.tail and d.tail_q=eo.tail_q
             and d.target_instrument=eo.target_instrument and d.horizon_seconds=eo.horizon_seconds
        )
        select s.phase,s.source_instrument,s.tail,s.tail_q,s.threshold,s.target_instrument,s.horizon_seconds,s.trade_direction,
               count(*)::bigint,avg(s.directed_gross),avg(s.net_return),
               percentile_cont(0.5) within group(order by s.net_return),
               avg((s.net_return>0)::integer::double precision),
               case when abs(sum(s.net_return) filter(where s.net_return<0))>0
                    then sum(s.net_return) filter(where s.net_return>0)/abs(sum(s.net_return) filter(where s.net_return<0)) end,
               min(s.net_return),avg(s.net_return) filter(where s.net_return>0),avg(s.net_return) filter(where s.net_return<0),
               case when min(s.net_return)>=0 then 0.0
                    when (avg(s.net_return) filter(where s.net_return>0))>0
                    then abs(min(s.net_return))/(avg(s.net_return) filter(where s.net_return>0)) end,
               stddev_samp(s.net_return)
        from scored s
        group by s.phase,s.source_instrument,s.tail,s.tail_q,s.threshold,s.target_instrument,s.horizon_seconds,s.trade_direction
    $sql$,v_column,v_column)
    using v_run.discovery_start,v_run.discovery_end,v_run.validation_start,v_run.validation_end,
          coalesce(v_run.config->'tail_quantiles','[0.02,0.05,0.10,0.20]'::jsonb),v_min_events,v_cost;

    insert into research_hub.experiment_tests(
        run_id,feature_key,outcome_key,source_instrument,target_instrument,slice_key,horizon_seconds,n,
        mean_gross,mean_net,median_net,hit_rate_net,profit_factor_net,worst_net,avg_winner_net,avg_loser_net,worst_loss_ratio,effect_size,
        validation_positive,validation_n,validation_mean_net,validation_median_net,validation_hit_rate_net,validation_profit_factor_net,
        validation_worst_net,validation_avg_winner_net,validation_avg_loser_net,validation_worst_loss_ratio,metadata
    )
    select v_task.run_id,v_feature,'horizon_'||d.horizon_seconds,d.source_instrument,d.target_instrument,
           d.tail||'_Q'||to_char(d.tail_q,'FM0.000'),d.horizon_seconds,d.n,
           d.mean_gross,d.mean_net,d.median_net,d.hit_rate_net,d.profit_factor_net,d.worst_net,d.avg_winner_net,d.avg_loser_net,d.worst_loss_ratio,
           case when d.sd_net is not null and d.sd_net>0 then d.mean_net/d.sd_net end,
           (coalesce(val.mean_net,-1e100)>0 and coalesce(val.n,0)>=v_min_validation and coalesce(val.hit_rate_net,0)>=v_min_hit),
           val.n,val.mean_net,val.median_net,val.hit_rate_net,val.profit_factor_net,val.worst_net,val.avg_winner_net,val.avg_loser_net,val.worst_loss_ratio,
           jsonb_build_object(
               'engine','research_hub_crypto_spot_futures_typed_v1',
               'task_id',p_task_id,
               'task_type','crypto_spot_futures_feature_screen',
               'threshold',d.threshold,'tail_quantile',d.tail_q,'trade_direction',d.trade_direction,
               'round_trip_cost_bps',v_cost*10000.0,
               'target_scope','all_materialized_spot_targets',
               'discovery',jsonb_build_object('n',d.n,'mean_net',d.mean_net,'median_net',d.median_net,'hit_rate_net',d.hit_rate_net,'profit_factor_net',d.profit_factor_net,'worst_net',d.worst_net,'avg_winner_net',d.avg_winner_net,'avg_loser_net',d.avg_loser_net,'worst_loss_ratio',d.worst_loss_ratio),
               'validation',case when val.n is null then null else jsonb_build_object('n',val.n,'mean_net',val.mean_net,'median_net',val.median_net,'hit_rate_net',val.hit_rate_net,'profit_factor_net',val.profit_factor_net,'worst_net',val.worst_net,'avg_winner_net',val.avg_winner_net,'avg_loser_net',val.avg_loser_net,'worst_loss_ratio',val.worst_loss_ratio) end,
               'holdout_accessed',false
           )
    from tmp_sf_metrics d
    left join tmp_sf_metrics val
      on val.phase='validation' and val.source_instrument=d.source_instrument and val.tail=d.tail and val.tail_q=d.tail_q
     and val.target_instrument=d.target_instrument and val.horizon_seconds=d.horizon_seconds
    where d.phase='discovery';

    select count(*) into v_tests from research_hub.experiment_tests where run_id=v_task.run_id and feature_key=v_feature;
    update research_hub.experiment_tasks
    set status='completed',result_summary=jsonb_build_object('tests',v_tests,'feature_key',v_feature,'target_scope','all_materialized_spot_targets','holdout_accessed',false),
        completed_at=now(),heartbeat_at=now(),updated_at=now()
    where task_id=p_task_id;
    return jsonb_build_object('task_id',p_task_id,'status','completed','feature_key',v_feature,'tests',v_tests,'holdout_accessed',false);
exception when others then
    update research_hub.experiment_tasks
    set status='failed',last_error=left(sqlerrm,4000),completed_at=now(),updated_at=now()
    where task_id=p_task_id;
    return jsonb_build_object('task_id',p_task_id,'status','failed','error',sqlerrm,'holdout_accessed',false);
end;
$$;

revoke all on function research_hub.run_crypto_spot_futures_feature_screen_task_v1(bigint,text) from public,anon,authenticated;

create or replace function research_hub.advance_crypto_spot_futures_experiment_v1()
returns jsonb
language plpgsql
security invoker
set search_path=pg_catalog,research_hub,pg_temp
as $$
declare
    v_feature_state text;
    v_experiment_state text;
    v_holdout_untouched boolean;
    v_run_id uuid;
    v_tasks bigint:=0;
begin
    select current_state into v_feature_state from research_hub.program_jobs where job_key='FEATURE-CRYPTO-SPOT-FUTURES-V1';
    select current_state into v_experiment_state from research_hub.program_jobs where job_key='EXPERIMENT-CRYPTO-SPOT-FUTURES-V1';
    select coalesce((research_hub.crypto_spot_futures15m_materialization_status_v1()->'sealed_holdout'->>'untouched')::boolean,false)
      into v_holdout_untouched;

    if not v_holdout_untouched then
        update research_hub.program_jobs set current_state='blocked_holdout_contamination',current_error='June 2026 sealed holdout is not untouched.',retry_state='hard safety stop',updated_at=now()
        where job_key='EXPERIMENT-CRYPTO-SPOT-FUTURES-V1';
        return jsonb_build_object('status','blocked_holdout_contamination','holdout_accessed',false);
    end if;
    if v_feature_state<>'ready_for_experiment_freeze' or v_experiment_state not in ('ready_for_automatic_execution','screen_tasks_planned_waiting_compute_slot') then
        return jsonb_build_object('status','waiting','feature_state',v_feature_state,'experiment_state',v_experiment_state,'holdout_accessed',false);
    end if;

    insert into research_hub.experiment_runs(
        run_key,name,status,feature_set_key,outcome_set_key,
        discovery_start,discovery_end,validation_start,validation_end,holdout_start,holdout_end,
        config,code_version,purpose,source_store_key,source_schema,source_table,dataset_keys,cost_model,execution_model,holdout_sealed,latest_result,provenance
    ) values(
        'RH-CRYPTO-SPOT-FUTURES-V1-20260812',
        'Typed crypto spot/futures 15m globally-corrected discovery v1','planned',
        'crypto.spot_futures15m.v1','crypto.binance_spot15m_returns.v1',
        timestamptz '2025-10-01 00:00:00+00',timestamptz '2026-04-01 00:00:00+00',
        timestamptz '2026-04-01 00:00:00+00',timestamptz '2026-06-01 00:00:00+00',null,null,
        jsonb_build_object(
            'fdr_q',0.05,
            'tail_quantiles',jsonb_build_array(0.02,0.05,0.10,0.20),
            'round_trip_cost_bps',20,
            'minimum_discovery_events',200,
            'minimum_validation_events',100,
            'minimum_hit_rate',0.0,
            'engine','research_hub_crypto_spot_futures_typed_v1',
            'adaptive_reuse',true,
            'promotion_requires_sealed_holdout',true,
            'sealed_holdout_start','2026-06-01T00:00:00Z',
            'target_scope','all_26_materialized_spot_targets',
            'placebo_controls_required',jsonb_build_array('time_permutation_within_symbol','symbol_permutation','horizon_direction_placebo'),
            'dependence_tests_required',jsonb_build_array('UTC_date_cluster_bootstrap','moving_block_bootstrap'),
            'cost_stress_bps',jsonb_build_array(20,50,100),
            'holdout_accessed',false
        ),
        'crypto-spot-futures15m-typed-screen-v1',
        'Hypothesis-free typed spot/futures state search across same-asset and cross-asset future spot returns. Discovery/validation only; June 2026 remains sealed until a candidate passes multiplicity, validation, placebo, dependence and execution gates.',
        'market_data_primary','research_hub','crypto_spot_futures15m_features_v1',
        array['primary.crypto_b001_spot_15m','primary.crypto_derivatives_metrics'],
        jsonb_build_object('screening_round_trip_cost_bps',20,'stress_bps',jsonb_build_array(20,50,100)),
        jsonb_build_object('entry','typed outcome close-to-close research labels','horizons_seconds',jsonb_build_array(900,3600,14400,86400),'requires_execution_replication_before_promotion',true),
        false,'{}'::jsonb,
        jsonb_build_object('typed_point_in_time_adapter',true,'funding_observed_at_lte_decision',true,'contract_multiplier_normalized',true,'june_2026_holdout_materialized',false)
    )
    on conflict(run_key) do update set updated_at=now();

    select run_id into v_run_id from research_hub.experiment_runs where run_key='RH-CRYPTO-SPOT-FUTURES-V1-20260812';
    if exists(select 1 from research_hub.experiment_runs where run_id=v_run_id and (holdout_start is not null or holdout_end is not null or holdout_sealed)) then
        raise exception 'Typed spot/futures discovery run may not be assigned holdout before candidate gate';
    end if;
    v_tasks:=research_hub.plan_crypto_spot_futures_screen_tasks_v1(v_run_id);

    insert into research_hub.experiment_dispatch_controls(run_id,dispatch_enabled,dispatch_class,reason,required_job_keys,metadata)
    values(v_run_id,false,'shared_primary_db','Held until B-001, MDM and typed spot/futures materialization are terminal/ready.',
           array['B001-24M-REPLICATION','MDM-30D-COLLECTION','FEATURE-CRYPTO-SPOT-FUTURES-V1'],
           jsonb_build_object('automatic_release',true,'user_action_required',false,'holdout_accessed',false))
    on conflict(run_id) do update set required_job_keys=excluded.required_job_keys,dispatch_class=excluded.dispatch_class,metadata=excluded.metadata,updated_at=now();

    update research_hub.program_jobs
    set current_state='screen_tasks_planned_waiting_compute_slot',progress_current=v_tasks,progress_total=27,
        completion_pct=case when v_tasks>=27 then 100 else 100.0*v_tasks/27.0 end,
        latest_successful_checkpoint=now(),current_error=null,retry_state='27 typed feature tasks frozen; dispatch held by shared-compute gate',
        next_automatic_action='When dispatch releases, execute the 27 typed tasks atomically across all 26 spot targets and four horizons, apply one global BH-FDR, then run placebo/dependence/execution gates. Do not open June 2026 holdout until candidate promotion prerequisites pass.',
        latest_result=jsonb_build_object('run_id',v_run_id,'run_key','RH-CRYPTO-SPOT-FUTURES-V1-20260812','tasks_total',(select count(*) from research_hub.experiment_tasks where run_id=v_run_id),'new_tasks_planned',v_tasks,'target_scope','all_26_materialized_spot_targets','historical_holdout_assigned',false,'holdout_accessed',false),
        updated_at=now()
    where job_key='EXPERIMENT-CRYPTO-SPOT-FUTURES-V1';

    return jsonb_build_object('status','screen_tasks_planned_waiting_compute_slot','run_id',v_run_id,'new_tasks_planned',v_tasks,'holdout_accessed',false);
end;
$$;

revoke all on function research_hub.advance_crypto_spot_futures_experiment_v1() from public,anon,authenticated;

do $$
begin
    if exists(select 1 from cron.job where jobname='research_hub_crypto_spot_futures_experiment_advance_v1') then
        perform cron.unschedule((select jobid from cron.job where jobname='research_hub_crypto_spot_futures_experiment_advance_v1' limit 1));
    end if;
    perform cron.schedule('research_hub_crypto_spot_futures_experiment_advance_v1','8,18,28,38,48,58 * * * *','select research_hub.refresh_program_job_dependencies(); select research_hub.advance_crypto_spot_futures_experiment_v1();');
end $$;