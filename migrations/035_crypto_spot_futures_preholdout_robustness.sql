create or replace function research_hub.plan_crypto_spot_futures_preholdout_tasks_v1(p_run_id uuid)
returns bigint
language plpgsql
security invoker
set search_path=pg_catalog,research_hub,pg_temp
as $$
declare v_inserted bigint:=0; v_candidates bigint:=0;
begin
    if not exists(select 1 from research_hub.experiment_runs where run_id=p_run_id and feature_set_key='crypto.spot_futures15m.v1' and outcome_set_key='crypto.binance_spot15m_returns.v1') then
        raise exception 'Unknown or incompatible crypto spot/futures run %',p_run_id;
    end if;
    select count(*) into v_candidates from research_hub.candidate_ledger where run_id=p_run_id and status='PREHOLDOUT_ROBUSTNESS_REQUIRED';
    insert into research_hub.experiment_tasks(run_id,task_key,task_type,payload,priority)
    select p_run_id,'preholdout:'||c.candidate_id,'crypto_spot_futures_preholdout_robustness',
           jsonb_build_object('candidate_id',c.candidate_id,'holdout_accessed',false),60
    from research_hub.candidate_ledger c
    where c.run_id=p_run_id and c.status='PREHOLDOUT_ROBUSTNESS_REQUIRED'
    on conflict(run_id,task_key) do nothing;
    get diagnostics v_inserted=row_count;
    if v_candidates>0 then
        update research_hub.experiment_runs
        set status='preholdout_robustness_running',updated_at=now(),
            config=coalesce(config,'{}'::jsonb)||jsonb_build_object('preholdout_robustness_planned',true,'holdout_accessed',false)
        where run_id=p_run_id;
    end if;
    return v_inserted;
end;
$$;

create or replace function research_hub.run_crypto_spot_futures_preholdout_candidate_v1(
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
    r research_hub.experiment_runs%rowtype;
    c research_hub.candidate_ledger%rowtype;
    v_candidate_id text;
    v_feature text;
    v_column text;
    v_source text;
    v_target text;
    v_tail text;
    v_threshold double precision;
    v_direction integer;
    v_horizon integer;
    v_base_cost_bps double precision;
    v_base_cost double precision;
    v_tail_q double precision;
    v_min_disc_weeks integer:=12;
    v_min_val_weeks integer:=6;
    v_disc_weeks bigint:=0;
    v_val_weeks bigint:=0;
    v_disc_week_mean double precision;
    v_val_week_mean double precision;
    v_disc_week_sd double precision;
    v_disc_week_p double precision;
    v_disc_base_mean double precision;
    v_val_base_mean double precision;
    v_disc_50_mean double precision;
    v_val_50_mean double precision;
    v_disc_100_mean double precision;
    v_val_100_mean double precision;
    v_neighbor_pass boolean:=false;
    v_neighbor_count integer:=0;
    v_time_placebo_disc_max double precision;
    v_time_placebo_val_max double precision;
    v_time_placebo_pass boolean:=false;
    v_symbol_placebo_disc_max double precision;
    v_symbol_placebo_val_max double precision;
    v_inverse_disc_mean double precision;
    v_inverse_val_mean double precision;
    v_weekly_pass boolean:=false;
    v_cost50_pass boolean:=false;
    v_direction_placebo_pass boolean:=false;
    v_pass boolean:=false;
begin
    select * into v_task from research_hub.experiment_tasks where task_id=p_task_id for update;
    if not found then raise exception 'Unknown experiment task %',p_task_id; end if;
    if v_task.task_type<>'crypto_spot_futures_preholdout_robustness' then raise exception 'Unsupported task type %',v_task.task_type; end if;
    if v_task.status='completed' then return jsonb_build_object('task_id',p_task_id,'status','already_completed','holdout_accessed',false); end if;
    if v_task.status in ('queued','failed') then
        update research_hub.experiment_tasks
        set status='running',claimed_by=p_worker_id,attempts=attempts+1,started_at=coalesce(started_at,now()),heartbeat_at=now(),completed_at=null,last_error=null,updated_at=now()
        where task_id=p_task_id;
    end if;

    v_candidate_id:=v_task.payload->>'candidate_id';
    select * into c from research_hub.candidate_ledger where candidate_id=v_candidate_id and run_id=v_task.run_id;
    if not found then raise exception 'Missing candidate % for task %',v_candidate_id,p_task_id; end if;
    if c.status not in ('PREHOLDOUT_ROBUSTNESS_REQUIRED','HOLDOUT_MATERIALIZATION_ELIGIBLE','REJECTED_PREHOLDOUT_ROBUSTNESS') then
        raise exception 'Candidate % is not in a pre-holdout robustness state: %',v_candidate_id,c.status;
    end if;
    if c.status in ('HOLDOUT_MATERIALIZATION_ELIGIBLE','REJECTED_PREHOLDOUT_ROBUSTNESS') then
        update research_hub.experiment_tasks set status='completed',completed_at=now(),updated_at=now() where task_id=p_task_id;
        return jsonb_build_object('task_id',p_task_id,'status','already_evaluated','candidate_id',v_candidate_id,'candidate_status',c.status,'holdout_accessed',false);
    end if;

    select * into r from research_hub.experiment_runs where run_id=v_task.run_id;
    if r.holdout_start is not null or r.holdout_end is not null or coalesce((r.config->>'holdout_accessed')::boolean,false) then
        raise exception 'Pre-holdout robustness may not access or assign holdout';
    end if;

    v_feature:=c.frozen_definition->>'feature_key';
    v_column:=substr(v_feature,4);
    v_source:=c.frozen_definition->>'source_instrument';
    v_target:=c.frozen_definition->>'target_instrument';
    v_tail:=c.frozen_definition->>'tail';
    v_threshold:=(c.frozen_definition->>'threshold')::double precision;
    v_direction:=(c.frozen_definition->>'trade_direction')::integer;
    v_horizon:=(c.frozen_definition->>'horizon_seconds')::integer;
    v_base_cost_bps:=coalesce((c.frozen_definition->>'round_trip_cost_bps')::double precision,20);
    v_base_cost:=v_base_cost_bps/10000.0;
    v_tail_q:=coalesce((c.frozen_definition->>'tail_quantile')::double precision,0.10);
    v_min_disc_weeks:=coalesce((r.config->>'minimum_discovery_weeks')::integer,12);
    v_min_val_weeks:=coalesce((r.config->>'minimum_validation_weeks')::integer,6);

    if not exists(select 1 from information_schema.columns where table_schema='research_hub' and table_name='crypto_spot_futures15m_features_v1' and column_name=v_column and data_type='double precision') then
        raise exception 'Candidate feature % does not map to typed column',v_feature;
    end if;

    create temporary table tmp_cf_events(phase text not null,decision_ts timestamptz not null,directed_gross double precision not null,primary key(phase,decision_ts)) on commit drop;
    execute format($sql$
        insert into tmp_cf_events(phase,decision_ts,directed_gross)
        select case when f.decision_ts>=$1 and f.decision_ts<$2 then 'discovery' else 'validation' end,
               f.decision_ts,$7*o.gross_return
        from research_hub.crypto_spot_futures15m_features_v1 f
        join research_hub.crypto_spot_futures15m_outcomes_v1 o
          on o.instrument_key=$5 and o.decision_ts=f.decision_ts and o.horizon_seconds=$6 and o.gross_return is not null
        where f.instrument_key=$4 and f.decision_ts>=$1 and f.decision_ts<$3 and f.%I is not null
          and (($8='LOW' and f.%I<=$9) or ($8='HIGH' and f.%I>=$9))
    $sql$,v_column,v_column,v_column)
    using r.discovery_start,r.discovery_end,r.validation_end,v_source,v_target,v_horizon,v_direction,v_tail,v_threshold;

    select avg(directed_gross-v_base_cost) filter(where phase='discovery'),
           avg(directed_gross-v_base_cost) filter(where phase='validation'),
           avg(directed_gross-0.005) filter(where phase='discovery'),
           avg(directed_gross-0.005) filter(where phase='validation'),
           avg(directed_gross-0.010) filter(where phase='discovery'),
           avg(directed_gross-0.010) filter(where phase='validation'),
           avg(-directed_gross-v_base_cost) filter(where phase='discovery'),
           avg(-directed_gross-v_base_cost) filter(where phase='validation')
      into v_disc_base_mean,v_val_base_mean,v_disc_50_mean,v_val_50_mean,v_disc_100_mean,v_val_100_mean,v_inverse_disc_mean,v_inverse_val_mean
    from tmp_cf_events;

    with weeks as(
        select phase,date_trunc('week',decision_ts) week_start,avg(directed_gross-v_base_cost) week_mean
        from tmp_cf_events group by phase,date_trunc('week',decision_ts)
    ), agg as(
        select phase,count(*)::bigint n,avg(week_mean) mean_net,stddev_samp(week_mean) sd_net from weeks group by phase
    )
    select coalesce(max(n) filter(where phase='discovery'),0),coalesce(max(n) filter(where phase='validation'),0),
           max(mean_net) filter(where phase='discovery'),max(mean_net) filter(where phase='validation'),max(sd_net) filter(where phase='discovery')
      into v_disc_weeks,v_val_weeks,v_disc_week_mean,v_val_week_mean,v_disc_week_sd
    from agg;
    v_disc_week_p:=case when v_disc_weeks>1 and v_disc_week_sd>0 then research_hub.direction_selected_pvalue_from_effect(v_disc_week_mean/v_disc_week_sd,v_disc_weeks) end;
    v_weekly_pass:=coalesce(v_disc_weeks,0)>=v_min_disc_weeks and coalesce(v_val_weeks,0)>=v_min_val_weeks
                   and coalesce(v_disc_week_mean,-1e100)>0 and coalesce(v_val_week_mean,-1e100)>0 and coalesce(v_disc_week_p,1.0)<=0.05;

    with peers as(
        select t.*,(t.metadata->>'tail_quantile')::double precision q,
               abs((t.metadata->>'tail_quantile')::double precision-v_tail_q) dist
        from research_hub.experiment_tests t
        where t.run_id=v_task.run_id and t.feature_key=v_feature and t.source_instrument=v_source and t.target_instrument=v_target
          and t.horizon_seconds=v_horizon and split_part(t.slice_key,'_',1)=v_tail
          and (t.metadata->>'tail_quantile')::double precision<>v_tail_q
    ), nearest as(select min(dist) d from peers)
    select count(*) filter(where p.mean_net>0 and p.validation_positive is true)
      into v_neighbor_count
    from peers p cross join nearest n where p.dist=n.d;
    v_neighbor_pass:=coalesce(v_neighbor_count,0)>0;

    create temporary table tmp_cf_time_placebo(phase text not null,seed integer not null,n bigint,mean_net double precision,primary key(phase,seed)) on commit drop;
    execute format($sql$
        insert into tmp_cf_time_placebo(phase,seed,n,mean_net)
        with counts as(select phase,count(*)::bigint k from tmp_cf_events group by phase),
        seeds(seed) as(values(1),(2),(3)),
        pool as(
            select case when f.decision_ts>=$1 and f.decision_ts<$2 then 'discovery' else 'validation' end phase,
                   s.seed,f.decision_ts,
                   row_number() over(partition by case when f.decision_ts>=$1 and f.decision_ts<$2 then 'discovery' else 'validation' end,s.seed
                                     order by md5(f.decision_ts::text||$7||'|'||s.seed::text)) rn
            from research_hub.crypto_spot_futures15m_features_v1 f cross join seeds s
            where f.instrument_key=$4 and f.decision_ts>=$1 and f.decision_ts<$3 and f.%I is not null
        ), selected as(
            select p.phase,p.seed,p.decision_ts from pool p join counts c using(phase) where p.rn<=c.k
        )
        select s.phase,s.seed,count(*)::bigint,avg($8*o.gross_return-$9)
        from selected s join research_hub.crypto_spot_futures15m_outcomes_v1 o
          on o.instrument_key=$5 and o.decision_ts=s.decision_ts and o.horizon_seconds=$6 and o.gross_return is not null
        group by s.phase,s.seed
    $sql$,v_column)
    using r.discovery_start,r.discovery_end,r.validation_end,v_source,v_target,v_horizon,v_candidate_id,v_direction,v_base_cost;
    select max(mean_net) filter(where phase='discovery'),max(mean_net) filter(where phase='validation')
      into v_time_placebo_disc_max,v_time_placebo_val_max from tmp_cf_time_placebo;
    v_time_placebo_pass:=coalesce(v_disc_base_mean,-1e100)>coalesce(v_time_placebo_disc_max,1e100)
                         and coalesce(v_val_base_mean,-1e100)>coalesce(v_time_placebo_val_max,1e100);

    create temporary table tmp_cf_symbol_placebo(phase text not null,alt_target text not null,mean_net double precision,primary key(phase,alt_target)) on commit drop;
    insert into tmp_cf_symbol_placebo(phase,alt_target,mean_net)
    with targets as(
        select instrument_key from (select distinct instrument_key from research_hub.crypto_spot_futures15m_outcomes_v1) x
        where instrument_key<>v_target order by md5(v_candidate_id||'|'||instrument_key) limit 3
    )
    select e.phase,t.instrument_key,avg(v_direction*o.gross_return-v_base_cost)
    from tmp_cf_events e cross join targets t
    join research_hub.crypto_spot_futures15m_outcomes_v1 o
      on o.instrument_key=t.instrument_key and o.decision_ts=e.decision_ts and o.horizon_seconds=v_horizon and o.gross_return is not null
    group by e.phase,t.instrument_key;
    select max(mean_net) filter(where phase='discovery'),max(mean_net) filter(where phase='validation')
      into v_symbol_placebo_disc_max,v_symbol_placebo_val_max from tmp_cf_symbol_placebo;

    v_cost50_pass:=coalesce(v_disc_50_mean,-1e100)>0 and coalesce(v_val_50_mean,-1e100)>0;
    v_direction_placebo_pass:=coalesce(v_inverse_disc_mean,1e100)<0 and coalesce(v_inverse_val_mean,1e100)<0;
    v_pass:=v_weekly_pass and v_neighbor_pass and v_time_placebo_pass and v_cost50_pass and v_direction_placebo_pass;

    update research_hub.candidate_ledger
    set metrics=coalesce(metrics,'{}'::jsonb)||jsonb_build_object(
            'preholdout_robustness',jsonb_build_object(
                'passed',v_pass,
                'weekly_block',jsonb_build_object('passed',v_weekly_pass,'discovery_weeks',v_disc_weeks,'validation_weeks',v_val_weeks,'discovery_mean_net',v_disc_week_mean,'validation_mean_net',v_val_week_mean,'discovery_p_value',v_disc_week_p),
                'adjacent_tail',jsonb_build_object('passed',v_neighbor_pass,'positive_nearest_neighbors',v_neighbor_count),
                'cost_stress',jsonb_build_object('base_cost_bps',v_base_cost_bps,'discovery_base_mean',v_disc_base_mean,'validation_base_mean',v_val_base_mean,'discovery_50bps_mean',v_disc_50_mean,'validation_50bps_mean',v_val_50_mean,'discovery_100bps_mean',v_disc_100_mean,'validation_100bps_mean',v_val_100_mean,'passed_50bps',v_cost50_pass),
                'time_permutation',jsonb_build_object('method','three deterministic label permutations within source symbol and phase','max_discovery_placebo_mean',v_time_placebo_disc_max,'max_validation_placebo_mean',v_time_placebo_val_max,'passed',v_time_placebo_pass),
                'symbol_placebo',jsonb_build_object('method','three deterministic alternate targets; diagnostic only','max_discovery_placebo_mean',v_symbol_placebo_disc_max,'max_validation_placebo_mean',v_symbol_placebo_val_max),
                'direction_placebo',jsonb_build_object('inverse_discovery_mean',v_inverse_disc_mean,'inverse_validation_mean',v_inverse_val_mean,'passed',v_direction_placebo_pass),
                'holdout_accessed',false
            )
        ),
        status=case when v_pass then 'HOLDOUT_MATERIALIZATION_ELIGIBLE' else 'REJECTED_PREHOLDOUT_ROBUSTNESS' end,
        confidence=case when v_pass then 'Pre-holdout robustness passed' else 'Rejected' end,
        next_test=case when v_pass then 'Materialize the sealed June 2026 holdout only under the one-way holdout gate, then evaluate this frozen definition exactly once.'
                           else 'Do not access holdout. Preserve rejection; any redesign must be a new experiment family.' end,
        updated_at=now()
    where candidate_id=v_candidate_id;

    update research_hub.experiment_tasks
    set status='completed',result_summary=jsonb_build_object('candidate_id',v_candidate_id,'passed',v_pass,'holdout_accessed',false),
        completed_at=now(),heartbeat_at=now(),last_error=null,updated_at=now()
    where task_id=p_task_id;
    return jsonb_build_object('task_id',p_task_id,'status','completed','candidate_id',v_candidate_id,'passed',v_pass,'holdout_accessed',false);
exception when others then
    update research_hub.experiment_tasks
    set status='failed',last_error=left(sqlerrm,4000),completed_at=now(),updated_at=now()
    where task_id=p_task_id;
    return jsonb_build_object('task_id',p_task_id,'status','failed','error',sqlerrm,'holdout_accessed',false);
end;
$$;

create or replace function research_hub.process_next_crypto_spot_futures_preholdout_task_v1()
returns jsonb
language plpgsql
security invoker
set search_path=pg_catalog,research_hub,pg_temp
as $$
declare v_task_id bigint;
begin
    if not pg_try_advisory_xact_lock(hashtext('rh-crypto-spot-futures-preholdout-v1')::bigint) then return jsonb_build_object('status','busy','holdout_accessed',false); end if;
    select t.task_id into v_task_id
    from research_hub.experiment_tasks t
    left join research_hub.experiment_dispatch_controls c on c.run_id=t.run_id
    where t.task_type='crypto_spot_futures_preholdout_robustness'
      and (t.status='queued' or (t.status='failed' and t.attempts<4))
      and coalesce(c.dispatch_enabled,true)
    order by t.priority,t.task_id for update of t skip locked limit 1;
    if v_task_id is null then return jsonb_build_object('status','idle','holdout_accessed',false); end if;
    return research_hub.run_crypto_spot_futures_preholdout_candidate_v1(v_task_id,'pg-cron-crypto-preholdout-v1');
end;
$$;

create or replace function research_hub.advance_crypto_spot_futures_preholdout_v1()
returns jsonb
language plpgsql
security invoker
set search_path=pg_catalog,research_hub,pg_temp
as $$
declare v_run_id uuid; v_run_status text; v_planned bigint:=0; v_total bigint:=0; v_completed bigint:=0; v_failed bigint:=0; v_eligible bigint:=0;
begin
    select run_id,status into v_run_id,v_run_status from research_hub.experiment_runs where run_key='RH-CRYPTO-SPOT-FUTURES-V1-20260812';
    if v_run_id is null then return jsonb_build_object('status','waiting_for_run','holdout_accessed',false); end if;
    if v_run_status='preholdout_candidates_frozen' then v_planned:=research_hub.plan_crypto_spot_futures_preholdout_tasks_v1(v_run_id); end if;
    select count(*),count(*) filter(where status='completed'),count(*) filter(where status='failed' and attempts>=4)
      into v_total,v_completed,v_failed from research_hub.experiment_tasks where run_id=v_run_id and task_type='crypto_spot_futures_preholdout_robustness';
    if v_failed>0 then
        update research_hub.experiment_runs set status='preholdout_robustness_failed',updated_at=now() where run_id=v_run_id;
        update research_hub.program_jobs set current_state='preholdout_robustness_failed',current_error='One or more frozen-candidate robustness tasks exhausted retries; holdout remains sealed.',retry_state='manual forensic review required; no holdout access',updated_at=now() where job_key='EXPERIMENT-CRYPTO-SPOT-FUTURES-V1';
        return jsonb_build_object('status','preholdout_robustness_failed','tasks',v_total,'completed',v_completed,'failed',v_failed,'holdout_accessed',false);
    end if;
    if v_total=0 or v_completed<v_total then
        return jsonb_build_object('status','preholdout_robustness_running','new_tasks_planned',v_planned,'tasks',v_total,'completed',v_completed,'holdout_accessed',false);
    end if;
    select count(*) into v_eligible from research_hub.candidate_ledger where run_id=v_run_id and status='HOLDOUT_MATERIALIZATION_ELIGIBLE';
    update research_hub.experiment_runs set status=case when v_eligible>0 then 'holdout_materialization_eligible' else 'rejected_preholdout_robustness' end,updated_at=now(),
        config=coalesce(config,'{}'::jsonb)||jsonb_build_object('preholdout_robustness_complete',true,'holdout_eligible_candidates',v_eligible,'holdout_accessed',false)
    where run_id=v_run_id;
    update research_hub.program_jobs
    set current_state=case when v_eligible>0 then 'holdout_materialization_eligible' else 'rejected_preholdout_robustness' end,
        progress_current=v_completed,progress_total=v_total,completion_pct=100,latest_successful_checkpoint=now(),current_error=null,
        retry_state=case when v_eligible>0 then 'frozen candidates passed all registered pre-holdout gates; June remains sealed pending one-way materialization/evaluation' else 'terminal for this first-pass family; no holdout access' end,
        next_automatic_action=case when v_eligible>0 then 'Open the one-way sealed June holdout gate for eligible frozen candidates only; materialize/evaluate exactly once.' else 'Continue to the next independent registered family without retuning.' end,
        latest_result=jsonb_build_object('run_id',v_run_id,'tasks',v_total,'completed',v_completed,'holdout_eligible_candidates',v_eligible,'holdout_accessed',false),updated_at=now()
    where job_key='EXPERIMENT-CRYPTO-SPOT-FUTURES-V1';
    return jsonb_build_object('status',case when v_eligible>0 then 'holdout_materialization_eligible' else 'rejected_preholdout_robustness' end,'run_id',v_run_id,'holdout_eligible_candidates',v_eligible,'holdout_accessed',false);
end;
$$;

revoke all on function research_hub.plan_crypto_spot_futures_preholdout_tasks_v1(uuid) from public,anon,authenticated;
revoke all on function research_hub.run_crypto_spot_futures_preholdout_candidate_v1(bigint,text) from public,anon,authenticated;
revoke all on function research_hub.process_next_crypto_spot_futures_preholdout_task_v1() from public,anon,authenticated;
revoke all on function research_hub.advance_crypto_spot_futures_preholdout_v1() from public,anon,authenticated;

comment on function research_hub.run_crypto_spot_futures_preholdout_candidate_v1(bigint,text) is
'Evaluates one frozen cross-asset candidate using only discovery/validation data: weekly blocks, adjacent tail perturbation, 50/100 bps cost stress, deterministic within-source time permutations, alternate-target symbol placebos and inverse-direction placebo. June holdout remains untouched.';

do $$
begin
    if exists(select 1 from cron.job where jobname='research_hub_crypto_spot_futures_preholdout_advance_v1') then perform cron.unschedule((select jobid from cron.job where jobname='research_hub_crypto_spot_futures_preholdout_advance_v1' limit 1)); end if;
    perform cron.schedule('research_hub_crypto_spot_futures_preholdout_advance_v1','11,31,51 * * * *','select research_hub.advance_crypto_spot_futures_preholdout_v1();');
    if exists(select 1 from cron.job where jobname='research_hub_crypto_spot_futures_preholdout_task_v1') then perform cron.unschedule((select jobid from cron.job where jobname='research_hub_crypto_spot_futures_preholdout_task_v1' limit 1)); end if;
    perform cron.schedule('research_hub_crypto_spot_futures_preholdout_task_v1','15,35,55 * * * *','set work_mem=''96MB''; set statement_timeout=''30min''; select research_hub.process_next_crypto_spot_futures_preholdout_task_v1();');
end $$;