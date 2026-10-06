-- Structural events are screened in the same experiment/multiplicity family as
-- scalar features. Dispatch remains blocked until event materialisation and task
-- planning are complete, preventing premature feature-only finalisation.

create or replace function research_hub.plan_event_screen_tasks(p_run_id uuid)
returns bigint
language plpgsql
set search_path=research_hub,pg_temp
as $$
declare r research_hub.experiment_runs%rowtype; inserted_count bigint;
begin
  select * into r from research_hub.experiment_runs where run_id=p_run_id;
  if not found then raise exception 'Unknown experiment run %',p_run_id; end if;
  if r.feature_set_key is null then raise exception 'Run % has no feature set',r.run_key; end if;
  insert into research_hub.experiment_tasks(run_id,task_key,task_type,payload)
  select p_run_id,'event:'||e.event_key||':v'||e.event_version,'event_screen',
         jsonb_build_object('event_key',e.event_key,'event_version',e.event_version,'feature_set_key',r.feature_set_key,'outcome_set_key',r.outcome_set_key,'discovery_start',r.discovery_start,'discovery_end',r.discovery_end,'validation_start',r.validation_start,'validation_end',r.validation_end,'round_trip_cost_bps',coalesce(r.config->'round_trip_cost_bps','0'::jsonb),'holdout_accessed',false)
  from research_hub.event_definitions e
  where e.source_feature_set_key=r.feature_set_key and e.point_in_time_safe is true and e.enabled is true
  on conflict(run_id,task_key) do nothing;
  get diagnostics inserted_count=row_count;
  return inserted_count;
end $$;

create or replace function research_hub.run_event_screen_task(p_task_id bigint,p_worker_id text)
returns jsonb
language plpgsql
set search_path=research_hub,pg_temp
as $$
declare
  v_task research_hub.experiment_tasks%rowtype; v_run research_hub.experiment_runs%rowtype;
  v_event_key text; v_event_version integer; v_cost double precision; v_min_events integer; v_min_validation integer;
  v_min_hit double precision; v_max_wlr double precision; v_test_feature_key text; v_tests bigint;
begin
  select * into v_task from research_hub.experiment_tasks where task_id=p_task_id for update;
  if not found then raise exception 'Unknown experiment task %',p_task_id; end if;
  if v_task.status='completed' then return jsonb_build_object('task_id',p_task_id,'status','already_completed'); end if;
  if v_task.task_type<>'event_screen' then raise exception 'Task % is not an event_screen task',p_task_id; end if;
  if v_task.status in ('queued','failed') then
    update research_hub.experiment_tasks set status='running',claimed_by=p_worker_id,attempts=attempts+1,started_at=coalesce(started_at,now()),heartbeat_at=now(),completed_at=null,last_error=null,updated_at=now() where task_id=p_task_id;
    v_task.status:='running'; v_task.claimed_by:=p_worker_id;
  end if;
  if v_task.status<>'running' or v_task.claimed_by is distinct from p_worker_id then raise exception 'Task % is not claimed by worker %',p_task_id,p_worker_id; end if;
  select * into v_run from research_hub.experiment_runs where run_id=v_task.run_id;
  if not found then raise exception 'Missing experiment run for task %',p_task_id; end if;
  v_event_key:=v_task.payload->>'event_key'; v_event_version:=(v_task.payload->>'event_version')::integer;
  if v_event_key is null or v_event_version is null then raise exception 'Task % has no event definition',p_task_id; end if;
  v_test_feature_key:='event:'||v_event_key||':v'||v_event_version;
  v_cost:=coalesce((v_run.config->>'round_trip_cost_bps')::double precision,0)/10000.0;
  v_min_events:=coalesce((v_run.config->>'minimum_discovery_events')::integer,100);
  v_min_validation:=coalesce((v_run.config->>'minimum_validation_events')::integer,greatest(30,v_min_events/3));
  v_min_hit:=coalesce((v_run.config->>'minimum_hit_rate')::double precision,0.0);
  v_max_wlr:=case when v_run.config ? 'maximum_worst_loss_ratio' then (v_run.config->>'maximum_worst_loss_ratio')::double precision else null end;
  delete from research_hub.experiment_tests where run_id=v_task.run_id and feature_key=v_test_feature_key;

  create temporary table tmp_rh_event_metrics(
    phase text not null,source_instrument text not null,target_instrument text not null,horizon_seconds integer not null,trade_direction integer not null,n bigint not null,
    mean_gross double precision,mean_net double precision,median_net double precision,hit_rate_net double precision,profit_factor_net double precision,worst_net double precision,
    avg_winner_net double precision,avg_loser_net double precision,worst_loss_ratio double precision,sd_net double precision,
    primary key(phase,source_instrument,target_instrument,horizon_seconds)
  ) on commit drop;

  insert into tmp_rh_event_metrics
  with events as materialized (
    select 'cv:'||de.asset_key source_instrument,de.event_ts decision_ts,
           case when de.event_ts>=v_run.discovery_start and de.event_ts<v_run.discovery_end then 'discovery'
                when de.event_ts>=v_run.validation_start and de.event_ts<v_run.validation_end then 'validation' end phase
    from research_hub.derived_events de
    where de.event_key=v_event_key and de.event_version=v_event_version and de.available_at<=de.event_ts
      and de.event_ts>=v_run.discovery_start and de.event_ts<v_run.validation_end
      and coalesce((de.quality->>'point_in_time_safe')::boolean,false) is true
  ), event_outcomes as materialized (
    select e.source_instrument,e.decision_ts,e.phase,o.instrument_key target_instrument,o.horizon_seconds,o.gross_return
    from events e join research_hub.outcome_rows o on o.outcome_set_key=v_run.outcome_set_key and o.decision_ts=e.decision_ts and o.gross_return is not null
    where e.phase is not null
  ), directions as materialized (
    select source_instrument,target_instrument,horizon_seconds,case when avg(gross_return)>=0 then 1 else -1 end trade_direction,count(*) discovery_n
    from event_outcomes where phase='discovery' group by source_instrument,target_instrument,horizon_seconds having count(*)>=v_min_events
  ), scored as (
    select eo.phase,eo.source_instrument,eo.target_instrument,eo.horizon_seconds,d.trade_direction,d.trade_direction*eo.gross_return directed_gross,d.trade_direction*eo.gross_return-v_cost net_return
    from event_outcomes eo join directions d using(source_instrument,target_instrument,horizon_seconds)
  )
  select sc.phase,sc.source_instrument,sc.target_instrument,sc.horizon_seconds,sc.trade_direction,count(*)::bigint,
         avg(sc.directed_gross),avg(sc.net_return),percentile_cont(0.5) within group(order by sc.net_return),avg((sc.net_return>0)::integer::double precision),
         case when abs(sum(sc.net_return) filter(where sc.net_return<0))>0 then sum(sc.net_return) filter(where sc.net_return>0)/abs(sum(sc.net_return) filter(where sc.net_return<0)) end,
         min(sc.net_return),avg(sc.net_return) filter(where sc.net_return>0),avg(sc.net_return) filter(where sc.net_return<0),
         case when min(sc.net_return)>=0 then 0.0 when (avg(sc.net_return) filter(where sc.net_return>0))>0 then abs(min(sc.net_return))/(avg(sc.net_return) filter(where sc.net_return>0)) end,
         stddev_samp(sc.net_return)
  from scored sc group by sc.phase,sc.source_instrument,sc.target_instrument,sc.horizon_seconds,sc.trade_direction;

  insert into research_hub.experiment_tests
    (run_id,feature_key,outcome_key,source_instrument,target_instrument,slice_key,horizon_seconds,n,mean_gross,mean_net,median_net,hit_rate_net,profit_factor_net,worst_net,avg_winner_net,avg_loser_net,worst_loss_ratio,effect_size,validation_positive,validation_n,validation_mean_net,validation_median_net,validation_hit_rate_net,validation_profit_factor_net,validation_worst_net,validation_avg_winner_net,validation_avg_loser_net,validation_worst_loss_ratio,metadata)
  select v_task.run_id,v_test_feature_key,'horizon_'||d.horizon_seconds,d.source_instrument,d.target_instrument,'EVENT',d.horizon_seconds,d.n,d.mean_gross,d.mean_net,d.median_net,d.hit_rate_net,d.profit_factor_net,d.worst_net,d.avg_winner_net,d.avg_loser_net,d.worst_loss_ratio,
         case when d.sd_net is not null and d.sd_net>0 then d.mean_net/d.sd_net end,
         (coalesce(val.mean_net,-1e100)>0 and coalesce(val.n,0)>=v_min_validation and coalesce(val.hit_rate_net,0)>=v_min_hit and (v_max_wlr is null or coalesce(val.worst_loss_ratio,1e100)<=v_max_wlr)),
         val.n,val.mean_net,val.median_net,val.hit_rate_net,val.profit_factor_net,val.worst_net,val.avg_winner_net,val.avg_loser_net,val.worst_loss_ratio,
         jsonb_build_object('task_id',p_task_id,'screen_type','event','event_key',v_event_key,'event_version',v_event_version,'trade_direction',d.trade_direction,'round_trip_cost_bps',v_cost*10000.0,'statistical_profile_key',v_run.config->>'statistical_profile_key','discovery',jsonb_build_object('n',d.n,'mean_net',d.mean_net,'median_net',d.median_net,'hit_rate_net',d.hit_rate_net,'profit_factor_net',d.profit_factor_net,'worst_net',d.worst_net,'avg_winner_net',d.avg_winner_net,'avg_loser_net',d.avg_loser_net,'worst_loss_ratio',d.worst_loss_ratio),'validation',case when val.n is null then null else jsonb_build_object('n',val.n,'mean_net',val.mean_net,'median_net',val.median_net,'hit_rate_net',val.hit_rate_net,'profit_factor_net',val.profit_factor_net,'worst_net',val.worst_net,'avg_winner_net',val.avg_winner_net,'avg_loser_net',val.avg_loser_net,'worst_loss_ratio',val.worst_loss_ratio) end,'promotion_constraints',jsonb_build_object('minimum_hit_rate',v_min_hit,'maximum_worst_loss_ratio',v_max_wlr),'adaptive_reuse',true,'promotion_requires_future_replication',true,'holdout_accessed',false)
  from tmp_rh_event_metrics d left join tmp_rh_event_metrics val on val.phase='validation' and val.source_instrument=d.source_instrument and val.target_instrument=d.target_instrument and val.horizon_seconds=d.horizon_seconds
  where d.phase='discovery';

  select count(*) into v_tests from research_hub.experiment_tests where run_id=v_task.run_id and feature_key=v_test_feature_key;
  update research_hub.experiment_tasks set status='completed',result_summary=jsonb_build_object('tests',v_tests,'event_key',v_event_key,'event_version',v_event_version,'holdout_accessed',false),completed_at=now(),heartbeat_at=now(),updated_at=now() where task_id=p_task_id;
  return jsonb_build_object('task_id',p_task_id,'status','completed','event_key',v_event_key,'event_version',v_event_version,'tests',v_tests,'holdout_accessed',false);
exception when others then
  update research_hub.experiment_tasks set status='failed',last_error=left(sqlerrm,4000),completed_at=now(),updated_at=now() where task_id=p_task_id;
  return jsonb_build_object('task_id',p_task_id,'status','failed','error',sqlerrm,'holdout_accessed',false);
end $$;

create or replace function research_hub.register_experiment_dispatch_control_v1()
returns trigger language plpgsql set search_path=research_hub,pg_temp as $$
declare v_required text[];
begin
  if new.feature_set_key like 'crypto.crossvenue.sync.v%' or new.run_key like 'RH-CV-SYNC-V%' then
    v_required:=case when new.feature_set_key='crypto.crossvenue.sync.v2' then array['B001-24M-REPLICATION','MDM-30D-COLLECTION','FEATURE-CROSSVENUE-LAG-V2'] else array['B001-24M-REPLICATION','MDM-30D-COLLECTION'] end;
    insert into research_hub.experiment_dispatch_controls(run_id,dispatch_enabled,dispatch_class,reason,required_job_keys,metadata)
    values(new.run_id,false,'shared_primary_db','Held until the complete frozen research family is materialised/planned and primary-database workloads are terminal.',v_required,jsonb_build_object('automatic_release',true,'user_action_required',false,'complete_family_required',new.feature_set_key='crypto.crossvenue.sync.v2'))
    on conflict(run_id) do update set required_job_keys=excluded.required_job_keys,dispatch_class=excluded.dispatch_class,metadata=research_hub.experiment_dispatch_controls.metadata||excluded.metadata,updated_at=now();
  end if;
  return new;
end $$;

create or replace function research_hub.advance_crypto_crossvenue_event_tasks_v2()
returns jsonb
language plpgsql
set search_path=research_hub,pg_temp
as $$
declare v_total bigint;v_completed bigint;v_failed bigint;v_run_id uuid;v_new bigint;v_feature_tasks bigint;v_event_tasks bigint;
begin
  select count(*),count(*) filter(where status='completed'),count(*) filter(where status='failed') into v_total,v_completed,v_failed from research_hub.event_materialization_checkpoints where event_family_key='crypto.crossvenue.structural.v2';
  if v_total=0 or v_completed<>v_total then return jsonb_build_object('status','event_materialization_incomplete','completed',v_completed,'total',v_total,'failed',v_failed,'holdout_accessed',false); end if;
  select run_id into v_run_id from research_hub.experiment_runs where run_key='RH-CV-SYNC-V2-20260812';
  if v_run_id is null then return jsonb_build_object('status','waiting_v2_run','holdout_accessed',false); end if;
  v_new:=research_hub.plan_event_screen_tasks(v_run_id);
  select count(*) filter(where task_type='feature_screen'),count(*) filter(where task_type='event_screen') into v_feature_tasks,v_event_tasks from research_hub.experiment_tasks where run_id=v_run_id;
  update research_hub.program_jobs set current_state='ready_for_statistical_dispatch',retry_state='complete frozen feature+event task family planned; waiting only on primary compute dependencies',next_automatic_action='When B-001 and MDM are terminal/ready, release the complete feature+event family to the atomic research worker. Apply one global BH-FDR across all resulting tests, then validation/placebo/dependence gates. Any survivor still requires future post-definition replication.',latest_result=coalesce(latest_result,'{}'::jsonb)||jsonb_build_object('feature_tasks',v_feature_tasks,'event_tasks',v_event_tasks,'new_event_tasks_planned',v_new,'complete_multiplicity_family_planned',true,'holdout_accessed',false),current_error=null,updated_at=now() where job_key='FEATURE-CROSSVENUE-LAG-V2';
  perform research_hub.refresh_experiment_dispatch_controls_v1();
  return jsonb_build_object('status','ready_for_statistical_dispatch','feature_tasks',v_feature_tasks,'event_tasks',v_event_tasks,'new_event_tasks',v_new,'holdout_accessed',false);
end $$;

-- Amend the v2 advance path after scalar materialisation: scalar tasks are
-- planned, event checkpoints seeded, but dispatch readiness is withheld until
-- all event tasks are also in the family. (The earlier migration provides the
-- full source-repair/feature materialisation/run creation body; this replacement
-- is the production version.)
create or replace function research_hub.advance_crypto_crossvenue_research_v2()
returns jsonb
language plpgsql
set search_path=research_hub,public,pg_temp
as $$
declare v_rep_total bigint;v_rep_done bigint;v_rep_updated bigint;v_total bigint;v_completed bigint;v_failed bigint;v_running bigint;v_queued bigint;v_run_id uuid;v_tasks bigint:=0;v_pct double precision;
begin
 select count(*),count(*) filter(where status='completed'),coalesce(sum(rows_updated),0) into v_rep_total,v_rep_done,v_rep_updated from research_hub.source_repair_checkpoints where repair_key='coinbase_usd_notional_proxy_v1';
 if v_rep_total=0 or v_rep_done<>v_rep_total then
   v_pct:=case when v_rep_total>0 then 100.0*v_rep_done/v_rep_total else 0 end;
   update research_hub.program_jobs set current_state='repairing_coinbase_volume_proxy_pre_screen',progress_current=v_rep_done,progress_total=v_rep_total,completion_pct=v_pct,latest_result=jsonb_build_object('source_repair_completed_symbols',v_rep_done,'source_repair_total_symbols',v_rep_total,'rows_updated',v_rep_updated,'feature_definition_version','crypto-crossvenue-sync-v2.0','holdout_accessed',false),current_error=null,updated_at=now() where job_key='FEATURE-CROSSVENUE-LAG-V2';
   return jsonb_build_object('status','source_repair_active','completed',v_rep_done,'total',v_rep_total,'rows_updated',v_rep_updated,'holdout_accessed',false);
 end if;
 insert into research_hub.feature_materialization_checkpoints(feature_set_key,partition_key,status,metadata) select 'crypto.crossvenue.sync.v2',partition_key,'queued',jsonb_build_object('attempts',0,'definition_version','crypto-crossvenue-sync-v2.0','outcome_accessed',false) from research_hub.source_repair_checkpoints where repair_key='coinbase_usd_notional_proxy_v1' and status='completed' on conflict(feature_set_key,partition_key) do nothing;
 select count(*),count(*) filter(where status='completed'),count(*) filter(where status='failed'),count(*) filter(where status='running'),count(*) filter(where status='queued') into v_total,v_completed,v_failed,v_running,v_queued from research_hub.feature_materialization_checkpoints where feature_set_key='crypto.crossvenue.sync.v2';
 v_pct:=case when v_total>0 then 100.0*v_completed/v_total else 0 end;
 if v_total=0 or v_completed<v_total then
   update research_hub.program_jobs set current_state='materializing_v2_canonical_features',progress_current=v_completed,progress_total=v_total,completion_pct=v_pct,latest_result=jsonb_build_object('completed_symbols',v_completed,'total_symbols',v_total,'failed_symbols',v_failed,'running_symbols',v_running,'queued_symbols',v_queued,'feature_set_key','crypto.crossvenue.sync.v2','outcome_set_key','crypto.crossvenue.nextopen.v1','source_repair_completed',true,'holdout_accessed',false),retry_state=case when v_failed>0 then 'bounded automatic feature retry active' else 'automatic v2 feature checkpoint queue active' end,current_error=case when v_failed>0 then v_failed||' v2 feature partition(s) failed and are subject to bounded retry' else null end,updated_at=now() where job_key='FEATURE-CROSSVENUE-LAG-V2';
   return jsonb_build_object('status','materializing_v2_features','completed',v_completed,'total',v_total,'failed',v_failed,'holdout_accessed',false);
 end if;
 insert into research_hub.experiment_runs(run_key,name,status,feature_set_key,outcome_set_key,discovery_start,discovery_end,validation_start,validation_end,holdout_start,holdout_end,config,code_version,purpose,source_store_key,source_schema,source_table,dataset_keys,cost_model,execution_model,holdout_sealed,latest_result,provenance)
 values('RH-CV-SYNC-V2-20260812','Crypto cross-venue proxy-aware screen v2','planned','crypto.crossvenue.sync.v2','crypto.crossvenue.nextopen.v1',timestamptz '2026-06-28 00:01:00+00',timestamptz '2026-07-18 00:00:00+00',timestamptz '2026-07-18 00:00:00+00',timestamptz '2026-07-28 16:53:00+00',null,null,jsonb_build_object('engine','research_hub_chunked_feature_v1','fdr_q',0.05,'tail_quantiles',jsonb_build_array(0.02,0.05,0.10,0.20),'round_trip_cost_bps',20,'minimum_discovery_events',100,'minimum_validation_events',50,'minimum_hit_rate',0.500001,'maximum_worst_loss_ratio',0.10,'adaptive_reuse',true,'promotion_requires_future_replication',true,'no_historical_holdout_assigned',true,'holdout_accessed',false,'statistical_profile_key','crypto_crossvenue_v2','coinbase_volume_proxy_formula','base_volume * OHLC4','complete_family_includes_events',true),'crypto_crossvenue_sync_v2.0','Hypothesis-free cross-venue v2 scalar+event screen on adaptive-reuse history. No historical holdout is assigned; survivors require future replication.','market_data_primary','research_hub','crypto_crossvenue_observations_v2',array['derived.crypto_crossvenue_observations_v2'],jsonb_build_object('screening_round_trip_cost_bps',20,'candidate_specific_execution_required',true),jsonb_build_object('entry','next synchronized venue bar open after decision','horizons_seconds',jsonb_build_array(60,300,900),'historical_window_role','discovery_and_validation_only'),false,'{}'::jsonb,jsonb_build_object('adaptive_reuse',true,'future_replication_required',true,'coinbase_volume_proxy_formula','base_volume * OHLC4','native_coinbase_quote_volume_used',false)) on conflict(run_key) do nothing;
 select run_id into v_run_id from research_hub.experiment_runs where run_key='RH-CV-SYNC-V2-20260812';
 perform research_hub.bind_crypto_crossvenue_experiment_inputs_v2(v_run_id);
 v_tasks:=research_hub.plan_feature_screen_tasks(v_run_id);
 insert into research_hub.event_materialization_checkpoints(event_family_key,partition_key,status,metadata) select 'crypto.crossvenue.structural.v2',partition_key,'queued',jsonb_build_object('feature_set_key','crypto.crossvenue.sync.v2','definition_version','crypto-crossvenue-sync-v2.0','outcome_accessed',false) from research_hub.feature_materialization_checkpoints where feature_set_key='crypto.crossvenue.sync.v2' and status='completed' on conflict(event_family_key,partition_key) do nothing;
 update research_hub.program_jobs set current_state='waiting_event_materialization_pre_dispatch',progress_current=v_total,progress_total=v_total,completion_pct=100,current_error=null,retry_state='v2 scalar materialisation complete; structural event materialisation required before statistical dispatch',next_automatic_action='Materialise all frozen structural events outcome-blind; then plan event-screen tasks into the same multiplicity family. Only after that may primary compute dependencies release dispatch.',latest_successful_checkpoint=now(),latest_result=jsonb_build_object('materialized_symbols',v_total,'experiment_run_key','RH-CV-SYNC-V2-20260812','experiment_run_id',v_run_id,'new_feature_tasks_planned',v_tasks,'historical_holdout_assigned',false,'promotion_requires_future_replication',true,'event_family_required',true,'coinbase_volume_proxy_formula','base_volume * OHLC4'),updated_at=now() where job_key='FEATURE-CROSSVENUE-LAG-V2';
 return jsonb_build_object('status','waiting_event_materialization_pre_dispatch','run_id',v_run_id,'feature_tasks_planned',v_tasks,'completed_symbols',v_total,'holdout_accessed',false);
end $$;

do $$ begin
 if exists(select 1 from cron.job where jobname='research_hub_crossvenue_event_task_advance_v2') then perform cron.unschedule((select jobid from cron.job where jobname='research_hub_crossvenue_event_task_advance_v2' limit 1)); end if;
 perform cron.schedule('research_hub_crossvenue_event_task_advance_v2','*/10 * * * *','select research_hub.advance_crypto_crossvenue_event_tasks_v2();');
end $$;