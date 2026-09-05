-- Automatically move the cross-venue workstream from materialisation to a frozen
-- discovery/validation experiment. The already-viewed June/July interval is never
-- assigned as an untouched holdout; future post-definition replication is required.

create or replace function research_hub.advance_crypto_crossvenue_research_v1()
returns jsonb language plpgsql security invoker set search_path=research_hub,public,pg_temp as $$
declare
  v_total bigint; v_completed bigint; v_failed bigint; v_running bigint; v_queued bigint;
  v_run_id uuid; v_tasks bigint:=0; v_pct double precision;
begin
 select count(*),count(*) filter(where status='completed'),count(*) filter(where status='failed'),count(*) filter(where status='running'),count(*) filter(where status='queued')
 into v_total,v_completed,v_failed,v_running,v_queued
 from research_hub.feature_materialization_checkpoints where feature_set_key='crypto.crossvenue.sync.v1';
 v_pct:=case when v_total>0 then 100.0*v_completed::double precision/v_total::double precision else 0 end;
 update research_hub.program_jobs set progress_current=v_completed,progress_total=v_total,completion_pct=v_pct,
   latest_successful_checkpoint=case when v_completed>0 then now() else latest_successful_checkpoint end,
   latest_result=jsonb_build_object('completed_symbols',v_completed,'total_symbols',v_total,'failed_symbols',v_failed,'running_symbols',v_running,'queued_symbols',v_queued,'feature_set_key','crypto.crossvenue.sync.v1','outcome_set_key','crypto.crossvenue.nextopen.v1'),
   current_error=case when v_failed>0 then v_failed||' symbol materialisation partition(s) currently failed and subject to bounded automatic retry' else null end,
   retry_state=case when v_failed>0 then 'bounded automatic retry active' when v_completed<v_total then 'automatic symbol checkpoint queue active' else 'materialisation complete' end,updated_at=now()
 where job_key='FEATURE-CROSSVENUE-LAG-V1';
 if v_total=0 then return jsonb_build_object('status','no_partitions_registered','holdout_accessed',false); end if;
 if v_completed<v_total then return jsonb_build_object('status','materialising','completed',v_completed,'total',v_total,'failed',v_failed,'running',v_running,'queued',v_queued,'holdout_accessed',false); end if;

 insert into research_hub.experiment_runs(run_key,name,status,feature_set_key,outcome_set_key,discovery_start,discovery_end,validation_start,validation_end,holdout_start,holdout_end,config,code_version,purpose,source_store_key,source_schema,source_table,dataset_keys,cost_model,execution_model,holdout_sealed,latest_result,provenance)
 values('RH-CV-SYNC-V1-20260812','Crypto cross-venue explicit-semantic screen v1','planned','crypto.crossvenue.sync.v1','crypto.crossvenue.nextopen.v1',
   timestamptz '2026-06-28 00:01:00+00',timestamptz '2026-07-18 00:00:00+00',timestamptz '2026-07-18 00:00:00+00',timestamptz '2026-07-28 16:53:00+00',null,null,
   jsonb_build_object('fdr_q',0.05,'tail_quantiles',jsonb_build_array(0.02,0.05,0.10,0.20),'round_trip_cost_bps',20,'minimum_discovery_events',100,'minimum_validation_events',50,'minimum_hit_rate',0.500001,'maximum_worst_loss_ratio',0.10,'adaptive_reuse',true,'promotion_requires_future_replication',true,'no_historical_holdout_assigned',true,'holdout_accessed',false,'placebo_family_required_before_promotion',true,'compute_isolation','plan tasks at completion; dispatch waits for a free compute slot or dedicated research worker'),
   'crypto_crossvenue_sync_v1','Hypothesis-free cross-venue lag/divergence screen on an already-viewed historical window. It may discover/validate but cannot provide untouched promotion evidence.',
   'market_data_primary','research_hub','crypto_crossvenue_observations_v1',array['primary.crypto_crossvenue_sync_1m_v1'],
   jsonb_build_object('screening_round_trip_cost_bps',20,'note','candidate-specific venue fees/spread/slippage required before execution promotion'),
   jsonb_build_object('entry','next synchronized venue bar open after decision','horizons_seconds',jsonb_build_array(60,300,900),'historical_window_role','discovery_and_validation_only'),false,'{}'::jsonb,
   jsonb_build_object('adaptive_reuse',true,'future_replication_required',true,'legacy_log_ratio_ignored',true,'explicit_price_gap_semantics','ln(Binance/Coinbase)'))
 on conflict(run_key) do update set feature_set_key=excluded.feature_set_key,outcome_set_key=excluded.outcome_set_key,discovery_start=excluded.discovery_start,discovery_end=excluded.discovery_end,validation_start=excluded.validation_start,validation_end=excluded.validation_end,config=excluded.config,code_version=excluded.code_version,purpose=excluded.purpose,source_store_key=excluded.source_store_key,source_schema=excluded.source_schema,source_table=excluded.source_table,dataset_keys=excluded.dataset_keys,cost_model=excluded.cost_model,execution_model=excluded.execution_model,provenance=excluded.provenance,updated_at=now();
 select run_id into v_run_id from research_hub.experiment_runs where run_key='RH-CV-SYNC-V1-20260812';
 v_tasks:=research_hub.plan_feature_screen_tasks(v_run_id);
 update research_hub.program_jobs set current_state='screen_tasks_planned_waiting_compute_slot',progress_current=v_total,progress_total=v_total,completion_pct=100,latest_successful_checkpoint=now(),current_error=null,retry_state='materialisation complete; statistical dispatch isolated from active ingestion compute',next_automatic_action='Keep the frozen feature-screen tasks queued until the primary ingestion/B-001 workload has a free compute slot or a dedicated research worker is provisioned. Then execute atomically, apply global BH-FDR, validation/placebo/dependence gates, and require future post-definition replication for any survivor.',latest_result=jsonb_build_object('materialized_symbols',v_total,'experiment_run_key','RH-CV-SYNC-V1-20260812','experiment_run_id',v_run_id,'new_tasks_planned',v_tasks,'historical_holdout_assigned',false,'promotion_requires_future_replication',true),updated_at=now()
 where job_key='FEATURE-CROSSVENUE-LAG-V1';
 return jsonb_build_object('status','screen_tasks_planned_waiting_compute_slot','run_id',v_run_id,'tasks_planned',v_tasks,'completed_symbols',v_total,'holdout_accessed',false);
end $$;

do $$ declare v_jobid bigint; begin
 select jobid into v_jobid from cron.job where jobname='research_hub_crossvenue_advance_v1' limit 1;
 if v_jobid is null then perform cron.schedule('research_hub_crossvenue_advance_v1','*/10 * * * *',$cmd$select research_hub.advance_crypto_crossvenue_research_v1();$cmd$); end if;
end $$;