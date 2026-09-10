-- Autonomous v2 handoff:
-- source repair -> point-in-time feature materialisation -> immutable feature input
-- binding (reusing immutable future labels) -> outcome-blind structural events ->
-- frozen task planning. Predictive dispatch remains isolated until B-001 + MDM
-- are terminal/ready, and the reused historical window can never be holdout.

update research_hub.program_jobs
set current_state='superseded_pre_screen',retry_state='terminal pre-screen supersession; no statistical tasks executed',
    next_automatic_action='No further work under v1.1. Preserve as an auditable never-executed family; continue under FEATURE-CROSSVENUE-LAG-V2.',
    latest_result=coalesce(latest_result,'{}'::jsonb)||jsonb_build_object('superseded_pre_screen',true,'replacement_job_key','FEATURE-CROSSVENUE-LAG-V2','holdout_accessed',false),updated_at=now()
where job_key='FEATURE-CROSSVENUE-LAG-V1';

insert into research_hub.program_jobs(job_key,exact_name,purpose,store_key,job_kind,current_state,progress_current,progress_total,completion_pct,latest_result,current_error,retry_state,next_automatic_action,intervention_required,exact_intervention,frozen_rule,holdout_sensitive,metadata)
values(
 'FEATURE-CROSSVENUE-LAG-V2','Cross-venue crypto lag/event feature expansion v2','Repair Coinbase notional-volume semantics, materialise the frozen v2 cross-venue feature/event family, bind immutable inputs, then run globally multiplicity-controlled discovery/validation without treating reused history as holdout.','market_data_primary','feature_materialization','repairing_coinbase_volume_proxy_pre_screen',0,20,0,'{}'::jsonb,null,'automatic source repair active','Complete bounded point-in-time Coinbase notional-volume proxy repair; then automatically materialise v2 features/events, bind immutable inputs and plan the frozen screen. Statistical dispatch stays compute-isolated and any survivor requires future replication.',false,null,true,true,
 jsonb_build_object('priority',1,'feature_set_key','crypto.crossvenue.sync.v2','outcome_set_key','crypto.crossvenue.nextopen.v1','definition_version','crypto-crossvenue-sync-v2.0','adaptive_reuse',true,'promotion_requires_future_replication',true,'coinbase_volume_proxy_formula','base_volume * OHLC4','user_action_required',false)
) on conflict(job_key) do update set exact_name=excluded.exact_name,purpose=excluded.purpose,current_state=excluded.current_state,metadata=excluded.metadata,next_automatic_action=excluded.next_automatic_action,updated_at=now();

create or replace function research_hub.materialize_next_crypto_crossvenue_symbol_v2()
returns jsonb
language plpgsql
set search_path=research_hub,public,pg_temp
as $$
declare v_symbol text; v_result jsonb; v_attempts integer; v_repairs bigint; v_repairs_done bigint;
begin
  select count(*),count(*) filter(where status='completed') into v_repairs,v_repairs_done
  from research_hub.source_repair_checkpoints where repair_key='coinbase_usd_notional_proxy_v1';
  if v_repairs=0 or v_repairs_done<>v_repairs then
    return jsonb_build_object('status','waiting_source_repair','completed_repairs',v_repairs_done,'total_repairs',v_repairs,'holdout_accessed',false);
  end if;
  if not pg_try_advisory_xact_lock(hashtext('research_hub_crossvenue_materialize_v2')::bigint) then return jsonb_build_object('status','busy','holdout_accessed',false); end if;

  insert into research_hub.feature_materialization_checkpoints(feature_set_key,partition_key,status,metadata)
  select 'crypto.crossvenue.sync.v2',partition_key,'queued',jsonb_build_object('attempts',0,'definition_version','crypto-crossvenue-sync-v2.0','outcome_accessed',false)
  from research_hub.source_repair_checkpoints where repair_key='coinbase_usd_notional_proxy_v1' and status='completed'
  on conflict(feature_set_key,partition_key) do nothing;

  select partition_key,coalesce((metadata->>'attempts')::integer,0) into v_symbol,v_attempts
  from research_hub.feature_materialization_checkpoints
  where feature_set_key='crypto.crossvenue.sync.v2' and status in ('queued','failed') and coalesce((metadata->>'attempts')::integer,0)<4
  order by case when status='failed' then 1 else 0 end,partition_key for update skip locked limit 1;
  if v_symbol is null then return jsonb_build_object('status','all_available_tasks_resolved','holdout_accessed',false); end if;
  update research_hub.feature_materialization_checkpoints set status='running',last_error=null,metadata=metadata||jsonb_build_object('attempts',v_attempts+1),updated_at=now()
   where feature_set_key='crypto.crossvenue.sync.v2' and partition_key=v_symbol;
  begin
    v_result:=research_hub.materialize_crypto_crossvenue_symbol_v2(v_symbol);
    update research_hub.feature_materialization_checkpoints
       set status='completed',row_count=(v_result->>'feature_rows')::bigint,last_source_ts=nullif(v_result->>'last_source_ts','')::timestamptz,
           code_version='crypto_crossvenue_sync_v2.0',last_error=null,
           metadata=metadata||jsonb_build_object('holdout_accessed',false,'outcome_accessed',false,'definition_version','crypto-crossvenue-sync-v2.0','coinbase_volume_proxy_formula','base_volume * OHLC4'),updated_at=now()
     where feature_set_key='crypto.crossvenue.sync.v2' and partition_key=v_symbol;
    return v_result||jsonb_build_object('status','completed');
  exception when others then
    update research_hub.feature_materialization_checkpoints set status='failed',last_error=left(sqlerrm,4000),updated_at=now()
     where feature_set_key='crypto.crossvenue.sync.v2' and partition_key=v_symbol;
    return jsonb_build_object('symbol',v_symbol,'status','failed','error',sqlerrm,'holdout_accessed',false);
  end;
end $$;

create or replace function research_hub.bind_crypto_crossvenue_experiment_inputs_v2(p_run_id uuid)
returns jsonb
language plpgsql
set search_path=research_hub,extensions,pg_temp
as $$
declare v_total bigint; v_completed bigint; v_feature_rows bigint; v_watermark timestamptz; v_manifest text; v_repairs text; v_feature_hash text; v_feature_snapshot uuid; v_outcome_snapshot uuid; v_outcome_hash text; v_existing text; v_run_key text;
begin
  select run_key into v_run_key from research_hub.experiment_runs where run_id=p_run_id;
  if v_run_key is null then raise exception 'Unknown experiment run %',p_run_id; end if;
  select count(*),count(*) filter(where status='completed'),coalesce(sum(row_count),0),max(last_source_ts)
    into v_total,v_completed,v_feature_rows,v_watermark
  from research_hub.feature_materialization_checkpoints where feature_set_key='crypto.crossvenue.sync.v2';
  if v_total=0 or v_completed<>v_total then raise exception 'Cross-venue v2 materialisation incomplete: %/%',v_completed,v_total; end if;
  select string_agg(partition_key||'|'||coalesce(row_count,0)||'|'||coalesce(last_source_ts::text,'')||'|'||coalesce(metadata->>'definition_version',''),E'\n' order by partition_key)
    into v_manifest from research_hub.feature_materialization_checkpoints where feature_set_key='crypto.crossvenue.sync.v2';
  select string_agg(partition_key||'|'||coalesce(rows_scanned,0)||'|'||coalesce(rows_updated,0)||'|'||coalesce(cursor_ts::text,''),E'\n' order by partition_key)
    into v_repairs from research_hub.source_repair_checkpoints where repair_key='coinbase_usd_notional_proxy_v1';
  v_feature_hash:=encode(digest('features|crypto-crossvenue-sync-v2.0'||E'\n'||coalesce(v_manifest,'')||E'\nrepairs\n'||coalesce(v_repairs,''),'sha256'),'hex');
  select content_hash into v_existing from research_hub.dataset_snapshots where snapshot_key='cv-sync-v2-features-20260628-20260728';
  if v_existing is not null and v_existing<>v_feature_hash then raise exception 'Immutable cross-venue v2 feature snapshot drift: existing % new %',v_existing,v_feature_hash; end if;
  insert into research_hub.dataset_snapshots(snapshot_key,dataset_key,start_ts,end_ts,row_count,content_hash,source_watermark,manifest,immutable,hash_type)
  values('cv-sync-v2-features-20260628-20260728','derived.crypto_crossvenue_features_v2',timestamptz '2026-06-28 00:01:00+00',timestamptz '2026-07-28 16:53:00+00',v_feature_rows,v_feature_hash,v_watermark::text,
    jsonb_build_object('feature_set_key','crypto.crossvenue.sync.v2','definition_version','crypto-crossvenue-sync-v2.0','symbols',v_total,'coinbase_volume_proxy_formula','base_volume * OHLC4','adaptive_reuse',true,'promotion_requires_future_replication',true),true,'manifest_sha256')
  on conflict(snapshot_key) do nothing;
  select snapshot_id into v_feature_snapshot from research_hub.dataset_snapshots where snapshot_key='cv-sync-v2-features-20260628-20260728';
  select snapshot_id,content_hash into v_outcome_snapshot,v_outcome_hash from research_hub.dataset_snapshots where snapshot_key='cv-nextopen-v1-outcomes-20260628-20260728';
  if v_outcome_snapshot is null then raise exception 'Existing immutable cross-venue outcome snapshot missing'; end if;
  insert into research_hub.experiment_inputs(run_id,input_role,dataset_key,snapshot_id,point_in_time_verified,metadata)
  values
  (p_run_id,'features','derived.crypto_crossvenue_features_v2',v_feature_snapshot,true,jsonb_build_object('feature_set_key','crypto.crossvenue.sync.v2','definition_version','crypto-crossvenue-sync-v2.0','coinbase_volume_proxy_formula','base_volume * OHLC4')),
  (p_run_id,'future_outcomes','derived.crypto_crossvenue_outcomes_v1',v_outcome_snapshot,false,jsonb_build_object('outcome_set_key','crypto.crossvenue.nextopen.v1','future_label_only',true,'never_predictor',true))
  on conflict(run_id,input_role,dataset_key) do update set snapshot_id=excluded.snapshot_id,point_in_time_verified=excluded.point_in_time_verified,metadata=excluded.metadata;
  update research_hub.experiment_runs
     set definition_hash=encode(digest(v_run_key||'|'||v_feature_hash||'|'||v_outcome_hash||'|'||coalesce(config::text,'{}'),'sha256'),'hex'),
         provenance=provenance||jsonb_build_object('feature_snapshot_id',v_feature_snapshot,'feature_snapshot_hash',v_feature_hash,'outcome_snapshot_id',v_outcome_snapshot,'outcome_snapshot_hash',v_outcome_hash,'input_manifest_bound',true,'coinbase_volume_proxy_formula','base_volume * OHLC4'),updated_at=now()
   where run_id=p_run_id;
  update research_hub.datasets set row_estimate=v_feature_rows,coverage_start=timestamptz '2026-06-28 00:01:00+00',coverage_end=timestamptz '2026-07-28 16:53:00+00',status='active',updated_at=now() where dataset_key='derived.crypto_crossvenue_features_v2';
  return jsonb_build_object('run_id',p_run_id,'feature_snapshot_id',v_feature_snapshot,'feature_hash',v_feature_hash,'feature_rows',v_feature_rows,'outcome_snapshot_id',v_outcome_snapshot,'outcome_hash',v_outcome_hash,'holdout_accessed',false);
end $$;

-- Structural events move to the corrected v2 predictor family. Volume-confirmed
-- definitions use the explicit proxy-aware volume-shock gap.
update research_hub.event_definitions
set source_feature_set_key='crypto.crossvenue.sync.v2',updated_at=now()
where event_key in ('cv.binance_lead_sign_v1','cv.coinbase_lead_sign_v1','cv.gap_flip_v1','cv.gap_acceleration_v1');

update research_hub.event_definitions
set source_feature_set_key='crypto.crossvenue.sync.v2',event_version=2,
    condition_spec=case when event_key='cv.binance_volume_confirms_v1' then jsonb_build_object('all',jsonb_build_array(jsonb_build_object('feature','cv.return_gap_1m_bc','operator','>','threshold',0),jsonb_build_object('feature','cv.volume_shock_gap_5m_bc_proxy','operator','>','threshold',0)))
                        else jsonb_build_object('all',jsonb_build_array(jsonb_build_object('feature','cv.return_gap_1m_bc','operator','<','threshold',0),jsonb_build_object('feature','cv.volume_shock_gap_5m_bc_proxy','operator','<','threshold',0))) end,
    metadata=metadata||jsonb_build_object('coinbase_volume_proxy_formula','base_volume * OHLC4','proxy_explicit',true),updated_at=now()
where event_key in ('cv.binance_volume_confirms_v1','cv.coinbase_volume_confirms_v1');

create or replace function research_hub.materialize_crypto_crossvenue_events_symbol_v2(p_symbol text)
returns jsonb
language plpgsql
set search_path=research_hub,pg_temp
as $$
declare v_symbol text:=upper(btrim(p_symbol)); v_deleted bigint; v_inserted bigint;
begin
  if v_symbol is null or v_symbol='' then raise exception 'symbol required'; end if;
  delete from research_hub.derived_events where source_dataset_key='derived.crypto_crossvenue_features_v2' and asset_key=v_symbol and event_key like 'cv.%';
  get diagnostics v_deleted=row_count;
  with base as (
    select v_symbol symbol,decision_ts,observable_at,
           (features->>'cv.return_gap_1m_bc')::double precision gap1,
           (features->>'cv.return_gap_lag1m_bc')::double precision lag1,
           (features->>'cv.volume_shock_gap_5m_bc_proxy')::double precision vol_gap
    from research_hub.feature_rows where feature_set_key='crypto.crossvenue.sync.v2' and instrument_key='cv:'||v_symbol
  ), e as (
    select 'cv.binance_lead_sign_v1'::text event_key,1 event_version,symbol,decision_ts,observable_at,jsonb_build_object('gap1',gap1) event_features from base where gap1>0
    union all select 'cv.coinbase_lead_sign_v1',1,symbol,decision_ts,observable_at,jsonb_build_object('gap1',gap1) from base where gap1<0
    union all select 'cv.binance_volume_confirms_v1',2,symbol,decision_ts,observable_at,jsonb_build_object('gap1',gap1,'volume_shock_gap_5m_bc_proxy',vol_gap) from base where gap1>0 and vol_gap>0
    union all select 'cv.coinbase_volume_confirms_v1',2,symbol,decision_ts,observable_at,jsonb_build_object('gap1',gap1,'volume_shock_gap_5m_bc_proxy',vol_gap) from base where gap1<0 and vol_gap<0
    union all select 'cv.gap_flip_v1',1,symbol,decision_ts,observable_at,jsonb_build_object('gap1',gap1,'lag1',lag1) from base where gap1 is not null and lag1 is not null and gap1*lag1<0
    union all select 'cv.gap_acceleration_v1',1,symbol,decision_ts,observable_at,jsonb_build_object('gap1',gap1,'lag1',lag1,'abs_acceleration',abs(gap1)-abs(lag1)) from base where gap1 is not null and lag1 is not null and sign(gap1)=sign(lag1) and gap1<>0 and abs(gap1)>abs(lag1)
  )
  insert into research_hub.derived_events(event_key,event_version,asset_key,event_ts,observed_at,available_at,source_dataset_key,event_features,quality,provenance)
  select event_key,event_version,symbol,decision_ts,observable_at,observable_at,'derived.crypto_crossvenue_features_v2',event_features,
         jsonb_build_object('point_in_time_safe',true,'feature_definition_version','crypto-crossvenue-sync-v2.0','adaptive_reuse',true,'outcome_accessed',false),
         jsonb_build_object('feature_set_key','crypto.crossvenue.sync.v2','source_instrument_key','cv:'||symbol,'coinbase_volume_proxy_formula','base_volume * OHLC4') from e
  on conflict(event_key,event_version,asset_key,event_ts) do update set event_features=excluded.event_features,quality=excluded.quality,provenance=excluded.provenance,observed_at=excluded.observed_at,available_at=excluded.available_at;
  get diagnostics v_inserted=row_count;
  return jsonb_build_object('symbol',v_symbol,'event_rows',v_inserted,'deleted_prior_rows',v_deleted,'outcome_accessed',false,'feature_definition_version','crypto-crossvenue-sync-v2.0');
end $$;

create or replace function research_hub.process_next_crypto_crossvenue_event_partition_v2()
returns jsonb language plpgsql set search_path=research_hub,pg_temp as $$
declare v_symbol text; v_result jsonb; v_attempts int;
begin
 if not pg_try_advisory_xact_lock(hashtext('rh-crossvenue-events-v2')::bigint) then return jsonb_build_object('status','busy'); end if;
 select partition_key,attempts into v_symbol,v_attempts from research_hub.event_materialization_checkpoints
 where event_family_key='crypto.crossvenue.structural.v2' and (status='queued' or (status='failed' and attempts<4)) order by attempts,partition_key limit 1 for update skip locked;
 if v_symbol is null then return jsonb_build_object('status','idle'); end if;
 update research_hub.event_materialization_checkpoints set status='running',attempts=attempts+1,last_error=null,updated_at=now() where event_family_key='crypto.crossvenue.structural.v2' and partition_key=v_symbol;
 begin
   v_result:=research_hub.materialize_crypto_crossvenue_events_symbol_v2(v_symbol);
   update research_hub.event_materialization_checkpoints set status='completed',row_count=(v_result->>'event_rows')::bigint,metadata=metadata||jsonb_build_object('result',v_result,'completed_at',now()),updated_at=now() where event_family_key='crypto.crossvenue.structural.v2' and partition_key=v_symbol;
   return jsonb_build_object('status','completed','result',v_result);
 exception when others then
   update research_hub.event_materialization_checkpoints set status='failed',last_error=sqlerrm,updated_at=now() where event_family_key='crypto.crossvenue.structural.v2' and partition_key=v_symbol;
   return jsonb_build_object('status','failed','symbol',v_symbol,'error',sqlerrm);
 end;
end $$;

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
   update research_hub.program_jobs set current_state='repairing_coinbase_volume_proxy_pre_screen',progress_current=v_rep_done,progress_total=v_rep_total,completion_pct=v_pct,
     latest_result=jsonb_build_object('source_repair_completed_symbols',v_rep_done,'source_repair_total_symbols',v_rep_total,'rows_updated',v_rep_updated,'feature_definition_version','crypto-crossvenue-sync-v2.0','holdout_accessed',false),
     retry_state='automatic bounded source repair active',current_error=null,updated_at=now() where job_key='FEATURE-CROSSVENUE-LAG-V2';
   return jsonb_build_object('status','source_repair_active','completed',v_rep_done,'total',v_rep_total,'rows_updated',v_rep_updated,'holdout_accessed',false);
 end if;

 insert into research_hub.feature_materialization_checkpoints(feature_set_key,partition_key,status,metadata)
 select 'crypto.crossvenue.sync.v2',partition_key,'queued',jsonb_build_object('attempts',0,'definition_version','crypto-crossvenue-sync-v2.0','outcome_accessed',false)
 from research_hub.source_repair_checkpoints where repair_key='coinbase_usd_notional_proxy_v1' and status='completed'
 on conflict(feature_set_key,partition_key) do nothing;
 select count(*),count(*) filter(where status='completed'),count(*) filter(where status='failed'),count(*) filter(where status='running'),count(*) filter(where status='queued')
 into v_total,v_completed,v_failed,v_running,v_queued from research_hub.feature_materialization_checkpoints where feature_set_key='crypto.crossvenue.sync.v2';
 v_pct:=case when v_total>0 then 100.0*v_completed/v_total else 0 end;
 if v_total=0 or v_completed<v_total then
   update research_hub.program_jobs set current_state='materializing_v2_canonical_features',progress_current=v_completed,progress_total=v_total,completion_pct=v_pct,
     latest_result=jsonb_build_object('completed_symbols',v_completed,'total_symbols',v_total,'failed_symbols',v_failed,'running_symbols',v_running,'queued_symbols',v_queued,'feature_set_key','crypto.crossvenue.sync.v2','outcome_set_key','crypto.crossvenue.nextopen.v1','source_repair_completed',true,'holdout_accessed',false),
     retry_state=case when v_failed>0 then 'bounded automatic feature retry active' else 'automatic v2 feature checkpoint queue active' end,
     current_error=case when v_failed>0 then v_failed||' v2 feature partition(s) failed and are subject to bounded retry' else null end,updated_at=now() where job_key='FEATURE-CROSSVENUE-LAG-V2';
   return jsonb_build_object('status','materializing_v2_features','completed',v_completed,'total',v_total,'failed',v_failed,'holdout_accessed',false);
 end if;

 insert into research_hub.experiment_runs(run_key,name,status,feature_set_key,outcome_set_key,discovery_start,discovery_end,validation_start,validation_end,holdout_start,holdout_end,config,code_version,purpose,source_store_key,source_schema,source_table,dataset_keys,cost_model,execution_model,holdout_sealed,latest_result,provenance)
 values('RH-CV-SYNC-V2-20260812','Crypto cross-venue proxy-aware screen v2','planned','crypto.crossvenue.sync.v2','crypto.crossvenue.nextopen.v1',
   timestamptz '2026-06-28 00:01:00+00',timestamptz '2026-07-18 00:00:00+00',timestamptz '2026-07-18 00:00:00+00',timestamptz '2026-07-28 16:53:00+00',null,null,
   jsonb_build_object('engine','research_hub_chunked_feature_v1','fdr_q',0.05,'tail_quantiles',jsonb_build_array(0.02,0.05,0.10,0.20),'round_trip_cost_bps',20,'minimum_discovery_events',100,'minimum_validation_events',50,'minimum_hit_rate',0.500001,'maximum_worst_loss_ratio',0.10,'adaptive_reuse',true,'promotion_requires_future_replication',true,'no_historical_holdout_assigned',true,'holdout_accessed',false,'statistical_profile_key','crypto_crossvenue_v2','coinbase_volume_proxy_formula','base_volume * OHLC4'),
   'crypto_crossvenue_sync_v2.0','Hypothesis-free cross-venue v2 screen on adaptive-reuse history. Predictor representation includes an explicit point-in-time Coinbase notional-volume proxy; this historical run cannot promote without future replication.',
   'market_data_primary','research_hub','crypto_crossvenue_observations_v2',array['derived.crypto_crossvenue_observations_v2'],
   jsonb_build_object('screening_round_trip_cost_bps',20,'candidate_specific_execution_required',true),
   jsonb_build_object('entry','next synchronized venue bar open after decision','horizons_seconds',jsonb_build_array(60,300,900),'historical_window_role','discovery_and_validation_only'),false,'{}'::jsonb,
   jsonb_build_object('adaptive_reuse',true,'future_replication_required',true,'coinbase_volume_proxy_formula','base_volume * OHLC4','native_coinbase_quote_volume_used',false))
 on conflict(run_key) do nothing;
 select run_id into v_run_id from research_hub.experiment_runs where run_key='RH-CV-SYNC-V2-20260812';
 perform research_hub.bind_crypto_crossvenue_experiment_inputs_v2(v_run_id);
 v_tasks:=research_hub.plan_feature_screen_tasks(v_run_id);

 insert into research_hub.event_materialization_checkpoints(event_family_key,partition_key,status,metadata)
 select 'crypto.crossvenue.structural.v2',partition_key,'queued',jsonb_build_object('feature_set_key','crypto.crossvenue.sync.v2','definition_version','crypto-crossvenue-sync-v2.0','outcome_accessed',false)
 from research_hub.feature_materialization_checkpoints where feature_set_key='crypto.crossvenue.sync.v2' and status='completed'
 on conflict(event_family_key,partition_key) do nothing;

 update research_hub.program_jobs set current_state='screen_tasks_planned_waiting_compute_slot',progress_current=v_total,progress_total=v_total,completion_pct=100,current_error=null,
   retry_state='v2 materialisation complete; statistical dispatch compute-isolated',
   next_automatic_action='Keep frozen v2 feature-screen tasks queued until B-001 and MDM are terminal/ready. Materialise structural events outcome-blind in parallel. Then execute tasks atomically, global BH-FDR, validation/placebos/dependence gates; require future post-definition replication for any survivor.',
   latest_successful_checkpoint=now(),latest_result=jsonb_build_object('materialized_symbols',v_total,'experiment_run_key','RH-CV-SYNC-V2-20260812','experiment_run_id',v_run_id,'new_tasks_planned',v_tasks,'historical_holdout_assigned',false,'promotion_requires_future_replication',true,'coinbase_volume_proxy_formula','base_volume * OHLC4'),updated_at=now()
 where job_key='FEATURE-CROSSVENUE-LAG-V2';
 return jsonb_build_object('status','screen_tasks_planned_waiting_compute_slot','run_id',v_run_id,'tasks_planned',v_tasks,'completed_symbols',v_total,'holdout_accessed',false);
end $$;

do $$ begin
 if exists(select 1 from cron.job where jobname='research_hub_crossvenue_feature_v2') then perform cron.unschedule((select jobid from cron.job where jobname='research_hub_crossvenue_feature_v2' limit 1)); end if;
 perform cron.schedule('research_hub_crossvenue_feature_v2','*/5 * * * *','select research_hub.materialize_next_crypto_crossvenue_symbol_v2();');
 if exists(select 1 from cron.job where jobname='research_hub_crossvenue_advance_v2') then perform cron.unschedule((select jobid from cron.job where jobname='research_hub_crossvenue_advance_v2' limit 1)); end if;
 perform cron.schedule('research_hub_crossvenue_advance_v2','*/5 * * * *','select research_hub.advance_crypto_crossvenue_research_v2();');
 if exists(select 1 from cron.job where jobname='research_hub_crossvenue_events_v2') then perform cron.unschedule((select jobid from cron.job where jobname='research_hub_crossvenue_events_v2' limit 1)); end if;
 perform cron.schedule('research_hub_crossvenue_events_v2','*/10 * * * *','select research_hub.process_next_crypto_crossvenue_event_partition_v2();');
end $$;