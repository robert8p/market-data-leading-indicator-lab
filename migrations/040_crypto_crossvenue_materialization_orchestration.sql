-- Reuse the generic Research Hub materialisation checkpoint table rather than
-- creating another queue. One symbol is materialised per atomic database job;
-- advisory locking prevents overlap and retries are bounded.

insert into research_hub.feature_materialization_checkpoints(feature_set_key,partition_key,status,code_version,metadata)
select 'crypto.crossvenue.sync.v1',symbol,'queued','crypto_crossvenue_sync_v1',jsonb_build_object('attempts',0,'outcome_set_key','crypto.crossvenue.nextopen.v1')
from (select distinct symbol from public.crypto_research_crossvenue_1m) s
on conflict(feature_set_key,partition_key) do nothing;

create or replace function research_hub.materialize_next_crypto_crossvenue_symbol_v1()
returns jsonb language plpgsql security invoker set search_path=research_hub,public,pg_temp as $$
declare v_symbol text; v_result jsonb; v_attempts integer;
begin
 if not pg_try_advisory_xact_lock(hashtext('research_hub_crossvenue_materialize_v1')::bigint) then
   return jsonb_build_object('status','busy','holdout_accessed',false);
 end if;
 select partition_key,coalesce((metadata->>'attempts')::integer,0)
 into v_symbol,v_attempts
 from research_hub.feature_materialization_checkpoints
 where feature_set_key='crypto.crossvenue.sync.v1' and status in ('queued','failed')
   and coalesce((metadata->>'attempts')::integer,0)<4
 order by case when status='failed' then 1 else 0 end,partition_key
 for update skip locked limit 1;
 if v_symbol is null then
   return jsonb_build_object('status','all_available_tasks_resolved','holdout_accessed',false);
 end if;
 update research_hub.feature_materialization_checkpoints
 set status='running',last_error=null,metadata=coalesce(metadata,'{}'::jsonb)||jsonb_build_object('attempts',v_attempts+1),updated_at=now()
 where feature_set_key='crypto.crossvenue.sync.v1' and partition_key=v_symbol;
 begin
   v_result:=research_hub.materialize_crypto_crossvenue_symbol_v1(v_symbol);
   update research_hub.feature_materialization_checkpoints
   set status='completed',row_count=(v_result->>'feature_rows')::bigint,
       last_source_ts=(select max(source_ts) from research_hub.crypto_crossvenue_observations_v1 where symbol=v_symbol),
       code_version='crypto_crossvenue_sync_v1',last_error=null,
       metadata=coalesce(metadata,'{}'::jsonb)||jsonb_build_object('outcome_rows',(v_result->>'outcome_rows')::bigint,'holdout_accessed',false),updated_at=now()
   where feature_set_key='crypto.crossvenue.sync.v1' and partition_key=v_symbol;
   return v_result||jsonb_build_object('status','completed');
 exception when others then
   update research_hub.feature_materialization_checkpoints set status='failed',last_error=left(sqlerrm,4000),updated_at=now()
   where feature_set_key='crypto.crossvenue.sync.v1' and partition_key=v_symbol;
   return jsonb_build_object('symbol',v_symbol,'status','failed','error',sqlerrm,'holdout_accessed',false);
 end;
end $$;

-- Avoid duplicate scheduler registration on redeploy.
do $$ declare v_jobid bigint; begin
 select jobid into v_jobid from cron.job where jobname='research_hub_crossvenue_materialize_v1' limit 1;
 if v_jobid is null then
   perform cron.schedule('research_hub_crossvenue_materialize_v1','*/10 * * * *',$cmd$select research_hub.materialize_next_crypto_crossvenue_symbol_v1();$cmd$);
 end if;
end $$;

-- The actual dependency is synchronized Binance/Coinbase bar readiness, not
-- completion of unrelated MDM enrichment tails.
insert into research_hub.program_jobs(job_key,exact_name,purpose,store_key,source_schema,source_table,source_id,job_kind,current_state,started_at,latest_successful_checkpoint,progress_current,progress_total,completion_pct,latest_result,current_error,retry_state,next_automatic_action,intervention_required,exact_intervention,frozen_rule,holdout_sensitive,metadata)
values('SOURCE-CROSSVENUE-BARS-V1','Synchronized Binance/Coinbase historical source — v1','Evidence-backed source readiness for the common fully resolved Binance/Coinbase one-minute window.','market_data_primary','public','crypto_research_crossvenue_1m','crypto_research_crossvenue_1m:2026-06-28..2026-07-28','data_quality_snapshot','completed_quality_ready',now(),now(),20,20,100,jsonb_build_object('rows',637056,'symbols',20,'coverage_start','2026-06-28T00:00:00Z','coverage_end','2026-07-28T16:52:00Z','legacy_log_ratio_ignored',true),null,'not applicable; frozen source interval','Materialise and screen the canonical explicit-semantic representation; require future post-definition replication for promotion.',false,null,true,true,jsonb_build_object('adaptive_reuse',true,'promotion_requires_future_replication',true))
on conflict(job_key) do update set current_state=excluded.current_state,progress_current=excluded.progress_current,progress_total=excluded.progress_total,completion_pct=excluded.completion_pct,latest_result=excluded.latest_result,current_error=null,retry_state=excluded.retry_state,next_automatic_action=excluded.next_automatic_action,metadata=excluded.metadata,updated_at=now();

delete from research_hub.job_dependencies where job_key='FEATURE-CROSSVENUE-LAG-V1' and depends_on_job_key='MDM-30D-COLLECTION';
insert into research_hub.job_dependencies(job_key,depends_on_job_key,dependency_type,required_state,satisfied,metadata)
values('FEATURE-CROSSVENUE-LAG-V1','SOURCE-CROSSVENUE-BARS-V1','quality_ready','completed_quality_ready',true,jsonb_build_object('minimum_requirement','Frozen synchronized Binance/Coinbase interval quality-ready; full MDM completion not required.'))
on conflict(job_key,depends_on_job_key,dependency_type) do update set required_state=excluded.required_state,satisfied=true,metadata=excluded.metadata,updated_at=now();

update research_hub.program_jobs set current_state='materializing_canonical_feature_outcomes',started_at=coalesce(started_at,now()),progress_current=greatest(progress_current,0),progress_total=20,retry_state='automatic symbol checkpoint queue active',next_automatic_action='Materialise canonical-symbol partitions atomically; then freeze discovery/validation split, generate placebo lags and run globally multiplicity-controlled cross-venue screens. Historical window cannot serve as untouched holdout.',current_error=null,metadata=coalesce(metadata,'{}'::jsonb)||jsonb_build_object('feature_set_key','crypto.crossvenue.sync.v1','outcome_set_key','crypto.crossvenue.nextopen.v1','materialization_cron','research_hub_crossvenue_materialize_v1','adaptive_reuse',true,'promotion_requires_future_replication',true),updated_at=now()
where job_key='FEATURE-CROSSVENUE-LAG-V1';