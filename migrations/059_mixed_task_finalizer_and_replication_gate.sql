-- Finalize scalar feature and structural-event screens under one global BH-FDR.
-- Adaptive-reuse families cannot be labelled validated/promoted solely from the
-- recycled historical window; survivors require genuinely new replication.

create or replace function research_hub.finalize_chunked_screen(p_run_id uuid)
returns jsonb
language plpgsql
set search_path=research_hub,pg_temp
as $$
declare
  r research_hub.experiment_runs%rowtype;
  v_fdr double precision;
  v_tests bigint;
  v_candidates bigint;
  v_replication_required boolean;
  v_next_gate text;
begin
  select * into r from research_hub.experiment_runs where run_id=p_run_id for update;
  if not found then raise exception 'Unknown experiment run %',p_run_id; end if;
  if exists(select 1 from research_hub.experiment_tasks where run_id=p_run_id and status<>'completed') then
    raise exception 'Run % still has incomplete research tasks',r.run_key;
  end if;
  v_fdr:=coalesce((r.config->>'fdr_q')::double precision,0.05);
  v_replication_required:=coalesce((r.config->>'promotion_requires_future_replication')::boolean,false);
  v_next_gate:=case when v_replication_required then 'dependence_robustness_and_future_replication' else 'dependence_robustness' end;

  with ranked as (
    select test_id,p_value,row_number() over(order by p_value,test_id) rn,count(*) over() m
    from research_hub.experiment_tests where run_id=p_run_id and p_value is not null
  ), raw_q as (
    select test_id,rn,least(1.0,p_value*m::double precision/rn::double precision) raw_q from ranked
  ), adjusted as (
    select test_id,least(1.0,min(raw_q) over(order by rn desc rows between unbounded preceding and current row)) q_value from raw_q
  )
  update research_hub.experiment_tests t set q_value=a.q_value from adjusted a where t.test_id=a.test_id;

  with ordered as (
    select test_id,
           lag(mean_net) over(partition by run_id,source_instrument,target_instrument,feature_key,slice_key order by horizon_seconds) prev_mean,
           lead(mean_net) over(partition by run_id,source_instrument,target_instrument,feature_key,slice_key order by horizon_seconds) next_mean
    from research_hub.experiment_tests where run_id=p_run_id
  )
  update research_hub.experiment_tests t
     set adjacent_horizon_positive=(coalesce(o.prev_mean,0)>0 or coalesce(o.next_mean,0)>0)
    from ordered o where t.test_id=o.test_id;

  delete from research_hub.candidate_ledger where run_id=p_run_id;
  insert into research_hub.candidate_ledger(candidate_id,run_id,status,descriptive_name,frozen_definition,metrics,confidence,next_test,frozen_at)
  select
    'RH-'||upper(substr(md5(r.run_key||'|'||t.source_instrument||'|'||t.feature_key||'|'||t.slice_key||'|'||t.target_instrument||'|'||t.horizon_seconds),1,12)),
    p_run_id,
    case when v_replication_required then 'FROZEN_SCREENING_SURVIVOR_REPLICATION_REQUIRED' else 'FROZEN_VALIDATION_PASSED' end,
    case when t.metadata->>'screen_type'='event'
         then t.source_instrument||' '||coalesce(t.metadata->>'event_key',t.feature_key)||' -> '||t.target_instrument||' @ '||t.horizon_seconds||'s'
         else t.source_instrument||' '||t.slice_key||' '||t.feature_key||' -> '||t.target_instrument||' @ '||t.horizon_seconds||'s' end,
    case when t.metadata->>'screen_type'='event' then
      jsonb_build_object(
        'engine','research_hub_chunked_event_v1','feature_set_key',r.feature_set_key,'outcome_set_key',r.outcome_set_key,
        'source_instrument',t.source_instrument,'event_key',t.metadata->'event_key','event_version',t.metadata->'event_version',
        'target_instrument',t.target_instrument,'horizon_seconds',t.horizon_seconds,'trade_direction',t.metadata->'trade_direction',
        'round_trip_cost_bps',t.metadata->'round_trip_cost_bps','event_definition_frozen_before_screen',true,
        'threshold_learning_period',jsonb_build_array(r.discovery_start,r.discovery_end),'validation_period',jsonb_build_array(r.validation_start,r.validation_end),
        'adaptive_reuse',coalesce(r.config->'adaptive_reuse','false'::jsonb),'promotion_requires_future_replication',coalesce(r.config->'promotion_requires_future_replication','false'::jsonb),'holdout_accessed',false)
    else
      jsonb_build_object(
        'engine','research_hub_chunked_feature_v1','feature_set_key',r.feature_set_key,'outcome_set_key',r.outcome_set_key,
        'source_instrument',t.source_instrument,'feature_key',t.feature_key,'tail',split_part(t.slice_key,'_',1),'tail_quantile',t.metadata->'tail_quantile','threshold',t.metadata->'threshold',
        'target_instrument',t.target_instrument,'horizon_seconds',t.horizon_seconds,'trade_direction',t.metadata->'trade_direction','round_trip_cost_bps',t.metadata->'round_trip_cost_bps',
        'threshold_learning_period',jsonb_build_array(r.discovery_start,r.discovery_end),'validation_period',jsonb_build_array(r.validation_start,r.validation_end),
        'adaptive_reuse',coalesce(r.config->'adaptive_reuse','false'::jsonb),'promotion_requires_future_replication',coalesce(r.config->'promotion_requires_future_replication','false'::jsonb),'holdout_accessed',false)
    end,
    jsonb_build_object('discovery',t.metadata->'discovery','validation',t.metadata->'validation','q_value',t.q_value,'effect_size',t.effect_size,'screen_type',coalesce(t.metadata->>'screen_type','feature')),
    case when v_replication_required then 'Screening survivor only; adaptive-reuse history requires genuinely new future/external replication.'
         when t.q_value<=v_fdr/10.0 then 'Strong screening result' else 'Screening result' end,
    case when v_replication_required then 'Run dependence/placebo robustness, then genuinely new post-definition replication before any holdout or deployment promotion.'
         else 'Run dependence-aware robustness before any sealed-holdout evaluation.' end,
    now()
  from research_hub.experiment_tests t
  where t.run_id=p_run_id and t.q_value is not null and t.q_value<=v_fdr and t.mean_net>0
    and t.validation_positive is true and t.adjacent_horizon_positive is true
  on conflict(candidate_id) do update set status=excluded.status,frozen_definition=excluded.frozen_definition,metrics=excluded.metrics,confidence=excluded.confidence,next_test=excluded.next_test,frozen_at=excluded.frozen_at,updated_at=now();

  select count(*) into v_tests from research_hub.experiment_tests where run_id=p_run_id;
  select count(*) into v_candidates from research_hub.candidate_ledger where run_id=p_run_id;
  update research_hub.experiment_runs
     set status=case when v_replication_required then 'screening_complete_replication_and_dependence_required' else 'screening_complete_dependence_review_required' end,
         search_space_tests=v_tests,completed_at=now(),updated_at=now(),
         config=config||jsonb_build_object('holdout_accessed',false,'engine',case when coalesce((config->>'complete_family_includes_events')::boolean,false) then 'research_hub_chunked_mixed_v1' else 'research_hub_chunked_feature_v1' end),
         latest_result=coalesce(latest_result,'{}'::jsonb)||jsonb_build_object('tests',v_tests,'screening_survivors',v_candidates,'promotion_requires_future_replication',v_replication_required,'holdout_accessed',false)
   where run_id=p_run_id;

  return jsonb_build_object('run_id',p_run_id,'tests',v_tests,'candidates',v_candidates,'promotion_requires_future_replication',v_replication_required,'holdout_accessed',false,'next_gate',v_next_gate);
end $$;