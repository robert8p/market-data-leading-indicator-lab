-- Dependency-driven feature/event expansion backlog for ChatGPT Pro research.
-- These rows are control-plane plans only; they do not start heavy materialisation
-- until authoritative input jobs are complete and quality-ready.

insert into research_hub.program_jobs
(job_key,exact_name,purpose,store_key,job_kind,current_state,progress_current,progress_total,completion_pct,latest_result,current_error,retry_state,next_automatic_action,intervention_required,exact_intervention,frozen_rule,holdout_sensitive,metadata)
values
('FEATURE-CRYPTO-MICRO-V1','Crypto microstructure + derivatives feature expansion v1','Materialise point-in-time crypto microstructure and derivatives state features for hypothesis-free discovery across spread, depth, order-flow, liquidation, funding, OI and basis interactions.','market_data_primary','feature_materialization','queued_waiting_data_completion',0,1,0,'{}'::jsonb,null,'waiting on input completion','When MDM 30-day collection is complete and quality-ready, create a versioned feature set using only observable-at-decision fields, register lineage, then schedule discovery tasks. Do not include future outcomes in the feature table.',false,null,false,true,jsonb_build_object('priority',1,'dataset_keys',array['primary.crypto_microstructure_1s','primary.crypto_derivatives_metrics'],'target_grains',array['5s','15s','1m','5m'],'feature_families',array['spread','depth_imbalance','microprice_dislocation','signed_flow','flow_acceleration','liquidation_imbalance','funding','basis','open_interest','cross_feature_interactions'],'user_action_required',false)),
('FEATURE-CROSSVENUE-LAG-V1','Cross-venue crypto lag/event feature expansion v1','Build point-in-time cross-venue divergence, lead/lag, shock and sequence features from Binance and Coinbase research-ready bars and microstructure.','market_data_primary','feature_materialization','queued_waiting_data_completion',0,1,0,'{}'::jsonb,null,'waiting on input completion','After source coverage is quality-ready, materialise synchronized cross-venue state and event tables, generate placebo lags, and schedule dynamic leader/follower screens with global multiplicity control.',false,null,false,true,jsonb_build_object('priority',2,'dataset_keys',array['primary.binance_bars_1m','primary.coinbase_bars_1m','primary.crypto_microstructure_1s'],'feature_families',array['venue_return_gap','venue_volume_gap','venue_spread_gap','leader_switch','shock_propagation','sequence_state','lag_network'],'user_action_required',false)),
('EVENT-EQUITY-MICRO-SEQUENCE-V1','Equity SIP event/sequence expansion v1','Convert existing point-in-time equity SIP microstructure features into event, sequence and interaction families rather than repeating wider univariate tails.','market_data_primary','event_materialization','queued_waiting_quote_trade_completion',0,1,0,'{}'::jsonb,null,'waiting on quote/trade tail completion','When quote/trade collection is complete, generate frozen event definitions for spread compression/expansion, quote pressure, trade imbalance, liquidity withdrawal, rejection and multi-step sequences; screen with quote-execution outcomes and dependence-aware validation.',false,null,false,true,jsonb_build_object('priority',3,'dataset_keys',array['primary.equity_microstructure_1m','primary.market_quotes_l1','primary.market_trades'],'feature_set_key','equity.sip.microstructure.v1','outcome_set_key','equity.sip.quote_exec.v1','research_shift','deeper interactions/sequences rather than broader univariate tails','user_action_required',false))
on conflict (job_key) do update set purpose=excluded.purpose,current_state=excluded.current_state,retry_state=excluded.retry_state,next_automatic_action=excluded.next_automatic_action,metadata=excluded.metadata,updated_at=now();

insert into research_hub.job_dependencies(job_key,depends_on_job_key,dependency_type,required_state,satisfied,metadata)
values
('FEATURE-CRYPTO-MICRO-V1','MDM-30D-COLLECTION','quality_ready','completed',false,jsonb_build_object('minimum_requirement','collection/enrichment complete and research-ready quality checks passed')),
('FEATURE-CROSSVENUE-LAG-V1','MDM-30D-COLLECTION','quality_ready','completed',false,jsonb_build_object('minimum_requirement','Binance/Coinbase synchronized coverage quality-ready')),
('EVENT-EQUITY-MICRO-SEQUENCE-V1','MDM-ALPACA-QUOTES-30D','completion','completed',false,'{}'::jsonb),
('EVENT-EQUITY-MICRO-SEQUENCE-V1','MDM-ALPACA-TRADES-30D','completion','completed',false,'{}'::jsonb)
on conflict (job_key,depends_on_job_key,dependency_type) do update set required_state=excluded.required_state,metadata=excluded.metadata,updated_at=now();

create or replace function research_hub.refresh_program_job_dependencies()
returns jsonb language plpgsql security invoker set search_path=research_hub,pg_temp as $$
declare v_dep_updates bigint; v_ready_updates bigint;
begin
  update research_hub.job_dependencies d
  set satisfied = case
      when d.dependency_type='completion' then exists(
        select 1 from research_hub.program_jobs p where p.job_key=d.depends_on_job_key
          and (p.current_state=d.required_state or p.current_state like 'completed%' or p.current_state in ('prospective_day_complete','quality_ready')))
      when d.dependency_type='quality_ready' then exists(
        select 1 from research_hub.program_jobs p where p.job_key=d.depends_on_job_key and p.current_error is null
          and (p.current_state in ('completed','quality_ready','completed_quality_ready') or p.current_state like 'completed%'))
      else exists(select 1 from research_hub.program_jobs p where p.job_key=d.depends_on_job_key and (d.required_state is null or p.current_state=d.required_state))
    end,
    updated_at=now();
  get diagnostics v_dep_updates=row_count;

  update research_hub.program_jobs p
  set current_state='ready_for_automatic_execution',retry_state='dependencies satisfied',updated_at=now()
  where p.current_state like 'queued_waiting%'
    and exists(select 1 from research_hub.job_dependencies d where d.job_key=p.job_key)
    and not exists(select 1 from research_hub.job_dependencies d where d.job_key=p.job_key and d.satisfied is false);
  get diagnostics v_ready_updates=row_count;
  return jsonb_build_object('dependencies_refreshed',v_dep_updates,'jobs_became_ready',v_ready_updates);
end $$;

create or replace view research_hub.ai_ready_program_jobs with (security_invoker=true) as
select p.* from research_hub.program_jobs p
where p.current_state='ready_for_automatic_execution'
order by coalesce((p.metadata->>'priority')::integer,100),p.updated_at;
