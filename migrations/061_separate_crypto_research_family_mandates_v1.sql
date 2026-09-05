update research_hub.program_jobs
set purpose='Materialise prospective point-in-time high-resolution crypto microstructure features for hypothesis-free discovery across spread, depth, microprice, signed flow, flow acceleration, liquidity withdrawal and liquidation/order-flow interactions. Funding, basis and mark-vs-spot state are owned by FEATURE-CRYPTO-SPOT-FUTURES-V1 and must not be duplicated here. Open-interest and positioning/taker-ratio history remain excluded until their bucket observability contracts are independently verified.',
    next_automatic_action='Wait for SOURCE-CRYPTO-MICRO-POSTFIX-V1 to pass its seven-day fixed-book quality gate. Then create only the clean prospective order-book/flow/liquidity feature and event family. Do not duplicate funding/basis/mark-vs-spot features from FEATURE-CRYPTO-SPOT-FUTURES-V1. Keep OI/positioning/taker-ratio historical fields gated until explicit observable-at semantics are proven.',
    metadata=(coalesce(metadata,'{}'::jsonb)-'feature_families')||jsonb_build_object(
        'feature_families',jsonb_build_array('spread','depth_imbalance','microprice_dislocation','signed_flow','flow_acceleration','liquidity_withdrawal','liquidation_imbalance','order_flow_interactions'),
        'excluded_owned_elsewhere',jsonb_build_array('funding','basis','mark_spot_divergence'),
        'owned_by_typed_family','FEATURE-CRYPTO-SPOT-FUTURES-V1',
        'temporally_gated_not_yet_searchable',jsonb_build_array('open_interest','global_long_short_ratio','top_account_long_short_ratio','top_position_long_short_ratio','taker_buy_sell_ratio'),
        'cross_family_interactions_policy','Combine independently screened typed-derivatives and clean-microstructure candidates only after each family has passed its own validation/dependence gates.'
    ),
    updated_at=now()
where job_key='FEATURE-CRYPTO-MICRO-V1';

insert into research_hub.program_jobs(
    job_key,exact_name,purpose,store_key,source_schema,source_table,job_kind,current_state,
    progress_current,progress_total,completion_pct,latest_result,current_error,retry_state,next_automatic_action,
    intervention_required,exact_intervention,frozen_rule,holdout_sensitive,metadata
)
values(
    'EXPERIMENT-CRYPTO-SPOT-FUTURES-V1',
    'Freeze and screen typed crypto spot/futures family v1',
    'After the typed spot/futures discovery and validation materialization is complete, freeze one auditable experiment family over crypto.spot_futures15m.v1 without opening the June 2026 holdout.',
    'market_data_primary','research_hub','crypto_spot_futures15m_features_v1','experiment',
    'queued_waiting_feature_materialization',0,1,0,'{}'::jsonb,null,'waiting on typed feature materialization',
    'When FEATURE-CRYPTO-SPOT-FUTURES-V1 reaches ready_for_experiment_freeze, build/verify the typed screening adapter, freeze discovery=2025-10-01..2026-04-01 and validation=2026-04-01..2026-06-01, predeclare multiplicity/placebo/dependence controls, and plan atomic screens. Do not materialize or access June 2026 holdout.',
    false,null,false,true,
    jsonb_build_object(
        'feature_set_key','crypto.spot_futures15m.v1',
        'outcome_set_key','crypto.binance_spot15m_returns.v1',
        'discovery_start','2025-10-01T00:00:00Z',
        'discovery_end','2026-04-01T00:00:00Z',
        'validation_start','2026-04-01T00:00:00Z',
        'validation_end','2026-06-01T00:00:00Z',
        'sealed_holdout_start','2026-06-01T00:00:00Z',
        'predeclared_feature_count',27,
        'direction_inference','two_sided_if_direction_selected_from_discovery',
        'global_multiple_testing_required',true,
        'dependence_tests_required',jsonb_build_array('UTC_date_cluster_bootstrap','moving_block_bootstrap'),
        'placebo_controls_required',jsonb_build_array('time_permutation_within_symbol','symbol_permutation','horizon_direction_placebo'),
        'execution_stress_required',jsonb_build_array('20bps','50bps','100bps'),
        'holdout_access_forbidden_until_candidate_gate',true,
        'user_action_required',false
    )
)
on conflict(job_key) do update set
    exact_name=excluded.exact_name,purpose=excluded.purpose,source_schema=excluded.source_schema,
    source_table=excluded.source_table,current_state=case when research_hub.program_jobs.current_state like 'queued_waiting%' then excluded.current_state else research_hub.program_jobs.current_state end,
    retry_state=excluded.retry_state,next_automatic_action=excluded.next_automatic_action,intervention_required=false,
    exact_intervention=null,holdout_sensitive=true,metadata=research_hub.program_jobs.metadata||excluded.metadata,updated_at=now();

insert into research_hub.job_dependencies(job_key,depends_on_job_key,dependency_type,required_state,satisfied,metadata,updated_at)
select 'EXPERIMENT-CRYPTO-SPOT-FUTURES-V1','FEATURE-CRYPTO-SPOT-FUTURES-V1','completion','ready_for_experiment_freeze',
       (select current_state='ready_for_experiment_freeze' from research_hub.program_jobs where job_key='FEATURE-CRYPTO-SPOT-FUTURES-V1'),
       jsonb_build_object('reason','Do not freeze or screen the typed family until all discovery/validation partitions are complete and June holdout remains untouched.'),now()
where not exists(
    select 1 from research_hub.job_dependencies
    where job_key='EXPERIMENT-CRYPTO-SPOT-FUTURES-V1' and depends_on_job_key='FEATURE-CRYPTO-SPOT-FUTURES-V1'
);

insert into research_hub.research_findings(
    finding_key,finding_type,title,statement,status,evidence,source_run_keys,source_candidate_ids,reusable,propagation_targets
)
values(
    'FIND-CRYPTO-FAMILY-SEPARATION-20260812','architecture','Crypto research families have non-overlapping mandates',
    'Funding, basis and mark-vs-spot state belong to the typed crypto.spot_futures15m.v1 family. Prospective crypto microstructure research is restricted to clean order-book/flow/liquidity information after the Binance book-fix cutover. Historical OI/positioning/taker-ratio buckets remain gated until explicit observable-at semantics are proven. Cross-family interaction searches should occur only after the component families have independently passed their own validation and dependence gates.',
    'active',
    jsonb_build_object('typed_family_job','FEATURE-CRYPTO-SPOT-FUTURES-V1','microstructure_family_job','FEATURE-CRYPTO-MICRO-V1','future_experiment_job','EXPERIMENT-CRYPTO-SPOT-FUTURES-V1','duplicate_feature_generation_prevented',true),
    array[]::text[],array[]::text[],true,array['ARCH-PRO','METHOD','XAL','EXEC-SIGNAL']
)
on conflict(finding_key) do update set statement=excluded.statement,status=excluded.status,evidence=excluded.evidence,propagation_targets=excluded.propagation_targets,updated_at=now();

select research_hub.refresh_program_job_dependencies();