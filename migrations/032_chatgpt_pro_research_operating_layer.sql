-- ChatGPT Pro research operating layer.
-- Production was verified before source-control packaging. This migration is
-- additive/idempotent and does not alter collector behaviour or sealed holdouts.

create table if not exists research_hub.workstream_registry (
    workstream_key text primary key,
    exact_chat_name text not null,
    purpose text not null,
    scope text not null,
    status text not null default 'active',
    owns_topics text[] not null default '{}',
    excludes_topics text[] not null default '{}',
    depends_on text[] not null default '{}',
    output_contract jsonb not null default '{}'::jsonb,
    next_automatic_action text,
    intervention_required boolean not null default false,
    exact_intervention text,
    last_handoff jsonb not null default '{}'::jsonb,
    active boolean not null default true,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists research_hub.research_findings (
    finding_id uuid primary key default gen_random_uuid(),
    finding_key text unique not null,
    finding_type text not null,
    title text not null,
    statement text not null,
    status text not null,
    evidence jsonb not null default '{}'::jsonb,
    source_run_keys text[] not null default '{}',
    source_candidate_ids text[] not null default '{}',
    reusable boolean not null default true,
    propagation_targets text[] not null default '{}',
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists research_hub.operating_policies (
    policy_key text primary key,
    policy_version integer not null,
    purpose text not null,
    policy jsonb not null,
    active boolean not null default true,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists research_hub.compact_extract_specs (
    extract_key text primary key,
    purpose text not null,
    source_dataset_keys text[] not null,
    grain text not null,
    required_columns jsonb not null default '[]'::jsonb,
    max_rows bigint,
    target_format text not null default 'parquet',
    refresh_policy text not null default 'on_demand',
    status text not null default 'planned',
    artifact_id uuid,
    freshness_ts timestamptz,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

insert into research_hub.operating_policies(policy_key,policy_version,purpose,policy)
values('market_edge_core_v1',1,
 'Project-wide empirical discovery and promotion policy for ChatGPT-assisted market-edge research',
 jsonb_build_object(
   'point_in_time_required',true,
   'multiple_testing_control','global BH-FDR within each frozen search family plus dependence-aware robustness before promotion',
   'discovery_validation_holdout','mandatory non-overlapping discovery and validation; holdout remains untouched until rule and execution definition are frozen',
   'execution_standard','observable trigger, information available at decision time, entry/exit/timing/instrument/direction/sizing/costs/spread/slippage/liquidity/borrow/funding must be specified before deployment readiness',
   'dependence_control','day-cluster, HAC or block-bootstrap evidence required before holdout-ready status for overlapping intraday observations',
   'automation','resume safely from checkpoints; transient infrastructure failures retry automatically; research failure remains distinct from infrastructure failure',
   'holdout_protection','never inspect holdout to tune feature, threshold, direction, cost model or execution assumptions',
   'chat_workstream_rule','specialist chats own distinct search domains; every new experiment checks central registries first to avoid duplicate work',
   'pro_usage_rule','use high-reasoning Pro capability for methodology, anomaly interpretation, falsification and synthesis; use database jobs for exhaustive enumeration and arithmetic'
 ))
on conflict (policy_key) do update set policy_version=excluded.policy_version,purpose=excluded.purpose,policy=excluded.policy,active=true,updated_at=now();

insert into research_hub.workstream_registry
(workstream_key,exact_chat_name,purpose,scope,owns_topics,excludes_topics,depends_on,output_contract,next_automatic_action)
values
('ARCH-PRO','Architecture Assessment for ChatGPT','Maximise the probability that ChatGPT Pro plus the research architecture discovers genuine, robust and economically exploitable market patterns.','Architecture, AI context, data accessibility, orchestration, reproducibility and research-system quality.',array['architecture','AI context','data accessibility','orchestration','reproducibility','research controls'],array['new hypothesis-specific signal mining'],array[]::text[],jsonb_build_object('deliverable','implemented architecture improvements plus prioritised roadmap'),'Keep the AI-facing context/catalogue, workstream boundaries and research tooling current; implement system improvements without duplicating specialist discovery.'),
('AUDIT-1700','17:00 Audit Watch','Central operational control point for the market-edge research programme.','Monitor jobs, failures, blockers, checkpoints, progress and Rob interventions; do not duplicate discovery.',array['programme status','job health','blockers','Rob actions'],array['blank-canvas discovery','parameter tuning'],array['ARCH-PRO'],jsonb_build_object('deliverable','operational status and intervention-only exceptions'),'Refresh central programme status and surface only genuine intervention requirements.'),
('METHOD','Data Analysis for Edge','Own the reusable empirical discovery and validation methodology.','Protocol, statistical controls, search-space governance, falsification and promotion/rejection rules.',array['methodology','statistics','validation','falsification','promotion rules'],array['architecture deployment','specific live strategy execution'],array['ARCH-PRO'],jsonb_build_object('deliverable','versioned research protocol'),'Keep the protocol versioned and propagate methodological changes to all experiment definitions.'),
('EXEC-SIGNAL','Tradeable Signal Discovery','Convert empirically validated relationships into fully executable signal candidates and continue falsification.','Executable definition, costs, liquidity, perturbation, regimes, placebo, degradation and holdout readiness.',array['candidate promotion','execution modelling','robustness','holdout readiness'],array['broad raw feature enumeration'],array['METHOD'],jsonb_build_object('deliverable','frozen executable candidate or rejection'),'Consume only centrally registered validated candidates; reject rather than rescue weak signals.'),
('XAL','Cross-Asset Leading Indicators','Discover dynamic cross-asset leader/follower and lag-network relationships.','Cross-asset lags, networks, regime-dependent leadership, transmission and placebo lags.',array['cross-asset','lag networks','leader follower','regime transmission'],array['same-instrument microstructure-only search'],array['METHOD','ARCH-PRO'],jsonb_build_object('deliverable','validated cross-asset candidates with lag stability'),'Use registered XAL feature/outcome sets and central experiment tasks; publish reusable findings.'),
('STRATEGY','Daily Trading Strategy Development','Optimise whole-strategy economics using independently validated executable edges.','Portfolio combination, opportunity frequency, correlation, capital utilisation and strategy-level drawdown.',array['strategy combination','portfolio economics','capital utilisation'],array['discovering weak signals solely to improve a combined backtest'],array['EXEC-SIGNAL'],jsonb_build_object('deliverable','holdout-tested strategy definition'),'Only combine frozen independently validated candidates; open a new untouched holdout after combination methodology is frozen.'),
('QUOTE-TRADE','Quote/Trade Completion','Own quote/trade data completeness and execution-evidence integrity.','Quote/trade completion, point-in-time execution evidence, resilient ingestion and failure recovery.',array['quotes','trades','execution evidence','ingestion resilience'],array['general blank-canvas hypothesis mining'],array['ARCH-PRO'],jsonb_build_object('deliverable','research-ready execution evidence and quality status'),'Continue resilient completion and expose execution-ready datasets to the Research Hub.'),
('LEGACY-XLE','XLE Predicts SPY Strength','Hypothesis-specific validation workstream retained as a bounded legacy experiment.','Validate or reject the named relationship without contaminating blank-canvas discovery.',array['XLE-SPY hypothesis validation'],array['blank-canvas discovery','unrelated cross-asset search'],array['METHOD'],jsonb_build_object('deliverable','frozen validation result or rejection'),'Keep isolated from blank-canvas candidate generation and register any reusable empirical findings centrally.')
on conflict (workstream_key) do update set exact_chat_name=excluded.exact_chat_name,purpose=excluded.purpose,scope=excluded.scope,owns_topics=excluded.owns_topics,excludes_topics=excluded.excludes_topics,depends_on=excluded.depends_on,output_contract=excluded.output_contract,next_automatic_action=excluded.next_automatic_action,active=true,updated_at=now();

create or replace view research_hub.ai_screened_test_review with (security_invoker=true) as
select r.run_key,r.name experiment_name,r.status experiment_status,r.feature_set_key,r.outcome_set_key,
       coalesce(r.config->>'research_conclusion','') research_conclusion,
       coalesce((r.config->>'holdout_accessed')::boolean,false) holdout_accessed,
       t.test_id,t.feature_key,t.outcome_key,t.source_instrument,t.target_instrument,t.slice_key,t.horizon_seconds,
       t.n,t.mean_net,t.median_net,t.hit_rate_net,t.profit_factor_net,t.worst_net,t.avg_winner_net,t.avg_loser_net,t.worst_loss_ratio,
       t.p_value,t.q_value,t.effect_size,t.validation_positive,t.validation_n,t.validation_mean_net,t.validation_median_net,
       t.validation_hit_rate_net,t.validation_profit_factor_net,t.validation_worst_net,t.validation_worst_loss_ratio,
       t.adjacent_horizon_positive,t.metadata,
       case when t.q_value<=0.05 and t.validation_positive is true then 'fdr_and_validation'
            when t.validation_positive is true then 'validation_only'
            when t.q_value<=0.05 then 'discovery_fdr_only'
            when t.mean_net>0 then 'discovery_positive_unconfirmed'
            else 'negative_or_null' end evidence_class,
       t.created_at
from research_hub.experiment_tests t join research_hub.experiment_runs r on r.run_id=t.run_id;

insert into research_hub.research_extracts
(extract_key,description,store_key,schema_name,relation_name,dataset_keys,grain,point_in_time_safe,row_estimate,status,metadata)
values('ai_screened_test_review_v1','Compact cross-experiment post-screen test evidence for ChatGPT Pro interpretation and falsification.','market_data_primary','research_hub','ai_screened_test_review',array[]::text[],'experiment-test evidence row',true,(select count(*) from research_hub.experiment_tests),'available',jsonb_build_object('preferred_for_ai_review',true,'raw_data_avoided',true,'authoritative_tables',array['research_hub.experiment_runs','research_hub.experiment_tests'],'review_only',true))
on conflict (extract_key) do update set description=excluded.description,relation_name=excluded.relation_name,row_estimate=excluded.row_estimate,status=excluded.status,metadata=excluded.metadata,updated_at=now();

create or replace function research_hub.get_ai_context_pack()
returns jsonb language sql stable security invoker set search_path=research_hub,pg_temp as $$
select coalesce((select c.context from research_hub.ai_research_context c limit 1),'{}'::jsonb)
 || jsonb_build_object(
      'workstreams',coalesce((select jsonb_agg(to_jsonb(w) order by w.workstream_key) from research_hub.workstream_registry w where w.active),'[]'::jsonb),
      'active_policy',(select policy from research_hub.operating_policies where active order by policy_version desc limit 1),
      'recent_findings',coalesce((select jsonb_agg(to_jsonb(f) order by f.updated_at desc) from (select * from research_hub.research_findings where reusable order by updated_at desc limit 30) f),'[]'::jsonb),
      'pro_review_extract','research_hub.ai_screened_test_review',
      'context_contract','Use this pack first. Treat source registries/tables as authoritative. Do not reconstruct project state from chat history unless a registry gap is explicitly identified.'
    )
$$;
