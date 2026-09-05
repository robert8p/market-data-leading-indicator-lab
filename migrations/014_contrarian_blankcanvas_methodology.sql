create schema if not exists research;

create table if not exists research.contrarian_methodology_registry (
    methodology_version text primary key,
    created_at timestamptz not null default now(),
    status text not null default 'active',
    principles jsonb not null,
    search_families jsonb not null,
    anti_bias_gates jsonb not null,
    split_policy jsonb not null,
    conditional_rescue_policy jsonb not null,
    promotion_policy jsonb not null
);

create table if not exists research.contrarian_search_ledger (
    id bigserial primary key,
    search_fingerprint text not null unique,
    methodology_version text not null references research.contrarian_methodology_registry(methodology_version),
    idea_family text not null,
    idea_label text not null,
    predictor_scope text,
    target_scope text,
    lag_scope text,
    conditioning_family text,
    complexity_level integer not null default 0,
    discovery_definition jsonb not null default '{}'::jsonb,
    tests_planned jsonb not null default '{}'::jsonb,
    tests_completed jsonb not null default '{}'::jsonb,
    discovery_result jsonb not null default '{}'::jsonb,
    validation_result jsonb not null default '{}'::jsonb,
    holdout_result jsonb not null default '{}'::jsonb,
    robustness_result jsonb not null default '{}'::jsonb,
    cost_result jsonb not null default '{}'::jsonb,
    status text not null default 'registered',
    rejection_class text,
    rejection_reason text,
    near_miss boolean not null default false,
    can_revisit boolean not null default false,
    revisit_condition text,
    first_tested_at timestamptz,
    last_tested_at timestamptz,
    notes text
);

create index if not exists contrarian_search_ledger_status_idx
    on research.contrarian_search_ledger(status, idea_family);
create index if not exists contrarian_search_ledger_rejection_idx
    on research.contrarian_search_ledger(rejection_class)
    where status='rejected';

create table if not exists research.contrarian_crypto_state_15m_v1 (
    bucket_start timestamptz primary key,
    n_symbols integer not null,
    mean_ret15 double precision,
    mean_abs_ret15 double precision,
    sd_ret15 double precision,
    breadth_up double precision,
    mean_range15 double precision,
    sd_range15 double precision,
    mean_buy_share15 double precision,
    sd_buy_share15 double precision,
    mean_body_efficiency double precision,
    mean_upper_wick_share double precision,
    mean_lower_wick_share double precision,
    mean_qv_accel1 double precision,
    mean_trade_accel1 double precision,
    mean_rv1h double precision,
    mean_rv4h double precision,
    high_liquidity_breadth_up double precision,
    low_liquidity_breadth_up double precision,
    high_liquidity_mean_ret15 double precision,
    low_liquidity_mean_ret15 double precision,
    created_at timestamptz not null default now()
);

create table if not exists research.contrarian_current_panel_15m_v1 (
    bucket_start timestamptz primary key,
    entry_ts timestamptz not null,
    session_date date,
    et_time time,
    n_symbols integer,
    breadth_up double precision,
    mean_ret15 double precision,
    median_ret15 double precision,
    dispersion15 double precision,
    p10_ret15 double precision,
    p90_ret15 double precision,
    mean_range15 double precision,
    btc_ret15 double precision,
    btc_ret60 double precision,
    eth_ret15 double precision,
    eth_ret60 double precision,
    tail_balance double precision,
    tail_span double precision,
    btc_vs_mean double precision,
    eth_vs_mean double precision,
    btc_eth_spread double precision,
    spy_open double precision,
    spy_ret15 double precision,
    spy_ret30 double precision,
    spy_ret60 double precision,
    spy_range30 double precision,
    spy_range60 double precision,
    outcome_split text,
    created_at timestamptz not null default now()
);

create index if not exists contrarian_current_panel_split_idx
    on research.contrarian_current_panel_15m_v1(outcome_split, session_date, et_time);

comment on table research.contrarian_search_ledger is
    'Persistent bias-control ledger for contrarian blank-canvas search paths, including rejected and conditional-rescue ideas.';
comment on table research.contrarian_crypto_state_15m_v1 is
    'Mechanical cross-sectional crypto state panel built from primitive replication features; no prior strategy signal definitions are reused.';
comment on table research.contrarian_current_panel_15m_v1 is
    'Current-period discovery/validation panel for contrarian blank-canvas screens; holdout outcomes are excluded by research policy.';

insert into research.contrarian_methodology_registry(
    methodology_version,status,principles,search_families,anti_bias_gates,
    split_policy,conditional_rescue_policy,promotion_policy
) values (
    'contrarian_blankcanvas_v2_2026-08-11',
    'active',
    '{"contrarian_is_search_diversification_not_trade_direction":true,"surprise_is_not_a_score":true,"data_support_over_narrative":true,"primitive_before_indicator":true,"negative_results_persist":true}'::jsonb,
    '["conditional_instability","low_frequency_high_value","asymmetric_response","unusual_lag","regime_sign_reversal","aggregation_hidden","cross_sectional","nonlinear_threshold","sequence_dependence","session_specific","weak_variable_interaction"]'::jsonb,
    '{"untouched_holdout_required":true,"multiple_testing_logged":true,"fdr_or_familywise_control_where_practical":true,"placebo_required":true,"time_permutation_required":true,"parameter_perturbation_required":true,"neighbor_threshold_required":true,"regime_stability_required":true,"outlier_sensitivity_required":true,"transaction_costs_required_for_economic_promotion":true,"no_holdout_repair":true}'::jsonb,
    '{"historical_source":"crypto replication features + SPY SIP 1m","discovery":["2024-07-01","2025-06-30"],"validation":["2025-07-01","2026-04-30"],"final_holdout":["2026-05-01","2026-06-28"],"holdout_rule":"do_not_query_target_outcomes_until_candidate_definition_and_test_family_are_frozen"}'::jsonb,
    '{"purpose":"rescue superficially unstable candidates without unlimited slicing","allowed_conditioners":["time_of_day_session","volatility_state","liquidity_state","breadth_state","prior_direction","shock_magnitude","cross_sectional_dispersion"],"max_conditioning_dimensions":2,"max_thresholds_per_numeric_conditioner":3,"conditioner_thresholds":"discovery_quantiles_only_or_independently_frozen","minimum_events_per_cell":30,"validation_required_before_holdout":true,"failed_rescue_is_ledgered":true}'::jsonb,
    '{"information_edge_requires_validation":true,"economic_edge_requires_holdout":true,"promotion_requires_predictor_precedes_target":true,"replication_required":true,"execution_realism_required":true,"tails_and_capacity_required":true,"surprising_but_fragile":"reject"}'::jsonb
)
on conflict (methodology_version) do nothing;