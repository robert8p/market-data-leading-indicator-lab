create index if not exists crypto_derivatives_funding_lookup_idx
    on public.crypto_derivatives_metrics(canonical_symbol,ts desc)
    where interval='funding';

create index if not exists crypto_derivatives_15m_ts_symbol_idx
    on public.crypto_derivatives_metrics(ts,canonical_symbol)
    where provider='binance_futures' and interval='15m';

with source_run as (
    select id,effective_start,effective_end,complete_15m_rows,completeness_pct,name
    from public.crypto_b001_replication_runs
    where completeness_pct=100
      and complete_15m_rows>0
      and effective_start<='2025-10-01 00:00:00+00'
      and effective_end>='2026-06-01 00:00:00+00'
    order by complete_15m_rows desc,created_at desc
    limit 1
)
insert into research_hub.datasets(
    dataset_key,store_key,schema_name,relation_name,asset_class,provider,frequency,grain,
    ts_column,instrument_column,observable_at_column,is_raw,point_in_time_safe,
    coverage_start,coverage_end,row_estimate,status,metadata
)
select
    'primary.crypto_b001_spot_15m','market_data_primary','public','crypto_b001_replication_15m',
    'crypto','binance','15m','symbol-15m','bucket_start','symbol','signal_ts',false,true,
    source_run.effective_start,source_run.effective_end,source_run.complete_15m_rows,'available',
    jsonb_build_object(
        'role','complete_15m_spot_source',
        'source_run_name',source_run.name,
        'use','data only; independent Research Hub hypotheses',
        'completeness_pct',source_run.completeness_pct
    )
from source_run
on conflict(dataset_key) do update set
    coverage_start=excluded.coverage_start,
    coverage_end=excluded.coverage_end,
    row_estimate=excluded.row_estimate,
    point_in_time_safe=true,
    status='available',
    metadata=excluded.metadata,
    updated_at=now();

insert into research_hub.feature_definitions(
    feature_key,dataset_key,feature_name,feature_family,value_type,source_expression,
    decision_time_rule,observable_at_rule,lookback_seconds,uses_future_data,enabled,metadata
)
values
('cf.basis_bps','primary.crypto_derivatives_metrics','Perpetual mark minus spot basis','derivatives_basis','double precision','(normalized_mark_price/spot_close-1)*10000','15m spot signal_ts','both 15m bars closed',900,false,true,'{"version":"v1","contract_multiplier_normalized":true}'),
('cf.abs_basis_bps','primary.crypto_derivatives_metrics','Absolute perpetual basis','derivatives_basis','double precision','abs(basis_bps)','15m spot signal_ts','both 15m bars closed',900,false,true,'{"version":"v1"}'),
('cf.basis_change_15m_bps','primary.crypto_derivatives_metrics','15m basis change','derivatives_basis','double precision','basis_bps-lag(basis_bps,1)','15m spot signal_ts','current and prior bars closed',1800,false,true,'{"version":"v1"}'),
('cf.basis_change_1h_bps','primary.crypto_derivatives_metrics','1h basis change','derivatives_basis','double precision','basis_bps-lag(basis_bps,4)','15m spot signal_ts','current and prior bars closed',4500,false,true,'{"version":"v1"}'),
('cf.basis_z_1d','primary.crypto_derivatives_metrics','Basis z-score vs prior day','derivatives_basis','double precision','rolling 96-bar z score','15m spot signal_ts','all rolling inputs closed',86400,false,true,'{"version":"v1"}'),
('cf.basis_z_7d','primary.crypto_derivatives_metrics','Basis z-score vs prior week','derivatives_basis','double precision','rolling 672-bar z score','15m spot signal_ts','all rolling inputs closed',604800,false,true,'{"version":"v1"}'),
('cf.mark_return_15m_bps','primary.crypto_derivatives_metrics','Perpetual mark 15m return','derivatives_price','double precision','normalized_mark/lag(normalized_mark,1)-1','15m spot signal_ts','current and prior mark bars closed',1800,false,true,'{"version":"v1"}'),
('cf.mark_spot_divergence_15m_bps','primary.crypto_derivatives_metrics','Mark minus spot 15m return divergence','cross_market','double precision','mark_return_15m_bps-spot_return_15m_bps','15m spot signal_ts','current and prior bars closed',1800,false,true,'{"version":"v1"}'),
('cf.abs_mark_spot_divergence_15m_bps','primary.crypto_derivatives_metrics','Absolute mark/spot return divergence','cross_market','double precision','abs(mark_spot_divergence_15m_bps)','15m spot signal_ts','current and prior bars closed',1800,false,true,'{"version":"v1"}'),
('cf.mark_intrabar_return_bps','primary.crypto_derivatives_metrics','Perpetual mark open-to-close move','derivatives_price','double precision','(normalized_mark_close/normalized_mark_open-1)*10000','15m spot signal_ts','mark bar closed',900,false,true,'{"version":"v1"}'),
('cf.mark_range_bps','primary.crypto_derivatives_metrics','Perpetual mark intrabar range','derivatives_volatility','double precision','(normalized_mark_high-normalized_mark_low)/normalized_mark_close*10000','15m spot signal_ts','mark bar closed',900,false,true,'{"version":"v1"}'),
('cf.funding_rate_bps','primary.crypto_derivatives_metrics','Last observed funding rate','funding','double precision','last funding_rate <= decision_ts *10000','15m spot signal_ts','funding event timestamp <= decision_ts',28800,false,true,'{"version":"v1"}'),
('cf.abs_funding_rate_bps','primary.crypto_derivatives_metrics','Absolute last funding rate','funding','double precision','abs(last funding_rate)*10000','15m spot signal_ts','funding event timestamp <= decision_ts',28800,false,true,'{"version":"v1"}'),
('cf.funding_change_bps','primary.crypto_derivatives_metrics','Change from preceding funding observation','funding','double precision','(last funding-prev funding)*10000','15m spot signal_ts','both funding timestamps <= decision_ts',57600,false,true,'{"version":"v1"}'),
('cf.funding_age_hours','primary.crypto_derivatives_metrics','Hours since last funding observation','funding','double precision','decision_ts-last funding ts','15m spot signal_ts','funding timestamp <= decision_ts',28800,false,true,'{"version":"v1"}'),
('cf.spot_return_15m_bps','primary.crypto_b001_spot_15m','Spot 15m return','spot_price','double precision','spot_close/lag(spot_close,1)-1','spot signal_ts','current and prior bars closed',1800,false,true,'{"version":"v1"}'),
('cf.spot_return_1h_bps','primary.crypto_b001_spot_15m','Spot trailing 1h return','spot_price','double precision','spot_close/lag(spot_close,4)-1','spot signal_ts','current and prior bars closed',4500,false,true,'{"version":"v1"}'),
('cf.spot_return_4h_bps','primary.crypto_b001_spot_15m','Spot trailing 4h return','spot_price','double precision','spot_close/lag(spot_close,16)-1','spot signal_ts','current and prior bars closed',15300,false,true,'{"version":"v1"}'),
('cf.spot_final_5m_return_bps','primary.crypto_b001_spot_15m','Final 5m return within spot bar','spot_path','double precision','final_5m_return*10000','spot signal_ts','15m source bar closed',900,false,true,'{"version":"v1"}'),
('cf.spot_close_vs_vwap_bps','primary.crypto_b001_spot_15m','Spot close vs intrabar VWAP','spot_pressure','double precision','close_vs_vwap*10000','spot signal_ts','15m source bar closed',900,false,true,'{"version":"v1"}'),
('cf.spot_high_to_close_rejection_bps','primary.crypto_b001_spot_15m','Spot high-to-close rejection','spot_path','double precision','high_to_close_rejection*10000','spot signal_ts','15m source bar closed',900,false,true,'{"version":"v1"}'),
('cf.spot_range_bps','primary.crypto_b001_spot_15m','Spot intrabar range','spot_volatility','double precision','(high-low)/close*10000','spot signal_ts','15m source bar closed',900,false,true,'{"version":"v1"}'),
('cf.spot_taker_buy_share','primary.crypto_b001_spot_15m','Spot taker-buy quote-volume share','spot_order_flow','double precision','taker_buy_quote_volume/quote_volume','spot signal_ts','15m source bar closed',900,false,true,'{"version":"v1"}'),
('cf.spot_taker_imbalance','primary.crypto_b001_spot_15m','Spot taker quote-volume imbalance','spot_order_flow','double precision','2*taker_buy_share-1','spot signal_ts','15m source bar closed',900,false,true,'{"version":"v1"}'),
('cf.spot_log_quote_volume','primary.crypto_b001_spot_15m','Log spot quote volume','spot_activity','double precision','ln(1+quote_volume)','spot signal_ts','15m source bar closed',900,false,true,'{"version":"v1"}'),
('cf.spot_log_trade_count','primary.crypto_b001_spot_15m','Log spot trade count','spot_activity','double precision','ln(1+trade_count)','spot signal_ts','15m source bar closed',900,false,true,'{"version":"v1"}'),
('cf.spot_quote_volume_z_1d','primary.crypto_b001_spot_15m','Spot quote-volume z-score vs prior day','spot_activity','double precision','rolling 96-bar z score of ln(1+quote_volume)','spot signal_ts','all rolling inputs closed',86400,false,true,'{"version":"v1"}')
on conflict(feature_key) do update set
    dataset_key=excluded.dataset_key,
    feature_name=excluded.feature_name,
    feature_family=excluded.feature_family,
    value_type=excluded.value_type,
    source_expression=excluded.source_expression,
    decision_time_rule=excluded.decision_time_rule,
    observable_at_rule=excluded.observable_at_rule,
    lookback_seconds=excluded.lookback_seconds,
    uses_future_data=false,
    enabled=true,
    metadata=excluded.metadata,
    updated_at=now();

insert into research_hub.feature_sets(
    feature_set_key,description,decision_grain,source_dataset_keys,feature_keys,
    materialization_schema,materialization_relation,point_in_time_verified,verification_notes,metadata
)
values(
    'crypto.spot_futures15m.v1',
    'Point-in-time Binance spot/perpetual 15m state combining mark-vs-spot basis, funding state and contemporaneous spot flow/path/activity for the 26-symbol derivatives overlap.',
    'instrument-15m',
    array['primary.crypto_b001_spot_15m','primary.crypto_derivatives_metrics'],
    array['cf.basis_bps','cf.abs_basis_bps','cf.basis_change_15m_bps','cf.basis_change_1h_bps','cf.basis_z_1d','cf.basis_z_7d','cf.mark_return_15m_bps','cf.mark_spot_divergence_15m_bps','cf.abs_mark_spot_divergence_15m_bps','cf.mark_intrabar_return_bps','cf.mark_range_bps','cf.funding_rate_bps','cf.abs_funding_rate_bps','cf.funding_change_bps','cf.funding_age_hours','cf.spot_return_15m_bps','cf.spot_return_1h_bps','cf.spot_return_4h_bps','cf.spot_final_5m_return_bps','cf.spot_close_vs_vwap_bps','cf.spot_high_to_close_rejection_bps','cf.spot_range_bps','cf.spot_taker_buy_share','cf.spot_taker_imbalance','cf.spot_log_quote_volume','cf.spot_log_trade_count','cf.spot_quote_volume_z_1d'],
    'research_hub','crypto_spot_futures15m_features_v1',true,
    'Typed materialization. decision_ts equals the closed spot-bar signal timestamp; matching futures mark bar is closed; funding observation is constrained to funding_observed_at <= decision_ts; rolling windows use current/prior closed bars only. 1000BONK/1000FLOKI/1000SHIB contract prices are normalized to one-token units before basis calculations.',
    '{"adapter":"crypto_spot_futures15m_v1","derivatives_symbols":26,"spot_source_usage":"data only; no B-001 signal reuse","storage":"typed","contract_multiplier_normalized":true,"durability":"unlogged_reconstructible_cache"}'::jsonb
)
on conflict(feature_set_key) do update set
    description=excluded.description,
    decision_grain=excluded.decision_grain,
    source_dataset_keys=excluded.source_dataset_keys,
    feature_keys=excluded.feature_keys,
    materialization_schema=excluded.materialization_schema,
    materialization_relation=excluded.materialization_relation,
    point_in_time_verified=true,
    verification_notes=excluded.verification_notes,
    metadata=excluded.metadata,
    updated_at=now();

insert into research_hub.outcome_definitions(
    outcome_key,outcome_name,target_asset_class,horizon_seconds,entry_rule,exit_rule,cost_model_key,enabled,metadata
)
values
('crypto.binance_spot_fwd_900s','Binance spot forward return 15m','crypto',900,'current closed 15m spot close at decision_ts','spot close at decision_ts +15m','experiment_engine_round_trip_bps',true,'{"price_basis":"15m_close"}'),
('crypto.binance_spot_fwd_3600s','Binance spot forward return 1h','crypto',3600,'current closed 15m spot close at decision_ts','spot close at decision_ts +1h','experiment_engine_round_trip_bps',true,'{"price_basis":"15m_close"}'),
('crypto.binance_spot_fwd_14400s','Binance spot forward return 4h','crypto',14400,'current closed 15m spot close at decision_ts','spot close at decision_ts +4h','experiment_engine_round_trip_bps',true,'{"price_basis":"15m_close"}'),
('crypto.binance_spot_fwd_86400s','Binance spot forward return 24h','crypto',86400,'current closed 15m spot close at decision_ts','spot close at decision_ts +24h','experiment_engine_round_trip_bps',true,'{"price_basis":"15m_close"}')
on conflict(outcome_key) do update set
    outcome_name=excluded.outcome_name,
    target_asset_class=excluded.target_asset_class,
    horizon_seconds=excluded.horizon_seconds,
    entry_rule=excluded.entry_rule,
    exit_rule=excluded.exit_rule,
    cost_model_key=excluded.cost_model_key,
    enabled=true,
    metadata=excluded.metadata,
    updated_at=now();

insert into research_hub.outcome_sets(
    outcome_set_key,description,outcome_keys,materialization_schema,materialization_relation,metadata
)
values(
    'crypto.binance_spot15m_returns.v1',
    'Exact-horizon Binance spot close-to-close returns for the 26-symbol spot/perpetual overlap. Trading costs are applied by the experiment engine; historical bid/ask is not assumed.',
    array['crypto.binance_spot_fwd_900s','crypto.binance_spot_fwd_3600s','crypto.binance_spot_fwd_14400s','crypto.binance_spot_fwd_86400s'],
    'research_hub','crypto_spot_futures15m_outcomes_v1',
    '{"adapter":"crypto_spot_futures15m_v1","execution_note":"close-to-close research outcome; requires execution replication before promotion","storage":"typed","durability":"unlogged_reconstructible_cache"}'::jsonb
)
on conflict(outcome_set_key) do update set
    description=excluded.description,
    outcome_keys=excluded.outcome_keys,
    materialization_schema=excluded.materialization_schema,
    materialization_relation=excluded.materialization_relation,
    metadata=excluded.metadata,
    updated_at=now();

create unlogged table if not exists research_hub.crypto_spot_futures15m_features_v1(
    instrument_key text not null,
    canonical_symbol text not null,
    spot_symbol text not null,
    decision_ts timestamptz not null,
    source_bucket_start timestamptz not null,
    funding_observed_at timestamptz,
    basis_bps double precision,
    abs_basis_bps double precision,
    basis_change_15m_bps double precision,
    basis_change_1h_bps double precision,
    basis_z_1d double precision,
    basis_z_7d double precision,
    mark_return_15m_bps double precision,
    mark_spot_divergence_15m_bps double precision,
    abs_mark_spot_divergence_15m_bps double precision,
    mark_intrabar_return_bps double precision,
    mark_range_bps double precision,
    funding_rate_bps double precision,
    abs_funding_rate_bps double precision,
    funding_change_bps double precision,
    funding_age_hours double precision,
    spot_return_15m_bps double precision,
    spot_return_1h_bps double precision,
    spot_return_4h_bps double precision,
    spot_final_5m_return_bps double precision,
    spot_close_vs_vwap_bps double precision,
    spot_high_to_close_rejection_bps double precision,
    spot_range_bps double precision,
    spot_taker_buy_share double precision,
    spot_taker_imbalance double precision,
    spot_log_quote_volume double precision,
    spot_log_trade_count double precision,
    spot_quote_volume_z_1d double precision,
    source_hash text not null,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    primary key(instrument_key,decision_ts),
    check(funding_observed_at is null or funding_observed_at<=decision_ts)
);

create index if not exists crypto_spot_futures15m_features_v1_ts_idx
    on research_hub.crypto_spot_futures15m_features_v1(decision_ts,instrument_key);
create index if not exists crypto_spot_futures15m_features_v1_symbol_ts_idx
    on research_hub.crypto_spot_futures15m_features_v1(canonical_symbol,decision_ts);

create unlogged table if not exists research_hub.crypto_spot_futures15m_outcomes_v1(
    instrument_key text not null,
    decision_ts timestamptz not null,
    horizon_seconds integer not null check(horizon_seconds>0),
    entry_ts timestamptz not null,
    exit_ts timestamptz not null,
    gross_return double precision not null,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    primary key(instrument_key,decision_ts,horizon_seconds),
    check(exit_ts>decision_ts)
);

create index if not exists crypto_spot_futures15m_outcomes_v1_ts_idx
    on research_hub.crypto_spot_futures15m_outcomes_v1(decision_ts,horizon_seconds,instrument_key);

revoke all on research_hub.crypto_spot_futures15m_features_v1 from public,anon,authenticated;
revoke all on research_hub.crypto_spot_futures15m_outcomes_v1 from public,anon,authenticated;

insert into research_hub.datasets(
    dataset_key,store_key,schema_name,relation_name,asset_class,provider,frequency,grain,
    ts_column,instrument_column,observable_at_column,is_raw,point_in_time_safe,status,metadata
)
values
('derived.crypto_spot_futures15m_features_v1','market_data_primary','research_hub','crypto_spot_futures15m_features_v1','crypto','binance','15m','instrument-15m','decision_ts','instrument_key','decision_ts',false,true,'available','{"role":"typed_feature_store","contract_multiplier_normalized":true,"funding_asof_safe":true,"durability":"unlogged_reconstructible_cache","source_of_truth":false,"rebuildable":true}'::jsonb),
('derived.crypto_spot_futures15m_outcomes_v1','market_data_primary','research_hub','crypto_spot_futures15m_outcomes_v1','crypto','binance','mixed','instrument-decision-horizon','decision_ts','instrument_key','decision_ts',false,true,'available','{"role":"typed_outcome_store","price_basis":"15m_close","costs_applied_in_experiment":true,"durability":"unlogged_reconstructible_cache","source_of_truth":false,"rebuildable":true}'::jsonb)
on conflict(dataset_key) do update set
    schema_name=excluded.schema_name,
    relation_name=excluded.relation_name,
    point_in_time_safe=true,
    status='available',
    metadata=excluded.metadata,
    updated_at=now();