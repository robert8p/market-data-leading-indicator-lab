-- Separate typed point-in-time family for recovered Binance OI, positioning and taker flow.
-- The historical 2026-07-14..2026-08-12 window is discovery/validation only.
-- No untouched historical holdout is claimed; post-definition future replication is mandatory.

create unlogged table if not exists research_hub.crypto_positioning15m_features_v1(
    canonical_symbol text not null,
    decision_ts timestamptz not null,
    spot_bucket_start timestamptz not null,
    observable_at timestamptz not null,
    spot_ret15 double precision, spot_ret60 double precision, spot_ret240 double precision,
    spot_log_quote_volume double precision, spot_log_trade_count double precision,
    log_oi_value double precision, oi_chg_5m double precision, oi_chg_15m double precision, oi_chg_60m double precision,
    global_ls_log double precision, global_ls_chg_15m double precision, global_ls_chg_60m double precision,
    top_account_ls_log double precision, top_account_ls_chg_15m double precision,
    top_position_ls_log double precision, top_position_ls_chg_15m double precision,
    account_global_div double precision, position_global_div double precision,
    taker_log_ratio double precision, taker_imbalance double precision, taker_chg_15m double precision, taker_chg_60m double precision,
    ret15_x_oi15 double precision, ret60_x_oi60 double precision, taker_x_oi15 double precision, position_div_x_ret15 double precision,
    cs_oi_chg15_rank double precision, cs_global_ls_rank double precision, cs_top_position_rank double precision,
    cs_taker_imbalance_rank double precision, cs_spot_ret15_rank double precision,
    oi_chg15_z_1d double precision, oi_chg60_z_1d double precision, global_ls_z_1d double precision,
    top_position_ls_z_1d double precision, position_div_z_1d double precision,
    taker_imbalance_z_1d double precision, spot_ret15_z_1d double precision, spot_log_quote_volume_z_1d double precision,
    price_up_oi_up boolean, price_down_oi_up boolean, price_up_oi_down boolean, price_down_oi_down boolean,
    crowded_long_buying boolean, crowded_long_selling boolean,
    oi_metric_ts timestamptz not null, taker_metric_ts timestamptz not null,
    source_hash text not null, quality jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(), updated_at timestamptz not null default now(),
    primary key(canonical_symbol,decision_ts)
);
create index if not exists crypto_positioning15m_features_ts_idx on research_hub.crypto_positioning15m_features_v1(decision_ts,canonical_symbol);
create index if not exists crypto_positioning15m_features_symbol_ts_idx on research_hub.crypto_positioning15m_features_v1(canonical_symbol,decision_ts);
revoke all on table research_hub.crypto_positioning15m_features_v1 from public,anon,authenticated;

create unlogged table if not exists research_hub.crypto_positioning15m_outcomes_v1(
    canonical_symbol text not null, decision_ts timestamptz not null, entry_open double precision not null,
    exit_open_900s double precision, exit_open_3600s double precision, exit_open_14400s double precision,
    gross_return_900s double precision, gross_return_3600s double precision, gross_return_14400s double precision,
    max_favourable_excursion_14400s double precision, max_adverse_excursion_14400s double precision,
    source_hash text not null, metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(), updated_at timestamptz not null default now(),
    primary key(canonical_symbol,decision_ts)
);
create index if not exists crypto_positioning15m_outcomes_ts_idx on research_hub.crypto_positioning15m_outcomes_v1(decision_ts,canonical_symbol);
revoke all on table research_hub.crypto_positioning15m_outcomes_v1 from public,anon,authenticated;

create table if not exists research_hub.crypto_positioning15m_work_v1(
    canonical_symbol text primary key, status text not null default 'waiting_source_quality',
    priority integer not null default 100, attempts integer not null default 0, max_attempts integer not null default 5,
    feature_rows bigint not null default 0, outcome_rows bigint not null default 0, last_error text,
    started_at timestamptz, completed_at timestamptz, updated_at timestamptz not null default now(), metadata jsonb not null default '{}'::jsonb
);
create index if not exists crypto_positioning15m_work_status_idx on research_hub.crypto_positioning15m_work_v1(status,priority desc,canonical_symbol);
revoke all on table research_hub.crypto_positioning15m_work_v1 from public,anon,authenticated;

create table if not exists research_hub.crypto_positioning15m_rank_work_v1(
    decision_date date primary key, status text not null default 'queued', attempts integer not null default 0,
    max_attempts integer not null default 5, rows_updated bigint not null default 0, last_error text,
    started_at timestamptz, completed_at timestamptz, updated_at timestamptz not null default now()
);
revoke all on table research_hub.crypto_positioning15m_rank_work_v1 from public,anon,authenticated;

create table if not exists research_hub.crypto_positioning15m_control_v1(
    singleton boolean primary key default true check(singleton), contract_version text not null, definition_hash text not null,
    discovery_start timestamptz not null, discovery_end timestamptz not null,
    validation_start timestamptz not null, validation_end timestamptz not null,
    future_replication_start timestamptz not null,
    cross_sectional_ranks_finalized boolean not null default false,
    experiment_frozen boolean not null default false, holdout_available boolean not null default false,
    metadata jsonb not null default '{}'::jsonb, created_at timestamptz not null default now(), updated_at timestamptz not null default now()
);
insert into research_hub.crypto_positioning15m_control_v1(
    singleton,contract_version,definition_hash,discovery_start,discovery_end,validation_start,validation_end,future_replication_start,metadata
) values(
    true,'crypto-positioning15m-v1',md5('crypto-positioning15m-v1|metric-specific-observable-at|spot-next-open|future-replication-required'),
    '2026-07-14 00:00+00','2026-07-30 00:00+00','2026-07-30 00:00+00','2026-08-12 10:15+00','2026-08-13 00:00+00',
    jsonb_build_object(
      'historical_window_role','discovery_validation_only','untouched_holdout_in_historical_recovery',false,
      'promotion_requires_post_definition_future_replication',true,
      'multiplicity_family','all scalar tails, sign/sequence events, regimes, liquidity tiers and horizons in one global family',
      'cost_stress_bps',jsonb_build_array(20,50,100),
      'dependence_controls',jsonb_build_array('UTC_date_cluster','moving_block_bootstrap','symbol_concentration'),
      'placebos',jsonb_build_array('within_symbol_time_permutation','symbol_permutation','future_lead_impossible_information'),
      'no_overlap_with_spot_futures_family',true,'no_overlap_with_clean_orderbook_microstructure_family',true
    )
) on conflict(singleton) do update set
    contract_version=excluded.contract_version,definition_hash=excluded.definition_hash,
    discovery_start=excluded.discovery_start,discovery_end=excluded.discovery_end,
    validation_start=excluded.validation_start,validation_end=excluded.validation_end,
    future_replication_start=excluded.future_replication_start,
    metadata=research_hub.crypto_positioning15m_control_v1.metadata||excluded.metadata,updated_at=now();
revoke all on table research_hub.crypto_positioning15m_control_v1 from public,anon,authenticated;

insert into research_hub.datasets(
    dataset_key,store_key,schema_name,relation_name,asset_class,provider,frequency,grain,
    ts_column,instrument_column,observable_at_column,is_raw,point_in_time_safe,coverage_start,coverage_end,row_estimate,status,metadata
) values(
    'research_hub.binance_spot15m_positioning_v1','market_data_primary','research_hub','binance_spot15m_positioning_v1',
    'crypto','binance','15m','symbol-15m','signal_ts','canonical_symbol','signal_ts',false,true,
    '2026-07-13 00:00+00','2026-08-12 14:30+00',null,'building',
    jsonb_build_object('role','spot features and outcomes for retention-recovered positioning data','credentials_required',false,'quality_table','research_hub.binance_spot15m_positioning_quality_v1')
) on conflict(dataset_key) do update set
    relation_name=excluded.relation_name,frequency=excluded.frequency,grain=excluded.grain,
    ts_column=excluded.ts_column,instrument_column=excluded.instrument_column,observable_at_column=excluded.observable_at_column,
    point_in_time_safe=true,coverage_start=excluded.coverage_start,coverage_end=excluded.coverage_end,
    status=excluded.status,metadata=research_hub.datasets.metadata||excluded.metadata,updated_at=now();

insert into research_hub.feature_definitions(
    feature_key,dataset_key,feature_name,feature_family,value_type,source_expression,
    decision_time_rule,observable_at_rule,lookback_seconds,uses_future_data,enabled,metadata
)
select * from (values
('pos.spot_ret15','research_hub.binance_spot15m_positioning_v1','Spot 15m return','spot_path','double precision','close/open-1','completed spot 15m signal_ts','spot bar closed at decision_ts',900,false,true,'{}'::jsonb),
('pos.spot_ret60','research_hub.binance_spot15m_positioning_v1','Spot 1h return','spot_path','double precision','close/close_lag_1h-1','completed spot 15m signal_ts','current and lagged spot bars closed',4500,false,true,'{}'::jsonb),
('pos.spot_ret240','research_hub.binance_spot15m_positioning_v1','Spot 4h return','spot_path','double precision','close/close_lag_4h-1','completed spot 15m signal_ts','current and lagged spot bars closed',15300,false,true,'{}'::jsonb),
('pos.spot_log_quote_volume','research_hub.binance_spot15m_positioning_v1','Spot log quote volume','spot_activity','double precision','ln(1+quote_volume)','completed spot 15m signal_ts','spot bar closed at decision_ts',900,false,true,'{}'::jsonb),
('pos.spot_log_trade_count','research_hub.binance_spot15m_positioning_v1','Spot log trade count','spot_activity','double precision','ln(1+trade_count)','completed spot 15m signal_ts','spot bar closed at decision_ts',900,false,true,'{}'::jsonb),
('pos.log_oi_value','primary.crypto_derivatives_metrics','Log open-interest notional','open_interest','double precision','ln(open_interest_value)','completed spot 15m signal_ts','latest OI timestamp <= decision_ts-60s',300,false,true,'{}'::jsonb),
('pos.oi_chg_5m','primary.crypto_derivatives_metrics','5m OI log change','open_interest','double precision','ln(oi_t/oi_t-5m)','completed spot 15m signal_ts','both OI rows observable',600,false,true,'{}'::jsonb),
('pos.oi_chg_15m','primary.crypto_derivatives_metrics','15m OI log change','open_interest','double precision','ln(oi_t/oi_t-15m)','completed spot 15m signal_ts','both OI rows observable',1200,false,true,'{}'::jsonb),
('pos.oi_chg_60m','primary.crypto_derivatives_metrics','1h OI log change','open_interest','double precision','ln(oi_t/oi_t-60m)','completed spot 15m signal_ts','both OI rows observable',3900,false,true,'{}'::jsonb),
('pos.global_ls_log','primary.crypto_derivatives_metrics','Log global long-short ratio','positioning','double precision','ln(global_long_short_ratio)','completed spot 15m signal_ts','latest ratio timestamp <= decision_ts-60s',300,false,true,'{}'::jsonb),
('pos.global_ls_chg_15m','primary.crypto_derivatives_metrics','15m global ratio change','positioning','double precision','ln(global_t/global_t-15m)','completed spot 15m signal_ts','both rows observable',1200,false,true,'{}'::jsonb),
('pos.global_ls_chg_60m','primary.crypto_derivatives_metrics','1h global ratio change','positioning','double precision','ln(global_t/global_t-60m)','completed spot 15m signal_ts','both rows observable',3900,false,true,'{}'::jsonb),
('pos.top_account_ls_log','primary.crypto_derivatives_metrics','Log top-account ratio','positioning','double precision','ln(top_account_long_short_ratio)','completed spot 15m signal_ts','latest ratio timestamp <= decision_ts-60s',300,false,true,'{}'::jsonb),
('pos.top_account_ls_chg_15m','primary.crypto_derivatives_metrics','15m top-account ratio change','positioning','double precision','ln(top_account_t/top_account_t-15m)','completed spot 15m signal_ts','both rows observable',1200,false,true,'{}'::jsonb),
('pos.top_position_ls_log','primary.crypto_derivatives_metrics','Log top-position ratio','positioning','double precision','ln(top_position_long_short_ratio)','completed spot 15m signal_ts','latest ratio timestamp <= decision_ts-60s',300,false,true,'{}'::jsonb),
('pos.top_position_ls_chg_15m','primary.crypto_derivatives_metrics','15m top-position ratio change','positioning','double precision','ln(top_position_t/top_position_t-15m)','completed spot 15m signal_ts','both rows observable',1200,false,true,'{}'::jsonb),
('pos.account_global_div','primary.crypto_derivatives_metrics','Top-account/global divergence','positioning_divergence','double precision','ln(top_account/global)','completed spot 15m signal_ts','same observable timestamp',300,false,true,'{}'::jsonb),
('pos.position_global_div','primary.crypto_derivatives_metrics','Top-position/global divergence','positioning_divergence','double precision','ln(top_position/global)','completed spot 15m signal_ts','same observable timestamp',300,false,true,'{}'::jsonb),
('pos.taker_log_ratio','primary.crypto_derivatives_metrics','Log taker buy-sell ratio','futures_flow','double precision','ln(taker_buy_sell_ratio)','completed spot 15m signal_ts','latest taker period start <= decision_ts-6m',600,false,true,'{}'::jsonb),
('pos.taker_imbalance','primary.crypto_derivatives_metrics','Taker flow imbalance','futures_flow','double precision','(ratio-1)/(ratio+1)','completed spot 15m signal_ts','latest taker period start <= decision_ts-6m',600,false,true,'{}'::jsonb),
('pos.taker_chg_15m','primary.crypto_derivatives_metrics','15m taker-ratio change','futures_flow','double precision','ln(taker_t/taker_t-15m)','completed spot 15m signal_ts','both periods observable',1500,false,true,'{}'::jsonb),
('pos.taker_chg_60m','primary.crypto_derivatives_metrics','1h taker-ratio change','futures_flow','double precision','ln(taker_t/taker_t-60m)','completed spot 15m signal_ts','both periods observable',4200,false,true,'{}'::jsonb),
('pos.ret15_x_oi15','primary.crypto_derivatives_metrics','15m return × OI interaction','interaction','double precision','spot_ret15*oi_chg_15m','completed spot 15m signal_ts','both inputs observable',1200,false,true,'{}'::jsonb),
('pos.ret60_x_oi60','primary.crypto_derivatives_metrics','1h return × OI interaction','interaction','double precision','spot_ret60*oi_chg_60m','completed spot 15m signal_ts','both inputs observable',4500,false,true,'{}'::jsonb),
('pos.taker_x_oi15','primary.crypto_derivatives_metrics','Taker × OI interaction','interaction','double precision','taker_imbalance*oi_chg_15m','completed spot 15m signal_ts','both inputs observable',1200,false,true,'{}'::jsonb),
('pos.position_div_x_ret15','primary.crypto_derivatives_metrics','Position divergence × return','interaction','double precision','position_global_div*spot_ret15','completed spot 15m signal_ts','both inputs observable',900,false,true,'{}'::jsonb),
('pos.cs_oi_chg15_rank','primary.crypto_derivatives_metrics','Cross-sectional OI-change rank','cross_sectional','double precision','percent_rank by decision_ts','completed spot 15m signal_ts','all inputs observable',1200,false,true,'{}'::jsonb),
('pos.cs_global_ls_rank','primary.crypto_derivatives_metrics','Cross-sectional global crowding rank','cross_sectional','double precision','percent_rank by decision_ts','completed spot 15m signal_ts','all inputs observable',300,false,true,'{}'::jsonb),
('pos.cs_top_position_rank','primary.crypto_derivatives_metrics','Cross-sectional top-position rank','cross_sectional','double precision','percent_rank by decision_ts','completed spot 15m signal_ts','all inputs observable',300,false,true,'{}'::jsonb),
('pos.cs_taker_imbalance_rank','primary.crypto_derivatives_metrics','Cross-sectional taker rank','cross_sectional','double precision','percent_rank by decision_ts','completed spot 15m signal_ts','all inputs observable',600,false,true,'{}'::jsonb),
('pos.cs_spot_ret15_rank','research_hub.binance_spot15m_positioning_v1','Cross-sectional spot-return rank','cross_sectional','double precision','percent_rank by decision_ts','completed spot 15m signal_ts','all inputs observable',900,false,true,'{}'::jsonb),
('pos.oi_chg15_z_1d','primary.crypto_derivatives_metrics','Rolling 1d z-score of 15m OI change','rolling_normalized','double precision','causal 96-bar rolling z','completed spot 15m signal_ts','window ends at decision_ts',86400,false,true,jsonb_build_object('window_bars',96,'minimum_bars',24)),
('pos.oi_chg60_z_1d','primary.crypto_derivatives_metrics','Rolling 1d z-score of 1h OI change','rolling_normalized','double precision','causal 96-bar rolling z','completed spot 15m signal_ts','window ends at decision_ts',86400,false,true,jsonb_build_object('window_bars',96,'minimum_bars',24)),
('pos.global_ls_z_1d','primary.crypto_derivatives_metrics','Rolling 1d global crowding z-score','rolling_normalized','double precision','causal 96-bar rolling z','completed spot 15m signal_ts','window ends at decision_ts',86400,false,true,jsonb_build_object('window_bars',96,'minimum_bars',24)),
('pos.top_position_ls_z_1d','primary.crypto_derivatives_metrics','Rolling 1d top-position z-score','rolling_normalized','double precision','causal 96-bar rolling z','completed spot 15m signal_ts','window ends at decision_ts',86400,false,true,jsonb_build_object('window_bars',96,'minimum_bars',24)),
('pos.position_div_z_1d','primary.crypto_derivatives_metrics','Rolling 1d position-divergence z-score','rolling_normalized','double precision','causal 96-bar rolling z','completed spot 15m signal_ts','window ends at decision_ts',86400,false,true,jsonb_build_object('window_bars',96,'minimum_bars',24)),
('pos.taker_imbalance_z_1d','primary.crypto_derivatives_metrics','Rolling 1d taker-imbalance z-score','rolling_normalized','double precision','causal 96-bar rolling z','completed spot 15m signal_ts','window ends at decision_ts',86400,false,true,jsonb_build_object('window_bars',96,'minimum_bars',24)),
('pos.spot_ret15_z_1d','research_hub.binance_spot15m_positioning_v1','Rolling 1d spot-return z-score','rolling_normalized','double precision','causal 96-bar rolling z','completed spot 15m signal_ts','window ends at decision_ts',86400,false,true,jsonb_build_object('window_bars',96,'minimum_bars',24)),
('pos.spot_log_quote_volume_z_1d','research_hub.binance_spot15m_positioning_v1','Rolling 1d spot-volume z-score','rolling_normalized','double precision','causal 96-bar rolling z','completed spot 15m signal_ts','window ends at decision_ts',86400,false,true,jsonb_build_object('window_bars',96,'minimum_bars',24))
) v(feature_key,dataset_key,feature_name,feature_family,value_type,source_expression,decision_time_rule,observable_at_rule,lookback_seconds,uses_future_data,enabled,metadata)
on conflict(feature_key) do update set
    dataset_key=excluded.dataset_key,feature_name=excluded.feature_name,feature_family=excluded.feature_family,
    value_type=excluded.value_type,source_expression=excluded.source_expression,
    decision_time_rule=excluded.decision_time_rule,observable_at_rule=excluded.observable_at_rule,
    lookback_seconds=excluded.lookback_seconds,uses_future_data=false,enabled=true,
    metadata=research_hub.feature_definitions.metadata||excluded.metadata,updated_at=now();

insert into research_hub.feature_sets(
    feature_set_key,description,decision_grain,source_dataset_keys,feature_keys,
    materialization_schema,materialization_relation,point_in_time_verified,verification_notes,metadata
) values(
    'crypto.positioning15m.v1',
    'Typed PIT Binance spot path/activity plus recovered OI, global/top positioning and futures taker flow. Funding, basis and order-book microstructure are separate families.',
    'instrument-15m',array['research_hub.binance_spot15m_positioning_v1','primary.crypto_derivatives_metrics'],
    array[
      'pos.spot_ret15','pos.spot_ret60','pos.spot_ret240','pos.spot_log_quote_volume','pos.spot_log_trade_count',
      'pos.log_oi_value','pos.oi_chg_5m','pos.oi_chg_15m','pos.oi_chg_60m','pos.global_ls_log','pos.global_ls_chg_15m','pos.global_ls_chg_60m',
      'pos.top_account_ls_log','pos.top_account_ls_chg_15m','pos.top_position_ls_log','pos.top_position_ls_chg_15m','pos.account_global_div','pos.position_global_div',
      'pos.taker_log_ratio','pos.taker_imbalance','pos.taker_chg_15m','pos.taker_chg_60m','pos.ret15_x_oi15','pos.ret60_x_oi60','pos.taker_x_oi15','pos.position_div_x_ret15',
      'pos.cs_oi_chg15_rank','pos.cs_global_ls_rank','pos.cs_top_position_rank','pos.cs_taker_imbalance_rank','pos.cs_spot_ret15_rank',
      'pos.oi_chg15_z_1d','pos.oi_chg60_z_1d','pos.global_ls_z_1d','pos.top_position_ls_z_1d','pos.position_div_z_1d','pos.taker_imbalance_z_1d','pos.spot_ret15_z_1d','pos.spot_log_quote_volume_z_1d'
    ],
    'research_hub','crypto_positioning15m_features_v1',true,
    'OI and long/short use provider end timestamp +60s; taker ratio uses provider start timestamp +5m period +60s. Spot bar is closed at decision_ts. Historical recovery is discovery/validation only.',
    jsonb_build_object('adapter','crypto_positioning15m_v1','storage','typed','durability','unlogged_reconstructible_cache','total_features',39,'future_replication_required',true,'observability_contract','binance-usdm-observability-v1')
) on conflict(feature_set_key) do update set
    description=excluded.description,decision_grain=excluded.decision_grain,source_dataset_keys=excluded.source_dataset_keys,
    feature_keys=excluded.feature_keys,materialization_schema=excluded.materialization_schema,
    materialization_relation=excluded.materialization_relation,point_in_time_verified=true,
    verification_notes=excluded.verification_notes,metadata=research_hub.feature_sets.metadata||excluded.metadata,updated_at=now();

insert into research_hub.outcome_sets(outcome_set_key,description,outcome_keys,materialization_schema,materialization_relation,metadata)
values(
    'crypto.positioning_spot_nextopen.v1','Separate future spot next-open outcomes at 15m, 60m and 240m plus four-hour MFE/MAE. Costs are applied by the experiment engine.',
    array['pos.gross_900s','pos.gross_3600s','pos.gross_14400s','pos.mfe_14400s','pos.mae_14400s'],
    'research_hub','crypto_positioning15m_outcomes_v1',
    jsonb_build_object('entry','open at decision_ts','exit','exact future bar open','historical_screening_only',true,'future_replication_required',true,'cost_stress_bps',jsonb_build_array(20,50,100),'execution_replication_required_before_promotion',true)
) on conflict(outcome_set_key) do update set
    description=excluded.description,outcome_keys=excluded.outcome_keys,materialization_schema=excluded.materialization_schema,
    materialization_relation=excluded.materialization_relation,metadata=research_hub.outcome_sets.metadata||excluded.metadata,updated_at=now();

insert into research_hub.event_definitions(event_key,event_version,description,source_feature_set_key,condition_spec,decision_time_rule,available_at_rule,point_in_time_safe,enabled,definition_hash,metadata)
select event_key,1,description,'crypto.positioning15m.v1',condition_spec,'completed spot 15m decision timestamp','all components observable by decision_ts',true,true,md5(event_key||'|v1|'||condition_spec::text),jsonb_build_object('historical_screening_only',true,'future_replication_required',true)
from (values
('pos.event.price_up_oi_up','Spot up with OI expansion',jsonb_build_object('all',jsonb_build_array(jsonb_build_object('feature','pos.spot_ret15','op','>','value',0),jsonb_build_object('feature','pos.oi_chg_15m','op','>','value',0)))),
('pos.event.price_down_oi_up','Spot down with OI expansion',jsonb_build_object('all',jsonb_build_array(jsonb_build_object('feature','pos.spot_ret15','op','<','value',0),jsonb_build_object('feature','pos.oi_chg_15m','op','>','value',0)))),
('pos.event.price_up_oi_down','Spot up with OI contraction',jsonb_build_object('all',jsonb_build_array(jsonb_build_object('feature','pos.spot_ret15','op','>','value',0),jsonb_build_object('feature','pos.oi_chg_15m','op','<','value',0)))),
('pos.event.price_down_oi_down','Spot down with OI contraction',jsonb_build_object('all',jsonb_build_array(jsonb_build_object('feature','pos.spot_ret15','op','<','value',0),jsonb_build_object('feature','pos.oi_chg_15m','op','<','value',0)))),
('pos.event.crowded_long_buying','Top-position crowding with aggressive buying',jsonb_build_object('all',jsonb_build_array(jsonb_build_object('feature','pos.position_global_div','op','>','value',0),jsonb_build_object('feature','pos.taker_imbalance','op','>','value',0)))),
('pos.event.crowded_long_selling','Top-position crowding with aggressive selling',jsonb_build_object('all',jsonb_build_array(jsonb_build_object('feature','pos.position_global_div','op','>','value',0),jsonb_build_object('feature','pos.taker_imbalance','op','<','value',0)))),
('pos.sequence.oi_expansion_3bars','OI expansion persists for three decisions',jsonb_build_object('sequence',jsonb_build_array(jsonb_build_object('lag_bars',2,'feature','pos.oi_chg_15m','op','>','value',0),jsonb_build_object('lag_bars',1,'feature','pos.oi_chg_15m','op','>','value',0),jsonb_build_object('lag_bars',0,'feature','pos.oi_chg_15m','op','>','value',0)))),
('pos.sequence.price_down_oi_up_then_taker_buy','Price-down/OI-up then positive taker imbalance',jsonb_build_object('sequence',jsonb_build_array(jsonb_build_object('lag_bars',1,'event','pos.event.price_down_oi_up'),jsonb_build_object('lag_bars',0,'feature','pos.taker_imbalance','op','>','value',0)))),
('pos.sequence.crowded_long_buying_then_spot_down','Crowded-long buying then negative spot return',jsonb_build_object('sequence',jsonb_build_array(jsonb_build_object('lag_bars',1,'event','pos.event.crowded_long_buying'),jsonb_build_object('lag_bars',0,'feature','pos.spot_ret15','op','<','value',0)))),
('pos.sequence.price_up_oi_down_2bars','Price-up/OI-down persists for two decisions',jsonb_build_object('sequence',jsonb_build_array(jsonb_build_object('lag_bars',1,'event','pos.event.price_up_oi_down'),jsonb_build_object('lag_bars',0,'event','pos.event.price_up_oi_down'))))
) e(event_key,description,condition_spec)
on conflict(event_key) do update set event_version=1,description=excluded.description,source_feature_set_key=excluded.source_feature_set_key,condition_spec=excluded.condition_spec,decision_time_rule=excluded.decision_time_rule,available_at_rule=excluded.available_at_rule,point_in_time_safe=true,enabled=true,definition_hash=excluded.definition_hash,metadata=research_hub.event_definitions.metadata||excluded.metadata,updated_at=now();

insert into research_hub.program_jobs(job_key,exact_name,purpose,store_key,source_schema,source_table,source_id,job_kind,current_state,started_at,progress_current,progress_total,completion_pct,latest_result,retry_state,next_automatic_action,intervention_required,exact_intervention,frozen_rule,holdout_sensitive,metadata)
values
('FEATURE-CRYPTO-POSITIONING-V1','Typed crypto OI/positioning/taker 15m panel v1','Materialise the separate PIT OI, positioning, taker-flow and spot interaction family after both sources pass quality gates.','market_data_primary','research_hub','crypto_positioning15m_features_v1','crypto.positioning15m.v1','feature_materialization','waiting_for_spot_recovery_quality',now(),0,218,0,jsonb_build_object('feature_count',39,'event_count',10,'historical_holdout_available',false,'future_replication_required',true),'automatic after source-quality completion','Continue public Binance spot recovery, then materialise one typed symbol per clean compute slot. No API key or Rob action required.',false,null,true,true,jsonb_build_object('discovery_start','2026-07-14T00:00:00Z','discovery_end','2026-07-30T00:00:00Z','validation_start','2026-07-30T00:00:00Z','validation_end','2026-08-12T10:15:00Z','future_replication_start','2026-08-13T00:00:00Z','credentials_required',false,'no_duplication_with','FEATURE-CRYPTO-SPOT-FUTURES-V1; FEATURE-CRYPTO-MICRO-V1')),
('EXPERIMENT-CRYPTO-POSITIONING-V1','Screen typed crypto OI/positioning/taker family v1','Run pooled cross-sectional and event/sequence discovery with one global multiplicity family and mandatory future replication.','market_data_primary','research_hub','crypto_positioning15m_features_v1','crypto.positioning15m.v1','experiment','queued_waiting_feature_materialization',now(),0,1,0,jsonb_build_object('holdout_available',false,'promotion_requires_future_replication',true),'frozen search contract','After panel completion, execute only the frozen manifest with global BH-FDR, dependence controls, placebos and 20/50/100bp stress.',false,null,true,true,jsonb_build_object('feature_set_key','crypto.positioning15m.v1','outcome_set_key','crypto.positioning_spot_nextopen.v1','pooled_cross_sectional_primary',true,'per_symbol_promotion_forbidden_due_short_window',true,'direction_inference','two_sided_if_selected_from_discovery','global_multiple_testing_required',true,'future_replication_start','2026-08-13T00:00:00Z'))
on conflict(job_key) do update set exact_name=excluded.exact_name,purpose=excluded.purpose,current_state=excluded.current_state,latest_result=research_hub.program_jobs.latest_result||excluded.latest_result,retry_state=excluded.retry_state,next_automatic_action=excluded.next_automatic_action,intervention_required=false,exact_intervention=null,frozen_rule=true,holdout_sensitive=true,metadata=research_hub.program_jobs.metadata||excluded.metadata,updated_at=now();
