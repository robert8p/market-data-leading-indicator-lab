-- Freeze the complete positioning search universe before any statistical result is inspected.
-- The 31,122 raw tests pay one global BH-FDR family; count is not presented as conceptual breadth.

create table if not exists research_hub.crypto_positioning15m_liquidity_rules_v1(
    tier_key text primary key,p10_quote_volume_min_usdt double precision not null,p10_trade_count_min double precision not null,
    role text not null,definition_frozen_at timestamptz not null default now(),metadata jsonb not null default '{}'::jsonb
);
insert into research_hub.crypto_positioning15m_liquidity_rules_v1(tier_key,p10_quote_volume_min_usdt,p10_trade_count_min,role,metadata)
values
('L1',1000000,500,'high-liquidity primary',jsonb_build_object('account_size_context_gbp',2000,'nested_tier',true)),
('L2',250000,150,'medium-liquidity primary',jsonb_build_object('account_size_context_gbp',2000,'nested_tier',true)),
('L3',50000,50,'minimum executable discovery tier',jsonb_build_object('account_size_context_gbp',2000,'nested_tier',true))
on conflict(tier_key) do update set p10_quote_volume_min_usdt=excluded.p10_quote_volume_min_usdt,p10_trade_count_min=excluded.p10_trade_count_min,role=excluded.role,metadata=research_hub.crypto_positioning15m_liquidity_rules_v1.metadata||excluded.metadata;
revoke all on table research_hub.crypto_positioning15m_liquidity_rules_v1 from public,anon,authenticated;

create table if not exists research_hub.crypto_positioning15m_liquidity_members_v1(
    tier_key text not null references research_hub.crypto_positioning15m_liquidity_rules_v1(tier_key),
    canonical_symbol text not null,discovery_bars bigint not null,p10_quote_volume_usdt double precision,
    p10_trade_count double precision,eligible boolean not null,membership_hash text not null,
    frozen_at timestamptz not null default now(),metadata jsonb not null default '{}'::jsonb,
    primary key(tier_key,canonical_symbol)
);
revoke all on table research_hub.crypto_positioning15m_liquidity_members_v1 from public,anon,authenticated;

create table if not exists research_hub.crypto_positioning15m_regimes_v1(
    regime_key text primary key,description text not null,condition_spec jsonb not null,
    learned_on_discovery_only boolean not null default false,frozen_at timestamptz not null default now()
);
insert into research_hub.crypto_positioning15m_regimes_v1(regime_key,description,condition_spec,learned_on_discovery_only)
values
('ALL','All eligible decisions',jsonb_build_object('type','all'),false),
('ASIA_00_08','UTC hour 00:00–07:59',jsonb_build_object('utc_hour_gte',0,'utc_hour_lt',8),false),
('EUROPE_08_14','UTC hour 08:00–13:59',jsonb_build_object('utc_hour_gte',8,'utc_hour_lt',14),false),
('US_14_21','UTC hour 14:00–20:59',jsonb_build_object('utc_hour_gte',14,'utc_hour_lt',21),false),
('LATE_21_24','UTC hour 21:00–23:59',jsonb_build_object('utc_hour_gte',21,'utc_hour_lt',24),false),
('WEEKDAY','Monday through Friday UTC',jsonb_build_object('iso_dow_in',jsonb_build_array(1,2,3,4,5)),false),
('WEEKEND','Saturday or Sunday UTC',jsonb_build_object('iso_dow_in',jsonb_build_array(6,7)),false),
('BTC_UP','BTC 1h return positive',jsonb_build_object('context_feature','btc_ret60','op','>','value',0),false),
('BTC_DOWN','BTC 1h return negative',jsonb_build_object('context_feature','btc_ret60','op','<','value',0),false),
('MARKET_HIGH_VOL','Absolute BTC 1h return above discovery median',jsonb_build_object('context_feature','abs_btc_ret60','op','>','threshold_source','discovery_median'),true),
('MARKET_LOW_VOL','Absolute BTC 1h return at or below discovery median',jsonb_build_object('context_feature','abs_btc_ret60','op','<=','threshold_source','discovery_median'),true),
('BREADTH_POS','Positive-return breadth above 50%',jsonb_build_object('context_feature','breadth_up_fraction','op','>','value',0.5),false),
('BREADTH_NEG','Positive-return breadth at or below 50%',jsonb_build_object('context_feature','breadth_up_fraction','op','<=','value',0.5),false)
on conflict(regime_key) do update set description=excluded.description,condition_spec=excluded.condition_spec,learned_on_discovery_only=excluded.learned_on_discovery_only;
revoke all on table research_hub.crypto_positioning15m_regimes_v1 from public,anon,authenticated;

create table if not exists research_hub.crypto_positioning15m_hypothesis_manifest_v1(
    hypothesis_id text primary key,test_kind text not null,predictor_key text not null,tail text,tail_quantile double precision,
    horizon_seconds integer not null,regime_key text not null references research_hub.crypto_positioning15m_regimes_v1(regime_key),
    liquidity_tier text not null references research_hub.crypto_positioning15m_liquidity_rules_v1(tier_key),
    direction_policy text not null,pvalue_policy text not null,multiplicity_family text not null,
    minimum_discovery_n integer not null,minimum_validation_n integer not null,minimum_symbols integer not null,minimum_utc_days integer not null,
    status text not null default 'FROZEN_UNEXECUTED',definition_hash text not null,frozen_at timestamptz not null default now(),metadata jsonb not null default '{}'::jsonb,
    unique(test_kind,predictor_key,tail,tail_quantile,horizon_seconds,regime_key,liquidity_tier)
);
revoke all on table research_hub.crypto_positioning15m_hypothesis_manifest_v1 from public,anon,authenticated;

truncate research_hub.crypto_positioning15m_hypothesis_manifest_v1;

with scalar_features(feature_key) as (
    select unnest(array[
      'pos.spot_ret15','pos.spot_ret60','pos.spot_ret240','pos.oi_chg_5m','pos.oi_chg_15m','pos.oi_chg_60m',
      'pos.global_ls_chg_15m','pos.global_ls_chg_60m','pos.top_account_ls_chg_15m','pos.top_position_ls_chg_15m',
      'pos.account_global_div','pos.position_global_div','pos.taker_imbalance','pos.taker_chg_15m','pos.taker_chg_60m',
      'pos.ret15_x_oi15','pos.ret60_x_oi60','pos.taker_x_oi15','pos.position_div_x_ret15',
      'pos.cs_oi_chg15_rank','pos.cs_global_ls_rank','pos.cs_top_position_rank','pos.cs_taker_imbalance_rank','pos.cs_spot_ret15_rank',
      'pos.oi_chg15_z_1d','pos.oi_chg60_z_1d','pos.global_ls_z_1d','pos.top_position_ls_z_1d',
      'pos.position_div_z_1d','pos.taker_imbalance_z_1d','pos.spot_ret15_z_1d','pos.spot_log_quote_volume_z_1d'
    ]::text[])
), grid as (
    select f.feature_key,t.tail,q.quantile,h.horizon_seconds,r.regime_key,l.tier_key
    from scalar_features f
    cross join (values('HIGH'),('LOW')) t(tail)
    cross join (values(0.01::double precision),(0.02),(0.05),(0.10)) q(quantile)
    cross join (values(900),(3600),(14400)) h(horizon_seconds)
    cross join research_hub.crypto_positioning15m_regimes_v1 r
    cross join research_hub.crypto_positioning15m_liquidity_rules_v1 l
)
insert into research_hub.crypto_positioning15m_hypothesis_manifest_v1(
    hypothesis_id,test_kind,predictor_key,tail,tail_quantile,horizon_seconds,regime_key,liquidity_tier,
    direction_policy,pvalue_policy,multiplicity_family,minimum_discovery_n,minimum_validation_n,minimum_symbols,minimum_utc_days,definition_hash,metadata
)
select 'POS-'||upper(substr(md5(concat_ws('|','scalar',feature_key,tail,quantile,horizon_seconds,regime_key,tier_key)),1,16)),
    'scalar_tail',feature_key,tail,quantile,horizon_seconds,regime_key,tier_key,
    'DISCOVERY_SIGN_WITHOUT_HOLDOUT','TWO_SIDED_SIGN_SELECTION_ADJUSTED','POSITIONING15M_GLOBAL_V1',
    1000,500,20,10,md5(concat_ws('|','scalar_tail',feature_key,tail,quantile,horizon_seconds,regime_key,tier_key,'v1')),
    jsonb_build_object('cost_stress_bps',jsonb_build_array(20,50,100),'future_replication_required',true,'historical_holdout_available',false)
from grid;

with event_features(event_key) as (
    select unnest(array[
      'pos.event.price_up_oi_up','pos.event.price_down_oi_up','pos.event.price_up_oi_down','pos.event.price_down_oi_down',
      'pos.event.crowded_long_buying','pos.event.crowded_long_selling',
      'pos.sequence.oi_expansion_3bars','pos.sequence.price_down_oi_up_then_taker_buy',
      'pos.sequence.crowded_long_buying_then_spot_down','pos.sequence.price_up_oi_down_2bars'
    ]::text[])
), grid as (
    select e.event_key,h.horizon_seconds,r.regime_key,l.tier_key
    from event_features e cross join (values(900),(3600),(14400)) h(horizon_seconds)
    cross join research_hub.crypto_positioning15m_regimes_v1 r
    cross join research_hub.crypto_positioning15m_liquidity_rules_v1 l
)
insert into research_hub.crypto_positioning15m_hypothesis_manifest_v1(
    hypothesis_id,test_kind,predictor_key,tail,tail_quantile,horizon_seconds,regime_key,liquidity_tier,
    direction_policy,pvalue_policy,multiplicity_family,minimum_discovery_n,minimum_validation_n,minimum_symbols,minimum_utc_days,definition_hash,metadata
)
select 'POS-'||upper(substr(md5(concat_ws('|','event',event_key,horizon_seconds,regime_key,tier_key)),1,16)),
    case when event_key like 'pos.sequence.%' then 'sequence_event' else 'sign_event' end,
    event_key,null,null,horizon_seconds,regime_key,tier_key,
    'DISCOVERY_SIGN_WITHOUT_HOLDOUT','TWO_SIDED_SIGN_SELECTION_ADJUSTED','POSITIONING15M_GLOBAL_V1',
    500,200,15,8,md5(concat_ws('|','event',event_key,horizon_seconds,regime_key,tier_key,'v1')),
    jsonb_build_object('cost_stress_bps',jsonb_build_array(20,50,100),'future_replication_required',true,'historical_holdout_available',false)
from grid;

do $$
declare v_count bigint; v_hash text;
begin
    select count(*),md5(string_agg(definition_hash,'|' order by hypothesis_id)) into v_count,v_hash
    from research_hub.crypto_positioning15m_hypothesis_manifest_v1;
    if v_count<>31122 then raise exception 'Unexpected positioning hypothesis count %, expected 31122',v_count; end if;
    update research_hub.crypto_positioning15m_control_v1
    set experiment_frozen=true,definition_hash=md5(definition_hash||'|'||v_hash),
        metadata=metadata||jsonb_build_object(
          'frozen_hypothesis_count',v_count,'hypothesis_manifest_hash',v_hash,
          'scalar_feature_count',32,'event_sequence_count',10,'regime_count',13,'liquidity_tier_count',3,
          'tail_quantiles',jsonb_build_array(0.01,0.02,0.05,0.10),'horizons_seconds',jsonb_build_array(900,3600,14400),
          'global_bh_fdr_required',true,'blank_canvas_breadth_claim',false,
          'note','Raw count is recorded for multiplicity and is not evidence of conceptual breadth.'
        ),updated_at=now()
    where singleton=true;
    update research_hub.program_jobs
    set latest_result=latest_result||jsonb_build_object('frozen_hypothesis_count',v_count,'hypothesis_manifest_hash',v_hash),
        metadata=metadata||jsonb_build_object('manifest_frozen_before_results',true,'global_bh_fdr_required',true),updated_at=now()
    where job_key='EXPERIMENT-CRYPTO-POSITIONING-V1';
end $$;
