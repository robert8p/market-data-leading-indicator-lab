-- Distinct v2 predictor contract. Coinbase native quote_volume remains absent;
-- use an explicitly named point-in-time notional-volume proxy (base volume * OHLC4).

create or replace view research_hub.crypto_crossvenue_observations_v2
with (security_invoker=true) as
with b as (
  select v.*,x.c_notional_volume_proxy,
         lag(v.source_ts,5) over(partition by v.symbol order by v.source_ts) as proxy_lag_ts_5,
         avg(x.c_notional_volume_proxy) over(partition by v.symbol order by v.source_ts rows between 5 preceding and 1 preceding) as avg_c_proxy_prev5
  from research_hub.crypto_crossvenue_observations_v1 v
  join public.crypto_research_crossvenue_1m x on x.symbol=v.symbol and x.ts=v.source_ts
), c as (
  select b.*,
         case when b.b_quote_volume is not null and b.b_quote_volume>=0 and b.c_notional_volume_proxy is not null and b.c_notional_volume_proxy>=0
              then ln(1+b.b_quote_volume)-ln(1+b.c_notional_volume_proxy) end as notional_volume_log_gap_bc_proxy,
         case when b.proxy_lag_ts_5=b.source_ts-interval '5 minutes' and b.avg_c_proxy_prev5 is not null and b.avg_c_proxy_prev5>=0 and b.c_notional_volume_proxy is not null and b.c_notional_volume_proxy>=0
              then ln((1+b.c_notional_volume_proxy)/(1+b.avg_c_proxy_prev5)) end as c_volume_shock_proxy_5m
  from b
)
select c.*,
       case when c.b_volume_shock_5m is not null and c.c_volume_shock_proxy_5m is not null
            then c.b_volume_shock_5m-c.c_volume_shock_proxy_5m end as volume_shock_gap_5m_bc_proxy
from c;

create or replace view research_hub.crypto_crossvenue_feature_rows_v2
with (security_invoker=true) as
select * from research_hub.feature_rows where feature_set_key='crypto.crossvenue.sync.v2';

insert into research_hub.datasets(dataset_key,store_key,schema_name,relation_name,asset_class,provider,frequency,grain,ts_column,instrument_column,observable_at_column,is_raw,point_in_time_safe,row_estimate,status,metadata)
values(
  'derived.crypto_crossvenue_observations_v2','market_data_primary','research_hub','crypto_crossvenue_observations_v2','crypto','binance+coinbase','1m','symbol-minute','source_ts','symbol','observable_at',false,true,637056,'active',
  jsonb_build_object('role','canonical_crossvenue_predictor_source','adaptive_reuse',true,'historical_window_role','discovery_validation_only','coinbase_volume_proxy_formula','base_volume * OHLC4','coinbase_volume_proxy_label','not native quote volume','promotion_requires_future_replication',true)
) on conflict(dataset_key) do update set relation_name=excluded.relation_name,point_in_time_safe=true,status='active',metadata=excluded.metadata,updated_at=now();

insert into research_hub.datasets(dataset_key,store_key,schema_name,relation_name,asset_class,provider,frequency,grain,ts_column,instrument_column,observable_at_column,is_raw,point_in_time_safe,row_estimate,status,metadata)
values(
  'derived.crypto_crossvenue_features_v2','market_data_primary','research_hub','crypto_crossvenue_feature_rows_v2','crypto','binance+coinbase','1m','symbol-minute','decision_ts','instrument_key','observable_at',false,true,0,'registered',
  jsonb_build_object('feature_set_key','crypto.crossvenue.sync.v2','adaptive_reuse',true,'promotion_requires_future_replication',true)
) on conflict(dataset_key) do update set point_in_time_safe=true,metadata=excluded.metadata,updated_at=now();

insert into research_hub.feature_sets(feature_set_key,description,decision_grain,source_dataset_keys,feature_keys,materialization_schema,materialization_relation,point_in_time_verified,verification_notes,metadata)
values(
 'crypto.crossvenue.sync.v2',
 'Point-in-time Binance/Coinbase synchronized cross-venue state v2 with explicit Coinbase notional-volume proxy and frozen 1/2/5/10/15/30m lag family.',
 '1 minute',array['derived.crypto_crossvenue_observations_v2'],
 array[
 'cv.b_ret_1m','cv.c_ret_1m','cv.return_gap_1m_bc','cv.price_log_gap_bc','cv.price_gap_change_1m_bc','cv.notional_volume_log_gap_bc_proxy','cv.range_gap_1m_bc',
 'cv.b_ret_2m','cv.c_ret_2m','cv.return_gap_2m_bc','cv.b_ret_5m','cv.c_ret_5m','cv.return_gap_5m_bc','cv.b_ret_10m','cv.c_ret_10m','cv.return_gap_10m_bc',
 'cv.b_ret_15m','cv.c_ret_15m','cv.return_gap_15m_bc','cv.b_ret_30m','cv.c_ret_30m','cv.return_gap_30m_bc','cv.b_volume_shock_5m','cv.c_volume_shock_proxy_5m','cv.volume_shock_gap_5m_bc_proxy',
 'cv.return_gap_lag1m_bc','cv.return_gap_lag2m_bc','cv.return_gap_lag5m_bc'],
 'research_hub','crypto_crossvenue_feature_rows_v2',true,
 'Source minute t becomes observable at t+60s. All lags use completed contiguous wall-clock minutes. Coinbase native quote volume is absent; notional proxy is base volume × OHLC4, computed from the same completed minute and explicitly labelled as a proxy.',
 jsonb_build_object('definition_version','crypto-crossvenue-sync-v2.0','feature_family_frozen_before_screen',true,'adaptive_reuse',true,'promotion_requires_future_replication',true,'coinbase_volume_proxy_formula','base_volume * OHLC4','frozen_lag_family_minutes',jsonb_build_array(1,2,5,10,15,30),'placebo_controls_required',jsonb_build_array('time_shuffle','symbol_permutation','reverse_venue_role'))
) on conflict(feature_set_key) do update set description=excluded.description,source_dataset_keys=excluded.source_dataset_keys,feature_keys=excluded.feature_keys,point_in_time_verified=true,verification_notes=excluded.verification_notes,metadata=excluded.metadata,updated_at=now();

insert into research_hub.feature_definitions(feature_key,dataset_key,feature_name,feature_family,value_type,source_expression,decision_time_rule,observable_at_rule,lookback_seconds,uses_future_data,enabled,metadata)
values
('cv.notional_volume_log_gap_bc_proxy','derived.crypto_crossvenue_observations_v2','Binance vs Coinbase notional-volume log gap proxy','cross_venue_volume','double precision','ln(1+Binance quote_volume)-ln(1+Coinbase base_volume*OHLC4)','decision_ts=source_ts+60s','observable_at=decision_ts',60,false,true,jsonb_build_object('coinbase_side_is_proxy',true,'proxy_formula','base_volume * OHLC4')),
('cv.c_volume_shock_proxy_5m','derived.crypto_crossvenue_observations_v2','Coinbase notional-volume proxy shock vs prior 5 minutes','cross_venue_volume','double precision','ln((1+current Coinbase notional proxy)/(1+mean prior 5 completed proxies))','decision_ts=source_ts+60s','observable_at=decision_ts',300,false,true,jsonb_build_object('coinbase_side_is_proxy',true,'proxy_formula','base_volume * OHLC4')),
('cv.volume_shock_gap_5m_bc_proxy','derived.crypto_crossvenue_observations_v2','Binance native quote-volume shock minus Coinbase notional-volume proxy shock','cross_venue_volume','double precision','cv.b_volume_shock_5m-cv.c_volume_shock_proxy_5m','decision_ts=source_ts+60s','observable_at=decision_ts',300,false,true,jsonb_build_object('coinbase_side_is_proxy',true,'proxy_formula','base_volume * OHLC4'))
on conflict(feature_key) do update set dataset_key=excluded.dataset_key,feature_name=excluded.feature_name,feature_family=excluded.feature_family,source_expression=excluded.source_expression,decision_time_rule=excluded.decision_time_rule,observable_at_rule=excluded.observable_at_rule,lookback_seconds=excluded.lookback_seconds,enabled=true,metadata=excluded.metadata,updated_at=now();

update research_hub.feature_definitions
set enabled=false,metadata=metadata||jsonb_build_object('deprecated_reason','Coinbase quote_volume absent in synchronized source; superseded by explicitly labelled v2 notional-volume proxy feature'),updated_at=now()
where feature_key in ('cv.quote_volume_log_gap_bc','cv.c_volume_shock_5m','cv.volume_shock_gap_5m_bc');

insert into research_hub.statistical_test_profiles(profile_key,description,test_spec,promotion_gate,enabled,version)
select 'crypto_crossvenue_v2',
       'Cross-venue v2 discovery/validation battery with explicit Coinbase notional-volume proxy and mandatory future replication.',
       test_spec||jsonb_build_object('coinbase_volume_semantics',jsonb_build_object('native_quote_volume_available',false,'proxy_formula','base_volume * OHLC4','proxy_must_remain_explicit_in_candidate_definition',true)),
       promotion_gate||jsonb_build_object('volume_proxy_candidate_requires_native_or_independent_execution_confirmation',true),
       true,2
from research_hub.statistical_test_profiles where profile_key='crypto_crossvenue_v1'
on conflict(profile_key) do update set description=excluded.description,test_spec=excluded.test_spec,promotion_gate=excluded.promotion_gate,enabled=true,version=2,updated_at=now();

create or replace function research_hub.materialize_crypto_crossvenue_symbol_v2(p_symbol text)
returns jsonb
language plpgsql
set search_path=research_hub,public,extensions,pg_temp
as $$
declare v_symbol text:=upper(btrim(p_symbol)); v_features bigint:=0; v_watermark timestamptz;
begin
 if v_symbol is null or v_symbol='' then raise exception 'symbol required'; end if;
 insert into research_hub.feature_rows(feature_set_key,instrument_key,decision_ts,observable_at,features,source_dataset_key,source_row_hash,quality)
 select 'crypto.crossvenue.sync.v2','cv:'||o.symbol,o.decision_ts,o.observable_at,
 jsonb_strip_nulls(jsonb_build_object(
 'cv.b_ret_1m',o.b_ret_1m,'cv.c_ret_1m',o.c_ret_1m,'cv.return_gap_1m_bc',o.return_gap_1m_bc,'cv.price_log_gap_bc',o.price_log_gap_bc,'cv.price_gap_change_1m_bc',o.price_gap_change_1m_bc,'cv.notional_volume_log_gap_bc_proxy',o.notional_volume_log_gap_bc_proxy,'cv.range_gap_1m_bc',o.range_gap_1m_bc,
 'cv.b_ret_2m',o.b_ret_2m,'cv.c_ret_2m',o.c_ret_2m,'cv.return_gap_2m_bc',o.return_gap_2m_bc,
 'cv.b_ret_5m',o.b_ret_5m,'cv.c_ret_5m',o.c_ret_5m,'cv.return_gap_5m_bc',o.return_gap_5m_bc,
 'cv.b_ret_10m',o.b_ret_10m,'cv.c_ret_10m',o.c_ret_10m,'cv.return_gap_10m_bc',o.return_gap_10m_bc,
 'cv.b_ret_15m',o.b_ret_15m,'cv.c_ret_15m',o.c_ret_15m,'cv.return_gap_15m_bc',o.return_gap_15m_bc,
 'cv.b_ret_30m',o.b_ret_30m,'cv.c_ret_30m',o.c_ret_30m,'cv.return_gap_30m_bc',o.return_gap_30m_bc,
 'cv.b_volume_shock_5m',o.b_volume_shock_5m,'cv.c_volume_shock_proxy_5m',o.c_volume_shock_proxy_5m,'cv.volume_shock_gap_5m_bc_proxy',o.volume_shock_gap_5m_bc_proxy,
 'cv.return_gap_lag1m_bc',o.return_gap_lag1m_bc,'cv.return_gap_lag2m_bc',o.return_gap_lag2m_bc,'cv.return_gap_lag5m_bc',o.return_gap_lag5m_bc)),
 'derived.crypto_crossvenue_observations_v2',
 encode(digest(concat_ws('|',o.symbol,o.source_ts,o.b_open,o.b_close,o.c_open,o.c_close,o.b_quote_volume,o.c_notional_volume_proxy),'sha256'),'hex'),
 jsonb_build_object('adaptive_reuse',true,'promotion_requires_future_replication',true,'feature_definition_version','crypto-crossvenue-sync-v2.0','coinbase_volume_proxy_formula','base_volume * OHLC4','coinbase_native_quote_volume_used',false)
 from research_hub.crypto_crossvenue_observations_v2 o where o.symbol=v_symbol
 on conflict(feature_set_key,instrument_key,decision_ts) do update set observable_at=excluded.observable_at,features=excluded.features,source_dataset_key=excluded.source_dataset_key,source_row_hash=excluded.source_row_hash,quality=excluded.quality;
 get diagnostics v_features=row_count;
 select max(source_ts) into v_watermark from research_hub.crypto_crossvenue_observations_v2 where symbol=v_symbol;
 return jsonb_build_object('symbol',v_symbol,'feature_rows',v_features,'last_source_ts',v_watermark,'holdout_accessed',false,'outcome_accessed',false,'feature_definition_version','crypto-crossvenue-sync-v2.0');
end $$;