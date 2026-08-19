-- Canonical Binance/Coinbase synchronized research representation.
-- Reuses synchronized raw close/open/volume fields but deliberately ignores the
-- legacy log_ratio because its sign convention was ambiguous. Every formula here
-- names Binance-minus-Coinbase semantics explicitly. Historical June/July data
-- are adaptive-reuse discovery/validation only; promotion requires new data.

create or replace view research_hub.crypto_crossvenue_observations_v1
with (security_invoker=true) as
with base as (
  select x.symbol,x.ts,x.b_open,x.b_high,x.b_low,x.b_close,x.b_quote_volume,
         x.c_open,x.c_high,x.c_low,x.c_close,x.c_quote_volume,
         case when x.b_open>0 and x.b_close>0 then x.b_close/x.b_open-1 end b_ret_1m,
         case when x.c_open>0 and x.c_close>0 then x.c_close/x.c_open-1 end c_ret_1m,
         case when x.b_close>0 and x.c_close>0 then ln(x.b_close/x.c_close) end price_log_gap_bc,
         ln(1+greatest(coalesce(x.b_quote_volume,0),0))-ln(1+greatest(coalesce(x.c_quote_volume,0),0)) quote_volume_log_gap_bc,
         case when x.b_open>0 then (x.b_high-x.b_low)/x.b_open end b_range_1m,
         case when x.c_open>0 then (x.c_high-x.c_low)/x.c_open end c_range_1m
  from public.crypto_research_crossvenue_1m x
  where x.ts>=timestamptz '2026-06-28 00:00:00+00'
    and x.ts<timestamptz '2026-07-28 16:53:00+00'
), w as (
  select b.*,
    lag(ts,1) over(partition by symbol order by ts) lag_ts_1,
    lag(ts,5) over(partition by symbol order by ts) lag_ts_5,
    lag(b_close,5) over(partition by symbol order by ts) lag_b_close_5,
    lag(c_close,5) over(partition by symbol order by ts) lag_c_close_5,
    lag(price_log_gap_bc,1) over(partition by symbol order by ts) lag_price_log_gap_1,
    lag(b_ret_1m,1) over(partition by symbol order by ts) lag_b_ret_1m,
    lag(c_ret_1m,1) over(partition by symbol order by ts) lag_c_ret_1m,
    avg(b_quote_volume) over(partition by symbol order by ts rows between 5 preceding and 1 preceding) avg_b_qv_prev5,
    avg(c_quote_volume) over(partition by symbol order by ts rows between 5 preceding and 1 preceding) avg_c_qv_prev5
  from base b
)
select symbol,ts source_ts,ts+interval '1 minute' decision_ts,ts+interval '1 minute' observable_at,
  b_ret_1m,c_ret_1m,
  case when b_ret_1m is not null and c_ret_1m is not null then b_ret_1m-c_ret_1m end return_gap_1m_bc,
  price_log_gap_bc,
  case when lag_ts_1=ts-interval '1 minute' then price_log_gap_bc-lag_price_log_gap_1 end price_gap_change_1m_bc,
  quote_volume_log_gap_bc,
  case when b_range_1m is not null and c_range_1m is not null then b_range_1m-c_range_1m end range_gap_1m_bc,
  case when lag_ts_5=ts-interval '5 minutes' and lag_b_close_5>0 then b_close/lag_b_close_5-1 end b_ret_5m,
  case when lag_ts_5=ts-interval '5 minutes' and lag_c_close_5>0 then c_close/lag_c_close_5-1 end c_ret_5m,
  case when lag_ts_5=ts-interval '5 minutes' and lag_b_close_5>0 and lag_c_close_5>0 then (b_close/lag_b_close_5-1)-(c_close/lag_c_close_5-1) end return_gap_5m_bc,
  case when lag_ts_5=ts-interval '5 minutes' and avg_b_qv_prev5>=0 then ln((1+greatest(coalesce(b_quote_volume,0),0))/(1+avg_b_qv_prev5)) end b_volume_shock_5m,
  case when lag_ts_5=ts-interval '5 minutes' and avg_c_qv_prev5>=0 then ln((1+greatest(coalesce(c_quote_volume,0),0))/(1+avg_c_qv_prev5)) end c_volume_shock_5m,
  case when lag_ts_5=ts-interval '5 minutes' and avg_b_qv_prev5>=0 and avg_c_qv_prev5>=0 then ln((1+greatest(coalesce(b_quote_volume,0),0))/(1+avg_b_qv_prev5))-ln((1+greatest(coalesce(c_quote_volume,0),0))/(1+avg_c_qv_prev5)) end volume_shock_gap_5m_bc,
  case when lag_ts_1=ts-interval '1 minute' and lag_b_ret_1m is not null and lag_c_ret_1m is not null then lag_b_ret_1m-lag_c_ret_1m end return_gap_lag1m_bc,
  b_open,b_close,c_open,c_close,b_quote_volume,c_quote_volume
from w;

insert into research_hub.datasets(dataset_key,store_key,schema_name,relation_name,asset_class,provider,frequency,grain,ts_column,instrument_column,observable_at_column,is_raw,point_in_time_safe,coverage_start,coverage_end,row_estimate,status,metadata)
values('primary.crypto_crossvenue_sync_1m_v1','market_data_primary','research_hub','crypto_crossvenue_observations_v1','crypto','binance+coinbase','1m','canonical-symbol-minute','decision_ts','symbol','observable_at',false,true,timestamptz '2026-06-28 00:01:00+00',timestamptz '2026-07-28 16:53:00+00',637056,'active',jsonb_build_object('source_relation','public.crypto_research_crossvenue_1m','legacy_log_ratio_ignored',true,'explicit_price_gap_semantics','ln(Binance close / Coinbase close)','adaptive_reuse',true,'promotion_requires_future_replication',true))
on conflict(dataset_key) do update set relation_name=excluded.relation_name,point_in_time_safe=true,coverage_start=excluded.coverage_start,coverage_end=excluded.coverage_end,row_estimate=excluded.row_estimate,status=excluded.status,metadata=excluded.metadata,updated_at=now();

insert into research_hub.feature_sets(feature_set_key,description,decision_grain,source_dataset_keys,feature_keys,materialization_schema,materialization_relation,point_in_time_verified,verification_notes,metadata)
values('crypto.crossvenue.sync.v1','Explicit Binance/Coinbase synchronized state features from completed one-minute bars.','canonical-symbol-minute',array['primary.crypto_crossvenue_sync_1m_v1'],array['cv.b_ret_1m','cv.c_ret_1m','cv.return_gap_1m_bc','cv.price_log_gap_bc','cv.price_gap_change_1m_bc','cv.quote_volume_log_gap_bc','cv.range_gap_1m_bc','cv.b_ret_5m','cv.c_ret_5m','cv.return_gap_5m_bc','cv.b_volume_shock_5m','cv.c_volume_shock_5m','cv.volume_shock_gap_5m_bc','cv.return_gap_lag1m_bc'],'research_hub','feature_rows',true,'Source bar t is observable only at t+60s. Rolling/lag features require exact timestamp continuity and use current/prior completed bars only.',jsonb_build_object('adaptive_reuse',true,'promotion_requires_future_replication',true,'purpose','cross-venue lag/event discovery'))
on conflict(feature_set_key) do update set description=excluded.description,source_dataset_keys=excluded.source_dataset_keys,feature_keys=excluded.feature_keys,point_in_time_verified=true,verification_notes=excluded.verification_notes,metadata=excluded.metadata,updated_at=now();

insert into research_hub.feature_definitions(feature_key,dataset_key,feature_name,feature_family,source_expression,decision_time_rule,observable_at_rule,lookback_seconds,uses_future_data,enabled)
values
('cv.b_ret_1m','primary.crypto_crossvenue_sync_1m_v1','Binance completed 1m return','crossvenue_return','b_close/b_open-1','decision_ts=source_ts+60s','completed source bar only',60,false,true),
('cv.c_ret_1m','primary.crypto_crossvenue_sync_1m_v1','Coinbase completed 1m return','crossvenue_return','c_close/c_open-1','decision_ts=source_ts+60s','completed source bar only',60,false,true),
('cv.return_gap_1m_bc','primary.crypto_crossvenue_sync_1m_v1','Binance minus Coinbase 1m return gap','crossvenue_divergence','b_ret_1m-c_ret_1m','decision_ts=source_ts+60s','completed source bar only',60,false,true),
('cv.price_log_gap_bc','primary.crypto_crossvenue_sync_1m_v1','Binance/Coinbase log price gap','crossvenue_divergence','ln(b_close/c_close)','decision_ts=source_ts+60s','completed source bar only',60,false,true),
('cv.price_gap_change_1m_bc','primary.crypto_crossvenue_sync_1m_v1','1m change in Binance/Coinbase log price gap','crossvenue_divergence','price_log_gap_bc-lag(price_log_gap_bc,1)','decision_ts=source_ts+60s','current/prior contiguous bars only',120,false,true),
('cv.quote_volume_log_gap_bc','primary.crypto_crossvenue_sync_1m_v1','Binance minus Coinbase quote-volume gap','crossvenue_volume','ln(1+b_qv)-ln(1+c_qv)','decision_ts=source_ts+60s','completed source bar only',60,false,true),
('cv.range_gap_1m_bc','primary.crypto_crossvenue_sync_1m_v1','Binance minus Coinbase intraminute range gap','crossvenue_range','b_range_1m-c_range_1m','decision_ts=source_ts+60s','completed source bar only',60,false,true),
('cv.b_ret_5m','primary.crypto_crossvenue_sync_1m_v1','Binance trailing 5m return','crossvenue_return','b_close/lag(b_close,5)-1','decision_ts=source_ts+60s','contiguous completed bars only',300,false,true),
('cv.c_ret_5m','primary.crypto_crossvenue_sync_1m_v1','Coinbase trailing 5m return','crossvenue_return','c_close/lag(c_close,5)-1','decision_ts=source_ts+60s','contiguous completed bars only',300,false,true),
('cv.return_gap_5m_bc','primary.crypto_crossvenue_sync_1m_v1','Binance minus Coinbase trailing 5m return gap','crossvenue_divergence','b_ret_5m-c_ret_5m','decision_ts=source_ts+60s','contiguous completed bars only',300,false,true),
('cv.b_volume_shock_5m','primary.crypto_crossvenue_sync_1m_v1','Binance current volume versus prior 5m','crossvenue_volume','ln((1+b_qv)/(1+avg prior5 b_qv))','decision_ts=source_ts+60s','current/prior completed bars only',360,false,true),
('cv.c_volume_shock_5m','primary.crypto_crossvenue_sync_1m_v1','Coinbase current volume versus prior 5m','crossvenue_volume','ln((1+c_qv)/(1+avg prior5 c_qv))','decision_ts=source_ts+60s','current/prior completed bars only',360,false,true),
('cv.volume_shock_gap_5m_bc','primary.crypto_crossvenue_sync_1m_v1','Binance minus Coinbase volume-shock gap','crossvenue_interaction','b_volume_shock_5m-c_volume_shock_5m','decision_ts=source_ts+60s','current/prior completed bars only',360,false,true),
('cv.return_gap_lag1m_bc','primary.crypto_crossvenue_sync_1m_v1','Prior minute Binance minus Coinbase return gap','crossvenue_lag','lag(b_ret_1m-c_ret_1m,1)','decision_ts=source_ts+60s','prior contiguous completed bar only',120,false,true)
on conflict(feature_key) do update set dataset_key=excluded.dataset_key,feature_name=excluded.feature_name,feature_family=excluded.feature_family,source_expression=excluded.source_expression,decision_time_rule=excluded.decision_time_rule,observable_at_rule=excluded.observable_at_rule,lookback_seconds=excluded.lookback_seconds,uses_future_data=false,enabled=true,updated_at=now();

insert into research_hub.outcome_sets(outcome_set_key,description,outcome_keys,materialization_schema,materialization_relation,metadata)
values('crypto.crossvenue.nextopen.v1','Same-symbol next-open to future-close returns on Binance and Coinbase at 1m, 5m and 15m horizons; execution costs applied separately.',array['cv.binance_nextopen_60s','cv.binance_nextopen_300s','cv.binance_nextopen_900s','cv.coinbase_nextopen_60s','cv.coinbase_nextopen_300s','cv.coinbase_nextopen_900s'],'research_hub','outcome_rows',jsonb_build_object('adaptive_reuse',true,'promotion_requires_future_replication',true,'costs_embedded',false))
on conflict(outcome_set_key) do update set description=excluded.description,outcome_keys=excluded.outcome_keys,metadata=excluded.metadata,updated_at=now();

insert into research_hub.outcome_definitions(outcome_key,outcome_name,target_asset_class,horizon_seconds,entry_rule,exit_rule,enabled,metadata)
values
('cv.binance_nextopen_60s','Binance next-open to 1m future close','crypto',60,'next synchronized Binance bar open after decision','close of that bar',true,jsonb_build_object('venue','binance','costs_embedded',false)),
('cv.binance_nextopen_300s','Binance next-open to 5m future close','crypto',300,'next synchronized Binance bar open after decision','future close at 5m',true,jsonb_build_object('venue','binance','costs_embedded',false)),
('cv.binance_nextopen_900s','Binance next-open to 15m future close','crypto',900,'next synchronized Binance bar open after decision','future close at 15m',true,jsonb_build_object('venue','binance','costs_embedded',false)),
('cv.coinbase_nextopen_60s','Coinbase next-open to 1m future close','crypto',60,'next synchronized Coinbase bar open after decision','close of that bar',true,jsonb_build_object('venue','coinbase','costs_embedded',false)),
('cv.coinbase_nextopen_300s','Coinbase next-open to 5m future close','crypto',300,'next synchronized Coinbase bar open after decision','future close at 5m',true,jsonb_build_object('venue','coinbase','costs_embedded',false)),
('cv.coinbase_nextopen_900s','Coinbase next-open to 15m future close','crypto',900,'next synchronized Coinbase bar open after decision','future close at 15m',true,jsonb_build_object('venue','coinbase','costs_embedded',false))
on conflict(outcome_key) do update set outcome_name=excluded.outcome_name,horizon_seconds=excluded.horizon_seconds,entry_rule=excluded.entry_rule,exit_rule=excluded.exit_rule,enabled=true,metadata=excluded.metadata,updated_at=now();

create or replace function research_hub.materialize_crypto_crossvenue_symbol_v1(p_symbol text)
returns jsonb language plpgsql security invoker set search_path=research_hub,public,extensions,pg_temp as $$
declare v_features bigint:=0; v_outcomes bigint:=0;
begin
 insert into research_hub.feature_rows(feature_set_key,instrument_key,decision_ts,observable_at,features,source_dataset_key,source_row_hash,quality)
 select 'crypto.crossvenue.sync.v1','cv:'||o.symbol,o.decision_ts,o.observable_at,
   jsonb_strip_nulls(jsonb_build_object('cv.b_ret_1m',o.b_ret_1m,'cv.c_ret_1m',o.c_ret_1m,'cv.return_gap_1m_bc',o.return_gap_1m_bc,'cv.price_log_gap_bc',o.price_log_gap_bc,'cv.price_gap_change_1m_bc',o.price_gap_change_1m_bc,'cv.quote_volume_log_gap_bc',o.quote_volume_log_gap_bc,'cv.range_gap_1m_bc',o.range_gap_1m_bc,'cv.b_ret_5m',o.b_ret_5m,'cv.c_ret_5m',o.c_ret_5m,'cv.return_gap_5m_bc',o.return_gap_5m_bc,'cv.b_volume_shock_5m',o.b_volume_shock_5m,'cv.c_volume_shock_5m',o.c_volume_shock_5m,'cv.volume_shock_gap_5m_bc',o.volume_shock_gap_5m_bc,'cv.return_gap_lag1m_bc',o.return_gap_lag1m_bc)),
   'primary.crypto_crossvenue_sync_1m_v1',encode(digest(concat_ws('|',o.symbol,o.source_ts,o.b_open,o.b_close,o.c_open,o.c_close,o.b_quote_volume,o.c_quote_volume),'sha256'),'hex'),
   jsonb_build_object('legacy_run_id',o.symbol,'adaptive_reuse',true,'promotion_requires_future_replication',true,'legacy_log_ratio_ignored',true)
 from research_hub.crypto_crossvenue_observations_v1 o where o.symbol=p_symbol
 on conflict(feature_set_key,instrument_key,decision_ts) do update set observable_at=excluded.observable_at,features=excluded.features,source_dataset_key=excluded.source_dataset_key,source_row_hash=excluded.source_row_hash,quality=excluded.quality;
 get diagnostics v_features=row_count;

 insert into research_hub.outcome_rows(outcome_set_key,instrument_key,decision_ts,horizon_seconds,entry_ts,exit_ts,gross_return,metadata)
 with d as (select * from research_hub.crypto_crossvenue_observations_v1 where symbol=p_symbol), h(horizon_seconds) as (values(60),(300),(900)), p as (
   select d.symbol,d.source_ts,d.decision_ts,h.horizon_seconds,e.b_open b_entry,e.c_open c_entry,z.b_close b_exit,z.c_close c_exit
   from d cross join h
   join public.crypto_research_crossvenue_1m e on e.symbol=d.symbol and e.ts=d.source_ts+interval '1 minute'
   join public.crypto_research_crossvenue_1m z on z.symbol=d.symbol and z.ts=d.source_ts+make_interval(secs=>h.horizon_seconds)
 )
 select 'crypto.crossvenue.nextopen.v1','binance:'||symbol,decision_ts,horizon_seconds,decision_ts,source_ts+make_interval(secs=>horizon_seconds)+interval '1 minute',case when b_entry>0 then b_exit/b_entry-1 end,jsonb_build_object('legacy_run_id',symbol,'venue','binance','entry_rule','next_open','costs_embedded',false,'adaptive_reuse',true,'promotion_requires_future_replication',true) from p
 union all
 select 'crypto.crossvenue.nextopen.v1','coinbase:'||symbol,decision_ts,horizon_seconds,decision_ts,source_ts+make_interval(secs=>horizon_seconds)+interval '1 minute',case when c_entry>0 then c_exit/c_entry-1 end,jsonb_build_object('legacy_run_id',symbol,'venue','coinbase','entry_rule','next_open','costs_embedded',false,'adaptive_reuse',true,'promotion_requires_future_replication',true) from p
 on conflict(outcome_set_key,instrument_key,decision_ts,horizon_seconds) do update set entry_ts=excluded.entry_ts,exit_ts=excluded.exit_ts,gross_return=excluded.gross_return,metadata=excluded.metadata;
 get diagnostics v_outcomes=row_count;
 return jsonb_build_object('symbol',p_symbol,'feature_rows',v_features,'outcome_rows',v_outcomes,'holdout_accessed',false);
end $$;

insert into research_hub.research_extracts(extract_key,description,store_key,schema_name,relation_name,dataset_keys,feature_set_key,outcome_set_key,grain,point_in_time_safe,row_estimate,status,metadata)
values('crypto_crossvenue_sync_v1','Canonical synchronized Binance/Coinbase source with explicit sign semantics.','market_data_primary','research_hub','crypto_crossvenue_observations_v1',array['primary.crypto_crossvenue_sync_1m_v1'],'crypto.crossvenue.sync.v1','crypto.crossvenue.nextopen.v1','canonical-symbol-minute',true,637056,'available',jsonb_build_object('preferred_for_ai_discovery',true,'adaptive_reuse',true,'promotion_requires_future_replication',true,'legacy_log_ratio_ignored',true))
on conflict(extract_key) do update set relation_name=excluded.relation_name,feature_set_key=excluded.feature_set_key,outcome_set_key=excluded.outcome_set_key,point_in_time_safe=true,row_estimate=excluded.row_estimate,status=excluded.status,metadata=excluded.metadata,updated_at=now();