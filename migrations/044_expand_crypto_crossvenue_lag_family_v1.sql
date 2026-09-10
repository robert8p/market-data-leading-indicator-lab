-- Expand the canonical cross-venue feature family before the first statistical
-- screen. Preserve the existing view column order and append new columns so
-- dependent functions remain valid. Previously completed canaries are requeued
-- so every symbol uses the identical frozen definition.

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
  where x.ts>=timestamptz '2026-06-28 00:00:00+00' and x.ts<timestamptz '2026-07-28 16:53:00+00'
),w as (
  select b.*,
    lag(ts,1) over(partition by symbol order by ts) lag_ts_1,lag(ts,2) over(partition by symbol order by ts) lag_ts_2,
    lag(ts,5) over(partition by symbol order by ts) lag_ts_5,lag(ts,10) over(partition by symbol order by ts) lag_ts_10,
    lag(ts,15) over(partition by symbol order by ts) lag_ts_15,lag(ts,30) over(partition by symbol order by ts) lag_ts_30,
    lag(b_close,2) over(partition by symbol order by ts) lag_b_close_2,lag(c_close,2) over(partition by symbol order by ts) lag_c_close_2,
    lag(b_close,5) over(partition by symbol order by ts) lag_b_close_5,lag(c_close,5) over(partition by symbol order by ts) lag_c_close_5,
    lag(b_close,10) over(partition by symbol order by ts) lag_b_close_10,lag(c_close,10) over(partition by symbol order by ts) lag_c_close_10,
    lag(b_close,15) over(partition by symbol order by ts) lag_b_close_15,lag(c_close,15) over(partition by symbol order by ts) lag_c_close_15,
    lag(b_close,30) over(partition by symbol order by ts) lag_b_close_30,lag(c_close,30) over(partition by symbol order by ts) lag_c_close_30,
    lag(price_log_gap_bc,1) over(partition by symbol order by ts) lag_price_log_gap_1,
    lag(b_ret_1m,1) over(partition by symbol order by ts) lag_b_ret_1,lag(c_ret_1m,1) over(partition by symbol order by ts) lag_c_ret_1,
    lag(b_ret_1m,2) over(partition by symbol order by ts) lag_b_ret_2,lag(c_ret_1m,2) over(partition by symbol order by ts) lag_c_ret_2,
    lag(b_ret_1m,5) over(partition by symbol order by ts) lag_b_ret_5,lag(c_ret_1m,5) over(partition by symbol order by ts) lag_c_ret_5,
    avg(b_quote_volume) over(partition by symbol order by ts rows between 5 preceding and 1 preceding) avg_b_qv_prev5,
    avg(c_quote_volume) over(partition by symbol order by ts rows between 5 preceding and 1 preceding) avg_c_qv_prev5
  from base b
)
select symbol,ts source_ts,ts+interval '1 minute' decision_ts,ts+interval '1 minute' observable_at,
 b_ret_1m,c_ret_1m,case when b_ret_1m is not null and c_ret_1m is not null then b_ret_1m-c_ret_1m end return_gap_1m_bc,
 price_log_gap_bc,case when lag_ts_1=ts-interval '1 minute' then price_log_gap_bc-lag_price_log_gap_1 end price_gap_change_1m_bc,
 quote_volume_log_gap_bc,case when b_range_1m is not null and c_range_1m is not null then b_range_1m-c_range_1m end range_gap_1m_bc,
 case when lag_ts_5=ts-interval '5 minutes' and lag_b_close_5>0 then b_close/lag_b_close_5-1 end b_ret_5m,
 case when lag_ts_5=ts-interval '5 minutes' and lag_c_close_5>0 then c_close/lag_c_close_5-1 end c_ret_5m,
 case when lag_ts_5=ts-interval '5 minutes' and lag_b_close_5>0 and lag_c_close_5>0 then (b_close/lag_b_close_5-1)-(c_close/lag_c_close_5-1) end return_gap_5m_bc,
 case when lag_ts_5=ts-interval '5 minutes' and avg_b_qv_prev5>=0 then ln((1+greatest(coalesce(b_quote_volume,0),0))/(1+avg_b_qv_prev5)) end b_volume_shock_5m,
 case when lag_ts_5=ts-interval '5 minutes' and avg_c_qv_prev5>=0 then ln((1+greatest(coalesce(c_quote_volume,0),0))/(1+avg_c_qv_prev5)) end c_volume_shock_5m,
 case when lag_ts_5=ts-interval '5 minutes' and avg_b_qv_prev5>=0 and avg_c_qv_prev5>=0 then ln((1+greatest(coalesce(b_quote_volume,0),0))/(1+avg_b_qv_prev5))-ln((1+greatest(coalesce(c_quote_volume,0),0))/(1+avg_c_qv_prev5)) end volume_shock_gap_5m_bc,
 case when lag_ts_1=ts-interval '1 minute' and lag_b_ret_1 is not null and lag_c_ret_1 is not null then lag_b_ret_1-lag_c_ret_1 end return_gap_lag1m_bc,
 b_open,b_close,c_open,c_close,b_quote_volume,c_quote_volume,
 case when lag_ts_2=ts-interval '2 minutes' and lag_b_close_2>0 then b_close/lag_b_close_2-1 end b_ret_2m,
 case when lag_ts_2=ts-interval '2 minutes' and lag_c_close_2>0 then c_close/lag_c_close_2-1 end c_ret_2m,
 case when lag_ts_2=ts-interval '2 minutes' and lag_b_close_2>0 and lag_c_close_2>0 then (b_close/lag_b_close_2-1)-(c_close/lag_c_close_2-1) end return_gap_2m_bc,
 case when lag_ts_10=ts-interval '10 minutes' and lag_b_close_10>0 then b_close/lag_b_close_10-1 end b_ret_10m,
 case when lag_ts_10=ts-interval '10 minutes' and lag_c_close_10>0 then c_close/lag_c_close_10-1 end c_ret_10m,
 case when lag_ts_10=ts-interval '10 minutes' and lag_b_close_10>0 and lag_c_close_10>0 then (b_close/lag_b_close_10-1)-(c_close/lag_c_close_10-1) end return_gap_10m_bc,
 case when lag_ts_15=ts-interval '15 minutes' and lag_b_close_15>0 then b_close/lag_b_close_15-1 end b_ret_15m,
 case when lag_ts_15=ts-interval '15 minutes' and lag_c_close_15>0 then c_close/lag_c_close_15-1 end c_ret_15m,
 case when lag_ts_15=ts-interval '15 minutes' and lag_b_close_15>0 and lag_c_close_15>0 then (b_close/lag_b_close_15-1)-(c_close/lag_c_close_15-1) end return_gap_15m_bc,
 case when lag_ts_30=ts-interval '30 minutes' and lag_b_close_30>0 then b_close/lag_b_close_30-1 end b_ret_30m,
 case when lag_ts_30=ts-interval '30 minutes' and lag_c_close_30>0 then c_close/lag_c_close_30-1 end c_ret_30m,
 case when lag_ts_30=ts-interval '30 minutes' and lag_b_close_30>0 and lag_c_close_30>0 then (b_close/lag_b_close_30-1)-(c_close/lag_c_close_30-1) end return_gap_30m_bc,
 case when lag_ts_2=ts-interval '2 minutes' and lag_b_ret_2 is not null and lag_c_ret_2 is not null then lag_b_ret_2-lag_c_ret_2 end return_gap_lag2m_bc,
 case when lag_ts_5=ts-interval '5 minutes' and lag_b_ret_5 is not null and lag_c_ret_5 is not null then lag_b_ret_5-lag_c_ret_5 end return_gap_lag5m_bc
from w;

update research_hub.feature_sets set
 feature_keys=array['cv.b_ret_1m','cv.c_ret_1m','cv.return_gap_1m_bc','cv.price_log_gap_bc','cv.price_gap_change_1m_bc','cv.quote_volume_log_gap_bc','cv.range_gap_1m_bc','cv.b_ret_2m','cv.c_ret_2m','cv.return_gap_2m_bc','cv.b_ret_5m','cv.c_ret_5m','cv.return_gap_5m_bc','cv.b_ret_10m','cv.c_ret_10m','cv.return_gap_10m_bc','cv.b_ret_15m','cv.c_ret_15m','cv.return_gap_15m_bc','cv.b_ret_30m','cv.c_ret_30m','cv.return_gap_30m_bc','cv.b_volume_shock_5m','cv.c_volume_shock_5m','cv.volume_shock_gap_5m_bc','cv.return_gap_lag1m_bc','cv.return_gap_lag2m_bc','cv.return_gap_lag5m_bc'],
 verification_notes='Source bar t is observable only at t+60s. 2/5/10/15/30m rolling returns require exact wall-clock continuity; lag-decay controls use only prior completed bars. Feature family frozen before first statistical screen.',
 metadata=coalesce(metadata,'{}'::jsonb)||jsonb_build_object('definition_version','crypto-crossvenue-sync-v1.1','frozen_lag_family_minutes',jsonb_build_array(1,2,5,10,15,30),'placebo_controls_required',jsonb_build_array('time_shuffle','symbol_permutation','reverse_venue_role'),'feature_family_frozen_before_screen',true),updated_at=now()
where feature_set_key='crypto.crossvenue.sync.v1';

insert into research_hub.feature_definitions(feature_key,dataset_key,feature_name,feature_family,source_expression,decision_time_rule,observable_at_rule,lookback_seconds,uses_future_data,enabled,metadata)
values
('cv.b_ret_2m','primary.crypto_crossvenue_sync_1m_v1','Binance trailing 2m return','crossvenue_return','b_close/lag(b_close,2)-1','decision_ts=source_ts+60s','contiguous completed bars only',120,false,true,'{}'),
('cv.c_ret_2m','primary.crypto_crossvenue_sync_1m_v1','Coinbase trailing 2m return','crossvenue_return','c_close/lag(c_close,2)-1','decision_ts=source_ts+60s','contiguous completed bars only',120,false,true,'{}'),
('cv.return_gap_2m_bc','primary.crypto_crossvenue_sync_1m_v1','Binance minus Coinbase trailing 2m return gap','crossvenue_divergence','b_ret_2m-c_ret_2m','decision_ts=source_ts+60s','contiguous completed bars only',120,false,true,'{}'),
('cv.b_ret_10m','primary.crypto_crossvenue_sync_1m_v1','Binance trailing 10m return','crossvenue_return','b_close/lag(b_close,10)-1','decision_ts=source_ts+60s','contiguous completed bars only',600,false,true,'{}'),
('cv.c_ret_10m','primary.crypto_crossvenue_sync_1m_v1','Coinbase trailing 10m return','crossvenue_return','c_close/lag(c_close,10)-1','decision_ts=source_ts+60s','contiguous completed bars only',600,false,true,'{}'),
('cv.return_gap_10m_bc','primary.crypto_crossvenue_sync_1m_v1','Binance minus Coinbase trailing 10m return gap','crossvenue_divergence','b_ret_10m-c_ret_10m','decision_ts=source_ts+60s','contiguous completed bars only',600,false,true,'{}'),
('cv.b_ret_15m','primary.crypto_crossvenue_sync_1m_v1','Binance trailing 15m return','crossvenue_return','b_close/lag(b_close,15)-1','decision_ts=source_ts+60s','contiguous completed bars only',900,false,true,'{}'),
('cv.c_ret_15m','primary.crypto_crossvenue_sync_1m_v1','Coinbase trailing 15m return','crossvenue_return','c_close/lag(c_close,15)-1','decision_ts=source_ts+60s','contiguous completed bars only',900,false,true,'{}'),
('cv.return_gap_15m_bc','primary.crypto_crossvenue_sync_1m_v1','Binance minus Coinbase trailing 15m return gap','crossvenue_divergence','b_ret_15m-c_ret_15m','decision_ts=source_ts+60s','contiguous completed bars only',900,false,true,'{}'),
('cv.b_ret_30m','primary.crypto_crossvenue_sync_1m_v1','Binance trailing 30m return','crossvenue_return','b_close/lag(b_close,30)-1','decision_ts=source_ts+60s','contiguous completed bars only',1800,false,true,'{}'),
('cv.c_ret_30m','primary.crypto_crossvenue_sync_1m_v1','Coinbase trailing 30m return','crossvenue_return','c_close/lag(c_close,30)-1','decision_ts=source_ts+60s','contiguous completed bars only',1800,false,true,'{}'),
('cv.return_gap_30m_bc','primary.crypto_crossvenue_sync_1m_v1','Binance minus Coinbase trailing 30m return gap','crossvenue_divergence','b_ret_30m-c_ret_30m','decision_ts=source_ts+60s','contiguous completed bars only',1800,false,true,'{}'),
('cv.return_gap_lag2m_bc','primary.crypto_crossvenue_sync_1m_v1','Two-minute-old Binance minus Coinbase 1m return gap','crossvenue_lag_control','lag(b_ret_1m-c_ret_1m,2)','decision_ts=source_ts+60s','prior contiguous completed bar only',180,false,true,jsonb_build_object('role','decay_control')),
('cv.return_gap_lag5m_bc','primary.crypto_crossvenue_sync_1m_v1','Five-minute-old Binance minus Coinbase 1m return gap','crossvenue_lag_control','lag(b_ret_1m-c_ret_1m,5)','decision_ts=source_ts+60s','prior contiguous completed bar only',360,false,true,jsonb_build_object('role','decay_control'))
on conflict(feature_key) do update set dataset_key=excluded.dataset_key,feature_name=excluded.feature_name,feature_family=excluded.feature_family,source_expression=excluded.source_expression,decision_time_rule=excluded.decision_time_rule,observable_at_rule=excluded.observable_at_rule,lookback_seconds=excluded.lookback_seconds,uses_future_data=false,enabled=true,metadata=excluded.metadata,updated_at=now();

-- Rebuild the materializer with the frozen v1.1 feature payload.
create or replace function research_hub.materialize_crypto_crossvenue_symbol_v1(p_symbol text)
returns jsonb language plpgsql security invoker set search_path=research_hub,public,extensions,pg_temp as $$
declare v_features bigint:=0; v_outcomes bigint:=0;
begin
 insert into research_hub.feature_rows(feature_set_key,instrument_key,decision_ts,observable_at,features,source_dataset_key,source_row_hash,quality)
 select 'crypto.crossvenue.sync.v1','cv:'||o.symbol,o.decision_ts,o.observable_at,
 jsonb_strip_nulls(jsonb_build_object('cv.b_ret_1m',o.b_ret_1m,'cv.c_ret_1m',o.c_ret_1m,'cv.return_gap_1m_bc',o.return_gap_1m_bc,'cv.price_log_gap_bc',o.price_log_gap_bc,'cv.price_gap_change_1m_bc',o.price_gap_change_1m_bc,'cv.quote_volume_log_gap_bc',o.quote_volume_log_gap_bc,'cv.range_gap_1m_bc',o.range_gap_1m_bc,'cv.b_ret_2m',o.b_ret_2m,'cv.c_ret_2m',o.c_ret_2m,'cv.return_gap_2m_bc',o.return_gap_2m_bc,'cv.b_ret_5m',o.b_ret_5m,'cv.c_ret_5m',o.c_ret_5m,'cv.return_gap_5m_bc',o.return_gap_5m_bc,'cv.b_ret_10m',o.b_ret_10m,'cv.c_ret_10m',o.c_ret_10m,'cv.return_gap_10m_bc',o.return_gap_10m_bc,'cv.b_ret_15m',o.b_ret_15m,'cv.c_ret_15m',o.c_ret_15m,'cv.return_gap_15m_bc',o.return_gap_15m_bc,'cv.b_ret_30m',o.b_ret_30m,'cv.c_ret_30m',o.c_ret_30m,'cv.return_gap_30m_bc',o.return_gap_30m_bc,'cv.b_volume_shock_5m',o.b_volume_shock_5m,'cv.c_volume_shock_5m',o.c_volume_shock_5m,'cv.volume_shock_gap_5m_bc',o.volume_shock_gap_5m_bc,'cv.return_gap_lag1m_bc',o.return_gap_lag1m_bc,'cv.return_gap_lag2m_bc',o.return_gap_lag2m_bc,'cv.return_gap_lag5m_bc',o.return_gap_lag5m_bc)),
 'primary.crypto_crossvenue_sync_1m_v1',encode(digest(concat_ws('|',o.symbol,o.source_ts,o.b_open,o.b_close,o.c_open,o.c_close,o.b_quote_volume,o.c_quote_volume),'sha256'),'hex'),
 jsonb_build_object('legacy_run_id',o.symbol,'adaptive_reuse',true,'promotion_requires_future_replication',true,'legacy_log_ratio_ignored',true,'feature_definition_version','crypto-crossvenue-sync-v1.1')
 from research_hub.crypto_crossvenue_observations_v1 o where o.symbol=p_symbol
 on conflict(feature_set_key,instrument_key,decision_ts) do update set observable_at=excluded.observable_at,features=excluded.features,source_dataset_key=excluded.source_dataset_key,source_row_hash=excluded.source_row_hash,quality=excluded.quality;
 get diagnostics v_features=row_count;
 insert into research_hub.outcome_rows(outcome_set_key,instrument_key,decision_ts,horizon_seconds,entry_ts,exit_ts,gross_return,metadata)
 with d as(select * from research_hub.crypto_crossvenue_observations_v1 where symbol=p_symbol),h(horizon_seconds) as(values(60),(300),(900)),p as(select d.symbol,d.source_ts,d.decision_ts,h.horizon_seconds,e.b_open b_entry,e.c_open c_entry,z.b_close b_exit,z.c_close c_exit from d cross join h join public.crypto_research_crossvenue_1m e on e.symbol=d.symbol and e.ts=d.source_ts+interval '1 minute' join public.crypto_research_crossvenue_1m z on z.symbol=d.symbol and z.ts=d.source_ts+make_interval(secs=>h.horizon_seconds))
 select 'crypto.crossvenue.nextopen.v1','binance:'||symbol,decision_ts,horizon_seconds,decision_ts,source_ts+make_interval(secs=>horizon_seconds)+interval '1 minute',case when b_entry>0 then b_exit/b_entry-1 end,jsonb_build_object('legacy_run_id',symbol,'venue','binance','entry_rule','next_open','costs_embedded',false,'adaptive_reuse',true,'promotion_requires_future_replication',true) from p
 union all select 'crypto.crossvenue.nextopen.v1','coinbase:'||symbol,decision_ts,horizon_seconds,decision_ts,source_ts+make_interval(secs=>horizon_seconds)+interval '1 minute',case when c_entry>0 then c_exit/c_entry-1 end,jsonb_build_object('legacy_run_id',symbol,'venue','coinbase','entry_rule','next_open','costs_embedded',false,'adaptive_reuse',true,'promotion_requires_future_replication',true) from p
 on conflict(outcome_set_key,instrument_key,decision_ts,horizon_seconds) do update set entry_ts=excluded.entry_ts,exit_ts=excluded.exit_ts,gross_return=excluded.gross_return,metadata=excluded.metadata;
 get diagnostics v_outcomes=row_count;
 return jsonb_build_object('symbol',p_symbol,'feature_rows',v_features,'outcome_rows',v_outcomes,'holdout_accessed',false,'feature_definition_version','crypto-crossvenue-sync-v1.1');
end $$;

update research_hub.feature_materialization_checkpoints set status='queued',row_count=null,last_source_ts=null,last_error=null,metadata=(coalesce(metadata,'{}'::jsonb)-'outcome_rows'-'canary_verified')||jsonb_build_object('rematerialize_reason','feature family expanded/frozen before first screen','definition_version','crypto-crossvenue-sync-v1.1'),updated_at=now() where feature_set_key='crypto.crossvenue.sync.v1' and status='completed';
update research_hub.program_jobs set current_state='materializing_canonical_feature_outcomes',progress_current=0,progress_total=20,completion_pct=0,current_error=null,retry_state='automatic rematerialisation after frozen lag-family expansion',next_automatic_action='Materialise all 20 symbols under frozen feature definition crypto-crossvenue-sync-v1.1. Only after all checkpoints complete may experiment tasks be created. Placebo family is frozen before screening.',metadata=coalesce(metadata,'{}'::jsonb)||jsonb_build_object('feature_definition_version','crypto-crossvenue-sync-v1.1','frozen_lag_family_minutes',jsonb_build_array(1,2,5,10,15,30),'placebo_controls',jsonb_build_array('time_shuffle','symbol_permutation','reverse_venue_role')),updated_at=now() where job_key='FEATURE-CROSSVENUE-LAG-V1';