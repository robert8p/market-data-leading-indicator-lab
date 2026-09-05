-- Historical Binance futures book snapshots before the fixed partial-depth state
-- handling are not promotion-grade. Start a conservative prospective clean-book
-- source after the verified cutover and block automated discovery until enough
-- independent clean history accumulates.

create or replace view research_hub.crypto_microstructure_valid_book_postfix_v1
with (security_invoker=true) as
select m.provider,m.market_type,m.venue_symbol,m.canonical_symbol,m.ts,
       m.ts+interval '1 second' decision_ts,m.ts+interval '1 second' observable_at,
       m.trade_count,m.buy_count,m.sell_count,m.buy_base_volume,m.sell_base_volume,m.buy_quote_volume,m.sell_quote_volume,m.last_trade_price,
       m.bid_price,m.bid_size,m.ask_price,m.ask_size,(m.ask_price-m.bid_price) spread_clean,
       case when (m.bid_price+m.ask_price)>0 then (m.ask_price-m.bid_price)/((m.bid_price+m.ask_price)/2.0)*10000.0 end spread_bps_clean,
       (m.bid_price+m.ask_price)/2.0 mid_price_clean,
       case when coalesce(m.bid_size,0)+coalesce(m.ask_size,0)>0 then (coalesce(m.bid_size,0)-coalesce(m.ask_size,0))/(coalesce(m.bid_size,0)+coalesce(m.ask_size,0)) end top_size_imbalance_clean,
       case when coalesce(m.bid_size,0)+coalesce(m.ask_size,0)>0 then (m.ask_price*coalesce(m.bid_size,0)+m.bid_price*coalesce(m.ask_size,0))/(coalesce(m.bid_size,0)+coalesce(m.ask_size,0)) end microprice_clean,
       m.bid_depth,m.ask_depth,
       case when coalesce(m.bid_depth,0)+coalesce(m.ask_depth,0)>0 then (coalesce(m.bid_depth,0)-coalesce(m.ask_depth,0))/(coalesce(m.bid_depth,0)+coalesce(m.ask_depth,0)) end depth_imbalance_clean,
       m.book_update_count,m.mark_price,m.index_price,m.funding_rate,m.next_funding_at,m.open_interest,m.open_interest_value,m.liquidation_buy_notional,m.liquidation_sell_notional
from public.crypto_microstructure_1s m
where m.ts>=timestamptz '2026-08-11 23:30:00+00'
  and m.bid_price>0 and m.ask_price>0 and m.ask_price>=m.bid_price;

insert into research_hub.datasets(dataset_key,store_key,schema_name,relation_name,asset_class,provider,frequency,grain,ts_column,instrument_column,observable_at_column,is_raw,point_in_time_safe,coverage_start,coverage_end,row_estimate,status,metadata)
values('primary.crypto_microstructure_valid_book_postfix_v1','market_data_primary','research_hub','crypto_microstructure_valid_book_postfix_v1','crypto','multi','1s','venue-symbol-second','decision_ts','canonical_symbol','observable_at',false,true,timestamptz '2026-08-11 23:30:01+00',null,1,'prospective_accumulating',jsonb_build_object('source_dataset_key','primary.crypto_microstructure_1s','collector_fix','Binance futures partial depth messages treated as complete top-N snapshots; crossed books discarded','stored_spread_ignored',true,'clean_spread_formula','(ask-bid)/mid * 10000','cutover_utc','2026-08-11T23:30:00Z','row_estimate_is_sentinel',true,'minimum_clean_history','7 full UTC days before automated discovery'))
on conflict(dataset_key) do update set relation_name=excluded.relation_name,point_in_time_safe=true,coverage_start=excluded.coverage_start,status=excluded.status,metadata=excluded.metadata,updated_at=now();

insert into research_hub.data_quality_issues(dataset_key,severity,issue_type,range_start,range_end,details)
select 'primary.crypto_microstructure_valid_book_postfix_v1','warning','insufficient_postfix_history',timestamptz '2026-08-11 23:30:00+00',null,jsonb_build_object('unlock_rule','Accumulate at least 7 full UTC days after fixed-book cutover and re-audit crossed/stale books, continuity and provider/symbol coverage before automated discovery.','user_action_required',false)
where not exists(select 1 from research_hub.data_quality_issues where dataset_key='primary.crypto_microstructure_valid_book_postfix_v1' and issue_type='insufficient_postfix_history' and resolved_at is null);

insert into research_hub.program_jobs(job_key,exact_name,purpose,store_key,source_schema,source_table,source_id,job_kind,current_state,started_at,latest_successful_checkpoint,progress_current,progress_total,completion_pct,latest_result,current_error,retry_state,next_automatic_action,intervention_required,exact_intervention,frozen_rule,holdout_sensitive,metadata)
values('SOURCE-CRYPTO-MICRO-POSTFIX-V1','Clean post-fix crypto microstructure history — v1','Accumulate prospective high-resolution crypto book/trade history after the Binance partial-depth state fix, with book metrics recomputed from valid bid/ask snapshots.','market_data_primary','research_hub','crypto_microstructure_valid_book_postfix_v1','postfix-cutover-2026-08-11T23:30Z','data_quality_accumulation','accumulating_prospective',now(),now(),0,7,0,jsonb_build_object('cutover_utc','2026-08-11T23:30:00Z','crossed_book_sample_postfix',0,'sample_symbols',jsonb_build_array('ARB','IOTX','PENGU')),null,'automatic accumulation','After seven full UTC days, audit crossed/stale books, continuity and symbol/provider coverage; if quality passes, resolve the temporary warning and mark source completed_quality_ready.',false,null,true,true,jsonb_build_object('minimum_clean_days',7,'user_action_required',false,'historical_pre_cutover_rows_quarantined',true))
on conflict(job_key) do update set current_state=excluded.current_state,latest_successful_checkpoint=excluded.latest_successful_checkpoint,latest_result=excluded.latest_result,current_error=null,retry_state=excluded.retry_state,next_automatic_action=excluded.next_automatic_action,metadata=excluded.metadata,updated_at=now();

delete from research_hub.job_dependencies where job_key='FEATURE-CRYPTO-MICRO-V1' and depends_on_job_key='MDM-30D-COLLECTION';
insert into research_hub.job_dependencies(job_key,depends_on_job_key,dependency_type,required_state,satisfied,metadata)
values('FEATURE-CRYPTO-MICRO-V1','SOURCE-CRYPTO-MICRO-POSTFIX-V1','quality_ready','completed_quality_ready',false,jsonb_build_object('minimum_requirement','Seven full UTC days of post-fix valid-book history plus clean continuity/crossed-book re-audit. Full MDM completion is not the gating condition.'))
on conflict(job_key,depends_on_job_key,dependency_type) do update set required_state=excluded.required_state,satisfied=false,metadata=excluded.metadata,updated_at=now();

update research_hub.program_jobs set current_state='queued_waiting_clean_postfix_history',progress_current=0,progress_total=7,completion_pct=0,retry_state='automatic prospective accumulation',next_automatic_action='Wait for SOURCE-CRYPTO-MICRO-POSTFIX-V1 to pass its seven-day fixed-book quality gate; then create the point-in-time microstructure feature/event set. Historical pre-fix spread/depth rows remain quarantined.',current_error=null,metadata=coalesce(metadata,'{}'::jsonb)||jsonb_build_object('clean_source_job','SOURCE-CRYPTO-MICRO-POSTFIX-V1','historical_book_rows_quarantined',true),updated_at=now()
where job_key='FEATURE-CRYPTO-MICRO-V1';