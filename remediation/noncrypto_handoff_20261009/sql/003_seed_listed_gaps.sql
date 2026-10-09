SET statement_timeout='120s';
SET lock_timeout='5s';
WITH calendar AS MATERIALIZED (
 SELECT session_date,row_number() over(order by session_date) i
 FROM market_governance.us_equity_fixed_session_calendar
), gaps AS MATERIALIZED (
 SELECT v.*,c.i-row_number() OVER(PARTITION BY v.native_ticker,v.instrument_key,date_trunc('month',v.session_date),v.coverage_state ORDER BY v.session_date) island
 FROM market_governance.remediation_listed_coverage_day_v1 v JOIN calendar c USING(session_date)
 WHERE v.coverage_state IN('NO_RETAINED_ROLLUP_ROW_REQUIRES_SOURCE_CHECK','NATIVE_ALIAS_COLLISION_PRICE_ROUTE_UNRESOLVED')
), grouped AS (
 SELECT native_ticker,instrument_key,date_trunc('month',session_date) AS bucket_month,island,coverage_state,
 min(session_date) first_date,max(session_date) last_date,
 jsonb_agg(jsonb_build_object('date',session_date,'observation_id',observation_id,'native_row_sha256',native_row_sha256,'native_source_sha256',native_source_sha256) ORDER BY session_date) days,
 encode(sha256(convert_to(string_agg(observation_id::text||':'||session_date::text||':'||native_row_sha256||':'||native_source_sha256,'|' ORDER BY session_date),'UTF8')),'hex') evidence_sha
 FROM gaps GROUP BY native_ticker,instrument_key,date_trunc('month',session_date),island,coverage_state
), inserted AS (
 INSERT INTO market_governance.remediation_source_batch_v1
 (run_id,batch_key,finding_ids,provider,source_type,symbol,instrument_id,start_ts,end_ts,interval_seconds,request_json,status,priority)
 SELECT 'market_data_remediation_20261008_v1','listed:gaps:20261009:'||evidence_sha,ARRAY['RT-01','IP-04'],'massive','massive_candles',native_ticker,NULL,
 first_date::timestamp AT TIME ZONE 'UTC',(last_date+1)::timestamp AT TIME ZONE 'UTC',60,
 jsonb_build_object('noncrypto_scope','LISTED_EQUITY_GAPS_20261009_V1','asset_class','stocks',
 'required_parser_version','ohlc_source_rules_20261008_v3_native_symbol_quote_contract',
 'affected_listing_days',days,'coverage_evidence_sha256',evidence_sha,'original_coverage_state',coverage_state,
 'candidate_instrument_key',instrument_key,'candidate_mapping_is_not_canonical_promotion',true,
 'price_status','INCLUDED_NO_INCREMENTAL_CHARGE',
 'price_evidence','Existing Massive Starter stocks REST entitlement; no subscription, paid activation, compute-plan or instance-count change. Existing provider spacing retained.',
 'license_evidence','User-authorized internal market-data remediation; native symbol and actual receipt retained privately.',
 'minimum_source_request_spacing_seconds',1.2,'acquisition_mode','FETCH',
 'strict_replay_eligible',false,'historical_first_receipt_recovered',false,
 'empty_response_does_not_establish_no_trade_or_irrecoverability',true,
 'next_stage','INDEPENDENT_IDENTITY_COVERAGE_VALIDATION_THEN_CANONICAL_PROMOTION'),
 'QUEUED',CASE WHEN coverage_state='NATIVE_ALIAS_COLLISION_PRICE_ROUTE_UNRESOLVED' THEN 10000 ELSE 100 END
 FROM grouped
 ON CONFLICT(run_id,batch_key) DO NOTHING RETURNING batch_id,request_json,source_type,symbol
)
SELECT count(*) inserted_tasks,sum(jsonb_array_length(request_json->'affected_listing_days')) listing_days,
 count(*) FILTER(WHERE request_json->>'original_coverage_state'='NATIVE_ALIAS_COLLISION_PRICE_ROUTE_UNRESOLVED') alias_tasks
FROM inserted;
