DO $test$ DECLARE tid uuid;aid uuid;tok text;h text;r jsonb;n int:=0;total_new bigint:=0; BEGIN
h:=encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.retained_price_route_v2(uuid,text)'::regprocedure),'UTF8')),'hex');
UPDATE population_top3000_202608_202609_v1.pipeline_adapter SET function_oid='population_top3000_202608_202609_v1.retained_price_route_v2(uuid,text)'::regprocedure,function_sha256=h WHERE family_id='P02' AND stage='NORMALIZE';
FOREACH tid IN ARRAY ARRAY['66bac9d2-ecec-438e-b826-845f09488ff8'::uuid,'367e70f4-ed0f-4f2b-b80c-8f038cf08d31'::uuid] LOOP
aid:=gen_random_uuid();tok:=gen_random_uuid()::text;
INSERT INTO population_top3000_202608_202609_v1.pipeline_attempt(attempt_id,task_id,execution_number,token_sha256,owner_fence,stop_generation,adapter_sha256) VALUES(aid,tid,999999,encode(sha256(convert_to(tok,'UTF8')),'hex'),3,1,h);
UPDATE population_top3000_202608_202609_v1.pipeline_task SET status='LEASED',attempt_id=aid,lease_until=clock_timestamp()+interval '2 minutes',owner_fence=3,stop_generation=1,payload=payload||'{"normalization_route":"RETAINED_PRICE_NORMALIZE_V2"}'::jsonb WHERE task_id=tid;
r:=population_top3000_202608_202609_v1.retained_price_route_v2(aid,tok);
IF r->>'state'<>'COMPLETE' THEN RAISE EXCEPTION 'POSITIVE_FAILED';END IF;n:=n+1;total_new:=total_new+(r#>>'{commit_receipt,new_committed_units}')::bigint;
r:=population_top3000_202608_202609_v1.retained_price_route_v2(aid,tok);
IF r#>>'{commit_receipt,new_committed_units}'<>'0' THEN RAISE EXCEPTION 'REPLAY_NOT_IDEMPOTENT';END IF;n:=n+1;
END LOOP;
IF total_new<>642 OR (SELECT count(*) FROM population_top3000_202608_202609_v1.working_retained_price_observations_v2)<>642 THEN RAISE EXCEPTION 'CONSUMER_COUNT_FAILED';END IF;n:=n+1;
IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.retained_price_observation_v2 o JOIN market_fixed_20250901_20260831.alpaca_sip_bars_1m_202608 b ON b.instrument_key=o.instrument_key AND b.bar_ts=o.bar_ts WHERE float8send(o.open)<>float8send(b.open) OR float8send(o.high)<>float8send(b.high) OR float8send(o.low)<>float8send(b.low) OR float8send(o.close)<>float8send(b.close) OR float8send(o.vwap)<>float8send(b.vwap) OR o.volume<>b.volume OR o.trade_count<>b.trade_count) THEN RAISE EXCEPTION 'BINARY_VALUE_MISMATCH';END IF;n:=n+1;
IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.retained_price_expectation_v2 WHERE expected_minute_bars IS NOT NULL OR expected_partition_units<>1) THEN RAISE EXCEPTION 'DENSE_CLOCK_EXPECTATION';END IF;n:=n+1;
BEGIN UPDATE population_top3000_202608_202609_v1.pipeline_task SET lease_until=clock_timestamp()-interval '1 second' WHERE task_id=tid;PERFORM population_top3000_202608_202609_v1.retained_price_route_v2(aid,tok);RAISE EXCEPTION 'EXPIRED_ACCEPTED';EXCEPTION WHEN raise_exception THEN IF SQLERRM<>'LEASE_LOST' THEN RAISE;END IF;n:=n+1;END;
BEGIN UPDATE population_top3000_202608_202609_v1.acquisition_control SET stopped=true WHERE singleton;PERFORM population_top3000_202608_202609_v1.retained_price_route_v2(aid,tok);RAISE EXCEPTION 'STOP_ACCEPTED';EXCEPTION WHEN raise_exception THEN IF SQLERRM<>'MISSION_HELD_OR_ADMISSION_EXPIRED' THEN RAISE;END IF;n:=n+1;END;
BEGIN UPDATE population_top3000_202608_202609_v1.retained_price_partition_v1 SET source_content_sha256=repeat('0',64) WHERE instrument_key=7751 AND session_date='2026-08-04';PERFORM population_top3000_202608_202609_v1.retained_price_route_v2(aid,tok);RAISE EXCEPTION 'SOURCE_CHANGE_ACCEPTED';EXCEPTION WHEN raise_exception THEN IF SQLERRM<>'SOURCE_CONTENT_CHANGED' THEN RAISE;END IF;n:=n+1;END;
BEGIN UPDATE population_top3000_202608_202609_v1.retained_price_observation_v2 SET normalization_sha256=repeat('0',64) WHERE session_date='2026-08-04';PERFORM population_top3000_202608_202609_v1.retained_price_route_v2(aid,tok);RAISE EXCEPTION 'CONFLICT_ACCEPTED';EXCEPTION WHEN raise_exception THEN IF SQLERRM<>'NORMALIZATION_VERSION_CONFLICT' THEN RAISE;END IF;n:=n+1;END;
BEGIN
UPDATE population_top3000_202608_202609_v1.pipeline_task SET payload=payload-'normalization_route' WHERE task_id=tid;
r:=population_top3000_202608_202609_v1.retained_price_route_v2(aid,tok);
IF r->>'blocker_code'<>'RETAINED_PRICE_COMPANY_IDENTITY_AND_EXPECTATION_REQUIRED' THEN RAISE EXCEPTION 'LEGACY_ROUTE_CHANGED';END IF;n:=n+1;
RAISE EXCEPTION 'ROLLBACK_ROUTE_FIXTURE';EXCEPTION WHEN raise_exception THEN IF SQLERRM<>'ROLLBACK_ROUTE_FIXTURE' THEN RAISE;END IF;END;
CREATE TEMP TABLE price_proof(proof jsonb) ON COMMIT DROP;
INSERT INTO price_proof VALUES(jsonb_build_object('checks',n,'all_pass',true,'normalized_rows',total_new,'function_sha256',h,'identity_method','PRESERVED_LOADER_SYMBOL_JOIN_AND_INDEPENDENT_DATED_COMPANY_POINT','expected_partition_units',2,'expected_minute_bars',NULL,'observed_dates',(SELECT jsonb_agg(jsonb_build_object('date',session_date,'rows',observed_bars)) FROM population_top3000_202608_202609_v1.retained_price_expectation_v2),'consumer_hash',(SELECT encode(sha256(convert_to(string_agg(source_row_sha256,chr(10) ORDER BY bar_ts),'UTF8')),'hex') FROM population_top3000_202608_202609_v1.retained_price_observation_v2),'source_completeness_verified',false,'family_accepted',false));
END $test$;