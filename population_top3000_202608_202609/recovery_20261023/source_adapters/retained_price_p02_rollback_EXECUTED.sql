-- Actual AAPL41/Aug3 premarket and postmarket retained rows, real dispatcher, full ROLLBACK.
-- Root pauses cron596 inside or before this transaction and reconciles all active leases first.
BEGIN;
SET LOCAL statement_timeout='5s';
SET LOCAL lock_timeout='500ms';
SET LOCAL TimeZone='UTC';
SELECT cron.alter_job(596,active:=false);
SELECT pg_advisory_xact_lock(2026083000261022);
DO $test$
DECLARE v_cal population_top3000_202608_202609_v1.catalog_calendar%rowtype;
 v_map jsonb;v_inputs jsonb;v_payload jsonb;v_hash text;v_partition text;v_session text;
 v_task uuid;v_normalize uuid;v_result jsonb;v_manifest population_top3000_202608_202609_v1.retained_price_partition_v1%rowtype;
 v_provider bigint;v_http bigint;v_stored bigint;v_source_count bigint;v_count_before bigint;v_observations bigint;v_expected bigint;
 v_started timestamptz;v_elapsed interval;v_session_start timestamptz;v_session_end timestamptz;v_results jsonb:='[]';
BEGIN
 IF EXISTS(SELECT 1 FROM cron.job WHERE jobid=596 AND active) THEN RAISE EXCEPTION 'PAUSE_CRON_FIRST';END IF;
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE status='LEASED')
  OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_provider_lease WHERE released_at IS NULL)
 THEN RAISE EXCEPTION 'RECONCILE_LIVE_LEASES_FIRST';END IF;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton
  AND owner_id='top3000-recovery-20261023-root-4b1314bcb5c2' AND owner_fence=2 AND stop_generation=0
  AND enabled AND NOT stopped AND hold_reason IS NULL AND admitted_until>clock_timestamp()) THEN RAISE EXCEPTION 'CURRENT_ADMITTED_OWNER_REQUIRED';END IF;
 SELECT provider_requests,http_invocations,stored_bytes INTO v_provider,v_http,v_stored FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton;
 SELECT count(*) INTO v_count_before FROM population_top3000_202608_202609_v1.retained_price_partition_v1;
 SELECT count(*) INTO v_observations FROM population_top3000_202608_202609_v1.source_observation WHERE family_id IN('P01','P02');
 SELECT count(*) INTO v_expected FROM population_top3000_202608_202609_v1.expected_observation WHERE family_id IN('P01','P02');
 SELECT * INTO v_cal FROM population_top3000_202608_202609_v1.catalog_calendar WHERE session_date='2026-08-03' ORDER BY scope_id LIMIT 1;
 IF v_cal.session_date IS NULL THEN RAISE EXCEPTION 'EXISTING_AUG03_CALENDAR_REQUIRED';END IF;
 SELECT coalesce(jsonb_agg(to_jsonb(map_rows) ORDER BY to_jsonb(map_rows)::text),'[]'::jsonb) INTO v_map
 FROM (SELECT m.instrument_key,m.symbol,m.alpaca_asset_id,m.name,m.exchange,m.asset_class,m.massive_type,m.classification_method,
  m.classification_observed_at,m.provenance FROM reference.alpaca_fixed_window_instruments_remediated_v1 m WHERE m.instrument_key=41 ORDER BY m.symbol,m.alpaca_asset_id LIMIT 2) map_rows;
 IF v_map#>>'{0,symbol}' IS DISTINCT FROM 'AAPL' OR jsonb_array_length(v_map)<>1 THEN RAISE EXCEPTION 'RETAINED_AAPL_MAPPING_FIXTURE_CHANGED';END IF;
 v_map:=jsonb_build_object('instrument_reference',v_map,'alias_rows','[]'::jsonb,'alias_run_id',NULL,
  'reference_candidate_limit_reached',false,'alias_candidate_limit_reached',false,
  'alias_date_boundary_policy','Broad inclusive candidate lookup; effective-boundary semantics require identity review.','historical_company_identity_certified',false);
 v_inputs:=jsonb_build_object('calendar_record_sha256',v_cal.source_record_sha256,'calendar_receipt_sha256',v_cal.source_receipt_sha256,
  'loader_definition_sha256',encode(sha256(convert_to(pg_get_functiondef('consolidation_audit.load_next_alpaca_partition()'::regprocedure),'UTF8')),'hex'),
  'mapping_snapshot_sha256',encode(sha256(convert_to(v_map::text,'UTF8')),'hex'),
  'source_relation_oid','market_fixed_20250901_20260831.alpaca_sip_bars_1m_202608'::regclass::oid::text,
  'hash_codec','PG_FLOAT8_BINARY_ROW_SHA256_V1','source_snapshot_version','ROLLBACK_ONLY');
 v_hash:=encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.price_retained_partition_v1(uuid,text)'::regprocedure),'UTF8')),'hex');
 INSERT INTO population_top3000_202608_202609_v1.pipeline_adapter(family_id,stage,function_oid,function_sha256,resource_class,enabled,reviewed_test_evidence,producer_revision,expected_batch_seconds)
 SELECT 'P02',st,'population_top3000_202608_202609_v1.price_retained_partition_v1(uuid,text)'::regprocedure::oid,v_hash,'DATABASE',true,
  '{"test":"retained_price_p02_rollback_test.sql","status":"IN_PROGRESS_ROLLBACK"}','retained-price-partition-v1',5
 FROM unnest(ARRAY['ACQUIRE','NORMALIZE']) st
 ON CONFLICT(family_id,stage) DO UPDATE SET function_oid=EXCLUDED.function_oid,function_sha256=EXCLUDED.function_sha256,
  resource_class=EXCLUDED.resource_class,provider=NULL,enabled=EXCLUDED.enabled,reviewed_test_evidence=EXCLUDED.reviewed_test_evidence,
  producer_revision=EXCLUDED.producer_revision,expected_batch_seconds=EXCLUDED.expected_batch_seconds;
 UPDATE population_top3000_202608_202609_v1.pipeline_task SET not_before='infinity' WHERE status IN('READY','BLOCKED');
 UPDATE population_top3000_202608_202609_v1.pipeline_task SET blocker_until=NULL WHERE status='BLOCKED';
 FOREACH v_session IN ARRAY ARRAY['premarket','postmarket'] LOOP
  v_partition:='rollback-price-aapl-aug03-'||v_session||'-'||gen_random_uuid()::text;
  v_payload:=jsonb_build_object('kind','RETAINED_ALPACA_AUGUST_SESSION','instrument_key',41,'session_date','2026-08-03','session_label',v_session,'calendar_scope_id',v_cal.scope_id);
  v_session_start:=CASE v_session WHEN 'premarket' THEN '2026-08-03T08:00:00Z'::timestamptz ELSE v_cal.regular_close_utc END;
  v_session_end:=CASE v_session WHEN 'premarket' THEN v_cal.regular_open_utc ELSE '2026-08-04T00:00:00Z'::timestamptz END;
  SELECT count(*) INTO v_source_count FROM market_fixed_20250901_20260831.alpaca_sip_bars_1m_202608
   WHERE instrument_key=41 AND bar_ts>=v_session_start AND bar_ts<v_session_end AND session_label::text=v_session;
  IF v_source_count=0 THEN RAISE EXCEPTION 'ACTUAL_AAPL_EXTENDED_SOURCE_REQUIRED session=%',v_session;END IF;
  UPDATE population_top3000_202608_202609_v1.pipeline_control SET enabled=true,next_wakeup_at=clock_timestamp() WHERE singleton;
  v_task:=population_top3000_202608_202609_v1.pipeline_enqueue_v1('P02','ACQUIRE',v_partition,v_payload,v_inputs);
  v_started:=clock_timestamp();v_result:=population_top3000_202608_202609_v1.pipeline_tick_v1();v_elapsed:=clock_timestamp()-v_started;
  IF v_result#>>'{result,state}' IS DISTINCT FROM 'COMPLETE' OR v_result->>'task_id' IS DISTINCT FROM v_task::text OR v_elapsed>interval '5 seconds'
  THEN RAISE EXCEPTION 'ACTUAL_RETAINED_P02_DISPATCH_FAILED session=% result=%',v_session,v_result;END IF;
  SELECT * INTO v_manifest FROM population_top3000_202608_202609_v1.retained_price_partition_v1 WHERE created_by_task=v_task;
  IF v_manifest.manifest_id IS NULL OR v_manifest.observed_rows IS DISTINCT FROM v_source_count
   OR v_manifest.session_label IS DISTINCT FROM v_session OR v_manifest.first_bar<v_session_start OR v_manifest.last_bar>=v_session_end
   OR v_manifest.checks->>'structural_state' IS DISTINCT FROM 'OBSERVED_ROWS_STRUCTURALLY_VALID'
   OR v_manifest.checks->>'outside_declared_session_rows' IS DISTINCT FROM '0'
   OR v_manifest.interpretation->>'publication_eligible' IS DISTINCT FROM 'false'
   OR v_manifest.interpretation->>'dense_clock_denominator_used' IS DISTINCT FROM 'false'
   OR v_manifest.interpretation->>'expected_units' IS NOT NULL
   OR v_manifest.lineage->>'original_historical_receipt_proven' IS DISTINCT FROM 'false'
   OR v_manifest.lineage->>'raw_bars_copied' IS DISTINCT FROM '0' THEN RAISE EXCEPTION 'P02_MANIFEST_OR_QUALIFICATION_FAILED session=%',v_session;END IF;
  v_normalize:=(v_result#>>'{result,successor_task_id}')::uuid;
  v_result:=population_top3000_202608_202609_v1.pipeline_tick_v1();
  IF v_result->>'task_id' IS DISTINCT FROM v_normalize::text OR v_result#>>'{result,state}' IS DISTINCT FROM 'BLOCKED'
   OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=v_normalize
    AND blocker_code='RETAINED_PRICE_COMPANY_IDENTITY_AND_EXPECTATION_REQUIRED' AND blocker_until IS NULL)
  THEN RAISE EXCEPTION 'UNRESOLVED_P02_NORMALIZATION_NOT_HELD session=% result=%',v_session,v_result;END IF;
  IF population_top3000_202608_202609_v1.pipeline_enqueue_v1('P02','ACQUIRE',v_partition,v_payload,v_inputs) IS DISTINCT FROM v_task
  THEN RAISE EXCEPTION 'RETAINED_P02_ENQUEUE_REPLAY_NOT_IDEMPOTENT';END IF;
  v_results:=v_results||jsonb_build_array(jsonb_build_object('session',v_session,'observed_rows',v_source_count,'elapsed_ms',extract(epoch FROM v_elapsed)*1000));
 END LOOP;
 IF (SELECT count(*) FROM population_top3000_202608_202609_v1.retained_price_partition_v1)<>v_count_before+2
  OR (SELECT provider_requests FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton) IS DISTINCT FROM v_provider
  OR (SELECT http_invocations FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton) IS DISTINCT FROM v_http
  OR (SELECT stored_bytes FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton) IS DISTINCT FROM v_stored
  OR (SELECT count(*) FROM population_top3000_202608_202609_v1.source_observation WHERE family_id IN('P01','P02'))<>v_observations
  OR (SELECT count(*) FROM population_top3000_202608_202609_v1.expected_observation WHERE family_id IN('P01','P02'))<>v_expected
  OR EXISTS(SELECT 1 FROM cron.job WHERE jobid=596 AND active) THEN RAISE EXCEPTION 'P02_TEST_CHANGED_NETWORK_STORAGE_OR_DENOMINATORS';END IF;
 RAISE NOTICE 'PASS actual retained extended-session fixtures %, tested_function_sha256 %',v_results,v_hash;
END $test$;
SELECT 'PASS_RETAINED_PRICE_P02_PREMARKET_POSTMARKET_MANIFEST_AND_IDENTITY_HOLD' AS result,
 encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.price_retained_partition_v1(uuid,text)'::regprocedure),'UTF8')),'hex') AS tested_function_sha256;
ROLLBACK;
