-- Actual AAPL41/Aug3 retained rows and real dispatcher. All mission changes roll back.
-- Pause cron596 and reconcile live leases first. Source relations are SELECT-only throughout.
BEGIN;
SET LOCAL statement_timeout='5s';
SET LOCAL lock_timeout='500ms';
SET LOCAL TimeZone='UTC';
SELECT pg_advisory_xact_lock(2026083000261022);
DO $test$
DECLARE v_cal population_top3000_202608_202609_v1.catalog_calendar%rowtype;
 v_map jsonb;v_inputs jsonb;v_payload jsonb;v_hash text;v_partition text:='rollback-price-aapl-aug03-'||gen_random_uuid()::text;
 v_task uuid;v_normalize uuid;v_bad uuid;v_result jsonb;v_manifest population_top3000_202608_202609_v1.retained_price_partition_v1%rowtype;
 v_provider bigint;v_http bigint;v_stored bigint;v_source_count bigint;v_count_before bigint;v_observations bigint;v_expected bigint;
 v_started timestamptz;v_elapsed interval;
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
 v_payload:=jsonb_build_object('kind','RETAINED_ALPACA_AUGUST_SESSION','instrument_key',41,'session_date','2026-08-03','session_label','regular','calendar_scope_id',v_cal.scope_id);
 SELECT count(*) INTO v_source_count FROM market_fixed_20250901_20260831.alpaca_sip_bars_1m_202608
  WHERE instrument_key=41 AND bar_ts>='2026-08-03T13:30:00Z' AND bar_ts<'2026-08-03T20:00:00Z' AND session_label::text='regular';
 IF v_source_count=0 THEN RAISE EXCEPTION 'ACTUAL_AAPL_AUG03_SOURCE_REQUIRED';END IF;
 v_hash:=encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.price_retained_partition_v1(uuid,text)'::regprocedure),'UTF8')),'hex');
 INSERT INTO population_top3000_202608_202609_v1.pipeline_adapter(family_id,stage,function_oid,function_sha256,resource_class,enabled,reviewed_test_evidence,producer_revision,expected_batch_seconds)
 SELECT 'P01',st,'population_top3000_202608_202609_v1.price_retained_partition_v1(uuid,text)'::regprocedure::oid,v_hash,'DATABASE',true,
  '{"test":"retained_price_partition_rollback_test.sql","status":"IN_PROGRESS_ROLLBACK"}','retained-price-partition-v1',5
 FROM unnest(ARRAY['ACQUIRE','NORMALIZE']) st
 ON CONFLICT(family_id,stage) DO UPDATE SET function_oid=EXCLUDED.function_oid,function_sha256=EXCLUDED.function_sha256,
  resource_class=EXCLUDED.resource_class,enabled=EXCLUDED.enabled,reviewed_test_evidence=EXCLUDED.reviewed_test_evidence,producer_revision=EXCLUDED.producer_revision,expected_batch_seconds=EXCLUDED.expected_batch_seconds;
 UPDATE population_top3000_202608_202609_v1.pipeline_task SET not_before='infinity' WHERE status IN('READY','BLOCKED');
 UPDATE population_top3000_202608_202609_v1.pipeline_task SET blocker_until=NULL WHERE status='BLOCKED';
 UPDATE population_top3000_202608_202609_v1.pipeline_control SET enabled=true,next_wakeup_at=clock_timestamp() WHERE singleton;
 v_task:=population_top3000_202608_202609_v1.pipeline_enqueue_v1('P01','ACQUIRE',v_partition,v_payload,v_inputs);
 v_started:=clock_timestamp();v_result:=population_top3000_202608_202609_v1.pipeline_tick_v1();v_elapsed:=clock_timestamp()-v_started;
 IF v_result#>>'{result,state}' IS DISTINCT FROM 'COMPLETE' OR v_result->>'task_id' IS DISTINCT FROM v_task::text OR v_elapsed>interval '5 seconds'
 THEN RAISE EXCEPTION 'ACTUAL_RETAINED_PRICE_DISPATCH_FAILED: %',v_result;END IF;
 SELECT * INTO v_manifest FROM population_top3000_202608_202609_v1.retained_price_partition_v1 WHERE created_by_task=v_task;
 IF v_manifest.manifest_id IS NULL OR v_manifest.observed_rows IS DISTINCT FROM v_source_count
  OR v_manifest.checks->>'structural_state' IS DISTINCT FROM 'OBSERVED_ROWS_STRUCTURALLY_VALID'
  OR v_manifest.interpretation->>'publication_eligible' IS DISTINCT FROM 'false'
  OR v_manifest.interpretation->>'dense_clock_denominator_used' IS DISTINCT FROM 'false'
  OR v_manifest.interpretation->>'expected_units' IS NOT NULL
  OR v_manifest.lineage->>'original_historical_receipt_proven' IS DISTINCT FROM 'false'
  OR v_manifest.lineage->>'raw_bars_copied' IS DISTINCT FROM '0' THEN RAISE EXCEPTION 'PRICE_MANIFEST_OR_QUALIFICATION_FAILED';END IF;
 v_normalize:=(v_result#>>'{result,successor_task_id}')::uuid;
 v_result:=population_top3000_202608_202609_v1.pipeline_tick_v1();
 IF v_result->>'task_id' IS DISTINCT FROM v_normalize::text OR v_result#>>'{result,state}' IS DISTINCT FROM 'BLOCKED'
  OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=v_normalize
   AND blocker_code='RETAINED_PRICE_COMPANY_IDENTITY_AND_EXPECTATION_REQUIRED' AND blocker_until IS NULL)
 THEN RAISE EXCEPTION 'UNRESOLVED_PRICE_NORMALIZATION_NOT_HELD: %',v_result;END IF;
 IF population_top3000_202608_202609_v1.pipeline_enqueue_v1('P01','ACQUIRE',v_partition,v_payload,v_inputs) IS DISTINCT FROM v_task
  OR (SELECT count(*) FROM population_top3000_202608_202609_v1.retained_price_partition_v1)<>v_count_before+1
 THEN RAISE EXCEPTION 'RETAINED_PRICE_ENQUEUE_REPLAY_NOT_IDEMPOTENT';END IF;
 -- A new immutable task with an unapproved mapping hash must block without altering any source.
 UPDATE population_top3000_202608_202609_v1.pipeline_task SET not_before='infinity' WHERE status IN('READY','BLOCKED');
 UPDATE population_top3000_202608_202609_v1.pipeline_control SET enabled=true,next_wakeup_at=clock_timestamp() WHERE singleton;
 v_bad:=population_top3000_202608_202609_v1.pipeline_enqueue_v1('P01','ACQUIRE',v_partition||'-bad-map',v_payload,
  jsonb_set(v_inputs,'{mapping_snapshot_sha256}',to_jsonb(repeat('0',64))));
 v_result:=population_top3000_202608_202609_v1.pipeline_tick_v1();
 IF v_result#>>'{result,state}' IS DISTINCT FROM 'BLOCKED' OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task
  WHERE task_id=v_bad AND blocker_code='RETAINED_PRICE_INPUT_VERSION_RECONCILIATION') THEN RAISE EXCEPTION 'UNBOUND_MAPPING_VERSION_ACCEPTED';END IF;
 IF (SELECT provider_requests FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton) IS DISTINCT FROM v_provider
  OR (SELECT http_invocations FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton) IS DISTINCT FROM v_http
  OR (SELECT stored_bytes FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton) IS DISTINCT FROM v_stored
  OR (SELECT count(*) FROM population_top3000_202608_202609_v1.source_observation WHERE family_id IN('P01','P02'))<>v_observations
  OR (SELECT count(*) FROM population_top3000_202608_202609_v1.expected_observation WHERE family_id IN('P01','P02'))<>v_expected
  OR EXISTS(SELECT 1 FROM cron.job WHERE jobid=596 AND active) THEN RAISE EXCEPTION 'PRICE_TEST_CHANGED_NETWORK_STORAGE_OR_DENOMINATORS';END IF;
 RAISE NOTICE 'PASS actual retained price rows %, manifest1, acquisition elapsed %, no bars copied; identity/denominator remain unresolved',v_source_count,v_elapsed;
END $test$;
SELECT 'PASS_RETAINED_PRICE_ACTUAL_PARTITION_MANIFEST_REPLAY_AND_IDENTITY_HOLD' AS result;
ROLLBACK;
