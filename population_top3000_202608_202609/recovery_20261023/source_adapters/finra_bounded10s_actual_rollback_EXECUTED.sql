-- Root-run actual FINRA Aug3 readback plus source-value/missing-row negative cases. FULL ROLLBACK.
-- Pause cron596 and reconcile leases before this transaction (or pause inside the transaction).
BEGIN;
SELECT cron.alter_job(596,active:=false);
SET LOCAL statement_timeout='45s';
SET LOCAL lock_timeout='500ms';
SET LOCAL TimeZone='UTC';
SELECT pg_advisory_xact_lock(2026083000261022);
DO $test$
DECLARE v_task population_top3000_202608_202609_v1.pipeline_task%rowtype;v_hash text;v_result jsonb;v_evidence jsonb;v_key text;
 v_provider bigint;v_http bigint;v_stored bigint;v_expected bigint;v_validations bigint;v_before_bundle text;v_readback_id text;
 v_started timestamptz;v_elapsed_ms numeric;v_timings jsonb:='[]';v_phase text;v_new_values jsonb;
BEGIN
 IF EXISTS(SELECT 1 FROM cron.job WHERE jobid=596 AND active) THEN RAISE EXCEPTION 'PAUSE_CRON_FIRST';END IF;
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE status='LEASED')
  OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_provider_lease WHERE released_at IS NULL) THEN RAISE EXCEPTION 'RECONCILE_ACTIVE_LEASES_FIRST';END IF;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton
  AND owner_id='top3000-recovery-20261023-root-4b1314bcb5c2' AND owner_fence=2 AND stop_generation=0 AND enabled AND NOT stopped
  AND hold_reason IS NULL AND admitted_until>clock_timestamp()) THEN RAISE EXCEPTION 'CURRENT_OWNER_AND_ADMISSION_REQUIRED';END IF;
 SELECT * INTO v_task FROM population_top3000_202608_202609_v1.pipeline_task WHERE family_id='S01' AND stage='VALIDATE'
  AND partition_key='CNMS:2026-09-30' AND status IN('READY','BLOCKED') ORDER BY created_at LIMIT 1;
 IF NOT FOUND THEN RAISE EXCEPTION 'ACTUAL_AUG03_VALIDATE_TASK_REQUIRED';END IF;
 IF (SELECT count(*) FROM population_top3000_202608_202609_v1.source_observation
  WHERE release_id='top3000_companies_202608_202609_v1' AND family_id='S01' AND partition_key=v_task.partition_key)<>12372
 THEN RAISE EXCEPTION 'ACTUAL_12372_ROW_FINRA_FIXTURE_CHANGED';END IF;
 SELECT provider_requests,http_invocations,stored_bytes INTO v_provider,v_http,v_stored FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton;
 SELECT count(*) INTO v_expected FROM population_top3000_202608_202609_v1.expected_observation WHERE family_id='S01';
 SELECT count(*) INTO v_validations FROM population_top3000_202608_202609_v1.pipeline_validation WHERE family_id='S01';
 v_before_bundle:=population_top3000_202608_202609_v1.finra_code_hash_v1();
 v_hash:=encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.finra_independent_validate_v1(uuid,text)'::regprocedure),'UTF8')),'hex');
 INSERT INTO population_top3000_202608_202609_v1.pipeline_adapter
  (family_id,stage,function_oid,function_sha256,resource_class,provider,enabled,reviewed_test_evidence,producer_revision,expected_batch_seconds)
 VALUES('S01','VALIDATE','population_top3000_202608_202609_v1.finra_independent_validate_v1(uuid,text)'::regprocedure::oid,v_hash,'DATABASE',NULL,true,
  '{"test":"finra_independent_readback_rollback_test.sql","status":"IN_PROGRESS_ROLLBACK"}','finra-regex-independent-readback-v2-bounded10s',10)
 ON CONFLICT(family_id,stage) DO UPDATE SET function_oid=EXCLUDED.function_oid,function_sha256=EXCLUDED.function_sha256,
  resource_class='DATABASE',provider=NULL,enabled=true,reviewed_test_evidence=EXCLUDED.reviewed_test_evidence,producer_revision=EXCLUDED.producer_revision,expected_batch_seconds=10;
 UPDATE population_top3000_202608_202609_v1.pipeline_task SET not_before='infinity' WHERE status IN('READY','BLOCKED');
 UPDATE population_top3000_202608_202609_v1.pipeline_task SET blocker_until=NULL WHERE status='BLOCKED';
 SELECT unit_key INTO v_key FROM population_top3000_202608_202609_v1.source_observation
  WHERE release_id='top3000_companies_202608_202609_v1' AND family_id='S01' AND partition_key=v_task.partition_key ORDER BY unit_key LIMIT 1;
 FOREACH v_phase IN ARRAY ARRAY['ACTUAL_SOURCE','VALID_HASH_WRONG_VALUE','MISSING_ROW'] LOOP
  IF v_phase='VALID_HASH_WRONG_VALUE' THEN
   SELECT jsonb_set(values_json,'{short_volume}',to_jsonb((values_json->>'short_volume')::numeric+1)) INTO v_new_values
    FROM population_top3000_202608_202609_v1.source_observation WHERE release_id='top3000_companies_202608_202609_v1'
    AND family_id='S01' AND partition_key=v_task.partition_key AND unit_key=v_key;
   UPDATE population_top3000_202608_202609_v1.source_observation SET values_json=v_new_values,
    record_sha256=encode(sha256(convert_to(jsonb_build_object('values',v_new_values,'states',field_state,'lineage',lineage)::text,'UTF8')),'hex')
    WHERE release_id='top3000_companies_202608_202609_v1' AND family_id='S01' AND partition_key=v_task.partition_key AND unit_key=v_key;
  ELSIF v_phase='MISSING_ROW' THEN
   DELETE FROM population_top3000_202608_202609_v1.source_observation WHERE release_id='top3000_companies_202608_202609_v1'
    AND family_id='S01' AND partition_key=v_task.partition_key AND unit_key=v_key;
  END IF;
  -- These control changes are fixture isolation only, and roll back; no counters are reset.
  UPDATE population_top3000_202608_202609_v1.pipeline_task SET status='READY',not_before=clock_timestamp(),blocker_code=NULL,
   blocker_until=NULL,resumption_condition=NULL WHERE task_id=v_task.task_id;
  UPDATE population_top3000_202608_202609_v1.pipeline_control SET enabled=true,next_wakeup_at=clock_timestamp() WHERE singleton;
  v_started:=clock_timestamp();v_result:=population_top3000_202608_202609_v1.pipeline_tick_v1();v_elapsed_ms:=extract(epoch FROM clock_timestamp()-v_started)*1000;
  IF v_result->>'task_id' IS DISTINCT FROM v_task.task_id::text OR v_result#>>'{result,state}' IS DISTINCT FROM 'BLOCKED' OR v_elapsed_ms>10000
   THEN RAISE EXCEPTION 'FINRA_ACTUAL_READBACK_DISPATCH_FAILED phase=% result=% elapsed=%',v_phase,v_result,v_elapsed_ms;END IF;
  SELECT resumption_condition->>'readback_id' INTO v_readback_id FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=v_task.task_id;
  SELECT evidence INTO v_evidence FROM population_top3000_202608_202609_v1.finra_source_readback_v1 WHERE readback_id=v_readback_id;
  IF v_evidence IS NULL OR v_evidence->>'reported_source_rows' IS DISTINCT FROM '12372' OR v_evidence->>'footer_records' IS DISTINCT FROM '12372'
   OR v_evidence->>'producer_parser_invoked' IS DISTINCT FROM 'false' OR v_evidence->>'missing_rows_are_zero' IS DISTINCT FROM 'false'
   OR v_evidence->>'company_expected_units' IS NOT NULL OR v_evidence->>'company_completeness_ratio' IS NOT NULL
   OR v_evidence->>'publication_eligible' IS DISTINCT FROM 'false' OR v_evidence->>'publication_time_verified' IS DISTINCT FROM 'false'
  THEN RAISE EXCEPTION 'FINRA_READBACK_QUALIFICATION_OR_FOOTER_FAILED phase=%',v_phase;END IF;
  IF v_phase='ACTUAL_SOURCE' THEN
   IF v_evidence->>'source_comparison_result' IS DISTINCT FROM 'PASS' OR v_evidence->>'normalized_rows' IS DISTINCT FROM '12372'
    OR v_evidence->>'missing_normalized_rows' IS DISTINCT FROM '0' OR v_evidence->>'different_normalized_rows' IS DISTINCT FROM '0'
    OR v_evidence->>'source_integrity_pass' IS DISTINCT FROM 'true' OR v_evidence->>'header_footer_pass' IS DISTINCT FROM 'true'
    OR (SELECT blocker_code FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=v_task.task_id) IS DISTINCT FROM 'FINRA_COMPANY_DENOMINATOR_AND_PUBLICATION_UNRESOLVED'
   THEN RAISE EXCEPTION 'ACTUAL_SOURCE_DID_NOT_PASS_READBACK_WITH_TRUTHFUL_HOLD: %',v_evidence;END IF;
  ELSIF v_phase='VALID_HASH_WRONG_VALUE' THEN
   IF v_evidence->>'source_comparison_result' IS DISTINCT FROM 'FAIL' OR v_evidence->>'different_normalized_rows' IS DISTINCT FROM '1'
    OR v_evidence->>'corrupt_normalized_hashes' IS DISTINCT FROM '0'
   THEN RAISE EXCEPTION 'VALID_HASH_WRONG_SOURCE_VALUE_NOT_DETECTED';END IF;
  ELSE
   IF v_evidence->>'source_comparison_result' IS DISTINCT FROM 'FAIL' OR v_evidence->>'missing_normalized_rows' IS DISTINCT FROM '1'
    OR v_evidence->>'normalized_rows' IS DISTINCT FROM '12371'
   THEN RAISE EXCEPTION 'MISSING_SOURCE_ROW_NOT_PRESERVED_AS_MISSING';END IF;
  END IF;
  v_timings:=v_timings||jsonb_build_array(jsonb_build_object('phase',v_phase,'elapsed_ms',v_elapsed_ms,'result',v_evidence->>'source_comparison_result'));
 END LOOP;
 IF population_top3000_202608_202609_v1.finra_code_hash_v1() IS DISTINCT FROM v_before_bundle
  OR (SELECT provider_requests FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton) IS DISTINCT FROM v_provider
  OR (SELECT http_invocations FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton) IS DISTINCT FROM v_http
  OR (SELECT stored_bytes FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton) IS DISTINCT FROM v_stored
  OR (SELECT count(*) FROM population_top3000_202608_202609_v1.expected_observation WHERE family_id='S01')<>v_expected
  OR (SELECT count(*) FROM population_top3000_202608_202609_v1.pipeline_validation WHERE family_id='S01')<>v_validations
  OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_partition WHERE family_id='S01' AND publication_eligible)
  OR EXISTS(SELECT 1 FROM cron.job WHERE jobid=596 AND active) THEN RAISE EXCEPTION 'FINRA_READBACK_TEST_CHANGED_UNADMITTED_STATE';END IF;
 RAISE NOTICE 'PASS actual-source and negative readbacks %, tested_function_sha256 %, running_finra_bundle_unchanged %',v_timings,v_hash,v_before_bundle;
END $test$;
SELECT 'PASS_FINRA_ACTUAL_ALTERNATE_PARSER_ALL_ROWS_VALUE_MISSINGNESS_AND_HOLD' AS result,
 encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.finra_independent_validate_v1(uuid,text)'::regprocedure),'UTF8')),'hex') AS tested_function_sha256;
ROLLBACK;
