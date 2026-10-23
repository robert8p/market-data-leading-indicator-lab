-- Root-run real N03 nonpassing validation diagnosis. No new source calls; full ROLLBACK.
BEGIN;
SELECT cron.alter_job(596,active:=false);
SET LOCAL statement_timeout='5s';
SET LOCAL lock_timeout='500ms';
SELECT pg_advisory_xact_lock(2026083000261022);
DO $test$
DECLARE v_task population_top3000_202608_202609_v1.pipeline_task%rowtype;v_hash text;v_result jsonb;v_validation uuid;
 v_provider bigint;v_http bigint;v_stored bigint;v_before_rows text;v_after_rows text;v_tasks bigint;v_started timestamptz;
BEGIN
 IF EXISTS(SELECT 1 FROM cron.job WHERE jobid=596 AND active) THEN RAISE EXCEPTION 'PAUSE_CRON_FIRST';END IF;
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE status='LEASED')
  OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_provider_lease WHERE released_at IS NULL) THEN RAISE EXCEPTION 'RECONCILE_ACTIVE_LEASES_FIRST';END IF;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton
  AND owner_id='top3000-recovery-20261023-root-4b1314bcb5c2' AND owner_fence=2 AND stop_generation=0 AND enabled AND NOT stopped
  AND hold_reason IS NULL AND admitted_until>clock_timestamp()) THEN RAISE EXCEPTION 'CURRENT_OWNER_AND_ADMISSION_REQUIRED';END IF;
 SELECT * INTO v_task FROM population_top3000_202608_202609_v1.pipeline_task WHERE family_id='N03' AND stage='REPAIR'
  AND partition_key='splits-execution-202608-202609' AND status IN('READY','BLOCKED') ORDER BY created_at LIMIT 1;
 IF NOT FOUND THEN RAISE EXCEPTION 'ACTUAL_N03_REPAIR_TASK_REQUIRED';END IF;
 SELECT last_validation_id INTO v_validation FROM population_top3000_202608_202609_v1.pipeline_partition WHERE family_id='N03' AND partition_key=v_task.partition_key;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_validation WHERE validation_id=v_validation
  AND result='DENOMINATOR_UNRESOLVED' AND input_versions=v_task.input_versions) THEN RAISE EXCEPTION 'ACTUAL_N03_DENOMINATOR_VALIDATION_REQUIRED';END IF;
 SELECT provider_requests,http_invocations,stored_bytes INTO v_provider,v_http,v_stored FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton;
 SELECT encode(sha256(convert_to(string_agg(unit_key||':'||record_sha256,E'\n' ORDER BY unit_key),'UTF8')),'hex') INTO v_before_rows
  FROM population_top3000_202608_202609_v1.source_observation WHERE family_id='N03' AND partition_key=v_task.partition_key;
 SELECT count(*) INTO v_tasks FROM population_top3000_202608_202609_v1.pipeline_task WHERE family_id='N03';
 v_hash:=encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.n03_repair_diagnose_and_hold_v1(uuid,text)'::regprocedure),'UTF8')),'hex');
 INSERT INTO population_top3000_202608_202609_v1.pipeline_adapter
  (family_id,stage,function_oid,function_sha256,resource_class,provider,enabled,reviewed_test_evidence,producer_revision,expected_batch_seconds)
 VALUES('N03','REPAIR','population_top3000_202608_202609_v1.n03_repair_diagnose_and_hold_v1(uuid,text)'::regprocedure::oid,v_hash,'DATABASE',NULL,true,
  '{"test":"n03_explicit_repair_hold_rollback_test.sql","status":"IN_PROGRESS_ROLLBACK"}','n03-diagnose-and-hold-v1',5)
 ON CONFLICT(family_id,stage) DO UPDATE SET function_oid=EXCLUDED.function_oid,function_sha256=EXCLUDED.function_sha256,resource_class='DATABASE',provider=NULL,
  enabled=true,reviewed_test_evidence=EXCLUDED.reviewed_test_evidence,producer_revision=EXCLUDED.producer_revision,expected_batch_seconds=5;
 UPDATE population_top3000_202608_202609_v1.pipeline_task SET not_before='infinity' WHERE status IN('READY','BLOCKED');
 UPDATE population_top3000_202608_202609_v1.pipeline_task SET blocker_until=NULL WHERE status='BLOCKED';
 UPDATE population_top3000_202608_202609_v1.pipeline_task SET status='READY',not_before=clock_timestamp(),blocker_code=NULL,blocker_until=NULL,resumption_condition=NULL WHERE task_id=v_task.task_id;
 UPDATE population_top3000_202608_202609_v1.pipeline_control SET enabled=true,next_wakeup_at=clock_timestamp() WHERE singleton;
 v_started:=clock_timestamp();v_result:=population_top3000_202608_202609_v1.pipeline_tick_v1();
 IF v_result->>'task_id' IS DISTINCT FROM v_task.task_id::text OR v_result#>>'{result,state}' IS DISTINCT FROM 'BLOCKED'
  OR clock_timestamp()-v_started>interval '5 seconds' OR v_result#>>'{result,successor_task_id}' IS NOT NULL
 THEN RAISE EXCEPTION 'N03_DIAGNOSTIC_HOLD_DISPATCH_FAILED: %',v_result;END IF;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=v_task.task_id
  AND blocker_code='N03_COMPANY_AND_SOURCE_QUALITY_EVIDENCE_REQUIRED' AND blocker_until IS NULL
  AND resumption_condition->>'repair_applied'='false' AND resumption_condition->>'publication_admitted'='false')
  OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_exception WHERE validation_id=v_validation AND code='DENOMINATOR_UNRESOLVED' AND affected_units>0 AND resolved_at IS NULL)
 THEN RAISE EXCEPTION 'N03_EXACT_DENOMINATOR_DIAGNOSIS_NOT_RECORDED_AND_HELD';END IF;
 SELECT encode(sha256(convert_to(string_agg(unit_key||':'||record_sha256,E'\n' ORDER BY unit_key),'UTF8')),'hex') INTO v_after_rows
  FROM population_top3000_202608_202609_v1.source_observation WHERE family_id='N03' AND partition_key=v_task.partition_key;
 IF v_after_rows IS DISTINCT FROM v_before_rows
  OR (SELECT count(*) FROM population_top3000_202608_202609_v1.pipeline_task WHERE family_id='N03')<>v_tasks
  OR (SELECT provider_requests FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton) IS DISTINCT FROM v_provider
  OR (SELECT http_invocations FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton) IS DISTINCT FROM v_http
  OR (SELECT stored_bytes FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton) IS DISTINCT FROM v_stored
  OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_partition WHERE family_id='N03' AND publication_eligible)
  OR EXISTS(SELECT 1 FROM cron.job WHERE jobid=596 AND active) THEN RAISE EXCEPTION 'N03_DIAGNOSTIC_CHANGED_SOURCE_OR_CASCADED';END IF;
 RAISE NOTICE 'PASS actual N03 validation % diagnosed without repair/publication, tested_function_sha256 %',v_validation,v_hash;
END $test$;
SELECT 'PASS_N03_ACTUAL_DIAGNOSIS_EXPLICIT_HOLD_NO_CASCADE' AS result,
 encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.n03_repair_diagnose_and_hold_v1(uuid,text)'::regprocedure),'UTF8')),'hex') AS tested_function_sha256;
ROLLBACK;
