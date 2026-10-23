-- Root-run test of both ACTUAL N02 validation diagnoses, after DERIVE/VALIDATE complete.
-- Root-reviewed bound revised from 15s to 45s after actual unchanged-measurement evidence: N02 ex-date 29467ms.
-- The original 50s statement deadline remains unchanged.
-- No provider/Edge requests. All task, adapter, exception and counter changes ROLLBACK.
BEGIN;
SET LOCAL statement_timeout='50s';
SET LOCAL lock_timeout='500ms';
SELECT pg_advisory_xact_lock(2026083000261022);
DO $test$
DECLARE v_task population_top3000_202608_202609_v1.pipeline_task%rowtype;v_hash text;v_result jsonb;v_validation uuid;
 v_provider bigint;v_http bigint;v_stored bigint;v_before_rows text;v_after_rows text;v_tasks bigint;v_started timestamptz;
 v_seen integer:=0;v_expected_diagnoses bigint;v_actual_diagnoses bigint;v_elapsed_ms numeric;v_dispatches jsonb:='[]';
BEGIN
 IF EXISTS(SELECT 1 FROM cron.job WHERE jobid=596 AND active) THEN RAISE EXCEPTION 'PAUSE_CRON_FIRST';END IF;
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE status='LEASED')
  OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_provider_lease WHERE released_at IS NULL) THEN RAISE EXCEPTION 'RECONCILE_ACTIVE_LEASES_FIRST';END IF;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton
  AND owner_id='top3000-recovery-20261023-root-4b1314bcb5c2' AND owner_fence=2 AND stop_generation=0 AND enabled AND NOT stopped
  AND hold_reason IS NULL AND admitted_until>clock_timestamp()) THEN RAISE EXCEPTION 'CURRENT_OWNER_AND_ADMISSION_REQUIRED';END IF;
 IF encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.pipeline_repair_v1(uuid,text)'::regprocedure),'UTF8')),'hex')
  IS DISTINCT FROM 'b8663b7bde533cfb33c50b092bb0200f16bb83965b3f11115b12ef6e61a9382c' THEN RAISE EXCEPTION 'GENERIC_DIAGNOSTIC_REVISION_CHANGED';END IF;
 IF encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.pipeline_measure_partition_v1(text,text)'::regprocedure),'UTF8')),'hex')
  IS DISTINCT FROM '88af4d4f5ea8f951e4c18b54764cf6fae742466b6de355d8fa29c200b074ad1b' THEN RAISE EXCEPTION 'REVIEWED_MEASUREMENT_REVISION_REQUIRED_FOR_45S_BOUND';END IF;
 IF (SELECT count(*) FROM population_top3000_202608_202609_v1.pipeline_task t JOIN population_top3000_202608_202609_v1.pipeline_partition p
  ON p.family_id=t.family_id AND p.partition_key=t.partition_key JOIN population_top3000_202608_202609_v1.pipeline_validation q ON q.validation_id=p.last_validation_id
  WHERE t.family_id='N02' AND t.stage='REPAIR' AND t.status IN('READY','BLOCKED') AND q.input_versions=t.input_versions AND q.result<>'PASS'
   AND t.partition_key IN('dividends-exdate-202608-202609','dividends-declaration-202608-202609'))<>2
 THEN RAISE EXCEPTION 'BOTH_ACTUAL_N02_NONPASS_REPAIR_TASKS_REQUIRED';END IF;
 SELECT provider_requests,http_invocations,stored_bytes INTO v_provider,v_http,v_stored FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton;
 SELECT encode(sha256(convert_to(coalesce(string_agg(family_id||':'||partition_key||':'||unit_key||':'||record_sha256,E'\n' ORDER BY family_id,partition_key,unit_key),''),'UTF8')),'hex') INTO v_before_rows
  FROM population_top3000_202608_202609_v1.source_observation WHERE family_id IN('N01','N02','D02');
 SELECT count(*) INTO v_tasks FROM population_top3000_202608_202609_v1.pipeline_task;
 v_hash:=encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.source_quality_diagnose_and_hold_v1(uuid,text)'::regprocedure),'UTF8')),'hex');
 INSERT INTO population_top3000_202608_202609_v1.pipeline_adapter
  (family_id,stage,function_oid,function_sha256,resource_class,provider,enabled,reviewed_test_evidence,producer_revision,expected_batch_seconds)
 VALUES('N02','REPAIR','population_top3000_202608_202609_v1.source_quality_diagnose_and_hold_v1(uuid,text)'::regprocedure::oid,v_hash,'DATABASE',NULL,true,
  '{"test":"source_quality_repair_hold_n02_rollback_test.sql","status":"IN_PROGRESS_ROLLBACK"}','source-diagnose-and-hold-20261023-v1',45)
 ON CONFLICT(family_id,stage) DO UPDATE SET function_oid=EXCLUDED.function_oid,function_sha256=EXCLUDED.function_sha256,resource_class='DATABASE',provider=NULL,
  enabled=true,reviewed_test_evidence=EXCLUDED.reviewed_test_evidence,producer_revision=EXCLUDED.producer_revision,expected_batch_seconds=45;
 UPDATE population_top3000_202608_202609_v1.pipeline_task SET not_before='infinity' WHERE status IN('READY','BLOCKED');
 UPDATE population_top3000_202608_202609_v1.pipeline_task SET blocker_until=NULL WHERE status='BLOCKED';
 FOR v_task IN SELECT t.* FROM population_top3000_202608_202609_v1.pipeline_task t
  JOIN population_top3000_202608_202609_v1.pipeline_partition p ON p.family_id=t.family_id AND p.partition_key=t.partition_key
  JOIN population_top3000_202608_202609_v1.pipeline_validation q ON q.validation_id=p.last_validation_id
  WHERE t.family_id='N02' AND t.stage='REPAIR' AND t.status IN('READY','BLOCKED') AND q.input_versions=t.input_versions AND q.result<>'PASS'
   AND t.partition_key IN('dividends-exdate-202608-202609','dividends-declaration-202608-202609') ORDER BY t.partition_key
 LOOP
  v_seen:=v_seen+1;
  SELECT last_validation_id INTO v_validation FROM population_top3000_202608_202609_v1.pipeline_partition WHERE family_id='N02' AND partition_key=v_task.partition_key;
  UPDATE population_top3000_202608_202609_v1.pipeline_task SET status='READY',not_before=clock_timestamp(),blocker_code=NULL,blocker_until=NULL,resumption_condition=NULL WHERE task_id=v_task.task_id;
  UPDATE population_top3000_202608_202609_v1.pipeline_control SET enabled=true,next_wakeup_at=clock_timestamp() WHERE singleton;
  v_started:=clock_timestamp();v_result:=population_top3000_202608_202609_v1.pipeline_tick_v1();
  v_elapsed_ms:=round(extract(epoch FROM clock_timestamp()-v_started)*1000,3);
  IF v_result->>'task_id' IS DISTINCT FROM v_task.task_id::text OR v_result#>>'{result,state}' IS DISTINCT FROM 'BLOCKED'
   OR v_elapsed_ms>45000 OR v_result#>>'{result,successor_task_id}' IS NOT NULL
  THEN RAISE EXCEPTION 'SOURCE_DIAGNOSTIC_HOLD_DISPATCH_FAILED: %',v_result;END IF;
  IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=v_task.task_id
   AND blocker_code='N02_COMPANY_AND_SOURCE_QUALITY_EVIDENCE_REQUIRED' AND blocker_until IS NULL
   AND resumption_condition->>'automatic_retry'='false' AND resumption_condition->>'repair_applied'='false' AND resumption_condition->>'publication_admitted'='false'
   AND resumption_condition->>'validation_id'=v_validation::text)
  THEN RAISE EXCEPTION 'N02_CURRENT_VALIDATION_NOT_EXPLICITLY_HELD';END IF;
  v_dispatches:=v_dispatches||jsonb_build_array(jsonb_build_object('partition_key',v_task.partition_key,'task_id',v_task.task_id,'elapsed_ms',v_elapsed_ms,'reviewed_max_batch_seconds',45));
  -- Compare every expected positive diagnosis with actual persisted exception units.
  WITH expected AS (
   SELECT code,units FROM population_top3000_202608_202609_v1.pipeline_validation q
   CROSS JOIN LATERAL (VALUES ('MISSING_SOURCE_UNITS',q.missing_units),('MISSING_REQUIRED_FIELDS',q.missing_required_fields),
    ('UNAVAILABLE_HISTORY',q.unavailable_fields),('CONFLICTING_FIELDS',q.conflicting_fields),('QUARANTINED_FIELDS',q.quarantined_fields),
    ('UNEXPECTED_UNITS',q.unexpected_units),('STRUCTURAL_FAILURES',q.structural_failures),('MEMBERSHIP_FAILURES',q.membership_failures),
    ('TEMPORAL_FAILURES',q.temporal_failures),('REQUIRED_INDEPENDENT_CHECKS',cardinality(q.required_checks_missing)::bigint),
    ('DENOMINATOR_UNRESOLVED',CASE WHEN q.result='DENOMINATOR_UNRESOLVED' THEN 1 ELSE 0 END::bigint)) x(code,units)
   WHERE q.validation_id=v_validation AND units>0
  ) SELECT count(*),count(e.code) FILTER(WHERE e.affected_units=x.units AND e.resolved_at IS NULL)
   INTO v_expected_diagnoses,v_actual_diagnoses FROM expected x LEFT JOIN population_top3000_202608_202609_v1.pipeline_exception e ON e.validation_id=v_validation AND e.code=x.code;
  IF v_expected_diagnoses=0 OR v_expected_diagnoses<>v_actual_diagnoses THEN RAISE EXCEPTION 'ACTUAL_N02_DIAGNOSIS_UNITS_NOT_PRESERVED';END IF;
 END LOOP;
 SELECT encode(sha256(convert_to(coalesce(string_agg(family_id||':'||partition_key||':'||unit_key||':'||record_sha256,E'\n' ORDER BY family_id,partition_key,unit_key),''),'UTF8')),'hex') INTO v_after_rows
  FROM population_top3000_202608_202609_v1.source_observation WHERE family_id IN('N01','N02','D02');
 IF v_seen<>2 OR v_after_rows IS DISTINCT FROM v_before_rows OR (SELECT count(*) FROM population_top3000_202608_202609_v1.pipeline_task)<>v_tasks
  OR (SELECT provider_requests FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton) IS DISTINCT FROM v_provider
  OR (SELECT http_invocations FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton) IS DISTINCT FROM v_http
  OR (SELECT stored_bytes FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton) IS DISTINCT FROM v_stored
  OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_partition WHERE family_id='N02' AND publication_eligible)
  OR EXISTS(SELECT 1 FROM cron.job WHERE jobid=596 AND active) THEN RAISE EXCEPTION 'SOURCE_DIAGNOSTIC_CHANGED_SOURCE_COUNTERS_OR_CASCADED';END IF;
 PERFORM set_config('top3000.source_hold_dispatch_timings',v_dispatches::text,true);
END $test$;
SELECT 'PASS' result,'SOURCE_EXPLICIT_REPAIR_HOLD_ROLLBACK_VERIFIED' receipt_event,
 'N02' tested_actual_family,2 tested_actual_partitions,2 owner_fence,0 stop_generation,45 expected_batch_seconds,
 current_setting('top3000.source_hold_dispatch_timings')::jsonb dispatch_timings,
 encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.pipeline_measure_partition_v1(text,text)'::regprocedure),'UTF8')),'hex') tested_measurement_sha256,
 '8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd' scope_sha256,
 encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.source_quality_diagnose_and_hold_v1(uuid,text)'::regprocedure),'UTF8')),'hex') tested_function_sha256,
 encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.pipeline_repair_v1(uuid,text)'::regprocedure),'UTF8')),'hex') generic_repair_sha256,
 false source_rows_modified,false provider_requested,false publication_admitted;
ROLLBACK;
