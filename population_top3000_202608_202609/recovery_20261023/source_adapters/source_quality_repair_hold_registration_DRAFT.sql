-- Root-reviewed 45s adapter bound; basis: actual N02 ex-date measurement 29467ms, unchanged outputs.
-- Root-only after persisting the actual matching-hash test output as the named event.
-- Installs the same tested wrapper for three explicitly allowlisted families.
-- No acquisition, validation or publication tasks are added. Other holds stay untouched.
BEGIN;
SET LOCAL statement_timeout='5s';
SET LOCAL lock_timeout='500ms';
SELECT pg_advisory_xact_lock(2026083000261022);
DO $admit$
DECLARE v_hash text;v_evidence jsonb;v_released bigint;
BEGIN
 PERFORM 1 FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton FOR UPDATE;
 PERFORM 1 FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton FOR UPDATE;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.acquisition_control a
  JOIN population_top3000_202608_202609_v1.pipeline_control p ON p.singleton=a.singleton
  WHERE a.singleton AND a.owner_id='top3000-recovery-20261023-root-4b1314bcb5c2' AND a.owner_fence=2 AND a.stop_generation=0
   AND a.owner_id=p.expected_owner_id AND a.owner_fence=p.expected_owner_fence AND a.stop_generation=p.expected_stop_generation
   AND a.scope_sha256=p.scope_sha256 AND a.scope_sha256='8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd'
   AND a.enabled AND NOT a.stopped AND a.hold_reason IS NULL AND a.admitted_until>clock_timestamp()
   AND p.shared_scheduler_verified AND p.budget_verified_at IS NOT NULL AND p.deadline>clock_timestamp() AND p.tick_count<p.max_ticks
   AND p.actual_usd+p.accrued_usd+p.unavoidable_usd+p.remaining_uncommitted_usd+p.repair_egress_reserve_usd+p.reserved_request_usd<=p.budget_cap_usd)
 THEN RAISE EXCEPTION 'QUALITY_ADAPTER_OWNER_STOP_SCOPE_OR_BUDGET_CHANGED';END IF;
 v_hash:=encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.source_quality_diagnose_and_hold_v1(uuid,text)'::regprocedure),'UTF8')),'hex');
 IF encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.pipeline_repair_v1(uuid,text)'::regprocedure),'UTF8')),'hex')
  IS DISTINCT FROM 'b8663b7bde533cfb33c50b092bb0200f16bb83965b3f11115b12ef6e61a9382c' THEN RAISE EXCEPTION 'GENERIC_DIAGNOSTIC_REVISION_CHANGED';END IF;
 SELECT details INTO v_evidence FROM population_top3000_202608_202609_v1.pipeline_event WHERE event='SOURCE_EXPLICIT_REPAIR_HOLD_ROLLBACK_VERIFIED'
  AND details->>'result'='PASS' AND details->>'owner_fence'='2' AND details->>'stop_generation'='0'
  AND details->>'scope_sha256'='8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd'
  AND details->>'tested_actual_family'='N02' AND details->>'tested_actual_partitions'='2'
  AND details->>'expected_batch_seconds'='45'
  AND details->>'tested_measurement_sha256'='88af4d4f5ea8f951e4c18b54764cf6fae742466b6de355d8fa29c200b074ad1b'
  AND details->>'tested_measurement_sha256'=encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.pipeline_measure_partition_v1(text,text)'::regprocedure),'UTF8')),'hex')
  AND jsonb_typeof(details->'dispatch_timings')='array' AND jsonb_array_length(details->'dispatch_timings')=2
  AND NOT EXISTS(SELECT 1 FROM jsonb_array_elements(details->'dispatch_timings') AS timing(value)
   WHERE jsonb_typeof(timing.value->'elapsed_ms') IS DISTINCT FROM 'number' OR (timing.value->>'elapsed_ms')::numeric>45000 OR (timing.value->>'elapsed_ms')::numeric<0)
  AND details->>'tested_function_sha256'=v_hash
  AND details->>'generic_repair_sha256'='b8663b7bde533cfb33c50b092bb0200f16bb83965b3f11115b12ef6e61a9382c'
  ORDER BY at DESC LIMIT 1;
 IF v_evidence IS NULL THEN RAISE EXCEPTION 'ACTUAL_CURRENT_CODE_SOURCE_QUALITY_ROLLBACK_EVIDENCE_REQUIRED';END IF;
 INSERT INTO population_top3000_202608_202609_v1.pipeline_adapter
  (family_id,stage,function_oid,function_sha256,resource_class,provider,enabled,reviewed_test_evidence,producer_revision,expected_batch_seconds)
 SELECT family_id,'REPAIR','population_top3000_202608_202609_v1.source_quality_diagnose_and_hold_v1(uuid,text)'::regprocedure::oid,
  v_hash,'DATABASE',NULL,true,v_evidence,'source-diagnose-and-hold-20261023-v1',45
 FROM (VALUES ('N01'),('N02'),('D02')) families(family_id)
 ON CONFLICT(family_id,stage) DO UPDATE SET expected_batch_seconds=EXCLUDED.expected_batch_seconds,reviewed_test_evidence=EXCLUDED.reviewed_test_evidence
 WHERE pipeline_adapter.function_oid=EXCLUDED.function_oid AND pipeline_adapter.function_sha256=EXCLUDED.function_sha256
  AND pipeline_adapter.resource_class='DATABASE' AND pipeline_adapter.provider IS NULL AND pipeline_adapter.enabled;
 IF (SELECT count(*) FROM population_top3000_202608_202609_v1.pipeline_adapter WHERE family_id IN('N01','N02','D02') AND stage='REPAIR'
  AND function_oid='population_top3000_202608_202609_v1.source_quality_diagnose_and_hold_v1(uuid,text)'::regprocedure::oid
  AND function_sha256=v_hash AND resource_class='DATABASE' AND provider IS NULL AND enabled AND expected_batch_seconds=45)<>3
 THEN RAISE EXCEPTION 'EXISTING_SOURCE_QUALITY_ADAPTER_CONFLICT';END IF;
 UPDATE population_top3000_202608_202609_v1.pipeline_task t SET status='READY',not_before=clock_timestamp(),blocker_code=NULL,blocker_until=NULL,resumption_condition=NULL
  WHERE t.stage='REPAIR' AND t.status='BLOCKED' AND t.blocker_code='ADAPTER_NOT_INSTALLED'
   AND t.input_versions->>'scope_sha256'='8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd'
   AND EXISTS(SELECT 1 FROM (VALUES ('N01','news-published-202608-202609'),('N02','dividends-exdate-202608-202609'),
    ('N02','dividends-declaration-202608-202609'),('D02','grouped-20260803-raw-no-otc')) routes(family_id,partition_key)
    WHERE routes.family_id=t.family_id AND routes.partition_key=t.partition_key);
 GET DIAGNOSTICS v_released=ROW_COUNT;
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES('SOURCE_EXPLICIT_REPAIR_HOLD_ADMITTED',
  jsonb_build_object('owner_fence',2,'stop_generation',0,'families',jsonb_build_array('N01','N02','D02'),'stage','REPAIR','function_sha256',v_hash,
   'adapter_missing_holds_released',v_released,'other_blocked_tasks_unchanged',true,'source_rows_modified',0,'provider_requests_admitted',0,
   'company_certification_admitted',false,'publication_admitted',false,'generic_repair_pinned',true,'expected_batch_seconds',45,
   'reviewed_runtime_basis','Unchanged full measurement: N02 ex-date 29467ms; actual wrapper rollback timings attached.',
   'tested_measurement_sha256',v_evidence->>'tested_measurement_sha256','dispatch_timings',v_evidence->'dispatch_timings'));
 PERFORM population_top3000_202608_202609_v1.pipeline_wake_v1('top3000-recovery-20261023-root-4b1314bcb5c2',2,'SOURCE_EXPLICIT_REPAIR_HOLD_ADMITTED');
END $admit$;
COMMIT;
