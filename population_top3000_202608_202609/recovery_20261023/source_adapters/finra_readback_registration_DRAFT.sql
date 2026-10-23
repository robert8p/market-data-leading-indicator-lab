-- Root-only admission after actual rollback PASS for this exact new helper.
-- Registers one stage and releases only its ADAPTER_NOT_INSTALLED holds. No source, budget or scope changes.
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
 v_hash:=encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.finra_independent_validate_v1(uuid,text)'::regprocedure),'UTF8')),'hex');
 SELECT details INTO v_evidence FROM population_top3000_202608_202609_v1.pipeline_event WHERE event='FINRA_INDEPENDENT_READBACK_ROLLBACK_VERIFIED'
  AND details->>'result'='PASS' AND details->>'owner_fence'='2' AND details->>'stop_generation'='0'
  AND details->>'tested_function_sha256'=v_hash ORDER BY at DESC LIMIT 1;
 IF v_evidence IS NULL THEN RAISE EXCEPTION 'ACTUAL_CURRENT_CODE_QUALITY_ROLLBACK_EVIDENCE_REQUIRED';END IF;
 INSERT INTO population_top3000_202608_202609_v1.pipeline_adapter
  (family_id,stage,function_oid,function_sha256,resource_class,provider,enabled,reviewed_test_evidence,producer_revision,expected_batch_seconds)
 VALUES('S01','VALIDATE','population_top3000_202608_202609_v1.finra_independent_validate_v1(uuid,text)'::regprocedure::oid,
  v_hash,'DATABASE',NULL,true,v_evidence,'finra-regex-independent-readback-v1',5)
 ON CONFLICT(family_id,stage) DO NOTHING;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_adapter WHERE family_id='S01' AND stage='VALIDATE'
  AND function_oid='population_top3000_202608_202609_v1.finra_independent_validate_v1(uuid,text)'::regprocedure::oid
  AND function_sha256=v_hash AND resource_class='DATABASE' AND provider IS NULL AND enabled)
 THEN RAISE EXCEPTION 'EXISTING_QUALITY_ADAPTER_CONFLICT';END IF;
 UPDATE population_top3000_202608_202609_v1.pipeline_task SET status='READY',not_before=clock_timestamp(),blocker_code=NULL,blocker_until=NULL,resumption_condition=NULL
  WHERE family_id='S01' AND stage='VALIDATE' AND status='BLOCKED' AND blocker_code='ADAPTER_NOT_INSTALLED'
   AND input_versions->>'scope_sha256'='8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd';
 GET DIAGNOSTICS v_released=ROW_COUNT;
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES('FINRA_INDEPENDENT_READBACK_ADMITTED',
  jsonb_build_object('owner_fence',2,'stop_generation',0,'family','S01','stage','VALIDATE','function_sha256',v_hash,
   'adapter_missing_holds_released',v_released,'other_blocked_tasks_unchanged',true,'source_rows_modified',0,
   'provider_requests_admitted',0,'company_certification_admitted',false,'publication_admitted',false));
 PERFORM population_top3000_202608_202609_v1.pipeline_wake_v1('top3000-recovery-20261023-root-4b1314bcb5c2',2,'FINRA_INDEPENDENT_READBACK_ADMITTED');
END $admit$;
COMMIT;
