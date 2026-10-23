-- Root-only admission after the two ACTUAL rollback receipts are persisted.
-- Installs12 database adapters and parks one retained-source task capped at25.
-- Does not enable cron, increase allowances, fetch data or certify the cohort.
BEGIN;
SET LOCAL lock_timeout='3s';SET LOCAL statement_timeout='25s';
SELECT pg_advisory_xact_lock(2026083000261022);
DO $admit$
DECLARE bundle text;math_hash text;fnmap jsonb;numeric_receipt jsonb;producer_receipt jsonb;
 prepared jsonb;rid uuid;vers jsonb;tid uuid;f text;st text;fn regprocedure;
BEGIN
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.acquisition_control c WHERE c.singleton
  AND c.owner_id='top3000-recovery-20261023-root-4b1314bcb5c2' AND c.owner_fence=2 AND c.stop_generation=0
  AND c.enabled AND NOT c.stopped AND c.hold_reason IS NULL AND c.admitted_until>clock_timestamp())
 THEN RAISE EXCEPTION 'DAILY_SHARED_OWNER_STOP_OR_ADMISSION_CHANGED';END IF;
 IF EXISTS(SELECT 1 FROM cron.job WHERE jobid=596 AND active)
  OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE status='LEASED')
  OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_provider_lease WHERE released_at IS NULL)
 THEN RAISE EXCEPTION 'DAILY_SHARED_REGISTRATION_REQUIRES_PAUSED_UNLEASED_QUEUE';END IF;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_control p WHERE p.singleton
  AND p.shared_scheduler_verified AND p.deadline>clock_timestamp() AND p.max_ticks-p.tick_count>=2)
 THEN RAISE EXCEPTION 'DAILY_SHARED_FIRST_BATCH_TICK_OR_DEADLINE_HEADROOM_REQUIRED';END IF;
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_adapter WHERE family_id IN('D03','D04','D05'))
  OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE family_id IN('D03','D04','D05'))
  OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.daily_shared_run_v1 WHERE run_key='retained-shared-daily-v1')
 THEN RAISE EXCEPTION 'DAILY_SHARED_EXISTING_WORK_REQUIRES_RECONCILIATION';END IF;
 PERFORM population_top3000_202608_202609_v1.d02_daily_resource_guard_v2();
 bundle:=population_top3000_202608_202609_v1.daily_shared_bundle_sha256_v1();
 math_hash:=encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.daily_shared_math_v1(jsonb)'::regprocedure),'UTF8')),'hex');
 SELECT jsonb_object_agg(f.family_id||':'||s.stage,encode(sha256(convert_to(pg_get_functiondef(
  ('population_top3000_202608_202609_v1.'||CASE WHEN s.stage='NORMALIZE'THEN 'daily_shared_normalize_v1'
   WHEN s.stage='DERIVE'THEN 'daily_shared_derive_v1'ELSE 'daily_shared_quality_hold_v1'END||'(uuid,text)')::regprocedure),'UTF8')),'hex'))
 INTO fnmap FROM(VALUES('D03'),('D04'),('D05'))f(family_id)CROSS JOIN(VALUES('NORMALIZE'),('DERIVE'),('VALIDATE'),('REPAIR'))s(stage);
 SELECT e.details INTO numeric_receipt FROM population_top3000_202608_202609_v1.pipeline_event e
 WHERE e.event='DAILY_SHARED_MATH_FIXTURES_VERIFIED' AND e.details->>'result'='PASS_INDEPENDENT_NUMERIC_FIXTURES'
  AND e.details->>'calculator_sha256'=math_hash AND e.details->>'cases'='8' AND e.details->>'target_rows_checked'='336'
  AND e.details->'future_causality_test'='true'::jsonb AND e.details->'extreme_range_guards'='true'::jsonb
  AND e.details->'adjusted_intermediate_gap_rejected'='true'::jsonb
  AND e.details->'gradual_horizon_range_guard'='true'::jsonb
  AND e.details->'provider_called'='false'::jsonb AND e.details->'source_accuracy_or_company_completeness_claimed'='false'::jsonb
 ORDER BY e.at DESC LIMIT 1;
 IF numeric_receipt IS NULL THEN RAISE EXCEPTION 'DAILY_SHARED_ACTUAL_CURRENT_NUMERIC_FIXTURE_EVIDENCE_REQUIRED';END IF;
 SELECT e.details INTO producer_receipt FROM population_top3000_202608_202609_v1.pipeline_event e
 WHERE e.event='DAILY_SHARED_RETAINED26_ROLLBACK_VERIFIED' AND e.details->>'result'='PASS_ACTUAL_RETAINED26_ROLLBACK'
  AND e.details->>'shared_daily_bundle_sha256'=bundle AND e.details->>'calculator_sha256'=math_hash
  AND e.details->'tested_function_sha256'=fnmap AND e.details->>'source_series'='26'
  AND e.details->>'input_grid_rows'='2678' AND e.details->>'target_output_rows'='1092'
  AND e.details->>'source_manifest_sha256'=encode(sha256(convert_to(population_top3000_202608_202609_v1.daily_shared_source_manifest_v1()::text,'UTF8')),'hex')
  AND e.details->'first25_cap_exercised'='true'::jsonb AND e.details->'input_output_exact_replay_exercised'='true'::jsonb
  AND e.details->'old_producers_sources_governance_and_universe_unchanged'='true'::jsonb
  AND e.details->'normal_commit_progress_timestamps_excluded_from_governance_comparison'='true'::jsonb
  AND e.details->'provider_called'='false'::jsonb AND e.details->'company_population_verified'='false'::jsonb
  AND e.details->'provider_accuracy_verified'='false'::jsonb AND e.details->'full_current_output_independent_reconciliation_pending'='true'::jsonb
 ORDER BY e.at DESC LIMIT 1;
 IF producer_receipt IS NULL THEN RAISE EXCEPTION 'DAILY_SHARED_ACTUAL_CURRENT_RETAINED26_EVIDENCE_REQUIRED';END IF;
 prepared:=population_top3000_202608_202609_v1.daily_shared_prepare_run_v1('retained-shared-daily-v1');
 rid:=(prepared->>'run_id')::uuid;vers:=prepared->'input_versions';
 IF(prepared->>'source_series')::integer<25 THEN RAISE EXCEPTION 'DAILY_SHARED_FIRST_BATCH_REQUIRES_AT_LEAST25_CENSUS_LINES';END IF;
 FOREACH f IN ARRAY ARRAY['D03','D04','D05']LOOP
  FOREACH st IN ARRAY ARRAY['NORMALIZE','DERIVE','VALIDATE','REPAIR']LOOP
   fn:=('population_top3000_202608_202609_v1.'||CASE WHEN st='NORMALIZE'THEN 'daily_shared_normalize_v1'
    WHEN st='DERIVE'THEN 'daily_shared_derive_v1'ELSE 'daily_shared_quality_hold_v1'END||'(uuid,text)')::regprocedure;
   INSERT INTO population_top3000_202608_202609_v1.pipeline_adapter(family_id,stage,function_oid,function_sha256,resource_class,provider,enabled,reviewed_test_evidence,producer_revision,expected_batch_seconds)
   VALUES(f,st,fn::oid,fnmap->>(f||':'||st),'DATABASE',NULL,true,
    jsonb_build_object('numeric_test',numeric_receipt,'actual_retained_test',producer_receipt,'initial_admission_series',25,
     'source_requests',0,'shared_storage_for_three_families',true,'company_population_verified',false,
     'requires_first_live_batch_resource_review',true),'retained-shared-daily-v1',20);
  END LOOP;
 END LOOP;
 tid:=population_top3000_202608_202609_v1.pipeline_enqueue_v1('D03','NORMALIZE','retained-shared-daily-v1',
  jsonb_build_object('kind','RETAINED_SHARED_DAILY_V1','run_id',rid,'max_series',25),vers);
 UPDATE population_top3000_202608_202609_v1.pipeline_task SET not_before='infinity' WHERE task_id=tid;
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(task_id,event,details)
 VALUES(tid,'DAILY_SHARED_FIRST25_REGISTERED',prepared||jsonb_build_object('shared_daily_bundle_sha256',bundle,
  'calculator_sha256',math_hash,'tested_function_sha256',fnmap,'initial_admission_series',25,'initial_input_grid_rows',2575,
  'registered_adapter_count',12,'task_parked_pending_controller_release',true,'source_requests',0,'http_requests',0,
  'new_budget_or_cadence_change',false,'source_candidate_series_are_not_company_denominator',true,
  'historical_receipt_observed',false,'production_asof_eligible',false));
END $admit$;
SELECT task_id,family_id,stage,status,not_before,executions,payload FROM population_top3000_202608_202609_v1.pipeline_task WHERE partition_key='retained-shared-daily-v1';COMMIT;
