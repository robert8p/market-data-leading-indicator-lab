-- Root-only second wave. The first real date and retained Aug3 must have completed
-- bounded typed reconciliation, with actual storage/runtime evidence reviewed by root.
-- Forty remaining source-day tasks are parked; no implicit resume or budget increase.
BEGIN;
SET LOCAL lock_timeout='3s';SET LOCAL statement_timeout='10s';
SELECT pg_advisory_xact_lock(2026083000261022);
DO $expand$
DECLARE cal jsonb;vers jsonb;bundle text;e jsonb;d date;tid uuid;task_ids uuid[]:='{}';n integer:=0;
BEGIN
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton AND owner_id='top3000-recovery-20261023-root-4b1314bcb5c2' AND owner_fence=2 AND stop_generation=0 AND enabled AND NOT stopped AND hold_reason IS NULL AND admitted_until>clock_timestamp()) THEN RAISE EXCEPTION 'OWNER_STOP_OR_ADMISSION_CHANGED';END IF;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton AND shared_scheduler_verified AND budget_evidence->>'provider_admissions_allowed'='true' AND budget_evidence->'allowed_providers' ? 'Massive') THEN RAISE EXCEPTION 'CURRENT_MASSIVE_RESOURCE_ADMISSION_REQUIRED';END IF;
 PERFORM population_top3000_202608_202609_v1.d02_daily_resource_guard_v2();
 bundle:=population_top3000_202608_202609_v1.d02_daily_bundle_sha256_v2();cal:=population_top3000_202608_202609_v1.d02_daily_calendar_v2();
 SELECT details INTO e FROM population_top3000_202608_202609_v1.pipeline_event WHERE event='D02_DAILY_V2_INITIAL_SOURCE_AND_STORAGE_REVIEWED' AND details->>'result'='PASS' AND details->>'daily_bundle_sha256'=bundle AND details->>'calendar_manifest_sha256'=cal->>'calendar_manifest_sha256' AND details->>'first_two_sources_reconciled'='true' AND details->>'projected_storage_within_current_admission'='true' AND details->>'bounded_batch_runtime_verified'='true' AND details->>'company_population_verified'='false' ORDER BY at DESC LIMIT 1;
 IF e IS NULL THEN RAISE EXCEPTION 'ACTUAL_FIRST_WAVE_SOURCE_STORAGE_AND_RUNTIME_REVIEW_REQUIRED';END IF;
 IF (SELECT count(*) FROM population_top3000_202608_202609_v1.d02_daily_source_v2 WHERE session_date IN(date '2026-08-03',date '2026-08-04') AND source_verified_at IS NOT NULL AND verified_source_records=result_count AND result_count>0 AND input_versions->>'daily_bundle_sha256'=bundle)<>2 THEN RAISE EXCEPTION 'FIRST_TWO_REAL_SOURCE_DAYS_NOT_RECONCILED';END IF;
 SELECT input_versions INTO vers FROM population_top3000_202608_202609_v1.d02_daily_source_v2 WHERE session_date='2026-08-04';
 IF vers->>'calendar_manifest_sha256' IS DISTINCT FROM cal->>'calendar_manifest_sha256' OR vers->>'fixture_only'='true' THEN RAISE EXCEPTION 'INITIAL_REAL_SOURCE_VERSION_MISMATCH';END IF;
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE family_id='D02' AND payload->>'kind'='MASSIVE_GROUPED_DAILY_V2' AND (payload->>'trading_date')::date NOT IN(date '2026-08-03',date '2026-08-04')) THEN RAISE EXCEPTION 'EXPANSION_ALREADY_STARTED_RECONCILE_PROGRESS';END IF;
 FOR d IN SELECT (x->>'date')::date FROM jsonb_array_elements(cal->'sessions') x WHERE (x->>'date')::date NOT IN(date '2026-08-03',date '2026-08-04') ORDER BY x->>'date' LOOP
  tid:=population_top3000_202608_202609_v1.pipeline_enqueue_v1('D02','ACQUIRE','daily-source-'||to_char(d,'YYYYMMDD')||'-raw-no-otc-v2',jsonb_build_object('kind','MASSIVE_GROUPED_DAILY_V2','trading_date',d,'source_mode','FETCH'),vers);
  task_ids:=array_append(task_ids,tid);n:=n+1;
 END LOOP;
 IF n<>40 THEN RAISE EXCEPTION 'FROZEN_REMAINING_SESSION_COUNT_CHANGED';END IF;
 UPDATE population_top3000_202608_202609_v1.pipeline_task SET not_before='infinity' WHERE task_id=ANY(task_ids);
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES('D02_DAILY_V2_REMAINING_SOURCE_DATES_REGISTERED',jsonb_build_object('owner_fence',2,'stop_generation',0,'daily_bundle_sha256',bundle,'calendar_manifest_sha256',cal->>'calendar_manifest_sha256','remaining_dates',40,'maximum_additional_provider_requests',40,'first_wave_review',e,'no_new_subscription',true,'budget_increased',false,'requires_controller_release',true,'source_superset_not_certified_company_population',true));
END $expand$;
COMMIT;
