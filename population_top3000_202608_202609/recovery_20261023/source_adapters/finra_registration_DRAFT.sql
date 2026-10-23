-- Root ONLY, after full DB rollback test + local worker test + deployed-code review.
-- Requires an actual FINRA_ADAPTER_REVIEWED event, never a fabricated test receipt.
-- Registers just the Aug 3 pilot. Remaining 41 files require first-file reconciliation.
BEGIN;
SET LOCAL lock_timeout='3s';
SELECT pg_advisory_xact_lock(2026083000261022);
DO $admit$
DECLARE proof jsonb;c record;bundle text;tid uuid;
BEGIN
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton
  AND owner_id='top3000-recovery-20261023-root-4b1314bcb5c2' AND owner_fence=2 AND stop_generation=0
  AND enabled AND NOT stopped AND hold_reason IS NULL AND admitted_until>clock_timestamp()) THEN RAISE EXCEPTION 'OWNER_STOP_OR_ADMISSION_CHANGED';END IF;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton AND enabled AND shared_scheduler_verified
  AND budget_evidence->>'provider_admissions_allowed'='true' AND budget_evidence->'allowed_providers' ? 'FINRA'
  AND (budget_evidence#>>'{resource_limits,max_relation_bytes}')::bigint<=2147483648)
 THEN RAISE EXCEPTION 'FINRA_RESOURCE_ADMISSION_REQUIRED';END IF;
 bundle:=population_top3000_202608_202609_v1.finra_code_hash_v1();
 SELECT details INTO proof FROM population_top3000_202608_202609_v1.pipeline_event
 WHERE event='FINRA_ADAPTER_REVIEWED' AND details->>'db_rollback_result'='PASS'
  AND details->>'worker_local_test_result'='PASS' AND details->>'finra_bundle_sha256'=bundle
  AND details->>'edge_worker_sha256'~'^[0-9a-f]{64}$' AND details->>'deployed_worker'='population-top3000-evidence-v1'
 ORDER BY at DESC LIMIT 1;
 IF proof IS NULL THEN RAISE EXCEPTION 'ACTUAL_FINRA_TEST_AND_DEPLOYMENT_EVIDENCE_REQUIRED';END IF;
 IF (SELECT count(*) FROM population_top3000_202608_202609_v1.catalog_calendar WHERE session_date BETWEEN '2026-08-03' AND '2026-09-30')<>42
 THEN RAISE EXCEPTION 'FROZEN_42_SESSION_CALENDAR_REQUIRED';END IF;
 INSERT INTO population_top3000_202608_202609_v1.pipeline_adapter
 (family_id,stage,function_oid,function_sha256,resource_class,provider,enabled,reviewed_test_evidence,producer_revision,expected_batch_seconds)
 SELECT 'S01',stage,fn::regprocedure::oid,encode(sha256(convert_to(pg_get_functiondef(fn::regprocedure),'UTF8')),'hex'),resource,
  CASE WHEN resource='PROVIDER' THEN 'FINRA' END,true,proof,'finra-cnms-v1',45
 FROM (VALUES('ACQUIRE','population_top3000_202608_202609_v1.finra_acquire_v1(uuid,text)','PROVIDER'),
 ('NORMALIZE','population_top3000_202608_202609_v1.finra_normalize_v1(uuid,text)','DATABASE'),
 ('DERIVE','population_top3000_202608_202609_v1.finra_derive_v1(uuid,text)','DATABASE'))v(stage,fn,resource)
 ON CONFLICT(family_id,stage) DO NOTHING;
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_adapter WHERE family_id='S01' AND stage IN('ACQUIRE','NORMALIZE','DERIVE')
  AND (NOT enabled OR reviewed_test_evidence->>'finra_bundle_sha256' IS DISTINCT FROM bundle)) THEN RAISE EXCEPTION 'EXISTING_S01_BINDING_CONFLICT';END IF;
 SELECT * INTO c FROM population_top3000_202608_202609_v1.catalog_calendar WHERE session_date='2026-08-03';
 tid:=population_top3000_202608_202609_v1.pipeline_enqueue_v1('S01','ACQUIRE','CNMS:'||c.session_date::text,
  jsonb_build_object('kind','FINRA_CNMS_DAILY','session_date',c.session_date,'provisional',true),
  jsonb_build_object('calendar_record_sha256',c.source_record_sha256,'finra_bundle_sha256',bundle,'scope_sha256','8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd'));
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(task_id,event,details)
 VALUES(tid,'FINRA_FIRST_FILE_ADMITTED',jsonb_build_object('date',c.session_date,'calendar_sessions',42,'remaining_sessions_enqueued',false,
  'finra_bundle_sha256',bundle,'company_universe_frozen',false,'provider_incremental_request_usd',0,'invoice_reconciled',false));
END $admit$;
COMMIT;
