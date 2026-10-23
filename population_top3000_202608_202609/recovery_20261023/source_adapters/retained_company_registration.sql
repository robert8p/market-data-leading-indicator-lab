-- Root-only: execute after observing the real rollback test PASS and persisting its receipt.
-- Required event: RETAINED_COMPANY_ROLLBACK_VERIFIED with result, source_sha256,
-- test_file_sha256, owner_fence=2, and function_sha256={ACQUIRE:hash,NORMALIZE:hash}.
-- This script never generates its own PASS receipt. It admits exactly one retained NVDA source task.
BEGIN;
SET LOCAL lock_timeout='3s';
SELECT pg_advisory_xact_lock(2026083000261022);
DO $admit$
DECLARE v_evidence jsonb;v_hashes jsonb;v_task_id uuid;v_payload jsonb;v_inputs jsonb;
BEGIN
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton
  AND owner_id='top3000-recovery-20261023-root-4b1314bcb5c2' AND owner_fence=2 AND stop_generation=0
  AND enabled AND NOT stopped AND hold_reason IS NULL AND admitted_until>clock_timestamp()
  AND scope_sha256='8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd'
  AND provider_requests=max_provider_requests AND http_invocations=max_http_invocations) THEN RAISE EXCEPTION 'RETAINED_CONTROLLER_OR_FROZEN_PROVIDER_LIMITS_CHANGED';END IF;
 IF EXISTS(SELECT 1 FROM cron.job WHERE jobid=596 AND active) THEN RAISE EXCEPTION 'PAUSE_CRON_FOR_ADMISSION';END IF;
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_provider_lease WHERE released_at IS NULL)
  OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_adapter WHERE enabled AND resource_class<>'DATABASE') THEN RAISE EXCEPTION 'DATABASE_ONLY_ADMISSION_REQUIRED';END IF;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton
  AND expected_owner_id='top3000-recovery-20261023-root-4b1314bcb5c2' AND expected_owner_fence=2 AND expected_stop_generation=0
  AND shared_scheduler_verified AND budget_verified_at IS NOT NULL
  AND budget_evidence->>'admission_class'='RETAINED_DATABASE_ONLY'
  AND budget_evidence->>'provider_admissions_allowed'='false' AND budget_evidence->>'external_http_calls_allowed'='false') THEN RAISE EXCEPTION 'RETAINED_ONLY_BUDGET_ADMISSION_REQUIRED';END IF;
 SELECT jsonb_object_agg(v.stage,encode(sha256(convert_to(pg_get_functiondef(v.fn::regprocedure),'UTF8')),'hex')) INTO v_hashes
 FROM (VALUES('ACQUIRE','population_top3000_202608_202609_v1.company_retained_acquire_v1(uuid,text)'),
  ('NORMALIZE','population_top3000_202608_202609_v1.company_reconcile_v1(uuid,text)')) v(stage,fn);
 SELECT e.details INTO v_evidence FROM population_top3000_202608_202609_v1.pipeline_event e
 WHERE e.event='RETAINED_COMPANY_ROLLBACK_VERIFIED' AND e.details->>'result'='PASS' AND e.details->>'owner_fence'='2'
  AND e.details->>'source_sha256'='682c49d59116ae49b317eb4adddf3ef9142dfd97f59c5d8e5a5acad7f9058ab4'
  AND e.details->>'test_file_sha256'='c7b0a445e08562045627d9dc16dc2ffa37c37b2cb8175301594e4e57720ab919'
  AND e.details->'function_sha256'=v_hashes ORDER BY e.at DESC LIMIT 1;
 IF v_evidence IS NULL THEN RAISE EXCEPTION 'ACTUAL_MATCHING_RETAINED_COMPANY_ROLLBACK_EVIDENCE_REQUIRED';END IF;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.sec_source_manifest_v1
  WHERE source_id='NVDA_SUBMISSIONS' AND raw_state='COMPLETE_VERIFIED' AND expected_parts=2 AND source_bytes=159761
   AND source_sha256='682c49d59116ae49b317eb4adddf3ef9142dfd97f59c5d8e5a5acad7f9058ab4') THEN RAISE EXCEPTION 'VERIFIED_NVDA_RETAINED_MANIFEST_CHANGED';END IF;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_adapter WHERE family_id='REF01' AND stage='NORMALIZE'
  AND enabled AND resource_class='DATABASE' AND function_oid='population_top3000_202608_202609_v1.company_reconcile_v1(uuid,text)'::regprocedure::oid
  AND function_sha256=v_hashes->>'NORMALIZE') THEN RAISE EXCEPTION 'TESTED_COMPANY_NORMALIZER_BINDING_CHANGED';END IF;
 IF (SELECT pg_get_userbyid(proowner) FROM pg_proc WHERE oid='population_top3000_202608_202609_v1.company_retained_acquire_v1(uuid,text)'::regprocedure) IS DISTINCT FROM 'postgres' THEN RAISE EXCEPTION 'PRIVATE_RUNTIME_FUNCTION_OWNER_CHANGED';END IF;

 REVOKE EXECUTE ON FUNCTION population_top3000_202608_202609_v1.company_retained_acquire_v1(uuid,text) FROM PUBLIC,anon,authenticated;
 -- Retain the private runtime's explicit service-role convention; do not inherit PUBLIC access.
 IF EXISTS(SELECT 1 FROM pg_proc rp CROSS JOIN LATERAL aclexplode(coalesce(rp.proacl,acldefault('f',rp.proowner))) acl_rows
  JOIN pg_roles grantee_role ON grantee_role.oid=acl_rows.grantee
  WHERE rp.oid='population_top3000_202608_202609_v1.company_reconcile_v1(uuid,text)'::regprocedure
   AND grantee_role.rolname='service_role' AND acl_rows.privilege_type='EXECUTE') THEN
  GRANT EXECUTE ON FUNCTION population_top3000_202608_202609_v1.company_retained_acquire_v1(uuid,text) TO service_role;
 END IF;
 INSERT INTO population_top3000_202608_202609_v1.pipeline_adapter(family_id,stage,function_oid,function_sha256,resource_class,enabled,reviewed_test_evidence,producer_revision,expected_batch_seconds)
 VALUES('REF01','ACQUIRE','population_top3000_202608_202609_v1.company_retained_acquire_v1(uuid,text)'::regprocedure::oid,
  v_hashes->>'ACQUIRE','DATABASE',true,v_evidence,'retained-company-nvda-recovery-20261023',10)
 ON CONFLICT(family_id,stage) DO NOTHING;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_adapter WHERE family_id='REF01' AND stage='ACQUIRE'
  AND enabled AND resource_class='DATABASE' AND function_oid='population_top3000_202608_202609_v1.company_retained_acquire_v1(uuid,text)'::regprocedure::oid
  AND function_sha256=v_hashes->>'ACQUIRE') THEN RAISE EXCEPTION 'EXISTING_COMPANY_ACQUIRE_ADAPTER_CONFLICT';END IF;
 v_payload:='{"kind":"SEC_SUBMISSIONS","cik":"0001045810","source_id":"company:sec-submissions:0001045810","retained_source_id":"NVDA_SUBMISSIONS","retained_only":true,"provisional":true}'::jsonb;
 v_inputs:='{"source_sha256":"682c49d59116ae49b317eb4adddf3ef9142dfd97f59c5d8e5a5acad7f9058ab4","retained_source_id":"NVDA_SUBMISSIONS","scope_sha256":"8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd"}'::jsonb;
 v_task_id:=population_top3000_202608_202609_v1.pipeline_enqueue_v1('REF01','ACQUIRE','issuer:0001045810',v_payload,v_inputs);
 UPDATE population_top3000_202608_202609_v1.company_source_request_v1 SET state='QUEUED',task_id=v_task_id,updated_at=clock_timestamp()
 WHERE request_key='issuer:0001045810' AND source_id='company:sec-submissions:0001045810' AND state='PENDING_ADMISSION';
 UPDATE population_top3000_202608_202609_v1.pipeline_control
 SET budget_evidence=jsonb_set(budget_evidence,'{allowed_work}',coalesce(budget_evidence->'allowed_work','[]'::jsonb)||
  jsonb_build_array('One pinned retained NVDA submissions source import and existing company normalization; exact missing filing documents block without network calls')),
  next_wakeup_at=clock_timestamp() WHERE singleton;
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,task_id,details)
 VALUES('RETAINED_COMPANY_SOURCE_ADMITTED',v_task_id,v_evidence||jsonb_build_object('source_id','company:sec-submissions:0001045810',
  'owner_id','top3000-recovery-20261023-root-4b1314bcb5c2','owner_fence',2,'explicit_source_tasks_admitted',1,
  'remaining_issuer_requests_not_admitted',true,'external_fetch_admitted',false,'maximum_additional_copy_bytes',159761));
 PERFORM population_top3000_202608_202609_v1.pipeline_wake_v1('top3000-recovery-20261023-root-4b1314bcb5c2',2,'TESTED_ONE_RETAINED_NVDA_SOURCE');
END $admit$;
COMMIT;
