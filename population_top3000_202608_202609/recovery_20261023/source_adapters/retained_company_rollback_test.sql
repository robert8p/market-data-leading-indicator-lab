-- DRAFT. Run after installing retained_company_adapter.sql, with cron 596 paused.
-- Exercises actual dispatcher -> retained acquire -> existing SEC_SUBMISSIONS normalizer.
-- All task/source/scheduler/accounting changes roll back. No external adapter is used.
BEGIN;
SET LOCAL lock_timeout='3s';
SET LOCAL statement_timeout='50s';
SELECT pg_advisory_xact_lock(2026083000261022);
DO $test$
DECLARE
 v_result jsonb; v_acquire uuid; v_normalize uuid; v_replay uuid; v_missing uuid;
 v_hash text; v_payload jsonb; v_inputs jsonb; v_stored bigint; v_provider bigint; v_http bigint;
 v_had_source boolean; v_expected_copy bigint; v_missing_exec integer; v_bad_task uuid;
 v_document_count bigint; v_expected_sha constant text:='682c49d59116ae49b317eb4adddf3ef9142dfd97f59c5d8e5a5acad7f9058ab4';
BEGIN
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton
  AND owner_id='top3000-recovery-20261023-root-4b1314bcb5c2' AND owner_fence=2 AND stop_generation=0
  AND enabled AND NOT stopped AND hold_reason IS NULL AND admitted_until>clock_timestamp()) THEN RAISE EXCEPTION 'CONTROLLER_NOT_ADMITTED';END IF;
 IF EXISTS(SELECT 1 FROM cron.job WHERE jobid=596 AND active) THEN RAISE EXCEPTION 'PAUSE_CRON_FIRST';END IF;
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE status='LEASED')
  OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_provider_lease WHERE released_at IS NULL) THEN RAISE EXCEPTION 'LIVE_LEASES_MUST_BE_RECONCILED_FIRST';END IF;
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_adapter WHERE enabled AND resource_class<>'DATABASE') THEN RAISE EXCEPTION 'DATABASE_ONLY_ADAPTERS_REQUIRED';END IF;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton AND shared_scheduler_verified
  AND budget_verified_at IS NOT NULL AND budget_evidence->>'provider_admissions_allowed'='false' AND deadline>clock_timestamp()) THEN RAISE EXCEPTION 'RETAINED_ONLY_BUDGET_ADMISSION_REQUIRED';END IF;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.sec_source_manifest_v1 WHERE source_id='NVDA_SUBMISSIONS' AND raw_state='COMPLETE_VERIFIED' AND source_sha256=v_expected_sha)
  OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.company_group_v1 WHERE group_key='CIK:0001045810') THEN RAISE EXCEPTION 'VERIFIED_NVDA_SOURCE_AND_PROVISIONAL_GROUP_REQUIRED';END IF;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_adapter WHERE family_id='REF01' AND stage='NORMALIZE' AND enabled
  AND function_oid='population_top3000_202608_202609_v1.company_reconcile_v1(uuid,text)'::regprocedure::oid) THEN RAISE EXCEPTION 'EXISTING_COMPANY_NORMALIZER_REQUIRED';END IF;
 SELECT stored_bytes,provider_requests,http_invocations INTO v_stored,v_provider,v_http FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton;
 SELECT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.source_document WHERE source_id='company:sec-submissions:0001045810') INTO v_had_source;
 v_expected_copy:=CASE WHEN v_had_source THEN 0 ELSE 159761 END;
 SELECT count(*) INTO v_document_count FROM population_top3000_202608_202609_v1.source_document;
 v_hash:=encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.company_retained_acquire_v1(uuid,text)'::regprocedure),'UTF8')),'hex');
 INSERT INTO population_top3000_202608_202609_v1.pipeline_adapter(family_id,stage,function_oid,function_sha256,resource_class,enabled,reviewed_test_evidence,producer_revision,expected_batch_seconds)
 VALUES('REF01','ACQUIRE','population_top3000_202608_202609_v1.company_retained_acquire_v1(uuid,text)'::regprocedure::oid,v_hash,'DATABASE',true,
  '{"test":"retained_company_rollback_test.sql","status":"IN_PROGRESS_TRANSACTION_WILL_ROLL_BACK"}','retained-company-nvda-v1',10)
 ON CONFLICT(family_id,stage) DO UPDATE SET function_oid=EXCLUDED.function_oid,function_sha256=EXCLUDED.function_sha256,resource_class='DATABASE',enabled=true,
  reviewed_test_evidence=EXCLUDED.reviewed_test_evidence,producer_revision=EXCLUDED.producer_revision,expected_batch_seconds=EXCLUDED.expected_batch_seconds;
 UPDATE population_top3000_202608_202609_v1.pipeline_task SET not_before='infinity' WHERE status IN('READY','BLOCKED');
 UPDATE population_top3000_202608_202609_v1.pipeline_task SET blocker_until=NULL WHERE status='BLOCKED';
 UPDATE population_top3000_202608_202609_v1.pipeline_control SET enabled=true,next_wakeup_at=clock_timestamp() WHERE singleton;
 v_payload:='{"kind":"SEC_SUBMISSIONS","cik":"0001045810","source_id":"company:sec-submissions:0001045810","retained_source_id":"NVDA_SUBMISSIONS","retained_only":true,"provisional":true}'::jsonb;
 v_inputs:=jsonb_build_object('source_sha256',v_expected_sha,'retained_source_id','NVDA_SUBMISSIONS','scope_sha256','8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd');
 v_acquire:=population_top3000_202608_202609_v1.pipeline_enqueue_v1('REF01','ACQUIRE','rollback-retained-nvda-'||gen_random_uuid()::text,v_payload,v_inputs);
 v_result:=population_top3000_202608_202609_v1.pipeline_tick_v1();
 IF v_result#>>'{result,state}' IS DISTINCT FROM 'COMPLETE' OR (v_result->>'task_id')::uuid IS DISTINCT FROM v_acquire THEN RAISE EXCEPTION 'RETAINED_NVDA_ACQUIRE_FAILED: %',v_result;END IF;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.source_document WHERE source_id='company:sec-submissions:0001045810'
  AND source_sha256=v_expected_sha AND content_bytes=159761 AND octet_length(content_text)=159761
  AND source_sha256=encode(sha256(convert_to(content_text,'UTF8')),'hex') AND historical_received_at IS NULL
  AND evidence->>'original_historical_receipt_proven'='false') THEN RAISE EXCEPTION 'RETAINED_NVDA_DOCUMENT_OR_QUALIFICATION_FAILED';END IF;
 IF (SELECT stored_bytes FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton)<>v_stored+v_expected_copy THEN RAISE EXCEPTION 'RETAINED_COPY_BYTES_NOT_ACCOUNTED_EXACTLY_ONCE';END IF;
 v_normalize:=(v_result#>>'{result,successor_task_id}')::uuid;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=v_normalize AND family_id='REF01' AND stage='NORMALIZE' AND status='READY') THEN RAISE EXCEPTION 'RETAINED_NORMALIZE_HANDOFF_MISSING';END IF;
 v_result:=population_top3000_202608_202609_v1.pipeline_tick_v1();
 IF v_result#>>'{result,state}' IS DISTINCT FROM 'COMPLETE' OR (v_result->>'task_id')::uuid IS DISTINCT FROM v_normalize THEN RAISE EXCEPTION 'REAL_SEC_SUBMISSIONS_BRANCH_FAILED: %',v_result;END IF;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.company_group_v1 WHERE group_key='CIK:0001045810'
  AND reconciliation_state='ISSUER_SOURCE_CORROBORATED_HISTORY_PENDING' AND identity_evidence->>'sec_source_sha256'=v_expected_sha
  AND identity_evidence->>'historical_listing_proven_by_current_tickers'='false') THEN RAISE EXCEPTION 'ISSUER_CORROBORATION_OR_HISTORICAL_QUALIFICATION_MISSING';END IF;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.company_filing_candidate_v1 WHERE group_key='CIK:0001045810' AND cik='0001045810'
  AND accepted_at<='2026-08-03T20:00Z' AND source_sha256=v_expected_sha) THEN RAISE EXCEPTION 'DATED_FILING_CANDIDATES_MISSING';END IF;
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_universe WHERE status='FROZEN') THEN RAISE EXCEPTION 'RETAINED_ISSUER_SOURCE_MUST_NOT_FREEZE_UNIVERSE';END IF;
 RAISE NOTICE 'PASS retained NVDA acquire -> actual SEC_SUBMISSIONS normalizer with historical qualifications';

 -- Materialising the same source through another immutable task must not copy or charge bytes again.
 UPDATE population_top3000_202608_202609_v1.pipeline_task SET not_before='infinity' WHERE status IN('READY','BLOCKED');
 v_replay:=population_top3000_202608_202609_v1.pipeline_enqueue_v1('REF01','ACQUIRE','rollback-retained-nvda-reuse-'||gen_random_uuid()::text,v_payload,v_inputs);
 v_result:=population_top3000_202608_202609_v1.pipeline_tick_v1();
 IF v_result#>>'{result,state}' IS DISTINCT FROM 'COMPLETE' OR (v_result->>'task_id')::uuid IS DISTINCT FROM v_replay
  OR (SELECT stored_bytes FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton)<>v_stored+v_expected_copy
  OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_commit WHERE task_id=v_replay AND new_committed_units=0)
  OR (SELECT count(*) FROM population_top3000_202608_202609_v1.source_document)<>v_document_count+(CASE WHEN v_had_source THEN 0 ELSE 1 END) THEN RAISE EXCEPTION 'RETAINED_SOURCE_REUSE_NOT_IDEMPOTENT: %',v_result;END IF;
 RAISE NOTICE 'PASS retained-source reuse adds no duplicate bytes or useful-unit count';

 -- A real document dependency emitted by the existing normalizer must fail closed once.
 SELECT q.task_id,q.executions INTO v_missing,v_missing_exec FROM population_top3000_202608_202609_v1.pipeline_task q
  JOIN population_top3000_202608_202609_v1.pipeline_dependency d ON d.task_id=q.task_id
  WHERE d.prerequisite_task_id=v_normalize AND q.family_id='REF01' AND q.stage='ACQUIRE' AND q.payload->>'kind'='SEC_DOCUMENT'
   AND NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.source_document sd WHERE sd.source_id=q.payload->>'source_id' AND sd.content_text IS NOT NULL)
  ORDER BY q.task_id LIMIT 1;
 IF v_missing IS NULL THEN RAISE EXCEPTION 'MISSING_RETAINED_DOCUMENT_TEST_PRECONDITION_NOT_MET';END IF;
 UPDATE population_top3000_202608_202609_v1.pipeline_task SET not_before='infinity' WHERE status IN('READY','BLOCKED');
 UPDATE population_top3000_202608_202609_v1.pipeline_task SET status='READY',not_before=clock_timestamp() WHERE task_id=v_missing;
 v_result:=population_top3000_202608_202609_v1.pipeline_tick_v1();
 IF v_result#>>'{result,state}' IS DISTINCT FROM 'BLOCKED' OR (v_result->>'task_id')::uuid IS DISTINCT FROM v_missing
  OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=v_missing AND status='BLOCKED'
   AND blocker_code='RETAINED_COMPANY_DOCUMENT_REQUIRED' AND blocker_until IS NULL
   AND resumption_condition->>'source_id'=payload->>'source_id' AND resumption_condition->>'source_uri'=payload->>'url'
   AND resumption_condition->>'external_fetch_admitted'='false') THEN RAISE EXCEPTION 'MISSING_DOCUMENT_DID_NOT_BLOCK_WITH_EXACT_REQUIREMENT: %',v_result;END IF;
 PERFORM population_top3000_202608_202609_v1.pipeline_tick_v1();
 IF (SELECT executions FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=v_missing)<>v_missing_exec+1 THEN RAISE EXCEPTION 'MISSING_DOCUMENT_RETRIED_AUTOMATICALLY';END IF;
 RAISE NOTICE 'PASS missing exact filing blocks once without external work or automatic recheck';

 -- Existing COMPLETE_VERIFIED metadata must not hide a damaged retained body.
 BEGIN
  UPDATE population_top3000_202608_202609_v1.sec_source_chunk_v1 SET raw_text=raw_text||' ',chunk_bytes=chunk_bytes+1,
   chunk_sha256=encode(sha256(convert_to(raw_text||' ','UTF8')),'hex') WHERE source_id='NVDA_SUBMISSIONS' AND part_number=1;
  UPDATE population_top3000_202608_202609_v1.pipeline_task SET not_before='infinity' WHERE status IN('READY','BLOCKED');
  UPDATE population_top3000_202608_202609_v1.pipeline_control SET enabled=true,next_wakeup_at=clock_timestamp() WHERE singleton;
  v_bad_task:=population_top3000_202608_202609_v1.pipeline_enqueue_v1('REF01','ACQUIRE','rollback-retained-damaged-'||gen_random_uuid()::text,v_payload,v_inputs);
  v_result:=population_top3000_202608_202609_v1.pipeline_tick_v1();
  IF v_result#>>'{result,state}' IS DISTINCT FROM 'BLOCKED'
   OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=v_bad_task AND blocker_code='RETAINED_COMPANY_SOURCE_INTEGRITY')
   OR (SELECT stored_bytes FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton)<>v_stored+v_expected_copy THEN RAISE EXCEPTION 'DAMAGED_RETAINED_SOURCE_WAS_NOT_REJECTED: %',v_result;END IF;
  RAISE SQLSTATE 'ZT001';
 EXCEPTION WHEN SQLSTATE 'ZT001' THEN RAISE NOTICE 'PASS whole-source bytes/hash rechecked despite verified manifest';END;

 IF (SELECT provider_requests FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton)<>v_provider
  OR (SELECT http_invocations FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton)<>v_http
  OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_provider_lease WHERE released_at IS NULL)
  OR EXISTS(SELECT 1 FROM cron.job WHERE jobid=596 AND active) THEN RAISE EXCEPTION 'RETAINED_TEST_CREATED_EXTERNAL_WORK_OR_ACTIVATED_CRON';END IF;
END $test$;
SELECT 'PASS_RETAINED_COMPANY_ACQUIRE_NORMALIZE_REUSE_AND_MISSING_SOURCE_GUARDS' AS result;
ROLLBACK;
