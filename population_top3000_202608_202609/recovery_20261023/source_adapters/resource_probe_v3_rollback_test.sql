-- Actual private capability functions, local transaction only; no pg_net or Edge request.
-- Run after v3 definitions and both failed-probe reconciliations, before slot3 admission.
BEGIN;
SET LOCAL lock_timeout='3s';
SET LOCAL statement_timeout='30s';
SELECT pg_advisory_xact_lock(2026083000261022);
DO $test$
DECLARE v_probe uuid:=gen_random_uuid();v_token text:=repeat('a',64);v_now timestamptz:=clock_timestamp();
 v_control population_top3000_202608_202609_v1.acquisition_control%rowtype;
 v_pipeline population_top3000_202608_202609_v1.pipeline_control%rowtype;
 v_result jsonb;v_snapshot jsonb;v_first_hash text;v_second_hash text;v_result_hash text;v_provider bigint;v_http bigint;v_actual numeric;v_accrued numeric;
BEGIN
 SELECT * INTO v_control FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton FOR UPDATE;
 SELECT * INTO v_pipeline FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton FOR UPDATE;
 IF v_control.owner_id IS DISTINCT FROM 'top3000-recovery-20261023-root-4b1314bcb5c2' OR v_control.owner_fence<>2 OR v_control.stop_generation<>0
  OR NOT v_control.enabled OR v_control.stopped OR v_control.hold_reason IS NOT NULL OR v_control.admitted_until<=clock_timestamp()
 THEN RAISE EXCEPTION 'CURRENT_ADMITTED_OWNER_REQUIRED';END IF;
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.resource_probe_v1 WHERE probe_attempt=3) THEN RAISE EXCEPTION 'THIRD_LIFETIME_SLOT_ALREADY_USED';END IF;
 SELECT result_sha256 INTO v_first_hash FROM population_top3000_202608_202609_v1.resource_probe_v1 WHERE probe_attempt=1
  AND state='FAILED' AND result->>'error_code'='UNKNOWN_READ_OUTCOME' AND encumbered_usd=2.01;
 IF v_first_hash IS NULL THEN RAISE EXCEPTION 'PRESERVED_FIRST_UNKNOWN_OUTCOME_REQUIRED';END IF;
 SELECT result_sha256 INTO v_second_hash FROM population_top3000_202608_202609_v1.resource_probe_v1 WHERE probe_attempt=2
  AND state='FAILED' AND result->>'error_code'='CONTROL_RESULT_SERIALIZATION_REJECTED' AND result->>'root_cause'='POSTGRES345_JSONB_DOUBLE_SERIALIZATION' AND encumbered_usd=2.01;
 IF v_second_hash IS NULL THEN RAISE EXCEPTION 'PRESERVED_SECOND_SERIALIZATION_FAILURE_REQUIRED';END IF;
 v_provider:=v_control.provider_requests;v_http:=v_control.http_invocations;v_actual:=v_pipeline.actual_usd;v_accrued:=v_pipeline.accrued_usd;
 -- Rollback-only admission fixture; actual/accrued totals and request counts are never changed.
 UPDATE population_top3000_202608_202609_v1.pipeline_control SET
  remaining_uncommitted_usd=remaining_uncommitted_usd-2.01,unavoidable_usd=unavoidable_usd+2.01,
  budget_evidence=jsonb_set(budget_evidence,'{resource_probe_admission}',
   '{"allowed":true,"worker":"population-top3000-resource-v1","probe_attempt":3,"max_lifetime_invocations":3,"max_invocations":1,"max_usd":2.01}')
 WHERE singleton;
 INSERT INTO population_top3000_202608_202609_v1.resource_probe_v1
 (probe_id,probe_kind,probe_attempt,max_runtime_ms,owner_id,owner_fence,stop_generation,scope_sha256,token_sha256,created_at,expires_at,http_invocation_ordinal,encumbered_usd,billing_basis)
 VALUES(v_probe,'FILESYSTEM_METRICS',3,30000,v_control.owner_id,2,0,v_control.scope_sha256,
  encode(sha256(convert_to(v_probe::text||':'||v_token,'UTF8')),'hex'),v_now,v_now+interval '5 minutes',v_http,2.01,'CONSERVATIVE_INFRA_ENVELOPE_NOT_ACTUAL_INVOICE');
 BEGIN
  INSERT INTO population_top3000_202608_202609_v1.resource_probe_v1
  (probe_id,probe_kind,owner_id,owner_fence,stop_generation,scope_sha256,token_sha256,state,created_at,expires_at,claimed_at,claim_deadline_at,finished_at,request_id,http_invocation_ordinal,encumbered_usd,billing_basis,result,result_sha256,probe_attempt,max_runtime_ms)
  SELECT gen_random_uuid(),probe_kind,owner_id,owner_fence,stop_generation,scope_sha256,token_sha256,state,created_at,expires_at,
   claimed_at,claim_deadline_at,finished_at,request_id,http_invocation_ordinal,encumbered_usd,billing_basis,result,result_sha256,4,30000
  FROM population_top3000_202608_202609_v1.resource_probe_v1 WHERE probe_id=v_probe;
  RAISE EXCEPTION 'FOURTH_LIFETIME_SLOT_WAS_ACCEPTED';
 EXCEPTION WHEN check_violation THEN NULL;END;
 BEGIN
  INSERT INTO population_top3000_202608_202609_v1.resource_probe_v1
  (probe_id,probe_kind,owner_id,owner_fence,stop_generation,scope_sha256,token_sha256,state,created_at,expires_at,claimed_at,claim_deadline_at,finished_at,request_id,http_invocation_ordinal,encumbered_usd,billing_basis,result,result_sha256,probe_attempt,max_runtime_ms)
  SELECT gen_random_uuid(),probe_kind,owner_id,owner_fence,stop_generation,scope_sha256,token_sha256,state,created_at,expires_at,
   claimed_at,claim_deadline_at,finished_at,request_id,http_invocation_ordinal,encumbered_usd,billing_basis,result,result_sha256,3,30000
  FROM population_top3000_202608_202609_v1.resource_probe_v1 WHERE probe_id=v_probe;
  RAISE EXCEPTION 'DUPLICATE_THIRD_SLOT_WAS_ACCEPTED';
 EXCEPTION WHEN unique_violation THEN NULL;END;
 v_result:=population_top3000_202608_202609_v1.resource_probe_claim_v1(v_probe,repeat('0',64));
 IF v_result->>'state' IS DISTINCT FROM 'DENIED' THEN RAISE EXCEPTION 'BAD_CAPABILITY_ACCEPTED';END IF;
 BEGIN
  UPDATE population_top3000_202608_202609_v1.acquisition_control SET owner_fence=3 WHERE singleton;
  v_result:=population_top3000_202608_202609_v1.resource_probe_claim_v1(v_probe,v_token);
  IF v_result->>'state' IS DISTINCT FROM 'DENIED' THEN RAISE EXCEPTION 'WRONG_FENCE_ACCEPTED';END IF;
  RAISE SQLSTATE 'ZT001';
 EXCEPTION WHEN SQLSTATE 'ZT001' THEN NULL;END;
 BEGIN
  UPDATE population_top3000_202608_202609_v1.acquisition_control SET stop_generation=1 WHERE singleton;
  v_result:=population_top3000_202608_202609_v1.resource_probe_claim_v1(v_probe,v_token);
  IF v_result->>'state' IS DISTINCT FROM 'DENIED' THEN RAISE EXCEPTION 'NEW_STOP_GENERATION_ACCEPTED';END IF;
  RAISE SQLSTATE 'ZT001';
 EXCEPTION WHEN SQLSTATE 'ZT001' THEN NULL;END;
 v_result:=population_top3000_202608_202609_v1.resource_probe_claim_v1(v_probe,v_token);
 IF v_result->>'state' IS DISTINCT FROM 'CLAIMED' OR v_result->>'probe_attempt' IS DISTINCT FROM '3'
  OR v_result->>'total_timeout_ms' IS DISTINCT FROM '30000' THEN RAISE EXCEPTION 'THIRD_PROBE_CLAIM_FAILED: %',v_result;END IF;
 v_result:=population_top3000_202608_202609_v1.resource_probe_claim_v1(v_probe,v_token);
 IF v_result->>'state' IS DISTINCT FROM 'DENIED' THEN RAISE EXCEPTION 'CLAIM_REPLAY_ACCEPTED';END IF;
 v_snapshot:=jsonb_build_object('outcome','SUCCEEDED','observed_at',clock_timestamp(),'metrics_http_status',200,'source_sha256',repeat('b',64),
  'metrics',jsonb_build_array(jsonb_build_object('filesystem_id',repeat('c',64),'mountpoint','/rollback-fixture','available_bytes',40,'free_bytes',50,'size_bytes',100)),
  'error_code',NULL,'elapsed_ms',12000,'capabilities',jsonb_build_object('sec_user_agent_configured',false,'sec_contact_email_configured',false,'massive_key_configured',false));
 BEGIN
  PERFORM population_top3000_202608_202609_v1.resource_probe_finish_v1(v_probe,v_token,v_snapshot||'{"private_error":"FORBIDDEN_FIXTURE"}'::jsonb);
  RAISE EXCEPTION 'UNSANITIZED_RESULT_ACCEPTED';
 EXCEPTION WHEN OTHERS THEN IF SQLERRM IS DISTINCT FROM 'INVALID_SANITIZED_PROBE_RESULT' THEN RAISE;END IF;END;
 v_result:=population_top3000_202608_202609_v1.resource_probe_finish_v1(v_probe,v_token,v_snapshot);
 IF v_result->>'state' IS DISTINCT FROM 'RECORDED' THEN RAISE EXCEPTION 'SANITIZED_THIRD_RESULT_FAILED: %',v_result;END IF;
 SELECT result_sha256 INTO v_result_hash FROM population_top3000_202608_202609_v1.resource_probe_v1 WHERE probe_id=v_probe;
 v_result:=population_top3000_202608_202609_v1.resource_probe_finish_v1(v_probe,v_token,v_snapshot);
 IF v_result->>'state' IS DISTINCT FROM 'ALREADY_RECORDED'
  OR (SELECT result_sha256 FROM population_top3000_202608_202609_v1.resource_probe_v1 WHERE probe_id=v_probe) IS DISTINCT FROM v_result_hash
 THEN RAISE EXCEPTION 'THIRD_RECEIPT_REPLAY_CHANGED_IMMUTABLE_RESULT';END IF;
 IF (SELECT result_sha256 FROM population_top3000_202608_202609_v1.resource_probe_v1 WHERE probe_attempt=1) IS DISTINCT FROM v_first_hash
  OR (SELECT result_sha256 FROM population_top3000_202608_202609_v1.resource_probe_v1 WHERE probe_attempt=2) IS DISTINCT FROM v_second_hash
  OR (SELECT provider_requests FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton) IS DISTINCT FROM v_provider
  OR (SELECT http_invocations FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton) IS DISTINCT FROM v_http
  OR (SELECT actual_usd FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton) IS DISTINCT FROM v_actual
  OR (SELECT accrued_usd FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton) IS DISTINCT FROM v_accrued
 THEN RAISE EXCEPTION 'PRIOR_RECEIPTS_OR_ACCOUNTING_CHANGED';END IF;
END $test$;
SELECT 'PASS_RESOURCE_PROBE_V3_THREE_SLOT_CAPABILITY_SANITIZATION_AND_ACCOUNTING_GUARDS' AS result;
ROLLBACK;
