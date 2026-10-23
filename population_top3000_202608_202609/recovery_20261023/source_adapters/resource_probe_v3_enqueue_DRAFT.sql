-- ROOT REVIEW/ADMISSION ONLY, EXPLICIT LIFETIME SLOT 3. This file queues one real HTTP invocation if executed.
-- Before this block, root must separately record current resource_probe_admission in budget_evidence:
-- {"allowed":true,"worker":"population-top3000-resource-v1","probe_attempt":3,"max_lifetime_invocations":3,"max_invocations":1,"max_usd":2.01}
-- Preserve provider_admissions_allowed=false and the US$50 repair reserve.
-- Deploy and validate the owned worker first. Never display the generated token or pg_net request body.
BEGIN;
SET LOCAL lock_timeout='3s';
SELECT pg_advisory_xact_lock(2026083000261022);
DO $enqueue$
DECLARE v_control population_top3000_202608_202609_v1.acquisition_control%rowtype;
 v_pipeline population_top3000_202608_202609_v1.pipeline_control%rowtype;
 v_probe_id uuid;v_token text;v_now timestamptz;v_expires timestamptz;v_request_id bigint;
BEGIN
 SELECT * INTO v_control FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton FOR UPDATE;
 SELECT * INTO v_pipeline FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton FOR UPDATE;
 IF v_control.owner_id IS DISTINCT FROM 'top3000-recovery-20261023-root-4b1314bcb5c2' OR v_control.owner_fence IS DISTINCT FROM 2::bigint
  OR v_control.stop_generation IS DISTINCT FROM 0::bigint OR v_control.enabled IS DISTINCT FROM true OR v_control.stopped IS DISTINCT FROM false OR v_control.hold_reason IS NOT NULL
  OR v_control.admitted_until IS NULL OR v_control.admitted_until<=clock_timestamp()
  OR v_pipeline.expected_owner_id IS DISTINCT FROM v_control.owner_id OR v_pipeline.expected_owner_fence IS DISTINCT FROM v_control.owner_fence
  OR v_pipeline.expected_stop_generation IS DISTINCT FROM v_control.stop_generation
  OR v_control.scope_sha256 IS DISTINCT FROM '8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd'
  OR v_pipeline.scope_sha256 IS DISTINCT FROM v_control.scope_sha256 OR v_pipeline.deadline IS NULL OR v_pipeline.deadline<=clock_timestamp()
 THEN RAISE EXCEPTION 'RESOURCE_PROBE_OWNER_STOP_SCOPE_OR_ADMISSION';END IF;
 -- Repeated execution after an uncertain enqueue never requeues, regenerates a token or spends again.
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.resource_probe_v1 WHERE probe_kind='FILESYSTEM_METRICS' AND probe_attempt=3) THEN RETURN;END IF;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.resource_probe_v1 WHERE probe_kind='FILESYSTEM_METRICS' AND probe_attempt=1
  AND state='FAILED' AND result->>'error_code'='UNKNOWN_READ_OUTCOME' AND encumbered_usd=2.01) THEN RAISE EXCEPTION 'PRESERVED_UNKNOWN_FIRST_PROBE_RECEIPT_REQUIRED';END IF;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.resource_probe_v1 WHERE probe_attempt=2 AND state='FAILED'
  AND result->>'error_code'='CONTROL_RESULT_SERIALIZATION_REJECTED' AND result->>'root_cause'='POSTGRES345_JSONB_DOUBLE_SERIALIZATION' AND encumbered_usd=2.01)
 THEN RAISE EXCEPTION 'PRESERVED_SECOND_SERIALIZATION_FAILURE_REQUIRED';END IF;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_event proof WHERE proof.event='RESOURCE_PROBE_JSON_SERIALIZATION_CAUSE_VERIFIED'
  AND proof.details->>'result'='CONFIRMED' AND proof.details->>'owner_fence'='2' AND proof.details->>'probe_attempt'='2'
  AND proof.details->>'validation_error'='INVALID_SANITIZED_PROBE_RESULT' AND proof.details->>'root_cause'='POSTGRES345_JSONB_DOUBLE_SERIALIZATION'
  AND proof.details->>'local_test_sha256'='e228ca07add51f48a3f1e04cd3e80b0bbf1652c57d2c3a08c89919aea84c2f21') THEN RAISE EXCEPTION 'AUDITED_MATERIAL_JSON_SERIALIZATION_CORRECTION_REQUIRED';END IF;
 IF v_pipeline.budget_verified_at IS NULL OR v_pipeline.budget_evidence#>>'{resource_probe_admission,allowed}' IS DISTINCT FROM 'true'
  OR v_pipeline.budget_evidence#>>'{resource_probe_admission,worker}' IS DISTINCT FROM 'population-top3000-resource-v1'
  OR v_pipeline.budget_evidence#>>'{resource_probe_admission,probe_attempt}' IS DISTINCT FROM '3'
  OR v_pipeline.budget_evidence#>>'{resource_probe_admission,max_lifetime_invocations}' IS DISTINCT FROM '3'
  OR v_pipeline.budget_evidence#>>'{resource_probe_admission,max_invocations}' IS DISTINCT FROM '1'
  OR v_pipeline.budget_evidence#>>'{resource_probe_admission,max_usd}' IS DISTINCT FROM '2.01'
  OR v_pipeline.budget_evidence->>'provider_admissions_allowed' IS DISTINCT FROM 'false'
  OR v_pipeline.unavoidable_usd<4.02 OR v_pipeline.remaining_uncommitted_usd<2.01 OR v_pipeline.repair_egress_reserve_usd<50
  OR (v_pipeline.actual_usd+v_pipeline.accrued_usd+v_pipeline.unavoidable_usd+v_pipeline.remaining_uncommitted_usd+v_pipeline.repair_egress_reserve_usd+v_pipeline.reserved_request_usd<=250) IS DISTINCT FROM true
  OR v_control.max_http_invocations IS DISTINCT FROM v_control.http_invocations
  OR v_control.max_provider_requests IS DISTINCT FROM v_control.provider_requests
 THEN RAISE EXCEPTION 'EXPLICIT_ONE_PROBE_BUDGET_ADMISSION_REQUIRED';END IF;
 v_now:=clock_timestamp();v_expires:=least(v_now+interval '5 minutes',v_control.admitted_until,v_pipeline.deadline);
 IF v_expires<=v_now+interval '45 seconds' THEN RAISE EXCEPTION 'INSUFFICIENT_PROBE_ADMISSION_WINDOW';END IF;
 v_probe_id:=gen_random_uuid();v_token:=encode(extensions.gen_random_bytes(32),'hex');
 INSERT INTO population_top3000_202608_202609_v1.resource_probe_v1
 (probe_id,probe_kind,probe_attempt,max_runtime_ms,owner_id,owner_fence,stop_generation,scope_sha256,token_sha256,created_at,expires_at,http_invocation_ordinal,encumbered_usd,billing_basis)
 VALUES(v_probe_id,'FILESYSTEM_METRICS',3,30000,v_control.owner_id,v_control.owner_fence,v_control.stop_generation,v_control.scope_sha256,
  encode(sha256(convert_to(v_probe_id::text||':'||v_token,'UTF8')),'hex'),v_now,v_expires,v_control.http_invocations+1,2.01,'CONSERVATIVE_INFRA_ENVELOPE_NOT_ACTUAL_INVOICE');
 UPDATE population_top3000_202608_202609_v1.pipeline_control
 SET remaining_uncommitted_usd=remaining_uncommitted_usd-2.01,unavoidable_usd=unavoidable_usd+2.01 WHERE singleton;
 -- The new HTTP allowance is consumed in this transaction; there is no spare admission for other workers.
 UPDATE population_top3000_202608_202609_v1.acquisition_control
 SET max_http_invocations=max_http_invocations+1,http_invocations=http_invocations+1,updated_at=clock_timestamp() WHERE singleton;
 SELECT net.http_post(url:='https://oxzabweahkoimtevbbny.supabase.co/functions/v1/population-top3000-resource-v1',
  headers:='{"Content-Type":"application/json"}'::jsonb,body:=jsonb_build_object('probe_id',v_probe_id,'token',v_token),timeout_milliseconds:=35000) INTO v_request_id;
 IF v_request_id IS NULL THEN RAISE EXCEPTION 'RESOURCE_PROBE_QUEUE_RECEIPT_MISSING';END IF;
 UPDATE population_top3000_202608_202609_v1.resource_probe_v1 SET request_id=v_request_id WHERE probe_id=v_probe_id;
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details)
 VALUES('RESOURCE_PROBE_V3_ADMITTED',jsonb_build_object('probe_id',v_probe_id,'request_id',v_request_id,'owner_id',v_control.owner_id,'owner_fence',v_control.owner_fence,
  'stop_generation',v_control.stop_generation,'expires_at',v_expires,'probe_attempt',3,'max_lifetime_invocations',3,'maximum_invocations',1,'maximum_infrastructure_encumbrance_usd',2.01,'prior_encumbrance_preserved_usd',4.02,
  'billing_basis','CONSERVATIVE_INFRA_ENVELOPE_NOT_ACTUAL_INVOICE','provider_requests',0,'no_capacity_change',true,'automatic_retry',false));
END $enqueue$;
COMMIT;
-- Safe reconciliation output: no token, token hash, request body or credentials.
SELECT probe_id,probe_attempt,max_runtime_ms,state,created_at,expires_at,claimed_at,claim_deadline_at,finished_at,request_id,encumbered_usd,result
FROM population_top3000_202608_202609_v1.resource_probe_v1 WHERE probe_kind='FILESYSTEM_METRICS';
