-- Root-only reconciliation. Sends no request; does not reset either capability or alter a financial counter.
-- Requires the actual final503/FINISH/P0001 response and separately audited log+driver-test evidence.
BEGIN;
SET LOCAL lock_timeout='3s';
SELECT pg_advisory_xact_lock(2026083000261022);
DO $reconcile$
DECLARE v_probe population_top3000_202608_202609_v1.resource_probe_v1%rowtype;
 v_control population_top3000_202608_202609_v1.acquisition_control%rowtype;
 v_pipeline population_top3000_202608_202609_v1.pipeline_control%rowtype;
 v_http jsonb;v_body jsonb;v_result jsonb;
BEGIN
 SELECT * INTO v_control FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton FOR UPDATE;
 SELECT * INTO v_pipeline FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton FOR UPDATE;
 SELECT * INTO v_probe FROM population_top3000_202608_202609_v1.resource_probe_v1 WHERE probe_kind='FILESYSTEM_METRICS' AND probe_attempt=2 FOR UPDATE;
 IF NOT FOUND THEN RAISE EXCEPTION 'SECOND_RESOURCE_PROBE_REQUIRED';END IF;
 IF v_control.owner_id IS DISTINCT FROM 'top3000-recovery-20261023-root-4b1314bcb5c2' OR v_control.owner_fence IS DISTINCT FROM 2::bigint
  OR v_control.stop_generation IS DISTINCT FROM 0::bigint OR v_control.owner_id IS DISTINCT FROM v_probe.owner_id
  OR v_control.owner_fence IS DISTINCT FROM v_probe.owner_fence OR v_control.stop_generation IS DISTINCT FROM v_probe.stop_generation
  OR v_pipeline.expected_owner_id IS DISTINCT FROM v_control.owner_id OR v_pipeline.expected_owner_fence IS DISTINCT FROM v_control.owner_fence
  OR v_pipeline.expected_stop_generation IS DISTINCT FROM v_control.stop_generation
  OR v_control.scope_sha256 IS DISTINCT FROM v_probe.scope_sha256 OR v_pipeline.scope_sha256 IS DISTINCT FROM v_probe.scope_sha256
 THEN RAISE EXCEPTION 'RESOURCE_PROBE_RECONCILIATION_OWNER_FENCE_STOP_OR_SCOPE_CHANGED';END IF;
 IF v_probe.state='FAILED' AND v_probe.result->>'error_code'='CONTROL_RESULT_SERIALIZATION_REJECTED' THEN RETURN;END IF;
 IF v_probe.state IS DISTINCT FROM 'RUNNING' OR v_probe.result IS NOT NULL OR v_probe.finished_at IS NOT NULL
  OR v_probe.claim_deadline_at IS NULL OR v_probe.claim_deadline_at>clock_timestamp()
  OR v_probe.encumbered_usd IS DISTINCT FROM 2.01::numeric OR v_pipeline.unavoidable_usd<4.02
 THEN RAISE EXCEPTION 'EXPIRED_UNRECORDED_SECOND_PROBE_WITH_RETAINED_ENCUMBRANCES_REQUIRED';END IF;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_event proof WHERE proof.event='RESOURCE_PROBE_JSON_SERIALIZATION_CAUSE_VERIFIED'
  AND proof.details->>'result'='CONFIRMED' AND proof.details->>'owner_fence'='2' AND proof.details->>'probe_attempt'='2'
  AND proof.details->>'validation_error'='INVALID_SANITIZED_PROBE_RESULT' AND proof.details->>'root_cause'='POSTGRES345_JSONB_DOUBLE_SERIALIZATION'
  AND proof.details->>'local_test_sha256'='e228ca07add51f48a3f1e04cd3e80b0bbf1652c57d2c3a08c89919aea84c2f21')
 THEN RAISE EXCEPTION 'AUDITED_SERVER_ERROR_AND_REAL_DRIVER_TEST_REQUIRED';END IF;
 SELECT to_jsonb(http_response) INTO v_http FROM net._http_response http_response WHERE http_response.id=v_probe.request_id;
 IF v_http->>'status_code' IS DISTINCT FROM '503' OR v_http->>'timed_out' IS DISTINCT FROM 'false'
 THEN RAISE EXCEPTION 'RETAINED_FINAL_SECOND_EDGE_503_RESPONSE_REQUIRED';END IF;
 v_body:=(v_http->>'content')::jsonb;
 IF v_body->>'phase' IS DISTINCT FROM 'FINISH' OR v_body->>'sqlstate' IS DISTINCT FROM 'P0001'
 THEN RAISE EXCEPTION 'RETAINED_FINISH_P0001_DIAGNOSTIC_REQUIRED';END IF;
 v_result:=jsonb_build_object('outcome','FAILED','error_code','CONTROL_RESULT_SERIALIZATION_REJECTED','reconciled_at',clock_timestamp(),
  'probe_attempt',2,'edge_http_status',503,'edge_transport_timed_out',false,'metrics_http_status',NULL,
  'phase','FINISH','sqlstate','P0001','validation_error','INVALID_SANITIZED_PROBE_RESULT',
  'root_cause','POSTGRES345_JSONB_DOUBLE_SERIALIZATION','metrics_fetch_outcome','UNKNOWN','metrics',jsonb_build_array(),
  'source_sha256',NULL,'capabilities',NULL,'sanitized_snapshot_lost',true,
  'reason','The already-stringified snapshot was serialized again by the JSONB driver. The object validator rejected it; its contents were not retained.',
  'operation_effect_class','READ_ONLY_OWNED_PROJECT_METRICS','no_capacity_change',true,'provider_requests',0,
  'automatic_retry',false,'encumbrance_preserved_usd',2.01,'billing_basis','CONSERVATIVE_INFRA_ENVELOPE_NOT_ACTUAL_INVOICE');
 UPDATE population_top3000_202608_202609_v1.resource_probe_v1 SET state='FAILED',finished_at=clock_timestamp(),result=v_result,
  result_sha256=encode(sha256(convert_to(v_result::text,'UTF8')),'hex') WHERE probe_id=v_probe.probe_id;
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details)
 VALUES('RESOURCE_PROBE_SERIALIZATION_FAILURE_RECONCILED',jsonb_build_object('probe_id',v_probe.probe_id,'request_id',v_probe.request_id,
  'probe_attempt',2,'owner_fence',2,'error_code','CONTROL_RESULT_SERIALIZATION_REJECTED','edge_http_status',503,
  'root_cause','POSTGRES345_JSONB_DOUBLE_SERIALIZATION','metrics_fetch_outcome','UNKNOWN',
  'encumbrance_preserved_usd',2.01,'new_http_requests',0,'no_capacity_change',true));
END $reconcile$;
COMMIT;
