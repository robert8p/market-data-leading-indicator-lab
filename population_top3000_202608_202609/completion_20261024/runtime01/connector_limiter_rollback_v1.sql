BEGIN;
SET LOCAL statement_timeout='15s';
SET LOCAL lock_timeout='3s';
CREATE TEMP TABLE connector_test_result(result jsonb) ON COMMIT DROP;
DO $test$
DECLARE a population_top3000_202608_202609_v1.acquisition_control%rowtype;
 p population_top3000_202608_202609_v1.pipeline_control%rowtype;
 r jsonb;r2 jsonb;receipt jsonb;bad_fence boolean:=false;bad_stop boolean:=false;
 bad_nan boolean:=false;bad_unknown boolean:=false;bad_size boolean:=false;before_calls bigint;
BEGIN
 PERFORM pg_advisory_xact_lock(2026083000261022);
 SELECT * INTO STRICT a FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton FOR UPDATE;
 SELECT * INTO STRICT p FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton FOR UPDATE;
 IF a.owner_id IS DISTINCT FROM 'top3000-completion-20261024-root-5db48b32d103' OR a.owner_fence<>3 OR a.stop_generation<>1 OR a.stopped
  OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_provider_lease WHERE released_at IS NULL)
  OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.connector_probe_lease_v1 WHERE released_at IS NULL)
  OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.acquisition_job WHERE status='LEASED' AND lease_until>clock_timestamp())
 THEN RAISE EXCEPTION 'CONNECTOR_TEST_CURRENT_IDLE_PROVIDER_REQUIRED';END IF;
 before_calls:=a.provider_requests;
 UPDATE population_top3000_202608_202609_v1.acquisition_control SET next_provider_at=clock_timestamp()-interval '1 second' WHERE singleton;
 BEGIN
  PERFORM population_top3000_202608_202609_v1.connector_probe_reserve_v1(a.owner_id,2,1,'limiter-rollback-1','options',repeat('a',64),100,0);
 EXCEPTION WHEN raise_exception THEN IF SQLERRM='CONNECTOR_CONTROLLER_STOP_SCOPE_ADMISSION_REJECTED' THEN bad_fence:=true;ELSE RAISE;END IF;END;
 BEGIN
  UPDATE population_top3000_202608_202609_v1.acquisition_control SET stopped=true WHERE singleton;
  PERFORM population_top3000_202608_202609_v1.connector_probe_reserve_v1(a.owner_id,3,1,'limiter-rollback-1','options',repeat('a',64),100,0);
  RAISE EXCEPTION 'LATEST_STOP_ACCEPTED';
 EXCEPTION WHEN raise_exception THEN IF SQLERRM='CONNECTOR_CONTROLLER_STOP_SCOPE_ADMISSION_REJECTED' THEN bad_stop:=true;ELSE RAISE;END IF;END;
 BEGIN
  PERFORM population_top3000_202608_202609_v1.connector_probe_reserve_v1(a.owner_id,3,1,'limiter-rollback-1','options',repeat('a',64),100,'NaN'::numeric);
 EXCEPTION WHEN raise_exception THEN IF SQLERRM='CONNECTOR_ARGUMENT_OR_COST_ADMISSION_REJECTED' THEN bad_nan:=true;ELSE RAISE;END IF;END;
 r:=population_top3000_202608_202609_v1.connector_probe_reserve_v1(a.owner_id,3,1,'limiter-rollback-1','options',repeat('a',64),100,0.01);
 IF r->>'state' IS DISTINCT FROM 'RESERVED' THEN RAISE EXCEPTION 'RESERVATION_FAILED %',r;END IF;
 r2:=population_top3000_202608_202609_v1.connector_probe_reserve_v1(a.owner_id,3,1,'limiter-rollback-1','options',repeat('a',64),100,0.01);
 IF r2->>'state' IS DISTINCT FROM 'ALREADY_RESERVED_DO_NOT_REISSUE' THEN RAISE EXCEPTION 'DUPLICATE_NOT_PROTECTED';END IF;
 UPDATE population_top3000_202608_202609_v1.acquisition_control SET next_provider_at=clock_timestamp()-interval '1 second' WHERE singleton;
 r2:=population_top3000_202608_202609_v1.connector_probe_reserve_v1(a.owner_id,3,1,'limiter-rollback-2','REF01',repeat('b',64),100,0);
 IF r2->>'state' IS DISTINCT FROM 'WAIT_UNRECONCILED_PROVIDER_OPERATION' THEN RAISE EXCEPTION 'CONCURRENT_NOT_BLOCKED';END IF;
 r2:=population_top3000_202608_202609_v1.population_recovery_request_v1('limiter-rollback-recovery','Synthetic rollback: unresolved provider check');
 IF r2#>>'{checks,active_operations}' IS DISTINCT FROM 'true' THEN RAISE EXCEPTION 'RECOVERY_IGNORES_CONNECTOR_LEASE';END IF;
 receipt:=jsonb_build_object('state','CONFIRMED_NOT_DISPATCHED','request_sha256',repeat('a',64),'physical_http_count','UNKNOWN_CONNECTOR_INTERNALS','response_sha256',encode(sha256(convert_to('synthetic rollback no request','UTF8')),'hex'),'response_bytes',0,'test_only',true);
 BEGIN
  PERFORM population_top3000_202608_202609_v1.connector_probe_reconcile_v1(a.owner_id,3,1,'limiter-rollback-1',receipt||jsonb_build_object('state','TRANSPORT_UNKNOWN'),15);
 EXCEPTION WHEN raise_exception THEN IF SQLERRM='CONNECTOR_RECEIPT_INVALID_OR_UNKNOWN_EFFECT' THEN bad_unknown:=true;ELSE RAISE;END IF;END;
 BEGIN
  PERFORM population_top3000_202608_202609_v1.connector_probe_reconcile_v1(a.owner_id,3,1,'limiter-rollback-1',receipt||jsonb_build_object('response_bytes',101),15);
 EXCEPTION WHEN raise_exception THEN IF SQLERRM='CONNECTOR_RESPONSE_EXCEEDED_RESERVATION' THEN bad_size:=true;ELSE RAISE;END IF;END;
 r2:=population_top3000_202608_202609_v1.connector_probe_reconcile_v1(a.owner_id,3,1,'limiter-rollback-1',receipt,15);
 IF r2->>'state' IS DISTINCT FROM 'RECONCILED' THEN RAISE EXCEPTION 'RECONCILE_FAILED';END IF;
 r2:=population_top3000_202608_202609_v1.connector_probe_reconcile_v1(a.owner_id,3,1,'limiter-rollback-1',receipt,15);
 IF r2->>'state' IS DISTINCT FROM 'ALREADY_RECONCILED' THEN RAISE EXCEPTION 'REPLAY_RECONCILE_FAILED';END IF;
 r2:=population_top3000_202608_202609_v1.connector_probe_reserve_v1(a.owner_id,3,1,'limiter-rollback-2','REF01',repeat('b',64),100,0);
 IF r2->>'state' IS DISTINCT FROM 'WAIT' THEN RAISE EXCEPTION 'COOLDOWN_NOT_ENFORCED';END IF;
 IF NOT(bad_fence AND bad_stop AND bad_nan AND bad_unknown AND bad_size)
  OR (SELECT provider_requests FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton)<>before_calls+1
  OR (SELECT reserved_bytes FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton)<>a.reserved_bytes
  OR (SELECT reserved_request_usd FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton)<>p.reserved_request_usd+0.01
  OR (SELECT accrued_usd FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton)<>p.accrued_usd
 THEN RAISE EXCEPTION 'CONNECTOR_FINAL_GUARD_OR_COUNTER_PROOF_FAILED';END IF;
 INSERT INTO connector_test_result VALUES(jsonb_build_object('result','PASS_SHARED_CONNECTOR_LIMITER_ROLLBACK','latest_stop_rejected',bad_stop,'stale_fence_rejected',bad_fence,'nonfinite_cost_rejected',bad_nan,'unknown_effect_not_released',bad_unknown,'oversize_not_released',bad_size,'duplicate_and_concurrent_protected',true,'recovery_checks_connector_lease',true,'cooldown_enforced',true,'actual_provider_calls',0,'test_mutations_rolled_back',true));
END $test$;
SELECT result FROM connector_test_result;
ROLLBACK;
