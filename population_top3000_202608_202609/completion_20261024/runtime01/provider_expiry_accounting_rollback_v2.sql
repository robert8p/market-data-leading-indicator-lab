-- Actual DB rollback fixture for future expiry. No external request and no tick invocation.
BEGIN;
SET LOCAL lock_timeout='15s'; SET LOCAL statement_timeout='30s';
CREATE TEMP TABLE expiry_accounting_test_proof(result jsonb) ON COMMIT DROP;
DO $test$
DECLARE a population_top3000_202608_202609_v1.acquisition_control%rowtype;
 p population_top3000_202608_202609_v1.pipeline_control%rowtype;
 t population_top3000_202608_202609_v1.pipeline_task%rowtype;
 fixture_task uuid;fixture_attempt uuid;reservation numeric;i integer;r jsonb;r2 jsonb;h text;before_a jsonb;before_p jsonb;
 bad_stop boolean:=false;bad_fence boolean:=false;bad_expiry boolean:=false;bad_charge boolean:=false;bad_reservation boolean:=false;
 preserved_nonzero numeric:=0;source_hash text;
BEGIN
 PERFORM pg_advisory_xact_lock(2026083000261022);
 SELECT * INTO STRICT a FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton FOR UPDATE;
 SELECT * INTO STRICT p FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton FOR UPDATE;
 IF a.owner_id IS DISTINCT FROM 'top3000-completion-20261024-root-5db48b32d103' OR a.owner_fence IS DISTINCT FROM 3 OR a.stop_generation IS DISTINCT FROM 1 THEN RAISE EXCEPTION 'EXPIRY_TEST_CURRENT_CONTROLLER_REQUIRED';END IF;
 h:=encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.pipeline_provider_expiry_accounting_v2(uuid)'::regprocedure),'UTF8')),'hex');
 IF h IS DISTINCT FROM 'ace964d10158e91bbdcfa658dd7583961a9e8557993266ed1d2cfcb1ac041eeb' THEN RAISE EXCEPTION 'EXPIRY_TEST_REVIEWED_HELPER_REQUIRED';END IF;
 FOR i IN 1..3 LOOP
  fixture_task:=gen_random_uuid();fixture_attempt:=gen_random_uuid();reservation:=CASE i WHEN 1 THEN 1.234567 WHEN 2 THEN 2.500000 ELSE 0 END;
  SELECT * INTO STRICT t FROM population_top3000_202608_202609_v1.pipeline_task WHERE family_id='D03' AND stage='DERIVE' ORDER BY created_at LIMIT 1;
  t.task_id:=fixture_task;t.operation_key:=encode(sha256(convert_to(fixture_task::text,'UTF8')),'hex');t.partition_key:='expiry-accounting-v2-rollback:'||fixture_task::text;t.status:=CASE WHEN i=2 THEN 'BLOCKED' ELSE 'LEASED' END;
  t.blocker_code:=CASE WHEN i=2 THEN 'UNCERTAIN_EFFECT' ELSE NULL END;t.resumption_condition:=CASE WHEN i=2 THEN jsonb_build_object('fixture_only',true,'required_action','retain unknown source effect') ELSE NULL END;t.blocker_until:=NULL;t.attempt_id:=fixture_attempt;t.owner_fence:=a.owner_fence;t.stop_generation:=a.stop_generation;
  t.lease_until:=clock_timestamp()-interval '1 second';t.executions:=1;t.cumulative_failures:=0;t.same_failure_count:=0;t.completed_at:=NULL;
  INSERT INTO population_top3000_202608_202609_v1.pipeline_task SELECT (t).*;
  INSERT INTO population_top3000_202608_202609_v1.pipeline_attempt(attempt_id,task_id,execution_number,token_sha256,owner_fence,stop_generation,adapter_sha256)
  SELECT fixture_attempt,fixture_task,1,encode(sha256(convert_to('expiry-rollback-only','UTF8')),'hex'),a.owner_fence,a.stop_generation,function_sha256
  FROM population_top3000_202608_202609_v1.pipeline_adapter WHERE family_id='D03' AND stage='DERIVE';
  INSERT INTO population_top3000_202608_202609_v1.pipeline_provider_lease(attempt_id,provider,reserved_bytes,reserved_usd)VALUES(fixture_attempt,'ROLLBACK_FIXTURE_NO_PROVIDER_CALL',123,reservation);
  UPDATE population_top3000_202608_202609_v1.pipeline_control SET reserved_request_usd=reserved_request_usd+reservation WHERE singleton;
  UPDATE population_top3000_202608_202609_v1.acquisition_control SET reserved_bytes=reserved_bytes+123 WHERE singleton;
  SELECT to_jsonb(x) INTO before_p FROM population_top3000_202608_202609_v1.pipeline_control x WHERE singleton;
  SELECT to_jsonb(x) INTO before_a FROM population_top3000_202608_202609_v1.acquisition_control x WHERE singleton;
  IF i=1 THEN
   BEGIN UPDATE population_top3000_202608_202609_v1.acquisition_control SET stopped=true WHERE singleton;
    PERFORM population_top3000_202608_202609_v1.pipeline_provider_expiry_accounting_v2(fixture_attempt);RAISE EXCEPTION 'BAD_STOP_ACCEPTED';
   EXCEPTION WHEN raise_exception THEN IF SQLERRM='EXPIRY_ACCOUNTING_CURRENT_CONTROLLER_REQUIRED' THEN bad_stop:=true;ELSE RAISE;END IF;END;
   BEGIN UPDATE population_top3000_202608_202609_v1.pipeline_task SET owner_fence=2 WHERE task_id=fixture_task;
    PERFORM population_top3000_202608_202609_v1.pipeline_provider_expiry_accounting_v2(fixture_attempt);RAISE EXCEPTION 'BAD_FENCE_ACCEPTED';
   EXCEPTION WHEN raise_exception THEN IF SQLERRM='EXPIRY_ACCOUNTING_ATTEMPT_FENCE_CHANGED' THEN bad_fence:=true;ELSE RAISE;END IF;END;
   BEGIN UPDATE population_top3000_202608_202609_v1.pipeline_task SET lease_until=clock_timestamp()+interval '1 hour' WHERE task_id=fixture_task;
    PERFORM population_top3000_202608_202609_v1.pipeline_provider_expiry_accounting_v2(fixture_attempt);RAISE EXCEPTION 'UNEXPIRED_ACCEPTED';
   EXCEPTION WHEN raise_exception THEN IF SQLERRM='EXPIRY_ACCOUNTING_EXPIRED_EFFECT_REQUIRED' THEN bad_expiry:=true;ELSE RAISE;END IF;END;
   BEGIN UPDATE population_top3000_202608_202609_v1.pipeline_provider_lease SET actual_usd=0.123456 WHERE attempt_id=fixture_attempt;
    PERFORM population_top3000_202608_202609_v1.pipeline_provider_expiry_accounting_v2(fixture_attempt);RAISE EXCEPTION 'EXISTING_CHARGE_OVERWRITTEN';
   EXCEPTION WHEN raise_exception THEN IF SQLERRM='EXPIRY_ACCOUNTING_EXISTING_CHARGE_MUST_NOT_BE_OVERWRITTEN' THEN bad_charge:=true;ELSE RAISE;END IF;END;
   BEGIN UPDATE population_top3000_202608_202609_v1.pipeline_control SET reserved_request_usd=0 WHERE singleton;
    PERFORM population_top3000_202608_202609_v1.pipeline_provider_expiry_accounting_v2(fixture_attempt);RAISE EXCEPTION 'MISSING_COMMITMENT_ACCEPTED';
   EXCEPTION WHEN raise_exception THEN IF SQLERRM='EXPIRY_ACCOUNTING_FINITE_COMMITMENT_REQUIRED' THEN bad_reservation:=true;ELSE RAISE;END IF;END;
  END IF;
  r:=population_top3000_202608_202609_v1.pipeline_provider_expiry_accounting_v2(fixture_attempt);
  r2:=population_top3000_202608_202609_v1.pipeline_provider_expiry_accounting_v2(fixture_attempt);
  IF r->>'state' IS DISTINCT FROM 'OPERATIONAL_LEASE_RELEASED_UNINVOICED_COMMITMENT_RETAINED' OR r2->>'state' IS DISTINCT FROM 'ALREADY_RELEASED'
  OR (r->>'reserved_usd_retained')::numeric IS DISTINCT FROM reservation OR r->>'actual_invoice_charge_usd' IS NOT NULL
  OR (SELECT to_jsonb(x) FROM population_top3000_202608_202609_v1.pipeline_control x WHERE singleton) IS DISTINCT FROM before_p
  OR (SELECT to_jsonb(x) FROM population_top3000_202608_202609_v1.acquisition_control x WHERE singleton) IS DISTINCT FROM before_a
  OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_provider_lease WHERE attempt_id=fixture_attempt AND released_at IS NOT NULL AND actual_usd IS NULL AND reserved_usd=reservation)
  OR (SELECT count(*) FROM population_top3000_202608_202609_v1.pipeline_event WHERE attempt_id=fixture_attempt AND event='PROVIDER_EXPIRY_UNINVOICED_COMMITMENT_RETAINED_V2')<>1
  THEN RAISE EXCEPTION 'EXPIRY_TEST_FINITE_UNKNOWN_COST_OR_REPLAY_FAILED';END IF;
  preserved_nonzero:=preserved_nonzero+reservation;
 END LOOP;
 IF NOT(bad_stop AND bad_fence AND bad_expiry AND bad_charge AND bad_reservation) THEN RAISE EXCEPTION 'EXPIRY_TEST_GUARD_FAILURE';END IF;
 INSERT INTO expiry_accounting_test_proof VALUES(jsonb_build_object('result','PASS_FUTURE_EXPIRY_ACCOUNTING_ROLLBACK','cases',3,'nonzero_reserved_usd_retained',preserved_nonzero,
 'zero_reservation_does_not_invent_zero_invoice',true,'actual_and_accrued_totals_unchanged',true,'entire_controls_unchanged_by_helper',true,'replay_extra_events',0,'negative_guards',5,
 'helper_sha256',h,'actual_external_calls',0,'historical_rows_or_totals_rewritten',false,'production_changes_rolled_back',true));
END;
$test$;
SELECT result FROM expiry_accounting_test_proof;
ROLLBACK;
