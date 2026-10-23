-- Root-run controlled SQLSTATE 57014 regression. No waiting, network or provider work.
-- Every fixture function/task/adapter/counter/event change ROLLBACK.
BEGIN;
SET LOCAL statement_timeout='15s';
SET LOCAL lock_timeout='500ms';
SELECT pg_advisory_xact_lock(2026083000261022);
DO $guard$
BEGIN
 IF EXISTS(SELECT 1 FROM cron.job WHERE jobid=596 AND active)
  OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE status='LEASED')
  OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_provider_lease WHERE released_at IS NULL)
 THEN RAISE EXCEPTION 'PAUSE_AND_RECONCILE_BEFORE_CANCELLATION_TEST';END IF;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.acquisition_control a
  JOIN population_top3000_202608_202609_v1.pipeline_control p USING(singleton)
  WHERE a.singleton AND a.owner_id='top3000-recovery-20261023-root-4b1314bcb5c2' AND a.owner_fence=2 AND a.stop_generation=0
   AND a.owner_id=p.expected_owner_id AND a.owner_fence=p.expected_owner_fence AND a.stop_generation=p.expected_stop_generation
   AND a.scope_sha256=p.scope_sha256 AND a.scope_sha256='8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd'
   AND a.enabled AND NOT a.stopped AND a.hold_reason IS NULL AND a.admitted_until>clock_timestamp()
   AND p.max_ticks-p.tick_count>=3 AND p.deadline>clock_timestamp())
 THEN RAISE EXCEPTION 'CURRENT_OWNER_SCOPE_STOP_ADMISSION_AND_TICK_BUDGET_REQUIRED';END IF;
END $guard$;
CREATE FUNCTION population_top3000_202608_202609_v1.pipeline_cancel_fixture_20261023_v1(p_attempt uuid,p_token text)
RETURNS jsonb LANGUAGE plpgsql SET search_path TO 'pg_catalog' AS $function$
DECLARE v_tid uuid;v_task population_top3000_202608_202609_v1.pipeline_task%rowtype;
BEGIN
 v_tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);
 SELECT * INTO v_task FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=v_tid;
 IF v_task.family_id<>'D02' OR v_task.stage<>'DERIVE' OR v_task.payload->>'fixture_only' IS DISTINCT FROM 'true'
  OR v_task.partition_key NOT IN('fixture-query-cancel-fast-20261023','fixture-query-cancel-independent-20261023','fixture-query-cancel-expired-20261023')
 THEN RAISE EXCEPTION 'CANCELLATION_FIXTURE_SCOPE_REQUIRED';END IF;
 IF v_task.payload->>'mode'='INDEPENDENT_HOLD' THEN
  RETURN jsonb_build_object('state','BLOCKED','blocker_code','FIXTURE_INDEPENDENT_WORK_EXECUTED',
   'resumption_condition',jsonb_build_object('fixture_only',true,'automatic_retry',false));
 END IF;
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(task_id,attempt_id,event,details)
 VALUES(v_tid,p_attempt,'CANCEL_FIXTURE_INNER_WRITE_MUST_ROLL_BACK','{"fixture_only":true}');
 RAISE EXCEPTION USING ERRCODE='57014',MESSAGE='CONTROLLED_QUERY_CANCEL_FIXTURE';
END $function$;
REVOKE ALL ON FUNCTION population_top3000_202608_202609_v1.pipeline_cancel_fixture_20261023_v1(uuid,text) FROM PUBLIC,anon,authenticated;

DO $test$
DECLARE v_cancel uuid;v_independent uuid;v_expired uuid;v_attempt uuid;v_expired_attempt uuid:=gen_random_uuid();
 v_token text:=replace(gen_random_uuid()::text,'-','')||replace(gen_random_uuid()::text,'-','');
 v_hash text;v_result jsonb;v_second jsonb;v_third jsonb;v_expired_result jsonb;v_replay jsonb;
 v_before jsonb;v_after jsonb;v_tasks bigint;v_admitted_until timestamptz;v_started timestamptz;v_elapsed_ms numeric;
 v_task population_top3000_202608_202609_v1.pipeline_task%rowtype;v_a population_top3000_202608_202609_v1.pipeline_attempt%rowtype;
 v_wrong_token_rejected boolean:=false;v_stale_fence_rejected boolean:=false;v_stop_rejected boolean:=false;
 v_counter_before integer;v_execution_before bigint;v_commit_count bigint;
BEGIN
 SELECT jsonb_build_object('provider',a.provider_requests,'http',a.http_invocations,'stored',a.stored_bytes,'reserved_bytes',a.reserved_bytes,
  'actual_usd',p.actual_usd,'accrued_usd',p.accrued_usd,'unavoidable_usd',p.unavoidable_usd,'reserved_request_usd',p.reserved_request_usd,
  'remaining_uncommitted_usd',p.remaining_uncommitted_usd,'reserve',p.repair_egress_reserve_usd,
  'owner',a.owner_id,'fence',a.owner_fence,'stop',a.stop_generation) INTO v_before
 FROM population_top3000_202608_202609_v1.acquisition_control a JOIN population_top3000_202608_202609_v1.pipeline_control p USING(singleton) WHERE a.singleton;
 SELECT count(*) INTO v_tasks FROM population_top3000_202608_202609_v1.pipeline_task;
 SELECT count(*) INTO v_commit_count FROM population_top3000_202608_202609_v1.pipeline_commit;
 SELECT admitted_until INTO v_admitted_until FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton;
 v_hash:=encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.pipeline_cancel_fixture_20261023_v1(uuid,text)'::regprocedure),'UTF8')),'hex');
 INSERT INTO population_top3000_202608_202609_v1.pipeline_adapter
 (family_id,stage,function_oid,function_sha256,resource_class,provider,enabled,reviewed_test_evidence,producer_revision,expected_batch_seconds)
 VALUES('D02','DERIVE','population_top3000_202608_202609_v1.pipeline_cancel_fixture_20261023_v1(uuid,text)'::regprocedure::oid,v_hash,'DATABASE',NULL,true,
  '{"fixture_only":true,"test":"pipeline_query_canceled_hold_rollback_test.sql"}','synthetic-cancellation-regression-20261023',1)
 ON CONFLICT(family_id,stage) DO UPDATE SET function_oid=EXCLUDED.function_oid,function_sha256=EXCLUDED.function_sha256,
  resource_class='DATABASE',provider=NULL,enabled=true,reviewed_test_evidence=EXCLUDED.reviewed_test_evidence,
  producer_revision=EXCLUDED.producer_revision,expected_batch_seconds=1;
 UPDATE population_top3000_202608_202609_v1.pipeline_task SET not_before='infinity' WHERE status IN('READY','BLOCKED');
 UPDATE population_top3000_202608_202609_v1.pipeline_task SET blocker_until=NULL WHERE status='BLOCKED';
 v_cancel:=population_top3000_202608_202609_v1.pipeline_enqueue_v1('D02','DERIVE','fixture-query-cancel-fast-20261023',
  '{"fixture_only":true,"mode":"CANCEL"}',jsonb_build_object('fixture_only',true,'revision','query-canceled-regression-v1','scope_sha256','8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd'));
 UPDATE population_top3000_202608_202609_v1.pipeline_task SET executions=5,max_executions=10,cumulative_failures=2,same_failure_count=1,
  last_failure_class='DATABASE_QUERY_CANCELED',last_failure_fingerprint='57014:DATABASE_QUERY_CANCELED:'||v_hash,
  checkpoint='{"fixture_only":true,"retained_checkpoint":7}',not_before=clock_timestamp() WHERE task_id=v_cancel;
 UPDATE population_top3000_202608_202609_v1.pipeline_control SET enabled=true,next_wakeup_at=clock_timestamp() WHERE singleton;
 v_started:=clock_timestamp();v_result:=population_top3000_202608_202609_v1.pipeline_tick_v1();
 v_elapsed_ms:=round(extract(epoch FROM clock_timestamp()-v_started)*1000,3);
 SELECT * INTO v_task FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=v_cancel;
 v_attempt:=v_task.attempt_id;
 SELECT * INTO v_a FROM population_top3000_202608_202609_v1.pipeline_attempt WHERE attempt_id=v_attempt;
 IF v_result->>'task_id' IS DISTINCT FROM v_cancel::text OR v_result#>>'{result,state}' IS DISTINCT FROM 'BLOCKED'
  OR v_task.status IS DISTINCT FROM 'BLOCKED' OR v_task.blocker_code IS DISTINCT FROM 'DATABASE_QUERY_CANCELED' OR v_task.blocker_until IS NOT NULL
  OR v_task.lease_until IS NOT NULL OR v_task.not_before<>'infinity'::timestamptz
  OR v_task.executions<>6 OR v_task.cumulative_failures<>3 OR v_task.same_failure_count<>2
  OR v_task.checkpoint<>'{"fixture_only":true,"retained_checkpoint":7}'::jsonb
  OR v_task.resumption_condition->>'automatic_retry' IS DISTINCT FROM 'false'
  OR v_a.outcome IS DISTINCT FROM 'BLOCKED' OR v_a.ended_at IS NULL
 THEN RAISE EXCEPTION 'QUERY_CANCELED_NOT_PERSISTED_AS_BOUNDED_FAILURE';END IF;
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_event WHERE task_id=v_cancel AND event='CANCEL_FIXTURE_INNER_WRITE_MUST_ROLL_BACK')
  OR (SELECT count(*) FROM population_top3000_202608_202609_v1.pipeline_event WHERE attempt_id=v_attempt AND event='FAILURE'
   AND details->>'class'='DATABASE_QUERY_CANCELED' AND details->>'retry'='false')<>1
 THEN RAISE EXCEPTION 'CANCELLATION_SUBTRANSACTION_OR_FAILURE_RECEIPT_INCORRECT';END IF;

 -- A separate eligible database task must execute after the canceled task is held.
 v_independent:=population_top3000_202608_202609_v1.pipeline_enqueue_v1('D02','DERIVE','fixture-query-cancel-independent-20261023',
  '{"fixture_only":true,"mode":"INDEPENDENT_HOLD"}',jsonb_build_object('fixture_only',true,'revision','query-canceled-regression-v1'));
 UPDATE population_top3000_202608_202609_v1.pipeline_control SET next_wakeup_at=clock_timestamp() WHERE singleton;
 v_second:=population_top3000_202608_202609_v1.pipeline_tick_v1();
 IF v_second->>'task_id' IS DISTINCT FROM v_independent::text OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task
  WHERE task_id=v_independent AND status='BLOCKED' AND blocker_code='FIXTURE_INDEPENDENT_WORK_EXECUTED' AND executions=1)
 THEN RAISE EXCEPTION 'CANCELLATION_HOLD_STILL_STARVES_INDEPENDENT_DATABASE_WORK';END IF;
 UPDATE population_top3000_202608_202609_v1.pipeline_control SET next_wakeup_at=clock_timestamp() WHERE singleton;
 v_third:=population_top3000_202608_202609_v1.pipeline_tick_v1();
 IF (SELECT count(*) FROM population_top3000_202608_202609_v1.pipeline_attempt WHERE task_id=v_cancel)<>1
  OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=v_cancel AND executions=6 AND cumulative_failures=3 AND status='BLOCKED')
 THEN RAISE EXCEPTION 'CANCELED_TASK_RETRIED_OR_COUNTERS_RESET';END IF;

 -- A synthetic attempt with an already-expired execution lease reproduces the
 -- second real bug: generic pipeline_fail would reject it with LEASE_LOST.
 v_expired:=population_top3000_202608_202609_v1.pipeline_enqueue_v1('D02','DERIVE','fixture-query-cancel-expired-20261023',
  '{"fixture_only":true,"mode":"CANCEL"}',jsonb_build_object('fixture_only',true,'revision','query-canceled-regression-v1'));
 UPDATE population_top3000_202608_202609_v1.pipeline_task SET status='LEASED',attempt_id=v_expired_attempt,
  lease_until=clock_timestamp()-interval '30 seconds',executions=1,owner_fence=2,stop_generation=0,
  checkpoint='{"fixture_only":true,"retained_checkpoint":11}' WHERE task_id=v_expired;
 INSERT INTO population_top3000_202608_202609_v1.pipeline_attempt(attempt_id,task_id,execution_number,token_sha256,owner_fence,stop_generation,adapter_sha256,started_at)
 VALUES(v_expired_attempt,v_expired,1,encode(sha256(convert_to(v_token,'UTF8')),'hex'),2,0,v_hash,clock_timestamp()-interval '120 seconds');
 BEGIN
  PERFORM population_top3000_202608_202609_v1.pipeline_record_database_cancel_v1(v_expired_attempt,'wrong-capability');
 EXCEPTION WHEN OTHERS THEN
  IF SQLERRM IS DISTINCT FROM 'CANCELLATION_ATTEMPT_CAPABILITY_INVALID' THEN RAISE;END IF;v_wrong_token_rejected:=true;
 END;
 BEGIN
  UPDATE population_top3000_202608_202609_v1.pipeline_attempt SET owner_fence=1 WHERE attempt_id=v_expired_attempt;
  PERFORM population_top3000_202608_202609_v1.pipeline_record_database_cancel_v1(v_expired_attempt,v_token);
 EXCEPTION WHEN OTHERS THEN
  IF SQLERRM IS DISTINCT FROM 'CANCELLATION_ATTEMPT_FENCE_OR_STOP_CHANGED' THEN RAISE;END IF;v_stale_fence_rejected:=true;
 END;
 BEGIN
  UPDATE population_top3000_202608_202609_v1.pipeline_attempt SET stop_generation=1 WHERE attempt_id=v_expired_attempt;
  PERFORM population_top3000_202608_202609_v1.pipeline_record_database_cancel_v1(v_expired_attempt,v_token);
 EXCEPTION WHEN OTHERS THEN
  IF SQLERRM IS DISTINCT FROM 'CANCELLATION_ATTEMPT_FENCE_OR_STOP_CHANGED' THEN RAISE;END IF;v_stop_rejected:=true;
 END;
 IF NOT v_wrong_token_rejected OR NOT v_stale_fence_rejected OR NOT v_stop_rejected THEN RAISE EXCEPTION 'CANCELLATION_CAPABILITY_FENCE_OR_STOP_GUARD_FAILED';END IF;
 -- Expiry grants no new work; it must not prevent recording the already running failure.
 UPDATE population_top3000_202608_202609_v1.acquisition_control SET admitted_until=clock_timestamp()-interval '1 second' WHERE singleton;
 UPDATE population_top3000_202608_202609_v1.pipeline_control SET enabled=true WHERE singleton;
 v_expired_result:=population_top3000_202608_202609_v1.pipeline_record_database_cancel_v1(v_expired_attempt,v_token);
 v_replay:=population_top3000_202608_202609_v1.pipeline_record_database_cancel_v1(v_expired_attempt,v_token);
 UPDATE population_top3000_202608_202609_v1.acquisition_control SET admitted_until=v_admitted_until WHERE singleton;
 IF v_expired_result->>'state' IS DISTINCT FROM 'BLOCKED' OR v_replay->>'state' IS DISTINCT FROM 'ALREADY_RECORDED'
  OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=v_expired AND status='BLOCKED'
   AND blocker_code='DATABASE_QUERY_CANCELED' AND executions=1 AND cumulative_failures=1 AND same_failure_count=1
   AND checkpoint='{"fixture_only":true,"retained_checkpoint":11}'::jsonb AND lease_until IS NULL
   AND resumption_condition#>>'{details,execution_lease_expired}'='true' AND resumption_condition#>>'{details,admission_expired}'='true')
  OR (SELECT count(*) FROM population_top3000_202608_202609_v1.pipeline_event WHERE attempt_id=v_expired_attempt AND event='FAILURE')<>1
 THEN RAISE EXCEPTION 'EXPIRED_CANCELLATION_FAILURE_OR_REPLAY_NOT_RECORDED_SAFELY';END IF;

 SELECT jsonb_build_object('provider',a.provider_requests,'http',a.http_invocations,'stored',a.stored_bytes,'reserved_bytes',a.reserved_bytes,
  'actual_usd',p.actual_usd,'accrued_usd',p.accrued_usd,'unavoidable_usd',p.unavoidable_usd,'reserved_request_usd',p.reserved_request_usd,
  'remaining_uncommitted_usd',p.remaining_uncommitted_usd,'reserve',p.repair_egress_reserve_usd,
  'owner',a.owner_id,'fence',a.owner_fence,'stop',a.stop_generation) INTO v_after
 FROM population_top3000_202608_202609_v1.acquisition_control a JOIN population_top3000_202608_202609_v1.pipeline_control p USING(singleton) WHERE a.singleton;
 IF v_before IS DISTINCT FROM v_after OR (SELECT count(*) FROM population_top3000_202608_202609_v1.pipeline_task)<>v_tasks+3
  OR (SELECT count(*) FROM population_top3000_202608_202609_v1.pipeline_commit)<>v_commit_count
  OR EXISTS(SELECT 1 FROM cron.job WHERE jobid=596 AND active)
  OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_provider_lease WHERE released_at IS NULL)
 THEN RAISE EXCEPTION 'CANCELLATION_TEST_CHANGED_EXTERNAL_COUNTERS_OR_CASCADED';END IF;
 PERFORM set_config('top3000.query_canceled_test',jsonb_build_object('result','PASS_DATABASE_CANCELLATION_PERSISTED_NO_AUTO_RETRY',
  'receipt_event','DATABASE_QUERY_CANCELED_HOLD_ROLLBACK_VERIFIED','controlled_cancel_elapsed_ms',v_elapsed_ms,
  'cancel_dispatch',v_result,'independent_dispatch',v_second,'third_tick',v_third,
  'expired_lease_and_admission_result',v_expired_result,'idempotent_replay',v_replay,
  'wrong_token_rejected',v_wrong_token_rejected,'stale_fence_rejected',v_stale_fence_rejected,'stop_generation_rejected',v_stop_rejected,
  'failure_counters_incremented_not_reset',true,'inner_write_rolled_back',true,'provider_requested',false,
  'dispatcher_sha256',encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.pipeline_tick_v1()'::regprocedure),'UTF8')),'hex'),
  'cancel_recorder_sha256',encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.pipeline_record_database_cancel_v1(uuid,text)'::regprocedure),'UTF8')),'hex'))::text,true);
END $test$;
SELECT current_setting('top3000.query_canceled_test')::jsonb result;
ROLLBACK;
