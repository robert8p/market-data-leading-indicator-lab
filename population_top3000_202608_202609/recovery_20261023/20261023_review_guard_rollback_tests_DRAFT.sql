-- DRAFT: controller executes only after applying patch, with cron 596 paused.
-- Every fixture and scheduler change rolls back; no worker/network adapter is called.
BEGIN;
SET LOCAL lock_timeout='3s';
SET LOCAL statement_timeout='40s';
SELECT pg_advisory_xact_lock(2026083000261022);
DO $test$
DECLARE r jsonb; tid uuid; aid uuid; token text:='ROLLBACK_FIXTURE_ONLY';
 owner_text text; fence_number bigint; stop_number bigint; ticks_before bigint;
 original_deadline timestamptz; accrued_before numeric; reserved_before numeric;
 bytes_before bigint; adapter_hash text; rejected boolean;
BEGIN
 SELECT owner_id,owner_fence,stop_generation INTO owner_text,fence_number,stop_number
  FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton FOR UPDATE;
 PERFORM 1 FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton FOR UPDATE;
 IF owner_text IS DISTINCT FROM 'top3000-recovery-20261023-root-4b1314bcb5c2' OR fence_number<>2 OR stop_number<>0 THEN RAISE EXCEPTION 'UNEXPECTED_CONTROLLER';END IF;
 IF EXISTS(SELECT 1 FROM cron.job WHERE jobid=596 AND active) THEN RAISE EXCEPTION 'PAUSE_CRON_FIRST';END IF;
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE status='LEASED')
  OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_provider_lease WHERE released_at IS NULL) THEN RAISE EXCEPTION 'DRAIN_OR_RECONCILE_LIVE_LEASES_FIRST';END IF;
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_adapter WHERE enabled AND resource_class<>'DATABASE') THEN RAISE EXCEPTION 'RETAINED_ONLY_TEST_REQUIRED';END IF;
 SELECT tick_count INTO ticks_before FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton;

 -- Persistent stop: disarm once, preserve stop, do not consume task/tick or wake.
 BEGIN
  UPDATE population_top3000_202608_202609_v1.acquisition_control SET stopped=true WHERE singleton;
  PERFORM cron.alter_job(596,active=>true);
  r:=population_top3000_202608_202609_v1.pipeline_tick_v1();
  IF r->>'state' IS DISTINCT FROM 'HELD' OR EXISTS(SELECT 1 FROM cron.job WHERE jobid=596 AND active)
   OR (SELECT tick_count FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton)<>ticks_before
   OR NOT(SELECT stopped FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton) THEN RAISE EXCEPTION 'STOP_GUARD_FAILED: %',r;END IF;
  rejected:=false;
  BEGIN PERFORM population_top3000_202608_202609_v1.pipeline_wake_v1(owner_text,fence_number,'ROLLBACK_STOP_TEST');
  EXCEPTION WHEN OTHERS THEN IF SQLERRM<>'OWNER_STOP_OR_ADMISSION' THEN RAISE;END IF;rejected:=true;END;
  IF NOT rejected THEN RAISE EXCEPTION 'WAKE_CLEARED_STOP';END IF;
  RAISE SQLSTATE 'ZT001';
 EXCEPTION WHEN SQLSTATE 'ZT001' THEN RAISE NOTICE 'PASS stop disarms, persists and rejects wake';END;

 -- Wrong expected fence cannot activate the controller or change its ownership.
 BEGIN
  UPDATE population_top3000_202608_202609_v1.pipeline_control SET expected_owner_fence=fence_number-1 WHERE singleton;
  PERFORM cron.alter_job(596,active=>true);
  r:=population_top3000_202608_202609_v1.pipeline_tick_v1();
  IF r->>'state' IS DISTINCT FROM 'OWNER_OR_STOP_CHANGED' OR EXISTS(SELECT 1 FROM cron.job WHERE jobid=596 AND active) THEN RAISE EXCEPTION 'OWNER_GUARD_FAILED: %',r;END IF;
  rejected:=false;
  BEGIN PERFORM population_top3000_202608_202609_v1.pipeline_wake_v1(owner_text,fence_number,'ROLLBACK_OWNER_TEST');
  EXCEPTION WHEN OTHERS THEN IF SQLERRM<>'OWNER_OR_STOP_CHANGED' THEN RAISE;END IF;rejected:=true;END;
  IF NOT rejected OR (SELECT expected_owner_fence FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton)<>fence_number-1 THEN RAISE EXCEPTION 'WAKE_REWROTE_OWNERSHIP';END IF;
  RAISE SQLSTATE 'ZT001';
 EXCEPTION WHEN SQLSTATE 'ZT001' THEN RAISE NOTICE 'PASS stale fence disarms and wake does not reconcile it';END;

 -- Missing budget evidence is a persistent hold, not indefinite two-second polling.
 BEGIN
  UPDATE population_top3000_202608_202609_v1.pipeline_control SET enabled=true,budget_verified_at=NULL WHERE singleton;
  PERFORM cron.alter_job(596,active=>true);
  r:=population_top3000_202608_202609_v1.pipeline_tick_v1();
  IF r->>'state' IS DISTINCT FROM 'BUDGET_HOLD' OR EXISTS(SELECT 1 FROM cron.job WHERE jobid=596 AND active) THEN RAISE EXCEPTION 'BUDGET_GUARD_FAILED: %',r;END IF;
  rejected:=false;
  BEGIN PERFORM population_top3000_202608_202609_v1.pipeline_wake_v1(owner_text,fence_number,'ROLLBACK_BUDGET_TEST');
  EXCEPTION WHEN OTHERS THEN IF SQLERRM<>'BUDGET_HOLD' THEN RAISE;END IF;rejected:=true;END;
  IF NOT rejected THEN RAISE EXCEPTION 'WAKE_IGNORED_BUDGET';END IF;
  RAISE SQLSTATE 'ZT001';
 EXCEPTION WHEN SQLSTATE 'ZT001' THEN RAISE NOTICE 'PASS budget hold disarms and rejects wake';END;

 -- Future stop generations are allowed only after both controls match and no stop is held.
 BEGIN
  UPDATE population_top3000_202608_202609_v1.acquisition_control SET stop_generation=stop_number+1 WHERE singleton;
  UPDATE population_top3000_202608_202609_v1.pipeline_control SET expected_stop_generation=stop_number+1,enabled=false WHERE singleton;
  r:=population_top3000_202608_202609_v1.pipeline_wake_v1(owner_text,fence_number,'ROLLBACK_MATCHED_GENERATION_TEST');
  IF r->>'state' IS DISTINCT FROM 'SCHEDULED' OR NOT EXISTS(SELECT 1 FROM cron.job WHERE jobid=596 AND active)
   OR (SELECT stop_generation FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton)<>stop_number+1 THEN RAISE EXCEPTION 'MATCHED_GENERATION_WAKE_FAILED: %',r;END IF;
  RAISE SQLSTATE 'ZT001';
 EXCEPTION WHEN SQLSTATE 'ZT001' THEN RAISE NOTICE 'PASS matching generation wake preserves generation';END;

 -- No eligible real task may run in remaining synthetic accounting tests.
 UPDATE population_top3000_202608_202609_v1.pipeline_task SET not_before='infinity' WHERE status='READY';
 UPDATE population_top3000_202608_202609_v1.pipeline_task SET blocker_until=NULL WHERE status='BLOCKED';

 BEGIN
  tid:=population_top3000_202608_202609_v1.pipeline_enqueue_v1('REF01','NORMALIZE','rollback-execution-limit-'||gen_random_uuid()::text,'{}','{"rollback_fixture":"execution-limit"}');
  UPDATE population_top3000_202608_202609_v1.pipeline_task SET executions=max_executions WHERE task_id=tid;
  UPDATE population_top3000_202608_202609_v1.pipeline_control SET enabled=true,next_wakeup_at=clock_timestamp() WHERE singleton;
  r:=population_top3000_202608_202609_v1.pipeline_tick_v1();
  IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=tid AND status='BLOCKED' AND blocker_code='EXECUTION_LIMIT' AND executions=max_executions)
   OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_attempt WHERE task_id=tid) THEN RAISE EXCEPTION 'EXECUTION_LIMIT_FAILED: %',r;END IF;
  RAISE SQLSTATE 'ZT001';
 EXCEPTION WHEN SQLSTATE 'ZT001' THEN RAISE NOTICE 'PASS exhausted task is explicitly blocked without dispatch';END;

 -- The real fail function must preserve the original deadline for uncertain effects.
 BEGIN
  tid:=population_top3000_202608_202609_v1.pipeline_enqueue_v1('REF01','NORMALIZE','rollback-uncertain-reservation-'||gen_random_uuid()::text,'{}','{"rollback_fixture":"uncertain-reservation"}');
  aid:=gen_random_uuid();original_deadline:=clock_timestamp()+interval '60 seconds';
  SELECT function_sha256 INTO adapter_hash FROM population_top3000_202608_202609_v1.pipeline_adapter WHERE family_id='REF01' AND stage='NORMALIZE' AND enabled;
  IF adapter_hash IS NULL THEN RAISE EXCEPTION 'REVIEWED_REF01_ADAPTER_REQUIRED';END IF;
  UPDATE population_top3000_202608_202609_v1.pipeline_task SET status='LEASED',attempt_id=aid,lease_until=original_deadline,owner_fence=fence_number,stop_generation=stop_number,executions=1 WHERE task_id=tid;
  INSERT INTO population_top3000_202608_202609_v1.pipeline_attempt(attempt_id,task_id,execution_number,token_sha256,owner_fence,stop_generation,adapter_sha256)
   VALUES(aid,tid,1,encode(sha256(convert_to(token,'UTF8')),'hex'),fence_number,stop_number,adapter_hash);
  SELECT accrued_usd,reserved_request_usd INTO accrued_before,reserved_before FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton;
  SELECT reserved_bytes INTO bytes_before FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton;
  UPDATE population_top3000_202608_202609_v1.pipeline_control SET enabled=true,next_wakeup_at=clock_timestamp(),idle_rechecks=0,remaining_uncommitted_usd=remaining_uncommitted_usd-0.01,reserved_request_usd=reserved_request_usd+0.01 WHERE singleton;
  UPDATE population_top3000_202608_202609_v1.acquisition_control SET reserved_bytes=reserved_bytes+64 WHERE singleton;
  INSERT INTO population_top3000_202608_202609_v1.pipeline_provider_lease(attempt_id,provider,reserved_bytes,reserved_usd) VALUES(aid,'ROLLBACK_NO_NETWORK',64,0.01);
  r:=population_top3000_202608_202609_v1.pipeline_fail_v1(aid,token,'TRANSPORT_READ','rollback-unknown-effect','{"rollback_fixture":true}',0);
  IF r->>'state' IS DISTINCT FROM 'BLOCKED' OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=tid AND blocker_code='UNCERTAIN_EFFECT' AND lease_until=original_deadline) THEN RAISE EXCEPTION 'UNCERTAIN_DEADLINE_NOT_PRESERVED: %',r;END IF;
  r:=population_top3000_202608_202609_v1.pipeline_tick_v1();
  IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_provider_lease WHERE attempt_id=aid AND released_at IS NULL) THEN RAISE EXCEPTION 'RESERVATION_RELEASED_BEFORE_ORIGINAL_DEADLINE';END IF;
  UPDATE population_top3000_202608_202609_v1.pipeline_task SET lease_until=clock_timestamp()-interval '1 second' WHERE task_id=tid;
  UPDATE population_top3000_202608_202609_v1.pipeline_control SET enabled=true,next_wakeup_at=clock_timestamp() WHERE singleton;
  r:=population_top3000_202608_202609_v1.pipeline_tick_v1();
  IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=tid AND status='BLOCKED' AND blocker_code='UNCERTAIN_EFFECT' AND lease_until IS NULL)
   OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_provider_lease WHERE attempt_id=aid AND released_at IS NOT NULL AND actual_usd=0.01)
   OR (SELECT accrued_usd FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton)<>accrued_before+0.01
   OR (SELECT reserved_request_usd FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton)<>reserved_before
   OR (SELECT reserved_bytes FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton)<>bytes_before THEN RAISE EXCEPTION 'UNCERTAIN_RESERVATION_RECONCILIATION_FAILED: %',r;END IF;
  UPDATE population_top3000_202608_202609_v1.pipeline_control SET enabled=true,next_wakeup_at=clock_timestamp() WHERE singleton;
  PERFORM population_top3000_202608_202609_v1.pipeline_tick_v1();
  IF (SELECT accrued_usd FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton)<>accrued_before+0.01
   OR (SELECT executions FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=tid)<>1 THEN RAISE EXCEPTION 'UNCERTAIN_RECONCILIATION_REPLAY_NOT_IDEMPOTENT';END IF;
  RAISE SQLSTATE 'ZT001';
 EXCEPTION WHEN SQLSTATE 'ZT001' THEN RAISE NOTICE 'PASS uncertain effects keep original deadline, accrue once, remain blocked';END;

 -- An unreported expired external attempt must also block, never auto-retry.
 BEGIN
  tid:=population_top3000_202608_202609_v1.pipeline_enqueue_v1('REF01','NORMALIZE','rollback-expired-provider-'||gen_random_uuid()::text,'{}','{"rollback_fixture":"expired-provider"}');
  aid:=gen_random_uuid();
  SELECT function_sha256 INTO adapter_hash FROM population_top3000_202608_202609_v1.pipeline_adapter WHERE family_id='REF01' AND stage='NORMALIZE' AND enabled;
  UPDATE population_top3000_202608_202609_v1.pipeline_task SET status='LEASED',attempt_id=aid,lease_until=clock_timestamp()-interval '1 second',owner_fence=fence_number,stop_generation=stop_number,executions=1 WHERE task_id=tid;
  INSERT INTO population_top3000_202608_202609_v1.pipeline_attempt(attempt_id,task_id,execution_number,token_sha256,owner_fence,stop_generation,adapter_sha256)
   VALUES(aid,tid,1,encode(sha256(convert_to(token,'UTF8')),'hex'),fence_number,stop_number,adapter_hash);
  UPDATE population_top3000_202608_202609_v1.pipeline_control SET enabled=true,next_wakeup_at=clock_timestamp(),remaining_uncommitted_usd=remaining_uncommitted_usd-0.01,reserved_request_usd=reserved_request_usd+0.01 WHERE singleton;
  UPDATE population_top3000_202608_202609_v1.acquisition_control SET reserved_bytes=reserved_bytes+64 WHERE singleton;
  INSERT INTO population_top3000_202608_202609_v1.pipeline_provider_lease(attempt_id,provider,reserved_bytes,reserved_usd) VALUES(aid,'ROLLBACK_NO_NETWORK',64,0.01);
  r:=population_top3000_202608_202609_v1.pipeline_tick_v1();
  IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=tid AND status='BLOCKED' AND blocker_code='UNCERTAIN_EFFECT' AND executions=1)
   OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_provider_lease WHERE attempt_id=aid AND released_at IS NULL) THEN RAISE EXCEPTION 'EXPIRED_PROVIDER_UNKNOWN_EFFECT_RETRIED: %',r;END IF;
  RAISE SQLSTATE 'ZT001';
 EXCEPTION WHEN SQLSTATE 'ZT001' THEN RAISE NOTICE 'PASS expired unreconciled provider attempt remains blocked';END;
END $test$;
SELECT 'PASS_GUARD_AND_RESERVATION_ROLLBACK_TESTS' AS result;
ROLLBACK;
