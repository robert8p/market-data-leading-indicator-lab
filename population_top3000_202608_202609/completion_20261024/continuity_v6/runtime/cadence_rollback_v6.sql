-- Execute BEGIN; reviewed cadence-only function; this fixture. All task/control/cron effects rolled back.
SET LOCAL lock_timeout='5s'; SET LOCAL statement_timeout='30s';
CREATE TEMP TABLE cadence_receipt_v6(r jsonb) ON COMMIT DROP;
DO $t$
DECLARE r jsonb;h text;ready_n bigint;
BEGIN
 PERFORM pg_advisory_xact_lock(2026083000261022);
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE status='LEASED') OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_provider_lease WHERE released_at IS NULL) THEN RAISE EXCEPTION 'CADENCE_TEST_REQUIRES_NO_EXTERNAL_ATTEMPT';END IF;
 SELECT count(*) INTO ready_n FROM population_top3000_202608_202609_v1.pipeline_task WHERE status='READY';
 IF ready_n=0 THEN RAISE EXCEPTION 'CADENCE_FIXTURE_REQUIRES_READY_TASK';END IF;
 UPDATE population_top3000_202608_202609_v1.pipeline_task SET not_before=clock_timestamp()+interval '2minutes' WHERE status='READY';
 UPDATE population_top3000_202608_202609_v1.pipeline_control SET next_wakeup_at=clock_timestamp(),idle_rechecks=0 WHERE singleton;
 r:=population_top3000_202608_202609_v1.pipeline_tick_v1();
 IF r->>'state'<>'BOUNDED_WAIT' OR (SELECT schedule FROM cron.job WHERE jobid=596)<>'2 seconds' THEN RAISE EXCEPTION 'READY_CADENCE_NOT_PRESERVED %',r;END IF;
 UPDATE population_top3000_202608_202609_v1.pipeline_task SET status='BLOCKED',blocker_code='ROLLBACK_CADENCE_FIXTURE',resumption_condition=jsonb_build_object('reason','rollback-only cadence fixture') WHERE status='READY';
 UPDATE population_top3000_202608_202609_v1.pipeline_control SET next_wakeup_at=clock_timestamp(),idle_rechecks=0 WHERE singleton;
 r:=population_top3000_202608_202609_v1.pipeline_tick_v1();
 IF r->>'state'<>'BOUNDED_WAIT' OR (SELECT schedule FROM cron.job WHERE jobid=596)<>'17 seconds' THEN RAISE EXCEPTION 'IDLE_CADENCE_NOT_DESYNCHRONIZED %',r;END IF;
 h:=encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.pipeline_tick_v1()'::regprocedure),'UTF8')),'hex');
 INSERT INTO cadence_receipt_v6 VALUES(jsonb_build_object('result','PASS_CADENCE_ACTUAL_ROLLBACK_V6','ready_tasks',ready_n,'future_ready_schedule','2 seconds','idle_schedule','17 seconds','existing_next_wakeup_bound_preserved',true,'provider_calls',0,'production_changes_rolled_back',true,'installed_function_sha256',h));
END;$t$;
SELECT r FROM cadence_receipt_v6;
ROLLBACK;
