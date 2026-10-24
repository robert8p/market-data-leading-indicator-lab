BEGIN;
SET LOCAL statement_timeout='60s';
SET LOCAL lock_timeout='6s';
SELECT pg_advisory_xact_lock(2026083000261022);
DO $guard$
DECLARE ctl jsonb;
BEGIN
 ctl:=population_top3000_202608_202609_v1.continuation_guard_v6('top3000-completion-20261024-root-5db48b32d103',3,1);
 PERFORM 1 FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton FOR UPDATE;
 PERFORM 1 FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton FOR UPDATE;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_event WHERE event_id=27679 AND event='ENGINEERING_ASSIGNMENT_CLAIMED_V5' AND details->>'assignment_event_id'='27678' AND details->>'executor'='controller:top3000-completion-20261024-root-5db48b32d103' AND (details->>'expires_at')::timestamptz>clock_timestamp())
 OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_event WHERE details->>'claim_event_id'='27679' AND event IN('ENGINEERING_ASSIGNMENT_COMPLETED_V5','ENGINEERING_ASSIGNMENT_EFFECTS_RECONCILED_RELEASE_V5'))
 THEN RAISE EXCEPTION 'V8_LIVE_CONTROLLER_CLAIM_REQUIRED';END IF;
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE status='LEASED') OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_attempt WHERE outcome='RUNNING') OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_provider_lease WHERE released_at IS NULL) OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.connector_probe_lease_v1 WHERE released_at IS NULL) THEN RAISE EXCEPTION 'V8_ACTIVE_OR_UNRECONCILED_OPERATION';END IF;
 IF encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.ctx02_clear_page_admission_v6(text,bigint,bigint,jsonb)'::regprocedure),'UTF8')),'hex') IS DISTINCT FROM '3337167897438f07077b666c57f252be90d6ffbf2f54c2e65a9fd9fd4eee430b'
 OR encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.continuation_dispatch_v6(text,bigint,bigint)'::regprocedure),'UTF8')),'hex') IS DISTINCT FROM 'eaf9bb45a3f5e3e6f23399a3758afe9e1603c3f2df2c1e6ad1751f6778b2d573'
 OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.continuation_handler_v6 WHERE kind='CTX02_PAGE_ADMISSION' AND function_sha256='3337167897438f07077b666c57f252be90d6ffbf2f54c2e65a9fd9fd4eee430b' AND bounds->>'absolute_page_ceiling'='1247' AND review_event_id=27067)
 THEN RAISE EXCEPTION 'V8_REVIEWED_PREIMAGE_CHANGED';END IF;
END $guard$;
