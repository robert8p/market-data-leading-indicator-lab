-- Execute header, reviewed classifier+dispatcher+classification registration, footer in one transaction.
BEGIN;
SET LOCAL lock_timeout='8s';SET LOCAL statement_timeout='60s';
DO $guard$
DECLARE ctl jsonb;
BEGIN
 ctl:=population_top3000_202608_202609_v1.continuation_guard_v6('top3000-completion-20261024-root-5db48b32d103',3,1);
 IF encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.continuation_classify_v6(text,text,text)'::regprocedure),'UTF8')),'hex')<>'dde9dee9b8e9302f7df169ceb9f8daf78f443d166e6f76d6f230d6756a02aa84' OR encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.continuation_dispatch_v6(text,bigint,bigint)'::regprocedure),'UTF8')),'hex')<>'bd97d65af2be7709f0f32c5db93377a8661d45489ed3f750ae046a0397044798' THEN RAISE EXCEPTION 'CONTINUATION_V7_EXACT_FUNCTION_PREIMAGES_CHANGED';END IF;
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE status='LEASED') OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_attempt WHERE outcome='RUNNING') OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_provider_lease WHERE released_at IS NULL) OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.connector_probe_lease_v1 WHERE released_at IS NULL) THEN RAISE EXCEPTION 'CONTINUATION_V7_ACTIVE_OPERATIONS_RETRY';END IF;
END;$guard$;
