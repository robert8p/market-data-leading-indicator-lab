BEGIN;
SET LOCAL lock_timeout='5s'; SET LOCAL statement_timeout='45s';
DO $g$
BEGIN
 PERFORM population_top3000_202608_202609_v1.continuation_guard_v6('top3000-completion-20261024-root-5db48b32d103',3,1);
 IF (SELECT count(*) FROM population_top3000_202608_202609_v1.pipeline_event e WHERE e.event_id IN(29634,29635) AND e.event='ENGINEERING_ASSIGNMENT_CLAIMED_V5' AND e.details->>'executor'='root:top3000-completion-20261024-root-5db48b32d103' AND (e.details->>'expires_at')::timestamptz>clock_timestamp() AND NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_event d WHERE d.event IN('ENGINEERING_ASSIGNMENT_COMPLETED_V5','ENGINEERING_ASSIGNMENT_EFFECTS_RECONCILED_RELEASE_V5') AND d.details->>'claim_event_id'=e.event_id::text))<>2 THEN RAISE EXCEPTION 'EXACT_ROOT_CLAIMS_REQUIRED';END IF;
 IF encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.pipeline_tick_v1()'::regprocedure),'UTF8')),'hex')<>'b2cccfdda864831a3de0e45d64f4f658a3802f4398e0d010502dda196a3bb2b1' THEN RAISE EXCEPTION 'PIPELINE_PREIMAGE_CHANGED';END IF;
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE family_id='REF01' AND status='LEASED') THEN RAISE EXCEPTION 'REF01_ACTIVE_OPERATION';END IF;
END;$g$;
