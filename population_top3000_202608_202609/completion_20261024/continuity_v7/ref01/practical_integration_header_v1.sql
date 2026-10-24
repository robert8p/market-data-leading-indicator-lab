BEGIN;
SET LOCAL lock_timeout='5s';SET LOCAL statement_timeout='90s';
SELECT pg_advisory_xact_lock(2026083000261022);
DO $guard$
DECLARE a population_top3000_202608_202609_v1.acquisition_control%rowtype;p population_top3000_202608_202609_v1.pipeline_control%rowtype;
BEGIN
 SELECT * INTO STRICT a FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton FOR UPDATE;
 SELECT * INTO STRICT p FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton FOR UPDATE;
 IF a.owner_id IS DISTINCT FROM 'top3000-completion-20261024-root-5db48b32d103' OR a.owner_fence<>3 OR a.stop_generation<>1 OR a.stopped OR NOT a.enabled OR a.hold_reason IS NOT NULL OR NOT p.enabled OR a.admitted_until<=clock_timestamp() OR p.deadline<>TIMESTAMPTZ '2026-10-25T22:36:17.687194Z'
 OR p.budget_evidence#>>'{cohort_selection_policy,policy_sha256}' IS DISTINCT FROM '3e49d661fcdb8533e5ce88a85689b41e9dde71eeae2dc8578c9e31f9ae1738bb'
 OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_event e WHERE event_id=23464 AND event='ENGINEERING_ASSIGNMENT_CLAIMED_V5' AND details->>'assignment_event_id'='23382' AND (details->>'expires_at')::timestamptz>clock_timestamp())
 OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_event WHERE details->>'claim_event_id'='23464' AND event IN('ENGINEERING_ASSIGNMENT_COMPLETED_V5','ENGINEERING_ASSIGNMENT_EFFECTS_RECONCILED_RELEASE_V5'))
 OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE family_id='REF01' AND status='LEASED')
 THEN RAISE EXCEPTION 'PRACTICAL_INTEGRATION_AUTHORITY_OR_LEASE_REJECTED';END IF;
 PERFORM population_top3000_202608_202609_v1.ref01_adjudication_controller_v3(false);
 IF encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.ref01_adjudication_route_v3(uuid,text,text)'::regprocedure),'UTF8')),'hex') IS DISTINCT FROM '8d04efdbf940361a7f9b139f636053c57bfc157a846386563859fdd392248338' THEN RAISE EXCEPTION 'PRACTICAL_8d04efdbf940361a7f9b139f636053c57bfc157a846386563859fdd392248338_CHANGED';END IF;
END $guard$;
