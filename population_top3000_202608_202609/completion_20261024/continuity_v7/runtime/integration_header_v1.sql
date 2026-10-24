BEGIN;
SET LOCAL lock_timeout='10s'; SET LOCAL statement_timeout='45s'; SET LOCAL "TimeZone"='UTC';
DO $guard$
DECLARE q jsonb;claim jsonb;row record;actual text;
BEGIN
 PERFORM population_top3000_202608_202609_v1.continuation_guard_v6('top3000-completion-20261024-root-5db48b32d103',3,1);
 SELECT details INTO STRICT q FROM population_top3000_202608_202609_v1.pipeline_event WHERE event_id=23928 AND event='ENGINEERING_IMPLEMENTATION_ASSIGNMENT_V5';
 IF encode(sha256(convert_to(q::text,'UTF8')),'hex')<>'528a828f527b339f0af7d79d93d61c9664bc954841fe6b8513be8e6dd1192b91' OR (q->>'not_after')::timestamptz<=clock_timestamp() THEN RAISE EXCEPTION 'SEQUENCING_ASSIGNMENT_CHANGED';END IF;
 SELECT details INTO STRICT claim FROM population_top3000_202608_202609_v1.pipeline_event WHERE event_id=REPLACE_CLAIM_EVENT_ID AND event='ENGINEERING_ASSIGNMENT_CLAIMED_V5';
 IF claim->>'assignment_event_id' IS DISTINCT FROM '23928' OR claim->>'assignment_details_sha256' IS DISTINCT FROM '528a828f527b339f0af7d79d93d61c9664bc954841fe6b8513be8e6dd1192b91' OR claim->>'automation_run_id' IS DISTINCT FROM 'REPLACE_RUN_ID' OR claim->>'executor' IS DISTINCT FROM 'automation:6adc2c96db348191bca7cf4eacfdac32' OR claim->>'expires_at' IS NULL OR (claim->>'expires_at')::timestamptz<=clock_timestamp() OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_event WHERE event IN('ENGINEERING_ASSIGNMENT_COMPLETED_V5','ENGINEERING_ASSIGNMENT_EFFECTS_RECONCILED_RELEASE_V5') AND details->>'claim_event_id'='REPLACE_CLAIM_EVENT_ID') THEN RAISE EXCEPTION 'SEQUENCING_EXACT_ACTIVE_CLAIM_REQUIRED';END IF;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_event WHERE event IN('ENGINEERING_ASSIGNMENT_EFFECTS_RECONCILED_RELEASE_V5','ENGINEERING_ASSIGNMENT_COMPLETED_V5') AND details->>'claim_event_id'='24525' AND details->>'effects_known'='true') THEN RAISE EXCEPTION 'PRIOR_COHORT_CLAIM_UNRECONCILED';END IF;
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_event e WHERE e.event='ENGINEERING_ASSIGNMENT_CLAIMED_V5' AND e.event_id<>REPLACE_CLAIM_EVENT_ID AND NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_event done WHERE done.event IN('ENGINEERING_ASSIGNMENT_COMPLETED_V5','ENGINEERING_ASSIGNMENT_EFFECTS_RECONCILED_RELEASE_V5') AND done.details->>'claim_event_id'=e.event_id::text AND done.details->>'effects_known'='true')) THEN RAISE EXCEPTION 'OTHER_ENGINEERING_CLAIM_UNRECONCILED';END IF;
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE status='LEASED') OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_attempt WHERE outcome='RUNNING') OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_provider_lease WHERE released_at IS NULL) THEN RAISE EXCEPTION 'SEQUENCING_ACTIVE_OPERATION';END IF;
 FOR row IN SELECT value FROM jsonb_array_elements(q#>'{input_versions,installed_function_preimages}') LOOP
 SELECT encode(sha256(convert_to(pg_get_functiondef(p.oid),'UTF8')),'hex') INTO actual FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='population_top3000_202608_202609_v1' AND p.proname=row.value->>'proname' AND pg_get_function_identity_arguments(p.oid)=row.value->>'signature';
 IF actual IS DISTINCT FROM row.value->>'definition_sha256' THEN RAISE EXCEPTION 'SEQUENCING_PREIMAGE_CHANGED:%',row.value->>'proname';END IF;END LOOP;
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_adapter WHERE family_id='F05') THEN RAISE EXCEPTION 'F05_ALREADY_REGISTERED';END IF;
END;$guard$;
