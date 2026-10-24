BEGIN;
SET LOCAL lock_timeout='5s'; SET LOCAL statement_timeout='90s';
DO $g$
DECLARE r record; h text;
BEGIN
 PERFORM population_top3000_202608_202609_v1.continuation_guard_v6('top3000-completion-20261024-root-5db48b32d103',3,1);
 IF (SELECT count(*) FROM population_top3000_202608_202609_v1.pipeline_event e WHERE e.event_id IN(29634,29635) AND e.event='ENGINEERING_ASSIGNMENT_CLAIMED_V5' AND e.details->>'executor'='root:top3000-completion-20261024-root-5db48b32d103' AND (e.details->>'expires_at')::timestamptz>clock_timestamp() AND NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_event d WHERE d.event IN('ENGINEERING_ASSIGNMENT_COMPLETED_V5','ENGINEERING_ASSIGNMENT_EFFECTS_RECONCILED_RELEASE_V5') AND d.details->>'claim_event_id'=e.event_id::text))<>2 THEN RAISE EXCEPTION 'EXACT_ROOT_CLAIMS_REQUIRED';END IF;
 FOR r IN SELECT key,value FROM jsonb_each_text($hash${"pipeline_tick_v1":"e697b7f01821471ceaabf5cb13870976937b69d6d82f220c553dc0794a8ec89b","ref01_date_admit_v4":"8e38052f114ca6ddfb43b360c02dbe191e412d7bae7621eabd2cf6462c070da4","ref01_date_bundle_v4":"cc5437f37289948c70d0ec44b094f06ff7b047832dc7dfea1d639952ac0774bb","ref01_date_commit_v4":"10373f73f6ef3a6350109c8c7c54ef72574a5422413152ede6e1e2310871cd46","ref01_date_acquire_v4":"d85e4c2ea0e13b4150219ecc2819338fd751e7e142e195846147e2d97056f4f6","ref01_date_context_v4":"0da8c55f043204516a064d507439b0b45b7158443f1fed27eaf072cf21d24739"}$hash$::jsonb) LOOP
 SELECT encode(sha256(convert_to(pg_get_functiondef(oid),'UTF8')),'hex') INTO STRICT h FROM pg_proc WHERE pronamespace='population_top3000_202608_202609_v1'::regnamespace AND proname=r.key;
 IF h IS DISTINCT FROM r.value THEN RAISE EXCEPTION 'CODE_PREIMAGE_CHANGED %',r.key;END IF;
 END LOOP;
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE family_id='REF01' AND status='LEASED') OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_attempt a JOIN population_top3000_202608_202609_v1.pipeline_task t USING(task_id) WHERE t.family_id='REF01' AND a.outcome='RUNNING') THEN RAISE EXCEPTION 'REF01_ACTIVE_OPERATION';END IF;
 PERFORM population_top3000_202608_202609_v1.d02_daily_resource_guard_v2();
END;$g$;

