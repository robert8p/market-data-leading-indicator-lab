-- Registered only by an independently reviewed integration; legacy unpinned tasks are never upgraded in place.
CREATE TABLE IF NOT EXISTS population_top3000_202608_202609_v1.sequencing_dependency_watch_v1(
 task_id uuid PRIMARY KEY REFERENCES population_top3000_202608_202609_v1.pipeline_task,
 input_versions_sha256 text NOT NULL, expected_blocker text NOT NULL,
 policy_sha256 text NOT NULL, cohort_sha256 text NOT NULL, acceptance_sha256 text NOT NULL,
 daily_run_id uuid NOT NULL,daily_input_sha256 text NOT NULL,
 prerequisite_task_id uuid NOT NULL REFERENCES population_top3000_202608_202609_v1.pipeline_task,
 required_proof_event text NOT NULL,required_proof_fields jsonb NOT NULL,
 adapter_sha256 text NOT NULL, registration_review_event_id bigint NOT NULL,
 woken_at timestamptz, created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 CHECK(jsonb_typeof(required_proof_fields)='object'),CHECK(expected_blocker='CTX06_PRACTICAL_DEPENDENCIES_REQUIRED_V2'));
ALTER TABLE population_top3000_202608_202609_v1.sequencing_dependency_watch_v1 ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON population_top3000_202608_202609_v1.sequencing_dependency_watch_v1 FROM PUBLIC,anon,authenticated;
CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.sequencing_dependency_wake_v1(p_owner text,p_fence bigint,p_stop bigint) RETURNS jsonb LANGUAGE plpgsql SET search_path TO 'pg_catalog' AS $fn$
DECLARE w record;n integer:=0;proof bigint;ctl jsonb;
BEGIN
 ctl:=population_top3000_202608_202609_v1.continuation_guard_v6(p_owner,p_fence,p_stop);
 FOR w IN SELECT q.*,t.input_versions,t.payload,t.family_id,t.stage,t.recheck_count,t.max_rechecks,t.executions,t.max_executions FROM population_top3000_202608_202609_v1.sequencing_dependency_watch_v1 q JOIN population_top3000_202608_202609_v1.pipeline_task t USING(task_id)
 WHERE q.woken_at IS NULL AND t.status='BLOCKED' AND t.blocker_code=q.expected_blocker AND t.family_id='CTX06' AND t.stage='ACQUIRE' AND t.recheck_count<t.max_rechecks AND t.executions<t.max_executions ORDER BY q.created_at,q.task_id LIMIT 24 FOR UPDATE OF q,t SKIP LOCKED LOOP
  IF w.policy_sha256 IS DISTINCT FROM '3e49d661fcdb8533e5ce88a85689b41e9dde71eeae2dc8578c9e31f9ae1738bb'
  OR w.cohort_sha256 IS DISTINCT FROM '2dc0f766befed6de6b052221c3c87482d507ba1bad931cbb98654eb3af899bec'
  OR w.acceptance_sha256 IS DISTINCT FROM '20b9dac759857e9b510ccaeca33e4a94d8c748d588fb0cc27740cc66c389b5f9'
  OR encode(sha256(convert_to(w.input_versions::text,'UTF8')),'hex') IS DISTINCT FROM w.input_versions_sha256
  OR w.input_versions->>'scope_sha256' IS DISTINCT FROM '8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd'
  OR w.payload->>'daily_run_id' IS DISTINCT FROM w.daily_run_id::text
  OR w.required_proof_fields->>'daily_run_id' IS DISTINCT FROM w.daily_run_id::text
  OR coalesce(w.input_versions->>'instrument_manifest_sha256','')!~'^[0-9a-f]{64}$'
  OR w.required_proof_fields->>'instrument_manifest_sha256' IS DISTINCT FROM w.input_versions->>'instrument_manifest_sha256'
  OR w.input_versions->>'policy_sha256' IS DISTINCT FROM w.policy_sha256 OR w.input_versions->>'cohort_sha256' IS DISTINCT FROM w.cohort_sha256
  OR w.input_versions->>'membership_acceptance_sha256' IS DISTINCT FROM w.acceptance_sha256
  OR w.input_versions->>'daily_input_versions_sha256' IS DISTINCT FROM w.daily_input_sha256
  OR w.required_proof_event IS DISTINCT FROM 'CTX06_PRACTICAL_FIRST_BATCH_VERIFIED_V2'
  OR w.required_proof_fields->>'result' IS DISTINCT FROM 'PASS_ACTUAL_FROZEN_COHORT_FIRST100'
  OR w.required_proof_fields->>'full_period_instrument_evidence_verified' IS DISTINCT FROM 'true'
  OR w.required_proof_fields->>'company_count' IS DISTINCT FROM '100' OR w.required_proof_fields->>'vote_rows' IS DISTINCT FROM '4200'
  OR w.required_proof_fields->>'cohort_sha256' IS DISTINCT FROM w.cohort_sha256 OR w.required_proof_fields->>'policy_sha256' IS DISTINCT FROM w.policy_sha256
  OR w.required_proof_fields->>'membership_acceptance_sha256' IS DISTINCT FROM w.acceptance_sha256
  OR w.required_proof_fields->>'daily_input_versions_sha256' IS DISTINCT FROM w.daily_input_sha256
  OR w.required_proof_fields->>'adapter_sha256' IS DISTINCT FROM w.adapter_sha256
  OR (SELECT budget_evidence#>>'{cohort_selection_policy,policy_sha256}' FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton) IS DISTINCT FROM w.policy_sha256
  OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.ref01_practical_membership_v1 m WHERE m.snapshot_sha256=w.cohort_sha256 AND m.acceptance_sha256=w.acceptance_sha256)
  OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.daily_shared_run_v1 d WHERE d.run_id=w.daily_run_id AND d.derived_at IS NOT NULL AND d.normalized_at IS NOT NULL AND d.derived_series=d.series_count AND d.normalized_series=d.series_count AND encode(sha256(convert_to(d.input_versions::text,'UTF8')),'hex')=w.daily_input_sha256)
  OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task t JOIN population_top3000_202608_202609_v1.pipeline_commit c USING(task_id) WHERE t.task_id=w.prerequisite_task_id AND t.status='COMPLETE' AND c.new_committed_units>0 AND c.receipt->'input_versions'->>'cohort_sha256'=w.cohort_sha256 AND c.receipt->'input_versions'->>'policy_sha256'=w.policy_sha256 AND c.receipt->'input_versions'->>'membership_acceptance_sha256'=w.acceptance_sha256)
  OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_adapter a WHERE a.family_id='CTX06' AND a.stage='ACQUIRE' AND a.enabled AND a.function_sha256=w.adapter_sha256 AND encode(sha256(convert_to(pg_get_functiondef(a.function_oid),'UTF8')),'hex')=w.adapter_sha256)
  OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_event e WHERE e.event_id=w.registration_review_event_id AND e.event='SEQUENCING_DEPENDENCY_REGISTRATION_REVIEW_V1' AND e.details->>'task_id'=w.task_id::text AND e.details->>'input_versions_sha256'=w.input_versions_sha256 AND e.details->>'result'='APPROVED' AND nullif(e.details->>'producer','') IS NOT NULL AND nullif(e.details->>'reviewer','') IS NOT NULL AND e.details->>'reviewer' IS DISTINCT FROM e.details->>'producer')
  THEN CONTINUE;END IF;
  SELECT e.event_id INTO proof FROM population_top3000_202608_202609_v1.pipeline_event e WHERE e.event=w.required_proof_event AND e.details @> w.required_proof_fields AND e.details->>'rolled_back'='true' AND e.details->>'source_and_membership_reconciled'='true' AND nullif(e.details->>'producer','') IS NOT NULL AND nullif(e.details->>'reviewer','') IS NOT NULL AND e.details->>'reviewer' IS DISTINCT FROM e.details->>'producer' ORDER BY e.event_id DESC LIMIT 1;
  IF proof IS NULL THEN CONTINUE;END IF;
  UPDATE population_top3000_202608_202609_v1.pipeline_task SET status='READY',blocker_code=NULL,blocker_until=NULL,resumption_condition=NULL,recheck_count=recheck_count+1,not_before=clock_timestamp() WHERE task_id=w.task_id;
  UPDATE population_top3000_202608_202609_v1.sequencing_dependency_watch_v1 SET woken_at=clock_timestamp() WHERE task_id=w.task_id;
  INSERT INTO population_top3000_202608_202609_v1.pipeline_event(task_id,event,details) VALUES(w.task_id,'SEQUENCING_DEPENDENCY_WOKEN_V1',ctl||jsonb_build_object('proof_event_id',proof,'registration_review_event_id',w.registration_review_event_id,'input_versions_sha256',w.input_versions_sha256,'cohort_sha256',w.cohort_sha256,'policy_sha256',w.policy_sha256,'prior_recheck_count',w.recheck_count,'preserved_executions',w.executions));n:=n+1;
 END LOOP;
 RETURN jsonb_build_object('woken',n,'maximum_examined',24,'legacy_tasks_reinterpreted',false);
END;$fn$;
REVOKE ALL ON FUNCTION population_top3000_202608_202609_v1.sequencing_dependency_wake_v1(text,bigint,bigint) FROM PUBLIC,anon,authenticated;
