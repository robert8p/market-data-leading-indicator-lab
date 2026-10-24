CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.ref01_review_admit_case_v4(p_run text, p_case text, p_expected_parent text, p_proposal jsonb, p_source_refs jsonb, p_verification uuid)
 RETURNS jsonb
 LANGUAGE plpgsql
 SET search_path TO 'pg_catalog'
 SET "TimeZone" TO 'UTC'
AS $function$
DECLARE v_r population_top3000_202608_202609_v1.ref01_adjudication_run_v3%rowtype;v_v jsonb;v_vs text;
 v_review text;v_input jsonb;v_payload jsonb;v_tid uuid;v_old record;
BEGIN
 -- The current controller locks both control rows; tick/claim uses those same locks.
 -- Only immutable reviewed inputs are staged here; worker retains exact parent CAS.
 PERFORM population_top3000_202608_202609_v1.ref01_adjudication_controller_v3(false);
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE family_id='REF01' AND status='LEASED')
 OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_provider_lease l
 JOIN population_top3000_202608_202609_v1.pipeline_attempt a USING(attempt_id)
 JOIN population_top3000_202608_202609_v1.pipeline_task t USING(task_id)
 WHERE t.family_id='REF01' AND l.released_at IS NULL)
 THEN RAISE EXCEPTION 'REF01_V4_LANE_MUST_BE_DRAINED';END IF;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton AND enabled)
 THEN RAISE EXCEPTION 'REF01_V4_PIPELINE_DISABLED';END IF;
 SELECT * INTO STRICT v_r FROM population_top3000_202608_202609_v1.ref01_adjudication_run_v3 r WHERE r.run_key=p_run;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.ref01_adjudication_head_v3 h WHERE h.run_key=p_run AND h.case_key=p_case AND h.record_sha256=p_expected_parent)
 THEN RAISE EXCEPTION 'REF01_V3_REVIEW_PARENT_MUST_BE_CURRENT_HEAD';END IF;
 SELECT to_jsonb(v) INTO STRICT v_v FROM population_top3000_202608_202609_v1.company_verification_v1 v WHERE v.verification_id=p_verification;
 v_vs:=population_top3000_202608_202609_v1.ref01_adjudication_hash_v3(v_v);
 v_review:=population_top3000_202608_202609_v1.ref01_adjudication_hash_v3(jsonb_build_object('run_key',p_run,'case_key',p_case,
  'expected_parent_sha256',p_expected_parent,'verification_id',p_verification,'proposal',p_proposal,'source_refs',p_source_refs,'verification_sha256',v_vs));
 INSERT INTO population_top3000_202608_202609_v1.ref01_adjudication_review_v3(review_sha256,run_key,case_key,expected_parent_sha256,verification_id,proposal,source_refs,verification_sha256)
 VALUES(v_review,p_run,p_case,p_expected_parent,p_verification,p_proposal,p_source_refs,v_vs) ON CONFLICT DO NOTHING;
 PERFORM population_top3000_202608_202609_v1.ref01_adjudication_review_check_v3(v_review);
 v_input:=v_r.input_versions||jsonb_build_object('review_sha256',v_review,'expected_parent_sha256',p_expected_parent);
 v_payload:=jsonb_build_object('kind','REF01_CASE_REVIEW_V3','run_key',p_run,'case_key',p_case,'review_sha256',v_review);
 v_tid:=population_top3000_202608_202609_v1.pipeline_enqueue_v1('REF01','DERIVE',p_run||':review:'||v_review,v_payload,v_input);
 SELECT * INTO v_old FROM population_top3000_202608_202609_v1.pipeline_task t WHERE t.task_id=v_tid;
 IF v_old.payload IS DISTINCT FROM v_payload OR v_old.input_versions IS DISTINCT FROM v_input THEN RAISE EXCEPTION 'REF01_V3_REVIEW_TASK_REPLAY_CONFLICT';END IF;
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(task_id,event,details)
 VALUES(v_tid,'REF01_CASE_REVIEW_V3_ADMITTED',jsonb_build_object('run_key',p_run,'case_key',p_case,'review_sha256',v_review,'verification_id',p_verification,
  'expected_parent_sha256',p_expected_parent,'no_provider_request',true,'automatic_validate_successor',true,'cohort_not_yet_frozen',true));
 RETURN jsonb_build_object('task_id',v_tid,'review_sha256',v_review,'no_provider_request',true,'explicit_source_review_required_and_checked',true);
END $function$;
REVOKE ALL ON FUNCTION population_top3000_202608_202609_v1.ref01_review_admit_case_v4(text,text,text,jsonb,jsonb,uuid) FROM PUBLIC,anon,authenticated;
