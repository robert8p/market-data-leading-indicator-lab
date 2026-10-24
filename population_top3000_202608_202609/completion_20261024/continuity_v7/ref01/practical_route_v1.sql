CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.ref01_adjudication_route_v3(p_attempt uuid, p_token text, p_stage text)
 RETURNS jsonb
 LANGUAGE plpgsql
 SET search_path TO 'pg_catalog'
 SET "TimeZone" TO 'UTC'
AS $function$
DECLARE v_tid uuid;v_kind text;v_l population_top3000_202608_202609_v1.ref01_adjudication_legacy_v3%rowtype;v_fn text;v_result jsonb;
BEGIN
 v_tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);
 SELECT t.payload->>'kind' INTO v_kind FROM population_top3000_202608_202609_v1.pipeline_task t WHERE t.task_id=v_tid AND t.stage=p_stage;
 IF v_kind IN('PRACTICAL_COHORT_CANDIDATE_V1','PRACTICAL_POPULATION_CONSUME_V1','PRACTICAL_ACCEPTED_POPULATION_V1') THEN RETURN population_top3000_202608_202609_v1.ref01_practical_worker_v1(p_attempt,p_token);END IF;
 IF v_kind IN('REF01_ADJUDICATE_V3','REF01_CASE_REVIEW_V3','REF01_COHORT_CERTIFY_V3') THEN
  IF p_stage='DERIVE' AND v_kind='REF01_ADJUDICATE_V3' THEN RETURN population_top3000_202608_202609_v1.ref01_adjudication_bulk_v3(p_attempt,p_token);
  ELSIF p_stage='DERIVE' AND v_kind='REF01_CASE_REVIEW_V3' THEN RETURN population_top3000_202608_202609_v1.ref01_adjudication_case_worker_v3(p_attempt,p_token);
  ELSIF p_stage='VALIDATE' THEN RETURN population_top3000_202608_202609_v1.ref01_adjudication_validate_v3(p_attempt,p_token);
  ELSIF p_stage='PUBLISH' AND v_kind='REF01_COHORT_CERTIFY_V3' THEN RETURN population_top3000_202608_202609_v1.ref01_cohort_publish_v3(p_attempt,p_token);
  ELSIF p_stage='DELIVER' AND v_kind='REF01_COHORT_CERTIFY_V3' THEN RETURN population_top3000_202608_202609_v1.ref01_cohort_deliver_v3(p_attempt,p_token);
  ELSE RAISE EXCEPTION 'REF01_V3_UNEXPECTED_KIND_STAGE';END IF;
 END IF;
 SELECT * INTO v_l FROM population_top3000_202608_202609_v1.ref01_adjudication_legacy_v3 l WHERE l.stage=p_stage;
 IF v_l.function_oid IS NULL THEN RETURN jsonb_build_object('state','BLOCKED','blocker_code','REF01_LEGACY_STAGE_WAS_NOT_REGISTERED',
  'resumption_condition',jsonb_build_object('stage',p_stage,'event','EXPLICIT_REVIEWED_LEGACY_STAGE_BINDING'));END IF;
 IF v_l.function_sha256 IS DISTINCT FROM encode(sha256(convert_to(pg_get_functiondef(v_l.function_oid),'UTF8')),'hex')
 THEN RAISE EXCEPTION 'REF01_V3_CAPTURED_LEGACY_DEFINITION_CHANGED';END IF;
 SELECT format('%I.%I',n.nspname,p.proname) INTO v_fn FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE p.oid=v_l.function_oid;
 EXECUTE format('SELECT %s($1,$2)',v_fn) INTO v_result USING p_attempt,p_token;
 RETURN v_result;
END $function$;
