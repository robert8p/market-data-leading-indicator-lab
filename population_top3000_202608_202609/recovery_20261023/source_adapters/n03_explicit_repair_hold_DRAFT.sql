-- New wrapper only. Existing generic diagnostics are preserved; no false repair or publication cascade.
CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.n03_repair_diagnose_and_hold_v1(p_attempt uuid,p_token text)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path TO 'pg_catalog' SET lock_timeout TO '500ms'
AS $function$
DECLARE v_task_id uuid;v_task population_top3000_202608_202609_v1.pipeline_task%rowtype;
 v_validation population_top3000_202608_202609_v1.pipeline_validation%rowtype;
 v_diagnosis jsonb;v_fresh boolean;v_exceptions jsonb;
BEGIN
 v_task_id:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);
 SELECT * INTO v_task FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=v_task_id;
 IF v_task.family_id IS DISTINCT FROM 'N03' OR v_task.stage IS DISTINCT FROM 'REPAIR'
  OR v_task.partition_key IS DISTINCT FROM 'splits-execution-202608-202609'
  OR v_task.input_versions->>'scope_sha256' IS DISTINCT FROM '8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd'
 THEN RAISE EXCEPTION 'N03_REPAIR_HOLD_SCOPE_MISMATCH';END IF;
 IF encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.pipeline_repair_v1(uuid,text)'::regprocedure),'UTF8')),'hex')
  IS DISTINCT FROM 'b8663b7bde533cfb33c50b092bb0200f16bb83965b3f11115b12ef6e61a9382c'
 THEN RETURN jsonb_build_object('state','BLOCKED','blocker_code','N03_DIAGNOSTIC_HELPER_REVISION_REVIEW_REQUIRED',
  'resumption_condition',jsonb_build_object('required_action','Review and test the changed generic repair definition before rebinding this diagnostic-only wrapper.','automatic_retry',false));END IF;
 SELECT q.* INTO v_validation FROM population_top3000_202608_202609_v1.pipeline_partition p
 JOIN population_top3000_202608_202609_v1.pipeline_validation q ON q.validation_id=p.last_validation_id
 WHERE p.family_id='N03' AND p.partition_key=v_task.partition_key;
 IF NOT FOUND OR v_validation.input_versions IS DISTINCT FROM v_task.input_versions THEN
  RETURN jsonb_build_object('state','BLOCKED','blocker_code','N03_CURRENT_INPUT_VALIDATION_REQUIRED',
   'resumption_condition',jsonb_build_object('required_action','Persist validation bound to the exact current N03 input revision.','automatic_retry',false));END IF;
 IF v_validation.result='PASS' THEN RETURN jsonb_build_object('state','BLOCKED','blocker_code','N03_VALIDATION_PASS_ROUTING_REVIEW',
  'resumption_condition',jsonb_build_object('validation_id',v_validation.validation_id,'required_action','The diagnostic-only adapter does not publish; review the explicit validated publication route.','automatic_retry',false));END IF;
 v_diagnosis:=population_top3000_202608_202609_v1.pipeline_repair_v1(p_attempt,p_token);
 IF v_diagnosis->>'state'='BLOCKED' THEN RETURN v_diagnosis;END IF;
 IF v_diagnosis->>'state' IS DISTINCT FROM 'COMPLETE' OR v_diagnosis->>'repair_applied' IS DISTINCT FROM 'false'
  OR v_diagnosis->>'repair_outcome' IS DISTINCT FROM 'NO_SAFE_AUTOMATIC_REPAIR'
 THEN RAISE EXCEPTION 'N03_DIAGNOSTIC_HELPER_BEHAVIOR_CHANGED';END IF;
 SELECT coalesce(jsonb_agg(jsonb_build_object('code',code,'affected_units',affected_units,'resumption_condition',resumption_condition) ORDER BY code),'[]'::jsonb)
 INTO v_exceptions FROM population_top3000_202608_202609_v1.pipeline_exception WHERE validation_id=v_validation.validation_id AND resolved_at IS NULL;
 IF jsonb_array_length(v_exceptions)=0 THEN RAISE EXCEPTION 'N03_NONPASS_VALIDATION_WITHOUT_DIAGNOSED_EXCEPTION';END IF;
 v_fresh:=population_top3000_202608_202609_v1.pipeline_record_commit_v1(p_attempt,p_token,v_diagnosis->'commit_receipt');
 IF v_fresh THEN INSERT INTO population_top3000_202608_202609_v1.pipeline_event(task_id,attempt_id,event,details)
  VALUES(v_task_id,p_attempt,'N03_EXPLICIT_REPAIR_EVIDENCE_HOLD_RECORDED',jsonb_build_object('validation_id',v_validation.validation_id,
   'quality_result',v_validation.result,'repair_applied',false,'publication_admitted',false,'unresolved_exceptions',v_exceptions));END IF;
 RETURN jsonb_build_object('state','BLOCKED','blocker_code','N03_COMPANY_AND_SOURCE_QUALITY_EVIDENCE_REQUIRED',
  'resumption_condition',jsonb_build_object('validation_id',v_validation.validation_id,'quality_result',v_validation.result,
   'unresolved_exceptions',v_exceptions,'required_action','Supply the specifically diagnosed company-denominator, identity, temporal and independent source-quality evidence before an explicitly reviewed repair or publication stage.',
   'automatic_retry',false,'repair_applied',false,'new_provider_request_admitted',false,'publication_admitted',false));
END $function$;
REVOKE ALL ON FUNCTION population_top3000_202608_202609_v1.n03_repair_diagnose_and_hold_v1(uuid,text) FROM PUBLIC,anon,authenticated;
GRANT EXECUTE ON FUNCTION population_top3000_202608_202609_v1.n03_repair_diagnose_and_hold_v1(uuid,text) TO postgres,service_role;
