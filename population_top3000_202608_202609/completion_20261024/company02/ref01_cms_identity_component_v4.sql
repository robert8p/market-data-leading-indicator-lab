-- Narrow CMS identity correction; no v3 function changes or cohort acceptance.
CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.ref01_cms_identity_check_v4(p_candidate jsonb,p_verification uuid)
RETURNS text LANGUAGE plpgsql SET search_path TO 'pg_catalog' SET "TimeZone" TO 'UTC' AS $function$
DECLARE v_v population_top3000_202608_202609_v1.company_verification_v1%rowtype;
 v_c population_top3000_202608_202609_v1.ref01_adjudication_case_v3%rowtype;
BEGIN
 PERFORM population_top3000_202608_202609_v1.ref01_adjudication_controller_v3(false);
 SELECT * INTO STRICT v_v FROM population_top3000_202608_202609_v1.company_verification_v1 WHERE verification_id=p_verification;
 SELECT * INTO STRICT v_c FROM population_top3000_202608_202609_v1.ref01_adjudication_case_v3
 WHERE run_key=p_candidate->>'run_key' AND case_key=p_candidate->>'case_key' AND record_sha256=p_candidate->>'expected_parent_sha256';
 IF octet_length(p_candidate::text)>100000 OR p_candidate->>'run_key' IS DISTINCT FROM 'ref01-company-adjudication-v3-20261023'
 OR p_candidate->>'case_key' IS DISTINCT FROM 'CIK:0000201533'
 OR p_candidate->>'expected_parent_sha256' IS DISTINCT FROM 'c4d69109beeb69d51d29a51e9bb434ecc353ae119a614d8d32cc9c6ade6518ca'
 OR p_candidate->>'producer_id' IS DISTINCT FROM 'top3000-completion-20261024-company02'
 OR p_candidate#>>'{proposal,decision}' IS DISTINCT FROM 'REVIEW_REQUIRED'
 OR p_candidate#>>'{proposal,company_id}' IS DISTINCT FROM 'CIK:0000811156'
 OR p_candidate#>'{proposal,original_historical_receipt_inferred}' IS DISTINCT FROM 'false'::jsonb
 OR p_candidate->'reviewed_member_tickers' IS DISTINCT FROM '["CMS"]'::jsonb
 OR v_c.content->'member_tickers' IS DISTINCT FROM '["CMS"]'::jsonb
 OR p_candidate#>'{proposal,unresolved_codes}' IS DISTINCT FROM '["RANK_DATE_ACTUAL_COMMON_SHARES_BRIDGE","OFFICIAL_REGULAR_SESSION_CLOSE_QUALIFICATION"]'::jsonb
 OR p_candidate#>>'{proposal,decision_evidence,component_scope}' IS DISTINCT FROM 'CANONICAL_ISSUER_AND_COMMON_SECURITY_IDENTITY_ONLY'
 OR p_candidate#>'{proposal,decision_evidence,actual_aug3_common_shares_proven}' IS DISTINCT FROM 'false'::jsonb
 OR p_candidate#>'{proposal,decision_evidence,official_aug3_close_proven}' IS DISTINCT FROM 'false'::jsonb
 OR nullif(p_candidate#>>'{proposal,decision_evidence,reason}','') IS NULL
 OR p_candidate#>>'{source_refs,0,kind}' IS DISTINCT FROM 'SOURCE_DOCUMENT'
 OR p_candidate#>>'{source_refs,0,sha256}' IS DISTINCT FROM 'ccbda9aa53c71d9b614140c46a66ae8765d96f0341de069700c37d631c3ab225'
 THEN RAISE EXCEPTION 'CMS_IDENTITY_EXACT_COMPONENT_SCOPE_REQUIRED';END IF;
 IF v_v.result->>'kind' IS DISTINCT FROM 'REF01_COMPONENT_IDENTITY_REVIEW_V4' OR v_v.result->>'result' IS DISTINCT FROM 'PASS'
 OR NOT population_top3000_202608_202609_v1.ref01_review_authorized_v4(v_v.result)
 OR v_v.result->>'produced_by' IS DISTINCT FROM p_candidate->>'producer_id'
 OR v_v.result->>'candidate_sha256' IS DISTINCT FROM population_top3000_202608_202609_v1.ref01_adjudication_hash_v3(p_candidate)
 OR v_v.result->>'run_key' IS DISTINCT FROM p_candidate->>'run_key'
 OR v_v.result->>'case_key' IS DISTINCT FROM p_candidate->>'case_key'
 OR v_v.result->>'expected_parent_sha256' IS DISTINCT FROM p_candidate->>'expected_parent_sha256'
 OR v_v.result->'reviewed_member_tickers' IS DISTINCT FROM '["CMS"]'::jsonb
 OR v_v.result->'canonical_issuer_and_common_security_identity_supported' IS DISTINCT FROM 'true'::jsonb
 OR v_v.result->'contrary_source_facts_reconciled' IS DISTINCT FROM 'true'::jsonb
 OR v_v.result->'valuation_accepted' IS DISTINCT FROM 'false'::jsonb
 OR v_v.result->'cohort_accepted' IS DISTINCT FROM 'false'::jsonb
 OR v_v.result->'remaining_requirement_codes' IS DISTINCT FROM p_candidate#>'{proposal,unresolved_codes}'
 THEN RAISE EXCEPTION 'CMS_IDENTITY_EXACT_INDEPENDENT_COMPONENT_REVIEW_REQUIRED';END IF;
 PERFORM population_top3000_202608_202609_v1.ref01_adjudication_sources_v3(p_candidate->'source_refs');
 RETURN population_top3000_202608_202609_v1.ref01_adjudication_hash_v3(to_jsonb(v_v));
END;
$function$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.ref01_cms_identity_admit_v4(p_candidate jsonb,p_verification uuid)
RETURNS jsonb LANGUAGE plpgsql SET search_path TO 'pg_catalog' SET "TimeZone" TO 'UTC' AS $function$
DECLARE v_r population_top3000_202608_202609_v1.ref01_adjudication_run_v3%rowtype;
 v_vs text;v_sha text;v_payload jsonb;v_input jsonb;v_part text;v_tid uuid;v_bundle text;
BEGIN
 PERFORM population_top3000_202608_202609_v1.ref01_adjudication_controller_v3(false);
 SELECT encode(sha256(convert_to(string_agg(p.proname||':'||encode(sha256(convert_to(pg_get_functiondef(p.oid),'UTF8')),'hex'),E'\n' ORDER BY p.proname),'UTF8')),'hex') INTO v_bundle
 FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='population_top3000_202608_202609_v1'
 AND p.proname IN('ref01_cms_identity_check_v4','ref01_cms_identity_admit_v4','ref01_cms_identity_worker_v4');
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_adapter a WHERE a.family_id='REF01' AND a.stage='REPAIR' AND a.enabled
 AND a.function_oid='population_top3000_202608_202609_v1.ref01_cms_identity_worker_v4(uuid,text)'::regprocedure AND a.resource_class='DATABASE'
 AND a.function_sha256=encode(sha256(convert_to(pg_get_functiondef(a.function_oid),'UTF8')),'hex')
 AND a.reviewed_test_evidence->>'cms_component_bundle_sha256'=v_bundle) THEN RAISE EXCEPTION 'CMS_IDENTITY_HELPER_BUNDLE_CHANGED';END IF;
 v_vs:=population_top3000_202608_202609_v1.ref01_cms_identity_check_v4(p_candidate,p_verification);
 SELECT * INTO STRICT v_r FROM population_top3000_202608_202609_v1.ref01_adjudication_run_v3 WHERE run_key=p_candidate->>'run_key';
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE family_id='REF01' AND status='LEASED')
 THEN RAISE EXCEPTION 'CMS_IDENTITY_REF01_LANE_DRAIN_REQUIRED';END IF;
 v_sha:=population_top3000_202608_202609_v1.ref01_adjudication_hash_v3(p_candidate);
 v_payload:=jsonb_build_object('kind','REF01_CASE_REVIEW_V3','component_kind','CMS_IDENTITY_ONLY_V4','run_key',v_r.run_key,'candidate',p_candidate,'verification_id',p_verification);
 v_input:=v_r.input_versions||jsonb_build_object('component_candidate_sha256',v_sha,'component_verification_sha256',v_vs,'cms_component_bundle_sha256',v_bundle);
 v_part:=v_r.run_key||':cms-identity-v4:'||v_sha;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.ref01_adjudication_head_v3 WHERE run_key=v_r.run_key AND case_key=p_candidate->>'case_key'
 AND record_sha256=p_candidate->>'expected_parent_sha256') AND NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task
 WHERE family_id='REF01' AND stage='REPAIR' AND partition_key=v_part AND payload=v_payload AND input_versions=v_input)
 THEN RAISE EXCEPTION 'CMS_IDENTITY_CURRENT_PARENT_REQUIRED';END IF;
 v_tid:=population_top3000_202608_202609_v1.pipeline_enqueue_v1('REF01','REPAIR',v_part,v_payload,v_input);
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=v_tid AND payload=v_payload AND input_versions=v_input AND repair_round=0)
 THEN RAISE EXCEPTION 'CMS_IDENTITY_TASK_REPLAY_CONFLICT';END IF;
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(task_id,event,details) VALUES(v_tid,'CMS_IDENTITY_COMPONENT_ADMITTED_V4',jsonb_build_object('candidate_sha256',v_sha,'verification_id',p_verification,'valuation_accepted',false,'automatic_validate_successor',true));
 RETURN jsonb_build_object('task_id',v_tid,'candidate_sha256',v_sha,'valuation_accepted',false);
END;
$function$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.ref01_cms_identity_worker_v4(p_attempt uuid,p_token text)
RETURNS jsonb LANGUAGE plpgsql SET search_path TO 'pg_catalog' SET "TimeZone" TO 'UTC' AS $function$
DECLARE v_tid uuid;v_t population_top3000_202608_202609_v1.pipeline_task%rowtype;
 v_r population_top3000_202608_202609_v1.ref01_adjudication_run_v3%rowtype;
 v_parent population_top3000_202608_202609_v1.ref01_adjudication_case_v3%rowtype;v_head record;
 v_q jsonb;v_id uuid;v_vs text;v_sha text;v_rev text;v_content jsonb;v_hash text;v_n integer;v_code text;v_key text;v_evidence jsonb;v_bundle text;
BEGIN
 v_tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);
 SELECT * INTO STRICT v_t FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=v_tid;
 v_r:=population_top3000_202608_202609_v1.ref01_adjudication_context_v3(v_tid,'REPAIR');
 SELECT encode(sha256(convert_to(string_agg(p.proname||':'||encode(sha256(convert_to(pg_get_functiondef(p.oid),'UTF8')),'hex'),E'\n' ORDER BY p.proname),'UTF8')),'hex') INTO v_bundle
 FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='population_top3000_202608_202609_v1'
 AND p.proname IN('ref01_cms_identity_check_v4','ref01_cms_identity_admit_v4','ref01_cms_identity_worker_v4');
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_adapter a WHERE a.family_id='REF01' AND a.stage='REPAIR' AND a.enabled
 AND a.function_oid='population_top3000_202608_202609_v1.ref01_cms_identity_worker_v4(uuid,text)'::regprocedure AND a.resource_class='DATABASE'
 AND a.function_sha256=encode(sha256(convert_to(pg_get_functiondef(a.function_oid),'UTF8')),'hex')
 AND a.reviewed_test_evidence->>'cms_component_bundle_sha256'=v_bundle) THEN RAISE EXCEPTION 'CMS_IDENTITY_HELPER_BUNDLE_CHANGED';END IF;
 IF v_t.input_versions->>'cms_component_bundle_sha256' IS DISTINCT FROM v_bundle THEN RAISE EXCEPTION 'CMS_IDENTITY_TASK_HELPER_BUNDLE_CHANGED';END IF;
 v_q:=v_t.payload->'candidate';v_id:=(v_t.payload->>'verification_id')::uuid;
 v_vs:=population_top3000_202608_202609_v1.ref01_cms_identity_check_v4(v_q,v_id);
 v_sha:=population_top3000_202608_202609_v1.ref01_adjudication_hash_v3(v_q);
 IF v_t.payload IS DISTINCT FROM jsonb_build_object('kind','REF01_CASE_REVIEW_V3','component_kind','CMS_IDENTITY_ONLY_V4','run_key',v_r.run_key,'candidate',v_q,'verification_id',v_id)
 OR v_t.input_versions IS DISTINCT FROM v_r.input_versions||jsonb_build_object('component_candidate_sha256',v_sha,'component_verification_sha256',v_vs,'cms_component_bundle_sha256',v_bundle)
 OR v_t.partition_key IS DISTINCT FROM v_r.run_key||':cms-identity-v4:'||v_sha OR v_t.checkpoint IS DISTINCT FROM '{}'::jsonb OR v_t.repair_round<>0
 THEN RAISE EXCEPTION 'CMS_IDENTITY_EXACT_TASK_REQUIRED';END IF;
 SELECT * INTO STRICT v_parent FROM population_top3000_202608_202609_v1.ref01_adjudication_case_v3 WHERE run_key=v_r.run_key AND case_key=v_q->>'case_key' AND record_sha256=v_q->>'expected_parent_sha256';
 SELECT * INTO STRICT v_head FROM population_top3000_202608_202609_v1.ref01_adjudication_head_v3 WHERE run_key=v_r.run_key AND case_key=v_q->>'case_key' FOR UPDATE;
 v_rev:=population_top3000_202608_202609_v1.ref01_adjudication_hash_v3(jsonb_build_object('candidate_sha256',v_sha,'verification_id',v_id,'verification_sha256',v_vs));
 IF v_head.record_sha256 IS DISTINCT FROM v_parent.record_sha256 AND v_head.revision_key IS DISTINCT FROM v_rev THEN RAISE EXCEPTION 'CMS_IDENTITY_STALE_PARENT_MUST_NOT_REWIND';END IF;
 v_content:=jsonb_build_object('run_key',v_r.run_key,'case_key',v_q->>'case_key','revision_key',v_rev,'case_ordinal',v_parent.case_ordinal,
 'source_decision_sha256',v_parent.source_decision_sha256,'parent_sha256',v_parent.record_sha256,'decision','REVIEW_REQUIRED','company_id','CIK:0000811156',
 'cap_usd',null,'cap_upper_bound_usd',null,'resolution_id',null,'retained_prior_resolution_id_unaccepted',v_parent.resolution_id,'retained_prior_cap_usd_unaccepted',v_parent.cap_usd,
 'member_tickers',v_parent.content->'member_tickers','unresolved_codes',v_q#>'{proposal,unresolved_codes}','risk_codes',v_parent.content->'risk_codes',
 'source_qualified_dated_vendor_route',false,'independent_case_review_verified',false,'independent_identity_component_verified',true,'verification_id',v_id,'verification_sha256',v_vs,
 'source_refs',v_q->'source_refs','decision_evidence',v_q#>'{proposal,decision_evidence}','full_population_or_cohort_certified',false,'original_historical_receipt_inferred',false);
 v_hash:=population_top3000_202608_202609_v1.ref01_adjudication_hash_v3(v_content);
 INSERT INTO population_top3000_202608_202609_v1.ref01_adjudication_case_v3(run_key,case_key,revision_key,case_ordinal,parent_sha256,source_decision_sha256,decision,company_id,cap_usd,cap_upper_bound_usd,resolution_id,content,record_sha256,first_attempt_id)
 VALUES(v_r.run_key,v_parent.case_key,v_rev,v_parent.case_ordinal,v_parent.record_sha256,v_parent.source_decision_sha256,'REVIEW_REQUIRED','CIK:0000811156',null,null,null,v_content,v_hash,p_attempt) ON CONFLICT DO NOTHING;
 GET DIAGNOSTICS v_n=ROW_COUNT;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.ref01_adjudication_case_v3 WHERE run_key=v_r.run_key AND case_key=v_parent.case_key AND revision_key=v_rev AND content=v_content AND record_sha256=v_hash)
 THEN RAISE EXCEPTION 'CMS_IDENTITY_IMMUTABLE_CASE_CONFLICT';END IF;
 UPDATE population_top3000_202608_202609_v1.ref01_adjudication_head_v3 SET revision_key=v_rev,record_sha256=v_hash WHERE run_key=v_r.run_key AND case_key=v_parent.case_key AND record_sha256 IN(v_parent.record_sha256,v_hash);
 IF NOT FOUND THEN RAISE EXCEPTION 'CMS_IDENTITY_HEAD_CAS_FAILED';END IF;
 v_evidence:=jsonb_build_object('run_key',v_r.run_key,'case_key',v_parent.case_key,'canonical_company_id','CIK:0000811156','case_sha256',v_hash,'candidate_sha256',v_sha,'verification_id',v_id,'retained_prior_resolution_id',v_parent.resolution_id,'valuation_accepted',false);
 FOR v_code IN SELECT jsonb_array_elements_text(v_q#>'{proposal,unresolved_codes}') LOOP
  v_key:='ref01-v3:'||v_r.run_key||':cms-identity-v4:'||v_rev||':'||v_code;
  INSERT INTO population_top3000_202608_202609_v1.company_exception_v1(exception_key,group_key,ticker,code,priority,status,evidence,resumption_condition)
  VALUES(v_key,v_parent.case_key,'CMS',v_code,100,'OPEN',v_evidence,jsonb_build_object('required_action','Acquire and independently review the exact named remaining rank-date component, then submit reviewed full case. Identity correction alone does not accept valuation.','required_component',v_code,'retained_source_cap_not_zero',true)) ON CONFLICT DO NOTHING;
  INSERT INTO population_top3000_202608_202609_v1.company_source_request_v1(request_key,group_key,kind,source_id,payload,priority,state)
  VALUES(v_key,v_parent.case_key,'REF01_TARGETED_EVIDENCE_V3','case-sha256:'||v_hash,v_evidence||jsonb_build_object('required_component',v_code,'required_action','Target exact Aug3 common shares or official-close evidence; retained July13 SEC shares and vendor source are anchors, not proof of unchanged state.'),100,'PENDING_ADMISSION') ON CONFLICT DO NOTHING;
 END LOOP;
 PERFORM population_top3000_202608_202609_v1.ref01_adjudication_controller_v3(false);
 RETURN jsonb_build_object('state','COMPLETE','commit_receipt',jsonb_build_object('commit_key','cms-identity-component-v4:'||v_rev,'output_sha256',v_hash,'new_committed_units',v_n,
 'immutable_output_ids',jsonb_build_array(v_r.run_key||':'||v_parent.case_key||':'||v_rev),'input_versions',v_t.input_versions,'identity_component_committed',true,'valuation_accepted',false,'automatic_validate_successor',true,'no_provider_request',true));
END;
$function$;
REVOKE ALL ON FUNCTION population_top3000_202608_202609_v1.ref01_cms_identity_check_v4(jsonb,uuid) FROM PUBLIC,anon,authenticated;
REVOKE ALL ON FUNCTION population_top3000_202608_202609_v1.ref01_cms_identity_admit_v4(jsonb,uuid) FROM PUBLIC,anon,authenticated;
REVOKE ALL ON FUNCTION population_top3000_202608_202609_v1.ref01_cms_identity_worker_v4(uuid,text) FROM PUBLIC,anon,authenticated;
