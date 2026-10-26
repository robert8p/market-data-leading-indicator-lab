-- Candidate only. Does not install routes, accept families or promote serving.
CREATE FUNCTION population_top3000_202608_202609_v1.practical_release_basis_evaluate_v2(m jsonb,r jsonb,rv jsonb,native_ok boolean)
RETURNS jsonb LANGUAGE plpgsql STABLE SET search_path TO pg_catalog AS $basis$
DECLARE errors text[]:='{}';membership_ok boolean;instrument_ok boolean:=false;shape jsonb;c jsonb;release_hash text;
BEGIN
membership_ok:=coalesce(m->>'view_sha256'='f13272d86a26c1f9034e5fd4c6423484d589b68c04a7cd426416b1007641a6f3'
 AND m->>'company_count'='3000' AND m->>'distinct_company_count'='3000' AND m->>'null_company_count'='0'
 AND m->>'cohort_sha256'='2dc0f766befed6de6b052221c3c87482d507ba1bad931cbb98654eb3af899bec'
 AND m->>'policy_sha256'='3e49d661fcdb8533e5ce88a85689b41e9dde71eeae2dc8578c9e31f9ae1738bb'
 AND m->>'membership_acceptance_sha256'='20b9dac759857e9b510ccaeca33e4a94d8c748d588fb0cc27740cc66c389b5f9'
 AND m->>'distinct_bindings'='1',false);
IF NOT membership_ok THEN errors:=array_append(errors,'ACCEPTED_PRACTICAL_MEMBERSHIP_BINDING_INVALID');END IF;
IF r IS NULL THEN errors:=array_append(errors,'QUALIFIED_FULL_PERIOD_INSTRUMENT_RELEASE_MISSING');
ELSE
 c:=r->'content';release_hash:=encode(sha256(convert_to(c::text,'UTF8')),'hex');
 IF r->>'release_sha256' IS DISTINCT FROM release_hash OR r->>'cohort_sha256' IS DISTINCT FROM m->>'cohort_sha256' OR r->>'policy_sha256' IS DISTINCT FROM m->>'policy_sha256' OR r->>'acceptance_sha256' IS DISTINCT FROM m->>'membership_acceptance_sha256' THEN errors:=array_append(errors,'INSTRUMENT_RELEASE_BINDING_INVALID');END IF;
 IF jsonb_typeof(c) IS DISTINCT FROM 'object' OR pg_column_size(c)>16777216 OR c->>'fixture_only' IS DISTINCT FROM 'false' OR c->>'full_period_verified' IS DISTINCT FROM 'true' OR c->>'window_start' IS DISTINCT FROM '2026-08-01' OR c->>'window_end' IS DISTINCT FROM '2026-09-30' OR c->>'evidence_semantics' IS DISTINCT FROM 'QUALIFIED_DATED_INSTRUMENT_INTERVALS_WITH_EXPLICIT_LIFECYCLE_COVERAGE_V2' THEN errors:=array_append(errors,'FULL_PERIOD_CONTENT_CONTRACT_INVALID');END IF;
 IF rv IS NULL OR rv->>'approved' IS DISTINCT FROM 'true' OR rv->>'fixture_only' IS DISTINCT FROM 'false' OR rv->>'full_period_instrument_evidence_verified' IS DISTINCT FROM 'true' OR rv->>'instrument_manifest_sha256' IS DISTINCT FROM release_hash OR rv->>'owner_id' IS DISTINCT FROM 'top3000-completion-20261024-root-5db48b32d103' OR rv->>'owner_fence' IS DISTINCT FROM '3' OR rv->>'stop_generation' IS DISTINCT FROM '1' OR rv->>'scope_sha256' IS DISTINCT FROM '8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd' OR nullif(rv->>'producer','') IS NULL OR nullif(rv->>'reviewer','') IS NULL OR rv->>'producer' IS NOT DISTINCT FROM rv->>'reviewer' THEN errors:=array_append(errors,'ACTUAL_DISTINCT_INSTRUMENT_REVIEW_MISSING');END IF;
 IF native_ok IS DISTINCT FROM true THEN errors:=array_append(errors,'COMMITTED_REF01_SOURCE_PREREQUISITE_MISSING');END IF;
 IF cardinality(errors)=0 THEN
 BEGIN
 shape:=population_top3000_202608_202609_v1.ctx06_release_shape_v3(c,m->'companies');
 EXCEPTION WHEN OTHERS THEN errors:=array_append(errors,'INSTRUMENT_SHAPE_INVALID:'||SQLSTATE);shape:=jsonb_build_object('error',SQLERRM);END;
 IF shape->>'structural_checks_passed'='true' THEN
  IF EXISTS(SELECT 1 FROM jsonb_array_elements(m->'companies') x WHERE NOT EXISTS(SELECT 1 FROM jsonb_array_elements(c->'instruments') i WHERE i->>'company_id'=x->>'company_id' AND i->>'eligible_common_equity'='true' AND (i->>'listing_start')::timestamptz<='2026-08-03T20:00:00Z'::timestamptz AND (i->>'listing_end' IS NULL OR (i->>'listing_end')::timestamptz>'2026-08-03T20:00:00Z'::timestamptz))) THEN errors:=array_append(errors,'ACCEPTED_ANCHOR_LISTING_NOT_COVERED');END IF;
 END IF;
 END IF;
 instrument_ok:=cardinality(errors)=0;
END IF;
RETURN jsonb_build_object('revision','PRACTICAL_RELEASE_BASIS_V2','membership_pass',membership_ok,'company_count',m->'company_count','cohort_sha256',m->>'cohort_sha256','policy_sha256',m->>'policy_sha256','membership_acceptance_sha256',m->>'membership_acceptance_sha256','instrument_release_sha256',r->>'release_sha256','full_period_instruments_qualified',instrument_ok,'release_basis_pass',membership_ok AND instrument_ok,'blocking_conditions',to_jsonb(errors),'shape',shape,'family_accepted',false,'root_acceptance_created',false,'serving_promoted',false,'exact_global_rank_required',false,'original_historical_receipt_inferred',false);
END $basis$;
REVOKE ALL ON FUNCTION population_top3000_202608_202609_v1.practical_release_basis_evaluate_v2(jsonb,jsonb,jsonb,boolean) FROM PUBLIC,anon,authenticated;
CREATE FUNCTION population_top3000_202608_202609_v1.practical_release_basis_v2(p_instrument_release_sha256 text DEFAULT NULL)
RETURNS jsonb LANGUAGE plpgsql STABLE SET search_path TO pg_catalog AS $read$
DECLARE m jsonb;r jsonb;rv jsonb;n boolean:=false;
BEGIN
WITH a AS MATERIALIZED(SELECT * FROM population_top3000_202608_202609_v1.working_accepted_companies_v1)
SELECT jsonb_build_object('view_sha256',encode(sha256(convert_to(pg_get_viewdef('population_top3000_202608_202609_v1.working_accepted_companies_v1'::regclass,false),'UTF8')),'hex'),'company_count',count(*),'distinct_company_count',count(DISTINCT company_id),'null_company_count',count(*) FILTER(WHERE company_id IS NULL),'distinct_bindings',count(DISTINCT(cohort_sha256,policy_sha256,membership_acceptance_sha256)),'cohort_sha256',min(cohort_sha256),'policy_sha256',min(policy_sha256),'membership_acceptance_sha256',min(membership_acceptance_sha256),'companies',jsonb_agg(jsonb_build_object('company_id',company_id) ORDER BY company_id)) INTO m FROM a;
SELECT to_jsonb(x) INTO r FROM population_top3000_202608_202609_v1.ctx06_practical_release_v2 x WHERE release_sha256=p_instrument_release_sha256;
IF r IS NOT NULL THEN
 SELECT details INTO rv FROM population_top3000_202608_202609_v1.pipeline_event WHERE event_id=(r->>'independent_review_event_id')::bigint AND event='CTX06_DATED_RELEASE_INDEPENDENT_REVIEW_V2';
 SELECT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task t JOIN population_top3000_202608_202609_v1.pipeline_commit c USING(task_id) WHERE t.task_id::text=rv->>'source_prerequisite_task_id' AND t.family_id='REF01' AND t.status='COMPLETE' AND c.new_committed_units>0 AND c.output_sha256=rv->>'source_prerequisite_output_sha256' AND t.input_versions->>'cohort_sha256'=m->>'cohort_sha256' AND t.input_versions->>'policy_sha256'=m->>'policy_sha256' AND t.input_versions->>'membership_acceptance_sha256'=m->>'membership_acceptance_sha256') INTO n;
END IF;
RETURN population_top3000_202608_202609_v1.practical_release_basis_evaluate_v2(m,r,rv,n);
END $read$;
REVOKE ALL ON FUNCTION population_top3000_202608_202609_v1.practical_release_basis_v2(text) FROM PUBLIC,anon,authenticated;