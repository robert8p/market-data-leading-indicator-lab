-- Reviewer authorization belongs to the current fenced mission authority.
-- Root supplies authority_20261024.independent_reviewers and monetary_policy.
CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.ref01_review_authorized_v4(p_receipt jsonb)
RETURNS boolean LANGUAGE sql STABLE SET search_path TO 'pg_catalog' AS $function$
SELECT EXISTS (
 SELECT 1 FROM population_top3000_202608_202609_v1.acquisition_control a
 JOIN population_top3000_202608_202609_v1.pipeline_control c ON c.singleton=a.singleton
 JOIN population_top3000_202608_202609_v1.pipeline_event e ON
  e.event_id::text=c.budget_evidence#>>'{monetary_policy,authority_event_id}'
 WHERE a.singleton AND a.enabled AND NOT a.stopped AND a.hold_reason IS NULL
 AND c.expected_owner_id=a.owner_id AND c.expected_owner_fence=a.owner_fence
 AND c.expected_stop_generation=a.stop_generation AND c.scope_sha256=a.scope_sha256
 AND e.event='USER_AUTHORITY_RESUMED_20261024'
 AND e.details->>'owner_id'=a.owner_id
 AND e.details->>'owner_fence'=a.owner_fence::text
 AND e.details->>'stop_generation'=a.stop_generation::text
 AND e.details->>'scope_sha256'=a.scope_sha256
 AND p_receipt->>'scope_sha256'=a.scope_sha256
 AND p_receipt->>'authority_event_id'=e.event_id::text
 AND p_receipt->>'owner_id'=a.owner_id
 AND p_receipt->>'owner_fence'=a.owner_fence::text
 AND p_receipt->>'stop_generation'=a.stop_generation::text
 AND jsonb_typeof(c.budget_evidence#>'{authority_20261024,independent_reviewers}')='array'
 AND (c.budget_evidence#>'{authority_20261024,independent_reviewers}') ? (p_receipt->>'reviewed_by')
 AND nullif(p_receipt->>'produced_by','') IS NOT NULL
 AND nullif(p_receipt->>'reviewed_by','') IS NOT NULL
 AND p_receipt->>'produced_by'<>p_receipt->>'reviewed_by'
 AND p_receipt->'independent_of_producer'='true'::jsonb
);
$function$;
REVOKE ALL ON FUNCTION population_top3000_202608_202609_v1.ref01_review_authorized_v4(jsonb) FROM PUBLIC, anon, authenticated;


CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.ref01_adjudication_controller_v3(p_require_paused boolean DEFAULT false)
 RETURNS void
 LANGUAGE plpgsql
 SET search_path TO 'pg_catalog'
 SET "TimeZone" TO 'UTC'
AS $function$
DECLARE v_a population_top3000_202608_202609_v1.acquisition_control%rowtype;
 v_c population_top3000_202608_202609_v1.pipeline_control%rowtype;v_limit bigint;v_bytes bigint;
BEGIN
 SELECT * INTO v_a FROM population_top3000_202608_202609_v1.acquisition_control a WHERE a.singleton FOR UPDATE;
 SELECT * INTO v_c FROM population_top3000_202608_202609_v1.pipeline_control c WHERE c.singleton FOR UPDATE;
 IF nullif(v_a.owner_id,'') IS NULL
 OR v_c.expected_owner_id IS DISTINCT FROM v_a.owner_id
 OR v_a.owner_fence IS NULL OR v_c.expected_owner_fence IS DISTINCT FROM v_a.owner_fence
 OR v_a.stop_generation IS NULL OR v_c.expected_stop_generation IS DISTINCT FROM v_a.stop_generation
 OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_event e
  WHERE e.event_id::text=v_c.budget_evidence#>>'{monetary_policy,authority_event_id}'
  AND e.event='USER_AUTHORITY_RESUMED_20261024'
  AND e.details->>'owner_id'=v_a.owner_id
  AND e.details->>'owner_fence'=v_a.owner_fence::text
  AND e.details->>'stop_generation'=v_a.stop_generation::text
  AND e.details->>'scope_sha256'=v_a.scope_sha256)
 OR v_c.scope_sha256 IS DISTINCT FROM '8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd'
 OR v_a.scope_sha256 IS DISTINCT FROM v_c.scope_sha256 OR NOT v_a.enabled OR v_a.stopped OR v_a.hold_reason IS NOT NULL
 OR v_a.admitted_until<=clock_timestamp() OR NOT v_c.shared_scheduler_verified OR v_c.deadline<=clock_timestamp()
 THEN RAISE EXCEPTION 'REF01_V3_CURRENT_OWNER_FENCE_SCOPE_REQUIRED';END IF;
 IF p_require_paused AND (EXISTS(SELECT 1 FROM cron.job WHERE jobid=596 AND active)
 OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE status='LEASED')
 OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_provider_lease WHERE released_at IS NULL))
 THEN RAISE EXCEPTION 'REF01_V3_PAUSED_DRAINED_QUEUE_REQUIRED';END IF;
 v_limit:=(v_c.budget_evidence#>>'{resource_limits,max_relation_bytes}')::bigint;
 SELECT coalesce(sum(pg_total_relation_size(c.oid)),0)::bigint INTO v_bytes FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
 WHERE n.nspname='population_top3000_202608_202609_v1' AND c.relkind IN('r','m');
 IF v_limit IS NULL OR v_limit<=0 OR v_bytes>v_limit THEN RAISE EXCEPTION 'REF01_V3_CURRENT_RESOURCE_BOUND_EXCEEDED';END IF;
END $function$;


CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.ref01_adjudication_review_check_v3(p_review text)
 RETURNS population_top3000_202608_202609_v1.ref01_adjudication_review_v3
 LANGUAGE plpgsql
 SET search_path TO 'pg_catalog'
 SET "TimeZone" TO 'UTC'
AS $function$
DECLARE v_q population_top3000_202608_202609_v1.ref01_adjudication_review_v3%rowtype;
 v_v population_top3000_202608_202609_v1.company_verification_v1%rowtype;
 v_c population_top3000_202608_202609_v1.ref01_adjudication_case_v3%rowtype;v_needed jsonb;v_covered jsonb;v_source_ids jsonb;
BEGIN
 SELECT * INTO STRICT v_q FROM population_top3000_202608_202609_v1.ref01_adjudication_review_v3 q WHERE q.review_sha256=p_review;
 SELECT * INTO STRICT v_v FROM population_top3000_202608_202609_v1.company_verification_v1 v WHERE v.verification_id=v_q.verification_id;
 SELECT * INTO STRICT v_c FROM population_top3000_202608_202609_v1.ref01_adjudication_case_v3 c
 WHERE c.run_key=v_q.run_key AND c.case_key=v_q.case_key AND c.record_sha256=v_q.expected_parent_sha256;
 IF v_q.review_sha256 IS DISTINCT FROM population_top3000_202608_202609_v1.ref01_adjudication_hash_v3(jsonb_build_object(
 'run_key',v_q.run_key,'case_key',v_q.case_key,'expected_parent_sha256',v_q.expected_parent_sha256,
 'verification_id',v_q.verification_id,'proposal',v_q.proposal,'source_refs',v_q.source_refs,'verification_sha256',v_q.verification_sha256))
 OR v_q.verification_sha256 IS DISTINCT FROM population_top3000_202608_202609_v1.ref01_adjudication_hash_v3(to_jsonb(v_v))
 OR v_v.result->>'kind' IS DISTINCT FROM 'REF01_CASE_EVIDENCE_REVIEW_V3' OR v_v.result->>'result' IS DISTINCT FROM 'PASS'
 OR v_v.result->>'scope_sha256' IS DISTINCT FROM '8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd'
 OR v_v.result->>'run_key' IS DISTINCT FROM v_q.run_key OR v_v.result->>'case_key' IS DISTINCT FROM v_q.case_key
 OR v_v.result->>'expected_parent_sha256' IS DISTINCT FROM v_q.expected_parent_sha256
 OR v_v.result->>'proposal_sha256' IS DISTINCT FROM population_top3000_202608_202609_v1.ref01_adjudication_hash_v3(v_q.proposal)
 OR v_v.result->>'source_refs_sha256' IS DISTINCT FROM population_top3000_202608_202609_v1.ref01_adjudication_hash_v3(v_q.source_refs)
 OR v_v.result->'independent_of_producer' IS DISTINCT FROM 'true'::jsonb
 OR NOT population_top3000_202608_202609_v1.ref01_review_authorized_v4(v_v.result)
 OR v_v.verifier_version LIKE 'REF01_ADJUDICATION_V3%'
 OR v_v.result->'identity_and_security_kind_supported' IS DISTINCT FROM 'true'::jsonb
 OR v_v.result->'contrary_source_facts_reconciled' IS DISTINCT FROM 'true'::jsonb
 THEN RAISE EXCEPTION 'REF01_V3_EXACT_INDEPENDENT_CASE_REVIEW_REQUIRED';END IF;
 IF v_v.result->'synthetic_fixture_only'='true'::jsonb AND NOT EXISTS(
  SELECT 1 FROM population_top3000_202608_202609_v1.ref01_adjudication_run_v3 r WHERE r.run_key=v_q.run_key AND r.purpose='ROLLBACK_FIXTURE'
  AND current_setting('ref01.adjudication_fixture',true)=r.run_key)
 THEN RAISE EXCEPTION 'REF01_V3_SYNTHETIC_REVIEW_FORBIDDEN_OUTSIDE_ROLLBACK';END IF;
 SELECT coalesce(jsonb_agg(DISTINCT x ORDER BY x),'[]'::jsonb) INTO v_needed FROM jsonb_array_elements(coalesce(v_c.content->'unresolved_codes','[]'::jsonb)) x;
 IF jsonb_typeof(v_v.result->'covered_requirement_codes') IS DISTINCT FROM 'array' THEN RAISE EXCEPTION 'REF01_V3_COMPLETE_REQUIREMENT_COVERAGE_REQUIRED';END IF;
 SELECT coalesce(jsonb_agg(DISTINCT x ORDER BY x),'[]'::jsonb) INTO v_covered FROM jsonb_array_elements(v_v.result->'covered_requirement_codes') x;
 IF NOT(v_covered @> v_needed) THEN RAISE EXCEPTION 'REF01_V3_COMPLETE_REQUIREMENT_COVERAGE_REQUIRED';END IF;
 IF v_q.proposal->>'decision' NOT IN('ELIGIBLE','INELIGIBLE','OUTSIDE_BOUND') OR v_q.proposal->>'decision' IS NULL
 OR v_q.proposal->'original_historical_receipt_inferred' IS DISTINCT FROM 'false'::jsonb
 OR jsonb_typeof(v_q.proposal->'decision_evidence') IS DISTINCT FROM 'object'
 OR nullif(v_q.proposal#>>'{decision_evidence,reason}','') IS NULL
 THEN RAISE EXCEPTION 'REF01_V3_SUPPORTED_CASE_PROPOSAL_REQUIRED';END IF;
 IF v_v.result->'reviewed_member_tickers' IS DISTINCT FROM v_c.content->'member_tickers'
 OR v_v.result->'all_member_security_lines_reviewed' IS DISTINCT FROM 'true'::jsonb
 OR (v_q.proposal->>'decision' IN('ELIGIBLE','OUTSIDE_BOUND') AND v_v.result->'one_economic_company_for_all_members_supported' IS DISTINCT FROM 'true'::jsonb)
 OR (v_q.proposal->>'decision'='INELIGIBLE' AND v_v.result->'all_member_security_lines_ineligible' IS DISTINCT FROM 'true'::jsonb)
 THEN RAISE EXCEPTION 'REF01_V3_COMPLETE_MEMBER_AND_ECONOMIC_COMPANY_REVIEW_REQUIRED';END IF;
 IF v_q.proposal->>'decision' IN('ELIGIBLE','OUTSIDE_BOUND') AND
 (v_v.result->'valuation_supported' IS DISTINCT FROM 'true'::jsonb OR v_v.result->>'rank_date' IS DISTINCT FROM '2026-08-03'
 OR nullif(v_q.proposal->>'company_id','') IS NULL) THEN RAISE EXCEPTION 'REF01_V3_DATED_COMPANY_VALUATION_REVIEW_REQUIRED';END IF;
 IF v_q.proposal->>'decision'='ELIGIBLE' AND
 (jsonb_typeof(v_q.proposal->'resolution') IS DISTINCT FROM 'object' OR v_q.proposal#>>'{resolution,decision}' IS DISTINCT FROM 'ELIGIBLE'
 OR v_q.proposal#>>'{resolution,review_state}' IS DISTINCT FROM 'VERIFIED' OR v_q.proposal#>>'{resolution,company_id}' IS DISTINCT FROM v_q.proposal->>'company_id'
 OR v_q.proposal#>>'{resolution,group_key}' IS DISTINCT FROM v_q.case_key)
 THEN RAISE EXCEPTION 'REF01_V3_REVIEWED_RESOLUTION_CONTENT_REQUIRED';END IF;
 IF v_q.proposal->>'decision'='ELIGIBLE' THEN
  SELECT coalesce(jsonb_agg(x ORDER BY x),'[]'::jsonb) INTO v_source_ids FROM(
   SELECT DISTINCT CASE WHEN q->>'kind'='ACQUISITION_RAW' THEN to_jsonb('acquisition-job:'||(q->>'source_id')) ELSE q->'source_id' END x
   FROM jsonb_array_elements(v_q.source_refs) q)refs;
  IF jsonb_typeof(v_q.proposal#>'{resolution,source_ids}') IS DISTINCT FROM 'array'
  OR NOT(v_source_ids @> v_q.proposal#>'{resolution,source_ids}')
  THEN RAISE EXCEPTION 'REF01_V3_RESOLUTION_REFERENCES_MUST_HAVE_EXACT_RETAINED_PROOFS';END IF;
  IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.ref01_adjudication_run_v3 r WHERE r.run_key=v_q.run_key
   AND(v_q.proposal#>'{resolution,input_versions}') @> (r.input_versions||jsonb_build_object('source_decision_sha256',v_c.source_decision_sha256)))
  THEN RAISE EXCEPTION 'REF01_V3_REVIEWED_RESOLUTION_SCOPE_AND_SOURCE_INPUTS_REQUIRED';END IF;
 END IF;
 IF v_q.proposal->>'decision'='OUTSIDE_BOUND' AND
 (jsonb_typeof(v_q.proposal->'cap_upper_bound_usd') IS DISTINCT FROM 'number' OR (v_q.proposal->>'cap_upper_bound_usd')::numeric<=0
 OR v_q.proposal#>>'{decision_evidence,bound_semantics}' IS DISTINCT FROM 'SUPPORTED_GLOBAL_COMMON_EQUITY_UPPER_BOUND_AT_RANK_DATE'
 OR v_v.result->'finite_upper_bound_independently_supported' IS DISTINCT FROM 'true'::jsonb)
 THEN RAISE EXCEPTION 'REF01_V3_EVIDENCED_FINITE_COMPANY_UPPER_BOUND_REQUIRED';END IF;
 PERFORM population_top3000_202608_202609_v1.ref01_adjudication_sources_v3(v_q.source_refs);
 RETURN v_q;
END $function$;


CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.ref01_cohort_approval_v3(p_snapshot text, p_verification uuid)
 RETURNS jsonb
 LANGUAGE plpgsql
 SET search_path TO 'pg_catalog'
 SET "TimeZone" TO 'UTC'
AS $function$
DECLARE v_s population_top3000_202608_202609_v1.ref01_adjudication_snapshot_v3%rowtype;
 v_r population_top3000_202608_202609_v1.ref01_adjudication_run_v3%rowtype;
 v_v population_top3000_202608_202609_v1.company_verification_v1%rowtype;v_current jsonb;v_expected jsonb;v_given jsonb;
 v_item jsonb;v_map jsonb;v_receipt population_top3000_202608_202609_v1.company_verification_v1%rowtype;
 v_instruments integer;v_im text;v_rank_sha text;v_bad integer;
BEGIN
 SELECT * INTO STRICT v_s FROM population_top3000_202608_202609_v1.ref01_adjudication_snapshot_v3 s WHERE s.snapshot_sha256=p_snapshot;
 SELECT * INTO STRICT v_r FROM population_top3000_202608_202609_v1.ref01_adjudication_run_v3 r WHERE r.run_key=v_s.run_key;
 IF v_r.purpose IS DISTINCT FROM 'FULL_CENSUS' THEN RAISE EXCEPTION 'REF01_V3_FIXTURE_MUST_NEVER_FREEZE_COHORT';END IF;
 v_current:=population_top3000_202608_202609_v1.ref01_adjudication_snapshot_v3(v_s.run_key);
 IF v_current->>'snapshot_sha256' IS DISTINCT FROM p_snapshot THEN RAISE EXCEPTION 'REF01_V3_STALE_COHORT_APPROVAL';END IF;
 IF v_s.content->>'selected_company_count' IS DISTINCT FROM '3000' OR v_s.content#>>'{counts,unresolved_cutoff_cases}' IS DISTINCT FROM '0'
 OR v_s.content#>>'{counts,duplicate_eligible_company_ids}' IS DISTINCT FROM '0'
 OR jsonb_array_length(v_s.content->'unresolved_cases')<>0 THEN RAISE EXCEPTION 'REF01_V3_ALL_CUTOFF_CHANGING_CASES_MUST_BE_RESOLVED';END IF;
 SELECT * INTO STRICT v_v FROM population_top3000_202608_202609_v1.company_verification_v1 v WHERE v.verification_id=p_verification;
 IF v_v.result->>'kind' IS DISTINCT FROM 'REF01_COHORT_INDEPENDENT_APPROVAL_V3' OR v_v.result->>'result' IS DISTINCT FROM 'PASS'
 OR v_v.result->>'scope_sha256' IS DISTINCT FROM '8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd'
 OR v_v.result->>'snapshot_sha256' IS DISTINCT FROM p_snapshot OR v_v.result->>'head_manifest_sha256' IS DISTINCT FROM v_s.head_manifest_sha256
 OR v_v.result->'input_versions' IS DISTINCT FROM v_r.input_versions
 OR v_v.result->'independent_of_producer' IS DISTINCT FROM 'true'::jsonb
 OR NOT population_top3000_202608_202609_v1.ref01_review_authorized_v4(v_v.result)
 OR v_v.verifier_version LIKE 'REF01_ADJUDICATION_V3%'
 OR v_v.result->'full_population_checks' IS DISTINCT FROM '{"missing_cases":0,"unexpected_cases":0,"duplicate_companies":0,"source_record_mismatches":0,"unresolved_cutoff_cases":0,"unresolved_class_or_issuer_conflicts":0,"unresolved_instrument_cases":0}'::jsonb
 OR v_v.result->'dated_vendor_reconstruction_method_reviewed' IS DISTINCT FROM 'true'::jsonb
 OR v_v.result->'rank_date_share_and_action_risks_reviewed' IS DISTINCT FROM 'true'::jsonb
 OR v_v.result->'all_potential_cutoff_crossings_resolved' IS DISTINCT FROM 'true'::jsonb
 OR v_v.result->'all_selected_company_classes_and_target_period_listings_reconciled' IS DISTINCT FROM 'true'::jsonb
 OR v_v.result->'original_historical_receipt_inferred' IS DISTINCT FROM 'false'::jsonb
 THEN RAISE EXCEPTION 'REF01_V3_COMPLETE_INDEPENDENT_COHORT_APPROVAL_REQUIRED';END IF;
 -- The approval must bind the exact predetermined risk set, including every critical case.
 IF jsonb_typeof(v_v.result->'risk_case_receipts') IS DISTINCT FROM 'array'
 OR jsonb_array_length(v_v.result->'risk_case_receipts')<>jsonb_array_length(v_s.content->'independent_risk_cases')
 THEN RAISE EXCEPTION 'REF01_V3_EXACT_RISK_CASE_SET_REQUIRED';END IF;
 FOR v_item IN SELECT value FROM jsonb_array_elements(v_s.content->'independent_risk_cases') LOOP
  SELECT q.value INTO v_map FROM jsonb_array_elements(v_v.result->'risk_case_receipts') q WHERE q.value->>'case_key'=v_item->>'case_key';
  IF v_map IS NULL OR (SELECT count(*) FROM jsonb_array_elements(v_v.result->'risk_case_receipts') q WHERE q.value->>'case_key'=v_item->>'case_key')<>1
  OR v_map->>'case_sha256' IS DISTINCT FROM v_item->>'record_sha256' OR nullif(v_map->>'verification_id','') IS NULL
  THEN RAISE EXCEPTION 'REF01_V3_RISK_CASE_RECEIPT_MISSING_OR_DUPLICATE';END IF;
  SELECT * INTO STRICT v_receipt FROM population_top3000_202608_202609_v1.company_verification_v1 v WHERE v.verification_id=(v_map->>'verification_id')::uuid;
  IF v_map->>'verification_sha256' IS DISTINCT FROM population_top3000_202608_202609_v1.ref01_adjudication_hash_v3(to_jsonb(v_receipt))
  OR v_receipt.result->>'kind' IS DISTINCT FROM 'REF01_RISK_CASE_VERIFICATION_V3' OR v_receipt.result->>'result' IS DISTINCT FROM 'PASS'
  OR v_receipt.result->>'case_key' IS DISTINCT FROM v_item->>'case_key' OR v_receipt.result->>'case_sha256' IS DISTINCT FROM v_item->>'record_sha256'
  OR v_receipt.result->'independent_of_producer' IS DISTINCT FROM 'true'::jsonb
  OR NOT population_top3000_202608_202609_v1.ref01_review_authorized_v4(v_receipt.result)
  OR v_receipt.result->'source_comparison_supported' IS DISTINCT FROM 'true'::jsonb
  OR v_receipt.result->'retained_source_bytes_independently_rehashed' IS DISTINCT FROM 'true'::jsonb
  OR v_receipt.result->'identity_class_and_material_cap_risks_resolved' IS DISTINCT FROM 'true'::jsonb
  OR jsonb_typeof(v_receipt.result->'source_refs') IS DISTINCT FROM 'array' OR jsonb_array_length(v_receipt.result->'source_refs')=0
  THEN RAISE EXCEPTION 'REF01_V3_SOURCE_GROUNDED_RISK_REVIEW_REQUIRED';END IF;
  PERFORM population_top3000_202608_202609_v1.ref01_adjudication_source_headers_v3(v_receipt.result->'source_refs');
 END LOOP;
 IF jsonb_typeof(v_v.result->'exclusion_sample_receipts') IS DISTINCT FROM 'array'
 OR jsonb_array_length(v_v.result->'exclusion_sample_receipts')<>jsonb_array_length(v_s.content->'independent_exclusion_sample')
 THEN RAISE EXCEPTION 'REF01_V3_EXACT_EXCLUSION_SAMPLE_SET_REQUIRED';END IF;
 FOR v_item IN SELECT value FROM jsonb_array_elements(v_s.content->'independent_exclusion_sample') LOOP
  SELECT q.value INTO v_map FROM jsonb_array_elements(v_v.result->'exclusion_sample_receipts') q WHERE q.value->>'ticker'=v_item->>'ticker';
  IF v_map IS NULL OR (SELECT count(*) FROM jsonb_array_elements(v_v.result->'exclusion_sample_receipts') q WHERE q.value->>'ticker'=v_item->>'ticker')<>1
  OR v_map->>'security_projection_sha256' IS DISTINCT FROM v_item->>'output_sha256'
  THEN RAISE EXCEPTION 'REF01_V3_EXCLUSION_SAMPLE_RECEIPT_MISSING';END IF;
  SELECT * INTO STRICT v_receipt FROM population_top3000_202608_202609_v1.company_verification_v1 v WHERE v.verification_id=(v_map->>'verification_id')::uuid;
  IF v_map->>'verification_sha256' IS DISTINCT FROM population_top3000_202608_202609_v1.ref01_adjudication_hash_v3(to_jsonb(v_receipt))
  OR v_receipt.result->>'kind' IS DISTINCT FROM 'REF01_EXCLUSION_SAMPLE_VERIFICATION_V3' OR v_receipt.result->>'result' IS DISTINCT FROM 'PASS'
  OR v_receipt.result->>'ticker' IS DISTINCT FROM v_item->>'ticker'
  OR v_receipt.result->>'security_projection_sha256' IS DISTINCT FROM v_item->>'output_sha256'
  OR v_receipt.result->'independent_of_producer' IS DISTINCT FROM 'true'::jsonb
  OR NOT population_top3000_202608_202609_v1.ref01_review_authorized_v4(v_receipt.result)
  OR v_receipt.result->'security_type_exclusion_supported' IS DISTINCT FROM 'true'::jsonb
  OR v_receipt.result->'retained_source_bytes_independently_rehashed' IS DISTINCT FROM 'true'::jsonb
  OR jsonb_typeof(v_receipt.result->'source_refs') IS DISTINCT FROM 'array' OR jsonb_array_length(v_receipt.result->'source_refs')=0
  THEN RAISE EXCEPTION 'REF01_V3_SOURCE_GROUNDED_EXCLUSION_REVIEW_REQUIRED';END IF;
  PERFORM population_top3000_202608_202609_v1.ref01_adjudication_source_headers_v3(v_receipt.result->'source_refs');
 END LOOP;
 SELECT count(*)::integer,encode(sha256(convert_to(string_agg(i.security_id||':'||i.listing_episode_id||':'||i.record_sha256,E'\n' ORDER BY i.security_id COLLATE "C",i.listing_episode_id COLLATE "C"),'UTF8')),'hex'),
 count(*) FILTER(WHERE i.record_sha256 IS DISTINCT FROM population_top3000_202608_202609_v1.ref01_adjudication_hash_v3(to_jsonb(i)-'snapshot_sha256'-'record_sha256'))::integer
 INTO v_instruments,v_im,v_bad FROM population_top3000_202608_202609_v1.ref01_cohort_instrument_v3 i WHERE i.snapshot_sha256=p_snapshot;
 IF v_instruments NOT BETWEEN 3000 AND 50000 OR v_bad<>0
 OR v_v.result->>'instrument_manifest_sha256' IS DISTINCT FROM v_im OR (v_v.result->>'instrument_count')::integer IS DISTINCT FROM v_instruments
 OR EXISTS(SELECT 1 FROM jsonb_array_elements(v_s.content->'ranking') r WHERE (r->>'company_rank')::integer<=3000 AND NOT EXISTS(
  SELECT 1 FROM population_top3000_202608_202609_v1.ref01_cohort_instrument_v3 i WHERE i.snapshot_sha256=p_snapshot AND i.company_id=r->>'company_id'
  AND i.listing_start<=TIMESTAMPTZ '2026-08-03 20:00Z' AND(i.listing_end IS NULL OR i.listing_end>TIMESTAMPTZ '2026-08-03 20:00Z')))
 OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.ref01_cohort_instrument_v3 a JOIN population_top3000_202608_202609_v1.ref01_cohort_instrument_v3 b
  ON b.snapshot_sha256=a.snapshot_sha256 AND b.security_id=a.security_id AND b.listing_episode_id>a.listing_episode_id
  AND a.listing_start<coalesce(b.listing_end,'infinity'::timestamptz) AND b.listing_start<coalesce(a.listing_end,'infinity'::timestamptz) WHERE a.snapshot_sha256=p_snapshot)
 THEN RAISE EXCEPTION 'REF01_V3_COMPLETE_UNAMBIGUOUS_DATED_INSTRUMENT_MANIFEST_REQUIRED';END IF;
 SELECT population_top3000_202608_202609_v1.ref01_adjudication_hash_v3(jsonb_agg(r.value ORDER BY (r.value->>'company_rank')::integer)) INTO v_rank_sha
 FROM jsonb_array_elements(v_s.content->'ranking') r WHERE (r.value->>'company_rank')::integer<=3000;
 IF v_v.result->>'selected_ranking_sha256' IS DISTINCT FROM v_rank_sha THEN RAISE EXCEPTION 'REF01_V3_EXACT_3000_COMPANY_RANKING_APPROVAL_REQUIRED';END IF;
 RETURN jsonb_build_object('snapshot_sha256',p_snapshot,'head_manifest_sha256',v_s.head_manifest_sha256,
  'approval_verification_id',p_verification,'approval_sha256',population_top3000_202608_202609_v1.ref01_adjudication_hash_v3(to_jsonb(v_v)),
  'selected_ranking_sha256',v_rank_sha,'company_count',3000,'instrument_count',v_instruments,'instrument_manifest_sha256',v_im,
  'qualification','INDEPENDENTLY_REVIEWED_DATED_SOURCE_RECONSTRUCTION','original_historical_receipt_proven',false,
  'full_24_family_mission_complete',false);
END $function$;


CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.ref01_cohort_stage_instruments_v3(p_snapshot text, p_rows jsonb, p_verification uuid)
 RETURNS jsonb
 LANGUAGE plpgsql
 SET search_path TO 'pg_catalog'
 SET "TimeZone" TO 'UTC'
AS $function$
DECLARE v_s population_top3000_202608_202609_v1.ref01_adjudication_snapshot_v3%rowtype;
 v_v population_top3000_202608_202609_v1.company_verification_v1%rowtype;v_row jsonb;v_record jsonb;v_old jsonb;v_hash text;v_new integer:=0;v_n integer;
BEGIN
 PERFORM population_top3000_202608_202609_v1.ref01_adjudication_controller_v3(true);
 SELECT * INTO STRICT v_s FROM population_top3000_202608_202609_v1.ref01_adjudication_snapshot_v3 s WHERE s.snapshot_sha256=p_snapshot;
 SELECT * INTO STRICT v_v FROM population_top3000_202608_202609_v1.company_verification_v1 v WHERE v.verification_id=p_verification;
 IF jsonb_typeof(p_rows) IS DISTINCT FROM 'array' OR jsonb_array_length(p_rows) NOT BETWEEN 1 AND 100
 OR v_v.result->>'kind' IS DISTINCT FROM 'REF01_INSTRUMENT_BATCH_REVIEW_V3' OR v_v.result->>'result' IS DISTINCT FROM 'PASS'
 OR v_v.result->>'snapshot_sha256' IS DISTINCT FROM p_snapshot
 OR v_v.result->>'rows_sha256' IS DISTINCT FROM population_top3000_202608_202609_v1.ref01_adjudication_hash_v3(p_rows)
 OR v_v.result->'independent_of_producer' IS DISTINCT FROM 'true'::jsonb
 OR v_v.result->'dated_interval_semantics_and_company_links_verified' IS DISTINCT FROM 'true'::jsonb
 OR v_v.result->'all_rows_include_explicit_source_bounds' IS DISTINCT FROM 'true'::jsonb
 OR v_v.result->'current_universe_or_interval_extension_inferred' IS DISTINCT FROM 'false'::jsonb
 OR NOT population_top3000_202608_202609_v1.ref01_review_authorized_v4(v_v.result)
 THEN RAISE EXCEPTION 'REF01_V3_EXACT_DATED_INSTRUMENT_BATCH_REVIEW_REQUIRED';END IF;
 PERFORM population_top3000_202608_202609_v1.ref01_adjudication_sources_v3(v_v.result->'source_refs');
 FOR v_row IN SELECT value FROM jsonb_array_elements(p_rows) LOOP
  IF jsonb_typeof(v_row) IS DISTINCT FROM 'object' OR nullif(v_row->>'security_id','') IS NULL OR nullif(v_row->>'listing_episode_id','') IS NULL
  OR nullif(v_row->>'ticker','') IS NULL OR v_row->>'security_type' NOT IN('CS','ADRC') OR v_row->>'security_type' IS NULL
  OR v_row->>'exchange' NOT IN('XNYS','XNAS','XASE','ARCX','BATS','IEXG') OR v_row->>'exchange' IS NULL
  OR nullif(v_row->>'listing_start','') IS NULL OR (v_row->>'listing_start')::timestamptz>=TIMESTAMPTZ '2026-10-01 04:00Z'
  OR ((v_row->>'listing_end') IS NOT NULL AND((v_row->>'listing_end')::timestamptz<=(v_row->>'listing_start')::timestamptz
    OR (v_row->>'listing_end')::timestamptz<=TIMESTAMPTZ '2026-08-01 04:00Z'))
  OR jsonb_typeof(v_row->'links_evidence') IS DISTINCT FROM 'object'
  OR v_row#>>'{links_evidence,qualification}' IS DISTINCT FROM 'DATED_SOURCE_RECONSTRUCTION'
  OR v_row#>'{links_evidence,original_historical_receipt_inferred}' IS DISTINCT FROM 'false'::jsonb
  OR nullif(v_row#>>'{links_evidence,source_bounds_basis}','') IS NULL
  OR(jsonb_typeof(v_row#>'{links_evidence,source_refs}') IS DISTINCT FROM 'array' OR jsonb_array_length(v_row#>'{links_evidence,source_refs}')=0)
  OR(nullif(v_row#>>'{links_evidence,share_class_figi}','') IS NULL AND nullif(v_row#>>'{links_evidence,composite_figi}','') IS NULL
   AND(nullif(v_row#>>'{links_evidence,instrument_key}','') IS NULL OR v_row#>'{links_evidence,persistent_key_date_scoped}' IS DISTINCT FROM 'true'::jsonb))
  OR NOT EXISTS(SELECT 1 FROM jsonb_array_elements(v_s.content->'ranking') r WHERE (r->>'company_rank')::integer<=3000 AND r->>'company_id'=v_row->>'company_id')
  THEN RAISE EXCEPTION 'REF01_V3_DATED_INSTRUMENT_ROW_INVALID';END IF;
  v_record:=jsonb_build_object('security_id',v_row->>'security_id','listing_episode_id',v_row->>'listing_episode_id',
   'company_id',v_row->>'company_id','ticker',v_row->>'ticker','security_type',v_row->>'security_type','exchange',v_row->>'exchange',
   'listing_start',(v_row->>'listing_start')::timestamptz,'listing_end',(v_row->>'listing_end')::timestamptz,
   'links_evidence',v_row->'links_evidence'||jsonb_build_object('instrument_batch_verification_id',p_verification,
    'instrument_batch_verification_sha256',population_top3000_202608_202609_v1.ref01_adjudication_hash_v3(to_jsonb(v_v))));
  v_hash:=population_top3000_202608_202609_v1.ref01_adjudication_hash_v3(v_record);
  INSERT INTO population_top3000_202608_202609_v1.ref01_cohort_instrument_v3(snapshot_sha256,security_id,listing_episode_id,company_id,ticker,security_type,exchange,listing_start,listing_end,links_evidence,record_sha256)
  VALUES(p_snapshot,v_row->>'security_id',v_row->>'listing_episode_id',v_row->>'company_id',v_row->>'ticker',v_row->>'security_type',v_row->>'exchange',
   (v_row->>'listing_start')::timestamptz,(v_row->>'listing_end')::timestamptz,v_record->'links_evidence',v_hash) ON CONFLICT DO NOTHING;
  GET DIAGNOSTICS v_n=ROW_COUNT;v_new:=v_new+v_n;
  SELECT to_jsonb(i)-'snapshot_sha256'-'record_sha256' INTO v_old FROM population_top3000_202608_202609_v1.ref01_cohort_instrument_v3 i
  WHERE i.snapshot_sha256=p_snapshot AND i.security_id=v_row->>'security_id' AND i.listing_episode_id=v_row->>'listing_episode_id';
  IF v_old IS DISTINCT FROM v_record THEN RAISE EXCEPTION 'REF01_V3_INSTRUMENT_REPLAY_CONFLICT';END IF;
 END LOOP;
 IF (SELECT count(*) FROM population_top3000_202608_202609_v1.ref01_cohort_instrument_v3 i WHERE i.snapshot_sha256=p_snapshot)>50000
 THEN RAISE EXCEPTION 'REF01_V3_INSTRUMENT_COUNT_BOUND';END IF;
 PERFORM population_top3000_202608_202609_v1.ref01_adjudication_controller_v3(true);
 RETURN jsonb_build_object('snapshot_sha256',p_snapshot,'reviewed_rows',jsonb_array_length(p_rows),'new_rows',v_new,
  'cohort_still_unfrozen',true,'no_provider_request',true);
END $function$;
