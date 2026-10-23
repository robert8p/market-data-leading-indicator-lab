-- Root only, after reviewing/installing the recorder. Synthetic source is explicit.
-- Every data/control/event effect below is rolled back. No provider request.
BEGIN;
SET LOCAL statement_timeout='20s';
SET LOCAL lock_timeout='2s';
SELECT cron.alter_job(596,active=>false);
SELECT pg_advisory_xact_lock(2026083000261022);
CREATE TEMP TABLE component_overlay_test_receipt(result jsonb) ON COMMIT DROP;
DO $test$
DECLARE
 v_id constant text:='__ROLLBACK_SYNTHETIC_REF01_COMPONENT_V1';
 v_body text:='{"results":{"accession_number":"0001193125-26-330643","entities":[{"company_data":{"cik":"0000025475","name":"SYNTHETIC CRAWFORD FIXTURE ONLY"}}]}}';
 v_sha text;v_payload jsonb;v_first jsonb;v_replay jsonb;v_negative jsonb;v_expected text;
 v_before jsonb;v_after jsonb;v_rejected integer:=0;v_existing bigint;v_refs jsonb;
BEGIN
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE status='LEASED')
  OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_provider_lease WHERE released_at IS NULL)
 THEN RAISE EXCEPTION 'TEST_REQUIRES_PAUSED_DRAINED_RUNTIME';END IF;
 SELECT jsonb_build_object(
  'control',(SELECT to_jsonb(c) FROM population_top3000_202608_202609_v1.acquisition_control c WHERE singleton),
  'pipeline',(SELECT to_jsonb(p) FROM population_top3000_202608_202609_v1.pipeline_control p WHERE singleton),
  'candidate_count',(SELECT count(*) FROM population_top3000_202608_202609_v1.company_candidate_v1),
  'group_count',(SELECT count(*) FROM population_top3000_202608_202609_v1.company_group_v1),
  'exception_count',(SELECT count(*) FROM population_top3000_202608_202609_v1.company_exception_v1),
  'crawford_candidates',(SELECT jsonb_agg(to_jsonb(c) ORDER BY ticker) FROM population_top3000_202608_202609_v1.company_candidate_v1 c WHERE ticker IN('CRD.A','CRD.B')),
  'crawford_group',(SELECT to_jsonb(g) FROM population_top3000_202608_202609_v1.company_group_v1 g WHERE group_key='CIK:0000025475'),
  'universe',(SELECT jsonb_agg(to_jsonb(u)) FROM population_top3000_202608_202609_v1.pipeline_universe u)
 ) INTO v_before;
 SELECT count(*) INTO v_existing FROM population_top3000_202608_202609_v1.company_component_source_evidence_v1;
 SELECT jsonb_agg(jsonb_build_object('ticker',ticker,'census_sha256',census_sha256,
   'overview_sha256',overview_sha256,'share_class_figi',share_class_figi) ORDER BY ticker)
 INTO v_refs FROM population_top3000_202608_202609_v1.company_candidate_v1 WHERE ticker IN('CRD.A','CRD.B');
 IF jsonb_array_length(v_refs) IS DISTINCT FROM 2 THEN RAISE EXCEPTION 'ACTUAL_CRAWFORD_CANDIDATES_REQUIRED';END IF;
 v_sha:=encode(sha256(convert_to(v_body,'UTF8')),'hex');
 INSERT INTO population_top3000_202608_202609_v1.source_document
  (source_id,source_sha256,source_uri,source_provider,retrieved_at,content_text,content_bytes,evidence,license_policy)
 VALUES(v_id,v_sha,'https://example.invalid/synthetic-rollback-only','SYNTHETIC_TEST_ONLY',clock_timestamp(),
  v_body,octet_length(v_body),'{"synthetic":true,"rollback_only":true,"external_requests":0}'::jsonb,'SYNTHETIC_ROLLBACK_ONLY');
 v_payload:=jsonb_build_object('case_id','CRAWFORD_CUTOFF_CLASS_VALUATION','tickers',jsonb_build_array('CRD.A','CRD.B'),
  'component','FILING_REGISTRANT','source_id',v_id,'source_sha256',v_sha,'source_boundary','CONNECTOR_RECEIPT',
  'source_anchor',jsonb_build_object('type','JSON_PATH','path',jsonb_build_array('results','entities','0','company_data')),
  'observed_fact',v_body::jsonb#>'{results,entities,0,company_data}','source_state_date',NULL,
  'contrary_evidence',jsonb_build_array('SYNTHETIC FIXTURE; no real issuer claim'),
  'unresolved_requirements',jsonb_build_array('ALL REAL IDENTITY/CAP REQUIREMENTS UNCHANGED'),
  'expected_candidate_refs',v_refs);
 v_first:=population_top3000_202608_202609_v1.company_record_component_source_v1(v_payload);
 v_replay:=population_top3000_202608_202609_v1.company_record_component_source_v1(v_payload);
 IF v_first->>'new_source_facts' IS DISTINCT FROM '1' OR v_replay->>'new_source_facts' IS DISTINCT FROM '0'
  OR v_replay->>'evidence_sha256' IS DISTINCT FROM v_first->>'evidence_sha256'
 THEN RAISE EXCEPTION 'COMPONENT_RECORD_OR_REPLAY_FAILED';END IF;
 FOR v_negative,v_expected IN SELECT * FROM (VALUES
  (jsonb_set(v_payload,'{source_sha256}',to_jsonb(repeat('0',64))),'COMPANY_COMPONENT_SOURCE_MISSING_CHANGED_OR_OVERSIZED'),
  (jsonb_set(v_payload,'{source_anchor,path}',jsonb_build_array('results','missing')),'COMPANY_COMPONENT_SOURCE_FACT_NOT_AT_JSON_ANCHOR'),
  (jsonb_set(v_payload,'{expected_candidate_refs,0,census_sha256}',to_jsonb(repeat('0',64))),'COMPANY_COMPONENT_RAW_CANDIDATE_REVISION_CHANGED'),
  (jsonb_set(v_payload,'{case_id}','"NBN_LPX_WRONG_ISSUER_CIK"'::jsonb),'COMPANY_COMPONENT_CASE_OR_TICKER_SCOPE_INVALID'),
  (v_payload||'{"universe_frozen":true}'::jsonb,'COMPANY_COMPONENT_RECORD_SHAPE_INVALID'),
  (jsonb_set(v_payload,'{unresolved_requirements}','[]'::jsonb),'COMPANY_COMPONENT_RECORD_SHAPE_INVALID')
 ) negative_cases(payload,expected_error) LOOP
  BEGIN
   PERFORM population_top3000_202608_202609_v1.company_record_component_source_v1(v_negative);
   RAISE EXCEPTION 'NEGATIVE_UNEXPECTEDLY_ACCEPTED';
  EXCEPTION WHEN OTHERS THEN
   IF SQLERRM IS DISTINCT FROM v_expected THEN RAISE EXCEPTION 'NEGATIVE_WRONG_REJECTION: %',v_expected;END IF;
   v_rejected:=v_rejected+1;
  END;
 END LOOP;
 BEGIN
  UPDATE population_top3000_202608_202609_v1.acquisition_control SET owner_fence=1 WHERE singleton;
  PERFORM population_top3000_202608_202609_v1.company_record_component_source_v1(v_payload);
  RAISE EXCEPTION 'STALE_FENCE_UNEXPECTEDLY_ACCEPTED';
 EXCEPTION WHEN OTHERS THEN
  IF SQLERRM IS DISTINCT FROM 'COMPANY_COMPONENT_OWNER_SCOPE_OR_ADMISSION_MISMATCH' THEN RAISE EXCEPTION 'STALE_FENCE_WRONG_REJECTION';END IF;
  v_rejected:=v_rejected+1;
 END;
 BEGIN
  UPDATE population_top3000_202608_202609_v1.acquisition_control SET stop_generation=1 WHERE singleton;
  PERFORM population_top3000_202608_202609_v1.company_record_component_source_v1(v_payload);
  RAISE EXCEPTION 'STOP_GENERATION_UNEXPECTEDLY_ACCEPTED';
 EXCEPTION WHEN OTHERS THEN
  IF SQLERRM IS DISTINCT FROM 'COMPANY_COMPONENT_OWNER_SCOPE_OR_ADMISSION_MISMATCH' THEN RAISE EXCEPTION 'STOP_WRONG_REJECTION';END IF;
  v_rejected:=v_rejected+1;
 END;
 SELECT jsonb_build_object(
  'control',(SELECT to_jsonb(c) FROM population_top3000_202608_202609_v1.acquisition_control c WHERE singleton),
  'pipeline',(SELECT to_jsonb(p) FROM population_top3000_202608_202609_v1.pipeline_control p WHERE singleton),
  'candidate_count',(SELECT count(*) FROM population_top3000_202608_202609_v1.company_candidate_v1),
  'group_count',(SELECT count(*) FROM population_top3000_202608_202609_v1.company_group_v1),
  'exception_count',(SELECT count(*) FROM population_top3000_202608_202609_v1.company_exception_v1),
  'crawford_candidates',(SELECT jsonb_agg(to_jsonb(c) ORDER BY ticker) FROM population_top3000_202608_202609_v1.company_candidate_v1 c WHERE ticker IN('CRD.A','CRD.B')),
  'crawford_group',(SELECT to_jsonb(g) FROM population_top3000_202608_202609_v1.company_group_v1 g WHERE group_key='CIK:0000025475'),
  'universe',(SELECT jsonb_agg(to_jsonb(u)) FROM population_top3000_202608_202609_v1.pipeline_universe u)
 ) INTO v_after;
 IF v_rejected<>8 OR v_after IS DISTINCT FROM v_before
  OR (SELECT count(*) FROM population_top3000_202608_202609_v1.company_component_source_evidence_v1)<>v_existing+1
  OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.company_component_source_evidence_v1
    WHERE identity_release_allowed OR strict_certified OR rank_day_shares_proven OR company_cap_verified OR selected_company_member)
 THEN RAISE EXCEPTION 'COMPONENT_PRESERVATION_OR_HOLD_INVARIANT_FAILED';END IF;
 INSERT INTO component_overlay_test_receipt VALUES(jsonb_build_object('result','PASS_SOURCE_COMPONENT_RECORD_REPLAY_NEGATIVES_AND_HOLDS',
  'synthetic_fixture',true,'real_identity_verified',false,'negative_rejections',v_rejected,
  'source_fact_rows_in_transaction',1,'provider_requests',0,'candidate_group_universe_and_control_unchanged',true,
  'all_release_and_cap_flags_false',true,'actual_source_semantic_review_still_required',true));
END $test$;
SELECT result FROM component_overlay_test_receipt;
ROLLBACK;
