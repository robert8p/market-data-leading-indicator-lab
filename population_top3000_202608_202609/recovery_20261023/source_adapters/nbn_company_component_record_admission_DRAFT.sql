-- ROOT ONLY DRAFT. Requires the separately reviewed company_component_source_evidence_v1 table
-- and company_record_component_source_v1 recorder. No fetch, source recopy, candidate/group change or publication.
-- Exact existing primary fact extracts and original vendor candidate fingerprints are pinned below.
BEGIN;
SET LOCAL statement_timeout='10s';
SET LOCAL lock_timeout='2s';
SELECT pg_advisory_xact_lock(2026083000261022);
CREATE TEMP TABLE nbn_component_record_receipts(component text,receipt jsonb) ON COMMIT DROP;
DO $record$
DECLARE
 records_text constant text:=$records$[{"case_id":"NBN_LPX_WRONG_ISSUER_CIK","source_boundary":"EXTRACTED_TEXT_WITH_ORIGINAL_FILE_REFERENCE","source_anchor":{"type":"JSON_PATH","path":["facts"]},"source_id":"ref01-primary-issuer-excerpt:NBN:20260716:v1","source_sha256":"623ccf39fce919e1052b0ab281c7b8380140a28032ca7f10f1c587e1a88a9031","tickers":["NBN"],"component":"ISSUER_SECURITY_ASSOCIATION","observed_fact":{"issuer_name":"Northeast Bank","ticker":"NBN","listing_market":"NASDAQ","issuer_type":"BANK","state":"Maine"},"source_state_date":"2026-07-16","expected_candidate_refs":[{"ticker":"NBN","census_sha256":"7d84b700dee4de26c643c718d3fd50fcfcf7fbb4bcd164e51bbc297c1fd499d4","overview_sha256":"06d69d3d92419a6f1996697e4cee7f8faaacff282f1064063557bef60deeba2d","share_class_figi":"BBG001SDS4K7"}],"contrary_evidence":{"preserved_vendor_cik":"0000060519","preserved_vendor_sic":"2400","preserved_vendor_sic_description":"LUMBER & WOOD PRODUCTS (NO FURNITURE)","primary_source_type":"OFFICIAL_ISSUER_PRESS_RELEASE","primary_source_location":"https://investor.northeastbank.com/news-releases/news-release-details/northeast-bank-announces-dates-fiscal-2026-fourth-quarter"},"unresolved_requirements":["The retained source is a root fact extraction with a primary-source location, not full original HTML/PDF or network bytes.","No replacement CIK is asserted; missing SEC CIK does not make an FDIC-reporting bank ineligible.","Company identity release, frozen membership, August3 shares and company market capitalisation remain unproven.","This append-only component record does not modify the original vendor candidate, grouping, exception or universe."]},{"case_id":"NBN_LPX_WRONG_ISSUER_CIK","source_boundary":"EXTRACTED_TEXT_WITH_ORIGINAL_FILE_REFERENCE","source_anchor":{"type":"JSON_PATH","path":["facts"]},"source_id":"ref01-primary-issuer-excerpt:NBN:20260716:v1","source_sha256":"623ccf39fce919e1052b0ab281c7b8380140a28032ca7f10f1c587e1a88a9031","tickers":["LPX","NBN"],"component":"REJECTED_SOURCE_ASSOCIATION","observed_fact":{"issuer_name":"Northeast Bank","ticker":"NBN","listing_market":"NASDAQ","issuer_type":"BANK","state":"Maine"},"source_state_date":"2026-07-16","expected_candidate_refs":[{"ticker":"LPX","census_sha256":"7d84b700dee4de26c643c718d3fd50fcfcf7fbb4bcd164e51bbc297c1fd499d4","overview_sha256":"9ff5e83de4b0299090b7e399e818913e28a700ab54ec57dbc5db8ad1d689a68b","share_class_figi":"BBG001S5SW04"},{"ticker":"NBN","census_sha256":"7d84b700dee4de26c643c718d3fd50fcfcf7fbb4bcd164e51bbc297c1fd499d4","overview_sha256":"06d69d3d92419a6f1996697e4cee7f8faaacff282f1064063557bef60deeba2d","share_class_figi":"BBG001SDS4K7"}],"contrary_evidence":{"association_rejected_for_review":"NBN and LPX being treated as one issuer solely through vendor CIK0000060519","preserved_vendor_nbn_cik":"0000060519","preserved_vendor_lpx_cik":"0000060519","preserved_lpx_name":"Louisiana-Pacific Corp.","distinct_retained_share_class_figis":{"NBN":"BBG001SDS4K7","LPX":"BBG001S5SW04"},"reason":"The issuer primary release identifies NBN as Northeast Bank, a Maine bank; the retained LPX candidate identifies Louisiana-Pacific. Preserve both source records and separate the erroneous association in a reviewed future identity revision."},"unresolved_requirements":["The retained source is a root fact extraction with a primary-source location, not full original HTML/PDF or network bytes.","No replacement CIK is asserted; missing SEC CIK does not make an FDIC-reporting bank ineligible.","Company identity release, frozen membership, August3 shares and company market capitalisation remain unproven.","This append-only component record does not modify the original vendor candidate, grouping, exception or universe.","No corrected grouping or canonical-company key is applied by this evidence recorder."]},{"case_id":"NBN_LPX_WRONG_ISSUER_CIK","source_boundary":"EXTRACTED_TEXT_WITH_ORIGINAL_FILE_REFERENCE","source_anchor":{"type":"JSON_PATH","path":["facts"]},"source_id":"ref01-primary-issuer-facts:NBN:20260727:v1","source_sha256":"ae7c2104d26ec183b94c381651116e152efcaa076588a6f2d0c2571394cfe1ca","tickers":["NBN"],"component":"SHARES_AT_SOURCE_DATE","observed_fact":{"issuer_name":"Northeast Bank","ticker":"NBN","listing_market":"NASDAQ","reporting_authority":"FDIC","shares_as_of":"2026-06-30","voting_common_shares_issued_and_outstanding":8555360,"nonvoting_common_shares_issued_and_outstanding":0,"preferred_shares_issued_and_outstanding":0},"source_state_date":"2026-06-30","expected_candidate_refs":[{"ticker":"NBN","census_sha256":"7d84b700dee4de26c643c718d3fd50fcfcf7fbb4bcd164e51bbc297c1fd499d4","overview_sha256":"06d69d3d92419a6f1996697e4cee7f8faaacff282f1064063557bef60deeba2d","share_class_figi":"BBG001SDS4K7"}],"contrary_evidence":{"primary_source_type":"OFFICIAL_ISSUER_EARNINGS_RELEASE_PDF","primary_source_location":"https://investor.northeastbank.com/node/16061/pdf","source_publication_date":"2026-07-27","table_location":"Balance sheets, PDF pages5–6; June30,2026 column; amounts in thousands except share/per-share data.","aug3_shares_proven":false,"aug3_cap_proven":false},"unresolved_requirements":["The retained source is a root fact extraction with a primary-source location, not full original HTML/PDF or network bytes.","No replacement CIK is asserted; missing SEC CIK does not make an FDIC-reporting bank ineligible.","Company identity release, frozen membership, August3 shares and company market capitalisation remain unproven.","This append-only component record does not modify the original vendor candidate, grouping, exception or universe.","Reconcile share issuance, repurchases and other changes from June30 through August3 before using this source-date count in an August3 company-cap calculation."]}]$records$;
 item jsonb;result jsonb;repeat_result jsonb;v_sha text;v_group_sha text;v_candidate_sha text;v_universe_sha text;
 v_after text;v_provider bigint;v_http bigint;v_stored bigint;v_reserved bigint;v_count bigint;v_new integer:=0;v_record_count integer:=0;
BEGIN
 IF encode(sha256(convert_to(records_text,'UTF8')),'hex') IS DISTINCT FROM 'bac0c242caf9158e0b4a8a6a3dacdc9f61826d6eb8b1c8af4a7c1f87cd28888e' THEN RAISE EXCEPTION 'NBN_COMPONENT_RECORD_MANIFEST_CHANGED';END IF;
 SELECT provider_requests,http_invocations,stored_bytes,reserved_bytes INTO v_provider,v_http,v_stored,v_reserved
 FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton FOR UPDATE;
 -- Exact two candidate rows plus their current grouping are small, stable proof boundaries.
 SELECT encode(sha256(convert_to(coalesce(jsonb_agg(to_jsonb(q) ORDER BY q.ticker)::text,'[]'),'UTF8')),'hex') INTO v_candidate_sha
 FROM population_top3000_202608_202609_v1.company_candidate_v1 q WHERE q.ticker IN('NBN','LPX');
 SELECT encode(sha256(convert_to(coalesce(jsonb_agg(to_jsonb(q) ORDER BY q.group_key)::text,'[]'),'UTF8')),'hex') INTO v_group_sha
 FROM population_top3000_202608_202609_v1.company_group_v1 q WHERE q.group_key IN('CIK:0000060519');
 SELECT encode(sha256(convert_to(coalesce(jsonb_agg(to_jsonb(q))::text,'[]'),'UTF8')),'hex') INTO v_universe_sha
 FROM population_top3000_202608_202609_v1.pipeline_universe q;
 FOR item IN SELECT value FROM jsonb_array_elements(records_text::jsonb) LOOP
  result:=population_top3000_202608_202609_v1.company_record_component_source_v1(item);
  repeat_result:=population_top3000_202608_202609_v1.company_record_component_source_v1(item);
  IF result->>'state' IS DISTINCT FROM 'SOURCE_FACT_RECORDED_REVIEW_PENDING'
   OR repeat_result->>'evidence_sha256' IS DISTINCT FROM result->>'evidence_sha256'
   OR repeat_result->>'new_source_facts' IS DISTINCT FROM '0'
   OR result->>'identity_release_allowed' IS DISTINCT FROM 'false'
   OR result->>'rank_day_shares_proven' IS DISTINCT FROM 'false'
   OR result->>'company_cap_verified' IS DISTINCT FROM 'false'
   OR result->>'universe_frozen' IS DISTINCT FROM 'false' THEN RAISE EXCEPTION 'NBN_COMPONENT_RECEIPT_OR_REPLAY_INVALID';END IF;
  v_new:=v_new+(result->>'new_source_facts')::integer;v_record_count:=v_record_count+1;
  INSERT INTO nbn_component_record_receipts VALUES(item->>'component',result);
 END LOOP;
 IF v_record_count<>3 THEN RAISE EXCEPTION 'NBN_THREE_COMPONENT_RECORDS_REQUIRED';END IF;
 SELECT encode(sha256(convert_to(coalesce(jsonb_agg(to_jsonb(q) ORDER BY q.ticker)::text,'[]'),'UTF8')),'hex') INTO v_after
 FROM population_top3000_202608_202609_v1.company_candidate_v1 q WHERE q.ticker IN('NBN','LPX');
 IF v_after IS DISTINCT FROM v_candidate_sha THEN RAISE EXCEPTION 'NBN_RAW_CANDIDATES_CHANGED';END IF;
 SELECT encode(sha256(convert_to(coalesce(jsonb_agg(to_jsonb(q) ORDER BY q.group_key)::text,'[]'),'UTF8')),'hex') INTO v_after
 FROM population_top3000_202608_202609_v1.company_group_v1 q WHERE q.group_key IN('CIK:0000060519');
 IF v_after IS DISTINCT FROM v_group_sha THEN RAISE EXCEPTION 'NBN_ORIGINAL_GROUP_CHANGED';END IF;
 SELECT encode(sha256(convert_to(coalesce(jsonb_agg(to_jsonb(q))::text,'[]'),'UTF8')),'hex') INTO v_after
 FROM population_top3000_202608_202609_v1.pipeline_universe q;
 IF v_after IS DISTINCT FROM v_universe_sha THEN RAISE EXCEPTION 'NBN_UNIVERSE_CHANGED';END IF;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.acquisition_control
  WHERE singleton AND provider_requests=v_provider AND http_invocations=v_http AND stored_bytes=v_stored AND reserved_bytes=v_reserved)
 THEN RAISE EXCEPTION 'NBN_NO_FETCH_OR_SOURCE_RECOPY_COUNTERS_CHANGED';END IF;
 INSERT INTO nbn_component_record_receipts VALUES('SUMMARY',jsonb_build_object('result','PASS_NBN_ACTUAL_PRIMARY_FACT_COMPONENT_RECORDS_AND_REPLAY',
  'recorded_components',v_record_count,'new_records',v_new,'manifest_sha256','bac0c242caf9158e0b4a8a6a3dacdc9f61826d6eb8b1c8af4a7c1f87cd28888e',
  'source_representation','ROOT_FACT_EXTRACTION_WITH_PRIMARY_SOURCE_LOCATION_NOT_ORIGINAL_DOCUMENT',
  'source_provider_calls',0,'candidate_and_group_unchanged',true,'replacement_cik_asserted',false,
  'aug3_shares_proven',false,'company_cap_verified',false,'selected_membership_released',false));
END $record$;
SELECT component,receipt FROM nbn_component_record_receipts ORDER BY component;
COMMIT;
