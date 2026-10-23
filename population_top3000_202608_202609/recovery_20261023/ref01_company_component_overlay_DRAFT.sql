-- Root-reviewed source-fact overlay only. No fetch, candidate/group mutation,
-- exception closure, canonical-company assignment, cap/rank or universe release.
-- DRAFT: root must review/install and exercise an actual-source rollback before use.
BEGIN;
SET LOCAL lock_timeout='2s';
SELECT pg_advisory_xact_lock(2026083000261022);
DO $guard$
DECLARE c population_top3000_202608_202609_v1.acquisition_control%ROWTYPE;
 p population_top3000_202608_202609_v1.pipeline_control%ROWTYPE;
BEGIN
 SELECT * INTO STRICT c FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton FOR UPDATE;
 SELECT * INTO STRICT p FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton FOR UPDATE;
 IF c.owner_id IS DISTINCT FROM 'top3000-recovery-20261023-root-4b1314bcb5c2'
  OR c.owner_fence IS DISTINCT FROM 2 OR c.stop_generation IS DISTINCT FROM 0
  OR p.expected_owner_id IS DISTINCT FROM c.owner_id OR p.expected_owner_fence IS DISTINCT FROM c.owner_fence
  OR p.expected_stop_generation IS DISTINCT FROM c.stop_generation
  OR c.scope_sha256 IS DISTINCT FROM '8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd'
  OR p.scope_sha256 IS DISTINCT FROM c.scope_sha256
  OR NOT c.enabled OR c.stopped OR c.hold_reason IS NOT NULL OR c.admitted_until IS NULL OR c.admitted_until<=clock_timestamp()
  OR NOT p.enabled OR NOT p.shared_scheduler_verified OR p.deadline IS NULL OR p.deadline<=clock_timestamp()
 THEN RAISE EXCEPTION 'COMPANY_COMPONENT_OWNER_SCOPE_OR_ADMISSION_MISMATCH'; END IF;
END $guard$;

CREATE TABLE population_top3000_202608_202609_v1.company_component_source_evidence_v1 (
 evidence_sha256 text PRIMARY KEY CHECK(evidence_sha256 ~ '^[0-9a-f]{64}$'),
 case_id text NOT NULL,
 tickers text[] NOT NULL CHECK(cardinality(tickers) BETWEEN 1 AND 5),
 component text NOT NULL CHECK(component IN('FILING_REGISTRANT','ISSUER_SECURITY_ASSOCIATION',
   'REJECTED_SOURCE_ASSOCIATION','SECURITY_CLASS','SHARES_AT_SOURCE_DATE','CORPORATE_RELATIONSHIP','SOURCE_PUBLICATION')),
 source_id text NOT NULL REFERENCES population_top3000_202608_202609_v1.source_document(source_id),
 source_sha256 text NOT NULL CHECK(source_sha256 ~ '^[0-9a-f]{64}$'),
 record_json jsonb NOT NULL CHECK(jsonb_typeof(record_json)='object'),
 review_state text NOT NULL DEFAULT 'SOURCE_FACT_RECORDED_REVIEW_PENDING'
   CHECK(review_state='SOURCE_FACT_RECORDED_REVIEW_PENDING'),
 owner_fence bigint NOT NULL CHECK(owner_fence=2),
 stop_generation bigint NOT NULL CHECK(stop_generation=0),
 recorded_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 identity_release_allowed boolean NOT NULL DEFAULT false CHECK(NOT identity_release_allowed),
 strict_certified boolean NOT NULL DEFAULT false CHECK(NOT strict_certified),
 rank_day_shares_proven boolean NOT NULL DEFAULT false CHECK(NOT rank_day_shares_proven),
 company_cap_verified boolean NOT NULL DEFAULT false CHECK(NOT company_cap_verified),
 selected_company_member boolean NOT NULL DEFAULT false CHECK(NOT selected_company_member)
);
REVOKE ALL ON population_top3000_202608_202609_v1.company_component_source_evidence_v1 FROM PUBLIC,anon,authenticated,service_role;

CREATE FUNCTION population_top3000_202608_202609_v1.company_record_component_source_v1(p_record jsonb)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path='pg_catalog'
AS $function$
DECLARE
 v_c population_top3000_202608_202609_v1.acquisition_control%ROWTYPE;
 v_p population_top3000_202608_202609_v1.pipeline_control%ROWTYPE;
 v_source population_top3000_202608_202609_v1.source_document%ROWTYPE;
 v_candidate population_top3000_202608_202609_v1.company_candidate_v1%ROWTYPE;
 v_allowed text[];v_tickers text[];v_ref_tickers text[];v_ref jsonb;v_path text[];
 v_literal text;v_start integer;v_sha text;v_new integer;v_payload jsonb;
BEGIN
 PERFORM pg_advisory_xact_lock(2026083000261022);
 SELECT * INTO STRICT v_c FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton FOR UPDATE;
 SELECT * INTO STRICT v_p FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton FOR UPDATE;
 IF v_c.owner_id IS DISTINCT FROM 'top3000-recovery-20261023-root-4b1314bcb5c2'
  OR v_c.owner_fence IS DISTINCT FROM 2 OR v_c.stop_generation IS DISTINCT FROM 0
  OR v_p.expected_owner_id IS DISTINCT FROM v_c.owner_id OR v_p.expected_owner_fence IS DISTINCT FROM v_c.owner_fence
  OR v_p.expected_stop_generation IS DISTINCT FROM v_c.stop_generation
  OR v_c.scope_sha256 IS DISTINCT FROM '8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd'
  OR v_p.scope_sha256 IS DISTINCT FROM v_c.scope_sha256
  OR NOT v_c.enabled OR v_c.stopped OR v_c.hold_reason IS NOT NULL OR v_c.admitted_until IS NULL OR v_c.admitted_until<=clock_timestamp()
  OR NOT v_p.enabled OR NOT v_p.shared_scheduler_verified OR v_p.deadline IS NULL OR v_p.deadline<=clock_timestamp()
 THEN RAISE EXCEPTION 'COMPANY_COMPONENT_OWNER_SCOPE_OR_ADMISSION_MISMATCH';END IF;
 IF jsonb_typeof(p_record) IS DISTINCT FROM 'object' OR octet_length(p_record::text)>65536
  OR p_record-ARRAY['case_id','tickers','component','source_id','source_sha256','source_boundary','source_anchor',
    'observed_fact','source_state_date','contrary_evidence','unresolved_requirements','expected_candidate_refs']::text[]<>'{}'::jsonb
  OR jsonb_typeof(p_record->'tickers') IS DISTINCT FROM 'array'
  OR jsonb_typeof(p_record->'expected_candidate_refs') IS DISTINCT FROM 'array'
  OR jsonb_typeof(p_record->'unresolved_requirements') IS DISTINCT FROM 'array'
  OR jsonb_array_length(p_record->'unresolved_requirements')<1
  OR NOT (p_record ? 'observed_fact') OR NOT (p_record ? 'contrary_evidence')
  OR coalesce(p_record->>'source_sha256','') !~ '^[0-9a-f]{64}$'
  OR coalesce(p_record->>'source_boundary','') NOT IN('PROVIDER_METADATA','PROVIDER_ORIGINAL_DOCUMENT_COPY',
     'PRIMARY_ISSUER_OR_REGULATOR_COPY','CONNECTOR_RECEIPT','EXTRACTED_TEXT_WITH_ORIGINAL_FILE_REFERENCE')
 THEN RAISE EXCEPTION 'COMPANY_COMPONENT_RECORD_SHAPE_INVALID';END IF;
 v_allowed:=CASE p_record->>'case_id'
  WHEN 'CRAWFORD_CUTOFF_CLASS_VALUATION' THEN ARRAY['CRD.A','CRD.B']
  WHEN 'COMMON_REIT_VENDOR_FUND_EXCLUSION' THEN ARRAY['DHC','DHCNI','DHCNL','ILPT','NXDT']
  WHEN 'LIBERTY_REORGANIZATION_CLASSES' THEN ARRAY['FWONA','FWONK','LLYVA','LLYVK']
  WHEN 'NBN_LPX_WRONG_ISSUER_CIK' THEN ARRAY['NBN','LPX']
  ELSE NULL END;
 SELECT array_agg(e.value ORDER BY e.value) INTO v_tickers FROM jsonb_array_elements_text(p_record->'tickers') e(value);
 SELECT array_agg(e.value->>'ticker' ORDER BY e.value->>'ticker') INTO v_ref_tickers FROM jsonb_array_elements(p_record->'expected_candidate_refs') e(value);
 IF v_allowed IS NULL OR cardinality(v_tickers) NOT BETWEEN 1 AND 5 OR NOT (v_tickers<@v_allowed)
  OR v_ref_tickers IS DISTINCT FROM v_tickers
  OR cardinality(v_tickers) IS DISTINCT FROM (SELECT count(DISTINCT t) FROM unnest(v_tickers) t)
 THEN RAISE EXCEPTION 'COMPANY_COMPONENT_CASE_OR_TICKER_SCOPE_INVALID';END IF;
 FOR v_ref IN SELECT e.value FROM jsonb_array_elements(p_record->'expected_candidate_refs') e(value) LOOP
  SELECT * INTO STRICT v_candidate FROM population_top3000_202608_202609_v1.company_candidate_v1 cand
   WHERE cand.ticker=v_ref->>'ticker' FOR SHARE;
  IF v_candidate.census_sha256 IS DISTINCT FROM v_ref->>'census_sha256'
   OR v_candidate.overview_sha256 IS DISTINCT FROM v_ref->>'overview_sha256'
   OR v_candidate.share_class_figi IS DISTINCT FROM v_ref->>'share_class_figi'
  THEN RAISE EXCEPTION 'COMPANY_COMPONENT_RAW_CANDIDATE_REVISION_CHANGED';END IF;
 END LOOP;
 SELECT * INTO STRICT v_source FROM population_top3000_202608_202609_v1.source_document src
  WHERE src.source_id=p_record->>'source_id' FOR SHARE;
 IF v_source.content_text IS NULL OR octet_length(v_source.content_text)>8388608
  OR v_source.source_sha256 IS DISTINCT FROM p_record->>'source_sha256'
  OR encode(sha256(convert_to(v_source.content_text,'UTF8')),'hex') IS DISTINCT FROM v_source.source_sha256
 THEN RAISE EXCEPTION 'COMPANY_COMPONENT_SOURCE_MISSING_CHANGED_OR_OVERSIZED';END IF;
 IF p_record#>>'{source_anchor,type}'='JSON_PATH' THEN
  IF jsonb_typeof(p_record#>'{source_anchor,path}') IS DISTINCT FROM 'array'
   OR jsonb_array_length(p_record#>'{source_anchor,path}') NOT BETWEEN 1 AND 12
  THEN RAISE EXCEPTION 'COMPANY_COMPONENT_JSON_ANCHOR_INVALID';END IF;
  SELECT array_agg(e.value ORDER BY e.ordinality) INTO v_path
   FROM jsonb_array_elements_text(p_record#>'{source_anchor,path}') WITH ORDINALITY e(value,ordinality);
  IF (v_source.content_text::jsonb#>v_path) IS DISTINCT FROM p_record->'observed_fact'
   OR (v_source.content_text::jsonb#>v_path) IS NULL
  THEN RAISE EXCEPTION 'COMPANY_COMPONENT_SOURCE_FACT_NOT_AT_JSON_ANCHOR';END IF;
 ELSIF p_record#>>'{source_anchor,type}'='TEXT_LITERAL' THEN
  IF p_record#>>'{source_anchor,start_character}' !~ '^[1-9][0-9]{0,7}$'
   OR jsonb_typeof(p_record->'observed_fact') IS DISTINCT FROM 'string'
  THEN RAISE EXCEPTION 'COMPANY_COMPONENT_LITERAL_ANCHOR_INVALID';END IF;
  v_start:=(p_record#>>'{source_anchor,start_character}')::integer;
  v_literal:=p_record->>'observed_fact';
  IF length(v_literal) NOT BETWEEN 1 AND 16384
   OR substring(v_source.content_text FROM v_start FOR length(v_literal)) IS DISTINCT FROM v_literal
  THEN RAISE EXCEPTION 'COMPANY_COMPONENT_SOURCE_LITERAL_NOT_AT_ANCHOR';END IF;
 ELSE RAISE EXCEPTION 'COMPANY_COMPONENT_ANCHOR_TYPE_INVALID'; END IF;
 -- The source facts are retained for review. No semantic identity or cap gate is bypassed.
 v_payload:=p_record||jsonb_build_object('scope_sha256',v_c.scope_sha256,'source_uri',v_source.source_uri,
  'source_provider',v_source.source_provider,'source_retrieved_at',v_source.retrieved_at,
  'owner_fence',v_c.owner_fence,'stop_generation',v_c.stop_generation,
  'temporal_mode','RETROSPECTIVE_SOURCE_EVIDENCE','review_state','SOURCE_FACT_RECORDED_REVIEW_PENDING',
  'identity_release_allowed',false,'strict_certified',false,'rank_day_shares_proven',false,
  'company_cap_verified',false,'selected_company_member',false);
 v_sha:=encode(sha256(convert_to(v_payload::text,'UTF8')),'hex');
 INSERT INTO population_top3000_202608_202609_v1.company_component_source_evidence_v1
  (evidence_sha256,case_id,tickers,component,source_id,source_sha256,record_json,owner_fence,stop_generation)
 VALUES(v_sha,p_record->>'case_id',v_tickers,p_record->>'component',v_source.source_id,v_source.source_sha256,
  v_payload,v_c.owner_fence,v_c.stop_generation) ON CONFLICT(evidence_sha256) DO NOTHING;
 GET DIAGNOSTICS v_new=ROW_COUNT;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.company_component_source_evidence_v1 existing_row
   WHERE existing_row.evidence_sha256=v_sha AND existing_row.record_json=v_payload)
 THEN RAISE EXCEPTION 'COMPANY_COMPONENT_IMMUTABLE_REPLAY_CONFLICT';END IF;
 IF v_new=1 THEN
  INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details)
  VALUES('COMPANY_COMPONENT_SOURCE_FACT_RECORDED',jsonb_build_object('evidence_sha256',v_sha,
   'case_id',p_record->>'case_id','component',p_record->>'component','tickers',v_tickers,
   'source_id',v_source.source_id,'source_sha256',v_source.source_sha256,'new_source_facts',1,
   'identity_released',false,'cap_verified',false,'universe_frozen',false,'provider_requests',0));
 END IF;
 RETURN jsonb_build_object('state','SOURCE_FACT_RECORDED_REVIEW_PENDING','evidence_sha256',v_sha,
  'new_source_facts',v_new,'identity_release_allowed',false,'rank_day_shares_proven',false,
  'company_cap_verified',false,'universe_frozen',false);
END $function$;
REVOKE ALL ON FUNCTION population_top3000_202608_202609_v1.company_record_component_source_v1(jsonb) FROM PUBLIC,anon,authenticated,service_role;
-- Owner/postgres only. No new external RPC permission or adapter registration.
COMMIT;

-- Required actual-source rollback before admission (root executes):
-- 1. Save group/candidate/exception/universe counts+hashes and provider counters.
-- 2. Record one actual new source fact with exact candidate refs and source anchor.
-- 3. Replay it: new_source_facts must be0, identical evidence_sha256, one fact row.
-- 4. Alter the literal/path, source SHA, candidate hash and case/ticker separately:
--    every negative must reject without a new fact or event.
-- 5. Set a transaction-local old fence or stop in rolled-back controls: recorder rejects.
-- 6. Assert original group/candidate/exception/universe and provider counters unchanged;
--    all release/strict/rank-share/cap/member flags remainfalse. ROLLBACK.
