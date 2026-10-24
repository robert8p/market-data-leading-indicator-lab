-- Scoped ingestion of the already acquired original SEC CRD XBRL instance.
-- Root integrates this migration after independent review. No network access in SQL.
ALTER TABLE population_top3000_202608_202609_v1.sec_source_manifest_v1
  DROP CONSTRAINT sec_source_manifest_v1_source_kind_check;
ALTER TABLE population_top3000_202608_202609_v1.sec_source_manifest_v1
  ADD CONSTRAINT sec_source_manifest_v1_source_kind_check CHECK
  (source_kind IN ('QUARTER_INDEX','SUBMISSIONS','COMPANYFACTS','INLINE_XBRL','EXTRACTED_XBRL'));

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.financial_sec_crd_import_v1(
 p_owner text,p_fence bigint,p_stop_generation bigint,p_part integer,p_text text,p_part_sha text)
RETURNS jsonb LANGUAGE plpgsql SET search_path TO 'pg_catalog' AS $function$
DECLARE
 c population_top3000_202608_202609_v1.acquisition_control%ROWTYPE;
 s population_top3000_202608_202609_v1.sec_source_manifest_v1%ROWTYPE;
 old population_top3000_202608_202609_v1.sec_source_chunk_v1%ROWTYPE;
 sid constant text:='financial-sec-original-crd-20260630:7a9176789eaf515caa6a53c8c803645744b3d0d2d8c9c1f08a0a254bf872630c';
 h constant text:='7a9176789eaf515caa6a53c8c803645744b3d0d2d8c9c1f08a0a254bf872630c';
 uri constant text:='https://www.sec.gov/Archives/edgar/data/25475/000119312526330643/crda-20260630_htm.xml';
 bytes constant integer:=3875138;
 parts constant integer:=15;
 observed_at constant timestamptz:='2026-10-24T03:33:20.166679Z';
 published_at constant timestamptz:='2026-08-03T20:24:33Z';
 body text;b integer;n integer;k text;m jsonb;mh text;versions jsonb;task uuid;ev jsonb;fresh integer:=0;
BEGIN
 SELECT * INTO c FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton FOR UPDATE;
 IF NOT FOUND OR c.owner_id IS DISTINCT FROM p_owner OR c.owner_fence IS DISTINCT FROM p_fence
  OR c.stop_generation IS DISTINCT FROM p_stop_generation OR c.stopped OR NOT c.enabled
  OR c.hold_reason IS NOT NULL OR c.admitted_until<=clock_timestamp()
  OR c.scope_sha256<>'8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd'
  OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_control p WHERE p.singleton
   AND p.enabled AND p.expected_owner_id=c.owner_id AND p.expected_owner_fence=c.owner_fence
   AND p.expected_stop_generation=c.stop_generation AND p.scope_sha256=c.scope_sha256)
 THEN RAISE EXCEPTION 'SEC_CRD_OWNER_STOP_OR_ADMISSION_CHANGED';END IF;
 PERFORM population_top3000_202608_202609_v1.financial_shared_resource_guard_v1();
 b:=octet_length(p_text);
 IF p_part IS NULL OR p_part NOT BETWEEN 0 AND parts-1 OR b IS NULL OR b NOT BETWEEN 1 AND 262144
  OR p_part_sha IS DISTINCT FROM encode(sha256(convert_to(p_text,'UTF8')),'hex')
 THEN RAISE EXCEPTION 'SEC_CRD_PART_INVALID';END IF;
 ev:=jsonb_build_object('provider','SEC','representation','EXACT_UTF8_ORIGINAL_EXTRACTED_XBRL',
  'http_status_observed',200,'content_type_observed','text/xml','elapsed_seconds',3.803171522999037,
  'source_bytes',bytes,'source_sha256',h,'local_file_written_at',observed_at,
  'retrieval_clock_basis','LOCAL_FILE_WRITE_TIME_AFTER_SUCCESSFUL_FETCH_NOT_ORIGINAL_HISTORICAL_RECEIPT',
  'source_index_url','https://www.sec.gov/Archives/edgar/data/25475/000119312526330643/0001193125-26-330643-index.htm',
  'acceptance_local_text','2026-08-03 16:24:33','acceptance_timezone','America/New_York',
  'after_ranking_close',true,'candidate_publication_at',published_at,'publication_time_independent_review_pending',true,'original_historical_receipt_proven',false,'company_population_verified',false,
  'prior_massive_html_unknown_outcome_preserved',true,'massive_xml_connector_content_type_failure_preserved',true,
  'acquisition_was_outside_database_worker',true,'external_request_observed_count',1,
  'actual_cost_invoice_reconciled',false,'import_revision','financial-sec-crd-import-v1');
 SELECT * INTO s FROM population_top3000_202608_202609_v1.sec_source_manifest_v1 WHERE source_id=sid FOR UPDATE;
 IF NOT FOUND THEN
  IF c.stored_bytes+c.reserved_bytes+2*bytes>c.max_source_bytes OR c.provider_requests+1>c.max_provider_requests
  THEN RAISE EXCEPTION 'SEC_CRD_FINITE_SOURCE_OR_REQUEST_ALLOWANCE_EXCEEDED';END IF;
  INSERT INTO population_top3000_202608_202609_v1.sec_source_manifest_v1
   (source_id,source_kind,source_uri,source_sha256,source_bytes,received_at,evidence,expected_parts,raw_state)
  VALUES(sid,'EXTRACTED_XBRL',uri,h,bytes,observed_at,ev,parts,'HASH_CATALOGUED');
  UPDATE population_top3000_202608_202609_v1.acquisition_control SET provider_requests=provider_requests+1,updated_at=clock_timestamp() WHERE singleton;
 ELSE
  IF s.source_kind IS DISTINCT FROM 'EXTRACTED_XBRL' OR s.source_uri IS DISTINCT FROM uri OR s.source_sha256 IS DISTINCT FROM h OR s.source_bytes IS DISTINCT FROM bytes
   OR s.expected_parts IS DISTINCT FROM parts OR s.received_at IS DISTINCT FROM observed_at
  THEN RAISE EXCEPTION 'SEC_CRD_SOURCE_IMMUTABLE_CONFLICT';END IF;
 END IF;
 SELECT * INTO old FROM population_top3000_202608_202609_v1.sec_source_chunk_v1 WHERE source_id=sid AND part_number=p_part;
 IF FOUND THEN
  IF old.chunk_sha256<>p_part_sha OR old.raw_text<>p_text OR old.chunk_bytes<>b
  THEN RAISE EXCEPTION 'SEC_CRD_CHUNK_IMMUTABLE_CONFLICT';END IF;
 ELSE
  IF c.stored_bytes+c.reserved_bytes+b>c.max_source_bytes THEN RAISE EXCEPTION 'SEC_CRD_SOURCE_BYTE_BOUND';END IF;
  INSERT INTO population_top3000_202608_202609_v1.sec_source_chunk_v1
   (source_id,part_number,first_source_line,raw_text,chunk_sha256,chunk_bytes) VALUES(sid,p_part,NULL,p_text,p_part_sha,b);
  UPDATE population_top3000_202608_202609_v1.acquisition_control SET stored_bytes=stored_bytes+b,
   last_useful_progress_at=clock_timestamp(),updated_at=clock_timestamp() WHERE singleton;
  fresh:=1;
 END IF;
 SELECT count(*) INTO n FROM population_top3000_202608_202609_v1.sec_source_chunk_v1 WHERE source_id=sid;
 IF n<>parts THEN
  UPDATE population_top3000_202608_202609_v1.sec_source_manifest_v1 SET raw_state='PARTIAL' WHERE source_id=sid;
  INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details)
   VALUES('FINANCIAL_SEC_CHUNK_COMMITTED',jsonb_build_object('source_id',sid,'part',p_part,'new_part',fresh,'received_parts',n,'expected_parts',parts,'owner_fence',p_fence,'stop_generation',p_stop_generation));
  RETURN jsonb_build_object('state','PARTIAL','source_id',sid,'received_parts',n,'expected_parts',parts,'new_bytes',fresh*b);
 END IF;
 SELECT string_agg(raw_text,'' ORDER BY part_number) INTO body FROM population_top3000_202608_202609_v1.sec_source_chunk_v1 WHERE source_id=sid;
 IF octet_length(body)<>bytes OR encode(sha256(convert_to(body,'UTF8')),'hex')<>h
 THEN RAISE EXCEPTION 'SEC_CRD_ASSEMBLED_HASH_MISMATCH';END IF;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.source_document WHERE source_id=sid) THEN
  IF (SELECT stored_bytes+reserved_bytes+bytes>max_source_bytes FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton)
  THEN RAISE EXCEPTION 'SEC_CRD_FINAL_COPY_BYTE_BOUND';END IF;
  INSERT INTO population_top3000_202608_202609_v1.source_document
   (source_id,source_sha256,source_uri,source_provider,retrieved_at,source_published_at,historical_received_at,content_text,content_bytes,evidence,license_policy)
  VALUES(sid,h,uri,'SEC',observed_at,NULL,NULL,body,bytes,ev,'PUBLIC_SEC_FILING_INTERNAL_RESEARCH_WITH_SOURCE_ATTRIBUTION');
  UPDATE population_top3000_202608_202609_v1.acquisition_control SET stored_bytes=stored_bytes+bytes,updated_at=clock_timestamp() WHERE singleton;
 END IF;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.source_document z WHERE z.source_id=sid
  AND z.source_provider='SEC' AND z.source_uri=uri AND z.source_sha256=h AND z.content_bytes=bytes
  AND z.content_text=body AND z.historical_received_at IS NULL AND z.source_published_at IS NULL)
 THEN RAISE EXCEPTION 'SEC_CRD_FINAL_SOURCE_REPLAY_CONFLICT';END IF;
 UPDATE population_top3000_202608_202609_v1.sec_source_manifest_v1 SET raw_state='COMPLETE_VERIFIED',raw_verified_at=coalesce(raw_verified_at,clock_timestamp()) WHERE source_id=sid;
 SELECT jsonb_object_agg(f.family_id||'_contract_sha256',f.contract_sha256) INTO versions
 FROM population_top3000_202608_202609_v1.catalog_family_contract f WHERE f.scope_id='corrected_company_scope_8e6ca1178f03822c' AND f.family_id IN('F02','F03','F04');
 versions:=versions||jsonb_build_object('scope_sha256',c.scope_sha256,'financial_shared_revision','financial-shared-document-v1',
  'financial_shared_bundle_sha256',population_top3000_202608_202609_v1.financial_shared_bundle_v1());
 k:='financial-document-v1:'||encode(sha256(convert_to(jsonb_build_array('0001193125-26-330643','crda-20260630_htm.xml',h)::text,'UTF8')),'hex');
 m:=jsonb_build_object('document_key',k,'scope_sha256',c.scope_sha256,'source_id',sid,'source_kind','MISSION_TEXT',
  'body_sha256',h,'container_sha256',h,'body_bytes',bytes,'source_uri',uri,'source_provider','SEC',
  'accession_number','0001193125-26-330643','file_id','crda-20260630_htm.xml','form_type','10-Q',
  'filing_ciks',jsonb_build_array('0000025475'),'filing_date','2026-08-03','report_date','2026-06-30','time_role','TARGET',
  'source_anchors',jsonb_build_array(jsonb_build_object('source_id',sid,'source_sha256',h)),
  'source_metadata_basis','SEC_INDEX_AND_EXACT_XBRL_IDENTITY','company_population_verified',false,
  'original_historical_receipt_proven',false,'publication_clock',jsonb_build_object('source_acceptance_local_text','20260803162433',
   'filing_date','2026-08-03','clock_basis','SEC_INDEX_AMERICA_NEW_YORK_PENDING_INDEPENDENT_REVIEW','effective_available_at',NULL,'candidate_publication_at',published_at,
   'precise_publication_verified',false,'independent_publication_review_required',true,'after_ranking_close',true,
   'original_historical_receipt_proven',false,'earliest_earnings_publication_proven',false));
 mh:=encode(sha256(convert_to(m::text,'UTF8')),'hex');
 INSERT INTO population_top3000_202608_202609_v1.financial_shared_admission_v1(document_key,manifest,manifest_sha256,input_versions)
  VALUES(k,m,mh,versions) ON CONFLICT DO NOTHING;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.financial_shared_admission_v1 a WHERE a.document_key=k AND a.manifest=m AND a.manifest_sha256=mh AND a.input_versions=versions)
 THEN RAISE EXCEPTION 'SEC_CRD_FINANCIAL_ADMISSION_REPLAY_CONFLICT';END IF;
 task:=population_top3000_202608_202609_v1.pipeline_enqueue_v1('F03','ACQUIRE',k,
  jsonb_build_object('kind','SHARED_FINANCIAL_DOCUMENT_V1','document_key',k,'document_manifest_sha256',mh),versions,NULL);
 PERFORM population_top3000_202608_202609_v1.financial_shared_body_v1(k);
 PERFORM population_top3000_202608_202609_v1.financial_shared_resource_guard_v1();
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(task_id,event,details)
  VALUES(task,'FINANCIAL_SEC_ORIGINAL_IMPORTED_AND_DISPATCHED',jsonb_build_object('source_id',sid,'source_sha256',h,
   'document_key',k,'bytes',bytes,'source_provider','SEC','original_historical_receipt_proven',false,'after_ranking_close',true,
   'owner_fence',p_fence,'stop_generation',p_stop_generation));
 RETURN jsonb_build_object('state','SOURCE_COMPLETE_AND_SUCCESSOR_QUEUED','source_id',sid,'source_sha256',h,'document_key',k,'task_id',task,'bytes',bytes);
END $function$;
REVOKE ALL ON FUNCTION population_top3000_202608_202609_v1.financial_sec_crd_import_v1(text,bigint,bigint,integer,text,text) FROM PUBLIC;
