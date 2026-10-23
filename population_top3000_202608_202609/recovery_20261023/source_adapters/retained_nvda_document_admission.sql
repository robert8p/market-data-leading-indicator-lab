-- Reuse an immutable previously collected Massive filing body. No old-mission changes or HTTP.
BEGIN;
SET LOCAL lock_timeout='3s';
SELECT pg_advisory_xact_lock(2026083000261022);
DO $import$
DECLARE d record; c record; e record; v_bytes bigint; v_source_id constant text:='company:sec-document:0001045810-26-000052';
 v_sha constant text:='1b5de37b973da4a3f1cd31a09aa455c01c519ea7cc409c73de2250ad156f99e4';v_new integer;
BEGIN
 SELECT * INTO c FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton FOR UPDATE;
 IF c.owner_id IS DISTINCT FROM 'top3000-recovery-20261023-root-4b1314bcb5c2' OR c.owner_fence<>2 OR c.stop_generation<>0
  OR NOT c.enabled OR c.stopped OR c.hold_reason IS NOT NULL OR c.admitted_until<=clock_timestamp() THEN RAISE EXCEPTION 'OWNER_NOT_ADMITTED';END IF;
 SELECT * INTO d FROM research_eq10_fundamental_pit_20261020.financial_documents_v1
 WHERE accession_number='0001045810-26-000052' AND file_id='nvda-20260426.htm' FOR SHARE;
 v_bytes:=octet_length(d.raw_document);
 IF NOT FOUND OR d.provider IS DISTINCT FROM 'Massive' OR d.document_sha256 IS DISTINCT FROM v_sha
  OR encode(sha256(convert_to(d.raw_document,'UTF8')),'hex') IS DISTINCT FROM v_sha OR v_bytes IS DISTINCT FROM 1167053::bigint
  OR d.route IS DISTINCT FROM '/v1/reference/sec/filings/0001045810-26-000052/files/nvda-20260426.htm'
  OR d.provider_metadata->>'http_status' IS DISTINCT FROM '200'
  OR d.provider_metadata->>'raw_bytes_sha256' IS DISTINCT FROM v_sha THEN RAISE EXCEPTION 'RETAINED_DOCUMENT_IDENTITY_OR_HASH_MISMATCH';END IF;
 SELECT * INTO e FROM population_top3000_202608_202609_v1.filing_acceptance_reconciliation_v1
 WHERE accession='0001045810-26-000052';
 IF NOT FOUND OR e.accepted_at IS DISTINCT FROM '2026-05-20T20:35:52Z'::timestamptz
  OR e.evidence->>'primary_document' IS DISTINCT FROM 'nvda-20260426.htm'
  OR e.evidence->>'official_index_document_bytes' IS DISTINCT FROM '1167053' THEN RAISE EXCEPTION 'OFFICIAL_INDEX_IDENTITY_EVIDENCE_REQUIRED';END IF;
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.source_document WHERE source_id=v_source_id
  AND (source_sha256 IS DISTINCT FROM v_sha OR content_text IS DISTINCT FROM d.raw_document
   OR source_provider IS DISTINCT FROM 'Massive' OR source_uri IS DISTINCT FROM 'https://api.massive.com'||d.route)) THEN RAISE EXCEPTION 'IMMUTABLE_RETAINED_DOCUMENT_CONFLICT';END IF;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.source_document WHERE source_id=v_source_id)
  AND c.stored_bytes+c.reserved_bytes+v_bytes>c.max_source_bytes THEN RAISE EXCEPTION 'SOURCE_STORAGE_LIMIT';END IF;
 INSERT INTO population_top3000_202608_202609_v1.source_document
  (source_id,source_sha256,source_uri,source_provider,retrieved_at,source_published_at,historical_received_at,content_text,content_external_uri,content_bytes,evidence,license_policy)
 VALUES(v_source_id,v_sha,'https://api.massive.com'||d.route,'Massive',d.collected_at,e.accepted_at,NULL,d.raw_document,NULL,v_bytes,
  jsonb_build_object('transfer_method','RETAINED_DATABASE_DOCUMENT_NO_NETWORK','retained_relation','research_eq10_fundamental_pit_20261020.financial_documents_v1',
   'accession','0001045810-26-000052','file_id',d.file_id,'provider_receipt',d.provider_metadata,
   'canonical_sec_url','https://www.sec.gov/Archives/edgar/data/1045810/000104581026000052/nvda-20260426.htm',
   'official_index_evidence_sha256',e.evidence_sha256,'official_index_size_and_identity_match',true,
   'official_sec_body_hash_independently_compared',false,'source_acquisition_provider','Massive',
   'original_historical_receipt_proven',false,'provider_requests',0,'old_mission_modified',false),
  'RETAINED_LICENSED_PROVIDER_COPY_OF_SEC_FILING_NO_NEW_EXTERNAL_ACCESS') ON CONFLICT(source_id) DO NOTHING;
 GET DIAGNOSTICS v_new=ROW_COUNT;
 UPDATE population_top3000_202608_202609_v1.acquisition_control SET stored_bytes=stored_bytes+v_new*v_bytes,updated_at=clock_timestamp() WHERE singleton;
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details)
 VALUES('RETAINED_COMPANY_FILING_BODY_IMPORTED',jsonb_build_object('source_id',v_source_id,'source_sha256',v_sha,'source_provider','Massive',
  'source_bytes',v_bytes,'new_source_rows',v_new,'additional_stored_bytes',v_new*v_bytes,'no_provider_request',true,'old_mission_modified',false,
  'historical_provider_receipt_proven',false));
END $import$;

DO $admit_document$
DECLARE v_hashes jsonb;v_evidence jsonb;v_task uuid;
BEGIN
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE status='LEASED') THEN RAISE EXCEPTION 'ACTIVE_TASK_RECONCILIATION_REQUIRED';END IF;
 SELECT jsonb_object_agg(v.role,encode(sha256(convert_to(pg_get_functiondef(v.fn::regprocedure),'UTF8')),'hex')) INTO v_hashes
 FROM (VALUES ('ACQUIRE','population_top3000_202608_202609_v1.company_retained_acquire_v1(uuid,text)'),
 ('NORMALIZE','population_top3000_202608_202609_v1.company_reconcile_v1(uuid,text)'),
 ('COVER_EXTRACTOR','population_top3000_202608_202609_v1.company_extract_cover_v1(text,text)'))v(role,fn);
 v_evidence:=jsonb_build_object('result','PASS','test','retained_nvda_document_cover_fix_rollback_test.sql','test_file_sha256','1be487bf2e4d756c08ee26bdce96f4f9ef708a7e61ee86836044177865edd058',
 'source_sha256','1b5de37b973da4a3f1cd31a09aa455c01c519ea7cc409c73de2250ad156f99e4','owner_fence',2,
 'function_sha256',v_hashes,'actual_dispatches',2,'reported_cover_shares',24200000000,'measurement_date','2026-05-15',
 'cover_precision','Source reports24.2b with decimals=-8; not an exact3August share count','no_provider_request',true,'raw_source_unchanged',true);
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES('RETAINED_DOCUMENT_AND_COVER_PARSER_VERIFIED',v_evidence);
 UPDATE population_top3000_202608_202609_v1.pipeline_adapter SET function_sha256=v_hashes->>'NORMALIZE',
 reviewed_test_evidence=reviewed_test_evidence||jsonb_build_object('cover_parser_recovery',v_evidence),producer_revision='company-normalizer-cover-root-v2'
 WHERE family_id='REF01' AND stage='NORMALIZE' AND function_oid='population_top3000_202608_202609_v1.company_reconcile_v1(uuid,text)'::regprocedure;
 IF NOT FOUND THEN RAISE EXCEPTION 'NORMALIZER_BINDING_CHANGED';END IF;
 v_task:=population_top3000_202608_202609_v1.pipeline_enqueue_v1('REF01','ACQUIRE','document:0001045810-26-000052',
 '{"kind":"SEC_DOCUMENT","url":"https://api.massive.com/v1/reference/sec/filings/0001045810-26-000052/files/nvda-20260426.htm","source_id":"company:sec-document:0001045810-26-000052","group_key":"CIK:0001045810","canonical_sec_url":"https://www.sec.gov/Archives/edgar/data/1045810/000104581026000052/nvda-20260426.htm","source_provider":"Massive","retained_only":true,"provisional":true}',
 '{"source_sha256":"1b5de37b973da4a3f1cd31a09aa455c01c519ea7cc409c73de2250ad156f99e4","accession":"0001045810-26-000052","source_provider":"Massive","admission_version":"retained-document-v1"}'::jsonb||jsonb_build_object('cover_extractor_sha256',v_hashes->>'COVER_EXTRACTOR'));
 UPDATE population_top3000_202608_202609_v1.pipeline_control SET budget_evidence=jsonb_set(budget_evidence,'{allowed_work}',
 coalesce(budget_evidence->'allowed_work','[]'::jsonb)||jsonb_build_array('One pinned retained Massive copy of NVDA10Q; exact source provider and official SEC identity retained; corrected XPath cover extraction; no new HTTP')),
 next_wakeup_at=clock_timestamp() WHERE singleton;
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,task_id,details)
 VALUES('RETAINED_DOCUMENT_VERSION_ADMITTED',v_task,jsonb_build_object('source_id','company:sec-document:0001045810-26-000052',
 'source_provider','Massive','original_blocked_task_preserved','a6f7b8b5-2c54-46fa-a98f-9eda6277c953',
 'automatic_source_fetch',false,'source_sha256','1b5de37b973da4a3f1cd31a09aa455c01c519ea7cc409c73de2250ad156f99e4',
 'test_evidence',v_evidence));
 PERFORM population_top3000_202608_202609_v1.pipeline_wake_v1('top3000-recovery-20261023-root-4b1314bcb5c2',2,'TESTED_RETAINED_NVDA_FILING_BODY_AND_COVER');
END $admit_document$;
COMMIT;
SELECT task_id,stage,status,partition_key,input_versions FROM population_top3000_202608_202609_v1.pipeline_task WHERE family_id='REF01' AND partition_key='document:0001045810-26-000052' ORDER BY created_at;

