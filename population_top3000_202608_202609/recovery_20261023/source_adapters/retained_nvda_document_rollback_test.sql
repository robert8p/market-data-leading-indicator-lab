-- Reuse an immutable previously collected Massive filing body. No old-mission changes or HTTP.
BEGIN;
SET LOCAL lock_timeout='3s';
SELECT pg_advisory_xact_lock(2026083000261022);
SELECT cron.alter_job(596,active=>false);
SET LOCAL statement_timeout='40s';
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


DO $test$
DECLARE a uuid;n uuid;r jsonb;p bigint;h bigint;
BEGIN
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE status='LEASED')
  OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_provider_lease WHERE released_at IS NULL)
  THEN RAISE EXCEPTION 'LIVE_LEASES_MUST_BE_RECONCILED';END IF;
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_adapter WHERE enabled AND resource_class<>'DATABASE')
  THEN RAISE EXCEPTION 'DATABASE_ONLY_ADAPTERS_REQUIRED';END IF;
 SELECT provider_requests,http_invocations INTO p,h FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton;
 UPDATE population_top3000_202608_202609_v1.pipeline_task SET not_before='infinity',blocker_until=NULL WHERE status IN('READY','BLOCKED');
 UPDATE population_top3000_202608_202609_v1.pipeline_control SET enabled=true,next_wakeup_at=clock_timestamp() WHERE singleton;
 a:=population_top3000_202608_202609_v1.pipeline_enqueue_v1('REF01','ACQUIRE','rollback-retained-nvda-document-'||gen_random_uuid(),
  '{"kind":"SEC_DOCUMENT","url":"https://api.massive.com/v1/reference/sec/filings/0001045810-26-000052/files/nvda-20260426.htm","source_id":"company:sec-document:0001045810-26-000052","group_key":"CIK:0001045810","canonical_sec_url":"https://www.sec.gov/Archives/edgar/data/1045810/000104581026000052/nvda-20260426.htm","source_provider":"Massive","retained_only":true,"provisional":true}',
  '{"source_sha256":"1b5de37b973da4a3f1cd31a09aa455c01c519ea7cc409c73de2250ad156f99e4","accession":"0001045810-26-000052","source_provider":"Massive","admission_version":"retained-document-v1"}');
 r:=population_top3000_202608_202609_v1.pipeline_tick_v1();
 IF r#>>'{result,state}' IS DISTINCT FROM 'COMPLETE' OR (r->>'task_id')::uuid IS DISTINCT FROM a THEN RAISE EXCEPTION 'RETAINED_DOCUMENT_ACQUIRE: %',r;END IF;
 n:=(r#>>'{result,successor_task_id}')::uuid;
 r:=population_top3000_202608_202609_v1.pipeline_tick_v1();
 IF r#>>'{result,state}' IS DISTINCT FROM 'COMPLETE' OR (r->>'task_id')::uuid IS DISTINCT FROM n THEN RAISE EXCEPTION 'REAL_SEC_DOCUMENT_NORMALIZE: %',r;END IF;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.company_cover_fact_v1
  WHERE source_id='company:sec-document:0001045810-26-000052' AND source_sha256='1b5de37b973da4a3f1cd31a09aa455c01c519ea7cc409c73de2250ad156f99e4'
   AND entity_cik='0001045810' AND extraction_state='PARSED_COVER_INSTANT_CANDIDATE' AND value>0 AND measurement_date<='2026-08-03')
  THEN RAISE EXCEPTION 'SOURCE_BOUND_COVER_FACT_NOT_EXTRACTED';END IF;
 IF (SELECT provider_requests FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton)<>p
  OR (SELECT http_invocations FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton)<>h
  OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_universe WHERE status='FROZEN')
  OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.source_document WHERE source_id='company:sec-document:0001045810-26-000052'
   AND (source_provider<>'Massive' OR historical_received_at IS NOT NULL)) THEN RAISE EXCEPTION 'SOURCE_SEMANTICS_OR_NO_NETWORK_ASSERTION';END IF;
END $test$;
SELECT 'PASS_RETAINED_MASSIVE_FILING_ACQUIRE_NORMALIZE_COVER_EXTRACTION' result,
 jsonb_agg(jsonb_build_object('source_id',source_id,'concept',concept,'value',value,'measurement_date',measurement_date,'entity_cik',entity_cik,'state',extraction_state)) facts
FROM population_top3000_202608_202609_v1.company_cover_fact_v1 WHERE source_id='company:sec-document:0001045810-26-000052';
ROLLBACK;
