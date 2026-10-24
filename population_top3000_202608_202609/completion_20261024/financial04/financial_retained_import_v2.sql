-- Root integration only; exact already acquired SEC text sources. No network or acceptance promotion.
CREATE TABLE IF NOT EXISTS population_top3000_202608_202609_v1.financial_retained_source_v2(
 source_id text PRIMARY KEY, manifest jsonb NOT NULL, manifest_sha256 text NOT NULL,
 resolved_source_id text, created_at timestamptz NOT NULL DEFAULT clock_timestamp(),completed_at timestamptz,
 CHECK(manifest_sha256=encode(sha256(convert_to(manifest::text,'UTF8')),'hex')));
CREATE TABLE IF NOT EXISTS population_top3000_202608_202609_v1.financial_retained_chunk_v2(
 source_id text REFERENCES population_top3000_202608_202609_v1.financial_retained_source_v2(source_id),
 part_number integer NOT NULL CHECK(part_number BETWEEN 0 AND 63),raw_text text NOT NULL,
 chunk_sha256 text NOT NULL,chunk_bytes integer NOT NULL CHECK(chunk_bytes BETWEEN 1 AND 262144),
 PRIMARY KEY(source_id,part_number),CHECK(chunk_bytes=octet_length(raw_text)),
 CHECK(chunk_sha256=encode(sha256(convert_to(raw_text,'UTF8')),'hex')));
ALTER TABLE population_top3000_202608_202609_v1.financial_retained_source_v2 ENABLE ROW LEVEL SECURITY;
ALTER TABLE population_top3000_202608_202609_v1.financial_retained_chunk_v2 ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON population_top3000_202608_202609_v1.financial_retained_source_v2,population_top3000_202608_202609_v1.financial_retained_chunk_v2 FROM PUBLIC,anon,authenticated;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.financial_retained_import_v2(
 p_owner text,p_fence bigint,p_stop bigint,p_manifest jsonb,p_part integer,p_text text,p_part_sha text)
RETURNS jsonb LANGUAGE plpgsql SET search_path TO 'pg_catalog' AS $function$
DECLARE c population_top3000_202608_202609_v1.acquisition_control%rowtype;p population_top3000_202608_202609_v1.pipeline_control%rowtype;
 a population_top3000_202608_202609_v1.financial_retained_source_v2%rowtype;old population_top3000_202608_202609_v1.financial_retained_chunk_v2%rowtype;
 sid text;h text;uri text;bytes integer;parts integer;at_time timestamptz;body text;fresh_bytes integer:=0;resolved text;
 n integer;meta jsonb;fm jsonb;fh text;k text;versions jsonb;tid uuid;ev jsonb;counts jsonb;did_complete boolean:=false;
BEGIN
 PERFORM pg_advisory_xact_lock(2026083000261022);
 SELECT * INTO c FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton FOR UPDATE;
 IF NOT FOUND THEN RAISE EXCEPTION 'FINANCIAL_RETAINED_CONTROL_MISSING';END IF;
 SELECT * INTO p FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton FOR UPDATE;
 IF NOT FOUND OR c.owner_id IS DISTINCT FROM p_owner OR c.owner_fence IS DISTINCT FROM p_fence OR c.stop_generation IS DISTINCT FROM p_stop
 OR c.owner_id IS DISTINCT FROM p.expected_owner_id OR c.owner_fence IS DISTINCT FROM p.expected_owner_fence OR c.stop_generation IS DISTINCT FROM p.expected_stop_generation
 OR c.enabled IS DISTINCT FROM true OR c.stopped IS DISTINCT FROM false OR c.hold_reason IS NOT NULL
 OR c.admitted_until IS NULL OR c.admitted_until<=clock_timestamp() OR p.enabled IS DISTINCT FROM true OR p.shared_scheduler_verified IS DISTINCT FROM true
 OR p.deadline IS NULL OR p.deadline<=clock_timestamp() OR c.scope_sha256 IS DISTINCT FROM '8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd' OR p.scope_sha256 IS DISTINCT FROM c.scope_sha256
 THEN RAISE EXCEPTION 'FINANCIAL_RETAINED_OWNER_STOP_OR_ADMISSION_CHANGED';END IF;
 PERFORM population_top3000_202608_202609_v1.financial_shared_resource_guard_v1();
 sid:=p_manifest->>'source_id';h:=p_manifest->>'source_sha256';uri:=p_manifest->>'source_uri';
 bytes:=(p_manifest->>'source_bytes')::integer;parts:=(p_manifest->>'expected_parts')::integer;at_time:=(p_manifest->>'retrieved_at')::timestamptz;
 IF jsonb_typeof(p_manifest) IS DISTINCT FROM 'object' OR h IS NULL OR h!~'^[0-9a-f]{64}$' OR sid IS DISTINCT FROM 'financial-sec-retained-v2:'||h
 OR uri IS NULL OR NOT(uri~'^https://www[.]sec[.]gov/Archives/edgar/data/[0-9]+/[0-9]{18}/[A-Za-z0-9._-]+[.](htm|html|xml)$' OR uri~'^https://data[.]sec[.]gov/(submissions/CIK[0-9]{10}[.]json|api/xbrl/companyfacts/CIK[0-9]{10}[.]json)$')
 OR bytes IS NULL OR bytes NOT BETWEEN 1 AND 8388608 OR parts IS NULL OR parts NOT BETWEEN 1 AND 64
 OR at_time IS NULL OR at_time<'2026-10-24T00:00:00Z' OR at_time>clock_timestamp()+interval '30 seconds'
 OR p_manifest->>'representation' IS DISTINCT FROM 'EXACT_UTF8_ORIGINAL_SEC_SOURCE'
 OR p_manifest->>'original_historical_receipt_proven' IS DISTINCT FROM 'false'
 OR p_manifest->>'source_published_at' IS NOT NULL OR p_manifest->>'historical_received_at' IS NOT NULL OR p_manifest->>'effective_available_at' IS NOT NULL
 OR p_manifest->>'financial_shared_bundle_sha256' IS DISTINCT FROM population_top3000_202608_202609_v1.financial_shared_bundle_v1()
 OR p_manifest->>'importer_sha256' IS DISTINCT FROM encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.financial_retained_import_v2(text,bigint,bigint,jsonb,integer,text,text)'::regprocedure),'UTF8')),'hex')
 OR p_manifest#>>'{retrieval_receipt,url}' IS DISTINCT FROM uri OR p_manifest#>>'{retrieval_receipt,sha256}' IS DISTINCT FROM h
 OR p_manifest#>>'{retrieval_receipt,bytes}' IS DISTINCT FROM bytes::text OR p_manifest#>>'{retrieval_receipt,http_status}' IS DISTINCT FROM '200'
 OR (p_manifest#>>'{retrieval_receipt,retrieved_at}')::timestamptz IS DISTINCT FROM at_time
 OR p_part IS NULL OR p_part NOT BETWEEN 0 AND parts-1 OR p_text IS NULL OR octet_length(p_text) NOT BETWEEN 1 AND 262144
 OR p_part_sha IS DISTINCT FROM encode(sha256(convert_to(p_text,'UTF8')),'hex')
 THEN RAISE EXCEPTION 'FINANCIAL_RETAINED_MANIFEST_OR_PART_INVALID';END IF;
 meta:=p_manifest->'financial_document';
 IF meta IS NOT NULL AND meta<>'null'::jsonb AND (jsonb_typeof(meta) IS DISTINCT FROM 'object'
  OR meta->>'accession_number' IS NULL OR meta->>'accession_number'!~'^[0-9]{10}-[0-9]{2}-[0-9]{6}$'
  OR meta->>'file_id' IS NULL OR meta->>'file_id'!~'^[A-Za-z0-9][A-Za-z0-9._-]{0,200}[.](htm|html|xml)$'
  OR meta->>'time_role' IS NULL OR meta->>'time_role' NOT IN('SUPPORT','TARGET')
  OR meta->>'cik' IS NULL OR meta->>'cik'!~'^[0-9]{10}$' OR meta->>'cik'='0000000000'
  OR jsonb_typeof(meta->'filing_ciks') IS DISTINCT FROM 'array' OR jsonb_array_length(meta->'filing_ciks') NOT BETWEEN 1 AND 10
  OR NOT(meta->'filing_ciks'? (meta->>'cik'))
  OR uri IS DISTINCT FROM 'https://www.sec.gov/Archives/edgar/data/'||ltrim(meta->>'cik','0')||'/'||replace(meta->>'accession_number','-','')||'/'||(meta->>'file_id')
  OR meta->>'source_published_at' IS NOT NULL OR meta->>'historical_received_at' IS NOT NULL OR meta->>'effective_available_at' IS NOT NULL)
 THEN RAISE EXCEPTION 'FINANCIAL_RETAINED_DOCUMENT_METADATA_INVALID';END IF;
 INSERT INTO population_top3000_202608_202609_v1.financial_retained_source_v2(source_id,manifest,manifest_sha256)
 VALUES(sid,p_manifest,encode(sha256(convert_to(p_manifest::text,'UTF8')),'hex'))ON CONFLICT DO NOTHING;
 SELECT * INTO STRICT a FROM population_top3000_202608_202609_v1.financial_retained_source_v2 WHERE source_id=sid FOR UPDATE;
 IF a.manifest IS DISTINCT FROM p_manifest OR a.manifest_sha256 IS DISTINCT FROM encode(sha256(convert_to(p_manifest::text,'UTF8')),'hex') THEN RAISE EXCEPTION 'FINANCIAL_RETAINED_ADMISSION_REPLAY_CONFLICT';END IF;
 -- Reuse independently verified existing exact raw sources, including previous indices. No duplicate stored-byte charge.
 SELECT s.source_id INTO resolved FROM population_top3000_202608_202609_v1.source_document s WHERE s.source_provider='SEC' AND s.source_uri=uri AND s.source_sha256=h
  AND s.content_bytes=bytes AND s.content_bytes=octet_length(s.content_text) AND s.source_sha256=encode(sha256(convert_to(s.content_text,'UTF8')),'hex')
 ORDER BY s.retrieved_at,s.source_id LIMIT 1;
 IF resolved IS NULL THEN
  SELECT * INTO old FROM population_top3000_202608_202609_v1.financial_retained_chunk_v2 WHERE source_id=sid AND part_number=p_part;
  IF FOUND THEN
   IF old.raw_text IS DISTINCT FROM p_text OR old.chunk_sha256 IS DISTINCT FROM p_part_sha THEN RAISE EXCEPTION 'FINANCIAL_RETAINED_CHUNK_REPLAY_CONFLICT';END IF;
  ELSE
   IF c.stored_bytes+c.reserved_bytes+octet_length(p_text)>c.max_source_bytes OR (SELECT coalesce(sum(chunk_bytes),0)+octet_length(p_text)>bytes FROM population_top3000_202608_202609_v1.financial_retained_chunk_v2 WHERE source_id=sid) THEN RAISE EXCEPTION 'FINANCIAL_RETAINED_BYTE_BOUND';END IF;
   INSERT INTO population_top3000_202608_202609_v1.financial_retained_chunk_v2 VALUES(sid,p_part,p_text,p_part_sha,octet_length(p_text));
   UPDATE population_top3000_202608_202609_v1.acquisition_control SET stored_bytes=stored_bytes+octet_length(p_text),last_useful_progress_at=clock_timestamp(),updated_at=clock_timestamp() WHERE singleton;
   fresh_bytes:=octet_length(p_text);
  END IF;
  SELECT count(*) INTO n FROM population_top3000_202608_202609_v1.financial_retained_chunk_v2 WHERE source_id=sid;
  IF n<>parts THEN
   PERFORM population_top3000_202608_202609_v1.financial_shared_resource_guard_v1();
   INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details)VALUES('FINANCIAL_RETAINED_CHUNK_COMMITTED',jsonb_build_object('source_id',sid,'part_number',p_part,'new_bytes',fresh_bytes,'parts_received',n,'parts_expected',parts,'owner_fence',p_fence,'stop_generation',p_stop));
   RETURN jsonb_build_object('state','PARTIAL','source_id',sid,'parts_received',n,'parts_expected',parts,'new_stored_bytes',fresh_bytes);
  END IF;
  SELECT string_agg(raw_text,'' ORDER BY part_number) INTO body FROM population_top3000_202608_202609_v1.financial_retained_chunk_v2 WHERE source_id=sid;
  IF octet_length(body) IS DISTINCT FROM bytes OR encode(sha256(convert_to(body,'UTF8')),'hex') IS DISTINCT FROM h THEN RAISE EXCEPTION 'FINANCIAL_RETAINED_ASSEMBLED_SOURCE_INVALID';END IF;
  IF (SELECT stored_bytes+reserved_bytes+bytes>max_source_bytes FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton) THEN RAISE EXCEPTION 'FINANCIAL_RETAINED_FINAL_BYTE_BOUND';END IF;
  ev:=jsonb_build_object('representation','EXACT_UTF8_ORIGINAL_SEC_SOURCE','retrieval_receipt',p_manifest->'retrieval_receipt','source_manifest_sha256',a.manifest_sha256,
   'acquired_outside_worker',true,'request_counter_accounting','EXTERNAL_HTTP_LEDGER_RECONCILED_SEPARATELY_ONCE','original_historical_receipt_proven',false,'company_population_verified',false);
  INSERT INTO population_top3000_202608_202609_v1.source_document(source_id,source_sha256,source_uri,source_provider,retrieved_at,source_published_at,historical_received_at,content_text,content_bytes,evidence,license_policy)
  VALUES(sid,h,uri,'SEC',at_time,NULL,NULL,body,bytes,ev,'PUBLIC_SEC_EDGAR_ORIGINAL_SOURCE_INTERNAL_MISSION_PRESERVATION');
  UPDATE population_top3000_202608_202609_v1.acquisition_control SET stored_bytes=stored_bytes+bytes,last_useful_progress_at=clock_timestamp(),updated_at=clock_timestamp() WHERE singleton;
  fresh_bytes:=fresh_bytes+bytes;resolved:=sid;did_complete:=true;
 END IF;
 IF a.resolved_source_id IS NOT NULL AND a.resolved_source_id IS DISTINCT FROM resolved THEN RAISE EXCEPTION 'FINANCIAL_RETAINED_RESOLVED_SOURCE_CHANGED';END IF;
 UPDATE population_top3000_202608_202609_v1.financial_retained_source_v2 SET resolved_source_id=resolved,completed_at=coalesce(completed_at,clock_timestamp()) WHERE source_id=sid;
 IF meta IS NOT NULL AND meta<>'null'::jsonb THEN
  SELECT content_text INTO STRICT body FROM population_top3000_202608_202609_v1.source_document WHERE source_id=resolved;
  counts:=population_top3000_202608_202609_v1.financial_document_counts_v1(body);
  IF (counts->>'source_fact_nodes')::integer<1 THEN RAISE EXCEPTION 'FINANCIAL_RETAINED_NO_XBRL_FACTS';END IF;
  k:='financial-document-v1:'||encode(sha256(convert_to(jsonb_build_array(meta->>'accession_number',meta->>'file_id',h)::text,'UTF8')),'hex');
  SELECT jsonb_object_agg(f.family_id||'_contract_sha256',f.contract_sha256) INTO versions FROM population_top3000_202608_202609_v1.catalog_family_contract f
   WHERE f.scope_id='corrected_company_scope_8e6ca1178f03822c' AND f.family_id IN('F02','F03','F04');
  versions:=versions||jsonb_build_object('scope_sha256',c.scope_sha256,'financial_shared_revision','financial-shared-document-v1','financial_shared_bundle_sha256',p_manifest->>'financial_shared_bundle_sha256');
  fm:=meta||jsonb_build_object('document_key',k,'scope_sha256',c.scope_sha256,'source_id',resolved,'source_kind','MISSION_TEXT','body_sha256',h,'container_sha256',h,'body_bytes',bytes,
   'source_uri',uri,'source_provider','SEC','source_anchors',jsonb_build_array(jsonb_build_object('source_id',resolved,'source_sha256',h)),
   'source_metadata_basis','EXACT_RETAINED_SEC_SOURCE_AND_ORIGINAL_FILING_IDENTITY','company_population_verified',false,'original_historical_receipt_proven',false,'effective_available_at',NULL,'source_published_at',NULL,'historical_received_at',NULL);
  fh:=encode(sha256(convert_to(fm::text,'UTF8')),'hex');
  INSERT INTO population_top3000_202608_202609_v1.financial_shared_admission_v1(document_key,manifest,manifest_sha256,input_versions)VALUES(k,fm,fh,versions)ON CONFLICT DO NOTHING;
  IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.financial_shared_admission_v1 WHERE document_key=k AND manifest=fm AND manifest_sha256=fh AND input_versions=versions) THEN RAISE EXCEPTION 'FINANCIAL_RETAINED_SHARED_ADMISSION_CONFLICT';END IF;
  PERFORM population_top3000_202608_202609_v1.financial_shared_body_v1(k);
  tid:=population_top3000_202608_202609_v1.pipeline_enqueue_v1('F03','ACQUIRE',k,jsonb_build_object('kind','SHARED_FINANCIAL_DOCUMENT_V1','document_key',k,'document_manifest_sha256',fh),versions,NULL);
 END IF;
 PERFORM population_top3000_202608_202609_v1.financial_shared_resource_guard_v1();
 IF did_complete OR a.completed_at IS NULL THEN
  INSERT INTO population_top3000_202608_202609_v1.pipeline_event(task_id,event,details)VALUES(tid,'FINANCIAL_RETAINED_SOURCE_IMPORTED',jsonb_build_object('source_id',resolved,'admission_source_id',sid,'sha256',h,'source_bytes',bytes,'new_stored_bytes',fresh_bytes,
  'reused_existing_source',NOT did_complete,'provider_requests_in_this_function',0,'external_request_reconciliation_separate',true,'owner_fence',p_fence,'stop_generation',p_stop,'financial_successor_task_id',tid,'no_population_acceptance',true));
 END IF;
 RETURN jsonb_build_object('state','SOURCE_RETAINED','source_id',resolved,'source_sha256',h,'source_bytes',bytes,'new_stored_bytes',fresh_bytes,'task_id',tid,'document_key',k,'no_population_acceptance',true);
END; $function$;
REVOKE ALL ON FUNCTION population_top3000_202608_202609_v1.financial_retained_import_v2(text,bigint,bigint,jsonb,integer,text,text) FROM PUBLIC,anon,authenticated;
