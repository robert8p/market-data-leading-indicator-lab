-- Official SEC exact admitted financial-document transport v2. Root-only deployment and registration.
CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.financial_sec_context_v2(p_task uuid)
RETURNS jsonb LANGUAGE plpgsql STABLE SET search_path TO 'pg_catalog' AS $function$
DECLARE t population_top3000_202608_202609_v1.pipeline_task%rowtype;
r population_top3000_202608_202609_v1.financial_target_request_v1%rowtype;anchor jsonb;
cik text;acc text;file_id text;uri text;anchor_count integer;has_index boolean:=false;
BEGIN
 SELECT * INTO STRICT t FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=p_task;
 IF t.family_id IS DISTINCT FROM 'F03' OR t.stage IS DISTINCT FROM 'ACQUIRE'
 OR t.payload->>'kind' IS DISTINCT FROM 'FRESH_FINANCIAL_DOCUMENT_V1'
 OR t.payload->>'source_transport' IS DISTINCT FROM 'OFFICIAL_SEC_V2'
 OR t.input_versions->>'scope_sha256' IS DISTINCT FROM '8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd'
 OR t.input_versions->>'financial_shared_bundle_sha256' IS DISTINCT FROM population_top3000_202608_202609_v1.financial_shared_bundle_v1()
 OR t.input_versions->>'financial_sec_bundle_sha256' IS DISTINCT FROM population_top3000_202608_202609_v1.financial_sec_bundle_v2()
 THEN RAISE EXCEPTION 'FINANCIAL_SEC_V2_SCOPE_OR_CODE_NOT_ADMITTED';END IF;
 SELECT * INTO STRICT r FROM population_top3000_202608_202609_v1.financial_target_request_v1 WHERE document_key=t.payload->>'document_key';
 cik:=r.manifest->>'cik';acc:=r.manifest->>'accession_number';file_id:=r.manifest->>'file_id';
 IF cik IS NULL OR cik!~'^[0-9]{10}$' OR cik='0000000000' OR acc IS NULL OR acc!~'^[0-9]{10}-[0-9]{2}-[0-9]{6}$'
 OR file_id IS NULL OR file_id!~'^[A-Za-z0-9][A-Za-z0-9._-]{0,200}\.(htm|html|xml)$'
 THEN RAISE EXCEPTION 'FINANCIAL_SEC_V2_IDENTIFIER_INVALID';END IF;
 uri:='https://www.sec.gov/Archives/edgar/data/'||ltrim(cik,'0')||'/'||replace(acc,'-','')||'/'||file_id;
 IF r.document_key IS DISTINCT FROM t.partition_key OR r.input_versions IS DISTINCT FROM t.input_versions
 OR r.manifest_sha256 IS DISTINCT FROM t.payload->>'request_manifest_sha256'
 OR r.manifest_sha256 IS DISTINCT FROM encode(sha256(convert_to(r.manifest::text,'UTF8')),'hex')
 OR r.manifest->>'request_url' IS DISTINCT FROM uri
 OR r.manifest->>'max_response_bytes' IS DISTINCT FROM '8388608'
 OR r.manifest->>'max_runtime_ms' IS DISTINCT FROM '45000'
 OR r.manifest->>'maximum_provider_requests' IS DISTINCT FROM '1'
 OR r.manifest->>'expected_file_bytes' IS NULL OR (r.manifest->>'expected_file_bytes')::integer NOT BETWEEN 1 AND 8388608
 OR (r.manifest->>'expected_file_sha256' IS NOT NULL AND r.manifest->>'expected_file_sha256'!~'^[0-9a-f]{64}$')
 OR jsonb_typeof(r.manifest->'document_metadata') IS DISTINCT FROM 'object'
 OR r.manifest#>>'{document_metadata,cik}' IS DISTINCT FROM cik
 OR r.manifest#>>'{document_metadata,accession_number}' IS DISTINCT FROM acc
 OR r.manifest#>>'{document_metadata,time_role}' IS NULL OR r.manifest#>>'{document_metadata,time_role}' NOT IN('TARGET','SUPPORT')
 OR r.manifest#>>'{document_metadata,effective_available_at}' IS NOT NULL
 OR r.manifest#>>'{document_metadata,historical_received_at}' IS NOT NULL
 OR r.manifest#>>'{document_metadata,source_published_at}' IS NOT NULL
 OR r.fulfilled_attempt_id IS NOT NULL
 OR jsonb_typeof(r.manifest->'source_anchors') IS DISTINCT FROM 'array'
 THEN RAISE EXCEPTION 'FINANCIAL_SEC_V2_EXACT_REQUEST_ADMISSION_CHANGED';END IF;
 anchor_count:=jsonb_array_length(r.manifest->'source_anchors');
 IF anchor_count NOT BETWEEN 1 AND 4 THEN RAISE EXCEPTION 'FINANCIAL_SEC_V2_METADATA_ANCHOR_BOUND';END IF;
 FOR anchor IN SELECT value FROM jsonb_array_elements(r.manifest->'source_anchors') LOOP
  IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.source_document s
    WHERE s.source_id=anchor->>'source_id' AND s.source_sha256=anchor->>'source_sha256'
    AND s.content_bytes=octet_length(s.content_text) AND s.source_sha256=encode(sha256(convert_to(s.content_text,'UTF8')),'hex'))
  THEN RAISE EXCEPTION 'FINANCIAL_SEC_V2_METADATA_ANCHOR_CHANGED';END IF;
  IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.source_document s WHERE s.source_id=anchor->>'source_id'
    AND s.source_provider='SEC' AND s.source_uri='https://www.sec.gov/Archives/edgar/data/'||ltrim(cik,'0')||'/'||replace(acc,'-','')||'/'||acc||'-index.htm'
    AND EXISTS(SELECT 1 FROM regexp_matches(s.content_text,'(<tr[^>]*>.*?</tr>)','gs') rr
      WHERE position('href="'||replace(uri,'https://www.sec.gov','')||'"' in rr[1])>0
      AND (regexp_match(rr[1],'<td[^>]*>[[:space:]]*([0-9]+)[[:space:]]*</td>[[:space:]]*</tr>$','s'))[1]::bigint
          =(r.manifest->>'expected_file_bytes')::bigint)) THEN has_index:=true;END IF;
 END LOOP;
 IF NOT has_index THEN RAISE EXCEPTION 'FINANCIAL_SEC_V2_ORIGINAL_INDEX_ANCHOR_REQUIRED';END IF;
 IF (SELECT count(*) FROM population_top3000_202608_202609_v1.catalog_family_contract f
 WHERE f.scope_id='corrected_company_scope_8e6ca1178f03822c' AND f.family_id IN('F02','F03','F04')
 AND f.contract_sha256=t.input_versions->>(f.family_id||'_contract_sha256')
 AND f.contract_sha256=encode(sha256(convert_to(f.contract_text,'UTF8')),'hex'))<>3
 THEN RAISE EXCEPTION 'FINANCIAL_SEC_V2_FAMILY_CONTRACT_CHANGED';END IF;
 RETURN r.manifest||jsonb_build_object('document_key',r.document_key,'request_manifest_sha256',r.manifest_sha256,
 'family_id',t.family_id,'partition_key',t.partition_key,'state','READY');
END $function$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.financial_sec_claim_v2(p_attempt uuid, p_token text)
 RETURNS jsonb
 LANGUAGE plpgsql
 SET search_path TO 'pg_catalog'
AS $function$
DECLARE tid uuid;c jsonb;r jsonb;
BEGIN
 tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);
 c:=population_top3000_202608_202609_v1.financial_sec_context_v2(tid);
 r:=population_top3000_202608_202609_v1.pipeline_provider_reserve_v1(p_attempt,p_token,'SEC',8388608,0);
 IF r->>'state'<>'RESERVED'THEN RETURN population_top3000_202608_202609_v1.pipeline_finish_v1(p_attempt,p_token,
  jsonb_build_object('state','BLOCKED','blocker_code','FINANCIAL_SEC_V2_PROVIDER_ADMISSION_HELD',
   'resumption_condition',jsonb_build_object('event','CONTROLLER_RECONCILES_BOUNDED_PROVIDER_ADMISSION','receipt',r,'automatic_retry',false)));END IF;
 RETURN c||jsonb_build_object('state','RESERVED');
END $function$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.financial_sec_commit_v2(p_attempt uuid, p_token text, p_source jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 SET search_path TO 'pg_catalog'
AS $function$
DECLARE tid uuid;c jsonb;t population_top3000_202608_202609_v1.pipeline_task%rowtype;txt text;h text;bytes integer;at_time timestamptz;
 sid text;v_manifest jsonb;mh text;k text;counts jsonb;ev jsonb;
BEGIN
 tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);
 SELECT*INTO STRICT t FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=tid;
 c:=population_top3000_202608_202609_v1.financial_sec_context_v2(tid);k:=c->>'document_key';
 PERFORM population_top3000_202608_202609_v1.financial_shared_resource_guard_v1();
 IF jsonb_typeof(p_source)IS DISTINCT FROM'object'OR p_source->>'document_key'IS DISTINCT FROM k
  OR p_source->>'request_url'IS DISTINCT FROM c->>'request_url'OR p_source->>'http_status'IS DISTINCT FROM'200'
  OR p_source->>'request_manifest_sha256'IS DISTINCT FROM c->>'request_manifest_sha256'
  OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_provider_lease l WHERE l.attempt_id=p_attempt AND l.provider='SEC'AND l.released_at IS NULL AND l.reserved_bytes=8388608)
 THEN RAISE EXCEPTION'FINANCIAL_SEC_V2_SCOPE_OR_LEASE_MISMATCH';END IF;
 txt:=p_source->>'content_text';bytes:=octet_length(txt);h:=encode(sha256(convert_to(txt,'UTF8')),'hex');
 IF (c->>'expected_file_sha256' IS NOT NULL AND h IS DISTINCT FROM c->>'expected_file_sha256') OR txt IS NULL OR bytes IS DISTINCT FROM(c->>'expected_file_bytes')::integer OR bytes NOT BETWEEN 1 AND 8388608
  OR h IS DISTINCT FROM p_source->>'source_sha256'OR h IS DISTINCT FROM p_source->>'wire_sha256'
  OR bytes IS DISTINCT FROM(p_source->>'wire_bytes')::integer OR coalesce((p_source->>'elapsed_ms')::integer,-1)NOT BETWEEN 0 AND 45000
 THEN RAISE EXCEPTION'FINANCIAL_SEC_V2_EXACT_RAW_BODY_RECEIPT_INVALID';END IF;
 counts:=population_top3000_202608_202609_v1.financial_document_counts_v1(txt);
 IF(counts->>'source_fact_nodes')::integer=0 THEN RAISE EXCEPTION'FINANCIAL_SEC_V2_XBRL_INSTANCE_HAS_NO_CONTEXT_FACTS';END IF;
 at_time:=(p_source->>'retrieved_at')::timestamptz;
 IF at_time IS NULL OR at_time<(SELECT a.started_at FROM population_top3000_202608_202609_v1.pipeline_attempt a WHERE a.attempt_id=p_attempt)-interval'30 seconds'
  OR at_time>clock_timestamp()+interval'30 seconds'THEN RAISE EXCEPTION'FINANCIAL_SEC_V2_RETRIEVAL_CLOCK_INVALID';END IF;
 sid:='financial-sec-exact-file-v2:'||substr(k,length('financial-document-v1:')+1);
 ev:=jsonb_build_object('request_manifest_sha256',c->>'request_manifest_sha256','attempt_id',p_attempt,'wire_sha256',h,'wire_bytes',bytes,
  'representation','EXACT_UTF8_RAW_PROVIDER_FILE_RESPONSE','metadata_anchors',c->'source_anchors','original_historical_receipt_proven',false,
  'provider_http_status',200,'provider_request_retries',0,'prior_html_unknown_preserved',true);
 INSERT INTO population_top3000_202608_202609_v1.source_document(source_id,source_sha256,source_uri,source_provider,retrieved_at,source_published_at,historical_received_at,content_text,content_bytes,evidence,license_policy)
 VALUES(sid,h,c->>'request_url','SEC',at_time,NULL,NULL,txt,bytes,ev,'PUBLIC_SEC_EDGAR_ORIGINAL_FILING_INTERNAL_MISSION_PRESERVATION')ON CONFLICT DO NOTHING;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.source_document s WHERE s.source_id=sid AND s.source_sha256=h AND s.source_uri=c->>'request_url'
  AND s.source_provider='SEC'AND s.retrieved_at=at_time AND s.content_text=txt AND s.content_bytes=bytes AND s.evidence=ev
  AND s.source_published_at IS NULL AND s.historical_received_at IS NULL)
 THEN RAISE EXCEPTION'FINANCIAL_SEC_V2_RETAINED_SOURCE_REPLAY_CONFLICT';END IF;
 v_manifest:=c->'document_metadata'||jsonb_build_object('document_key',k,'source_kind','MISSION_TEXT','source_id',sid,'container_sha256',h,
  'body_sha256',h,'body_bytes',bytes,'source_provider','SEC','source_uri',c->>'request_url','request_manifest_sha256',c->>'request_manifest_sha256',
  'source_anchors',c->'source_anchors','source_retrieved_at',at_time,'effective_available_at',NULL,'precise_publication_verified',false,'original_historical_receipt_proven',false);
 mh:=encode(sha256(convert_to(v_manifest::text,'UTF8')),'hex');
 INSERT INTO population_top3000_202608_202609_v1.financial_shared_admission_v1(document_key,manifest,manifest_sha256,input_versions)
 VALUES(k,v_manifest,mh,t.input_versions)ON CONFLICT DO NOTHING;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.financial_shared_admission_v1 a WHERE a.document_key=k AND a.manifest=v_manifest AND a.manifest_sha256=mh AND a.input_versions=t.input_versions)
 THEN RAISE EXCEPTION'FINANCIAL_SEC_V2_ADMISSION_REPLAY_CONFLICT';END IF;
 INSERT INTO population_top3000_202608_202609_v1.financial_shared_document_v1(document_key,body_sha256,body_bytes,source_fact_count,context_count,unit_count)
 VALUES(k,h,bytes,(counts->>'source_fact_nodes')::integer,(counts->>'contexts')::integer,(counts->>'units')::integer);
 UPDATE population_top3000_202608_202609_v1.financial_target_request_v1 SET fulfilled_attempt_id=p_attempt,fulfilled_at=clock_timestamp()WHERE document_key=k AND fulfilled_attempt_id IS NULL;
 IF NOT FOUND THEN RAISE EXCEPTION'FINANCIAL_SEC_V2_REQUEST_ALREADY_FULFILLED';END IF;
 PERFORM population_top3000_202608_202609_v1.financial_shared_resource_guard_v1();
 PERFORM population_top3000_202608_202609_v1.pipeline_provider_release_v1(p_attempt,p_token,bytes,0,0);
 RETURN population_top3000_202608_202609_v1.pipeline_finish_v1(p_attempt,p_token,jsonb_build_object('state','COMPLETE',
  'commit_receipt',jsonb_build_object('commit_key','financial-exact-file:'||sid,'output_sha256',h,'new_committed_units',1,
   'immutable_output_ids',jsonb_build_array(sid,k),'input_versions',t.input_versions,'source_fact_nodes',counts->'source_fact_nodes',
   'wire_byte_identity_verified',true,'prior_html_unknown_preserved',true,'company_population_verified',false)));
END $function$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.financial_sec_failure_v2(p_attempt uuid, p_token text, p_failure jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 SET search_path TO 'pg_catalog'
AS $function$
DECLARE tid uuid;code text;class text;bytes bigint;retry integer;unknown_effect boolean;details jsonb;
BEGIN
 tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);PERFORM population_top3000_202608_202609_v1.financial_sec_context_v2(tid);
 code:=p_failure->>'code';class:=p_failure->>'failure_class';bytes:=(p_failure->>'received_bytes')::bigint;retry:=(p_failure->>'retry_after_seconds')::integer;
 IF code IS NULL OR code!~'^[A-Z0-9_]{1,80}$'OR class IS NULL OR class NOT IN('RATE_LIMIT','TRANSPORT_READ','AUTHENTICATION','PARSER','SOURCE_ABSENT')
  OR jsonb_typeof(p_failure->'fetch_started') IS DISTINCT FROM 'boolean'
  OR (p_failure->>'wire_sha256' IS NOT NULL AND p_failure->>'wire_sha256'!~'^[0-9a-f]{64}$')
  OR (p_failure->>'http_status' IS NOT NULL AND (jsonb_typeof(p_failure->'http_status') IS DISTINCT FROM 'number'
    OR p_failure->>'http_status'!~'^[0-9]{3}$' OR (p_failure->>'http_status')::integer NOT BETWEEN 100 AND 599))
  OR bytes IS NULL OR bytes NOT BETWEEN 0 AND 8388608 OR retry IS NULL OR retry NOT BETWEEN 0 AND 3600 THEN RAISE EXCEPTION'FINANCIAL_SEC_V2_FAILURE_RECEIPT_INVALID';END IF;
 details:=jsonb_build_object('code',code,'http_status',p_failure->'http_status','response_bytes_observed',bytes,'wire_sha256',p_failure->>'wire_sha256',
  'fetch_started',p_failure->'fetch_started','raw_body_retained',false,'automatic_retry',false,'prior_html_unknown_preserved',true);
 unknown_effect:=coalesce((p_failure->>'fetch_started')::boolean,false)AND(p_failure->>'http_status'IS NULL OR code IN('DEADLINE','FETCH_FAILED','EMPTY_BODY')
  OR(p_failure->>'wire_sha256'IS NULL AND p_failure->>'http_status'='200'));
 IF unknown_effect THEN RETURN population_top3000_202608_202609_v1.pipeline_fail_v1(p_attempt,p_token,'UNCERTAIN_EFFECT','FINANCIAL_SEC_V2:'||code,details,retry);END IF;
 PERFORM population_top3000_202608_202609_v1.pipeline_provider_release_v1(p_attempt,p_token,0,0,CASE WHEN class='RATE_LIMIT'THEN retry ELSE 0 END);
 RETURN population_top3000_202608_202609_v1.pipeline_finish_v1(p_attempt,p_token,jsonb_build_object('state','BLOCKED','blocker_code','FINANCIAL_SEC_V2_'||code,
  'resumption_condition',jsonb_build_object('event','CONTROLLER_REVIEWS_EXACT_FILE_FAILURE','details',details,'automatic_retry',false)));
END $function$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.financial_source_acquire_v2(p_attempt uuid,p_token text)
RETURNS jsonb LANGUAGE plpgsql SET search_path TO 'pg_catalog' AS $function$
DECLARE tid uuid;t population_top3000_202608_202609_v1.pipeline_task%rowtype;
BEGIN
 tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);
 SELECT * INTO STRICT t FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=tid;
 IF t.payload->>'source_transport'='OFFICIAL_SEC_V2' THEN
  PERFORM population_top3000_202608_202609_v1.financial_sec_context_v2(tid);
  RETURN population_top3000_202608_202609_v1.pipeline_dispatch_edge_v1(p_attempt,p_token,'population-top3000-sec-financial-v2');
 END IF;
 RETURN population_top3000_202608_202609_v1.financial_shared_acquire_v1(p_attempt,p_token);
END $function$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.financial_sec_bundle_v2()
RETURNS text LANGUAGE sql STABLE SET search_path TO 'pg_catalog' AS $function$
 SELECT encode(sha256(convert_to(string_agg(pg_get_functiondef(p.oid),E'\n' ORDER BY p.proname),'UTF8')),'hex')
 FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
 WHERE n.nspname='population_top3000_202608_202609_v1' AND p.proname IN
 ('financial_sec_context_v2','financial_sec_claim_v2','financial_sec_commit_v2','financial_sec_failure_v2','financial_source_acquire_v2');
$function$;

REVOKE ALL ON FUNCTION population_top3000_202608_202609_v1.financial_sec_context_v2(uuid),population_top3000_202608_202609_v1.financial_sec_claim_v2(uuid,text),population_top3000_202608_202609_v1.financial_sec_commit_v2(uuid,text,jsonb),population_top3000_202608_202609_v1.financial_sec_failure_v2(uuid,text,jsonb),population_top3000_202608_202609_v1.financial_source_acquire_v2(uuid,text),population_top3000_202608_202609_v1.financial_sec_bundle_v2() FROM PUBLIC,anon,authenticated;
