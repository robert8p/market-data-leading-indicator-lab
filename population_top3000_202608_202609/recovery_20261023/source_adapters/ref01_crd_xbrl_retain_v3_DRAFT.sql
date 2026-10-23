-- ROOT ONLY DRAFT. Do not execute before controller review.
-- Uses no new worker, RPC, adapter binding, fake pipeline attempt or automatic retry.
BEGIN;
SET LOCAL statement_timeout='10s';
SET LOCAL lock_timeout='2s';
SELECT pg_advisory_xact_lock(2026083000261022);
-- V3: retain complete result, then verify bounded file-list metadata or optional exact XML string.
-- Root provides existing ref01_* GUCs and optional top3000.ref01_xml_json_path.
DO $retain$
DECLARE
 c population_top3000_202608_202609_v1.acquisition_control%rowtype;
 p population_top3000_202608_202609_v1.pipeline_control%rowtype;
 m jsonb;route jsonb;route_id integer;reservation_id uuid;reservation jsonb;prior_receipt jsonb;
 relation_bytes bigint;limit_bytes bigint;budget_total numeric;changed integer;
v_raw bytea;v_content text;v_doc jsonb;v_hash text;v_sid text;v_observed_at timestamptz;
v_tool text;outcome_class text;v_evidence jsonb;v_receipt jsonb;v_fresh integer;v_retention_cap bigint;
v_uri text;v_wave_bytes bigint;v_v3_bytes bigint;v_size_evidence jsonb;v_xml_evidence jsonb;
v_csv text;v_rows jsonb;v_fields text[];v_field text;v_quoted boolean;v_after_quote boolean;v_i integer;v_ch text;v_selected jsonb;v_file_size bigint;
v_xml_path jsonb;v_xml_path_array text[];v_xml_text text;v_xml_value xml;v_xml_bytes bigint;v_xml_hash text;
BEGIN
SELECT * INTO c FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton FOR UPDATE;
SELECT * INTO p FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton FOR UPDATE;
IF c.owner_id IS DISTINCT FROM 'top3000-recovery-20261023-root-4b1314bcb5c2' OR c.owner_fence IS DISTINCT FROM 2 OR c.stop_generation IS DISTINCT FROM 0
 OR p.expected_owner_id IS DISTINCT FROM c.owner_id OR p.expected_owner_fence IS DISTINCT FROM c.owner_fence OR p.expected_stop_generation IS DISTINCT FROM c.stop_generation
 OR c.scope_sha256 IS DISTINCT FROM '8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd' OR p.scope_sha256 IS DISTINCT FROM c.scope_sha256
 OR NOT c.enabled OR c.stopped OR c.admitted_until IS NULL OR c.admitted_until<=clock_timestamp() OR NOT p.enabled OR NOT p.shared_scheduler_verified OR p.deadline IS NULL OR p.deadline<=clock_timestamp()
 THEN RAISE EXCEPTION 'CURRENT_ROOT_OWNER_SCOPE_STOP_ADMISSION_REQUIRED';END IF;
IF NOT EXISTS(SELECT 1 FROM cron.job WHERE jobid=596 AND active=false) THEN RAISE EXCEPTION 'CRON_596_MUST_BE_PAUSED';END IF;
IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE status='LEASED')
 OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_provider_lease WHERE released_at IS NULL)
 OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.acquisition_job WHERE status='LEASED')
 THEN RAISE EXCEPTION 'NO_TASK_OR_PROVIDER_LEASE_MAY_OVERLAP_MANUAL_CONNECTOR';END IF;
IF coalesce(current_setting('top3000.ref01_reservation_id',true),'')!~'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN RAISE EXCEPTION 'RESERVATION_ID_REQUIRED';END IF;
reservation_id:=current_setting('top3000.ref01_reservation_id')::uuid;
SELECT e.details INTO reservation FROM population_top3000_202608_202609_v1.pipeline_event e
WHERE e.event='ROOT_REF01_CONNECTOR_RESERVED_V1' AND e.details->>'reservation_id'=reservation_id::text ORDER BY e.at DESC LIMIT 1;
IF reservation IS NULL OR reservation->>'manifest_sha256' IS DISTINCT FROM 'ace46fc9d094f58f18cdfac0b1b1adf9fa1c295a9608b3375eeb3d0a6af1e4b7'
 OR reservation->>'controller' IS DISTINCT FROM c.owner_id OR reservation->>'owner_fence' IS DISTINCT FROM c.owner_fence::text
 OR reservation->>'stop_generation' IS DISTINCT FROM c.stop_generation::text OR reservation->>'scope_sha256' IS DISTINCT FROM c.scope_sha256
 THEN RAISE EXCEPTION 'RESERVATION_OWNER_OR_MANIFEST_MISMATCH';END IF;
route:=reservation->'route';route_id:=(route->>'route_id')::integer;
IF route_id IS NULL OR route IS NULL OR route_id NOT IN(11,12) OR route IS DISTINCT FROM ($manifest$[{"route_id":11,"family_id":"REF01","case":"CRD","accession":"0001193125-26-330643","expected_cik":"0000025475","kind":"xbrl_file_size_metadata","method":"GET","provider":"Massive","route":"/v1/reference/sec/filings/0001193125-26-330643/files","parameters":{"filename":"crda-20260630_htm.xml","sort":"filename","order":"asc","limit":10},"observed_filename":"crda-20260630_htm.xml","observed_xbrl_instance_url":"https://api.massive.com/v1/reference/sec/filings/0001193125-26-330643/files/crda-20260630_htm.xml","metadata_anchor":{"source_id":"ref01-massive-connector:2d4e9adf-4f48-4957-8f81-82f5341ba97a","source_sha256":"f3cde4b06fc290ae57c000d4e44826f1b397e2c36f09ae3dac00b9001effcbc5","content_bytes":1449},"prior_html_failure_anchor":{"source_id":"ref01-massive-connector:43bf9ea0-439d-475d-a49e-0e6772fcc487","source_sha256":"98e070dee59bc8c3a3a14d0c2d0c67b56d3ad6577e6aa2d289557ae7f33261b4","content_bytes":157,"reservation_id":"43bf9ea0-439d-475d-a49e-0e6772fcc487","provider_outcome":"UNKNOWN","error_reconciled_event":"ROOT_REF01_CONNECTOR_ERROR_RECONCILED_V1","unknown_event_preserved":"ROOT_REF01_CONNECTOR_UNKNOWN_V1","html_retry_admitted":false},"maximum_connector_calls":1,"maximum_retained_bytes":131072,"maximum_xbrl_file_bytes_for_body_admission":1800000,"v3_maximum_connector_operations":2,"v3_maximum_retained_bytes":3801088,"three_manifest_wave_maximum_retained_bytes":25165824,"body_requires_prior_verified_size_receipt":false,"original_wire_byte_identity_claimed":false},{"route_id":12,"family_id":"REF01","case":"CRD","accession":"0001193125-26-330643","expected_cik":"0000025475","kind":"xbrl_instance_body","method":"GET","provider":"Massive","route":"/v1/reference/sec/filings/0001193125-26-330643/files/crda-20260630_htm.xml","parameters":{},"observed_filename":"crda-20260630_htm.xml","observed_xbrl_instance_url":"https://api.massive.com/v1/reference/sec/filings/0001193125-26-330643/files/crda-20260630_htm.xml","metadata_anchor":{"source_id":"ref01-massive-connector:2d4e9adf-4f48-4957-8f81-82f5341ba97a","source_sha256":"f3cde4b06fc290ae57c000d4e44826f1b397e2c36f09ae3dac00b9001effcbc5","content_bytes":1449},"prior_html_failure_anchor":{"source_id":"ref01-massive-connector:43bf9ea0-439d-475d-a49e-0e6772fcc487","source_sha256":"98e070dee59bc8c3a3a14d0c2d0c67b56d3ad6577e6aa2d289557ae7f33261b4","content_bytes":157,"reservation_id":"43bf9ea0-439d-475d-a49e-0e6772fcc487","provider_outcome":"UNKNOWN","error_reconciled_event":"ROOT_REF01_CONNECTOR_ERROR_RECONCILED_V1","unknown_event_preserved":"ROOT_REF01_CONNECTOR_UNKNOWN_V1","html_retry_admitted":false},"maximum_connector_calls":1,"maximum_retained_bytes":3670016,"maximum_xbrl_file_bytes_for_body_admission":1800000,"v3_maximum_connector_operations":2,"v3_maximum_retained_bytes":3801088,"three_manifest_wave_maximum_retained_bytes":25165824,"body_requires_prior_verified_size_receipt":true,"original_wire_byte_identity_claimed":false}]$manifest$::jsonb)->(route_id-11)
 THEN RAISE EXCEPTION 'EXACT_V3_RESERVED_ROUTE_REQUIRED';END IF;
v_retention_cap:=(reservation->>'reserved_bytes')::bigint;
IF v_retention_cap IS NULL OR v_retention_cap IS DISTINCT FROM (route->>'maximum_retained_bytes')::bigint
 OR reservation->>'content_fetch_admitted' IS DISTINCT FROM (route_id=12)::text THEN RAISE EXCEPTION 'EXACT_V3_RESERVATION_BOUND_REQUIRED';END IF;
v_raw:=decode(current_setting('top3000.ref01_content_base64'),'base64');v_content:=convert_from(v_raw,'UTF8');v_hash:=encode(sha256(v_raw),'hex');
IF v_raw IS NULL OR v_content IS NULL OR octet_length(v_raw) NOT BETWEEN 1 AND v_retention_cap OR v_hash IS DISTINCT FROM current_setting('top3000.ref01_content_sha256')
 OR jsonb_typeof(v_content::jsonb) NOT IN('object','array','string') THEN RAISE EXCEPTION 'COMPLETE_REPRESENTATION_HASH_OR_BOUND_INVALID';END IF;
v_doc:=v_content::jsonb;v_observed_at:=current_setting('top3000.ref01_observed_at')::timestamptz;
v_tool:=current_setting('top3000.ref01_tool_name');outcome_class:=current_setting('top3000.ref01_outcome_class');
IF v_observed_at IS NULL OR v_observed_at<(reservation->>'reserved_at')::timestamptz-interval '5 seconds'
 OR v_observed_at>clock_timestamp()+interval '30 seconds' OR v_tool IS DISTINCT FROM 'mcp__codex_apps__massive_call_api'
 OR outcome_class IS NULL OR outcome_class NOT IN('SUCCESS','DEFINITIVE_FAILURE')
 OR (outcome_class='SUCCESS' AND v_doc->'isError'='true'::jsonb) THEN RAISE EXCEPTION 'OBSERVED_RESPONSE_METADATA_INVALID';END IF;
-- Strict bounded CSV parse of the observed connector representation, not guessed file metadata.
v_size_evidence:=jsonb_build_object('state','NOT_A_FILE_LIST_RESPONSE','body_admission_allowed',false);
IF route_id=11 AND outcome_class='SUCCESS' THEN
 BEGIN
  IF jsonb_typeof(v_doc#>'{structuredContent,result}') IS DISTINCT FROM 'string'
  OR jsonb_typeof(v_doc#>'{content,0,text}') IS DISTINCT FROM 'string'
  OR (v_doc#>>'{structuredContent,result}') IS DISTINCT FROM (v_doc#>>'{content,0,text}')
  THEN RAISE EXCEPTION 'FILELIST_COMPLETE_MATCHING_CSV_COPIES_REQUIRED';END IF;
  v_csv:=v_doc#>>'{structuredContent,result}';
  IF octet_length(v_csv)>16384 OR octet_length(v_csv)<1 THEN RAISE EXCEPTION 'FILELIST_CSV_PARSE_BOUND';END IF;
  v_rows:='[]'::jsonb;v_fields:='{}'::text[];v_field:='';v_quoted:=false;v_after_quote:=false;v_i:=1;
  WHILE v_i<=length(v_csv) LOOP
   v_ch:=substring(v_csv,v_i,1);
   IF v_quoted THEN
    IF v_ch='"' THEN
     IF substring(v_csv,v_i+1,1)='"' THEN v_field:=v_field||'"';v_i:=v_i+1;
     ELSE v_quoted:=false;v_after_quote:=true;END IF;
    ELSE v_field:=v_field||v_ch;END IF;
   ELSE
    IF v_after_quote AND v_ch NOT IN(',',chr(10),chr(13)) THEN RAISE EXCEPTION 'FILELIST_CSV_TRAILING_QUOTE_DATA';END IF;
    IF v_ch=',' THEN v_fields:=array_append(v_fields,v_field);v_field:='';v_after_quote:=false;
    ELSIF v_ch IN(chr(10),chr(13)) THEN
     v_fields:=array_append(v_fields,v_field);v_rows:=v_rows||jsonb_build_array(to_jsonb(v_fields));
     v_fields:='{}'::text[];v_field:='';v_after_quote:=false;
     IF v_ch=chr(13) AND substring(v_csv,v_i+1,1)=chr(10) THEN v_i:=v_i+1;END IF;
    ELSIF v_ch='"' THEN
     IF v_field<>'' THEN RAISE EXCEPTION 'FILELIST_CSV_QUOTE_INSIDE_UNQUOTED_FIELD';END IF;v_quoted:=true;
    ELSE v_field:=v_field||v_ch;END IF;
   END IF;
   v_i:=v_i+1;
  END LOOP;
  IF v_quoted THEN RAISE EXCEPTION 'FILELIST_CSV_UNTERMINATED_QUOTE';END IF;
  IF cardinality(v_fields)>0 OR v_field<>'' OR v_after_quote THEN
   v_fields:=array_append(v_fields,v_field);v_rows:=v_rows||jsonb_build_array(to_jsonb(v_fields));
  END IF;
  IF jsonb_array_length(v_rows)<>2 OR jsonb_array_length(v_rows->0)<>jsonb_array_length(v_rows->1)
  OR jsonb_array_length(v_rows->0)<3 OR jsonb_array_length(v_rows->0)>32
  OR (SELECT count(DISTINCT x.value) FROM jsonb_array_elements_text(v_rows->0) x)<>jsonb_array_length(v_rows->0)
  THEN RAISE EXCEPTION 'EXACT_ONE_FILE_LIST_ROW_AND_UNIQUE_HEADERS_REQUIRED';END IF;
  SELECT jsonb_object_agg((v_rows->0)->>g.n,(v_rows->1)->g.n) INTO v_selected
  FROM generate_series(0,jsonb_array_length(v_rows->0)-1) g(n);
  IF v_selected->>'filing_id' IS DISTINCT FROM '0001193125-26-330643'
  OR v_selected->>'filename' IS DISTINCT FROM 'crda-20260630_htm.xml'
  OR coalesce(v_selected->>'size_bytes','')!~'^[0-9]{1,10}$'
  THEN RAISE EXCEPTION 'EXACT_OBSERVED_XBRL_FILENAME_ACCESSION_SIZE_REQUIRED';END IF;
  v_file_size:=(v_selected->>'size_bytes')::bigint;
  v_size_evidence:=jsonb_build_object('state',CASE WHEN v_file_size BETWEEN 1 AND 1800000 THEN 'VERIFIED_SIZE_WITHIN_BODY_LIMIT' ELSE 'OBSERVED_SIZE_OUTSIDE_BODY_LIMIT' END,
  'body_admission_allowed',v_file_size BETWEEN 1 AND 1800000,'selected_file',v_selected,
  'selected_row_count',1,'header_names',v_rows->0,'csv_utf8_sha256',encode(sha256(convert_to(v_csv,'UTF8')),'hex'),
  'csv_utf8_bytes',octet_length(v_csv),'parser_revision','BOUNDED_STRICT_CSV_NO_NETWORK_V1',
  'maximum_body_source_bytes',1800000,'file_size_is_provider_metadata_not_wire_proof',true);
 EXCEPTION WHEN OTHERS THEN
  v_size_evidence:=jsonb_build_object('state','RETAINED_FILE_LIST_REQUIRES_FORMAT_REVIEW','body_admission_allowed',false,
  'reason',left(SQLERRM,160),'parser_revision','BOUNDED_STRICT_CSV_NO_NETWORK_V1');
 END;
END IF;

v_xml_evidence:=jsonb_build_object('extracted',false,'reason','NO_VERIFIED_XBRL_STRING_SELECTED','original_wire_identity_proven',false);
IF route_id=12 THEN
 v_file_size:=(reservation->'verified_filelist_anchor'->>'reported_file_bytes')::bigint;
 IF v_file_size IS NULL OR v_file_size NOT BETWEEN 1 AND 1800000 THEN RAISE EXCEPTION 'VERIFIED_SMALL_XBRL_RESERVATION_REQUIRED';END IF;
 v_xml_path:=coalesce(nullif(current_setting('top3000.ref01_xml_json_path',true),''),'null')::jsonb;
 IF v_xml_path<>'null'::jsonb THEN
  IF outcome_class IS DISTINCT FROM 'SUCCESS' OR v_xml_path NOT IN('[]'::jsonb,'["structuredContent","result"]'::jsonb,'["content","0","text"]'::jsonb)
   THEN RAISE EXCEPTION 'EXACT_ALLOWED_XML_STRING_PATH_REQUIRED';END IF;
  SELECT coalesce(array_agg(value ORDER BY ord),'{}'::text[]) INTO v_xml_path_array
  FROM jsonb_array_elements_text(v_xml_path) WITH ORDINALITY x(value,ord);
  IF jsonb_typeof(v_doc#>v_xml_path_array) IS DISTINCT FROM 'string' THEN RAISE EXCEPTION 'PLAIN_XML_STRING_REQUIRED';END IF;
  v_xml_text:=v_doc#>>v_xml_path_array;v_xml_bytes:=octet_length(v_xml_text);v_xml_hash:=encode(sha256(convert_to(v_xml_text,'UTF8')),'hex');
  -- Reject declarations before invoking an XML parser; SQL boolean evaluation order is unspecified.
  IF v_xml_bytes IS DISTINCT FROM v_file_size OR v_xml_text~* '<![[:space:]]*(DOCTYPE|ENTITY)'
   THEN RAISE EXCEPTION 'EXACT_BOUNDED_WELL_FORMED_XML_WITHOUT_DTD_REQUIRED';END IF;
  IF NOT xml_is_well_formed_document(v_xml_text) THEN RAISE EXCEPTION 'EXACT_BOUNDED_WELL_FORMED_XML_WITHOUT_DTD_REQUIRED';END IF;
  v_xml_value:=XMLPARSE(DOCUMENT v_xml_text);
  IF ((xpath('local-name(/*)',v_xml_value))[1])::text IS DISTINCT FROM 'xbrl' THEN RAISE EXCEPTION 'XBRL_INSTANCE_ROOT_REQUIRED';END IF;
  IF jsonb_typeof(v_doc#>'{structuredContent,result}')='string' AND jsonb_typeof(v_doc#>'{content,0,text}')='string'
   AND (v_doc#>>'{structuredContent,result}') IS DISTINCT FROM (v_doc#>>'{content,0,text}') THEN RAISE EXCEPTION 'CONNECTOR_XML_COPIES_DIFFER';END IF;
  v_xml_evidence:=jsonb_build_object('extracted',true,'json_path',v_xml_path,'utf8_bytes',v_xml_bytes,'utf8_sha256',v_xml_hash,
   'filelist_reported_bytes',v_file_size,'filelist_size_matches',true,'format_check','WELL_FORMED_XML_XBRL_ROOT_NO_DTD_OR_ENTITY_DECLARATION',
   'original_wire_identity_proven',false,'representation','EXACT_PLAIN_STRING_EXTRACTED_FROM_COMPLETE_CONNECTOR_JSON','additional_body_copy_stored',false);
 END IF;
END IF;
v_sid:='ref01-massive-connector:'||reservation_id::text;
v_uri:='https://api.massive.com'||(route->>'route')||CASE WHEN route_id=11 THEN '?filename=crda-20260630_htm.xml&sort=filename&order=asc&limit=10' ELSE '' END;
v_evidence:=jsonb_build_object('mode','ROOT_DIRECT_CONNECTOR_NO_PIPELINE_ADAPTER_EXECUTED','representation','CONNECTOR_REPRESENTATION_NOT_WIRE',
 'hash_representation','EXACT_UTF8_SERIALIZATION_OF_COMPLETE_RETURNED_CONNECTOR_VALUE','reservation_id',reservation_id,'manifest_sha256','ace46fc9d094f58f18cdfac0b1b1adf9fa1c295a9608b3375eeb3d0a6af1e4b7',
 'request',route,'connector_tool_name',v_tool,'outcome_class',outcome_class,'connector_response_observed_at',v_observed_at,
 'original_wire_bytes_retained',false,'original_wire_hash_verified',false,'provider_http_status_observed',false,'provider_numeric_lexemes_verified',false,
 'connector_internal_retries_observed',false,'current_receipt_is_not_historical_availability',true,'company_mapping_applied',false,
 'expected_cik_is_review_target_not_verified_fact',true,'accession_identity_independently_verified',false,
 'source_document_content_fetched',coalesce((v_xml_evidence->>'extracted')::boolean,false),
 'xbrl_file_size_evidence',v_size_evidence,'xml_extraction',v_xml_evidence,'verified_filelist_anchor',reservation->'verified_filelist_anchor',
 'metadata_anchor',route->'metadata_anchor','prior_html_failure_anchor',route->'prior_html_failure_anchor',
 'prior_html_provider_outcome','UNKNOWN_PRESERVED_NOT_RETRIED','automatic_normalization_or_publication',false,'full_invoice_reconciled',false);
SELECT e.details INTO prior_receipt FROM population_top3000_202608_202609_v1.pipeline_event e
 WHERE e.event='ROOT_REF01_CONNECTOR_RETAINED_V1' AND e.details->>'reservation_id'=reservation_id::text ORDER BY e.at DESC LIMIT 1;
IF prior_receipt IS NOT NULL THEN
 IF prior_receipt->>'source_sha256' IS DISTINCT FROM v_hash OR prior_receipt->>'outcome_class' IS DISTINCT FROM outcome_class
 OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.source_document d WHERE d.source_id=v_sid
 AND d.source_sha256=v_hash AND d.content_text=v_content AND d.content_bytes=octet_length(v_raw)
 AND d.evidence=v_evidence AND d.retrieved_at=v_observed_at AND d.source_uri=v_uri AND d.source_provider='Massive'
 AND d.source_published_at IS NULL AND d.historical_received_at IS NULL
 AND d.license_policy='EXISTING_MASSIVE_SUBSCRIPTION_CONNECTOR_RESPONSE_INTERNAL_USE') THEN RAISE EXCEPTION 'EXISTING_CONNECTOR_RECEIPT_CONFLICT';END IF;
 PERFORM set_config('top3000.ref01_source_id',v_sid,true);RETURN;
END IF;
IF c.hold_reason IS DISTINCT FROM 'ROOT_CONNECTOR_RESERVATION:'||reservation_id::text OR c.reserved_bytes<v_retention_cap
 THEN RAISE EXCEPTION 'EXACT_MANUAL_RESERVATION_HOLD_REQUIRED';END IF;
SELECT coalesce(sum(d.content_bytes),0)+octet_length(v_raw) INTO v_wave_bytes FROM (
 SELECT DISTINCT e.details->>'reservation_id' reservation_id FROM population_top3000_202608_202609_v1.pipeline_event e
 WHERE e.event='ROOT_REF01_CONNECTOR_RESERVED_V1' AND e.details->>'manifest_sha256' IN('1ebab0c5574d665162f5ee13b98a7f8632fca153b95798d500851f00b35170b9','7c6c572fb8e4fed3f4e5136d5928a71a3d819d61fbe0697793086da70b55c07f','ace46fc9d094f58f18cdfac0b1b1adf9fa1c295a9608b3375eeb3d0a6af1e4b7')
) q JOIN population_top3000_202608_202609_v1.source_document d ON d.source_id='ref01-massive-connector:'||q.reservation_id;
SELECT coalesce(sum(d.content_bytes),0)+octet_length(v_raw) INTO v_v3_bytes FROM (
 SELECT DISTINCT e.details->>'reservation_id' reservation_id FROM population_top3000_202608_202609_v1.pipeline_event e
 WHERE e.event='ROOT_REF01_CONNECTOR_RESERVED_V1' AND e.details->>'manifest_sha256'='ace46fc9d094f58f18cdfac0b1b1adf9fa1c295a9608b3375eeb3d0a6af1e4b7'
) q JOIN population_top3000_202608_202609_v1.source_document d ON d.source_id='ref01-massive-connector:'||q.reservation_id;
IF v_wave_bytes>25165824 OR v_v3_bytes>3801088 OR c.stored_bytes+c.reserved_bytes-v_retention_cap+octet_length(v_raw)>c.max_source_bytes
 THEN RAISE EXCEPTION 'V3_WAVE_OR_MISSION_RAW_CAP';END IF;
INSERT INTO population_top3000_202608_202609_v1.source_document
 (source_id,source_sha256,source_uri,source_provider,retrieved_at,source_published_at,historical_received_at,content_text,content_bytes,evidence,license_policy)
VALUES(v_sid,v_hash,v_uri,'Massive',v_observed_at,NULL,NULL,v_content,octet_length(v_raw),v_evidence,'EXISTING_MASSIVE_SUBSCRIPTION_CONNECTOR_RESPONSE_INTERNAL_USE')
ON CONFLICT(source_id) DO NOTHING;
GET DIAGNOSTICS v_fresh=ROW_COUNT;
IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.source_document d WHERE d.source_id=v_sid AND d.source_sha256=v_hash
 AND d.content_text=v_content AND d.content_bytes=octet_length(v_raw) AND d.evidence=v_evidence AND d.retrieved_at=v_observed_at
 AND d.source_uri=v_uri AND d.source_provider='Massive' AND d.source_published_at IS NULL AND d.historical_received_at IS NULL
 AND d.license_policy='EXISTING_MASSIVE_SUBSCRIPTION_CONNECTOR_RESPONSE_INTERNAL_USE') THEN RAISE EXCEPTION 'IMMUTABLE_CONNECTOR_SOURCE_CONFLICT';END IF;
limit_bytes:=nullif(p.budget_evidence#>>'{resource_limits,max_relation_bytes}','')::bigint;
SELECT coalesce(sum(pg_total_relation_size(r.oid)),0) INTO relation_bytes FROM pg_class r JOIN pg_namespace n ON n.oid=r.relnamespace
 WHERE n.nspname='population_top3000_202608_202609_v1' AND r.relkind IN('r','m');
IF limit_bytes IS NULL OR relation_bytes>limit_bytes THEN RAISE EXCEPTION 'MISSION_POST_RETAIN_RELATION_CAP';END IF;
UPDATE population_top3000_202608_202609_v1.acquisition_control SET reserved_bytes=reserved_bytes-v_retention_cap,
 stored_bytes=stored_bytes+octet_length(v_raw),hold_reason=NULL,last_useful_progress_at=clock_timestamp(),updated_at=clock_timestamp() WHERE singleton;
UPDATE population_top3000_202608_202609_v1.pipeline_control SET last_useful_progress_at=clock_timestamp() WHERE singleton;
v_receipt:=jsonb_build_object('reservation_id',reservation_id,'manifest_sha256','ace46fc9d094f58f18cdfac0b1b1adf9fa1c295a9608b3375eeb3d0a6af1e4b7','route_id',route_id,'route',route,
 'source_id',v_sid,'source_sha256',v_hash,'retained_bytes',octet_length(v_raw),'new_source_documents',v_fresh,'outcome_class',outcome_class,
 'source_representation','CONNECTOR_REPRESENTATION_NOT_WIRE','manual_hold_released',true,'cron_resumed',false,'automatic_retry',false,
 'adapter_executed',false,'company_mapping_applied',false,'publication_admitted',false,'xbrl_file_size_evidence',v_size_evidence,
 'xml_extraction',v_xml_evidence,'v3_retained_bytes',v_v3_bytes,'v3_maximum_retained_bytes',3801088,
 'three_manifest_wave_retained_bytes',v_wave_bytes,'three_manifest_wave_maximum_retained_bytes',25165824,
 'prior_html_provider_outcome','UNKNOWN_PRESERVED_NOT_RETRIED','incremental_provider_charge_under_existing_entitlement',0,'full_invoice_reconciled',false);
INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES('ROOT_REF01_CONNECTOR_RETAINED_V1',v_receipt);
PERFORM set_config('top3000.ref01_source_id',v_sid,true);
END $retain$;
SELECT d.source_id,d.source_sha256,d.content_bytes,d.retrieved_at,d.evidence,clock_timestamp() verified_at
FROM population_top3000_202608_202609_v1.source_document d WHERE d.source_id=current_setting('top3000.ref01_source_id')
 AND d.source_sha256=encode(sha256(convert_to(d.content_text,'UTF8')),'hex') AND d.content_bytes=octet_length(d.content_text);
COMMIT;
