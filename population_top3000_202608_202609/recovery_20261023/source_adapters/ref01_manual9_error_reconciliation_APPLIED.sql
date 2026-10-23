-- ROOT ONLY v2 DRAFT. Retain a complete connector representation for the exact reserved observed filename.
-- Hash/extraction checks never claim that the connector exposed original network bytes.
-- Root must pass the complete serialization via a structured tool argument; never a huge shell argument.
-- Uses no new worker, RPC, adapter binding, fake pipeline attempt or automatic retry.
BEGIN;
SELECT set_config('top3000.ref01_reservation_id','43bf9ea0-439d-475d-a49e-0e6772fcc487',true);
SELECT set_config('top3000.ref01_content_base64','eyJjb250ZW50IjpbeyJ0eXBlIjoidGV4dCIsInRleHQiOiJ0b29sIGNhbGwgZXJyb3I6IHRvb2wgY2FsbCBmYWlsZWQgZm9yIGBjb2RleF9hcHBzL21hc3NpdmUuY2FsbF9hcGlgXG5cbkNhdXNlZCBieTpcbiAgICBUcmFuc3BvcnQgY2xvc2VkIn1dLCJpc0Vycm9yIjp0cnVlfQ==',true);
SELECT set_config('top3000.ref01_content_sha256','98e070dee59bc8c3a3a14d0c2d0c67b56d3ad6577e6aa2d289557ae7f33261b4',true);
SELECT set_config('top3000.ref01_observed_at','2026-10-23T14:58:48.465Z',true);
SELECT set_config('top3000.ref01_tool_name','mcp__codex_apps__massive_call_api',true);
SELECT set_config('top3000.ref01_outcome_class','DEFINITIVE_FAILURE',true);
SELECT set_config('top3000.ref01_html_json_path','null',true);
SET LOCAL statement_timeout='10s';
SET LOCAL lock_timeout='2s';
SELECT pg_advisory_xact_lock(2026083000261022);
-- Root sets reservation_id, content_base64, content_sha256, observed_at, tool_name,
-- optional html_json_path (JSON array; see notes), and outcome_class ('SUCCESS' or 'DEFINITIVE_FAILURE') as top3000.ref01_* GUCs after BEGIN.
-- content_base64 encodes the unchanged complete JSON serialization of the returned connector value.
-- Do not pass a rendered/truncated tool-output excerpt or a reformatted jsonb rendering.
DO $retain$
DECLARE
 c population_top3000_202608_202609_v1.acquisition_control%rowtype;
 p population_top3000_202608_202609_v1.pipeline_control%rowtype;
 m jsonb;route jsonb;route_id integer;reservation_id uuid;reservation jsonb;prior_receipt jsonb;
 relation_bytes bigint;limit_bytes bigint;budget_total numeric;changed integer;
raw_bytes bytea;content text;doc jsonb;html_path jsonb;html_path_array text[];html text;html_sha text;html_bytes bigint;html_evidence jsonb;wave_bytes bigint;reserved_cap bigint;h text;sid text;observed_at timestamptz;tool_name text;outcome_class text;v_evidence jsonb;receipt jsonb;fresh integer;
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
SELECT details INTO reservation FROM population_top3000_202608_202609_v1.pipeline_event WHERE event='ROOT_REF01_CONNECTOR_RESERVED_V1' AND details->>'reservation_id'=reservation_id::text ORDER BY at DESC LIMIT 1;
IF reservation IS NULL OR reservation->>'manifest_sha256'<>'7c6c572fb8e4fed3f4e5136d5928a71a3d819d61fbe0697793086da70b55c07f' OR reservation->>'controller' IS DISTINCT FROM c.owner_id
 OR reservation->>'owner_fence' IS DISTINCT FROM c.owner_fence::text OR reservation->>'stop_generation' IS DISTINCT FROM c.stop_generation::text
 OR reservation->>'scope_sha256' IS DISTINCT FROM c.scope_sha256 THEN RAISE EXCEPTION 'RESERVATION_OWNER_OR_MANIFEST_MISMATCH';END IF;
route:=reservation->'route';
reserved_cap:=(reservation->>'reserved_bytes')::bigint;
IF route->>'kind' IS DISTINCT FROM 'filing_document_body' OR route->>'route_id' NOT IN('9','10')
 OR reserved_cap IS NULL OR reserved_cap<=0 OR reserved_cap>(route->>'maximum_retained_bytes')::bigint
 OR reservation->>'content_fetch_admitted' IS DISTINCT FROM 'true'
 OR reservation->>'aggregate_wave_maximum_retained_bytes' IS DISTINCT FROM '25165824'
 THEN RAISE EXCEPTION 'EXACT_V2_BODY_RESERVATION_REQUIRED';END IF;
raw_bytes:=decode(current_setting('top3000.ref01_content_base64'),'base64');content:=convert_from(raw_bytes,'UTF8');
h:=encode(sha256(raw_bytes),'hex');
IF octet_length(raw_bytes) NOT BETWEEN 1 AND reserved_cap OR h IS DISTINCT FROM current_setting('top3000.ref01_content_sha256')
 OR jsonb_typeof(content::jsonb) NOT IN('object','array','string') THEN RAISE EXCEPTION 'COMPLETE_REPRESENTATION_HASH_OR_BOUND_INVALID';END IF;
doc:=content::jsonb;
observed_at:=current_setting('top3000.ref01_observed_at')::timestamptz;tool_name:=current_setting('top3000.ref01_tool_name');outcome_class:=current_setting('top3000.ref01_outcome_class');
IF observed_at<(reservation->>'reserved_at')::timestamptz-interval '5 seconds' OR observed_at>clock_timestamp()+interval '30 seconds'
 OR tool_name IS DISTINCT FROM 'mcp__codex_apps__massive_call_api' OR outcome_class IS NULL OR outcome_class NOT IN('SUCCESS','DEFINITIVE_FAILURE')
 OR (outcome_class='SUCCESS' AND content::jsonb->'isError'='true'::jsonb) THEN RAISE EXCEPTION 'OBSERVED_RESPONSE_METADATA_INVALID';END IF;
-- Optional exact string extraction from the preserved MCP representation, not a reconstructed HTTP body.
-- Root chooses one of three known literal paths only after inspecting the complete returned value.
html_path:=coalesce(nullif(current_setting('top3000.ref01_html_json_path',true),''),'null')::jsonb;
html_evidence:=jsonb_build_object('extracted',false,'reason','NO_VERIFIED_PLAIN_HTML_STRING_SELECTED',
 'provider_filelist_bytes',(route->>'provider_filelist_bytes')::bigint,'original_wire_identity_proven',false);
IF html_path<>'null'::jsonb THEN
 IF outcome_class IS DISTINCT FROM 'SUCCESS' OR html_path NOT IN ('[]'::jsonb,'["structuredContent","result"]'::jsonb,'["content","0","text"]'::jsonb)
  THEN RAISE EXCEPTION 'EXACT_ALLOWED_HTML_STRING_PATH_REQUIRED';END IF;
 SELECT coalesce(array_agg(value ORDER BY ord),'{}'::text[]) INTO html_path_array FROM jsonb_array_elements_text(html_path) WITH ORDINALITY AS x(value,ord);
 IF jsonb_typeof(doc#>html_path_array) IS DISTINCT FROM 'string' THEN RAISE EXCEPTION 'PLAIN_HTML_STRING_REQUIRED';END IF;
 html:=doc#>>html_path_array;html_bytes:=octet_length(html);html_sha:=encode(sha256(convert_to(html,'UTF8')),'hex');
 IF html_bytes IS DISTINCT FROM (route->>'provider_filelist_bytes')::bigint
  OR html !~* '^\s*(<\?xml[^>]*>\s*)?(<!DOCTYPE[^>]*>\s*)?<html([[:space:]>])'
  OR html !~* '</html>[[:space:]]*$' THEN RAISE EXCEPTION 'EXTRACTED_HTML_SIZE_OR_FULL_DOCUMENT_MISMATCH';END IF;
 IF jsonb_typeof(doc#>'{structuredContent,result}')='string' AND jsonb_typeof(doc#>'{content,0,text}')='string'
  AND (doc#>>'{structuredContent,result}') IS DISTINCT FROM (doc#>>'{content,0,text}')
  THEN RAISE EXCEPTION 'CONNECTOR_PLAIN_TEXT_COPIES_DIFFER';END IF;
 html_evidence:=jsonb_build_object('extracted',true,'json_path',html_path,'utf8_bytes',html_bytes,'utf8_sha256',html_sha,
  'provider_filelist_bytes',(route->>'provider_filelist_bytes')::bigint,'filelist_size_matches',true,
  'format_check','FULL_HTML_OPEN_CLOSE_WITH_OPTIONAL_XML_AND_DOCTYPE','original_wire_identity_proven',false,
  'representation','EXACT_PLAIN_STRING_EXTRACTED_FROM_COMPLETE_CONNECTOR_JSON','additional_body_copy_stored',false);
END IF;
sid:='ref01-massive-connector:'||reservation_id::text;
SELECT details INTO prior_receipt FROM population_top3000_202608_202609_v1.pipeline_event WHERE event='ROOT_REF01_CONNECTOR_RETAINED_V1' AND details->>'reservation_id'=reservation_id::text ORDER BY at DESC LIMIT 1;
IF prior_receipt IS NOT NULL THEN
 IF prior_receipt->>'source_sha256' IS DISTINCT FROM h OR prior_receipt->>'outcome_class' IS DISTINCT FROM outcome_class
  OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.source_document d WHERE d.source_id=sid AND d.source_sha256=h AND d.content_text=content AND d.content_bytes=octet_length(raw_bytes) AND d.retrieved_at=observed_at AND d.evidence->>'connector_tool_name'=tool_name AND d.evidence->'request'=route AND d.evidence->>'outcome_class'=outcome_class AND d.evidence->'html_extraction'=html_evidence AND d.source_provider='Massive' AND d.source_published_at IS NULL AND d.historical_received_at IS NULL)
 THEN RAISE EXCEPTION 'EXISTING_CONNECTOR_RECEIPT_CONFLICT';END IF;
 PERFORM set_config('top3000.ref01_source_id',sid,true);RETURN;
END IF;
IF c.hold_reason IS DISTINCT FROM 'ROOT_CONNECTOR_RESERVATION:'||reservation_id::text OR c.reserved_bytes<(reservation->>'reserved_bytes')::bigint THEN RAISE EXCEPTION 'EXACT_MANUAL_RESERVATION_HOLD_REQUIRED';END IF;
SELECT 7996+coalesce(sum((e.details->>'retained_bytes')::bigint),0)+octet_length(raw_bytes) INTO wave_bytes
FROM population_top3000_202608_202609_v1.pipeline_event e WHERE e.event='ROOT_REF01_CONNECTOR_RETAINED_V1'
 AND e.details->>'manifest_sha256'='7c6c572fb8e4fed3f4e5136d5928a71a3d819d61fbe0697793086da70b55c07f';
IF wave_bytes>25165824 OR c.stored_bytes+c.reserved_bytes-reserved_cap+octet_length(raw_bytes)>c.max_source_bytes
 THEN RAISE EXCEPTION 'AGGREGATE_WAVE_OR_MISSION_RAW_CAP';END IF;
v_evidence:=jsonb_build_object('mode','ROOT_DIRECT_CONNECTOR_NO_PIPELINE_ADAPTER_EXECUTED','representation','CONNECTOR_REPRESENTATION_NOT_WIRE',
 'hash_representation','EXACT_UTF8_SERIALIZATION_OF_COMPLETE_RETURNED_CONNECTOR_VALUE','reservation_id',reservation_id,'manifest_sha256','7c6c572fb8e4fed3f4e5136d5928a71a3d819d61fbe0697793086da70b55c07f',
 'request',route,'connector_tool_name',tool_name,'outcome_class',outcome_class,'connector_response_observed_at',observed_at,
 'original_wire_bytes_retained',false,'original_wire_hash_verified',false,'provider_http_status_observed',false,'provider_numeric_lexemes_verified',false,
 'connector_internal_retries_observed',false,'current_receipt_is_not_historical_availability',true,'company_mapping_applied',false,
 'expected_cik_is_review_target_not_verified_fact',true,'accession_identity_independently_verified',false,'source_document_content_fetched',coalesce((html_evidence->>'extracted')::boolean,false),'html_extraction',html_evidence,
 'filing_body_route_requested',true,'metadata_anchor',route->'metadata_anchor','filelist_anchor',route->'filelist_anchor',
 'canonical_sec_url',route->>'canonical_sec_url','acceptance_timezone_interpretation_applied',false,
 'automatic_normalization_or_publication',false,'full_invoice_reconciled',false,'error_scope','CONNECTOR_TRANSPORT','provider_request_outcome','UNKNOWN','remote_request_cancelled','UNKNOWN','no_auto_retry',true,'outcome_class_scope','TERMINAL_CONNECTOR_RESULT_ONLY','database_writer_dispatched',false,'provider_lease_dispatched',false,'filing_body_acquired',false);
INSERT INTO population_top3000_202608_202609_v1.source_document
 (source_id,source_sha256,source_uri,source_provider,retrieved_at,source_published_at,historical_received_at,content_text,content_bytes,evidence,license_policy)
VALUES(sid,h,'https://api.massive.com'||(route->>'route')||CASE WHEN route->>'kind'='files_sequence1' THEN '?sequence=1&sort=sequence&order=asc&limit=10' ELSE '' END,
 'Massive',observed_at,NULL,NULL,content,octet_length(raw_bytes),v_evidence,'EXISTING_MASSIVE_SUBSCRIPTION_CONNECTOR_RESPONSE_INTERNAL_USE')
ON CONFLICT(source_id) DO NOTHING;
GET DIAGNOSTICS fresh=ROW_COUNT;
IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.source_document d WHERE d.source_id=sid AND d.source_sha256=h AND d.content_text=content
 AND d.content_bytes=octet_length(raw_bytes) AND d.evidence=v_evidence AND d.retrieved_at=observed_at AND d.source_provider='Massive' AND d.source_uri='https://api.massive.com'||(route->>'route')||CASE WHEN route->>'kind'='files_sequence1' THEN '?sequence=1&sort=sequence&order=asc&limit=10' ELSE '' END AND d.license_policy='EXISTING_MASSIVE_SUBSCRIPTION_CONNECTOR_RESPONSE_INTERNAL_USE' AND d.source_published_at IS NULL AND d.historical_received_at IS NULL)
 THEN RAISE EXCEPTION 'IMMUTABLE_CONNECTOR_SOURCE_CONFLICT';END IF;
limit_bytes:=nullif(p.budget_evidence#>>'{resource_limits,max_relation_bytes}','')::bigint;
SELECT coalesce(sum(pg_total_relation_size(r.oid)),0) INTO relation_bytes FROM pg_class r JOIN pg_namespace n ON n.oid=r.relnamespace
 WHERE n.nspname='population_top3000_202608_202609_v1' AND r.relkind IN('r','m');
IF limit_bytes IS NULL OR relation_bytes>limit_bytes THEN RAISE EXCEPTION 'MISSION_POST_RETAIN_RELATION_CAP';END IF;
UPDATE population_top3000_202608_202609_v1.acquisition_control SET reserved_bytes=reserved_bytes-(reservation->>'reserved_bytes')::bigint,
 stored_bytes=stored_bytes+octet_length(raw_bytes),hold_reason=NULL,last_useful_progress_at=clock_timestamp(),updated_at=clock_timestamp() WHERE singleton;
UPDATE population_top3000_202608_202609_v1.pipeline_control SET last_useful_progress_at=clock_timestamp() WHERE singleton;
receipt:=jsonb_build_object('reservation_id',reservation_id,'manifest_sha256','7c6c572fb8e4fed3f4e5136d5928a71a3d819d61fbe0697793086da70b55c07f','route_id',route->'route_id','route',route,
 'source_id',sid,'source_sha256',h,'retained_bytes',octet_length(raw_bytes),'new_source_documents',fresh,'outcome_class',outcome_class,
 'source_representation','CONNECTOR_REPRESENTATION_NOT_WIRE','manual_hold_released',true,'cron_resumed',false,'automatic_retry',false,
 'adapter_executed',false,'company_mapping_applied',false,'publication_admitted',false,'html_extraction',html_evidence,'aggregate_wave_retained_bytes',wave_bytes,'aggregate_wave_maximum_retained_bytes',25165824,'incremental_provider_charge_under_existing_entitlement',0,'full_invoice_reconciled',false);
INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES('ROOT_REF01_CONNECTOR_RETAINED_V1',receipt);
INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES('ROOT_REF01_CONNECTOR_ERROR_RECONCILED_V1',jsonb_build_object('reservation_id',reservation_id,'source_id',sid,'source_sha256',h,'error_scope','CONNECTOR_TRANSPORT','provider_request_outcome','UNKNOWN','remote_request_cancelled','UNKNOWN','no_auto_retry',true,'consumed_request_counters_preserved',true,'unused_raw_reservation_released',reserved_cap,'release_basis','TERMINAL_COMPLETE_CONNECTOR_ERROR_RETURNED; MANUAL_GET_HAS_NO_DB_WRITER_OR_PROVIDER_LEASE_ABLE_TO_COMMIT_LATE_SOURCE; NO_PROVIDER_BODY_RETURNED','filing_body_acquired',false,'unknown_event_preserved',true));
PERFORM set_config('top3000.ref01_source_id',sid,true);
END $retain$;
SELECT d.source_id,d.source_sha256,d.content_bytes,d.retrieved_at,d.evidence,clock_timestamp() verified_at
FROM population_top3000_202608_202609_v1.source_document d WHERE d.source_id=current_setting('top3000.ref01_source_id')
 AND d.source_sha256=encode(sha256(convert_to(d.content_text,'UTF8')),'hex') AND d.content_bytes=octet_length(d.content_text);
COMMIT;
