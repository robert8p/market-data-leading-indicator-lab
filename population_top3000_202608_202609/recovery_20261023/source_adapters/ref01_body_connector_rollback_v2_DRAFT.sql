-- ROOT ONLY rollback fixture. Creates only transaction-local pg_temp helpers; no provider call.
-- Uses clearly synthetic HTML with exactly the observed byte counts, not a fabricated filing source.
BEGIN;
SET LOCAL statement_timeout='30s';
SET LOCAL lock_timeout='2s';
SELECT pg_advisory_xact_lock(2026083000261022);
UPDATE cron.job SET active=false WHERE jobid=596;
CREATE OR REPLACE FUNCTION pg_temp.ref01_body_reserve_fixture_v2() RETURNS void LANGUAGE plpgsql AS $fixture_reserve$
DECLARE
 c population_top3000_202608_202609_v1.acquisition_control%rowtype;
 p population_top3000_202608_202609_v1.pipeline_control%rowtype;
 m jsonb;route jsonb;route_id integer;reservation_id uuid;reservation jsonb;prior_receipt jsonb;
 relation_bytes bigint;limit_bytes bigint;budget_total numeric;changed integer;
manifest_text constant text:=$manifest$[{"route_id":9,"family_id":"REF01","case":"CRD","accession":"0001193125-26-330643","expected_cik":"0000025475","kind":"filing_document_body","method":"GET","provider":"Massive","route":"/v1/reference/sec/filings/0001193125-26-330643/files/crda-20260630.htm","parameters":{},"observed_filename":"crda-20260630.htm","provider_filelist_bytes":4812375,"canonical_sec_url":"https://www.sec.gov/Archives/edgar/data/25475/000119312526330643/crda-20260630.htm","metadata_anchor":{"route_id":1,"source_id":"ref01-massive-connector:2d4e9adf-4f48-4957-8f81-82f5341ba97a","source_sha256":"f3cde4b06fc290ae57c000d4e44826f1b397e2c36f09ae3dac00b9001effcbc5","content_bytes":1449},"filelist_anchor":{"route_id":2,"source_id":"ref01-massive-connector:dd2499a5-dfb5-4f02-b005-dedd6c3c7cf7","source_sha256":"e8ed0a06b012509c3e575eedf727e671c2c190d579baf23a5268f71f5ce9ffc7","content_bytes":553},"maximum_connector_calls":1,"maximum_retained_bytes":16777216,"aggregate_wave_maximum_retained_bytes":25165824,"maximum_wave_connector_operations":10,"original_wire_byte_identity_claimed":false},{"route_id":10,"family_id":"REF01","case":"DHC","accession":"0001075415-26-000030","expected_cik":"0001075415","kind":"filing_document_body","method":"GET","provider":"Massive","route":"/v1/reference/sec/filings/0001075415-26-000030/files/dhc-20260630.htm","parameters":{},"observed_filename":"dhc-20260630.htm","provider_filelist_bytes":2021465,"canonical_sec_url":"https://www.sec.gov/Archives/edgar/data/1075415/000107541526000030/dhc-20260630.htm","metadata_anchor":{"route_id":3,"source_id":"ref01-massive-connector:649201bd-b1e3-4187-b67c-3584362756a4","source_sha256":"0e884840a1e6cd29f385cd33f53afa8a5055d8e825dc34ba00374f83217467da","content_bytes":1501},"filelist_anchor":{"route_id":4,"source_id":"ref01-massive-connector:6b6e7182-b756-45a4-adee-3dfe292c482c","source_sha256":"71067d6a4907d7e42a778bda75cd22fdda6d954a6dee15acd9eee4290a556ab6","content_bytes":551},"maximum_connector_calls":1,"maximum_retained_bytes":8388608,"aggregate_wave_maximum_retained_bytes":25165824,"maximum_wave_connector_operations":10,"original_wire_byte_identity_claimed":false}]$manifest$;
metadata_anchors constant text:=$anchors$[{"route_id":1,"source_id":"ref01-massive-connector:2d4e9adf-4f48-4957-8f81-82f5341ba97a","source_sha256":"f3cde4b06fc290ae57c000d4e44826f1b397e2c36f09ae3dac00b9001effcbc5","content_bytes":1449},{"route_id":2,"source_id":"ref01-massive-connector:dd2499a5-dfb5-4f02-b005-dedd6c3c7cf7","source_sha256":"e8ed0a06b012509c3e575eedf727e671c2c190d579baf23a5268f71f5ce9ffc7","content_bytes":553},{"route_id":3,"source_id":"ref01-massive-connector:649201bd-b1e3-4187-b67c-3584362756a4","source_sha256":"0e884840a1e6cd29f385cd33f53afa8a5055d8e825dc34ba00374f83217467da","content_bytes":1501},{"route_id":4,"source_id":"ref01-massive-connector:6b6e7182-b756-45a4-adee-3dfe292c482c","source_sha256":"71067d6a4907d7e42a778bda75cd22fdda6d954a6dee15acd9eee4290a556ab6","content_bytes":551},{"route_id":5,"source_id":"ref01-massive-connector:13c44092-9497-467c-a2cf-f0920922dcd5","source_sha256":"b7360bd56c2984909fcdd99314de5a86d5b0a0553034ca86ba5fe47a7ea32c89","content_bytes":1337},{"route_id":6,"source_id":"ref01-massive-connector:499ad68e-518d-4f8c-81ed-f0f39edf481e","source_sha256":"2deffab097ef74b4e0a0cba2d07ded7f8d9c4ac1d7fe583236bff1adaaf50e48","content_bytes":575},{"route_id":7,"source_id":"ref01-massive-connector:a8fb8e52-230f-4db3-8556-5079a284ce0e","source_sha256":"e7a77f22ca8f6f13c69498e7d2c22aaf50767c7c22b759f96751a9eae953a2c4","content_bytes":1449},{"route_id":8,"source_id":"ref01-massive-connector:76cca03d-7a77-4bd7-bdca-01470b6aed43","source_sha256":"bdde2adb70f3b20711b6848aa99388470492d011b036c1cfba687c9555c1b1f3","content_bytes":581}]$anchors$;
metadata_count bigint;metadata_bytes bigint;body_bytes bigint;prior_body_requests bigint;retention_cap bigint;wave_remaining bigint;
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
IF c.hold_reason IS NOT NULL THEN RAISE EXCEPTION 'EXISTING_CONTROL_HOLD_MUST_BE_RECONCILED';END IF;
IF encode(sha256(convert_to(manifest_text,'UTF8')),'hex')<>'7c6c572fb8e4fed3f4e5136d5928a71a3d819d61fbe0697793086da70b55c07f' THEN RAISE EXCEPTION 'MANIFEST_BYTES_CHANGED';END IF;
m:=manifest_text::jsonb;
IF coalesce(current_setting('top3000.ref01_route_id',true),'')!~'^(9|10)$' THEN RAISE EXCEPTION 'EXACT_ONE_OF_TWO_OBSERVED_BODY_ROUTES_REQUIRED';END IF;
route_id:=current_setting('top3000.ref01_route_id')::integer;route:=m->(route_id-9);
IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.catalog_family_contract WHERE family_id='REF01') THEN RAISE EXCEPTION 'REF01_SCOPE_CONTRACT_REQUIRED';END IF;
IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_event e WHERE e.event='ROOT_REF01_CONNECTOR_RESERVED_V1'
 AND e.details->>'manifest_sha256'='7c6c572fb8e4fed3f4e5136d5928a71a3d819d61fbe0697793086da70b55c07f' AND e.details->>'route_id'=route_id::text)
 THEN RAISE EXCEPTION 'ROUTE_ALREADY_RESERVED_NO_AUTOMATIC_REISSUE';END IF;
IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_event e WHERE e.event='ROOT_REF01_CONNECTOR_RESERVED_V1'
 AND NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_event closed WHERE closed.event='ROOT_REF01_CONNECTOR_RETAINED_V1'
  AND closed.details->>'reservation_id'=e.details->>'reservation_id')) THEN RAISE EXCEPTION 'EARLIER_MANUAL_CONNECTOR_OUTCOME_UNRESOLVED';END IF;
-- The first eight actual responses are immutable dependencies, not just expected source names.
SELECT count(*),coalesce(sum(d.content_bytes),0) INTO metadata_count,metadata_bytes
FROM jsonb_to_recordset(metadata_anchors::jsonb) AS a(source_id text,source_sha256 text,content_bytes bigint,route_id integer)
JOIN population_top3000_202608_202609_v1.source_document d ON d.source_id=a.source_id AND d.source_sha256=a.source_sha256
 AND d.content_bytes=a.content_bytes AND octet_length(d.content_text)=a.content_bytes
 AND encode(sha256(convert_to(d.content_text,'UTF8')),'hex')=a.source_sha256
WHERE EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_event e
 WHERE e.event='ROOT_REF01_CONNECTOR_RETAINED_V1' AND e.details->>'manifest_sha256'='1ebab0c5574d665162f5ee13b98a7f8632fca153b95798d500851f00b35170b9'
 AND e.details->>'source_id'=a.source_id AND e.details->>'source_sha256'=a.source_sha256 AND e.details->>'outcome_class'='SUCCESS');
IF metadata_count IS DISTINCT FROM 8::bigint OR metadata_bytes IS DISTINCT FROM 7996::bigint
 OR (SELECT count(*) FROM population_top3000_202608_202609_v1.pipeline_event e WHERE e.event='ROOT_REF01_CONNECTOR_RESERVED_V1' AND e.details->>'manifest_sha256'='1ebab0c5574d665162f5ee13b98a7f8632fca153b95798d500851f00b35170b9')<>8
 THEN RAISE EXCEPTION 'EXACT_EIGHT_METADATA_RECEIPTS_REQUIRED';END IF;
SELECT count(*) INTO prior_body_requests FROM population_top3000_202608_202609_v1.pipeline_event e
 WHERE e.event='ROOT_REF01_CONNECTOR_RESERVED_V1' AND e.details->>'manifest_sha256'='7c6c572fb8e4fed3f4e5136d5928a71a3d819d61fbe0697793086da70b55c07f';
SELECT coalesce(sum((e.details->>'retained_bytes')::bigint),0) INTO body_bytes FROM population_top3000_202608_202609_v1.pipeline_event e
 WHERE e.event='ROOT_REF01_CONNECTOR_RETAINED_V1' AND e.details->>'manifest_sha256'='7c6c572fb8e4fed3f4e5136d5928a71a3d819d61fbe0697793086da70b55c07f';
IF prior_body_requests IS DISTINCT FROM (route_id-9)::bigint THEN RAISE EXCEPTION 'BODY_WAVE_SEQUENCE_OR_REQUEST_LIMIT';END IF;
wave_remaining:=25165824-metadata_bytes-body_bytes;
retention_cap:=least((route->>'maximum_retained_bytes')::bigint,wave_remaining);
IF retention_cap<(route->>'provider_filelist_bytes')::bigint OR retention_cap<=0 THEN RAISE EXCEPTION 'AGGREGATE_24_MIB_WAVE_HEADROOM_REQUIRED';END IF;
IF p.budget_verified_at IS NULL OR coalesce((p.budget_evidence->>'provider_admissions_allowed')::boolean,false) IS NOT TRUE
 OR (coalesce(p.budget_evidence->'allowed_providers','[]'::jsonb) ? 'Massive') IS NOT TRUE THEN RAISE EXCEPTION 'CURRENT_MASSIVE_ADMISSION_REQUIRED';END IF;
limit_bytes:=nullif(p.budget_evidence#>>'{resource_limits,max_relation_bytes}','')::bigint;
SELECT coalesce(sum(pg_total_relation_size(r.oid)),0) INTO relation_bytes FROM pg_class r JOIN pg_namespace n ON n.oid=r.relnamespace
 WHERE n.nspname='population_top3000_202608_202609_v1' AND r.relkind IN('r','m');
IF limit_bytes IS NULL OR relation_bytes>=limit_bytes THEN RAISE EXCEPTION 'MISSION_RELATION_CAP';END IF;
budget_total:=p.actual_usd+p.accrued_usd+p.unavoidable_usd+p.remaining_uncommitted_usd+p.repair_egress_reserve_usd+p.reserved_request_usd;
IF budget_total IS NULL OR budget_total>250 OR c.provider_requests>=c.max_provider_requests OR c.http_invocations>=c.max_http_invocations
 OR c.stored_bytes+c.reserved_bytes+retention_cap>c.max_source_bytes THEN RAISE EXCEPTION 'BUDGET_REQUEST_OR_RAW_CAP';END IF;
IF c.next_provider_at>clock_timestamp() THEN RAISE EXCEPTION 'PROVIDER_PACING_WINDOW_NOT_READY';END IF;
reservation_id:=gen_random_uuid();
reservation:=jsonb_build_object('reservation_id',reservation_id,'manifest_sha256','7c6c572fb8e4fed3f4e5136d5928a71a3d819d61fbe0697793086da70b55c07f','route_id',route_id,'route',route,
 'controller',c.owner_id,'owner_fence',c.owner_fence,'stop_generation',c.stop_generation,'scope_sha256',c.scope_sha256,
 'reserved_at',clock_timestamp(),'review_after',clock_timestamp()+interval '5 minutes','reserved_bytes',retention_cap,'aggregate_wave_maximum_retained_bytes',25165824,'aggregate_wave_previously_retained_bytes',metadata_bytes+body_bytes,'aggregate_wave_remaining_before_reservation',wave_remaining,'prior_metadata_manifest_sha256','1ebab0c5574d665162f5ee13b98a7f8632fca153b95798d500851f00b35170b9','content_fetch_admission_version',2,'reserved_incremental_usd',0,
 'mode','ROOT_DIRECT_CONNECTOR_NO_PIPELINE_ADAPTER_EXECUTED','source_representation','CONNECTOR_REPRESENTATION_NOT_WIRE',
 'provider_counter_increment',1,'http_counter_increment',1,'http_counter_meaning','CONSERVATIVE_CONNECTOR_OPERATION_ALLOWANCE_NOT_OBSERVED_EDGE_INVOCATION',
 'wire_request_count_observed',false,'connector_internal_retries_observed',false,'automatic_retry',false,'automatic_pagination',false,
 'automatic_normalization_or_publication',false,'content_fetch_admitted',true,'invoice_reconciled',false,
 'response_byte_limit_kind','RETAINED_REPRESENTATION_CAP; connector transport cap not independently controllable',
 'max_relation_bytes',limit_bytes,'budget_total_at_reservation',budget_total);
UPDATE population_top3000_202608_202609_v1.acquisition_control SET provider_requests=provider_requests+1,http_invocations=http_invocations+1,
 reserved_bytes=reserved_bytes+retention_cap,next_provider_at=clock_timestamp()+interval '500 milliseconds',
 hold_reason='ROOT_CONNECTOR_RESERVATION:'||reservation_id::text,updated_at=clock_timestamp() WHERE singleton;
INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES('ROOT_REF01_CONNECTOR_RESERVED_V1',reservation);
PERFORM set_config('top3000.ref01_reservation_id',reservation_id::text,true);
END
$fixture_reserve$;
CREATE OR REPLACE FUNCTION pg_temp.ref01_body_retain_fixture_v2() RETURNS void LANGUAGE plpgsql AS $fixture_retain$
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
 'automatic_normalization_or_publication',false,'full_invoice_reconciled',false);
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
PERFORM set_config('top3000.ref01_source_id',sid,true);
END
$fixture_retain$;
CREATE TEMP TABLE ref01_body_test_result(receipt jsonb) ON COMMIT DROP;
DO $test$
DECLARE
 c0 population_top3000_202608_202609_v1.acquisition_control%rowtype;
 c1 population_top3000_202608_202609_v1.acquisition_control%rowtype;
 r jsonb; html text; envelope text; observed timestamptz; h text; first_sid text; first_reservation text;
 n bigint; old_documents bigint; v_route integer; v_expected bigint; v_negative integer:=0; v_cap bigint;
BEGIN
 SELECT * INTO c0 FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton;
 SELECT count(*) INTO old_documents FROM population_top3000_202608_202609_v1.source_document;
 -- Route10 cannot jump ahead of the audited sequence; its failed statement changes no counters.
 PERFORM set_config('top3000.ref01_route_id','10',true);
 BEGIN
  PERFORM pg_temp.ref01_body_reserve_fixture_v2();
  RAISE EXCEPTION 'TEST_EXPECTED_SEQUENCE_REJECTION';
 EXCEPTION WHEN raise_exception THEN
  IF SQLERRM<>'BODY_WAVE_SEQUENCE_OR_REQUEST_LIMIT' THEN RAISE;END IF;v_negative:=v_negative+1;
 END;
 FOR v_route IN 9..10 LOOP
  IF c0.next_provider_at>clock_timestamp() THEN PERFORM pg_sleep(0.51);END IF;
  IF v_route=10 THEN PERFORM pg_sleep(0.51);END IF;
  PERFORM set_config('top3000.ref01_route_id',v_route::text,true);
  PERFORM pg_temp.ref01_body_reserve_fixture_v2();
  SELECT details INTO r FROM population_top3000_202608_202609_v1.pipeline_event
   WHERE event='ROOT_REF01_CONNECTOR_RESERVED_V1' AND details->>'reservation_id'=current_setting('top3000.ref01_reservation_id') ORDER BY at DESC LIMIT 1;
  v_expected:=(r#>>'{route,provider_filelist_bytes}')::bigint;v_cap:=(r->>'reserved_bytes')::bigint;
  IF (v_route=9 AND v_cap<>16777216) OR (v_route=10 AND v_cap>8388608)
   OR v_cap>25165824-(r->>'aggregate_wave_previously_retained_bytes')::bigint THEN RAISE EXCEPTION 'TEST_RETENTION_RESERVATION_BOUND';END IF;
  -- Synthetic, compressible, full HTML wrapper. The fixture asserts bytes and hashing only.
  html:='<html><body>ROLLBACK_ONLY_SYNTHETIC_FIXTURE:'||repeat('x',(v_expected-octet_length('<html><body>ROLLBACK_ONLY_SYNTHETIC_FIXTURE:</body></html>'))::integer)||'</body></html>';
  IF octet_length(html)<>v_expected THEN RAISE EXCEPTION 'TEST_FIXTURE_BYTE_LENGTH';END IF;
  envelope:=jsonb_build_object('content',jsonb_build_array(jsonb_build_object('type','text','text',html)),
   'structuredContent',jsonb_build_object('result',html),'isError',false,'fixture','ROLLBACK_ONLY_SYNTHETIC_NOT_A_FILING')::text;
  observed:=clock_timestamp();h:=encode(sha256(convert_to(envelope,'UTF8')),'hex');
  PERFORM set_config('top3000.ref01_content_base64',encode(convert_to(envelope,'UTF8'),'base64'),true);
  PERFORM set_config('top3000.ref01_content_sha256',h,true);
  PERFORM set_config('top3000.ref01_observed_at',observed::text,true);
  PERFORM set_config('top3000.ref01_tool_name','mcp__codex_apps__massive_call_api',true);
  PERFORM set_config('top3000.ref01_outcome_class','SUCCESS',true);
  PERFORM set_config('top3000.ref01_html_json_path','["structuredContent","result"]',true);
  IF v_route=9 THEN
   -- A selected non-HTML metadata path must not be accepted as the source body.
   PERFORM set_config('top3000.ref01_html_json_path','["fixture"]',true);
   BEGIN
    PERFORM pg_temp.ref01_body_retain_fixture_v2();RAISE EXCEPTION 'TEST_EXPECTED_PATH_REJECTION';
   EXCEPTION WHEN raise_exception THEN
    IF SQLERRM<>'EXACT_ALLOWED_HTML_STRING_PATH_REQUIRED' THEN RAISE;END IF;v_negative:=v_negative+1;
   END;
   PERFORM set_config('top3000.ref01_html_json_path','["structuredContent","result"]',true);
   -- A wrong whole-response hash leaves the reservation/counters unchanged.
   PERFORM set_config('top3000.ref01_content_sha256',repeat('0',64),true);
   BEGIN
    PERFORM pg_temp.ref01_body_retain_fixture_v2();RAISE EXCEPTION 'TEST_EXPECTED_HASH_REJECTION';
   EXCEPTION WHEN raise_exception THEN
    IF SQLERRM<>'COMPLETE_REPRESENTATION_HASH_OR_BOUND_INVALID' THEN RAISE;END IF;v_negative:=v_negative+1;
   END;
   PERFORM set_config('top3000.ref01_content_sha256',h,true);
  END IF;
  PERFORM pg_temp.ref01_body_retain_fixture_v2();
  PERFORM pg_temp.ref01_body_retain_fixture_v2(); -- exact replay: no duplicate source/counter/receipt
  IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.source_document d
    WHERE d.source_id=current_setting('top3000.ref01_source_id') AND d.source_sha256=h
    AND d.evidence#>>'{html_extraction,utf8_sha256}'=encode(sha256(convert_to(html,'UTF8')),'hex')
    AND d.evidence#>>'{html_extraction,utf8_bytes}'=v_expected::text
    AND d.evidence#>>'{html_extraction,original_wire_identity_proven}'='false'
    AND d.source_published_at IS NULL AND d.historical_received_at IS NULL)
   THEN RAISE EXCEPTION 'TEST_SEPARATE_RESPONSE_AND_HTML_LINEAGE';END IF;
  BEGIN
   PERFORM pg_temp.ref01_body_reserve_fixture_v2();RAISE EXCEPTION 'TEST_EXPECTED_REISSUE_REJECTION';
  EXCEPTION WHEN raise_exception THEN
   IF SQLERRM<>'ROUTE_ALREADY_RESERVED_NO_AUTOMATIC_REISSUE' THEN RAISE;END IF;v_negative:=v_negative+1;
  END;
 END LOOP;
 SELECT * INTO c1 FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton;
 IF c1.provider_requests<>c0.provider_requests+2 OR c1.http_invocations<>c0.http_invocations+2
  OR c1.reserved_bytes<>c0.reserved_bytes OR c1.hold_reason IS NOT NULL OR c1.owner_fence<>c0.owner_fence OR c1.stop_generation<>c0.stop_generation
  OR c1.stored_bytes-c0.stored_bytes+7996>25165824
  OR (SELECT count(*) FROM population_top3000_202608_202609_v1.source_document)<>old_documents+2
  OR v_negative<>5 THEN RAISE EXCEPTION 'TEST_COUNTER_REPLAY_OR_AGGREGATE_BOUND';END IF;
 INSERT INTO ref01_body_test_result VALUES(jsonb_build_object('result','PASS_EXACT_TWO_BODY_CONNECTOR_ACCOUNTING_AND_SEPARATE_HTML_LINEAGE',
  'synthetic_fixture',true,'provider_calls',0,'new_sources_inside_rollback',2,'provider_counter_delta_inside_rollback',2,
  'http_counter_delta_inside_rollback',2,'prior_metadata_bytes',7996,'new_representation_bytes_inside_rollback',c1.stored_bytes-c0.stored_bytes,
  'wave_bytes_inside_rollback',c1.stored_bytes-c0.stored_bytes+7996,'negative_guards',v_negative,'all_changes_rolled_back',true));
END $test$;
SELECT receipt FROM ref01_body_test_result;
ROLLBACK;
