-- ROOT ONLY. No network/provider calls. All fixture writes are rolled back.
-- The exact current owner, source anchors, paused cron and drained leases are required.
-- The three pg_temp helpers contain the unchanged production-candidate DO bodies.
BEGIN;
SET LOCAL statement_timeout='30s';
SET LOCAL lock_timeout='2s';
SET LOCAL timezone='UTC';
SELECT pg_advisory_xact_lock(2026083000261022);
CREATE OR REPLACE FUNCTION pg_temp.ref01_crd_xbrl_reserve_fixture_v3() RETURNS void LANGUAGE plpgsql AS $fixture_reserve$

DECLARE
 c population_top3000_202608_202609_v1.acquisition_control%rowtype;
 p population_top3000_202608_202609_v1.pipeline_control%rowtype;
 m jsonb;route jsonb;route_id integer;reservation_id uuid;reservation jsonb;prior_receipt jsonb;
 relation_bytes bigint;limit_bytes bigint;budget_total numeric;changed integer;
v_manifest_text constant text:=$manifest$[{"route_id":11,"family_id":"REF01","case":"CRD","accession":"0001193125-26-330643","expected_cik":"0000025475","kind":"xbrl_file_size_metadata","method":"GET","provider":"Massive","route":"/v1/reference/sec/filings/0001193125-26-330643/files","parameters":{"filename":"crda-20260630_htm.xml","sort":"filename","order":"asc","limit":10},"observed_filename":"crda-20260630_htm.xml","observed_xbrl_instance_url":"https://api.massive.com/v1/reference/sec/filings/0001193125-26-330643/files/crda-20260630_htm.xml","metadata_anchor":{"source_id":"ref01-massive-connector:2d4e9adf-4f48-4957-8f81-82f5341ba97a","source_sha256":"f3cde4b06fc290ae57c000d4e44826f1b397e2c36f09ae3dac00b9001effcbc5","content_bytes":1449},"prior_html_failure_anchor":{"source_id":"ref01-massive-connector:43bf9ea0-439d-475d-a49e-0e6772fcc487","source_sha256":"98e070dee59bc8c3a3a14d0c2d0c67b56d3ad6577e6aa2d289557ae7f33261b4","content_bytes":157,"reservation_id":"43bf9ea0-439d-475d-a49e-0e6772fcc487","provider_outcome":"UNKNOWN","error_reconciled_event":"ROOT_REF01_CONNECTOR_ERROR_RECONCILED_V1","unknown_event_preserved":"ROOT_REF01_CONNECTOR_UNKNOWN_V1","html_retry_admitted":false},"maximum_connector_calls":1,"maximum_retained_bytes":131072,"maximum_xbrl_file_bytes_for_body_admission":1800000,"v3_maximum_connector_operations":2,"v3_maximum_retained_bytes":3801088,"three_manifest_wave_maximum_retained_bytes":25165824,"body_requires_prior_verified_size_receipt":false,"original_wire_byte_identity_claimed":false},{"route_id":12,"family_id":"REF01","case":"CRD","accession":"0001193125-26-330643","expected_cik":"0000025475","kind":"xbrl_instance_body","method":"GET","provider":"Massive","route":"/v1/reference/sec/filings/0001193125-26-330643/files/crda-20260630_htm.xml","parameters":{},"observed_filename":"crda-20260630_htm.xml","observed_xbrl_instance_url":"https://api.massive.com/v1/reference/sec/filings/0001193125-26-330643/files/crda-20260630_htm.xml","metadata_anchor":{"source_id":"ref01-massive-connector:2d4e9adf-4f48-4957-8f81-82f5341ba97a","source_sha256":"f3cde4b06fc290ae57c000d4e44826f1b397e2c36f09ae3dac00b9001effcbc5","content_bytes":1449},"prior_html_failure_anchor":{"source_id":"ref01-massive-connector:43bf9ea0-439d-475d-a49e-0e6772fcc487","source_sha256":"98e070dee59bc8c3a3a14d0c2d0c67b56d3ad6577e6aa2d289557ae7f33261b4","content_bytes":157,"reservation_id":"43bf9ea0-439d-475d-a49e-0e6772fcc487","provider_outcome":"UNKNOWN","error_reconciled_event":"ROOT_REF01_CONNECTOR_ERROR_RECONCILED_V1","unknown_event_preserved":"ROOT_REF01_CONNECTOR_UNKNOWN_V1","html_retry_admitted":false},"maximum_connector_calls":1,"maximum_retained_bytes":3670016,"maximum_xbrl_file_bytes_for_body_admission":1800000,"v3_maximum_connector_operations":2,"v3_maximum_retained_bytes":3801088,"three_manifest_wave_maximum_retained_bytes":25165824,"body_requires_prior_verified_size_receipt":true,"original_wire_byte_identity_claimed":false}]$manifest$;
v_retention_cap bigint;v_wave_bytes bigint;v_v3_bytes bigint;v_v3_requests bigint;
v_file_receipt jsonb;v_file_doc population_top3000_202608_202609_v1.source_document%rowtype;
v_file_size bigint;v_size_evidence jsonb;v_file_anchor jsonb;
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
IF encode(sha256(convert_to(v_manifest_text,'UTF8')),'hex')<>'ace46fc9d094f58f18cdfac0b1b1adf9fa1c295a9608b3375eeb3d0a6af1e4b7' THEN RAISE EXCEPTION 'MANIFEST_BYTES_CHANGED';END IF;
m:=v_manifest_text::jsonb;
IF coalesce(current_setting('top3000.ref01_route_id',true),'')!~'^(11|12)$' THEN RAISE EXCEPTION 'EXACT_V3_ROUTE_11_OR_12_REQUIRED';END IF;
route_id:=current_setting('top3000.ref01_route_id')::integer;route:=m->(route_id-11);
IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.catalog_family_contract WHERE family_id='REF01') THEN RAISE EXCEPTION 'REF01_SCOPE_CONTRACT_REQUIRED';END IF;
IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_event e WHERE e.event='ROOT_REF01_CONNECTOR_RESERVED_V1'
 AND e.details->>'manifest_sha256'='ace46fc9d094f58f18cdfac0b1b1adf9fa1c295a9608b3375eeb3d0a6af1e4b7' AND e.details->>'route_id'=route_id::text)
 THEN RAISE EXCEPTION 'ROUTE_ALREADY_RESERVED_NO_AUTOMATIC_REISSUE';END IF;
IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_event e WHERE e.event='ROOT_REF01_CONNECTOR_RESERVED_V1'
 AND NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_event closed
 WHERE closed.details->>'reservation_id'=e.details->>'reservation_id'
 AND (closed.event='ROOT_REF01_CONNECTOR_RETAINED_V1' OR
 (closed.event='ROOT_REF01_CONNECTOR_ERROR_RECONCILED_V1' AND e.details->>'reservation_id'='43bf9ea0-439d-475d-a49e-0e6772fcc487'))))
 THEN RAISE EXCEPTION 'EARLIER_MANUAL_CONNECTOR_OUTCOME_UNRESOLVED';END IF;
-- Exact retained metadata and the reconciled but still UNKNOWN HTML result are dependencies.
IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.source_document d
 WHERE d.source_id='ref01-massive-connector:2d4e9adf-4f48-4957-8f81-82f5341ba97a'
 AND d.source_sha256='f3cde4b06fc290ae57c000d4e44826f1b397e2c36f09ae3dac00b9001effcbc5'
 AND d.content_bytes=1449 AND octet_length(d.content_text)=1449
 AND encode(sha256(convert_to(d.content_text,'UTF8')),'hex')=d.source_sha256
 AND d.source_provider='Massive' AND position('https://api.massive.com/v1/reference/sec/filings/0001193125-26-330643/files/crda-20260630_htm.xml' IN d.content_text)>0)
 THEN RAISE EXCEPTION 'EXACT_RETAINED_METADATA_XBRL_URL_REQUIRED';END IF;
IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.source_document d
 WHERE d.source_id='ref01-massive-connector:43bf9ea0-439d-475d-a49e-0e6772fcc487'
 AND d.source_sha256='98e070dee59bc8c3a3a14d0c2d0c67b56d3ad6577e6aa2d289557ae7f33261b4'
 AND d.content_bytes=157 AND octet_length(d.content_text)=157
 AND encode(sha256(convert_to(d.content_text,'UTF8')),'hex')=d.source_sha256)
 OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_event e
 WHERE e.event='ROOT_REF01_CONNECTOR_UNKNOWN_V1' AND e.details->>'reservation_id'='43bf9ea0-439d-475d-a49e-0e6772fcc487')
 OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_event e
 WHERE e.event='ROOT_REF01_CONNECTOR_ERROR_RECONCILED_V1' AND e.details->>'reservation_id'='43bf9ea0-439d-475d-a49e-0e6772fcc487')
 THEN RAISE EXCEPTION 'PRESERVED_CRD_HTML_FAILURE_AND_EXPLICIT_RECONCILIATION_REQUIRED';END IF;

SELECT count(*) INTO v_v3_requests FROM population_top3000_202608_202609_v1.pipeline_event e
 WHERE e.event='ROOT_REF01_CONNECTOR_RESERVED_V1' AND e.details->>'manifest_sha256'='ace46fc9d094f58f18cdfac0b1b1adf9fa1c295a9608b3375eeb3d0a6af1e4b7';
IF v_v3_requests<>(route_id-11) OR v_v3_requests>=2 THEN RAISE EXCEPTION 'EXACT_V3_TWO_OPERATION_SEQUENCE_REQUIRED';END IF;
v_file_anchor:=NULL;
IF route_id=12 THEN
 SELECT e.details INTO v_file_receipt FROM population_top3000_202608_202609_v1.pipeline_event e
 WHERE e.event='ROOT_REF01_CONNECTOR_RETAINED_V1' AND e.details->>'manifest_sha256'='ace46fc9d094f58f18cdfac0b1b1adf9fa1c295a9608b3375eeb3d0a6af1e4b7'
 AND e.details->>'route_id'='11' AND e.details->>'outcome_class'='SUCCESS' ORDER BY e.at DESC LIMIT 1;
 IF v_file_receipt IS NULL THEN RAISE EXCEPTION 'RETAINED_ACTUAL_XBRL_FILE_SIZE_REQUIRED';END IF;
 SELECT d.* INTO v_file_doc FROM population_top3000_202608_202609_v1.source_document d
 WHERE d.source_id=v_file_receipt->>'source_id';
 IF NOT FOUND OR v_file_doc.source_sha256 IS DISTINCT FROM v_file_receipt->>'source_sha256'
 OR v_file_doc.source_sha256 IS DISTINCT FROM encode(sha256(convert_to(v_file_doc.content_text,'UTF8')),'hex')
 OR v_file_doc.content_bytes IS DISTINCT FROM octet_length(v_file_doc.content_text)::bigint
 OR v_file_doc.content_bytes>131072 OR v_file_doc.evidence->>'manifest_sha256' IS DISTINCT FROM 'ace46fc9d094f58f18cdfac0b1b1adf9fa1c295a9608b3375eeb3d0a6af1e4b7'
 OR v_file_doc.evidence->>'outcome_class' IS DISTINCT FROM 'SUCCESS'
 OR v_file_doc.evidence->'request' IS DISTINCT FROM m->0
 THEN RAISE EXCEPTION 'XBRL_FILE_SIZE_RECEIPT_SOURCE_CHANGED';END IF;
 v_size_evidence:=v_file_doc.evidence->'xbrl_file_size_evidence';
 IF v_size_evidence->>'state' IS DISTINCT FROM 'VERIFIED_SIZE_WITHIN_BODY_LIMIT'
 OR v_size_evidence->>'body_admission_allowed' IS DISTINCT FROM 'true'
 OR v_size_evidence->'selected_file'->>'filing_id' IS DISTINCT FROM '0001193125-26-330643'
 OR v_size_evidence->'selected_file'->>'filename' IS DISTINCT FROM 'crda-20260630_htm.xml'
 OR coalesce(v_size_evidence->'selected_file'->>'size_bytes','')!~'^[0-9]{1,10}$'
 THEN RAISE EXCEPTION 'EXACT_UNAMBIGUOUS_SMALL_XBRL_FILE_REQUIRED';END IF;
 v_file_size:=(v_size_evidence->'selected_file'->>'size_bytes')::bigint;
 IF v_file_size NOT BETWEEN 1 AND 1800000 THEN RAISE EXCEPTION 'XBRL_SOURCE_SIZE_EXCEEDS_ADMITTED_SCREEN';END IF;
 v_file_anchor:=jsonb_build_object('source_id',v_file_doc.source_id,'source_sha256',v_file_doc.source_sha256,
 'content_bytes',v_file_doc.content_bytes,'reported_file_bytes',v_file_size,'selected_file',v_size_evidence->'selected_file',
 'size_evidence',v_size_evidence,'original_wire_identity_proven',false);
END IF;
SELECT coalesce(sum(d.content_bytes),0) INTO v_wave_bytes FROM (
 SELECT DISTINCT e.details->>'reservation_id' reservation_id FROM population_top3000_202608_202609_v1.pipeline_event e
 WHERE e.event='ROOT_REF01_CONNECTOR_RESERVED_V1' AND e.details->>'manifest_sha256' IN('1ebab0c5574d665162f5ee13b98a7f8632fca153b95798d500851f00b35170b9','7c6c572fb8e4fed3f4e5136d5928a71a3d819d61fbe0697793086da70b55c07f','ace46fc9d094f58f18cdfac0b1b1adf9fa1c295a9608b3375eeb3d0a6af1e4b7')
) q JOIN population_top3000_202608_202609_v1.source_document d ON d.source_id='ref01-massive-connector:'||q.reservation_id;
SELECT coalesce(sum(d.content_bytes),0) INTO v_v3_bytes FROM (
 SELECT DISTINCT e.details->>'reservation_id' reservation_id FROM population_top3000_202608_202609_v1.pipeline_event e
 WHERE e.event='ROOT_REF01_CONNECTOR_RESERVED_V1' AND e.details->>'manifest_sha256'='ace46fc9d094f58f18cdfac0b1b1adf9fa1c295a9608b3375eeb3d0a6af1e4b7'
) q JOIN population_top3000_202608_202609_v1.source_document d ON d.source_id='ref01-massive-connector:'||q.reservation_id;
v_retention_cap:=(route->>'maximum_retained_bytes')::bigint;
IF v_wave_bytes+v_retention_cap>25165824 OR v_v3_bytes+v_retention_cap>3801088 THEN RAISE EXCEPTION 'V3_OR_THREE_MANIFEST_WAVE_RAW_CAP';END IF;
IF p.budget_verified_at IS NULL OR coalesce((p.budget_evidence->>'provider_admissions_allowed')::boolean,false) IS NOT TRUE
 OR (coalesce(p.budget_evidence->'allowed_providers','[]'::jsonb) ? 'Massive') IS NOT TRUE THEN RAISE EXCEPTION 'CURRENT_MASSIVE_ADMISSION_REQUIRED';END IF;
limit_bytes:=nullif(p.budget_evidence#>>'{resource_limits,max_relation_bytes}','')::bigint;
SELECT coalesce(sum(pg_total_relation_size(r.oid)),0) INTO relation_bytes FROM pg_class r JOIN pg_namespace n ON n.oid=r.relnamespace
 WHERE n.nspname='population_top3000_202608_202609_v1' AND r.relkind IN('r','m');
IF limit_bytes IS NULL OR relation_bytes>=limit_bytes THEN RAISE EXCEPTION 'MISSION_RELATION_CAP';END IF;
budget_total:=p.actual_usd+p.accrued_usd+p.unavoidable_usd+p.remaining_uncommitted_usd+p.repair_egress_reserve_usd+p.reserved_request_usd;
IF budget_total IS NULL OR budget_total>250 OR c.provider_requests>=c.max_provider_requests OR c.http_invocations>=c.max_http_invocations
 OR c.stored_bytes+c.reserved_bytes+v_retention_cap>c.max_source_bytes THEN RAISE EXCEPTION 'BUDGET_REQUEST_OR_RAW_CAP';END IF;
IF c.next_provider_at>clock_timestamp() THEN RAISE EXCEPTION 'PROVIDER_PACING_WINDOW_NOT_READY';END IF;
reservation_id:=gen_random_uuid();
reservation:=jsonb_build_object('reservation_id',reservation_id,'manifest_sha256','ace46fc9d094f58f18cdfac0b1b1adf9fa1c295a9608b3375eeb3d0a6af1e4b7','route_id',route_id,'route',route,
 'controller',c.owner_id,'owner_fence',c.owner_fence,'stop_generation',c.stop_generation,'scope_sha256',c.scope_sha256,
 'reserved_at',clock_timestamp(),'review_after',clock_timestamp()+interval '5 minutes','reserved_bytes',v_retention_cap,'reserved_incremental_usd',0,
 'mode','ROOT_DIRECT_CONNECTOR_NO_PIPELINE_ADAPTER_EXECUTED','source_representation','CONNECTOR_REPRESENTATION_NOT_WIRE',
 'provider_counter_increment',1,'http_counter_increment',1,'http_counter_meaning','CONSERVATIVE_CONNECTOR_OPERATION_ALLOWANCE_NOT_OBSERVED_EDGE_INVOCATION',
 'wire_request_count_observed',false,'connector_internal_retries_observed',false,'automatic_retry',false,'automatic_pagination',false,
 'automatic_normalization_or_publication',false,'content_fetch_admitted',route_id=12,'invoice_reconciled',false,
 'response_byte_limit_kind','RETAINED_REPRESENTATION_CAP; connector transport cap not independently controllable',
 'max_relation_bytes',limit_bytes,'budget_total_at_reservation',budget_total,'verified_filelist_anchor',v_file_anchor,
 'v3_previously_retained_bytes',v_v3_bytes,'v3_maximum_retained_bytes',3801088,
 'three_manifest_wave_previously_retained_bytes',v_wave_bytes,'three_manifest_wave_maximum_retained_bytes',25165824,
 'prior_html_provider_outcome','UNKNOWN_PRESERVED_NOT_RETRIED','filename_filter_spec','ROOT_VERIFIED_MASSIVE_ENDPOINT_REGISTRY');
UPDATE population_top3000_202608_202609_v1.acquisition_control SET provider_requests=provider_requests+1,http_invocations=http_invocations+1,
 reserved_bytes=reserved_bytes+v_retention_cap,next_provider_at=clock_timestamp()+interval '500 milliseconds',
 hold_reason='ROOT_CONNECTOR_RESERVATION:'||reservation_id::text,updated_at=clock_timestamp() WHERE singleton;
INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES('ROOT_REF01_CONNECTOR_RESERVED_V1',reservation);
PERFORM set_config('top3000.ref01_reservation_id',reservation_id::text,true);
END;
$fixture_reserve$;
CREATE OR REPLACE FUNCTION pg_temp.ref01_crd_xbrl_retain_fixture_v3() RETURNS void LANGUAGE plpgsql AS $fixture_retain$

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
END;
$fixture_retain$;
CREATE OR REPLACE FUNCTION pg_temp.ref01_crd_xbrl_unknown_fixture_v3() RETURNS void LANGUAGE plpgsql AS $fixture_unknown$

DECLARE
 c population_top3000_202608_202609_v1.acquisition_control%rowtype;
 p population_top3000_202608_202609_v1.pipeline_control%rowtype;
 m jsonb;route jsonb;route_id integer;reservation_id uuid;reservation jsonb;prior_receipt jsonb;
 relation_bytes bigint;limit_bytes bigint;budget_total numeric;changed integer;
failure_code text;
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
reservation_id:=current_setting('top3000.ref01_reservation_id')::uuid;failure_code:=current_setting('top3000.ref01_failure_code');
SELECT details INTO reservation FROM population_top3000_202608_202609_v1.pipeline_event WHERE event='ROOT_REF01_CONNECTOR_RESERVED_V1' AND details->>'reservation_id'=reservation_id::text ORDER BY at DESC LIMIT 1;
IF reservation IS NULL OR reservation->>'manifest_sha256' IS DISTINCT FROM 'ace46fc9d094f58f18cdfac0b1b1adf9fa1c295a9608b3375eeb3d0a6af1e4b7' OR reservation->>'controller' IS DISTINCT FROM c.owner_id
 OR reservation->>'owner_fence' IS DISTINCT FROM c.owner_fence::text OR reservation->>'stop_generation' IS DISTINCT FROM c.stop_generation::text
 OR reservation->>'scope_sha256' IS DISTINCT FROM c.scope_sha256 OR c.hold_reason IS DISTINCT FROM 'ROOT_CONNECTOR_RESERVATION:'||reservation_id::text
 OR failure_code IS NULL OR failure_code!~'^[A-Z0-9_]{1,80}$' THEN RAISE EXCEPTION 'EXACT_PENDING_MANUAL_RESERVATION_REQUIRED';END IF;
IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_event WHERE event='ROOT_REF01_CONNECTOR_RETAINED_V1' AND details->>'reservation_id'=reservation_id::text)
 THEN RAISE EXCEPTION 'RECONCILE_ALREADY_RETAINED_OUTCOME';END IF;
IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_event WHERE event='ROOT_REF01_CONNECTOR_UNKNOWN_V1' AND details->>'reservation_id'=reservation_id::text)
 THEN INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES('ROOT_REF01_CONNECTOR_UNKNOWN_V1',
  jsonb_build_object('reservation_id',reservation_id,'manifest_sha256','ace46fc9d094f58f18cdfac0b1b1adf9fa1c295a9608b3375eeb3d0a6af1e4b7','route',reservation->'route','failure_code',failure_code,
   'outcome','UNCERTAIN_EFFECT','raw_response_retained',false,'provider_counter_preserved',true,'http_counter_preserved',true,'reserved_bytes_preserved',(reservation->>'reserved_bytes')::bigint,
   'manual_hold_preserved',true,'automatic_retry',false,'resolution_required','RETAIN_LATE_ACTUAL_RESPONSE_OR_ROOT_RECONCILES_EXISTING_OPERATION; no reissue'));
END IF;
END;
$fixture_unknown$;

CREATE OR REPLACE FUNCTION pg_temp.ref01_crd_xbrl_envelope_fixture_v3(p_payload text,p_xml_path text DEFAULT 'null')
RETURNS text LANGUAGE plpgsql AS $fixture_envelope$
DECLARE v_envelope text;
BEGIN
 v_envelope:=jsonb_build_object('content',jsonb_build_array(jsonb_build_object('type','text','text',p_payload)),
  'structuredContent',jsonb_build_object('result',p_payload),'isError',false,
  'fixture','ROLLBACK_ONLY_SYNTHETIC_NOT_A_FILING')::text;
 PERFORM set_config('top3000.ref01_content_base64',encode(convert_to(v_envelope,'UTF8'),'base64'),true);
 PERFORM set_config('top3000.ref01_content_sha256',encode(sha256(convert_to(v_envelope,'UTF8')),'hex'),true);
 PERFORM set_config('top3000.ref01_observed_at',clock_timestamp()::text,true);
 PERFORM set_config('top3000.ref01_tool_name','mcp__codex_apps__massive_call_api',true);
 PERFORM set_config('top3000.ref01_outcome_class','SUCCESS',true);
 PERFORM set_config('top3000.ref01_xml_json_path',p_xml_path,true);
 RETURN v_envelope;
END;
$fixture_envelope$;

DO $test$
DECLARE
 v_before population_top3000_202608_202609_v1.acquisition_control%rowtype;
 v_after population_top3000_202608_202609_v1.acquisition_control%rowtype;
 v_doc population_top3000_202608_202609_v1.source_document%rowtype;
 v_reservation jsonb;v_rid text;v_meta_rid text;v_meta_sid text;v_body_sid text;
 v_xml text:='<xbrli:xbrl xmlns:xbrli="http://www.xbrl.org/2003/instance"><synthetic>ROLLBACK_ONLY_NO_FACT</synthetic></xbrli:xbrl>';
 v_header text:='filing_id,filename,size_bytes,description'||chr(13)||chr(10);
 v_description text:='Synthetic, "quoted"'||chr(10)||'only';
 v_csv text;v_env text;v_meta_env text;v_body_env text;v_hash text;v_case integer;v_unsafe_xml text;
 v_expected_bytes bigint:=0;v_negative integer:=0;v_prior_events jsonb;v_after_events jsonb;
 v_prior_anchor jsonb;v_after_anchor jsonb;
BEGIN
 SELECT * INTO v_before FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton FOR UPDATE;
 IF v_before.hold_reason IS NOT NULL THEN RAISE EXCEPTION 'FIXTURE_REQUIRES_NO_EXISTING_MANUAL_HOLD';END IF;
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_event e
  WHERE e.event IN('ROOT_REF01_CONNECTOR_RESERVED_V1','ROOT_REF01_CONNECTOR_RETAINED_V1','ROOT_REF01_CONNECTOR_UNKNOWN_V1') AND e.details->>'manifest_sha256'='ace46fc9d094f58f18cdfac0b1b1adf9fa1c295a9608b3375eeb3d0a6af1e4b7') THEN RAISE EXCEPTION 'FIXTURE_MUST_PRECEDE_REAL_V3_ADMISSION';END IF;
 SELECT jsonb_agg(jsonb_build_object('event',e.event,'at',e.at,'details',e.details) ORDER BY e.event,e.at)
 INTO v_prior_events FROM population_top3000_202608_202609_v1.pipeline_event e
 WHERE e.details->>'reservation_id'='43bf9ea0-439d-475d-a49e-0e6772fcc487';
 SELECT jsonb_agg(jsonb_build_object('source_id',d.source_id,'sha256',d.source_sha256,
  'bytes',d.content_bytes,'current_sha256',encode(sha256(convert_to(d.content_text,'UTF8')),'hex'),'evidence',d.evidence) ORDER BY d.source_id)
 INTO v_prior_anchor FROM population_top3000_202608_202609_v1.source_document d
 WHERE d.source_id IN('ref01-massive-connector:2d4e9adf-4f48-4957-8f81-82f5341ba97a','ref01-massive-connector:43bf9ea0-439d-475d-a49e-0e6772fcc487');

 -- A body cannot precede its actual file-size receipt.
 PERFORM set_config('top3000.ref01_route_id','12',true);
 BEGIN
  PERFORM pg_temp.ref01_crd_xbrl_reserve_fixture_v3();RAISE EXCEPTION 'TEST_EXPECTED_BODY_SEQUENCE_REJECTION';
 EXCEPTION WHEN OTHERS THEN
  IF SQLERRM<>'EXACT_V3_TWO_OPERATION_SEQUENCE_REQUIRED' THEN RAISE;END IF;v_negative:=v_negative+1;
 END;

 -- Unknown response: one counter increment and full reservation remain; repeat is blocked.
 BEGIN
  PERFORM pg_sleep(0.51);
  PERFORM set_config('top3000.ref01_route_id','11',true);
  PERFORM pg_temp.ref01_crd_xbrl_reserve_fixture_v3();v_rid:=current_setting('top3000.ref01_reservation_id');
  PERFORM set_config('top3000.ref01_failure_code','ROLLBACK_SYNTHETIC_TRANSPORT_UNKNOWN',true);
  PERFORM pg_temp.ref01_crd_xbrl_unknown_fixture_v3();
  PERFORM pg_temp.ref01_crd_xbrl_unknown_fixture_v3();
  SELECT * INTO v_after FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton;
  IF v_after.provider_requests<>v_before.provider_requests+1 OR v_after.http_invocations<>v_before.http_invocations+1
   OR v_after.reserved_bytes<>v_before.reserved_bytes+131072 OR v_after.stored_bytes<>v_before.stored_bytes
   OR v_after.hold_reason IS DISTINCT FROM 'ROOT_CONNECTOR_RESERVATION:'||v_rid
   OR (SELECT count(*) FROM population_top3000_202608_202609_v1.pipeline_event e
     WHERE e.event='ROOT_REF01_CONNECTOR_UNKNOWN_V1' AND e.details->>'reservation_id'=v_rid)<>1
   OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.source_document d WHERE d.source_id='ref01-massive-connector:'||v_rid)
   THEN RAISE EXCEPTION 'TEST_UNKNOWN_RESERVATION_NOT_PRESERVED';END IF;
  BEGIN
   PERFORM pg_temp.ref01_crd_xbrl_reserve_fixture_v3();RAISE EXCEPTION 'TEST_EXPECTED_UNKNOWN_REISSUE_REJECTION';
  EXCEPTION WHEN OTHERS THEN
   IF SQLERRM<>'EXISTING_CONTROL_HOLD_MUST_BE_RECONCILED' THEN RAISE;END IF;v_negative:=v_negative+1;
  END;
  RAISE EXCEPTION USING ERRCODE='ZX001',MESSAGE='ROLLBACK_UNKNOWN_FIXTURE_BRANCH';
 EXCEPTION WHEN SQLSTATE 'ZX001' THEN NULL;
 END;

 -- Both an unexpected filename and an oversized exact file retain metadata but deny body admission.
 FOR v_case IN 1..2 LOOP
  BEGIN
   PERFORM pg_sleep(0.51);
   PERFORM set_config('top3000.ref01_route_id','11',true);
   PERFORM pg_temp.ref01_crd_xbrl_reserve_fixture_v3();
   v_csv:=v_header||'0001193125-26-330643,'||CASE WHEN v_case=1 THEN 'different-file.xml' ELSE 'crda-20260630_htm.xml' END||','||
    CASE WHEN v_case=2 THEN '1800001' ELSE octet_length(v_xml)::text END||',synthetic'||chr(13)||chr(10);
   v_env:=pg_temp.ref01_crd_xbrl_envelope_fixture_v3(v_csv);
   PERFORM pg_temp.ref01_crd_xbrl_retain_fixture_v3();
   SELECT d.* INTO v_doc FROM population_top3000_202608_202609_v1.source_document d WHERE d.source_id=current_setting('top3000.ref01_source_id');
   IF v_doc.content_text IS DISTINCT FROM v_env OR v_doc.evidence#>>'{xbrl_file_size_evidence,body_admission_allowed}' IS DISTINCT FROM 'false'
    OR v_doc.evidence#>>'{xbrl_file_size_evidence,state}' IS DISTINCT FROM
     (CASE WHEN v_case=1 THEN 'RETAINED_FILE_LIST_REQUIRES_FORMAT_REVIEW' ELSE 'OBSERVED_SIZE_OUTSIDE_BODY_LIMIT' END)
    THEN RAISE EXCEPTION 'TEST_INVALID_FILELIST_NOT_RETAINED_WITH_CLOSED_BODY_GATE';END IF;
   PERFORM set_config('top3000.ref01_route_id','12',true);
   BEGIN
    PERFORM pg_temp.ref01_crd_xbrl_reserve_fixture_v3();RAISE EXCEPTION 'TEST_EXPECTED_FILE_SIZE_GATE_REJECTION';
   EXCEPTION WHEN OTHERS THEN
    IF SQLERRM<>'EXACT_UNAMBIGUOUS_SMALL_XBRL_FILE_REQUIRED' THEN RAISE;END IF;v_negative:=v_negative+1;
   END;
   RAISE EXCEPTION USING ERRCODE='ZX002',MESSAGE='ROLLBACK_INVALID_FILELIST_FIXTURE_BRANCH';
  EXCEPTION WHEN SQLSTATE 'ZX002' THEN NULL;
  END;
 END LOOP;
 SELECT * INTO v_after FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton;
 IF v_after.provider_requests<>v_before.provider_requests OR v_after.http_invocations<>v_before.http_invocations
  OR v_after.stored_bytes<>v_before.stored_bytes OR v_after.reserved_bytes<>v_before.reserved_bytes
  OR v_after.hold_reason IS DISTINCT FROM v_before.hold_reason
  OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_event e WHERE e.event IN('ROOT_REF01_CONNECTOR_RESERVED_V1','ROOT_REF01_CONNECTOR_RETAINED_V1','ROOT_REF01_CONNECTOR_UNKNOWN_V1') AND e.details->>'manifest_sha256'='ace46fc9d094f58f18cdfac0b1b1adf9fa1c295a9608b3375eeb3d0a6af1e4b7')
  THEN RAISE EXCEPTION 'TEST_NEGATIVE_BRANCHES_DID_NOT_ROLL_BACK';END IF;

 -- Main path: exact quoted CSV, immutable replay, then the one admitted XML body.
 PERFORM pg_sleep(0.51);
 PERFORM set_config('top3000.ref01_route_id','11',true);
 PERFORM pg_temp.ref01_crd_xbrl_reserve_fixture_v3();v_meta_rid:=current_setting('top3000.ref01_reservation_id');
 v_csv:=v_header||'0001193125-26-330643,crda-20260630_htm.xml,'||octet_length(v_xml)::text||',"Synthetic, ""quoted""'||chr(10)||'only"'||chr(13)||chr(10);
 v_meta_env:=pg_temp.ref01_crd_xbrl_envelope_fixture_v3(v_csv);
 PERFORM pg_temp.ref01_crd_xbrl_retain_fixture_v3();v_meta_sid:=current_setting('top3000.ref01_source_id');
 PERFORM pg_temp.ref01_crd_xbrl_retain_fixture_v3();
 SELECT d.* INTO v_doc FROM population_top3000_202608_202609_v1.source_document d WHERE d.source_id=v_meta_sid;
 IF v_doc.content_text IS DISTINCT FROM v_meta_env OR v_doc.evidence#>>'{xbrl_file_size_evidence,state}' IS DISTINCT FROM 'VERIFIED_SIZE_WITHIN_BODY_LIMIT'
  OR v_doc.evidence#>>'{xbrl_file_size_evidence,body_admission_allowed}' IS DISTINCT FROM 'true'
  OR v_doc.evidence#>>'{xbrl_file_size_evidence,selected_file,description}' IS DISTINCT FROM v_description
  OR (v_doc.evidence#>>'{xbrl_file_size_evidence,selected_file,size_bytes}')::bigint IS DISTINCT FROM octet_length(v_xml)::bigint
  THEN RAISE EXCEPTION 'TEST_EXACT_QUOTED_CSV_REPLAY_FAILED';END IF;
 v_expected_bytes:=octet_length(v_meta_env);
 BEGIN
  PERFORM pg_temp.ref01_crd_xbrl_reserve_fixture_v3();RAISE EXCEPTION 'TEST_EXPECTED_METADATA_REISSUE_REJECTION';
 EXCEPTION WHEN OTHERS THEN
  IF SQLERRM<>'ROUTE_ALREADY_RESERVED_NO_AUTOMATIC_REISSUE' THEN RAISE;END IF;v_negative:=v_negative+1;
 END;
 PERFORM pg_sleep(0.51);
 PERFORM set_config('top3000.ref01_route_id','12',true);
 PERFORM pg_temp.ref01_crd_xbrl_reserve_fixture_v3();v_rid:=current_setting('top3000.ref01_reservation_id');
 SELECT e.details INTO v_reservation FROM population_top3000_202608_202609_v1.pipeline_event e
  WHERE e.event='ROOT_REF01_CONNECTOR_RESERVED_V1' AND e.details->>'reservation_id'=v_rid;
 IF v_reservation#>>'{verified_filelist_anchor,source_id}' IS DISTINCT FROM v_meta_sid
  OR v_reservation#>>'{verified_filelist_anchor,source_sha256}' IS DISTINCT FROM v_doc.source_sha256
  OR (v_reservation#>>'{verified_filelist_anchor,reported_file_bytes}')::bigint IS DISTINCT FROM octet_length(v_xml)::bigint
  OR (v_reservation->>'reserved_bytes')::bigint<>3670016 THEN RAISE EXCEPTION 'TEST_BODY_NOT_PINNED_TO_ACTUAL_FILELIST';END IF;
 v_body_env:=pg_temp.ref01_crd_xbrl_envelope_fixture_v3(v_xml,'["structuredContent","result"]');
 v_hash:=current_setting('top3000.ref01_content_sha256');
 PERFORM set_config('top3000.ref01_content_sha256',repeat('0',64),true);
 BEGIN
  PERFORM pg_temp.ref01_crd_xbrl_retain_fixture_v3();RAISE EXCEPTION 'TEST_EXPECTED_HASH_TAMPER_REJECTION';
 EXCEPTION WHEN OTHERS THEN
  IF SQLERRM<>'COMPLETE_REPRESENTATION_HASH_OR_BOUND_INVALID' THEN RAISE;END IF;v_negative:=v_negative+1;
 END;
 PERFORM set_config('top3000.ref01_content_sha256',v_hash,true);
 v_env:=pg_temp.ref01_crd_xbrl_envelope_fixture_v3(replace(v_xml,'xbrli:xbrl','xbrli:fake'),'["structuredContent","result"]');
 BEGIN
  PERFORM pg_temp.ref01_crd_xbrl_retain_fixture_v3();RAISE EXCEPTION 'TEST_EXPECTED_NON_XBRL_ROOT_REJECTION';
 EXCEPTION WHEN OTHERS THEN
  IF SQLERRM<>'XBRL_INSTANCE_ROOT_REQUIRED' THEN RAISE;END IF;v_negative:=v_negative+1;
 END;
 -- Match the accepted byte length so this tests declaration rejection, not the length gate.
 v_unsafe_xml:='<!DOCTYPE xbrl [<!ENTITY test "test">]><xbrl/>';
 v_unsafe_xml:=v_unsafe_xml||repeat(' ',octet_length(v_xml)-octet_length(v_unsafe_xml));
 IF octet_length(v_unsafe_xml)<>octet_length(v_xml) THEN RAISE EXCEPTION 'TEST_DTD_FIXTURE_EQUAL_BYTES_REQUIRED';END IF;
 v_env:=pg_temp.ref01_crd_xbrl_envelope_fixture_v3(v_unsafe_xml,'["structuredContent","result"]');
 BEGIN
  PERFORM pg_temp.ref01_crd_xbrl_retain_fixture_v3();RAISE EXCEPTION 'TEST_EXPECTED_DTD_ENTITY_REJECTION';
 EXCEPTION WHEN OTHERS THEN
  IF SQLERRM<>'EXACT_BOUNDED_WELL_FORMED_XML_WITHOUT_DTD_REQUIRED' THEN RAISE;END IF;v_negative:=v_negative+1;
 END;
 SELECT * INTO v_after FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton;
 IF v_after.reserved_bytes<>v_before.reserved_bytes+3670016 OR v_after.hold_reason IS DISTINCT FROM 'ROOT_CONNECTOR_RESERVATION:'||v_rid
  OR v_after.stored_bytes<>v_before.stored_bytes+v_expected_bytes
  OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.source_document d WHERE d.source_id='ref01-massive-connector:'||v_rid)
  THEN RAISE EXCEPTION 'TEST_REJECTED_BODY_DID_NOT_KEEP_RESERVATION';END IF;
 v_body_env:=pg_temp.ref01_crd_xbrl_envelope_fixture_v3(v_xml,'["structuredContent","result"]');
 PERFORM pg_temp.ref01_crd_xbrl_retain_fixture_v3();v_body_sid:=current_setting('top3000.ref01_source_id');
 PERFORM pg_temp.ref01_crd_xbrl_retain_fixture_v3();
 SELECT d.* INTO v_doc FROM population_top3000_202608_202609_v1.source_document d WHERE d.source_id=v_body_sid;
 IF v_doc.content_text IS DISTINCT FROM v_body_env OR v_doc.evidence#>>'{xml_extraction,extracted}' IS DISTINCT FROM 'true'
  OR v_doc.evidence#>>'{xml_extraction,utf8_sha256}' IS DISTINCT FROM encode(sha256(convert_to(v_xml,'UTF8')),'hex')
  OR (v_doc.evidence#>>'{xml_extraction,utf8_bytes}')::bigint IS DISTINCT FROM octet_length(v_xml)::bigint
  OR v_doc.evidence#>>'{xml_extraction,original_wire_identity_proven}' IS DISTINCT FROM 'false'
  OR v_doc.evidence#>>'{xml_extraction,additional_body_copy_stored}' IS DISTINCT FROM 'false'
  OR v_doc.source_published_at IS NOT NULL OR v_doc.historical_received_at IS NOT NULL
  THEN RAISE EXCEPTION 'TEST_XML_REPLAY_OR_HONEST_REPRESENTATION_FAILED';END IF;
 v_expected_bytes:=v_expected_bytes+octet_length(v_body_env);
 BEGIN
  PERFORM pg_temp.ref01_crd_xbrl_reserve_fixture_v3();RAISE EXCEPTION 'TEST_EXPECTED_BODY_REISSUE_REJECTION';
 EXCEPTION WHEN OTHERS THEN
  IF SQLERRM<>'ROUTE_ALREADY_RESERVED_NO_AUTOMATIC_REISSUE' THEN RAISE;END IF;v_negative:=v_negative+1;
 END;
 SELECT * INTO v_after FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton;
 SELECT jsonb_agg(jsonb_build_object('event',e.event,'at',e.at,'details',e.details) ORDER BY e.event,e.at)
 INTO v_after_events FROM population_top3000_202608_202609_v1.pipeline_event e
 WHERE e.details->>'reservation_id'='43bf9ea0-439d-475d-a49e-0e6772fcc487';
 SELECT jsonb_agg(jsonb_build_object('source_id',d.source_id,'sha256',d.source_sha256,
  'bytes',d.content_bytes,'current_sha256',encode(sha256(convert_to(d.content_text,'UTF8')),'hex'),'evidence',d.evidence) ORDER BY d.source_id)
 INTO v_after_anchor FROM population_top3000_202608_202609_v1.source_document d
 WHERE d.source_id IN('ref01-massive-connector:2d4e9adf-4f48-4957-8f81-82f5341ba97a','ref01-massive-connector:43bf9ea0-439d-475d-a49e-0e6772fcc487');
 IF v_after.provider_requests<>v_before.provider_requests+2 OR v_after.http_invocations<>v_before.http_invocations+2
  OR v_after.stored_bytes<>v_before.stored_bytes+v_expected_bytes OR v_after.reserved_bytes<>v_before.reserved_bytes
  OR v_after.hold_reason IS NOT NULL OR v_prior_events IS DISTINCT FROM v_after_events OR v_prior_anchor IS DISTINCT FROM v_after_anchor
  OR (SELECT count(*) FROM population_top3000_202608_202609_v1.pipeline_event e WHERE e.event='ROOT_REF01_CONNECTOR_RESERVED_V1'
   AND e.details->>'manifest_sha256'='ace46fc9d094f58f18cdfac0b1b1adf9fa1c295a9608b3375eeb3d0a6af1e4b7')<>2
  OR (SELECT count(*) FROM population_top3000_202608_202609_v1.pipeline_event e WHERE e.event='ROOT_REF01_CONNECTOR_RETAINED_V1'
   AND e.details->>'manifest_sha256'='ace46fc9d094f58f18cdfac0b1b1adf9fa1c295a9608b3375eeb3d0a6af1e4b7')<>2
  OR NOT EXISTS(SELECT 1 FROM cron.job WHERE jobid=596 AND active=false) OR v_negative<>9
  THEN RAISE EXCEPTION 'TEST_FINAL_COUNTER_REPLAY_ANCHOR_OR_PAUSED_CRON_INVARIANT';END IF;
 PERFORM set_config('top3000.ref01_crd_xbrl_fixture_result',jsonb_build_object('status','PASS_ROLLBACK_ONLY',
  'synthetic_fixture',true,'provider_calls',0,'new_sources_inside_rollback',2,
  'provider_counter_delta_inside_rollback',2,'http_counter_delta_inside_rollback',2,
  'retained_bytes_inside_rollback',v_expected_bytes,'negative_assertions',v_negative,
  'quoted_csv_parser',true,'metadata_size_gate',true,'xml_root_hash_size_and_replay',true,'equal_byte_dtd_entity_rejected',true,
  'unknown_hold_and_counter_preservation',true,'prior_failure_and_metadata_unchanged',true,
  'cron_remains_paused',true,'actual_filing_content_verified',false,'company_population_certified',false,
  'manifest_sha256','ace46fc9d094f58f18cdfac0b1b1adf9fa1c295a9608b3375eeb3d0a6af1e4b7')::text,true);
END $test$;
SELECT current_setting('top3000.ref01_crd_xbrl_fixture_result')::jsonb AS fixture_result;
ROLLBACK;

