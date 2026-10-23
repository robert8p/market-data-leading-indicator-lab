-- ROOT ONLY DRAFT. Do not execute before controller review.
-- Uses no new worker, RPC, adapter binding, fake pipeline attempt or automatic retry.
BEGIN;
SET LOCAL statement_timeout='10s';
SET LOCAL lock_timeout='2s';
SELECT pg_advisory_xact_lock(2026083000261022);
-- V3 only: route11 exact filename/size metadata, then conditional route12 XBRL body.
DO $reserve$
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
END $reserve$;
SELECT details AS reservation FROM population_top3000_202608_202609_v1.pipeline_event
 WHERE event='ROOT_REF01_CONNECTOR_RESERVED_V1' AND details->>'reservation_id'=current_setting('top3000.ref01_reservation_id') ORDER BY at DESC LIMIT 1;
COMMIT;
