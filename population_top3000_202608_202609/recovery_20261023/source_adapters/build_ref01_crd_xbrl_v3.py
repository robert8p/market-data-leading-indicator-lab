"""Local-only bounded CRD XBRL alternative after the preserved HTML failure.

Reads existing guard text without importing/executing the v1 generator. Produces
root-reviewed anonymous transactions, never a worker, RPC, new table or request.
"""
from pathlib import Path
import ast
import hashlib
import json

P = Path(__file__).parent
base_ast = ast.parse((P / "build_ref01_manual_connector_flow.py").read_text())
parts = {}
for node in base_ast.body:
    if isinstance(node, ast.Assign) and len(node.targets) == 1 and isinstance(node.targets[0], ast.Name):
        if node.targets[0].id in {"header", "declare", "common"}:
            parts[node.targets[0].id] = ast.literal_eval(node.value)
header, declare, common = (parts[k] for k in ("header", "declare", "common"))
META_MANIFEST = "1ebab0c5574d665162f5ee13b98a7f8632fca153b95798d500851f00b35170b9"
BODY_MANIFEST = "7c6c572fb8e4fed3f4e5136d5928a71a3d819d61fbe0697793086da70b55c07f"
metadata_anchor = {
    "source_id": "ref01-massive-connector:2d4e9adf-4f48-4957-8f81-82f5341ba97a",
    "source_sha256": "f3cde4b06fc290ae57c000d4e44826f1b397e2c36f09ae3dac00b9001effcbc5",
    "content_bytes": 1449,
}
failure_anchor = {
    "source_id": "ref01-massive-connector:43bf9ea0-439d-475d-a49e-0e6772fcc487",
    "source_sha256": "98e070dee59bc8c3a3a14d0c2d0c67b56d3ad6577e6aa2d289557ae7f33261b4",
    "content_bytes": 157,
    "reservation_id": "43bf9ea0-439d-475d-a49e-0e6772fcc487",
    "provider_outcome": "UNKNOWN",
    "error_reconciled_event": "ROOT_REF01_CONNECTOR_ERROR_RECONCILED_V1",
    "unknown_event_preserved": "ROOT_REF01_CONNECTOR_UNKNOWN_V1",
    "html_retry_admitted": False,
}
ACC = "0001193125-26-330643"
FILENAME = "crda-20260630_htm.xml"
BASE = "/v1/reference/sec/filings/" + ACC + "/files"
XML_URL = "https://api.massive.com" + BASE + "/" + FILENAME
manifest = []
for rid, kind, route, params, cap in [
    (11, "xbrl_file_size_metadata", BASE, {"filename": FILENAME, "sort": "filename", "order": "asc", "limit": 10}, 131072),
    (12, "xbrl_instance_body", BASE + "/" + FILENAME, {}, 3670016),
]:
    manifest.append({
        "route_id": rid, "family_id": "REF01", "case": "CRD", "accession": ACC,
        "expected_cik": "0000025475", "kind": kind, "method": "GET", "provider": "Massive",
        "route": route, "parameters": params, "observed_filename": FILENAME,
        "observed_xbrl_instance_url": XML_URL, "metadata_anchor": metadata_anchor,
        "prior_html_failure_anchor": failure_anchor, "maximum_connector_calls": 1,
        "maximum_retained_bytes": cap, "maximum_xbrl_file_bytes_for_body_admission": 1800000,
        "v3_maximum_connector_operations": 2, "v3_maximum_retained_bytes": 3801088,
        "three_manifest_wave_maximum_retained_bytes": 25165824,
        "body_requires_prior_verified_size_receipt": rid == 12,
        "original_wire_byte_identity_claimed": False,
    })
manifest_text = json.dumps(manifest, separators=(",", ":"), ensure_ascii=False)
manifest_sha = hashlib.sha256(manifest_text.encode()).hexdigest()
all_manifests = f"'{META_MANIFEST}','{BODY_MANIFEST}','{manifest_sha}'"

anchors = r"""-- Exact retained metadata and the reconciled but still UNKNOWN HTML result are dependencies.
IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.source_document d
 WHERE d.source_id='ref01-massive-connector:2d4e9adf-4f48-4957-8f81-82f5341ba97a'
 AND d.source_sha256='f3cde4b06fc290ae57c000d4e44826f1b397e2c36f09ae3dac00b9001effcbc5'
 AND d.content_bytes=1449 AND octet_length(d.content_text)=1449
 AND encode(sha256(convert_to(d.content_text,'UTF8')),'hex')=d.source_sha256
 AND d.source_provider='Massive' AND position('__XML_URL__' IN d.content_text)>0)
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
""".replace("__XML_URL__", XML_URL)

reserve = header + """-- V3 only: route11 exact filename/size metadata, then conditional route12 XBRL body.
DO $reserve$
""" + declare + r"""v_manifest_text constant text:=$manifest$__MANIFEST__$manifest$;
v_retention_cap bigint;v_wave_bytes bigint;v_v3_bytes bigint;v_v3_requests bigint;
v_file_receipt jsonb;v_file_doc population_top3000_202608_202609_v1.source_document%rowtype;
v_file_size bigint;v_size_evidence jsonb;v_file_anchor jsonb;
BEGIN
""" + common + r"""IF c.hold_reason IS NOT NULL THEN RAISE EXCEPTION 'EXISTING_CONTROL_HOLD_MUST_BE_RECONCILED';END IF;
IF encode(sha256(convert_to(v_manifest_text,'UTF8')),'hex')<>'__SHA__' THEN RAISE EXCEPTION 'MANIFEST_BYTES_CHANGED';END IF;
m:=v_manifest_text::jsonb;
IF coalesce(current_setting('top3000.ref01_route_id',true),'')!~'^(11|12)$' THEN RAISE EXCEPTION 'EXACT_V3_ROUTE_11_OR_12_REQUIRED';END IF;
route_id:=current_setting('top3000.ref01_route_id')::integer;route:=m->(route_id-11);
IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.catalog_family_contract WHERE family_id='REF01') THEN RAISE EXCEPTION 'REF01_SCOPE_CONTRACT_REQUIRED';END IF;
IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_event e WHERE e.event='ROOT_REF01_CONNECTOR_RESERVED_V1'
 AND e.details->>'manifest_sha256'='__SHA__' AND e.details->>'route_id'=route_id::text)
 THEN RAISE EXCEPTION 'ROUTE_ALREADY_RESERVED_NO_AUTOMATIC_REISSUE';END IF;
IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_event e WHERE e.event='ROOT_REF01_CONNECTOR_RESERVED_V1'
 AND NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_event closed
 WHERE closed.details->>'reservation_id'=e.details->>'reservation_id'
 AND (closed.event='ROOT_REF01_CONNECTOR_RETAINED_V1' OR
 (closed.event='ROOT_REF01_CONNECTOR_ERROR_RECONCILED_V1' AND e.details->>'reservation_id'='43bf9ea0-439d-475d-a49e-0e6772fcc487'))))
 THEN RAISE EXCEPTION 'EARLIER_MANUAL_CONNECTOR_OUTCOME_UNRESOLVED';END IF;
__ANCHORS__
SELECT count(*) INTO v_v3_requests FROM population_top3000_202608_202609_v1.pipeline_event e
 WHERE e.event='ROOT_REF01_CONNECTOR_RESERVED_V1' AND e.details->>'manifest_sha256'='__SHA__';
IF v_v3_requests<>(route_id-11) OR v_v3_requests>=2 THEN RAISE EXCEPTION 'EXACT_V3_TWO_OPERATION_SEQUENCE_REQUIRED';END IF;
v_file_anchor:=NULL;
IF route_id=12 THEN
 SELECT e.details INTO v_file_receipt FROM population_top3000_202608_202609_v1.pipeline_event e
 WHERE e.event='ROOT_REF01_CONNECTOR_RETAINED_V1' AND e.details->>'manifest_sha256'='__SHA__'
 AND e.details->>'route_id'='11' AND e.details->>'outcome_class'='SUCCESS' ORDER BY e.at DESC LIMIT 1;
 IF v_file_receipt IS NULL THEN RAISE EXCEPTION 'RETAINED_ACTUAL_XBRL_FILE_SIZE_REQUIRED';END IF;
 SELECT d.* INTO v_file_doc FROM population_top3000_202608_202609_v1.source_document d
 WHERE d.source_id=v_file_receipt->>'source_id';
 IF NOT FOUND OR v_file_doc.source_sha256 IS DISTINCT FROM v_file_receipt->>'source_sha256'
 OR v_file_doc.source_sha256 IS DISTINCT FROM encode(sha256(convert_to(v_file_doc.content_text,'UTF8')),'hex')
 OR v_file_doc.content_bytes IS DISTINCT FROM octet_length(v_file_doc.content_text)::bigint
 OR v_file_doc.content_bytes>131072 OR v_file_doc.evidence->>'manifest_sha256' IS DISTINCT FROM '__SHA__'
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
 WHERE e.event='ROOT_REF01_CONNECTOR_RESERVED_V1' AND e.details->>'manifest_sha256' IN(__ALL_MANIFESTS__)
) q JOIN population_top3000_202608_202609_v1.source_document d ON d.source_id='ref01-massive-connector:'||q.reservation_id;
SELECT coalesce(sum(d.content_bytes),0) INTO v_v3_bytes FROM (
 SELECT DISTINCT e.details->>'reservation_id' reservation_id FROM population_top3000_202608_202609_v1.pipeline_event e
 WHERE e.event='ROOT_REF01_CONNECTOR_RESERVED_V1' AND e.details->>'manifest_sha256'='__SHA__'
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
reservation:=jsonb_build_object('reservation_id',reservation_id,'manifest_sha256','__SHA__','route_id',route_id,'route',route,
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
"""

csv_parse = r"""-- Strict bounded CSV parse of the observed connector representation, not guessed file metadata.
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
"""

retain = header + """-- V3: retain complete result, then verify bounded file-list metadata or optional exact XML string.
-- Root provides existing ref01_* GUCs and optional top3000.ref01_xml_json_path.
DO $retain$
""" + declare + r"""v_raw bytea;v_content text;v_doc jsonb;v_hash text;v_sid text;v_observed_at timestamptz;
v_tool text;outcome_class text;v_evidence jsonb;v_receipt jsonb;v_fresh integer;v_retention_cap bigint;
v_uri text;v_wave_bytes bigint;v_v3_bytes bigint;v_size_evidence jsonb;v_xml_evidence jsonb;
v_csv text;v_rows jsonb;v_fields text[];v_field text;v_quoted boolean;v_after_quote boolean;v_i integer;v_ch text;v_selected jsonb;v_file_size bigint;
v_xml_path jsonb;v_xml_path_array text[];v_xml_text text;v_xml_value xml;v_xml_bytes bigint;v_xml_hash text;
BEGIN
""" + common + r"""IF coalesce(current_setting('top3000.ref01_reservation_id',true),'')!~'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN RAISE EXCEPTION 'RESERVATION_ID_REQUIRED';END IF;
reservation_id:=current_setting('top3000.ref01_reservation_id')::uuid;
SELECT e.details INTO reservation FROM population_top3000_202608_202609_v1.pipeline_event e
WHERE e.event='ROOT_REF01_CONNECTOR_RESERVED_V1' AND e.details->>'reservation_id'=reservation_id::text ORDER BY e.at DESC LIMIT 1;
IF reservation IS NULL OR reservation->>'manifest_sha256' IS DISTINCT FROM '__SHA__'
 OR reservation->>'controller' IS DISTINCT FROM c.owner_id OR reservation->>'owner_fence' IS DISTINCT FROM c.owner_fence::text
 OR reservation->>'stop_generation' IS DISTINCT FROM c.stop_generation::text OR reservation->>'scope_sha256' IS DISTINCT FROM c.scope_sha256
 THEN RAISE EXCEPTION 'RESERVATION_OWNER_OR_MANIFEST_MISMATCH';END IF;
route:=reservation->'route';route_id:=(route->>'route_id')::integer;
IF route_id IS NULL OR route IS NULL OR route_id NOT IN(11,12) OR route IS DISTINCT FROM ($manifest$__MANIFEST__$manifest$::jsonb)->(route_id-11)
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
__CSV_PARSE__
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
 'hash_representation','EXACT_UTF8_SERIALIZATION_OF_COMPLETE_RETURNED_CONNECTOR_VALUE','reservation_id',reservation_id,'manifest_sha256','__SHA__',
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
 WHERE e.event='ROOT_REF01_CONNECTOR_RESERVED_V1' AND e.details->>'manifest_sha256' IN(__ALL_MANIFESTS__)
) q JOIN population_top3000_202608_202609_v1.source_document d ON d.source_id='ref01-massive-connector:'||q.reservation_id;
SELECT coalesce(sum(d.content_bytes),0)+octet_length(v_raw) INTO v_v3_bytes FROM (
 SELECT DISTINCT e.details->>'reservation_id' reservation_id FROM population_top3000_202608_202609_v1.pipeline_event e
 WHERE e.event='ROOT_REF01_CONNECTOR_RESERVED_V1' AND e.details->>'manifest_sha256'='__SHA__'
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
v_receipt:=jsonb_build_object('reservation_id',reservation_id,'manifest_sha256','__SHA__','route_id',route_id,'route',route,
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
"""

unknown = (P / "ref01_body_connector_unknown_v2_DRAFT.sql").read_text().replace(BODY_MANIFEST, manifest_sha).replace("v2 DRAFT", "v3 DRAFT")
unknown = unknown.replace("reservation->>'manifest_sha256'<>", "reservation->>'manifest_sha256' IS DISTINCT FROM ")
for key, value in {
    "__MANIFEST__": manifest_text,
    "__SHA__": manifest_sha,
    "__ALL_MANIFESTS__": all_manifests,
    "__ANCHORS__": anchors,
    "__CSV_PARSE__": csv_parse,
}.items():
    reserve = reserve.replace(key, value)
    retain = retain.replace(key, value)

notes = f"""# CRD XBRL v3: two conditional manual connector operations

The controller reported that Crawford's observed HTML file is4,812,375 bytes, exceeding the primary reader's4,194,304-byte limit. Its Massive HTML request ended with a transport-closed result. The exact157-byte error representation is retained as `{failure_anchor['source_id']}` / `{failure_anchor['source_sha256']}`. The UNKNOWN event is preserved, and a separate root reconciliation released unused database-storage reserve; it did not establish the provider outcome or authorize a retry.

This separate v3 wave uses the **XBRL URL already present in retained metadata source1**. It is a different observed representation, not an HTML retry. Root confirmed the current endpoint registry supports the filename filter.

| Route | Exact operation | Retained JSON cap |
|---|---|---:|
|11|GET `{BASE}` with `filename={FILENAME}`, `sort=filename`, `order=asc`, `limit=10`|131,072 bytes|
|12|GET `{BASE}/{FILENAME}` with empty parameters, only after route11's actual retained result proves a single exact matching file of1–1,800,000 bytes|3,670,016 bytes (3.5MiB)|

The manifest hash excluding its final newline is **`{manifest_sha}`**. At most two new logical connector operations are admitted. V3 retains at most3,801,088 bytes; the combined original metadata/body/v3 wave remains at most25,165,824 bytes. All current mission request/raw/relation/$250 limits remain enforced. No money, subscription, compute or disk allowance is increased.

## Controller flow

1. Finish retaining the pending DHC response, pause cron596 and drain actual leases. The v3 reserve requires no control hold and no earlier unmatched manual reservation. It explicitly recognizes only the known CRD ERROR_RECONCILED event as closure of that preserved UNKNOWN; it does not clear or reclassify it.
2. Set `top3000.ref01_route_id=11` in the reserve transaction. After a known committed reservation, call only the pinned `mcp__codex_apps__massive_call_api` route/parameters. Serialize its complete returned value once, hash the exact UTF8 JSON and retain using the usual reservation/content_base64/content_sha256/observed_at/tool_name/outcome_class GUCs.
3. Route11 retention compares the two complete CSV text copies and uses a strict bounded16KiB CSV parser supporting quoted commas,newlines and doubled quotes. Exactly one data row with unique headers must identify the observed accession/filename and an integer size. A format or size problem preserves the complete source but sets body_admission_allowed=false. The route12 reserve independently rehashes that retained source and requires its verified size evidence.
4. Only if the route12 reserve commits, request the exact observed XML filename once. The source-size screen does **not guarantee** that connector JSON escaping/duplication will fit transport or retention limits. Complete representations exceeding3.5MiB, missing/truncated results and unknown effects remain held without automatic retry. A displayed excerpt is never a complete source.
5. Retain the complete XML response representation. Optionally set `top3000.ref01_xml_json_path` to `[\"structuredContent\",\"result\"]`, `[\"content\",\"0\",\"text\"]`, or`[]` after observing the actual shape. Verification requires exact reported UTF8 length, matching duplicate strings when both exist, well-formed XML with root local-name`xbrl`, and no DTD/entity declarations. No external entity or schema request is made. Only the JSON is stored; extraction records a path/hash/length without storing a third body copy.
6. The retain event releases only its matching reservation; cron remains paused for root to resume. The unknown script preserves counters,reserved bytes and hold. No function/table/worker or fictitious pipeline attempt is created.

The scripts can be composed inside one outer rollback fixture by stripping their own BEGIN/COMMIT and readback statements. Exercise route12-before-size rejection, one correct metadata CSV, quoted-field parsing, wrong filename/oversize blocking, exact XML retention/replay, hash tamper rejection and unknown preservation. Use synthetic XML/CSV only; never make a provider call from a fixture. Actual SQL execution remains root's gate.

These are connector representations and exact extracted strings, not independently verified original HTTP bytes. Publication/receipt timestamps remain NULL, company identities remain unresolved and no shares/ranking/membership are applied. Later source-specific share/context review can consume the retained XML without a new provider call. Full invoices remain unreconciled; no zero-invoice claim is made.
"""

files = {
    "ref01_crd_xbrl_priority_manifest_v3.json": manifest_text + "\n",
    "ref01_crd_xbrl_reserve_v3_DRAFT.sql": reserve,
    "ref01_crd_xbrl_retain_v3_DRAFT.sql": retain,
    "ref01_crd_xbrl_unknown_v3_DRAFT.sql": unknown,
    "ref01_crd_xbrl_flow_v3_DRAFT.md": notes,
}
for name, content in files.items():
    (P / name).write_text(content)
result = {
    "status": "LOCAL_DRAFT_NOT_DATABASE_EXECUTED",
    "manifest_sha256": manifest_sha,
    "source_spec_authority": "ROOT_CURRENT_SESSION_CONFIRMED_PRIMARY_MASSIVE_ENDPOINT_REGISTRY",
    "maximum_additional_connector_operations": 2,
    "maximum_v3_retained_bytes": 3801088,
    "root_mission_budget_cap_usd": 250,
    "external_actions_by_author": 0,
    "files": [{"file": n, "sha256": hashlib.sha256((P / n).read_bytes()).hexdigest(), "bytes": (P / n).stat().st_size} for n in files],
}
(P / "ref01_crd_xbrl_manifest_v3_DRAFT.json").write_text(json.dumps(result, indent=2) + "\n")
print(json.dumps(result, indent=2))
