"""Build a no-network, rollback-only fixture from the exact v3 anonymous blocks."""
from pathlib import Path
import hashlib
import json

P = Path(__file__).parent
parts = {}
for action in ("reserve", "retain", "unknown"):
    source = (P / f"ref01_crd_xbrl_{action}_v3_DRAFT.sql").read_text()
    parts[action] = source.split(f"DO ${action}$", 1)[1].split(f"END ${action}$;", 1)[0] + "END;\n"

fixture = """-- ROOT ONLY. No network/provider calls. All fixture writes are rolled back.
-- The exact current owner, source anchors, paused cron and drained leases are required.
-- The three pg_temp helpers contain the unchanged production-candidate DO bodies.
BEGIN;
SET LOCAL statement_timeout='30s';
SET LOCAL lock_timeout='2s';
SET LOCAL timezone='UTC';
SELECT pg_advisory_xact_lock(2026083000261022);
"""
for action, body in parts.items():
    fixture += f"CREATE OR REPLACE FUNCTION pg_temp.ref01_crd_xbrl_{action}_fixture_v3() RETURNS void LANGUAGE plpgsql AS $fixture_{action}$\n{body}$fixture_{action}$;\n"

fixture += r'''
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
  WHERE e.details->>'manifest_sha256'='__MANIFEST_SHA__'
  AND e.event IN('ROOT_REF01_CONNECTOR_RESERVED_V1','ROOT_REF01_CONNECTOR_RETAINED_V1','ROOT_REF01_CONNECTOR_UNKNOWN_V1'))
  THEN RAISE EXCEPTION 'FIXTURE_MUST_PRECEDE_REAL_V3_ADMISSION';END IF;
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
  OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_event e WHERE e.details->>'manifest_sha256'='__MANIFEST_SHA__'
   AND e.event IN('ROOT_REF01_CONNECTOR_RESERVED_V1','ROOT_REF01_CONNECTOR_RETAINED_V1','ROOT_REF01_CONNECTOR_UNKNOWN_V1'))
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
   AND e.details->>'manifest_sha256'='__MANIFEST_SHA__')<>2
  OR (SELECT count(*) FROM population_top3000_202608_202609_v1.pipeline_event e WHERE e.event='ROOT_REF01_CONNECTOR_RETAINED_V1'
   AND e.details->>'manifest_sha256'='__MANIFEST_SHA__')<>2
  OR NOT EXISTS(SELECT 1 FROM cron.job WHERE jobid=596 AND active=false) OR v_negative<>9
  THEN RAISE EXCEPTION 'TEST_FINAL_COUNTER_REPLAY_ANCHOR_OR_PAUSED_CRON_INVARIANT';END IF;
 PERFORM set_config('top3000.ref01_crd_xbrl_fixture_result',jsonb_build_object('status','PASS_ROLLBACK_ONLY',
  'synthetic_fixture',true,'provider_calls',0,'new_sources_inside_rollback',2,
  'provider_counter_delta_inside_rollback',2,'http_counter_delta_inside_rollback',2,
  'retained_bytes_inside_rollback',v_expected_bytes,'negative_assertions',v_negative,
  'quoted_csv_parser',true,'metadata_size_gate',true,'xml_root_hash_size_and_replay',true,'equal_byte_dtd_entity_rejected',true,
  'unknown_hold_and_counter_preservation',true,'prior_failure_and_metadata_unchanged',true,
  'cron_remains_paused',true,'actual_filing_content_verified',false,'company_population_certified',false,
  'manifest_sha256','__MANIFEST_SHA__')::text,true);
END $test$;
SELECT current_setting('top3000.ref01_crd_xbrl_fixture_result')::jsonb AS fixture_result;
ROLLBACK;
'''
manifest = json.loads((P / "ref01_crd_xbrl_manifest_v3_DRAFT.json").read_text())
fixture = fixture.replace("__MANIFEST_SHA__", manifest["manifest_sha256"])
path = P / "ref01_crd_xbrl_rollback_v3_DRAFT.sql"
path.write_text(fixture)
record = {"file": path.name, "sha256": hashlib.sha256(path.read_bytes()).hexdigest(), "bytes": path.stat().st_size}
manifest["files"] = [item for item in manifest["files"] if item["file"] != path.name] + [record]
manifest["rollback_fixture_database_execution"] = "NOT_RUN_BY_AUTHOR; ROOT_ACTUAL_EXECUTION_REQUIRED"
(P / "ref01_crd_xbrl_manifest_v3_DRAFT.json").write_text(json.dumps(manifest, indent=2) + "\n")
print(json.dumps(record, indent=2))
