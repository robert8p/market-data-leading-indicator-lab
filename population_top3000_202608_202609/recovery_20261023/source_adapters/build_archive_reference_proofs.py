"""Local deterministic compilation of actual audit receipts. Does not connect to any service."""
from pathlib import Path
import json, hashlib
P=Path(__file__).resolve().parent
ROOT=P.parent
S='population_top3000_202608_202609_v1'
def compact(x): return json.dumps(x,separators=(',',':'),ensure_ascii=False)
daily=json.loads((ROOT/'d02_independent_full_1541/d02_independent_reconciliation_report.json').read_text())
short=json.loads((ROOT/'s02_independent_all135_actual_1626.json').read_text())
pages=json.loads((P/'s02_all135_page_receipts_actual_1626.json').read_text())
assert daily['result']=='PASS_SOURCE_ARCHIVE_RECONCILIATION_ONLY' and len(daily['dates'])==42
assert short['result']=='PASS_SOURCE_TRANSFORMATION_ONLY' and len(pages)==135
proofs={}
proofs['D02']=[]
for i,d in enumerate(sorted(daily['dates'],key=lambda x:x['session_date']),1):
 pc=json.loads((ROOT/f"d02_independent_full_1541/postcheck_{d['session_date'].replace('-','')}.json").read_text())
 assert pc['source_id']==d['source_id'] and pc['actual_typed_manifest_sha256']==d['typed_manifest_sha256']
 proofs['D02'].append({'unit_ordinal':i,'unit_key':d['session_date'],'session_date':d['session_date'],
  'source_id':d['source_id'],'source_sha256':d['source_sha256'],'source_bytes':d['source_bytes'],
  'source_occurrences':d['source_occurrences'],'canonical_rows':d['source_occurrences'],'unit_receipt':d,'postcheck':pc})
proofs['S02']=[]
for i,r in enumerate(sorted(pages,key=lambda x:x['page_number']),1):
 assert r['page_number']==i and r['result']=='PASS_PAGE_SOURCE_TRANSFORMATION_ONLY' and set(r['mismatches'].values())=={0}
 proofs['S02'].append({'unit_ordinal':i,'unit_key':f'page:{i}','page_number':i,'archive_key':r['archive_key'],
  'source_id':r['archive_key']+f':page:{i}','source_sha256':r['source_sha256'],'source_bytes':r['retained_source_bytes'],
  'source_occurrences':r['raw_occurrences'],'canonical_rows':r['canonical_versions'],'unit_receipt':r})
for family,units in proofs.items():
 event='D02_ALL_SOURCE_DATES_INDEPENDENT_RECONCILIATION_VERIFIED' if family=='D02' else 'S02_ALL_SOURCE_PAGES_INDEPENDENT_RECONCILIATION_VERIFIED'
 receipt=daily if family=='D02' else short
 guards='''
 IF v_family='D02' THEN
  IF (SELECT jsonb_agg(u->'unit_receipt' ORDER BY (u->>'unit_ordinal')::integer) FROM jsonb_array_elements(v_units) u) IS DISTINCT FROM v_actual->'dates'
  THEN RAISE EXCEPTION 'ARCHIVE_D02_DATE_PROOFS_DO_NOT_MATCH_ACTUAL_AUDIT'; END IF;
  IF EXISTS(SELECT 1 FROM jsonb_array_elements(v_units) u WHERE u#>>'{postcheck,source_id}' IS DISTINCT FROM u->>'source_id'
   OR u#>>'{postcheck,actual_typed_manifest_sha256}' IS DISTINCT FROM u#>>'{unit_receipt,typed_manifest_sha256}'
   OR u#>'{postcheck,producer_complete}' IS DISTINCT FROM 'true'::jsonb OR u#>'{postcheck,source_present}' IS DISTINCT FROM 'true'::jsonb
   OR u#>'{postcheck,typed_occurrences}' IS DISTINCT FROM u->'canonical_rows'
   OR u#>>'{postcheck,calculated_source_sha256}' IS DISTINCT FROM u->>'source_sha256'
   OR u#>'{postcheck,calculated_source_bytes}' IS DISTINCT FROM u->'source_bytes')
  THEN RAISE EXCEPTION 'ARCHIVE_D02_POSTCHECK_BINDING_INVALID'; END IF;
 ELSE
  IF encode(sha256(convert_to((SELECT jsonb_agg(u->'unit_receipt' ORDER BY (u->>'unit_ordinal')::integer) FROM jsonb_array_elements(v_units) u)::text,'UTF8')),'hex')
   IS DISTINCT FROM v_actual->>'page_receipts_sha256'
  THEN RAISE EXCEPTION 'ARCHIVE_S02_ALL135_RECEIPT_HASH_MISMATCH'; END IF;
  IF EXISTS(SELECT 1 FROM jsonb_array_elements(v_units) u WHERE u#>>'{unit_receipt,plan_before_sha256}' IS DISTINCT FROM v_actual->>'plan_sha256'
   OR u#>'{unit_receipt,page_number}' IS DISTINCT FROM u->'unit_ordinal'
   OR u#>>'{unit_receipt,expected_canonical_manifest_sha256}' IS DISTINCT FROM u#>>'{unit_receipt,actual_canonical_manifest_sha256}')
  THEN RAISE EXCEPTION 'ARCHIVE_S02_PAGE_BINDING_INVALID'; END IF;
 END IF;
'''
 sql=f'''-- Actual independent receipt import only. No source/quality/task mutation or work admission.
BEGIN;
SET LOCAL search_path=pg_catalog; SET LOCAL timezone='UTC'; SET LOCAL statement_timeout='8s'; SET LOCAL lock_timeout='500ms';
SELECT pg_advisory_xact_lock(2026083000261022);
DO $seed$
DECLARE v_family text:='{family}';v_units jsonb:=$units${compact(units)}$units$::jsonb;
 v_actual jsonb:=$actual${compact(receipt)}$actual$::jsonb;
 v_control {S}.acquisition_control%rowtype;v_pipeline {S}.pipeline_control%rowtype;
 v_event_id bigint;v_event jsonb;v_payload_sha text;v_unit jsonb;v_proof jsonb;v_sha text;v_existing jsonb;
BEGIN
 SELECT * INTO v_control FROM {S}.acquisition_control WHERE singleton FOR UPDATE;
 SELECT * INTO v_pipeline FROM {S}.pipeline_control WHERE singleton FOR UPDATE;
 IF v_control.owner_id IS DISTINCT FROM 'top3000-recovery-20261023-root-4b1314bcb5c2'
 OR v_control.owner_fence IS DISTINCT FROM 2::bigint OR v_control.stop_generation IS DISTINCT FROM 0::bigint
 OR v_control.stopped IS DISTINCT FROM false OR v_control.hold_reason IS NOT NULL OR v_control.admitted_until IS NULL OR v_control.admitted_until<=clock_timestamp()
 OR v_pipeline.expected_owner_id IS DISTINCT FROM v_control.owner_id OR v_pipeline.expected_owner_fence IS DISTINCT FROM v_control.owner_fence
 OR v_pipeline.expected_stop_generation IS DISTINCT FROM v_control.stop_generation OR v_pipeline.scope_sha256 IS DISTINCT FROM v_control.scope_sha256
 OR v_control.scope_sha256 IS DISTINCT FROM '8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd'
 THEN RAISE EXCEPTION 'ARCHIVE_PROOF_IMPORT_OWNER_SCOPE_ADMISSION_CHANGED'; END IF;
 v_payload_sha:=encode(sha256(convert_to(v_actual::text,'UTF8')),'hex');
 SELECT ev.event_id,ev.details INTO v_event_id,v_event FROM {S}.pipeline_event ev
 WHERE ev.event='{event}' AND ev.details->'audit_payload'=v_actual
 AND ev.details->>'audit_payload_canonical_json_sha256'=v_payload_sha
 AND ev.details->>'owner_id'=v_control.owner_id AND ev.details->>'owner_fence'=v_control.owner_fence::text
 AND ev.details->>'stop_generation'=v_control.stop_generation::text AND ev.details->>'scope_sha256'=v_control.scope_sha256
 ORDER BY ev.at DESC,ev.event_id DESC LIMIT 1;
 IF NOT FOUND THEN RAISE EXCEPTION 'ARCHIVE_ACTUAL_PERSISTED_AUDIT_REQUIRED'; END IF;
 IF jsonb_array_length(v_units)<>{len(units)} OR (SELECT jsonb_agg((u->>'unit_ordinal')::integer ORDER BY (u->>'unit_ordinal')::integer) FROM jsonb_array_elements(v_units) u)
 IS DISTINCT FROM (SELECT jsonb_agg(i ORDER BY i) FROM generate_series(1,{len(units)}) i)
 THEN RAISE EXCEPTION 'ARCHIVE_COMPLETE_UNIT_ORDINAL_SET_REQUIRED'; END IF;
 {guards}
 FOR v_unit IN SELECT value FROM jsonb_array_elements(v_units) LOOP
  v_proof:=v_unit-'unit_ordinal'-'unit_key';v_sha:=encode(sha256(convert_to(v_proof::text,'UTF8')),'hex');
  INSERT INTO {S}.archive_source_proof_v1(family_id,unit_ordinal,unit_key,proof,proof_sha256,audit_event_id,audit_payload_sha256)
  VALUES(v_family,(v_unit->>'unit_ordinal')::integer,v_unit->>'unit_key',v_proof,v_sha,v_event_id,v_payload_sha)
  ON CONFLICT(family_id,unit_ordinal) DO NOTHING;
  SELECT jsonb_build_object('proof',p.proof,'proof_sha256',p.proof_sha256,'unit_key',p.unit_key,'audit_event_id',p.audit_event_id,'audit_payload_sha256',p.audit_payload_sha256)
  INTO v_existing FROM {S}.archive_source_proof_v1 p WHERE p.family_id=v_family AND p.unit_ordinal=(v_unit->>'unit_ordinal')::integer;
  IF v_existing IS DISTINCT FROM jsonb_build_object('proof',v_proof,'proof_sha256',v_sha,'unit_key',v_unit->>'unit_key','audit_event_id',v_event_id,'audit_payload_sha256',v_payload_sha)
  THEN RAISE EXCEPTION 'ARCHIVE_ACTUAL_PROOF_IMMUTABLE_CONFLICT'; END IF;
 END LOOP;
 IF (SELECT count(*) FROM {S}.archive_source_proof_v1 WHERE family_id=v_family)<>{len(units)}
 THEN RAISE EXCEPTION 'ARCHIVE_UNEXPECTED_PROOF_UNITS'; END IF;
END $seed$;
SELECT family_id,count(*) source_units,sum((proof->>'canonical_rows')::bigint) canonical_rows,
 encode(sha256(convert_to(string_agg(unit_ordinal::text||':'||proof_sha256,E'\\n' ORDER BY unit_ordinal),'UTF8')),'hex') all_source_proof_set_sha256
FROM {S}.archive_source_proof_v1 WHERE family_id='{family}' GROUP BY family_id;
COMMIT;
'''
 (P/f'archive_reference_proofs_{family}_seed_DRAFT.sql').write_text(sql)
print(json.dumps({'D02_units':len(proofs['D02']),'S02_units':len(proofs['S02']),
 'D02_rows':sum(u['canonical_rows'] for u in proofs['D02']),'S02_rows':sum(u['canonical_rows'] for u in proofs['S02'])}))
