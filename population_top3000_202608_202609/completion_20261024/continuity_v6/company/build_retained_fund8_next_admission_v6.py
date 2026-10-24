"""Generate private fenced V4 admission batch from exact independent approvals.
No private source bytes or credentials reside in this safe builder.
"""
from pathlib import Path
import json,hashlib,sys,uuid
D=Path('continuity_v6/company');C=D/'private_retained_fund8_next_candidates_v6.json'
C_SHA='bdc4301991624903f6b6284fd6e753b627ed96caea2581ba66809eaa4c16576e'
def literal(x):return "'"+json.dumps(x,separators=(',',':')).replace("'","''")+"'"
def main(receipt_path,output_path):
 assert hashlib.sha256(C.read_bytes()).hexdigest()==C_SHA
 cs=json.loads(C.read_text());rs=json.loads(Path(receipt_path).read_text());rs=rs if isinstance(rs,list)else rs['receipts'];assert len(rs)==len(cs)==8
 pins={r['case_key']:r for r in json.loads((D/'retained_fund8_next_canonical_pins_v6.json').read_text())};reviews={r['result']['case_key']:r for r in rs};items=[]
 for c in cs:
  r=reviews[c['case_key']];p=pins[c['case_key']];v=r['result'];assert v['kind']=='REF01_CASE_EVIDENCE_REVIEW_V3' and v['result']=='PASS' and v['proposal_sha256']==p['proposal_sha256'] and v['source_refs_sha256']==p['source_refs_sha256'];assert v['expected_parent_sha256']==c['expected_parent_sha256'] and v['produced_by']==c['producer_id'] and v['reviewed_by']!=v['produced_by'] and v['independent_of_producer'] is True
  vid=str(uuid.uuid5(uuid.NAMESPACE_URL,'top3000-continuity-fund8_next:'+hashlib.sha256(json.dumps(r,sort_keys=True,separators=(',',':')).encode()).hexdigest()));items.append({'candidate':c,'receipt':r,'pins':p,'verification_id':vid})
 sql="""BEGIN;
SET LOCAL lock_timeout='15s';SET LOCAL statement_timeout='45s';
DO $fund8_next$
DECLARE a population_top3000_202608_202609_v1.acquisition_control%rowtype;p population_top3000_202608_202609_v1.pipeline_control%rowtype;
 items jsonb:=__ITEMS__::jsonb;i jsonb;q jsonb;r jsonb;v jsonb;vid uuid;first_result jsonb;replay_result jsonb;results jsonb:='[]'::jsonb;
BEGIN
 PERFORM pg_advisory_xact_lock(2026083000261022);
 SELECT * INTO STRICT a FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton FOR UPDATE;
 SELECT * INTO STRICT p FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton FOR UPDATE;
 IF a.owner_id IS DISTINCT FROM 'top3000-completion-20261024-root-5db48b32d103' OR a.owner_fence IS DISTINCT FROM 3 OR a.stop_generation IS DISTINCT FROM 1
 OR a.enabled IS DISTINCT FROM true OR a.stopped IS DISTINCT FROM false OR a.hold_reason IS NOT NULL OR p.enabled IS DISTINCT FROM true
 OR p.expected_owner_id IS DISTINCT FROM a.owner_id OR p.expected_owner_fence IS DISTINCT FROM 3 OR p.expected_stop_generation IS DISTINCT FROM 1
 OR a.scope_sha256 IS DISTINCT FROM '8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd' OR p.scope_sha256 IS DISTINCT FROM a.scope_sha256
 OR a.admitted_until<=clock_timestamp() OR p.deadline<=clock_timestamp() OR a.admitted_until IS NULL OR p.deadline IS NULL
 OR p.budget_evidence#>>'{monetary_policy,authority_event_id}' IS DISTINCT FROM '9528'
 THEN RAISE EXCEPTION 'FUND8_NEXT_CURRENT_AUTHORITY_REQUIRED';END IF;
 IF jsonb_array_length(items)<>8 OR encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.ref01_review_admit_case_v4(text,text,text,jsonb,jsonb,uuid)'::regprocedure),'UTF8')),'hex') IS DISTINCT FROM '9ccdcdbc8f7746886937f894ce0b1cd1e418fcabb0c6e8f2bb59c8c941cf7f51' THEN RAISE EXCEPTION 'FUND8_NEXT_ADMIT_PREIMAGE_CHANGED';END IF;
 FOR i IN SELECT value FROM jsonb_array_elements(items) LOOP
 q:=i->'candidate';r:=i->'receipt';v:=r->'result';vid:=(i->>'verification_id')::uuid;
 IF population_top3000_202608_202609_v1.ref01_adjudication_hash_v3(q->'proposal') IS DISTINCT FROM i#>>'{pins,proposal_sha256}'
 OR population_top3000_202608_202609_v1.ref01_adjudication_hash_v3(q->'source_refs') IS DISTINCT FROM i#>>'{pins,source_refs_sha256}'
 OR NOT population_top3000_202608_202609_v1.ref01_review_authorized_v4(v)
 THEN RAISE EXCEPTION 'FUND8_NEXT_PINNED_TRUE_INDEPENDENT_REVIEW_REQUIRED';END IF;
 PERFORM population_top3000_202608_202609_v1.ref01_adjudication_sources_v3(q->'source_refs');
 INSERT INTO population_top3000_202608_202609_v1.company_verification_v1(verification_id,verifier_version,result)
 VALUES(vid,r->>'verifier_version',v) ON CONFLICT DO NOTHING;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.company_verification_v1 WHERE verification_id=vid AND verifier_version=r->>'verifier_version' AND result=v) THEN RAISE EXCEPTION 'FUND8_NEXT_RECEIPT_REPLAY_CONFLICT';END IF;
 first_result:=population_top3000_202608_202609_v1.ref01_review_admit_case_v4(q->>'run_key',q->>'case_key',q->>'expected_parent_sha256',q->'proposal',q->'source_refs',vid);
 replay_result:=population_top3000_202608_202609_v1.ref01_review_admit_case_v4(q->>'run_key',q->>'case_key',q->>'expected_parent_sha256',q->'proposal',q->'source_refs',vid);
 IF first_result IS DISTINCT FROM replay_result THEN RAISE EXCEPTION 'FUND8_NEXT_NONIDEMPOTENT_ADMISSION';END IF;
 results:=results||jsonb_build_array(jsonb_build_object('case_key',q->>'case_key','task',first_result,'verification_id',vid));
 END LOOP;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton AND provider_requests=a.provider_requests AND http_invocations=a.http_invocations AND stored_bytes=a.stored_bytes AND dispatch_calls=a.dispatch_calls AND owner_id=a.owner_id AND owner_fence=a.owner_fence AND stop_generation=a.stop_generation) THEN RAISE EXCEPTION 'FUND8_NEXT_COUNTERS_CHANGED';END IF;
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES('CONTINUITY_RETAINED_FUND8_NEXT_CLASSIFICATION_ADMITTED_V6',jsonb_build_object('results',results,'cases',8,'provider_calls',0,'new_source_bytes',0,'counters_preserved',true,'dedup_replay_pass',true,'full_company_population_accepted',false,'owner_id',a.owner_id,'owner_fence',a.owner_fence,'stop_generation',a.stop_generation,'scope_sha256',a.scope_sha256));
END;$fund8_next$;
COMMIT;
""".replace('__ITEMS__',literal(items))
 Path(output_path).write_text(sql);print(json.dumps({'sql_sha256':hashlib.sha256(sql.encode()).hexdigest(),'sql_bytes':len(sql.encode()),'private_output':True,'verification_ids':[i['verification_id']for i in items]}))
if __name__=='__main__':main(sys.argv[1],sys.argv[2])
