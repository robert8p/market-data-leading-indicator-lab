from pathlib import Path
import hashlib,json,sys
p=Path(__file__).parent
r=json.loads((p/'dates/independent_review.json').read_text())
for n,h in r['file_sha256'].items():
    assert hashlib.sha256((p/'dates'/n).read_bytes()).hexdigest()==h,n
for n,h in r['root_file_sha256'].items():
    assert hashlib.sha256((p/n).read_bytes()).hexdigest()==h,n
schema='population_top3000_202608_202609_v1'
owner='top3000-completion-20261024-root-5db48b32d103'
scope='8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd'
r.update(owner_id=owner,owner_fence=3,stop_generation=1,scope_sha256=scope,reviewed_manifest_sha256=r['projected']['manifest_sha256'],code_manifest=r['projected']['manifest'])
admission=dict(actor='root:'+owner,authority_event_id=9528,owner_id=owner,owner_fence=3,stop_generation=1,scope_sha256=scope,claim_event_ids=[29634,29635],assignment_event_id=28605,plan_id='ref01-date-fullperiod-v9',plan_sha256=r['projected']['plan_sha256'],max_provider_calls=1200,max_http_invocations=1200,max_source_bytes=300000000,max_combined_relation_bytes=3000000000,date_count=60,period_start='2026-08-01',period_end='2026-09-30',global_limits_unchanged=True,counters_preserved=True,original_deadline_preserved=True,authority_basis='Current user direction to complete instrument continuation and existing authority9528; new finite source admission supersedes only inherited144call allowance; original assignment history remains unchanged.',family_accepted=False)
sql=(p/'date_integration_header.sql').read_text()
sql+=f"\nCREATE TEMP TABLE ref01_integration_metadata_v9(review_event_id bigint,admission_event_id bigint);\nWITH e AS (INSERT INTO {schema}.pipeline_event(event,details) VALUES('REF01_CONTINUATION_INDEPENDENT_REVIEW_V9',$j${json.dumps(r)}$j$::jsonb) RETURNING event_id) INSERT INTO ref01_integration_metadata_v9(review_event_id) SELECT event_id FROM e;\nWITH e AS (INSERT INTO {schema}.pipeline_event(event,details) SELECT 'REF01_FULL_PERIOD_FINITE_ADMISSION_V9',$j${json.dumps(admission)}$j$::jsonb || jsonb_build_object('independent_review_event_id',review_event_id,'admitted_at',clock_timestamp()) FROM ref01_integration_metadata_v9 RETURNING event_id) UPDATE ref01_integration_metadata_v9 SET admission_event_id=e.event_id FROM e;\n"
for n in ['schema.sql','worker.sql','admit.sql','continue.sql','wrapper.sql','bundle.sql','seed.sql']:
    sql+='\n'+(p/'dates'/n).read_text()+'\n'
sql+='\n'+(p/'pipeline_tick_dates_v9.sql').read_text()+'\n;\n'
sql+=f"""DO $verify$
DECLARE r record;h text;
BEGIN
 FOR r IN SELECT key,value FROM jsonb_each_text($j${json.dumps(r['projected']['functions'])}$j$::jsonb) LOOP
  SELECT encode(sha256(convert_to(pg_get_functiondef(oid),'UTF8')),'hex') INTO STRICT h FROM pg_proc WHERE pronamespace='{schema}'::regnamespace AND proname=r.key;
  IF h IS DISTINCT FROM r.value THEN RAISE EXCEPTION 'POSTINSTALL_HASH_MISMATCH %',r.key;END IF;
 END LOOP;
 IF {schema}.ref01_date_bundle_v4() IS DISTINCT FROM '{r['projected']['bundle']}' THEN RAISE EXCEPTION 'POSTINSTALL_BUNDLE_MISMATCH';END IF;
 IF (SELECT content_sha256 FROM {schema}.ref01_date_plan_v9 WHERE plan_id='ref01-date-fullperiod-v9') IS DISTINCT FROM '{r['projected']['plan_sha256']}' THEN RAISE EXCEPTION 'POSTINSTALL_PLAN_MISMATCH';END IF;
 IF encode(sha256(convert_to({schema}.population_recovery_manifest_v1()::text,'UTF8')),'hex') IS DISTINCT FROM '{r['projected']['manifest_sha256']}' THEN RAISE EXCEPTION 'POSTINSTALL_RECOVERY_MANIFEST_MISMATCH';END IF;
 INSERT INTO {schema}.pipeline_event(event,details) SELECT 'REF01_FULL_PERIOD_CONTINUATION_DEPLOYED_V9',jsonb_build_object('owner_id','{owner}','owner_fence',3,'stop_generation',1,'scope_sha256','{scope}','code_commit','__CODE_COMMIT__','claim_event_ids',jsonb_build_array(29634,29635),'independent_review_event_id',review_event_id,'admission_event_id',admission_event_id,'plan_sha256','{r['projected']['plan_sha256']}','bundle_sha256','{r['projected']['bundle']}','recovery_manifest_sha256','{r['projected']['manifest_sha256']}','family_accepted',false,'actual_installation_hashes_verified',true) FROM ref01_integration_metadata_v9;
END;$verify$;
COMMIT;
"""
(p/'date_integration_template.sql').write_text(sql)
qual=f"BEGIN; SET LOCAL lock_timeout='3s'; SET LOCAL statement_timeout='30s'; CREATE TEMP TABLE recovery_qualification_metadata_v6(review_event_id bigint,manifest_sha256 text); WITH e AS (INSERT INTO {schema}.pipeline_event(event,details) VALUES('RECOVERY_MANIFEST_INDEPENDENT_REVIEW_V1',$j${json.dumps(r)}$j$::jsonb) RETURNING event_id) INSERT INTO recovery_qualification_metadata_v6 SELECT event_id,'{r['projected']['manifest_sha256']}' FROM e;\n"+(p/'requalification_body.sql').read_text()
(p/'date_requalification.sql').write_text(qual)
print(json.dumps({'verified_reviewed_files':len(r['file_sha256'])+len(r['root_file_sha256']),'integration_bytes':len(sql),'requalification_bytes':len(qual)}))
