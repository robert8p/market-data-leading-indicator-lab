BEGIN;
SET LOCAL lock_timeout='15s'; SET LOCAL statement_timeout='180s';
SELECT population_top3000_202608_202609_v1.continuation_guard_v6('top3000-completion-20261024-root-5db48b32d103',3,1);
CREATE TABLE population_top3000_202608_202609_v1.ref01_retained_alias_point_v1(
 manifest_sha256 text NOT NULL, company_id text NOT NULL, point_date date NOT NULL,
 source_id text NOT NULL, source_ordinal integer NOT NULL, source_sha256 text NOT NULL,
 anchor_sha256 text NOT NULL, normalized_sha256 text NOT NULL,
 content jsonb NOT NULL, record_sha256 text NOT NULL,
 PRIMARY KEY(manifest_sha256,company_id,point_date),
 UNIQUE(manifest_sha256,source_id,source_ordinal),
 CHECK(content->>'evidence_semantics'='STRONG_TWO_FIGI_ALTERNATE_TICKER_DATE_POINT_NOT_INTERVAL_V1'),
 CHECK(content->>'full_period_qualified'='false' AND content->>'family_accepted'='false' AND content->>'original_historical_receipt'='false'),
 CHECK(record_sha256=encode(sha256(convert_to(content::text,'UTF8')),'hex'))
);
ALTER TABLE population_top3000_202608_202609_v1.ref01_retained_alias_point_v1 ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON population_top3000_202608_202609_v1.ref01_retained_alias_point_v1 FROM PUBLIC,anon,authenticated;
CREATE FUNCTION population_top3000_202608_202609_v1.ref01_alias_point_append_v2()
RETURNS jsonb LANGUAGE plpgsql SECURITY INVOKER SET search_path='pg_catalog' SET "TimeZone"='UTC' AS $append$
DECLARE ctl jsonb;cl jsonb;ass jsonb;x jsonb;pin jsonb;rr record;aa record;sd record;pg record;
 c jsonb;out_hash text;tid uuid;cid uuid;aid uuid;tok text;self_sha text;review_id bigint;ar record;tr record;newrows integer:=0;sz bigint;mission_before bigint;mission_after bigint;count_strong integer;strong_counts jsonb;
 manifest constant text:='aee4f53ab389fe6fe0ca0fedc7664c838b83a6d8f192f69f8f0fe0a32e6eb2d7';
BEGIN
ctl:=population_top3000_202608_202609_v1.continuation_guard_v6('top3000-completion-20261024-root-5db48b32d103',3,1);
SELECT details INTO STRICT cl FROM population_top3000_202608_202609_v1.pipeline_event WHERE event_id=42511 AND event='ENGINEERING_ASSIGNMENT_CLAIMED_V5';
SELECT details INTO STRICT ass FROM population_top3000_202608_202609_v1.pipeline_event WHERE event_id=40974 AND event='ENGINEERING_IMPLEMENTATION_ASSIGNMENT_V5';
IF cl->>'assignment_details_sha256' IS DISTINCT FROM '42462f2e912eec964b942209959fa5469dc7d9f46c9077f3174a1c6625fa60e5' OR cl->>'assignment_event_id' IS DISTINCT FROM '40974' OR cl->>'executor' IS DISTINCT FROM 'automation:6adc2c96db348191bca7cf4eacfdac32' OR cl->>'owner_id' IS DISTINCT FROM ctl->>'owner_id' OR cl->>'owner_fence' IS DISTINCT FROM '3' OR cl->>'stop_generation' IS DISTINCT FROM '1' OR cl->>'scope_sha256' IS DISTINCT FROM ctl->>'scope_sha256' OR (cl->>'expires_at')::timestamptz<=clock_timestamp() OR (ass->>'not_after')::timestamptz<=clock_timestamp() OR encode(sha256(convert_to(ass::text,'UTF8')),'hex') IS DISTINCT FROM '42462f2e912eec964b942209959fa5469dc7d9f46c9077f3174a1c6625fa60e5' OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_event WHERE event IN('ENGINEERING_ASSIGNMENT_COMPLETED_V5','ENGINEERING_ASSIGNMENT_EFFECTS_RECONCILED_RELEASE_V5') AND details->>'claim_event_id'='42511') THEN RAISE EXCEPTION 'ALIAS_V1_EXACT_ACTIVE_CLAIM';END IF;
SELECT encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.ref01_alias_point_append_v2()'::regprocedure),'UTF8')),'hex') INTO self_sha;
SELECT event_id INTO review_id FROM population_top3000_202608_202609_v1.pipeline_event WHERE event='REF01_ALIAS_V2_EXECUTOR_REVIEW_V1'
AND details->>'claim_event_id'='42511' AND details->>'assignment_event_id'='40974' AND details->>'function_sha256'=self_sha
AND details->>'approved'='true' AND details->>'execution_binding'='REVIEWED_PRIVATE_FUNCTION_ONE_SHOT'
AND details->>'owner_id'=ctl->>'owner_id' AND details->>'owner_fence'='3' AND details->>'stop_generation'='1' AND details->>'scope_sha256'=ctl->>'scope_sha256'
AND nullif(details->>'producer','') IS NOT NULL AND nullif(details->>'reviewer','') IS NOT NULL AND details->>'producer' IS DISTINCT FROM details->>'reviewer'
ORDER BY event_id DESC LIMIT 1;
IF review_id IS NULL THEN RAISE EXCEPTION 'ALIAS_V2_SELF_CODE_REVIEW_REQUIRED';END IF;
IF jsonb_array_length(ass->'input_manifest')<>99 OR encode(sha256(convert_to((ass->'input_manifest')::text,'UTF8')),'hex') IS DISTINCT FROM manifest THEN RAISE EXCEPTION 'ALIAS_V1_MANIFEST_CHANGED';END IF;
IF EXISTS(SELECT 1 FROM jsonb_each_text(ass->'fresh_code_preimages') z LEFT JOIN pg_proc p ON p.proname=z.key AND p.pronamespace='population_top3000_202608_202609_v1'::regnamespace WHERE p.oid IS NULL OR encode(sha256(convert_to(pg_get_functiondef(p.oid),'UTF8')),'hex') IS DISTINCT FROM z.value) THEN RAISE EXCEPTION 'ALIAS_V1_CODE_CHANGED';END IF;
IF (SELECT jsonb_array_length(content->'companies') FROM population_top3000_202608_202609_v1.ref01_practical_snapshot_v1 WHERE snapshot_sha256=ass#>>'{input_versions,cohort_sha256}') IS DISTINCT FROM 3000 THEN RAISE EXCEPTION 'ALIAS_V1_DENOMINATOR';END IF;
SELECT sum(pg_total_relation_size(oid)) INTO mission_before FROM pg_class WHERE relnamespace='population_top3000_202608_202609_v1'::regnamespace AND relkind IN('r','m');
IF mission_before>12000000000 THEN RAISE EXCEPTION 'ALIAS_V1_GLOBAL_RELATION_BOUND';END IF;
sz:=pg_total_relation_size('population_top3000_202608_202609_v1.ref01_retained_alias_point_v1');
IF (SELECT count(*) FROM population_top3000_202608_202609_v1.ref01_retained_alias_point_v1 WHERE manifest_sha256=manifest)=0 THEN
 tid:=population_top3000_202608_202609_v1.pipeline_enqueue_v1('REF01','DERIVE','retained-alias-point-v1:'||manifest,jsonb_build_object('kind','REF01_RETAINED_ALIAS_POINT_V1','assignment_event_id',40974),ass->'input_versions');
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=tid AND (status<>'READY' OR executions<>0)) THEN RAISE EXCEPTION 'ALIAS_V1_TASK_PREEXISTS';END IF;
 -- A genuine direct SQL execution attempt. The compatibility adapter_sha256 column
 -- binds this reviewed private function, never a scheduler adapter that did not run.
 aid:=gen_random_uuid();tok:=gen_random_uuid()::text||gen_random_uuid()::text;
 INSERT INTO population_top3000_202608_202609_v1.pipeline_attempt(attempt_id,task_id,execution_number,token_sha256,owner_fence,stop_generation,adapter_sha256,outcome)
 VALUES(aid,tid,1,encode(sha256(convert_to(tok,'UTF8')),'hex'),3,1,self_sha,'RUNNING');
 UPDATE population_top3000_202608_202609_v1.pipeline_task SET status='LEASED',attempt_id=aid,owner_fence=3,stop_generation=1,executions=1,started_at=clock_timestamp(),lease_until=least((cl->>'expires_at')::timestamptz,clock_timestamp()+interval '5 minutes') WHERE task_id=tid;
 SELECT * INTO STRICT ar FROM population_top3000_202608_202609_v1.pipeline_attempt WHERE attempt_id=aid FOR UPDATE;
 SELECT * INTO STRICT tr FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=tid FOR UPDATE;
 IF ar.task_id IS DISTINCT FROM tid OR ar.token_sha256 IS DISTINCT FROM encode(sha256(convert_to(tok,'UTF8')),'hex') OR ar.adapter_sha256 IS DISTINCT FROM self_sha OR ar.owner_fence<>3 OR ar.stop_generation<>1 OR ar.outcome IS DISTINCT FROM 'RUNNING' OR tr.status IS DISTINCT FROM 'LEASED' OR tr.attempt_id IS DISTINCT FROM aid OR tr.lease_until<=clock_timestamp() OR tr.owner_fence<>3 OR tr.stop_generation<>1 THEN RAISE EXCEPTION 'ALIAS_V2_ONE_SHOT_ATTEMPT_BINDING';END IF;
END IF;

-- Recompute uniqueness once over all retained rows on the exact case dates.
-- Both FIGIs are mandatory; CIK alone never qualifies or groups a company.
WITH pins AS MATERIALIZED(SELECT value x FROM jsonb_array_elements(ass->'input_manifest')),
anchors AS MATERIALIZED(SELECT a.company_id,a.content FROM population_top3000_202608_202609_v1.ref01_practical_instrument_point_v3 a
 WHERE a.eligible AND a.point_date='2026-08-03' AND a.cohort_sha256=ass#>>'{input_versions,cohort_sha256}'
 AND a.company_id IN(SELECT pins.x->>'company_id' FROM pins)),
counts AS MATERIALIZED(SELECT a.company_id,r.point_date,count(*) n
 FROM anchors a JOIN population_top3000_202608_202609_v1.ref01_date_record_v4 r
 ON r.content#>>'{source_record,composite_figi}'=a.content#>>'{source_record,composite_figi}'
 AND r.content#>>'{source_record,share_class_figi}'=a.content#>>'{source_record,share_class_figi}'
 AND r.content#>>'{source_record,cik}' IS NOT DISTINCT FROM a.content#>>'{source_record,cik}'
 AND r.content#>>'{source_record,type}'=a.content#>>'{source_record,type}'
 AND r.content#>>'{source_record,primary_exchange}'=a.content#>>'{source_record,primary_exchange}'
 AND r.content#>>'{source_record,active}'='true' AND r.content#>>'{source_record,market}'='stocks' AND r.content#>>'{source_record,locale}'='us'
 WHERE r.point_date IN(SELECT DISTINCT (pins.x->>'point_date')::date FROM pins)
 AND nullif(a.content#>>'{source_record,composite_figi}','') IS NOT NULL AND nullif(a.content#>>'{source_record,share_class_figi}','') IS NOT NULL
 GROUP BY a.company_id,r.point_date)
SELECT coalesce(jsonb_object_agg(company_id||'|'||point_date::text,n),'{}'::jsonb) INTO strong_counts FROM counts;

FOR x IN SELECT value FROM jsonb_array_elements(ass->'input_manifest') LOOP
 SELECT value INTO STRICT pin FROM jsonb_array_elements(x->'candidate_pins') WHERE value->>'both_figi'='true' AND value->>'context_ok'='true';
 SELECT * INTO STRICT rr FROM population_top3000_202608_202609_v1.ref01_date_record_v4 WHERE source_id=pin->>'source_id' AND ordinal=(pin->>'ordinal')::integer FOR SHARE;
 SELECT * INTO STRICT aa FROM population_top3000_202608_202609_v1.ref01_practical_instrument_point_v3 WHERE eligible AND point_date='2026-08-03' AND cohort_sha256=ass#>>'{input_versions,cohort_sha256}' AND content->>'company_id'=x->>'company_id' FOR SHARE;
 SELECT * INTO STRICT sd FROM population_top3000_202608_202609_v1.source_document WHERE source_id=rr.source_id FOR SHARE;
 SELECT * INTO STRICT pg FROM population_top3000_202608_202609_v1.ref01_date_page_v4 WHERE source_id=rr.source_id FOR SHARE;
 IF rr.record_sha256 IS DISTINCT FROM pin->>'record_sha256' OR rr.record_sha256 IS DISTINCT FROM encode(sha256(convert_to(rr.content::text,'UTF8')),'hex') OR aa.record_sha256 IS DISTINCT FROM x->>'expected_anchor_sha' OR aa.record_sha256 IS DISTINCT FROM pin->>'anchor_record_sha256' OR aa.record_sha256 IS DISTINCT FROM encode(sha256(convert_to(aa.content::text,'UTF8')),'hex') OR rr.point_date IS DISTINCT FROM (x->>'point_date')::date OR rr.content->>'source_id' IS DISTINCT FROM rr.source_id OR rr.content->>'source_ordinal' IS DISTINCT FROM rr.ordinal::text OR rr.content->>'point_date' IS DISTINCT FROM rr.point_date::text OR rr.content->>'source_sha256' IS DISTINCT FROM sd.source_sha256 OR pg.source_sha256 IS DISTINCT FROM sd.source_sha256 OR sd.source_sha256 IS DISTINCT FROM encode(sha256(convert_to(sd.content_text,'UTF8')),'hex') OR sd.content_bytes IS DISTINCT FROM octet_length(sd.content_text)::bigint OR rr.content->'source_record' IS DISTINCT FROM (sd.content_text::jsonb->'results'->(rr.ordinal-1)) THEN RAISE EXCEPTION 'ALIAS_V1_SOURCE_OR_ANCHOR_CHANGED';END IF;
 IF nullif(aa.content#>>'{source_record,composite_figi}','') IS NULL OR nullif(aa.content#>>'{source_record,share_class_figi}','') IS NULL OR rr.content#>>'{source_record,composite_figi}' IS DISTINCT FROM aa.content#>>'{source_record,composite_figi}' OR rr.content#>>'{source_record,share_class_figi}' IS DISTINCT FROM aa.content#>>'{source_record,share_class_figi}' OR rr.content#>>'{source_record,cik}' IS DISTINCT FROM aa.content#>>'{source_record,cik}' OR rr.content#>>'{source_record,type}' IS DISTINCT FROM aa.content#>>'{source_record,type}' OR rr.content#>>'{source_record,primary_exchange}' IS DISTINCT FROM aa.content#>>'{source_record,primary_exchange}' OR rr.content#>>'{source_record,active}' IS DISTINCT FROM 'true' OR rr.content#>>'{source_record,market}' IS DISTINCT FROM 'stocks' OR rr.content#>>'{source_record,locale}' IS DISTINCT FROM 'us' OR rr.content#>>'{source_record,ticker}' IS NOT DISTINCT FROM aa.ticker THEN RAISE EXCEPTION 'ALIAS_V1_STRONG_POINT_REQUIRED';END IF;
 count_strong:=coalesce((strong_counts->>(x->>'company_id'||'|'||rr.point_date::text))::integer,0);
 IF count_strong<>1 THEN RAISE EXCEPTION 'ALIAS_V1_UNIQUE_POINT_REQUIRED';END IF;
 c:=jsonb_build_object('company_id',x->>'company_id','point_date',rr.point_date,'ticker',rr.content#>>'{source_record,ticker}','source_id',rr.source_id,'source_ordinal',rr.ordinal,'source_sha256',sd.source_sha256,'normalized_sha256',rr.record_sha256,'anchor_sha256',aa.record_sha256,'cohort_sha256',ass#>>'{input_versions,cohort_sha256}','policy_sha256',ass#>>'{input_versions,policy_sha256}','membership_acceptance_sha256',ass#>>'{input_versions,membership_acceptance_sha256}','evidence_semantics','STRONG_TWO_FIGI_ALTERNATE_TICKER_DATE_POINT_NOT_INTERVAL_V1','full_period_qualified',false,'family_accepted',false,'original_historical_receipt',false);
 INSERT INTO population_top3000_202608_202609_v1.ref01_retained_alias_point_v1 VALUES(manifest,x->>'company_id',rr.point_date,rr.source_id,rr.ordinal,sd.source_sha256,aa.record_sha256,rr.record_sha256,c,encode(sha256(convert_to(c::text,'UTF8')),'hex')) ON CONFLICT DO NOTHING;
 IF FOUND THEN newrows:=newrows+1;END IF;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.ref01_retained_alias_point_v1 q WHERE q.manifest_sha256=manifest AND q.company_id=x->>'company_id' AND q.point_date=rr.point_date AND q.content=c) THEN RAISE EXCEPTION 'ALIAS_V1_IMMUTABLE_CONFLICT';END IF;
END LOOP;
IF (SELECT count(*) FROM population_top3000_202608_202609_v1.ref01_retained_alias_point_v1 WHERE manifest_sha256=manifest)<>99 OR pg_total_relation_size('population_top3000_202608_202609_v1.ref01_retained_alias_point_v1')-sz>33554432 THEN RAISE EXCEPTION 'ALIAS_V1_COUNT_OR_GROWTH';END IF;
SELECT encode(sha256(convert_to(string_agg(record_sha256,E'\n' ORDER BY company_id,point_date),'UTF8')),'hex') INTO out_hash FROM population_top3000_202608_202609_v1.ref01_retained_alias_point_v1 WHERE manifest_sha256=manifest;
IF newrows NOT IN(0,99) THEN RAISE EXCEPTION 'ALIAS_V1_PARTIAL_PRIOR_OUTPUT';END IF;
IF newrows=99 THEN
 SELECT * INTO STRICT ar FROM population_top3000_202608_202609_v1.pipeline_attempt WHERE attempt_id=aid FOR UPDATE;
 SELECT * INTO STRICT tr FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=tid FOR UPDATE;
 IF self_sha IS DISTINCT FROM encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.ref01_alias_point_append_v2()'::regprocedure),'UTF8')),'hex') OR ar.task_id IS DISTINCT FROM tid OR ar.token_sha256 IS DISTINCT FROM encode(sha256(convert_to(tok,'UTF8')),'hex') OR ar.adapter_sha256 IS DISTINCT FROM self_sha OR ar.owner_fence<>3 OR ar.stop_generation<>1 OR ar.outcome IS DISTINCT FROM 'RUNNING' OR tr.status IS DISTINCT FROM 'LEASED' OR tr.attempt_id IS DISTINCT FROM aid OR tr.lease_until<=clock_timestamp() OR tr.owner_fence<>3 OR tr.stop_generation<>1 THEN RAISE EXCEPTION 'ALIAS_V2_ONE_SHOT_ATTEMPT_BINDING';END IF;
 INSERT INTO population_top3000_202608_202609_v1.pipeline_commit(attempt_id,task_id,commit_key,output_sha256,receipt,new_committed_units) VALUES(aid,tid,'retained-alias-point-v1:'||manifest,out_hash,jsonb_build_object('assignment_event_id',40974,'claim_event_id',42511,'rows',99,'evidence_semantics','POINT_ONLY_NOT_INTERVAL','family_accepted',false,'source_calls',0,'executor_kind','REVIEWED_PRIVATE_FUNCTION_ONE_SHOT','function_sha256',self_sha,'independent_review_event_id',review_id,'adapter_registry_used',false,'pipeline_assert_attempt_v1_used',false),99) RETURNING commit_id INTO cid;
 UPDATE population_top3000_202608_202609_v1.pipeline_task SET lease_until=NULL,status='COMPLETE',owner_fence=3,stop_generation=1,executions=1,useful_committed_units=99,last_useful_progress_at=clock_timestamp(),completed_at=clock_timestamp(),checkpoint=jsonb_build_object('output_sha256',out_hash,'rows',99,'quality_held',true) WHERE task_id=tid;
 UPDATE population_top3000_202608_202609_v1.pipeline_attempt SET outcome='COMPLETE',ended_at=clock_timestamp() WHERE attempt_id=aid;
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES('REF01_RETAINED_ALIAS_POINT_APPENDED_V2',ctl||jsonb_build_object('assignment_event_id',40974,'claim_event_id',42511,'task_id',tid,'commit_id',cid,'attempt_id',aid,'function_sha256',self_sha,'execution_binding','REVIEWED_PRIVATE_FUNCTION_ONE_SHOT','adapter_registry_used',false,'output_sha256',out_hash,'manifest_sha256',manifest,'new_committed_units',99,'intervals_qualified',false,'family_accepted',false));
ELSIF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_commit pc JOIN population_top3000_202608_202609_v1.pipeline_attempt pa USING(attempt_id) JOIN population_top3000_202608_202609_v1.pipeline_task pt ON pt.task_id=pc.task_id WHERE pc.commit_key='retained-alias-point-v1:'||manifest AND pc.output_sha256=out_hash AND pc.new_committed_units=99 AND pa.task_id=pt.task_id AND pt.attempt_id=pa.attempt_id AND pt.status='COMPLETE' AND pa.outcome='COMPLETE' AND pa.adapter_sha256=self_sha AND pc.receipt->>'executor_kind'='REVIEWED_PRIVATE_FUNCTION_ONE_SHOT') THEN RAISE EXCEPTION 'ALIAS_V1_REPLAY_RECEIPT_REQUIRED';END IF;
SELECT sum(pg_total_relation_size(oid)) INTO mission_after FROM pg_class WHERE relnamespace='population_top3000_202608_202609_v1'::regnamespace AND relkind IN('r','m');
IF mission_after>12000000000 OR mission_after-mission_before>33554432 THEN RAISE EXCEPTION 'ALIAS_V1_CUMULATIVE_RELATION_BOUND';END IF;
RETURN jsonb_build_object('new_rows',newrows,'rows',99,'output_sha256',out_hash,'task_id',tid,'attempt_id',aid,'commit_id',cid,'function_sha256',self_sha,'review_id',review_id,'family_accepted',false);
END $append$;
REVOKE ALL ON FUNCTION population_top3000_202608_202609_v1.ref01_alias_point_append_v2() FROM PUBLIC,anon,authenticated;

INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) SELECT 'REF01_ALIAS_V2_EXECUTOR_REVIEW_V1', population_top3000_202608_202609_v1.continuation_guard_v6('top3000-completion-20261024-root-5db48b32d103',3,1)||jsonb_build_object('claim_event_id',42511,'assignment_event_id',40974,'function_sha256',encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.ref01_alias_point_append_v2()'::regprocedure),'UTF8')),'hex'),'execution_binding','REVIEWED_PRIVATE_FUNCTION_ONE_SHOT','approved',true,'fixture_only',true,'producer','delegated-agent:/root/alias_v2_review','reviewer','automation:6adc2c96db348191bca7cf4eacfdac32');

CREATE TEMP TABLE alias_review_before AS SELECT
 (SELECT to_jsonb(a) FROM population_top3000_202608_202609_v1.acquisition_control a) acquisition,
 (SELECT to_jsonb(p) FROM population_top3000_202608_202609_v1.pipeline_control p) pipeline,
 (SELECT encode(sha256(convert_to(jsonb_agg(to_jsonb(r) ORDER BY r.source_id,r.ordinal)::text,'UTF8')),'hex') FROM population_top3000_202608_202609_v1.ref01_date_record_v4 r WHERE (r.source_id,r.ordinal) IN(SELECT p->>'source_id',(p->>'ordinal')::int FROM population_top3000_202608_202609_v1.pipeline_event e CROSS JOIN LATERAL jsonb_array_elements(e.details->'input_manifest') x CROSS JOIN LATERAL jsonb_array_elements(x->'candidate_pins')p WHERE event_id=40974)) legacy_hash,
 (SELECT jsonb_array_length(content->'companies') FROM population_top3000_202608_202609_v1.ref01_practical_snapshot_v1 WHERE snapshot_sha256='2dc0f766befed6de6b052221c3c87482d507ba1bad931cbb98654eb3af899bec') company_count,
 (SELECT encode(sha256(convert_to(string_agg(encode(sha256(convert_to(to_jsonb(t)::text,'UTF8')),'hex'),E'\n' ORDER BY task_id),'UTF8')),'hex') FROM population_top3000_202608_202609_v1.pipeline_task t) task_hash;
SELECT population_top3000_202608_202609_v1.ref01_alias_point_append_v2();
CREATE TEMP TABLE alias_review_first AS SELECT * FROM population_top3000_202608_202609_v1.ref01_retained_alias_point_v1;
CREATE TEMP TABLE alias_review_first_commit AS SELECT * FROM population_top3000_202608_202609_v1.pipeline_commit WHERE commit_key='retained-alias-point-v1:aee4f53ab389fe6fe0ca0fedc7664c838b83a6d8f192f69f8f0fe0a32e6eb2d7';
SELECT population_top3000_202608_202609_v1.ref01_alias_point_append_v2();
DO $proof$
DECLARE before record; actual text; task_after text;
BEGIN
 SELECT * INTO STRICT before FROM alias_review_before;
 IF (SELECT count(*) FROM population_top3000_202608_202609_v1.ref01_retained_alias_point_v1)<>99
 OR (SELECT count(DISTINCT company_id) FROM population_top3000_202608_202609_v1.ref01_retained_alias_point_v1)<>5
 OR EXISTS(SELECT * FROM alias_review_first EXCEPT SELECT * FROM population_top3000_202608_202609_v1.ref01_retained_alias_point_v1)
 OR EXISTS(SELECT * FROM population_top3000_202608_202609_v1.ref01_retained_alias_point_v1 EXCEPT SELECT * FROM alias_review_first)
 OR (SELECT count(*) FROM alias_review_first_commit)<>1
 OR (SELECT sum(new_committed_units) FROM alias_review_first_commit)<>99
 OR EXISTS(SELECT * FROM alias_review_first_commit EXCEPT SELECT * FROM population_top3000_202608_202609_v1.pipeline_commit)
 THEN RAISE EXCEPTION 'REVIEW_EXACT99_OR_REPLAY_FAILED';END IF;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_commit c JOIN population_top3000_202608_202609_v1.pipeline_attempt a USING(attempt_id) JOIN population_top3000_202608_202609_v1.pipeline_task t ON t.task_id=c.task_id WHERE c.commit_key='retained-alias-point-v1:aee4f53ab389fe6fe0ca0fedc7664c838b83a6d8f192f69f8f0fe0a32e6eb2d7' AND c.new_committed_units=99 AND a.task_id=t.task_id AND t.attempt_id=a.attempt_id AND t.status='COMPLETE' AND a.outcome='COMPLETE' AND t.lease_until IS NULL AND a.request_id IS NULL AND a.execution_number=1 AND a.adapter_sha256=encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.ref01_alias_point_append_v2()'::regprocedure),'UTF8')),'hex') AND c.receipt->>'executor_kind'='REVIEWED_PRIVATE_FUNCTION_ONE_SHOT' AND c.receipt->>'adapter_registry_used'='false') THEN RAISE EXCEPTION 'REVIEW_GENUINE_ATTEMPT_BINDING_FAILED';END IF;
 SELECT encode(sha256(convert_to(jsonb_agg(to_jsonb(r) ORDER BY r.source_id,r.ordinal)::text,'UTF8')),'hex') INTO actual FROM population_top3000_202608_202609_v1.ref01_date_record_v4 r WHERE (r.source_id,r.ordinal) IN(SELECT p->>'source_id',(p->>'ordinal')::int FROM population_top3000_202608_202609_v1.pipeline_event e CROSS JOIN LATERAL jsonb_array_elements(e.details->'input_manifest') x CROSS JOIN LATERAL jsonb_array_elements(x->'candidate_pins')p WHERE event_id=40974);
 SELECT encode(sha256(convert_to(string_agg(encode(sha256(convert_to(to_jsonb(t)::text,'UTF8')),'hex'),E'\n' ORDER BY task_id),'UTF8')),'hex') INTO task_after FROM population_top3000_202608_202609_v1.pipeline_task t WHERE partition_key<>'retained-alias-point-v1:aee4f53ab389fe6fe0ca0fedc7664c838b83a6d8f192f69f8f0fe0a32e6eb2d7';
 IF task_after IS DISTINCT FROM before.task_hash OR actual IS DISTINCT FROM before.legacy_hash OR (SELECT to_jsonb(a) FROM population_top3000_202608_202609_v1.acquisition_control a) IS DISTINCT FROM before.acquisition OR (SELECT to_jsonb(p) FROM population_top3000_202608_202609_v1.pipeline_control p) IS DISTINCT FROM before.pipeline OR (SELECT jsonb_array_length(content->'companies') FROM population_top3000_202608_202609_v1.ref01_practical_snapshot_v1 WHERE snapshot_sha256='2dc0f766befed6de6b052221c3c87482d507ba1bad931cbb98654eb3af899bec') IS DISTINCT FROM before.company_count THEN RAISE EXCEPTION 'REVIEW_LEGACY_CONTROLS_DENOMINATOR_CHANGED';END IF;
END $proof$;

DO $negative_owner$
DECLARE rejected boolean:=false;
BEGIN
 BEGIN
 UPDATE population_top3000_202608_202609_v1.acquisition_control SET owner_id='negative-owner' WHERE singleton;
 EXECUTE $append_under_test$SELECT population_top3000_202608_202609_v1.ref01_alias_point_append_v2();$append_under_test$;
 EXCEPTION WHEN OTHERS THEN
  IF position('CONTINUATION_OWNER_STOP_SCOPE_CHANGED' IN SQLERRM)=0 THEN RAISE;END IF;
  rejected:=true;
 END;
 IF NOT rejected THEN RAISE EXCEPTION 'REVIEW_NEGATIVE_OWNER_NOT_REJECTED';END IF;
END $negative_owner$;

DO $negative_stop$
DECLARE rejected boolean:=false;
BEGIN
 BEGIN
 UPDATE population_top3000_202608_202609_v1.acquisition_control SET stopped=true WHERE singleton;
 EXECUTE $append_under_test$SELECT population_top3000_202608_202609_v1.ref01_alias_point_append_v2();$append_under_test$;
 EXCEPTION WHEN OTHERS THEN
  IF position('CONTINUATION_USER_OR_CONTROL_HOLD' IN SQLERRM)=0 THEN RAISE;END IF;
  rejected:=true;
 END;
 IF NOT rejected THEN RAISE EXCEPTION 'REVIEW_NEGATIVE_STOP_NOT_REJECTED';END IF;
END $negative_stop$;

DO $negative_claim$
DECLARE rejected boolean:=false;
BEGIN
 BEGIN
 UPDATE population_top3000_202608_202609_v1.pipeline_event SET details=jsonb_set(details,'{expires_at}',to_jsonb('2000-01-01T00:00:00Z'::text)) WHERE event_id=42511;
 EXECUTE $append_under_test$SELECT population_top3000_202608_202609_v1.ref01_alias_point_append_v2();$append_under_test$;
 EXCEPTION WHEN OTHERS THEN
  IF position('ALIAS_V1_EXACT_ACTIVE_CLAIM' IN SQLERRM)=0 THEN RAISE;END IF;
  rejected:=true;
 END;
 IF NOT rejected THEN RAISE EXCEPTION 'REVIEW_NEGATIVE_CLAIM_NOT_REJECTED';END IF;
END $negative_claim$;

DO $negative_policy$
DECLARE rejected boolean:=false;
BEGIN
 BEGIN
 UPDATE population_top3000_202608_202609_v1.pipeline_event SET details=jsonb_set(details,'{input_versions,policy_sha256}',to_jsonb(repeat('0',64))) WHERE event_id=40974;
 EXECUTE $append_under_test$SELECT population_top3000_202608_202609_v1.ref01_alias_point_append_v2();$append_under_test$;
 EXCEPTION WHEN OTHERS THEN
  IF position('ALIAS_V1_EXACT_ACTIVE_CLAIM' IN SQLERRM)=0 THEN RAISE;END IF;
  rejected:=true;
 END;
 IF NOT rejected THEN RAISE EXCEPTION 'REVIEW_NEGATIVE_POLICY_NOT_REJECTED';END IF;
END $negative_policy$;

DO $negative_source$
DECLARE rejected boolean:=false;
BEGIN
 BEGIN
 UPDATE population_top3000_202608_202609_v1.source_document SET content_text=content_text||' ',source_sha256=encode(sha256(convert_to(content_text||' ','UTF8')),'hex'),content_bytes=octet_length(content_text||' ') WHERE source_id=(SELECT details#>>'{input_manifest,0,candidate_pins,0,source_id}' FROM population_top3000_202608_202609_v1.pipeline_event WHERE event_id=40974);
 EXECUTE $append_under_test$SELECT population_top3000_202608_202609_v1.ref01_alias_point_append_v2();$append_under_test$;
 EXCEPTION WHEN OTHERS THEN
  IF position('ALIAS_V1_SOURCE_OR_ANCHOR_CHANGED' IN SQLERRM)=0 THEN RAISE;END IF;
  rejected:=true;
 END;
 IF NOT rejected THEN RAISE EXCEPTION 'REVIEW_NEGATIVE_SOURCE_NOT_REJECTED';END IF;
END $negative_source$;

DO $negative_normalized$
DECLARE rejected boolean:=false;
BEGIN
 BEGIN
 UPDATE population_top3000_202608_202609_v1.ref01_date_record_v4 SET record_sha256=repeat('0',64) WHERE (source_id,ordinal)=(SELECT details#>>'{input_manifest,0,candidate_pins,0,source_id}',(details#>>'{input_manifest,0,candidate_pins,0,ordinal}')::int FROM population_top3000_202608_202609_v1.pipeline_event WHERE event_id=40974);
 EXECUTE $append_under_test$SELECT population_top3000_202608_202609_v1.ref01_alias_point_append_v2();$append_under_test$;
 EXCEPTION WHEN OTHERS THEN
  IF position('ALIAS_V1_SOURCE_OR_ANCHOR_CHANGED' IN SQLERRM)=0 THEN RAISE;END IF;
  rejected:=true;
 END;
 IF NOT rejected THEN RAISE EXCEPTION 'REVIEW_NEGATIVE_NORMALIZED_NOT_REJECTED';END IF;
END $negative_normalized$;

DO $negative_selfcode$
DECLARE rejected boolean:=false;
BEGIN
 BEGIN
 EXECUTE replace(pg_get_functiondef('population_top3000_202608_202609_v1.ref01_alias_point_append_v2()'::regprocedure),E'$function$\n',E'$function$\n-- selfcode rejection\n');
 EXECUTE $append_under_test$SELECT population_top3000_202608_202609_v1.ref01_alias_point_append_v2();$append_under_test$;
 EXCEPTION WHEN OTHERS THEN
  IF position('ALIAS_V2_SELF_CODE_REVIEW_REQUIRED' IN SQLERRM)=0 THEN RAISE;END IF;
  rejected:=true;
 END;
 IF NOT rejected THEN RAISE EXCEPTION 'REVIEW_NEGATIVE_SELFCODE_NOT_REJECTED';END IF;
END $negative_selfcode$;

DO $negative_code$
DECLARE rejected boolean:=false;
BEGIN
 BEGIN
 EXECUTE replace(pg_get_functiondef('population_top3000_202608_202609_v1.ctx06_release_shape_v3(jsonb,jsonb)'::regprocedure),E'$function$\n',E'$function$\n-- reviewer negative code preimage\n');
 EXECUTE $append_under_test$SELECT population_top3000_202608_202609_v1.ref01_alias_point_append_v2();$append_under_test$;
 EXCEPTION WHEN OTHERS THEN
  IF position('ALIAS_V1_CODE_CHANGED' IN SQLERRM)=0 THEN RAISE;END IF;
  rejected:=true;
 END;
 IF NOT rejected THEN RAISE EXCEPTION 'REVIEW_NEGATIVE_CODE_NOT_REJECTED';END IF;
END $negative_code$;
SELECT jsonb_build_object('result','PASS_INDEPENDENT_EXACT99_ROLLBACK_REPLAY_AND_NEGATIVES','exact_rows',count(*),'companies',count(DISTINCT company_id),'negative_checks',8,'legacy_control_denominator_checks',true,'attempt_task_commit_coherence',true,'output_sha256',(select output_sha256 from alias_review_first_commit),'function_sha256',encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.ref01_alias_point_append_v2()'::regprocedure),'UTF8')),'hex'),'code_manifest',population_top3000_202608_202609_v1.population_recovery_manifest_v1(),'family_accepted',false) FROM population_top3000_202608_202609_v1.ref01_retained_alias_point_v1;
ROLLBACK;
