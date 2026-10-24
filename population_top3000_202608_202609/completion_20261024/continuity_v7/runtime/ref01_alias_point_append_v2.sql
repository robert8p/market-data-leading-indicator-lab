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
