-- INCOMPLETE / NOT APPROVED FOR PRODUCTION.
-- Actual rollback derives99 source-linked point rows, but pipeline_commit requires a real pipeline_attempt.
-- Two repair iterations exhausted; exact successor must implement/review/test attempt binding before integration.
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
-- One exact retained-only append. No persistent function or adapter changes.
DO $append$
DECLARE ctl jsonb;cl jsonb;ass jsonb;x jsonb;pin jsonb;rr record;aa record;sd record;pg record;
 c jsonb;out_hash text;tid uuid;cid uuid;newrows integer:=0;sz bigint;mission_before bigint;mission_after bigint;count_strong integer;strong_counts jsonb;
 manifest constant text:='aee4f53ab389fe6fe0ca0fedc7664c838b83a6d8f192f69f8f0fe0a32e6eb2d7';
BEGIN
ctl:=population_top3000_202608_202609_v1.continuation_guard_v6('top3000-completion-20261024-root-5db48b32d103',3,1);
SELECT details INTO STRICT cl FROM population_top3000_202608_202609_v1.pipeline_event WHERE event_id=40855 AND event='ENGINEERING_ASSIGNMENT_CLAIMED_V5';
SELECT details INTO STRICT ass FROM population_top3000_202608_202609_v1.pipeline_event WHERE event_id=39256 AND event='ENGINEERING_IMPLEMENTATION_ASSIGNMENT_V5';
IF cl->>'assignment_details_sha256' IS DISTINCT FROM '0f0b2a25d63df8b4012fa4f4b5c31ea99613405746516a05598991d5dfaab014' OR cl->>'assignment_event_id' IS DISTINCT FROM '39256' OR cl->>'executor' IS DISTINCT FROM 'automation:6adc2c96db348191bca7cf4eacfdac32' OR cl->>'owner_id' IS DISTINCT FROM ctl->>'owner_id' OR cl->>'owner_fence' IS DISTINCT FROM '3' OR cl->>'stop_generation' IS DISTINCT FROM '1' OR cl->>'scope_sha256' IS DISTINCT FROM ctl->>'scope_sha256' OR (cl->>'expires_at')::timestamptz<=clock_timestamp() OR (ass->>'not_after')::timestamptz<=clock_timestamp() OR encode(sha256(convert_to(ass::text,'UTF8')),'hex') IS DISTINCT FROM '0f0b2a25d63df8b4012fa4f4b5c31ea99613405746516a05598991d5dfaab014' OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_event WHERE event IN('ENGINEERING_ASSIGNMENT_COMPLETED_V5','ENGINEERING_ASSIGNMENT_EFFECTS_RECONCILED_RELEASE_V5') AND details->>'claim_event_id'='40855') THEN RAISE EXCEPTION 'ALIAS_V1_EXACT_ACTIVE_CLAIM';END IF;
IF jsonb_array_length(ass->'input_manifest')<>99 OR encode(sha256(convert_to((ass->'input_manifest')::text,'UTF8')),'hex') IS DISTINCT FROM manifest THEN RAISE EXCEPTION 'ALIAS_V1_MANIFEST_CHANGED';END IF;
IF EXISTS(SELECT 1 FROM jsonb_each_text(ass->'fresh_code_preimages') z LEFT JOIN pg_proc p ON p.proname=z.key AND p.pronamespace='population_top3000_202608_202609_v1'::regnamespace WHERE p.oid IS NULL OR encode(sha256(convert_to(pg_get_functiondef(p.oid),'UTF8')),'hex') IS DISTINCT FROM z.value) THEN RAISE EXCEPTION 'ALIAS_V1_CODE_CHANGED';END IF;
IF (SELECT jsonb_array_length(content->'companies') FROM population_top3000_202608_202609_v1.ref01_practical_snapshot_v1 WHERE snapshot_sha256=ass#>>'{input_versions,cohort_sha256}') IS DISTINCT FROM 3000 THEN RAISE EXCEPTION 'ALIAS_V1_DENOMINATOR';END IF;
SELECT sum(pg_total_relation_size(oid)) INTO mission_before FROM pg_class WHERE relnamespace='population_top3000_202608_202609_v1'::regnamespace AND relkind IN('r','m');
IF mission_before>12000000000 THEN RAISE EXCEPTION 'ALIAS_V1_GLOBAL_RELATION_BOUND';END IF;
sz:=pg_total_relation_size('population_top3000_202608_202609_v1.ref01_retained_alias_point_v1');
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
 tid:=population_top3000_202608_202609_v1.pipeline_enqueue_v1('REF01','DERIVE','retained-alias-point-v1:'||manifest,jsonb_build_object('kind','REF01_RETAINED_ALIAS_POINT_V1','assignment_event_id',39256),ass->'input_versions');
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=tid AND (status<>'READY' OR executions<>0)) THEN RAISE EXCEPTION 'ALIAS_V1_TASK_PREEXISTS';END IF;
 INSERT INTO population_top3000_202608_202609_v1.pipeline_commit(task_id,commit_key,output_sha256,receipt,new_committed_units) VALUES(tid,'retained-alias-point-v1:'||manifest,out_hash,jsonb_build_object('assignment_event_id',39256,'claim_event_id',40855,'rows',99,'evidence_semantics','POINT_ONLY_NOT_INTERVAL','family_accepted',false,'source_calls',0),99) RETURNING commit_id INTO cid;
 UPDATE population_top3000_202608_202609_v1.pipeline_task SET status='COMPLETE',owner_fence=3,stop_generation=1,executions=1,useful_committed_units=99,last_useful_progress_at=clock_timestamp(),started_at=clock_timestamp(),completed_at=clock_timestamp(),checkpoint=jsonb_build_object('output_sha256',out_hash,'rows',99,'quality_held',true) WHERE task_id=tid;
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES('REF01_RETAINED_ALIAS_POINT_APPENDED_V1',ctl||jsonb_build_object('assignment_event_id',39256,'claim_event_id',40855,'task_id',tid,'commit_id',cid,'output_sha256',out_hash,'manifest_sha256',manifest,'new_committed_units',99,'intervals_qualified',false,'family_accepted',false));
ELSIF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_commit WHERE commit_key='retained-alias-point-v1:'||manifest AND output_sha256=out_hash AND new_committed_units=99) THEN RAISE EXCEPTION 'ALIAS_V1_REPLAY_RECEIPT_REQUIRED';END IF;
SELECT sum(pg_total_relation_size(oid)) INTO mission_after FROM pg_class WHERE relnamespace='population_top3000_202608_202609_v1'::regnamespace AND relkind IN('r','m');
IF mission_after>12000000000 OR mission_after-mission_before>33554432 THEN RAISE EXCEPTION 'ALIAS_V1_CUMULATIVE_RELATION_BOUND';END IF;
END $append$;
