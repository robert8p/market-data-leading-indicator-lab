-- ROOT ONLY: actual PostgreSQL fixture, no Edge dispatch or HTTP call, every change rolled back.
-- Requires cron596 paused and no task/provider lease in flight. Existing source data are preserved.
BEGIN;
SET LOCAL lock_timeout='3s';SET LOCAL statement_timeout='55s';SET LOCAL timezone='UTC';
SELECT pg_advisory_xact_lock(2026083000261022);
CREATE TEMP TABLE massive_minute_fixture_timing(stage text,batch_number integer,elapsed_seconds numeric,source_rows integer,manifest_sha256 text) ON COMMIT DROP;
CREATE OR REPLACE FUNCTION pg_temp.massive_minute_fixture_lease(p_task uuid)
RETURNS jsonb LANGUAGE plpgsql SET search_path TO 'pg_catalog' AS $f$
DECLARE a uuid:=gen_random_uuid();tok text:=encode(sha256(convert_to(gen_random_uuid()::text,'UTF8')),'hex');
BEGIN
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=p_task AND family_id='P01' AND payload->>'kind'='MASSIVE_MINUTE_PILOT_V1' AND payload->>'fixture_only'='true' AND status IN('READY','BLOCKED')) THEN RAISE EXCEPTION 'FIXTURE_LEASE_SCOPE';END IF;
 UPDATE population_top3000_202608_202609_v1.pipeline_task SET status='LEASED',attempt_id=a,lease_until=clock_timestamp()+interval '60 seconds',owner_fence=2,stop_generation=0,executions=executions+1 WHERE task_id=p_task;
 INSERT INTO population_top3000_202608_202609_v1.pipeline_attempt(attempt_id,task_id,execution_number,token_sha256,owner_fence,stop_generation,adapter_sha256)
 SELECT a,p_task,t.executions,encode(sha256(convert_to(tok,'UTF8')),'hex'),2,0,ad.function_sha256 FROM population_top3000_202608_202609_v1.pipeline_task t JOIN population_top3000_202608_202609_v1.pipeline_adapter ad ON ad.family_id=t.family_id AND ad.stage=t.stage WHERE t.task_id=p_task;
 RETURN jsonb_build_object('attempt_id',a,'token',tok);
END $f$;
CREATE OR REPLACE FUNCTION pg_temp.massive_minute_fixture_commit(p_attempt uuid,p_token text,p_claim jsonb,p_body text)
RETURNS jsonb LANGUAGE plpgsql SET search_path TO 'pg_catalog' AS $f$
DECLARE b jsonb:=p_body::jsonb;h text:=encode(sha256(convert_to(p_body,'UTF8')),'hex');
BEGIN
 RETURN population_top3000_202608_202609_v1.massive_minute_commit_v1(p_attempt,p_token,jsonb_build_object('archive_key',p_claim->>'archive_key','source_symbol','AAPL','trading_date','2026-09-08','window_start_ms',1788840000000::bigint,'window_end_exclusive_ms',1788926400000::bigint,'page_number',(p_claim->>'page_number')::integer,'request_url',p_claim->>'request_url','content_text',p_body,'source_sha256',h,'wire_sha256',h,'wire_bytes',octet_length(p_body),'http_status',200,'retrieved_at',clock_timestamp(),'result_count',jsonb_array_length(population_top3000_202608_202609_v1.massive_minute_results_v1(b)),'results_presence',CASE WHEN b ? 'results' THEN 'ARRAY' ELSE 'OMITTED_WITH_EXPLICIT_ZERO' END,'next_url',b->>'next_url','provider_request_id',b->>'request_id','elapsed_ms',1));
END $f$;
DO $test$
DECLARE tid uuid;a uuid;tok text;l jsonb;c jsonb;r jsonb;replay jsonb;vers jsonb;branchvers jsonb;payload jsonb;body text;records jsonb;first_row jsonb;next_url text;archive text;firstpage bigint;lastpage bigint;i integer;rejected boolean;ad record;new_fn regprocedure;batch_start timestamptz;
 old_requests bigint;old_storage bigint;body_bytes bigint:=0;old_p02 jsonb;old_binding jsonb;old_universe jsonb;old_legacy_functions jsonb;old_alpaca_hash text;old_retained_hash text;old_manifests text;current_manifests text;legacy_fn regprocedure;direct_result jsonb;legacy_task population_top3000_202608_202609_v1.pipeline_task%rowtype;
BEGIN
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton AND owner_id='top3000-recovery-20261023-root-4b1314bcb5c2' AND owner_fence=2 AND stop_generation=0 AND enabled AND NOT stopped AND hold_reason IS NULL AND admitted_until>clock_timestamp()) THEN RAISE EXCEPTION 'OWNER_STOP_OR_ADMISSION_CHANGED';END IF;
 IF EXISTS(SELECT 1 FROM cron.job WHERE jobid=596 AND active) THEN RAISE EXCEPTION 'CRON_596_MUST_BE_INACTIVE_DURING_MINUTE_TEST';END IF;
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE status='LEASED') OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_provider_lease WHERE released_at IS NULL) THEN RAISE EXCEPTION 'EXISTING_LEASE_MUST_FINISH_BEFORE_TEST';END IF;
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.massive_minute_page_v1) THEN RAISE EXCEPTION 'MINUTE_LANE_ALREADY_STARTED_RECONCILE_BEFORE_FIXTURE';END IF;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton AND shared_scheduler_verified AND budget_evidence->>'provider_admissions_allowed'='true' AND budget_evidence->'allowed_providers' ? 'Massive') THEN RAISE EXCEPTION 'EXISTING_MASSIVE_ADMISSION_REQUIRED';END IF;
 SELECT provider_requests,stored_bytes INTO old_requests,old_storage FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton;
 SELECT jsonb_object_agg(stage,encode(sha256(convert_to(pg_get_functiondef(function_oid),'UTF8')),'hex')) INTO old_legacy_functions FROM population_top3000_202608_202609_v1.pipeline_adapter WHERE family_id='P01' AND stage IN('ACQUIRE','NORMALIZE','DERIVE','VALIDATE');
 IF (SELECT count(*) FROM jsonb_object_keys(old_legacy_functions))<>4 THEN RAISE EXCEPTION 'ALL_FOUR_CURRENT_P01_STAGE_ADAPTERS_REQUIRED';END IF;
 SELECT jsonb_build_object('adapters',(SELECT jsonb_agg(to_jsonb(x) ORDER BY stage) FROM population_top3000_202608_202609_v1.pipeline_adapter x WHERE family_id='P02'),'binding',(SELECT to_jsonb(x) FROM population_top3000_202608_202609_v1.pipeline_family_binding x WHERE family_id='P02'),'tasks',(SELECT jsonb_agg(to_jsonb(x) ORDER BY task_id) FROM population_top3000_202608_202609_v1.pipeline_task x WHERE family_id='P02')) INTO old_p02;
 SELECT to_jsonb(x) INTO old_binding FROM population_top3000_202608_202609_v1.pipeline_family_binding x WHERE family_id='P01';SELECT coalesce(jsonb_agg(to_jsonb(x)),'[]'::jsonb) INTO old_universe FROM population_top3000_202608_202609_v1.pipeline_universe x;
 old_alpaca_hash:=population_top3000_202608_202609_v1.alpaca_sip_code_hash_v1();old_retained_hash:=encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.price_retained_partition_v1(uuid,text)'::regprocedure),'UTF8')),'hex');
 SELECT encode(sha256(convert_to(coalesce(string_agg(to_jsonb(x)::text,E'\n' ORDER BY manifest_id),''),'UTF8')),'hex') INTO old_manifests FROM population_top3000_202608_202609_v1.retained_price_partition_v1 x;
 UPDATE population_top3000_202608_202609_v1.pipeline_control SET enabled=true WHERE singleton;
 FOR ad IN SELECT * FROM population_top3000_202608_202609_v1.pipeline_adapter WHERE family_id='P01' AND stage IN('ACQUIRE','NORMALIZE','DERIVE','VALIDATE') LOOP
  IF NOT ad.enabled OR ad.function_sha256 IS DISTINCT FROM encode(sha256(convert_to(pg_get_functiondef(ad.function_oid),'UTF8')),'hex') THEN RAISE EXCEPTION 'CURRENT_P01_ADAPTER_NOT_VERIFIED';END IF;
  INSERT INTO population_top3000_202608_202609_v1.massive_minute_legacy_adapter_v1(stage,function_oid,function_sha256,registration_evidence) VALUES(ad.stage,ad.function_oid,ad.function_sha256,jsonb_build_object('fixture_only',true,'previous_registration',ad.reviewed_test_evidence)) ON CONFLICT(stage) DO NOTHING;
  IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.massive_minute_legacy_adapter_v1 WHERE stage=ad.stage AND function_oid=ad.function_oid AND function_sha256=ad.function_sha256) THEN RAISE EXCEPTION 'LEGACY_REGISTRY_CONFLICT';END IF;
  new_fn:=('population_top3000_202608_202609_v1.massive_minute_route_'||lower(ad.stage)||'_v1(uuid,text)')::regprocedure;
  UPDATE population_top3000_202608_202609_v1.pipeline_adapter SET function_oid=new_fn::oid,function_sha256=encode(sha256(convert_to(pg_get_functiondef(new_fn),'UTF8')),'hex'),resource_class=CASE WHEN ad.stage='ACQUIRE' THEN 'PROVIDER' ELSE 'DATABASE' END,provider=NULL,enabled=true,reviewed_test_evidence='{"test":"MASSIVE_MINUTE_PILOT_ROLLBACK","status":"IN_PROGRESS_ROLLBACK_ONLY"}',producer_revision='massive-minute-aapl-20260908-v1',expected_batch_seconds=45 WHERE family_id='P01' AND stage=ad.stage;
 END LOOP;
 vers:=jsonb_build_object('scope_sha256','8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd','source_contract_revision','massive-minute-aapl-20260908-v1','massive_minute_bundle_sha256',population_top3000_202608_202609_v1.massive_minute_bundle_sha256_v1(),'fixture_only',true);
 payload:='{"kind":"MASSIVE_MINUTE_PILOT_V1","source_symbol":"AAPL","trading_date":"2026-09-08","max_pages":1,"fixture_only":true}'::jsonb;
 -- Explicit omitted resultsCount=0 is zero source occurrences, never a zero-volume bar.
 rejected:=false;
 BEGIN
  branchvers:=vers||'{"fixture_variant":"EMPTY_SOURCE"}';tid:=population_top3000_202608_202609_v1.pipeline_enqueue_v1('P01','ACQUIRE','massive-minute-AAPL-20260908-v1',payload,branchvers);
  l:=pg_temp.massive_minute_fixture_lease(tid);a:=(l->>'attempt_id')::uuid;tok:=l->>'token';UPDATE population_top3000_202608_202609_v1.acquisition_control SET next_provider_at=clock_timestamp() WHERE singleton;c:=population_top3000_202608_202609_v1.massive_minute_claim_v1(a,tok);
  IF c->>'state' IS DISTINCT FROM 'RESERVED' THEN RAISE EXCEPTION 'MINUTE_EMPTY_FIXTURE_RESERVE_FAILED %',c;END IF;
  r:=pg_temp.massive_minute_fixture_commit(a,tok,c,'{"status":"OK","ticker":"AAPL","adjusted":false,"resultsCount":0}');IF r->>'state' IS DISTINCT FROM 'COMPLETE' THEN RAISE EXCEPTION 'MINUTE_EMPTY_SOURCE_NOT_RETAINED';END IF;
  FOR i IN 1..2 LOOP
   SELECT task_id INTO tid FROM population_top3000_202608_202609_v1.pipeline_task WHERE family_id='P01' AND partition_key='massive-minute-AAPL-20260908-v1' AND input_versions=branchvers AND stage=(CASE WHEN i=1 THEN 'NORMALIZE' ELSE 'DERIVE' END);
   l:=pg_temp.massive_minute_fixture_lease(tid);a:=(l->>'attempt_id')::uuid;tok:=l->>'token';r:=population_top3000_202608_202609_v1.massive_minute_route_v1(a,tok,CASE WHEN i=1 THEN 'NORMALIZE' ELSE 'DERIVE' END);
   IF r->>'state' IS DISTINCT FROM 'COMPLETE' OR r#>>'{commit_receipt,new_committed_units}' IS DISTINCT FROM '0' THEN RAISE EXCEPTION 'MINUTE_EMPTY_SOURCE_BECAME_BAR';END IF;PERFORM population_top3000_202608_202609_v1.pipeline_finish_v1(a,tok,r);
  END LOOP;
  IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.massive_minute_bar_v1) OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.massive_minute_page_v1 WHERE results_presence='OMITTED_WITH_EXPLICIT_ZERO' AND source_summary->>'raw_occurrences'='0' AND source_summary->>'source_records_reconciled'='true') THEN RAISE EXCEPTION 'MINUTE_EMPTY_SEMANTICS_FAILED';END IF;
  RAISE EXCEPTION 'ROLLBACK_EXPECTED_EMPTY_SOURCE_BRANCH';
 EXCEPTION WHEN OTHERS THEN IF SQLERRM='ROLLBACK_EXPECTED_EMPTY_SOURCE_BRANCH' THEN rejected:=true;ELSE RAISE;END IF;END;
 IF NOT rejected THEN RAISE EXCEPTION 'MINUTE_EMPTY_BRANCH_NOT_EXERCISED';END IF;
 -- A partial/oversized HTTP200 is uncertain even when the parser classified the failure.
 rejected:=false;
 BEGIN
  branchvers:=vers||'{"fixture_variant":"UNKNOWN_EFFECT"}';tid:=population_top3000_202608_202609_v1.pipeline_enqueue_v1('P01','ACQUIRE','massive-minute-AAPL-20260908-v1',payload,branchvers);
  l:=pg_temp.massive_minute_fixture_lease(tid);a:=(l->>'attempt_id')::uuid;tok:=l->>'token';UPDATE population_top3000_202608_202609_v1.acquisition_control SET next_provider_at=clock_timestamp() WHERE singleton;c:=population_top3000_202608_202609_v1.massive_minute_claim_v1(a,tok);
  IF c->>'state' IS DISTINCT FROM 'RESERVED' THEN RAISE EXCEPTION 'MINUTE_UNKNOWN_FIXTURE_RESERVE_FAILED %',c;END IF;
  r:=population_top3000_202608_202609_v1.massive_minute_failure_v1(a,tok,'{"code":"RESPONSE_TOO_LARGE","failure_class":"PARSER","http_status":200,"fetch_started":true,"received_bytes":8388608,"wire_sha256":null,"retry_after_seconds":0}');
  IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_provider_lease WHERE attempt_id=a AND released_at IS NULL) OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=tid AND status='BLOCKED' AND last_failure_class='UNCERTAIN_EFFECT') THEN RAISE EXCEPTION 'MINUTE_UNKNOWN_EFFECT_LEASE_NOT_RETAINED';END IF;
  RAISE EXCEPTION 'ROLLBACK_EXPECTED_UNKNOWN_EFFECT_BRANCH';
 EXCEPTION WHEN OTHERS THEN IF SQLERRM='ROLLBACK_EXPECTED_UNKNOWN_EFFECT_BRANCH' THEN rejected:=true;ELSE RAISE;END IF;END;
 IF NOT rejected THEN RAISE EXCEPTION 'MINUTE_UNKNOWN_BRANCH_NOT_EXERCISED';END IF;
 -- First page has 1,003 raw occurrences, exercising a real 1,000+3 checkpoint split.
 tid:=population_top3000_202608_202609_v1.pipeline_enqueue_v1('P01','ACQUIRE','massive-minute-AAPL-20260908-v1',payload,vers);l:=pg_temp.massive_minute_fixture_lease(tid);a:=(l->>'attempt_id')::uuid;tok:=l->>'token';
 UPDATE population_top3000_202608_202609_v1.acquisition_control SET next_provider_at=clock_timestamp() WHERE singleton;c:=population_top3000_202608_202609_v1.massive_minute_claim_v1(a,tok);
 IF c->>'state' IS DISTINCT FROM 'RESERVED' OR c->>'page_number' IS DISTINCT FROM '1' THEN RAISE EXCEPTION 'MINUTE_FIXED_DAY_CLAIM_FAILED %',c;END IF;archive:=c->>'archive_key';
 rejected:=false;BEGIN PERFORM population_top3000_202608_202609_v1.massive_minute_claim_v1(a,tok);EXCEPTION WHEN OTHERS THEN IF SQLERRM='MASSIVE_MINUTE_PRIOR_REQUEST_REQUIRES_RECONCILIATION' THEN rejected:=true;ELSE RAISE;END IF;END;
 IF NOT rejected OR (SELECT provider_requests-old_requests FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton)<>1 THEN RAISE EXCEPTION 'MINUTE_DUPLICATE_PROVIDER_CLAIM';END IF;
 SELECT jsonb_agg(jsonb_build_object('t',1788840000000::bigint+(n-1)*60000::bigint,'o',10,'h',12,'l',9,'c',11,'v',100,'n',5,'vw',10.5,'otc',false) ORDER BY n) INTO records FROM generate_series(1,1003) g(n);
 records:=jsonb_set(records,'{0}',(records->0)||'{"v":0,"n":0}');records:=jsonb_set(records,'{1}',(records->1)-'v'-'n'-'vw'-'otc');
 records:=jsonb_set(records,'{2}',(records->2)||jsonb_build_object('o',0.00000000000000000001,'h',0.00000000000000000002,'l',0.00000000000000000001,'c',0.00000000000000000001,'v',9007199254740993::numeric,'vw',0.00000000000000000001));
 records:=jsonb_set(records,'{3}',(records->3)||'{"h":1,"l":20}');records:=jsonb_set(records,'{4}',(records->4)||'{"v":-1,"n":1.5}');first_row:=records->0;
 next_url:='https://api.massive.com/v2/aggs/ticker/AAPL/range/1/minute/1788840000000/1788926399999?cursor=fixture-page-2%3D';
 PERFORM population_top3000_202608_202609_v1.massive_minute_url_v1(next_url,false);
 body:=jsonb_build_object('status','OK','ticker','AAPL','adjusted',false,'queryCount',1003.0::numeric,'resultsCount',1003.0::numeric,'results',records,'next_url',next_url,'request_id','FIXTURE-MINUTE-1')::text;body_bytes:=octet_length(body);
 r:=pg_temp.massive_minute_fixture_commit(a,tok,c,body);IF r->>'state' IS DISTINCT FROM 'CONTINUE' THEN RAISE EXCEPTION 'MINUTE_FIRST_CURSOR_NOT_CHECKPOINTED %',r;END IF;
 SELECT page_id INTO firstpage FROM population_top3000_202608_202609_v1.massive_minute_page_v1 WHERE archive_key=archive AND page_number=1;
 -- A real one-page cap blocks before any second reserve or Edge dispatch; it never truncates.
 l:=pg_temp.massive_minute_fixture_lease(tid);a:=(l->>'attempt_id')::uuid;tok:=l->>'token';r:=population_top3000_202608_202609_v1.massive_minute_route_acquire_v1(a,tok);
 IF r->>'state' IS DISTINCT FROM 'BLOCKED' OR r->>'blocker_code' IS DISTINCT FROM 'MASSIVE_MINUTE_PAGE_CAP_REVIEW_REQUIRED' OR (SELECT provider_requests-old_requests FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton)<>1 THEN RAISE EXCEPTION 'MINUTE_FIRST_PAGE_CAP_FAILED';END IF;PERFORM population_top3000_202608_202609_v1.pipeline_finish_v1(a,tok,r);
 UPDATE population_top3000_202608_202609_v1.pipeline_task t SET payload=jsonb_set(t.payload,'{max_pages}','2'),not_before=clock_timestamp(),blocker_until=NULL WHERE t.task_id=tid;
 l:=pg_temp.massive_minute_fixture_lease(tid);a:=(l->>'attempt_id')::uuid;tok:=l->>'token';UPDATE population_top3000_202608_202609_v1.acquisition_control SET next_provider_at=clock_timestamp() WHERE singleton;c:=population_top3000_202608_202609_v1.massive_minute_claim_v1(a,tok);
 IF c->>'state' IS DISTINCT FROM 'RESERVED' OR c->>'request_url' IS DISTINCT FROM next_url OR c->>'page_number' IS DISTINCT FROM '2' THEN RAISE EXCEPTION 'MINUTE_SECOND_PAGE_CURSOR_CHANGED %',c;END IF;
 records:=jsonb_build_array(first_row,first_row||'{"c":11.5}',jsonb_build_object('t',1788926400000::bigint,'o',10,'h',12,'l',9,'c',11,'v',5),jsonb_build_object('t',1788839999999::bigint,'o',10,'h',12,'l',9,'c',11,'v',5),jsonb_build_object('t',1788840000001::bigint,'o',10,'h',12,'l',9,'c',11,'v',5),jsonb_build_object('t',1788926340000::bigint,'o',10,'h',12,'l',9,'c',11,'v',5),'{}'::jsonb);
 body:=jsonb_build_object('status','OK','ticker','AAPL','adjusted',false,'resultsCount',7,'results',records,'request_id','FIXTURE-MINUTE-2')::text;body_bytes:=body_bytes+octet_length(body);
 r:=pg_temp.massive_minute_fixture_commit(a,tok,c,body);IF r->>'state' IS DISTINCT FROM 'COMPLETE' THEN RAISE EXCEPTION 'MINUTE_TERMINAL_SOURCE_NOT_COMPLETE %',r;END IF;
 SELECT page_id INTO lastpage FROM population_top3000_202608_202609_v1.massive_minute_page_v1 WHERE archive_key=archive AND page_number=2;
 IF (SELECT provider_requests-old_requests FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton)<>2 OR (SELECT stored_bytes-old_storage FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton)<>body_bytes THEN RAISE EXCEPTION 'MINUTE_EXACT_TWO_REQUEST_STORAGE_ACCOUNTING_FAILED';END IF;
 FOR i IN 1..3 LOOP
  SELECT task_id INTO tid FROM population_top3000_202608_202609_v1.pipeline_task WHERE family_id='P01' AND stage='NORMALIZE' AND partition_key='massive-minute-AAPL-20260908-v1' AND input_versions=vers;
  l:=pg_temp.massive_minute_fixture_lease(tid);a:=(l->>'attempt_id')::uuid;tok:=l->>'token';batch_start:=clock_timestamp();r:=population_top3000_202608_202609_v1.massive_minute_route_normalize_v1(a,tok);
  IF r->>'state' IS DISTINCT FROM (CASE WHEN i=3 THEN 'COMPLETE' ELSE 'CONTINUE' END) OR (r#>>'{commit_receipt,new_committed_units}')::integer<>(CASE i WHEN 1 THEN 1000 WHEN 2 THEN 3 ELSE 7 END) THEN RAISE EXCEPTION 'MINUTE_BOUNDED_NORMALIZE_FAILED i=% r=%',i,r;END IF;
  replay:=population_top3000_202608_202609_v1.massive_minute_route_normalize_v1(a,tok);IF replay#>>'{commit_receipt,new_committed_units}' IS DISTINCT FROM '0' OR replay#>>'{commit_receipt,output_sha256}' IS DISTINCT FROM r#>>'{commit_receipt,output_sha256}' THEN RAISE EXCEPTION 'MINUTE_EXACT_REPLAY_FAILED';END IF;
  IF i=1 THEN
   rejected:=false;BEGIN UPDATE population_top3000_202608_202609_v1.massive_minute_bar_v1 SET provider_close=999 WHERE page_id=firstpage AND source_row=1;PERFORM population_top3000_202608_202609_v1.massive_minute_route_normalize_v1(a,tok);EXCEPTION WHEN OTHERS THEN IF SQLERRM='MASSIVE_MINUTE_TYPED_BATCH_REPLAY_CONFLICT' THEN rejected:=true;ELSE RAISE;END IF;END;IF NOT rejected THEN RAISE EXCEPTION 'MINUTE_FORGED_TYPED_VALUE_ACCEPTED';END IF;
   rejected:=false;BEGIN UPDATE population_top3000_202608_202609_v1.source_document SET retrieved_at=retrieved_at+interval '1 second' WHERE source_id=(SELECT source_id FROM population_top3000_202608_202609_v1.massive_minute_page_v1 WHERE page_id=firstpage);PERFORM population_top3000_202608_202609_v1.massive_minute_route_normalize_v1(a,tok);EXCEPTION WHEN OTHERS THEN IF SQLERRM='MASSIVE_MINUTE_RETAINED_SOURCE_INTEGRITY' THEN rejected:=true;ELSE RAISE;END IF;END;IF NOT rejected THEN RAISE EXCEPTION 'MINUTE_CHANGED_SOURCE_METADATA_ACCEPTED';END IF;
   rejected:=false;BEGIN UPDATE population_top3000_202608_202609_v1.pipeline_control SET budget_evidence=jsonb_set(budget_evidence,'{resource_limits,max_relation_bytes}','1') WHERE singleton;PERFORM population_top3000_202608_202609_v1.massive_minute_route_normalize_v1(a,tok);EXCEPTION WHEN OTHERS THEN IF SQLERRM='MASSIVE_MINUTE_MISSION_RELATION_CAP_EXCEEDED' THEN rejected:=true;ELSE RAISE;END IF;END;IF NOT rejected THEN RAISE EXCEPTION 'MINUTE_CURRENT_RESOURCE_CAP_IGNORED';END IF;
  END IF;
  PERFORM population_top3000_202608_202609_v1.pipeline_finish_v1(a,tok,r);INSERT INTO pg_temp.massive_minute_fixture_timing VALUES('NORMALIZE',i,extract(epoch from clock_timestamp()-batch_start),(r#>>'{commit_receipt,new_committed_units}')::integer,r#>>'{commit_receipt,output_sha256}');
 END LOOP;
 FOR i IN 1..3 LOOP
  SELECT task_id INTO tid FROM population_top3000_202608_202609_v1.pipeline_task WHERE family_id='P01' AND stage='DERIVE' AND partition_key='massive-minute-AAPL-20260908-v1' AND input_versions=vers;
  l:=pg_temp.massive_minute_fixture_lease(tid);a:=(l->>'attempt_id')::uuid;tok:=l->>'token';batch_start:=clock_timestamp();
  IF i=3 THEN
   rejected:=false;
   BEGIN
    UPDATE population_top3000_202608_202609_v1.source_document SET content_text=content_text||E'\n',source_sha256=encode(sha256(convert_to(content_text||E'\n','UTF8')),'hex'),content_bytes=content_bytes+1,evidence=jsonb_set(evidence,'{stored_content_bytes}',to_jsonb(content_bytes+1)) WHERE source_id=(SELECT source_id FROM population_top3000_202608_202609_v1.massive_minute_page_v1 WHERE page_id=firstpage);
    UPDATE population_top3000_202608_202609_v1.massive_minute_page_v1 p SET source_sha256=d.source_sha256,response_bytes=d.content_bytes FROM population_top3000_202608_202609_v1.source_document d WHERE p.page_id=firstpage AND d.source_id=p.source_id;
    PERFORM population_top3000_202608_202609_v1.massive_minute_route_derive_v1(a,tok);
   EXCEPTION WHEN OTHERS THEN IF SQLERRM='MASSIVE_MINUTE_NORMALIZED_SOURCE_MANIFEST_CHANGED' THEN rejected:=true;ELSE RAISE;END IF;END;
   IF NOT rejected THEN RAISE EXCEPTION 'MINUTE_JOINT_SOURCE_MANIFEST_REVISION_ACCEPTED';END IF;
   rejected:=false;BEGIN UPDATE population_top3000_202608_202609_v1.massive_minute_bar_v1 SET provider_close=provider_close+1 WHERE page_id=firstpage AND source_row=1;PERFORM population_top3000_202608_202609_v1.massive_minute_route_derive_v1(a,tok);EXCEPTION WHEN OTHERS THEN IF SQLERRM='MASSIVE_MINUTE_TERMINAL_CURRENT_BATCH_PROOF_MISMATCH' THEN rejected:=true;ELSE RAISE;END IF;END;IF NOT rejected THEN RAISE EXCEPTION 'MINUTE_STALE_PRIOR_PROOF_ACCEPTED';END IF;
  END IF;
  r:=population_top3000_202608_202609_v1.massive_minute_route_derive_v1(a,tok);IF r->>'state' IS DISTINCT FROM (CASE WHEN i=3 THEN 'COMPLETE' ELSE 'CONTINUE' END) OR (r#>>'{commit_receipt,new_committed_units}')::integer<>(CASE i WHEN 1 THEN 1000 WHEN 2 THEN 3 ELSE 7 END) THEN RAISE EXCEPTION 'MINUTE_BOUNDED_DERIVE_FAILED i=% r=%',i,r;END IF;
  PERFORM population_top3000_202608_202609_v1.pipeline_finish_v1(a,tok,r);INSERT INTO pg_temp.massive_minute_fixture_timing VALUES('DERIVE',i,extract(epoch from clock_timestamp()-batch_start),(r#>>'{commit_receipt,new_committed_units}')::integer,r#>>'{commit_receipt,output_sha256}');
 END LOOP;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.massive_minute_bar_v1 WHERE page_id=firstpage AND source_row=1 AND share_volume=0 AND trade_count=0 AND (missing_mask&96)=0 AND (invalid_mask&96)=0)
 OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.massive_minute_bar_v1 WHERE page_id=firstpage AND source_row=2 AND share_volume IS NULL AND (missing_mask&32)=32)
 OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.massive_minute_bar_v1 WHERE page_id=firstpage AND source_row=3 AND provider_close=0.00000000000000000001 AND share_volume=9007199254740993::numeric)
 OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.massive_minute_provisional_v1 WHERE page_number=1 AND source_row=1 AND timestamp_in_requested_window AND bar_start_at='2026-09-08T04:00:00Z')
 OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.massive_minute_provisional_v1 WHERE page_number=2 AND source_row=3 AND NOT timestamp_in_requested_window AND (structure_mask&2)=2)
 OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.massive_minute_provisional_v1 WHERE page_number=2 AND source_row=4 AND NOT timestamp_in_requested_window AND (structure_mask&2)=2)
 OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.massive_minute_provisional_v1 WHERE page_number=2 AND source_row=5 AND NOT timestamp_in_requested_window AND (structure_mask&4)=4)
 OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.massive_minute_provisional_v1 WHERE page_number=2 AND source_row=6 AND timestamp_in_requested_window AND bar_start_at='2026-09-09T03:59:00Z')
 OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.massive_minute_page_v1 WHERE page_id=lastpage AND source_summary->>'raw_occurrences'='1010' AND source_summary->>'conflicting_timestamp_groups'='1' AND source_summary->>'duplicate_identical_occurrences'='1' AND source_summary->>'source_records_reconciled'='true') THEN RAISE EXCEPTION 'MINUTE_BOUNDARIES_PRECISION_NULL_ZERO_OR_DUPLICATE_SEMANTICS_FAILED';END IF;
 SELECT task_id INTO tid FROM population_top3000_202608_202609_v1.pipeline_task WHERE family_id='P01' AND stage='VALIDATE' AND partition_key='massive-minute-AAPL-20260908-v1' AND input_versions=vers;
 l:=pg_temp.massive_minute_fixture_lease(tid);a:=(l->>'attempt_id')::uuid;tok:=l->>'token';r:=population_top3000_202608_202609_v1.massive_minute_route_validate_v1(a,tok);
 IF r->>'state' IS DISTINCT FROM 'BLOCKED' OR r->>'blocker_code' IS DISTINCT FROM 'MASSIVE_MINUTE_IDENTITY_EXPECTED_PRINTS_AND_ACCURACY_REQUIRED' THEN RAISE EXCEPTION 'MINUTE_FALSE_COMPANY_CERTIFICATION';END IF;PERFORM population_top3000_202608_202609_v1.pipeline_finish_v1(a,tok,r);
 -- Exercise the ACTUAL retained P01 NORMALIZE handler through the compatibility wrapper.
 rejected:=false;
 BEGIN
  SELECT t.* INTO legacy_task FROM population_top3000_202608_202609_v1.pipeline_task t WHERE t.family_id='P01' AND t.stage='NORMALIZE' AND t.payload->>'kind'='RETAINED_ALPACA_AUGUST_SESSION' ORDER BY t.created_at,t.task_id LIMIT 1;
  IF NOT FOUND THEN RAISE EXCEPTION 'EXISTING_RETAINED_P01_NORMALIZE_TASK_REQUIRED';END IF;
  a:=gen_random_uuid();tok:=encode(sha256(convert_to(gen_random_uuid()::text,'UTF8')),'hex');UPDATE population_top3000_202608_202609_v1.pipeline_task SET status='LEASED',attempt_id=a,lease_until=clock_timestamp()+interval '60 seconds',owner_fence=2,stop_generation=0,executions=executions+1 WHERE task_id=legacy_task.task_id;
  INSERT INTO population_top3000_202608_202609_v1.pipeline_attempt(attempt_id,task_id,execution_number,token_sha256,owner_fence,stop_generation,adapter_sha256) SELECT a,t.task_id,t.executions,encode(sha256(convert_to(tok,'UTF8')),'hex'),2,0,pa.function_sha256 FROM population_top3000_202608_202609_v1.pipeline_task t JOIN population_top3000_202608_202609_v1.pipeline_adapter pa ON pa.family_id=t.family_id AND pa.stage=t.stage WHERE t.task_id=legacy_task.task_id;
  SELECT function_oid::regprocedure INTO legacy_fn FROM population_top3000_202608_202609_v1.massive_minute_legacy_adapter_v1 WHERE stage='NORMALIZE';EXECUTE format('SELECT %s($1,$2)',regexp_replace(legacy_fn::text,'\(.*$','')) INTO direct_result USING a,tok;r:=population_top3000_202608_202609_v1.massive_minute_route_normalize_v1(a,tok);
  IF r IS DISTINCT FROM direct_result OR r->>'state' IS DISTINCT FROM 'BLOCKED' OR r->>'blocker_code' IS DISTINCT FROM 'RETAINED_PRICE_COMPANY_IDENTITY_AND_EXPECTATION_REQUIRED' THEN RAISE EXCEPTION 'MINUTE_RETAINED_P01_DELEGATION_CHANGED direct=% wrapper=%',direct_result,r;END IF;
  IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=legacy_task.task_id AND input_versions=legacy_task.input_versions AND checkpoint=legacy_task.checkpoint) THEN RAISE EXCEPTION 'MINUTE_LEGACY_INPUTS_OR_CHECKPOINT_CHANGED';END IF;
  RAISE EXCEPTION 'ROLLBACK_EXPECTED_LEGACY_DELEGATION_BRANCH';
 EXCEPTION WHEN OTHERS THEN IF SQLERRM='ROLLBACK_EXPECTED_LEGACY_DELEGATION_BRANCH' THEN rejected:=true;ELSE RAISE;END IF;END;
 IF NOT rejected THEN RAISE EXCEPTION 'MINUTE_LEGACY_BRANCH_NOT_EXERCISED';END IF;
 IF old_p02 IS DISTINCT FROM (SELECT jsonb_build_object('adapters',(SELECT jsonb_agg(to_jsonb(x) ORDER BY stage) FROM population_top3000_202608_202609_v1.pipeline_adapter x WHERE family_id='P02'),'binding',(SELECT to_jsonb(x) FROM population_top3000_202608_202609_v1.pipeline_family_binding x WHERE family_id='P02'),'tasks',(SELECT jsonb_agg(to_jsonb(x) ORDER BY task_id) FROM population_top3000_202608_202609_v1.pipeline_task x WHERE family_id='P02')))
 OR old_binding IS DISTINCT FROM (SELECT to_jsonb(x) FROM population_top3000_202608_202609_v1.pipeline_family_binding x WHERE family_id='P01') OR old_universe IS DISTINCT FROM (SELECT coalesce(jsonb_agg(to_jsonb(x)),'[]'::jsonb) FROM population_top3000_202608_202609_v1.pipeline_universe x) THEN RAISE EXCEPTION 'MINUTE_CHANGED_P02_BINDING_OR_UNIVERSE';END IF;
 IF old_legacy_functions IS DISTINCT FROM (SELECT jsonb_object_agg(stage,encode(sha256(convert_to(pg_get_functiondef(function_oid),'UTF8')),'hex')) FROM population_top3000_202608_202609_v1.massive_minute_legacy_adapter_v1) OR old_alpaca_hash IS DISTINCT FROM population_top3000_202608_202609_v1.alpaca_sip_code_hash_v1() OR old_retained_hash IS DISTINCT FROM encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.price_retained_partition_v1(uuid,text)'::regprocedure),'UTF8')),'hex') THEN RAISE EXCEPTION 'MINUTE_CHANGED_EXISTING_HANDLER_CODE';END IF;
 SELECT encode(sha256(convert_to(coalesce(string_agg(to_jsonb(x)::text,E'\n' ORDER BY manifest_id),''),'UTF8')),'hex') INTO current_manifests FROM population_top3000_202608_202609_v1.retained_price_partition_v1 x;IF current_manifests IS DISTINCT FROM old_manifests THEN RAISE EXCEPTION 'MINUTE_CHANGED_EXISTING_RETAINED_SOURCE_MANIFEST';END IF;
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.massive_minute_provisional_v1 WHERE company_id IS NOT NULL OR security_id IS NOT NULL OR source_published_at IS NOT NULL OR historical_received_at IS NOT NULL OR company_population_verified OR independent_price_accuracy_verified OR sip_feed_verified OR publication_eligible)
 OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_partition WHERE family_id='P01' AND partition_key='massive-minute-AAPL-20260908-v1' AND (publication_eligible OR denominator_state='FROZEN' OR expected_units IS NOT NULL)) THEN RAISE EXCEPTION 'MINUTE_FALSE_IDENTITY_TIMING_OR_COMPLETENESS';END IF;
END $test$;
SELECT 'PASS' AS result,'MASSIVE_MINUTE_PILOT_ROLLBACK_VERIFIED' AS receipt_event,clock_timestamp() AS observed_at,false AS live_provider_requested,false AS provider_accuracy_verified,
 population_top3000_202608_202609_v1.massive_minute_bundle_sha256_v1() AS minute_bundle_sha256,
 (SELECT jsonb_object_agg(stage,function_sha256) FROM population_top3000_202608_202609_v1.pipeline_adapter WHERE family_id='P01' AND stage IN('ACQUIRE','NORMALIZE','DERIVE','VALIDATE')) AS tested_function_sha256,
 (SELECT jsonb_object_agg(stage,function_sha256) FROM population_top3000_202608_202609_v1.massive_minute_legacy_adapter_v1) AS preserved_legacy_function_sha256,
 population_top3000_202608_202609_v1.alpaca_sip_code_hash_v1() AS preserved_alpaca_bundle_sha256,
 (SELECT count(*) FROM population_top3000_202608_202609_v1.massive_minute_page_v1) AS fixture_source_pages,
 (SELECT count(*) FROM population_top3000_202608_202609_v1.massive_minute_bar_v1) AS fixture_raw_occurrences,
 (SELECT source_summary FROM population_top3000_202608_202609_v1.massive_minute_page_v1 WHERE next_url IS NULL) AS fixture_summary,
 (SELECT jsonb_agg(to_jsonb(x) ORDER BY stage,batch_number) FROM pg_temp.massive_minute_fixture_timing x) AS batch_timings,
 true AS empty_source_retained_without_bar,true AS unknown_effect_lease_preserved,true AS original_retained_p01_delegation_preserved,true AS p02_unchanged;
ROLLBACK;
