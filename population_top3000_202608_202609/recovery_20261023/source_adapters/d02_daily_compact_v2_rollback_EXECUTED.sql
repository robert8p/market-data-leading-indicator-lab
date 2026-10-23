-- Root-only real SQL test. No Edge dispatch and no HTTP call. All writes roll back.
-- Run while the existing queue is quiescent; completed one-day D02 data stay in place.
BEGIN;
SET LOCAL lock_timeout='3s';SET LOCAL statement_timeout='55s';
SELECT pg_advisory_xact_lock(2026083000261022);
CREATE TEMP TABLE d02_v2_fixture_measurement(stage text,batch_number integer,elapsed_seconds numeric,source_rows integer,manifest_sha256 text) ON COMMIT DROP;
CREATE OR REPLACE FUNCTION pg_temp.d02_daily_fixture_lease(p_task uuid)
RETURNS jsonb LANGUAGE plpgsql SET search_path TO 'pg_catalog' AS $f$
DECLARE a uuid:=gen_random_uuid();tok text:=encode(sha256(convert_to(gen_random_uuid()::text,'UTF8')),'hex');
BEGIN
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=p_task AND family_id='D02' AND payload->>'kind'='MASSIVE_GROUPED_DAILY_V2' AND payload->>'fixture_only'='true' AND status IN('READY','BLOCKED')) THEN RAISE EXCEPTION 'FIXTURE_LEASE_SCOPE';END IF;
 UPDATE population_top3000_202608_202609_v1.pipeline_task SET status='LEASED',attempt_id=a,lease_until=clock_timestamp()+interval '60 seconds',owner_fence=2,stop_generation=0,executions=executions+1 WHERE task_id=p_task;
 INSERT INTO population_top3000_202608_202609_v1.pipeline_attempt(attempt_id,task_id,execution_number,token_sha256,owner_fence,stop_generation,adapter_sha256)
 SELECT a,p_task,t.executions,encode(sha256(convert_to(tok,'UTF8')),'hex'),2,0,ad.function_sha256 FROM population_top3000_202608_202609_v1.pipeline_task t JOIN population_top3000_202608_202609_v1.pipeline_adapter ad ON ad.family_id=t.family_id AND ad.stage=t.stage WHERE t.task_id=p_task;
 RETURN jsonb_build_object('attempt_id',a,'token',tok);
END $f$;
DO $test$
DECLARE tid uuid;a uuid;tok text;l jsonb;c jsonb;r jsonb;replay jsonb;vers jsonb;cal jsonb;body text;page jsonb;records jsonb;first_row jsonb;ms bigint;dayid bigint;i integer;rejected boolean;started timestamptz:=clock_timestamp();
 old_requests bigint;old_storage bigint;old_legacy_hash text;old_legacy_functions jsonb;old_binding jsonb;old_universe jsonb;actual_legacy_hash text;ad record;new_fn regprocedure;remaining_count bigint;batch_started timestamptz;legacy_direct_error text;legacy_route_error text;legacy_fn regprocedure;legacy_task population_top3000_202608_202609_v1.pipeline_task%rowtype;
BEGIN
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton AND owner_id='top3000-recovery-20261023-root-4b1314bcb5c2' AND owner_fence=2 AND stop_generation=0 AND enabled AND NOT stopped AND hold_reason IS NULL AND admitted_until>clock_timestamp()) THEN RAISE EXCEPTION 'OWNER_STOP_OR_ADMISSION_CHANGED';END IF;
 IF EXISTS(SELECT 1 FROM cron.job WHERE jobid=596 AND active) THEN RAISE EXCEPTION 'CRON_596_MUST_BE_INACTIVE_DURING_V2_TRANSFER_TEST';END IF;
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE status='LEASED') OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_provider_lease WHERE released_at IS NULL) THEN RAISE EXCEPTION 'EXISTING_LEASE_MUST_FINISH_BEFORE_TEST';END IF;
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.d02_daily_source_v2) THEN RAISE EXCEPTION 'V2_ALREADY_STARTED_RECONCILE_BEFORE_FIXTURE';END IF;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton AND shared_scheduler_verified AND budget_evidence->>'provider_admissions_allowed'='true' AND budget_evidence->'allowed_providers' ? 'Massive') THEN RAISE EXCEPTION 'EXISTING_MASSIVE_ADMISSION_REQUIRED';END IF;
 SELECT provider_requests,stored_bytes INTO old_requests,old_storage FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton;
 SELECT jsonb_object_agg(stage,encode(sha256(convert_to(pg_get_functiondef(function_oid),'UTF8')),'hex')) INTO old_legacy_functions FROM population_top3000_202608_202609_v1.pipeline_adapter WHERE family_id='D02' AND stage IN('ACQUIRE','NORMALIZE','DERIVE','VALIDATE','REPAIR');
 IF (SELECT count(*) FROM jsonb_object_keys(old_legacy_functions))<>5 THEN RAISE EXCEPTION 'ALL_FIVE_CURRENT_LEGACY_STAGE_ADAPTERS_REQUIRED';END IF;
 SELECT encode(sha256(convert_to(string_agg(unit_key||':'||record_sha256,E'\n' ORDER BY unit_key),'UTF8')),'hex') INTO old_legacy_hash FROM population_top3000_202608_202609_v1.source_observation WHERE release_id='top3000_companies_202608_202609_v1' AND family_id='D02' AND partition_key='grouped-20260803-raw-no-otc';
 SELECT to_jsonb(b) INTO old_binding FROM population_top3000_202608_202609_v1.pipeline_family_binding b WHERE family_id='D02';
 SELECT coalesce(jsonb_agg(to_jsonb(u)),'[]'::jsonb) INTO old_universe FROM population_top3000_202608_202609_v1.pipeline_universe u;
 UPDATE population_top3000_202608_202609_v1.pipeline_task SET not_before='infinity',blocker_until=NULL WHERE status IN('READY','BLOCKED');
 UPDATE population_top3000_202608_202609_v1.pipeline_control SET enabled=true WHERE singleton;
 FOR ad IN SELECT * FROM population_top3000_202608_202609_v1.pipeline_adapter WHERE family_id='D02' AND stage IN('ACQUIRE','NORMALIZE','DERIVE','VALIDATE','REPAIR') LOOP
  IF ad.function_sha256 IS DISTINCT FROM encode(sha256(convert_to(pg_get_functiondef(ad.function_oid),'UTF8')),'hex') OR NOT ad.enabled THEN RAISE EXCEPTION 'CURRENT_LEGACY_ADAPTER_NOT_VERIFIED';END IF;
  INSERT INTO population_top3000_202608_202609_v1.d02_daily_legacy_adapter_v2(stage,function_oid,function_sha256,registration_evidence) VALUES(ad.stage,ad.function_oid,ad.function_sha256,jsonb_build_object('fixture_only',true,'previous_registration',ad.reviewed_test_evidence)) ON CONFLICT(stage) DO NOTHING;
  IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.d02_daily_legacy_adapter_v2 WHERE stage=ad.stage AND function_oid=ad.function_oid AND function_sha256=ad.function_sha256) THEN RAISE EXCEPTION 'LEGACY_REGISTRY_CONFLICT';END IF;
  new_fn:=('population_top3000_202608_202609_v1.d02_daily_route_'||lower(ad.stage)||'_v2(uuid,text)')::regprocedure;
  UPDATE population_top3000_202608_202609_v1.pipeline_adapter SET function_oid=new_fn::oid,function_sha256=encode(sha256(convert_to(pg_get_functiondef(new_fn),'UTF8')),'hex'),enabled=true,reviewed_test_evidence='{"test":"D02_DAILY_V2_ROLLBACK","status":"IN_PROGRESS_ROLLBACK_ONLY"}',producer_revision='massive-daily-42-v2',expected_batch_seconds=45 WHERE family_id='D02' AND stage=ad.stage;
 END LOOP;
 cal:=population_top3000_202608_202609_v1.d02_daily_calendar_v2();
 vers:=jsonb_build_object('scope_sha256','8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd','source_contract_revision','massive-daily-42-v2','daily_bundle_sha256',population_top3000_202608_202609_v1.d02_daily_bundle_sha256_v2(),'calendar_manifest_sha256',cal->>'calendar_manifest_sha256','retained_aug3_source_sha256','787163eb9f6ffb408898c8a133c6ca63c822577187793d25c94b436f639bb97b','fixture_only',true);
 tid:=population_top3000_202608_202609_v1.pipeline_enqueue_v1('D02','ACQUIRE','daily-source-20260804-raw-no-otc-v2','{"kind":"MASSIVE_GROUPED_DAILY_V2","trading_date":"2026-08-04","source_mode":"FETCH","fixture_only":true}',vers);
 l:=pg_temp.d02_daily_fixture_lease(tid);a:=(l->>'attempt_id')::uuid;tok:=l->>'token';
 UPDATE population_top3000_202608_202609_v1.acquisition_control SET next_provider_at=clock_timestamp() WHERE singleton;
 rejected:=false;
 BEGIN
  UPDATE population_top3000_202608_202609_v1.pipeline_task SET input_versions=jsonb_set(input_versions,'{calendar_manifest_sha256}',to_jsonb(repeat('0',64))) WHERE task_id=tid;
  PERFORM population_top3000_202608_202609_v1.d02_daily_claim_v2(a,tok);
 EXCEPTION WHEN OTHERS THEN IF SQLERRM='D02_V2_SESSION_OR_SOURCE_MODE_NOT_ADMITTED' THEN rejected:=true;ELSE RAISE;END IF;END;
 IF NOT rejected OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_provider_lease WHERE attempt_id=a) THEN RAISE EXCEPTION 'MUTATED_CALENDAR_ADMITTED_PROVIDER_REQUEST';END IF;
 c:=population_top3000_202608_202609_v1.d02_daily_claim_v2(a,tok);
 IF c->>'state' IS DISTINCT FROM 'RESERVED' OR c->>'trading_date'<>'2026-08-04' THEN RAISE EXCEPTION 'D02_V2_FIXED_SESSION_CLAIM_FAILED %',c;END IF;
 rejected:=false;
 BEGIN PERFORM population_top3000_202608_202609_v1.d02_daily_claim_v2(a,tok);
 EXCEPTION WHEN OTHERS THEN IF SQLERRM='D02_V2_PRIOR_SOURCE_OR_REQUEST_REQUIRES_RECONCILIATION' THEN rejected:=true;ELSE RAISE;END IF;END;
 IF NOT rejected OR (SELECT provider_requests-old_requests FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton)<>1 THEN RAISE EXCEPTION 'D02_V2_DUPLICATE_CLAIM';END IF;
 ms:=(extract(epoch from timestamptz '2026-08-04 04:00:00Z')*1000)::bigint;
 SELECT jsonb_agg(jsonb_build_object('T','FIXTURE_'||lpad(n::text,4,'0'),'o',10,'h',12,'l',9,'c',11,'v',100,'n',5,'vw',10.5,'t',ms) ORDER BY n) INTO records FROM generate_series(1,1000) g(n);
 records:=jsonb_set(records,'{0}',(records->0)||'{"v":0,"n":0}');
 records:=jsonb_set(records,'{1}',(records->1)-'v'-'n'-'vw');
 records:=jsonb_set(records,'{2}',(records->2)||jsonb_build_object('o',0.00000000000000000001,'h',0.00000000000000000002,'l',0.00000000000000000001,'c',0.00000000000000000001,'v',9007199254740993::numeric,'vw',0.00000000000000000001));
 records:=jsonb_set(records,'{3}',(records->3)||jsonb_build_object('T',repeat('X',1000)));
 records:=jsonb_set(records,'{4}',(records->4)||jsonb_build_object('t',ms+86400000));
 records:=jsonb_set(records,'{5}',(records->5)||'{"h":1,"l":20}');
 records:=jsonb_set(records,'{6}',(records->6)||'{"v":-1,"n":1.5}');
 first_row:=records->0;records:=records||jsonb_build_array(first_row,first_row||'{"c":11.5}','{}'::jsonb);
 body:=jsonb_build_object('status','OK','adjusted',false,'queryCount',1003.0::numeric,'resultsCount',1003.0::numeric,'request_id','FIXTURE-D02-V2','results',records)::text;
 page:=jsonb_build_object('archive_key',c->>'archive_key','trading_date','2026-08-04','request_url',c->>'request_url','content_text',body,'source_sha256',encode(sha256(convert_to(body,'UTF8')),'hex'),'wire_sha256',encode(sha256(convert_to(body,'UTF8')),'hex'),'wire_bytes',octet_length(body),'http_status',200,'retrieved_at',clock_timestamp(),'result_count',1003,'provider_request_id','FIXTURE-D02-V2','elapsed_ms',1);
 rejected:=false;
 BEGIN PERFORM population_top3000_202608_202609_v1.d02_daily_commit_v2(a,tok,jsonb_set(page,'{source_sha256}',to_jsonb(repeat('0',64))));
 EXCEPTION WHEN OTHERS THEN IF SQLERRM='D02_V2_EXACT_WIRE_HASH_MISMATCH' THEN rejected:=true;ELSE RAISE;END IF;END;
 IF NOT rejected THEN RAISE EXCEPTION 'D02_V2_BAD_HASH_ACCEPTED';END IF;
 r:=population_top3000_202608_202609_v1.d02_daily_commit_v2(a,tok,page);
 IF r->>'state' IS DISTINCT FROM 'COMPLETE' THEN RAISE EXCEPTION 'D02_V2_SOURCE_COMMIT_FAILED %',r;END IF;
 SELECT source_day_id INTO dayid FROM population_top3000_202608_202609_v1.d02_daily_source_v2 WHERE session_date='2026-08-04';
 FOR i IN 1..2 LOOP
  SELECT task_id INTO tid FROM population_top3000_202608_202609_v1.pipeline_task WHERE family_id='D02' AND stage='NORMALIZE' AND partition_key='daily-source-20260804-raw-no-otc-v2' AND input_versions=vers;
  l:=pg_temp.d02_daily_fixture_lease(tid);a:=(l->>'attempt_id')::uuid;tok:=l->>'token';
  r:=population_top3000_202608_202609_v1.d02_daily_route_normalize_v2(a,tok);
  IF r->>'state' IS DISTINCT FROM (CASE WHEN i=1 THEN 'CONTINUE' ELSE 'COMPLETE' END) OR (r#>>'{commit_receipt,new_committed_units}')::integer<>(CASE WHEN i=1 THEN 1000 ELSE 3 END) THEN RAISE EXCEPTION 'D02_V2_BOUNDED_NORMALIZATION_FAILED %',r;END IF;
  replay:=population_top3000_202608_202609_v1.d02_daily_route_normalize_v2(a,tok);
  IF (replay#>>'{commit_receipt,new_committed_units}')::integer<>0 OR replay#>>'{commit_receipt,output_sha256}' IS DISTINCT FROM r#>>'{commit_receipt,output_sha256}' THEN RAISE EXCEPTION 'D02_V2_EXACT_REPLAY_FAILED';END IF;
  IF i=1 THEN
   rejected:=false;
   BEGIN
    UPDATE population_top3000_202608_202609_v1.d02_daily_bar_v2 SET provider_close=999 WHERE source_day_id=dayid AND source_row=1;
    PERFORM population_top3000_202608_202609_v1.d02_daily_route_normalize_v2(a,tok);
   EXCEPTION WHEN OTHERS THEN IF SQLERRM='D02_V2_TYPED_BATCH_REPLAY_CONFLICT' THEN rejected:=true;ELSE RAISE;END IF;END;
   IF NOT rejected THEN RAISE EXCEPTION 'D02_V2_FORGED_TYPED_ROW_ACCEPTED';END IF;
   rejected:=false;
   BEGIN
    UPDATE population_top3000_202608_202609_v1.pipeline_control SET budget_evidence=jsonb_set(budget_evidence,'{resource_limits,max_relation_bytes}','1') WHERE singleton;
    PERFORM population_top3000_202608_202609_v1.d02_daily_route_normalize_v2(a,tok);
   EXCEPTION WHEN OTHERS THEN IF SQLERRM='D02_V2_MISSION_RELATION_CAP_EXCEEDED' THEN rejected:=true;ELSE RAISE;END IF;END;
   IF NOT rejected THEN RAISE EXCEPTION 'D02_V2_CURRENT_RELATION_CAP_IGNORED';END IF;
  END IF;
  PERFORM population_top3000_202608_202609_v1.pipeline_finish_v1(a,tok,r);
 END LOOP;
 FOR i IN 1..2 LOOP
  SELECT task_id INTO tid FROM population_top3000_202608_202609_v1.pipeline_task WHERE family_id='D02' AND stage='DERIVE' AND partition_key='daily-source-20260804-raw-no-otc-v2' AND input_versions=vers;
  l:=pg_temp.d02_daily_fixture_lease(tid);a:=(l->>'attempt_id')::uuid;tok:=l->>'token';
  IF i=2 THEN
   rejected:=false;
   BEGIN
    UPDATE population_top3000_202608_202609_v1.d02_daily_bar_v2 SET provider_close=provider_close+1 WHERE source_day_id=dayid AND source_row=1;
    PERFORM population_top3000_202608_202609_v1.d02_daily_route_derive_v2(a,tok);
   EXCEPTION WHEN OTHERS THEN IF SQLERRM='D02_V2_TERMINAL_CURRENT_BATCH_PROOF_MISMATCH' THEN rejected:=true;ELSE RAISE;END IF;END;
   IF NOT rejected THEN RAISE EXCEPTION 'D02_V2_STALE_PRIOR_BATCH_PROOF_ACCEPTED';END IF;
  END IF;
  r:=population_top3000_202608_202609_v1.d02_daily_route_derive_v2(a,tok);
  IF r->>'state' IS DISTINCT FROM (CASE WHEN i=1 THEN 'CONTINUE' ELSE 'COMPLETE' END) THEN RAISE EXCEPTION 'D02_V2_BOUNDED_SOURCE_RECONCILIATION_FAILED %',r;END IF;
  PERFORM population_top3000_202608_202609_v1.pipeline_finish_v1(a,tok,r);
 END LOOP;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.d02_daily_bar_v2 WHERE source_day_id=dayid AND source_row=1 AND share_volume=0 AND trade_count=0 AND (missing_mask&192)=0 AND (invalid_mask&192)=0)
 OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.d02_daily_bar_v2 WHERE source_day_id=dayid AND source_row=2 AND share_volume IS NULL AND (missing_mask&64)=64)
 OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.d02_daily_bar_v2 WHERE source_day_id=dayid AND source_row=3 AND provider_close=0.00000000000000000001 AND share_volume=9007199254740993::numeric)
 OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.d02_daily_bar_v2 WHERE source_day_id=dayid AND source_row=4 AND source_ticker IS NULL AND (invalid_mask&1)=1)
 OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.d02_daily_bar_v2 WHERE source_day_id=dayid AND source_row=5 AND (structure_mask&2)=2)
 OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.d02_daily_bar_v2 WHERE source_day_id=dayid AND source_row=6 AND (structure_mask&1)=1)
 OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.d02_daily_source_v2 WHERE source_day_id=dayid AND verified_source_records=1003 AND source_summary->>'conflicting_ticker_groups'='1' AND source_summary->>'duplicate_identical_occurrences'='1') THEN RAISE EXCEPTION 'D02_V2_PRECISION_NULL_ZERO_OR_OCCURRENCE_SEMANTICS_FAILED';END IF;
 SELECT task_id INTO tid FROM population_top3000_202608_202609_v1.pipeline_task WHERE family_id='D02' AND stage='VALIDATE' AND partition_key='daily-source-20260804-raw-no-otc-v2' AND input_versions=vers;
 l:=pg_temp.d02_daily_fixture_lease(tid);a:=(l->>'attempt_id')::uuid;tok:=l->>'token';r:=population_top3000_202608_202609_v1.d02_daily_route_validate_v2(a,tok);
 IF r->>'state' IS DISTINCT FROM 'BLOCKED' OR r->>'blocker_code' IS DISTINCT FROM 'D02_V2_COMPANY_DAY_DENOMINATOR_AND_CLOSE_SEMANTICS_REQUIRED' THEN RAISE EXCEPTION 'D02_V2_FALSE_COMPANY_QUALITY_CERTIFICATION';END IF;
 PERFORM population_top3000_202608_202609_v1.pipeline_finish_v1(a,tok,r);
 -- Actual preserved Aug3 source is linked without making or charging another request.
 tid:=population_top3000_202608_202609_v1.pipeline_enqueue_v1('D02','ACQUIRE','daily-source-20260803-raw-no-otc-v2',jsonb_build_object('kind','MASSIVE_GROUPED_DAILY_V2','trading_date','2026-08-03','source_mode','REUSE_RETAINED_V1','fixture_only',true,'retained_source_id','d02-massive-grouped:4b94dfcf6c9e0582557862aa631bf1480a12dbc433ed0a53f2b3b844c6db2925:source'),vers);
 l:=pg_temp.d02_daily_fixture_lease(tid);a:=(l->>'attempt_id')::uuid;tok:=l->>'token';r:=population_top3000_202608_202609_v1.d02_daily_route_acquire_v2(a,tok);
 IF r->>'state' IS DISTINCT FROM 'COMPLETE' OR r#>>'{commit_receipt,reused_without_provider_request}' IS DISTINCT FROM 'true' OR (r#>>'{commit_receipt,source_records}')::integer<>12402 THEN RAISE EXCEPTION 'D02_V2_ACTUAL_AUG3_REUSE_FAILED';END IF;
 PERFORM population_top3000_202608_202609_v1.pipeline_finish_v1(a,tok,r);
 IF (SELECT provider_requests-old_requests FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton)<>1 OR (SELECT stored_bytes-old_storage FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton)<>octet_length(body) THEN RAISE EXCEPTION 'D02_V2_REUSE_CHARGED_OR_STORAGE_ACCOUNTING_FAILED';END IF;
 -- Benchmark the ACTUAL preserved 12,402-row source through all 13+13 bounded stages.
 FOR i IN 1..26 LOOP
  SELECT task_id INTO tid FROM population_top3000_202608_202609_v1.pipeline_task WHERE family_id='D02' AND stage=(CASE WHEN i<=13 THEN 'NORMALIZE' ELSE 'DERIVE' END) AND partition_key='daily-source-20260803-raw-no-otc-v2' AND input_versions=vers;
  l:=pg_temp.d02_daily_fixture_lease(tid);a:=(l->>'attempt_id')::uuid;tok:=l->>'token';batch_started:=clock_timestamp();
  r:=population_top3000_202608_202609_v1.d02_daily_route_v2(a,tok,CASE WHEN i<=13 THEN 'NORMALIZE' ELSE 'DERIVE' END);
  IF r->>'state' IS DISTINCT FROM (CASE WHEN i IN(13,26) THEN 'COMPLETE' ELSE 'CONTINUE' END) OR (r#>>'{commit_receipt,new_committed_units}')::integer<>(CASE WHEN i IN(13,26) THEN 402 ELSE 1000 END) THEN RAISE EXCEPTION 'ACTUAL_AUG3_BOUNDED_COMPACT_STAGE_FAILED batch=% receipt=%',i,r;END IF;
  PERFORM population_top3000_202608_202609_v1.pipeline_finish_v1(a,tok,r);
  INSERT INTO pg_temp.d02_v2_fixture_measurement VALUES(CASE WHEN i<=13 THEN 'NORMALIZE' ELSE 'DERIVE' END,CASE WHEN i<=13 THEN i ELSE i-13 END,extract(epoch from clock_timestamp()-batch_started),(r#>>'{commit_receipt,new_committed_units}')::integer,r#>>'{commit_receipt,output_sha256}');
 END LOOP;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.d02_daily_source_v2 WHERE session_date='2026-08-03' AND result_count=12402 AND normalized_source_records=12402 AND verified_source_records=12402 AND source_verified_at IS NOT NULL AND source_summary->>'source_records_reconciled'='true') THEN RAISE EXCEPTION 'ACTUAL_AUG3_FULL_COMPACT_RECONCILIATION_FAILED';END IF;
 -- Exercise real completed legacy NORMALIZE without changing its original inputs/checkpoint.
 -- Both calls must reach its existing explicit replay guard; this is delegation evidence.
 rejected:=false;
 BEGIN
  SELECT * INTO legacy_task FROM population_top3000_202608_202609_v1.pipeline_task WHERE family_id='D02' AND partition_key='grouped-20260803-raw-no-otc' AND stage='NORMALIZE' AND status='COMPLETE' ORDER BY created_at LIMIT 1;
  IF NOT FOUND THEN RAISE EXCEPTION 'COMPLETED_ORIGINAL_NORMALIZE_TASK_REQUIRED';END IF;
  a:=gen_random_uuid();tok:=encode(sha256(convert_to(gen_random_uuid()::text,'UTF8')),'hex');
  UPDATE population_top3000_202608_202609_v1.pipeline_task SET status='LEASED',attempt_id=a,lease_until=clock_timestamp()+interval '60 seconds',owner_fence=2,stop_generation=0,executions=executions+1 WHERE task_id=legacy_task.task_id;
  INSERT INTO population_top3000_202608_202609_v1.pipeline_attempt(attempt_id,task_id,execution_number,token_sha256,owner_fence,stop_generation,adapter_sha256)
  SELECT a,t.task_id,t.executions,encode(sha256(convert_to(tok,'UTF8')),'hex'),2,0,pa.function_sha256 FROM population_top3000_202608_202609_v1.pipeline_task t JOIN population_top3000_202608_202609_v1.pipeline_adapter pa ON pa.family_id=t.family_id AND pa.stage=t.stage WHERE t.task_id=legacy_task.task_id;
  SELECT function_oid::regprocedure INTO legacy_fn FROM population_top3000_202608_202609_v1.d02_daily_legacy_adapter_v2 WHERE stage='NORMALIZE';
  BEGIN
   EXECUTE format('SELECT %s($1,$2)',regexp_replace(legacy_fn::text,'\(.*$','')) INTO r USING a,tok;
  EXCEPTION WHEN OTHERS THEN legacy_direct_error:=SQLERRM;END;
  BEGIN
   PERFORM population_top3000_202608_202609_v1.d02_daily_route_normalize_v2(a,tok);
  EXCEPTION WHEN OTHERS THEN legacy_route_error:=SQLERRM;END;
  IF legacy_direct_error IS DISTINCT FROM 'D02_GROUPED_NORMALIZE_CHECKPOINT_MISMATCH' OR legacy_route_error IS DISTINCT FROM legacy_direct_error THEN RAISE EXCEPTION 'ORIGINAL_LEGACY_GUARD_NOT_PRESERVED direct=% wrapper=%',legacy_direct_error,legacy_route_error;END IF;
  IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=legacy_task.task_id AND input_versions=legacy_task.input_versions AND checkpoint=legacy_task.checkpoint) THEN RAISE EXCEPTION 'LEGACY_INPUTS_OR_CHECKPOINT_CHANGED';END IF;
  RAISE EXCEPTION 'ROLLBACK_EXPECTED_LEGACY_DELEGATION_BRANCH';
 EXCEPTION WHEN OTHERS THEN IF SQLERRM='ROLLBACK_EXPECTED_LEGACY_DELEGATION_BRANCH' THEN rejected:=true;ELSE RAISE;END IF;END;
 IF NOT rejected THEN RAISE EXCEPTION 'LEGACY_DELEGATION_NOT_EXERCISED';END IF;
 -- Empty source is a retained response with zero typed rows, never an invented zero-volume bar.
 tid:=population_top3000_202608_202609_v1.pipeline_enqueue_v1('D02','ACQUIRE','daily-source-20260805-raw-no-otc-v2','{"kind":"MASSIVE_GROUPED_DAILY_V2","trading_date":"2026-08-05","source_mode":"FETCH","fixture_only":true}',vers);
 l:=pg_temp.d02_daily_fixture_lease(tid);a:=(l->>'attempt_id')::uuid;tok:=l->>'token';UPDATE population_top3000_202608_202609_v1.acquisition_control SET next_provider_at=clock_timestamp() WHERE singleton;c:=population_top3000_202608_202609_v1.d02_daily_claim_v2(a,tok);
 IF c->>'state' IS DISTINCT FROM 'RESERVED' THEN RAISE EXCEPTION 'D02_V2_EMPTY_FIXTURE_RESERVE_FAILED %',c;END IF;
 body:='{"status":"OK","adjusted":false,"resultsCount":0,"results":[]}';
 page:=jsonb_build_object('archive_key',c->>'archive_key','trading_date','2026-08-05','request_url',c->>'request_url','content_text',body,'source_sha256',encode(sha256(convert_to(body,'UTF8')),'hex'),'wire_sha256',encode(sha256(convert_to(body,'UTF8')),'hex'),'wire_bytes',octet_length(body),'http_status',200,'retrieved_at',clock_timestamp(),'result_count',0,'elapsed_ms',1);
 r:=population_top3000_202608_202609_v1.d02_daily_commit_v2(a,tok,page);
 FOR i IN 1..2 LOOP
  SELECT task_id INTO tid FROM population_top3000_202608_202609_v1.pipeline_task WHERE family_id='D02' AND stage=(CASE WHEN i=1 THEN 'NORMALIZE' ELSE 'DERIVE' END) AND partition_key='daily-source-20260805-raw-no-otc-v2' AND input_versions=vers;
  l:=pg_temp.d02_daily_fixture_lease(tid);a:=(l->>'attempt_id')::uuid;tok:=l->>'token';r:=population_top3000_202608_202609_v1.d02_daily_route_v2(a,tok,CASE WHEN i=1 THEN 'NORMALIZE' ELSE 'DERIVE' END);
  IF r->>'state' IS DISTINCT FROM 'COMPLETE' OR r#>>'{commit_receipt,new_committed_units}' IS DISTINCT FROM '0' THEN RAISE EXCEPTION 'D02_V2_EMPTY_RESPONSE_BECAME_BAR';END IF;
  PERFORM population_top3000_202608_202609_v1.pipeline_finish_v1(a,tok,r);
 END LOOP;
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.d02_daily_bar_v2 b JOIN population_top3000_202608_202609_v1.d02_daily_source_v2 d USING(source_day_id) WHERE d.session_date='2026-08-05') THEN RAISE EXCEPTION 'D02_V2_EMPTY_SOURCE_INSERTED_ZERO_BAR';END IF;
 -- Partial HTTP200 classified PARSER still has unknown provider effect and retains its lease.
 rejected:=false;
 BEGIN
  tid:=population_top3000_202608_202609_v1.pipeline_enqueue_v1('D02','ACQUIRE','daily-source-20260806-raw-no-otc-v2','{"kind":"MASSIVE_GROUPED_DAILY_V2","trading_date":"2026-08-06","source_mode":"FETCH","fixture_only":true}',vers);
  l:=pg_temp.d02_daily_fixture_lease(tid);a:=(l->>'attempt_id')::uuid;tok:=l->>'token';UPDATE population_top3000_202608_202609_v1.acquisition_control SET next_provider_at=clock_timestamp() WHERE singleton;c:=population_top3000_202608_202609_v1.d02_daily_claim_v2(a,tok);
  IF c->>'state' IS DISTINCT FROM 'RESERVED' THEN RAISE EXCEPTION 'D02_V2_UNKNOWN_FIXTURE_RESERVE_FAILED %',c;END IF;
  r:=population_top3000_202608_202609_v1.d02_daily_failure_v2(a,tok,'{"code":"RESPONSE_TOO_LARGE","failure_class":"PARSER","http_status":200,"fetch_started":true,"received_bytes":8388608,"wire_sha256":null,"retry_after_seconds":0}');
  IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_provider_lease WHERE attempt_id=a AND released_at IS NULL) OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=tid AND status='BLOCKED' AND last_failure_class='UNCERTAIN_EFFECT') THEN RAISE EXCEPTION 'D02_V2_UNKNOWN_EFFECT_NOT_RETAINED';END IF;
  RAISE EXCEPTION 'ROLLBACK_EXPECTED_UNKNOWN_EFFECT_BRANCH';
 EXCEPTION WHEN OTHERS THEN IF SQLERRM='ROLLBACK_EXPECTED_UNKNOWN_EFFECT_BRANCH' THEN rejected:=true;ELSE RAISE;END IF;END;
 IF NOT rejected THEN RAISE EXCEPTION 'D02_V2_UNKNOWN_EFFECT_BRANCH_NOT_EXERCISED';END IF;
 SELECT encode(sha256(convert_to(string_agg(unit_key||':'||record_sha256,E'\n' ORDER BY unit_key),'UTF8')),'hex') INTO actual_legacy_hash FROM population_top3000_202608_202609_v1.source_observation WHERE release_id='top3000_companies_202608_202609_v1' AND family_id='D02' AND partition_key='grouped-20260803-raw-no-otc';
 IF actual_legacy_hash IS DISTINCT FROM old_legacy_hash OR old_binding IS DISTINCT FROM (SELECT to_jsonb(b) FROM population_top3000_202608_202609_v1.pipeline_family_binding b WHERE family_id='D02') OR old_universe IS DISTINCT FROM (SELECT coalesce(jsonb_agg(to_jsonb(u)),'[]'::jsonb) FROM population_top3000_202608_202609_v1.pipeline_universe u) THEN RAISE EXCEPTION 'D02_V2_CHANGED_ORIGINAL_DATA_BINDING_OR_UNIVERSE';END IF;
 IF old_legacy_functions IS DISTINCT FROM (SELECT jsonb_object_agg(stage,encode(sha256(convert_to(pg_get_functiondef(function_oid),'UTF8')),'hex')) FROM population_top3000_202608_202609_v1.d02_daily_legacy_adapter_v2) THEN RAISE EXCEPTION 'D02_V2_CHANGED_ORIGINAL_FUNCTIONS';END IF;
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_partition WHERE family_id='D02' AND partition_key LIKE 'daily-source-%-v2' AND (publication_eligible OR denominator_state='FROZEN' OR expected_units IS NOT NULL)) THEN RAISE EXCEPTION 'D02_V2_FABRICATED_COMPANY_DENOMINATOR';END IF;
END $test$;
SELECT 'PASS' result,'D02_DAILY_V2_ROLLBACK_VERIFIED' receipt_event,clock_timestamp() observed_at,false live_provider_requested,false provider_accuracy_verified,
 population_top3000_202608_202609_v1.d02_daily_bundle_sha256_v2() daily_bundle_sha256,
 (SELECT jsonb_object_agg(stage,function_sha256) FROM population_top3000_202608_202609_v1.pipeline_adapter WHERE family_id='D02' AND stage IN('ACQUIRE','NORMALIZE','DERIVE','VALIDATE','REPAIR')) tested_function_sha256,
 (SELECT jsonb_object_agg(stage,function_sha256) FROM population_top3000_202608_202609_v1.d02_daily_legacy_adapter_v2) preserved_legacy_function_sha256,
 (SELECT sum(result_count) FROM population_top3000_202608_202609_v1.d02_daily_source_v2) source_rows_including_reused_aug3,
 (SELECT count(*) FROM population_top3000_202608_202609_v1.d02_daily_bar_v2) typed_occurrences_including_reused_aug3,
 (SELECT jsonb_build_object('batches',count(*),'total_seconds',sum(elapsed_seconds),'max_seconds',max(elapsed_seconds),'by_batch',jsonb_agg(to_jsonb(m) ORDER BY stage,batch_number)) FROM pg_temp.d02_v2_fixture_measurement m) actual_aug3_bounded_benchmark,
 (SELECT encode(sha256(convert_to(string_agg(encode(sha256(convert_to((to_jsonb(b)-'source_day_id')::text,'UTF8')),'hex'),E'\n' ORDER BY b.source_row),'UTF8')),'hex') FROM population_top3000_202608_202609_v1.d02_daily_bar_v2 b JOIN population_top3000_202608_202609_v1.d02_daily_source_v2 d USING(source_day_id) WHERE d.session_date='2026-08-03') actual_aug3_full_typed_manifest_sha256,
 true original_legacy_normalize_guard_preserved,
 (SELECT jsonb_agg(jsonb_build_object('date',session_date,'count',result_count,'normalized',normalized_source_records,'verified',verified_source_records,'summary',source_summary) ORDER BY session_date) FROM population_top3000_202608_202609_v1.d02_daily_source_v2) fixture_summary;
ROLLBACK;
