-- Root-only rollback fixture. Two synthetic source pages; no Edge/provider request.
BEGIN;
SET LOCAL lock_timeout='3s';SET LOCAL statement_timeout='50s';
SELECT pg_advisory_xact_lock(2026083000261022);
DO $test$
DECLARE tid uuid;normal_tid uuid;failure_tid uuid;a uuid;token text;replay jsonb;normalized_result jsonb;vers jsonb;bundle text;c jsonb;r jsonb;body text;page jsonb;cursor_url text;i integer;j integer;reject_seen boolean;
july15 jsonb;july31 jsonb;aug14 jsonb;aug31 jsonb;sep15 jsonb;sep30 jsonb;changed_event jsonb;bad_event jsonb;large_symbol jsonb;
old_storage bigint;old_requests bigint;expected_stored_bytes bigint:=0;universe_before text;universe_after text;
BEGIN
IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton AND owner_id='top3000-recovery-20261023-root-4b1314bcb5c2' AND owner_fence=2 AND stop_generation=0 AND NOT stopped AND enabled AND hold_reason IS NULL AND admitted_until>clock_timestamp()) THEN RAISE EXCEPTION 'OWNER_STOP_OR_ADMISSION_CHANGED';END IF;
IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE status='LEASED') OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_provider_lease WHERE released_at IS NULL) THEN RAISE EXCEPTION 'EXISTING_LEASE_MUST_FINISH_BEFORE_TEST';END IF;
IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE family_id='S02') OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.source_observation WHERE family_id='S02') THEN RAISE EXCEPTION 'S02_ALREADY_STARTED_RECONCILE_BEFORE_FIXTURE_TEST';END IF;
IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton AND budget_evidence->>'provider_admissions_allowed'='true' AND budget_evidence->'allowed_providers' ? 'Massive') THEN RAISE EXCEPTION 'EXISTING_MASSIVE_ADMISSION_REQUIRED';END IF;
SELECT stored_bytes,provider_requests INTO old_storage,old_requests FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton;
SELECT encode(sha256(convert_to(coalesce(jsonb_agg(to_jsonb(u)),'[]'::jsonb)::text,'UTF8')),'hex') INTO universe_before FROM population_top3000_202608_202609_v1.pipeline_universe u;
UPDATE population_top3000_202608_202609_v1.pipeline_task SET not_before='infinity'::timestamptz,blocker_until=NULL WHERE status IN('READY','BLOCKED');
INSERT INTO population_top3000_202608_202609_v1.pipeline_family_binding(family_id,expected_relation,observed_relation,expected_definition_sha256,observed_definition_sha256,binding_version,requires_company_universe,expected_partition_count,partition_manifest_evidence,partition_manifest_frozen)
VALUES('S02','population_top3000_202608_202609_v1.expected_observation'::regclass,'population_top3000_202608_202609_v1.source_observation'::regclass,population_top3000_202608_202609_v1.pipeline_relation_hash_v1('population_top3000_202608_202609_v1.expected_observation'::regclass),population_top3000_202608_202609_v1.pipeline_relation_hash_v1('population_top3000_202608_202609_v1.source_observation'::regclass),'s02-provisional-reports-20261023',true,NULL,'{"source_partition":"short-interest-settlement-20260715-20260930","company_report_census_verified":false,"scheduled_publication_has_date_precision_only":true}',false)
ON CONFLICT(family_id) DO NOTHING;
INSERT INTO population_top3000_202608_202609_v1.pipeline_adapter(family_id,stage,function_oid,function_sha256,resource_class,provider,enabled,reviewed_test_evidence,producer_revision,expected_batch_seconds)
SELECT 'S02',stage,('population_top3000_202608_202609_v1.'||fn)::regprocedure::oid,encode(sha256(convert_to(pg_get_functiondef(('population_top3000_202608_202609_v1.'||fn)::regprocedure),'UTF8')),'hex'),resource,CASE WHEN resource='PROVIDER' THEN 'Massive' ELSE NULL END,true,'{"test":"S02_TWO_PAGE_SOURCE_AND_SCHEDULE_ROLLBACK","status":"IN_PROGRESS_ROLLBACK_ONLY","provider_accuracy_verified":false}'::jsonb,'massive-shortinterest-20261023-v1',45
FROM (VALUES ('ACQUIRE','massive_shortinterest_acquire_v1(uuid,text)','PROVIDER'),('NORMALIZE','massive_shortinterest_normalize_v1(uuid,text)','DATABASE'),('DERIVE','massive_shortinterest_derive_v1(uuid,text)','DATABASE'),('VALIDATE','pipeline_validate_v1(uuid,text)','DATABASE'),('REPAIR','massive_shortinterest_repair_hold_v1(uuid,text)','DATABASE')) x(stage,fn,resource) ON CONFLICT(family_id,stage) DO NOTHING;
bundle:=population_top3000_202608_202609_v1.massive_shortinterest_bundle_sha256_v1();
vers:=jsonb_build_object('scope_sha256','8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd','archive_contract_revision','massive-shortinterest-20261023-v1','shortinterest_bundle_sha256',bundle,'fixture_only',true);
tid:=population_top3000_202608_202609_v1.pipeline_enqueue_v1('S02','ACQUIRE','short-interest-settlement-20260715-20260930','{"max_pages":1,"fixture_only":true}'::jsonb,vers);
july15:='{"ticker":"FIXTURE_A","settlement_date":"2026-07-15","short_interest":0,"avg_daily_volume":100,"days_to_cover":0}'::jsonb;
july31:=july15||'{"settlement_date":"2026-07-31","short_interest":10,"avg_daily_volume":5,"days_to_cover":2}'::jsonb;
aug14:=july15||'{"settlement_date":"2026-08-14","short_interest":100,"avg_daily_volume":50,"days_to_cover":2}'::jsonb;
changed_event:=aug14||'{"short_interest":101}'::jsonb;
aug31:=july15||'{"settlement_date":"2026-08-31","short_interest":9007199254740993,"avg_daily_volume":0.00000000000000000001,"days_to_cover":0.00000000000000000001}'::jsonb;
sep15:=july15||'{"settlement_date":"2026-09-15","short_interest":50,"avg_daily_volume":0,"days_to_cover":null}'::jsonb;
sep30:=july15||'{"settlement_date":"2026-09-30","short_interest":70,"avg_daily_volume":10,"days_to_cover":7}'::jsonb;
bad_event:=aug14||'{"ticker":"FIXTURE_BAD","settlement_date":"2026-08-15","short_interest":-1}'::jsonb;
large_symbol:=sep15||jsonb_build_object('ticker',repeat('MALFORMED',1000));
cursor_url:='https://api.massive.com/stocks/v1/short-interest?cursor=FIXTURE_PAGE_2';
FOR i IN 1..2 LOOP
 a:=gen_random_uuid();token:=repeat(CASE WHEN i=1 THEN 'a' ELSE 'b' END,64);
 UPDATE population_top3000_202608_202609_v1.acquisition_control SET next_provider_at=clock_timestamp() WHERE singleton;
 UPDATE population_top3000_202608_202609_v1.pipeline_task SET status='LEASED',attempt_id=a,lease_until=clock_timestamp()+interval '60 seconds',owner_fence=2,stop_generation=0,executions=executions+1 WHERE task_id=tid;
 INSERT INTO population_top3000_202608_202609_v1.pipeline_attempt(attempt_id,task_id,execution_number,token_sha256,owner_fence,stop_generation,adapter_sha256)
 SELECT a,tid,t.executions,encode(sha256(convert_to(token,'UTF8')),'hex'),2,0,ad.function_sha256 FROM population_top3000_202608_202609_v1.pipeline_task t JOIN population_top3000_202608_202609_v1.pipeline_adapter ad ON ad.family_id=t.family_id AND ad.stage=t.stage WHERE t.task_id=tid;
 IF i=1 THEN
  reject_seen:=false;
  BEGIN
   UPDATE population_top3000_202608_202609_v1.pipeline_task SET input_versions=jsonb_set(input_versions,'{shortinterest_bundle_sha256}',to_jsonb(repeat('0',64))) WHERE task_id=tid;
   PERFORM population_top3000_202608_202609_v1.massive_shortinterest_claim_v1(a,token);
  EXCEPTION WHEN OTHERS THEN IF SQLERRM='MASSIVE_SHORTINTEREST_SCOPE_NOT_ADMITTED' THEN reject_seen:=true;ELSE RAISE;END IF;
  END;
  IF NOT reject_seen OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_provider_lease WHERE attempt_id=a) THEN RAISE EXCEPTION 'S02_WRONG_BUNDLE_ADMITTED_REQUEST';END IF;
 END IF;
 c:=population_top3000_202608_202609_v1.massive_shortinterest_claim_v1(a,token);
 IF c->>'state' IS DISTINCT FROM 'RESERVED' OR c->>'family_id' IS DISTINCT FROM 'S02' OR (c->>'page_number')::integer<>i THEN RAISE EXCEPTION 'S02_CLAIM_FAILED %',c;END IF;
 IF i=1 AND c->>'request_url' IS DISTINCT FROM 'https://api.massive.com/stocks/v1/short-interest?settlement_date.gte=2026-07-15&settlement_date.lte=2026-09-30&sort=settlement_date.asc,ticker.asc&limit=1000' THEN RAISE EXCEPTION 'S02_FIXED_ROUTE_MISMATCH';END IF;
 IF i=2 AND c->>'request_url' IS DISTINCT FROM cursor_url THEN RAISE EXCEPTION 'S02_CURSOR_NOT_PRESERVED';END IF;
 body:=CASE WHEN i=1 THEN jsonb_build_object('status','OK','request_id','FIXTURE-S02-1','results',jsonb_build_array(july15,july31,aug14,aug14),'next_url',cursor_url)::text ELSE jsonb_build_object('status','OK','request_id','FIXTURE-S02-2','results',jsonb_build_array(aug14,changed_event,aug31,sep15,sep30,bad_event,large_symbol))::text END;
 page:=jsonb_build_object('archive_key',c->>'archive_key','page_number',i,'request_url',c->>'request_url','next_url',CASE WHEN i=1 THEN cursor_url ELSE NULL END,'content_text',body,'source_sha256',encode(sha256(convert_to(body,'UTF8')),'hex'),'wire_sha256',encode(sha256(convert_to(body,'UTF8')),'hex'),'wire_bytes',octet_length(body),'http_status',200,'retrieved_at',clock_timestamp(),'result_count',CASE WHEN i=1 THEN 4 ELSE 7 END,'provider_request_id','FIXTURE-S02-'||i::text,'elapsed_ms',1);
 IF i=1 THEN
  -- The bound is the current admission, not a worker-owned replacement cap.
  reject_seen:=false;
  BEGIN
   UPDATE population_top3000_202608_202609_v1.pipeline_control SET budget_evidence=jsonb_set(budget_evidence,'{resource_limits,max_relation_bytes}','1'::jsonb) WHERE singleton;
   PERFORM population_top3000_202608_202609_v1.massive_shortinterest_commit_v1(a,token,page);
  EXCEPTION WHEN OTHERS THEN IF SQLERRM='S02_POST_BATCH_RELATION_CAP_EXCEEDED' THEN reject_seen:=true;ELSE RAISE;END IF;
  END;
  IF NOT reject_seen OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.massive_shortinterest_page_v1) OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_provider_lease WHERE attempt_id=a AND released_at IS NULL) THEN RAISE EXCEPTION 'S02_RESOURCE_BOUND_DID_NOT_BLOCK_ATOMICALLY';END IF;
 END IF;
 r:=population_top3000_202608_202609_v1.massive_shortinterest_commit_v1(a,token,page);
 IF r->>'state' IS DISTINCT FROM (CASE WHEN i=1 THEN 'CONTINUE' ELSE 'COMPLETE' END) THEN RAISE EXCEPTION 'S02_PAGE_COMMIT_FAILED %',r;END IF;
 expected_stored_bytes:=expected_stored_bytes+octet_length(body);
 IF i=1 THEN
  UPDATE population_top3000_202608_202609_v1.pipeline_control SET enabled=true,next_wakeup_at=clock_timestamp() WHERE singleton;
  UPDATE population_top3000_202608_202609_v1.pipeline_task SET not_before=clock_timestamp() WHERE task_id=tid;
  UPDATE population_top3000_202608_202609_v1.acquisition_control SET next_provider_at=clock_timestamp() WHERE singleton;
  r:=population_top3000_202608_202609_v1.pipeline_tick_v1();
  IF r->>'state' IS DISTINCT FROM 'DISPATCHED' OR r#>>'{result,state}' IS DISTINCT FROM 'BLOCKED' OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=tid AND blocker_code='S02_PAGE_CAP_REVIEW' AND checkpoint->>'next_page'='2') OR (SELECT provider_requests-old_requests FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton)<>1 THEN RAISE EXCEPTION 'S02_ONE_PAGE_CAP_DID_NOT_HOLD_WITHOUT_REQUEST %',r;END IF;
  -- A synthetic controller-reviewed cap increase, only inside this rollback transaction.
  UPDATE population_top3000_202608_202609_v1.pipeline_task SET payload=jsonb_set(payload,'{max_pages}','2'::jsonb),status='READY',blocker_code=NULL,blocker_until=NULL,resumption_condition=NULL,not_before=clock_timestamp() WHERE task_id=tid;
 END IF;
END LOOP;
-- Lease the first normalization page once. A repeated invocation at its unchanged
-- active checkpoint must verify every existing occurrence and add zero rows.
SELECT task_id INTO STRICT normal_tid FROM population_top3000_202608_202609_v1.pipeline_task WHERE family_id='S02' AND stage='NORMALIZE' AND input_versions=vers;
a:=gen_random_uuid();token:=repeat('e',64);
UPDATE population_top3000_202608_202609_v1.pipeline_task SET status='LEASED',attempt_id=a,lease_until=clock_timestamp()+interval '60 seconds',owner_fence=2,stop_generation=0,executions=executions+1 WHERE task_id=normal_tid;
INSERT INTO population_top3000_202608_202609_v1.pipeline_attempt(attempt_id,task_id,execution_number,token_sha256,owner_fence,stop_generation,adapter_sha256)
SELECT a,normal_tid,t.executions,encode(sha256(convert_to(token,'UTF8')),'hex'),2,0,ad.function_sha256 FROM population_top3000_202608_202609_v1.pipeline_task t JOIN population_top3000_202608_202609_v1.pipeline_adapter ad ON ad.family_id=t.family_id AND ad.stage=t.stage WHERE t.task_id=normal_tid;
normalized_result:=population_top3000_202608_202609_v1.massive_shortinterest_normalize_v1(a,token);
replay:=population_top3000_202608_202609_v1.massive_shortinterest_normalize_v1(a,token);
IF normalized_result->>'state' IS DISTINCT FROM 'CONTINUE' OR normalized_result#>>'{commit_receipt,new_committed_units}' IS DISTINCT FROM '4' OR replay#>>'{commit_receipt,new_committed_units}' IS DISTINCT FROM '0' OR normalized_result#>>'{commit_receipt,output_sha256}' IS DISTINCT FROM replay#>>'{commit_receipt,output_sha256}' THEN RAISE EXCEPTION 'S02_NORMALIZE_REPLAY_DID_NOT_RECONCILE';END IF;
reject_seen:=false;
BEGIN
 UPDATE population_top3000_202608_202609_v1.massive_shortinterest_record_v1
 SET raw_record=jsonb_set(raw_record,'{short_interest}','99'::jsonb),raw_record_sha256=encode(sha256(convert_to(jsonb_set(raw_record,'{short_interest}','99'::jsonb)::text,'UTF8')),'hex')
 WHERE page_number=1 AND source_row=1;
 PERFORM population_top3000_202608_202609_v1.massive_shortinterest_normalize_v1(a,token);
EXCEPTION WHEN OTHERS THEN IF SQLERRM='S02_RAW_OCCURRENCE_REPLAY_CONFLICT' THEN reject_seen:=true;ELSE RAISE;END IF;
END;
IF NOT reject_seen THEN RAISE EXCEPTION 'S02_ATOMIC_RAW_TAMPER_WAS_ACCEPTED';END IF;
reject_seen:=false;
BEGIN
 UPDATE population_top3000_202608_202609_v1.source_document SET source_published_at=clock_timestamp()
 WHERE source_id=(SELECT source_id FROM population_top3000_202608_202609_v1.massive_shortinterest_page_v1 WHERE page_number=1);
 PERFORM population_top3000_202608_202609_v1.massive_shortinterest_normalize_v1(a,token);
EXCEPTION WHEN OTHERS THEN IF SQLERRM='S02_RETAINED_SOURCE_INTEGRITY' THEN reject_seen:=true;ELSE RAISE;END IF;
END;
IF NOT reject_seen THEN RAISE EXCEPTION 'S02_FORGED_PUBLICATION_TIME_WAS_ACCEPTED';END IF;
PERFORM population_top3000_202608_202609_v1.pipeline_finish_v1(a,token,normalized_result);
UPDATE population_top3000_202608_202609_v1.pipeline_control SET enabled=true,next_wakeup_at=clock_timestamp() WHERE singleton;
FOR j IN 1..5 LOOP
 r:=population_top3000_202608_202609_v1.pipeline_tick_v1();
 IF r->>'state' IS DISTINCT FROM 'DISPATCHED' OR r#>>'{result,state}' IS DISTINCT FROM (CASE WHEN j=2 THEN 'CONTINUE' WHEN j=5 THEN 'BLOCKED' ELSE 'COMPLETE' END) OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=(r->>'task_id')::uuid AND family_id='S02' AND stage=(CASE WHEN j=1 THEN 'NORMALIZE' WHEN j<=3 THEN 'DERIVE' WHEN j=4 THEN 'VALIDATE' ELSE 'REPAIR' END)) THEN RAISE EXCEPTION 'S02_PIPELINE_DISPATCH_FAILED tick=% receipt=%',j,r;END IF;
END LOOP;
IF (SELECT count(*) FROM population_top3000_202608_202609_v1.massive_shortinterest_page_v1)<>2 OR (SELECT count(*) FROM population_top3000_202608_202609_v1.massive_shortinterest_record_v1)<>11 OR (SELECT count(*) FROM population_top3000_202608_202609_v1.source_observation WHERE family_id='S02')<>9 OR (SELECT count(*) FROM population_top3000_202608_202609_v1.expected_observation WHERE family_id='S02')<>9 THEN RAISE EXCEPTION 'S02_PAGE_OR_REPORT_DEDUPLICATION_COUNT';END IF;
IF (SELECT count(*) FROM population_top3000_202608_202609_v1.source_observation WHERE family_id='S02' AND values_json->>'ticker'='FIXTURE_A' AND effective_date='2026-08-14' AND field_state->>'short_interest'='CONFLICTING')<>2 OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.source_observation WHERE family_id='S02' AND values_json->>'ticker'='FIXTURE_A' AND effective_date='2026-08-14' AND values_json->>'short_interest'='100' AND lineage->>'source_occurrences'='3' AND lineage->>'source_page'='1') THEN RAISE EXCEPTION 'S02_DUPLICATE_PROVENANCE_OR_CHANGED_VERSION_LOST';END IF;
IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.source_observation WHERE family_id='S02' AND effective_date='2026-07-15' AND time_role='SUPPORT' AND values_json->>'scheduled_publication_date'='2026-07-24' AND values_json->>'scheduled_publication_in_target_window'='false') OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.source_observation WHERE family_id='S02' AND effective_date='2026-07-31' AND time_role='SUPPORT' AND values_json->>'scheduled_publication_date'='2026-08-11' AND values_json->>'scheduled_publication_in_target_window'='true') THEN RAISE EXCEPTION 'S02_JULY_SUPPORT_OR_PUBLICATION_DATE_LOST';END IF;
IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.source_observation WHERE family_id='S02' AND effective_date='2026-09-30' AND time_role='TARGET' AND values_json->>'scheduled_publication_date'='2026-10-09' AND values_json->>'scheduled_publication_in_target_window'='false' AND available_at IS NULL) THEN RAISE EXCEPTION 'S02_SEPTEMBER_30_FALSE_SEPTEMBER_PUBLICATION';END IF;
IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.source_observation WHERE family_id='S02' AND effective_date='2026-08-31' AND values_json->>'short_interest'='9007199254740993' AND (values_json->>'avg_daily_volume')::numeric=0.00000000000000000001) THEN RAISE EXCEPTION 'S02_NUMERIC_PRECISION_LOST';END IF;
IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.source_observation WHERE family_id='S02' AND values_json->>'ticker'='FIXTURE_BAD' AND field_state->>'settlement_date'='QUARANTINED' AND field_state->>'short_interest'='QUARANTINED') OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.source_observation WHERE family_id='S02' AND length(values_json->>'ticker')=9000 AND field_state->>'ticker'='QUARANTINED' AND length(unit_key)<128) THEN RAISE EXCEPTION 'S02_BAD_DATE_VALUE_OR_SYMBOL_NOT_QUARANTINED';END IF;
IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.source_observation WHERE family_id='S02' AND effective_date='2026-09-15' AND values_json->>'ticker'='FIXTURE_A' AND field_state->>'days_to_cover'='UNAVAILABLE' AND field_state->>'avg_daily_volume'='VALUE') THEN RAISE EXCEPTION 'S02_MISSING_DAYS_TO_COVER_INVENTED';END IF;
IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.source_observation WHERE family_id='S02' AND (available_at IS NOT NULL OR source_published_at IS NOT NULL OR historical_received_at IS NOT NULL OR event_at IS NOT NULL OR company_id IS NOT NULL OR security_id IS NOT NULL OR availability_policy<>'ARCHIVED_OBSERVATION')) THEN RAISE EXCEPTION 'S02_FALSE_IDENTITY_OR_INTRADAY_AVAILABILITY';END IF;
IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_partition p JOIN population_top3000_202608_202609_v1.pipeline_validation v ON v.validation_id=p.last_validation_id WHERE p.family_id='S02' AND p.denominator_state='PROVISIONAL' AND NOT p.publication_eligible AND v.result='DENOMINATOR_UNRESOLVED') OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE family_id='S02' AND stage='REPAIR' AND status='BLOCKED' AND blocker_code='S02_COMPANY_REPORT_DENOMINATOR_AND_PUBLICATION_TIME_REQUIRED') THEN RAISE EXCEPTION 'S02_FALSE_COVERAGE_OR_MISSING_EXPLICIT_REPAIR_HOLD';END IF;
IF (SELECT ctrl.stored_bytes-old_storage FROM population_top3000_202608_202609_v1.acquisition_control ctrl WHERE singleton)<>expected_stored_bytes OR (SELECT provider_requests-old_requests FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton)<>2 THEN RAISE EXCEPTION 'S02_STORAGE_OR_REQUEST_ACCOUNTING';END IF;
-- An HTTP 200 oversized/partial body is unknown even when the worker labels
-- it PARSER. Its reservation must remain available for expiry reconciliation.
failure_tid:=population_top3000_202608_202609_v1.pipeline_enqueue_v1('S02','ACQUIRE','short-interest-settlement-20260715-20260930','{"max_pages":1,"fixture_only":true}'::jsonb,vers||'{"oversized_body_fixture":true}'::jsonb);
a:=gen_random_uuid();token:=repeat('f',64);
UPDATE population_top3000_202608_202609_v1.acquisition_control SET next_provider_at=clock_timestamp() WHERE singleton;
UPDATE population_top3000_202608_202609_v1.pipeline_task SET status='LEASED',attempt_id=a,lease_until=clock_timestamp()+interval '60 seconds',owner_fence=2,stop_generation=0,executions=executions+1 WHERE task_id=failure_tid;
INSERT INTO population_top3000_202608_202609_v1.pipeline_attempt(attempt_id,task_id,execution_number,token_sha256,owner_fence,stop_generation,adapter_sha256)
SELECT a,failure_tid,t.executions,encode(sha256(convert_to(token,'UTF8')),'hex'),2,0,ad.function_sha256 FROM population_top3000_202608_202609_v1.pipeline_task t JOIN population_top3000_202608_202609_v1.pipeline_adapter ad ON ad.family_id=t.family_id AND ad.stage=t.stage WHERE t.task_id=failure_tid;
c:=population_top3000_202608_202609_v1.massive_shortinterest_claim_v1(a,token);
IF c->>'state' IS DISTINCT FROM 'RESERVED' THEN RAISE EXCEPTION 'S02_FAILURE_FIXTURE_RESERVATION_FAILED';END IF;
r:=population_top3000_202608_202609_v1.massive_shortinterest_failure_v1(a,token,jsonb_build_object('code','RESPONSE_TOO_LARGE','failure_class','PARSER','http_status',200,'received_bytes',8388608,'retry_after_seconds',0,'fetch_started',true,'wire_sha256',NULL));
IF r->>'state' IS DISTINCT FROM 'BLOCKED' OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_provider_lease WHERE attempt_id=a AND released_at IS NULL) OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=failure_tid AND status='BLOCKED' AND blocker_code='UNCERTAIN_EFFECT' AND lease_until IS NOT NULL) THEN RAISE EXCEPTION 'S02_PARTIAL_200_PARSER_EFFECT_WAS_RELEASED_OR_RETRIED';END IF;
IF (SELECT ctrl.stored_bytes-old_storage FROM population_top3000_202608_202609_v1.acquisition_control ctrl WHERE singleton)<>expected_stored_bytes OR (SELECT provider_requests-old_requests FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton)<>3 THEN RAISE EXCEPTION 'S02_UNKNOWN_BODY_ACCOUNTING_CHANGED';END IF;
SELECT encode(sha256(convert_to(coalesce(jsonb_agg(to_jsonb(u)),'[]'::jsonb)::text,'UTF8')),'hex') INTO universe_after FROM population_top3000_202608_202609_v1.pipeline_universe u;
IF universe_before IS DISTINCT FROM universe_after THEN RAISE EXCEPTION 'S02_CHANGED_COMPANY_UNIVERSE';END IF;
END $test$;
SELECT 'PASS_ROLLBACK_ONLY_S02_TWO_PAGE_REPORT_ARCHIVE_AND_SCHEDULE' result,clock_timestamp() observed_at,false provider_accuracy_verified,false live_provider_requested,
population_top3000_202608_202609_v1.massive_shortinterest_bundle_sha256_v1() shortinterest_bundle_sha256,
(SELECT count(*) FROM population_top3000_202608_202609_v1.massive_shortinterest_page_v1) source_pages,
(SELECT count(*) FROM population_top3000_202608_202609_v1.massive_shortinterest_record_v1) source_occurrences,
(SELECT count(*) FROM population_top3000_202608_202609_v1.source_observation WHERE family_id='S02') distinct_report_versions,
(SELECT jsonb_object_agg(stage,encode(sha256(convert_to(pg_get_functiondef(('population_top3000_202608_202609_v1.'||fn)::regprocedure),'UTF8')),'hex')) FROM (VALUES ('ACQUIRE','massive_shortinterest_acquire_v1(uuid,text)','PROVIDER'),('NORMALIZE','massive_shortinterest_normalize_v1(uuid,text)','DATABASE'),('DERIVE','massive_shortinterest_derive_v1(uuid,text)','DATABASE'),('VALIDATE','pipeline_validate_v1(uuid,text)','DATABASE'),('REPAIR','massive_shortinterest_repair_hold_v1(uuid,text)','DATABASE')) x(stage,fn,resource)) tested_function_sha256;
ROLLBACK;
