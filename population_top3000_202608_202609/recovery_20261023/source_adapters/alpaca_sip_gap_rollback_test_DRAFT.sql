-- ROOT ONLY after definitions are installed. Synthetic fixtures; NO provider, HTTP or Edge calls.
-- All task, budget-admission, counter, source and compact-row changes roll back.
-- Existing work must be quiescent; the transaction holds the mission scheduler lock.
BEGIN;
SET LOCAL lock_timeout='3s';
SET LOCAL statement_timeout='50s';
SET LOCAL timezone='UTC';
SELECT pg_advisory_xact_lock(2026083000261022);
CREATE FUNCTION pg_temp.alpaca_fixture_lease_v1(p_task uuid,p_token text) RETURNS uuid
LANGUAGE plpgsql SET search_path TO 'pg_catalog' AS $lease$
DECLARE a uuid:=gen_random_uuid();
BEGIN
 UPDATE population_top3000_202608_202609_v1.pipeline_task SET status='LEASED',attempt_id=a,lease_until=clock_timestamp()+interval '60 seconds',owner_fence=2,stop_generation=0,executions=executions+1,not_before=clock_timestamp() WHERE task_id=p_task;
 INSERT INTO population_top3000_202608_202609_v1.pipeline_attempt(attempt_id,task_id,execution_number,token_sha256,owner_fence,stop_generation,adapter_sha256)
 SELECT a,p_task,t.executions,encode(sha256(convert_to(p_token,'UTF8')),'hex'),2,0,ad.function_sha256 FROM population_top3000_202608_202609_v1.pipeline_task t JOIN population_top3000_202608_202609_v1.pipeline_adapter ad ON ad.family_id=t.family_id AND ad.stage=t.stage WHERE t.task_id=p_task;
 RETURN a;
END $lease$;
DO $test$
DECLARE tid uuid;normal_tid uuid;failure_tid uuid;legacy_tid uuid;a uuid;token text;payload jsonb;spec jsonb;vers jsonb;specsha text;k text;part text;c jsonb;r jsonb;replay jsonb;page jsonb;body text;h text;i integer;
 rejected boolean;old_storage bigint;old_requests bigint;expected_storage bigint:=0;before_universe text;after_universe text;before_p02 text;after_p02 text;
BEGIN
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton AND owner_id='top3000-recovery-20261023-root-4b1314bcb5c2' AND owner_fence=2 AND stop_generation=0 AND NOT stopped AND enabled AND hold_reason IS NULL AND admitted_until>clock_timestamp()) THEN RAISE EXCEPTION 'OWNER_STOP_OR_ADMISSION_CHANGED';END IF;
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE status='LEASED') OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_provider_lease WHERE released_at IS NULL) THEN RAISE EXCEPTION 'EXISTING_LEASE_MUST_FINISH_BEFORE_TEST';END IF;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton AND enabled AND shared_scheduler_verified AND budget_evidence->>'provider_admissions_allowed'='true') THEN RAISE EXCEPTION 'EXISTING_BOUNDED_SOURCE_ADMISSION_REQUIRED';END IF;
 IF encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.price_retained_partition_v1(uuid,text)'::regprocedure),'UTF8')),'hex')<>'54ee6c5efb998ef35c9064a8e3767261f9498ab0546f3472675fd4cd3dceb586' THEN RAISE EXCEPTION 'RETAINED_PRICE_HANDLER_CHANGED';END IF;
 -- Reservation-path simulation only, isolated in this rolled-back transaction. No live Alpaca admission is granted by this test.
 UPDATE population_top3000_202608_202609_v1.pipeline_control SET budget_evidence=jsonb_set(budget_evidence,'{allowed_providers}',coalesce(budget_evidence->'allowed_providers','[]'::jsonb)||'"Alpaca"'::jsonb) WHERE singleton AND NOT coalesce(budget_evidence->'allowed_providers' ? 'Alpaca',false);
 SELECT stored_bytes,provider_requests INTO old_storage,old_requests FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton;
 SELECT encode(sha256(convert_to(coalesce(jsonb_agg(to_jsonb(u) ORDER BY to_jsonb(u)::text),'[]'::jsonb)::text,'UTF8')),'hex') INTO before_universe FROM population_top3000_202608_202609_v1.pipeline_universe u;
 SELECT encode(sha256(convert_to(coalesce(jsonb_agg(to_jsonb(ad) ORDER BY stage),'[]'::jsonb)::text,'UTF8')),'hex') INTO before_p02 FROM population_top3000_202608_202609_v1.pipeline_adapter ad WHERE family_id='P02';
 UPDATE population_top3000_202608_202609_v1.pipeline_task SET not_before='infinity'::timestamptz,blocker_until=NULL WHERE status IN('READY','BLOCKED');
 INSERT INTO population_top3000_202608_202609_v1.pipeline_adapter(family_id,stage,function_oid,function_sha256,resource_class,provider,enabled,reviewed_test_evidence,producer_revision,expected_batch_seconds)
 SELECT 'P01',stage,fn::regprocedure::oid,encode(sha256(convert_to(pg_get_functiondef(fn::regprocedure),'UTF8')),'hex'),resource,CASE WHEN resource='PROVIDER' THEN 'Alpaca' ELSE NULL END,true,'{"test":"ALPACA_SYNTHETIC_TWO_PAGE_ROLLBACK","status":"IN_PROGRESS_ROLLBACK_ONLY","provider_accuracy_verified":false}'::jsonb,'alpaca-sip-gap-20261023-v1',45
 FROM (VALUES ('ACQUIRE','population_top3000_202608_202609_v1.price_pipeline_acquire_v2(uuid,text)','PROVIDER'),('NORMALIZE','population_top3000_202608_202609_v1.price_pipeline_normalize_v2(uuid,text)','DATABASE'),('DERIVE','population_top3000_202608_202609_v1.alpaca_sip_derive_v1(uuid,text)','DATABASE'),('VALIDATE','population_top3000_202608_202609_v1.alpaca_sip_quality_hold_v1(uuid,text)','DATABASE')) v(stage,fn,resource)
 ON CONFLICT(family_id,stage) DO UPDATE SET function_oid=EXCLUDED.function_oid,function_sha256=EXCLUDED.function_sha256,resource_class=EXCLUDED.resource_class,provider=EXCLUDED.provider,enabled=EXCLUDED.enabled,reviewed_test_evidence=EXCLUDED.reviewed_test_evidence,producer_revision=EXCLUDED.producer_revision,expected_batch_seconds=EXCLUDED.expected_batch_seconds;
 payload:='{"kind":"ALPACA_SIP_GAP_BATCH_V1","symbols":["AAPL","NVDA"],"start_ny_date":"2026-09-08","end_ny_date_exclusive":"2026-09-09","calendar_scope_id":"corrected_company_scope_8e6ca1178f03822c","max_pages":1,"fixture_only":true}'::jsonb;
 spec:=population_top3000_202608_202609_v1.alpaca_sip_spec_v1(payload);specsha:=encode(sha256(convert_to(spec::text,'UTF8')),'hex');part:='alpaca-sip-gap:'||specsha;
 vers:=jsonb_build_object('scope_sha256','8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd','archive_contract_revision','alpaca-sip-gap-20261023-v1','alpaca_bundle_sha256',population_top3000_202608_202609_v1.alpaca_sip_code_hash_v1(),'request_spec_sha256',specsha,'fixture_only',true,'fixture_nonce',gen_random_uuid());
 tid:=population_top3000_202608_202609_v1.pipeline_enqueue_v1('P01','ACQUIRE',part,payload,vers);
 c:=population_top3000_202608_202609_v1.alpaca_sip_context_v1(tid);k:=c->>'archive_key';
 IF c->>'request_url'<>'https://data.alpaca.markets/v2/stocks/bars?symbols=AAPL%2CNVDA&timeframe=1Min&start=2026-09-08T04%3A00%3A00Z&end=2026-09-09T03%3A59%3A59.999999Z&limit=10000&adjustment=raw&feed=sip&sort=asc&asof=-&currency=USD' THEN RAISE EXCEPTION 'SQL_URL_ENCODER_OR_SCOPE_MISMATCH %',c;END IF;
 IF population_top3000_202608_202609_v1.alpaca_sip_timestamp_v1('2026-09-09T03:59:59.999999999Z') IS DISTINCT FROM timestamptz '2026-09-09 03:59:59.999999+00'
 OR population_top3000_202608_202609_v1.alpaca_sip_timestamp_v1('2026-09-08T03:59:59.9999999Z') IS DISTINCT FROM timestamptz '2026-09-08 03:59:59.999999+00'
 OR population_top3000_202608_202609_v1.alpaca_sip_timestamp_v1('2026-09-08T09:30:00-04:00') IS DISTINCT FROM timestamptz '2026-09-08 13:30:00+00'
 OR population_top3000_202608_202609_v1.alpaca_sip_timestamp_v1('2026-09-08T13:30:00') IS NOT NULL THEN RAISE EXCEPTION 'TIMESTAMP_SCOPE_PRECISION_OR_TIMEZONE_FAILURE';END IF;
 FOR i IN 1..2 LOOP
  token:=repeat(CASE WHEN i=1 THEN 'a' ELSE 'b' END,64);a:=pg_temp.alpaca_fixture_lease_v1(tid,token);
  UPDATE population_top3000_202608_202609_v1.acquisition_control SET next_provider_at=clock_timestamp() WHERE singleton;
  IF i=1 THEN
   rejected:=false;BEGIN PERFORM population_top3000_202608_202609_v1.alpaca_sip_claim_v1(a,repeat('0',64));EXCEPTION WHEN OTHERS THEN IF SQLERRM='ATTEMPT_AUTH_FAILED' THEN rejected:=true;ELSE RAISE;END IF;END;
   IF NOT rejected THEN RAISE EXCEPTION 'INVALID_TOKEN_ACCEPTED';END IF;
  END IF;
  c:=population_top3000_202608_202609_v1.alpaca_sip_claim_v1(a,token);IF c->>'state'<>'RESERVED' OR (c->>'page_number')::integer<>i THEN RAISE EXCEPTION 'CLAIM_FAILED %',c;END IF;
  rejected:=false;BEGIN PERFORM population_top3000_202608_202609_v1.alpaca_sip_claim_v1(a,token);EXCEPTION WHEN OTHERS THEN IF SQLERRM='REQUEST_ALREADY_RESERVED_FOR_ATTEMPT' THEN rejected:=true;ELSE RAISE;END IF;END;
  IF NOT rejected THEN RAISE EXCEPTION 'DUPLICATE_PROVIDER_REQUEST_ACCEPTED';END IF;
  IF i=2 AND (c->>'request_token' IS DISTINCT FROM 'Ab+/=' OR right(c->>'request_url',length('&page_token=Ab%2B%2F%3D')) IS DISTINCT FROM '&page_token=Ab%2B%2F%3D') THEN RAISE EXCEPTION 'OPAQUE_CURSOR_OR_SQL_ENCODING_FAILED';END IF;
  body:=CASE WHEN i=1 THEN $page1${"bars":{"AAPL":[{"t":"2026-09-08T13:29:00Z","o":100.12345678901234567890,"h":101,"l":99,"c":100,"v":10.125,"n":9007199254740993,"vw":100.000000000000000001},{"t":"2026-09-08T13:30:00Z","o":100,"h":101,"l":99,"c":100,"v":10,"n":2,"vw":100}]},"next_page_token":"Ab+/="}$page1$
  ELSE $page2${"bars":{"AAPL":[{"t":"2026-09-08T20:00:00Z","o":100,"h":101,"l":99,"c":100,"v":10,"n":2,"vw":100},{"t":"2026-09-09T00:00:00Z","o":100,"h":101,"l":99,"c":100,"v":10,"n":2,"vw":100},{"t":"2026-09-09T04:00:00Z","o":100,"h":101,"l":99,"c":100,"v":10,"n":2,"vw":100}],"NVDA":[{"t":"2026-09-08T08:00:00Z","o":100,"h":101,"l":99,"c":100,"v":0,"n":0,"vw":100}]},"next_page_token":null}$page2$ END;
  h:=encode(sha256(convert_to(body,'UTF8')),'hex');
  page:=jsonb_build_object('archive_key',k,'page_number',i,'request_url',c->>'request_url','next_page_token',CASE WHEN i=1 THEN 'Ab+/=' ELSE NULL END,'content_text',body,'source_sha256',h,'wire_sha256',h,'wire_bytes',octet_length(body),'http_status',200,'retrieved_at',clock_timestamp(),'result_count',CASE WHEN i=1 THEN 2 ELSE 4 END,'provider_request_id','SYNTHETIC_FIXTURE_'||i,'elapsed_ms',1);
  IF i=1 THEN
   rejected:=false;BEGIN PERFORM population_top3000_202608_202609_v1.alpaca_sip_commit_v1(a,token,jsonb_set(page,'{source_sha256}',to_jsonb(repeat('0',64))));EXCEPTION WHEN OTHERS THEN IF SQLERRM='ALPACA_SIP_SOURCE_HASH_MISMATCH' THEN rejected:=true;ELSE RAISE;END IF;END;
   IF NOT rejected OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.alpaca_sip_source_page_v1 WHERE archive_key=k) THEN RAISE EXCEPTION 'FORGED_SOURCE_HASH_ACCEPTED_OR_PARTIALLY_SAVED';END IF;
  END IF;
  r:=population_top3000_202608_202609_v1.alpaca_sip_commit_v1(a,token,page);IF r->>'state' IS DISTINCT FROM (CASE WHEN i=1 THEN 'CONTINUE' ELSE 'COMPLETE' END) THEN RAISE EXCEPTION 'PAGE_COMMIT_HANDOFF_FAILED %',r;END IF;
  expected_storage:=expected_storage+octet_length(body);
  IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_provider_lease WHERE attempt_id=a AND (released_at IS NULL OR received_bytes<>octet_length(body))) THEN RAISE EXCEPTION 'RETAINED_BYTES_OR_LEASE_RELEASE_MISMATCH';END IF;
  IF i=1 THEN
   c:=population_top3000_202608_202609_v1.alpaca_sip_context_v1(tid);
   IF c->>'state'<>'PAGE_LIMIT_REVIEW' OR c->>'page_number'<>'2' OR c->>'request_token'<>'Ab+/=' THEN RAISE EXCEPTION 'PAGE_CAP_TRUNCATED_OR_LOST_SHORT_PAGE_CONTINUATION';END IF;
  UPDATE population_top3000_202608_202609_v1.pipeline_task t SET payload=jsonb_set(t.payload,'{max_pages}','2'::jsonb) WHERE t.task_id=tid;
  END IF;
 END LOOP;
 IF (SELECT stored_bytes-old_storage FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton)<>expected_storage OR (SELECT provider_requests-old_requests FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton)<>2 THEN RAISE EXCEPTION 'SOURCE_STORAGE_OR_ONE_REQUEST_PER_PAGE_ACCOUNTING';END IF;
 IF (SELECT content_text::jsonb#>>'{bars,AAPL,0,n}' FROM population_top3000_202608_202609_v1.source_document WHERE source_id=k||':page:1')<>'9007199254740993' THEN RAISE EXCEPTION 'EXACT_SOURCE_INTEGER_LOST';END IF;
 SELECT task_id INTO normal_tid FROM population_top3000_202608_202609_v1.pipeline_task WHERE family_id='P01' AND stage='NORMALIZE' AND input_versions=vers;
 IF normal_tid IS NULL THEN RAISE EXCEPTION 'NORMALIZE_SUCCESSOR_MISSING';END IF;
 token:=repeat('e',64);a:=pg_temp.alpaca_fixture_lease_v1(normal_tid,token);
 r:=population_top3000_202608_202609_v1.price_pipeline_normalize_v2(a,token);
 IF r->>'state'<>'CONTINUE' OR r#>>'{commit_receipt,new_committed_units}'<>'2' THEN RAISE EXCEPTION 'FIRST_NORMALIZE_FAILED %',r;END IF;
 replay:=population_top3000_202608_202609_v1.price_pipeline_normalize_v2(a,token);
 IF replay->>'state'<>'CONTINUE' OR replay#>>'{commit_receipt,new_committed_units}'<>'0' OR replay#>>'{commit_receipt,output_sha256}' IS DISTINCT FROM r#>>'{commit_receipt,output_sha256}' THEN RAISE EXCEPTION 'FULL_REPLAY_NOT_IDEMPOTENT';END IF;
 rejected:=false;
 BEGIN
  UPDATE population_top3000_202608_202609_v1.alpaca_sip_compact_bar_v1 SET open=999,quality_bits=quality_bits+16 WHERE page_id=(SELECT page_id FROM population_top3000_202608_202609_v1.alpaca_sip_source_page_v1 WHERE archive_key=k AND page_number=1) AND source_row=1;
  PERFORM population_top3000_202608_202609_v1.price_pipeline_normalize_v2(a,token);RAISE EXCEPTION 'TAMPERED_NUMERIC_VALUES_ACCEPTED';
 EXCEPTION WHEN OTHERS THEN IF SQLERRM='ALPACA_SIP_COMPACT_FULL_REPLAY_MISMATCH' THEN rejected:=true;ELSE RAISE;END IF;END;
 IF NOT rejected THEN RAISE EXCEPTION 'VALUE_STATE_HASH_REPLAY_CHECK_NOT_EXERCISED';END IF;
 FOR i IN 1..2 LOOP
  rejected:=false;
  BEGIN
   UPDATE population_top3000_202608_202609_v1.source_document SET content_bytes=content_bytes+CASE WHEN i=1 THEN 1 ELSE 0 END,source_uri=source_uri||CASE WHEN i=2 THEN '&tampered=1' ELSE '' END WHERE source_id=k||':page:1';
   PERFORM population_top3000_202608_202609_v1.price_pipeline_normalize_v2(a,token);RAISE EXCEPTION 'TAMPERED_SOURCE_METADATA_ACCEPTED';
  EXCEPTION WHEN OTHERS THEN IF SQLERRM='ALPACA_SIP_RETAINED_SOURCE_INTEGRITY' THEN rejected:=true;ELSE RAISE;END IF;END;
  IF NOT rejected THEN RAISE EXCEPTION 'SOURCE_BYTES_OR_URI_CHECK_NOT_EXERCISED';END IF;
 END LOOP;
 rejected:=false;
 BEGIN
  UPDATE population_top3000_202608_202609_v1.pipeline_control SET budget_evidence=jsonb_set(budget_evidence,'{resource_limits,max_relation_bytes}','1'::jsonb) WHERE singleton;
  PERFORM population_top3000_202608_202609_v1.price_pipeline_normalize_v2(a,token);RAISE EXCEPTION 'RESOURCE_BOUND_IGNORED';
 EXCEPTION WHEN OTHERS THEN IF SQLERRM='ALPACA_SIP_POST_BATCH_RELATION_CAP_EXCEEDED' THEN rejected:=true;ELSE RAISE;END IF;END;
 IF NOT rejected THEN RAISE EXCEPTION 'NORMALIZATION_RESOURCE_BOUND_NOT_EXERCISED';END IF;
 PERFORM population_top3000_202608_202609_v1.pipeline_finish_v1(a,token,r);
 UPDATE population_top3000_202608_202609_v1.pipeline_control SET next_wakeup_at=clock_timestamp() WHERE singleton;
 FOR i IN 1..3 LOOP
  r:=population_top3000_202608_202609_v1.pipeline_tick_v1();
  IF r->>'state' IS DISTINCT FROM 'DISPATCHED' OR r#>>'{result,state}' IS DISTINCT FROM (CASE WHEN i=3 THEN 'BLOCKED' ELSE 'COMPLETE' END) THEN RAISE EXCEPTION 'NORMALIZE_DERIVE_QUALITY_HOLD_DISPATCH_FAILED tick=% receipt=%',i,r;END IF;
  IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=(r->>'task_id')::uuid AND input_versions=vers) THEN RAISE EXCEPTION 'UNRELATED_TASK_DISPATCHED';END IF;
 END LOOP;
 IF (SELECT count(*) FROM population_top3000_202608_202609_v1.alpaca_sip_compact_bar_v1 b JOIN population_top3000_202608_202609_v1.alpaca_sip_source_page_v1 p USING(page_id) WHERE p.archive_key=k)<>6 THEN RAISE EXCEPTION 'DROPPED_OR_DUPLICATED_COMPACT_SOURCE_ROWS';END IF;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.alpaca_sip_compact_bar_v1 b JOIN population_top3000_202608_202609_v1.alpaca_sip_source_page_v1 p USING(page_id) WHERE p.archive_key=k AND b.source_symbol='AAPL' AND b.bar_ts=timestamptz '2026-09-08 13:29:00+00' AND b.open=100.12345678901234567890 AND b.volume=10.125 AND b.trade_count=9007199254740993 AND b.vwap=100.000000000000000001 AND b.session_code=1 AND b.quality_bits=0)
 OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.alpaca_sip_compact_bar_v1 b JOIN population_top3000_202608_202609_v1.alpaca_sip_source_page_v1 p USING(page_id) WHERE p.archive_key=k AND b.bar_ts=timestamptz '2026-09-08 13:30:00+00' AND b.session_code=2)
 OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.alpaca_sip_compact_bar_v1 b JOIN population_top3000_202608_202609_v1.alpaca_sip_source_page_v1 p USING(page_id) WHERE p.archive_key=k AND b.bar_ts=timestamptz '2026-09-08 20:00:00+00' AND b.session_code=3)
 OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.alpaca_sip_compact_bar_v1 b JOIN population_top3000_202608_202609_v1.alpaca_sip_source_page_v1 p USING(page_id) WHERE p.archive_key=k AND b.bar_ts=timestamptz '2026-09-09 00:00:00+00' AND b.session_code=4 AND (b.quality_bits&64)=64)
 OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.alpaca_sip_compact_bar_v1 b JOIN population_top3000_202608_202609_v1.alpaca_sip_source_page_v1 p USING(page_id) WHERE p.archive_key=k AND b.bar_ts=timestamptz '2026-09-09 04:00:00+00' AND (b.quality_bits&4)=4)
 OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.alpaca_sip_compact_bar_v1 b JOIN population_top3000_202608_202609_v1.alpaca_sip_source_page_v1 p USING(page_id) WHERE p.archive_key=k AND b.source_symbol='NVDA' AND b.volume=0 AND b.trade_count=0 AND b.quality_bits=0)
 THEN RAISE EXCEPTION 'EXACT_NUMERIC_SESSION_ZERO_OR_SCOPE_STATE_FAILURE';END IF;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE input_versions=vers AND stage='VALIDATE' AND status='BLOCKED' AND blocker_code='ALPACA_SIP_COMPACT_POPULATION_REQUIRES_IDENTITY_AND_EXPECTATIONS') THEN RAISE EXCEPTION 'UNRESOLVED_IDENTITY_FALSELY_CERTIFIED';END IF;
 -- The existing retained NORMALIZE route must still return its original, precise hold.
 legacy_tid:=population_top3000_202608_202609_v1.pipeline_enqueue_v1('P01','NORMALIZE','rollback-retained-wrapper-fixture','{"kind":"RETAINED_ALPACA_AUGUST_SESSION"}'::jsonb,vers||'{"legacy_wrapper_fixture":true}'::jsonb);
 token:=repeat('f',64);a:=pg_temp.alpaca_fixture_lease_v1(legacy_tid,token);
 r:=population_top3000_202608_202609_v1.price_pipeline_normalize_v2(a,token);replay:=population_top3000_202608_202609_v1.price_retained_partition_v1(a,token);
 IF r IS DISTINCT FROM replay OR r->>'blocker_code'<>'RETAINED_PRICE_MANIFEST_REQUIRED' THEN RAISE EXCEPTION 'RETAINED_PRICE_ROUTE_CHANGED';END IF;
 PERFORM population_top3000_202608_202609_v1.pipeline_finish_v1(a,token,r);
 -- Definitive HTTP failure releases zero retained bytes; unknown transport effect keeps its reservation for controller reconciliation.
 FOR i IN 1..2 LOOP
  failure_tid:=population_top3000_202608_202609_v1.pipeline_enqueue_v1('P01','ACQUIRE',part,payload,vers||jsonb_build_object('failure_fixture',i));
  token:=repeat(CASE WHEN i=1 THEN 'c' ELSE 'd' END,64);a:=pg_temp.alpaca_fixture_lease_v1(failure_tid,token);
  UPDATE population_top3000_202608_202609_v1.acquisition_control SET next_provider_at=clock_timestamp() WHERE singleton;
  c:=population_top3000_202608_202609_v1.alpaca_sip_claim_v1(a,token);IF c->>'state'<>'RESERVED' THEN RAISE EXCEPTION 'FAILURE_FIXTURE_RESERVATION_FAILED';END IF;
  r:=population_top3000_202608_202609_v1.alpaca_sip_failure_v1(a,token,jsonb_build_object('code',CASE WHEN i=1 THEN 'HTTP_403' ELSE 'FETCH_FAILED' END,'failure_class',CASE WHEN i=1 THEN 'AUTHENTICATION' ELSE 'TRANSPORT_READ' END,'http_status',CASE WHEN i=1 THEN 403 ELSE NULL END,'received_bytes',123,'retry_after_seconds',60,'fetch_started',true,'wire_sha256',NULL));
  IF r->>'state'<>'BLOCKED' OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=failure_tid AND status='BLOCKED') THEN RAISE EXCEPTION 'SOURCE_FAILURE_AUTO_RETRY_ADMITTED';END IF;
  IF i=1 AND NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_provider_lease WHERE attempt_id=a AND released_at IS NOT NULL AND received_bytes=0) THEN RAISE EXCEPTION 'UNRETAINED_ERROR_BYTES_COUNTED_AS_STORAGE';END IF;
  IF i=2 AND (NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_provider_lease WHERE attempt_id=a AND released_at IS NULL) OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=failure_tid AND blocker_code='UNCERTAIN_EFFECT' AND lease_until IS NOT NULL)) THEN RAISE EXCEPTION 'UNKNOWN_EFFECT_LEASE_OR_DEADLINE_DISCARDED';END IF;
 END LOOP;
 IF (SELECT stored_bytes-old_storage FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton)<>expected_storage THEN RAISE EXCEPTION 'FAILED_PAYLOAD_COUNTED_AS_RETAINED_STORAGE';END IF;
 SELECT encode(sha256(convert_to(coalesce(jsonb_agg(to_jsonb(u) ORDER BY to_jsonb(u)::text),'[]'::jsonb)::text,'UTF8')),'hex') INTO after_universe FROM population_top3000_202608_202609_v1.pipeline_universe u;
 SELECT encode(sha256(convert_to(coalesce(jsonb_agg(to_jsonb(ad) ORDER BY stage),'[]'::jsonb)::text,'UTF8')),'hex') INTO after_p02 FROM population_top3000_202608_202609_v1.pipeline_adapter ad WHERE family_id='P02';
 IF before_universe IS DISTINCT FROM after_universe OR before_p02 IS DISTINCT FROM after_p02 THEN RAISE EXCEPTION 'EXISTING_UNIVERSE_OR_P02_BINDING_CHANGED';END IF;
END $test$;
SELECT 'PASS_ROLLBACK_ONLY_ALPACA_TWO_PAGE_COMPACT_EXACT_NUMERIC_SCOPE_REPLAY_AND_COEXISTENCE' result,clock_timestamp() observed_at,false provider_accuracy_verified,false live_provider_requested,
(SELECT jsonb_object_agg(fn,encode(sha256(convert_to(pg_get_functiondef(('population_top3000_202608_202609_v1.'||fn)::regprocedure),'UTF8')),'hex')) FROM (VALUES ('alpaca_sip_urlencode_v1(text)'),('alpaca_sip_spec_v1(jsonb)'),('alpaca_sip_context_v1(uuid)'),('alpaca_sip_acquire_v1(uuid,text)'),('alpaca_sip_claim_v1(uuid,text)'),('alpaca_sip_commit_v1(uuid,text,jsonb)'),('alpaca_sip_failure_v1(uuid,text,jsonb)'),('alpaca_sip_timestamp_v1(text)'),('alpaca_sip_number_v1(jsonb)'),('alpaca_sip_parse_page_v1(text,jsonb)'),('alpaca_sip_resource_guard_v1()'),('alpaca_sip_normalize_v1(uuid,text)'),('alpaca_sip_derive_v1(uuid,text)'),('alpaca_sip_quality_hold_v1(uuid,text)'),('price_pipeline_acquire_v2(uuid,text)'),('price_pipeline_normalize_v2(uuid,text)'),('alpaca_sip_code_hash_v1()'),('price_retained_partition_v1(uuid,text)')) h(fn)) tested_function_sha256;
ROLLBACK;
