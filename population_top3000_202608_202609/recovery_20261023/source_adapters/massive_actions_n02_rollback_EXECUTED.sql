-- Root-only; current N03 functions and source work remain unchanged.
-- Synthetic two-stream fixture exercises N02's real fenced SQL; no Edge/provider requests.
BEGIN;
SET LOCAL lock_timeout='3s';
SET LOCAL statement_timeout='50s';
SELECT cron.alter_job(596,active:=false);
SELECT pg_advisory_xact_lock(2026083000261022);
DO $test$
DECLARE part text;tid uuid;a uuid;token text;vers jsonb;bundle text;c jsonb;receipt jsonb;body text;page jsonb;k text;i integer;j integer;
common_event jsonb;changed_event jsonb;ex_only jsonb;decl_only jsonb;optional_event jsonb;outside_event jsonb;reject_seen boolean;
old_storage bigint;old_requests bigint;expected_bytes bigint:=0;universe_before text;universe_after text;
BEGIN
IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton AND owner_id='top3000-recovery-20261023-root-4b1314bcb5c2' AND owner_fence=2 AND stop_generation=0 AND NOT stopped AND enabled AND hold_reason IS NULL AND admitted_until>clock_timestamp()) THEN RAISE EXCEPTION 'OWNER_STOP_OR_ADMISSION_CHANGED';END IF;
IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE status='LEASED') OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_provider_lease WHERE released_at IS NULL) THEN RAISE EXCEPTION 'EXISTING_LEASE_MUST_FINISH_BEFORE_TEST';END IF;
IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE family_id='N02') OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.source_observation WHERE family_id='N02') THEN RAISE EXCEPTION 'N02_ALREADY_STARTED_RECONCILE_BEFORE_FIXTURE_TEST';END IF;
IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton AND budget_evidence->>'provider_admissions_allowed'='true' AND budget_evidence->'allowed_providers' ? 'Massive') THEN RAISE EXCEPTION 'EXISTING_MASSIVE_ADMISSION_REQUIRED';END IF;
SELECT stored_bytes,provider_requests INTO old_storage,old_requests FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton;
SELECT encode(sha256(convert_to(coalesce(jsonb_agg(to_jsonb(u)),'[]'::jsonb)::text,'UTF8')),'hex') INTO universe_before FROM population_top3000_202608_202609_v1.pipeline_universe u;
UPDATE population_top3000_202608_202609_v1.pipeline_task SET not_before='infinity'::timestamptz WHERE status IN('READY','BLOCKED');
INSERT INTO population_top3000_202608_202609_v1.pipeline_family_binding(family_id,expected_relation,observed_relation,expected_definition_sha256,observed_definition_sha256,binding_version,requires_company_universe,expected_partition_count,partition_manifest_evidence,partition_manifest_frozen)
VALUES('N02','population_top3000_202608_202609_v1.expected_observation'::regclass,'population_top3000_202608_202609_v1.source_observation'::regclass,population_top3000_202608_202609_v1.pipeline_relation_hash_v1('population_top3000_202608_202609_v1.expected_observation'::regclass),population_top3000_202608_202609_v1.pipeline_relation_hash_v1('population_top3000_202608_202609_v1.source_observation'::regclass),
'n02-provisional-date-union-20261023',true,2,'{"partitions":["dividends-exdate-202608-202609","dividends-declaration-202608-202609"],"independent_dividend_denominator":false,"company_membership_verified":false,"deduplicated_union":true}',false)
ON CONFLICT(family_id) DO NOTHING;
INSERT INTO population_top3000_202608_202609_v1.pipeline_adapter(family_id,stage,function_oid,function_sha256,resource_class,provider,enabled,reviewed_test_evidence,producer_revision,expected_batch_seconds)
SELECT 'N02',stage,('population_top3000_202608_202609_v1.'||fn)::regprocedure::oid,encode(sha256(convert_to(pg_get_functiondef(('population_top3000_202608_202609_v1.'||fn)::regprocedure),'UTF8')),'hex'),resource,CASE WHEN resource='PROVIDER' THEN 'Massive' ELSE NULL END,true,
'{"test":"N02_SYNTHETIC_TWO_STREAM_UNION_ROLLBACK","status":"IN_PROGRESS_ROLLBACK_ONLY","provider_accuracy_verified":false}'::jsonb,'massive-dividends-20261023-v1',45
FROM (VALUES ('ACQUIRE','massive_dividends_acquire_v1(uuid,text)','PROVIDER'),
('NORMALIZE','massive_dividends_normalize_v1(uuid,text)','DATABASE'),
('DERIVE','massive_dividends_derive_v1(uuid,text)','DATABASE'),
('VALIDATE','pipeline_validate_v1(uuid,text)','DATABASE')) x(stage,fn,resource) ON CONFLICT(family_id,stage) DO NOTHING;
bundle:=population_top3000_202608_202609_v1.massive_dividends_bundle_sha256_v1();
vers:=jsonb_build_object('scope_sha256','8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd','archive_contract_revision','massive-dividends-20261023-v1','dividends_bundle_sha256',bundle,'fixture_only',true);
common_event:='{"id":"FIXTURE-common","ticker":"FIXTURE_A","cash_amount":0.2,"currency":"USD","dividend_type":"CD","frequency":4,"ex_dividend_date":"2026-08-20","declaration_date":"2026-08-05","pay_date":"2026-09-10","record_date":"2026-08-20"}'::jsonb;
changed_event:=jsonb_set(common_event,'{cash_amount}','0.3'::jsonb);
ex_only:=common_event||'{"id":"FIXTURE-ex-only","ticker":"FIXTURE_B","ex_dividend_date":"2026-08-10","declaration_date":"2026-07-15"}'::jsonb;
decl_only:=common_event||'{"id":"FIXTURE-decl-only","ticker":"FIXTURE_C","ex_dividend_date":"2026-10-10","declaration_date":"2026-09-05"}'::jsonb;
optional_event:='{"id":"FIXTURE-optional","ticker":"FIXTURE_D","declaration_date":"2026-08-06","cash_amount":0.00000000000000000001,"huge_integer":9007199254740993,"frequency":0}'::jsonb;
outside_event:=common_event||'{"id":"FIXTURE-outside","ticker":"FIXTURE_E","ex_dividend_date":"2026-10-10","declaration_date":"2026-10-01","cash_amount":-0.1}'::jsonb;
FOR i IN 1..2 LOOP
 part:=CASE WHEN i=1 THEN 'dividends-exdate-202608-202609' ELSE 'dividends-declaration-202608-202609' END;
 tid:=population_top3000_202608_202609_v1.pipeline_enqueue_v1('N02','ACQUIRE',part,'{"max_pages":1,"fixture_only":true}'::jsonb,vers);
 a:=gen_random_uuid();token:=repeat(CASE WHEN i=1 THEN 'a' ELSE 'b' END,64);
 UPDATE population_top3000_202608_202609_v1.acquisition_control SET next_provider_at=clock_timestamp() WHERE singleton;
 UPDATE population_top3000_202608_202609_v1.pipeline_task SET status='LEASED',attempt_id=a,lease_until=clock_timestamp()+interval '60 seconds',owner_fence=2,stop_generation=0,executions=executions+1 WHERE task_id=tid;
 INSERT INTO population_top3000_202608_202609_v1.pipeline_attempt(attempt_id,task_id,execution_number,token_sha256,owner_fence,stop_generation,adapter_sha256)
 SELECT a,tid,t.executions,encode(sha256(convert_to(token,'UTF8')),'hex'),2,0,ad.function_sha256 FROM population_top3000_202608_202609_v1.pipeline_task t JOIN population_top3000_202608_202609_v1.pipeline_adapter ad ON ad.family_id=t.family_id AND ad.stage=t.stage WHERE t.task_id=tid;
 IF i=1 THEN
  reject_seen:=false;
  BEGIN
   UPDATE population_top3000_202608_202609_v1.pipeline_task SET input_versions=jsonb_set(input_versions,'{dividends_bundle_sha256}',to_jsonb(repeat('0',64))) WHERE task_id=tid;
   PERFORM population_top3000_202608_202609_v1.massive_actions_route_claim_v2(a,token);
  EXCEPTION WHEN OTHERS THEN IF SQLERRM='MASSIVE_DIVIDEND_SCOPE_NOT_ADMITTED' THEN reject_seen:=true;ELSE RAISE;END IF;
  END;
  IF NOT reject_seen OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_provider_lease WHERE attempt_id=a) THEN RAISE EXCEPTION 'MUTATED_HELPER_BUNDLE_ADMITTED_PROVIDER_REQUEST';END IF;
 END IF;
 c:=population_top3000_202608_202609_v1.massive_actions_route_claim_v2(a,token);
 IF c->>'state' IS DISTINCT FROM 'RESERVED' OR c->>'family_id' IS DISTINCT FROM 'N02' OR c->>'partition_key' IS DISTINCT FROM part THEN RAISE EXCEPTION 'DIVIDEND_ROUTE_CLAIM_FAILED %',c;END IF;
 IF (i=1 AND c->>'request_url' NOT LIKE '%?ex_dividend_date.gte=2026-08-01&ex_dividend_date.lte=2026-09-30&order=asc&sort=ex_dividend_date&limit=1000') OR (i=2 AND c->>'request_url' NOT LIKE '%?declaration_date.gte=2026-08-01&declaration_date.lte=2026-09-30&order=asc&sort=ex_dividend_date&limit=1000') THEN RAISE EXCEPTION 'DATE_FILTER_PARTITION_MISMATCH';END IF;
 k:=c->>'archive_key';
 -- Deliberately omit optional status on the second response; explicit results remain required.
 body:=CASE WHEN i=1 THEN jsonb_build_object('status','OK','request_id','FIXTURE-ex','results',jsonb_build_array(common_event,ex_only,common_event))::text ELSE jsonb_build_object('request_id','FIXTURE-decl','results',jsonb_build_array(common_event,changed_event,decl_only,optional_event,outside_event))::text END;
 page:=jsonb_build_object('archive_key',k,'page_number',1,'request_url',c->>'request_url','next_url',NULL,'content_text',body,'source_sha256',encode(sha256(convert_to(body,'UTF8')),'hex'),'wire_sha256',encode(sha256(convert_to(body,'UTF8')),'hex'),'wire_bytes',octet_length(body),'http_status',200,'retrieved_at',clock_timestamp(),'result_count',CASE WHEN i=1 THEN 3 ELSE 5 END,'provider_request_id','FIXTURE-page'||i,'elapsed_ms',1);
 receipt:=population_top3000_202608_202609_v1.massive_actions_route_commit_v2(a,token,page);
 IF receipt->>'state' IS DISTINCT FROM 'COMPLETE' THEN RAISE EXCEPTION 'DIVIDEND_PAGE_COMMIT_FAILED %',receipt;END IF;
 expected_bytes:=expected_bytes+octet_length(body);
 UPDATE population_top3000_202608_202609_v1.pipeline_control SET enabled=true,next_wakeup_at=clock_timestamp() WHERE singleton;
 receipt:=population_top3000_202608_202609_v1.pipeline_tick_v1();
 IF receipt->>'state' IS DISTINCT FROM 'DISPATCHED' OR receipt#>>'{result,state}' IS DISTINCT FROM 'COMPLETE' OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=(receipt->>'task_id')::uuid AND family_id='N02' AND stage='NORMALIZE' AND partition_key=part) THEN RAISE EXCEPTION 'DIVIDEND_NORMALIZE_DISPATCH_FAILED %',receipt;END IF;
 IF i=1 THEN
  receipt:=population_top3000_202608_202609_v1.pipeline_tick_v1();
  IF receipt->>'state' IS DISTINCT FROM 'DISPATCHED' OR receipt#>>'{result,state}' IS DISTINCT FROM 'BLOCKED' OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=(receipt->>'task_id')::uuid AND family_id='N02' AND stage='DERIVE' AND blocker_code='DEPENDENCY_PENDING') THEN RAISE EXCEPTION 'DIVIDEND_UNION_DID_NOT_WAIT_FOR_SECOND_STREAM %',receipt;END IF;
  IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.source_observation WHERE family_id='N02') THEN RAISE EXCEPTION 'PREMATURE_PARTIAL_UNION_OBSERVATION';END IF;
 END IF;
END LOOP;
UPDATE population_top3000_202608_202609_v1.pipeline_task SET blocker_until=clock_timestamp(),not_before=clock_timestamp() WHERE family_id='N02' AND stage='DERIVE' AND status='BLOCKED' AND blocker_code='DEPENDENCY_PENDING';
FOR j IN 1..4 LOOP
 receipt:=population_top3000_202608_202609_v1.pipeline_tick_v1();
 IF j=1 AND NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=(receipt->>'task_id')::uuid AND stage='DERIVE' AND partition_key='dividends-declaration-202608-202609') THEN RAISE EXCEPTION 'FIXTURE_REQUIRES_DECLARATION_DERIVATION_FIRST';END IF;
 IF receipt->>'state' IS DISTINCT FROM 'DISPATCHED' OR receipt#>>'{result,state}' IS DISTINCT FROM 'COMPLETE' OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=(receipt->>'task_id')::uuid AND family_id='N02' AND stage IN('DERIVE','VALIDATE')) THEN RAISE EXCEPTION 'DIVIDEND_UNION_DERIVE_VALIDATE_FAILED tick=% receipt=%',j,receipt;END IF;
END LOOP;
IF (SELECT count(*) FROM population_top3000_202608_202609_v1.massive_dividend_record_v1)<>8 OR (SELECT count(*) FROM population_top3000_202608_202609_v1.source_observation WHERE family_id='N02')<>6 OR (SELECT count(*) FROM population_top3000_202608_202609_v1.expected_observation WHERE family_id='N02')<>6 THEN RAISE EXCEPTION 'DIVIDEND_UNION_SOURCE_OR_DEDUPLICATION_COUNT';END IF;
IF EXISTS(SELECT unit_key FROM population_top3000_202608_202609_v1.source_observation WHERE family_id='N02' GROUP BY unit_key HAVING count(*)<>1) THEN RAISE EXCEPTION 'DIVIDEND_UNION_DUPLICATED_ACROSS_PARTITIONS';END IF;
IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.source_observation WHERE family_id='N02' AND source_record_key='FIXTURE-common' AND values_json->>'cash_amount'='0.2' AND partition_key='dividends-exdate-202608-202609' AND lineage->>'source_occurrences'='3') THEN RAISE EXCEPTION 'DIVIDEND_DUPLICATE_SOURCE_PROVENANCE_LOST';END IF;
IF (SELECT count(*) FROM population_top3000_202608_202609_v1.source_observation WHERE family_id='N02' AND source_record_key='FIXTURE-common' AND field_state->>'provider_event_id'='CONFLICTING')<>2 THEN RAISE EXCEPTION 'DIVIDEND_SOURCE_VERSION_CONFLICT_LOST';END IF;
IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.source_observation WHERE family_id='N02' AND source_record_key='FIXTURE-decl-only' AND effective_date='2026-10-10' AND field_state->>'declaration_date'='VALUE' AND field_state->>'ex_dividend_date'='VALUE' AND available_at IS NULL) THEN RAISE EXCEPTION 'IN_WINDOW_DECLARATION_FUTURE_EXDATE_DROPPED_OR_FALSE_TIMESTAMP';END IF;
IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.source_observation WHERE family_id='N02' AND source_record_key='FIXTURE-optional' AND field_state->>'currency'='UNAVAILABLE' AND field_state->>'ex_dividend_date'='UNAVAILABLE' AND values_json#>>'{raw_event,huge_integer}'='9007199254740993' AND (values_json->>'cash_amount')::numeric=0.00000000000000000001) THEN RAISE EXCEPTION 'OPTIONAL_FIELD_INVENTED_OR_NUMERIC_PRECISION_LOST';END IF;
IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.source_observation WHERE family_id='N02' AND source_record_key='FIXTURE-outside' AND field_state->>'declaration_date'='QUARANTINED' AND field_state->>'cash_amount'='QUARANTINED') THEN RAISE EXCEPTION 'OUTSIDE_QUERY_OR_NEGATIVE_AMOUNT_FALSELY_VERIFIED';END IF;
IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.source_observation WHERE family_id='N02' AND (available_at IS NOT NULL OR source_published_at IS NOT NULL OR historical_received_at IS NOT NULL OR company_id IS NOT NULL OR security_id IS NOT NULL OR availability_policy<>'ARCHIVED_OBSERVATION')) THEN RAISE EXCEPTION 'FALSE_DIVIDEND_IDENTITY_OR_AVAILABILITY';END IF;
IF (SELECT count(*) FROM population_top3000_202608_202609_v1.pipeline_partition p JOIN population_top3000_202608_202609_v1.pipeline_validation v ON v.validation_id=p.last_validation_id WHERE p.family_id='N02' AND p.denominator_state='PROVISIONAL' AND NOT p.publication_eligible AND v.result='DENOMINATOR_UNRESOLVED')<>2 THEN RAISE EXCEPTION 'FALSE_DIVIDEND_COHORT_COMPLETENESS';END IF;
IF (SELECT stored_bytes-old_storage FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton)<>expected_bytes OR (SELECT provider_requests-old_requests FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton)<>2 THEN RAISE EXCEPTION 'DIVIDEND_SOURCE_STORAGE_OR_REQUEST_ACCOUNTING';END IF;
SELECT encode(sha256(convert_to(coalesce(jsonb_agg(to_jsonb(u)),'[]'::jsonb)::text,'UTF8')),'hex') INTO universe_after FROM population_top3000_202608_202609_v1.pipeline_universe u;
IF universe_before IS DISTINCT FROM universe_after THEN RAISE EXCEPTION 'DIVIDEND_LANE_CHANGED_COMPANY_UNIVERSE';END IF;
END $test$;
SELECT 'PASS_ROLLBACK_ONLY_N02_TWO_STREAM_UNION_PROVISIONAL_NORMALIZATION' result,clock_timestamp() observed_at,
false provider_accuracy_verified,false live_provider_requested,
population_top3000_202608_202609_v1.massive_dividends_bundle_sha256_v1() dividends_bundle_sha256,
(SELECT count(*) FROM population_top3000_202608_202609_v1.massive_dividend_page_v1) fixture_pages,
(SELECT count(*) FROM population_top3000_202608_202609_v1.massive_dividend_record_v1) source_occurrences,
(SELECT count(*) FROM population_top3000_202608_202609_v1.source_observation WHERE family_id='N02') deduplicated_event_versions,
(SELECT jsonb_object_agg(stage,encode(sha256(convert_to(pg_get_functiondef(('population_top3000_202608_202609_v1.'||fn)::regprocedure),'UTF8')),'hex')) FROM (VALUES ('ACQUIRE','massive_dividends_acquire_v1(uuid,text)','PROVIDER'),
('NORMALIZE','massive_dividends_normalize_v1(uuid,text)','DATABASE'),
('DERIVE','massive_dividends_derive_v1(uuid,text)','DATABASE'),
('VALIDATE','pipeline_validate_v1(uuid,text)','DATABASE')) x(stage,fn,resource)) tested_function_sha256;
ROLLBACK;
