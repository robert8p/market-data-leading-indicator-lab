-- Root-only after installing the schema draft. NO net/http/Edge calls occur here.
-- Synthetic fixture transport tests real fenced SQL, reservations, normalization and dispatcher handoff.
-- This is NOT a provider accuracy check. Every fixture, task and counter mutation rolls back.
BEGIN;
SET LOCAL lock_timeout='3s';
SET LOCAL statement_timeout='50s';
SELECT pg_advisory_xact_lock(2026083000261022);
DO $test$
DECLARE tid uuid;a uuid;token text;vers jsonb;k text;c jsonb;r jsonb;page jsonb;body text;wire_body text;h text;i integer;old_storage bigint;old_requests bigint;
rejected boolean;fwd jsonb;missing jsonb;outside jsonb;altered jsonb;expected_storage bigint:=0;before_universe text;after_universe text;failure_tid uuid;
BEGIN
IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton AND owner_id='top3000-recovery-20261023-root-4b1314bcb5c2' AND owner_fence=2 AND stop_generation=0 AND NOT stopped AND enabled AND hold_reason IS NULL AND admitted_until>clock_timestamp()) THEN RAISE EXCEPTION 'OWNER_STOP_OR_ADMISSION_CHANGED';END IF;
IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE status='LEASED') OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_provider_lease WHERE released_at IS NULL) THEN RAISE EXCEPTION 'EXISTING_LEASE_MUST_FINISH_BEFORE_TEST';END IF;
IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE family_id='N03') OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.source_observation WHERE family_id='N03') THEN RAISE EXCEPTION 'N03_ALREADY_STARTED_RECONCILE_BEFORE_FIXTURE_TEST';END IF;
IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton AND budget_evidence->>'provider_admissions_allowed'='true' AND budget_evidence->'allowed_providers' ? 'Massive') THEN RAISE EXCEPTION 'EXISTING_MASSIVE_ADMISSION_REQUIRED';END IF;
SELECT stored_bytes,provider_requests INTO old_storage,old_requests FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton;
SELECT encode(sha256(convert_to(coalesce(jsonb_agg(to_jsonb(u)),'[]'::jsonb)::text,'UTF8')),'hex') INTO before_universe FROM population_top3000_202608_202609_v1.pipeline_universe u;
UPDATE population_top3000_202608_202609_v1.pipeline_task SET not_before='infinity'::timestamptz WHERE status IN('READY','BLOCKED');
INSERT INTO population_top3000_202608_202609_v1.pipeline_family_binding(family_id,expected_relation,observed_relation,expected_definition_sha256,observed_definition_sha256,binding_version,requires_company_universe,expected_partition_count,partition_manifest_evidence,partition_manifest_frozen)
VALUES('N03','population_top3000_202608_202609_v1.expected_observation'::regclass,'population_top3000_202608_202609_v1.source_observation'::regclass,
population_top3000_202608_202609_v1.pipeline_relation_hash_v1('population_top3000_202608_202609_v1.expected_observation'::regclass),population_top3000_202608_202609_v1.pipeline_relation_hash_v1('population_top3000_202608_202609_v1.source_observation'::regclass),
'n03-provisional-source-archive-20261023',true,1,'{"partitions":["splits-execution-202608-202609"],"independent_action_denominator":false,"company_membership_verified":false}',false)
ON CONFLICT(family_id) DO NOTHING;
INSERT INTO population_top3000_202608_202609_v1.pipeline_adapter(family_id,stage,function_oid,function_sha256,resource_class,provider,enabled,reviewed_test_evidence,producer_revision,expected_batch_seconds)
SELECT 'N03',stage,fn::regprocedure::oid,encode(sha256(convert_to(pg_get_functiondef(fn::regprocedure),'UTF8')),'hex'),resource,
CASE WHEN resource='PROVIDER' THEN 'Massive' ELSE NULL END,true,'{"test":"N03_SYNTHETIC_TWO_PAGE_ROLLBACK","status":"IN_PROGRESS_ROLLBACK_ONLY","provider_accuracy_verified":false}'::jsonb,'massive-actions-20261023-v1',45
FROM (VALUES ('ACQUIRE','population_top3000_202608_202609_v1.massive_actions_acquire_v1(uuid,text)','PROVIDER'),
('NORMALIZE','population_top3000_202608_202609_v1.massive_actions_normalize_v1(uuid,text)','DATABASE'),
('DERIVE','population_top3000_202608_202609_v1.massive_actions_derive_v1(uuid,text)','DATABASE'),
('VALIDATE','population_top3000_202608_202609_v1.pipeline_validate_v1(uuid,text)','DATABASE'))v(stage,fn,resource) ON CONFLICT(family_id,stage) DO NOTHING;
vers:=jsonb_build_object('scope_sha256','8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd','archive_contract_revision','massive-actions-20261023-v1','fixture_only',true);
tid:=population_top3000_202608_202609_v1.pipeline_enqueue_v1('N03','ACQUIRE','splits-execution-202608-202609','{"max_pages":1,"fixture_only":true}'::jsonb,vers);
k:=population_top3000_202608_202609_v1.massive_archive_key_v1('N03','splits-execution-202608-202609',vers);
fwd:='{"id":"FIXTURE-forward","ticker":"FIXTURE_A","execution_date":"2026-08-03","split_from":1,"split_to":2,"adjustment_type":"forward_split","historical_adjustment_factor":0.1}'::jsonb;
missing:='{"id":"FIXTURE-optional","ticker":"FIXTURE_B","huge_integer":9007199254740993}'::jsonb;
altered:=jsonb_set(fwd,'{split_to}','3'::jsonb);
outside:='{"id":"FIXTURE-outside","ticker":"FIXTURE_C","execution_date":"2026-10-01","split_from":0,"split_to":1,"adjustment_type":"reverse_split"}'::jsonb;
FOR i IN 1..2 LOOP
 a:=gen_random_uuid();token:=repeat(CASE WHEN i=1 THEN 'a' ELSE 'b' END,64);
 UPDATE population_top3000_202608_202609_v1.acquisition_control SET next_provider_at=clock_timestamp() WHERE singleton;
 UPDATE population_top3000_202608_202609_v1.pipeline_task SET status='LEASED',attempt_id=a,lease_until=clock_timestamp()+interval '60 seconds',owner_fence=2,stop_generation=0,executions=executions+1,not_before=clock_timestamp() WHERE task_id=tid;
 INSERT INTO population_top3000_202608_202609_v1.pipeline_attempt(attempt_id,task_id,execution_number,token_sha256,owner_fence,stop_generation,adapter_sha256)
 SELECT a,tid,t.executions,encode(sha256(convert_to(token,'UTF8')),'hex'),2,0,ad.function_sha256 FROM population_top3000_202608_202609_v1.pipeline_task t JOIN population_top3000_202608_202609_v1.pipeline_adapter ad ON ad.family_id=t.family_id AND ad.stage=t.stage WHERE t.task_id=tid;
 IF i=1 THEN
  rejected:=false;BEGIN PERFORM population_top3000_202608_202609_v1.massive_actions_claim_v1(a,repeat('0',64));EXCEPTION WHEN OTHERS THEN IF SQLERRM='ATTEMPT_AUTH_FAILED' THEN rejected:=true;ELSE RAISE;END IF;END;
  IF NOT rejected THEN RAISE EXCEPTION 'INVALID_TOKEN_ACCEPTED';END IF;
 END IF;
 c:=population_top3000_202608_202609_v1.massive_actions_claim_v1(a,token);
 IF c->>'state' IS DISTINCT FROM 'RESERVED' OR (c->>'page_number')::integer<>i THEN RAISE EXCEPTION 'CLAIM_FAILED page=% receipt=%',i,c;END IF;
 rejected:=false;BEGIN PERFORM population_top3000_202608_202609_v1.massive_actions_claim_v1(a,token);EXCEPTION WHEN OTHERS THEN IF SQLERRM='REQUEST_ALREADY_RESERVED_FOR_ATTEMPT' THEN rejected:=true;ELSE RAISE;END IF;END;
 IF NOT rejected THEN RAISE EXCEPTION 'DUPLICATE_REQUEST_RESERVATION_ACCEPTED';END IF;
 body:=CASE WHEN i=1 THEN jsonb_build_object('status','OK','request_id','FIXTURE-page1','results',jsonb_build_array(fwd,missing),'next_url','https://api.massive.com/stocks/v1/splits?cursor=FIXTURE2')::text ELSE jsonb_build_object('status','OK','request_id','FIXTURE-page2','results',jsonb_build_array(fwd,altered,outside))::text END;
 wire_body:=CASE WHEN i=1 THEN replace(body,'cursor=FIXTURE2','cursor=FIXTURE2&apiKey=NON_SECRET_FIXTURE') ELSE body END;
 h:=encode(sha256(convert_to(body,'UTF8')),'hex');
 page:=jsonb_build_object('archive_key',k,'page_number',i,'request_url',c->>'request_url','next_url',CASE WHEN i=1 THEN 'https://api.massive.com/stocks/v1/splits?cursor=FIXTURE2' ELSE NULL END,
 'content_text',body,'source_sha256',h,'wire_sha256',encode(sha256(convert_to(wire_body,'UTF8')),'hex'),'wire_bytes',octet_length(wire_body),'http_status',200,'retrieved_at',clock_timestamp(),'result_count',CASE WHEN i=1 THEN 2 ELSE 3 END,'provider_request_id','FIXTURE-page'||i,'elapsed_ms',1);
 IF i=1 THEN
  rejected:=false;BEGIN PERFORM population_top3000_202608_202609_v1.massive_actions_commit_v1(a,token,jsonb_set(page,'{source_sha256}',to_jsonb(repeat('0',64))));EXCEPTION WHEN OTHERS THEN IF SQLERRM='MASSIVE_ACTION_STORED_HASH_MISMATCH' THEN rejected:=true;ELSE RAISE;END IF;END;
  IF NOT rejected OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.massive_archive_page_v1 WHERE archive_key=k) THEN RAISE EXCEPTION 'BAD_SOURCE_HASH_ACCEPTED_OR_PARTIALLY_SAVED';END IF;
 END IF;
 r:=population_top3000_202608_202609_v1.massive_actions_commit_v1(a,token,page);
 IF r->>'state' IS DISTINCT FROM (CASE WHEN i=1 THEN 'CONTINUE' ELSE 'COMPLETE' END) THEN RAISE EXCEPTION 'PAGE_COMMIT_HANDOFF_FAILED %',r;END IF;
 expected_storage:=expected_storage+octet_length(body);
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_provider_lease WHERE attempt_id=a AND (released_at IS NULL OR received_bytes<>octet_length(body))) THEN RAISE EXCEPTION 'STORED_BYTES_OR_RESERVATION_RELEASE_MISMATCH';END IF;
 IF i=1 THEN
  c:=population_top3000_202608_202609_v1.massive_actions_context_v1(tid);
  IF c->>'state'<>'PAGE_LIMIT_REVIEW' OR c->>'page_number'<>'2' OR (SELECT count(*) FROM population_top3000_202608_202609_v1.massive_archive_page_v1 WHERE archive_key=k)<>1 THEN RAISE EXCEPTION 'PAGE_CAP_TRUNCATED_OR_LOST_CONTINUATION';END IF;
  UPDATE population_top3000_202608_202609_v1.pipeline_task SET payload=jsonb_set(payload,'{max_pages}','2'::jsonb) WHERE task_id=tid;
 END IF;
END LOOP;
IF (SELECT stored_bytes-old_storage FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton)<>expected_storage OR (SELECT provider_requests-old_requests FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton)<>2 THEN RAISE EXCEPTION 'SOURCE_STORAGE_OR_ONE_REQUEST_PER_PAGE_ACCOUNTING';END IF;
IF (SELECT content_text::jsonb#>>'{results,1,huge_integer}' FROM population_top3000_202608_202609_v1.source_document WHERE source_id=k||':page:1')<>'9007199254740993' THEN RAISE EXCEPTION 'EXACT_SOURCE_INTEGER_LEXEME_LOST';END IF;
UPDATE population_top3000_202608_202609_v1.pipeline_control SET enabled=true,next_wakeup_at=clock_timestamp() WHERE singleton;
FOR i IN 1..4 LOOP
 r:=population_top3000_202608_202609_v1.pipeline_tick_v1();
 IF r->>'state' IS DISTINCT FROM 'DISPATCHED' OR r#>>'{result,state}' IS DISTINCT FROM (CASE WHEN i=1 THEN 'CONTINUE' ELSE 'COMPLETE' END) THEN RAISE EXCEPTION 'N03_NORMALIZE_DERIVE_VALIDATE_DISPATCH_FAILED tick=% receipt=%',i,r;END IF;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=(r->>'task_id')::uuid AND family_id='N03') THEN RAISE EXCEPTION 'UNRELATED_TASK_DISPATCHED';END IF;
END LOOP;
IF (SELECT count(*) FROM population_top3000_202608_202609_v1.source_observation WHERE family_id='N03')<>4 OR (SELECT count(*) FROM population_top3000_202608_202609_v1.expected_observation WHERE family_id='N03')<>4 THEN RAISE EXCEPTION 'SOURCE_EVENT_DUPLICATION_OR_DROPPED_OPTIONAL_RECORD';END IF;
IF (SELECT count(*) FROM population_top3000_202608_202609_v1.source_observation WHERE family_id='N03' AND field_state->>'provider_event_id'='CONFLICTING')<>2 THEN RAISE EXCEPTION 'SOURCE_ID_VERSION_CONFLICT_NOT_PRESERVED';END IF;
IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.source_observation WHERE family_id='N03' AND source_record_key='FIXTURE-outside' AND field_state->>'execution_date'='QUARANTINED' AND field_state->>'split_from'='QUARANTINED' AND values_json->>'share_unit_ratio_candidate' IS NULL)
 OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.source_observation WHERE family_id='N03' AND source_record_key='FIXTURE-optional' AND field_state->>'execution_date'='UNAVAILABLE') THEN RAISE EXCEPTION 'OPTIONAL_INVALID_OR_OUTSIDE_FIELDS_FALSELY_VERIFIED';END IF;
IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.source_observation WHERE family_id='N03' AND (available_at IS NOT NULL OR source_published_at IS NOT NULL OR historical_received_at IS NOT NULL OR company_id IS NOT NULL OR security_id IS NOT NULL OR availability_policy<>'ARCHIVED_OBSERVATION')) THEN RAISE EXCEPTION 'FALSE_IDENTITY_OR_AVAILABILITY';END IF;
IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_partition p JOIN population_top3000_202608_202609_v1.pipeline_validation v ON v.validation_id=p.last_validation_id WHERE p.family_id='N03' AND p.denominator_state='PROVISIONAL' AND NOT p.publication_eligible AND v.result='DENOMINATOR_UNRESOLVED') THEN RAISE EXCEPTION 'FALSE_COHORT_COMPLETENESS';END IF;
-- Definitive HTTP500 blocks without retry and adds no retained source bytes.
FOR i IN 1..2 LOOP
 failure_tid:=population_top3000_202608_202609_v1.pipeline_enqueue_v1('N03','ACQUIRE','splits-execution-202608-202609','{"max_pages":1,"fixture_only":true}'::jsonb,vers||jsonb_build_object('failure_fixture',i));
 a:=gen_random_uuid();token:=repeat(CASE WHEN i=1 THEN 'c' ELSE 'd' END,64);
 UPDATE population_top3000_202608_202609_v1.acquisition_control SET next_provider_at=clock_timestamp() WHERE singleton;
 UPDATE population_top3000_202608_202609_v1.pipeline_task SET status='LEASED',attempt_id=a,lease_until=clock_timestamp()+interval '60 seconds',owner_fence=2,stop_generation=0,executions=executions+1 WHERE task_id=failure_tid;
 INSERT INTO population_top3000_202608_202609_v1.pipeline_attempt(attempt_id,task_id,execution_number,token_sha256,owner_fence,stop_generation,adapter_sha256)
 SELECT a,failure_tid,t.executions,encode(sha256(convert_to(token,'UTF8')),'hex'),2,0,ad.function_sha256 FROM population_top3000_202608_202609_v1.pipeline_task t JOIN population_top3000_202608_202609_v1.pipeline_adapter ad ON ad.family_id=t.family_id AND ad.stage=t.stage WHERE t.task_id=failure_tid;
 c:=population_top3000_202608_202609_v1.massive_actions_claim_v1(a,token);IF c->>'state'<>'RESERVED' THEN RAISE EXCEPTION 'FAILURE_FIXTURE_RESERVATION_FAILED';END IF;
 r:=population_top3000_202608_202609_v1.massive_actions_failure_v1(a,token,jsonb_build_object('code',CASE WHEN i=1 THEN 'HTTP_500' ELSE 'FETCH_FAILED' END,'failure_class','TRANSPORT_READ','http_status',CASE WHEN i=1 THEN 500 ELSE NULL END,'received_bytes',123,'retry_after_seconds',60,'fetch_started',true,'wire_sha256',NULL));
 IF r->>'state'<>'BLOCKED' OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=failure_tid AND status='BLOCKED') THEN RAISE EXCEPTION 'SOURCE_FAILURE_AUTO_RETRY_ADMITTED';END IF;
 IF i=1 AND NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_provider_lease WHERE attempt_id=a AND released_at IS NOT NULL AND received_bytes=0) THEN RAISE EXCEPTION 'DEFINITIVE_UNRETAINED_ERROR_BYTES_CHARGED_TO_STORAGE';END IF;
 IF i=2 AND (NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_provider_lease WHERE attempt_id=a AND released_at IS NULL) OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=failure_tid AND blocker_code='UNCERTAIN_EFFECT' AND lease_until IS NOT NULL)) THEN RAISE EXCEPTION 'UNKNOWN_EFFECT_RESERVATION_OR_DEADLINE_DISCARDED';END IF;
END LOOP;
IF (SELECT stored_bytes-old_storage FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton)<>expected_storage THEN RAISE EXCEPTION 'FAILED_PAYLOAD_COUNTED_AS_RETAINED_STORAGE';END IF;
SELECT encode(sha256(convert_to(coalesce(jsonb_agg(to_jsonb(u)),'[]'::jsonb)::text,'UTF8')),'hex') INTO after_universe FROM population_top3000_202608_202609_v1.pipeline_universe u;
IF before_universe IS DISTINCT FROM after_universe THEN RAISE EXCEPTION 'SOURCE_LANE_CHANGED_COMPANY_UNIVERSE';END IF;
END $test$;
SELECT 'PASS_ROLLBACK_ONLY_N03_SYNTHETIC_TRANSPORT_AND_PROVISIONAL_NORMALIZATION' result,clock_timestamp() observed_at,
false provider_accuracy_verified,false live_provider_requested,
(SELECT count(*) FROM population_top3000_202608_202609_v1.massive_archive_page_v1 WHERE family_id='N03') retained_fixture_pages,
(SELECT count(*) FROM population_top3000_202608_202609_v1.source_observation WHERE family_id='N03') deduplicated_fixture_versions,
(SELECT jsonb_object_agg(fn,encode(sha256(convert_to(pg_get_functiondef(('population_top3000_202608_202609_v1.'||fn)::regprocedure),'UTF8')),'hex')) FROM (VALUES ('massive_archive_key_v1(text,text,jsonb)'),('massive_actions_context_v1(uuid)'),('massive_actions_acquire_v1(uuid,text)'),('massive_actions_claim_v1(uuid,text)'),('massive_actions_commit_v1(uuid,text,jsonb)'),('massive_actions_failure_v1(uuid,text,jsonb)'),('massive_actions_date_v1(text)'),('massive_actions_normalize_v1(uuid,text)'),('massive_actions_derive_v1(uuid,text)'),('pipeline_validate_v1(uuid,text)')) h(fn)) tested_function_sha256,
(SELECT jsonb_object_agg(stage,encode(sha256(convert_to(pg_get_functiondef(fn::regprocedure),'UTF8')),'hex')) FROM (VALUES ('ACQUIRE','population_top3000_202608_202609_v1.massive_actions_acquire_v1(uuid,text)','PROVIDER'),
('NORMALIZE','population_top3000_202608_202609_v1.massive_actions_normalize_v1(uuid,text)','DATABASE'),
('DERIVE','population_top3000_202608_202609_v1.massive_actions_derive_v1(uuid,text)','DATABASE'),
('VALIDATE','population_top3000_202608_202609_v1.pipeline_validate_v1(uuid,text)','DATABASE'))v(stage,fn,resource)) function_sha256;
ROLLBACK;
