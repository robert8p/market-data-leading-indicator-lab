-- Append after BEGIN + changed function DDL + source-policy fixture below; all changes ROLLBACK.
CREATE TEMP TABLE options_v8_test_receipts(test_name text,receipt jsonb);
DO $fixture$
DECLARE fixture_bounds jsonb:=$policy${"policy_revision": "CTX02_MEASURED_REQUEST_CONTINUATION_V8", "maximum_new_pages": 50, "global_ceiling": {"provider_requests": 10000, "http_invocations": 10055, "source_bytes": 1000000000, "relation_bytes": 8000000000}, "request_policies": {"d377d980-5dbd-4276-a2c9-d5638c60b668": {"request_key": "ctx02-census-v1:0869cb2de1b0fe2ad0789fd4498df455de76ad92aa32805c36417a4cf87bb5cd", "manifest_sha256": "0869cb2de1b0fe2ad0789fd4498df455de76ad92aa32805c36417a4cf87bb5cd", "input_versions_sha256": "0fc0c143e7e26d60c08f63180b3ede47bab24d311e7a91977b251c747e1b3f52", "payload_sha256": "da7b69b5121893f463d6a31c311d70059eb29b7cf5b7b60ff38a8ec81b2b007d"}, "da79b746-8550-41c9-a6f0-3d87f4e352fd": {"request_key": "ctx02-census-v1:082572b99c04ab298a3794bc6013aca7114cd07f8c1d78564e6ad6b4209a3ef9", "manifest_sha256": "082572b99c04ab298a3794bc6013aca7114cd07f8c1d78564e6ad6b4209a3ef9", "input_versions_sha256": "c4d9c4f89fe47bc721fa23f0663ae3525adb56c8a858657489b5ced667040af8", "payload_sha256": "ff93c5b0ce687a432d4c3bac7c8da163cb2f51f030528c2ea6bb4d2aaa805cc2"}, "f55551ce-df3f-4726-8c16-855b93a92d21": {"request_key": "ctx02-census-v1:b48c9de45f2af8c141349682d83218995fbf18544fcc24f035a90e247435dc52", "manifest_sha256": "b48c9de45f2af8c141349682d83218995fbf18544fcc24f035a90e247435dc52", "input_versions_sha256": "c4d9c4f89fe47bc721fa23f0663ae3525adb56c8a858657489b5ced667040af8", "payload_sha256": "abb31160eb7d5703e88b346d3dec3e381772f89d9bb82a751c661e286a4ab56e"}}}$policy$::jsonb; sha text; ev bigint;
BEGIN
 PERFORM pg_advisory_xact_lock(2026083000261022);
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE status='LEASED') OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_provider_lease WHERE released_at IS NULL) OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.connector_probe_lease_v1 WHERE released_at IS NULL) THEN RAISE EXCEPTION 'FIXTURE_ACTIVE_OR_UNKNOWN_WAIT';END IF;
 sha:=encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.ctx02_clear_page_admission_v6(text,bigint,bigint,jsonb)'::regprocedure),'UTF8')),'hex');
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES('ROLLBACK_TEST_V8_POLICY_REVIEW_FIXTURE',jsonb_build_object('approved',true,'producer','rollback-producer','reviewer','rollback-distinct-reviewer','handler_function_sha256',sha,'handler_bounds_sha256',encode(sha256(convert_to(fixture_bounds::text,'UTF8')),'hex'),'owner_id','top3000-completion-20261024-root-5db48b32d103','owner_fence',3,'stop_generation',1,'scope_sha256','8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd')) RETURNING event_id INTO ev;
 UPDATE population_top3000_202608_202609_v1.continuation_handler_v6 SET function_sha256=sha,bounds=fixture_bounds,review_event_id=ev WHERE kind='CTX02_PAGE_ADMISSION';
END;$fixture$;
DO $test$
DECLARE req text:='ctx02-census-v1:082572b99c04ab298a3794bc6013aca7114cd07f8c1d78564e6ad6b4209a3ef9';tid uuid:='da79b746-8550-41c9-a6f0-3d87f4e352fd';bounds jsonb;item jsonb;r jsonb;before_t jsonb;after_t jsonb;before_a jsonb;before_p jsonb;n text;expected text;rejected boolean;pages integer;calls integer;
BEGIN
 SELECT h.bounds INTO bounds FROM population_top3000_202608_202609_v1.continuation_handler_v6 h WHERE kind='CTX02_PAGE_ADMISSION';item:=jsonb_build_object('source_task_id',tid,'bounds',bounds);
 SELECT count(*) INTO pages FROM population_top3000_202608_202609_v1.ctx02_census_page_v1 WHERE partition_key=req;
 SELECT count(*) INTO calls FROM population_top3000_202608_202609_v1.pipeline_provider_lease l JOIN population_top3000_202608_202609_v1.pipeline_attempt z USING(attempt_id) WHERE z.task_id=tid AND l.provider='Massive';
 UPDATE population_top3000_202608_202609_v1.ctx02_census_admission_v1 SET admitted_page_ceiling=pages,max_provider_calls=calls,expires_at=clock_timestamp()-interval '1minute' WHERE request_key=req;
 UPDATE population_top3000_202608_202609_v1.pipeline_task SET status='BLOCKED',blocker_code='CTX02_CENSUS_PAGE_CAP_REVIEW',resumption_condition='{"event":"ROLLBACK_V8_EXHAUSTED_TRANCHE_FIXTURE"}'::jsonb WHERE task_id=tid AND status IN('READY','BLOCKED') AND lease_until IS NULL;
 IF NOT FOUND THEN RAISE EXCEPTION 'FIXTURE_SEPTEMBER_TASK_NOT_IDLE';END IF;
 FOREACH n IN ARRAY ARRAY['owner','stop','unknown_provider','resource','chain','preimage','caller_bounds','registry_bounds','manifest','tick','reserved'] LOOP
  rejected:=false;
  BEGIN
   CASE n
   WHEN 'owner' THEN UPDATE population_top3000_202608_202609_v1.acquisition_control SET owner_id='invalid-test-owner' WHERE singleton;expected:='OPTIONS_V8_CURRENT_AUTHORITY_REQUIRED';
   WHEN 'stop' THEN UPDATE population_top3000_202608_202609_v1.acquisition_control SET stop_generation=stop_generation+1 WHERE singleton;expected:='OPTIONS_V8_CURRENT_AUTHORITY_REQUIRED';
   WHEN 'unknown_provider' THEN UPDATE population_top3000_202608_202609_v1.pipeline_provider_lease SET released_at=NULL WHERE attempt_id=(SELECT attempt_id FROM population_top3000_202608_202609_v1.pipeline_provider_lease WHERE released_at IS NOT NULL ORDER BY released_at DESC LIMIT 1);expected:='OPTIONS_V8_ACTIVE_OR_UNRECONCILED_EFFECT';
   WHEN 'resource' THEN UPDATE population_top3000_202608_202609_v1.pipeline_control SET budget_evidence=jsonb_set(budget_evidence,'{resource_limits,max_relation_bytes}','1') WHERE singleton;expected:='OPTIONS_V8_MEASURED_RESOURCE_REVIEW_REQUIRED';
   WHEN 'tick' THEN UPDATE population_top3000_202608_202609_v1.pipeline_control SET max_ticks=tick_count+1 WHERE singleton;expected:='OPTIONS_V8_MEASURED_RESOURCE_REVIEW_REQUIRED';
   WHEN 'reserved' THEN UPDATE population_top3000_202608_202609_v1.acquisition_control SET reserved_bytes=max_source_bytes-stored_bytes WHERE singleton;expected:='OPTIONS_V8_MEASURED_RESOURCE_REVIEW_REQUIRED';
   WHEN 'chain' THEN UPDATE population_top3000_202608_202609_v1.ctx02_census_page_v1 SET source_sha256=repeat('0',64) WHERE partition_key=req AND page_number=1;expected:='OPTIONS_V8_RETAINED_CHAIN_CHANGED';
   WHEN 'preimage' THEN EXECUTE replace(pg_get_functiondef('population_top3000_202608_202609_v1.ctx02_census_context_v1(uuid)'::regprocedure),'BEGIN','BEGIN /* rollback preimage test */');expected:='OPTIONS_V8_REVIEWED_PREIMAGE_CHANGED';
   WHEN 'caller_bounds' THEN expected:='OPTIONS_V8_EXACT_REVIEWED_POLICY_REQUIRED';PERFORM population_top3000_202608_202609_v1.ctx02_clear_page_admission_v6('top3000-completion-20261024-root-5db48b32d103',3,1,jsonb_set(item,'{bounds,maximum_new_pages}','51'));RAISE EXCEPTION 'NEGATIVE_NOT_REJECTED';
   WHEN 'registry_bounds' THEN UPDATE population_top3000_202608_202609_v1.continuation_handler_v6 SET bounds=jsonb_set(continuation_handler_v6.bounds,'{maximum_new_pages}','49') WHERE kind='CTX02_PAGE_ADMISSION';expected:='OPTIONS_V8_EXACT_REVIEWED_POLICY_REQUIRED';
    PERFORM population_top3000_202608_202609_v1.ctx02_clear_page_admission_v6('top3000-completion-20261024-root-5db48b32d103',3,1,jsonb_build_object('source_task_id',tid,'bounds',(SELECT h.bounds FROM population_top3000_202608_202609_v1.continuation_handler_v6 h WHERE kind='CTX02_PAGE_ADMISSION')));RAISE EXCEPTION 'NEGATIVE_NOT_REJECTED';
   WHEN 'manifest' THEN
    expected:='OPTIONS_V8_PINNED_REQUEST_CHANGED';
    UPDATE population_top3000_202608_202609_v1.continuation_handler_v6 h SET bounds=jsonb_set(h.bounds,ARRAY['request_policies',tid::text,'manifest_sha256'],to_jsonb(repeat('0',64))) WHERE kind='CTX02_PAGE_ADMISSION';
    UPDATE population_top3000_202608_202609_v1.pipeline_event e SET details=jsonb_set(e.details,'{handler_bounds_sha256}',to_jsonb((SELECT encode(sha256(convert_to(h.bounds::text,'UTF8')),'hex') FROM population_top3000_202608_202609_v1.continuation_handler_v6 h WHERE kind='CTX02_PAGE_ADMISSION'))) WHERE event_id=(SELECT review_event_id FROM population_top3000_202608_202609_v1.continuation_handler_v6 WHERE kind='CTX02_PAGE_ADMISSION');
    PERFORM population_top3000_202608_202609_v1.ctx02_clear_page_admission_v6('top3000-completion-20261024-root-5db48b32d103',3,1,jsonb_build_object('source_task_id',tid,'bounds',(SELECT h.bounds FROM population_top3000_202608_202609_v1.continuation_handler_v6 h WHERE kind='CTX02_PAGE_ADMISSION')));RAISE EXCEPTION 'NEGATIVE_NOT_REJECTED';
   END CASE;
   PERFORM population_top3000_202608_202609_v1.ctx02_clear_page_admission_v6('top3000-completion-20261024-root-5db48b32d103',3,1,item);RAISE EXCEPTION 'NEGATIVE_NOT_REJECTED';
  EXCEPTION WHEN raise_exception THEN IF SQLERRM=expected THEN rejected:=true;ELSE RAISE;END IF;END;
  IF NOT rejected THEN RAISE EXCEPTION 'NEGATIVE_FAILED %',n;END IF;
  INSERT INTO options_v8_test_receipts VALUES(n,jsonb_build_object('rejected',true,'expected',expected));
 END LOOP;
 -- Expiry-only renewal cannot create additional calls/pages.
 BEGIN
  UPDATE population_top3000_202608_202609_v1.ctx02_census_admission_v1 SET admitted_page_ceiling=pages+10,max_provider_calls=calls+10,expires_at=clock_timestamp()-interval '1minute' WHERE request_key=req;
  r:=population_top3000_202608_202609_v1.ctx02_clear_page_admission_v6('top3000-completion-20261024-root-5db48b32d103',3,1,item);
  IF r->>'reason'<>'EXPIRY_ONLY' OR (r->>'admitted_page_ceiling')::integer<>pages+10 OR (r->>'admitted_call_ceiling')::integer<>calls+10 THEN RAISE EXCEPTION 'EXPIRY_WINDFALL';END IF;
  RAISE EXCEPTION 'EXPECTED_SUBTRANSACTION_ROLLBACK';
 EXCEPTION WHEN raise_exception THEN IF SQLERRM<>'EXPECTED_SUBTRANSACTION_ROLLBACK' THEN RAISE;END IF;END;
 INSERT INTO options_v8_test_receipts VALUES('expiry_only_preserves_ceiling',r);
 BEGIN
  UPDATE population_top3000_202608_202609_v1.ctx02_census_admission_v1 SET admitted_page_ceiling=pages+10,max_provider_calls=calls,expires_at=clock_timestamp()+interval '1hour' WHERE request_key=req;
  r:=population_top3000_202608_202609_v1.ctx02_clear_page_admission_v6('top3000-completion-20261024-root-5db48b32d103',3,1,item);
  IF r->>'reason'<>'CALL_ALLOWANCE' OR (r->>'admitted_page_ceiling')::integer<>pages+10 OR (r->>'admitted_call_ceiling')::integer<>calls+10 THEN RAISE EXCEPTION 'CALL_ALLOWANCE_WIDENED_PAGES';END IF;
  RAISE EXCEPTION 'EXPECTED_SUBTRANSACTION_ROLLBACK';
 EXCEPTION WHEN raise_exception THEN IF SQLERRM<>'EXPECTED_SUBTRANSACTION_ROLLBACK' THEN RAISE;END IF;END;
 INSERT INTO options_v8_test_receipts VALUES('call_only_preserves_page_ceiling',r);
 SELECT to_jsonb(t) INTO before_t FROM population_top3000_202608_202609_v1.pipeline_task t WHERE task_id=tid;
 SELECT to_jsonb(a) INTO before_a FROM population_top3000_202608_202609_v1.acquisition_control a WHERE singleton;
 SELECT to_jsonb(p) INTO before_p FROM population_top3000_202608_202609_v1.pipeline_control p WHERE singleton;
 r:=population_top3000_202608_202609_v1.ctx02_clear_page_admission_v6('top3000-completion-20261024-root-5db48b32d103',3,1,item);
 IF r->>'state'<>'CLEARED' OR (r->>'next_page')::integer<>pages+1 OR (r->>'admitted_page_ceiling')::integer<>pages+50 THEN RAISE EXCEPTION 'SEPTEMBER_POSITIVE_INVALID';END IF;
 SELECT to_jsonb(t) INTO after_t FROM population_top3000_202608_202609_v1.pipeline_task t WHERE task_id=tid;
 IF (after_t-ARRAY['status','blocker_code','blocker_until','resumption_condition','not_before']) IS DISTINCT FROM (before_t-ARRAY['status','blocker_code','blocker_until','resumption_condition','not_before']) OR before_a IS DISTINCT FROM (SELECT to_jsonb(a) FROM population_top3000_202608_202609_v1.acquisition_control a WHERE singleton) THEN RAISE EXCEPTION 'COUNTERS_CHANGED';END IF;
 INSERT INTO options_v8_test_receipts VALUES('september_positive',r||jsonb_build_object('task_and_acquisition_counters_preserved',true));
 -- Direct-helper proof must emulate the dispatcher acknowledgement so its old queue row cannot win the next fairness test.
 UPDATE population_top3000_202608_202609_v1.continuation_clear_queue_v6 SET status='WAITING_OUTPUT',attempts=attempts+1,claimed_at=clock_timestamp(),last_receipt=r||jsonb_build_object('rollback_fixture_dispatcher_ack',true),updated_at=clock_timestamp()
 WHERE status='READY' AND attempts=0 AND kind='OPERATIONAL_REVIEW' AND family_id='CTX02' AND blocker_code='CTX02_CENSUS_PAGE_CAP_REVIEW' AND jsonb_array_length(input_manifest)=1 AND input_manifest->0->>'task_id'=tid::text;
 BEGIN PERFORM population_top3000_202608_202609_v1.ctx02_clear_page_admission_v6('top3000-completion-20261024-root-5db48b32d103',3,1,item);RAISE EXCEPTION 'DUPLICATE_ACCEPTED';EXCEPTION WHEN raise_exception THEN IF SQLERRM<>'OPTIONS_V8_EXACT_IDLE_CHECKPOINT_REQUIRED' THEN RAISE;END IF;END;
 INSERT INTO options_v8_test_receipts VALUES('duplicate',jsonb_build_object('rejected',true));
END;$test$;
-- Append additional boundary/dispatcher proof below before the final rollback.
DO $boundary$
DECLARE tid uuid:='d377d980-5dbd-4276-a2c9-d5638c60b668';req text:='ctx02-census-v1:0869cb2de1b0fe2ad0789fd4498df455de76ad92aa32805c36417a4cf87bb5cd';lastp population_top3000_202608_202609_v1.ctx02_census_page_v1%rowtype;doc jsonb;attempt jsonb;lease jsonb;bounds jsonb;pages integer;target integer;calls integer;seq bigint;i integer;aid uuid;sid text;body text;sha text;url text;r jsonb;
BEGIN
 -- Compact synthetic records exist only in this rollback transaction. No source transport or production fixture survives.
 SELECT h.bounds INTO bounds FROM population_top3000_202608_202609_v1.continuation_handler_v6 h WHERE kind='CTX02_PAGE_ADMISSION';
 SELECT * INTO STRICT lastp FROM population_top3000_202608_202609_v1.ctx02_census_page_v1 WHERE partition_key=req ORDER BY page_number DESC LIMIT 1;
 pages:=lastp.page_number;target:=greatest(1248,pages);url:=lastp.next_url;
 SELECT to_jsonb(d) INTO doc FROM population_top3000_202608_202609_v1.source_document d WHERE source_id=lastp.source_id;
 SELECT to_jsonb(z) INTO attempt FROM population_top3000_202608_202609_v1.pipeline_attempt z WHERE attempt_id=lastp.attempt_id;
 SELECT to_jsonb(l) INTO lease FROM population_top3000_202608_202609_v1.pipeline_provider_lease l WHERE attempt_id=lastp.attempt_id;
 SELECT max(execution_number) INTO seq FROM population_top3000_202608_202609_v1.pipeline_attempt WHERE task_id=tid;
 IF target>pages+400 OR lease IS NULL OR url IS NULL THEN RAISE EXCEPTION 'BOUNDED_SYNTHETIC_FIXTURE_NOT_SUITABLE';END IF;
 FOR i IN pages+1..target LOOP
  aid:=gen_random_uuid();sid:='ROLLBACK-ONLY-V8:'||aid::text;body:=jsonb_build_object('rollback_only',true,'page',i)::text;sha:=encode(sha256(convert_to(body,'UTF8')),'hex');
  INSERT INTO population_top3000_202608_202609_v1.source_document SELECT * FROM jsonb_populate_record(NULL::population_top3000_202608_202609_v1.source_document,doc||jsonb_build_object('source_id',sid,'source_uri','urn:rollback-v8:'||aid::text,'source_sha256',sha,'content_text',body,'content_bytes',octet_length(body),'content_external_uri',null,'evidence',jsonb_build_object('rollback_only',true)));
  INSERT INTO population_top3000_202608_202609_v1.pipeline_attempt SELECT * FROM jsonb_populate_record(NULL::population_top3000_202608_202609_v1.pipeline_attempt,attempt||jsonb_build_object('attempt_id',aid,'execution_number',seq+i-pages,'started_at',clock_timestamp(),'ended_at',clock_timestamp(),'request_id',null));
  INSERT INTO population_top3000_202608_202609_v1.pipeline_provider_lease SELECT * FROM jsonb_populate_record(NULL::population_top3000_202608_202609_v1.pipeline_provider_lease,lease||jsonb_build_object('attempt_id',aid,'started_at',clock_timestamp(),'released_at',clock_timestamp(),'received_bytes',octet_length(body)));
  INSERT INTO population_top3000_202608_202609_v1.ctx02_census_page_v1 SELECT * FROM jsonb_populate_record(NULL::population_top3000_202608_202609_v1.ctx02_census_page_v1,to_jsonb(lastp)||jsonb_build_object('page_number',i,'attempt_id',aid,'source_id',sid,'source_sha256',sha,'wire_sha256',sha,'wire_bytes',octet_length(body),'result_count',0,'request_url',url,'next_url','https://api.massive.com/v3/reference/options/contracts?cursor=ROLLBACKV8'||i,'retrieved_at',clock_timestamp(),'normalized_at',null,'normalized_source_records',null));
  url:='https://api.massive.com/v3/reference/options/contracts?cursor=ROLLBACKV8'||i;
 END LOOP;
 IF target>pages THEN UPDATE population_top3000_202608_202609_v1.pipeline_task SET checkpoint=checkpoint||jsonb_build_object('next_page',target+1,'last_source_id',sid,'last_source_sha256',sha,'next_url',url) WHERE task_id=tid;END IF;
 SELECT count(*) INTO calls FROM population_top3000_202608_202609_v1.pipeline_provider_lease l JOIN population_top3000_202608_202609_v1.pipeline_attempt z USING(attempt_id) WHERE z.task_id=tid AND l.provider='Massive';
 UPDATE population_top3000_202608_202609_v1.ctx02_census_admission_v1 SET admitted_page_ceiling=target,max_provider_calls=calls,expires_at=clock_timestamp()+interval '1hour' WHERE request_key=req;
 UPDATE population_top3000_202608_202609_v1.pipeline_task SET status='BLOCKED',blocker_code='CTX02_CENSUS_PAGE_CAP_REVIEW',resumption_condition='{"event":"ROLLBACK_V8_EXHAUSTED_TRANCHE_FIXTURE"}'::jsonb WHERE task_id=tid AND status IN('READY','BLOCKED') AND lease_until IS NULL;
 IF NOT FOUND THEN RAISE EXCEPTION 'FIXTURE_ORIGINAL_TASK_NOT_IDLE';END IF;
 r:=population_top3000_202608_202609_v1.ctx02_clear_page_admission_v6('top3000-completion-20261024-root-5db48b32d103',3,1,jsonb_build_object('source_task_id',tid,'bounds',bounds));
 IF (r->>'admitted_page_ceiling')::integer<>target+50 OR target<=1247 THEN RAISE EXCEPTION 'POST1247_BOUNDARY_NOT_PROVEN';END IF;
 INSERT INTO options_v8_test_receipts VALUES('post1247_original',r||jsonb_build_object('synthetic_rows_rollback_only',target-pages,'external_requests',0));
END;$boundary$;
DO $dispatcher$
DECLARE tid uuid:='f55551ce-df3f-4726-8c16-855b93a92d21';req text:='ctx02-census-v1:b48c9de45f2af8c141349682d83218995fbf18544fcc24f035a90e247435dc52';r jsonb;old_attempts bigint;new_attempts bigint;v_clearing text;resource_assignment bigint;
BEGIN
 -- Disable unrelated helper effects only inside rollback proof. Classification and claim selection stay installed.
 UPDATE population_top3000_202608_202609_v1.continuation_handler_v6 SET enabled=false WHERE kind<>'CTX02_PAGE_ADMISSION';
 UPDATE population_top3000_202608_202609_v1.pipeline_event SET at=at-interval '2minutes' WHERE event='CONTINUATION_DISPATCH_PASS_V6' AND at>clock_timestamp()-interval '60seconds';
 SELECT coalesce(sum(attempts),0) INTO old_attempts FROM population_top3000_202608_202609_v1.continuation_clear_queue_v6 WHERE jsonb_array_length(input_manifest)=1 AND input_manifest->0->>'task_id'=tid::text;
 BEGIN
  UPDATE population_top3000_202608_202609_v1.pipeline_provider_lease SET released_at=NULL WHERE attempt_id=(SELECT attempt_id FROM population_top3000_202608_202609_v1.pipeline_provider_lease WHERE released_at IS NOT NULL ORDER BY released_at DESC LIMIT 1);
  r:=population_top3000_202608_202609_v1.continuation_dispatch_v6('top3000-completion-20261024-root-5db48b32d103',3,1);
  SELECT coalesce(sum(attempts),0) INTO new_attempts FROM population_top3000_202608_202609_v1.continuation_clear_queue_v6 WHERE jsonb_array_length(input_manifest)=1 AND input_manifest->0->>'task_id'=tid::text;
  IF old_attempts<>new_attempts OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=tid AND status='BLOCKED') THEN RAISE EXCEPTION 'UNKNOWN_LEASE_DISPATCHED';END IF;
  RAISE EXCEPTION 'EXPECTED_SUBTRANSACTION_ROLLBACK';
 EXCEPTION WHEN raise_exception THEN IF SQLERRM<>'EXPECTED_SUBTRANSACTION_ROLLBACK' THEN RAISE;END IF;END;
 INSERT INTO options_v8_test_receipts VALUES('dispatcher_unknown_lease',jsonb_build_object('attempts_preserved',true,'source_task_still_blocked',true));
 BEGIN
  UPDATE population_top3000_202608_202609_v1.pipeline_control SET max_ticks=tick_count+1 WHERE singleton;
  r:=population_top3000_202608_202609_v1.continuation_dispatch_v6('top3000-completion-20261024-root-5db48b32d103',3,1);
  SELECT clearing_id INTO v_clearing FROM population_top3000_202608_202609_v1.continuation_clear_queue_v6 WHERE status='NEEDS_REVIEW' AND jsonb_array_length(input_manifest)=1 AND input_manifest->0->>'task_id'=tid::text AND last_receipt->>'recoverable_condition'='OPTIONS_V8_MEASURED_RESOURCE_REVIEW_REQUIRED' ORDER BY updated_at DESC LIMIT 1;
  SELECT assignment_event_id INTO resource_assignment FROM population_top3000_202608_202609_v1.continuation_clear_queue_v6 WHERE kind='OPERATIONAL_REVIEW' AND family_id='RESOURCE' AND status='READY' AND input_manifest->0->>'max_ticks'=(SELECT max_ticks::text FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton) ORDER BY created_at DESC LIMIT 1;
  IF v_clearing IS NULL OR resource_assignment IS NULL THEN RAISE EXCEPTION 'RESOURCE_FAILURE_NOT_EXECUTABLE %',jsonb_build_object('dispatcher_result',r,'source_clearing',v_clearing,'resource_assignment',resource_assignment,'tick_limit',(SELECT max_ticks FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton),'queues',(SELECT jsonb_agg(x) FROM (SELECT clearing_id,kind,family_id,status,attempts,assignment_event_id,input_manifest->0->>'task_id' source_task,input_manifest->0->>'max_ticks' manifest_max_ticks,last_receipt FROM population_top3000_202608_202609_v1.continuation_clear_queue_v6 WHERE (family_id='RESOURCE' OR (jsonb_array_length(input_manifest)=1 AND input_manifest->0->>'task_id'=tid::text)) ORDER BY updated_at DESC LIMIT 8)x));END IF;
  IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_event WHERE event_id=resource_assignment AND event='ENGINEERING_CLEARING_ASSIGNMENT_V6') THEN RAISE EXCEPTION 'RESOURCE_ASSIGNMENT_MISSING';END IF;
  RAISE EXCEPTION 'EXPECTED_SUBTRANSACTION_ROLLBACK';
 EXCEPTION WHEN raise_exception THEN IF SQLERRM<>'EXPECTED_SUBTRANSACTION_ROLLBACK' THEN RAISE;END IF;END;
 INSERT INTO options_v8_test_receipts VALUES('dispatcher_resource_successor',jsonb_build_object('exact_source_clearing',v_clearing,'executable_resource_assignment',resource_assignment,'no_source_retry',true));
 r:=population_top3000_202608_202609_v1.continuation_dispatch_v6('top3000-completion-20261024-root-5db48b32d103',3,1);
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=tid AND status='READY') THEN RAISE EXCEPTION 'DISPATCHER_SECOND_REQUEST_NOT_REQUEUED';END IF;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.continuation_clear_queue_v6 WHERE status='WAITING_OUTPUT' AND jsonb_array_length(input_manifest)=1 AND input_manifest->0->>'task_id'=tid::text AND last_receipt->>'policy_sha256' IS NOT NULL) THEN RAISE EXCEPTION 'DISPATCHER_DID_NOT_REQUIRE_USEFUL_OUTPUT';END IF;
 INSERT INTO options_v8_test_receipts VALUES('dispatcher_second_request',r||jsonb_build_object('exact_source_ready',true,'waiting_for_real_output',true));
END;$dispatcher$;

SELECT test_name,receipt FROM options_v8_test_receipts ORDER BY test_name;
ROLLBACK;
