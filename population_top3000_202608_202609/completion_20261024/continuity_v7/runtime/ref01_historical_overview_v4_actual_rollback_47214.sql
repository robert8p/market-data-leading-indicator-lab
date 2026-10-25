-- Run route.sql inside this SAME rollback transaction before this body.
-- net.http_post queue rows are never committed; pg_net sends only after commit.
-- No provider or external worker call is performed by this SQL test.
SET LOCAL statement_timeout='35s';SET LOCAL lock_timeout='1s';
SELECT population_top3000_202608_202609_v1.ov4_seed('f2bbf487eca6f67fe4e27ea5091f0cee726e7c97e46b67299f776e691558a5e4');
CREATE TEMP TABLE ov4_test_result(name text,result jsonb);
DO $t$
DECLARE d jsonb;review bigint;aid bigint;c jsonb;s jsonb;tok text;b text;sha text;rid bigint;x jsonb;before_count int;i int;first_aid bigint;source_assignment bigint;source_claim bigint;sa jsonb;before_stored bigint;before_reserved bigint;quality_tested boolean:=false;probe jsonb;bad_review bigint;bad_aid bigint;BEGIN
 IF (SELECT count(*) FROM population_top3000_202608_202609_v1.ov4_request)<>200 THEN RAISE EXCEPTION 'TEST_200_FAILED';END IF;
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.ov4_request rq WHERE rq.r->>'exact_url'<>'https://api.massive.com/v3/reference/tickers/'||(rq.r->>'ticker')||'?date='||(rq.r->>'point_date')) THEN RAISE EXCEPTION 'TEST_URL_BINDING_FAILED';END IF;
 INSERT INTO ov4_test_result VALUES('200_exact_private_bindings','{"pass":true}');
 d:=jsonb_build_object('owner_id','top3000-completion-20261024-root-5db48b32d103','owner_fence',3,'stop_generation',1,'scope_sha256','8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd','worker_sha256','f2bbf487eca6f67fe4e27ea5091f0cee726e7c97e46b67299f776e691558a5e4','bundle_sha256',population_top3000_202608_202609_v1.ov4_bundle(),'request_manifest_sha256','13dafeef5c83990b6c73f8c8d9a7edcbfd81bff22b2f3e268e38d26d5c863bef','input_manifest_sha256','c6a21603b1015a3548de6e41b30ccdeb31c81b566f330e581d9ffc0a138cd57f','not_after',clock_timestamp()+interval '10 minutes','max_provider_calls',100,'max_http_calls',200,'max_source_bytes',33554432,'max_relation_bytes',33554432,'automatic_retry',false,'TEST_ONLY_ROLLBACK',true);

 -- Exact source claim/assignment are SYNTHETIC rollback-only fixtures, never source admission.
 sa:=jsonb_build_object('kind','EXACT200_HISTORICAL_OVERVIEW_FINITE_SOURCE_EXECUTION_V4','assigned_executor','automation:6adc2c96db348191bca7cf4eacfdac32','owner_id',d->>'owner_id','owner_fence',3,'stop_generation',1,'scope_sha256',d->>'scope_sha256','not_after',d->>'not_after','input_manifest_sha256',d->>'input_manifest_sha256','new_provider_requests',100,'maximum_http_invocations',200,'maximum_source_bytes',33554432,'maximum_additional_relation_bytes',33554432,'SYNTHETIC_ROLLBACK_ONLY',true);
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES('ENGINEERING_IMPLEMENTATION_ASSIGNMENT_V5',sa) RETURNING event_id INTO source_assignment;
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES('ENGINEERING_ASSIGNMENT_CLAIMED_V5',jsonb_build_object('assignment_event_id',source_assignment,'assignment_details_sha256',encode(sha256(convert_to(sa::text,'UTF8')),'hex'),'executor',sa->>'assigned_executor','owner_id',sa->>'owner_id','owner_fence',3,'stop_generation',1,'scope_sha256',sa->>'scope_sha256','expires_at',sa->>'not_after','SYNTHETIC_ROLLBACK_ONLY',true)) RETURNING event_id INTO source_claim;
 d:=d||jsonb_build_object('execution_claim_event_id',source_claim);
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES('REF01_OVERVIEW_FINITE_SOURCE_REVIEW_V4',jsonb_build_object('approved',true,'producer','synthetic-test-producer','reviewer','synthetic-test-reviewer','proposal_sha256',encode(sha256(convert_to(d::text,'UTF8')),'hex'))) RETURNING event_id INTO review;
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES('REF01_OVERVIEW_FINITE_SOURCE_ADMITTED_V4',d||jsonb_build_object('review_event_id',review)) RETURNING event_id INTO aid;
 -- Six exact guard negatives; only synthetic events or rollback-only controls change.
 BEGIN
 UPDATE population_top3000_202608_202609_v1.pipeline_event SET details=jsonb_set(details,'{expires_at}',to_jsonb((clock_timestamp()-interval '1 second')::text)) WHERE event_id=source_claim;
 PERFORM population_top3000_202608_202609_v1.ov4_guard(aid);
 RAISE EXCEPTION 'NEGATIVE_NOT_REJECTED';
 EXCEPTION WHEN OTHERS THEN IF SQLERRM IS DISTINCT FROM 'OV4_ACTIVE_EXACT_FINITE_SOURCE_EXECUTION_CLAIM_REQUIRED' THEN RAISE;END IF;END;
 BEGIN
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES('ENGINEERING_ASSIGNMENT_COMPLETED_V5',jsonb_build_object('claim_event_id',source_claim,'SYNTHETIC_ROLLBACK_ONLY',true));
 PERFORM population_top3000_202608_202609_v1.ov4_guard(aid);
 RAISE EXCEPTION 'NEGATIVE_NOT_REJECTED';
 EXCEPTION WHEN OTHERS THEN IF SQLERRM IS DISTINCT FROM 'OV4_ACTIVE_EXACT_FINITE_SOURCE_EXECUTION_CLAIM_REQUIRED' THEN RAISE;END IF;END;
 BEGIN
 UPDATE population_top3000_202608_202609_v1.acquisition_control SET owner_id='SYNTHETIC_STALE_OWNER_ROLLBACK_ONLY' WHERE singleton;
 PERFORM population_top3000_202608_202609_v1.ov4_guard(aid);
 RAISE EXCEPTION 'NEGATIVE_NOT_REJECTED';
 EXCEPTION WHEN OTHERS THEN IF SQLERRM IS DISTINCT FROM 'CONTINUATION_OWNER_STOP_SCOPE_CHANGED' THEN RAISE;END IF;END;
 BEGIN
 UPDATE population_top3000_202608_202609_v1.pipeline_control SET budget_evidence=jsonb_set(budget_evidence,'{cohort_selection_policy,policy_sha256}',to_jsonb(repeat('0',64))) WHERE singleton;
 PERFORM population_top3000_202608_202609_v1.ov4_guard(aid);
 RAISE EXCEPTION 'NEGATIVE_NOT_REJECTED';
 EXCEPTION WHEN OTHERS THEN IF SQLERRM IS DISTINCT FROM 'OV4_POLICY_CHANGED' THEN RAISE;END IF;END;
 BEGIN
 probe:=d||jsonb_build_object('bundle_sha256',repeat('0',64));
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES('REF01_OVERVIEW_FINITE_SOURCE_REVIEW_V4',jsonb_build_object('approved',true,'producer','synthetic-test-producer','reviewer','synthetic-test-reviewer','proposal_sha256',encode(sha256(convert_to(probe::text,'UTF8')),'hex'))) RETURNING event_id INTO bad_review;
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES('REF01_OVERVIEW_FINITE_SOURCE_ADMITTED_V4',probe||jsonb_build_object('review_event_id',bad_review)) RETURNING event_id INTO bad_aid;
 PERFORM population_top3000_202608_202609_v1.ov4_guard(bad_aid);
 RAISE EXCEPTION 'NEGATIVE_NOT_REJECTED';
 EXCEPTION WHEN OTHERS THEN IF SQLERRM IS DISTINCT FROM 'OV4_REVIEWED_FINITE_ADMISSION_REQUIRED' THEN RAISE;END IF;END;
 BEGIN
 probe:=d||jsonb_build_object('input_manifest_sha256',repeat('0',64));
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES('REF01_OVERVIEW_FINITE_SOURCE_REVIEW_V4',jsonb_build_object('approved',true,'producer','synthetic-test-producer','reviewer','synthetic-test-reviewer','proposal_sha256',encode(sha256(convert_to(probe::text,'UTF8')),'hex'))) RETURNING event_id INTO bad_review;
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES('REF01_OVERVIEW_FINITE_SOURCE_ADMITTED_V4',probe||jsonb_build_object('review_event_id',bad_review)) RETURNING event_id INTO bad_aid;
 PERFORM population_top3000_202608_202609_v1.ov4_guard(bad_aid);
 RAISE EXCEPTION 'NEGATIVE_NOT_REJECTED';
 EXCEPTION WHEN OTHERS THEN IF SQLERRM IS DISTINCT FROM 'OV4_REVIEWED_FINITE_ADMISSION_REQUIRED' THEN RAISE;END IF;END;
 INSERT INTO ov4_test_result VALUES('six_exact_guard_negatives','{"pass":true,"expired_source_claim":true,"completed_source_claim":true,"stale_owner":true,"stale_policy":true,"changed_bundle":true,"changed_input_manifest":true}'::jsonb);
 -- start durably queues exactly one wake; all queue effects roll back at outer boundary.
 x:=population_top3000_202608_202609_v1.ov4_start(aid);IF x->>'state'<>'ASYNC' THEN RAISE EXCEPTION 'TEST_START_FAILED %',x;END IF;
 x:=population_top3000_202608_202609_v1.ov4_start(aid);IF x->>'state'<>'ALREADY_STARTED_NO_DUPLICATE_WAKE' THEN RAISE EXCEPTION 'TEST_START_REPLAY';END IF;
 SELECT wake_token INTO tok FROM population_top3000_202608_202609_v1.ov4_batch WHERE admission=aid;
 BEGIN PERFORM population_top3000_202608_202609_v1.ov4_wake(aid,'stale',d->>'worker_sha256');RAISE EXCEPTION 'NEGATIVE_NOT_REJECTED';EXCEPTION WHEN OTHERS THEN IF SQLERRM IS DISTINCT FROM 'OV4_WAKE_AUTH_OR_CODE_REJECTED' THEN RAISE;END IF;END;
 BEGIN PERFORM population_top3000_202608_202609_v1.ov4_wake(aid,tok,repeat('0',64));RAISE EXCEPTION 'NEGATIVE_NOT_REJECTED';EXCEPTION WHEN OTHERS THEN IF SQLERRM IS DISTINCT FROM 'OV4_WAKE_AUTH_OR_CODE_REJECTED' THEN RAISE;END IF;END;
 UPDATE population_top3000_202608_202609_v1.acquisition_control SET next_provider_at=clock_timestamp()-interval '1 second' WHERE singleton;
 c:=population_top3000_202608_202609_v1.ov4_wake(aid,tok,d->>'worker_sha256');IF c->>'state'<>'CLAIMED' THEN RAISE EXCEPTION 'TEST_NEEDS_QUIET_PROVIDER_SLOT %',c;END IF;

 BEGIN
 SELECT stored_bytes,reserved_bytes INTO before_stored,before_reserved FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton;
 b:='SYNTHETIC_HTTP_404_BODY_NOT_ABSENCE';sha:=encode(sha256(convert_to(b,'UTF8')),'hex');
 s:=jsonb_build_object('body_base64',encode(convert_to(b,'UTF8'),'base64'),'sha256',sha,'bytes',octet_length(b),'http_status',404,'physical_http_count',1,'representation','FETCH_RESPONSE_BODY_BYTES_NOT_ORIGINAL_HTTP_WIRE');
 x:=population_top3000_202608_202609_v1.ov4_spool_save(c#>>'{request,request_key}',c->>'token',s::text,d->>'worker_sha256');
 x:=population_top3000_202608_202609_v1.ov4_consume(c#>>'{request,request_key}',aid);
 IF x->>'state'<>'BODY_RETAINED_QUALITY_HOLD' OR x->>'automatic_retry'<>'false' THEN RAISE EXCEPTION 'TEST_404_QUALITY_HOLD_FAILED';END IF;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.ov4_cap cp JOIN population_top3000_202608_202609_v1.pipeline_provider_lease l USING(attempt_id) JOIN population_top3000_202608_202609_v1.pipeline_attempt at USING(attempt_id) JOIN population_top3000_202608_202609_v1.pipeline_task t ON t.task_id=at.task_id WHERE cp.k=c#>>'{request,request_key}' AND l.released_at IS NOT NULL AND l.received_bytes=octet_length(b) AND l.actual_usd IS NULL AND at.outcome='BLOCKED' AND t.status='BLOCKED' AND t.blocker_code='OV4_RETAINED_SOURCE_QUALITY_HOLD' AND t.lease_until IS NULL) THEN RAISE EXCEPTION 'TEST_404_PHYSICAL_SETTLEMENT_FAILED';END IF;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton AND stored_bytes=before_stored+octet_length(b) AND reserved_bytes=before_reserved-1048576) THEN RAISE EXCEPTION 'TEST_404_COUNTER_SETTLEMENT_FAILED';END IF;
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.ov4_point) OR (SELECT dispatches FROM population_top3000_202608_202609_v1.ov4_batch WHERE admission=aid)<>1 THEN RAISE EXCEPTION 'TEST_404_FALSE_POINT_OR_NEXT_WAKE';END IF;
 x:=population_top3000_202608_202609_v1.ov4_consume(c#>>'{request,request_key}',aid);IF x->>'state'<>'BODY_RETAINED_QUALITY_ALREADY_SETTLED' THEN RAISE EXCEPTION 'TEST_404_SETTLEMENT_REPLAY_FAILED';END IF;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton AND stored_bytes=before_stored+octet_length(b) AND reserved_bytes=before_reserved-1048576) THEN RAISE EXCEPTION 'TEST_404_COUNTER_DOUBLE_SETTLEMENT';END IF;
 RAISE EXCEPTION 'ROLLBACK_SYNTHETIC_QUALITY_CASE';
 EXCEPTION WHEN OTHERS THEN IF SQLERRM IS DISTINCT FROM 'ROLLBACK_SYNTHETIC_QUALITY_CASE' THEN RAISE;END IF;END;
 INSERT INTO ov4_test_result VALUES('known404body_settlement_no_shared_stall_no_fact_no_retry','{"pass":true,"actual_usd":null,"source_facts":0,"automatic_retry":false}'::jsonb);
 b:=jsonb_build_object('status','OK','results',jsonb_build_object('ticker',c#>>'{request,ticker}','name','SYNTHETIC_FIXTURE_NOT_PROVIDER_FACT'))::text;sha:=encode(sha256(convert_to(b,'UTF8')),'hex');
 s:=jsonb_build_object('body_base64',encode(convert_to(b,'UTF8'),'base64'),'sha256',sha,'bytes',octet_length(b),'http_status',200,'physical_http_count',1,'representation','FETCH_RESPONSE_BODY_BYTES_NOT_ORIGINAL_HTTP_WIRE');
 BEGIN PERFORM population_top3000_202608_202609_v1.ov4_spool_save(c#>>'{request,request_key}','bad',s::text,d->>'worker_sha256');RAISE EXCEPTION 'NEGATIVE_NOT_REJECTED';EXCEPTION WHEN OTHERS THEN IF SQLERRM IS DISTINCT FROM 'OV4_SPOOL_CAPABILITY_REJECTED' THEN RAISE;END IF;END;
 BEGIN PERFORM population_top3000_202608_202609_v1.ov4_spool_save(c#>>'{request,request_key}',c->>'token',(s||jsonb_build_object('sha256',repeat('0',64)))::text,d->>'worker_sha256');RAISE EXCEPTION 'NEGATIVE_NOT_REJECTED';EXCEPTION WHEN OTHERS THEN IF SQLERRM IS DISTINCT FROM 'OV4_SOURCE_HASH_OR_BOUND_REJECTED' THEN RAISE;END IF;END;
 x:=population_top3000_202608_202609_v1.ov4_spool_save(c#>>'{request,request_key}',c->>'token',s::text,d->>'worker_sha256');
 x:=population_top3000_202608_202609_v1.ov4_spool_save(c#>>'{request,request_key}',c->>'token',s::text,d->>'worker_sha256');IF x->>'state'<>'BODY_ALREADY_RETAINED' THEN RAISE EXCEPTION 'TEST_SPOOL_REPLAY_FAILED';END IF;
 -- Immutable source evidence cannot be changed even by normal privileged UPDATE.
 BEGIN UPDATE population_top3000_202608_202609_v1.ov4_spool SET status=404 WHERE k=c#>>'{request,request_key}';RAISE EXCEPTION 'NEGATIVE_NOT_REJECTED';EXCEPTION WHEN OTHERS THEN IF SQLERRM IS DISTINCT FROM 'OV4_APPEND_ONLY_IMMUTABLE_EVIDENCE' THEN RAISE;END IF;END;
 -- Critical failure case: controls stop AFTER byte retention; FINISH rejects but spool survives.
 UPDATE population_top3000_202608_202609_v1.acquisition_control SET stopped=true WHERE singleton;
 BEGIN PERFORM population_top3000_202608_202609_v1.ov4_spool_save(c#>>'{request,request_key}',c->>'token',s::text,d->>'worker_sha256');RAISE EXCEPTION 'NEGATIVE_NOT_REJECTED';EXCEPTION WHEN OTHERS THEN IF SQLERRM IS DISTINCT FROM 'OV4_SPOOL_CURRENT_OWNER_STOP_POLICY_CODE_REQUIRED' THEN RAISE;END IF;END;
 BEGIN PERFORM population_top3000_202608_202609_v1.ov4_consume(c#>>'{request,request_key}',aid);RAISE EXCEPTION 'NEGATIVE_NOT_REJECTED';EXCEPTION WHEN OTHERS THEN IF SQLERRM IS DISTINCT FROM 'CONTINUATION_USER_OR_CONTROL_HOLD' THEN RAISE;END IF;END;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.ov4_spool WHERE k=c#>>'{request,request_key}' AND ov4_spool.sha=s->>'sha256') THEN RAISE EXCEPTION 'TEST_SPOOL_LOST';END IF;
 UPDATE population_top3000_202608_202609_v1.acquisition_control SET stopped=false WHERE singleton;
 BEGIN
 UPDATE population_top3000_202608_202609_v1.pipeline_task SET lease_until=clock_timestamp()-interval '1 second' WHERE task_id=(SELECT task_id FROM population_top3000_202608_202609_v1.ov4_batch WHERE admission=aid);
 PERFORM population_top3000_202608_202609_v1.ov4_consume(c#>>'{request,request_key}',aid);RAISE EXCEPTION 'NEGATIVE_NOT_REJECTED';
 EXCEPTION WHEN OTHERS THEN IF SQLERRM IS DISTINCT FROM 'OV4_ATTEMPT_TASK_CODE_BINDING' THEN RAISE;END IF;END;
 x:=population_top3000_202608_202609_v1.ov4_consume(c#>>'{request,request_key}',aid);IF x->>'state'<>'POINT_RETAINED' THEN RAISE EXCEPTION 'TEST_POSITIVE_CONSUME_FAILED %',x;END IF;
 x:=population_top3000_202608_202609_v1.ov4_consume(c#>>'{request,request_key}',aid);IF x->>'state'<>'POINT_ALREADY_RETAINED' THEN RAISE EXCEPTION 'TEST_POINT_REPLAY_FAILED';END IF;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.ov4_cap c1 JOIN population_top3000_202608_202609_v1.pipeline_attempt a USING(attempt_id) JOIN population_top3000_202608_202609_v1.pipeline_commit p USING(attempt_id) JOIN population_top3000_202608_202609_v1.pipeline_task t ON t.task_id=a.task_id WHERE c1.k=c#>>'{request,request_key}' AND a.request_id IS NOT NULL AND a.execution_number=t.executions AND a.outcome='COMPLETE' AND p.new_committed_units=1 AND t.useful_committed_units=1) THEN RAISE EXCEPTION 'TEST_ATTEMPT_TASK_COMMIT_BINDING';END IF;
 IF (SELECT dispatches FROM population_top3000_202608_202609_v1.ov4_batch WHERE admission=aid)<>2 THEN RAISE EXCEPTION 'TEST_NEXT_WAKE_NOT_DURABLE_QUEUED';END IF;
 INSERT INTO ov4_test_result VALUES('positive_spool_replay_control_failure_commit_next_wake',jsonb_build_object('pass',true,'points',1,'queued_wakes',2,'provider_calls',0,'external_http',0,'test_reservations_rolled_back',true));
 first_aid:=aid;
 FOR i IN 2..200 LOOP
 IF i=101 THEN
   INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES('ENGINEERING_IMPLEMENTATION_ASSIGNMENT_V5',sa) RETURNING event_id INTO source_assignment;
   INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES('ENGINEERING_ASSIGNMENT_CLAIMED_V5',jsonb_build_object('assignment_event_id',source_assignment,'assignment_details_sha256',encode(sha256(convert_to(sa::text,'UTF8')),'hex'),'executor',sa->>'assigned_executor','owner_id',sa->>'owner_id','owner_fence',3,'stop_generation',1,'scope_sha256',sa->>'scope_sha256','expires_at',sa->>'not_after','SYNTHETIC_ROLLBACK_ONLY',true)) RETURNING event_id INTO source_claim;
   d:=d||jsonb_build_object('execution_claim_event_id',source_claim);
   INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES('REF01_OVERVIEW_FINITE_SOURCE_REVIEW_V4',jsonb_build_object('approved',true,'producer','synthetic-test-producer','reviewer','synthetic-test-reviewer','proposal_sha256',encode(sha256(convert_to(d::text,'UTF8')),'hex'))) RETURNING event_id INTO review;
   INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES('REF01_OVERVIEW_FINITE_SOURCE_ADMITTED_V4',d||jsonb_build_object('review_event_id',review)) RETURNING event_id INTO aid;
   x:=population_top3000_202608_202609_v1.ov4_start(aid);
   SELECT wake_token INTO tok FROM population_top3000_202608_202609_v1.ov4_batch WHERE admission=aid;
 END IF;
 UPDATE population_top3000_202608_202609_v1.acquisition_control SET next_provider_at=clock_timestamp()-interval '1 second' WHERE singleton;
 c:=population_top3000_202608_202609_v1.ov4_wake(aid,tok,d->>'worker_sha256');IF c->>'state'<>'CLAIMED' THEN RAISE EXCEPTION 'TEST_200_LOOP_CLAIM_FAILED ordinal% %',i,c;END IF;
 b:=jsonb_build_object('status','OK','results',jsonb_build_object('ticker',c#>>'{request,ticker}','name','SYNTHETIC_FIXTURE_NOT_PROVIDER_FACT'))::text;sha:=encode(sha256(convert_to(b,'UTF8')),'hex');
 s:=jsonb_build_object('body_base64',encode(convert_to(b,'UTF8'),'base64'),'sha256',sha,'bytes',octet_length(b),'http_status',200,'physical_http_count',1,'representation','FETCH_RESPONSE_BODY_BYTES_NOT_ORIGINAL_HTTP_WIRE');
 x:=population_top3000_202608_202609_v1.ov4_spool_save(c#>>'{request,request_key}',c->>'token',s::text,d->>'worker_sha256');
 x:=population_top3000_202608_202609_v1.ov4_consume(c#>>'{request,request_key}',aid);IF x->>'state'<>'POINT_RETAINED' THEN RAISE EXCEPTION 'TEST_200_LOOP_CONSUME_FAILED ordinal% %',i,x;END IF;
 IF i=101 AND NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.ov4_cap cp JOIN population_top3000_202608_202609_v1.pipeline_attempt at USING(attempt_id) WHERE cp.k=c#>>'{request,request_key}' AND at.execution_number=1 AND at.request_id IS NOT NULL) THEN RAISE EXCEPTION 'TEST_SECOND_TRANCHE_EXECUTION_OR_DISPATCH_BINDING';END IF;
 END LOOP;
 IF (SELECT count(*) FROM population_top3000_202608_202609_v1.ov4_point)<>200 OR (SELECT count(*) FROM population_top3000_202608_202609_v1.ov4_spool)<>200 OR (SELECT sum(dispatches) FROM population_top3000_202608_202609_v1.ov4_batch)<>200 THEN RAISE EXCEPTION 'TEST_200_COUNTS_FAILED';END IF;
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.ov4_point WHERE point->>'intervals_qualified'<>'false' OR point->>'family_accepted'<>'false' OR point->>'original_historical_receipt_proven'<>'false') THEN RAISE EXCEPTION 'TEST_FALSE_QUALIFICATION';END IF;
 INSERT INTO ov4_test_result VALUES('200_points_two_finite_tranches',jsonb_build_object('pass',true,'synthetic_points',200,'genuine_rollback_attempts',200,'genuine_rollback_commits',200,'uncommitted_internal_wakes',200,'second_tranche_first_execution',1,'actual_provider_calls',0,'actual_external_http',0,'source_facts_qualified',false));

END;$t$;
SELECT * FROM ov4_test_result;
-- Caller ROLLBACK mandatory. Separate two-transaction spool durability test remains required.
