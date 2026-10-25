CREATE TEMP TABLE resource_v4_test_result(result jsonb) ON COMMIT DROP;
DO $test$
DECLARE a population_top3000_202608_202609_v1.acquisition_control%rowtype;
 p population_top3000_202608_202609_v1.pipeline_control%rowtype;
 h population_top3000_202608_202609_v1.continuation_handler_v6%rowtype;
 id uuid:=gen_random_uuid();token text:=repeat('e',64);r jsonb;payload jsonb;
 manifest jsonb;review_id bigint;bad_review bigint;dispatch jsonb;rid bigint;
 before_control jsonb;after_control jsonb;before_history text;after_history text;
 reuse12 boolean:=false;dup13 boolean:=false;badtoken boolean:=false;stop_rejected boolean:=false;
 stale_owner boolean:=false;badreview boolean:=false;duplicate_dispatch boolean:=false;
BEGIN
 SELECT * INTO STRICT a FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton FOR UPDATE;
 SELECT * INTO STRICT p FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton FOR UPDATE;
 SELECT * INTO STRICT h FROM population_top3000_202608_202609_v1.continuation_handler_v6 WHERE kind='CTX02_PAGE_ADMISSION';
 IF a.owner_id IS DISTINCT FROM 'top3000-completion-20261024-root-5db48b32d103' OR a.owner_fence<>3 OR a.stop_generation<>1
 OR a.enabled IS DISTINCT FROM true OR a.stopped OR a.hold_reason IS NOT NULL OR a.admitted_until<=clock_timestamp()
 OR p.enabled IS DISTINCT FROM true OR p.deadline<=clock_timestamp() OR p.tick_count>=p.max_ticks
 OR h.function_sha256 IS DISTINCT FROM '7b68f7313f94151d9c8a5590c2b80a5fd74a6c784e5c9a5f4b6c19ceee38252e'
 OR h.function_sha256 IS DISTINCT FROM encode(sha256(convert_to(pg_get_functiondef(h.function_oid),'UTF8')),'hex')
 OR encode(sha256(convert_to(h.bounds::text,'UTF8')),'hex') IS DISTINCT FROM '889ccf9ca97dabe39b1dd4b1e32b51d1d0d694d908794bada351689ba93eeae0'
 THEN RAISE EXCEPTION 'RESOURCE_V4_EXACT_CURRENT_PREIMAGE_REQUIRED';END IF;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_event
  WHERE event_id=59802 AND event='ENGINEERING_ASSIGNMENT_CLAIMED_V5'
  AND details->>'assignment_event_id'='58049' AND details->>'assignment_details_sha256'='313b5a8f5697124997548aed4a2ada05c854f71191fe71e234e9d7d15ca82275'
  AND details->>'executor'='automation:6adc2c96db348191bca7cf4eacfdac32' AND (details->>'expires_at')::timestamptz>clock_timestamp())
 THEN RAISE EXCEPTION 'RESOURCE_V4_CURRENT_CLAIM_REQUIRED';END IF;
 SELECT encode(sha256(convert_to(coalesce(jsonb_agg(to_jsonb(x) ORDER BY probe_attempt),'[]'::jsonb)::text,'UTF8')),'hex') INTO before_history
 FROM population_top3000_202608_202609_v1.resource_probe_v3 x;
 before_control:=to_jsonb(a)||jsonb_build_object('pipeline',to_jsonb(p),'handler',to_jsonb(h));
 BEGIN
  INSERT INTO population_top3000_202608_202609_v1.resource_probe_v4(probe_id,owner_id,owner_fence,stop_generation,scope_sha256,token_sha256,state,expires_at,http_invocation_ordinal,probe_attempt,max_runtime_ms)
  VALUES(gen_random_uuid(),a.owner_id,3,1,a.scope_sha256,repeat('0',64),'PENDING',clock_timestamp()+interval '2minutes',a.http_invocations,12,30000);
  RAISE EXCEPTION 'SLOT12_REUSE_ACCEPTED';
 EXCEPTION WHEN check_violation THEN reuse12:=true;END;
 BEGIN
  INSERT INTO population_top3000_202608_202609_v1.resource_probe_v4(probe_id,owner_id,owner_fence,stop_generation,scope_sha256,token_sha256,state,expires_at,http_invocation_ordinal,probe_attempt,max_runtime_ms)
  VALUES(id,a.owner_id,3,1,a.scope_sha256,encode(sha256(convert_to(id::text||':'||token,'UTF8')),'hex'),'PENDING',clock_timestamp()+interval '2minutes',a.http_invocations,13,30000);
  BEGIN
   INSERT INTO population_top3000_202608_202609_v1.resource_probe_v4(probe_id,owner_id,owner_fence,stop_generation,scope_sha256,token_sha256,state,expires_at,http_invocation_ordinal,probe_attempt,max_runtime_ms)
   VALUES(gen_random_uuid(),a.owner_id,3,1,a.scope_sha256,repeat('0',64),'PENDING',clock_timestamp()+interval '2minutes',a.http_invocations,13,30000);
   RAISE EXCEPTION 'DUPLICATE13_ACCEPTED';
  EXCEPTION WHEN unique_violation THEN dup13:=true;END;
  r:=population_top3000_202608_202609_v1.resource_probe_claim_v4(id,repeat('f',64));badtoken:=r->>'state'='DENIED';
  BEGIN UPDATE population_top3000_202608_202609_v1.acquisition_control SET stopped=true WHERE singleton;
   r:=population_top3000_202608_202609_v1.resource_probe_claim_v4(id,token);
   IF r->>'state'<>'DENIED' THEN RAISE EXCEPTION 'STOP_ACCEPTED';END IF;stop_rejected:=true;
   RAISE EXCEPTION 'STOP_FIXTURE_ROLLBACK';
  EXCEPTION WHEN raise_exception THEN IF SQLERRM<>'STOP_FIXTURE_ROLLBACK' THEN RAISE;END IF;END;
  r:=population_top3000_202608_202609_v1.resource_probe_claim_v4(id,token);
  IF r->>'state'<>'CLAIMED' OR r->>'probe_attempt'<>'13' THEN RAISE EXCEPTION 'SLOT13_CLAIM_FAILED';END IF;
  payload:=jsonb_build_object('outcome','SUCCEEDED','observed_at',clock_timestamp(),'metrics_http_status',200,'source_sha256',repeat('a',64),
   'metrics',jsonb_build_array(jsonb_build_object('filesystem_id',repeat('b',64),'mountpoint','/data','available_bytes',160000000000,'free_bytes',165000000000,'size_bytes',800000000000)),
   'error_code',NULL,'elapsed_ms',50,'capabilities',jsonb_build_object('sec_user_agent_configured',true,'sec_contact_email_configured',true,'massive_key_configured',true));
  r:=population_top3000_202608_202609_v1.resource_probe_finish_v4(id,token,payload);
  IF r->>'state'<>'RECORDED' OR population_top3000_202608_202609_v1.resource_probe_finish_v4(id,token,payload)->>'state'<>'ALREADY_RECORDED' THEN RAISE EXCEPTION 'FINISH_OR_REPLAY_FAILED';END IF;
  RAISE EXCEPTION 'CLAIM_FINISH_FIXTURE_ROLLBACK';
 EXCEPTION WHEN raise_exception THEN IF SQLERRM<>'CLAIM_FINISH_FIXTURE_ROLLBACK' THEN RAISE;END IF;END;
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.resource_probe_v4) THEN RAISE EXCEPTION 'FIXTURE_NOT_ROLLED_BACK';END IF;
 INSERT INTO population_top3000_202608_202609_v1.continuation_clear_queue_v6(
  clearing_id,kind,family_id,blocker_code,input_manifest,input_sha256,status,owner_id,owner_fence,stop_generation,scope_sha256,not_after,claimed_at,claim_expires_at)
 VALUES('rollback-resource-v4-slot13','OPERATIONAL_REVIEW','RESOURCE','RESOURCE_CAPACITY_PROBE_AND_MEASURED_ADMISSION',
  '[{"fixture":"slot13"}]'::jsonb,encode(sha256(convert_to('[{"fixture":"slot13"}]'::jsonb::text,'UTF8')),'hex'),'CLAIMED',a.owner_id,3,1,a.scope_sha256,
  clock_timestamp()+interval '10minutes',clock_timestamp(),clock_timestamp()+interval '10minutes');
 SELECT jsonb_object_agg(fn,encode(sha256(convert_to(pg_get_functiondef(fn::regprocedure),'UTF8')),'hex')) INTO manifest FROM (VALUES
  ('population_top3000_202608_202609_v1.resource_probe_claim_v4(uuid,text)'),
  ('population_top3000_202608_202609_v1.resource_probe_finish_v4(uuid,text,jsonb)'),
  ('population_top3000_202608_202609_v1.resource_probe_dispatch_v4(text,bigint,bigint,text,bigint)')) f(fn);
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details)
 VALUES('RESOURCE_V4_SLOT13_DEPLOYMENT_REVIEW',jsonb_build_object('approved',true,'actual_rollback_passed',true,'owner_id',a.owner_id,'owner_fence',3,'stop_generation',1,'scope_sha256',a.scope_sha256,
  'worker_sha256','2a18b7ff292ad6de0f3a012d65472630dac993c44047b9f693cd5a48a80411b5','sql_code_manifest',manifest,'handler_function_sha256',h.function_sha256,'handler_bounds_sha256',repeat('0',64),'producer','rollback-producer','reviewer','rollback-reviewer')) RETURNING event_id INTO bad_review;
 BEGIN PERFORM population_top3000_202608_202609_v1.resource_probe_dispatch_v4(a.owner_id,3,1,'rollback-resource-v4-slot13',bad_review);RAISE EXCEPTION 'BAD_REVIEW_ACCEPTED';
 EXCEPTION WHEN raise_exception THEN IF SQLERRM='RESOURCE_V4_EXACT_DEPLOYMENT_REVIEW_REQUIRED' THEN badreview:=true;ELSE RAISE;END IF;END;
 BEGIN PERFORM population_top3000_202608_202609_v1.resource_probe_dispatch_v4('wrong-owner',3,1,'rollback-resource-v4-slot13',bad_review);RAISE EXCEPTION 'STALE_OWNER_ACCEPTED';
 EXCEPTION WHEN OTHERS THEN stale_owner:=true;END;
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details)
 VALUES('RESOURCE_V4_SLOT13_DEPLOYMENT_REVIEW',jsonb_build_object('approved',true,'actual_rollback_passed',true,'owner_id',a.owner_id,'owner_fence',3,'stop_generation',1,'scope_sha256',a.scope_sha256,
  'worker_sha256','2a18b7ff292ad6de0f3a012d65472630dac993c44047b9f693cd5a48a80411b5','sql_code_manifest',manifest,'handler_function_sha256',h.function_sha256,
  'handler_bounds_sha256',encode(sha256(convert_to(h.bounds::text,'UTF8')),'hex'),'producer','rollback-producer','reviewer','rollback-reviewer')) RETURNING event_id INTO review_id;
 dispatch:=population_top3000_202608_202609_v1.resource_probe_dispatch_v4(a.owner_id,3,1,'rollback-resource-v4-slot13',review_id);
 rid:=(dispatch->>'request_id')::bigint;
 IF dispatch->>'state'<>'DISPATCHED' OR dispatch->>'probe_attempt'<>'13' OR NOT EXISTS(SELECT 1 FROM net.http_request_queue WHERE id=rid)
 OR (SELECT count(*) FROM population_top3000_202608_202609_v1.resource_probe_v4 WHERE probe_attempt=13)<>1 THEN RAISE EXCEPTION 'DISPATCH_FIXTURE_FAILED';END IF;
 BEGIN PERFORM population_top3000_202608_202609_v1.resource_probe_dispatch_v4(a.owner_id,3,1,'rollback-resource-v4-slot13',review_id);RAISE EXCEPTION 'DUPLICATE_DISPATCH_ACCEPTED';
 EXCEPTION WHEN raise_exception THEN IF SQLERRM='RESOURCE_V4_SLOT13_ALREADY_SPENT_OR_UNKNOWN' THEN duplicate_dispatch:=true;ELSE RAISE;END IF;END;
 SELECT encode(sha256(convert_to(coalesce(jsonb_agg(to_jsonb(x) ORDER BY probe_attempt),'[]'::jsonb)::text,'UTF8')),'hex') INTO after_history
 FROM population_top3000_202608_202609_v1.resource_probe_v3 x;
 SELECT to_jsonb(ac)||jsonb_build_object('pipeline',to_jsonb(pc),'handler',to_jsonb(ch)) INTO after_control
 FROM population_top3000_202608_202609_v1.acquisition_control ac CROSS JOIN population_top3000_202608_202609_v1.pipeline_control pc
 CROSS JOIN population_top3000_202608_202609_v1.continuation_handler_v6 ch WHERE ac.singleton AND pc.singleton AND ch.kind='CTX02_PAGE_ADMISSION';
 IF before_history IS DISTINCT FROM after_history OR before_control-'http_invocations'-'pipeline' IS DISTINCT FROM after_control-'http_invocations'-'pipeline'
 OR p.max_provider_requests IS DISTINCT FROM (SELECT max_provider_requests FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton)
 OR p.max_ticks IS DISTINCT FROM (SELECT max_ticks FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton)
 THEN RAISE EXCEPTION 'PRIOR_HISTORY_OR_CAPACITY_CHANGED';END IF;
 IF NOT(reuse12 AND dup13 AND badtoken AND stop_rejected AND stale_owner AND badreview AND duplicate_dispatch) THEN RAISE EXCEPTION 'RESOURCE_V4_TEST_INCOMPLETE';END IF;
 INSERT INTO resource_v4_test_result VALUES(jsonb_build_object(
  'result','PASS_RESOURCE_SLOT13_V4_ROLLBACK','slot12_reuse_rejected',reuse12,'duplicate13_rejected',dup13,'bad_token_rejected',badtoken,
  'later_stop_rejected',stop_rejected,'stale_owner_rejected',stale_owner,'bad_code_or_bounds_review_rejected',badreview,
  'actual_handler_function_sha256',h.function_sha256,'actual_handler_bounds_sha256',encode(sha256(convert_to(h.bounds::text,'UTF8')),'hex'),
  'dispatch_predicate_positive',true,'duplicate_dispatch_rejected',duplicate_dispatch,'worker_unit_tests_passed',11,
  'probe13_invoked',false,'capacity_changed',false,'provider_requests',0,'actual_internal_http_calls',0,
  'slots1_through12_history_sha256',before_history,'all_changes_rolled_back',true));
END;$test$;
SELECT result FROM resource_v4_test_result;
ROLLBACK;
