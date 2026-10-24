-- BEGIN; exact classifier + dispatcher definitions; exact classification registration; then fixture; ROLLBACK.
SET LOCAL lock_timeout='8s';SET LOCAL statement_timeout='60s';
CREATE FUNCTION population_top3000_202608_202609_v1.continuation_fixture_source_successors_v7(text,bigint,bigint)
RETURNS jsonb LANGUAGE plpgsql SET search_path=pg_catalog AS $f$
BEGIN
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES('ROLLBACK_SOURCE_SUCCESSOR_CALLED_V7','{"fixture_only":true}'::jsonb);
 RETURN '{"state":"FIXTURE_SUCCESSOR_EMITTED"}'::jsonb;
END;$f$;
CREATE TEMP TABLE continuation_v7_actual_receipt(receipt jsonb) ON COMMIT DROP;
DO $test$
DECLARE a uuid:=gen_random_uuid();b uuid:=gen_random_uuid();ctl jsonb;before_ctl jsonb;after_ctl jsonb;h text;ev bigint;n integer;installed jsonb;wronghash boolean:=false;r jsonb;
BEGIN
 ctl:=population_top3000_202608_202609_v1.continuation_guard_v6('top3000-completion-20261024-root-5db48b32d103',3,1);
 SELECT to_jsonb(ac)||jsonb_build_object('pipeline',to_jsonb(pc)) INTO before_ctl FROM population_top3000_202608_202609_v1.acquisition_control ac CROSS JOIN population_top3000_202608_202609_v1.pipeline_control pc WHERE ac.singleton AND pc.singleton;
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE status='LEASED') THEN RAISE EXCEPTION 'DELTA_FIXTURE_ACTIVE_TASK_RETRY';END IF;
 IF population_top3000_202608_202609_v1.continuation_classify_v6('CTX14','ACQUIRE','CTX14_AUTHENTIC_SETTLEMENT_SOURCE_RESOLUTION_REQUIRED')<>'SOURCE_RECOVERY' OR population_top3000_202608_202609_v1.continuation_classify_v6('CTX14','ACQUIRE','CTX14_AUTH_REQUIRED')<>'EXTERNAL_CONDITION' OR population_top3000_202608_202609_v1.continuation_classify_v6('CTX07','ACQUIRE','CTX07_V6_ENFORCED_ACCESS_DENIAL')<>'EXTERNAL_CONDITION' OR population_top3000_202608_202609_v1.continuation_classify_v6('CTX07','ACQUIRE','AUTHENTICATION_REQUIRED')<>'EXTERNAL_CONDITION' OR population_top3000_202608_202609_v1.continuation_classify_v6('CTX07','ACQUIRE','UNKNOWN_HTTP_OUTCOME')<>'EXTERNAL_CONDITION' THEN RAISE EXCEPTION 'AUTHENTIC_V_AUTH_BOUNDARY_FAILED';END IF;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.continuation_clear_queue_v6 WHERE clearing_id='3b7a37a6a455cbe44834d1031c6284089954f11e12adde175cc32320e48499fc' AND status='SUPERSEDED' AND attempts=0) OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.continuation_clear_queue_v6 WHERE clearing_id='3b7a37a6a455cbe44834d1031c6284089954f11e12adde175cc32320e48499fc:classification-v7' AND status='READY' AND kind='SOURCE_RECOVERY') THEN RAISE EXCEPTION 'EXACT_CLASSIFICATION_SUCCESSOR_REQUIRED';END IF;
 -- Prevent a live original boundary from being serviced during this fixture; all source rows/checkpoints are restored by rollback.
 UPDATE population_top3000_202608_202609_v1.pipeline_task SET status='BLOCKED',blocker_code='ROLLBACK_ORIGINAL_PAUSE',resumption_condition='{"fixture_only":true}'::jsonb WHERE task_id='d377d980-5dbd-4276-a2c9-d5638c60b668';
 INSERT INTO population_top3000_202608_202609_v1.pipeline_task(task_id,operation_key,family_id,stage,partition_key,input_versions,status,blocker_code,resumption_condition)
 VALUES(a,a::text,'CTX02','ACQUIRE','rollback-new-date-a','{"fixture_only":true}'::jsonb,'BLOCKED','CTX02_CENSUS_PAGE_CAP_REVIEW','{"fixture_only":true}'::jsonb),(b,b::text,'CTX02','ACQUIRE','rollback-new-date-b','{"fixture_only":true}'::jsonb,'BLOCKED','CTX02_CENSUS_PAGE_CAP_REVIEW','{"fixture_only":true}'::jsonb);
 h:=encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.continuation_fixture_source_successors_v7(text,bigint,bigint)'::regprocedure),'UTF8')),'hex');
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES('ROLLBACK_SOURCE_HANDLER_REVIEW_V7',ctl||jsonb_build_object('approved',true,'handler_function_sha256',h,'producer','synthetic-producer','reviewer','synthetic-distinct-reviewer')) RETURNING event_id INTO ev;
 INSERT INTO population_top3000_202608_202609_v1.continuation_handler_v6(kind,function_oid,function_sha256,review_event_id,bounds,enabled) VALUES('CTX07_SOURCE_SUCCESSORS','population_top3000_202608_202609_v1.continuation_fixture_source_successors_v7(text,bigint,bigint)'::regprocedure,h,ev,'{"provider_calls":0,"max_per_pass":1}'::jsonb,true) ON CONFLICT(kind) DO UPDATE SET function_oid=excluded.function_oid,function_sha256=excluded.function_sha256,review_event_id=excluded.review_event_id,bounds=excluded.bounds,enabled=true;
 UPDATE population_top3000_202608_202609_v1.pipeline_event SET at=clock_timestamp()-interval '2minutes' WHERE event='CONTINUATION_DISPATCH_PASS_V6';
 r:=population_top3000_202608_202609_v1.continuation_dispatch_v6(ctl->>'owner_id',3,1);
 SELECT count(*) INTO n FROM population_top3000_202608_202609_v1.continuation_clear_queue_v6 WHERE status='READY' AND kind='OPERATIONAL_REVIEW' AND jsonb_array_length(input_manifest)=1 AND input_manifest->0->>'task_id' IN(a::text,b::text) AND attempts=0;
 IF n<>2 OR (SELECT count(*) FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id IN(a,b) AND status='BLOCKED')<>2 THEN RAISE EXCEPTION 'MULTI_OPTIONS_TASK_ISOLATION_FAILED';END IF;
 IF (SELECT count(*) FROM population_top3000_202608_202609_v1.pipeline_event WHERE event='ROLLBACK_SOURCE_SUCCESSOR_CALLED_V7')<>1 THEN RAISE EXCEPTION 'HASH_PINNED_SOURCE_HOOK_NOT_RUN';END IF;
 PERFORM population_top3000_202608_202609_v1.continuation_dispatch_v6(ctl->>'owner_id',3,1);
 IF (SELECT count(*) FROM population_top3000_202608_202609_v1.pipeline_event WHERE event='ROLLBACK_SOURCE_SUCCESSOR_CALLED_V7')<>1 THEN RAISE EXCEPTION 'SOURCE_HOOK_RATE_LIMIT_FAILED';END IF;
 UPDATE population_top3000_202608_202609_v1.pipeline_event SET at=clock_timestamp()-interval '2minutes' WHERE event='CONTINUATION_DISPATCH_PASS_V6';
 PERFORM population_top3000_202608_202609_v1.continuation_dispatch_v6(ctl->>'owner_id',3,1);
 IF (SELECT count(*) FROM population_top3000_202608_202609_v1.continuation_clear_queue_v6 WHERE status='READY' AND kind='OPERATIONAL_REVIEW' AND jsonb_array_length(input_manifest)=1 AND input_manifest->0->>'task_id' IN(a::text,b::text) AND attempts=0)<>2 THEN RAISE EXCEPTION 'SIBLING_TASK_SUPERSESSION_REGRESSION';END IF;
 IF (SELECT count(*) FROM population_top3000_202608_202609_v1.pipeline_event WHERE event='ROLLBACK_SOURCE_SUCCESSOR_CALLED_V7')<>2 THEN RAISE EXCEPTION 'SOURCE_HOOK_SECOND_PASS_FAILED';END IF;
 BEGIN
 UPDATE population_top3000_202608_202609_v1.pipeline_event SET at=clock_timestamp()-interval '2minutes' WHERE event='CONTINUATION_DISPATCH_PASS_V6';
 UPDATE population_top3000_202608_202609_v1.continuation_handler_v6 SET function_sha256=repeat('0',64) WHERE kind='CTX07_SOURCE_SUCCESSORS';
 PERFORM population_top3000_202608_202609_v1.continuation_dispatch_v6(ctl->>'owner_id',3,1);RAISE EXCEPTION 'WRONG_HANDLER_HASH_ACCEPTED';
 EXCEPTION WHEN raise_exception THEN IF SQLERRM='CONTINUATION_SOURCE_SUCCESSOR_HANDLER_CHANGED' THEN wronghash:=true;ELSE RAISE;END IF;END;
 SELECT to_jsonb(ac)||jsonb_build_object('pipeline',to_jsonb(pc)) INTO after_ctl FROM population_top3000_202608_202609_v1.acquisition_control ac CROSS JOIN population_top3000_202608_202609_v1.pipeline_control pc WHERE ac.singleton AND pc.singleton;
 IF before_ctl IS DISTINCT FROM after_ctl THEN RAISE EXCEPTION 'DELTA_FIXTURE_CONTROLS_CHANGED';END IF;
 SELECT jsonb_object_agg(p.oid::regprocedure::text,encode(sha256(convert_to(pg_get_functiondef(p.oid),'UTF8')),'hex')) INTO installed FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='population_top3000_202608_202609_v1' AND p.proname IN('continuation_classify_v6','continuation_dispatch_v6');
 INSERT INTO continuation_v7_actual_receipt VALUES(jsonb_build_object('result','PASS_CONTINUATION_DELTA_V7_ROLLBACK','installed_functions',installed,'authentic_source_is_recoverable',true,'access_denial_and_unknown_remain_external',true,'new_source_dates_isolated',n,'unreviewed_task_auto_admission',false,'source_successor_hook_once_per_pass',true,'sibling_tasks_ready_after_two_passes',true,'wrong_helper_hash_rejected',wronghash,'control_counters_and_deadline_unchanged',true,'provider_calls',0,'all_changes_rolled_back',true));
END;$test$;
SELECT receipt FROM continuation_v7_actual_receipt;
ROLLBACK;
