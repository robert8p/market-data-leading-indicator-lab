-- Installed-code behavior proof only: no DDL, no external operation, no permanent history change.
BEGIN;
SET LOCAL lock_timeout='8s';
SET LOCAL statement_timeout='30s';
DO $fixture$
DECLARE ctl jsonb;before_ctl jsonb;simulated_ctl jsonb;after_ctl jsonb;
 simulated_last timestamptz:=clock_timestamp()-interval '11minutes';original_last timestamptz;
 dispatch_result jsonb;finding jsonb;event_floor bigint;dispatch_ids bigint[];n integer;
 installed_hash text;ready_count bigint;ready_data_count bigint;active_count bigint;
BEGIN
 ctl:=population_top3000_202608_202609_v1.continuation_guard_v6('top3000-completion-20261024-root-5db48b32d103',3,1);
 installed_hash:=encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.continuation_dispatch_v6(text,bigint,bigint)'::regprocedure),'UTF8')),'hex');
 IF installed_hash IS DISTINCT FROM '9f2c2c2dbab45940b98a698db1f5774122094cddc606fd80d91abf2e7541f38a' THEN RAISE EXCEPTION 'NO_USEFUL_FIXTURE_DISPATCH_PREIMAGE_CHANGED';END IF;
 IF NOT EXISTS(SELECT 1 FROM cron.job WHERE jobid=596 AND active AND command='select population_top3000_202608_202609_v1.pipeline_tick_v1();') THEN RAISE EXCEPTION 'NO_USEFUL_FIXTURE_CURRENT_SCHEDULER_NOT_ACTIVE';END IF;
 -- Do not bypass legitimate admission, useful-progress deduplication, provider or handler guards.
 IF (ctl->>'not_after')::timestamptz<=clock_timestamp()+interval '31minutes' THEN RAISE EXCEPTION 'NO_USEFUL_FIXTURE_ADMISSION_RENEWAL_WINDOW_RETRY';END IF;
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_event WHERE event='CONTINUATION_NO_USEFUL_PROGRESS_V6' AND at>clock_timestamp()-interval '10minutes') THEN RAISE EXCEPTION 'NO_USEFUL_FIXTURE_RECENT_DETECTION_ALREADY_EXISTS';END IF;
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.continuation_clear_queue_v6 WHERE status='READY' AND kind='OPERATIONAL_REVIEW' AND family_id='CTX02' AND blocker_code='CTX02_CENSUS_PAGE_CAP_REVIEW' AND jsonb_array_length(input_manifest)=1 AND input_manifest->0->>'task_id'='d377d980-5dbd-4276-a2c9-d5638c60b668' AND not_after>clock_timestamp() AND attempts<max_attempts) THEN RAISE EXCEPTION 'NO_USEFUL_FIXTURE_REAL_OPTIONS_CLEARING_PENDING_RETRY';END IF;
 SELECT to_jsonb(a)||jsonb_build_object('pipeline',to_jsonb(p)) INTO before_ctl FROM population_top3000_202608_202609_v1.acquisition_control a CROSS JOIN population_top3000_202608_202609_v1.pipeline_control p WHERE a.singleton AND p.singleton;
 original_last:=(before_ctl#>>'{pipeline,last_useful_progress_at}')::timestamptz;
 SELECT coalesce(max(event_id),0) INTO event_floor FROM population_top3000_202608_202609_v1.pipeline_event;
 -- Exactly the recent (at most two) cadence gate receipts move inside this rollback transaction only.
 SELECT array_agg(event_id ORDER BY event_id) INTO dispatch_ids FROM population_top3000_202608_202609_v1.pipeline_event WHERE event='CONTINUATION_DISPATCH_PASS_V6' AND at>clock_timestamp()-interval '60seconds';
 IF coalesce(cardinality(dispatch_ids),0)>2 THEN RAISE EXCEPTION 'NO_USEFUL_FIXTURE_DISPATCH_RATE_PREIMAGE_UNEXPECTED';END IF;
 UPDATE population_top3000_202608_202609_v1.pipeline_event SET at=clock_timestamp()-interval '61seconds' WHERE event_id=ANY(coalesce(dispatch_ids,'{}'::bigint[])) AND event='CONTINUATION_DISPATCH_PASS_V6';
 UPDATE population_top3000_202608_202609_v1.pipeline_control SET last_useful_progress_at=simulated_last WHERE singleton;
 SELECT to_jsonb(a)||jsonb_build_object('pipeline',to_jsonb(p)) INTO simulated_ctl FROM population_top3000_202608_202609_v1.acquisition_control a CROSS JOIN population_top3000_202608_202609_v1.pipeline_control p WHERE a.singleton AND p.singleton;
 dispatch_result:=population_top3000_202608_202609_v1.continuation_dispatch_v6(ctl->>'owner_id',3,1);
 IF dispatch_result->>'state' IS DISTINCT FROM 'CLEARING_WORK_DISPATCHED' OR dispatch_result::text LIKE '%ALREADY_ACTIVE%' THEN RAISE EXCEPTION 'NO_USEFUL_FIXTURE_NO_CLASSIFICATION_PASS';END IF;
 SELECT count(*) INTO n FROM population_top3000_202608_202609_v1.pipeline_event WHERE event='CONTINUATION_NO_USEFUL_PROGRESS_V6' AND event_id>event_floor;
 IF n<>1 THEN RAISE EXCEPTION 'NO_USEFUL_FIXTURE_EXPECTED_ONE_DETECTION_GOT_%',n;END IF;
 SELECT details INTO STRICT finding FROM population_top3000_202608_202609_v1.pipeline_event WHERE event='CONTINUATION_NO_USEFUL_PROGRESS_V6' AND event_id>event_floor;
 SELECT count(*) INTO ready_count FROM population_top3000_202608_202609_v1.continuation_clear_queue_v6 WHERE status='READY';
 SELECT count(*) INTO ready_data_count FROM population_top3000_202608_202609_v1.pipeline_task WHERE status='READY';
 SELECT count(*) INTO active_count FROM population_top3000_202608_202609_v1.pipeline_task WHERE status='LEASED';
 IF finding->>'scheduler_is_not_progress' IS DISTINCT FROM 'true' OR (finding->>'last_useful_progress_at')::timestamptz IS DISTINCT FROM simulated_last OR finding->>'ready_clearing_groups' IS DISTINCT FROM ready_count::text OR ready_count=0 OR finding->>'ready_data_tasks' IS DISTINCT FROM ready_data_count::text OR finding->>'active_attempts' IS DISTINCT FROM active_count::text OR finding::text LIKE '%ALREADY_ACTIVE%' THEN RAISE EXCEPTION 'NO_USEFUL_FIXTURE_FALSE_HEALTH_OR_INEXACT_COUNTS';END IF;
 SELECT to_jsonb(a)||jsonb_build_object('pipeline',to_jsonb(p)) INTO after_ctl FROM population_top3000_202608_202609_v1.acquisition_control a CROSS JOIN population_top3000_202608_202609_v1.pipeline_control p WHERE a.singleton AND p.singleton;
 IF after_ctl IS DISTINCT FROM simulated_ctl THEN RAISE EXCEPTION 'NO_USEFUL_FIXTURE_DISPATCH_CHANGED_CONTROL_COUNTERS';END IF;
 -- Immediate second pass must preserve the normal one-minute dispatch throttle and single detection.
 dispatch_result:=population_top3000_202608_202609_v1.continuation_dispatch_v6(ctl->>'owner_id',3,1);
 IF dispatch_result->>'state' IS DISTINCT FROM 'BOUNDED_RECHECK_NOT_DUE' OR (SELECT count(*) FROM population_top3000_202608_202609_v1.pipeline_event WHERE event='CONTINUATION_NO_USEFUL_PROGRESS_V6' AND event_id>event_floor)<>1 THEN RAISE EXCEPTION 'NO_USEFUL_FIXTURE_DUPLICATE_DETECTION';END IF;
 UPDATE population_top3000_202608_202609_v1.pipeline_control SET last_useful_progress_at=original_last WHERE singleton;
 SELECT to_jsonb(a)||jsonb_build_object('pipeline',to_jsonb(p)) INTO after_ctl FROM population_top3000_202608_202609_v1.acquisition_control a CROSS JOIN population_top3000_202608_202609_v1.pipeline_control p WHERE a.singleton AND p.singleton;
 IF after_ctl IS DISTINCT FROM before_ctl THEN RAISE EXCEPTION 'NO_USEFUL_FIXTURE_ORIGINAL_CONTROLS_NOT_RESTORED';END IF;
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES('ROLLBACK_NO_USEFUL_PROGRESS_PROOF_V7',ctl||jsonb_build_object('result','PASS_INSTALLED_NO_USEFUL_PROGRESS_V7','dispatcher_sha256',installed_hash,'scheduler_active',true,'simulated_no_useful_minutes',11,'scheduler_is_not_progress',true,'ready_clearing_groups',ready_count,'ready_data_tasks',ready_data_count,'active_attempts',active_count,'detection_receipt',finding,'gate_receipts_adjusted',coalesce(cardinality(dispatch_ids),0),'normal_second_pass_throttled',true,'control_counters_and_original_deadline_unchanged',true,'provider_calls',0,'http_invocations',0,'ddl_statements',0,'all_changes_rolled_back',true));
END;
$fixture$;
SELECT details FROM population_top3000_202608_202609_v1.pipeline_event WHERE event='ROLLBACK_NO_USEFUL_PROGRESS_PROOF_V7' ORDER BY event_id DESC LIMIT 1;
ROLLBACK;
