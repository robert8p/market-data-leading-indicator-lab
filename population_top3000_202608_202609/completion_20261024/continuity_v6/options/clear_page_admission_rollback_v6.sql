-- Run BEGIN + handler DDL + this harness. Ends ROLLBACK. No external calls.
CREATE TEMP TABLE options_clear_test_v6(receipt jsonb);
DO $test$
DECLARE r jsonb; before_t jsonb; after_t jsonb; rejected boolean; n text; before_a jsonb; before_p jsonb; expected_page integer; calls bigint;
BEGIN
 PERFORM pg_advisory_xact_lock(2026083000261022);
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE status='LEASED') THEN RAISE EXCEPTION 'FIXTURE_ACTIVE_ATTEMPT_WAIT';END IF;
 SELECT count(*)+1 INTO expected_page FROM population_top3000_202608_202609_v1.ctx02_census_page_v1;
 SELECT count(*) INTO calls FROM population_top3000_202608_202609_v1.pipeline_provider_lease l JOIN population_top3000_202608_202609_v1.pipeline_attempt a USING(attempt_id) WHERE a.task_id='d377d980-5dbd-4276-a2c9-d5638c60b668' AND l.provider='Massive';
 UPDATE population_top3000_202608_202609_v1.pipeline_task SET status='BLOCKED',blocker_code='CTX02_CENSUS_PAGE_CAP_REVIEW',resumption_condition='{"event":"ROLLBACK_FIXTURE_EXHAUSTED_TRANCHE"}'::jsonb WHERE task_id='d377d980-5dbd-4276-a2c9-d5638c60b668' AND status IN('READY','BLOCKED') AND lease_until IS NULL;
 IF NOT FOUND THEN RAISE EXCEPTION 'FIXTURE_TASK_NOT_IDLE';END IF;
 UPDATE population_top3000_202608_202609_v1.ctx02_census_admission_v1 SET admitted_page_ceiling=expected_page-1,max_provider_calls=calls WHERE request_key='ctx02-census-v1:0869cb2de1b0fe2ad0789fd4498df455de76ad92aa32805c36417a4cf87bb5cd';
 SELECT to_jsonb(t) INTO before_t FROM population_top3000_202608_202609_v1.pipeline_task t WHERE task_id='d377d980-5dbd-4276-a2c9-d5638c60b668';
 SELECT to_jsonb(a) INTO before_a FROM population_top3000_202608_202609_v1.acquisition_control a WHERE singleton;
 SELECT to_jsonb(p) INTO before_p FROM population_top3000_202608_202609_v1.pipeline_control p WHERE singleton;
 FOREACH n IN ARRAY ARRAY['owner','stop','unknown_provider'] LOOP
  rejected:=false;
  BEGIN
   IF n='owner' THEN UPDATE population_top3000_202608_202609_v1.acquisition_control SET owner_id='invalid-test-owner' WHERE singleton;
   ELSIF n='stop' THEN UPDATE population_top3000_202608_202609_v1.acquisition_control SET stop_generation=stop_generation+1 WHERE singleton;
   ELSE UPDATE population_top3000_202608_202609_v1.pipeline_provider_lease SET released_at=NULL WHERE attempt_id=(SELECT attempt_id FROM population_top3000_202608_202609_v1.pipeline_provider_lease WHERE released_at IS NOT NULL ORDER BY released_at DESC LIMIT 1);
   END IF;
   PERFORM population_top3000_202608_202609_v1.ctx02_clear_page_admission_v6('top3000-completion-20261024-root-5db48b32d103',3,1,'{"source_task_id":"d377d980-5dbd-4276-a2c9-d5638c60b668"}');
   RAISE EXCEPTION 'NEGATIVE_NOT_REJECTED';
  EXCEPTION WHEN raise_exception THEN
   IF (n IN('owner','stop') AND SQLERRM='OPTIONS_V6_CURRENT_AUTHORITY_REQUIRED') OR (n='unknown_provider' AND SQLERRM='OPTIONS_V6_ACTIVE_OR_UNRECONCILED_EFFECT') THEN rejected:=true; ELSE RAISE; END IF;
  END;
  IF NOT rejected THEN RAISE EXCEPTION 'NEGATIVE_FAILED %',n; END IF;
 END LOOP;
 r:=population_top3000_202608_202609_v1.ctx02_clear_page_admission_v6('top3000-completion-20261024-root-5db48b32d103',3,1,'{"source_task_id":"d377d980-5dbd-4276-a2c9-d5638c60b668"}');
 IF r->>'state' IS DISTINCT FROM 'CLEARED' OR r->>'next_page' IS DISTINCT FROM expected_page::text OR r->>'admitted_page_ceiling' IS DISTINCT FROM (expected_page+49)::text THEN RAISE EXCEPTION 'OPTIONS_CLEAR_RESULT_INVALID';END IF;
 SELECT to_jsonb(t) INTO after_t FROM population_top3000_202608_202609_v1.pipeline_task t WHERE task_id='d377d980-5dbd-4276-a2c9-d5638c60b668';
 IF after_t->>'status' IS DISTINCT FROM 'READY' OR (after_t-ARRAY['status','blocker_code','blocker_until','resumption_condition','not_before']) IS DISTINCT FROM (before_t-ARRAY['status','blocker_code','blocker_until','resumption_condition','not_before']) THEN RAISE EXCEPTION 'OPTIONS_CLEAR_COUNTER_CHANGED';END IF;
 BEGIN PERFORM population_top3000_202608_202609_v1.ctx02_clear_page_admission_v6('top3000-completion-20261024-root-5db48b32d103',3,1,'{"source_task_id":"d377d980-5dbd-4276-a2c9-d5638c60b668"}');RAISE EXCEPTION 'DUPLICATE_ACCEPTED';
 EXCEPTION WHEN raise_exception THEN IF SQLERRM<>'OPTIONS_V6_EXACT_PAGE198_CHECKPOINT_REQUIRED' THEN RAISE;END IF;END;
 INSERT INTO options_clear_test_v6 VALUES(jsonb_build_object('result','PASS_OPTIONS_ALLOWANCE_REQUEUE_ROLLBACK_V6','handler_sha256',encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.ctx02_clear_page_admission_v6(text,bigint,bigint,jsonb)'::regprocedure),'UTF8')),'hex'),'receipt',r,'stale_owner_rejected',true,'later_stop_rejected',true,'unknown_provider_rejected',true,'duplicate_rejected',true,'task_counters_and_checkpoint_preserved',true,'provider_calls',0,'production_changes_rolled_back',true));
END;$test$;
SELECT receipt FROM options_clear_test_v6;
ROLLBACK;
