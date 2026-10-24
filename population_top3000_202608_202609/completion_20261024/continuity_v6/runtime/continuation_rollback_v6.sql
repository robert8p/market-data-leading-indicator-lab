-- Run BEGIN; reviewed queue DDL; reviewed tick definition; then this rollback fixture. No external provider calls.
SET LOCAL lock_timeout='5s'; SET LOCAL statement_timeout='60s';
CREATE TEMP TABLE continuation_v6_fixture_receipt(receipt jsonb) ON COMMIT DROP;
DO $test$
DECLARE ctl jsonb;before_ctl jsonb;after_ctl jsonb;a jsonb;b jsonb;n1 integer;n2 integer;rejected boolean;labels text[]:='{}';d jsonb;prior_admit timestamptz;prior_deadline timestamptz;renew_target text;
BEGIN
 PERFORM pg_advisory_xact_lock(2026083000261022);
 SELECT to_jsonb(x)||jsonb_build_object('pipeline',(SELECT to_jsonb(p) FROM population_top3000_202608_202609_v1.pipeline_control p WHERE singleton)) INTO before_ctl FROM population_top3000_202608_202609_v1.acquisition_control x WHERE singleton;
 IF population_top3000_202608_202609_v1.continuation_classify_v6('CTX02','ACQUIRE','CTX02_CENSUS_PAGE_CAP_REVIEW')<>'OPERATIONAL_REVIEW'
 OR population_top3000_202608_202609_v1.continuation_classify_v6('CTX07','VALIDATE','CTX07_SOURCE_NUMERICAL_VINTAGE_ACCEPTANCE_REQUIRED')<>'SOURCE_RECOVERY'
 OR population_top3000_202608_202609_v1.continuation_classify_v6('REF01','VALIDATE','CUTOFF_REQUIRED')<>'COMPANY_RECONCILIATION'
 OR population_top3000_202608_202609_v1.continuation_classify_v6('D01','VALIDATE','NUMERICAL_REQUIRED')<>'INDEPENDENT_VERIFICATION'
 OR population_top3000_202608_202609_v1.continuation_classify_v6('CTX02','NORMALIZE','PARSER')<>'IMPLEMENTATION_REPAIR'
 OR population_top3000_202608_202609_v1.continuation_classify_v6('P01','ACQUIRE','ALPACA_SIP_MISSING_PROVIDER_KEY')<>'EXTERNAL_CONDITION'
 OR population_top3000_202608_202609_v1.continuation_classify_v6('CTX07','ACQUIRE','CTX07_V6_ENFORCED_ACCESS_DENIAL')<>'EXTERNAL_CONDITION'
 OR population_top3000_202608_202609_v1.continuation_classify_v6('CTX07','ACQUIRE','CTX07_V6_UNKNOWN_HTTP')<>'EXTERNAL_CONDITION'
 OR population_top3000_202608_202609_v1.continuation_classify_v6('CTX02','ACQUIRE','UNCERTAIN_EFFECT')<>'EXTERNAL_CONDITION' THEN RAISE EXCEPTION 'CLASSIFICATION_FAILED';END IF;
 d:=population_top3000_202608_202609_v1.continuation_dispatch_v6('top3000-completion-20261024-root-5db48b32d103',3,1);
 SELECT count(*) INTO n1 FROM population_top3000_202608_202609_v1.continuation_clear_queue_v6;
 IF n1=0 OR n1>20 THEN RAISE EXCEPTION 'QUEUE_BOUNDS_FAILED';END IF;
 PERFORM population_top3000_202608_202609_v1.continuation_dispatch_v6('top3000-completion-20261024-root-5db48b32d103',3,1);
 SELECT count(*) INTO n2 FROM population_top3000_202608_202609_v1.continuation_clear_queue_v6;
 IF n2<>n1 THEN RAISE EXCEPTION 'DEDUP_FAILED';END IF;
 a:=population_top3000_202608_202609_v1.continuation_claim_v6('top3000-completion-20261024-root-5db48b32d103',3,1,'automation:6adc2c96db348191bca7cf4eacfdac32','rollback-run-first');
 b:=population_top3000_202608_202609_v1.continuation_claim_v6('top3000-completion-20261024-root-5db48b32d103',3,1,'automation:6adc2c96db348191bca7cf4eacfdac32','rollback-run-second');
 IF a->>'state'<>'CLAIMED' OR b->>'state'<>'CLAIMED' OR a#>>'{assignment,family_id}'=b#>>'{assignment,family_id}' THEN RAISE EXCEPTION 'DISJOINT_CONTINUATION_FAILED';END IF;
 BEGIN UPDATE population_top3000_202608_202609_v1.acquisition_control SET stopped=true WHERE singleton;
 PERFORM population_top3000_202608_202609_v1.continuation_dispatch_v6('top3000-completion-20261024-root-5db48b32d103',3,1);RAISE EXCEPTION 'STOP_NOT_REJECTED';
 EXCEPTION WHEN raise_exception THEN IF SQLERRM<>'CONTINUATION_USER_OR_CONTROL_HOLD' THEN RAISE;END IF;labels:=array_append(labels,'user_stop');END;
 BEGIN UPDATE population_top3000_202608_202609_v1.acquisition_control SET owner_fence=4 WHERE singleton;
 PERFORM population_top3000_202608_202609_v1.continuation_dispatch_v6('top3000-completion-20261024-root-5db48b32d103',3,1);RAISE EXCEPTION 'OWNER_NOT_REJECTED';
 EXCEPTION WHEN raise_exception THEN IF SQLERRM<>'CONTINUATION_OWNER_STOP_SCOPE_CHANGED' THEN RAISE;END IF;labels:=array_append(labels,'owner_fence');END;
 BEGIN UPDATE population_top3000_202608_202609_v1.acquisition_control SET stop_generation=2 WHERE singleton;
 PERFORM population_top3000_202608_202609_v1.continuation_dispatch_v6('top3000-completion-20261024-root-5db48b32d103',3,1);RAISE EXCEPTION 'STOP_GENERATION_NOT_REJECTED';
 EXCEPTION WHEN raise_exception THEN IF SQLERRM<>'CONTINUATION_OWNER_STOP_SCOPE_CHANGED' THEN RAISE;END IF;labels:=array_append(labels,'later_stop_generation');END;
 BEGIN PERFORM population_top3000_202608_202609_v1.continuation_finish_v6('top3000-completion-20261024-root-5db48b32d103',3,1,a->>'clearing_id','bad-token','{"state":"COMPLETE","effects_known":true}'::jsonb);RAISE EXCEPTION 'TOKEN_NOT_REJECTED';
 EXCEPTION WHEN raise_exception THEN IF SQLERRM<>'CONTINUATION_CLAIM_REQUIRED' THEN RAISE;END IF;labels:=array_append(labels,'bad_token');END;
 BEGIN PERFORM population_top3000_202608_202609_v1.continuation_finish_v6('top3000-completion-20261024-root-5db48b32d103',3,1,a->>'clearing_id',a->>'token','{"state":"COMPLETE","effects_known":true}'::jsonb);RAISE EXCEPTION 'FALSE_COMPLETION_NOT_REJECTED';
 EXCEPTION WHEN raise_exception THEN IF SQLERRM NOT IN('CONTINUATION_REVIEW_COMMIT_SUCCESSOR_REQUIRED','CONTINUATION_REVIEW_EFFECT_RECEIPT_SUCCESSOR_REQUIRED') THEN RAISE;END IF;labels:=array_append(labels,'missing_useful_commit');END;
 BEGIN UPDATE population_top3000_202608_202609_v1.continuation_clear_queue_v6 SET claim_expires_at=clock_timestamp()-interval '1second' WHERE clearing_id=a->>'clearing_id';
 PERFORM population_top3000_202608_202609_v1.continuation_finish_v6('top3000-completion-20261024-root-5db48b32d103',3,1,a->>'clearing_id',a->>'token','{"state":"BLOCKED_EXTERNAL","effects_known":true,"external_condition":"fixture only external","bounded_alternatives_evidence":"fixture"}'::jsonb);RAISE EXCEPTION 'EXPIRED_NOT_REJECTED';
 EXCEPTION WHEN raise_exception THEN IF SQLERRM<>'CONTINUATION_CLAIM_REQUIRED' THEN RAISE;END IF;labels:=array_append(labels,'expired_claim');END;

 -- Exercise finite proactive admission renewal inside a rolled-back subtransaction, preserving counters/deadline.
 BEGIN
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE status='LEASED') OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_provider_lease WHERE released_at IS NULL) OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.connector_probe_lease_v1 WHERE released_at IS NULL) THEN RAISE EXCEPTION 'RENEWAL_FIXTURE_WAIT_FOR_ACTIVE_WORK';END IF;
 prior_admit:=clock_timestamp()+interval '5minutes';
 SELECT deadline INTO prior_deadline FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton;
 UPDATE population_top3000_202608_202609_v1.acquisition_control SET admitted_until=prior_admit WHERE singleton;
 SELECT clearing_id INTO renew_target FROM population_top3000_202608_202609_v1.continuation_clear_queue_v6 WHERE status='READY' ORDER BY created_at LIMIT 1;
 IF renew_target IS NULL THEN RAISE EXCEPTION 'RENEWAL_TARGET_MISSING';END IF;
 UPDATE population_top3000_202608_202609_v1.continuation_clear_queue_v6 SET not_after=prior_admit WHERE clearing_id=renew_target;
 UPDATE population_top3000_202608_202609_v1.pipeline_event SET at=clock_timestamp()-interval '2minutes' WHERE event='CONTINUATION_DISPATCH_PASS_V6';
 PERFORM population_top3000_202608_202609_v1.continuation_dispatch_v6('top3000-completion-20261024-root-5db48b32d103',3,1);
 IF (SELECT admitted_until FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton)<=prior_admit OR (SELECT deadline FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton) IS DISTINCT FROM prior_deadline THEN RAISE EXCEPTION 'RENEWAL_DID_NOT_PRESERVE_ORIGINAL_DEADLINE';END IF;
 IF (SELECT not_after FROM population_top3000_202608_202609_v1.continuation_clear_queue_v6 WHERE clearing_id=renew_target)<=prior_admit THEN RAISE EXCEPTION 'UNCLAIMED_ADMISSION_NOT_RENEWED';END IF;
 RAISE EXCEPTION 'RENEWAL_FIXTURE_PASS_ROLLBACK';
 EXCEPTION WHEN raise_exception THEN IF SQLERRM<>'RENEWAL_FIXTURE_PASS_ROLLBACK' THEN RAISE;END IF;labels:=array_append(labels,'finite_time_and_unclaimed_assignment_renewal');END;
 SELECT to_jsonb(x)||jsonb_build_object('pipeline',(SELECT to_jsonb(p) FROM population_top3000_202608_202609_v1.pipeline_control p WHERE singleton)) INTO after_ctl FROM population_top3000_202608_202609_v1.acquisition_control x WHERE singleton;
 IF before_ctl IS DISTINCT FROM after_ctl THEN RAISE EXCEPTION 'CONTROLS_CHANGED';END IF;
 INSERT INTO continuation_v6_fixture_receipt VALUES(jsonb_build_object('result','PASS_CLEARING_QUEUE_ROLLBACK_V6','groups',n1,'deduplicated',true,'disjoint_claim_families',jsonb_build_array(a#>>'{assignment,family_id}',b#>>'{assignment,family_id}'),'negative_guards',labels,'provider_calls',0,'controls_counters_unchanged',true,'no_data_acceptance',true,'production_changes_rolled_back',true));
END;$test$;
SELECT receipt FROM continuation_v6_fixture_receipt;
ROLLBACK;
