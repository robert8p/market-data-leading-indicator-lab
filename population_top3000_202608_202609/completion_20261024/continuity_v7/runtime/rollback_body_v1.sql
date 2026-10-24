CREATE TEMP TABLE sequencing_test_receipt(v jsonb);
DO $test$
DECLARE iv jsonb;tid uuid;aid uuid;sid uuid;token text:='sequencing-test-only';r jsonb;f jsonb;h text;watch_tid uuid;proof jsonb;reviewid bigint;before_ex bigint;bad boolean;
BEGIN
 PERFORM population_top3000_202608_202609_v1.continuation_guard_v6('top3000-completion-20261024-root-5db48b32d103',3,1);
 iv:=jsonb_build_object('scope_sha256','8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd','policy_sha256','3e49d661fcdb8533e5ce88a85689b41e9dde71eeae2dc8578c9e31f9ae1738bb','cohort_sha256','2dc0f766befed6de6b052221c3c87482d507ba1bad931cbb98654eb3af899bec','membership_acceptance_sha256','20b9dac759857e9b510ccaeca33e4a94d8c748d588fb0cc27740cc66c389b5f9','source_set_sha256','7adaa1eadfd956686ae2a02fa346d6a3ad5a27c51a5180a6a7894bd46de973f9','F05_contract_sha256','c5284ff9a46765d008337e4b9b8d84a44342da93465e4c957c642475f15b2a7a');
 tid:=population_top3000_202608_202609_v1.pipeline_enqueue_v1('F05','DERIVE','f05-retained-reported-candidates-v1','{"kind":"F05_RETAINED_REPORTED_CANDIDATES_V1"}',iv);
 aid:=gen_random_uuid();
 UPDATE population_top3000_202608_202609_v1.pipeline_task SET status='LEASED',attempt_id=aid,lease_until=clock_timestamp()+interval '10minutes',owner_fence=3,stop_generation=1 WHERE task_id=tid;
 INSERT INTO population_top3000_202608_202609_v1.pipeline_attempt(attempt_id,task_id,execution_number,token_sha256,owner_fence,stop_generation,adapter_sha256) SELECT aid,tid,1,encode(sha256(convert_to(token,'UTF8')),'hex'),3,1,function_sha256 FROM population_top3000_202608_202609_v1.pipeline_adapter WHERE family_id='F05' AND stage='DERIVE';
 BEGIN UPDATE population_top3000_202608_202609_v1.pipeline_task SET input_versions=jsonb_set(input_versions,'{policy_sha256}','"bad"') WHERE task_id=tid;PERFORM population_top3000_202608_202609_v1.f05_retained_candidate_worker_v1(aid,token);RAISE EXCEPTION 'BAD_POLICY_ACCEPTED';EXCEPTION WHEN OTHERS THEN IF SQLERRM<>'F05_POLICY_COHORT_SOURCE_SCOPE_CHANGED' THEN RAISE;END IF;END;
 BEGIN UPDATE population_top3000_202608_202609_v1.sec_financial_fact_v1 SET value=value+1 WHERE fact_key='20a3ecf8851309cc49d6ac5bb28de5c306ecef1b613b69db928c090a67c94f47';PERFORM population_top3000_202608_202609_v1.f05_retained_candidate_worker_v1(aid,token);RAISE EXCEPTION 'BAD_SOURCE_ACCEPTED';EXCEPTION WHEN OTHERS THEN IF SQLERRM<>'F05_EXACT_RETAINED_SOURCE_CHANGED' THEN RAISE;END IF;END;
 r:=population_top3000_202608_202609_v1.f05_retained_candidate_worker_v1(aid,token);
 IF (r#>>'{commit_receipt,new_committed_units}')::int<>12 THEN RAISE EXCEPTION 'F05_NO_USEFUL_OUTPUT';END IF;
 IF population_top3000_202608_202609_v1.f05_retained_candidate_worker_v1(aid,token) IS DISTINCT FROM r THEN RAISE EXCEPTION 'F05_REPLAY_CONFLICT';END IF;
 f:=population_top3000_202608_202609_v1.pipeline_finish_v1(aid,token,r);sid:=(f->>'successor_task_id')::uuid;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_commit WHERE task_id=tid AND new_committed_units=12) OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=sid AND family_id='F05' AND stage='VALIDATE') THEN RAISE EXCEPTION 'F05_CHAIN_MISSING';END IF;
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.f05_retained_candidate_v1 WHERE record->>'actual_earnings_value' IS NOT NULL OR record->>'earnings_event_at' IS NOT NULL OR record->>'surprise' IS NOT NULL OR record->>'family_accepted'<>'false') THEN RAISE EXCEPTION 'FALSE_EARNINGS_ACCEPTANCE';END IF;
 aid:=gen_random_uuid();UPDATE population_top3000_202608_202609_v1.pipeline_task SET status='LEASED',attempt_id=aid,lease_until=clock_timestamp()+interval '10minutes',owner_fence=3,stop_generation=1 WHERE task_id=sid;
 INSERT INTO population_top3000_202608_202609_v1.pipeline_attempt(attempt_id,task_id,execution_number,token_sha256,owner_fence,stop_generation,adapter_sha256) SELECT aid,sid,1,encode(sha256(convert_to(token,'UTF8')),'hex'),3,1,function_sha256 FROM population_top3000_202608_202609_v1.pipeline_adapter WHERE family_id='F05' AND stage='VALIDATE';
 r:=population_top3000_202608_202609_v1.f05_retained_candidate_worker_v1(aid,token);PERFORM population_top3000_202608_202609_v1.pipeline_finish_v1(aid,token,r);
 IF r->>'state'<>'BLOCKED' THEN RAISE EXCEPTION 'F05_HOLD_CLEARED';END IF;
 -- Synthetic dependency protocol proof only: this is not qualification of the old breadth adapter for new membership.
 iv:=iv||jsonb_build_object('daily_input_versions_sha256','6fe3d08080861b40b3d4c7cd0763a18a4fdf861d533b4a63c3164a8c1a23c539','instrument_manifest_sha256',repeat('1',64));
 watch_tid:=population_top3000_202608_202609_v1.pipeline_enqueue_v1('CTX06','ACQUIRE','synthetic-sequencing-rollback-v1','{"kind":"SYNTHETIC_ROLLBACK_ONLY","daily_run_id":"19a35bfa-6023-4092-9809-3ed950871d7f"}',iv);
 UPDATE population_top3000_202608_202609_v1.pipeline_task SET status='BLOCKED',blocker_code='CTX06_PRACTICAL_DEPENDENCIES_REQUIRED_V2',resumption_condition='{"synthetic_rollback_only":true,"required_action":"Supply exact proof"}'::jsonb WHERE task_id=watch_tid;
 SELECT function_sha256 INTO h FROM population_top3000_202608_202609_v1.pipeline_adapter WHERE family_id='CTX06' AND stage='ACQUIRE';
 proof:=iv||jsonb_build_object('result','PASS_ACTUAL_FROZEN_COHORT_FIRST100','company_count',100,'vote_rows',4200,'full_period_instrument_evidence_verified',true,'adapter_sha256',h,'daily_run_id','19a35bfa-6023-4092-9809-3ed950871d7f','rolled_back',true,'source_and_membership_reconciled',true,'reviewer','synthetic-reviewer','producer','synthetic-producer','synthetic_rollback_only',true);
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES('SEQUENCING_DEPENDENCY_REGISTRATION_REVIEW_V1',jsonb_build_object('task_id',watch_tid,'input_versions_sha256',encode(sha256(convert_to(iv::text,'UTF8')),'hex'),'result','APPROVED','reviewer','synthetic-reviewer','producer','synthetic-producer','synthetic_rollback_only',true)) RETURNING event_id INTO reviewid;
 INSERT INTO population_top3000_202608_202609_v1.sequencing_dependency_watch_v1(task_id,input_versions_sha256,expected_blocker,policy_sha256,cohort_sha256,acceptance_sha256,daily_run_id,daily_input_sha256,prerequisite_task_id,required_proof_event,required_proof_fields,adapter_sha256,registration_review_event_id)
 VALUES(watch_tid,encode(sha256(convert_to(iv::text,'UTF8')),'hex'),'CTX06_PRACTICAL_DEPENDENCIES_REQUIRED_V2',iv->>'policy_sha256',iv->>'cohort_sha256',iv->>'membership_acceptance_sha256','19a35bfa-6023-4092-9809-3ed950871d7f',iv->>'daily_input_versions_sha256','2448c2cc-56a7-4785-bb64-fb8f50b11ec8','CTX06_PRACTICAL_FIRST_BATCH_VERIFIED_V2',proof,h,reviewid);
 IF (population_top3000_202608_202609_v1.sequencing_dependency_wake_v1('top3000-completion-20261024-root-5db48b32d103',3,1)->>'woken')::int<>0 THEN RAISE EXCEPTION 'UNSATISFIED_PROOF_WOKEN';END IF;
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES('CTX06_PRACTICAL_FIRST_BATCH_VERIFIED_V2',proof);
 BEGIN UPDATE population_top3000_202608_202609_v1.pipeline_task SET input_versions=jsonb_set(input_versions,'{policy_sha256}','"stale"') WHERE task_id=watch_tid;IF (population_top3000_202608_202609_v1.sequencing_dependency_wake_v1('top3000-completion-20261024-root-5db48b32d103',3,1)->>'woken')::int<>0 THEN RAISE EXCEPTION 'STALE_POLICY_WOKEN';END IF;RAISE EXCEPTION 'EXPECTED_REVERT';EXCEPTION WHEN OTHERS THEN IF SQLERRM<>'EXPECTED_REVERT' THEN RAISE;END IF;END;
 BEGIN UPDATE population_top3000_202608_202609_v1.acquisition_control SET stopped=true WHERE singleton;PERFORM population_top3000_202608_202609_v1.sequencing_dependency_wake_v1('top3000-completion-20261024-root-5db48b32d103',3,1);RAISE EXCEPTION 'STOP_ACCEPTED';EXCEPTION WHEN OTHERS THEN IF SQLERRM<>'CONTINUATION_USER_OR_CONTROL_HOLD' THEN RAISE;END IF;END;
 BEGIN UPDATE population_top3000_202608_202609_v1.acquisition_control SET owner_fence=4 WHERE singleton;PERFORM population_top3000_202608_202609_v1.sequencing_dependency_wake_v1('top3000-completion-20261024-root-5db48b32d103',3,1);RAISE EXCEPTION 'OWNER_ACCEPTED';EXCEPTION WHEN OTHERS THEN IF SQLERRM<>'CONTINUATION_OWNER_STOP_SCOPE_CHANGED' THEN RAISE;END IF;END;
 IF (population_top3000_202608_202609_v1.sequencing_dependency_wake_v1('top3000-completion-20261024-root-5db48b32d103',3,1)->>'woken')::int<>1 THEN RAISE EXCEPTION 'SATISFIED_NOT_WOKEN';END IF;
 IF (population_top3000_202608_202609_v1.sequencing_dependency_wake_v1('top3000-completion-20261024-root-5db48b32d103',3,1)->>'woken')::int<>0 THEN RAISE EXCEPTION 'DUPLICATE_WAKE';END IF;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=watch_tid AND recheck_count=1 AND executions=0 AND status='READY') THEN RAISE EXCEPTION 'COUNTERS_RESET';END IF;
 INSERT INTO sequencing_test_receipt VALUES(jsonb_build_object('result','PASS','f05_candidate_rows',12,'f05_actual_commit_and_validation_successor',true,'f05_validation_still_held',true,'source_and_policy_change_rejected',true,'dependency_missing_proof_rejected',true,'dependency_stale_policy_rejected',true,'dependency_owner_stop_rejected',true,'dependency_satisfied_synthetic_wake',true,'dependency_duplicate_wake_rejected',true,'production_ctx06_qualified',false,'rolled_back',true));
END;$test$;
-- Insert inside outer rollback transaction after candidate function installation.
-- Subtransaction rolls back even on success; no persistent queue, claim or admission effects.
DO $fair$
DECLARE r jsonb; passed boolean:=false;
BEGIN
 BEGIN
  UPDATE population_top3000_202608_202609_v1.continuation_clear_queue_v6 SET not_after=clock_timestamp()-interval '1second' WHERE status IN('READY','NEEDS_REVIEW');
  INSERT INTO population_top3000_202608_202609_v1.continuation_clear_queue_v6(clearing_id,kind,family_id,blocker_code,input_manifest,input_sha256,status,owner_id,owner_fence,stop_generation,scope_sha256,not_after,created_at,claimed_at)
  SELECT k,kind,family,'SYNTHETIC_ROLLBACK_ONLY','[]'::jsonb,encode(sha256(convert_to('[]','UTF8')),'hex'),state,'top3000-completion-20261024-root-5db48b32d103',3,1,'8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd',clock_timestamp()+interval '5minutes',clock_timestamp()+age,history
  FROM (VALUES
   ('synthetic-fair-operational-v1','OPERATIONAL_REVIEW','SYNTHETIC_OP','READY',interval '-1hour',NULL::timestamptz),
   ('synthetic-fair-source-v1','SOURCE_RECOVERY','SYNTHETIC_SOURCE','READY',interval '-3hour',NULL::timestamptz),
   ('synthetic-fair-verification-v1','INDEPENDENT_VERIFICATION','SYNTHETIC_VERIFY','READY',interval '-2hour',NULL::timestamptz),
   ('synthetic-fair-source-history-v1','SOURCE_RECOVERY','SYNTHETIC_SOURCE','COMPLETE',interval '-4hour',clock_timestamp()-interval '1minute')
  )v(k,kind,family,state,age,history);
  r:=population_top3000_202608_202609_v1.continuation_claim_v6('top3000-completion-20261024-root-5db48b32d103',3,1,'automation:6adc2c96db348191bca7cf4eacfdac32','synthetic-fair-op-20261024');
  IF r->>'clearing_id' IS DISTINCT FROM 'synthetic-fair-operational-v1' THEN RAISE EXCEPTION 'FAIR_OPERATIONAL_PRIORITY_FAILED';END IF;
  r:=population_top3000_202608_202609_v1.continuation_claim_v6('top3000-completion-20261024-root-5db48b32d103',3,1,'automation:6adc2c96db348191bca7cf4eacfdac32','synthetic-fair-verification-20261024');
  IF r->>'clearing_id' IS DISTINCT FROM 'synthetic-fair-verification-v1' THEN RAISE EXCEPTION 'FAIR_NEVER_CLAIMED_FAMILY_FAILED';END IF;
  r:=population_top3000_202608_202609_v1.continuation_claim_v6('top3000-completion-20261024-root-5db48b32d103',3,1,'automation:6adc2c96db348191bca7cf4eacfdac32','synthetic-fair-source-20261024');
  IF r->>'clearing_id' IS DISTINCT FROM 'synthetic-fair-source-v1' THEN RAISE EXCEPTION 'FAIR_DISJOINT_SOURCE_FAILED';END IF;
  RAISE EXCEPTION 'EXPECTED_FAIRNESS_TEST_ROLLBACK';
 EXCEPTION WHEN OTHERS THEN
  IF SQLERRM<>'EXPECTED_FAIRNESS_TEST_ROLLBACK' THEN RAISE;END IF;
  passed:=true;
 END;
 IF NOT passed OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.continuation_clear_queue_v6 WHERE clearing_id LIKE 'synthetic-fair-%') THEN RAISE EXCEPTION 'FAIRNESS_ROLLBACK_FAILED';END IF;
 INSERT INTO sequencing_test_receipt VALUES(jsonb_build_object('fairness','PASS_ACTUAL_FUNCTION_SYNTHETIC_ROLLBACK','operational_priority_preserved',true,'never_claimed_verification_ahead_of_recent_source_family',true,'independent_source_selected_while_other_families_claimed',true,'temporary_effects_rolled_back',true));
END;$fair$;

UPDATE sequencing_test_receipt SET v=v||jsonb_build_object('resulting_recovery_manifest',population_top3000_202608_202609_v1.population_recovery_manifest_v1(),'resulting_recovery_manifest_sha256',encode(sha256(convert_to(population_top3000_202608_202609_v1.population_recovery_manifest_v1()::text,'UTF8')),'hex'));
SELECT v FROM sequencing_test_receipt;
ROLLBACK;
