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
