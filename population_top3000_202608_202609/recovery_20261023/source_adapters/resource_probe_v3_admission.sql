BEGIN;
SET LOCAL lock_timeout='3s';
DO $admit$
DECLARE v_c population_top3000_202608_202609_v1.acquisition_control%rowtype;
BEGIN
 SELECT * INTO v_c FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton FOR UPDATE;
 IF v_c.owner_id IS DISTINCT FROM 'top3000-recovery-20261023-root-4b1314bcb5c2' OR v_c.owner_fence<>2 OR v_c.stop_generation<>0 OR NOT v_c.enabled OR v_c.stopped OR v_c.hold_reason IS NOT NULL OR v_c.admitted_until<=clock_timestamp() THEN RAISE EXCEPTION 'ADMITTED_OWNER_REQUIRED';END IF;
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.resource_probe_v1 WHERE probe_attempt=3) THEN RAISE EXCEPTION 'FINAL_SLOT_ALREADY_ADMITTED';END IF;
 UPDATE population_top3000_202608_202609_v1.pipeline_control SET budget_evidence=jsonb_set(budget_evidence,'{resource_probe_admission}',jsonb_build_object(
 'allowed',true,'worker','population-top3000-resource-v1','probe_attempt',3,'max_lifetime_invocations',3,'max_invocations',1,'max_usd',2.01,
 'source_sha256','d9f4fb2bbf26a40ca11ce98fa67a4c921944182f6723aaff109fd2824816314a',
 'scope','OWNED_PROJECT_READ_ONLY_METRICS','root_cause','POSTGRES345_JSONB_DOUBLE_SERIALIZATION',
 'no_provider_requests',true,'no_capacity_change',true,'automatic_retry',false))
 WHERE singleton AND expected_owner_id=v_c.owner_id AND expected_owner_fence=v_c.owner_fence AND expected_stop_generation=v_c.stop_generation AND budget_evidence->>'provider_admissions_allowed'='false';
 IF NOT FOUND THEN RAISE EXCEPTION 'PIPELINE_ADMISSION_GUARD';END IF;
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details)
 VALUES('RESOURCE_PROBE_V3_ROLLBACK_VERIFIED',jsonb_build_object('result','PASS_RESOURCE_PROBE_V3_THREE_SLOT_CAPABILITY_SANITIZATION_AND_ACCOUNTING_GUARDS','owner_fence',2,'sql_test_sha256','ceb5f024738c99c366f1b517bbdf92a63ab8fc75fd47bcff967ed7559877bfe2','definition_sha256','c0b04b28a91d09ab1932fc1a0722aeb0ff5c5d9fe4c9b7076ba94122649d996f','no_external_test_requests',true,'all_test_mutations_rolled_back',true));
END $admit$;
COMMIT;
