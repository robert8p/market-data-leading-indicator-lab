BEGIN;
SET LOCAL lock_timeout='3s';
SELECT pg_advisory_xact_lock(2026083000261022);
DO $admit$
BEGIN
IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton AND owner_id='top3000-recovery-20261023-root-4b1314bcb5c2' AND owner_fence=2 AND enabled AND NOT stopped AND admitted_until>clock_timestamp()) THEN RAISE EXCEPTION 'OWNER_OR_ADMISSION_CHANGED'; END IF;
IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_adapter WHERE family_id='REF01' AND stage='VALIDATE' AND enabled AND function_oid='population_top3000_202608_202609_v1.pipeline_validate_v1(uuid,text)'::regprocedure::oid AND function_sha256=encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.pipeline_validate_v1(uuid,text)'::regprocedure),'UTF8')),'hex')) THEN RAISE EXCEPTION 'VERIFIED_GENERIC_VALIDATOR_MISSING_OR_CHANGED';END IF;
INSERT INTO population_top3000_202608_202609_v1.pipeline_adapter(family_id,stage,function_oid,function_sha256,resource_class,enabled,reviewed_test_evidence,producer_revision,expected_batch_seconds)
SELECT 'F01',stage,function_oid,function_sha256,'DATABASE',true,reviewed_test_evidence||jsonb_build_object('admission_basis','Reuse exact generic validator already exercised through real REF01 and CTX07 dispatcher tests; preserve absent-denominator and universe blockers','additional_family','F01','no_new_quality_evidence',true),'generic-validator-reuse-recovery-20261023',30
FROM population_top3000_202608_202609_v1.pipeline_adapter WHERE family_id='REF01' AND stage='VALIDATE'
ON CONFLICT(family_id,stage) DO NOTHING;
UPDATE population_top3000_202608_202609_v1.pipeline_task SET status='READY',not_before=clock_timestamp(),blocker_code=NULL,resumption_condition=NULL,blocker_until=NULL
WHERE task_id='adfc962c-b912-4133-b6e5-758c27030187' AND family_id='F01' AND stage='VALIDATE' AND status='BLOCKED' AND blocker_code='ADAPTER_NOT_INSTALLED';
INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES('F01_GENERIC_VALIDATOR_REUSED',jsonb_build_object('owner_fence',2,'same_verified_function','pipeline_validate_v1','no_quality_threshold_change',true,'no_denominator_invented',true));
PERFORM population_top3000_202608_202609_v1.pipeline_wake_v1('top3000-recovery-20261023-root-4b1314bcb5c2',2,'F01_VERIFIED_GENERIC_VALIDATOR_REGISTERED');
END $admit$;
COMMIT;
