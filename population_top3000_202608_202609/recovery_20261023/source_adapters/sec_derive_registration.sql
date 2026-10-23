-- Root-only after successful sec_derive_rollback_test.sql and persistence of its real receipt.
BEGIN;
SET LOCAL lock_timeout='3s';
SELECT pg_advisory_xact_lock(2026083000261022);
DO $register$
DECLARE evidence jsonb;
BEGIN
IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton AND owner_id='top3000-recovery-20261023-root-4b1314bcb5c2' AND owner_fence=2 AND stop_generation=0 AND NOT stopped AND enabled AND hold_reason IS NULL AND admitted_until>clock_timestamp()) THEN RAISE EXCEPTION 'OWNER_STOP_OR_ADMISSION_CHANGED';END IF;
SELECT details INTO evidence FROM population_top3000_202608_202609_v1.pipeline_event
WHERE event='SEC_CANDIDATE_ROLLBACK_VERIFIED' AND details->>'result'='PASS' AND details->>'source_sha256'='24f0ee423c27ffe0d2e04ad989ba45e4a0613bad88f805f145b554a5b3e81a62'
ORDER BY at DESC LIMIT 1;
IF evidence IS NULL THEN RAISE EXCEPTION 'REAL_SEC_CANDIDATE_TEST_RECEIPT_REQUIRED';END IF;
INSERT INTO population_top3000_202608_202609_v1.pipeline_adapter(family_id,stage,function_oid,function_sha256,resource_class,enabled,reviewed_test_evidence,producer_revision,expected_batch_seconds)
VALUES('F01','DERIVE','population_top3000_202608_202609_v1.sec_candidate_link_v1(uuid,text)'::regprocedure::oid,
encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.sec_candidate_link_v1(uuid,text)'::regprocedure),'UTF8')),'hex'),
'DATABASE',true,evidence,'sec-candidate-link-recovery-20261023',30)
ON CONFLICT(family_id,stage) DO NOTHING;
-- A material change now satisfies this precise adapter-missing blocker. Attempts and cumulative errors survive.
UPDATE population_top3000_202608_202609_v1.pipeline_task SET status='READY',not_before=clock_timestamp(),blocker_code=NULL,resumption_condition=NULL,blocker_until=NULL
WHERE family_id='F01' AND stage='DERIVE' AND partition_key='sec-master-2026q3'
AND input_versions->>'source_sha256'='24f0ee423c27ffe0d2e04ad989ba45e4a0613bad88f805f145b554a5b3e81a62' AND status='BLOCKED' AND blocker_code='ADAPTER_NOT_INSTALLED';
UPDATE population_top3000_202608_202609_v1.pipeline_control SET budget_evidence=jsonb_set(budget_evidence,'{allowed_work}',coalesce(budget_evidence->'allowed_work','[]'::jsonb)||jsonb_build_array('F01 retained SEC index candidate links; provisional universe; no provider calls')),next_wakeup_at=clock_timestamp() WHERE singleton;
INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES('SEC_CANDIDATE_ADAPTER_ADMITTED',evidence||jsonb_build_object('owner_fence',2,'provider_calls',0,'company_universe_certified',false));
PERFORM population_top3000_202608_202609_v1.pipeline_wake_v1('top3000-recovery-20261023-root-4b1314bcb5c2',2,'TESTED_RETAINED_SEC_CANDIDATE_DERIVATION');
END $register$;
COMMIT;
