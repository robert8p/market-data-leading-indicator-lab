-- Execute only AFTER macro_rollback_test.sql PASS is independently observed and
-- root records a real MACRO_RETAINED_ROLLBACK_VERIFIED event with result PASS.
BEGIN;
SET LOCAL lock_timeout='3s';
SELECT pg_advisory_xact_lock(2026083000261022);
DO $admit$
DECLARE part text;
BEGIN
IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton AND owner_id='top3000-recovery-20261023-root-4b1314bcb5c2' AND owner_fence=2 AND stop_generation=0 AND NOT stopped AND enabled AND hold_reason IS NULL AND admitted_until>clock_timestamp()) THEN RAISE EXCEPTION 'OWNER_STOP_OR_ADMISSION_CHANGED'; END IF;
IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_event WHERE event='MACRO_RETAINED_ROLLBACK_VERIFIED' AND details->>'result'='PASS' AND details->>'macro_manifest_sha256'='36fa6a66060f9771dd270c50ddeddc1c241e624ff1e0764f478eda2428ae364d') THEN RAISE EXCEPTION 'REAL_MACRO_TEST_EVIDENCE_REQUIRED';END IF;
IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton AND shared_scheduler_verified AND budget_evidence->>'admission_class'='RETAINED_DATABASE_ONLY' AND budget_evidence->>'provider_admissions_allowed'='false') THEN RAISE EXCEPTION 'RETAINED_ONLY_BUDGET_ADMISSION_REQUIRED';END IF;
INSERT INTO population_top3000_202608_202609_v1.pipeline_family_binding
(family_id,expected_relation,observed_relation,expected_definition_sha256,observed_definition_sha256,binding_version,requires_company_universe,expected_partition_count,partition_manifest_evidence,partition_manifest_frozen)
VALUES('CTX07','population_top3000_202608_202609_v1.expected_observation'::regclass,
'population_top3000_202608_202609_v1.source_observation'::regclass,
population_top3000_202608_202609_v1.pipeline_relation_hash_v1('population_top3000_202608_202609_v1.expected_observation'::regclass),
population_top3000_202608_202609_v1.pipeline_relation_hash_v1('population_top3000_202608_202609_v1.source_observation'::regclass),
'ctx07-retained-provisional-binding-20261023',false,3,
'{"partitions":["headline","revision","warmup"],"scope":"retained staging only","exhaustive_release_census_verified":false,"company_universe_dependency":false}',false)
ON CONFLICT(family_id) DO NOTHING;
INSERT INTO population_top3000_202608_202609_v1.pipeline_adapter
(family_id,stage,function_oid,function_sha256,resource_class,enabled,reviewed_test_evidence,producer_revision,expected_batch_seconds)
SELECT 'CTX07',stage,fn::regprocedure::oid,encode(sha256(convert_to(pg_get_functiondef(fn::regprocedure),'UTF8')),'hex'),
'DATABASE',true,(SELECT details FROM population_top3000_202608_202609_v1.pipeline_event WHERE event='MACRO_RETAINED_ROLLBACK_VERIFIED' AND details->>'result'='PASS' AND details->>'macro_manifest_sha256'='36fa6a66060f9771dd270c50ddeddc1c241e624ff1e0764f478eda2428ae364d' ORDER BY at DESC LIMIT 1)::jsonb,'macro-retained-recovery-20261023',30
FROM (VALUES
('ACQUIRE','population_top3000_202608_202609_v1.macro_cached_acquire_v1(uuid,text)'),
('NORMALIZE','population_top3000_202608_202609_v1.macro_normalize_v1(uuid,text)'),
('DERIVE','population_top3000_202608_202609_v1.macro_derive_v1(uuid,text)'),
('VALIDATE','population_top3000_202608_202609_v1.pipeline_validate_v1(uuid,text)')
)v(stage,fn)
ON CONFLICT(family_id,stage) DO NOTHING;
FOR part IN SELECT unnest(ARRAY['headline','revision','warmup']) LOOP
PERFORM population_top3000_202608_202609_v1.pipeline_enqueue_v1('CTX07','ACQUIRE',part,
jsonb_build_object('retained_only',true),
jsonb_build_object('macro_manifest_sha256','36fa6a66060f9771dd270c50ddeddc1c241e624ff1e0764f478eda2428ae364d','scope_sha256','8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd'));
END LOOP;
UPDATE population_top3000_202608_202609_v1.pipeline_control SET budget_evidence=jsonb_set(budget_evidence,'{allowed_work}',coalesce(budget_evidence->'allowed_work','[]'::jsonb)||jsonb_build_array('CTX07 retained official-source normalization and provisional quality readback; no provider calls')),next_wakeup_at=clock_timestamp() WHERE singleton;
INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES('MACRO_RETAINED_ADMITTED',jsonb_build_object('owner_fence',2,'macro_manifest_sha256','36fa6a66060f9771dd270c50ddeddc1c241e624ff1e0764f478eda2428ae364d','provider_calls',0,'partition_manifest_frozen',false,'company_universe_required',false));
PERFORM population_top3000_202608_202609_v1.pipeline_wake_v1('top3000-recovery-20261023-root-4b1314bcb5c2',2,'TESTED_CTX07_RETAINED_PARTITIONS_ADMITTED');
END $admit$;
COMMIT;
