-- Root-only execution. Uses real retained rows; every mutation rolls back.
BEGIN;
SET LOCAL lock_timeout='3s';
SET LOCAL statement_timeout='50s';
SELECT pg_advisory_xact_lock(2026083000261022);
DO $test$
DECLARE part text;r jsonb;i integer;v jsonb;
BEGIN
IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton AND owner_id='top3000-recovery-20261023-root-4b1314bcb5c2' AND owner_fence=2 AND stop_generation=0 AND NOT stopped AND enabled AND hold_reason IS NULL AND admitted_until>clock_timestamp()) THEN RAISE EXCEPTION 'OWNER_STOP_OR_ADMISSION_CHANGED'; END IF;
IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE status='LEASED' AND lease_until>clock_timestamp()) THEN RAISE EXCEPTION 'LIVE_TASK_MUST_FINISH_BEFORE_TEST'; END IF;
IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE family_id='CTX07') THEN RAISE EXCEPTION 'MACRO_TASKS_ALREADY_EXIST_RECONCILE_BEFORE_FIRST_TEST'; END IF;
v:=population_top3000_202608_202609_v1.macro_staging_verify_v1();
IF v->>'manifest_sha256'<>'36fa6a66060f9771dd270c50ddeddc1c241e624ff1e0764f478eda2428ae364d' THEN RAISE EXCEPTION 'MACRO_MANIFEST_CHANGED'; END IF;
-- Isolate this rollback test from any real READY work without changing durable work state.
UPDATE population_top3000_202608_202609_v1.pipeline_task SET not_before='infinity'::timestamptz WHERE status='READY';
UPDATE population_top3000_202608_202609_v1.pipeline_control SET next_wakeup_at=clock_timestamp() WHERE singleton;
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
'DATABASE',true,'{"test":"ctx07_three_partition_actual_data_rollback","status":"IN_PROGRESS_ROLLBACK_ONLY"}'::jsonb,'macro-retained-recovery-20261023',30
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
FOR i IN 1..12 LOOP
r:=population_top3000_202608_202609_v1.pipeline_tick_v1();
IF r->>'state'<>'DISPATCHED' OR r#>>'{result,state}'<>'COMPLETE' THEN RAISE EXCEPTION 'MACRO_STAGE_TEST_FAILED tick=% receipt=%',i,r; END IF;
END LOOP;
IF (SELECT count(*) FROM population_top3000_202608_202609_v1.source_observation WHERE family_id='CTX07')<>73 THEN RAISE EXCEPTION 'MACRO_OBSERVED_COUNT';END IF;
IF (SELECT count(*) FROM population_top3000_202608_202609_v1.expected_observation WHERE family_id='CTX07')<>79 THEN RAISE EXCEPTION 'MACRO_EXPECTED_COUNT';END IF;
IF (SELECT count(*) FROM population_top3000_202608_202609_v1.pipeline_validation WHERE family_id='CTX07' AND result='DENOMINATOR_UNRESOLVED')<>3 THEN RAISE EXCEPTION 'MACRO_PROVISIONAL_VALIDATION_SEMANTICS';END IF;
IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_partition WHERE family_id='CTX07' AND (denominator_state<>'PROVISIONAL' OR publication_eligible)) THEN RAISE EXCEPTION 'MACRO_FALSE_COMPLETENESS';END IF;
IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.source_observation WHERE family_id='CTX07' AND (historical_received_at IS NOT NULL OR availability_policy<>'PUBLICATION_SUPPORTED_RECONSTRUCTION')) THEN RAISE EXCEPTION 'MACRO_FALSE_HISTORICAL_RECEIPT';END IF;
IF (SELECT count(*) FROM population_top3000_202608_202609_v1.pipeline_task WHERE family_id='CTX07' AND stage='REPAIR' AND status='READY')<>3 THEN RAISE EXCEPTION 'AUTOMATIC_REPAIR_HANDOFF_MISSING';END IF;
END $test$;
SELECT 'PASS_ROLLBACK_ONLY_CTX07' result,clock_timestamp() observed_at,
(SELECT jsonb_agg(jsonb_build_object('partition',partition_key,'expected',expected_units,'received',received_units,'result',result,'missing',missing_units,'unavailable_fields',unavailable_fields,'checks',required_checks_missing) ORDER BY partition_key)
FROM population_top3000_202608_202609_v1.pipeline_validation WHERE family_id='CTX07') verification;
ROLLBACK;
