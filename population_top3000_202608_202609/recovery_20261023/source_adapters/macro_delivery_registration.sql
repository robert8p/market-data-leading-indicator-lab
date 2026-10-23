-- Root-only AFTER observing macro_delivery_rollback_test.sql PASS and persisting
-- MACRO_DELIVERY_ROLLBACK_VERIFIED with result PASS, macro_manifest_sha256 and
-- the test's function_sha256 map. This script never creates its own PASS receipt.
BEGIN;
SET LOCAL lock_timeout='3s';
SELECT pg_advisory_xact_lock(2026083000261022);
DO $admit$
DECLARE evidence jsonb;actual_hashes jsonb;
BEGIN
IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton AND owner_id='top3000-recovery-20261023-root-4b1314bcb5c2' AND owner_fence=2 AND stop_generation=0 AND NOT stopped AND enabled AND hold_reason IS NULL AND admitted_until>clock_timestamp()) THEN RAISE EXCEPTION 'OWNER_STOP_OR_ADMISSION_CHANGED';END IF;
IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton AND shared_scheduler_verified AND budget_evidence->>'admission_class'='RETAINED_DATABASE_ONLY' AND budget_evidence->>'provider_admissions_allowed'='false' AND budget_evidence->>'external_http_calls_allowed'='false') THEN RAISE EXCEPTION 'RETAINED_ONLY_BUDGET_ADMISSION_REQUIRED';END IF;
SELECT details INTO evidence FROM population_top3000_202608_202609_v1.pipeline_event WHERE event='MACRO_DELIVERY_ROLLBACK_VERIFIED' AND details->>'result'='PASS' AND details->>'macro_manifest_sha256'='36fa6a66060f9771dd270c50ddeddc1c241e624ff1e0764f478eda2428ae364d' ORDER BY at DESC LIMIT 1;
IF evidence IS NULL THEN RAISE EXCEPTION 'REAL_MACRO_DELIVERY_TEST_EVIDENCE_REQUIRED';END IF;
SELECT jsonb_object_agg(stage,encode(sha256(convert_to(pg_get_functiondef(fn::regprocedure),'UTF8')),'hex')) INTO actual_hashes FROM (VALUES ('REPAIR','population_top3000_202608_202609_v1.pipeline_repair_v1(uuid,text)'),
('PUBLISH','population_top3000_202608_202609_v1.pipeline_publish_v1(uuid,text)'),
('DELIVER','population_top3000_202608_202609_v1.pipeline_deliver_v1(uuid,text)')) v(stage,fn);
IF evidence->'function_sha256' IS DISTINCT FROM actual_hashes THEN RAISE EXCEPTION 'TESTED_MACRO_DELIVERY_FUNCTIONS_CHANGED_OR_HASH_EVIDENCE_MISSING';END IF;
INSERT INTO population_top3000_202608_202609_v1.pipeline_adapter(family_id,stage,function_oid,function_sha256,resource_class,enabled,reviewed_test_evidence,producer_revision,expected_batch_seconds)
SELECT 'CTX07',stage,fn::regprocedure::oid,encode(sha256(convert_to(pg_get_functiondef(fn::regprocedure),'UTF8')),'hex'),
'DATABASE',true,evidence,'macro-provisional-delivery-recovery-20261023',30
FROM (VALUES
('REPAIR','population_top3000_202608_202609_v1.pipeline_repair_v1(uuid,text)'),
('PUBLISH','population_top3000_202608_202609_v1.pipeline_publish_v1(uuid,text)'),
('DELIVER','population_top3000_202608_202609_v1.pipeline_deliver_v1(uuid,text)')
)v(stage,fn)
ON CONFLICT(family_id,stage) DO NOTHING;
IF EXISTS(SELECT 1 FROM (VALUES
('REPAIR','population_top3000_202608_202609_v1.pipeline_repair_v1(uuid,text)'),
('PUBLISH','population_top3000_202608_202609_v1.pipeline_publish_v1(uuid,text)'),
('DELIVER','population_top3000_202608_202609_v1.pipeline_deliver_v1(uuid,text)')
)v(stage,fn) LEFT JOIN population_top3000_202608_202609_v1.pipeline_adapter a ON a.family_id='CTX07' AND a.stage=v.stage
WHERE a.function_oid IS DISTINCT FROM v.fn::regprocedure::oid OR a.function_sha256 IS DISTINCT FROM encode(sha256(convert_to(pg_get_functiondef(v.fn::regprocedure),'UTF8')),'hex') OR NOT a.enabled OR a.resource_class<>'DATABASE') THEN RAISE EXCEPTION 'EXISTING_MACRO_DELIVERY_ADAPTER_CONFLICT';END IF;
-- Release only the exact adapter-absent blocker now satisfied by this registration.
UPDATE population_top3000_202608_202609_v1.pipeline_task SET status='READY',not_before=clock_timestamp(),blocker_code=NULL,resumption_condition=NULL,blocker_until=NULL
WHERE family_id='CTX07' AND stage='REPAIR' AND partition_key IN('headline','revision','warmup')
AND input_versions->>'macro_manifest_sha256'='36fa6a66060f9771dd270c50ddeddc1c241e624ff1e0764f478eda2428ae364d'
AND (status='READY' OR status='BLOCKED' AND blocker_code='ADAPTER_NOT_INSTALLED');
UPDATE population_top3000_202608_202609_v1.pipeline_control SET budget_evidence=jsonb_set(budget_evidence,'{allowed_work}',coalesce(budget_evidence->'allowed_work','[]'::jsonb)||jsonb_build_array('CTX07 retained repair diagnosis, bounded revalidation, provisional evidence snapshots and delivery manifests; no provider calls or quality certification')),next_wakeup_at=clock_timestamp() WHERE singleton;
INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES('MACRO_PROVISIONAL_DELIVERY_ADMITTED',evidence||jsonb_build_object('owner_fence',2,'provider_calls',0,'publication_classification','PROVISIONAL_SOURCE_EVIDENCE','final_delivery',false,'artifact_export','PENDING_EXTERNAL_EXPORT'));
PERFORM population_top3000_202608_202609_v1.pipeline_wake_v1('top3000-recovery-20261023-root-4b1314bcb5c2',2,'TESTED_CTX07_PROVISIONAL_EVIDENCE_DELIVERY');
END $admit$;
COMMIT;
