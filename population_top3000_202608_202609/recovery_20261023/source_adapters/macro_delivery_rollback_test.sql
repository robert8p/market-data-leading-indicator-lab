-- Root-only execution against the three existing retained CTX07 REPAIR tasks.
-- Real dispatcher, real rows and existing bounded admission; all writes roll back.
BEGIN;
SET LOCAL lock_timeout='3s';
SET LOCAL statement_timeout='50s';
SELECT pg_advisory_xact_lock(2026083000261022);
DO $test$
DECLARE r jsonb;i integer;n integer;before_checks text;after_checks text;v jsonb;
BEGIN
IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton AND owner_id='top3000-recovery-20261023-root-4b1314bcb5c2' AND owner_fence=2 AND stop_generation=0 AND NOT stopped AND enabled AND hold_reason IS NULL AND admitted_until>clock_timestamp()) THEN RAISE EXCEPTION 'OWNER_STOP_OR_ADMISSION_CHANGED';END IF;
IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton AND shared_scheduler_verified AND budget_evidence->>'admission_class'='RETAINED_DATABASE_ONLY' AND budget_evidence->>'provider_admissions_allowed'='false' AND budget_evidence->>'external_http_calls_allowed'='false') THEN RAISE EXCEPTION 'RETAINED_ONLY_BUDGET_ADMISSION_REQUIRED';END IF;
IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE status='LEASED' AND lease_until>clock_timestamp()) THEN RAISE EXCEPTION 'LIVE_TASK_MUST_FINISH_BEFORE_TEST';END IF;
IF (SELECT count(*) FROM population_top3000_202608_202609_v1.pipeline_task WHERE family_id='CTX07' AND stage='REPAIR' AND partition_key IN('headline','revision','warmup') AND input_versions->>'macro_manifest_sha256'='36fa6a66060f9771dd270c50ddeddc1c241e624ff1e0764f478eda2428ae364d' AND (status='READY' OR status='BLOCKED' AND blocker_code='ADAPTER_NOT_INSTALLED'))<>3 THEN RAISE EXCEPTION 'EXPECTED_THREE_UNPROCESSED_MACRO_REPAIR_TASKS';END IF;
IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE family_id='CTX07' AND (stage IN('PUBLISH','DELIVER') OR stage='VALIDATE' AND repair_round>=1)) OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_publication WHERE manifest->>'family_id'='CTX07') THEN RAISE EXCEPTION 'MACRO_DELIVERY_ALREADY_STARTED_RECONCILE_BEFORE_FIRST_TEST';END IF;
v:=population_top3000_202608_202609_v1.macro_staging_verify_v1();
IF v->>'manifest_sha256' IS DISTINCT FROM '36fa6a66060f9771dd270c50ddeddc1c241e624ff1e0764f478eda2428ae364d' THEN RAISE EXCEPTION 'MACRO_MANIFEST_CHANGED';END IF;
SELECT encode(sha256(convert_to(coalesce(jsonb_agg(to_jsonb(e) ORDER BY to_jsonb(e)::text),'[]'::jsonb)::text,'UTF8')),'hex') INTO before_checks FROM population_top3000_202608_202609_v1.pipeline_quality_evidence e WHERE family_id='CTX07';
-- The transaction-level dispatcher advisory lock keeps the live cron from entering.
-- Hide other READY/BLOCKED tasks only within this rollback transaction, including timed rechecks.
UPDATE population_top3000_202608_202609_v1.pipeline_task SET not_before='infinity'::timestamptz WHERE status IN('READY','BLOCKED');
INSERT INTO population_top3000_202608_202609_v1.pipeline_adapter(family_id,stage,function_oid,function_sha256,resource_class,enabled,reviewed_test_evidence,producer_revision,expected_batch_seconds)
SELECT 'CTX07',stage,fn::regprocedure::oid,encode(sha256(convert_to(pg_get_functiondef(fn::regprocedure),'UTF8')),'hex'),
'DATABASE',true,'{"test":"ctx07_repair_revalidate_provisional_publish_delivery_rollback","status":"IN_PROGRESS_ROLLBACK_ONLY"}'::jsonb,'macro-provisional-delivery-recovery-20261023',30
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
UPDATE population_top3000_202608_202609_v1.pipeline_task SET status='READY',not_before=clock_timestamp(),blocker_code=NULL,resumption_condition=NULL,blocker_until=NULL
WHERE family_id='CTX07' AND stage='REPAIR' AND partition_key IN('headline','revision','warmup')
AND input_versions->>'macro_manifest_sha256'='36fa6a66060f9771dd270c50ddeddc1c241e624ff1e0764f478eda2428ae364d'
AND (status='READY' OR status='BLOCKED' AND blocker_code='ADAPTER_NOT_INSTALLED');
UPDATE population_top3000_202608_202609_v1.pipeline_control SET enabled=true,next_wakeup_at=clock_timestamp() WHERE singleton;
-- Three partitions x REPAIR -> VALIDATE(round one) -> PUBLISH -> DELIVER.
FOR i IN 1..12 LOOP
r:=population_top3000_202608_202609_v1.pipeline_tick_v1();
IF r->>'state' IS DISTINCT FROM 'DISPATCHED' OR r#>>'{result,state}' IS DISTINCT FROM 'COMPLETE' THEN RAISE EXCEPTION 'MACRO_DELIVERY_DISPATCH_FAILED tick=% receipt=%',i,r;END IF;
IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=(r->>'task_id')::uuid AND family_id='CTX07' AND stage IN('REPAIR','VALIDATE','PUBLISH','DELIVER')) THEN RAISE EXCEPTION 'UNRELATED_TASK_DISPATCHED';END IF;
END LOOP;
IF (SELECT count(*) FROM population_top3000_202608_202609_v1.pipeline_task WHERE family_id='CTX07' AND stage='DELIVER' AND status='COMPLETE')<>3 THEN RAISE EXCEPTION 'THREE_MACRO_DELIVERIES_REQUIRED';END IF;
IF (SELECT count(*) FROM population_top3000_202608_202609_v1.pipeline_task WHERE family_id='CTX07' AND stage='VALIDATE' AND repair_round=1 AND status='COMPLETE')<>3 THEN RAISE EXCEPTION 'BOUNDED_REVALIDATION_HANDOFF_MISSING';END IF;
IF (SELECT count(*) FROM population_top3000_202608_202609_v1.pipeline_partition p JOIN population_top3000_202608_202609_v1.pipeline_validation v ON v.validation_id=p.last_validation_id WHERE p.family_id='CTX07' AND v.result='DENOMINATOR_UNRESOLVED' AND cardinality(v.required_checks_missing)>0)<>3 THEN RAISE EXCEPTION 'UNRESOLVED_QUALITY_STATUS_LOST';END IF;
IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_partition WHERE family_id='CTX07' AND (denominator_state<>'PROVISIONAL' OR publication_eligible)) OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_family_binding WHERE family_id='CTX07' AND partition_manifest_frozen) THEN RAISE EXCEPTION 'MACRO_FALSE_COMPLETENESS';END IF;
IF (SELECT count(*) FROM population_top3000_202608_202609_v1.pipeline_exception WHERE family_id='CTX07' AND code='DENOMINATOR_UNRESOLVED' AND resolved_at IS NULL)<>3 OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_exception WHERE family_id='CTX07' AND resolved_at IS NOT NULL) THEN RAISE EXCEPTION 'MACRO_UNRESOLVED_EXCEPTIONS_NOT_PRESERVED';END IF;
IF (SELECT count(*) FROM population_top3000_202608_202609_v1.pipeline_publication WHERE manifest->>'family_id'='CTX07' AND classification='PROVISIONAL_SOURCE_EVIDENCE' AND manifest->>'validation_result'='DENOMINATOR_UNRESOLVED')<>3 THEN RAISE EXCEPTION 'MACRO_PUBLICATION_CLASSIFICATION_INCORRECT';END IF;
IF (SELECT count(*) FROM population_top3000_202608_202609_v1.pipeline_publication_member m JOIN population_top3000_202608_202609_v1.pipeline_release_pointer p ON p.singleton AND p.publication_id=m.publication_id WHERE m.family_id='CTX07')<>3 THEN RAISE EXCEPTION 'LATEST_RELEASE_LOST_EARLIER_PARTITION_REFERENCES';END IF;
IF (SELECT count(*) FROM population_top3000_202608_202609_v1.population_source_evidence_current_v1 WHERE family_id='CTX07')<>73 THEN RAISE EXCEPTION 'MACRO_EVIDENCE_TOTAL_MISMATCH';END IF;
IF EXISTS(SELECT 1 FROM (VALUES('headline',39),('revision',15),('warmup',19)) x(part,units) WHERE x.units<>(SELECT count(*) FROM population_top3000_202608_202609_v1.population_source_evidence_current_v1 WHERE family_id='CTX07' AND partition_key=x.part)) THEN RAISE EXCEPTION 'MACRO_EVIDENCE_PARTITION_COUNT_MISMATCH';END IF;
IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.population_source_evidence_current_v1 WHERE family_id='CTX07' AND (validation_state IS DISTINCT FROM 'DENOMINATOR_UNRESOLVED' OR observation->>'historical_received_at' IS NOT NULL OR observation->>'availability_policy' IS DISTINCT FROM 'PUBLICATION_SUPPORTED_RECONSTRUCTION')) THEN RAISE EXCEPTION 'PROVISIONAL_RECEIPT_SEMANTICS_CHANGED';END IF;
-- Independent exact snapshot/readback equality to the complete retained normalized rows.
IF EXISTS(WITH observed AS(SELECT partition_key,unit_key,to_jsonb(o) body FROM population_top3000_202608_202609_v1.source_observation o WHERE family_id='CTX07'),published AS(SELECT partition_key,unit_key,observation body FROM population_top3000_202608_202609_v1.population_source_evidence_current_v1 WHERE family_id='CTX07')
SELECT 1 FROM ((SELECT * FROM observed EXCEPT SELECT * FROM published) UNION ALL (SELECT * FROM published EXCEPT SELECT * FROM observed)) delta) THEN RAISE EXCEPTION 'MACRO_PUBLISHED_PAYLOAD_DIFFERS_FROM_SOURCE';END IF;
IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_published_observation WHERE family_id='CTX07' AND snapshot_sha256 IS DISTINCT FROM encode(sha256(convert_to(observation::text,'UTF8')),'hex')) THEN RAISE EXCEPTION 'MACRO_PUBLISHED_SNAPSHOT_HASH_MISMATCH';END IF;
-- All provisional observations remain excluded even after every target publication date.
IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.population_read_asof_v1('2026-10-23T23:59:59Z',false) WHERE family_id='CTX07') OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.population_read_asof_v1('2026-10-23T23:59:59Z',true) WHERE family_id='CTX07') OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.population_read_asof_v1('1900-01-01T00:00:00Z',false) WHERE family_id='CTX07') THEN RAISE EXCEPTION 'UNVERIFIED_MACRO_BECAME_CONSUMER_ELIGIBLE';END IF;
IF (SELECT count(*) FROM population_top3000_202608_202609_v1.pipeline_delivery WHERE manifest->>'family_id'='CTX07')<>3 OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_delivery WHERE manifest->>'family_id'='CTX07' AND (manifest->>'artifact_export' IS DISTINCT FROM 'PENDING_EXTERNAL_EXPORT' OR manifest->>'final_delivery' IS DISTINCT FROM 'false' OR manifest->>'consumer_future_rejection' IS DISTINCT FROM 'PASS' OR (manifest->>'source_evidence_rows_read')::integer<>(CASE manifest->>'partition_key' WHEN 'headline' THEN 39 WHEN 'revision' THEN 15 WHEN 'warmup' THEN 19 ELSE -1 END))) THEN RAISE EXCEPTION 'DELIVERY_MANIFEST_MISSTATES_EXPORT_OR_EVIDENCE';END IF;
IF population_top3000_202608_202609_v1.pipeline_full_gate_v1()->>'full_gate_pass' IS DISTINCT FROM 'false' THEN RAISE EXCEPTION 'FALSE_FULL_MISSION_COMPLETION';END IF;
SELECT encode(sha256(convert_to(coalesce(jsonb_agg(to_jsonb(e) ORDER BY to_jsonb(e)::text),'[]'::jsonb)::text,'UTF8')),'hex') INTO after_checks FROM population_top3000_202608_202609_v1.pipeline_quality_evidence e WHERE family_id='CTX07';
IF before_checks IS DISTINCT FROM after_checks THEN RAISE EXCEPTION 'DELIVERY_WROTE_UNAUTHORIZED_QUALITY_CERTIFICATION';END IF;
END $test$;
SELECT 'PASS_ROLLBACK_ONLY_CTX07_PROVISIONAL_DELIVERY' result,clock_timestamp() observed_at,
'36fa6a66060f9771dd270c50ddeddc1c241e624ff1e0764f478eda2428ae364d' macro_manifest_sha256,
(SELECT jsonb_object_agg(stage,encode(sha256(convert_to(pg_get_functiondef(fn::regprocedure),'UTF8')),'hex')) FROM (VALUES ('REPAIR','population_top3000_202608_202609_v1.pipeline_repair_v1(uuid,text)'),
('PUBLISH','population_top3000_202608_202609_v1.pipeline_publish_v1(uuid,text)'),
('DELIVER','population_top3000_202608_202609_v1.pipeline_deliver_v1(uuid,text)')) v(stage,fn)) function_sha256,
(SELECT count(*) FROM population_top3000_202608_202609_v1.population_source_evidence_current_v1 WHERE family_id='CTX07') evidence_rows,
(SELECT jsonb_agg(jsonb_build_object('partition',partition_key,'result',v.result,'expected',v.expected_units,'received',v.received_units,'required_checks_missing',v.required_checks_missing) ORDER BY partition_key) FROM population_top3000_202608_202609_v1.pipeline_partition p JOIN population_top3000_202608_202609_v1.pipeline_validation v USING(family_id,partition_key) WHERE p.family_id='CTX07' AND v.validation_id=p.last_validation_id) quality,
(SELECT count(*) FROM population_top3000_202608_202609_v1.pipeline_delivery WHERE manifest->>'family_id'='CTX07') delivery_manifests,
population_top3000_202608_202609_v1.pipeline_full_gate_v1() full_gate;
ROLLBACK;
