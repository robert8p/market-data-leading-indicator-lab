-- Root only, AFTER the actual rollback passed and its receipt was persisted as
-- pipeline_event.event=N03_ARCHIVE_PUBLICATION_ROLLBACK_VERIFIED, details=receipt.
-- Enqueues one source-only PUBLISH task; DELIVER follows in the existing dispatcher.
-- The existing required-family REPAIR task remains BLOCKED and is not superseded.
BEGIN;
SET LOCAL search_path=pg_catalog;
SET LOCAL statement_timeout='8s';
SET LOCAL lock_timeout='500ms';
SET LOCAL timezone='UTC';
SELECT pg_advisory_xact_lock(2026083000261022);
DO $admit$
DECLARE v_control population_top3000_202608_202609_v1.acquisition_control%rowtype;
 v_pipeline population_top3000_202608_202609_v1.pipeline_control%rowtype;
 v_part population_top3000_202608_202609_v1.pipeline_partition%rowtype;
 v_evidence jsonb;v_hashes jsonb;v_task uuid;v_dependency uuid;v_check jsonb;
BEGIN
 SELECT * INTO v_control FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton FOR UPDATE;
 SELECT * INTO v_pipeline FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton FOR UPDATE;
 IF v_control.owner_id IS DISTINCT FROM 'top3000-recovery-20261023-root-4b1314bcb5c2'
  OR v_control.owner_fence IS DISTINCT FROM 2::bigint OR v_control.stop_generation IS DISTINCT FROM 0::bigint
  OR v_control.stopped IS DISTINCT FROM false OR v_control.enabled IS DISTINCT FROM true
  OR v_control.hold_reason IS NOT NULL OR v_control.admitted_until IS NULL OR v_control.admitted_until<=clock_timestamp()
  OR v_pipeline.expected_owner_id IS DISTINCT FROM v_control.owner_id
  OR v_pipeline.expected_owner_fence IS DISTINCT FROM v_control.owner_fence
  OR v_pipeline.expected_stop_generation IS DISTINCT FROM v_control.stop_generation
  OR v_pipeline.scope_sha256 IS DISTINCT FROM v_control.scope_sha256
  OR v_control.scope_sha256 IS DISTINCT FROM '8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd'
  OR v_pipeline.shared_scheduler_verified IS DISTINCT FROM true
  OR v_pipeline.deadline IS NULL OR v_pipeline.deadline<=clock_timestamp()
  OR v_pipeline.tick_count+2>v_pipeline.max_ticks
 THEN RAISE EXCEPTION 'N03_ARCHIVE_ADMISSION_OWNER_SCOPE_STOP_OR_BOUNDS_CHANGED'; END IF;
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE status='LEASED')
  OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_provider_lease WHERE released_at IS NULL)
 THEN RAISE EXCEPTION 'N03_ARCHIVE_ADMISSION_REQUIRES_DRAINED_LEASES'; END IF;
 SELECT jsonb_object_agg(x.name,encode(sha256(convert_to(pg_get_functiondef(x.signature::regprocedure),'UTF8')),'hex'))
 INTO v_hashes FROM (VALUES
  ('SOURCE_CHECK','population_top3000_202608_202609_v1.n03_archive_source_check_v1()'),
  ('PUBLISH','population_top3000_202608_202609_v1.n03_archive_publish_v1(uuid,text)'),
  ('DELIVER','population_top3000_202608_202609_v1.n03_archive_deliver_v1(uuid,text)'),
  ('MEASURE','population_top3000_202608_202609_v1.pipeline_measure_partition_v1(text,text)'),
  ('ASOF','population_top3000_202608_202609_v1.population_read_asof_v1(timestamptz,boolean)')
 ) x(name,signature);
 SELECT ev.details INTO v_evidence FROM population_top3000_202608_202609_v1.pipeline_event ev
 WHERE ev.event='N03_ARCHIVE_PUBLICATION_ROLLBACK_VERIFIED'
  AND ev.details->>'result'='PASS_ROLLBACK_N03_SOURCE_ARCHIVE_PUBLICATION_ONLY'
  AND ev.details->'function_sha256'=v_hashes
  AND ev.details->>'source_sha256'='9609fe84088fc84a6ba2705270806fa06c1450cf460651819d10038be9fc0dfe'
  AND ev.details->'source_rows'='297'::jsonb AND ev.details->'negative_guards'='3'::jsonb
  AND ev.details->'real_dispatches'='2'::jsonb
  AND ev.details->'required_family_validation_pass'='false'::jsonb
  AND ev.details->'required_repair_hold_preserved'='true'::jsonb
  AND ev.details->'prior_publication_references_preserved'='true'::jsonb
  AND ev.details->'final_delivery'='false'::jsonb AND ev.details->'provider_requests'='0'::jsonb
 ORDER BY ev.at DESC,ev.event_id DESC LIMIT 1;
 IF v_evidence IS NULL THEN RAISE EXCEPTION 'N03_ARCHIVE_ACTUAL_MATCHING_REVISION_ROLLBACK_REQUIRED'; END IF;
 v_check:=population_top3000_202608_202609_v1.n03_archive_source_check_v1();
 IF v_check IS DISTINCT FROM v_evidence->'source_check' THEN RAISE EXCEPTION 'N03_ARCHIVE_SOURCE_CHANGED_AFTER_TEST'; END IF;
 SELECT * INTO v_part FROM population_top3000_202608_202609_v1.pipeline_partition
 WHERE family_id='N03' AND partition_key='splits-execution-202608-202609';
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task t
  WHERE t.family_id='N03' AND t.partition_key=v_part.partition_key AND t.stage='REPAIR'
   AND t.status='BLOCKED' AND t.input_versions=v_part.immutable_input_versions)
 THEN RAISE EXCEPTION 'N03_ARCHIVE_REQUIRED_QUALITY_REPAIR_HOLD_MISSING'; END IF;
 INSERT INTO population_top3000_202608_202609_v1.pipeline_adapter
 (family_id,stage,function_oid,function_sha256,resource_class,enabled,reviewed_test_evidence,producer_revision,expected_batch_seconds)
 SELECT 'N03',x.stage,x.signature::regprocedure::oid,v_hashes->>x.stage,'DATABASE',true,v_evidence,
  'n03-source-archive-publication-v1',5
 FROM (VALUES('PUBLISH','population_top3000_202608_202609_v1.n03_archive_publish_v1(uuid,text)'),
  ('DELIVER','population_top3000_202608_202609_v1.n03_archive_deliver_v1(uuid,text)')) x(stage,signature)
 ON CONFLICT(family_id,stage) DO NOTHING;
 IF EXISTS(SELECT 1 FROM (VALUES('PUBLISH','population_top3000_202608_202609_v1.n03_archive_publish_v1(uuid,text)'),
  ('DELIVER','population_top3000_202608_202609_v1.n03_archive_deliver_v1(uuid,text)')) x(stage,signature)
  LEFT JOIN population_top3000_202608_202609_v1.pipeline_adapter a ON a.family_id='N03' AND a.stage=x.stage
  WHERE a.function_oid IS DISTINCT FROM x.signature::regprocedure::oid OR a.function_sha256 IS DISTINCT FROM v_hashes->>x.stage
   OR a.enabled IS DISTINCT FROM true OR a.resource_class IS DISTINCT FROM 'DATABASE'
   OR a.reviewed_test_evidence->'function_sha256' IS DISTINCT FROM v_hashes)
 THEN RAISE EXCEPTION 'N03_ARCHIVE_EXISTING_PUBLICATION_BINDING_CONFLICT'; END IF;
 SELECT task_id INTO v_dependency FROM population_top3000_202608_202609_v1.pipeline_task
 WHERE family_id='N03' AND partition_key=v_part.partition_key AND stage='VALIDATE' AND status='COMPLETE'
  AND input_versions=v_part.immutable_input_versions ORDER BY completed_at DESC LIMIT 1;
 IF NOT FOUND THEN RAISE EXCEPTION 'N03_ARCHIVE_COMPLETED_FAMILY_VALIDATION_REQUIRED'; END IF;
 v_task:=population_top3000_202608_202609_v1.pipeline_enqueue_v1('N03','PUBLISH',v_part.partition_key,
  '{"publication_purpose":"VERIFIED_SOURCE_ARCHIVE_ONLY"}'::jsonb,v_part.immutable_input_versions,v_dependency);
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task t WHERE t.task_id=v_task
  AND t.payload='{"publication_purpose":"VERIFIED_SOURCE_ARCHIVE_ONLY"}'::jsonb AND t.input_versions=v_part.immutable_input_versions)
 THEN RAISE EXCEPTION 'N03_ARCHIVE_EXISTING_TASK_CONFLICT'; END IF;
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(task_id,event,details)
 VALUES(v_task,'N03_SOURCE_ARCHIVE_PUBLICATION_ADMITTED',jsonb_build_object(
  'owner_id',v_control.owner_id,'owner_fence',v_control.owner_fence,'stop_generation',v_control.stop_generation,
  'function_sha256',v_hashes,'source_check',v_check,'new_provider_requests',0,'new_http_invocations',0,
  'required_family_validation_pass',false,'required_repair_hold_preserved',true,'mission_final_delivery',false));
 PERFORM population_top3000_202608_202609_v1.pipeline_wake_v1(v_control.owner_id,v_control.owner_fence,'TESTED_N03_SOURCE_ARCHIVE_PUBLICATION_ONLY');
END $admit$;
COMMIT;
