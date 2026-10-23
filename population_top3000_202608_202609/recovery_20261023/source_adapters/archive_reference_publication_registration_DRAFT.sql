-- Root only AFTER the actual rollback receipt is persisted as event
-- ARCHIVE_REFERENCE_PUBLICATION_ROLLBACK_VERIFIED, details=returned receipt.
-- Exactly 404 useful executions for the current 42/135 source-unit sets, before idle/retry allowance.
-- Any scheduler/tick budget extension is a separate root-reviewed admission; counters never reset here.
BEGIN;
SET LOCAL search_path=pg_catalog; SET LOCAL timezone='UTC'; SET LOCAL statement_timeout='8s'; SET LOCAL lock_timeout='500ms';
SELECT pg_advisory_xact_lock(2026083000261022);
DO $admit$
DECLARE v_control population_top3000_202608_202609_v1.acquisition_control%rowtype;
 v_pipeline population_top3000_202608_202609_v1.pipeline_control%rowtype;
 v_hashes jsonb;v_evidence jsonb;v_family text;v_selection jsonb;v_versions jsonb;v_payload jsonb;
 v_set_sha text;v_task uuid;v_count bigint;v_total bigint;v_batches bigint;v_ticks bigint;v_admitted jsonb:='[]';
BEGIN
 SELECT * INTO v_control FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton FOR UPDATE;
 SELECT * INTO v_pipeline FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton FOR UPDATE;
 IF v_control.owner_id IS DISTINCT FROM 'top3000-recovery-20261023-root-4b1314bcb5c2'
 OR v_control.owner_fence IS DISTINCT FROM 2::bigint OR v_control.stop_generation IS DISTINCT FROM 0::bigint
 OR v_control.stopped IS DISTINCT FROM false OR v_control.enabled IS DISTINCT FROM true OR v_control.hold_reason IS NOT NULL
 OR v_control.admitted_until IS NULL OR v_control.admitted_until<=clock_timestamp()
 OR v_pipeline.expected_owner_id IS DISTINCT FROM v_control.owner_id OR v_pipeline.expected_owner_fence IS DISTINCT FROM v_control.owner_fence
 OR v_pipeline.expected_stop_generation IS DISTINCT FROM v_control.stop_generation OR v_pipeline.scope_sha256 IS DISTINCT FROM v_control.scope_sha256
 OR v_control.scope_sha256 IS DISTINCT FROM '8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd'
 OR v_pipeline.shared_scheduler_verified IS DISTINCT FROM true OR v_pipeline.deadline IS NULL OR v_pipeline.deadline<=clock_timestamp()
 OR v_pipeline.tick_count+404>v_pipeline.max_ticks
 THEN RAISE EXCEPTION 'ARCHIVE_ADMISSION_OWNER_SCOPE_STOP_DEADLINE_OR_TICK_BOUNDS_CHANGED'; END IF;
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE status='LEASED')
 OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_provider_lease WHERE released_at IS NULL)
 THEN RAISE EXCEPTION 'ARCHIVE_ADMISSION_REQUIRES_DRAINED_LEASES'; END IF;
 v_hashes:=population_top3000_202608_202609_v1.archive_reference_bundle_v1();
 SELECT ev.details INTO v_evidence FROM population_top3000_202608_202609_v1.pipeline_event ev
 WHERE ev.event='ARCHIVE_REFERENCE_PUBLICATION_ROLLBACK_VERIFIED'
 AND ev.details->>'result'='PASS_ACTUAL_ARCHIVE_REFERENCE_PUBLISH_DELIVER_ROLLBACK'
 AND ev.details->'function_sha256'=v_hashes AND ev.details->'d02_rows'='12402'::jsonb AND ev.details->'s02_rows'='1000'::jsonb
 AND ev.details->'real_dispatches'='9'::jsonb AND ev.details->'negative_guards'='6'::jsonb
 AND ev.details->'identical_terminal_replays'='4'::jsonb AND (ev.details->>'max_dispatch_ms')::numeric BETWEEN 0 AND 5000
 AND ev.details->'all_required_holds_preserved'='true'::jsonb AND ev.details->'legacy_publication_references_preserved'='true'::jsonb
 AND ev.details->'release_reversal_verified'='true'::jsonb AND ev.details->'source_only_reader_verified'='true'::jsonb
 AND ev.details->'company_population_verified'='false'::jsonb AND ev.details->'full_required_family_validation_pass'='false'::jsonb
 AND ev.details->'final_delivery'='false'::jsonb AND ev.details->'provider_requests'='0'::jsonb AND ev.details->'source_payload_copies'='0'::jsonb
 ORDER BY ev.at DESC,ev.event_id DESC LIMIT 1;
 IF v_evidence IS NULL THEN RAISE EXCEPTION 'ARCHIVE_ACTUAL_MATCHING_REVISION_RUNTIME_RECEIPT_REQUIRED'; END IF;
 INSERT INTO population_top3000_202608_202609_v1.pipeline_adapter
 (family_id,stage,function_oid,function_sha256,resource_class,enabled,reviewed_test_evidence,producer_revision,expected_batch_seconds)
 SELECT fam.family,x.stage,x.signature::regprocedure::oid,v_hashes->>x.stage,'DATABASE',true,v_evidence,'reference-archive-20261023-v1',5
 FROM (VALUES('D02'),('S02')) fam(family) CROSS JOIN
 (VALUES('PUBLISH','population_top3000_202608_202609_v1.archive_reference_publish_v1(uuid,text)'),
 ('DELIVER','population_top3000_202608_202609_v1.archive_reference_deliver_v1(uuid,text)')) x(stage,signature)
 ON CONFLICT(family_id,stage) DO NOTHING;
 IF EXISTS(SELECT 1 FROM (VALUES('D02'),('S02')) fam(family) CROSS JOIN
 (VALUES('PUBLISH','population_top3000_202608_202609_v1.archive_reference_publish_v1(uuid,text)'),
 ('DELIVER','population_top3000_202608_202609_v1.archive_reference_deliver_v1(uuid,text)')) x(stage,signature)
 LEFT JOIN population_top3000_202608_202609_v1.pipeline_adapter a ON a.family_id=fam.family AND a.stage=x.stage
 WHERE a.function_oid IS DISTINCT FROM x.signature::regprocedure::oid OR a.function_sha256 IS DISTINCT FROM v_hashes->>x.stage
 OR a.enabled IS DISTINCT FROM true OR a.resource_class IS DISTINCT FROM 'DATABASE' OR a.reviewed_test_evidence->'function_sha256' IS DISTINCT FROM v_hashes)
 THEN RAISE EXCEPTION 'ARCHIVE_EXISTING_PUBLICATION_BINDING_CONFLICT'; END IF;
 FOREACH v_family IN ARRAY ARRAY['D02','S02'] LOOP
  SELECT count(*),sum((proof->>'canonical_rows')::bigint),sum(ceil((proof->>'source_occurrences')::numeric/1000))::bigint,
   jsonb_agg(unit_ordinal ORDER BY unit_ordinal),encode(sha256(convert_to(string_agg(unit_ordinal::text||':'||proof_sha256,E'\n' ORDER BY unit_ordinal),'UTF8')),'hex')
  INTO v_count,v_total,v_batches,v_selection,v_set_sha FROM population_top3000_202608_202609_v1.archive_source_proof_v1 WHERE family_id=v_family;
  IF v_count IS DISTINCT FROM (CASE v_family WHEN 'D02' THEN 42::bigint ELSE 135::bigint END)
   OR v_total IS DISTINCT FROM (CASE v_family WHEN 'D02' THEN 525999::bigint ELSE 134931::bigint END)
   OR v_batches IS DISTINCT FROM (CASE v_family WHEN 'D02' THEN 546::bigint ELSE 135::bigint END)
  THEN RAISE EXCEPTION 'ARCHIVE_CURRENT_PROOF_SET_COUNT_CHANGED'; END IF;
  -- Recheck both end anchors without reading large source bodies during admission.
  PERFORM population_top3000_202608_202609_v1.archive_proof_v1(v_family,1);
  PERFORM population_top3000_202608_202609_v1.archive_proof_v1(v_family,v_count::integer);
  v_ticks:=v_count+ceil(v_batches/3.0)::bigint;
  v_versions:=jsonb_build_object('scope_sha256',v_control.scope_sha256,'archive_publication_revision','reference-archive-20261023-v1',
   'publication_bundle_sha256',encode(sha256(convert_to(v_hashes::text,'UTF8')),'hex'),'all_source_proof_set_sha256',v_set_sha,
   'selection_sha256',encode(sha256(convert_to(v_selection::text,'UTF8')),'hex'));
  v_payload:=jsonb_build_object('publication_purpose','VERIFIED_SOURCE_ARCHIVE_ONLY','proof_ordinals',v_selection);
  v_task:=population_top3000_202608_202609_v1.pipeline_enqueue_v1(v_family,'PUBLISH','verified-source-archive-reference-v1',v_payload,v_versions,NULL);
  IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task t WHERE t.task_id=v_task AND t.payload=v_payload AND t.input_versions=v_versions)
  THEN RAISE EXCEPTION 'ARCHIVE_ADMISSION_EXISTING_TASK_PAYLOAD_CONFLICT'; END IF;
  IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_event ev WHERE ev.task_id=v_task AND ev.event='VERIFIED_SOURCE_ARCHIVE_PUBLICATION_ADMITTED' AND ev.details->'input_versions'=v_versions) THEN
   INSERT INTO population_top3000_202608_202609_v1.pipeline_event(task_id,event,details)
   VALUES(v_task,'VERIFIED_SOURCE_ARCHIVE_PUBLICATION_ADMITTED',jsonb_build_object('owner_id',v_control.owner_id,'owner_fence',v_control.owner_fence,'stop_generation',v_control.stop_generation,
    'family_id',v_family,'input_versions',v_versions,'source_units',v_count,'source_rows',v_total,'reader_batches',v_batches,'useful_execution_bound',v_ticks,
    'function_sha256',v_hashes,'provider_requests',0,'http_invocations',0,'source_payload_copies',0,'all_required_quality_holds_preserved',true,
    'full_required_family_validation_pass',false,'company_population_verified',false,'final_delivery',false));
  END IF;
  v_admitted:=v_admitted||jsonb_build_array(jsonb_build_object('family',v_family,'task_id',v_task,'source_units',v_count,'rows',v_total,'execution_bound',v_ticks));
 END LOOP;
 PERFORM population_top3000_202608_202609_v1.pipeline_wake_v1(v_control.owner_id,v_control.owner_fence,'TESTED_D02_S02_VERIFIED_SOURCE_ARCHIVE_PUBLICATION');
 PERFORM set_config('top3000.archive_reference_admitted',v_admitted::text,true);
END $admit$;
SELECT clock_timestamp() observed_at,current_setting('top3000.archive_reference_admitted')::jsonb admitted;
COMMIT;
