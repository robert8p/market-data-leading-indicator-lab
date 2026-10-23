-- Root only. Actual retained source, actual dispatcher, no provider call; every write rolls back.
BEGIN;
SET LOCAL search_path=pg_catalog;
SET LOCAL statement_timeout='30s';
SET LOCAL lock_timeout='500ms';
SET LOCAL timezone='UTC';
SELECT pg_advisory_xact_lock(2026083000261022);
SELECT cron.alter_job(596,active=>false);
DO $test$
DECLARE v_control population_top3000_202608_202609_v1.acquisition_control%rowtype;
 v_pipeline population_top3000_202608_202609_v1.pipeline_control%rowtype;
 v_part population_top3000_202608_202609_v1.pipeline_partition%rowtype;
 v_hashes jsonb;v_check jsonb;v_reply jsonb;v_task uuid;v_dependency uuid;v_pub uuid;
 v_old_pointer uuid;v_old_members jsonb;v_new_members jsonb;v_repairs jsonb;v_after_repairs jsonb;
 v_universe jsonb;v_before_part jsonb;v_unit text;v_values jsonb;v_negative integer:=0;v_i integer;
BEGIN
 SELECT * INTO v_control FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton FOR UPDATE;
 SELECT * INTO v_pipeline FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton FOR UPDATE;
 IF v_control.owner_id IS DISTINCT FROM 'top3000-recovery-20261023-root-4b1314bcb5c2'
  OR v_control.owner_fence IS DISTINCT FROM 2::bigint OR v_control.stop_generation IS DISTINCT FROM 0::bigint
  OR v_control.stopped OR v_control.hold_reason IS NOT NULL OR v_control.admitted_until<=clock_timestamp()
  OR v_pipeline.expected_owner_id IS DISTINCT FROM v_control.owner_id
  OR v_pipeline.expected_owner_fence IS DISTINCT FROM v_control.owner_fence
  OR v_pipeline.expected_stop_generation IS DISTINCT FROM v_control.stop_generation
  OR v_pipeline.scope_sha256 IS DISTINCT FROM v_control.scope_sha256
  OR v_control.scope_sha256 IS DISTINCT FROM '8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd'
  OR v_pipeline.tick_count+2>v_pipeline.max_ticks
 THEN RAISE EXCEPTION 'N03_ARCHIVE_TEST_OWNER_ADMISSION_OR_TICK_BOUND_CHANGED'; END IF;
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE status='LEASED')
  OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_provider_lease WHERE released_at IS NULL)
 THEN RAISE EXCEPTION 'N03_ARCHIVE_TEST_REQUIRES_DRAINED_LEASES'; END IF;
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE family_id='N03' AND stage IN('PUBLISH','DELIVER'))
 THEN RAISE EXCEPTION 'N03_ARCHIVE_PUBLICATION_ALREADY_STARTED_RECONCILE'; END IF;
 SELECT * INTO v_part FROM population_top3000_202608_202609_v1.pipeline_partition
 WHERE family_id='N03' AND partition_key='splits-execution-202608-202609';
 v_before_part:=to_jsonb(v_part);
 SELECT to_jsonb(u) INTO v_universe FROM population_top3000_202608_202609_v1.pipeline_universe u WHERE singleton;
 SELECT publication_id INTO v_old_pointer FROM population_top3000_202608_202609_v1.pipeline_release_pointer WHERE singleton;
 SELECT coalesce(jsonb_agg(to_jsonb(m)-'publication_id' ORDER BY family_id,partition_key),'[]'::jsonb) INTO v_old_members
 FROM population_top3000_202608_202609_v1.pipeline_publication_member m WHERE publication_id=v_old_pointer;
 SELECT coalesce(jsonb_agg(to_jsonb(t) ORDER BY task_id),'[]'::jsonb) INTO v_repairs
 FROM population_top3000_202608_202609_v1.pipeline_task t WHERE family_id='N03' AND stage='REPAIR';
 IF jsonb_array_length(v_repairs)=0 OR EXISTS(SELECT 1 FROM jsonb_array_elements(v_repairs) r WHERE r->>'status' IS DISTINCT FROM 'BLOCKED')
 THEN RAISE EXCEPTION 'N03_ARCHIVE_ORIGINAL_REPAIR_HOLD_REQUIRED'; END IF;
 v_check:=population_top3000_202608_202609_v1.n03_archive_source_check_v1();
 IF v_check->>'result' IS DISTINCT FROM 'PASS_SOURCE_ARCHIVE_ONLY' THEN RAISE EXCEPTION 'N03_ARCHIVE_SOURCE_CHECK_DID_NOT_PASS'; END IF;
 -- Hash-consistent body modification still fails the pinned original-source requirement.
 BEGIN
  UPDATE population_top3000_202608_202609_v1.source_document SET content_text=content_text||' ',
   content_bytes=content_bytes+1,source_sha256=encode(sha256(convert_to(content_text||' ','UTF8')),'hex')
  WHERE source_id=v_check->>'source_id';
  PERFORM population_top3000_202608_202609_v1.n03_archive_source_check_v1();
  RAISE EXCEPTION 'NEGATIVE_BODY_DID_NOT_REJECT';
 EXCEPTION WHEN raise_exception THEN
  IF SQLERRM<>'N03_ARCHIVE_SOURCE_HASH_OR_METADATA_CHANGED' THEN RAISE; END IF;v_negative:=v_negative+1;
 END;
 SELECT unit_key,values_json INTO v_unit,v_values FROM population_top3000_202608_202609_v1.source_observation
 WHERE family_id='N03' AND partition_key=v_part.partition_key ORDER BY unit_key LIMIT 1;
 -- A forged normalized value with a correctly recomputed row hash must still fail source comparison.
 BEGIN
  UPDATE population_top3000_202608_202609_v1.source_observation SET
   values_json=jsonb_set(values_json,'{ticker}','"FORGED"'::jsonb),
   record_sha256=encode(sha256(convert_to(jsonb_build_object('values',jsonb_set(values_json,'{ticker}','"FORGED"'::jsonb),'states',field_state,'lineage',lineage)::text,'UTF8')),'hex')
  WHERE family_id='N03' AND partition_key=v_part.partition_key AND unit_key=v_unit;
  PERFORM population_top3000_202608_202609_v1.n03_archive_source_check_v1();
  RAISE EXCEPTION 'NEGATIVE_VALUE_DID_NOT_REJECT';
 EXCEPTION WHEN raise_exception THEN
  IF SQLERRM<>'N03_ARCHIVE_SOURCE_RECONCILIATION_FAILED' THEN RAISE; END IF;v_negative:=v_negative+1;
 END;
 BEGIN
  UPDATE population_top3000_202608_202609_v1.source_observation SET historical_received_at=clock_timestamp()
  WHERE family_id='N03' AND partition_key=v_part.partition_key AND unit_key=v_unit;
  PERFORM population_top3000_202608_202609_v1.n03_archive_source_check_v1();
  RAISE EXCEPTION 'NEGATIVE_RECEIPT_DID_NOT_REJECT';
 EXCEPTION WHEN raise_exception THEN
  IF SQLERRM<>'N03_ARCHIVE_SOURCE_RECONCILIATION_FAILED' THEN RAISE; END IF;v_negative:=v_negative+1;
 END;
 SELECT jsonb_object_agg(x.name,encode(sha256(convert_to(pg_get_functiondef(x.signature::regprocedure),'UTF8')),'hex'))
 INTO v_hashes FROM (VALUES
  ('SOURCE_CHECK','population_top3000_202608_202609_v1.n03_archive_source_check_v1()'),
  ('PUBLISH','population_top3000_202608_202609_v1.n03_archive_publish_v1(uuid,text)'),
  ('DELIVER','population_top3000_202608_202609_v1.n03_archive_deliver_v1(uuid,text)'),
  ('MEASURE','population_top3000_202608_202609_v1.pipeline_measure_partition_v1(text,text)'),
  ('ASOF','population_top3000_202608_202609_v1.population_read_asof_v1(timestamptz,boolean)')
 ) x(name,signature);
 INSERT INTO population_top3000_202608_202609_v1.pipeline_adapter
 (family_id,stage,function_oid,function_sha256,resource_class,enabled,reviewed_test_evidence,producer_revision,expected_batch_seconds)
 SELECT 'N03',x.stage,x.signature::regprocedure::oid,v_hashes->>x.stage,'DATABASE',true,
  jsonb_build_object('test_state','ROLLBACK_ONLY_IN_PROGRESS','function_sha256',v_hashes),'n03-source-archive-publication-v1',5
 FROM (VALUES('PUBLISH','population_top3000_202608_202609_v1.n03_archive_publish_v1(uuid,text)'),
  ('DELIVER','population_top3000_202608_202609_v1.n03_archive_deliver_v1(uuid,text)')) x(stage,signature)
 ON CONFLICT(family_id,stage) DO NOTHING;
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_adapter a WHERE a.family_id='N03'
  AND a.stage IN('PUBLISH','DELIVER') AND (NOT a.enabled OR a.function_sha256 IS DISTINCT FROM v_hashes->>a.stage
   OR a.reviewed_test_evidence->'function_sha256' IS DISTINCT FROM v_hashes))
 THEN RAISE EXCEPTION 'N03_ARCHIVE_TEST_BINDING_CONFLICT'; END IF;
 -- Isolate all READY and timed BLOCKED work only inside this rollback transaction.
 UPDATE population_top3000_202608_202609_v1.pipeline_task SET not_before='infinity'::timestamptz WHERE status IN('READY','BLOCKED');
 SELECT task_id INTO v_dependency FROM population_top3000_202608_202609_v1.pipeline_task
 WHERE family_id='N03' AND partition_key=v_part.partition_key AND stage='VALIDATE' AND status='COMPLETE'
  AND input_versions=v_part.immutable_input_versions ORDER BY completed_at DESC LIMIT 1;
 IF NOT FOUND THEN RAISE EXCEPTION 'N03_ARCHIVE_COMPLETED_FAMILY_VALIDATION_REQUIRED'; END IF;
 v_task:=population_top3000_202608_202609_v1.pipeline_enqueue_v1('N03','PUBLISH',v_part.partition_key,
  '{"publication_purpose":"VERIFIED_SOURCE_ARCHIVE_ONLY"}'::jsonb,v_part.immutable_input_versions,v_dependency);
 UPDATE population_top3000_202608_202609_v1.pipeline_control SET enabled=true,next_wakeup_at=clock_timestamp() WHERE singleton;
 FOR v_i IN 1..2 LOOP
  v_reply:=population_top3000_202608_202609_v1.pipeline_tick_v1();
  IF v_reply->>'state' IS DISTINCT FROM 'DISPATCHED' OR v_reply#>>'{result,state}' IS DISTINCT FROM 'COMPLETE'
   OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task t WHERE t.task_id=(v_reply->>'task_id')::uuid
    AND t.family_id='N03' AND t.stage=CASE v_i WHEN 1 THEN 'PUBLISH' ELSE 'DELIVER' END)
  THEN RAISE EXCEPTION 'N03_ARCHIVE_ACTUAL_DISPATCH_FAILED:%',v_reply; END IF;
 END LOOP;
 SELECT publication_id INTO v_pub FROM population_top3000_202608_202609_v1.pipeline_release_pointer WHERE singleton;
 SELECT coalesce(jsonb_agg(to_jsonb(m)-'publication_id' ORDER BY family_id,partition_key),'[]'::jsonb) INTO v_new_members
 FROM population_top3000_202608_202609_v1.pipeline_publication_member m WHERE publication_id=v_pub AND family_id<>'N03';
 IF v_new_members IS DISTINCT FROM v_old_members THEN RAISE EXCEPTION 'N03_ARCHIVE_PRIOR_RELEASE_REFERENCES_CHANGED'; END IF;
 IF (SELECT count(*) FROM population_top3000_202608_202609_v1.n03_source_archive_current_v1)<>297
  OR EXISTS(WITH source_rows AS(SELECT unit_key,to_jsonb(o) body FROM population_top3000_202608_202609_v1.source_observation o
    WHERE family_id='N03' AND partition_key=v_part.partition_key),published AS(
    SELECT unit_key,observation body FROM population_top3000_202608_202609_v1.population_source_evidence_current_v1 WHERE family_id='N03')
   SELECT 1 FROM ((SELECT * FROM source_rows EXCEPT SELECT * FROM published) UNION ALL
    (SELECT * FROM published EXCEPT SELECT * FROM source_rows)) differences)
 THEN RAISE EXCEPTION 'N03_ARCHIVE_CONSUMER_SOURCE_SNAPSHOT_MISMATCH'; END IF;
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.population_read_asof_v1(clock_timestamp(),false) WHERE family_id='N03')
  OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.population_read_asof_v1(clock_timestamp(),true) WHERE family_id='N03')
  OR population_top3000_202608_202609_v1.pipeline_full_gate_v1()->>'full_gate_pass' IS DISTINCT FROM 'false'
 THEN RAISE EXCEPTION 'N03_ARCHIVE_FALSE_ASOF_OR_MISSION_COMPLETION'; END IF;
 SELECT coalesce(jsonb_agg(to_jsonb(t)-'not_before' ORDER BY task_id),'[]'::jsonb) INTO v_after_repairs
 FROM population_top3000_202608_202609_v1.pipeline_task t WHERE family_id='N03' AND stage='REPAIR';
 IF v_after_repairs IS DISTINCT FROM (SELECT jsonb_agg(r-'not_before' ORDER BY r->>'task_id') FROM jsonb_array_elements(v_repairs) r)
  OR v_before_part IS DISTINCT FROM (SELECT to_jsonb(p) FROM population_top3000_202608_202609_v1.pipeline_partition p WHERE family_id='N03' AND partition_key=v_part.partition_key)
  OR v_universe IS DISTINCT FROM (SELECT to_jsonb(u) FROM population_top3000_202608_202609_v1.pipeline_universe u WHERE singleton)
  OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.acquisition_control c WHERE singleton
   AND (c.provider_requests<>v_control.provider_requests OR c.http_invocations<>v_control.http_invocations OR c.stored_bytes<>v_control.stored_bytes))
 THEN RAISE EXCEPTION 'N03_ARCHIVE_REQUIRED_HOLDS_UNIVERSE_OR_SOURCE_COUNTERS_CHANGED'; END IF;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_delivery d WHERE d.publication_id=v_pub
  AND d.artifact_state='PENDING_EXPORT' AND d.manifest->>'final_delivery'='false'
  AND d.manifest->>'source_evidence_rows_read'='297')
 THEN RAISE EXCEPTION 'N03_ARCHIVE_REAL_DELIVERY_MANIFEST_MISSING'; END IF;
 PERFORM set_config('top3000.n03_archive_test_receipt',jsonb_build_object('result','PASS_ROLLBACK_N03_SOURCE_ARCHIVE_PUBLICATION_ONLY',
  'source_sha256',v_check->'source_sha256','source_rows',297,'source_check',v_check,'function_sha256',v_hashes,
  'negative_guards',v_negative,'real_dispatches',2,'prior_publication_references_preserved',true,
  'required_family_validation_pass',false,'required_repair_hold_preserved',true,'final_delivery',false,'provider_requests',0)::text,true);
END $test$;
SELECT clock_timestamp() observed_at,current_setting('top3000.n03_archive_test_receipt')::jsonb receipt;
ROLLBACK;
