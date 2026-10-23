-- Root actual runtime gate, after schema + both actual proof imports. No provider call.
-- Uses an explicit one-date/one-page archive subset; all changes roll back.
BEGIN;
SET LOCAL search_path=pg_catalog; SET LOCAL timezone='UTC'; SET LOCAL statement_timeout='60s'; SET LOCAL lock_timeout='500ms';
SELECT pg_advisory_xact_lock(2026083000261022);
SELECT cron.alter_job(596,active=>false);
CREATE TEMP TABLE archive_reference_fixture_marker_v1(id integer) ON COMMIT DROP;
CREATE FUNCTION pg_temp.archive_terminal_replay_probe_v1(p_attempt uuid,p_token text)
RETURNS jsonb LANGUAGE plpgsql SET search_path TO 'pg_catalog' AS $probe$
DECLARE v_stage text;v_first jsonb;v_second jsonb;v_n integer;
BEGIN
 SELECT t.stage INTO v_stage FROM population_top3000_202608_202609_v1.pipeline_attempt a
 JOIN population_top3000_202608_202609_v1.pipeline_task t ON t.task_id=a.task_id WHERE a.attempt_id=p_attempt;
 IF v_stage='PUBLISH' THEN v_first:=population_top3000_202608_202609_v1.archive_reference_publish_v1(p_attempt,p_token);
 ELSE v_first:=population_top3000_202608_202609_v1.archive_reference_deliver_v1(p_attempt,p_token); END IF;
 IF v_first->>'state'='COMPLETE' THEN
  IF v_stage='PUBLISH' THEN v_second:=population_top3000_202608_202609_v1.archive_reference_publish_v1(p_attempt,p_token);
  ELSE v_second:=population_top3000_202608_202609_v1.archive_reference_deliver_v1(p_attempt,p_token); END IF;
  IF v_second IS DISTINCT FROM jsonb_set(v_first,'{commit_receipt,new_committed_units}','0'::jsonb)
  THEN RAISE EXCEPTION 'ARCHIVE_TERMINAL_HANDLER_REPLAY_NOT_IDENTICAL'; END IF;
  v_n:=coalesce(nullif(current_setting('top3000.archive_terminal_replays',true),''),'0')::integer;
  PERFORM set_config('top3000.archive_terminal_replays',(v_n+1)::text,true);
 END IF;
 RETURN v_first;
END $probe$;
DO $test$
DECLARE v_control population_top3000_202608_202609_v1.acquisition_control%rowtype;
 v_pipeline population_top3000_202608_202609_v1.pipeline_control%rowtype;
 v_hashes jsonb;v_versions jsonb;v_payload jsonb;v_set_sha text;v_task uuid;v_reply jsonb;v_family text;
 v_pub uuid;v_proof jsonb;v_check jsonb;v_read jsonb;v_partitions jsonb;v_holds jsonb;v_universe jsonb;
 v_old_pointer uuid;v_old_members jsonb;v_members jsonb;v_negative integer:=0;v_dispatches integer:=0;
 v_timings jsonb:='[]';v_started timestamptz;v_elapsed numeric;v_unit text;v_expected_rows bigint;v_batch_count integer;v_i integer;
 v_original_values jsonb;v_reader_rows bigint;v_last_pub uuid;v_d02_pub uuid;v_s02_pub uuid;
BEGIN
 SELECT * INTO v_control FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton FOR UPDATE;
 SELECT * INTO v_pipeline FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton FOR UPDATE;
 IF v_control.owner_id IS DISTINCT FROM 'top3000-recovery-20261023-root-4b1314bcb5c2'
 OR v_control.owner_fence IS DISTINCT FROM 2::bigint OR v_control.stop_generation IS DISTINCT FROM 0::bigint
 OR v_control.stopped IS DISTINCT FROM false OR v_control.hold_reason IS NOT NULL OR v_control.admitted_until IS NULL OR v_control.admitted_until<=clock_timestamp()
 OR v_pipeline.expected_owner_id IS DISTINCT FROM v_control.owner_id OR v_pipeline.expected_owner_fence IS DISTINCT FROM v_control.owner_fence
 OR v_pipeline.expected_stop_generation IS DISTINCT FROM v_control.stop_generation OR v_pipeline.scope_sha256 IS DISTINCT FROM v_control.scope_sha256
 OR v_control.scope_sha256 IS DISTINCT FROM '8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd'
 OR v_pipeline.tick_count+9>v_pipeline.max_ticks
 THEN RAISE EXCEPTION 'ARCHIVE_TEST_OWNER_SCOPE_ADMISSION_OR_TICKS_CHANGED'; END IF;
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE status='LEASED')
 OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_provider_lease WHERE released_at IS NULL)
 THEN RAISE EXCEPTION 'ARCHIVE_TEST_REQUIRES_DRAINED_LEASES'; END IF;
 v_hashes:=population_top3000_202608_202609_v1.archive_reference_bundle_v1();
 SELECT jsonb_agg(to_jsonb(p) ORDER BY family_id,partition_key) INTO v_partitions FROM population_top3000_202608_202609_v1.pipeline_partition p WHERE family_id IN('D02','S02');
 SELECT jsonb_agg(to_jsonb(t)-'not_before' ORDER BY task_id) INTO v_holds FROM population_top3000_202608_202609_v1.pipeline_task t WHERE family_id IN('D02','S02') AND stage IN('VALIDATE','REPAIR');
 SELECT to_jsonb(u) INTO v_universe FROM population_top3000_202608_202609_v1.pipeline_universe u WHERE singleton;
 SELECT publication_id INTO v_old_pointer FROM population_top3000_202608_202609_v1.pipeline_release_pointer WHERE singleton;
 SELECT coalesce(jsonb_agg(to_jsonb(m)-'publication_id' ORDER BY family_id,partition_key),'[]'::jsonb) INTO v_old_members
 FROM population_top3000_202608_202609_v1.pipeline_publication_member m WHERE publication_id=v_old_pointer;
 INSERT INTO population_top3000_202608_202609_v1.pipeline_adapter(family_id,stage,function_oid,function_sha256,resource_class,enabled,reviewed_test_evidence,producer_revision,expected_batch_seconds)
 SELECT fam.family,x.stage,x.signature::regprocedure::oid,v_hashes->>x.stage,'DATABASE',true,
 jsonb_build_object('test_state','ROLLBACK_ONLY_IN_PROGRESS','function_sha256',v_hashes),'reference-archive-20261023-v1',5
 FROM (VALUES('D02'),('S02')) fam(family) CROSS JOIN
 (VALUES('PUBLISH','population_top3000_202608_202609_v1.archive_reference_publish_v1(uuid,text)'),('DELIVER','population_top3000_202608_202609_v1.archive_reference_deliver_v1(uuid,text)')) x(stage,signature)
 ON CONFLICT(family_id,stage) DO NOTHING;
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_adapter a WHERE family_id IN('D02','S02') AND stage IN('PUBLISH','DELIVER')
  AND (a.function_sha256 IS DISTINCT FROM v_hashes->>a.stage OR a.reviewed_test_evidence->'function_sha256' IS DISTINCT FROM v_hashes OR NOT a.enabled))
 THEN RAISE EXCEPTION 'ARCHIVE_TEST_EXISTING_BINDING_CONFLICT'; END IF;
 -- Rollback-only dispatcher wrapper calls each actual terminal handler twice while
 -- the same lease is still valid. All semantic production helpers remain hash-pinned.
 UPDATE population_top3000_202608_202609_v1.pipeline_adapter
 SET function_oid='pg_temp.archive_terminal_replay_probe_v1(uuid,text)'::regprocedure::oid,
 function_sha256=encode(sha256(convert_to(pg_get_functiondef('pg_temp.archive_terminal_replay_probe_v1(uuid,text)'::regprocedure),'UTF8')),'hex')
 WHERE family_id IN('D02','S02') AND stage IN('PUBLISH','DELIVER');
 PERFORM set_config('top3000.archive_terminal_replays','0',true);
 UPDATE population_top3000_202608_202609_v1.pipeline_task SET not_before='infinity'::timestamptz WHERE status IN('READY','BLOCKED');
 UPDATE population_top3000_202608_202609_v1.pipeline_control SET enabled=true,next_wakeup_at=clock_timestamp() WHERE singleton;
 FOREACH v_family IN ARRAY ARRAY['D02','S02'] LOOP
  SELECT encode(sha256(convert_to(string_agg(unit_ordinal::text||':'||proof_sha256,E'\n' ORDER BY unit_ordinal),'UTF8')),'hex') INTO v_set_sha
  FROM population_top3000_202608_202609_v1.archive_source_proof_v1 WHERE family_id=v_family;
  v_versions:=jsonb_build_object('scope_sha256',v_control.scope_sha256,'archive_publication_revision','reference-archive-20261023-v1',
   'publication_bundle_sha256',encode(sha256(convert_to(v_hashes::text,'UTF8')),'hex'),'all_source_proof_set_sha256',v_set_sha,
   'selection_sha256',encode(sha256(convert_to('[1]'::jsonb::text,'UTF8')),'hex'));
  v_payload:='{"publication_purpose":"VERIFIED_SOURCE_ARCHIVE_ONLY","proof_ordinals":[1]}'::jsonb;
  v_proof:=population_top3000_202608_202609_v1.archive_proof_v1(v_family,1);
  v_expected_rows:=(v_proof->>'canonical_rows')::bigint;
  v_task:=population_top3000_202608_202609_v1.pipeline_enqueue_v1(v_family,'PUBLISH','verified-source-archive-reference-v1',v_payload,v_versions,NULL);
  IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=v_task AND payload=v_payload AND input_versions=v_versions AND status='READY')
  THEN RAISE EXCEPTION 'ARCHIVE_TEST_TASK_CONFLICT'; END IF;
  -- One real PUBLISH followed by automatic resumable DELIVER; exact subset size is explicit.
  v_started:=clock_timestamp();v_reply:=population_top3000_202608_202609_v1.pipeline_tick_v1();
  v_elapsed:=extract(epoch FROM clock_timestamp()-v_started)*1000;v_dispatches:=v_dispatches+1;
  v_timings:=v_timings||jsonb_build_array(jsonb_build_object('family',v_family,'stage','PUBLISH','elapsed_ms',v_elapsed));
  IF v_reply->>'task_id' IS DISTINCT FROM v_task::text OR v_reply#>>'{result,state}' IS DISTINCT FROM 'COMPLETE' OR v_elapsed>5000
  THEN RAISE EXCEPTION 'ARCHIVE_ACTUAL_PUBLISH_RUNTIME_FAILED:% elapsed_ms=%',v_reply,v_elapsed; END IF;
  SELECT publication_id INTO v_pub FROM population_top3000_202608_202609_v1.pipeline_release_pointer WHERE singleton;
  IF v_family='D02' THEN v_d02_pub:=v_pub; ELSE v_s02_pub:=v_pub; END IF;
  SELECT count(*) INTO v_batch_count FROM population_top3000_202608_202609_v1.archive_publication_member_v1 m
   JOIN population_top3000_202608_202609_v1.archive_batch_snapshot_v1 b ON b.snapshot_id=m.snapshot_id WHERE m.publication_id=v_pub;
  FOR v_i IN 1..greatest(1,ceil(v_batch_count/3.0)::integer) LOOP
   v_started:=clock_timestamp();v_reply:=population_top3000_202608_202609_v1.pipeline_tick_v1();
   v_elapsed:=extract(epoch FROM clock_timestamp()-v_started)*1000;v_dispatches:=v_dispatches+1;
   v_timings:=v_timings||jsonb_build_array(jsonb_build_object('family',v_family,'stage','DELIVER','batch',v_i,'elapsed_ms',v_elapsed));
   IF v_reply#>>'{result,state}' IS DISTINCT FROM (CASE WHEN v_i=greatest(1,ceil(v_batch_count/3.0)::integer) THEN 'COMPLETE' ELSE 'CONTINUE' END)
    OR v_elapsed>5000 OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task t WHERE t.task_id=(v_reply->>'task_id')::uuid AND t.family_id=v_family AND t.stage='DELIVER')
   THEN RAISE EXCEPTION 'ARCHIVE_ACTUAL_DELIVER_RUNTIME_FAILED:% elapsed_ms=%',v_reply,v_elapsed; END IF;
  END LOOP;
  IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_delivery d WHERE d.publication_id=v_pub AND d.artifact_state='PENDING_EXPORT'
   AND d.manifest->>'source_rows_read'=v_expected_rows::text AND d.manifest->>'final_delivery'='false'
   AND d.manifest->>'required_family_validation_pass'='false')
  THEN RAISE EXCEPTION 'ARCHIVE_REAL_DELIVERY_MANIFEST_REQUIRED'; END IF;
  -- Real bounded consumer proves current values and all scope limitations are visible.
  v_read:=population_top3000_202608_202609_v1.population_archive_read_batch_v1(v_pub,v_family,1,1);
  IF v_read->>'result' IS DISTINCT FROM 'PASS_CURRENT_PUBLISHED_SOURCE_BATCH_ONLY' OR (v_read->>'row_count')::integer<>1000
   OR v_read->'company_population_verified' IS DISTINCT FROM 'false'::jsonb OR v_read->'historical_predictor_eligible' IS DISTINCT FROM 'false'::jsonb
  THEN RAISE EXCEPTION 'ARCHIVE_ACTUAL_BOUNDED_READER_FAILED'; END IF;
 END LOOP;
 -- A second publication must preserve the prior archive and all legacy snapshot references.
 IF (SELECT count(*) FROM population_top3000_202608_202609_v1.population_archive_catalog_current_v1 WHERE family_id IN('D02','S02'))<>2
 THEN RAISE EXCEPTION 'ARCHIVE_PREVIOUS_FAMILY_NOT_DISCOVERABLE'; END IF;
 SELECT publication_id INTO v_last_pub FROM population_top3000_202608_202609_v1.pipeline_release_pointer WHERE singleton;
 SELECT coalesce(jsonb_agg(to_jsonb(m)-'publication_id' ORDER BY family_id,partition_key),'[]'::jsonb) INTO v_members
 FROM population_top3000_202608_202609_v1.pipeline_publication_member m WHERE publication_id=v_last_pub;
 IF v_members IS DISTINCT FROM v_old_members THEN RAISE EXCEPTION 'ARCHIVE_LEGACY_SNAPSHOT_REFERENCES_CHANGED'; END IF;
 -- The existing reversible pointer excludes later archives when rolled back to the prior release.
 UPDATE population_top3000_202608_202609_v1.pipeline_release_pointer SET publication_id=v_old_pointer WHERE singleton;
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.population_archive_catalog_current_v1 WHERE publication_id IN(v_d02_pub,v_s02_pub))
 THEN RAISE EXCEPTION 'ARCHIVE_RELEASE_REVERSAL_FAILED'; END IF;
 UPDATE population_top3000_202608_202609_v1.pipeline_release_pointer SET publication_id=v_last_pub WHERE singleton;
 v_proof:=population_top3000_202608_202609_v1.archive_proof_v1('D02',1);
 BEGIN
  UPDATE population_top3000_202608_202609_v1.source_document SET content_text=content_text||' ',content_bytes=content_bytes+1,
   source_sha256=encode(sha256(convert_to(content_text||' ','UTF8')),'hex') WHERE source_id=v_proof->>'source_id';
  PERFORM population_top3000_202608_202609_v1.archive_source_unit_check_v1('D02',1);
  RAISE EXCEPTION 'NEGATIVE_SOURCE_BODY_ACCEPTED';
 EXCEPTION WHEN raise_exception THEN IF SQLERRM<>'ARCHIVE_SOURCE_DOCUMENT_BINDING_CHANGED' THEN RAISE; END IF;v_negative:=v_negative+1;END;
 BEGIN
  UPDATE population_top3000_202608_202609_v1.d02_daily_bar_v2 SET provider_close=provider_close+1
  WHERE source_day_id=(v_proof#>>'{postcheck,source_day_id}')::bigint AND source_row=1;
  PERFORM population_top3000_202608_202609_v1.archive_source_unit_check_v1('D02',1);
  RAISE EXCEPTION 'NEGATIVE_D02_TYPED_VALUE_ACCEPTED';
 EXCEPTION WHEN raise_exception THEN IF SQLERRM<>'ARCHIVE_D02_AUDITED_TYPED_VALUES_CHANGED' THEN RAISE; END IF;v_negative:=v_negative+1;END;
 BEGIN
  UPDATE population_top3000_202608_202609_v1.d02_daily_bar_v2 SET provider_close=provider_close+1
  WHERE source_day_id=(v_proof#>>'{postcheck,source_day_id}')::bigint AND source_row=1;
  PERFORM population_top3000_202608_202609_v1.population_archive_read_batch_v1(v_d02_pub,'D02',1,1);
  RAISE EXCEPTION 'NEGATIVE_PUBLISHED_D02_VALUE_ACCEPTED';
 EXCEPTION WHEN raise_exception THEN IF SQLERRM<>'ARCHIVE_PUBLISHED_REFERENCE_VALUES_CHANGED' THEN RAISE; END IF;v_negative:=v_negative+1;END;
 SELECT r.unit_key INTO v_unit FROM population_top3000_202608_202609_v1.massive_shortinterest_record_v1 r
 WHERE r.archive_key='massive-archive:S02:c1ed3ed6aa5bad110f122c2459ba27f6daa14a7ac484363d4915c0fc9ddb1f88' AND r.page_number=1 AND r.source_row=1;
 BEGIN
  UPDATE population_top3000_202608_202609_v1.source_observation SET values_json=jsonb_set(values_json,'{ticker}','"FORGED"'::jsonb),
   record_sha256=encode(sha256(convert_to(jsonb_build_object('values',jsonb_set(values_json,'{ticker}','"FORGED"'::jsonb),'states',field_state,'lineage',lineage)::text,'UTF8')),'hex')
  WHERE release_id='top3000_companies_202608_202609_v1' AND family_id='S02' AND partition_key='short-interest-settlement-20260715-20260930' AND unit_key=v_unit;
  PERFORM population_top3000_202608_202609_v1.archive_source_unit_check_v1('S02',1);
  RAISE EXCEPTION 'NEGATIVE_S02_FORGED_HASH_ACCEPTED';
 EXCEPTION WHEN raise_exception THEN IF SQLERRM<>'ARCHIVE_S02_AUDITED_NORMALIZED_VALUES_CHANGED' THEN RAISE; END IF;v_negative:=v_negative+1;END;
 BEGIN
  UPDATE population_top3000_202608_202609_v1.source_observation SET historical_received_at=clock_timestamp()
  WHERE release_id='top3000_companies_202608_202609_v1' AND family_id='S02' AND partition_key='short-interest-settlement-20260715-20260930' AND unit_key=v_unit;
  PERFORM population_top3000_202608_202609_v1.population_archive_read_batch_v1(v_s02_pub,'S02',1,1);
  RAISE EXCEPTION 'NEGATIVE_PUBLISHED_RECEIPT_ACCEPTED';
 EXCEPTION WHEN raise_exception THEN IF SQLERRM<>'ARCHIVE_PUBLISHED_REFERENCE_VALUES_CHANGED' THEN RAISE; END IF;v_negative:=v_negative+1;END;
 BEGIN
  PERFORM population_top3000_202608_202609_v1.archive_current_rows_v1(v_proof,1,1001);
  RAISE EXCEPTION 'NEGATIVE_UNBOUNDED_READ_ACCEPTED';
 EXCEPTION WHEN raise_exception THEN IF SQLERRM<>'ARCHIVE_READ_RANGE_MUST_BE_AT_MOST_1000' THEN RAISE; END IF;v_negative:=v_negative+1;END;
 -- Separate actual checkpoint test: first of two units must CONTINUE and cannot expose a partial new release.
 SELECT encode(sha256(convert_to(string_agg(unit_ordinal::text||':'||proof_sha256,E'\n' ORDER BY unit_ordinal),'UTF8')),'hex') INTO v_set_sha
 FROM population_top3000_202608_202609_v1.archive_source_proof_v1 WHERE family_id='D02';
 v_versions:=jsonb_build_object('scope_sha256',v_control.scope_sha256,'archive_publication_revision','reference-archive-20261023-v1',
 'publication_bundle_sha256',encode(sha256(convert_to(v_hashes::text,'UTF8')),'hex'),'all_source_proof_set_sha256',v_set_sha,
 'selection_sha256',encode(sha256(convert_to('[1,2]'::jsonb::text,'UTF8')),'hex'));
 v_task:=population_top3000_202608_202609_v1.pipeline_enqueue_v1('D02','PUBLISH','verified-source-archive-reference-v1',
 '{"publication_purpose":"VERIFIED_SOURCE_ARCHIVE_ONLY","proof_ordinals":[1,2]}'::jsonb,v_versions,NULL);
 v_started:=clock_timestamp();v_reply:=population_top3000_202608_202609_v1.pipeline_tick_v1();v_elapsed:=extract(epoch FROM clock_timestamp()-v_started)*1000;
 v_dispatches:=v_dispatches+1;v_timings:=v_timings||jsonb_build_array(jsonb_build_object('family','D02','stage','PUBLISH_CONTINUE_REUSE','elapsed_ms',v_elapsed));
 IF v_reply->>'task_id' IS DISTINCT FROM v_task::text OR v_reply#>>'{result,state}' IS DISTINCT FROM 'CONTINUE' OR v_elapsed>5000
  OR (SELECT publication_id FROM population_top3000_202608_202609_v1.pipeline_release_pointer WHERE singleton) IS DISTINCT FROM v_last_pub
 THEN RAISE EXCEPTION 'ARCHIVE_ACTUAL_CONTINUATION_OR_REPLAY_FAILED:% elapsed_ms=%',v_reply,v_elapsed; END IF;
 IF v_partitions IS DISTINCT FROM (SELECT jsonb_agg(to_jsonb(p) ORDER BY family_id,partition_key) FROM population_top3000_202608_202609_v1.pipeline_partition p WHERE family_id IN('D02','S02'))
  OR v_holds IS DISTINCT FROM (SELECT jsonb_agg(to_jsonb(t)-'not_before' ORDER BY task_id) FROM population_top3000_202608_202609_v1.pipeline_task t WHERE family_id IN('D02','S02') AND stage IN('VALIDATE','REPAIR'))
  OR v_universe IS DISTINCT FROM (SELECT to_jsonb(u) FROM population_top3000_202608_202609_v1.pipeline_universe u WHERE singleton)
  OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.acquisition_control c WHERE singleton AND
   (c.provider_requests<>v_control.provider_requests OR c.http_invocations<>v_control.http_invocations OR c.stored_bytes<>v_control.stored_bytes))
  OR population_top3000_202608_202609_v1.pipeline_full_gate_v1()->>'full_gate_pass' IS DISTINCT FROM 'false'
 THEN RAISE EXCEPTION 'ARCHIVE_REQUIRED_HOLDS_SCOPE_COUNTERS_OR_FULL_GATE_CHANGED'; END IF;
 IF current_setting('top3000.archive_terminal_replays') IS DISTINCT FROM '4' THEN RAISE EXCEPTION 'ARCHIVE_FOUR_TERMINAL_REPLAYS_REQUIRED'; END IF;
 PERFORM set_config('top3000.archive_reference_test_receipt',jsonb_build_object('result','PASS_ACTUAL_ARCHIVE_REFERENCE_PUBLISH_DELIVER_ROLLBACK',
  'function_sha256',v_hashes,'d02_rows',12402,'s02_rows',1000,'real_dispatches',v_dispatches,'negative_guards',v_negative,'identical_terminal_replays',4,
  'dispatch_timings',v_timings,'max_dispatch_ms',(SELECT max((j->>'elapsed_ms')::numeric) FROM jsonb_array_elements(v_timings) j),
  'all_required_holds_preserved',true,'legacy_publication_references_preserved',true,'release_reversal_verified',true,
  'source_only_reader_verified',true,'company_population_verified',false,'full_required_family_validation_pass',false,
  'final_delivery',false,'provider_requests',0,'source_payload_copies',0)::text,true);
END $test$;
SELECT clock_timestamp() observed_at,current_setting('top3000.archive_reference_test_receipt')::jsonb receipt;
ROLLBACK;
