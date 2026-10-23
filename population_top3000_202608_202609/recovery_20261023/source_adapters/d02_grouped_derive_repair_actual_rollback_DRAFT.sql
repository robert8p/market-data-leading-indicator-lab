-- ROOT ONLY, actual retained 12,402-row Aug3 source. No synthetic replacement, provider call or worker dispatch.
-- Roll back all observed-row changes, preserved-baseline rows, binding/task/attempt/receipt changes.
BEGIN;
SET LOCAL lock_timeout='3s';SET LOCAL statement_timeout='60s';SET LOCAL timezone='UTC';
SELECT pg_advisory_xact_lock(2026083000261022);
CREATE TEMP TABLE d02_repair_before_observations ON COMMIT DROP AS
 SELECT o.unit_key,o.source_record_key,o.values_json,o.field_state,o.lineage,o.record_sha256,
 to_jsonb(o)-'values_json'-'field_state'-'lineage'-'record_sha256' AS unchanged_metadata
 FROM population_top3000_202608_202609_v1.source_observation o WHERE o.release_id='top3000_companies_202608_202609_v1' AND o.family_id='D02' AND o.partition_key='grouped-20260803-raw-no-otc';
ALTER TABLE d02_repair_before_observations ADD PRIMARY KEY(unit_key);
CREATE TEMP TABLE d02_repair_test_result(result jsonb) ON COMMIT DROP;
DO $test$
DECLARE v_tid uuid:='2c92d689-55bb-4f1e-83ac-1bd13ecb71c6';v_task population_top3000_202608_202609_v1.pipeline_task%rowtype;v_archive population_top3000_202608_202609_v1.d02_grouped_archive_v1%rowtype;
 v_row_count integer;v_baseline text;v_deriver_sha text;v_repair_bundle text;v_binding jsonb;v_evidence jsonb;
 v_a uuid;v_token text;v_receipt jsonb;v_replay jsonb;v_tick jsonb;v_rejected boolean;v_batches integer:=1;v_started timestamptz:=clock_timestamp();
 v_expected_before text;v_expected_after text;v_universe_before text;v_universe_after text;v_requests bigint;v_stored bigint;v_doc_before text;v_profile jsonb;
BEGIN
 v_evidence:='{"status":"IN_PROGRESS_ACTUAL_RETAINED_SOURCE_ROLLBACK_ONLY","live_provider_requested":false,"provider_accuracy_verified":false,"actual_source_rows":12402}'::jsonb;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton AND owner_id='top3000-recovery-20261023-root-4b1314bcb5c2' AND owner_fence=2 AND stop_generation=0 AND enabled AND NOT stopped AND hold_reason IS NULL AND admitted_until>clock_timestamp()) THEN RAISE EXCEPTION 'D02_REPAIR_OWNER_OR_ADMISSION_CHANGED';END IF;
 IF EXISTS(SELECT 1 FROM cron.job WHERE jobid=596 AND active) THEN RAISE EXCEPTION 'D02_REPAIR_SHARED_CRON_MUST_REMAIN_PAUSED_DURING_TRANSFER';END IF;
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE status='LEASED') OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_provider_lease WHERE released_at IS NULL) THEN RAISE EXCEPTION 'D02_REPAIR_EXISTING_ATTEMPTS_MUST_FINISH';END IF;
 SELECT * INTO v_task FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=v_tid FOR UPDATE;
 IF NOT FOUND OR v_task.family_id<>'D02' OR v_task.stage<>'DERIVE' OR v_task.partition_key<>'grouped-20260803-raw-no-otc' OR v_task.status<>'READY' OR v_task.executions<>0 OR v_task.checkpoint<>'{}'::jsonb
 OR v_task.input_versions IS DISTINCT FROM '{"scope_sha256":"8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd","grouped_bundle_sha256":"3e267e75a2c471ca6975c9ffe3dc9baabf93640500398992d9f32bf0482085da","source_contract_revision":"massive-grouped-20260803-v1"}'::jsonb THEN RAISE EXCEPTION 'D02_REPAIR_ORIGINAL_TASK_CHANGED';END IF;
 IF population_top3000_202608_202609_v1.d02_grouped_bundle_sha256_v1()<>'3e267e75a2c471ca6975c9ffe3dc9baabf93640500398992d9f32bf0482085da'
 OR encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.d02_grouped_normalize_v1(uuid,text)'::regprocedure),'UTF8')),'hex')<>'eddc9f2ef9e4640b59452e3a3a1a17cad7e3ba0bc96c80f1686e7af4d3fff331'
 OR encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.d02_grouped_derive_v1(uuid,text)'::regprocedure),'UTF8')),'hex')<>'a0d404181138d9f6ec2e4e71e4e797e4d92c744672e3c4e48146aa6d50abc45d' THEN RAISE EXCEPTION 'D02_REPAIR_IMMUTABLE_SOURCE_BUNDLE_CHANGED';END IF;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_adapter WHERE family_id='D02' AND stage='DERIVE' AND function_oid='population_top3000_202608_202609_v1.d02_grouped_derive_v1(uuid,text)'::regprocedure::oid AND function_sha256='a0d404181138d9f6ec2e4e71e4e797e4d92c744672e3c4e48146aa6d50abc45d' AND enabled AND resource_class='DATABASE' AND provider IS NULL) THEN RAISE EXCEPTION 'D02_REPAIR_OLD_ADAPTER_CHANGED';END IF;
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.d02_grouped_repair_binding_v2) OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.d02_grouped_repair_baseline_v2) THEN RAISE EXCEPTION 'D02_REPAIR_ALREADY_STARTED_RECONCILE_PRESERVED_CHECKPOINT';END IF;
 SELECT * INTO v_archive FROM population_top3000_202608_202609_v1.d02_grouped_archive_v1 WHERE archive_key='d02-massive-grouped:4b94dfcf6c9e0582557862aa631bf1480a12dbc433ed0a53f2b3b844c6db2925';
 IF NOT FOUND OR v_archive.source_sha256<>'787163eb9f6ffb408898c8a133c6ca63c822577187793d25c94b436f639bb97b' OR v_archive.result_count<>12402 OR v_archive.response_bytes<>1355997 OR v_archive.normalized_source_records<>12402 OR v_archive.normalized_at IS NULL THEN RAISE EXCEPTION 'D02_REPAIR_ACTUAL_12402_ROW_SOURCE_REQUIRED';END IF;
 SELECT count(*),encode(sha256(convert_to(coalesce(string_agg(o.unit_key||':'||o.record_sha256,E'\n' ORDER BY o.unit_key),''),'UTF8')),'hex') INTO v_row_count,v_baseline
 FROM population_top3000_202608_202609_v1.source_observation o WHERE o.release_id='top3000_companies_202608_202609_v1' AND o.family_id='D02' AND o.partition_key=v_task.partition_key AND o.vintage_key=v_archive.archive_key;
 IF v_row_count<>12402 OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.source_observation o WHERE o.release_id='top3000_companies_202608_202609_v1' AND o.family_id='D02' AND o.partition_key=v_task.partition_key
 AND (o.vintage_key IS DISTINCT FROM v_archive.archive_key OR o.source_id IS DISTINCT FROM v_archive.source_id OR o.normalization_sha256<>'eddc9f2ef9e4640b59452e3a3a1a17cad7e3ba0bc96c80f1686e7af4d3fff331' OR o.lineage ? 'derivation_function_sha256' OR o.values_json ? 'dollar_volume_vwap_approximation')) THEN RAISE EXCEPTION 'D02_REPAIR_ACTUAL_NORMALIZED_BASELINE_CHANGED';END IF;
 v_deriver_sha:=encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.d02_grouped_derive_repair_v2(uuid,text)'::regprocedure),'UTF8')),'hex');v_repair_bundle:=population_top3000_202608_202609_v1.d02_grouped_repair_bundle_sha256_v2();
 v_binding:=jsonb_build_object('task_id',v_tid,'release_id','top3000_companies_202608_202609_v1','partition_key',v_task.partition_key,'archive_key',v_archive.archive_key,'source_id',v_archive.source_id,'source_sha256',v_archive.source_sha256,'input_versions',v_task.input_versions,'original_bundle_sha256',v_task.input_versions->>'grouped_bundle_sha256','original_normalizer_sha256','eddc9f2ef9e4640b59452e3a3a1a17cad7e3ba0bc96c80f1686e7af4d3fff331','original_deriver_sha256','a0d404181138d9f6ec2e4e71e4e797e4d92c744672e3c4e48146aa6d50abc45d','repair_bundle_sha256',v_repair_bundle,'repair_deriver_sha256',v_deriver_sha,'source_records',12402,'normalized_rows',v_row_count,'baseline_normalized_sha256',v_baseline,'batch_limit',500);
 v_binding:=v_binding||jsonb_build_object('binding_sha256',population_top3000_202608_202609_v1.d02_grouped_repair_binding_hash_v2(v_binding),'evidence',v_evidence,'admitted_at',clock_timestamp());
 INSERT INTO population_top3000_202608_202609_v1.d02_grouped_repair_binding_v2 SELECT (jsonb_populate_record(NULL::population_top3000_202608_202609_v1.d02_grouped_repair_binding_v2,v_binding)).*;
 UPDATE population_top3000_202608_202609_v1.pipeline_adapter SET function_oid='population_top3000_202608_202609_v1.d02_grouped_derive_repair_v2(uuid,text)'::regprocedure::oid,function_sha256=v_deriver_sha,producer_revision='d02-bounded-derivation-repair-20261023-v2',expected_batch_seconds=10,
 reviewed_test_evidence=reviewed_test_evidence||jsonb_build_object('bounded_repair',v_evidence,'original_producer_bundle_preserved',true,'original_deriver_sha256','a0d404181138d9f6ec2e4e71e4e797e4d92c744672e3c4e48146aa6d50abc45d','repair_bundle_sha256',v_repair_bundle,'batch_limit',500)
 WHERE family_id='D02' AND stage='DERIVE';
 PERFORM population_top3000_202608_202609_v1.d02_grouped_repair_context_v2(v_tid);

 SELECT provider_requests,stored_bytes INTO v_requests,v_stored FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton;
 SELECT encode(sha256(convert_to(jsonb_agg(to_jsonb(e) ORDER BY unit_key)::text,'UTF8')),'hex') INTO v_expected_before FROM population_top3000_202608_202609_v1.expected_observation e WHERE e.release_id='top3000_companies_202608_202609_v1' AND e.family_id='D02' AND e.partition_key=v_task.partition_key;
 SELECT encode(sha256(convert_to(coalesce(jsonb_agg(to_jsonb(u) ORDER BY to_jsonb(u)::text),'[]'::jsonb)::text,'UTF8')),'hex') INTO v_universe_before FROM population_top3000_202608_202609_v1.pipeline_universe u;
 SELECT encode(sha256(convert_to(to_jsonb(d)::text,'UTF8')),'hex') INTO v_doc_before FROM population_top3000_202608_202609_v1.source_document d WHERE source_id=v_archive.source_id;
 UPDATE population_top3000_202608_202609_v1.pipeline_task SET not_before='infinity'::timestamptz,blocker_until=NULL WHERE status IN('READY','BLOCKED') AND task_id<>v_tid;
 v_a:=gen_random_uuid();v_token:=repeat('a',64);
 UPDATE population_top3000_202608_202609_v1.pipeline_task SET status='LEASED',attempt_id=v_a,lease_until=clock_timestamp()+interval '60 seconds',owner_fence=2,stop_generation=0,executions=executions+1 WHERE task_id=v_tid;
 INSERT INTO population_top3000_202608_202609_v1.pipeline_attempt(attempt_id,task_id,execution_number,token_sha256,owner_fence,stop_generation,adapter_sha256)
 SELECT v_a,v_tid,executions,encode(sha256(convert_to(v_token,'UTF8')),'hex'),2,0,v_deriver_sha FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=v_tid;
 v_receipt:=population_top3000_202608_202609_v1.d02_grouped_derive_repair_v2(v_a,v_token);
 IF v_receipt->>'state'<>'CONTINUE' OR v_receipt#>>'{commit_receipt,batch_rows}'<>'500' OR v_receipt#>>'{commit_receipt,new_committed_units}'<>'500' THEN RAISE EXCEPTION 'D02_REPAIR_FIRST_ACTUAL_BATCH_FAILED %',v_receipt;END IF;
 v_replay:=population_top3000_202608_202609_v1.d02_grouped_derive_repair_v2(v_a,v_token);
 IF v_replay#>>'{commit_receipt,new_committed_units}'<>'0' OR v_replay#>>'{commit_receipt,output_sha256}' IS DISTINCT FROM v_receipt#>>'{commit_receipt,output_sha256}' OR v_replay->'checkpoint' IS DISTINCT FROM v_receipt->'checkpoint' THEN RAISE EXCEPTION 'D02_REPAIR_FULL_REPLAY_NOT_IDEMPOTENT';END IF;
 v_rejected:=false;
 BEGIN
  UPDATE population_top3000_202608_202609_v1.source_observation SET values_json=jsonb_set(values_json,'{dollar_volume_vwap_approximation}',to_jsonb(coalesce((values_json->>'dollar_volume_vwap_approximation')::numeric,0)+1)),record_sha256=encode(sha256(convert_to(jsonb_build_object('values',jsonb_set(values_json,'{dollar_volume_vwap_approximation}',to_jsonb(coalesce((values_json->>'dollar_volume_vwap_approximation')::numeric,0)+1)),'states',field_state,'lineage',lineage)::text,'UTF8')),'hex')
  WHERE release_id='top3000_companies_202608_202609_v1' AND family_id='D02' AND partition_key=v_task.partition_key AND unit_key=(SELECT min(unit_key) FROM population_top3000_202608_202609_v1.d02_grouped_repair_baseline_v2 WHERE task_id=v_tid);
  PERFORM population_top3000_202608_202609_v1.d02_grouped_derive_repair_v2(v_a,v_token);
 EXCEPTION WHEN OTHERS THEN IF SQLERRM='D02_REPAIR_EXISTING_ROW_REPLAY_CONFLICT' THEN v_rejected:=true;ELSE RAISE;END IF;END;
 IF NOT v_rejected THEN RAISE EXCEPTION 'D02_REPAIR_ATOMIC_FORGED_OUTPUT_ACCEPTED';END IF;
 v_rejected:=false;
 BEGIN
  UPDATE population_top3000_202608_202609_v1.pipeline_control SET budget_evidence=jsonb_set(budget_evidence,'{resource_limits,max_relation_bytes}','1'::jsonb) WHERE singleton;
  PERFORM population_top3000_202608_202609_v1.d02_grouped_derive_repair_v2(v_a,v_token);
 EXCEPTION WHEN OTHERS THEN IF SQLERRM='D02_REPAIR_RELATION_CAP_EXCEEDED' THEN v_rejected:=true;ELSE RAISE;END IF;END;
 IF NOT v_rejected THEN RAISE EXCEPTION 'D02_REPAIR_RESOURCE_GUARD_NOT_EXERCISED';END IF;
 PERFORM population_top3000_202608_202609_v1.pipeline_finish_v1(v_a,v_token,v_receipt);
 UPDATE population_top3000_202608_202609_v1.pipeline_control SET next_wakeup_at=clock_timestamp() WHERE singleton;
 FOR i IN 1..30 LOOP
  v_tick:=population_top3000_202608_202609_v1.pipeline_tick_v1();v_batches:=v_batches+1;
  IF v_tick->>'state' IS DISTINCT FROM 'DISPATCHED' OR v_tick->>'task_id' IS DISTINCT FROM v_tid::text OR v_tick#>>'{result,state}' NOT IN('CONTINUE','COMPLETE') THEN RAISE EXCEPTION 'D02_REPAIR_REAL_DISPATCH_FAILED batch=% receipt=%',v_batches,v_tick;END IF;
  EXIT WHEN v_tick#>>'{result,state}'='COMPLETE';
 END LOOP;
 IF v_batches<>25 OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=v_tid AND status='COMPLETE' AND input_versions=v_task.input_versions) THEN RAISE EXCEPTION 'D02_REPAIR_ACTUAL_12402_ROWS_DID_NOT_COMPLETE_IN_25_BOUNDED_BATCHES';END IF;
 IF (SELECT count(*) FROM population_top3000_202608_202609_v1.d02_grouped_repair_baseline_v2 WHERE task_id=v_tid)<>12402 THEN RAISE EXCEPTION 'D02_REPAIR_PRE_NORMALIZED_EVIDENCE_NOT_PRESERVED';END IF;
 WITH conflicts AS MATERIALIZED (SELECT source_record_key FROM d02_repair_before_observations GROUP BY source_record_key HAVING count(*)>1)
 SELECT EXISTS(SELECT 1 FROM d02_repair_before_observations prev JOIN population_top3000_202608_202609_v1.source_observation o ON o.release_id='top3000_companies_202608_202609_v1' AND o.family_id='D02' AND o.partition_key=v_task.partition_key AND o.unit_key=prev.unit_key
 LEFT JOIN conflicts cf ON cf.source_record_key=prev.source_record_key
 JOIN population_top3000_202608_202609_v1.d02_grouped_repair_baseline_v2 q ON q.task_id=v_tid AND q.release_id=o.release_id AND q.family_id=o.family_id AND q.partition_key=o.partition_key AND q.unit_key=o.unit_key
 WHERE (to_jsonb(o)-'values_json'-'field_state'-'lineage'-'record_sha256') IS DISTINCT FROM prev.unchanged_metadata
 OR (o.values_json-'dollar_volume_vwap_approximation') IS DISTINCT FROM prev.values_json
 OR (o.field_state-'dollar_volume_vwap_approximation'-'ticker') IS DISTINCT FROM (prev.field_state-'ticker')
 OR o.field_state->>'dollar_volume_vwap_approximation' IS DISTINCT FROM (CASE WHEN prev.field_state->>'share_volume'='VALUE' AND prev.field_state->>'provider_vwap'='VALUE' THEN 'VALUE' ELSE 'UNAVAILABLE' END)
 OR o.field_state->>'ticker' IS DISTINCT FROM (CASE WHEN cf.source_record_key IS NOT NULL THEN 'CONFLICTING' ELSE prev.field_state->>'ticker' END)
 OR o.values_json->'dollar_volume_vwap_approximation' IS DISTINCT FROM coalesce(to_jsonb(CASE WHEN prev.field_state->>'share_volume'='VALUE' AND prev.field_state->>'provider_vwap'='VALUE' THEN (prev.values_json->>'share_volume')::numeric*(prev.values_json->>'provider_vwap')::numeric ELSE NULL::numeric END),'null'::jsonb)
 OR (o.lineage-ARRAY['derivation_function_sha256','derivation_revision','dollar_volume_formula','raw_price_adjustment_applied','repair_binding_sha256','pre_derivation_record_sha256','original_source_bundle_sha256']) IS DISTINCT FROM prev.lineage
 OR q.pre_values_json IS DISTINCT FROM prev.values_json OR q.pre_field_state IS DISTINCT FROM prev.field_state OR q.pre_lineage IS DISTINCT FROM prev.lineage OR q.pre_record_sha256 IS DISTINCT FROM prev.record_sha256 OR q.post_record_sha256 IS DISTINCT FROM o.record_sha256) INTO v_rejected;
 IF v_rejected THEN RAISE EXCEPTION 'D02_REPAIR_ALL_ROW_FORMULA_OR_PRESERVED_SOURCE_EQUIVALENCE_FAILED';END IF;
 SELECT encode(sha256(convert_to(jsonb_agg(to_jsonb(e) ORDER BY unit_key)::text,'UTF8')),'hex') INTO v_expected_after FROM population_top3000_202608_202609_v1.expected_observation e WHERE e.release_id='top3000_companies_202608_202609_v1' AND e.family_id='D02' AND e.partition_key=v_task.partition_key;
 SELECT encode(sha256(convert_to(coalesce(jsonb_agg(to_jsonb(u) ORDER BY to_jsonb(u)::text),'[]'::jsonb)::text,'UTF8')),'hex') INTO v_universe_after FROM population_top3000_202608_202609_v1.pipeline_universe u;
 IF v_expected_before IS DISTINCT FROM v_expected_after OR v_universe_before IS DISTINCT FROM v_universe_after OR v_doc_before IS DISTINCT FROM (SELECT encode(sha256(convert_to(to_jsonb(d)::text,'UTF8')),'hex') FROM population_top3000_202608_202609_v1.source_document d WHERE source_id=v_archive.source_id)
 OR population_top3000_202608_202609_v1.d02_grouped_bundle_sha256_v1() IS DISTINCT FROM v_task.input_versions->>'grouped_bundle_sha256'
 OR (SELECT provider_requests FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton)<>v_requests OR (SELECT stored_bytes FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton)<>v_stored THEN RAISE EXCEPTION 'D02_REPAIR_CHANGED_ORIGINAL_SOURCE_EXPECTATIONS_UNIVERSE_OR_PROVIDER_COUNTERS';END IF;
 SELECT jsonb_build_object('batches',count(*),'max_batch_rows',max((receipt->>'batch_rows')::integer),'total_new_rows',sum(new_committed_units),'min_batch_ms',min((receipt->>'elapsed_ms')::numeric),'max_batch_ms',max((receipt->>'elapsed_ms')::numeric),'total_batch_ms',sum((receipt->>'elapsed_ms')::numeric)) INTO v_profile FROM population_top3000_202608_202609_v1.pipeline_commit WHERE task_id=v_tid;
 INSERT INTO d02_repair_test_result VALUES(jsonb_build_object('result','PASS','test','ACTUAL_RETAINED_D02_12402_ROW_BOUNDED_REPAIR_ROLLBACK','source_records',12402,'normalized_rows',v_row_count,'baseline_normalized_sha256',v_baseline,'original_task_id',v_tid,'original_input_versions',v_task.input_versions,'original_bundle_sha256',v_task.input_versions->>'grouped_bundle_sha256','repair_bundle_sha256',v_repair_bundle,'repair_deriver_sha256',v_deriver_sha,'source_sha256',v_archive.source_sha256,'batch_limit',500,'profile',v_profile,'elapsed_ms',round(extract(epoch FROM clock_timestamp()-v_started)*1000,3),'provider_accuracy_verified',false,'live_provider_requested',false,'all_prederived_records_preserved',true,'all_raw_value_formula_and_metadata_comparisons_passed',true));
END $test$;
SELECT result FROM d02_repair_test_result;
ROLLBACK;
