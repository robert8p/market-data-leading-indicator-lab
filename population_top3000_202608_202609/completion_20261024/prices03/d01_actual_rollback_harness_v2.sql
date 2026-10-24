-- Root executes only after installing reviewed D01 files. Every test mutation rolls back.
BEGIN;
SET LOCAL statement_timeout='45s';
SET LOCAL lock_timeout='15s';
CREATE TEMP TABLE d01_runtime_proof(result jsonb) ON COMMIT DROP;
DO $test$
DECLARE ctl population_top3000_202608_202609_v1.acquisition_control%rowtype;
 tid uuid;a uuid:=gen_random_uuid();tok text:='prices03-d01-rollback-only';h text;vh text;snapshot jsonb;versions jsonb;payload jsonb;r1 jsonb;r2 jsonb;
 token_bad boolean:=false;fence_bad boolean:=false;stop_bad boolean:=false;input_bad boolean:=false;typed_bad boolean:=false;scope_bad boolean:=false;
 old_output jsonb;precision_checked integer; n integer;checked integer;child uuid;v population_top3000_202608_202609_v1.d01_retained_session_value_v1%rowtype;
BEGIN
 PERFORM pg_advisory_xact_lock(2026083000261022);
 SELECT * INTO ctl FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton FOR UPDATE;
 IF NOT FOUND OR ctl.owner_id IS DISTINCT FROM 'top3000-completion-20261024-root-5db48b32d103' OR ctl.owner_fence IS DISTINCT FROM 3 OR ctl.stop_generation IS DISTINCT FROM 1 OR ctl.stopped IS DISTINCT FROM false THEN RAISE EXCEPTION 'CONTROLLER_CHANGED';END IF;
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE family_id='D01' AND stage IN('DERIVE','VALIDATE') AND status='LEASED') THEN RAISE EXCEPTION 'D01_ROLLBACK_WILL_NOT_DISPLACE_ACTIVE_ATTEMPT';END IF;
 SELECT to_jsonb(orig) INTO old_output FROM population_top3000_202608_202609_v1.d01_retained_session_value_v1 orig WHERE orig.task_id='b09f6ce2-0054-43f1-9e98-185471703f60';
 IF old_output IS NULL OR old_output->>'output_sha256' IS DISTINCT FROM 'aa844c83290d19fd05daf7545b470fcc0b1df1984de102f720af7f0fc5a0776a' THEN RAISE EXCEPTION 'D01_ORIGINAL_V1_OUTPUT_REQUIRED';END IF;
 snapshot:=population_top3000_202608_202609_v1.d01_retained_snapshot_v1(41,date '2026-08-03','corrected_company_scope_8e6ca1178f03822c');
 IF jsonb_array_length(snapshot->'source_rows')<>390 THEN RAISE EXCEPTION 'D01_AAPL390_SOURCE_REQUIRED';END IF;
 SELECT jsonb_build_object('scope_sha256','8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd','source_snapshot_sha256',encode(sha256(convert_to(snapshot::text,'UTF8')),'hex'),
 'calculator_sha256',encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.d01_feature_grid_json_v2(jsonb,jsonb,timestamptz,timestamptz,jsonb,integer)'::regprocedure),'UTF8')),'hex'),
 'snapshot_code_sha256',encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.d01_retained_snapshot_v1(integer,date,text)'::regprocedure),'UTF8')),'hex'),
 'dictionary_sha256',encode(sha256(convert_to(jsonb_agg(jsonb_build_array(item_key,payload_sha256,applicability) order by item_key)::text,'UTF8')),'hex')) INTO versions
 FROM population_top3000_202608_202609_v1.catalog_dictionary WHERE family_id='D01' AND scope_id='corrected_company_scope_8e6ca1178f03822c';
 IF versions->>'source_snapshot_sha256' IS DISTINCT FROM 'eee92d0083f8b933bae01d835de061f150a8311a2d4665c4eb82022e0f97a20a' OR versions->>'snapshot_code_sha256' IS DISTINCT FROM 'fd289ea1c1a1b6b24eead49048499052d3aa16e86e88954caf8574fcb011f065' OR versions->>'dictionary_sha256' IS DISTINCT FROM '87be6557b3c9a9d28521143c90a5c0abc2ad4fdeecb121f7ac70550b5c5492ec' THEN RAISE EXCEPTION 'D01_REVIEWED_FIRST_MANIFEST_CHANGED';END IF;
 payload:=jsonb_build_object('kind','D01_RETAINED_AUGUST_SESSION_NUMERIC_V2','instrument_key',41,'session_date','2026-08-03','calendar_scope_id','corrected_company_scope_8e6ca1178f03822c');
 h:=encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.d01_retained_derive_v2(uuid,text)'::regprocedure),'UTF8')),'hex');
 vh:=encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.d01_retained_validate_handoff_v2(uuid,text)'::regprocedure),'UTF8')),'hex');
 INSERT INTO population_top3000_202608_202609_v1.pipeline_adapter(family_id,stage,function_oid,function_sha256,resource_class,enabled,reviewed_test_evidence,producer_revision,expected_batch_seconds)
 VALUES('D01','DERIVE','population_top3000_202608_202609_v1.d01_retained_derive_v2(uuid,text)'::regprocedure,h,'DATABASE',true,'{"test":"ROLLBACK_ONLY"}', 'd01-retained-20261024-v2',4.5),
 ('D01','VALIDATE','population_top3000_202608_202609_v1.d01_retained_validate_handoff_v2(uuid,text)'::regprocedure,vh,'DATABASE',true,'{"test":"ROLLBACK_ONLY"}','d01-retained-20261024-v2',1)
 ON CONFLICT(family_id,stage) DO UPDATE SET function_oid=excluded.function_oid,function_sha256=excluded.function_sha256,resource_class=excluded.resource_class,enabled=true,reviewed_test_evidence=excluded.reviewed_test_evidence,producer_revision=excluded.producer_revision,expected_batch_seconds=excluded.expected_batch_seconds;
 tid:=population_top3000_202608_202609_v1.pipeline_enqueue_v1('D01','DERIVE','d01-rollback-only:'||a,payload,versions,null);
 UPDATE population_top3000_202608_202609_v1.pipeline_task SET status='LEASED',attempt_id=a,lease_until=clock_timestamp()+interval '90 seconds',owner_fence=ctl.owner_fence,stop_generation=ctl.stop_generation,executions=1 WHERE task_id=tid;
 INSERT INTO population_top3000_202608_202609_v1.pipeline_attempt(attempt_id,task_id,execution_number,token_sha256,owner_fence,stop_generation,adapter_sha256)
 VALUES(a,tid,1,encode(sha256(convert_to(tok,'UTF8')),'hex'),ctl.owner_fence,ctl.stop_generation,h);
 BEGIN PERFORM population_top3000_202608_202609_v1.d01_retained_derive_v2(a,'bad-token');EXCEPTION WHEN raise_exception THEN IF SQLERRM='ATTEMPT_AUTH_FAILED' THEN token_bad:=true;ELSE RAISE;END IF;END;
 BEGIN UPDATE population_top3000_202608_202609_v1.pipeline_attempt SET owner_fence=2 WHERE attempt_id=a;PERFORM population_top3000_202608_202609_v1.d01_retained_derive_v2(a,tok);EXCEPTION WHEN raise_exception THEN IF SQLERRM='STALE_OWNER_OR_STOP' THEN fence_bad:=true;ELSE RAISE;END IF;END;
 BEGIN UPDATE population_top3000_202608_202609_v1.acquisition_control SET stopped=true WHERE singleton;PERFORM population_top3000_202608_202609_v1.d01_retained_derive_v2(a,tok);EXCEPTION WHEN raise_exception THEN IF SQLERRM='MISSION_HELD_OR_ADMISSION_EXPIRED' THEN stop_bad:=true;ELSE RAISE;END IF;END;
 BEGIN UPDATE population_top3000_202608_202609_v1.pipeline_task SET input_versions=jsonb_set(input_versions,'{source_snapshot_sha256}',to_jsonb(repeat('f',64))) WHERE task_id=tid;PERFORM population_top3000_202608_202609_v1.d01_retained_derive_v2(a,tok);EXCEPTION WHEN raise_exception THEN IF SQLERRM='D01_IMMUTABLE_INPUT_VERSION_CHANGED' THEN input_bad:=true;ELSE RAISE;END IF;END;
 BEGIN UPDATE population_top3000_202608_202609_v1.pipeline_task pt SET payload=jsonb_set(pt.payload,'{calendar_scope_id}','"wrong-scope"') WHERE pt.task_id=tid;PERFORM population_top3000_202608_202609_v1.d01_retained_derive_v2(a,tok);EXCEPTION WHEN raise_exception THEN IF SQLERRM='D01_ADMISSION_SCOPE_MISMATCH' THEN scope_bad:=true;ELSE RAISE;END IF;END;
 r1:=population_top3000_202608_202609_v1.d01_retained_derive_v2(a,tok);
 r2:=population_top3000_202608_202609_v1.d01_retained_derive_v2(a,tok);
 IF r1->>'state'<>'COMPLETE' OR (r1#>>'{commit_receipt,new_committed_units}')::integer<>390 OR (r2#>>'{commit_receipt,new_committed_units}')::integer<>0 OR r1#>>'{commit_receipt,output_sha256}' IS DISTINCT FROM r2#>>'{commit_receipt,output_sha256}' THEN RAISE EXCEPTION 'D01_COMMIT_OR_REPLAY_FAILED';END IF;
 SELECT * INTO v FROM population_top3000_202608_202609_v1.d01_retained_session_value_v1 WHERE task_id=tid;
 IF v.source_rows<>390 OR v.scheduled_grid_rows<>390 OR jsonb_array_length(v.feature_rows)<>390 OR jsonb_array_length(v.column_manifest)<>58 OR v.publication_eligible THEN RAISE EXCEPTION 'D01_OUTPUT_SHAPE_FAILED';END IF;
 WITH actual AS (
  SELECT jsonb_object_agg(k.key,r.rowvalue->(k.ord::integer-1)) j
  FROM jsonb_array_elements(v.feature_rows) WITH ORDINALITY r(rowvalue,roword)
  CROSS JOIN jsonb_array_elements_text(v.column_manifest) WITH ORDINALITY k(key,ord) GROUP BY r.roword
 ), raw AS (
  SELECT (r->>'bar_ts')::timestamptz ts,(r->>'close')::numeric c,(r->>'volume')::numeric volume,
   lag((r->>'close')::numeric,1) over(order by (r->>'bar_ts')::timestamptz) c1,
   lag((r->>'close')::numeric,120) over(order by (r->>'bar_ts')::timestamptz) c120,
   avg((r->>'volume')::numeric) over(order by (r->>'bar_ts')::timestamptz rows between 19 preceding and current row) vm20,
   avg((r->>'close')::numeric*(r->>'volume')::numeric) over(order by (r->>'bar_ts')::timestamptz rows between 19 preceding and current row) dv20
  FROM jsonb_array_elements(snapshot->'source_rows')r
 ) SELECT count(*) INTO checked FROM actual a JOIN raw b ON (a.j->>'provider_interval_start')::timestamptz=b.ts
 WHERE (a.j->>'close')::numeric=b.c AND (a.j->>'volume')::numeric=b.volume
 AND (a.j->>'provider_interval_end')::timestamptz=b.ts+interval '1 minute'
 AND (a.j->>'availability_assumed_ts')::timestamptz=b.ts+interval '61 seconds'
 AND (CASE WHEN b.c1 IS NULL THEN a.j->'return_1m'='null'::jsonb ELSE abs((a.j->>'return_1m')::numeric-(b.c/b.c1-1))<1e-12 END)
 AND (CASE WHEN b.c120 IS NULL THEN a.j->'return_120m'='null'::jsonb ELSE abs((a.j->>'return_120m')::numeric-(b.c/b.c120-1))<1e-12 END)
 AND (CASE WHEN (a.j->>'minute_index')::integer<19 THEN a.j->'volume_mean_20m'='null'::jsonb AND a.j->'dollar_volume_mean_20m'='null'::jsonb ELSE abs((a.j->>'volume_mean_20m')::numeric-b.vm20)<1e-8 AND abs((a.j->>'dollar_volume_mean_20m')::numeric-b.dv20)<0.01 END)
 AND a.j->'microstructure_available'='false'::jsonb AND a.j#>>'{quality_flags,record_hash_role}'='RETAINED_TYPED_CANONICAL_ROW_NOT_ORIGINAL_WIRE';
 IF checked<>390 THEN RAISE EXCEPTION 'D01_NUMERICAL390_FAILED %',checked;END IF;
 WITH actual AS (
 SELECT jsonb_object_agg(k.key,r.rowvalue->(k.ord::integer-1)) j
 FROM jsonb_array_elements(v.feature_rows) WITH ORDINALITY r(rowvalue,roword)
 CROSS JOIN jsonb_array_elements_text(v.column_manifest) WITH ORDINALITY k(key,ord) GROUP BY r.roword
 ), expected(minute_index,field,value) AS (VALUES
 (204,'close_vs_sma20_bps',-0.00619790517981444059061599968345014807937194::numeric),
 (366,'close_vs_provider_vwap_bps',-0.00868723531899437940792164074403842983761771::numeric),
 (367,'close_vs_provider_vwap_bps',0.00852333988778170703746604514472204962458950::numeric))
 SELECT count(*) INTO precision_checked FROM actual a JOIN expected e ON (a.j->>'minute_index')::integer=e.minute_index
 WHERE abs((a.j->>e.field)::numeric-e.value)<=greatest(1e-12::numeric,abs(e.value)*1e-10::numeric);
 IF precision_checked<>3 THEN RAISE EXCEPTION 'D01_ORIGINAL_THREE_PRECISION_REGRESSIONS %',precision_checked;END IF;
 IF old_output IS DISTINCT FROM (SELECT to_jsonb(x) FROM population_top3000_202608_202609_v1.d01_retained_session_value_v1 x WHERE task_id='b09f6ce2-0054-43f1-9e98-185471703f60') THEN RAISE EXCEPTION 'D01_ORIGINAL_OUTPUT_CHANGED';END IF;
 BEGIN UPDATE population_top3000_202608_202609_v1.d01_retained_session_value_v1 SET source_rows=389 WHERE task_id=tid;PERFORM population_top3000_202608_202609_v1.d01_retained_derive_v2(a,tok);EXCEPTION WHEN raise_exception THEN IF SQLERRM='D01_REPLAY_CONFLICT' THEN typed_bad:=true;ELSE RAISE;END IF;END;
 child:=(r1#>>'{commit_receipt,successor_task_id}')::uuid;
 SELECT count(*) INTO n FROM population_top3000_202608_202609_v1.pipeline_dependency dep JOIN population_top3000_202608_202609_v1.pipeline_task t ON t.task_id=dep.task_id WHERE dep.prerequisite_task_id=tid AND t.task_id=child AND t.family_id='D01' AND t.stage='VALIDATE' AND t.input_versions=versions;
 IF n<>1 OR NOT(token_bad AND fence_bad AND stop_bad AND input_bad AND typed_bad AND scope_bad) THEN RAISE EXCEPTION 'D01_GUARD_OR_SUCCESSOR_FAILED';END IF;
 INSERT INTO d01_runtime_proof VALUES(jsonb_build_object('result','PASS_ACTUAL_D01_NUMERIC_V2_390_ROLLBACK','admission_payload',payload,'input_versions',versions,'derive_sha256',h,'validate_sha256',vh,'first_commit',r1,'replay_new_units',r2#>'{commit_receipt,new_committed_units}','numerical_rows_checked',checked,'precision_regression_cells_checked',precision_checked,'original_v1_output_unchanged',true,'stale_token_rejected',token_bad,'stale_fence_rejected',fence_bad,'later_stop_rejected',stop_bad,'input_tamper_rejected',input_bad,'typed_replay_tamper_rejected',typed_bad,'scope_tamper_rejected',scope_bad,'successor_verified',true,'actual_provider_calls',0,'production_changes_rolled_back',true));
END;
$test$;
SELECT result FROM d01_runtime_proof;
ROLLBACK;
