-- Current root controller only. Code/metadata registration and exactly one retained AAPL session.
-- Requires a truthful root-recorded D01_ACTUAL_RUNTIME_TEST_NUMERIC_V2 event containing the actual rollback receipt.
BEGIN;
SET LOCAL lock_timeout='15s';
SET LOCAL statement_timeout='30s';
DO $register$
DECLARE a population_top3000_202608_202609_v1.acquisition_control%rowtype;
 p population_top3000_202608_202609_v1.pipeline_control%rowtype;
 old_adapters jsonb;new_adapters jsonb;test_event bigint;receipt jsonb;review jsonb;snapshot jsonb;versions jsonb;task uuid;wake jsonb;
 adapter_file_hash constant text:='804b547d6b8706298c26cabce13c64884af56300ab14901a354abd007e1a0ec4';
 executed_harness_hash constant text:='9b7b537f4fa42093feae1eb4f63937be7fde0b56e148b79a8e2de00bb5c524b6';
 derive_hash text;
 validate_hash text;
BEGIN
 PERFORM pg_advisory_xact_lock(2026083000261022);
 SELECT * INTO a FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton FOR UPDATE;
 IF NOT FOUND THEN RAISE EXCEPTION 'D01_ACQUISITION_CONTROL_MISSING';END IF;
 SELECT * INTO p FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton FOR UPDATE;
 IF NOT FOUND THEN RAISE EXCEPTION 'D01_PIPELINE_CONTROL_MISSING';END IF;
 IF a.owner_id IS DISTINCT FROM 'top3000-completion-20261024-root-5db48b32d103'
 OR a.owner_fence IS DISTINCT FROM 3 OR a.stop_generation IS DISTINCT FROM 1 OR a.stopped IS DISTINCT FROM false
 OR p.expected_owner_id IS DISTINCT FROM a.owner_id OR p.expected_owner_fence IS DISTINCT FROM a.owner_fence
 OR p.expected_stop_generation IS DISTINCT FROM a.stop_generation
 OR a.enabled IS DISTINCT FROM true OR p.enabled IS DISTINCT FROM true
 OR a.hold_reason IS NOT NULL OR a.admitted_until IS NULL OR a.admitted_until<=clock_timestamp()
 OR p.shared_scheduler_verified IS DISTINCT FROM true OR p.deadline IS NULL OR p.deadline<=clock_timestamp()
 OR a.scope_sha256 IS DISTINCT FROM '8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd'
 OR p.scope_sha256 IS DISTINCT FROM a.scope_sha256
 OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_event e WHERE e.event_id::text=p.budget_evidence#>>'{monetary_policy,authority_event_id}' AND e.event='USER_AUTHORITY_RESUMED_20261024' AND e.details->>'owner_id'=a.owner_id AND e.details->>'owner_fence'=a.owner_fence::text AND e.details->>'stop_generation'=a.stop_generation::text AND e.details->>'scope_sha256'=a.scope_sha256) THEN RAISE EXCEPTION 'D01_REGISTRATION_CURRENT_CONTROLLER_REQUIRED';END IF;
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE family_id='D01' AND stage IN('DERIVE','VALIDATE') AND status='LEASED')
 THEN RAISE EXCEPTION 'D01_REGISTRATION_WILL_NOT_DISPLACE_ACTIVE_ATTEMPT';END IF;
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE family_id='D01' AND stage IN('DERIVE','VALIDATE') AND status='READY' AND payload->>'kind' IS DISTINCT FROM 'D01_RETAINED_AUGUST_SESSION_NUMERIC_V2')
 THEN RAISE EXCEPTION 'D01_EXISTING_READY_LEGACY_TASK_REQUIRES_SEPARATE_RECONCILIATION';END IF;
 SELECT event_id,details->'runtime_receipt' INTO test_event,receipt
 FROM population_top3000_202608_202609_v1.pipeline_event WHERE event='D01_ACTUAL_RUNTIME_TEST_NUMERIC_V2'
 AND details->>'owner_id'=a.owner_id AND details->>'owner_fence'=a.owner_fence::text AND details->>'stop_generation'=a.stop_generation::text
 AND details->>'calculator_file_sha256'='d3ee23ed46ea5d2dd106357c3d300712ad6a667f0889c152f5fbe5fb68a5e2c2'
 AND details->>'independent_review_sha256'='4499bec8ed2c9f10ce3d2aa819514aeb3bfcf077bbee9ef1ecbf53866cce0b36'
 AND details->>'adapter_file_sha256'=adapter_file_hash AND details->>'harness_sha256'=executed_harness_hash
 ORDER BY event_id DESC LIMIT 1;
 derive_hash:=receipt->>'derive_sha256';validate_hash:=receipt->>'validate_sha256';
 IF test_event IS NULL OR receipt->>'result' IS DISTINCT FROM 'PASS_ACTUAL_D01_NUMERIC_V2_390_ROLLBACK'
 OR receipt->>'numerical_rows_checked' IS DISTINCT FROM '390' OR receipt->>'replay_new_units' IS DISTINCT FROM '0'
 OR receipt->>'stale_token_rejected' IS DISTINCT FROM 'true' OR receipt->>'stale_fence_rejected' IS DISTINCT FROM 'true'
 OR receipt->>'later_stop_rejected' IS DISTINCT FROM 'true' OR receipt->>'input_tamper_rejected' IS DISTINCT FROM 'true'
 OR receipt->>'typed_replay_tamper_rejected' IS DISTINCT FROM 'true' OR receipt->>'scope_tamper_rejected' IS DISTINCT FROM 'true'
 OR receipt->>'successor_verified' IS DISTINCT FROM 'true' OR receipt->>'actual_provider_calls' IS DISTINCT FROM '0'
 OR receipt->>'production_changes_rolled_back' IS DISTINCT FROM 'true'
 OR derive_hash IS NULL OR validate_hash IS NULL
 OR receipt->>'precision_regression_cells_checked' IS DISTINCT FROM '3' OR receipt->>'original_v1_output_unchanged' IS DISTINCT FROM 'true'
 THEN RAISE EXCEPTION 'D01_ACTUAL_RUNTIME_TEST_RECEIPT_REQUIRED';END IF;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.d01_retained_session_value_v1 WHERE task_id='b09f6ce2-0054-43f1-9e98-185471703f60' AND output_sha256='aa844c83290d19fd05daf7545b470fcc0b1df1984de102f720af7f0fc5a0776a') THEN RAISE EXCEPTION 'D01_ORIGINAL_V1_OUTPUT_MUST_REMAIN';END IF;
 IF derive_hash IS DISTINCT FROM encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.d01_retained_derive_v2(uuid,text)'::regprocedure),'UTF8')),'hex')
 OR validate_hash IS DISTINCT FROM encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.d01_retained_validate_handoff_v2(uuid,text)'::regprocedure),'UTF8')),'hex')
 THEN RAISE EXCEPTION 'D01_REVIEWED_ADAPTER_CHANGED';END IF;
 PERFORM population_top3000_202608_202609_v1.d02_daily_resource_guard_v2();
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_adapter WHERE family_id='D01' AND stage='DERIVE' AND function_sha256 IN('7b610f5fab06c231248de5314aed1aaf241272f1265a06fa828faa92f4d48d21',derive_hash) AND function_sha256=encode(sha256(convert_to(pg_get_functiondef(function_oid),'UTF8')),'hex'))
 OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_adapter WHERE family_id='D01' AND stage='VALIDATE' AND function_sha256 IN('392fa106d6a600d11ec6c1223cb615c87bc3e50dd90b743a32b13799af711bfe',validate_hash) AND function_sha256=encode(sha256(convert_to(pg_get_functiondef(function_oid),'UTF8')),'hex')) THEN RAISE EXCEPTION 'D01_PRIOR_REVIEWED_ADAPTER_REQUIRED';END IF;
 snapshot:=population_top3000_202608_202609_v1.d01_retained_snapshot_v1(41,date '2026-08-03','corrected_company_scope_8e6ca1178f03822c');
 SELECT jsonb_build_object('scope_sha256',a.scope_sha256,'source_snapshot_sha256',encode(sha256(convert_to(snapshot::text,'UTF8')),'hex'),
 'calculator_sha256',encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.d01_feature_grid_json_v2(jsonb,jsonb,timestamptz,timestamptz,jsonb,integer)'::regprocedure),'UTF8')),'hex'),
 'snapshot_code_sha256',encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.d01_retained_snapshot_v1(integer,date,text)'::regprocedure),'UTF8')),'hex'),
 'dictionary_sha256',encode(sha256(convert_to(jsonb_agg(jsonb_build_array(item_key,payload_sha256,applicability) order by item_key)::text,'UTF8')),'hex')) INTO versions
 FROM population_top3000_202608_202609_v1.catalog_dictionary WHERE family_id='D01' AND scope_id='corrected_company_scope_8e6ca1178f03822c';
 IF versions IS DISTINCT FROM receipt->'input_versions' OR jsonb_array_length(snapshot->'source_rows') IS DISTINCT FROM 390 THEN RAISE EXCEPTION 'D01_FIRST_ADMISSION_MANIFEST_CHANGED';END IF;
 SELECT coalesce(jsonb_agg(to_jsonb(x) ORDER BY stage),'[]'::jsonb) INTO old_adapters FROM population_top3000_202608_202609_v1.pipeline_adapter x WHERE family_id='D01' AND stage IN('DERIVE','VALIDATE');
 review:=jsonb_build_object('adapter_file_sha256',adapter_file_hash,'calculator_file_sha256','d3ee23ed46ea5d2dd106357c3d300712ad6a667f0889c152f5fbe5fb68a5e2c2',
 'actual_runtime_event_id',test_event,'executed_harness_sha256',executed_harness_hash,
 'independent_review_sha256','4499bec8ed2c9f10ce3d2aa819514aeb3bfcf077bbee9ef1ecbf53866cce0b36',
 'admitted_sessions',1,'scheduled_grid_rows',390,'family_acceptance',false,'source_representation','RETAINED_TYPED_CANONICAL_NOT_ORIGINAL_WIRE');
 INSERT INTO population_top3000_202608_202609_v1.pipeline_adapter AS prior_adapter(family_id,stage,function_oid,function_sha256,resource_class,provider,enabled,reviewed_test_evidence,producer_revision,expected_batch_seconds)
 VALUES('D01','DERIVE','population_top3000_202608_202609_v1.d01_retained_derive_v2(uuid,text)'::regprocedure,derive_hash,'DATABASE',NULL,true,jsonb_build_object('d01_retained_numeric_v2',review),'d01-retained-20261024-v2',4.5),
 ('D01','VALIDATE','population_top3000_202608_202609_v1.d01_retained_validate_handoff_v2(uuid,text)'::regprocedure,validate_hash,'DATABASE',NULL,true,jsonb_build_object('d01_retained_numeric_v2',review),'d01-retained-20261024-v2',1)
 ON CONFLICT(family_id,stage) DO UPDATE SET function_oid=excluded.function_oid,function_sha256=excluded.function_sha256,resource_class=excluded.resource_class,provider=NULL,enabled=true,
 reviewed_test_evidence=coalesce(prior_adapter.reviewed_test_evidence,'{}'::jsonb)||excluded.reviewed_test_evidence,producer_revision=excluded.producer_revision,expected_batch_seconds=excluded.expected_batch_seconds;
 task:=population_top3000_202608_202609_v1.pipeline_enqueue_v1('D01','DERIVE','d01-retained-alpaca:41:2026-08-03:v2',
 jsonb_build_object('kind','D01_RETAINED_AUGUST_SESSION_NUMERIC_V2','instrument_key',41,'session_date','2026-08-03','calendar_scope_id','corrected_company_scope_8e6ca1178f03822c'),versions,NULL);
 PERFORM population_top3000_202608_202609_v1.d02_daily_resource_guard_v2();
 wake:=population_top3000_202608_202609_v1.pipeline_wake_v1(a.owner_id,a.owner_fence,'D01 reviewed retained AAPL Aug3 session registered; 390-row independent validation successor required');
 SELECT jsonb_agg(to_jsonb(x) ORDER BY stage) INTO new_adapters FROM population_top3000_202608_202609_v1.pipeline_adapter x WHERE family_id='D01' AND stage IN('DERIVE','VALIDATE');
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES('D01_RETAINED_FIRST_SESSION_REGISTERED_NUMERIC_V2',
 jsonb_build_object('owner_id',a.owner_id,'owner_fence',a.owner_fence,'stop_generation',a.stop_generation,'assignment','completion-20261024-prompt03','assignment_event_id',9532,
 'review',review,'prior_adapters',old_adapters,'registered_adapters',new_adapters,'task_id',task,'input_versions',versions,'wake_receipt',wake,'legacy_task_counters_preserved',true,'accepted_units',0,'family_acceptance',false));
END;
$register$;
COMMIT;
