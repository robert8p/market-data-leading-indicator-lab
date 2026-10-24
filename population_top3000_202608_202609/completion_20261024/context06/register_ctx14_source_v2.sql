-- Root/current-controller delegate only. Code and hashes only; no licensed source content.
-- Prerequisite root records pipeline_event event='CTX14_ACTUAL_RUNTIME_TEST_V2', details:
-- {owner_id,owner_fence:3,stop_generation:1,handler_file_sha256,harness_sha256,runtime_receipt:<rollback result>}.
-- This event records an actually observed rollback result, never an anticipated result.
BEGIN;
SET LOCAL lock_timeout='3s';
SET LOCAL statement_timeout='15s';
DO $register$
DECLARE a population_top3000_202608_202609_v1.acquisition_control%rowtype;
 c population_top3000_202608_202609_v1.pipeline_control%rowtype;
 q record;old_adapters jsonb;new_adapters jsonb;test_event bigint;test_receipt jsonb;review jsonb;n integer;held_task_ids uuid[];prior_tasks jsonb;wake_receipt jsonb;
 helper_hash constant text:='ddf50e861608a42694fdefc17102a9b27517e21409b197bb4a2aa85c1dfc9c82';
 handler_file_hash constant text:='a5f3066166e1516abd041ba29fef639ab4e6d1777921efe187a397ec524b78e8';
 harness_hash constant text:='668028e2f33e7841fad220be659f778c299e5eb1f8fcb9b546e083b012e71d8c';
BEGIN
 PERFORM pg_advisory_xact_lock(2026083000261022);
 SELECT * INTO a FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton FOR UPDATE;
 IF NOT FOUND THEN RAISE EXCEPTION 'CTX14_ACQUISITION_CONTROL_MISSING';END IF;
 SELECT * INTO c FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton FOR UPDATE;
 IF NOT FOUND THEN RAISE EXCEPTION 'CTX14_PIPELINE_CONTROL_MISSING';END IF;
 IF a.owner_id IS DISTINCT FROM 'top3000-completion-20261024-root-5db48b32d103'
 OR a.owner_fence IS DISTINCT FROM 3 OR a.stop_generation IS DISTINCT FROM 1 OR a.stopped IS DISTINCT FROM false
 OR c.expected_owner_id IS DISTINCT FROM a.owner_id OR c.expected_owner_fence IS DISTINCT FROM a.owner_fence
 OR c.expected_stop_generation IS DISTINCT FROM a.stop_generation
 OR a.enabled IS DISTINCT FROM true OR c.enabled IS DISTINCT FROM true
 OR a.hold_reason IS NOT NULL OR a.admitted_until IS NULL OR a.admitted_until<=clock_timestamp()
 OR c.shared_scheduler_verified IS DISTINCT FROM true OR (c.deadline IS NOT NULL AND c.deadline<=clock_timestamp())
 OR a.scope_sha256 IS DISTINCT FROM '8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd'
 OR c.scope_sha256 IS DISTINCT FROM a.scope_sha256 THEN RAISE EXCEPTION 'CTX14_REGISTRATION_CURRENT_CONTROLLER_REQUIRED';END IF;
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE family_id='CTX14' AND stage IN('ACQUIRE','NORMALIZE','DERIVE','VALIDATE') AND status='LEASED')
 THEN RAISE EXCEPTION 'CTX14_REGISTRATION_WILL_NOT_DISPLACE_ACTIVE_ATTEMPT';END IF;
 IF helper_hash IS DISTINCT FROM encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.ctx14_connector_stage_v2(uuid,text)'::regprocedure),'UTF8')),'hex') THEN RAISE EXCEPTION 'CTX14_REVIEWED_HELPER_CHANGED';END IF;
 SELECT event_id,details->'runtime_receipt' INTO test_event,test_receipt
 FROM population_top3000_202608_202609_v1.pipeline_event WHERE event='CTX14_ACTUAL_RUNTIME_TEST_V2'
 AND details->>'owner_id'=a.owner_id AND details->>'owner_fence'=a.owner_fence::text AND details->>'stop_generation'=a.stop_generation::text
 AND details->>'handler_file_sha256'=handler_file_hash AND details->>'harness_sha256'=harness_hash
 ORDER BY event_id DESC LIMIT 1;
 IF test_event IS NULL OR test_receipt->>'result' IS DISTINCT FROM 'PASS_ACTUAL_CTX14_SOURCE_PROCESSING_ROLLBACK'
 OR test_receipt->>'sources' IS DISTINCT FROM '7' OR test_receipt->>'rows' IS DISTINCT FROM '2375'
 OR jsonb_array_length(test_receipt->'stage_proofs') IS DISTINCT FROM 28
 OR test_receipt->>'bad_token_rejected' IS DISTINCT FROM 'true' OR test_receipt->>'stale_fence_rejected' IS DISTINCT FROM 'true'
 OR test_receipt->>'latest_stop_rejected' IS DISTINCT FROM 'true' OR test_receipt->>'raw_source_mutation_rejected' IS DISTINCT FROM 'true'
 OR test_receipt->>'typed_kind_mutation_rejected' IS DISTINCT FROM 'true' OR test_receipt->>'provider_calls' IS DISTINCT FROM '0'
 OR test_receipt->>'production_changes_rolled_back' IS DISTINCT FROM 'true' THEN RAISE EXCEPTION 'CTX14_ACTUAL_RUNTIME_TEST_RECEIPT_REQUIRED';END IF;
 SELECT count(DISTINCT t.partition_key),array_agg(t.task_id) INTO n,held_task_ids FROM population_top3000_202608_202609_v1.pipeline_task t
 JOIN population_top3000_202608_202609_v1.source_document d ON d.source_id=t.partition_key
 WHERE t.family_id='CTX14' AND t.stage='ACQUIRE' AND t.payload->>'kind'='CTX14_CONNECTOR_SOURCE_V2'
 AND t.status='BLOCKED' AND t.blocker_code='ENGINEERING_REGISTRATION_REQUIRED' AND t.blocker_until IS NULL
 AND t.input_versions->>'connector_stage_sha256'=helper_hash AND t.input_versions->>'scope_sha256'=a.scope_sha256
 AND t.input_versions->>'source_sha256'=d.source_sha256
 AND d.source_id IN('ctx14-connector-5a4d20f42bf615fc406ef53c','ctx14-connector-e6146acb984bc980c3b8f008','ctx14-connector-4766ed3b301dddcc0d715f64','ctx14-connector-91a879b82fd47c7970799813','ctx14-connector-d2f1d9fb6dddc902c8707464','ctx14-connector-19d10f7d90ab9a49b939a0d1','ctx14-connector-c6c611b9183578f1afa2eedc');
 IF n<>7 OR cardinality(held_task_ids)<>7 THEN RAISE EXCEPTION 'CTX14_EXACT_SEVEN_REGISTRATION_HELD_SOURCES_REQUIRED';END IF;
 PERFORM 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=ANY(held_task_ids) FOR UPDATE;
 SELECT jsonb_agg(jsonb_build_object('task_id',task_id,'partition_key',partition_key,'status',status,'blocker_code',blocker_code,'executions',executions,'cumulative_failures',cumulative_failures,'recheck_count',recheck_count) ORDER BY partition_key) INTO prior_tasks FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=ANY(held_task_ids);
 SELECT jsonb_agg(to_jsonb(x) ORDER BY stage) INTO old_adapters FROM population_top3000_202608_202609_v1.pipeline_adapter x WHERE family_id='CTX14' AND stage IN('ACQUIRE','NORMALIZE','DERIVE','VALIDATE');
 IF jsonb_array_length(old_adapters) IS DISTINCT FROM 4 THEN RAISE EXCEPTION 'CTX14_EXISTING_FOUR_ADAPTERS_REQUIRED';END IF;
 review:=jsonb_build_object('handler_file_sha256',handler_file_hash,'helper_function_sha256',helper_hash,'harness_sha256',harness_hash,'actual_runtime_event_id',test_event,
 'independent_code_review_sha256','4e3c1a7afeea7c77fae712358e8609d5a426243010ab73e0ea029c286a76cb8f',
 'independent_projection_review_sha256','6460f44d98ddf69f528aa87ae254000ce773b549f670a70dab19b55865ede322',
 'independent_staging_review_sha256','c9c5399d153d33621f4ced54a19a824c35afd5c7ec93f66cf9e7dd019d5aaa18',
 'source_documents',7,'source_records',2375,'all_48_source_requirements_preserved',true,'family_acceptance',false,'source_representation','CONNECTED_TOOL_CSV_NOT_ORIGINAL_WIRE');
 FOR q IN SELECT * FROM (VALUES
 ('ACQUIRE','population_top3000_202608_202609_v1.ctx14_source_acquire_v2(uuid,text)','cd24482cf7a2e17ea40ecc0615abcbd735bcebae296842d5ba7432526b270d34'),
 ('NORMALIZE','population_top3000_202608_202609_v1.ctx14_source_normalize_v2(uuid,text)','1b643f1310983977e15fd5b4c42b89c02d3273dc86b9f25026aaf0bedfc2c3b5'),
 ('DERIVE','population_top3000_202608_202609_v1.ctx14_source_derive_v2(uuid,text)','c0ac6201936fae2018c3b8041871ac31c9961f521749ccf5c31d2e7ff421001e'),
 ('VALIDATE','population_top3000_202608_202609_v1.ctx14_source_validate_v2(uuid,text)','a8202734ab9ef97ea68a113123f4f2bbd4e314a927fe83056f54e25755369ee5')) v(stage,fn,sha) LOOP
  IF q.sha IS DISTINCT FROM encode(sha256(convert_to(pg_get_functiondef(q.fn::regprocedure),'UTF8')),'hex') THEN RAISE EXCEPTION 'CTX14_REVIEWED_WRAPPER_CHANGED %',q.stage;END IF;
  UPDATE population_top3000_202608_202609_v1.pipeline_adapter SET function_oid=q.fn::regprocedure,function_sha256=q.sha,resource_class='DATABASE',provider=NULL,enabled=true,
   reviewed_test_evidence=coalesce(reviewed_test_evidence,'{}'::jsonb)||jsonb_build_object('ctx14_connector_v2',review),producer_revision='ctx14-connector-v2-'||left(handler_file_hash,16),expected_batch_seconds=20
   WHERE family_id='CTX14' AND stage=q.stage;
 END LOOP;
 UPDATE population_top3000_202608_202609_v1.pipeline_task SET status='READY',blocker_code=NULL,blocker_until=NULL,resumption_condition=NULL,not_before=clock_timestamp()
 WHERE task_id=ANY(held_task_ids) AND status='BLOCKED' AND blocker_code='ENGINEERING_REGISTRATION_REQUIRED' AND blocker_until IS NULL;
 GET DIAGNOSTICS n=ROW_COUNT;
 IF n<>7 THEN RAISE EXCEPTION 'CTX14_EXACT_REGISTRATION_RELEASE_FAILED';END IF;
 wake_receipt:=population_top3000_202608_202609_v1.pipeline_wake_v1(a.owner_id,a.owner_fence,'CTX14 reviewed connector sources registered; seven engineering holds released with source requirements preserved');
 SELECT jsonb_agg(to_jsonb(x) ORDER BY stage) INTO new_adapters FROM population_top3000_202608_202609_v1.pipeline_adapter x WHERE family_id='CTX14' AND stage IN('ACQUIRE','NORMALIZE','DERIVE','VALIDATE');
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES('CTX14_CONNECTOR_ADAPTERS_REGISTERED_V2',jsonb_build_object('owner_id',a.owner_id,'owner_fence',a.owner_fence,'stop_generation',a.stop_generation,'scope_sha256',a.scope_sha256,'review',review,'prior_adapters',old_adapters,'registered_adapters',new_adapters,'task_states_modified',true,'released_engineering_holds',7,'prior_tasks',prior_tasks,'task_counters_preserved',true,'wake_receipt',wake_receipt,'legacy_routes_preserved',true));
END $register$;
COMMIT;
