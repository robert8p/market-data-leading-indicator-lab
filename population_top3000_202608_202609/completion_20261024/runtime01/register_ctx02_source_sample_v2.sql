-- Root/current-controller delegate only. Code and hashes only; no licensed source content.
-- Prerequisite root records pipeline_event event='CTX02_ACTUAL_RUNTIME_TEST_V2', details:
-- {owner_id,owner_fence:3,stop_generation:1,handler_file_sha256,harness_sha256,runtime_receipt:<rollback result>}.
-- This event records an actually observed rollback result, never an anticipated result.
BEGIN;
SET LOCAL lock_timeout='3s';
SET LOCAL statement_timeout='15s';
DO $register$
DECLARE a population_top3000_202608_202609_v1.acquisition_control%rowtype;
 c population_top3000_202608_202609_v1.pipeline_control%rowtype;
 q record;old_adapters jsonb;new_adapters jsonb;test_event bigint;test_receipt jsonb;review jsonb;n integer;held_task_ids uuid[];prior_tasks jsonb;wake_receipt jsonb;
 handler_file_hash constant text:='eb1394130f571975ae308e09a12c337e2ca3f9a91a2682b94adde589f3dd9a75';
 harness_hash constant text:='188b2a6f6099603f0531c776d9aeb452fe14422d86fbf384a74cc1d57f670b45';
 bundle_hash constant text:='13f338336169f76139d3b162ac3832ec4f5ac4243910ce0c31d5ffde0145baff';
BEGIN
 PERFORM pg_advisory_xact_lock(2026083000261022);
 SELECT * INTO a FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton FOR UPDATE;
 IF NOT FOUND THEN RAISE EXCEPTION 'CTX02_ACQUISITION_CONTROL_MISSING';END IF;
 SELECT * INTO c FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton FOR UPDATE;
 IF NOT FOUND THEN RAISE EXCEPTION 'CTX02_PIPELINE_CONTROL_MISSING';END IF;
 IF a.owner_id IS DISTINCT FROM 'top3000-completion-20261024-root-5db48b32d103'
 OR a.owner_fence IS DISTINCT FROM 3 OR a.stop_generation IS DISTINCT FROM 1 OR a.stopped IS DISTINCT FROM false
 OR c.expected_owner_id IS DISTINCT FROM a.owner_id OR c.expected_owner_fence IS DISTINCT FROM a.owner_fence
 OR c.expected_stop_generation IS DISTINCT FROM a.stop_generation
 OR a.enabled IS DISTINCT FROM true OR c.enabled IS DISTINCT FROM true
 OR a.hold_reason IS NOT NULL OR a.admitted_until IS NULL OR a.admitted_until<=clock_timestamp()
 OR c.shared_scheduler_verified IS DISTINCT FROM true OR (c.deadline IS NOT NULL AND c.deadline<=clock_timestamp())
 OR a.scope_sha256 IS DISTINCT FROM '8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd'
 OR c.scope_sha256 IS DISTINCT FROM a.scope_sha256 THEN RAISE EXCEPTION 'CTX02_REGISTRATION_CURRENT_CONTROLLER_REQUIRED';END IF;
 IF NOT population_top3000_202608_202609_v1.pipeline_cost_admission_v2(c,0) OR c.tick_count>=c.max_ticks THEN RAISE EXCEPTION 'CTX02_CURRENT_ADMISSION_EXHAUSTED';END IF;
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE family_id='CTX02' AND status='LEASED') THEN RAISE EXCEPTION 'CTX02_WILL_NOT_DISPLACE_ACTIVE_ATTEMPT';END IF;
 FOR q IN SELECT * FROM (VALUES
 ('population_top3000_202608_202609_v1.ctx02_derive_sample_v1(uuid,text)','a448f5ef3b52cd2b94a8ee698cac13ad7466a2989c6cdad8744de5cfe8493385'),
 ('population_top3000_202608_202609_v1.ctx02_import_connector_sample_v1(text,bigint,bigint,jsonb)','19a17b3056ecfc4eba4b2bc6c06fc4685837fdbb57afda3473474b19f829c54f'),
 ('population_top3000_202608_202609_v1.ctx02_normalize_sample_v1(uuid,text)','98261d5511f3c5d589c7bb7706efc8128184d1f3a60f1d7aee3be6f889144566'),
 ('population_top3000_202608_202609_v1.ctx02_number_v1(jsonb)','ac0403b3873fd4933ab29b4c6bcc33074024fb51341a4bf397c55f20b83c24f1'),
 ('population_top3000_202608_202609_v1.ctx02_parse_rows_v1(text,jsonb,jsonb)','30d0eca808a1eab2e5360e371d53ac2990a028370dc90d301cdc21cf0a89180d'),
 ('population_top3000_202608_202609_v1.ctx02_parser_bundle_v1()','366df2721b2bd028fe68dd24d04782f151ec6f14bdc2bf75757855f52d5cb919'),
 ('population_top3000_202608_202609_v1.ctx02_sample_quality_hold_v1(uuid,text)','89c15395689c832dafd35f01d7ac935968b73d7cb3b2fae89273635f28f601ac') ) v(fn,sha) LOOP
 IF q.sha IS DISTINCT FROM encode(sha256(convert_to(pg_get_functiondef(q.fn::regprocedure),'UTF8')),'hex') THEN RAISE EXCEPTION 'CTX02_REVIEWED_FUNCTION_CHANGED %',q.fn;END IF;
 END LOOP;
 IF population_top3000_202608_202609_v1.ctx02_parser_bundle_v1() IS DISTINCT FROM bundle_hash THEN RAISE EXCEPTION 'CTX02_REVIEWED_BUNDLE_CHANGED';END IF;
 SELECT event_id,details->'runtime_receipt' INTO test_event,test_receipt FROM population_top3000_202608_202609_v1.pipeline_event WHERE event='CTX02_ACTUAL_RUNTIME_TEST_V2'
 AND details->>'owner_id'=a.owner_id AND details->>'owner_fence'=a.owner_fence::text AND details->>'stop_generation'=a.stop_generation::text
 AND details->>'handler_file_sha256'=handler_file_hash AND details->>'harness_sha256'=harness_hash ORDER BY event_id DESC LIMIT 1;
 IF test_event IS NULL OR test_receipt->>'result' IS DISTINCT FROM 'PASS_ACTUAL_CTX02_SOURCE_ROLLBACK'
 OR test_receipt->>'sources' IS DISTINCT FROM '5' OR test_receipt->>'records' IS DISTINCT FROM '203' OR jsonb_array_length(test_receipt->'stage_proofs') IS DISTINCT FROM 15
 OR test_receipt->>'token_rejected' IS DISTINCT FROM 'true' OR test_receipt->>'fence_rejected' IS DISTINCT FROM 'true' OR test_receipt->>'stop_rejected' IS DISTINCT FROM 'true'
 OR test_receipt->>'records_mutation_rejected' IS DISTINCT FROM 'true' OR test_receipt->>'typed_column_mutation_rejected' IS DISTINCT FROM 'true'
 OR test_receipt->>'null_kind_rejected' IS DISTINCT FROM 'true' OR test_receipt->>'missing_selector_dates_rejected' IS DISTINCT FROM 'true'
 OR test_receipt->>'normalize_derive_replay_zero' IS DISTINCT FROM 'true' OR test_receipt->>'provider_calls' IS DISTINCT FROM '0'
 OR test_receipt->>'production_changes_rolled_back' IS DISTINCT FROM 'true' THEN RAISE EXCEPTION 'CTX02_ACTUAL_RUNTIME_RECEIPT_REQUIRED';END IF;
 SELECT count(DISTINCT s.source_envelope_sha256),array_agg(t.task_id) INTO n,held_task_ids FROM population_top3000_202608_202609_v1.pipeline_task t
 JOIN population_top3000_202608_202609_v1.ctx02_source_page_v1 s ON s.source_id=t.payload->>'source_id'
 JOIN population_top3000_202608_202609_v1.source_document d USING(source_id)
 WHERE t.family_id='CTX02' AND t.stage='NORMALIZE' AND t.partition_key='connector-sample:'||s.source_id
 AND t.status='BLOCKED' AND t.blocker_code='ENGINEERING_REGISTRATION_REQUIRED' AND t.blocker_until IS NULL AND t.executions=0
 AND t.input_versions->>'scope_sha256'=a.scope_sha256 AND t.input_versions->>'parser_bundle_sha256'=bundle_hash
 AND t.input_versions->>'projection_sha256'=s.projection_sha256 AND t.input_versions->>'source_sha256'=d.source_sha256
 AND encode(sha256(convert_to(d.content_text,'UTF8')),'hex')=d.source_sha256
 AND encode(sha256(convert_to(s.records::text,'UTF8')),'hex')=s.projection_sha256
 AND encode(sha256(convert_to((s.evidence||jsonb_build_object('records',s.records,'content_csv',d.content_text))::text,'UTF8')),'hex')=s.source_envelope_sha256
 AND s.source_envelope_sha256=ANY(ARRAY['cf94cbad52c1fa0792f0498b2ec30b0b3cddd6a192bec60409f0126cdce55869','6c16f38bb51b21ae0292fe5d9145a3a1e6a2fabb41433f1e2387c7b4efa5adc4','e7bdc4edf135b0e2af77da61c70a29928e08d2b1295902209192f354a8b4e114','394e880de2edd2b5c4b46d39365156fd5506937d89f768a11a628eeb1aacf2f8','bb76b4c938e44a0c4a71858fb2c0c53ff0ecf08b651f49a133e2fe9530dd5d0c']::text[]);
 IF n<>5 OR cardinality(held_task_ids)<>5 THEN RAISE EXCEPTION 'CTX02_EXACT_FIVE_HELD_SOURCES_REQUIRED';END IF;
 PERFORM 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=ANY(held_task_ids) FOR UPDATE;
 SELECT jsonb_agg(jsonb_build_object('task_id',task_id,'partition_key',partition_key,'status',status,'blocker_code',blocker_code,'executions',executions,'cumulative_failures',cumulative_failures,'recheck_count',recheck_count) ORDER BY partition_key) INTO prior_tasks FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=ANY(held_task_ids);
 SELECT coalesce(jsonb_agg(to_jsonb(x) ORDER BY stage),'[]'::jsonb) INTO old_adapters FROM population_top3000_202608_202609_v1.pipeline_adapter x WHERE family_id='CTX02' AND stage IN('NORMALIZE','DERIVE','VALIDATE');
 review:=jsonb_build_object('handler_file_sha256',handler_file_hash,'harness_sha256',harness_hash,'parser_bundle_sha256',bundle_hash,'actual_runtime_event_id',test_event,'independent_reviewer','acceptance_07','source_documents',5,'source_records',203,'sample_only',true,'family_acceptance',false,'source_representation','CONNECTED_TOOL_CSV_NOT_ORIGINAL_WIRE');
 FOR q IN SELECT * FROM (VALUES
 ('NORMALIZE','population_top3000_202608_202609_v1.ctx02_normalize_sample_v1(uuid,text)','98261d5511f3c5d589c7bb7706efc8128184d1f3a60f1d7aee3be6f889144566'),
 ('DERIVE','population_top3000_202608_202609_v1.ctx02_derive_sample_v1(uuid,text)','a448f5ef3b52cd2b94a8ee698cac13ad7466a2989c6cdad8744de5cfe8493385'),
 ('VALIDATE','population_top3000_202608_202609_v1.ctx02_sample_quality_hold_v1(uuid,text)','89c15395689c832dafd35f01d7ac935968b73d7cb3b2fae89273635f28f601ac')) v(stage,fn,sha) LOOP
 INSERT INTO population_top3000_202608_202609_v1.pipeline_adapter(family_id,stage,function_oid,function_sha256,resource_class,provider,enabled,reviewed_test_evidence,producer_revision,expected_batch_seconds)
 VALUES('CTX02',q.stage,q.fn::regprocedure,q.sha,'DATABASE',NULL,true,jsonb_build_object('ctx02_connector_sample_v2',review),'ctx02-connector-v2-'||left(handler_file_hash,16),20)
 ON CONFLICT(family_id,stage) DO UPDATE SET function_oid=excluded.function_oid,function_sha256=excluded.function_sha256,resource_class=excluded.resource_class,provider=excluded.provider,enabled=excluded.enabled,
 reviewed_test_evidence=coalesce(population_top3000_202608_202609_v1.pipeline_adapter.reviewed_test_evidence,'{}'::jsonb)||excluded.reviewed_test_evidence,producer_revision=excluded.producer_revision,expected_batch_seconds=excluded.expected_batch_seconds;
 END LOOP;
 UPDATE population_top3000_202608_202609_v1.pipeline_task SET status='READY',blocker_code=NULL,blocker_until=NULL,resumption_condition=NULL,not_before=clock_timestamp()
 WHERE task_id=ANY(held_task_ids) AND status='BLOCKED' AND blocker_code='ENGINEERING_REGISTRATION_REQUIRED' AND blocker_until IS NULL;
 GET DIAGNOSTICS n=ROW_COUNT;IF n<>5 THEN RAISE EXCEPTION 'CTX02_EXACT_REGISTRATION_RELEASE_FAILED';END IF;
 wake_receipt:=population_top3000_202608_202609_v1.pipeline_wake_v1(a.owner_id,a.owner_fence,'CTX02 reviewed connector samples registered; five engineering holds released; full census successor remains required');
 SELECT jsonb_agg(to_jsonb(x) ORDER BY stage) INTO new_adapters FROM population_top3000_202608_202609_v1.pipeline_adapter x WHERE family_id='CTX02' AND stage IN('NORMALIZE','DERIVE','VALIDATE');
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES('CTX02_CONNECTOR_SAMPLE_ADAPTERS_REGISTERED_V2',jsonb_build_object('owner_id',a.owner_id,'owner_fence',a.owner_fence,'stop_generation',a.stop_generation,'scope_sha256',a.scope_sha256,'review',review,'prior_adapters',old_adapters,'registered_adapters',new_adapters,'task_states_modified',true,'released_engineering_holds',5,'prior_tasks',prior_tasks,'task_counters_preserved',true,'wake_receipt',wake_receipt,'required_successor','FULL_DAILY_HISTORICAL_CONTRACT_CENSUS_AND_BULK_OR_REST_BARS'));
END $register$;
COMMIT;
