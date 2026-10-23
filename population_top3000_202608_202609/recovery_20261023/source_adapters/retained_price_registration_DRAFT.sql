-- Root-only bounded inventory admission after both actual-source rollback tests pass.
-- 5 retained securities x21 August sessions x3 session labels =315 manifests and <=630 stage tasks.
-- These securities are fixtures, not five companies or a selection of the final company universe.
-- No new source fetch, raw-bar copy, new family denominator, certification, budget or deadline change.
BEGIN;
SET LOCAL statement_timeout='20s';
SET LOCAL lock_timeout='500ms';
SET LOCAL TimeZone='UTC';
SELECT pg_advisory_xact_lock(2026083000261022);
DO $admit$
DECLARE v_hash text;v_test_p01 jsonb;v_test_p02 jsonb;v_cal_scope text;v_loader_sha text;v_source_oid text;
 v_map jsonb;v_map_sha text;v_inputs jsonb;v_payload jsonb;v_partition text;v_family text;v_session text;
 v_security record;v_calendar population_top3000_202608_202609_v1.catalog_calendar%rowtype;v_stage text;v_task uuid;
 v_tasks jsonb:='[]';v_new_count integer:=0;v_provider bigint;v_http bigint;v_stored bigint;
 v_batch constant text:='RETAINED_ALPACA_AUGUST_FIVE_SECURITY_INVENTORY_20261023_V1';
BEGIN
 PERFORM 1 FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton FOR UPDATE;
 PERFORM 1 FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton FOR UPDATE;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.acquisition_control a
  JOIN population_top3000_202608_202609_v1.pipeline_control p ON p.singleton=a.singleton
  WHERE a.singleton AND a.owner_id='top3000-recovery-20261023-root-4b1314bcb5c2' AND a.owner_fence=2 AND a.stop_generation=0
   AND a.owner_id=p.expected_owner_id AND a.owner_fence=p.expected_owner_fence AND a.stop_generation=p.expected_stop_generation
   AND a.scope_sha256=p.scope_sha256 AND a.scope_sha256='8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd'
   AND NOT a.stopped AND a.enabled AND a.hold_reason IS NULL AND a.admitted_until>clock_timestamp()
   AND p.shared_scheduler_verified AND p.budget_verified_at IS NOT NULL AND p.deadline>clock_timestamp()
   AND p.actual_usd+p.accrued_usd+p.unavoidable_usd+p.remaining_uncommitted_usd+p.repair_egress_reserve_usd+p.reserved_request_usd<=p.budget_cap_usd
   AND p.max_ticks-p.tick_count>=630)
 THEN RAISE EXCEPTION 'RETAINED_PRICE_OWNER_BUDGET_TIME_OR_TICK_ADMISSION_REQUIRED';END IF;
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE payload->>'inventory_batch'=v_batch)
  OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_event WHERE event='RETAINED_PRICE_FIVE_SECURITY_INVENTORY_ADMITTED' AND details->>'inventory_batch'=v_batch)
 THEN RAISE EXCEPTION 'RETAINED_PRICE_BATCH_ALREADY_ADMITTED_RECONCILE_PRESERVED_RECEIPTS';END IF;
 IF position('case when q.family_id in(''P01'',''P02'') and q.payload->>''kind''=''RETAINED_ALPACA_AUGUST_SESSION'' then 20 else 0 end'
  in pg_get_functiondef('population_top3000_202608_202609_v1.pipeline_tick_v1()'::regprocedure))=0
 THEN RAISE EXCEPTION 'RETAINED_PRICE_LOWER_PRIORITY_GUARD_REQUIRED';END IF;
 v_hash:=encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.price_retained_partition_v1(uuid,text)'::regprocedure),'UTF8')),'hex');
 SELECT details INTO v_test_p01 FROM population_top3000_202608_202609_v1.pipeline_event
  WHERE event='RETAINED_PRICE_P01_ROLLBACK_VERIFIED' AND details->>'result'='PASS' AND details->>'owner_fence'='2'
   AND details->>'stop_generation'='0' AND details->>'tested_function_sha256'=v_hash ORDER BY at DESC LIMIT 1;
 SELECT details INTO v_test_p02 FROM population_top3000_202608_202609_v1.pipeline_event
  WHERE event='RETAINED_PRICE_P02_ROLLBACK_VERIFIED' AND details->>'result'='PASS' AND details->>'owner_fence'='2'
   AND details->>'stop_generation'='0' AND details->>'tested_function_sha256'=v_hash ORDER BY at DESC LIMIT 1;
 IF v_test_p01 IS NULL OR v_test_p02 IS NULL THEN RAISE EXCEPTION 'BOTH_ACTUAL_PRICE_TEST_RECEIPTS_FOR_CURRENT_CODE_REQUIRED';END IF;
 SELECT min(scope_id) INTO v_cal_scope FROM population_top3000_202608_202609_v1.catalog_calendar
  WHERE session_date BETWEEN '2026-08-01' AND '2026-08-31';
 IF (SELECT count(DISTINCT scope_id) FROM population_top3000_202608_202609_v1.catalog_calendar WHERE session_date BETWEEN '2026-08-01' AND '2026-08-31')<>1
  OR (SELECT count(*) FROM population_top3000_202608_202609_v1.catalog_calendar WHERE scope_id=v_cal_scope AND session_date BETWEEN '2026-08-01' AND '2026-08-31')<>21
  OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.catalog_calendar WHERE scope_id=v_cal_scope AND session_date BETWEEN '2026-08-01' AND '2026-08-31'
   AND (source_record_sha256 IS DISTINCT FROM encode(sha256(convert_to(source_record_text,'UTF8')),'hex')
    OR source_receipt_sha256 IS DISTINCT FROM encode(sha256(convert_to(source_receipt_text,'UTF8')),'hex')
    OR qualification IS DISTINCT FROM 'PROVIDER_AND_NYSE_RULES_RECONCILED'))
 THEN RAISE EXCEPTION 'EXACT_21_SESSION_CALENDAR_WITH_MATCHING_RAW_HASHES_REQUIRED';END IF;
 v_loader_sha:=encode(sha256(convert_to(pg_get_functiondef('consolidation_audit.load_next_alpaca_partition()'::regprocedure),'UTF8')),'hex');
 v_source_oid:='market_fixed_20250901_20260831.alpaca_sip_bars_1m_202608'::regclass::oid::text;
 SELECT provider_requests,http_invocations,stored_bytes INTO v_provider,v_http,v_stored FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton;
 FOREACH v_family IN ARRAY ARRAY['P01','P02'] LOOP
  FOREACH v_stage IN ARRAY ARRAY['ACQUIRE','NORMALIZE'] LOOP
   INSERT INTO population_top3000_202608_202609_v1.pipeline_adapter
    (family_id,stage,function_oid,function_sha256,resource_class,provider,enabled,reviewed_test_evidence,producer_revision,expected_batch_seconds)
   VALUES(v_family,v_stage,'population_top3000_202608_202609_v1.price_retained_partition_v1(uuid,text)'::regprocedure::oid,
    v_hash,'DATABASE',NULL,true,CASE v_family WHEN 'P01' THEN v_test_p01 ELSE v_test_p02 END,
    'retained-price-partition-v1',5) ON CONFLICT(family_id,stage) DO NOTHING;
   IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_adapter WHERE family_id=v_family AND stage=v_stage
    AND function_oid='population_top3000_202608_202609_v1.price_retained_partition_v1(uuid,text)'::regprocedure::oid
    AND function_sha256=v_hash AND resource_class='DATABASE' AND provider IS NULL AND enabled)
   THEN RAISE EXCEPTION 'EXISTING_PRICE_ADAPTER_CONFLICT family=% stage=%',v_family,v_stage;END IF;
  END LOOP;
 END LOOP;
 FOR v_security IN SELECT * FROM (VALUES (41,'AAPL'),(5083,'GOOG'),(5084,'GOOGL'),(8372,'NVDA'),(12595,'WEAV')) x(instrument_key,symbol) LOOP
  SELECT coalesce(jsonb_agg(to_jsonb(map_rows) ORDER BY to_jsonb(map_rows)::text),'[]'::jsonb) INTO v_map
  FROM (SELECT m.instrument_key,m.symbol,m.alpaca_asset_id,m.name,m.exchange,m.asset_class,m.massive_type,m.classification_method,
   m.classification_observed_at,m.provenance FROM reference.alpaca_fixed_window_instruments_remediated_v1 m
   WHERE m.instrument_key=v_security.instrument_key ORDER BY m.symbol,m.alpaca_asset_id LIMIT 2) map_rows;
  IF jsonb_array_length(v_map)<>1 OR v_map#>>'{0,symbol}' IS DISTINCT FROM v_security.symbol THEN RAISE EXCEPTION 'RETAINED_PRICE_FIXTURE_MAPPING_CHANGED key=%',v_security.instrument_key;END IF;
  v_map:=jsonb_build_object('instrument_reference',v_map,'alias_rows','[]'::jsonb,'alias_run_id',NULL,
   'reference_candidate_limit_reached',false,'alias_candidate_limit_reached',false,
   'alias_date_boundary_policy','Broad inclusive candidate lookup; effective-boundary semantics require identity review.','historical_company_identity_certified',false);
  v_map_sha:=encode(sha256(convert_to(v_map::text,'UTF8')),'hex');
  FOR v_calendar IN SELECT * FROM population_top3000_202608_202609_v1.catalog_calendar
   WHERE scope_id=v_cal_scope AND session_date BETWEEN '2026-08-01' AND '2026-08-31' ORDER BY session_date LOOP
   v_inputs:=jsonb_build_object('scope_sha256','8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd',
    'calendar_record_sha256',v_calendar.source_record_sha256,'calendar_receipt_sha256',v_calendar.source_receipt_sha256,
    'loader_definition_sha256',v_loader_sha,'mapping_snapshot_sha256',v_map_sha,'source_relation_oid',v_source_oid,
    'hash_codec','PG_FLOAT8_BINARY_ROW_SHA256_V1','source_snapshot_version',v_batch);
   FOREACH v_session IN ARRAY ARRAY['regular','premarket','postmarket'] LOOP
    v_family:=CASE v_session WHEN 'regular' THEN 'P01' ELSE 'P02' END;
    v_partition:='retained-alpaca-august:'||v_security.instrument_key||':'||v_calendar.session_date||':'||v_session;
    v_payload:=jsonb_build_object('kind','RETAINED_ALPACA_AUGUST_SESSION','instrument_key',v_security.instrument_key,
     'fixture_symbol',v_security.symbol,'session_date',v_calendar.session_date,'session_label',v_session,
     'calendar_scope_id',v_cal_scope,'inventory_batch',v_batch,'scope_qualification','RETAINED_SECURITY_FIXTURE_NOT_COMPANY_UNIVERSE');
    v_task:=population_top3000_202608_202609_v1.pipeline_enqueue_v1(v_family,'ACQUIRE',v_partition,v_payload,v_inputs);
    IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=v_task AND status='READY'
     AND executions=0 AND payload=v_payload AND input_versions=v_inputs)
    THEN RAISE EXCEPTION 'PRICE_TASK_REPLAY_OR_INPUT_CONFLICT partition=%',v_partition;END IF;
    UPDATE population_top3000_202608_202609_v1.pipeline_task SET max_executions=2 WHERE task_id=v_task;
    v_tasks:=v_tasks||jsonb_build_array(v_task);v_new_count:=v_new_count+1;
   END LOOP;
  END LOOP;
 END LOOP;
 IF v_new_count<>315 OR (SELECT provider_requests FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton) IS DISTINCT FROM v_provider
  OR (SELECT http_invocations FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton) IS DISTINCT FROM v_http
  OR (SELECT stored_bytes FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton) IS DISTINCT FROM v_stored
 THEN RAISE EXCEPTION 'RETAINED_PRICE_ADMISSION_COUNT_OR_RESOURCE_COUNTER_CHANGED';END IF;
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details)
 VALUES('RETAINED_PRICE_FIVE_SECURITY_INVENTORY_ADMITTED',jsonb_build_object('inventory_batch',v_batch,'owner_fence',2,'stop_generation',0,
  'securities',5,'calendar_sessions',21,'session_labels',3,'acquire_partitions',315,'maximum_stage_tasks',630,
  'task_ids',v_tasks,'adapter_sha256',v_hash,'loader_sha256',v_loader_sha,'source_relation_oid',v_source_oid,
  'inventory_only',true,'company_universe_selected',false,'raw_bars_copied',0,'network_calls',0,'source_priority_over_inventory',true));
 PERFORM population_top3000_202608_202609_v1.pipeline_wake_v1('top3000-recovery-20261023-root-4b1314bcb5c2',2,'ACTUAL_TESTED_RETAINED_PRICE_INVENTORY_ADMITTED');
END $admit$;
COMMIT;
