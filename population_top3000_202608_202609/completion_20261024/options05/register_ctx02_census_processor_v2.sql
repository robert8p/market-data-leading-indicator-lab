-- Root-only additive repair after actual per-page regression and independent review.
-- Required event CTX02_CENSUS_V2_PROCESSOR_ROLLBACK_ACCEPTED includes current owner/fence/stop,
-- processor_file_sha256, harness_sha256, independent_review_receipt_sha256 and actual runtime_receipt.
BEGIN;
SET LOCAL lock_timeout='15s';SET LOCAL statement_timeout='25s';
DO $register$
DECLARE a population_top3000_202608_202609_v1.acquisition_control%rowtype;p population_top3000_202608_202609_v1.pipeline_control%rowtype;
 ev bigint;proof jsonb;review_sha text;x record;fn regprocedure;old_adapters jsonb;tasks jsonb;before_counts jsonb;after_counts jsonb;wake jsonb;
BEGIN
 PERFORM pg_advisory_xact_lock(2026083000261022);
 SELECT * INTO STRICT a FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton FOR UPDATE;
 SELECT * INTO STRICT p FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton FOR UPDATE;
 IF a.owner_id IS DISTINCT FROM 'top3000-completion-20261024-root-5db48b32d103' OR a.owner_fence IS DISTINCT FROM 3 OR a.stop_generation IS DISTINCT FROM 1
 OR a.stopped IS DISTINCT FROM false OR a.enabled IS DISTINCT FROM true OR p.enabled IS DISTINCT FROM true OR a.hold_reason IS NOT NULL
 OR a.admitted_until IS NULL OR a.admitted_until<=clock_timestamp() OR p.deadline IS NULL OR p.deadline<=clock_timestamp() OR p.shared_scheduler_verified IS DISTINCT FROM true
 OR p.expected_owner_id IS DISTINCT FROM a.owner_id OR p.expected_owner_fence IS DISTINCT FROM a.owner_fence OR p.expected_stop_generation IS DISTINCT FROM a.stop_generation
 OR a.scope_sha256 IS DISTINCT FROM '8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd' OR p.scope_sha256 IS DISTINCT FROM a.scope_sha256
 OR population_top3000_202608_202609_v1.ctx02_census_bundle_v1() IS DISTINCT FROM '0a5cebaf6c91f612c8a5ec0b5c02203823f7e33cc1449db4500fbd08ab540161'
 THEN RAISE EXCEPTION 'CTX02_PROCESSOR_V2_CURRENT_CONTROLLER_OR_ORIGINAL_SOURCE_BUNDLE_CHANGED';END IF;
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE family_id='CTX02' AND stage IN('NORMALIZE','DERIVE','VALIDATE') AND status='LEASED') THEN RAISE EXCEPTION 'CTX02_PROCESSOR_V2_ACTIVE_ATTEMPT_MUST_FINISH';END IF;
 SELECT event_id,details->'runtime_receipt',details->>'independent_review_receipt_sha256' INTO ev,proof,review_sha FROM population_top3000_202608_202609_v1.pipeline_event
 WHERE event='CTX02_CENSUS_V2_PROCESSOR_ROLLBACK_ACCEPTED' AND details->>'owner_id'=a.owner_id AND details->>'owner_fence'=a.owner_fence::text AND details->>'stop_generation'=a.stop_generation::text
 AND details->>'processor_file_sha256'='75a7c4d3d53aa5179933e4e97094cc313b5be4a0c9e1baeff6711063b25a10aa'
 AND details->>'harness_sha256'='159db48dc3a76cc374f053603e89e1965c71c24089f844081e4cf8e5c24d00bd' ORDER BY event_id DESC LIMIT 1;
 IF ev IS NULL OR review_sha IS NULL OR review_sha!~'^[0-9a-f]{64}$' OR proof->>'result' IS DISTINCT FROM 'PASS_ACTUAL_CTX02_NONTERMINAL_PAGE_PROCESSOR_V2_ROLLBACK'
 OR proof->>'provider_http_calls' IS DISTINCT FROM '0' OR proof->>'retained_source_rows' IS DISTINCT FROM '1000'
 OR proof->>'old_source_bundle_preserved' IS DISTINCT FROM 'true' OR proof->>'old_source_input_versions_preserved' IS DISTINCT FROM 'true'
 OR proof->>'provider_counters_unchanged' IS DISTINCT FROM 'true' OR proof->>'replay_new_rows' IS DISTINCT FROM '0' OR proof->>'parsed_tamper_rejected' IS DISTINCT FROM 'true'
 OR proof->>'production_changes_rolled_back' IS DISTINCT FROM 'true' OR jsonb_array_length(proof->'successor_stages') IS DISTINCT FROM 3
 OR proof->'function_hashes' IS DISTINCT FROM (SELECT jsonb_object_agg(pr.proname,encode(sha256(convert_to(pg_get_functiondef(pr.oid),'UTF8')),'hex')) FROM pg_proc pr JOIN pg_namespace n ON n.oid=pr.pronamespace WHERE n.nspname='population_top3000_202608_202609_v1' AND pr.proname IN('ctx02_census_process_v2','ctx02_census_normalize_v2','ctx02_census_derive_v2','ctx02_census_validate_v2'))
 THEN RAISE EXCEPTION 'CTX02_PROCESSOR_V2_ACTUAL_REVIEWED_REGRESSION_REQUIRED';END IF;
 PERFORM population_top3000_202608_202609_v1.d02_daily_resource_guard_v2();
 before_counts:=jsonb_build_object('provider_requests',a.provider_requests,'http_invocations',a.http_invocations,'stored_bytes',a.stored_bytes,'reserved_bytes',a.reserved_bytes,'accrued_usd',p.accrued_usd,'reserved_request_usd',p.reserved_request_usd);
 SELECT jsonb_agg(to_jsonb(z) ORDER BY z.stage) INTO old_adapters FROM population_top3000_202608_202609_v1.pipeline_adapter z WHERE family_id='CTX02' AND stage IN('NORMALIZE','DERIVE','VALIDATE');
 FOR x IN SELECT * FROM (VALUES ('NORMALIZE','ctx02_census_normalize_v2','987defcd6f06ff3f35335b6ee56c33f4755b9bc9f9dc137b0f31f59d3d8edade'),('DERIVE','ctx02_census_derive_v2','b692d70492053bbeb54121691df0818f5d3acf91c3505b882fa1dae513e18876'),('VALIDATE','ctx02_census_validate_v2','8fa074513501d1240e87e9afd76a74d51b5cf1532b43bafbf2e87308bd7e5073')) z(stage,fn,old_sha) LOOP
  fn:=('population_top3000_202608_202609_v1.'||x.fn||'(uuid,text)')::regprocedure;
  IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_adapter z WHERE z.family_id='CTX02' AND z.stage=x.stage AND z.function_sha256 IN(x.old_sha,proof->'function_hashes'->>x.fn) AND z.function_sha256=encode(sha256(convert_to(pg_get_functiondef(z.function_oid),'UTF8')),'hex')) THEN RAISE EXCEPTION 'CTX02_PROCESSOR_V2_PRIOR_ADAPTER_CHANGED';END IF;
  UPDATE population_top3000_202608_202609_v1.pipeline_adapter SET function_oid=fn,function_sha256=proof->'function_hashes'->>x.fn,producer_revision='ctx02-census-processor-v2-75a7c4d3',reviewed_test_evidence=coalesce(reviewed_test_evidence,'{}'::jsonb)||jsonb_build_object('processor_v2',jsonb_build_object('actual_regression_event_id',ev,'independent_review_receipt_sha256',review_sha,'v1_source_bundle_preserved',true)) WHERE family_id='CTX02' AND stage=x.stage;
 END LOOP;
 WITH changed AS (
  UPDATE population_top3000_202608_202609_v1.pipeline_task SET status='READY',blocker_code=NULL,blocker_until=NULL,resumption_condition=NULL,not_before=clock_timestamp()
  WHERE family_id='CTX02' AND stage='NORMALIZE' AND status='BLOCKED' AND blocker_code='PARSER'
  AND payload->>'kind'='CTX02_CENSUS_SOURCE_V1' AND payload->>'source_page_number' IS NOT NULL
  AND resumption_condition#>>'{details,message}'='column reference "q.archive_key" is ambiguous'
  RETURNING task_id,input_versions,checkpoint,executions,max_executions
 ) SELECT jsonb_agg(to_jsonb(changed)) INTO tasks FROM changed;
 SELECT jsonb_build_object('provider_requests',ac.provider_requests,'http_invocations',ac.http_invocations,'stored_bytes',ac.stored_bytes,'reserved_bytes',ac.reserved_bytes,'accrued_usd',pc.accrued_usd,'reserved_request_usd',pc.reserved_request_usd) INTO after_counts FROM population_top3000_202608_202609_v1.acquisition_control ac CROSS JOIN population_top3000_202608_202609_v1.pipeline_control pc WHERE ac.singleton AND pc.singleton;
 IF after_counts IS DISTINCT FROM before_counts THEN RAISE EXCEPTION 'CTX02_PROCESSOR_V2_COUNTERS_CHANGED';END IF;
 PERFORM population_top3000_202608_202609_v1.d02_daily_resource_guard_v2();
 wake:=population_top3000_202608_202609_v1.pipeline_wake_v1(a.owner_id,a.owner_fence,'Reviewed additive options census processor resolves per-page alias; source archive, cursor and failed-attempt history preserved');
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details)VALUES('CTX02_CENSUS_PROCESSOR_V2_REGISTERED',jsonb_build_object('owner_id',a.owner_id,'owner_fence',a.owner_fence,'stop_generation',a.stop_generation,'actual_regression_event_id',ev,'independent_review_receipt_sha256',review_sha,'prior_adapters',old_adapters,'requeued_exact_alias_failures',tasks,'source_bundle_input_versions_archive_cursor_unchanged',true,'cumulative_counters_preserved',true,'counters',after_counts,'wake_receipt',wake));
END;$register$;
COMMIT;
