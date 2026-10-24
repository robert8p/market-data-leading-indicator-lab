-- Run in controller transaction after reviewed DDL and private evidence stage. No provider call.
DO $reg$
DECLARE a population_top3000_202608_202609_v1.acquisition_control%rowtype;p population_top3000_202608_202609_v1.pipeline_control%rowtype;
 proof jsonb;review jsonb;x record;fn regprocedure;h text;old_t jsonb;new_t jsonb;review_id bigint;test_id bigint;tid uuid;live_hashes jsonb;adapter_count integer;
BEGIN
 PERFORM pg_advisory_xact_lock(2026083000261022);
 SELECT * INTO STRICT a FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton FOR UPDATE;
 SELECT * INTO STRICT p FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton FOR UPDATE;
 IF a.owner_id IS DISTINCT FROM 'top3000-completion-20261024-root-5db48b32d103' OR a.owner_fence IS DISTINCT FROM 3 OR a.stop_generation IS DISTINCT FROM 1 OR a.enabled IS DISTINCT FROM true OR a.stopped IS DISTINCT FROM false OR a.hold_reason IS NOT NULL OR p.enabled IS DISTINCT FROM true
 OR p.expected_owner_id IS DISTINCT FROM a.owner_id OR p.expected_owner_fence IS DISTINCT FROM a.owner_fence OR p.expected_stop_generation IS DISTINCT FROM a.stop_generation
 OR a.scope_sha256 IS DISTINCT FROM '8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd' OR p.scope_sha256 IS DISTINCT FROM a.scope_sha256
 OR a.admitted_until IS NULL OR a.admitted_until<=clock_timestamp() OR p.deadline IS NULL OR p.deadline<=clock_timestamp()
 OR p.budget_evidence#>>'{monetary_policy,authority_event_id}' IS DISTINCT FROM '9528' OR population_top3000_202608_202609_v1.pipeline_cost_admission_v2(p,0) IS DISTINCT FROM true
 THEN RAISE EXCEPTION 'CTX02_V6_REGISTRATION_AUTHORITY_REQUIRED';END IF;
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE family_id='CTX02' AND stage IN('NORMALIZE','DERIVE','VALIDATE') AND status='LEASED') THEN RAISE EXCEPTION 'CTX02_V6_AFFECTED_ATTEMPT_ACTIVE';END IF;
 SELECT event_id,details INTO test_id,proof FROM population_top3000_202608_202609_v1.pipeline_event WHERE event='CTX02_V6_LIFECYCLE_ROLLBACK_ACCEPTED' ORDER BY event_id DESC LIMIT 1;
 SELECT event_id,details INTO review_id,review FROM population_top3000_202608_202609_v1.pipeline_event WHERE event='CTX02_V6_LIFECYCLE_INDEPENDENT_REVIEW' ORDER BY event_id DESC LIMIT 1;
 SELECT jsonb_object_agg(pr.proname,encode(sha256(convert_to(pg_get_functiondef(pr.oid),'UTF8')),'hex')) INTO live_hashes FROM pg_proc pr JOIN pg_namespace n ON n.oid=pr.pronamespace WHERE n.nspname='population_top3000_202608_202609_v1' AND pr.proname IN('ctx02_census_process_v6','ctx02_census_classify_record_v6','ctx02_lifecycle_bundle_v6','ctx02_census_normalize_v6','ctx02_census_derive_v6','ctx02_census_validate_v6');
 IF proof->>'result' IS DISTINCT FROM 'PASS_ACTUAL_CTX02_NONTERMINAL_PAGE_PROCESSOR_V6_ROLLBACK' OR proof->>'lifecycle_supported_records' IS DISTINCT FROM '20'
 OR proof->>'production_changes_rolled_back' IS DISTINCT FROM 'true' OR proof->>'parsed_tamper_rejected' IS DISTINCT FROM 'true' OR proof->>'provider_counters_unchanged' IS DISTINCT FROM 'true'
 OR proof->>'bundle_sha256' IS DISTINCT FROM population_top3000_202608_202609_v1.ctx02_lifecycle_bundle_v6()
 OR review->>'approved' IS DISTINCT FROM 'true' OR review->>'reviewer' IS NULL OR review->>'producer' IS NULL OR review->>'reviewer'=review->>'producer'
 OR review->>'runtime_event_id' IS DISTINCT FROM test_id::text
 OR review->>'bundle_sha256' IS DISTINCT FROM proof->>'bundle_sha256' OR review->'function_hashes' IS DISTINCT FROM proof->'function_hashes' OR proof->'function_hashes' IS DISTINCT FROM live_hashes
 OR coalesce(review->>'review_sha256','')!~'^[0-9a-f]{64}$' OR proof->>'quarantine_storage_path_verified' IS DISTINCT FROM 'true'
 OR population_top3000_202608_202609_v1.ctx02_census_bundle_v1() IS DISTINCT FROM '0a5cebaf6c91f612c8a5ec0b5c02203823f7e33cc1449db4500fbd08ab540161'
 THEN RAISE EXCEPTION 'CTX02_V6_INDEPENDENT_RUNTIME_PROOF_REQUIRED';END IF;
 SELECT count(*) INTO adapter_count FROM population_top3000_202608_202609_v1.pipeline_adapter WHERE family_id='CTX02' AND stage IN('NORMALIZE','DERIVE','VALIDATE');
 IF adapter_count<>3 THEN RAISE EXCEPTION 'CTX02_V6_EXACT_THREE_ADAPTERS_REQUIRED';END IF;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.ctx02_lifecycle_evidence_v6 WHERE evidence_id='OCC59450_AVNS_20260727' AND source_sha256='9bf2b2b4edc3e00c9014dbf373cc3d3f52a39918a0cfeec9f3d069dbc46363eb' AND source_sha256=encode(sha256(convert_to(source_body,'UTF8')),'hex') AND review_sha256=review->>'review_sha256') THEN RAISE EXCEPTION 'CTX02_V6_EXACT_SOURCE_REVIEW_REQUIRED';END IF;
 FOR x IN SELECT stage,function_oid,function_sha256 FROM population_top3000_202608_202609_v1.pipeline_adapter WHERE family_id='CTX02' AND stage IN('NORMALIZE','DERIVE','VALIDATE') FOR UPDATE LOOP
  IF x.function_oid IS DISTINCT FROM ('population_top3000_202608_202609_v1.ctx02_census_'||lower(x.stage)||'_v2(uuid,text)')::regprocedure
  OR x.function_sha256 IS DISTINCT FROM CASE x.stage WHEN 'NORMALIZE' THEN 'cf5ce3279c6439fc333b79e5e8c464655a8da9d5742da92bb4b7576991ee3799' WHEN 'DERIVE' THEN '0c0a9934fabce89113334dcbd8b84befc0a434334e05d477a084d15a431082b5' WHEN 'VALIDATE' THEN 'b817f4e33f6f9b0b5ffcc92616ba38ca00eecbaf17ecc7bb3a1d7555230cbbcd' END
  OR x.function_sha256 IS DISTINCT FROM encode(sha256(convert_to(pg_get_functiondef(x.function_oid),'UTF8')),'hex') THEN RAISE EXCEPTION 'CTX02_V6_PRIOR_ADAPTER_CHANGED';END IF;
  fn:=('population_top3000_202608_202609_v1.ctx02_census_'||lower(x.stage)||'_v6(uuid,text)')::regprocedure;
  h:=encode(sha256(convert_to(pg_get_functiondef(fn),'UTF8')),'hex');
  IF proof#>>ARRAY['function_hashes','ctx02_census_'||lower(x.stage)||'_v6'] IS DISTINCT FROM h THEN RAISE EXCEPTION 'CTX02_V6_REVIEWED_WRAPPER_CHANGED';END IF;
  UPDATE population_top3000_202608_202609_v1.pipeline_adapter SET function_oid=fn,function_sha256=h,producer_revision='ctx02-lifecycle-quarantine-v6',reviewed_test_evidence=reviewed_test_evidence||jsonb_build_object('ctx02_v6',jsonb_build_object('bundle_sha256',proof->>'bundle_sha256','actual_runtime_event_id',test_id,'independent_review_event_id',review_id,'full_family_accepted',false)) WHERE family_id='CTX02' AND stage=x.stage;
 END LOOP;
 tid:='82403ec2-bfd5-4a48-9b83-9deea3e0876c';
 SELECT to_jsonb(t) INTO old_t FROM population_top3000_202608_202609_v1.pipeline_task t WHERE task_id=tid FOR UPDATE;
 IF old_t->>'status' IS DISTINCT FROM 'BLOCKED' OR old_t->>'blocker_code' IS DISTINCT FROM 'PARSER' OR old_t#>>'{resumption_condition,details,message}' IS DISTINCT FROM 'CTX02_CONTRACT_SYMBOL_FIELDS_CONFLICT' OR old_t->>'lease_until' IS NOT NULL THEN RAISE EXCEPTION 'CTX02_V6_EXACT_PAGE165_HOLD_REQUIRED';END IF;
 UPDATE population_top3000_202608_202609_v1.pipeline_task SET status='READY',blocker_code=NULL,blocker_until=NULL,resumption_condition=NULL,not_before=clock_timestamp() WHERE task_id=tid;
 SELECT to_jsonb(t) INTO new_t FROM population_top3000_202608_202609_v1.pipeline_task t WHERE task_id=tid;
 IF (old_t-ARRAY['status','blocker_code','blocker_until','resumption_condition','not_before']) IS DISTINCT FROM (new_t-ARRAY['status','blocker_code','blocker_until','resumption_condition','not_before']) THEN RAISE EXCEPTION 'CTX02_V6_TASK_HISTORY_CHANGED';END IF;
 PERFORM population_top3000_202608_202609_v1.d02_daily_resource_guard_v2();
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES('CTX02_LIFECYCLE_QUARANTINE_REGISTERED_V6',jsonb_build_object('owner_id',a.owner_id,'owner_fence',a.owner_fence,'stop_generation',a.stop_generation,'runtime_event_id',test_id,'review_event_id',review_id,'requeued_task_id',tid,'original_source_bundle_preserved',true,'family_accepted',false,'historical_receipt_qualified',false));
 PERFORM population_top3000_202608_202609_v1.pipeline_wake_v1(a.owner_id,a.owner_fence,'Reviewed lifecycle/quarantine processor; exact page165 parsing hold requeued');
END;$reg$;
