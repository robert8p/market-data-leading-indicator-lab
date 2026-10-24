-- Root-only admission: five source candidates, Sep9 only, at most two pages each.
-- No company/rank/expected-print acceptance. Shared limiter and cumulative counters are preserved.
BEGIN;
SET LOCAL lock_timeout='15s'; SET LOCAL statement_timeout='30s';
DO $register$
DECLARE a population_top3000_202608_202609_v1.acquisition_control%rowtype;
 p population_top3000_202608_202609_v1.pipeline_control%rowtype;
 proof jsonb;deployment jsonb;proof_id bigint;deployment_id bigint;bundle text;prior_adapters jsonb;registered jsonb;review jsonb;
 mapping jsonb;versions jsonb;r record;st text;fn text;old_hash text;new_hash text;task uuid;tasks jsonb:='[]';wake jsonb;n bigint;cap bigint;
 sql_sha constant text:='a7fcc3c638c1ab34815a9759b7332b2fdec248cea4b707b52e25eb7ff0ab4263';
 view_sha constant text:='906e7e566f6b51c55f11972168813f2ebe2552084bd9a3e50a9911c3e7c63c5a';
 harness_sha constant text:='62a4a689c7abd29b2f79c862d0eac91e2a5654ffbe63f8364aa481654db89fd6';
 worker_sha constant text:='7d15af259fc0698847a685e7c69160edae3a7ba0816868b5048e6669c04deb9b';
BEGIN
 PERFORM pg_advisory_xact_lock(2026083000261022);
 SELECT * INTO a FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton FOR UPDATE;
 IF NOT FOUND THEN RAISE EXCEPTION 'MINUTE_V2_ACQUISITION_CONTROL_MISSING';END IF;
 SELECT * INTO p FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton FOR UPDATE;
 IF NOT FOUND THEN RAISE EXCEPTION 'MINUTE_V2_PIPELINE_CONTROL_MISSING';END IF;
 IF a.owner_id IS DISTINCT FROM 'top3000-completion-20261024-root-5db48b32d103' OR a.owner_fence IS DISTINCT FROM 3 OR a.stop_generation IS DISTINCT FROM 1 OR a.stopped IS DISTINCT FROM false
 OR p.expected_owner_id IS DISTINCT FROM a.owner_id OR p.expected_owner_fence IS DISTINCT FROM a.owner_fence OR p.expected_stop_generation IS DISTINCT FROM a.stop_generation
 OR a.enabled IS DISTINCT FROM true OR p.enabled IS DISTINCT FROM true OR a.hold_reason IS NOT NULL
 OR a.admitted_until IS NULL OR a.admitted_until<=clock_timestamp() OR p.deadline IS NULL OR p.deadline<=clock_timestamp() OR p.shared_scheduler_verified IS DISTINCT FROM true
 OR a.scope_sha256 IS DISTINCT FROM '8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd' OR p.scope_sha256 IS DISTINCT FROM a.scope_sha256
 OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_event e WHERE e.event_id::text=p.budget_evidence#>>'{monetary_policy,authority_event_id}' AND e.event='USER_AUTHORITY_RESUMED_20261024' AND e.details->>'owner_id'=a.owner_id AND e.details->>'owner_fence'=a.owner_fence::text AND e.details->>'stop_generation'=a.stop_generation::text AND e.details->>'scope_sha256'=a.scope_sha256)
 THEN RAISE EXCEPTION 'MINUTE_V2_CURRENT_CONTROLLER_REQUIRED';END IF;
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE family_id='P01' AND status='LEASED') THEN RAISE EXCEPTION 'MINUTE_V2_ACTIVE_P01_MUST_FINISH';END IF;
 SELECT event_id,details INTO proof_id,proof FROM population_top3000_202608_202609_v1.pipeline_event
 WHERE event='MASSIVE_MINUTE_V2_ACTUAL_RUNTIME_TEST' AND details->>'owner_id'=a.owner_id AND details->>'owner_fence'=a.owner_fence::text AND details->>'stop_generation'=a.stop_generation::text
 AND details->>'sql_sha256'=sql_sha AND details->>'view_sha256'=view_sha AND details->>'harness_sha256'=harness_sha
 ORDER BY event_id DESC LIMIT 1;
 IF proof_id IS NULL OR proof->>'independent_review_sha256' IS DISTINCT FROM 'c00d46c0208d62fcf59abfd61ddb595bce38502574e9c9c321170aa5fa12ec0c'
 OR proof#>>'{runtime_receipt,result}' IS DISTINCT FROM 'PASS_MASSIVE_MINUTE_V2_ACTUAL_ROLLBACK'
 OR proof#>>'{runtime_receipt,fixture_source_rows}' IS DISTINCT FROM '888' OR proof#>>'{runtime_receipt,candidate_view_rows}' IS DISTINCT FROM '888'
 OR proof#>>'{runtime_receipt,wrong_token_rejected}' IS DISTINCT FROM 'true' OR proof#>>'{runtime_receipt,cross_symbol_rejected}' IS DISTINCT FROM 'true'
 OR proof#>>'{runtime_receipt,cross_symbol_url_rejected}' IS DISTINCT FROM 'true' OR proof#>>'{runtime_receipt,input_tamper_rejected}' IS DISTINCT FROM 'true'
 OR proof#>>'{runtime_receipt,instrument_key_mismatch_rejected}' IS DISTINCT FROM 'true' OR proof#>>'{runtime_receipt,retained_source_mutation_rejected}' IS DISTINCT FROM 'true'
 OR proof#>>'{runtime_receipt,numeric_lexeme_exact}' IS DISTINCT FROM 'true' OR proof#>>'{runtime_receipt,typed_normalize_replay_new_units}' IS DISTINCT FROM '0'
 OR proof#>>'{runtime_receipt,original_v1_archive_unchanged}' IS DISTINCT FROM 'true' OR proof#>>'{runtime_receipt,actual_provider_requests}' IS DISTINCT FROM '0'
 OR proof#>>'{runtime_receipt,synthetic_fixture_only}' IS DISTINCT FROM 'true' OR proof#>>'{runtime_receipt,all_changes_rolled_back}' IS DISTINCT FROM 'true'
 THEN RAISE EXCEPTION 'MINUTE_V2_ACTUAL_ROLLBACK_PROOF_REQUIRED';END IF;
 bundle:=population_top3000_202608_202609_v1.massive_minute_bundle_sha256_v2();
 IF bundle IS DISTINCT FROM proof#>>'{runtime_receipt,bundle_sha256}' THEN RAISE EXCEPTION 'MINUTE_V2_TESTED_CODE_CHANGED';END IF;
 IF proof->>'view_definition_sha256' IS DISTINCT FROM encode(sha256(convert_to(pg_get_viewdef('population_top3000_202608_202609_v1.massive_minute_session_candidate_v2'::regclass,true),'UTF8')),'hex') THEN RAISE EXCEPTION 'MINUTE_V2_TESTED_VIEW_CHANGED';END IF;
 SELECT event_id,details INTO deployment_id,deployment FROM population_top3000_202608_202609_v1.pipeline_event
 WHERE event='MASSIVE_MINUTE_V2_DEPLOYMENT_VERIFIED' AND details->>'owner_id'=a.owner_id AND details->>'owner_fence'=a.owner_fence::text AND details->>'stop_generation'=a.stop_generation::text
 AND details->>'function_slug'='population-top3000-minute-v2' AND details->>'worker_sha256'=worker_sha ORDER BY event_id DESC LIMIT 1;
 IF deployment_id IS NULL OR deployment->>'status' IS DISTINCT FROM 'ACTIVE' OR deployment->>'deployed_source_matches' IS DISTINCT FROM 'true'
 OR deployment->>'invalid_attempt_rejected' IS DISTINCT FROM 'true' OR deployment->>'dispatch_allowlist_verified' IS DISTINCT FROM 'true'
 OR coalesce(deployment->>'deployment_id','')='' OR position('population-top3000-minute-v2' in pg_get_functiondef('population_top3000_202608_202609_v1.pipeline_dispatch_edge_v1(uuid,text,text)'::regprocedure))=0
 THEN RAISE EXCEPTION 'MINUTE_V2_DEPLOYMENT_AND_DISPATCH_REQUIRED';END IF;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_event e WHERE e.event='MASSIVE_MINUTE_V2_DISPATCH_ADMITTED' AND e.details->>'owner_id'=a.owner_id AND e.details->>'owner_fence'=a.owner_fence::text AND e.details->>'stop_generation'=a.stop_generation::text AND e.details->>'scope_sha256'=a.scope_sha256 AND e.details->>'added_slug'='population-top3000-minute-v2' AND e.details->>'new_function_sha256'=encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.pipeline_dispatch_edge_v1(uuid,text,text)'::regprocedure),'UTF8')),'hex')) THEN RAISE EXCEPTION 'MINUTE_V2_PINNED_CURRENT_DISPATCH_REQUIRED';END IF;
 SELECT jsonb_agg(to_jsonb(m) ORDER BY instrument_key) INTO mapping FROM
 (SELECT instrument_key,symbol,alpaca_asset_id,classification_method,classification_observed_at FROM reference.alpaca_fixed_window_instruments_remediated_v1 WHERE instrument_key IN(41,5083,5084,8372,12595))m;
 IF jsonb_array_length(mapping) IS DISTINCT FROM 5 OR EXISTS(SELECT 1 FROM (VALUES(41,'AAPL'),(5083,'GOOG'),(5084,'GOOGL'),(8372,'NVDA'),(12595,'WEAV')) known(k,s) WHERE NOT EXISTS(SELECT 1 FROM jsonb_array_elements(mapping)m WHERE m->>'instrument_key'=known.k::text AND m->>'symbol'=known.s)) THEN RAISE EXCEPTION 'MINUTE_V2_RETAINED_CANDIDATE_MAP_CHANGED';END IF;
 n:=population_top3000_202608_202609_v1.massive_minute_resource_guard_v2();cap:=(p.budget_evidence#>>'{resource_limits,max_relation_bytes}')::bigint;
 IF cap-n<134217728 THEN RAISE EXCEPTION 'MINUTE_V2_INITIAL_128MIB_HEADROOM_REQUIRED';END IF;
 SELECT jsonb_agg(to_jsonb(x) ORDER BY stage) INTO prior_adapters FROM population_top3000_202608_202609_v1.pipeline_adapter x WHERE family_id='P01';
 review:=jsonb_build_object('assignment','completion-20261024-prompt03','assignment_event_id',9532,'sql_sha256',sql_sha,'view_sha256',view_sha,'worker_sha256',worker_sha,'actual_runtime_event_id',proof_id,'deployment_event_id',deployment_id,'independent_review_sha256',proof->>'independent_review_sha256','source_only_candidate',true,'family_acceptance',false,'shared_provider_limiter',true);
 FOREACH st IN ARRAY ARRAY['ACQUIRE','NORMALIZE','DERIVE','VALIDATE'] LOOP
  fn:='population_top3000_202608_202609_v1.massive_minute_route_'||lower(st)||'_v2(uuid,text)';
  new_hash:=encode(sha256(convert_to(pg_get_functiondef(fn::regprocedure),'UTF8')),'hex');
  old_hash:=CASE st WHEN 'ACQUIRE' THEN 'e2d3708bd407b88f1e0f18a4af2462cb06ec84e931fb2ee2c64e855ea5122817' WHEN 'NORMALIZE' THEN '21640001e4d4fa739e6f8b491be8ef9870e865b7a8826bf8dbb3da8dadaf7514' WHEN 'DERIVE' THEN '74daa35500cbea1b321ccae38242ba62e92b5b89acb6eeeb09b1d16c47bc8622' ELSE '47eb2d3e20d18118f71774f3efa95ff6ac916b68a40ae244b61f0890fe1aec82' END;
  IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_adapter x WHERE x.family_id='P01' AND x.stage=st AND x.function_sha256 IN(old_hash,new_hash) AND x.function_sha256=encode(sha256(convert_to(pg_get_functiondef(x.function_oid),'UTF8')),'hex')) THEN RAISE EXCEPTION 'MINUTE_V2_PRIOR_REVIEWED_ROUTE_REQUIRED';END IF;
  UPDATE population_top3000_202608_202609_v1.pipeline_adapter x SET function_oid=fn::regprocedure,function_sha256=new_hash,resource_class=CASE WHEN st='ACQUIRE' THEN 'PROVIDER' ELSE 'DATABASE' END,provider=NULL,enabled=true,reviewed_test_evidence=coalesce(x.reviewed_test_evidence,'{}'::jsonb)||jsonb_build_object('massive_minute_v2',review),producer_revision='massive-minute-known5-20260909-v2',expected_batch_seconds=CASE WHEN st='ACQUIRE' THEN 45 ELSE 5 END WHERE x.family_id='P01' AND x.stage=st;
 END LOOP;
 versions:=jsonb_build_object('scope_sha256',a.scope_sha256,'source_contract_revision','massive-minute-known5-20260909-v2','massive_minute_bundle_sha256',bundle,'retained_candidate_mapping_sha256',encode(sha256(convert_to(mapping::text,'UTF8')),'hex'));
 FOR r IN SELECT * FROM (VALUES('AAPL',41),('GOOG',5083),('GOOGL',5084),('NVDA',8372),('WEAV',12595))known(symbol,instrument_key) LOOP
  task:=population_top3000_202608_202609_v1.pipeline_enqueue_v1('P01','ACQUIRE','massive-minute-'||r.symbol||'-20260909-v2',jsonb_build_object('kind','MASSIVE_MINUTE_KNOWN5_V2','source_symbol',r.symbol,'instrument_key',r.instrument_key,'trading_date','2026-09-09','max_pages',2,'provider_accuracy_verified',false,'company_population_verified',false),versions,NULL);
  tasks:=tasks||jsonb_build_array(jsonb_build_object('task_id',task,'source_symbol',r.symbol,'candidate_instrument_key',r.instrument_key));
 END LOOP;
 PERFORM population_top3000_202608_202609_v1.massive_minute_resource_guard_v2();
 wake:=population_top3000_202608_202609_v1.pipeline_wake_v1(a.owner_id,a.owner_fence,'Reviewed native Massive five-symbol Sep9 source acquisition; P01/P02 sessions remain candidate only');
 SELECT jsonb_agg(to_jsonb(x) ORDER BY stage) INTO registered FROM population_top3000_202608_202609_v1.pipeline_adapter x WHERE family_id='P01';
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES('MASSIVE_MINUTE_KNOWN5_SOURCE_ADMITTED_V2',jsonb_build_object('owner_id',a.owner_id,'owner_fence',a.owner_fence,'stop_generation',a.stop_generation,'scope_sha256',a.scope_sha256,'authority_event_id',p.budget_evidence#>>'{monetary_policy,authority_event_id}','review',review,'prior_adapters',prior_adapters,'registered_adapters',registered,'tasks',tasks,'input_versions',versions,'retained_candidate_mapping',mapping,'maximum_sources',5,'maximum_total_pages',10,'maximum_response_bytes_each',8388608,'maximum_source_records_each',50000,'wake_receipt',wake,'accepted_company_units',0,'accepted_family_units',0,'legacy_and_cumulative_counters_preserved',true));
END;
$register$;
COMMIT;
