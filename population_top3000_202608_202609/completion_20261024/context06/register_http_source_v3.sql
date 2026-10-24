-- Root/current fenced delegate only. No provider source content or provider request.
-- Requires actual root events (not anticipated evidence):
-- CTX14_HTTP_V3_ROLLBACK_ACCEPTED details {owner_id,owner_fence,stop_generation,handler_file_sha256,harness_sha256,bundle_sha256,runtime_receipt}.
-- CTX14_HTTP_V3_EDGE_DEPLOYED details {owner_id,owner_fence,stop_generation,worker_file_sha256,slug,status:'ACTIVE',deployment_receipt:<actual nonempty object>}.
BEGIN;
SET LOCAL lock_timeout='3s';SET LOCAL statement_timeout='15s';
DO $register$
DECLARE a population_top3000_202608_202609_v1.acquisition_control%rowtype;c population_top3000_202608_202609_v1.pipeline_control%rowtype;
 q record;actual jsonb;test_event bigint;deployment_event bigint;old_adapters jsonb;registered jsonb;review jsonb;dispatch text;before_counters jsonb;after_counters jsonb;wake jsonb;
 handler_hash constant text:='6f7029de367053b043279b3f62fe951c6d44c5fbf55b788f1ead18ddd0feeeef';worker_hash constant text:='fda2d898ceb5cc1b68a8d119940b0cd143318fdab676e8ec55f2a5c8011f37bf';harness_hash constant text:='ba56fd6bf6408f3ad6c48e3b4e082760ab958184b67da1367f479090d8f68855';bundle_hash constant text:='774b504540d44f04eaf00e8c39bd74840e51de4eaf093a782a6e0103c6f079b8';
BEGIN
 PERFORM pg_advisory_xact_lock(2026083000261022);
 SELECT * INTO a FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton FOR UPDATE;
 IF NOT FOUND THEN RAISE EXCEPTION 'CTX14_HTTP_CONTROL_MISSING';END IF;
 SELECT * INTO c FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton FOR UPDATE;
 IF NOT FOUND OR a.owner_id IS DISTINCT FROM 'top3000-completion-20261024-root-5db48b32d103' OR a.owner_fence IS DISTINCT FROM 3 OR a.stop_generation IS DISTINCT FROM 1
 OR a.stopped IS DISTINCT FROM false OR a.enabled IS DISTINCT FROM true OR c.enabled IS DISTINCT FROM true OR a.hold_reason IS NOT NULL
 OR a.admitted_until IS NULL OR a.admitted_until<=clock_timestamp() OR c.deadline IS NULL OR c.deadline<=clock_timestamp() OR c.shared_scheduler_verified IS DISTINCT FROM true
 OR c.expected_owner_id IS DISTINCT FROM a.owner_id OR c.expected_owner_fence IS DISTINCT FROM a.owner_fence OR c.expected_stop_generation IS DISTINCT FROM a.stop_generation
 OR a.scope_sha256 IS DISTINCT FROM '8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd' OR c.scope_sha256 IS DISTINCT FROM a.scope_sha256
 THEN RAISE EXCEPTION 'CTX14_HTTP_REGISTRATION_CURRENT_CONTROLLER_REQUIRED';END IF;
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task t WHERE t.family_id='CTX14' AND t.stage IN('ACQUIRE','NORMALIZE','DERIVE','VALIDATE') AND t.status='LEASED') THEN RAISE EXCEPTION 'CTX14_HTTP_ACTIVE_ATTEMPT_MUST_FINISH';END IF;
 IF NOT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema='population_top3000_202608_202609_v1' AND table_name='pipeline_task' AND column_name='max_executions' AND column_default='10000') THEN RAISE EXCEPTION 'CTX14_HTTP_FINITE_EXECUTION_DEFAULT_CHANGED';END IF;
 IF bundle_hash IS DISTINCT FROM population_top3000_202608_202609_v1.ctx14_http_bundle_v3()
 OR encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.ctx14_http_bundle_v3()'::regprocedure),'UTF8')),'hex') IS DISTINCT FROM 'f4b33d011d658f17ea27c95960460e292924f50f91cab457220d5ce3882b435f' THEN RAISE EXCEPTION 'CTX14_HTTP_REVIEWED_BUNDLE_CHANGED';END IF;
 SELECT event_id,details->'runtime_receipt' INTO test_event,actual FROM population_top3000_202608_202609_v1.pipeline_event
 WHERE event='CTX14_HTTP_V3_ROLLBACK_ACCEPTED' AND details->>'owner_id'=a.owner_id AND details->>'owner_fence'=a.owner_fence::text AND details->>'stop_generation'=a.stop_generation::text
 AND details->>'handler_file_sha256'=handler_hash AND details->>'harness_sha256'=harness_hash AND details->>'bundle_sha256'=bundle_hash ORDER BY event_id DESC LIMIT 1;
 IF test_event IS NULL OR actual->>'result' IS DISTINCT FROM 'PASS_ACTUAL_CTX14_HTTP_FUNCTIONS_ROLLBACK' OR actual->>'records' IS DISTINCT FROM '5'
 OR jsonb_array_length(actual->'stages') IS DISTINCT FROM 4 OR actual->>'provider_http_calls' IS DISTINCT FROM '0'
 OR actual->>'bad_token_rejected' IS DISTINCT FROM 'true' OR actual->>'stale_fence_rejected' IS DISTINCT FROM 'true' OR actual->>'later_stop_rejected' IS DISTINCT FROM 'true'
 OR actual->>'source_hash_rejected' IS DISTINCT FROM 'true' OR actual->>'helper_bundle_tamper_rejected' IS DISTINCT FROM 'true'
 OR actual->>'parsed_tamper_rejected' IS DISTINCT FROM 'true' OR actual->>'nonterminal_chain_rejected' IS DISTINCT FROM 'true'
 OR actual->>'next_cursor_continuation_verified' IS DISTINCT FROM 'true' OR actual->>'fractional_volume_rejected' IS DISTINCT FROM 'true'
 OR actual->>'production_changes_rolled_back' IS DISTINCT FROM 'true' THEN RAISE EXCEPTION 'CTX14_HTTP_ACTUAL_ROLLBACK_RECEIPT_REQUIRED';END IF;
 SELECT event_id INTO deployment_event FROM population_top3000_202608_202609_v1.pipeline_event WHERE event='CTX14_HTTP_V3_EDGE_DEPLOYED'
 AND details->>'owner_id'=a.owner_id AND details->>'owner_fence'=a.owner_fence::text AND details->>'stop_generation'=a.stop_generation::text
 AND details->>'worker_file_sha256'=worker_hash AND details->>'slug'='population-top3000-futures-source-v3' AND details->>'status'='ACTIVE'
 AND jsonb_typeof(details->'deployment_receipt')='object' AND details->'deployment_receipt'<>'{}'::jsonb ORDER BY event_id DESC LIMIT 1;
 IF deployment_event IS NULL THEN RAISE EXCEPTION 'CTX14_HTTP_ACTUAL_EDGE_DEPLOYMENT_REQUIRED';END IF;
 SELECT jsonb_agg(to_jsonb(x) ORDER BY x.stage) INTO old_adapters FROM population_top3000_202608_202609_v1.pipeline_adapter x WHERE x.family_id='CTX14' AND x.stage IN('ACQUIRE','NORMALIZE','DERIVE','VALIDATE');
 IF jsonb_array_length(old_adapters) IS DISTINCT FROM 4 THEN RAISE EXCEPTION 'CTX14_HTTP_FOUR_PRIOR_ADAPTERS_REQUIRED';END IF;
 before_counters:=jsonb_build_object('provider_requests',a.provider_requests,'http_invocations',a.http_invocations,'stored_bytes',a.stored_bytes,'reserved_bytes',a.reserved_bytes,'accrued_usd',c.accrued_usd,'reserved_request_usd',c.reserved_request_usd,'budget_evidence_sha256',encode(sha256(convert_to(c.budget_evidence::text,'UTF8')),'hex'));
 dispatch:=pg_get_functiondef('population_top3000_202608_202609_v1.pipeline_dispatch_edge_v1(uuid,text,text)'::regprocedure);
 IF encode(sha256(convert_to(dispatch,'UTF8')),'hex')='5600e2c70676a6a6f7115c733c3b91e7a72117396e971d43689bcd3829731fd1' THEN
  dispatch:=replace(dispatch,$needle$'population-top3000-sec-financial-v2')$needle$,$replacement$'population-top3000-sec-financial-v2','population-top3000-futures-source-v3')$replacement$);
  IF encode(sha256(convert_to(dispatch,'UTF8')),'hex') IS DISTINCT FROM '5fbe2aa9752fe3ac1da5d9bfa2f644908e83b4685015fa850c5cd9cc4dc2c7ca' THEN RAISE EXCEPTION 'CTX14_HTTP_DISPATCH_PATCH_NOT_EXACT';END IF;
  EXECUTE dispatch;
 ELSIF encode(sha256(convert_to(dispatch,'UTF8')),'hex') IS DISTINCT FROM '5fbe2aa9752fe3ac1da5d9bfa2f644908e83b4685015fa850c5cd9cc4dc2c7ca' THEN RAISE EXCEPTION 'CTX14_HTTP_DISPATCH_PREIMAGE_CHANGED';END IF;
 IF encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.pipeline_dispatch_edge_v1(uuid,text,text)'::regprocedure),'UTF8')),'hex') IS DISTINCT FROM '5fbe2aa9752fe3ac1da5d9bfa2f644908e83b4685015fa850c5cd9cc4dc2c7ca' THEN RAISE EXCEPTION 'CTX14_HTTP_DISPATCH_POSTIMAGE_CHANGED';END IF;
 review:=jsonb_build_object('handler_file_sha256',handler_hash,'worker_file_sha256',worker_hash,'harness_sha256',harness_hash,'bundle_sha256',bundle_hash,'actual_runtime_event_id',test_event,'deployment_event_id',deployment_event,
 'independent_review_receipt_sha256','be70cd4e5f1527e9d0e79e3b925073c5ec08705cf17d78abfa75670a09563f0c','worker_tests_sha256','86334121288ccc4158fcff8c7453bad72ca29756bf87cda2ba4cfaa3be318a2c','max_pages_per_query',64,'max_executions_default',10000,'source_rows_per_normalize_or_derive_chunk',1000,
 'shared_provider_minimum_gap_seconds',15,'rate_limit_minimum_seconds',60,'original48_requirements_preserved',true,'required_scope_accepted',false,'current_source_receipt_only',true);
 FOR q IN SELECT * FROM (VALUES ('ACQUIRE','population_top3000_202608_202609_v1.ctx14_http_acquire_v3(uuid,text)','197401ab5fef0c64eb28b677fd1d8b6173c9d1aa5a9d02f04fc8fc6f57684db9','cd24482cf7a2e17ea40ecc0615abcbd735bcebae296842d5ba7432526b270d34'),('NORMALIZE','population_top3000_202608_202609_v1.ctx14_http_normalize_v3(uuid,text)','e054552fd1bb1d863f2059d7cce3453b3a031f73c3e7b1ad3511bc867570125b','1b643f1310983977e15fd5b4c42b89c02d3273dc86b9f25026aaf0bedfc2c3b5'),('DERIVE','population_top3000_202608_202609_v1.ctx14_http_derive_v3(uuid,text)','e936caa046b92827529a32375988f24dccecb6aaf3b26c477b4a535c6a8941ee','c0ac6201936fae2018c3b8041871ac31c9961f521749ccf5c31d2e7ff421001e'),('VALIDATE','population_top3000_202608_202609_v1.ctx14_http_validate_v3(uuid,text)','46c194507ee23f221665ca87a8d84afeced6db21998ce38d03216199e6b695af','a8202734ab9ef97ea68a113123f4f2bbd4e314a927fe83056f54e25755369ee5')) v(stage,fn,sha,old_sha) LOOP
  IF q.sha IS DISTINCT FROM encode(sha256(convert_to(pg_get_functiondef(q.fn::regprocedure),'UTF8')),'hex') THEN RAISE EXCEPTION 'CTX14_HTTP_REVIEWED_WRAPPER_CHANGED %',q.stage;END IF;
  IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_adapter pa WHERE pa.family_id='CTX14' AND pa.stage=q.stage AND pa.function_sha256 IN(q.old_sha,q.sha) AND pa.function_sha256=encode(sha256(convert_to(pg_get_functiondef(pa.function_oid),'UTF8')),'hex')) THEN RAISE EXCEPTION 'CTX14_HTTP_PRIOR_ADAPTER_CHANGED %',q.stage;END IF;
  UPDATE population_top3000_202608_202609_v1.pipeline_adapter SET function_oid=q.fn::regprocedure,function_sha256=q.sha,resource_class=CASE WHEN q.stage='ACQUIRE' THEN 'PROVIDER' ELSE 'DATABASE' END,
  provider=CASE WHEN q.stage='ACQUIRE' THEN 'Massive' ELSE NULL END,enabled=true,producer_revision='ctx14-http-v3-'||left(handler_hash,16),expected_batch_seconds=CASE WHEN q.stage='ACQUIRE' THEN 45 ELSE 20 END,
  reviewed_test_evidence=coalesce(reviewed_test_evidence,'{}'::jsonb)||jsonb_build_object('ctx14_http_v3',review) WHERE family_id='CTX14' AND stage=q.stage;
 END LOOP;
 SELECT jsonb_agg(to_jsonb(x) ORDER BY x.stage) INTO registered FROM population_top3000_202608_202609_v1.pipeline_adapter x WHERE x.family_id='CTX14' AND x.stage IN('ACQUIRE','NORMALIZE','DERIVE','VALIDATE');
 SELECT jsonb_build_object('provider_requests',ac.provider_requests,'http_invocations',ac.http_invocations,'stored_bytes',ac.stored_bytes,'reserved_bytes',ac.reserved_bytes,'accrued_usd',pc.accrued_usd,'reserved_request_usd',pc.reserved_request_usd,'budget_evidence_sha256',encode(sha256(convert_to(pc.budget_evidence::text,'UTF8')),'hex')) INTO after_counters FROM population_top3000_202608_202609_v1.acquisition_control ac CROSS JOIN population_top3000_202608_202609_v1.pipeline_control pc WHERE ac.singleton AND pc.singleton;
 IF after_counters IS DISTINCT FROM before_counters THEN RAISE EXCEPTION 'CTX14_HTTP_REGISTRATION_COUNTERS_CHANGED';END IF;
 wake:=population_top3000_202608_202609_v1.pipeline_wake_v1(a.owner_id,a.owner_fence,'CTX14 reviewed futures HTTP v3 registered; retained routes preserved; finite pilot admission follows');
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details)VALUES('CTX14_HTTP_V3_REGISTERED',jsonb_build_object('owner_id',a.owner_id,'owner_fence',a.owner_fence,'stop_generation',a.stop_generation,'scope_sha256',a.scope_sha256,'bundle_sha256',bundle_hash,
 'review',review,'prior_adapters',old_adapters,'registered_adapters',registered,'task_states_modified',false,'cumulative_counters_preserved',true,'counters',after_counters,'dispatcher_before_sha256','5600e2c70676a6a6f7115c733c3b91e7a72117396e971d43689bcd3829731fd1','dispatcher_after_sha256','5fbe2aa9752fe3ac1da5d9bfa2f644908e83b4685015fa850c5cd9cc4dc2c7ca','legacy_routes_preserved',true,'wake_receipt',wake));
END; $register$;
COMMIT;
