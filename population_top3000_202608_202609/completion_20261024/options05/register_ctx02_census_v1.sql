-- Root current-controller integration only, after independent review, actual rollback and actual Edge deployment.
-- Required root events: CTX02_CENSUS_V1_ROLLBACK_ACCEPTED and CTX02_CENSUS_V1_EDGE_DEPLOYED.
-- No assertions in this file are anticipated evidence; all receipt predicates must find actual records.
BEGIN;
SET LOCAL lock_timeout='15s';SET LOCAL statement_timeout='25s';
DO $register$
DECLARE a population_top3000_202608_202609_v1.acquisition_control%rowtype;p population_top3000_202608_202609_v1.pipeline_control%rowtype;
 proof jsonb;ev bigint;deployed bigint;review_sha text;f record;fn regprocedure;prior jsonb;dispatch text;before_counters jsonb;after_counters jsonb;
 handler_hash constant text:='1c9032bb84616059d62a052316c8277f21ba0600d3dedc5f3cf5184661283f23';worker_hash constant text:='e5b9b2621df834eaec1f8443f00a6363fb75c6dde3287bfb0b4b63e9645d18f6';harness_hash constant text:='30388ecdca3b44267b0c52d5560ed5494e2d952c8e376834e010d9d72ac0e329';
BEGIN
 PERFORM pg_advisory_xact_lock(2026083000261022);
 SELECT * INTO STRICT a FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton FOR UPDATE;
 SELECT * INTO STRICT p FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton FOR UPDATE;
 IF a.owner_id IS DISTINCT FROM 'top3000-completion-20261024-root-5db48b32d103' OR a.owner_fence IS DISTINCT FROM 3 OR a.stop_generation IS DISTINCT FROM 1
 OR a.stopped IS DISTINCT FROM false OR a.enabled IS DISTINCT FROM true OR p.enabled IS DISTINCT FROM true OR a.hold_reason IS NOT NULL
 OR a.admitted_until IS NULL OR a.admitted_until<=clock_timestamp() OR p.deadline IS NULL OR p.deadline<=clock_timestamp() OR p.shared_scheduler_verified IS DISTINCT FROM true
 OR p.expected_owner_id IS DISTINCT FROM a.owner_id OR p.expected_owner_fence IS DISTINCT FROM a.owner_fence OR p.expected_stop_generation IS DISTINCT FROM a.stop_generation
 OR a.scope_sha256 IS DISTINCT FROM '8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd' OR p.scope_sha256 IS DISTINCT FROM a.scope_sha256
 THEN RAISE EXCEPTION 'CTX02_CENSUS_REGISTRATION_CURRENT_CONTROLLER_REQUIRED';END IF;
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE family_id='CTX02' AND stage IN('ACQUIRE','NORMALIZE','DERIVE','VALIDATE') AND status='LEASED') THEN RAISE EXCEPTION 'CTX02_CENSUS_ACTIVE_ATTEMPT_MUST_FINISH';END IF;
 SELECT event_id,details->'runtime_receipt',details->>'independent_review_receipt_sha256' INTO ev,proof,review_sha
 FROM population_top3000_202608_202609_v1.pipeline_event WHERE event='CTX02_CENSUS_V1_ROLLBACK_ACCEPTED'
 AND details->>'owner_id'=a.owner_id AND details->>'owner_fence'=a.owner_fence::text AND details->>'stop_generation'=a.stop_generation::text
 AND details->>'handler_file_sha256'=handler_hash AND details->>'harness_sha256'=harness_hash ORDER BY event_id DESC LIMIT 1;
 IF ev IS NULL OR review_sha IS NULL OR review_sha!~'^[0-9a-f]{64}$' OR proof->>'result' IS DISTINCT FROM 'PASS_ACTUAL_CTX02_CENSUS_FUNCTIONS_ROLLBACK'
 OR proof->>'provider_http_calls' IS DISTINCT FROM '0' OR proof->>'records' IS DISTINCT FROM '2' OR jsonb_array_length(proof->'stages') IS DISTINCT FROM 4
 OR proof->>'bad_token_rejected' IS DISTINCT FROM 'true' OR proof->>'stale_fence_rejected' IS DISTINCT FROM 'true' OR proof->>'later_stop_rejected' IS DISTINCT FROM 'true'
 OR proof->>'source_hash_rejected' IS DISTINCT FROM 'true' OR proof->>'helper_bundle_tamper_rejected' IS DISTINCT FROM 'true' OR proof->>'parsed_tamper_rejected' IS DISTINCT FROM 'true'
 OR proof->>'nonterminal_chain_rejected' IS DISTINCT FROM 'true' OR proof->>'next_cursor_continuation_verified' IS DISTINCT FROM 'true' OR proof->>'contract_fields_rejected' IS DISTINCT FROM 'true'
 OR proof->>'finite_tranche_blocked' IS DISTINCT FROM 'true' OR proof->>'tranche_extended_same_archive_cursor' IS DISTINCT FROM 'true'
 OR proof->>'production_changes_rolled_back' IS DISTINCT FROM 'true'
 OR proof->>'bundle_sha256' IS DISTINCT FROM population_top3000_202608_202609_v1.ctx02_census_bundle_v1()
 OR proof->>'parser_bundle_sha256' IS DISTINCT FROM population_top3000_202608_202609_v1.ctx02_parser_bundle_v1()
 OR proof->'function_hashes' IS DISTINCT FROM (SELECT jsonb_object_agg(pr.proname,encode(sha256(convert_to(pg_get_functiondef(pr.oid),'UTF8')),'hex')) FROM pg_proc pr JOIN pg_namespace n ON n.oid=pr.pronamespace WHERE n.nspname='population_top3000_202608_202609_v1' AND pr.proname LIKE 'ctx02_census_%_v1')
 THEN RAISE EXCEPTION 'CTX02_CENSUS_ACTUAL_REVIEWED_ROLLBACK_REQUIRED';END IF;
 SELECT event_id INTO deployed FROM population_top3000_202608_202609_v1.pipeline_event WHERE event='CTX02_CENSUS_V1_EDGE_DEPLOYED'
 AND details->>'owner_id'=a.owner_id AND details->>'owner_fence'=a.owner_fence::text AND details->>'stop_generation'=a.stop_generation::text
 AND details->>'worker_file_sha256'=worker_hash AND details->>'slug'='population-top3000-options-census-v1' AND details->>'status'='ACTIVE'
 AND jsonb_typeof(details->'deployment_receipt')='object' AND details->'deployment_receipt'<>'{}'::jsonb ORDER BY event_id DESC LIMIT 1;
 IF deployed IS NULL THEN RAISE EXCEPTION 'CTX02_CENSUS_ACTUAL_DEPLOYMENT_REQUIRED';END IF;
 SELECT jsonb_agg(to_jsonb(x) ORDER BY x.stage) INTO prior FROM population_top3000_202608_202609_v1.pipeline_adapter x WHERE family_id='CTX02';
 before_counters:=jsonb_build_object('provider_requests',a.provider_requests,'http_invocations',a.http_invocations,'stored_bytes',a.stored_bytes,'reserved_bytes',a.reserved_bytes,'accrued_usd',p.accrued_usd,'reserved_request_usd',p.reserved_request_usd);
 dispatch:=pg_get_functiondef('population_top3000_202608_202609_v1.pipeline_dispatch_edge_v1(uuid,text,text)'::regprocedure);
 IF encode(sha256(convert_to(dispatch,'UTF8')),'hex')='5fbe2aa9752fe3ac1da5d9bfa2f644908e83b4685015fa850c5cd9cc4dc2c7ca' THEN
  dispatch:=replace(dispatch,$needle$'population-top3000-futures-source-v3')$needle$,$replacement$'population-top3000-futures-source-v3','population-top3000-options-census-v1')$replacement$);
  IF encode(sha256(convert_to(dispatch,'UTF8')),'hex') IS DISTINCT FROM '6dc0df9f22a9b30294d2f521a843ad93bd9102de4b40dff241f0792c37a36bea' THEN RAISE EXCEPTION 'CTX02_CENSUS_DISPATCH_PATCH_NOT_EXACT';END IF;
  EXECUTE dispatch;
 ELSIF encode(sha256(convert_to(dispatch,'UTF8')),'hex') IS DISTINCT FROM '6dc0df9f22a9b30294d2f521a843ad93bd9102de4b40dff241f0792c37a36bea' THEN RAISE EXCEPTION 'CTX02_CENSUS_DISPATCH_PREIMAGE_CHANGED';END IF;
 FOR f IN SELECT * FROM (VALUES ('ACQUIRE','ctx02_census_acquire_v1',NULL::text),('NORMALIZE','ctx02_census_normalize_v1','98261d5511f3c5d589c7bb7706efc8128184d1f3a60f1d7aee3be6f889144566'),('DERIVE','ctx02_census_derive_v1','a448f5ef3b52cd2b94a8ee698cac13ad7466a2989c6cdad8744de5cfe8493385'),('VALIDATE','ctx02_census_validate_v1','89c15395689c832dafd35f01d7ac935968b73d7cb3b2fae89273635f28f601ac')) x(stage,fn,old_sha) LOOP
  fn:=('population_top3000_202608_202609_v1.'||f.fn||'(uuid,text)')::regprocedure;
  IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_adapter z WHERE z.family_id='CTX02' AND z.stage=f.stage AND (z.function_sha256 IS DISTINCT FROM encode(sha256(convert_to(pg_get_functiondef(z.function_oid),'UTF8')),'hex') OR z.function_sha256 NOT IN(coalesce(f.old_sha,''),proof->'function_hashes'->>f.fn))) THEN RAISE EXCEPTION 'CTX02_CENSUS_PRIOR_ADAPTER_CHANGED %',f.stage;END IF;
  INSERT INTO population_top3000_202608_202609_v1.pipeline_adapter(family_id,stage,function_oid,function_sha256,resource_class,provider,enabled,reviewed_test_evidence,producer_revision,expected_batch_seconds)
  VALUES('CTX02',f.stage,fn,proof->'function_hashes'->>f.fn,CASE WHEN f.stage='ACQUIRE' THEN 'PROVIDER' ELSE 'DATABASE' END,CASE WHEN f.stage='ACQUIRE' THEN 'Massive' ELSE NULL END,true,
   jsonb_build_object('rollback_event_id',ev,'deployment_event_id',deployed,'independent_review_receipt_sha256',review_sha,'handler_sha256',handler_hash,'worker_sha256',worker_hash,'original_scope_preserved',true,'source_acceptance',false),'ctx02-census-v1-'||left(handler_hash,16),CASE WHEN f.stage='ACQUIRE' THEN 45 ELSE 20 END)
  ON CONFLICT(family_id,stage) DO UPDATE SET function_oid=excluded.function_oid,function_sha256=excluded.function_sha256,resource_class=excluded.resource_class,provider=excluded.provider,enabled=true,reviewed_test_evidence=coalesce(population_top3000_202608_202609_v1.pipeline_adapter.reviewed_test_evidence,'{}'::jsonb)||excluded.reviewed_test_evidence,producer_revision=excluded.producer_revision,expected_batch_seconds=excluded.expected_batch_seconds;
 END LOOP;
 SELECT jsonb_build_object('provider_requests',ac.provider_requests,'http_invocations',ac.http_invocations,'stored_bytes',ac.stored_bytes,'reserved_bytes',ac.reserved_bytes,'accrued_usd',pc.accrued_usd,'reserved_request_usd',pc.reserved_request_usd) INTO after_counters FROM population_top3000_202608_202609_v1.acquisition_control ac CROSS JOIN population_top3000_202608_202609_v1.pipeline_control pc WHERE ac.singleton AND pc.singleton;
 IF after_counters IS DISTINCT FROM before_counters THEN RAISE EXCEPTION 'CTX02_CENSUS_REGISTRATION_COUNTERS_CHANGED';END IF;
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details)VALUES('CTX02_CENSUS_V1_REGISTERED',jsonb_build_object('owner_id',a.owner_id,'owner_fence',a.owner_fence,'stop_generation',a.stop_generation,'prior_adapters',prior,'rollback_event_id',ev,'deployment_event_id',deployed,'independent_review_receipt_sha256',review_sha,'handler_file_sha256',handler_hash,'worker_file_sha256',worker_hash,'cumulative_counters_preserved',true,'counters',after_counters,'dispatch_preimage','5fbe2aa9752fe3ac1da5d9bfa2f644908e83b4685015fa850c5cd9cc4dc2c7ca','dispatch_postimage','6dc0df9f22a9b30294d2f521a843ad93bd9102de4b40dff241f0792c37a36bea'));
END;$register$;
COMMIT;
