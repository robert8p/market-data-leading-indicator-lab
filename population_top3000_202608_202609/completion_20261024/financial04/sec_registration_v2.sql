-- Root integration only. No reset of monetary, request, byte, tick or attempt counters.
BEGIN;
SET LOCAL lock_timeout='3s';SET LOCAL statement_timeout='15s';
DO $root$
DECLARE c population_top3000_202608_202609_v1.acquisition_control%rowtype;p population_top3000_202608_202609_v1.pipeline_control%rowtype;old_def text;new_def text;fn regprocedure;old_adapter jsonb;
BEGIN
 PERFORM pg_advisory_xact_lock(2026083000261022);
 SELECT * INTO c FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton FOR UPDATE;
 SELECT * INTO p FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton FOR UPDATE;
 IF c.owner_id IS DISTINCT FROM 'top3000-completion-20261024-root-5db48b32d103' OR c.owner_fence IS DISTINCT FROM 3 OR c.stop_generation IS DISTINCT FROM 1
 OR c.stopped IS DISTINCT FROM false OR c.enabled IS DISTINCT FROM true OR c.hold_reason IS NOT NULL OR c.admitted_until IS NULL OR c.admitted_until<=clock_timestamp()
 OR p.enabled IS DISTINCT FROM true OR p.shared_scheduler_verified IS DISTINCT FROM true OR p.deadline IS NULL OR p.deadline<=clock_timestamp()
 OR p.expected_owner_id IS DISTINCT FROM c.owner_id OR p.expected_owner_fence IS DISTINCT FROM c.owner_fence OR p.expected_stop_generation IS DISTINCT FROM c.stop_generation
 OR c.scope_sha256 IS DISTINCT FROM '8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd' OR p.scope_sha256 IS DISTINCT FROM c.scope_sha256
 THEN RAISE EXCEPTION 'SEC_V2_CURRENT_CONTROLLER_REQUIRED';END IF;
 IF population_top3000_202608_202609_v1.financial_sec_bundle_v2() IS DISTINCT FROM '81bd32ee79f96ecfe01c67950b8268a5203887a7baac6d3b539fd1582cce828a' OR encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.financial_source_acquire_v2(uuid,text)'::regprocedure),'UTF8')),'hex') IS DISTINCT FROM '476533ee53c61b7a08487fd8ee644cf485277d89edc890db4dfef874dfbf41af' THEN RAISE EXCEPTION 'SEC_REVIEWED_DEPLOYED_CODE_CHANGED';END IF;

 SELECT pg_get_functiondef('population_top3000_202608_202609_v1.pipeline_dispatch_edge_v1(uuid,text,text)'::regprocedure) INTO old_def;
 IF encode(sha256(convert_to(old_def,'UTF8')),'hex') NOT IN('9b624ecf7580d11699e980a41bfd09bd035ae775e5b2c5aa1d6fa09673335ba8','5600e2c70676a6a6f7115c733c3b91e7a72117396e971d43689bcd3829731fd1') THEN RAISE EXCEPTION 'SEC_DISPATCHER_BASE_CHANGED';END IF;
 IF encode(sha256(convert_to(old_def,'UTF8')),'hex')='9b624ecf7580d11699e980a41bfd09bd035ae775e5b2c5aa1d6fa09673335ba8' THEN
  IF encode(sha256(convert_to(old_def,'UTF8')),'hex') IS DISTINCT FROM '9b624ecf7580d11699e980a41bfd09bd035ae775e5b2c5aa1d6fa09673335ba8' THEN RAISE EXCEPTION 'SEC_DISPATCHER_BASE_CHANGED';END IF;
  new_def:=replace(old_def,$old$'population-top3000-financial-v1')$old$, $new$'population-top3000-financial-v1','population-top3000-sec-financial-v2')$new$);
  EXECUTE new_def;
 END IF;
 IF encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.pipeline_dispatch_edge_v1(uuid,text,text)'::regprocedure),'UTF8')),'hex') IS DISTINCT FROM '5600e2c70676a6a6f7115c733c3b91e7a72117396e971d43689bcd3829731fd1' THEN RAISE EXCEPTION 'SEC_DISPATCHER_PATCH_MISMATCH';END IF;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_event e WHERE e.event='FINANCIAL_SEC_V2_ROLLBACK_ACCEPTED' AND e.details->>'result'='PASS_ACTUAL_OFFICIAL_SEC_FUNCTIONS_ROLLBACK' AND e.details->>'sql_sha256'='4fefeed94a919fb2d84bcdc9764397ddb31cdfdb045a343d95fdd6a680f83d23' AND e.details->>'worker_sha256'='8ff8404ee3610e8302ee9f7f1f78de2375bcdb124c2b212271b58ffe8f3468d4' AND e.details->>'sec_bundle_sha256'='81bd32ee79f96ecfe01c67950b8268a5203887a7baac6d3b539fd1582cce828a' AND e.details->>'rollback_harness_sha256'='50f73d1c8ed18f071b94e15986e3e426cb3c43abb595081ba840e243dff637e5' AND e.details->>'owner_fence'='3' AND e.details->>'stop_generation'='1') THEN RAISE EXCEPTION 'SEC_ACTUAL_ROOT_ROLLBACK_RECEIPT_REQUIRED';END IF;
 SELECT to_jsonb(a) INTO old_adapter FROM population_top3000_202608_202609_v1.pipeline_adapter a WHERE family_id='F03' AND stage='ACQUIRE';
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE family_id='F03' AND stage='ACQUIRE' AND status='LEASED' AND lease_until>clock_timestamp())
 THEN RAISE EXCEPTION 'SEC_ADAPTER_ACTIVE_ATTEMPT_MUST_FINISH_FIRST';END IF;
 UPDATE population_top3000_202608_202609_v1.pipeline_control SET budget_evidence=jsonb_set(budget_evidence,'{allowed_providers}',coalesce(budget_evidence->'allowed_providers','[]'::jsonb)||'"SEC"'::jsonb)
 WHERE singleton AND NOT coalesce(budget_evidence->'allowed_providers','[]'::jsonb)?'SEC';
 fn:='population_top3000_202608_202609_v1.financial_source_acquire_v2(uuid,text)'::regprocedure;
 UPDATE population_top3000_202608_202609_v1.pipeline_adapter SET function_oid=fn,function_sha256=encode(sha256(convert_to(pg_get_functiondef(fn),'UTF8')),'hex'),
 resource_class='PROVIDER',provider='SEC',expected_batch_seconds=45,producer_revision='financial-official-sec-v2',
 reviewed_test_evidence=reviewed_test_evidence||jsonb_build_object('official_sec_v2',jsonb_build_object('sql_sha256','4fefeed94a919fb2d84bcdc9764397ddb31cdfdb045a343d95fdd6a680f83d23','worker_sha256','8ff8404ee3610e8302ee9f7f1f78de2375bcdb124c2b212271b58ffe8f3468d4','independent_review','context06/sec_transport_independent_review.json','root_actual_rollback_required',true,'one_request',true,'source_bytes_cap',8388608,'prior_handler_fallback_preserved',true))
 WHERE family_id='F03' AND stage='ACQUIRE' AND enabled;
 IF NOT FOUND THEN RAISE EXCEPTION 'SEC_F03_EXISTING_ENABLED_ADAPTER_REQUIRED';END IF;
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details)VALUES('FINANCIAL_SEC_V2_PROVIDER_AND_WORKER_REGISTERED',jsonb_build_object('owner_fence',c.owner_fence,'stop_generation',c.stop_generation,'worker','population-top3000-sec-financial-v2','provider','SEC','scope_sha256',c.scope_sha256,'cost_counter_reset',false,'prior_adapter',old_adapter));
END $root$;
COMMIT;
