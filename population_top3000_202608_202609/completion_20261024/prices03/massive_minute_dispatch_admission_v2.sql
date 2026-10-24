-- Root-only additive dispatch admission. Exact live base preserved; no task/lease/counter resets.
BEGIN;
SET LOCAL lock_timeout='15s'; SET LOCAL statement_timeout='15s';
DO $dispatch$
DECLARE a population_top3000_202608_202609_v1.acquisition_control%rowtype;
 p population_top3000_202608_202609_v1.pipeline_control%rowtype;
 old_def text;new_def text;old_hash text;new_hash text;
BEGIN
 PERFORM pg_advisory_xact_lock(2026083000261022);
 SELECT * INTO a FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton FOR UPDATE;
 IF NOT FOUND THEN RAISE EXCEPTION 'MINUTE_DISPATCH_CONTROL_REQUIRED';END IF;
 SELECT * INTO p FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton FOR UPDATE;
 IF NOT FOUND OR a.owner_id IS DISTINCT FROM 'top3000-completion-20261024-root-5db48b32d103' OR a.owner_fence IS DISTINCT FROM 3 OR a.stop_generation IS DISTINCT FROM 1 OR a.stopped IS DISTINCT FROM false
 OR p.expected_owner_id IS DISTINCT FROM a.owner_id OR p.expected_owner_fence IS DISTINCT FROM a.owner_fence OR p.expected_stop_generation IS DISTINCT FROM a.stop_generation
 OR a.enabled IS DISTINCT FROM true OR p.enabled IS DISTINCT FROM true OR a.hold_reason IS NOT NULL
 OR a.admitted_until IS NULL OR a.admitted_until<=clock_timestamp() OR p.deadline IS NULL OR p.deadline<=clock_timestamp()
 OR p.scope_sha256 IS DISTINCT FROM '8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd' OR a.scope_sha256 IS DISTINCT FROM p.scope_sha256
 OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_event e WHERE e.event_id::text=p.budget_evidence#>>'{monetary_policy,authority_event_id}' AND e.event='USER_AUTHORITY_RESUMED_20261024' AND e.details->>'owner_id'=a.owner_id AND e.details->>'owner_fence'=a.owner_fence::text AND e.details->>'stop_generation'=a.stop_generation::text AND e.details->>'scope_sha256'=a.scope_sha256)
 THEN RAISE EXCEPTION 'MINUTE_DISPATCH_CURRENT_CONTROLLER_REQUIRED';END IF;
 old_def:=pg_get_functiondef('population_top3000_202608_202609_v1.pipeline_dispatch_edge_v1(uuid,text,text)'::regprocedure);
 old_hash:=encode(sha256(convert_to(old_def,'UTF8')),'hex');
 IF old_hash IS DISTINCT FROM '6dc0df9f22a9b30294d2f521a843ad93bd9102de4b40dff241f0792c37a36bea' THEN RAISE EXCEPTION 'MINUTE_DISPATCH_RECONCILE_CHANGED_LIVE_BASE';END IF;
 IF position('population-top3000-minute-v2' in old_def)>0 OR (length(old_def)-length(replace(old_def,'''population-top3000-minute-v1''','')))/length('''population-top3000-minute-v1''')<>1 THEN RAISE EXCEPTION 'MINUTE_DISPATCH_EXPECTED_SINGLE_ALLOWLIST_ENTRY';END IF;
 new_def:=replace(old_def,'''population-top3000-minute-v1''','''population-top3000-minute-v1'',''population-top3000-minute-v2''');
 EXECUTE new_def||';';
 new_hash:=encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.pipeline_dispatch_edge_v1(uuid,text,text)'::regprocedure),'UTF8')),'hex');
 IF pg_get_functiondef('population_top3000_202608_202609_v1.pipeline_dispatch_edge_v1(uuid,text,text)'::regprocedure) IS DISTINCT FROM new_def THEN RAISE EXCEPTION 'MINUTE_DISPATCH_UNEXPECTED_CODE_DELTA';END IF;
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES('MASSIVE_MINUTE_V2_DISPATCH_ADMITTED',jsonb_build_object('owner_id',a.owner_id,'owner_fence',a.owner_fence,'stop_generation',a.stop_generation,'scope_sha256',a.scope_sha256,'old_function_sha256',old_hash,'new_function_sha256',new_hash,'added_slug','population-top3000-minute-v2','all_existing_slugs_preserved',true,'ownership_attempt_and_provider_guards_preserved',true,'counter_resets',false,'provider_requests',0));
END;
$dispatch$;
COMMIT;
