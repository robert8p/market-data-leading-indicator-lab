-- Deployed 2026-10-23. Private mission-scoped recovery, not a population-quality waiver.
-- The state mutation entrypoint is in delegated_recovery_request_v1.sql.
-- Run only through the existing migration workflow after live ownership reconciliation.
CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.population_recovery_manifest_v1() RETURNS jsonb LANGUAGE sql STABLE SET search_path TO pg_catalog AS $fn$
 SELECT jsonb_build_object('functions', (SELECT encode(sha256(convert_to(string_agg(p.oid::regprocedure::text||':'||encode(sha256(convert_to(pg_get_functiondef(p.oid),'UTF8')),'hex'),E'\n' ORDER BY p.oid::regprocedure::text),'UTF8')),'hex') FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='population_top3000_202608_202609_v1' AND p.prokind='f'), 'adapters', (SELECT encode(sha256(convert_to(coalesce(jsonb_agg(jsonb_build_object('family',a.family_id,'stage',a.stage,'function',a.function_oid::regprocedure::text,'sha256',a.function_sha256,'enabled',a.enabled,'resource',a.resource_class,'revision',a.producer_revision,'tests',a.reviewed_test_evidence) ORDER BY a.family_id,a.stage)::text,'[]'),'UTF8')),'hex') FROM population_top3000_202608_202609_v1.pipeline_adapter a));
$fn$;
CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.population_recovery_decision_v1(s jsonb) RETURNS text LANGUAGE sql IMMUTABLE SET search_path TO pg_catalog AS $fn$
 SELECT CASE
 WHEN s->>'qualified' IS DISTINCT FROM 'true' THEN 'NO_QUALIFIED_DELEGATION'
 WHEN s->>'owner_stop_matches' IS DISTINCT FROM 'true' THEN 'OWNER_OR_STOP_CHANGED'
 WHEN s->>'scope_matches' IS DISTINCT FROM 'true' THEN 'SCOPE_CHANGED'
 WHEN s->>'stopped' IS DISTINCT FROM 'false' THEN 'USER_STOP'
 WHEN s->>'controls_enabled' IS DISTINCT FROM 'true' THEN 'EXPLICIT_HOLD'
 WHEN s->>'admission_valid' IS DISTINCT FROM 'true' THEN 'AUTHORISATION_EXPIRED'
 WHEN s->>'ticks_available' IS DISTINCT FROM 'true' THEN 'TICK_LIMIT'
 WHEN s->>'budget_scheduler_valid' IS DISTINCT FROM 'true' THEN 'BUDGET_OR_SCHEDULER_HOLD'
 WHEN s->>'scheduler_binding_valid' IS DISTINCT FROM 'true' THEN 'SCHEDULER_BINDING_CHANGED'
 WHEN s->>'scheduler_active'='true' THEN 'ALREADY_ACTIVE'
 WHEN s->>'maintenance_wait'='true' THEN 'BOUNDED_MAINTENANCE_WAIT'
 WHEN s->>'engineering_active'='true' THEN 'ACTIVE_ENGINEERING_CHECKPOINT'
 WHEN s->>'active_operations' IS DISTINCT FROM 'false' THEN 'ACTIVE_OR_UNCERTAIN_OPERATION'
 WHEN s->>'competing_session' IS DISTINCT FROM 'false' THEN 'COMPETING_DATABASE_OPERATION'
 WHEN s->>'code_matches' IS DISTINCT FROM 'true' THEN 'CODE_OR_REGISTRATION_CHANGED_REQUALIFY'
 WHEN s->>'resources_valid' IS DISTINCT FROM 'true' THEN 'RESOURCE_LIMIT'
 WHEN coalesce((s->>'runnable_count')::integer,0)<=0 THEN 'NO_RUNNABLE_WORK'
 WHEN s->>'database_only_reviewed' IS DISTINCT FROM 'true' THEN 'UNREVIEWED_OR_PROVIDER_WORK_REQUIRES_CONTROLLER'
 WHEN s->>'restart_allowance' IS DISTINCT FROM 'true' THEN 'BOUNDED_RECOVERY_LIMIT'
 WHEN s->>'cooldown_and_progress' IS DISTINCT FROM 'true' THEN 'COOLDOWN_OR_NO_USEFUL_PROGRESS'
 ELSE 'ELIGIBLE_FOR_GUARDED_WAKE' END;
$fn$;
REVOKE ALL ON FUNCTION population_top3000_202608_202609_v1.population_recovery_manifest_v1() FROM PUBLIC,anon,authenticated;
REVOKE ALL ON FUNCTION population_top3000_202608_202609_v1.population_recovery_decision_v1(jsonb) FROM PUBLIC,anon,authenticated;
