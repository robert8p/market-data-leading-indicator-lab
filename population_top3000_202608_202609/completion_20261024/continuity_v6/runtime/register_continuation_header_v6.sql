-- Execute header, exact reviewed queue/helper/tick definitions, footer, COMMIT in one transaction.
BEGIN;
SET LOCAL lock_timeout='8s'; SET LOCAL statement_timeout='60s';
DO $guard$
DECLARE a population_top3000_202608_202609_v1.acquisition_control%rowtype;p population_top3000_202608_202609_v1.pipeline_control%rowtype;
BEGIN
 PERFORM pg_advisory_xact_lock(2026083000261022);
 SELECT * INTO STRICT a FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton FOR UPDATE;
 SELECT * INTO STRICT p FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton FOR UPDATE;
 IF a.owner_id IS DISTINCT FROM 'top3000-completion-20261024-root-5db48b32d103' OR a.owner_fence IS DISTINCT FROM 3 OR a.stop_generation IS DISTINCT FROM 1 OR a.stopped IS DISTINCT FROM false OR a.enabled IS DISTINCT FROM true OR a.hold_reason IS NOT NULL OR p.enabled IS DISTINCT FROM true
 OR p.expected_owner_id IS DISTINCT FROM a.owner_id OR p.expected_owner_fence IS DISTINCT FROM a.owner_fence OR p.expected_stop_generation IS DISTINCT FROM a.stop_generation
 OR a.scope_sha256 IS DISTINCT FROM '8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd' OR p.scope_sha256 IS DISTINCT FROM a.scope_sha256
 OR a.admitted_until IS NULL OR a.admitted_until<=clock_timestamp() OR p.deadline IS DISTINCT FROM '2026-10-25T22:36:17.687194+00:00'::timestamptz OR p.deadline<=clock_timestamp()
 OR p.tick_count>=p.max_ticks OR p.shared_scheduler_verified IS DISTINCT FROM true OR p.budget_evidence#>>'{monetary_policy,authority_event_id}' IS DISTINCT FROM '9528' OR population_top3000_202608_202609_v1.pipeline_cost_admission_v2(p,0) IS DISTINCT FROM true THEN RAISE EXCEPTION 'CONTINUATION_REGISTRATION_AUTHORITY_REQUIRED';END IF;
 IF encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.pipeline_tick_v1()'::regprocedure),'UTF8')),'hex')<>'43b4c5652c7801874b7a09433bccbdc5e53a39610773f2a1e7a097440f9a2a34' THEN RAISE EXCEPTION 'CONTINUATION_REVIEWED_TICK_PREIMAGE_CHANGED';END IF;
 IF to_regclass('population_top3000_202608_202609_v1.continuation_clear_queue_v6') IS NOT NULL OR to_regprocedure('population_top3000_202608_202609_v1.ctx02_clear_page_admission_v6(text,bigint,bigint,jsonb)') IS NOT NULL THEN RAISE EXCEPTION 'CONTINUATION_ALREADY_INSTALLED_RECONCILE_EFFECTS';END IF;
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE status='LEASED') OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_attempt WHERE outcome='RUNNING') OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_provider_lease WHERE released_at IS NULL) OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.connector_probe_lease_v1 WHERE released_at IS NULL) THEN RAISE EXCEPTION 'CONTINUATION_ACTIVE_OPERATIONS_RETRY';END IF;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_event WHERE event_id=17776 AND event='CONTINUITY_REPAIR_USER_AUTHORITY_V6' AND details->>'owner_id'=a.owner_id AND details->>'owner_fence'=a.owner_fence::text AND details->>'stop_generation'=a.stop_generation::text)
 OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_event WHERE event_id=18030 AND event='CONTINUITY_QUEUE_ACTUAL_ROLLBACK_V6' AND details->>'result'='PASS_CLEARING_QUEUE_ROLLBACK_V6' AND details->>'controls_counters_unchanged'='true' AND details->>'production_changes_rolled_back'='true') THEN RAISE EXCEPTION 'CONTINUATION_EXACT_AUTHORITY_ACTUAL_TEST_REQUIRED';END IF;
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_event c WHERE c.event='ENGINEERING_ASSIGNMENT_CLAIMED_V5' AND (c.details->>'exclusive_component' ILIKE '%CONTINUATION%' OR c.details->>'exclusive_component' ILIKE '%BLOCKED_CLEARING%' OR c.details->>'exclusive_component' ILIKE '%PIPELINE_TICK%') AND NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_event r WHERE r.event IN('ENGINEERING_ASSIGNMENT_COMPLETED_V5','ENGINEERING_ASSIGNMENT_EFFECTS_RECONCILED_RELEASE_V5') AND r.details->>'claim_event_id'=c.event_id::text AND r.details->>'effects_known'='true')) THEN RAISE EXCEPTION 'CONTINUATION_OVERLAPPING_ENGINEERING_CLAIM';END IF;
END;$guard$;
