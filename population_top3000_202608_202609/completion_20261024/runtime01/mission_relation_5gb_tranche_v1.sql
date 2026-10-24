-- Root-only finite relation growth after freshly measured owned-project /data capacity.
BEGIN;
SET LOCAL lock_timeout='15s';SET LOCAL statement_timeout='30s';
DO $tranche$
DECLARE a population_top3000_202608_202609_v1.acquisition_control%rowtype;p population_top3000_202608_202609_v1.pipeline_control%rowtype;
 probe population_top3000_202608_202609_v1.resource_probe_v2%rowtype;metric jsonb;relation_bytes bigint;available bigint;prior jsonb;new_limit constant bigint:=5000000000;
BEGIN
 PERFORM pg_advisory_xact_lock(2026083000261022);
 SELECT * INTO STRICT a FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton FOR UPDATE;
 SELECT * INTO STRICT p FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton FOR UPDATE;
 IF a.owner_id IS DISTINCT FROM 'top3000-completion-20261024-root-5db48b32d103' OR a.owner_fence IS DISTINCT FROM 3 OR a.stop_generation IS DISTINCT FROM 1 OR a.stopped IS DISTINCT FROM false OR a.enabled IS DISTINCT FROM true OR p.enabled IS DISTINCT FROM true OR a.hold_reason IS NOT NULL
 OR a.admitted_until IS NULL OR a.admitted_until<=clock_timestamp() OR p.deadline IS NULL OR p.deadline<=clock_timestamp()
 OR p.expected_owner_id IS DISTINCT FROM a.owner_id OR p.expected_owner_fence IS DISTINCT FROM a.owner_fence OR p.expected_stop_generation IS DISTINCT FROM a.stop_generation
 OR a.scope_sha256 IS DISTINCT FROM '8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd' OR p.scope_sha256 IS DISTINCT FROM a.scope_sha256
 OR p.shared_scheduler_verified IS DISTINCT FROM true OR p.tick_count>=p.max_ticks
 OR population_top3000_202608_202609_v1.pipeline_cost_admission_v2(p,0) IS DISTINCT FROM true
 OR p.budget_evidence#>>'{resource_limits,max_relation_bytes}' IS DISTINCT FROM '4000000000'
 THEN RAISE EXCEPTION 'RELATION_TRANCHE_CONTROLLER_OR_PREIMAGE_CHANGED';END IF;
 SELECT * INTO probe FROM population_top3000_202608_202609_v1.resource_probe_v2 WHERE probe_attempt=4 AND state='SUCCEEDED';
 IF NOT FOUND OR probe.owner_id IS DISTINCT FROM a.owner_id OR probe.owner_fence<>a.owner_fence OR probe.stop_generation<>a.stop_generation OR probe.scope_sha256 IS DISTINCT FROM a.scope_sha256
 OR probe.finished_at<clock_timestamp()-interval '15 minutes' OR (probe.result->>'observed_at')::timestamptz<clock_timestamp()-interval '15 minutes'
 OR probe.result->>'metrics_http_status' IS DISTINCT FROM '200' OR probe.result_sha256 IS DISTINCT FROM encode(sha256(convert_to(probe.result::text,'UTF8')),'hex')
 THEN RAISE EXCEPTION 'FRESH_RESOURCE_SLOT4_PROOF_REQUIRED';END IF;
 SELECT value INTO STRICT metric FROM jsonb_array_elements(probe.result->'metrics') WHERE value->>'mountpoint'='/data';
 available:=(metric->>'available_bytes')::bigint;
 SELECT coalesce(sum(pg_total_relation_size(c.oid)),0) INTO relation_bytes FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='population_top3000_202608_202609_v1' AND c.relkind IN('r','m');
 IF relation_bytes>=new_limit OR available<20000000000+greatest(0,new_limit-relation_bytes) THEN RAISE EXCEPTION 'INSUFFICIENT_MEASURED_SHARED_PROJECT_HEADROOM';END IF;
 prior:=p.budget_evidence->'resource_limits';
 UPDATE population_top3000_202608_202609_v1.pipeline_control SET budget_evidence=jsonb_set(budget_evidence,'{resource_limits,max_relation_bytes}',to_jsonb(new_limit)) WHERE singleton;
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES('MISSION_RELATION_FINITE_5GB_TRANCHE_V1',jsonb_build_object('owner_id',a.owner_id,'owner_fence',a.owner_fence,'stop_generation',a.stop_generation,'scope_sha256',a.scope_sha256,'before_resource_limits',prior,'new_max_relation_bytes',new_limit,'observed_mission_relation_bytes',relation_bytes,'capacity_probe_id',probe.probe_id,'capacity_result_sha256',probe.result_sha256,'data_available_bytes',available,'minimum_other_project_headroom_preserved_bytes',20000000000,'admitted_uses',jsonb_build_array('RETAINED_DAILY_REMAINING_13044_DENOMINATOR','105_RETAINED_SESSION_BOUNDED_440401920_BYTES','BOUNDED_FINANCIAL_AND_CONTEXT_SUCCESSORS'),'all_cumulative_counters_preserved',true,'deadlines_and_stop_controls_preserved',true,'data_scope_reduced',false,'infrastructure_capacity_changed',false));
END; $tranche$;
COMMIT;
