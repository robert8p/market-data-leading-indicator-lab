-- Controller-only finite operational extension. Run as one reviewed migration.
-- No provider calls, capacity deployment, counter reset, or deadline changes.
DO $guard$
DECLARE a population_top3000_202608_202609_v1.acquisition_control%rowtype;
 p population_top3000_202608_202609_v1.pipeline_control%rowtype;
 before_counters jsonb; after_counters jsonb; cost_proof boolean; tranche_event_id bigint;
BEGIN
 IF NOT pg_try_advisory_xact_lock(2026083000261022) THEN RAISE EXCEPTION 'MISSION_CONTROLLER_LOCK_BUSY';END IF;
 SELECT * INTO STRICT a FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton FOR UPDATE;
 SELECT * INTO STRICT p FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton FOR UPDATE;
 IF a.owner_id IS DISTINCT FROM 'top3000-completion-20261024-root-5db48b32d103'
  OR a.owner_fence IS DISTINCT FROM 3 OR a.stop_generation IS DISTINCT FROM 1 OR a.stopped IS DISTINCT FROM false
  OR p.expected_owner_id IS DISTINCT FROM a.owner_id OR p.expected_owner_fence IS DISTINCT FROM a.owner_fence
  OR p.expected_stop_generation IS DISTINCT FROM a.stop_generation
  OR a.scope_sha256 IS DISTINCT FROM '8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd'
  OR p.scope_sha256 IS DISTINCT FROM a.scope_sha256
  OR a.admitted_until IS NULL OR a.admitted_until<=clock_timestamp()
  OR p.deadline IS NULL OR p.deadline<=clock_timestamp()
 THEN RAISE EXCEPTION 'OPERATIONAL_TRANCHE_OWNER_STOP_SCOPE_ADMISSION_REJECTED';END IF;
 cost_proof:=population_top3000_202608_202609_v1.pipeline_cost_admission_v2(p,0);
 IF cost_proof IS DISTINCT FROM true OR p.budget_evidence#>>'{monetary_policy,mode}' IS DISTINCT FROM 'UNCAPPED_AUTHORIZED'
 THEN RAISE EXCEPTION 'OPERATIONAL_TRANCHE_AUTHORITY_NOT_EFFECTIVE: %',cost_proof;END IF;
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_event WHERE event='OPERATIONAL_TRANCHE_20261024_V3') THEN
  IF a.max_provider_requests<>8039 OR a.max_http_invocations<>8055 OR a.max_source_bytes<>500000000
   THEN RAISE EXCEPTION 'OPERATIONAL_TRANCHE_REPLAY_CONFIGURATION_DIFFERS';END IF;
  RETURN;
 END IF;
 IF a.max_provider_requests<>6039 OR a.max_http_invocations<>6055 OR a.max_source_bytes<>250000000
  OR p.max_active_tasks<>1 OR p.budget_evidence#>>'{resource_limits,max_relation_bytes}' IS DISTINCT FROM '4000000000'
 THEN RAISE EXCEPTION 'OPERATIONAL_TRANCHE_PREIMAGE_DIFFERS';END IF;
 before_counters:=jsonb_build_object('provider_requests',a.provider_requests,'http_invocations',a.http_invocations,
  'dispatch_calls',a.dispatch_calls,'stored_bytes',a.stored_bytes,'reserved_bytes',a.reserved_bytes,
  'tick_count',p.tick_count,'actual_usd',p.actual_usd,'accrued_usd',p.accrued_usd,'unavoidable_usd',p.unavoidable_usd,
  'reserved_request_usd',p.reserved_request_usd,'remaining_uncommitted_usd',p.remaining_uncommitted_usd,
  'repair_egress_reserve_usd',p.repair_egress_reserve_usd,'deadline',p.deadline,'admitted_until',a.admitted_until);
 ALTER TABLE population_top3000_202608_202609_v1.acquisition_control DROP CONSTRAINT acquisition_control_max_source_bytes_check;
 ALTER TABLE population_top3000_202608_202609_v1.acquisition_control ADD CONSTRAINT acquisition_control_max_source_bytes_check CHECK(max_source_bytes BETWEEN 0 AND 500000000);
 UPDATE population_top3000_202608_202609_v1.acquisition_control SET max_provider_requests=8039,max_http_invocations=8055,max_source_bytes=500000000,updated_at=clock_timestamp() WHERE singleton;
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details)
 VALUES('OPERATIONAL_TRANCHE_20261024_V3',jsonb_build_object(
  'owner_id',a.owner_id,'owner_fence',a.owner_fence,'stop_generation',a.stop_generation,'scope_sha256',a.scope_sha256,
  'authority_event_id',9528,'before_counters',before_counters,'before_resource_limits',p.budget_evidence->'resource_limits',
  'before_ceilings',jsonb_build_object('provider',6039,'http',6055,'source_bytes',250000000),
  'after_ceilings',jsonb_build_object('provider',8039,'http',8055,'source_bytes',500000000),
  'additional_calls_allocations',jsonb_build_object('REF01',800,'financial',800,'prices',100,'options',200,'context',100),
  'measurement_basis',jsonb_build_object('observed_at','2026-10-24T03:43:07.850362Z','pipeline_provider_requests',5986,'pipeline_http_invocations',6002,'raw_source_bytes',187522771,
   'direct_connector_calls_in_pipeline_counters',false,'direct_connector_physical_http_count','UNKNOWN_REQUIRES_SEPARATE_LEDGER'),
  'conditions',jsonb_build_array('One active pipeline task','Mission relation guard remains4000000000bytes','Per-response guard remains8388608bytes',
   'Shared provider limiting required before further direct connector calls','Preserve unknown-effect leases; no blind retry',
   'SEC independent acquisition respects published provider limit','Additional source bytes do not authorize exceeding relation guard'),
  'no_counter_or_deadline_reset',true,'no_capacity_change_performed',true,'invoice_reconciled',false)) RETURNING event_id INTO tranche_event_id;
 UPDATE population_top3000_202608_202609_v1.pipeline_control SET budget_evidence=
  jsonb_set(budget_evidence,'{resource_limits}',(budget_evidence->'resource_limits')||jsonb_build_object(
    'max_source_bytes',500000000,'provider_request_ceiling',8039,'http_invocation_ceiling',8055,
    'no_paid_model_calls',false,'no_capacity_change',false,
    'necessary_capacity_and_model_use_authorized',true,'finite_resource_admission_still_required',true,
    'operational_tranche_event_id',tranche_event_id,'shared_connector_limiter_required',true))
  ||jsonb_build_object('operational_tranche_20261024_v3',jsonb_build_object('event_id',tranche_event_id,'monetary_authority_event_id',9528,
    'additional_calls_allocations',jsonb_build_object('REF01',800,'financial',800,'prices',100,'options',200,'context',100),
    'direct_connector_probes_require_separate_reconciled_ledger',true))
 WHERE singleton;
 SELECT * INTO STRICT a FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton;
 SELECT * INTO STRICT p FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton;
 after_counters:=jsonb_build_object('provider_requests',a.provider_requests,'http_invocations',a.http_invocations,
  'dispatch_calls',a.dispatch_calls,'stored_bytes',a.stored_bytes,'reserved_bytes',a.reserved_bytes,
  'tick_count',p.tick_count,'actual_usd',p.actual_usd,'accrued_usd',p.accrued_usd,'unavoidable_usd',p.unavoidable_usd,
  'reserved_request_usd',p.reserved_request_usd,'remaining_uncommitted_usd',p.remaining_uncommitted_usd,
  'repair_egress_reserve_usd',p.repair_egress_reserve_usd,'deadline',p.deadline,'admitted_until',a.admitted_until);
 IF after_counters IS DISTINCT FROM before_counters THEN RAISE EXCEPTION 'OPERATIONAL_TRANCHE_COUNTER_OR_DEADLINE_CHANGED';END IF;
END $guard$;
