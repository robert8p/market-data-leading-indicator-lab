CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.pipeline_provider_expiry_accounting_v2(p_attempt uuid)
 RETURNS jsonb
 LANGUAGE plpgsql
 SET search_path TO 'pg_catalog'
AS $function$
DECLARE a population_top3000_202608_202609_v1.acquisition_control%rowtype;
 p population_top3000_202608_202609_v1.pipeline_control%rowtype;
 t population_top3000_202608_202609_v1.pipeline_task%rowtype;
 v population_top3000_202608_202609_v1.pipeline_attempt%rowtype;
 l population_top3000_202608_202609_v1.pipeline_provider_lease%rowtype;
 prior_accounting jsonb;outcome_name text;receipt jsonb;
BEGIN
 PERFORM pg_advisory_xact_lock(2026083000261022);
 SELECT * INTO STRICT a FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton FOR UPDATE;
 SELECT * INTO STRICT p FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton FOR UPDATE;
 IF a.owner_id IS DISTINCT FROM p.expected_owner_id OR a.owner_fence IS DISTINCT FROM p.expected_owner_fence OR a.stop_generation IS DISTINCT FROM p.expected_stop_generation
 OR a.enabled IS DISTINCT FROM true OR a.stopped IS DISTINCT FROM false OR a.hold_reason IS NOT NULL OR p.enabled IS DISTINCT FROM true
 OR population_top3000_202608_202609_v1.pipeline_cost_admission_v2(p,0) IS DISTINCT FROM true
 OR a.admitted_until IS NULL OR a.admitted_until<=clock_timestamp() OR p.deadline IS NULL OR p.deadline<=clock_timestamp()
 OR a.scope_sha256 IS DISTINCT FROM '8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd' OR p.scope_sha256 IS DISTINCT FROM a.scope_sha256
 THEN RAISE EXCEPTION 'EXPIRY_ACCOUNTING_CURRENT_CONTROLLER_REQUIRED';END IF;
 SELECT * INTO STRICT v FROM population_top3000_202608_202609_v1.pipeline_attempt WHERE attempt_id=p_attempt FOR UPDATE;
 SELECT * INTO STRICT t FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=v.task_id FOR UPDATE;
 SELECT * INTO STRICT l FROM population_top3000_202608_202609_v1.pipeline_provider_lease WHERE attempt_id=p_attempt FOR UPDATE;
 IF t.attempt_id IS DISTINCT FROM p_attempt OR v.owner_fence IS DISTINCT FROM a.owner_fence OR v.stop_generation IS DISTINCT FROM a.stop_generation
 OR t.owner_fence IS DISTINCT FROM a.owner_fence OR t.stop_generation IS DISTINCT FROM a.stop_generation
 THEN RAISE EXCEPTION 'EXPIRY_ACCOUNTING_ATTEMPT_FENCE_CHANGED';END IF;
 IF l.released_at IS NOT NULL THEN
  RETURN jsonb_build_object('state','ALREADY_RELEASED','attempt_id',p_attempt,'existing_outcome',l.outcome,'no_accounting_mutation',true);
 END IF;
 IF t.lease_until IS NULL OR t.lease_until>clock_timestamp() OR NOT(t.status='LEASED' OR (t.status='BLOCKED' AND t.blocker_code='UNCERTAIN_EFFECT'))
 THEN RAISE EXCEPTION 'EXPIRY_ACCOUNTING_EXPIRED_EFFECT_REQUIRED';END IF;
 IF l.actual_usd IS NOT NULL THEN RAISE EXCEPTION 'EXPIRY_ACCOUNTING_EXISTING_CHARGE_MUST_NOT_BE_OVERWRITTEN';END IF;
 IF l.reserved_usd::text IN('NaN','Infinity','-Infinity') OR l.reserved_usd<0 OR p.reserved_request_usd<l.reserved_usd
 THEN RAISE EXCEPTION 'EXPIRY_ACCOUNTING_FINITE_COMMITMENT_REQUIRED';END IF;
 prior_accounting:=jsonb_build_object('actual_usd',p.actual_usd,'accrued_usd',p.accrued_usd,'unavoidable_usd',p.unavoidable_usd,
  'reserved_request_usd',p.reserved_request_usd,'remaining_uncommitted_usd',p.remaining_uncommitted_usd,'repair_egress_reserve_usd',p.repair_egress_reserve_usd);
 outcome_name:=CASE WHEN t.status='LEASED' THEN 'EXPIRED_UNINVOICED_RESERVATION_RETAINED' ELSE 'UNCERTAIN_EFFECT_UNINVOICED_RESERVATION_RETAINED' END;
 -- Operational release is separate from billing. The original reservation remains a conservative uninvoiced commitment.
 UPDATE population_top3000_202608_202609_v1.pipeline_provider_lease SET released_at=clock_timestamp(),outcome=outcome_name WHERE attempt_id=p_attempt AND released_at IS NULL;
 receipt:=jsonb_build_object('state','OPERATIONAL_LEASE_RELEASED_UNINVOICED_COMMITMENT_RETAINED','attempt_id',p_attempt,'provider',l.provider,
  'outcome',outcome_name,'reserved_usd_retained',l.reserved_usd,'actual_invoice_charge_usd',null,'invoice_observed',false,
  'actual_usd_recorded',false,'accrued_usd_delta',0,'reserved_request_usd_delta',0,'refund_inferred',false,
  'billing_basis','UNINVOICED_RESERVATION_NOT_ACTUAL_CHARGE','all_historical_totals_preserved',true,'prior_accounting',prior_accounting,
  'owner_id',a.owner_id,'owner_fence',a.owner_fence,'stop_generation',a.stop_generation,'required_action','Reconcile exact provider billing/source receipts before releasing or recognizing the retained commitment; uncertain source task remains blocked.');
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(task_id,attempt_id,event,details)
 VALUES(t.task_id,p_attempt,'PROVIDER_EXPIRY_UNINVOICED_COMMITMENT_RETAINED_V2',receipt);
 RETURN receipt;
END;
$function$
