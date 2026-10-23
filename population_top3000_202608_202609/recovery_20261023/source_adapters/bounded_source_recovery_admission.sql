BEGIN;
SET LOCAL lock_timeout='3s';
SET LOCAL statement_timeout='10s';
SELECT pg_advisory_xact_lock(2026083000261022);
DO $admission$
DECLARE a population_top3000_202608_202609_v1.acquisition_control%rowtype;
 p population_top3000_202608_202609_v1.pipeline_control%rowtype;
 v_probe population_top3000_202608_202609_v1.resource_probe_v1%rowtype;
 v_evidence jsonb; v_data jsonb;v_rel bigint;
BEGIN
 SELECT * INTO a FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton FOR UPDATE;
 SELECT * INTO p FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton FOR UPDATE;
 IF a.owner_id IS DISTINCT FROM 'top3000-recovery-20261023-root-4b1314bcb5c2' OR a.owner_fence<>2 OR a.stop_generation<>0
 OR p.expected_owner_id IS DISTINCT FROM a.owner_id OR p.expected_owner_fence<>2 OR p.expected_stop_generation<>0
 OR NOT a.enabled OR a.stopped OR a.hold_reason IS NOT NULL OR NOT p.enabled
 OR a.scope_sha256 IS DISTINCT FROM '8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd'
 OR p.deadline IS DISTINCT FROM '2026-10-25T22:36:17.687194Z'::timestamptz
 THEN RAISE EXCEPTION 'MISSION_OWNER_SCOPE_STOP_OR_DEADLINE_CHANGED';END IF;
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_event WHERE event='BOUNDED_SOURCE_RECOVERY_ADMISSION' AND details->>'admission_id'='recovery-wave1-20261023') THEN RETURN;END IF;
 IF a.provider_requests<>5699 OR a.http_invocations<>5707 OR a.reserved_bytes<>0
 OR p.unavoidable_usd<>6.03 OR p.remaining_uncommitted_usd<>193.97 OR p.repair_egress_reserve_usd<>50
 OR p.actual_usd<>0 OR p.accrued_usd<>0 OR p.reserved_request_usd<>0
 OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_provider_lease WHERE released_at IS NULL)
 THEN RAISE EXCEPTION 'BUDGET_BASELINE_CHANGED_RECONCILE_BEFORE_ADMISSION';END IF;
 SELECT * INTO v_probe FROM population_top3000_202608_202609_v1.resource_probe_v1
 WHERE probe_attempt=3 AND state='SUCCEEDED' AND result_sha256='b3e4972e2f0a773ee705258d739c8d46fcf36daeb7138d258451f57ffba1bd4b';
 IF NOT FOUND OR v_probe.finished_at<clock_timestamp()-interval '1 hour' THEN RAISE EXCEPTION 'CURRENT_RESOURCE_PROOF_REQUIRED';END IF;
 SELECT x INTO v_data FROM jsonb_array_elements(v_probe.result->'metrics')x WHERE x->>'mountpoint'='/data';
 IF (v_data->>'available_bytes')::bigint<100000000000 THEN RAISE EXCEPTION 'MEASURED_DISK_HEADROOM_INSUFFICIENT';END IF;
 SELECT sum(pg_total_relation_size(r.oid)) INTO v_rel FROM pg_class r JOIN pg_namespace n ON n.oid=r.relnamespace
 WHERE n.nspname='population_top3000_202608_202609_v1' AND r.relkind IN('r','m');
 IF v_rel>=2000000000 THEN RAISE EXCEPTION 'MISSION_RELATION_BYTE_LIMIT';END IF;
 v_evidence:=p.budget_evidence||jsonb_build_object(
 'verified_at',clock_timestamp(),'admission_id','recovery-wave1-20261023','admission_class','BOUNDED_FREE_AND_PREPAID_SOURCE_RECOVERY',
 'provider_admissions_allowed',true,'external_http_calls_allowed',true,'allowed_providers',jsonb_build_array('FINRA','Massive'),
 'paid_headroom_usd',0,'new_service_or_subscription',false,'full_mission_invoice_reconciled',false,
 'recorded_cost_fields_are_not_invoice_confirmation',true,
 'billing_policy','Bounded free/public and existing prepaid endpoints only. Conservative encumbrances cover recorded prior activity, recovery infrastructure and a separate first-month storage contingency. No purchase or capacity change admitted.',
 'nonrepair_budget_encumbered_usd',76.03,
 'encumbrance_components_usd',jsonb_build_object('prior_documented_mission_activity',10,'resource_probes_preserved',6.03,'new_bounded_worker_infrastructure',10,'unreconciled_prior_storage_first_month_contingency',50),
 'uncommitted_is_not_purchase_authority',true,
 'resource_limits',jsonb_build_object('review_due_at','2026-10-24T12:15:00Z','max_total_ticks',3000,'max_active_tasks',1,'max_source_bytes',250000000,
 'max_relation_bytes',2000000000,'max_single_response_bytes',8388608,'additional_provider_requests',200,'additional_http_invocations',250,
 'provider_request_ceiling',5899,'http_invocation_ceiling',5957,'no_capacity_change',true,'no_paid_model_calls',true),
 'resource_proof',jsonb_build_object('probe_id',v_probe.probe_id,'result_sha256',v_probe.result_sha256,'data_available_bytes',v_data->'available_bytes',
 'observed_at',v_probe.result->'observed_at','mission_relation_bytes',v_rel),
 'pricing_sources',jsonb_build_array('https://supabase.com/docs/guides/platform/manage-your-usage/edge-function-invocations',
 'https://supabase.com/docs/guides/platform/manage-your-usage/egress','https://supabase.com/docs/guides/platform/manage-your-usage/compute','https://supabase.com/docs/guides/platform/database-size'));
 v_evidence:=jsonb_set(v_evidence,'{resource_probe_admission,allowed}','false'::jsonb);
 v_evidence:=jsonb_set(v_evidence,'{allowed_work}',coalesce(v_evidence->'allowed_work','[]')||jsonb_build_array(
 'Bounded retained-price manifests and structural verification in place; provisional cohort coverage',
 'Official FINRA CNMS daily file acquisition and exact source normalization for 42 frozen sessions; unresolved cohort mapping stays explicit',
 'Existing entitled Massive splits, dividends and news archive pages for the frozen target dates; full pagination tracked; no paid add-ons'));
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details)
 VALUES('BOUNDED_SOURCE_RECOVERY_ADMISSION',jsonb_build_object('admission_id','recovery-wave1-20261023','owner_id',a.owner_id,'owner_fence',2,
 'scope_sha256',a.scope_sha256,'previous_budget_evidence',p.budget_evidence,'new_budget_evidence',v_evidence,
 'recorded_cost_not_invoice_verified',true,'original_deadline_preserved',p.deadline,'new_purchases',0));
 UPDATE population_top3000_202608_202609_v1.pipeline_control
 SET unavoidable_usd=76.03,remaining_uncommitted_usd=123.97,budget_verified_at=clock_timestamp(),budget_evidence=v_evidence,max_ticks=3000 WHERE singleton;
 UPDATE population_top3000_202608_202609_v1.acquisition_control
 SET max_provider_requests=5899,max_http_invocations=5957,admitted_until='2026-10-24T12:15:00Z',updated_at=clock_timestamp() WHERE singleton;
END $admission$;
COMMIT;
SELECT jsonb_build_object('actual_recorded',p.actual_usd,'accrued_recorded',p.accrued_usd,'encumbrances',p.unavoidable_usd,
'repair_reserve',p.repair_egress_reserve_usd,'uncommitted',p.remaining_uncommitted_usd,'cap',p.budget_cap_usd,
'provider_ceiling',a.max_provider_requests,'http_ceiling',a.max_http_invocations,'admitted_until',a.admitted_until,'deadline',p.deadline,
'provider_admissions_allowed',p.budget_evidence->'provider_admissions_allowed') admission
FROM population_top3000_202608_202609_v1.acquisition_control a CROSS JOIN population_top3000_202608_202609_v1.pipeline_control p;
