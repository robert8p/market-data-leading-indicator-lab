CREATE OR REPLACE VIEW population_top3000_202608_202609_v1.recovery_status_v1 AS
SELECT clock_timestamp() AS observed_at,
 a.mission_id,a.owner_id,a.owner_fence,a.stop_generation,a.scope_sha256,
 r.status AS governance_status,r.baseline->>'controller' AS governance_owner,
 r.baseline->>'first_verified_clock' AS mission_accepted_at,p.deadline,
 a.enabled AS acquisition_enabled,a.stopped,a.hold_reason,a.admitted_until,
 p.enabled AS pipeline_enabled,p.shared_scheduler_verified,
 p.last_useful_progress_at,p.next_wakeup_at,p.wake_reason,p.tick_count,p.max_ticks,
 (SELECT to_jsonb(cj) FROM cron.job cj WHERE cj.jobid=596) AS scheduler,
 (SELECT to_jsonb(cr) FROM (SELECT runid,status,start_time,end_time FROM cron.job_run_details WHERE jobid=596 ORDER BY runid DESC LIMIT 1) cr) AS latest_scheduler_run,
 (SELECT coalesce(jsonb_agg(to_jsonb(ts) ORDER BY ts.family_id,ts.stage,ts.status),'[]'::jsonb)
  FROM (SELECT family_id,stage,status,count(*) AS tasks,sum(executions) AS executions FROM population_top3000_202608_202609_v1.pipeline_task GROUP BY family_id,stage,status) ts) AS task_summary,
 (SELECT coalesce(jsonb_agg(jsonb_build_object('task_id',bt.task_id,'family_id',bt.family_id,'stage',bt.stage,'partition_key',bt.partition_key,'blocker',bt.blocker_code,'resumption_condition',bt.resumption_condition,'not_before',bt.not_before,'blocker_until',bt.blocker_until)),'[]'::jsonb)
  FROM population_top3000_202608_202609_v1.pipeline_task bt WHERE bt.status='BLOCKED') AS blocked_tasks,
 jsonb_build_object('candidate_security_rows',(SELECT count(*) FROM population_top3000_202608_202609_v1.company_candidate_v1),
  'provisional_company_groups',(SELECT count(*) FROM population_top3000_202608_202609_v1.company_group_v1),
  'retained_overviews',(SELECT count(*) FROM population_top3000_202608_202609_v1.ticker_overview),
  'target_sec_index_rows',(SELECT count(*) FROM population_top3000_202608_202609_v1.sec_filing_index_v1),
  'candidate_sec_links',(SELECT count(*) FROM population_top3000_202608_202609_v1.sec_candidate_filing_link_v1),
  'macro_provisional_evidence',(SELECT count(*) FROM population_top3000_202608_202609_v1.population_source_evidence_current_v1 WHERE family_id='CTX07'),
  'macro_original_historical_receipts',(SELECT count(*) FROM population_top3000_202608_202609_v1.source_observation WHERE family_id='CTX07' AND historical_received_at IS NOT NULL),
  'provider_requests',a.provider_requests,'http_invocations',a.http_invocations,'stored_source_bytes',a.stored_bytes) AS source_counts,
 jsonb_build_object('cap_usd',p.budget_cap_usd,'recorded_actual_usd',p.actual_usd,'recorded_accrued_usd',p.accrued_usd,
  'recorded_unavoidable_usd',p.unavoidable_usd,'reserved_request_usd',p.reserved_request_usd,
  'remaining_work_or_uncertainty_encumbrance_usd',p.remaining_uncommitted_usd,'repair_egress_reserve_usd',p.repair_egress_reserve_usd,
  'full_mission_invoice_reconciled',coalesce((p.budget_evidence->>'full_mission_invoice_reconciled')::boolean,false),
  'admission_class',p.budget_evidence->>'admission_class','provider_admissions_allowed',p.budget_evidence->'provider_admissions_allowed',
  'evidence',p.budget_evidence,'caution','Recorded ledger zeros do not establish invoice-confirmed zero spend; distinguish bounds, encumbrances and actual invoices.') AS budget,
 population_top3000_202608_202609_v1.pipeline_full_gate_v1() AS completion_gate
FROM population_top3000_202608_202609_v1.acquisition_control a
JOIN population_top3000_202608_202609_v1.pipeline_control p USING(singleton)
LEFT JOIN market_governance.remediation_run_v1 r ON r.run_id=a.mission_id
WHERE a.singleton;
REVOKE ALL ON population_top3000_202608_202609_v1.recovery_status_v1 FROM PUBLIC,anon,authenticated;
GRANT SELECT ON population_top3000_202608_202609_v1.recovery_status_v1 TO service_role;
