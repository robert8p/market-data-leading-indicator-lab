CREATE OR REPLACE VIEW population_top3000_202608_202609_v1.recovery_status_v1 AS
 SELECT clock_timestamp() AS observed_at,
    a.mission_id,
    a.owner_id,
    a.owner_fence,
    a.stop_generation,
    a.scope_sha256,
    r.status AS governance_status,
    r.baseline ->> 'controller'::text AS governance_owner,
    r.baseline ->> 'first_verified_clock'::text AS mission_accepted_at,
    p.deadline,
    a.enabled AS acquisition_enabled,
    a.stopped,
    a.hold_reason,
    a.admitted_until,
    p.enabled AS pipeline_enabled,
    p.shared_scheduler_verified,
    p.last_useful_progress_at,
    p.next_wakeup_at,
    p.wake_reason,
    p.tick_count,
    p.max_ticks,
    ( SELECT to_jsonb(cj.*) AS to_jsonb
           FROM cron.job cj
          WHERE cj.jobid = 596) AS scheduler,
    ( SELECT to_jsonb(cr.*) AS to_jsonb
           FROM ( SELECT job_run_details.runid,
                    job_run_details.status,
                    job_run_details.start_time,
                    job_run_details.end_time
                   FROM cron.job_run_details
                  WHERE job_run_details.jobid = 596
                  ORDER BY job_run_details.runid DESC
                 LIMIT 1) cr) AS latest_scheduler_run,
    ( SELECT COALESCE(jsonb_agg(to_jsonb(ts.*) ORDER BY ts.family_id, ts.stage, ts.status), '[]'::jsonb) AS "coalesce"
           FROM ( SELECT pipeline_task.family_id,
                    pipeline_task.stage,
                    pipeline_task.status,
                    count(*) AS tasks,
                    sum(pipeline_task.executions) AS executions
                   FROM population_top3000_202608_202609_v1.pipeline_task
                  GROUP BY pipeline_task.family_id, pipeline_task.stage, pipeline_task.status) ts) AS task_summary,
    ( SELECT COALESCE(jsonb_agg(jsonb_build_object('task_id', bt.task_id, 'family_id', bt.family_id, 'stage', bt.stage, 'partition_key', bt.partition_key, 'blocker', bt.blocker_code, 'resumption_condition', bt.resumption_condition, 'not_before', bt.not_before, 'blocker_until', bt.blocker_until)), '[]'::jsonb) AS "coalesce"
           FROM population_top3000_202608_202609_v1.pipeline_task bt
          WHERE bt.status = 'BLOCKED'::text) AS blocked_tasks,
    jsonb_build_object('candidate_security_rows', ( SELECT count(*) AS count
           FROM population_top3000_202608_202609_v1.company_candidate_v1), 'provisional_company_groups', ( SELECT count(*) AS count
           FROM population_top3000_202608_202609_v1.company_group_v1), 'retained_overviews', ( SELECT count(*) AS count
           FROM population_top3000_202608_202609_v1.ticker_overview), 'target_sec_index_rows', ( SELECT count(*) AS count
           FROM population_top3000_202608_202609_v1.sec_filing_index_v1), 'candidate_sec_links', ( SELECT count(*) AS count
           FROM population_top3000_202608_202609_v1.sec_candidate_filing_link_v1), 'macro_provisional_evidence', ( SELECT count(*) AS count
           FROM population_top3000_202608_202609_v1.population_source_evidence_current_v1
          WHERE population_source_evidence_current_v1.family_id = 'CTX07'::text), 'macro_original_historical_receipts', ( SELECT count(*) AS count
           FROM population_top3000_202608_202609_v1.source_observation
          WHERE source_observation.family_id = 'CTX07'::text AND source_observation.historical_received_at IS NOT NULL), 'provider_requests', a.provider_requests, 'http_invocations', a.http_invocations, 'stored_source_bytes', a.stored_bytes) || jsonb_build_object(
'finra_retained_session_files',(SELECT count(*) FROM population_top3000_202608_202609_v1.source_document WHERE source_id LIKE 'S01:FINRA_CNMS:%'),
'finra_source_readbacks',(SELECT coalesce(jsonb_agg(x),'[]'::jsonb) FROM (SELECT source_comparison_result,count(*) AS files,sum((evidence->>'normalized_rows')::bigint) AS source_rows FROM population_top3000_202608_202609_v1.finra_source_readback_v1 GROUP BY source_comparison_result) x),
'splits_source_records',(SELECT count(*) FROM population_top3000_202608_202609_v1.source_observation WHERE release_id='top3000_companies_202608_202609_v1' AND family_id='N03'),
'dividend_archive',(SELECT coalesce(jsonb_agg(x),'[]'::jsonb) FROM (SELECT partition_key,count(*) AS pages,sum(result_count) AS raw_occurrences,bool_or(next_url IS NULL) AS terminal,count(*) FILTER(WHERE normalized_at IS NOT NULL) AS normalized_pages FROM population_top3000_202608_202609_v1.massive_dividend_page_v1 GROUP BY partition_key) x),
'dividend_deduplicated_observations',(SELECT count(*) FROM population_top3000_202608_202609_v1.source_observation WHERE release_id='top3000_companies_202608_202609_v1' AND family_id='N02'),
'retained_price_inventory_partitions',(SELECT count(*) FROM population_top3000_202608_202609_v1.retained_price_partition_v1),
'count_qualification','Source file and row counts are not certified completeness for the 3000-company target.') AS source_counts,
    jsonb_build_object('cap_usd', p.budget_cap_usd, 'recorded_actual_usd', p.actual_usd, 'recorded_accrued_usd', p.accrued_usd, 'recorded_unavoidable_usd', p.unavoidable_usd, 'reserved_request_usd', p.reserved_request_usd, 'remaining_work_or_uncertainty_encumbrance_usd', p.remaining_uncommitted_usd, 'repair_egress_reserve_usd', p.repair_egress_reserve_usd, 'full_mission_invoice_reconciled', COALESCE((p.budget_evidence ->> 'full_mission_invoice_reconciled'::text)::boolean, false), 'admission_class', p.budget_evidence ->> 'admission_class'::text, 'provider_admissions_allowed', p.budget_evidence -> 'provider_admissions_allowed'::text, 'evidence', p.budget_evidence, 'caution', 'Recorded ledger zeros do not establish invoice-confirmed zero spend; distinguish bounds, encumbrances and actual invoices.') AS budget,
    population_top3000_202608_202609_v1.pipeline_full_gate_v1() AS completion_gate
   FROM population_top3000_202608_202609_v1.acquisition_control a
     JOIN population_top3000_202608_202609_v1.pipeline_control p USING (singleton)
     LEFT JOIN market_governance.remediation_run_v1 r ON r.run_id = a.mission_id
  WHERE a.singleton;
