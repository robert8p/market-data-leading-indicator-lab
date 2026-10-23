-- ROOT ONLY. Additive dynamic source_counts update; no acquisition or scheduling changes.
-- Applied v2 baseline SHA256: 73011b5001673e33b4bfee82556f6da2416c5068327997664f4f3ec6eb363004
BEGIN;
SET LOCAL statement_timeout='15s';
SET LOCAL lock_timeout='2s';
SELECT pg_advisory_xact_lock(2026083000261022);
SET LOCAL statement_timeout='15s';SET LOCAL lock_timeout='2s';DO $owner$ BEGIN IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton AND owner_id='top3000-recovery-20261023-root-4b1314bcb5c2' AND owner_fence=2 AND stop_generation=0) THEN RAISE EXCEPTION 'CURRENT_OWNER_REQUIRED';END IF;END $owner$;
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
'finra_source_readbacks',(SELECT coalesce(jsonb_agg(x),'[]'::jsonb) FROM (SELECT source_comparison_result,count(*) AS files,sum((evidence->>'normalized_rows')::bigint) AS source_rows FROM (SELECT DISTINCT ON(partition_key) partition_key,source_comparison_result,evidence FROM population_top3000_202608_202609_v1.finra_source_readback_v1 ORDER BY partition_key,created_at DESC,readback_id DESC) latest GROUP BY source_comparison_result) x),
'splits_source_records',(SELECT count(*) FROM population_top3000_202608_202609_v1.source_observation WHERE release_id='top3000_companies_202608_202609_v1' AND family_id='N03'),
'dividend_archive',(SELECT coalesce(jsonb_agg(x),'[]'::jsonb) FROM (SELECT partition_key,count(*) AS pages,sum(result_count) AS raw_occurrences,bool_or(next_url IS NULL) AS terminal,count(*) FILTER(WHERE normalized_at IS NOT NULL) AS normalized_pages FROM population_top3000_202608_202609_v1.massive_dividend_page_v1 GROUP BY partition_key) x),
'dividend_deduplicated_observations',(SELECT count(*) FROM population_top3000_202608_202609_v1.source_observation WHERE release_id='top3000_companies_202608_202609_v1' AND family_id='N02'),
'retained_price_inventory_partitions',(SELECT count(*) FROM population_top3000_202608_202609_v1.retained_price_partition_v1),
'count_qualification','Source file and row counts are not certified completeness for the 3000-company target.') || jsonb_build_object(
'news_archive',(SELECT coalesce(jsonb_agg(x),'[]'::jsonb) FROM (SELECT partition_key,count(*) AS pages,sum(result_count) AS source_occurrences,bool_or(next_url IS NULL) AS terminal,count(*) FILTER(WHERE normalized_at IS NOT NULL) AS normalized_pages FROM population_top3000_202608_202609_v1.massive_news_archive_page_v1 GROUP BY partition_key) x),
'news_canonical_versions',(SELECT count(DISTINCT unit_key) FROM population_top3000_202608_202609_v1.massive_news_record_link_v1),
'short_interest_archive',(SELECT coalesce(jsonb_agg(x),'[]'::jsonb) FROM (SELECT partition_key,count(*) AS pages,sum(result_count) AS source_occurrences,bool_or(next_url IS NULL) AS terminal,count(*) FILTER(WHERE normalized_at IS NOT NULL) AS normalized_pages FROM population_top3000_202608_202609_v1.massive_shortinterest_page_v1 GROUP BY partition_key) x),
'daily_compact',(SELECT jsonb_build_object('source_dates_expected',42,'source_dates',count(*),'source_records',coalesce(sum(result_count),0),'normalized_records',coalesce(sum(normalized_source_records),0),'source_reconciled_records',coalesce(sum(verified_source_records),0),'source_reconciled_dates',count(*) FILTER(WHERE source_verified_at IS NOT NULL),'new_source_dates',count(*) FILTER(WHERE acquisition_mode='FETCHED_V2'),'reused_source_dates',count(*) FILTER(WHERE acquisition_mode='REUSED_V1')) FROM population_top3000_202608_202609_v1.d02_daily_source_v2),
'daily_original_repair',(SELECT jsonb_build_object('status',status,'executions',executions,'completed_at',completed_at,'raw_source_records',12402) FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id='2c92d689-55bb-4f1e-83ac-1bd13ecb71c6'),
'dividend_independent_all_page_reconciliation',(SELECT details FROM population_top3000_202608_202609_v1.pipeline_event WHERE event='N02_ALL_SOURCE_PAGES_INDEPENDENT_RECONCILIATION_VERIFIED' ORDER BY at DESC LIMIT 1),
'company_universe',(SELECT jsonb_build_object('status',status,'requested_companies',3000,'frozen_company_count',company_count,'instrument_count',instrument_count,'frozen_at',frozen_at,'unresolved_cutoff_cases',unresolved_cutoff_cases,'unresolved_instrument_cases',unresolved_instrument_cases) FROM population_top3000_202608_202609_v1.pipeline_universe WHERE singleton),
'alpaca_pilot',(SELECT coalesce(jsonb_agg(jsonb_build_object('stage',stage,'status',status,'blocker',blocker_code,'executions',executions,'completed_at',completed_at)),'[]'::jsonb) FROM population_top3000_202608_202609_v1.pipeline_task WHERE family_id='P01' AND partition_key LIKE 'alpaca-sip-%'),
'mission_relation_bytes',(SELECT coalesce(sum(pg_total_relation_size(c.oid)),0) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='population_top3000_202608_202609_v1' AND c.relkind IN('r','m')),
'source_reporting_revision','20261023_recovery_dynamic_sources_v2') || (
-- JSON expression only. Builder appends this to the existing source_counts expression.
-- No source-body reads, parsing, source rehashing, provider calls, or state changes.
jsonb_build_object(
 'source_reporting_revision','20261023_recovery_dynamic_sources_v3',
 'massive_minute_pilot',(
  WITH pilot_tasks AS MATERIALIZED (
   SELECT t.task_id,t.stage,t.status,t.blocker_code,t.executions,t.completed_at,t.last_useful_progress_at,t.payload
   FROM population_top3000_202608_202609_v1.pipeline_task t
   WHERE t.family_id='P01' AND t.partition_key='massive-minute-AAPL-20260908-v1'
    AND t.payload->>'kind'='MASSIVE_MINUTE_PILOT_V1'
  ), pilot_pages AS MATERIALIZED (
   SELECT mp.page_id,mp.archive_key,mp.page_number,mp.next_url,mp.result_count,mp.response_bytes,
    mp.normalized_rows,mp.normalized_at,mp.verified_rows,mp.verified_at,mp.source_summary,mp.retrieved_at
   FROM population_top3000_202608_202609_v1.massive_minute_page_v1 mp
   JOIN pilot_tasks pt ON pt.task_id=mp.acquire_task_id AND pt.stage='ACQUIRE'
  ), page_counts AS (
   SELECT count(*) AS pages,count(DISTINCT pp.archive_key) AS archives,
    coalesce(sum(pp.result_count),0) AS source_occurrences,coalesce(sum(pp.response_bytes),0) AS retained_bytes,
    coalesce(sum(pp.normalized_rows),0) AS normalized_occurrences,
    coalesce(sum(pp.verified_rows),0) AS producer_reconciled_occurrences,
    count(*) FILTER(WHERE pp.normalized_at IS NOT NULL) AS normalized_pages,
    count(*) FILTER(WHERE pp.verified_at IS NOT NULL) AS producer_reconciled_pages,
    count(*) FILTER(WHERE pp.next_url IS NULL) AS terminal_pages,
    coalesce(bool_or(pp.source_summary->>'source_records_reconciled'='true'),false) AS producer_terminal_source_reconciliation_recorded,
    max(pp.retrieved_at) AS latest_source_retrieved_at,max(pp.verified_at) AS latest_source_reconciled_at
   FROM pilot_pages pp
  ), typed_counts AS (
   SELECT count(*) AS typed_rows,
    count(*) FILTER(WHERE mb.share_volume=0) AS explicit_zero_volume_rows,
    count(*) FILTER(WHERE (mb.missing_mask&32)<>0) AS missing_volume_rows,
    count(*) FILTER(WHERE (mb.invalid_mask&32)<>0) AS invalid_volume_rows,
    count(*) FILTER(WHERE mb.missing_mask<>0) AS rows_with_missing_fields,
    count(*) FILTER(WHERE mb.invalid_mask<>0) AS rows_with_invalid_fields,
    count(*) FILTER(WHERE mb.structure_mask<>0) AS rows_with_structural_flags,
    min(mb.bar_start_at) AS first_provider_bar_start,max(mb.bar_start_at) AS last_provider_bar_start
   FROM pilot_pages pp JOIN population_top3000_202608_202609_v1.massive_minute_bar_v1 mb ON mb.page_id=pp.page_id
  )
  SELECT to_jsonb(pc)||to_jsonb(tc)||jsonb_build_object(
   'provider','Massive',
   'source_symbol',(SELECT pt.payload->>'source_symbol' FROM pilot_tasks pt WHERE pt.stage='ACQUIRE' ORDER BY pt.task_id LIMIT 1),
   'source_date',(SELECT pt.payload->>'trading_date' FROM pilot_tasks pt WHERE pt.stage='ACQUIRE' ORDER BY pt.task_id LIMIT 1),
   'stages',(SELECT coalesce(jsonb_agg(jsonb_build_object('stage',pt.stage,'status',pt.status,
     'blocker',pt.blocker_code,'executions',pt.executions,'completed_at',pt.completed_at,
     'last_useful_progress_at',pt.last_useful_progress_at) ORDER BY pt.stage,pt.task_id),'[]'::jsonb) FROM pilot_tasks pt),
   'qualification','One-symbol one-date source pilot. Qualifying-trade bars across New York sessions; no missing-minute zero fill. Source reconciliation does not establish company coverage, SIP-feed identity or independent price accuracy.'
  ) FROM page_counts pc CROSS JOIN typed_counts tc
 ),
 'company_component_source_evidence',(
  WITH facts AS MATERIALIZED (
   SELECT ce.case_id,ce.component,ce.review_state,ce.source_id,ce.recorded_at,
    ce.identity_release_allowed,ce.strict_certified,ce.rank_day_shares_proven,ce.company_cap_verified,ce.selected_company_member
   FROM population_top3000_202608_202609_v1.company_component_source_evidence_v1 ce
  )
  SELECT jsonb_build_object(
   'source_fact_records',count(*),'distinct_source_documents',count(DISTINCT f.source_id),
   'latest_recorded_at',max(f.recorded_at),
   'identity_release_allowed_records',count(*) FILTER(WHERE f.identity_release_allowed),
   'strict_certified_records',count(*) FILTER(WHERE f.strict_certified),
   'rank_day_shares_proven_records',count(*) FILTER(WHERE f.rank_day_shares_proven),
   'company_cap_verified_records',count(*) FILTER(WHERE f.company_cap_verified),
   'selected_company_member_records',count(*) FILTER(WHERE f.selected_company_member),
   'by_case_component_review_state',(SELECT coalesce(jsonb_agg(to_jsonb(g) ORDER BY g.case_id,g.component,g.review_state),'[]'::jsonb)
    FROM (SELECT f2.case_id,f2.component,f2.review_state,count(*) AS source_fact_records,
     count(DISTINCT f2.source_id) AS distinct_source_documents,max(f2.recorded_at) AS latest_recorded_at
     FROM facts f2 GROUP BY f2.case_id,f2.component,f2.review_state) g),
   'qualification','These are retained source facts with their recorded review states; facts are not resolved cases, verified rank-day company capitalisations or selected-company memberships.'
  ) FROM facts f
 ),
 'manual_ref01_source_outcomes',(
  WITH manual_events AS MATERIALIZED (
   SELECT pe.event,pe.at,pe.details
   FROM population_top3000_202608_202609_v1.pipeline_event pe
   WHERE pe.event IN('ROOT_REF01_CONNECTOR_RESERVED_V1','ROOT_REF01_CONNECTOR_RETAINED_V1',
    'ROOT_REF01_CONNECTOR_UNKNOWN_V1','ROOT_REF01_CONNECTOR_ERROR_RECONCILED_V1')
  ), reservations AS MATERIALIZED (
   SELECT DISTINCT ON(me.details->>'reservation_id')
    me.details->>'reservation_id' AS reservation_id,me.at AS reserved_at,me.details AS reservation
   FROM manual_events me
   WHERE me.event='ROOT_REF01_CONNECTOR_RESERVED_V1'
    AND me.details->>'manifest_sha256' IN(
     '1ebab0c5574d665162f5ee13b98a7f8632fca153b95798d500851f00b35170b9',
     '7c6c572fb8e4fed3f4e5136d5928a71a3d819d61fbe0697793086da70b55c07f',
     'ace46fc9d094f58f18cdfac0b1b1adf9fa1c295a9608b3375eeb3d0a6af1e4b7')
   ORDER BY me.details->>'reservation_id',me.at DESC
  ), per_reservation AS MATERIALIZED (
   SELECT rs.reservation_id,rs.reserved_at,rs.reservation->>'manifest_sha256' AS manifest_sha256,
    rs.reservation->>'route_id' AS route_id,rs.reservation#>>'{route,case}' AS company_case,
    rs.reservation#>>'{route,kind}' AS requested_representation,
    sd.source_id,sd.source_sha256,sd.content_bytes AS retained_bytes,sd.retrieved_at,
    sd.evidence->>'outcome_class' AS connector_outcome_class,
    coalesce(sd.evidence->>'error_scope',er.details->>'error_scope') AS error_scope,
    coalesce(sd.evidence->>'provider_request_outcome',er.details->>'provider_request_outcome',
     CASE WHEN rt.at IS NULL AND un.at IS NOT NULL THEN 'UNKNOWN' ELSE 'UNOBSERVED' END) AS provider_request_outcome,
    rt.at AS retention_recorded_at,rt.details->'manual_hold_released' AS retention_hold_released,
    un.at AS unknown_recorded_at,er.at AS error_reconciled_at,
    (a.hold_reason='ROOT_CONNECTOR_RESERVATION:'||rs.reservation_id) IS TRUE AS current_manual_hold,
    coalesce(sd.evidence->'filing_body_acquired',sd.evidence->'source_document_content_fetched',
     sd.evidence#>'{html_extraction,extracted}',sd.evidence#>'{xml_extraction,extracted}') AS source_body_extraction_recorded,
    sd.evidence->'original_wire_bytes_retained' AS original_wire_bytes_retained,
    sd.evidence->'current_receipt_is_not_historical_availability' AS current_receipt_is_not_historical_availability
   FROM reservations rs
   LEFT JOIN population_top3000_202608_202609_v1.source_document sd
    ON sd.source_id='ref01-massive-connector:'||rs.reservation_id AND sd.source_provider='Massive'
   LEFT JOIN LATERAL (
    SELECT me.at,me.details FROM manual_events me WHERE me.event='ROOT_REF01_CONNECTOR_RETAINED_V1'
     AND me.details->>'reservation_id'=rs.reservation_id ORDER BY me.at DESC LIMIT 1
   ) rt ON true
   LEFT JOIN LATERAL (
    SELECT me.at FROM manual_events me WHERE me.event='ROOT_REF01_CONNECTOR_UNKNOWN_V1'
     AND me.details->>'reservation_id'=rs.reservation_id ORDER BY me.at DESC LIMIT 1
   ) un ON true
   LEFT JOIN LATERAL (
    SELECT me.at,me.details FROM manual_events me WHERE me.event='ROOT_REF01_CONNECTOR_ERROR_RECONCILED_V1'
     AND me.details->>'reservation_id'=rs.reservation_id ORDER BY me.at DESC LIMIT 1
   ) er ON true
  )
  SELECT jsonb_build_object(
   'logical_connector_operations_reserved',count(*),
   'retained_representations',count(*) FILTER(WHERE pr.source_id IS NOT NULL),
   'retained_bytes',coalesce(sum(pr.retained_bytes),0),
   'connector_success_results',count(*) FILTER(WHERE pr.connector_outcome_class='SUCCESS'),
   'connector_definitive_failure_results',count(*) FILTER(WHERE pr.connector_outcome_class='DEFINITIVE_FAILURE'),
   'provider_request_outcome_unknown',count(*) FILTER(WHERE pr.provider_request_outcome='UNKNOWN'),
   'outcomes_without_retention_or_error_reconciliation',count(*) FILTER(WHERE pr.retention_recorded_at IS NULL AND pr.error_reconciled_at IS NULL),
   'current_manual_holds',count(*) FILTER(WHERE pr.current_manual_hold),
   'operations',(SELECT coalesce(jsonb_agg(to_jsonb(pr2) ORDER BY pr2.reserved_at,pr2.reservation_id),'[]'::jsonb) FROM per_reservation pr2),
   'transport_errors_with_provider_outcome_unknown',(SELECT coalesce(jsonb_agg(jsonb_build_object(
      'reservation_id',pr3.reservation_id,'company_case',pr3.company_case,'requested_representation',pr3.requested_representation,
      'source_id',pr3.source_id,'source_sha256',pr3.source_sha256,'connector_outcome_class',pr3.connector_outcome_class,
      'error_scope',pr3.error_scope,'provider_request_outcome',pr3.provider_request_outcome,
      'unknown_recorded_at',pr3.unknown_recorded_at,'error_reconciled_at',pr3.error_reconciled_at,
      'current_manual_hold',pr3.current_manual_hold) ORDER BY pr3.reserved_at,pr3.reservation_id),'[]'::jsonb)
     FROM per_reservation pr3 WHERE pr3.error_scope='CONNECTOR_TRANSPORT' AND pr3.provider_request_outcome='UNKNOWN'),
   'qualification','Counts are logical connector operations and retained connector representations. A terminal connector error or released storage hold does not resolve an unknown provider outcome. No automatic refetch, wire-byte identity or historical availability is implied.'
  ) FROM per_reservation pr
 ),
 'news_independent_all_page_reconciliation',(
  SELECT jsonb_build_object('recorded_at',ne.at,'receipt',jsonb_build_object(
   'observed_at',ne.details->'observed_at','check_revision',ne.details->'check_revision','result',ne.details->'result',
   'plan_sha256',ne.details->'plan_sha256','source_pages',ne.details->'source_pages',
   'source_occurrences',ne.details->'source_occurrences','canonical_versions',ne.details->'canonical_versions',
   'observed_versions',ne.details->'observed_versions','duplicate_occurrences',ne.details->'duplicate_occurrences',
   'conflicting_provider_ids',ne.details->'conflicting_provider_ids','mismatches',ne.details->'mismatches',
   'global_mismatches',ne.details->'global_mismatches','page_receipts_sha256',ne.details->'page_receipts_sha256',
   'compared_normalizer_sha256',ne.details->'compared_normalizer_sha256',
   'compared_timestamp_helper_sha256',ne.details->'compared_timestamp_helper_sha256',
   'original_wire_bytes_independently_rehashed',ne.details->'original_wire_bytes_independently_rehashed',
   'independent_provider_accuracy_verified',ne.details->'independent_provider_accuracy_verified',
   'company_completeness_verified',ne.details->'company_completeness_verified',
   'historical_availability_certified',ne.details->'historical_availability_certified',
   'publication_admitted',ne.details->'publication_admitted'))
  FROM population_top3000_202608_202609_v1.pipeline_event ne
  WHERE ne.event='N01_ALL_SOURCE_PAGES_INDEPENDENT_RECONCILIATION_VERIFIED'
  ORDER BY ne.at DESC LIMIT 1
 )
)
) AS source_counts,
    jsonb_build_object('cap_usd', p.budget_cap_usd, 'recorded_actual_usd', p.actual_usd, 'recorded_accrued_usd', p.accrued_usd, 'recorded_unavoidable_usd', p.unavoidable_usd, 'reserved_request_usd', p.reserved_request_usd, 'remaining_work_or_uncertainty_encumbrance_usd', p.remaining_uncommitted_usd, 'repair_egress_reserve_usd', p.repair_egress_reserve_usd, 'full_mission_invoice_reconciled', COALESCE((p.budget_evidence ->> 'full_mission_invoice_reconciled'::text)::boolean, false), 'admission_class', p.budget_evidence ->> 'admission_class'::text, 'provider_admissions_allowed', p.budget_evidence -> 'provider_admissions_allowed'::text, 'evidence', p.budget_evidence, 'caution', 'Recorded ledger zeros do not establish invoice-confirmed zero spend; distinguish bounds, encumbrances and actual invoices.') AS budget,
    population_top3000_202608_202609_v1.pipeline_full_gate_v1() AS completion_gate
   FROM population_top3000_202608_202609_v1.acquisition_control a
     JOIN population_top3000_202608_202609_v1.pipeline_control p USING (singleton)
     LEFT JOIN market_governance.remediation_run_v1 r ON r.run_id = a.mission_id
  WHERE a.singleton;

COMMIT;

