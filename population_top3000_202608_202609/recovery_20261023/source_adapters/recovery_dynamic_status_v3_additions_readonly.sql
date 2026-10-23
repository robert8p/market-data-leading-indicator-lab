-- ROOT READ ONLY. Benchmark only new status expressions before installation.
BEGIN TRANSACTION READ ONLY;
SET LOCAL statement_timeout='8s';
SET LOCAL lock_timeout='1s';
SELECT clock_timestamp() AS observed_at,
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
) AS source_count_additions
FROM population_top3000_202608_202609_v1.acquisition_control a
WHERE a.singleton;
COMMIT;
