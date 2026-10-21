create or replace view research_eq10_fundamental_pit_20261020.financial_worker_progress_v3 with(security_invoker=true) as  SELECT clock_timestamp() AS observed_at,
    to_jsonb(c.*) AS control,
    ( SELECT jsonb_agg(to_jsonb(q.*)) AS jsonb_agg
           FROM ( SELECT financial_worker_jobs_v1.job_kind,
                    financial_worker_jobs_v1.status,
                    count(*) AS jobs
                   FROM research_eq10_fundamental_pit_20261020.financial_worker_jobs_v1
                  GROUP BY financial_worker_jobs_v1.job_kind, financial_worker_jobs_v1.status
                  ORDER BY financial_worker_jobs_v1.job_kind, financial_worker_jobs_v1.status) q) AS jobs,
    ( SELECT count(*) AS count
           FROM research_eq10_fundamental_pit_20261020.financial_filing_metadata_v1) AS metadata_records,
    ( SELECT count(*) AS count
           FROM research_eq10_fundamental_pit_20261020.financial_document_registry_v3) AS stored_documents,
    ( SELECT count(*) AS count
           FROM research_eq10_fundamental_pit_20261020.financial_fact_v1) AS fact_entries,
    ( SELECT count(*) AS count
           FROM research_eq10_fundamental_pit_20261020.financial_metrics_v1) AS normalized_metrics,
    ( SELECT jsonb_agg(to_jsonb(q.*)) AS jsonb_agg
           FROM ( SELECT financial_worker_jobs_v1.last_error_class,
                    count(*) AS jobs
                   FROM research_eq10_fundamental_pit_20261020.financial_worker_jobs_v1
                  WHERE financial_worker_jobs_v1.status = 'BLOCKED'::text
                  GROUP BY financial_worker_jobs_v1.last_error_class) q) AS blocked_reasons,
    ( SELECT jsonb_agg(to_jsonb(q.*)) AS jsonb_agg
           FROM ( SELECT financial_source_pages_v1.page_key,
                    financial_source_pages_v1.expected_rows,
                    financial_source_pages_v1.imported_rows,
                    financial_source_pages_v1.next_params,
                    financial_source_pages_v1.checkpointed_at
                   FROM research_eq10_fundamental_pit_20261020.financial_source_pages_v1
                  WHERE financial_source_pages_v1.route = '/v1/reference/sec/filings'::text
                  ORDER BY financial_source_pages_v1.checkpointed_at DESC
                 LIMIT 1) q) AS metadata_checkpoint
,
(select count(*) from research_eq10_fundamental_pit_20261020.financial_stream_capture_v3 where status='CAPTURING') as partial_source_captures,
(select count(*) from research_eq10_fundamental_pit_20261020.financial_company_states_v3) as company_state_versions,
(select count(distinct entity_cik) from research_eq10_fundamental_pit_20261020.financial_company_states_v3) as state_issuers
   FROM research_eq10_fundamental_pit_20261020.financial_worker_control_v1 c;
create or replace view research_eq10_fundamental_pit_20261020.financial_metric_missingness_v3 with(security_invoker=true) as  WITH docs AS (
         SELECT d_1.accession_number,
            d_1.file_id,
            m.report_date,
            m.filing_date,
            m.form_type,
            a.classification,
            a.effective_available_at,
 coalesce((select j.status='COMPLETE' from research_eq10_fundamental_pit_20261020.financial_worker_jobs_v1 j where j.job_kind='DOCUMENT' and j.accession_number=d_1.accession_number and j.file_id=d_1.file_id limit 1),exists(select 1 from research_eq10_fundamental_pit_20261020.financial_fact_v1 f where f.accession_number=d_1.accession_number and f.file_id=d_1.file_id)) extraction_complete
           FROM research_eq10_fundamental_pit_20261020.financial_document_registry_v3 d_1
             JOIN research_eq10_fundamental_pit_20261020.financial_filing_metadata_v1 m USING (accession_number)
             JOIN research_eq10_fundamental_pit_20261020.financial_availability_review_v2 a USING (accession_number)
        ), mapped AS (
         SELECT f.accession_number,
            f.file_id,
            k_1.metric,
            count(*) AS source_entries,
            count(*) FILTER (WHERE f.extraction_state = 'PARSED'::text) AS parsed_entries,
            count(*) FILTER (WHERE f.extraction_state = 'NIL'::text) AS nil_entries,
            count(*) FILTER (WHERE f.extraction_state = 'FORMAT_UNSUPPORTED'::text) AS unsupported_entries,
            count(*) FILTER (WHERE f.extraction_state = 'PARSED'::text AND f.dimension_count = 0) AS consolidated_entries,
            count(*) FILTER (WHERE f.extraction_state = 'PARSED'::text AND f.dimension_count = 0 AND (f.period_end = m.report_date OR k_1.metric = 'shares_outstanding'::text AND f.period_end >= m.report_date AND f.period_end <= m.filing_date)) AS current_consolidated_entries
           FROM research_eq10_fundamental_pit_20261020.financial_fact_v1 f
             JOIN research_eq10_fundamental_pit_20261020.financial_metric_dictionary_v1 k_1 USING (concept)
             JOIN research_eq10_fundamental_pit_20261020.financial_filing_metadata_v1 m USING (accession_number)
          GROUP BY f.accession_number, f.file_id, k_1.metric
        ), retained AS (
         SELECT financial_metrics_v1.accession_number,
            financial_metrics_v1.file_id,
            financial_metrics_v1.metric,
            count(*) AS n
           FROM research_eq10_fundamental_pit_20261020.financial_metrics_v1
          GROUP BY financial_metrics_v1.accession_number, financial_metrics_v1.file_id, financial_metrics_v1.metric
        )
 SELECT d.accession_number,
    d.file_id,
    k.metric,
    COALESCE(s.source_entries, 0::bigint) AS source_entries,
    COALESCE(r.n, 0::bigint) AS retained_values,
        CASE
            WHEN NOT d.extraction_complete THEN 'EXTRACTION_PENDING'::text
            WHEN COALESCE(r.n, 0::bigint) > 0 THEN 'PRESENT'::text
            WHEN d.form_type = ANY (ARRAY['10-K/A'::text, '10-Q/A'::text]) THEN 'AMENDMENT_NORMALIZATION_HELD'::text
            WHEN d.effective_available_at IS NULL THEN d.classification
            WHEN s.source_entries IS NULL THEN 'SOURCE_CONCEPT_ABSENT_OR_UNMAPPED'::text
            WHEN s.parsed_entries = 0 AND s.unsupported_entries > 0 THEN 'FORMAT_UNSUPPORTED'::text
            WHEN s.parsed_entries = 0 AND s.nil_entries > 0 THEN 'NIL'::text
            WHEN s.consolidated_entries = 0 THEN 'DIMENSIONED_ONLY'::text
            WHEN s.current_consolidated_entries = 0 THEN 'NO_CURRENT_CONSOLIDATED_PERIOD'::text
            ELSE 'UNIT_CONFLICT_OR_OTHER_NORMALIZATION_GUARD'::text
        END AS missingness_classification
   FROM docs d
     CROSS JOIN ( SELECT DISTINCT financial_metric_dictionary_v1.metric
           FROM research_eq10_fundamental_pit_20261020.financial_metric_dictionary_v1) k
     LEFT JOIN mapped s ON s.accession_number = d.accession_number AND s.file_id = d.file_id AND s.metric = k.metric
     LEFT JOIN retained r ON r.accession_number = d.accession_number AND r.file_id = d.file_id AND r.metric = k.metric;
revoke all on research_eq10_fundamental_pit_20261020.financial_worker_progress_v3,research_eq10_fundamental_pit_20261020.financial_metric_missingness_v3 from public,anon,authenticated;