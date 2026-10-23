-- Root-only read. No mutation, no body dump, no network, no identity promotion.
-- Exact accession targets come from retained SEC_Q3_2026_MASTER_INDEX.
BEGIN READ ONLY;
SET LOCAL statement_timeout='10s';
SET LOCAL lock_timeout='1s';
WITH wanted(priority,case_id,cik,accession) AS MATERIALIZED (VALUES
 (1,'CRAWFORD_CUTOFF_CLASS_VALUATION','0000025475','0001193125-26-330643'),
 (2,'COMMON_REIT_DHC','0001075415','0001075415-26-000030'),
 (3,'LIBERTY_LIVE_REORGANIZATION','0002078416','0001104659-26-092020'),
 (4,'COMMON_REIT_NXDT','0001356115','0001193125-26-350420'),
 (5,'NBN_LPX_REJECTED_ASSOCIATION','0000060519','0000060519-26-000036'),
 (6,'EP_KMI_REJECTED_ASSOCIATION','0000887396','0001104659-26-097162'),
 (7,'ESROP_ELIGIBILITY_AND_DUPLICATION','0001553079','0001553079-26-000009'),
 (8,'RAMACO_BOUNDARY_CLASSES','0001687187','0001104659-26-090902')
), metadata_rows AS MATERIALIZED (
 SELECT w.priority,w.case_id,w.cik expected_cik,w.accession,
        m.accession_number IS NOT NULL metadata_retained,m.source_sha256,
        m.accepted_at,m.acceptance_local,
        m.raw_payload->>'accession_number' provider_accession,
        m.raw_payload->>'cik' provider_cik,
        m.raw_payload->>'form_type' provider_form_type,
        m.raw_payload->>'period_of_report' provider_period_of_report,
        m.raw_payload->>'acceptance_datetime' provider_acceptance_literal,
        split_part(m.raw_payload->>'main_file_url','?',1) main_file_url_without_query
 FROM wanted w
 LEFT JOIN research_eq10_fundamental_pit_20261020.financial_filing_metadata_v1 m
   ON m.accession_number=w.accession
), document_rows AS MATERIALIZED (
 SELECT w.priority,w.case_id,w.cik expected_cik,w.accession,
        d.file_id,d.provider,d.route,d.document_sha256,d.collected_at,
        octet_length(d.raw_document) retained_document_bytes,
        d.provider_metadata->>'http_status' http_status,
        d.provider_metadata->>'raw_bytes_sha256' original_wire_sha256
 FROM wanted w
 JOIN research_eq10_fundamental_pit_20261020.financial_documents_v1 d
   ON d.accession_number=w.accession
), additional_index AS MATERIALIZED (
 SELECT source_id,source_line_number,filing_cik,company_name,form,
        filing_date,accession_number,source_filename,
        row_number() OVER(PARTITION BY filing_cik ORDER BY
          CASE WHEN form IN('10-Q','10-K') THEN 0 ELSE 1 END,
          filing_date DESC,source_line_number) evidence_order
 FROM population_top3000_202608_202609_v1.sec_filing_index_v1
 WHERE source_id='SEC_Q3_2026_MASTER_INDEX'
   AND filing_cik=ANY(ARRAY['0001560385','0001717307','0000869614'])
   AND filing_date BETWEEN '2026-07-01'::date AND '2026-08-14'::date
   AND form IN('10-Q','10-K','8-K')
), bank_sources AS MATERIALIZED (
 SELECT c.ticker,c.group_key,c.cik_candidate,c.classification,
        c.census_job_id,c.census_ordinal,c.census_sha256,c.overview_sha256,
        r.raw_record->>'share_class_figi' share_class_figi,
        r.raw_record->>'primary_exchange' primary_exchange,
        o.snapshot_date,o.raw_record->>'name' provider_name,
        o.raw_record->>'homepage_url' homepage_candidate,
        o.raw_record->>'description' description_candidate
 FROM population_top3000_202608_202609_v1.company_candidate_v1 c
 JOIN population_top3000_202608_202609_v1.reference_row r
   ON r.page_job_id=c.census_job_id AND r.ordinal=c.census_ordinal
 LEFT JOIN population_top3000_202608_202609_v1.ticker_overview o
   ON o.ticker=c.ticker
 WHERE c.ticker IN('NBN','LPX')
)
SELECT jsonb_build_object(
 'observed_at',clock_timestamp(),
 'purpose','TARGETED_RETAINED_SOURCE_DISCOVERY_NO_IDENTITY_ADMISSION',
 'filing_metadata',(SELECT jsonb_agg(to_jsonb(m) ORDER BY m.priority) FROM metadata_rows m),
 'retained_documents',(SELECT jsonb_agg(to_jsonb(d) ORDER BY d.priority,d.file_id) FROM document_rows d),
 'formula_one_and_ilpt_index',(SELECT jsonb_agg(to_jsonb(i)-'evidence_order' ORDER BY i.filing_cik,i.evidence_order)
   FROM additional_index i WHERE i.evidence_order<=6),
 'bank_source_candidates',(SELECT jsonb_agg(to_jsonb(b) ORDER BY b.ticker) FROM bank_sources b),
 'limits',jsonb_build_object('source_bodies_returned',false,'external_requests',0,
   'identity_released',false,'missing_document_means_ineligible',false)
) ref01_closure_retained_inventory;
ROLLBACK;
