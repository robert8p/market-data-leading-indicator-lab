BEGIN TRANSACTION READ ONLY;
SET LOCAL statement_timeout='8s';
SET LOCAL lock_timeout='1s';
SELECT observed_at,owner_id,owner_fence,stop_generation,
 source_counts->>'source_reporting_revision' AS source_reporting_revision,
 source_counts->'massive_minute_pilot' AS massive_minute_pilot,
 source_counts->'company_component_source_evidence' AS company_component_source_evidence,
 source_counts->'manual_ref01_source_outcomes' AS manual_ref01_source_outcomes,
 source_counts->'news_independent_all_page_reconciliation' AS news_independent_all_page_reconciliation
FROM population_top3000_202608_202609_v1.recovery_status_v1;
COMMIT;
