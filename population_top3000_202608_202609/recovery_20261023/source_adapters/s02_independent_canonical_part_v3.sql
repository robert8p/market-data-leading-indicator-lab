-- Root read-only standalone part; no connection continuity is required.
BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY;
SET LOCAL statement_timeout='10s';
SET LOCAL lock_timeout='500ms';
SET LOCAL timezone='UTC';
SET LOCAL datestyle='ISO, YMD';
SET LOCAL work_mem='16MB';
WITH canonical AS MATERIALIZED (
 SELECT DISTINCT ON(r.unit_key) r.unit_key,r.page_number,r.source_row
 FROM population_top3000_202608_202609_v1.massive_shortinterest_record_v1 r
 WHERE r.archive_key='massive-archive:S02:c1ed3ed6aa5bad110f122c2459ba27f6daa14a7ac484363d4915c0fc9ddb1f88'
 ORDER BY r.unit_key,r.page_number,r.source_row
), page_groups AS MATERIALIZED (
 SELECT page_number,count(*) canonical_versions FROM canonical GROUP BY page_number
), summary AS MATERIALIZED (
 SELECT jsonb_build_object('canonical_versions',(SELECT count(*) FROM canonical),
  'pages',(SELECT coalesce(jsonb_agg(to_jsonb(g) ORDER BY page_number),'[]'::jsonb) FROM page_groups g)) value
)
SELECT value || jsonb_build_object('check_revision','S02_READBACK_CANONICAL_PART_V3',
 'archive_key','massive-archive:S02:c1ed3ed6aa5bad110f122c2459ba27f6daa14a7ac484363d4915c0fc9ddb1f88','observed_at',clock_timestamp(),
 'company_completeness_verified',false,'independent_provider_accuracy_verified',false) AS s02_canonical_part
FROM summary;
COMMIT;
