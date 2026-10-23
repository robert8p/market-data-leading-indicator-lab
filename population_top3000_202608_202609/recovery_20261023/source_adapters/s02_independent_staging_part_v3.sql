-- Root read-only standalone part; no connection continuity is required.
BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY;
SET LOCAL statement_timeout='10s';
SET LOCAL lock_timeout='500ms';
SET LOCAL timezone='UTC';
SET LOCAL datestyle='ISO, YMD';
SET LOCAL work_mem='16MB';
WITH page_groups AS MATERIALIZED (
 SELECT p.page_number,s.*
 FROM population_top3000_202608_202609_v1.massive_shortinterest_page_v1 p
 CROSS JOIN LATERAL (
  SELECT count(*) staged_occurrences,min(r.source_row) first_row,max(r.source_row) last_row,
   encode(sha256(convert_to(string_agg(jsonb_build_array(r.archive_key,r.page_number,r.source_row,r.source_id,r.source_record_key,r.unit_key,r.raw_record_sha256)::text,E'\n' ORDER BY r.source_row),'UTF8')),'hex') staged_links_manifest_sha256,
   encode(sha256(convert_to(string_agg(jsonb_build_array(r.source_row,r.xmin::text)::text,E'\n' ORDER BY r.source_row),'UTF8')),'hex') staged_row_versions_sha256
  FROM population_top3000_202608_202609_v1.massive_shortinterest_record_v1 r
  WHERE r.archive_key=p.archive_key AND r.page_number=p.page_number
  HAVING count(*)>0
 ) s
 WHERE p.archive_key='massive-archive:S02:c1ed3ed6aa5bad110f122c2459ba27f6daa14a7ac484363d4915c0fc9ddb1f88'
), summary AS MATERIALIZED (
 SELECT jsonb_build_object('source_occurrences',
  (SELECT count(*) FROM population_top3000_202608_202609_v1.massive_shortinterest_record_v1 r WHERE r.archive_key='massive-archive:S02:c1ed3ed6aa5bad110f122c2459ba27f6daa14a7ac484363d4915c0fc9ddb1f88'),
  'represented_occurrences',(SELECT coalesce(sum(staged_occurrences),0) FROM page_groups),
  'pages',(SELECT coalesce(jsonb_agg(to_jsonb(g) ORDER BY page_number),'[]'::jsonb) FROM page_groups g)) value
)
SELECT value || jsonb_build_object('check_revision','S02_READBACK_STAGING_PART_V3',
 'archive_key','massive-archive:S02:c1ed3ed6aa5bad110f122c2459ba27f6daa14a7ac484363d4915c0fc9ddb1f88','observed_at',clock_timestamp(),
 'company_completeness_verified',false,'independent_provider_accuracy_verified',false) AS s02_staging_part
FROM summary;
COMMIT;
