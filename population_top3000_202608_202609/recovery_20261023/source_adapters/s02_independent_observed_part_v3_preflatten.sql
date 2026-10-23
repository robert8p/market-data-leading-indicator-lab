-- Root read-only standalone part. No session continuity or production helper call.
BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY;
SET LOCAL statement_timeout='10s';
SET LOCAL lock_timeout='500ms';
SET LOCAL timezone='UTC';
SET LOCAL datestyle='ISO, YMD';
SET LOCAL work_mem='16MB';
WITH canonical_keys AS MATERIALIZED (
 SELECT DISTINCT r.unit_key FROM population_top3000_202608_202609_v1.massive_shortinterest_record_v1 r
 WHERE r.archive_key='massive-archive:S02:c1ed3ed6aa5bad110f122c2459ba27f6daa14a7ac484363d4915c0fc9ddb1f88'
), population_keys AS MATERIALIZED (
 SELECT o.unit_key,o.record_sha256,o.xmin::text row_xmin
 FROM population_top3000_202608_202609_v1.source_observation o
 WHERE o.release_id='top3000_companies_202608_202609_v1' AND o.family_id='S02'
 AND o.partition_key='short-interest-settlement-20260715-20260930'
), membership AS MATERIALIZED (
 SELECT k.*,c.unit_key IS NULL unexpected
 FROM population_keys k LEFT JOIN canonical_keys c USING(unit_key)
), summary AS MATERIALIZED (
 SELECT count(*) versions,count(*) FILTER(WHERE unexpected) unexpected,
 encode(sha256(convert_to(coalesce(string_agg(jsonb_build_array(unit_key,record_sha256,row_xmin)::text,E'\n' ORDER BY unit_key),''),'UTF8')),'hex') manifest_sha256
 FROM membership
)
SELECT jsonb_build_object('check_revision','S02_READBACK_OBSERVED_PART_V3',
 'archive_key','massive-archive:S02:c1ed3ed6aa5bad110f122c2459ba27f6daa14a7ac484363d4915c0fc9ddb1f88','observed_at',clock_timestamp(),
 'canonical_versions',(SELECT count(*) FROM canonical_keys),
 'versions',s.versions,'unexpected',s.unexpected,
 'missing',(SELECT count(*) FROM canonical_keys)-s.versions+s.unexpected,
 'manifest_sha256',s.manifest_sha256,'source_version_membership_checked',true,
 'company_completeness_verified',false,'independent_provider_accuracy_verified',false) AS s02_observed_part
FROM summary s;
COMMIT;
