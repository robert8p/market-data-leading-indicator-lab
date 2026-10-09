-- Read-only reproduction; original 240-session batch, including complete row equivalence.
BEGIN READ ONLY;
SET LOCAL work_mem='32MB';
SET LOCAL statement_timeout='30s';
SET LOCAL TimeZone='UTC';
WITH r AS MATERIALIZED (
 SELECT * FROM astra_shared_equity_features_20260928_v1.compute_feature_sessions_remediated_v1(6039,6050,'2026-05-01','2026-05-29',false)
), c AS (
 SELECT * FROM astra_shared_equity_features_20260928_v1.feature_session_cache_remediated_v1 WHERE instrument_key BETWEEN 6039 AND 6050 AND session_date BETWEEN '2026-05-01' AND '2026-05-29'
)
SELECT count(*) sessions,
 count(*) FILTER(WHERE (to_jsonb(r)-'built_at') IS DISTINCT FROM (to_jsonb(c)-'built_at')) all_fields_mismatches,
 count(*) FILTER(WHERE r.output_sha256 IS DISTINCT FROM c.output_sha256) output_mismatches,
 count(*) FILTER(WHERE r.source_rows_sha256 IS DISTINCT FROM c.source_rows_sha256) source_mismatches
FROM r FULL JOIN c USING(instrument_key,session_date);
ROLLBACK;
-- Expected: sessions=240; every mismatch count=0.
-- Compare full output and identity fingerprints with evidence/baseline_parity.json.
