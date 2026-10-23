-- Root read-only postcheck: one date; no raw JSON parse or production helper call.
BEGIN READ ONLY;SET LOCAL statement_timeout='10s';SET LOCAL lock_timeout='1s';SET LOCAL timezone='UTC';
WITH requested AS MATERIALIZED(SELECT DATE '__SESSION_DATE__' AS session_date),day_source AS MATERIALIZED(
 SELECT d.*,d.xmin::text AS source_row_version,s.xmin::text AS document_row_version,s.source_sha256 AS document_sha256,s.content_bytes AS document_bytes,
 encode(sha256(convert_to(s.content_text,'UTF8')),'hex') AS calculated_source_sha256,octet_length(s.content_text)::bigint AS calculated_source_bytes
 FROM population_top3000_202608_202609_v1.d02_daily_source_v2 d JOIN requested q USING(session_date) LEFT JOIN population_top3000_202608_202609_v1.source_document s ON s.source_id=d.source_id
),typed_rows AS MATERIALIZED(
 SELECT b.source_row,b.xmin::text AS typed_row_version,
 jsonb_build_array(b.source_row,b.source_ticker,b.provider_time_ms,b.provider_ny_date,b.provider_open,b.provider_high,b.provider_low,b.provider_close,b.share_volume,b.trade_count,b.provider_vwap,b.missing_mask,b.invalid_mask,b.structure_mask,encode(b.raw_record_sha256,'hex')) AS row_value
 FROM population_top3000_202608_202609_v1.d02_daily_bar_v2 b WHERE b.source_day_id=(SELECT source_day_id FROM day_source)
),typed_manifest AS MATERIALIZED(
 SELECT count(*) AS typed_occurrences,
 encode(sha256(convert_to(coalesce(string_agg(source_row::text||':'||encode(sha256(convert_to(row_value::text,'UTF8')),'hex'),E'\n' ORDER BY source_row),''),'UTF8')),'hex') AS actual_typed_manifest_sha256,
 encode(sha256(convert_to(coalesce(string_agg(source_row::text||':'||typed_row_version,E'\n' ORDER BY source_row),''),'UTF8')),'hex') AS typed_row_version_manifest_sha256 FROM typed_rows
)
SELECT jsonb_build_object('verifier_version','D02_INDEPENDENT_POSTCHECK_20261023_V1','observed_at',clock_timestamp(),'session_date',q.session_date,
 'source_present',d.source_day_id IS NOT NULL,'source_day_id',d.source_day_id,'source_id',d.source_id,'source_sha256',d.source_sha256,'document_sha256',d.document_sha256,'calculated_source_sha256',d.calculated_source_sha256,'source_bytes',d.response_bytes,'document_bytes',d.document_bytes,'calculated_source_bytes',d.calculated_source_bytes,'result_count',d.result_count,'source_row_version',d.source_row_version,'document_row_version',d.document_row_version,'calendar_manifest_sha256',d.calendar_manifest_sha256,'input_versions',d.input_versions,'producer_complete',coalesce(d.normalized_at IS NOT NULL AND d.source_verified_at IS NOT NULL AND d.normalized_source_records=d.result_count AND d.verified_source_records=d.result_count,false),
 'typed_occurrences',m.typed_occurrences,'actual_typed_manifest_sha256',m.actual_typed_manifest_sha256,'typed_row_version_manifest_sha256',m.typed_row_version_manifest_sha256,'company_population_verified',false,'independent_provider_accuracy_verified',false) AS d02_independent_date_postcheck
FROM requested q LEFT JOIN day_source d USING(session_date) CROSS JOIN typed_manifest m;
COMMIT;
