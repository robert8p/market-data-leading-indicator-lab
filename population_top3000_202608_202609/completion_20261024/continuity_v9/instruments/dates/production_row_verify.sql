WITH raw AS MATERIALIZED (
 SELECT d.source_id,x.ordinality::int ordinal,x.value original FROM population_top3000_202608_202609_v1.ref01_date_page_v4 p JOIN population_top3000_202608_202609_v1.ref01_date_request_v4 q ON q.request_key=p.partition_key JOIN population_top3000_202608_202609_v1.source_document d USING(source_id) CROSS JOIN LATERAL jsonb_array_elements(d.content_text::jsonb->'results') WITH ORDINALITY x(value,ordinality) WHERE q.manifest->>'as_of' IN('2026-08-04','2026-08-05','2026-08-06')
), anchors AS MATERIALIZED (
 SELECT ticker,content FROM population_top3000_202608_202609_v1.ref01_practical_instrument_point_v3 WHERE point_date='2026-08-03' AND eligible
), checked AS MATERIALIZED (
 SELECT r.point_date,r.company_id,r.content,r.record_sha256,raw.original,a.content anchor FROM population_top3000_202608_202609_v1.ref01_date_record_v4 r JOIN raw USING(source_id,ordinal) LEFT JOIN anchors a ON a.ticker=r.content#>>'{source_record,ticker}' WHERE r.point_date IN('2026-08-04','2026-08-05','2026-08-06')
)
SELECT point_date,count(*) records_checked,count(*)filter(where company_id IS NOT NULL) selected_identity_records,
 count(*) FILTER(WHERE content->'source_record' IS DISTINCT FROM original) original_record_mismatches,
 count(*) FILTER(WHERE record_sha256 IS DISTINCT FROM encode(sha256(convert_to(content::text,'UTF8')),'hex')) record_hash_mismatches,
 count(*) FILTER(WHERE company_id IS NOT NULL AND (anchor IS NULL OR anchor->>'company_id' IS DISTINCT FROM company_id OR anchor#>>'{source_record,composite_figi}' IS DISTINCT FROM content#>>'{source_record,composite_figi}' OR anchor#>>'{source_record,share_class_figi}' IS DISTINCT FROM content#>>'{source_record,share_class_figi}' OR anchor#>>'{source_record,cik}' IS DISTINCT FROM content#>>'{source_record,cik}' OR anchor#>>'{source_record,type}' IS DISTINCT FROM content#>>'{source_record,type}' OR anchor#>>'{source_record,primary_exchange}' IS DISTINCT FROM content#>>'{source_record,primary_exchange}')) identity_mismatches
FROM checked GROUP BY point_date ORDER BY point_date;
