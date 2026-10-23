-- Root read-only. Replace only __SESSION_DATE__ with a date from the frozen 42-date list.
-- One source document (<=8MiB) and one typed primary-key range (<=50,000 rows).
-- No production parser, decoder, calendar helper, or validation routine is invoked.
BEGIN READ ONLY;
SET LOCAL statement_timeout='10s';SET LOCAL lock_timeout='1s';SET LOCAL timezone='UTC';
WITH requested AS MATERIALIZED (SELECT DATE '__SESSION_DATE__' AS session_date),
 day_source AS MATERIALIZED (
  SELECT d.*,d.xmin::text AS source_row_version,s.source_provider,s.source_uri,s.source_published_at,s.historical_received_at,s.content_text,s.content_bytes AS document_bytes,s.source_sha256 AS document_sha256,s.retrieved_at AS document_retrieved_at,s.evidence AS document_evidence,s.license_policy,s.xmin::text AS document_row_version,
   a.task_id IS NOT NULL AND a.family_id='D02' AND a.stage='ACQUIRE' AND a.status='COMPLETE' AND a.payload->>'kind'='MASSIVE_GROUPED_DAILY_V2' AND a.payload->>'trading_date'=d.session_date::text AND a.input_versions=d.input_versions AND pa.task_id=a.task_id AND pa.outcome='COMPLETE' AS acquire_link_pass,
   encode(sha256(convert_to(s.content_text,'UTF8')),'hex') AS calculated_source_sha256,octet_length(s.content_text)::bigint AS calculated_source_bytes,
   CASE WHEN s.content_text IS NOT NULL AND octet_length(s.content_text) BETWEEN 1 AND 8388608 THEN s.content_text::jsonb ELSE NULL END AS body
  FROM population_top3000_202608_202609_v1.d02_daily_source_v2 d JOIN requested q USING(session_date)
  LEFT JOIN population_top3000_202608_202609_v1.source_document s ON s.source_id=d.source_id
  LEFT JOIN population_top3000_202608_202609_v1.pipeline_task a ON a.task_id=d.acquire_task_id
  LEFT JOIN population_top3000_202608_202609_v1.pipeline_attempt pa ON pa.attempt_id=d.acquire_attempt_id
 ), envelope AS MATERIALIZED (
  SELECT d.*,
   coalesce(d.content_text IS NOT NULL AND d.source_sha256=d.document_sha256 AND d.source_sha256=d.calculated_source_sha256 AND d.response_bytes=d.document_bytes AND d.response_bytes=d.calculated_source_bytes
    AND d.response_bytes BETWEEN 1 AND 8388608 AND d.source_provider='Massive' AND d.document_retrieved_at=d.retrieved_at AND d.source_published_at IS NULL AND d.historical_received_at IS NULL
    AND d.source_uri='https://api.massive.com/v2/aggs/grouped/locale/us/market/stocks/'||d.session_date::text||'?adjusted=false&include_otc=false'
    AND d.document_evidence->>'wire_sha256'=d.source_sha256 AND d.document_evidence->'wire_bytes'=to_jsonb(d.response_bytes) AND d.document_evidence->'stored_content_bytes'=to_jsonb(d.response_bytes)
    AND d.document_evidence->>'trading_date'=d.session_date::text AND d.document_evidence->'adjusted'='false'::jsonb AND d.document_evidence->'include_otc'='false'::jsonb
    AND d.document_evidence->>'hash_representation'='EXACT_UTF8_APPLICATION_RESPONSE_BODY' AND d.license_policy='EXISTING_MASSIVE_SUBSCRIPTION_API_RESPONSE_INTERNAL_USE'
    AND d.calendar_scope_id='corrected_company_scope_8e6ca1178f03822c' AND d.input_versions->>'calendar_manifest_sha256'=d.calendar_manifest_sha256
    AND d.input_versions->>'scope_sha256'='8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd' AND d.input_versions->>'source_contract_revision'='massive-daily-42-v2' AND d.acquire_link_pass,false) AS source_integrity_pass,
   coalesce(jsonb_typeof(d.body)='object' AND d.body->>'status'='OK' AND d.body->'adjusted'='false'::jsonb AND jsonb_typeof(d.body->'results')='array' AND nullif(d.body->>'next_url','') IS NULL,false) AS envelope_shape_pass,
   CASE WHEN jsonb_typeof(d.body->'results')='array' THEN jsonb_array_length(d.body->'results') ELSE NULL END AS source_array_count,
   coalesce(d.normalized_at IS NOT NULL AND d.source_verified_at IS NOT NULL AND d.normalized_source_records=d.result_count AND d.verified_source_records=d.result_count,false) AS producer_complete
  FROM day_source d
 ), raw AS MATERIALIZED (
  SELECT d.source_day_id,d.session_date,e.ordinality::integer AS source_row,e.value AS rec
  FROM envelope d CROSS JOIN LATERAL jsonb_array_elements(CASE WHEN d.envelope_shape_pass AND d.source_array_count BETWEEN 0 AND 50000 THEN d.body->'results' ELSE '[]'::jsonb END) WITH ORDINALITY e
 ), numeric_fields AS MATERIALIZED (
  SELECT r.*,
   CASE WHEN jsonb_typeof(r.rec->'T')='string' AND length(r.rec->>'T') BETWEEN 1 AND 64 THEN r.rec->>'T' END AS source_ticker,
   CASE WHEN jsonb_typeof(r.rec->'t')='number' AND length((r.rec->'t')::text)<=96 THEN (r.rec->>'t')::numeric END AS provider_time_ms,
   CASE WHEN jsonb_typeof(r.rec->'o')='number' AND length((r.rec->'o')::text)<=96 THEN (r.rec->>'o')::numeric END AS provider_open,
   CASE WHEN jsonb_typeof(r.rec->'h')='number' AND length((r.rec->'h')::text)<=96 THEN (r.rec->>'h')::numeric END AS provider_high,
   CASE WHEN jsonb_typeof(r.rec->'l')='number' AND length((r.rec->'l')::text)<=96 THEN (r.rec->>'l')::numeric END AS provider_low,
   CASE WHEN jsonb_typeof(r.rec->'c')='number' AND length((r.rec->'c')::text)<=96 THEN (r.rec->>'c')::numeric END AS provider_close,
   CASE WHEN jsonb_typeof(r.rec->'v')='number' AND length((r.rec->'v')::text)<=96 THEN (r.rec->>'v')::numeric END AS share_volume,
   CASE WHEN jsonb_typeof(r.rec->'n')='number' AND length((r.rec->'n')::text)<=96 THEN (r.rec->>'n')::numeric END AS trade_count,
   CASE WHEN jsonb_typeof(r.rec->'vw')='number' AND length((r.rec->'vw')::text)<=96 THEN (r.rec->>'vw')::numeric END AS provider_vwap
  FROM raw r
 ), dated AS MATERIALIZED (
  SELECT n.*,CASE WHEN n.provider_time_ms BETWEEN 0 AND 4102444800000 AND n.provider_time_ms=trunc(n.provider_time_ms)
   THEN ((timestamptz '1970-01-01 00:00:00+00'+(n.provider_time_ms::bigint/1000)*interval '1 second'+(n.provider_time_ms::bigint%1000)*interval '1 millisecond') AT TIME ZONE 'America/New_York')::date END AS provider_ny_date
  FROM numeric_fields n
 ), expected AS MATERIALIZED (
  SELECT d.source_day_id,d.source_row,d.source_ticker,d.provider_time_ms,d.provider_ny_date,d.provider_open,d.provider_high,d.provider_low,d.provider_close,d.share_volume,d.trade_count,d.provider_vwap,
   ((CASE WHEN d.rec->>'T' IS NULL OR d.rec->>'T'='' THEN 1 ELSE 0 END)+
    (SELECT coalesce(sum(f.bit),0)::integer FROM (VALUES('t',2),('o',4),('h',8),('l',16),('c',32),('v',64),('n',128),('vw',256)) f(key,bit) WHERE d.rec->>f.key IS NULL)) AS missing_mask,
   ((CASE WHEN d.rec->>'T' IS NOT NULL AND d.rec->>'T'<>'' AND (d.source_ticker IS NULL OR d.source_ticker~'[[:cntrl:][:space:]]') THEN 1 ELSE 0 END)+
    (CASE WHEN d.rec->>'t' IS NOT NULL AND (d.provider_time_ms IS NULL OR d.provider_ny_date IS NULL) THEN 2 ELSE 0 END)+
    (SELECT coalesce(sum(f.bit),0)::integer FROM (VALUES('o',4,d.provider_open),('h',8,d.provider_high),('l',16,d.provider_low),('c',32,d.provider_close),('vw',256,d.provider_vwap)) f(key,bit,val) WHERE d.rec->>f.key IS NOT NULL AND (f.val IS NULL OR f.val<=0))+
    (CASE WHEN d.rec->>'v' IS NOT NULL AND (d.share_volume IS NULL OR d.share_volume<0) THEN 64 ELSE 0 END)+
    (CASE WHEN d.rec->>'n' IS NOT NULL AND (d.trade_count IS NULL OR d.trade_count<0 OR d.trade_count<>trunc(d.trade_count)) THEN 128 ELSE 0 END)) AS invalid_mask,
   ((CASE WHEN least(d.provider_open,d.provider_high,d.provider_low,d.provider_close)>0 AND d.provider_open IS NOT NULL AND d.provider_high IS NOT NULL AND d.provider_low IS NOT NULL AND d.provider_close IS NOT NULL
     AND (d.provider_low>d.provider_high OR d.provider_open NOT BETWEEN d.provider_low AND d.provider_high OR d.provider_close NOT BETWEEN d.provider_low AND d.provider_high) THEN 1 ELSE 0 END)+
    (CASE WHEN d.provider_ny_date IS NOT NULL AND d.provider_ny_date<>d.session_date THEN 2 ELSE 0 END)) AS structure_mask,
   sha256(convert_to(d.rec::text,'UTF8')) AS raw_record_sha256
  FROM dated d
 ), typed AS MATERIALIZED (
  SELECT b.*,b.xmin::text AS typed_row_version FROM population_top3000_202608_202609_v1.d02_daily_bar_v2 b
  WHERE b.source_day_id=(SELECT source_day_id FROM day_source)
 ), expected_rows AS MATERIALIZED (
  SELECT e.source_row,jsonb_build_array(e.source_row,e.source_ticker,e.provider_time_ms,e.provider_ny_date,e.provider_open,e.provider_high,e.provider_low,e.provider_close,e.share_volume,e.trade_count,e.provider_vwap,e.missing_mask,e.invalid_mask,e.structure_mask,encode(e.raw_record_sha256,'hex')) AS row_value FROM expected e
 ), typed_rows AS MATERIALIZED (
  SELECT b.source_row,b.typed_row_version,jsonb_build_array(b.source_row,b.source_ticker,b.provider_time_ms,b.provider_ny_date,b.provider_open,b.provider_high,b.provider_low,b.provider_close,b.share_volume,b.trade_count,b.provider_vwap,b.missing_mask,b.invalid_mask,b.structure_mask,encode(b.raw_record_sha256,'hex')) AS row_value FROM typed b
 ), compared AS MATERIALIZED (
  SELECT coalesce(e.source_row,b.source_row) AS source_row,e.source_row IS NULL AS extra_typed,b.source_row IS NULL AS missing_typed,
   e.row_value IS DISTINCT FROM b.row_value AS differs,e.row_value AS expected_row,b.row_value AS typed_row
  FROM expected_rows e FULL JOIN typed_rows b USING(source_row)
 ), metrics AS MATERIALIZED (
  SELECT count(*) AS compared_units,count(*) FILTER(WHERE extra_typed) AS extra_typed,count(*) FILTER(WHERE missing_typed) AS missing_typed,
   count(*) FILTER(WHERE NOT extra_typed AND NOT missing_typed AND differs) AS exact_typed_mismatches FROM compared
 ), fingerprints AS MATERIALIZED (
  SELECT (SELECT count(*) FROM expected) AS source_occurrences,(SELECT count(*) FROM typed) AS typed_occurrences,
   (SELECT encode(sha256(convert_to(coalesce(string_agg(source_row::text||':'||encode(sha256(convert_to(row_value::text,'UTF8')),'hex'),E'\n' ORDER BY source_row),''),'UTF8')),'hex') FROM expected_rows) AS expected_typed_manifest_sha256,
   (SELECT encode(sha256(convert_to(coalesce(string_agg(source_row::text||':'||encode(sha256(convert_to(row_value::text,'UTF8')),'hex'),E'\n' ORDER BY source_row),''),'UTF8')),'hex') FROM typed_rows) AS actual_typed_manifest_sha256,
   (SELECT encode(sha256(convert_to(coalesce(string_agg(source_row::text||':'||typed_row_version,E'\n' ORDER BY source_row),''),'UTF8')),'hex') FROM typed_rows) AS typed_row_version_manifest_sha256
 ), envelope_checks AS MATERIALIZED (
  SELECT d.*,
   coalesce(d.envelope_shape_pass AND d.source_array_count=d.result_count AND d.result_count BETWEEN 0 AND 50000
    AND NOT EXISTS(SELECT 1 FROM raw WHERE jsonb_typeof(rec) IS DISTINCT FROM 'object')
    AND NOT EXISTS(SELECT 1 FROM (VALUES('queryCount'),('resultsCount')) f(key) WHERE d.body ? f.key AND
      CASE WHEN jsonb_typeof(d.body->f.key) IS DISTINCT FROM 'number' THEN true ELSE ((d.body->>f.key)::numeric<0 OR (d.body->>f.key)::numeric>9999999999 OR (d.body->>f.key)::numeric<>trunc((d.body->>f.key)::numeric) OR (f.key='resultsCount' AND (d.body->>f.key)::numeric<>d.source_array_count)) END),false) AS source_envelope_pass
  FROM envelope d
 ), final AS MATERIALIZED (
  SELECT d.*,q.session_date AS requested_session_date,m.compared_units,m.extra_typed,m.missing_typed,m.exact_typed_mismatches,f.source_occurrences,f.typed_occurrences,f.expected_typed_manifest_sha256,f.actual_typed_manifest_sha256,f.typed_row_version_manifest_sha256
  FROM requested q LEFT JOIN envelope_checks d USING(session_date) CROSS JOIN metrics m CROSS JOIN fingerprints f
 )
SELECT jsonb_build_object('verifier_version','D02_INDEPENDENT_SOURCE_TO_TYPED_20261023_V1','observed_at',clock_timestamp(),'session_date',f.requested_session_date,
 'status',CASE WHEN f.source_day_id IS NULL THEN 'NO_RETAINED_SOURCE' WHEN NOT f.producer_complete THEN 'SOURCE_PROCESSING_NOT_COMPLETE' WHEN NOT f.source_integrity_pass THEN 'SOURCE_INTEGRITY_FAILED' WHEN NOT f.source_envelope_pass THEN 'SOURCE_ENVELOPE_FAILED' WHEN f.extra_typed<>0 OR f.missing_typed<>0 OR f.exact_typed_mismatches<>0 OR f.expected_typed_manifest_sha256 IS DISTINCT FROM f.actual_typed_manifest_sha256 THEN 'SOURCE_TO_TYPED_MISMATCH' ELSE 'SOURCE_TO_TYPED_RECONCILED' END,
 'manifest',jsonb_build_object('source_day_id',f.source_day_id,'archive_key',f.archive_key,'source_id',f.source_id,'source_sha256',f.source_sha256,'calculated_source_sha256',f.calculated_source_sha256,'source_bytes',f.response_bytes,'calculated_source_bytes',f.calculated_source_bytes,'result_count',f.result_count,'source_array_count',f.source_array_count,'source_row_version',f.source_row_version,'document_row_version',f.document_row_version,'calendar_scope_id',f.calendar_scope_id,'calendar_manifest_sha256',f.calendar_manifest_sha256,'input_versions',f.input_versions,'retrieved_at',f.retrieved_at,'acquisition_mode',f.acquisition_mode),
 'checks',jsonb_build_object('source_integrity_pass',coalesce(f.source_integrity_pass,false),'source_envelope_pass',coalesce(f.source_envelope_pass,false),'producer_complete',coalesce(f.producer_complete,false),'source_occurrences',f.source_occurrences,'typed_occurrences',f.typed_occurrences,'compared_units',f.compared_units,'missing_typed',f.missing_typed,'extra_typed',f.extra_typed,'exact_typed_mismatches',f.exact_typed_mismatches,'expected_typed_manifest_sha256',f.expected_typed_manifest_sha256,'actual_typed_manifest_sha256',f.actual_typed_manifest_sha256,'typed_row_version_manifest_sha256',f.typed_row_version_manifest_sha256),
 'source_semantics',(SELECT jsonb_build_object('rows_with_missing_fields',count(*) FILTER(WHERE e.missing_mask<>0),'rows_with_invalid_fields',count(*) FILTER(WHERE e.invalid_mask<>0),'rows_with_structure_flags',count(*) FILTER(WHERE e.structure_mask<>0),'explicit_zero_volume_rows',count(*) FILTER(WHERE e.share_volume=0),'missing_volume_rows',count(*) FILTER(WHERE (e.missing_mask&64)<>0),'invalid_volume_rows',count(*) FILTER(WHERE (e.invalid_mask&64)<>0),'explicit_zero_trade_count_rows',count(*) FILTER(WHERE e.trade_count=0),'missing_trade_count_rows',count(*) FILTER(WHERE (e.missing_mask&128)<>0),'missing_vwap_rows',count(*) FILTER(WHERE (e.missing_mask&256)<>0),'source_min_ny_date',min(e.provider_ny_date),'source_max_ny_date',max(e.provider_ny_date),'empty_response_is_not_zero_volume',count(*)=0) FROM expected e),
 'field_mismatch_counts',(SELECT coalesce(jsonb_object_agg(x.field,x.mismatches),'{}'::jsonb) FROM (SELECT v.field,count(*) FILTER(WHERE c.expected_row->v.idx IS DISTINCT FROM c.typed_row->v.idx) AS mismatches FROM compared c CROSS JOIN (VALUES(0,'source_row'),(1,'ticker'),(2,'provider_time_ms'),(3,'provider_ny_date'),(4,'open'),(5,'high'),(6,'low'),(7,'close'),(8,'volume'),(9,'trade_count'),(10,'vwap'),(11,'missing_mask'),(12,'invalid_mask'),(13,'structure_mask'),(14,'raw_record_sha256')) v(idx,field) WHERE NOT c.extra_typed AND NOT c.missing_typed GROUP BY v.field) x),
 'mismatch_examples',(SELECT coalesce(jsonb_agg(to_jsonb(x) ORDER BY source_row),'[]'::jsonb) FROM (SELECT source_row,extra_typed,missing_typed,expected_row,typed_row FROM compared WHERE differs ORDER BY source_row LIMIT 10) x),
 'production_parser_invoked',false,'independent_provider_accuracy_verified',false,'company_population_verified',false,'company_day_completeness',NULL,'original_historical_receipt_proven',false,'price_adjustment','UNADJUSTED_PROVIDER_SOURCE','aggregate_window_position','UNVERIFIED_NOT_PUBLICATION_TIME') AS d02_independent_date_verification
FROM final f;
COMMIT;
