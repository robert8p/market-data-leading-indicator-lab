-- Root read-only global before/after manifest. Exactly 42 expected dates; no typed-bar scan.
-- Source/proof row versions detect changes outside per-date verification snapshots.
BEGIN READ ONLY;SET LOCAL statement_timeout='10s';SET LOCAL lock_timeout='1s';SET LOCAL timezone='UTC';
WITH expected_dates(session_date) AS (VALUES (DATE '2026-08-03'),(DATE '2026-08-04'),(DATE '2026-08-05'),(DATE '2026-08-06'),(DATE '2026-08-07'),(DATE '2026-08-10'),(DATE '2026-08-11'),(DATE '2026-08-12'),(DATE '2026-08-13'),(DATE '2026-08-14'),(DATE '2026-08-17'),(DATE '2026-08-18'),(DATE '2026-08-19'),(DATE '2026-08-20'),(DATE '2026-08-21'),(DATE '2026-08-24'),(DATE '2026-08-25'),(DATE '2026-08-26'),(DATE '2026-08-27'),(DATE '2026-08-28'),(DATE '2026-08-31'),(DATE '2026-09-01'),(DATE '2026-09-02'),(DATE '2026-09-03'),(DATE '2026-09-04'),(DATE '2026-09-08'),(DATE '2026-09-09'),(DATE '2026-09-10'),(DATE '2026-09-11'),(DATE '2026-09-14'),(DATE '2026-09-15'),(DATE '2026-09-16'),(DATE '2026-09-17'),(DATE '2026-09-18'),(DATE '2026-09-21'),(DATE '2026-09-22'),(DATE '2026-09-23'),(DATE '2026-09-24'),(DATE '2026-09-25'),(DATE '2026-09-28'),(DATE '2026-09-29'),(DATE '2026-09-30')),
 calendar_rows AS MATERIALIZED(
  SELECT c.*,c.xmin::text AS calendar_row_version,
   coalesce(c.qualification='PROVIDER_AND_NYSE_RULES_RECONCILED' AND c.source_record_sha256=encode(sha256(convert_to(c.source_record_text,'UTF8')),'hex') AND c.source_receipt_sha256=encode(sha256(convert_to(c.source_receipt_text,'UTF8')),'hex') AND c.regular_open_utc<c.regular_close_utc AND (c.regular_open_utc AT TIME ZONE 'America/New_York')::date=c.session_date AND (c.regular_close_utc AT TIME ZONE 'America/New_York')::date=c.session_date,false) AS calendar_row_pass
  FROM population_top3000_202608_202609_v1.catalog_calendar c WHERE c.scope_id='corrected_company_scope_8e6ca1178f03822c' AND c.session_date BETWEEN date '2026-08-03' AND date '2026-09-30'
 ),calendar_summary AS MATERIALIZED(
  SELECT count(*) AS rows,coalesce(bool_and(calendar_row_pass),false) AS all_rows_valid,
   encode(sha256(convert_to(coalesce(string_agg(session_date::text,E'\n' ORDER BY session_date),''),'UTF8')),'hex') AS calendar_date_list_sha256,
   encode(sha256(convert_to(coalesce(jsonb_agg(jsonb_build_object('date',session_date,'open',regular_open_utc,'close',regular_close_utc,'record_sha256',source_record_sha256,'receipt_sha256',source_receipt_sha256) ORDER BY session_date),'[]'::jsonb)::text,'UTF8')),'hex') AS calendar_manifest_sha256,
   encode(sha256(convert_to(coalesce(jsonb_agg(jsonb_build_object('date',session_date,'xmin',calendar_row_version,'record',source_record_sha256,'receipt',source_receipt_sha256,'qualified',qualification) ORDER BY session_date),'[]'::jsonb)::text,'UTF8')),'hex') AS calendar_row_version_manifest_sha256
  FROM calendar_rows
 ),source_rows AS MATERIALIZED(
  SELECT d.session_date,d.source_day_id,d.source_id,d.source_sha256,d.response_bytes,d.result_count,d.calendar_manifest_sha256,d.source_verified_at,d.normalized_source_records,d.verified_source_records,
   jsonb_build_object('source',to_jsonb(d),'source_xmin',d.xmin::text,'document_metadata',CASE WHEN s.source_id IS NULL THEN NULL ELSE jsonb_build_object('source_id',s.source_id,'sha256',s.source_sha256,'bytes',s.content_bytes,'uri',s.source_uri,'provider',s.source_provider,'retrieved_at',s.retrieved_at,'source_published_at',s.source_published_at,'historical_received_at',s.historical_received_at,'evidence',s.evidence,'license_policy',s.license_policy,'content_present',s.content_text IS NOT NULL,'document_xmin',s.xmin::text) END) AS source_metadata
  FROM population_top3000_202608_202609_v1.d02_daily_source_v2 d LEFT JOIN population_top3000_202608_202609_v1.source_document s USING(source_id)
 ),proofs AS MATERIALIZED(
  SELECT d.session_date,count(*) AS batches,coalesce(sum(b.source_rows),0) AS proof_source_rows,
   coalesce(bool_and(b.verified_at IS NOT NULL),false) AS all_batches_verified,
   encode(sha256(convert_to(jsonb_agg(to_jsonb(b)||jsonb_build_object('proof_xmin',b.xmin::text) ORDER BY b.first_source_row)::text,'UTF8')),'hex') AS proof_manifest_sha256
  FROM population_top3000_202608_202609_v1.d02_daily_batch_v2 b JOIN source_rows d USING(source_day_id) GROUP BY d.session_date
 ),manifest_rows AS MATERIALIZED(
  SELECT d.*,jsonb_build_object('session_date',d.session_date,'source_metadata',d.source_metadata,'batches',coalesce(p.batches,0),'proof_source_rows',coalesce(p.proof_source_rows,0),'all_batches_verified',CASE WHEN d.result_count=0 THEN p.batches IS NULL ELSE coalesce(p.all_batches_verified,false) END,'proof_manifest_sha256',p.proof_manifest_sha256) AS manifest_row
  FROM source_rows d LEFT JOIN proofs p USING(session_date)
 ),snapshot AS MATERIALIZED(
  SELECT jsonb_build_object('calendar',to_jsonb(c),'source_dates',coalesce((SELECT jsonb_agg(m.manifest_row ORDER BY m.session_date) FROM manifest_rows m),'[]'::jsonb)) AS value FROM calendar_summary c
 )
SELECT jsonb_build_object('verifier_version','D02_INDEPENDENT_42_DATE_MANIFEST_20261023_V1','observed_at',clock_timestamp(),'expected_dates',(SELECT jsonb_agg(session_date ORDER BY session_date) FROM expected_dates),'expected_date_count',42,
 'calendar',(SELECT to_jsonb(c)||jsonb_build_object('date_list_matches_frozen',c.calendar_date_list_sha256='188cc82e93e2c3446d9fba86cec682b996913997f56a2263d3fd680475998368' AND c.rows=42) FROM calendar_summary c),
 'observed_source_date_count',(SELECT count(*) FROM source_rows),'producer_completed_date_count',(SELECT count(*) FROM source_rows WHERE source_verified_at IS NOT NULL AND normalized_source_records=result_count AND verified_source_records=result_count),
 'missing_source_dates',(SELECT coalesce(jsonb_agg(e.session_date ORDER BY e.session_date),'[]'::jsonb) FROM expected_dates e LEFT JOIN source_rows d USING(session_date) WHERE d.source_day_id IS NULL),
 'unexpected_source_dates',(SELECT coalesce(jsonb_agg(d.session_date ORDER BY d.session_date),'[]'::jsonb) FROM source_rows d LEFT JOIN expected_dates e USING(session_date) WHERE e.session_date IS NULL),
 'missing_calendar_dates',(SELECT coalesce(jsonb_agg(e.session_date ORDER BY e.session_date),'[]'::jsonb) FROM expected_dates e LEFT JOIN calendar_rows c USING(session_date) WHERE c.session_date IS NULL),
 'unexpected_calendar_dates',(SELECT coalesce(jsonb_agg(c.session_date ORDER BY c.session_date),'[]'::jsonb) FROM calendar_rows c LEFT JOIN expected_dates e USING(session_date) WHERE e.session_date IS NULL),
 'source_calendar_hash_mismatches',(SELECT count(*) FROM source_rows d CROSS JOIN calendar_summary c WHERE d.calendar_manifest_sha256 IS DISTINCT FROM c.calendar_manifest_sha256),
 'snapshot_manifest_sha256',(SELECT encode(sha256(convert_to(value::text,'UTF8')),'hex') FROM snapshot),
 'date_manifests',(SELECT coalesce(jsonb_agg(jsonb_build_object('session_date',m.session_date,'source_day_id',m.source_day_id,'source_id',m.source_id,'source_sha256',m.source_sha256,'source_bytes',m.response_bytes,'result_count',m.result_count,'metadata_and_proof_sha256',encode(sha256(convert_to(m.manifest_row::text,'UTF8')),'hex')) ORDER BY m.session_date),'[]'::jsonb) FROM manifest_rows m),
 'mutating_stage_tasks_not_complete',(SELECT count(*) FROM population_top3000_202608_202609_v1.pipeline_task t WHERE t.family_id='D02' AND t.payload->>'kind'='MASSIVE_GROUPED_DAILY_V2' AND t.stage IN('ACQUIRE','NORMALIZE','DERIVE') AND t.status<>'COMPLETE'),
 'source_dates_only_not_company_coverage',true,'independent_provider_accuracy_verified',false,'company_population_verified',false) AS d02_independent_manifest;
COMMIT;
