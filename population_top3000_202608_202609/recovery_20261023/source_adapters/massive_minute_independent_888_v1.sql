-- ROOT READ ONLY: exact AAPL September 8 pilot, one page / 888 rows / 90,747 bytes.
-- No network, persistent DML/DDL, production parser/normalizer/helper call or certification mutation.
-- Eight-second DO statement limit covers both version snapshots and the independent reconstruction.
BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED READ ONLY;
SET LOCAL statement_timeout='8s';
SET LOCAL lock_timeout='500ms';
SET LOCAL search_path='pg_catalog';
SET LOCAL timezone='UTC';
SET LOCAL datestyle='ISO,YMD';
SET LOCAL work_mem='16MB';
DO $independent$
DECLARE v_before jsonb;v_after jsonb;v_check jsonb;v_result jsonb;v_started timestamptz:=clock_timestamp();v_stable boolean;v_pass boolean;
BEGIN

WITH pages AS MATERIALIZED (
 SELECT p.*,p.xmin::text AS row_version FROM population_top3000_202608_202609_v1.massive_minute_page_v1 p
 WHERE p.archive_key='p01-massive-minute-v1:d3ed13031b937c7f84d0017dc5432cdee2b75d73429464e02a7c26c4a4d18b5d' ORDER BY p.page_number LIMIT 2
), bars AS MATERIALIZED (
 SELECT b.*,b.xmin::text AS row_version FROM population_top3000_202608_202609_v1.massive_minute_bar_v1 b
 WHERE b.page_id=(SELECT p.page_id FROM pages p WHERE p.page_number=1) ORDER BY b.source_row LIMIT 889
), proofs AS MATERIALIZED (
 SELECT q.*,q.xmin::text AS row_version FROM population_top3000_202608_202609_v1.massive_minute_batch_v1 q
 WHERE q.page_id=(SELECT p.page_id FROM pages p WHERE p.page_number=1) ORDER BY q.first_source_row LIMIT 2
), doc AS MATERIALIZED (
 SELECT d.source_id,d.source_sha256,d.content_bytes,d.source_uri,d.source_provider,d.retrieved_at,d.source_published_at,d.historical_received_at,d.evidence,d.license_policy,d.xmin::text AS row_version,
 octet_length(d.content_text) AS actual_bytes,
 CASE WHEN octet_length(d.content_text)=90747 THEN encode(sha256(convert_to(d.content_text,'UTF8')),'hex') END AS actual_sha256
 FROM population_top3000_202608_202609_v1.source_document d WHERE d.source_id='p01-massive-minute-v1:d3ed13031b937c7f84d0017dc5432cdee2b75d73429464e02a7c26c4a4d18b5d:page:1'
), tasks AS MATERIALIZED (
 SELECT t.task_id,t.family_id,t.partition_key,t.stage,t.status,t.payload,t.input_versions,t.completed_at,t.xmin::text AS row_version
 FROM population_top3000_202608_202609_v1.pipeline_task t WHERE t.task_id IN('03e5cc13-dc86-4e0a-a88a-841f0d3f723d','44cfda84-d050-4aba-9eac-ffd111e5ee58','65ede012-fa91-43ac-bcb1-c65e7238bc31')
), part AS MATERIALIZED (
 SELECT p.*,p.xmin::text AS row_version FROM population_top3000_202608_202609_v1.pipeline_partition p WHERE p.family_id='P01' AND p.partition_key='massive-minute-AAPL-20260908-v1'
), calendar AS MATERIALIZED (
 SELECT c.*,c.xmin::text AS row_version FROM population_top3000_202608_202609_v1.catalog_calendar c WHERE c.scope_id='corrected_company_scope_8e6ca1178f03822c' AND c.session_date=DATE '2026-09-08'
), acquisition AS MATERIALIZED (
 SELECT a.attempt_id,a.task_id,a.outcome,a.owner_fence,a.stop_generation,a.adapter_sha256,a.started_at,a.ended_at,a.xmin::text AS attempt_version,
 l.provider,l.reserved_bytes,l.received_bytes,l.released_at,l.outcome AS lease_outcome,l.xmin::text AS lease_version
 FROM population_top3000_202608_202609_v1.pipeline_attempt a LEFT JOIN population_top3000_202608_202609_v1.pipeline_provider_lease l USING(attempt_id)
 WHERE a.attempt_id=(SELECT p.attempt_id FROM pages p WHERE p.page_number=1)
), committed AS MATERIALIZED (
 SELECT c.commit_id,c.attempt_id,c.task_id,c.commit_key,c.output_sha256,c.receipt,c.new_committed_units,c.xmin::text AS row_version
 FROM population_top3000_202608_202609_v1.pipeline_commit c WHERE c.commit_key='massive-minute-source:p01-massive-minute-v1:d3ed13031b937c7f84d0017dc5432cdee2b75d73429464e02a7c26c4a4d18b5d:page:1' ORDER BY c.committed_at LIMIT 2
)
SELECT jsonb_build_object(
 'pages',(SELECT coalesce(jsonb_agg(to_jsonb(p) ORDER BY p.page_number),'[]') FROM pages p),
 'source',(SELECT coalesce(jsonb_agg(to_jsonb(d)),'[]') FROM doc d),
 'typed_count_with_one_extra_bound',(SELECT count(*) FROM bars),
 'typed_values_and_versions_sha256',(SELECT encode(sha256(convert_to(coalesce(string_agg(to_jsonb(b)::text,E'\n' ORDER BY b.source_row),''),'UTF8')),'hex') FROM bars b),
 'proofs',(SELECT coalesce(jsonb_agg(to_jsonb(q) ORDER BY q.first_source_row),'[]') FROM proofs q),
 'tasks',(SELECT coalesce(jsonb_agg(to_jsonb(t) ORDER BY t.task_id),'[]') FROM tasks t),
 'partition',(SELECT coalesce(jsonb_agg(to_jsonb(p)),'[]') FROM part p),
 'calendar',(SELECT coalesce(jsonb_agg(to_jsonb(c)),'[]') FROM calendar c),
 'acquisition',(SELECT coalesce(jsonb_agg(to_jsonb(a)),'[]') FROM acquisition a),
 'source_commit',(SELECT coalesce(jsonb_agg(to_jsonb(c) ORDER BY c.commit_id),'[]') FROM committed c)
) INTO v_before;


WITH pages AS MATERIALIZED (
 SELECT p.* FROM population_top3000_202608_202609_v1.massive_minute_page_v1 p WHERE p.archive_key='p01-massive-minute-v1:d3ed13031b937c7f84d0017dc5432cdee2b75d73429464e02a7c26c4a4d18b5d' ORDER BY p.page_number LIMIT 2
), page AS MATERIALIZED (SELECT p.* FROM pages p WHERE p.page_number=1),
 document AS MATERIALIZED (
 SELECT d.*,CASE WHEN octet_length(d.content_text)=90747 THEN d.content_text::jsonb END AS body,
 CASE WHEN octet_length(d.content_text)=90747 THEN encode(sha256(convert_to(d.content_text,'UTF8')),'hex') END AS calculated_sha256
 FROM population_top3000_202608_202609_v1.source_document d WHERE d.source_id='p01-massive-minute-v1:d3ed13031b937c7f84d0017dc5432cdee2b75d73429464e02a7c26c4a4d18b5d:page:1'
), envelope AS MATERIALIZED (
 SELECT d.*,CASE WHEN jsonb_typeof(d.body->'results')='array' THEN jsonb_array_length(d.body->'results') END AS array_count FROM document d
), raw AS MATERIALIZED (
 SELECT e.ordinality::integer AS source_row,e.value AS rec FROM envelope d
 CROSS JOIN LATERAL jsonb_array_elements(CASE WHEN d.array_count=888 THEN d.body->'results' ELSE '[]'::jsonb END) WITH ORDINALITY e
), numbers AS MATERIALIZED (
 SELECT r.*,
 CASE WHEN jsonb_typeof(r.rec->'t')='number' AND length((r.rec->'t')::text)<=96 THEN (r.rec->>'t')::numeric END AS provider_time_ms,
 CASE WHEN jsonb_typeof(r.rec->'o')='number' AND length((r.rec->'o')::text)<=96 THEN (r.rec->>'o')::numeric END AS provider_open,
 CASE WHEN jsonb_typeof(r.rec->'h')='number' AND length((r.rec->'h')::text)<=96 THEN (r.rec->>'h')::numeric END AS provider_high,
 CASE WHEN jsonb_typeof(r.rec->'l')='number' AND length((r.rec->'l')::text)<=96 THEN (r.rec->>'l')::numeric END AS provider_low,
 CASE WHEN jsonb_typeof(r.rec->'c')='number' AND length((r.rec->'c')::text)<=96 THEN (r.rec->>'c')::numeric END AS provider_close,
 CASE WHEN jsonb_typeof(r.rec->'v')='number' AND length((r.rec->'v')::text)<=96 THEN (r.rec->>'v')::numeric END AS share_volume,
 CASE WHEN jsonb_typeof(r.rec->'n')='number' AND length((r.rec->'n')::text)<=96 THEN (r.rec->>'n')::numeric END AS trade_count,
 CASE WHEN jsonb_typeof(r.rec->'vw')='number' AND length((r.rec->'vw')::text)<=96 THEN (r.rec->>'vw')::numeric END AS provider_vwap,
 CASE WHEN jsonb_typeof(r.rec->'otc')='boolean' THEN (r.rec->>'otc')::boolean END AS provider_otc FROM raw r
), dated AS MATERIALIZED (
 SELECT n.*,CASE WHEN n.provider_time_ms BETWEEN 0 AND 4102444800000 AND n.provider_time_ms=trunc(n.provider_time_ms)
 THEN timestamptz '1970-01-01 00:00:00+00'+(n.provider_time_ms::bigint/1000)*interval '1 second'+(n.provider_time_ms::bigint%1000)*interval '1 millisecond' END AS bar_start_at
 FROM numbers n
), expected AS MATERIALIZED (
 SELECT d.source_row,d.provider_time_ms,d.bar_start_at,(d.bar_start_at AT TIME ZONE 'America/New_York')::date AS provider_ny_date,
 d.provider_open,d.provider_high,d.provider_low,d.provider_close,d.share_volume,d.trade_count,d.provider_vwap,d.provider_otc,
 (SELECT coalesce(sum(f.bit),0)::integer FROM (VALUES('t',1),('o',2),('h',4),('l',8),('c',16),('v',32),('n',64),('vw',128),('otc',256)) f(key,bit) WHERE d.rec->>f.key IS NULL) AS missing_mask,
 ((CASE WHEN d.rec->>'t' IS NOT NULL AND (d.provider_time_ms IS NULL OR d.bar_start_at IS NULL) THEN 1 ELSE 0 END)+
  (SELECT coalesce(sum(f.bit),0)::integer FROM (VALUES('o',2,d.provider_open),('h',4,d.provider_high),('l',8,d.provider_low),('c',16,d.provider_close),('vw',128,d.provider_vwap)) f(key,bit,val) WHERE d.rec->>f.key IS NOT NULL AND (f.val IS NULL OR f.val<=0))+
  (CASE WHEN d.rec->>'v' IS NOT NULL AND (d.share_volume IS NULL OR d.share_volume<0) THEN 32 ELSE 0 END)+
  (CASE WHEN d.rec->>'n' IS NOT NULL AND (d.trade_count IS NULL OR d.trade_count<0 OR d.trade_count<>trunc(d.trade_count)) THEN 64 ELSE 0 END)+
  (CASE WHEN d.rec->>'otc' IS NOT NULL AND d.provider_otc IS NULL THEN 256 ELSE 0 END)) AS invalid_mask,
 ((CASE WHEN d.provider_open>0 AND d.provider_high>0 AND d.provider_low>0 AND d.provider_close>0 AND
  (d.provider_low>d.provider_high OR d.provider_open NOT BETWEEN d.provider_low AND d.provider_high OR d.provider_close NOT BETWEEN d.provider_low AND d.provider_high) THEN 1 ELSE 0 END)+
  (CASE WHEN d.provider_time_ms IS NOT NULL AND (d.provider_time_ms<1788840000000 OR d.provider_time_ms>=1788926400000) THEN 2 ELSE 0 END)+
  (CASE WHEN d.provider_time_ms IS NOT NULL AND mod(d.provider_time_ms,60000)<>0 THEN 4 ELSE 0 END)) AS structure_mask,
 sha256(convert_to(d.rec::text,'UTF8')) AS raw_record_sha256 FROM dated d
), typed AS MATERIALIZED (
 SELECT b.* FROM population_top3000_202608_202609_v1.massive_minute_bar_v1 b WHERE b.page_id=(SELECT p.page_id FROM page p) ORDER BY b.source_row LIMIT 889
), expected_rows AS MATERIALIZED (SELECT e.source_row,to_jsonb(e) AS row_value FROM expected e),
 typed_rows AS MATERIALIZED (SELECT b.source_row,to_jsonb(b)-'page_id' AS row_value FROM typed b),
 compared AS MATERIALIZED (
 SELECT coalesce(e.source_row,b.source_row) AS source_row,e.source_row IS NULL AS extra_typed,b.source_row IS NULL AS missing_typed,
 e.row_value IS DISTINCT FROM b.row_value AS differs,e.row_value AS expected_row,b.row_value AS actual_row
 FROM expected_rows e FULL JOIN typed_rows b USING(source_row)
), fingerprints AS MATERIALIZED (
 SELECT (SELECT count(*) FROM expected) AS expected_rows,(SELECT count(*) FROM typed) AS typed_rows,
 (SELECT encode(sha256(convert_to(coalesce(string_agg(encode(sha256(convert_to(e.row_value::text,'UTF8')),'hex'),E'\n' ORDER BY e.source_row),''),'UTF8')),'hex') FROM expected_rows e) AS expected_typed_sha256,
 (SELECT encode(sha256(convert_to(coalesce(string_agg(encode(sha256(convert_to(b.row_value::text,'UTF8')),'hex'),E'\n' ORDER BY b.source_row),''),'UTF8')),'hex') FROM typed_rows b) AS actual_typed_sha256
), chain AS MATERIALIZED (
 SELECT jsonb_build_object('pages',count(*),'records',coalesce(sum(p.result_count),0),'source_manifest_sha256',
 encode(sha256(convert_to(jsonb_agg(jsonb_build_object('page',p.page_number,'source_id',p.source_id,'source_sha256',p.source_sha256,'wire_sha256',p.wire_sha256,'bytes',p.response_bytes,'records',p.result_count,'request_url',p.request_url,'next_url',p.next_url) ORDER BY p.page_number)::text,'UTF8')),'hex')) AS manifest FROM pages p
), tasks AS MATERIALIZED (
 SELECT t.* FROM population_top3000_202608_202609_v1.pipeline_task t WHERE t.task_id IN('03e5cc13-dc86-4e0a-a88a-841f0d3f723d','44cfda84-d050-4aba-9eac-ffd111e5ee58','65ede012-fa91-43ac-bcb1-c65e7238bc31')
), proofs AS MATERIALIZED (
 SELECT q.* FROM population_top3000_202608_202609_v1.massive_minute_batch_v1 q WHERE q.page_id=(SELECT p.page_id FROM page p) ORDER BY q.first_source_row LIMIT 2
), calendar AS MATERIALIZED (
 SELECT c.* FROM population_top3000_202608_202609_v1.catalog_calendar c WHERE c.scope_id='corrected_company_scope_8e6ca1178f03822c' AND c.session_date=DATE '2026-09-08'
), provenance_view AS MATERIALIZED (
 SELECT v.* FROM population_top3000_202608_202609_v1.massive_minute_provisional_v1 v WHERE v.archive_key='p01-massive-minute-v1:d3ed13031b937c7f84d0017dc5432cdee2b75d73429464e02a7c26c4a4d18b5d' AND v.page_number=1 ORDER BY v.source_row LIMIT 889
), semantic_summary AS MATERIALIZED (
 SELECT jsonb_build_object('raw_occurrences',count(*),'rows_with_missing_fields',count(*) FILTER(WHERE e.missing_mask<>0),
 'rows_with_invalid_fields',count(*) FILTER(WHERE e.invalid_mask<>0),'rows_with_structural_flags',count(*) FILTER(WHERE e.structure_mask<>0),
 'outside_window_rows',count(*) FILTER(WHERE (e.structure_mask&2)<>0),'unaligned_timestamp_rows',count(*) FILTER(WHERE (e.structure_mask&4)<>0),
 'missing_or_invalid_timestamp_rows',count(*) FILTER(WHERE (e.missing_mask&1)<>0 OR (e.invalid_mask&1)<>0),
 'explicit_zero_volume_rows',count(*) FILTER(WHERE e.share_volume=0),'missing_volume_rows',count(*) FILTER(WHERE (e.missing_mask&32)<>0),
 'first_provider_bar',min(e.bar_start_at),'last_provider_bar',max(e.bar_start_at)) AS value FROM expected e
), mismatch AS MATERIALIZED (
 SELECT jsonb_build_object(
 'source_page_count',(SELECT count(*)<>1 FROM pages)::integer,
 'exact_page_anchor',(NOT EXISTS(SELECT 1 FROM page p WHERE p.source_id='p01-massive-minute-v1:d3ed13031b937c7f84d0017dc5432cdee2b75d73429464e02a7c26c4a4d18b5d:page:1' AND p.source_sha256='b227d73395a38f11d5838b98235834aeb9138dc666837a00d0c0adbd121f308a' AND p.wire_sha256='b227d73395a38f11d5838b98235834aeb9138dc666837a00d0c0adbd121f308a' AND p.response_bytes=90747 AND p.wire_bytes=90747 AND p.result_count=888 AND p.results_presence='ARRAY' AND p.next_url IS NULL AND p.request_url='https://api.massive.com/v2/aggs/ticker/AAPL/range/1/minute/1788840000000/1788926399999?adjusted=false&sort=asc&limit=50000' AND p.input_versions='{"scope_sha256":"8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd","source_contract_revision":"massive-minute-aapl-20260908-v1","massive_minute_bundle_sha256":"4a84c6d12fa68d6010009d4d6a029332bfa8c5f65e39a086f7b78523a4cd33db"}'::jsonb AND p.acquire_task_id='03e5cc13-dc86-4e0a-a88a-841f0d3f723d'
 AND p.normalized_rows=888 AND p.verified_rows=888 AND p.normalized_at IS NOT NULL AND p.verified_at IS NOT NULL))::integer,
 'source_document_integrity',(NOT EXISTS(SELECT 1 FROM document d JOIN page p USING(source_id) WHERE d.content_bytes=90747 AND octet_length(d.content_text)=90747 AND d.calculated_sha256='b227d73395a38f11d5838b98235834aeb9138dc666837a00d0c0adbd121f308a' AND d.source_sha256='b227d73395a38f11d5838b98235834aeb9138dc666837a00d0c0adbd121f308a' AND d.source_uri='https://api.massive.com/v2/aggs/ticker/AAPL/range/1/minute/1788840000000/1788926399999?adjusted=false&sort=asc&limit=50000' AND d.source_provider='Massive' AND d.retrieved_at=p.retrieved_at AND d.source_published_at IS NULL AND d.historical_received_at IS NULL AND d.license_policy='EXISTING_MASSIVE_SUBSCRIPTION_API_RESPONSE_INTERNAL_USE'
 AND d.evidence->>'wire_sha256'='b227d73395a38f11d5838b98235834aeb9138dc666837a00d0c0adbd121f308a' AND d.evidence->'wire_bytes'='90747'::jsonb AND d.evidence->'stored_content_bytes'='90747'::jsonb AND d.evidence->>'hash_representation'='EXACT_UTF8_RESPONSE_EXCEPT_CREDENTIAL_REDACTED_NEXT_URL'
 AND d.evidence->>'source_symbol'='AAPL' AND d.evidence->>'trading_date'='2026-09-08' AND d.evidence->'window_start_ms'='1788840000000'::jsonb AND d.evidence->'window_end_exclusive_ms'='1788926400000'::jsonb AND d.evidence->'adjusted'='false'::jsonb AND d.evidence->'page_number'='1'::jsonb AND d.evidence->>'results_presence'='ARRAY' AND d.evidence->'result_count'='888'::jsonb
 AND d.evidence->>'provider_contract'='MASSIVE_QUALIFYING_TRADE_MINUTE_AGGREGATES_ALL_ET_SESSIONS' AND d.evidence->'sip_feed_verified'='false'::jsonb AND d.evidence->'company_membership_verified'='false'::jsonb AND d.evidence->'original_historical_receipt'='false'::jsonb))::integer,
 'source_envelope',(NOT EXISTS(SELECT 1 FROM envelope d WHERE jsonb_typeof(d.body)='object' AND d.body->>'status'='OK' AND d.body->>'ticker'='AAPL' AND d.body->'adjusted'='false'::jsonb AND d.array_count=888 AND nullif(d.body->>'next_url','') IS NULL
 AND NOT EXISTS(SELECT 1 FROM (VALUES('queryCount'),('resultsCount')) f(key) WHERE d.body ? f.key AND CASE WHEN jsonb_typeof(d.body->f.key) IS DISTINCT FROM 'number' THEN true ELSE ((d.body->>f.key)::numeric<0 OR (d.body->>f.key)::numeric>9007199254740991 OR (d.body->>f.key)::numeric<>trunc((d.body->>f.key)::numeric) OR (f.key='resultsCount' AND (d.body->>f.key)::numeric<>888)) END)))::integer,
 'source_record_shape',(SELECT count(*) FROM raw r WHERE jsonb_typeof(r.rec) IS DISTINCT FROM 'object'),
 'source_symbol_conflict',(SELECT count(*) FROM raw r WHERE (r.rec ? 'T' AND r.rec->>'T' IS DISTINCT FROM 'AAPL') OR (r.rec ? 'ticker' AND r.rec->>'ticker' IS DISTINCT FROM 'AAPL')),
 'source_or_typed_count',((SELECT expected_rows<>888 OR typed_rows<>888 FROM fingerprints))::integer,
 'missing_typed',(SELECT count(*) FROM compared c WHERE c.missing_typed),
 'unexpected_typed',(SELECT count(*) FROM compared c WHERE c.extra_typed),
 'typed_field_or_mask',(SELECT count(*) FROM compared c WHERE NOT c.missing_typed AND NOT c.extra_typed AND c.differs),
 'typed_manifest',((SELECT expected_typed_sha256 IS DISTINCT FROM actual_typed_sha256 FROM fingerprints))::integer,
 'batch_proof',(NOT EXISTS(SELECT 1 FROM proofs q CROSS JOIN fingerprints f WHERE q.first_source_row=1 AND q.last_source_row=888 AND q.source_rows=888 AND q.typed_manifest_sha256=f.expected_typed_sha256 AND q.typed_manifest_sha256=f.actual_typed_sha256 AND q.parser_sha256='ffca8c7abf732006415a3c602f5f1895f547b56b797427152343c252afa77a37' AND q.normalizer_sha256='8ceb1e7fd44a29b6942d5ad242724b78ea8979571f0da45cb77998e3a5e69ba6' AND q.verifier_sha256='94b2e1ecb71a4856f5b8a751fd8adb37c06157463b17380901881421a83d24c7' AND q.verified_at IS NOT NULL AND (SELECT count(*) FROM proofs)=1))::integer,
 'completed_data_stages',(NOT EXISTS(SELECT 1 FROM tasks HAVING count(*)=3 AND count(DISTINCT stage)=3 AND bool_and(coalesce(family_id='P01' AND partition_key='massive-minute-AAPL-20260908-v1' AND stage IN('ACQUIRE','NORMALIZE','DERIVE') AND status='COMPLETE' AND completed_at IS NOT NULL AND input_versions='{"scope_sha256":"8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd","source_contract_revision":"massive-minute-aapl-20260908-v1","massive_minute_bundle_sha256":"4a84c6d12fa68d6010009d4d6a029332bfa8c5f65e39a086f7b78523a4cd33db"}'::jsonb AND payload->>'kind'='MASSIVE_MINUTE_PILOT_V1' AND payload->>'source_symbol'='AAPL' AND payload->>'trading_date'='2026-09-08',false))))::integer,
 'acquisition_receipt',(NOT EXISTS(SELECT 1 FROM page p JOIN population_top3000_202608_202609_v1.pipeline_attempt a ON a.attempt_id=p.attempt_id JOIN population_top3000_202608_202609_v1.pipeline_provider_lease l ON l.attempt_id=a.attempt_id JOIN population_top3000_202608_202609_v1.pipeline_commit c ON c.attempt_id=a.attempt_id AND c.commit_key='massive-minute-source:p01-massive-minute-v1:d3ed13031b937c7f84d0017dc5432cdee2b75d73429464e02a7c26c4a4d18b5d:page:1'
 WHERE a.task_id=p.acquire_task_id AND a.outcome='COMPLETE' AND a.ended_at IS NOT NULL AND a.owner_fence=2 AND a.stop_generation=0 AND l.provider='Massive' AND l.reserved_bytes=8388608 AND l.received_bytes=90747 AND l.released_at IS NOT NULL AND l.outcome='RECEIVED' AND c.task_id=p.acquire_task_id AND c.output_sha256='b227d73395a38f11d5838b98235834aeb9138dc666837a00d0c0adbd121f308a' AND c.receipt->'input_versions'=p.input_versions AND c.receipt->'source_records'='888'::jsonb AND c.receipt->'source_chain_terminal'='true'::jsonb))::integer,
 'chain_or_archive',(NOT EXISTS(SELECT 1 FROM chain c CROSS JOIN page p WHERE c.manifest='{"pages":1,"records":888,"source_manifest_sha256":"6851d00f54aeea900fe6e031d51b4fa3c72d643edde9b6d39a370ec04f56c9f4"}'::jsonb
 AND p.archive_key='p01-massive-minute-v1:'||encode(sha256(convert_to(jsonb_build_object('family','P01','partition','massive-minute-AAPL-20260908-v1','input_versions',p.input_versions)::text,'UTF8')),'hex')))::integer,
 'provisional_partition',(NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_partition q CROSS JOIN chain c WHERE q.family_id='P01' AND q.partition_key='massive-minute-AAPL-20260908-v1' AND q.time_role='TARGET' AND q.denominator_state='PROVISIONAL' AND q.producer_revision='massive-minute-aapl-20260908-v1' AND q.immutable_input_versions='{"scope_sha256":"8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd","source_contract_revision":"massive-minute-aapl-20260908-v1","massive_minute_bundle_sha256":"4a84c6d12fa68d6010009d4d6a029332bfa8c5f65e39a086f7b78523a4cd33db"}'::jsonb AND NOT q.publication_eligible AND q.expected_units IS NULL
 AND q.denominator_evidence=jsonb_build_object('basis','PROVISIONAL_MASSIVE_MINUTE_SOURCE_OCCURRENCES_ONLY','archive_key','p01-massive-minute-v1:d3ed13031b937c7f84d0017dc5432cdee2b75d73429464e02a7c26c4a4d18b5d','source_symbol','AAPL','source_date','2026-09-08','source_manifest',c.manifest,'company_membership_verified',false,'expected_printed_bars',NULL,'dense_clock_denominator_used',false,'missing_minutes_filled',0,'sip_feed_verified',false,'original_historical_receipt',false,'source_window_start_is_not_publication_time',true)))::integer,
 'calendar_link',(NOT EXISTS(SELECT 1 FROM calendar c WHERE c.regular_open_utc=timestamptz '2026-09-08 13:30:00+00' AND c.regular_close_utc=timestamptz '2026-09-08 20:00:00+00' AND c.regular_scheduled_minutes=390 AND c.source_timezone='America/New_York' AND c.source_record_sha256=encode(sha256(convert_to(c.source_record_text,'UTF8')),'hex') AND c.source_receipt_sha256=encode(sha256(convert_to(c.source_receipt_text,'UTF8')),'hex')))::integer,
 'summary_values',(NOT EXISTS(SELECT 1 FROM page p CROSS JOIN semantic_summary s CROSS JOIN chain c WHERE p.source_summary @> s.value AND p.source_summary->'source_manifest'=c.manifest AND p.source_summary->'source_records_reconciled'='true'::jsonb AND p.source_summary->'company_population_verified'='false'::jsonb AND p.source_summary->'independent_price_accuracy_verified'='false'::jsonb AND p.source_summary->'sip_feed_verified'='false'::jsonb AND p.source_summary->'historical_receipt_proven'='false'::jsonb AND p.source_summary->'expected_printed_bars'='null'::jsonb AND p.source_summary->'missing_minutes_filled'='0'::jsonb))::integer,
 'view_count',((SELECT count(*)<>888 FROM provenance_view))::integer,
 'view_link_or_false_qualification',(SELECT count(*) FROM provenance_view v LEFT JOIN expected e USING(source_row) WHERE e.source_row IS NULL OR v.source_provider IS DISTINCT FROM 'Massive' OR v.source_symbol IS DISTINCT FROM 'AAPL' OR v.source_date IS DISTINCT FROM DATE '2026-09-08' OR v.source_id IS DISTINCT FROM 'p01-massive-minute-v1:d3ed13031b937c7f84d0017dc5432cdee2b75d73429464e02a7c26c4a4d18b5d:page:1' OR v.source_sha256 IS DISTINCT FROM 'b227d73395a38f11d5838b98235834aeb9138dc666837a00d0c0adbd121f308a' OR v.raw_record_sha256 IS DISTINCT FROM encode(e.raw_record_sha256,'hex') OR v.bar_start_at IS DISTINCT FROM e.bar_start_at OR v.provider_ny_date IS DISTINCT FROM e.provider_ny_date
 OR v.timestamp_in_requested_window IS DISTINCT FROM ((e.missing_mask&1)=0 AND (e.invalid_mask&1)=0 AND (e.structure_mask&6)=0) OR v.required_bar_fields_structurally_usable IS DISTINCT FROM ((e.missing_mask&63)=0 AND (e.invalid_mask&511)=0 AND e.structure_mask=0)
 OR v.company_id IS NOT NULL OR v.security_id IS NOT NULL OR v.source_published_at IS NOT NULL OR v.historical_received_at IS NOT NULL OR v.company_population_verified IS DISTINCT FROM false OR v.independent_price_accuracy_verified IS DISTINCT FROM false OR v.sip_feed_verified IS DISTINCT FROM false OR v.publication_eligible IS DISTINCT FROM false OR v.timestamp_role IS DISTINCT FROM 'AGGREGATE_WINDOW_START_NOT_PUBLICATION_TIME')
 ) AS counts
)
SELECT jsonb_build_object('mismatches',m.counts,'fingerprints',to_jsonb(f),'source_semantics',s.value,
 'session_counts',(SELECT jsonb_object_agg(x.session_label,x.source_rows) FROM (
  SELECT CASE WHEN e.provider_ny_date IS DISTINCT FROM DATE '2026-09-08' THEN 'OUTSIDE_TARGET_DATE'
   WHEN e.bar_start_at>=timestamptz '2026-09-08 08:00:00+00' AND e.bar_start_at<timestamptz '2026-09-08 13:30:00+00' THEN 'PREMARKET_04_00_TO_09_30_ET'
   WHEN e.bar_start_at>=timestamptz '2026-09-08 13:30:00+00' AND e.bar_start_at<timestamptz '2026-09-08 20:00:00+00' THEN 'REGULAR_09_30_TO_16_00_ET'
   WHEN e.bar_start_at>=timestamptz '2026-09-08 20:00:00+00' AND e.bar_start_at<timestamptz '2026-09-09 00:00:00+00' THEN 'POSTMARKET_16_00_TO_20_00_ET' ELSE 'OTHER_NY_DAY_TIME' END AS session_label,count(*) AS source_rows FROM expected e GROUP BY 1) x),
 'field_mismatch_counts',(SELECT jsonb_object_agg(x.field,x.mismatches) FROM (
  SELECT v.field,count(*) FILTER(WHERE c.expected_row->v.field IS DISTINCT FROM c.actual_row->v.field) AS mismatches FROM compared c
  CROSS JOIN (VALUES('provider_time_ms'),('bar_start_at'),('provider_ny_date'),('provider_open'),('provider_high'),('provider_low'),('provider_close'),('share_volume'),('trade_count'),('provider_vwap'),('provider_otc'),('missing_mask'),('invalid_mask'),('structure_mask'),('raw_record_sha256')) v(field)
  WHERE NOT c.extra_typed AND NOT c.missing_typed GROUP BY v.field) x),
 'mismatch_examples',(SELECT coalesce(jsonb_agg(to_jsonb(x) ORDER BY x.source_row),'[]') FROM (SELECT * FROM compared WHERE differs ORDER BY source_row LIMIT 5) x))
INTO v_check FROM mismatch m CROSS JOIN fingerprints f CROSS JOIN semantic_summary s;


WITH pages AS MATERIALIZED (
 SELECT p.*,p.xmin::text AS row_version FROM population_top3000_202608_202609_v1.massive_minute_page_v1 p
 WHERE p.archive_key='p01-massive-minute-v1:d3ed13031b937c7f84d0017dc5432cdee2b75d73429464e02a7c26c4a4d18b5d' ORDER BY p.page_number LIMIT 2
), bars AS MATERIALIZED (
 SELECT b.*,b.xmin::text AS row_version FROM population_top3000_202608_202609_v1.massive_minute_bar_v1 b
 WHERE b.page_id=(SELECT p.page_id FROM pages p WHERE p.page_number=1) ORDER BY b.source_row LIMIT 889
), proofs AS MATERIALIZED (
 SELECT q.*,q.xmin::text AS row_version FROM population_top3000_202608_202609_v1.massive_minute_batch_v1 q
 WHERE q.page_id=(SELECT p.page_id FROM pages p WHERE p.page_number=1) ORDER BY q.first_source_row LIMIT 2
), doc AS MATERIALIZED (
 SELECT d.source_id,d.source_sha256,d.content_bytes,d.source_uri,d.source_provider,d.retrieved_at,d.source_published_at,d.historical_received_at,d.evidence,d.license_policy,d.xmin::text AS row_version,
 octet_length(d.content_text) AS actual_bytes,
 CASE WHEN octet_length(d.content_text)=90747 THEN encode(sha256(convert_to(d.content_text,'UTF8')),'hex') END AS actual_sha256
 FROM population_top3000_202608_202609_v1.source_document d WHERE d.source_id='p01-massive-minute-v1:d3ed13031b937c7f84d0017dc5432cdee2b75d73429464e02a7c26c4a4d18b5d:page:1'
), tasks AS MATERIALIZED (
 SELECT t.task_id,t.family_id,t.partition_key,t.stage,t.status,t.payload,t.input_versions,t.completed_at,t.xmin::text AS row_version
 FROM population_top3000_202608_202609_v1.pipeline_task t WHERE t.task_id IN('03e5cc13-dc86-4e0a-a88a-841f0d3f723d','44cfda84-d050-4aba-9eac-ffd111e5ee58','65ede012-fa91-43ac-bcb1-c65e7238bc31')
), part AS MATERIALIZED (
 SELECT p.*,p.xmin::text AS row_version FROM population_top3000_202608_202609_v1.pipeline_partition p WHERE p.family_id='P01' AND p.partition_key='massive-minute-AAPL-20260908-v1'
), calendar AS MATERIALIZED (
 SELECT c.*,c.xmin::text AS row_version FROM population_top3000_202608_202609_v1.catalog_calendar c WHERE c.scope_id='corrected_company_scope_8e6ca1178f03822c' AND c.session_date=DATE '2026-09-08'
), acquisition AS MATERIALIZED (
 SELECT a.attempt_id,a.task_id,a.outcome,a.owner_fence,a.stop_generation,a.adapter_sha256,a.started_at,a.ended_at,a.xmin::text AS attempt_version,
 l.provider,l.reserved_bytes,l.received_bytes,l.released_at,l.outcome AS lease_outcome,l.xmin::text AS lease_version
 FROM population_top3000_202608_202609_v1.pipeline_attempt a LEFT JOIN population_top3000_202608_202609_v1.pipeline_provider_lease l USING(attempt_id)
 WHERE a.attempt_id=(SELECT p.attempt_id FROM pages p WHERE p.page_number=1)
), committed AS MATERIALIZED (
 SELECT c.commit_id,c.attempt_id,c.task_id,c.commit_key,c.output_sha256,c.receipt,c.new_committed_units,c.xmin::text AS row_version
 FROM population_top3000_202608_202609_v1.pipeline_commit c WHERE c.commit_key='massive-minute-source:p01-massive-minute-v1:d3ed13031b937c7f84d0017dc5432cdee2b75d73429464e02a7c26c4a4d18b5d:page:1' ORDER BY c.committed_at LIMIT 2
)
SELECT jsonb_build_object(
 'pages',(SELECT coalesce(jsonb_agg(to_jsonb(p) ORDER BY p.page_number),'[]') FROM pages p),
 'source',(SELECT coalesce(jsonb_agg(to_jsonb(d)),'[]') FROM doc d),
 'typed_count_with_one_extra_bound',(SELECT count(*) FROM bars),
 'typed_values_and_versions_sha256',(SELECT encode(sha256(convert_to(coalesce(string_agg(to_jsonb(b)::text,E'\n' ORDER BY b.source_row),''),'UTF8')),'hex') FROM bars b),
 'proofs',(SELECT coalesce(jsonb_agg(to_jsonb(q) ORDER BY q.first_source_row),'[]') FROM proofs q),
 'tasks',(SELECT coalesce(jsonb_agg(to_jsonb(t) ORDER BY t.task_id),'[]') FROM tasks t),
 'partition',(SELECT coalesce(jsonb_agg(to_jsonb(p)),'[]') FROM part p),
 'calendar',(SELECT coalesce(jsonb_agg(to_jsonb(c)),'[]') FROM calendar c),
 'acquisition',(SELECT coalesce(jsonb_agg(to_jsonb(a)),'[]') FROM acquisition a),
 'source_commit',(SELECT coalesce(jsonb_agg(to_jsonb(c) ORDER BY c.commit_id),'[]') FROM committed c)
) INTO v_after;

v_stable:=v_before IS NOT DISTINCT FROM v_after;
v_pass:=v_stable AND jsonb_typeof(v_check->'mismatches')='object'
 AND (SELECT count(*) FROM jsonb_each(v_check->'mismatches'))=20
 AND NOT EXISTS(SELECT 1 FROM jsonb_each(v_check->'mismatches') x WHERE jsonb_typeof(x.value) IS DISTINCT FROM 'number' OR x.value<>'0'::jsonb)
 AND v_check#>>'{fingerprints,expected_typed_sha256}'~'^[0-9a-f]{64}$'
 AND v_check#>>'{fingerprints,actual_typed_sha256}'~'^[0-9a-f]{64}$'
 AND v_check#>>'{fingerprints,expected_typed_sha256}'=v_check#>>'{fingerprints,actual_typed_sha256}';
v_result:=jsonb_build_object('result',CASE WHEN coalesce(v_pass,false) THEN 'PASS_SOURCE_TRANSFORMATION_ONLY' ELSE 'FAIL_SOURCE_RECONCILIATION' END,
 'verifier_revision','MASSIVE_MINUTE_888_INDEPENDENT_V1','observed_at',clock_timestamp(),'elapsed_ms',extract(epoch FROM clock_timestamp()-v_started)*1000,
 'archive_key','p01-massive-minute-v1:d3ed13031b937c7f84d0017dc5432cdee2b75d73429464e02a7c26c4a4d18b5d','source_id','p01-massive-minute-v1:d3ed13031b937c7f84d0017dc5432cdee2b75d73429464e02a7c26c4a4d18b5d:page:1','source_sha256','b227d73395a38f11d5838b98235834aeb9138dc666837a00d0c0adbd121f308a','source_pages',1,'expected_source_records',888,'expected_source_bytes',90747,
 'before_manifest_sha256',encode(sha256(convert_to(v_before::text,'UTF8')),'hex'),'after_manifest_sha256',encode(sha256(convert_to(v_after::text,'UTF8')),'hex'),
 'source_and_typed_versions_unchanged',v_stable,'checks',v_check,'production_parser_invoked',false,'production_validation_helper_invoked',false,
 'timestamp_method','INTEGER_MILLISECOND_EPOCH_DECOMPOSITION; NO_FLOATING_POINT_EPOCH_CONVERSION',
 'numeric_method','EXACT_POSTGRES_NUMERIC_FROM_RETAINED_JSON; NULL_AND_MISSING_PRESERVED',
 'session_counts_are_observed_source_rows_not_expected_bar_denominators',true,
 'independent_provider_accuracy_verified',false,'company_population_verified',false,'sip_feed_verified',false,'original_wire_bytes_independently_observed',false,
 'historical_receipt_proven',false,'publication_admitted',false,'source_window_timestamp_is_not_publication_time',true,'missing_minutes_filled',0);
PERFORM set_config('top3000.minute_independent_result',v_result::text,true);
END $independent$;
SELECT current_setting('top3000.minute_independent_result')::jsonb AS independent_minute_reconciliation;
COMMIT;
