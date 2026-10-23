-- ROOT ONLY, READ ONLY. No production parser, normalizer, calendar or context helper is called.
BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY;
SET LOCAL statement_timeout='10s';
SET LOCAL lock_timeout='500ms';
SET LOCAL timezone='UTC';
SET LOCAL datestyle='ISO, YMD';
SET LOCAL work_mem='16MB';
WITH preparation_parts AS MATERIALIZED (
 SELECT v.kind,v.value,
 CASE WHEN pg_input_is_valid(v.value->>'observed_at','timestamp with time zone')
 THEN (v.value->>'observed_at')::timestamptz END observed_at,
 encode(sha256(convert_to((v.value-'observed_at')::text,'UTF8')),'hex') content_manifest_sha256
 FROM (VALUES
 ('staging',current_setting('top3000.s02_partitioned_plan_staging')::jsonb),
 ('canonical',current_setting('top3000.s02_partitioned_plan_canonical')::jsonb),
 ('observed',current_setting('top3000.s02_partitioned_plan_observed')::jsonb),
 ('expected',current_setting('top3000.s02_partitioned_plan_expected')::jsonb)
 ) v(kind,value)
), preparation_guard AS MATERIALIZED (
 SELECT CASE WHEN count(*)=4 AND count(DISTINCT kind)=4 AND bool_and((
  jsonb_typeof(value)='object'
  AND value->>'check_revision'='S02_READBACK_'||upper(kind)||'_PART_V3'
  AND value->>'archive_key'='massive-archive:S02:c1ed3ed6aa5bad110f122c2459ba27f6daa14a7ac484363d4915c0fc9ddb1f88'
  AND observed_at IS NOT NULL AND observed_at<=clock_timestamp()
  AND observed_at>=(SELECT max(t.completed_at) FROM population_top3000_202608_202609_v1.pipeline_task t
    WHERE t.family_id='S02' AND t.partition_key='short-interest-settlement-20260715-20260930' AND t.stage IN('NORMALIZE','DERIVE') AND t.status='COMPLETE')
  AND value->'company_completeness_verified'='false'::jsonb
  AND value->'independent_provider_accuracy_verified'='false'::jsonb
  AND NOT EXISTS(SELECT 1 FROM unnest(CASE kind
    WHEN 'staging' THEN ARRAY['source_occurrences','represented_occurrences']
    WHEN 'canonical' THEN ARRAY['canonical_versions']
    ELSE ARRAY['canonical_versions','versions','unexpected','missing'] END) count_field(name)
   WHERE jsonb_typeof(value->count_field.name) IS DISTINCT FROM 'number'
    OR coalesce(value->>count_field.name,'')!~'^(0|[1-9][0-9]*)$'
    OR NOT coalesce(pg_input_is_valid(value->>count_field.name,'bigint'),false))
  AND (kind NOT IN('observed','expected') OR (
    value->'source_version_membership_checked'='true'::jsonb
    AND value->'canonical_versions'=current_setting('top3000.s02_partitioned_plan_canonical')::jsonb->'canonical_versions'
    AND coalesce(value->>'manifest_sha256','')~'^[0-9a-f]{64}$'))
  AND (kind NOT IN('staging','canonical') OR jsonb_typeof(value->'pages')='array')
 ) IS TRUE) THEN 0 ELSE 1 END mismatch,
 jsonb_object_agg(kind,content_manifest_sha256 ORDER BY kind) parts_manifest,
 min(observed_at) earliest_at,max(observed_at) latest_at
 FROM preparation_parts
), helper_definitions AS MATERIALIZED (
 SELECT fn,pg_get_functiondef(('population_top3000_202608_202609_v1.'||fn)::regprocedure) definition
 FROM (VALUES
 ('massive_shortinterest_archive_key_v1(text,text,jsonb)'),('massive_shortinterest_context_v1(uuid)'),
 ('massive_shortinterest_acquire_v1(uuid,text)'),('massive_shortinterest_claim_v1(uuid,text)'),
 ('massive_shortinterest_resource_guard_v1()'),('massive_shortinterest_commit_v1(uuid,text,jsonb)'),
 ('massive_shortinterest_failure_v1(uuid,text,jsonb)'),('massive_shortinterest_date_v1(text)'),
 ('massive_shortinterest_numeric_v1(jsonb)'),('massive_shortinterest_calendar_v1()'),
 ('massive_shortinterest_normalize_v1(uuid,text)'),('massive_shortinterest_derive_v1(uuid,text)'),
 ('massive_shortinterest_repair_hold_v1(uuid,text)'),('massive_shortinterest_bundle_sha256_v1()')) h(fn)
), helper_map AS MATERIALIZED (
 SELECT jsonb_object_agg(fn,encode(sha256(convert_to(definition,'UTF8')),'hex')) function_sha256
 FROM helper_definitions
), helper_hashes AS MATERIALIZED (
 SELECT function_sha256,encode(sha256(convert_to(function_sha256::text,'UTF8')),'hex') bundle_sha256
 FROM helper_map
), literal_calendar(settlement_date,scheduled_publication_date,time_role) AS (
 VALUES(date '2026-07-15',date '2026-07-24','SUPPORT'::text),
 (date '2026-07-31',date '2026-08-11','SUPPORT'),
 (date '2026-08-14',date '2026-08-25','TARGET'),
 (date '2026-08-31',date '2026-09-10','TARGET'),
 (date '2026-09-15',date '2026-09-24','TARGET'),
 (date '2026-09-30',date '2026-10-09','TARGET')
), acquires AS MATERIALIZED (
 SELECT t.task_id,t.partition_key,t.input_versions,t.status,
 'massive-archive:S02:'||encode(sha256(convert_to(jsonb_build_object('family','S02','partition',t.partition_key,'input_versions',t.input_versions)::text,'UTF8')),'hex') archive_key
 FROM population_top3000_202608_202609_v1.pipeline_task t
 WHERE t.family_id='S02' AND t.stage='ACQUIRE' AND t.partition_key='short-interest-settlement-20260715-20260930'
 AND t.input_versions->>'scope_sha256'='8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd'
 AND t.input_versions->>'archive_contract_revision'='massive-shortinterest-20261023-v1'
 AND coalesce(t.input_versions->>'fixture_only','false')='false'
), page_meta_base AS MATERIALIZED (
 SELECT p.archive_key,p.family_id,p.partition_key,p.page_number,p.acquire_task_id,p.attempt_id,
 p.source_id,p.source_sha256,p.request_url,p.next_url,p.wire_sha256,p.wire_bytes,p.result_count,
 p.retrieved_at,p.normalized_at,p.normalized_source_records,p.xmin::text page_row_xmin,
 d.source_id document_source_id,d.source_sha256 document_sha256,d.content_bytes document_bytes,
 d.source_uri document_uri,d.source_provider document_provider,d.retrieved_at document_retrieved_at,
 d.source_published_at document_published_at,d.historical_received_at document_historical_received_at,
 d.license_policy,d.xmin::text document_row_xmin,
 encode(sha256(convert_to(d.evidence::text,'UTF8')),'hex') document_evidence_sha256,
 pa.task_id attempt_task_id,pa.owner_fence,pa.stop_generation,pa.started_at attempt_started_at,
 pa.ended_at attempt_ended_at,pa.outcome attempt_outcome,pa.adapter_sha256,
 pl.provider lease_provider,pl.reserved_bytes,pl.received_bytes,pl.released_at,pl.outcome lease_outcome,
 pc.commit_id,pc.attempt_id commit_attempt_id,pc.task_id commit_task_id,pc.output_sha256 committed_source_sha256,
 pc.receipt source_commit_receipt,pc.new_committed_units,pc.xmin::text commit_row_xmin
 FROM population_top3000_202608_202609_v1.massive_shortinterest_page_v1 p
 LEFT JOIN population_top3000_202608_202609_v1.source_document d ON d.source_id=p.source_id
 LEFT JOIN population_top3000_202608_202609_v1.pipeline_attempt pa ON pa.attempt_id=p.attempt_id
 LEFT JOIN population_top3000_202608_202609_v1.pipeline_provider_lease pl ON pl.attempt_id=p.attempt_id
 LEFT JOIN population_top3000_202608_202609_v1.pipeline_commit pc
 ON pc.commit_key='massive-page:'||p.source_id||':'||p.source_sha256
 WHERE p.archive_key='massive-archive:S02:c1ed3ed6aa5bad110f122c2459ba27f6daa14a7ac484363d4915c0fc9ddb1f88'
), page_meta AS MATERIALIZED (
 SELECT p.*,encode(sha256(convert_to(to_jsonb(p)::text,'UTF8')),'hex') source_snapshot_sha256
 FROM page_meta_base p
), staging_counts AS MATERIALIZED (
 SELECT * FROM jsonb_to_recordset(current_setting('top3000.s02_partitioned_plan_staging')::jsonb->'pages')
 AS x(page_number integer,staged_occurrences bigint,first_row integer,last_row integer,
 staged_links_manifest_sha256 text,staged_row_versions_sha256 text)
), canonical_counts AS MATERIALIZED (
 SELECT * FROM jsonb_to_recordset(current_setting('top3000.s02_partitioned_plan_canonical')::jsonb->'pages')
 AS x(page_number integer,canonical_versions bigint)
), pages AS MATERIALIZED (
 SELECT p.*,lag(p.next_url) OVER(ORDER BY p.page_number) prior_next_url,
 coalesce(s.staged_occurrences,0) staged_occurrences,s.first_row,s.last_row,
 coalesce(s.staged_links_manifest_sha256,encode(sha256(convert_to('','UTF8')),'hex')) staged_links_manifest_sha256,
 s.staged_row_versions_sha256,coalesce(c.canonical_versions,0) canonical_versions
 FROM page_meta p LEFT JOIN staging_counts s USING(page_number) LEFT JOIN canonical_counts c USING(page_number)
), partition_state AS MATERIALIZED (
 SELECT family_id,partition_key,time_role,denominator_state,denominator_evidence,
 producer_revision,immutable_input_versions,publication_eligible
 FROM population_top3000_202608_202609_v1.pipeline_partition
 WHERE family_id='S02' AND partition_key='short-interest-settlement-20260715-20260930'
), global_counts AS MATERIALIZED (
 SELECT jsonb_build_object(
 'acquire_revision_or_helper_bundle',CASE WHEN (SELECT count(*) FROM acquires)=1
 AND NOT EXISTS(SELECT 1 FROM acquires a CROSS JOIN helper_hashes h WHERE a.status<>'COMPLETE'
 OR a.archive_key<>'massive-archive:S02:c1ed3ed6aa5bad110f122c2459ba27f6daa14a7ac484363d4915c0fc9ddb1f88' OR a.input_versions->>'shortinterest_bundle_sha256' IS DISTINCT FROM h.bundle_sha256) THEN 0 ELSE 1 END,
 'normalization_or_derivation_not_complete',(SELECT count(*) FROM acquires a CROSS JOIN (VALUES('NORMALIZE'),('DERIVE')) s(stage)
 WHERE NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task t WHERE t.family_id='S02'
 AND t.partition_key=a.partition_key AND t.stage=s.stage AND t.input_versions=a.input_versions AND t.status='COMPLETE')),
 'source_page_or_occurrence_count',CASE WHEN (SELECT count(*) FROM pages)=135 AND (SELECT sum(result_count) FROM pages)=134931
 AND (current_setting('top3000.s02_partitioned_plan_staging')::jsonb->>'source_occurrences')::bigint=134931 THEN 0 ELSE 1 END,
 'page_sequence_or_terminal',CASE WHEN (SELECT min(page_number) FROM pages)=1 AND (SELECT max(page_number) FROM pages)=135
 AND (SELECT count(*) FROM pages WHERE next_url IS NULL)=1 AND (SELECT max(page_number) FROM pages WHERE next_url IS NULL)=135 THEN 0 ELSE 1 END,
 'page_cursor_or_task',(SELECT count(*) FROM pages p WHERE p.family_id IS DISTINCT FROM 'S02'
 OR p.partition_key IS DISTINCT FROM 'short-interest-settlement-20260715-20260930'
 OR NOT EXISTS(SELECT 1 FROM acquires a WHERE a.task_id=p.acquire_task_id AND a.archive_key=p.archive_key)
 OR (p.page_number=1 AND p.request_url IS DISTINCT FROM 'https://api.massive.com/stocks/v1/short-interest?settlement_date.gte=2026-07-15&settlement_date.lte=2026-09-30&sort=settlement_date.asc,ticker.asc&limit=1000')
 OR (p.page_number>1 AND p.request_url IS DISTINCT FROM p.prior_next_url)
 OR (p.next_url IS NOT NULL AND p.next_url!~'^https://api[.]massive[.]com/stocks/v1/short-interest[?]cursor=[-A-Za-z0-9_.~%+]+$')),
 'source_metadata',(SELECT count(*) FROM pages p CROSS JOIN helper_hashes h WHERE p.document_source_id IS NULL OR p.source_sha256 IS DISTINCT FROM p.document_sha256
 OR p.document_uri IS DISTINCT FROM p.request_url OR p.document_provider IS DISTINCT FROM 'Massive'
 OR p.document_retrieved_at IS DISTINCT FROM p.retrieved_at OR p.document_published_at IS NOT NULL OR p.document_historical_received_at IS NOT NULL
 OR p.license_policy IS DISTINCT FROM 'EXISTING_MASSIVE_SUBSCRIPTION_API_RESPONSE_INTERNAL_USE'
 OR p.attempt_task_id IS DISTINCT FROM p.acquire_task_id OR p.owner_fence IS DISTINCT FROM 2 OR p.stop_generation IS DISTINCT FROM 0
 OR p.lease_provider IS DISTINCT FROM 'Massive' OR p.reserved_bytes IS DISTINCT FROM 8388608 OR p.released_at IS NULL
 OR p.received_bytes IS DISTINCT FROM p.document_bytes OR p.attempt_ended_at IS NULL
 OR p.attempt_outcome IS DISTINCT FROM (CASE WHEN p.next_url IS NULL THEN 'COMPLETE' ELSE 'CONTINUE' END)
 OR p.lease_outcome IS DISTINCT FROM 'RECEIVED'
 OR p.adapter_sha256 IS DISTINCT FROM h.function_sha256->>'massive_shortinterest_acquire_v1(uuid,text)'
 OR p.commit_id IS NULL OR p.commit_attempt_id IS DISTINCT FROM p.attempt_id OR p.commit_task_id IS DISTINCT FROM p.acquire_task_id
 OR p.committed_source_sha256 IS DISTINCT FROM p.source_sha256 OR p.new_committed_units IS DISTINCT FROM 1
 OR p.source_commit_receipt IS DISTINCT FROM jsonb_build_object('commit_key','massive-page:'||p.source_id||':'||p.source_sha256,
 'output_sha256',p.source_sha256,'new_committed_units',1,'immutable_output_ids',jsonb_build_array(p.source_id),
 'input_versions',(SELECT a.input_versions FROM acquires a WHERE a.task_id=p.acquire_task_id),
 'raw_source_records',p.result_count,'terminal_page',p.next_url IS NULL,'cohort_certified',false)),
 'staged_page_count_or_range',(SELECT count(*) FROM pages WHERE staged_occurrences<>result_count OR normalized_at IS NULL
 OR normalized_source_records IS DISTINCT FROM result_count OR (result_count>0 AND (first_row<>1 OR last_row<>result_count))),
 'staged_occurrence_without_page',((current_setting('top3000.s02_partitioned_plan_staging')::jsonb->>'source_occurrences')::bigint-(current_setting('top3000.s02_partitioned_plan_staging')::jsonb->>'represented_occurrences')::bigint),
 'observation_missing',(current_setting('top3000.s02_partitioned_plan_observed')::jsonb->>'missing')::bigint,
 'observation_unexpected',(current_setting('top3000.s02_partitioned_plan_observed')::jsonb->>'unexpected')::bigint,
 'expectation_missing',(current_setting('top3000.s02_partitioned_plan_expected')::jsonb->>'missing')::bigint,
 'expectation_unexpected',(current_setting('top3000.s02_partitioned_plan_expected')::jsonb->>'unexpected')::bigint,
 'partition_denominator_or_publication',CASE WHEN (SELECT count(*) FROM partition_state)=1 AND NOT EXISTS(
 SELECT 1 FROM partition_state p CROSS JOIN acquires a WHERE p.time_role IS DISTINCT FROM 'TARGET'
 OR p.denominator_state IS DISTINCT FROM 'PROVISIONAL' OR p.publication_eligible IS DISTINCT FROM false
 OR p.producer_revision IS DISTINCT FROM 'massive-shortinterest-20261023-v1' OR p.immutable_input_versions IS DISTINCT FROM a.input_versions
 OR p.denominator_evidence IS DISTINCT FROM jsonb_build_object('archive_key',a.archive_key,'provider_page_chain_terminal',true,
 'independent_company_report_denominator',false,'july_support_rows_retained',true,
 'report_calendar',(SELECT jsonb_agg(to_jsonb(c) ORDER BY c.settlement_date) FROM literal_calendar c),
 'scheduled_dates_are_not_exact_publication_times',true)) THEN 0 ELSE 1 END
 ) || jsonb_build_object('preparation_parts_binding',(SELECT mismatch FROM preparation_guard)) mismatches
), plan_receipt AS MATERIALIZED (
 SELECT clock_timestamp() observed_at,'S02_BOUNDED_READBACK_PLAN_V1' check_revision,
 'PLAN_ONLY_REQUIRES_ALL135_PAGE_CHECKS' result,
 encode(sha256(convert_to(jsonb_build_object(
 'helpers',(SELECT to_jsonb(h) FROM helper_hashes h),
 'acquires',(SELECT jsonb_agg(to_jsonb(a) ORDER BY task_id) FROM acquires a),
 'pages',(SELECT jsonb_agg(to_jsonb(p) ORDER BY page_number) FROM pages p),
 'observed_rows',current_setting('top3000.s02_partitioned_plan_observed')::jsonb->>'manifest_sha256',
 'expected_rows',current_setting('top3000.s02_partitioned_plan_expected')::jsonb->>'manifest_sha256',
 'partition',(SELECT to_jsonb(p) FROM partition_state p))::text,'UTF8')),'hex') plan_sha256,
 (SELECT count(*) FROM pages) source_pages,(current_setting('top3000.s02_partitioned_plan_staging')::jsonb->>'source_occurrences')::bigint source_occurrences,
 (current_setting('top3000.s02_partitioned_plan_canonical')::jsonb->>'canonical_versions')::bigint canonical_versions,(current_setting('top3000.s02_partitioned_plan_observed')::jsonb->>'versions')::bigint observed_versions,
 (current_setting('top3000.s02_partitioned_plan_staging')::jsonb->>'source_occurrences')::bigint-(current_setting('top3000.s02_partitioned_plan_canonical')::jsonb->>'canonical_versions')::bigint duplicate_occurrences,
 (SELECT bundle_sha256 FROM helper_hashes) compared_helper_bundle_sha256,
 (SELECT function_sha256->>'massive_shortinterest_normalize_v1(uuid,text)' FROM helper_hashes) compared_normalizer_sha256,
 (SELECT function_sha256->>'massive_shortinterest_derive_v1(uuid,text)' FROM helper_hashes) compared_deriver_sha256,
 (SELECT function_sha256->>'massive_shortinterest_calendar_v1()' FROM helper_hashes) compared_calendar_sha256,
 mismatches global_mismatches,(SELECT jsonb_agg(to_jsonb(p) ORDER BY page_number) FROM pages p) pages_to_verify,
 false independent_provider_accuracy_verified,false company_completeness_verified,
 false historical_availability_certified,false publication_admitted,
 'SEPARATE_BOUNDED_READ_ONLY_PARTS_WITH_FRESH_BEFORE_AFTER_V3'::text preparation_method,
 (SELECT parts_manifest FROM preparation_guard) preparation_parts_manifest,
 (SELECT earliest_at FROM preparation_guard) preparation_parts_earliest_at,
 (SELECT latest_at FROM preparation_guard) preparation_parts_latest_at
 FROM global_counts
)
SELECT * FROM plan_receipt;
COMMIT;
