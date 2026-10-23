-- Metadata only: no raw content is fetched, parsed or hashed. Run before all pages.
-- READ ONLY. No producer parser, normalizer, context or mutation helper is called.
-- Read hashes of producer definitions solely to identify the compared source revision.
BEGIN READ ONLY;
SET LOCAL statement_timeout='10s';
SET LOCAL lock_timeout='500ms';
SET LOCAL timezone='UTC';
WITH helper_definitions AS MATERIALIZED (
 SELECT fn,pg_get_functiondef(('population_top3000_202608_202609_v1.'||fn)::regprocedure) definition
 FROM (VALUES ('massive_news_archive_key_v1(text,text,jsonb)'),('massive_news_context_v1(uuid)'),
  ('massive_news_acquire_v1(uuid,text)'),('massive_news_claim_v1(uuid,text)'),('massive_news_commit_v1(uuid,text,jsonb)'),
  ('massive_news_failure_v1(uuid,text,jsonb)'),('massive_news_timestamp_v1(text)'),('massive_news_resource_guard_v1()'),
  ('massive_news_normalize_v1(uuid,text)'),('massive_news_derive_v1(uuid,text)')) h(fn)
), helper_hashes AS MATERIALIZED (
 SELECT encode(sha256(convert_to(string_agg(fn||':'||definition,E'\n' ORDER BY fn),'UTF8')),'hex') bundle_sha256,
  jsonb_object_agg(fn,encode(sha256(convert_to(definition,'UTF8')),'hex')) function_sha256
 FROM helper_definitions
), acquires AS MATERIALIZED (
 SELECT t.task_id,t.partition_key,t.input_versions,t.status,
  'massive-archive:N01:'||encode(sha256(convert_to(jsonb_build_object('family','N01','partition',t.partition_key,'input_versions',t.input_versions)::text,'UTF8')),'hex') archive_key
 FROM population_top3000_202608_202609_v1.pipeline_task t
 WHERE t.family_id='N01' AND t.stage='ACQUIRE' AND t.partition_key='news-published-202608-202609'
  AND t.input_versions->>'scope_sha256'='8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd'
  AND t.input_versions->>'archive_contract_revision'='massive-news-20261023-v1'
  AND coalesce(t.input_versions->>'fixture_only','false')='false'
), pages AS MATERIALIZED (
 SELECT p.archive_key,p.partition_key,p.page_number,p.source_id,p.request_url,p.next_url,p.source_sha256,
  p.wire_sha256,p.wire_bytes,p.result_count,p.normalized_source_records,p.normalized_at,p.retrieved_at,
  d.source_sha256 document_sha256,d.content_bytes document_bytes,d.source_uri document_uri,d.source_provider document_provider,
  d.retrieved_at document_retrieved_at,d.source_published_at document_published_at,d.historical_received_at document_historical_received_at,
  d.license_policy,encode(sha256(convert_to(d.evidence::text,'UTF8')),'hex') document_evidence_sha256,
  lag(p.next_url) OVER(PARTITION BY p.archive_key ORDER BY p.page_number) prior_next_url
 FROM population_top3000_202608_202609_v1.massive_news_archive_page_v1 p
 JOIN acquires a ON a.task_id=p.acquire_task_id AND a.archive_key=p.archive_key AND a.partition_key=p.partition_key
 LEFT JOIN population_top3000_202608_202609_v1.source_document d ON d.source_id=p.source_id
), links AS MATERIALIZED (
 SELECT l.* FROM population_top3000_202608_202609_v1.massive_news_record_link_v1 l JOIN acquires a USING(archive_key)
), canonical AS MATERIALIZED (
 SELECT DISTINCT ON(archive_key,unit_key) * FROM links ORDER BY archive_key,unit_key,page_number,source_row
), observed_keys AS MATERIALIZED (
 SELECT partition_key,unit_key,record_sha256,
  encode(sha256(convert_to(jsonb_build_array(company_id,security_id,reporting_entity_id,time_role,event_at,effective_date,source_published_at,
   historical_received_at,available_at,ingested_at,availability_policy,vintage_key,source_id,source_record_key,normalization_sha256)::text,'UTF8')),'hex') metadata_sha256
 FROM population_top3000_202608_202609_v1.source_observation
 WHERE release_id='top3000_companies_202608_202609_v1' AND family_id='N01' AND partition_key='news-published-202608-202609'
), expected_keys AS MATERIALIZED (
 SELECT unit_key,encode(sha256(convert_to(to_jsonb(e)::text,'UTF8')),'hex') row_sha256
 FROM population_top3000_202608_202609_v1.expected_observation e
 WHERE release_id='top3000_companies_202608_202609_v1' AND family_id='N01' AND partition_key='news-published-202608-202609'
), observed_check AS MATERIALIZED (
 SELECT c.unit_key IS NULL unexpected,o.unit_key IS NULL missing FROM canonical c FULL JOIN observed_keys o USING(unit_key)
), expected_check AS MATERIALIZED (
 SELECT c.unit_key IS NULL unexpected,e.unit_key IS NULL missing FROM canonical c FULL JOIN expected_keys e USING(unit_key)
), stream_stats AS MATERIALIZED (
 SELECT archive_key,count(*) pages,min(page_number) first_page,max(page_number) last_page,
  count(*) FILTER(WHERE next_url IS NULL) terminal_pages,max(page_number) FILTER(WHERE next_url IS NULL) terminal_page,
  sum(result_count) raw_occurrences,bool_and(normalized_at IS NOT NULL AND normalized_source_records=result_count) normalized
 FROM pages GROUP BY archive_key
), link_counts AS MATERIALIZED (
 SELECT archive_key,page_number,count(*) staged_occurrences,min(source_row) first_row,max(source_row) last_row,
  encode(sha256(convert_to(coalesce(string_agg(jsonb_build_array(archive_key,page_number,source_row,source_id,source_record_key,raw_record_sha256,unit_key)::text,E'\n' ORDER BY source_row),''),'UTF8')),'hex') staged_links_manifest_sha256
 FROM links GROUP BY archive_key,page_number
), canonical_counts AS MATERIALIZED (
 SELECT archive_key,page_number,count(*) canonical_versions FROM canonical GROUP BY archive_key,page_number
), plan AS MATERIALIZED (
 SELECT p.*,coalesce(l.staged_occurrences,0) staged_occurrences,l.first_row,l.last_row,
  coalesce(l.staged_links_manifest_sha256,encode(sha256(convert_to('','UTF8')),'hex')) staged_links_manifest_sha256,
  coalesce(c.canonical_versions,0) canonical_versions
 FROM pages p LEFT JOIN link_counts l USING(archive_key,page_number) LEFT JOIN canonical_counts c USING(archive_key,page_number)
), versions AS MATERIALIZED (
 SELECT archive_key,source_record_key,array_agg(DISTINCT raw_record_sha256 ORDER BY raw_record_sha256) hashes
 FROM links GROUP BY archive_key,source_record_key HAVING count(DISTINCT raw_record_sha256)>1
), conflict_expectation AS MATERIALIZED (
 SELECT v.*,jsonb_build_object('status','UNRESOLVED_PROVIDER_ID_CONTENT_VERSIONS','versions_retained',true,
  'canonical_article_version_chosen',false,'independent_accuracy_verified',false) evidence FROM versions v
), conflict_actual AS MATERIALIZED (
 SELECT v.* FROM population_top3000_202608_202609_v1.massive_news_version_conflict_v1 v JOIN acquires a USING(archive_key)
), conflict_check AS MATERIALIZED (
 SELECT e.source_record_key IS NULL unexpected,v.source_record_key IS NULL missing,
  e.source_record_key IS NOT NULL AND v.source_record_key IS NOT NULL AND (
   v.version_count IS DISTINCT FROM cardinality(e.hashes) OR v.version_sha256 IS DISTINCT FROM e.hashes OR v.evidence IS DISTINCT FROM e.evidence
   OR v.record_sha256 IS DISTINCT FROM encode(sha256(convert_to(jsonb_build_object('archive_key',e.archive_key,'source_record_key',e.source_record_key,'versions',e.hashes,'evidence',e.evidence)::text,'UTF8')),'hex')) mismatch
 FROM conflict_expectation e FULL JOIN conflict_actual v USING(archive_key,source_record_key)
), partition_state AS MATERIALIZED (
 SELECT family_id,partition_key,time_role,denominator_state,denominator_evidence,producer_revision,immutable_input_versions,publication_eligible
 FROM population_top3000_202608_202609_v1.pipeline_partition WHERE family_id='N01' AND partition_key='news-published-202608-202609'
), global_checks AS MATERIALIZED (
 SELECT jsonb_build_object(
  'acquire_revision_or_helper_bundle',CASE WHEN (SELECT count(*) FROM acquires)=1
   AND NOT EXISTS(SELECT 1 FROM acquires CROSS JOIN helper_hashes WHERE status<>'COMPLETE' OR input_versions->>'news_bundle_sha256' IS DISTINCT FROM bundle_sha256)
   AND (SELECT archive_key FROM acquires)='massive-archive:N01:e9d1bdec926cc865b7ae140a6a475726898a9f35fa9b1cd5c41608a6909c2576' THEN 0 ELSE 1 END,
  'source_count_changed',CASE WHEN (SELECT count(*) FROM pages)=11 AND (SELECT count(*) FROM links)=10282 AND (SELECT sum(result_count) FROM pages)=10282 THEN 0 ELSE 1 END,
  'normalization_or_derivation_not_complete',(SELECT count(*) FROM acquires a WHERE EXISTS(SELECT 1 FROM (VALUES('NORMALIZE'),('DERIVE')) s(stage)
   WHERE NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task t WHERE t.family_id='N01' AND t.stage=s.stage AND t.partition_key=a.partition_key AND t.input_versions=a.input_versions AND t.status='COMPLETE'))),
  'page_chain',(SELECT count(*) FROM stream_stats WHERE pages<>11 OR first_page<>1 OR last_page<>pages OR terminal_pages<>1 OR terminal_page<>last_page OR NOT coalesce(normalized,false))
   +(SELECT count(*) FROM pages WHERE (page_number>1 AND request_url IS DISTINCT FROM prior_next_url)
    OR (page_number=1 AND request_url IS DISTINCT FROM 'https://api.massive.com/v2/reference/news?published_utc.gte=2026-08-01T04%3A00%3A00Z&published_utc.lt=2026-10-01T04%3A00%3A00Z&order=asc&sort=published_utc&limit=1000')
    OR (next_url IS NOT NULL AND next_url!~'^https://api[.]massive[.]com/v2/reference/news[?]cursor=[A-Za-z0-9_.~%+\-]+$')),
  'staged_page_count_or_range',(SELECT count(*) FROM plan WHERE staged_occurrences<>result_count OR (result_count>0 AND (first_row<>1 OR last_row<>result_count))),
  'staged_occurrence_without_page',(SELECT count(*) FROM links l LEFT JOIN pages p USING(archive_key,page_number) WHERE p.archive_key IS NULL),
  'observation_missing',(SELECT count(*) FROM observed_check WHERE missing),'observation_unexpected',(SELECT count(*) FROM observed_check WHERE unexpected),
  'expectation_missing',(SELECT count(*) FROM expected_check WHERE missing),'expectation_unexpected',(SELECT count(*) FROM expected_check WHERE unexpected),
  'conflict_receipt_missing',(SELECT count(*) FROM conflict_check WHERE missing),'conflict_receipt_unexpected',(SELECT count(*) FROM conflict_check WHERE unexpected),
  'conflict_receipt_fields_or_hash',(SELECT count(*) FROM conflict_check WHERE mismatch),
  'partition_denominator_or_publication',CASE WHEN (SELECT count(*) FROM partition_state)=1 AND NOT EXISTS(
   SELECT 1 FROM partition_state q CROSS JOIN acquires a WHERE q.time_role IS DISTINCT FROM 'TARGET' OR q.denominator_state IS DISTINCT FROM 'PROVISIONAL'
    OR q.producer_revision IS DISTINCT FROM 'massive-news-20261023-v1' OR q.immutable_input_versions IS DISTINCT FROM a.input_versions OR q.publication_eligible IS DISTINCT FROM false
    OR q.denominator_evidence IS DISTINCT FROM jsonb_build_object('archive_key',a.archive_key,'provider_page_chain_terminal',true,'source_pages',11,
     'independent_company_news_denominator',false,'source_record_versions_only',true,'requires_dated_company_mapping',true,
     'independent_publication_accuracy_unverified',true,'historical_receipt_not_observed',true)) THEN 0 ELSE 1 END
 ) counts
), plan_receipt AS MATERIALIZED (
 SELECT clock_timestamp() observed_at,'N01_BOUNDED_READBACK_PLAN_V1' check_revision,
  'PLAN_ONLY_REQUIRES_ALL_PAGE_CHECKS' result,
  encode(sha256(convert_to(jsonb_build_object(
   'helpers',(SELECT to_jsonb(h) FROM helper_hashes h),'acquires',(SELECT jsonb_agg(to_jsonb(a) ORDER BY partition_key) FROM acquires a),
   'pages',(SELECT jsonb_agg(to_jsonb(p) ORDER BY archive_key,page_number) FROM plan p),
   'observed_key_manifest',(SELECT encode(sha256(convert_to(coalesce(string_agg(jsonb_build_array(unit_key,record_sha256,metadata_sha256)::text,E'\n' ORDER BY unit_key),''),'UTF8')),'hex') FROM observed_keys),
   'expected_key_manifest',(SELECT encode(sha256(convert_to(coalesce(string_agg(jsonb_build_array(unit_key,row_sha256)::text,E'\n' ORDER BY unit_key),''),'UTF8')),'hex') FROM expected_keys),
   'conflicts',(SELECT coalesce(jsonb_agg(to_jsonb(c) ORDER BY archive_key,source_record_key),'[]'::jsonb) FROM conflict_actual c),
   'partition',(SELECT to_jsonb(q) FROM partition_state q)
  )::text,'UTF8')),'hex') plan_sha256,
  (SELECT count(*) FROM pages) source_pages,(SELECT count(*) FROM links) source_occurrences,
  (SELECT count(*) FROM canonical) canonical_versions,(SELECT count(*) FROM observed_keys) observed_versions,
  (SELECT count(*) FROM links)-(SELECT count(*) FROM canonical) duplicate_occurrences,(SELECT count(*) FROM versions) conflicting_provider_ids,
  (SELECT function_sha256->>'massive_news_normalize_v1(uuid,text)' FROM helper_hashes) compared_normalizer_sha256,
  (SELECT function_sha256->>'massive_news_timestamp_v1(text)' FROM helper_hashes) compared_timestamp_helper_sha256,
  counts global_mismatches,(SELECT coalesce(jsonb_agg(to_jsonb(p) ORDER BY p.page_number),'[]'::jsonb) FROM plan p) pages_to_verify,
  false independent_provider_accuracy_verified,false company_completeness_verified,false historical_availability_certified,false publication_admitted
 FROM global_checks
)
SELECT * FROM plan_receipt;
COMMIT;
