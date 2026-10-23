-- Root inserts SET LOCAL top3000.n01_readback_archive = '<archive from plan>';
-- and SET LOCAL top3000.n01_readback_page = '<1..11>' immediately after BEGIN.
-- Missing configuration fails closed. Each execution reads one raw page only.
-- Core pg_input_is_valid validates calendar/range syntax; integer component arithmetic
-- constructs the instant, with excess fractional digits discarded before arithmetic.
-- Original wire bytes were not retained: only the redacted retained UTF8 is rehashed.
-- READ ONLY. No producer parser, normalizer, context or mutation helper is called.
-- Read hashes of producer definitions solely to identify the compared source revision.
BEGIN READ ONLY;
SET LOCAL statement_timeout='10s';
SET LOCAL lock_timeout='500ms';
SET LOCAL timezone='UTC';
WITH cfg AS MATERIALIZED (
 SELECT nullif(current_setting('top3000.n01_readback_archive',true),'') archive_key,
  CASE WHEN current_setting('top3000.n01_readback_page',true)~'^[1-9][0-9]{0,2}$'
   THEN current_setting('top3000.n01_readback_page',true)::integer END page_number,clock_timestamp() started_at
), helper_definitions AS MATERIALIZED (
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
), source_pages AS MATERIALIZED (
 SELECT p.archive_key,p.partition_key,p.page_number,p.source_id,p.request_url,p.next_url,p.source_sha256,p.wire_sha256,p.wire_bytes,
  p.result_count,p.normalized_source_records,p.normalized_at,p.retrieved_at,
  d.content_text::jsonb body,d.source_id document_source_id,d.source_sha256 document_sha256,
  encode(sha256(convert_to(d.content_text,'UTF8')),'hex') recomputed_source_sha256,
  octet_length(d.content_text)::bigint recomputed_bytes,d.content_bytes,d.source_uri,d.source_provider,d.evidence document_evidence,
  d.retrieved_at document_retrieved_at,d.source_published_at document_published_at,d.historical_received_at document_historical_received_at,d.license_policy
 FROM cfg c JOIN population_top3000_202608_202609_v1.massive_news_archive_page_v1 p ON p.archive_key=c.archive_key AND p.page_number=c.page_number
 JOIN acquires a ON a.task_id=p.acquire_task_id AND a.archive_key=p.archive_key AND a.partition_key=p.partition_key
 LEFT JOIN population_top3000_202608_202609_v1.source_document d ON d.source_id=p.source_id
), raw_occurrences AS MATERIALIZED (
 SELECT p.archive_key,p.partition_key,p.page_number,p.source_id,x.ordinality::integer source_row,x.value raw_event,
  encode(sha256(convert_to(x.value::text,'UTF8')),'hex') raw_record_sha256
 FROM source_pages p CROSS JOIN LATERAL jsonb_array_elements(CASE WHEN jsonb_typeof(p.body->'results')='array' THEN p.body->'results' ELSE '[]'::jsonb END)
  WITH ORDINALITY x(value,ordinality)
), raw_ids AS MATERIALIZED (
 SELECT r.*,CASE WHEN jsonb_typeof(raw_event->'id')='string' AND octet_length(raw_event->>'id') BETWEEN 1 AND 512
  THEN raw_event->>'id' ELSE 'unidentified:'||raw_record_sha256 END source_record_key
 FROM raw_occurrences r
), raw_keys AS MATERIALIZED (
 SELECT r.*,'Massive:news:'||encode(sha256(convert_to(source_record_key,'UTF8')),'hex')||':'||raw_record_sha256 unit_key FROM raw_ids r
), staged_rows AS MATERIALIZED (
 SELECT l.* FROM cfg c JOIN population_top3000_202608_202609_v1.massive_news_record_link_v1 l ON l.archive_key=c.archive_key AND l.page_number=c.page_number
), staging_check AS MATERIALIZED (
 SELECT coalesce(s.source_row,r.source_row) source_row,r.source_id IS NULL raw_missing,s.source_id IS NULL staging_missing,
  r.source_id IS NOT NULL AND s.source_id IS NOT NULL AND (s.source_id IS DISTINCT FROM r.source_id OR s.source_record_key IS DISTINCT FROM r.source_record_key
   OR s.unit_key IS DISTINCT FROM r.unit_key OR s.raw_record_sha256 IS DISTINCT FROM r.raw_record_sha256) mismatch
 FROM raw_keys r FULL JOIN staged_rows s USING(archive_key,page_number,source_row)
), page_units AS MATERIALIZED (
 SELECT DISTINCT archive_key,unit_key FROM raw_keys
), canonical_positions AS MATERIALIZED (
 SELECT u.archive_key,u.unit_key,l.page_number,l.source_row,l.source_id
 FROM page_units u CROSS JOIN LATERAL (
  SELECT r.page_number,r.source_row,r.source_id FROM population_top3000_202608_202609_v1.massive_news_record_link_v1 r
  WHERE r.archive_key=u.archive_key AND r.unit_key=u.unit_key ORDER BY r.page_number,r.source_row LIMIT 1
 ) l
), canonical AS MATERIALIZED (
 SELECT r.*,p.retrieved_at canonical_retrieved_at FROM canonical_positions first_position
 JOIN raw_keys r USING(archive_key,unit_key,page_number,source_row,source_id)
 JOIN source_pages p USING(archive_key,page_number,source_id)
), timestamp_inputs AS MATERIALIZED (
 SELECT c.unit_key,c.raw_event->>'published_utc' raw_timestamp FROM canonical c
 UNION ALL
 SELECT fixture_key,raw_timestamp FROM (VALUES
  ('__time_fixture_upper_inside','2026-10-01T03:59:59.9999999Z'),
  ('__time_fixture_lower_outside','2026-08-01T03:59:59.9999999Z'),
  ('__time_fixture_lower_inside','2026-08-01T04:00:00.0000001Z'),
  ('__time_fixture_upper_excluded','2026-10-01T04:00:00.0000001Z'),
  ('__time_fixture_offset','2026-08-01T00:00:00.123456789-04:00'),
  ('__time_fixture_invalid_calendar','2026-02-30T04:00:00Z'),
  ('__time_fixture_no_zone','2026-08-01T04:00:00'),
  ('__time_fixture_leap_second','2026-08-01T03:59:60Z')
 ) f(fixture_key,raw_timestamp)
), timestamp_parts AS MATERIALIZED (
 SELECT unit_key,raw_timestamp,
  regexp_match(raw_timestamp,'^([0-9]{4})-([0-9]{2})-([0-9]{2})[Tt]([01][0-9]|2[0-3]):([0-5][0-9]):([0-5][0-9])([.][0-9]{1,9})?([Zz]|[+-]([01][0-9]|2[0-3]):[0-5][0-9])$') parts
 FROM timestamp_inputs
), timestamp_components AS MATERIALIZED (
 SELECT unit_key,raw_timestamp,parts,
  parts[1]::integer yr,parts[2]::integer mo,parts[3]::integer dy,parts[4]::integer hh,parts[5]::integer mi,parts[6]::integer ss,
  rpad(left(coalesce(substring(parts[7] FROM 2),''),6),6,'0')::integer micros,
  CASE WHEN parts[8] IN('Z','z') THEN 0 ELSE (CASE WHEN left(parts[8],1)='-' THEN -1 ELSE 1 END)
   *(substring(parts[8],2,2)::integer*60+substring(parts[8],5,2)::integer) END offset_minutes,
  CASE WHEN parts IS NOT NULL THEN left(raw_timestamp,19)||'.'||rpad(left(coalesce(substring(parts[7] FROM 2),''),6),6,'0')||parts[8] END truncated_validation_text
 FROM timestamp_parts
), timestamp_values AS MATERIALIZED (
 SELECT unit_key,raw_timestamp,
  CASE WHEN parts IS NOT NULL AND pg_input_is_valid(truncated_validation_text,'timestamp with time zone')
   THEN make_timestamptz(yr,mo,dy,hh,mi,ss,'UTC')+micros*interval '1 microsecond'-offset_minutes*interval '1 minute' END expected_published_at
 FROM timestamp_components
), timestamp_checks AS MATERIALIZED (
 SELECT count(*) FILTER(WHERE actual.expected_published_at IS DISTINCT FROM fixtures.expected_published_at
  OR coalesce(actual.expected_published_at>=timestamptz '2026-08-01 04:00:00+00' AND actual.expected_published_at<timestamptz '2026-10-01 04:00:00+00',false) IS DISTINCT FROM fixtures.expected_in_scope) mismatches
 FROM (VALUES
  ('__time_fixture_upper_inside',timestamptz '2026-10-01 03:59:59.999999+00',true),
  ('__time_fixture_lower_outside',timestamptz '2026-08-01 03:59:59.999999+00',false),
  ('__time_fixture_lower_inside',timestamptz '2026-08-01 04:00:00+00',true),
  ('__time_fixture_upper_excluded',timestamptz '2026-10-01 04:00:00+00',false),
  ('__time_fixture_offset',timestamptz '2026-08-01 04:00:00.123456+00',true),
  ('__time_fixture_invalid_calendar',NULL::timestamptz,false),('__time_fixture_no_zone',NULL::timestamptz,false),
  ('__time_fixture_leap_second',NULL::timestamptz,false)
 ) fixtures(unit_key,expected_published_at,expected_in_scope)
 LEFT JOIN timestamp_values actual USING(unit_key)
), expected_core AS MATERIALIZED (
 SELECT c.*,v.expected_published_at,
  coalesce(v.expected_published_at>=timestamptz '2026-08-01 04:00:00+00' AND v.expected_published_at<timestamptz '2026-10-01 04:00:00+00',false) publication_in_scope,
  (v.expected_published_at AT TIME ZONE 'America/New_York')::date expected_effective_date,
  jsonb_build_object('provider_article_id',raw_event->'id','title',raw_event->'title','article_url',raw_event->'article_url','author',raw_event->'author','published_utc',raw_event->'published_utc',
   'publisher',raw_event->'publisher','tickers',raw_event->'tickers','description',raw_event->'description','keywords',raw_event->'keywords','insights',raw_event->'insights',
   'company_id',NULL,'security_id',NULL,'available_at',canonical_retrieved_at,'raw_event',raw_event) expected_values
 FROM canonical c JOIN timestamp_values v USING(unit_key)
), string_states AS MATERIALIZED (
 SELECT c.unit_key,jsonb_object_agg(f.name,CASE WHEN NOT raw_event ? f.name OR raw_event->f.name='null'::jsonb THEN 'UNAVAILABLE'
  WHEN jsonb_typeof(raw_event->f.name)<>'string' OR btrim(raw_event->>f.name)='' THEN 'QUARANTINED'
  WHEN f.name='article_url' AND (raw_event->>f.name !~ '^https?://[^[:space:]]+$' OR raw_event->>f.name~'^https?://[^/]*@') THEN 'QUARANTINED' ELSE 'VALUE' END) field_states
 FROM canonical c CROSS JOIN (VALUES('title'),('article_url'),('author'),('description')) f(name) GROUP BY c.unit_key
), array_states AS MATERIALIZED (
 SELECT c.unit_key,jsonb_object_agg(f.name,CASE WHEN NOT raw_event ? f.name OR raw_event->f.name='null'::jsonb THEN 'UNAVAILABLE'
  WHEN jsonb_typeof(raw_event->f.name)<>'array' THEN 'QUARANTINED'
  WHEN EXISTS(SELECT 1 FROM jsonb_array_elements(CASE WHEN jsonb_typeof(raw_event->f.name)='array' THEN raw_event->f.name ELSE '[]'::jsonb END) v
   WHERE (f.name IN('tickers','keywords') AND (jsonb_typeof(v)<>'string' OR btrim(v#>>'{}')='')) OR (f.name='insights' AND jsonb_typeof(v)<>'object')) THEN 'QUARANTINED' ELSE 'VALUE' END) field_states
 FROM canonical c CROSS JOIN (VALUES('tickers'),('keywords'),('insights')) f(name) GROUP BY c.unit_key
), expected_rows AS MATERIALIZED (
 SELECT e.*,s.field_states||a.field_states||jsonb_build_object(
   'provider_article_id',CASE WHEN NOT raw_event ? 'id' OR raw_event->'id'='null'::jsonb THEN 'UNAVAILABLE' WHEN jsonb_typeof(raw_event->'id')<>'string' OR octet_length(raw_event->>'id') NOT BETWEEN 1 AND 512 THEN 'QUARANTINED' ELSE 'VALUE' END,
   'published_utc',CASE WHEN NOT raw_event ? 'published_utc' OR raw_event->'published_utc'='null'::jsonb THEN 'UNAVAILABLE' WHEN expected_published_at IS NULL OR NOT publication_in_scope THEN 'QUARANTINED' ELSE 'VALUE' END,
   'publisher',CASE WHEN NOT raw_event ? 'publisher' OR raw_event->'publisher'='null'::jsonb THEN 'UNAVAILABLE' WHEN jsonb_typeof(raw_event->'publisher')<>'object' OR jsonb_typeof(raw_event#>'{publisher,name}') IS DISTINCT FROM 'string' OR btrim(raw_event#>>'{publisher,name}')='' THEN 'QUARANTINED' ELSE 'VALUE' END,
   'company_id','UNAVAILABLE','security_id','UNAVAILABLE','available_at','VALUE') expected_states,
  jsonb_build_object('source_id',source_id,'source_record_key',source_record_key,'raw_record_sha256',raw_record_sha256,'archive_key',archive_key,'source_page',page_number,'source_row',source_row,
   'all_occurrences_relation','massive_news_record_link_v1','company_membership','UNVERIFIED','source_scope','ALL_RETURNED_PROVIDER_NEWS_FOR_FIXED_PUBLICATION_TIME_QUERY',
   'publication_time_basis','PROVIDER_REPORTED_RFC3339; independent publisher timestamp not verified',
   'timestamp_storage_precision','PostgreSQL microseconds; excess fractional digits truncated before timestamp cast; exact provider timestamp retained in raw_event',
   'historical_receipt','NOT_OBSERVED','availability_basis','CURRENT_API_RETRIEVAL_ONLY; not evidence of historical availability',
   'article_bodies_fetched',false,'insights_basis','OPTIONAL_PROVIDER_METADATA_ONLY; no new research or model sentiment',
   'version_conflicts_relation','massive_news_version_conflict_v1','independent_accuracy_verified',false) expected_lineage,
  jsonb_build_object('basis','PROVISIONAL_ENUMERATION_OF_RETURNED_SOURCE_RECORD_VERSIONS','independent_news_census',false,'top3000_company_membership_verified',false,
   'source_record_sha256',raw_record_sha256,'strict_receipt_required',false,'optional_metadata_fields',jsonb_build_array('description','keywords','insights'),
   'current_receipt_is_not_historical_availability',true) expected_applicability
 FROM expected_core e JOIN string_states s USING(unit_key) JOIN array_states a USING(unit_key)
), observed_checks AS MATERIALIZED (
 SELECT e.unit_key,o.unit_key IS NULL missing,
  o.unit_key IS NOT NULL AND o.values_json IS DISTINCT FROM e.expected_values values_mismatch,
  o.unit_key IS NOT NULL AND o.field_state IS DISTINCT FROM e.expected_states states_mismatch,
  o.unit_key IS NOT NULL AND o.lineage IS DISTINCT FROM e.expected_lineage provenance_mismatch,
  o.unit_key IS NOT NULL AND (o.source_id IS DISTINCT FROM e.source_id OR o.source_record_key IS DISTINCT FROM e.source_record_key
   OR o.normalization_sha256 IS DISTINCT FROM h.function_sha256->>'massive_news_normalize_v1(uuid,text)' OR o.vintage_key IS DISTINCT FROM e.archive_key
   OR o.company_id IS NOT NULL OR o.security_id IS NOT NULL OR o.reporting_entity_id IS NOT NULL) identity_or_source_mismatch,
  o.unit_key IS NOT NULL AND (o.time_role IS DISTINCT FROM 'TARGET' OR o.event_at IS DISTINCT FROM e.expected_published_at
   OR o.source_published_at IS DISTINCT FROM e.expected_published_at OR o.effective_date IS DISTINCT FROM e.expected_effective_date
   OR o.available_at IS DISTINCT FROM e.canonical_retrieved_at OR o.historical_received_at IS NOT NULL OR o.availability_policy IS DISTINCT FROM 'ARCHIVED_OBSERVATION') temporal_mismatch,
  o.unit_key IS NOT NULL AND o.record_sha256 IS DISTINCT FROM encode(sha256(convert_to(jsonb_build_object('values',e.expected_values,'states',e.expected_states,'lineage',e.expected_lineage)::text,'UTF8')),'hex') expected_hash_mismatch
 FROM expected_rows e CROSS JOIN helper_hashes h LEFT JOIN LATERAL (
  SELECT o.* FROM population_top3000_202608_202609_v1.source_observation o
  WHERE o.release_id='top3000_companies_202608_202609_v1' AND o.family_id='N01' AND o.partition_key=e.partition_key AND o.unit_key=e.unit_key LIMIT 1
 ) o ON true
), expectation_checks AS MATERIALIZED (
 SELECT e.unit_key,x.unit_key IS NULL missing,x.unit_key IS NOT NULL AND (
  x.company_id IS NOT NULL OR x.security_id IS NOT NULL OR x.reporting_entity_id IS NOT NULL OR x.time_role IS DISTINCT FROM 'TARGET'
  OR x.expectation_source_id IS DISTINCT FROM e.source_id OR x.expectation_record_key IS DISTINCT FROM e.source_record_key
  OR x.required_fields IS DISTINCT FROM ARRAY['provider_article_id','title','article_url','author','published_utc','publisher','tickers','company_id','security_id','available_at']::text[]
  OR x.applicability_evidence IS DISTINCT FROM e.expected_applicability) mismatch
 FROM expected_rows e LEFT JOIN LATERAL (
  SELECT x.* FROM population_top3000_202608_202609_v1.expected_observation x
  WHERE x.release_id='top3000_companies_202608_202609_v1' AND x.family_id='N01' AND x.partition_key=e.partition_key AND x.unit_key=e.unit_key LIMIT 1
 ) x ON true
), checks AS MATERIALIZED (
 SELECT jsonb_build_object(
  'configuration_or_acquire_revision',CASE WHEN (SELECT count(*) FROM acquires)=1 AND (SELECT count(*) FROM source_pages)=1
   AND (SELECT archive_key FROM cfg)='massive-archive:N01:e9d1bdec926cc865b7ae140a6a475726898a9f35fa9b1cd5c41608a6909c2576' AND (SELECT page_number FROM cfg) BETWEEN 1 AND 11
   AND NOT EXISTS(SELECT 1 FROM acquires CROSS JOIN helper_hashes WHERE status<>'COMPLETE' OR input_versions->>'news_bundle_sha256' IS DISTINCT FROM bundle_sha256) THEN 0 ELSE 1 END,
  'source_page_integrity',(SELECT count(*) FROM source_pages WHERE document_source_id IS NULL OR document_sha256 IS DISTINCT FROM source_sha256
   OR recomputed_source_sha256 IS DISTINCT FROM source_sha256 OR recomputed_bytes IS DISTINCT FROM content_bytes OR source_uri IS DISTINCT FROM request_url
   OR source_provider IS DISTINCT FROM 'Massive' OR document_retrieved_at IS DISTINCT FROM retrieved_at OR document_published_at IS NOT NULL OR document_historical_received_at IS NOT NULL
   OR license_policy IS DISTINCT FROM 'EXISTING_MASSIVE_SUBSCRIPTION_API_RESPONSE_INTERNAL_USE'
   OR document_evidence->>'wire_sha256' IS DISTINCT FROM wire_sha256 OR document_evidence->>'wire_bytes' IS DISTINCT FROM wire_bytes::text
   OR document_evidence->>'stored_content_bytes' IS DISTINCT FROM content_bytes::text
   OR document_evidence->>'hash_representation' IS DISTINCT FROM 'EXACT_UTF8_RESPONSE_WITH_TOP_LEVEL_NEXT_URL_CREDENTIAL_REDACTION_ONLY'
   OR document_evidence->'wire_bytes_retained' IS DISTINCT FROM 'false'::jsonb OR document_evidence->'original_historical_receipt' IS DISTINCT FROM 'false'::jsonb
   OR document_evidence->'independent_publication_time_verified' IS DISTINCT FROM 'false'::jsonb OR document_evidence->'article_bodies_fetched' IS DISTINCT FROM 'false'::jsonb
   OR wire_sha256!~'^[0-9a-f]{64}$' OR wire_bytes NOT BETWEEN 1 AND 8388608 OR content_bytes NOT BETWEEN 1 AND 8388608
   OR jsonb_typeof(body) IS DISTINCT FROM 'object' OR jsonb_typeof(body->'results') IS DISTINCT FROM 'array'
   OR (body ? 'status' AND body->>'status' IS DISTINCT FROM 'OK')
   OR result_count IS DISTINCT FROM CASE WHEN jsonb_typeof(body->'results')='array' THEN jsonb_array_length(body->'results') END
   OR (body ? 'count' AND CASE WHEN jsonb_typeof(body->'count')='number' AND body->>'count'~'^[0-9]{1,4}$' THEN (body->>'count')::integer IS DISTINCT FROM result_count ELSE true END)
   OR body->>'next_url' IS DISTINCT FROM next_url OR result_count NOT BETWEEN 0 AND 1000 OR normalized_at IS NULL OR normalized_source_records IS DISTINCT FROM result_count),
  'raw_record_not_object',(SELECT count(*) FROM raw_keys WHERE jsonb_typeof(raw_event) IS DISTINCT FROM 'object'),
  'raw_occurrence_missing',(SELECT count(*) FROM staging_check WHERE raw_missing),'staging_occurrence_missing',(SELECT count(*) FROM staging_check WHERE staging_missing),
  'staging_exact_fields',(SELECT count(*) FROM staging_check WHERE mismatch),
  'canonical_lookup_missing',(SELECT count(*) FROM page_units)-(SELECT count(*) FROM canonical_positions),
  'timestamp_algorithm_boundary_fixtures',(SELECT mismatches FROM timestamp_checks),
  'observation_missing',(SELECT count(*) FROM observed_checks WHERE missing),'exact_values',(SELECT count(*) FROM observed_checks WHERE values_mismatch),
  'field_states',(SELECT count(*) FROM observed_checks WHERE states_mismatch),'first_occurrence_provenance',(SELECT count(*) FROM observed_checks WHERE provenance_mismatch),
  'identity_or_source_metadata',(SELECT count(*) FROM observed_checks WHERE identity_or_source_mismatch),'timestamp_and_availability_semantics',(SELECT count(*) FROM observed_checks WHERE temporal_mismatch),
  'expected_record_hash',(SELECT count(*) FROM observed_checks WHERE expected_hash_mismatch),
  'expectation_missing',(SELECT count(*) FROM expectation_checks WHERE missing),'expectation_evidence',(SELECT count(*) FROM expectation_checks WHERE mismatch)
 ) mismatches
)
SELECT clock_timestamp() observed_at,'N01_RETAINED_SOURCE_PAGE_RECONCILIATION_V1' check_revision,
 CASE WHEN NOT EXISTS(SELECT 1 FROM jsonb_each_text(checks.mismatches) x WHERE x.value::bigint<>0)
  THEN 'PASS_PAGE_SOURCE_TRANSFORMATION_ONLY' ELSE 'FAIL_OR_NOT_READY' END result,
 (SELECT archive_key FROM cfg) archive_key,(SELECT page_number FROM cfg) page_number,
 (SELECT partition_key FROM source_pages) partition_key,(SELECT source_id FROM source_pages) source_id,(SELECT source_sha256 FROM source_pages) source_sha256,
 (SELECT count(*) FROM raw_keys) source_occurrences,(SELECT count(*) FROM canonical) canonical_versions_on_page,
 (SELECT encode(sha256(convert_to(coalesce(string_agg(jsonb_build_array(archive_key,page_number,source_row,source_id,source_record_key,raw_record_sha256,unit_key)::text,E'\n' ORDER BY source_row),''),'UTF8')),'hex') FROM raw_keys) independently_reparsed_links_manifest_sha256,
 (SELECT encode(sha256(convert_to(coalesce(string_agg(jsonb_build_array(unit_key,
  encode(sha256(convert_to(jsonb_build_object('values',expected_values,'states',expected_states,'lineage',expected_lineage)::text,'UTF8')),'hex'))::text,E'\n' ORDER BY unit_key),''),'UTF8')),'hex') FROM expected_rows) independently_rebuilt_canonical_manifest_sha256,
 (SELECT function_sha256->>'massive_news_normalize_v1(uuid,text)' FROM helper_hashes) compared_normalizer_sha256,
 (SELECT function_sha256->>'massive_news_timestamp_v1(text)' FROM helper_hashes) compared_timestamp_helper_sha256,
 checks.mismatches,
 (SELECT coalesce(jsonb_agg(to_jsonb(m)),'[]'::jsonb) FROM (SELECT * FROM observed_checks WHERE missing OR values_mismatch OR states_mismatch OR provenance_mismatch OR identity_or_source_mismatch OR temporal_mismatch OR expected_hash_mismatch ORDER BY unit_key LIMIT 10) m) observation_mismatch_examples,
 (SELECT coalesce(jsonb_agg(to_jsonb(m)),'[]'::jsonb) FROM (SELECT * FROM staging_check WHERE raw_missing OR staging_missing OR mismatch ORDER BY source_row LIMIT 10) m) staging_mismatch_examples,
 (SELECT count(*) FROM expected_core WHERE NOT publication_in_scope) out_of_query_scope_versions,
 (SELECT count(*) FROM expected_core WHERE length(coalesce(substring(raw_event->>'published_utc' FROM '[.]([0-9]+)'),''))>6) truncated_timestamp_versions,
 round(extract(epoch FROM clock_timestamp()-(SELECT started_at FROM cfg))*1000,3) elapsed_ms,
 true all_pages_and_global_plan_still_required,false original_wire_bytes_independently_rehashed,
 false independent_provider_accuracy_verified,false company_completeness_verified,false historical_availability_certified,false publication_admitted
FROM checks;
COMMIT;
