"""Generate root-only, read-only S02 source/canonical reconciliation queries.

No network access, credentials, producer-helper execution, or database writes.
The source population is the retained 135-page archive, not the 3,000-company cohort.
"""
from pathlib import Path
import hashlib
import json

P = Path(__file__).parent
S = "population_top3000_202608_202609_v1"
ARCHIVE = "massive-archive:S02:c1ed3ed6aa5bad110f122c2459ba27f6daa14a7ac484363d4915c0fc9ddb1f88"
PARTITION = "short-interest-settlement-20260715-20260930"
HEADER = """-- ROOT ONLY, READ ONLY. No production parser, normalizer, calendar or context helper is called.
BEGIN READ ONLY;
SET LOCAL statement_timeout='10s';
SET LOCAL lock_timeout='500ms';
SET LOCAL timezone='UTC';
SET LOCAL datestyle='ISO, YMD';
SET LOCAL work_mem='16MB';
"""

COMMON = r"""helper_definitions AS MATERIALIZED (
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
 WHERE p.archive_key='__ARCHIVE__'
), page_meta AS MATERIALIZED (
 SELECT p.*,encode(sha256(convert_to(to_jsonb(p)::text,'UTF8')),'hex') source_snapshot_sha256
 FROM page_meta_base p
)""".replace("__ARCHIVE__", ARCHIVE)

PLAN = COMMON + r""", staged AS MATERIALIZED (
 SELECT r.archive_key,r.page_number,r.source_row,r.source_id,r.source_record_key,r.unit_key,r.raw_record_sha256,r.xmin::text row_xmin
 FROM population_top3000_202608_202609_v1.massive_shortinterest_record_v1 r
 WHERE r.archive_key='__ARCHIVE__'
), canonical AS MATERIALIZED (
 SELECT DISTINCT ON(unit_key) unit_key,page_number,source_row,source_id
 FROM staged ORDER BY unit_key,page_number,source_row
), observed_keys AS MATERIALIZED (
 SELECT o.unit_key,o.record_sha256,o.xmin::text row_xmin
 FROM population_top3000_202608_202609_v1.source_observation o
 WHERE o.release_id='top3000_companies_202608_202609_v1' AND o.family_id='S02'
 AND o.partition_key='short-interest-settlement-20260715-20260930'
), expected_keys AS MATERIALIZED (
 SELECT e.unit_key,e.xmin::text row_xmin
 FROM population_top3000_202608_202609_v1.expected_observation e
 WHERE e.release_id='top3000_companies_202608_202609_v1' AND e.family_id='S02'
 AND e.partition_key='short-interest-settlement-20260715-20260930'
), observed_membership AS MATERIALIZED (
 SELECT c.unit_key IS NULL unexpected,o.unit_key IS NULL missing
 FROM canonical c FULL JOIN observed_keys o USING(unit_key)
), expected_membership AS MATERIALIZED (
 SELECT c.unit_key IS NULL unexpected,e.unit_key IS NULL missing
 FROM canonical c FULL JOIN expected_keys e USING(unit_key)
), staging_counts AS MATERIALIZED (
 SELECT page_number,count(*) staged_occurrences,min(source_row) first_row,max(source_row) last_row,
 encode(sha256(convert_to(coalesce(string_agg(jsonb_build_array(archive_key,page_number,source_row,source_id,source_record_key,unit_key,raw_record_sha256)::text,E'\n' ORDER BY source_row),''),'UTF8')),'hex') staged_links_manifest_sha256,
 encode(sha256(convert_to(coalesce(string_agg(jsonb_build_array(source_row,row_xmin)::text,E'\n' ORDER BY source_row),''),'UTF8')),'hex') staged_row_versions_sha256
 FROM staged GROUP BY page_number
), canonical_counts AS MATERIALIZED (
 SELECT page_number,count(*) canonical_versions FROM canonical GROUP BY page_number
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
 OR a.archive_key<>'__ARCHIVE__' OR a.input_versions->>'shortinterest_bundle_sha256' IS DISTINCT FROM h.bundle_sha256) THEN 0 ELSE 1 END,
 'normalization_or_derivation_not_complete',(SELECT count(*) FROM acquires a CROSS JOIN (VALUES('NORMALIZE'),('DERIVE')) s(stage)
 WHERE NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task t WHERE t.family_id='S02'
 AND t.partition_key=a.partition_key AND t.stage=s.stage AND t.input_versions=a.input_versions AND t.status='COMPLETE')),
 'source_page_or_occurrence_count',CASE WHEN (SELECT count(*) FROM pages)=135 AND (SELECT sum(result_count) FROM pages)=134931
 AND (SELECT count(*) FROM staged)=134931 THEN 0 ELSE 1 END,
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
 'staged_occurrence_without_page',(SELECT count(*) FROM staged s LEFT JOIN pages p USING(page_number) WHERE p.page_number IS NULL),
 'observation_missing',(SELECT count(*) FROM observed_membership WHERE missing),
 'observation_unexpected',(SELECT count(*) FROM observed_membership WHERE unexpected),
 'expectation_missing',(SELECT count(*) FROM expected_membership WHERE missing),
 'expectation_unexpected',(SELECT count(*) FROM expected_membership WHERE unexpected),
 'partition_denominator_or_publication',CASE WHEN (SELECT count(*) FROM partition_state)=1 AND NOT EXISTS(
 SELECT 1 FROM partition_state p CROSS JOIN acquires a WHERE p.time_role IS DISTINCT FROM 'TARGET'
 OR p.denominator_state IS DISTINCT FROM 'PROVISIONAL' OR p.publication_eligible IS DISTINCT FROM false
 OR p.producer_revision IS DISTINCT FROM 'massive-shortinterest-20261023-v1' OR p.immutable_input_versions IS DISTINCT FROM a.input_versions
 OR p.denominator_evidence IS DISTINCT FROM jsonb_build_object('archive_key',a.archive_key,'provider_page_chain_terminal',true,
 'independent_company_report_denominator',false,'july_support_rows_retained',true,
 'report_calendar',(SELECT jsonb_agg(to_jsonb(c) ORDER BY c.settlement_date) FROM literal_calendar c),
 'scheduled_dates_are_not_exact_publication_times',true)) THEN 0 ELSE 1 END
 ) mismatches
), plan_receipt AS MATERIALIZED (
 SELECT clock_timestamp() observed_at,'S02_BOUNDED_READBACK_PLAN_V1' check_revision,
 'PLAN_ONLY_REQUIRES_ALL135_PAGE_CHECKS' result,
 encode(sha256(convert_to(jsonb_build_object(
 'helpers',(SELECT to_jsonb(h) FROM helper_hashes h),
 'acquires',(SELECT jsonb_agg(to_jsonb(a) ORDER BY task_id) FROM acquires a),
 'pages',(SELECT jsonb_agg(to_jsonb(p) ORDER BY page_number) FROM pages p),
 'observed_rows',(SELECT encode(sha256(convert_to(coalesce(string_agg(jsonb_build_array(unit_key,record_sha256,row_xmin)::text,E'\n' ORDER BY unit_key),''),'UTF8')),'hex') FROM observed_keys),
 'expected_rows',(SELECT encode(sha256(convert_to(coalesce(string_agg(jsonb_build_array(unit_key,row_xmin)::text,E'\n' ORDER BY unit_key),''),'UTF8')),'hex') FROM expected_keys),
 'partition',(SELECT to_jsonb(p) FROM partition_state p))::text,'UTF8')),'hex') plan_sha256,
 (SELECT count(*) FROM pages) source_pages,(SELECT count(*) FROM staged) source_occurrences,
 (SELECT count(*) FROM canonical) canonical_versions,(SELECT count(*) FROM observed_keys) observed_versions,
 (SELECT count(*) FROM staged)-(SELECT count(*) FROM canonical) duplicate_occurrences,
 (SELECT bundle_sha256 FROM helper_hashes) compared_helper_bundle_sha256,
 (SELECT function_sha256->>'massive_shortinterest_normalize_v1(uuid,text)' FROM helper_hashes) compared_normalizer_sha256,
 (SELECT function_sha256->>'massive_shortinterest_derive_v1(uuid,text)' FROM helper_hashes) compared_deriver_sha256,
 (SELECT function_sha256->>'massive_shortinterest_calendar_v1()' FROM helper_hashes) compared_calendar_sha256,
 mismatches global_mismatches,(SELECT jsonb_agg(to_jsonb(p) ORDER BY page_number) FROM pages p) pages_to_verify,
 false independent_provider_accuracy_verified,false company_completeness_verified,
 false historical_availability_certified,false publication_admitted
 FROM global_counts
)""".replace("__ARCHIVE__", ARCHIVE)

PAGE = COMMON + r""", cfg AS MATERIALIZED (
 SELECT CASE WHEN current_setting('top3000.s02_readback_page',true)~'^[1-9][0-9]{0,2}$'
 THEN current_setting('top3000.s02_readback_page',true)::integer END page_number,
 nullif(current_setting('top3000.s02_plan_sha256',true),'') plan_before_sha256,clock_timestamp() started_at
), source_page AS MATERIALIZED (
 SELECT p.*,d.evidence document_evidence,d.content_bytes,
 CASE WHEN pg_input_is_valid(d.content_text,'jsonb') THEN d.content_text::jsonb END body,
 encode(sha256(convert_to(d.content_text,'UTF8')),'hex') recomputed_source_sha256,
 octet_length(d.content_text)::bigint recomputed_bytes
 FROM cfg c JOIN page_meta p ON p.page_number=c.page_number
 LEFT JOIN population_top3000_202608_202609_v1.source_document d ON d.source_id=p.source_id
), raw_occurrences AS MATERIALIZED (
 SELECT p.archive_key,p.partition_key,p.page_number,p.source_id,p.retrieved_at,
 r.ordinality::integer source_row,r.value raw_record,
 encode(sha256(convert_to(r.value::text,'UTF8')),'hex') raw_record_sha256
 FROM source_page p CROSS JOIN LATERAL jsonb_array_elements(
 CASE WHEN jsonb_typeof(p.body->'results')='array' THEN p.body->'results' ELSE '[]'::jsonb END)
 WITH ORDINALITY r(value,ordinality)
), literal_fixtures(input_key,raw_record,expected_ticker,expected_date,expected_short,expected_average,expected_cover,expected_short_valid) AS (
 VALUES
 ('fixture_zero','{"ticker":"A","settlement_date":"2026-08-14","short_interest":0,"avg_daily_volume":0,"days_to_cover":0}'::jsonb,'A',date '2026-08-14',0::numeric,0::numeric,0::numeric,true),
 ('fixture_fraction','{"ticker":"A","settlement_date":"2026-08-14","short_interest":1.5,"avg_daily_volume":0.125,"days_to_cover":1.25}'::jsonb,'A',date '2026-08-14',1.5,0.125,1.25,false),
 ('fixture_negative','{"ticker":"A","settlement_date":"2026-08-14","short_interest":-1,"avg_daily_volume":-1,"days_to_cover":-1}'::jsonb,'A',date '2026-08-14',-1,-1,-1,false),
 ('fixture_missing','{}'::jsonb,NULL,NULL,NULL,NULL,NULL,false),
 ('fixture_string_number','{"ticker":"A","settlement_date":"2026-08-14","short_interest":"5","avg_daily_volume":"0","days_to_cover":"0"}'::jsonb,'A',date '2026-08-14',NULL,NULL,NULL,false),
 ('fixture_invalid_date','{"ticker":"BAD SPACE","settlement_date":"2026-02-30","short_interest":10}'::jsonb,NULL,NULL,10,NULL,NULL,true),
 ('fixture_provider_cap','{"ticker":"A","settlement_date":"2026-09-30","short_interest":10,"avg_daily_volume":0,"days_to_cover":999.99}'::jsonb,'A',date '2026-09-30',10,0,999.99,true),
 ('fixture_exact_large','{"ticker":"A","settlement_date":"2026-07-15","short_interest":9007199254740993,"avg_daily_volume":0.00000000000000000001,"days_to_cover":1}'::jsonb,'A',date '2026-07-15',9007199254740993,0.00000000000000000001,1,true)
), parse_inputs AS MATERIALIZED (
 SELECT source_row::text input_key,raw_record FROM raw_occurrences
 UNION ALL SELECT input_key,raw_record FROM literal_fixtures
), parsed AS MATERIALIZED (
 SELECT input_key,raw_record,
 CASE WHEN jsonb_typeof(raw_record->'ticker')='string' AND length(raw_record->>'ticker') BETWEEN 1 AND 64
 AND raw_record->>'ticker'!~'[[:space:][:cntrl:]]' THEN raw_record->>'ticker' END ticker,
 CASE WHEN raw_record->>'settlement_date'~'^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
 AND pg_input_is_valid(raw_record->>'settlement_date','date') THEN (raw_record->>'settlement_date')::date END settlement_date,
 CASE WHEN jsonb_typeof(raw_record->'short_interest')='number' AND length((raw_record->'short_interest')::text)<=96
 THEN (raw_record->>'short_interest')::numeric END short_interest,
 CASE WHEN jsonb_typeof(raw_record->'avg_daily_volume')='number' AND length((raw_record->'avg_daily_volume')::text)<=96
 THEN (raw_record->>'avg_daily_volume')::numeric END avg_daily_volume,
 CASE WHEN jsonb_typeof(raw_record->'days_to_cover')='number' AND length((raw_record->'days_to_cover')::text)<=96
 THEN (raw_record->>'days_to_cover')::numeric END days_to_cover
 FROM parse_inputs
), fixture_checks AS MATERIALIZED (
 SELECT count(*) FILTER(WHERE p.input_key IS NULL OR p.ticker IS DISTINCT FROM f.expected_ticker
 OR p.settlement_date IS DISTINCT FROM f.expected_date OR p.short_interest IS DISTINCT FROM f.expected_short
 OR p.avg_daily_volume IS DISTINCT FROM f.expected_average OR p.days_to_cover IS DISTINCT FROM f.expected_cover
 OR coalesce(p.short_interest>=0 AND p.short_interest=trunc(p.short_interest),false) IS DISTINCT FROM f.expected_short_valid) mismatches
 FROM literal_fixtures f LEFT JOIN parsed p USING(input_key)
), raw_keys AS MATERIALIZED (
 SELECT r.*,p.ticker,p.settlement_date,p.short_interest,p.avg_daily_volume,p.days_to_cover,
 CASE WHEN p.ticker IS NOT NULL AND p.settlement_date IS NOT NULL
 THEN 'MassiveSI:business:'||encode(sha256(convert_to(jsonb_build_array(p.ticker,p.settlement_date)::text,'UTF8')),'hex')
 ELSE 'MassiveSI:unidentified:'||r.raw_record_sha256 END source_record_key,
 'MassiveSI:record:'||r.raw_record_sha256 unit_key
 FROM raw_occurrences r JOIN parsed p ON p.input_key=r.source_row::text
), staged_rows AS MATERIALIZED (
 SELECT r.* FROM cfg c JOIN population_top3000_202608_202609_v1.massive_shortinterest_record_v1 r
 ON r.archive_key='__ARCHIVE__' AND r.page_number=c.page_number
), staging_checks AS MATERIALIZED (
 SELECT coalesce(r.source_row,s.source_row) source_row,r.source_row IS NULL unexpected,s.source_row IS NULL missing,
 r.source_row IS NOT NULL AND s.source_row IS NOT NULL AND
 (s.source_id IS DISTINCT FROM r.source_id OR s.source_record_key IS DISTINCT FROM r.source_record_key
 OR s.unit_key IS DISTINCT FROM r.unit_key OR s.raw_record IS DISTINCT FROM r.raw_record
 OR s.raw_record_sha256 IS DISTINCT FROM r.raw_record_sha256) mismatch
 FROM raw_keys r FULL JOIN staged_rows s USING(archive_key,page_number,source_row)
), page_units AS MATERIALIZED (
 SELECT DISTINCT archive_key,unit_key FROM raw_keys
), first_positions AS MATERIALIZED (
 SELECT u.archive_key,u.unit_key,q.page_number,q.source_row,q.source_id
 FROM page_units u CROSS JOIN LATERAL (
 SELECT r.page_number,r.source_row,r.source_id FROM population_top3000_202608_202609_v1.massive_shortinterest_record_v1 r
 WHERE r.archive_key=u.archive_key AND r.unit_key=u.unit_key ORDER BY r.page_number,r.source_row LIMIT 1
 ) q
), canonical AS MATERIALIZED (
 SELECT r.*,cal.scheduled_publication_date,
 CASE WHEN r.settlement_date<date '2026-08-01' THEN 'SUPPORT' ELSE 'TARGET' END expected_time_role,
 r.settlement_date BETWEEN date '2026-07-15' AND date '2026-09-30' settlement_in_query,
 EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.massive_shortinterest_record_v1 q
 WHERE q.archive_key=r.archive_key AND q.source_record_key=r.source_record_key AND q.raw_record_sha256<>r.raw_record_sha256) business_conflict,
 provenance.occurrences,provenance.manifest_sha256
 FROM first_positions f JOIN raw_keys r USING(archive_key,unit_key,page_number,source_row,source_id)
 LEFT JOIN literal_calendar cal USING(settlement_date)
 CROSS JOIN LATERAL (
 SELECT count(*) occurrences,encode(sha256(convert_to(string_agg(q.source_id||':'||q.source_row::text,E'\n' ORDER BY q.page_number,q.source_row),'UTF8')),'hex') manifest_sha256
 FROM population_top3000_202608_202609_v1.massive_shortinterest_record_v1 q WHERE q.archive_key=r.archive_key AND q.unit_key=r.unit_key
 ) provenance
), field_rules AS MATERIALIZED (
 SELECT c.unit_key,v.name,v.missing,v.valid,v.conflictable,c.business_conflict
 FROM canonical c CROSS JOIN LATERAL (VALUES
 ('ticker',c.raw_record->>'ticker' IS NULL OR c.raw_record->>'ticker'='',c.ticker IS NOT NULL,true),
 ('settlement_date',c.raw_record->>'settlement_date' IS NULL,c.settlement_date IS NOT NULL AND coalesce(c.settlement_in_query,false) AND c.scheduled_publication_date IS NOT NULL,false),
 ('short_interest',c.raw_record->>'short_interest' IS NULL,c.short_interest IS NOT NULL AND c.short_interest>=0 AND c.short_interest=trunc(c.short_interest),true),
 ('avg_daily_volume',c.raw_record->>'avg_daily_volume' IS NULL,c.avg_daily_volume IS NOT NULL AND c.avg_daily_volume>=0,true),
 ('days_to_cover',c.raw_record->>'days_to_cover' IS NULL,c.days_to_cover IS NOT NULL AND c.days_to_cover>=0,true),
 ('scheduled_publication_date',c.scheduled_publication_date IS NULL,true,false),
 ('scheduled_publication_in_target_window',c.scheduled_publication_date IS NULL,true,false),
 ('exact_publication_at',true,true,false),('company_id',true,true,false),('security_id',true,true,false),('available_at',true,true,false)
 ) v(name,missing,valid,conflictable)
), expected_states AS MATERIALIZED (
 SELECT unit_key,jsonb_object_agg(name,CASE WHEN missing THEN 'UNAVAILABLE' WHEN NOT coalesce(valid,false) THEN 'QUARANTINED'
 WHEN conflictable AND business_conflict THEN 'CONFLICTING' ELSE 'VALUE' END) states
 FROM field_rules GROUP BY unit_key
), expected_content AS MATERIALIZED (
 SELECT c.*,s.states,
 jsonb_build_object('ticker',c.raw_record->'ticker','settlement_date',c.raw_record->'settlement_date',
 'short_interest',c.raw_record->'short_interest','avg_daily_volume',c.raw_record->'avg_daily_volume','days_to_cover',c.raw_record->'days_to_cover',
 'scheduled_publication_date',c.scheduled_publication_date,'scheduled_publication_in_target_window',
 CASE WHEN c.scheduled_publication_date IS NOT NULL THEN c.scheduled_publication_date BETWEEN date '2026-08-01' AND date '2026-09-30' END,
 'exact_publication_at',NULL,'company_id',NULL,'security_id',NULL,'available_at',NULL,'raw_event',c.raw_record) vals,
 jsonb_build_object('source_id',c.source_id,'source_record_key',c.source_record_key,'raw_record_sha256',c.raw_record_sha256,
 'source_page',c.page_number,'source_row',c.source_row,'archive_key',c.archive_key,
 'canonical_provenance_rule','FIRST_PAGE_THEN_FIRST_ROW_FOR_IDENTICAL_VERSION','source_occurrences',c.occurrences,
 'source_occurrence_manifest_sha256',c.manifest_sha256,'all_provenance_relation','population_top3000_202608_202609_v1.massive_shortinterest_record_v1',
 'all_provenance_unit_key',c.unit_key,'normalization_function_sha256',h.function_sha256->>'massive_shortinterest_normalize_v1(uuid,text)',
 'derivation_function_sha256',h.function_sha256->>'massive_shortinterest_derive_v1(uuid,text)',
 'calendar_function_sha256',h.function_sha256->>'massive_shortinterest_calendar_v1()',
 'scheduled_publication_source','https://www.finra.org/filing-reporting/regulatory-filing-systems/short-interest',
 'calendar_evidence_kind','CONTROLLER_VERIFIED_OFFICIAL_SCHEDULE_EXTRACT','scheduled_date_is_not_observed_publication_time',true,
 'source_publication_time','UNKNOWN','historical_receipt','NOT_OBSERVED','company_membership','UNVERIFIED',
 'query_settlement_date_matches',c.settlement_in_query,'july_settlements_support_target_state',true,
 'september_30_scheduled_publication_is_october_9',true,'company_report_census_verified',false) expected_lineage,
 jsonb_build_object('basis','PROVISIONAL_RETURNED_SYMBOL_SETTLEMENT_VERSION_ENUMERATION','independent_company_report_census',false,
 'top3000_company_membership_verified',false,'source_record_sha256',c.raw_record_sha256,'settlement_role',c.expected_time_role,
 'scheduled_publication_is_date_only',true,'strict_receipt_required',false) expected_applicability,
 h.function_sha256->>'massive_shortinterest_derive_v1(uuid,text)' expected_normalization_sha256
 FROM canonical c JOIN expected_states s USING(unit_key) CROSS JOIN helper_hashes h
), expected_rows AS MATERIALIZED (
 SELECT e.*,encode(sha256(convert_to(jsonb_build_object('values',vals,'states',states,'lineage',expected_lineage)::text,'UTF8')),'hex') expected_record_sha256
 FROM expected_content e
), observed_comparison AS MATERIALIZED (
 SELECT e.unit_key,o.unit_key IS NULL missing,
 o.unit_key IS NOT NULL AND (o.values_json IS DISTINCT FROM e.vals OR o.field_state IS DISTINCT FROM e.states
 OR o.lineage IS DISTINCT FROM e.expected_lineage OR o.record_sha256 IS DISTINCT FROM e.expected_record_sha256
 OR o.record_sha256 IS DISTINCT FROM encode(sha256(convert_to(jsonb_build_object('values',o.values_json,'states',o.field_state,'lineage',o.lineage)::text,'UTF8')),'hex')) fields_lineage_or_hash,
 o.unit_key IS NOT NULL AND (o.company_id IS NOT NULL OR o.security_id IS NOT NULL OR o.reporting_entity_id IS NOT NULL
 OR o.time_role IS DISTINCT FROM e.expected_time_role OR o.effective_date IS DISTINCT FROM e.settlement_date OR o.event_at IS NOT NULL
 OR o.source_published_at IS NOT NULL OR o.historical_received_at IS NOT NULL OR o.available_at IS NOT NULL
 OR o.availability_policy IS DISTINCT FROM 'ARCHIVED_OBSERVATION' OR o.vintage_key IS DISTINCT FROM e.archive_key
 OR o.source_id IS DISTINCT FROM e.source_id OR o.source_record_key IS DISTINCT FROM e.source_record_key
 OR o.normalization_sha256 IS DISTINCT FROM e.expected_normalization_sha256 OR o.ingested_at IS NULL) metadata,
 o.record_sha256 actual_record_sha256,e.expected_record_sha256
 FROM expected_rows e LEFT JOIN population_top3000_202608_202609_v1.source_observation o
 ON o.release_id='top3000_companies_202608_202609_v1' AND o.family_id='S02'
 AND o.partition_key=e.partition_key AND o.unit_key=e.unit_key
), expectation_comparison AS MATERIALIZED (
 SELECT e.unit_key,x.unit_key IS NULL missing,x.unit_key IS NOT NULL AND
 (x.company_id IS NOT NULL OR x.security_id IS NOT NULL OR x.reporting_entity_id IS NOT NULL
 OR x.time_role IS DISTINCT FROM e.expected_time_role OR x.expectation_source_id IS DISTINCT FROM e.source_id
 OR x.expectation_record_key IS DISTINCT FROM e.source_record_key OR x.applicability_evidence IS DISTINCT FROM e.expected_applicability
 OR x.required_fields IS DISTINCT FROM ARRAY['ticker','settlement_date','short_interest','avg_daily_volume','days_to_cover',
 'scheduled_publication_date','scheduled_publication_in_target_window','exact_publication_at','company_id','security_id','available_at']::text[]) mismatch
 FROM expected_rows e LEFT JOIN population_top3000_202608_202609_v1.expected_observation x
 ON x.release_id='top3000_companies_202608_202609_v1' AND x.family_id='S02'
 AND x.partition_key=e.partition_key AND x.unit_key=e.unit_key
), page_checks AS MATERIALIZED (
 SELECT jsonb_build_object(
 'config_or_source_page',CASE WHEN (SELECT page_number FROM cfg) BETWEEN 1 AND 135
 AND (SELECT plan_before_sha256 FROM cfg)~'^[0-9a-f]{64}$' AND (SELECT count(*) FROM source_page)=1 THEN 0 ELSE 1 END,
 'acquire_revision_or_helper_bundle',CASE WHEN (SELECT count(*) FROM acquires)=1
 AND NOT EXISTS(SELECT 1 FROM acquires a CROSS JOIN helper_hashes h WHERE a.status<>'COMPLETE'
 OR a.archive_key<>'__ARCHIVE__' OR a.input_versions->>'shortinterest_bundle_sha256' IS DISTINCT FROM h.bundle_sha256) THEN 0 ELSE 1 END,
 'literal_date_numeric_semantics',(SELECT mismatches FROM fixture_checks),
 'source_envelope_hash_or_bytes',(SELECT count(*) FROM source_page p WHERE p.body IS NULL
 OR jsonb_typeof(p.body) IS DISTINCT FROM 'object' OR p.body->>'status' IS DISTINCT FROM 'OK'
 OR jsonb_typeof(p.body->'results') IS DISTINCT FROM 'array' OR p.result_count IS DISTINCT FROM (SELECT count(*) FROM raw_occurrences)
 OR p.body->>'next_url' IS DISTINCT FROM p.next_url
 OR p.recomputed_source_sha256 IS DISTINCT FROM p.source_sha256 OR p.recomputed_source_sha256 IS DISTINCT FROM p.document_sha256
 OR p.recomputed_bytes IS DISTINCT FROM p.content_bytes OR p.content_bytes>8388608 OR p.content_bytes<1
 OR p.document_evidence IS DISTINCT FROM jsonb_build_object('wire_sha256',p.wire_sha256,'wire_bytes',p.wire_bytes,
 'stored_content_bytes',p.content_bytes,'hash_representation','EXACT_UTF8_RESPONSE_WITH_TOP_LEVEL_NEXT_URL_CREDENTIAL_REDACTION_ONLY',
 'wire_bytes_retained',false,'provider_request_id',CASE WHEN jsonb_typeof(p.body->'request_id')='string' THEN left(p.body->>'request_id',160) END,
 'scope','S02 settlement dates 2026-07-15 through 2026-09-30; July support and Aug/Sep target; publication schedule retained separately',
 'original_historical_receipt',false)),
 'raw_nonobject_or_foreign_date',(SELECT count(*) FROM raw_keys WHERE jsonb_typeof(raw_record) IS DISTINCT FROM 'object'
 OR settlement_date IS NULL OR settlement_date<date '2026-07-15' OR settlement_date>date '2026-09-30'),
 'staging_missing',(SELECT count(*) FROM staging_checks WHERE missing),'staging_unexpected',(SELECT count(*) FROM staging_checks WHERE unexpected),
 'staging_fields_or_hash',(SELECT count(*) FROM staging_checks WHERE mismatch),
 'canonical_position_missing',(SELECT count(*) FROM page_units)-(SELECT count(*) FROM first_positions),
 'observation_missing',(SELECT count(*) FROM observed_comparison WHERE missing),
 'observation_fields_lineage_or_hash',(SELECT count(*) FROM observed_comparison WHERE fields_lineage_or_hash),
 'observation_identity_timing_or_source',(SELECT count(*) FROM observed_comparison WHERE metadata),
 'expectation_missing',(SELECT count(*) FROM expectation_comparison WHERE missing),
 'expectation_fields_or_provenance',(SELECT count(*) FROM expectation_comparison WHERE mismatch)
 ) mismatches
)
SELECT clock_timestamp() observed_at,'S02_SINGLE_PAGE_SOURCE_RECONCILIATION_V1' check_revision,
 CASE WHEN NOT EXISTS(SELECT 1 FROM jsonb_each_text(ch.mismatches) e WHERE e.value::bigint<>0)
 THEN 'PASS_PAGE_SOURCE_TRANSFORMATION_ONLY' ELSE 'FAIL_OR_NOT_READY' END result,
 '__ARCHIVE__' archive_key,c.page_number,c.plan_before_sha256,
 (SELECT source_snapshot_sha256 FROM source_page) source_snapshot_sha256,
 (SELECT source_sha256 FROM source_page) source_sha256,(SELECT recomputed_bytes FROM source_page) retained_source_bytes,
 (SELECT count(*) FROM raw_occurrences) raw_occurrences,(SELECT count(*) FROM expected_rows) canonical_versions,
 encode(sha256(convert_to((SELECT coalesce(string_agg(jsonb_build_array(archive_key,page_number,source_row,source_id,source_record_key,unit_key,raw_record_sha256)::text,E'\n' ORDER BY source_row),'') FROM raw_keys),'UTF8')),'hex') expected_raw_links_manifest_sha256,
 encode(sha256(convert_to((SELECT coalesce(string_agg(jsonb_build_array(unit_key,expected_record_sha256)::text,E'\n' ORDER BY unit_key),'') FROM expected_rows),'UTF8')),'hex') expected_canonical_manifest_sha256,
 encode(sha256(convert_to((SELECT coalesce(string_agg(jsonb_build_array(unit_key,actual_record_sha256)::text,E'\n' ORDER BY unit_key),'') FROM observed_comparison),'UTF8')),'hex') actual_canonical_manifest_sha256,
 (SELECT bundle_sha256 FROM helper_hashes) compared_helper_bundle_sha256,
 ch.mismatches,extract(epoch FROM clock_timestamp()-c.started_at) elapsed_seconds,
 false original_wire_bytes_independently_rehashed,false independent_provider_accuracy_verified,
 false company_completeness_verified,false historical_availability_certified,false publication_admitted
 FROM cfg c CROSS JOIN page_checks ch""".replace("__ARCHIVE__", ARCHIVE)

GLOBAL = PLAN + r""", receipt_cfg AS MATERIALIZED (
 SELECT nullif(current_setting('top3000.s02_plan_before',true),'')::jsonb plan_before,
 nullif(current_setting('top3000.s02_page_receipts',true),'')::jsonb page_receipts
), receipts AS MATERIALIZED (
 SELECT value receipt FROM receipt_cfg c CROSS JOIN LATERAL jsonb_array_elements(
 CASE WHEN jsonb_typeof(c.page_receipts)='array' THEN c.page_receipts ELSE '[]'::jsonb END)
), receipt_rows AS MATERIALIZED (
 SELECT receipt,CASE WHEN receipt->>'page_number'~'^[1-9][0-9]{0,2}$' THEN (receipt->>'page_number')::integer END page_number
 FROM receipts
), final_checks AS MATERIALIZED (
 SELECT jsonb_build_object(
 'global_mismatch_fields',(SELECT count(*) FROM jsonb_each_text(p.global_mismatches) e WHERE e.value::bigint<>0),
 'plan_before_changed',CASE WHEN c.plan_before->>'check_revision'='S02_BOUNDED_READBACK_PLAN_V1'
 AND c.plan_before->>'plan_sha256'=p.plan_sha256 AND c.plan_before->>'source_pages'='135'
 AND c.plan_before->>'source_occurrences'='134931' AND jsonb_typeof(c.plan_before->'global_mismatches')='object'
 AND c.plan_before->'global_mismatches'=p.global_mismatches
 AND NOT EXISTS(SELECT 1 FROM jsonb_each_text(c.plan_before->'global_mismatches') e WHERE e.value IS DISTINCT FROM '0') THEN 0 ELSE 1 END,
 'receipt_coverage',CASE WHEN (SELECT count(*) FROM receipt_rows)=135 AND (SELECT count(DISTINCT page_number) FROM receipt_rows)=135
 AND (SELECT min(page_number) FROM receipt_rows)=1 AND (SELECT max(page_number) FROM receipt_rows)=135 THEN 0 ELSE 1 END,
 'receipt_content_or_page_binding',(SELECT count(*) FROM receipt_rows r LEFT JOIN pages q USING(page_number)
 WHERE q.page_number IS NULL OR r.receipt->>'check_revision' IS DISTINCT FROM 'S02_SINGLE_PAGE_SOURCE_RECONCILIATION_V1'
 OR r.receipt->>'result' IS DISTINCT FROM 'PASS_PAGE_SOURCE_TRANSFORMATION_ONLY'
 OR r.receipt->>'archive_key' IS DISTINCT FROM '__ARCHIVE__' OR r.receipt->>'plan_before_sha256' IS DISTINCT FROM p.plan_sha256
 OR r.receipt->>'source_snapshot_sha256' IS DISTINCT FROM q.source_snapshot_sha256
 OR r.receipt->>'source_sha256' IS DISTINCT FROM q.source_sha256
 OR r.receipt->>'retained_source_bytes' IS DISTINCT FROM q.document_bytes::text
 OR r.receipt->>'raw_occurrences' IS DISTINCT FROM q.result_count::text
 OR r.receipt->>'canonical_versions' IS DISTINCT FROM q.canonical_versions::text
 OR r.receipt->>'expected_raw_links_manifest_sha256' IS DISTINCT FROM q.staged_links_manifest_sha256
 OR r.receipt->>'expected_canonical_manifest_sha256' IS DISTINCT FROM r.receipt->>'actual_canonical_manifest_sha256'
 OR coalesce(r.receipt->>'expected_canonical_manifest_sha256','')!~'^[0-9a-f]{64}$'
 OR coalesce(r.receipt->>'actual_canonical_manifest_sha256','')!~'^[0-9a-f]{64}$'
 OR r.receipt->>'compared_helper_bundle_sha256' IS DISTINCT FROM p.compared_helper_bundle_sha256
 OR r.receipt->'mismatches' IS DISTINCT FROM '{"config_or_source_page":0,"acquire_revision_or_helper_bundle":0,"literal_date_numeric_semantics":0,"source_envelope_hash_or_bytes":0,"raw_nonobject_or_foreign_date":0,"staging_missing":0,"staging_unexpected":0,"staging_fields_or_hash":0,"canonical_position_missing":0,"observation_missing":0,"observation_fields_lineage_or_hash":0,"observation_identity_timing_or_source":0,"expectation_missing":0,"expectation_fields_or_provenance":0}'::jsonb
 OR r.receipt->'independent_provider_accuracy_verified' IS DISTINCT FROM 'false'::jsonb
 OR r.receipt->'company_completeness_verified' IS DISTINCT FROM 'false'::jsonb
 OR r.receipt->'historical_availability_certified' IS DISTINCT FROM 'false'::jsonb
 OR r.receipt->'publication_admitted' IS DISTINCT FROM 'false'::jsonb)
 ) mismatches
 FROM receipt_cfg c CROSS JOIN plan_receipt p
)
SELECT clock_timestamp() observed_at,'S02_ALL135_RETAINED_SOURCE_CANONICAL_RECONCILIATION_V1' check_revision,
 CASE WHEN NOT EXISTS(SELECT 1 FROM jsonb_each_text(c.mismatches) e WHERE e.value::bigint<>0)
 THEN 'PASS_SOURCE_TRANSFORMATION_ONLY' ELSE 'FAIL_OR_NOT_READY' END result,
 p.plan_sha256,p.source_pages,p.source_occurrences,p.canonical_versions,p.observed_versions,p.duplicate_occurrences,
 p.compared_helper_bundle_sha256,p.compared_normalizer_sha256,p.compared_deriver_sha256,p.compared_calendar_sha256,
 c.mismatches,p.global_mismatches,
 encode(sha256(convert_to((SELECT jsonb_agg(receipt ORDER BY page_number) FROM receipt_rows)::text,'UTF8')),'hex') page_receipts_sha256,
 false original_wire_bytes_independently_rehashed,false independent_provider_accuracy_verified,
 false company_completeness_verified,false historical_availability_certified,false publication_admitted
 FROM final_checks c CROSS JOIN plan_receipt p""".replace("__ARCHIVE__", ARCHIVE)

files = {
    "s02_independent_reconciliation_plan.sql": HEADER + "WITH " + PLAN + "\nSELECT * FROM plan_receipt;\nCOMMIT;\n",
    "s02_independent_source_page_reconciliation.sql": HEADER + "WITH " + PAGE + ";\nCOMMIT;\n",
    "s02_independent_reconciliation_global.sql": HEADER + "WITH " + GLOBAL + ";\nCOMMIT;\n",
}

notes = f"""# S02 independent retained-source and canonical reconciliation

## Scope

This read-only draft verifies all **135 retained pages / 134,931 source occurrences** in `{ARCHIVE}`. The source window covers settlement dates July15–September30,2026; July records support the August/September mission. The requested population remains 3,000 companies, whose membership and report denominator are unresolved. A terminal provider page chain does not establish that company denominator.

The controller's earlier actual raw-only receipt `s02_all135_source_raw_independent_actual.json` reports zero hash/byte, count, cursor and date/shape mismatches at14:41:50.513806Z, source manifest `b1632ed886746c9c8266ee42bd8e31e6a37cddbc0262dafded6334f406cdea1a`. It is a preserved prior result, not a claim these new canonical queries were executed.

## Controller execution sequence

1. Wait for the admitted S02 NORMALIZE and DERIVE tasks to finish. Run `s02_independent_reconciliation_plan.sql`; retain its single result and require every `global_mismatches` value to be zero. The plan intentionally returns `PLAN_ONLY_REQUIRES_ALL135_PAGE_CHECKS`.
2. For each page1–135, insert `SET LOCAL top3000.s02_readback_page='<page>';` and `SET LOCAL top3000.s02_plan_sha256='<original plan_sha256>';` immediately after BEGIN in `s02_independent_source_page_reconciliation.sql`. Retain each actual receipt. Every page must return `PASS_PAGE_SOURCE_TRANSFORMATION_ONLY` with all mismatch counts zero.
3. Insert the original plan object into `top3000.s02_plan_before` and the array of all135 actual page receipts into `top3000.s02_page_receipts` in the global query. It recomputes the plan, requires an unchanged fingerprint, exact page membership and successful matching receipts, and returns only `PASS_SOURCE_TRANSFORMATION_ONLY` on success.

Each SQL statement has a **10-second timeout**,500ms lock timeout and16MiB work-memory limit. Each page query rehashes/parses one retained body of at most1,000 records. The metadata plan uses indexed source keys and row-version fingerprints, without loading all source bodies or canonical JSON. Each page performs bounded exact raw/staging, canonical and expectation comparisons. If a query times out, preserve the failure and diagnose its plan; do not silently raise the deadline or relabel an unexecuted page as passed. Separate transactions permit resumable receipts; a changed final plan invalidates this combined pass.

## Independent checks

- Exact retained UTF8 source SHA/length and full document evidence; source status,count,request/cursor identity and reserved provider-attempt linkage. Original wire bytes are not retained and cannot be independently rehashed.
- Every source occurrence, raw JSON, business key and content-version key, with missing/unexpected staging rows. Every canonical version uses the first page/row for an identical record; all occurrences contribute to its exact provenance hash. Distinct content versions for one business key remain conflicting.
- Set-based expected fields and states using core PostgreSQL date/numeric parsing. No production parser, normalizer, calendar, context or bundle helper is executed. Eight embedded literal cases cover zero/missing/string numeric data,fractional and negative short interest,invalid dates,provider999.99 values,and exact numbers beyond binary-float precision.
- A literal six-date controller-verified FINRA schedule extract is compared independently with output dates. Scheduled dates remain DATE values; they are never manufactured into publication instants. September30 settlement has scheduled October9 publication. `event_at`,observed publication,historical receipt,and available-at remain NULL;company/security/reporting-entity IDs remain unresolved.
- Exact values,all11 field states,lineage,record hash,expected-field list,expectation provenance,source references and provisional publication-ineligible partition. Global set comparisons reject missing or unexpected canonical/expected rows.
- Before/after fingerprints include source/staging/observation/expectation row `xmin` versions as well as declared hashes. Any committed update or replacement during the audit changes the fingerprint even if an application hash was left unchanged. This is a short-lived same-database concurrency check,not a durable identity or cross-database hash.

## Limits and authority

These queries test faithful transformation and retained provenance. They do not establish provider/exchange accuracy,all-company report coverage,source entitlement beyond the existing admission,or strict historical usability. Provider days-to-cover is preserved without recomputing or endorsing its rounding/censoring semantics. The verifier uses independently written set-based logic but shares PostgreSQL JSONB/date/numeric/SHA primitives with the producer; that shared canonicalization is disclosed. The source calendar derives from the controller's already verified official schedule extract; this local task made no new external calendar request.

Root alone executes and retains actual SQL/receipts and performs durable Git/database checkpointing. No helper functions,tables,publication routes,budget limits,leases or worker states are changed by this draft. Supabase/data-quality guidance informed the review; the skill's fixed historical-scope executable review helper was not run or relabeled as a live operational certificate. The current explicitly authorized August/September2026 scope and $250 cap remain authoritative. **Database execution: NOT_RUN by this child. Certification: NOT_ISSUED.**
"""
files["s02_independent_reconciliation_README.md"] = notes
for name, text in files.items():
    (P / name).write_text(text)
manifest = {
    "status": "LOCAL_READ_ONLY_DRAFT_NOT_DATABASE_EXECUTED",
    "expected_archive_key": ARCHIVE,
    "expected_source_pages": 135,
    "expected_source_occurrences": 134931,
    "statement_timeout_seconds": 10,
    "query_calls_production_parser_or_normalizer": False,
    "company_denominator_verified": False,
    "provider_accuracy_verified": False,
    "certification_status": "NOT_ISSUED",
    "files": [{"file": n, "sha256": hashlib.sha256((P / n).read_bytes()).hexdigest(), "bytes": (P / n).stat().st_size} for n in files],
}
(P / "s02_independent_reconciliation_manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
print(json.dumps(manifest, indent=2))
