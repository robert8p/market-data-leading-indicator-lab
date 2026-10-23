-- ROOT ONLY, READ ONLY. No production parser, normalizer, calendar or context helper is called.
BEGIN READ ONLY;
SET LOCAL statement_timeout='10s';
SET LOCAL lock_timeout='500ms';
SET LOCAL timezone='UTC';
SET LOCAL datestyle='ISO, YMD';
SET LOCAL work_mem='16MB';
WITH helper_definitions AS MATERIALIZED (
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
), cfg AS MATERIALIZED (
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
 ON r.archive_key='massive-archive:S02:c1ed3ed6aa5bad110f122c2459ba27f6daa14a7ac484363d4915c0fc9ddb1f88' AND r.page_number=c.page_number
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
 OR a.archive_key<>'massive-archive:S02:c1ed3ed6aa5bad110f122c2459ba27f6daa14a7ac484363d4915c0fc9ddb1f88' OR a.input_versions->>'shortinterest_bundle_sha256' IS DISTINCT FROM h.bundle_sha256) THEN 0 ELSE 1 END,
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
 'massive-archive:S02:c1ed3ed6aa5bad110f122c2459ba27f6daa14a7ac484363d4915c0fc9ddb1f88' archive_key,c.page_number,c.plan_before_sha256,
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
 FROM cfg c CROSS JOIN page_checks ch;
COMMIT;
