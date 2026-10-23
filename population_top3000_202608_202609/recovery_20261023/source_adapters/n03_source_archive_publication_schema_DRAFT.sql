-- Root-reviewed, retained-database-only publication slice. Installation admits no task.
-- Existing family validation/REPAIR holds and original publisher/deliverer are preserved.
-- A successful source archive check is not a PASS for the full N03 family contract.
CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.n03_archive_source_check_v1()
RETURNS jsonb LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path TO 'pg_catalog'
AS $f$
DECLARE
 v_page population_top3000_202608_202609_v1.massive_archive_page_v1%rowtype;
 v_doc population_top3000_202608_202609_v1.source_document%rowtype;
 v_part population_top3000_202608_202609_v1.pipeline_partition%rowtype;
 v_json jsonb;v_mismatches jsonb;v_observed_sha text;v_expected_sha text;
 v_archive constant text:='massive-archive:N03:e79ba7c86316b20d7181cf986511edda75c4abeb7c053d56acc3510489886aec';
 v_source constant text:='massive-archive:N03:e79ba7c86316b20d7181cf986511edda75c4abeb7c053d56acc3510489886aec:page:1';
 v_source_sha constant text:='9609fe84088fc84a6ba2705270806fa06c1450cf460651819d10038be9fc0dfe';
BEGIN
 SELECT * INTO v_page FROM population_top3000_202608_202609_v1.massive_archive_page_v1
 WHERE archive_key=v_archive AND page_number=1;
 IF NOT FOUND OR v_page.family_id IS DISTINCT FROM 'N03'
  OR v_page.partition_key IS DISTINCT FROM 'splits-execution-202608-202609'
  OR v_page.source_id IS DISTINCT FROM v_source OR v_page.source_sha256 IS DISTINCT FROM v_source_sha
  OR v_page.result_count IS DISTINCT FROM 297 OR v_page.normalized_source_records IS DISTINCT FROM 297
  OR v_page.normalized_at IS NULL OR v_page.next_url IS NOT NULL
  OR v_page.request_url IS DISTINCT FROM 'https://api.massive.com/stocks/v1/splits?execution_date.gte=2026-08-01&execution_date.lte=2026-09-30&sort=execution_date.asc&limit=1000'
  OR (SELECT count(*) FROM population_top3000_202608_202609_v1.massive_archive_page_v1 WHERE archive_key=v_archive)<>1
 THEN RAISE EXCEPTION 'N03_ARCHIVE_SOURCE_PAGE_CHANGED'; END IF;
 SELECT * INTO v_doc FROM population_top3000_202608_202609_v1.source_document WHERE source_id=v_source;
 IF NOT FOUND OR v_doc.content_text IS NULL OR v_doc.source_sha256 IS DISTINCT FROM v_source_sha
  OR v_doc.content_bytes IS DISTINCT FROM 67010::bigint
  OR octet_length(convert_to(v_doc.content_text,'UTF8')) IS DISTINCT FROM 67010
  OR encode(sha256(convert_to(v_doc.content_text,'UTF8')),'hex') IS DISTINCT FROM v_source_sha
  OR v_doc.source_provider IS DISTINCT FROM 'Massive' OR v_doc.source_uri IS DISTINCT FROM v_page.request_url
  OR v_doc.retrieved_at IS DISTINCT FROM v_page.retrieved_at
  OR v_doc.source_published_at IS NOT NULL OR v_doc.historical_received_at IS NOT NULL
  OR v_doc.evidence->>'wire_sha256' IS DISTINCT FROM v_page.wire_sha256
  OR (v_doc.evidence->>'wire_bytes')::bigint IS DISTINCT FROM v_page.wire_bytes
 THEN RAISE EXCEPTION 'N03_ARCHIVE_SOURCE_HASH_OR_METADATA_CHANGED'; END IF;
 SELECT * INTO v_part FROM population_top3000_202608_202609_v1.pipeline_partition
 WHERE family_id='N03' AND partition_key='splits-execution-202608-202609';
 IF NOT FOUND OR v_part.denominator_state IS DISTINCT FROM 'PROVISIONAL' OR v_part.publication_eligible IS DISTINCT FROM false
  OR v_part.immutable_input_versions->>'scope_sha256' IS DISTINCT FROM '8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd'
  OR v_part.immutable_input_versions->>'archive_contract_revision' IS DISTINCT FROM 'massive-actions-20261023-v1'
  OR v_part.denominator_evidence->>'archive_key' IS DISTINCT FROM v_archive
  OR EXISTS(SELECT 1 FROM (VALUES('ACQUIRE'),('NORMALIZE'),('DERIVE')) required_stage(stage)
    WHERE NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task completed_task
     WHERE completed_task.family_id='N03' AND completed_task.partition_key=v_part.partition_key
      AND completed_task.stage=required_stage.stage AND completed_task.status='COMPLETE'
      AND completed_task.input_versions=v_part.immutable_input_versions))
 THEN RAISE EXCEPTION 'N03_ARCHIVE_COMPLETED_INPUT_OR_PROVISIONAL_SCOPE_CHANGED'; END IF;
 v_json:=v_doc.content_text::jsonb;
 IF v_json->>'status' IS DISTINCT FROM 'OK' OR jsonb_typeof(v_json->'results') IS DISTINCT FROM 'array'
  OR nullif(v_json->>'next_url','') IS NOT NULL
 THEN RAISE EXCEPTION 'N03_ARCHIVE_SOURCE_ENVELOPE_CHANGED'; END IF;
 IF jsonb_array_length(v_json->'results')<>297 THEN RAISE EXCEPTION 'N03_ARCHIVE_SOURCE_COUNT_CHANGED'; END IF;

 -- Independent set-based reconstruction; no producer parser/normalizer/deriver is called.
 WITH raw_rows AS MATERIALIZED (
  SELECT a.value raw,a.ordinality::integer source_row,
   encode(sha256(convert_to(a.value::text,'UTF8')),'hex') raw_sha
  FROM jsonb_array_elements(v_json->'results') WITH ORDINALITY a(value,ordinality)
 ), expected_rows AS MATERIALIZED (
  SELECT r.*, 'Massive:'||(r.raw->>'id')||':'||r.raw_sha unit_key,
   CASE WHEN r.raw->>'execution_date'~'^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
    AND pg_input_is_valid(r.raw->>'execution_date','date') THEN (r.raw->>'execution_date')::date END event_date,
   CASE WHEN jsonb_typeof(r.raw->'split_from')='number' AND jsonb_typeof(r.raw->'split_to')='number'
    AND (r.raw->>'split_from')::numeric>0 AND (r.raw->>'split_to')::numeric>0
    THEN (r.raw->>'split_to')::numeric/(r.raw->>'split_from')::numeric END ratio
  FROM raw_rows r
 ), actual_rows AS MATERIALIZED (
  SELECT o.* FROM population_top3000_202608_202609_v1.source_observation o
  WHERE o.release_id='top3000_companies_202608_202609_v1' AND o.family_id='N03' AND o.partition_key=v_part.partition_key
 ), expected_observations AS MATERIALIZED (
  SELECT e.* FROM population_top3000_202608_202609_v1.expected_observation e
  WHERE e.release_id='top3000_companies_202608_202609_v1' AND e.family_id='N03' AND e.partition_key=v_part.partition_key
 ), compared AS MATERIALIZED (
  SELECT r.*,o.unit_key observed_unit,o.values_json,o.field_state,o.lineage,o.record_sha256,
   o.source_id,o.source_record_key,o.time_role,o.event_at,o.effective_date,o.source_published_at,
   o.historical_received_at,o.available_at,o.availability_policy,o.vintage_key,o.company_id,o.security_id,
   e.unit_key expected_unit,e.required_fields,e.expectation_source_id,e.expectation_record_key,e.applicability_evidence,
   e.company_id expected_company,e.security_id expected_security,e.time_role expected_time_role
  FROM expected_rows r LEFT JOIN actual_rows o USING(unit_key) LEFT JOIN expected_observations e USING(unit_key)
 )
 SELECT jsonb_build_object(
  'source_shape_or_date',(SELECT count(*) FROM expected_rows r WHERE jsonb_typeof(r.raw) IS DISTINCT FROM 'object'
   OR jsonb_typeof(r.raw->'id') IS DISTINCT FROM 'string' OR nullif(r.raw->>'id','') IS NULL
   OR jsonb_typeof(r.raw->'ticker') IS DISTINCT FROM 'string' OR nullif(r.raw->>'ticker','') IS NULL
   OR r.event_date IS NULL OR r.event_date NOT BETWEEN date '2026-08-01' AND date '2026-09-30'
   OR r.ratio IS NULL OR r.raw->>'adjustment_type' IS NULL OR r.raw->>'adjustment_type' NOT IN('forward_split','reverse_split','stock_dividend')),
  'duplicate_source_ids',(SELECT count(*) FROM (SELECT raw->>'id' FROM raw_rows GROUP BY raw->>'id' HAVING count(*)<>1) duplicates),
  'missing_normalized',(SELECT count(*) FROM compared WHERE observed_unit IS NULL),
  'unexpected_normalized',(SELECT count(*) FROM actual_rows o WHERE NOT EXISTS(SELECT 1 FROM expected_rows r WHERE r.unit_key=o.unit_key)),
  'source_values_or_ratio',(SELECT count(*) FROM compared c WHERE c.values_json IS DISTINCT FROM jsonb_build_object(
   'provider_event_id',c.raw->'id','ticker',c.raw->'ticker','execution_date',c.raw->'execution_date',
   'split_from',c.raw->'split_from','split_to',c.raw->'split_to','adjustment_type',c.raw->'adjustment_type',
   'effective_at',NULL,'company_id',NULL,'security_id',NULL,'available_at',NULL,'raw_event',c.raw,'share_unit_ratio_candidate',c.ratio)),
  'field_states',(SELECT count(*) FROM compared c WHERE c.field_state IS DISTINCT FROM
   '{"provider_event_id":"VALUE","ticker":"VALUE","execution_date":"VALUE","split_from":"VALUE","split_to":"VALUE","adjustment_type":"VALUE","effective_at":"UNAVAILABLE","company_id":"UNAVAILABLE","security_id":"UNAVAILABLE","available_at":"UNAVAILABLE","share_unit_ratio_candidate":"VALUE"}'::jsonb),
  'lineage_or_record_hash',(SELECT count(*) FROM compared c WHERE c.lineage->>'source_id' IS DISTINCT FROM v_source
   OR c.lineage->>'source_record_key' IS DISTINCT FROM c.raw->>'id' OR c.lineage->>'raw_record_sha256' IS DISTINCT FROM c.raw_sha
   OR c.lineage->>'archive_key' IS DISTINCT FROM v_archive OR c.lineage->>'source_page' IS DISTINCT FROM '1'
   OR c.lineage->>'source_row' IS DISTINCT FROM c.source_row::text
   OR c.lineage->>'company_membership' IS DISTINCT FROM 'UNVERIFIED'
   OR c.lineage->>'source_publication_time' IS DISTINCT FROM 'UNKNOWN'
   OR c.lineage->>'historical_receipt' IS DISTINCT FROM 'NOT_OBSERVED'
   OR c.lineage->'price_adjustment_applied' IS DISTINCT FROM 'false'::jsonb
   OR c.record_sha256 IS DISTINCT FROM encode(sha256(convert_to(jsonb_build_object('values',c.values_json,'states',c.field_state,'lineage',c.lineage)::text,'UTF8')),'hex')),
  'identity_time_or_source',(SELECT count(*) FROM compared c WHERE c.source_id IS DISTINCT FROM v_source
   OR c.source_record_key IS DISTINCT FROM c.raw->>'id' OR c.time_role IS DISTINCT FROM 'TARGET'
   OR c.effective_date IS DISTINCT FROM c.event_date OR c.vintage_key IS DISTINCT FROM v_archive
   OR c.availability_policy IS DISTINCT FROM 'ARCHIVED_OBSERVATION'
   OR c.event_at IS NOT NULL OR c.source_published_at IS NOT NULL OR c.historical_received_at IS NOT NULL
   OR c.available_at IS NOT NULL OR c.company_id IS NOT NULL OR c.security_id IS NOT NULL),
  'missing_expectation',(SELECT count(*) FROM compared WHERE expected_unit IS NULL),
  'unexpected_expectation',(SELECT count(*) FROM expected_observations e WHERE NOT EXISTS(SELECT 1 FROM expected_rows r WHERE r.unit_key=e.unit_key)),
  'expectation_fields_or_scope',(SELECT count(*) FROM compared c WHERE c.required_fields IS DISTINCT FROM
   ARRAY['provider_event_id','ticker','execution_date','split_from','split_to','adjustment_type','effective_at','company_id','security_id','available_at']::text[]
   OR c.expectation_source_id IS DISTINCT FROM v_source OR c.expectation_record_key IS DISTINCT FROM c.raw->>'id'
   OR c.expected_company IS NOT NULL OR c.expected_security IS NOT NULL OR c.expected_time_role IS DISTINCT FROM 'TARGET'
   OR c.applicability_evidence->>'source_record_sha256' IS DISTINCT FROM c.raw_sha
   OR c.applicability_evidence->'independent_event_census' IS DISTINCT FROM 'false'::jsonb
   OR c.applicability_evidence->'top3000_company_membership_verified' IS DISTINCT FROM 'false'::jsonb
   OR c.applicability_evidence->'strict_receipt_required' IS DISTINCT FROM 'false'::jsonb)
 ),
 (SELECT encode(sha256(convert_to(coalesce(string_agg(unit_key||':'||record_sha256,E'\n' ORDER BY unit_key),''),'UTF8')),'hex') FROM actual_rows),
 (SELECT encode(sha256(convert_to(coalesce(string_agg(unit_key||':'||raw_sha,E'\n' ORDER BY unit_key),''),'UTF8')),'hex') FROM expected_rows)
 INTO v_mismatches,v_observed_sha,v_expected_sha;
 IF EXISTS(SELECT 1 FROM jsonb_each_text(v_mismatches) mismatch WHERE mismatch.value IS DISTINCT FROM '0')
 THEN RAISE EXCEPTION 'N03_ARCHIVE_SOURCE_RECONCILIATION_FAILED'; END IF;
 RETURN jsonb_build_object('result','PASS_SOURCE_ARCHIVE_ONLY','verifier_revision','n03-source-archive-independent-sql-v1',
  'source_id',v_source,'source_sha256',v_source_sha,'source_bytes',67010,'source_pages',1,'source_occurrences',297,
  'normalized_occurrences',297,'source_value_and_ratio_fields',2079,'unavailable_required_fields',1188,
  'mismatches',v_mismatches,'source_units_manifest_sha256',v_expected_sha,'normalized_units_manifest_sha256',v_observed_sha,
  'independent_provider_economic_accuracy_verified',false,'company_population_verified',false,
  'company_event_denominator_verified',false,'historical_receipt_verified',false,
  'historical_predictor_eligible',false,'full_required_family_validation_pass',false);
END $f$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.n03_archive_publish_v1(p_attempt uuid,p_token text)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path TO 'pg_catalog' SET lock_timeout TO '500ms'
AS $f$
DECLARE v_tid uuid;v_task population_top3000_202608_202609_v1.pipeline_task%rowtype;
 v_validation population_top3000_202608_202609_v1.pipeline_validation%rowtype;
 v_check jsonb;v_measure jsonb;v_reply jsonb;v_manifest jsonb;v_sha text;v_evidence_id uuid;v_hashes jsonb;
BEGIN
 v_tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);
 SELECT * INTO v_task FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=v_tid;
 IF v_task.family_id IS DISTINCT FROM 'N03' OR v_task.stage IS DISTINCT FROM 'PUBLISH'
  OR v_task.partition_key IS DISTINCT FROM 'splits-execution-202608-202609'
  OR v_task.payload->>'publication_purpose' IS DISTINCT FROM 'VERIFIED_SOURCE_ARCHIVE_ONLY'
 THEN RAISE EXCEPTION 'N03_ARCHIVE_PUBLICATION_SCOPE_MISMATCH'; END IF;
 IF encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.pipeline_publish_v1(uuid,text)'::regprocedure),'UTF8')),'hex')
   IS DISTINCT FROM '689d2eb1fb21ce68ae888ff4917945aeadd26d8358bf5cd45cd23a5d9588fbd9'
 THEN RAISE EXCEPTION 'N03_ARCHIVE_PUBLISH_HELPER_REVIEW_REQUIRED'; END IF;
 SELECT jsonb_object_agg(x.name,encode(sha256(convert_to(pg_get_functiondef(x.signature::regprocedure),'UTF8')),'hex'))
 INTO v_hashes FROM (VALUES
  ('SOURCE_CHECK','population_top3000_202608_202609_v1.n03_archive_source_check_v1()'),
  ('PUBLISH','population_top3000_202608_202609_v1.n03_archive_publish_v1(uuid,text)'),
  ('DELIVER','population_top3000_202608_202609_v1.n03_archive_deliver_v1(uuid,text)'),
  ('MEASURE','population_top3000_202608_202609_v1.pipeline_measure_partition_v1(text,text)'),
  ('ASOF','population_top3000_202608_202609_v1.population_read_asof_v1(timestamptz,boolean)')
 ) x(name,signature);
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_adapter registered
  WHERE registered.family_id='N03' AND registered.stage='PUBLISH'
   AND registered.reviewed_test_evidence->'function_sha256'=v_hashes)
 THEN RAISE EXCEPTION 'N03_ARCHIVE_TESTED_DEPENDENCY_HASH_CHANGED'; END IF;
 SELECT q.* INTO v_validation FROM population_top3000_202608_202609_v1.pipeline_partition part
 JOIN population_top3000_202608_202609_v1.pipeline_validation q ON q.validation_id=part.last_validation_id
 WHERE part.family_id='N03' AND part.partition_key=v_task.partition_key
  AND part.immutable_input_versions=v_task.input_versions AND q.input_versions=v_task.input_versions;
 IF NOT FOUND OR v_validation.result IS DISTINCT FROM 'DENOMINATOR_UNRESOLVED'
  OR v_validation.received_units<>297 OR v_validation.duplicate_observed_units<>0
 THEN RAISE EXCEPTION 'N03_ARCHIVE_CURRENT_NONPASS_FAMILY_VALIDATION_REQUIRED'; END IF;
 v_check:=population_top3000_202608_202609_v1.n03_archive_source_check_v1();
 v_measure:=population_top3000_202608_202609_v1.pipeline_measure_partition_v1('N03',v_task.partition_key);
 IF v_measure->>'state' IS DISTINCT FROM 'MEASURED'
  OR v_measure->>'expected_manifest_sha256' IS DISTINCT FROM v_validation.expected_manifest_sha256
  OR v_measure->>'observed_manifest_sha256' IS DISTINCT FROM v_validation.observed_manifest_sha256
 THEN RAISE EXCEPTION 'N03_ARCHIVE_CURRENT_VALIDATION_MANIFEST_CHANGED'; END IF;
 -- This extra check code is archival evidence only, not a replacement for required family checks.
 INSERT INTO population_top3000_202608_202609_v1.pipeline_quality_evidence
  (family_id,partition_key,check_code,check_version,result,verifier_revision,verifier_identity,independent_of_producer,
   input_versions,selection_evidence,discrepancy_count,evidence)
 VALUES('N03',v_task.partition_key,'ARCHIVE_SOURCE_VALUE_RECONCILIATION',v_validation.observed_manifest_sha256,
  'PASS','n03-source-archive-independent-sql-v1','root-reviewed-independent-set-reconstruction',true,
  v_task.input_versions,jsonb_build_object('selection','ALL_297_RETURNED_SOURCE_RECORDS','sampling',false),0,
  v_check||jsonb_build_object('expected_manifest_sha256',v_validation.expected_manifest_sha256,'observed_manifest_sha256',v_validation.observed_manifest_sha256))
 ON CONFLICT(family_id,partition_key,check_code,check_version,verifier_revision) DO NOTHING;
 SELECT evidence_id INTO v_evidence_id FROM population_top3000_202608_202609_v1.pipeline_quality_evidence ev
 WHERE ev.family_id='N03' AND ev.partition_key=v_task.partition_key AND ev.check_code='ARCHIVE_SOURCE_VALUE_RECONCILIATION'
  AND ev.check_version=v_validation.observed_manifest_sha256 AND ev.verifier_revision='n03-source-archive-independent-sql-v1'
  AND ev.result='PASS' AND ev.discrepancy_count=0 AND ev.independent_of_producer
  AND ev.input_versions=v_task.input_versions AND ev.evidence=v_check||jsonb_build_object(
   'expected_manifest_sha256',v_validation.expected_manifest_sha256,'observed_manifest_sha256',v_validation.observed_manifest_sha256);
 IF NOT FOUND THEN RAISE EXCEPTION 'N03_ARCHIVE_SOURCE_CHECK_REPLAY_CONFLICT'; END IF;
 v_reply:=population_top3000_202608_202609_v1.pipeline_publish_v1(p_attempt,p_token);
 IF v_reply->>'state' IS DISTINCT FROM 'COMPLETE' OR v_reply->>'publication_classification' IS DISTINCT FROM 'PROVISIONAL_SOURCE_EVIDENCE'
 THEN RAISE EXCEPTION 'N03_ARCHIVE_PUBLISH_RESULT_CHANGED'; END IF;
 SELECT manifest INTO v_manifest FROM population_top3000_202608_202609_v1.pipeline_publication
 WHERE publication_id=(v_reply->>'publication_id')::uuid FOR UPDATE;
 v_manifest:=v_manifest||jsonb_build_object('publication_purpose','VERIFIED_SOURCE_ARCHIVE_ONLY',
  'source_archive_validation',v_check,'source_archive_quality_evidence_id',v_evidence_id,
  'required_family_validation_result',v_validation.result,'full_required_family_validation_pass',false,
  'company_membership_frozen',false,'historical_predictor_eligible',false,'mission_final_delivery',false,
  'remaining_required_work','Resolve company population, instrument applicability, primary split mechanics and effective/publication timing; preserve the existing required REPAIR hold.');
 v_sha:=encode(sha256(convert_to(v_manifest::text,'UTF8')),'hex');
 UPDATE population_top3000_202608_202609_v1.pipeline_publication SET manifest=v_manifest,manifest_sha256=v_sha
 WHERE publication_id=(v_reply->>'publication_id')::uuid;
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(task_id,attempt_id,event,details)
 VALUES(v_tid,p_attempt,'N03_SOURCE_ARCHIVE_VALIDATION_AND_PUBLICATION',jsonb_build_object(
  'source_archive_validation',v_check,'quality_evidence_id',v_evidence_id,'publication_id',v_reply->'publication_id',
  'family_validation_id',v_validation.validation_id,'family_validation_result',v_validation.result,
  'required_repair_hold_preserved',true,'mission_complete',false));
 RETURN jsonb_set(v_reply,'{commit_receipt,output_sha256}',to_jsonb(v_sha))||jsonb_build_object(
  'source_archive_result','PASS_SOURCE_ARCHIVE_ONLY','required_family_validation_result',v_validation.result,'mission_complete',false);
END $f$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.n03_archive_deliver_v1(p_attempt uuid,p_token text)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path TO 'pg_catalog' SET lock_timeout TO '500ms'
AS $f$
DECLARE v_tid uuid;v_task population_top3000_202608_202609_v1.pipeline_task%rowtype;v_pub uuid;v_count bigint;
BEGIN
 v_tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);
 SELECT * INTO v_task FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=v_tid;
 IF v_task.family_id IS DISTINCT FROM 'N03' OR v_task.stage IS DISTINCT FROM 'DELIVER'
  OR v_task.partition_key IS DISTINCT FROM 'splits-execution-202608-202609'
  OR v_task.payload->>'publication_purpose' IS DISTINCT FROM 'VERIFIED_SOURCE_ARCHIVE_ONLY'
 THEN RAISE EXCEPTION 'N03_ARCHIVE_DELIVERY_SCOPE_MISMATCH'; END IF;
 IF encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.pipeline_deliver_v1(uuid,text)'::regprocedure),'UTF8')),'hex')
   IS DISTINCT FROM '770201f9a08224d332b0d6828d0952bdb4b3af76cb16ddbeaa02c296689412eb'
 THEN RAISE EXCEPTION 'N03_ARCHIVE_DELIVERY_HELPER_REVIEW_REQUIRED'; END IF;
 SELECT member.snapshot_publication_id INTO v_pub FROM population_top3000_202608_202609_v1.pipeline_release_pointer ptr
 JOIN population_top3000_202608_202609_v1.pipeline_publication_member member USING(publication_id)
 JOIN population_top3000_202608_202609_v1.pipeline_publication pub ON pub.publication_id=member.snapshot_publication_id
 WHERE ptr.singleton AND member.family_id='N03' AND member.partition_key=v_task.partition_key
  AND pub.classification='PROVISIONAL_SOURCE_EVIDENCE'
  AND pub.manifest->>'publication_purpose'='VERIFIED_SOURCE_ARCHIVE_ONLY'
  AND pub.manifest#>>'{source_archive_validation,result}'='PASS_SOURCE_ARCHIVE_ONLY'
  AND pub.manifest#>>'{source_archive_validation,source_sha256}'='9609fe84088fc84a6ba2705270806fa06c1450cf460651819d10038be9fc0dfe';
 IF NOT FOUND THEN RAISE EXCEPTION 'N03_ARCHIVE_VERIFIED_PUBLICATION_REQUIRED'; END IF;
 SELECT count(*) INTO v_count FROM population_top3000_202608_202609_v1.population_source_evidence_current_v1
 WHERE family_id='N03' AND partition_key=v_task.partition_key;
 IF v_count<>297 OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_published_observation snap
  WHERE snap.publication_id=v_pub AND snap.family_id='N03' AND (snap.validation_state IS DISTINCT FROM 'DENOMINATOR_UNRESOLVED'
   OR snap.snapshot_sha256 IS DISTINCT FROM encode(sha256(convert_to(snap.observation::text,'UTF8')),'hex')))
 THEN RAISE EXCEPTION 'N03_ARCHIVE_PUBLISHED_SOURCE_COUNT_OR_HASH_CHANGED'; END IF;
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.population_read_asof_v1(clock_timestamp(),false) WHERE family_id='N03')
  OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.population_read_asof_v1(clock_timestamp(),true) WHERE family_id='N03')
 THEN RAISE EXCEPTION 'N03_ARCHIVE_FALSE_HISTORICAL_CONSUMER_ELIGIBILITY'; END IF;
 RETURN population_top3000_202608_202609_v1.pipeline_deliver_v1(p_attempt,p_token);
END $f$;

CREATE OR REPLACE VIEW population_top3000_202608_202609_v1.n03_source_archive_current_v1 AS
SELECT archived.publication_id,archived.classification,archived.unit_key,archived.validation_state,
 archived.observation->>'source_id' source_id,archived.observation->>'source_record_key' provider_event_id,
 archived.observation#>>'{values_json,ticker}' source_ticker,
 (archived.observation->>'effective_date')::date execution_date,
 (archived.observation#>>'{values_json,split_from}')::numeric split_from,
 (archived.observation#>>'{values_json,split_to}')::numeric split_to,
 archived.observation#>>'{values_json,adjustment_type}' adjustment_type,
 (archived.observation#>>'{values_json,share_unit_ratio_candidate}')::numeric share_unit_ratio_candidate,
 archived.observation->'field_state' field_states,archived.snapshot_sha256,
 false company_population_verified,false primary_mechanics_independently_verified,
 false historical_predictor_eligible,false original_historical_receipt_verified
FROM population_top3000_202608_202609_v1.population_source_evidence_current_v1 archived
WHERE archived.family_id='N03' AND archived.partition_key='splits-execution-202608-202609';

REVOKE ALL ON FUNCTION population_top3000_202608_202609_v1.n03_archive_source_check_v1(),
 population_top3000_202608_202609_v1.n03_archive_publish_v1(uuid,text),
 population_top3000_202608_202609_v1.n03_archive_deliver_v1(uuid,text) FROM PUBLIC,anon,authenticated;
GRANT EXECUTE ON FUNCTION population_top3000_202608_202609_v1.n03_archive_source_check_v1(),
 population_top3000_202608_202609_v1.n03_archive_publish_v1(uuid,text),
 population_top3000_202608_202609_v1.n03_archive_deliver_v1(uuid,text) TO postgres,service_role;
REVOKE ALL ON population_top3000_202608_202609_v1.n03_source_archive_current_v1 FROM PUBLIC,anon,authenticated;
GRANT SELECT ON population_top3000_202608_202609_v1.n03_source_archive_current_v1 TO postgres,service_role;
