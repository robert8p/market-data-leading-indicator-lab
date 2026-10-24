-- Zero-HTTP, evidence-only immutable retained CTX07 verification. No source body or numeric value is public.
CREATE TABLE population_top3000_202608_202609_v1.ctx07_source_verification_run_v5 (
 run_id text PRIMARY KEY, observation_sha256 text NOT NULL, registry_sha256 text NOT NULL,
 input_versions jsonb NOT NULL, first_attempt_id uuid NOT NULL, created_at timestamptz NOT NULL DEFAULT clock_timestamp());
CREATE TABLE population_top3000_202608_202609_v1.ctx07_source_verification_unit_v5 (
 run_id text NOT NULL REFERENCES population_top3000_202608_202609_v1.ctx07_source_verification_run_v5, unit_key text NOT NULL,
 evidence jsonb NOT NULL, evidence_sha256 text NOT NULL, first_attempt_id uuid NOT NULL,
 PRIMARY KEY(run_id,unit_key), CHECK(evidence_sha256=encode(sha256(convert_to(evidence::text,'UTF8')),'hex')));
CREATE VIEW population_top3000_202608_202609_v1.ctx07_source_verification_projection_v5 WITH(security_invoker=true) AS
WITH expected(factor_name,ref_date,release_date,scheduled_at,role) AS (VALUES ('job_openings_millions_as_released','2026-05-01'::date,'2026-06-30'::date,'2026-06-30T14:00:00+00:00'::timestamptz,'SUPPORT'),
('ism_manufacturing_pmi_as_released','2026-06-01'::date,'2026-07-01'::date,'2026-07-01T14:00:00+00:00'::timestamptz,'SUPPORT'),
('average_hourly_earnings_as_released','2026-06-01'::date,'2026-07-02'::date,'2026-07-02T12:30:00+00:00'::timestamptz,'SUPPORT'),
('labor_force_participation_as_released','2026-06-01'::date,'2026-07-02'::date,'2026-07-02T12:30:00+00:00'::timestamptz,'SUPPORT'),
('unemployment_rate_as_released','2026-06-01'::date,'2026-07-02'::date,'2026-07-02T12:30:00+00:00'::timestamptz,'SUPPORT'),
('ism_services_pmi_as_released','2026-06-01'::date,'2026-07-06'::date,'2026-07-06T14:00:00+00:00'::timestamptz,'SUPPORT'),
('cpi_core_index_nsa_as_released','2026-06-01'::date,'2026-07-14'::date,'2026-07-14T12:30:00+00:00'::timestamptz,'SUPPORT'),
('cpi_core_mom_as_released','2026-06-01'::date,'2026-07-14'::date,'2026-07-14T12:30:00+00:00'::timestamptz,'SUPPORT'),
('cpi_core_yoy_as_released','2026-06-01'::date,'2026-07-14'::date,'2026-07-14T12:30:00+00:00'::timestamptz,'SUPPORT'),
('cpi_index_nsa_as_released','2026-06-01'::date,'2026-07-14'::date,'2026-07-14T12:30:00+00:00'::timestamptz,'SUPPORT'),
('cpi_mom_as_released','2026-06-01'::date,'2026-07-14'::date,'2026-07-14T12:30:00+00:00'::timestamptz,'SUPPORT'),
('cpi_yoy_as_released','2026-06-01'::date,'2026-07-14'::date,'2026-07-14T12:30:00+00:00'::timestamptz,'SUPPORT'),
('retail_sales_billion_as_released','2026-06-01'::date,'2026-07-16'::date,'2026-07-16T12:30:00+00:00'::timestamptz,'SUPPORT'),
('retail_sales_mom_as_released','2026-06-01'::date,'2026-07-16'::date,'2026-07-16T12:30:00+00:00'::timestamptz,'SUPPORT'),
('pce_core_mom_as_released','2026-06-01'::date,'2026-07-30'::date,'2026-07-30T12:30:00+00:00'::timestamptz,'SUPPORT'),
('pce_core_yoy_as_released','2026-06-01'::date,'2026-07-30'::date,'2026-07-30T12:30:00+00:00'::timestamptz,'SUPPORT'),
('pce_mom_as_released','2026-06-01'::date,'2026-07-30'::date,'2026-07-30T12:30:00+00:00'::timestamptz,'SUPPORT'),
('pce_spending_mom_as_released','2026-06-01'::date,'2026-07-30'::date,'2026-07-30T12:30:00+00:00'::timestamptz,'SUPPORT'),
('pce_yoy_as_released','2026-06-01'::date,'2026-07-30'::date,'2026-07-30T12:30:00+00:00'::timestamptz,'SUPPORT'),
('real_gdp_annualized_change_as_released','2026-04-01'::date,'2026-07-30'::date,'2026-07-30T12:30:00+00:00'::timestamptz,'SUPPORT'),
('real_pce_spending_mom_as_released','2026-06-01'::date,'2026-07-30'::date,'2026-07-30T12:30:00+00:00'::timestamptz,'SUPPORT'),
('ism_manufacturing_pmi_as_released','2026-07-01'::date,'2026-08-03'::date,'2026-08-03T14:00:00+00:00'::timestamptz,'TARGET'),
('job_openings_millions_as_released','2026-06-01'::date,'2026-08-04'::date,'2026-08-04T14:00:00+00:00'::timestamptz,'TARGET'),
('ism_services_pmi_as_released','2026-07-01'::date,'2026-08-05'::date,'2026-08-05T14:00:00+00:00'::timestamptz,'TARGET'),
('average_hourly_earnings_as_released','2026-07-01'::date,'2026-08-07'::date,'2026-08-07T12:30:00+00:00'::timestamptz,'TARGET'),
('labor_force_participation_as_released','2026-07-01'::date,'2026-08-07'::date,'2026-08-07T12:30:00+00:00'::timestamptz,'TARGET'),
('unemployment_rate_as_released','2026-07-01'::date,'2026-08-07'::date,'2026-08-07T12:30:00+00:00'::timestamptz,'TARGET'),
('cpi_core_index_nsa_as_released','2026-07-01'::date,'2026-08-12'::date,'2026-08-12T12:30:00+00:00'::timestamptz,'TARGET'),
('cpi_core_mom_as_released','2026-07-01'::date,'2026-08-12'::date,'2026-08-12T12:30:00+00:00'::timestamptz,'TARGET'),
('cpi_core_yoy_as_released','2026-07-01'::date,'2026-08-12'::date,'2026-08-12T12:30:00+00:00'::timestamptz,'TARGET'),
('cpi_index_nsa_as_released','2026-07-01'::date,'2026-08-12'::date,'2026-08-12T12:30:00+00:00'::timestamptz,'TARGET'),
('cpi_mom_as_released','2026-07-01'::date,'2026-08-12'::date,'2026-08-12T12:30:00+00:00'::timestamptz,'TARGET'),
('cpi_yoy_as_released','2026-07-01'::date,'2026-08-12'::date,'2026-08-12T12:30:00+00:00'::timestamptz,'TARGET'),
('retail_sales_billion_as_released','2026-07-01'::date,'2026-08-14'::date,'2026-08-14T12:30:00+00:00'::timestamptz,'TARGET'),
('retail_sales_mom_as_released','2026-07-01'::date,'2026-08-14'::date,'2026-08-14T12:30:00+00:00'::timestamptz,'TARGET'),
('pce_core_mom_as_released','2026-07-01'::date,'2026-08-26'::date,'2026-08-26T12:30:00+00:00'::timestamptz,'TARGET'),
('pce_core_yoy_as_released','2026-07-01'::date,'2026-08-26'::date,'2026-08-26T12:30:00+00:00'::timestamptz,'TARGET'),
('pce_mom_as_released','2026-07-01'::date,'2026-08-26'::date,'2026-08-26T12:30:00+00:00'::timestamptz,'TARGET'),
('pce_spending_mom_as_released','2026-07-01'::date,'2026-08-26'::date,'2026-08-26T12:30:00+00:00'::timestamptz,'TARGET'),
('pce_yoy_as_released','2026-07-01'::date,'2026-08-26'::date,'2026-08-26T12:30:00+00:00'::timestamptz,'TARGET'),
('real_gdp_annualized_change_as_released','2026-04-01'::date,'2026-08-26'::date,'2026-08-26T12:30:00+00:00'::timestamptz,'TARGET'),
('real_pce_spending_mom_as_released','2026-07-01'::date,'2026-08-26'::date,'2026-08-26T12:30:00+00:00'::timestamptz,'TARGET'),
('ism_manufacturing_pmi_as_released','2026-08-01'::date,'2026-09-01'::date,'2026-09-01T14:00:00+00:00'::timestamptz,'TARGET'),
('job_openings_millions_as_released','2026-07-01'::date,'2026-09-01'::date,'2026-09-01T14:00:00+00:00'::timestamptz,'TARGET'),
('ism_services_pmi_as_released','2026-08-01'::date,'2026-09-03'::date,'2026-09-03T14:00:00+00:00'::timestamptz,'TARGET'),
('average_hourly_earnings_as_released','2026-08-01'::date,'2026-09-04'::date,'2026-09-04T12:30:00+00:00'::timestamptz,'TARGET'),
('labor_force_participation_as_released','2026-08-01'::date,'2026-09-04'::date,'2026-09-04T12:30:00+00:00'::timestamptz,'TARGET'),
('unemployment_rate_as_released','2026-08-01'::date,'2026-09-04'::date,'2026-09-04T12:30:00+00:00'::timestamptz,'TARGET'),
('cpi_core_index_nsa_as_released','2026-08-01'::date,'2026-09-11'::date,'2026-09-11T12:30:00+00:00'::timestamptz,'TARGET'),
('cpi_core_mom_as_released','2026-08-01'::date,'2026-09-11'::date,'2026-09-11T12:30:00+00:00'::timestamptz,'TARGET'),
('cpi_core_yoy_as_released','2026-08-01'::date,'2026-09-11'::date,'2026-09-11T12:30:00+00:00'::timestamptz,'TARGET'),
('cpi_index_nsa_as_released','2026-08-01'::date,'2026-09-11'::date,'2026-09-11T12:30:00+00:00'::timestamptz,'TARGET'),
('cpi_mom_as_released','2026-08-01'::date,'2026-09-11'::date,'2026-09-11T12:30:00+00:00'::timestamptz,'TARGET'),
('cpi_yoy_as_released','2026-08-01'::date,'2026-09-11'::date,'2026-09-11T12:30:00+00:00'::timestamptz,'TARGET'),
('retail_sales_billion_as_released','2026-08-01'::date,'2026-09-16'::date,'2026-09-16T12:30:00+00:00'::timestamptz,'TARGET'),
('retail_sales_mom_as_released','2026-08-01'::date,'2026-09-16'::date,'2026-09-16T12:30:00+00:00'::timestamptz,'TARGET'),
('job_openings_millions_as_released','2026-08-01'::date,'2026-09-29'::date,'2026-09-29T14:00:00+00:00'::timestamptz,'TARGET'),
('pce_core_mom_as_released','2026-08-01'::date,'2026-09-30'::date,'2026-09-30T12:30:00+00:00'::timestamptz,'TARGET'),
('pce_core_yoy_as_released','2026-08-01'::date,'2026-09-30'::date,'2026-09-30T12:30:00+00:00'::timestamptz,'TARGET'),
('pce_mom_as_released','2026-08-01'::date,'2026-09-30'::date,'2026-09-30T12:30:00+00:00'::timestamptz,'TARGET'),
('pce_spending_mom_as_released','2026-08-01'::date,'2026-09-30'::date,'2026-09-30T12:30:00+00:00'::timestamptz,'TARGET'),
('pce_yoy_as_released','2026-08-01'::date,'2026-09-30'::date,'2026-09-30T12:30:00+00:00'::timestamptz,'TARGET'),
('real_gdp_annualized_change_as_released','2026-04-01'::date,'2026-09-30'::date,'2026-09-30T12:30:00+00:00'::timestamptz,'TARGET'),
('real_pce_spending_mom_as_released','2026-08-01'::date,'2026-09-30'::date,'2026-09-30T12:30:00+00:00'::timestamptz,'TARGET')),
 selected AS (
 SELECT e.factor_name||'|'||e.ref_date||'|'||e.release_date unit_key,e.role,e.scheduled_at,o.* FROM expected e
 LEFT JOIN population_top3000_202608_202609_v1.macro_observations o ON o.factor_name=e.factor_name
 AND (o.reference_period AT TIME ZONE 'America/New_York')::date=e.ref_date
 AND (o.released_at AT TIME ZONE 'America/New_York')::date=e.release_date AND o.record_role<>'revision'
 UNION ALL SELECT 'revision|'||o.observation_id,'RETAINED_REVISION',NULL::timestamptz,o.* FROM population_top3000_202608_202609_v1.macro_observations o WHERE record_role='revision'
)
SELECT o.unit_key,jsonb_build_object('observation_id',o.observation_id,'population_role',o.role,
 'factor_name',o.factor_name,'reference_period',o.reference_period,'record_role',o.record_role,
 'retained_numeric',o.actual,'units',o.units,'scheduled_at',o.scheduled_at,'retained_release_at',o.released_at,
 'retained_available_at',o.available_at,'actual_historical_receipt',o.actual_historical_receipt,
 'source_current_retrieved_at',r.received_at,'original_source_sha256',o.source_content_sha256,
 'observation_payload_sha256',o.payload_sha256,'registry_payload_sha256',r.payload_sha256,
 'exact_body_sha256',b.body_sha,'body_location',b.location,
 'status',CASE WHEN o.observation_id IS NULL THEN 'OBSERVATION_UNAVAILABLE'
 WHEN b.body_sha IS NULL THEN 'SOURCE_BODY_UNAVAILABLE' ELSE 'SOURCE_BODY_AVAILABLE_EXTRACTION_UNVERIFIED' END,
 'numeric_status',CASE WHEN o.actual IS NULL THEN 'NUMERIC_VALUE_UNAVAILABLE' ELSE 'SOURCE_NUMERICAL_COMPARISON_UNVERIFIED' END,
 'source_location_verified',false,'calendar_status','PINNED_SCHEDULE_NOT_INDEPENDENTLY_REVERIFIED',
 'publication_header_verified',false,'strict_original_receipt_qualified',false,'all_vintages_accepted',false,
 'asof_eligible',false,'source_complete',false) evidence
FROM selected o LEFT JOIN population_top3000_202608_202609_v1.macro_source_registry r ON r.source_id=o.source_id
LEFT JOIN LATERAL (
 SELECT body_sha,location FROM (
 SELECT encode(sha256(convert_to(d.content_text,'UTF8')),'hex') body_sha,'source_document:'||d.source_id location
 FROM population_top3000_202608_202609_v1.source_document d WHERE d.source_sha256=o.source_content_sha256 AND d.content_text IS NOT NULL
 UNION ALL SELECT encode(sha256(convert_to(a.raw_text,'UTF8')),'hex'),'acquisition_raw:'||a.job_id
 FROM population_top3000_202608_202609_v1.acquisition_raw a WHERE a.sha256=o.source_content_sha256 AND a.raw_text IS NOT NULL
 ) raw WHERE body_sha=o.source_content_sha256 ORDER BY location LIMIT 1
) b ON true;
CREATE FUNCTION population_top3000_202608_202609_v1.ctx07_source_verification_stage_v5(p_attempt uuid,p_token text) RETURNS jsonb LANGUAGE plpgsql SET search_path=pg_catalog AS $f$
DECLARE tid uuid;t population_top3000_202608_202609_v1.pipeline_task%rowtype;a population_top3000_202608_202609_v1.acquisition_control%rowtype;p population_top3000_202608_202609_v1.pipeline_control%rowtype;
 oh text;rh text;out_hash text;n integer;fresh integer:=0;rr population_top3000_202608_202609_v1.ctx07_source_verification_run_v5%rowtype;
BEGIN
 PERFORM pg_advisory_xact_lock(2026083000261022);
 tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);
 SELECT * INTO STRICT t FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=tid;
 SELECT * INTO STRICT a FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton;
 SELECT * INTO STRICT p FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton;
 IF a.owner_id IS DISTINCT FROM 'top3000-completion-20261024-root-5db48b32d103' OR a.owner_fence IS DISTINCT FROM 3 OR a.stop_generation IS DISTINCT FROM 1
 OR a.admitted_until IS NULL OR p.deadline IS NULL OR p.tick_count>=p.max_ticks
 OR p.budget_evidence#>>'{monetary_policy,authority_event_id}' IS DISTINCT FROM '9528'
 OR population_top3000_202608_202609_v1.pipeline_cost_admission_v2(p,0) IS DISTINCT FROM true THEN RAISE EXCEPTION 'CTX07_AUTHORITY';END IF;
 IF t.family_id IS DISTINCT FROM 'CTX07' OR t.partition_key IS DISTINCT FROM 'ctx07-retained-numerical-vintage-evidence-v5'
 OR t.payload IS DISTINCT FROM '{"kind":"CTX07_SOURCE_VERIFICATION_V5"}'::jsonb
 OR t.input_versions->>'scope_sha256' IS DISTINCT FROM a.scope_sha256
 OR t.input_versions->>'assignment_event_id' IS DISTINCT FROM '14379'
 OR t.input_versions->>'calendar_sha256' IS DISTINCT FROM 'b7efa4d83ac8ee9adcd5cdefe6e3dba6b3cff9c5ea0cca1e55a794ee69e11f1a'
 OR t.input_versions->>'helper_sha256' IS DISTINCT FROM encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.ctx07_source_verification_stage_v5(uuid,text)'::regprocedure),'UTF8')),'hex')
 OR t.input_versions->>'projection_sha256' IS DISTINCT FROM encode(sha256(convert_to(pg_get_viewdef('population_top3000_202608_202609_v1.ctx07_source_verification_projection_v5'::regclass,true),'UTF8')),'hex')
 THEN RAISE EXCEPTION 'CTX07_INPUT_CODE_PIN';END IF;
 SELECT encode(sha256(convert_to(string_agg(to_jsonb(o)::text,E'\n' ORDER BY observation_id),'UTF8')),'hex') INTO oh FROM population_top3000_202608_202609_v1.macro_observations o;
 SELECT encode(sha256(convert_to(string_agg(to_jsonb(r)::text,E'\n' ORDER BY source_id),'UTF8')),'hex') INTO rh FROM population_top3000_202608_202609_v1.macro_source_registry r;
 IF oh IS DISTINCT FROM '9be383bf89f79c454d0ca41bb4c4e1228649134612ba3766cfcae0004aa69dda'
 OR rh IS DISTINCT FROM '04095c8a9fe311bf84250de03d21a8391f2aa93c02bdf87769a6f176367e8785' THEN RAISE EXCEPTION 'CTX07_SOURCE_PREIMAGE_CHANGED';END IF;
 IF EXISTS(WITH expected(item_key,sha) AS(VALUES ('average_hourly_earnings_as_released','1b04cb87594f6b9ae15c0eed9709388b45c54dd7fb06565857247985df5f0c90'),('cpi_core_index_nsa_as_released','862d1813f740fe6be01e5923399ea76da1e2abbd7f7c305d2768938c22b05dfe'),('cpi_core_mom_as_released','9995da16b4f707a80acdb74e12b9021f6e45dde117b60c476b7f59e9ba70b021'),('cpi_core_yoy_as_released','b407a5fef35052db90e8e7a4c0833545e55bf95a78fb47348b65dfb4f1590889'),('cpi_index_nsa_as_released','89ded6cc8f4082429ec42e0018b577a827fb2b9bbc181b513f9d3f28bbcdf639'),('cpi_mom_as_released','6a4fdb6b08ee73804d23a78fffc63351333f076853233413dae9b3aec6bec359'),('cpi_yoy_as_released','2d7d1703761097541f46082214f8532058163c13e97f12e39081b8aaf5b35b39'),('ism_manufacturing_pmi_as_released','52ffce9b657533babf1fbf08324403d2a3dcc2e398fa62bbcc9170ec9cfaaec0'),('ism_services_pmi_as_released','293cb7b00f0a8173f7a427a4cb25981fef8882d6d54d67bc739c08d391caece2'),('job_openings_millions_as_released','7ff6f2bbe6a991732122ff81b3817a2f7ac224c446e1a8908056a3b0cc6a97f1'),('labor_force_participation_as_released','374cc89b57eab70b9867a8b2ee7d5e5f820b08098857bf0ac25972c530dde2c9'),('pce_core_mom_as_released','e8e48f5e78d77b2b829318e1bb642476ab0489f8675fa951d775b8b4633bd199'),('pce_core_yoy_as_released','d2fcc63a99e3fcec60120daaf40672b20fef0f733d29fb3035d06ba381306eb0'),('pce_mom_as_released','c2f73763a317df43313fe3d926d0b5abd75d1fde4e5e1a5ab36237cb661788f9'),('pce_spending_mom_as_released','6024e5e70ce023910da3517c7b88188c856d728e588b13083a4d0cd117dae922'),('pce_yoy_as_released','b0b39e368f2841798de5a28a63b78efed824efd97e87952e83fd4042890d4063'),('real_gdp_annualized_change_as_released','c31c3c027a2d56a60db3d090a560e4f77e7c83875e4de229bf68bd9becc9e99e'),('real_pce_spending_mom_as_released','1784a61bbd00606d65187255bb9688a8cfba70201018a1d342ae3e133e8a7854'),('retail_sales_billion_as_released','b860abae0a421dbd54a68a7c32f952c96645a9ae11681c06f12860c47c44ff19'),('retail_sales_mom_as_released','0f261c6a32a894da135c25ef6370d09ca2db19e506c71a9f54c6123c81bfe1e6'),('unemployment_rate_as_released','1eb06a6c64370ec2799ac706a4f5c58eed68e939e78738c232d189c8f67b810c')) SELECT 1 FROM expected e FULL JOIN
 (SELECT * FROM population_top3000_202608_202609_v1.catalog_dictionary WHERE family_id='CTX07' AND scope_id='corrected_company_scope_8e6ca1178f03822c') d USING(item_key)
 WHERE e.sha IS DISTINCT FROM d.payload_sha256 OR e.sha IS DISTINCT FROM encode(sha256(convert_to(d.payload_text,'UTF8')),'hex')) THEN RAISE EXCEPTION 'CTX07_DICTIONARY_CHANGED';END IF;
 IF (SELECT count(*) FROM population_top3000_202608_202609_v1.ctx07_source_verification_projection_v5)<>79
 OR (SELECT count(DISTINCT unit_key) FROM population_top3000_202608_202609_v1.ctx07_source_verification_projection_v5)<>79
 OR (SELECT count(*) FROM population_top3000_202608_202609_v1.ctx07_source_verification_projection_v5 WHERE evidence->>'population_role'='TARGET')<>43
 OR (SELECT count(*) FROM population_top3000_202608_202609_v1.ctx07_source_verification_projection_v5 WHERE evidence->>'population_role'='SUPPORT')<>21
 OR (SELECT count(*) FROM population_top3000_202608_202609_v1.ctx07_source_verification_projection_v5 WHERE evidence->>'population_role'='RETAINED_REVISION')<>15
 THEN RAISE EXCEPTION 'CTX07_EXACT_POPULATION';END IF;
 PERFORM population_top3000_202608_202609_v1.d02_daily_resource_guard_v2();
 IF pg_total_relation_size('population_top3000_202608_202609_v1.ctx07_source_verification_unit_v5')+pg_total_relation_size('population_top3000_202608_202609_v1.ctx07_source_verification_run_v5')>16777216 THEN RAISE EXCEPTION 'CTX07_STORAGE_BOUND';END IF;
 IF t.stage='ACQUIRE' THEN
 INSERT INTO population_top3000_202608_202609_v1.ctx07_source_verification_run_v5 VALUES(t.partition_key,oh,rh,t.input_versions,p_attempt,clock_timestamp()) ON CONFLICT DO NOTHING;
 GET DIAGNOSTICS fresh=ROW_COUNT;
 SELECT * INTO STRICT rr FROM population_top3000_202608_202609_v1.ctx07_source_verification_run_v5 WHERE run_id=t.partition_key;
 IF rr.observation_sha256 IS DISTINCT FROM oh OR rr.registry_sha256 IS DISTINCT FROM rh OR rr.input_versions IS DISTINCT FROM t.input_versions THEN RAISE EXCEPTION 'CTX07_RUN_IMMUTABLE_CONFLICT';END IF;
 out_hash:=encode(sha256(convert_to(oh||rh,'UTF8')),'hex');
 ELSIF t.stage IN('NORMALIZE','DERIVE','VALIDATE') THEN
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.ctx07_source_verification_run_v5 WHERE run_id=t.partition_key AND input_versions=t.input_versions AND observation_sha256=oh AND registry_sha256=rh) THEN RAISE EXCEPTION 'CTX07_RUN_REQUIRED';END IF;
 IF t.stage='NORMALIZE' THEN
 INSERT INTO population_top3000_202608_202609_v1.ctx07_source_verification_unit_v5 SELECT t.partition_key,unit_key,evidence,encode(sha256(convert_to(evidence::text,'UTF8')),'hex'),p_attempt FROM population_top3000_202608_202609_v1.ctx07_source_verification_projection_v5 ON CONFLICT DO NOTHING;
 GET DIAGNOSTICS fresh=ROW_COUNT;
 END IF;
 IF (SELECT count(*) FROM population_top3000_202608_202609_v1.ctx07_source_verification_unit_v5 WHERE run_id=t.partition_key)<>79 OR EXISTS(
 SELECT 1 FROM population_top3000_202608_202609_v1.ctx07_source_verification_projection_v5 v FULL JOIN (SELECT * FROM population_top3000_202608_202609_v1.ctx07_source_verification_unit_v5 WHERE run_id=t.partition_key) u USING(unit_key)
 WHERE v.evidence IS DISTINCT FROM u.evidence OR u.evidence_sha256 IS DISTINCT FROM encode(sha256(convert_to(v.evidence::text,'UTF8')),'hex')) THEN RAISE EXCEPTION 'CTX07_NUMERIC_PROJECTION_CHANGED';END IF;
 SELECT encode(sha256(convert_to(string_agg(evidence_sha256,E'\n' ORDER BY unit_key),'UTF8')),'hex') INTO out_hash FROM population_top3000_202608_202609_v1.ctx07_source_verification_unit_v5 WHERE run_id=t.partition_key;
 IF t.stage='VALIDATE' THEN RETURN jsonb_build_object('state','BLOCKED','blocker_code','CTX07_SOURCE_NUMERICAL_VINTAGE_ACCEPTANCE_REQUIRED','resumption_condition',jsonb_build_object('run_id',t.partition_key,'units',79,'source_complete',false,'all_vintage_denominator_complete',false,'strict_original_receipt_qualified',false,'next_action','Recover exact original bodies and independently verify source locations, numeric lexemes, periods, vintages and publication evidence; root acceptance required.'));END IF;
 ELSE RAISE EXCEPTION 'CTX07_STAGE';END IF;
 PERFORM population_top3000_202608_202609_v1.d02_daily_resource_guard_v2();
 IF pg_total_relation_size('population_top3000_202608_202609_v1.ctx07_source_verification_unit_v5')+pg_total_relation_size('population_top3000_202608_202609_v1.ctx07_source_verification_run_v5')>16777216 THEN RAISE EXCEPTION 'CTX07_STORAGE_BOUND';END IF;
 RETURN jsonb_build_object('state','COMPLETE','commit_receipt',jsonb_build_object('commit_key','ctx07-source-verification-v5:'||t.stage,'output_sha256',out_hash,'new_committed_units',fresh,'immutable_output_ids',jsonb_build_array(t.partition_key),'input_versions',t.input_versions,'required_units',79,'source_complete',false,'strict_original_receipt_qualified',false));
END;$f$;
CREATE FUNCTION population_top3000_202608_202609_v1.ctx07_source_verification_acquire_v5(p_attempt uuid,p_token text) RETURNS jsonb LANGUAGE plpgsql SET search_path=pg_catalog AS $f$
DECLARE tid uuid;k text;BEGIN
 tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);
 SELECT payload->>'kind' INTO k FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=tid;
 IF k='CTX07_SOURCE_VERIFICATION_V5' THEN RETURN population_top3000_202608_202609_v1.ctx07_source_verification_stage_v5(p_attempt,p_token);END IF;
 RETURN population_top3000_202608_202609_v1.macro_cached_acquire_v1(p_attempt,p_token);END;$f$;
REVOKE ALL ON FUNCTION population_top3000_202608_202609_v1.ctx07_source_verification_acquire_v5(uuid,text) FROM PUBLIC,anon,authenticated;
CREATE FUNCTION population_top3000_202608_202609_v1.ctx07_source_verification_normalize_v5(p_attempt uuid,p_token text) RETURNS jsonb LANGUAGE plpgsql SET search_path=pg_catalog AS $f$
DECLARE tid uuid;k text;BEGIN
 tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);
 SELECT payload->>'kind' INTO k FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=tid;
 IF k='CTX07_SOURCE_VERIFICATION_V5' THEN RETURN population_top3000_202608_202609_v1.ctx07_source_verification_stage_v5(p_attempt,p_token);END IF;
 RETURN population_top3000_202608_202609_v1.macro_normalize_v1(p_attempt,p_token);END;$f$;
REVOKE ALL ON FUNCTION population_top3000_202608_202609_v1.ctx07_source_verification_normalize_v5(uuid,text) FROM PUBLIC,anon,authenticated;
CREATE FUNCTION population_top3000_202608_202609_v1.ctx07_source_verification_derive_v5(p_attempt uuid,p_token text) RETURNS jsonb LANGUAGE plpgsql SET search_path=pg_catalog AS $f$
DECLARE tid uuid;k text;BEGIN
 tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);
 SELECT payload->>'kind' INTO k FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=tid;
 IF k='CTX07_SOURCE_VERIFICATION_V5' THEN RETURN population_top3000_202608_202609_v1.ctx07_source_verification_stage_v5(p_attempt,p_token);END IF;
 RETURN population_top3000_202608_202609_v1.macro_derive_v1(p_attempt,p_token);END;$f$;
REVOKE ALL ON FUNCTION population_top3000_202608_202609_v1.ctx07_source_verification_derive_v5(uuid,text) FROM PUBLIC,anon,authenticated;
CREATE FUNCTION population_top3000_202608_202609_v1.ctx07_source_verification_validate_v5(p_attempt uuid,p_token text) RETURNS jsonb LANGUAGE plpgsql SET search_path=pg_catalog AS $f$
DECLARE tid uuid;k text;BEGIN
 tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);
 SELECT payload->>'kind' INTO k FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=tid;
 IF k='CTX07_SOURCE_VERIFICATION_V5' THEN RETURN population_top3000_202608_202609_v1.ctx07_source_verification_stage_v5(p_attempt,p_token);END IF;
 RETURN population_top3000_202608_202609_v1.pipeline_validate_v1(p_attempt,p_token);END;$f$;
REVOKE ALL ON FUNCTION population_top3000_202608_202609_v1.ctx07_source_verification_validate_v5(uuid,text) FROM PUBLIC,anon,authenticated;
REVOKE ALL ON FUNCTION population_top3000_202608_202609_v1.ctx07_source_verification_stage_v5(uuid,text) FROM PUBLIC,anon,authenticated;
REVOKE ALL ON population_top3000_202608_202609_v1.ctx07_source_verification_run_v5,population_top3000_202608_202609_v1.ctx07_source_verification_unit_v5,population_top3000_202608_202609_v1.ctx07_source_verification_projection_v5 FROM PUBLIC,anon,authenticated;
