-- Definitions only. New S01 VALIDATE helper; the running FINRA helper bundle is untouched.
-- Independent source-to-normalized reconciliation is narrower than company completeness or provider accuracy.
CREATE TABLE IF NOT EXISTS population_top3000_202608_202609_v1.finra_source_readback_v1 (
 readback_id text PRIMARY KEY,
 family_id text NOT NULL CHECK(family_id='S01'),
 partition_key text NOT NULL,
 source_id text NOT NULL REFERENCES population_top3000_202608_202609_v1.source_document(source_id),
 source_sha256 text NOT NULL CHECK(source_sha256~'^[0-9a-f]{64}$'),
 input_versions jsonb NOT NULL,
 verifier_sha256 text NOT NULL CHECK(verifier_sha256~'^[0-9a-f]{64}$'),
 source_comparison_result text NOT NULL CHECK(source_comparison_result IN('PASS','FAIL')),
 evidence jsonb NOT NULL,
 evidence_sha256 text NOT NULL CHECK(evidence_sha256~'^[0-9a-f]{64}$'),
 created_by_task uuid NOT NULL REFERENCES population_top3000_202608_202609_v1.pipeline_task(task_id),
 created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
ALTER TABLE population_top3000_202608_202609_v1.finra_source_readback_v1 ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON population_top3000_202608_202609_v1.finra_source_readback_v1 FROM PUBLIC,anon,authenticated,service_role;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.finra_independent_validate_v1(p_attempt uuid,p_token text)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path TO 'pg_catalog' SET TimeZone TO 'UTC' SET lock_timeout TO '500ms'
AS $function$
DECLARE v_scope jsonb;v_task population_top3000_202608_202609_v1.pipeline_task%rowtype;
 v_doc population_top3000_202608_202609_v1.source_document%rowtype;v_part population_top3000_202608_202609_v1.pipeline_partition%rowtype;
 v_date date;v_actual_hash text;v_parser_hash text;v_verifier_hash text;v_last_line bigint;v_header text;v_footer text;
 v_footer_count bigint;v_structure_ok boolean;v_source_ok boolean;v_partition_ok boolean;
 v_rows bigint;v_valid_rows bigint;v_invalid_rows bigint;v_source_duplicates bigint;v_observed_rows bigint;v_observed_duplicates bigint;
 v_missing bigint;v_unexpected bigint;v_different bigint;v_hash_bad bigint;v_zero_total bigint;v_value_zero bigint;
 v_expected_hash text;v_observed_hash text;v_evidence jsonb;v_evidence_hash text;v_readback_id text;v_result text;v_fresh boolean;
 v_started timestamptz:=clock_timestamp();
BEGIN
 -- Calls the unchanged fenced scope guard before any mutation, including STOP and current owner checks.
 v_scope:=population_top3000_202608_202609_v1.finra_scope_v1(p_attempt,p_token,'VALIDATE');
 SELECT * INTO v_task FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=(v_scope->>'task_id')::uuid;
 IF v_task.input_versions->>'scope_sha256' IS DISTINCT FROM '8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd'
 THEN RAISE EXCEPTION 'FINRA_READBACK_SCOPE_HASH_MISMATCH';END IF;
 v_date:=(v_scope->>'session_date')::date;
 SELECT * INTO v_doc FROM population_top3000_202608_202609_v1.source_document WHERE source_id=v_scope->>'source_id';
 IF NOT FOUND OR v_doc.content_text IS NULL OR octet_length(v_doc.content_text)>8388608 THEN
  RETURN jsonb_build_object('state','BLOCKED','blocker_code','FINRA_READBACK_RETAINED_SOURCE_REQUIRED',
   'resumption_condition',jsonb_build_object('source_id',v_scope->>'source_id','required_action','Supply the exact bounded retained FINRA file and receipt. A missing file is never a zero-volume observation.','automatic_retry',false));
 END IF;
 v_actual_hash:=encode(sha256(convert_to(v_doc.content_text,'UTF8')),'hex');
 v_parser_hash:=encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.finra_parse_cnms_v1(text,date)'::regprocedure),'UTF8')),'hex');
 v_verifier_hash:=encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.finra_independent_validate_v1(uuid,text)'::regprocedure),'UTF8')),'hex');
 v_source_ok:=v_doc.source_sha256=v_actual_hash AND v_doc.content_bytes=octet_length(v_doc.content_text)::bigint
  AND v_doc.source_provider='FINRA' AND v_doc.source_uri=v_scope->>'source_uri'
  AND v_doc.source_published_at IS NULL AND v_doc.historical_received_at IS NULL
  AND v_doc.evidence->>'response_body_sha256'=v_actual_hash
  AND v_doc.evidence->>'response_body_bytes'=octet_length(v_doc.content_text)::text
  AND v_doc.evidence->>'session_date'=v_date::text
  AND v_doc.evidence->>'original_historical_receipt_proven'='false'
  AND v_doc.evidence->>'publication_time_verified'='false';
 -- Alternate parser: set-based regex line splitting and anchored six-field matching.
 -- The producer's array/loop parser is never called by this verifier.
 WITH lines AS MATERIALIZED (
  SELECT row_number::bigint source_line,line_text FROM regexp_split_to_table(v_doc.content_text,E'\\r?\\n') WITH ORDINALITY q(line_text,row_number)
 ), last_nonempty AS(SELECT max(source_line) n FROM lines WHERE line_text<>'')
 SELECT z.n,(SELECT line_text FROM lines WHERE source_line=1),(SELECT line_text FROM lines WHERE source_line=z.n)
 INTO v_last_line,v_header,v_footer FROM last_nonempty z;
 v_footer_count:=CASE WHEN v_footer~'^[0-9]{1,7}$' THEN v_footer::bigint END;
 v_structure_ok:=v_header='Date|Symbol|ShortVolume|ShortExemptVolume|TotalVolume|Market'
  AND v_last_line>=2 AND v_footer_count=v_last_line-2 AND v_footer_count<=100000;
 WITH lines AS MATERIALIZED (
  SELECT row_number::bigint source_line,line_text FROM regexp_split_to_table(v_doc.content_text,E'\\r?\\n') WITH ORDINALITY q(line_text,row_number)
  WHERE row_number>1 AND row_number<v_last_line
 ), matched AS MATERIALIZED (
  SELECT source_line,regexp_match(line_text,'^([0-9]{8})[|]([^|]{1,40})[|]([0-9.]{1,40})[|]([0-9.]{1,40})[|]([0-9.]{1,40})[|]([A-Z,]{1,32})$') fields FROM lines
 ), checked AS MATERIALIZED (
  SELECT source_line,fields,coalesce(fields[1]=to_char(v_date,'YYYYMMDD') AND fields[2]!~'[[:cntrl:]]'
   AND fields[3]~'^[0-9]+([.][0-9]+)?$' AND fields[4]~'^[0-9]+([.][0-9]+)?$'
   AND fields[5]~'^[0-9]+([.][0-9]+)?$' AND fields[6]~'^[A-Z]+(,[A-Z]+)*$',false) valid_fields FROM matched
 ), parsed AS MATERIALIZED (
  SELECT source_line,fields[2] symbol,fields[6] market,valid_fields,
   CASE WHEN valid_fields THEN fields[3]::numeric END short_volume,
   CASE WHEN valid_fields THEN fields[4]::numeric END short_exempt_volume,
   CASE WHEN valid_fields THEN fields[5]::numeric END total_volume FROM checked
 ), valid AS MATERIALIZED (
  SELECT *,symbol||':'||market unit_key FROM parsed WHERE valid_fields AND short_volume<=total_volume AND short_exempt_volume<=total_volume
 ), expected_values AS MATERIALIZED (
  SELECT source_line,unit_key,total_volume,short_volume,
   jsonb_build_object('symbol',symbol,'market',market,'session_date',v_date::text,'short_volume',short_volume,
    'short_exempt_volume',short_exempt_volume,'total_volume',total_volume,
    'short_volume_ratio',CASE WHEN total_volume=0 THEN NULL ELSE short_volume/total_volume END) vals,
   jsonb_build_object('symbol','VALUE','market','VALUE','session_date','VALUE','short_volume','VALUE','short_exempt_volume','VALUE','total_volume','VALUE',
    'short_volume_ratio',CASE WHEN total_volume=0 THEN 'NOT_APPLICABLE' ELSE 'VALUE' END) states,
   jsonb_build_object('source_sha256',v_actual_hash,'source_line',source_line,'footer_records',v_footer_count,'parser_sha256',v_parser_hash,
    'historical_symbol_mapping','PENDING','company_universe_frozen',false,'publication_time_verified',false,'original_historical_receipt_proven',false,
    'coverage','FINRA_CNMS_REPORTED_TRADING_ONLY','missing_symbol_meaning','NOT_REPORTED_OR_UNRESOLVED; never synthesize zero',
    'zero_total_ratio_reason',CASE WHEN total_volume=0 THEN 'No reported total volume; ratio undefined' END) lin
  FROM valid
 ), expected AS MATERIALIZED (
  SELECT *,encode(sha256(convert_to(jsonb_build_object('values',vals,'states',states,'lineage',lin)::text,'UTF8')),'hex') expected_record_hash FROM expected_values
 ), observed AS MATERIALIZED (
  SELECT * FROM population_top3000_202608_202609_v1.source_observation
  WHERE release_id='top3000_companies_202608_202609_v1' AND family_id='S01' AND partition_key=v_task.partition_key
 ), compared AS MATERIALIZED (
  SELECT e.unit_key expected_key,o.unit_key observed_key,
   CASE WHEN e.unit_key IS NOT NULL AND o.unit_key IS NOT NULL THEN
    o.release_id IS DISTINCT FROM 'top3000_companies_202608_202609_v1'
    OR o.values_json IS DISTINCT FROM e.vals OR o.field_state IS DISTINCT FROM e.states OR o.lineage IS DISTINCT FROM e.lin
    OR o.source_id IS DISTINCT FROM v_doc.source_id OR o.source_record_key IS DISTINCT FROM e.source_line::text
    OR o.normalization_sha256 IS DISTINCT FROM v_parser_hash OR o.vintage_key IS DISTINCT FROM v_actual_hash
    OR o.effective_date IS DISTINCT FROM v_date OR o.reporting_entity_id IS DISTINCT FROM 'FINRA_CNMS'
    OR o.time_role IS DISTINCT FROM 'TARGET' OR o.available_at IS DISTINCT FROM v_doc.retrieved_at
    OR o.availability_policy IS DISTINCT FROM 'ARCHIVED_OBSERVATION'
    OR o.company_id IS NOT NULL OR o.security_id IS NOT NULL OR o.event_at IS NOT NULL
    OR o.historical_received_at IS NOT NULL OR o.source_published_at IS NOT NULL
    OR o.record_sha256 IS DISTINCT FROM e.expected_record_hash ELSE false END row_diff
  FROM expected e FULL JOIN observed o ON o.unit_key=e.unit_key
 ) SELECT (SELECT count(*) FROM parsed),(SELECT count(*) FROM valid),
  (SELECT count(*) FROM parsed WHERE NOT valid_fields OR short_volume>total_volume OR short_exempt_volume>total_volume),
  (SELECT count(*)-count(DISTINCT unit_key) FROM valid),(SELECT count(*) FROM observed),
  (SELECT count(*)-count(DISTINCT unit_key) FROM observed),
  count(*) FILTER(WHERE expected_key IS NOT NULL AND observed_key IS NULL),
  count(*) FILTER(WHERE expected_key IS NULL AND observed_key IS NOT NULL),count(*) FILTER(WHERE row_diff),
  (SELECT count(*) FROM observed o WHERE o.record_sha256 IS DISTINCT FROM encode(sha256(convert_to(jsonb_build_object('values',o.values_json,'states',o.field_state,'lineage',o.lineage)::text,'UTF8')),'hex')),
  (SELECT count(*) FROM valid WHERE total_volume=0),(SELECT count(*) FROM valid WHERE short_volume=0),
  (SELECT encode(sha256(convert_to(coalesce(string_agg(unit_key||':'||expected_record_hash,E'\n' ORDER BY unit_key,source_line),''),'UTF8')),'hex') FROM expected),
  (SELECT encode(sha256(convert_to(coalesce(string_agg(unit_key||':'||record_sha256,E'\n' ORDER BY unit_key,release_id),''),'UTF8')),'hex') FROM observed)
 INTO v_rows,v_valid_rows,v_invalid_rows,v_source_duplicates,v_observed_rows,v_observed_duplicates,v_missing,v_unexpected,v_different,v_hash_bad,v_zero_total,v_value_zero,v_expected_hash,v_observed_hash FROM compared;
 SELECT * INTO v_part FROM population_top3000_202608_202609_v1.pipeline_partition WHERE family_id='S01' AND partition_key=v_task.partition_key;
 v_partition_ok:=FOUND AND v_part.immutable_input_versions=v_task.input_versions AND v_part.time_role='TARGET'
  AND v_part.denominator_state='PROVISIONAL' AND NOT v_part.publication_eligible
  AND v_part.denominator_evidence->>'source_sha256'=v_actual_hash
  AND v_part.denominator_evidence->>'reported_source_rows'=v_rows::text
  AND v_part.denominator_evidence->>'footer_records'=v_footer_count::text
  AND v_part.denominator_evidence->>'company_universe_frozen'='false'
  AND v_part.denominator_evidence->>'company_mapping_complete'='false'
  AND v_part.denominator_evidence->>'absent_symbols_are_zero'='false';
 v_result:=CASE WHEN coalesce(v_source_ok,false) AND coalesce(v_structure_ok,false) AND coalesce(v_partition_ok,false)
  AND v_rows=v_footer_count AND v_observed_rows=v_rows AND v_invalid_rows+v_source_duplicates+v_observed_duplicates+v_missing+v_unexpected+v_different+v_hash_bad=0
  AND v_expected_hash=v_observed_hash THEN 'PASS' ELSE 'FAIL' END;
 v_evidence:=jsonb_build_object('verification_scope','EXACT_RETAINED_FINRA_SOURCE_TO_NORMALIZED_ROWS','release_id','top3000_companies_202608_202609_v1',
  'alternate_parser','SET_BASED_REGEX_LINES_AND_SIX_FIELD_MATCH_V1','producer_parser_invoked',false,
  'source_integrity_pass',coalesce(v_source_ok,false),'header_footer_pass',coalesce(v_structure_ok,false),'partition_receipt_pass',coalesce(v_partition_ok,false),
  'source_actual_sha256',v_actual_hash,'source_recorded_sha256',v_doc.source_sha256,'source_uri',v_doc.source_uri,
  'reported_source_rows',v_rows,'valid_source_rows',v_valid_rows,'footer_records',v_footer_count,'normalized_rows',v_observed_rows,
  'invalid_source_rows',v_invalid_rows,'duplicate_source_keys',v_source_duplicates,'duplicate_normalized_keys',v_observed_duplicates,
  'missing_normalized_rows',v_missing,'unexpected_normalized_rows',v_unexpected,'different_normalized_rows',v_different,'corrupt_normalized_hashes',v_hash_bad,
  'reported_zero_short_volume_rows',v_value_zero,'reported_zero_total_volume_rows',v_zero_total,
  'zero_total_ratio_policy','NULL_AND_NOT_APPLICABLE','missing_rows_are_zero',false,
  'expected_source_manifest_sha256',v_expected_hash,'observed_manifest_sha256',v_observed_hash,
  'source_comparison_result',v_result,'company_expected_units',NULL,'company_completeness_ratio',NULL,
  'company_denominator_state','UNRESOLVED','company_mapping_verified',false,'independent_provider_accuracy_verified',false,
  'original_historical_receipt_proven',false,'publication_time_verified',false,'publication_eligible',false,
  'input_versions',v_task.input_versions,'verifier_sha256',v_verifier_hash);
 v_evidence_hash:=encode(sha256(convert_to(v_evidence::text,'UTF8')),'hex');
 v_readback_id:='finra-readback:'||v_evidence_hash;
 IF clock_timestamp()-v_started>interval '9500 milliseconds' THEN RAISE EXCEPTION 'FINRA_READBACK_TIME_BOUND';END IF;
 INSERT INTO population_top3000_202608_202609_v1.finra_source_readback_v1
  (readback_id,family_id,partition_key,source_id,source_sha256,input_versions,verifier_sha256,source_comparison_result,evidence,evidence_sha256,created_by_task)
 VALUES(v_readback_id,'S01',v_task.partition_key,v_doc.source_id,v_actual_hash,v_task.input_versions,v_verifier_hash,v_result,v_evidence,v_evidence_hash,v_task.task_id)
 ON CONFLICT(readback_id) DO NOTHING;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.finra_source_readback_v1 WHERE readback_id=v_readback_id
  AND evidence=v_evidence AND evidence_sha256=v_evidence_hash) THEN RAISE EXCEPTION 'FINRA_READBACK_IMMUTABLE_RECEIPT_CONFLICT';END IF;
 v_fresh:=population_top3000_202608_202609_v1.pipeline_record_commit_v1(p_attempt,p_token,
  jsonb_build_object('commit_key',v_readback_id,'output_sha256',v_evidence_hash,'new_committed_units',1,
   'immutable_output_ids',jsonb_build_array(v_readback_id),'input_versions',v_task.input_versions,'unit_kind','PROVISIONAL_SOURCE_READBACK_MANIFEST'));
 IF v_fresh THEN INSERT INTO population_top3000_202608_202609_v1.pipeline_event(task_id,attempt_id,event,details)
  VALUES(v_task.task_id,p_attempt,'FINRA_INDEPENDENT_SOURCE_READBACK_RECORDED',jsonb_build_object('readback_id',v_readback_id,
   'source_comparison_result',v_result,'reported_source_rows',v_rows,'normalized_rows',v_observed_rows,
   'company_denominator_state','UNRESOLVED','publication_eligible',false));END IF;
 RETURN jsonb_build_object('state','BLOCKED','blocker_code',CASE v_result WHEN 'PASS' THEN 'FINRA_COMPANY_DENOMINATOR_AND_PUBLICATION_UNRESOLVED' ELSE 'FINRA_SOURCE_NORMALIZATION_RECONCILIATION_REQUIRED' END,
  'resumption_condition',jsonb_build_object('readback_id',v_readback_id,'source_comparison_result',v_result,
   'required_action',CASE v_result WHEN 'PASS' THEN 'Reconcile dated company/security membership and a defensible company-level applicability denominator; verify publication and historical receipt semantics before a reviewed certification stage.'
    ELSE 'Inspect the retained source and exact discrepancy counts; repair only evidenced mismatches and independently reverify. Preserve missing observations as missing.' END,
   'automatic_retry',false,'new_provider_request_admitted',false,'publication_eligible',false));
END $function$;
REVOKE ALL ON FUNCTION population_top3000_202608_202609_v1.finra_independent_validate_v1(uuid,text) FROM PUBLIC,anon,authenticated;
GRANT EXECUTE ON FUNCTION population_top3000_202608_202609_v1.finra_independent_validate_v1(uuid,text) TO postgres,service_role;
