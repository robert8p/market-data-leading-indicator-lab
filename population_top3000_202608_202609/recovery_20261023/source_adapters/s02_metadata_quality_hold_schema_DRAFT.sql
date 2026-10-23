-- ROOT ONLY. Isolated VALIDATE-only helper; no source-bundle changes and no binding/admission here.
BEGIN;
SET LOCAL statement_timeout='8s';SET LOCAL lock_timeout='500ms';
DO $guard$ BEGIN 
 IF NOT pg_try_advisory_xact_lock(2026083000261022) THEN RAISE EXCEPTION 'S02_METADATA_CONTROLLER_BUSY';END IF;
 PERFORM 1 FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton FOR UPDATE;
 PERFORM 1 FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton FOR UPDATE;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.acquisition_control a JOIN population_top3000_202608_202609_v1.pipeline_control p USING(singleton)
 WHERE a.singleton AND a.owner_id='top3000-recovery-20261023-root-4b1314bcb5c2' AND a.owner_fence=2 AND a.stop_generation=0
 AND a.owner_id=p.expected_owner_id AND a.owner_fence=p.expected_owner_fence AND a.stop_generation=p.expected_stop_generation
 AND a.scope_sha256='8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd' AND p.scope_sha256=a.scope_sha256 AND a.enabled AND NOT a.stopped
 AND a.hold_reason IS NULL AND a.admitted_until>clock_timestamp() AND a.reserved_bytes=0
 AND p.enabled AND p.shared_scheduler_verified AND p.budget_verified_at IS NOT NULL AND p.deadline>clock_timestamp() AND p.tick_count<p.max_ticks
 AND p.actual_usd+p.accrued_usd+p.unavoidable_usd+p.reserved_request_usd+p.repair_egress_reserve_usd+p.remaining_uncommitted_usd<=p.budget_cap_usd)
 THEN RAISE EXCEPTION 'S02_METADATA_OWNER_STOP_SCOPE_OR_ADMISSION_CHANGED';END IF;
 IF NOT EXISTS(SELECT 1 FROM cron.job WHERE jobid=596 AND NOT active AND database=current_database()
 AND btrim(command)='select population_top3000_202608_202609_v1.pipeline_tick_v1();') THEN RAISE EXCEPTION 'S02_METADATA_BOUND_CRON_MUST_BE_PAUSED';END IF;
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE status='LEASED')
 OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_provider_lease WHERE released_at IS NULL) THEN RAISE EXCEPTION 'S02_METADATA_EXISTING_LEASES_MUST_DRAIN';END IF;
 IF population_top3000_202608_202609_v1.massive_shortinterest_bundle_sha256_v1() IS DISTINCT FROM '18f46852bf04cd495613e333c2fcedc9df66c9e3e3c68600070d3d77314ad15b' THEN RAISE EXCEPTION 'S02_METADATA_OLD_PRODUCER_BUNDLE_CHANGED';END IF;
 END $guard$;
CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.massive_shortinterest_metadata_quality_hold_v1(p_attempt uuid,p_token text)
RETURNS jsonb LANGUAGE plpgsql SET search_path TO 'pg_catalog' AS $function$
DECLARE v_tid uuid;v_task population_top3000_202608_202609_v1.pipeline_task%rowtype;v_partition population_top3000_202608_202609_v1.pipeline_partition%rowtype;
 v_context jsonb;v_headers jsonb;v_producer record;v_done population_top3000_202608_202609_v1.pipeline_task%rowtype;v_commit population_top3000_202608_202609_v1.pipeline_commit%rowtype;
 v_terminal population_top3000_202608_202609_v1.massive_shortinterest_page_v1%rowtype;v_key text;v_output text;v_producer_count integer:=0;
BEGIN
 v_tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);
 SELECT * INTO v_task FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=v_tid;
 IF v_task.family_id IS DISTINCT FROM 'S02' OR v_task.stage IS DISTINCT FROM 'VALIDATE'
 OR v_task.partition_key IS DISTINCT FROM 'short-interest-settlement-20260715-20260930' THEN RAISE EXCEPTION 'S02_METADATA_STAGE_OR_PARTITION_MISMATCH';END IF;
 IF v_task.input_versions IS DISTINCT FROM '{"scope_sha256":"8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd","archive_contract_revision":"massive-shortinterest-20261023-v1","shortinterest_bundle_sha256":"18f46852bf04cd495613e333c2fcedc9df66c9e3e3c68600070d3d77314ad15b"}'::jsonb
 OR v_task.checkpoint IS DISTINCT FROM '{}'::jsonb THEN RAISE EXCEPTION 'S02_METADATA_IMMUTABLE_INPUTS_CHANGED';END IF;
 v_context:=population_top3000_202608_202609_v1.massive_shortinterest_context_v1(v_tid);
 IF v_context->>'archive_key' IS DISTINCT FROM 'massive-archive:S02:c1ed3ed6aa5bad110f122c2459ba27f6daa14a7ac484363d4915c0fc9ddb1f88'
 OR population_top3000_202608_202609_v1.massive_shortinterest_bundle_sha256_v1() IS DISTINCT FROM '18f46852bf04cd495613e333c2fcedc9df66c9e3e3c68600070d3d77314ad15b'
 THEN RAISE EXCEPTION 'S02_METADATA_OLD_PRODUCER_BUNDLE_CHANGED';END IF;
 SELECT * INTO v_partition FROM population_top3000_202608_202609_v1.pipeline_partition WHERE family_id='S02' AND partition_key=v_task.partition_key;
 IF NOT FOUND OR v_partition.immutable_input_versions IS DISTINCT FROM v_task.input_versions
 OR v_partition.producer_revision IS DISTINCT FROM 'massive-shortinterest-20261023-v1'
 OR v_partition.denominator_state IS DISTINCT FROM 'PROVISIONAL' OR v_partition.publication_eligible IS DISTINCT FROM false
 OR v_partition.denominator_evidence->>'archive_key' IS DISTINCT FROM 'massive-archive:S02:c1ed3ed6aa5bad110f122c2459ba27f6daa14a7ac484363d4915c0fc9ddb1f88'
 OR v_partition.denominator_evidence->'independent_company_report_denominator' IS DISTINCT FROM 'false'::jsonb
 OR v_partition.denominator_evidence->'scheduled_dates_are_not_exact_publication_times' IS DISTINCT FROM 'true'::jsonb
 THEN RAISE EXCEPTION 'S02_METADATA_PROVISIONAL_PARTITION_REQUIRED';END IF;

 WITH headers AS MATERIALIZED (
  SELECT q.* FROM population_top3000_202608_202609_v1.massive_shortinterest_page_v1 q
  WHERE q.archive_key='massive-archive:S02:c1ed3ed6aa5bad110f122c2459ba27f6daa14a7ac484363d4915c0fc9ddb1f88' ORDER BY q.page_number LIMIT 145
 ), bound_headers AS MATERIALIZED (
  SELECT q.*,lag(q.next_url) OVER(ORDER BY q.page_number) previous_next_url,
   d.source_id document_id,d.source_sha256 document_sha,d.content_bytes document_bytes,d.source_uri document_uri,
   d.source_provider document_provider,d.retrieved_at document_retrieved_at,d.source_published_at document_publication_at,
   d.historical_received_at document_historical_received_at,d.license_policy document_license
  FROM headers q LEFT JOIN population_top3000_202608_202609_v1.source_document d ON d.source_id=q.source_id
 )
 SELECT jsonb_build_object('source_pages',count(*),'source_occurrences',coalesce(sum(q.result_count),0),
  'first_page',min(q.page_number),'last_page',max(q.page_number),
  'terminal_pages',count(*) FILTER(WHERE q.next_url IS NULL),
  'terminal_page',max(q.page_number) FILTER(WHERE q.next_url IS NULL),
  'incomplete_or_changed_headers',count(*) FILTER(WHERE q.family_id IS DISTINCT FROM 'S02'
   OR q.partition_key IS DISTINCT FROM 'short-interest-settlement-20260715-20260930' OR q.acquire_task_id IS DISTINCT FROM 'd4a27e18-ff2f-49e9-86c3-7200f2ee8814'::uuid
   OR q.source_id IS DISTINCT FROM q.archive_key||':page:'||q.page_number::text
   OR q.normalized_at IS NULL OR q.normalized_source_records IS DISTINCT FROM q.result_count
   OR q.document_id IS DISTINCT FROM q.source_id OR q.document_sha IS DISTINCT FROM q.source_sha256
   OR q.document_bytes IS NULL OR q.document_bytes<1 OR q.document_bytes>8388608
   OR q.document_uri IS DISTINCT FROM q.request_url OR q.document_provider IS DISTINCT FROM 'Massive'
   OR q.document_retrieved_at IS DISTINCT FROM q.retrieved_at OR q.document_publication_at IS NOT NULL
   OR q.document_historical_received_at IS NOT NULL OR q.document_license IS DISTINCT FROM 'EXISTING_MASSIVE_SUBSCRIPTION_API_RESPONSE_INTERNAL_USE'
   OR (q.page_number=1 AND q.request_url IS DISTINCT FROM 'https://api.massive.com/stocks/v1/short-interest?settlement_date.gte=2026-07-15&settlement_date.lte=2026-09-30&sort=settlement_date.asc,ticker.asc&limit=1000')
   OR (q.page_number>1 AND q.request_url IS DISTINCT FROM q.previous_next_url)),
  'header_manifest_sha256',encode(sha256(convert_to(coalesce(jsonb_agg(to_jsonb(q) ORDER BY q.page_number),'[]'::jsonb)::text,'UTF8')),'hex'))
 INTO v_headers FROM bound_headers q;

 IF v_headers->>'source_pages' IS DISTINCT FROM '135' OR v_headers->>'source_occurrences' IS DISTINCT FROM '134931'
 OR v_headers->>'first_page' IS DISTINCT FROM '1' OR v_headers->>'last_page' IS DISTINCT FROM '135'
 OR v_headers->>'terminal_pages' IS DISTINCT FROM '1' OR v_headers->>'terminal_page' IS DISTINCT FROM '135'
 OR v_headers->>'incomplete_or_changed_headers' IS DISTINCT FROM '0' THEN
 RETURN jsonb_build_object('state','BLOCKED','blocker_code','S02_SOURCE_METADATA_INCOMPLETE_OR_CHANGED',
  'resumption_condition',jsonb_build_object('event','REVIEWED_COMPLETE_IMMUTABLE_SOURCE_METADATA_AVAILABLE',
  'archive_key','massive-archive:S02:c1ed3ed6aa5bad110f122c2459ba27f6daa14a7ac484363d4915c0fc9ddb1f88','metadata',v_headers,'automatic_retry',false,'source_or_canonical_rows_scanned',false,'publication_admitted',false));END IF;
 SELECT * INTO v_terminal FROM population_top3000_202608_202609_v1.massive_shortinterest_page_v1 WHERE archive_key='massive-archive:S02:c1ed3ed6aa5bad110f122c2459ba27f6daa14a7ac484363d4915c0fc9ddb1f88' AND page_number=135;
 FOR v_producer IN SELECT * FROM (VALUES
  ('d4a27e18-ff2f-49e9-86c3-7200f2ee8814'::uuid,'ACQUIRE','28a74c03e2d794725a35edf489bf1d28e1c8a44c67978395c1eaadbb3a09505c'),
  ('4f12f545-ca86-4e03-8c57-29da768e5def'::uuid,'NORMALIZE','472056fd7e835e91fa805f01acf32933b6aea83955fa100afb1a943f64708603'),
  ('5b5ec9d2-bb1b-478a-b51f-86df942a5258'::uuid,'DERIVE','172aa83338c9246ddd4fad659758850312119f65a6e3e4a130b41d0218ff5552')) x(task_id,stage,adapter_sha256)
 LOOP
  SELECT * INTO v_done FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=v_producer.task_id;
  IF NOT FOUND OR v_done.family_id IS DISTINCT FROM 'S02' OR v_done.stage IS DISTINCT FROM v_producer.stage
   OR v_done.partition_key IS DISTINCT FROM v_task.partition_key OR v_done.input_versions IS DISTINCT FROM v_task.input_versions
   OR v_done.status IS DISTINCT FROM 'COMPLETE' OR v_done.completed_at IS NULL OR v_done.lease_until IS NOT NULL
   OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_attempt a WHERE a.attempt_id=v_done.attempt_id AND a.task_id=v_done.task_id
    AND a.outcome='COMPLETE' AND a.ended_at IS NOT NULL AND a.adapter_sha256=v_producer.adapter_sha256)
  THEN RETURN jsonb_build_object('state','BLOCKED','blocker_code','S02_PRODUCER_CHAIN_INCOMPLETE',
   'resumption_condition',jsonb_build_object('event','EXISTING_SOURCE_PRODUCER_TASK_COMPLETED','stage',v_producer.stage,'task_id',v_producer.task_id,
   'automatic_retry',false,'source_or_canonical_rows_scanned',false,'publication_admitted',false));END IF;
  IF v_producer.stage='ACQUIRE' THEN
   v_key:='massive-page:'||v_terminal.source_id||':'||v_terminal.source_sha256;v_output:=v_terminal.source_sha256;
  ELSIF v_producer.stage='NORMALIZE' THEN
   v_key:='s02-normalized:'||v_terminal.source_id||':472056fd7e835e91fa805f01acf32933b6aea83955fa100afb1a943f64708603';
   v_output:=encode(sha256(convert_to(jsonb_build_object('source_id',v_terminal.source_id,'source_sha256',v_terminal.source_sha256,'raw_records',v_terminal.result_count,'normalizer_sha256','472056fd7e835e91fa805f01acf32933b6aea83955fa100afb1a943f64708603')::text,'UTF8')),'hex');
  ELSE
   v_key:='s02-derived:massive-archive:S02:c1ed3ed6aa5bad110f122c2459ba27f6daa14a7ac484363d4915c0fc9ddb1f88:135:172aa83338c9246ddd4fad659758850312119f65a6e3e4a130b41d0218ff5552';
   v_output:=encode(sha256(convert_to(jsonb_build_object('archive_key','massive-archive:S02:c1ed3ed6aa5bad110f122c2459ba27f6daa14a7ac484363d4915c0fc9ddb1f88','derived_page',135,'deriver_sha256','172aa83338c9246ddd4fad659758850312119f65a6e3e4a130b41d0218ff5552','source_record_count',v_terminal.result_count)::text,'UTF8')),'hex');
  END IF;
  SELECT * INTO v_commit FROM population_top3000_202608_202609_v1.pipeline_commit WHERE commit_key=v_key;
  IF NOT FOUND OR v_commit.task_id IS DISTINCT FROM v_done.task_id OR v_commit.attempt_id IS DISTINCT FROM v_done.attempt_id
   OR v_commit.output_sha256 IS DISTINCT FROM v_output OR v_commit.receipt->>'output_sha256' IS DISTINCT FROM v_output
   OR v_commit.receipt->'input_versions' IS DISTINCT FROM v_task.input_versions
   OR v_commit.receipt->'cohort_certified' IS DISTINCT FROM 'false'::jsonb
   OR (v_commit.receipt->'immutable_output_ids' @> jsonb_build_array(v_terminal.source_id)) IS DISTINCT FROM true
  THEN RAISE EXCEPTION 'S02_METADATA_TERMINAL_PRODUCER_COMMIT_CHANGED';END IF;
  v_producer_count:=v_producer_count+1;
 END LOOP;
 RETURN jsonb_build_object('state','BLOCKED','blocker_code','S02_COMPANY_REPORT_DENOMINATOR_AND_PUBLICATION_TIME_REQUIRED',
  'resumption_condition',jsonb_build_object('event','REVIEWED_COMPANY_REPORT_CENSUS_IDENTITY_AND_TEMPORAL_EVIDENCE_AVAILABLE',
   'required_evidence',jsonb_build_array('FROZEN_3000_COMPANY_UNIVERSE_AND_HISTORICAL_SECURITY_MAPPING',
    'INDEPENDENT_EXPECTED_REPORT_CENSUS_INCLUDING_EXPLICIT_ABSENCE_RULES','INDEPENDENT_SOURCE_RECONCILIATION',
    'PUBLISHED_DATE_VS_EXACT_AVAILABILITY_POLICY','ORIGINAL_RECEIPT_OR_EXPLICIT_ARCHIVED_AVAILABILITY_POLICY'),
   'archive_key','massive-archive:S02:c1ed3ed6aa5bad110f122c2459ba27f6daa14a7ac484363d4915c0fc9ddb1f88','metadata',v_headers,'completed_producer_stages',v_producer_count,
   'assessment_kind','BOUNDED_METADATA_QUALITY_HOLD_ONLY','source_bundle_sha256','18f46852bf04cd495613e333c2fcedc9df66c9e3e3c68600070d3d77314ad15b',
   'provider_chain_terminal_is_not_company_coverage',true,'preserve_all_source_pages_and_versions',true,
   'source_bytes_rehashed',false,'canonical_rows_reconciled_by_this_handler',false,'provider_accuracy_verified',false,
   'company_completeness_verified',false,'publication_admitted',false,'automatic_retry',false));
END $function$;
REVOKE ALL ON FUNCTION population_top3000_202608_202609_v1.massive_shortinterest_metadata_quality_hold_v1(uuid,text) FROM PUBLIC,anon,authenticated;
GRANT EXECUTE ON FUNCTION population_top3000_202608_202609_v1.massive_shortinterest_metadata_quality_hold_v1(uuid,text) TO postgres,service_role;

COMMIT;
