from pathlib import Path
import hashlib,json
P=Path(__file__).resolve().parent
S='population_top3000_202608_202609_v1'
scope='8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd'
bundle='18f46852bf04cd495613e333c2fcedc9df66c9e3e3c68600070d3d77314ad15b'
archive='massive-archive:S02:c1ed3ed6aa5bad110f122c2459ba27f6daa14a7ac484363d4915c0fc9ddb1f88'
part='short-interest-settlement-20260715-20260930'
versions=json.dumps(dict(scope_sha256=scope,archive_contract_revision='massive-shortinterest-20261023-v1',shortinterest_bundle_sha256=bundle),separators=(',',':'))
fn=f'{S}.massive_shortinterest_metadata_quality_hold_v1'
oldsha='3c139ee0eb3a10f37c79dc7fe6827d5450cbc1403b44e1dee3a5aada652e360d'
acq='d4a27e18-ff2f-49e9-86c3-7200f2ee8814';norm='4f12f545-ca86-4e03-8c57-29da768e5def';derive='5b5ec9d2-bb1b-478a-b51f-86df942a5258';validate='3443b551-f67a-4f4c-841b-9f6a67532eac';cancelled='02e60669-6ddf-4a91-91bf-0aba7d64e0fe'
nh='472056fd7e835e91fa805f01acf32933b6aea83955fa100afb1a943f64708603';dh='172aa83338c9246ddd4fad659758850312119f65a6e3e4a130b41d0218ff5552';ah='28a74c03e2d794725a35edf489bf1d28e1c8a44c67978395c1eaadbb3a09505c'
guard=f'''
 IF NOT pg_try_advisory_xact_lock(2026083000261022) THEN RAISE EXCEPTION 'S02_METADATA_CONTROLLER_BUSY';END IF;
 PERFORM 1 FROM {S}.acquisition_control WHERE singleton FOR UPDATE;
 PERFORM 1 FROM {S}.pipeline_control WHERE singleton FOR UPDATE;
 IF NOT EXISTS(SELECT 1 FROM {S}.acquisition_control a JOIN {S}.pipeline_control p USING(singleton)
 WHERE a.singleton AND a.owner_id='top3000-recovery-20261023-root-4b1314bcb5c2' AND a.owner_fence=2 AND a.stop_generation=0
 AND a.owner_id=p.expected_owner_id AND a.owner_fence=p.expected_owner_fence AND a.stop_generation=p.expected_stop_generation
 AND a.scope_sha256='{scope}' AND p.scope_sha256=a.scope_sha256 AND a.enabled AND NOT a.stopped
 AND a.hold_reason IS NULL AND a.admitted_until>clock_timestamp() AND a.reserved_bytes=0
 AND p.enabled AND p.shared_scheduler_verified AND p.budget_verified_at IS NOT NULL AND p.deadline>clock_timestamp() AND p.tick_count<p.max_ticks
 AND p.actual_usd+p.accrued_usd+p.unavoidable_usd+p.reserved_request_usd+p.repair_egress_reserve_usd+p.remaining_uncommitted_usd<=p.budget_cap_usd)
 THEN RAISE EXCEPTION 'S02_METADATA_OWNER_STOP_SCOPE_OR_ADMISSION_CHANGED';END IF;
 IF NOT EXISTS(SELECT 1 FROM cron.job WHERE jobid=596 AND NOT active AND database=current_database()
 AND btrim(command)='select {S}.pipeline_tick_v1();') THEN RAISE EXCEPTION 'S02_METADATA_BOUND_CRON_MUST_BE_PAUSED';END IF;
 IF EXISTS(SELECT 1 FROM {S}.pipeline_task WHERE status='LEASED')
 OR EXISTS(SELECT 1 FROM {S}.pipeline_provider_lease WHERE released_at IS NULL) THEN RAISE EXCEPTION 'S02_METADATA_EXISTING_LEASES_MUST_DRAIN';END IF;
 IF {S}.massive_shortinterest_bundle_sha256_v1() IS DISTINCT FROM '{bundle}' THEN RAISE EXCEPTION 'S02_METADATA_OLD_PRODUCER_BUNDLE_CHANGED';END IF;
'''
# Bounded primary-key source-header lookups. This deliberately does not select raw bodies or typed records.
metadata_query=f'''
 WITH headers AS MATERIALIZED (
  SELECT q.* FROM {S}.massive_shortinterest_page_v1 q
  WHERE q.archive_key='{archive}' ORDER BY q.page_number LIMIT 145
 ), bound_headers AS MATERIALIZED (
  SELECT q.*,lag(q.next_url) OVER(ORDER BY q.page_number) previous_next_url,
   d.source_id document_id,d.source_sha256 document_sha,d.content_bytes document_bytes,d.source_uri document_uri,
   d.source_provider document_provider,d.retrieved_at document_retrieved_at,d.source_published_at document_publication_at,
   d.historical_received_at document_historical_received_at,d.license_policy document_license
  FROM headers q LEFT JOIN {S}.source_document d ON d.source_id=q.source_id
 )
 SELECT jsonb_build_object('source_pages',count(*),'source_occurrences',coalesce(sum(q.result_count),0),
  'first_page',min(q.page_number),'last_page',max(q.page_number),
  'terminal_pages',count(*) FILTER(WHERE q.next_url IS NULL),
  'terminal_page',max(q.page_number) FILTER(WHERE q.next_url IS NULL),
  'incomplete_or_changed_headers',count(*) FILTER(WHERE q.family_id IS DISTINCT FROM 'S02'
   OR q.partition_key IS DISTINCT FROM '{part}' OR q.acquire_task_id IS DISTINCT FROM '{acq}'::uuid
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
'''
function=f'''CREATE OR REPLACE FUNCTION {fn}(p_attempt uuid,p_token text)
RETURNS jsonb LANGUAGE plpgsql SET search_path TO 'pg_catalog' AS $function$
DECLARE v_tid uuid;v_task {S}.pipeline_task%rowtype;v_partition {S}.pipeline_partition%rowtype;
 v_context jsonb;v_headers jsonb;v_producer record;v_done {S}.pipeline_task%rowtype;v_commit {S}.pipeline_commit%rowtype;
 v_terminal {S}.massive_shortinterest_page_v1%rowtype;v_key text;v_output text;v_producer_count integer:=0;
BEGIN
 v_tid:={S}.pipeline_assert_attempt_v1(p_attempt,p_token);
 SELECT * INTO v_task FROM {S}.pipeline_task WHERE task_id=v_tid;
 IF v_task.family_id IS DISTINCT FROM 'S02' OR v_task.stage IS DISTINCT FROM 'VALIDATE'
 OR v_task.partition_key IS DISTINCT FROM '{part}' THEN RAISE EXCEPTION 'S02_METADATA_STAGE_OR_PARTITION_MISMATCH';END IF;
 IF v_task.input_versions IS DISTINCT FROM '{versions}'::jsonb
 OR v_task.checkpoint IS DISTINCT FROM '{{}}'::jsonb THEN RAISE EXCEPTION 'S02_METADATA_IMMUTABLE_INPUTS_CHANGED';END IF;
 v_context:={S}.massive_shortinterest_context_v1(v_tid);
 IF v_context->>'archive_key' IS DISTINCT FROM '{archive}'
 OR {S}.massive_shortinterest_bundle_sha256_v1() IS DISTINCT FROM '{bundle}'
 THEN RAISE EXCEPTION 'S02_METADATA_OLD_PRODUCER_BUNDLE_CHANGED';END IF;
 SELECT * INTO v_partition FROM {S}.pipeline_partition WHERE family_id='S02' AND partition_key=v_task.partition_key;
 IF NOT FOUND OR v_partition.immutable_input_versions IS DISTINCT FROM v_task.input_versions
 OR v_partition.producer_revision IS DISTINCT FROM 'massive-shortinterest-20261023-v1'
 OR v_partition.denominator_state IS DISTINCT FROM 'PROVISIONAL' OR v_partition.publication_eligible IS DISTINCT FROM false
 OR v_partition.denominator_evidence->>'archive_key' IS DISTINCT FROM '{archive}'
 OR v_partition.denominator_evidence->'independent_company_report_denominator' IS DISTINCT FROM 'false'::jsonb
 OR v_partition.denominator_evidence->'scheduled_dates_are_not_exact_publication_times' IS DISTINCT FROM 'true'::jsonb
 THEN RAISE EXCEPTION 'S02_METADATA_PROVISIONAL_PARTITION_REQUIRED';END IF;
{metadata_query}
 IF v_headers->>'source_pages' IS DISTINCT FROM '135' OR v_headers->>'source_occurrences' IS DISTINCT FROM '134931'
 OR v_headers->>'first_page' IS DISTINCT FROM '1' OR v_headers->>'last_page' IS DISTINCT FROM '135'
 OR v_headers->>'terminal_pages' IS DISTINCT FROM '1' OR v_headers->>'terminal_page' IS DISTINCT FROM '135'
 OR v_headers->>'incomplete_or_changed_headers' IS DISTINCT FROM '0' THEN
 RETURN jsonb_build_object('state','BLOCKED','blocker_code','S02_SOURCE_METADATA_INCOMPLETE_OR_CHANGED',
  'resumption_condition',jsonb_build_object('event','REVIEWED_COMPLETE_IMMUTABLE_SOURCE_METADATA_AVAILABLE',
  'archive_key','{archive}','metadata',v_headers,'automatic_retry',false,'source_or_canonical_rows_scanned',false,'publication_admitted',false));END IF;
 SELECT * INTO v_terminal FROM {S}.massive_shortinterest_page_v1 WHERE archive_key='{archive}' AND page_number=135;
 FOR v_producer IN SELECT * FROM (VALUES
  ('{acq}'::uuid,'ACQUIRE','{ah}'),
  ('{norm}'::uuid,'NORMALIZE','{nh}'),
  ('{derive}'::uuid,'DERIVE','{dh}')) x(task_id,stage,adapter_sha256)
 LOOP
  SELECT * INTO v_done FROM {S}.pipeline_task WHERE task_id=v_producer.task_id;
  IF NOT FOUND OR v_done.family_id IS DISTINCT FROM 'S02' OR v_done.stage IS DISTINCT FROM v_producer.stage
   OR v_done.partition_key IS DISTINCT FROM v_task.partition_key OR v_done.input_versions IS DISTINCT FROM v_task.input_versions
   OR v_done.status IS DISTINCT FROM 'COMPLETE' OR v_done.completed_at IS NULL OR v_done.lease_until IS NOT NULL
   OR NOT EXISTS(SELECT 1 FROM {S}.pipeline_attempt a WHERE a.attempt_id=v_done.attempt_id AND a.task_id=v_done.task_id
    AND a.outcome='COMPLETE' AND a.ended_at IS NOT NULL AND a.adapter_sha256=v_producer.adapter_sha256)
  THEN RETURN jsonb_build_object('state','BLOCKED','blocker_code','S02_PRODUCER_CHAIN_INCOMPLETE',
   'resumption_condition',jsonb_build_object('event','EXISTING_SOURCE_PRODUCER_TASK_COMPLETED','stage',v_producer.stage,'task_id',v_producer.task_id,
   'automatic_retry',false,'source_or_canonical_rows_scanned',false,'publication_admitted',false));END IF;
  IF v_producer.stage='ACQUIRE' THEN
   v_key:='massive-page:'||v_terminal.source_id||':'||v_terminal.source_sha256;v_output:=v_terminal.source_sha256;
  ELSIF v_producer.stage='NORMALIZE' THEN
   v_key:='s02-normalized:'||v_terminal.source_id||':{nh}';
   v_output:=encode(sha256(convert_to(jsonb_build_object('source_id',v_terminal.source_id,'source_sha256',v_terminal.source_sha256,'raw_records',v_terminal.result_count,'normalizer_sha256','{nh}')::text,'UTF8')),'hex');
  ELSE
   v_key:='s02-derived:{archive}:135:{dh}';
   v_output:=encode(sha256(convert_to(jsonb_build_object('archive_key','{archive}','derived_page',135,'deriver_sha256','{dh}','source_record_count',v_terminal.result_count)::text,'UTF8')),'hex');
  END IF;
  SELECT * INTO v_commit FROM {S}.pipeline_commit WHERE commit_key=v_key;
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
   'archive_key','{archive}','metadata',v_headers,'completed_producer_stages',v_producer_count,
   'assessment_kind','BOUNDED_METADATA_QUALITY_HOLD_ONLY','source_bundle_sha256','{bundle}',
   'provider_chain_terminal_is_not_company_coverage',true,'preserve_all_source_pages_and_versions',true,
   'source_bytes_rehashed',false,'canonical_rows_reconciled_by_this_handler',false,'provider_accuracy_verified',false,
   'company_completeness_verified',false,'publication_admitted',false,'automatic_retry',false));
END $function$;
REVOKE ALL ON FUNCTION {fn}(uuid,text) FROM PUBLIC,anon,authenticated;
GRANT EXECUTE ON FUNCTION {fn}(uuid,text) TO postgres,service_role;
'''
installer=f'''-- ROOT ONLY. Isolated VALIDATE-only helper; no source-bundle changes and no binding/admission here.
BEGIN;
SET LOCAL statement_timeout='8s';SET LOCAL lock_timeout='500ms';
DO $guard$ BEGIN {guard} END $guard$;
{function}
COMMIT;
'''
(P/'s02_metadata_quality_hold_schema_DRAFT.sql').write_text(installer)
(P/'s02_metadata_quality_hold_header_query_FRAGMENT.sql').write_text('-- PL/pgSQL metadata fragment shared verbatim by handler/test/admission. Not standalone SQL.\n'+metadata_query)
# Preserve exact original cancelled execution; helper tests use one additional transaction-local lease.
fixture=f'''-- ROOT ONLY. Install helper, pause/drain cron, then run this transaction. All mutations ROLLBACK.
-- No raw body or 134,931-row canonical scan; actual 135-page metadata, producer receipts and known quality hold only.
BEGIN;
SET LOCAL statement_timeout='10s';SET LOCAL lock_timeout='500ms';
DO $test$
DECLARE v_hash text;v_headers jsonb;v_headers_before jsonb;v_before_task jsonb;v_before_cancelled jsonb;
 v_before_partition jsonb;v_before_producers jsonb;v_before_counters jsonb;v_result jsonb;v_replay jsonb;
 v_attempt uuid:=gen_random_uuid();v_token text:=gen_random_uuid()::text;v_start timestamptz;v_ms numeric;
 v_negative integer:=0;v_finish jsonb;v_after_task jsonb;
BEGIN
{guard}
 SELECT to_jsonb(t) INTO v_before_task FROM {S}.pipeline_task t WHERE t.task_id='{validate}';
 SELECT to_jsonb(a) INTO v_before_cancelled FROM {S}.pipeline_attempt a WHERE a.attempt_id='{cancelled}';
 IF v_before_task->>'status' IS DISTINCT FROM 'BLOCKED' OR v_before_task->>'blocker_code' IS DISTINCT FROM 'DATABASE_QUERY_CANCELED'
 OR v_before_task->>'executions' IS DISTINCT FROM '1' OR v_before_task->>'cumulative_failures' IS DISTINCT FROM '1'
 OR v_before_task->>'attempt_id' IS DISTINCT FROM '{cancelled}' OR v_before_task->'input_versions' IS DISTINCT FROM '{versions}'::jsonb
 OR v_before_task->'checkpoint' IS DISTINCT FROM '{{}}'::jsonb OR v_before_cancelled->>'outcome' IS DISTINCT FROM 'BLOCKED'
 OR v_before_cancelled->>'adapter_sha256' IS DISTINCT FROM '{oldsha}' THEN RAISE EXCEPTION 'S02_METADATA_ORIGINAL_CANCELLATION_CHANGED';END IF;
 SELECT to_jsonb(q) INTO v_before_partition FROM {S}.pipeline_partition q WHERE family_id='S02' AND partition_key='{part}';
 SELECT jsonb_agg(to_jsonb(t) ORDER BY t.stage) INTO v_before_producers FROM {S}.pipeline_task t
  WHERE task_id IN('{acq}'::uuid,'{norm}'::uuid,'{derive}'::uuid);
 SELECT jsonb_build_object('provider',provider_requests,'http',http_invocations,'stored',stored_bytes,'reserved',reserved_bytes)
 INTO v_before_counters FROM {S}.acquisition_control WHERE singleton;
{metadata_query}
 v_headers_before:=v_headers;
 IF NOT EXISTS(SELECT 1 FROM {S}.pipeline_adapter WHERE family_id='S02' AND stage='VALIDATE'
  AND function_oid='{S}.pipeline_validate_v1(uuid,text)'::regprocedure::oid AND function_sha256='{oldsha}' AND enabled AND resource_class='DATABASE' AND provider IS NULL)
 THEN RAISE EXCEPTION 'S02_METADATA_ORIGINAL_VALIDATE_BINDING_CHANGED';END IF;
 v_hash:=encode(sha256(convert_to(pg_get_functiondef('{fn}(uuid,text)'::regprocedure),'UTF8')),'hex');
 UPDATE {S}.pipeline_adapter SET function_oid='{fn}(uuid,text)'::regprocedure::oid,function_sha256=v_hash,
  producer_revision='s02-bounded-metadata-quality-hold-v1',expected_batch_seconds=5 WHERE family_id='S02' AND stage='VALIDATE';
 UPDATE {S}.pipeline_task SET status='LEASED',attempt_id=v_attempt,executions=executions+1,lease_until=clock_timestamp()+interval '60 seconds'
  WHERE task_id='{validate}';
 INSERT INTO {S}.pipeline_attempt(attempt_id,task_id,execution_number,token_sha256,owner_fence,stop_generation,adapter_sha256)
 VALUES(v_attempt,'{validate}',2,encode(sha256(convert_to(v_token,'UTF8')),'hex'),2,0,v_hash);
 v_start:=clock_timestamp();v_result:={fn}(v_attempt,v_token);v_ms:=round(extract(epoch FROM clock_timestamp()-v_start)*1000,3);
 IF v_ms>5000 OR v_result->>'state' IS DISTINCT FROM 'BLOCKED'
 OR v_result->>'blocker_code' IS DISTINCT FROM 'S02_COMPANY_REPORT_DENOMINATOR_AND_PUBLICATION_TIME_REQUIRED'
 OR v_result#>'{{resumption_condition,metadata}}' IS DISTINCT FROM v_headers_before
 OR v_result#>>'{{resumption_condition,completed_producer_stages}}' IS DISTINCT FROM '3'
 OR v_result#>'{{resumption_condition,publication_admitted}}' IS DISTINCT FROM 'false'::jsonb
 OR v_result#>'{{resumption_condition,canonical_rows_reconciled_by_this_handler}}' IS DISTINCT FROM 'false'::jsonb
 THEN RAISE EXCEPTION 'S02_METADATA_ACTUAL_HOLD_FAILED: %',v_result;END IF;
 v_replay:={fn}(v_attempt,v_token);IF v_replay IS DISTINCT FROM v_result THEN RAISE EXCEPTION 'S02_METADATA_ACTIVE_REPLAY_CHANGED';END IF;
 BEGIN
  UPDATE {S}.acquisition_control SET owner_fence=3 WHERE singleton;
  PERFORM {fn}(v_attempt,v_token);RAISE EXCEPTION 'FIXTURE_EXPECTED_OWNER_REJECTION';
 EXCEPTION WHEN raise_exception THEN IF SQLERRM<>'CONTROLLER_OWNERSHIP_CHANGED' THEN RAISE;END IF;END;v_negative:=v_negative+1;
 BEGIN
  PERFORM {fn}(v_attempt,v_token||'-wrong');RAISE EXCEPTION 'FIXTURE_EXPECTED_AUTH_REJECTION';
 EXCEPTION WHEN raise_exception THEN IF SQLERRM<>'ATTEMPT_AUTH_FAILED' THEN RAISE;END IF;END;v_negative:=v_negative+1;
 BEGIN
  UPDATE {S}.pipeline_adapter SET function_oid='{fn}(uuid,text)'::regprocedure::oid,function_sha256=v_hash WHERE family_id='S02' AND stage='REPAIR';
  UPDATE {S}.pipeline_task SET stage='REPAIR' WHERE task_id='{validate}';
  PERFORM {fn}(v_attempt,v_token);RAISE EXCEPTION 'FIXTURE_EXPECTED_STAGE_REJECTION';
 EXCEPTION WHEN raise_exception THEN IF SQLERRM<>'S02_METADATA_STAGE_OR_PARTITION_MISMATCH' THEN RAISE;END IF;END;v_negative:=v_negative+1;
 BEGIN
  UPDATE {S}.pipeline_task SET input_versions=input_versions||'{{"shortinterest_bundle_sha256":"changed"}}'::jsonb WHERE task_id='{validate}';
  PERFORM {fn}(v_attempt,v_token);RAISE EXCEPTION 'FIXTURE_EXPECTED_INPUT_REJECTION';
 EXCEPTION WHEN raise_exception THEN IF SQLERRM<>'S02_METADATA_IMMUTABLE_INPUTS_CHANGED' THEN RAISE;END IF;END;v_negative:=v_negative+1;
 BEGIN
  UPDATE {S}.pipeline_task SET status='READY' WHERE task_id='{derive}';
  v_replay:={fn}(v_attempt,v_token);
  IF v_replay->>'blocker_code' IS DISTINCT FROM 'S02_PRODUCER_CHAIN_INCOMPLETE' THEN RAISE EXCEPTION 'FIXTURE_EXPECTED_PRODUCER_HOLD';END IF;
  RAISE EXCEPTION 'FIXTURE_ROLLBACK_EXPECTED_PRODUCER_HOLD';
 EXCEPTION WHEN raise_exception THEN IF SQLERRM<>'FIXTURE_ROLLBACK_EXPECTED_PRODUCER_HOLD' THEN RAISE;END IF;END;v_negative:=v_negative+1;
 BEGIN
  UPDATE {S}.massive_shortinterest_page_v1 SET normalized_source_records=result_count-1 WHERE archive_key='{archive}' AND page_number=1;
  v_replay:={fn}(v_attempt,v_token);
  IF v_replay->>'blocker_code' IS DISTINCT FROM 'S02_SOURCE_METADATA_INCOMPLETE_OR_CHANGED' THEN RAISE EXCEPTION 'FIXTURE_EXPECTED_HEADER_HOLD';END IF;
  RAISE EXCEPTION 'FIXTURE_ROLLBACK_EXPECTED_HEADER_HOLD';
 EXCEPTION WHEN raise_exception THEN IF SQLERRM<>'FIXTURE_ROLLBACK_EXPECTED_HEADER_HOLD' THEN RAISE;END IF;END;v_negative:=v_negative+1;
 BEGIN
  UPDATE {S}.pipeline_partition SET publication_eligible=true WHERE family_id='S02' AND partition_key='{part}';
  PERFORM {fn}(v_attempt,v_token);RAISE EXCEPTION 'FIXTURE_EXPECTED_PUBLICATION_REJECTION';
 EXCEPTION WHEN raise_exception THEN IF SQLERRM<>'S02_METADATA_PROVISIONAL_PARTITION_REQUIRED' THEN RAISE;END IF;END;v_negative:=v_negative+1;
 BEGIN
  UPDATE {S}.pipeline_commit SET output_sha256=repeat('0',64) WHERE commit_key='s02-derived:{archive}:135:{dh}';
  PERFORM {fn}(v_attempt,v_token);RAISE EXCEPTION 'FIXTURE_EXPECTED_COMMIT_REJECTION';
 EXCEPTION WHEN raise_exception THEN IF SQLERRM<>'S02_METADATA_TERMINAL_PRODUCER_COMMIT_CHANGED' THEN RAISE;END IF;END;v_negative:=v_negative+1;
 BEGIN
  UPDATE {S}.pipeline_commit SET receipt=receipt-'immutable_output_ids' WHERE commit_key='s02-derived:{archive}:135:{dh}';
  PERFORM {fn}(v_attempt,v_token);RAISE EXCEPTION 'FIXTURE_EXPECTED_IMMUTABLE_IDS_REJECTION';
 EXCEPTION WHEN raise_exception THEN IF SQLERRM<>'S02_METADATA_TERMINAL_PRODUCER_COMMIT_CHANGED' THEN RAISE;END IF;END;v_negative:=v_negative+1;
 v_finish:={S}.pipeline_finish_v1(v_attempt,v_token,v_result);
 IF v_finish->>'state' IS DISTINCT FROM 'BLOCKED' OR v_finish->>'successor_task_id' IS NOT NULL THEN RAISE EXCEPTION 'S02_METADATA_FINISH_NOT_HELD';END IF;
 v_replay:={S}.pipeline_finish_v1(v_attempt,v_token,v_result);
 IF v_replay->>'state' IS DISTINCT FROM 'ALREADY_RECORDED' OR v_replay->>'outcome' IS DISTINCT FROM 'BLOCKED' THEN RAISE EXCEPTION 'S02_METADATA_FINISHED_REPLAY_NOT_IDEMPOTENT';END IF;
 SELECT to_jsonb(t) INTO v_after_task FROM {S}.pipeline_task t WHERE task_id='{validate}';
 IF (v_after_task-ARRAY['status','attempt_id','executions','lease_until','blocker_code','blocker_until','resumption_condition'])
  IS DISTINCT FROM (v_before_task-ARRAY['status','attempt_id','executions','lease_until','blocker_code','blocker_until','resumption_condition'])
 OR v_after_task->>'executions' IS DISTINCT FROM '2'
 OR (SELECT to_jsonb(a) FROM {S}.pipeline_attempt a WHERE attempt_id='{cancelled}') IS DISTINCT FROM v_before_cancelled
 OR (SELECT to_jsonb(q) FROM {S}.pipeline_partition q WHERE family_id='S02' AND partition_key='{part}') IS DISTINCT FROM v_before_partition
 OR (SELECT jsonb_agg(to_jsonb(t) ORDER BY t.stage) FROM {S}.pipeline_task t WHERE task_id IN('{acq}'::uuid,'{norm}'::uuid,'{derive}'::uuid)) IS DISTINCT FROM v_before_producers
 OR (SELECT jsonb_build_object('provider',provider_requests,'http',http_invocations,'stored',stored_bytes,'reserved',reserved_bytes) FROM {S}.acquisition_control WHERE singleton) IS DISTINCT FROM v_before_counters
 THEN RAISE EXCEPTION 'S02_METADATA_HISTORY_SOURCE_OR_CONTROL_CHANGED';END IF;
{metadata_query}
 IF v_headers IS DISTINCT FROM v_headers_before OR v_negative<>9 OR EXISTS(SELECT 1 FROM cron.job WHERE jobid=596 AND active)
 THEN RAISE EXCEPTION 'S02_METADATA_HEADERS_OR_CRON_CHANGED';END IF;
 PERFORM set_config('top3000.s02_metadata_quality_hold_receipt',jsonb_build_object('result','PASS',
  'test','ACTUAL_S02_135_PAGE_METADATA_ONLY_QUALITY_HOLD_ROLLBACK','receipt_event','S02_METADATA_QUALITY_HOLD_ROLLBACK_VERIFIED',
  'observed_at',clock_timestamp(),'owner_fence',2,'stop_generation',0,'scope_sha256','{scope}',
  'tested_function_sha256',v_hash,'source_bundle_sha256','{bundle}','handler_elapsed_ms',v_ms,'expected_batch_seconds',5,
  'negative_guards',v_negative,'active_replay_identical',true,'finished_replay_idempotent',true,
  'source_headers',v_headers_before,'original_task',v_before_task,'original_cancelled_attempt',v_before_cancelled,
  'original_partition',v_before_partition,'original_producer_tasks',v_before_producers,'hold_receipt',v_result,
  'source_or_canonical_rows_scanned',false,'source_rows_modified',false,'new_provider_requests',0,
  'prior_failure_counters_preserved',true,'publication_admitted',false,'independent_provider_accuracy_verified',false)::text,true);
END $test$;
SELECT current_setting('top3000.s02_metadata_quality_hold_receipt')::jsonb AS receipt;
ROLLBACK;
'''
(P/'s02_metadata_quality_hold_rollback_DRAFT.sql').write_text(fixture)
registration=f'''-- ROOT ONLY after actual unchanged-code rollback PASS is persisted in the named pipeline_event.
-- One reviewed VALIDATE retry; original input versions, checkpoint and prior failure/attempt history are preserved.
-- Leaves cron596 PAUSED. No source/typed rows, REPAIR binding, source bundle or publication gates are changed.
BEGIN;
SET LOCAL statement_timeout='8s';SET LOCAL lock_timeout='500ms';
DO $admit$
DECLARE v_hash text;v_evidence jsonb;v_headers jsonb;v_before_task jsonb;v_after_task jsonb;v_updated integer;
BEGIN
{guard}
 v_hash:=encode(sha256(convert_to(pg_get_functiondef('{fn}(uuid,text)'::regprocedure),'UTF8')),'hex');
 SELECT e.details INTO v_evidence FROM {S}.pipeline_event e WHERE e.event='S02_METADATA_QUALITY_HOLD_ROLLBACK_VERIFIED'
 AND e.details->>'result'='PASS' AND e.details->>'test'='ACTUAL_S02_135_PAGE_METADATA_ONLY_QUALITY_HOLD_ROLLBACK'
 AND e.details->>'tested_function_sha256'=v_hash AND e.details->>'source_bundle_sha256'='{bundle}'
 AND e.details->>'scope_sha256'='{scope}' AND e.details->>'owner_fence'='2' AND e.details->>'stop_generation'='0'
 AND e.details->>'negative_guards'='9' AND e.details->>'active_replay_identical'='true' AND e.details->>'finished_replay_idempotent'='true'
 AND e.details->>'new_provider_requests'='0' AND e.details->>'source_rows_modified'='false'
 AND e.details->>'source_or_canonical_rows_scanned'='false' AND e.details->>'publication_admitted'='false'
 AND e.details->>'prior_failure_counters_preserved'='true' AND e.details->>'expected_batch_seconds'='5'
 AND jsonb_typeof(e.details->'handler_elapsed_ms')='number' AND (e.details->>'handler_elapsed_ms')::numeric BETWEEN 0 AND 5000
 ORDER BY e.at DESC LIMIT 1;
 IF v_evidence IS NULL THEN RAISE EXCEPTION 'S02_METADATA_ACTUAL_CURRENT_CODE_ROLLBACK_REQUIRED';END IF;
 SELECT to_jsonb(t) INTO v_before_task FROM {S}.pipeline_task t WHERE t.task_id='{validate}' FOR UPDATE;
 IF v_before_task IS DISTINCT FROM v_evidence->'original_task'
 OR v_before_task->>'status' IS DISTINCT FROM 'BLOCKED' OR v_before_task->>'blocker_code' IS DISTINCT FROM 'DATABASE_QUERY_CANCELED'
 OR v_before_task->>'executions' IS DISTINCT FROM '1' OR v_before_task->>'cumulative_failures' IS DISTINCT FROM '1'
 OR v_before_task->>'attempt_id' IS DISTINCT FROM '{cancelled}'
 OR (SELECT to_jsonb(a) FROM {S}.pipeline_attempt a WHERE a.attempt_id='{cancelled}') IS DISTINCT FROM v_evidence->'original_cancelled_attempt'
 OR (SELECT to_jsonb(q) FROM {S}.pipeline_partition q WHERE q.family_id='S02' AND q.partition_key='{part}') IS DISTINCT FROM v_evidence->'original_partition'
 OR (SELECT jsonb_agg(to_jsonb(t) ORDER BY t.stage) FROM {S}.pipeline_task t WHERE t.task_id IN('{acq}'::uuid,'{norm}'::uuid,'{derive}'::uuid)) IS DISTINCT FROM v_evidence->'original_producer_tasks'
 THEN RAISE EXCEPTION 'S02_METADATA_TESTED_TASK_PRODUCER_OR_CANCELLATION_CHANGED';END IF;
{metadata_query}
 IF v_headers IS DISTINCT FROM v_evidence->'source_headers' THEN RAISE EXCEPTION 'S02_METADATA_TESTED_SOURCE_HEADERS_CHANGED';END IF;
 UPDATE {S}.pipeline_adapter SET function_oid='{fn}(uuid,text)'::regprocedure::oid,function_sha256=v_hash,
  producer_revision='s02-bounded-metadata-quality-hold-v1',expected_batch_seconds=5,
  reviewed_test_evidence=reviewed_test_evidence||jsonb_build_object('bounded_metadata_quality_hold',v_evidence,
   'source_producer_bundle_preserved',true,'source_or_canonical_row_measurement_performed',false,'publication_admitted',false)
 WHERE family_id='S02' AND stage='VALIDATE' AND function_oid='{S}.pipeline_validate_v1(uuid,text)'::regprocedure::oid
 AND function_sha256='{oldsha}' AND enabled AND resource_class='DATABASE' AND provider IS NULL;
 GET DIAGNOSTICS v_updated=ROW_COUNT;IF v_updated<>1 THEN RAISE EXCEPTION 'S02_METADATA_ORIGINAL_VALIDATE_BINDING_CHANGED';END IF;
 UPDATE {S}.pipeline_task SET status='READY',not_before=clock_timestamp(),blocker_code=NULL,blocker_until=NULL,resumption_condition=NULL
 WHERE task_id='{validate}';
 SELECT to_jsonb(t) INTO v_after_task FROM {S}.pipeline_task t WHERE t.task_id='{validate}';
 IF (v_after_task-ARRAY['status','not_before','blocker_code','blocker_until','resumption_condition']) IS DISTINCT FROM
  (v_before_task-ARRAY['status','not_before','blocker_code','blocker_until','resumption_condition'])
 THEN RAISE EXCEPTION 'S02_METADATA_UNREVIEWED_TASK_FIELD_CHANGED';END IF;
 INSERT INTO {S}.pipeline_event(task_id,event,details) VALUES('{validate}','S02_METADATA_QUALITY_HOLD_ADMITTED',
  jsonb_build_object('owner_id','top3000-recovery-20261023-root-4b1314bcb5c2','owner_fence',2,'stop_generation',0,'scope_sha256','{scope}',
   'new_handler_sha256',v_hash,'old_generic_validator_sha256','{oldsha}','source_bundle_sha256','{bundle}',
   'source_headers',v_headers,'original_task',v_before_task,'original_cancelled_attempt',v_evidence->'original_cancelled_attempt',
   'reviewed_retry_only',true,'failure_and_execution_counters_reset',false,'source_or_typed_rows_modified',false,
   'publication_admitted',false,'new_provider_requests',0,'cron_remains_paused',true));
 IF EXISTS(SELECT 1 FROM cron.job WHERE jobid=596 AND active) THEN RAISE EXCEPTION 'S02_METADATA_CRON_UNEXPECTEDLY_ACTIVE';END IF;
END $admit$;
COMMIT;
'''
(P/'s02_metadata_quality_hold_registration_DRAFT.sql').write_text(registration)
notes='''# S02 bounded metadata quality hold — root review draft

The existing generic VALIDATE operation spent about120seconds measuring134,931canonical records and was safely canceled. Its one execution, one failure, originalattempt and complete source/producer checkpoints remain evidence. The missing company-report denominator and exact historical availability already require a quality hold.

This isolated helper is bound only to S02 VALIDATE, outside the existing producer bundle. It reads at most145page headers (the actual archive has135),135source-document metadata rows without bodies, three completed producer tasks/final attempts, and three terminal commit receipts by unique keys. It never scans source_observation, expected_observation or massive_shortinterest_record_v1. It reports BLOCKED with the original substantive company/report/publication requirements; it creates no validation PASS, no successor, no company mapping and no publication eligibility.

The page134 checkpoints on completed producers are intentional existing pipeline COMPLETE behavior. The handler checks their page135 terminal commit receipts instead. It checks producer completion metadata, not the independently re-parsed data result. Source accuracy and company completeness remain separate.

Root execution order: pause bound cron596 and drain leases; install the schema helper; execute the entire rollback fixture unchanged; retain its receipt and persist it as S02_METADATA_QUALITY_HOLD_ROLLBACK_VERIFIED; then run the guarded registration. Registration admits exactly the existing canceled VALIDATE task once with all prior counters/input versions unchanged and leaves cron paused. Root separately resumes the scheduler and reads the resulting durable quality hold.

The rollback uses the actual135-page metadata and final producer receipts, exercises nine negative guards (owner, token, stage, immutable input, incomplete producer, incomplete page normalization metadata, publication gate, terminal commit integrity and missing immutable output IDs), compares active replay, and checks finish replay. It does not issue provider requests. All temporary metadata/task/adapter changes roll back. It does not claim to test every data row or verify provider accuracy. The actual PostgreSQL result and runtime are required before admission; local generation/static review is not execution evidence.

Root may retain the historical cancellation trace and the new blocker side by side. No retry of the generic full scan is proposed. The all135 independent canonical verifier is a separate source-only audit and is not modified here.
'''
(P/'s02_metadata_quality_hold_ROOT_NOTES.md').write_text(notes)
files=['build_s02_metadata_quality_hold_v1.py','s02_metadata_quality_hold_schema_DRAFT.sql','s02_metadata_quality_hold_header_query_FRAGMENT.sql','s02_metadata_quality_hold_rollback_DRAFT.sql','s02_metadata_quality_hold_registration_DRAFT.sql','s02_metadata_quality_hold_ROOT_NOTES.md']
manifest={'status':'LOCAL_DRAFT_REQUIRES_ROOT_ACTUAL_ROLLBACK','scope_sha256':scope,'source_bundle_sha256':bundle,'original_validate_task':validate,'original_cancelled_attempt':cancelled,'read_bound':{'page_headers':145,'actual_pages':135,'actual_source_occurrences_not_scanned':134931,'producer_tasks':3,'terminal_commit_keys':3},'files':[{ 'file':n,'bytes':(P/n).stat().st_size,'sha256':hashlib.sha256((P/n).read_bytes()).hexdigest()} for n in files]}
(P/'s02_metadata_quality_hold_manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
print(json.dumps(manifest,indent=2))
