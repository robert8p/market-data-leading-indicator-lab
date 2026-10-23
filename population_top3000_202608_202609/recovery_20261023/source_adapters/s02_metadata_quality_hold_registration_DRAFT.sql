-- ROOT ONLY after actual unchanged-code rollback PASS is persisted in the named pipeline_event.
-- One reviewed VALIDATE retry; original input versions, checkpoint and prior failure/attempt history are preserved.
-- Leaves cron596 PAUSED. No source/typed rows, REPAIR binding, source bundle or publication gates are changed.
BEGIN;
SET LOCAL statement_timeout='8s';SET LOCAL lock_timeout='500ms';
DO $admit$
DECLARE v_hash text;v_evidence jsonb;v_headers jsonb;v_before_task jsonb;v_after_task jsonb;v_updated integer;
BEGIN

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

 v_hash:=encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.massive_shortinterest_metadata_quality_hold_v1(uuid,text)'::regprocedure),'UTF8')),'hex');
 SELECT e.details INTO v_evidence FROM population_top3000_202608_202609_v1.pipeline_event e WHERE e.event='S02_METADATA_QUALITY_HOLD_ROLLBACK_VERIFIED'
 AND e.details->>'result'='PASS' AND e.details->>'test'='ACTUAL_S02_135_PAGE_METADATA_ONLY_QUALITY_HOLD_ROLLBACK'
 AND e.details->>'tested_function_sha256'=v_hash AND e.details->>'source_bundle_sha256'='18f46852bf04cd495613e333c2fcedc9df66c9e3e3c68600070d3d77314ad15b'
 AND e.details->>'scope_sha256'='8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd' AND e.details->>'owner_fence'='2' AND e.details->>'stop_generation'='0'
 AND e.details->>'negative_guards'='9' AND e.details->>'active_replay_identical'='true' AND e.details->>'finished_replay_idempotent'='true'
 AND e.details->>'new_provider_requests'='0' AND e.details->>'source_rows_modified'='false'
 AND e.details->>'source_or_canonical_rows_scanned'='false' AND e.details->>'publication_admitted'='false'
 AND e.details->>'prior_failure_counters_preserved'='true' AND e.details->>'expected_batch_seconds'='5'
 AND jsonb_typeof(e.details->'handler_elapsed_ms')='number' AND (e.details->>'handler_elapsed_ms')::numeric BETWEEN 0 AND 5000
 ORDER BY e.at DESC LIMIT 1;
 IF v_evidence IS NULL THEN RAISE EXCEPTION 'S02_METADATA_ACTUAL_CURRENT_CODE_ROLLBACK_REQUIRED';END IF;
 SELECT to_jsonb(t) INTO v_before_task FROM population_top3000_202608_202609_v1.pipeline_task t WHERE t.task_id='3443b551-f67a-4f4c-841b-9f6a67532eac' FOR UPDATE;
 IF v_before_task IS DISTINCT FROM v_evidence->'original_task'
 OR v_before_task->>'status' IS DISTINCT FROM 'BLOCKED' OR v_before_task->>'blocker_code' IS DISTINCT FROM 'DATABASE_QUERY_CANCELED'
 OR v_before_task->>'executions' IS DISTINCT FROM '1' OR v_before_task->>'cumulative_failures' IS DISTINCT FROM '1'
 OR v_before_task->>'attempt_id' IS DISTINCT FROM '02e60669-6ddf-4a91-91bf-0aba7d64e0fe'
 OR (SELECT to_jsonb(a) FROM population_top3000_202608_202609_v1.pipeline_attempt a WHERE a.attempt_id='02e60669-6ddf-4a91-91bf-0aba7d64e0fe') IS DISTINCT FROM v_evidence->'original_cancelled_attempt'
 OR (SELECT to_jsonb(q) FROM population_top3000_202608_202609_v1.pipeline_partition q WHERE q.family_id='S02' AND q.partition_key='short-interest-settlement-20260715-20260930') IS DISTINCT FROM v_evidence->'original_partition'
 OR (SELECT jsonb_agg(to_jsonb(t) ORDER BY t.stage) FROM population_top3000_202608_202609_v1.pipeline_task t WHERE t.task_id IN('d4a27e18-ff2f-49e9-86c3-7200f2ee8814'::uuid,'4f12f545-ca86-4e03-8c57-29da768e5def'::uuid,'5b5ec9d2-bb1b-478a-b51f-86df942a5258'::uuid)) IS DISTINCT FROM v_evidence->'original_producer_tasks'
 THEN RAISE EXCEPTION 'S02_METADATA_TESTED_TASK_PRODUCER_OR_CANCELLATION_CHANGED';END IF;

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

 IF v_headers IS DISTINCT FROM v_evidence->'source_headers' THEN RAISE EXCEPTION 'S02_METADATA_TESTED_SOURCE_HEADERS_CHANGED';END IF;
 UPDATE population_top3000_202608_202609_v1.pipeline_adapter SET function_oid='population_top3000_202608_202609_v1.massive_shortinterest_metadata_quality_hold_v1(uuid,text)'::regprocedure::oid,function_sha256=v_hash,
  producer_revision='s02-bounded-metadata-quality-hold-v1',expected_batch_seconds=5,
  reviewed_test_evidence=reviewed_test_evidence||jsonb_build_object('bounded_metadata_quality_hold',v_evidence,
   'source_producer_bundle_preserved',true,'source_or_canonical_row_measurement_performed',false,'publication_admitted',false)
 WHERE family_id='S02' AND stage='VALIDATE' AND function_oid='population_top3000_202608_202609_v1.pipeline_validate_v1(uuid,text)'::regprocedure::oid
 AND function_sha256='3c139ee0eb3a10f37c79dc7fe6827d5450cbc1403b44e1dee3a5aada652e360d' AND enabled AND resource_class='DATABASE' AND provider IS NULL;
 GET DIAGNOSTICS v_updated=ROW_COUNT;IF v_updated<>1 THEN RAISE EXCEPTION 'S02_METADATA_ORIGINAL_VALIDATE_BINDING_CHANGED';END IF;
 UPDATE population_top3000_202608_202609_v1.pipeline_task SET status='READY',not_before=clock_timestamp(),blocker_code=NULL,blocker_until=NULL,resumption_condition=NULL
 WHERE task_id='3443b551-f67a-4f4c-841b-9f6a67532eac';
 SELECT to_jsonb(t) INTO v_after_task FROM population_top3000_202608_202609_v1.pipeline_task t WHERE t.task_id='3443b551-f67a-4f4c-841b-9f6a67532eac';
 IF (v_after_task-ARRAY['status','not_before','blocker_code','blocker_until','resumption_condition']) IS DISTINCT FROM
  (v_before_task-ARRAY['status','not_before','blocker_code','blocker_until','resumption_condition'])
 THEN RAISE EXCEPTION 'S02_METADATA_UNREVIEWED_TASK_FIELD_CHANGED';END IF;
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(task_id,event,details) VALUES('3443b551-f67a-4f4c-841b-9f6a67532eac','S02_METADATA_QUALITY_HOLD_ADMITTED',
  jsonb_build_object('owner_id','top3000-recovery-20261023-root-4b1314bcb5c2','owner_fence',2,'stop_generation',0,'scope_sha256','8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd',
   'new_handler_sha256',v_hash,'old_generic_validator_sha256','3c139ee0eb3a10f37c79dc7fe6827d5450cbc1403b44e1dee3a5aada652e360d','source_bundle_sha256','18f46852bf04cd495613e333c2fcedc9df66c9e3e3c68600070d3d77314ad15b',
   'source_headers',v_headers,'original_task',v_before_task,'original_cancelled_attempt',v_evidence->'original_cancelled_attempt',
   'reviewed_retry_only',true,'failure_and_execution_counters_reset',false,'source_or_typed_rows_modified',false,
   'publication_admitted',false,'new_provider_requests',0,'cron_remains_paused',true));
 IF EXISTS(SELECT 1 FROM cron.job WHERE jobid=596 AND active) THEN RAISE EXCEPTION 'S02_METADATA_CRON_UNEXPECTEDLY_ACTIVE';END IF;
END $admit$;
COMMIT;
