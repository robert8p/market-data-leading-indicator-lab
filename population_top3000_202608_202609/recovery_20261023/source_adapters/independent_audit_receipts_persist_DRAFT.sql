-- ROOT ONLY. Replace exactly the three JSON placeholders with retained actual results:
-- MINUTE: complete {query_sha256, receipt} wrapper; D02: collector report object;
-- S02: the final all-135-page canonical reconciliation row object (not a tool wrapper).
-- This records historical audit evidence only. It does not rerun a verifier or change
-- tasks, source rows, quality gates, counters, scheduler state, ownership or admissions.
-- pipeline_event.at remains the insertion time; details.observed_at is audit time.
-- Identical replay is a no-op. Changed evidence under the same identity is rejected.
BEGIN;
SET LOCAL search_path=pg_catalog;
SET LOCAL timezone='UTC';
SET LOCAL statement_timeout='8s';
SET LOCAL lock_timeout='500ms';
DO $persist_audits$
DECLARE
 v_minute jsonb := $minute_json$__MINUTE_AUDIT_JSON__$minute_json$::jsonb;
 v_daily jsonb := $daily_json$__D02_AUDIT_JSON__$daily_json$::jsonb;
 v_short jsonb := $short_json$__S02_AUDIT_JSON__$short_json$::jsonb;
 v_control population_top3000_202608_202609_v1.acquisition_control%rowtype;
 v_pipeline population_top3000_202608_202609_v1.pipeline_control%rowtype;
 v_payload jsonb; v_receipt jsonb; v_details jsonb; v_existing jsonb;
 v_i integer; v_existing_count integer; v_inserted integer:=0; v_replayed integer:=0;
 v_event text; v_family text; v_identity text; v_observed timestamptz; v_payload_sha text;
 v_scope constant text:='8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd';
BEGIN
 IF NOT pg_try_advisory_xact_lock(2026083000261022) THEN
  RAISE EXCEPTION 'AUDIT_PERSISTENCE_CONTROLLER_BUSY';
 END IF;
 SELECT * INTO v_control FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton FOR UPDATE;
 IF NOT FOUND THEN RAISE EXCEPTION 'AUDIT_PERSISTENCE_CONTROL_MISSING'; END IF;
 SELECT * INTO v_pipeline FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton FOR UPDATE;
 IF NOT FOUND THEN RAISE EXCEPTION 'AUDIT_PERSISTENCE_PIPELINE_CONTROL_MISSING'; END IF;
 IF v_control.owner_id IS DISTINCT FROM 'top3000-recovery-20261023-root-4b1314bcb5c2'
  OR v_control.owner_fence IS DISTINCT FROM 2::bigint OR v_control.stop_generation IS DISTINCT FROM 0::bigint
  OR v_pipeline.expected_owner_id IS DISTINCT FROM v_control.owner_id
  OR v_pipeline.expected_owner_fence IS DISTINCT FROM v_control.owner_fence
  OR v_pipeline.expected_stop_generation IS DISTINCT FROM v_control.stop_generation
  OR v_control.scope_sha256 IS DISTINCT FROM v_scope OR v_pipeline.scope_sha256 IS DISTINCT FROM v_scope
 THEN RAISE EXCEPTION 'AUDIT_PERSISTENCE_OWNER_FENCE_STOP_OR_SCOPE_CHANGED'; END IF;
 IF v_control.stopped IS DISTINCT FROM false OR v_control.hold_reason IS NOT NULL
  OR v_control.admitted_until IS NULL OR v_control.admitted_until<=clock_timestamp()
  OR (v_pipeline.deadline IS NOT NULL AND v_pipeline.deadline<=clock_timestamp())
 THEN RAISE EXCEPTION 'AUDIT_PERSISTENCE_STOPPED_HELD_OR_ADMISSION_EXPIRED'; END IF;

 FOR v_i IN 1..3 LOOP
  v_payload:=CASE v_i WHEN 1 THEN v_minute WHEN 2 THEN v_daily ELSE v_short END;
  IF jsonb_typeof(v_payload) IS DISTINCT FROM 'object'
   OR octet_length(convert_to(v_payload::text,'UTF8'))>262144
  THEN RAISE EXCEPTION 'AUDIT_PERSISTENCE_BOUNDED_ACTUAL_OBJECT_REQUIRED'; END IF;
  v_receipt:=CASE WHEN v_i=1 THEN v_payload->'receipt' ELSE v_payload END;
  IF jsonb_typeof(v_receipt) IS DISTINCT FROM 'object'
   OR v_receipt->'independent_provider_accuracy_verified' IS DISTINCT FROM 'false'::jsonb
  THEN RAISE EXCEPTION 'AUDIT_PERSISTENCE_SOURCE_ONLY_RECEIPT_REQUIRED'; END IF;

  IF v_i=1 THEN
   v_family:='P01'; v_event:='P01_MINUTE_PILOT_INDEPENDENT_RECONCILIATION_VERIFIED';
   v_observed:='2026-10-23T15:36:11.772754Z'::timestamptz;
   v_identity:='p01-aapl-20260908-independent-20261023T153611.772754Z-v1';
   IF v_payload->>'query_sha256' IS DISTINCT FROM 'd8b2a5f978f37ca60f590b9d3aefe0df8650bd79cc62d55d500d165d27eee96a'
    OR v_receipt->>'result' IS DISTINCT FROM 'PASS_SOURCE_TRANSFORMATION_ONLY'
    OR v_receipt->>'verifier_revision' IS DISTINCT FROM 'MASSIVE_MINUTE_888_INDEPENDENT_V1'
    OR (v_receipt->>'observed_at')::timestamptz IS DISTINCT FROM v_observed
    OR v_receipt->>'source_id' IS DISTINCT FROM 'p01-massive-minute-v1:d3ed13031b937c7f84d0017dc5432cdee2b75d73429464e02a7c26c4a4d18b5d:page:1'
    OR v_receipt->>'source_sha256' IS DISTINCT FROM 'b227d73395a38f11d5838b98235834aeb9138dc666837a00d0c0adbd121f308a'
    OR v_receipt->'source_pages' IS DISTINCT FROM '1'::jsonb
    OR v_receipt->'expected_source_records' IS DISTINCT FROM '888'::jsonb
    OR v_receipt->'expected_source_bytes' IS DISTINCT FROM '90747'::jsonb
    OR v_receipt->'source_and_typed_versions_unchanged' IS DISTINCT FROM 'true'::jsonb
    OR v_receipt->>'before_manifest_sha256' IS DISTINCT FROM 'f9117f4b618c1d9a9838f886b3f448be4630e09b51db3099edb1d0357edefbe0'
    OR v_receipt->>'after_manifest_sha256' IS DISTINCT FROM v_receipt->>'before_manifest_sha256'
    OR v_receipt->'company_population_verified' IS DISTINCT FROM 'false'::jsonb
    OR v_receipt->'sip_feed_verified' IS DISTINCT FROM 'false'::jsonb
    OR v_receipt->'historical_receipt_proven' IS DISTINCT FROM 'false'::jsonb
    OR v_receipt->'publication_admitted' IS DISTINCT FROM 'false'::jsonb
   THEN RAISE EXCEPTION 'MINUTE_ACTUAL_INDEPENDENT_RECEIPT_MISMATCH'; END IF;
   IF jsonb_typeof(v_receipt#>'{checks,mismatches}') IS DISTINCT FROM 'object'
    OR jsonb_typeof(v_receipt#>'{checks,field_mismatch_counts}') IS DISTINCT FROM 'object'
   THEN RAISE EXCEPTION 'MINUTE_ACTUAL_MISMATCH_OBJECTS_REQUIRED'; END IF;
   IF v_receipt#>'{checks,mismatches}' IS DISTINCT FROM '{"view_count":0,"batch_proof":0,"calendar_link":0,"missing_typed":0,"summary_values":0,"typed_manifest":0,"source_envelope":0,"chain_or_archive":0,"unexpected_typed":0,"exact_page_anchor":0,"source_page_count":0,"acquisition_receipt":0,"source_record_shape":0,"typed_field_or_mask":0,"completed_data_stages":0,"provisional_partition":0,"source_or_typed_count":0,"source_symbol_conflict":0,"source_document_integrity":0,"view_link_or_false_qualification":0}'::jsonb
    OR v_receipt#>'{checks,field_mismatch_counts}' IS DISTINCT FROM '{"trade_count":0,"bar_start_at":0,"invalid_mask":0,"missing_mask":0,"provider_low":0,"provider_otc":0,"share_volume":0,"provider_high":0,"provider_open":0,"provider_vwap":0,"provider_close":0,"structure_mask":0,"provider_ny_date":0,"provider_time_ms":0,"raw_record_sha256":0}'::jsonb
   THEN RAISE EXCEPTION 'MINUTE_ACTUAL_ZERO_MISMATCH_GATES_REQUIRED'; END IF;
  ELSIF v_i=2 THEN
   v_family:='D02'; v_event:='D02_ALL_SOURCE_DATES_INDEPENDENT_RECONCILIATION_VERIFIED';
   v_observed:='2026-10-23T15:47:05.261899Z'::timestamptz;
   v_identity:='d02-all42-independent-20261023T154146.033343Z-154705.261899Z-v1';
   IF v_receipt->>'result' IS DISTINCT FROM 'PASS_SOURCE_ARCHIVE_RECONCILIATION_ONLY'
    OR v_receipt->'expected_source_dates' IS DISTINCT FROM '42'::jsonb
    OR v_receipt->'source_dates_reconciled_and_postchecked' IS DISTINCT FROM '42'::jsonb
    OR v_receipt->'all_42_source_dates_reconciled' IS DISTINCT FROM 'true'::jsonb
    OR v_receipt->'total_source_occurrences' IS DISTINCT FROM '525999'::jsonb
    OR v_receipt->'issues' IS DISTINCT FROM '[]'::jsonb
    OR v_receipt->>'calendar_date_list_sha256' IS DISTINCT FROM '188cc82e93e2c3446d9fba86cec682b996913997f56a2263d3fd680475998368'
    OR v_receipt->>'before_snapshot_manifest_sha256' IS DISTINCT FROM '3d854024c1050909658430ad87847e566d86bb1a257a8f1e85e73a1eef3311b3'
    OR v_receipt->>'after_snapshot_manifest_sha256' IS DISTINCT FROM v_receipt->>'before_snapshot_manifest_sha256'
    OR v_receipt->'per_date_snapshot_verification' IS DISTINCT FROM 'true'::jsonb
    OR v_receipt->'company_population_verified' IS DISTINCT FROM 'false'::jsonb
    OR v_receipt->'company_day_completeness' IS DISTINCT FROM 'null'::jsonb
    OR v_receipt->'original_historical_receipt_proven' IS DISTINCT FROM 'false'::jsonb
   THEN RAISE EXCEPTION 'D02_ACTUAL_ALL42_COLLECTOR_RECEIPT_REQUIRED'; END IF;
   IF jsonb_typeof(v_receipt->'dates') IS DISTINCT FROM 'array' THEN RAISE EXCEPTION 'D02_DATE_RECEIPTS_REQUIRED'; END IF;
   IF jsonb_array_length(v_receipt->'dates')<>42
    OR EXISTS(SELECT 1 FROM jsonb_array_elements(v_receipt->'dates') d WHERE d->>'status' IS DISTINCT FROM 'RECONCILED_AND_POSTCHECKED')
   THEN RAISE EXCEPTION 'D02_ALL42_POSTCHECKED_DATE_RECEIPTS_REQUIRED'; END IF;
  ELSE
   v_family:='S02'; v_event:='S02_ALL_SOURCE_PAGES_INDEPENDENT_RECONCILIATION_VERIFIED';
   IF v_receipt->>'result' IS DISTINCT FROM 'PASS_SOURCE_TRANSFORMATION_ONLY'
    OR v_receipt->>'check_revision' IS DISTINCT FROM 'S02_ALL135_RETAINED_SOURCE_CANONICAL_RECONCILIATION_V1'
    OR v_receipt->'source_pages' IS DISTINCT FROM '135'::jsonb
    OR v_receipt->'source_occurrences' IS DISTINCT FROM '134931'::jsonb
    OR v_receipt->'mismatches' IS DISTINCT FROM '{"global_mismatch_fields":0,"plan_before_changed":0,"receipt_coverage":0,"receipt_content_or_page_binding":0}'::jsonb
    OR v_receipt->'company_completeness_verified' IS DISTINCT FROM 'false'::jsonb
    OR v_receipt->'historical_availability_certified' IS DISTINCT FROM 'false'::jsonb
    OR v_receipt->'publication_admitted' IS DISTINCT FROM 'false'::jsonb
    OR v_receipt->'original_wire_bytes_independently_rehashed' IS DISTINCT FROM 'false'::jsonb
   THEN RAISE EXCEPTION 'S02_ACTUAL_ALL135_CANONICAL_RECEIPT_REQUIRED'; END IF;
   IF jsonb_typeof(v_receipt->'global_mismatches') IS DISTINCT FROM 'object'
    OR coalesce(v_receipt->>'plan_sha256','')!~'^[0-9a-f]{64}$'
    OR coalesce(v_receipt->>'page_receipts_sha256','')!~'^[0-9a-f]{64}$'
   THEN RAISE EXCEPTION 'S02_ACTUAL_GLOBAL_PROOF_BINDINGS_REQUIRED'; END IF;
   IF v_receipt->'global_mismatches' IS DISTINCT FROM '{"acquire_revision_or_helper_bundle":0,"normalization_or_derivation_not_complete":0,"source_page_or_occurrence_count":0,"page_sequence_or_terminal":0,"page_cursor_or_task":0,"source_metadata":0,"staged_page_count_or_range":0,"staged_occurrence_without_page":0,"observation_missing":0,"observation_unexpected":0,"expectation_missing":0,"expectation_unexpected":0,"partition_denominator_or_publication":0,"preparation_parts_binding":0}'::jsonb
   THEN RAISE EXCEPTION 'S02_ACTUAL_GLOBAL_ZERO_MISMATCHES_REQUIRED'; END IF;
   IF v_receipt->>'preparation_method' IS DISTINCT FROM 'SEPARATE_BOUNDED_READ_ONLY_PARTS_WITH_FRESH_BEFORE_AFTER_V3'
    OR jsonb_typeof(v_receipt->'preparation_parts_manifest') IS DISTINCT FROM 'object'
   THEN RAISE EXCEPTION 'S02_ACTUAL_V3_PREPARATION_BINDINGS_REQUIRED'; END IF;
   IF (SELECT array_agg(k ORDER BY k) FROM jsonb_object_keys(v_receipt->'preparation_parts_manifest') k)
     IS DISTINCT FROM ARRAY['canonical','expected','observed','staging']::text[]
    OR EXISTS(SELECT 1 FROM jsonb_each_text(v_receipt->'preparation_parts_manifest') p
      WHERE coalesce(p.value,'')!~'^[0-9a-f]{64}$')
    OR NOT coalesce(pg_input_is_valid(v_receipt->>'preparation_parts_earliest_at','timestamp with time zone'),false)
    OR NOT coalesce(pg_input_is_valid(v_receipt->>'preparation_parts_latest_at','timestamp with time zone'),false)
   THEN RAISE EXCEPTION 'S02_ACTUAL_V3_PREPARATION_HASHES_OR_TIMES_REQUIRED'; END IF;
   IF coalesce(v_receipt->>'observed_at','')!~'(Z|[+-][0-9]{2}(:[0-9]{2})?)$'
   THEN RAISE EXCEPTION 'S02_ACTUAL_AUDIT_TIME_WITH_TIMEZONE_REQUIRED'; END IF;
   v_observed:=(v_receipt->>'observed_at')::timestamptz;
   IF (v_receipt->>'preparation_parts_earliest_at')::timestamptz>(v_receipt->>'preparation_parts_latest_at')::timestamptz
    OR (v_receipt->>'preparation_parts_latest_at')::timestamptz>v_observed
   THEN RAISE EXCEPTION 'S02_ACTUAL_V3_PREPARATION_TIME_ORDER_REQUIRED'; END IF;
   v_identity:='s02-all135-independent-'||to_char(v_observed AT TIME ZONE 'UTC','YYYYMMDD"T"HH24MISS.US"Z"')||'-v1';
  END IF;
  IF v_observed<'2026-10-23T10:49:16.549857Z'::timestamptz OR v_observed>clock_timestamp()
  THEN RAISE EXCEPTION 'AUDIT_PERSISTENCE_ACTUAL_OBSERVATION_TIME_REQUIRED'; END IF;
  v_payload_sha:=encode(sha256(convert_to(v_payload::text,'UTF8')),'hex');
  v_details:=v_receipt||jsonb_build_object('audit_identity',v_identity,'family_id',v_family,
   'observed_at',v_observed,'owner_id',v_control.owner_id,'owner_fence',v_control.owner_fence,
   'stop_generation',v_control.stop_generation,'scope_sha256',v_scope,
   'audit_payload',v_payload,'audit_payload_canonical_json_sha256',v_payload_sha,
   'audit_persistence_only',true,'tasks_or_quality_gates_changed',false,
   'dataset_certification_promoted',false,'new_provider_requests',0);
  IF v_i=1 THEN v_details:=v_details||jsonb_build_object('executed_query_sha256',v_payload->>'query_sha256'); END IF;
  IF v_i=2 THEN v_details:=v_details||jsonb_build_object(
   'global_before_observed_at','2026-10-23T15:41:46.033343Z',
   'global_after_observed_at','2026-10-23T15:47:05.261899Z'); END IF;
  SELECT count(*) INTO v_existing_count FROM population_top3000_202608_202609_v1.pipeline_event ev
   WHERE ev.event=v_event AND ev.details->>'audit_identity'=v_identity;
  IF v_existing_count>1 THEN RAISE EXCEPTION 'AUDIT_PERSISTENCE_DUPLICATE_IDENTITY_REQUIRES_REVIEW'; END IF;
  IF v_existing_count=1 THEN
   SELECT ev.details INTO v_existing FROM population_top3000_202608_202609_v1.pipeline_event ev
    WHERE ev.event=v_event AND ev.details->>'audit_identity'=v_identity;
   IF v_existing IS DISTINCT FROM v_details THEN RAISE EXCEPTION 'AUDIT_PERSISTENCE_IDENTITY_CONTENT_CONFLICT'; END IF;
   v_replayed:=v_replayed+1;
  ELSE
   INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES(v_event,v_details);
   v_inserted:=v_inserted+1;
  END IF;
 END LOOP;
 RAISE NOTICE 'AUDIT_EVIDENCE_ONLY inserted=% identical_replays=%; no task or quality changes',v_inserted,v_replayed;
END $persist_audits$;
COMMIT;
