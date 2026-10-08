DO $preconditions$ BEGIN
IF (SELECT encode(sha256(convert_to(pg_get_functiondef(p.oid),'UTF8')),'hex') FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='astra_shared_equity_features_20260928_v1' AND p.proname='rebuild_feature_sessions_remediated_v1') IS DISTINCT FROM '200b09a5e7eb2aa72bbf06a790b359ffca00f7624fe6041553ba3209eb3fd882' THEN RAISE EXCEPTION 'function_predecessor_changed: rebuild_feature_sessions_remediated_v1';END IF;
IF (SELECT encode(sha256(convert_to(pg_get_functiondef(p.oid),'UTF8')),'hex') FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='market_governance' AND p.proname='remediation_import_native_listing_page_v1') IS DISTINCT FROM 'b33b1dcd166622069dbfe93c998e7f2b8cc0dd3d5246f655e14fe591b294ed29' THEN RAISE EXCEPTION 'function_predecessor_changed: remediation_import_native_listing_page_v1';END IF;
IF (SELECT encode(sha256(convert_to(pg_get_functiondef(p.oid),'UTF8')),'hex') FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='public' AND p.proname='market_data_remediation_compact_feature_step_v1') IS DISTINCT FROM '79bd4b77834fa245fae4efa6ebfdab4d039915ebac9034ff5c911011c2558edd' THEN RAISE EXCEPTION 'function_predecessor_changed: market_data_remediation_compact_feature_step_v1';END IF;
IF (SELECT encode(sha256(convert_to(pg_get_functiondef(p.oid),'UTF8')),'hex') FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='public' AND p.proname='market_data_remediation_native_listing_step_v1') IS DISTINCT FROM '0db28a8c8dc6a2f4aaf900b39a82d66308b8802ab73baf2221c8bf1675493889' THEN RAISE EXCEPTION 'function_predecessor_changed: market_data_remediation_native_listing_step_v1';END IF;
IF (SELECT encode(sha256(convert_to(pg_get_functiondef(p.oid),'UTF8')),'hex') FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='market_governance' AND p.proname='remediation_physical_capacity_gate_v2') IS DISTINCT FROM '03fc7c69973aa0769856851f5307160503c67bd479c6a36cdb7cd52b65b785ed' THEN RAISE EXCEPTION 'function_predecessor_changed: remediation_physical_capacity_gate_v2';END IF;
IF (SELECT encode(sha256(convert_to(pg_get_functiondef(p.oid),'UTF8')),'hex') FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='market_governance' AND p.proname='remediation_worker_gate_v1') IS DISTINCT FROM 'c736c794446a1ba5a22b4d4e46a61efcd906b17249679e15125245f7e1950f73' THEN RAISE EXCEPTION 'function_predecessor_changed: remediation_worker_gate_v1';END IF;
IF (SELECT encode(sha256(convert_to(pg_get_functiondef(p.oid),'UTF8')),'hex') FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='public' AND p.proname='market_data_remediation_capacity_observation_v2') IS DISTINCT FROM 'e2ddbe7ceb444959456b49b64e0b7c163b598a84bf56af7bef7943dec340e4cb' THEN RAISE EXCEPTION 'function_predecessor_changed: market_data_remediation_capacity_observation_v2';END IF;
IF (SELECT encode(sha256(convert_to(pg_get_functiondef(p.oid),'UTF8')),'hex') FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='public' AND p.proname='market_data_remediation_claim_v1') IS DISTINCT FROM '7412430c71aa3af0c57603a934587f8ac1ecfc17e173306255d3e51d7092b2cb' THEN RAISE EXCEPTION 'function_predecessor_changed: market_data_remediation_claim_v1';END IF;
IF (SELECT encode(sha256(convert_to(pg_get_functiondef(p.oid),'UTF8')),'hex') FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='market_governance' AND p.proname='remediation_binance_daily_frozen_task_v1') IS DISTINCT FROM 'eb7784699900d8952c7565091843de57946f9de96150b6079a7fe9b973c083e3' THEN RAISE EXCEPTION 'function_predecessor_changed: remediation_binance_daily_frozen_task_v1';END IF;
IF (SELECT encode(sha256(convert_to(pg_get_functiondef(p.oid),'UTF8')),'hex') FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='market_governance' AND p.proname='remediation_coinbase_sealed_task_v1') IS DISTINCT FROM '1c9a03accf7ccf436c3d98fdc1f6a28e1bd70df7b92d17f9456c21a0b0225da7' THEN RAISE EXCEPTION 'function_predecessor_changed: remediation_coinbase_sealed_task_v1';END IF;
IF (SELECT encode(sha256(convert_to(pg_get_functiondef(p.oid),'UTF8')),'hex') FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='market_governance' AND p.proname='remediation_enable_compact_feature_lane_v1') IS DISTINCT FROM 'e97e3454e4b17c4290cd3df5e8c860f890fd5433a109f66cc22fd056b4d92577' THEN RAISE EXCEPTION 'function_predecessor_changed: remediation_enable_compact_feature_lane_v1';END IF;
IF (SELECT encode(sha256(convert_to(pg_get_functiondef(p.oid),'UTF8')),'hex') FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='market_governance' AND p.proname='remediation_enable_native_listing_interleave_v1') IS DISTINCT FROM '62817be2d1b6ed59e23248133d1e5cb7b93eabb472555af6dd0028be02f8e78c' THEN RAISE EXCEPTION 'function_predecessor_changed: remediation_enable_native_listing_interleave_v1';END IF;
END $preconditions$;
SET LOCAL lock_timeout='10s';
SELECT pg_advisory_xact_lock(hashtextextended('market_data_remediation_20261008_v1',0));
SELECT pg_advisory_xact_lock(hashtextextended('market_data_remediation_compact_shared_features_v1',0));
SELECT pg_advisory_xact_lock(hashtextextended('market_data_remediation_native_listing_finite_runner_v1',0));

CREATE OR REPLACE FUNCTION market_governance.remediation_budgets_removed_v1()
RETURNS boolean LANGUAGE sql STABLE SET search_path TO 'pg_catalog' AS $policy$
SELECT coalesce((SELECT r.protections#>>'{budget_policy,mode}'='UNLIMITED_USER_AUTHORIZED'
 AND r.protections#>>'{budget_policy,authorization_change_id}'='OPS:BUDGET_REMOVAL:20261008_130925'
 AND EXISTS(SELECT 1 FROM market_governance.remediation_change_v1 c WHERE c.change_id='OPS:BUDGET_REMOVAL:20261008_130925' AND c.run_id=r.run_id AND c.evidence->>'explicit_user_instruction'='Remove budgets applies to everything')
FROM market_governance.remediation_run_v1 r WHERE r.run_id='market_data_remediation_20261008_v1' AND r.project_ref='oxzabweahkoimtevbbny'),false)
$policy$;
REVOKE ALL ON FUNCTION market_governance.remediation_budgets_removed_v1() FROM PUBLIC,anon,authenticated;
GRANT EXECUTE ON FUNCTION market_governance.remediation_budgets_removed_v1() TO service_role;

CREATE OR REPLACE FUNCTION public.market_data_remediation_budget_policy_v1(p_request jsonb)
RETURNS jsonb LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path TO 'pg_catalog' AS $policy$
BEGIN
 IF p_request IS NULL OR p_request<>jsonb_build_object('run_id','market_data_remediation_20261008_v1') THEN RAISE EXCEPTION 'exact_remediation_run_required';END IF;
 RETURN jsonb_build_object('run_id','market_data_remediation_20261008_v1','project_ref','oxzabweahkoimtevbbny','budgets_removed',market_governance.remediation_budgets_removed_v1(),'authorization_change_id','OPS:BUDGET_REMOVAL:20261008_130925');
END $policy$;
REVOKE ALL ON FUNCTION public.market_data_remediation_budget_policy_v1(jsonb) FROM PUBLIC,anon,authenticated;
GRANT EXECUTE ON FUNCTION public.market_data_remediation_budget_policy_v1(jsonb) TO service_role;

-- Explicitly remove the cumulative claim ceiling while retaining the monotonic counter.
ALTER TABLE market_governance.remediation_coinbase_manifest_checkpoint_v1
 DROP CONSTRAINT remediation_coinbase_manifest_checkpoint_v1_check,
 ADD CONSTRAINT remediation_coinbase_manifest_checkpoint_v1_claim_counter_nonnegative CHECK(claims_issued>=0);

CREATE OR REPLACE FUNCTION astra_shared_equity_features_20260928_v1.rebuild_feature_sessions_remediated_v1(p_lo integer, p_hi integer, p_start date, p_end date)
 RETURNS jsonb
 LANGUAGE plpgsql
 SET search_path TO 'pg_catalog'
 SET "TimeZone" TO 'UTC'
AS $function$
DECLARE expected_count bigint;already_count bigint;written_count bigint;metrics jsonb;budget jsonb;source_budget jsonb;total_bytes bigint;max_bytes bigint;prospective_bytes bigint;max_wal numeric;start_wal numeric;current_wal numeric;expected_wal_reset timestamptz;current_wal_reset timestamptz;
BEGIN
 IF p_lo<1 OR p_hi>13277 OR p_hi<p_lo OR p_hi-p_lo>99 OR p_start<'2025-09-01' OR p_end>'2026-07-31' OR p_start>p_end OR p_end-p_start>31 THEN RAISE EXCEPTION 'Exact authorized window and bounded key/date batch required';END IF;
 SELECT protections->'shared_feature_materialization',protections->'source_acquisition' INTO budget,source_budget FROM market_governance.remediation_run_v1 WHERE run_id='market_data_remediation_20261008_v1'AND project_ref='oxzabweahkoimtevbbny'AND phase IN(3,4);
 IF NOT market_governance.remediation_budgets_removed_v1() AND (budget IS NULL OR budget->>'no_new_paid_service_costs' IS DISTINCT FROM 'true' OR nullif(budget->>'existing_storage_headroom_evidence','')IS NULL) THEN RAISE EXCEPTION 'Coordinator measured joint storage budget required before materialization';END IF;
 max_bytes:=(budget->>'max_storage_bytes')::bigint;max_wal:=(budget->>'max_wal_growth_bytes')::numeric;start_wal:=(budget->>'wal_start_bytes')::numeric;expected_wal_reset:=(budget->>'wal_stats_reset')::timestamptz;
 IF NOT market_governance.remediation_budgets_removed_v1() AND (max_bytes IS NULL OR max_bytes<=0 OR max_wal IS NULL OR max_wal<=0 OR start_wal IS NULL OR expected_wal_reset IS NULL OR coalesce((budget->>'verified_bytes_before_storage_autoscale')::bigint,0)<max_bytes+coalesce((source_budget->>'max_storage_bytes')::bigint,0)+coalesce((budget->>'other_reserved_storage_bytes')::bigint,0)) THEN RAISE EXCEPTION 'Finite cache/WAL limits and joint headroom evidence required';END IF;
 IF market_governance.remediation_budgets_removed_v1() AND (market_governance.remediation_physical_capacity_gate_v2(67108864)->>'ready') IS DISTINCT FROM 'true' THEN RAISE EXCEPTION 'actual_physical_capacity_not_ready'; END IF;
 PERFORM pg_advisory_xact_lock(hashtextextended('market_data_remediation_compact_shared_features_v1',0));
 SELECT count(*)INTO expected_count FROM reference.alpaca_fixed_window_instruments_v1 u CROSS JOIN market_governance.us_equity_fixed_session_calendar c WHERE u.instrument_key BETWEEN p_lo AND p_hi AND c.session_date BETWEEN p_start AND p_end;
 IF expected_count=0 OR expected_count>625 THEN RAISE EXCEPTION 'One to 625 scheduled instrument-sessions per transaction required';END IF;
 SELECT count(*)INTO already_count FROM astra_shared_equity_features_20260928_v1.feature_session_cache_remediated_v1 WHERE instrument_key BETWEEN p_lo AND p_hi AND session_date BETWEEN p_start AND p_end;
 IF already_count=expected_count THEN RETURN jsonb_build_object('already_complete',true,'sessions',already_count,'note','Completed rows preserved; source/hash validation is a separate gate');END IF;
 total_bytes:=pg_total_relation_size('astra_shared_equity_features_20260928_v1.feature_session_cache_remediated_v1');
 prospective_bytes:=total_bytes+(expected_count-already_count)*70000;
 SELECT wal_bytes,stats_reset INTO current_wal,current_wal_reset FROM pg_stat_wal;
 IF NOT market_governance.remediation_budgets_removed_v1() AND (prospective_bytes>max_bytes OR current_wal_reset IS DISTINCT FROM expected_wal_reset OR current_wal-start_wal+(expected_count-already_count)*140000>max_wal) THEN RAISE EXCEPTION 'Finite storage or WAL guard reached; preserve completed checkpoints';END IF;
 WITH inserted AS(
 INSERT INTO astra_shared_equity_features_20260928_v1.feature_session_cache_remediated_v1
 SELECT * FROM astra_shared_equity_features_20260928_v1.compute_feature_sessions_remediated_v1(p_lo,p_hi,p_start,p_end,true)
 RETURNING *
 )SELECT count(*),jsonb_build_object('sessions_written',count(*),'observed_minutes',coalesce(sum(aligned_source_rows),0),'scheduled_minutes',coalesce(sum(expected_grid_rows),0),'unaligned_rows',coalesce(sum(unaligned_source_rows),0),'invalid_ohlc_rows',coalesce(sum(invalid_ohlc_rows),0),'invalid_volume_rows',coalesce(sum(invalid_volume_rows),0),'source_manifest_sha256',encode(sha256(coalesce(string_agg(source_rows_sha256,''::bytea ORDER BY instrument_key,session_date),''::bytea)),'hex'),'output_manifest_sha256',encode(sha256(coalesce(string_agg(output_sha256,''::bytea ORDER BY instrument_key,session_date),''::bytea)),'hex'))INTO written_count,metrics FROM inserted;
 IF written_count<>expected_count-already_count THEN RAISE EXCEPTION 'Expected denominator mismatch';END IF;
 IF EXISTS(SELECT 1 FROM astra_shared_equity_features_20260928_v1.feature_session_cache_remediated_v1 c CROSS JOIN LATERAL generate_subscripts(c.minute_indices,1)n WHERE c.instrument_key BETWEEN p_lo AND p_hi AND c.session_date BETWEEN p_start AND p_end AND(c.minute_indices[n]<0 OR c.minute_indices[n]>=c.expected_grid_rows OR(n>1 AND c.minute_indices[n]<=c.minute_indices[n-1])OR c.validity_codes[n] NOT BETWEEN 0 AND 63))THEN RAISE EXCEPTION 'Packed clock/validity invariant failed';END IF;
 IF EXISTS(SELECT 1 FROM astra_shared_equity_features_20260928_v1.feature_session_cache_remediated_v1 c CROSS JOIN LATERAL unnest(c.coefficient_values)v WHERE c.instrument_key BETWEEN p_lo AND p_hi AND c.session_date BETWEEN p_start AND p_end AND v IS NOT NULL AND NOT market_factors_20250901_20260731_v1.finite_float_remediated_v1(v))THEN RAISE EXCEPTION 'Nonfinite derived coefficient';END IF;
 total_bytes:=pg_total_relation_size('astra_shared_equity_features_20260928_v1.feature_session_cache_remediated_v1');
 IF NOT market_governance.remediation_budgets_removed_v1() AND (total_bytes>max_bytes) THEN RAISE EXCEPTION 'Actual cache storage limit exceeded';END IF;
 RETURN metrics||jsonb_build_object('existing_sessions_preserved',already_count,'cache_total_bytes',total_bytes,'completed_at',clock_timestamp(),'version','compact_intraday_float64_v1','strict_original_receipt_recovered',false);
END $function$;

CREATE OR REPLACE FUNCTION market_governance.remediation_binance_daily_frozen_task_v1()
 RETURNS trigger
 LANGUAGE plpgsql
 SET search_path TO 'pg_catalog'
AS $function$
DECLARE old_member boolean=false;new_member boolean=false;r market_governance.remediation_run_v1%rowtype;c jsonb;g jsonb;
BEGIN
 IF TG_OP<>'INSERT' THEN old_member=OLD.request_json->>'source_generation_id'='binance_um_daily_remaining_17533_20261008_v1';END IF;
 IF TG_OP<>'DELETE' THEN new_member=NEW.request_json->>'source_generation_id'='binance_um_daily_remaining_17533_20261008_v1';END IF;
 IF NOT coalesce(old_member,false) AND NOT coalesce(new_member,false) THEN
  IF TG_OP='DELETE' THEN RETURN OLD;ELSE RETURN NEW;END IF;
 END IF;
 IF TG_OP='DELETE' THEN RAISE EXCEPTION 'binance_finite_population_immutable';END IF;
 IF market_governance.remediation_binance_daily_identity_v1(NEW) IS DISTINCT FROM true THEN RAISE EXCEPTION 'binance_finite_identity_required';END IF;
 IF TG_OP='INSERT' THEN
  SELECT * INTO STRICT r FROM market_governance.remediation_run_v1 WHERE run_id=NEW.run_id;
  c=r.protections#>'{source_acquisition,binance_daily_finite}';
  IF r.phase<>3 OR r.status<>'IN_PROGRESS' OR r.project_ref<>'oxzabweahkoimtevbbny'
   OR r.protections#>>'{source_acquisition,claims_paused}' IS DISTINCT FROM 'true'
   OR c->>'generation_id' IS DISTINCT FROM 'binance_um_daily_remaining_17533_20261008_v1' OR c->>'manifest_sha256' IS DISTINCT FROM '2825d3786055e65d9aa27e4947b836ce5a9b43cfcbb3ab35f20a23b874b5196c'
   OR c->>'enabled' IS DISTINCT FROM 'false' OR c->>'seed_complete' IS DISTINCT FROM 'false' THEN RAISE EXCEPTION 'binance_finite_seeding_closed';END IF;
  IF NEW.status<>'QUEUED' OR NEW.attempts<>0 OR NEW.worker_id IS NOT NULL OR NEW.lease_token IS NOT NULL
   OR NEW.lease_expires_at IS NOT NULL OR NEW.current_source_id IS NOT NULL OR NEW.heartbeat_at IS NOT NULL
   OR NEW.acquired_count<>0 OR NEW.valid_count<>0 OR NEW.invalid_count<>0 OR NEW.duplicate_count<>0 OR NEW.outside_count<>0
   OR NEW.validation<>'{}'::jsonb OR NEW.attempt_history<>'[]'::jsonb OR NEW.last_error IS NOT NULL OR NEW.completed_at IS NOT NULL
   THEN RAISE EXCEPTION 'binance_finite_pristine_seed_required';END IF;
  RETURN NEW;
 END IF;
 IF (NEW.batch_id,NEW.run_id,NEW.batch_key,NEW.finding_ids,NEW.provider,NEW.source_type,NEW.symbol,NEW.instrument_id,
     NEW.start_ts,NEW.end_ts,NEW.interval_seconds,NEW.request_json,NEW.priority,NEW.created_at)
  IS DISTINCT FROM(OLD.batch_id,OLD.run_id,OLD.batch_key,OLD.finding_ids,OLD.provider,OLD.source_type,OLD.symbol,OLD.instrument_id,
     OLD.start_ts,OLD.end_ts,OLD.interval_seconds,OLD.request_json,OLD.priority,OLD.created_at)
  THEN RAISE EXCEPTION 'binance_finite_identity_immutable';END IF;
 IF NEW.attempts<OLD.attempts OR NEW.attempts>OLD.attempts+1 OR NEW.attempts>3 THEN RAISE EXCEPTION 'binance_finite_attempt_monotonic';END IF;
 IF OLD.status IN('COMPLETE','BLOCKED','SOURCE_INVALID','FAILED','CANCELLED') AND NEW.status IS DISTINCT FROM OLD.status THEN
  RAISE EXCEPTION 'binance_finite_terminal_task_cannot_reopen';END IF;
 IF NEW.attempts=OLD.attempts+1 THEN
  IF OLD.status NOT IN('QUEUED','RETRY') OR NEW.status<>'RUNNING' OR NEW.worker_id IS NULL OR NEW.lease_token IS NULL
   OR NEW.lease_expires_at IS NULL THEN RAISE EXCEPTION 'binance_finite_claim_transition_required';END IF;
  SELECT * INTO STRICT r FROM market_governance.remediation_run_v1 WHERE run_id=NEW.run_id;
  c=r.protections#>'{source_acquisition,binance_daily_finite}';
  IF coalesce(c->>'execution_stage','') NOT IN('MEASUREMENT52','FULL')
   OR (c->>'execution_stage'='MEASUREMENT52' AND (NEW.start_ts<>'2025-09-01T00:00:00Z'::timestamptz OR OLD.attempts<>0))
   THEN RAISE EXCEPTION 'binance_finite_measurement_or_full_stage_required';END IF;
  IF c->>'enabled' IS DISTINCT FROM 'true' OR c->>'seed_complete' IS DISTINCT FROM 'true'
   OR c->>'generation_id' IS DISTINCT FROM 'binance_um_daily_remaining_17533_20261008_v1' OR c->>'manifest_sha256' IS DISTINCT FROM '2825d3786055e65d9aa27e4947b836ce5a9b43cfcbb3ab35f20a23b874b5196c'
   OR r.protections#>>'{source_acquisition,claims_paused}' IS DISTINCT FROM 'false'
   OR r.protections#>>'{shared_feature_runner,enabled}'='true' OR r.protections#>>'{native_listing_runner,enabled}'='true'
   OR r.protections#>>'{source_acquisition,coinbase_cohort,enabled}'='true'
   OR r.protections#>>'{source_acquisition,fomc_two_windows,enabled}'='true'
   OR (NOT market_governance.remediation_budgets_removed_v1() AND (coalesce((c->>'expires_at')::timestamptz,'-infinity'::timestamptz)<=clock_timestamp()
   OR (c->>'expires_at')::timestamptz>'2026-10-10T04:15:49.938363Z'::timestamptz)) THEN RAISE EXCEPTION 'binance_finite_claim_control_required';END IF;
  g=market_governance.remediation_worker_gate_v1(NEW.run_id);
  IF g->>'ready' IS DISTINCT FROM 'true' OR (g->>'used_bytes')::bigint+67108864>=(g->>'max_bytes')::bigint THEN
   RAISE EXCEPTION 'binance_finite_claim_capacity_required';END IF;
 ELSIF NEW.status IS DISTINCT FROM OLD.status AND NOT(OLD.status='RUNNING' AND NEW.status IN('COMPLETE','RETRY','FAILED','BLOCKED','SOURCE_INVALID','CANCELLED')) THEN
  RAISE EXCEPTION 'binance_finite_status_transition_required';
 END IF;
 RETURN NEW;
END
$function$;

CREATE OR REPLACE FUNCTION market_governance.remediation_coinbase_sealed_task_v1()
 RETURNS trigger
 LANGUAGE plpgsql
 SET search_path TO 'pg_catalog'
AS $function$
DECLARE gen text; changed integer;
BEGIN
 gen=CASE WHEN TG_OP='DELETE' THEN OLD.request_json->>'cohort_generation_id' ELSE NEW.request_json->>'cohort_generation_id' END;
 IF TG_OP='UPDATE' AND OLD.request_json->>'cohort_generation_id'='coinbase_native_minute_full_remaining_20261008_v1' THEN gen='coinbase_native_minute_full_remaining_20261008_v1';END IF;
 IF gen IS DISTINCT FROM 'coinbase_native_minute_full_remaining_20261008_v1' THEN IF TG_OP='DELETE' THEN RETURN OLD;ELSE RETURN NEW;END IF;END IF;
 IF NOT EXISTS(SELECT 1 FROM market_governance.remediation_coinbase_manifest_checkpoint_v1 WHERE generation_id=gen) THEN
  IF TG_OP='DELETE' THEN RETURN OLD;ELSE RETURN NEW;END IF;
 END IF;
 IF TG_OP<>'UPDATE' THEN RAISE EXCEPTION 'sealed_coinbase_task_population_immutable';END IF;
 IF (NEW.run_id,NEW.batch_key,NEW.finding_ids,NEW.provider,NEW.source_type,NEW.symbol,NEW.instrument_id,NEW.start_ts,NEW.end_ts,NEW.interval_seconds,NEW.request_json,NEW.priority)
  IS DISTINCT FROM(OLD.run_id,OLD.batch_key,OLD.finding_ids,OLD.provider,OLD.source_type,OLD.symbol,OLD.instrument_id,OLD.start_ts,OLD.end_ts,OLD.interval_seconds,OLD.request_json,OLD.priority)
 THEN RAISE EXCEPTION 'sealed_coinbase_task_identity_immutable';END IF;
 IF NEW.attempts=OLD.attempts THEN RETURN NEW;END IF;
 IF NEW.attempts<>OLD.attempts+1 OR NEW.attempts>3 OR NEW.status<>'RUNNING' THEN RAISE EXCEPTION 'sealed_coinbase_claim_attempt_monotonic';END IF;
 UPDATE market_governance.remediation_coinbase_manifest_checkpoint_v1 SET claims_issued=claims_issued+1,updated_at=clock_timestamp()
 WHERE generation_id=gen AND run_id=NEW.run_id AND (market_governance.remediation_budgets_removed_v1() OR (claims_issued<max_claims AND expires_at>clock_timestamp()));
 GET DIAGNOSTICS changed=ROW_COUNT;
 IF changed<>1 THEN RAISE EXCEPTION 'sealed_coinbase_global_claim_budget_or_expiry';END IF;
 RETURN NEW;
END $function$;

CREATE OR REPLACE FUNCTION market_governance.remediation_enable_compact_feature_lane_v1(p_snapshot jsonb, p_cache_cap bigint, p_source_reserve bigint, p_native_reserve bigint, p_wal_growth_cap bigint, p_expiry timestamp with time zone)
 RETURNS jsonb
 LANGUAGE plpgsql
 SET search_path TO 'pg_catalog'
 SET "TimeZone" TO 'UTC'
AS $function$
DECLARE v_run market_governance.remediation_run_v1%ROWTYPE;v_old jsonb;v_new jsonb;v_cfg jsonb;v_wal record;
 v_total bigint;v_used bigint;v_headroom bigint;v_time timestamptz;v_source_cap bigint;v_other bigint;v_sequence integer;v_change text;
BEGIN
 IF jsonb_typeof(p_snapshot)IS DISTINCT FROM 'object'OR p_snapshot->>'project_ref'IS DISTINCT FROM 'oxzabweahkoimtevbbny'
 OR p_snapshot->>'mount'IS DISTINCT FROM '/data'OR p_snapshot->>'data_directory'IS DISTINCT FROM '/data/pgdata'
 OR p_snapshot->>'evidence_sha256'IS NULL OR p_snapshot->>'evidence_sha256'!~'^[a-f0-9]{64}$'
 OR nullif(p_snapshot->>'evidence_path','')IS NULL THEN RAISE EXCEPTION 'compact_actual_filesystem_snapshot_required';END IF;
 v_total:=(p_snapshot->>'filesystem_bytes')::bigint;v_used:=(p_snapshot->>'used_bytes')::bigint;v_time:=(p_snapshot->>'observed_at')::timestamptz;
 IF v_total IS NULL OR v_used IS NULL OR v_used<0 OR v_total<=v_used OR v_time IS NULL OR v_time>clock_timestamp()OR v_time<clock_timestamp()-interval'15 minutes'THEN RAISE EXCEPTION 'compact_fresh_measured_headroom_required';END IF;
 v_headroom:=floor(v_total::numeric*0.90-v_used)::bigint;
 IF NOT market_governance.remediation_budgets_removed_v1() AND (p_cache_cap IS NULL OR p_cache_cap NOT BETWEEN 67108864 AND 60000000000
 OR p_source_reserve IS NULL OR p_source_reserve<0 OR p_native_reserve IS NULL OR p_native_reserve<0
 OR p_wal_growth_cap IS NULL OR p_wal_growth_cap NOT BETWEEN 536870912 AND 40000000000
 OR p_cache_cap+p_source_reserve+p_native_reserve+p_wal_growth_cap>v_headroom
 OR p_expiry IS NULL OR p_expiry<=clock_timestamp()OR p_expiry>clock_timestamp()+interval'48 hours') THEN RAISE EXCEPTION 'compact_finite_joint_allocation_and_expiry_required';END IF;
 PERFORM pg_advisory_xact_lock(hashtextextended('market_data_remediation_compact_shared_features_v1',0));
 SELECT *INTO v_run FROM market_governance.remediation_run_v1 WHERE run_id='market_data_remediation_20261008_v1'AND project_ref='oxzabweahkoimtevbbny'AND phase IN(3,4)FOR UPDATE;
 IF NOT FOUND OR v_run.protections#>>'{source_acquisition,claims_paused}'IS DISTINCT FROM 'true'
 OR EXISTS(SELECT 1 FROM market_governance.remediation_source_batch_v1 WHERE run_id=v_run.run_id AND status='RUNNING')THEN RAISE EXCEPTION 'compact_existing_source_lane_must_be_paused_and_drained';END IF;
 v_cfg:=v_run.protections->'shared_feature_runner';
 IF v_cfg->>'manifest_sha256'IS DISTINCT FROM 'db596062a61de56b36863ab2415772863e1764885b32356f4de89611703eccad'OR v_cfg->>'generation_id'IS DISTINCT FROM 'compact_full_20261008_v1'
 OR v_cfg->>'status'='COMPLETE_PENDING_RELEASE_VALIDATION'THEN RAISE EXCEPTION 'compact_exact_unfinished_manifest_required';END IF;
 v_source_cap:=coalesce((v_run.protections#>>'{source_acquisition,max_storage_bytes}')::bigint,0);
 IF NOT market_governance.remediation_budgets_removed_v1() AND (p_source_reserve<v_source_cap) THEN RAISE EXCEPTION 'compact_source_reservation_below_existing_source_cap';END IF;
 IF NOT market_governance.remediation_budgets_removed_v1() AND (pg_total_relation_size('astra_shared_equity_features_20260928_v1.feature_session_cache_remediated_v1')>=p_cache_cap) THEN RAISE EXCEPTION 'compact_cache_cap_already_reached';END IF;
 v_other:=p_source_reserve-v_source_cap+p_native_reserve+p_wal_growth_cap;
 SELECT wal_bytes,stats_reset INTO v_wal FROM pg_stat_wal;
 IF v_wal.wal_bytes IS NULL OR v_wal.stats_reset IS NULL THEN RAISE EXCEPTION 'compact_observed_wal_counter_required';END IF;
 v_old:=jsonb_build_object('budget',v_run.protections->'shared_feature_materialization','runner',v_cfg);
 v_sequence:=coalesce((v_cfg->>'allocation_sequence')::integer,0)+1;
 v_new:=jsonb_build_object('budget_version','compact_intraday_float64_v1_finite_full','max_storage_bytes',p_cache_cap,
 'max_wal_growth_bytes',p_wal_growth_cap,'wal_start_bytes',v_wal.wal_bytes,'wal_stats_reset',v_wal.stats_reset,
 'no_new_paid_service_costs',true,'verified_bytes_before_storage_autoscale',v_headroom,
 'other_reserved_storage_bytes',v_other,'source_full_relation_reserved_bytes',p_source_reserve,'native_reference_reserved_bytes',p_native_reserve,
 'global_wal_reserved_bytes',p_wal_growth_cap,'headroom_recorded_at',v_time,
 'existing_storage_headroom_evidence',p_snapshot->>'evidence_path','filesystem_snapshot',p_snapshot,'configured_at',clock_timestamp());
 v_cfg:=v_cfg||jsonb_build_object('enabled',true,'status','READY','expires_at',p_expiry,'allocation_sequence',v_sequence,'worker_id',NULL,'lease_expires_at',NULL,'heartbeat_at',clock_timestamp(),'last_error',NULL,'headroom_snapshot_sha256',p_snapshot->>'evidence_sha256');
 UPDATE market_governance.remediation_run_v1 SET protections=jsonb_set(jsonb_set(protections,'{shared_feature_materialization}',v_new,true),'{shared_feature_runner}',v_cfg,true),updated_at=clock_timestamp()WHERE run_id=v_run.run_id;
 v_change:='RT07:COMPACT_FEATURES:ALLOCATION_V1:'||lpad(v_sequence::text,3,'0');
 INSERT INTO market_governance.remediation_change_v1(change_id,run_id,finding_ids,relation_name,row_identity,before_row,after_row,before_sha256,after_sha256,operation,evidence,validation)
 VALUES(v_change,v_run.run_id,ARRAY['RT-07'],'market_governance.remediation_run_v1',jsonb_build_object('run_id',v_run.run_id,'generation_id','compact_full_20261008_v1','allocation_sequence',v_sequence),v_old,jsonb_build_object('budget',v_new,'runner',v_cfg),encode(sha256(convert_to(v_old::text,'UTF8')),'hex'),encode(sha256(convert_to(jsonb_build_object('budget',v_new,'runner',v_cfg)::text,'UTF8')),'hex'),'COMPACT_FEATURE_FRESH_CAPACITY_ALLOCATION_V1',p_snapshot,jsonb_build_object('WAL_baseline_rebased_only_after_fresh_actual_filesystem_snapshot',true,'completed_manifest_checkpoints_preserved',true,'source_lane_paused_and_drained',true,'new_paid_service_costs',0,'joint_reserved_bytes',p_cache_cap+p_source_reserve+p_native_reserve+p_wal_growth_cap,'headroom_before_existing_90percent_trigger',v_headroom));
 RETURN jsonb_build_object('budget',v_new,'runner',v_cfg);
END $function$;

CREATE OR REPLACE FUNCTION market_governance.remediation_enable_native_listing_interleave_v1()
 RETURNS jsonb
 LANGUAGE plpgsql
 SET search_path TO 'pg_catalog'
 SET "TimeZone" TO 'UTC'
AS $function$
DECLARE
 v_run market_governance.remediation_run_v1%ROWTYPE;v_shared jsonb;v_feature jsonb;v_cfg jsonb;v_old jsonb;v_budget jsonb;
 v_wal record;v_cap bigint;v_current bigint;v_sequence integer;v_expiry timestamptz;
BEGIN
 PERFORM pg_advisory_xact_lock(hashtextextended('market_data_remediation_native_listing_finite_runner_v1',0));
 SELECT * INTO v_run FROM market_governance.remediation_run_v1
 WHERE run_id='market_data_remediation_20261008_v1' AND project_ref='oxzabweahkoimtevbbny' AND phase IN(3,4) FOR NO KEY UPDATE;
 IF NOT FOUND OR v_run.protections#>>'{source_acquisition,claims_paused}' IS DISTINCT FROM 'true'
 OR EXISTS(SELECT 1 FROM market_governance.remediation_source_batch_v1 WHERE run_id=v_run.run_id AND status='RUNNING') THEN
  RAISE EXCEPTION 'native_source_FETCH_must_remain_paused_and_drained';
 END IF;
 v_shared:=v_run.protections->'shared_feature_materialization';v_feature:=v_run.protections->'shared_feature_runner';
 v_cap:=(v_shared->>'native_reference_reserved_bytes')::bigint;v_expiry:=(v_feature->>'expires_at')::timestamptz;
 IF NOT market_governance.remediation_budgets_removed_v1() AND (v_feature->>'generation_id' IS DISTINCT FROM 'compact_full_20261008_v1'
 OR v_feature->>'manifest_sha256' IS DISTINCT FROM 'db596062a61de56b36863ab2415772863e1764885b32356f4de89611703eccad'
 OR v_shared->>'budget_version' IS DISTINCT FROM 'compact_intraday_float64_v1_finite_full'
 OR v_shared->>'no_new_paid_service_costs' IS DISTINCT FROM 'true'
 OR nullif(v_shared->>'existing_storage_headroom_evidence','') IS NULL
 OR v_cap IS NULL OR v_cap NOT BETWEEN 67108864 AND 8589934592
 OR v_expiry IS NULL OR v_expiry<=clock_timestamp()
 OR (v_shared->>'max_storage_bytes')::bigint+(v_shared->>'source_full_relation_reserved_bytes')::bigint+v_cap+(v_shared->>'global_wal_reserved_bytes')::bigint>(v_shared->>'verified_bytes_before_storage_autoscale')::bigint) THEN
  RAISE EXCEPTION 'native_existing_verified_joint_reservation_required';
 END IF;
 SELECT wal_bytes,stats_reset INTO v_wal FROM pg_stat_wal;
 IF NOT market_governance.remediation_budgets_removed_v1() AND (v_wal.stats_reset IS DISTINCT FROM (v_shared->>'wal_stats_reset')::timestamptz
 OR v_wal.wal_bytes<(v_shared->>'wal_start_bytes')::numeric
 OR v_wal.wal_bytes-(v_shared->>'wal_start_bytes')::numeric>=(v_shared->>'max_wal_growth_bytes')::numeric) THEN
  RAISE EXCEPTION 'native_shared_global_WAL_review_required';
 END IF;
 v_current:=pg_total_relation_size('reference.massive_security_master_observation_v1');
 IF NOT market_governance.remediation_budgets_removed_v1() AND (v_current>=v_cap) THEN RAISE EXCEPTION 'native_reserved_relation_cap_reached'; END IF;
 IF (SELECT count(*) FROM market_governance.us_equity_fixed_session_calendar WHERE session_date>='2025-09-01' AND session_date<'2026-08-01')<>230
 OR (SELECT sum(provider_rows) FROM reference.equity_reference_backfill_day_v1 WHERE observation_date>='2025-09-01' AND observation_date<'2026-08-01')<>2830917 THEN
  RAISE EXCEPTION 'native_original_source_denominator_changed';
 END IF;
 v_cfg:=coalesce(v_run.protections->'native_listing_runner','{}'::jsonb);
 IF v_cfg ? 'generation_id' AND v_cfg->>'generation_id'<>'native_listing_full_20261008_v1' THEN RAISE EXCEPTION 'native_distinct_existing_generation'; END IF;
 IF v_cfg->>'enabled'='true' AND v_cfg->>'execution_mode'='INTERLEAVED_WITH_COMPACT'
 AND v_cfg->'joint_feature_allocation_sequence'=v_feature->'allocation_sequence'
 AND (v_cfg->>'expires_at')::timestamptz=v_expiry
 AND v_cfg->'wal_start_bytes'=v_shared->'wal_start_bytes'
 AND v_cfg->'wal_stats_reset'=v_shared->'wal_stats_reset'
 AND v_cfg->'max_wal_growth_bytes'=v_shared->'max_wal_growth_bytes'
 AND v_cfg->>'status' NOT IN('BLOCKED','EXPIRED') THEN RETURN v_cfg||jsonb_build_object('already_enabled',true); END IF;
 v_old:=jsonb_build_object('runner',v_cfg,'native_budget',v_run.protections->'native_listing_import');
 v_sequence:=coalesce((v_cfg->>'allocation_sequence')::integer,0)+1;
 v_budget:=jsonb_build_object('budget_version','native_listing_full_20261008_v1','max_relation_bytes',v_cap,
  'no_new_paid_service_costs',true,'existing_storage_headroom_evidence',v_shared->>'existing_storage_headroom_evidence',
  'filesystem_snapshot',v_shared->'filesystem_snapshot','joint_feature_allocation_sequence',v_feature->'allocation_sequence',
  'uses_previously_reserved_native_capacity',true,'configured_at',clock_timestamp());
 v_cfg:=v_cfg||jsonb_build_object('enabled',true,'status','READY','execution_mode','INTERLEAVED_WITH_COMPACT',
  'generation_id','native_listing_full_20261008_v1','version','native_listing_retained_runner_20261008_v1',
  'original_provider_rows',2830917,'original_unmatched_rows',208750,'target_session_dates',230,'maximum_source_pages',3680,
  'expires_at',v_expiry,'allocation_sequence',v_sequence,'joint_feature_allocation_sequence',v_feature->'allocation_sequence',
  'worker_id',NULL,'lease_expires_at',NULL,'heartbeat_at',clock_timestamp(),'pending_batch_key',NULL,'pending_source_sha256',NULL,
  'last_error',NULL,'attempts_at_page',0,'error_batch_key',NULL,
  'wal_start_bytes',v_shared->'wal_start_bytes','wal_stats_reset',v_shared->'wal_stats_reset','max_wal_growth_bytes',v_shared->'max_wal_growth_bytes',
  'source_acquisition_enabled',false,'historical_state_writes',false,'headroom_snapshot_sha256',v_feature->>'headroom_snapshot_sha256');
 UPDATE market_governance.remediation_run_v1 SET protections=jsonb_set(jsonb_set(protections,'{native_listing_runner}',v_cfg,true),'{native_listing_import}',v_budget,true),updated_at=clock_timestamp()
 WHERE run_id=v_run.run_id;
 INSERT INTO market_governance.remediation_change_v1(change_id,run_id,finding_ids,relation_name,row_identity,before_row,after_row,before_sha256,after_sha256,operation,evidence,validation)
 VALUES('RT01:NATIVE_LISTING_RUNNER:JOINT_ALLOCATION_V1:'||lpad(v_sequence::text,3,'0'),v_run.run_id,ARRAY['RT-01','IP-03','IP-04'],
 'market_governance.remediation_run_v1',jsonb_build_object('run_id',v_run.run_id,'generation_id','native_listing_full_20261008_v1','allocation_sequence',v_sequence),
 v_old,jsonb_build_object('runner',v_cfg,'native_budget',v_budget),encode(sha256(convert_to(v_old::text,'UTF8')),'hex'),
 encode(sha256(convert_to(jsonb_build_object('runner',v_cfg,'native_budget',v_budget)::text,'UTF8')),'hex'),
 'NATIVE_LISTING_EXISTING_JOINT_ALLOCATION_ACTIVATION',jsonb_build_object('existing_feature_allocation',v_shared,'feature_allocation_sequence',v_feature->'allocation_sequence'),
 jsonb_build_object('budget_increase',0,'expiry_extended',false,'WAL_baseline_reset',false,'source_FETCH_enabled',false,'feature_inputs_modified',false,'historical_state_modified',false));
 RETURN v_cfg||jsonb_build_object('relation_bytes_before',v_current,'native_total_relation_cap',v_cap);
END $function$;

CREATE OR REPLACE FUNCTION market_governance.remediation_import_native_listing_page_v1(p_batch_key text, p_expected_source_sha256 text, p_retained_raw_text text)
 RETURNS jsonb
 LANGUAGE plpgsql
 SET search_path TO 'pg_catalog'
 SET "TimeZone" TO 'UTC'
AS $function$
DECLARE v_batch market_governance.remediation_source_batch_v1%ROWTYPE;v_source market_governance.remediation_source_response_v1%ROWTYPE;
 v_date date;v_page integer;v_payload jsonb;v_rows integer;v_distinct integer;v_bad integer;v_first text;v_last text;
 v_route constant text:='market_data_remediation_20261008_v1.massive_native_listing_pages_v1';
 v_change_key text;v_before_count bigint;v_after_count bigint;v_inserted bigint;v_result jsonb;v_prior jsonb;v_current_manifest text;v_acq jsonb;v_budget jsonb;v_storage_limit bigint;v_existing_bytes bigint;
BEGIN
 IF p_expected_source_sha256 IS NULL OR p_expected_source_sha256!~'^[a-f0-9]{64}$'OR p_retained_raw_text IS NULL OR octet_length(p_retained_raw_text)>2097152 THEN RAISE EXCEPTION 'Bounded retained native source text and exact SHA256 required';END IF;
 IF NOT EXISTS(SELECT 1 FROM market_governance.remediation_run_v1 WHERE run_id='market_data_remediation_20261008_v1'AND project_ref='oxzabweahkoimtevbbny'AND phase IN(3,4))THEN RAISE EXCEPTION 'Authorized phase required';END IF;
 SELECT *INTO v_batch FROM market_governance.remediation_source_batch_v1 WHERE run_id='market_data_remediation_20261008_v1'AND batch_key=p_batch_key FOR SHARE;
 IF NOT FOUND OR v_batch.status IS DISTINCT FROM 'COMPLETE'OR v_batch.source_type IS DISTINCT FROM 'massive_reference_tickers'OR v_batch.provider IS DISTINCT FROM 'massive'OR v_batch.request_json->>'required_parser_version'IS DISTINCT FROM 'massive_native_reference_page_20261008_v1'THEN RAISE EXCEPTION 'Completed reviewed native reference page required';END IF;
 SELECT *INTO v_source FROM market_governance.remediation_source_response_v1 WHERE source_id=v_batch.current_source_id AND batch_id=v_batch.batch_id;
 IF NOT FOUND OR v_source.role IS DISTINCT FROM 'primary'OR v_source.http_status IS DISTINCT FROM 200 OR v_source.parser_version IS DISTINCT FROM 'massive_native_reference_page_20261008_v1'OR v_source.credential_redactions IS DISTINCT FROM 0 OR v_source.received_at IS NULL
 OR encode(v_source.source_sha256,'hex')IS DISTINCT FROM p_expected_source_sha256 OR v_source.source_sha256 IS DISTINCT FROM v_source.stored_body_sha256 OR sha256(convert_to(p_retained_raw_text,'UTF8'))IS DISTINCT FROM v_source.stored_body_sha256
 OR octet_length(p_retained_raw_text)IS DISTINCT FROM v_source.original_bytes OR sha256(v_source.compressed_body)IS DISTINCT FROM v_source.compressed_sha256 THEN RAISE EXCEPTION 'Retained source version, receipt or byte hash mismatch';END IF;
 v_date:=(v_batch.request_json->>'reference_date')::date;v_page:=(v_batch.request_json->>'page_number')::integer;
 IF v_date IS NULL OR v_date<'2025-09-01'OR v_date>='2026-08-01'OR v_page IS NULL OR v_page NOT BETWEEN 1 AND 16 OR v_batch.batch_key IS DISTINCT FROM 'massive:reference:stocks:active:'||v_date::text||':p'||lpad(v_page::text,2,'0')THEN RAISE EXCEPTION 'Native date/page scope mismatch';END IF;
 v_payload:=p_retained_raw_text::jsonb;
 IF jsonb_typeof(v_payload)IS DISTINCT FROM 'object'OR v_payload->>'status'IS DISTINCT FROM 'OK'OR jsonb_typeof(v_payload->'results')IS DISTINCT FROM 'array'OR jsonb_typeof(v_payload->'count')IS DISTINCT FROM 'number' THEN RAISE EXCEPTION 'Native payload shape invalid';END IF;
 v_rows:=jsonb_array_length(v_payload->'results');
 IF v_rows NOT BETWEEN 0 AND 1000 OR(v_payload->>'count')::integer IS DISTINCT FROM v_rows OR(v_batch.validation->>'source_native_reference_rows')::integer IS DISTINCT FROM v_rows
 OR v_batch.validation->>'source_symbol_case_preserved'IS DISTINCT FROM 'true'OR v_batch.validation->>'source_native_order_verified'IS DISTINCT FROM 'true'
 OR v_batch.validation->>'source_page_complete'IS DISTINCT FROM 'true'OR v_batch.validation->>'source_reference_date'IS DISTINCT FROM v_date::text
 OR(v_batch.validation->>'source_page_number')::integer IS DISTINCT FROM v_page OR nullif(v_payload->>'next_url','')IS DISTINCT FROM nullif(v_batch.validation->>'next_url','')THEN RAISE EXCEPTION 'Native page validation proof mismatch';END IF;
 WITH x AS(SELECT value,ordinality,value->>'ticker'ticker,lag(value->>'ticker')OVER(ORDER BY ordinality)prior FROM jsonb_array_elements(v_payload->'results')WITH ORDINALITY)
 SELECT count(DISTINCT ticker),count(*)FILTER(WHERE jsonb_typeof(value)<>'object'OR ticker IS NULL OR ticker!~'^[A-Za-z0-9][A-Za-z0-9._/-]{0,63}$'OR position('..'IN ticker)>0 OR value->>'market'IS DISTINCT FROM 'stocks'OR value->'active'IS DISTINCT FROM 'true'::jsonb OR(prior IS NOT NULL AND ticker COLLATE "C"<=prior COLLATE "C")),min(ticker COLLATE "C"),max(ticker COLLATE "C")INTO v_distinct,v_bad,v_first,v_last FROM x;
 IF v_distinct<>v_rows OR v_bad<>0 OR v_first IS DISTINCT FROM v_batch.validation->>'first_native_ticker'OR v_last IS DISTINCT FROM v_batch.validation->>'last_native_ticker'THEN RAISE EXCEPTION 'Native row identity/order contract invalid';END IF;
 v_change_key:='RT01:NATIVE_LISTING_IMPORT:'||v_date::text||':P'||lpad(v_page::text,2,'0')||':'||p_expected_source_sha256;
 PERFORM pg_advisory_xact_lock(hashtextextended('market_data_remediation_native_listing_import:'||v_date::text,0));
 SELECT count(*),encode(sha256(convert_to(coalesce(string_agg(row_hash,''ORDER BY ticker COLLATE "C"),''),'UTF8')),'hex')INTO v_before_count,v_current_manifest
 FROM reference.massive_security_master_observation_v1 WHERE source_route=v_route AND requested_as_of=v_date AND raw_payload#>>'{_remediation_acquisition,source_sha256}'=p_expected_source_sha256;
 SELECT after_row INTO v_prior FROM market_governance.remediation_change_v1 WHERE change_id=v_change_key AND run_id=v_batch.run_id;
 IF FOUND THEN
  IF v_before_count<>v_rows OR v_prior->>'source_sha256'IS DISTINCT FROM p_expected_source_sha256 OR v_prior->>'stored_row_manifest_sha256'IS DISTINCT FROM v_current_manifest THEN RAISE EXCEPTION 'Existing page import checkpoint differs from retained rows';END IF;
  RETURN v_prior||jsonb_build_object('already_complete',true);
 END IF;
 IF v_before_count<>0 OR EXISTS(SELECT 1 FROM reference.massive_security_master_observation_v1 o JOIN jsonb_array_elements(v_payload->'results')j ON o.ticker=j->>'ticker'WHERE o.source_route=v_route AND o.requested_as_of=v_date)THEN RAISE EXCEPTION 'Uncheckpointed/overlapping page rows require recovery review';END IF;
 SELECT protections->'native_listing_import'INTO v_budget FROM market_governance.remediation_run_v1 WHERE run_id=v_batch.run_id;
 v_storage_limit:=(v_budget->>'max_relation_bytes')::bigint;v_existing_bytes:=pg_total_relation_size('reference.massive_security_master_observation_v1');
 IF NOT market_governance.remediation_budgets_removed_v1() AND (v_budget->>'no_new_paid_service_costs'IS DISTINCT FROM 'true'OR nullif(v_budget->>'existing_storage_headroom_evidence','')IS NULL OR v_storage_limit IS NULL OR v_storage_limit<=0 OR v_existing_bytes+v_rows*4096>v_storage_limit) THEN RAISE EXCEPTION 'Finite reviewed native listing import storage budget required';END IF;
 IF market_governance.remediation_budgets_removed_v1() AND (market_governance.remediation_physical_capacity_gate_v2(67108864)->>'ready') IS DISTINCT FROM 'true' THEN RAISE EXCEPTION 'actual_physical_capacity_not_ready'; END IF;
 v_acq:=jsonb_build_object('run_id',v_batch.run_id,'batch_key',v_batch.batch_key,'source_id',v_source.source_id,'page_number',v_page,'source_sha256',p_expected_source_sha256,'compressed_sha256',encode(v_source.compressed_sha256,'hex'),'source_url',v_source.source_url,'received_at',v_source.received_at,'requested_as_of',v_date,'parser_version',v_source.parser_version,'source_payload_form','IMMUTABLE_NATIVE_HTTP_JSON_BYTES_AND_NATIVE_ROW_FIELDS','native_symbol_case_preserved',true,'historical_effective_date_semantics','PROVIDER_DATED_ACTIVE_LISTING_SNAPSHOT','historical_publication_recovered',false,'original_historical_first_receipt_recovered',false,'original_revision_history_complete',false);
 WITH native AS MATERIALIZED(
 SELECT j.value provider_fields,j.ordinality source_ordinal,j.value->>'ticker'ticker,
 nullif(j.value->>'composite_figi','')composite_figi,nullif(j.value->>'share_class_figi','')share_class_figi,nullif(j.value->>'cik','')cik
 FROM jsonb_array_elements(v_payload->'results')WITH ORDINALITY j(value,ordinality)
), overrides AS MATERIALIZED(
 SELECT instrument_key,native_symbol ticker,composite_figi,share_class_figi,cik,'REMEDIATED_EXACT_DATE_NATIVE_SOURCE'::text anchor_route
 FROM reference.equity_identity_day_overrides_remediation_v1 WHERE session_date=v_date
), reconciled AS MATERIALIZED(
 SELECT r.instrument_key,r.historical_source_symbol ticker,r.composite_figi,r.share_class_figi,nullif(r.raw_payload->>'cik','')cik,'CERTIFIED_EXACT_DATE_NATIVE_RECONCILIATION_V8'::text anchor_route
 FROM market_factors_20250901_20260731_v1.precursor_identity_day_reconciliation_v8 r
 WHERE r.session_date=v_date AND r.identity_certification_state='CERTIFIED_EXACT_DATE_COMPOSITE_AND_SHARE_FIGI_NATIVE_SOURCE_RECONCILED_V8'
 AND NOT EXISTS(SELECT 1 FROM overrides o WHERE o.instrument_key=r.instrument_key)
), states AS MATERIALIZED(
 SELECT s.*,count(*)OVER(PARTITION BY s.instrument_key)state_count
 FROM reference.equity_security_state_v1 s WHERE s.valid_from<=v_date AND s.valid_to>v_date
), anchors AS MATERIALIZED(
 SELECT *FROM overrides UNION ALL SELECT *FROM reconciled
 UNION ALL SELECT s.instrument_key,s.ticker,s.composite_figi,s.share_class_figi,s.cik,'UNIQUE_DATED_STRONG_STATE_FIGI'::text
 FROM states s WHERE s.state_count=1 AND s.mapping_strength='STRONG'
 AND NOT EXISTS(SELECT 1 FROM overrides o WHERE o.instrument_key=s.instrument_key)
 AND NOT EXISTS(SELECT 1 FROM reconciled r WHERE r.instrument_key=s.instrument_key)
), strong_candidates AS MATERIALIZED(
 SELECT x.ticker,count(DISTINCT a.instrument_key)::integer strong_count,min(a.instrument_key)candidate_key,
 array_agg(DISTINCT a.instrument_key)FILTER(WHERE a.instrument_key IS NOT NULL)candidate_keys,
 array_agg(DISTINCT a.anchor_route)FILTER(WHERE a.instrument_key IS NOT NULL)anchor_routes,
 coalesce(bool_or(a.ticker IS DISTINCT FROM x.ticker)FILTER(WHERE a.instrument_key IS NOT NULL),false)native_ticker_differs_from_retained_label
 FROM native x LEFT JOIN anchors a ON x.composite_figi IS NOT NULL AND x.composite_figi=a.composite_figi
 AND(x.share_class_figi IS NULL OR a.share_class_figi IS NULL OR x.share_class_figi=a.share_class_figi)
 GROUP BY x.ticker
), issuer_candidates AS MATERIALIZED(
 SELECT x.ticker,count(DISTINCT a.instrument_key)::integer issuer_count,
 array_agg(DISTINCT a.instrument_key)FILTER(WHERE a.instrument_key IS NOT NULL)issuer_candidate_keys
 FROM native x LEFT JOIN anchors a ON x.cik IS NOT NULL AND x.cik=a.cik GROUP BY x.ticker
), mapped AS(
 SELECT x.*,CASE WHEN s.strong_count=1 THEN s.candidate_key END instrument_key,s.strong_count,
 coalesce(s.candidate_keys,'{}'::integer[])candidate_keys,coalesce(s.anchor_routes,'{}'::text[])anchor_routes,
 s.native_ticker_differs_from_retained_label,i.issuer_count,coalesce(i.issuer_candidate_keys,'{}'::integer[])issuer_candidate_keys,
 CASE WHEN s.strong_count=1 THEN 'UNIQUE_DATED_COMPOSITE_FIGI_SHARE_COMPATIBLE'
 WHEN s.strong_count>1 THEN 'AMBIGUOUS_DATED_SECURITY_IDENTITY'
 WHEN i.issuer_count>0 THEN 'ISSUER_KNOWN_SECURITY_UNMATCHED'
 ELSE 'NO_EXISTING_DATED_SECURITY_ANCHOR'END mapping_state,
 (x.composite_figi~'^[A-Z0-9]{12}$')provider_composite_identifier_present
 FROM native x JOIN strong_candidates s USING(ticker)JOIN issuer_candidates i USING(ticker)
)

 ,prepared AS(
 SELECT m.*,m.provider_fields||jsonb_build_object('_remediation_acquisition',jsonb_build_object('source_id',v_source.source_id,'source_sha256',p_expected_source_sha256,'source_ordinal',m.source_ordinal,'page_number',v_page),'_remediation_mapping',jsonb_build_object('state',m.mapping_state,'strong_candidate_count',m.strong_count,'strong_candidate_instrument_keys',CASE WHEN m.strong_count>1 THEN m.candidate_keys ELSE '{}'::integer[]END,'anchor_routes',m.anchor_routes,'issuer_candidate_count',m.issuer_count,'issuer_candidate_instrument_keys',CASE WHEN m.strong_count=0 THEN m.issuer_candidate_keys ELSE '{}'::integer[]END,'native_ticker_differs_from_retained_label',m.native_ticker_differs_from_retained_label,'provider_composite_identifier_present',coalesce(m.provider_composite_identifier_present,false)))row_payload FROM mapped m
 ), inserted AS(
 INSERT INTO reference.massive_security_master_observation_v1(instrument_key,ticker,requested_as_of,active,security_type,name,market,locale,primary_exchange,currency_name,cik,composite_figi,share_class_figi,delisted_utc,provider_last_updated_utc,source_observed_at,research_available_at,source_endpoint,source_route,raw_payload,row_hash)
 SELECT instrument_key,ticker,v_date,true,provider_fields->>'type',provider_fields->>'name',provider_fields->>'market',provider_fields->>'locale',provider_fields->>'primary_exchange',provider_fields->>'currency_name',cik,composite_figi,share_class_figi,nullif(provider_fields->>'delisted_utc','')::timestamptz,nullif(provider_fields->>'last_updated_utc','')::timestamptz,v_source.received_at,statement_timestamp(),'/v3/reference/tickers',v_route,row_payload,encode(sha256(convert_to(row_payload::text,'UTF8')),'hex')
 FROM prepared ORDER BY source_ordinal RETURNING *
 )SELECT count(*),jsonb_build_object('batch_key',v_batch.batch_key,'source_date',v_date,'page_number',v_page,'source_sha256',p_expected_source_sha256,'source_route',v_route,'expected_page_rows',v_rows,'acquired_rows',count(*),'valid_native_rows',count(*),'available_at_window_cutoff',count(*)FILTER(WHERE research_available_at<'2026-08-01'),'mapped_unique_dated_figi',count(instrument_key),'issuer_known_security_unmatched',count(*)FILTER(WHERE raw_payload#>>'{_remediation_mapping,state}'='ISSUER_KNOWN_SECURITY_UNMATCHED'),'ambiguous_security_identity',count(*)FILTER(WHERE raw_payload#>>'{_remediation_mapping,state}'='AMBIGUOUS_DATED_SECURITY_IDENTITY'),'no_existing_security_anchor',count(*)FILTER(WHERE raw_payload#>>'{_remediation_mapping,state}'='NO_EXISTING_DATED_SECURITY_ANCHOR'),'provider_composite_identifier_present',count(*)FILTER(WHERE raw_payload#>>'{_remediation_mapping,provider_composite_identifier_present}'='true'),'retained_native_label_differences',count(*)FILTER(WHERE raw_payload#>>'{_remediation_mapping,native_ticker_differs_from_retained_label}'='true'),'excluded_rows',0,'quarantined_rows',0,'unresolved_existing_security_mapping',count(*)FILTER(WHERE instrument_key IS NULL),'stored_row_manifest_sha256',encode(sha256(convert_to(coalesce(string_agg(row_hash,''ORDER BY ticker COLLATE "C"),''),'UTF8')),'hex'),'source_received_at',v_source.received_at,'mapping_recorded_at',statement_timestamp(),'source_native_fields_preserved',true,'date_endpoint_complete_requires_all_pages',true)
 INTO v_inserted,v_result FROM inserted;
 IF v_inserted<>v_rows THEN RAISE EXCEPTION 'Native source page denominator not preserved';END IF;
 SELECT count(*)INTO v_after_count FROM reference.massive_security_master_observation_v1 WHERE source_route=v_route AND requested_as_of=v_date AND raw_payload#>>'{_remediation_acquisition,source_sha256}'=p_expected_source_sha256;
 IF v_after_count<>v_rows THEN RAISE EXCEPTION 'Actual stored page count mismatch';END IF;
 IF pg_total_relation_size('reference.massive_security_master_observation_v1')>v_storage_limit THEN RAISE EXCEPTION 'Actual native reference relation storage limit exceeded';END IF;
 INSERT INTO market_governance.remediation_change_v1(change_id,run_id,finding_ids,relation_name,row_identity,before_row,after_row,before_sha256,after_sha256,operation,evidence,validation)
 VALUES(v_change_key,v_batch.run_id,v_batch.finding_ids,'reference.massive_security_master_observation_v1',jsonb_build_object('source_route',v_route,'source_date',v_date,'page_number',v_page,'batch_key',v_batch.batch_key),jsonb_build_object('existing_page_rows',v_before_count,'original_source_rows_modified',0),v_result,encode(sha256(convert_to(jsonb_build_object('existing_page_rows',v_before_count,'original_source_rows_modified',0)::text,'UTF8')),'hex'),encode(sha256(convert_to(v_result::text,'UTF8')),'hex'),'NATIVE_HISTORICAL_LISTING_PAGE_IMPORT',v_acq,jsonb_build_object('original_http_bytes_hash_verified',true,'all_native_source_rows_retained',true,'existing_source_rows_modified',0,'original_2830917_provider_rows_preserved_as_baseline',true,'original_208750_unmatched_denominator_not_shrunk',true,'new_observations_do_not_modify_dated_state_intervals',true,'no_generated_database_ids_hardcoded',true));
 RETURN v_result||jsonb_build_object('already_complete',false);
END $function$;

CREATE OR REPLACE FUNCTION market_governance.remediation_physical_capacity_gate_v2(p_batch_reserve bigint DEFAULT 134217728)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'pg_catalog'
 SET "TimeZone" TO 'UTC'
AS $function$
DECLARE r market_governance.remediation_run_v1%rowtype;c jsonb;s jsonb;w record;cache_b bigint;source_b bigint;native_b bigint;db_b bigint;headroom numeric;required_b numeric;wal_gap numeric;
BEGIN
SELECT * INTO STRICT r FROM market_governance.remediation_run_v1 WHERE run_id='market_data_remediation_20261008_v1' AND project_ref='oxzabweahkoimtevbbny';
c:=r.protections->'resource_accounting_v2';s:=r.protections->'physical_capacity_observation_v2';
IF c->>'enabled' IS DISTINCT FROM 'true' THEN RETURN jsonb_build_object('ready',true,'reason','legacy_finite_capacity_gates_unchanged');END IF;
IF p_batch_reserve IS NULL OR p_batch_reserve<0 OR p_batch_reserve>1073741824 THEN RAISE EXCEPTION 'bounded_batch_reserve_required';END IF;
IF c->>'version' IS DISTINCT FROM 'physical_capacity_and_generated_wal_20261008_v2' OR s IS NULL OR s->>'verified_data_mount' IS DISTINCT FROM 'true' THEN RETURN jsonb_build_object('ready',false,'reason','verified_physical_observation_required');END IF;
IF (s->>'observed_at')::timestamptz<clock_timestamp()-interval '180 seconds' OR (s->>'observed_at')::timestamptz>clock_timestamp()+interval '5 seconds' THEN RETURN jsonb_build_object('ready',false,'reason','physical_observation_stale');END IF;
SELECT wal_bytes,stats_reset INTO w FROM pg_stat_wal;
IF w.stats_reset IS DISTINCT FROM (s->>'wal_stats_reset')::timestamptz OR w.stats_reset IS DISTINCT FROM (c->>'original_wal_stats_reset')::timestamptz OR w.wal_bytes<(s->>'wal_bytes')::numeric THEN RETURN jsonb_build_object('ready',false,'reason','original_wal_counter_changed');END IF;
wal_gap:=w.wal_bytes-(s->>'wal_bytes')::numeric;
IF wal_gap>2147483648 THEN RETURN jsonb_build_object('ready',false,'reason','physical_observation_write_gap_requires_refresh');END IF;
cache_b:=pg_total_relation_size('astra_shared_equity_features_20260928_v1.feature_session_cache_remediated_v1');
source_b:=pg_total_relation_size('market_governance.remediation_source_batch_v1')+pg_total_relation_size('market_governance.remediation_source_response_v1')+pg_total_relation_size('market_governance.remediation_observation_v1')+pg_total_relation_size('market_governance.remediation_option_bar_v1')+pg_total_relation_size('market_governance.remediation_equity_event_v1')+pg_total_relation_size('market_governance.remediation_coinbase_manifest_checkpoint_v1');
native_b:=pg_total_relation_size('reference.massive_security_master_observation_v1');
db_b:=pg_database_size(current_database());
IF NOT market_governance.remediation_budgets_removed_v1() AND (cache_b>(c->>'cache_relation_cap_bytes')::bigint OR source_b>(c->>'source_relation_cap_bytes')::bigint OR native_b>(c->>'native_relation_cap_bytes')::bigint) THEN RETURN jsonb_build_object('ready',false,'reason','finite_relation_allocation_reached');END IF;
IF market_governance.remediation_budgets_removed_v1() THEN
 headroom:=least((s->>'available_bytes')::numeric,(s->>'filesystem_bytes')::numeric-(s->>'used_bytes')::numeric)-greatest(db_b-(s->>'database_bytes')::bigint,0)-2*wal_gap-p_batch_reserve;
 required_b:=(c->>'operational_margin_bytes')::bigint+(c->>'transient_filesystem_reserve_bytes')::bigint;
 RETURN jsonb_build_object('ready',headroom>=required_b,'reason',CASE WHEN headroom>=required_b THEN 'actual_free_capacity_verified_budgets_removed' ELSE 'actual_free_capacity_insufficient' END,'budgets_removed',true,'free_bytes_after_pending_batch',headroom,'operational_reserve_bytes',required_b,'cache_bytes',cache_b,'source_bytes',source_b,'native_bytes',native_b,'observation_change_id',s->'change_id');
END IF;
headroom:=(s->>'headroom_before_90_percent_bytes')::numeric-greatest(db_b-(s->>'database_bytes')::bigint,0)-2*wal_gap-p_batch_reserve;
required_b:=greatest((c->>'cache_relation_cap_bytes')::bigint-cache_b,0)+greatest((c->>'source_relation_cap_bytes')::bigint-source_b,0)+greatest((c->>'native_relation_cap_bytes')::bigint-native_b,0)+(c->>'transient_filesystem_reserve_bytes')::bigint+(c->>'operational_margin_bytes')::bigint;
RETURN jsonb_build_object('ready',headroom>=required_b,'reason',CASE WHEN headroom>=required_b THEN 'measured_existing_capacity_verified' ELSE 'physical_headroom_insufficient_for_remaining_allocation' END,'measured_headroom_bytes',s->'headroom_before_90_percent_bytes','conservative_headroom_bytes',headroom,'required_remaining_reserved_bytes',required_b,'cache_bytes',cache_b,'source_bytes',source_b,'native_bytes',native_b,'generated_wal_since_measurement_bytes',wal_gap,'cumulative_wal_is_retained_disk',false,'observation_change_id',s->'change_id');
END $function$;

CREATE OR REPLACE FUNCTION market_governance.remediation_worker_gate_v1(p_run_id text)
 RETURNS jsonb
 LANGUAGE plpgsql
 SET search_path TO 'pg_catalog'
AS $function$
declare r market_governance.remediation_run_v1%rowtype; used_bytes bigint; limit_bytes bigint;
 wal_now bigint; wal_reset timestamptz; wal_start bigint; wal_limit bigint; wal_growth bigint;
 unbilled_headroom bigint; physical_gate jsonb;
begin
 if p_run_id is distinct from 'market_data_remediation_20261008_v1' then raise exception 'remediation_run_not_allowed'; end if;
 select * into strict r from market_governance.remediation_run_v1 where run_id=p_run_id;
 if r.project_ref<>'oxzabweahkoimtevbbny' then raise exception 'remediation_project_mismatch'; end if;
 if r.phase<>3 or r.status ~* '(cancel|stop|complete|closed)' then
  return jsonb_build_object('ready',false,'reason','phase_not_ready','phase',r.phase,'run_status',r.status);
 end if;
 limit_bytes=coalesce((r.protections->'source_acquisition'->>'max_storage_bytes')::bigint,0);
 used_bytes=pg_total_relation_size('market_governance.remediation_source_batch_v1')+
            pg_total_relation_size('market_governance.remediation_source_response_v1')+
            pg_total_relation_size('market_governance.remediation_observation_v1')+
            pg_total_relation_size('market_governance.remediation_option_bar_v1')+
            pg_total_relation_size('market_governance.remediation_equity_event_v1')+
            pg_total_relation_size('market_governance.remediation_coinbase_manifest_checkpoint_v1');
 if market_governance.remediation_budgets_removed_v1() then
  physical_gate=market_governance.remediation_physical_capacity_gate_v2(67108864);
  return physical_gate||jsonb_build_object('budgets_removed',true,'used_bytes',used_bytes,'max_bytes',NULL,'max_wal_growth_bytes',NULL,'phase',r.phase);
 end if;
 if limit_bytes<=0 or nullif(r.protections->'source_acquisition'->>'price_evidence','') is null then
  return jsonb_build_object('ready',false,'reason','priced_storage_budget_required','used_bytes',used_bytes);
 end if;
 select wal_bytes::bigint,stats_reset into wal_now,wal_reset from pg_stat_wal;
 wal_start=(r.protections->'source_acquisition'->>'wal_start_bytes')::bigint;
 wal_limit=(r.protections->'source_acquisition'->>'max_wal_growth_bytes')::bigint;
 if wal_start is null or wal_limit is null or wal_limit<=0 or wal_reset is distinct from
 nullif(r.protections->'source_acquisition'->>'wal_stats_reset','')::timestamptz or wal_now<wal_start then
  return jsonb_build_object('ready',false,'reason','wal_baseline_required_or_reset','used_bytes',used_bytes);
 end if;
 wal_growth=wal_now-wal_start;
 if wal_growth+33554432>=wal_limit then
  return jsonb_build_object('ready',false,'reason','global_wal_growth_ceiling_reached','used_bytes',used_bytes,'wal_growth_bytes',wal_growth,'max_wal_growth_bytes',wal_limit);
 end if;
 -- Public pricing is not authorization for extra charges. Bulk acquisition must
 -- fit measured space before the current allocated disk's autoscale threshold.
 unbilled_headroom=(r.protections->'source_acquisition'->>'verified_bytes_before_storage_autoscale')::bigint;
 if unbilled_headroom is null or unbilled_headroom<=0 or
 nullif(r.protections->'source_acquisition'->>'existing_storage_headroom_evidence','') is null then
  return jsonb_build_object('ready',false,'reason','verified_existing_unbilled_storage_headroom_required','used_bytes',used_bytes,'wal_growth_bytes',wal_growth);
 end if;
 if r.protections#>>'{resource_accounting_v2,enabled}'='true' then
  physical_gate=market_governance.remediation_physical_capacity_gate_v2(67108864);
  if physical_gate->>'ready' is distinct from 'true' then return physical_gate||jsonb_build_object('used_bytes',used_bytes,'max_bytes',limit_bytes,'wal_growth_bytes',wal_growth);end if;
 elsif used_bytes+wal_growth+33554432>=unbilled_headroom then
  return jsonb_build_object('ready',false,'reason','existing_unbilled_storage_headroom_ceiling_reached','used_bytes',used_bytes,'wal_growth_bytes',wal_growth,'verified_bytes_before_storage_autoscale',unbilled_headroom);
 end if;
 if used_bytes+16777216>=limit_bytes then
  return jsonb_build_object('ready',false,'reason','storage_budget_reached','used_bytes',used_bytes,'max_bytes',limit_bytes);
 end if;
 return jsonb_build_object('ready',true,'used_bytes',used_bytes,'max_bytes',limit_bytes,'wal_growth_bytes',wal_growth,'max_wal_growth_bytes',wal_limit,'verified_bytes_before_storage_autoscale',unbilled_headroom,'phase',r.phase);
end $function$;

CREATE OR REPLACE FUNCTION public.market_data_remediation_capacity_observation_v2(p_snapshot jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'pg_catalog'
 SET "TimeZone" TO 'UTC'
 SET statement_timeout TO '20s'
 SET lock_timeout TO '15s'
AS $function$
DECLARE r market_governance.remediation_run_v1%rowtype;c jsonb;s jsonb;w record;stamp timestamptz;sz bigint;used_b bigint;avail_b bigint;n integer;cid text;scope_until timestamptz;
BEGIN
IF p_snapshot IS NULL OR jsonb_typeof(p_snapshot)<>'object' OR octet_length(p_snapshot::text)>2048 OR EXISTS(SELECT 1 FROM jsonb_object_keys(p_snapshot)k WHERE k NOT IN('project_ref','mount','filesystem_bytes','used_bytes','available_bytes','observed_at','readonly','device_error','probe_version')) THEN RAISE EXCEPTION 'fixed_capacity_snapshot_shape_required';END IF;
IF p_snapshot->>'project_ref' IS DISTINCT FROM 'oxzabweahkoimtevbbny' OR p_snapshot->>'mount' IS DISTINCT FROM '/data' OR p_snapshot->>'probe_version' IS DISTINCT FROM 'read_only_filesystem_metrics_20261008_v1' OR p_snapshot->>'readonly' IS DISTINCT FROM '0' OR p_snapshot->>'device_error' IS DISTINCT FROM '0' THEN RAISE EXCEPTION 'fixed_complete_writable_data_mount_required';END IF;
IF EXISTS(SELECT 1 FROM jsonb_each(p_snapshot)e WHERE e.key IN('filesystem_bytes','used_bytes','available_bytes') AND(jsonb_typeof(e.value)<>'number' OR e.value::text !~ '^[0-9]{1,15}$')) THEN RAISE EXCEPTION 'finite_integer_capacity_counters_required';END IF;
stamp:=(p_snapshot->>'observed_at')::timestamptz;sz:=(p_snapshot->>'filesystem_bytes')::bigint;used_b:=(p_snapshot->>'used_bytes')::bigint;avail_b:=(p_snapshot->>'available_bytes')::bigint;
IF stamp IS NULL OR NOT isfinite(stamp) OR stamp<clock_timestamp()-interval '120 seconds' OR stamp>clock_timestamp()+interval '5 seconds' OR sz IS NULL OR sz<=0 OR (NOT market_governance.remediation_budgets_removed_v1() AND sz IS DISTINCT FROM 792631238656::bigint) OR used_b IS NULL OR avail_b IS NULL OR used_b<0 OR used_b>sz OR avail_b<0 OR avail_b>sz-used_b THEN RAISE EXCEPTION 'fresh_preexisting_filesystem_allocation_required';END IF;
SELECT * INTO STRICT r FROM market_governance.remediation_run_v1 WHERE run_id='market_data_remediation_20261008_v1' AND project_ref='oxzabweahkoimtevbbny' FOR NO KEY UPDATE;
IF r.phase NOT IN(3,4) OR r.status<>'IN_PROGRESS' THEN RAISE EXCEPTION 'active_remediation_required';END IF;
c:=r.protections->'resource_accounting_v2';
IF c->>'version' IS DISTINCT FROM 'physical_capacity_and_generated_wal_20261008_v2' THEN RAISE EXCEPTION 'capacity_contract_not_installed';END IF;
scope_until:=greatest((r.protections#>>'{shared_feature_runner,expires_at}')::timestamptz,CASE WHEN r.protections#>>'{source_acquisition,coinbase_cohort,enabled}'='true' THEN (r.protections#>>'{source_acquisition,coinbase_cohort,expires_at}')::timestamptz END);
IF NOT market_governance.remediation_budgets_removed_v1() AND (scope_until IS NULL OR scope_until<=clock_timestamp()) THEN RETURN jsonb_build_object('status','EXPIRED','scope_expires_at',scope_until);END IF;
cid:='OPS:CAPACITY_OBSERVATION_V2:'||encode(sha256(convert_to(p_snapshot::text,'UTF8')),'hex');
IF EXISTS(SELECT 1 FROM market_governance.remediation_change_v1 WHERE change_id=cid) THEN RETURN jsonb_build_object('status','ALREADY_RECORDED','change_id',cid,'scope_expires_at',scope_until);END IF;
n:=coalesce((c->>'observations_recorded')::integer,0)+1;IF NOT market_governance.remediation_budgets_removed_v1() AND n>14000 THEN RETURN jsonb_build_object('status','FINITE_OBSERVATION_LIMIT');END IF;
IF r.protections#>>'{physical_capacity_observation_v2,observed_at}' IS NOT NULL AND stamp<(r.protections#>>'{physical_capacity_observation_v2,observed_at}')::timestamptz THEN RAISE EXCEPTION 'capacity_observation_regression';END IF;
SELECT wal_bytes,stats_reset INTO w FROM pg_stat_wal;
IF w.stats_reset IS DISTINCT FROM (c->>'original_wal_stats_reset')::timestamptz THEN RAISE EXCEPTION 'original_WAL_statistics_reset_changed';END IF;
s:=p_snapshot||jsonb_build_object('change_id',cid,'verified_data_mount',true,'received_at',clock_timestamp(),'headroom_before_90_percent_bytes',least(avail_b,floor(sz::numeric*0.90-used_b)),'database_bytes',pg_database_size(current_database()),'wal_bytes',w.wal_bytes,'wal_stats_reset',w.stats_reset,'original_budget_baselines_unchanged',true,'snapshot_counter_is_measurement_anchor_not_new_budget_baseline',true);
INSERT INTO market_governance.remediation_change_v1(change_id,run_id,finding_ids,relation_name,row_identity,before_row,after_row,before_sha256,after_sha256,operation,evidence,validation)
VALUES(cid,r.run_id,ARRAY['RT-07','CD-F06'],'market_governance.remediation_run_v1',jsonb_build_object('capacity_observation',n),r.protections->'physical_capacity_observation_v2',s,CASE WHEN r.protections->'physical_capacity_observation_v2' IS NULL THEN NULL ELSE encode(sha256(convert_to((r.protections->'physical_capacity_observation_v2')::text,'UTF8')),'hex') END,encode(sha256(convert_to(s::text,'UTF8')),'hex'),'BOUNDED_PHYSICAL_CAPACITY_OBSERVATION',jsonb_build_object('endpoint','https://oxzabweahkoimtevbbny.supabase.co/customer/v1/privileged/metrics','selected_counters_only',true),jsonb_build_object('original_baselines_preserved',true,'expiry_extended',false,'new_paid_costs',0));
UPDATE market_governance.remediation_run_v1 SET protections=jsonb_set(jsonb_set(protections,'{physical_capacity_observation_v2}',s,true),'{resource_accounting_v2,observations_recorded}',to_jsonb(n),true),updated_at=clock_timestamp() WHERE run_id=r.run_id;
RETURN jsonb_build_object('status','RECORDED','change_id',cid,'observations_recorded',n,'max_observations',14000,'scope_expires_at',scope_until,'headroom_before_90_percent_bytes',s->'headroom_before_90_percent_bytes');
END $function$;

CREATE OR REPLACE FUNCTION public.market_data_remediation_claim_v1(p_request jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 SET search_path TO 'pg_catalog'
AS $function$
declare rid text=p_request->>'run_id'; wid text=p_request->>'worker_id'; g jsonb; q market_governance.remediation_source_batch_v1%rowtype;
 import_mode boolean=coalesce((p_request->>'import_only')::boolean,false); requested_key text=p_request->>'batch_key'; manifest_sha text=p_request->>'manifest_sha256';
 cohort_mode boolean=coalesce((p_request->>'coinbase_cohort')::boolean,false); control jsonb; generation_tasks bigint; generation_claims bigint; generation_valid boolean; slots integer; batches jsonb; resume_source jsonb; sealed_mode boolean=false; checkpoint market_governance.remediation_coinbase_manifest_checkpoint_v1%rowtype;
begin
 g=market_governance.remediation_worker_gate_v1(rid);
 if not (g->>'ready')::boolean then return g||jsonb_build_object('status','idle'); end if;
 if not import_mode and exists(select 1 from market_governance.remediation_run_v1 where run_id=rid and protections->'source_acquisition'->>'claims_paused'='true') then return g||jsonb_build_object('status','idle','reason','claims_paused_for_drain');end if;
 if wid is null or length(wid)>160 then raise exception 'worker_identity_required'; end if;

 -- Exact finite FOMC acquisition; original options/cohort/import branches stay below unchanged.
 if p_request ? 'fomc_probe_version' then
  if p_request->>'fomc_probe_version' is distinct from 'massive_fomc_two_windows_20261008_v1' or import_mode or cohort_mode or requested_key is not null or manifest_sha is not null or wid like 'saved-payload-import:%' then raise exception 'exact_native_fomc_mode_required';end if;
  select protections#>'{source_acquisition,fomc_two_windows}' into control from market_governance.remediation_run_v1 where run_id=rid;
  if control->>'enabled' is distinct from 'true' or control->>'version' is distinct from 'massive_fomc_two_windows_20261008_v1' or (NOT market_governance.remediation_budgets_removed_v1() AND ((control->>'expires_at')::timestamptz<=clock_timestamp() or (control->>'expires_at')::timestamptz>'2026-10-10T04:15:49.938363Z'::timestamptz)) then return g||jsonb_build_object('status','idle','reason','finite_fomc_probe_disabled_or_expired');end if;
  if exists(select 1 from market_governance.remediation_run_v1 where run_id=rid and (protections#>>'{shared_feature_runner,enabled}'='true' or protections#>>'{native_listing_runner,enabled}'='true' or protections#>>'{source_acquisition,coinbase_cohort,enabled}'='true'))then return g||jsonb_build_object('status','idle','reason','finite_fomc_conflicting_lane');end if;
  if p_request#>>'{source_capabilities,massive_fomc_futures}' is distinct from 'massive_fomc_two_windows_20261008_v1' then return g||jsonb_build_object('status','idle','reason','finite_fomc_capability_required');end if;
  perform pg_advisory_xact_lock(hashtextextended(rid||':source_claim',0));
  update market_governance.remediation_source_batch_v1 set status='FAILED',
   attempt_history=attempt_history||jsonb_build_array(jsonb_build_object('attempt',attempts,'status','FAILED',
    'source_id',current_source_id,'acquired_count',acquired_count,'valid_count',valid_count,'invalid_count',invalid_count,
    'duplicate_count',duplicate_count,'outside_count',outside_count,'validation',validation,
    'error',jsonb_build_object('code','finite_fomc_single_attempt_lease_expired'),'completed_at',clock_timestamp())),
   worker_id=null,last_error=jsonb_build_object('code','finite_fomc_single_attempt_lease_expired'),lease_token=null,lease_expires_at=null,updated_at=clock_timestamp()
   where run_id=rid and batch_key in('massive:fomc:ZQF6:2026-01-28:1845-1900','massive:fomc:ZQJ6:2026-04-29:1745-1800') and status='RUNNING' and attempts>=1 and lease_expires_at<clock_timestamp();
  if exists(select 1 from market_governance.remediation_source_batch_v1 where run_id=rid and status='RUNNING' and lease_expires_at>=clock_timestamp())then return g||jsonb_build_object('status','idle','reason','existing_source_lease');end if;
  select b.* into q from market_governance.remediation_source_batch_v1 b
   join (values
    ('massive:fomc:ZQF6:2026-01-28:1845-1900','ZQF6','2026-01-28T18:45:00Z'::timestamptz,'2026-01-28T19:00:00Z'::timestamptz),
    ('massive:fomc:ZQJ6:2026-04-29:1745-1800','ZQJ6','2026-04-29T17:45:00Z'::timestamptz,'2026-04-29T18:00:00Z'::timestamptz)
   )s(k,t,lo,hi) on b.batch_key=s.k and b.symbol=s.t and b.start_ts=s.lo and b.end_ts=s.hi
   where b.run_id=rid and b.provider='massive' and b.source_type='massive_candles' and b.interval_seconds=60
    and b.status='QUEUED' and b.attempts=0 and b.not_before<=clock_timestamp()
    and b.request_json->>'probe_id'='fm03_native_two_windows_20261008_v1'
    and b.request_json->>'required_parser_version'='massive_fomc_two_windows_20261008_v1'
    and b.request_json->>'required_futures_version'='massive_fomc_two_windows_20261008_v1'
    and b.request_json->>'provider_calls_cap'='1' and b.request_json->>'max_attempts'='1'
    and b.request_json->>'maximum_incremental_cost_usd'='0'
    and b.request_json->>'price_status'='INCLUDED_NO_INCREMENTAL_CHARGE'
    and b.request_json->>'price_evidence'='https://massive.com/docs/rest/futures/aggregates'
   order by b.batch_key for update of b skip locked limit 1;
  if not found then return g||jsonb_build_object('status','idle','reason','finite_fomc_no_ready_batches');end if;
  update market_governance.remediation_source_batch_v1 set status='RUNNING',attempts=1,worker_id=wid,
    lease_token=gen_random_uuid(),lease_expires_at=clock_timestamp()+interval '3 minutes',heartbeat_at=clock_timestamp(),
    current_source_id=null,acquired_count=0,valid_count=0,invalid_count=0,duplicate_count=0,outside_count=0,
    validation='{}'::jsonb,completed_at=null,updated_at=clock_timestamp(),last_error=null where batch_id=q.batch_id returning * into q;
  return g||jsonb_build_object('status','claimed','batch',to_jsonb(q));
 end if;
 if import_mode then
  if wid not like 'saved-payload-import:%' or requested_key is null or length(requested_key)>512 or manifest_sha !~ '^[0-9a-f]{64}$' or manifest_sha is null then
   raise exception 'exact_saved_payload_import_identity_required';
  end if;
 elsif requested_key is not null or manifest_sha is not null or wid like 'saved-payload-import:%' then
  raise exception 'saved_payload_import_mode_required';
 end if;
 perform pg_advisory_xact_lock(hashtextextended(rid||':source_claim',0));
 update market_governance.remediation_source_batch_v1 set status=case when attempts>=3 or (source_type in ('twelvedata_earnings_probe','massive_option_reference','massive_option_minutes') and attempts>=1) then 'FAILED' else 'RETRY' end,
 attempt_history=attempt_history||jsonb_build_array(jsonb_build_object('attempt',attempts,'status',case when attempts>=3 or (source_type in ('twelvedata_earnings_probe','massive_option_reference','massive_option_minutes') and attempts>=1) then 'FAILED' else 'RETRY' end,
 'source_id',current_source_id,'acquired_count',acquired_count,'valid_count',valid_count,'invalid_count',invalid_count,
 'duplicate_count',duplicate_count,'outside_count',outside_count,'validation',validation,'error',jsonb_build_object('code','expired_worker_lease'),'completed_at',clock_timestamp())),
 last_error=jsonb_build_object('code','expired_worker_lease'),worker_id=null,lease_token=null,lease_expires_at=null,updated_at=clock_timestamp()
 where run_id=rid and status='RUNNING' and lease_expires_at<clock_timestamp();
 if import_mode then
  select * into q from market_governance.remediation_source_batch_v1
  where run_id=rid and batch_key=requested_key and request_json->>'acquisition_mode'='IMPORT_SAVED_PAYLOAD'
    and request_json->>'import_manifest_sha256'=manifest_sha
    and ((status='RUNNING' and worker_id=wid and lease_expires_at>=clock_timestamp()) or status='COMPLETE');
  if found then
   return g||jsonb_build_object('status',case when q.status='COMPLETE' then 'complete' else 'claimed' end,'replayed',true,'batch',to_jsonb(q));
  end if;
 end if;
 if exists(select 1 from market_governance.remediation_source_batch_v1 where run_id=rid and status='RUNNING' and lease_expires_at>=clock_timestamp()) then
  return g||jsonb_build_object('status','idle','reason','existing_source_lease');
 end if;
 -- Exact finite Coinbase generation only. Normal/import claims retain the global
 -- single-lease rule and cannot take any task carrying a cohort generation key.
 if cohort_mode then
  select protections->'source_acquisition'->'coinbase_cohort' into control
  from market_governance.remediation_run_v1 where run_id=rid;
  sealed_mode=control->>'budget_version'='coinbase_sealed_manifest_budget_20261008_v2';
  if import_mode or wid not like 'remediation:%' or p_request->'source_capabilities'->>'coinbase_fetch_cohort' is distinct from 'coinbase_finite_fetch_cohort_20261008_v1'
  or control->>'enabled' is distinct from 'true'
  or control->>'generation_id' is distinct from p_request->>'cohort_generation_id'
  or control->>'manifest_sha256' is distinct from p_request->>'cohort_manifest_sha256'
  or coalesce((p_request->>'max_cohort_tasks')::integer,0) not between 1 and 8
  or coalesce(control->>'generation_id','') !~ '^[A-Za-z0-9_-]{1,80}$'
  or coalesce(control->>'manifest_sha256','') !~ '^[a-f0-9]{64}$'
  or (NOT market_governance.remediation_budgets_removed_v1() AND coalesce((control->>'expires_at')::timestamptz,'-infinity'::timestamptz)<=clock_timestamp())
  or (not coalesce(sealed_mode,false) and ((control->>'max_tasks')::integer not between 1 and 5000
   or (control->>'max_claims')::integer not between (control->>'max_tasks')::integer and 15000))
  or (coalesce(sealed_mode,false) and (control->>'generation_id'<>'coinbase_native_minute_full_remaining_20261008_v1'
   or control->>'manifest_sha256'<>'3ebe9402349311dd2be04951768830badb9a7abe2da4cc8fb6dceb709b65e651'
   or (control->>'max_tasks')::integer<>826993 or (control->>'max_claims')::integer<>2480979
   or p_request->'source_capabilities'->>'coinbase_manifest_budget' is distinct from 'coinbase_sealed_manifest_budget_20261008_v2'))
  or (control->>'max_concurrency')::integer not between 1 and 8
  or (control->>'rate_limit_rps')::numeric not between 0.01 and 2.5
  or ((control->>'max_tasks')::integer>24 and (control->>'pilot_verified' is distinct from 'true' or coalesce(control->>'pilot_evidence_sha256','') !~ '^[a-f0-9]{64}$'))
  then raise exception 'source_cohort_generation_not_authorized'; end if;
  if (control->>'max_tasks') is null or (control->>'max_claims') is null or (control->>'max_concurrency') is null or (control->>'rate_limit_rps') is null then raise exception 'source_cohort_generation_limits_required';end if;
  if coalesce(sealed_mode,false) then
   select * into checkpoint from market_governance.remediation_coinbase_manifest_checkpoint_v1
   where run_id=rid and generation_id=control->>'generation_id' for update;
   if not found or checkpoint.manifest_sha256<>control->>'manifest_sha256'
    or checkpoint.expected_tasks<>(control->>'max_tasks')::integer
    or checkpoint.max_claims<>(control->>'max_claims')::integer
    or (NOT market_governance.remediation_budgets_removed_v1() AND (checkpoint.expires_at<=clock_timestamp()
    or (control->>'expires_at')::timestamptz>checkpoint.expires_at)) then
    raise exception 'source_cohort_immutable_manifest_checkpoint_required';end if;
   generation_tasks=checkpoint.expected_tasks;generation_claims=checkpoint.claims_issued;generation_valid=true;
  else
  select count(*),coalesce(sum(attempts),0),coalesce(bool_and(coalesce(provider='coinbase' and source_type='coinbase_candles'
   and interval_seconds=60 and end_ts-start_ts<=interval '5 hours'
   and request_json->>'cohort_manifest_sha256'=control->>'manifest_sha256'
   and request_json->>'required_cohort_version'='coinbase_finite_fetch_cohort_20261008_v1'
   and request_json->>'required_parser_version'=p_request->'source_capabilities'->>source_type
   and coalesce(request_json->>'acquisition_mode','FETCH')='FETCH',false)),false)
  into generation_tasks,generation_claims,generation_valid
  from market_governance.remediation_source_batch_v1 where run_id=rid and request_json->>'cohort_generation_id'=control->>'generation_id';
  end if;
  if generation_tasks<>(control->>'max_tasks')::integer or not generation_valid then raise exception 'source_cohort_exact_manifest_population_required';end if;
  slots=least((control->>'max_concurrency')::integer,greatest(0,(p_request->>'max_cohort_tasks')::integer),
   CASE WHEN market_governance.remediation_budgets_removed_v1() THEN (control->>'max_concurrency')::integer ELSE greatest(0,(control->>'max_claims')::integer-generation_claims) END);
  if slots is null or slots not between 1 and 8 then return g||jsonb_build_object('status','idle','reason','cohort_finite_generation_claim_limit','generation_claims',generation_claims);end if;
  batches='[]'::jsonb;
  for q in select * from market_governance.remediation_source_batch_v1
   where run_id=rid and request_json->>'cohort_generation_id'=control->>'generation_id'
   and status in('QUEUED','RETRY') and attempts<3 and not_before<=clock_timestamp()
   and request_json->>'required_parser_version'=p_request->'source_capabilities'->>source_type
   order by priority desc,batch_id for update skip locked limit slots
  loop
   resume_source=null;
   if q.current_source_id is not null and (q.last_error->>'code' in('expired_worker_lease','worker_lease_or_stop','worker_stopping') or q.last_error->>'code' like 'rpc_%') then
    select jsonb_build_object('source_id',r.source_id,'role',r.role,'source_url',r.source_url,'http_status',r.http_status,
     'headers',r.response_headers,'received_at',r.received_at,'revision_at',r.revision_at,
     'source_sha256',encode(r.source_sha256,'hex'),'stored_body_sha256',encode(r.stored_body_sha256,'hex'),
     'compressed_sha256',encode(r.compressed_sha256,'hex'),'original_bytes',r.original_bytes,
     'compressed_base64',encode(r.compressed_body,'base64'),'parser_version',r.parser_version,
     'credential_redactions',r.credential_redactions,'provenance',r.provenance)
    into resume_source from market_governance.remediation_source_response_v1 r
    where r.source_id=q.current_source_id and r.batch_id=q.batch_id and r.role='primary' and r.http_status=200
     and r.parser_version=q.request_json->>'required_parser_version' and r.credential_redactions=0
     and r.storage_encoding='gzip' and r.original_bytes between 0 and 1048576 and octet_length(r.compressed_body)<=65536
     and coalesce(r.provenance->>'response_body_complete','true')='true';
   end if;
   update market_governance.remediation_source_batch_v1 set status='RUNNING',attempts=attempts+1,worker_id=wid,
    lease_token=gen_random_uuid(),lease_expires_at=clock_timestamp()+interval '3 minutes',heartbeat_at=clock_timestamp(),
    current_source_id=null,acquired_count=0,valid_count=0,invalid_count=0,duplicate_count=0,outside_count=0,
    validation='{}'::jsonb,completed_at=null,updated_at=clock_timestamp(),last_error=null
    where batch_id=q.batch_id returning * into q;
   batches=batches||jsonb_build_array(to_jsonb(q)||jsonb_build_object('resume_source',resume_source));
  end loop;
  return g||jsonb_build_object('status',case when jsonb_array_length(batches)>0 then 'cohort_claimed' else 'idle' end,
   'reason',case when jsonb_array_length(batches)=0 then 'cohort_no_ready_tasks' else null end,
   'batches',batches,'generation_id',control->>'generation_id','manifest_sha256',control->>'manifest_sha256',
   'generation_claims_before',generation_claims,'max_generation_claims',CASE WHEN market_governance.remediation_budgets_removed_v1() THEN NULL ELSE (control->>'max_claims')::integer END,'budgets_removed',market_governance.remediation_budgets_removed_v1(),'rate_limit_rps',(control->>'rate_limit_rps')::numeric);
 end if;

 -- Exact finite Binance daily archive generation. Earlier FOMC/import/Coinbase branches and the later ordinary options selector are unchanged.
 if not import_mode then
  select protections#>'{source_acquisition,binance_daily_finite}' into control
  from market_governance.remediation_run_v1 where run_id=rid;
  if control->>'enabled'='true' then
   if wid not like 'remediation:%'
    or control->>'version' is distinct from 'binance_um_daily_finite_manifest_20261008_v1'
    or control->>'generation_id' is distinct from 'binance_um_daily_remaining_17533_20261008_v1'
    or control->>'manifest_sha256' is distinct from '2825d3786055e65d9aa27e4947b836ce5a9b43cfcbb3ab35f20a23b874b5196c'
    or control->>'seed_complete' is distinct from 'true'
    or control->'max_distinct_tasks' is distinct from '17533'::jsonb
    or control->'max_claims' is distinct from '52599'::jsonb
    or control->'max_source_requests' is distinct from '105198'::jsonb
    or control->'max_attempts_per_task' is distinct from '3'::jsonb
    or control->'minimum_source_request_spacing_seconds' is distinct from '1'::jsonb
    or control->'source_price_usd' is distinct from '0'::jsonb
    or control->'maximum_incremental_cost_usd' is distinct from '0'::jsonb
    or (NOT market_governance.remediation_budgets_removed_v1() AND (control->'source_max_storage_bytes' is distinct from '65000000000'::jsonb
    or control->>'no_new_paid_service_costs' is distinct from 'true'))
    or coalesce(control->>'execution_stage','') not in('MEASUREMENT52','FULL')
    or control->'measurement_distinct_tasks' is distinct from '52'::jsonb
    then return g||jsonb_build_object('status','idle','reason','binance_finite_exact_manifest_control_required');end if;
   if NOT market_governance.remediation_budgets_removed_v1() AND (coalesce((control->>'expires_at')::timestamptz,'-infinity'::timestamptz)<=clock_timestamp()
    or (control->>'expires_at')::timestamptz>'2026-10-10T04:15:49.938363Z'::timestamptz)
    then return g||jsonb_build_object('status','idle','reason','binance_finite_original_expiry_required');end if;
   if exists(select 1 from market_governance.remediation_run_v1 where run_id=rid and
    (protections#>>'{shared_feature_runner,enabled}'='true' or protections#>>'{native_listing_runner,enabled}'='true'
     or protections#>>'{source_acquisition,coinbase_cohort,enabled}'='true'
     or protections#>>'{source_acquisition,fomc_two_windows,enabled}'='true'))then
    return g||jsonb_build_object('status','idle','reason','binance_finite_conflicting_execution_lane');end if;
   if p_request#>>'{source_capabilities,binance_book_depth}' is distinct from 'binance_depth_profile_remediation_v2'
    or p_request#>>'{source_capabilities,binance_metrics}' is distinct from 'binance_archive_source_rules_20261008_v1'
    then return g||jsonb_build_object('status','idle','reason','binance_finite_parser_capabilities_required');end if;
   if NOT market_governance.remediation_budgets_removed_v1() AND ((g->>'max_bytes')::bigint<>65000000000 or (g->>'used_bytes')::bigint+67108864>=(g->>'max_bytes')::bigint) then
    return g||jsonb_build_object('status','idle','reason','binance_finite_source_allocation_required');end if;
   select b.* into q from market_governance.remediation_source_batch_v1 b
    where b.run_id=rid and b.request_json->>'source_generation_id'='binance_um_daily_remaining_17533_20261008_v1'
     and b.status in('QUEUED','RETRY') and b.attempts<3 and b.not_before<=clock_timestamp()
     and (control->>'execution_stage'='FULL' or (b.start_ts='2025-09-01T00:00:00Z'::timestamptz and b.attempts=0))
    order by b.priority desc,b.batch_id for update of b skip locked limit 1;
   if not found then return g||jsonb_build_object('status','idle','reason','binance_finite_no_ready_files','generation_id','binance_um_daily_remaining_17533_20261008_v1');end if;
   if market_governance.remediation_binance_daily_identity_v1(q) is distinct from true then raise exception 'binance_finite_claimed_identity_mismatch';end if;
   update market_governance.remediation_source_batch_v1 set status='RUNNING',attempts=attempts+1,worker_id=wid,
    lease_token=gen_random_uuid(),lease_expires_at=clock_timestamp()+interval '3 minutes',heartbeat_at=clock_timestamp(),
    current_source_id=null,acquired_count=0,valid_count=0,invalid_count=0,duplicate_count=0,outside_count=0,
    validation='{}'::jsonb,completed_at=null,updated_at=clock_timestamp(),last_error=null
    where batch_id=q.batch_id returning * into q;
   return g||jsonb_build_object('status','claimed','batch',to_jsonb(q),'generation_id','binance_um_daily_remaining_17533_20261008_v1','manifest_sha256','2825d3786055e65d9aa27e4947b836ce5a9b43cfcbb3ab35f20a23b874b5196c');
  end if;
 end if;
 select b.* into q
 from unnest(case when import_mode then ARRAY[requested_key] else ARRAY['massive:options:pilot-v1:1d009a6da92d33f39a6a8723b2621461514da0e0deb83b45d717c28683376e7c:minutes:recovery:429-v1','massive:options:pilot-v1:6820e2108a87f03e78e2860c89be43acbcaa87638d2c75c2ca58f881f59ba5cf:minutes:recovery:429-v1','massive:options:pilot-v1:cdcd01811ce03ccbf260a87192b1a6b3be3cd6516b3f910cbd084dce74b1e910:minutes:recovery:429-v1'] end) as allowed(eligible_batch_key)
 join market_governance.remediation_source_batch_v1 b
  on b.run_id=rid and b.batch_key=allowed.eligible_batch_key
 where status in ('QUEUED','RETRY') and attempts<3 and not_before<=clock_timestamp()
 -- Finite ordinary fetch window: exact options pilot only. Earlier Coinbase branch and saved-payload import remain intact.
 and (import_mode or (
      source_type='massive_option_minutes'
      and request_json->'recovery'->>'recovery_id'='massive_options_429_recovery_20261008_v1'
      and p_request->'source_capabilities'->>'massive_option_minutes_429_recovery'='massive_options_429_recovery_20261008_v1'
      and request_json->>'pilot_id'='intended_cohort_options_native_pilot_20261008_v1'
      and not exists (
       select 1 from unnest(ARRAY['massive:options:pilot-v1:1d009a6da92d33f39a6a8723b2621461514da0e0deb83b45d717c28683376e7c:minutes:recovery:429-v1','massive:options:pilot-v1:6820e2108a87f03e78e2860c89be43acbcaa87638d2c75c2ca58f881f59ba5cf:minutes:recovery:429-v1','massive:options:pilot-v1:cdcd01811ce03ccbf260a87192b1a6b3be3cd6516b3f910cbd084dce74b1e910:minutes:recovery:429-v1']) recent_key(batch_key)
       join market_governance.remediation_source_batch_v1 recent
        on recent.run_id=rid and recent.batch_key=recent_key.batch_key
       where recent.attempts>0 and recent.updated_at>clock_timestamp()-interval '20 seconds')
      and request_json->>'required_parser_version'='massive_native_options_pilot_20261008_v1'
      and market_governance.remediation_options_candidate_v1(request_json->>'candidate_id') is not null
      and attempts=0 and p_request->'source_capabilities'->>source_type='massive_native_options_pilot_20261008_v1'))
 and (source_type not in ('massive_option_reference','massive_option_minutes') or
      (attempts=0 and p_request->'source_capabilities'->>source_type='massive_native_options_pilot_20261008_v1'))
 and (source_type<>'massive_option_minutes' or market_governance.remediation_options_minute_proof_v1(to_jsonb(b)))
 and (source_type<>'twelvedata_earnings_probe' or
      (attempts=0 and p_request->'source_capabilities'->>'twelvedata_earnings_probe'='twelvedata_earnings_included_quota_probe_20261008_v1'))
 and case when import_mode then
  batch_key=requested_key and request_json->>'acquisition_mode'='IMPORT_SAVED_PAYLOAD' and request_json->>'import_manifest_sha256'=manifest_sha
 else coalesce(request_json->>'acquisition_mode','FETCH')<>'IMPORT_SAVED_PAYLOAD'
  and request_json->>'cohort_generation_id' is null
  and (
    (provider in ('coinbase','binance_archive','twelvedata','massive') and source_type<>'massive_reference_tickers'
     and (p_request->'source_capabilities' is null
          or coalesce(request_json->>'required_parser_version','')=''
          or p_request->'source_capabilities'->>source_type=request_json->>'required_parser_version'))
    or (provider='alpaca' and source_type in ('alpaca_option_bars','alpaca_equity_quotes_probe','alpaca_equity_trades_probe')
        and request_json->>'required_parser_version'='alpaca_historical_source_contract_20261008_v1'
        and p_request->'source_capabilities'->>source_type=request_json->>'required_parser_version')
    or (provider='massive' and source_type='massive_reference_tickers'
        and request_json->>'required_parser_version'='massive_native_reference_page_20261008_v1'
        and p_request->'source_capabilities'->>source_type=request_json->>'required_parser_version')
    or (provider='massive' and source_type='massive_reference_tickers'
        and request_json->>'required_parser_version'='massive_native_warmup_reference_page_20261008_v1'
        and request_json->>'warmup_contract'='RTH_DAILY_LOOKBACK61_PREWINDOW_2025-06-04_2025-08-29_V1'
        and p_request->'source_capabilities'->>'massive_warmup_reference'=request_json->>'required_parser_version')
    or (provider='alpaca' and source_type in ('alpaca_option_daily_panel','alpaca_equity_quotes','alpaca_equity_trades')
        and request_json->>'required_parser_version'='alpaca_multi_instrument_source_contract_20261008_v2'
        and p_request->'source_capabilities'->>source_type=request_json->>'required_parser_version'
        and (request_json->>'required_continuation_version' IS NULL
             or (request_json->>'required_continuation_version'='alpaca_native_tick_cursor_chain_20261008_v1'
                 and p_request->'source_capabilities'->>'alpaca_tick_continuation'=request_json->>'required_continuation_version')))
    or (provider='alpaca' and source_type='alpaca_assets_snapshot'
        and request_json->>'required_parser_version'='alpaca_current_asset_metadata_20261008_v1'
        and p_request->'source_capabilities'->>source_type=request_json->>'required_parser_version')
  ) end
 order by priority desc,batch_id for update of b skip locked limit 1;
 if not found then return g||jsonb_build_object('status','idle','reason','no_ready_batches'); end if;
 update market_governance.remediation_source_batch_v1 set status='RUNNING',attempts=attempts+1,worker_id=wid,
 lease_token=gen_random_uuid(),lease_expires_at=clock_timestamp()+interval '3 minutes',heartbeat_at=clock_timestamp(),
 current_source_id=null,acquired_count=0,valid_count=0,invalid_count=0,duplicate_count=0,outside_count=0,
 validation='{}'::jsonb,completed_at=null,updated_at=clock_timestamp(),last_error=null where batch_id=q.batch_id returning * into q;
 return g||jsonb_build_object('status','claimed','batch',to_jsonb(q));
end $function$;

CREATE OR REPLACE FUNCTION public.market_data_remediation_compact_feature_step_v1(p_request jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'pg_catalog'
 SET "TimeZone" TO 'UTC'
 SET statement_timeout TO '30s'
 SET lock_timeout TO '5s'
AS $function$
DECLARE v_run market_governance.remediation_run_v1%ROWTYPE;v_cfg jsonb;v_item jsonb;v_prior jsonb;v_metrics jsonb;v_build jsonb;
 v_worker text;v_action text;v_ordinal integer;v_expected integer;v_attempt integer;v_before jsonb;v_response jsonb;
 v_base jsonb;v_begin timestamptz;v_error_state text;v_error_message text;v_error_code text;v_signature record;
 v_manifest constant text:='db596062a61de56b36863ab2415772863e1764885b32356f4de89611703eccad';v_generation constant text:='compact_full_20261008_v1';
 v_capacity jsonb;v_checkpoint_id text;v_error_id text;v_cache_bytes bigint;v_full_count bigint;v_full_sources numeric;v_full_grid numeric;
BEGIN
 IF p_request IS NULL OR jsonb_typeof(p_request)<>'object'OR octet_length(p_request::text)>4096
 OR EXISTS(SELECT 1 FROM jsonb_object_keys(p_request)k WHERE k NOT IN('run_id','worker_id','version','manifest_sha256','generation_id','action','expected_ordinal'))
 OR p_request->>'run_id'IS DISTINCT FROM 'market_data_remediation_20261008_v1'
 OR p_request->>'version'IS DISTINCT FROM 'compact_feature_finite_runner_20261008_v1'
 OR p_request->>'manifest_sha256'IS DISTINCT FROM v_manifest OR p_request->>'generation_id'IS DISTINCT FROM v_generation
 THEN RAISE EXCEPTION 'compact_request_scope_invalid';END IF;
 v_worker:=p_request->>'worker_id';v_action:=p_request->>'action';
 IF v_worker IS NULL OR v_worker!~'^compact-[a-f0-9-]{36}$'OR v_action IS NULL OR v_action NOT IN('status','step')THEN RAISE EXCEPTION 'compact_worker_action_invalid';END IF;
 IF v_action='step'AND(jsonb_typeof(p_request->'expected_ordinal')IS DISTINCT FROM 'number'OR p_request->>'expected_ordinal'!~'^[0-9]{1,5}$')THEN RAISE EXCEPTION 'compact_exact_ordinal_required';END IF;
 v_base:=jsonb_build_object('manifest_sha256',v_manifest,'generation_id',v_generation);
 IF NOT pg_try_advisory_xact_lock(hashtextextended('market_data_remediation_compact_shared_features_v1',0))THEN RETURN v_base||jsonb_build_object('status','BUSY');END IF;
 SELECT *INTO v_run FROM market_governance.remediation_run_v1 WHERE run_id='market_data_remediation_20261008_v1'AND project_ref='oxzabweahkoimtevbbny'FOR NO KEY UPDATE;
 IF NOT FOUND OR v_run.phase NOT IN(3,4)THEN RAISE EXCEPTION 'compact_remediation_phase_required';END IF;
 IF v_run.status~*'(cancel|stop|complete|closed)' OR v_run.protections#>>'{user_stop,active}'='true' THEN RETURN v_base||jsonb_build_object('status','PAUSED','reason','run_stopped');END IF;
 v_cfg:=v_run.protections->'shared_feature_runner';
 IF v_cfg->>'manifest_sha256'IS DISTINCT FROM v_manifest OR v_cfg->>'generation_id'IS DISTINCT FROM v_generation THEN RAISE EXCEPTION 'compact_manifest_not_installed';END IF;
 v_base:=v_base||jsonb_build_object('expires_at',v_cfg->'expires_at');
 IF v_cfg->>'status'='COMPLETE_PENDING_RELEASE_VALIDATION'THEN RETURN v_base||v_cfg||jsonb_build_object('status','COMPLETE_PENDING_RELEASE_VALIDATION');END IF;
 IF v_cfg->>'enabled'IS DISTINCT FROM 'true'OR v_run.protections#>>'{source_acquisition,claims_paused}'IS DISTINCT FROM 'true'
 OR EXISTS(SELECT 1 FROM market_governance.remediation_source_batch_v1 WHERE run_id=v_run.run_id AND status='RUNNING')THEN RETURN v_base||jsonb_build_object('status','PAUSED','reason','exclusive_materialization_lane_required');END IF;
 IF NOT market_governance.remediation_budgets_removed_v1() AND ((v_cfg->>'expires_at')::timestamptz IS NULL OR(v_cfg->>'expires_at')::timestamptz<=clock_timestamp())THEN RETURN v_base||jsonb_build_object('status','EXPIRED');END IF;
 IF v_cfg->>'status'='BLOCKED'THEN RETURN v_base||jsonb_build_object('status','BLOCKED','reason',v_cfg->'last_error');END IF;
 IF v_cfg->>'worker_id'IS NOT NULL AND v_cfg->>'worker_id'<>v_worker AND(v_cfg->>'lease_expires_at')::timestamptz>clock_timestamp()THEN RETURN v_base||jsonb_build_object('status','LEASE_HELD');END IF;
 v_ordinal:=(v_cfg->>'next_ordinal')::integer;
 IF v_ordinal IS NULL OR v_ordinal NOT BETWEEN 1 AND 11290 THEN RAISE EXCEPTION 'compact_checkpoint_cursor_invalid';END IF;
 IF v_action='status'THEN
  v_cfg:=v_cfg||jsonb_build_object('worker_id',v_worker,'heartbeat_at',clock_timestamp(),'lease_expires_at',clock_timestamp()+interval'90 seconds');
  UPDATE market_governance.remediation_run_v1 SET protections=jsonb_set(protections,'{shared_feature_runner}',v_cfg,true),updated_at=clock_timestamp()WHERE run_id=v_run.run_id;
  RETURN v_base||jsonb_build_object('status','READY','next_ordinal',v_ordinal);
 END IF;
 v_expected:=(p_request->>'expected_ordinal')::integer;
 IF v_expected<v_ordinal THEN
  SELECT after_row INTO v_prior FROM market_governance.remediation_change_v1 WHERE run_id=v_run.run_id AND change_id='RT07:COMPACT_FEATURES:CHECKPOINT_V1:B'||lpad(v_expected::text,5,'0');
  IF NOT FOUND OR v_prior->>'manifest_sha256'IS DISTINCT FROM v_manifest THEN RAISE EXCEPTION 'compact_replay_checkpoint_missing';END IF;
  RETURN v_base||v_prior||jsonb_build_object('status','ALREADY_COMPLETE','completed_ordinal',v_expected,'next_ordinal',v_expected+1);
 END IF;
 IF v_expected<>v_ordinal THEN RAISE EXCEPTION 'compact_out_of_order_step';END IF;
 IF v_ordinal=11290 THEN
  SELECT count(*),sum((after_row->>'sessions')::bigint),sum((after_row->>'retained_minutes')::bigint)
  INTO v_full_count,v_full_grid,v_full_sources FROM market_governance.remediation_change_v1
  WHERE run_id=v_run.run_id AND operation='COMPACT_FEATURE_FINITE_CHECKPOINT_V1'AND after_row->>'manifest_sha256'=v_manifest;
  IF v_full_count<>11289 OR v_full_grid<>3053710 OR v_full_sources<>371008247 THEN RAISE EXCEPTION 'compact_full_checkpoint_reconciliation_failed';END IF;
  SELECT count(*),sum(retained_source_rows),sum(expected_grid_rows)INTO v_full_count,v_full_sources,v_full_grid FROM astra_shared_equity_features_20260928_v1.feature_session_cache_remediated_v1;
  IF v_full_count<>3053710 OR v_full_sources<>371008247 OR v_full_grid<>1186167180 THEN RAISE EXCEPTION 'compact_actual_full_grid_reconciliation_failed';END IF;
  v_cfg:=v_cfg||jsonb_build_object('status','COMPLETE_PENDING_RELEASE_VALIDATION','enabled',false,'completed_at',clock_timestamp(),'heartbeat_at',clock_timestamp(),'sessions',v_full_count,'retained_minutes',v_full_sources,'scheduled_minutes',v_full_grid,'source_lane_automatically_resumed',false);
  UPDATE market_governance.remediation_run_v1 SET protections=jsonb_set(protections,'{shared_feature_runner}',v_cfg,true),updated_at=clock_timestamp()WHERE run_id=v_run.run_id;
  RETURN v_base||v_cfg;
 END IF;
 v_capacity:=market_governance.remediation_physical_capacity_gate_v2(134217728);
 IF v_capacity->>'ready'IS DISTINCT FROM 'true'THEN RETURN v_base||jsonb_build_object('status','PAUSED','reason',v_capacity);END IF;
 FOR v_signature IN SELECT value FROM jsonb_array_elements((SELECT after_row->'function_hashes'FROM market_governance.remediation_change_v1 WHERE change_id='RT07:COMPACT_FEATURES:MANIFEST_V1'))LOOP
  IF encode(sha256(convert_to(pg_get_functiondef((v_signature.value->>'signature')::regprocedure),'UTF8')),'hex')IS DISTINCT FROM coalesce((SELECT after_row->'function_hash_overrides'->>(v_signature.value->>'signature') FROM market_governance.remediation_change_v1 WHERE change_id='OPS:BUDGET_REMOVAL:20261008_130925'),v_signature.value->>'sha256')THEN RAISE EXCEPTION 'compact_frozen_function_changed';END IF;
 END LOOP;
 SELECT after_row INTO v_item FROM market_governance.remediation_change_v1 WHERE change_id='RT07:COMPACT_FEATURES:MANIFEST_V1:B'||lpad(v_ordinal::text,5,'0')AND run_id=v_run.run_id;
 IF NOT FOUND OR v_item->>'manifest_sha256'IS DISTINCT FROM v_manifest OR(v_item->>'ordinal')::integer<>v_ordinal THEN RAISE EXCEPTION 'compact_manifest_batch_missing';END IF;
 v_checkpoint_id:='RT07:COMPACT_FEATURES:CHECKPOINT_V1:B'||lpad(v_ordinal::text,5,'0');
 IF EXISTS(SELECT 1 FROM market_governance.remediation_change_v1 WHERE change_id=v_checkpoint_id)THEN RAISE EXCEPTION 'compact_cursor_checkpoint_conflict';END IF;
 v_before:=v_cfg;v_begin:=clock_timestamp();v_attempt:=coalesce((v_cfg->>'attempts_at_next')::integer,0)+1;
 BEGIN
  v_build:=astra_shared_equity_features_20260928_v1.rebuild_feature_sessions_remediated_v1((v_item->>'lo_key')::integer,(v_item->>'hi_key')::integer,(v_item->>'start_date')::date,(v_item->>'end_date')::date);
  v_metrics:=market_governance.remediation_compact_chunk_metrics_v1((v_item->>'lo_key')::integer,(v_item->>'hi_key')::integer,(v_item->>'start_date')::date,(v_item->>'end_date')::date);
  IF(v_metrics->>'sessions')::bigint<>(v_item->>'expected_sessions')::bigint OR(v_metrics->>'retained_minutes')::bigint<>(v_item->>'expected_retained_minutes')::bigint OR(v_metrics->>'scheduled_minutes')::bigint<>(v_item->>'expected_grid_minutes')::bigint
  OR(v_metrics->>'identity_failures')::bigint<>0 OR(v_metrics->>'output_failures')::bigint<>0 OR(v_metrics->>'nonfinite_coefficients')::bigint<>0 THEN RAISE EXCEPTION 'compact_batch_reconciliation_failed';END IF;
  v_cache_bytes:=pg_total_relation_size('astra_shared_equity_features_20260928_v1.feature_session_cache_remediated_v1');
  v_response:=v_metrics||jsonb_build_object('manifest_sha256',v_manifest,'generation_id',v_generation,'completed_ordinal',v_ordinal,'next_ordinal',v_ordinal+1,'status','BATCH_COMPLETE','cache_total_bytes',v_cache_bytes,'elapsed_seconds',extract(epoch FROM clock_timestamp()-v_begin),'completed_at',clock_timestamp(),'builder_result',v_build,'expected_batch',v_item,'attempt',v_attempt);
  INSERT INTO market_governance.remediation_change_v1(change_id,run_id,finding_ids,relation_name,row_identity,before_row,after_row,before_sha256,after_sha256,operation,evidence,validation)
  VALUES(v_checkpoint_id,v_run.run_id,ARRAY['RT-07'],'astra_shared_equity_features_20260928_v1.feature_session_cache_remediated_v1',v_item,jsonb_build_object('existing_sessions_preserved',v_build->'existing_sessions_preserved'),v_response,NULL,encode(sha256(convert_to(v_response::text,'UTF8')),'hex'),'COMPACT_FEATURE_FINITE_CHECKPOINT_V1',jsonb_build_object('manifest_sha256',v_manifest,'worker_version','compact_feature_finite_runner_20261008_v1','no_source_acquisition',true,'coefficient_order','return1,5,10,20,60,120;vol5,20,60;range20;volume_mean20;volume_sd20;dollar_volume_mean20;SMA5,20,60;session_vwap'),jsonb_build_object('all_batch_rows_checked',true,'source_denominator_matches_retained_daily_counts',true,'identity_and_output_hashes_verified',true,'full_observed_coefficient_populations_measured',true,'calendar_grid_not_provider_eligible_completeness',true));
  v_cfg:=v_cfg||jsonb_build_object('next_ordinal',v_ordinal+1,'completed_batches',v_ordinal,'sessions_completed',coalesce((v_cfg->>'sessions_completed')::bigint,0)+(v_metrics->>'sessions')::bigint,'observed_minutes_completed',coalesce((v_cfg->>'observed_minutes_completed')::bigint,0)+(v_metrics->>'retained_minutes')::bigint,'scheduled_minutes_completed',coalesce((v_cfg->>'scheduled_minutes_completed')::bigint,0)+(v_metrics->>'scheduled_minutes')::bigint,'worker_id',v_worker,'status','RUNNING','heartbeat_at',clock_timestamp(),'lease_expires_at',clock_timestamp()+interval'90 seconds','last_completed_ordinal',v_ordinal,'cache_total_bytes',v_cache_bytes,'attempts_at_next',0,'last_error',NULL);
 EXCEPTION WHEN query_canceled OR OTHERS THEN
  GET STACKED DIAGNOSTICS v_error_state=RETURNED_SQLSTATE,v_error_message=MESSAGE_TEXT;
  v_error_code:=CASE WHEN v_error_state IN('57014','40001','40P01','55P03')THEN 'TRANSIENT_BOUNDED_SQL_FAILURE'
   WHEN v_error_message LIKE 'Finite storage%'OR v_error_message LIKE 'Actual cache storage%'THEN 'STORAGE_OR_GLOBAL_WAL_GUARD'
   WHEN v_error_message LIKE 'compact_batch_reconciliation%'THEN 'BATCH_RECONCILIATION_FAILED'ELSE 'SQL_IMPLEMENTATION_OR_DEPENDENCY_FAILURE'END;
  v_cfg:=v_before||jsonb_build_object('status',CASE WHEN v_error_code='TRANSIENT_BOUNDED_SQL_FAILURE'AND v_attempt<3 THEN 'RETRYABLE'ELSE 'BLOCKED'END,'attempts_at_next',v_attempt,'worker_id',v_worker,'heartbeat_at',clock_timestamp(),'lease_expires_at',clock_timestamp()+interval'90 seconds','last_error',jsonb_build_object('code',v_error_code,'sqlstate',v_error_state,'message_sha256',encode(sha256(convert_to(v_error_message,'UTF8')),'hex'),'ordinal',v_ordinal,'partial_batch_rolled_back',true));
  v_response:=v_base||jsonb_build_object('status',v_cfg->>'status','next_ordinal',v_ordinal,'reason',v_cfg->'last_error');
  v_error_id:='RT07:COMPACT_FEATURES:ERROR_V1:B'||lpad(v_ordinal::text,5,'0')||':A'||v_attempt::text;
  INSERT INTO market_governance.remediation_change_v1(change_id,run_id,finding_ids,relation_name,row_identity,before_row,after_row,operation,evidence,validation)
  VALUES(v_error_id,v_run.run_id,ARRAY['RT-07'],'astra_shared_equity_features_20260928_v1.feature_session_cache_remediated_v1',v_item,v_before,v_cfg,'COMPACT_FEATURE_FINITE_ERROR_V1',jsonb_build_object('manifest_sha256',v_manifest,'no_automatic_budget_reset',true),jsonb_build_object('current_batch_rolled_back',true,'completed_checkpoints_preserved',true));
 END;
 UPDATE market_governance.remediation_run_v1 SET protections=jsonb_set(protections,'{shared_feature_runner}',v_cfg,true),updated_at=clock_timestamp()WHERE run_id=v_run.run_id;
 RETURN v_base||v_response;
END $function$;

CREATE OR REPLACE FUNCTION public.market_data_remediation_native_listing_step_v1(p_request jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'pg_catalog'
 SET "TimeZone" TO 'UTC'
 SET statement_timeout TO '30s'
 SET lock_timeout TO '5s'
AS $function$
DECLARE
 v_run market_governance.remediation_run_v1%ROWTYPE;
 v_batch market_governance.remediation_source_batch_v1%ROWTYPE;
 v_source market_governance.remediation_source_response_v1%ROWTYPE;
 v_cfg jsonb; v_action text; v_worker text; v_result jsonb; v_response jsonb; v_base jsonb;
 v_sha text; v_change text; v_pages bigint; v_rows bigint; v_dates bigint; v_all_dates bigint; v_source_pages bigint; v_source_rows bigint;
 v_capacity jsonb; v_wal record; v_attempt integer; v_error text; v_state text;
BEGIN
 IF p_request IS NULL OR jsonb_typeof(p_request)<>'object' OR octet_length(p_request::text)>4194304
 OR EXISTS(SELECT 1 FROM jsonb_object_keys(p_request) k WHERE k NOT IN('run_id','version','generation_id','worker_id','action','batch_key','source_sha256','retained_raw_text'))
 OR p_request->>'run_id' IS DISTINCT FROM 'market_data_remediation_20261008_v1'
 OR p_request->>'version' IS DISTINCT FROM 'native_listing_retained_runner_20261008_v1'
 OR p_request->>'generation_id' IS DISTINCT FROM 'native_listing_full_20261008_v1'
 OR coalesce(p_request->>'worker_id','')!~'^native-[a-f0-9-]{36}$'
 OR coalesce(p_request->>'action','') NOT IN('status','next','commit') THEN
  RAISE EXCEPTION 'native_fixed_scope_request_required';
 END IF;
 v_action:=p_request->>'action'; v_worker:=p_request->>'worker_id';
 IF v_action<>'commit' AND (p_request ? 'batch_key' OR p_request ? 'source_sha256' OR p_request ? 'retained_raw_text') THEN
  RAISE EXCEPTION 'native_unexpected_action_payload';
 END IF;
 v_base:=jsonb_build_object('version','native_listing_retained_runner_20261008_v1','generation_id','native_listing_full_20261008_v1');
 IF NOT pg_try_advisory_xact_lock(hashtextextended('market_data_remediation_native_listing_finite_runner_v1',0)) THEN
  RETURN v_base||jsonb_build_object('status','BUSY');
 END IF;
 IF v_action='status' THEN
  SELECT * INTO v_run FROM market_governance.remediation_run_v1
  WHERE run_id='market_data_remediation_20261008_v1' AND project_ref='oxzabweahkoimtevbbny';
 ELSE
  SELECT * INTO v_run FROM market_governance.remediation_run_v1
  WHERE run_id='market_data_remediation_20261008_v1' AND project_ref='oxzabweahkoimtevbbny' FOR NO KEY UPDATE;
 END IF;
 IF NOT FOUND OR v_run.phase NOT IN(3,4) THEN RAISE EXCEPTION 'native_authorized_phase_required'; END IF;
 IF v_run.status~*'(cancel|stop|complete|closed)' OR v_run.protections#>>'{user_stop,active}'='true' THEN RETURN v_base||jsonb_build_object('status','PAUSED','reason','run_stopped');END IF;
 v_cfg:=v_run.protections->'native_listing_runner';
 IF v_cfg->>'generation_id' IS DISTINCT FROM 'native_listing_full_20261008_v1' OR v_cfg->>'enabled' IS DISTINCT FROM 'true' THEN
  RETURN v_base||jsonb_build_object('status','PAUSED');
 END IF;
 IF v_action='status' THEN RETURN v_base||v_cfg; END IF;
 IF NOT market_governance.remediation_budgets_removed_v1() AND (v_cfg->>'expires_at')::timestamptz<=clock_timestamp() THEN RETURN v_base||jsonb_build_object('status','EXPIRED'); END IF;
 IF v_cfg->>'status' IN('BLOCKED','COMPLETE_PENDING_RELEASE_VALIDATION') THEN RETURN v_base||v_cfg; END IF;
 IF v_run.protections#>>'{source_acquisition,claims_paused}' IS DISTINCT FROM 'true'
 OR EXISTS(SELECT 1 FROM market_governance.remediation_source_batch_v1 WHERE run_id=v_run.run_id AND status='RUNNING') THEN
  RETURN v_base||jsonb_build_object('status','PAUSED','reason','source_fetch_must_remain_paused_and_drained');
 END IF;
 IF v_cfg->>'execution_mode' IS DISTINCT FROM 'INTERLEAVED_WITH_COMPACT'
 OR v_run.protections#>>'{shared_feature_runner,generation_id}' IS DISTINCT FROM 'compact_full_20261008_v1'
 OR v_run.protections#>>'{shared_feature_runner,manifest_sha256}' IS DISTINCT FROM 'db596062a61de56b36863ab2415772863e1764885b32356f4de89611703eccad'
 OR (v_cfg->>'expires_at')::timestamptz IS DISTINCT FROM (v_run.protections#>>'{shared_feature_runner,expires_at}')::timestamptz
 OR v_cfg->'wal_start_bytes' IS DISTINCT FROM v_run.protections#>'{shared_feature_materialization,wal_start_bytes}'
 OR v_cfg->'wal_stats_reset' IS DISTINCT FROM v_run.protections#>'{shared_feature_materialization,wal_stats_reset}'
 OR v_cfg->'max_wal_growth_bytes' IS DISTINCT FROM v_run.protections#>'{shared_feature_materialization,max_wal_growth_bytes}'
 OR (v_run.protections#>>'{native_listing_import,max_relation_bytes}')::bigint > (v_run.protections#>>'{shared_feature_materialization,native_reference_reserved_bytes}')::bigint THEN
  RAISE EXCEPTION 'native_original_shared_allocation_or_expiry_changed';
 END IF;
 IF encode(sha256(convert_to(pg_get_functiondef('market_governance.remediation_import_native_listing_page_v1(text,text,text)'::regprocedure),'UTF8')),'hex') IS DISTINCT FROM coalesce((SELECT after_row->'function_hash_overrides'->>'market_governance.remediation_import_native_listing_page_v1(text,text,text)' FROM market_governance.remediation_change_v1 WHERE change_id='OPS:BUDGET_REMOVAL:20261008_130925'),'b33b1dcd166622069dbfe93c998e7f2b8cc0dd3d5246f655e14fe591b294ed29') THEN
  RAISE EXCEPTION 'native_verified_private_import_predecessor_changed';
 END IF;
 v_capacity:=market_governance.remediation_physical_capacity_gate_v2(67108864);
 IF v_capacity->>'ready' IS DISTINCT FROM 'true' THEN RETURN v_base||jsonb_build_object('status',CASE WHEN v_capacity->>'reason' IN('physical_observation_stale','verified_physical_observation_required','physical_observation_write_gap_requires_refresh') THEN 'PAUSED' ELSE 'CAPACITY_REVIEW_REQUIRED' END,'reason',v_capacity);END IF;
 SELECT wal_bytes,stats_reset INTO v_wal FROM pg_stat_wal;
 IF v_wal.stats_reset IS DISTINCT FROM (v_cfg->>'wal_stats_reset')::timestamptz
 OR v_wal.wal_bytes<(v_cfg->>'wal_start_bytes')::numeric
 OR (NOT market_governance.remediation_budgets_removed_v1() AND v_wal.wal_bytes-(v_cfg->>'wal_start_bytes')::numeric>(v_cfg->>'max_wal_growth_bytes')::numeric) THEN
  RETURN v_base||jsonb_build_object('status','CAPACITY_REVIEW_REQUIRED','reason','global_wal_counter_gate','no_automatic_rebase',true);
 END IF;
 IF v_cfg->>'worker_id' IS DISTINCT FROM v_worker AND (v_cfg->>'lease_expires_at')::timestamptz>clock_timestamp() THEN
  RETURN v_base||jsonb_build_object('status','LEASE_HELD');
 END IF;
 IF v_action='next' THEN
  SELECT q.* INTO v_batch FROM market_governance.remediation_source_batch_v1 q
  JOIN market_governance.remediation_source_response_v1 r ON r.source_id=q.current_source_id AND r.batch_id=q.batch_id
  WHERE q.run_id=v_run.run_id AND q.provider='massive' AND q.source_type='massive_reference_tickers' AND q.status='COMPLETE'
  AND (q.request_json->>'reference_date')::date>='2025-09-01' AND (q.request_json->>'reference_date')::date<'2026-08-01'
  AND NOT EXISTS(SELECT 1 FROM market_governance.remediation_change_v1 c
    WHERE c.change_id='RT01:NATIVE_LISTING_IMPORT:'||(q.request_json->>'reference_date')||':P'||lpad(q.request_json->>'page_number',2,'0')||':'||encode(r.source_sha256,'hex'))
  ORDER BY q.request_json->>'reference_date',(q.request_json->>'page_number')::integer LIMIT 1;
  IF NOT FOUND THEN
   SELECT count(*),coalesce(sum((after_row->>'acquired_rows')::bigint),0),count(DISTINCT after_row->>'source_date')
   INTO v_pages,v_rows,v_dates FROM market_governance.remediation_change_v1
   WHERE run_id=v_run.run_id AND operation='NATIVE_HISTORICAL_LISTING_PAGE_IMPORT';
   SELECT count(*) FILTER(WHERE dated_endpoint_capture_complete),coalesce(sum(page_tasks),0),coalesce(sum(native_rows_acquired),0)
   INTO v_all_dates,v_source_pages,v_source_rows FROM market_governance.remediation_reference_date_coverage_v1 WHERE run_id=v_run.run_id;
   IF v_pages>3680 OR v_dates>230 OR v_pages>v_source_pages OR v_rows>v_source_rows THEN RAISE EXCEPTION 'native_finite_page_or_row_reconciliation_failed'; END IF;
   v_result:=jsonb_build_object('status',CASE WHEN v_all_dates=230 AND v_dates=230 AND v_pages=v_source_pages AND v_rows=v_source_rows THEN 'COMPLETE_PENDING_RELEASE_VALIDATION' ELSE 'WAITING_SOURCE' END,
     'imported_pages',v_pages,'imported_rows',v_rows,'imported_dates',v_dates,'complete_source_dates',v_all_dates,'source_page_tasks',v_source_pages,'source_native_rows',v_source_rows,
     'original_provider_rows',2830917,'original_unmatched_rows',208750,'new_provider_count_difference',v_rows-2830917,
     'source_revisions_require_reconciliation',v_rows<>2830917,'excluded_native_rows',0,'heartbeat_at',clock_timestamp(),
     'worker_id',NULL,'lease_expires_at',NULL,'pending_batch_key',NULL,'pending_source_sha256',NULL);
   v_cfg:=v_cfg||v_result;
   UPDATE market_governance.remediation_run_v1 SET protections=jsonb_set(protections,'{native_listing_runner}',v_cfg,true),updated_at=clock_timestamp() WHERE run_id=v_run.run_id;
   RETURN v_base||v_cfg;
  END IF;
  SELECT * INTO v_source FROM market_governance.remediation_source_response_v1 WHERE source_id=v_batch.current_source_id AND batch_id=v_batch.batch_id;
  IF v_source.role<>'primary' OR v_source.http_status<>200 OR v_source.parser_version<>'massive_native_reference_page_20261008_v1'
  OR v_source.storage_encoding IS DISTINCT FROM 'gzip' OR v_source.credential_redactions<>0
  OR v_source.original_bytes NOT BETWEEN 1 AND 2097152 OR octet_length(v_source.compressed_body)>2097152
  OR v_source.source_sha256<>v_source.stored_body_sha256 OR sha256(v_source.compressed_body)<>v_source.compressed_sha256 THEN
   RAISE EXCEPTION 'native_retained_artifact_invalid';
  END IF;
  v_cfg:=v_cfg||jsonb_build_object('status','RUNNING','worker_id',v_worker,'lease_expires_at',clock_timestamp()+interval'90 seconds',
    'heartbeat_at',clock_timestamp(),'pending_batch_key',v_batch.batch_key,'pending_source_sha256',encode(v_source.source_sha256,'hex'));
  UPDATE market_governance.remediation_run_v1 SET protections=jsonb_set(protections,'{native_listing_runner}',v_cfg,true),updated_at=clock_timestamp() WHERE run_id=v_run.run_id;
  RETURN v_base||jsonb_build_object('status','PAGE_READY','batch_key',v_batch.batch_key,'source_sha256',encode(v_source.source_sha256,'hex'),
    'compressed_sha256',encode(v_source.compressed_sha256,'hex'),'original_bytes',v_source.original_bytes,'compressed_bytes',octet_length(v_source.compressed_body),
    'compressed_body_base64',encode(v_source.compressed_body,'base64'),'expires_at',v_cfg->>'expires_at');
 END IF;
 IF coalesce(p_request->>'source_sha256','')!~'^[a-f0-9]{64}$' OR p_request->>'retained_raw_text' IS NULL
 OR octet_length(p_request->>'retained_raw_text')>2097152 THEN RAISE EXCEPTION 'native_bounded_exact_source_commit_required'; END IF;
 SELECT * INTO v_batch FROM market_governance.remediation_source_batch_v1
 WHERE run_id=v_run.run_id AND batch_key=p_request->>'batch_key' AND provider='massive' AND source_type='massive_reference_tickers' AND status='COMPLETE';
 IF NOT FOUND THEN RAISE EXCEPTION 'native_completed_page_missing'; END IF;
 v_sha:=p_request->>'source_sha256';
 v_change:='RT01:NATIVE_LISTING_IMPORT:'||(v_batch.request_json->>'reference_date')||':P'||lpad(v_batch.request_json->>'page_number',2,'0')||':'||v_sha;
 IF NOT EXISTS(SELECT 1 FROM market_governance.remediation_change_v1 WHERE change_id=v_change)
 AND (v_cfg->>'pending_batch_key' IS DISTINCT FROM v_batch.batch_key OR v_cfg->>'pending_source_sha256' IS DISTINCT FROM v_sha) THEN
  RAISE EXCEPTION 'native_commit_not_current_lease';
 END IF;
 v_attempt:=CASE WHEN v_cfg->>'error_batch_key'=v_batch.batch_key THEN coalesce((v_cfg->>'attempts_at_page')::integer,0)+1 ELSE 1 END;
 BEGIN
  v_result:=market_governance.remediation_import_native_listing_page_v1(v_batch.batch_key,v_sha,p_request->>'retained_raw_text');
  v_cfg:=v_cfg||jsonb_build_object('status','READY','heartbeat_at',clock_timestamp(),'worker_id',v_worker,
    'lease_expires_at',clock_timestamp()+interval'90 seconds','last_completed_batch_key',v_batch.batch_key,'last_completed_source_sha256',v_sha,
    'pending_batch_key',NULL,'pending_source_sha256',NULL,'attempts_at_page',0,'last_error',NULL,'error_batch_key',NULL);
  v_response:=v_base||jsonb_build_object('status','PAGE_COMPLETE','result',v_result,'relation_bytes',pg_total_relation_size('reference.massive_security_master_observation_v1'));
 EXCEPTION WHEN query_canceled OR OTHERS THEN
  GET STACKED DIAGNOSTICS v_state=RETURNED_SQLSTATE,v_error=MESSAGE_TEXT;
  v_cfg:=v_cfg||jsonb_build_object('status',CASE WHEN v_state IN('57014','40001','40P01','55P03') AND v_attempt<3 THEN 'RETRYABLE' ELSE 'BLOCKED' END,
   'error_batch_key',v_batch.batch_key,'attempts_at_page',v_attempt,'last_error',jsonb_build_object('sqlstate',v_state,'message_sha256',encode(sha256(convert_to(v_error,'UTF8')),'hex')),
   'heartbeat_at',clock_timestamp(),'worker_id',NULL,'lease_expires_at',NULL);
  v_response:=v_base||jsonb_build_object('status',v_cfg->>'status','attempt',v_attempt,'sqlstate',v_state,'page_rolled_back',true);
 END;
 UPDATE market_governance.remediation_run_v1 SET protections=jsonb_set(protections,'{native_listing_runner}',v_cfg,true),updated_at=clock_timestamp() WHERE run_id=v_run.run_id;
 RETURN v_response;
END $function$;
DO $record$
DECLARE r market_governance.remediation_run_v1%rowtype;p jsonb;policy jsonb;path text[];k text;hashes jsonb;after_record jsonb;
BEGIN
 SELECT * INTO STRICT r FROM market_governance.remediation_run_v1 WHERE run_id='market_data_remediation_20261008_v1' AND project_ref='oxzabweahkoimtevbbny' FOR NO KEY UPDATE;
 IF r.status<>'IN_PROGRESS' OR r.protections#>>'{user_stop,active}'='true' OR r.protections#>>'{continuation_coordinator,session_id}'<>'c5e2e9ec1d3f' THEN RAISE EXCEPTION 'active_owned_remediation_required';END IF;
 IF EXISTS(SELECT 1 FROM market_governance.remediation_change_v1 WHERE change_id='OPS:BUDGET_REMOVAL:20261008_130925') THEN RAISE EXCEPTION 'budget_change_already_recorded';END IF;
 policy:=jsonb_build_object('mode','UNLIMITED_USER_AUTHORIZED','authorization_change_id','OPS:BUDGET_REMOVAL:20261008_130925','explicit_user_instruction','Remove budgets applies to everything','requested_at','2026-10-08T13:09:25Z','applied_at',clock_timestamp(),'scope','All work and necessary paid services within the market-data remediation mission','financial_limit',NULL,'work_request_limit',NULL,'runtime_expiry',NULL,'storage_budget',NULL,'cumulative_wal_budget',NULL,'supersedes_all_legacy_budget_values',true,'legacy_source_manifest_fields_are_historical_contract_evidence',true,'necessary_new_paid_services_and_upgrades_authorized',true,'physical_health_checks_retained',true,'source_integrity_and_stop_controls_retained',true);
 p:=jsonb_set(r.protections,'{budget_policy}',policy,true);
 p:=jsonb_set(p,'{no_new_paid_service_costs}','false'::jsonb,true);
 p:=jsonb_set(p,'{large_backfill_commit_gate}',to_jsonb('Verified actual available storage and valid source access; financial and work budgets removed by explicit user instruction'::text),true);
 FOREACH path SLICE 1 IN ARRAY ARRAY[['source_acquisition'],['daily_feature_build'],['native_listing_import'],['resource_accounting_v2'],['shared_feature_materialization']] LOOP
  p:=jsonb_set(p,path,(p#>path)||jsonb_build_object('no_new_paid_service_costs',false,'budget_limits_enforced',false),true);
 END LOOP;
 p:=jsonb_set(p,'{source_acquisition,binance_daily_finite}',(p#>'{source_acquisition,binance_daily_finite}')||jsonb_build_object('no_new_paid_service_costs',false,'budget_limits_enforced',false),true);
 FOREACH path SLICE 1 IN ARRAY ARRAY[['shared_feature_runner','expires_at'],['native_listing_runner','expires_at'],['source_acquisition','max_storage_bytes'],['source_acquisition','max_wal_growth_bytes'],['shared_feature_materialization','max_storage_bytes'],['shared_feature_materialization','max_wal_growth_bytes'],['native_listing_import','max_relation_bytes'],['native_listing_runner','max_wal_growth_bytes'],['daily_feature_build','max_relation_bytes'],['resource_accounting_v2','max_observations'],['resource_accounting_v2','generated_wal_cap_bytes'],['resource_accounting_v2','cache_relation_cap_bytes'],['resource_accounting_v2','source_relation_cap_bytes'],['resource_accounting_v2','native_relation_cap_bytes']] LOOP
  p:=jsonb_set(p,path,'null'::jsonb,true);
 END LOOP;
 -- Counters, leases, source claims, task identities and completed checkpoints are preserved.
 SELECT jsonb_object_agg(signature,encode(sha256(convert_to(pg_get_functiondef(signature::regprocedure),'UTF8')),'hex')) INTO hashes
 FROM (VALUES('astra_shared_equity_features_20260928_v1.rebuild_feature_sessions_remediated_v1(integer,integer,date,date)'),('market_governance.remediation_import_native_listing_page_v1(text,text,text)')) f(signature);
 after_record:=jsonb_build_object('protections',p,'function_hash_overrides',hashes,'worker_deployment_pending',true);
 INSERT INTO market_governance.remediation_change_v1(change_id,run_id,finding_ids,relation_name,row_identity,before_row,after_row,before_sha256,after_sha256,operation,evidence,validation)
 VALUES('OPS:BUDGET_REMOVAL:20261008_130925',r.run_id,ARRAY['RT-07','IP-06','CD-F06','CD-F07'],'market_governance.remediation_run_v1',jsonb_build_object('run_id',r.run_id),jsonb_build_object('protections',r.protections),after_record,encode(sha256(convert_to(r.protections::text,'UTF8')),'hex'),encode(sha256(convert_to(after_record::text,'UTF8')),'hex'),'EXPLICIT_USER_REMOVAL_OF_ALL_REMEDIATION_BUDGETS',jsonb_build_object('explicit_user_instruction','Remove budgets applies to everything','requested_at','2026-10-08T13:09:25Z','prior_clarification','Includes lifting the restriction on new paid-service costs','sql_changes','budget_removal/migration.sql'),jsonb_build_object('counter_resets',0,'scope_and_manifest_changes',0,'source_acquisition_automatically_restarted',false,'unrelated_stops_preserved',true,'frozen_data_changes',0,'function_hash_guards_revised_with_exact_successor_hashes',true));
 UPDATE market_governance.remediation_run_v1 SET protections=p,updated_at=clock_timestamp() WHERE run_id=r.run_id;
 IF NOT market_governance.remediation_budgets_removed_v1() THEN RAISE EXCEPTION 'policy_activation_verification_failed';END IF;
END $record$;
NOTIFY pgrst,'reload schema';
