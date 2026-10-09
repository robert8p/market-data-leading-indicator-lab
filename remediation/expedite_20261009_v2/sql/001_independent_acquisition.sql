CREATE OR REPLACE FUNCTION public.market_data_remediation_compact_feature_step_v1(p_request jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'pg_catalog'
 SET "TimeZone" TO 'UTC'
 SET statement_timeout TO '30s'
 SET lock_timeout TO '5s'
 SET work_mem TO '32MB'
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
 OR EXISTS(SELECT 1 FROM market_governance.remediation_source_batch_v1 b WHERE run_id=v_run.run_id AND status='RUNNING'
 AND NOT (coalesce(v_run.protections#>>'{expedite_v2,acquisition_overlap_enabled}'='true',false)
 AND market_governance.remediation_noncrypto_task_v1(to_jsonb(b))))THEN RETURN v_base||jsonb_build_object('status','PAUSED','reason','exclusive_materialization_lane_required');END IF;
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

