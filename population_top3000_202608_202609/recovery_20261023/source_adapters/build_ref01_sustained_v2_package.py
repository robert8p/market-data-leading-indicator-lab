from pathlib import Path
import json, hashlib
D=Path(__file__).resolve().parent
policy=json.loads((D/'ref01_sustained_v2_policy_DRAFT.json').read_text())
policy_sql="$policy$"+json.dumps(policy,separators=(',',':'))+"$policy$::jsonb"
fixture=r'''-- ROOT-ONLY actual retained-data rollback. No external call. Run after installing the schema,
-- cron 596 paused and leases drained. All actual changes and test-only runs roll back.
-- A disclosed 13-line subset uses all 14 real census pages plus its actual dated overviews.
-- It exercises all three new producer stages and the bounded validation bridge through the real dispatcher.
BEGIN;
SET LOCAL lock_timeout='3s';SET LOCAL statement_timeout='55s';
SELECT pg_advisory_xact_lock(2026083000261022);
CREATE TEMP TABLE ref01_rollback_receipt_v2(result jsonb) ON COMMIT DROP;
CREATE FUNCTION pg_temp.ref01_fixture_lease_v2(p_task uuid,p_token text)
RETURNS uuid LANGUAGE plpgsql AS $f$
DECLARE v_attempt uuid:=gen_random_uuid();v_exec bigint;v_hash text;
BEGIN
 SELECT t.executions+1,a.function_sha256 INTO v_exec,v_hash FROM population_top3000_202608_202609_v1.pipeline_task t JOIN population_top3000_202608_202609_v1.pipeline_adapter a USING(family_id,stage) WHERE t.task_id=p_task;
 UPDATE population_top3000_202608_202609_v1.pipeline_task SET status='LEASED',attempt_id=v_attempt,lease_until=clock_timestamp()+interval '2 minutes',owner_fence=2,stop_generation=0,executions=v_exec WHERE task_id=p_task;
 INSERT INTO population_top3000_202608_202609_v1.pipeline_attempt(attempt_id,task_id,execution_number,token_sha256,owner_fence,stop_generation,adapter_sha256)
 VALUES(v_attempt,p_task,v_exec,encode(sha256(convert_to(p_token,'UTF8')),'hex'),2,0,v_hash);
 RETURN v_attempt;
END $f$;
DO $test$
DECLARE v_run text:='ref01-v2-rollback-'||gen_random_uuid()::text;v_policy jsonb:=__POLICY__;
 v_seed jsonb;v_legacy record;v_hash text;v_tid uuid;v_acquire uuid;v_normalize uuid;v_derive uuid;v_validate uuid;v_attempt uuid;v_token text:='transaction-only-ref01-test-token';
 v_result jsonb;v_again jsonb;v_timings jsonb:='[]';v_stage text;v_started timestamptz;v_guard_count integer:=0;v_ticks integer:=0;v_loops integer;v_hashes jsonb;v_old_hashes jsonb;
 v_before jsonb;v_after jsonb;v_frozen_before jsonb;v_tasks_before bigint;v_legacy_outcome text;v_wrapped_outcome text;v_fixture_units integer;
BEGIN
 IF EXISTS(SELECT 1 FROM cron.job WHERE jobid=596 AND active) OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE status='LEASED') OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_provider_lease WHERE released_at IS NULL) THEN RAISE EXCEPTION 'REF01_TEST_REQUIRES_PAUSED_DRAINED_QUEUE';END IF;
 SELECT jsonb_build_object('provider',c.provider_requests,'http',c.http_invocations,'stored_bytes',c.stored_bytes,'reserved_bytes',c.reserved_bytes) INTO v_before FROM population_top3000_202608_202609_v1.acquisition_control c WHERE c.singleton;
 SELECT to_jsonb(u) INTO v_frozen_before FROM population_top3000_202608_202609_v1.pipeline_universe u WHERE u.singleton;
 SELECT count(*) INTO v_tasks_before FROM population_top3000_202608_202609_v1.company_source_request_v1;
 UPDATE population_top3000_202608_202609_v1.pipeline_task SET not_before='infinity',blocker_until=NULL WHERE status IN('READY','BLOCKED');
 UPDATE population_top3000_202608_202609_v1.pipeline_control SET enabled=true,next_wakeup_at=clock_timestamp() WHERE singleton;
 PERFORM set_config('ref01.rollback_fixture',v_run,true);
 v_started:=clock_timestamp();
 v_seed:=population_top3000_202608_202609_v1.ref01_resolution_seed_v2(v_run,v_policy,ARRAY['AAPL','CRD.A','CRD.B','DHC','DHCNI','DHCNL','GOOG','GOOGL','LPX','NBN','TENX','WEAV','TSM']);
 v_timings:=v_timings||jsonb_build_array(jsonb_build_object('step','real_input_snapshot','elapsed_ms',extract(epoch FROM clock_timestamp()-v_started)*1000));
 IF v_seed->>'purpose'<>'ROLLBACK_FIXTURE' OR v_seed->>'census_lines'<>'13' OR v_seed->>'census_sources'<>'14' THEN RAISE EXCEPTION 'REF01_TEST_SUBSET_NOT_EXPLICIT';END IF;
 FOR v_legacy IN SELECT * FROM population_top3000_202608_202609_v1.pipeline_adapter WHERE family_id='REF01' AND stage IN('ACQUIRE','NORMALIZE','DERIVE','VALIDATE') LOOP
  IF v_legacy.function_sha256 IS DISTINCT FROM encode(sha256(convert_to(pg_get_functiondef(v_legacy.function_oid),'UTF8')),'hex') THEN RAISE EXCEPTION 'REF01_TEST_LEGACY_HASH_CHANGED';END IF;
  INSERT INTO population_top3000_202608_202609_v1.ref01_sustained_legacy_adapter_v2(stage,function_oid,function_sha256,registration_evidence)
  VALUES(v_legacy.stage,v_legacy.function_oid,v_legacy.function_sha256,jsonb_build_object('fixture_capture',true,'original_adapter',to_jsonb(v_legacy))) ON CONFLICT DO NOTHING;
  IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.ref01_sustained_legacy_adapter_v2 l WHERE l.stage=v_legacy.stage AND l.function_oid=v_legacy.function_oid AND l.function_sha256=v_legacy.function_sha256) THEN RAISE EXCEPTION 'REF01_TEST_LEGACY_CAPTURE_CONFLICT';END IF;
  v_hash:=encode(sha256(convert_to(pg_get_functiondef(('population_top3000_202608_202609_v1.ref01_resolution_route_'||lower(v_legacy.stage)||'_v2(uuid,text)')::regprocedure),'UTF8')),'hex');
  UPDATE population_top3000_202608_202609_v1.pipeline_adapter SET function_oid=('population_top3000_202608_202609_v1.ref01_resolution_route_'||lower(v_legacy.stage)||'_v2(uuid,text)')::regprocedure::oid,
   function_sha256=v_hash,resource_class='DATABASE',provider=NULL,enabled=true,expected_batch_seconds=10,producer_revision='ref01-sustained-v2-rollback',reviewed_test_evidence='{"test":"actual-retained-subset","state":"ROLLBACK_ONLY"}' WHERE family_id='REF01' AND stage=v_legacy.stage;
 END LOOP;
 SELECT jsonb_object_agg(a.stage,a.function_sha256) INTO v_hashes FROM population_top3000_202608_202609_v1.pipeline_adapter a WHERE a.family_id='REF01';
 SELECT jsonb_object_agg(a.stage,a.function_sha256) INTO v_old_hashes FROM population_top3000_202608_202609_v1.ref01_sustained_legacy_adapter_v2 a;
 IF (SELECT count(*) FROM jsonb_object_keys(v_old_hashes))<>4 THEN RAISE EXCEPTION 'REF01_FOUR_LEGACY_ADAPTERS_REQUIRED';END IF;
 v_acquire:=population_top3000_202608_202609_v1.pipeline_enqueue_v1('REF01','ACQUIRE',v_run,jsonb_build_object('kind','REF01_SUSTAINED_RECONCILIATION_V2','run_key',v_run),v_seed->'input_versions');
 v_tid:=v_acquire;
 FOREACH v_stage IN ARRAY ARRAY['ACQUIRE','NORMALIZE','DERIVE','VALIDATE'] LOOP
  v_loops:=0;
  LOOP
   v_started:=clock_timestamp();v_result:=population_top3000_202608_202609_v1.pipeline_tick_v1();v_ticks:=v_ticks+1;v_loops:=v_loops+1;
   v_timings:=v_timings||jsonb_build_array(jsonb_build_object('step',v_stage,'iteration',v_loops,'elapsed_ms',extract(epoch FROM clock_timestamp()-v_started)*1000,'result_state',v_result#>>'{result,state}'));
   IF (v_result->>'task_id')::uuid IS DISTINCT FROM v_tid OR (v_result#>>'{result,state}' IN('CONTINUE','COMPLETE','BLOCKED')) IS DISTINCT FROM true THEN RAISE EXCEPTION 'REF01_ACTUAL_DISPATCH_FAILED stage=% result=%',v_stage,v_result;END IF;
   EXIT WHEN v_result#>>'{result,state}'<>'CONTINUE';
   IF v_loops>=5 THEN RAISE EXCEPTION 'REF01_FIXTURE_BATCH_BOUND_EXCEEDED';END IF;
  END LOOP;
  IF v_stage='VALIDATE' THEN
   IF v_result#>>'{result,state}' IS DISTINCT FROM 'BLOCKED' OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task t WHERE t.task_id=v_tid AND t.blocker_code='REF01_INDEPENDENT_COMPANY_CUTOFF_AND_LISTING_RECONCILIATION_REQUIRED' AND t.blocker_until IS NULL AND t.resumption_condition->>'mission_complete'='false') THEN RAISE EXCEPTION 'REF01_UNRESOLVED_COMPANY_GATES_WERE_NOT_PRESERVED';END IF;
   v_validate:=v_tid;
  ELSE
   IF v_result#>>'{result,state}' IS DISTINCT FROM 'COMPLETE' THEN RAISE EXCEPTION 'REF01_PRODUCER_FAILED stage=% result=%',v_stage,v_result;END IF;
   IF v_stage='ACQUIRE' THEN v_normalize:=(v_result#>>'{result,successor_task_id}')::uuid;v_tid:=v_normalize;
   ELSIF v_stage='NORMALIZE' THEN v_derive:=(v_result#>>'{result,successor_task_id}')::uuid;v_tid:=v_derive;
   ELSE v_tid:=(v_result#>>'{result,successor_task_id}')::uuid;END IF;
   IF v_tid IS NULL THEN RAISE EXCEPTION 'REF01_AUTOMATIC_STAGE_SUCCESSOR_MISSING';END IF;
  END IF;
 END LOOP;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.ref01_security_decision_v2 s WHERE s.run_key=v_run AND s.ticker='DHC' AND s.eligibility_state='SOURCE_SUPPORTED_COMMON')
 OR (SELECT count(*) FROM population_top3000_202608_202609_v1.ref01_security_decision_v2 s WHERE s.run_key=v_run AND s.ticker IN('DHCNI','DHCNL') AND s.eligibility_state='SOURCE_SUPPORTED_EXCLUSION')<>2
 OR (SELECT case_key FROM population_top3000_202608_202609_v1.ref01_security_decision_v2 WHERE run_key=v_run AND ticker='NBN') IS NOT DISTINCT FROM (SELECT case_key FROM population_top3000_202608_202609_v1.ref01_security_decision_v2 WHERE run_key=v_run AND ticker='LPX') THEN RAISE EXCEPTION 'REF01_KNOWN_COMPONENT_RESOLUTIONS_LOST';END IF;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.ref01_company_decision_v2 d WHERE d.run_key=v_run AND d.content->'member_tickers' ? 'AAPL' AND d.decision_state='SOURCE_QUALIFIED_DATED_VENDOR' AND d.verified_rank_cap_usd IS NULL AND d.content->>'actual_rank_date_shares_proven'='false') THEN RAISE EXCEPTION 'REF01_STRAIGHTFORWARD_VENDOR_ROUTE_NOT_USABLE';END IF;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.ref01_company_decision_v2 d WHERE d.run_key=v_run AND d.content->'member_tickers' ? 'CRD.A' AND d.source_company_cap_usd IS NULL AND d.content->'targeted_requirement_codes' ? 'MULTICLASS_OR_SHARED_CIK_RECONCILIATION') THEN RAISE EXCEPTION 'REF01_MULTICLASS_CAP_WAS_ARBITRARILY_AGGREGATED';END IF;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.ref01_company_decision_v2 d WHERE d.run_key=v_run AND d.content->'member_tickers' ? 'TSM' AND d.content->'targeted_requirement_codes' ? 'ADR_GLOBAL_CAP_AND_RATIO_RECONCILIATION') THEN RAISE EXCEPTION 'REF01_ADR_SPECIALIST_GATE_MISSING';END IF;
 -- Exact producer replays, including existing output value/state/provenance, add no useful units.
 FOREACH v_stage IN ARRAY ARRAY['ACQUIRE','NORMALIZE','DERIVE'] LOOP
  BEGIN
   v_tid:=CASE v_stage WHEN 'ACQUIRE' THEN v_acquire WHEN 'NORMALIZE' THEN v_normalize ELSE v_derive END;
   v_attempt:=pg_temp.ref01_fixture_lease_v2(v_tid,v_token);
   v_started:=clock_timestamp();v_again:=population_top3000_202608_202609_v1.ref01_resolution_route_v2(v_attempt,v_token,v_stage);
   IF v_again->>'state' IS DISTINCT FROM 'COMPLETE' OR v_again#>>'{commit_receipt,new_committed_units}' IS DISTINCT FROM '0' THEN RAISE EXCEPTION 'REF01_EXACT_REPLAY_NOT_ZERO stage=% result=%',v_stage,v_again;END IF;
   v_timings:=v_timings||jsonb_build_array(jsonb_build_object('step',v_stage||'_REPLAY','elapsed_ms',extract(epoch FROM clock_timestamp()-v_started)*1000));
   RAISE SQLSTATE 'ZT001';
  EXCEPTION WHEN SQLSTATE 'ZT001' THEN NULL;END;
 END LOOP;
 -- Fail-closed assertions are isolated subtransactions and preserve completed producer rows.
 BEGIN
  v_attempt:=pg_temp.ref01_fixture_lease_v2(v_acquire,v_token);
  UPDATE population_top3000_202608_202609_v1.pipeline_attempt SET owner_fence=3 WHERE attempt_id=v_attempt;
  PERFORM population_top3000_202608_202609_v1.ref01_resolution_acquire_v2(v_attempt,v_token);RAISE EXCEPTION 'STALE_OWNER_WAS_ACCEPTED';
 EXCEPTION WHEN raise_exception THEN IF SQLERRM IS DISTINCT FROM 'STALE_OWNER_OR_STOP' THEN RAISE;END IF;v_guard_count:=v_guard_count+1;END;
 BEGIN
  v_attempt:=pg_temp.ref01_fixture_lease_v2(v_acquire,v_token);
  PERFORM population_top3000_202608_202609_v1.ref01_resolution_acquire_v2(v_attempt,'wrong-token');RAISE EXCEPTION 'BAD_TOKEN_WAS_ACCEPTED';
 EXCEPTION WHEN raise_exception THEN IF SQLERRM IS DISTINCT FROM 'ATTEMPT_AUTH_FAILED' THEN RAISE;END IF;v_guard_count:=v_guard_count+1;END;
 BEGIN
  v_attempt:=pg_temp.ref01_fixture_lease_v2(v_acquire,v_token);
  UPDATE population_top3000_202608_202609_v1.pipeline_task SET input_versions=input_versions||'{"unexpected_revision":true}' WHERE task_id=v_acquire;
  PERFORM population_top3000_202608_202609_v1.ref01_resolution_acquire_v2(v_attempt,v_token);RAISE EXCEPTION 'FORGED_INPUTS_WERE_ACCEPTED';
 EXCEPTION WHEN raise_exception THEN IF SQLERRM IS DISTINCT FROM 'REF01_RUN_STAGE_INPUT_OR_BUNDLE_MISMATCH' THEN RAISE;END IF;v_guard_count:=v_guard_count+1;END;
 BEGIN
  v_attempt:=pg_temp.ref01_fixture_lease_v2(v_acquire,v_token);
  UPDATE population_top3000_202608_202609_v1.ref01_resolution_source_v2 SET source_sha256=repeat('0',64) WHERE run_key=v_run AND source_ordinal=1;
  PERFORM population_top3000_202608_202609_v1.ref01_resolution_acquire_v2(v_attempt,v_token);RAISE EXCEPTION 'FORGED_SOURCE_WAS_ACCEPTED';
 EXCEPTION WHEN raise_exception THEN IF SQLERRM NOT LIKE 'REF01_SOURCE_VERSION_OR_DATE_MISMATCH%' THEN RAISE;END IF;v_guard_count:=v_guard_count+1;END;
 BEGIN
  v_attempt:=pg_temp.ref01_fixture_lease_v2(v_normalize,v_token);
  DELETE FROM population_top3000_202608_202609_v1.ref01_resolution_stage_v2 WHERE run_key=v_run AND stage='ACQUIRE';
  PERFORM population_top3000_202608_202609_v1.ref01_resolution_normalize_v2(v_attempt,v_token);RAISE EXCEPTION 'INCOMPLETE_ACQUIRE_WAS_ACCEPTED';
 EXCEPTION WHEN raise_exception THEN IF SQLERRM IS DISTINCT FROM 'REF01_COMPLETE_SOURCE_STAGE_REQUIRED' THEN RAISE;END IF;v_guard_count:=v_guard_count+1;END;
 BEGIN
  v_attempt:=pg_temp.ref01_fixture_lease_v2(v_normalize,v_token);
  UPDATE population_top3000_202608_202609_v1.ref01_resolution_record_proof_v2 SET record_sha256=repeat('0',64) WHERE run_key=v_run AND ticker='AAPL' AND source_job_id LIKE 'overview:%';
  PERFORM population_top3000_202608_202609_v1.ref01_resolution_normalize_v2(v_attempt,v_token);RAISE EXCEPTION 'FORGED_RECORD_PROOF_WAS_ACCEPTED';
 EXCEPTION WHEN raise_exception THEN IF SQLERRM IS DISTINCT FROM 'REF01_OVERVIEW_RECORD_NOT_VERIFIED' THEN RAISE;END IF;v_guard_count:=v_guard_count+1;END;
 BEGIN
  -- Keep the exact price/share arithmetic intact while marking its source OHLC row
  -- internally incoherent. The projection itself must refuse price qualification.
  UPDATE population_top3000_202608_202609_v1.d02_daily_bar_v2 b SET structure_mask=b.structure_mask|1
  FROM population_top3000_202608_202609_v1.d02_daily_source_v2 s
  WHERE s.source_day_id=b.source_day_id AND s.session_date=DATE '2026-08-03'
   AND s.source_sha256=v_policy->>'rank_price_source_sha256' AND b.source_ticker='AAPL';
  GET DIAGNOSTICS v_fixture_units=ROW_COUNT;
  IF v_fixture_units<>1 THEN RAISE EXCEPTION 'REF01_STRUCTURE_MASK_TEST_REQUIRES_ONE_PRICE';END IF;
  v_result:=population_top3000_202608_202609_v1.ref01_resolution_security_projection_v2(v_run,'AAPL');
  IF v_result->'rank_price_arithmetic_consistent' IS DISTINCT FROM 'false'::jsonb
   OR ((v_result->'diagnostic_codes') ? 'RANK_PRICE_OR_UNIT_CONFLICT') IS DISTINCT FROM true
   OR coalesce((v_result#>>'{rank_price_rows,0,structure_mask}')::integer,0)=0
  THEN RAISE EXCEPTION 'REF01_STRUCTURALLY_FLAGGED_PRICE_WAS_QUALIFIED';END IF;
  RAISE EXCEPTION 'REF01_STRUCTURALLY_FLAGGED_PRICE_REJECTED';
 EXCEPTION WHEN raise_exception THEN IF SQLERRM IS DISTINCT FROM 'REF01_STRUCTURALLY_FLAGGED_PRICE_REJECTED' THEN RAISE;END IF;v_guard_count:=v_guard_count+1;END;
 BEGIN
  v_attempt:=pg_temp.ref01_fixture_lease_v2(v_normalize,v_token);
  UPDATE population_top3000_202608_202609_v1.ref01_security_decision_v2 SET projection=projection||'{"native_name":"FORGED"}',output_sha256=encode(sha256(convert_to((projection||'{"native_name":"FORGED"}')::text,'UTF8')),'hex') WHERE run_key=v_run AND ticker='AAPL';
  PERFORM population_top3000_202608_202609_v1.ref01_resolution_normalize_v2(v_attempt,v_token);RAISE EXCEPTION 'JOINT_SECURITY_HASH_TAMPER_WAS_ACCEPTED';
 EXCEPTION WHEN raise_exception THEN IF SQLERRM IS DISTINCT FROM 'REF01_SECURITY_REPLAY_CONFLICT' THEN RAISE;END IF;v_guard_count:=v_guard_count+1;END;
 BEGIN
  v_attempt:=pg_temp.ref01_fixture_lease_v2(v_derive,v_token);
  UPDATE population_top3000_202608_202609_v1.ref01_company_decision_v2 SET content=content||'{"valuation_method":"FORGED"}',output_sha256=encode(sha256(convert_to((content||'{"valuation_method":"FORGED"}')::text,'UTF8')),'hex') WHERE run_key=v_run AND content->'member_tickers' ? 'AAPL';
  PERFORM population_top3000_202608_202609_v1.ref01_resolution_derive_v2(v_attempt,v_token);RAISE EXCEPTION 'JOINT_COMPANY_HASH_TAMPER_WAS_ACCEPTED';
 EXCEPTION WHEN raise_exception THEN IF SQLERRM IS DISTINCT FROM 'REF01_COMPANY_DECISION_REPLAY_CONFLICT' THEN RAISE;END IF;v_guard_count:=v_guard_count+1;END;
 BEGIN
  v_attempt:=pg_temp.ref01_fixture_lease_v2(v_normalize,v_token);
  UPDATE population_top3000_202608_202609_v1.pipeline_task SET checkpoint='{"last_ordinal":1,"batch_number":99}' WHERE task_id=v_normalize;
  PERFORM population_top3000_202608_202609_v1.ref01_resolution_normalize_v2(v_attempt,v_token);RAISE EXCEPTION 'UNCOMMITTED_CHECKPOINT_WAS_ACCEPTED';
 EXCEPTION WHEN raise_exception THEN IF SQLERRM IS DISTINCT FROM 'REF01_CHECKPOINT_WITHOUT_COMMITTED_PREDECESSOR' THEN RAISE;END IF;v_guard_count:=v_guard_count+1;END;
 SELECT jsonb_build_object('provider',c.provider_requests,'http',c.http_invocations,'stored_bytes',c.stored_bytes,'reserved_bytes',c.reserved_bytes) INTO v_after FROM population_top3000_202608_202609_v1.acquisition_control c WHERE c.singleton;
 IF v_before IS DISTINCT FROM v_after OR v_guard_count<>10 OR EXISTS(SELECT 1 FROM cron.job WHERE jobid=596 AND active)
 OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_provider_lease WHERE released_at IS NULL)
 OR v_frozen_before IS DISTINCT FROM (SELECT to_jsonb(u) FROM population_top3000_202608_202609_v1.pipeline_universe u WHERE u.singleton)
 OR v_tasks_before<>(SELECT count(*) FROM population_top3000_202608_202609_v1.company_source_request_v1) THEN RAISE EXCEPTION 'REF01_TEST_CHANGED_EXTERNAL_ACCOUNTING_UNIVERSE_OR_OLD_SOURCE_QUEUE';END IF;
 INSERT INTO ref01_rollback_receipt_v2 VALUES(jsonb_build_object('result','PASS','test','ACTUAL_RETAINED_SUBSET_REF01_RECONCILIATION_V2','tested_at',clock_timestamp(),
 'resolution_bundle_sha256',population_top3000_202608_202609_v1.ref01_resolution_bundle_sha256_v2(),'policy_sha256',encode(sha256(convert_to(v_policy::text,'UTF8')),'hex'),
 'tested_function_sha256',v_hashes,'preserved_legacy_function_sha256',v_old_hashes,'input_subset',v_seed,
 'actual_dispatches',v_ticks,'negative_guards',v_guard_count,'known_DHC_and_NBN_components_preserved',true,'multiclass_and_ADR_not_auto_accepted',true,'three_exact_replays_zero_new_units',true,'internally_incoherent_daily_price_rejected',true,
 'automatic_stage_successors',true,'original_universe_unchanged',true,'old_blanket_source_queue_unchanged',true,'no_provider_request',true,
 'company_population_verified',false,'original_historical_receipt_verified',false,'rank_cap_independently_verified',false,'all_writes_rolled_back',true,'timings',v_timings));
END $test$;
SELECT result FROM ref01_rollback_receipt_v2;
ROLLBACK;
'''.replace('__POLICY__',policy_sql)
(D/'ref01_sustained_v2_rollback_test_DRAFT.sql').write_text(fixture)
registration=r'''-- ROOT-ONLY after actual rollback PASS. No automatic wake; root separately admits/release.
-- Snapshot all actual retained inputs, capture all old handlers, bind reviewed new wrappers,
-- and create one parked ACQUIRE task. Its normal successors are produced by pipeline_finish_v1.
BEGIN;
SET LOCAL lock_timeout='3s';SET LOCAL statement_timeout='15s';
SELECT pg_advisory_xact_lock(2026083000261022);
DO $admit$
DECLARE v_policy jsonb:=__POLICY__;v_run text:='ref01-company-reconciliation-v2-20261023';v_seed jsonb;v_test jsonb;v_bundle text;v_hashes jsonb;v_old_hashes jsonb;v_legacy record;v_tid uuid;v_existing record;
BEGIN
 IF EXISTS(SELECT 1 FROM cron.job WHERE jobid=596 AND active) OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE status='LEASED') OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_provider_lease WHERE released_at IS NULL) THEN RAISE EXCEPTION 'REF01_ADMISSION_REQUIRES_PAUSED_DRAINED_QUEUE';END IF;
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.ref01_resolution_run_v2) OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.ref01_sustained_legacy_adapter_v2) THEN RAISE EXCEPTION 'REF01_EXISTING_V2_PROGRESS_REQUIRES_RECONCILIATION';END IF;
 v_bundle:=population_top3000_202608_202609_v1.ref01_resolution_bundle_sha256_v2();
 SELECT jsonb_object_agg(x.stage,encode(sha256(convert_to(pg_get_functiondef(('population_top3000_202608_202609_v1.ref01_resolution_route_'||lower(x.stage)||'_v2(uuid,text)')::regprocedure),'UTF8')),'hex')) INTO v_hashes FROM(VALUES('ACQUIRE'),('NORMALIZE'),('DERIVE'),('VALIDATE'))x(stage);
 SELECT jsonb_object_agg(a.stage,a.function_sha256) INTO v_old_hashes FROM population_top3000_202608_202609_v1.pipeline_adapter a WHERE a.family_id='REF01' AND a.stage IN('ACQUIRE','NORMALIZE','DERIVE','VALIDATE');
 SELECT e.details INTO v_test FROM population_top3000_202608_202609_v1.pipeline_event e WHERE e.event='REF01_SUSTAINED_V2_ROLLBACK_VERIFIED' AND e.details->>'result'='PASS'
  AND e.details->>'resolution_bundle_sha256'=v_bundle AND e.details->>'policy_sha256'=encode(sha256(convert_to(v_policy::text,'UTF8')),'hex')
  AND e.details->'tested_function_sha256'=v_hashes AND e.details->'preserved_legacy_function_sha256'=v_old_hashes AND e.details->>'negative_guards'='10'
  AND e.details->>'internally_incoherent_daily_price_rejected'='true'
  AND e.details->>'no_provider_request'='true' AND e.details->>'automatic_stage_successors'='true' AND e.details->>'all_writes_rolled_back'='true' ORDER BY e.at DESC LIMIT 1;
 IF v_test IS NULL THEN RAISE EXCEPTION 'REF01_ACTUAL_CURRENT_FUNCTION_AND_POLICY_ROLLBACK_PASS_REQUIRED';END IF;
 v_seed:=population_top3000_202608_202609_v1.ref01_resolution_seed_v2(v_run,v_policy,NULL);
 IF v_seed->>'purpose' IS DISTINCT FROM 'FULL_CENSUS' OR v_seed->>'census_lines' IS DISTINCT FROM '13044' OR v_seed->>'overview_sources' IS DISTINCT FROM '5684' OR v_seed->>'source_jobs' IS DISTINCT FROM '5698' THEN RAISE EXCEPTION 'REF01_FULL_ORIGINAL_POPULATION_REQUIRED';END IF;
 FOR v_legacy IN SELECT * FROM population_top3000_202608_202609_v1.pipeline_adapter WHERE family_id='REF01' AND stage IN('ACQUIRE','NORMALIZE','DERIVE','VALIDATE') LOOP
  IF NOT v_legacy.enabled OR v_legacy.function_sha256 IS DISTINCT FROM encode(sha256(convert_to(pg_get_functiondef(v_legacy.function_oid),'UTF8')),'hex') THEN RAISE EXCEPTION 'REF01_CURRENT_LEGACY_ADAPTER_CHANGED';END IF;
  INSERT INTO population_top3000_202608_202609_v1.ref01_sustained_legacy_adapter_v2(stage,function_oid,function_sha256,registration_evidence)
  VALUES(v_legacy.stage,v_legacy.function_oid,v_legacy.function_sha256,jsonb_build_object('original_adapter',to_jsonb(v_legacy),'actual_rollback',v_test,'captured_without_function_mutation',true));
  UPDATE population_top3000_202608_202609_v1.pipeline_adapter SET function_oid=('population_top3000_202608_202609_v1.ref01_resolution_route_'||lower(v_legacy.stage)||'_v2(uuid,text)')::regprocedure::oid,
   function_sha256=v_hashes->>v_legacy.stage,resource_class='DATABASE',provider=NULL,enabled=true,expected_batch_seconds=10,producer_revision='ref01-sustained-retained-v2',
   reviewed_test_evidence=jsonb_build_object('actual_rollback',v_test,'legacy_adapter',to_jsonb(v_legacy),'resolution_bundle_sha256',v_bundle,'source_only',true,'company_universe_certified',false)
  WHERE family_id='REF01' AND stage=v_legacy.stage;
 END LOOP;
 v_tid:=population_top3000_202608_202609_v1.pipeline_enqueue_v1('REF01','ACQUIRE',v_run,jsonb_build_object('kind','REF01_SUSTAINED_RECONCILIATION_V2','run_key',v_run),v_seed->'input_versions');
 SELECT * INTO v_existing FROM population_top3000_202608_202609_v1.pipeline_task t WHERE t.task_id=v_tid;
 IF v_existing.payload IS DISTINCT FROM jsonb_build_object('kind','REF01_SUSTAINED_RECONCILIATION_V2','run_key',v_run) OR v_existing.input_versions IS DISTINCT FROM v_seed->'input_versions' OR v_existing.status IS DISTINCT FROM 'READY' OR v_existing.executions<>0 THEN RAISE EXCEPTION 'REF01_ENQUEUE_REPLAY_OR_PAYLOAD_CONFLICT';END IF;
 UPDATE population_top3000_202608_202609_v1.pipeline_task SET not_before='infinity' WHERE task_id=v_tid;
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(task_id,event,details) VALUES(v_tid,'REF01_SUSTAINED_V2_REGISTERED',jsonb_build_object(
 'run_key',v_run,'owner_id','top3000-recovery-20261023-root-4b1314bcb5c2','owner_fence',2,'stop_generation',0,'resolution_bundle_sha256',v_bundle,'seed',v_seed,
 'preserved_legacy_function_sha256',v_old_hashes,'new_function_sha256',v_hashes,'actual_rollback',v_test,'source_batch_jobs',50,'source_batch_max_bytes',8388608,'security_batch_rows',200,'company_batch_cases',100,
 'automatic_stage_successors',true,'retained_only',true,'external_requests_admitted',0,'source_qualified_reconstruction_not_rank_certification',true,'requires_explicit_controller_release',true,'scope_or_budget_changed',false));
END $admit$;
COMMIT;
'''.replace('__POLICY__',policy_sql)
(D/'ref01_sustained_v2_registration_DRAFT.sql').write_text(registration)
readback='''BEGIN READ ONLY;SET LOCAL statement_timeout='8s';
SELECT jsonb_build_object('at',clock_timestamp(),
 'new_ref01_relation_bytes',(SELECT coalesce(sum(pg_total_relation_size(c.oid)),0) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='population_top3000_202608_202609_v1' AND c.relkind IN('r','m') AND (c.relname LIKE 'ref01_resolution_%_v2' OR c.relname IN('ref01_security_decision_v2','ref01_company_work_v2','ref01_company_decision_v2','ref01_evidence_requirement_v2','ref01_sustained_legacy_adapter_v2'))),
 'runs',(SELECT jsonb_agg(to_jsonb(r)-'policy' ORDER BY r.created_at) FROM population_top3000_202608_202609_v1.ref01_resolution_run_v2 r),
 'stages',(SELECT jsonb_agg(to_jsonb(s) ORDER BY s.run_key,s.stage) FROM population_top3000_202608_202609_v1.ref01_resolution_stage_v2 s),
 'tasks',(SELECT jsonb_agg(jsonb_build_object('task_id',t.task_id,'stage',t.stage,'status',t.status,'checkpoint',t.checkpoint,'executions',t.executions,'cumulative_failures',t.cumulative_failures,'blocker_code',t.blocker_code,'not_before',t.not_before) ORDER BY t.created_at) FROM population_top3000_202608_202609_v1.pipeline_task t WHERE t.family_id='REF01' AND t.payload->>'kind'='REF01_SUSTAINED_RECONCILIATION_V2'),
 'security_states',(SELECT jsonb_agg(to_jsonb(x)) FROM(SELECT run_key,eligibility_state,company_identity_state,count(*) rows FROM population_top3000_202608_202609_v1.ref01_security_decision_v2 GROUP BY run_key,eligibility_state,company_identity_state)x),
 'company_states',(SELECT jsonb_agg(to_jsonb(x)) FROM(SELECT run_key,decision_revision,decision_state,count(*) cases FROM population_top3000_202608_202609_v1.ref01_company_decision_v2 GROUP BY run_key,decision_revision,decision_state)x),
 'requirements',(SELECT jsonb_agg(to_jsonb(x)) FROM(SELECT run_key,requirement_code,count(*) cases FROM population_top3000_202608_202609_v1.ref01_evidence_requirement_v2 GROUP BY run_key,requirement_code)x),
 'universe',(SELECT to_jsonb(u) FROM population_top3000_202608_202609_v1.pipeline_universe u WHERE singleton)) AS ref01_sustained_status;
ROLLBACK;
'''
(D/'ref01_sustained_v2_first_readback.sql').write_text(readback)
files=['ref01_sustained_v2_schema_DRAFT.sql','ref01_sustained_v2_policy_DRAFT.json','ref01_sustained_v2_rollback_test_DRAFT.sql','ref01_sustained_v2_registration_DRAFT.sql','ref01_sustained_v2_first_readback.sql']
manifest={'status':'LOCAL_DRAFT_ROOT_ACTUAL_ROLLBACK_REQUIRED','scope_sha256':policy['scope_sha256'],'external_calls':0,'production_mutations':0,'facts':'Retained-source reconciliation and source-qualified vendor reconstruction; no certified cohort, historical receipt or rank-day share assertion.','files':{f:{'sha256':hashlib.sha256((D/f).read_bytes()).hexdigest(),'bytes':(D/f).stat().st_size} for f in files}}
(D/'ref01_sustained_v2_DRAFT_manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
print(json.dumps(manifest,indent=2))
