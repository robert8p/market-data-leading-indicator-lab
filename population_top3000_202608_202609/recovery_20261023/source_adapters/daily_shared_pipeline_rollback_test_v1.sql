-- Actual retained-source test. Root installs the reviewed math/schema first.
-- No network or provider reservation. Every fixture mutation is rolled back.
BEGIN;
SET LOCAL lock_timeout='3s';
SET LOCAL statement_timeout='55s';
SELECT pg_advisory_xact_lock(2026083000261022);
CREATE TEMP TABLE daily_shared_fixture_measurements(stage text,batch_number integer,elapsed_seconds numeric,new_rows bigint,manifest_sha256 text)ON COMMIT DROP;
CREATE TEMP TABLE daily_shared_fixture_receipt(result jsonb)ON COMMIT DROP;
CREATE OR REPLACE FUNCTION pg_temp.daily_shared_fixture_lease(p_task uuid)
RETURNS jsonb LANGUAGE plpgsql SET search_path TO 'pg_catalog' AS $f$
DECLARE a uuid:=gen_random_uuid();tok text:=encode(sha256(convert_to(gen_random_uuid()::text,'UTF8')),'hex');
BEGIN
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task t
  WHERE t.task_id=p_task AND t.family_id IN('D03','D04','D05') AND t.payload->>'kind'='RETAINED_SHARED_DAILY_V1'
   AND t.payload->>'fixture_only'='true' AND t.status IN('READY','BLOCKED'))
 THEN RAISE EXCEPTION 'DAILY_SHARED_FIXTURE_LEASE_SCOPE';END IF;
 UPDATE population_top3000_202608_202609_v1.pipeline_task SET status='LEASED',attempt_id=a,
  lease_until=clock_timestamp()+interval '60 seconds',owner_fence=2,stop_generation=0,executions=executions+1 WHERE task_id=p_task;
 INSERT INTO population_top3000_202608_202609_v1.pipeline_attempt(attempt_id,task_id,execution_number,token_sha256,owner_fence,stop_generation,adapter_sha256)
 SELECT a,p_task,t.executions,encode(sha256(convert_to(tok,'UTF8')),'hex'),2,0,ad.function_sha256
 FROM population_top3000_202608_202609_v1.pipeline_task t
 JOIN population_top3000_202608_202609_v1.pipeline_adapter ad ON ad.family_id=t.family_id AND ad.stage=t.stage WHERE t.task_id=p_task;
 RETURN jsonb_build_object('attempt_id',a,'token',tok);
END $f$;
DO $test$
DECLARE tid uuid;a uuid;tok text;l jsonb;r jsonb;again jsonb;prepared jsonb;vers jsonb;rid uuid;
 runkey text:='fixture-shared-daily-v1-'||gen_random_uuid()::text;
 f text;st text;fn regprocedure;fnmap jsonb;math_hash text;bundle text;
 old_sources jsonb;old_universe jsonb;old_adapters jsonb;old_control jsonb;old_functions jsonb;
 new_sources jsonb;new_universe jsonb;new_adapters jsonb;new_control jsonb;new_functions jsonb;
 i integer;rejected boolean;batch_start timestamptz;started timestamptz:=clock_timestamp();
 bytes_before bigint;bytes_after bigint;sample jsonb;measurements jsonb;
BEGIN
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.acquisition_control c WHERE c.singleton
  AND c.owner_id='top3000-recovery-20261023-root-4b1314bcb5c2' AND c.owner_fence=2 AND c.stop_generation=0
  AND c.enabled AND NOT c.stopped AND c.hold_reason IS NULL AND c.admitted_until>clock_timestamp())
 THEN RAISE EXCEPTION 'DAILY_SHARED_FIXTURE_OWNER_OR_ADMISSION_CHANGED';END IF;
 IF EXISTS(SELECT 1 FROM cron.job WHERE jobid=596 AND active)
  OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE status='LEASED')
  OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_provider_lease WHERE released_at IS NULL)
 THEN RAISE EXCEPTION 'DAILY_SHARED_FIXTURE_REQUIRES_PAUSED_UNLEASED_QUEUE';END IF;
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_adapter WHERE family_id IN('D03','D04','D05'))
 THEN RAISE EXCEPTION 'DAILY_SHARED_EXISTING_ADAPTER_REQUIRES_RECONCILIATION';END IF;
 -- Real commit recording advances progress timestamps. Ownership, allowance,
 -- stop, source counts, spending and all other governance fields must match.
 SELECT to_jsonb(c)-'last_useful_progress_at'-'updated_at'INTO old_control FROM population_top3000_202608_202609_v1.acquisition_control c WHERE c.singleton;
 SELECT coalesce(jsonb_agg(to_jsonb(u)ORDER BY to_jsonb(u)::text),'[]')INTO old_universe FROM population_top3000_202608_202609_v1.pipeline_universe u;
 SELECT coalesce(jsonb_agg(to_jsonb(ad)ORDER BY ad.family_id,ad.stage),'[]')INTO old_adapters FROM population_top3000_202608_202609_v1.pipeline_adapter ad;
 old_sources:=population_top3000_202608_202609_v1.daily_shared_source_manifest_v1();
 old_functions:=old_sources->'reused_formula_definitions';
 SELECT coalesce(sum(pg_total_relation_size(c.oid)),0)INTO bytes_before FROM pg_class c
 WHERE c.relnamespace='population_top3000_202608_202609_v1'::regnamespace AND c.relkind='r' AND c.relname LIKE 'daily_shared_%';
 UPDATE population_top3000_202608_202609_v1.pipeline_control SET enabled=true WHERE singleton;
 FOREACH f IN ARRAY ARRAY['D03','D04','D05']LOOP
  FOREACH st IN ARRAY ARRAY['NORMALIZE','DERIVE','VALIDATE','REPAIR']LOOP
   fn:=('population_top3000_202608_202609_v1.'||CASE WHEN st='NORMALIZE'THEN 'daily_shared_normalize_v1'
    WHEN st='DERIVE'THEN 'daily_shared_derive_v1'ELSE 'daily_shared_quality_hold_v1'END||'(uuid,text)')::regprocedure;
   INSERT INTO population_top3000_202608_202609_v1.pipeline_adapter(family_id,stage,function_oid,function_sha256,resource_class,provider,enabled,reviewed_test_evidence,producer_revision,expected_batch_seconds)
   VALUES(f,st,fn::oid,encode(sha256(convert_to(pg_get_functiondef(fn),'UTF8')),'hex'),'DATABASE',NULL,true,
    '{"test":"DAILY_SHARED_RETAINED26_ROLLBACK","status":"IN_PROGRESS_ROLLBACK_ONLY"}', 'retained-shared-daily-v1',20);
  END LOOP;
 END LOOP;
 SELECT jsonb_object_agg(ad.family_id||':'||ad.stage,ad.function_sha256)INTO fnmap
 FROM population_top3000_202608_202609_v1.pipeline_adapter ad WHERE ad.family_id IN('D03','D04','D05');
 math_hash:=encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.daily_shared_math_v1(jsonb)'::regprocedure),'UTF8')),'hex');
 bundle:=population_top3000_202608_202609_v1.daily_shared_bundle_sha256_v1();
 prepared:=population_top3000_202608_202609_v1.daily_shared_prepare_run_v1(runkey,26);
 rid:=(prepared->>'run_id')::uuid;vers:=prepared->'input_versions';
 IF prepared->>'source_series'IS DISTINCT FROM '26' OR prepared->>'provider_requests'IS DISTINCT FROM '0'
 THEN RAISE EXCEPTION 'DAILY_SHARED_FIXTURE_CENSUS_PREPARATION';END IF;
 tid:=population_top3000_202608_202609_v1.pipeline_enqueue_v1('D03','NORMALIZE',runkey,
  jsonb_build_object('kind','RETAINED_SHARED_DAILY_V1','run_id',rid,'max_series',25,'fixture_only',true),vers);
 l:=pg_temp.daily_shared_fixture_lease(tid);a:=(l->>'attempt_id')::uuid;tok:=l->>'token';
 rejected:=false;
 BEGIN PERFORM population_top3000_202608_202609_v1.daily_shared_normalize_v1(a,tok||'wrong');
 EXCEPTION WHEN OTHERS THEN IF SQLERRM='ATTEMPT_AUTH_FAILED'THEN rejected:=true;ELSE RAISE;END IF;END;
 IF NOT rejected THEN RAISE EXCEPTION 'DAILY_SHARED_INVALID_TOKEN_ACCEPTED';END IF;
 rejected:=false;
 BEGIN
  UPDATE population_top3000_202608_202609_v1.pipeline_attempt SET owner_fence=1 WHERE attempt_id=a;
  PERFORM population_top3000_202608_202609_v1.daily_shared_normalize_v1(a,tok);
 EXCEPTION WHEN OTHERS THEN IF SQLERRM='STALE_OWNER_OR_STOP'THEN rejected:=true;ELSE RAISE;END IF;END;
 IF NOT rejected THEN RAISE EXCEPTION 'DAILY_SHARED_STALE_FENCE_ACCEPTED';END IF;
 rejected:=false;
 BEGIN
  UPDATE population_top3000_202608_202609_v1.pipeline_task t SET input_versions=jsonb_set(t.input_versions,'{shared_daily_bundle_sha256}',to_jsonb(repeat('0',64)))WHERE t.task_id=tid;
  PERFORM population_top3000_202608_202609_v1.daily_shared_normalize_v1(a,tok);
 EXCEPTION WHEN OTHERS THEN IF SQLERRM='DAILY_SHARED_SCOPE_OR_BUNDLE_NOT_ADMITTED'THEN rejected:=true;ELSE RAISE;END IF;END;
 IF NOT rejected THEN RAISE EXCEPTION 'DAILY_SHARED_CHANGED_BUNDLE_ACCEPTED';END IF;
 rejected:=false;
 BEGIN
  UPDATE population_top3000_202608_202609_v1.d02_daily_source_v2 d SET source_summary=d.source_summary||'{"fixture_mutation":true}'WHERE d.session_date='2026-08-03';
  PERFORM population_top3000_202608_202609_v1.daily_shared_normalize_v1(a,tok);
 EXCEPTION WHEN OTHERS THEN IF SQLERRM='DAILY_SHARED_IMMUTABLE_INPUT_CHANGED'THEN rejected:=true;ELSE RAISE;END IF;END;
 IF NOT rejected THEN RAISE EXCEPTION 'DAILY_SHARED_CHANGED_SOURCE_HEADER_ACCEPTED';END IF;
 rejected:=false;
 BEGIN
  UPDATE population_top3000_202608_202609_v1.pipeline_control p SET budget_evidence=jsonb_set(p.budget_evidence,'{resource_limits,max_relation_bytes}','1')WHERE p.singleton;
  PERFORM population_top3000_202608_202609_v1.daily_shared_normalize_v1(a,tok);
 EXCEPTION WHEN OTHERS THEN IF SQLERRM='D02_V2_MISSION_RELATION_CAP_EXCEEDED'THEN rejected:=true;ELSE RAISE;END IF;END;
 IF NOT rejected THEN RAISE EXCEPTION 'DAILY_SHARED_RESOURCE_LIMIT_IGNORED';END IF;
 batch_start:=clock_timestamp();r:=population_top3000_202608_202609_v1.daily_shared_normalize_v1(a,tok);
 IF r->>'state'IS DISTINCT FROM 'CONTINUE' OR r#>>'{commit_receipt,new_committed_units}'IS DISTINCT FROM '2575'
  OR r#>>'{checkpoint,last_series}'IS DISTINCT FROM '25'THEN RAISE EXCEPTION 'DAILY_SHARED_FIRST25_NORMALIZE_FAILED %',r;END IF;
 INSERT INTO daily_shared_fixture_measurements VALUES('NORMALIZE',1,extract(epoch FROM clock_timestamp()-batch_start),2575,r#>>'{commit_receipt,output_sha256}');
 again:=population_top3000_202608_202609_v1.daily_shared_normalize_v1(a,tok);
 IF again#>>'{commit_receipt,new_committed_units}'IS DISTINCT FROM '0'
  OR again#>>'{commit_receipt,output_sha256}'IS DISTINCT FROM r#>>'{commit_receipt,output_sha256}'
 THEN RAISE EXCEPTION 'DAILY_SHARED_INPUT_EXACT_REPLAY_FAILED';END IF;
 rejected:=false;
 BEGIN
  UPDATE population_top3000_202608_202609_v1.daily_shared_input_v1 q SET raw_close=coalesce(q.raw_close,1)+1
   WHERE q.run_id=rid AND q.series_no=1 AND q.ordinal=62;
  PERFORM population_top3000_202608_202609_v1.daily_shared_normalize_v1(a,tok);
 EXCEPTION WHEN OTHERS THEN IF SQLERRM='DAILY_SHARED_INPUT_REPLAY_CONFLICT series=1'THEN rejected:=true;ELSE RAISE;END IF;END;
 IF NOT rejected THEN RAISE EXCEPTION 'DAILY_SHARED_CHANGED_INPUT_ACCEPTED';END IF;
 PERFORM population_top3000_202608_202609_v1.pipeline_finish_v1(a,tok,r);
 -- The bounded first wave must stop before series26 and preserve its checkpoint.
 l:=pg_temp.daily_shared_fixture_lease(tid);a:=(l->>'attempt_id')::uuid;tok:=l->>'token';
 r:=population_top3000_202608_202609_v1.daily_shared_normalize_v1(a,tok);
 IF r->>'state'IS DISTINCT FROM 'BLOCKED' OR r->>'blocker_code'IS DISTINCT FROM 'DAILY_SHARED_FIRST_BATCH_RESOURCE_REVIEW'
  OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.daily_shared_input_v1 q WHERE q.run_id=rid AND q.series_no=26)
 THEN RAISE EXCEPTION 'DAILY_SHARED_FIRST_BATCH_CAP_FAILED';END IF;
 PERFORM population_top3000_202608_202609_v1.pipeline_finish_v1(a,tok,r);
 UPDATE population_top3000_202608_202609_v1.pipeline_task t SET payload=jsonb_set(t.payload,'{max_series}','26'),status='READY',blocker_code=NULL,blocker_until=NULL,resumption_condition=NULL WHERE t.task_id=tid;
 l:=pg_temp.daily_shared_fixture_lease(tid);a:=(l->>'attempt_id')::uuid;tok:=l->>'token';batch_start:=clock_timestamp();
 r:=population_top3000_202608_202609_v1.daily_shared_normalize_v1(a,tok);
 IF r->>'state'IS DISTINCT FROM 'COMPLETE' OR r#>>'{commit_receipt,new_committed_units}'IS DISTINCT FROM '103'
 THEN RAISE EXCEPTION 'DAILY_SHARED_FINAL1_NORMALIZE_FAILED %',r;END IF;
 INSERT INTO daily_shared_fixture_measurements VALUES('NORMALIZE',2,extract(epoch FROM clock_timestamp()-batch_start),103,r#>>'{commit_receipt,output_sha256}');
 PERFORM population_top3000_202608_202609_v1.pipeline_finish_v1(a,tok,r);
 FOR i IN 1..2 LOOP
  SELECT t.task_id INTO tid FROM population_top3000_202608_202609_v1.pipeline_task t WHERE t.family_id='D03' AND t.stage='DERIVE' AND t.partition_key=runkey AND t.input_versions=vers;
  l:=pg_temp.daily_shared_fixture_lease(tid);a:=(l->>'attempt_id')::uuid;tok:=l->>'token';
  IF i=1 THEN
   rejected:=false;
   BEGIN
    UPDATE population_top3000_202608_202609_v1.daily_shared_input_v1 q SET raw_close=coalesce(q.raw_close,1)+1 WHERE q.run_id=rid AND q.series_no=1 AND q.ordinal=62;
    PERFORM population_top3000_202608_202609_v1.daily_shared_derive_v1(a,tok);
   EXCEPTION WHEN OTHERS THEN IF SQLERRM='DAILY_SHARED_FROZEN_INPUT_INTEGRITY series=1'THEN rejected:=true;ELSE RAISE;END IF;END;
   IF NOT rejected THEN RAISE EXCEPTION 'DAILY_SHARED_FORGED_FROZEN_INPUT_ACCEPTED';END IF;
  END IF;
  batch_start:=clock_timestamp();r:=population_top3000_202608_202609_v1.daily_shared_derive_v1(a,tok);
  IF r->>'state'IS DISTINCT FROM(CASE WHEN i=1 THEN 'CONTINUE'ELSE 'COMPLETE'END)
   OR(r#>>'{commit_receipt,new_committed_units}')::integer<>(CASE WHEN i=1 THEN 1050 ELSE 42 END)
  THEN RAISE EXCEPTION 'DAILY_SHARED_DERIVE_BATCH_FAILED %',r;END IF;
  INSERT INTO daily_shared_fixture_measurements VALUES('DERIVE',i,extract(epoch FROM clock_timestamp()-batch_start),(r#>>'{commit_receipt,new_committed_units}')::bigint,r#>>'{commit_receipt,output_sha256}');
  again:=population_top3000_202608_202609_v1.daily_shared_derive_v1(a,tok);
  IF again#>>'{commit_receipt,new_committed_units}'IS DISTINCT FROM '0'
   OR again#>>'{commit_receipt,output_sha256}'IS DISTINCT FROM r#>>'{commit_receipt,output_sha256}'
  THEN RAISE EXCEPTION 'DAILY_SHARED_OUTPUT_EXACT_REPLAY_FAILED';END IF;
  IF i=1 THEN
   rejected:=false;
   BEGIN
    UPDATE population_top3000_202608_202609_v1.daily_shared_value_v1 q SET raw_returns[1]=coalesce(q.raw_returns[1],0)+1 WHERE q.run_id=rid AND q.series_no=1 AND q.session_date='2026-08-03';
    PERFORM population_top3000_202608_202609_v1.daily_shared_derive_v1(a,tok);
   EXCEPTION WHEN OTHERS THEN IF SQLERRM='DAILY_SHARED_OUTPUT_REPLAY_CONFLICT series=1'THEN rejected:=true;ELSE RAISE;END IF;END;
   IF NOT rejected THEN RAISE EXCEPTION 'DAILY_SHARED_CHANGED_OUTPUT_ACCEPTED';END IF;
  END IF;
  PERFORM population_top3000_202608_202609_v1.pipeline_finish_v1(a,tok,r);
 END LOOP;
 FOREACH f IN ARRAY ARRAY['D04','D05']LOOP
  FOREACH st IN ARRAY ARRAY['NORMALIZE','DERIVE']LOOP
   SELECT t.task_id INTO tid FROM population_top3000_202608_202609_v1.pipeline_task t WHERE t.family_id=f AND t.stage=st AND t.partition_key=runkey AND t.input_versions=vers;
   l:=pg_temp.daily_shared_fixture_lease(tid);a:=(l->>'attempt_id')::uuid;tok:=l->>'token';
   IF st='NORMALIZE'THEN r:=population_top3000_202608_202609_v1.daily_shared_normalize_v1(a,tok);
   ELSE r:=population_top3000_202608_202609_v1.daily_shared_derive_v1(a,tok);END IF;
   IF r->>'state'IS DISTINCT FROM 'COMPLETE' OR r#>>'{commit_receipt,new_committed_units}'IS DISTINCT FROM '0'
    OR r#>>'{commit_receipt,shared_rows_reused}'IS DISTINCT FROM '1092'
   THEN RAISE EXCEPTION 'DAILY_SHARED_DEPENDENT_FAMILY_DUPLICATED_OR_MISSING_ROWS % % %',f,st,r;END IF;
   PERFORM population_top3000_202608_202609_v1.pipeline_finish_v1(a,tok,r);
  END LOOP;
 END LOOP;
 FOREACH f IN ARRAY ARRAY['D03','D04','D05']LOOP
  SELECT t.task_id INTO tid FROM population_top3000_202608_202609_v1.pipeline_task t WHERE t.family_id=f AND t.stage='VALIDATE' AND t.partition_key=runkey AND t.input_versions=vers;
  l:=pg_temp.daily_shared_fixture_lease(tid);a:=(l->>'attempt_id')::uuid;tok:=l->>'token';r:=population_top3000_202608_202609_v1.daily_shared_quality_hold_v1(a,tok);
  IF r->>'state'IS DISTINCT FROM 'BLOCKED' OR r->>'blocker_code'IS DISTINCT FROM 'DAILY_SHARED_SECURITY_SESSION_ACTION_AND_COMPANY_QUALIFICATION_REQUIRED'
   OR r#>'{resumption_condition,quality_promoted}'IS DISTINCT FROM 'false'::jsonb
  THEN RAISE EXCEPTION 'DAILY_SHARED_FALSE_QUALITY_PROMOTION %',r;END IF;
  PERFORM population_top3000_202608_202609_v1.pipeline_finish_v1(a,tok,r);
 END LOOP;
 tid:=population_top3000_202608_202609_v1.pipeline_enqueue_v1('D03','REPAIR',runkey,
  jsonb_build_object('kind','RETAINED_SHARED_DAILY_V1','run_id',rid,'max_series',26,'fixture_only',true),vers);
 l:=pg_temp.daily_shared_fixture_lease(tid);a:=(l->>'attempt_id')::uuid;tok:=l->>'token';r:=population_top3000_202608_202609_v1.daily_shared_quality_hold_v1(a,tok);
 IF r->>'state'IS DISTINCT FROM 'BLOCKED' OR r#>'{resumption_condition,company_population_verified}'IS DISTINCT FROM 'false'::jsonb
 THEN RAISE EXCEPTION 'DAILY_SHARED_REPAIR_FALSE_PROMOTION';END IF;
 PERFORM population_top3000_202608_202609_v1.pipeline_finish_v1(a,tok,r);
 IF(SELECT count(*)FROM population_top3000_202608_202609_v1.daily_shared_input_v1 q WHERE q.run_id=rid)<>2678
  OR(SELECT count(*)FROM population_top3000_202608_202609_v1.daily_shared_value_v1 q WHERE q.run_id=rid)<>1092
  OR(SELECT count(*)FROM population_top3000_202608_202609_v1.daily_shared_series_v1 q WHERE q.run_id=rid AND q.source_ticker IN('AAPL','MSFT','NVDA')
    AND q.support_rows_present=61 AND q.target_rows_present=42 AND q.target_conflict_rows=0)<>3
 THEN RAISE EXCEPTION 'DAILY_SHARED_ACTUAL_GRID_OR_REFERENCE_SUPPORT_FAILED';END IF;
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.daily_shared_input_v1 q WHERE q.run_id=rid AND q.is_target AND(q.quality_mask&36)<>36)
  OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.daily_shared_candidates_v1 v WHERE v.run_key=runkey AND(v.publication_eligible OR v.production_asof_eligible OR v.company_population_verified OR v.historical_receipt_observed))
  OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_partition p WHERE p.partition_key=runkey AND(p.denominator_state<>'PROVISIONAL' OR p.expected_units IS NOT NULL OR p.publication_eligible))
  OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task t WHERE t.partition_key=runkey AND t.stage IN('PUBLISH','DELIVER'))
 THEN RAISE EXCEPTION 'DAILY_SHARED_PROVISIONAL_OR_MISSING_INPUT_QUALIFICATION_LOST';END IF;
 SELECT jsonb_agg(jsonb_build_object('ticker',s.source_ticker,'support_rows',s.support_rows_present,'target_rows',s.target_rows_present,
  'input_sha256',encode(s.input_sha256,'hex'),'output_sha256',encode(s.output_sha256,'hex'))ORDER BY s.source_ticker)
 INTO sample FROM population_top3000_202608_202609_v1.daily_shared_series_v1 s WHERE s.run_id=rid AND s.source_ticker IN('AAPL','MSFT','NVDA');
 new_sources:=population_top3000_202608_202609_v1.daily_shared_source_manifest_v1();new_functions:=new_sources->'reused_formula_definitions';
 SELECT to_jsonb(c)-'last_useful_progress_at'-'updated_at'INTO new_control FROM population_top3000_202608_202609_v1.acquisition_control c WHERE c.singleton;
 SELECT coalesce(jsonb_agg(to_jsonb(u)ORDER BY to_jsonb(u)::text),'[]')INTO new_universe FROM population_top3000_202608_202609_v1.pipeline_universe u;
 SELECT coalesce(jsonb_agg(to_jsonb(ad)ORDER BY ad.family_id,ad.stage),'[]')INTO new_adapters FROM population_top3000_202608_202609_v1.pipeline_adapter ad WHERE ad.family_id NOT IN('D03','D04','D05');
 IF new_sources IS DISTINCT FROM old_sources OR new_functions IS DISTINCT FROM old_functions
  OR new_control IS DISTINCT FROM old_control OR new_universe IS DISTINCT FROM old_universe OR new_adapters IS DISTINCT FROM old_adapters
 THEN RAISE EXCEPTION 'DAILY_SHARED_PRESERVED_SOURCE_CODE_UNIVERSE_OR_CONTROL_CHANGED';END IF;
 SELECT coalesce(sum(pg_total_relation_size(c.oid)),0)INTO bytes_after FROM pg_class c
 WHERE c.relnamespace='population_top3000_202608_202609_v1'::regnamespace AND c.relkind='r' AND c.relname LIKE 'daily_shared_%';
 SELECT jsonb_agg(to_jsonb(m)ORDER BY m.stage,m.batch_number)INTO measurements FROM daily_shared_fixture_measurements m;
 INSERT INTO daily_shared_fixture_receipt VALUES(jsonb_build_object('result','PASS_ACTUAL_RETAINED26_ROLLBACK',
  'shared_daily_bundle_sha256',bundle,'calculator_sha256',math_hash,'tested_function_sha256',fnmap,
  'source_manifest_sha256',vers->>'source_manifest_sha256','calendar_manifest_sha256',vers->>'calendar_manifest_sha256',
  'source_series',26,'input_grid_rows',2678,'target_output_rows',1092,'target_families',jsonb_build_array('D03','D04','D05'),
  'source_requests',0,'http_requests',0,'provider_called',false,'actual_reference_securities',sample,
  'batch_measurements',measurements,'elapsed_seconds',extract(epoch FROM clock_timestamp()-started),
  'fixture_relation_growth_bytes',bytes_after-bytes_before,'storage_measurement_includes_rollback_test_replays',true,
  'first25_cap_exercised',true,'continuation_exercised',true,'input_output_exact_replay_exercised',true,
  'forged_input_output_and_source_metadata_rejected',true,'token_fence_bundle_resource_guards_exercised',true,
  'dependent_families_reused_shared_values',true,'old_producers_sources_governance_and_universe_unchanged',true,
  'normal_commit_progress_timestamps_excluded_from_governance_comparison',true,
  'quality_hold_exercised',true,'company_population_verified',false,'provider_accuracy_verified',false,
  'historical_receipt_observed',false,'full_current_output_independent_reconciliation_pending',true));
END $test$;
SELECT result FROM daily_shared_fixture_receipt;
ROLLBACK;
