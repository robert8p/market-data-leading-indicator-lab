-- Root only. Executes the actual retained nonterminal per-page branch; all normalized rows/attempts/counters roll back.
BEGIN;
SET LOCAL lock_timeout='15s';SET LOCAL statement_timeout='45s';
CREATE TEMP TABLE ctx02_processor_v2_proof(result jsonb) ON COMMIT DROP;
DO $test$
DECLARE ctl population_top3000_202608_202609_v1.acquisition_control%rowtype;
 t population_top3000_202608_202609_v1.pipeline_task%rowtype;
 q population_top3000_202608_202609_v1.ctx02_census_page_v1%rowtype;
 tid uuid;aid uuid;tok text:=repeat('7',64);fn regprocedure;stage_name text;fh text;r jsonb;again jsonb;proofs jsonb:='[]';old_inputs jsonb;
 wrong_hash boolean:=false;wrong_row boolean:=false;before_counts jsonb;after_counts jsonb;started timestamptz:=clock_timestamp();
BEGIN
 PERFORM pg_advisory_xact_lock(2026083000261022);
 SELECT * INTO STRICT ctl FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton FOR UPDATE;
 IF ctl.owner_id IS DISTINCT FROM 'top3000-completion-20261024-root-5db48b32d103' OR ctl.owner_fence IS DISTINCT FROM 3 OR ctl.stop_generation IS DISTINCT FROM 1 OR ctl.stopped IS DISTINCT FROM false OR ctl.enabled IS DISTINCT FROM true THEN RAISE EXCEPTION 'CTX02_PROCESSOR_V2_TEST_CURRENT_CONTROLLER_REQUIRED';END IF;
 IF population_top3000_202608_202609_v1.ctx02_census_bundle_v1() IS DISTINCT FROM '0a5cebaf6c91f612c8a5ec0b5c02203823f7e33cc1449db4500fbd08ab540161' THEN RAISE EXCEPTION 'CTX02_V1_BUNDLE_CHANGED';END IF;
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task pt WHERE pt.family_id='CTX02' AND pt.stage IN('NORMALIZE','DERIVE','VALIDATE') AND pt.status='LEASED') THEN RAISE EXCEPTION 'CTX02_PROCESSOR_V2_TEST_WILL_NOT_DISPLACE_ATTEMPT';END IF;
 SELECT * INTO STRICT t FROM population_top3000_202608_202609_v1.pipeline_task pt WHERE pt.family_id='CTX02' AND pt.stage='NORMALIZE' AND pt.payload->>'kind'='CTX02_CENSUS_SOURCE_V1' AND pt.payload->>'source_page_number'='1' AND pt.status IN('BLOCKED','READY') ORDER BY pt.created_at LIMIT 1;
 tid:=t.task_id;old_inputs:=t.input_versions;
 SELECT * INTO STRICT q FROM population_top3000_202608_202609_v1.ctx02_census_page_v1 sp WHERE sp.partition_key=t.payload->>'request_key' AND sp.page_number=1;
 IF q.next_url IS NULL OR q.result_count<>1000 THEN RAISE EXCEPTION 'CTX02_REAL_NONTERMINAL_THOUSAND_ROW_PAGE_REQUIRED';END IF;
 before_counts:=jsonb_build_object('provider_requests',ctl.provider_requests,'http_invocations',ctl.http_invocations,'stored_bytes',ctl.stored_bytes,'reserved_bytes',ctl.reserved_bytes);
 FOREACH stage_name IN ARRAY ARRAY['NORMALIZE','DERIVE','VALIDATE'] LOOP
  fn:=('population_top3000_202608_202609_v1.ctx02_census_'||lower(stage_name)||'_v2(uuid,text)')::regprocedure;
  fh:=encode(sha256(convert_to(pg_get_functiondef(fn),'UTF8')),'hex');
  UPDATE population_top3000_202608_202609_v1.pipeline_adapter SET function_oid=fn,function_sha256=fh,enabled=true WHERE family_id='CTX02' AND stage=stage_name;
 END LOOP;
 FOREACH stage_name IN ARRAY ARRAY['NORMALIZE','DERIVE','VALIDATE'] LOOP
  SELECT * INTO STRICT t FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=tid;
  IF t.input_versions IS DISTINCT FROM old_inputs OR t.payload->>'source_page_number' IS DISTINCT FROM '1' OR t.stage IS DISTINCT FROM stage_name THEN RAISE EXCEPTION 'CTX02_PER_PAGE_SUCCESSOR_INPUTS_CHANGED';END IF;
  SELECT function_sha256 INTO fh FROM population_top3000_202608_202609_v1.pipeline_adapter WHERE family_id='CTX02' AND stage=stage_name;
  aid:=gen_random_uuid();
  UPDATE population_top3000_202608_202609_v1.pipeline_task SET status='LEASED',attempt_id=aid,lease_until=clock_timestamp()+interval '90 seconds',owner_fence=ctl.owner_fence,stop_generation=ctl.stop_generation,executions=executions+1 WHERE task_id=tid;
  INSERT INTO population_top3000_202608_202609_v1.pipeline_attempt(attempt_id,task_id,execution_number,token_sha256,owner_fence,stop_generation,adapter_sha256)VALUES(aid,tid,t.executions+1,encode(sha256(convert_to(tok,'UTF8')),'hex'),ctl.owner_fence,ctl.stop_generation,fh);
  IF stage_name='NORMALIZE' THEN
   r:=population_top3000_202608_202609_v1.ctx02_census_normalize_v2(aid,tok);
   IF r->>'state' IS DISTINCT FROM 'COMPLETE' OR r#>>'{commit_receipt,new_committed_units}' IS DISTINCT FROM '1000' OR r#>>'{commit_receipt,processing_function_sha256}' IS DISTINCT FROM '06f74300bb742f2e7a23d98e6099225b851cf4917a05d4d8a5115c50f6788e41' THEN RAISE EXCEPTION 'CTX02_PER_PAGE_NORMALIZE_FAILED %',r;END IF;
   again:=population_top3000_202608_202609_v1.ctx02_census_normalize_v2(aid,tok);
   IF again#>>'{commit_receipt,new_committed_units}' IS DISTINCT FROM '0' OR again#>>'{commit_receipt,output_sha256}' IS DISTINCT FROM r#>>'{commit_receipt,output_sha256}' THEN RAISE EXCEPTION 'CTX02_PER_PAGE_REPLAY_FAILED';END IF;
  ELSIF stage_name='DERIVE' THEN
   BEGIN
    UPDATE population_top3000_202608_202609_v1.ctx02_census_record_v1 SET payload=jsonb_set(payload,'{strike_price}','0'),payload_sha256=encode(sha256(convert_to(jsonb_set(payload,'{strike_price}','0')::text,'UTF8')),'hex') WHERE source_id=q.source_id AND source_ordinal=1;
    PERFORM population_top3000_202608_202609_v1.ctx02_census_derive_v2(aid,tok);RAISE EXCEPTION 'CTX02_V2_TAMPER_NOT_REJECTED';
   EXCEPTION WHEN raise_exception THEN IF SQLERRM='CTX02_CENSUS_RECORD_REPLAY_OR_DERIVE_MISMATCH' THEN wrong_row:=true;ELSE RAISE;END IF;END;
   r:=population_top3000_202608_202609_v1.ctx02_census_derive_v2(aid,tok);
   IF r->>'state' IS DISTINCT FROM 'COMPLETE' THEN RAISE EXCEPTION 'CTX02_PER_PAGE_DERIVE_FAILED';END IF;
  ELSE
   r:=population_top3000_202608_202609_v1.ctx02_census_validate_v2(aid,tok);
   IF r->>'state' IS DISTINCT FROM 'BLOCKED' OR r->>'blocker_code' IS DISTINCT FROM 'CTX02_CENSUS_INDEPENDENT_POPULATION_LIFECYCLE_ACCEPTANCE_REQUIRED' THEN RAISE EXCEPTION 'CTX02_V2_PREMATURE_ACCEPTANCE';END IF;
  END IF;
  proofs:=proofs||jsonb_build_array(jsonb_build_object('stage',stage_name,'state',r->>'state','receipt',r));
  IF stage_name<>'VALIDATE' THEN r:=population_top3000_202608_202609_v1.pipeline_finish_v1(aid,tok,r);tid:=(r->>'successor_task_id')::uuid;IF tid IS NULL THEN RAISE EXCEPTION 'CTX02_V2_SUCCESSOR_MISSING';END IF;END IF;
 END LOOP;
 SELECT jsonb_build_object('provider_requests',a.provider_requests,'http_invocations',a.http_invocations,'stored_bytes',a.stored_bytes,'reserved_bytes',a.reserved_bytes) INTO after_counts FROM population_top3000_202608_202609_v1.acquisition_control a WHERE singleton;
 IF before_counts IS DISTINCT FROM after_counts OR NOT wrong_row OR population_top3000_202608_202609_v1.ctx02_census_bundle_v1() IS DISTINCT FROM '0a5cebaf6c91f612c8a5ec0b5c02203823f7e33cc1449db4500fbd08ab540161' THEN RAISE EXCEPTION 'CTX02_V2_COUNTER_OR_SOURCE_BUNDLE_CHANGED';END IF;
 INSERT INTO ctx02_processor_v2_proof VALUES(jsonb_build_object('result','PASS_ACTUAL_CTX02_NONTERMINAL_PAGE_PROCESSOR_V2_ROLLBACK','provider_http_calls',0,'retained_source_rows',1000,'source_id',q.source_id,'source_sha256',q.source_sha256,'archive_key',q.archive_key,'old_source_bundle_preserved',true,'old_source_input_versions_preserved',true,'provider_counters_unchanged',true,'replay_new_rows',0,'parsed_tamper_rejected',wrong_row,'successor_stages',proofs,'elapsed_seconds',extract(epoch FROM clock_timestamp()-started),'function_hashes',(SELECT jsonb_object_agg(pr.proname,encode(sha256(convert_to(pg_get_functiondef(pr.oid),'UTF8')),'hex')) FROM pg_proc pr JOIN pg_namespace n ON n.oid=pr.pronamespace WHERE n.nspname='population_top3000_202608_202609_v1' AND pr.proname IN('ctx02_census_process_v2','ctx02_census_normalize_v2','ctx02_census_derive_v2','ctx02_census_validate_v2')),'production_changes_rolled_back',true));
END;$test$;
SELECT result FROM ctx02_processor_v2_proof;
ROLLBACK;
