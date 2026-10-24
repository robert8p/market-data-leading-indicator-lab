-- Root only. Executes the actual retained nonterminal per-page branch; all normalized rows/attempts/counters roll back.
SET LOCAL lock_timeout='15s';SET LOCAL statement_timeout='45s';
CREATE TEMP TABLE ctx02_processor_v6_proof(result jsonb) ON COMMIT DROP;
DO $test$
DECLARE ctl population_top3000_202608_202609_v1.acquisition_control%rowtype;
 t population_top3000_202608_202609_v1.pipeline_task%rowtype;
 q population_top3000_202608_202609_v1.ctx02_census_page_v1%rowtype;
 tid uuid;original_tid uuid;aid uuid;tok text:=repeat('7',64);fn regprocedure;stage_name text;fh text;r jsonb;again jsonb;proofs jsonb:='[]';old_inputs jsonb;quarantine_proof jsonb;
 wrong_hash boolean:=false;sample jsonb;cls jsonb;life_n integer;wrong_row boolean:=false;before_counts jsonb;after_counts jsonb;started timestamptz:=clock_timestamp();
BEGIN
 PERFORM pg_advisory_xact_lock(2026083000261022);
 SELECT * INTO STRICT ctl FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton FOR UPDATE;
 IF ctl.owner_id IS DISTINCT FROM 'top3000-completion-20261024-root-5db48b32d103' OR ctl.owner_fence IS DISTINCT FROM 3 OR ctl.stop_generation IS DISTINCT FROM 1 OR ctl.stopped IS DISTINCT FROM false OR ctl.enabled IS DISTINCT FROM true THEN RAISE EXCEPTION 'CTX02_PROCESSOR_V6_TEST_CURRENT_CONTROLLER_REQUIRED';END IF;
 IF population_top3000_202608_202609_v1.ctx02_census_bundle_v1() IS DISTINCT FROM '0a5cebaf6c91f612c8a5ec0b5c02203823f7e33cc1449db4500fbd08ab540161' THEN RAISE EXCEPTION 'CTX02_V1_BUNDLE_CHANGED';END IF;
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task pt WHERE pt.family_id='CTX02' AND pt.stage IN('NORMALIZE','DERIVE','VALIDATE') AND pt.status='LEASED') THEN RAISE EXCEPTION 'CTX02_PROCESSOR_V6_TEST_WILL_NOT_DISPLACE_ATTEMPT';END IF;
 SELECT * INTO STRICT t FROM population_top3000_202608_202609_v1.pipeline_task pt WHERE pt.family_id='CTX02' AND pt.stage='NORMALIZE' AND pt.payload->>'kind'='CTX02_CENSUS_SOURCE_V1' AND pt.payload->>'source_page_number'='165' AND pt.status IN('BLOCKED','READY') ORDER BY pt.created_at LIMIT 1;
 tid:=t.task_id;original_tid:=tid;old_inputs:=t.input_versions;
 SELECT * INTO STRICT q FROM population_top3000_202608_202609_v1.ctx02_census_page_v1 sp WHERE sp.partition_key=t.payload->>'request_key' AND sp.page_number=165;
 IF q.next_url IS NULL OR q.result_count<>1000 THEN RAISE EXCEPTION 'CTX02_REAL_NONTERMINAL_THOUSAND_ROW_PAGE_REQUIRED';END IF;
 before_counts:=jsonb_build_object('provider_requests',ctl.provider_requests,'http_invocations',ctl.http_invocations,'stored_bytes',ctl.stored_bytes,'reserved_bytes',ctl.reserved_bytes);
 SELECT (d.content_text::jsonb->'results')->514 INTO sample FROM population_top3000_202608_202609_v1.source_document d WHERE d.source_id=q.source_id;
 cls:=population_top3000_202608_202609_v1.ctx02_census_classify_record_v6(sample,'{"as_of":"2026-08-03"}');
 IF cls->>'classification' IS DISTINCT FROM 'VALID_LIFECYCLE_SUPPORTED' OR cls#>>'{payload,expiration_date}' IS DISTINCT FROM '2026-08-21' OR cls#>>'{payload,symbol_expiration_date}' IS DISTINCT FROM '2026-09-18' OR cls#>>'{payload,effective_deliverable_cash_usd}' IS DISTINCT FROM '2500' OR cls#>>'{payload,source_record}' IS NULL THEN RAISE EXCEPTION 'CTX02_V6_LIFECYCLE_FIELDS_WRONG';END IF;
 cls:=population_top3000_202608_202609_v1.ctx02_census_classify_record_v6(jsonb_set(sample,'{additional_underlyings}','[]'),'{}'::jsonb||jsonb_build_object('as_of','2026-08-03'));
 IF cls->>'classification' IS DISTINCT FROM 'QUARANTINED' THEN RAISE EXCEPTION 'CTX02_V6_UNSUPPORTED_NOT_QUARANTINED';END IF;
 cls:=population_top3000_202608_202609_v1.ctx02_census_classify_record_v6(jsonb_set(sample,'{strike_price}','0'),'{}'::jsonb||jsonb_build_object('as_of','2026-08-03'));
 IF cls->>'classification' IS DISTINCT FROM 'QUARANTINED' OR cls->'source_record' IS DISTINCT FROM jsonb_set(sample,'{strike_price}','0') THEN RAISE EXCEPTION 'CTX02_V6_OTHER_CONFLICT_FALSE_LIFECYCLE_ACCEPTANCE';END IF;
 BEGIN
 DELETE FROM population_top3000_202608_202609_v1.ctx02_lifecycle_evidence_v6 WHERE evidence_id='OCC59450_AVNS_20260727';
 cls:=population_top3000_202608_202609_v1.ctx02_census_classify_record_v6(sample,'{"as_of":"2026-08-03"}');
 IF cls->>'classification' IS DISTINCT FROM 'QUARANTINED' THEN RAISE EXCEPTION 'CTX02_V6_MISSING_EVIDENCE_ACCEPTED';END IF;
 RAISE EXCEPTION 'ROLLBACK_EXPECTED_MISSING_EVIDENCE';
 EXCEPTION WHEN raise_exception THEN IF SQLERRM<>'ROLLBACK_EXPECTED_MISSING_EVIDENCE' THEN RAISE;END IF;END;
 FOREACH stage_name IN ARRAY ARRAY['NORMALIZE','DERIVE','VALIDATE'] LOOP
  fn:=('population_top3000_202608_202609_v1.ctx02_census_'||lower(stage_name)||'_v6(uuid,text)')::regprocedure;
  fh:=encode(sha256(convert_to(pg_get_functiondef(fn),'UTF8')),'hex');
  UPDATE population_top3000_202608_202609_v1.pipeline_adapter SET function_oid=fn,function_sha256=fh,enabled=true,reviewed_test_evidence=reviewed_test_evidence||jsonb_build_object('ctx02_v6',jsonb_build_object('bundle_sha256',population_top3000_202608_202609_v1.ctx02_lifecycle_bundle_v6())) WHERE family_id='CTX02' AND stage=stage_name;
 END LOOP;
 -- Exercise the complete storage/replay/DERIVE quarantine branch against retained page165.
 -- This nested subtransaction is rolled back before the source-supported run below.
 BEGIN
  DELETE FROM population_top3000_202608_202609_v1.ctx02_lifecycle_evidence_v6 WHERE evidence_id='OCC59450_AVNS_20260727';
  SELECT * INTO STRICT t FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=original_tid;
  SELECT function_sha256 INTO fh FROM population_top3000_202608_202609_v1.pipeline_adapter WHERE family_id='CTX02' AND stage='NORMALIZE';
  aid:=gen_random_uuid();
  UPDATE population_top3000_202608_202609_v1.pipeline_task SET status='LEASED',attempt_id=aid,lease_until=clock_timestamp()+interval '90 seconds',owner_fence=ctl.owner_fence,stop_generation=ctl.stop_generation,executions=executions+1 WHERE task_id=original_tid;
  INSERT INTO population_top3000_202608_202609_v1.pipeline_attempt(attempt_id,task_id,execution_number,token_sha256,owner_fence,stop_generation,adapter_sha256)VALUES(aid,original_tid,t.executions+1,encode(sha256(convert_to(tok,'UTF8')),'hex'),ctl.owner_fence,ctl.stop_generation,fh);
  r:=population_top3000_202608_202609_v1.ctx02_census_normalize_v6(aid,tok);
  IF r->>'state' IS DISTINCT FROM 'COMPLETE' OR r#>>'{commit_receipt,page_valid_units}' IS DISTINCT FROM '980' OR r#>>'{commit_receipt,page_quarantined_units}' IS DISTINCT FROM '20' OR r#>>'{commit_receipt,source_page_records}' IS DISTINCT FROM '1000' OR r#>>'{commit_receipt,page_source_fully_normalized}' IS DISTINCT FROM 'false' THEN RAISE EXCEPTION 'CTX02_V6_FULL_QUARANTINE_PATH_WRONG %',r;END IF;
  IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.ctx02_census_page_v1 WHERE source_id=q.source_id AND normalized_at IS NULL AND normalized_source_records=980) THEN RAISE EXCEPTION 'CTX02_V6_QUARANTINED_PAGE_FALSE_COMPLETENESS';END IF;
  again:=population_top3000_202608_202609_v1.ctx02_census_normalize_v6(aid,tok);
  IF again#>>'{commit_receipt,new_committed_units}' IS DISTINCT FROM '0' OR again#>>'{commit_receipt,output_sha256}' IS DISTINCT FROM r#>>'{commit_receipt,output_sha256}' THEN RAISE EXCEPTION 'CTX02_V6_QUARANTINE_REPLAY_FAILED';END IF;
  quarantine_proof:=r->'commit_receipt';
  r:=population_top3000_202608_202609_v1.pipeline_finish_v1(aid,tok,r);tid:=(r->>'successor_task_id')::uuid;
  IF tid IS NULL THEN RAISE EXCEPTION 'CTX02_V6_QUARANTINE_DERIVE_SUCCESSOR_MISSING';END IF;
  SELECT * INTO STRICT t FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=tid;
  IF t.stage IS DISTINCT FROM 'DERIVE' OR t.input_versions IS DISTINCT FROM old_inputs THEN RAISE EXCEPTION 'CTX02_V6_QUARANTINE_SUCCESSOR_PINS_CHANGED';END IF;
  SELECT function_sha256 INTO fh FROM population_top3000_202608_202609_v1.pipeline_adapter WHERE family_id='CTX02' AND stage='DERIVE';
  aid:=gen_random_uuid();
  UPDATE population_top3000_202608_202609_v1.pipeline_task SET status='LEASED',attempt_id=aid,lease_until=clock_timestamp()+interval '90 seconds',owner_fence=ctl.owner_fence,stop_generation=ctl.stop_generation,executions=executions+1 WHERE task_id=tid;
  INSERT INTO population_top3000_202608_202609_v1.pipeline_attempt(attempt_id,task_id,execution_number,token_sha256,owner_fence,stop_generation,adapter_sha256)VALUES(aid,tid,t.executions+1,encode(sha256(convert_to(tok,'UTF8')),'hex'),ctl.owner_fence,ctl.stop_generation,fh);
  r:=population_top3000_202608_202609_v1.ctx02_census_derive_v6(aid,tok);
  IF r->>'state' IS DISTINCT FROM 'COMPLETE' OR r#>>'{commit_receipt,page_quarantined_units}' IS DISTINCT FROM '20' OR r#>>'{commit_receipt,output_sha256}' IS DISTINCT FROM quarantine_proof->>'output_sha256' THEN RAISE EXCEPTION 'CTX02_V6_QUARANTINE_DERIVE_FAILED';END IF;
  RAISE EXCEPTION 'ROLLBACK_EXPECTED_QUARANTINE_STORAGE';
 EXCEPTION WHEN raise_exception THEN IF SQLERRM<>'ROLLBACK_EXPECTED_QUARANTINE_STORAGE' THEN RAISE;END IF;END;
 tid:=original_tid;
 FOREACH stage_name IN ARRAY ARRAY['NORMALIZE','DERIVE','VALIDATE'] LOOP
  SELECT * INTO STRICT t FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=tid;
  IF t.input_versions IS DISTINCT FROM old_inputs OR t.payload->>'source_page_number' IS DISTINCT FROM '165' OR t.stage IS DISTINCT FROM stage_name THEN RAISE EXCEPTION 'CTX02_PER_PAGE_SUCCESSOR_INPUTS_CHANGED';END IF;
  SELECT function_sha256 INTO fh FROM population_top3000_202608_202609_v1.pipeline_adapter WHERE family_id='CTX02' AND stage=stage_name;
  aid:=gen_random_uuid();
  UPDATE population_top3000_202608_202609_v1.pipeline_task SET status='LEASED',attempt_id=aid,lease_until=clock_timestamp()+interval '90 seconds',owner_fence=ctl.owner_fence,stop_generation=ctl.stop_generation,executions=executions+1 WHERE task_id=tid;
  INSERT INTO population_top3000_202608_202609_v1.pipeline_attempt(attempt_id,task_id,execution_number,token_sha256,owner_fence,stop_generation,adapter_sha256)VALUES(aid,tid,t.executions+1,encode(sha256(convert_to(tok,'UTF8')),'hex'),ctl.owner_fence,ctl.stop_generation,fh);
  IF stage_name='NORMALIZE' THEN
   r:=population_top3000_202608_202609_v1.ctx02_census_normalize_v6(aid,tok);
   IF r->>'state' IS DISTINCT FROM 'COMPLETE' OR r#>>'{commit_receipt,new_committed_units}' IS DISTINCT FROM '1000' OR r#>>'{commit_receipt,page_quarantined_units}' IS DISTINCT FROM '0' THEN RAISE EXCEPTION 'CTX02_PER_PAGE_NORMALIZE_FAILED %',r;END IF;
   again:=population_top3000_202608_202609_v1.ctx02_census_normalize_v6(aid,tok);
   IF again#>>'{commit_receipt,new_committed_units}' IS DISTINCT FROM '0' OR again#>>'{commit_receipt,output_sha256}' IS DISTINCT FROM r#>>'{commit_receipt,output_sha256}' THEN RAISE EXCEPTION 'CTX02_PER_PAGE_REPLAY_FAILED';END IF;
  ELSIF stage_name='DERIVE' THEN
   BEGIN
    UPDATE population_top3000_202608_202609_v1.ctx02_census_record_v1 SET payload=jsonb_set(payload,'{strike_price}','0'),payload_sha256=encode(sha256(convert_to(jsonb_set(payload,'{strike_price}','0')::text,'UTF8')),'hex') WHERE source_id=q.source_id AND source_ordinal=1;
    PERFORM population_top3000_202608_202609_v1.ctx02_census_derive_v6(aid,tok);RAISE EXCEPTION 'CTX02_V6_TAMPER_NOT_REJECTED';
   EXCEPTION WHEN raise_exception THEN IF SQLERRM='CTX02_CENSUS_RECORD_REPLAY_OR_DERIVE_MISMATCH' THEN wrong_row:=true;ELSE RAISE;END IF;END;
   r:=population_top3000_202608_202609_v1.ctx02_census_derive_v6(aid,tok);
   IF r->>'state' IS DISTINCT FROM 'COMPLETE' THEN RAISE EXCEPTION 'CTX02_PER_PAGE_DERIVE_FAILED';END IF;
  ELSE
   r:=population_top3000_202608_202609_v1.ctx02_census_validate_v6(aid,tok);
   IF r->>'state' IS DISTINCT FROM 'BLOCKED' OR r->>'blocker_code' IS DISTINCT FROM 'CTX02_CENSUS_INDEPENDENT_POPULATION_LIFECYCLE_ACCEPTANCE_REQUIRED' THEN RAISE EXCEPTION 'CTX02_V6_PREMATURE_ACCEPTANCE';END IF;
  END IF;
  proofs:=proofs||jsonb_build_array(jsonb_build_object('stage',stage_name,'state',r->>'state','receipt',r));
  IF stage_name<>'VALIDATE' THEN r:=population_top3000_202608_202609_v1.pipeline_finish_v1(aid,tok,r);tid:=(r->>'successor_task_id')::uuid;IF tid IS NULL THEN RAISE EXCEPTION 'CTX02_V6_SUCCESSOR_MISSING';END IF;END IF;
 END LOOP;
 SELECT count(*) INTO life_n FROM population_top3000_202608_202609_v1.ctx02_census_record_v1 WHERE source_id=q.source_id AND payload->>'contract_life_state'='CASH_SETTLED_ACCELERATED_EXPIRY_SOURCE_SUPPORTED';
 IF life_n<>20 THEN RAISE EXCEPTION 'CTX02_V6_EXACT20_LIFECYCLE_UNITS_REQUIRED';END IF;
 SELECT jsonb_build_object('provider_requests',a.provider_requests,'http_invocations',a.http_invocations,'stored_bytes',a.stored_bytes,'reserved_bytes',a.reserved_bytes) INTO after_counts FROM population_top3000_202608_202609_v1.acquisition_control a WHERE singleton;
 IF before_counts IS DISTINCT FROM after_counts OR NOT wrong_row OR population_top3000_202608_202609_v1.ctx02_census_bundle_v1() IS DISTINCT FROM '0a5cebaf6c91f612c8a5ec0b5c02203823f7e33cc1449db4500fbd08ab540161' THEN RAISE EXCEPTION 'CTX02_V6_COUNTER_OR_SOURCE_BUNDLE_CHANGED';END IF;
 INSERT INTO ctx02_processor_v6_proof VALUES(jsonb_build_object('result','PASS_ACTUAL_CTX02_NONTERMINAL_PAGE_PROCESSOR_V6_ROLLBACK','lifecycle_supported_records',life_n,'unsupported_quarantined',true,'missing_evidence_quarantined',true,'second_invariant_conflict_quarantined',true,'quarantine_storage_path_verified',true,'quarantine_storage_proof',quarantine_proof,'bundle_sha256',population_top3000_202608_202609_v1.ctx02_lifecycle_bundle_v6(),'provider_http_calls',0,'retained_source_rows',1000,'source_id',q.source_id,'source_sha256',q.source_sha256,'archive_key',q.archive_key,'old_source_bundle_preserved',true,'old_source_input_versions_preserved',true,'provider_counters_unchanged',true,'replay_new_rows',0,'parsed_tamper_rejected',wrong_row,'successor_stages',proofs,'elapsed_seconds',extract(epoch FROM clock_timestamp()-started),'function_hashes',(SELECT jsonb_object_agg(pr.proname,encode(sha256(convert_to(pg_get_functiondef(pr.oid),'UTF8')),'hex')) FROM pg_proc pr JOIN pg_namespace n ON n.oid=pr.pronamespace WHERE n.nspname='population_top3000_202608_202609_v1' AND pr.proname IN('ctx02_census_process_v6','ctx02_census_classify_record_v6','ctx02_lifecycle_bundle_v6','ctx02_census_normalize_v6','ctx02_census_derive_v6','ctx02_census_validate_v6')),'production_changes_rolled_back',true));
END;$test$;
SELECT result FROM ctx02_processor_v6_proof;
ROLLBACK;
