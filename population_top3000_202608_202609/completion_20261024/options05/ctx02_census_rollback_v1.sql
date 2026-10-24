-- Root only. Synthetic transport fixtures, never claimed as real provider HTTP.
-- All requests, provider reservations, source documents, rows, adapter changes and counters roll back.
BEGIN;
SET LOCAL lock_timeout='15s';SET LOCAL statement_timeout='45s';
CREATE TEMP TABLE ctx02_census_runtime_proof_v1(result jsonb) ON COMMIT DROP;
DO $test$
DECLARE ctl population_top3000_202608_202609_v1.acquisition_control%rowtype;
 m jsonb;v jsonb;h text;k text;tid uuid;a uuid;tok text:=repeat('e',64);fn regprocedure;fh text;t population_top3000_202608_202609_v1.pipeline_task%rowtype;
 fixture jsonb;raw text;src jsonb;c jsonb;r jsonb;r2 jsonb;sid text;archive text;normal uuid;derived uuid;validated uuid;v_stage text;
 bad_tranche boolean:=false;extended_tranche boolean:=false;bad_token boolean:=false;bad_fence boolean:=false;bad_stop boolean:=false;bad_hash boolean:=false;bad_scope boolean:=false;bad_typed boolean:=false;bad_chain boolean:=false;cursor_proved boolean:=false;bad_contract_fields boolean:=false;
 next_uri text;changed_text text;changed_hash text;pp jsonb;n integer;proofs jsonb:='[]'::jsonb;started timestamptz:=clock_timestamp();
BEGIN
 PERFORM pg_advisory_xact_lock(2026083000261022);
 SELECT * INTO ctl FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton FOR UPDATE;
 IF NOT FOUND OR ctl.owner_id IS DISTINCT FROM 'top3000-completion-20261024-root-5db48b32d103' OR ctl.owner_fence IS DISTINCT FROM 3 OR ctl.stop_generation IS DISTINCT FROM 1 OR ctl.stopped IS DISTINCT FROM false THEN RAISE EXCEPTION 'CTX02_TEST_CURRENT_CONTROLLER_REQUIRED';END IF;
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task active_task WHERE active_task.family_id='CTX02' AND active_task.stage IN('ACQUIRE','NORMALIZE','DERIVE','VALIDATE') AND active_task.status='LEASED') THEN RAISE EXCEPTION 'CTX02_TEST_WILL_NOT_DISPLACE_ACTIVE_ATTEMPT';END IF;
 fixture:='[{"ticker":"O:SYNTH260814C00001250","underlying_ticker":"SYNTH","expiration_date":"2026-08-14","contract_type":"call","strike_price":1.25,"shares_per_contract":100},{"ticker":"O:SYNTH1260918P00002000","underlying_ticker":"SYNTH","expiration_date":"2026-09-18","contract_type":"put","strike_price":2,"shares_per_contract":6,"additional_underlyings":[{"amount":3.51,"type":"cash","currency":"USD"}]}]'::jsonb;
 m:=jsonb_build_object('as_of','2026-08-03','expired',false,'population','ALL_PROVIDER_HISTORICAL_REFERENCE_NO_COHORT_FILTER','first_url','https://api.massive.com/v3/reference/options/contracts?as_of=2026-08-03&expired=false&limit=1000&sort=ticker&order=asc','max_response_bytes',1048576,'max_runtime_ms',45000,'max_pages',200,'fixture_role','ROLLBACK_ONLY_SYNTHETIC_RECORDS_NOT_PROVIDER_DATA');
 h:=encode(sha256(convert_to(m::text,'UTF8')),'hex');k:='ctx02-census-v1:'||h;
 v:=jsonb_build_object('scope_sha256',ctl.scope_sha256,'CTX02_contract_sha256','474b7aced2e7e97a4b0af5e81f264f93008967669dc4968017b4b48cc1a4b3af','ctx02_census_bundle_sha256',population_top3000_202608_202609_v1.ctx02_census_bundle_v1(),'parser_bundle_sha256',population_top3000_202608_202609_v1.ctx02_parser_bundle_v1(),'rollback_fixture_only',true);
 INSERT INTO population_top3000_202608_202609_v1.ctx02_census_request_v1(request_key,manifest,manifest_sha256,input_versions)VALUES(k,m,h,v);
 PERFORM population_top3000_202608_202609_v1.ctx02_census_admit_v1(ctl.owner_id,ctl.owner_fence,ctl.stop_generation,k,1,1,least(ctl.admitted_until,(SELECT deadline FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton),clock_timestamp()+interval '1 hour')-interval '1 second','Synthetic rollback proves final admitted request can commit and source lineage is preserved');
 FOREACH v_stage IN ARRAY ARRAY['ACQUIRE','NORMALIZE','DERIVE','VALIDATE'] LOOP
  fn:=CASE v_stage WHEN 'ACQUIRE' THEN 'population_top3000_202608_202609_v1.ctx02_census_acquire_v1(uuid,text)'::regprocedure WHEN 'NORMALIZE' THEN 'population_top3000_202608_202609_v1.ctx02_census_normalize_v1(uuid,text)'::regprocedure WHEN 'DERIVE' THEN 'population_top3000_202608_202609_v1.ctx02_census_derive_v1(uuid,text)'::regprocedure ELSE 'population_top3000_202608_202609_v1.ctx02_census_validate_v1(uuid,text)'::regprocedure END;
  fh:=encode(sha256(convert_to(pg_get_functiondef(fn),'UTF8')),'hex');
  INSERT INTO population_top3000_202608_202609_v1.pipeline_adapter(family_id,stage,function_oid,function_sha256,resource_class,provider,enabled,reviewed_test_evidence,producer_revision,expected_batch_seconds)
  VALUES('CTX02',v_stage,fn,fh,CASE WHEN v_stage='ACQUIRE' THEN 'PROVIDER' ELSE 'DATABASE' END,CASE WHEN v_stage='ACQUIRE' THEN 'Massive' ELSE NULL END,true,'{"test_only":true}'::jsonb,'rollback-only',45)
  ON CONFLICT(family_id,stage) DO UPDATE SET function_oid=excluded.function_oid,function_sha256=excluded.function_sha256,enabled=true;

 END LOOP;
 tid:=population_top3000_202608_202609_v1.pipeline_enqueue_v1('CTX02','ACQUIRE',k,jsonb_build_object('kind','CTX02_CENSUS_SOURCE_V1','request_key',k,'request_manifest_sha256',h),v,NULL);
 FOREACH v_stage IN ARRAY ARRAY['ACQUIRE','NORMALIZE','DERIVE','VALIDATE'] LOOP
  SELECT * INTO STRICT t FROM population_top3000_202608_202609_v1.pipeline_task pt WHERE pt.task_id=tid;
  IF t.stage IS DISTINCT FROM v_stage OR t.status='LEASED' THEN RAISE EXCEPTION 'CTX02_SUCCESSOR_OR_LEASE_MISMATCH';END IF;
  SELECT function_sha256 INTO fh FROM population_top3000_202608_202609_v1.pipeline_adapter pa WHERE family_id='CTX02' AND pa.stage=v_stage;
  a:=gen_random_uuid();
  UPDATE population_top3000_202608_202609_v1.pipeline_task SET status='LEASED',attempt_id=a,lease_until=clock_timestamp()+interval '90 seconds',owner_fence=ctl.owner_fence,stop_generation=ctl.stop_generation,executions=executions+1 WHERE task_id=tid;
  INSERT INTO population_top3000_202608_202609_v1.pipeline_attempt(attempt_id,task_id,execution_number,token_sha256,owner_fence,stop_generation,adapter_sha256)VALUES(a,tid,t.executions+1,encode(sha256(convert_to(tok,'UTF8')),'hex'),ctl.owner_fence,ctl.stop_generation,fh);
  IF v_stage='ACQUIRE' THEN
   BEGIN PERFORM population_top3000_202608_202609_v1.ctx02_census_claim_v1(a,'wrong');RAISE EXCEPTION 'TEST_BAD_TOKEN_NOT_REJECTED';EXCEPTION WHEN raise_exception THEN IF SQLERRM='ATTEMPT_AUTH_FAILED' THEN bad_token:=true;ELSE RAISE;END IF;END;
   BEGIN UPDATE population_top3000_202608_202609_v1.pipeline_attempt SET owner_fence=2 WHERE attempt_id=a;PERFORM population_top3000_202608_202609_v1.ctx02_census_claim_v1(a,tok);RAISE EXCEPTION 'TEST_STALE_FENCE_NOT_REJECTED';EXCEPTION WHEN raise_exception THEN IF SQLERRM='STALE_OWNER_OR_STOP' THEN bad_fence:=true;ELSE RAISE;END IF;END;
   BEGIN UPDATE population_top3000_202608_202609_v1.acquisition_control SET stopped=true WHERE singleton;PERFORM population_top3000_202608_202609_v1.ctx02_census_claim_v1(a,tok);RAISE EXCEPTION 'TEST_STOP_NOT_REJECTED';EXCEPTION WHEN raise_exception THEN IF SQLERRM='MISSION_HELD_OR_ADMISSION_EXPIRED' THEN bad_stop:=true;ELSE RAISE;END IF;END;
   BEGIN UPDATE population_top3000_202608_202609_v1.pipeline_task SET input_versions=jsonb_set(input_versions,'{ctx02_census_bundle_sha256}',to_jsonb(repeat('0',64))) WHERE task_id=tid;PERFORM population_top3000_202608_202609_v1.ctx02_census_claim_v1(a,tok);RAISE EXCEPTION 'TEST_BUNDLE_NOT_REJECTED';EXCEPTION WHEN raise_exception THEN IF SQLERRM='CTX02_CENSUS_SCOPE_OR_CODE_NOT_ADMITTED' THEN bad_scope:=true;ELSE RAISE;END IF;END;
   c:=population_top3000_202608_202609_v1.ctx02_census_claim_v1(a,tok);
   IF c->>'state' IS DISTINCT FROM 'RESERVED' THEN RAISE EXCEPTION 'CTX02_TEST_PROVIDER_SLOT_BUSY %',c;END IF;
   BEGIN PERFORM population_top3000_202608_202609_v1.ctx02_census_parse_record_v1(jsonb_set(fixture->0,'{strike_price}','0.5'),c);RAISE EXCEPTION 'TEST_CONTRACT_FIELD_CONFLICT_NOT_REJECTED';EXCEPTION WHEN raise_exception THEN IF SQLERRM='CTX02_CONTRACT_SYMBOL_FIELDS_CONFLICT' THEN bad_contract_fields:=true;ELSE RAISE;END IF;END;
   archive:=c->>'archive_key';sid:=archive||':page:1';
   raw:=jsonb_build_object('status','OK','results',fixture,'request_id','ROLLBACK_SYNTHETIC_FIXTURE_NOT_HTTP')::text;
   src:=jsonb_build_object('archive_key',archive,'request_manifest_sha256',h,'page_number',1,'request_url',c->>'request_url','next_url',NULL,'content_text',raw,'source_sha256',encode(sha256(convert_to(raw,'UTF8')),'hex'),'wire_sha256',encode(sha256(convert_to(raw,'UTF8')),'hex'),'wire_bytes',octet_length(raw),'http_status',200,'retrieved_at',clock_timestamp(),'result_count',2,'provider_request_id','ROLLBACK_FIXTURE','elapsed_ms',1);
   BEGIN PERFORM population_top3000_202608_202609_v1.ctx02_census_commit_v1(a,tok,jsonb_set(src,'{source_sha256}',to_jsonb(repeat('0',64))));RAISE EXCEPTION 'TEST_HASH_NOT_REJECTED';EXCEPTION WHEN raise_exception THEN IF SQLERRM='CTX02_CENSUS_STORED_HASH_MISMATCH' THEN bad_hash:=true;ELSE RAISE;END IF;END;
   r:=population_top3000_202608_202609_v1.ctx02_census_commit_v1(a,tok,src);
   IF r->>'state' IS DISTINCT FROM 'COMPLETE' THEN RAISE EXCEPTION 'CTX02_CENSUS_ACQUIRE_COMMIT_FAILED %',r;END IF;
   IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task p WHERE p.family_id='CTX02' AND p.stage='NORMALIZE' AND p.partition_key=k||':page:1' AND p.payload->>'source_page_number'='1' AND p.status='READY') THEN RAISE EXCEPTION 'CTX02_PAGE_NORMALIZATION_SUCCESSOR_MISSING';END IF;
   -- Exercise continuation context against a truthful reconstructed test page, then roll it back.
   BEGIN
    next_uri:='https://api.massive.com/v3/reference/options/contracts?cursor=ROLLBACK_ONLY_CURSOR';
    changed_text:=jsonb_set(raw::jsonb,'{next_url}',to_jsonb(next_uri))::text;changed_hash:=encode(sha256(convert_to(changed_text,'UTF8')),'hex');
    UPDATE population_top3000_202608_202609_v1.source_document SET content_text=changed_text,source_sha256=changed_hash,content_bytes=octet_length(changed_text) WHERE source_id=sid;
    UPDATE population_top3000_202608_202609_v1.ctx02_census_page_v1 SET next_url=next_uri,source_sha256=changed_hash WHERE archive_key=archive AND page_number=1;
    UPDATE population_top3000_202608_202609_v1.pipeline_task SET checkpoint=jsonb_build_object('next_page',2,'last_source_id',sid,'last_source_sha256',changed_hash) WHERE task_id=tid;
    pp:=population_top3000_202608_202609_v1.ctx02_census_context_v1(tid);
    IF pp->>'request_url' IS DISTINCT FROM next_uri OR pp->>'page_number' IS DISTINCT FROM '2' THEN RAISE EXCEPTION 'CTX02_CURSOR_CONTINUATION_FAILED';END IF;
    IF pp->>'state' IS DISTINCT FROM 'PAGE_LIMIT_REVIEW' THEN RAISE EXCEPTION 'CTX02_TRANCHE_SECOND_PAGE_NOT_BLOCKED';END IF;
    bad_tranche:=true;
    PERFORM population_top3000_202608_202609_v1.ctx02_census_admit_v1(ctl.owner_id,ctl.owner_fence,ctl.stop_generation,k,2,2,least(ctl.admitted_until,(SELECT deadline FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton),clock_timestamp()+interval '1 hour')-interval '1 second','Synthetic rollback proves finite extension preserves source archive and cursor');
    pp:=population_top3000_202608_202609_v1.ctx02_census_context_v1(tid);
    IF pp->>'state' IS DISTINCT FROM 'READY' OR pp->>'archive_key' IS DISTINCT FROM archive OR pp->>'request_url' IS DISTINCT FROM next_uri
    OR pp->>'request_manifest_sha256' IS DISTINCT FROM h
    OR (SELECT provider_requests FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton) IS DISTINCT FROM ctl.provider_requests+1
    THEN RAISE EXCEPTION 'CTX02_TRANCHE_EXTENSION_CHANGED_ARCHIVE_CURSOR_OR_COUNTERS';END IF;
    extended_tranche:=true;
    RAISE EXCEPTION 'ROLLBACK_CURSOR_PROOF';
   EXCEPTION WHEN raise_exception THEN IF SQLERRM='ROLLBACK_CURSOR_PROOF' THEN cursor_proved:=true;ELSE RAISE;END IF;END;
  ELSE
   IF v_stage='NORMALIZE' THEN
    BEGIN UPDATE population_top3000_202608_202609_v1.ctx02_census_page_v1 SET next_url='https://api.massive.com/v3/reference/options/contracts?cursor=NOT_TERMINAL' WHERE archive_key=archive AND page_number=1;PERFORM population_top3000_202608_202609_v1.ctx02_census_process_v1(a,tok);RAISE EXCEPTION 'TEST_NONTERMINAL_CHAIN_NOT_REJECTED';EXCEPTION WHEN raise_exception THEN IF SQLERRM='CTX02_CENSUS_PAGE_CHAIN_NOT_TERMINAL_OR_CONTIGUOUS' THEN bad_chain:=true;ELSE RAISE;END IF;END;
   ELSIF v_stage='DERIVE' THEN
    BEGIN
     UPDATE population_top3000_202608_202609_v1.ctx02_census_record_v1 SET payload=jsonb_set(payload,'{strike_price}','0'),payload_sha256=encode(sha256(convert_to(jsonb_set(payload,'{strike_price}','0')::text,'UTF8')),'hex') WHERE source_id=sid AND source_ordinal=1;
     PERFORM population_top3000_202608_202609_v1.ctx02_census_process_v1(a,tok);RAISE EXCEPTION 'TEST_PARSED_TAMPER_NOT_REJECTED';
    EXCEPTION WHEN raise_exception THEN IF SQLERRM='CTX02_CENSUS_RECORD_REPLAY_OR_DERIVE_MISMATCH' THEN bad_typed:=true;ELSE RAISE;END IF;END;
   END IF;
   r:=population_top3000_202608_202609_v1.ctx02_census_process_v1(a,tok);
   IF v_stage='VALIDATE' THEN
    IF r->>'state' IS DISTINCT FROM 'BLOCKED' OR r->>'blocker_code' IS DISTINCT FROM 'CTX02_CENSUS_INDEPENDENT_POPULATION_LIFECYCLE_ACCEPTANCE_REQUIRED' THEN RAISE EXCEPTION 'CTX02_PREMATURE_SOURCE_ACCEPTANCE';END IF;
   ELSE
    IF r->>'state' IS DISTINCT FROM 'COMPLETE' THEN RAISE EXCEPTION 'CTX02_PROCESS_FAILED %',r;END IF;
    IF v_stage='NORMALIZE' THEN
     IF r#>>'{commit_receipt,new_committed_units}' IS DISTINCT FROM '2' THEN RAISE EXCEPTION 'CTX02_NORMALIZE_TWO_RECORDS_REQUIRED';END IF;
     r2:=population_top3000_202608_202609_v1.ctx02_census_process_v1(a,tok);
     IF r2#>>'{commit_receipt,new_committed_units}' IS DISTINCT FROM '0' OR r2#>>'{commit_receipt,output_sha256}' IS DISTINCT FROM r#>>'{commit_receipt,output_sha256}' THEN RAISE EXCEPTION 'CTX02_REPLAY_FAILED';END IF;
    END IF;
    r:=population_top3000_202608_202609_v1.pipeline_finish_v1(a,tok,r);
   END IF;
  END IF;
  proofs:=proofs||jsonb_build_array(jsonb_build_object('stage',v_stage,'state',r->>'state','receipt',r));
  IF v_stage<>'VALIDATE' THEN tid:=(r->>'successor_task_id')::uuid;IF tid IS NULL THEN RAISE EXCEPTION 'CTX02_SUCCESSOR_NOT_REGISTERED';END IF;END IF;
 END LOOP;
 SELECT count(*) INTO n FROM population_top3000_202608_202609_v1.ctx02_census_record_v1 WHERE source_id=sid;
 IF n<>2 OR NOT(bad_token AND bad_fence AND bad_stop AND bad_hash AND bad_scope AND bad_typed AND bad_chain AND cursor_proved AND bad_contract_fields AND bad_tranche AND extended_tranche) THEN RAISE EXCEPTION 'CTX02_CENSUS_FINAL_PROOF_INCOMPLETE';END IF;
 INSERT INTO ctx02_census_runtime_proof_v1 VALUES(jsonb_build_object('result','PASS_ACTUAL_CTX02_CENSUS_FUNCTIONS_ROLLBACK','provider_http_calls',0,'bundle_sha256',population_top3000_202608_202609_v1.ctx02_census_bundle_v1(),'parser_bundle_sha256',population_top3000_202608_202609_v1.ctx02_parser_bundle_v1(),'function_hashes',(SELECT jsonb_object_agg(p.proname,encode(sha256(convert_to(pg_get_functiondef(p.oid),'UTF8')),'hex')) FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='population_top3000_202608_202609_v1' AND p.proname LIKE 'ctx02_census_%_v1'),'fixture_role','SYNTHETIC_RECORDS_TRANSPORT_FIXTURE_NOT_FRESH_HTTP','records',n,'stages',proofs,'bad_token_rejected',bad_token,'stale_fence_rejected',bad_fence,'later_stop_rejected',bad_stop,'source_hash_rejected',bad_hash,'helper_bundle_tamper_rejected',bad_scope,'parsed_tamper_rejected',bad_typed,'nonterminal_chain_rejected',bad_chain,'next_cursor_continuation_verified',cursor_proved,'contract_fields_rejected',bad_contract_fields,'elapsed_seconds',extract(epoch from clock_timestamp()-started),'finite_tranche_blocked',bad_tranche,'tranche_extended_same_archive_cursor',extended_tranche,'production_changes_rolled_back',true));
END; $test$;
SELECT result FROM ctx02_census_runtime_proof_v1;
ROLLBACK;
