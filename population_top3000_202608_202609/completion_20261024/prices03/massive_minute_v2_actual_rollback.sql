-- ROOT ONLY. Synthetic time-shifted fixture from preserved AAPL raw archive; all writes/counters rolled back.
BEGIN;
SET LOCAL statement_timeout='45s'; SET LOCAL lock_timeout='15s';
CREATE TEMP TABLE minute_v2_proof(result jsonb) ON COMMIT DROP;
DO $test$
DECLARE ctl population_top3000_202608_202609_v1.acquisition_control%rowtype;
 st text;fn text;h text;bundle text;payload jsonb;versions jsonb;tid uuid;aid uuid;tok text:='minute-v2-rollback-fixture';
 claim jsonb;r jsonb;r2 jsonb;body jsonb;txt text;sourcehash text;page jsonb;original_manifest text;n integer;
 token_bad boolean:=false;symbol_bad boolean:=false;url_bad boolean:=false;input_bad boolean:=false;key_bad boolean:=false;source_bad boolean:=false;numeric_exact boolean:=false;pageid bigint;
 results jsonb:='[]';before_control jsonb;after_control jsonb;
BEGIN
 PERFORM pg_advisory_xact_lock(2026083000261022);
 SELECT * INTO ctl FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton FOR UPDATE;
 IF NOT FOUND OR ctl.owner_id IS DISTINCT FROM 'top3000-completion-20261024-root-5db48b32d103' OR ctl.owner_fence IS DISTINCT FROM 3 OR ctl.stop_generation IS DISTINCT FROM 1 OR ctl.stopped IS DISTINCT FROM false THEN RAISE EXCEPTION 'CONTROLLER_CHANGED';END IF;
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE family_id='P01' AND status='LEASED') THEN RAISE EXCEPTION 'MINUTE_V2_WILL_NOT_DISPLACE_ACTIVE_P01';END IF;
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_provider_lease WHERE released_at IS NULL) OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.connector_probe_lease_v1 WHERE released_at IS NULL) THEN RAISE EXCEPTION 'MINUTE_V2_WAIT_SHARED_PROVIDER_SLOT';END IF;
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE partition_key='massive-minute-GOOG-20260909-v2') THEN RAISE EXCEPTION 'MINUTE_V2_FIXTURE_REQUIRES_UNADMITTED_PARTITION';END IF;
 SELECT encode(sha256(convert_to(jsonb_agg(to_jsonb(x) ORDER BY page_id)::text,'UTF8')),'hex') INTO original_manifest FROM population_top3000_202608_202609_v1.massive_minute_page_v1 x;
 bundle:=population_top3000_202608_202609_v1.massive_minute_bundle_sha256_v2();
 payload:=jsonb_build_object('kind','MASSIVE_MINUTE_KNOWN5_V2','source_symbol','GOOG','instrument_key',5083,'trading_date','2026-09-09','max_pages',2,'provider_accuracy_verified',false);
 versions:=jsonb_build_object('scope_sha256',ctl.scope_sha256,'source_contract_revision','massive-minute-known5-20260909-v2','massive_minute_bundle_sha256',bundle);
 -- Exact known v1 source integrity is checked before the explicitly synthetic fixture transformation.
 body:=population_top3000_202608_202609_v1.massive_minute_read_source_v1(4);
 SELECT jsonb_set(jsonb_set(body,'{ticker}','"GOOG"'),'{results}',jsonb_agg(jsonb_set(x,'{t}',to_jsonb((x->>'t')::numeric+86400000)) ORDER BY ord)) INTO body
 FROM jsonb_array_elements(body->'results') WITH ORDINALITY z(x,ord);
 body:=jsonb_set(body-'next_url','{results,0,vw}','202.123456789012345678901234567890123456789'::jsonb);txt:=body::text;sourcehash:=encode(sha256(convert_to(txt,'UTF8')),'hex');
 IF jsonb_array_length(body->'results')<>888 THEN RAISE EXCEPTION 'MINUTE_V2_KNOWN888_FIXTURE_REQUIRED';END IF;
 FOREACH st IN ARRAY ARRAY['ACQUIRE','NORMALIZE','DERIVE','VALIDATE'] LOOP
  fn:='population_top3000_202608_202609_v1.massive_minute_route_'||lower(st)||'_v2(uuid,text)';
  h:=encode(sha256(convert_to(pg_get_functiondef(fn::regprocedure),'UTF8')),'hex');
  INSERT INTO population_top3000_202608_202609_v1.pipeline_adapter(family_id,stage,function_oid,function_sha256,resource_class,enabled,reviewed_test_evidence,producer_revision,expected_batch_seconds)
  VALUES('P01',st,fn::regprocedure,h,CASE WHEN st='ACQUIRE' THEN 'PROVIDER' ELSE 'DATABASE' END,true,'{"mode":"ROLLBACK_SYNTHETIC_ONLY"}','massive-minute-known5-20260909-v2',45)
  ON CONFLICT(family_id,stage) DO UPDATE SET function_oid=excluded.function_oid,function_sha256=excluded.function_sha256,resource_class=excluded.resource_class,provider=NULL,enabled=true,producer_revision=excluded.producer_revision;
 END LOOP;
 tid:=population_top3000_202608_202609_v1.pipeline_enqueue_v1('P01','ACQUIRE','massive-minute-GOOG-20260909-v2',payload,versions,NULL);
 FOREACH st IN ARRAY ARRAY['ACQUIRE','NORMALIZE','DERIVE','VALIDATE'] LOOP
  aid:=gen_random_uuid();SELECT function_sha256 INTO h FROM population_top3000_202608_202609_v1.pipeline_adapter WHERE family_id='P01' AND stage=st;
  UPDATE population_top3000_202608_202609_v1.pipeline_task SET status='LEASED',attempt_id=aid,lease_until=clock_timestamp()+interval '90 seconds',owner_fence=ctl.owner_fence,stop_generation=ctl.stop_generation,executions=1 WHERE task_id=tid;
  INSERT INTO population_top3000_202608_202609_v1.pipeline_attempt(attempt_id,task_id,execution_number,token_sha256,owner_fence,stop_generation,adapter_sha256) VALUES(aid,tid,1,encode(sha256(convert_to(tok,'UTF8')),'hex'),ctl.owner_fence,ctl.stop_generation,h);
  IF st='ACQUIRE' THEN
   BEGIN PERFORM population_top3000_202608_202609_v1.massive_minute_claim_v2(aid,'bad');EXCEPTION WHEN raise_exception THEN IF SQLERRM='ATTEMPT_AUTH_FAILED' THEN token_bad:=true;ELSE RAISE;END IF;END;
   BEGIN UPDATE population_top3000_202608_202609_v1.pipeline_task pt SET input_versions=jsonb_set(pt.input_versions,'{massive_minute_bundle_sha256}',to_jsonb(repeat('f',64))) WHERE task_id=tid;PERFORM population_top3000_202608_202609_v1.massive_minute_context_v2(tid);EXCEPTION WHEN raise_exception THEN IF SQLERRM='MASSIVE_MINUTE_SCOPE_OR_BUNDLE_NOT_ADMITTED' THEN input_bad:=true;ELSE RAISE;END IF;END;
   BEGIN UPDATE population_top3000_202608_202609_v1.pipeline_task pt SET payload=jsonb_set(pt.payload,'{instrument_key}','8372') WHERE task_id=tid;PERFORM population_top3000_202608_202609_v1.massive_minute_context_v2(tid);EXCEPTION WHEN raise_exception THEN IF SQLERRM='MASSIVE_MINUTE_KNOWN_INSTRUMENT_REQUIRED' THEN key_bad:=true;ELSE RAISE;END IF;END;
   claim:=population_top3000_202608_202609_v1.massive_minute_claim_v2(aid,tok);
   IF claim->>'state' IS DISTINCT FROM 'RESERVED' THEN RAISE EXCEPTION 'MINUTE_V2_PROVIDER_SLOT_NOT_RESERVED %',claim->>'state';END IF;
   page:=jsonb_build_object('archive_key',claim->>'archive_key','page_number',1,'source_symbol','GOOG','trading_date','2026-09-09','window_start_ms',1788926400000,'window_end_exclusive_ms',1789012800000,'request_url',claim->>'request_url','http_status',200,'content_text',txt,'source_sha256',sourcehash,'wire_sha256',sourcehash,'wire_bytes',octet_length(txt),'result_count',888,'results_presence','ARRAY','next_url',NULL,'retrieved_at',clock_timestamp(),'elapsed_ms',0,'provider_request_id','SYNTHETIC_ROLLBACK_FIXTURE');
   BEGIN PERFORM population_top3000_202608_202609_v1.massive_minute_commit_v2(aid,tok,jsonb_set(page,'{source_symbol}','"NVDA"'));EXCEPTION WHEN raise_exception THEN IF SQLERRM='MASSIVE_MINUTE_COMMIT_SCOPE_MISMATCH' THEN symbol_bad:=true;ELSE RAISE;END IF;END;
   BEGIN PERFORM population_top3000_202608_202609_v1.massive_minute_url_v2(replace(claim->>'request_url','/GOOG/','/NVDA/'),true,'GOOG');EXCEPTION WHEN raise_exception THEN IF SQLERRM='MASSIVE_MINUTE_UNADMITTED_URL' THEN url_bad:=true;ELSE RAISE;END IF;END;
   r:=population_top3000_202608_202609_v1.massive_minute_commit_v2(aid,tok,page);
   SELECT mp.page_id INTO pageid FROM population_top3000_202608_202609_v1.massive_minute_page_v2 mp WHERE mp.archive_key=claim->>'archive_key' AND mp.page_number=1;
   BEGIN
    UPDATE population_top3000_202608_202609_v1.source_document sd SET content_text=sd.content_text||' ',content_bytes=sd.content_bytes+1,source_sha256=encode(sha256(convert_to(sd.content_text||' ','UTF8')),'hex') WHERE sd.source_id=(claim->>'archive_key')||':page:1';
    PERFORM population_top3000_202608_202609_v1.massive_minute_read_source_v2(pageid);
   EXCEPTION WHEN raise_exception THEN IF SQLERRM='MASSIVE_MINUTE_RETAINED_SOURCE_INTEGRITY' THEN source_bad:=true;ELSE RAISE;END IF;END;
  ELSIF st='NORMALIZE' THEN
   r:=population_top3000_202608_202609_v1.massive_minute_normalize_v2(aid,tok);
   r2:=population_top3000_202608_202609_v1.massive_minute_normalize_v2(aid,tok);
   IF r#>>'{commit_receipt,new_committed_units}' IS DISTINCT FROM '888' OR r2#>>'{commit_receipt,new_committed_units}' IS DISTINCT FROM '0' THEN RAISE EXCEPTION 'MINUTE_V2_REPLAY_NOT_EXACT';END IF;
   SELECT provider_vwap=202.123456789012345678901234567890123456789::numeric INTO numeric_exact FROM population_top3000_202608_202609_v1.massive_minute_bar_v2 mb WHERE mb.page_id=pageid AND mb.source_row=1;
   IF numeric_exact IS DISTINCT FROM true THEN RAISE EXCEPTION 'MINUTE_V2_NUMERIC_LEXEME_NOT_EXACT';END IF;
   r:=population_top3000_202608_202609_v1.pipeline_finish_v1(aid,tok,r);
  ELSIF st='DERIVE' THEN
   r:=population_top3000_202608_202609_v1.massive_minute_derive_v2(aid,tok);
   IF r#>>'{commit_receipt,new_committed_units}' IS DISTINCT FROM '888' OR r#>>'{commit_receipt,source_chain_verified}' IS DISTINCT FROM 'true' THEN RAISE EXCEPTION 'MINUTE_V2_TYPED_RECONCILIATION_FAILED';END IF;
   r:=population_top3000_202608_202609_v1.pipeline_finish_v1(aid,tok,r);
  ELSE
   r:=population_top3000_202608_202609_v1.massive_minute_quality_hold_v2(aid,tok);
   IF r->>'state' IS DISTINCT FROM 'BLOCKED' THEN RAISE EXCEPTION 'MINUTE_V2_ACCEPTANCE_HOLD_REQUIRED';END IF;
   r:=population_top3000_202608_202609_v1.pipeline_finish_v1(aid,tok,r);
  END IF;
  results:=results||jsonb_build_array(jsonb_build_object('stage',st,'result',r));
  IF st<>'VALIDATE' THEN tid:=(r->>'successor_task_id')::uuid;IF tid IS NULL THEN RAISE EXCEPTION 'MINUTE_V2_SUCCESSOR_REQUIRED';END IF;END IF;
 END LOOP;
 SELECT count(*) INTO n FROM population_top3000_202608_202609_v1.massive_minute_session_candidate_v2 WHERE source_symbol='GOOG' AND source_date='2026-09-09' AND source_sha256=sourcehash AND NOT publication_eligible;
 IF n<>888 OR NOT(token_bad AND symbol_bad AND url_bad AND input_bad AND key_bad AND source_bad AND numeric_exact) THEN RAISE EXCEPTION 'MINUTE_V2_SHAPE_OR_GUARD_FAILURE';END IF;
 IF original_manifest IS DISTINCT FROM (SELECT encode(sha256(convert_to(jsonb_agg(to_jsonb(x) ORDER BY page_id)::text,'UTF8')),'hex') FROM population_top3000_202608_202609_v1.massive_minute_page_v1 x) THEN RAISE EXCEPTION 'MINUTE_V2_V1_ARCHIVE_CHANGED';END IF;
 INSERT INTO minute_v2_proof VALUES(jsonb_build_object('result','PASS_MASSIVE_MINUTE_V2_ACTUAL_ROLLBACK','bundle_sha256',bundle,'fixture_source_rows',888,'candidate_view_rows',n,'stage_results',results,'wrong_token_rejected',token_bad,'cross_symbol_rejected',symbol_bad,'cross_symbol_url_rejected',url_bad,'input_tamper_rejected',input_bad,'instrument_key_mismatch_rejected',key_bad,'retained_source_mutation_rejected',source_bad,'numeric_lexeme_exact',numeric_exact,'typed_normalize_replay_new_units',0,'original_v1_archive_unchanged',true,'actual_provider_requests',0,'synthetic_fixture_only',true,'all_changes_rolled_back',true));
END;
$test$;
SELECT result FROM minute_v2_proof;
ROLLBACK;
