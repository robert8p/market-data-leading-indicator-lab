-- Root executes BEGIN + reviewed implementation + this fixture. No network invocation. Ends ROLLBACK.
SET LOCAL statement_timeout='90s';SET LOCAL lock_timeout='5s';
CREATE TEMP TABLE ctx07_v6_test_receipt(receipt jsonb) ON COMMIT DROP;
DO $test$
DECLARE tid uuid;aid uuid;tok text:='ctx07-v6-synthetic-fixture';r jsonb;c jsonb;iv jsonb;fn regprocedure;h text;st text;body text;bh text;negative record;proof jsonb:='[]';before_counters jsonb;outc jsonb;BEGIN
 PERFORM pg_advisory_xact_lock(2026083000261022);
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE family_id='CTX07' AND status='LEASED') THEN RAISE EXCEPTION 'ACTIVE_CTX07';END IF;
 iv:=jsonb_build_object('scope_sha256','8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd','bundle_sha256',population_top3000_202608_202609_v1.ctx07_recovery_bundle_v6(),'observations_sha256',(SELECT encode(sha256(convert_to(string_agg(to_jsonb(o)::text,E'\n' ORDER BY observation_id),'UTF8')),'hex') FROM population_top3000_202608_202609_v1.macro_observations o),'dictionary_sha256',(SELECT encode(sha256(convert_to(string_agg(payload_sha256,E'\n' ORDER BY item_key),'UTF8')),'hex') FROM population_top3000_202608_202609_v1.catalog_dictionary WHERE family_id='CTX07' AND scope_id='corrected_company_scope_8e6ca1178f03822c'));
 INSERT INTO population_top3000_202608_202609_v1.ctx07_recovery_request_v6 SELECT 'ctx07-v6-test',source_id,source_url,raw_sha256,'BLS',iv,least(clock_timestamp()+interval '30minutes',(SELECT admitted_until FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton)),8388608 FROM population_top3000_202608_202609_v1.macro_source_registry WHERE source_id='empsit_202607';
 UPDATE population_top3000_202608_202609_v1.pipeline_control SET budget_evidence=jsonb_set(budget_evidence,'{allowed_providers}',(budget_evidence->'allowed_providers')||'["BLS"]') WHERE singleton;
 tid:=population_top3000_202608_202609_v1.pipeline_enqueue_v1('CTX07','ACQUIRE','ctx07-v6-test','{"kind":"CTX07_SOURCE_RECOVERY_V6","request_key":"ctx07-v6-test"}',iv);
 body:='<html>8:30 a.m. (ET) Friday, August 7, 2026 THE EMPLOYMENT SITUATION - JULY 2026 In July, average hourly earnings for all employees on private nonfarm payrolls, at $1.23. Both the labor force participation rate, at 2.34 percent. Both the unemployment rate, at 3.45 percent.</html>';
 bh:=encode(sha256(convert_to(body,'UTF8')),'hex');
 FOREACH st IN ARRAY ARRAY['ACQUIRE','NORMALIZE','DERIVE','VALIDATE'] LOOP
 fn:=('population_top3000_202608_202609_v1.ctx07_recovery_'||lower(st)||'_v6(uuid,text)')::regprocedure;h:=encode(sha256(convert_to(pg_get_functiondef(fn),'UTF8')),'hex');
 UPDATE population_top3000_202608_202609_v1.pipeline_adapter SET function_oid=fn,function_sha256=h WHERE family_id='CTX07' AND stage=st;
 aid:=gen_random_uuid();UPDATE population_top3000_202608_202609_v1.pipeline_task SET status='LEASED',attempt_id=aid,lease_until=clock_timestamp()+interval '90seconds',owner_fence=3,stop_generation=1 WHERE task_id=tid AND status='READY';IF NOT FOUND THEN RAISE EXCEPTION 'NO_FORCED_TASK';END IF;
 INSERT INTO population_top3000_202608_202609_v1.pipeline_attempt(attempt_id,task_id,execution_number,token_sha256,owner_fence,stop_generation,adapter_sha256) VALUES(aid,tid,1,encode(sha256(convert_to(tok,'UTF8')),'hex'),3,1,h);
 IF st='ACQUIRE' THEN
 FOR negative IN SELECT * FROM(VALUES
 ('owner','UPDATE population_top3000_202608_202609_v1.acquisition_control SET owner_id=''stale'' WHERE singleton'),
 ('stop','UPDATE population_top3000_202608_202609_v1.acquisition_control SET stop_generation=2 WHERE singleton'))n(label,command) LOOP
 BEGIN EXECUTE negative.command;PERFORM population_top3000_202608_202609_v1.ctx07_recovery_claim_v6(aid,tok);RAISE EXCEPTION 'NEGATIVE_ACCEPTED';EXCEPTION WHEN OTHERS THEN IF SQLERRM IS DISTINCT FROM 'CONTROLLER_OWNERSHIP_CHANGED' THEN RAISE;END IF;END;
 proof:=proof||jsonb_build_array(jsonb_build_object('negative',negative.label,'rejected',true));END LOOP;
 -- Rollback-only component limits include direct source calls and exact retained bytes.
 BEGIN
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES('CTX07_V6_DIRECT_SOURCE_IMPORT','{"synthetic_test_only":true,"source_http_requests":64}');
 PERFORM population_top3000_202608_202609_v1.ctx07_recovery_claim_v6(aid,tok);RAISE EXCEPTION 'REQUEST64_ACCEPTED';
 EXCEPTION WHEN raise_exception THEN IF SQLERRM IS DISTINCT FROM 'CTX07_V6_REQUEST_CAP' THEN RAISE;END IF;END;
 BEGIN
 INSERT INTO population_top3000_202608_202609_v1.ctx07_recovery_request_v6 SELECT 'ctx07-bytecap-'||i,source_id,source_url,expected_raw_sha256,provider,input_versions,not_after,max_bytes,created_at FROM population_top3000_202608_202609_v1.ctx07_recovery_request_v6 CROSS JOIN generate_series(1,8)i WHERE request_key='ctx07-v6-test';
 INSERT INTO population_top3000_202608_202609_v1.ctx07_recovery_body_v6 SELECT 'ctx07-bytecap-'||i,b,encode(sha256(b),'hex'),clock_timestamp(),'application/octet-stream',NULL FROM generate_series(1,8)i CROSS JOIN (SELECT convert_to(repeat('x',8388608),'UTF8') b)z;
 PERFORM population_top3000_202608_202609_v1.ctx07_recovery_claim_v6(aid,tok);RAISE EXCEPTION 'BYTE64_ACCEPTED';
 EXCEPTION WHEN raise_exception THEN IF SQLERRM IS DISTINCT FROM 'CTX07_V6_BYTE_ADMISSION' THEN RAISE;END IF;END;
 proof:=proof||jsonb_build_array(jsonb_build_object('request_cap_includes_direct',true,'aggregate64MiB_cap',true));
 c:=population_top3000_202608_202609_v1.ctx07_recovery_claim_v6(aid,tok);IF c->>'state'<>'RESERVED' THEN RAISE EXCEPTION 'CLAIM_NOT_RESERVED %',c;END IF;
 BEGIN
 PERFORM population_top3000_202608_202609_v1.ctx07_recovery_commit_v6(aid,tok,jsonb_build_object('request_key','ctx07-v6-test','request_url',c->>'source_url','http_status',200,'body_base64',encode(convert_to(body,'UTF8'),'base64'),'raw_sha256',repeat('0',64),'retrieved_at',clock_timestamp(),'content_type','text/html'));
 RAISE EXCEPTION 'BADHASH_ACCEPTED';
 EXCEPTION WHEN raise_exception THEN IF SQLERRM IS DISTINCT FROM 'CTX07_V6_BODY_OR_TIME_INVALID' THEN RAISE;END IF;END;
 BEGIN UPDATE population_top3000_202608_202609_v1.acquisition_control SET stop_generation=2 WHERE singleton;
 PERFORM population_top3000_202608_202609_v1.ctx07_recovery_commit_v6(aid,tok,'{}'::jsonb);RAISE EXCEPTION 'POSTFETCH_STOP_ACCEPTED';
 EXCEPTION WHEN raise_exception THEN IF SQLERRM IS DISTINCT FROM 'CONTROLLER_OWNERSHIP_CHANGED' THEN RAISE;END IF;END;
 r:=population_top3000_202608_202609_v1.ctx07_recovery_commit_v6(aid,tok,jsonb_build_object('request_key','ctx07-v6-test','request_url',c->>'source_url','http_status',200,'body_base64',encode(convert_to(body,'UTF8'),'base64'),'raw_sha256',bh,'retrieved_at',clock_timestamp(),'content_type','text/html'));
 ELSE
 EXECUTE format('select %s($1,$2)',fn::oid::regproc) INTO c USING aid,tok;
 IF st<>'VALIDATE' AND c->>'state'<>'COMPLETE' THEN RAISE EXCEPTION 'STAGE_FAILED %',c;END IF;
 IF st='NORMALIZE' THEN
 outc:=population_top3000_202608_202609_v1.ctx07_recovery_process_v6(aid,tok);IF outc#>>'{commit_receipt,new_committed_units}'<>'0' THEN RAISE EXCEPTION 'DUPLICATE_UNITS';END IF;
 END IF;
 r:=population_top3000_202608_202609_v1.pipeline_finish_v1(aid,tok,c);END IF;
 proof:=proof||jsonb_build_array(jsonb_build_object('stage',st,'finish',r));tid:=(r->>'successor_task_id')::uuid;
 END LOOP;
 -- Synthetic extraction tests independent of retained numeric fields.
 c:=population_top3000_202608_202609_v1.ctx07_empsit_extract_v6(body,'average_hourly_earnings_as_released','headline','2026-07-01T04:00Z','2026-08-07T12:30Z');IF c->>'value'<>'1.23' OR c->>'units'<>'USD/hour' THEN RAISE EXCEPTION 'NUMERIC_EXTRACTION';END IF;
 c:=population_top3000_202608_202609_v1.ctx07_empsit_extract_v6(body,'average_hourly_earnings_as_released','headline','2026-06-01T04:00Z','2026-08-07T12:30Z');IF c->>'status'<>'PUBLICATION_OR_REFERENCE_PERIOD_UNVERIFIED' THEN RAISE EXCEPTION 'WRONG_REFERENCE_ACCEPTED';END IF;
 -- Explicit synthetic as-of fixture; future release cannot replace past release.
 INSERT INTO population_top3000_202608_202609_v1.ctx07_recovery_evidence_v6(request_key,observation_id,evidence,evidence_sha256,first_attempt_id)
 SELECT 'ctx07-v6-test','synthetic-asof-'||n,e,encode(sha256(convert_to(e::text,'UTF8')),'hex'),aid FROM (SELECT n,jsonb_build_object('factor_name','synthetic-factor','status','NUMERICAL_PUBLICATION_RECONSTRUCTION_VERIFIED','publication_supported_available_at',v,'reference_period','2026-07-01','synthetic',true) e FROM (VALUES(1,'2026-08-07T12:30:00Z'),(2,'2026-09-04T12:30:00Z'))x(n,v))z;
 SELECT x INTO c FROM population_top3000_202608_202609_v1.ctx07_recovery_asof_v6('synthetic-factor','2026-08-08T00:00Z')x;
 IF c->>'publication_supported_available_at'<>'2026-08-07T12:30:00Z' THEN RAISE EXCEPTION 'FUTURE_VINTAGE_LEAK';END IF;
 INSERT INTO ctx07_v6_test_receipt VALUES(jsonb_build_object('result','PASS_CTX07_V6_SYNTHETIC_ROLLBACK','bundle_sha256',population_top3000_202608_202609_v1.ctx07_recovery_bundle_v6(),'proof',proof,'synthetic_test_only',true,'future_vintage_excluded',true,'provider_requests_sent',0,'production_changes_rolled_back',true));
END;$test$;
SELECT receipt FROM ctx07_v6_test_receipt;
ROLLBACK;
