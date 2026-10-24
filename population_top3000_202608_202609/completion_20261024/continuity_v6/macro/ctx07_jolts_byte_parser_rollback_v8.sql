-- Execute BEGIN + exact implementation + this private retained-body fixture. No HTTP. ROLLBACK mandatory.
SET LOCAL statement_timeout='90s';SET LOCAL lock_timeout='5s';
CREATE TEMP TABLE jolts_v8_test_receipt(receipt jsonb) ON COMMIT DROP;
DO $test$
DECLARE z record;o record;st text;tid uuid;aid uuid;tok text:='jolts-v8-rollback-only';fn regprocedure;fh text;r jsonb;res jsonb;ev jsonb;before_counts jsonb;after_counts jsonb;proof jsonb:='[]';n integer:=0;synthetic bytea;BEGIN
 PERFORM pg_advisory_xact_lock(2026083000261022);
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE family_id='CTX07' AND stage IN('NORMALIZE','DERIVE','VALIDATE') AND status='LEASED') THEN RAISE EXCEPTION 'JOLTS_ACTIVE';END IF;
 SELECT jsonb_build_object('provider_requests',a.provider_requests,'http_invocations',a.http_invocations,'stored_bytes',a.stored_bytes,'reserved_bytes',a.reserved_bytes,'deadline',p.deadline,'admitted_until',a.admitted_until) INTO before_counts FROM population_top3000_202608_202609_v1.acquisition_control a CROSS JOIN population_top3000_202608_202609_v1.pipeline_control p WHERE a.singleton AND p.singleton;
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) SELECT 'CTX07_V8_JOLTS_PARSER_ADMISSION',jsonb_build_object('owner_id',a.owner_id,'owner_fence',3,'stop_generation',1,'scope_sha256',a.scope_sha256,'bundle_sha256',population_top3000_202608_202609_v1.ctx07_jolts_bundle_v8(),'assignment_id','ctx07-four-retained-jolts-byte-parser-v8','source_http_requests',0,'not_after',least(clock_timestamp()+interval '20minutes',a.admitted_until,p.deadline),'rollback_fixture_only',true) FROM population_top3000_202608_202609_v1.acquisition_control a CROSS JOIN population_top3000_202608_202609_v1.pipeline_control p WHERE a.singleton AND p.singleton;
 FOREACH st IN ARRAY ARRAY['NORMALIZE','DERIVE','VALIDATE'] LOOP
 fn:=('population_top3000_202608_202609_v1.ctx07_recovery_'||lower(st)||'_v8(uuid,text)')::regprocedure;fh:=encode(sha256(convert_to(pg_get_functiondef(fn),'UTF8')),'hex');
 UPDATE population_top3000_202608_202609_v1.pipeline_adapter SET function_oid=fn,function_sha256=fh WHERE family_id='CTX07' AND stage=st;
 END LOOP;
 FOR z IN SELECT * FROM population_top3000_202608_202609_v1.ctx07_jolts_scope_v8() ORDER BY source_id LOOP
 tid:=z.normalize_task_id;
 PERFORM population_top3000_202608_202609_v1.ctx07_jolts_context_v8(tid);
 BEGIN UPDATE population_top3000_202608_202609_v1.acquisition_control SET owner_id='wrong-owner' WHERE singleton;PERFORM population_top3000_202608_202609_v1.ctx07_jolts_context_v8(tid);RAISE EXCEPTION 'WRONG_OWNER_ACCEPTED';EXCEPTION WHEN raise_exception THEN IF SQLERRM<>'CTX07_V6_CURRENT_AUTHORITY_REQUIRED' THEN RAISE;END IF;END;
 BEGIN UPDATE population_top3000_202608_202609_v1.acquisition_control SET stop_generation=2 WHERE singleton;PERFORM population_top3000_202608_202609_v1.ctx07_jolts_context_v8(tid);RAISE EXCEPTION 'STOP_ACCEPTED';EXCEPTION WHEN raise_exception THEN IF SQLERRM<>'CTX07_V6_CURRENT_AUTHORITY_REQUIRED' THEN RAISE;END IF;END;
 BEGIN UPDATE population_top3000_202608_202609_v1.ctx07_recovery_request_v6 SET expected_raw_sha256=repeat('0',64) WHERE source_id=z.source_id;PERFORM population_top3000_202608_202609_v1.ctx07_jolts_context_v8(tid);RAISE EXCEPTION 'WRONG_SOURCE_ACCEPTED';EXCEPTION WHEN raise_exception THEN IF SQLERRM<>'CTX07_V6_SCOPE_SOURCE_OR_CODE_CHANGED' THEN RAISE;END IF;END;
 BEGIN UPDATE population_top3000_202608_202609_v1.pipeline_event SET details=jsonb_set(details,'{not_after}',to_jsonb('2020-01-01T00:00:00Z'::text)) WHERE event='CTX07_V8_JOLTS_PARSER_ADMISSION';PERFORM population_top3000_202608_202609_v1.ctx07_jolts_context_v8(tid);RAISE EXCEPTION 'EXPIRED_ACCEPTED';EXCEPTION WHEN raise_exception THEN IF SQLERRM<>'CTX07_V8_PARSER_ADMISSION_REQUIRED' THEN RAISE;END IF;END;
 UPDATE population_top3000_202608_202609_v1.pipeline_task SET status='READY',blocker_code=NULL,not_before=clock_timestamp() WHERE task_id=tid AND status='BLOCKED' AND blocker_code='PARSER' AND lease_until IS NULL;
 IF NOT FOUND THEN RAISE EXCEPTION 'EXACT_PARSER_TASK_NOT_BLOCKED';END IF;
 FOREACH st IN ARRAY ARRAY['NORMALIZE','DERIVE','VALIDATE'] LOOP
 fn:=('population_top3000_202608_202609_v1.ctx07_recovery_'||lower(st)||'_v8(uuid,text)')::regprocedure;fh:=encode(sha256(convert_to(pg_get_functiondef(fn),'UTF8')),'hex');
 aid:=gen_random_uuid();UPDATE population_top3000_202608_202609_v1.pipeline_task SET status='LEASED',attempt_id=aid,lease_until=clock_timestamp()+interval '90seconds',owner_fence=3,stop_generation=1 WHERE task_id=tid AND status='READY';IF NOT FOUND THEN RAISE EXCEPTION 'NO_FORCED_TASK';END IF;
 INSERT INTO population_top3000_202608_202609_v1.pipeline_attempt(attempt_id,task_id,execution_number,token_sha256,owner_fence,stop_generation,adapter_sha256) SELECT aid,tid,executions+1,encode(sha256(convert_to(tok,'UTF8')),'hex'),3,1,fh FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=tid;
 EXECUTE format('SELECT %s($1,$2)',fn::oid::regproc) INTO r USING aid,tok;
 IF st='NORMALIZE' THEN
 IF (r#>>'{commit_receipt,new_committed_units}')::integer<>z.expected_units THEN RAISE EXCEPTION 'EXACT_UNITS_REQUIRED';END IF;
 n:=n+z.expected_units;res:=population_top3000_202608_202609_v1.ctx07_jolts_process_v8(aid,tok);IF res#>>'{commit_receipt,new_committed_units}'<>'0' OR res#>>'{commit_receipt,output_sha256}' IS DISTINCT FROM r#>>'{commit_receipt,output_sha256}' THEN RAISE EXCEPTION 'REPLAY_FAILED';END IF;
 ELSIF st='DERIVE' THEN IF r#>>'{commit_receipt,new_committed_units}'<>'0' THEN RAISE EXCEPTION 'DERIVE_DUPLICATE';END IF;
 ELSE IF r->>'blocker_code'<>'CTX07_V6_INDEPENDENT_NUMERICAL_TEMPORAL_REVIEW' THEN RAISE EXCEPTION 'VALIDATE_FALSE_ACCEPTANCE';END IF;END IF;
 res:=population_top3000_202608_202609_v1.pipeline_finish_v1(aid,tok,r);
 proof:=proof||jsonb_build_array(jsonb_build_object('source_id',z.source_id,'stage',st,'task_id',tid,'finish',res,'output_sha256',r#>>'{commit_receipt,output_sha256}'));
 tid:=(res->>'successor_task_id')::uuid;
 END LOOP;
 END LOOP;
 IF n<>7 THEN RAISE EXCEPTION 'DENOMINATOR7';END IF;
 FOR o IN SELECT m.* FROM population_top3000_202608_202609_v1.macro_observations m JOIN population_top3000_202608_202609_v1.ctx07_jolts_scope_v8() s USING(source_id) LOOP
 SELECT e.evidence INTO STRICT ev FROM population_top3000_202608_202609_v1.ctx07_recovery_evidence_v6 e WHERE e.observation_id=o.observation_id;
 IF ev->>'status'<>'NUMERICAL_PUBLICATION_RECONSTRUCTION_VERIFIED' OR ev->>'strict_original_receipt_qualified'<>'false' OR ev->>'document_charset_qualified'<>'false' OR ev#>>'{extraction,matched_bytes_sha256}' IS NULL OR (ev->>'publication_supported_available_at')::timestamptz IS DISTINCT FROM o.released_at THEN RAISE EXCEPTION 'EXACT_PRIVATE_EVIDENCE_UNVERIFIED %',o.observation_id;END IF;
 SELECT x INTO r FROM population_top3000_202608_202609_v1.ctx07_jolts_asof_v8(o.reference_period,o.released_at-interval '1microsecond')x;
 IF r->>'observation_id'=o.observation_id OR (r->>'publication_supported_available_at')::timestamptz>=o.released_at THEN RAISE EXCEPTION 'FUTURE_VINTAGE_LEAK';END IF;
 SELECT x INTO r FROM population_top3000_202608_202609_v1.ctx07_jolts_asof_v8(o.reference_period,o.released_at)x;
 IF r->>'observation_id' IS DISTINCT FROM o.observation_id THEN RAISE EXCEPTION 'RELEASE_VINTAGE_SELECTION';END IF;
 END LOOP;
 synthetic:=convert_to('For release 10:00 a.m. (ET) Tuesday, June 30, 2026 JOB OPENINGS AND LABOR TURNOVER '||chr(150)||' MAY 2026 The number of job openings was unchanged at 1.2 million in May, the U.S. Bureau of Labor Statistics reported today. '||chr(149),'LATIN1');
 r:=population_top3000_202608_202609_v1.ctx07_jolts_extract_v8(synthetic,'job_openings_millions_as_released','headline','2026-05-01T04:00:00Z','2026-06-30T14:00:00Z');IF r->>'value'<>'1.2' THEN RAISE EXCEPTION 'SYNTHETIC_VALUE';END IF;
 r:=population_top3000_202608_202609_v1.ctx07_jolts_extract_v8(synthetic||convert_to(' The number of job openings was unchanged at 9.8 million in May, the U.S. Bureau of Labor Statistics reported today.','LATIN1'),'job_openings_millions_as_released','headline','2026-05-01T04:00:00Z','2026-06-30T14:00:00Z');IF r->>'status'<>'NUMERICAL_PATTERN_UNVERIFIED' OR r->>'matching_spans'<>'2' THEN RAISE EXCEPTION 'DUPLICATE_NUMERICAL_SPAN_ACCEPTED';END IF;
 r:=population_top3000_202608_202609_v1.ctx07_jolts_extract_v8(convert_to(replace(convert_from(synthetic,'LATIN1'),'1.2 million','1.2 '||chr(150)||'million'),'LATIN1'),'job_openings_millions_as_released','headline','2026-05-01T04:00:00Z','2026-06-30T14:00:00Z');IF r->>'status'='EXTRACTED' THEN RAISE EXCEPTION 'NONASCII_ANCHOR_ACCEPTED';END IF;
 r:=population_top3000_202608_202609_v1.ctx07_jolts_extract_v8(synthetic||synthetic,'job_openings_millions_as_released','headline','2026-05-01T04:00:00Z','2026-06-30T14:00:00Z');IF r->>'status'='EXTRACTED' THEN RAISE EXCEPTION 'AMBIGUITY_ACCEPTED';END IF;
 r:=population_top3000_202608_202609_v1.ctx07_jolts_extract_v8(synthetic,'job_openings_millions_as_released','headline','2026-04-01T04:00:00Z','2026-06-30T14:00:00Z');IF r->>'status'='EXTRACTED' THEN RAISE EXCEPTION 'WRONG_PERIOD_ACCEPTED';END IF;
 r:=population_top3000_202608_202609_v1.ctx07_jolts_extract_v8(synthetic,'job_openings_millions_as_released','headline','2026-05-01T04:00:00Z','2026-06-30T15:00:00Z');IF r->>'status'='EXTRACTED' THEN RAISE EXCEPTION 'WRONG_TIME_ACCEPTED';END IF;
 SELECT jsonb_build_object('provider_requests',a.provider_requests,'http_invocations',a.http_invocations,'stored_bytes',a.stored_bytes,'reserved_bytes',a.reserved_bytes,'deadline',p.deadline,'admitted_until',a.admitted_until) INTO after_counts FROM population_top3000_202608_202609_v1.acquisition_control a CROSS JOIN population_top3000_202608_202609_v1.pipeline_control p WHERE a.singleton AND p.singleton;
 IF before_counts IS DISTINCT FROM after_counts THEN RAISE EXCEPTION 'SOURCE_COUNTERS_OR_CLOCKS_CHANGED';END IF;
 INSERT INTO jolts_v8_test_receipt VALUES(jsonb_build_object('result','PASS_CTX07_V8_JOLTS_PRIVATE_ROLLBACK','bundle_sha256',population_top3000_202608_202609_v1.ctx07_jolts_bundle_v8(),'source_bodies',4,'verified_units',7,'future_vintage_excluded',true,'source_counters_and_clocks_preserved',true,'source_http_requests',0,'all_production_changes_rolled_back',true,'proof',proof));
END;$test$;
SELECT receipt FROM jolts_v8_test_receipt;
ROLLBACK;
