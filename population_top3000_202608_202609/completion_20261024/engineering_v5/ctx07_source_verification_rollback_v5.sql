-- Execute BEGIN + reviewed implementation SQL + this harness; this file ends ROLLBACK.
SET LOCAL statement_timeout='90s'; SET LOCAL lock_timeout='5s';
CREATE TEMP TABLE ctx07_actual_test_receipt(result jsonb) ON COMMIT DROP;
DO $test$
DECLARE tid uuid;aid uuid;tok text:='ctx07-fixture-only';st text;fn regprocedure;h text;iv jsonb;r jsonb;r2 jsonb;done jsonb;neg record;rejected boolean;proof jsonb:='[]';
BEGIN
 PERFORM pg_advisory_xact_lock(2026083000261022);
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE family_id='CTX07' AND status='LEASED') THEN RAISE EXCEPTION 'LIVE_CTX07_ATTEMPT';END IF;
 iv:=jsonb_build_object('scope_sha256','8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd','assignment_event_id',14379,'calendar_sha256','b7efa4d83ac8ee9adcd5cdefe6e3dba6b3cff9c5ea0cca1e55a794ee69e11f1a',
 'helper_sha256',encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.ctx07_source_verification_stage_v5(uuid,text)'::regprocedure),'UTF8')),'hex'),
 'projection_sha256',encode(sha256(convert_to(pg_get_viewdef('population_top3000_202608_202609_v1.ctx07_source_verification_projection_v5'::regclass,true),'UTF8')),'hex'));
 tid:=population_top3000_202608_202609_v1.pipeline_enqueue_v1('CTX07','ACQUIRE','ctx07-retained-numerical-vintage-evidence-v5','{"kind":"CTX07_SOURCE_VERIFICATION_V5"}',iv);
 FOREACH st IN ARRAY ARRAY['ACQUIRE','NORMALIZE','DERIVE','VALIDATE'] LOOP
 fn:=('population_top3000_202608_202609_v1.ctx07_source_verification_'||lower(st)||'_v5(uuid,text)')::regprocedure;
 h:=encode(sha256(convert_to(pg_get_functiondef(fn),'UTF8')),'hex');
 UPDATE population_top3000_202608_202609_v1.pipeline_adapter SET function_oid=fn,function_sha256=h WHERE family_id='CTX07' AND stage=st;
 aid:=gen_random_uuid();
 UPDATE population_top3000_202608_202609_v1.pipeline_task SET status='LEASED',attempt_id=aid,lease_until=clock_timestamp()+interval '90seconds',owner_fence=3,stop_generation=1 WHERE task_id=tid AND status='READY';
 IF NOT FOUND THEN RAISE EXCEPTION 'TEST_WILL_NOT_FORCE_TASK';END IF;
 INSERT INTO population_top3000_202608_202609_v1.pipeline_attempt(attempt_id,task_id,execution_number,token_sha256,owner_fence,stop_generation,adapter_sha256) VALUES(aid,tid,1,encode(sha256(convert_to(tok,'UTF8')),'hex'),3,1,h);
 IF st='ACQUIRE' THEN
 FOR neg IN SELECT * FROM (VALUES
 ('owner',$q$UPDATE population_top3000_202608_202609_v1.acquisition_control SET owner_id='stale-fixture' WHERE singleton$q$,'CONTROLLER_OWNERSHIP_CHANGED'),
 ('fence',$q$UPDATE population_top3000_202608_202609_v1.pipeline_attempt SET owner_fence=2 WHERE attempt_id=$q$||quote_literal(aid),'STALE_OWNER_OR_STOP'),
 ('stop',$q$UPDATE population_top3000_202608_202609_v1.acquisition_control SET stop_generation=2 WHERE singleton$q$,'CONTROLLER_OWNERSHIP_CHANGED'),
 ('disabled',$q$UPDATE population_top3000_202608_202609_v1.pipeline_control SET enabled=false WHERE singleton$q$,'MISSION_HELD_OR_ADMISSION_EXPIRED'),
 ('input_pin',$q$UPDATE population_top3000_202608_202609_v1.pipeline_task SET input_versions=jsonb_set(input_versions,'{calendar_sha256}','"tamper"') WHERE task_id=$q$||quote_literal(tid),'CTX07_INPUT_CODE_PIN'),
 ('source',$q$UPDATE population_top3000_202608_202609_v1.macro_observations SET source_content_sha256=repeat('0',64) WHERE observation_id=(SELECT min(observation_id) FROM population_top3000_202608_202609_v1.macro_observations)$q$,'CTX07_SOURCE_PREIMAGE_CHANGED'),
 ('storage',$q$UPDATE population_top3000_202608_202609_v1.pipeline_control SET budget_evidence=jsonb_set(budget_evidence,'{resource_limits,max_relation_bytes}','1') WHERE singleton$q$,'D02_V2_MISSION_RELATION_CAP_EXCEEDED')
 ) x(label,mutation,expected) LOOP
 rejected:=false;
 BEGIN EXECUTE neg.mutation;PERFORM population_top3000_202608_202609_v1.ctx07_source_verification_stage_v5(aid,tok);RAISE EXCEPTION 'NEGATIVE_NOT_REJECTED';
 EXCEPTION WHEN raise_exception THEN IF SQLERRM=neg.expected THEN rejected:=true;ELSE RAISE;END IF;END;
 IF NOT rejected THEN RAISE EXCEPTION 'NEGATIVE_FAILED %',neg.label;END IF;
 proof:=proof||jsonb_build_array(jsonb_build_object('negative',neg.label,'rejected',true));
 END LOOP;
 END IF;
 EXECUTE format('SELECT %s($1,$2)',fn::text::regprocedure::oid::regproc) INTO r USING aid,tok;
 IF st<>'VALIDATE' THEN
 IF r->>'state'<>'COMPLETE' THEN RAISE EXCEPTION 'NOT_COMPLETE %',r;END IF;
 r2:=population_top3000_202608_202609_v1.ctx07_source_verification_stage_v5(aid,tok);
 IF r2#>>'{commit_receipt,new_committed_units}'<>'0' THEN RAISE EXCEPTION 'REPLAY_CREATED_UNITS';END IF;
 IF st='DERIVE' THEN
 BEGIN UPDATE population_top3000_202608_202609_v1.ctx07_source_verification_unit_v5 SET evidence=jsonb_set(evidence,'{retained_numeric}','999'),evidence_sha256=encode(sha256(convert_to(jsonb_set(evidence,'{retained_numeric}','999')::text,'UTF8')),'hex') WHERE unit_key=(SELECT min(unit_key) FROM population_top3000_202608_202609_v1.ctx07_source_verification_unit_v5);
 PERFORM population_top3000_202608_202609_v1.ctx07_source_verification_stage_v5(aid,tok);RAISE EXCEPTION 'NUMERIC_TAMPER_ACCEPTED';
 EXCEPTION WHEN raise_exception THEN IF SQLERRM<>'CTX07_NUMERIC_PROJECTION_CHANGED' THEN RAISE;END IF;END;
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.ctx07_source_verification_unit_v5 WHERE evidence->>'asof_eligible'<>'false') THEN RAISE EXCEPTION 'FUTURE_REVISION_ELIGIBLE';END IF;
 END IF;
 ELSE IF r->>'state'<>'BLOCKED' THEN RAISE EXCEPTION 'VALIDATION_MUST_HOLD';END IF;END IF;
 done:=population_top3000_202608_202609_v1.pipeline_finish_v1(aid,tok,r);
 proof:=proof||jsonb_build_array(jsonb_build_object('stage',st,'receipt',r,'finish',done));
 tid:=(done->>'successor_task_id')::uuid;
 END LOOP;
 INSERT INTO ctx07_actual_test_receipt VALUES(jsonb_build_object('result','PASS_CTX07_ACTUAL_ROLLBACK','proof',proof,'units',(SELECT count(*) FROM population_top3000_202608_202609_v1.ctx07_source_verification_unit_v5),'projection_sha256',iv->>'projection_sha256','helper_sha256',iv->>'helper_sha256','adapter_hashes',(SELECT jsonb_object_agg(stage,function_sha256) FROM population_top3000_202608_202609_v1.pipeline_adapter WHERE family_id='CTX07' AND stage IN('ACQUIRE','NORMALIZE','DERIVE','VALIDATE')),'provider_calls',0,'production_changes_rolled_back',true));
END;$test$;
SELECT result FROM ctx07_actual_test_receipt;
ROLLBACK;
