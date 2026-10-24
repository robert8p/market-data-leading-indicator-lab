BEGIN;
SET LOCAL statement_timeout='45s';
SET LOCAL lock_timeout='5s';
CREATE TEMP TABLE daily_wrapper_proof_v2(result jsonb) ON COMMIT DROP;
DO $test$
DECLARE c population_top3000_202608_202609_v1.acquisition_control%rowtype;
 t population_top3000_202608_202609_v1.pipeline_task%rowtype;
 a uuid:=gen_random_uuid();tok text:='runtime01-derive-rollback-only';h text;r1 jsonb;r2 jsonb;replay jsonb;
 token_rejected boolean:=false;fence_rejected boolean:=false;stop_rejected boolean:=false;n bigint;
BEGIN
 PERFORM pg_advisory_xact_lock(2026083000261022);
 SELECT * INTO c FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton FOR UPDATE;
 IF c.owner_id<>'top3000-completion-20261024-root-5db48b32d103' OR c.owner_fence<>3 OR c.stop_generation<>1 OR c.stopped THEN RAISE EXCEPTION 'CONTROLLER_CHANGED';END IF;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.daily_shared_run_v1 WHERE run_id='19a35bfa-6023-4092-9809-3ed950871d7f' AND normalized_series=13044 AND normalized_at IS NOT NULL AND derived_series=0) THEN RAISE EXCEPTION 'FULL_NORMALIZATION_AND_ZERO_DERIVE_REQUIRED';END IF;
 SELECT * INTO t FROM population_top3000_202608_202609_v1.pipeline_task WHERE family_id='D03' AND stage='DERIVE' AND partition_key='retained-shared-daily-v1' FOR UPDATE;
 IF NOT FOUND OR t.status='LEASED' THEN RAISE EXCEPTION 'REVIEWABLE_DERIVE_SUCCESSOR_REQUIRED';END IF;
 h:=encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.daily_shared_derive_admitted_v2(uuid,text)'::regprocedure),'UTF8')),'hex');
 UPDATE population_top3000_202608_202609_v1.pipeline_adapter SET enabled=true,function_oid='population_top3000_202608_202609_v1.daily_shared_derive_admitted_v2(uuid,text)'::regprocedure,function_sha256=h WHERE family_id='D03' AND stage='DERIVE';
 UPDATE population_top3000_202608_202609_v1.pipeline_task SET status='LEASED',attempt_id=a,lease_until=clock_timestamp()+interval '90 seconds',owner_fence=c.owner_fence,stop_generation=c.stop_generation,executions=executions+1,payload=payload||'{"max_derived_series":25}'::jsonb WHERE task_id=t.task_id;
 INSERT INTO population_top3000_202608_202609_v1.pipeline_attempt(attempt_id,task_id,execution_number,token_sha256,owner_fence,stop_generation,adapter_sha256)
 VALUES(a,t.task_id,t.executions+1,encode(sha256(convert_to(tok,'UTF8')),'hex'),c.owner_fence,c.stop_generation,h);
 BEGIN PERFORM population_top3000_202608_202609_v1.daily_shared_derive_admitted_v2(a,'bad-token');EXCEPTION WHEN raise_exception THEN IF SQLERRM='ATTEMPT_AUTH_FAILED' THEN token_rejected:=true;ELSE RAISE;END IF;END;
 UPDATE population_top3000_202608_202609_v1.pipeline_attempt SET owner_fence=2 WHERE attempt_id=a;
 BEGIN PERFORM population_top3000_202608_202609_v1.daily_shared_derive_admitted_v2(a,tok);EXCEPTION WHEN raise_exception THEN IF SQLERRM='STALE_OWNER_OR_STOP' THEN fence_rejected:=true;ELSE RAISE;END IF;END;
 UPDATE population_top3000_202608_202609_v1.pipeline_attempt SET owner_fence=3 WHERE attempt_id=a;
 UPDATE population_top3000_202608_202609_v1.acquisition_control SET stopped=true WHERE singleton;
 BEGIN PERFORM population_top3000_202608_202609_v1.daily_shared_derive_admitted_v2(a,tok);EXCEPTION WHEN raise_exception THEN IF SQLERRM='MISSION_HELD_OR_ADMISSION_EXPIRED' THEN stop_rejected:=true;ELSE RAISE;END IF;END;
 UPDATE population_top3000_202608_202609_v1.acquisition_control SET stopped=false WHERE singleton;
 r1:=population_top3000_202608_202609_v1.daily_shared_derive_admitted_v2(a,tok);
 IF r1->>'state' IS DISTINCT FROM 'CONTINUE' OR (r1#>>'{commit_receipt,new_committed_units}')::integer IS DISTINCT FROM 1050 THEN RAISE EXCEPTION 'FIRST_BATCH_FAILED %',r1;END IF;
 PERFORM population_top3000_202608_202609_v1.pipeline_checkpoint_v1(a,tok,r1->'checkpoint',r1->'commit_receipt');
 r2:=population_top3000_202608_202609_v1.daily_shared_derive_admitted_v2(a,tok);
 replay:=population_top3000_202608_202609_v1.daily_shared_derive_admitted_v2(a,tok);
 SELECT count(*) INTO n FROM population_top3000_202608_202609_v1.daily_shared_value_v1 WHERE run_id='19a35bfa-6023-4092-9809-3ed950871d7f';
 IF r2->>'blocker_code' IS DISTINCT FROM 'DAILY_DERIVE_MEASURED_RESOURCE_REVIEW' OR replay IS DISTINCT FROM r2 OR n<>1050 OR NOT token_rejected OR NOT fence_rejected OR NOT stop_rejected THEN RAISE EXCEPTION 'BOUNDARY_OR_GUARD_FAILED';END IF;
 INSERT INTO daily_wrapper_proof_v2 VALUES(jsonb_build_object('result','PASS_ACTUAL_DERIVE25_ROLLBACK','rows',n,'original_series_denominator',13044,'wrapper_sha256',h,'first_batch',r1,'bounded_successor',r2,'idempotent_boundary_replay',true,'stale_token_rejected',token_rejected,'stale_fence_rejected',fence_rejected,'latest_stop_rejected',stop_rejected,'provider_calls',0,'production_changes_rolled_back',true));
END $test$;
SELECT result FROM daily_wrapper_proof_v2;
ROLLBACK;
