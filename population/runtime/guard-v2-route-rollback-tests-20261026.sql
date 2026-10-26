-- ROLLBACK-ONLY fixtures. Begin transaction and install candidates, run tests, read route_test_result, then ROLLBACK.
DO $test$
DECLARE tid uuid:='3dcfc9d4-98b6-4322-ab62-a994eff2ee96'; aid uuid:=gen_random_uuid(); tok text:=gen_random_uuid()::text; h text;before_hash text;after_hash text;r jsonb;checks int:=0;
BEGIN
SELECT encode(sha256(convert_to(string_agg(record_sha256,E'\n' ORDER BY unit_key),'UTF8')),'hex') INTO before_hash FROM population_top3000_202608_202609_v1.source_observation WHERE family_id='N01';
h:=encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.source_guarded_stage_v2(uuid,text)'::regprocedure),'UTF8')),'hex');
UPDATE population_top3000_202608_202609_v1.pipeline_adapter SET function_oid='population_top3000_202608_202609_v1.source_guarded_stage_v2(uuid,text)'::regprocedure,function_sha256=h WHERE family_id='N01' AND stage='DERIVE';
INSERT INTO population_top3000_202608_202609_v1.pipeline_attempt(attempt_id,task_id,execution_number,token_sha256,owner_fence,stop_generation,adapter_sha256) VALUES(aid,tid,999999,encode(sha256(convert_to(tok,'UTF8')),'hex'),3,1,h);
UPDATE population_top3000_202608_202609_v1.pipeline_task SET status='LEASED',attempt_id=aid,lease_until=clock_timestamp()+interval '2 minutes',owner_fence=3,stop_generation=1,payload=payload||'{"execution_route":"SOURCE_GUARDED_STAGE_V2_20261026"}'::jsonb WHERE task_id=tid;
r:=population_top3000_202608_202609_v1.source_guarded_stage_v2(aid,tok);
IF r->>'state'<>'COMPLETE' OR r->>'execution_route'<>'SOURCE_GUARDED_STAGE_V2_20261026' THEN RAISE EXCEPTION 'ROUTE_POSITIVE_FAILED';END IF;checks:=checks+1;
SELECT encode(sha256(convert_to(string_agg(record_sha256,E'\n' ORDER BY unit_key),'UTF8')),'hex') INTO after_hash FROM population_top3000_202608_202609_v1.source_observation WHERE family_id='N01';
IF before_hash IS DISTINCT FROM after_hash THEN RAISE EXCEPTION 'LEGACY_OBSERVATIONS_CHANGED';END IF;checks:=checks+1;
BEGIN
UPDATE population_top3000_202608_202609_v1.pipeline_task SET payload=payload-'execution_route' WHERE task_id=tid;
PERFORM population_top3000_202608_202609_v1.source_guarded_stage_v2(aid,tok);
RAISE EXCEPTION 'MISSING_ROUTE_ACCEPTED';
EXCEPTION WHEN raise_exception THEN IF SQLERRM<>'GUARDED_V2_EXPLICIT_ROUTE_BINDING_REQUIRED' THEN RAISE;END IF;checks:=checks+1;END;
BEGIN
UPDATE population_top3000_202608_202609_v1.pipeline_task SET lease_until=clock_timestamp()-interval '1 second' WHERE task_id=tid;
PERFORM population_top3000_202608_202609_v1.source_guarded_stage_v2(aid,tok);
RAISE EXCEPTION 'EXPIRED_LEASE_ACCEPTED';
EXCEPTION WHEN raise_exception THEN IF SQLERRM<>'LEASE_LOST' THEN RAISE;END IF;checks:=checks+1;END;
BEGIN
UPDATE population_top3000_202608_202609_v1.acquisition_control SET stopped=true WHERE singleton;
PERFORM population_top3000_202608_202609_v1.source_guarded_stage_v2(aid,tok);
RAISE EXCEPTION 'STOP_ACCEPTED';
EXCEPTION WHEN raise_exception THEN IF SQLERRM<>'MISSION_HELD_OR_ADMISSION_EXPIRED' THEN RAISE;END IF;checks:=checks+1;END;
CREATE TEMP TABLE route_test_result(proof jsonb) ON COMMIT DROP;
INSERT INTO route_test_result VALUES(jsonb_build_object('actual_tests',checks,'all_pass',true,'existing_observation_hash_unchanged',before_hash,'caller_result',r,'qualification','CALLER_COMPATIBILITY_ONLY_NOT_NEW_DATA_OR_FAMILY_ACCEPTANCE'));
END $test$;