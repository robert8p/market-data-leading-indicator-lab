-- Root only: BEGIN + fullwindow_register_v6.sql + this fixture (ends ROLLBACK).
-- Fixture review/assignment records are synthetic and roll back; they are never production authority.
SET LOCAL lock_timeout='15s';SET LOCAL statement_timeout='40s';
CREATE TEMP TABLE ctx02_window_proof_v6(result jsonb);
DO $fixture$
DECLARE a population_top3000_202608_202609_v1.acquisition_control%rowtype;assignment_id bigint;review_id bigint;claim_id bigint;claim_missing_ok boolean:=false;claim_expired_ok boolean:=false;claim_overlap_ok boolean:=false;admission_expired_ok boolean:=false;h text;r jsonb;again jsonb;item jsonb;ctx jsonb;ids jsonb;
 stop_ok boolean:=false;owner_ok boolean:=false;unknown_ok boolean:=false;prior_attempt uuid;original_archive jsonb;after_archive jsonb;
BEGIN
 PERFORM pg_advisory_xact_lock(2026083000261022);
 SELECT * INTO STRICT a FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton FOR UPDATE;
 IF a.owner_id IS DISTINCT FROM 'top3000-completion-20261024-root-5db48b32d103' OR a.owner_fence<>3 OR a.stop_generation<>1 OR a.stopped IS DISTINCT FROM false OR a.enabled IS DISTINCT FROM true THEN RAISE EXCEPTION 'CTX02_WINDOW_FIXTURE_AUTHORITY_CHANGED';END IF;
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE status='LEASED') THEN RAISE EXCEPTION 'CTX02_WINDOW_FIXTURE_ACTIVE_ATTEMPT';END IF;
 SELECT to_jsonb(t) INTO original_archive FROM population_top3000_202608_202609_v1.pipeline_task t WHERE task_id='d377d980-5dbd-4276-a2c9-d5638c60b668';
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details)VALUES('ENGINEERING_IMPLEMENTATION_ASSIGNMENT_V5',jsonb_build_object('assignment_id','ctx02-fullwindow-reference-and-bulk-capability-v6','owner_id',a.owner_id,'owner_fence',a.owner_fence,'stop_generation',a.stop_generation,'scope_sha256',a.scope_sha256,'not_after',a.admitted_until,'exclusive_component','CTX02_FULLWINDOW_REFERENCE_ROUTE_V6','root_will_not_concurrently_modify',true,'assigned_executor','automation:6adc2c96db348191bca7cf4eacfdac32','authority_event_id',9528,'bounds',jsonb_build_object('provider_requests',2,'source_bytes',2097152,'initial_requests',2),'source_admission',jsonb_build_object('direct_executor_provider_calls',0),'code_commit',repeat('1',40),'fixture_only',true))RETURNING event_id INTO assignment_id;
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details)
 SELECT 'ENGINEERING_ASSIGNMENT_CLAIMED_V5',jsonb_build_object('assignment_event_id',assignment_id,'assignment_id',e.details->>'assignment_id','assignment_details_sha256',encode(sha256(convert_to(e.details::text,'UTF8')),'hex'),'exclusive_component',e.details->>'exclusive_component','automation_run_id','ROLLBACK_FIXTURE_FULLWINDOW','executor','automation:6adc2c96db348191bca7cf4eacfdac32','owner_id',a.owner_id,'owner_fence',a.owner_fence,'stop_generation',a.stop_generation,'scope_sha256',a.scope_sha256,'authority_event_id',9528,'claimed_at',clock_timestamp(),'expires_at',least(a.admitted_until,clock_timestamp()+interval '20minutes'),'fixture_only',true)
 FROM population_top3000_202608_202609_v1.pipeline_event e WHERE e.event_id=assignment_id RETURNING event_id INTO claim_id;
 h:=encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.ctx02_fullwindow_register_v6(text,bigint,bigint,bigint,bigint)'::regprocedure),'UTF8')),'hex');
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details)VALUES('CTX02_FULLWINDOW_REGISTRATION_REVIEW_V6',jsonb_build_object('approved',true,'assignment_event_id',assignment_id,'reviewer','SYNTHETIC_REVIEWER_ROLLBACK_ONLY','producer','SYNTHETIC_PRODUCER_ROLLBACK_ONLY','function_sha256',h,'claim_event_id',claim_id,'code_commit',repeat('1',40),'fixture_only',true))RETURNING event_id INTO review_id;
 BEGIN
  UPDATE population_top3000_202608_202609_v1.acquisition_control SET stop_generation=stop_generation+1 WHERE singleton;
  PERFORM population_top3000_202608_202609_v1.ctx02_fullwindow_register_v6(a.owner_id,3,1,assignment_id,review_id);RAISE EXCEPTION 'CTX02_WINDOW_STOP_NOT_REJECTED';
 EXCEPTION WHEN raise_exception THEN IF SQLERRM='CTX02_WINDOW_CONTROLLER_ADMISSION_REQUIRED' THEN stop_ok:=true;ELSE RAISE;END IF;END;
 BEGIN
  UPDATE population_top3000_202608_202609_v1.acquisition_control SET owner_id='ROLLBACK_ONLY_OTHER_OWNER' WHERE singleton;
  PERFORM population_top3000_202608_202609_v1.ctx02_fullwindow_register_v6(a.owner_id,3,1,assignment_id,review_id);RAISE EXCEPTION 'CTX02_WINDOW_OWNER_NOT_REJECTED';
 EXCEPTION WHEN raise_exception THEN IF SQLERRM='CTX02_WINDOW_CONTROLLER_ADMISSION_REQUIRED' THEN owner_ok:=true;ELSE RAISE;END IF;END;
 BEGIN
  SELECT l.attempt_id INTO prior_attempt FROM population_top3000_202608_202609_v1.pipeline_provider_lease l WHERE released_at IS NOT NULL ORDER BY released_at DESC LIMIT 1;
  IF prior_attempt IS NULL THEN RAISE EXCEPTION 'CTX02_WINDOW_FIXTURE_RELEASED_LEASE_REQUIRED';END IF;
  UPDATE population_top3000_202608_202609_v1.pipeline_provider_lease l SET released_at=NULL WHERE l.attempt_id=prior_attempt;
  PERFORM population_top3000_202608_202609_v1.ctx02_fullwindow_register_v6(a.owner_id,3,1,assignment_id,review_id);RAISE EXCEPTION 'CTX02_WINDOW_UNKNOWN_NOT_REJECTED';
 EXCEPTION WHEN raise_exception THEN IF SQLERRM='CTX02_WINDOW_PROVIDER_EFFECT_UNRESOLVED' THEN unknown_ok:=true;ELSE RAISE;END IF;END;
 BEGIN
  UPDATE population_top3000_202608_202609_v1.acquisition_control SET admitted_until=clock_timestamp()-interval '1second' WHERE singleton;
  PERFORM population_top3000_202608_202609_v1.ctx02_fullwindow_register_v6(a.owner_id,3,1,assignment_id,review_id);RAISE EXCEPTION 'CTX02_WINDOW_EXPIRED_ADMISSION_NOT_REJECTED';
 EXCEPTION WHEN raise_exception THEN IF SQLERRM='CTX02_WINDOW_CONTROLLER_ADMISSION_REQUIRED' THEN admission_expired_ok:=true;ELSE RAISE;END IF;END;
 BEGIN
  DELETE FROM population_top3000_202608_202609_v1.pipeline_event WHERE event_id=claim_id;
  PERFORM population_top3000_202608_202609_v1.ctx02_fullwindow_register_v6(a.owner_id,3,1,assignment_id,review_id);RAISE EXCEPTION 'CTX02_WINDOW_MISSING_CLAIM_NOT_REJECTED';
 EXCEPTION WHEN raise_exception THEN IF SQLERRM='CTX02_WINDOW_LIVE_EXCLUSIVE_CLAIM_REQUIRED' THEN claim_missing_ok:=true;ELSE RAISE;END IF;END;
 BEGIN
  UPDATE population_top3000_202608_202609_v1.pipeline_event SET details=jsonb_set(details,'{expires_at}',to_jsonb(clock_timestamp()-interval '1second')) WHERE event_id=claim_id;
  PERFORM population_top3000_202608_202609_v1.ctx02_fullwindow_register_v6(a.owner_id,3,1,assignment_id,review_id);RAISE EXCEPTION 'CTX02_WINDOW_EXPIRED_CLAIM_NOT_REJECTED';
 EXCEPTION WHEN raise_exception THEN IF SQLERRM='CTX02_WINDOW_LIVE_CLAIM_PINS_CHANGED' THEN claim_expired_ok:=true;ELSE RAISE;END IF;END;
 BEGIN
  INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) SELECT event,details||jsonb_build_object('assignment_event_id',0) FROM population_top3000_202608_202609_v1.pipeline_event WHERE event_id=claim_id;
  PERFORM population_top3000_202608_202609_v1.ctx02_fullwindow_register_v6(a.owner_id,3,1,assignment_id,review_id);RAISE EXCEPTION 'CTX02_WINDOW_OVERLAPPING_CLAIM_NOT_REJECTED';
 EXCEPTION WHEN raise_exception THEN IF SQLERRM='CTX02_WINDOW_LIVE_EXCLUSIVE_CLAIM_REQUIRED' THEN claim_overlap_ok:=true;ELSE RAISE;END IF;END;
 r:=population_top3000_202608_202609_v1.ctx02_fullwindow_register_v6(a.owner_id,3,1,assignment_id,review_id);
 again:=population_top3000_202608_202609_v1.ctx02_fullwindow_register_v6(a.owner_id,3,1,assignment_id,review_id);
 IF r->>'state' IS DISTINCT FROM 'REGISTERED_WAITING_FOR_SOURCE_COMMIT' OR jsonb_array_length(r->'requests')<>2 OR (SELECT jsonb_agg(value->>'task_id' ORDER BY value->>'as_of') FROM jsonb_array_elements(again->'requests')) IS DISTINCT FROM (SELECT jsonb_agg(value->>'task_id' ORDER BY value->>'as_of') FROM jsonb_array_elements(r->'requests')) THEN RAISE EXCEPTION 'CTX02_WINDOW_IDEMPOTENT_REGISTRATION_FAILED';END IF;
 FOR item IN SELECT value FROM jsonb_array_elements(r->'requests') LOOP
  ctx:=population_top3000_202608_202609_v1.ctx02_census_context_v1((item->>'task_id')::uuid);
  IF ctx->>'state' IS DISTINCT FROM 'READY' OR ctx->>'page_number' IS DISTINCT FROM '1' OR ctx->>'max_provider_calls' IS DISTINCT FROM '1' OR ctx->>'admitted_page_ceiling' IS DISTINCT FROM '1' OR ctx->>'as_of' IS DISTINCT FROM item->>'as_of' THEN RAISE EXCEPTION 'CTX02_WINDOW_NATIVE_CONTEXT_INCOMPATIBLE';END IF;
 END LOOP;
 SELECT to_jsonb(t) INTO after_archive FROM population_top3000_202608_202609_v1.pipeline_task t WHERE task_id='d377d980-5dbd-4276-a2c9-d5638c60b668';
 IF after_archive IS DISTINCT FROM original_archive OR NOT stop_ok OR NOT owner_ok OR NOT unknown_ok OR NOT admission_expired_ok OR NOT claim_missing_ok OR NOT claim_expired_ok OR NOT claim_overlap_ok THEN RAISE EXCEPTION 'CTX02_WINDOW_PRESERVATION_OR_NEGATIVE_FAILED';END IF;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.continuation_clear_queue_v6 WHERE clearing_id=r->>'external_clearing_id' AND status='BLOCKED_EXTERNAL' AND input_manifest->0->>'provider_fetches_authorized'='0') THEN RAISE EXCEPTION 'CTX02_WINDOW_CREDENTIAL_CONDITION_NOT_DURABLE';END IF;
 INSERT INTO ctx02_window_proof_v6 VALUES(jsonb_build_object('result','PASS_ACTUAL_CTX02_FULLWINDOW_REGISTRATION_ROLLBACK_V6','function_sha256',h,'registered_new_requests',2,'registered_request_dates',jsonb_build_array('2026-09-01','2026-09-30'),'native_context_ready',true,'idempotent_task_ids',true,'original_aug3_task_unchanged',true,'later_stop_rejected',stop_ok,'owner_change_rejected',owner_ok,'unknown_provider_lease_rejected',unknown_ok,'expired_admission_rejected',admission_expired_ok,'missing_claim_rejected',claim_missing_ok,'expired_claim_rejected',claim_expired_ok,'overlapping_claim_rejected',claim_overlap_ok,'actual_provider_calls',0,'useful_output_observed',false,'production_changes_rolled_back',true));
END;$fixture$;
SELECT result FROM ctx02_window_proof_v6;
ROLLBACK;
