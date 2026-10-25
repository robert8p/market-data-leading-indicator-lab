-- UNINTEGRATED PREPARATION: rollback could not obtain natural quiet on 2026-10-25.
-- DO NOT EXECUTE without a current exact claim, successful PostgreSQL rollback, and distinct review.
-- Slot-13 dispatcher only. It cannot admit capacity and requires an exact current review
-- binding the deployed v4 worker, all v4 SQL functions, and the live CTX02 handler/bounds.
CREATE FUNCTION population_top3000_202608_202609_v1.resource_probe_dispatch_v4(
 p_owner text,p_fence bigint,p_stop bigint,p_clearing_id text,p_review_event_id bigint)
RETURNS jsonb LANGUAGE plpgsql SET search_path=pg_catalog AS $f$
DECLARE ctl jsonb;
 a population_top3000_202608_202609_v1.acquisition_control%rowtype;
 p population_top3000_202608_202609_v1.pipeline_control%rowtype;
 h population_top3000_202608_202609_v1.continuation_handler_v6%rowtype;
 id uuid:=gen_random_uuid();token text:=replace(gen_random_uuid()::text,'-','')||replace(gen_random_uuid()::text,'-','');
 rid bigint;manifest jsonb;handler_bounds_sha text;
BEGIN
 ctl:=population_top3000_202608_202609_v1.continuation_guard_v6(p_owner,p_fence,p_stop);
 SELECT * INTO a FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton;
 SELECT * INTO p FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton;
 SELECT * INTO STRICT h FROM population_top3000_202608_202609_v1.continuation_handler_v6 WHERE kind='CTX02_PAGE_ADMISSION';
 handler_bounds_sha:=encode(sha256(convert_to(h.bounds::text,'UTF8')),'hex');
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.continuation_clear_queue_v6
  WHERE clearing_id=p_clearing_id AND family_id='RESOURCE' AND status='CLAIMED'
  AND owner_id=p_owner AND owner_fence=p_fence AND stop_generation=p_stop AND claim_expires_at>clock_timestamp())
 THEN RAISE EXCEPTION 'RESOURCE_V4_CURRENT_CLEARING_CLAIM_REQUIRED';END IF;
 IF a.admitted_until<=clock_timestamp()+interval '4minutes' OR p.deadline<=clock_timestamp()+interval '4minutes'
 OR a.http_invocations>=a.max_http_invocations OR population_top3000_202608_202609_v1.pipeline_cost_admission_v2(p,1) IS DISTINCT FROM true
 THEN RAISE EXCEPTION 'RESOURCE_V4_FINITE_HTTP_ADMISSION_REQUIRED';END IF;
 IF (SELECT count(*) FROM population_top3000_202608_202609_v1.resource_probe_v1)<>3
 OR (SELECT count(*) FROM population_top3000_202608_202609_v1.resource_probe_v2 WHERE probe_attempt=4 AND state='SUCCEEDED')<>1
 OR (SELECT count(*) FROM population_top3000_202608_202609_v1.resource_probe_v3 WHERE probe_attempt BETWEEN 5 AND 12 AND state='SUCCEEDED')<>8
 OR (SELECT count(*) FROM population_top3000_202608_202609_v1.resource_probe_v3)<>8
 THEN RAISE EXCEPTION 'RESOURCE_V4_IMMUTABLE_PRIOR_HISTORY_REQUIRED';END IF;
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.resource_probe_v4)
 THEN RAISE EXCEPTION 'RESOURCE_V4_SLOT13_ALREADY_SPENT_OR_UNKNOWN';END IF;
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.resource_probe_v3 WHERE state IN('PENDING','RUNNING','EXPIRED'))
 THEN RAISE EXCEPTION 'RESOURCE_V4_PRIOR_UNKNOWN_EFFECT_RECONCILIATION_REQUIRED';END IF;
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.resource_probe_v3 WHERE created_at>clock_timestamp()-interval '30minutes')
 THEN RAISE EXCEPTION 'RESOURCE_V4_MEASUREMENT_RATE_BOUND';END IF;
 IF h.enabled IS DISTINCT FROM true OR h.function_sha256 IS DISTINCT FROM encode(sha256(convert_to(pg_get_functiondef(h.function_oid),'UTF8')),'hex')
 THEN RAISE EXCEPTION 'RESOURCE_V4_ACTUAL_TOP_LEVEL_HANDLER_REQUIRED';END IF;
 SELECT jsonb_object_agg(fn,encode(sha256(convert_to(pg_get_functiondef(fn::regprocedure),'UTF8')),'hex')) INTO manifest
 FROM (VALUES
  ('population_top3000_202608_202609_v1.resource_probe_claim_v4(uuid,text)'),
  ('population_top3000_202608_202609_v1.resource_probe_finish_v4(uuid,text,jsonb)'),
  ('population_top3000_202608_202609_v1.resource_probe_dispatch_v4(text,bigint,bigint,text,bigint)')) f(fn);
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_event e
  WHERE e.event_id=p_review_event_id AND e.event='RESOURCE_V4_SLOT13_DEPLOYMENT_REVIEW'
  AND e.details->>'approved'='true' AND e.details->>'actual_rollback_passed'='true'
  AND e.details->>'owner_id'=p_owner AND e.details->>'owner_fence'=p_fence::text AND e.details->>'stop_generation'=p_stop::text
  AND e.details->>'scope_sha256'=ctl->>'scope_sha256'
  AND e.details->>'worker_sha256'='2a18b7ff292ad6de0f3a012d65472630dac993c44047b9f693cd5a48a80411b5'
  AND e.details->'sql_code_manifest'=manifest
  AND e.details->>'handler_function_sha256'=h.function_sha256
  AND e.details->>'handler_bounds_sha256'=handler_bounds_sha
  AND e.details->>'producer' IS DISTINCT FROM e.details->>'reviewer'
  AND nullif(e.details->>'producer','') IS NOT NULL AND nullif(e.details->>'reviewer','') IS NOT NULL)
 THEN RAISE EXCEPTION 'RESOURCE_V4_EXACT_DEPLOYMENT_REVIEW_REQUIRED';END IF;
 INSERT INTO population_top3000_202608_202609_v1.resource_probe_v4(
  probe_id,owner_id,owner_fence,stop_generation,scope_sha256,token_sha256,state,expires_at,http_invocation_ordinal,probe_attempt,max_runtime_ms)
 VALUES(id,p_owner,p_fence,p_stop,ctl->>'scope_sha256',encode(sha256(convert_to(id::text||':'||token,'UTF8')),'hex'),
  'PENDING',clock_timestamp()+interval '3minutes',a.http_invocations+1,13,30000);
 UPDATE population_top3000_202608_202609_v1.pipeline_control SET reserved_request_usd=reserved_request_usd+1 WHERE singleton;
 SELECT net.http_post(
  url:='https://oxzabweahkoimtevbbny.supabase.co/functions/v1/population-top3000-resource-v4',
  headers:='{"Content-Type":"application/json"}'::jsonb,
  body:=jsonb_build_object('probe_id',id,'token',token),timeout_milliseconds:=40000) INTO rid;
 UPDATE population_top3000_202608_202609_v1.resource_probe_v4 SET request_id=rid WHERE probe_id=id;
 UPDATE population_top3000_202608_202609_v1.acquisition_control SET http_invocations=http_invocations+1 WHERE singleton;
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details)
 VALUES('RESOURCE_V4_SLOT13_DISPATCHED',ctl||jsonb_build_object(
  'clearing_id',p_clearing_id,'probe_id',id,'request_id',rid,'cumulative_probe_attempt',13,
  'new_internal_http_invocations',1,'provider_requests',0,'maximum_response_bytes',2097152,
  'slots1_through12_preserved',true,'original_deadline_preserved',true,'no_capacity_change',true,
  'handler_function_sha256',h.function_sha256,'handler_bounds_sha256',handler_bounds_sha));
 RETURN jsonb_build_object('state','DISPATCHED','probe_id',id,'probe_attempt',13,'request_id',rid,'useful_output_observed',false);
END;$f$;
REVOKE ALL ON FUNCTION population_top3000_202608_202609_v1.resource_probe_dispatch_v4(text,bigint,bigint,text,bigint) FROM public,anon,authenticated;
