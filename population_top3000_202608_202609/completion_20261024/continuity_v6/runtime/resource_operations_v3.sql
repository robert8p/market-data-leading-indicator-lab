-- Private operational functions. Reviewed finite source/relation admission; no capacity purchase, counter reset or data acceptance.
CREATE FUNCTION population_top3000_202608_202609_v1.resource_probe_dispatch_v3(p_owner text,p_fence bigint,p_stop bigint,p_clearing_id text,p_review_event_id bigint)
RETURNS jsonb LANGUAGE plpgsql SET search_path=pg_catalog AS $f$
DECLARE ctl jsonb;a population_top3000_202608_202609_v1.acquisition_control%rowtype;p population_top3000_202608_202609_v1.pipeline_control%rowtype;
 id uuid:=gen_random_uuid();token text:=replace(gen_random_uuid()::text,'-','')||replace(gen_random_uuid()::text,'-','');rid bigint;attempt integer;manifest jsonb;
BEGIN
 ctl:=population_top3000_202608_202609_v1.continuation_guard_v6(p_owner,p_fence,p_stop);
 SELECT * INTO a FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton;
 SELECT * INTO p FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.continuation_clear_queue_v6 WHERE clearing_id=p_clearing_id AND family_id='RESOURCE' AND status='CLAIMED' AND owner_id=p_owner AND owner_fence=p_fence AND stop_generation=p_stop AND claim_expires_at>clock_timestamp()) THEN RAISE EXCEPTION 'RESOURCE_V3_CURRENT_CLEARING_CLAIM_REQUIRED';END IF;
 IF a.admitted_until<=clock_timestamp()+interval '4minutes' OR p.deadline<=clock_timestamp()+interval '4minutes' OR a.http_invocations>=a.max_http_invocations OR population_top3000_202608_202609_v1.pipeline_cost_admission_v2(p,1) IS DISTINCT FROM true THEN RAISE EXCEPTION 'RESOURCE_V3_FINITE_HTTP_ADMISSION_REQUIRED';END IF;
 IF (SELECT count(*) FROM population_top3000_202608_202609_v1.resource_probe_v1)<>3 OR (SELECT count(*) FROM population_top3000_202608_202609_v1.resource_probe_v2 WHERE probe_attempt=4 AND state='SUCCEEDED')<>1 THEN RAISE EXCEPTION 'RESOURCE_V3_PRIOR_HISTORY_REQUIRED';END IF;
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.resource_probe_v3 WHERE state IN('PENDING','RUNNING','EXPIRED')) THEN RAISE EXCEPTION 'RESOURCE_V3_UNKNOWN_EFFECT_RECONCILIATION_REQUIRED';END IF;
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.resource_probe_v3 WHERE created_at>clock_timestamp()-interval '30minutes') THEN RAISE EXCEPTION 'RESOURCE_V3_MEASUREMENT_RATE_BOUND';END IF;
 SELECT coalesce(max(probe_attempt),4)+1 INTO attempt FROM population_top3000_202608_202609_v1.resource_probe_v3;
 IF attempt>12 THEN RAISE EXCEPTION 'RESOURCE_V3_EIGHT_MEASUREMENTS_EXHAUSTED';END IF;
 SELECT jsonb_object_agg(fn,encode(sha256(convert_to(pg_get_functiondef(fn::regprocedure),'UTF8')),'hex')) INTO manifest FROM (VALUES
 ('population_top3000_202608_202609_v1.resource_probe_claim_v3(uuid,text)'),('population_top3000_202608_202609_v1.resource_probe_finish_v3(uuid,text,jsonb)'),('population_top3000_202608_202609_v1.resource_probe_dispatch_v3(text,bigint,bigint,text,bigint)'),('population_top3000_202608_202609_v1.resource_measured_admission_v3(text,bigint,bigint,text,uuid,jsonb,bigint)')) f(fn);
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_event e WHERE event_id=p_review_event_id AND event='RESOURCE_V3_DEPLOYMENT_VERIFIED' AND details->>'approved'='true' AND details->>'actual_rollback_passed'='true' AND details->>'owner_id'=p_owner AND details->>'owner_fence'=p_fence::text AND details->>'stop_generation'=p_stop::text AND details->>'scope_sha256'=ctl->>'scope_sha256' AND details->>'worker_sha256'='1142afb4e20c2643bbba09f1f117eec96c436a285738cf741e035bfc7192af98' AND details->'sql_code_manifest'=manifest AND nullif(details->>'producer','') IS NOT NULL AND nullif(details->>'reviewer','') IS NOT NULL AND details->>'reviewer' IS DISTINCT FROM details->>'producer') THEN RAISE EXCEPTION 'RESOURCE_V3_PINNED_DEPLOYMENT_REVIEW_REQUIRED';END IF;
 INSERT INTO population_top3000_202608_202609_v1.resource_probe_v3(probe_id,owner_id,owner_fence,stop_generation,scope_sha256,token_sha256,state,expires_at,http_invocation_ordinal,probe_attempt,max_runtime_ms)
 VALUES(id,p_owner,p_fence,p_stop,ctl->>'scope_sha256',encode(sha256(convert_to(id::text||':'||token,'UTF8')),'hex'),'PENDING',clock_timestamp()+interval '3minutes',a.http_invocations+1,attempt,30000);
 UPDATE population_top3000_202608_202609_v1.pipeline_control SET reserved_request_usd=reserved_request_usd+1 WHERE singleton;
 SELECT net.http_post(url:='https://oxzabweahkoimtevbbny.supabase.co/functions/v1/population-top3000-resource-v3',headers:='{"Content-Type":"application/json"}'::jsonb,body:=jsonb_build_object('probe_id',id,'token',token),timeout_milliseconds:=40000) INTO rid;
 UPDATE population_top3000_202608_202609_v1.resource_probe_v3 SET request_id=rid WHERE probe_id=id;
 UPDATE population_top3000_202608_202609_v1.acquisition_control SET http_invocations=http_invocations+1 WHERE singleton;
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES('RESOURCE_V3_DISPATCHED',ctl||jsonb_build_object('clearing_id',p_clearing_id,'probe_id',id,'request_id',rid,'cumulative_probe_attempt',attempt,'new_internal_http_invocations',1,'provider_requests',0,'maximum_response_bytes',2097152,'original_probes_preserved',true,'original_deadline_preserved',true,'no_capacity_change',true));
 RETURN jsonb_build_object('state','DISPATCHED','probe_id',id,'probe_attempt',attempt,'request_id',rid,'useful_output_observed',false);
END;$f$;

CREATE FUNCTION population_top3000_202608_202609_v1.resource_measured_admission_v3(p_owner text,p_fence bigint,p_stop bigint,p_clearing_id text,p_probe_id uuid,p_proposal jsonb,p_review_event_id bigint)
RETURNS jsonb LANGUAGE plpgsql SET search_path=pg_catalog AS $f$
DECLARE ctl jsonb;a population_top3000_202608_202609_v1.acquisition_control%rowtype;p population_top3000_202608_202609_v1.pipeline_control%rowtype;v population_top3000_202608_202609_v1.resource_probe_v3%rowtype;
 q population_top3000_202608_202609_v1.continuation_clear_queue_v6%rowtype;old_relation bigint;new_relation bigint;new_source bigint;relation_used bigint;available bigint;available_numeric numeric;delta bigint;h text;ev bigint;before_counters jsonb;after_counters jsonb;
BEGIN
 ctl:=population_top3000_202608_202609_v1.continuation_guard_v6(p_owner,p_fence,p_stop);
 SELECT * INTO a FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton;
 SELECT * INTO p FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton;
 SELECT * INTO STRICT q FROM population_top3000_202608_202609_v1.continuation_clear_queue_v6 WHERE clearing_id=p_clearing_id FOR UPDATE;
 IF q.family_id<>'RESOURCE' OR q.status<>'CLAIMED' OR q.owner_id<>p_owner OR q.owner_fence<>p_fence OR q.stop_generation<>p_stop OR q.claim_expires_at IS NULL OR q.claim_expires_at<=clock_timestamp() THEN RAISE EXCEPTION 'RESOURCE_V3_CURRENT_CLEARING_CLAIM_REQUIRED';END IF;
 SELECT * INTO STRICT v FROM population_top3000_202608_202609_v1.resource_probe_v3 WHERE probe_id=p_probe_id;
 IF v.state<>'SUCCEEDED' OR v.owner_id<>p_owner OR v.owner_fence<>p_fence OR v.stop_generation<>p_stop OR v.result_sha256 IS DISTINCT FROM encode(sha256(convert_to(v.result::text,'UTF8')),'hex') OR v.result->>'observed_at' IS NULL OR (v.result->>'observed_at')::timestamptz<clock_timestamp()-interval '10minutes' THEN RAISE EXCEPTION 'RESOURCE_V3_FRESH_MEASUREMENT_REQUIRED';END IF;
 IF (SELECT count(*) FROM jsonb_array_elements(v.result->'metrics') m WHERE m->>'mountpoint'='/data')<>1 OR current_setting('data_directory') NOT LIKE '/data/%' THEN RAISE EXCEPTION 'RESOURCE_V3_DATA_MOUNT_REQUIRED';END IF;
 SELECT (m->>'available_bytes')::numeric INTO available_numeric FROM jsonb_array_elements(v.result->'metrics') m WHERE m->>'mountpoint'='/data';
 IF available_numeric IS NULL OR available_numeric<0 OR available_numeric>9007199254740991 OR trunc(available_numeric)<>available_numeric THEN RAISE EXCEPTION 'RESOURCE_V3_INVALID_DATA_AVAILABLE_BYTES';END IF;
 available:=available_numeric::bigint;
 old_relation:=(p.budget_evidence#>>'{resource_limits,max_relation_bytes}')::bigint;
 IF old_relation IS NULL OR old_relation<=0 THEN RAISE EXCEPTION 'RESOURCE_V3_REQUIRED_RELATION_LIMIT_MISSING';END IF;
 IF p_proposal->>'old_max_relation_bytes' IS DISTINCT FROM old_relation::text OR p_proposal->>'old_max_source_bytes' IS DISTINCT FROM a.max_source_bytes::text OR p_proposal->>'probe_result_sha256' IS DISTINCT FROM v.result_sha256 OR p_proposal->>'original_deadline' IS DISTINCT FROM p.deadline::text THEN RAISE EXCEPTION 'RESOURCE_V3_EXACT_PREIMAGE_PROPOSAL_REQUIRED';END IF;
 new_relation:=(p_proposal->>'new_max_relation_bytes')::bigint;new_source:=(p_proposal->>'new_max_source_bytes')::bigint;
 IF new_relation IS NULL OR new_source IS NULL OR new_relation<old_relation OR new_relation>least(20000000000,old_relation+5000000000) OR new_source IS DISTINCT FROM a.max_source_bytes OR (new_relation=old_relation AND new_source=a.max_source_bytes) THEN RAISE EXCEPTION 'RESOURCE_V3_FINITE_INCREMENT_REQUIRED';END IF;
 SELECT coalesce(sum(pg_total_relation_size(c.oid)),0) INTO relation_used FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='population_top3000_202608_202609_v1' AND c.relkind IN('r','m');
 -- Conservatively cover all remaining admitted relation growth plus remaining source allowance; source bytes may also reside in those relations.
 delta:=greatest(new_relation-relation_used,0)+greatest(new_source-a.stored_bytes-a.reserved_bytes,0)+a.reserved_bytes;
 IF available-delta<20000000000 OR new_relation-relation_used<250000000 OR new_source-a.stored_bytes-a.reserved_bytes<64000000 THEN RAISE EXCEPTION 'RESOURCE_V3_MEASURED_HEADROOM_INSUFFICIENT';END IF;
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE status='LEASED') OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_provider_lease WHERE released_at IS NULL) OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.connector_probe_lease_v1 WHERE released_at IS NULL) THEN RAISE EXCEPTION 'RESOURCE_V3_ACTIVE_WORK_RETRY_LATER';END IF;
 h:=encode(sha256(convert_to(p_proposal::text,'UTF8')),'hex');
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_event e WHERE event_id=p_review_event_id AND event='RESOURCE_V3_MEASURED_ADMISSION_REVIEW' AND details->>'approved'='true' AND details->>'clearing_id'=p_clearing_id AND details->>'input_sha256'=q.input_sha256 AND details->>'proposal_sha256'=h AND details->>'probe_result_sha256'=v.result_sha256 AND details->>'owner_id'=p_owner AND details->>'owner_fence'=p_fence::text AND details->>'stop_generation'=p_stop::text AND details->>'scope_sha256'=ctl->>'scope_sha256' AND nullif(details->>'producer','') IS NOT NULL AND nullif(details->>'reviewer','') IS NOT NULL AND details->>'reviewer' IS DISTINCT FROM details->>'producer') THEN RAISE EXCEPTION 'RESOURCE_V3_INDEPENDENT_MEASURED_REVIEW_REQUIRED';END IF;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=(p_proposal->>'successor_task_id')::uuid AND status IN('READY','BLOCKED')) THEN RAISE EXCEPTION 'RESOURCE_V3_EXACT_CONTINUING_TASK_REQUIRED';END IF;
 before_counters:=to_jsonb(a)-'max_source_bytes'||jsonb_build_object('pipeline',to_jsonb(p)-'budget_evidence','budget',p.budget_evidence#-'{resource_limits,max_relation_bytes}');
 UPDATE population_top3000_202608_202609_v1.acquisition_control SET max_source_bytes=new_source WHERE singleton;
 UPDATE population_top3000_202608_202609_v1.pipeline_control SET budget_evidence=jsonb_set(budget_evidence,'{resource_limits,max_relation_bytes}',to_jsonb(new_relation)) WHERE singleton;
 SELECT (to_jsonb(ac)-'max_source_bytes')||jsonb_build_object('pipeline',to_jsonb(pc)-'budget_evidence','budget',pc.budget_evidence#-'{resource_limits,max_relation_bytes}') INTO after_counters FROM population_top3000_202608_202609_v1.acquisition_control ac CROSS JOIN population_top3000_202608_202609_v1.pipeline_control pc WHERE ac.singleton AND pc.singleton;
 IF before_counters IS DISTINCT FROM after_counters THEN RAISE EXCEPTION 'RESOURCE_V3_COUNTER_OR_HISTORY_CHANGED';END IF;
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES('CONTINUATION_MEASURED_ADMISSION_V6',ctl||jsonb_build_object('clearing_id',p_clearing_id,'probe_id',p_probe_id,'probe_result_sha256',v.result_sha256,'proposal',p_proposal,'proposal_sha256',h,'independent_review_event_id',p_review_event_id,'available_bytes',available,'mission_relation_bytes',relation_used,'cumulative_counters_preserved',true,'original_deadline_preserved',true,'effects_known',true,'successor_task_id',p_proposal->>'successor_task_id','source_holds_unchanged',true)) RETURNING event_id INTO ev;
 RETURN jsonb_build_object('state','ADMITTED','useful_event_id',ev,'new_max_relation_bytes',new_relation,'new_max_source_bytes',new_source,'cumulative_counters_preserved',true,'original_deadline_preserved',true,'data_acceptance',false);
END;$f$;
REVOKE ALL ON FUNCTION population_top3000_202608_202609_v1.resource_probe_dispatch_v3(text,bigint,bigint,text,bigint),population_top3000_202608_202609_v1.resource_measured_admission_v3(text,bigint,bigint,text,uuid,jsonb,bigint) FROM public,anon,authenticated;
