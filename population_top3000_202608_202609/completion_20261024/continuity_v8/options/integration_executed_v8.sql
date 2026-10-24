BEGIN;
SET LOCAL statement_timeout='60s';
SET LOCAL lock_timeout='6s';
SELECT pg_advisory_xact_lock(2026083000261022);
DO $guard$
DECLARE ctl jsonb;
BEGIN
 ctl:=population_top3000_202608_202609_v1.continuation_guard_v6('top3000-completion-20261024-root-5db48b32d103',3,1);
 PERFORM 1 FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton FOR UPDATE;
 PERFORM 1 FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton FOR UPDATE;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_event WHERE event_id=27679 AND event='ENGINEERING_ASSIGNMENT_CLAIMED_V5' AND details->>'assignment_event_id'='27678' AND details->>'executor'='controller:top3000-completion-20261024-root-5db48b32d103' AND (details->>'expires_at')::timestamptz>clock_timestamp())
 OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_event WHERE details->>'claim_event_id'='27679' AND event IN('ENGINEERING_ASSIGNMENT_COMPLETED_V5','ENGINEERING_ASSIGNMENT_EFFECTS_RECONCILED_RELEASE_V5'))
 THEN RAISE EXCEPTION 'V8_LIVE_CONTROLLER_CLAIM_REQUIRED';END IF;
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE status='LEASED') OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_attempt WHERE outcome='RUNNING') OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_provider_lease WHERE released_at IS NULL) OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.connector_probe_lease_v1 WHERE released_at IS NULL) THEN RAISE EXCEPTION 'V8_ACTIVE_OR_UNRECONCILED_OPERATION';END IF;
 IF encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.ctx02_clear_page_admission_v6(text,bigint,bigint,jsonb)'::regprocedure),'UTF8')),'hex') IS DISTINCT FROM '3337167897438f07077b666c57f252be90d6ffbf2f54c2e65a9fd9fd4eee430b'
 OR encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.continuation_dispatch_v6(text,bigint,bigint)'::regprocedure),'UTF8')),'hex') IS DISTINCT FROM 'eaf9bb45a3f5e3e6f23399a3758afe9e1603c3f2df2c1e6ad1751f6778b2d573'
 OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.continuation_handler_v6 WHERE kind='CTX02_PAGE_ADMISSION' AND function_sha256='3337167897438f07077b666c57f252be90d6ffbf2f54c2e65a9fd9fd4eee430b' AND bounds->>'absolute_page_ceiling'='1247' AND review_event_id=27067)
 THEN RAISE EXCEPTION 'V8_REVIEWED_PREIMAGE_CHANGED';END IF;
END $guard$;
-- Exact reviewed request-policy continuation. No transport, source rewrite or family acceptance.
CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.ctx02_clear_page_admission_v6(p_owner text,p_fence bigint,p_stop bigint,p_item jsonb)
RETURNS jsonb LANGUAGE plpgsql SET search_path TO pg_catalog AS $function$
DECLARE
 a population_top3000_202608_202609_v1.acquisition_control%rowtype;
 p population_top3000_202608_202609_v1.pipeline_control%rowtype;
 t population_top3000_202608_202609_v1.pipeline_task%rowtype;
 r population_top3000_202608_202609_v1.ctx02_census_request_v1%rowtype;
 adm population_top3000_202608_202609_v1.ctx02_census_admission_v1%rowtype;
 handler population_top3000_202608_202609_v1.continuation_handler_v6%rowtype;
 policy jsonb; bounds_hash text; req text; before_t jsonb; after_t jsonb; before_a jsonb; before_p jsonb; ans jsonb; wake jsonb;
 used_calls bigint; pages bigint; bad bigint; relbytes bigint; maxwire bigint; recordbytes bigint; estimated bigint; normalized bigint;
 new_ceiling integer; new_calls integer; tranche integer; future_pages integer; max_pages integer; reason text; last_url text; first_url text;
 provider_limit bigint; http_limit bigint; source_limit bigint; relation_limit bigint;
BEGIN
 PERFORM pg_advisory_xact_lock(2026083000261022);
 SELECT * INTO STRICT a FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton FOR UPDATE;
 SELECT * INTO STRICT p FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton FOR UPDATE;
 IF p_owner IS DISTINCT FROM 'top3000-completion-20261024-root-5db48b32d103' OR p_fence IS DISTINCT FROM 3 OR p_stop IS DISTINCT FROM 1
 OR a.owner_id IS DISTINCT FROM p_owner OR a.owner_fence IS DISTINCT FROM p_fence OR a.stop_generation IS DISTINCT FROM p_stop
 OR a.enabled IS DISTINCT FROM true OR a.stopped IS DISTINCT FROM false OR a.hold_reason IS NOT NULL OR p.enabled IS DISTINCT FROM true
 OR p.expected_owner_id IS DISTINCT FROM a.owner_id OR p.expected_owner_fence IS DISTINCT FROM a.owner_fence OR p.expected_stop_generation IS DISTINCT FROM a.stop_generation
 OR a.scope_sha256 IS DISTINCT FROM '8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd' OR p.scope_sha256 IS DISTINCT FROM a.scope_sha256
 OR a.admitted_until IS NULL OR a.admitted_until<=clock_timestamp()+interval '1 second' OR p.deadline IS NULL OR p.deadline<=clock_timestamp()+interval '1 second'
 OR p.budget_evidence#>>'{monetary_policy,authority_event_id}' IS DISTINCT FROM '9528'
 OR population_top3000_202608_202609_v1.pipeline_cost_admission_v2(p,0) IS DISTINCT FROM true
 THEN RAISE EXCEPTION 'OPTIONS_V8_CURRENT_AUTHORITY_REQUIRED'; END IF;
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE status='LEASED')
 OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_provider_lease WHERE released_at IS NULL)
 OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.connector_probe_lease_v1 WHERE released_at IS NULL)
 THEN RAISE EXCEPTION 'OPTIONS_V8_ACTIVE_OR_UNRECONCILED_EFFECT'; END IF;
 SELECT * INTO STRICT handler FROM population_top3000_202608_202609_v1.continuation_handler_v6 WHERE kind='CTX02_PAGE_ADMISSION' AND enabled;
 bounds_hash:=encode(sha256(convert_to(handler.bounds::text,'UTF8')),'hex');
 IF handler.function_oid IS DISTINCT FROM 'population_top3000_202608_202609_v1.ctx02_clear_page_admission_v6(text,bigint,bigint,jsonb)'::regprocedure
 OR handler.function_sha256 IS DISTINCT FROM encode(sha256(convert_to(pg_get_functiondef(handler.function_oid),'UTF8')),'hex')
 OR p_item->'bounds' IS DISTINCT FROM handler.bounds
 OR handler.bounds->>'policy_revision' IS DISTINCT FROM 'CTX02_MEASURED_REQUEST_CONTINUATION_V8'
 OR jsonb_typeof(handler.bounds->'request_policies') IS DISTINCT FROM 'object'
 OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_event e WHERE e.event_id=handler.review_event_id AND e.details->>'approved'='true'
 AND e.details->>'handler_function_sha256'=handler.function_sha256 AND e.details->>'handler_bounds_sha256'=bounds_hash
 AND e.details->>'owner_id'=p_owner AND e.details->>'owner_fence'=p_fence::text AND e.details->>'stop_generation'=p_stop::text AND e.details->>'scope_sha256'=a.scope_sha256
 AND nullif(e.details->>'reviewer','') IS NOT NULL AND nullif(e.details->>'producer','') IS NOT NULL AND e.details->>'reviewer' IS DISTINCT FROM e.details->>'producer')
 THEN RAISE EXCEPTION 'OPTIONS_V8_EXACT_REVIEWED_POLICY_REQUIRED'; END IF;
 IF coalesce(handler.bounds->>'maximum_new_pages','')!~'^[0-9]{1,2}$' OR (handler.bounds->>'maximum_new_pages')::integer NOT BETWEEN 1 AND 50
 OR coalesce(handler.bounds#>>'{global_ceiling,provider_requests}','')!~'^[0-9]{1,10}$'
 OR coalesce(handler.bounds#>>'{global_ceiling,http_invocations}','')!~'^[0-9]{1,10}$'
 OR coalesce(handler.bounds#>>'{global_ceiling,source_bytes}','')!~'^[0-9]{1,15}$'
 OR coalesce(handler.bounds#>>'{global_ceiling,relation_bytes}','')!~'^[0-9]{1,15}$'
 THEN RAISE EXCEPTION 'OPTIONS_V8_FINITE_POLICY_REQUIRED'; END IF;
 policy:=handler.bounds->'request_policies'->(p_item->>'source_task_id');
 IF jsonb_typeof(policy) IS DISTINCT FROM 'object' THEN RAISE EXCEPTION 'OPTIONS_V8_TASK_NOT_ALLOWLISTED'; END IF;
 SELECT * INTO STRICT t FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=(p_item->>'source_task_id')::uuid FOR UPDATE;
 IF t.status IS DISTINCT FROM 'BLOCKED' OR t.blocker_code IS DISTINCT FROM 'CTX02_CENSUS_PAGE_CAP_REVIEW' OR t.family_id IS DISTINCT FROM 'CTX02' OR t.stage IS DISTINCT FROM 'ACQUIRE'
 OR t.lease_until IS NOT NULL OR t.executions>=t.max_executions OR t.cumulative_failures>=t.max_failures
 OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_attempt WHERE attempt_id=t.attempt_id AND task_id=t.task_id AND ended_at IS NOT NULL)
 THEN RAISE EXCEPTION 'OPTIONS_V8_EXACT_IDLE_CHECKPOINT_REQUIRED'; END IF;
 req:=policy->>'request_key';
 SELECT * INTO STRICT r FROM population_top3000_202608_202609_v1.ctx02_census_request_v1 WHERE request_key=req;
 IF t.payload->>'request_key' IS DISTINCT FROM req OR t.partition_key IS DISTINCT FROM req
 OR r.manifest_sha256 IS DISTINCT FROM policy->>'manifest_sha256'
 OR r.manifest_sha256 IS DISTINCT FROM encode(sha256(convert_to(r.manifest::text,'UTF8')),'hex')
 OR r.request_key IS DISTINCT FROM 'ctx02-census-v1:'||r.manifest_sha256
 OR encode(sha256(convert_to(t.payload::text,'UTF8')),'hex') IS DISTINCT FROM policy->>'payload_sha256'
 OR encode(sha256(convert_to(t.input_versions::text,'UTF8')),'hex') IS DISTINCT FROM policy->>'input_versions_sha256'
 OR r.input_versions IS DISTINCT FROM t.input_versions
 THEN RAISE EXCEPTION 'OPTIONS_V8_PINNED_REQUEST_CHANGED'; END IF;
 IF encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.ctx02_census_admit_v1(text,bigint,bigint,text,integer,integer,timestamptz,text)'::regprocedure),'UTF8')),'hex') IS DISTINCT FROM 'fee6da97908c100dd903e42cbf5a04d23548429af8b8f00b0a917850551f471d'
 OR encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.ctx02_census_context_v1(uuid)'::regprocedure),'UTF8')),'hex') IS DISTINCT FROM 'c55812c698b6b87e42560d639a0cb9fde89ec52dc68f26efe91d872bf7664a95'
 THEN RAISE EXCEPTION 'OPTIONS_V8_REVIEWED_PREIMAGE_CHANGED'; END IF;
 -- Existing context validates exact URL, cursor, manifest scope and final-source proof before any allowance change.
 ans:=population_top3000_202608_202609_v1.ctx02_census_context_v1(t.task_id);
 IF ans->>'state' IS DISTINCT FROM 'PAGE_LIMIT_REVIEW' THEN RAISE EXCEPTION 'OPTIONS_V8_EXHAUSTED_OR_EXPIRED_ADMISSION_REQUIRED'; END IF;
 SELECT * INTO STRICT adm FROM population_top3000_202608_202609_v1.ctx02_census_admission_v1 WHERE request_key=req FOR UPDATE;
 SELECT count(*),max(wire_bytes) INTO pages,maxwire FROM population_top3000_202608_202609_v1.ctx02_census_page_v1 WHERE partition_key=req;
 SELECT count(*) INTO used_calls FROM population_top3000_202608_202609_v1.pipeline_provider_lease l JOIN population_top3000_202608_202609_v1.pipeline_attempt z USING(attempt_id) WHERE z.task_id=t.task_id AND l.provider='Massive';
 IF pages<1 OR used_calls<pages OR used_calls>pages+4 OR t.checkpoint->>'next_page' IS DISTINCT FROM (pages+1)::text
 OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_provider_lease l JOIN population_top3000_202608_202609_v1.pipeline_attempt z USING(attempt_id) WHERE z.task_id=t.task_id AND (z.ended_at IS NULL OR l.released_at IS NULL))
 THEN RAISE EXCEPTION 'OPTIONS_V8_PAGE_CALL_RECONCILIATION_CHANGED'; END IF;
 SELECT next_url INTO last_url FROM population_top3000_202608_202609_v1.ctx02_census_page_v1 WHERE partition_key=req AND page_number=pages;
 SELECT request_url INTO first_url FROM population_top3000_202608_202609_v1.ctx02_census_page_v1 WHERE partition_key=req AND page_number=1;
 IF last_url IS NULL OR last_url IS DISTINCT FROM t.checkpoint->>'next_url' OR first_url IS DISTINCT FROM r.manifest->>'first_url' THEN RAISE EXCEPTION 'OPTIONS_V8_NONTERMINAL_CHAIN_REQUIRED'; END IF;
 SELECT count(*) INTO bad FROM (SELECT q.*,row_number() OVER(ORDER BY page_number) expected,lag(next_url) OVER(ORDER BY page_number) prior_url FROM population_top3000_202608_202609_v1.ctx02_census_page_v1 q WHERE partition_key=req)x LEFT JOIN population_top3000_202608_202609_v1.source_document d USING(source_id)
 WHERE d.source_id IS NULL OR page_number<>expected OR (page_number>1 AND request_url IS DISTINCT FROM prior_url) OR x.source_sha256 IS DISTINCT FROM d.source_sha256
 OR d.source_sha256 IS DISTINCT FROM encode(sha256(convert_to(d.content_text,'UTF8')),'hex') OR d.content_bytes IS DISTINCT FROM octet_length(d.content_text) OR x.acquire_task_id IS DISTINCT FROM t.task_id;
 IF bad<>0 THEN RAISE EXCEPTION 'OPTIONS_V8_RETAINED_CHAIN_CHANGED'; END IF;
 tranche:=(handler.bounds->>'maximum_new_pages')::integer; max_pages:=(r.manifest->>'max_pages')::integer;
 IF pages>=max_pages OR adm.admitted_page_ceiling<pages OR adm.max_provider_calls<used_calls THEN RAISE EXCEPTION 'OPTIONS_V8_MANIFEST_OR_ACCOUNTING_LIMIT'; END IF;
 new_ceiling:=adm.admitted_page_ceiling; new_calls:=adm.max_provider_calls;
 IF pages=adm.admitted_page_ceiling THEN
  new_ceiling:=least(max_pages,pages+tranche); new_calls:=greatest(adm.max_provider_calls,used_calls+new_ceiling-pages); reason:='PAGE_ALLOWANCE';
 ELSIF used_calls=adm.max_provider_calls THEN
  new_calls:=used_calls+least(tranche,adm.admitted_page_ceiling-pages); reason:='CALL_ALLOWANCE';
 ELSIF adm.expires_at<=clock_timestamp() THEN reason:='EXPIRY_ONLY';
 ELSE RAISE EXCEPTION 'OPTIONS_V8_NO_ELIGIBLE_LIMIT'; END IF;
 IF new_ceiling-adm.admitted_page_ceiling NOT BETWEEN 0 AND tranche OR new_calls-adm.max_provider_calls NOT BETWEEN 0 AND tranche THEN RAISE EXCEPTION 'OPTIONS_V8_TRANCHE_LIMIT'; END IF;
 future_pages:=least(new_ceiling-pages,new_calls-used_calls);
 IF future_pages NOT BETWEEN 1 AND tranche THEN RAISE EXCEPTION 'OPTIONS_V8_FINITE_REMAINING_TRANCHE_REQUIRED'; END IF;
 SELECT coalesce(sum(pg_total_relation_size(c.oid)),0) INTO relbytes FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='population_top3000_202608_202609_v1' AND c.relkind IN('r','m');
 recordbytes:=pg_total_relation_size('population_top3000_202608_202609_v1.ctx02_census_record_v1');
 SELECT count(*) INTO normalized FROM population_top3000_202608_202609_v1.ctx02_census_record_v1;
 IF normalized<1000 OR maxwire IS NULL OR maxwire<=0 THEN RAISE EXCEPTION 'OPTIONS_V8_MEASURED_POPULATION_REQUIRED'; END IF;
 estimated:=ceil(recordbytes::numeric/normalized*(future_pages*1000)*1.5)::bigint + maxwire*future_pages*2 +16777216;
 provider_limit:=least(a.max_provider_requests,(handler.bounds#>>'{global_ceiling,provider_requests}')::bigint);
 http_limit:=least(a.max_http_invocations,(handler.bounds#>>'{global_ceiling,http_invocations}')::bigint);
 source_limit:=least(a.max_source_bytes,(handler.bounds#>>'{global_ceiling,source_bytes}')::bigint);
 relation_limit:=least((p.budget_evidence#>>'{resource_limits,max_relation_bytes}')::bigint,(handler.bounds#>>'{global_ceiling,relation_bytes}')::bigint);
 IF provider_limit IS NULL OR http_limit IS NULL OR source_limit IS NULL OR relation_limit IS NULL OR least(provider_limit,http_limit,source_limit,relation_limit)<=0
 OR relbytes+estimated>relation_limit OR a.stored_bytes+a.reserved_bytes+future_pages*(r.manifest->>'max_response_bytes')::bigint>source_limit
 OR a.provider_requests+future_pages>provider_limit OR a.http_invocations+future_pages>http_limit
 OR p.tick_count+future_pages*4>p.max_ticks OR a.dispatch_calls+future_pages*4>a.max_dispatch_calls
 THEN RAISE EXCEPTION 'OPTIONS_V8_MEASURED_RESOURCE_REVIEW_REQUIRED'; END IF;
 before_t:=to_jsonb(t); before_a:=to_jsonb(a); before_p:=to_jsonb(p);
 ans:=population_top3000_202608_202609_v1.ctx02_census_admit_v1(a.owner_id,a.owner_fence,a.stop_generation,req,new_ceiling,new_calls,least(a.admitted_until,p.deadline)-interval '1 second','V8 reviewed exact request policy: '||reason||'; <=50 measured pages; source chain, counters, current finite global resources and stop controls preserved');
 IF population_top3000_202608_202609_v1.ctx02_census_context_v1(t.task_id)->>'state' IS DISTINCT FROM 'READY' THEN RAISE EXCEPTION 'OPTIONS_V8_ADMISSION_NOT_READY'; END IF;
 UPDATE population_top3000_202608_202609_v1.pipeline_task SET status='READY',blocker_code=NULL,blocker_until=NULL,resumption_condition=NULL,not_before=clock_timestamp() WHERE task_id=t.task_id AND status='BLOCKED' AND blocker_code='CTX02_CENSUS_PAGE_CAP_REVIEW';
 SELECT to_jsonb(z) INTO after_t FROM population_top3000_202608_202609_v1.pipeline_task z WHERE task_id=t.task_id;
 IF (after_t-ARRAY['status','blocker_code','blocker_until','resumption_condition','not_before']) IS DISTINCT FROM (before_t-ARRAY['status','blocker_code','blocker_until','resumption_condition','not_before'])
 OR before_a IS DISTINCT FROM (SELECT to_jsonb(z) FROM population_top3000_202608_202609_v1.acquisition_control z WHERE singleton)
 OR before_p IS DISTINCT FROM (SELECT to_jsonb(z) FROM population_top3000_202608_202609_v1.pipeline_control z WHERE singleton)
 THEN RAISE EXCEPTION 'OPTIONS_V8_COUNTER_OR_HISTORY_MUTATION'; END IF;
 wake:=population_top3000_202608_202609_v1.pipeline_wake_v1(a.owner_id,a.owner_fence,'Reviewed V8 policy: exact options task requeued within measured finite global allowance');
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES('CTX02_BOUNDED_ALLOWANCE_CLEARED_V8',jsonb_build_object('task_id',t.task_id,'request_key',req,'owner_id',a.owner_id,'owner_fence',a.owner_fence,'stop_generation',a.stop_generation,'scope_sha256',a.scope_sha256,'policy_sha256',bounds_hash,'policy_review_event_id',handler.review_event_id,'reason',reason,'before_task',before_t,'admission',ans,'mission_relation_bytes',relbytes,'estimated_additional_bytes',estimated,'provider_calls_preserved',used_calls,'existing_pages',pages,'future_admitted_pages',future_pages,'allowed_new_pages',new_ceiling-adm.admitted_page_ceiling,'source_denominator_unchanged',true,'parser_acceptance_holds_unchanged',true,'wake',wake));
 RETURN jsonb_build_object('state','CLEARED','source_task_id',t.task_id,'request_key',req,'next_page',pages+1,'admitted_page_ceiling',new_ceiling,'admitted_call_ceiling',new_calls,'policy_sha256',bounds_hash,'reason',reason,'expected_successor','REAL_ACQUIRE_COMMIT_THEN_NORMALIZE','useful_output_observed',false,'estimated_additional_bytes',estimated);
END;$function$;
REVOKE ALL ON FUNCTION population_top3000_202608_202609_v1.ctx02_clear_page_admission_v6(text,bigint,bigint,jsonb) FROM PUBLIC,anon,authenticated;
CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.continuation_dispatch_v6(p_owner text, p_fence bigint, p_stop bigint)
 RETURNS jsonb
 LANGUAGE plpgsql
 SET search_path TO 'pg_catalog'
AS $function$
DECLARE ctl jsonb;q record;manifest jsonb;h text;k text;v_kind text;ev bigint;inserted integer:=0;external_count integer:=0;changed integer;last_progress timestamptz;pending integer;handler population_top3000_202608_202609_v1.continuation_handler_v6%rowtype;ans jsonb;fn text;src uuid;cm record;ac population_top3000_202608_202609_v1.acquisition_control%rowtype;pc population_top3000_202608_202609_v1.pipeline_control%rowtype;usage_bytes bigint;policy_generation text;
BEGIN
 ctl:=population_top3000_202608_202609_v1.continuation_guard_v6(p_owner,p_fence,p_stop);

 PERFORM population_top3000_202608_202609_v1.sequencing_dependency_wake_v1(p_owner,p_fence,p_stop);
 -- At most one full classification pass per minute, including when scheduler cadence is two seconds.
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_event WHERE event='CONTINUATION_DISPATCH_PASS_V6' AND at>clock_timestamp()-interval '60seconds') THEN RETURN '{"state":"BOUNDED_RECHECK_NOT_DUE"}'::jsonb;END IF;
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES('CONTINUATION_DISPATCH_PASS_V6',ctl);
 -- Renew operational time admission before expiry. This does not raise call/storage/tick limits or the original deadline.
 SELECT * INTO ac FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton;SELECT * INTO pc FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton;
 IF ac.admitted_until<clock_timestamp()+interval '30minutes' AND pc.deadline>ac.admitted_until
 AND (SELECT count(*) FROM population_top3000_202608_202609_v1.pipeline_event WHERE event='CONTINUATION_TIME_ADMISSION_REVIEWED_V6')<16 THEN
 SELECT coalesce(sum(pg_total_relation_size(c.oid)),0) INTO usage_bytes FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='population_top3000_202608_202609_v1' AND c.relkind IN('r','m');
 IF usage_bytes < (pc.budget_evidence#>>'{resource_limits,max_relation_bytes}')::bigint
 AND ac.stored_bytes+ac.reserved_bytes<ac.max_source_bytes
 AND ac.provider_requests<ac.max_provider_requests AND ac.http_invocations<ac.max_http_invocations
 AND NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_provider_lease WHERE released_at IS NULL)
 AND NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.connector_probe_lease_v1 WHERE released_at IS NULL)
 AND NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE status='LEASED')
 THEN
 UPDATE population_top3000_202608_202609_v1.acquisition_control SET admitted_until=least(pc.deadline,clock_timestamp()+interval '3hours') WHERE singleton;
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES('CONTINUATION_TIME_ADMISSION_REVIEWED_V6',ctl||jsonb_build_object('old_admitted_until',ac.admitted_until,'new_admitted_until',least(pc.deadline,clock_timestamp()+interval '3hours'),'original_deadline',pc.deadline,'mission_relation_bytes',usage_bytes,'source_bytes',ac.stored_bytes,'cumulative_counters_preserved',true,'call_storage_tick_caps_unchanged',true,'reviewed_policy','CURRENT_USER_CONTINUITY_AUTHORITY_WITH_FINITE3H_TRANCHE_MAX16'));
 ctl:=population_top3000_202608_202609_v1.continuation_guard_v6(p_owner,p_fence,p_stop);
 END IF;END IF;

 -- Re-admit only unclaimed clearing work to the newly reviewed operational horizon, preserving every prior deadline and attempt.
 FOR q IN SELECT * FROM population_top3000_202608_202609_v1.continuation_clear_queue_v6 WHERE status IN('READY','NEEDS_REVIEW') AND not_after<(ctl->>'not_after')::timestamptz AND not_after<=clock_timestamp()+interval '30minutes' ORDER BY created_at LIMIT 96 FOR UPDATE LOOP
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES('ENGINEERING_CLEARING_ADMISSION_RENEWED_V6',ctl||jsonb_build_object('clearing_id',q.clearing_id,'original_assignment_event_id',q.assignment_event_id,'prior_not_after',q.not_after,'new_not_after',ctl->>'not_after','input_sha256',q.input_sha256,'attempts_preserved',q.attempts,'claimed_rows_excluded',true)) RETURNING event_id INTO ev;
 UPDATE population_top3000_202608_202609_v1.continuation_clear_queue_v6 SET not_after=(ctl->>'not_after')::timestamptz,last_receipt=coalesce(last_receipt,'{}'::jsonb)||jsonb_build_object('admission_renewal_event_id',ev),updated_at=clock_timestamp() WHERE clearing_id=q.clearing_id;
 END LOOP;
 -- An admitted/requeued task is not yet cleared. Require its real source commit and automatic normalization successor.
 FOR q IN SELECT * FROM population_top3000_202608_202609_v1.continuation_clear_queue_v6 WHERE status='WAITING_OUTPUT' AND kind='OPERATIONAL_REVIEW' ORDER BY created_at LIMIT 4 LOOP
  src:=(q.last_receipt->>'source_task_id')::uuid;
  SELECT c.commit_id,c.committed_at,p.source_id,p.page_number,p.partition_key INTO cm FROM population_top3000_202608_202609_v1.pipeline_commit c JOIN population_top3000_202608_202609_v1.ctx02_census_page_v1 p ON p.acquire_task_id=c.task_id AND c.receipt->'immutable_output_ids' @> jsonb_build_array(p.source_id) WHERE c.task_id=src AND c.committed_at>q.claimed_at AND c.new_committed_units>0 AND p.page_number=(q.last_receipt->>'next_page')::integer ORDER BY c.committed_at LIMIT 1;
  IF FOUND AND EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task t WHERE t.family_id=q.family_id AND t.stage='NORMALIZE' AND t.partition_key=cm.partition_key||':page:'||cm.page_number::text AND t.payload->>'source_page_number'=cm.page_number::text AND t.created_at>q.claimed_at) THEN
   UPDATE population_top3000_202608_202609_v1.continuation_clear_queue_v6 SET status='COMPLETE',last_receipt=last_receipt||jsonb_build_object('useful_commit_id',cm.commit_id,'useful_committed_at',cm.committed_at,'useful_output_observed',true),updated_at=clock_timestamp() WHERE clearing_id=q.clearing_id;
   INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES('CONTINUATION_OPERATIONAL_USEFUL_OUTPUT_V6',ctl||jsonb_build_object('clearing_id',q.clearing_id,'source_task_id',src,'commit_id',cm.commit_id,'normalized_successor_exists',true));
  END IF;
 END LOOP;
 -- At most96 durable clearing groups, at most16 newly dispatched in one tick. Full source task denominator is never edited.
 FOR q IN SELECT t.family_id,t.blocker_code, population_top3000_202608_202609_v1.continuation_classify_v6(t.family_id,min(t.stage),t.blocker_code) kind,
 jsonb_agg(jsonb_build_object('task_id',t.task_id,'stage',t.stage,'partition_key',t.partition_key,'payload_sha256',encode(sha256(convert_to(t.payload::text,'UTF8')),'hex'),
 'input_versions',t.input_versions,'checkpoint',t.checkpoint,'attempt_id',t.attempt_id,'executions',t.executions,'cumulative_failures',t.cumulative_failures)
 ORDER BY t.task_id) manifest
 FROM population_top3000_202608_202609_v1.pipeline_task t WHERE t.status='BLOCKED' GROUP BY t.family_id,t.blocker_code,CASE WHEN t.family_id='CTX02' AND t.stage='ACQUIRE' AND t.blocker_code='CTX02_CENSUS_PAGE_CAP_REVIEW' THEN t.task_id ELSE NULL END ORDER BY t.family_id,t.blocker_code LIMIT 96 LOOP
  v_kind:=q.kind;manifest:=q.manifest;h:=encode(sha256(convert_to(manifest::text,'UTF8')),'hex');
  -- Reviewed policy generation gives an exact failed/old admission a new immutable clearing identity.
  policy_generation:='';
  IF q.family_id='CTX02' AND q.blocker_code='CTX02_CENSUS_PAGE_CAP_REVIEW' AND jsonb_array_length(manifest)=1 THEN
   SELECT encode(sha256(convert_to(ch.bounds::text,'UTF8')),'hex') INTO policy_generation
   FROM population_top3000_202608_202609_v1.continuation_handler_v6 ch
   WHERE ch.kind='CTX02_PAGE_ADMISSION' AND ch.enabled AND ch.bounds->>'policy_revision'='CTX02_MEASURED_REQUEST_CONTINUATION_V8'
   AND ch.bounds->'request_policies' ? (manifest->0->>'task_id');
   policy_generation:=coalesce(policy_generation,'');
  END IF;
  k:=encode(sha256(convert_to(p_owner||':'||p_fence||':'||p_stop||':'||q.family_id||':'||coalesce(q.blocker_code,'')||':'||h||CASE WHEN policy_generation<>'' THEN ':'||policy_generation ELSE '' END,'UTF8')),'hex');
  IF policy_generation<>'' THEN
   -- Never supersede claimed/unknown effects or reset attempt history. A new reviewed data policy is an explicit new generation.
   UPDATE population_top3000_202608_202609_v1.continuation_clear_queue_v6 old
   SET status='SUPERSEDED',last_receipt=coalesce(old.last_receipt,'{}'::jsonb)||jsonb_build_object('effects_known',true,'reason','REVIEWED_EXACT_REQUEST_POLICY_GENERATION_ADVANCED','successor_clearing_id',k,'successor_policy_sha256',policy_generation,'prior_attempts_preserved',old.attempts),updated_at=clock_timestamp()
   WHERE old.family_id='CTX02' AND old.blocker_code='CTX02_CENSUS_PAGE_CAP_REVIEW' AND old.clearing_id<>k
   AND jsonb_array_length(old.input_manifest)=1 AND old.input_manifest->0->>'task_id'=manifest->0->>'task_id'
   AND ((old.status='READY' AND old.attempts=0) OR (old.status='NEEDS_REVIEW' AND old.last_receipt->>'failed_handler_transaction_rolled_back'='true' AND old.last_receipt->>'effects_known'='true' AND old.last_receipt->>'policy_sha256' IS DISTINCT FROM policy_generation));
  END IF;

  UPDATE population_top3000_202608_202609_v1.continuation_clear_queue_v6 SET status='SUPERSEDED',last_receipt=coalesce(last_receipt,'{}'::jsonb)||jsonb_build_object('effects_known',true,'reason','UNCLAIMED_BLOCKER_MANIFEST_ADVANCED','successor_input_sha256',h,'implementation_executed',false),updated_at=clock_timestamp()
  WHERE status IN('READY','NEEDS_REVIEW') AND attempts=0 AND family_id=q.family_id AND blocker_code=q.blocker_code AND input_sha256<>h AND (q.family_id<>'CTX02' OR q.blocker_code<>'CTX02_CENSUS_PAGE_CAP_REVIEW' OR (jsonb_array_length(input_manifest)=1 AND input_manifest->0->>'task_id'=q.manifest->0->>'task_id'));
  IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.continuation_clear_queue_v6 WHERE clearing_id=k) THEN CONTINUE;END IF;
  IF (SELECT count(*) FROM population_top3000_202608_202609_v1.continuation_clear_queue_v6 WHERE status NOT IN('COMPLETE','SUPERSEDED'))>=96 OR inserted>=16 THEN EXIT;END IF;
  INSERT INTO population_top3000_202608_202609_v1.continuation_clear_queue_v6(clearing_id,kind,family_id,blocker_code,input_manifest,input_sha256,status,owner_id,owner_fence,stop_generation,scope_sha256,not_after,last_receipt)
  VALUES(k,v_kind,q.family_id,coalesce(q.blocker_code,'UNSPECIFIED'),manifest,h,CASE WHEN v_kind='EXTERNAL_CONDITION' THEN 'BLOCKED_EXTERNAL' ELSE 'READY' END,
  p_owner,p_fence,p_stop,ctl->>'scope_sha256',(ctl->>'not_after')::timestamptz,
  CASE WHEN v_kind='EXTERNAL_CONDITION' THEN jsonb_build_object('external_condition',q.blocker_code,'retry_allowed',false,'affected_tasks',jsonb_array_length(manifest)) END) ON CONFLICT DO NOTHING;
  GET DIAGNOSTICS changed=ROW_COUNT; IF changed=0 THEN CONTINUE;END IF;
  INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES('ENGINEERING_CLEARING_ASSIGNMENT_V6',jsonb_build_object(
    'assignment_id','clearing-v6:'||k,'clearing_id',k,'kind',v_kind,'family_id',q.family_id,'blocker_code',q.blocker_code,
    'owner_id',p_owner,'owner_fence',p_fence,'stop_generation',p_stop,'scope_sha256',ctl->>'scope_sha256','authority_event_id',9528,
    'assigned_executor','automation:6adc2c96db348191bca7cf4eacfdac32','input_sha256',h,'input_manifest',manifest,
    'exclusive_component',v_kind||':'||q.family_id,'root_will_not_concurrently_modify',true,'not_after',ctl->>'not_after',
    'allowed_repository_path','population_top3000_202608_202609/completion_20261024/continuity_v6/'||lower(q.family_id),
    'maximum_elapsed_minutes',30,'maximum_tool_calls',80,'maximum_repair_iterations',2,'maximum_production_integrations',1,
    'new_provider_requests',CASE WHEN v_kind='SOURCE_RECOVERY' THEN 32 ELSE 0 END,'maximum_source_bytes',CASE WHEN v_kind='SOURCE_RECOVERY' THEN 16777216 ELSE 0 END,
    'request_admission_required',true,'producer_role','Delegated distinct clearing executor','independent_reviewer_role','Distinct reviewer; producer cannot self-certify',
    'execution_recipe',CASE v_kind WHEN 'OPERATIONAL_REVIEW' THEN 'Measure current counters, retained hash chain, source bytes and storage; independently review finite next tranche; apply guarded admission and exact blocked acquisition requeue preserving checkpoint/counters; prove useful commit and normalization successor.'
      WHEN 'SOURCE_RECOVERY' THEN 'Read pinned source requirements; reuse exact original retained bodies first; reserve finite requests under current controls; acquire authoritative originals into private mission storage; implement or invoke reviewed numerical/vintage verification; dispatch independent review successor. Preserve original-receipt limitations.'
      WHEN 'COMPANY_RECONCILIATION' THEN 'Route to Prompt02 company producer only; collect exact company/cutoff/listing source facts from Prompt04; reconcile immutable candidate decisions with independent review; no other executor may decide membership/rank.'
      WHEN 'INDEPENDENT_VERIFICATION' THEN 'Dispatch distinct reviewer for pinned numerical/population/temporal output; run meaningful exact-scope tests; retain evidence holds; return review receipt to root for acceptance; never self-certify.'
      WHEN 'IMPLEMENTATION_REPAIR' THEN 'Inspect exact failure and pinned inputs; produce smallest scoped correction with synthetic+rollback tests; independent review before guarded installation; requeue only named defect hold; prove useful commit and successor.'
      ELSE 'Record exact external condition; do not retry or release source task.' END,
    'expected_useful_commit','Immutable output/evidence commit for pinned affected task or reviewed clearing receipt; READY/enabled/heartbeat is insufficient',
    'expected_successor','Existing pipeline automatic successor or explicitly dispatched bounded independent-review assignment',
    'protected_invariants',jsonb_build_array('Preserve owner/fence/stop and original deadline','Preserve all source rows, denominators, costs, attempts and failures','No unrelated scanner/trading/D06 generation','Keep provider bytes private','No family acceptance or serving promotion')))
  RETURNING event_id INTO ev;
  UPDATE population_top3000_202608_202609_v1.continuation_clear_queue_v6 SET assignment_event_id=ev WHERE clearing_id=k;
  inserted:=inserted+1;IF v_kind='EXTERNAL_CONDITION' THEN external_count:=external_count+1;END IF;
 END LOOP;

 -- Exactly one registered, independently reviewed operational helper per pass. No generic source-task hold clearing.
 SELECT * INTO handler FROM population_top3000_202608_202609_v1.continuation_handler_v6 WHERE kind='CTX02_PAGE_ADMISSION' AND enabled;
 IF FOUND THEN
 IF handler.function_sha256 IS DISTINCT FROM encode(sha256(convert_to(pg_get_functiondef(handler.function_oid),'UTF8')),'hex') THEN RAISE EXCEPTION 'CONTINUATION_OPERATIONAL_HANDLER_CHANGED';END IF;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_event e WHERE e.event_id=handler.review_event_id AND e.details->>'approved'='true' AND e.details->>'handler_function_sha256'=handler.function_sha256 AND e.details->>'owner_id'=p_owner AND e.details->>'owner_fence'=p_fence::text AND e.details->>'stop_generation'=p_stop::text AND e.details->>'scope_sha256'=ctl->>'scope_sha256' AND nullif(e.details->>'reviewer','') IS NOT NULL AND nullif(e.details->>'producer','') IS NOT NULL AND e.details->>'reviewer' IS DISTINCT FROM e.details->>'producer' AND e.details->>'handler_bounds_sha256'=encode(sha256(convert_to(handler.bounds::text,'UTF8')),'hex')) THEN RAISE EXCEPTION 'CONTINUATION_OPERATIONAL_REVIEW_REQUIRED';END IF;
 SELECT * INTO q FROM population_top3000_202608_202609_v1.continuation_clear_queue_v6 WHERE status='READY' AND kind='OPERATIONAL_REVIEW' AND family_id='CTX02' AND blocker_code='CTX02_CENSUS_PAGE_CAP_REVIEW' AND jsonb_array_length(input_manifest)=1 AND handler.bounds->'request_policies' ? (input_manifest->0->>'task_id') AND not_after>clock_timestamp() AND attempts<max_attempts
 AND NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.continuation_clear_queue_v6 other WHERE other.status IN('CLAIMED','WAITING_OUTPUT') AND other.clearing_id<>continuation_clear_queue_v6.clearing_id AND jsonb_array_length(other.input_manifest)=1 AND other.input_manifest->0->>'task_id'=continuation_clear_queue_v6.input_manifest->0->>'task_id')
 ORDER BY created_at,clearing_id LIMIT 1 FOR UPDATE;
 IF FOUND AND NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE status='LEASED')
 AND NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_provider_lease WHERE released_at IS NULL)
 AND NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.connector_probe_lease_v1 WHERE released_at IS NULL) THEN
 src:=(q.input_manifest->0->>'task_id')::uuid;
 IF jsonb_array_length(q.input_manifest)<>1 THEN RAISE EXCEPTION 'CONTINUATION_EXACT_SOURCE_TASK_REQUIRED';END IF;
 SELECT format('%I.%I',n.nspname,p.proname) INTO fn FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE p.oid=handler.function_oid;
 BEGIN
 EXECUTE format('select %s($1,$2,$3,$4)',fn) INTO ans USING p_owner,p_fence,p_stop,jsonb_build_object('source_task_id',src,'clearing_id',q.clearing_id,'bounds',handler.bounds);
 IF ans->>'state' IS DISTINCT FROM 'CLEARED' THEN RAISE EXCEPTION 'CONTINUATION_HANDLER_NOT_CLEARED';END IF;
 UPDATE population_top3000_202608_202609_v1.continuation_clear_queue_v6 SET status='WAITING_OUTPUT',attempts=attempts+1,claimed_at=clock_timestamp(),last_receipt=ans||jsonb_build_object('independent_review_event_id',handler.review_event_id,'policy_sha256',encode(sha256(convert_to(handler.bounds::text,'UTF8')),'hex')),updated_at=clock_timestamp() WHERE clearing_id=q.clearing_id;
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES('CONTINUATION_OPERATIONAL_REQUEUED_V6',ctl||jsonb_build_object('clearing_id',q.clearing_id,'receipt',ans,'useful_output_observed',false));
 EXCEPTION WHEN OTHERS THEN
 UPDATE population_top3000_202608_202609_v1.continuation_clear_queue_v6 SET status='NEEDS_REVIEW',attempts=attempts+1,last_receipt=jsonb_build_object('recoverable_condition',left(SQLERRM,400),'execution_recipe','Measure resource/counter/lease evidence; use finite reviewed admission or exact defect correction, then explicit guarded requeue; do not bypass the failed guard','effects_known',true,'failed_handler_transaction_rolled_back',true,'policy_sha256',encode(sha256(convert_to(handler.bounds::text,'UTF8')),'hex')),updated_at=clock_timestamp() WHERE clearing_id=q.clearing_id;
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES('CONTINUATION_OPERATIONAL_REVIEW_HELD_V6',ctl||jsonb_build_object('clearing_id',q.clearing_id,'error',left(SQLERRM,400),'source_task_unchanged',true,'policy_sha256',encode(sha256(convert_to(handler.bounds::text,'UTF8')),'hex'),'required_successor',CASE WHEN SQLERRM='OPTIONS_V8_MEASURED_RESOURCE_REVIEW_REQUIRED' THEN 'RESOURCE_CAPACITY_PROBE_AND_MEASURED_ADMISSION' ELSE 'EXACT_GUARDED_DEFECT_OR_EVIDENCE_REVIEW' END));
 END;
 END IF;END IF;

 -- A separately reviewed CTX07 helper materializes exact-source parser and independent-review successors without source downloads.
 SELECT * INTO handler FROM population_top3000_202608_202609_v1.continuation_handler_v6 WHERE kind='CTX07_SOURCE_SUCCESSORS' AND enabled;
 IF FOUND THEN
  IF handler.function_sha256 IS DISTINCT FROM encode(sha256(convert_to(pg_get_functiondef(handler.function_oid),'UTF8')),'hex') THEN RAISE EXCEPTION 'CONTINUATION_SOURCE_SUCCESSOR_HANDLER_CHANGED';END IF;
  IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_event e WHERE e.event_id=handler.review_event_id AND e.details->>'approved'='true' AND e.details->>'handler_function_sha256'=handler.function_sha256 AND e.details->>'owner_id'=p_owner AND e.details->>'owner_fence'=p_fence::text AND e.details->>'stop_generation'=p_stop::text AND e.details->>'scope_sha256'=ctl->>'scope_sha256' AND nullif(e.details->>'producer','') IS NOT NULL AND nullif(e.details->>'reviewer','') IS NOT NULL AND e.details->>'reviewer' IS DISTINCT FROM e.details->>'producer') THEN RAISE EXCEPTION 'CONTINUATION_SOURCE_SUCCESSOR_REVIEW_REQUIRED';END IF;
  IF (SELECT count(*) FROM population_top3000_202608_202609_v1.pipeline_event e WHERE e.event='CONTINUATION_SOURCE_SUCCESSOR_HANDLER_FAILED_V7' AND e.details->>'handler_sha256'=handler.function_sha256)<3 THEN
   SELECT format('%I.%I',n.nspname,p.proname) INTO fn FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE p.oid=handler.function_oid;
   BEGIN
    EXECUTE format('select %s($1,$2,$3)',fn) INTO ans USING p_owner,p_fence,p_stop;
   EXCEPTION WHEN OTHERS THEN
    INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES('CONTINUATION_SOURCE_SUCCESSOR_HANDLER_FAILED_V7',ctl||jsonb_build_object('handler_sha256',handler.function_sha256,'error',left(SQLERRM,400),'effects_known',true,'helper_mutations_rolled_back',true,'attempt_limit',3,'existing_source_clearing_assignment_remains_eligible',true));
   END;
  END IF;
 END IF;
 -- Expired engineering claims are executable effect-reconciliation work, never automatic lease stealing.
 FOR q IN SELECT e.event_id,e.details FROM population_top3000_202608_202609_v1.pipeline_event e WHERE e.event='ENGINEERING_ASSIGNMENT_CLAIMED_V5'
  AND e.details->>'owner_id'=p_owner AND e.details->>'owner_fence'=p_fence::text AND e.details->>'stop_generation'=p_stop::text
  AND (e.details->>'expires_at')::timestamptz<=clock_timestamp()
  AND NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_event r WHERE r.event IN('ENGINEERING_ASSIGNMENT_COMPLETED_V5','ENGINEERING_ASSIGNMENT_EFFECTS_RECONCILED_RELEASE_V5') AND r.details->>'claim_event_id'=e.event_id::text AND r.details->>'effects_known'='true')
  ORDER BY e.event_id LIMIT 4 LOOP
  manifest:=jsonb_build_array(jsonb_build_object('claim_event_id',q.event_id,'claim_details_sha256',encode(sha256(convert_to(q.details::text,'UTF8')),'hex')));h:=encode(sha256(convert_to(manifest::text,'UTF8')),'hex');k:='claim:'||q.event_id;
  IF (SELECT count(*) FROM population_top3000_202608_202609_v1.continuation_clear_queue_v6 WHERE status NOT IN('COMPLETE','SUPERSEDED'))>=96 THEN EXIT;END IF;
  INSERT INTO population_top3000_202608_202609_v1.continuation_clear_queue_v6(clearing_id,kind,family_id,blocker_code,input_manifest,input_sha256,status,owner_id,owner_fence,stop_generation,scope_sha256,not_after)
  VALUES(k,'CLAIM_RECONCILIATION','ENGINEERING','EXPIRED_CLAIM_EFFECTS_UNRECONCILED',manifest,h,'READY',p_owner,p_fence,p_stop,ctl->>'scope_sha256',(ctl->>'not_after')::timestamptz) ON CONFLICT DO NOTHING;
 END LOOP;

 -- Expired executor claims require effect reconciliation. They never prevent a disjoint family claim.
 FOR q IN SELECT * FROM population_top3000_202608_202609_v1.continuation_clear_queue_v6 WHERE status='CLAIMED' AND claim_expires_at<=clock_timestamp() AND kind<>'CLAIM_RECONCILIATION' ORDER BY claimed_at LIMIT 4 LOOP
 manifest:=jsonb_build_array(jsonb_build_object('clearing_claim_id',q.clearing_id,'claimed_at',q.claimed_at,'input_sha256',q.input_sha256,'required_action','Reconcile immutable Git/deployment/database/provider effects; release only under current controller with exact evidence; unknown effects stay held'));
 h:=encode(sha256(convert_to(manifest::text,'UTF8')),'hex');k:='clear-claim:'||q.clearing_id;
 IF (SELECT count(*) FROM population_top3000_202608_202609_v1.continuation_clear_queue_v6 WHERE status NOT IN('COMPLETE','SUPERSEDED'))<96 THEN
 INSERT INTO population_top3000_202608_202609_v1.continuation_clear_queue_v6(clearing_id,kind,family_id,blocker_code,input_manifest,input_sha256,status,owner_id,owner_fence,stop_generation,scope_sha256,not_after)
 VALUES(k,'CLAIM_RECONCILIATION','ENGINEERING','EXPIRED_V6_CLAIM_EFFECTS_UNRECONCILED',manifest,h,'READY',p_owner,p_fence,p_stop,ctl->>'scope_sha256',(ctl->>'not_after')::timestamptz) ON CONFLICT DO NOTHING;END IF;
 END LOOP;
 -- Proactive capacity review has a concrete executable measurement task before an admitted source tranche hits shared limits.
 SELECT coalesce(sum(pg_total_relation_size(c.oid)),0) INTO usage_bytes FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='population_top3000_202608_202609_v1' AND c.relkind IN('r','m');
 IF usage_bytes>(pc.budget_evidence#>>'{resource_limits,max_relation_bytes}')::bigint*0.88 OR ac.stored_bytes+ac.reserved_bytes>ac.max_source_bytes*0.88 OR ac.provider_requests>ac.max_provider_requests*0.88
 OR ac.http_invocations>ac.max_http_invocations*0.88 OR pc.tick_count>pc.max_ticks*0.88 OR ac.dispatch_calls>ac.max_dispatch_calls*0.88
 OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.continuation_clear_queue_v6 cq WHERE cq.kind='OPERATIONAL_REVIEW' AND cq.family_id='CTX02' AND cq.status='NEEDS_REVIEW' AND cq.last_receipt->>'effects_known'='true' AND cq.last_receipt->>'failed_handler_transaction_rolled_back'='true' AND cq.last_receipt->>'recoverable_condition'='OPTIONS_V8_MEASURED_RESOURCE_REVIEW_REQUIRED')
 OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.continuation_handler_v6 ch WHERE ch.kind='CTX02_PAGE_ADMISSION' AND ch.enabled AND ch.bounds->>'policy_revision'='CTX02_MEASURED_REQUEST_CONTINUATION_V8'
 AND (usage_bytes>(ch.bounds#>>'{global_ceiling,relation_bytes}')::bigint*0.88 OR ac.stored_bytes+ac.reserved_bytes>(ch.bounds#>>'{global_ceiling,source_bytes}')::bigint*0.88 OR ac.provider_requests>(ch.bounds#>>'{global_ceiling,provider_requests}')::bigint*0.88 OR ac.http_invocations>(ch.bounds#>>'{global_ceiling,http_invocations}')::bigint*0.88)) THEN
 manifest:=jsonb_build_array(jsonb_build_object('resource_limits',pc.budget_evidence->'resource_limits','source_limit',ac.max_source_bytes,'provider_limit',ac.max_provider_requests,'original_deadline',pc.deadline,'max_ticks',pc.max_ticks,'max_dispatch_calls',ac.max_dispatch_calls,'max_http_invocations',ac.max_http_invocations,'options_policy_sha256',(SELECT encode(sha256(convert_to(bounds::text,'UTF8')),'hex') FROM population_top3000_202608_202609_v1.continuation_handler_v6 WHERE kind='CTX02_PAGE_ADMISSION'),'options_reviewed_global_ceilings',(SELECT bounds->'global_ceiling' FROM population_top3000_202608_202609_v1.continuation_handler_v6 WHERE kind='CTX02_PAGE_ADMISSION')));
 h:=encode(sha256(convert_to(manifest::text,'UTF8')),'hex');k:='resource:'||h;
 IF (SELECT count(*) FROM population_top3000_202608_202609_v1.continuation_clear_queue_v6 WHERE status NOT IN('COMPLETE','SUPERSEDED'))<96 THEN
 INSERT INTO population_top3000_202608_202609_v1.continuation_clear_queue_v6(clearing_id,kind,family_id,blocker_code,input_manifest,input_sha256,status,owner_id,owner_fence,stop_generation,scope_sha256,not_after,last_receipt)
 VALUES(k,'OPERATIONAL_REVIEW','RESOURCE','RESOURCE_CAPACITY_PROBE_AND_MEASURED_ADMISSION',manifest,h,'READY',p_owner,p_fence,p_stop,ctl->>'scope_sha256',(ctl->>'not_after')::timestamptz,
 jsonb_build_object('measured_relation_bytes',usage_bytes,'source_bytes',ac.stored_bytes,'reserved_bytes',ac.reserved_bytes,'http_invocations',ac.http_invocations,'tick_count',pc.tick_count,'dispatch_calls',ac.dispatch_calls,'execution_recipe','Inspect exact reviewed options policy and live ceilings; use a newly independently reviewed policy data generation to consume increased headroom without a code patch or resetting failed clearing attempts. Inspect existing resource probe and use a separately versioned finite slot for fresh read-only project capacity; calculate finite new source/relation/call allowance with at least20GB other-work headroom; distinct reviewer and counter-preservation rollback before controller admission. Do not reset original probe slots, counters or deadline.')) ON CONFLICT DO NOTHING;END IF;END IF;
 -- Every special clearing row is discoverable as a runnable assignment event, including effect-reconciliation and resource review.
 FOR q IN SELECT * FROM population_top3000_202608_202609_v1.continuation_clear_queue_v6 WHERE assignment_event_id IS NULL ORDER BY created_at LIMIT 8 LOOP
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES('ENGINEERING_CLEARING_ASSIGNMENT_V6',ctl||jsonb_build_object('assignment_id','clearing-v6:'||q.clearing_id,'clearing_id',q.clearing_id,'kind',q.kind,'family_id',q.family_id,'blocker_code',q.blocker_code,'input_manifest',q.input_manifest,'input_sha256',q.input_sha256,'assigned_executor',q.assigned_executor,'maximum_elapsed_minutes',30,'maximum_tool_calls',80,'maximum_repair_iterations',2,'maximum_production_integrations',1,'producer_role','Distinct clearing executor','independent_reviewer_role','Distinct reviewer','exclusive_component',q.kind||':'||q.family_id,'root_will_not_concurrently_modify',true,'execution_recipe',coalesce(q.last_receipt->>'execution_recipe',q.input_manifest->0->>'required_action','Reconcile exact claim effects using immutable Git/deployment/data receipts; current controller alone records release without data acceptance'),'expected_useful_commit','Pinned effect reconciliation or measured operational admission evidence; not an enabled scheduler','expected_successor','Exact authorized implementation or data task under existing mission','new_provider_requests',0,'new_http_invocations',CASE WHEN q.family_id='RESOURCE' THEN 1 ELSE 0 END,'maximum_source_bytes',0,'preserve_counters_deadline_and_holds',true)) RETURNING event_id INTO ev;
 UPDATE population_top3000_202608_202609_v1.continuation_clear_queue_v6 SET assignment_event_id=ev WHERE clearing_id=q.clearing_id;
 END LOOP;
 last_progress:=(ctl->>'last_useful_progress_at')::timestamptz;
 IF last_progress IS NULL OR last_progress<clock_timestamp()-interval '10minutes' THEN
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_event WHERE event='CONTINUATION_NO_USEFUL_PROGRESS_V6' AND at>clock_timestamp()-interval '10minutes') THEN
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES('CONTINUATION_NO_USEFUL_PROGRESS_V6',ctl||jsonb_build_object('scheduler_is_not_progress',true,'ready_clearing_groups',(SELECT count(*) FROM population_top3000_202608_202609_v1.continuation_clear_queue_v6 WHERE status='READY'),'ready_data_tasks',(SELECT count(*) FROM population_top3000_202608_202609_v1.pipeline_task WHERE status='READY'),'active_attempts',(SELECT count(*) FROM population_top3000_202608_202609_v1.pipeline_task WHERE status='LEASED')));END IF;END IF;
 RETURN jsonb_build_object('state','CLEARING_WORK_DISPATCHED','new_groups',inserted,'new_external_groups',external_count,'ready_groups',(SELECT count(*) FROM population_top3000_202608_202609_v1.continuation_clear_queue_v6 WHERE status='READY'));
END;$function$;

REVOKE ALL ON FUNCTION population_top3000_202608_202609_v1.continuation_dispatch_v6(text,bigint,bigint) FROM PUBLIC,anon,authenticated;
DO $install$
DECLARE rev bigint;tst bigint;integration bigint;sha text;dsha text;ctl jsonb;
 policy jsonb:=$policy${
  "policy_revision": "CTX02_MEASURED_REQUEST_CONTINUATION_V8",
  "maximum_new_pages": 50,
  "global_ceiling": {
    "provider_requests": 10000,
    "http_invocations": 10055,
    "source_bytes": 1000000000,
    "relation_bytes": 8000000000
  },
  "request_policies": {
    "d377d980-5dbd-4276-a2c9-d5638c60b668": {
      "request_key": "ctx02-census-v1:0869cb2de1b0fe2ad0789fd4498df455de76ad92aa32805c36417a4cf87bb5cd",
      "manifest_sha256": "0869cb2de1b0fe2ad0789fd4498df455de76ad92aa32805c36417a4cf87bb5cd",
      "input_versions_sha256": "0fc0c143e7e26d60c08f63180b3ede47bab24d311e7a91977b251c747e1b3f52",
      "payload_sha256": "da7b69b5121893f463d6a31c311d70059eb29b7cf5b7b60ff38a8ec81b2b007d"
    },
    "da79b746-8550-41c9-a6f0-3d87f4e352fd": {
      "request_key": "ctx02-census-v1:082572b99c04ab298a3794bc6013aca7114cd07f8c1d78564e6ad6b4209a3ef9",
      "manifest_sha256": "082572b99c04ab298a3794bc6013aca7114cd07f8c1d78564e6ad6b4209a3ef9",
      "input_versions_sha256": "c4d9c4f89fe47bc721fa23f0663ae3525adb56c8a858657489b5ced667040af8",
      "payload_sha256": "ff93c5b0ce687a432d4c3bac7c8da163cb2f51f030528c2ea6bb4d2aaa805cc2"
    },
    "f55551ce-df3f-4726-8c16-855b93a92d21": {
      "request_key": "ctx02-census-v1:b48c9de45f2af8c141349682d83218995fbf18544fcc24f035a90e247435dc52",
      "manifest_sha256": "b48c9de45f2af8c141349682d83218995fbf18544fcc24f035a90e247435dc52",
      "input_versions_sha256": "c4d9c4f89fe47bc721fa23f0663ae3525adb56c8a858657489b5ced667040af8",
      "payload_sha256": "abb31160eb7d5703e88b346d3dec3e381772f89d9bb82a751c661e286a4ab56e"
    }
  }
}
$policy$::jsonb;
 review jsonb:=$review${
  "approved": true,
  "approval_scope": "CODE_EXACT_REQUEST_POLICY_AND_ACTUAL_ROLLBACK_APPROVED_PENDING_REAL_USEFUL_OUTPUT",
  "reviewer": "root/continuation_design_review",
  "producer": "root/continuation_implementation",
  "assignment_event_id": 27678,
  "claim_event_id": 27679,
  "file_sha256": {
    "helper_v8.sql": "0c7bec46eebcbae2af3d24bbac9e4710ff77f8405dc9f58a879d9cae27215ec4",
    "dispatcher_v8.sql": "6482f929e14dde49de6ae88704b0dd7b7db1578a908536c9ee81d8c343791574",
    "policy_bounds_v8.json": "15b83d68a6e8009884d5345d6bc423b4e9ec61bfb1c19a41e10eb64359b98548"
  },
  "expected_database_function_sha256": {
    "helper": "7b68f7313f94151d9c8a5590c2b80a5fd74a6c784e5c9a5f4b6c19ceee38252e",
    "dispatcher": "79bf0f1f700d1256025f3e5df3a485085a3acc01ee63b66e8c59a9ad1d677f2c"
  },
  "production_mutations_by_reviewer": 0,
  "independently_checked": [
    "All three exact task payload/input-version hashes match current live database",
    "No1247/1197/997/947 request-page literal remains in helper or dispatcher",
    "Caller/registry/code/review policy hashes bound, explicit exact request allowlist",
    "Current owner/fence/stop/scope, finite policy and live caps checked",
    "Retained first URL, nonterminal last URL, contiguous full chain hashes and byte lengths verified",
    "Expiry-only and call-only do not add page allowance; maximum50 newpages/calls pertranche",
    "Reserved source bytes, global/provider/HTTP/relation/tick/dispatch finite headroom preserved",
    "Old task checkpoint/attempt/failure/history preserved; exact original task requeued only after contextREADY",
    "Known failed old-policy rows may be explicitly superseded without attempt reset; active/unknown claimed effects remain",
    "Resource failure dispatches deduplicated executable RESOURCE assignment, including tick/HTTP/dispatch exhaustion",
    "Existing generic claimant handles NEEDS_REVIEW; no new claim bypass or source retry loop"
  ],
  "changed_risk_tests_reviewed": [
    "Owner/stop",
    "Unknown provider outcome",
    "Source chain tamper",
    "Input manifest pin tamper under valid fixture policy hash",
    "Registry bound hash tamper with matching callerbounds",
    "Callerbound mismatch",
    "Dependency preimage change",
    "Relation/tick/reserved source guard",
    "Expiry-only ceiling preservation",
    "Call-only page preservation",
    "Duplicate rejection",
    "Rollback-only1248to1298 generic continuation",
    "September exact requests via helper and dispatcher",
    "Resource failure emits exact executable assignment",
    "WAITING_OUTPUT until real source/successor proof"
  ],
  "integration_conditions": [
    "Current root claim, controls and exact live preimages still match",
    "Actual PostgreSQL rollback must pass and all synthetic fixture changes must disappear",
    "Register exact canonical bounds SHA in independent review event",
    "Requalify changed recovery manifest without resetting counters/restarts",
    "Observe real scheduled source+normalization commits and independently verify exact bytes/rows before declaring runtime completion"
  ],
  "not_certified": [
    "Full options population",
    "All24data family completion",
    "Historical original receipt qualification",
    "Unlimited capacity or future source admission"
  ],
  "canonical_policy_bounds_sha256": "b38e09dc71a4dcaef996a8b062dee353eae67a44d8f5e13b2362b0857ba09f51",
  "canonical_policy_hash_verified_via_postgres": true,
  "runtime_review": {
    "result": "PASS_POLICY_CONTINUATION_ROLLBACK_V8",
    "tests_passed": 19,
    "actual_provider_calls": 0,
    "synthetic_changes_rolled_back": true,
    "test_names": [
      "call_only_preserves_page_ceiling",
      "caller_bounds",
      "chain",
      "dispatcher_resource_successor",
      "dispatcher_second_request",
      "dispatcher_unknown_lease",
      "duplicate",
      "expiry_only_preserves_ceiling",
      "manifest",
      "owner",
      "post1247_original",
      "preimage",
      "registry_bounds",
      "reserved",
      "resource",
      "september_positive",
      "stop",
      "tick",
      "unknown_provider"
    ],
    "receipt_file_sha256": "016be5164c76622d3a69b109ed63030faabf7c69b49487f5c0d5dce04a1598e8",
    "independent_harness_source_review": true,
    "actual_execution_performed_by": "root",
    "reviewer_reused_valid_actual_evidence": true,
    "test_only_failure_history_preserved": true,
    "beyond_old_ceiling_test": "1248 retained fixture pages to1298, all synthetic additions rollback-only"
  },
  "integration_sql_review": {
    "header_reviewed": true,
    "footer_reviewed": true,
    "root_claim": 27679,
    "pins_function_and_policy_hashes": true,
    "unchanged_registry_preimage_required": true,
    "counter_or_deadline_changes": false,
    "direct_source_calls_or_requeue_in_install": false,
    "actual_commit_placeholder_requires_published_code_revision": true
  }
}
$review$::jsonb;
 runtime jsonb:=$runtime${
  "result": "PASS_POLICY_CONTINUATION_ROLLBACK_V8",
  "assignment_event_id": 27678,
  "claim_event_id": 27679,
  "actual_provider_calls": 0,
  "test_mutations_rolled_back": true,
  "tests_passed": 19,
  "receipts": [
    {
      "test_name": "call_only_preserves_page_ceiling",
      "receipt": {
        "state": "CLEARED",
        "reason": "CALL_ALLOWANCE",
        "next_page": 12,
        "request_key": "ctx02-census-v1:082572b99c04ab298a3794bc6013aca7114cd07f8c1d78564e6ad6b4209a3ef9",
        "policy_sha256": "b38e09dc71a4dcaef996a8b062dee353eae67a44d8f5e13b2362b0857ba09f51",
        "source_task_id": "da79b746-8550-41c9-a6f0-3d87f4e352fd",
        "expected_successor": "REAL_ACQUIRE_COMMIT_THEN_NORMALIZE",
        "admitted_call_ceiling": 21,
        "admitted_page_ceiling": 21,
        "useful_output_observed": false,
        "estimated_additional_bytes": 42584040
      }
    },
    {
      "test_name": "caller_bounds",
      "receipt": {
        "expected": "OPTIONS_V8_EXACT_REVIEWED_POLICY_REQUIRED",
        "rejected": true
      }
    },
    {
      "test_name": "chain",
      "receipt": {
        "expected": "OPTIONS_V8_RETAINED_CHAIN_CHANGED",
        "rejected": true
      }
    },
    {
      "test_name": "dispatcher_resource_successor",
      "receipt": {
        "no_source_retry": true,
        "exact_source_clearing": "252a3c2cb06e3168047ec39e2b279fd7eaae030d32afd37a5378d226212bde06",
        "executable_resource_assignment": 28654
      }
    },
    {
      "test_name": "dispatcher_second_request",
      "receipt": {
        "state": "CLEARING_WORK_DISPATCHED",
        "new_groups": 2,
        "ready_groups": 64,
        "exact_source_ready": true,
        "new_external_groups": 0,
        "waiting_for_real_output": true
      }
    },
    {
      "test_name": "dispatcher_unknown_lease",
      "receipt": {
        "attempts_preserved": true,
        "source_task_still_blocked": true
      }
    },
    {
      "test_name": "duplicate",
      "receipt": {
        "rejected": true
      }
    },
    {
      "test_name": "expiry_only_preserves_ceiling",
      "receipt": {
        "state": "CLEARED",
        "reason": "EXPIRY_ONLY",
        "next_page": 12,
        "request_key": "ctx02-census-v1:082572b99c04ab298a3794bc6013aca7114cd07f8c1d78564e6ad6b4209a3ef9",
        "policy_sha256": "b38e09dc71a4dcaef996a8b062dee353eae67a44d8f5e13b2362b0857ba09f51",
        "source_task_id": "da79b746-8550-41c9-a6f0-3d87f4e352fd",
        "expected_successor": "REAL_ACQUIRE_COMMIT_THEN_NORMALIZE",
        "admitted_call_ceiling": 21,
        "admitted_page_ceiling": 21,
        "useful_output_observed": false,
        "estimated_additional_bytes": 42584040
      }
    },
    {
      "test_name": "manifest",
      "receipt": {
        "expected": "OPTIONS_V8_PINNED_REQUEST_CHANGED",
        "rejected": true
      }
    },
    {
      "test_name": "owner",
      "receipt": {
        "expected": "OPTIONS_V8_CURRENT_AUTHORITY_REQUIRED",
        "rejected": true
      }
    },
    {
      "test_name": "post1247_original",
      "receipt": {
        "state": "CLEARED",
        "reason": "PAGE_ALLOWANCE",
        "next_page": 1249,
        "request_key": "ctx02-census-v1:0869cb2de1b0fe2ad0789fd4498df455de76ad92aa32805c36417a4cf87bb5cd",
        "policy_sha256": "b38e09dc71a4dcaef996a8b062dee353eae67a44d8f5e13b2362b0857ba09f51",
        "source_task_id": "d377d980-5dbd-4276-a2c9-d5638c60b668",
        "external_requests": 0,
        "expected_successor": "REAL_ACQUIRE_COMMIT_THEN_NORMALIZE",
        "admitted_call_ceiling": 1298,
        "admitted_page_ceiling": 1298,
        "useful_output_observed": false,
        "estimated_additional_bytes": 151967833,
        "synthetic_rows_rollback_only": 124
      }
    },
    {
      "test_name": "preimage",
      "receipt": {
        "expected": "OPTIONS_V8_REVIEWED_PREIMAGE_CHANGED",
        "rejected": true
      }
    },
    {
      "test_name": "registry_bounds",
      "receipt": {
        "expected": "OPTIONS_V8_EXACT_REVIEWED_POLICY_REQUIRED",
        "rejected": true
      }
    },
    {
      "test_name": "reserved",
      "receipt": {
        "expected": "OPTIONS_V8_MEASURED_RESOURCE_REVIEW_REQUIRED",
        "rejected": true
      }
    },
    {
      "test_name": "resource",
      "receipt": {
        "expected": "OPTIONS_V8_MEASURED_RESOURCE_REVIEW_REQUIRED",
        "rejected": true
      }
    },
    {
      "test_name": "september_positive",
      "receipt": {
        "state": "CLEARED",
        "reason": "PAGE_ALLOWANCE",
        "next_page": 12,
        "request_key": "ctx02-census-v1:082572b99c04ab298a3794bc6013aca7114cd07f8c1d78564e6ad6b4209a3ef9",
        "policy_sha256": "b38e09dc71a4dcaef996a8b062dee353eae67a44d8f5e13b2362b0857ba09f51",
        "source_task_id": "da79b746-8550-41c9-a6f0-3d87f4e352fd",
        "expected_successor": "REAL_ACQUIRE_COMMIT_THEN_NORMALIZE",
        "admitted_call_ceiling": 61,
        "admitted_page_ceiling": 61,
        "useful_output_observed": false,
        "estimated_additional_bytes": 145811333,
        "task_and_acquisition_counters_preserved": true
      }
    },
    {
      "test_name": "stop",
      "receipt": {
        "expected": "OPTIONS_V8_CURRENT_AUTHORITY_REQUIRED",
        "rejected": true
      }
    },
    {
      "test_name": "tick",
      "receipt": {
        "expected": "OPTIONS_V8_MEASURED_RESOURCE_REVIEW_REQUIRED",
        "rejected": true
      }
    },
    {
      "test_name": "unknown_provider",
      "receipt": {
        "expected": "OPTIONS_V8_ACTIVE_OR_UNRECONCILED_EFFECT",
        "rejected": true
      }
    }
  ],
  "prior_failed_harness_attempts": {
    "count": 4,
    "all_rolled_back": true,
    "causes": [
      "manifest fixture hit table check before helper",
      "BLOCKED fixture lacked required resumption_condition",
      "fair dispatcher selected stale Sep1 queue after direct helper test; diagnostic then fixture acknowledgment added"
    ],
    "production_code_changed": false
  }
}
$runtime$::jsonb;
BEGIN
 ctl:=population_top3000_202608_202609_v1.continuation_guard_v6('top3000-completion-20261024-root-5db48b32d103',3,1);
 sha:=encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.ctx02_clear_page_admission_v6(text,bigint,bigint,jsonb)'::regprocedure),'UTF8')),'hex');
 dsha:=encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.continuation_dispatch_v6(text,bigint,bigint)'::regprocedure),'UTF8')),'hex');
 IF sha<>'7b68f7313f94151d9c8a5590c2b80a5fd74a6c784e5c9a5f4b6c19ceee38252e' OR dsha<>'79bf0f1f700d1256025f3e5df3a485085a3acc01ee63b66e8c59a9ad1d677f2c' THEN RAISE EXCEPTION 'OPTIONS_V8_POSTIMAGE_MISMATCH';END IF;
 IF encode(sha256(convert_to(policy::text,'UTF8')),'hex')<>'b38e09dc71a4dcaef996a8b062dee353eae67a44d8f5e13b2362b0857ba09f51' OR review->>'approved' IS DISTINCT FROM 'true' OR review->>'reviewer' IS DISTINCT FROM 'root/continuation_design_review' OR review->>'producer' IS DISTINCT FROM 'root/continuation_implementation' OR runtime->>'result' IS DISTINCT FROM 'PASS_POLICY_CONTINUATION_ROLLBACK_V8' THEN RAISE EXCEPTION 'OPTIONS_V8_ACTUAL_REVIEW_AND_TEST_REQUIRED';END IF;
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES('ENGINEERING_INDEPENDENT_REVIEW_V5',ctl||review||jsonb_build_object('assignment_event_id',27678,'claim_event_id',27679,'handler_function_sha256',sha,'dispatcher_function_sha256',dsha,'handler_bounds_sha256',encode(sha256(convert_to(policy::text,'UTF8')),'hex'),'code_commit','59227a7824298fb6ba6e09d7f1a87ddbac0bde8e')) RETURNING event_id INTO rev;
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES('ENGINEERING_ACTUAL_ROLLBACK_TEST_V5',ctl||runtime||jsonb_build_object('assignment_event_id',27678,'claim_event_id',27679,'code_commit','59227a7824298fb6ba6e09d7f1a87ddbac0bde8e')) RETURNING event_id INTO tst;
 UPDATE population_top3000_202608_202609_v1.continuation_handler_v6 SET function_sha256=sha,review_event_id=rev,bounds=policy WHERE kind='CTX02_PAGE_ADMISSION' AND function_sha256='3337167897438f07077b666c57f252be90d6ffbf2f54c2e65a9fd9fd4eee430b' AND review_event_id=27067 AND bounds->>'absolute_page_ceiling'='1247';
 IF NOT FOUND THEN RAISE EXCEPTION 'OPTIONS_V8_REGISTRY_PREIMAGE_CHANGED';END IF;
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES('ENGINEERING_INTEGRATED_V5',ctl||jsonb_build_object('assignment_event_id',27678,'claim_event_id',27679,'code_commit','59227a7824298fb6ba6e09d7f1a87ddbac0bde8e','independent_review_event_id',rev,'actual_test_event_id',tst,'handler_sha256',sha,'dispatcher_sha256',dsha,'policy_sha256',encode(sha256(convert_to(policy::text,'UTF8')),'hex'),'effects_known',true,'state','INTEGRATED_WAITING_FOR_AUTOMATIC_TRANCHE_DATA_COMMIT','exact_requests',3,'maximum_new_pages',50,'counters_and_original_deadline_preserved',true,'family_accepted',false)) RETURNING event_id INTO integration;
END;$install$;
SELECT event_id,at,event,details FROM population_top3000_202608_202609_v1.pipeline_event WHERE event='ENGINEERING_INTEGRATED_V5' AND details->>'claim_event_id'='27679' ORDER BY event_id DESC LIMIT 1;
COMMIT;


