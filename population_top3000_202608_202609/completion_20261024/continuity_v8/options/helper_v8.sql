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
