-- Reviewed bounded controller clearing handler. No provider calls; consumes retained proof.
CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.ctx02_clear_page_admission_v6(p_owner text,p_fence bigint,p_stop bigint,p_item jsonb)
RETURNS jsonb LANGUAGE plpgsql SET search_path=pg_catalog AS $restore$
DECLARE a population_top3000_202608_202609_v1.acquisition_control%rowtype;
 p population_top3000_202608_202609_v1.pipeline_control%rowtype;
 t population_top3000_202608_202609_v1.pipeline_task%rowtype;
 before_t jsonb; after_t jsonb; before_a jsonb; before_p jsonb; result jsonb;
 used_calls bigint; pages bigint; bad bigint; relbytes bigint; maxwire bigint; recordbytes bigint; estimated bigint; wake jsonb; normalized bigint; new_ceiling integer;
 req constant text:='ctx02-census-v1:0869cb2de1b0fe2ad0789fd4498df455de76ad92aa32805c36417a4cf87bb5cd';
BEGIN
 PERFORM pg_advisory_xact_lock(2026083000261022);
 SELECT * INTO STRICT a FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton FOR UPDATE;
 SELECT * INTO STRICT p FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton FOR UPDATE;
 IF p_owner IS DISTINCT FROM 'top3000-completion-20261024-root-5db48b32d103' OR p_fence IS DISTINCT FROM 3 OR p_stop IS DISTINCT FROM 1 OR a.owner_id IS DISTINCT FROM p_owner OR a.owner_fence IS DISTINCT FROM 3 OR a.stop_generation IS DISTINCT FROM 1
 OR a.enabled IS DISTINCT FROM true OR a.stopped IS DISTINCT FROM false OR a.hold_reason IS NOT NULL OR p.enabled IS DISTINCT FROM true
 OR p.expected_owner_id IS DISTINCT FROM a.owner_id OR p.expected_owner_fence IS DISTINCT FROM a.owner_fence OR p.expected_stop_generation IS DISTINCT FROM a.stop_generation
 OR a.scope_sha256 IS DISTINCT FROM '8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd' OR p.scope_sha256 IS DISTINCT FROM a.scope_sha256
 OR a.admitted_until<=clock_timestamp() OR p.deadline<=clock_timestamp() OR a.admitted_until IS NULL OR p.deadline IS NULL
 OR p.budget_evidence#>>'{monetary_policy,authority_event_id}' IS DISTINCT FROM '9528'
 OR population_top3000_202608_202609_v1.pipeline_cost_admission_v2(p,0) IS DISTINCT FROM true
 THEN RAISE EXCEPTION 'OPTIONS_V6_CURRENT_AUTHORITY_REQUIRED';END IF;
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE status='LEASED')
 OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_provider_lease WHERE released_at IS NULL)
 OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.connector_probe_lease_v1 WHERE released_at IS NULL)
 THEN RAISE EXCEPTION 'OPTIONS_V6_ACTIVE_OR_UNRECONCILED_EFFECT';END IF;
 SELECT * INTO STRICT t FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id='d377d980-5dbd-4276-a2c9-d5638c60b668' FOR UPDATE;
 IF p_item->>'source_task_id' IS DISTINCT FROM t.task_id::text THEN RAISE EXCEPTION 'OPTIONS_V6_EXACT_CLEARING_TASK_REQUIRED';END IF;
 IF t.status IS DISTINCT FROM 'BLOCKED' OR t.blocker_code IS DISTINCT FROM 'CTX02_CENSUS_PAGE_CAP_REVIEW'
 OR t.payload->>'request_key' IS DISTINCT FROM req
 OR t.lease_until IS NOT NULL OR t.executions>=t.max_executions OR t.cumulative_failures>=t.max_failures
 OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_attempt WHERE attempt_id=t.attempt_id AND ended_at IS NOT NULL)
 THEN RAISE EXCEPTION 'OPTIONS_V6_EXACT_PAGE198_CHECKPOINT_REQUIRED';END IF;
 IF encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.ctx02_census_admit_v1(text,bigint,bigint,text,integer,integer,timestamptz,text)'::regprocedure),'UTF8')),'hex') IS DISTINCT FROM 'fee6da97908c100dd903e42cbf5a04d23548429af8b8f00b0a917850551f471d'
 OR encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.ctx02_census_context_v1(uuid)'::regprocedure),'UTF8')),'hex') IS DISTINCT FROM 'c55812c698b6b87e42560d639a0cb9fde89ec52dc68f26efe91d872bf7664a95'
 THEN RAISE EXCEPTION 'OPTIONS_V6_REVIEWED_PREIMAGE_CHANGED';END IF;
 SELECT count(*),max(wire_bytes) INTO pages,maxwire FROM population_top3000_202608_202609_v1.ctx02_census_page_v1 WHERE partition_key=req;
 SELECT count(*) INTO used_calls FROM population_top3000_202608_202609_v1.pipeline_provider_lease l JOIN population_top3000_202608_202609_v1.pipeline_attempt z USING(attempt_id) WHERE z.task_id=t.task_id AND l.provider='Massive';
 IF pages<197 OR pages>947 OR used_calls<pages OR used_calls>pages+4 OR t.checkpoint->>'next_page' IS DISTINCT FROM (pages+1)::text THEN RAISE EXCEPTION 'OPTIONS_V6_PAGE_CALL_RECONCILIATION_CHANGED';END IF;
 new_ceiling:=pages+50;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.ctx02_census_admission_v1 WHERE request_key=req AND admitted_page_ceiling=pages AND max_provider_calls<=used_calls+4) THEN RAISE EXCEPTION 'OPTIONS_V6_EXHAUSTED_ALLOWANCE_REQUIRED';END IF;
 SELECT count(*) INTO bad FROM (SELECT q.*,row_number() OVER(ORDER BY page_number) expected,lag(next_url) OVER(ORDER BY page_number) prior_url FROM population_top3000_202608_202609_v1.ctx02_census_page_v1 q WHERE partition_key=req)x LEFT JOIN population_top3000_202608_202609_v1.source_document d USING(source_id)
 WHERE d.source_id IS NULL OR page_number<>expected OR (page_number>1 AND request_url IS DISTINCT FROM prior_url) OR x.source_sha256 IS DISTINCT FROM d.source_sha256 OR d.source_sha256 IS DISTINCT FROM encode(sha256(convert_to(d.content_text,'UTF8')),'hex') OR d.content_bytes IS DISTINCT FROM octet_length(d.content_text);
 IF bad<>0 THEN RAISE EXCEPTION 'OPTIONS_V6_RETAINED_CHAIN_CHANGED';END IF;
 SELECT sum(pg_total_relation_size(c.oid)) INTO relbytes FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='population_top3000_202608_202609_v1' AND c.relkind IN('r','m');
 recordbytes:=pg_total_relation_size('population_top3000_202608_202609_v1.ctx02_census_record_v1');
 SELECT count(*) INTO normalized FROM population_top3000_202608_202609_v1.ctx02_census_record_v1;
 IF normalized<196000 THEN RAISE EXCEPTION 'OPTIONS_V6_MEASURED_NORMALIZED_POPULATION_REQUIRED';END IF;
 --50 pages, measured normalized bytes/196000 rows with 50% margin, max observed wire with2x margin, plus16MiB overhead.
 estimated:=ceil(recordbytes::numeric/normalized*50000*1.5)::bigint + maxwire*50*2 +16777216;
 IF relbytes+estimated>(p.budget_evidence#>>'{resource_limits,max_relation_bytes}')::bigint
 OR a.stored_bytes+50*1048576>a.max_source_bytes OR a.provider_requests+50>a.max_provider_requests OR a.http_invocations+50>a.max_http_invocations
 THEN RAISE EXCEPTION 'OPTIONS_V6_MEASURED_RESOURCE_REVIEW_REQUIRED';END IF;
 before_t:=to_jsonb(t);before_a:=to_jsonb(a);before_p:=to_jsonb(p);
 result:=population_top3000_202608_202609_v1.ctx02_census_admit_v1(a.owner_id,a.owner_fence,a.stop_generation,req,new_ceiling,(used_calls+50)::integer,least(a.admitted_until,p.deadline)-interval '1second','Automatic measured50-page recovery tranche; retained pages, released calls, cursor, original sources and shared storage bounds rechecked; cumulative counters preserved');
 IF population_top3000_202608_202609_v1.ctx02_census_context_v1(t.task_id)->>'state' IS DISTINCT FROM 'READY' THEN RAISE EXCEPTION 'OPTIONS_V6_ADMISSION_NOT_READY';END IF;
 UPDATE population_top3000_202608_202609_v1.pipeline_task SET status='READY',blocker_code=NULL,blocker_until=NULL,resumption_condition=NULL,not_before=clock_timestamp()
 WHERE task_id=t.task_id AND status='BLOCKED' AND blocker_code='CTX02_CENSUS_PAGE_CAP_REVIEW';
 SELECT to_jsonb(z) INTO after_t FROM population_top3000_202608_202609_v1.pipeline_task z WHERE task_id=t.task_id;
 IF (after_t-ARRAY['status','blocker_code','blocker_until','resumption_condition','not_before']) IS DISTINCT FROM (before_t-ARRAY['status','blocker_code','blocker_until','resumption_condition','not_before'])
 OR before_a IS DISTINCT FROM (SELECT to_jsonb(z) FROM population_top3000_202608_202609_v1.acquisition_control z WHERE singleton)
 OR before_p IS DISTINCT FROM (SELECT to_jsonb(z) FROM population_top3000_202608_202609_v1.pipeline_control z WHERE singleton)
 THEN RAISE EXCEPTION 'OPTIONS_V6_COUNTER_OR_HISTORY_MUTATION';END IF;
 wake:=population_top3000_202608_202609_v1.pipeline_wake_v1(a.owner_id,a.owner_fence,'Reviewed automatic options recovery: exact retained checkpoint requeued; measured50-page tranche');
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES('CTX02_BOUNDED_ALLOWANCE_CLEARED_V6',jsonb_build_object('task_id',t.task_id,'owner_id',a.owner_id,'owner_fence',a.owner_fence,'stop_generation',a.stop_generation,'scope_sha256',a.scope_sha256,'before_task',before_t,'admission',result,'mission_relation_bytes',relbytes,'estimated_additional_bytes',estimated,'provider_calls_preserved',used_calls,'existing_pages',pages,'allowed_new_pages',50,'source_denominator_unchanged',true,'parser_acceptance_holds_unchanged',true,'wake',wake));
 RETURN jsonb_build_object('state','CLEARED','source_task_id',t.task_id,'next_page',pages+1,'admitted_page_ceiling',new_ceiling,'expected_successor','REAL_ACQUIRE_COMMIT_THEN_NORMALIZE','useful_output_observed',false,'estimated_additional_bytes',estimated);
END;$restore$;
REVOKE ALL ON FUNCTION population_top3000_202608_202609_v1.ctx02_clear_page_admission_v6(text,bigint,bigint,jsonb) FROM PUBLIC,anon,authenticated;
