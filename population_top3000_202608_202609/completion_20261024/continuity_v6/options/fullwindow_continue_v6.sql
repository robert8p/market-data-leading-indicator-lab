-- Exact additive continuation for assignment20231; does not change any adapter or August request.
CREATE FUNCTION population_top3000_202608_202609_v1.ctx02_fullwindow_continue_v6(p_owner text,p_fence bigint,p_stop bigint,p_assignment bigint,p_review bigint)
RETURNS jsonb LANGUAGE plpgsql SET search_path=pg_catalog AS $f$
DECLARE a population_top3000_202608_202609_v1.acquisition_control%rowtype;p population_top3000_202608_202609_v1.pipeline_control%rowtype;
 t population_top3000_202608_202609_v1.pipeline_task%rowtype;
 assignment jsonb; review jsonb;claim jsonb; claim_id bigint;claim_count integer;adapter_count integer;adapter_bad integer;
 item jsonb;before_t jsonb;before_a jsonb;before_p jsonb;result jsonb:='[]'; prior jsonb;pages bigint;used_calls bigint;bad bigint;relbytes bigint;recordbytes bigint;normalized bigint;maxwire bigint;estimated bigint;ev bigint;
BEGIN
 PERFORM pg_advisory_xact_lock(2026083000261022);
 SELECT * INTO STRICT a FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton FOR UPDATE;
 SELECT * INTO STRICT p FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton FOR UPDATE;
 IF p_owner IS DISTINCT FROM 'top3000-completion-20261024-root-5db48b32d103' OR p_fence IS DISTINCT FROM 3 OR p_stop IS DISTINCT FROM 1
 OR a.owner_id IS DISTINCT FROM p_owner OR a.owner_fence IS DISTINCT FROM p_fence OR a.stop_generation IS DISTINCT FROM p_stop
 OR a.enabled IS DISTINCT FROM true OR a.stopped IS DISTINCT FROM false OR a.hold_reason IS NOT NULL OR p.enabled IS DISTINCT FROM true
 OR p.expected_owner_id IS DISTINCT FROM a.owner_id OR p.expected_owner_fence IS DISTINCT FROM a.owner_fence OR p.expected_stop_generation IS DISTINCT FROM a.stop_generation
 OR a.scope_sha256 IS DISTINCT FROM '8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd' OR p.scope_sha256 IS DISTINCT FROM a.scope_sha256
 OR a.admitted_until IS NULL OR a.admitted_until<=clock_timestamp()+interval '2minutes' OR p.deadline IS NULL OR p.deadline<=clock_timestamp()+interval '2minutes'
 OR p.budget_evidence#>>'{monetary_policy,authority_event_id}' IS DISTINCT FROM '9528' OR population_top3000_202608_202609_v1.pipeline_cost_admission_v2(p,0) IS DISTINCT FROM true THEN RAISE EXCEPTION 'CTX02_WINDOW_CONTROLLER_ADMISSION_REQUIRED';END IF;
 SELECT details INTO assignment FROM population_top3000_202608_202609_v1.pipeline_event WHERE event_id=p_assignment AND event='ENGINEERING_IMPLEMENTATION_ASSIGNMENT_V5';
 SELECT details INTO review FROM population_top3000_202608_202609_v1.pipeline_event WHERE event_id=p_review AND event='CTX02_FULLWINDOW_CONTINUATION_REVIEW_V6';
 IF assignment->>'assignment_id' IS DISTINCT FROM 'ctx02-fullwindow-two-date-pages2-through11-v6'
 OR assignment->>'owner_id' IS DISTINCT FROM a.owner_id OR assignment->>'owner_fence' IS DISTINCT FROM a.owner_fence::text OR assignment->>'stop_generation' IS DISTINCT FROM a.stop_generation::text
 OR assignment->>'assigned_executor' IS DISTINCT FROM 'automation:6adc2c96db348191bca7cf4eacfdac32' OR assignment->>'authority_event_id' IS DISTINCT FROM '9528'
 OR assignment#>>'{bounds,provider_requests}' IS DISTINCT FROM '20' OR assignment#>>'{bounds,source_bytes}' IS DISTINCT FROM '20971520'
 OR assignment#>>'{source_admission,direct_executor_provider_calls}' IS DISTINCT FROM '0'
 OR assignment->>'scope_sha256' IS DISTINCT FROM a.scope_sha256 OR coalesce((assignment->>'not_after')::timestamptz,'-infinity')<=clock_timestamp()
 OR coalesce(assignment->>'code_commit','')!~'^[0-9a-f]{40}$' OR coalesce(review->>'code_commit','')!~'^[0-9a-f]{40}$'
 OR assignment->>'exclusive_component' IS DISTINCT FROM 'CTX02_FULLWINDOW_REFERENCE_ROUTE_V6' OR assignment->>'root_will_not_concurrently_modify' IS DISTINCT FROM 'true'
 OR review->>'approved' IS DISTINCT FROM 'true' OR review->>'assignment_event_id' IS DISTINCT FROM p_assignment::text
 OR nullif(review->>'reviewer','') IS NULL OR nullif(review->>'producer','') IS NULL OR review->>'owner_id' IS DISTINCT FROM a.owner_id OR review->>'owner_fence' IS DISTINCT FROM '3' OR review->>'stop_generation' IS DISTINCT FROM '1' OR review->>'scope_sha256' IS DISTINCT FROM a.scope_sha256 OR review->>'reviewer'=review->>'producer'
 OR review->>'function_sha256' IS DISTINCT FROM encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.ctx02_fullwindow_continue_v6(text,bigint,bigint,bigint,bigint)'::regprocedure),'UTF8')),'hex')
 THEN RAISE EXCEPTION 'CTX02_WINDOW_EXACT_ASSIGNMENT_AND_REVIEW_REQUIRED';END IF;
 SELECT count(*),min(e.event_id) INTO claim_count,claim_id FROM population_top3000_202608_202609_v1.pipeline_event e
 WHERE e.event='ENGINEERING_ASSIGNMENT_CLAIMED_V5'
 AND (e.details->>'assignment_event_id'=p_assignment::text OR e.details->>'exclusive_component'=assignment->>'exclusive_component')
 AND NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_event done
  WHERE done.event IN('ENGINEERING_ASSIGNMENT_COMPLETED_V5','ENGINEERING_ASSIGNMENT_EFFECTS_RECONCILED_RELEASE_V5')
  AND done.details->>'claim_event_id'=e.event_id::text AND done.details->>'effects_known'='true');
 IF claim_count<>1 THEN RAISE EXCEPTION 'CTX02_WINDOW_LIVE_EXCLUSIVE_CLAIM_REQUIRED';END IF;
 SELECT details INTO STRICT claim FROM population_top3000_202608_202609_v1.pipeline_event WHERE event_id=claim_id;
 IF claim->>'assignment_event_id' IS DISTINCT FROM p_assignment::text OR claim->>'assignment_id' IS DISTINCT FROM assignment->>'assignment_id'
 OR claim->>'assignment_details_sha256' IS DISTINCT FROM encode(sha256(convert_to(assignment::text,'UTF8')),'hex')
 OR claim->>'exclusive_component' IS DISTINCT FROM assignment->>'exclusive_component' OR claim->>'executor' IS DISTINCT FROM assignment->>'assigned_executor'
 OR claim->>'owner_id' IS DISTINCT FROM a.owner_id OR claim->>'owner_fence' IS DISTINCT FROM a.owner_fence::text OR claim->>'stop_generation' IS DISTINCT FROM a.stop_generation::text
 OR claim->>'scope_sha256' IS DISTINCT FROM a.scope_sha256 OR claim->>'authority_event_id' IS DISTINCT FROM '9528'
 OR coalesce(length(claim->>'automation_run_id'),0) NOT BETWEEN 8 AND 128
 OR coalesce((claim->>'claimed_at')::timestamptz,'infinity')>clock_timestamp()
 OR coalesce((claim->>'expires_at')::timestamptz,'-infinity')<=clock_timestamp()
 OR (claim->>'expires_at')::timestamptz>least((assignment->>'not_after')::timestamptz,a.admitted_until,p.deadline,(claim->>'claimed_at')::timestamptz+interval '45minutes')
 OR review->>'claim_event_id' IS DISTINCT FROM claim_id::text THEN RAISE EXCEPTION 'CTX02_WINDOW_LIVE_CLAIM_PINS_CHANGED';END IF;
 IF population_top3000_202608_202609_v1.ctx02_census_bundle_v1() IS DISTINCT FROM '0a5cebaf6c91f612c8a5ec0b5c02203823f7e33cc1449db4500fbd08ab540161'
 OR population_top3000_202608_202609_v1.ctx02_parser_bundle_v1() IS DISTINCT FROM '13f338336169f76139d3b162ac3832ec4f5ac4243910ce0c31d5ffde0145baff'
 OR encode(sha256(convert_to(population_top3000_202608_202609_v1.daily_shared_calendar_v1()::text,'UTF8')),'hex') IS DISTINCT FROM '2489051535c8169bc836f8ab216c32c3ad68ad8e517f00f135186e74723e81d2' THEN RAISE EXCEPTION 'CTX02_WINDOW_SOURCE_OR_CALENDAR_PIN_CHANGED';END IF;
 SELECT count(*),count(*) FILTER(WHERE function_sha256 IS DISTINCT FROM encode(sha256(convert_to(pg_get_functiondef(function_oid),'UTF8')),'hex')
 OR function_sha256 IS DISTINCT FROM (CASE stage WHEN 'ACQUIRE' THEN 'a639de358ea23a90844dbb831facb339e89d37d5c32de228657296c4284c29ad' WHEN 'NORMALIZE' THEN 'c52c1d0e7dfe1df0e78e498c1783a68439920feb30164d0ffe0436a960133228' WHEN 'DERIVE' THEN '2d76a92ca4ff91ef1787bdd39c419e4cd9c2d2361a1472da2b86e3832b5f85c5' WHEN 'VALIDATE' THEN '9d4efc770fb30e863d6b5008918c67a1a65d4f0fc138397c2b1204ee0b47f7bf' END))
 INTO adapter_count,adapter_bad FROM population_top3000_202608_202609_v1.pipeline_adapter WHERE family_id='CTX02' AND stage IN('ACQUIRE','NORMALIZE','DERIVE','VALIDATE') AND enabled;
 IF adapter_count<>4 OR adapter_bad<>0 THEN RAISE EXCEPTION 'CTX02_WINDOW_ADAPTER_PINS_CHANGED';END IF;
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_provider_lease WHERE released_at IS NULL)
 OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.connector_probe_lease_v1 WHERE released_at IS NULL)
 THEN RAISE EXCEPTION 'CTX02_WINDOW_PROVIDER_EFFECT_UNRESOLVED';END IF;
 IF encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.continuation_dispatch_v6(text,bigint,bigint)'::regprocedure),'UTF8')),'hex') IS DISTINCT FROM '9f2c2c2dbab45940b98a698db1f5774122094cddc606fd80d91abf2e7541f38a' THEN RAISE EXCEPTION 'WINDOW_DISPATCH_PIN_CHANGED';END IF;
 IF p_assignment<>20231 OR encode(sha256(convert_to(assignment::text,'UTF8')),'hex')<>'675191e135349cdbb72b24f29e16b84c51a38157b2a6063c23e51d76ead7b0d6' THEN RAISE EXCEPTION 'WINDOW_EXACT_ASSIGNMENT_REQUIRED'; END IF;
 IF p.deadline IS DISTINCT FROM '2026-10-25T22:36:17.687194Z'::timestamptz OR p.tick_count+100>p.max_ticks THEN RAISE EXCEPTION 'WINDOW_DEADLINE_TICK_BOUND';END IF;
 IF encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.ctx02_census_admit_v1(text,bigint,bigint,text,integer,integer,timestamptz,text)'::regprocedure),'UTF8')),'hex') IS DISTINCT FROM 'fee6da97908c100dd903e42cbf5a04d23548429af8b8f00b0a917850551f471d'
 OR encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.ctx02_census_context_v1(uuid)'::regprocedure),'UTF8')),'hex') IS DISTINCT FROM 'c55812c698b6b87e42560d639a0cb9fde89ec52dc68f26efe91d872bf7664a95' THEN RAISE EXCEPTION 'WINDOW_DEPENDENCY_CHANGED';END IF;
 SELECT details INTO prior FROM population_top3000_202608_202609_v1.pipeline_event WHERE event='CTX02_FULLWINDOW_CONTINUATION_INTEGRATED_V6' AND details->>'assignment_event_id'=p_assignment::text;
 IF prior IS NOT NULL THEN RETURN prior||jsonb_build_object('replay',true);END IF;
 SELECT sum(pg_total_relation_size(c.oid)) INTO relbytes FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='population_top3000_202608_202609_v1' AND c.relkind IN('r','m');
 recordbytes:=pg_total_relation_size('population_top3000_202608_202609_v1.ctx02_census_record_v1');
 SELECT count(*) INTO normalized FROM population_top3000_202608_202609_v1.ctx02_census_record_v1;
 SELECT max(wire_bytes) INTO maxwire FROM population_top3000_202608_202609_v1.ctx02_census_page_v1 WHERE partition_key IN(SELECT x->>'request_key' FROM jsonb_array_elements(assignment->'source_tasks')x);
 IF normalized<=0 OR maxwire IS NULL THEN RAISE EXCEPTION 'WINDOW_MEASUREMENTS_MISSING';END IF;
 estimated:=ceil(recordbytes::numeric/normalized*20000*1.5)::bigint+maxwire*20*2+16777216;
 IF estimated>100663296 OR relbytes+100663296>(p.budget_evidence#>>'{resource_limits,max_relation_bytes}')::bigint OR a.stored_bytes+a.reserved_bytes+20971520>a.max_source_bytes OR a.provider_requests+20>a.max_provider_requests OR a.http_invocations+20>a.max_http_invocations THEN RAISE EXCEPTION 'WINDOW_MEASURED_RESOURCES_EXHAUSTED';END IF;
 before_a:=to_jsonb(a);before_p:=to_jsonb(p);
 FOR item IN SELECT value FROM jsonb_array_elements(assignment->'source_tasks') LOOP
 SELECT * INTO STRICT t FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=(item->>'task_id')::uuid FOR UPDATE;
 IF t.partition_key IS DISTINCT FROM item->>'request_key' OR t.stage IS DISTINCT FROM 'ACQUIRE' OR t.family_id IS DISTINCT FROM 'CTX02' OR t.input_versions IS DISTINCT FROM jsonb_build_object('scope_sha256',a.scope_sha256,'CTX02_contract_sha256','474b7aced2e7e97a4b0af5e81f264f93008967669dc4968017b4b48cc1a4b3af','parser_bundle_sha256','13f338336169f76139d3b162ac3832ec4f5ac4243910ce0c31d5ffde0145baff','ctx02_census_bundle_sha256','0a5cebaf6c91f612c8a5ec0b5c02203823f7e33cc1449db4500fbd08ab540161','fullwindow_manifest_file_sha256','9e08481bffd1747992069c1bd77564fafffe241fd6e70a9249dbc49cfcd693bd') OR t.status IS DISTINCT FROM 'BLOCKED' OR t.blocker_code IS DISTINCT FROM 'CTX02_CENSUS_PAGE_CAP_REVIEW' OR t.payload->>'request_key' IS DISTINCT FROM item->>'request_key'
 OR t.lease_until IS NOT NULL OR t.attempt_id::text IS DISTINCT FROM item->>'attempt_id' OR t.executions<>2 OR t.cumulative_failures<>0
 OR t.owner_fence<>3 OR t.stop_generation<>1 OR t.executions>=t.max_executions OR t.cumulative_failures>=t.max_failures
 OR encode(sha256(convert_to(t.payload::text,'UTF8')),'hex') IS DISTINCT FROM item->>'payload_sha256'
 OR encode(sha256(convert_to(t.checkpoint::text,'UTF8')),'hex') IS DISTINCT FROM item->>'checkpoint_sha256'
 OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_attempt WHERE attempt_id=t.attempt_id AND ended_at IS NOT NULL)
 THEN RAISE EXCEPTION 'WINDOW_EXACT_TASK_PREIMAGE_CHANGED';END IF;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.ctx02_census_admission_v1 WHERE request_key=t.partition_key AND admitted_page_ceiling=1 AND max_provider_calls=1 AND owner_id=a.owner_id AND owner_fence=3 AND stop_generation=1 AND expires_at>clock_timestamp()) THEN RAISE EXCEPTION 'WINDOW_ADMISSION_PREIMAGE_CHANGED';END IF;
 SELECT count(*) INTO used_calls FROM population_top3000_202608_202609_v1.pipeline_provider_lease l JOIN population_top3000_202608_202609_v1.pipeline_attempt z USING(attempt_id) WHERE z.task_id=t.task_id AND l.provider='Massive';
 SELECT count(*) INTO pages FROM population_top3000_202608_202609_v1.ctx02_census_page_v1 WHERE partition_key=t.partition_key;
 IF pages<>1 OR used_calls<>1 THEN RAISE EXCEPTION 'WINDOW_PAGE_CALL_PREIMAGE_CHANGED';END IF;
 SELECT count(*) INTO bad FROM population_top3000_202608_202609_v1.ctx02_census_page_v1 x LEFT JOIN population_top3000_202608_202609_v1.source_document d USING(source_id) LEFT JOIN population_top3000_202608_202609_v1.ctx02_census_request_v1 q ON q.request_key=x.partition_key WHERE x.partition_key=t.partition_key AND (d.source_id IS NULL OR q.request_key IS NULL OR x.page_number<>1 OR x.request_url IS DISTINCT FROM q.manifest->>'first_url' OR x.next_url IS DISTINCT FROM t.checkpoint->>'next_url' OR x.source_sha256 IS DISTINCT FROM item->>'last_source_sha256' OR x.source_sha256 IS DISTINCT FROM d.source_sha256 OR d.source_sha256 IS DISTINCT FROM encode(sha256(convert_to(d.content_text,'UTF8')),'hex') OR d.content_bytes IS DISTINCT FROM octet_length(d.content_text));
 IF bad<>0 THEN RAISE EXCEPTION 'WINDOW_SOURCE_CHAIN_CHANGED';END IF;
 PERFORM 1 FROM population_top3000_202608_202609_v1.continuation_clear_queue_v6 WHERE clearing_id=item#>>'{clearing,clearing_id}' AND owner_id=a.owner_id AND owner_fence=3 AND stop_generation=1 AND scope_sha256=a.scope_sha256 AND assigned_executor=assignment->>'assigned_executor' AND not_after>clock_timestamp() AND status='READY' AND attempts=0 AND assignment_event_id=(item#>>'{clearing,assignment_event_id}')::bigint AND input_sha256=item#>>'{clearing,input_sha256}' FOR UPDATE;
 IF NOT FOUND THEN RAISE EXCEPTION 'WINDOW_CLEARING_PREIMAGE_CHANGED';END IF;
 before_t:=to_jsonb(t);
 PERFORM population_top3000_202608_202609_v1.ctx02_census_admit_v1(a.owner_id,3,1,t.partition_key,11,11,least(a.admitted_until,p.deadline,(assignment->>'not_after')::timestamptz)-interval '1second','Reviewed assignment20231 exact10additional native pages; original checkpoint/counters preserved; finite page11 boundary only');
 IF population_top3000_202608_202609_v1.ctx02_census_context_v1(t.task_id)->>'state' IS DISTINCT FROM 'READY' THEN RAISE EXCEPTION 'WINDOW_ADMISSION_NOT_READY';END IF;
 UPDATE population_top3000_202608_202609_v1.pipeline_task SET status='READY',blocker_code=NULL,blocker_until=NULL,resumption_condition=NULL,not_before=clock_timestamp() WHERE task_id=t.task_id;
 IF (SELECT to_jsonb(z)-ARRAY['status','blocker_code','blocker_until','resumption_condition','not_before'] FROM population_top3000_202608_202609_v1.pipeline_task z WHERE task_id=t.task_id) IS DISTINCT FROM before_t-ARRAY['status','blocker_code','blocker_until','resumption_condition','not_before'] THEN RAISE EXCEPTION 'WINDOW_TASK_HISTORY_CHANGED';END IF;
 UPDATE population_top3000_202608_202609_v1.continuation_clear_queue_v6 SET status='WAITING_OUTPUT',claimed_at=clock_timestamp(),last_receipt=jsonb_build_object('assignment_event_id',p_assignment,'claim_event_id',claim_id,'status','INTEGRATED_WAITING_FOR_DATA_COMMIT','source_task_id',t.task_id,'next_page',2,'absolute_page_ceiling',11),updated_at=clock_timestamp() WHERE clearing_id=item#>>'{clearing,clearing_id}';
 result:=result||jsonb_build_array(jsonb_build_object('task_id',t.task_id,'request_key',t.partition_key,'next_page',2,'page_ceiling',11,'call_ceiling',11));
 END LOOP;
 IF before_a IS DISTINCT FROM (SELECT to_jsonb(z) FROM population_top3000_202608_202609_v1.acquisition_control z WHERE singleton) OR before_p IS DISTINCT FROM (SELECT to_jsonb(z) FROM population_top3000_202608_202609_v1.pipeline_control z WHERE singleton) THEN RAISE EXCEPTION 'WINDOW_COUNTER_HISTORY_CHANGED';END IF;
 prior:=jsonb_build_object('assignment_event_id',p_assignment,'claim_event_id',claim_id,'review_event_id',p_review,'code_commit',review->>'code_commit','function_sha256',review->>'function_sha256','owner_id',a.owner_id,'owner_fence',3,'stop_generation',1,'scope_sha256',a.scope_sha256,'status','INTEGRATED_WAITING_FOR_DATA_COMMIT','tasks',result,'measured_relation_bytes',relbytes,'estimated_growth_bytes',estimated,'new_calls',20,'source_bytes_ceiling',20971520,'preserved_counters',true,'preserved_evidence_holds',true);
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES('CTX02_FULLWINDOW_CONTINUATION_INTEGRATED_V6',prior) RETURNING event_id INTO ev;
 PERFORM population_top3000_202608_202609_v1.pipeline_wake_v1(a.owner_id,3,'Reviewed exact20231 September continuation');
 RETURN prior||jsonb_build_object('integration_event_id',ev);
END;$f$;
REVOKE ALL ON FUNCTION population_top3000_202608_202609_v1.ctx02_fullwindow_continue_v6(text,bigint,bigint,bigint,bigint) FROM PUBLIC,anon,authenticated;
