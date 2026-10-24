CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.ref01_date_continue_impl_v9(p_owner text,p_fence bigint,p_stop bigint)
RETURNS jsonb LANGUAGE plpgsql SET search_path=pg_catalog AS $f$
DECLARE ctl jsonb; pl population_top3000_202608_202609_v1.ref01_date_plan_v9%rowtype;
 ac population_top3000_202608_202609_v1.acquisition_control%rowtype;
 pc population_top3000_202608_202609_v1.pipeline_control%rowtype;
 r population_top3000_202608_202609_v1.ref01_date_request_v4%rowtype;
 t population_top3000_202608_202609_v1.pipeline_task%rowtype;
 adm population_top3000_202608_202609_v1.ref01_date_admission_v4%rowtype;
 dt date;k text;h text;iv jsonb;m jsonb;n int;pages int;calls bigint;http bigint;raw bigint;rel bigint;allrel bigint;
 cap int;callcap int;until_at timestamptz;reason text;tid uuid;ctx jsonb;clear_manifest jsonb;clear_hash text;
BEGIN
 ctl:=population_top3000_202608_202609_v1.continuation_guard_v6(p_owner,p_fence,p_stop);
 SELECT * INTO pl FROM population_top3000_202608_202609_v1.ref01_date_plan_v9 WHERE enabled FOR UPDATE;
 IF NOT FOUND THEN RETURN '{"state":"NO_ENABLED_REF01_DATE_PLAN"}';END IF;
 IF pl.content_sha256 IS DISTINCT FROM encode(sha256(convert_to(pl.content::text,'UTF8')),'hex')
 OR pl.code_bundle_sha256 IS DISTINCT FROM population_top3000_202608_202609_v1.ref01_date_bundle_v4()
 OR pl.content->>'owner_id' IS DISTINCT FROM p_owner OR pl.content->>'owner_fence' IS DISTINCT FROM p_fence::text
 OR pl.content->>'stop_generation' IS DISTINCT FROM p_stop::text THEN RAISE EXCEPTION 'REF01_DATE_PLAN_AUTHORITY_OR_CODE_CHANGED';END IF;
 SELECT * INTO STRICT ac FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton;
 SELECT * INTO STRICT pc FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton;
 -- Date order comes from the pinned complete population, not from an implementation ceiling.
 SELECT x::date INTO dt FROM jsonb_array_elements_text(pl.content->'dates') x
 WHERE NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.ref01_date_plan_request_v9 pr
 JOIN population_top3000_202608_202609_v1.pipeline_task q ON q.payload->>'request_key'=pr.request_key AND q.family_id='REF01' AND q.stage='ACQUIRE'
 WHERE pr.plan_id=pl.plan_id AND pr.as_of=x::date AND q.status='COMPLETE'
 AND EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.ref01_date_page_v4 pg
 JOIN population_top3000_202608_202609_v1.source_document ds USING(source_id)
 WHERE pg.acquire_task_id=q.task_id AND pg.partition_key=pr.request_key AND pg.next_url IS NULL
 AND ds.source_sha256=pg.source_sha256 AND ds.source_sha256=encode(sha256(convert_to(ds.content_text,'UTF8')),'hex')
 AND ds.content_bytes=octet_length(ds.content_text) AND ds.content_text::jsonb->>'next_url' IS NULL
 AND jsonb_array_length(ds.content_text::jsonb->'results')=pg.result_count)) ORDER BY x::date LIMIT 1;
 IF dt IS NULL THEN RETURN jsonb_build_object('state','ALL_PLANNED_DATE_SOURCES_TERMINAL','plan_id',pl.plan_id,'family_accepted',false,'full_instrument_history_accepted',false);END IF;
 SELECT rq.* INTO r FROM population_top3000_202608_202609_v1.ref01_date_plan_request_v9 pr
 JOIN population_top3000_202608_202609_v1.ref01_date_request_v4 rq USING(request_key) WHERE pr.plan_id=pl.plan_id AND pr.as_of=dt;
 IF FOUND THEN
  SELECT * INTO t FROM population_top3000_202608_202609_v1.pipeline_task WHERE family_id='REF01' AND stage='ACQUIRE' AND payload->>'request_key'=r.request_key;
  IF FOUND AND t.status='LEASED' THEN RETURN jsonb_build_object('state','SOURCE_ATTEMPT_ACTIVE','task_id',t.task_id);END IF;
  -- Neither expired leases nor failed HTTP requests are interpreted as harmless waits.
  IF t.task_id IS NOT NULL AND (t.cumulative_failures>0 OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_provider_lease l WHERE l.attempt_id=t.attempt_id AND l.released_at IS NULL)
   OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_attempt a WHERE a.task_id=t.task_id AND a.outcome='RUNNING')
   OR (t.status='BLOCKED' AND t.blocker_code NOT IN('REF01_DATE_FINITE_ADMISSION_REVIEW_V4','REF01_CENSUS_PAGE_CAP_REVIEW','REF01_DATE_PLAN_RESOURCE_REVIEW_V9')
      AND (t.blocker_code='DEPENDENCY_PENDING' AND t.resumption_condition->>'event'='PROVIDER_SLOT_AVAILABLE'
 AND NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_attempt a WHERE a.attempt_id=t.attempt_id AND a.request_id IS NOT NULL)
 AND NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_provider_lease l WHERE l.attempt_id=t.attempt_id)) IS NOT TRUE))
  THEN RETURN jsonb_build_object('state','PRESERVED_EVIDENCE_OR_UNKNOWN_OUTCOME_HOLD','task_id',t.task_id,'blocker',t.blocker_code);END IF;
  SELECT * INTO adm FROM population_top3000_202608_202609_v1.ref01_date_admission_v4 WHERE request_key=r.request_key;
  n:=coalesce((t.checkpoint->>'next_page')::int,1);
  IF adm.expires_at>clock_timestamp()+interval '5minutes' AND n<=adm.admitted_page_ceiling AND (t.status='READY' OR (t.status='BLOCKED' AND t.blocker_code='DEPENDENCY_PENDING' AND t.resumption_condition->>'event'='PROVIDER_SLOT_AVAILABLE' AND NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_attempt a WHERE a.attempt_id=t.attempt_id AND a.request_id IS NOT NULL))) THEN
   RETURN jsonb_build_object('state','EXISTING_ADMITTED_SOURCE_READY','task_id',t.task_id,'as_of',dt);
  END IF;
 ELSE n:=1;END IF;
 SELECT count(*) INTO http FROM population_top3000_202608_202609_v1.pipeline_attempt a JOIN population_top3000_202608_202609_v1.pipeline_task q USING(task_id)
 WHERE q.family_id='REF01' AND q.payload->>'kind'='REF01_CENSUS_SOURCE_V1' AND a.request_id IS NOT NULL;
 SELECT count(*) INTO calls FROM population_top3000_202608_202609_v1.pipeline_provider_lease l JOIN population_top3000_202608_202609_v1.pipeline_attempt a USING(attempt_id)
 JOIN population_top3000_202608_202609_v1.pipeline_task q USING(task_id) WHERE q.family_id='REF01' AND q.payload->>'kind'='REF01_CENSUS_SOURCE_V1' AND l.provider='Massive';
 SELECT coalesce(sum(wire_bytes),0),count(*) INTO raw,pages FROM population_top3000_202608_202609_v1.ref01_date_page_v4;
 SELECT coalesce(sum(pg_total_relation_size(oid)),0) INTO rel FROM pg_class WHERE relnamespace='population_top3000_202608_202609_v1'::regnamespace AND relkind='r'
 AND (relname LIKE 'ref01_date_%' OR relname='ref01_practical_dated_instrument_v3');
 SELECT coalesce(sum(pg_total_relation_size(oid)),0) INTO allrel FROM pg_class WHERE relnamespace='population_top3000_202608_202609_v1'::regnamespace AND relkind IN('r','m');
 -- Enough headroom for one exact maximum response and conservative in-database expansion.
 IF http>=(pl.content->>'max_http_invocations')::bigint OR calls>=(pl.content->>'max_provider_calls')::bigint THEN reason:='PLAN_CALL_REVIEW';
 ELSIF raw+8388608>(pl.content->>'max_source_bytes')::bigint OR rel+2*raw+16777216>(pl.content->>'max_relation_bytes')::bigint THEN reason:='PLAN_STORAGE_REVIEW';
 ELSIF ac.http_invocations>=ac.max_http_invocations OR ac.provider_requests>=ac.max_provider_requests OR ac.stored_bytes+ac.reserved_bytes+8388608>ac.max_source_bytes THEN reason:='GLOBAL_SOURCE_CAPACITY_REVIEW';
 ELSIF allrel+16777216>(pc.budget_evidence#>>'{resource_limits,max_relation_bytes}')::bigint THEN reason:='GLOBAL_RELATION_CAPACITY_REVIEW';
 ELSIF least(ac.admitted_until,pc.deadline)<=clock_timestamp()+interval '2minutes' THEN reason:='GLOBAL_TIME_ADMISSION_REVIEW';
 ELSIF n>(pl.content->>'max_pages_per_date')::int THEN reason:='PLAN_DATE_PAGE_REVIEW';
 ELSIF r.request_key IS NOT NULL AND n>(r.manifest->>'max_pages')::int THEN reason:='LEGACY_REQUEST_VERSION_SUCCESSOR_REQUIRED';END IF;
 IF reason IS NOT NULL THEN
  IF pl.last_blocker IS DISTINCT FROM reason THEN
   INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES('REF01_DATE_PLAN_REVIEW_REQUIRED_V9',ctl||jsonb_build_object('plan_id',pl.plan_id,'reason',reason,'as_of',dt,'task_id',t.task_id,'http_calls',http,'provider_calls',calls,'raw_bytes',raw,'relation_bytes',rel,'mission_relation_bytes',allrel,'required_action','Current controller executes measured finite resource admission or separately versioned request; preserve counters, sources, cursor and acceptance holds','assignment_event_id',pl.content->'assignment_event_id'));
   UPDATE population_top3000_202608_202609_v1.ref01_date_plan_v9 SET last_blocker=reason,updated_at=clock_timestamp() WHERE plan_id=pl.plan_id;
  END IF;

  clear_manifest:=jsonb_build_array(jsonb_build_object('plan_id',pl.plan_id,'plan_sha256',pl.content_sha256,'reason',reason,'request_key',r.request_key,
   'global_source_ceiling',ac.max_source_bytes,'global_relation_ceiling',pc.budget_evidence#>'{resource_limits,max_relation_bytes}',
   'global_provider_ceiling',ac.max_provider_requests,'global_http_ceiling',ac.max_http_invocations,'original_deadline',pc.deadline));
  clear_hash:=encode(sha256(convert_to(clear_manifest::text,'UTF8')),'hex');
  INSERT INTO population_top3000_202608_202609_v1.continuation_clear_queue_v6(clearing_id,kind,family_id,blocker_code,input_manifest,input_sha256,status,owner_id,owner_fence,stop_generation,scope_sha256,not_after,last_receipt)
  VALUES('ref01-date-plan:'||clear_hash,'OPERATIONAL_REVIEW',CASE WHEN reason LIKE 'GLOBAL_%' THEN 'RESOURCE' ELSE 'REF01' END,'REF01_DATE_PLAN_'||reason,clear_manifest,clear_hash,'READY',p_owner,p_fence,p_stop,ctl->>'scope_sha256',(ctl->>'not_after')::timestamptz,
   jsonb_build_object('source_bytes',raw,'relation_bytes',rel,'mission_relation_bytes',allrel,'provider_calls',calls,'http_calls',http,
   'execution_recipe','Inspect pinned instrument plan, exact retained cursor and actual measured source/relation/call/time headroom. Execute independently reviewed finite operational admission or separately versioned request/plan generation. Preserve all previous request manifests, counters, failures and original deadline. Reinvoke ref01_date_continue_v9 and verify next source plus normalization commits. Do not reset clearing attempts or claim family acceptance.')) ON CONFLICT DO NOTHING;
  RETURN jsonb_build_object('state','REVIEW_REQUIRED','reason',reason,'as_of',dt,'clearing_id','ref01-date-plan:'||clear_hash);
 END IF;
 IF r.request_key IS NULL THEN
  iv:=pl.content->'input_versions'||jsonb_build_object('transport_bundle_sha256',pl.code_bundle_sha256,'date_plan_id',pl.plan_id,'date_plan_sha256',pl.content_sha256);
  m:=jsonb_build_object('as_of',dt::text,'active',true,'population','ALL_PROVIDER_HISTORICAL_REFERENCE_NO_COHORT_FILTER','max_pages',100000,
  'first_url','https://api.massive.com/v3/reference/tickers?date='||dt::text||'&sort=ticker&limit=1000&order=asc&active=true&locale=us&market=stocks','max_response_bytes',8388608,'max_runtime_ms',45000,'input_versions',iv,
  'maximum_source_bytes',(pl.content->>'max_source_bytes')::bigint,'maximum_http_invocations',(pl.content->>'max_http_invocations')::bigint,'maximum_provider_calls',(pl.content->>'max_provider_calls')::bigint,'maximum_additional_relation_bytes',(pl.content->>'max_relation_bytes')::bigint);
  h:=encode(sha256(convert_to(m::text,'UTF8')),'hex');k:='ref01-date-v4:'||h;
  INSERT INTO population_top3000_202608_202609_v1.ref01_date_request_v4(request_key,manifest,manifest_sha256,input_versions) VALUES(k,m,h,iv);
  INSERT INTO population_top3000_202608_202609_v1.ref01_date_plan_request_v9 VALUES(pl.plan_id,dt,k,h,encode(sha256(convert_to(iv::text,'UTF8')),'hex'));
  SELECT * INTO STRICT r FROM population_top3000_202608_202609_v1.ref01_date_request_v4 WHERE request_key=k;
 END IF;
 cap:=least((r.manifest->>'max_pages')::int,(pl.content->>'max_pages_per_date')::int,
 greatest(coalesce(adm.admitted_page_ceiling,0),(pl.content->>'initial_page_tranche')::int,n+(pl.content->>'renewal_page_tranche')::int-1));
 callcap:=cap;
 until_at:=least(ac.admitted_until,pc.deadline,clock_timestamp()+interval '29minutes');
 PERFORM population_top3000_202608_202609_v1.ref01_date_admit_v4(p_owner,p_fence,p_stop,r.request_key,cap,callcap,until_at,
 'Reviewed versioned finite plan '||pl.plan_id||'; retained cursor and cumulative accounting checked; dates advance sequentially; source and family acceptance remain separate.');
 IF t.task_id IS NULL THEN
  tid:=population_top3000_202608_202609_v1.pipeline_enqueue_v1('REF01','ACQUIRE',r.request_key,jsonb_build_object('kind','REF01_CENSUS_SOURCE_V1','request_key',r.request_key,'request_manifest_sha256',r.manifest_sha256),r.input_versions);
 ELSE
  tid:=t.task_id;
  ctx:=population_top3000_202608_202609_v1.ref01_date_context_v4(tid);
  IF ctx->>'state'='READY' AND t.status='BLOCKED' AND t.blocker_code IN('REF01_DATE_FINITE_ADMISSION_REVIEW_V4','REF01_CENSUS_PAGE_CAP_REVIEW','REF01_DATE_PLAN_RESOURCE_REVIEW_V9') THEN
   UPDATE population_top3000_202608_202609_v1.pipeline_task SET status='READY',blocker_code=NULL,blocker_until=NULL,resumption_condition=NULL WHERE task_id=tid;
  END IF;
 END IF;
 UPDATE population_top3000_202608_202609_v1.ref01_date_plan_v9 SET last_blocker=NULL,updated_at=clock_timestamp() WHERE plan_id=pl.plan_id;
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES('REF01_DATE_PLAN_ADVANCED_V9',ctl||jsonb_build_object('plan_id',pl.plan_id,'as_of',dt,'task_id',tid,'request_key',r.request_key,'next_page',n,'page_ceiling',cap,'expires_at',until_at,'cumulative_provider_calls',calls,'cumulative_http_calls',http,'source_bytes',raw,'relation_bytes',rel,'all_counters_preserved',true,'family_accepted',false));
 RETURN jsonb_build_object('state','ADMITTED_AND_QUEUED','task_id',tid,'as_of',dt,'page_ceiling',cap);
END;$f$;
REVOKE ALL ON FUNCTION population_top3000_202608_202609_v1.ref01_date_continue_impl_v9(text,bigint,bigint) FROM PUBLIC,anon,authenticated;
