CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.ref01_date_context_v4(p_task uuid)
 RETURNS jsonb
 LANGUAGE plpgsql
 SET search_path TO 'pg_catalog'
AS $function$
DECLARE t population_top3000_202608_202609_v1.pipeline_task%rowtype;
 r population_top3000_202608_202609_v1.ref01_date_request_v4%rowtype;
 prior population_top3000_202608_202609_v1.ref01_date_page_v4%rowtype;
 admission population_top3000_202608_202609_v1.ref01_date_admission_v4%rowtype;
 ctl population_top3000_202608_202609_v1.acquisition_control%rowtype;
 plan population_top3000_202608_202609_v1.ref01_date_plan_v9%rowtype;
 asof date;first_url text;url text;k text;n integer;lim integer;page_only integer;used_calls integer;
BEGIN
 PERFORM population_top3000_202608_202609_v1.ref01_practical_guard_v1();
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.ref01_practical_membership_v1 WHERE snapshot_sha256='2dc0f766befed6de6b052221c3c87482d507ba1bad931cbb98654eb3af899bec' AND acceptance_sha256='20b9dac759857e9b510ccaeca33e4a94d8c748d588fb0cc27740cc66c389b5f9' AND acceptance_sha256=encode(sha256(convert_to(content::text,'UTF8')),'hex')) THEN RAISE EXCEPTION 'REF01_MEMBERSHIP_CHANGED';END IF;
 SELECT * INTO STRICT t FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=p_task;
 IF t.family_id IS DISTINCT FROM 'REF01' OR t.payload->>'kind' IS DISTINCT FROM 'REF01_CENSUS_SOURCE_V1'
 OR t.input_versions->>'scope_sha256' IS DISTINCT FROM '8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd'

 OR t.input_versions->>'policy_sha256' IS DISTINCT FROM '3e49d661fcdb8533e5ce88a85689b41e9dde71eeae2dc8578c9e31f9ae1738bb'
 OR t.input_versions->>'cohort_sha256' IS DISTINCT FROM '2dc0f766befed6de6b052221c3c87482d507ba1bad931cbb98654eb3af899bec'
 OR t.input_versions->>'membership_acceptance_sha256' IS DISTINCT FROM '20b9dac759857e9b510ccaeca33e4a94d8c748d588fb0cc27740cc66c389b5f9' THEN RAISE EXCEPTION 'REF01_CENSUS_SCOPE_OR_CODE_NOT_ADMITTED';END IF;
 SELECT * INTO STRICT r FROM population_top3000_202608_202609_v1.ref01_date_request_v4 WHERE request_key=t.payload->>'request_key';
 SELECT p.* INTO STRICT plan FROM population_top3000_202608_202609_v1.ref01_date_plan_v9 p
 JOIN population_top3000_202608_202609_v1.ref01_date_plan_request_v9 pr USING(plan_id)
 WHERE pr.request_key=r.request_key AND pr.as_of=(r.manifest->>'as_of')::date
 AND pr.manifest_sha256=r.manifest_sha256 AND pr.input_versions_sha256=encode(sha256(convert_to(r.input_versions::text,'UTF8')),'hex');
 IF (t.stage='ACQUIRE' AND plan.enabled IS DISTINCT FROM true) OR plan.content_sha256 IS DISTINCT FROM encode(sha256(convert_to(plan.content::text,'UTF8')),'hex')
 OR plan.code_bundle_sha256 IS DISTINCT FROM population_top3000_202608_202609_v1.ref01_date_bundle_v4()
 OR plan.content->>'owner_id' IS DISTINCT FROM 'top3000-completion-20261024-root-5db48b32d103'
 OR plan.content->>'owner_fence' IS DISTINCT FROM '3' OR plan.content->>'stop_generation' IS DISTINCT FROM '1'
 THEN RAISE EXCEPTION 'REF01_CENSUS_PLAN_CODE_OR_AUTHORITY_CHANGED';END IF;

 IF r.input_versions IS DISTINCT FROM t.input_versions OR r.manifest_sha256 IS DISTINCT FROM t.payload->>'request_manifest_sha256'
 OR r.manifest_sha256 IS DISTINCT FROM encode(sha256(convert_to(r.manifest::text,'UTF8')),'hex')
 OR r.request_key IS DISTINCT FROM 'ref01-date-v4:'||r.manifest_sha256 THEN RAISE EXCEPTION 'REF01_CENSUS_MANIFEST_CHANGED';END IF;
 IF r.manifest->>'as_of' IS NULL OR NOT (plan.content->'dates' ? (r.manifest->>'as_of')) OR r.manifest->>'as_of'!~'^2026-(08|09)-[0-9]{2}$'
 OR r.manifest->'active' IS DISTINCT FROM 'true'::jsonb OR r.manifest->>'population' IS DISTINCT FROM 'ALL_PROVIDER_HISTORICAL_REFERENCE_NO_COHORT_FILTER'
 OR coalesce(r.manifest->>'max_pages','')!~'^[0-9]{1,6}$' OR (r.manifest->>'max_pages')::integer NOT BETWEEN 1 AND 100000
 THEN RAISE EXCEPTION 'REF01_CENSUS_EXACT_QUERY_REQUIRED';END IF;
 asof:=(r.manifest->>'as_of')::date;
 IF asof<DATE '2026-08-01' OR asof>DATE '2026-09-30' THEN RAISE EXCEPTION 'REF01_CENSUS_ASOF_OUT_OF_SCOPE';END IF;
 first_url:='https://api.massive.com/v3/reference/tickers?date='||asof::text||'&sort=ticker&limit=1000&order=asc&active=true&locale=us&market=stocks';
 IF r.manifest->>'first_url' IS DISTINCT FROM first_url OR r.manifest->>'max_response_bytes' IS DISTINCT FROM '8388608'
 OR r.manifest->>'max_runtime_ms' IS DISTINCT FROM '45000' THEN RAISE EXCEPTION 'REF01_CENSUS_QUERY_BOUNDS_CHANGED';END IF;
 k:='ref01-date-archive-v4:'||encode(sha256(convert_to(r.request_key||E'\n'||r.input_versions::text,'UTF8')),'hex');
 IF t.payload ? 'source_page_number' THEN
  IF coalesce(t.payload->>'source_page_number','')!~'^[0-9]{1,6}$' OR t.stage='ACQUIRE' THEN RAISE EXCEPTION 'REF01_CENSUS_PAGE_TASK_INVALID';END IF;
  page_only:=(t.payload->>'source_page_number')::integer;
  IF page_only NOT BETWEEN 1 AND (r.manifest->>'max_pages')::integer OR t.partition_key IS DISTINCT FROM r.request_key||':page:'||page_only::text THEN RAISE EXCEPTION 'REF01_CENSUS_PAGE_TASK_INVALID';END IF;
 ELSE
  IF t.partition_key IS DISTINCT FROM r.request_key THEN RAISE EXCEPTION 'REF01_CENSUS_STREAM_TASK_INVALID';END IF;
 END IF;
 IF t.stage<>'ACQUIRE' THEN RETURN r.manifest||jsonb_build_object('state','READY','request_key',r.request_key,'request_manifest_sha256',r.manifest_sha256,'archive_key',k,'family_id',t.family_id,'partition_key',t.partition_key,'source_page_number',page_only);END IF;
 SELECT * INTO STRICT admission FROM population_top3000_202608_202609_v1.ref01_date_admission_v4 WHERE request_key=r.request_key;
 SELECT * INTO STRICT ctl FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton;
 IF admission.owner_id IS DISTINCT FROM ctl.owner_id OR admission.owner_fence IS DISTINCT FROM ctl.owner_fence OR admission.stop_generation IS DISTINCT FROM ctl.stop_generation THEN RAISE EXCEPTION 'REF01_CENSUS_TRANCHE_OWNER_CHANGED';END IF;
 SELECT count(*) INTO used_calls FROM population_top3000_202608_202609_v1.pipeline_provider_lease l JOIN population_top3000_202608_202609_v1.pipeline_attempt a ON a.attempt_id=l.attempt_id JOIN population_top3000_202608_202609_v1.pipeline_task pt ON pt.task_id=a.task_id WHERE pt.family_id='REF01' AND pt.payload->>'request_key'=r.request_key AND pt.stage='ACQUIRE' AND l.provider='Massive' AND l.attempt_id IS DISTINCT FROM t.attempt_id;
 IF coalesce(t.checkpoint->>'next_page','1')!~'^[0-9]{1,6}$' THEN RAISE EXCEPTION 'REF01_CENSUS_CHECKPOINT_INVALID';END IF;
 n:=coalesce(t.checkpoint->>'next_page','1')::integer;lim:=(r.manifest->>'max_pages')::integer;
 IF n NOT BETWEEN 1 AND 100001 THEN RAISE EXCEPTION 'REF01_CENSUS_PAGE_BOUND';END IF;
 IF n=1 THEN url:=first_url;ELSE
  SELECT * INTO prior FROM population_top3000_202608_202609_v1.ref01_date_page_v4 WHERE archive_key=k AND page_number=n-1;
  IF NOT FOUND OR prior.next_url IS NULL OR t.checkpoint->>'last_source_id' IS DISTINCT FROM prior.source_id
  OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.source_document d WHERE d.source_id=prior.source_id AND d.source_sha256=prior.source_sha256 AND d.source_sha256=t.checkpoint->>'last_source_sha256' AND d.source_sha256=encode(sha256(convert_to(d.content_text,'UTF8')),'hex') AND d.content_bytes=octet_length(d.content_text))
  THEN RAISE EXCEPTION 'REF01_CENSUS_CONTINUATION_PROOF_MISSING';END IF;
  url:=prior.next_url;
  IF url!~'^https://api[.]massive[.]com/v3/reference/tickers[?]cursor=[A-Za-z0-9_.~%+\-]+$' OR length(url)>16384 THEN RAISE EXCEPTION 'REF01_CENSUS_CURSOR_SCOPE_INVALID';END IF;
 END IF;
 RETURN r.manifest||jsonb_build_object('state',CASE WHEN (SELECT coalesce(sum(wire_bytes),0)+8388608 FROM population_top3000_202608_202609_v1.ref01_date_page_v4)>(plan.content->>'max_source_bytes')::bigint OR 16777216+(SELECT coalesce(sum(wire_bytes),0)*2 FROM population_top3000_202608_202609_v1.ref01_date_page_v4)+(SELECT coalesce(sum(pg_total_relation_size(oid)),0) FROM pg_class WHERE relnamespace='population_top3000_202608_202609_v1'::regnamespace AND (relname LIKE 'ref01_date_%' OR relname='ref01_practical_dated_instrument_v3') AND relkind='r')>(plan.content->>'max_relation_bytes')::bigint OR n>lim OR n>admission.admitted_page_ceiling OR used_calls>=admission.max_provider_calls OR admission.expires_at<=clock_timestamp() THEN 'PAGE_LIMIT_REVIEW' ELSE 'READY' END,'source_admission_expires_at',admission.expires_at,'maximum_http_invocations',(plan.content->>'max_http_invocations')::bigint,'maximum_provider_calls',(plan.content->>'max_provider_calls')::bigint,'request_key',r.request_key,'request_manifest_sha256',r.manifest_sha256,'archive_key',k,'page_number',n,'family_id',t.family_id,'partition_key',t.partition_key,'request_url',url,'provider_calls_used',used_calls,'max_provider_calls',admission.max_provider_calls,'admitted_page_ceiling',admission.admitted_page_ceiling);
END; $function$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.ref01_date_acquire_v4(p_attempt uuid, p_token text)
 RETURNS jsonb
 LANGUAGE plpgsql
 SET search_path TO 'pg_catalog'
AS $function$
DECLARE tid uuid;c jsonb;ctl population_top3000_202608_202609_v1.acquisition_control%rowtype;used int;rid bigint;
BEGIN
 tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);c:=population_top3000_202608_202609_v1.ref01_date_context_v4(tid);
 SELECT * INTO STRICT ctl FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton;
 SELECT count(*) INTO used FROM population_top3000_202608_202609_v1.pipeline_attempt a JOIN population_top3000_202608_202609_v1.pipeline_task t USING(task_id) WHERE t.payload->>'kind'='REF01_CENSUS_SOURCE_V1' AND a.request_id IS NOT NULL;
 IF c->>'state'<>'READY' OR (c->>'source_admission_expires_at')::timestamptz<=clock_timestamp()+interval '120seconds' OR used>=(c->>'maximum_http_invocations')::bigint OR ctl.http_invocations>=ctl.max_http_invocations THEN RETURN jsonb_build_object('state','BLOCKED','blocker_code','REF01_DATE_FINITE_ADMISSION_REVIEW_V4','resumption_condition',jsonb_build_object('request_key',c->>'request_key','next_page',c->'page_number','used_http',used,'engineering_assignment_event_id',(SELECT input_versions->'assignment_event_id' FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=tid)));END IF;
 IF ctl.next_provider_at>clock_timestamp() OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_provider_lease WHERE released_at IS NULL) THEN RETURN jsonb_build_object('state','BLOCKED','blocker_code','DEPENDENCY_PENDING','recheck_at',greatest(ctl.next_provider_at,clock_timestamp()+interval '5 seconds'),'resumption_condition',jsonb_build_object('event','PROVIDER_SLOT_AVAILABLE'));END IF;
 SELECT net.http_post(url:='https://oxzabweahkoimtevbbny.supabase.co/functions/v1/population-top3000-instrument-dates-v4',headers:='{"Content-Type":"application/json"}',body:=jsonb_build_object('attempt_id',p_attempt,'token',p_token),timeout_milliseconds:=55000) INTO rid;
 UPDATE population_top3000_202608_202609_v1.acquisition_control SET http_invocations=http_invocations+1 WHERE singleton;
 UPDATE population_top3000_202608_202609_v1.pipeline_attempt SET request_id=rid WHERE attempt_id=p_attempt;
 RETURN jsonb_build_object('state','ASYNC','request_id',rid,'worker','population-top3000-instrument-dates-v4');
END $function$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.ref01_date_commit_v4(p_attempt uuid, p_token text, p_page jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 SET search_path TO 'pg_catalog'
AS $function$
DECLARE tid uuid;t population_top3000_202608_202609_v1.pipeline_task%rowtype;c jsonb;body jsonb;txt text;h text;wire text;
 sid text;k text;n integer;cnt integer;bytes bigint;stored_bytes bigint;v_next_url text;at_time timestamptz;fresh integer;receipt jsonb;source_evidence jsonb;
BEGIN
 tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);
 SELECT * INTO t FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=tid;
 IF t.stage<>'ACQUIRE' THEN RAISE EXCEPTION 'REF01_CENSUS_STAGE_MISMATCH';END IF;
 c:=population_top3000_202608_202609_v1.ref01_date_context_v4(tid);
 PERFORM population_top3000_202608_202609_v1.d02_daily_resource_guard_v2();
 IF c->>'state'<>'READY' OR jsonb_typeof(p_page) IS DISTINCT FROM 'object' OR p_page->>'archive_key' IS DISTINCT FROM c->>'archive_key'
 OR p_page->>'page_number' IS DISTINCT FROM c->>'page_number' OR p_page->>'request_url' IS DISTINCT FROM c->>'request_url'
 OR p_page->>'request_manifest_sha256' IS DISTINCT FROM c->>'request_manifest_sha256' OR p_page->>'http_status' IS DISTINCT FROM '200' THEN RAISE EXCEPTION 'REF01_CENSUS_RECEIPT_SCOPE_MISMATCH';END IF;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_provider_lease WHERE attempt_id=p_attempt AND provider='Massive' AND released_at IS NULL AND reserved_bytes=(c->>'max_response_bytes')::bigint) THEN RAISE EXCEPTION 'REF01_CENSUS_RESERVATION_REQUIRED';END IF;
 txt:=p_page->>'content_text';stored_bytes:=octet_length(txt);bytes:=(p_page->>'wire_bytes')::bigint;wire:=p_page->>'wire_sha256';
 IF txt IS NULL OR stored_bytes>(c->>'max_response_bytes')::bigint OR bytes IS NULL OR bytes<1 OR bytes>(c->>'max_response_bytes')::bigint OR wire IS NULL OR wire!~'^[0-9a-f]{64}$'
 OR coalesce((p_page->>'elapsed_ms')::bigint,-1) NOT BETWEEN 0 AND 45000 THEN RAISE EXCEPTION 'REF01_CENSUS_SOURCE_BOUND_INVALID';END IF;
 h:=encode(sha256(convert_to(txt,'UTF8')),'hex');
 IF wire IS DISTINCT FROM h OR bytes IS DISTINCT FROM stored_bytes THEN RAISE EXCEPTION 'REF01_EXACT_ORIGINAL_REQUIRED';END IF;
 IF h IS DISTINCT FROM p_page->>'source_sha256' THEN RAISE EXCEPTION 'REF01_CENSUS_STORED_HASH_MISMATCH';END IF;
 body:=txt::jsonb;
 IF body->>'status' IS DISTINCT FROM 'OK' OR jsonb_typeof(body->'results') IS DISTINCT FROM 'array' THEN RAISE EXCEPTION 'REF01_CENSUS_INVALID_SOURCE_ENVELOPE';END IF;
 cnt:=jsonb_array_length(body->'results');
 IF cnt>1000 OR cnt IS DISTINCT FROM (p_page->>'result_count')::integer OR EXISTS(SELECT 1 FROM jsonb_array_elements(body->'results') v WHERE jsonb_typeof(v) IS DISTINCT FROM 'object') THEN RAISE EXCEPTION 'REF01_CENSUS_SOURCE_RECORD_SHAPE';END IF;
 v_next_url:=p_page->>'next_url';
 IF v_next_url IS DISTINCT FROM body->>'next_url' OR (v_next_url IS NOT NULL AND (v_next_url!~'^https://api[.]massive[.]com/v3/reference/tickers[?]cursor=[A-Za-z0-9_.~%+\-]+$' OR length(v_next_url)>16384 OR v_next_url=c->>'request_url')) THEN RAISE EXCEPTION 'REF01_CENSUS_CURSOR_MISMATCH';END IF;
 IF txt~* '"(apiKey|api_key|access_token|authorization)"[[:space:]]*:' OR txt~* '[?&](apiKey|api_key|access_token)=' THEN RAISE EXCEPTION 'REF01_CENSUS_UNREDACTED_CREDENTIAL_FIELD';END IF;
 k:=c->>'archive_key';n:=(c->>'page_number')::integer;sid:=k||':page:'||n::text;at_time:=(p_page->>'retrieved_at')::timestamptz;
 IF at_time IS NULL OR at_time<(SELECT started_at FROM population_top3000_202608_202609_v1.pipeline_attempt WHERE attempt_id=p_attempt)-interval '30 seconds' OR at_time>clock_timestamp()+interval '30 seconds' THEN RAISE EXCEPTION 'REF01_CENSUS_RETRIEVAL_CLOCK_INVALID';END IF;
 IF v_next_url IS NOT NULL AND EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.ref01_date_page_v4 WHERE archive_key=k AND request_url=v_next_url) THEN RAISE EXCEPTION 'REF01_CENSUS_CURSOR_CYCLE';END IF;
 source_evidence:=jsonb_build_object('wire_sha256',wire,'wire_bytes',bytes,'stored_content_bytes',stored_bytes,'hash_representation','EXACT_UTF8_ORIGINAL_RESPONSE','wire_bytes_retained',true,'provider_request_id',p_page->>'provider_request_id','scope','Full historical active stock ticker census; not complete inactive instrument history','request_manifest_sha256',c->>'request_manifest_sha256','as_of',c->>'as_of','active',true,'source_completeness_certified',false,'no_trade_inferred',false,'original_historical_receipt',false);
 INSERT INTO population_top3000_202608_202609_v1.source_document
 (source_id,source_sha256,source_uri,source_provider,retrieved_at,source_published_at,historical_received_at,content_text,content_bytes,evidence,license_policy)
 VALUES(sid,h,c->>'request_url','Massive',at_time,NULL,NULL,txt,stored_bytes,
 source_evidence,
 'EXISTING_MASSIVE_SUBSCRIPTION_API_RESPONSE_INTERNAL_USE') ON CONFLICT(source_id) DO NOTHING;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.source_document d WHERE d.source_id=sid AND d.source_sha256=h AND d.source_uri=c->>'request_url' AND d.content_text=txt AND d.content_bytes=stored_bytes AND d.source_provider='Massive' AND d.retrieved_at=at_time AND d.source_published_at IS NULL AND d.historical_received_at IS NULL AND d.evidence=source_evidence AND d.license_policy='EXISTING_MASSIVE_SUBSCRIPTION_API_RESPONSE_INTERNAL_USE') THEN RAISE EXCEPTION 'REF01_CENSUS_SOURCE_REVISION_CONFLICT';END IF;
 INSERT INTO population_top3000_202608_202609_v1.ref01_date_page_v4
 (archive_key,family_id,partition_key,page_number,acquire_task_id,attempt_id,source_id,source_sha256,request_url,next_url,wire_sha256,wire_bytes,result_count,retrieved_at)
 VALUES(k,t.family_id,t.partition_key,n,tid,p_attempt,sid,h,c->>'request_url',v_next_url,wire,bytes,cnt,at_time)
 ON CONFLICT(archive_key,page_number) DO NOTHING;
 GET DIAGNOSTICS fresh=ROW_COUNT;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.ref01_date_page_v4 q WHERE q.archive_key=k AND q.page_number=n AND q.source_id=sid AND q.source_sha256=h AND q.wire_sha256=wire AND q.wire_bytes=bytes AND q.request_url=c->>'request_url' AND q.next_url IS NOT DISTINCT FROM v_next_url AND q.result_count=cnt AND q.family_id=t.family_id AND q.partition_key=t.partition_key AND q.acquire_task_id=tid AND q.attempt_id=p_attempt AND q.retrieved_at=at_time) THEN RAISE EXCEPTION 'REF01_CENSUS_PAGE_REVISION_CONFLICT';END IF;
 PERFORM population_top3000_202608_202609_v1.d02_daily_resource_guard_v2();
 PERFORM population_top3000_202608_202609_v1.pipeline_provider_release_v1(p_attempt,p_token,stored_bytes,0,0);
 PERFORM population_top3000_202608_202609_v1.pipeline_enqueue_v1('REF01','NORMALIZE',t.partition_key||':page:'||n::text,
 t.payload||jsonb_build_object('source_page_number',n),t.input_versions,NULL);
 receipt:=jsonb_build_object('state',CASE WHEN v_next_url IS NULL THEN 'COMPLETE' ELSE 'CONTINUE' END,
 'checkpoint',jsonb_build_object('next_page',n+1,'last_source_id',sid,'last_source_sha256',h,'next_url',v_next_url,'source_records_seen',coalesce((t.checkpoint->>'source_records_seen')::bigint,0)+cnt),
 'not_before',(SELECT next_provider_at FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton),
 'commit_receipt',jsonb_build_object('commit_key','massive-page:'||sid||':'||h,'output_sha256',h,'new_committed_units',fresh,'immutable_output_ids',jsonb_build_array(sid),'input_versions',t.input_versions,'raw_source_records',cnt,'terminal_page',v_next_url IS NULL,'cohort_certified',false,'reference_lifecycle_qualified',false,'original_receipt_qualified',false));
 RETURN population_top3000_202608_202609_v1.pipeline_finish_v1(p_attempt,p_token,receipt);
END; $function$;
