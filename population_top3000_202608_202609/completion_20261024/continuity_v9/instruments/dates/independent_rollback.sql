BEGIN;
SET LOCAL statement_timeout='55s'; SET LOCAL lock_timeout='5s';
SELECT pg_advisory_xact_lock(2026083000261022);
CREATE TABLE population_top3000_202608_202609_v1.ref01_date_plan_v9(
 plan_id text PRIMARY KEY, content jsonb NOT NULL, content_sha256 text NOT NULL,
 code_bundle_sha256 text NOT NULL, enabled boolean NOT NULL DEFAULT false,
 last_blocker text, updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 CHECK(content_sha256=encode(sha256(convert_to(content::text,'UTF8')),'hex')),
 CHECK(code_bundle_sha256 ~ '^[0-9a-f]{64}$'),
 CHECK(jsonb_typeof(content->'dates')='array' AND jsonb_array_length(content->'dates')=60),
 CHECK(content ?& ARRAY['max_provider_calls','max_http_invocations','max_source_bytes','max_relation_bytes','max_pages_per_date','initial_page_tranche','renewal_page_tranche','owner_id','owner_fence','stop_generation','dates','input_versions']),
 CHECK((content->>'max_provider_calls')::bigint BETWEEN 1 AND 1000000 AND (content->>'max_http_invocations')::bigint BETWEEN 1 AND 1000000),
 CHECK((content->>'max_source_bytes')::bigint BETWEEN 8388608 AND 100000000000 AND (content->>'max_relation_bytes')::bigint BETWEEN 16777216 AND 1000000000000),
 CHECK((content->>'max_pages_per_date')::int BETWEEN 1 AND 100000 AND (content->>'initial_page_tranche')::int BETWEEN 1 AND 100000 AND (content->>'renewal_page_tranche')::int BETWEEN 1 AND 100000)
);
CREATE UNIQUE INDEX ref01_date_one_enabled_plan_v9 ON population_top3000_202608_202609_v1.ref01_date_plan_v9(enabled) WHERE enabled;
CREATE TABLE population_top3000_202608_202609_v1.ref01_date_plan_request_v9(
 plan_id text NOT NULL REFERENCES population_top3000_202608_202609_v1.ref01_date_plan_v9,
 as_of date NOT NULL, request_key text NOT NULL UNIQUE REFERENCES population_top3000_202608_202609_v1.ref01_date_request_v4,
 manifest_sha256 text NOT NULL, input_versions_sha256 text NOT NULL,
 PRIMARY KEY(plan_id,as_of), CHECK(as_of BETWEEN DATE '2026-08-01' AND DATE '2026-09-30')
);
ALTER TABLE population_top3000_202608_202609_v1.ref01_date_plan_v9 ENABLE ROW LEVEL SECURITY;
ALTER TABLE population_top3000_202608_202609_v1.ref01_date_plan_request_v9 ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON population_top3000_202608_202609_v1.ref01_date_plan_v9,population_top3000_202608_202609_v1.ref01_date_plan_request_v9 FROM PUBLIC,anon,authenticated;
;
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
;
CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.ref01_date_admit_v4(p_owner text, p_fence bigint, p_stop bigint, p_request text, p_page_ceiling integer, p_call_ceiling integer, p_expires timestamp with time zone, p_reason text)
 RETURNS jsonb
 LANGUAGE plpgsql
 SET search_path TO 'pg_catalog'
AS $function$
DECLARE a population_top3000_202608_202609_v1.acquisition_control%rowtype;
 p population_top3000_202608_202609_v1.pipeline_control%rowtype;
 old population_top3000_202608_202609_v1.ref01_date_admission_v4%rowtype;r jsonb; page_limit int;
BEGIN
 PERFORM pg_advisory_xact_lock(2026083000261022);
 SELECT * INTO STRICT a FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton FOR UPDATE;
 SELECT * INTO STRICT p FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton FOR UPDATE;
 IF p_owner IS NULL OR p_fence IS NULL OR p_stop IS NULL OR a.owner_id IS DISTINCT FROM p_owner OR a.owner_fence IS DISTINCT FROM p_fence OR a.stop_generation IS DISTINCT FROM p_stop
 OR a.stopped IS DISTINCT FROM false OR a.enabled IS DISTINCT FROM true OR p.enabled IS DISTINCT FROM true OR a.hold_reason IS NOT NULL
 OR a.admitted_until IS NULL OR a.admitted_until<=clock_timestamp() OR p.deadline IS NULL OR p.deadline<=clock_timestamp()
 OR p.expected_owner_id IS DISTINCT FROM a.owner_id OR p.expected_owner_fence IS DISTINCT FROM a.owner_fence OR p.expected_stop_generation IS DISTINCT FROM a.stop_generation
 OR a.scope_sha256 IS DISTINCT FROM '8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd' OR p.scope_sha256 IS DISTINCT FROM a.scope_sha256
 OR p.shared_scheduler_verified IS DISTINCT FROM true OR population_top3000_202608_202609_v1.pipeline_cost_admission_v2(p,0) IS DISTINCT FROM true THEN RAISE EXCEPTION 'REF01_CENSUS_CURRENT_CONTROLLER_ADMISSION_REQUIRED';END IF;
 PERFORM population_top3000_202608_202609_v1.d02_daily_resource_guard_v2();
 SELECT least((prq.manifest->>'max_pages')::integer,(pl.content->>'max_pages_per_date')::integer) INTO STRICT page_limit
 FROM population_top3000_202608_202609_v1.ref01_date_plan_v9 pl
 JOIN population_top3000_202608_202609_v1.ref01_date_plan_request_v9 pr USING(plan_id)
 JOIN population_top3000_202608_202609_v1.ref01_date_request_v4 prq USING(request_key)
 WHERE pr.request_key=p_request AND pl.enabled
 AND pl.content_sha256=encode(sha256(convert_to(pl.content::text,'UTF8')),'hex')
 AND pl.code_bundle_sha256=population_top3000_202608_202609_v1.ref01_date_bundle_v4();
 SELECT * INTO old FROM population_top3000_202608_202609_v1.ref01_date_admission_v4 WHERE request_key=p_request FOR UPDATE;
 IF p_page_ceiling IS NULL OR p_call_ceiling IS NULL OR p_page_ceiling NOT BETWEEN greatest(1,coalesce(old.admitted_page_ceiling,0)) AND least(100000,page_limit)
 OR p_call_ceiling NOT BETWEEN greatest(1,coalesce(old.max_provider_calls,0)) AND least(100000,page_limit)
 OR p_expires IS NULL OR p_expires<=clock_timestamp() OR p_expires>least(a.admitted_until,p.deadline,clock_timestamp()+interval '30 minutes')
 OR p_reason IS NULL OR length(p_reason) NOT BETWEEN 10 AND 1000
 OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.ref01_date_request_v4 x WHERE x.request_key=p_request AND x.manifest_sha256=encode(sha256(convert_to(x.manifest::text,'UTF8')),'hex') AND (x.manifest->>'max_pages')::integer>=p_page_ceiling)
 THEN RAISE EXCEPTION 'REF01_CENSUS_FINITE_TRANCHE_INVALID';END IF;
 INSERT INTO population_top3000_202608_202609_v1.ref01_date_admission_v4(request_key,owner_id,owner_fence,stop_generation,admitted_page_ceiling,max_provider_calls,expires_at)
 VALUES(p_request,p_owner,p_fence,p_stop,p_page_ceiling,p_call_ceiling,p_expires)
 ON CONFLICT(request_key) DO UPDATE SET owner_id=excluded.owner_id,owner_fence=excluded.owner_fence,stop_generation=excluded.stop_generation,admitted_page_ceiling=excluded.admitted_page_ceiling,max_provider_calls=excluded.max_provider_calls,expires_at=excluded.expires_at,updated_at=clock_timestamp();
 r:=jsonb_build_object('request_key',p_request,'owner_id',p_owner,'owner_fence',p_fence,'stop_generation',p_stop,'prior',to_jsonb(old),'admitted_page_ceiling',p_page_ceiling,'max_provider_calls',p_call_ceiling,'expires_at',p_expires,'reason',p_reason,'source_denominator_changed',false,'cumulative_counters_preserved',true);
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details)VALUES('REF01_CENSUS_TRANCHE_ADMITTED_V1',r);
 PERFORM population_top3000_202608_202609_v1.d02_daily_resource_guard_v2();
 RETURN r;
END;$function$

;
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
;
CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.ref01_date_continue_v9(p_owner text,p_fence bigint,p_stop bigint)
RETURNS jsonb LANGUAGE plpgsql SET search_path=pg_catalog AS $f$
DECLARE ctl jsonb; result jsonb; err text; errstate text; manifest jsonb;h text;integrity_fault boolean;
BEGIN
 -- Owner, user stop, scope and global admission rejection are outside fault isolation.
 ctl:=population_top3000_202608_202609_v1.continuation_guard_v6(p_owner,p_fence,p_stop);
 BEGIN
  result:=population_top3000_202608_202609_v1.ref01_date_continue_impl_v9(p_owner,p_fence,p_stop);
  RETURN result;
 EXCEPTION WHEN OTHERS THEN
  GET STACKED DIAGNOSTICS err=MESSAGE_TEXT,errstate=RETURNED_SQLSTATE;
  -- The failed implementation subtransaction has no committed effects. Hold only its source lane.
  manifest:=jsonb_build_array(jsonb_build_object('plan_ids',(SELECT jsonb_agg(plan_id ORDER BY plan_id) FROM population_top3000_202608_202609_v1.ref01_date_plan_v9 WHERE enabled),
   'error_state',errstate,'error',left(err,500),'installed_bundle',population_top3000_202608_202609_v1.ref01_date_bundle_v4()));
  h:=encode(sha256(convert_to(manifest::text,'UTF8')),'hex');
  SELECT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.ref01_date_plan_v9 p WHERE p.enabled AND
   (p.code_bundle_sha256 IS DISTINCT FROM population_top3000_202608_202609_v1.ref01_date_bundle_v4()
    OR p.content_sha256 IS DISTINCT FROM encode(sha256(convert_to(p.content::text,'UTF8')),'hex')
    OR p.content->>'owner_id' IS DISTINCT FROM p_owner OR p.content->>'owner_fence' IS DISTINCT FROM p_fence::text OR p.content->>'stop_generation' IS DISTINCT FROM p_stop::text)) INTO integrity_fault;
  UPDATE population_top3000_202608_202609_v1.ref01_date_plan_v9 SET enabled=false,last_blocker='IMPLEMENTATION_REPAIR',updated_at=clock_timestamp() WHERE enabled;
  UPDATE population_top3000_202608_202609_v1.pipeline_task SET status='BLOCKED',blocker_code=CASE WHEN integrity_fault THEN 'CODE_INTEGRITY' ELSE 'REF01_DATE_PLAN_IMPLEMENTATION_HOLD_V9' END,blocker_until=NULL,
   resumption_condition=jsonb_build_object('clearing_id','ref01-date-code:'||h,'required_action','Repair and independently verify exact dated-source plan; preserve source hashes, attempts and counters')
   WHERE family_id='REF01' AND payload->>'kind'='REF01_CENSUS_SOURCE_V1' AND (stage='ACQUIRE' OR integrity_fault) AND status='READY';
  INSERT INTO population_top3000_202608_202609_v1.continuation_clear_queue_v6(clearing_id,kind,family_id,blocker_code,input_manifest,input_sha256,status,owner_id,owner_fence,stop_generation,scope_sha256,not_after,last_receipt)
  VALUES('ref01-date-code:'||h,'IMPLEMENTATION_REPAIR','REF01','REF01_DATE_PLAN_IMPLEMENTATION_HOLD_V9',manifest,h,'READY',p_owner,p_fence,p_stop,ctl->>'scope_sha256',(ctl->>'not_after')::timestamptz,
   jsonb_build_object('effects_known',true,'failed_handler_transaction_rolled_back',true,'execution_recipe','Reconcile exact dated source plan code/hash or malformed-state failure. Implement narrowly reviewed correction; prove rollback tests, retain source originals and counts, then re-enable exact plan and only named implementation holds. Independent mission lanes continue.')) ON CONFLICT DO NOTHING;
  INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details)
   SELECT 'REF01_DATE_PLAN_FAILURE_ISOLATED_V9',ctl||jsonb_build_object('clearing_id','ref01-date-code:'||h,'error',left(err,500),'error_state',errstate,'failed_subtransaction_rolled_back',true,'family_accepted',false)
   WHERE NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_event WHERE event='REF01_DATE_PLAN_FAILURE_ISOLATED_V9' AND details->>'clearing_id'='ref01-date-code:'||h);
  RETURN jsonb_build_object('state','REF01_PLAN_HELD_REPAIR_DISPATCHED','clearing_id','ref01-date-code:'||h);
 END;
END;$f$;
REVOKE ALL ON FUNCTION population_top3000_202608_202609_v1.ref01_date_continue_v9(text,bigint,bigint) FROM PUBLIC,anon,authenticated;
;
CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.ref01_date_bundle_v4()
 RETURNS text
 LANGUAGE sql
 STABLE
 SET search_path TO 'pg_catalog'
AS $function$ SELECT encode(sha256(convert_to(string_agg(pg_get_functiondef(oid),E'\n' ORDER BY proname),'UTF8')),'hex') FROM pg_proc WHERE pronamespace='population_top3000_202608_202609_v1'::regnamespace AND proname IN('ref01_date_context_v4','ref01_date_claim_v4','ref01_date_commit_v4','ref01_date_failure_v4','ref01_date_acquire_v4','ref01_date_process_v4','ref01_date_admit_v4','ref01_date_continue_v9','ref01_date_continue_impl_v9','ref01_date_bundle_v4'); $function$

;
DO $f$
DECLARE p jsonb;dates jsonb;iv jsonb;
BEGIN
 PERFORM population_top3000_202608_202609_v1.continuation_guard_v6('top3000-completion-20261024-root-5db48b32d103',3,1);
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.ref01_date_plan_v9) THEN RAISE EXCEPTION 'REF01_DATE_PLAN_ALREADY_EXISTS';END IF;
 SELECT jsonb_agg(d::date::text ORDER BY d) INTO dates FROM generate_series(DATE '2026-08-01',DATE '2026-09-30',INTERVAL '1day') d WHERE d::date<>DATE '2026-08-03';
 iv:=jsonb_build_object('scope_sha256','8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd','policy_sha256','3e49d661fcdb8533e5ce88a85689b41e9dde71eeae2dc8578c9e31f9ae1738bb','cohort_sha256','2dc0f766befed6de6b052221c3c87482d507ba1bad931cbb98654eb3af899bec','membership_acceptance_sha256','20b9dac759857e9b510ccaeca33e4a94d8c748d588fb0cc27740cc66c389b5f9','assignment_event_id',28605,'engineering_successor_event_id',28605);
 p:=jsonb_build_object('revision','REF01_FULL_PERIOD_DATED_SOURCE_CONTINUATION_V9','owner_id','top3000-completion-20261024-root-5db48b32d103','owner_fence',3,'stop_generation',1,'authority_event_id',9528,'assignment_event_id',28605,'root_claim_event_id',29634,'source_admission_basis','New independently reviewed fullperiod finite plan under current user authority9528 and rootclaim29634;1200 cumulativecalls supersede inherited144-call assignment allowance only after current root records this plan admission.',
 'dates',dates,'retained_aug3_anchor','27c81878e9ea5ce6eb05ead025f5c9aaae11108af50ba59ecd65b3402c877c86','input_versions',iv,
 'max_provider_calls',1200,'max_http_invocations',1200,'max_source_bytes',300000000,'max_relation_bytes',3000000000,
 'initial_page_tranche',14,'renewal_page_tranche',10,'max_pages_per_date',24,'expected_date_count',60,
 'measurement',jsonb_build_object('retained_pages',40,'retained_rows',38096,'retained_wire_bytes',11268945,'date_relation_bytes',60907520,'dated_relation_bytes',21127168,'measurement_utc','2026-10-24T15:24:00Z'),
 'planning_basis','40 retained pages measured; 840 pages expected across 60 date censuses, <=1200 finite cumulative requests. 300MB source and 3GB combined relation plan ceiling (relation+2*raw+16MiB); 40-page measured~82MB relations scales to~1.72GB across840pages plus~473MB source allowance and16MiB margin, about2.21GB forecast;3GB includes~36percent margin; global limits remain binding and trigger deduplicated executable clearing work. Every date includes all active provider stocks; no inactive history or family acceptance inferred.');
 INSERT INTO population_top3000_202608_202609_v1.ref01_date_plan_v9(plan_id,content,content_sha256,code_bundle_sha256,enabled)
 VALUES('ref01-date-fullperiod-v9',p,encode(sha256(convert_to(p::text,'UTF8')),'hex'),population_top3000_202608_202609_v1.ref01_date_bundle_v4(),true);
 INSERT INTO population_top3000_202608_202609_v1.ref01_date_plan_request_v9(plan_id,as_of,request_key,manifest_sha256,input_versions_sha256)
 SELECT 'ref01-date-fullperiod-v9',(manifest->>'as_of')::date,request_key,manifest_sha256,encode(sha256(convert_to(input_versions::text,'UTF8')),'hex')
 FROM population_top3000_202608_202609_v1.ref01_date_request_v4
 WHERE (manifest->>'as_of')::date IN(DATE '2026-08-01',DATE '2026-08-02',DATE '2026-08-04',DATE '2026-08-05');
 IF (SELECT count(*) FROM population_top3000_202608_202609_v1.ref01_date_plan_request_v9)<>4 THEN RAISE EXCEPTION 'EXACT_LEGACY_REQUEST_POPULATION_REQUIRED';END IF;
END;$f$;
;

CREATE TEMP TABLE review_date_results(test text PRIMARY KEY,result text);
DO $t$
DECLARE t population_top3000_202608_202609_v1.pipeline_task%rowtype; orig population_top3000_202608_202609_v1.pipeline_task%rowtype;
 a population_top3000_202608_202609_v1.acquisition_control%rowtype;
 p population_top3000_202608_202609_v1.ref01_date_plan_v9%rowtype;
 r jsonb;h text;v jsonb;raw bigint;rel bigint;q uuid;aid uuid;req bigint;sha text;expiry timestamptz;n bigint;tid uuid;fk text;
BEGIN
 SELECT * INTO STRICT a FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton;
 SELECT * INTO STRICT p FROM population_top3000_202608_202609_v1.ref01_date_plan_v9 WHERE enabled;
 IF jsonb_array_length(p.content->'dates')<>60 OR p.content->'dates' ? '2026-08-03' THEN RAISE EXCEPTION 'PINNED_DATE_POPULATION';END IF;
 INSERT INTO review_date_results VALUES('60 explicit dates preserve retained Aug3 separately','PASS');
 -- Every retained normalized page remains interpretable using its frozen old input versions.
 FOR t IN SELECT * FROM population_top3000_202608_202609_v1.pipeline_task WHERE payload->>'kind'='REF01_CENSUS_SOURCE_V1' AND stage='NORMALIZE' AND payload ? 'source_page_number' LOOP
  r:=population_top3000_202608_202609_v1.ref01_date_context_v4(t.task_id);
  IF r->>'state'<>'READY' THEN RAISE EXCEPTION 'LEGACY_PAGE_CONTEXT_CHANGED';END IF;
 END LOOP;
 INSERT INTO review_date_results VALUES('Legacy retained page input hashes remain accepted by exact plan map','PASS');
 SELECT q.* INTO STRICT orig FROM population_top3000_202608_202609_v1.pipeline_task q JOIN population_top3000_202608_202609_v1.ref01_date_request_v4 r ON r.request_key=q.payload->>'request_key' WHERE q.stage='ACQUIRE' AND r.manifest->>'as_of'='2026-08-05';
 tid:=orig.task_id;aid:=orig.attempt_id;
 -- Source admission expiry is renewed with exact counters and cursor preserved.
 UPDATE population_top3000_202608_202609_v1.pipeline_task SET status='BLOCKED',blocker_code='REF01_DATE_FINITE_ADMISSION_REVIEW_V4',resumption_condition='{}'::jsonb,lease_until=NULL,cumulative_failures=0 WHERE task_id=tid;
 IF aid IS NOT NULL THEN UPDATE population_top3000_202608_202609_v1.pipeline_attempt SET outcome='BLOCKED',request_id=NULL WHERE attempt_id=aid;END IF;
 UPDATE population_top3000_202608_202609_v1.ref01_date_admission_v4 SET expires_at=clock_timestamp()-interval '1s' WHERE request_key=orig.payload->>'request_key';
 r:=population_top3000_202608_202609_v1.ref01_date_continue_v9(a.owner_id,a.owner_fence,a.stop_generation);
 IF r->>'state'<>'ADMITTED_AND_QUEUED' OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=tid AND status='READY' AND checkpoint=orig.checkpoint AND recheck_count=orig.recheck_count AND executions=orig.executions) THEN RAISE EXCEPTION 'EXPIRED_WINDOW_REQUEUE_FAILED %',r;END IF;
 INSERT INTO review_date_results VALUES('Expired exact source window renews and requeues preserving counters/cursor','PASS');
 -- Null condition stays a hold rather than becoming renewable provider wait.
 UPDATE population_top3000_202608_202609_v1.pipeline_task SET status='BLOCKED',blocker_code='DEPENDENCY_PENDING',resumption_condition='{}' WHERE task_id=tid;
 r:=population_top3000_202608_202609_v1.ref01_date_continue_v9(a.owner_id,a.owner_fence,a.stop_generation);
 IF r->>'state'<>'PRESERVED_EVIDENCE_OR_UNKNOWN_OUTCOME_HOLD' THEN RAISE EXCEPTION 'NULL_CONDITION_NOT_PRESERVED %',r;END IF;
 INSERT INTO review_date_results VALUES('Unknown/null dependency condition remains held','PASS');
 -- Changed owner and explicit user stop reject operational maintenance.
 UPDATE population_top3000_202608_202609_v1.acquisition_control SET stopped=true WHERE singleton;
 BEGIN PERFORM population_top3000_202608_202609_v1.ref01_date_continue_v9(a.owner_id,a.owner_fence,a.stop_generation);RAISE EXCEPTION 'TEST_STOP_ACCEPTED';EXCEPTION WHEN OTHERS THEN IF SQLERRM='TEST_STOP_ACCEPTED' THEN RAISE;END IF;END;
 UPDATE population_top3000_202608_202609_v1.acquisition_control SET stopped=false,owner_fence=owner_fence+1 WHERE singleton;
 BEGIN PERFORM population_top3000_202608_202609_v1.ref01_date_continue_v9(a.owner_id,a.owner_fence,a.stop_generation);RAISE EXCEPTION 'TEST_OWNER_ACCEPTED';EXCEPTION WHEN OTHERS THEN IF SQLERRM='TEST_OWNER_ACCEPTED' THEN RAISE;END IF;END;
 UPDATE population_top3000_202608_202609_v1.acquisition_control SET owner_fence=a.owner_fence WHERE singleton;
 INSERT INTO review_date_results VALUES('User stop and changed owner reject date continuation','PASS');
 -- A code bundle alteration prevents all plan maintenance.
 UPDATE population_top3000_202608_202609_v1.ref01_date_plan_v9 SET code_bundle_sha256=repeat('0',64) WHERE plan_id=p.plan_id;
 r:=population_top3000_202608_202609_v1.ref01_date_continue_v9(a.owner_id,a.owner_fence,a.stop_generation);
 IF r->>'state'<>'REF01_PLAN_HELD_REPAIR_DISPATCHED' OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.continuation_clear_queue_v6 WHERE clearing_id=r->>'clearing_id' AND kind='IMPLEMENTATION_REPAIR' AND status='READY') THEN RAISE EXCEPTION 'CODE_FAILURE_NOT_ISOLATED %',r;END IF;
 UPDATE population_top3000_202608_202609_v1.ref01_date_plan_v9 SET code_bundle_sha256=p.code_bundle_sha256,enabled=true WHERE plan_id=p.plan_id;
 INSERT INTO review_date_results VALUES('Changed code bundle isolated with executable repair and source lane held','PASS');
 -- Capacity formula must agree with context; old expression would have missed this narrow range.
 SELECT coalesce(sum(wire_bytes),0) INTO raw FROM population_top3000_202608_202609_v1.ref01_date_page_v4;
 SELECT coalesce(sum(pg_total_relation_size(oid)),0) INTO rel FROM pg_class WHERE relnamespace='population_top3000_202608_202609_v1'::regnamespace AND relkind='r' AND (relname LIKE 'ref01_date_%' OR relname='ref01_practical_dated_instrument_v3');
 v:=jsonb_set(p.content,'{max_relation_bytes}',to_jsonb(rel+raw+16777216));
 UPDATE population_top3000_202608_202609_v1.ref01_date_plan_v9 SET content=v,content_sha256=encode(sha256(convert_to(v::text,'UTF8')),'hex') WHERE plan_id=p.plan_id;
 UPDATE population_top3000_202608_202609_v1.pipeline_task SET blocker_code='REF01_DATE_FINITE_ADMISSION_REVIEW_V4' WHERE task_id=tid;
 r:=population_top3000_202608_202609_v1.ref01_date_continue_v9(a.owner_id,a.owner_fence,a.stop_generation);
 IF r->>'reason'<>'PLAN_STORAGE_REVIEW' OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.continuation_clear_queue_v6 WHERE clearing_id=r->>'clearing_id' AND status='READY' AND last_receipt->>'execution_recipe' IS NOT NULL) THEN RAISE EXCEPTION 'CAPACITY_REVIEW_NOT_EXECUTABLE %',r;END IF;
 fk:=r->>'clearing_id';
 r:=population_top3000_202608_202609_v1.ref01_date_continue_v9(a.owner_id,a.owner_fence,a.stop_generation);
 IF r->>'clearing_id'<>fk OR (SELECT count(*) FROM population_top3000_202608_202609_v1.continuation_clear_queue_v6 WHERE clearing_id=fk)<>1 THEN RAISE EXCEPTION 'CAPACITY_REVIEW_DUPLICATED';END IF;
 UPDATE population_top3000_202608_202609_v1.ref01_date_plan_v9 SET content=p.content,content_sha256=p.content_sha256 WHERE plan_id=p.plan_id;
 INSERT INTO review_date_results VALUES('Consistent capacity bound produces deduplicated executable clearing work','PASS');
 -- Explicit synthetic terminal fixture, not source acceptance evidence.
 UPDATE population_top3000_202608_202609_v1.pipeline_task SET status='COMPLETE',completed_at=clock_timestamp() WHERE task_id=tid;
 UPDATE population_top3000_202608_202609_v1.source_document d SET content_text=(d.content_text::jsonb-'next_url')::text,content_bytes=octet_length((d.content_text::jsonb-'next_url')::text),source_sha256=encode(sha256(convert_to((d.content_text::jsonb-'next_url')::text,'UTF8')),'hex') FROM population_top3000_202608_202609_v1.ref01_date_page_v4 pg WHERE d.source_id=pg.source_id AND pg.acquire_task_id=tid AND pg.page_number=(SELECT max(page_number) FROM population_top3000_202608_202609_v1.ref01_date_page_v4 WHERE acquire_task_id=tid);
 UPDATE population_top3000_202608_202609_v1.ref01_date_page_v4 SET source_sha256=d.source_sha256,wire_sha256=d.source_sha256,wire_bytes=d.content_bytes FROM population_top3000_202608_202609_v1.source_document d WHERE d.source_id=population_top3000_202608_202609_v1.ref01_date_page_v4.source_id AND acquire_task_id=tid;
 UPDATE population_top3000_202608_202609_v1.ref01_date_page_v4 SET next_url=NULL WHERE acquire_task_id=tid AND page_number=(SELECT max(page_number) FROM population_top3000_202608_202609_v1.ref01_date_page_v4 WHERE acquire_task_id=tid);
 r:=population_top3000_202608_202609_v1.ref01_date_continue_v9(a.owner_id,a.owner_fence,a.stop_generation);
 IF r->>'as_of'<>'2026-08-06' OR r->>'state'<>'ADMITTED_AND_QUEUED' THEN RAISE EXCEPTION 'NEXT_DATE_NOT_CREATED % %',r,(SELECT input_manifest FROM population_top3000_202608_202609_v1.continuation_clear_queue_v6 WHERE clearing_id=r->>'clearing_id');END IF;
 q:=(r->>'task_id')::uuid;
 r:=population_top3000_202608_202609_v1.ref01_date_context_v4(q);
 IF r->>'as_of'<>'2026-08-06' OR r->>'state'<>'READY' OR r->>'maximum_http_invocations'<>'1200' THEN RAISE EXCEPTION 'NEW_DATE_CONTEXT_FAILED %',r;END IF;
 r:=population_top3000_202608_202609_v1.ref01_date_continue_v9(a.owner_id,a.owner_fence,a.stop_generation);
 IF r->>'task_id'<>q::text OR (SELECT count(*) FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=q)<>1 THEN RAISE EXCEPTION 'NEXT_DATE_DUPLICATED';END IF;
 INSERT INTO review_date_results VALUES('Terminal source automatically admits next date beyond old whitelist idempotently','PASS');
 IF (SELECT provider_requests FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton)<>a.provider_requests OR (SELECT http_invocations FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton)<>a.http_invocations THEN RAISE EXCEPTION 'SOURCE_CALLS_OCCURRED';END IF;
 INSERT INTO review_date_results VALUES('Zero external HTTP/provider calls in review','PASS');
END $t$;
SELECT jsonb_build_object('result','PASS','reviewer','/root/instrument_review','tests',jsonb_agg(to_jsonb(r)),'rolls_back',true) as result FROM review_date_results r;
ROLLBACK;
