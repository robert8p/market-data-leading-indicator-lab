-- Historical CTX02 full reference census. Finite page admission is operational only; a nonterminal cursor never means source completeness.
-- Root installs/registers only after independent review and actual fenced rollback tests.
CREATE TABLE IF NOT EXISTS population_top3000_202608_202609_v1.ctx02_census_request_v1(
 request_key text PRIMARY KEY,manifest jsonb NOT NULL,manifest_sha256 text NOT NULL,input_versions jsonb NOT NULL,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 CHECK(manifest_sha256=encode(sha256(convert_to(manifest::text,'UTF8')),'hex')),
 CHECK(request_key='ctx02-census-v1:'||manifest_sha256));
CREATE TABLE IF NOT EXISTS population_top3000_202608_202609_v1.ctx02_census_page_v1(
 archive_key text NOT NULL,family_id text NOT NULL CHECK(family_id='CTX02'),partition_key text NOT NULL,
 page_number integer NOT NULL CHECK(page_number BETWEEN 1 AND 100000),
 acquire_task_id uuid NOT NULL REFERENCES population_top3000_202608_202609_v1.pipeline_task(task_id),
 attempt_id uuid NOT NULL REFERENCES population_top3000_202608_202609_v1.pipeline_attempt(attempt_id),
 source_id text NOT NULL REFERENCES population_top3000_202608_202609_v1.source_document(source_id),source_sha256 text NOT NULL,
 request_url text NOT NULL,next_url text,wire_sha256 text NOT NULL,wire_bytes bigint NOT NULL CHECK(wire_bytes BETWEEN 1 AND 8388608),
 result_count integer NOT NULL CHECK(result_count BETWEEN 0 AND 1000),retrieved_at timestamptz NOT NULL,
 normalized_at timestamptz,normalized_source_records integer,PRIMARY KEY(archive_key,page_number),UNIQUE(source_id));
CREATE TABLE IF NOT EXISTS population_top3000_202608_202609_v1.ctx02_census_record_v1(
 source_id text NOT NULL REFERENCES population_top3000_202608_202609_v1.source_document(source_id),
 source_ordinal integer NOT NULL CHECK(source_ordinal BETWEEN 1 AND 1000),
 payload jsonb NOT NULL,payload_sha256 text NOT NULL,
 CHECK(payload_sha256=encode(sha256(convert_to(payload::text,'UTF8')),'hex')),PRIMARY KEY(source_id,source_ordinal));
ALTER TABLE population_top3000_202608_202609_v1.ctx02_census_request_v1 ENABLE ROW LEVEL SECURITY;
ALTER TABLE population_top3000_202608_202609_v1.ctx02_census_page_v1 ENABLE ROW LEVEL SECURITY;
ALTER TABLE population_top3000_202608_202609_v1.ctx02_census_record_v1 ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON population_top3000_202608_202609_v1.ctx02_census_request_v1,population_top3000_202608_202609_v1.ctx02_census_page_v1,population_top3000_202608_202609_v1.ctx02_census_record_v1 FROM PUBLIC,anon,authenticated;


CREATE TABLE IF NOT EXISTS population_top3000_202608_202609_v1.ctx02_census_admission_v1(
 request_key text PRIMARY KEY REFERENCES population_top3000_202608_202609_v1.ctx02_census_request_v1(request_key),
 owner_id text NOT NULL,owner_fence bigint NOT NULL,stop_generation bigint NOT NULL,
 admitted_page_ceiling integer NOT NULL CHECK(admitted_page_ceiling BETWEEN 1 AND 100000),
 max_provider_calls integer NOT NULL CHECK(max_provider_calls BETWEEN 1 AND 100000),
 expires_at timestamptz NOT NULL,updated_at timestamptz NOT NULL DEFAULT clock_timestamp());
ALTER TABLE population_top3000_202608_202609_v1.ctx02_census_admission_v1 ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON population_top3000_202608_202609_v1.ctx02_census_admission_v1 FROM PUBLIC,anon,authenticated;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.ctx02_census_admit_v1(p_owner text,p_fence bigint,p_stop bigint,p_request text,p_page_ceiling integer,p_call_ceiling integer,p_expires timestamptz,p_reason text)
RETURNS jsonb LANGUAGE plpgsql SET search_path=pg_catalog AS $f$
DECLARE a population_top3000_202608_202609_v1.acquisition_control%rowtype;
 p population_top3000_202608_202609_v1.pipeline_control%rowtype;
 old population_top3000_202608_202609_v1.ctx02_census_admission_v1%rowtype;r jsonb;
BEGIN
 PERFORM pg_advisory_xact_lock(2026083000261022);
 SELECT * INTO STRICT a FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton FOR UPDATE;
 SELECT * INTO STRICT p FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton FOR UPDATE;
 IF p_owner IS NULL OR p_fence IS NULL OR p_stop IS NULL OR a.owner_id IS DISTINCT FROM p_owner OR a.owner_fence IS DISTINCT FROM p_fence OR a.stop_generation IS DISTINCT FROM p_stop
 OR a.stopped IS DISTINCT FROM false OR a.enabled IS DISTINCT FROM true OR p.enabled IS DISTINCT FROM true OR a.hold_reason IS NOT NULL
 OR a.admitted_until IS NULL OR a.admitted_until<=clock_timestamp() OR p.deadline IS NULL OR p.deadline<=clock_timestamp()
 OR p.expected_owner_id IS DISTINCT FROM a.owner_id OR p.expected_owner_fence IS DISTINCT FROM a.owner_fence OR p.expected_stop_generation IS DISTINCT FROM a.stop_generation
 OR a.scope_sha256 IS DISTINCT FROM '8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd' OR p.scope_sha256 IS DISTINCT FROM a.scope_sha256
 OR p.shared_scheduler_verified IS DISTINCT FROM true OR population_top3000_202608_202609_v1.pipeline_cost_admission_v2(p,0) IS DISTINCT FROM true THEN RAISE EXCEPTION 'CTX02_CENSUS_CURRENT_CONTROLLER_ADMISSION_REQUIRED';END IF;
 PERFORM population_top3000_202608_202609_v1.d02_daily_resource_guard_v2();
 SELECT * INTO old FROM population_top3000_202608_202609_v1.ctx02_census_admission_v1 WHERE request_key=p_request FOR UPDATE;
 IF p_page_ceiling IS NULL OR p_call_ceiling IS NULL OR p_page_ceiling NOT BETWEEN greatest(1,coalesce(old.admitted_page_ceiling,0)) AND least(100000,coalesce(old.admitted_page_ceiling,0)+200)
 OR p_call_ceiling NOT BETWEEN greatest(1,coalesce(old.max_provider_calls,0)) AND least(100000,coalesce(old.max_provider_calls,0)+200)
 OR p_expires IS NULL OR p_expires<=clock_timestamp() OR p_expires>least(a.admitted_until,p.deadline,clock_timestamp()+interval '24 hours')
 OR p_reason IS NULL OR length(p_reason) NOT BETWEEN 10 AND 1000
 OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.ctx02_census_request_v1 x WHERE x.request_key=p_request AND x.manifest_sha256=encode(sha256(convert_to(x.manifest::text,'UTF8')),'hex') AND (x.manifest->>'max_pages')::integer>=p_page_ceiling)
 THEN RAISE EXCEPTION 'CTX02_CENSUS_FINITE_TRANCHE_INVALID';END IF;
 INSERT INTO population_top3000_202608_202609_v1.ctx02_census_admission_v1(request_key,owner_id,owner_fence,stop_generation,admitted_page_ceiling,max_provider_calls,expires_at)
 VALUES(p_request,p_owner,p_fence,p_stop,p_page_ceiling,p_call_ceiling,p_expires)
 ON CONFLICT(request_key) DO UPDATE SET owner_id=excluded.owner_id,owner_fence=excluded.owner_fence,stop_generation=excluded.stop_generation,admitted_page_ceiling=excluded.admitted_page_ceiling,max_provider_calls=excluded.max_provider_calls,expires_at=excluded.expires_at,updated_at=clock_timestamp();
 r:=jsonb_build_object('request_key',p_request,'owner_id',p_owner,'owner_fence',p_fence,'stop_generation',p_stop,'prior',to_jsonb(old),'admitted_page_ceiling',p_page_ceiling,'max_provider_calls',p_call_ceiling,'expires_at',p_expires,'reason',p_reason,'source_denominator_changed',false,'cumulative_counters_preserved',true);
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details)VALUES('CTX02_CENSUS_TRANCHE_ADMITTED_V1',r);
 PERFORM population_top3000_202608_202609_v1.d02_daily_resource_guard_v2();
 RETURN r;
END;$f$;
REVOKE ALL ON FUNCTION population_top3000_202608_202609_v1.ctx02_census_admit_v1(text,bigint,bigint,text,integer,integer,timestamptz,text) FROM PUBLIC,anon,authenticated;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.ctx02_census_context_v1(p_task uuid)
RETURNS jsonb LANGUAGE plpgsql SET search_path TO pg_catalog AS $f$
DECLARE t population_top3000_202608_202609_v1.pipeline_task%rowtype;
 r population_top3000_202608_202609_v1.ctx02_census_request_v1%rowtype;
 prior population_top3000_202608_202609_v1.ctx02_census_page_v1%rowtype;
 admission population_top3000_202608_202609_v1.ctx02_census_admission_v1%rowtype;
 ctl population_top3000_202608_202609_v1.acquisition_control%rowtype;
 asof date;first_url text;url text;k text;n integer;lim integer;page_only integer;used_calls integer;
BEGIN
 SELECT * INTO STRICT t FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=p_task;
 IF t.family_id IS DISTINCT FROM 'CTX02' OR t.payload->>'kind' IS DISTINCT FROM 'CTX02_CENSUS_SOURCE_V1'
 OR t.input_versions->>'scope_sha256' IS DISTINCT FROM '8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd'
 OR t.input_versions->>'CTX02_contract_sha256' IS DISTINCT FROM '474b7aced2e7e97a4b0af5e81f264f93008967669dc4968017b4b48cc1a4b3af'
 OR t.input_versions->>'ctx02_census_bundle_sha256' IS DISTINCT FROM population_top3000_202608_202609_v1.ctx02_census_bundle_v1()
 OR t.input_versions->>'parser_bundle_sha256' IS DISTINCT FROM population_top3000_202608_202609_v1.ctx02_parser_bundle_v1()
 OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.catalog_family_contract f WHERE f.scope_id='corrected_company_scope_8e6ca1178f03822c' AND f.family_id='CTX02' AND f.contract_sha256=t.input_versions->>'CTX02_contract_sha256' AND f.contract_sha256=encode(sha256(convert_to(f.contract_text,'UTF8')),'hex'))
 THEN RAISE EXCEPTION 'CTX02_CENSUS_SCOPE_OR_CODE_NOT_ADMITTED';END IF;
 SELECT * INTO STRICT r FROM population_top3000_202608_202609_v1.ctx02_census_request_v1 WHERE request_key=t.payload->>'request_key';
 IF r.input_versions IS DISTINCT FROM t.input_versions OR r.manifest_sha256 IS DISTINCT FROM t.payload->>'request_manifest_sha256'
 OR r.manifest_sha256 IS DISTINCT FROM encode(sha256(convert_to(r.manifest::text,'UTF8')),'hex')
 OR r.request_key IS DISTINCT FROM 'ctx02-census-v1:'||r.manifest_sha256 THEN RAISE EXCEPTION 'CTX02_CENSUS_MANIFEST_CHANGED';END IF;
 IF r.manifest->>'as_of' IS NULL OR r.manifest->>'as_of'!~'^2026-(08|09)-[0-9]{2}$'
 OR r.manifest->'expired' IS DISTINCT FROM 'false'::jsonb OR r.manifest->>'population' IS DISTINCT FROM 'ALL_PROVIDER_HISTORICAL_REFERENCE_NO_COHORT_FILTER'
 OR coalesce(r.manifest->>'max_pages','')!~'^[0-9]{1,6}$' OR (r.manifest->>'max_pages')::integer NOT BETWEEN 1 AND 100000
 THEN RAISE EXCEPTION 'CTX02_CENSUS_EXACT_QUERY_REQUIRED';END IF;
 asof:=(r.manifest->>'as_of')::date;
 IF asof<DATE '2026-08-01' OR asof>DATE '2026-09-30' THEN RAISE EXCEPTION 'CTX02_CENSUS_ASOF_OUT_OF_SCOPE';END IF;
 first_url:='https://api.massive.com/v3/reference/options/contracts?as_of='||asof::text||'&expired=false&limit=1000&sort=ticker&order=asc';
 IF r.manifest->>'first_url' IS DISTINCT FROM first_url OR r.manifest->>'max_response_bytes' IS DISTINCT FROM '1048576'
 OR r.manifest->>'max_runtime_ms' IS DISTINCT FROM '45000' THEN RAISE EXCEPTION 'CTX02_CENSUS_QUERY_BOUNDS_CHANGED';END IF;
 k:='ctx02-census-archive-v1:'||encode(sha256(convert_to(r.request_key||E'\n'||r.input_versions::text,'UTF8')),'hex');
 IF t.payload ? 'source_page_number' THEN
  IF coalesce(t.payload->>'source_page_number','')!~'^[0-9]{1,6}$' OR t.stage='ACQUIRE' THEN RAISE EXCEPTION 'CTX02_CENSUS_PAGE_TASK_INVALID';END IF;
  page_only:=(t.payload->>'source_page_number')::integer;
  IF page_only NOT BETWEEN 1 AND (r.manifest->>'max_pages')::integer OR t.partition_key IS DISTINCT FROM r.request_key||':page:'||page_only::text THEN RAISE EXCEPTION 'CTX02_CENSUS_PAGE_TASK_INVALID';END IF;
 ELSE
  IF t.partition_key IS DISTINCT FROM r.request_key THEN RAISE EXCEPTION 'CTX02_CENSUS_STREAM_TASK_INVALID';END IF;
 END IF;
 IF t.stage<>'ACQUIRE' THEN RETURN r.manifest||jsonb_build_object('state','READY','request_key',r.request_key,'request_manifest_sha256',r.manifest_sha256,'archive_key',k,'family_id',t.family_id,'partition_key',t.partition_key,'source_page_number',page_only);END IF;
 SELECT * INTO STRICT admission FROM population_top3000_202608_202609_v1.ctx02_census_admission_v1 WHERE request_key=r.request_key;
 SELECT * INTO STRICT ctl FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton;
 IF admission.owner_id IS DISTINCT FROM ctl.owner_id OR admission.owner_fence IS DISTINCT FROM ctl.owner_fence OR admission.stop_generation IS DISTINCT FROM ctl.stop_generation THEN RAISE EXCEPTION 'CTX02_CENSUS_TRANCHE_OWNER_CHANGED';END IF;
 SELECT count(*) INTO used_calls FROM population_top3000_202608_202609_v1.pipeline_provider_lease l JOIN population_top3000_202608_202609_v1.pipeline_attempt a ON a.attempt_id=l.attempt_id JOIN population_top3000_202608_202609_v1.pipeline_task pt ON pt.task_id=a.task_id WHERE pt.family_id='CTX02' AND pt.payload->>'request_key'=r.request_key AND pt.stage='ACQUIRE' AND l.provider='Massive' AND l.attempt_id IS DISTINCT FROM t.attempt_id;
 IF coalesce(t.checkpoint->>'next_page','1')!~'^[0-9]{1,6}$' THEN RAISE EXCEPTION 'CTX02_CENSUS_CHECKPOINT_INVALID';END IF;
 n:=coalesce(t.checkpoint->>'next_page','1')::integer;lim:=(r.manifest->>'max_pages')::integer;
 IF n NOT BETWEEN 1 AND 100001 THEN RAISE EXCEPTION 'CTX02_CENSUS_PAGE_BOUND';END IF;
 IF n=1 THEN url:=first_url;ELSE
  SELECT * INTO prior FROM population_top3000_202608_202609_v1.ctx02_census_page_v1 WHERE archive_key=k AND page_number=n-1;
  IF NOT FOUND OR prior.next_url IS NULL OR t.checkpoint->>'last_source_id' IS DISTINCT FROM prior.source_id
  OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.source_document d WHERE d.source_id=prior.source_id AND d.source_sha256=prior.source_sha256 AND d.source_sha256=t.checkpoint->>'last_source_sha256' AND d.source_sha256=encode(sha256(convert_to(d.content_text,'UTF8')),'hex') AND d.content_bytes=octet_length(d.content_text))
  THEN RAISE EXCEPTION 'CTX02_CENSUS_CONTINUATION_PROOF_MISSING';END IF;
  url:=prior.next_url;
  IF url!~'^https://api[.]massive[.]com/v3/reference/options/contracts[?]cursor=[A-Za-z0-9_.~%+\-]+$' OR length(url)>16384 THEN RAISE EXCEPTION 'CTX02_CENSUS_CURSOR_SCOPE_INVALID';END IF;
 END IF;
 RETURN r.manifest||jsonb_build_object('state',CASE WHEN n>lim OR n>admission.admitted_page_ceiling OR used_calls>=admission.max_provider_calls OR admission.expires_at<=clock_timestamp() THEN 'PAGE_LIMIT_REVIEW' ELSE 'READY' END,'request_key',r.request_key,'request_manifest_sha256',r.manifest_sha256,'archive_key',k,'page_number',n,'family_id',t.family_id,'partition_key',t.partition_key,'request_url',url,'provider_calls_used',used_calls,'max_provider_calls',admission.max_provider_calls,'admitted_page_ceiling',admission.admitted_page_ceiling);
END; $f$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.ctx02_census_claim_v1(p_attempt uuid, p_token text)
 RETURNS jsonb
 LANGUAGE plpgsql
 SET search_path TO 'pg_catalog'
AS $function$
DECLARE tid uuid;c jsonb;r jsonb;
BEGIN
 tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=tid AND stage='ACQUIRE') THEN RAISE EXCEPTION 'CTX02_CENSUS_STAGE_MISMATCH';END IF;
 c:=population_top3000_202608_202609_v1.ctx02_census_context_v1(tid);
 IF c->>'state'<>'READY' THEN
  RETURN population_top3000_202608_202609_v1.pipeline_finish_v1(p_attempt,p_token,jsonb_build_object('state','BLOCKED','blocker_code','CTX02_CENSUS_PAGE_CAP_REVIEW','resumption_condition',jsonb_build_object('event','RETAINED_PAGE_REVIEW_AND_BOUNDED_CAP_ADMISSION','next_page',c->'page_number','existing_pages_preserved',true)));
 END IF;
 -- Every valid attempt may reserve at most one request, before credentials are used.
 r:=population_top3000_202608_202609_v1.pipeline_provider_reserve_v1(p_attempt,p_token,'Massive',(c->>'max_response_bytes')::bigint,0);
 IF r->>'state'<>'RESERVED' THEN
  RETURN population_top3000_202608_202609_v1.pipeline_finish_v1(p_attempt,p_token,jsonb_build_object('state','BLOCKED',
   'blocker_code',CASE WHEN r->>'state'='WAIT' THEN 'DEPENDENCY_PENDING' ELSE 'PROVIDER_BUDGET_OR_RESOURCE_HOLD' END,
   'recheck_at',CASE WHEN r->>'state'='WAIT' THEN r->>'not_before' ELSE NULL END,
   'resumption_condition',jsonb_build_object('event',CASE WHEN r->>'state'='WAIT' THEN 'PROVIDER_SLOT_AVAILABLE' ELSE 'CONTROLLER_ADMISSION_RECONCILED' END,'receipt',r)));
 END IF;
 RETURN c||jsonb_build_object('state','RESERVED');
END; $function$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.ctx02_census_commit_v1(p_attempt uuid, p_token text, p_page jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 SET search_path TO 'pg_catalog'
AS $function$
DECLARE tid uuid;t population_top3000_202608_202609_v1.pipeline_task%rowtype;c jsonb;body jsonb;txt text;h text;wire text;
 sid text;k text;n integer;cnt integer;bytes bigint;stored_bytes bigint;v_next_url text;at_time timestamptz;fresh integer;receipt jsonb;source_evidence jsonb;
BEGIN
 tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);
 SELECT * INTO t FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=tid;
 IF t.stage<>'ACQUIRE' THEN RAISE EXCEPTION 'CTX02_CENSUS_STAGE_MISMATCH';END IF;
 c:=population_top3000_202608_202609_v1.ctx02_census_context_v1(tid);
 PERFORM population_top3000_202608_202609_v1.d02_daily_resource_guard_v2();
 IF c->>'state'<>'READY' OR jsonb_typeof(p_page) IS DISTINCT FROM 'object' OR p_page->>'archive_key' IS DISTINCT FROM c->>'archive_key'
 OR p_page->>'page_number' IS DISTINCT FROM c->>'page_number' OR p_page->>'request_url' IS DISTINCT FROM c->>'request_url'
 OR p_page->>'request_manifest_sha256' IS DISTINCT FROM c->>'request_manifest_sha256' OR p_page->>'http_status' IS DISTINCT FROM '200' THEN RAISE EXCEPTION 'CTX02_CENSUS_RECEIPT_SCOPE_MISMATCH';END IF;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_provider_lease WHERE attempt_id=p_attempt AND provider='Massive' AND released_at IS NULL AND reserved_bytes=(c->>'max_response_bytes')::bigint) THEN RAISE EXCEPTION 'CTX02_CENSUS_RESERVATION_REQUIRED';END IF;
 txt:=p_page->>'content_text';stored_bytes:=octet_length(txt);bytes:=(p_page->>'wire_bytes')::bigint;wire:=p_page->>'wire_sha256';
 IF txt IS NULL OR stored_bytes>(c->>'max_response_bytes')::bigint OR bytes IS NULL OR bytes<1 OR bytes>(c->>'max_response_bytes')::bigint OR wire IS NULL OR wire!~'^[0-9a-f]{64}$'
 OR coalesce((p_page->>'elapsed_ms')::bigint,-1) NOT BETWEEN 0 AND 45000 THEN RAISE EXCEPTION 'CTX02_CENSUS_SOURCE_BOUND_INVALID';END IF;
 h:=encode(sha256(convert_to(txt,'UTF8')),'hex');
 IF h IS DISTINCT FROM p_page->>'source_sha256' THEN RAISE EXCEPTION 'CTX02_CENSUS_STORED_HASH_MISMATCH';END IF;
 body:=txt::jsonb;
 IF body->>'status' IS DISTINCT FROM 'OK' OR jsonb_typeof(body->'results') IS DISTINCT FROM 'array' THEN RAISE EXCEPTION 'CTX02_CENSUS_INVALID_SOURCE_ENVELOPE';END IF;
 cnt:=jsonb_array_length(body->'results');
 IF cnt>1000 OR cnt IS DISTINCT FROM (p_page->>'result_count')::integer OR EXISTS(SELECT 1 FROM jsonb_array_elements(body->'results') v WHERE jsonb_typeof(v) IS DISTINCT FROM 'object') THEN RAISE EXCEPTION 'CTX02_CENSUS_SOURCE_RECORD_SHAPE';END IF;
 v_next_url:=p_page->>'next_url';
 IF v_next_url IS DISTINCT FROM body->>'next_url' OR (v_next_url IS NOT NULL AND (v_next_url!~'^https://api[.]massive[.]com/v3/reference/options/contracts[?]cursor=[A-Za-z0-9_.~%+\-]+$' OR length(v_next_url)>16384 OR v_next_url=c->>'request_url')) THEN RAISE EXCEPTION 'CTX02_CENSUS_CURSOR_MISMATCH';END IF;
 IF txt~* '"(apiKey|api_key|access_token|authorization)"[[:space:]]*:' OR txt~* '[?&](apiKey|api_key|access_token)=' THEN RAISE EXCEPTION 'CTX02_CENSUS_UNREDACTED_CREDENTIAL_FIELD';END IF;
 k:=c->>'archive_key';n:=(c->>'page_number')::integer;sid:=k||':page:'||n::text;at_time:=(p_page->>'retrieved_at')::timestamptz;
 IF at_time IS NULL OR at_time<(SELECT started_at FROM population_top3000_202608_202609_v1.pipeline_attempt WHERE attempt_id=p_attempt)-interval '30 seconds' OR at_time>clock_timestamp()+interval '30 seconds' THEN RAISE EXCEPTION 'CTX02_CENSUS_RETRIEVAL_CLOCK_INVALID';END IF;
 IF v_next_url IS NOT NULL AND EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.ctx02_census_page_v1 WHERE archive_key=k AND request_url=v_next_url) THEN RAISE EXCEPTION 'CTX02_CENSUS_CURSOR_CYCLE';END IF;
 source_evidence:=jsonb_build_object('wire_sha256',wire,'wire_bytes',bytes,'stored_content_bytes',stored_bytes,'hash_representation','EXACT_UTF8_RESPONSE_WITH_TOP_LEVEL_NEXT_URL_CANONICALIZATION_AND_CREDENTIAL_REDACTION','wire_bytes_retained',false,'provider_request_id',p_page->>'provider_request_id','scope','CTX02 full historical reference census page without cohort/strike/expiry filters','request_manifest_sha256',c->>'request_manifest_sha256','as_of',c->>'as_of','expired',false,'source_completeness_certified',false,'no_trade_inferred',false,'original_historical_receipt',false);
 INSERT INTO population_top3000_202608_202609_v1.source_document
 (source_id,source_sha256,source_uri,source_provider,retrieved_at,source_published_at,historical_received_at,content_text,content_bytes,evidence,license_policy)
 VALUES(sid,h,c->>'request_url','Massive',at_time,NULL,NULL,txt,stored_bytes,
 source_evidence,
 'EXISTING_MASSIVE_SUBSCRIPTION_API_RESPONSE_INTERNAL_USE') ON CONFLICT(source_id) DO NOTHING;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.source_document d WHERE d.source_id=sid AND d.source_sha256=h AND d.source_uri=c->>'request_url' AND d.content_text=txt AND d.content_bytes=stored_bytes AND d.source_provider='Massive' AND d.retrieved_at=at_time AND d.source_published_at IS NULL AND d.historical_received_at IS NULL AND d.evidence=source_evidence AND d.license_policy='EXISTING_MASSIVE_SUBSCRIPTION_API_RESPONSE_INTERNAL_USE') THEN RAISE EXCEPTION 'CTX02_CENSUS_SOURCE_REVISION_CONFLICT';END IF;
 INSERT INTO population_top3000_202608_202609_v1.ctx02_census_page_v1
 (archive_key,family_id,partition_key,page_number,acquire_task_id,attempt_id,source_id,source_sha256,request_url,next_url,wire_sha256,wire_bytes,result_count,retrieved_at)
 VALUES(k,t.family_id,t.partition_key,n,tid,p_attempt,sid,h,c->>'request_url',v_next_url,wire,bytes,cnt,at_time)
 ON CONFLICT(archive_key,page_number) DO NOTHING;
 GET DIAGNOSTICS fresh=ROW_COUNT;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.ctx02_census_page_v1 q WHERE q.archive_key=k AND q.page_number=n AND q.source_id=sid AND q.source_sha256=h AND q.wire_sha256=wire AND q.wire_bytes=bytes AND q.request_url=c->>'request_url' AND q.next_url IS NOT DISTINCT FROM v_next_url AND q.result_count=cnt AND q.family_id=t.family_id AND q.partition_key=t.partition_key AND q.acquire_task_id=tid AND q.attempt_id=p_attempt AND q.retrieved_at=at_time) THEN RAISE EXCEPTION 'CTX02_CENSUS_PAGE_REVISION_CONFLICT';END IF;
 PERFORM population_top3000_202608_202609_v1.d02_daily_resource_guard_v2();
 PERFORM population_top3000_202608_202609_v1.pipeline_provider_release_v1(p_attempt,p_token,stored_bytes,0,0);
 PERFORM population_top3000_202608_202609_v1.pipeline_enqueue_v1('CTX02','NORMALIZE',t.partition_key||':page:'||n::text,
 t.payload||jsonb_build_object('source_page_number',n),t.input_versions,NULL);
 receipt:=jsonb_build_object('state',CASE WHEN v_next_url IS NULL THEN 'COMPLETE' ELSE 'CONTINUE' END,
 'checkpoint',jsonb_build_object('next_page',n+1,'last_source_id',sid,'last_source_sha256',h,'next_url',v_next_url,'source_records_seen',coalesce((t.checkpoint->>'source_records_seen')::bigint,0)+cnt),
 'not_before',(SELECT next_provider_at FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton),
 'commit_receipt',jsonb_build_object('commit_key','massive-page:'||sid||':'||h,'output_sha256',h,'new_committed_units',fresh,'immutable_output_ids',jsonb_build_array(sid),'input_versions',t.input_versions,'raw_source_records',cnt,'terminal_page',v_next_url IS NULL,'cohort_certified',false,'reference_lifecycle_qualified',false,'original_receipt_qualified',false));
 RETURN population_top3000_202608_202609_v1.pipeline_finish_v1(p_attempt,p_token,receipt);
END; $function$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.ctx02_census_failure_v1(p_attempt uuid, p_token text, p_failure jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 SET search_path TO 'pg_catalog'
AS $function$
DECLARE tid uuid;code text;class text;bytes bigint;retry integer;unknown_effect boolean;details jsonb;
BEGIN
 tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=tid AND family_id='CTX02' AND stage='ACQUIRE') THEN RAISE EXCEPTION 'CTX02_CENSUS_STAGE_MISMATCH';END IF;
 PERFORM population_top3000_202608_202609_v1.ctx02_census_context_v1(tid);
 code:=p_failure->>'code';class:=p_failure->>'failure_class';bytes:=(p_failure->>'received_bytes')::bigint;retry:=(p_failure->>'retry_after_seconds')::integer;
 IF jsonb_typeof(p_failure) IS DISTINCT FROM 'object' OR jsonb_typeof(p_failure->'fetch_started') IS DISTINCT FROM 'boolean' OR (p_failure->>'wire_sha256' IS NOT NULL AND p_failure->>'wire_sha256'!~'^[0-9a-f]{64}$') OR (p_failure->>'http_status' IS NOT NULL AND (jsonb_typeof(p_failure->'http_status') IS DISTINCT FROM 'number' OR p_failure->>'http_status'!~'^[1-5][0-9]{2}$')) OR code IS NULL OR code!~'^[A-Z0-9_]{1,80}$' OR class IS NULL OR class NOT IN('RATE_LIMIT','TRANSPORT_READ','AUTHENTICATION','PARSER','SOURCE_ABSENT') OR bytes IS NULL OR bytes NOT BETWEEN 0 AND 8388608 OR retry IS NULL OR retry NOT BETWEEN 0 AND 3600 THEN RAISE EXCEPTION 'CTX02_CENSUS_FAILURE_RECEIPT_INVALID';END IF;
 IF class='RATE_LIMIT' THEN retry:=greatest(60,retry);END IF;
 details:=jsonb_build_object('code',code,'http_status',p_failure->'http_status','response_bytes_observed',bytes,'wire_sha256',p_failure->>'wire_sha256','fetch_started',p_failure->'fetch_started','raw_body_retained',false,'automatic_retry',false);
 unknown_effect:=coalesce((p_failure->>'fetch_started')::boolean,false)
  AND (p_failure->>'http_status' IS NULL OR code IN('DEADLINE','FETCH_FAILED','EMPTY_BODY') OR (p_failure->>'wire_sha256' IS NULL AND p_failure->>'http_status'='200'));
 IF unknown_effect THEN
  -- Retain the reservation. The existing expiry reconciler conservatively accounts it.
  -- No new request is admitted until the controller reconciles the uncertain effect.
  RETURN population_top3000_202608_202609_v1.pipeline_fail_v1(p_attempt,p_token,'UNCERTAIN_EFFECT','CTX02_CENSUS:'||code,details,retry);
 END IF;
 -- This counter measures retained source bytes. An unretained error body adds zero.
 PERFORM population_top3000_202608_202609_v1.pipeline_provider_release_v1(p_attempt,p_token,0,0,CASE WHEN class='RATE_LIMIT' THEN retry ELSE 0 END);
 RETURN population_top3000_202608_202609_v1.pipeline_finish_v1(p_attempt,p_token,jsonb_build_object('state','BLOCKED','blocker_code','CTX02_CENSUS_'||code,
 'resumption_condition',jsonb_build_object('event','CONTROLLER_REVIEW_OF_DEFINITIVE_SOURCE_FAILURE','details',details,'retry_after_seconds',retry,'preserve_counters_and_cursor',true)));
END; $function$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.ctx02_census_parse_record_v1(r jsonb,c jsonb)
RETURNS jsonb LANGUAGE sql STABLE SET search_path=pg_catalog AS $f$
 SELECT normalized||jsonb_build_object('availability_policy','CURRENT_PROVIDER_ARCHIVE','reference_population','FULL_PROVIDER_CENSUS_SOURCE_COHORT_JOIN_PENDING')
 FROM population_top3000_202608_202609_v1.ctx02_parse_rows_v1('CONTRACT_REFERENCE',jsonb_build_object('as_of',c->>'as_of'),jsonb_build_array(r));
$f$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.ctx02_census_process_v1(p_attempt uuid,p_token text)
RETURNS jsonb LANGUAGE plpgsql SET search_path TO pg_catalog AS $f$
DECLARE tid uuid;t population_top3000_202608_202609_v1.pipeline_task%rowtype;c jsonb;
 q population_top3000_202608_202609_v1.ctx02_census_page_v1%rowtype;
 d population_top3000_202608_202609_v1.source_document%rowtype;
 total_pages integer;page_only integer;terminal boolean;page_no integer;off integer;stop_at integer;cnt integer;bad integer;ins integer:=0;
 body jsonb;out_sha text;state text;next_page integer;next_off integer;receipt jsonb;
BEGIN
 tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);
 SELECT * INTO STRICT t FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=tid;
 c:=population_top3000_202608_202609_v1.ctx02_census_context_v1(tid);
 IF t.stage NOT IN('NORMALIZE','DERIVE','VALIDATE') THEN RAISE EXCEPTION 'CTX02_CENSUS_PROCESS_STAGE_INVALID';END IF;
 PERFORM population_top3000_202608_202609_v1.d02_daily_resource_guard_v2();

 SELECT count(*),bool_or(next_url IS NULL) INTO total_pages,terminal FROM population_top3000_202608_202609_v1.ctx02_census_page_v1 WHERE archive_key=c->>'archive_key';
 IF total_pages<1 OR total_pages>(c->>'max_pages')::integer THEN RAISE EXCEPTION 'CTX02_CENSUS_ACQUISITION_EMPTY_OR_OUT_OF_BOUNDS';END IF;
 page_only:=(c->>'source_page_number')::integer;
 IF page_only IS NULL THEN
  SELECT count(*) INTO bad FROM (
   SELECT p.*,row_number() OVER(ORDER BY page_number) expected_page,lag(next_url) OVER(ORDER BY page_number) prior_next
   FROM population_top3000_202608_202609_v1.ctx02_census_page_v1 p WHERE archive_key=c->>'archive_key') x
  WHERE x.page_number<>x.expected_page OR x.family_id<>'CTX02' OR x.partition_key<>c->>'request_key'
  OR (page_number=1 AND request_url IS DISTINCT FROM c->>'first_url')
  OR (page_number>1 AND request_url IS DISTINCT FROM prior_next)
  OR (page_number<total_pages AND next_url IS NULL) OR (page_number=total_pages AND next_url IS NOT NULL);
  IF bad<>0 OR terminal IS DISTINCT FROM true THEN RAISE EXCEPTION 'CTX02_CENSUS_PAGE_CHAIN_NOT_TERMINAL_OR_CONTIGUOUS';END IF;
 ELSE
  IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.ctx02_census_page_v1 q WHERE q.archive_key=c->>'archive_key' AND q.page_number=page_only AND q.partition_key=c->>'request_key') THEN RAISE EXCEPTION 'CTX02_CENSUS_PAGE_TASK_SOURCE_MISSING';END IF;
 END IF;
 IF t.stage='VALIDATE' THEN RETURN jsonb_build_object('state','BLOCKED','blocker_code','CTX02_CENSUS_INDEPENDENT_POPULATION_LIFECYCLE_ACCEPTANCE_REQUIRED',
 'resumption_condition',jsonb_build_object('archive_key',c->>'archive_key','request_key',c->>'request_key','retained_pages',total_pages,'source_page_number',page_only,'terminal_cursor_seen',terminal,'required_action','Independently verify pinned rows/page chain; resolve exact company/instrument membership, all historical contract listing/deliverable changes and daily traded/no-trade evidence. Controller alone accepts and promotes.','no_trade_inferred',false,'original_receipt_qualified',false,'automatic_retry',false));END IF;
 IF coalesce(t.checkpoint->>'work_page','1')!~'^[0-9]{1,6}$' OR coalesce(t.checkpoint->>'work_offset','0')!~'^[0-9]{1,4}$' THEN RAISE EXCEPTION 'CTX02_CENSUS_PROCESS_CHECKPOINT_INVALID';END IF;
 page_no:=coalesce(page_only,coalesce(t.checkpoint->>'work_page','1')::integer);off:=coalesce(t.checkpoint->>'work_offset','0')::integer;
 SELECT * INTO STRICT q FROM population_top3000_202608_202609_v1.ctx02_census_page_v1 WHERE archive_key=c->>'archive_key' AND page_number=page_no;
 SELECT * INTO STRICT d FROM population_top3000_202608_202609_v1.source_document WHERE source_id=q.source_id;
 IF d.source_sha256 IS DISTINCT FROM q.source_sha256 OR d.source_sha256 IS DISTINCT FROM encode(sha256(convert_to(d.content_text,'UTF8')),'hex')
 OR d.content_bytes IS DISTINCT FROM octet_length(d.content_text) OR d.content_bytes>(c->>'max_response_bytes')::bigint
 OR d.source_uri IS DISTINCT FROM q.request_url OR d.source_provider IS DISTINCT FROM 'Massive' OR d.retrieved_at IS DISTINCT FROM q.retrieved_at
 OR d.source_published_at IS NOT NULL OR d.historical_received_at IS NOT NULL
 OR d.evidence->>'request_manifest_sha256' IS DISTINCT FROM c->>'request_manifest_sha256'
 THEN RAISE EXCEPTION 'CTX02_CENSUS_IMMUTABLE_PAGE_SOURCE_CHANGED';END IF;
 body:=d.content_text::jsonb;
 IF body->>'status' IS DISTINCT FROM 'OK' OR jsonb_typeof(body->'results') IS DISTINCT FROM 'array'
 OR body->>'next_url' IS DISTINCT FROM q.next_url OR jsonb_array_length(body->'results') IS DISTINCT FROM q.result_count THEN RAISE EXCEPTION 'CTX02_CENSUS_PAGE_ENVELOPE_CHANGED';END IF;
 cnt:=q.result_count;
 IF off<0 OR off>cnt OR (cnt>0 AND off=cnt) THEN RAISE EXCEPTION 'CTX02_CENSUS_PROCESS_OFFSET_INVALID';END IF;
 stop_at:=least(cnt,off+1000);
 IF t.stage='NORMALIZE' THEN
  INSERT INTO population_top3000_202608_202609_v1.ctx02_census_record_v1(source_id,source_ordinal,payload,payload_sha256)
  SELECT d.source_id,x.ordinality,p,encode(sha256(convert_to(p::text,'UTF8')),'hex')
  FROM jsonb_array_elements(body->'results') WITH ORDINALITY x(value,ordinality)
  CROSS JOIN LATERAL(SELECT population_top3000_202608_202609_v1.ctx02_census_parse_record_v1(x.value,c) p) parsed
  WHERE x.ordinality>off AND x.ordinality<=stop_at ON CONFLICT(source_id,source_ordinal) DO NOTHING;
  GET DIAGNOSTICS ins=ROW_COUNT;
 END IF;
 SELECT count(*) INTO bad FROM jsonb_array_elements(body->'results') WITH ORDINALITY x(value,ordinality)
 LEFT JOIN population_top3000_202608_202609_v1.ctx02_census_record_v1 r ON r.source_id=d.source_id AND r.source_ordinal=x.ordinality
 WHERE x.ordinality>off AND x.ordinality<=stop_at AND (r.payload IS DISTINCT FROM population_top3000_202608_202609_v1.ctx02_census_parse_record_v1(x.value,c)
 OR r.payload_sha256 IS DISTINCT FROM encode(sha256(convert_to(r.payload::text,'UTF8')),'hex'));
 IF bad<>0 THEN RAISE EXCEPTION 'CTX02_CENSUS_RECORD_REPLAY_OR_DERIVE_MISMATCH';END IF;
 IF stop_at=cnt THEN
  SELECT count(*) INTO bad FROM population_top3000_202608_202609_v1.ctx02_census_record_v1 WHERE source_id=d.source_id;
  IF bad<>cnt THEN RAISE EXCEPTION 'CTX02_CENSUS_SOURCE_RECORD_DENOMINATOR_MISMATCH';END IF;
  IF t.stage='NORMALIZE' THEN UPDATE population_top3000_202608_202609_v1.ctx02_census_page_v1 SET normalized_at=coalesce(normalized_at,clock_timestamp()),normalized_source_records=cnt WHERE archive_key=q.archive_key AND page_number=q.page_number;END IF;
  next_page:=page_no+1;next_off:=0;
 ELSE next_page:=page_no;next_off:=stop_at;END IF;
 SELECT encode(sha256(convert_to(d.source_sha256||E'\n'||coalesce(string_agg(payload_sha256,E'\n' ORDER BY source_ordinal),''),'UTF8')),'hex') INTO out_sha
 FROM population_top3000_202608_202609_v1.ctx02_census_record_v1 WHERE source_id=d.source_id AND source_ordinal>off AND source_ordinal<=stop_at;
 state:=CASE WHEN (page_only IS NOT NULL AND stop_at=cnt) OR next_page>total_pages THEN 'COMPLETE' ELSE 'CONTINUE' END;
 PERFORM population_top3000_202608_202609_v1.d02_daily_resource_guard_v2();
 receipt:=jsonb_build_object('commit_key','ctx02-census-v1:'||t.stage||':'||d.source_id||':'||off||':'||stop_at,'output_sha256',out_sha,'new_committed_units',ins,
 'immutable_output_ids',jsonb_build_array(d.source_id),'input_versions',t.input_versions,'source_records_in_chunk',stop_at-off,'source_page_records',cnt,'retained_acquisition_pages',total_pages,'source_page_chain_terminal',terminal,'source_page_number',page_only,'required_scope_accepted',false,'original_receipt_qualified',false,'no_trade_inferred',false);
 RETURN jsonb_build_object('state',state,'checkpoint',jsonb_build_object('work_page',next_page,'work_offset',next_off),'commit_receipt',receipt);
END; $f$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.ctx02_census_acquire_v1(p_attempt uuid,p_token text)
RETURNS jsonb LANGUAGE plpgsql SET search_path TO pg_catalog AS $f$
DECLARE tid uuid;k text;
BEGIN
 tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);
 SELECT payload->>'kind' INTO k FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=tid;
 IF k='CTX02_CENSUS_SOURCE_V1' THEN PERFORM population_top3000_202608_202609_v1.ctx02_census_context_v1(tid);RETURN population_top3000_202608_202609_v1.pipeline_dispatch_edge_v1(p_attempt,p_token,'population-top3000-options-census-v1');END IF;
 RAISE EXCEPTION 'CTX02_CENSUS_ACQUIRE_KIND_NOT_ADMITTED';
END; $f$;
CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.ctx02_census_normalize_v1(p_attempt uuid,p_token text)
RETURNS jsonb LANGUAGE plpgsql SET search_path TO pg_catalog AS $f$
DECLARE tid uuid;k text;BEGIN tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);SELECT payload->>'kind' INTO k FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=tid;
 IF k='CTX02_CENSUS_SOURCE_V1' THEN RETURN population_top3000_202608_202609_v1.ctx02_census_process_v1(p_attempt,p_token);END IF;
 RETURN population_top3000_202608_202609_v1.ctx02_normalize_sample_v1(p_attempt,p_token);END; $f$;
CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.ctx02_census_derive_v1(p_attempt uuid,p_token text)
RETURNS jsonb LANGUAGE plpgsql SET search_path TO pg_catalog AS $f$
DECLARE tid uuid;k text;BEGIN tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);SELECT payload->>'kind' INTO k FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=tid;
 IF k='CTX02_CENSUS_SOURCE_V1' THEN RETURN population_top3000_202608_202609_v1.ctx02_census_process_v1(p_attempt,p_token);END IF;
 RETURN population_top3000_202608_202609_v1.ctx02_derive_sample_v1(p_attempt,p_token);END; $f$;
CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.ctx02_census_validate_v1(p_attempt uuid,p_token text)
RETURNS jsonb LANGUAGE plpgsql SET search_path TO pg_catalog AS $f$
DECLARE tid uuid;k text;BEGIN tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);SELECT payload->>'kind' INTO k FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=tid;
 IF k='CTX02_CENSUS_SOURCE_V1' THEN RETURN population_top3000_202608_202609_v1.ctx02_census_process_v1(p_attempt,p_token);END IF;
 RETURN population_top3000_202608_202609_v1.ctx02_sample_quality_hold_v1(p_attempt,p_token);END; $f$;
CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.ctx02_census_bundle_v1()
RETURNS text LANGUAGE sql STABLE SET search_path TO pg_catalog AS $f$
 SELECT encode(sha256(convert_to(string_agg(pg_get_functiondef(p.oid),E'\n' ORDER BY p.proname),'UTF8')),'hex') FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
 WHERE n.nspname='population_top3000_202608_202609_v1' AND p.proname IN('ctx02_census_context_v1','ctx02_census_claim_v1','ctx02_census_commit_v1','ctx02_census_failure_v1','ctx02_census_parse_record_v1','ctx02_census_process_v1','ctx02_census_acquire_v1','ctx02_census_normalize_v1','ctx02_census_derive_v1','ctx02_census_validate_v1');
$f$;
REVOKE ALL ON FUNCTION population_top3000_202608_202609_v1.ctx02_census_context_v1(uuid),population_top3000_202608_202609_v1.ctx02_census_claim_v1(uuid,text),population_top3000_202608_202609_v1.ctx02_census_commit_v1(uuid,text,jsonb),population_top3000_202608_202609_v1.ctx02_census_failure_v1(uuid,text,jsonb),population_top3000_202608_202609_v1.ctx02_census_parse_record_v1(jsonb,jsonb),population_top3000_202608_202609_v1.ctx02_census_process_v1(uuid,text),population_top3000_202608_202609_v1.ctx02_census_acquire_v1(uuid,text),population_top3000_202608_202609_v1.ctx02_census_normalize_v1(uuid,text),population_top3000_202608_202609_v1.ctx02_census_derive_v1(uuid,text),population_top3000_202608_202609_v1.ctx02_census_validate_v1(uuid,text),population_top3000_202608_202609_v1.ctx02_census_bundle_v1() FROM PUBLIC,anon,authenticated;
