-- LOCAL ONLY definitions. Root reviews/tests/admits/deploys. No existing source or adapter is replaced here.
-- One shared SIP/raw acquisition supports P01 and P02. Source symbols are not certified company identities.
CREATE TABLE IF NOT EXISTS population_top3000_202608_202609_v1.alpaca_sip_source_page_v1 (
 page_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
 archive_key text NOT NULL,page_number integer NOT NULL CHECK(page_number BETWEEN 1 AND 64),
 source_id text NOT NULL UNIQUE REFERENCES population_top3000_202608_202609_v1.source_document(source_id),
 source_sha256 text NOT NULL CHECK(source_sha256~'^[0-9a-f]{64}$'),request_url text NOT NULL,
 request_token text,next_token text,result_count integer NOT NULL CHECK(result_count BETWEEN 0 AND 10000),
 wire_bytes bigint NOT NULL CHECK(wire_bytes BETWEEN 1 AND 8388608),retrieved_at timestamptz NOT NULL,
 request_spec jsonb NOT NULL,input_versions jsonb NOT NULL,
 acquire_task_id uuid NOT NULL REFERENCES population_top3000_202608_202609_v1.pipeline_task(task_id),
 attempt_id uuid NOT NULL REFERENCES population_top3000_202608_202609_v1.pipeline_attempt(attempt_id),
 normalized_at timestamptz,normalized_rows integer,normalized_sha256 text,
 UNIQUE(archive_key,page_number),UNIQUE(archive_key,request_url),
 CHECK(request_url LIKE 'https://data.alpaca.markets/v2/stocks/bars?%'),
 CHECK(request_url !~* '(apikey|api_key|secret|authorization)=')
);
CREATE TABLE IF NOT EXISTS population_top3000_202608_202609_v1.alpaca_sip_compact_bar_v1 (
 page_id bigint NOT NULL REFERENCES population_top3000_202608_202609_v1.alpaca_sip_source_page_v1(page_id),
 source_row smallint NOT NULL CHECK(source_row BETWEEN 1 AND 10000),
 source_symbol text NOT NULL CHECK(length(source_symbol) BETWEEN 1 AND 32),symbol_row smallint NOT NULL CHECK(symbol_row BETWEEN 1 AND 10000),
 bar_ts timestamptz,session_date date,session_code smallint NOT NULL CHECK(session_code BETWEEN 0 AND 4),
 open numeric,high numeric,low numeric,close numeric,volume numeric,trade_count numeric,vwap numeric,
 quality_bits integer NOT NULL CHECK(quality_bits>=0),raw_record_sha256 bytea NOT NULL CHECK(octet_length(raw_record_sha256)=32),
 PRIMARY KEY(page_id,source_row)
);
CREATE INDEX IF NOT EXISTS alpaca_sip_compact_symbol_time_v1 ON population_top3000_202608_202609_v1.alpaca_sip_compact_bar_v1(source_symbol,bar_ts,page_id);
COMMENT ON TABLE population_top3000_202608_202609_v1.alpaca_sip_compact_bar_v1 IS 'Private source-symbol minute observations; no company/security assignment. Exact source page retained once in source_document. session_code:0 unknown,1 pre,2 regular,3 post,4 outside declared session. quality_bits:1 invalid timestamp,2 off-minute,4 outside requested time,8 missing/nonnumeric field,16 OHLC/negative volume-count,32 noninteger trade_count,64 no frozen calendar/declared session,128 source order/duplicate timestamp. No dense-grid zero filling.';
ALTER TABLE population_top3000_202608_202609_v1.alpaca_sip_source_page_v1 ENABLE ROW LEVEL SECURITY;
ALTER TABLE population_top3000_202608_202609_v1.alpaca_sip_compact_bar_v1 ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON population_top3000_202608_202609_v1.alpaca_sip_source_page_v1,population_top3000_202608_202609_v1.alpaca_sip_compact_bar_v1 FROM PUBLIC,anon,authenticated,service_role;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.alpaca_sip_urlencode_v1(v text)
RETURNS text LANGUAGE plpgsql IMMUTABLE STRICT SET search_path TO 'pg_catalog' AS $f$
DECLARE b bytea:=convert_to(v,'UTF8');i integer;n integer;r text:='';
BEGIN
 FOR i IN 0..octet_length(b)-1 LOOP
  n:=get_byte(b,i);
  r:=r||CASE WHEN n BETWEEN 65 AND 90 OR n BETWEEN 97 AND 122 OR n BETWEEN 48 AND 57 OR n IN(42,45,46,95) THEN chr(n) WHEN n=32 THEN '+' ELSE '%'||upper(lpad(to_hex(n),2,'0')) END;
 END LOOP;RETURN r;
END $f$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.alpaca_sip_spec_v1(p_payload jsonb)
RETURNS jsonb LANGUAGE plpgsql SET search_path TO 'pg_catalog' SET timezone TO 'UTC' AS $f$
DECLARE symbols text[];d1 date;d2 date;cal jsonb;refs jsonb;spec jsonb;
BEGIN
 IF p_payload->>'kind' IS DISTINCT FROM 'ALPACA_SIP_GAP_BATCH_V1' OR jsonb_typeof(p_payload->'symbols') IS DISTINCT FROM 'array'
 OR jsonb_array_length(p_payload->'symbols') NOT BETWEEN 1 AND 50
 OR coalesce(p_payload->>'start_ny_date','')!~'^2026-(08|09)-[0-9]{2}$'
 OR coalesce(p_payload->>'end_ny_date_exclusive','')!~'^2026-(08|09|10)-[0-9]{2}$'
 THEN RAISE EXCEPTION 'ALPACA_SIP_SPEC_INVALID';END IF;
 SELECT array_agg(value ORDER BY value COLLATE "C") INTO symbols FROM jsonb_array_elements_text(p_payload->'symbols');
 IF EXISTS(SELECT 1 FROM unnest(symbols) s WHERE s!~'^[A-Z0-9][A-Z0-9.-]{0,31}$') OR cardinality(symbols)<>(SELECT count(DISTINCT s) FROM unnest(symbols) s)
 OR to_jsonb(symbols) IS DISTINCT FROM p_payload->'symbols' THEN RAISE EXCEPTION 'ALPACA_SIP_SYMBOL_MANIFEST_INVALID';END IF;
 d1:=(p_payload->>'start_ny_date')::date;d2:=(p_payload->>'end_ny_date_exclusive')::date;
 IF d1<date '2026-08-01' OR d2>date '2026-10-01' OR d2<=d1 OR d2-d1>7 THEN RAISE EXCEPTION 'ALPACA_SIP_DATE_SCOPE_INVALID';END IF;
 SELECT jsonb_agg(jsonb_build_object('date',session_date,'open',regular_open_utc,'close',regular_close_utc,'record_sha256',source_record_sha256,'receipt_sha256',source_receipt_sha256) ORDER BY session_date)
 INTO cal FROM population_top3000_202608_202609_v1.catalog_calendar
 WHERE scope_id=p_payload->>'calendar_scope_id' AND session_date>=d1 AND session_date<d2;
 IF cal IS NULL OR jsonb_array_length(cal)<>(SELECT count(DISTINCT session_date) FROM population_top3000_202608_202609_v1.catalog_calendar WHERE scope_id=p_payload->>'calendar_scope_id' AND session_date>=d1 AND session_date<d2)
 OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.catalog_calendar WHERE scope_id=p_payload->>'calendar_scope_id' AND session_date>=d1 AND session_date<d2 AND
 (qualification IS DISTINCT FROM 'PROVIDER_AND_NYSE_RULES_RECONCILED' OR source_record_sha256 IS DISTINCT FROM encode(sha256(convert_to(source_record_text,'UTF8')),'hex') OR source_receipt_sha256 IS DISTINCT FROM encode(sha256(convert_to(source_receipt_text,'UTF8')),'hex')))
 THEN RAISE EXCEPTION 'ALPACA_SIP_FROZEN_CALENDAR_REQUIRED';END IF;
 SELECT jsonb_agg(jsonb_build_object('symbol',symbol,'instrument_key',instrument_key,'alpaca_asset_id',alpaca_asset_id,'classification_observed_at',classification_observed_at,'classification_method',classification_method) ORDER BY symbol COLLATE "C",instrument_key)
 INTO refs FROM reference.alpaca_fixed_window_instruments_remediated_v1 WHERE symbol=ANY(symbols);
 IF coalesce(jsonb_array_length(refs),0)<>cardinality(symbols)
 OR EXISTS(SELECT 1 FROM reference.alpaca_fixed_window_instruments_remediated_v1 WHERE symbol=ANY(symbols) GROUP BY symbol HAVING count(*)<>1)
 THEN RAISE EXCEPTION 'ALPACA_SIP_RETAINED_SYMBOL_CANDIDATES_REQUIRED';END IF;
 spec:=jsonb_build_object('provider','Alpaca','symbols',symbols,'timeframe','1Min','feed','sip','adjustment','raw','asof','-','currency','USD','sort','asc','limit',10000,
 'start_utc',to_char(d1::timestamp AT TIME ZONE 'America/New_York','YYYY-MM-DD"T"HH24:MI:SS"Z"'),
 'end_exclusive_utc',to_char(d2::timestamp AT TIME ZONE 'America/New_York','YYYY-MM-DD"T"HH24:MI:SS"Z"'),
 'end_inclusive_utc',to_char((d2::timestamp AT TIME ZONE 'America/New_York')-interval '1 microsecond','YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
 'calendar_scope_id',p_payload->>'calendar_scope_id','calendar_manifest',cal,'source_symbol_candidates',refs,
 'identity_state','SOURCE_SYMBOL_CANDIDATES_ONLY_NOT_HISTORICAL_COMPANY_MEMBERSHIP','shared_target_families',jsonb_build_array('P01','P02'));
 RETURN spec;
END $f$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.alpaca_sip_context_v1(p_task uuid)
RETURNS jsonb LANGUAGE plpgsql SET search_path TO 'pg_catalog' SET timezone TO 'UTC' AS $f$
DECLARE t population_top3000_202608_202609_v1.pipeline_task%rowtype;s jsonb;k text;specsha text;n integer;lim integer;url text;token text;prior population_top3000_202608_202609_v1.alpaca_sip_source_page_v1%rowtype;
BEGIN
 SELECT * INTO t FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=p_task;
 IF NOT FOUND OR t.family_id<>'P01' OR t.payload->>'kind' IS DISTINCT FROM 'ALPACA_SIP_GAP_BATCH_V1'
 OR t.input_versions->>'scope_sha256' IS DISTINCT FROM '8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd'
 OR t.input_versions->>'archive_contract_revision' IS DISTINCT FROM 'alpaca-sip-gap-20261023-v1'
 OR t.input_versions->>'alpaca_bundle_sha256' IS DISTINCT FROM population_top3000_202608_202609_v1.alpaca_sip_code_hash_v1()
 THEN RAISE EXCEPTION 'ALPACA_SIP_SCOPE_OR_HELPER_VERSION';END IF;
 s:=population_top3000_202608_202609_v1.alpaca_sip_spec_v1(t.payload);specsha:=encode(sha256(convert_to(s::text,'UTF8')),'hex');
 IF t.input_versions->>'request_spec_sha256' IS DISTINCT FROM specsha OR t.partition_key IS DISTINCT FROM 'alpaca-sip-gap:'||specsha THEN RAISE EXCEPTION 'ALPACA_SIP_SPEC_VERSION_CHANGED';END IF;
 k:='alpaca-sip:'||encode(sha256(convert_to(t.input_versions::text,'UTF8')),'hex');
 IF coalesce(t.checkpoint->>'next_page','1')!~'^[0-9]{1,2}$' OR coalesce(t.payload->>'max_pages','1')!~'^[0-9]{1,2}$' THEN RAISE EXCEPTION 'ALPACA_SIP_PAGE_BOUND_INVALID';END IF;
 n:=coalesce(t.checkpoint->>'next_page','1')::integer;lim:=coalesce(t.payload->>'max_pages','1')::integer;
 IF n NOT BETWEEN 1 AND 65 OR lim NOT BETWEEN 1 AND 64 THEN RAISE EXCEPTION 'ALPACA_SIP_PAGE_BOUND_INVALID';END IF;
 IF n>1 THEN
  SELECT * INTO prior FROM population_top3000_202608_202609_v1.alpaca_sip_source_page_v1 WHERE archive_key=k AND page_number=n-1;
  IF NOT FOUND OR prior.next_token IS NULL OR prior.source_id IS DISTINCT FROM t.checkpoint->>'last_source_id' OR prior.source_sha256 IS DISTINCT FROM t.checkpoint->>'last_source_sha256'
  OR prior.next_token IS DISTINCT FROM t.checkpoint->>'next_page_token'
  OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.source_document d WHERE d.source_id=prior.source_id AND d.source_sha256=prior.source_sha256 AND d.content_bytes=prior.wire_bytes AND d.source_uri=prior.request_url)
  THEN RAISE EXCEPTION 'ALPACA_SIP_CONTINUATION_EVIDENCE_MISSING';END IF;
  token:=prior.next_token;
 END IF;
 url:='https://data.alpaca.markets/v2/stocks/bars?symbols='||population_top3000_202608_202609_v1.alpaca_sip_urlencode_v1((SELECT string_agg(v,',' ORDER BY ord) FROM jsonb_array_elements_text(s->'symbols') WITH ORDINALITY e(v,ord)))||'&timeframe=1Min&start='||population_top3000_202608_202609_v1.alpaca_sip_urlencode_v1(s->>'start_utc')||'&end='||population_top3000_202608_202609_v1.alpaca_sip_urlencode_v1(s->>'end_inclusive_utc')||'&limit=10000&adjustment=raw&feed=sip&sort=asc&asof=-&currency=USD';
 IF token IS NOT NULL THEN url:=url||'&page_token='||population_top3000_202608_202609_v1.alpaca_sip_urlencode_v1(token);END IF;
 RETURN jsonb_build_object('state',CASE WHEN n>lim THEN 'PAGE_LIMIT_REVIEW' ELSE 'READY' END,'archive_key',k,'page_number',n,'max_pages',lim,'request_url',url,'request_token',token,
 'request_spec',s,'family_id','P01','partition_key',t.partition_key,'max_response_bytes',8388608,'max_runtime_ms',45000,'max_bars',10000);
END $f$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.alpaca_sip_acquire_v1(p_attempt uuid,p_token text)
RETURNS jsonb LANGUAGE plpgsql SET search_path TO 'pg_catalog' AS $f$
DECLARE tid uuid;c jsonb;
BEGIN
 tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=tid AND stage='ACQUIRE') THEN RAISE EXCEPTION 'ALPACA_SIP_STAGE_MISMATCH';END IF;
 c:=population_top3000_202608_202609_v1.alpaca_sip_context_v1(tid);
 IF c->>'state'='PAGE_LIMIT_REVIEW' THEN RETURN jsonb_build_object('state','BLOCKED','blocker_code','ALPACA_SIP_PAGE_CAP_REVIEW','resumption_condition',jsonb_build_object('required_action','Review retained page, measured bar count, source bytes and remaining request/resource budget before changing this task page cap. Preserve its cursor and input versions.','next_page',c->'page_number','automatic_retry',false));END IF;
 RETURN population_top3000_202608_202609_v1.pipeline_dispatch_edge_v1(p_attempt,p_token,'population-top3000-bars-v1');
END $f$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.alpaca_sip_claim_v1(p_attempt uuid,p_token text)
RETURNS jsonb LANGUAGE plpgsql SET search_path TO 'pg_catalog' AS $f$
DECLARE tid uuid;c jsonb;r jsonb;
BEGIN
 tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=tid AND stage='ACQUIRE') THEN RAISE EXCEPTION 'ALPACA_SIP_STAGE_MISMATCH';END IF;
 c:=population_top3000_202608_202609_v1.alpaca_sip_context_v1(tid);
 IF c->>'state'<>'READY' THEN RETURN population_top3000_202608_202609_v1.pipeline_finish_v1(p_attempt,p_token,jsonb_build_object('state','BLOCKED','blocker_code','ALPACA_SIP_PAGE_CAP_REVIEW','resumption_condition',jsonb_build_object('required_action','Controller reviews retained pages and resource bounds before a larger cap.','automatic_retry',false)));END IF;
 r:=population_top3000_202608_202609_v1.pipeline_provider_reserve_v1(p_attempt,p_token,'Alpaca',8388608,0);
 IF r->>'state'<>'RESERVED' THEN RETURN population_top3000_202608_202609_v1.pipeline_finish_v1(p_attempt,p_token,jsonb_build_object('state','BLOCKED','blocker_code','ALPACA_SIP_PROVIDER_ADMISSION_HOLD','resumption_condition',jsonb_build_object('required_action','Controller reconciles provider admission, pacing, storage and budget condition.','receipt',r,'automatic_retry',false)));END IF;
 RETURN c||jsonb_build_object('state','RESERVED');
END $f$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.alpaca_sip_commit_v1(p_attempt uuid,p_token text,p_page jsonb)
RETURNS jsonb LANGUAGE plpgsql SET search_path TO 'pg_catalog' SET timezone TO 'UTC' AS $f$
DECLARE tid uuid;t population_top3000_202608_202609_v1.pipeline_task%rowtype;c jsonb;body jsonb;txt text;h text;bytes bigint;cnt integer;k text;n integer;sid text;nexttoken text;at_time timestamptz;fresh integer;r jsonb;
BEGIN
 tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);
 SELECT * INTO t FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=tid;
 IF t.stage<>'ACQUIRE' THEN RAISE EXCEPTION 'ALPACA_SIP_STAGE_MISMATCH';END IF;
 c:=population_top3000_202608_202609_v1.alpaca_sip_context_v1(tid);
 IF c->>'state'<>'READY' OR jsonb_typeof(p_page) IS DISTINCT FROM 'object' OR p_page->>'archive_key' IS DISTINCT FROM c->>'archive_key'
 OR p_page->>'page_number' IS DISTINCT FROM c->>'page_number' OR p_page->>'request_url' IS DISTINCT FROM c->>'request_url' OR p_page->>'http_status' IS DISTINCT FROM '200'
 THEN RAISE EXCEPTION 'ALPACA_SIP_RECEIPT_SCOPE_MISMATCH';END IF;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_provider_lease WHERE attempt_id=p_attempt AND provider='Alpaca' AND released_at IS NULL AND reserved_bytes=8388608 AND started_at+interval '45 seconds'>clock_timestamp()) THEN RAISE EXCEPTION 'ALPACA_SIP_ACTIVE_RESERVATION_REQUIRED';END IF;
 txt:=p_page->>'content_text';bytes:=octet_length(txt);
 IF txt IS NULL OR bytes NOT BETWEEN 1 AND 8388608 OR (p_page->>'wire_bytes')::bigint IS DISTINCT FROM bytes OR coalesce((p_page->>'elapsed_ms')::bigint,-1) NOT BETWEEN 0 AND 45000 THEN RAISE EXCEPTION 'ALPACA_SIP_SOURCE_BOUND_INVALID';END IF;
 h:=encode(sha256(convert_to(txt,'UTF8')),'hex');
 IF h IS DISTINCT FROM p_page->>'source_sha256' OR h IS DISTINCT FROM p_page->>'wire_sha256' THEN RAISE EXCEPTION 'ALPACA_SIP_SOURCE_HASH_MISMATCH';END IF;
 body:=txt::jsonb;
 IF jsonb_typeof(body) IS DISTINCT FROM 'object' OR jsonb_typeof(body->'bars') IS DISTINCT FROM 'object' OR NOT body ? 'next_page_token'
 OR (body->'next_page_token'<>'null'::jsonb AND jsonb_typeof(body->'next_page_token')<>'string') THEN RAISE EXCEPTION 'ALPACA_SIP_SOURCE_ENVELOPE';END IF;
 IF EXISTS(SELECT 1 FROM jsonb_each(body->'bars') e WHERE NOT ((c#>'{request_spec,symbols}') ? e.key) OR jsonb_typeof(e.value)<>'array') THEN RAISE EXCEPTION 'ALPACA_SIP_UNREQUESTED_SYMBOL_OR_ARRAY';END IF;
 SELECT coalesce(sum(jsonb_array_length(value)),0)::integer INTO cnt FROM jsonb_each(body->'bars');
 IF cnt>10000 OR cnt IS DISTINCT FROM (p_page->>'result_count')::integer OR EXISTS(SELECT 1 FROM jsonb_each(body->'bars') e CROSS JOIN LATERAL jsonb_array_elements(e.value) b WHERE jsonb_typeof(b)<>'object') THEN RAISE EXCEPTION 'ALPACA_SIP_BAR_COUNT_OR_SHAPE';END IF;
 nexttoken:=body->>'next_page_token';
 IF nexttoken IS DISTINCT FROM p_page->>'next_page_token' OR (nexttoken IS NOT NULL AND (length(nexttoken) NOT BETWEEN 1 AND 8192 OR nexttoken~'[[:cntrl:][:space:]]' OR nexttoken=c->>'request_token')) THEN RAISE EXCEPTION 'ALPACA_SIP_PAGE_TOKEN_INVALID';END IF;
 k:=c->>'archive_key';n:=(c->>'page_number')::integer;sid:=k||':page:'||n;
 IF nexttoken IS NOT NULL AND EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.alpaca_sip_source_page_v1 WHERE archive_key=k AND request_token=nexttoken) THEN RAISE EXCEPTION 'ALPACA_SIP_PAGE_TOKEN_CYCLE';END IF;
 at_time:=(p_page->>'retrieved_at')::timestamptz;
 IF at_time IS NULL OR at_time<(SELECT started_at FROM population_top3000_202608_202609_v1.pipeline_attempt WHERE attempt_id=p_attempt)-interval '5 seconds' OR at_time>clock_timestamp()+interval '5 seconds' THEN RAISE EXCEPTION 'ALPACA_SIP_RETRIEVAL_CLOCK_INVALID';END IF;
 INSERT INTO population_top3000_202608_202609_v1.source_document(source_id,source_sha256,source_uri,source_provider,retrieved_at,source_published_at,historical_received_at,content_text,content_bytes,evidence,license_policy)
 VALUES(sid,h,c->>'request_url','Alpaca',at_time,NULL,NULL,txt,bytes,jsonb_build_object('wire_sha256',h,'wire_bytes',bytes,'hash_representation','EXACT_UTF8_RESPONSE','request_spec',c->'request_spec','request_id',p_page->>'provider_request_id','historical_first_receipt_proven',false,'shared_target_families',jsonb_build_array('P01','P02'),'stored_once_with_postgresql_extended_toast',true),'EXISTING_ALPACA_SIP_SUBSCRIPTION_INTERNAL_USE') ON CONFLICT(source_id) DO NOTHING;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.source_document WHERE source_id=sid AND source_sha256=h AND source_uri=c->>'request_url' AND source_provider='Alpaca' AND content_text=txt AND content_bytes=bytes AND retrieved_at=at_time AND historical_received_at IS NULL AND source_published_at IS NULL) THEN RAISE EXCEPTION 'ALPACA_SIP_SOURCE_REVISION_CONFLICT';END IF;
 INSERT INTO population_top3000_202608_202609_v1.alpaca_sip_source_page_v1(archive_key,page_number,source_id,source_sha256,request_url,request_token,next_token,result_count,wire_bytes,retrieved_at,request_spec,input_versions,acquire_task_id,attempt_id)
 VALUES(k,n,sid,h,c->>'request_url',c->>'request_token',nexttoken,cnt,bytes,at_time,c->'request_spec',t.input_versions,tid,p_attempt) ON CONFLICT(archive_key,page_number) DO NOTHING;
 GET DIAGNOSTICS fresh=ROW_COUNT;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.alpaca_sip_source_page_v1 q WHERE q.archive_key=k AND q.page_number=n AND q.source_id=sid AND q.source_sha256=h AND q.request_url=c->>'request_url' AND q.request_token IS NOT DISTINCT FROM c->>'request_token' AND q.next_token IS NOT DISTINCT FROM nexttoken AND q.result_count=cnt AND q.wire_bytes=bytes AND q.retrieved_at=at_time AND q.request_spec=c->'request_spec' AND q.input_versions=t.input_versions) THEN RAISE EXCEPTION 'ALPACA_SIP_PAGE_REVISION_CONFLICT';END IF;
 PERFORM population_top3000_202608_202609_v1.alpaca_sip_resource_guard_v1();
 PERFORM population_top3000_202608_202609_v1.pipeline_provider_release_v1(p_attempt,p_token,bytes,0,0);
 r:=jsonb_build_object('state',CASE WHEN nexttoken IS NULL THEN 'COMPLETE' ELSE 'CONTINUE' END,'checkpoint',jsonb_build_object('next_page',n+1,'last_source_id',sid,'last_source_sha256',h,'next_page_token',nexttoken,'source_bars_seen',coalesce((t.checkpoint->>'source_bars_seen')::bigint,0)+cnt),
 'not_before',(SELECT next_provider_at FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton),
 'commit_receipt',jsonb_build_object('commit_key','alpaca-source-page:'||sid||':'||h,'output_sha256',h,'new_committed_units',fresh,'immutable_output_ids',jsonb_build_array(sid),'input_versions',t.input_versions,'raw_source_bars',cnt,'terminal_page',nexttoken IS NULL,'company_membership_certified',false));
 RETURN population_top3000_202608_202609_v1.pipeline_finish_v1(p_attempt,p_token,r);
END $f$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.alpaca_sip_failure_v1(p_attempt uuid,p_token text,p_failure jsonb)
RETURNS jsonb LANGUAGE plpgsql SET search_path TO 'pg_catalog' AS $f$
DECLARE tid uuid;code text;class text;bytes bigint;retry integer;unknown_effect boolean;details jsonb;
BEGIN
 tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=tid AND family_id='P01' AND stage='ACQUIRE' AND payload->>'kind'='ALPACA_SIP_GAP_BATCH_V1') THEN RAISE EXCEPTION 'ALPACA_SIP_STAGE_MISMATCH';END IF;
 PERFORM population_top3000_202608_202609_v1.alpaca_sip_context_v1(tid);
 code:=p_failure->>'code';class:=p_failure->>'failure_class';bytes:=(p_failure->>'received_bytes')::bigint;retry:=(p_failure->>'retry_after_seconds')::integer;
 IF code IS NULL OR code!~'^[A-Z0-9_]{1,80}$' OR class IS NULL OR class NOT IN('RATE_LIMIT','TRANSPORT_READ','AUTHENTICATION','PARSER','SOURCE_ABSENT') OR bytes IS NULL OR bytes NOT BETWEEN 0 AND 8388608 OR retry IS NULL OR retry NOT BETWEEN 0 AND 3600 THEN RAISE EXCEPTION 'ALPACA_SIP_FAILURE_RECEIPT_INVALID';END IF;
 details:=jsonb_build_object('code',code,'http_status',p_failure->'http_status','response_bytes_observed',bytes,'wire_sha256',p_failure->>'wire_sha256','fetch_started',p_failure->'fetch_started','raw_body_retained',false,'automatic_retry',false);
 unknown_effect:=coalesce((p_failure->>'fetch_started')::boolean,false) AND (p_failure->>'http_status' IS NULL OR code IN('DEADLINE','FETCH_FAILED','EMPTY_BODY') OR (p_failure->>'wire_sha256' IS NULL AND p_failure->>'http_status'='200'));
 IF unknown_effect THEN RETURN population_top3000_202608_202609_v1.pipeline_fail_v1(p_attempt,p_token,'UNCERTAIN_EFFECT','ALPACA_SIP:'||code,details,retry);END IF;
 PERFORM population_top3000_202608_202609_v1.pipeline_provider_release_v1(p_attempt,p_token,0,0,CASE WHEN class='RATE_LIMIT' THEN retry ELSE 0 END);
 RETURN population_top3000_202608_202609_v1.pipeline_finish_v1(p_attempt,p_token,jsonb_build_object('state','BLOCKED','blocker_code','ALPACA_SIP_'||code,'resumption_condition',jsonb_build_object('required_action','Controller reviews definitive provider/configuration failure before an explicit retry.','details',details,'automatic_retry',false)));
END $f$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.alpaca_sip_timestamp_v1(v text)
RETURNS timestamptz LANGUAGE plpgsql IMMUTABLE SET search_path TO 'pg_catalog' SET timezone TO 'UTC' AS $f$
DECLARE parsed timestamptz;truncated text;
BEGIN
 IF v IS NULL OR v!~'^[0-9]{4}-[0-9]{2}-[0-9]{2}[Tt]([01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9]([.][0-9]{1,9})?([Zz]|[+-]([01][0-9]|2[0-3]):[0-5][0-9])$' THEN RETURN NULL;END IF;
 truncated:=regexp_replace(v,'([.][0-9]{6})[0-9]+','\1');
 BEGIN parsed:=truncated::timestamptz;EXCEPTION WHEN datetime_field_overflow OR invalid_datetime_format THEN RETURN NULL;END;
 RETURN parsed;
END $f$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.alpaca_sip_number_v1(v jsonb)
RETURNS numeric LANGUAGE sql IMMUTABLE SET search_path TO 'pg_catalog' AS $f$
 SELECT CASE WHEN jsonb_typeof(v)='number' AND length(v::text)<=96 THEN (v#>>'{}')::numeric ELSE NULL END;
$f$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.alpaca_sip_parse_page_v1(p_text text,p_spec jsonb)
RETURNS TABLE(source_row smallint,source_symbol text,symbol_row smallint,bar_ts timestamptz,session_date date,session_code smallint,
 open numeric,high numeric,low numeric,close numeric,volume numeric,trade_count numeric,vwap numeric,quality_bits integer,raw_record_sha256 bytea)
LANGUAGE sql STABLE SET search_path TO 'pg_catalog' SET timezone TO 'UTC' AS $f$
 WITH raw AS MATERIALIZED (
 SELECT row_number() OVER(ORDER BY e.key COLLATE "C",b.ord)::smallint rownum,e.key symbol,b.ord::smallint ordinal,b.rec,
 population_top3000_202608_202609_v1.alpaca_sip_timestamp_v1(b.rec->>'t') ts,
 population_top3000_202608_202609_v1.alpaca_sip_number_v1(b.rec->'o') o,population_top3000_202608_202609_v1.alpaca_sip_number_v1(b.rec->'h') h,
 population_top3000_202608_202609_v1.alpaca_sip_number_v1(b.rec->'l') l,population_top3000_202608_202609_v1.alpaca_sip_number_v1(b.rec->'c') c,
 population_top3000_202608_202609_v1.alpaca_sip_number_v1(b.rec->'v') v,population_top3000_202608_202609_v1.alpaca_sip_number_v1(b.rec->'n') n,
 population_top3000_202608_202609_v1.alpaca_sip_number_v1(b.rec->'vw') vw
 FROM jsonb_each(p_text::jsonb->'bars') e CROSS JOIN LATERAL jsonb_array_elements(e.value) WITH ORDINALITY b(rec,ord)
 ), located AS (
 SELECT r.*,(r.ts AT TIME ZONE 'America/New_York')::date ny_date,
 lag(ts) OVER(PARTITION BY symbol ORDER BY ordinal) previous_ts FROM raw r
 ), qualified AS (
 SELECT r.*,cal.regular_open_utc,cal.regular_close_utc,cal.session_date calendar_date,
 CASE WHEN r.ts IS NULL THEN 0 WHEN cal.session_date IS NULL THEN 4
 WHEN r.ts>=((r.ny_date+time '04:00') AT TIME ZONE 'America/New_York') AND r.ts<cal.regular_open_utc THEN 1
 WHEN r.ts>=cal.regular_open_utc AND r.ts<cal.regular_close_utc THEN 2
 WHEN r.ts>=cal.regular_close_utc AND r.ts<((r.ny_date+time '20:00') AT TIME ZONE 'America/New_York') THEN 3 ELSE 4 END code
 FROM located r LEFT JOIN population_top3000_202608_202609_v1.catalog_calendar cal ON cal.scope_id=p_spec->>'calendar_scope_id' AND cal.session_date=r.ny_date
 )
 SELECT rownum,symbol,ordinal,ts,ny_date,code::smallint,o,h,l,c,v,n,vw,
 (CASE WHEN ts IS NULL THEN 1 ELSE 0 END)
 +(CASE WHEN ts IS NOT NULL AND (ts<>date_trunc('minute',ts) OR coalesce(rec->>'t','')!~':[0-5][0-9]:00([.]0{1,9})?([Zz]|[+-][0-9]{2}:[0-9]{2})$') THEN 2 ELSE 0 END)
 +(CASE WHEN ts IS NOT NULL AND (ts<(p_spec->>'start_utc')::timestamptz OR ts>=(p_spec->>'end_exclusive_utc')::timestamptz) THEN 4 ELSE 0 END)
 +(CASE WHEN o IS NULL OR h IS NULL OR l IS NULL OR c IS NULL OR v IS NULL OR n IS NULL OR vw IS NULL THEN 8 ELSE 0 END)
 +(CASE WHEN o<=0 OR h<=0 OR l<=0 OR c<=0 OR vw<=0 OR v<0 OR n<0 OR h<greatest(o,l,c) OR l>least(o,h,c) THEN 16 ELSE 0 END)
 +(CASE WHEN n IS NOT NULL AND trunc(n)<>n THEN 32 ELSE 0 END)
 +(CASE WHEN code=4 THEN 64 ELSE 0 END)
 +(CASE WHEN previous_ts IS NOT NULL AND ts<=previous_ts THEN 128 ELSE 0 END),
 sha256(convert_to(rec::text,'UTF8'))
 FROM qualified ORDER BY rownum;
$f$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.alpaca_sip_resource_guard_v1()
RETURNS bigint LANGUAGE plpgsql SET search_path TO 'pg_catalog' AS $f$
DECLARE bound bigint;actual bigint;
BEGIN
 SELECT (budget_evidence#>>'{resource_limits,max_relation_bytes}')::bigint INTO bound FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton;
 IF bound IS NULL OR bound<1 OR bound>2000000000 THEN RAISE EXCEPTION 'ALPACA_SIP_RESOURCE_BOUND_NOT_ADMITTED';END IF;
 SELECT coalesce(sum(pg_total_relation_size(c.oid)),0) INTO actual FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='population_top3000_202608_202609_v1' AND c.relkind IN('r','m');
 IF actual>bound THEN RAISE EXCEPTION 'ALPACA_SIP_POST_BATCH_RELATION_CAP_EXCEEDED';END IF;
 RETURN actual;
END $f$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.alpaca_sip_normalize_v1(p_attempt uuid,p_token text)
RETURNS jsonb LANGUAGE plpgsql SET search_path TO 'pg_catalog' SET timezone TO 'UTC' AS $f$
DECLARE tid uuid;t population_top3000_202608_202609_v1.pipeline_task%rowtype;c jsonb;p population_top3000_202608_202609_v1.alpaca_sip_source_page_v1%rowtype;
 d population_top3000_202608_202609_v1.source_document%rowtype;k text;lastpage integer;total integer;terminal integer;cnt bigint;newrows bigint;h text;actual_h text;actual_cnt bigint;relations bigint;
BEGIN
 tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);
 SELECT * INTO t FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=tid;
 IF t.stage<>'NORMALIZE' THEN RAISE EXCEPTION 'ALPACA_SIP_STAGE_MISMATCH';END IF;
 c:=population_top3000_202608_202609_v1.alpaca_sip_context_v1(tid);k:=c->>'archive_key';
 PERFORM population_top3000_202608_202609_v1.alpaca_sip_resource_guard_v1();
 SELECT count(*),max(page_number) FILTER(WHERE next_token IS NULL) INTO total,terminal FROM population_top3000_202608_202609_v1.alpaca_sip_source_page_v1 WHERE archive_key=k;
 IF terminal IS NULL OR total<>terminal OR (SELECT count(*) FROM population_top3000_202608_202609_v1.alpaca_sip_source_page_v1 WHERE archive_key=k AND next_token IS NULL)<>1
 OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.alpaca_sip_source_page_v1 q LEFT JOIN population_top3000_202608_202609_v1.alpaca_sip_source_page_v1 prev ON prev.archive_key=q.archive_key AND prev.page_number=q.page_number-1 WHERE q.archive_key=k AND q.page_number>1 AND q.request_token IS DISTINCT FROM prev.next_token)
 THEN RETURN jsonb_build_object('state','BLOCKED','blocker_code','ALPACA_SIP_SOURCE_CHAIN_INCOMPLETE','resumption_condition',jsonb_build_object('required_action','Finish admitted source pagination or explicitly reconcile the stopped cursor. No missing-minute zeros are inferred.','automatic_retry',false));END IF;
 lastpage:=coalesce((t.checkpoint->>'last_normalized_page')::integer,0);
 IF lastpage NOT BETWEEN 0 AND terminal-1 OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.alpaca_sip_source_page_v1 WHERE archive_key=k AND page_number<=lastpage AND normalized_at IS NULL) THEN RAISE EXCEPTION 'ALPACA_SIP_NORMALIZE_CHECKPOINT_INVALID';END IF;
 SELECT * INTO p FROM population_top3000_202608_202609_v1.alpaca_sip_source_page_v1 WHERE archive_key=k AND page_number=lastpage+1 FOR UPDATE;
 IF NOT FOUND OR p.request_spec IS DISTINCT FROM c->'request_spec' OR p.input_versions IS DISTINCT FROM t.input_versions THEN RAISE EXCEPTION 'ALPACA_SIP_PAGE_INPUTS_CHANGED';END IF;
 SELECT * INTO d FROM population_top3000_202608_202609_v1.source_document WHERE source_id=p.source_id;
 IF NOT FOUND OR d.content_text IS NULL OR d.source_sha256 IS DISTINCT FROM p.source_sha256 OR d.source_sha256 IS DISTINCT FROM encode(sha256(convert_to(d.content_text,'UTF8')),'hex')
 OR d.content_bytes IS DISTINCT FROM octet_length(d.content_text)::bigint OR d.content_bytes<>p.wire_bytes OR d.source_uri<>p.request_url OR d.source_provider<>'Alpaca' OR d.retrieved_at<>p.retrieved_at OR d.historical_received_at IS NOT NULL
 OR d.content_text::jsonb->>'next_page_token' IS DISTINCT FROM p.next_token THEN RAISE EXCEPTION 'ALPACA_SIP_RETAINED_SOURCE_INTEGRITY';END IF;
 INSERT INTO population_top3000_202608_202609_v1.alpaca_sip_compact_bar_v1(page_id,source_row,source_symbol,symbol_row,bar_ts,session_date,session_code,open,high,low,close,volume,trade_count,vwap,quality_bits,raw_record_sha256)
 SELECT p.page_id,x.* FROM population_top3000_202608_202609_v1.alpaca_sip_parse_page_v1(d.content_text,p.request_spec) x ON CONFLICT(page_id,source_row) DO NOTHING;
 GET DIAGNOSTICS newrows=ROW_COUNT;
 SELECT count(*),encode(sha256(convert_to(coalesce(string_agg(encode(sha256(convert_to(to_jsonb(x)::text,'UTF8')),'hex'),E'\n' ORDER BY source_row),''),'UTF8')),'hex') INTO cnt,h FROM population_top3000_202608_202609_v1.alpaca_sip_parse_page_v1(d.content_text,p.request_spec) x;
 SELECT count(*),encode(sha256(convert_to(coalesce(string_agg(encode(sha256(convert_to((to_jsonb(x)-'page_id')::text,'UTF8')),'hex'),E'\n' ORDER BY source_row),''),'UTF8')),'hex') INTO actual_cnt,actual_h FROM population_top3000_202608_202609_v1.alpaca_sip_compact_bar_v1 x WHERE page_id=p.page_id;
 IF cnt<>p.result_count OR actual_cnt<>cnt OR actual_h IS DISTINCT FROM h OR (p.normalized_sha256 IS NOT NULL AND p.normalized_sha256<>h) THEN RAISE EXCEPTION 'ALPACA_SIP_COMPACT_FULL_REPLAY_MISMATCH';END IF;
 UPDATE population_top3000_202608_202609_v1.alpaca_sip_source_page_v1 SET normalized_at=coalesce(normalized_at,clock_timestamp()),normalized_rows=cnt,normalized_sha256=h WHERE page_id=p.page_id;
 relations:=population_top3000_202608_202609_v1.alpaca_sip_resource_guard_v1();
 RETURN jsonb_build_object('state',CASE WHEN p.page_number=terminal THEN 'COMPLETE' ELSE 'CONTINUE' END,'checkpoint',jsonb_build_object('last_normalized_page',p.page_number),
 'commit_receipt',jsonb_build_object('commit_key','alpaca-compact:'||p.source_id||':'||h,'output_sha256',h,'new_committed_units',newrows,'immutable_output_ids',jsonb_build_array(p.source_id),'input_versions',t.input_versions,'unit_kind','SOURCE_SYMBOL_MINUTE_BAR','source_bars',cnt,'shared_families',jsonb_build_array('P01','P02'),'mission_relation_bytes',relations,'historical_company_membership_verified',false));
END $f$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.alpaca_sip_derive_v1(p_attempt uuid,p_token text)
RETURNS jsonb LANGUAGE plpgsql SET search_path TO 'pg_catalog' SET timezone TO 'UTC' AS $f$
DECLARE tid uuid;t population_top3000_202608_202609_v1.pipeline_task%rowtype;c jsonb;k text;h text;summary jsonb;n bigint;
BEGIN
 tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);
 SELECT * INTO t FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=tid;
 IF t.stage<>'DERIVE' THEN RAISE EXCEPTION 'ALPACA_SIP_STAGE_MISMATCH';END IF;
 c:=population_top3000_202608_202609_v1.alpaca_sip_context_v1(tid);k:=c->>'archive_key';
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.alpaca_sip_source_page_v1 WHERE archive_key=k AND next_token IS NULL)
 OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.alpaca_sip_source_page_v1 WHERE archive_key=k AND (normalized_at IS NULL OR normalized_rows IS DISTINCT FROM result_count)) THEN RAISE EXCEPTION 'ALPACA_SIP_NORMALIZATION_INCOMPLETE';END IF;
 SELECT count(*),jsonb_build_object('source_rows',count(*),'premarket_rows',count(*) FILTER(WHERE b.session_code=1),'regular_rows',count(*) FILTER(WHERE b.session_code=2),'postmarket_rows',count(*) FILTER(WHERE b.session_code=3),'outside_declared_session_rows',count(*) FILTER(WHERE b.session_code=4),'rows_with_quality_flags',count(*) FILTER(WHERE b.quality_bits<>0),'source_symbols',count(DISTINCT b.source_symbol),'first_bar',min(b.bar_ts),'last_bar',max(b.bar_ts)) INTO n,summary
 FROM population_top3000_202608_202609_v1.alpaca_sip_compact_bar_v1 b JOIN population_top3000_202608_202609_v1.alpaca_sip_source_page_v1 p USING(page_id) WHERE p.archive_key=k;
 IF n<>(SELECT sum(result_count) FROM population_top3000_202608_202609_v1.alpaca_sip_source_page_v1 WHERE archive_key=k) THEN RAISE EXCEPTION 'ALPACA_SIP_BATCH_ROW_RECONCILIATION';END IF;
 summary:=summary||jsonb_build_object('same_symbol_timestamp_duplicate_occurrences',(SELECT coalesce(sum(duplicate_counts.occurrences-1),0) FROM (SELECT count(*) occurrences FROM population_top3000_202608_202609_v1.alpaca_sip_compact_bar_v1 b JOIN population_top3000_202608_202609_v1.alpaca_sip_source_page_v1 p USING(page_id) WHERE p.archive_key=k AND b.bar_ts IS NOT NULL GROUP BY b.source_symbol,b.bar_ts HAVING count(*)>1) duplicate_counts),
 'same_symbol_timestamp_conflicting_keys',(SELECT count(*) FROM (SELECT b.source_symbol,b.bar_ts FROM population_top3000_202608_202609_v1.alpaca_sip_compact_bar_v1 b JOIN population_top3000_202608_202609_v1.alpaca_sip_source_page_v1 p USING(page_id) WHERE p.archive_key=k AND b.bar_ts IS NOT NULL GROUP BY b.source_symbol,b.bar_ts HAVING count(DISTINCT b.raw_record_sha256)>1) d),
 'identity_state','UNRESOLVED_SOURCE_SYMBOL_ONLY','expected_printed_bars',NULL,'source_query_terminal',true,'independent_price_accuracy_verified',false,'historical_first_receipt_proven',false,'missing_minutes_filled',0,'shared_target_families',jsonb_build_array('P01','P02'));
 h:=encode(sha256(convert_to(jsonb_build_object('archive_key',k,'summary',summary,'pages',(SELECT jsonb_agg(jsonb_build_array(page_number,source_sha256,normalized_sha256) ORDER BY page_number) FROM population_top3000_202608_202609_v1.alpaca_sip_source_page_v1 WHERE archive_key=k))::text,'UTF8')),'hex');
 RETURN jsonb_build_object('state','COMPLETE','commit_receipt',jsonb_build_object('commit_key','alpaca-batch-source-summary:'||k||':'||h,'output_sha256',h,'new_committed_units',n,'immutable_output_ids',jsonb_build_array(k),'input_versions',t.input_versions,'source_summary',summary,'company_population_certified',false));
END $f$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.alpaca_sip_quality_hold_v1(p_attempt uuid,p_token text)
RETURNS jsonb LANGUAGE plpgsql SET search_path TO 'pg_catalog' AS $f$
DECLARE tid uuid;c jsonb;
BEGIN
 tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);c:=population_top3000_202608_202609_v1.alpaca_sip_context_v1(tid);
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=tid AND stage='VALIDATE') THEN RAISE EXCEPTION 'ALPACA_SIP_STAGE_MISMATCH';END IF;
 RETURN jsonb_build_object('state','BLOCKED','blocker_code','ALPACA_SIP_COMPACT_POPULATION_REQUIRES_IDENTITY_AND_EXPECTATIONS','resumption_condition',jsonb_build_object('archive_key',c->>'archive_key','required_action','Independently reconcile the compact source population, resolve dated company/security identity and trade/session applicability, and bind reviewed compact validation projections. Existing source pages and compact bars remain preserved.','dense_clock_denominator_used',false,'historical_receipt_proven',false,'automatic_retry',false));
END $f$;

-- Safe coexistence: retain the exact reviewed original handler for all existing retained-source tasks.
-- Root changes adapter bindings only after tests and after every old attempt has finished.
CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.price_pipeline_acquire_v2(p_attempt uuid,p_token text)
RETURNS jsonb LANGUAGE plpgsql SET search_path TO 'pg_catalog' AS $f$
DECLARE tid uuid;kind text;
BEGIN
 tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);
 SELECT payload->>'kind' INTO kind FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=tid AND stage='ACQUIRE' AND family_id='P01';
 IF kind='RETAINED_ALPACA_AUGUST_SESSION' THEN
  IF encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.price_retained_partition_v1(uuid,text)'::regprocedure),'UTF8')),'hex')<>'54ee6c5efb998ef35c9064a8e3767261f9498ab0546f3472675fd4cd3dceb586' THEN RAISE EXCEPTION 'RETAINED_PRICE_HANDLER_CHANGED';END IF;
  RETURN population_top3000_202608_202609_v1.price_retained_partition_v1(p_attempt,p_token);
 ELSIF kind='ALPACA_SIP_GAP_BATCH_V1' THEN RETURN population_top3000_202608_202609_v1.alpaca_sip_acquire_v1(p_attempt,p_token);
 ELSE RAISE EXCEPTION 'PRICE_ACQUIRE_KIND_NOT_ADMITTED';END IF;
END $f$;
CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.price_pipeline_normalize_v2(p_attempt uuid,p_token text)
RETURNS jsonb LANGUAGE plpgsql SET search_path TO 'pg_catalog' AS $f$
DECLARE tid uuid;kind text;
BEGIN
 tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);
 SELECT payload->>'kind' INTO kind FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=tid AND stage='NORMALIZE' AND family_id='P01';
 IF kind='RETAINED_ALPACA_AUGUST_SESSION' THEN
  IF encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.price_retained_partition_v1(uuid,text)'::regprocedure),'UTF8')),'hex')<>'54ee6c5efb998ef35c9064a8e3767261f9498ab0546f3472675fd4cd3dceb586' THEN RAISE EXCEPTION 'RETAINED_PRICE_HANDLER_CHANGED';END IF;
  RETURN population_top3000_202608_202609_v1.price_retained_partition_v1(p_attempt,p_token);
 ELSIF kind='ALPACA_SIP_GAP_BATCH_V1' THEN RETURN population_top3000_202608_202609_v1.alpaca_sip_normalize_v1(p_attempt,p_token);
 ELSE RAISE EXCEPTION 'PRICE_NORMALIZE_KIND_NOT_ADMITTED';END IF;
END $f$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.alpaca_sip_code_hash_v1()
RETURNS text LANGUAGE sql STABLE SET search_path TO 'pg_catalog' AS $f$
 SELECT encode(sha256(convert_to(string_agg(fn||':'||pg_get_functiondef(('population_top3000_202608_202609_v1.'||fn)::regprocedure),E'\n' ORDER BY fn),'UTF8')),'hex')
 FROM (VALUES ('alpaca_sip_urlencode_v1(text)'),('alpaca_sip_spec_v1(jsonb)'),('alpaca_sip_context_v1(uuid)'),('alpaca_sip_acquire_v1(uuid,text)'),('alpaca_sip_claim_v1(uuid,text)'),('alpaca_sip_commit_v1(uuid,text,jsonb)'),('alpaca_sip_failure_v1(uuid,text,jsonb)'),('alpaca_sip_timestamp_v1(text)'),('alpaca_sip_number_v1(jsonb)'),('alpaca_sip_parse_page_v1(text,jsonb)'),('alpaca_sip_resource_guard_v1()'),('alpaca_sip_normalize_v1(uuid,text)'),('alpaca_sip_derive_v1(uuid,text)'),('alpaca_sip_quality_hold_v1(uuid,text)'),('price_pipeline_acquire_v2(uuid,text)'),('price_pipeline_normalize_v2(uuid,text)')) v(fn);
$f$;
DO $permissions$
DECLARE r record;
BEGIN
 FOR r IN SELECT p.oid::regprocedure fn FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='population_top3000_202608_202609_v1' AND (p.proname LIKE 'alpaca_sip_%_v1' OR p.proname IN('price_pipeline_acquire_v2','price_pipeline_normalize_v2')) LOOP
  EXECUTE format('REVOKE ALL ON FUNCTION %s FROM PUBLIC,anon,authenticated',r.fn);
  EXECUTE format('GRANT EXECUTE ON FUNCTION %s TO postgres,service_role',r.fn);
 END LOOP;
END $permissions$;
