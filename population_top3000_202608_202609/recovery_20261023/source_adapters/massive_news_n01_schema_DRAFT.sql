-- Root-only reviewed installation draft. Does not enable, seed or dispatch any worker.
-- Separate N01 news metadata lane. No article fetching, N02/N03 mutation or cohort certification.
CREATE TABLE IF NOT EXISTS population_top3000_202608_202609_v1.massive_news_archive_page_v1 (
 archive_key text NOT NULL,
 family_id text NOT NULL CHECK(family_id='N01'),
 partition_key text NOT NULL CHECK(partition_key='news-published-202608-202609'),
 page_number integer NOT NULL CHECK(page_number BETWEEN 1 AND 100),
 acquire_task_id uuid NOT NULL REFERENCES population_top3000_202608_202609_v1.pipeline_task(task_id),
 attempt_id uuid NOT NULL REFERENCES population_top3000_202608_202609_v1.pipeline_attempt(attempt_id),
 source_sha256 text NOT NULL CHECK(source_sha256~'^[0-9a-f]{64}$'),
 source_id text NOT NULL UNIQUE REFERENCES population_top3000_202608_202609_v1.source_document(source_id),
 request_url text NOT NULL,
 next_url text,
 wire_sha256 text NOT NULL CHECK(wire_sha256~'^[0-9a-f]{64}$'),
 wire_bytes bigint NOT NULL CHECK(wire_bytes BETWEEN 1 AND 8388608),
 result_count integer NOT NULL CHECK(result_count BETWEEN 0 AND 1000),
 retrieved_at timestamptz NOT NULL,
 normalized_at timestamptz,
 normalized_source_records integer,
 PRIMARY KEY(archive_key,page_number),
 UNIQUE(archive_key,request_url),
 CHECK(request_url !~* '(apikey|api_key|access_token|authorization)='),
 CHECK(next_url IS NULL OR next_url !~* '(apikey|api_key|access_token|authorization)=')
);
ALTER TABLE population_top3000_202608_202609_v1.massive_news_archive_page_v1 ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON population_top3000_202608_202609_v1.massive_news_archive_page_v1 FROM PUBLIC,anon,authenticated;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.massive_news_archive_key_v1(p_family text,p_partition text,p_versions jsonb)
RETURNS text LANGUAGE sql IMMUTABLE SET search_path TO 'pg_catalog' SET timezone TO 'UTC' AS $function$
 SELECT 'massive-archive:'||p_family||':'||encode(sha256(convert_to(jsonb_build_object('family',p_family,'partition',p_partition,'input_versions',p_versions)::text,'UTF8')),'hex');
$function$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.massive_news_context_v1(p_task uuid)
RETURNS jsonb LANGUAGE plpgsql SET search_path TO 'pg_catalog' SET timezone TO 'UTC' AS $function$
DECLARE t population_top3000_202608_202609_v1.pipeline_task%rowtype;k text;n integer;lim integer;url text;prior population_top3000_202608_202609_v1.massive_news_archive_page_v1%rowtype;
BEGIN
 SELECT * INTO t FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=p_task;
 IF NOT FOUND OR t.family_id<>'N01' OR t.partition_key<>'news-published-202608-202609'
 OR t.input_versions->>'scope_sha256' IS DISTINCT FROM '8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd'
 OR t.input_versions->>'archive_contract_revision' IS DISTINCT FROM 'massive-news-20261023-v1'
 THEN RAISE EXCEPTION 'MASSIVE_NEWS_SCOPE_NOT_ADMITTED';END IF;
 IF t.input_versions->>'news_bundle_sha256' IS DISTINCT FROM population_top3000_202608_202609_v1.massive_news_code_hash_v1() THEN RAISE EXCEPTION 'MASSIVE_NEWS_HELPER_BUNDLE_CHANGED';END IF;
 k:=population_top3000_202608_202609_v1.massive_news_archive_key_v1(t.family_id,t.partition_key,t.input_versions);
 IF coalesce(t.checkpoint->>'next_page','1') !~ '^[0-9]{1,4}$' OR coalesce(t.payload->>'max_pages','1') !~ '^[0-9]{1,4}$' THEN RAISE EXCEPTION 'MASSIVE_NEWS_PAGE_LIMIT_INVALID';END IF;
 n:=coalesce(t.checkpoint->>'next_page','1')::integer;lim:=coalesce(t.payload->>'max_pages','1')::integer;
 IF n<1 OR n>101 OR lim<1 OR lim>100 THEN RAISE EXCEPTION 'MASSIVE_NEWS_PAGE_LIMIT_INVALID';END IF;
 IF n=1 THEN
  url:='https://api.massive.com/v2/reference/news?published_utc.gte=2026-08-01T04%3A00%3A00Z&published_utc.lt=2026-10-01T04%3A00%3A00Z&order=asc&sort=published_utc&limit=1000';
 ELSE
  SELECT * INTO prior FROM population_top3000_202608_202609_v1.massive_news_archive_page_v1 WHERE archive_key=k AND page_number=n-1;
  IF NOT FOUND OR prior.next_url IS NULL OR t.checkpoint->>'last_source_id' IS DISTINCT FROM prior.source_id
  OR prior.source_sha256 IS DISTINCT FROM t.checkpoint->>'last_source_sha256'
  OR (SELECT source_sha256 FROM population_top3000_202608_202609_v1.source_document WHERE source_id=prior.source_id) IS DISTINCT FROM prior.source_sha256
  THEN RAISE EXCEPTION 'MASSIVE_NEWS_CONTINUATION_EVIDENCE_MISSING';END IF;
  url:=prior.next_url;
  IF url !~ '^https://api[.]massive[.]com/v2/reference/news[?]cursor=[A-Za-z0-9_.~%+\-]+$' THEN RAISE EXCEPTION 'MASSIVE_NEWS_UNSAFE_CURSOR';END IF;
 END IF;
 RETURN jsonb_build_object('state',CASE WHEN n>lim THEN 'PAGE_LIMIT_REVIEW' ELSE 'READY' END,'archive_key',k,'page_number',n,'max_pages',lim,
 'family_id',t.family_id,'partition_key',t.partition_key,'request_url',url,'max_response_bytes',8388608,'max_runtime_ms',45000);
END $function$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.massive_news_acquire_v1(p_attempt uuid,p_token text)
RETURNS jsonb LANGUAGE plpgsql SET search_path TO 'pg_catalog' SET timezone TO 'UTC' AS $function$
DECLARE tid uuid;c jsonb;
BEGIN
 tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=tid AND stage='ACQUIRE') THEN RAISE EXCEPTION 'MASSIVE_NEWS_STAGE_MISMATCH';END IF;
 c:=population_top3000_202608_202609_v1.massive_news_context_v1(tid);
 IF c->>'state'='PAGE_LIMIT_REVIEW' THEN RETURN jsonb_build_object('state','BLOCKED','blocker_code','MASSIVE_NEWS_PAGE_CAP_REVIEW','resumption_condition',jsonb_build_object('event','RETAINED_PAGE_REVIEW_AND_BOUNDED_CAP_ADMISSION','next_page',c->'page_number','existing_pages_preserved',true));END IF;
 RETURN population_top3000_202608_202609_v1.pipeline_dispatch_edge_v1(p_attempt,p_token,'population-top3000-news-v1');
END $function$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.massive_news_claim_v1(p_attempt uuid,p_token text)
RETURNS jsonb LANGUAGE plpgsql SET search_path TO 'pg_catalog' SET timezone TO 'UTC' AS $function$
DECLARE tid uuid;c jsonb;r jsonb;
BEGIN
 tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=tid AND stage='ACQUIRE') THEN RAISE EXCEPTION 'MASSIVE_NEWS_STAGE_MISMATCH';END IF;
 c:=population_top3000_202608_202609_v1.massive_news_context_v1(tid);
 IF c->>'state'<>'READY' THEN
  RETURN population_top3000_202608_202609_v1.pipeline_finish_v1(p_attempt,p_token,jsonb_build_object('state','BLOCKED','blocker_code','MASSIVE_NEWS_PAGE_CAP_REVIEW','resumption_condition',jsonb_build_object('event','RETAINED_PAGE_REVIEW_AND_BOUNDED_CAP_ADMISSION','next_page',c->'page_number','existing_pages_preserved',true)));
 END IF;
 -- Every valid attempt may reserve at most one request, before credentials are used.
 r:=population_top3000_202608_202609_v1.pipeline_provider_reserve_v1(p_attempt,p_token,'Massive',8388608,0);
 IF r->>'state'<>'RESERVED' THEN
  RETURN population_top3000_202608_202609_v1.pipeline_finish_v1(p_attempt,p_token,jsonb_build_object('state','BLOCKED',
   'blocker_code',CASE WHEN r->>'state'='WAIT' THEN 'DEPENDENCY_PENDING' ELSE 'PROVIDER_BUDGET_OR_RESOURCE_HOLD' END,
   'recheck_at',CASE WHEN r->>'state'='WAIT' THEN r->>'not_before' ELSE NULL END,
   'resumption_condition',jsonb_build_object('event',CASE WHEN r->>'state'='WAIT' THEN 'PROVIDER_SLOT_AVAILABLE' ELSE 'CONTROLLER_ADMISSION_RECONCILED' END,'receipt',r)));
 END IF;
 RETURN c||jsonb_build_object('state','RESERVED');
END $function$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.massive_news_commit_v1(p_attempt uuid,p_token text,p_page jsonb)
RETURNS jsonb LANGUAGE plpgsql SET search_path TO 'pg_catalog' SET timezone TO 'UTC' AS $function$
DECLARE tid uuid;t population_top3000_202608_202609_v1.pipeline_task%rowtype;c jsonb;body jsonb;txt text;h text;wire text;
 sid text;k text;n integer;cnt integer;bytes bigint;stored_bytes bigint;v_next_url text;at_time timestamptz;fresh integer;receipt jsonb;
BEGIN
 tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);
 SELECT * INTO t FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=tid;
 IF t.stage<>'ACQUIRE' THEN RAISE EXCEPTION 'MASSIVE_NEWS_STAGE_MISMATCH';END IF;
 c:=population_top3000_202608_202609_v1.massive_news_context_v1(tid);
 IF c->>'state'<>'READY' OR jsonb_typeof(p_page) IS DISTINCT FROM 'object' OR p_page->>'archive_key' IS DISTINCT FROM c->>'archive_key'
 OR p_page->>'page_number' IS DISTINCT FROM c->>'page_number' OR p_page->>'request_url' IS DISTINCT FROM c->>'request_url'
 OR p_page->>'http_status' IS DISTINCT FROM '200' THEN RAISE EXCEPTION 'MASSIVE_NEWS_RECEIPT_SCOPE_MISMATCH';END IF;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_provider_lease WHERE attempt_id=p_attempt AND provider='Massive' AND released_at IS NULL AND reserved_bytes=8388608 AND started_at+interval '45 seconds'>clock_timestamp()) THEN RAISE EXCEPTION 'MASSIVE_NEWS_RESERVATION_REQUIRED';END IF;
 txt:=p_page->>'content_text';stored_bytes:=octet_length(txt);bytes:=(p_page->>'wire_bytes')::bigint;wire:=p_page->>'wire_sha256';
 IF txt IS NULL OR stored_bytes>8388608 OR bytes IS NULL OR bytes<1 OR bytes>8388608 OR wire IS NULL OR wire!~'^[0-9a-f]{64}$'
 OR coalesce((p_page->>'elapsed_ms')::bigint,-1) NOT BETWEEN 0 AND 45000 THEN RAISE EXCEPTION 'MASSIVE_NEWS_SOURCE_BOUND_INVALID';END IF;
 h:=encode(sha256(convert_to(txt,'UTF8')),'hex');
 IF h IS DISTINCT FROM p_page->>'source_sha256' THEN RAISE EXCEPTION 'MASSIVE_NEWS_STORED_HASH_MISMATCH';END IF;
 body:=txt::jsonb;
 IF jsonb_typeof(body) IS DISTINCT FROM 'object' OR (body ? 'status' AND body->>'status' IS DISTINCT FROM 'OK') OR jsonb_typeof(body->'results') IS DISTINCT FROM 'array' THEN RAISE EXCEPTION 'MASSIVE_NEWS_INVALID_SOURCE_ENVELOPE';END IF;
 cnt:=jsonb_array_length(body->'results');
 IF body ? 'count' AND (jsonb_typeof(body->'count') IS DISTINCT FROM 'number' OR body->>'count' !~ '^[0-9]{1,4}$' OR (body->>'count')::integer<>cnt) THEN RAISE EXCEPTION 'MASSIVE_NEWS_SOURCE_COUNT_MISMATCH';END IF;
 IF cnt>1000 OR cnt IS DISTINCT FROM (p_page->>'result_count')::integer OR EXISTS(SELECT 1 FROM jsonb_array_elements(body->'results') v WHERE jsonb_typeof(v) IS DISTINCT FROM 'object') THEN RAISE EXCEPTION 'MASSIVE_NEWS_SOURCE_RECORD_SHAPE';END IF;
 v_next_url:=p_page->>'next_url';
 IF v_next_url IS DISTINCT FROM body->>'next_url' OR (v_next_url IS NOT NULL AND (v_next_url!~'^https://api[.]massive[.]com/v2/reference/news[?]cursor=[A-Za-z0-9_.~%+\-]+$' OR length(v_next_url)>16384 OR v_next_url=c->>'request_url')) THEN RAISE EXCEPTION 'MASSIVE_NEWS_CURSOR_MISMATCH';END IF;
 IF txt~* '"(apiKey|api_key|access_token|authorization)"[[:space:]]*:' OR txt~* '[?&](apiKey|api_key|access_token)=' THEN RAISE EXCEPTION 'MASSIVE_NEWS_UNREDACTED_CREDENTIAL_FIELD';END IF;
 k:=c->>'archive_key';n:=(c->>'page_number')::integer;sid:=k||':page:'||n::text;at_time:=(p_page->>'retrieved_at')::timestamptz;
 IF at_time IS NULL OR at_time<(SELECT started_at FROM population_top3000_202608_202609_v1.pipeline_attempt WHERE attempt_id=p_attempt)-interval '30 seconds' OR at_time>clock_timestamp()+interval '30 seconds' THEN RAISE EXCEPTION 'MASSIVE_NEWS_RETRIEVAL_CLOCK_INVALID';END IF;
 IF v_next_url IS NOT NULL AND EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.massive_news_archive_page_v1 WHERE archive_key=k AND request_url=v_next_url) THEN RAISE EXCEPTION 'MASSIVE_NEWS_CURSOR_CYCLE';END IF;
 INSERT INTO population_top3000_202608_202609_v1.source_document
 (source_id,source_sha256,source_uri,source_provider,retrieved_at,source_published_at,historical_received_at,content_text,content_bytes,evidence,license_policy)
 VALUES(sid,h,c->>'request_url','Massive',at_time,NULL,NULL,txt,stored_bytes,
 jsonb_build_object('wire_sha256',wire,'wire_bytes',bytes,'stored_content_bytes',stored_bytes,'hash_representation','EXACT_UTF8_RESPONSE_WITH_TOP_LEVEL_NEXT_URL_CREDENTIAL_REDACTION_ONLY','wire_bytes_retained',false,'provider_request_id',p_page->>'provider_request_id','scope','N01 provider publication times 2026-08-01T04:00:00Z inclusive through 2026-10-01T04:00:00Z exclusive','original_historical_receipt',false,'independent_publication_time_verified',false,'article_bodies_fetched',false),
 'EXISTING_MASSIVE_SUBSCRIPTION_API_RESPONSE_INTERNAL_USE') ON CONFLICT(source_id) DO NOTHING;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.source_document d WHERE d.source_id=sid AND d.source_sha256=h AND d.source_uri=c->>'request_url' AND d.content_text=txt AND d.content_bytes=stored_bytes AND d.evidence->>'wire_sha256'=wire AND (d.evidence->>'wire_bytes')::bigint=bytes) THEN RAISE EXCEPTION 'MASSIVE_NEWS_SOURCE_REVISION_CONFLICT';END IF;
 INSERT INTO population_top3000_202608_202609_v1.massive_news_archive_page_v1
 (archive_key,family_id,partition_key,page_number,acquire_task_id,attempt_id,source_id,source_sha256,request_url,next_url,wire_sha256,wire_bytes,result_count,retrieved_at)
 VALUES(k,t.family_id,t.partition_key,n,tid,p_attempt,sid,h,c->>'request_url',v_next_url,wire,bytes,cnt,at_time)
 ON CONFLICT(archive_key,page_number) DO NOTHING;
 GET DIAGNOSTICS fresh=ROW_COUNT;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.massive_news_archive_page_v1 q WHERE q.archive_key=k AND q.page_number=n AND q.source_id=sid AND q.source_sha256=h AND q.wire_sha256=wire AND q.wire_bytes=bytes AND q.request_url=c->>'request_url' AND q.next_url IS NOT DISTINCT FROM v_next_url AND q.result_count=cnt) THEN RAISE EXCEPTION 'MASSIVE_NEWS_PAGE_REVISION_CONFLICT';END IF;
 PERFORM population_top3000_202608_202609_v1.pipeline_provider_release_v1(p_attempt,p_token,stored_bytes,0,0);
 receipt:=jsonb_build_object('state',CASE WHEN v_next_url IS NULL THEN 'COMPLETE' ELSE 'CONTINUE' END,
 'checkpoint',jsonb_build_object('next_page',n+1,'last_source_id',sid,'last_source_sha256',h,'next_url',v_next_url,'source_records_seen',coalesce((t.checkpoint->>'source_records_seen')::bigint,0)+cnt),
 'not_before',(SELECT next_provider_at FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton),
 'commit_receipt',jsonb_build_object('commit_key','massive-page:'||sid||':'||h,'output_sha256',h,'new_committed_units',fresh,'immutable_output_ids',jsonb_build_array(sid),'input_versions',t.input_versions,'raw_source_records',cnt,'terminal_page',v_next_url IS NULL,'cohort_certified',false));
 RETURN population_top3000_202608_202609_v1.pipeline_finish_v1(p_attempt,p_token,receipt);
END $function$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.massive_news_failure_v1(p_attempt uuid,p_token text,p_failure jsonb)
RETURNS jsonb LANGUAGE plpgsql SET search_path TO 'pg_catalog' SET timezone TO 'UTC' AS $function$
DECLARE tid uuid;code text;class text;bytes bigint;retry integer;unknown_effect boolean;details jsonb;
BEGIN
 tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=tid AND family_id='N01' AND stage='ACQUIRE') THEN RAISE EXCEPTION 'MASSIVE_NEWS_STAGE_MISMATCH';END IF;
 code:=p_failure->>'code';class:=p_failure->>'failure_class';bytes:=(p_failure->>'received_bytes')::bigint;retry:=(p_failure->>'retry_after_seconds')::integer;
 IF code IS NULL OR code!~'^[A-Z0-9_]{1,80}$' OR class IS NULL OR class NOT IN('RATE_LIMIT','TRANSPORT_READ','AUTHENTICATION','PARSER','SOURCE_ABSENT') OR bytes IS NULL OR bytes NOT BETWEEN 0 AND 8388608 OR retry IS NULL OR retry NOT BETWEEN 0 AND 3600 THEN RAISE EXCEPTION 'MASSIVE_NEWS_FAILURE_RECEIPT_INVALID';END IF;
 details:=jsonb_build_object('code',code,'http_status',p_failure->'http_status','response_bytes_observed',bytes,'wire_sha256',p_failure->>'wire_sha256','fetch_started',p_failure->'fetch_started','raw_body_retained',false,'automatic_retry',false);
 unknown_effect:=coalesce((p_failure->>'fetch_started')::boolean,false)
  AND (p_failure->>'http_status' IS NULL OR code IN('DEADLINE','FETCH_FAILED','EMPTY_BODY') OR (p_failure->>'wire_sha256' IS NULL AND p_failure->>'http_status'='200'));
 IF unknown_effect THEN
  -- Retain the reservation. The existing expiry reconciler conservatively accounts it.
  -- No new request is admitted until the controller reconciles the uncertain effect.
  RETURN population_top3000_202608_202609_v1.pipeline_fail_v1(p_attempt,p_token,'UNCERTAIN_EFFECT','MASSIVE_NEWS:'||code,details,retry);
 END IF;
 -- This counter measures retained source bytes. An unretained error body adds zero.
 PERFORM population_top3000_202608_202609_v1.pipeline_provider_release_v1(p_attempt,p_token,0,0,CASE WHEN class='RATE_LIMIT' THEN retry ELSE 0 END);
 RETURN population_top3000_202608_202609_v1.pipeline_finish_v1(p_attempt,p_token,jsonb_build_object('state','BLOCKED','blocker_code','MASSIVE_NEWS_'||code,
 'resumption_condition',jsonb_build_object('event','CONTROLLER_REVIEW_OF_DEFINITIVE_SOURCE_FAILURE','details',details,'retry_after_seconds',retry,'preserve_counters_and_cursor',true)));
END $function$;

-- Every occurrence is retained, while identical provider-id/content versions normalize once.
CREATE TABLE IF NOT EXISTS population_top3000_202608_202609_v1.massive_news_record_link_v1 (
 archive_key text NOT NULL,page_number integer NOT NULL,source_row integer NOT NULL CHECK(source_row BETWEEN 1 AND 1000),
 source_id text NOT NULL REFERENCES population_top3000_202608_202609_v1.source_document(source_id),
 source_record_key text NOT NULL,raw_record_sha256 text NOT NULL CHECK(raw_record_sha256~'^[0-9a-f]{64}$'),unit_key text NOT NULL,
 PRIMARY KEY(archive_key,page_number,source_row),
 FOREIGN KEY(archive_key,page_number) REFERENCES population_top3000_202608_202609_v1.massive_news_archive_page_v1(archive_key,page_number)
);
CREATE INDEX IF NOT EXISTS massive_news_record_link_unit_v1 ON population_top3000_202608_202609_v1.massive_news_record_link_v1(archive_key,unit_key,page_number,source_row);
CREATE TABLE IF NOT EXISTS population_top3000_202608_202609_v1.massive_news_version_conflict_v1 (
 archive_key text NOT NULL,source_record_key text NOT NULL,version_count integer NOT NULL CHECK(version_count>1),
 version_sha256 text[] NOT NULL,evidence jsonb NOT NULL,record_sha256 text NOT NULL,
 PRIMARY KEY(archive_key,source_record_key)
);
ALTER TABLE population_top3000_202608_202609_v1.massive_news_record_link_v1 ENABLE ROW LEVEL SECURITY;
ALTER TABLE population_top3000_202608_202609_v1.massive_news_version_conflict_v1 ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON population_top3000_202608_202609_v1.massive_news_record_link_v1,population_top3000_202608_202609_v1.massive_news_version_conflict_v1 FROM PUBLIC,anon,authenticated;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.massive_news_timestamp_v1(p_value text)
RETURNS timestamptz LANGUAGE plpgsql IMMUTABLE SET search_path TO 'pg_catalog' SET timezone TO 'UTC' AS $function$
DECLARE d timestamptz;cast_text text;
BEGIN
 -- Explicit timezone is mandatory. Leap seconds are retained but not silently normalized.
 IF p_value IS NULL OR p_value !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}[Tt]([01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9]([.][0-9]{1,9})?([Zz]|[+-]([01][0-9]|2[0-3]):[0-5][0-9])$' THEN RETURN NULL;END IF;
 -- PostgreSQL rounds excess fractional precision. Truncate it before casting so
 -- instants just below an inclusive/exclusive boundary cannot cross the boundary.
 -- The exact provider timestamp remains unchanged in the retained raw record.
 cast_text:=p_value;
 IF p_value ~ '[.][0-9]{7,9}' THEN
  cast_text:=left(p_value,strpos(p_value,'.')+6)||substring(p_value from '([Zz]|[+-][0-9]{2}:[0-9]{2})$');
 END IF;
 BEGIN d:=cast_text::timestamptz;EXCEPTION WHEN datetime_field_overflow OR invalid_datetime_format THEN RETURN NULL;END;
 RETURN d;
END $function$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.massive_news_resource_guard_v1()
RETURNS bigint LANGUAGE plpgsql SET search_path TO 'pg_catalog' SET timezone TO 'UTC' AS $function$
DECLARE bound bigint;actual bigint;
BEGIN
 SELECT (budget_evidence#>>'{resource_limits,max_relation_bytes}')::bigint INTO bound
 FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton;
 IF bound IS NULL OR bound<1 OR bound>2000000000 THEN RAISE EXCEPTION 'MASSIVE_NEWS_RESOURCE_BOUND_NOT_ADMITTED';END IF;
 SELECT coalesce(sum(pg_total_relation_size(c.oid)),0) INTO actual FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
 WHERE n.nspname='population_top3000_202608_202609_v1' AND c.relkind IN('r','m');
 IF actual>bound THEN RAISE EXCEPTION 'MASSIVE_NEWS_POST_BATCH_RELATION_CAP_EXCEEDED actual=% bound=%',actual,bound;END IF;
 RETURN actual;
END $function$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.massive_news_normalize_v1(p_attempt uuid,p_token text)
RETURNS jsonb LANGUAGE plpgsql SET search_path TO 'pg_catalog' SET timezone TO 'UTC' AS $function$
DECLARE tid uuid;t population_top3000_202608_202609_v1.pipeline_task%rowtype;k text;p population_top3000_202608_202609_v1.massive_news_archive_page_v1%rowtype;
 first_link population_top3000_202608_202609_v1.massive_news_record_link_v1%rowtype;first_page population_top3000_202608_202609_v1.massive_news_archive_page_v1%rowtype;
 total integer;terminal integer;last_page integer;row_count integer;new_rows integer:=0;changed integer;rec jsonb;rec_hash text;unit text;sourcekey text;
 pub timestamptz;d date;vals jsonb;states jsonb;line jsonb;expect jsonb;denom jsonb;h text;nh text;good_scope boolean;
 raw_text text;doc population_top3000_202608_202609_v1.source_document%rowtype;relations bigint;name text;
 required text[]:=ARRAY['provider_article_id','title','article_url','author','published_utc','publisher','tickers','company_id','security_id','available_at'];
BEGIN
 tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);
 SELECT * INTO t FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=tid;
 IF t.family_id<>'N01' OR t.stage<>'NORMALIZE' OR t.partition_key<>'news-published-202608-202609' THEN RAISE EXCEPTION 'MASSIVE_NEWS_STAGE_MISMATCH';END IF;
 PERFORM population_top3000_202608_202609_v1.massive_news_context_v1(tid);
 PERFORM population_top3000_202608_202609_v1.massive_news_resource_guard_v1();
 k:=population_top3000_202608_202609_v1.massive_news_archive_key_v1(t.family_id,t.partition_key,t.input_versions);
 SELECT count(*),max(page_number) FILTER(WHERE next_url IS NULL) INTO total,terminal FROM population_top3000_202608_202609_v1.massive_news_archive_page_v1 WHERE archive_key=k;
 IF terminal IS NULL OR total<>terminal OR (SELECT count(*) FROM population_top3000_202608_202609_v1.massive_news_archive_page_v1 WHERE archive_key=k AND next_url IS NULL)<>1
 OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.massive_news_archive_page_v1 q LEFT JOIN population_top3000_202608_202609_v1.massive_news_archive_page_v1 prior ON prior.archive_key=q.archive_key AND prior.page_number=q.page_number-1 WHERE q.archive_key=k AND q.page_number>1 AND q.request_url IS DISTINCT FROM prior.next_url)
 THEN RETURN jsonb_build_object('state','BLOCKED','blocker_code','MASSIVE_NEWS_ARCHIVE_PAGE_CHAIN_INCOMPLETE','resumption_condition',jsonb_build_object('event','TERMINAL_PAGE_AND_CONTIGUOUS_SOURCE_CHAIN_VERIFIED','archive_key',k));END IF;
 last_page:=coalesce((t.checkpoint->>'last_normalized_page')::integer,0);
 IF last_page<0 OR last_page>=terminal OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.massive_news_archive_page_v1 WHERE archive_key=k AND page_number<=last_page AND normalized_at IS NULL) THEN RAISE EXCEPTION 'MASSIVE_NEWS_NORMALIZATION_CHECKPOINT_OUTSIDE_ARCHIVE';END IF;
 SELECT * INTO p FROM population_top3000_202608_202609_v1.massive_news_archive_page_v1 WHERE archive_key=k AND page_number=last_page+1 FOR UPDATE;
 IF NOT FOUND THEN RAISE EXCEPTION 'MASSIVE_NEWS_NORMALIZATION_PAGE_MISSING';END IF;
 SELECT * INTO doc FROM population_top3000_202608_202609_v1.source_document WHERE source_id=p.source_id;
 raw_text:=doc.content_text;
 IF NOT FOUND OR raw_text IS NULL OR doc.source_sha256 IS DISTINCT FROM p.source_sha256
 OR doc.source_sha256 IS DISTINCT FROM encode(sha256(convert_to(raw_text,'UTF8')),'hex')
 OR doc.content_bytes IS DISTINCT FROM octet_length(raw_text)::bigint OR doc.source_uri IS DISTINCT FROM p.request_url OR doc.source_provider IS DISTINCT FROM 'Massive'
 OR doc.evidence->>'wire_sha256' IS DISTINCT FROM p.wire_sha256 OR (doc.evidence->>'wire_bytes')::bigint IS DISTINCT FROM p.wire_bytes
 OR doc.retrieved_at IS DISTINCT FROM p.retrieved_at OR doc.historical_received_at IS NOT NULL OR doc.source_published_at IS NOT NULL
 THEN RAISE EXCEPTION 'MASSIVE_NEWS_NORMALIZATION_RETAINED_SOURCE_INTEGRITY';END IF;
 IF jsonb_typeof(raw_text::jsonb->'results') IS DISTINCT FROM 'array' OR (raw_text::jsonb ? 'status' AND raw_text::jsonb->>'status' IS DISTINCT FROM 'OK')
 OR raw_text::jsonb->>'next_url' IS DISTINCT FROM p.next_url THEN RAISE EXCEPTION 'MASSIVE_NEWS_NORMALIZATION_ENVELOPE_CHANGED';END IF;
 nh:=encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.massive_news_normalize_v1(uuid,text)'::regprocedure),'UTF8')),'hex');
 row_count:=0;
 FOR rec IN SELECT v FROM jsonb_array_elements(raw_text::jsonb->'results') v LOOP
  row_count:=row_count+1;
  IF jsonb_typeof(rec) IS DISTINCT FROM 'object' THEN RAISE EXCEPTION 'MASSIVE_NEWS_SOURCE_RECORD_SHAPE';END IF;
  rec_hash:=encode(sha256(convert_to(rec::text,'UTF8')),'hex');
  sourcekey:=CASE WHEN jsonb_typeof(rec->'id')='string' AND octet_length(rec->>'id') BETWEEN 1 AND 512 THEN rec->>'id' ELSE 'unidentified:'||rec_hash END;
  unit:='Massive:news:'||encode(sha256(convert_to(sourcekey,'UTF8')),'hex')||':'||rec_hash;
  INSERT INTO population_top3000_202608_202609_v1.massive_news_record_link_v1(archive_key,page_number,source_row,source_id,source_record_key,raw_record_sha256,unit_key)
  VALUES(k,p.page_number,row_count,p.source_id,sourcekey,rec_hash,unit) ON CONFLICT(archive_key,page_number,source_row) DO NOTHING;
  IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.massive_news_record_link_v1 l WHERE l.archive_key=k AND l.page_number=p.page_number AND l.source_row=row_count AND l.source_id=p.source_id AND l.source_record_key=sourcekey AND l.raw_record_sha256=rec_hash AND l.unit_key=unit) THEN RAISE EXCEPTION 'MASSIVE_NEWS_OCCURRENCE_REVISION_CONFLICT';END IF;
  SELECT * INTO first_link FROM population_top3000_202608_202609_v1.massive_news_record_link_v1 WHERE archive_key=k AND unit_key=unit ORDER BY page_number,source_row LIMIT 1;
  SELECT * INTO first_page FROM population_top3000_202608_202609_v1.massive_news_archive_page_v1 WHERE archive_key=k AND page_number=first_link.page_number;
  pub:=population_top3000_202608_202609_v1.massive_news_timestamp_v1(rec->>'published_utc');
  good_scope:=coalesce(pub>=timestamptz '2026-08-01 04:00:00+00' AND pub<timestamptz '2026-10-01 04:00:00+00',false);
  d:=CASE WHEN pub IS NOT NULL THEN (pub AT TIME ZONE 'America/New_York')::date ELSE NULL END;
  vals:=jsonb_build_object('provider_article_id',rec->'id','title',rec->'title','article_url',rec->'article_url','author',rec->'author','published_utc',rec->'published_utc',
   'publisher',rec->'publisher','tickers',rec->'tickers','description',rec->'description','keywords',rec->'keywords','insights',rec->'insights',
   'company_id',NULL,'security_id',NULL,'available_at',first_page.retrieved_at,'raw_event',rec);
  states:='{}'::jsonb;
  FOREACH name IN ARRAY ARRAY['title','article_url','author','description'] LOOP
   states:=states||jsonb_build_object(name,CASE WHEN NOT rec ? name OR rec->name='null'::jsonb THEN 'UNAVAILABLE' WHEN jsonb_typeof(rec->name)<>'string' OR btrim(rec->>name)='' THEN 'QUARANTINED' WHEN name='article_url' AND (rec->>name !~ '^https?://[^[:space:]]+$' OR rec->>name~'^https?://[^/]*@') THEN 'QUARANTINED' ELSE 'VALUE' END);
  END LOOP;
  states:=states||jsonb_build_object('provider_article_id',CASE WHEN NOT rec ? 'id' OR rec->'id'='null'::jsonb THEN 'UNAVAILABLE' WHEN jsonb_typeof(rec->'id')<>'string' OR octet_length(rec->>'id') NOT BETWEEN 1 AND 512 THEN 'QUARANTINED' ELSE 'VALUE' END,
   'published_utc',CASE WHEN NOT rec ? 'published_utc' OR rec->'published_utc'='null'::jsonb THEN 'UNAVAILABLE' WHEN pub IS NULL OR NOT good_scope THEN 'QUARANTINED' ELSE 'VALUE' END,
   'publisher',CASE WHEN NOT rec ? 'publisher' OR rec->'publisher'='null'::jsonb THEN 'UNAVAILABLE' WHEN jsonb_typeof(rec->'publisher')<>'object' OR jsonb_typeof(rec#>'{publisher,name}') IS DISTINCT FROM 'string' OR btrim(rec#>>'{publisher,name}')='' THEN 'QUARANTINED' ELSE 'VALUE' END,
   'company_id','UNAVAILABLE','security_id','UNAVAILABLE','available_at','VALUE');
  FOREACH name IN ARRAY ARRAY['tickers','keywords','insights'] LOOP
   IF NOT rec ? name OR rec->name='null'::jsonb THEN states:=states||jsonb_build_object(name,'UNAVAILABLE');
   ELSIF jsonb_typeof(rec->name)<>'array' THEN states:=states||jsonb_build_object(name,'QUARANTINED');
   ELSIF name IN('tickers','keywords') AND EXISTS(SELECT 1 FROM jsonb_array_elements(rec->name) x WHERE jsonb_typeof(x)<>'string' OR btrim(x#>>'{}')='') THEN states:=states||jsonb_build_object(name,'QUARANTINED');
   ELSIF name='insights' AND EXISTS(SELECT 1 FROM jsonb_array_elements(rec->name) x WHERE jsonb_typeof(x)<>'object') THEN states:=states||jsonb_build_object(name,'QUARANTINED');
   ELSE states:=states||jsonb_build_object(name,'VALUE');END IF;
  END LOOP;
  line:=jsonb_build_object('source_id',first_link.source_id,'source_record_key',sourcekey,'raw_record_sha256',rec_hash,'archive_key',k,'source_page',first_link.page_number,'source_row',first_link.source_row,
   'all_occurrences_relation','massive_news_record_link_v1','company_membership','UNVERIFIED','source_scope','ALL_RETURNED_PROVIDER_NEWS_FOR_FIXED_PUBLICATION_TIME_QUERY',
   'publication_time_basis','PROVIDER_REPORTED_RFC3339; independent publisher timestamp not verified','timestamp_storage_precision','PostgreSQL microseconds; excess fractional digits truncated before timestamp cast; exact provider timestamp retained in raw_event',
   'historical_receipt','NOT_OBSERVED','availability_basis','CURRENT_API_RETRIEVAL_ONLY; not evidence of historical availability',
   'article_bodies_fetched',false,'insights_basis','OPTIONAL_PROVIDER_METADATA_ONLY; no new research or model sentiment',
   'version_conflicts_relation','massive_news_version_conflict_v1','independent_accuracy_verified',false);
  h:=encode(sha256(convert_to(jsonb_build_object('values',vals,'states',states,'lineage',line)::text,'UTF8')),'hex');
  INSERT INTO population_top3000_202608_202609_v1.source_observation
   (family_id,partition_key,unit_key,time_role,event_at,effective_date,source_published_at,historical_received_at,available_at,availability_policy,vintage_key,source_id,source_record_key,normalization_sha256,values_json,field_state,lineage,record_sha256)
   VALUES('N01',t.partition_key,unit,'TARGET',pub,d,pub,NULL,first_page.retrieved_at,'ARCHIVED_OBSERVATION',k,first_link.source_id,sourcekey,nh,vals,states,line,h)
   ON CONFLICT(release_id,family_id,partition_key,unit_key) DO NOTHING;
  GET DIAGNOSTICS changed=ROW_COUNT;new_rows:=new_rows+changed;
  IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.source_observation o WHERE o.release_id='top3000_companies_202608_202609_v1' AND o.family_id='N01' AND o.partition_key=t.partition_key AND o.unit_key=unit
   AND o.company_id IS NULL AND o.security_id IS NULL AND o.reporting_entity_id IS NULL AND o.time_role='TARGET'
   AND o.event_at IS NOT DISTINCT FROM pub AND o.effective_date IS NOT DISTINCT FROM d AND o.source_published_at IS NOT DISTINCT FROM pub AND o.historical_received_at IS NULL
   AND o.available_at=first_page.retrieved_at AND o.availability_policy='ARCHIVED_OBSERVATION' AND o.vintage_key=k AND o.source_id=first_link.source_id AND o.source_record_key=sourcekey
   AND o.normalization_sha256=nh AND o.values_json=vals AND o.field_state=states AND o.lineage=line AND o.record_sha256=h)
  THEN RAISE EXCEPTION 'MASSIVE_NEWS_NORMALIZATION_EXISTING_OBSERVATION_CONFLICT';END IF;
  expect:=jsonb_build_object('basis','PROVISIONAL_ENUMERATION_OF_RETURNED_SOURCE_RECORD_VERSIONS','independent_news_census',false,'top3000_company_membership_verified',false,'source_record_sha256',rec_hash,'strict_receipt_required',false,'optional_metadata_fields',jsonb_build_array('description','keywords','insights'),'current_receipt_is_not_historical_availability',true);
  INSERT INTO population_top3000_202608_202609_v1.expected_observation
   (family_id,partition_key,unit_key,time_role,required_fields,expectation_source_id,expectation_record_key,applicability_evidence)
   VALUES('N01',t.partition_key,unit,'TARGET',required,first_link.source_id,sourcekey,expect)
   ON CONFLICT(release_id,family_id,partition_key,unit_key) DO NOTHING;
  IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.expected_observation e WHERE e.release_id='top3000_companies_202608_202609_v1' AND e.family_id='N01' AND e.partition_key=t.partition_key AND e.unit_key=unit
   AND e.company_id IS NULL AND e.security_id IS NULL AND e.reporting_entity_id IS NULL AND e.time_role='TARGET' AND e.required_fields=required AND e.expectation_source_id=first_link.source_id AND e.expectation_record_key=sourcekey AND e.applicability_evidence=expect)
  THEN RAISE EXCEPTION 'MASSIVE_NEWS_NORMALIZATION_EXISTING_EXPECTATION_CONFLICT';END IF;
 END LOOP;
 IF row_count<>p.result_count OR (SELECT count(*) FROM population_top3000_202608_202609_v1.massive_news_record_link_v1 WHERE archive_key=k AND page_number=p.page_number)<>row_count THEN RAISE EXCEPTION 'MASSIVE_NEWS_NORMALIZATION_SOURCE_COUNT_MISMATCH';END IF;
 UPDATE population_top3000_202608_202609_v1.massive_news_archive_page_v1 SET normalized_at=coalesce(normalized_at,clock_timestamp()),normalized_source_records=row_count WHERE archive_key=k AND page_number=p.page_number;
 denom:=jsonb_build_object('archive_key',k,'provider_page_chain_terminal',true,'source_pages',total,'independent_company_news_denominator',false,'source_record_versions_only',true,'requires_dated_company_mapping',true,'independent_publication_accuracy_unverified',true,'historical_receipt_not_observed',true);
 INSERT INTO population_top3000_202608_202609_v1.pipeline_partition(family_id,partition_key,time_role,denominator_state,denominator_evidence,producer_revision,immutable_input_versions,publication_eligible)
 VALUES('N01',t.partition_key,'TARGET','PROVISIONAL',denom,'massive-news-20261023-v1',t.input_versions,false) ON CONFLICT(family_id,partition_key) DO NOTHING;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_partition WHERE family_id='N01' AND partition_key=t.partition_key AND time_role='TARGET' AND producer_revision='massive-news-20261023-v1' AND immutable_input_versions=t.input_versions AND denominator_state='PROVISIONAL' AND denominator_evidence=denom AND NOT publication_eligible) THEN RAISE EXCEPTION 'MASSIVE_NEWS_NORMALIZATION_PARTITION_REVISION_CONFLICT';END IF;
 -- Recheck after inserted observations, expectations, links and indexes; failure rolls back this batch.
 relations:=population_top3000_202608_202609_v1.massive_news_resource_guard_v1();
 h:=encode(sha256(convert_to(jsonb_build_object('source_id',p.source_id,'source_records',row_count,'normalizer_sha256',nh)::text,'UTF8')),'hex');
 RETURN jsonb_build_object('state',CASE WHEN p.page_number=terminal THEN 'COMPLETE' ELSE 'CONTINUE' END,'checkpoint',jsonb_build_object('last_normalized_page',p.page_number),
 'commit_receipt',jsonb_build_object('commit_key','massive-news-normalized:'||p.source_id||':'||nh,'output_sha256',h,'new_committed_units',new_rows,'immutable_output_ids',jsonb_build_array(p.source_id),'input_versions',t.input_versions,'source_records',row_count,'mission_relation_bytes_after_batch',relations,'cohort_certified',false));
END $function$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.massive_news_derive_v1(p_attempt uuid,p_token text)
RETURNS jsonb LANGUAGE plpgsql SET search_path TO 'pg_catalog' SET timezone TO 'UTC' AS $function$
DECLARE tid uuid;t population_top3000_202608_202609_v1.pipeline_task%rowtype;k text;h text;n bigint;occurrences bigint;conflicts bigint;versions text[];issue record;e jsonb;
BEGIN
 tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);
 SELECT * INTO t FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=tid;
 IF t.family_id<>'N01' OR t.stage<>'DERIVE' OR t.partition_key<>'news-published-202608-202609' THEN RAISE EXCEPTION 'MASSIVE_NEWS_STAGE_MISMATCH';END IF;
 PERFORM population_top3000_202608_202609_v1.massive_news_context_v1(tid);
 k:=population_top3000_202608_202609_v1.massive_news_archive_key_v1(t.family_id,t.partition_key,t.input_versions);
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.massive_news_archive_page_v1 WHERE archive_key=k AND next_url IS NULL)
 OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.massive_news_archive_page_v1 WHERE archive_key=k AND (normalized_at IS NULL OR normalized_source_records IS DISTINCT FROM result_count)) THEN RAISE EXCEPTION 'MASSIVE_NEWS_NORMALIZATION_INCOMPLETE';END IF;
 FOR issue IN SELECT source_record_key,array_agg(DISTINCT raw_record_sha256 ORDER BY raw_record_sha256) hashes FROM population_top3000_202608_202609_v1.massive_news_record_link_v1 WHERE archive_key=k GROUP BY source_record_key HAVING count(DISTINCT raw_record_sha256)>1 LOOP
  versions:=issue.hashes;e:=jsonb_build_object('status','UNRESOLVED_PROVIDER_ID_CONTENT_VERSIONS','versions_retained',true,'canonical_article_version_chosen',false,'independent_accuracy_verified',false);
  h:=encode(sha256(convert_to(jsonb_build_object('archive_key',k,'source_record_key',issue.source_record_key,'versions',versions,'evidence',e)::text,'UTF8')),'hex');
  INSERT INTO population_top3000_202608_202609_v1.massive_news_version_conflict_v1(archive_key,source_record_key,version_count,version_sha256,evidence,record_sha256)
  VALUES(k,issue.source_record_key,cardinality(versions),versions,e,h) ON CONFLICT(archive_key,source_record_key) DO NOTHING;
  IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.massive_news_version_conflict_v1 WHERE archive_key=k AND source_record_key=issue.source_record_key AND version_count=cardinality(versions) AND version_sha256=versions AND evidence=e AND record_sha256=h) THEN RAISE EXCEPTION 'MASSIVE_NEWS_VERSION_CONFLICT_RECEIPT_CHANGED';END IF;
 END LOOP;
 SELECT count(*) INTO conflicts FROM population_top3000_202608_202609_v1.massive_news_version_conflict_v1 WHERE archive_key=k;
 SELECT count(*) INTO occurrences FROM population_top3000_202608_202609_v1.massive_news_record_link_v1 WHERE archive_key=k;
 SELECT count(*),encode(sha256(convert_to(coalesce(string_agg(unit_key||':'||record_sha256,E'\n' ORDER BY unit_key),''),'UTF8')),'hex') INTO n,h FROM population_top3000_202608_202609_v1.source_observation WHERE family_id='N01' AND partition_key=t.partition_key AND vintage_key=k;
 IF n IS DISTINCT FROM (SELECT count(DISTINCT unit_key) FROM population_top3000_202608_202609_v1.massive_news_record_link_v1 WHERE archive_key=k) OR occurrences IS DISTINCT FROM (SELECT sum(result_count) FROM population_top3000_202608_202609_v1.massive_news_archive_page_v1 WHERE archive_key=k) THEN RAISE EXCEPTION 'MASSIVE_NEWS_DERIVED_SOURCE_RECONCILIATION_FAILED';END IF;
 PERFORM population_top3000_202608_202609_v1.massive_news_resource_guard_v1();
 RETURN jsonb_build_object('state','COMPLETE','commit_receipt',jsonb_build_object('commit_key','massive-news-derived:'||k||':'||h,'output_sha256',h,'new_committed_units',n,'immutable_output_ids',jsonb_build_array(k),'input_versions',t.input_versions,'raw_source_occurrences',occurrences,'distinct_content_versions',n,'duplicate_occurrences',occurrences-n,'unresolved_provider_id_version_conflicts',conflicts,'cohort_certified',false,'historical_availability_certified',false,'article_bodies_fetched',false,'independent_accuracy_verified',false));
END $function$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.massive_news_code_hash_v1()
RETURNS text LANGUAGE sql STABLE SET search_path TO 'pg_catalog' SET timezone TO 'UTC' AS $function$
 SELECT encode(sha256(convert_to(string_agg(fn||':'||pg_get_functiondef(('population_top3000_202608_202609_v1.'||fn)::regprocedure),E'\n' ORDER BY fn),'UTF8')),'hex')
 FROM (VALUES ('massive_news_archive_key_v1(text,text,jsonb)'),('massive_news_context_v1(uuid)'),('massive_news_acquire_v1(uuid,text)'),('massive_news_claim_v1(uuid,text)'),('massive_news_commit_v1(uuid,text,jsonb)'),('massive_news_failure_v1(uuid,text,jsonb)'),('massive_news_timestamp_v1(text)'),('massive_news_resource_guard_v1()'),('massive_news_normalize_v1(uuid,text)'),('massive_news_derive_v1(uuid,text)')) v(fn);
$function$;
REVOKE ALL ON FUNCTION population_top3000_202608_202609_v1.massive_news_archive_key_v1(text,text,jsonb),population_top3000_202608_202609_v1.massive_news_context_v1(uuid),population_top3000_202608_202609_v1.massive_news_acquire_v1(uuid,text),population_top3000_202608_202609_v1.massive_news_claim_v1(uuid,text),population_top3000_202608_202609_v1.massive_news_commit_v1(uuid,text,jsonb),population_top3000_202608_202609_v1.massive_news_failure_v1(uuid,text,jsonb),population_top3000_202608_202609_v1.massive_news_timestamp_v1(text),population_top3000_202608_202609_v1.massive_news_resource_guard_v1(),population_top3000_202608_202609_v1.massive_news_normalize_v1(uuid,text),population_top3000_202608_202609_v1.massive_news_derive_v1(uuid,text),population_top3000_202608_202609_v1.massive_news_code_hash_v1() FROM PUBLIC,anon,authenticated;
GRANT EXECUTE ON FUNCTION population_top3000_202608_202609_v1.massive_news_archive_key_v1(text,text,jsonb),population_top3000_202608_202609_v1.massive_news_context_v1(uuid),population_top3000_202608_202609_v1.massive_news_acquire_v1(uuid,text),population_top3000_202608_202609_v1.massive_news_claim_v1(uuid,text),population_top3000_202608_202609_v1.massive_news_commit_v1(uuid,text,jsonb),population_top3000_202608_202609_v1.massive_news_failure_v1(uuid,text,jsonb),population_top3000_202608_202609_v1.massive_news_timestamp_v1(text),population_top3000_202608_202609_v1.massive_news_resource_guard_v1(),population_top3000_202608_202609_v1.massive_news_normalize_v1(uuid,text),population_top3000_202608_202609_v1.massive_news_derive_v1(uuid,text),population_top3000_202608_202609_v1.massive_news_code_hash_v1() TO postgres,service_role;
