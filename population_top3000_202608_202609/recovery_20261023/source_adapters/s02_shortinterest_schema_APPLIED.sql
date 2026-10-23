begin;set local statement_timeout='10s';set local lock_timeout='500ms';select pg_advisory_xact_lock(2026083000261022);do $g$ begin if exists(select 1 from cron.job where jobid=596 and active) or exists(select 1 from population_top3000_202608_202609_v1.pipeline_task where status='LEASED') or not exists(select 1 from population_top3000_202608_202609_v1.acquisition_control where singleton and owner_id='top3000-recovery-20261023-root-4b1314bcb5c2' and owner_fence=2 and stop_generation=0 and enabled and not stopped and admitted_until>clock_timestamp()) then raise exception 'PAUSED_ROOT_REQUIRED';end if;end $g$;
-- Root-only reviewed installation draft. Does not enable, seed or dispatch any worker.
-- S02 only: bounded fixed-date archive; no company mapping or certification.
CREATE TABLE IF NOT EXISTS population_top3000_202608_202609_v1.massive_shortinterest_page_v1 (
 archive_key text NOT NULL,
 family_id text NOT NULL CHECK(family_id='S02'),
 partition_key text NOT NULL CHECK(partition_key='short-interest-settlement-20260715-20260930'),
 page_number integer NOT NULL CHECK(page_number BETWEEN 1 AND 1000),
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
ALTER TABLE population_top3000_202608_202609_v1.massive_shortinterest_page_v1 ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON population_top3000_202608_202609_v1.massive_shortinterest_page_v1 FROM PUBLIC,anon,authenticated;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.massive_shortinterest_archive_key_v1(p_family text,p_partition text,p_versions jsonb)
RETURNS text LANGUAGE sql IMMUTABLE SET search_path TO 'pg_catalog' AS $function$
 SELECT 'massive-archive:'||p_family||':'||encode(sha256(convert_to(jsonb_build_object('family',p_family,'partition',p_partition,'input_versions',p_versions)::text,'UTF8')),'hex');
$function$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.massive_shortinterest_context_v1(p_task uuid)
RETURNS jsonb LANGUAGE plpgsql SET search_path TO 'pg_catalog' AS $function$
DECLARE t population_top3000_202608_202609_v1.pipeline_task%rowtype;k text;n integer;lim integer;url text;prior population_top3000_202608_202609_v1.massive_shortinterest_page_v1%rowtype;
BEGIN
 SELECT * INTO t FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=p_task;
 IF NOT FOUND OR t.family_id<>'S02' OR t.partition_key<>'short-interest-settlement-20260715-20260930'
 OR t.input_versions->>'scope_sha256' IS DISTINCT FROM '8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd'
 OR t.input_versions->>'archive_contract_revision' IS DISTINCT FROM 'massive-shortinterest-20261023-v1'
 OR t.input_versions->>'shortinterest_bundle_sha256' IS DISTINCT FROM population_top3000_202608_202609_v1.massive_shortinterest_bundle_sha256_v1()
 THEN RAISE EXCEPTION 'MASSIVE_SHORTINTEREST_SCOPE_NOT_ADMITTED';END IF;
 k:=population_top3000_202608_202609_v1.massive_shortinterest_archive_key_v1(t.family_id,t.partition_key,t.input_versions);
 IF coalesce(t.checkpoint->>'next_page','1') !~ '^[0-9]{1,4}$' OR coalesce(t.payload->>'max_pages','1') !~ '^[0-9]{1,4}$' THEN RAISE EXCEPTION 'MASSIVE_SHORTINTEREST_PAGE_LIMIT_INVALID';END IF;
 n:=coalesce(t.checkpoint->>'next_page','1')::integer;lim:=coalesce(t.payload->>'max_pages','1')::integer;
 IF n<1 OR n>1000 OR lim<1 OR lim>1000 THEN RAISE EXCEPTION 'MASSIVE_SHORTINTEREST_PAGE_LIMIT_INVALID';END IF;
 IF n=1 THEN
  url:='https://api.massive.com/stocks/v1/short-interest?settlement_date.gte=2026-07-15&settlement_date.lte=2026-09-30&sort=settlement_date.asc,ticker.asc&limit=1000';
 ELSE
  SELECT * INTO prior FROM population_top3000_202608_202609_v1.massive_shortinterest_page_v1 WHERE archive_key=k AND page_number=n-1;
  IF NOT FOUND OR prior.next_url IS NULL OR t.checkpoint->>'last_source_id' IS DISTINCT FROM prior.source_id
  OR (SELECT source_sha256 FROM population_top3000_202608_202609_v1.source_document WHERE source_id=prior.source_id) IS DISTINCT FROM t.checkpoint->>'last_source_sha256'
  THEN RAISE EXCEPTION 'MASSIVE_SHORTINTEREST_CONTINUATION_EVIDENCE_MISSING';END IF;
  url:=prior.next_url;
  IF url !~ '^https://api[.]massive[.]com/stocks/v1/short-interest[?]cursor=[A-Za-z0-9_.~%+\-]+$' THEN RAISE EXCEPTION 'MASSIVE_SHORTINTEREST_UNSAFE_CURSOR';END IF;
 END IF;
 RETURN jsonb_build_object('state',CASE WHEN n>lim THEN 'PAGE_LIMIT_REVIEW' ELSE 'READY' END,'archive_key',k,'page_number',n,'max_pages',lim,
 'family_id',t.family_id,'partition_key',t.partition_key,'request_url',url,'max_response_bytes',8388608,'max_runtime_ms',45000);
END $function$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.massive_shortinterest_acquire_v1(p_attempt uuid,p_token text)
RETURNS jsonb LANGUAGE plpgsql SET search_path TO 'pg_catalog' AS $function$
DECLARE tid uuid;c jsonb;
BEGIN
 tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=tid AND stage='ACQUIRE') THEN RAISE EXCEPTION 'MASSIVE_SHORTINTEREST_STAGE_MISMATCH';END IF;
 c:=population_top3000_202608_202609_v1.massive_shortinterest_context_v1(tid);
 IF c->>'state'='PAGE_LIMIT_REVIEW' THEN RETURN jsonb_build_object('state','BLOCKED','blocker_code','S02_PAGE_CAP_REVIEW','resumption_condition',jsonb_build_object('event','RETAINED_PAGE_REVIEW_AND_BOUNDED_CAP_ADMISSION','next_page',c->'page_number','existing_pages_preserved',true));END IF;
 RETURN population_top3000_202608_202609_v1.pipeline_dispatch_edge_v1(p_attempt,p_token,'population-top3000-shortinterest-v1');
END $function$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.massive_shortinterest_claim_v1(p_attempt uuid,p_token text)
RETURNS jsonb LANGUAGE plpgsql SET search_path TO 'pg_catalog' AS $function$
DECLARE tid uuid;c jsonb;r jsonb;
BEGIN
 tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=tid AND stage='ACQUIRE') THEN RAISE EXCEPTION 'MASSIVE_SHORTINTEREST_STAGE_MISMATCH';END IF;
 c:=population_top3000_202608_202609_v1.massive_shortinterest_context_v1(tid);
 IF c->>'state'<>'READY' THEN
  RETURN population_top3000_202608_202609_v1.pipeline_finish_v1(p_attempt,p_token,jsonb_build_object('state','BLOCKED','blocker_code','S02_PAGE_CAP_REVIEW','resumption_condition',jsonb_build_object('event','RETAINED_PAGE_REVIEW_AND_BOUNDED_CAP_ADMISSION','next_page',c->'page_number','existing_pages_preserved',true)));
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

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.massive_shortinterest_resource_guard_v1()
RETURNS bigint LANGUAGE plpgsql SET search_path TO 'pg_catalog' AS $function$
DECLARE admitted_bound bigint;actual_bytes bigint;
BEGIN
 SELECT (budget_evidence#>>'{resource_limits,max_relation_bytes}')::bigint INTO admitted_bound
 FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton;
 IF admitted_bound IS NULL OR admitted_bound<1 THEN RAISE EXCEPTION 'S02_RESOURCE_BOUND_NOT_ADMITTED';END IF;
 SELECT coalesce(sum(pg_total_relation_size(c.oid)),0) INTO actual_bytes
 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
 WHERE n.nspname='population_top3000_202608_202609_v1' AND c.relkind IN('r','m');
 IF actual_bytes>admitted_bound THEN RAISE EXCEPTION 'S02_POST_BATCH_RELATION_CAP_EXCEEDED';END IF;
 RETURN actual_bytes;
END $function$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.massive_shortinterest_commit_v1(p_attempt uuid,p_token text,p_page jsonb)
RETURNS jsonb LANGUAGE plpgsql SET search_path TO 'pg_catalog' AS $function$
DECLARE tid uuid;t population_top3000_202608_202609_v1.pipeline_task%rowtype;c jsonb;body jsonb;txt text;h text;wire text;
 sid text;k text;n integer;cnt integer;bytes bigint;stored_bytes bigint;v_next_url text;at_time timestamptz;fresh integer;receipt jsonb;source_evidence jsonb;
BEGIN
 tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);
 SELECT * INTO t FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=tid;
 IF t.stage<>'ACQUIRE' THEN RAISE EXCEPTION 'MASSIVE_SHORTINTEREST_STAGE_MISMATCH';END IF;
 c:=population_top3000_202608_202609_v1.massive_shortinterest_context_v1(tid);
 PERFORM population_top3000_202608_202609_v1.massive_shortinterest_resource_guard_v1();
 IF c->>'state'<>'READY' OR jsonb_typeof(p_page) IS DISTINCT FROM 'object' OR p_page->>'archive_key' IS DISTINCT FROM c->>'archive_key'
 OR p_page->>'page_number' IS DISTINCT FROM c->>'page_number' OR p_page->>'request_url' IS DISTINCT FROM c->>'request_url'
 OR p_page->>'http_status' IS DISTINCT FROM '200' THEN RAISE EXCEPTION 'MASSIVE_SHORTINTEREST_RECEIPT_SCOPE_MISMATCH';END IF;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_provider_lease WHERE attempt_id=p_attempt AND provider='Massive' AND released_at IS NULL AND reserved_bytes=8388608) THEN RAISE EXCEPTION 'MASSIVE_SHORTINTEREST_RESERVATION_REQUIRED';END IF;
 txt:=p_page->>'content_text';stored_bytes:=octet_length(txt);bytes:=(p_page->>'wire_bytes')::bigint;wire:=p_page->>'wire_sha256';
 IF txt IS NULL OR stored_bytes>8388608 OR bytes IS NULL OR bytes<1 OR bytes>8388608 OR wire IS NULL OR wire!~'^[0-9a-f]{64}$'
 OR coalesce((p_page->>'elapsed_ms')::bigint,-1) NOT BETWEEN 0 AND 45000 THEN RAISE EXCEPTION 'MASSIVE_SHORTINTEREST_SOURCE_BOUND_INVALID';END IF;
 h:=encode(sha256(convert_to(txt,'UTF8')),'hex');
 IF h IS DISTINCT FROM p_page->>'source_sha256' THEN RAISE EXCEPTION 'MASSIVE_SHORTINTEREST_STORED_HASH_MISMATCH';END IF;
 body:=txt::jsonb;
 IF body->>'status' IS DISTINCT FROM 'OK' OR jsonb_typeof(body->'results') IS DISTINCT FROM 'array' THEN RAISE EXCEPTION 'MASSIVE_SHORTINTEREST_INVALID_SOURCE_ENVELOPE';END IF;
 cnt:=jsonb_array_length(body->'results');
 IF cnt>1000 OR cnt IS DISTINCT FROM (p_page->>'result_count')::integer OR EXISTS(SELECT 1 FROM jsonb_array_elements(body->'results') v WHERE jsonb_typeof(v) IS DISTINCT FROM 'object') THEN RAISE EXCEPTION 'MASSIVE_SHORTINTEREST_SOURCE_RECORD_SHAPE';END IF;
 v_next_url:=p_page->>'next_url';
 IF v_next_url IS DISTINCT FROM body->>'next_url' OR (v_next_url IS NOT NULL AND (v_next_url!~'^https://api[.]massive[.]com/stocks/v1/short-interest[?]cursor=[A-Za-z0-9_.~%+\-]+$' OR length(v_next_url)>16384 OR v_next_url=c->>'request_url')) THEN RAISE EXCEPTION 'MASSIVE_SHORTINTEREST_CURSOR_MISMATCH';END IF;
 IF txt~* '"(apiKey|api_key|access_token|authorization)"[[:space:]]*:' OR txt~* '[?&](apiKey|api_key|access_token)=' THEN RAISE EXCEPTION 'MASSIVE_SHORTINTEREST_UNREDACTED_CREDENTIAL_FIELD';END IF;
 k:=c->>'archive_key';n:=(c->>'page_number')::integer;sid:=k||':page:'||n::text;at_time:=(p_page->>'retrieved_at')::timestamptz;
 IF at_time IS NULL OR at_time<(SELECT started_at FROM population_top3000_202608_202609_v1.pipeline_attempt WHERE attempt_id=p_attempt)-interval '30 seconds' OR at_time>clock_timestamp()+interval '30 seconds' THEN RAISE EXCEPTION 'MASSIVE_SHORTINTEREST_RETRIEVAL_CLOCK_INVALID';END IF;
 IF v_next_url IS NOT NULL AND EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.massive_shortinterest_page_v1 WHERE archive_key=k AND request_url=v_next_url) THEN RAISE EXCEPTION 'MASSIVE_SHORTINTEREST_CURSOR_CYCLE';END IF;
 source_evidence:=jsonb_build_object('wire_sha256',wire,'wire_bytes',bytes,'stored_content_bytes',stored_bytes,'hash_representation','EXACT_UTF8_RESPONSE_WITH_TOP_LEVEL_NEXT_URL_CREDENTIAL_REDACTION_ONLY','wire_bytes_retained',false,'provider_request_id',p_page->>'provider_request_id','scope','S02 settlement dates 2026-07-15 through 2026-09-30; July support and Aug/Sep target; publication schedule retained separately','original_historical_receipt',false);
 INSERT INTO population_top3000_202608_202609_v1.source_document
 (source_id,source_sha256,source_uri,source_provider,retrieved_at,source_published_at,historical_received_at,content_text,content_bytes,evidence,license_policy)
 VALUES(sid,h,c->>'request_url','Massive',at_time,NULL,NULL,txt,stored_bytes,
 source_evidence,
 'EXISTING_MASSIVE_SUBSCRIPTION_API_RESPONSE_INTERNAL_USE') ON CONFLICT(source_id) DO NOTHING;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.source_document d WHERE d.source_id=sid AND d.source_sha256=h AND d.source_uri=c->>'request_url' AND d.content_text=txt AND d.content_bytes=stored_bytes AND d.source_provider='Massive' AND d.retrieved_at=at_time AND d.source_published_at IS NULL AND d.historical_received_at IS NULL AND d.evidence=source_evidence AND d.license_policy='EXISTING_MASSIVE_SUBSCRIPTION_API_RESPONSE_INTERNAL_USE') THEN RAISE EXCEPTION 'MASSIVE_SHORTINTEREST_SOURCE_REVISION_CONFLICT';END IF;
 INSERT INTO population_top3000_202608_202609_v1.massive_shortinterest_page_v1
 (archive_key,family_id,partition_key,page_number,acquire_task_id,attempt_id,source_id,source_sha256,request_url,next_url,wire_sha256,wire_bytes,result_count,retrieved_at)
 VALUES(k,t.family_id,t.partition_key,n,tid,p_attempt,sid,h,c->>'request_url',v_next_url,wire,bytes,cnt,at_time)
 ON CONFLICT(archive_key,page_number) DO NOTHING;
 GET DIAGNOSTICS fresh=ROW_COUNT;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.massive_shortinterest_page_v1 q WHERE q.archive_key=k AND q.page_number=n AND q.source_id=sid AND q.source_sha256=h AND q.wire_sha256=wire AND q.wire_bytes=bytes AND q.request_url=c->>'request_url' AND q.next_url IS NOT DISTINCT FROM v_next_url AND q.result_count=cnt AND q.family_id=t.family_id AND q.partition_key=t.partition_key AND q.acquire_task_id=tid AND q.attempt_id=p_attempt AND q.retrieved_at=at_time) THEN RAISE EXCEPTION 'MASSIVE_SHORTINTEREST_PAGE_REVISION_CONFLICT';END IF;
 PERFORM population_top3000_202608_202609_v1.massive_shortinterest_resource_guard_v1();
 PERFORM population_top3000_202608_202609_v1.pipeline_provider_release_v1(p_attempt,p_token,stored_bytes,0,0);
 receipt:=jsonb_build_object('state',CASE WHEN v_next_url IS NULL THEN 'COMPLETE' ELSE 'CONTINUE' END,
 'checkpoint',jsonb_build_object('next_page',n+1,'last_source_id',sid,'last_source_sha256',h,'next_url',v_next_url,'source_records_seen',coalesce((t.checkpoint->>'source_records_seen')::bigint,0)+cnt),
 'not_before',(SELECT next_provider_at FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton),
 'commit_receipt',jsonb_build_object('commit_key','massive-page:'||sid||':'||h,'output_sha256',h,'new_committed_units',fresh,'immutable_output_ids',jsonb_build_array(sid),'input_versions',t.input_versions,'raw_source_records',cnt,'terminal_page',v_next_url IS NULL,'cohort_certified',false));
 RETURN population_top3000_202608_202609_v1.pipeline_finish_v1(p_attempt,p_token,receipt);
END $function$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.massive_shortinterest_failure_v1(p_attempt uuid,p_token text,p_failure jsonb)
RETURNS jsonb LANGUAGE plpgsql SET search_path TO 'pg_catalog' AS $function$
DECLARE tid uuid;code text;class text;bytes bigint;retry integer;unknown_effect boolean;details jsonb;
BEGIN
 tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=tid AND family_id='S02' AND stage='ACQUIRE') THEN RAISE EXCEPTION 'MASSIVE_SHORTINTEREST_STAGE_MISMATCH';END IF;
 PERFORM population_top3000_202608_202609_v1.massive_shortinterest_context_v1(tid);
 code:=p_failure->>'code';class:=p_failure->>'failure_class';bytes:=(p_failure->>'received_bytes')::bigint;retry:=(p_failure->>'retry_after_seconds')::integer;
 IF code IS NULL OR code!~'^[A-Z0-9_]{1,80}$' OR class IS NULL OR class NOT IN('RATE_LIMIT','TRANSPORT_READ','AUTHENTICATION','PARSER','SOURCE_ABSENT') OR bytes IS NULL OR bytes NOT BETWEEN 0 AND 8388608 OR retry IS NULL OR retry NOT BETWEEN 0 AND 3600 THEN RAISE EXCEPTION 'MASSIVE_SHORTINTEREST_FAILURE_RECEIPT_INVALID';END IF;
 details:=jsonb_build_object('code',code,'http_status',p_failure->'http_status','response_bytes_observed',bytes,'wire_sha256',p_failure->>'wire_sha256','fetch_started',p_failure->'fetch_started','raw_body_retained',false,'automatic_retry',false);
 unknown_effect:=coalesce((p_failure->>'fetch_started')::boolean,false)
  AND (p_failure->>'http_status' IS NULL OR code IN('DEADLINE','FETCH_FAILED','EMPTY_BODY') OR (p_failure->>'wire_sha256' IS NULL AND p_failure->>'http_status'='200'));
 IF unknown_effect THEN
  -- Retain the reservation. The existing expiry reconciler conservatively accounts it.
  -- No new request is admitted until the controller reconciles the uncertain effect.
  RETURN population_top3000_202608_202609_v1.pipeline_fail_v1(p_attempt,p_token,'UNCERTAIN_EFFECT','MASSIVE_SHORTINTEREST:'||code,details,retry);
 END IF;
 -- This counter measures retained source bytes. An unretained error body adds zero.
 PERFORM population_top3000_202608_202609_v1.pipeline_provider_release_v1(p_attempt,p_token,0,0,CASE WHEN class='RATE_LIMIT' THEN retry ELSE 0 END);
 RETURN population_top3000_202608_202609_v1.pipeline_finish_v1(p_attempt,p_token,jsonb_build_object('state','BLOCKED','blocker_code','MASSIVE_SHORTINTEREST_'||code,
 'resumption_condition',jsonb_build_object('event','CONTROLLER_REVIEW_OF_DEFINITIVE_SOURCE_FAILURE','details',details,'retry_after_seconds',retry,'preserve_counters_and_cursor',true)));
END $function$;

CREATE TABLE IF NOT EXISTS population_top3000_202608_202609_v1.massive_shortinterest_record_v1 (
 archive_key text NOT NULL,
 page_number integer NOT NULL,
 source_row integer NOT NULL CHECK(source_row BETWEEN 1 AND 1000),
 source_id text NOT NULL REFERENCES population_top3000_202608_202609_v1.source_document(source_id),
 source_record_key text NOT NULL,
 unit_key text NOT NULL,
 raw_record jsonb NOT NULL CHECK(jsonb_typeof(raw_record)='object'),
 raw_record_sha256 text NOT NULL CHECK(raw_record_sha256=encode(sha256(convert_to(raw_record::text,'UTF8')),'hex')),
 PRIMARY KEY(archive_key,page_number,source_row),
 FOREIGN KEY(archive_key,page_number) REFERENCES population_top3000_202608_202609_v1.massive_shortinterest_page_v1(archive_key,page_number)
);
CREATE INDEX IF NOT EXISTS massive_shortinterest_record_unit_v1 ON population_top3000_202608_202609_v1.massive_shortinterest_record_v1(archive_key,unit_key,page_number,source_row);
CREATE INDEX IF NOT EXISTS massive_shortinterest_record_business_key_v1 ON population_top3000_202608_202609_v1.massive_shortinterest_record_v1(archive_key,source_record_key,raw_record_sha256);
ALTER TABLE population_top3000_202608_202609_v1.massive_shortinterest_record_v1 ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON population_top3000_202608_202609_v1.massive_shortinterest_record_v1 FROM PUBLIC,anon,authenticated;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.massive_shortinterest_date_v1(p_value text)
RETURNS date LANGUAGE plpgsql IMMUTABLE SET search_path TO 'pg_catalog' AS $function$
DECLARE d date;
BEGIN
 IF p_value IS NULL OR p_value!~'^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN RETURN NULL;END IF;
 BEGIN d:=p_value::date;EXCEPTION WHEN OTHERS THEN RETURN NULL;END;
 IF to_char(d,'YYYY-MM-DD')<>p_value THEN RETURN NULL;END IF;
 RETURN d;
END $function$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.massive_shortinterest_numeric_v1(p_value jsonb)
RETURNS numeric LANGUAGE plpgsql IMMUTABLE SET search_path TO 'pg_catalog' AS $function$
BEGIN
 IF p_value IS NULL OR jsonb_typeof(p_value)<>'number' OR length(p_value::text)>96 THEN RETURN NULL;END IF;
 RETURN p_value::text::numeric;
END $function$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.massive_shortinterest_calendar_v1()
RETURNS TABLE(settlement_date date,scheduled_publication_date date,time_role text)
LANGUAGE sql IMMUTABLE SET search_path TO 'pg_catalog' AS $function$
 -- Controller-verified extract from the official FINRA reporting calendar.
 -- These are scheduled publication dates, not observed timestamps or historical receipts.
 SELECT * FROM (VALUES
 (date '2026-07-15',date '2026-07-24','SUPPORT'::text),
 (date '2026-07-31',date '2026-08-11','SUPPORT'::text),
 (date '2026-08-14',date '2026-08-25','TARGET'::text),
 (date '2026-08-31',date '2026-09-10','TARGET'::text),
 (date '2026-09-15',date '2026-09-24','TARGET'::text),
 (date '2026-09-30',date '2026-10-09','TARGET'::text)
 ) x(settlement_date,scheduled_publication_date,time_role);
$function$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.massive_shortinterest_normalize_v1(p_attempt uuid,p_token text)
RETURNS jsonb LANGUAGE plpgsql SET search_path TO 'pg_catalog' AS $function$
DECLARE tid uuid;t population_top3000_202608_202609_v1.pipeline_task%rowtype;c jsonb;k text;
 p population_top3000_202608_202609_v1.massive_shortinterest_page_v1%rowtype;rr record;rec jsonb;raw_text text;doc_hash text;doc_bytes bigint;doc_uri text;doc_evidence jsonb;doc population_top3000_202608_202609_v1.source_document%rowtype;terminal integer;count_pages bigint;terminal_count bigint;
 ticker text;settled date;sourcekey text;unit text;rh text;nh text;h text;new_rows integer:=0;changed integer;
BEGIN
 tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);
 SELECT * INTO t FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=tid;
 IF t.family_id<>'S02' OR t.stage<>'NORMALIZE' THEN RAISE EXCEPTION 'S02_STAGE_MISMATCH';END IF;
 c:=population_top3000_202608_202609_v1.massive_shortinterest_context_v1(tid);k:=c->>'archive_key';
 PERFORM population_top3000_202608_202609_v1.massive_shortinterest_resource_guard_v1();
 SELECT count(*),max(page_number) FILTER(WHERE next_url IS NULL),count(*) FILTER(WHERE next_url IS NULL) INTO count_pages,terminal,terminal_count
 FROM population_top3000_202608_202609_v1.massive_shortinterest_page_v1 WHERE archive_key=k;
 IF terminal IS NULL OR terminal_count<>1 OR count_pages<>terminal THEN RETURN jsonb_build_object('state','BLOCKED','blocker_code','S02_PAGE_CHAIN_INCOMPLETE','resumption_condition',jsonb_build_object('event','ALL_ADMITTED_SOURCE_PAGES_RETAINED_THROUGH_TERMINAL_CURSOR','archive_key',k,'company_census_not_established',true));END IF;
 SELECT * INTO p FROM population_top3000_202608_202609_v1.massive_shortinterest_page_v1 WHERE archive_key=k AND page_number=coalesce((t.checkpoint->>'last_normalized_page')::integer,0)+1 FOR UPDATE;
 IF NOT FOUND OR (p.normalized_at IS NOT NULL AND p.normalized_source_records IS DISTINCT FROM p.result_count) THEN RAISE EXCEPTION 'S02_NORMALIZATION_CHECKPOINT_MISMATCH';END IF;
 SELECT * INTO doc FROM population_top3000_202608_202609_v1.source_document WHERE source_id=p.source_id;
 IF NOT FOUND THEN RAISE EXCEPTION 'S02_RETAINED_SOURCE_INTEGRITY';END IF;
 raw_text:=doc.content_text;doc_hash:=doc.source_sha256;doc_bytes:=doc.content_bytes;doc_uri:=doc.source_uri;doc_evidence:=doc.evidence;
 IF doc.source_provider IS DISTINCT FROM 'Massive' OR doc.retrieved_at IS DISTINCT FROM p.retrieved_at OR doc.source_published_at IS NOT NULL OR doc.historical_received_at IS NOT NULL OR doc.license_policy IS DISTINCT FROM 'EXISTING_MASSIVE_SUBSCRIPTION_API_RESPONSE_INTERNAL_USE' OR raw_text IS NULL OR doc_hash IS DISTINCT FROM p.source_sha256 OR doc_hash IS DISTINCT FROM encode(sha256(convert_to(raw_text,'UTF8')),'hex') OR doc_bytes IS DISTINCT FROM octet_length(raw_text)::bigint OR doc_uri IS DISTINCT FROM p.request_url OR doc_evidence->>'wire_sha256' IS DISTINCT FROM p.wire_sha256 OR (doc_evidence->>'wire_bytes')::bigint IS DISTINCT FROM p.wire_bytes OR (doc_evidence->>'stored_content_bytes')::bigint IS DISTINCT FROM doc_bytes OR raw_text::jsonb->>'status' IS DISTINCT FROM 'OK' OR jsonb_typeof(raw_text::jsonb->'results') IS DISTINCT FROM 'array' OR jsonb_array_length(raw_text::jsonb->'results') IS DISTINCT FROM p.result_count OR raw_text::jsonb->>'next_url' IS DISTINCT FROM p.next_url THEN RAISE EXCEPTION 'S02_RETAINED_SOURCE_INTEGRITY';END IF;
 nh:=encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.massive_shortinterest_normalize_v1(uuid,text)'::regprocedure),'UTF8')),'hex');
 FOR rr IN SELECT value,ordinality::integer source_row FROM jsonb_array_elements(raw_text::jsonb->'results') WITH ORDINALITY x(value,ordinality) ORDER BY ordinality LOOP
  rec:=rr.value;rh:=encode(sha256(convert_to(rec::text,'UTF8')),'hex');
  ticker:=CASE WHEN jsonb_typeof(rec->'ticker')='string' AND length(rec->>'ticker') BETWEEN 1 AND 64 AND rec->>'ticker'!~'[[:cntrl:][:space:]]' THEN rec->>'ticker' ELSE NULL END;
  settled:=population_top3000_202608_202609_v1.massive_shortinterest_date_v1(rec->>'settlement_date');
  sourcekey:=CASE WHEN ticker IS NOT NULL AND settled IS NOT NULL THEN 'MassiveSI:business:'||encode(sha256(convert_to(jsonb_build_array(ticker,settled)::text,'UTF8')),'hex') ELSE 'MassiveSI:unidentified:'||rh END;
  unit:='MassiveSI:record:'||rh;
  INSERT INTO population_top3000_202608_202609_v1.massive_shortinterest_record_v1(archive_key,page_number,source_row,source_id,source_record_key,unit_key,raw_record,raw_record_sha256)
  VALUES(k,p.page_number,rr.source_row,p.source_id,sourcekey,unit,rec,rh) ON CONFLICT(archive_key,page_number,source_row) DO NOTHING;
  GET DIAGNOSTICS changed=ROW_COUNT;new_rows:=new_rows+changed;
  IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.massive_shortinterest_record_v1 q WHERE q.archive_key=k AND q.page_number=p.page_number AND q.source_row=rr.source_row AND q.source_id=p.source_id AND q.source_record_key=sourcekey AND q.unit_key=unit AND q.raw_record=rec AND q.raw_record_sha256=rh) THEN RAISE EXCEPTION 'S02_RAW_OCCURRENCE_REPLAY_CONFLICT';END IF;
 END LOOP;
 IF (SELECT count(*) FROM population_top3000_202608_202609_v1.massive_shortinterest_record_v1 WHERE archive_key=k AND page_number=p.page_number)<>p.result_count THEN RAISE EXCEPTION 'S02_RAW_OCCURRENCE_CENSUS_MISMATCH';END IF;
 UPDATE population_top3000_202608_202609_v1.massive_shortinterest_page_v1 SET normalized_at=coalesce(normalized_at,clock_timestamp()),normalized_source_records=result_count WHERE archive_key=k AND page_number=p.page_number;
 PERFORM population_top3000_202608_202609_v1.massive_shortinterest_resource_guard_v1();
 h:=encode(sha256(convert_to(jsonb_build_object('source_id',p.source_id,'source_sha256',p.source_sha256,'raw_records',p.result_count,'normalizer_sha256',nh)::text,'UTF8')),'hex');
 RETURN jsonb_build_object('state',CASE WHEN p.page_number=terminal THEN 'COMPLETE' ELSE 'CONTINUE' END,'checkpoint',jsonb_build_object('last_normalized_page',p.page_number),'commit_receipt',jsonb_build_object('commit_key','s02-normalized:'||p.source_id||':'||nh,'output_sha256',h,'new_committed_units',new_rows,'immutable_output_ids',jsonb_build_array(p.source_id),'input_versions',t.input_versions,'cohort_certified',false));
END $function$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.massive_shortinterest_derive_v1(p_attempt uuid,p_token text)
RETURNS jsonb LANGUAGE plpgsql SET search_path TO 'pg_catalog' AS $function$
DECLARE tid uuid;t population_top3000_202608_202609_v1.pipeline_task%rowtype;c jsonb;k text;terminal integer;pages bigint;terminals bigint;normalized boolean;
 p population_top3000_202608_202609_v1.massive_shortinterest_page_v1%rowtype;r population_top3000_202608_202609_v1.massive_shortinterest_record_v1%rowtype;
 rec jsonb;page_body jsonb;page_text text;page_sha text;page_bytes bigint;doc population_top3000_202608_202609_v1.source_document%rowtype;ticker text;settled date;published_date date;role text;shorts numeric;average_volume numeric;cover numeric;scope_match boolean;business_conflict boolean;
 vals jsonb;states jsonb;line jsonb;expect_evidence jsonb;h text;dh text;nh text;calendar_hash text;new_rows integer:=0;changed integer;occurrences bigint;provenance_hash text;
 required text[]:=ARRAY['ticker','settlement_date','short_interest','avg_daily_volume','days_to_cover','scheduled_publication_date','scheduled_publication_in_target_window','exact_publication_at','company_id','security_id','available_at'];
BEGIN
 tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);
 SELECT * INTO t FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=tid;
 IF t.family_id<>'S02' OR t.stage<>'DERIVE' THEN RAISE EXCEPTION 'S02_STAGE_MISMATCH';END IF;
 c:=population_top3000_202608_202609_v1.massive_shortinterest_context_v1(tid);k:=c->>'archive_key';
 PERFORM population_top3000_202608_202609_v1.massive_shortinterest_resource_guard_v1();
 SELECT count(*),max(page_number) FILTER(WHERE next_url IS NULL),count(*) FILTER(WHERE next_url IS NULL),bool_and(normalized_at IS NOT NULL AND normalized_source_records=result_count)
 INTO pages,terminal,terminals,normalized FROM population_top3000_202608_202609_v1.massive_shortinterest_page_v1 WHERE archive_key=k;
 IF terminal IS NULL OR terminals<>1 OR pages<>terminal OR NOT coalesce(normalized,false) THEN RETURN jsonb_build_object('state','BLOCKED','blocker_code','S02_SOURCE_NORMALIZATION_INCOMPLETE','resumption_condition',jsonb_build_object('event','COMPLETE_SOURCE_PAGE_CHAIN_NORMALIZED','archive_key',k));END IF;
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.massive_shortinterest_page_v1 q WHERE q.archive_key=k AND q.result_count<>(SELECT count(*) FROM population_top3000_202608_202609_v1.massive_shortinterest_record_v1 rr WHERE rr.archive_key=k AND rr.page_number=q.page_number)) THEN RAISE EXCEPTION 'S02_RAW_CENSUS_MISMATCH';END IF;
 SELECT * INTO p FROM population_top3000_202608_202609_v1.massive_shortinterest_page_v1 WHERE archive_key=k AND page_number=coalesce((t.checkpoint->>'last_derived_page')::integer,0)+1;
 IF NOT FOUND THEN RAISE EXCEPTION 'S02_DERIVE_CHECKPOINT_MISMATCH';END IF;
 SELECT * INTO doc FROM population_top3000_202608_202609_v1.source_document WHERE source_id=p.source_id;
 IF NOT FOUND THEN RAISE EXCEPTION 'S02_DERIVE_SOURCE_INTEGRITY';END IF;
 page_text:=doc.content_text;page_sha:=doc.source_sha256;page_bytes:=doc.content_bytes;
 IF doc.source_provider IS DISTINCT FROM 'Massive' OR doc.retrieved_at IS DISTINCT FROM p.retrieved_at OR doc.source_published_at IS NOT NULL OR doc.historical_received_at IS NOT NULL OR doc.license_policy IS DISTINCT FROM 'EXISTING_MASSIVE_SUBSCRIPTION_API_RESPONSE_INTERNAL_USE' OR doc.source_uri IS DISTINCT FROM p.request_url OR doc.evidence->>'wire_sha256' IS DISTINCT FROM p.wire_sha256 OR (doc.evidence->>'wire_bytes')::bigint IS DISTINCT FROM p.wire_bytes OR (doc.evidence->>'stored_content_bytes')::bigint IS DISTINCT FROM doc.content_bytes OR page_text IS NULL OR page_sha IS DISTINCT FROM p.source_sha256 OR page_sha IS DISTINCT FROM encode(sha256(convert_to(page_text,'UTF8')),'hex') OR page_bytes IS DISTINCT FROM octet_length(page_text)::bigint THEN RAISE EXCEPTION 'S02_DERIVE_SOURCE_INTEGRITY';END IF;
 page_body:=page_text::jsonb;
 IF page_body->>'status' IS DISTINCT FROM 'OK' OR jsonb_typeof(page_body->'results') IS DISTINCT FROM 'array' OR jsonb_array_length(page_body->'results') IS DISTINCT FROM p.result_count OR page_body->>'next_url' IS DISTINCT FROM p.next_url THEN RAISE EXCEPTION 'S02_DERIVE_SOURCE_COUNT_MISMATCH';END IF;
 dh:=encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.massive_shortinterest_derive_v1(uuid,text)'::regprocedure),'UTF8')),'hex');
 nh:=encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.massive_shortinterest_normalize_v1(uuid,text)'::regprocedure),'UTF8')),'hex');
 calendar_hash:=encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.massive_shortinterest_calendar_v1()'::regprocedure),'UTF8')),'hex');
 FOR r IN SELECT rr.* FROM population_top3000_202608_202609_v1.massive_shortinterest_record_v1 rr
 WHERE rr.archive_key=k AND rr.page_number=p.page_number AND NOT EXISTS(
  SELECT 1 FROM population_top3000_202608_202609_v1.massive_shortinterest_record_v1 earlier WHERE earlier.archive_key=k AND earlier.unit_key=rr.unit_key AND (earlier.page_number,earlier.source_row)<(rr.page_number,rr.source_row)
 ) ORDER BY rr.source_row LOOP
  rec:=r.raw_record;
  IF r.raw_record_sha256 IS DISTINCT FROM encode(sha256(convert_to(rec::text,'UTF8')),'hex') OR page_body->'results'->(r.source_row-1) IS DISTINCT FROM rec OR r.source_id IS DISTINCT FROM p.source_id THEN RAISE EXCEPTION 'S02_RAW_RECORD_HASH_OR_SOURCE_MISMATCH';END IF;
  ticker:=CASE WHEN jsonb_typeof(rec->'ticker')='string' AND length(rec->>'ticker') BETWEEN 1 AND 64 AND rec->>'ticker'!~'[[:cntrl:][:space:]]' THEN rec->>'ticker' ELSE NULL END;
  settled:=population_top3000_202608_202609_v1.massive_shortinterest_date_v1(rec->>'settlement_date');
  published_date:=NULL;role:=CASE WHEN settled<date '2026-08-01' THEN 'SUPPORT' ELSE 'TARGET' END;
  SELECT cal.scheduled_publication_date INTO published_date FROM population_top3000_202608_202609_v1.massive_shortinterest_calendar_v1() cal WHERE cal.settlement_date=settled;
  scope_match:=settled BETWEEN date '2026-07-15' AND date '2026-09-30';
  shorts:=population_top3000_202608_202609_v1.massive_shortinterest_numeric_v1(rec->'short_interest');
  average_volume:=population_top3000_202608_202609_v1.massive_shortinterest_numeric_v1(rec->'avg_daily_volume');
  cover:=population_top3000_202608_202609_v1.massive_shortinterest_numeric_v1(rec->'days_to_cover');
  business_conflict:=EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.massive_shortinterest_record_v1 q WHERE q.archive_key=k AND q.source_record_key=r.source_record_key AND q.raw_record_sha256<>r.raw_record_sha256);
  vals:=jsonb_build_object('ticker',rec->'ticker','settlement_date',rec->'settlement_date','short_interest',rec->'short_interest','avg_daily_volume',rec->'avg_daily_volume','days_to_cover',rec->'days_to_cover',
   'scheduled_publication_date',published_date,'scheduled_publication_in_target_window',CASE WHEN published_date IS NOT NULL THEN published_date BETWEEN date '2026-08-01' AND date '2026-09-30' ELSE NULL END,
   'exact_publication_at',NULL,'company_id',NULL,'security_id',NULL,'available_at',NULL,'raw_event',rec);
  states:=jsonb_build_object(
   'ticker',CASE WHEN rec->>'ticker' IS NULL OR rec->>'ticker'='' THEN 'UNAVAILABLE' WHEN ticker IS NULL THEN 'QUARANTINED' WHEN business_conflict THEN 'CONFLICTING' ELSE 'VALUE' END,
   'settlement_date',CASE WHEN rec->>'settlement_date' IS NULL THEN 'UNAVAILABLE' WHEN settled IS NULL OR NOT coalesce(scope_match,false) OR published_date IS NULL THEN 'QUARANTINED' ELSE 'VALUE' END,
   'short_interest',CASE WHEN rec->>'short_interest' IS NULL THEN 'UNAVAILABLE' WHEN shorts IS NULL OR shorts<0 OR shorts<>trunc(shorts) THEN 'QUARANTINED' WHEN business_conflict THEN 'CONFLICTING' ELSE 'VALUE' END,
   'avg_daily_volume',CASE WHEN rec->>'avg_daily_volume' IS NULL THEN 'UNAVAILABLE' WHEN average_volume IS NULL OR average_volume<0 THEN 'QUARANTINED' WHEN business_conflict THEN 'CONFLICTING' ELSE 'VALUE' END,
   'days_to_cover',CASE WHEN rec->>'days_to_cover' IS NULL THEN 'UNAVAILABLE' WHEN cover IS NULL OR cover<0 THEN 'QUARANTINED' WHEN business_conflict THEN 'CONFLICTING' ELSE 'VALUE' END,
   'scheduled_publication_date',CASE WHEN published_date IS NULL THEN 'UNAVAILABLE' ELSE 'VALUE' END,'scheduled_publication_in_target_window',CASE WHEN published_date IS NULL THEN 'UNAVAILABLE' ELSE 'VALUE' END,
   'exact_publication_at','UNAVAILABLE','company_id','UNAVAILABLE','security_id','UNAVAILABLE','available_at','UNAVAILABLE');
  SELECT count(*),encode(sha256(convert_to(string_agg(q.source_id||':'||q.source_row::text,E'\n' ORDER BY q.page_number,q.source_row),'UTF8')),'hex') INTO occurrences,provenance_hash FROM population_top3000_202608_202609_v1.massive_shortinterest_record_v1 q WHERE q.archive_key=k AND q.unit_key=r.unit_key;
  line:=jsonb_build_object('source_id',r.source_id,'source_record_key',r.source_record_key,'raw_record_sha256',r.raw_record_sha256,'source_page',r.page_number,'source_row',r.source_row,
   'archive_key',k,'canonical_provenance_rule','FIRST_PAGE_THEN_FIRST_ROW_FOR_IDENTICAL_VERSION','source_occurrences',occurrences,'source_occurrence_manifest_sha256',provenance_hash,
   'all_provenance_relation','population_top3000_202608_202609_v1.massive_shortinterest_record_v1','all_provenance_unit_key',r.unit_key,'normalization_function_sha256',nh,'derivation_function_sha256',dh,
   'calendar_function_sha256',calendar_hash,'scheduled_publication_source','https://www.finra.org/filing-reporting/regulatory-filing-systems/short-interest','calendar_evidence_kind','CONTROLLER_VERIFIED_OFFICIAL_SCHEDULE_EXTRACT',
   'scheduled_date_is_not_observed_publication_time',true,'source_publication_time','UNKNOWN','historical_receipt','NOT_OBSERVED','company_membership','UNVERIFIED','query_settlement_date_matches',scope_match,
   'july_settlements_support_target_state',true,'september_30_scheduled_publication_is_october_9',true,'company_report_census_verified',false);
  expect_evidence:=jsonb_build_object('basis','PROVISIONAL_RETURNED_SYMBOL_SETTLEMENT_VERSION_ENUMERATION','independent_company_report_census',false,'top3000_company_membership_verified',false,'source_record_sha256',r.raw_record_sha256,'settlement_role',role,'scheduled_publication_is_date_only',true,'strict_receipt_required',false);
  h:=encode(sha256(convert_to(jsonb_build_object('values',vals,'states',states,'lineage',line)::text,'UTF8')),'hex');
  INSERT INTO population_top3000_202608_202609_v1.source_observation(family_id,partition_key,unit_key,time_role,event_at,effective_date,source_published_at,historical_received_at,available_at,availability_policy,vintage_key,source_id,source_record_key,normalization_sha256,values_json,field_state,lineage,record_sha256)
  VALUES('S02',t.partition_key,r.unit_key,role,NULL,settled,NULL,NULL,NULL,'ARCHIVED_OBSERVATION',k,r.source_id,r.source_record_key,dh,vals,states,line,h)
  ON CONFLICT(release_id,family_id,partition_key,unit_key) DO NOTHING;
  GET DIAGNOSTICS changed=ROW_COUNT;new_rows:=new_rows+changed;
  IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.source_observation o WHERE o.release_id='top3000_companies_202608_202609_v1' AND o.family_id='S02' AND o.partition_key=t.partition_key AND o.unit_key=r.unit_key AND o.source_id=r.source_id AND o.source_record_key=r.source_record_key AND o.normalization_sha256=dh AND o.record_sha256=h AND o.values_json=vals AND o.field_state=states AND o.lineage=line AND o.time_role=role AND o.effective_date IS NOT DISTINCT FROM settled AND o.event_at IS NULL AND o.source_published_at IS NULL AND o.historical_received_at IS NULL AND o.available_at IS NULL AND o.company_id IS NULL AND o.security_id IS NULL AND o.reporting_entity_id IS NULL AND o.availability_policy='ARCHIVED_OBSERVATION' AND o.vintage_key=k) THEN RAISE EXCEPTION 'S02_OBSERVATION_REPLAY_CONFLICT';END IF;
  INSERT INTO population_top3000_202608_202609_v1.expected_observation(family_id,partition_key,unit_key,time_role,required_fields,expectation_source_id,expectation_record_key,applicability_evidence)
  VALUES('S02',t.partition_key,r.unit_key,role,required,r.source_id,r.source_record_key,expect_evidence) ON CONFLICT(release_id,family_id,partition_key,unit_key) DO NOTHING;
  IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.expected_observation e WHERE e.release_id='top3000_companies_202608_202609_v1' AND e.family_id='S02' AND e.partition_key=t.partition_key AND e.unit_key=r.unit_key AND e.time_role=role AND e.required_fields=required AND e.expectation_source_id=r.source_id AND e.expectation_record_key=r.source_record_key AND e.applicability_evidence=expect_evidence AND e.company_id IS NULL AND e.security_id IS NULL AND e.reporting_entity_id IS NULL) THEN RAISE EXCEPTION 'S02_EXPECTATION_REPLAY_CONFLICT';END IF;
 END LOOP;
 INSERT INTO population_top3000_202608_202609_v1.pipeline_partition(family_id,partition_key,time_role,denominator_state,denominator_evidence,producer_revision,immutable_input_versions,publication_eligible)
 VALUES('S02',t.partition_key,'TARGET','PROVISIONAL',jsonb_build_object('archive_key',k,'provider_page_chain_terminal',true,'independent_company_report_denominator',false,'july_support_rows_retained',true,'report_calendar',(SELECT jsonb_agg(to_jsonb(cal) ORDER BY cal.settlement_date) FROM population_top3000_202608_202609_v1.massive_shortinterest_calendar_v1() cal),'scheduled_dates_are_not_exact_publication_times',true),'massive-shortinterest-20261023-v1',t.input_versions,false)
 ON CONFLICT(family_id,partition_key) DO NOTHING;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_partition WHERE family_id='S02' AND partition_key=t.partition_key AND immutable_input_versions=t.input_versions AND denominator_state='PROVISIONAL' AND NOT publication_eligible) THEN RAISE EXCEPTION 'S02_PARTITION_REVISION_CONFLICT';END IF;
 PERFORM population_top3000_202608_202609_v1.massive_shortinterest_resource_guard_v1();
 h:=encode(sha256(convert_to(jsonb_build_object('archive_key',k,'derived_page',p.page_number,'deriver_sha256',dh,'source_record_count',p.result_count)::text,'UTF8')),'hex');
 RETURN jsonb_build_object('state',CASE WHEN p.page_number=terminal THEN 'COMPLETE' ELSE 'CONTINUE' END,'checkpoint',jsonb_build_object('last_derived_page',p.page_number),'commit_receipt',jsonb_build_object('commit_key','s02-derived:'||k||':'||p.page_number::text||':'||dh,'output_sha256',h,'new_committed_units',new_rows,'immutable_output_ids',jsonb_build_array(k,p.source_id),'input_versions',t.input_versions,'cohort_certified',false,'independent_shortinterest_accuracy_verified',false));
END $function$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.massive_shortinterest_repair_hold_v1(p_attempt uuid,p_token text)
RETURNS jsonb LANGUAGE plpgsql SET search_path TO 'pg_catalog' AS $function$
DECLARE tid uuid;
BEGIN
 tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=tid AND family_id='S02' AND stage='REPAIR') THEN RAISE EXCEPTION 'S02_STAGE_MISMATCH';END IF;
 PERFORM population_top3000_202608_202609_v1.massive_shortinterest_context_v1(tid);
 RETURN jsonb_build_object('state','BLOCKED','blocker_code','S02_COMPANY_REPORT_DENOMINATOR_AND_PUBLICATION_TIME_REQUIRED','resumption_condition',jsonb_build_object('event','REVIEWED_COMPANY_REPORT_CENSUS_IDENTITY_AND_TEMPORAL_EVIDENCE_AVAILABLE','required_evidence',jsonb_build_array('FROZEN_3000_COMPANY_UNIVERSE_AND_HISTORICAL_SECURITY_MAPPING','INDEPENDENT_EXPECTED_REPORT_CENSUS_INCLUDING_EXPLICIT_ABSENCE_RULES','INDEPENDENT_SOURCE_RECONCILIATION','PUBLISHED_DATE_VS_EXACT_AVAILABILITY_POLICY'),'provider_chain_terminal_is_not_company_coverage',true,'preserve_all_source_pages_and_versions',true));
END $function$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.massive_shortinterest_bundle_sha256_v1()
RETURNS text LANGUAGE sql STABLE SET search_path TO 'pg_catalog' AS $function$
 SELECT encode(sha256(convert_to(jsonb_object_agg(fn,encode(sha256(convert_to(pg_get_functiondef(('population_top3000_202608_202609_v1.'||fn)::regprocedure),'UTF8')),'hex'))::text,'UTF8')),'hex')
 FROM (VALUES ('massive_shortinterest_archive_key_v1(text,text,jsonb)'),('massive_shortinterest_context_v1(uuid)'),('massive_shortinterest_acquire_v1(uuid,text)'),('massive_shortinterest_claim_v1(uuid,text)'),('massive_shortinterest_resource_guard_v1()'),('massive_shortinterest_commit_v1(uuid,text,jsonb)'),('massive_shortinterest_failure_v1(uuid,text,jsonb)'),('massive_shortinterest_date_v1(text)'),('massive_shortinterest_numeric_v1(jsonb)'),('massive_shortinterest_calendar_v1()'),('massive_shortinterest_normalize_v1(uuid,text)'),('massive_shortinterest_derive_v1(uuid,text)'),('massive_shortinterest_repair_hold_v1(uuid,text)'),('massive_shortinterest_bundle_sha256_v1()')) f(fn);
$function$;

REVOKE ALL ON FUNCTION population_top3000_202608_202609_v1.massive_shortinterest_archive_key_v1(text,text,jsonb),population_top3000_202608_202609_v1.massive_shortinterest_context_v1(uuid),population_top3000_202608_202609_v1.massive_shortinterest_acquire_v1(uuid,text),population_top3000_202608_202609_v1.massive_shortinterest_claim_v1(uuid,text),population_top3000_202608_202609_v1.massive_shortinterest_resource_guard_v1(),population_top3000_202608_202609_v1.massive_shortinterest_commit_v1(uuid,text,jsonb),population_top3000_202608_202609_v1.massive_shortinterest_failure_v1(uuid,text,jsonb),population_top3000_202608_202609_v1.massive_shortinterest_date_v1(text),population_top3000_202608_202609_v1.massive_shortinterest_numeric_v1(jsonb),population_top3000_202608_202609_v1.massive_shortinterest_calendar_v1(),population_top3000_202608_202609_v1.massive_shortinterest_normalize_v1(uuid,text),population_top3000_202608_202609_v1.massive_shortinterest_derive_v1(uuid,text),population_top3000_202608_202609_v1.massive_shortinterest_repair_hold_v1(uuid,text),population_top3000_202608_202609_v1.massive_shortinterest_bundle_sha256_v1() FROM PUBLIC,anon,authenticated;
GRANT EXECUTE ON FUNCTION population_top3000_202608_202609_v1.massive_shortinterest_archive_key_v1(text,text,jsonb),population_top3000_202608_202609_v1.massive_shortinterest_context_v1(uuid),population_top3000_202608_202609_v1.massive_shortinterest_acquire_v1(uuid,text),population_top3000_202608_202609_v1.massive_shortinterest_claim_v1(uuid,text),population_top3000_202608_202609_v1.massive_shortinterest_resource_guard_v1(),population_top3000_202608_202609_v1.massive_shortinterest_commit_v1(uuid,text,jsonb),population_top3000_202608_202609_v1.massive_shortinterest_failure_v1(uuid,text,jsonb),population_top3000_202608_202609_v1.massive_shortinterest_date_v1(text),population_top3000_202608_202609_v1.massive_shortinterest_numeric_v1(jsonb),population_top3000_202608_202609_v1.massive_shortinterest_calendar_v1(),population_top3000_202608_202609_v1.massive_shortinterest_normalize_v1(uuid,text),population_top3000_202608_202609_v1.massive_shortinterest_derive_v1(uuid,text),population_top3000_202608_202609_v1.massive_shortinterest_repair_hold_v1(uuid,text),population_top3000_202608_202609_v1.massive_shortinterest_bundle_sha256_v1() TO postgres,service_role;

commit;