-- Root-only reviewed installation draft. Does not enable, seed or dispatch any worker.
-- N03 core only. N01/N02 are deliberately not admitted by these routines.
CREATE TABLE IF NOT EXISTS population_top3000_202608_202609_v1.massive_archive_page_v1 (
 archive_key text NOT NULL,
 family_id text NOT NULL CHECK(family_id='N03'),
 partition_key text NOT NULL CHECK(partition_key='splits-execution-202608-202609'),
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
ALTER TABLE population_top3000_202608_202609_v1.massive_archive_page_v1 ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON population_top3000_202608_202609_v1.massive_archive_page_v1 FROM PUBLIC,anon,authenticated;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.massive_archive_key_v1(p_family text,p_partition text,p_versions jsonb)
RETURNS text LANGUAGE sql IMMUTABLE SET search_path TO 'pg_catalog' AS $function$
 SELECT 'massive-archive:'||p_family||':'||encode(sha256(convert_to(jsonb_build_object('family',p_family,'partition',p_partition,'input_versions',p_versions)::text,'UTF8')),'hex');
$function$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.massive_actions_context_v1(p_task uuid)
RETURNS jsonb LANGUAGE plpgsql SET search_path TO 'pg_catalog' AS $function$
DECLARE t population_top3000_202608_202609_v1.pipeline_task%rowtype;k text;n integer;lim integer;url text;prior population_top3000_202608_202609_v1.massive_archive_page_v1%rowtype;
BEGIN
 SELECT * INTO t FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=p_task;
 IF NOT FOUND OR t.family_id<>'N03' OR t.partition_key<>'splits-execution-202608-202609'
 OR t.input_versions->>'scope_sha256' IS DISTINCT FROM '8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd'
 OR t.input_versions->>'archive_contract_revision' IS DISTINCT FROM 'massive-actions-20261023-v1'
 THEN RAISE EXCEPTION 'MASSIVE_ACTION_SCOPE_NOT_ADMITTED';END IF;
 k:=population_top3000_202608_202609_v1.massive_archive_key_v1(t.family_id,t.partition_key,t.input_versions);
 IF coalesce(t.checkpoint->>'next_page','1') !~ '^[0-9]{1,4}$' OR coalesce(t.payload->>'max_pages','1') !~ '^[0-9]{1,4}$' THEN RAISE EXCEPTION 'MASSIVE_ACTION_PAGE_LIMIT_INVALID';END IF;
 n:=coalesce(t.checkpoint->>'next_page','1')::integer;lim:=coalesce(t.payload->>'max_pages','1')::integer;
 IF n<1 OR n>1000 OR lim<1 OR lim>1000 THEN RAISE EXCEPTION 'MASSIVE_ACTION_PAGE_LIMIT_INVALID';END IF;
 IF n=1 THEN
  url:='https://api.massive.com/stocks/v1/splits?execution_date.gte=2026-08-01&execution_date.lte=2026-09-30&sort=execution_date.asc&limit=1000';
 ELSE
  SELECT * INTO prior FROM population_top3000_202608_202609_v1.massive_archive_page_v1 WHERE archive_key=k AND page_number=n-1;
  IF NOT FOUND OR prior.next_url IS NULL OR t.checkpoint->>'last_source_id' IS DISTINCT FROM prior.source_id
  OR (SELECT source_sha256 FROM population_top3000_202608_202609_v1.source_document WHERE source_id=prior.source_id) IS DISTINCT FROM t.checkpoint->>'last_source_sha256'
  THEN RAISE EXCEPTION 'MASSIVE_ACTION_CONTINUATION_EVIDENCE_MISSING';END IF;
  url:=prior.next_url;
  IF url !~ '^https://api[.]massive[.]com/stocks/v1/splits[?]cursor=[A-Za-z0-9_.~%+\-]+$' THEN RAISE EXCEPTION 'MASSIVE_ACTION_UNSAFE_CURSOR';END IF;
 END IF;
 RETURN jsonb_build_object('state',CASE WHEN n>lim THEN 'PAGE_LIMIT_REVIEW' ELSE 'READY' END,'archive_key',k,'page_number',n,'max_pages',lim,
 'family_id',t.family_id,'partition_key',t.partition_key,'request_url',url,'max_response_bytes',8388608,'max_runtime_ms',45000);
END $function$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.massive_actions_acquire_v1(p_attempt uuid,p_token text)
RETURNS jsonb LANGUAGE plpgsql SET search_path TO 'pg_catalog' AS $function$
DECLARE tid uuid;c jsonb;
BEGIN
 tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=tid AND stage='ACQUIRE') THEN RAISE EXCEPTION 'MASSIVE_ACTION_STAGE_MISMATCH';END IF;
 c:=population_top3000_202608_202609_v1.massive_actions_context_v1(tid);
 IF c->>'state'='PAGE_LIMIT_REVIEW' THEN RETURN jsonb_build_object('state','BLOCKED','blocker_code','MASSIVE_PAGE_CAP_REVIEW','resumption_condition',jsonb_build_object('event','RETAINED_PAGE_REVIEW_AND_BOUNDED_CAP_ADMISSION','next_page',c->'page_number','existing_pages_preserved',true));END IF;
 RETURN population_top3000_202608_202609_v1.pipeline_dispatch_edge_v1(p_attempt,p_token,'population-top3000-actions-v1');
END $function$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.massive_actions_claim_v1(p_attempt uuid,p_token text)
RETURNS jsonb LANGUAGE plpgsql SET search_path TO 'pg_catalog' AS $function$
DECLARE tid uuid;c jsonb;r jsonb;
BEGIN
 tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=tid AND stage='ACQUIRE') THEN RAISE EXCEPTION 'MASSIVE_ACTION_STAGE_MISMATCH';END IF;
 c:=population_top3000_202608_202609_v1.massive_actions_context_v1(tid);
 IF c->>'state'<>'READY' THEN
  RETURN population_top3000_202608_202609_v1.pipeline_finish_v1(p_attempt,p_token,jsonb_build_object('state','BLOCKED','blocker_code','MASSIVE_PAGE_CAP_REVIEW','resumption_condition',jsonb_build_object('event','RETAINED_PAGE_REVIEW_AND_BOUNDED_CAP_ADMISSION','next_page',c->'page_number','existing_pages_preserved',true)));
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

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.massive_actions_commit_v1(p_attempt uuid,p_token text,p_page jsonb)
RETURNS jsonb LANGUAGE plpgsql SET search_path TO 'pg_catalog' AS $function$
DECLARE tid uuid;t population_top3000_202608_202609_v1.pipeline_task%rowtype;c jsonb;body jsonb;txt text;h text;wire text;
 sid text;k text;n integer;cnt integer;bytes bigint;stored_bytes bigint;v_next_url text;at_time timestamptz;fresh integer;receipt jsonb;
BEGIN
 tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);
 SELECT * INTO t FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=tid;
 IF t.stage<>'ACQUIRE' THEN RAISE EXCEPTION 'MASSIVE_ACTION_STAGE_MISMATCH';END IF;
 c:=population_top3000_202608_202609_v1.massive_actions_context_v1(tid);
 IF c->>'state'<>'READY' OR jsonb_typeof(p_page) IS DISTINCT FROM 'object' OR p_page->>'archive_key' IS DISTINCT FROM c->>'archive_key'
 OR p_page->>'page_number' IS DISTINCT FROM c->>'page_number' OR p_page->>'request_url' IS DISTINCT FROM c->>'request_url'
 OR p_page->>'http_status' IS DISTINCT FROM '200' THEN RAISE EXCEPTION 'MASSIVE_ACTION_RECEIPT_SCOPE_MISMATCH';END IF;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_provider_lease WHERE attempt_id=p_attempt AND provider='Massive' AND released_at IS NULL AND reserved_bytes=8388608) THEN RAISE EXCEPTION 'MASSIVE_ACTION_RESERVATION_REQUIRED';END IF;
 txt:=p_page->>'content_text';stored_bytes:=octet_length(txt);bytes:=(p_page->>'wire_bytes')::bigint;wire:=p_page->>'wire_sha256';
 IF txt IS NULL OR stored_bytes>8388608 OR bytes IS NULL OR bytes<1 OR bytes>8388608 OR wire IS NULL OR wire!~'^[0-9a-f]{64}$'
 OR coalesce((p_page->>'elapsed_ms')::bigint,-1) NOT BETWEEN 0 AND 45000 THEN RAISE EXCEPTION 'MASSIVE_ACTION_SOURCE_BOUND_INVALID';END IF;
 h:=encode(sha256(convert_to(txt,'UTF8')),'hex');
 IF h IS DISTINCT FROM p_page->>'source_sha256' THEN RAISE EXCEPTION 'MASSIVE_ACTION_STORED_HASH_MISMATCH';END IF;
 body:=txt::jsonb;
 IF body->>'status' IS DISTINCT FROM 'OK' OR jsonb_typeof(body->'results') IS DISTINCT FROM 'array' THEN RAISE EXCEPTION 'MASSIVE_ACTION_INVALID_SOURCE_ENVELOPE';END IF;
 cnt:=jsonb_array_length(body->'results');
 IF cnt>1000 OR cnt IS DISTINCT FROM (p_page->>'result_count')::integer OR EXISTS(SELECT 1 FROM jsonb_array_elements(body->'results') v WHERE jsonb_typeof(v) IS DISTINCT FROM 'object') THEN RAISE EXCEPTION 'MASSIVE_ACTION_SOURCE_RECORD_SHAPE';END IF;
 v_next_url:=p_page->>'next_url';
 IF v_next_url IS DISTINCT FROM body->>'next_url' OR (v_next_url IS NOT NULL AND (v_next_url!~'^https://api[.]massive[.]com/stocks/v1/splits[?]cursor=[A-Za-z0-9_.~%+\-]+$' OR length(v_next_url)>16384 OR v_next_url=c->>'request_url')) THEN RAISE EXCEPTION 'MASSIVE_ACTION_CURSOR_MISMATCH';END IF;
 IF txt~* '"(apiKey|api_key|access_token|authorization)"[[:space:]]*:' OR txt~* '[?&](apiKey|api_key|access_token)=' THEN RAISE EXCEPTION 'MASSIVE_ACTION_UNREDACTED_CREDENTIAL_FIELD';END IF;
 k:=c->>'archive_key';n:=(c->>'page_number')::integer;sid:=k||':page:'||n::text;at_time:=(p_page->>'retrieved_at')::timestamptz;
 IF at_time IS NULL OR at_time<(SELECT started_at FROM population_top3000_202608_202609_v1.pipeline_attempt WHERE attempt_id=p_attempt)-interval '30 seconds' OR at_time>clock_timestamp()+interval '30 seconds' THEN RAISE EXCEPTION 'MASSIVE_ACTION_RETRIEVAL_CLOCK_INVALID';END IF;
 IF v_next_url IS NOT NULL AND EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.massive_archive_page_v1 WHERE archive_key=k AND request_url=v_next_url) THEN RAISE EXCEPTION 'MASSIVE_ACTION_CURSOR_CYCLE';END IF;
 INSERT INTO population_top3000_202608_202609_v1.source_document
 (source_id,source_sha256,source_uri,source_provider,retrieved_at,source_published_at,historical_received_at,content_text,content_bytes,evidence,license_policy)
 VALUES(sid,h,c->>'request_url','Massive',at_time,NULL,NULL,txt,stored_bytes,
 jsonb_build_object('wire_sha256',wire,'wire_bytes',bytes,'stored_content_bytes',stored_bytes,'hash_representation','EXACT_UTF8_RESPONSE_WITH_TOP_LEVEL_NEXT_URL_CREDENTIAL_REDACTION_ONLY','wire_bytes_retained',false,'provider_request_id',p_page->>'provider_request_id','scope','N03 execution dates 2026-08-01 through 2026-09-30','original_historical_receipt',false),
 'EXISTING_MASSIVE_SUBSCRIPTION_API_RESPONSE_INTERNAL_USE') ON CONFLICT(source_id) DO NOTHING;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.source_document d WHERE d.source_id=sid AND d.source_sha256=h AND d.source_uri=c->>'request_url' AND d.content_text=txt AND d.content_bytes=stored_bytes AND d.evidence->>'wire_sha256'=wire AND (d.evidence->>'wire_bytes')::bigint=bytes) THEN RAISE EXCEPTION 'MASSIVE_ACTION_SOURCE_REVISION_CONFLICT';END IF;
 INSERT INTO population_top3000_202608_202609_v1.massive_archive_page_v1
 (archive_key,family_id,partition_key,page_number,acquire_task_id,attempt_id,source_id,source_sha256,request_url,next_url,wire_sha256,wire_bytes,result_count,retrieved_at)
 VALUES(k,t.family_id,t.partition_key,n,tid,p_attempt,sid,h,c->>'request_url',v_next_url,wire,bytes,cnt,at_time)
 ON CONFLICT(archive_key,page_number) DO NOTHING;
 GET DIAGNOSTICS fresh=ROW_COUNT;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.massive_archive_page_v1 q WHERE q.archive_key=k AND q.page_number=n AND q.source_id=sid AND q.source_sha256=h AND q.wire_sha256=wire AND q.wire_bytes=bytes AND q.request_url=c->>'request_url' AND q.next_url IS NOT DISTINCT FROM v_next_url AND q.result_count=cnt) THEN RAISE EXCEPTION 'MASSIVE_ACTION_PAGE_REVISION_CONFLICT';END IF;
 PERFORM population_top3000_202608_202609_v1.pipeline_provider_release_v1(p_attempt,p_token,stored_bytes,0,0);
 receipt:=jsonb_build_object('state',CASE WHEN v_next_url IS NULL THEN 'COMPLETE' ELSE 'CONTINUE' END,
 'checkpoint',jsonb_build_object('next_page',n+1,'last_source_id',sid,'last_source_sha256',h,'next_url',v_next_url,'source_records_seen',coalesce((t.checkpoint->>'source_records_seen')::bigint,0)+cnt),
 'not_before',(SELECT next_provider_at FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton),
 'commit_receipt',jsonb_build_object('commit_key','massive-page:'||sid||':'||h,'output_sha256',h,'new_committed_units',fresh,'immutable_output_ids',jsonb_build_array(sid),'input_versions',t.input_versions,'raw_source_records',cnt,'terminal_page',v_next_url IS NULL,'cohort_certified',false));
 RETURN population_top3000_202608_202609_v1.pipeline_finish_v1(p_attempt,p_token,receipt);
END $function$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.massive_actions_failure_v1(p_attempt uuid,p_token text,p_failure jsonb)
RETURNS jsonb LANGUAGE plpgsql SET search_path TO 'pg_catalog' AS $function$
DECLARE tid uuid;code text;class text;bytes bigint;retry integer;unknown_effect boolean;details jsonb;
BEGIN
 tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=tid AND family_id='N03' AND stage='ACQUIRE') THEN RAISE EXCEPTION 'MASSIVE_ACTION_STAGE_MISMATCH';END IF;
 code:=p_failure->>'code';class:=p_failure->>'failure_class';bytes:=(p_failure->>'received_bytes')::bigint;retry:=(p_failure->>'retry_after_seconds')::integer;
 IF code IS NULL OR code!~'^[A-Z0-9_]{1,80}$' OR class IS NULL OR class NOT IN('RATE_LIMIT','TRANSPORT_READ','AUTHENTICATION','PARSER','SOURCE_ABSENT') OR bytes IS NULL OR bytes NOT BETWEEN 0 AND 8388608 OR retry IS NULL OR retry NOT BETWEEN 0 AND 3600 THEN RAISE EXCEPTION 'MASSIVE_ACTION_FAILURE_RECEIPT_INVALID';END IF;
 details:=jsonb_build_object('code',code,'http_status',p_failure->'http_status','response_bytes_observed',bytes,'wire_sha256',p_failure->>'wire_sha256','fetch_started',p_failure->'fetch_started','raw_body_retained',false,'automatic_retry',false);
 unknown_effect:=coalesce((p_failure->>'fetch_started')::boolean,false)
  AND (p_failure->>'http_status' IS NULL OR code IN('DEADLINE','FETCH_FAILED','EMPTY_BODY') OR (class='TRANSPORT_READ' AND p_failure->>'wire_sha256' IS NULL AND p_failure->>'http_status'='200'));
 IF unknown_effect THEN
  -- Retain the reservation. The existing expiry reconciler conservatively accounts it.
  -- No new request is admitted until the controller reconciles the uncertain effect.
  RETURN population_top3000_202608_202609_v1.pipeline_fail_v1(p_attempt,p_token,'UNCERTAIN_EFFECT','MASSIVE_ACTION:'||code,details,retry);
 END IF;
 -- This counter measures retained source bytes. An unretained error body adds zero.
 PERFORM population_top3000_202608_202609_v1.pipeline_provider_release_v1(p_attempt,p_token,0,0,CASE WHEN class='RATE_LIMIT' THEN retry ELSE 0 END);
 RETURN population_top3000_202608_202609_v1.pipeline_finish_v1(p_attempt,p_token,jsonb_build_object('state','BLOCKED','blocker_code','MASSIVE_ACTION_'||code,
 'resumption_condition',jsonb_build_object('event','CONTROLLER_REVIEW_OF_DEFINITIVE_SOURCE_FAILURE','details',details,'retry_after_seconds',retry,'preserve_counters_and_cursor',true)));
END $function$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.massive_actions_date_v1(p_value text)
RETURNS date LANGUAGE plpgsql IMMUTABLE SET search_path TO 'pg_catalog' AS $function$
DECLARE d date;
BEGIN
 IF p_value IS NULL OR p_value!~'^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN RETURN NULL;END IF;
 BEGIN d:=p_value::date;EXCEPTION WHEN datetime_field_overflow OR invalid_datetime_format THEN RETURN NULL;END;
 RETURN d;
END $function$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.massive_actions_normalize_v1(p_attempt uuid,p_token text)
RETURNS jsonb LANGUAGE plpgsql SET search_path TO 'pg_catalog' AS $function$
DECLARE tid uuid;t population_top3000_202608_202609_v1.pipeline_task%rowtype;k text;p population_top3000_202608_202609_v1.massive_archive_page_v1%rowtype;
 total integer;terminal integer;last_page integer;row_count integer;new_rows integer:=0;changed integer;rec jsonb;rec_hash text;unit text;sourcekey text;d date;vals jsonb;states jsonb;line jsonb;h text;nh text;ratio_from numeric;ratio_to numeric;good_scope boolean;raw_text text;document_hash text;document_bytes bigint;document_evidence jsonb;document_uri text;
BEGIN
 tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);
 SELECT * INTO t FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=tid;
 IF t.family_id<>'N03' OR t.stage<>'NORMALIZE' OR t.partition_key<>'splits-execution-202608-202609' THEN RAISE EXCEPTION 'MASSIVE_ACTION_STAGE_MISMATCH';END IF;
 PERFORM population_top3000_202608_202609_v1.massive_actions_context_v1(tid);
 k:=population_top3000_202608_202609_v1.massive_archive_key_v1(t.family_id,t.partition_key,t.input_versions);
 SELECT count(*),max(page_number) FILTER(WHERE next_url IS NULL) INTO total,terminal FROM population_top3000_202608_202609_v1.massive_archive_page_v1 WHERE archive_key=k;
 IF terminal IS NULL OR total<>terminal OR (SELECT count(*) FROM population_top3000_202608_202609_v1.massive_archive_page_v1 WHERE archive_key=k AND next_url IS NULL)<>1
 OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.massive_archive_page_v1 q LEFT JOIN population_top3000_202608_202609_v1.massive_archive_page_v1 prior ON prior.archive_key=q.archive_key AND prior.page_number=q.page_number-1 WHERE q.archive_key=k AND q.page_number>1 AND q.request_url IS DISTINCT FROM prior.next_url)
 THEN RETURN jsonb_build_object('state','BLOCKED','blocker_code','MASSIVE_ARCHIVE_PAGE_CHAIN_INCOMPLETE','resumption_condition',jsonb_build_object('event','TERMINAL_PAGE_AND_CONTIGUOUS_SOURCE_CHAIN_VERIFIED','archive_key',k));END IF;
 last_page:=coalesce((t.checkpoint->>'last_normalized_page')::integer,0);
 SELECT * INTO p FROM population_top3000_202608_202609_v1.massive_archive_page_v1 WHERE archive_key=k AND page_number=last_page+1 FOR UPDATE;
 IF NOT FOUND THEN RAISE EXCEPTION 'MASSIVE_NORMALIZATION_CHECKPOINT_OUTSIDE_ARCHIVE';END IF;
 SELECT content_text,source_sha256,content_bytes,evidence,source_uri INTO raw_text,document_hash,document_bytes,document_evidence,document_uri
 FROM population_top3000_202608_202609_v1.source_document WHERE source_id=p.source_id;
 IF NOT FOUND OR raw_text IS NULL OR document_hash IS DISTINCT FROM p.source_sha256
 OR document_hash IS DISTINCT FROM encode(sha256(convert_to(raw_text,'UTF8')),'hex')
 OR document_bytes IS DISTINCT FROM octet_length(raw_text)::bigint OR document_uri IS DISTINCT FROM p.request_url
 OR document_evidence->>'wire_sha256' IS DISTINCT FROM p.wire_sha256 OR (document_evidence->>'wire_bytes')::bigint IS DISTINCT FROM p.wire_bytes
 THEN RAISE EXCEPTION 'MASSIVE_NORMALIZATION_RETAINED_SOURCE_INTEGRITY';END IF;
 nh:=encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.massive_actions_normalize_v1(uuid,text)'::regprocedure),'UTF8')),'hex');
 row_count:=0;
 FOR rec IN SELECT v FROM jsonb_array_elements(raw_text::jsonb->'results') v LOOP
  row_count:=row_count+1;rec_hash:=encode(sha256(convert_to(rec::text,'UTF8')),'hex');
  sourcekey:=coalesce(nullif(rec->>'id',''),'unidentified:'||rec_hash);unit:='Massive:'||sourcekey||':'||rec_hash;
  d:=population_top3000_202608_202609_v1.massive_actions_date_v1(rec->>'execution_date');good_scope:=d BETWEEN date '2026-08-01' AND date '2026-09-30';
  ratio_from:=CASE WHEN coalesce(rec->>'split_from','')~'^[0-9]{1,30}([.][0-9]{1,20})?$' THEN (rec->>'split_from')::numeric ELSE NULL END;
  ratio_to:=CASE WHEN coalesce(rec->>'split_to','')~'^[0-9]{1,30}([.][0-9]{1,20})?$' THEN (rec->>'split_to')::numeric ELSE NULL END;
  vals:=jsonb_build_object('provider_event_id',rec->'id','ticker',rec->'ticker','execution_date',rec->'execution_date','split_from',rec->'split_from','split_to',rec->'split_to','adjustment_type',rec->'adjustment_type','effective_at',NULL,'company_id',NULL,'security_id',NULL,'available_at',NULL,'raw_event',rec);
  states:=jsonb_build_object('provider_event_id',CASE WHEN nullif(rec->>'id','') IS NULL THEN 'UNAVAILABLE' ELSE 'VALUE' END,'ticker',CASE WHEN nullif(rec->>'ticker','') IS NULL THEN 'UNAVAILABLE' ELSE 'VALUE' END,
   'execution_date',CASE WHEN d IS NULL THEN 'UNAVAILABLE' WHEN NOT good_scope THEN 'QUARANTINED' ELSE 'VALUE' END,
   'split_from',CASE WHEN rec->>'split_from' IS NULL THEN 'UNAVAILABLE' WHEN ratio_from IS NULL OR ratio_from<=0 THEN 'QUARANTINED' ELSE 'VALUE' END,
   'split_to',CASE WHEN rec->>'split_to' IS NULL THEN 'UNAVAILABLE' WHEN ratio_to IS NULL OR ratio_to<=0 THEN 'QUARANTINED' ELSE 'VALUE' END,
   'adjustment_type',CASE WHEN rec->>'adjustment_type' IS NULL THEN 'UNAVAILABLE' WHEN rec->>'adjustment_type' NOT IN('forward_split','reverse_split','stock_dividend') THEN 'QUARANTINED' ELSE 'VALUE' END,
   'effective_at','UNAVAILABLE','company_id','UNAVAILABLE','security_id','UNAVAILABLE','available_at','UNAVAILABLE');
  line:=jsonb_build_object('source_id',p.source_id,'source_record_key',sourcekey,'raw_record_sha256',rec_hash,'archive_key',k,'source_page',p.page_number,'source_row',row_count,'company_membership','UNVERIFIED','source_scope','ALL_RETURNED_PROVIDER_SPLITS_FOR_DECLARED_EXECUTION_DATE_QUERY','date_semantics','execution_date only; exact effective timestamp not provided','source_publication_time','UNKNOWN','historical_receipt','NOT_OBSERVED','historical_adjustment_factor','Retained only inside raw_event; current cumulative provider factor is not a causal adjustment');
  h:=encode(sha256(convert_to(jsonb_build_object('values',vals,'states',states,'lineage',line)::text,'UTF8')),'hex');
  INSERT INTO population_top3000_202608_202609_v1.source_observation
   (family_id,partition_key,unit_key,time_role,event_at,effective_date,source_published_at,historical_received_at,available_at,availability_policy,vintage_key,source_id,source_record_key,normalization_sha256,values_json,field_state,lineage,record_sha256)
   VALUES('N03',t.partition_key,unit,'TARGET',NULL,d,NULL,NULL,NULL,'ARCHIVED_OBSERVATION',k,p.source_id,sourcekey,nh,vals,states,line,h)
   ON CONFLICT(release_id,family_id,partition_key,unit_key) DO NOTHING;
  GET DIAGNOSTICS changed=ROW_COUNT;new_rows:=new_rows+changed;
  IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.source_observation o WHERE o.family_id='N03' AND o.partition_key=t.partition_key AND o.unit_key=unit
    AND o.source_record_key=sourcekey AND o.normalization_sha256=nh AND o.values_json->'raw_event'=rec AND o.lineage->>'raw_record_sha256'=rec_hash
    AND o.record_sha256=encode(sha256(convert_to(jsonb_build_object('values',o.values_json,'states',o.field_state,'lineage',o.lineage)::text,'UTF8')),'hex'))
  THEN RAISE EXCEPTION 'MASSIVE_NORMALIZATION_EXISTING_OBSERVATION_CONFLICT';END IF;
  INSERT INTO population_top3000_202608_202609_v1.expected_observation
   (family_id,partition_key,unit_key,time_role,required_fields,expectation_source_id,expectation_record_key,applicability_evidence)
   VALUES('N03',t.partition_key,unit,'TARGET',ARRAY['provider_event_id','ticker','execution_date','split_from','split_to','adjustment_type','effective_at','company_id','security_id','available_at'],p.source_id,sourcekey,
    jsonb_build_object('basis','PROVISIONAL_ENUMERATION_OF_RETURNED_SOURCE_RECORDS','independent_event_census',false,'top3000_company_membership_verified',false,'source_record_sha256',rec_hash,'strict_receipt_required',false))
   ON CONFLICT(release_id,family_id,partition_key,unit_key) DO NOTHING;
  IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.expected_observation e WHERE e.family_id='N03' AND e.partition_key=t.partition_key AND e.unit_key=unit
    AND e.expectation_record_key=sourcekey AND e.required_fields=ARRAY['provider_event_id','ticker','execution_date','split_from','split_to','adjustment_type','effective_at','company_id','security_id','available_at']
    AND e.applicability_evidence->>'source_record_sha256'=rec_hash AND e.applicability_evidence->>'independent_event_census'='false' AND e.company_id IS NULL AND e.security_id IS NULL)
  THEN RAISE EXCEPTION 'MASSIVE_NORMALIZATION_EXISTING_EXPECTATION_CONFLICT';END IF;
 END LOOP;
 IF row_count<>p.result_count THEN RAISE EXCEPTION 'MASSIVE_NORMALIZATION_SOURCE_COUNT_MISMATCH';END IF;
 UPDATE population_top3000_202608_202609_v1.massive_archive_page_v1 SET normalized_at=clock_timestamp(),normalized_source_records=row_count WHERE archive_key=k AND page_number=p.page_number;
 INSERT INTO population_top3000_202608_202609_v1.pipeline_partition(family_id,partition_key,time_role,denominator_state,denominator_evidence,producer_revision,immutable_input_versions,publication_eligible)
 VALUES('N03',t.partition_key,'TARGET','PROVISIONAL',jsonb_build_object('archive_key',k,'provider_page_chain_terminal',true,'source_pages',total,'independent_company_event_denominator',false,'source_event_keys_only',true,'requires_dated_company_mapping',true,'exact_effective_and_publication_time_unresolved',true),'massive-actions-20261023-v1',t.input_versions,false)
 ON CONFLICT(family_id,partition_key) DO NOTHING;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_partition WHERE family_id='N03' AND partition_key=t.partition_key AND immutable_input_versions=t.input_versions AND denominator_state='PROVISIONAL' AND NOT publication_eligible) THEN RAISE EXCEPTION 'MASSIVE_NORMALIZATION_PARTITION_REVISION_CONFLICT';END IF;
 h:=encode(sha256(convert_to(jsonb_build_object('source_id',p.source_id,'source_records',row_count,'normalizer_sha256',nh)::text,'UTF8')),'hex');
 RETURN jsonb_build_object('state',CASE WHEN p.page_number=terminal THEN 'COMPLETE' ELSE 'CONTINUE' END,
  'checkpoint',jsonb_build_object('last_normalized_page',p.page_number),
  'commit_receipt',jsonb_build_object('commit_key','massive-normalized:'||p.source_id||':'||nh,'output_sha256',h,'new_committed_units',new_rows,'immutable_output_ids',jsonb_build_array(p.source_id),'input_versions',t.input_versions,'source_records',row_count,'cohort_certified',false));
END $function$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.massive_actions_derive_v1(p_attempt uuid,p_token text)
RETURNS jsonb LANGUAGE plpgsql SET search_path TO 'pg_catalog' AS $function$
DECLARE tid uuid;t population_top3000_202608_202609_v1.pipeline_task%rowtype;k text;h text;n bigint;
BEGIN
 tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);
 SELECT * INTO t FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=tid;
 IF t.family_id<>'N03' OR t.stage<>'DERIVE' OR t.partition_key<>'splits-execution-202608-202609' THEN RAISE EXCEPTION 'MASSIVE_ACTION_STAGE_MISMATCH';END IF;
 PERFORM population_top3000_202608_202609_v1.massive_actions_context_v1(tid);
 k:=population_top3000_202608_202609_v1.massive_archive_key_v1(t.family_id,t.partition_key,t.input_versions);
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.massive_archive_page_v1 WHERE archive_key=k AND next_url IS NULL)
 OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.massive_archive_page_v1 WHERE archive_key=k AND normalized_at IS NULL) THEN RAISE EXCEPTION 'MASSIVE_NORMALIZATION_INCOMPLETE';END IF;
 -- Version conflicts remain visible, and exact provider ratios remain source-backed candidates.
 WITH conflict_ids AS (
 SELECT source_record_key FROM population_top3000_202608_202609_v1.source_observation WHERE family_id='N03' AND partition_key=t.partition_key AND vintage_key=k GROUP BY source_record_key HAVING count(*)>1
), changed AS (
 SELECT o.unit_key,
 o.values_json||jsonb_build_object('share_unit_ratio_candidate',CASE WHEN field_state->>'split_from'='VALUE' AND field_state->>'split_to'='VALUE' THEN (values_json->>'split_to')::numeric/(values_json->>'split_from')::numeric ELSE NULL END) vals,
 o.field_state||jsonb_build_object('share_unit_ratio_candidate',CASE WHEN field_state->>'split_from'='VALUE' AND field_state->>'split_to'='VALUE' THEN 'VALUE' ELSE 'UNAVAILABLE' END,
 'provider_event_id',CASE WHEN o.source_record_key IN(SELECT source_record_key FROM conflict_ids) THEN 'CONFLICTING' ELSE o.field_state->>'provider_event_id' END) states,
 o.lineage||jsonb_build_object('ratio_formula','split_to / split_from; candidate only; primary mechanics not independently verified','price_adjustment_applied',false) lineage
 FROM population_top3000_202608_202609_v1.source_observation o WHERE family_id='N03' AND partition_key=t.partition_key AND vintage_key=k
 )
 UPDATE population_top3000_202608_202609_v1.source_observation o SET values_json=c.vals,field_state=c.states,lineage=c.lineage,
 record_sha256=encode(sha256(convert_to(jsonb_build_object('values',c.vals,'states',c.states,'lineage',c.lineage)::text,'UTF8')),'hex')
 FROM changed c WHERE o.family_id='N03' AND o.partition_key=t.partition_key AND o.unit_key=c.unit_key;
 SELECT count(*),encode(sha256(convert_to(coalesce(string_agg(unit_key||':'||record_sha256,E'\n' ORDER BY unit_key),''),'UTF8')),'hex') INTO n,h
 FROM population_top3000_202608_202609_v1.source_observation WHERE family_id='N03' AND partition_key=t.partition_key AND vintage_key=k;
 RETURN jsonb_build_object('state','COMPLETE','commit_receipt',jsonb_build_object('commit_key','massive-derived:'||k||':'||h,'output_sha256',h,'new_committed_units',n,'immutable_output_ids',jsonb_build_array(k),'input_versions',t.input_versions,'cohort_certified',false,'mechanics_independently_verified',false));
END $function$;

REVOKE ALL ON FUNCTION population_top3000_202608_202609_v1.massive_archive_key_v1(text,text,jsonb),population_top3000_202608_202609_v1.massive_actions_context_v1(uuid),population_top3000_202608_202609_v1.massive_actions_acquire_v1(uuid,text),population_top3000_202608_202609_v1.massive_actions_claim_v1(uuid,text),population_top3000_202608_202609_v1.massive_actions_commit_v1(uuid,text,jsonb),population_top3000_202608_202609_v1.massive_actions_failure_v1(uuid,text,jsonb),population_top3000_202608_202609_v1.massive_actions_date_v1(text),population_top3000_202608_202609_v1.massive_actions_normalize_v1(uuid,text),population_top3000_202608_202609_v1.massive_actions_derive_v1(uuid,text) FROM PUBLIC,anon,authenticated;
GRANT EXECUTE ON FUNCTION population_top3000_202608_202609_v1.massive_archive_key_v1(text,text,jsonb),population_top3000_202608_202609_v1.massive_actions_context_v1(uuid),population_top3000_202608_202609_v1.massive_actions_acquire_v1(uuid,text),population_top3000_202608_202609_v1.massive_actions_claim_v1(uuid,text),population_top3000_202608_202609_v1.massive_actions_commit_v1(uuid,text,jsonb),population_top3000_202608_202609_v1.massive_actions_failure_v1(uuid,text,jsonb),population_top3000_202608_202609_v1.massive_actions_date_v1(text),population_top3000_202608_202609_v1.massive_actions_normalize_v1(uuid,text),population_top3000_202608_202609_v1.massive_actions_derive_v1(uuid,text) TO postgres,service_role;
