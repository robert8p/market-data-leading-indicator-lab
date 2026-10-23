SET LOCAL statement_timeout='25s';SET LOCAL lock_timeout='2s';DO $root_guard$ BEGIN IF EXISTS(SELECT 1 FROM cron.job WHERE jobid=596 AND active) OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE status='LEASED') OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_provider_lease WHERE released_at IS NULL) OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton AND owner_id='top3000-recovery-20261023-root-4b1314bcb5c2' AND owner_fence=2 AND stop_generation=0 AND enabled AND NOT stopped AND admitted_until>clock_timestamp()) THEN RAISE EXCEPTION 'PAUSED_CURRENT_OWNER_NO_LEASES_REQUIRED';END IF;END $root_guard$;
-- Root-only installation draft. Separate 42-session compact source lane; no admission.
-- Existing one-day D02 functions, source documents and observations are untouched.
CREATE TABLE IF NOT EXISTS population_top3000_202608_202609_v1.d02_daily_source_v2 (
 source_day_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
 archive_key text NOT NULL UNIQUE,
 session_date date NOT NULL UNIQUE CHECK(session_date BETWEEN date '2026-08-03' AND date '2026-09-30'),
 calendar_scope_id text NOT NULL,
 calendar_manifest_sha256 text NOT NULL CHECK(calendar_manifest_sha256~'^[0-9a-f]{64}$'),
 input_versions jsonb NOT NULL,
 source_id text NOT NULL UNIQUE REFERENCES population_top3000_202608_202609_v1.source_document(source_id),
 source_sha256 text NOT NULL CHECK(source_sha256~'^[0-9a-f]{64}$'),
 response_bytes bigint NOT NULL CHECK(response_bytes BETWEEN 1 AND 8388608),
 result_count integer NOT NULL CHECK(result_count BETWEEN 0 AND 50000),
 retrieved_at timestamptz NOT NULL,
 acquisition_mode text NOT NULL CHECK(acquisition_mode IN('FETCHED_V2','REUSED_V1')),
 acquire_task_id uuid NOT NULL REFERENCES population_top3000_202608_202609_v1.pipeline_task(task_id),
 acquire_attempt_id uuid NOT NULL REFERENCES population_top3000_202608_202609_v1.pipeline_attempt(attempt_id),
 legacy_archive_key text,
 normalized_source_records integer NOT NULL DEFAULT 0 CHECK(normalized_source_records>=0),
 normalized_at timestamptz,
 verified_source_records integer NOT NULL DEFAULT 0 CHECK(verified_source_records>=0),
 source_verified_at timestamptz,
 source_summary jsonb,
 CHECK(normalized_source_records<=result_count AND verified_source_records<=normalized_source_records),
 CHECK((acquisition_mode='REUSED_V1')=(legacy_archive_key IS NOT NULL))
);
CREATE TABLE IF NOT EXISTS population_top3000_202608_202609_v1.d02_daily_bar_v2 (
 source_day_id bigint NOT NULL REFERENCES population_top3000_202608_202609_v1.d02_daily_source_v2(source_day_id),
 source_row integer NOT NULL CHECK(source_row BETWEEN 1 AND 50000),
 source_ticker text CHECK(length(source_ticker) BETWEEN 1 AND 64),
 provider_time_ms numeric,
 provider_ny_date date,
 provider_open numeric,provider_high numeric,provider_low numeric,provider_close numeric,
 share_volume numeric,trade_count numeric,provider_vwap numeric,
 missing_mask integer NOT NULL CHECK(missing_mask BETWEEN 0 AND 511),
 invalid_mask integer NOT NULL CHECK(invalid_mask BETWEEN 0 AND 511),
 structure_mask integer NOT NULL CHECK(structure_mask BETWEEN 0 AND 7),
 raw_record_sha256 bytea NOT NULL CHECK(octet_length(raw_record_sha256)=32),
 PRIMARY KEY(source_day_id,source_row)
);
CREATE INDEX IF NOT EXISTS d02_daily_bar_symbol_v2 ON population_top3000_202608_202609_v1.d02_daily_bar_v2(source_ticker,source_day_id);
CREATE TABLE IF NOT EXISTS population_top3000_202608_202609_v1.d02_daily_batch_v2 (
 source_day_id bigint NOT NULL REFERENCES population_top3000_202608_202609_v1.d02_daily_source_v2(source_day_id),
 first_source_row integer NOT NULL CHECK(first_source_row BETWEEN 1 AND 50000),
 last_source_row integer NOT NULL CHECK(last_source_row BETWEEN first_source_row AND first_source_row+999),
 source_rows integer NOT NULL CHECK(source_rows BETWEEN 1 AND 1000),
 typed_manifest_sha256 text NOT NULL CHECK(typed_manifest_sha256~'^[0-9a-f]{64}$'),
 parser_sha256 text NOT NULL CHECK(parser_sha256~'^[0-9a-f]{64}$'),
 normalizer_sha256 text NOT NULL CHECK(normalizer_sha256~'^[0-9a-f]{64}$'),
 normalized_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 verified_at timestamptz,
 verifier_sha256 text,
 PRIMARY KEY(source_day_id,first_source_row),
 CHECK(source_rows=last_source_row-first_source_row+1)
);
CREATE TABLE IF NOT EXISTS population_top3000_202608_202609_v1.d02_daily_legacy_adapter_v2 (
 stage text PRIMARY KEY CHECK(stage IN('ACQUIRE','NORMALIZE','DERIVE','VALIDATE','REPAIR')),
 function_oid oid NOT NULL,
 function_sha256 text NOT NULL CHECK(function_sha256~'^[0-9a-f]{64}$'),
 registration_evidence jsonb NOT NULL
);
ALTER TABLE population_top3000_202608_202609_v1.d02_daily_source_v2 ENABLE ROW LEVEL SECURITY;
ALTER TABLE population_top3000_202608_202609_v1.d02_daily_bar_v2 ENABLE ROW LEVEL SECURITY;
ALTER TABLE population_top3000_202608_202609_v1.d02_daily_batch_v2 ENABLE ROW LEVEL SECURITY;
ALTER TABLE population_top3000_202608_202609_v1.d02_daily_legacy_adapter_v2 ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON population_top3000_202608_202609_v1.d02_daily_source_v2,population_top3000_202608_202609_v1.d02_daily_bar_v2,population_top3000_202608_202609_v1.d02_daily_batch_v2,population_top3000_202608_202609_v1.d02_daily_legacy_adapter_v2 FROM PUBLIC,anon,authenticated;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.d02_daily_calendar_v2()
RETURNS jsonb LANGUAGE plpgsql STABLE SET search_path TO 'pg_catalog' AS $f$
DECLARE cal jsonb;date_hash text;n bigint;
BEGIN
 SELECT count(*),encode(sha256(convert_to(string_agg(session_date::text,E'\n' ORDER BY session_date),'UTF8')),'hex'),
 jsonb_agg(jsonb_build_object('date',session_date,'open',regular_open_utc,'close',regular_close_utc,'record_sha256',source_record_sha256,'receipt_sha256',source_receipt_sha256) ORDER BY session_date)
 INTO n,date_hash,cal FROM population_top3000_202608_202609_v1.catalog_calendar
 WHERE scope_id='corrected_company_scope_8e6ca1178f03822c' AND session_date BETWEEN date '2026-08-03' AND date '2026-09-30';
 IF n<>42 OR date_hash IS DISTINCT FROM '188cc82e93e2c3446d9fba86cec682b996913997f56a2263d3fd680475998368'
 OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.catalog_calendar WHERE scope_id='corrected_company_scope_8e6ca1178f03822c' AND session_date BETWEEN date '2026-08-03' AND date '2026-09-30'
 AND (qualification IS DISTINCT FROM 'PROVIDER_AND_NYSE_RULES_RECONCILED' OR source_record_sha256 IS DISTINCT FROM encode(sha256(convert_to(source_record_text,'UTF8')),'hex') OR source_receipt_sha256 IS DISTINCT FROM encode(sha256(convert_to(source_receipt_text,'UTF8')),'hex')))
 THEN RAISE EXCEPTION 'D02_V2_FROZEN_CALENDAR_CHANGED';END IF;
 RETURN jsonb_build_object('calendar_scope_id','corrected_company_scope_8e6ca1178f03822c','calendar_date_list_sha256',date_hash,'calendar_manifest_sha256',encode(sha256(convert_to(cal::text,'UTF8')),'hex'),'sessions',cal);
END $f$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.d02_daily_context_v2(p_task uuid)
RETURNS jsonb LANGUAGE plpgsql STABLE SET search_path TO 'pg_catalog' AS $f$
DECLARE t population_top3000_202608_202609_v1.pipeline_task%rowtype;cal jsonb;d date;k text;
BEGIN
 SELECT * INTO t FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=p_task;
 IF NOT FOUND OR t.family_id<>'D02' OR t.payload->>'kind' IS DISTINCT FROM 'MASSIVE_GROUPED_DAILY_V2'
 OR t.input_versions->>'scope_sha256' IS DISTINCT FROM '8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd'
 OR t.input_versions->>'source_contract_revision' IS DISTINCT FROM 'massive-daily-42-v2'
 OR t.input_versions->>'daily_bundle_sha256' IS DISTINCT FROM population_top3000_202608_202609_v1.d02_daily_bundle_sha256_v2()
 OR coalesce(t.payload->>'trading_date','')!~'^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN RAISE EXCEPTION 'D02_V2_SCOPE_OR_BUNDLE_NOT_ADMITTED';END IF;
 d:=(t.payload->>'trading_date')::date;cal:=population_top3000_202608_202609_v1.d02_daily_calendar_v2();
 IF to_char(d,'YYYY-MM-DD') IS DISTINCT FROM t.payload->>'trading_date'
 OR t.partition_key IS DISTINCT FROM 'daily-source-'||to_char(d,'YYYYMMDD')||'-raw-no-otc-v2'
 OR t.input_versions->>'calendar_manifest_sha256' IS DISTINCT FROM cal->>'calendar_manifest_sha256'
 OR NOT EXISTS(SELECT 1 FROM jsonb_array_elements(cal->'sessions') x WHERE x->>'date'=d::text)
 OR (d=date '2026-08-03' AND t.payload->>'source_mode' IS DISTINCT FROM 'REUSE_RETAINED_V1')
 OR (d<>date '2026-08-03' AND t.payload->>'source_mode' IS DISTINCT FROM 'FETCH')
 THEN RAISE EXCEPTION 'D02_V2_SESSION_OR_SOURCE_MODE_NOT_ADMITTED';END IF;
 k:='d02-daily-v2:'||encode(sha256(convert_to(jsonb_build_object('family','D02','partition',t.partition_key,'input_versions',t.input_versions)::text,'UTF8')),'hex');
 RETURN (cal-'sessions')||jsonb_build_object('state','READY','archive_key',k,'family_id','D02','partition_key',t.partition_key,'trading_date',d,'source_mode',t.payload->>'source_mode',
 'request_url','https://api.massive.com/v2/aggs/grouped/locale/us/market/stocks/'||d::text||'?adjusted=false&include_otc=false',
 'max_response_bytes',8388608,'max_runtime_ms',45000,'max_result_count',50000,'adjusted',false,'include_otc',false);
END $f$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.d02_daily_resource_guard_v2()
RETURNS bigint LANGUAGE plpgsql SET search_path TO 'pg_catalog' AS $f$
DECLARE admitted_bound bigint;actual_bytes bigint;
BEGIN
 SELECT (budget_evidence#>>'{resource_limits,max_relation_bytes}')::bigint INTO admitted_bound FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton;
 IF admitted_bound IS NULL OR admitted_bound<1 THEN RAISE EXCEPTION 'D02_V2_RESOURCE_BOUND_NOT_ADMITTED';END IF;
 SELECT coalesce(sum(pg_total_relation_size(c.oid)),0) INTO actual_bytes FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='population_top3000_202608_202609_v1' AND c.relkind IN('r','m');
 IF actual_bytes>admitted_bound THEN RAISE EXCEPTION 'D02_V2_MISSION_RELATION_CAP_EXCEEDED';END IF;
 RETURN actual_bytes;
END $f$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.d02_daily_read_source_v2(p_source_id text,p_date date,p_hash text,p_bytes bigint,p_count integer,p_retrieved_at timestamptz)
RETURNS jsonb LANGUAGE plpgsql STABLE SET search_path TO 'pg_catalog' AS $f$
DECLARE d population_top3000_202608_202609_v1.source_document%rowtype;b jsonb;field_name text;n numeric;
BEGIN
 SELECT * INTO d FROM population_top3000_202608_202609_v1.source_document WHERE source_id=p_source_id;
 IF NOT FOUND OR d.content_text IS NULL OR d.content_bytes IS DISTINCT FROM p_bytes OR p_bytes NOT BETWEEN 1 AND 8388608
 OR d.content_bytes IS DISTINCT FROM octet_length(d.content_text)::bigint OR d.source_sha256 IS DISTINCT FROM p_hash OR d.source_sha256 IS DISTINCT FROM encode(sha256(convert_to(d.content_text,'UTF8')),'hex')
 OR d.source_provider IS DISTINCT FROM 'Massive' OR d.retrieved_at IS DISTINCT FROM p_retrieved_at OR d.source_published_at IS NOT NULL OR d.historical_received_at IS NOT NULL
 OR d.source_uri IS DISTINCT FROM 'https://api.massive.com/v2/aggs/grouped/locale/us/market/stocks/'||p_date::text||'?adjusted=false&include_otc=false'
 OR d.evidence->>'wire_sha256' IS DISTINCT FROM p_hash OR (d.evidence->>'wire_bytes')::bigint IS DISTINCT FROM p_bytes OR (d.evidence->>'stored_content_bytes')::bigint IS DISTINCT FROM p_bytes
 OR d.evidence->>'trading_date' IS DISTINCT FROM p_date::text OR d.evidence->'adjusted' IS DISTINCT FROM 'false'::jsonb OR d.evidence->'include_otc' IS DISTINCT FROM 'false'::jsonb
 OR d.evidence->>'hash_representation' IS DISTINCT FROM 'EXACT_UTF8_APPLICATION_RESPONSE_BODY'
 OR d.license_policy IS DISTINCT FROM 'EXISTING_MASSIVE_SUBSCRIPTION_API_RESPONSE_INTERNAL_USE' THEN RAISE EXCEPTION 'D02_V2_RETAINED_SOURCE_INTEGRITY';END IF;
 b:=d.content_text::jsonb;
 IF b->>'status' IS DISTINCT FROM 'OK' OR b->'adjusted' IS DISTINCT FROM 'false'::jsonb OR jsonb_typeof(b->'results') IS DISTINCT FROM 'array'
 OR nullif(b->>'next_url','') IS NOT NULL OR jsonb_array_length(b->'results') IS DISTINCT FROM p_count OR p_count NOT BETWEEN 0 AND 50000
 OR EXISTS(SELECT 1 FROM jsonb_array_elements(b->'results') r WHERE jsonb_typeof(r) IS DISTINCT FROM 'object') THEN RAISE EXCEPTION 'D02_V2_SOURCE_ENVELOPE_COUNT_OR_ADJUSTMENT';END IF;
 FOREACH field_name IN ARRAY ARRAY['queryCount','resultsCount'] LOOP
  IF b ? field_name THEN
   IF jsonb_typeof(b->field_name) IS DISTINCT FROM 'number' THEN RAISE EXCEPTION 'D02_V2_SOURCE_COUNT_INVALID';END IF;
   n:=(b->>field_name)::numeric;
   IF n<0 OR n>9999999999 OR n<>trunc(n) OR (field_name='resultsCount' AND n<>p_count) THEN RAISE EXCEPTION 'D02_V2_SOURCE_COUNT_INVALID';END IF;
  END IF;
 END LOOP;
 RETURN b;
END $f$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.d02_daily_store_source_v2(p_attempt uuid,p_token text,p_source_id text,p_mode text)
RETURNS jsonb LANGUAGE plpgsql SET search_path TO 'pg_catalog' AS $f$
DECLARE tid uuid;t population_top3000_202608_202609_v1.pipeline_task%rowtype;c jsonb;d population_top3000_202608_202609_v1.source_document%rowtype;
 body jsonb;cnt integer;legacy_key text;fresh integer;dayrow population_top3000_202608_202609_v1.d02_daily_source_v2%rowtype;
BEGIN
 tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);
 SELECT * INTO t FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=tid;
 IF t.stage<>'ACQUIRE' THEN RAISE EXCEPTION 'D02_V2_STAGE_MISMATCH';END IF;
 c:=population_top3000_202608_202609_v1.d02_daily_context_v2(tid);
 PERFORM population_top3000_202608_202609_v1.d02_daily_resource_guard_v2();
 IF p_mode='REUSED_V1' THEN
  IF c->>'source_mode'<>'REUSE_RETAINED_V1' OR p_source_id IS DISTINCT FROM t.payload->>'retained_source_id' THEN RAISE EXCEPTION 'D02_V2_REUSE_SCOPE_MISMATCH';END IF;
  SELECT archive_key INTO legacy_key FROM population_top3000_202608_202609_v1.d02_grouped_archive_v1
  WHERE source_id=p_source_id AND source_sha256=t.input_versions->>'retained_aug3_source_sha256';
  IF NOT FOUND OR c->>'trading_date'<>'2026-08-03' OR t.input_versions->>'retained_aug3_source_sha256' IS DISTINCT FROM '787163eb9f6ffb408898c8a133c6ca63c822577187793d25c94b436f639bb97b' THEN RAISE EXCEPTION 'D02_V2_RETAINED_AUG3_RECEIPT_REQUIRED';END IF;
 ELSIF p_mode='FETCHED_V2' THEN
  IF c->>'source_mode'<>'FETCH' OR p_source_id IS DISTINCT FROM (c->>'archive_key')||':source' THEN RAISE EXCEPTION 'D02_V2_FETCH_SCOPE_MISMATCH';END IF;
 ELSE RAISE EXCEPTION 'D02_V2_SOURCE_MODE_INVALID';END IF;
 SELECT * INTO d FROM population_top3000_202608_202609_v1.source_document WHERE source_id=p_source_id;
 IF NOT FOUND THEN RAISE EXCEPTION 'D02_V2_SOURCE_DOCUMENT_REQUIRED';END IF;
 body:=d.content_text::jsonb;cnt:=jsonb_array_length(body->'results');
 PERFORM population_top3000_202608_202609_v1.d02_daily_read_source_v2(p_source_id,(c->>'trading_date')::date,d.source_sha256,d.content_bytes,cnt,d.retrieved_at);
 INSERT INTO population_top3000_202608_202609_v1.d02_daily_source_v2
 (archive_key,session_date,calendar_scope_id,calendar_manifest_sha256,input_versions,source_id,source_sha256,response_bytes,result_count,retrieved_at,acquisition_mode,acquire_task_id,acquire_attempt_id,legacy_archive_key)
 VALUES(c->>'archive_key',(c->>'trading_date')::date,c->>'calendar_scope_id',c->>'calendar_manifest_sha256',t.input_versions,p_source_id,d.source_sha256,d.content_bytes,cnt,d.retrieved_at,p_mode,tid,p_attempt,legacy_key)
 ON CONFLICT(archive_key) DO NOTHING;
 GET DIAGNOSTICS fresh=ROW_COUNT;
 SELECT * INTO dayrow FROM population_top3000_202608_202609_v1.d02_daily_source_v2 WHERE archive_key=c->>'archive_key';
 IF NOT FOUND OR dayrow.session_date IS DISTINCT FROM (c->>'trading_date')::date OR dayrow.calendar_scope_id IS DISTINCT FROM c->>'calendar_scope_id'
 OR dayrow.calendar_manifest_sha256 IS DISTINCT FROM c->>'calendar_manifest_sha256' OR dayrow.input_versions IS DISTINCT FROM t.input_versions
 OR dayrow.source_id IS DISTINCT FROM p_source_id OR dayrow.source_sha256 IS DISTINCT FROM d.source_sha256 OR dayrow.response_bytes IS DISTINCT FROM d.content_bytes OR dayrow.result_count IS DISTINCT FROM cnt OR dayrow.retrieved_at IS DISTINCT FROM d.retrieved_at
 OR dayrow.acquisition_mode IS DISTINCT FROM p_mode OR dayrow.acquire_task_id IS DISTINCT FROM tid OR dayrow.acquire_attempt_id IS DISTINCT FROM p_attempt OR dayrow.legacy_archive_key IS DISTINCT FROM legacy_key THEN RAISE EXCEPTION 'D02_V2_SOURCE_DAY_REPLAY_CONFLICT';END IF;
 PERFORM population_top3000_202608_202609_v1.d02_daily_resource_guard_v2();
 RETURN jsonb_build_object('source_day_id',dayrow.source_day_id,'archive_key',dayrow.archive_key,'source_id',p_source_id,'source_sha256',d.source_sha256,'source_records',cnt,'new_manifest_rows',fresh,'reused_without_provider_request',p_mode='REUSED_V1');
END $f$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.d02_daily_acquire_v2(p_attempt uuid,p_token text)
RETURNS jsonb LANGUAGE plpgsql SET search_path TO 'pg_catalog' AS $f$
DECLARE tid uuid;t population_top3000_202608_202609_v1.pipeline_task%rowtype;c jsonb;r jsonb;
BEGIN
 tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);
 SELECT * INTO t FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=tid;
 IF t.stage<>'ACQUIRE' THEN RAISE EXCEPTION 'D02_V2_STAGE_MISMATCH';END IF;
 c:=population_top3000_202608_202609_v1.d02_daily_context_v2(tid);
 IF c->>'source_mode'='REUSE_RETAINED_V1' THEN
  r:=population_top3000_202608_202609_v1.d02_daily_store_source_v2(p_attempt,p_token,t.payload->>'retained_source_id','REUSED_V1');
  RETURN jsonb_build_object('state','COMPLETE','commit_receipt',jsonb_build_object('commit_key','d02-daily-reused:'||(r->>'archive_key'),'output_sha256',r->>'source_sha256','new_committed_units',r->'new_manifest_rows','immutable_output_ids',jsonb_build_array(r->>'source_id'),'input_versions',t.input_versions,'source_records',r->'source_records','reused_without_provider_request',true,'company_population_verified',false));
 END IF;
 RETURN population_top3000_202608_202609_v1.pipeline_dispatch_edge_v1(p_attempt,p_token,'population-top3000-daily-v2');
END $f$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.d02_daily_claim_v2(p_attempt uuid,p_token text)
RETURNS jsonb LANGUAGE plpgsql SET search_path TO 'pg_catalog' AS $f$
DECLARE tid uuid;c jsonb;r jsonb;
BEGIN
 tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=tid AND stage='ACQUIRE') THEN RAISE EXCEPTION 'D02_V2_STAGE_MISMATCH';END IF;
 c:=population_top3000_202608_202609_v1.d02_daily_context_v2(tid);
 IF c->>'source_mode'<>'FETCH' THEN RAISE EXCEPTION 'D02_V2_RETAINED_DAY_MUST_NOT_FETCH';END IF;
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.d02_daily_source_v2 WHERE session_date=(c->>'trading_date')::date)
 OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_provider_lease l JOIN population_top3000_202608_202609_v1.pipeline_attempt a USING(attempt_id) WHERE a.task_id=tid)
 THEN RAISE EXCEPTION 'D02_V2_PRIOR_SOURCE_OR_REQUEST_REQUIRES_RECONCILIATION';END IF;
 r:=population_top3000_202608_202609_v1.pipeline_provider_reserve_v1(p_attempt,p_token,'Massive',8388608,0);
 IF r->>'state'<>'RESERVED' THEN RETURN population_top3000_202608_202609_v1.pipeline_finish_v1(p_attempt,p_token,jsonb_build_object('state','BLOCKED','blocker_code',CASE WHEN r->>'state'='WAIT' THEN 'DEPENDENCY_PENDING' ELSE 'PROVIDER_BUDGET_OR_RESOURCE_HOLD' END,'recheck_at',CASE WHEN r->>'state'='WAIT' THEN r->>'not_before' ELSE NULL END,'resumption_condition',jsonb_build_object('event','CONTROLLER_ADMISSION_OR_PROVIDER_SLOT_RECONCILED','receipt',r)));END IF;
 RETURN c||jsonb_build_object('state','RESERVED');
END $f$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.d02_daily_commit_v2(p_attempt uuid,p_token text,p_page jsonb)
RETURNS jsonb LANGUAGE plpgsql SET search_path TO 'pg_catalog' AS $f$
DECLARE tid uuid;t population_top3000_202608_202609_v1.pipeline_task%rowtype;c jsonb;txt text;b jsonb;h text;sid text;bytes bigint;cnt integer;at_time timestamptz;v_evidence jsonb;r jsonb;
BEGIN
 tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);
 SELECT * INTO t FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=tid;
 IF t.stage<>'ACQUIRE' THEN RAISE EXCEPTION 'D02_V2_STAGE_MISMATCH';END IF;
 c:=population_top3000_202608_202609_v1.d02_daily_context_v2(tid);
 PERFORM population_top3000_202608_202609_v1.d02_daily_resource_guard_v2();
 IF c->>'source_mode'<>'FETCH' OR jsonb_typeof(p_page) IS DISTINCT FROM 'object' OR p_page->>'archive_key' IS DISTINCT FROM c->>'archive_key'
 OR p_page->>'trading_date' IS DISTINCT FROM c->>'trading_date' OR p_page->>'request_url' IS DISTINCT FROM c->>'request_url' OR p_page->>'http_status' IS DISTINCT FROM '200'
 THEN RAISE EXCEPTION 'D02_V2_COMMIT_SCOPE_MISMATCH';END IF;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_provider_lease WHERE attempt_id=p_attempt AND provider='Massive' AND released_at IS NULL AND reserved_bytes=8388608) THEN RAISE EXCEPTION 'D02_V2_RESERVATION_REQUIRED';END IF;
 txt:=p_page->>'content_text';bytes:=octet_length(txt);
 IF txt IS NULL OR bytes NOT BETWEEN 1 AND 8388608 OR (p_page->>'wire_bytes')::bigint IS DISTINCT FROM bytes OR coalesce((p_page->>'elapsed_ms')::bigint,-1) NOT BETWEEN 0 AND 45000 THEN RAISE EXCEPTION 'D02_V2_RESPONSE_BOUND_INVALID';END IF;
 h:=encode(sha256(convert_to(txt,'UTF8')),'hex');
 IF h IS DISTINCT FROM p_page->>'source_sha256' OR h IS DISTINCT FROM p_page->>'wire_sha256' THEN RAISE EXCEPTION 'D02_V2_EXACT_WIRE_HASH_MISMATCH';END IF;
 b:=txt::jsonb;
 IF b->>'status' IS DISTINCT FROM 'OK' OR b->'adjusted' IS DISTINCT FROM 'false'::jsonb OR jsonb_typeof(b->'results') IS DISTINCT FROM 'array' OR nullif(b->>'next_url','') IS NOT NULL THEN RAISE EXCEPTION 'D02_V2_SOURCE_ENVELOPE_OR_ADJUSTMENT';END IF;
 cnt:=jsonb_array_length(b->'results');
 IF cnt NOT BETWEEN 0 AND 50000 OR cnt IS DISTINCT FROM (p_page->>'result_count')::integer THEN RAISE EXCEPTION 'D02_V2_SOURCE_COUNT_INVALID';END IF;
 IF txt~* '"(apiKey|api_key|access_token|authorization)"[[:space:]]*:' OR txt~* '[?&](apiKey|api_key|access_token)=' THEN RAISE EXCEPTION 'D02_V2_CREDENTIAL_ECHO';END IF;
 at_time:=(p_page->>'retrieved_at')::timestamptz;
 IF at_time IS NULL OR at_time<(SELECT started_at FROM population_top3000_202608_202609_v1.pipeline_attempt WHERE attempt_id=p_attempt)-interval '30 seconds' OR at_time>clock_timestamp()+interval '30 seconds' THEN RAISE EXCEPTION 'D02_V2_RETRIEVAL_CLOCK_INVALID';END IF;
 sid:=(c->>'archive_key')||':source';
 v_evidence:=jsonb_build_object('wire_sha256',h,'wire_bytes',bytes,'stored_content_bytes',bytes,'hash_representation','EXACT_UTF8_APPLICATION_RESPONSE_BODY','trading_date',c->>'trading_date','adjusted',false,'include_otc',false,'result_count',cnt,'provider_request_id',p_page->>'provider_request_id','regular_session_close_independently_verified',false,'top3000_company_membership_verified',false,'original_historical_receipt',false);
 INSERT INTO population_top3000_202608_202609_v1.source_document(source_id,source_sha256,source_uri,source_provider,retrieved_at,source_published_at,historical_received_at,content_text,content_bytes,evidence,license_policy)
 VALUES(sid,h,c->>'request_url','Massive',at_time,NULL,NULL,txt,bytes,v_evidence,'EXISTING_MASSIVE_SUBSCRIPTION_API_RESPONSE_INTERNAL_USE') ON CONFLICT(source_id) DO NOTHING;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.source_document d WHERE d.source_id=sid AND d.source_sha256=h AND d.content_text=txt AND d.content_bytes=bytes AND d.source_uri=c->>'request_url' AND d.source_provider='Massive' AND d.retrieved_at=at_time AND d.source_published_at IS NULL AND d.historical_received_at IS NULL AND d.evidence=v_evidence AND d.license_policy='EXISTING_MASSIVE_SUBSCRIPTION_API_RESPONSE_INTERNAL_USE') THEN RAISE EXCEPTION 'D02_V2_SOURCE_REVISION_CONFLICT';END IF;
 r:=population_top3000_202608_202609_v1.d02_daily_store_source_v2(p_attempt,p_token,sid,'FETCHED_V2');
 PERFORM population_top3000_202608_202609_v1.d02_daily_resource_guard_v2();
 PERFORM population_top3000_202608_202609_v1.pipeline_provider_release_v1(p_attempt,p_token,bytes,0,0);
 RETURN population_top3000_202608_202609_v1.pipeline_finish_v1(p_attempt,p_token,jsonb_build_object('state','COMPLETE','commit_receipt',jsonb_build_object('commit_key','d02-daily-source:'||(r->>'archive_key'),'output_sha256',h,'new_committed_units',r->'new_manifest_rows','immutable_output_ids',jsonb_build_array(sid),'input_versions',t.input_versions,'source_records',cnt,'empty_source_is_not_zero_volume',cnt=0,'company_population_verified',false)));
END $f$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.d02_daily_failure_v2(p_attempt uuid,p_token text,p_failure jsonb)
RETURNS jsonb LANGUAGE plpgsql SET search_path TO 'pg_catalog' AS $f$
DECLARE tid uuid;code text;class text;bytes bigint;retry integer;unknown_effect boolean;details jsonb;
BEGIN
 tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=tid AND family_id='D02' AND stage='ACQUIRE') THEN RAISE EXCEPTION 'D02_V2_STAGE_MISMATCH';END IF;
 PERFORM population_top3000_202608_202609_v1.d02_daily_context_v2(tid);
 code:=p_failure->>'code';class:=p_failure->>'failure_class';bytes:=(p_failure->>'received_bytes')::bigint;retry:=(p_failure->>'retry_after_seconds')::integer;
 IF code IS NULL OR code!~'^[A-Z0-9_]{1,80}$' OR class IS NULL OR class NOT IN('RATE_LIMIT','TRANSPORT_READ','AUTHENTICATION','PARSER','SOURCE_ABSENT') OR bytes IS NULL OR bytes NOT BETWEEN 0 AND 8388608 OR retry IS NULL OR retry NOT BETWEEN 0 AND 3600 THEN RAISE EXCEPTION 'D02_V2_FAILURE_RECEIPT_INVALID';END IF;
 details:=jsonb_build_object('code',code,'http_status',p_failure->'http_status','response_bytes_observed',bytes,'wire_sha256',p_failure->>'wire_sha256','fetch_started',p_failure->'fetch_started','raw_body_retained',false,'automatic_retry',false);
 unknown_effect:=coalesce((p_failure->>'fetch_started')::boolean,false) AND (p_failure->>'http_status' IS NULL OR code IN('DEADLINE','FETCH_FAILED','EMPTY_BODY') OR (p_failure->>'http_status'='200' AND p_failure->>'wire_sha256' IS NULL));
 IF unknown_effect THEN RETURN population_top3000_202608_202609_v1.pipeline_fail_v1(p_attempt,p_token,'UNCERTAIN_EFFECT','D02_V2:'||code,details,retry);END IF;
 PERFORM population_top3000_202608_202609_v1.pipeline_provider_release_v1(p_attempt,p_token,0,0,CASE WHEN class='RATE_LIMIT' THEN retry ELSE 0 END);
 RETURN population_top3000_202608_202609_v1.pipeline_finish_v1(p_attempt,p_token,jsonb_build_object('state','BLOCKED','blocker_code','D02_V2_'||code,'resumption_condition',jsonb_build_object('event','CONTROLLER_REVIEW_OF_DEFINITIVE_SOURCE_FAILURE','details',details,'automatic_retry',false)));
END $f$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.d02_daily_number_v2(p_value jsonb)
RETURNS numeric LANGUAGE sql IMMUTABLE SET search_path TO 'pg_catalog' AS $f$
 SELECT CASE WHEN jsonb_typeof(p_value)='number' AND length(p_value::text)<=96 THEN (p_value::text)::numeric ELSE NULL END;
$f$;

-- One row for every exact source-array occurrence, including duplicates and invalid records.
-- Masks: ticker=1,t=2,o=4,h=8,l=16,c=32,v=64,n=128,vw=256.
CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.d02_daily_parse_v2(p_body jsonb,p_date date,p_first integer,p_last integer)
RETURNS TABLE(source_row integer,source_ticker text,provider_time_ms numeric,provider_ny_date date,
 provider_open numeric,provider_high numeric,provider_low numeric,provider_close numeric,share_volume numeric,trade_count numeric,provider_vwap numeric,
 missing_mask integer,invalid_mask integer,structure_mask integer,raw_record_sha256 bytea)
LANGUAGE sql STABLE SET search_path TO 'pg_catalog' AS $f$
 WITH raw AS MATERIALIZED (
  SELECT e.value AS rec,e.ordinality::integer AS ord FROM jsonb_array_elements(p_body->'results') WITH ORDINALITY e
  WHERE e.ordinality BETWEEN p_first AND p_last AND p_first>=1 AND p_last<=p_first+999 AND p_last<=50000
 ), nums AS MATERIALIZED (
  SELECT r.*,CASE WHEN jsonb_typeof(rec->'T')='string' AND length(rec->>'T') BETWEEN 1 AND 64 THEN rec->>'T' ELSE NULL END AS symbol,
   population_top3000_202608_202609_v1.d02_daily_number_v2(rec->'t') AS tm,
   population_top3000_202608_202609_v1.d02_daily_number_v2(rec->'o') AS op,
   population_top3000_202608_202609_v1.d02_daily_number_v2(rec->'h') AS hi,
   population_top3000_202608_202609_v1.d02_daily_number_v2(rec->'l') AS lo,
   population_top3000_202608_202609_v1.d02_daily_number_v2(rec->'c') AS cl,
   population_top3000_202608_202609_v1.d02_daily_number_v2(rec->'v') AS vol,
   population_top3000_202608_202609_v1.d02_daily_number_v2(rec->'n') AS trades,
   population_top3000_202608_202609_v1.d02_daily_number_v2(rec->'vw') AS vwap FROM raw r
 ), dated AS MATERIALIZED (
  SELECT n.*,CASE WHEN tm BETWEEN 0 AND 4102444800000 AND tm=trunc(tm) THEN (to_timestamp((tm/1000)::double precision) AT TIME ZONE 'America/New_York')::date ELSE NULL END AS ny_date FROM nums n
 )
 SELECT ord,symbol,tm,ny_date,op,hi,lo,cl,vol,trades,vwap,
  (CASE WHEN rec->>'T' IS NULL OR rec->>'T'='' THEN 1 ELSE 0 END)+
  (CASE WHEN rec->>'t' IS NULL THEN 2 ELSE 0 END)+
  (CASE WHEN rec->>'o' IS NULL THEN 4 ELSE 0 END)+
  (CASE WHEN rec->>'h' IS NULL THEN 8 ELSE 0 END)+
  (CASE WHEN rec->>'l' IS NULL THEN 16 ELSE 0 END)+
  (CASE WHEN rec->>'c' IS NULL THEN 32 ELSE 0 END)+
  (CASE WHEN rec->>'v' IS NULL THEN 64 ELSE 0 END)+
  (CASE WHEN rec->>'n' IS NULL THEN 128 ELSE 0 END)+
  (CASE WHEN rec->>'vw' IS NULL THEN 256 ELSE 0 END),
  (CASE WHEN rec->>'T' IS NOT NULL AND rec->>'T'<>'' AND (symbol IS NULL OR symbol~'[[:cntrl:][:space:]]') THEN 1 ELSE 0 END)+
  (CASE WHEN rec->>'t' IS NOT NULL AND (tm IS NULL OR ny_date IS NULL) THEN 2 ELSE 0 END)+
  (CASE WHEN rec->>'o' IS NOT NULL AND (op IS NULL OR op<=0) THEN 4 ELSE 0 END)+
  (CASE WHEN rec->>'h' IS NOT NULL AND (hi IS NULL OR hi<=0) THEN 8 ELSE 0 END)+
  (CASE WHEN rec->>'l' IS NOT NULL AND (lo IS NULL OR lo<=0) THEN 16 ELSE 0 END)+
  (CASE WHEN rec->>'c' IS NOT NULL AND (cl IS NULL OR cl<=0) THEN 32 ELSE 0 END)+
  (CASE WHEN rec->>'v' IS NOT NULL AND (vol IS NULL OR vol<0) THEN 64 ELSE 0 END)+
  (CASE WHEN rec->>'n' IS NOT NULL AND (trades IS NULL OR trades<0 OR trades<>trunc(trades)) THEN 128 ELSE 0 END)+
  (CASE WHEN rec->>'vw' IS NOT NULL AND (vwap IS NULL OR vwap<=0) THEN 256 ELSE 0 END),
  (CASE WHEN op>0 AND hi>0 AND lo>0 AND cl>0 AND (lo>hi OR op<lo OR op>hi OR cl<lo OR cl>hi) THEN 1 ELSE 0 END)+
  (CASE WHEN ny_date IS NOT NULL AND ny_date<>p_date THEN 2 ELSE 0 END),
  sha256(convert_to(rec::text,'UTF8'))
 FROM dated ORDER BY ord;
$f$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.d02_daily_normalize_v2(p_attempt uuid,p_token text)
RETURNS jsonb LANGUAGE plpgsql SET search_path TO 'pg_catalog' AS $f$
DECLARE tid uuid;t population_top3000_202608_202609_v1.pipeline_task%rowtype;c jsonb;d population_top3000_202608_202609_v1.d02_daily_source_v2%rowtype;
 b jsonb;prior_row integer;last_row integer;new_rows bigint;expected_rows bigint;actual_rows bigint;expected_hash text;actual_hash text;parser_hash text;normalizer_hash text;part_evidence jsonb;
BEGIN
 tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);
 SELECT * INTO t FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=tid;
 IF t.stage<>'NORMALIZE' THEN RAISE EXCEPTION 'D02_V2_STAGE_MISMATCH';END IF;
 c:=population_top3000_202608_202609_v1.d02_daily_context_v2(tid);
 PERFORM population_top3000_202608_202609_v1.d02_daily_resource_guard_v2();
 SELECT * INTO d FROM population_top3000_202608_202609_v1.d02_daily_source_v2 WHERE archive_key=c->>'archive_key' FOR UPDATE;
 IF NOT FOUND OR d.input_versions IS DISTINCT FROM t.input_versions OR d.session_date IS DISTINCT FROM (c->>'trading_date')::date OR d.calendar_manifest_sha256 IS DISTINCT FROM c->>'calendar_manifest_sha256' THEN RAISE EXCEPTION 'D02_V2_SOURCE_DAY_INPUT_MISMATCH';END IF;
 b:=population_top3000_202608_202609_v1.d02_daily_read_source_v2(d.source_id,d.session_date,d.source_sha256,d.response_bytes,d.result_count,d.retrieved_at);
 prior_row:=coalesce((t.checkpoint->>'last_source_row')::integer,0);last_row:=least(prior_row+1000,d.result_count);
 IF prior_row<0 OR prior_row>d.result_count OR (prior_row>0 AND prior_row%1000<>0) OR d.normalized_source_records NOT IN(prior_row,last_row) THEN RAISE EXCEPTION 'D02_V2_NORMALIZE_CHECKPOINT_DIVERGED';END IF;
 parser_hash:=encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.d02_daily_parse_v2(jsonb,date,integer,integer)'::regprocedure),'UTF8')),'hex');
 normalizer_hash:=encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.d02_daily_normalize_v2(uuid,text)'::regprocedure),'UTF8')),'hex');
 new_rows:=0;
 IF last_row>prior_row THEN
  INSERT INTO population_top3000_202608_202609_v1.d02_daily_bar_v2
  (source_day_id,source_row,source_ticker,provider_time_ms,provider_ny_date,provider_open,provider_high,provider_low,provider_close,share_volume,trade_count,provider_vwap,missing_mask,invalid_mask,structure_mask,raw_record_sha256)
  SELECT d.source_day_id,p.* FROM population_top3000_202608_202609_v1.d02_daily_parse_v2(b,d.session_date,prior_row+1,last_row) p
  ON CONFLICT(source_day_id,source_row) DO NOTHING;
  GET DIAGNOSTICS new_rows=ROW_COUNT;
  SELECT count(*),encode(sha256(convert_to(string_agg(encode(sha256(convert_to(to_jsonb(p)::text,'UTF8')),'hex'),E'\n' ORDER BY p.source_row),'UTF8')),'hex') INTO expected_rows,expected_hash
  FROM population_top3000_202608_202609_v1.d02_daily_parse_v2(b,d.session_date,prior_row+1,last_row) p;
  SELECT count(*),encode(sha256(convert_to(string_agg(encode(sha256(convert_to((to_jsonb(v)-'source_day_id')::text,'UTF8')),'hex'),E'\n' ORDER BY v.source_row),'UTF8')),'hex') INTO actual_rows,actual_hash
  FROM population_top3000_202608_202609_v1.d02_daily_bar_v2 v WHERE v.source_day_id=d.source_day_id AND v.source_row BETWEEN prior_row+1 AND last_row;
  IF expected_rows<>last_row-prior_row OR actual_rows<>expected_rows OR actual_hash IS DISTINCT FROM expected_hash THEN RAISE EXCEPTION 'D02_V2_TYPED_BATCH_REPLAY_CONFLICT';END IF;
  INSERT INTO population_top3000_202608_202609_v1.d02_daily_batch_v2(source_day_id,first_source_row,last_source_row,source_rows,typed_manifest_sha256,parser_sha256,normalizer_sha256)
  VALUES(d.source_day_id,prior_row+1,last_row,last_row-prior_row,expected_hash,parser_hash,normalizer_hash) ON CONFLICT(source_day_id,first_source_row) DO NOTHING;
  IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.d02_daily_batch_v2 q WHERE q.source_day_id=d.source_day_id AND q.first_source_row=prior_row+1 AND q.last_source_row=last_row AND q.source_rows=last_row-prior_row AND q.typed_manifest_sha256=expected_hash AND q.parser_sha256=parser_hash AND q.normalizer_sha256=normalizer_hash) THEN RAISE EXCEPTION 'D02_V2_BATCH_PROOF_REPLAY_CONFLICT';END IF;
 ELSE
  expected_hash:=encode(sha256(convert_to('EMPTY_SOURCE_ARRAY','UTF8')),'hex');
  IF d.result_count<>0 OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.d02_daily_bar_v2 WHERE source_day_id=d.source_day_id) THEN RAISE EXCEPTION 'D02_V2_EMPTY_SOURCE_CONFLICT';END IF;
 END IF;
 UPDATE population_top3000_202608_202609_v1.d02_daily_source_v2 SET normalized_source_records=last_row,normalized_at=CASE WHEN last_row=result_count THEN coalesce(normalized_at,clock_timestamp()) ELSE NULL END WHERE source_day_id=d.source_day_id;
 part_evidence:=jsonb_build_object('basis','COMPACT_VENDOR_SOURCE_OCCURRENCES_ONLY','source_day_id',d.source_day_id,'source_id',d.source_id,'source_sha256',d.source_sha256,'source_records',d.result_count,'company_day_denominator_verified',false,'company_membership_verified',false,'provider_aggregate_window_position','UNVERIFIED','regular_close_independently_verified',false,'original_historical_receipt',false,'empty_source_is_not_zero_volume',true);
 INSERT INTO population_top3000_202608_202609_v1.pipeline_partition(family_id,partition_key,time_role,denominator_state,denominator_evidence,producer_revision,immutable_input_versions,publication_eligible)
 VALUES('D02',t.partition_key,'TARGET','PROVISIONAL',part_evidence,'massive-daily-42-v2',t.input_versions,false) ON CONFLICT(family_id,partition_key) DO NOTHING;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_partition p WHERE p.family_id='D02' AND p.partition_key=t.partition_key AND p.time_role='TARGET' AND p.denominator_state='PROVISIONAL' AND p.denominator_evidence=part_evidence AND p.producer_revision='massive-daily-42-v2' AND p.immutable_input_versions=t.input_versions AND NOT p.publication_eligible AND p.expected_units IS NULL) THEN RAISE EXCEPTION 'D02_V2_PARTITION_REPLAY_CONFLICT';END IF;
 PERFORM population_top3000_202608_202609_v1.d02_daily_resource_guard_v2();
 RETURN jsonb_build_object('state',CASE WHEN last_row=d.result_count THEN 'COMPLETE' ELSE 'CONTINUE' END,'checkpoint',jsonb_build_object('last_source_row',last_row),
 'commit_receipt',jsonb_build_object('commit_key','d02-daily-normalize:'||d.archive_key||':'||prior_row::text||':'||last_row::text,'output_sha256',expected_hash,'new_committed_units',new_rows,'immutable_output_ids',jsonb_build_array(d.source_id,d.source_day_id),'input_versions',t.input_versions,'source_occurrences',last_row-prior_row,'company_population_verified',false,'typed_source_quality_only',true));
END $f$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.d02_daily_derive_v2(p_attempt uuid,p_token text)
RETURNS jsonb LANGUAGE plpgsql SET search_path TO 'pg_catalog' AS $f$
DECLARE tid uuid;t population_top3000_202608_202609_v1.pipeline_task%rowtype;c jsonb;d population_top3000_202608_202609_v1.d02_daily_source_v2%rowtype;
 proof population_top3000_202608_202609_v1.d02_daily_batch_v2%rowtype;b jsonb;prior_row integer;last_row integer;actual_rows bigint;expected_rows bigint;actual_hash text;expected_hash text;verifier_hash text;parser_hash text;normalizer_hash text;new_verified bigint;summary_json jsonb;duplicate_summary jsonb;
BEGIN
 tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);
 SELECT * INTO t FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=tid;
 IF t.stage<>'DERIVE' THEN RAISE EXCEPTION 'D02_V2_STAGE_MISMATCH';END IF;
 c:=population_top3000_202608_202609_v1.d02_daily_context_v2(tid);
 PERFORM population_top3000_202608_202609_v1.d02_daily_resource_guard_v2();
 SELECT * INTO d FROM population_top3000_202608_202609_v1.d02_daily_source_v2 WHERE archive_key=c->>'archive_key' FOR UPDATE;
 IF NOT FOUND OR d.input_versions IS DISTINCT FROM t.input_versions OR d.session_date IS DISTINCT FROM (c->>'trading_date')::date OR d.normalized_at IS NULL OR d.normalized_source_records<>d.result_count THEN RAISE EXCEPTION 'D02_V2_COMPLETE_SOURCE_NORMALIZATION_REQUIRED';END IF;
 b:=population_top3000_202608_202609_v1.d02_daily_read_source_v2(d.source_id,d.session_date,d.source_sha256,d.response_bytes,d.result_count,d.retrieved_at);
 prior_row:=coalesce((t.checkpoint->>'last_verified_row')::integer,0);last_row:=least(prior_row+1000,d.result_count);
 IF prior_row<0 OR prior_row>d.result_count OR (prior_row>0 AND prior_row%1000<>0) OR d.verified_source_records NOT IN(prior_row,last_row) THEN RAISE EXCEPTION 'D02_V2_VERIFY_CHECKPOINT_DIVERGED';END IF;
 parser_hash:=encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.d02_daily_parse_v2(jsonb,date,integer,integer)'::regprocedure),'UTF8')),'hex');
 normalizer_hash:=encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.d02_daily_normalize_v2(uuid,text)'::regprocedure),'UTF8')),'hex');
 verifier_hash:=encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.d02_daily_derive_v2(uuid,text)'::regprocedure),'UTF8')),'hex');new_verified:=0;
 IF last_row>prior_row THEN
  SELECT * INTO proof FROM population_top3000_202608_202609_v1.d02_daily_batch_v2 WHERE source_day_id=d.source_day_id AND first_source_row=prior_row+1 FOR UPDATE;
  IF NOT FOUND OR proof.last_source_row<>last_row OR proof.source_rows<>last_row-prior_row OR proof.parser_sha256<>parser_hash OR proof.normalizer_sha256<>normalizer_hash OR (proof.verified_at IS NOT NULL AND proof.verifier_sha256 IS DISTINCT FROM verifier_hash) THEN RAISE EXCEPTION 'D02_V2_BATCH_PROOF_VERSION_CONFLICT';END IF;
  SELECT count(*),encode(sha256(convert_to(string_agg(encode(sha256(convert_to(to_jsonb(p)::text,'UTF8')),'hex'),E'\n' ORDER BY p.source_row),'UTF8')),'hex') INTO expected_rows,expected_hash
  FROM population_top3000_202608_202609_v1.d02_daily_parse_v2(b,d.session_date,prior_row+1,last_row) p;
  SELECT count(*),encode(sha256(convert_to(string_agg(encode(sha256(convert_to((to_jsonb(v)-'source_day_id')::text,'UTF8')),'hex'),E'\n' ORDER BY v.source_row),'UTF8')),'hex') INTO actual_rows,actual_hash
  FROM population_top3000_202608_202609_v1.d02_daily_bar_v2 v WHERE v.source_day_id=d.source_day_id AND v.source_row BETWEEN prior_row+1 AND last_row;
  IF expected_rows<>last_row-prior_row OR actual_rows<>expected_rows OR actual_hash IS DISTINCT FROM expected_hash OR proof.typed_manifest_sha256 IS DISTINCT FROM expected_hash THEN RAISE EXCEPTION 'D02_V2_SOURCE_TO_TYPED_RECONCILIATION_FAILED';END IF;
  IF proof.verified_at IS NULL THEN
   UPDATE population_top3000_202608_202609_v1.d02_daily_batch_v2 SET verified_at=clock_timestamp(),verifier_sha256=verifier_hash WHERE source_day_id=d.source_day_id AND first_source_row=prior_row+1;
   new_verified:=last_row-prior_row;
  END IF;
 ELSE
  expected_hash:=encode(sha256(convert_to('EMPTY_SOURCE_ARRAY','UTF8')),'hex');
  IF d.result_count<>0 OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.d02_daily_bar_v2 WHERE source_day_id=d.source_day_id) THEN RAISE EXCEPTION 'D02_V2_EMPTY_SOURCE_CONFLICT';END IF;
 END IF;
 IF last_row=d.result_count THEN
  IF (SELECT coalesce(sum(source_rows),0) FROM population_top3000_202608_202609_v1.d02_daily_batch_v2 WHERE source_day_id=d.source_day_id AND verified_at IS NOT NULL)<>d.result_count THEN RAISE EXCEPTION 'D02_V2_ALL_BATCHES_NOT_VERIFIED';END IF;
  -- Reconcile every current typed batch at terminal time; prior proof timestamps alone are insufficient.
  IF EXISTS(
   WITH actual AS MATERIALIZED (
    SELECT ((v.source_row-1)/1000)*1000+1 AS first_source_row,min(v.source_row) AS minimum_source_row,max(v.source_row) AS last_source_row,count(*) AS source_rows,
     encode(sha256(convert_to(string_agg(encode(sha256(convert_to((to_jsonb(v)-'source_day_id')::text,'UTF8')),'hex'),E'\n' ORDER BY v.source_row),'UTF8')),'hex') AS typed_manifest_sha256
    FROM population_top3000_202608_202609_v1.d02_daily_bar_v2 v WHERE v.source_day_id=d.source_day_id GROUP BY ((v.source_row-1)/1000)*1000+1
   ),saved AS MATERIALIZED (
    SELECT * FROM population_top3000_202608_202609_v1.d02_daily_batch_v2 WHERE source_day_id=d.source_day_id
   )
   SELECT 1 FROM actual x FULL JOIN saved y USING(first_source_row)
   WHERE x.first_source_row IS NULL OR y.first_source_row IS NULL OR x.minimum_source_row<>x.first_source_row OR x.last_source_row<>least(x.first_source_row+999,d.result_count)
    OR x.source_rows<>x.last_source_row-x.first_source_row+1 OR x.last_source_row<>y.last_source_row OR x.source_rows<>y.source_rows
    OR x.typed_manifest_sha256 IS DISTINCT FROM y.typed_manifest_sha256 OR y.verified_at IS NULL OR y.parser_sha256 IS DISTINCT FROM parser_hash OR y.normalizer_sha256 IS DISTINCT FROM normalizer_hash OR y.verifier_sha256 IS DISTINCT FROM verifier_hash
  ) THEN RAISE EXCEPTION 'D02_V2_TERMINAL_CURRENT_BATCH_PROOF_MISMATCH';END IF;
  SELECT jsonb_build_object('raw_occurrences',count(*),'distinct_nonnull_source_tickers',count(DISTINCT source_ticker),'rows_with_missing_fields',count(*) FILTER(WHERE missing_mask<>0),'rows_with_invalid_fields',count(*) FILTER(WHERE invalid_mask<>0),'rows_with_structural_conflicts',count(*) FILTER(WHERE structure_mask<>0),'reported_zero_volume_rows',count(*) FILTER(WHERE share_volume=0),'missing_volume_rows',count(*) FILTER(WHERE (missing_mask&64)<>0),'invalid_volume_rows',count(*) FILTER(WHERE (invalid_mask&64)<>0),'minimum_provider_ny_date',min(provider_ny_date),'maximum_provider_ny_date',max(provider_ny_date))
  INTO summary_json FROM population_top3000_202608_202609_v1.d02_daily_bar_v2 WHERE source_day_id=d.source_day_id;
  IF (summary_json->>'raw_occurrences')::integer<>d.result_count THEN RAISE EXCEPTION 'D02_V2_TOTAL_OCCURRENCE_COUNT_MISMATCH';END IF;
  SELECT jsonb_build_object('repeated_ticker_groups',count(*) FILTER(WHERE occurrences>1),'conflicting_ticker_groups',count(*) FILTER(WHERE versions>1),'duplicate_identical_occurrences',coalesce(sum(occurrences-versions),0)) INTO duplicate_summary
  FROM (SELECT source_ticker,count(*) AS occurrences,count(DISTINCT raw_record_sha256) AS versions FROM population_top3000_202608_202609_v1.d02_daily_bar_v2 WHERE source_day_id=d.source_day_id AND source_ticker IS NOT NULL GROUP BY source_ticker) q;
  summary_json:=summary_json||duplicate_summary||jsonb_build_object('source_sha256',d.source_sha256,'source_records_reconciled',true,'source_reconciliation_is_not_independent_price_accuracy',true,'company_population_verified',false,'company_day_denominator_verified',false,'provider_aggregate_window_position','UNVERIFIED_NOT_PUBLICATION_TIME','regular_session_close_independently_verified',false,'original_historical_receipt',false,'empty_source_is_not_zero_volume',d.result_count=0,'parser_sha256',parser_hash,'normalizer_sha256',normalizer_hash,'verifier_sha256',verifier_hash);
  IF d.source_summary IS NOT NULL AND d.source_summary IS DISTINCT FROM summary_json THEN RAISE EXCEPTION 'D02_V2_SOURCE_SUMMARY_REPLAY_CONFLICT';END IF;
 END IF;
 UPDATE population_top3000_202608_202609_v1.d02_daily_source_v2 SET verified_source_records=last_row,source_verified_at=CASE WHEN last_row=result_count THEN coalesce(source_verified_at,clock_timestamp()) ELSE NULL END,source_summary=CASE WHEN last_row=result_count THEN summary_json ELSE source_summary END WHERE source_day_id=d.source_day_id;
 PERFORM population_top3000_202608_202609_v1.d02_daily_resource_guard_v2();
 RETURN jsonb_build_object('state',CASE WHEN last_row=d.result_count THEN 'COMPLETE' ELSE 'CONTINUE' END,'checkpoint',jsonb_build_object('last_verified_row',last_row),
 'commit_receipt',jsonb_build_object('commit_key','d02-daily-reconcile:'||d.archive_key||':'||prior_row::text||':'||last_row::text,'output_sha256',expected_hash,'new_committed_units',new_verified,'immutable_output_ids',jsonb_build_array(d.source_id,d.source_day_id),'input_versions',t.input_versions,'source_occurrences_verified',last_row-prior_row,'company_population_verified',false,'independent_price_accuracy_verified',false));
END $f$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.d02_daily_quality_hold_v2(p_attempt uuid,p_token text)
RETURNS jsonb LANGUAGE plpgsql SET search_path TO 'pg_catalog' AS $f$
DECLARE tid uuid;t population_top3000_202608_202609_v1.pipeline_task%rowtype;c jsonb;
BEGIN
 tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);
 SELECT * INTO t FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=tid;
 IF t.stage NOT IN('VALIDATE','REPAIR') THEN RAISE EXCEPTION 'D02_V2_STAGE_MISMATCH';END IF;
 c:=population_top3000_202608_202609_v1.d02_daily_context_v2(tid);
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.d02_daily_source_v2 WHERE archive_key=c->>'archive_key' AND source_verified_at IS NOT NULL AND result_count=verified_source_records AND input_versions=t.input_versions) THEN RAISE EXCEPTION 'D02_V2_SOURCE_RECONCILIATION_REQUIRED';END IF;
 RETURN jsonb_build_object('state','BLOCKED','blocker_code','D02_V2_COMPANY_DAY_DENOMINATOR_AND_CLOSE_SEMANTICS_REQUIRED','resumption_condition',jsonb_build_object('event','REVIEWED_COMPANY_DAY_CENSUS_AND_PRICE_SEMANTICS_AVAILABLE','required_evidence',jsonb_build_array('FROZEN_3000_COMPANY_UNIVERSE_AND_HISTORICAL_SECURITY_MAPPING','INDEPENDENT_EXPECTED_COMPANY_SESSION_CENSUS_AND_ABSENCE_RULES','VERIFIED_AGGREGATE_SESSION_AND_CLOSE_SEMANTICS','INDEPENDENT_PRICE_AND_CORPORATE_ACTION_RECONCILIATION','ORIGINAL_RECEIPT_OR_EXPLICIT_ARCHIVED_AVAILABILITY_POLICY'),'raw_source_completion_is_not_company_coverage',true,'preserve_typed_occurrences_and_exact_raw_sources',true,'no_automatic_provider_retry',true));
END $f$;

-- Existing one-date tasks continue through the root-captured, hash-bound adapters.
CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.d02_daily_route_v2(p_attempt uuid,p_token text,p_stage text)
RETURNS jsonb LANGUAGE plpgsql SET search_path TO 'pg_catalog' AS $f$
DECLARE tid uuid;t population_top3000_202608_202609_v1.pipeline_task%rowtype;l population_top3000_202608_202609_v1.d02_daily_legacy_adapter_v2%rowtype;r jsonb;n text;fn text;
BEGIN
 tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);
 SELECT * INTO t FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=tid;
 IF t.family_id<>'D02' OR t.stage IS DISTINCT FROM p_stage OR p_stage NOT IN('ACQUIRE','NORMALIZE','DERIVE','VALIDATE','REPAIR') THEN RAISE EXCEPTION 'D02_V2_ROUTE_STAGE_MISMATCH';END IF;
 IF t.payload->>'kind'='MASSIVE_GROUPED_DAILY_V2' THEN
  CASE p_stage
   WHEN 'ACQUIRE' THEN RETURN population_top3000_202608_202609_v1.d02_daily_acquire_v2(p_attempt,p_token);
   WHEN 'NORMALIZE' THEN RETURN population_top3000_202608_202609_v1.d02_daily_normalize_v2(p_attempt,p_token);
   WHEN 'DERIVE' THEN RETURN population_top3000_202608_202609_v1.d02_daily_derive_v2(p_attempt,p_token);
   ELSE RETURN population_top3000_202608_202609_v1.d02_daily_quality_hold_v2(p_attempt,p_token);
  END CASE;
 END IF;
 IF t.partition_key IS DISTINCT FROM 'grouped-20260803-raw-no-otc' OR t.input_versions->>'source_contract_revision' IS DISTINCT FROM 'massive-grouped-20260803-v1' THEN RAISE EXCEPTION 'D02_V2_UNKNOWN_LEGACY_ROUTE';END IF;
 SELECT * INTO l FROM population_top3000_202608_202609_v1.d02_daily_legacy_adapter_v2 WHERE stage=p_stage;
 IF NOT FOUND OR l.function_sha256 IS DISTINCT FROM encode(sha256(convert_to(pg_get_functiondef(l.function_oid),'UTF8')),'hex') THEN RAISE EXCEPTION 'D02_V2_LEGACY_ADAPTER_HASH_CHANGED';END IF;
 SELECT ns.nspname,p.proname INTO n,fn FROM pg_proc p JOIN pg_namespace ns ON ns.oid=p.pronamespace WHERE p.oid=l.function_oid AND p.prorettype='jsonb'::regtype AND p.pronargs=2 AND p.proargtypes[0]='uuid'::regtype AND p.proargtypes[1]='text'::regtype;
 IF n IS DISTINCT FROM 'population_top3000_202608_202609_v1' OR fn LIKE 'd02_daily_%' THEN RAISE EXCEPTION 'D02_V2_INVALID_LEGACY_FUNCTION';END IF;
 EXECUTE format('SELECT %I.%I($1,$2)',n,fn) INTO r USING p_attempt,p_token;
 RETURN r;
END $f$;
CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.d02_daily_route_acquire_v2(p_attempt uuid,p_token text) RETURNS jsonb LANGUAGE sql SET search_path TO 'pg_catalog' AS $f$ SELECT population_top3000_202608_202609_v1.d02_daily_route_v2(p_attempt,p_token,'ACQUIRE'); $f$;
CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.d02_daily_route_normalize_v2(p_attempt uuid,p_token text) RETURNS jsonb LANGUAGE sql SET search_path TO 'pg_catalog' AS $f$ SELECT population_top3000_202608_202609_v1.d02_daily_route_v2(p_attempt,p_token,'NORMALIZE'); $f$;
CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.d02_daily_route_derive_v2(p_attempt uuid,p_token text) RETURNS jsonb LANGUAGE sql SET search_path TO 'pg_catalog' AS $f$ SELECT population_top3000_202608_202609_v1.d02_daily_route_v2(p_attempt,p_token,'DERIVE'); $f$;
CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.d02_daily_route_validate_v2(p_attempt uuid,p_token text) RETURNS jsonb LANGUAGE sql SET search_path TO 'pg_catalog' AS $f$ SELECT population_top3000_202608_202609_v1.d02_daily_route_v2(p_attempt,p_token,'VALIDATE'); $f$;
CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.d02_daily_route_repair_v2(p_attempt uuid,p_token text) RETURNS jsonb LANGUAGE sql SET search_path TO 'pg_catalog' AS $f$ SELECT population_top3000_202608_202609_v1.d02_daily_route_v2(p_attempt,p_token,'REPAIR'); $f$;

CREATE OR REPLACE VIEW population_top3000_202608_202609_v1.d02_daily_provisional_v2 AS
 SELECT d.session_date,d.source_day_id,b.source_row,b.source_ticker,b.provider_open,b.provider_high,b.provider_low,b.provider_close,b.share_volume,b.trade_count,b.provider_vwap,b.provider_time_ms,b.provider_ny_date,b.missing_mask,b.invalid_mask,b.structure_mask,
 encode(b.raw_record_sha256,'hex') AS raw_record_sha256,d.source_id,d.source_sha256,d.retrieved_at,d.acquisition_mode,d.source_verified_at,
 NULL::text AS company_id,NULL::text AS security_id,NULL::timestamptz AS available_at,false AS publication_eligible,false AS company_population_verified,
 'PROVIDER_AGGREGATE_TIMESTAMP_WINDOW_POSITION_UNVERIFIED_NOT_PUBLICATION_TIME'::text AS timestamp_semantics,
 'ARCHIVED_VENDOR_SOURCE_OCCURRENCE_PROVISIONAL'::text AS quality_status
 FROM population_top3000_202608_202609_v1.d02_daily_source_v2 d JOIN population_top3000_202608_202609_v1.d02_daily_bar_v2 b USING(source_day_id);
REVOKE ALL ON population_top3000_202608_202609_v1.d02_daily_provisional_v2 FROM PUBLIC,anon,authenticated;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.d02_daily_bundle_sha256_v2()
RETURNS text LANGUAGE sql STABLE SET search_path TO 'pg_catalog' AS $f$
 SELECT encode(sha256(convert_to(jsonb_object_agg(fn,encode(sha256(convert_to(pg_get_functiondef(('population_top3000_202608_202609_v1.'||fn)::regprocedure),'UTF8')),'hex'))::text,'UTF8')),'hex')
 FROM (VALUES ('d02_daily_calendar_v2()'),('d02_daily_context_v2(uuid)'),('d02_daily_resource_guard_v2()'),('d02_daily_read_source_v2(text,date,text,bigint,integer,timestamp with time zone)'),('d02_daily_store_source_v2(uuid,text,text,text)'),('d02_daily_acquire_v2(uuid,text)'),('d02_daily_claim_v2(uuid,text)'),('d02_daily_commit_v2(uuid,text,jsonb)'),('d02_daily_failure_v2(uuid,text,jsonb)'),('d02_daily_number_v2(jsonb)'),('d02_daily_parse_v2(jsonb,date,integer,integer)'),('d02_daily_normalize_v2(uuid,text)'),('d02_daily_derive_v2(uuid,text)'),('d02_daily_quality_hold_v2(uuid,text)'),('d02_daily_route_v2(uuid,text,text)'),('d02_daily_route_acquire_v2(uuid,text)'),('d02_daily_route_normalize_v2(uuid,text)'),('d02_daily_route_derive_v2(uuid,text)'),('d02_daily_route_validate_v2(uuid,text)'),('d02_daily_route_repair_v2(uuid,text)'),('d02_daily_bundle_sha256_v2()')) f(fn);
$f$;

REVOKE ALL ON FUNCTION population_top3000_202608_202609_v1.d02_daily_calendar_v2(),population_top3000_202608_202609_v1.d02_daily_context_v2(uuid),population_top3000_202608_202609_v1.d02_daily_resource_guard_v2(),population_top3000_202608_202609_v1.d02_daily_read_source_v2(text,date,text,bigint,integer,timestamp with time zone),population_top3000_202608_202609_v1.d02_daily_store_source_v2(uuid,text,text,text),population_top3000_202608_202609_v1.d02_daily_acquire_v2(uuid,text),population_top3000_202608_202609_v1.d02_daily_claim_v2(uuid,text),population_top3000_202608_202609_v1.d02_daily_commit_v2(uuid,text,jsonb),population_top3000_202608_202609_v1.d02_daily_failure_v2(uuid,text,jsonb),population_top3000_202608_202609_v1.d02_daily_number_v2(jsonb),population_top3000_202608_202609_v1.d02_daily_parse_v2(jsonb,date,integer,integer),population_top3000_202608_202609_v1.d02_daily_normalize_v2(uuid,text),population_top3000_202608_202609_v1.d02_daily_derive_v2(uuid,text),population_top3000_202608_202609_v1.d02_daily_quality_hold_v2(uuid,text),population_top3000_202608_202609_v1.d02_daily_route_v2(uuid,text,text),population_top3000_202608_202609_v1.d02_daily_route_acquire_v2(uuid,text),population_top3000_202608_202609_v1.d02_daily_route_normalize_v2(uuid,text),population_top3000_202608_202609_v1.d02_daily_route_derive_v2(uuid,text),population_top3000_202608_202609_v1.d02_daily_route_validate_v2(uuid,text),population_top3000_202608_202609_v1.d02_daily_route_repair_v2(uuid,text),population_top3000_202608_202609_v1.d02_daily_bundle_sha256_v2() FROM PUBLIC,anon,authenticated;
GRANT EXECUTE ON FUNCTION population_top3000_202608_202609_v1.d02_daily_calendar_v2(),population_top3000_202608_202609_v1.d02_daily_context_v2(uuid),population_top3000_202608_202609_v1.d02_daily_resource_guard_v2(),population_top3000_202608_202609_v1.d02_daily_read_source_v2(text,date,text,bigint,integer,timestamp with time zone),population_top3000_202608_202609_v1.d02_daily_store_source_v2(uuid,text,text,text),population_top3000_202608_202609_v1.d02_daily_acquire_v2(uuid,text),population_top3000_202608_202609_v1.d02_daily_claim_v2(uuid,text),population_top3000_202608_202609_v1.d02_daily_commit_v2(uuid,text,jsonb),population_top3000_202608_202609_v1.d02_daily_failure_v2(uuid,text,jsonb),population_top3000_202608_202609_v1.d02_daily_number_v2(jsonb),population_top3000_202608_202609_v1.d02_daily_parse_v2(jsonb,date,integer,integer),population_top3000_202608_202609_v1.d02_daily_normalize_v2(uuid,text),population_top3000_202608_202609_v1.d02_daily_derive_v2(uuid,text),population_top3000_202608_202609_v1.d02_daily_quality_hold_v2(uuid,text),population_top3000_202608_202609_v1.d02_daily_route_v2(uuid,text,text),population_top3000_202608_202609_v1.d02_daily_route_acquire_v2(uuid,text),population_top3000_202608_202609_v1.d02_daily_route_normalize_v2(uuid,text),population_top3000_202608_202609_v1.d02_daily_route_derive_v2(uuid,text),population_top3000_202608_202609_v1.d02_daily_route_validate_v2(uuid,text),population_top3000_202608_202609_v1.d02_daily_route_repair_v2(uuid,text),population_top3000_202608_202609_v1.d02_daily_bundle_sha256_v2() TO postgres,service_role;
