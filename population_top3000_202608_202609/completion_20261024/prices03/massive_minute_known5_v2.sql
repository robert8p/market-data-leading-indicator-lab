SET check_function_bodies=off;
-- Separately versioned five-instrument Sep9 source pilot. Root deployment only after independent review.
-- Existing v1 code, archive, counters and blocked acceptance remain untouched.

CREATE TABLE IF NOT EXISTS population_top3000_202608_202609_v1.massive_minute_page_v2 (LIKE population_top3000_202608_202609_v1.massive_minute_page_v1 INCLUDING ALL);
ALTER TABLE population_top3000_202608_202609_v1.massive_minute_page_v2 ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON population_top3000_202608_202609_v1.massive_minute_page_v2 FROM PUBLIC,anon,authenticated;

CREATE TABLE IF NOT EXISTS population_top3000_202608_202609_v1.massive_minute_bar_v2 (LIKE population_top3000_202608_202609_v1.massive_minute_bar_v1 INCLUDING ALL);
ALTER TABLE population_top3000_202608_202609_v1.massive_minute_bar_v2 ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON population_top3000_202608_202609_v1.massive_minute_bar_v2 FROM PUBLIC,anon,authenticated;

CREATE TABLE IF NOT EXISTS population_top3000_202608_202609_v1.massive_minute_batch_v2 (LIKE population_top3000_202608_202609_v1.massive_minute_batch_v1 INCLUDING ALL);
ALTER TABLE population_top3000_202608_202609_v1.massive_minute_batch_v2 ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON population_top3000_202608_202609_v1.massive_minute_batch_v2 FROM PUBLIC,anon,authenticated;

DO $fk$ BEGIN
 IF NOT EXISTS(SELECT 1 FROM pg_constraint WHERE conrelid='population_top3000_202608_202609_v1.massive_minute_page_v2'::regclass AND conname='massive_minute_page_v2_source_fkey') THEN
 ALTER TABLE population_top3000_202608_202609_v1.massive_minute_page_v2 ADD CONSTRAINT massive_minute_page_v2_source_fkey FOREIGN KEY(source_id) REFERENCES population_top3000_202608_202609_v1.source_document(source_id);
 ALTER TABLE population_top3000_202608_202609_v1.massive_minute_page_v2 ADD CONSTRAINT massive_minute_page_v2_task_fkey FOREIGN KEY(acquire_task_id) REFERENCES population_top3000_202608_202609_v1.pipeline_task(task_id);
 ALTER TABLE population_top3000_202608_202609_v1.massive_minute_page_v2 ADD CONSTRAINT massive_minute_page_v2_attempt_fkey FOREIGN KEY(attempt_id) REFERENCES population_top3000_202608_202609_v1.pipeline_attempt(attempt_id);
 ALTER TABLE population_top3000_202608_202609_v1.massive_minute_bar_v2 ADD CONSTRAINT massive_minute_bar_v2_page_fkey FOREIGN KEY(page_id) REFERENCES population_top3000_202608_202609_v1.massive_minute_page_v2(page_id);
 ALTER TABLE population_top3000_202608_202609_v1.massive_minute_batch_v2 ADD CONSTRAINT massive_minute_batch_v2_page_fkey FOREIGN KEY(page_id) REFERENCES population_top3000_202608_202609_v1.massive_minute_page_v2(page_id);
 END IF; END; $fk$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.massive_minute_acquire_v2(p_attempt uuid, p_token text)
 RETURNS jsonb
 LANGUAGE plpgsql
 SET search_path TO 'pg_catalog'
AS $function$
DECLARE tid uuid;c jsonb;t population_top3000_202608_202609_v1.pipeline_task%rowtype;last_page integer;
BEGIN
 tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);SELECT * INTO t FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=tid;
 IF t.stage<>'ACQUIRE' THEN RAISE EXCEPTION 'MASSIVE_MINUTE_STAGE_MISMATCH';END IF;c:=population_top3000_202608_202609_v1.massive_minute_context_v2(tid);last_page:=coalesce((t.checkpoint->>'last_page')::integer,0);
 IF last_page>=(c->>'max_pages')::integer THEN RETURN jsonb_build_object('state','BLOCKED','blocker_code','MASSIVE_MINUTE_PAGE_CAP_REVIEW_REQUIRED','resumption_condition',jsonb_build_object('event','CONTROLLER_REVIEW_OF_RETAINED_PAGE_AND_CURSOR','archive_key',c->>'archive_key','last_page',last_page,'no_truncation',true,'source_chain_terminal',false));END IF;
 RETURN population_top3000_202608_202609_v1.pipeline_dispatch_edge_v1(p_attempt,p_token,'population-top3000-minute-v2');
END; $function$;


REVOKE ALL ON FUNCTION population_top3000_202608_202609_v1.massive_minute_acquire_v2(uuid,text) FROM PUBLIC,anon,authenticated;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.massive_minute_bundle_sha256_v2()
 RETURNS text
 LANGUAGE sql
 STABLE
 SET search_path TO 'pg_catalog'
AS $function$
 SELECT encode(sha256(convert_to(jsonb_object_agg(fn,encode(sha256(convert_to(pg_get_functiondef(('population_top3000_202608_202609_v1.'||fn)::regprocedure),'UTF8')),'hex'))::text,'UTF8')),'hex')
 FROM (VALUES ('massive_minute_url_v2(text,boolean,text)'),('massive_minute_context_v2(uuid)'),('massive_minute_resource_guard_v2()'),('massive_minute_results_v2(jsonb,text)'),('massive_minute_acquire_v2(uuid,text)'),('massive_minute_claim_v2(uuid,text)'),('massive_minute_read_source_v2(bigint)'),('massive_minute_commit_v2(uuid,text,jsonb)'),('massive_minute_failure_v2(uuid,text,jsonb)'),('massive_minute_number_v2(jsonb)'),('massive_minute_parse_v2(jsonb,integer,integer)'),('massive_minute_chain_v2(text,jsonb)'),('massive_minute_normalize_v2(uuid,text)'),('massive_minute_derive_v2(uuid,text)'),('massive_minute_quality_hold_v2(uuid,text)'),('massive_minute_route_v2(uuid,text,text)'),('massive_minute_route_acquire_v2(uuid,text)'),('massive_minute_route_normalize_v2(uuid,text)'),('massive_minute_route_derive_v2(uuid,text)'),('massive_minute_route_validate_v2(uuid,text)'),('massive_minute_bundle_sha256_v2()')) f(fn);
$function$;


REVOKE ALL ON FUNCTION population_top3000_202608_202609_v1.massive_minute_bundle_sha256_v2() FROM PUBLIC,anon,authenticated;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.massive_minute_chain_v2(p_archive text, p_versions jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 STABLE
 SET search_path TO 'pg_catalog'
 SET "TimeZone" TO 'UTC'
AS $function$
DECLARE pages integer;terminal integer;records bigint;h text;
BEGIN
 SELECT count(*),max(page_number) FILTER(WHERE next_url IS NULL),coalesce(sum(result_count),0),encode(sha256(convert_to(jsonb_agg(jsonb_build_object('page',page_number,'source_id',source_id,'source_sha256',source_sha256,'wire_sha256',wire_sha256,'bytes',response_bytes,'records',result_count,'request_url',request_url,'next_url',next_url) ORDER BY page_number)::text,'UTF8')),'hex')
 INTO pages,terminal,records,h FROM population_top3000_202608_202609_v1.massive_minute_page_v2 WHERE archive_key=p_archive;
 IF pages<1 OR terminal IS NULL OR terminal<>pages OR records>50000
 OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.massive_minute_page_v2 p WHERE p.archive_key=p_archive AND (p.input_versions IS DISTINCT FROM p_versions OR p.page_number>terminal OR (p.page_number<terminal AND p.next_url IS NULL) OR (p.page_number>1 AND NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.massive_minute_page_v2 prev WHERE prev.archive_key=p_archive AND prev.page_number=p.page_number-1 AND prev.next_url=p.request_url))))
 THEN RAISE EXCEPTION 'MASSIVE_MINUTE_COMPLETE_IMMUTABLE_SOURCE_CHAIN_REQUIRED';END IF;
 RETURN jsonb_build_object('pages',pages,'records',records,'source_manifest_sha256',h);
END; $function$;


REVOKE ALL ON FUNCTION population_top3000_202608_202609_v1.massive_minute_chain_v2(text,jsonb) FROM PUBLIC,anon,authenticated;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.massive_minute_claim_v2(p_attempt uuid, p_token text)
 RETURNS jsonb
 LANGUAGE plpgsql
 SET search_path TO 'pg_catalog'
AS $function$
DECLARE tid uuid;t population_top3000_202608_202609_v1.pipeline_task%rowtype;c jsonb;r jsonb;last_page integer;url text;max_page integer;
BEGIN
 tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);SELECT * INTO t FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=tid;
 IF t.stage<>'ACQUIRE' THEN RAISE EXCEPTION 'MASSIVE_MINUTE_STAGE_MISMATCH';END IF;c:=population_top3000_202608_202609_v1.massive_minute_context_v2(tid);last_page:=coalesce((t.checkpoint->>'last_page')::integer,0);
 IF last_page<0 OR last_page>=(c->>'max_pages')::integer THEN RAISE EXCEPTION 'MASSIVE_MINUTE_PAGE_CAP_REVIEW_REQUIRED';END IF;
 SELECT coalesce(max(page_number),0) INTO max_page FROM population_top3000_202608_202609_v1.massive_minute_page_v2 WHERE archive_key=c->>'archive_key';
 IF max_page<>last_page THEN RAISE EXCEPTION 'MASSIVE_MINUTE_SOURCE_CHECKPOINT_DIVERGED';END IF;
 IF last_page=0 THEN url:=c->>'first_url';ELSE
  SELECT next_url INTO url FROM population_top3000_202608_202609_v1.massive_minute_page_v2 WHERE archive_key=c->>'archive_key' AND page_number=last_page AND input_versions=t.input_versions;
  IF NOT FOUND OR url IS NULL THEN RAISE EXCEPTION 'MASSIVE_MINUTE_TERMINAL_OR_MISSING_PRIOR_PAGE';END IF;
 END IF;
 PERFORM population_top3000_202608_202609_v1.massive_minute_url_v2(url,last_page=0,c->>'source_symbol');
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_provider_lease l JOIN population_top3000_202608_202609_v1.pipeline_attempt a USING(attempt_id) WHERE a.task_id=tid AND NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.massive_minute_page_v2 p WHERE p.attempt_id=a.attempt_id)) THEN RAISE EXCEPTION 'MASSIVE_MINUTE_PRIOR_REQUEST_REQUIRES_RECONCILIATION';END IF;
 r:=population_top3000_202608_202609_v1.pipeline_provider_reserve_v1(p_attempt,p_token,'Massive',8388608,0);
 IF r->>'state'<>'RESERVED' THEN RETURN population_top3000_202608_202609_v1.pipeline_finish_v1(p_attempt,p_token,jsonb_build_object('state','BLOCKED','blocker_code',CASE WHEN r->>'state'='WAIT' THEN 'DEPENDENCY_PENDING' ELSE 'PROVIDER_BUDGET_OR_RESOURCE_HOLD' END,'recheck_at',CASE WHEN r->>'state'='WAIT' THEN r->>'not_before' ELSE NULL END,'resumption_condition',jsonb_build_object('event','CONTROLLER_ADMISSION_OR_PROVIDER_SLOT_RECONCILED','receipt',r)));END IF;
 RETURN c||jsonb_build_object('state','RESERVED','page_number',last_page+1,'request_url',url);
END; $function$;


REVOKE ALL ON FUNCTION population_top3000_202608_202609_v1.massive_minute_claim_v2(uuid,text) FROM PUBLIC,anon,authenticated;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.massive_minute_commit_v2(p_attempt uuid, p_token text, p_page jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 SET search_path TO 'pg_catalog'
AS $function$
DECLARE tid uuid;t population_top3000_202608_202609_v1.pipeline_task%rowtype;c jsonb;txt text;b jsonb;arr jsonb;h text;sid text;v_next text;url text;last_page integer;n integer;cnt integer;bytes bigint;wire_n bigint;at_time timestamptz;fresh integer;pageid bigint;v_evidence jsonb;presence text;
BEGIN
 tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);SELECT * INTO t FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=tid;
 IF t.stage<>'ACQUIRE' THEN RAISE EXCEPTION 'MASSIVE_MINUTE_STAGE_MISMATCH';END IF;c:=population_top3000_202608_202609_v1.massive_minute_context_v2(tid);PERFORM population_top3000_202608_202609_v1.massive_minute_resource_guard_v2();
 last_page:=coalesce((t.checkpoint->>'last_page')::integer,0);n:=(p_page->>'page_number')::integer;
 IF last_page=0 THEN url:=c->>'first_url';ELSE SELECT next_url INTO url FROM population_top3000_202608_202609_v1.massive_minute_page_v2 WHERE archive_key=c->>'archive_key' AND page_number=last_page AND input_versions=t.input_versions;END IF;
 IF jsonb_typeof(p_page) IS DISTINCT FROM 'object' OR n IS NULL OR n<>last_page+1 OR n>(c->>'max_pages')::integer OR p_page->>'archive_key' IS DISTINCT FROM c->>'archive_key' OR p_page->>'source_symbol' IS DISTINCT FROM c->>'source_symbol' OR p_page->>'trading_date' IS DISTINCT FROM '2026-09-09' OR p_page->>'window_start_ms' IS DISTINCT FROM '1788926400000' OR p_page->>'window_end_exclusive_ms' IS DISTINCT FROM '1789012800000' OR p_page->>'request_url' IS DISTINCT FROM url OR p_page->>'http_status' IS DISTINCT FROM '200' THEN RAISE EXCEPTION 'MASSIVE_MINUTE_COMMIT_SCOPE_MISMATCH';END IF;
 PERFORM population_top3000_202608_202609_v1.massive_minute_url_v2(url,n=1,c->>'source_symbol');
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_provider_lease WHERE attempt_id=p_attempt AND provider='Massive' AND released_at IS NULL AND reserved_bytes=8388608) THEN RAISE EXCEPTION 'MASSIVE_MINUTE_RESERVATION_REQUIRED';END IF;
 txt:=p_page->>'content_text';bytes:=octet_length(txt);wire_n:=(p_page->>'wire_bytes')::bigint;
 IF txt IS NULL OR bytes NOT BETWEEN 1 AND 8388608 OR wire_n IS NULL OR wire_n NOT BETWEEN 1 AND 8388608 OR coalesce((p_page->>'elapsed_ms')::bigint,-1) NOT BETWEEN 0 AND 45000 OR coalesce(p_page->>'wire_sha256','')!~'^[0-9a-f]{64}$' THEN RAISE EXCEPTION 'MASSIVE_MINUTE_RESPONSE_BOUNDS_INVALID';END IF;
 h:=encode(sha256(convert_to(txt,'UTF8')),'hex');IF h IS DISTINCT FROM p_page->>'source_sha256' THEN RAISE EXCEPTION 'MASSIVE_MINUTE_STORED_HASH_MISMATCH';END IF;
 b:=txt::jsonb;arr:=population_top3000_202608_202609_v1.massive_minute_results_v2(b,c->>'source_symbol');cnt:=jsonb_array_length(arr);presence:=CASE WHEN b ? 'results' THEN 'ARRAY' ELSE 'OMITTED_WITH_EXPLICIT_ZERO' END;
 v_next:=nullif(b->>'next_url','');IF v_next IS NOT NULL THEN PERFORM population_top3000_202608_202609_v1.massive_minute_url_v2(v_next,false,c->>'source_symbol');END IF;
 IF cnt IS DISTINCT FROM (p_page->>'result_count')::integer OR presence IS DISTINCT FROM p_page->>'results_presence' OR v_next IS DISTINCT FROM nullif(p_page->>'next_url','') THEN RAISE EXCEPTION 'MASSIVE_MINUTE_PAGE_ENVELOPE_MISMATCH';END IF;
 IF cnt+(SELECT coalesce(sum(result_count),0) FROM population_top3000_202608_202609_v1.massive_minute_page_v2 WHERE archive_key=c->>'archive_key' AND page_number<>n)>50000 THEN RAISE EXCEPTION 'MASSIVE_MINUTE_ARCHIVE_RECORD_CAP_REVIEW_REQUIRED';END IF;
 IF v_next=url OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.massive_minute_page_v2 WHERE archive_key=c->>'archive_key' AND request_url=v_next) THEN RAISE EXCEPTION 'MASSIVE_MINUTE_REPEATED_PAGE_CURSOR';END IF;
 IF txt~* '"(apiKey|api_key|access_token|authorization)"[[:space:]]*:' OR txt~* '[?&](apiKey|api_key|access_token)=' THEN RAISE EXCEPTION 'MASSIVE_MINUTE_CREDENTIAL_ECHO';END IF;
 at_time:=(p_page->>'retrieved_at')::timestamptz;
 IF at_time IS NULL OR at_time<(SELECT started_at FROM population_top3000_202608_202609_v1.pipeline_attempt WHERE attempt_id=p_attempt)-interval '30 seconds' OR at_time>clock_timestamp()+interval '30 seconds' THEN RAISE EXCEPTION 'MASSIVE_MINUTE_RETRIEVAL_CLOCK_INVALID';END IF;
 sid:=(c->>'archive_key')||':page:'||n::text;
 v_evidence:=jsonb_build_object('wire_sha256',p_page->>'wire_sha256','wire_bytes',wire_n,'stored_content_bytes',bytes,'hash_representation','EXACT_UTF8_RESPONSE_EXCEPT_CREDENTIAL_REDACTED_NEXT_URL','source_symbol',c->>'source_symbol','trading_date','2026-09-09','window_start_ms',1788926400000::bigint,'window_end_exclusive_ms',1789012800000::bigint,'adjusted',false,'page_number',n,'results_presence',presence,'result_count',cnt,'provider_request_id',p_page->>'provider_request_id','provider_contract','MASSIVE_QUALIFYING_TRADE_MINUTE_AGGREGATES_ALL_ET_SESSIONS','sip_feed_verified',false,'window_timestamp_role','AGGREGATE_WINDOW_START_NOT_PUBLICATION_TIME','company_membership_verified',false,'original_historical_receipt',false);
 INSERT INTO population_top3000_202608_202609_v1.source_document(source_id,source_sha256,source_uri,source_provider,retrieved_at,source_published_at,historical_received_at,content_text,content_bytes,evidence,license_policy)
 VALUES(sid,h,url,'Massive',at_time,NULL,NULL,txt,bytes,v_evidence,'EXISTING_MASSIVE_SUBSCRIPTION_API_RESPONSE_INTERNAL_USE') ON CONFLICT(source_id) DO NOTHING;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.source_document d WHERE d.source_id=sid AND d.source_sha256=h AND d.source_uri=url AND d.source_provider='Massive' AND d.retrieved_at=at_time AND d.source_published_at IS NULL AND d.historical_received_at IS NULL AND d.content_text=txt AND d.content_bytes=bytes AND d.evidence=v_evidence AND d.license_policy='EXISTING_MASSIVE_SUBSCRIPTION_API_RESPONSE_INTERNAL_USE') THEN RAISE EXCEPTION 'MASSIVE_MINUTE_SOURCE_REPLAY_CONFLICT';END IF;
 INSERT INTO population_top3000_202608_202609_v1.massive_minute_page_v2(archive_key,page_number,request_url,next_url,input_versions,acquire_task_id,attempt_id,source_id,source_sha256,response_bytes,wire_sha256,wire_bytes,result_count,results_presence,retrieved_at)
 VALUES(c->>'archive_key',n,url,v_next,t.input_versions,tid,p_attempt,sid,h,bytes,p_page->>'wire_sha256',wire_n,cnt,presence,at_time) ON CONFLICT(archive_key,page_number) DO NOTHING;
 GET DIAGNOSTICS fresh=ROW_COUNT;
 SELECT page_id INTO pageid FROM population_top3000_202608_202609_v1.massive_minute_page_v2 p WHERE p.archive_key=c->>'archive_key' AND p.page_number=n AND p.request_url=url AND p.next_url IS NOT DISTINCT FROM v_next AND p.input_versions=t.input_versions AND p.acquire_task_id=tid AND p.attempt_id=p_attempt AND p.source_id=sid AND p.source_sha256=h AND p.response_bytes=bytes AND p.wire_sha256=p_page->>'wire_sha256' AND p.wire_bytes=wire_n AND p.result_count=cnt AND p.results_presence=presence AND p.retrieved_at=at_time;
 IF NOT FOUND THEN RAISE EXCEPTION 'MASSIVE_MINUTE_PAGE_REPLAY_CONFLICT';END IF;
 PERFORM population_top3000_202608_202609_v1.massive_minute_read_source_v2(pageid);PERFORM population_top3000_202608_202609_v1.massive_minute_resource_guard_v2();
 PERFORM population_top3000_202608_202609_v1.pipeline_provider_release_v1(p_attempt,p_token,bytes,0,0);
 RETURN population_top3000_202608_202609_v1.pipeline_finish_v1(p_attempt,p_token,jsonb_build_object('state',CASE WHEN v_next IS NULL THEN 'COMPLETE' ELSE 'CONTINUE' END,'checkpoint',jsonb_build_object('last_page',n),'commit_receipt',jsonb_build_object('commit_key','massive-minute-source:'||sid,'output_sha256',h,'new_committed_units',fresh,'immutable_output_ids',jsonb_build_array(sid),'input_versions',t.input_versions,'source_records',cnt,'source_chain_terminal',v_next IS NULL,'company_population_verified',false)));
END; $function$;


REVOKE ALL ON FUNCTION population_top3000_202608_202609_v1.massive_minute_commit_v2(uuid,text,jsonb) FROM PUBLIC,anon,authenticated;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.massive_minute_context_v2(p_task uuid)
 RETURNS jsonb
 LANGUAGE plpgsql
 STABLE
 SET search_path TO 'pg_catalog'
AS $function$
DECLARE t population_top3000_202608_202609_v1.pipeline_task%rowtype;k text;cap integer;
BEGIN
 SELECT * INTO t FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=p_task;
 IF NOT FOUND OR t.family_id<>'P01' OR t.partition_key IS DISTINCT FROM 'massive-minute-'||(t.payload->>'source_symbol')||'-20260909-v2' OR t.payload->>'kind' IS DISTINCT FROM 'MASSIVE_MINUTE_KNOWN5_V2'
 OR coalesce(t.payload->>'source_symbol','') NOT IN('AAPL','GOOG','GOOGL','NVDA','WEAV') OR t.payload->>'trading_date' IS DISTINCT FROM '2026-09-09'
 OR t.input_versions->>'scope_sha256' IS DISTINCT FROM '8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd'
 OR t.input_versions->>'source_contract_revision' IS DISTINCT FROM 'massive-minute-known5-20260909-v2'
 OR t.input_versions->>'massive_minute_bundle_sha256' IS DISTINCT FROM population_top3000_202608_202609_v1.massive_minute_bundle_sha256_v2()
 THEN RAISE EXCEPTION 'MASSIVE_MINUTE_SCOPE_OR_BUNDLE_NOT_ADMITTED';END IF;
 IF (t.payload->>'instrument_key') IS DISTINCT FROM (CASE t.payload->>'source_symbol' WHEN 'AAPL' THEN '41' WHEN 'GOOG' THEN '5083' WHEN 'GOOGL' THEN '5084' WHEN 'NVDA' THEN '8372' WHEN 'WEAV' THEN '12595' END) THEN RAISE EXCEPTION 'MASSIVE_MINUTE_KNOWN_INSTRUMENT_REQUIRED';END IF;
 cap:=(t.payload->>'max_pages')::integer;IF cap IS NULL OR cap NOT BETWEEN 1 AND 2 THEN RAISE EXCEPTION 'MASSIVE_MINUTE_PAGE_CAP_NOT_ADMITTED';END IF;
 k:='p01-massive-minute-v2:'||encode(sha256(convert_to(jsonb_build_object('family','P01','partition',t.partition_key,'input_versions',t.input_versions)::text,'UTF8')),'hex');
 RETURN jsonb_build_object('archive_key',k,'family_id','P01','partition_key',t.partition_key,'source_symbol',t.payload->>'source_symbol','trading_date','2026-09-09','window_start_ms',1788926400000::bigint,'window_end_exclusive_ms',1789012800000::bigint,'adjusted',false,'max_response_bytes',8388608,'max_runtime_ms',45000,'max_result_count',50000,'max_archive_records',50000,'max_pages',cap,'first_url','https://api.massive.com/v2/aggs/ticker/'||(t.payload->>'source_symbol')||'/range/1/minute/1788926400000/1789012799999?adjusted=false&sort=asc&limit=50000');
END; $function$;


REVOKE ALL ON FUNCTION population_top3000_202608_202609_v1.massive_minute_context_v2(uuid) FROM PUBLIC,anon,authenticated;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.massive_minute_derive_v2(p_attempt uuid, p_token text)
 RETURNS jsonb
 LANGUAGE plpgsql
 SET search_path TO 'pg_catalog'
 SET "TimeZone" TO 'UTC'
AS $function$
DECLARE tid uuid;t population_top3000_202608_202609_v1.pipeline_task%rowtype;c jsonb;chain jsonb;p population_top3000_202608_202609_v1.massive_minute_page_v2%rowtype;q population_top3000_202608_202609_v1.massive_minute_batch_v2%rowtype;b jsonb;
 pn integer;prior_row integer;last_row integer;expected_rows bigint;actual_rows bigint;expected_hash text;actual_hash text;parser_hash text;normalizer_hash text;verifier_hash text;new_verified bigint;done boolean;cp jsonb;summary_json jsonb;duplicate_json jsonb;
BEGIN
 tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);SELECT * INTO t FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=tid;
 IF t.stage<>'DERIVE' THEN RAISE EXCEPTION 'MASSIVE_MINUTE_STAGE_MISMATCH';END IF;c:=population_top3000_202608_202609_v1.massive_minute_context_v2(tid);PERFORM population_top3000_202608_202609_v1.massive_minute_resource_guard_v2();chain:=population_top3000_202608_202609_v1.massive_minute_chain_v2(c->>'archive_key',t.input_versions);
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_partition z WHERE z.family_id='P01' AND z.partition_key=t.partition_key AND z.producer_revision='massive-minute-known5-20260909-v2' AND z.immutable_input_versions=t.input_versions AND z.denominator_evidence->'source_manifest'=chain AND z.denominator_state='PROVISIONAL' AND NOT z.publication_eligible AND z.expected_units IS NULL) THEN RAISE EXCEPTION 'MASSIVE_MINUTE_NORMALIZED_SOURCE_MANIFEST_CHANGED';END IF;
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.massive_minute_page_v2 WHERE archive_key=c->>'archive_key' AND (normalized_at IS NULL OR normalized_rows<>result_count)) THEN RAISE EXCEPTION 'MASSIVE_MINUTE_COMPLETE_NORMALIZATION_REQUIRED';END IF;
 pn:=coalesce((t.checkpoint->>'next_page')::integer,1);prior_row:=coalesce((t.checkpoint->>'last_verified_row')::integer,0);
 SELECT * INTO p FROM population_top3000_202608_202609_v1.massive_minute_page_v2 WHERE archive_key=c->>'archive_key' AND page_number=pn FOR UPDATE;
 IF NOT FOUND OR pn<1 OR p.input_versions IS DISTINCT FROM t.input_versions THEN RAISE EXCEPTION 'MASSIVE_MINUTE_SOURCE_PAGE_INPUT_MISMATCH';END IF;
 b:=population_top3000_202608_202609_v1.massive_minute_read_source_v2(p.page_id);last_row:=least(prior_row+1000,p.result_count);
 IF prior_row<0 OR prior_row>p.result_count OR (prior_row>0 AND prior_row%1000<>0) OR p.verified_rows NOT IN(prior_row,last_row) THEN RAISE EXCEPTION 'MASSIVE_MINUTE_VERIFY_CHECKPOINT_DIVERGED';END IF;
 parser_hash:=encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.massive_minute_parse_v2(jsonb,integer,integer)'::regprocedure),'UTF8')),'hex');normalizer_hash:=encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.massive_minute_normalize_v2(uuid,text)'::regprocedure),'UTF8')),'hex');verifier_hash:=encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.massive_minute_derive_v2(uuid,text)'::regprocedure),'UTF8')),'hex');new_verified:=0;
 IF last_row>prior_row THEN
  SELECT * INTO q FROM population_top3000_202608_202609_v1.massive_minute_batch_v2 WHERE page_id=p.page_id AND first_source_row=prior_row+1 FOR UPDATE;
  IF NOT FOUND OR q.last_source_row<>last_row OR q.source_rows<>last_row-prior_row OR q.parser_sha256<>parser_hash OR q.normalizer_sha256<>normalizer_hash OR (q.verified_at IS NOT NULL AND q.verifier_sha256 IS DISTINCT FROM verifier_hash) THEN RAISE EXCEPTION 'MASSIVE_MINUTE_BATCH_PROOF_VERSION_CONFLICT';END IF;
  SELECT count(*),encode(sha256(convert_to(string_agg(encode(sha256(convert_to(to_jsonb(x)::text,'UTF8')),'hex'),E'\n' ORDER BY x.source_row),'UTF8')),'hex') INTO expected_rows,expected_hash FROM population_top3000_202608_202609_v1.massive_minute_parse_v2(b,prior_row+1,last_row) x;
  SELECT count(*),encode(sha256(convert_to(string_agg(encode(sha256(convert_to((to_jsonb(x)-'page_id')::text,'UTF8')),'hex'),E'\n' ORDER BY x.source_row),'UTF8')),'hex') INTO actual_rows,actual_hash FROM population_top3000_202608_202609_v1.massive_minute_bar_v2 x WHERE x.page_id=p.page_id AND x.source_row BETWEEN prior_row+1 AND last_row;
  IF expected_rows<>last_row-prior_row OR actual_rows<>expected_rows OR actual_hash IS DISTINCT FROM expected_hash OR q.typed_manifest_sha256 IS DISTINCT FROM expected_hash THEN RAISE EXCEPTION 'MASSIVE_MINUTE_SOURCE_TO_TYPED_RECONCILIATION_FAILED';END IF;
  IF q.verified_at IS NULL THEN UPDATE population_top3000_202608_202609_v1.massive_minute_batch_v2 SET verified_at=clock_timestamp(),verifier_sha256=verifier_hash WHERE page_id=p.page_id AND first_source_row=prior_row+1;new_verified:=last_row-prior_row;END IF;
 ELSE
  expected_hash:=encode(sha256(convert_to('EMPTY_SOURCE_PAGE','UTF8')),'hex');IF p.result_count<>0 OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.massive_minute_bar_v2 WHERE page_id=p.page_id) THEN RAISE EXCEPTION 'MASSIVE_MINUTE_EMPTY_SOURCE_CONFLICT';END IF;
 END IF;
 done:=last_row=p.result_count AND p.next_url IS NULL;
 UPDATE population_top3000_202608_202609_v1.massive_minute_page_v2 SET verified_rows=last_row,verified_at=CASE WHEN last_row=result_count THEN coalesce(verified_at,clock_timestamp()) ELSE NULL END WHERE page_id=p.page_id;
 IF done THEN
  IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.massive_minute_page_v2 v WHERE v.archive_key=c->>'archive_key' AND (v.verified_at IS NULL OR v.verified_rows<>v.result_count)) THEN RAISE EXCEPTION 'MASSIVE_MINUTE_ALL_PAGES_NOT_VERIFIED';END IF;
  IF EXISTS(
   WITH actual AS MATERIALIZED (
    SELECT x.page_id,((x.source_row-1)/1000)*1000+1 AS first_source_row,min(x.source_row) AS minimum_source_row,max(x.source_row) AS last_source_row,count(*) AS source_rows,
     encode(sha256(convert_to(string_agg(encode(sha256(convert_to((to_jsonb(x)-'page_id')::text,'UTF8')),'hex'),E'\n' ORDER BY x.source_row),'UTF8')),'hex') AS typed_manifest_sha256
    FROM population_top3000_202608_202609_v1.massive_minute_bar_v2 x JOIN population_top3000_202608_202609_v1.massive_minute_page_v2 y USING(page_id) WHERE y.archive_key=c->>'archive_key' GROUP BY x.page_id,((x.source_row-1)/1000)*1000+1
   ),saved AS MATERIALIZED (
    SELECT x.* FROM population_top3000_202608_202609_v1.massive_minute_batch_v2 x JOIN population_top3000_202608_202609_v1.massive_minute_page_v2 y USING(page_id) WHERE y.archive_key=c->>'archive_key'
   )
   SELECT 1 FROM actual x FULL JOIN saved y USING(page_id,first_source_row)
   LEFT JOIN population_top3000_202608_202609_v1.massive_minute_page_v2 z ON z.page_id=coalesce(x.page_id,y.page_id)
   WHERE x.page_id IS NULL OR y.page_id IS NULL OR x.minimum_source_row<>x.first_source_row OR x.last_source_row<>least(x.first_source_row+999,z.result_count) OR x.source_rows<>x.last_source_row-x.first_source_row+1 OR x.last_source_row<>y.last_source_row OR x.source_rows<>y.source_rows OR x.typed_manifest_sha256 IS DISTINCT FROM y.typed_manifest_sha256 OR y.verified_at IS NULL OR y.parser_sha256 IS DISTINCT FROM parser_hash OR y.normalizer_sha256 IS DISTINCT FROM normalizer_hash OR y.verifier_sha256 IS DISTINCT FROM verifier_hash
  ) THEN RAISE EXCEPTION 'MASSIVE_MINUTE_TERMINAL_CURRENT_BATCH_PROOF_MISMATCH';END IF;
  IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.massive_minute_page_v2 x LEFT JOIN population_top3000_202608_202609_v1.source_document d ON d.source_id=x.source_id WHERE x.archive_key=c->>'archive_key' AND (d.source_id IS NULL OR d.content_text IS NULL OR d.source_sha256 IS DISTINCT FROM x.source_sha256 OR d.content_bytes IS DISTINCT FROM x.response_bytes OR d.source_uri IS DISTINCT FROM x.request_url OR d.source_provider IS DISTINCT FROM 'Massive' OR d.retrieved_at IS DISTINCT FROM x.retrieved_at OR d.source_published_at IS NOT NULL OR d.historical_received_at IS NOT NULL OR d.evidence->>'wire_sha256' IS DISTINCT FROM x.wire_sha256)) THEN RAISE EXCEPTION 'MASSIVE_MINUTE_TERMINAL_SOURCE_MANIFEST_CHANGED';END IF;
  SELECT jsonb_build_object('raw_occurrences',count(*),'rows_with_missing_fields',count(*) FILTER(WHERE x.missing_mask<>0),'rows_with_invalid_fields',count(*) FILTER(WHERE x.invalid_mask<>0),'rows_with_structural_flags',count(*) FILTER(WHERE x.structure_mask<>0),'outside_window_rows',count(*) FILTER(WHERE (x.structure_mask&2)<>0),'unaligned_timestamp_rows',count(*) FILTER(WHERE (x.structure_mask&4)<>0),'missing_or_invalid_timestamp_rows',count(*) FILTER(WHERE (x.missing_mask&1)<>0 OR (x.invalid_mask&1)<>0),'explicit_zero_volume_rows',count(*) FILTER(WHERE x.share_volume=0),'missing_volume_rows',count(*) FILTER(WHERE (x.missing_mask&32)<>0),'first_provider_bar',min(x.bar_start_at),'last_provider_bar',max(x.bar_start_at))
  INTO summary_json FROM population_top3000_202608_202609_v1.massive_minute_bar_v2 x JOIN population_top3000_202608_202609_v1.massive_minute_page_v2 y USING(page_id) WHERE y.archive_key=c->>'archive_key';
  IF (summary_json->>'raw_occurrences')::bigint<>(chain->>'records')::bigint THEN RAISE EXCEPTION 'MASSIVE_MINUTE_TOTAL_OCCURRENCE_COUNT_MISMATCH';END IF;
  SELECT jsonb_build_object('repeated_timestamp_groups',count(*) FILTER(WHERE occurrences>1),'conflicting_timestamp_groups',count(*) FILTER(WHERE versions>1),'duplicate_identical_occurrences',coalesce(sum(occurrences-versions),0)) INTO duplicate_json FROM (
   SELECT x.provider_time_ms,count(*) AS occurrences,count(DISTINCT x.raw_record_sha256) AS versions FROM population_top3000_202608_202609_v1.massive_minute_bar_v2 x JOIN population_top3000_202608_202609_v1.massive_minute_page_v2 y USING(page_id) WHERE y.archive_key=c->>'archive_key' AND x.provider_time_ms IS NOT NULL GROUP BY x.provider_time_ms
  ) g;
  summary_json:=summary_json||duplicate_json||jsonb_build_object('source_manifest',chain,'source_symbol',c->>'source_symbol','source_date','2026-09-09','source_records_reconciled',true,'provider_contract','MASSIVE_QUALIFYING_TRADE_MINUTE_AGGREGATES_ALL_ET_SESSIONS','timestamp_role','AGGREGATE_WINDOW_START_NOT_PUBLICATION_TIME','expected_printed_bars',NULL,'missing_minutes_filled',0,'empty_source_is_not_zero_volume',true,'company_population_verified',false,'independent_price_accuracy_verified',false,'sip_feed_verified',false,'historical_receipt_proven',false,'parser_sha256',parser_hash,'normalizer_sha256',normalizer_hash,'verifier_sha256',verifier_hash);
  IF p.source_summary IS NOT NULL AND p.source_summary IS DISTINCT FROM summary_json THEN RAISE EXCEPTION 'MASSIVE_MINUTE_SUMMARY_REPLAY_CONFLICT';END IF;
  UPDATE population_top3000_202608_202609_v1.massive_minute_page_v2 SET source_summary=summary_json WHERE page_id=p.page_id;
  expected_hash:=encode(sha256(convert_to(jsonb_build_object('summary',summary_json,'typed_batches',(SELECT jsonb_agg(jsonb_build_array(y.page_number,x.first_source_row,x.last_source_row,x.typed_manifest_sha256) ORDER BY y.page_number,x.first_source_row) FROM population_top3000_202608_202609_v1.massive_minute_batch_v2 x JOIN population_top3000_202608_202609_v1.massive_minute_page_v2 y USING(page_id) WHERE y.archive_key=c->>'archive_key'))::text,'UTF8')),'hex');
 END IF;
 PERFORM population_top3000_202608_202609_v1.massive_minute_resource_guard_v2();cp:=CASE WHEN last_row=p.result_count THEN jsonb_build_object('next_page',pn+1,'last_verified_row',0) ELSE jsonb_build_object('next_page',pn,'last_verified_row',last_row) END;
 RETURN jsonb_build_object('state',CASE WHEN done THEN 'COMPLETE' ELSE 'CONTINUE' END,'checkpoint',cp,'commit_receipt',jsonb_build_object('commit_key','massive-minute-reconcile:'||p.source_id||':'||prior_row::text||':'||last_row::text,'output_sha256',expected_hash,'new_committed_units',new_verified,'immutable_output_ids',jsonb_build_array(p.source_id,p.page_id),'input_versions',t.input_versions,'source_chain_verified',done,'company_population_verified',false,'independent_price_accuracy_verified',false));
END; $function$;


REVOKE ALL ON FUNCTION population_top3000_202608_202609_v1.massive_minute_derive_v2(uuid,text) FROM PUBLIC,anon,authenticated;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.massive_minute_failure_v2(p_attempt uuid, p_token text, p_failure jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 SET search_path TO 'pg_catalog'
AS $function$
DECLARE tid uuid;code text;class text;bytes bigint;retry integer;unknown_effect boolean;details jsonb;
BEGIN
 tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=tid AND family_id='P01' AND stage='ACQUIRE') THEN RAISE EXCEPTION 'MASSIVE_MINUTE_STAGE_MISMATCH';END IF;
 PERFORM population_top3000_202608_202609_v1.massive_minute_context_v2(tid);code:=p_failure->>'code';class:=p_failure->>'failure_class';bytes:=(p_failure->>'received_bytes')::bigint;retry:=(p_failure->>'retry_after_seconds')::integer;
 IF code IS NULL OR code!~'^[A-Z0-9_]{1,80}$' OR class IS NULL OR class NOT IN('RATE_LIMIT','TRANSPORT_READ','AUTHENTICATION','PARSER','SOURCE_ABSENT') OR bytes IS NULL OR bytes NOT BETWEEN 0 AND 8388608 OR retry IS NULL OR retry NOT BETWEEN 0 AND 3600 THEN RAISE EXCEPTION 'MASSIVE_MINUTE_FAILURE_RECEIPT_INVALID';END IF;
 IF jsonb_typeof(p_failure->'fetch_started') IS DISTINCT FROM 'boolean' OR (p_failure->>'wire_sha256' IS NOT NULL AND p_failure->>'wire_sha256'!~'^[a-f0-9]{64}$') OR (p_failure->>'http_status' IS NOT NULL AND (jsonb_typeof(p_failure->'http_status') IS DISTINCT FROM 'number' OR p_failure->>'http_status'!~'^[0-9]{3}$' OR (p_failure->>'http_status')::integer NOT BETWEEN 100 AND 599)) THEN RAISE EXCEPTION 'MASSIVE_MINUTE_FAILURE_RECEIPT_INVALID';END IF;
 details:=jsonb_build_object('code',code,'http_status',p_failure->'http_status','response_bytes_observed',bytes,'wire_sha256',p_failure->>'wire_sha256','fetch_started',p_failure->'fetch_started','raw_body_retained',false,'automatic_retry',false);
 unknown_effect:=coalesce((p_failure->>'fetch_started')::boolean,false) AND (p_failure->>'http_status' IS NULL OR code IN('DEADLINE','FETCH_FAILED','EMPTY_BODY') OR (p_failure->>'http_status'='200' AND p_failure->>'wire_sha256' IS NULL));
 IF unknown_effect THEN RETURN population_top3000_202608_202609_v1.pipeline_fail_v1(p_attempt,p_token,'UNCERTAIN_EFFECT','MASSIVE_MINUTE:'||code,details,retry);END IF;
 PERFORM population_top3000_202608_202609_v1.pipeline_provider_release_v1(p_attempt,p_token,0,0,CASE WHEN class='RATE_LIMIT' THEN retry ELSE 0 END);
 RETURN population_top3000_202608_202609_v1.pipeline_finish_v1(p_attempt,p_token,jsonb_build_object('state','BLOCKED','blocker_code','MASSIVE_MINUTE_'||code,'resumption_condition',jsonb_build_object('event','CONTROLLER_REVIEW_OF_DEFINITIVE_SOURCE_FAILURE','details',details,'automatic_retry',false)));
END; $function$;


REVOKE ALL ON FUNCTION population_top3000_202608_202609_v1.massive_minute_failure_v2(uuid,text,jsonb) FROM PUBLIC,anon,authenticated;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.massive_minute_normalize_v2(p_attempt uuid, p_token text)
 RETURNS jsonb
 LANGUAGE plpgsql
 SET search_path TO 'pg_catalog'
 SET "TimeZone" TO 'UTC'
AS $function$
DECLARE tid uuid;t population_top3000_202608_202609_v1.pipeline_task%rowtype;c jsonb;chain jsonb;p population_top3000_202608_202609_v1.massive_minute_page_v2%rowtype;b jsonb;
 pn integer;prior_row integer;last_row integer;new_rows bigint;expected_rows bigint;actual_rows bigint;expected_hash text;actual_hash text;parser_hash text;normalizer_hash text;part_evidence jsonb;done boolean;cp jsonb;
BEGIN
 tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);SELECT * INTO t FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=tid;
 IF t.stage<>'NORMALIZE' THEN RAISE EXCEPTION 'MASSIVE_MINUTE_STAGE_MISMATCH';END IF;c:=population_top3000_202608_202609_v1.massive_minute_context_v2(tid);PERFORM population_top3000_202608_202609_v1.massive_minute_resource_guard_v2();
 chain:=population_top3000_202608_202609_v1.massive_minute_chain_v2(c->>'archive_key',t.input_versions);pn:=coalesce((t.checkpoint->>'next_page')::integer,1);prior_row:=coalesce((t.checkpoint->>'last_source_row')::integer,0);
 SELECT * INTO p FROM population_top3000_202608_202609_v1.massive_minute_page_v2 WHERE archive_key=c->>'archive_key' AND page_number=pn FOR UPDATE;
 IF NOT FOUND OR pn<1 OR p.input_versions IS DISTINCT FROM t.input_versions THEN RAISE EXCEPTION 'MASSIVE_MINUTE_SOURCE_PAGE_INPUT_MISMATCH';END IF;
 b:=population_top3000_202608_202609_v1.massive_minute_read_source_v2(p.page_id);last_row:=least(prior_row+1000,p.result_count);
 IF prior_row<0 OR prior_row>p.result_count OR (prior_row>0 AND prior_row%1000<>0) OR p.normalized_rows NOT IN(prior_row,last_row) THEN RAISE EXCEPTION 'MASSIVE_MINUTE_NORMALIZE_CHECKPOINT_DIVERGED';END IF;
 parser_hash:=encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.massive_minute_parse_v2(jsonb,integer,integer)'::regprocedure),'UTF8')),'hex');normalizer_hash:=encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.massive_minute_normalize_v2(uuid,text)'::regprocedure),'UTF8')),'hex');new_rows:=0;
 IF last_row>prior_row THEN
  INSERT INTO population_top3000_202608_202609_v1.massive_minute_bar_v2(page_id,source_row,provider_time_ms,bar_start_at,provider_ny_date,provider_open,provider_high,provider_low,provider_close,share_volume,trade_count,provider_vwap,provider_otc,missing_mask,invalid_mask,structure_mask,raw_record_sha256)
  SELECT p.page_id,x.* FROM population_top3000_202608_202609_v1.massive_minute_parse_v2(b,prior_row+1,last_row) x ON CONFLICT(page_id,source_row) DO NOTHING;
  GET DIAGNOSTICS new_rows=ROW_COUNT;
  SELECT count(*),encode(sha256(convert_to(string_agg(encode(sha256(convert_to(to_jsonb(x)::text,'UTF8')),'hex'),E'\n' ORDER BY x.source_row),'UTF8')),'hex') INTO expected_rows,expected_hash FROM population_top3000_202608_202609_v1.massive_minute_parse_v2(b,prior_row+1,last_row) x;
  SELECT count(*),encode(sha256(convert_to(string_agg(encode(sha256(convert_to((to_jsonb(x)-'page_id')::text,'UTF8')),'hex'),E'\n' ORDER BY x.source_row),'UTF8')),'hex') INTO actual_rows,actual_hash FROM population_top3000_202608_202609_v1.massive_minute_bar_v2 x WHERE x.page_id=p.page_id AND x.source_row BETWEEN prior_row+1 AND last_row;
  IF expected_rows<>last_row-prior_row OR actual_rows<>expected_rows OR actual_hash IS DISTINCT FROM expected_hash THEN RAISE EXCEPTION 'MASSIVE_MINUTE_TYPED_BATCH_REPLAY_CONFLICT';END IF;
  INSERT INTO population_top3000_202608_202609_v1.massive_minute_batch_v2(page_id,first_source_row,last_source_row,source_rows,typed_manifest_sha256,parser_sha256,normalizer_sha256) VALUES(p.page_id,prior_row+1,last_row,last_row-prior_row,expected_hash,parser_hash,normalizer_hash) ON CONFLICT(page_id,first_source_row) DO NOTHING;
  IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.massive_minute_batch_v2 q WHERE q.page_id=p.page_id AND q.first_source_row=prior_row+1 AND q.last_source_row=last_row AND q.source_rows=last_row-prior_row AND q.typed_manifest_sha256=expected_hash AND q.parser_sha256=parser_hash AND q.normalizer_sha256=normalizer_hash) THEN RAISE EXCEPTION 'MASSIVE_MINUTE_BATCH_PROOF_REPLAY_CONFLICT';END IF;
 ELSE
  expected_hash:=encode(sha256(convert_to('EMPTY_SOURCE_PAGE','UTF8')),'hex');IF p.result_count<>0 OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.massive_minute_bar_v2 WHERE page_id=p.page_id) THEN RAISE EXCEPTION 'MASSIVE_MINUTE_EMPTY_SOURCE_CONFLICT';END IF;
 END IF;
 UPDATE population_top3000_202608_202609_v1.massive_minute_page_v2 SET normalized_rows=last_row,normalized_at=CASE WHEN last_row=result_count THEN coalesce(normalized_at,clock_timestamp()) ELSE NULL END WHERE page_id=p.page_id;
 part_evidence:=jsonb_build_object('basis','PROVISIONAL_MASSIVE_MINUTE_SOURCE_OCCURRENCES_ONLY','archive_key',c->>'archive_key','source_symbol',c->>'source_symbol','source_date','2026-09-09','source_manifest',chain,'company_membership_verified',false,'expected_printed_bars',NULL,'dense_clock_denominator_used',false,'missing_minutes_filled',0,'sip_feed_verified',false,'original_historical_receipt',false,'source_window_start_is_not_publication_time',true);
 INSERT INTO population_top3000_202608_202609_v1.pipeline_partition(family_id,partition_key,time_role,denominator_state,denominator_evidence,producer_revision,immutable_input_versions,publication_eligible)
 VALUES('P01',t.partition_key,'TARGET','PROVISIONAL',part_evidence,'massive-minute-known5-20260909-v2',t.input_versions,false) ON CONFLICT(family_id,partition_key) DO NOTHING;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_partition q WHERE q.family_id='P01' AND q.partition_key=t.partition_key AND q.time_role='TARGET' AND q.denominator_state='PROVISIONAL' AND q.denominator_evidence=part_evidence AND q.producer_revision='massive-minute-known5-20260909-v2' AND q.immutable_input_versions=t.input_versions AND NOT q.publication_eligible AND q.expected_units IS NULL) THEN RAISE EXCEPTION 'MASSIVE_MINUTE_PARTITION_REPLAY_CONFLICT';END IF;
 PERFORM population_top3000_202608_202609_v1.massive_minute_resource_guard_v2();done:=last_row=p.result_count AND p.next_url IS NULL;cp:=CASE WHEN last_row=p.result_count THEN jsonb_build_object('next_page',pn+1,'last_source_row',0) ELSE jsonb_build_object('next_page',pn,'last_source_row',last_row) END;
 RETURN jsonb_build_object('state',CASE WHEN done THEN 'COMPLETE' ELSE 'CONTINUE' END,'checkpoint',cp,'commit_receipt',jsonb_build_object('commit_key','massive-minute-normalize:'||p.source_id||':'||prior_row::text||':'||last_row::text,'output_sha256',expected_hash,'new_committed_units',new_rows,'immutable_output_ids',jsonb_build_array(p.source_id,p.page_id),'input_versions',t.input_versions,'company_population_verified',false,'source_occurrences_only',true));
END; $function$;


REVOKE ALL ON FUNCTION population_top3000_202608_202609_v1.massive_minute_normalize_v2(uuid,text) FROM PUBLIC,anon,authenticated;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.massive_minute_number_v2(p_value jsonb)
 RETURNS numeric
 LANGUAGE sql
 IMMUTABLE
 SET search_path TO 'pg_catalog'
AS $function$
 SELECT CASE WHEN jsonb_typeof(p_value)='number' AND length(p_value::text)<=96 THEN (p_value::text)::numeric ELSE NULL END;
$function$;


REVOKE ALL ON FUNCTION population_top3000_202608_202609_v1.massive_minute_number_v2(jsonb) FROM PUBLIC,anon,authenticated;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.massive_minute_parse_v2(p_body jsonb, p_first integer, p_last integer)
 RETURNS TABLE(source_row integer, provider_time_ms numeric, bar_start_at timestamp with time zone, provider_ny_date date, provider_open numeric, provider_high numeric, provider_low numeric, provider_close numeric, share_volume numeric, trade_count numeric, provider_vwap numeric, provider_otc boolean, missing_mask integer, invalid_mask integer, structure_mask integer, raw_record_sha256 bytea)
 LANGUAGE sql
 STABLE
 SET search_path TO 'pg_catalog'
 SET "TimeZone" TO 'UTC'
AS $function$
 WITH raw AS MATERIALIZED (
  SELECT e.value AS rec,e.ordinality::integer AS ord FROM jsonb_array_elements(CASE WHEN jsonb_typeof(p_body->'results')='array' THEN p_body->'results' ELSE '[]'::jsonb END) WITH ORDINALITY e
  WHERE e.ordinality BETWEEN p_first AND p_last AND p_first>=1 AND p_last<=p_first+999 AND p_last<=50000
 ),nums AS MATERIALIZED (
  SELECT r.*,population_top3000_202608_202609_v1.massive_minute_number_v2(rec->'t') AS tm,
   population_top3000_202608_202609_v1.massive_minute_number_v2(rec->'o') AS op,population_top3000_202608_202609_v1.massive_minute_number_v2(rec->'h') AS hi,
   population_top3000_202608_202609_v1.massive_minute_number_v2(rec->'l') AS lo,population_top3000_202608_202609_v1.massive_minute_number_v2(rec->'c') AS cl,
   population_top3000_202608_202609_v1.massive_minute_number_v2(rec->'v') AS vol,population_top3000_202608_202609_v1.massive_minute_number_v2(rec->'n') AS trades,
   population_top3000_202608_202609_v1.massive_minute_number_v2(rec->'vw') AS vwap,CASE WHEN jsonb_typeof(rec->'otc')='boolean' THEN (rec->>'otc')::boolean ELSE NULL END AS otc FROM raw r
 ),dated AS MATERIALIZED (
  SELECT n.*,CASE WHEN tm BETWEEN 0 AND 4102444800000 AND tm=trunc(tm) THEN to_timestamp((tm/1000)::double precision) ELSE NULL END AS ts FROM nums n
 )
 SELECT ord,tm,ts,(ts AT TIME ZONE 'America/New_York')::date,op,hi,lo,cl,vol,trades,vwap,otc,
  (CASE WHEN rec->>'t' IS NULL THEN 1 ELSE 0 END)+(CASE WHEN rec->>'o' IS NULL THEN 2 ELSE 0 END)+(CASE WHEN rec->>'h' IS NULL THEN 4 ELSE 0 END)+(CASE WHEN rec->>'l' IS NULL THEN 8 ELSE 0 END)+(CASE WHEN rec->>'c' IS NULL THEN 16 ELSE 0 END)+(CASE WHEN rec->>'v' IS NULL THEN 32 ELSE 0 END)+(CASE WHEN rec->>'n' IS NULL THEN 64 ELSE 0 END)+(CASE WHEN rec->>'vw' IS NULL THEN 128 ELSE 0 END)+(CASE WHEN rec->>'otc' IS NULL THEN 256 ELSE 0 END),
  (CASE WHEN rec->>'t' IS NOT NULL AND (tm IS NULL OR ts IS NULL) THEN 1 ELSE 0 END)+
  (CASE WHEN rec->>'o' IS NOT NULL AND (op IS NULL OR op<=0) THEN 2 ELSE 0 END)+(CASE WHEN rec->>'h' IS NOT NULL AND (hi IS NULL OR hi<=0) THEN 4 ELSE 0 END)+
  (CASE WHEN rec->>'l' IS NOT NULL AND (lo IS NULL OR lo<=0) THEN 8 ELSE 0 END)+(CASE WHEN rec->>'c' IS NOT NULL AND (cl IS NULL OR cl<=0) THEN 16 ELSE 0 END)+
  (CASE WHEN rec->>'v' IS NOT NULL AND (vol IS NULL OR vol<0) THEN 32 ELSE 0 END)+(CASE WHEN rec->>'n' IS NOT NULL AND (trades IS NULL OR trades<0 OR trades<>trunc(trades)) THEN 64 ELSE 0 END)+
  (CASE WHEN rec->>'vw' IS NOT NULL AND (vwap IS NULL OR vwap<=0) THEN 128 ELSE 0 END)+(CASE WHEN rec->>'otc' IS NOT NULL AND otc IS NULL THEN 256 ELSE 0 END),
  (CASE WHEN op>0 AND hi>0 AND lo>0 AND cl>0 AND (lo>hi OR op<lo OR op>hi OR cl<lo OR cl>hi) THEN 1 ELSE 0 END)+
  (CASE WHEN tm IS NOT NULL AND (tm<1788926400000 OR tm>=1789012800000) THEN 2 ELSE 0 END)+
  (CASE WHEN tm IS NOT NULL AND mod(tm,60000)<>0 THEN 4 ELSE 0 END),sha256(convert_to(rec::text,'UTF8'))
 FROM dated ORDER BY ord;
$function$;


REVOKE ALL ON FUNCTION population_top3000_202608_202609_v1.massive_minute_parse_v2(jsonb,integer,integer) FROM PUBLIC,anon,authenticated;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.massive_minute_quality_hold_v2(p_attempt uuid, p_token text)
 RETURNS jsonb
 LANGUAGE plpgsql
 SET search_path TO 'pg_catalog'
AS $function$
DECLARE tid uuid;t population_top3000_202608_202609_v1.pipeline_task%rowtype;c jsonb;chain jsonb;
BEGIN
 tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);SELECT * INTO t FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=tid;
 IF t.stage<>'VALIDATE' THEN RAISE EXCEPTION 'MASSIVE_MINUTE_STAGE_MISMATCH';END IF;c:=population_top3000_202608_202609_v1.massive_minute_context_v2(tid);chain:=population_top3000_202608_202609_v1.massive_minute_chain_v2(c->>'archive_key',t.input_versions);
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.massive_minute_page_v2 WHERE archive_key=c->>'archive_key' AND (verified_at IS NULL OR verified_rows<>result_count))
 OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.massive_minute_page_v2 WHERE archive_key=c->>'archive_key' AND next_url IS NULL AND source_summary->>'source_records_reconciled'='true' AND source_summary->'source_manifest'=chain) THEN RAISE EXCEPTION 'MASSIVE_MINUTE_COMPLETE_SOURCE_RECONCILIATION_REQUIRED';END IF;
 RETURN jsonb_build_object('state','BLOCKED','blocker_code','MASSIVE_MINUTE_IDENTITY_EXPECTED_PRINTS_AND_ACCURACY_REQUIRED','resumption_condition',jsonb_build_object('event','REVIEWED_COMPANY_IDENTITY_EXPECTED_PRINTS_AND_PRICE_EVIDENCE_AVAILABLE','archive_key',c->>'archive_key','required_evidence',jsonb_build_array('FROZEN_3000_COMPANY_UNIVERSE_AND_DATED_SECURITY_MAPPING','INDEPENDENT_EXPECTED_TRADE_SESSION_POPULATION_AND_ABSENCE_RULES','QUALIFYING_TRADE_AND_REGULAR_EXTENDED_SESSION_SEMANTICS','INDEPENDENT_PRICE_AND_CORPORATE_ACTION_RECONCILIATION','ORIGINAL_RECEIPT_OR_EXPLICIT_ARCHIVED_AVAILABILITY_POLICY'),'raw_source_reconciliation_is_not_company_coverage',true,'dense_clock_denominator_used',false,'no_missing_bar_zero_fill',true,'sip_feed_verified',false,'publication_eligible',false,'automatic_retry',false));
END; $function$;


REVOKE ALL ON FUNCTION population_top3000_202608_202609_v1.massive_minute_quality_hold_v2(uuid,text) FROM PUBLIC,anon,authenticated;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.massive_minute_read_source_v2(p_page_id bigint)
 RETURNS jsonb
 LANGUAGE plpgsql
 STABLE
 SET search_path TO 'pg_catalog'
AS $function$
DECLARE c jsonb;p population_top3000_202608_202609_v1.massive_minute_page_v2%rowtype;d population_top3000_202608_202609_v1.source_document%rowtype;b jsonb;arr jsonb;presence text;next_value text;
BEGIN
 SELECT * INTO p FROM population_top3000_202608_202609_v1.massive_minute_page_v2 WHERE page_id=p_page_id;IF NOT FOUND THEN RAISE EXCEPTION 'MASSIVE_MINUTE_PAGE_REQUIRED';END IF;
 c:=population_top3000_202608_202609_v1.massive_minute_context_v2(p.acquire_task_id);
 SELECT * INTO d FROM population_top3000_202608_202609_v1.source_document WHERE source_id=p.source_id;
 IF NOT FOUND OR d.source_provider IS DISTINCT FROM 'Massive' OR d.content_text IS NULL OR d.source_sha256 IS DISTINCT FROM p.source_sha256 OR d.source_sha256 IS DISTINCT FROM encode(sha256(convert_to(d.content_text,'UTF8')),'hex')
 OR d.content_bytes IS DISTINCT FROM p.response_bytes OR d.content_bytes IS DISTINCT FROM octet_length(d.content_text)::bigint OR d.source_uri IS DISTINCT FROM p.request_url OR d.retrieved_at IS DISTINCT FROM p.retrieved_at OR d.source_published_at IS NOT NULL OR d.historical_received_at IS NOT NULL
 OR d.license_policy IS DISTINCT FROM 'EXISTING_MASSIVE_SUBSCRIPTION_API_RESPONSE_INTERNAL_USE'
 OR d.evidence->>'wire_sha256' IS DISTINCT FROM p.wire_sha256 OR (d.evidence->>'wire_bytes')::bigint IS DISTINCT FROM p.wire_bytes OR (d.evidence->>'stored_content_bytes')::bigint IS DISTINCT FROM p.response_bytes
 OR d.evidence->>'hash_representation' IS DISTINCT FROM 'EXACT_UTF8_RESPONSE_EXCEPT_CREDENTIAL_REDACTED_NEXT_URL'
 OR d.evidence->>'source_symbol' IS DISTINCT FROM c->>'source_symbol' OR d.evidence->>'trading_date' IS DISTINCT FROM '2026-09-09' OR d.evidence->>'window_start_ms' IS DISTINCT FROM '1788926400000' OR d.evidence->>'window_end_exclusive_ms' IS DISTINCT FROM '1789012800000' OR d.evidence->'adjusted' IS DISTINCT FROM 'false'::jsonb
 OR (d.evidence->>'page_number')::integer IS DISTINCT FROM p.page_number OR d.evidence->>'results_presence' IS DISTINCT FROM p.results_presence THEN RAISE EXCEPTION 'MASSIVE_MINUTE_RETAINED_SOURCE_INTEGRITY';END IF;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_attempt a JOIN population_top3000_202608_202609_v1.pipeline_provider_lease l USING(attempt_id) WHERE a.attempt_id=p.attempt_id AND a.task_id=p.acquire_task_id AND l.provider='Massive' AND l.reserved_bytes=8388608) THEN RAISE EXCEPTION 'MASSIVE_MINUTE_SOURCE_ACQUISITION_PROVENANCE';END IF;
 PERFORM population_top3000_202608_202609_v1.massive_minute_url_v2(p.request_url,p.page_number=1,c->>'source_symbol');
 IF p.page_number>1 AND NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.massive_minute_page_v2 prev WHERE prev.archive_key=p.archive_key AND prev.page_number=p.page_number-1 AND prev.next_url=p.request_url AND prev.input_versions=p.input_versions) THEN RAISE EXCEPTION 'MASSIVE_MINUTE_PAGE_CHAIN_BROKEN';END IF;
 b:=d.content_text::jsonb;arr:=population_top3000_202608_202609_v1.massive_minute_results_v2(b,c->>'source_symbol');presence:=CASE WHEN b ? 'results' THEN 'ARRAY' ELSE 'OMITTED_WITH_EXPLICIT_ZERO' END;next_value:=nullif(b->>'next_url','');
 IF next_value IS NOT NULL THEN PERFORM population_top3000_202608_202609_v1.massive_minute_url_v2(next_value,false,c->>'source_symbol');END IF;
 IF jsonb_array_length(arr)<>p.result_count OR presence IS DISTINCT FROM p.results_presence OR next_value IS DISTINCT FROM p.next_url THEN RAISE EXCEPTION 'MASSIVE_MINUTE_ENVELOPE_MANIFEST_MISMATCH';END IF;
 RETURN b;
END; $function$;


REVOKE ALL ON FUNCTION population_top3000_202608_202609_v1.massive_minute_read_source_v2(bigint) FROM PUBLIC,anon,authenticated;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.massive_minute_resource_guard_v2()
 RETURNS bigint
 LANGUAGE plpgsql
 SET search_path TO 'pg_catalog'
AS $function$
DECLARE cap bigint;n bigint;
BEGIN
 SELECT (budget_evidence#>>'{resource_limits,max_relation_bytes}')::bigint INTO cap FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton;
 IF cap IS NULL OR cap<1 THEN RAISE EXCEPTION 'MASSIVE_MINUTE_RESOURCE_BOUND_NOT_ADMITTED';END IF;
 SELECT coalesce(sum(pg_total_relation_size(c.oid)),0) INTO n FROM pg_class c JOIN pg_namespace ns ON ns.oid=c.relnamespace WHERE ns.nspname='population_top3000_202608_202609_v1' AND c.relkind IN('r','m');
 IF n>cap THEN RAISE EXCEPTION 'MASSIVE_MINUTE_MISSION_RELATION_CAP_EXCEEDED';END IF;RETURN n;
END; $function$;


REVOKE ALL ON FUNCTION population_top3000_202608_202609_v1.massive_minute_resource_guard_v2() FROM PUBLIC,anon,authenticated;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.massive_minute_results_v2(p_body jsonb, p_symbol text)
 RETURNS jsonb
 LANGUAGE plpgsql
 IMMUTABLE
 SET search_path TO 'pg_catalog'
AS $function$
DECLARE a jsonb;f text;n numeric;
BEGIN
 IF jsonb_typeof(p_body) IS DISTINCT FROM 'object' OR p_body->>'status' IS DISTINCT FROM 'OK' OR p_symbol IS NULL OR p_symbol NOT IN('AAPL','GOOG','GOOGL','NVDA','WEAV') OR p_body->>'ticker' IS DISTINCT FROM p_symbol OR p_body->'adjusted' IS DISTINCT FROM 'false'::jsonb THEN RAISE EXCEPTION 'MASSIVE_MINUTE_SOURCE_ENVELOPE_OR_ADJUSTMENT';END IF;
 IF jsonb_typeof(p_body->'results')='array' THEN a:=p_body->'results';
 ELSIF NOT(p_body ? 'results') AND jsonb_typeof(p_body->'resultsCount')='number' AND (p_body->>'resultsCount')::numeric=0 THEN a:='[]'::jsonb;
 ELSE RAISE EXCEPTION 'MASSIVE_MINUTE_UNPROVEN_EMPTY_RESULTS';END IF;
 IF jsonb_array_length(a)>50000 OR EXISTS(SELECT 1 FROM jsonb_array_elements(a) x WHERE jsonb_typeof(x) IS DISTINCT FROM 'object') THEN RAISE EXCEPTION 'MASSIVE_MINUTE_SOURCE_RECORD_LIMIT_OR_TYPE';END IF;
 FOREACH f IN ARRAY ARRAY['queryCount','resultsCount'] LOOP
  IF p_body ? f THEN
   IF jsonb_typeof(p_body->f) IS DISTINCT FROM 'number' THEN RAISE EXCEPTION 'MASSIVE_MINUTE_SOURCE_COUNT_INVALID';END IF;
   n:=(p_body->>f)::numeric;
   IF n<0 OR n>9007199254740991 OR n<>trunc(n) OR (f='resultsCount' AND n<>jsonb_array_length(a)) THEN RAISE EXCEPTION 'MASSIVE_MINUTE_SOURCE_COUNT_INVALID';END IF;
  END IF;
 END LOOP;
 RETURN a;
END; $function$;


REVOKE ALL ON FUNCTION population_top3000_202608_202609_v1.massive_minute_results_v2(jsonb,text) FROM PUBLIC,anon,authenticated;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.massive_minute_route_acquire_v2(p_attempt uuid, p_token text)
 RETURNS jsonb
 LANGUAGE sql
 SET search_path TO 'pg_catalog'
AS $function$ SELECT population_top3000_202608_202609_v1.massive_minute_route_v2(p_attempt,p_token,'ACQUIRE'); $function$;


REVOKE ALL ON FUNCTION population_top3000_202608_202609_v1.massive_minute_route_acquire_v2(uuid,text) FROM PUBLIC,anon,authenticated;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.massive_minute_route_derive_v2(p_attempt uuid, p_token text)
 RETURNS jsonb
 LANGUAGE sql
 SET search_path TO 'pg_catalog'
AS $function$ SELECT population_top3000_202608_202609_v1.massive_minute_route_v2(p_attempt,p_token,'DERIVE'); $function$;


REVOKE ALL ON FUNCTION population_top3000_202608_202609_v1.massive_minute_route_derive_v2(uuid,text) FROM PUBLIC,anon,authenticated;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.massive_minute_route_normalize_v2(p_attempt uuid, p_token text)
 RETURNS jsonb
 LANGUAGE sql
 SET search_path TO 'pg_catalog'
AS $function$ SELECT population_top3000_202608_202609_v1.massive_minute_route_v2(p_attempt,p_token,'NORMALIZE'); $function$;


REVOKE ALL ON FUNCTION population_top3000_202608_202609_v1.massive_minute_route_normalize_v2(uuid,text) FROM PUBLIC,anon,authenticated;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.massive_minute_route_v2(p_attempt uuid, p_token text, p_stage text)
 RETURNS jsonb
 LANGUAGE plpgsql
 SET search_path TO 'pg_catalog'
AS $function$
DECLARE tid uuid;t population_top3000_202608_202609_v1.pipeline_task%rowtype;l population_top3000_202608_202609_v1.massive_minute_legacy_adapter_v1%rowtype;r jsonb;n text;fn text;
BEGIN
 tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);SELECT * INTO t FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=tid;
 IF t.family_id<>'P01' OR t.stage IS DISTINCT FROM p_stage OR p_stage NOT IN('ACQUIRE','NORMALIZE','DERIVE','VALIDATE') THEN RAISE EXCEPTION 'MASSIVE_MINUTE_ROUTE_STAGE_MISMATCH';END IF;
 IF t.payload->>'kind'='MASSIVE_MINUTE_KNOWN5_V2' THEN
  CASE p_stage
   WHEN 'ACQUIRE' THEN RETURN population_top3000_202608_202609_v1.massive_minute_acquire_v2(p_attempt,p_token);
   WHEN 'NORMALIZE' THEN RETURN population_top3000_202608_202609_v1.massive_minute_normalize_v2(p_attempt,p_token);
   WHEN 'DERIVE' THEN RETURN population_top3000_202608_202609_v1.massive_minute_derive_v2(p_attempt,p_token);
   ELSE RETURN population_top3000_202608_202609_v1.massive_minute_quality_hold_v2(p_attempt,p_token);
  END CASE;
 END IF;
 IF encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.massive_minute_route_v1(uuid,text,text)'::regprocedure),'UTF8')),'hex') IS DISTINCT FROM '9feeaa3683b3003942ec9f2fd4cfd01fb91df299d02174b6f3391a8d719bd8ac' THEN RAISE EXCEPTION 'MASSIVE_MINUTE_PRESERVED_ROUTE_CHANGED';END IF;
 RETURN population_top3000_202608_202609_v1.massive_minute_route_v1(p_attempt,p_token,p_stage);
END; $function$;


REVOKE ALL ON FUNCTION population_top3000_202608_202609_v1.massive_minute_route_v2(uuid,text,text) FROM PUBLIC,anon,authenticated;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.massive_minute_route_validate_v2(p_attempt uuid, p_token text)
 RETURNS jsonb
 LANGUAGE sql
 SET search_path TO 'pg_catalog'
AS $function$ SELECT population_top3000_202608_202609_v1.massive_minute_route_v2(p_attempt,p_token,'VALIDATE'); $function$;


REVOKE ALL ON FUNCTION population_top3000_202608_202609_v1.massive_minute_route_validate_v2(uuid,text) FROM PUBLIC,anon,authenticated;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.massive_minute_url_v2(p_url text, p_first_allowed boolean, p_symbol text)
 RETURNS text
 LANGUAGE plpgsql
 IMMUTABLE
 SET search_path TO 'pg_catalog'
AS $function$
BEGIN
 IF p_symbol IS NULL OR p_symbol NOT IN('AAPL','GOOG','GOOGL','NVDA','WEAV') THEN RAISE EXCEPTION 'MASSIVE_MINUTE_KNOWN_SYMBOL_REQUIRED';END IF;
 IF p_first_allowed AND p_url='https://api.massive.com/v2/aggs/ticker/'||p_symbol||'/range/1/minute/1788926400000/1789012799999?adjusted=false&sort=asc&limit=50000' THEN RETURN p_url;END IF;
 IF p_url IS NULL OR length(p_url)>16384 OR p_url!~('^https://api[.]massive[.]com/v2/aggs/ticker/'||p_symbol||'/range/1/minute/1788926400000/1789012799999[?]cursor=[-A-Za-z0-9._~%+*]+$') THEN RAISE EXCEPTION 'MASSIVE_MINUTE_UNADMITTED_URL';END IF;
 RETURN p_url;
END; $function$;


REVOKE ALL ON FUNCTION population_top3000_202608_202609_v1.massive_minute_url_v2(text,boolean,text) FROM PUBLIC,anon,authenticated;
SET check_function_bodies=on;
