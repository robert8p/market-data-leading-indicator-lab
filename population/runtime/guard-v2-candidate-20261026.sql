-- REVIEW CANDIDATE ONLY. Do not run standalone in production.
-- Legacy v1 definitions and immutable bundle hashes must remain unchanged.
CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.alpaca_sip_resource_guard_v2()
 RETURNS bigint
 LANGUAGE plpgsql
 SET search_path TO 'pg_catalog'
AS $function$
DECLARE bound bigint;actual bigint;
BEGIN
 SELECT (budget_evidence#>>'{resource_limits,max_relation_bytes}')::bigint INTO bound FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton;
 IF bound IS NULL OR bound<1 THEN RAISE EXCEPTION 'ALPACA_SIP_RESOURCE_BOUND_NOT_ADMITTED';END IF;
 SELECT coalesce(sum(pg_total_relation_size(c.oid)),0) INTO actual FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='population_top3000_202608_202609_v1' AND c.relkind IN('r','m');
 IF actual>bound THEN RAISE EXCEPTION 'ALPACA_SIP_POST_BATCH_RELATION_CAP_EXCEEDED';END IF;
 RETURN actual;
END $function$
;
REVOKE ALL ON FUNCTION population_top3000_202608_202609_v1.alpaca_sip_resource_guard_v2() FROM PUBLIC,anon,authenticated;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.massive_news_resource_guard_v2()
 RETURNS bigint
 LANGUAGE plpgsql
 SET search_path TO 'pg_catalog'
 SET "TimeZone" TO 'UTC'
AS $function$
DECLARE bound bigint;actual bigint;
BEGIN
 SELECT (budget_evidence#>>'{resource_limits,max_relation_bytes}')::bigint INTO bound
 FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton;
 IF bound IS NULL OR bound<1 THEN RAISE EXCEPTION 'MASSIVE_NEWS_RESOURCE_BOUND_NOT_ADMITTED';END IF;
 SELECT coalesce(sum(pg_total_relation_size(c.oid)),0) INTO actual FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
 WHERE n.nspname='population_top3000_202608_202609_v1' AND c.relkind IN('r','m');
 IF actual>bound THEN RAISE EXCEPTION 'MASSIVE_NEWS_POST_BATCH_RELATION_CAP_EXCEEDED actual=% bound=%',actual,bound;END IF;
 RETURN actual;
END $function$
;
REVOKE ALL ON FUNCTION population_top3000_202608_202609_v1.massive_news_resource_guard_v2() FROM PUBLIC,anon,authenticated;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.alpaca_sip_commit_v2(p_attempt uuid, p_token text, p_page jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 SET search_path TO 'pg_catalog'
 SET "TimeZone" TO 'UTC'
AS $function$
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
 PERFORM population_top3000_202608_202609_v1.alpaca_sip_resource_guard_v2();
 PERFORM population_top3000_202608_202609_v1.pipeline_provider_release_v1(p_attempt,p_token,bytes,0,0);
 r:=jsonb_build_object('state',CASE WHEN nexttoken IS NULL THEN 'COMPLETE' ELSE 'CONTINUE' END,'checkpoint',jsonb_build_object('next_page',n+1,'last_source_id',sid,'last_source_sha256',h,'next_page_token',nexttoken,'source_bars_seen',coalesce((t.checkpoint->>'source_bars_seen')::bigint,0)+cnt),
 'not_before',(SELECT next_provider_at FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton),
 'commit_receipt',jsonb_build_object('commit_key','alpaca-source-page:'||sid||':'||h,'output_sha256',h,'new_committed_units',fresh,'immutable_output_ids',jsonb_build_array(sid),'input_versions',t.input_versions,'raw_source_bars',cnt,'terminal_page',nexttoken IS NULL,'company_membership_certified',false));
 RETURN population_top3000_202608_202609_v1.pipeline_finish_v1(p_attempt,p_token,r);
END $function$
;
REVOKE ALL ON FUNCTION population_top3000_202608_202609_v1.alpaca_sip_commit_v2(uuid,text,jsonb) FROM PUBLIC,anon,authenticated;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.alpaca_sip_normalize_v2(p_attempt uuid, p_token text)
 RETURNS jsonb
 LANGUAGE plpgsql
 SET search_path TO 'pg_catalog'
 SET "TimeZone" TO 'UTC'
AS $function$
DECLARE tid uuid;t population_top3000_202608_202609_v1.pipeline_task%rowtype;c jsonb;p population_top3000_202608_202609_v1.alpaca_sip_source_page_v1%rowtype;
 d population_top3000_202608_202609_v1.source_document%rowtype;k text;lastpage integer;total integer;terminal integer;cnt bigint;newrows bigint;h text;actual_h text;actual_cnt bigint;relations bigint;
BEGIN
 tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);
 SELECT * INTO t FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=tid;
 IF t.stage<>'NORMALIZE' THEN RAISE EXCEPTION 'ALPACA_SIP_STAGE_MISMATCH';END IF;
 c:=population_top3000_202608_202609_v1.alpaca_sip_context_v1(tid);k:=c->>'archive_key';
 PERFORM population_top3000_202608_202609_v1.alpaca_sip_resource_guard_v2();
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
 relations:=population_top3000_202608_202609_v1.alpaca_sip_resource_guard_v2();
 RETURN jsonb_build_object('state',CASE WHEN p.page_number=terminal THEN 'COMPLETE' ELSE 'CONTINUE' END,'checkpoint',jsonb_build_object('last_normalized_page',p.page_number),
 'commit_receipt',jsonb_build_object('commit_key','alpaca-compact:'||p.source_id||':'||h,'output_sha256',h,'new_committed_units',newrows,'immutable_output_ids',jsonb_build_array(p.source_id),'input_versions',t.input_versions,'unit_kind','SOURCE_SYMBOL_MINUTE_BAR','source_bars',cnt,'shared_families',jsonb_build_array('P01','P02'),'mission_relation_bytes',relations,'historical_company_membership_verified',false));
END $function$
;
REVOKE ALL ON FUNCTION population_top3000_202608_202609_v1.alpaca_sip_normalize_v2(uuid,text) FROM PUBLIC,anon,authenticated;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.massive_news_derive_v2(p_attempt uuid, p_token text)
 RETURNS jsonb
 LANGUAGE plpgsql
 SET search_path TO 'pg_catalog'
 SET "TimeZone" TO 'UTC'
AS $function$
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
 PERFORM population_top3000_202608_202609_v1.massive_news_resource_guard_v2();
 RETURN jsonb_build_object('state','COMPLETE','commit_receipt',jsonb_build_object('commit_key','massive-news-derived:'||k||':'||h,'output_sha256',h,'new_committed_units',n,'immutable_output_ids',jsonb_build_array(k),'input_versions',t.input_versions,'raw_source_occurrences',occurrences,'distinct_content_versions',n,'duplicate_occurrences',occurrences-n,'unresolved_provider_id_version_conflicts',conflicts,'cohort_certified',false,'historical_availability_certified',false,'article_bodies_fetched',false,'independent_accuracy_verified',false));
END $function$
;
REVOKE ALL ON FUNCTION population_top3000_202608_202609_v1.massive_news_derive_v2(uuid,text) FROM PUBLIC,anon,authenticated;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.massive_news_normalize_v2(p_attempt uuid, p_token text)
 RETURNS jsonb
 LANGUAGE plpgsql
 SET search_path TO 'pg_catalog'
 SET "TimeZone" TO 'UTC'
AS $function$
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
 PERFORM population_top3000_202608_202609_v1.massive_news_resource_guard_v2();
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
 nh:=encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.massive_news_normalize_v2(uuid,text)'::regprocedure),'UTF8')),'hex');
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
 relations:=population_top3000_202608_202609_v1.massive_news_resource_guard_v2();
 h:=encode(sha256(convert_to(jsonb_build_object('source_id',p.source_id,'source_records',row_count,'normalizer_sha256',nh)::text,'UTF8')),'hex');
 RETURN jsonb_build_object('state',CASE WHEN p.page_number=terminal THEN 'COMPLETE' ELSE 'CONTINUE' END,'checkpoint',jsonb_build_object('last_normalized_page',p.page_number),
 'commit_receipt',jsonb_build_object('commit_key','massive-news-normalized:'||p.source_id||':'||nh,'output_sha256',h,'new_committed_units',new_rows,'immutable_output_ids',jsonb_build_array(p.source_id),'input_versions',t.input_versions,'source_records',row_count,'mission_relation_bytes_after_batch',relations,'cohort_certified',false));
END $function$
;
REVOKE ALL ON FUNCTION population_top3000_202608_202609_v1.massive_news_normalize_v2(uuid,text) FROM PUBLIC,anon,authenticated;
