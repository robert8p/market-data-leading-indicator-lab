-- Additive per-page processor repair. All v1 source functions, source bundle, input versions and archive keys remain unchanged.
CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.ctx02_census_process_v2(p_attempt uuid, p_token text)
 RETURNS jsonb
 LANGUAGE plpgsql
 SET search_path TO 'pg_catalog'
AS $function$
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
  IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.ctx02_census_page_v1 sp WHERE sp.archive_key=c->>'archive_key' AND sp.page_number=page_only AND sp.partition_key=c->>'request_key') THEN RAISE EXCEPTION 'CTX02_CENSUS_PAGE_TASK_SOURCE_MISSING';END IF;
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
 'immutable_output_ids',jsonb_build_array(d.source_id),'input_versions',t.input_versions,'processing_function_sha256',encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.ctx02_census_process_v2(uuid,text)'::regprocedure),'UTF8')),'hex'),'processing_revision','CTX02_CENSUS_PER_PAGE_ALIAS_FIX_V2','source_input_versions_preserved',true,'source_records_in_chunk',stop_at-off,'source_page_records',cnt,'retained_acquisition_pages',total_pages,'source_page_chain_terminal',terminal,'source_page_number',page_only,'required_scope_accepted',false,'original_receipt_qualified',false,'no_trade_inferred',false);
 RETURN jsonb_build_object('state',state,'checkpoint',jsonb_build_object('work_page',next_page,'work_offset',next_off),'commit_receipt',receipt);
END; $function$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.ctx02_census_normalize_v2(p_attempt uuid,p_token text)
RETURNS jsonb LANGUAGE plpgsql SET search_path=pg_catalog AS $f$
DECLARE tid uuid;k text;
BEGIN
 tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);
 SELECT payload->>'kind' INTO k FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=tid;
 IF k='CTX02_CENSUS_SOURCE_V1' THEN
  IF encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.ctx02_census_process_v2(uuid,text)'::regprocedure),'UTF8')),'hex') IS DISTINCT FROM '06f74300bb742f2e7a23d98e6099225b851cf4917a05d4d8a5115c50f6788e41' THEN RAISE EXCEPTION 'CTX02_CENSUS_V2_PROCESSOR_PIN_CHANGED';END IF;
  RETURN population_top3000_202608_202609_v1.ctx02_census_process_v2(p_attempt,p_token);
 END IF;
 RETURN population_top3000_202608_202609_v1.ctx02_normalize_sample_v1(p_attempt,p_token);
END;$f$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.ctx02_census_derive_v2(p_attempt uuid,p_token text)
RETURNS jsonb LANGUAGE plpgsql SET search_path=pg_catalog AS $f$
DECLARE tid uuid;k text;
BEGIN
 tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);
 SELECT payload->>'kind' INTO k FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=tid;
 IF k='CTX02_CENSUS_SOURCE_V1' THEN
  IF encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.ctx02_census_process_v2(uuid,text)'::regprocedure),'UTF8')),'hex') IS DISTINCT FROM '06f74300bb742f2e7a23d98e6099225b851cf4917a05d4d8a5115c50f6788e41' THEN RAISE EXCEPTION 'CTX02_CENSUS_V2_PROCESSOR_PIN_CHANGED';END IF;
  RETURN population_top3000_202608_202609_v1.ctx02_census_process_v2(p_attempt,p_token);
 END IF;
 RETURN population_top3000_202608_202609_v1.ctx02_derive_sample_v1(p_attempt,p_token);
END;$f$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.ctx02_census_validate_v2(p_attempt uuid,p_token text)
RETURNS jsonb LANGUAGE plpgsql SET search_path=pg_catalog AS $f$
DECLARE tid uuid;k text;
BEGIN
 tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);
 SELECT payload->>'kind' INTO k FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=tid;
 IF k='CTX02_CENSUS_SOURCE_V1' THEN
  IF encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.ctx02_census_process_v2(uuid,text)'::regprocedure),'UTF8')),'hex') IS DISTINCT FROM '06f74300bb742f2e7a23d98e6099225b851cf4917a05d4d8a5115c50f6788e41' THEN RAISE EXCEPTION 'CTX02_CENSUS_V2_PROCESSOR_PIN_CHANGED';END IF;
  RETURN population_top3000_202608_202609_v1.ctx02_census_process_v2(p_attempt,p_token);
 END IF;
 RETURN population_top3000_202608_202609_v1.ctx02_sample_quality_hold_v1(p_attempt,p_token);
END;$f$;
REVOKE ALL ON FUNCTION population_top3000_202608_202609_v1.ctx02_census_process_v2(uuid,text),population_top3000_202608_202609_v1.ctx02_census_normalize_v2(uuid,text),population_top3000_202608_202609_v1.ctx02_census_derive_v2(uuid,text),population_top3000_202608_202609_v1.ctx02_census_validate_v2(uuid,text) FROM PUBLIC,anon,authenticated;
