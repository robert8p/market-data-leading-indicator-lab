-- Additive source-supported lifecycle exceptions and explicit per-record quarantine.
-- Original v1 parser, source bundle, requests and all accepted rows remain immutable.
CREATE TABLE population_top3000_202608_202609_v1.ctx02_lifecycle_evidence_v6(
 evidence_id text PRIMARY KEY,source_uri text NOT NULL,source_body text NOT NULL,source_sha256 text NOT NULL,
 representation text NOT NULL CHECK(representation='FIRECRAWL_PDF_EXTRACTED_MARKDOWN_NOT_ORIGINAL_WIRE_PDF'),
 retained_at timestamptz NOT NULL,source_acquired_at timestamptz,source_publication_date date NOT NULL,historical_received_at timestamptz,
 review_sha256 text NOT NULL CHECK(review_sha256~'^[0-9a-f]{64}$'),
 CHECK(source_sha256=encode(sha256(convert_to(source_body,'UTF8')),'hex')),CHECK(historical_received_at IS NULL));
CREATE TABLE population_top3000_202608_202609_v1.ctx02_census_quarantine_v6(
 source_id text NOT NULL REFERENCES population_top3000_202608_202609_v1.source_document,
 source_ordinal integer NOT NULL CHECK(source_ordinal BETWEEN 1 AND 1000),source_record jsonb NOT NULL,
 source_record_sha256 text NOT NULL,reason text NOT NULL,first_attempt_id uuid NOT NULL,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),PRIMARY KEY(source_id,source_ordinal),
 CHECK(source_record_sha256=encode(sha256(convert_to(source_record::text,'UTF8')),'hex')));
ALTER TABLE population_top3000_202608_202609_v1.ctx02_lifecycle_evidence_v6 ENABLE ROW LEVEL SECURITY;
ALTER TABLE population_top3000_202608_202609_v1.ctx02_census_quarantine_v6 ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON population_top3000_202608_202609_v1.ctx02_lifecycle_evidence_v6,population_top3000_202608_202609_v1.ctx02_census_quarantine_v6 FROM PUBLIC,anon,authenticated;
CREATE FUNCTION population_top3000_202608_202609_v1.ctx02_census_classify_record_v6(r jsonb,c jsonb)
RETURNS jsonb LANGUAGE plpgsql STABLE SET search_path=pg_catalog AS $f$
DECLARE n jsonb;msg text;symdate date;bodysha text;
BEGIN
 BEGIN
  n:=population_top3000_202608_202609_v1.ctx02_census_parse_record_v1(r,c);
  RETURN jsonb_build_object('classification','VALID','payload',n);
 EXCEPTION WHEN raise_exception THEN msg:=SQLERRM;END;
 IF msg='CTX02_CONTRACT_SYMBOL_FIELDS_CONFLICT' AND r->>'underlying_ticker'='AVNS'
 AND r->>'ticker'~'^O:AVNS[0-9]{6}[CP][0-9]{8}$' AND r->>'expiration_date'='2026-08-21'
 AND r->'additional_underlyings'='[{"type":"currency","amount":2500,"underlying":"USD"}]'::jsonb
 AND r->>'shares_per_contract'='100' AND c->>'as_of'='2026-08-03'
 THEN
  symdate:=to_date(substring(r->>'ticker' from '([0-9]{6})[CP][0-9]{8}$'),'YYMMDD');
  SELECT source_sha256 INTO bodysha FROM population_top3000_202608_202609_v1.ctx02_lifecycle_evidence_v6
  WHERE evidence_id='OCC59450_AVNS_20260727' AND source_uri='https://infomemo.theocc.com/infomemos?number=59450'
  AND source_sha256='9bf2b2b4edc3e00c9014dbf373cc3d3f52a39918a0cfeec9f3d069dbc46363eb'
  AND source_sha256=encode(sha256(convert_to(source_body,'UTF8')),'hex') AND source_publication_date=DATE '2026-07-27'
  AND retained_at<=clock_timestamp() AND historical_received_at IS NULL;
  IF bodysha IS NOT NULL AND symdate>DATE '2026-08-21' THEN
   -- Validate every other old invariant with the symbol date; restore untouched original source values.
   BEGIN
    n:=population_top3000_202608_202609_v1.ctx02_census_parse_record_v1(jsonb_set(r,'{expiration_date}',to_jsonb(symdate::text)),c);
   EXCEPTION WHEN raise_exception THEN n:=NULL;msg:=SQLERRM;END;
   IF n IS NOT NULL THEN
   n:=n||jsonb_build_object('source_record',r,'expiration_date',r->'expiration_date','symbol_expiration_date',symdate,
    'contract_life_state','CASH_SETTLED_ACCELERATED_EXPIRY_SOURCE_SUPPORTED','deliverable_kind','CASH_ONLY',
    'effective_deliverable_cash_usd',2500,'effective_deliverable_shares',0,'source_shares_per_contract_preserved',r->'shares_per_contract',
    'lifecycle_evidence_id','OCC59450_AVNS_20260727','lifecycle_evidence_sha256',bodysha,
    'lifecycle_effective_date','2026-07-27','lifecycle_source_representation','FIRECRAWL_PDF_EXTRACTED_MARKDOWN_NOT_ORIGINAL_WIRE_PDF',
    'historical_received_at',NULL,'original_receipt_qualified',false,'family_accepted',false);
   RETURN jsonb_build_object('classification','VALID_LIFECYCLE_SUPPORTED','payload',n);
   END IF;
  END IF;
 END IF;
 IF msg IN('CTX02_CONTRACT_SYMBOL_FIELDS_CONFLICT','CTX02_REQUIRED_CONTRACT_NUMBER_MISSING','CTX02_CONTRACT_IDENTITY_INVALID','CTX02_REFERENCE_DATE_INVALID','CTX02_REFERENCE_DATE_OUT_OF_SCOPE','CTX02_ADDITIONAL_DELIVERABLE_INVALID') THEN
  RETURN jsonb_build_object('classification','QUARANTINED','reason',msg,'source_record',r);
 END IF;
 RAISE EXCEPTION '%',msg;
END;$f$;
REVOKE ALL ON FUNCTION population_top3000_202608_202609_v1.ctx02_census_classify_record_v6(jsonb,jsonb) FROM PUBLIC,anon,authenticated;

-- Additive per-page processor repair. All v1 source functions, source bundle, input versions and archive keys remain unchanged.
CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.ctx02_census_process_v6(p_attempt uuid, p_token text)
 RETURNS jsonb
 LANGUAGE plpgsql
 SET search_path TO 'pg_catalog'
AS $function$
DECLARE tid uuid;t population_top3000_202608_202609_v1.pipeline_task%rowtype;c jsonb;
 q population_top3000_202608_202609_v1.ctx02_census_page_v1%rowtype;
 d population_top3000_202608_202609_v1.source_document%rowtype;
 total_pages integer;page_only integer;terminal boolean;page_no integer;off integer;stop_at integer;cnt integer;bad integer;ins integer:=0;qins integer:=0;validn integer;quarantinen integer;cls jsonb;x record;foundpayload jsonb;
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
 FOR x IN SELECT value,ordinality FROM jsonb_array_elements(body->'results') WITH ORDINALITY y(value,ordinality) WHERE ordinality>off AND ordinality<=stop_at LOOP
  cls:=population_top3000_202608_202609_v1.ctx02_census_classify_record_v6(x.value,c);
  IF cls->>'classification'='QUARANTINED' THEN
   IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.ctx02_census_record_v1 z WHERE z.source_id=d.source_id AND z.source_ordinal=x.ordinality) THEN RAISE EXCEPTION 'CTX02_V6_ACCEPTED_ROW_CANNOT_BECOME_QUARANTINED';END IF;
   IF t.stage='NORMALIZE' THEN
    INSERT INTO population_top3000_202608_202609_v1.ctx02_census_quarantine_v6(source_id,source_ordinal,source_record,source_record_sha256,reason,first_attempt_id)
    VALUES(d.source_id,x.ordinality,x.value,encode(sha256(convert_to(x.value::text,'UTF8')),'hex'),cls->>'reason',p_attempt) ON CONFLICT DO NOTHING;
    GET DIAGNOSTICS bad=ROW_COUNT;qins:=qins+bad;
   END IF;
   IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.ctx02_census_quarantine_v6 z WHERE z.source_id=d.source_id AND z.source_ordinal=x.ordinality AND z.source_record=x.value AND z.reason=cls->>'reason' AND z.source_record_sha256=encode(sha256(convert_to(x.value::text,'UTF8')),'hex')) THEN RAISE EXCEPTION 'CTX02_V6_QUARANTINE_REPLAY_MISMATCH';END IF;
  ELSE
   IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.ctx02_census_quarantine_v6 z WHERE z.source_id=d.source_id AND z.source_ordinal=x.ordinality) THEN RAISE EXCEPTION 'CTX02_V6_QUARANTINE_REQUIRES_VERSIONED_RESOLUTION';END IF;
   IF t.stage='NORMALIZE' THEN
    INSERT INTO population_top3000_202608_202609_v1.ctx02_census_record_v1(source_id,source_ordinal,payload,payload_sha256)
    VALUES(d.source_id,x.ordinality,cls->'payload',encode(sha256(convert_to((cls->'payload')::text,'UTF8')),'hex')) ON CONFLICT DO NOTHING;
    GET DIAGNOSTICS bad=ROW_COUNT;ins:=ins+bad;
   END IF;
   IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.ctx02_census_record_v1 z WHERE z.source_id=d.source_id AND z.source_ordinal=x.ordinality AND z.payload=cls->'payload' AND z.payload_sha256=encode(sha256(convert_to(z.payload::text,'UTF8')),'hex')) THEN RAISE EXCEPTION 'CTX02_CENSUS_RECORD_REPLAY_OR_DERIVE_MISMATCH';END IF;
  END IF;
 END LOOP;
 IF stop_at=cnt THEN
  SELECT count(*) INTO validn FROM population_top3000_202608_202609_v1.ctx02_census_record_v1 WHERE source_id=d.source_id;
  SELECT count(*) INTO quarantinen FROM population_top3000_202608_202609_v1.ctx02_census_quarantine_v6 WHERE source_id=d.source_id;
  IF validn+quarantinen<>cnt THEN RAISE EXCEPTION 'CTX02_CENSUS_SOURCE_RECORD_DENOMINATOR_MISMATCH';END IF;
  IF t.stage='NORMALIZE' THEN UPDATE population_top3000_202608_202609_v1.ctx02_census_page_v1 SET normalized_at=CASE WHEN quarantinen=0 THEN coalesce(normalized_at,clock_timestamp()) ELSE normalized_at END,normalized_source_records=validn WHERE archive_key=q.archive_key AND page_number=q.page_number;END IF;
  next_page:=page_no+1;next_off:=0;
 ELSE next_page:=page_no;next_off:=stop_at;END IF;
 SELECT encode(sha256(convert_to(d.source_sha256||E'\n'||coalesce(string_agg(h,E'\n' ORDER BY source_ordinal),''),'UTF8')),'hex') INTO out_sha FROM (SELECT source_ordinal,payload_sha256 h FROM population_top3000_202608_202609_v1.ctx02_census_record_v1 WHERE source_id=d.source_id AND source_ordinal>off AND source_ordinal<=stop_at UNION ALL SELECT source_ordinal,'QUARANTINED:'||source_record_sha256||':'||reason FROM population_top3000_202608_202609_v1.ctx02_census_quarantine_v6 WHERE source_id=d.source_id AND source_ordinal>off AND source_ordinal<=stop_at)x;
 state:=CASE WHEN (page_only IS NOT NULL AND stop_at=cnt) OR next_page>total_pages THEN 'COMPLETE' ELSE 'CONTINUE' END;
 PERFORM population_top3000_202608_202609_v1.d02_daily_resource_guard_v2();
 receipt:=jsonb_build_object('commit_key','ctx02-census-v1:'||t.stage||':'||d.source_id||':'||off||':'||stop_at,'output_sha256',out_sha,'new_committed_units',ins+qins,'new_valid_units',ins,'new_quarantined_units',qins,'page_valid_units',validn,'page_quarantined_units',quarantinen,'page_source_fully_normalized',quarantinen=0,
 'immutable_output_ids',jsonb_build_array(d.source_id),'input_versions',t.input_versions,'processing_function_sha256',encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.ctx02_census_process_v6(uuid,text)'::regprocedure),'UTF8')),'hex'),'processing_revision','CTX02_CENSUS_LIFECYCLE_QUARANTINE_V6','source_input_versions_preserved',true,'source_records_in_chunk',stop_at-off,'source_page_records',cnt,'retained_acquisition_pages',total_pages,'source_page_chain_terminal',terminal,'source_page_number',page_only,'required_scope_accepted',false,'original_receipt_qualified',false,'no_trade_inferred',false);
 RETURN jsonb_build_object('state',state,'checkpoint',jsonb_build_object('work_page',next_page,'work_offset',next_off),'commit_receipt',receipt);
END; $function$;


CREATE FUNCTION population_top3000_202608_202609_v1.ctx02_lifecycle_bundle_v6() RETURNS text LANGUAGE sql STABLE SET search_path=pg_catalog AS $f$
SELECT encode(sha256(convert_to(string_agg(pg_get_functiondef(p.oid),E'\n' ORDER BY p.proname),'UTF8')),'hex') FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='population_top3000_202608_202609_v1' AND p.proname IN('ctx02_census_classify_record_v6','ctx02_census_process_v6');$f$;

CREATE FUNCTION population_top3000_202608_202609_v1.ctx02_census_normalize_v6(p_attempt uuid,p_token text)
RETURNS jsonb LANGUAGE plpgsql SET search_path=pg_catalog AS $f$
DECLARE tid uuid;k text;expected text;
BEGIN
 tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);
 SELECT payload->>'kind' INTO k FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=tid;
 IF k='CTX02_CENSUS_SOURCE_V1' THEN
  SELECT reviewed_test_evidence#>>'{ctx02_v6,bundle_sha256}' INTO expected FROM population_top3000_202608_202609_v1.pipeline_adapter WHERE family_id='CTX02' AND stage=upper('normalize');
  IF expected IS NULL OR expected IS DISTINCT FROM population_top3000_202608_202609_v1.ctx02_lifecycle_bundle_v6() THEN RAISE EXCEPTION 'CTX02_V6_HELPER_PIN_CHANGED';END IF;
  RETURN population_top3000_202608_202609_v1.ctx02_census_process_v6(p_attempt,p_token);
 END IF;
 RETURN population_top3000_202608_202609_v1.ctx02_normalize_sample_v1(p_attempt,p_token);
END;$f$;

CREATE FUNCTION population_top3000_202608_202609_v1.ctx02_census_derive_v6(p_attempt uuid,p_token text)
RETURNS jsonb LANGUAGE plpgsql SET search_path=pg_catalog AS $f$
DECLARE tid uuid;k text;expected text;
BEGIN
 tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);
 SELECT payload->>'kind' INTO k FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=tid;
 IF k='CTX02_CENSUS_SOURCE_V1' THEN
  SELECT reviewed_test_evidence#>>'{ctx02_v6,bundle_sha256}' INTO expected FROM population_top3000_202608_202609_v1.pipeline_adapter WHERE family_id='CTX02' AND stage=upper('derive');
  IF expected IS NULL OR expected IS DISTINCT FROM population_top3000_202608_202609_v1.ctx02_lifecycle_bundle_v6() THEN RAISE EXCEPTION 'CTX02_V6_HELPER_PIN_CHANGED';END IF;
  RETURN population_top3000_202608_202609_v1.ctx02_census_process_v6(p_attempt,p_token);
 END IF;
 RETURN population_top3000_202608_202609_v1.ctx02_derive_sample_v1(p_attempt,p_token);
END;$f$;

CREATE FUNCTION population_top3000_202608_202609_v1.ctx02_census_validate_v6(p_attempt uuid,p_token text)
RETURNS jsonb LANGUAGE plpgsql SET search_path=pg_catalog AS $f$
DECLARE tid uuid;k text;expected text;
BEGIN
 tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);
 SELECT payload->>'kind' INTO k FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=tid;
 IF k='CTX02_CENSUS_SOURCE_V1' THEN
  SELECT reviewed_test_evidence#>>'{ctx02_v6,bundle_sha256}' INTO expected FROM population_top3000_202608_202609_v1.pipeline_adapter WHERE family_id='CTX02' AND stage=upper('validate');
  IF expected IS NULL OR expected IS DISTINCT FROM population_top3000_202608_202609_v1.ctx02_lifecycle_bundle_v6() THEN RAISE EXCEPTION 'CTX02_V6_HELPER_PIN_CHANGED';END IF;
  RETURN population_top3000_202608_202609_v1.ctx02_census_process_v6(p_attempt,p_token);
 END IF;
 RETURN population_top3000_202608_202609_v1.ctx02_sample_quality_hold_v1(p_attempt,p_token);
END;$f$;
REVOKE ALL ON FUNCTION population_top3000_202608_202609_v1.ctx02_census_process_v6(uuid,text),population_top3000_202608_202609_v1.ctx02_lifecycle_bundle_v6(),population_top3000_202608_202609_v1.ctx02_census_normalize_v6(uuid,text),population_top3000_202608_202609_v1.ctx02_census_derive_v6(uuid,text),population_top3000_202608_202609_v1.ctx02_census_validate_v6(uuid,text) FROM PUBLIC,anon,authenticated;
