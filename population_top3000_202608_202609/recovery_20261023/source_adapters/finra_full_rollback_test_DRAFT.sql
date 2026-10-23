BEGIN;
-- LOCAL DRAFT. Root reviews, rollback-tests and installs. No external work here.
-- Source symbol rows remain provisional until dated company/security mapping exists.
CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.finra_scope_v1(p_attempt uuid,p_token text,p_stage text)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path TO 'pg_catalog' AS $f$
DECLARE tid uuid;t population_top3000_202608_202609_v1.pipeline_task%rowtype;
 d date;c population_top3000_202608_202609_v1.catalog_calendar%rowtype;
BEGIN
 tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);
 SELECT * INTO t FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=tid;
 IF t.input_versions->>'finra_bundle_sha256' IS DISTINCT FROM population_top3000_202608_202609_v1.finra_code_hash_v1()
 THEN RAISE EXCEPTION 'FINRA_HELPER_BUNDLE_CHANGED';END IF;
 IF t.family_id IS DISTINCT FROM 'S01' OR t.stage IS DISTINCT FROM p_stage
  OR t.payload->>'kind' IS DISTINCT FROM 'FINRA_CNMS_DAILY'
  OR coalesce(t.payload->>'session_date','') !~ '^2026-(08|09)-[0-9]{2}$'
 THEN RAISE EXCEPTION 'FINRA_SCOPE_MISMATCH';END IF;
 d:=(t.payload->>'session_date')::date;
 IF d<'2026-08-03' OR d>'2026-09-30' OR t.partition_key IS DISTINCT FROM 'CNMS:'||d::text
  OR (SELECT count(*) FROM population_top3000_202608_202609_v1.catalog_calendar WHERE session_date=d)<>1
 THEN RAISE EXCEPTION 'FINRA_DATE_NOT_IN_FROZEN_CALENDAR';END IF;
 SELECT * INTO c FROM population_top3000_202608_202609_v1.catalog_calendar WHERE session_date=d;
 IF t.input_versions->>'calendar_record_sha256' IS DISTINCT FROM c.source_record_sha256
  OR c.source_record_sha256 IS DISTINCT FROM encode(sha256(convert_to(c.source_record_text,'UTF8')),'hex')
 THEN RAISE EXCEPTION 'FINRA_CALENDAR_HASH_MISMATCH';END IF;
 RETURN jsonb_build_object('task_id',tid,'session_date',d,'source_id','S01:FINRA_CNMS:'||d::text,
  'source_uri','https://cdn.finra.org/equity/regsho/daily/CNMSshvol'||to_char(d,'YYYYMMDD')||'.txt',
  'partition_key',t.partition_key,'input_versions',t.input_versions);
END $f$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.finra_parse_cnms_v1(p_text text,p_date date)
RETURNS TABLE(source_line integer,symbol text,short_volume numeric,short_exempt_volume numeric,total_volume numeric,market text,footer_records integer)
LANGUAGE plpgsql IMMUTABLE SET search_path TO 'pg_catalog' AS $f$
DECLARE lines text[];fields text[];n integer;i integer;tail text;
BEGIN
 IF p_text IS NULL OR octet_length(p_text)>8388608 OR p_date IS NULL THEN RAISE EXCEPTION 'FINRA_INPUT_BOUND';END IF;
 lines:=string_to_array(replace(p_text,E'\r\n',E'\n'),E'\n');n:=cardinality(lines);
 WHILE n>0 AND lines[n]='' LOOP n:=n-1;END LOOP;
 IF n<2 OR lines[1] IS DISTINCT FROM 'Date|Symbol|ShortVolume|ShortExemptVolume|TotalVolume|Market'
 THEN RAISE EXCEPTION 'FINRA_HEADER_INVALID';END IF;
 tail:=lines[n];
 IF tail !~ '^[0-9]{1,7}$' THEN RAISE EXCEPTION 'FINRA_FOOTER_UNRECOGNIZED';END IF;
 footer_records:=tail::integer;
 IF footer_records<>n-2 OR footer_records>100000 THEN RAISE EXCEPTION 'FINRA_FOOTER_COUNT_MISMATCH';END IF;
 FOR i IN 2..n-1 LOOP
  fields:=string_to_array(lines[i],'|');
  IF cardinality(fields)<>6 OR fields[1] IS DISTINCT FROM to_char(p_date,'YYYYMMDD')
   OR fields[2]='' OR length(fields[2])>40 OR fields[2]~'[[:cntrl:]]'
   OR length(fields[3])>40 OR fields[3]!~'^[0-9]+([.][0-9]+)?$'
   OR length(fields[4])>40 OR fields[4]!~'^[0-9]+([.][0-9]+)?$'
   OR length(fields[5])>40 OR fields[5]!~'^[0-9]+([.][0-9]+)?$'
   OR fields[6]!~'^[A-Z]+(,[A-Z]+)*$' OR length(fields[6])>32
  THEN RAISE EXCEPTION 'FINRA_ROW_INVALID_AT_%',i;END IF;
  source_line:=i;symbol:=fields[2];short_volume:=fields[3]::numeric;
  short_exempt_volume:=fields[4]::numeric;total_volume:=fields[5]::numeric;market:=fields[6];
  IF short_volume>total_volume OR short_exempt_volume>total_volume THEN RAISE EXCEPTION 'FINRA_VOLUME_BOUNDS_AT_%',i;END IF;
  RETURN NEXT;
 END LOOP;
END $f$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.finra_acquire_v1(p_attempt uuid,p_token text)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path TO 'pg_catalog' AS $f$
DECLARE s jsonb;d population_top3000_202608_202609_v1.source_document%rowtype;
BEGIN
 s:=population_top3000_202608_202609_v1.finra_scope_v1(p_attempt,p_token,'ACQUIRE');
 SELECT * INTO d FROM population_top3000_202608_202609_v1.source_document WHERE source_id=s->>'source_id';
 IF FOUND THEN
  IF d.source_uri IS DISTINCT FROM s->>'source_uri' OR d.content_text IS NULL
   OR d.source_sha256 IS DISTINCT FROM encode(sha256(convert_to(d.content_text,'UTF8')),'hex')
   OR d.content_bytes IS DISTINCT FROM octet_length(d.content_text)::bigint THEN RAISE EXCEPTION 'FINRA_RETAINED_SOURCE_INVALID';END IF;
  RETURN jsonb_build_object('state','COMPLETE','commit_receipt',jsonb_build_object(
   'commit_key','finra-acquire:'||d.source_id||':'||d.source_sha256,'output_sha256',d.source_sha256,
   'input_versions',s->'input_versions','immutable_output_ids',jsonb_build_array(d.source_id),'new_committed_units',0,'retained_reuse',true));
 END IF;
 RETURN population_top3000_202608_202609_v1.pipeline_dispatch_edge_v1(p_attempt,p_token,'population-top3000-evidence-v1');
END $f$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.finra_claim_v1(p_attempt uuid,p_token text)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path TO 'pg_catalog' AS $f$
DECLARE s jsonb;r jsonb;
BEGIN
 s:=population_top3000_202608_202609_v1.finra_scope_v1(p_attempt,p_token,'ACQUIRE');
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.source_document WHERE source_id=s->>'source_id')
 THEN RAISE EXCEPTION 'FINRA_SOURCE_ALREADY_RETAINED';END IF;
 r:=population_top3000_202608_202609_v1.pipeline_provider_reserve_v1(p_attempt,p_token,'FINRA',8388608,0);
 IF r->>'state' IS DISTINCT FROM 'RESERVED' THEN
  PERFORM population_top3000_202608_202609_v1.pipeline_finish_v1(p_attempt,p_token,jsonb_build_object(
   'state','BLOCKED','blocker_code','FINRA_PROVIDER_ADMISSION_WAIT','resumption_condition',jsonb_build_object(
   'required_action','Controller reviews resource, provider pacing or budget condition before an explicit retry.','reservation',r,'automatic_retry',false)));
  RETURN jsonb_build_object('state','DENIED','reason',r);
 END IF;
 RETURN s||jsonb_build_object('state','CLAIMED','max_response_bytes',8388608,'provider_timeout_ms',25000,'total_timeout_ms',45000);
END $f$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.finra_receive_v1(p_attempt uuid,p_token text,p_result jsonb)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path TO 'pg_catalog' AS $f$
DECLARE s jsonb;l population_top3000_202608_202609_v1.pipeline_provider_lease%rowtype;
 d population_top3000_202608_202609_v1.source_document%rowtype;body text;h text;n bigint;received timestamptz;
 status integer;retry_seconds integer;existing boolean:=false;outcome text;error_code text;
BEGIN
 s:=population_top3000_202608_202609_v1.finra_scope_v1(p_attempt,p_token,'ACQUIRE');
 SELECT * INTO l FROM population_top3000_202608_202609_v1.pipeline_provider_lease WHERE attempt_id=p_attempt FOR UPDATE;
 IF NOT FOUND OR l.provider<>'FINRA' OR l.released_at IS NOT NULL OR l.started_at+interval '45 seconds'<=clock_timestamp()
 THEN RAISE EXCEPTION 'FINRA_ACTIVE_RESERVATION_REQUIRED';END IF;
 IF jsonb_typeof(p_result) IS DISTINCT FROM 'object' OR octet_length(p_result::text)>16780000
  OR EXISTS(SELECT 1 FROM jsonb_object_keys(p_result) k WHERE k NOT IN('outcome','http_status','received_at','content_text','source_sha256','content_bytes','headers','error_code','retry_after_seconds'))
 THEN RAISE EXCEPTION 'FINRA_RECEIPT_OBJECT_INVALID';END IF;
 outcome:=p_result->>'outcome';status:=(p_result->>'http_status')::integer;
 received:=(p_result->>'received_at')::timestamptz;retry_seconds:=coalesce((p_result->>'retry_after_seconds')::integer,0);
 IF received IS NULL OR received<l.started_at-interval '5 seconds' OR received>clock_timestamp()+interval '5 seconds'
  OR retry_seconds<0 OR retry_seconds>86400 OR (status IS NOT NULL AND status NOT BETWEEN 100 AND 599)
 THEN RAISE EXCEPTION 'FINRA_RECEIPT_TIMING_INVALID';END IF;
 IF outcome='FAILED' THEN
  error_code:=p_result->>'error_code';
  IF error_code IS NULL OR error_code NOT IN('HTTP_ERROR','RESPONSE_TOO_LARGE','PROVIDER_TIMEOUT','INVALID_UTF8','NETWORK_ERROR','UNEXPECTED_FAILURE')
   OR p_result->>'content_text' IS NOT NULL THEN RAISE EXCEPTION 'FINRA_FAILURE_INVALID';END IF;
  IF error_code<>'HTTP_ERROR' OR status IS NULL OR status=200 THEN
   RETURN population_top3000_202608_202609_v1.pipeline_fail_v1(p_attempt,p_token,'UNCERTAIN_EFFECT','FINRA:'||error_code,
    jsonb_build_object('source_uri',s->>'source_uri','http_status',status,'error_code',error_code,
     'automatic_retry',false,'reservation_preserved',true,'required_action','Reconcile request outcome and retained evidence before an explicitly admitted retry.'),0);
  END IF;
  PERFORM population_top3000_202608_202609_v1.pipeline_provider_release_v1(p_attempt,p_token,0,0,retry_seconds);
  UPDATE population_top3000_202608_202609_v1.pipeline_provider_lease SET outcome='FAILED_NO_SOURCE_RETAINED' WHERE attempt_id=p_attempt;
  RETURN population_top3000_202608_202609_v1.pipeline_finish_v1(p_attempt,p_token,jsonb_build_object('state','BLOCKED',
   'blocker_code','FINRA_'||error_code,'resumption_condition',jsonb_build_object('source_uri',s->>'source_uri','http_status',status,
   'required_action','Reconcile the failed request and inspect source availability before controller-admitted retry. Missing response is never a zero-volume report.',
   'automatic_retry',false,'missing_is_zero',false)));
 END IF;
 IF outcome IS DISTINCT FROM 'RECEIVED' OR status IS DISTINCT FROM 200 OR jsonb_typeof(p_result->'content_text') IS DISTINCT FROM 'string'
  OR jsonb_typeof(p_result->'headers') IS DISTINCT FROM 'object' OR octet_length((p_result->'headers')::text)>8192
  OR p_result->>'error_code' IS NOT NULL THEN RAISE EXCEPTION 'FINRA_SUCCESS_RECEIPT_INVALID';END IF;
 body:=p_result->>'content_text';h:=encode(sha256(convert_to(body,'UTF8')),'hex');n:=octet_length(body);
 IF n<=0 OR n>8388608 OR n IS DISTINCT FROM (p_result->>'content_bytes')::bigint
  OR h IS DISTINCT FROM p_result->>'source_sha256' THEN RAISE EXCEPTION 'FINRA_RAW_BYTE_HASH_MISMATCH';END IF;
 SELECT * INTO d FROM population_top3000_202608_202609_v1.source_document WHERE source_id=s->>'source_id' FOR UPDATE;
 existing:=FOUND;
 IF existing AND (d.source_sha256 IS DISTINCT FROM h OR d.content_text IS DISTINCT FROM body)
 THEN RAISE EXCEPTION 'FINRA_IMMUTABLE_SOURCE_CONFLICT';END IF;
 IF NOT existing THEN
  INSERT INTO population_top3000_202608_202609_v1.source_document
  (source_id,source_sha256,source_uri,source_provider,retrieved_at,source_published_at,historical_received_at,content_text,content_bytes,evidence,license_policy)
  VALUES(s->>'source_id',h,s->>'source_uri','FINRA',received,NULL,NULL,body,n,
   jsonb_build_object('response_body_sha256',h,'response_body_bytes',n,'session_date',s->>'session_date','headers',p_result->'headers',
    'received_after_target_period',true,'original_historical_receipt_proven',false,'publication_time_verified',false,
    'reporting_scope','CNMS reported daily short-sale volume; not consolidated all-exchange volume or short interest'),
   'OFFICIAL_PUBLIC_FINRA_DAILY_FILE_RETAINED_FOR_INTERNAL_ANALYSIS');
 END IF;
 PERFORM population_top3000_202608_202609_v1.pipeline_provider_release_v1(p_attempt,p_token,CASE WHEN existing THEN 0 ELSE n END,0,retry_seconds);
 RETURN population_top3000_202608_202609_v1.pipeline_finish_v1(p_attempt,p_token,jsonb_build_object('state','COMPLETE','commit_receipt',
  jsonb_build_object('commit_key','finra-acquire:'||(s->>'source_id')||':'||h,'output_sha256',h,'input_versions',s->'input_versions',
  'immutable_output_ids',jsonb_build_array(s->>'source_id'),'new_committed_units',CASE WHEN existing THEN 0 ELSE 1 END,
  'provider_incremental_request_usd',0,'invoice_reconciled',false)));
END $f$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.finra_normalize_v1(p_attempt uuid,p_token text)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path TO 'pg_catalog' AS $f$
DECLARE s jsonb;d population_top3000_202608_202609_v1.source_document%rowtype;n bigint;before_n bigint;h text;footer integer;
 parser_hash text;err text;conflicts bigint;partition_evidence jsonb;
BEGIN
 s:=population_top3000_202608_202609_v1.finra_scope_v1(p_attempt,p_token,'NORMALIZE');
 SELECT * INTO d FROM population_top3000_202608_202609_v1.source_document WHERE source_id=s->>'source_id';
 IF NOT FOUND OR d.content_text IS NULL OR d.source_sha256 IS DISTINCT FROM encode(sha256(convert_to(d.content_text,'UTF8')),'hex')
  OR d.content_bytes IS DISTINCT FROM octet_length(d.content_text)::bigint OR d.source_uri IS DISTINCT FROM s->>'source_uri'
  OR d.source_provider IS DISTINCT FROM 'FINRA'
 THEN RAISE EXCEPTION 'FINRA_RAW_SOURCE_MISSING_OR_CORRUPT';END IF;
 parser_hash:=encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.finra_parse_cnms_v1(text,date)'::regprocedure),'UTF8')),'hex');
 SELECT count(*) INTO before_n FROM population_top3000_202608_202609_v1.source_observation WHERE family_id='S01' AND partition_key=s->>'partition_key';
 BEGIN
  SELECT count(*),max(r.footer_records) INTO n,footer
   FROM population_top3000_202608_202609_v1.finra_parse_cnms_v1(d.content_text,(s->>'session_date')::date) r;
  IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.finra_parse_cnms_v1(d.content_text,(s->>'session_date')::date) r
   GROUP BY r.symbol,r.market HAVING count(*)<>1) THEN RAISE EXCEPTION 'FINRA_DUPLICATE_SYMBOL_MARKET';END IF;
  -- A zero-row file retains its genuine footer without creating fabricated zero rows.
  IF n=0 THEN footer:=0;END IF;
  WITH parsed AS(SELECT * FROM population_top3000_202608_202609_v1.finra_parse_cnms_v1(d.content_text,(s->>'session_date')::date)),
  shaped AS(SELECT r.*,r.symbol||':'||r.market unit_key,
   jsonb_build_object('symbol',r.symbol,'market',r.market,'session_date',s->>'session_date','short_volume',r.short_volume,
    'short_exempt_volume',r.short_exempt_volume,'total_volume',r.total_volume,
    'short_volume_ratio',CASE WHEN r.total_volume>0 THEN r.short_volume/r.total_volume ELSE NULL END) vals,
   jsonb_build_object('symbol','VALUE','market','VALUE','session_date','VALUE','short_volume','VALUE','short_exempt_volume','VALUE','total_volume','VALUE',
    'short_volume_ratio',CASE WHEN r.total_volume>0 THEN 'VALUE' ELSE 'NOT_APPLICABLE' END) states,
   jsonb_build_object('source_sha256',d.source_sha256,'source_line',r.source_line,'footer_records',r.footer_records,'parser_sha256',parser_hash,
    'historical_symbol_mapping','PENDING','company_universe_frozen',false,'publication_time_verified',false,'original_historical_receipt_proven',false,
    'coverage','FINRA_CNMS_REPORTED_TRADING_ONLY','missing_symbol_meaning','NOT_REPORTED_OR_UNRESOLVED; never synthesize zero',
    'zero_total_ratio_reason',CASE WHEN r.total_volume=0 THEN 'No reported total volume; ratio undefined' END) lin
   FROM parsed r),expected AS(SELECT shaped.*,encode(sha256(convert_to(jsonb_build_object('values',vals,'states',states,'lineage',lin)::text,'UTF8')),'hex') expected_hash FROM shaped),
  prior_conflicts AS(SELECT e.unit_key FROM expected e JOIN population_top3000_202608_202609_v1.source_observation o
   ON o.family_id='S01' AND o.partition_key=s->>'partition_key' AND o.unit_key=e.unit_key
   WHERE o.values_json IS DISTINCT FROM e.vals OR o.field_state IS DISTINCT FROM e.states OR o.lineage IS DISTINCT FROM e.lin
    OR o.record_sha256 IS DISTINCT FROM e.expected_hash OR o.source_id IS DISTINCT FROM d.source_id
    OR o.source_record_key IS DISTINCT FROM e.source_line::text OR o.normalization_sha256 IS DISTINCT FROM parser_hash
    OR o.vintage_key IS DISTINCT FROM d.source_sha256 OR o.effective_date IS DISTINCT FROM (s->>'session_date')::date
    OR o.reporting_entity_id IS DISTINCT FROM 'FINRA_CNMS' OR o.time_role IS DISTINCT FROM 'TARGET'
    OR o.available_at IS DISTINCT FROM d.retrieved_at OR o.availability_policy IS DISTINCT FROM 'ARCHIVED_OBSERVATION'
    OR o.company_id IS NOT NULL OR o.security_id IS NOT NULL OR o.event_at IS NOT NULL
    OR o.historical_received_at IS NOT NULL OR o.source_published_at IS NOT NULL),
  inserted AS(INSERT INTO population_top3000_202608_202609_v1.source_observation
   (family_id,partition_key,unit_key,reporting_entity_id,time_role,event_at,effective_date,source_published_at,historical_received_at,available_at,
    availability_policy,vintage_key,source_id,source_record_key,normalization_sha256,values_json,field_state,lineage,record_sha256)
  SELECT 'S01',s->>'partition_key',unit_key,'FINRA_CNMS','TARGET',NULL,(s->>'session_date')::date,NULL,NULL,d.retrieved_at,
   'ARCHIVED_OBSERVATION',d.source_sha256,d.source_id,source_line::text,parser_hash,vals,states,lin,
   expected_hash FROM expected ON CONFLICT DO NOTHING RETURNING unit_key)
  SELECT count(*) INTO conflicts FROM prior_conflicts;
  IF conflicts>0 OR (SELECT count(*) FROM population_top3000_202608_202609_v1.source_observation WHERE family_id='S01' AND partition_key=s->>'partition_key')<>n
  THEN RAISE EXCEPTION 'FINRA_NORMALIZATION_RECONCILIATION_FAILED';END IF;
  IF (SELECT coalesce(sum(pg_total_relation_size(r.oid)),0) FROM pg_class r JOIN pg_namespace ns ON ns.oid=r.relnamespace
   WHERE ns.nspname='population_top3000_202608_202609_v1' AND r.relkind IN('r','m')) >=
   (SELECT (budget_evidence#>>'{resource_limits,max_relation_bytes}')::bigint FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton)
  THEN RAISE EXCEPTION 'FINRA_NORMALIZED_RESOURCE_BOUND';END IF;
 EXCEPTION WHEN OTHERS THEN
  GET STACKED DIAGNOSTICS err=MESSAGE_TEXT;
  RETURN jsonb_build_object('state','BLOCKED','blocker_code','FINRA_PARSER_REVIEW_REQUIRED','resumption_condition',
   jsonb_build_object('source_id',d.source_id,'source_sha256',d.source_sha256,'diagnostic',left(err,160),
   'required_action','Review retained file and parser. Explicitly re-admit changed parser; no automatic download or missing-to-zero substitution.','automatic_retry',false));
 END;
 SELECT encode(sha256(convert_to(coalesce(string_agg(unit_key||':'||record_sha256,E'\n' ORDER BY unit_key),''),'UTF8')),'hex') INTO h
  FROM population_top3000_202608_202609_v1.source_observation WHERE family_id='S01' AND partition_key=s->>'partition_key';
 partition_evidence:=jsonb_build_object('reported_source_rows',n,'footer_records',footer,
  'source_sha256',d.source_sha256,'company_universe_frozen',false,'company_mapping_complete',false,'absent_symbols_are_zero',false);
 INSERT INTO population_top3000_202608_202609_v1.pipeline_partition
  (family_id,partition_key,time_role,denominator_state,denominator_evidence,producer_revision,immutable_input_versions,publication_eligible)
 VALUES('S01',s->>'partition_key','TARGET','PROVISIONAL',partition_evidence,
  'finra-cnms-v1',s->'input_versions',false) ON CONFLICT(family_id,partition_key) DO NOTHING;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_partition WHERE family_id='S01' AND partition_key=s->>'partition_key'
  AND time_role='TARGET' AND denominator_state='PROVISIONAL' AND denominator_evidence=partition_evidence AND producer_revision='finra-cnms-v1'
  AND immutable_input_versions=s->'input_versions' AND NOT publication_eligible)
 THEN RAISE EXCEPTION 'FINRA_PARTITION_IMMUTABLE_CONFLICT';END IF;
 RETURN jsonb_build_object('state','COMPLETE','commit_receipt',jsonb_build_object('commit_key','finra-normalize:'||d.source_id||':'||h,
  'input_versions',s->'input_versions','immutable_output_ids',jsonb_build_array(s->>'partition_key'),'output_sha256',h,
  'new_committed_units',n-before_n,'source_rows',n,'footer_records',footer,'company_universe_frozen',false));
END $f$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.finra_derive_v1(p_attempt uuid,p_token text)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path TO 'pg_catalog' AS $f$
DECLARE s jsonb;h text;n bigint;
BEGIN
 s:=population_top3000_202608_202609_v1.finra_scope_v1(p_attempt,p_token,'DERIVE');
 SELECT count(*),encode(sha256(convert_to(coalesce(string_agg(unit_key||':'||record_sha256,E'\n' ORDER BY unit_key),''),'UTF8')),'hex') INTO n,h
 FROM population_top3000_202608_202609_v1.source_observation WHERE family_id='S01' AND partition_key=s->>'partition_key';
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_partition WHERE family_id='S01' AND partition_key=s->>'partition_key'
  AND denominator_state='PROVISIONAL' AND NOT publication_eligible AND (denominator_evidence->>'reported_source_rows')::bigint=n)
 THEN RAISE EXCEPTION 'FINRA_PROVISIONAL_PARTITION_RECEIPT_REQUIRED';END IF;
 RETURN jsonb_build_object('state','COMPLETE','commit_receipt',jsonb_build_object('commit_key','finra-provisional-derive:'||(s->>'partition_key')||':'||h,
  'input_versions',s->'input_versions','immutable_output_ids',jsonb_build_array(s->>'partition_key'),'output_sha256',h,'new_committed_units',0,
  'source_rows',n,'company_universe_frozen',false,'publication_eligible',false,'company_level_features_certified',false));
END $f$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.finra_code_hash_v1()
RETURNS text LANGUAGE sql STABLE SET search_path TO 'pg_catalog' AS $f$
 SELECT encode(sha256(convert_to(string_agg(pg_get_functiondef(p.oid),E'\n' ORDER BY p.proname),'UTF8')),'hex')
 FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
 WHERE n.nspname='population_top3000_202608_202609_v1' AND p.proname IN
 ('finra_scope_v1','finra_parse_cnms_v1','finra_acquire_v1','finra_claim_v1','finra_receive_v1','finra_normalize_v1','finra_derive_v1','finra_code_hash_v1');
$f$;

REVOKE ALL ON FUNCTION population_top3000_202608_202609_v1.finra_scope_v1(uuid,text,text),
 population_top3000_202608_202609_v1.finra_code_hash_v1(),
 population_top3000_202608_202609_v1.finra_parse_cnms_v1(text,date),population_top3000_202608_202609_v1.finra_acquire_v1(uuid,text),
 population_top3000_202608_202609_v1.finra_claim_v1(uuid,text),population_top3000_202608_202609_v1.finra_receive_v1(uuid,text,jsonb),
 population_top3000_202608_202609_v1.finra_normalize_v1(uuid,text),population_top3000_202608_202609_v1.finra_derive_v1(uuid,text) FROM PUBLIC,anon,authenticated;
GRANT EXECUTE ON FUNCTION population_top3000_202608_202609_v1.finra_scope_v1(uuid,text,text),
 population_top3000_202608_202609_v1.finra_code_hash_v1(),
 population_top3000_202608_202609_v1.finra_parse_cnms_v1(text,date),population_top3000_202608_202609_v1.finra_acquire_v1(uuid,text),
 population_top3000_202608_202609_v1.finra_claim_v1(uuid,text),population_top3000_202608_202609_v1.finra_receive_v1(uuid,text,jsonb),
 population_top3000_202608_202609_v1.finra_normalize_v1(uuid,text),population_top3000_202608_202609_v1.finra_derive_v1(uuid,text) TO postgres,service_role;

-- Root executes inside BEGIN, after installing draft definitions in that same transaction.
-- Synthetic FINRA format fixture; never present as an observed official file.
-- No pipeline_tick or network dispatcher is invoked by this test.
SET LOCAL lock_timeout='3s';
SET LOCAL statement_timeout='45s';
SELECT pg_advisory_xact_lock(2026083000261022);
CREATE OR REPLACE FUNCTION pg_temp.finra_test_lease(p_task uuid)
RETURNS jsonb LANGUAGE plpgsql AS $test$
DECLARE t population_top3000_202608_202609_v1.pipeline_task%rowtype;a uuid:=gen_random_uuid();tok text:=repeat('b',64);h text;
BEGIN
 SELECT * INTO t FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=p_task;
 IF t.family_id<>'S01' OR t.input_versions->>'synthetic_rollback_fixture' IS DISTINCT FROM 'true' THEN RAISE EXCEPTION 'TEST_TASK_REQUIRED';END IF;
 SELECT function_sha256 INTO h FROM population_top3000_202608_202609_v1.pipeline_adapter WHERE family_id='S01' AND stage=t.stage;
 UPDATE population_top3000_202608_202609_v1.pipeline_task SET status='LEASED',attempt_id=a,lease_until=clock_timestamp()+interval '85 seconds',
  owner_fence=2,stop_generation=0,executions=executions+1 WHERE task_id=p_task;
 INSERT INTO population_top3000_202608_202609_v1.pipeline_attempt(attempt_id,task_id,execution_number,token_sha256,owner_fence,stop_generation,adapter_sha256)
 VALUES(a,p_task,t.executions+1,encode(sha256(convert_to(tok,'UTF8')),'hex'),2,0,h);
 RETURN jsonb_build_object('attempt',a,'token',tok);
END $test$;
DO $test$
DECLARE raw text:=E'Date|Symbol|ShortVolume|ShortExemptVolume|TotalVolume|Market\r\n20260803|SYNTHA|10.125|0.125|20.25|B,Q,N\r\n20260803|SYNTHB|0|0|0|Q\r\n20260803|SYNTHC|1.000001|0|3.000003|N\r\n3\r\n';
 raw_hash text;before_provider bigint;before_http bigint;before_bytes bigint;before_docs bigint;
 inputs jsonb;t uuid;a jsonb;r jsonb;next_task uuid;rejected boolean;v_date date;v_footer integer;
BEGIN
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton AND owner_id='top3000-recovery-20261023-root-4b1314bcb5c2'
  AND owner_fence=2 AND stop_generation=0 AND enabled AND NOT stopped AND hold_reason IS NULL AND admitted_until>clock_timestamp()) THEN RAISE EXCEPTION 'CONTROLLER_NOT_ADMITTED';END IF;
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE status='LEASED')
  OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_provider_lease WHERE released_at IS NULL) THEN RAISE EXCEPTION 'TEST_REQUIRES_NO_ACTIVE_LEASE';END IF;
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.source_document WHERE source_id IN('S01:FINRA_CNMS:2026-08-03','S01:FINRA_CNMS:2026-08-04'))
 THEN RAISE EXCEPTION 'FIXTURE_COLLISION_ABORT_WITHOUT_OVERWRITE';END IF;
 SELECT provider_requests,http_invocations,stored_bytes INTO before_provider,before_http,before_bytes FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton;
 SELECT count(*) INTO before_docs FROM population_top3000_202608_202609_v1.source_document;
 UPDATE population_top3000_202608_202609_v1.acquisition_control SET next_provider_at='-infinity' WHERE singleton;
 INSERT INTO population_top3000_202608_202609_v1.pipeline_adapter
 (family_id,stage,function_oid,function_sha256,resource_class,provider,enabled,reviewed_test_evidence,producer_revision,expected_batch_seconds)
 SELECT 'S01',stage,fn::regprocedure::oid,encode(sha256(convert_to(pg_get_functiondef(fn::regprocedure),'UTF8')),'hex'),resource,
  CASE WHEN resource='PROVIDER' THEN 'FINRA' END,true,'{"test":"SYNTHETIC_IN_PROGRESS_ROLLBACK_ONLY"}'::jsonb,'finra-cnms-v1',45
 FROM (VALUES ('ACQUIRE','population_top3000_202608_202609_v1.finra_acquire_v1(uuid,text)','PROVIDER'),
 ('NORMALIZE','population_top3000_202608_202609_v1.finra_normalize_v1(uuid,text)','DATABASE'),
 ('DERIVE','population_top3000_202608_202609_v1.finra_derive_v1(uuid,text)','DATABASE')) v(stage,fn,resource)
 ON CONFLICT(family_id,stage) DO UPDATE SET function_oid=excluded.function_oid,function_sha256=excluded.function_sha256,
  resource_class=excluded.resource_class,provider=excluded.provider,enabled=true,reviewed_test_evidence=excluded.reviewed_test_evidence;
 SELECT max(footer_records) INTO v_footer FROM population_top3000_202608_202609_v1.finra_parse_cnms_v1(raw,'2026-08-03');
 IF v_footer<>3 OR (SELECT short_volume FROM population_top3000_202608_202609_v1.finra_parse_cnms_v1(raw,'2026-08-03') WHERE symbol='SYNTHA')<>10.125::numeric
 THEN RAISE EXCEPTION 'DECIMAL_OR_FOOTER_LOSS';END IF;
 rejected:=false;
 BEGIN PERFORM * FROM population_top3000_202608_202609_v1.finra_parse_cnms_v1(replace(raw,E'\r\n3\r\n',E'\r\n4\r\n'),'2026-08-03');
 EXCEPTION WHEN OTHERS THEN rejected:=SQLERRM='FINRA_FOOTER_COUNT_MISMATCH';END;
 IF NOT rejected THEN RAISE EXCEPTION 'BAD_FOOTER_COUNT_ACCEPTED';END IF;
 rejected:=false;
 BEGIN PERFORM * FROM population_top3000_202608_202609_v1.finra_parse_cnms_v1(replace(raw,E'\r\n3\r\n',E'\r\nTOTAL:3\r\n'),'2026-08-03');
 EXCEPTION WHEN OTHERS THEN rejected:=SQLERRM='FINRA_FOOTER_UNRECOGNIZED';END;
 IF NOT rejected THEN RAISE EXCEPTION 'UNRECOGNIZED_FOOTER_ACCEPTED';END IF;
 rejected:=false;
 BEGIN PERFORM * FROM population_top3000_202608_202609_v1.finra_parse_cnms_v1(raw,'2026-08-04');
 EXCEPTION WHEN OTHERS THEN rejected:=SQLERRM LIKE 'FINRA_ROW_INVALID_AT_%';END;
 IF NOT rejected THEN RAISE EXCEPTION 'WRONG_SESSION_ACCEPTED';END IF;
 IF (SELECT count(*) FROM population_top3000_202608_202609_v1.finra_parse_cnms_v1(E'Date|Symbol|ShortVolume|ShortExemptVolume|TotalVolume|Market\n0\n','2026-08-03'))<>0
 THEN RAISE EXCEPTION 'ZERO_FOOTER_FABRICATED_ROWS';END IF;
 raw_hash:=encode(sha256(convert_to(raw,'UTF8')),'hex');
 inputs:=jsonb_build_object('calendar_record_sha256',(SELECT source_record_sha256 FROM population_top3000_202608_202609_v1.catalog_calendar WHERE session_date='2026-08-03'),
  'finra_bundle_sha256',population_top3000_202608_202609_v1.finra_code_hash_v1(),'synthetic_rollback_fixture',true,'fixture_nonce',gen_random_uuid());
 t:=population_top3000_202608_202609_v1.pipeline_enqueue_v1('S01','ACQUIRE','CNMS:2026-08-03','{"kind":"FINRA_CNMS_DAILY","session_date":"2026-08-03","provisional":true}',inputs);
 a:=pg_temp.finra_test_lease(t);
 rejected:=false;
 BEGIN PERFORM population_top3000_202608_202609_v1.finra_claim_v1((a->>'attempt')::uuid,repeat('a',64));
 EXCEPTION WHEN OTHERS THEN rejected:=SQLERRM='ATTEMPT_AUTH_FAILED';END;
 IF NOT rejected OR (SELECT provider_requests FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton)<>before_provider THEN RAISE EXCEPTION 'AUTH_BYPASS';END IF;
 r:=population_top3000_202608_202609_v1.finra_claim_v1((a->>'attempt')::uuid,a->>'token');
 IF r->>'state'<>'CLAIMED' OR r->>'source_uri'<>'https://cdn.finra.org/equity/regsho/daily/CNMSshvol20260803.txt' THEN RAISE EXCEPTION 'CLAIM_FAILED:%',r;END IF;
 rejected:=false;
 BEGIN PERFORM population_top3000_202608_202609_v1.finra_claim_v1((a->>'attempt')::uuid,a->>'token');
 EXCEPTION WHEN OTHERS THEN rejected:=SQLERRM='REQUEST_ALREADY_RESERVED_FOR_ATTEMPT';END;
 IF NOT rejected THEN RAISE EXCEPTION 'DUPLICATE_FETCH_RESERVATION_ALLOWED';END IF;
 rejected:=false;
 BEGIN PERFORM population_top3000_202608_202609_v1.finra_receive_v1((a->>'attempt')::uuid,a->>'token',jsonb_build_object('outcome','RECEIVED','http_status',200,
  'received_at',clock_timestamp(),'content_text',raw,'source_sha256',repeat('0',64),'content_bytes',octet_length(raw),'headers','{}'::jsonb,'retry_after_seconds',0));
 EXCEPTION WHEN OTHERS THEN rejected:=SQLERRM='FINRA_RAW_BYTE_HASH_MISMATCH';END;
 IF NOT rejected THEN RAISE EXCEPTION 'CORRUPTED_SOURCE_ACCEPTED';END IF;
 r:=population_top3000_202608_202609_v1.finra_receive_v1((a->>'attempt')::uuid,a->>'token',jsonb_build_object('outcome','RECEIVED','http_status',200,
  'received_at',clock_timestamp(),'content_text',raw,'source_sha256',raw_hash,'content_bytes',octet_length(raw),'headers',jsonb_build_object('fixture','SYNTHETIC_ROLLBACK_ONLY'),'retry_after_seconds',0));
 IF r->>'state'<>'COMPLETE' THEN RAISE EXCEPTION 'RECEIVE_FAILED:%',r;END IF;
 next_task:=(r->>'successor_task_id')::uuid;a:=pg_temp.finra_test_lease(next_task);
 r:=population_top3000_202608_202609_v1.finra_normalize_v1((a->>'attempt')::uuid,a->>'token');
 IF r->>'state'<>'COMPLETE' OR r#>>'{commit_receipt,source_rows}'<>'3' THEN RAISE EXCEPTION 'NORMALIZE_FAILED:%',r;END IF;
 r:=population_top3000_202608_202609_v1.pipeline_finish_v1((a->>'attempt')::uuid,a->>'token',r);
 IF (SELECT count(*) FROM population_top3000_202608_202609_v1.source_observation WHERE family_id='S01' AND partition_key='CNMS:2026-08-03')<>3
  OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.source_observation WHERE family_id='S01' AND partition_key='CNMS:2026-08-03'
   AND values_json->>'symbol'='SYNTHA' AND (values_json->>'short_volume')::numeric=10.125 AND (values_json->>'short_exempt_volume')::numeric=0.125)
  OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.source_observation WHERE family_id='S01' AND partition_key='CNMS:2026-08-03'
   AND values_json->>'symbol'='SYNTHB' AND values_json->>'short_volume_ratio' IS NULL AND field_state->>'short_volume_ratio'='NOT_APPLICABLE')
  OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.source_observation WHERE family_id='S01' AND partition_key='CNMS:2026-08-03'
   AND (company_id IS NOT NULL OR security_id IS NOT NULL OR historical_received_at IS NOT NULL OR source_published_at IS NOT NULL OR availability_policy<>'ARCHIVED_OBSERVATION'))
 THEN RAISE EXCEPTION 'PROVISIONAL_NUMERIC_OR_MISSING_ZERO_SEMANTICS_FAILED';END IF;
 a:=pg_temp.finra_test_lease((r->>'successor_task_id')::uuid);
 r:=population_top3000_202608_202609_v1.finra_derive_v1((a->>'attempt')::uuid,a->>'token');
 IF r#>>'{commit_receipt,company_universe_frozen}'<>'false' OR r#>>'{commit_receipt,publication_eligible}'<>'false' THEN RAISE EXCEPTION 'FALSE_CERTIFICATION';END IF;
 PERFORM population_top3000_202608_202609_v1.pipeline_finish_v1((a->>'attempt')::uuid,a->>'token',r);
 -- A failed daily report must leave no source rows and no timed automatic retry.
 UPDATE population_top3000_202608_202609_v1.acquisition_control SET next_provider_at='-infinity' WHERE singleton;
 inputs:=jsonb_set(inputs,'{calendar_record_sha256}',to_jsonb((SELECT source_record_sha256 FROM population_top3000_202608_202609_v1.catalog_calendar WHERE session_date='2026-08-04')));
 t:=population_top3000_202608_202609_v1.pipeline_enqueue_v1('S01','ACQUIRE','CNMS:2026-08-04','{"kind":"FINRA_CNMS_DAILY","session_date":"2026-08-04","provisional":true}',inputs);
 a:=pg_temp.finra_test_lease(t);r:=population_top3000_202608_202609_v1.finra_claim_v1((a->>'attempt')::uuid,a->>'token');
 IF r->>'state'<>'CLAIMED' THEN RAISE EXCEPTION 'SECOND_CLAIM_FAILED:%',r;END IF;
 r:=population_top3000_202608_202609_v1.finra_receive_v1((a->>'attempt')::uuid,a->>'token',jsonb_build_object('outcome','FAILED','http_status',404,
  'received_at',clock_timestamp(),'error_code','HTTP_ERROR','retry_after_seconds',0));
 IF r->>'state'<>'BLOCKED' OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.source_document WHERE source_id='S01:FINRA_CNMS:2026-08-04')
  OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=t AND status='BLOCKED' AND blocker_until IS NULL
  AND resumption_condition->>'missing_is_zero'='false') THEN RAISE EXCEPTION 'HTTP_FAILURE_ZERO_FILL_OR_RETRY';END IF;
 IF (SELECT provider_requests FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton)<>before_provider+2
  OR (SELECT http_invocations FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton)<>before_http
  OR (SELECT stored_bytes FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton)<>before_bytes+octet_length(raw)
  OR (SELECT count(*) FROM population_top3000_202608_202609_v1.source_document)<>before_docs+1
  OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_provider_lease WHERE released_at IS NULL)
 THEN RAISE EXCEPTION 'REQUEST_OR_STORAGE_ACCOUNTING_FAILED';END IF;
 RAISE NOTICE 'PASS_FINRA_AUTH_SINGLE_REQUEST_DECIMALS_HASH_FOOTER_PROVISIONAL_AND_404_NO_ZERO';
END $test$;
SELECT 'PASS_FINRA_DB_ROLLBACK_SYNTHETIC_FIXTURES_NO_NETWORK' AS result;

ROLLBACK;
