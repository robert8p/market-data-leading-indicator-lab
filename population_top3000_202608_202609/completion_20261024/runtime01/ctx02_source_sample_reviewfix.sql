-- CTX02 bounded source-sample route. Controller integrates after independent review.
-- No source/family certification, no provider calls, and no changes to other schemas.
CREATE TABLE IF NOT EXISTS population_top3000_202608_202609_v1.ctx02_source_page_v1 (
 source_id text PRIMARY KEY REFERENCES population_top3000_202608_202609_v1.source_document(source_id),
 source_kind text NOT NULL CHECK (source_kind IN ('CONTRACT_REFERENCE','DAILY_BARS')),
 selector jsonb NOT NULL, records jsonb NOT NULL, record_count integer NOT NULL CHECK(record_count BETWEEN 0 AND 1000),
 projection_sha256 text NOT NULL, source_envelope_sha256 text NOT NULL, evidence jsonb NOT NULL, created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
ALTER TABLE population_top3000_202608_202609_v1.ctx02_source_page_v1 ADD COLUMN IF NOT EXISTS source_envelope_sha256 text;
ALTER TABLE population_top3000_202608_202609_v1.ctx02_source_page_v1 ALTER COLUMN source_envelope_sha256 SET NOT NULL;
CREATE TABLE IF NOT EXISTS population_top3000_202608_202609_v1.ctx02_normalized_sample_v1 (
 source_id text NOT NULL REFERENCES population_top3000_202608_202609_v1.ctx02_source_page_v1(source_id),
 source_row integer NOT NULL, source_kind text NOT NULL, option_ticker text NOT NULL,
 observation_date date NOT NULL, underlying_ticker text, normalized jsonb NOT NULL,
 record_sha256 text NOT NULL, PRIMARY KEY(source_id,source_row)
);
CREATE TABLE IF NOT EXISTS population_top3000_202608_202609_v1.ctx02_sample_evidence_v1 (
 source_id text PRIMARY KEY REFERENCES population_top3000_202608_202609_v1.ctx02_source_page_v1(source_id),
 evidence jsonb NOT NULL, evidence_sha256 text NOT NULL, created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
ALTER TABLE population_top3000_202608_202609_v1.ctx02_source_page_v1 ENABLE ROW LEVEL SECURITY;
ALTER TABLE population_top3000_202608_202609_v1.ctx02_normalized_sample_v1 ENABLE ROW LEVEL SECURITY;
ALTER TABLE population_top3000_202608_202609_v1.ctx02_sample_evidence_v1 ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON population_top3000_202608_202609_v1.ctx02_source_page_v1,population_top3000_202608_202609_v1.ctx02_normalized_sample_v1,population_top3000_202608_202609_v1.ctx02_sample_evidence_v1 FROM public,anon,authenticated;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.ctx02_number_v1(p_value jsonb)
RETURNS numeric LANGUAGE plpgsql IMMUTABLE SET search_path=pg_catalog AS $$
DECLARE s text;
BEGIN
 IF p_value IS NULL OR jsonb_typeof(p_value)='null' THEN RETURN NULL; END IF;
 IF jsonb_typeof(p_value) NOT IN ('number','string') THEN RAISE EXCEPTION 'CTX02_INVALID_NUMBER_TYPE'; END IF;
 s:=p_value#>>'{}';
 IF length(s)>40 OR s !~ '^[0-9]+([.][0-9]+)?$' THEN RAISE EXCEPTION 'CTX02_INVALID_NUMBER'; END IF;
 RETURN s::numeric;
END; $$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.ctx02_parse_rows_v1(p_kind text,p_selector jsonb,p_records jsonb)
RETURNS TABLE(source_row integer,option_ticker text,observation_date date,underlying_ticker text,normalized jsonb)
LANGUAGE plpgsql IMMUTABLE SET search_path=pg_catalog AS $$
DECLARE r jsonb;i integer:=0;v text;d date;ex date;mult numeric;strike numeric;
 o numeric;h numeric;l numeric;c numeric;vol numeric;vw numeric;n numeric;t numeric;extra jsonb;
BEGIN
 IF p_kind IS NULL OR p_kind NOT IN ('CONTRACT_REFERENCE','DAILY_BARS') OR jsonb_typeof(p_selector) IS DISTINCT FROM 'object'
 OR jsonb_typeof(p_records) IS DISTINCT FROM 'array' OR jsonb_array_length(p_records)>1000 THEN RAISE EXCEPTION 'CTX02_INPUT_SHAPE'; END IF;
 FOR r IN SELECT value FROM jsonb_array_elements(p_records) LOOP
  i:=i+1; IF jsonb_typeof(r) IS DISTINCT FROM 'object' THEN RAISE EXCEPTION 'CTX02_ROW_SHAPE'; END IF;
  source_row:=i;
  IF p_kind='CONTRACT_REFERENCE' THEN
   option_ticker:=r->>'ticker'; underlying_ticker:=r->>'underlying_ticker';
   IF option_ticker IS NULL OR option_ticker !~ '^O:[A-Za-z0-9.]+[0-9]{6}[CP][0-9]{8}$' OR length(option_ticker)>80
    OR nullif(underlying_ticker,'') IS NULL OR length(underlying_ticker)>40
    OR r->>'contract_type' NOT IN ('call','put','other') OR r->>'contract_type' IS NULL THEN RAISE EXCEPTION 'CTX02_CONTRACT_IDENTITY_INVALID';END IF;
   IF p_selector->>'as_of' !~ '^2026-(08|09)-[0-9]{2}$' OR p_selector->>'as_of' IS NULL
    OR r->>'expiration_date' !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' OR r->>'expiration_date' IS NULL THEN RAISE EXCEPTION 'CTX02_REFERENCE_DATE_INVALID';END IF;
   d:=(p_selector->>'as_of')::date;ex:=(r->>'expiration_date')::date;
   IF d<'2026-08-01' OR d>'2026-09-30' OR ex<d THEN RAISE EXCEPTION 'CTX02_REFERENCE_DATE_OUT_OF_SCOPE';END IF;
   IF p_selector ? 'underlying_ticker' AND p_selector->>'underlying_ticker' IS DISTINCT FROM underlying_ticker THEN RAISE EXCEPTION 'CTX02_REFERENCE_UNDERLYING_MISMATCH';END IF;
   strike:=population_top3000_202608_202609_v1.ctx02_number_v1(r->'strike_price');
   mult:=population_top3000_202608_202609_v1.ctx02_number_v1(r->'shares_per_contract');
   IF strike IS NULL OR strike<0 OR mult IS NULL OR mult<=0 THEN RAISE EXCEPTION 'CTX02_REQUIRED_CONTRACT_NUMBER_MISSING';END IF;
   IF substring(option_ticker from '([0-9]{6})[CP][0-9]{8}$') IS DISTINCT FROM to_char(ex,'YYMMDD')
    OR substring(option_ticker from '([CP])[0-9]{8}$') IS DISTINCT FROM (CASE r->>'contract_type' WHEN 'call' THEN 'C' WHEN 'put' THEN 'P' ELSE NULL END)
    OR right(option_ticker,8)::numeric/1000 IS DISTINCT FROM strike THEN RAISE EXCEPTION 'CTX02_CONTRACT_SYMBOL_FIELDS_CONFLICT';END IF;
   extra:=r->'additional_underlyings';
   IF extra IS NOT NULL AND extra<>'null'::jsonb AND jsonb_typeof(extra)<>'array' THEN RAISE EXCEPTION 'CTX02_ADDITIONAL_DELIVERABLE_INVALID';END IF;
   observation_date:=d;
   normalized:=jsonb_build_object('ticker',option_ticker,'underlying_ticker',underlying_ticker,'reference_as_of',d,'expiration_date',ex,
    'contract_type',r->>'contract_type','strike_price',strike,'shares_per_contract',mult,'additional_underlyings',extra,
    'exercise_style',r->'exercise_style','primary_exchange',r->'primary_exchange','correction',r->'correction',
    'source_record',r,'availability_policy','CURRENT_CONNECTOR_ARCHIVE','historical_received_at',NULL,
    'source_published_at',NULL,'company_mapping_state','UNRESOLVED','contract_life_state','OBSERVED_AS_OF_ONLY');
  ELSE
   option_ticker:=p_selector->>'ticker';underlying_ticker:=NULL;
   IF option_ticker IS NULL OR option_ticker !~ '^O:[A-Za-z0-9.]+[0-9]{6}[CP][0-9]{8}$'
    OR p_selector->'adjusted' IS DISTINCT FROM 'false'::jsonb THEN RAISE EXCEPTION 'CTX02_BAR_SCOPE_INVALID';END IF;
   IF p_selector->>'from' IS NULL OR p_selector->>'to' IS NULL
    OR p_selector->>'from' !~ '^2026-(08|09)-[0-9]{2}$' OR p_selector->>'to' !~ '^2026-(08|09)-[0-9]{2}$'
    OR (p_selector->>'from')::date>(p_selector->>'to')::date
    OR (p_selector->>'from')::date<DATE '2026-08-01' OR (p_selector->>'to')::date>DATE '2026-09-30'
   THEN RAISE EXCEPTION 'CTX02_BAR_SELECTOR_DATES_REQUIRED';END IF;
   o:=population_top3000_202608_202609_v1.ctx02_number_v1(r->'o');h:=population_top3000_202608_202609_v1.ctx02_number_v1(r->'h');
   l:=population_top3000_202608_202609_v1.ctx02_number_v1(r->'l');c:=population_top3000_202608_202609_v1.ctx02_number_v1(r->'c');
   vol:=population_top3000_202608_202609_v1.ctx02_number_v1(r->'v');vw:=population_top3000_202608_202609_v1.ctx02_number_v1(r->'vw');
   n:=population_top3000_202608_202609_v1.ctx02_number_v1(r->'n');t:=population_top3000_202608_202609_v1.ctx02_number_v1(r->'t');
   IF o IS NULL OR h IS NULL OR l IS NULL OR c IS NULL OR vol IS NULL OR t IS NULL
    OR trunc(t)<>t OR t<1785556800000 OR t>1790827200000 OR trunc(vol)<>vol
    OR (n IS NOT NULL AND trunc(n)<>n) OR l>least(o,c,h) OR h<greatest(o,c,l)
    OR (vw IS NOT NULL AND (vw<l OR vw>h)) THEN RAISE EXCEPTION 'CTX02_BAR_VALUE_INVALID';END IF;
   d:=(to_timestamp((t/1000)::double precision) AT TIME ZONE 'America/New_York')::date;
   IF d<'2026-08-01' OR d>'2026-09-30' OR d<(p_selector->>'from')::date OR d>(p_selector->>'to')::date THEN RAISE EXCEPTION 'CTX02_BAR_DATE_OUT_OF_SCOPE';END IF;
   IF (to_timestamp((t/1000)::double precision) AT TIME ZONE 'America/New_York')::time<>'00:00'::time THEN RAISE EXCEPTION 'CTX02_DAILY_BAR_TIMESTAMP_NOT_MIDNIGHT';END IF;
   observation_date:=d;
   normalized:=jsonb_build_object('ticker',option_ticker,'session_date',d,'timestamp_ms',t,'open',o,'high',h,'low',l,'close',c,'volume',vol,
    'vwap',vw,'trade_count',n,'adjusted',false,'source_record',r,'availability_policy','CURRENT_CONNECTOR_ARCHIVE',
    'historical_received_at',NULL,'source_published_at',NULL,'company_mapping_state','UNRESOLVED',
    'field_state',jsonb_build_object('vwap',CASE WHEN vw IS NULL THEN 'MISSING_REQUIRED' ELSE 'VALUE' END,'trade_count',CASE WHEN n IS NULL THEN 'MISSING_REQUIRED' ELSE 'VALUE' END));
  END IF;
  RETURN NEXT;
 END LOOP;
END; $$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.ctx02_parser_bundle_v1()
RETURNS text LANGUAGE sql STABLE SET search_path=pg_catalog AS $$
 SELECT encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.ctx02_parse_rows_v1(text,jsonb,jsonb)'::regprocedure)||pg_get_functiondef('population_top3000_202608_202609_v1.ctx02_number_v1(jsonb)'::regprocedure),'UTF8')),'hex')
$$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.ctx02_import_connector_sample_v1(p_owner text,p_fence bigint,p_stop bigint,p_source jsonb)
RETURNS jsonb LANGUAGE plpgsql SET search_path=pg_catalog AS $$
DECLARE c population_top3000_202608_202609_v1.acquisition_control%rowtype;p population_top3000_202608_202609_v1.pipeline_control%rowtype;v_envelope text;v_id text;v_hash text;v_projection text;v_rows integer;v_old record;v_new boolean;
BEGIN
 SELECT * INTO c FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton FOR UPDATE;
 IF NOT FOUND THEN RAISE EXCEPTION 'CTX02_CONTROL_MISSING';END IF;
 SELECT * INTO p FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton FOR UPDATE;
 IF NOT FOUND OR c.owner_id IS DISTINCT FROM p_owner OR c.owner_fence IS DISTINCT FROM p_fence OR c.stop_generation IS DISTINCT FROM p_stop
 OR c.stopped IS DISTINCT FROM false OR c.enabled IS DISTINCT FROM true OR p.enabled IS DISTINCT FROM true
 OR c.hold_reason IS NOT NULL OR c.admitted_until IS NULL OR c.admitted_until<=clock_timestamp()
 OR p.deadline IS NULL OR p.deadline<=clock_timestamp() OR p.tick_count>=p.max_ticks OR p.shared_scheduler_verified IS DISTINCT FROM true
 OR p.expected_owner_id IS DISTINCT FROM c.owner_id OR p.expected_owner_fence IS DISTINCT FROM c.owner_fence OR p.expected_stop_generation IS DISTINCT FROM c.stop_generation
 OR p.scope_sha256 IS DISTINCT FROM c.scope_sha256 OR population_top3000_202608_202609_v1.pipeline_cost_admission_v2(p,0) IS DISTINCT FROM true
 OR c.scope_sha256 IS DISTINCT FROM '8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd' THEN RAISE EXCEPTION 'CTX02_OWNER_STOP_SCOPE_REJECTED';END IF;
 v_envelope:=encode(sha256(convert_to(p_source::text,'UTF8')),'hex');
 IF v_envelope IS NULL OR NOT(v_envelope=ANY(ARRAY['cf94cbad52c1fa0792f0498b2ec30b0b3cddd6a192bec60409f0126cdce55869','6c16f38bb51b21ae0292fe5d9145a3a1e6a2fabb41433f1e2387c7b4efa5adc4','e7bdc4edf135b0e2af77da61c70a29928e08d2b1295902209192f354a8b4e114','394e880de2edd2b5c4b46d39365156fd5506937d89f768a11a628eeb1aacf2f8','bb76b4c938e44a0c4a71858fb2c0c53ff0ecf08b651f49a133e2fe9530dd5d0c']::text[])) THEN RAISE EXCEPTION 'CTX02_UNREVIEWED_SOURCE_PROJECTION';END IF;
 PERFORM population_top3000_202608_202609_v1.d02_daily_resource_guard_v2();
 IF p_source->>'coverage_class' IS DISTINCT FROM 'REPRESENTATIVE_SOURCE_SAMPLE_NOT_COHORT_CENSUS'
 OR p_source->'wire_receipt_retained' IS DISTINCT FROM 'false'::jsonb OR p_source->'original_historical_receipt_proven' IS DISTINCT FROM 'false'::jsonb
 OR p_source->'source_completeness_verified' IS DISTINCT FROM 'false'::jsonb
 OR octet_length(p_source->>'content_csv')>1048576 OR octet_length(p_source::text)>2097152 THEN RAISE EXCEPTION 'CTX02_SOURCE_EVIDENCE_SCOPE_INVALID';END IF;
 v_id:=p_source->>'source_id';v_hash:=encode(sha256(convert_to(p_source->>'content_csv','UTF8')),'hex');
 IF v_id IS NULL OR v_id NOT LIKE 'ctx02-connector-sample:%' OR v_hash IS DISTINCT FROM p_source->>'source_content_sha256' THEN RAISE EXCEPTION 'CTX02_SOURCE_HASH_INVALID';END IF;
 SELECT count(*) INTO v_rows FROM population_top3000_202608_202609_v1.ctx02_parse_rows_v1(p_source->>'source_kind',p_source->'selector',p_source->'records');
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.ctx02_parse_rows_v1(p_source->>'source_kind',p_source->'selector',p_source->'records') GROUP BY option_ticker,observation_date HAVING count(*)>1) THEN RAISE EXCEPTION 'CTX02_DUPLICATE_SOURCE_KEY';END IF;
 v_projection:=encode(sha256(convert_to((p_source->'records')::text,'UTF8')),'hex');
 SELECT * INTO v_old FROM population_top3000_202608_202609_v1.ctx02_source_page_v1 WHERE source_id=v_id;
 IF FOUND THEN
  IF v_old.source_envelope_sha256 IS DISTINCT FROM v_envelope OR v_old.records IS DISTINCT FROM p_source->'records' OR v_old.selector IS DISTINCT FROM p_source->'selector' OR v_old.source_kind IS DISTINCT FROM p_source->>'source_kind'
   OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.source_document WHERE source_id=v_id AND source_sha256=v_hash AND content_text=p_source->>'content_csv') THEN RAISE EXCEPTION 'CTX02_IMMUTABLE_SOURCE_CONFLICT';END IF;
  RETURN jsonb_build_object('state','ALREADY_RECORDED','source_id',v_id,'rows',v_rows);
 END IF;
 IF c.stored_bytes+c.reserved_bytes+octet_length(p_source->>'content_csv')>c.max_source_bytes THEN RAISE EXCEPTION 'CTX02_SOURCE_RESOURCE_BOUND';END IF;
 INSERT INTO population_top3000_202608_202609_v1.source_document(source_id,source_sha256,source_uri,source_provider,retrieved_at,content_text,content_bytes,evidence,license_policy)
 VALUES(v_id,v_hash,p_source->>'source_uri','Massive via connected tool',clock_timestamp(),p_source->>'content_csv',octet_length(p_source->>'content_csv'),
  (p_source-'content_csv'-'records')||jsonb_build_object('retrieved_at_meaning','CURRENT_IMPORT_OF_RETAINED_CONNECTOR_TRANSCRIPT','upstream_http_receipt',NULL,'projection_sha256',v_projection),
  'AUTHENTICATED_MASSIVE_CONNECTOR_INTERNAL_USE_SAMPLE');
 INSERT INTO population_top3000_202608_202609_v1.ctx02_source_page_v1(source_id,source_kind,selector,records,record_count,projection_sha256,source_envelope_sha256,evidence)
 VALUES(v_id,p_source->>'source_kind',p_source->'selector',p_source->'records',v_rows,v_projection,v_envelope,p_source-'content_csv'-'records');
 UPDATE population_top3000_202608_202609_v1.acquisition_control SET stored_bytes=stored_bytes+octet_length(p_source->>'content_csv'),updated_at=clock_timestamp() WHERE singleton;
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES('CTX02_CONNECTOR_SOURCE_SAMPLE_IMPORTED',jsonb_build_object('source_id',v_id,'rows',v_rows,'owner',p_owner,'fence',p_fence,'stop_generation',p_stop,'wire_receipt_retained',false));
 PERFORM population_top3000_202608_202609_v1.d02_daily_resource_guard_v2();
 RETURN jsonb_build_object('state','IMPORTED','source_id',v_id,'rows',v_rows);
END; $$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.ctx02_normalize_sample_v1(p_attempt uuid,p_token text)
RETURNS jsonb LANGUAGE plpgsql SET search_path=pg_catalog AS $$
DECLARE tid uuid;t population_top3000_202608_202609_v1.pipeline_task%rowtype;s population_top3000_202608_202609_v1.ctx02_source_page_v1%rowtype;r record;h text;n integer:=0;inserted integer;expected_hash text;
BEGIN
 tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);
 SELECT * INTO t FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=tid;
 IF t.family_id<>'CTX02' OR t.stage<>'NORMALIZE' THEN RAISE EXCEPTION 'CTX02_STAGE_INVALID';END IF;
 SELECT * INTO s FROM population_top3000_202608_202609_v1.ctx02_source_page_v1 WHERE source_id=t.payload->>'source_id';
 IF NOT FOUND OR s.projection_sha256 IS DISTINCT FROM t.input_versions->>'projection_sha256'
 OR population_top3000_202608_202609_v1.ctx02_parser_bundle_v1() IS DISTINCT FROM t.input_versions->>'parser_bundle_sha256'
 OR t.input_versions->>'scope_sha256' IS DISTINCT FROM '8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd'
 OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.source_document d WHERE d.source_id=s.source_id AND d.source_sha256=t.input_versions->>'source_sha256'
  AND d.source_sha256=encode(sha256(convert_to(d.content_text,'UTF8')),'hex')) THEN RAISE EXCEPTION 'CTX02_PINNED_SOURCE_CHANGED';END IF;

 IF s.source_envelope_sha256 IS NULL OR NOT(s.source_envelope_sha256=ANY(ARRAY['cf94cbad52c1fa0792f0498b2ec30b0b3cddd6a192bec60409f0126cdce55869','6c16f38bb51b21ae0292fe5d9145a3a1e6a2fabb41433f1e2387c7b4efa5adc4','e7bdc4edf135b0e2af77da61c70a29928e08d2b1295902209192f354a8b4e114','394e880de2edd2b5c4b46d39365156fd5506937d89f768a11a628eeb1aacf2f8','bb76b4c938e44a0c4a71858fb2c0c53ff0ecf08b651f49a133e2fe9530dd5d0c']::text[]))
 OR s.projection_sha256 IS DISTINCT FROM encode(sha256(convert_to(s.records::text,'UTF8')),'hex')
 OR s.record_count IS DISTINCT FROM jsonb_array_length(s.records)
 OR s.selector IS DISTINCT FROM s.evidence->'selector' OR s.source_kind IS DISTINCT FROM s.evidence->>'source_kind'
 OR s.source_id IS DISTINCT FROM s.evidence->>'source_id'
 OR t.input_versions->>'scope_sha256' IS DISTINCT FROM '8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd'
 OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.source_document d WHERE d.source_id=s.source_id
  AND d.source_sha256=t.input_versions->>'source_sha256' AND d.source_sha256=encode(sha256(convert_to(d.content_text,'UTF8')),'hex')
  AND d.content_bytes=octet_length(d.content_text) AND d.historical_received_at IS NULL AND d.source_published_at IS NULL
  AND d.source_provider='Massive via connected tool'
  AND s.source_envelope_sha256=encode(sha256(convert_to((s.evidence||jsonb_build_object('records',s.records,'content_csv',d.content_text))::text,'UTF8')),'hex'))
 THEN RAISE EXCEPTION 'CTX02_RETAINED_SOURCE_PROJECTION_CHANGED';END IF;
 PERFORM population_top3000_202608_202609_v1.d02_daily_resource_guard_v2();
 FOR r IN SELECT * FROM population_top3000_202608_202609_v1.ctx02_parse_rows_v1(s.source_kind,s.selector,s.records) LOOP
  h:=encode(sha256(convert_to(r.normalized::text,'UTF8')),'hex');
  INSERT INTO population_top3000_202608_202609_v1.ctx02_normalized_sample_v1(source_id,source_row,source_kind,option_ticker,observation_date,underlying_ticker,normalized,record_sha256)
  VALUES(s.source_id,r.source_row,s.source_kind,r.option_ticker,r.observation_date,r.underlying_ticker,r.normalized,h) ON CONFLICT DO NOTHING;
  GET DIAGNOSTICS inserted=ROW_COUNT;
  IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.ctx02_normalized_sample_v1 z WHERE z.source_id=s.source_id AND z.source_row=r.source_row AND z.normalized=r.normalized AND z.record_sha256=h
   AND z.source_kind=s.source_kind AND z.option_ticker=r.option_ticker AND z.observation_date=r.observation_date AND z.underlying_ticker IS NOT DISTINCT FROM r.underlying_ticker) THEN RAISE EXCEPTION 'CTX02_IMMUTABLE_NORMALIZATION_CONFLICT';END IF;
  n:=n+inserted;
 END LOOP;
 IF (SELECT count(*) FROM population_top3000_202608_202609_v1.ctx02_normalized_sample_v1 WHERE source_id=s.source_id)<>s.record_count THEN RAISE EXCEPTION 'CTX02_NORMALIZED_EXACT_COUNT_MISMATCH';END IF;
 PERFORM population_top3000_202608_202609_v1.d02_daily_resource_guard_v2();
 SELECT encode(sha256(convert_to(coalesce(string_agg(source_row::text||':'||record_sha256,E'\n' ORDER BY source_row),''),'UTF8')),'hex') INTO expected_hash FROM population_top3000_202608_202609_v1.ctx02_normalized_sample_v1 WHERE source_id=s.source_id;
 RETURN jsonb_build_object('state','COMPLETE','commit_receipt',jsonb_build_object('commit_key','ctx02-normalized:'||s.source_id,'output_sha256',expected_hash,'new_committed_units',n,'immutable_output_ids',jsonb_build_array(s.source_id),'input_versions',t.input_versions,'unit_kind','PROVISIONAL_OPTION_SOURCE_RECORD','cohort_certified',false));
END; $$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.ctx02_derive_sample_v1(p_attempt uuid,p_token text)
RETURNS jsonb LANGUAGE plpgsql SET search_path=pg_catalog AS $$
DECLARE tid uuid;t population_top3000_202608_202609_v1.pipeline_task%rowtype;s population_top3000_202608_202609_v1.ctx02_source_page_v1%rowtype;e jsonb;h text;n bigint;inserted integer;
BEGIN
 tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);
 SELECT * INTO t FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=tid;
 IF t.family_id<>'CTX02' OR t.stage<>'DERIVE' THEN RAISE EXCEPTION 'CTX02_STAGE_INVALID';END IF;
 SELECT * INTO s FROM population_top3000_202608_202609_v1.ctx02_source_page_v1 WHERE source_id=t.payload->>'source_id';
 IF NOT FOUND OR s.projection_sha256 IS DISTINCT FROM t.input_versions->>'projection_sha256'
 OR population_top3000_202608_202609_v1.ctx02_parser_bundle_v1() IS DISTINCT FROM t.input_versions->>'parser_bundle_sha256' THEN RAISE EXCEPTION 'CTX02_PINNED_SOURCE_CHANGED';END IF;

 IF s.source_envelope_sha256 IS NULL OR NOT(s.source_envelope_sha256=ANY(ARRAY['cf94cbad52c1fa0792f0498b2ec30b0b3cddd6a192bec60409f0126cdce55869','6c16f38bb51b21ae0292fe5d9145a3a1e6a2fabb41433f1e2387c7b4efa5adc4','e7bdc4edf135b0e2af77da61c70a29928e08d2b1295902209192f354a8b4e114','394e880de2edd2b5c4b46d39365156fd5506937d89f768a11a628eeb1aacf2f8','bb76b4c938e44a0c4a71858fb2c0c53ff0ecf08b651f49a133e2fe9530dd5d0c']::text[]))
 OR s.projection_sha256 IS DISTINCT FROM encode(sha256(convert_to(s.records::text,'UTF8')),'hex')
 OR s.record_count IS DISTINCT FROM jsonb_array_length(s.records)
 OR s.selector IS DISTINCT FROM s.evidence->'selector' OR s.source_kind IS DISTINCT FROM s.evidence->>'source_kind'
 OR s.source_id IS DISTINCT FROM s.evidence->>'source_id'
 OR t.input_versions->>'scope_sha256' IS DISTINCT FROM '8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd'
 OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.source_document d WHERE d.source_id=s.source_id
  AND d.source_sha256=t.input_versions->>'source_sha256' AND d.source_sha256=encode(sha256(convert_to(d.content_text,'UTF8')),'hex')
  AND d.content_bytes=octet_length(d.content_text) AND d.historical_received_at IS NULL AND d.source_published_at IS NULL
  AND d.source_provider='Massive via connected tool'
  AND s.source_envelope_sha256=encode(sha256(convert_to((s.evidence||jsonb_build_object('records',s.records,'content_csv',d.content_text))::text,'UTF8')),'hex'))
 THEN RAISE EXCEPTION 'CTX02_RETAINED_SOURCE_PROJECTION_CHANGED';END IF;
 PERFORM population_top3000_202608_202609_v1.d02_daily_resource_guard_v2();
 SELECT count(*) INTO n FROM population_top3000_202608_202609_v1.ctx02_normalized_sample_v1 WHERE source_id=s.source_id;
 IF n<>s.record_count THEN RAISE EXCEPTION 'CTX02_NORMALIZATION_INCOMPLETE';END IF;
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.ctx02_parse_rows_v1(s.source_kind,s.selector,s.records) r
 LEFT JOIN population_top3000_202608_202609_v1.ctx02_normalized_sample_v1 z ON z.source_id=s.source_id AND z.source_row=r.source_row
 WHERE z.normalized IS DISTINCT FROM r.normalized OR z.record_sha256 IS DISTINCT FROM encode(sha256(convert_to(r.normalized::text,'UTF8')),'hex')
 OR z.source_kind IS DISTINCT FROM s.source_kind OR z.option_ticker IS DISTINCT FROM r.option_ticker
 OR z.observation_date IS DISTINCT FROM r.observation_date OR z.underlying_ticker IS DISTINCT FROM r.underlying_ticker)
 THEN RAISE EXCEPTION 'CTX02_NORMALIZED_EXACT_PROJECTION_MISMATCH';END IF;
 e:=jsonb_build_object('source_id',s.source_id,'source_kind',s.source_kind,'selector',s.selector,'received_sample_records',n,'normalized_sample_records',n,
 'cohort_expected_units',NULL,'population_census_complete',false,'missing_bar_is_no_trade',false,'wire_receipt_retained',false,'original_historical_receipt_proven',false,
 'qualification','REPRESENTATIVE_SOURCE_SAMPLE_ONLY','required_successor','FULL_DAILY_HISTORICAL_CONTRACT_CENSUS_AND_BULK_OR_REST_BARS','input_versions',t.input_versions);
 h:=encode(sha256(convert_to(e::text,'UTF8')),'hex');
 INSERT INTO population_top3000_202608_202609_v1.ctx02_sample_evidence_v1(source_id,evidence,evidence_sha256) VALUES(s.source_id,e,h) ON CONFLICT DO NOTHING;
 GET DIAGNOSTICS inserted=ROW_COUNT;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.ctx02_sample_evidence_v1 WHERE source_id=s.source_id AND evidence=e AND evidence_sha256=h) THEN RAISE EXCEPTION 'CTX02_IMMUTABLE_EVIDENCE_CONFLICT';END IF;
 PERFORM population_top3000_202608_202609_v1.d02_daily_resource_guard_v2();
 RETURN jsonb_build_object('state','COMPLETE','commit_receipt',jsonb_build_object('commit_key','ctx02-sample-evidence:'||s.source_id,'output_sha256',h,'new_committed_units',inserted,'immutable_output_ids',jsonb_build_array(s.source_id),'input_versions',t.input_versions,'unit_kind','PROVISIONAL_OPTION_SAMPLE_MANIFEST','cohort_certified',false));
END; $$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.ctx02_sample_quality_hold_v1(p_attempt uuid,p_token text)
RETURNS jsonb LANGUAGE plpgsql SET search_path=pg_catalog AS $$
DECLARE tid uuid;t population_top3000_202608_202609_v1.pipeline_task%rowtype;
BEGIN
 tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);
 SELECT * INTO t FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=tid;
 IF t.family_id<>'CTX02' OR t.stage<>'VALIDATE' THEN RAISE EXCEPTION 'CTX02_STAGE_INVALID';END IF;
 RETURN jsonb_build_object('state','BLOCKED','blocker_code','CTX02_FULL_CENSUS_BARS_IDENTITY_TEMPORAL_EVIDENCE_REQUIRED','resumption_condition',jsonb_build_object(
 'source_id',t.payload->>'source_id','automatic_retry',false,'required_evidence',jsonb_build_array('COMPLETE_DAILY_CONTRACT_CENSUS','DATED_COHORT_UNDERLYING_MAPPING','ALL_REQUIRED_CONTRACT_DAY_FIELDS','DOCUMENTED_MISSING_VS_NO_ELIGIBLE_TRADE_STATES','INDEPENDENT_PROVIDER_RISK_CHECKS','SEPARATE_PUBLICATION_AND_ORIGINAL_RECEIPT_QUALIFICATION'),
 'sample_does_not_establish_family_completion',true));
END; $$;
REVOKE ALL ON FUNCTION population_top3000_202608_202609_v1.ctx02_number_v1(jsonb),population_top3000_202608_202609_v1.ctx02_parse_rows_v1(text,jsonb,jsonb),population_top3000_202608_202609_v1.ctx02_import_connector_sample_v1(text,bigint,bigint,jsonb),population_top3000_202608_202609_v1.ctx02_normalize_sample_v1(uuid,text),population_top3000_202608_202609_v1.ctx02_derive_sample_v1(uuid,text),population_top3000_202608_202609_v1.ctx02_sample_quality_hold_v1(uuid,text) FROM public,anon,authenticated;
REVOKE ALL ON FUNCTION population_top3000_202608_202609_v1.ctx02_parser_bundle_v1() FROM public,anon,authenticated;
