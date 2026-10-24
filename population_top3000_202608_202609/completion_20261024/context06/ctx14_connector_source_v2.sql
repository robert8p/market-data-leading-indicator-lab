-- Mission-scoped connector source ingestion. Does not alter existing CTX14 run or denominators.
CREATE TABLE IF NOT EXISTS population_top3000_202608_202609_v1.ctx14_connector_record_v2 (
 source_id text NOT NULL REFERENCES population_top3000_202608_202609_v1.source_document(source_id),
 source_ordinal integer NOT NULL CHECK(source_ordinal BETWEEN 1 AND 1500),
 record_kind text NOT NULL CHECK(record_kind IN('CONTRACT','CALENDAR','SESSION','MINUTE')),
 root text NOT NULL CHECK(root IN('ES','MES','NQ','MNQ','RTY','M2K','YM','MYM')),
 contract_symbol text,
 source_date date,
 source_start_ns bigint,
 source_record jsonb NOT NULL,
 record_sha256 text NOT NULL CHECK(record_sha256 ~ '^[0-9a-f]{64}$'),
 source_sha256 text NOT NULL CHECK(source_sha256 ~ '^[0-9a-f]{64}$'),
 first_attempt_id uuid NOT NULL REFERENCES population_top3000_202608_202609_v1.pipeline_attempt(attempt_id),
 ingested_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(source_id,source_ordinal)
);
ALTER TABLE population_top3000_202608_202609_v1.ctx14_connector_record_v2 ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON population_top3000_202608_202609_v1.ctx14_connector_record_v2 FROM PUBLIC,anon,authenticated;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.ctx14_connector_stage_v2(p_attempt uuid,p_token text)
RETURNS jsonb LANGUAGE plpgsql SET search_path TO pg_catalog AS $f$
DECLARE tid uuid;t population_top3000_202608_202609_v1.pipeline_task%rowtype;
 d population_top3000_202608_202609_v1.source_document%rowtype;
 e jsonb;r jsonb;k text;root text;symbol text;dt date;ns bigint;
 n integer;ins integer:=0;bad integer;out_sha text;check_sha text;receipt jsonb;
BEGIN
 tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);
 SELECT * INTO t FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=tid;
 IF t.family_id IS DISTINCT FROM 'CTX14' OR t.payload->>'kind' IS DISTINCT FROM 'CTX14_CONNECTOR_SOURCE_V2'
  OR t.input_versions->>'scope_sha256' IS DISTINCT FROM '8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd'
  OR t.input_versions->>'CTX14_contract_sha256' IS DISTINCT FROM '688f499b22342f8a6fad2b158f17e9049b12b312e648abd19bdca7ba4e1ce592'
  OR t.input_versions->>'parser_version' IS DISTINCT FROM 'ctx14-connector-csv-v2'
  OR t.input_versions->>'connector_stage_sha256' IS DISTINCT FROM encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.ctx14_connector_stage_v2(uuid,text)'::regprocedure),'UTF8')),'hex')
  OR t.partition_key IS DISTINCT FROM t.payload->>'source_id'
  OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.catalog_family_contract
    WHERE family_id='CTX14' AND scope_id='corrected_company_scope_8e6ca1178f03822c'
      AND contract_sha256=t.input_versions->>'CTX14_contract_sha256')
 THEN RAISE EXCEPTION 'CTX14_CONNECTOR_SCOPE_MISMATCH';END IF;
 SELECT * INTO d FROM population_top3000_202608_202609_v1.source_document WHERE source_id=t.payload->>'source_id';
 IF NOT FOUND OR NOT(d.source_sha256=ANY(ARRAY['5a4d20f42bf615fc406ef53c3426327fd1454a3ab5ad7cd7cb85d638297c01a2','e6146acb984bc980c3b8f00873cf4c37e1e5dae7fe9d21af4b941b7e458e31fb','4766ed3b301dddcc0d715f64de693cad4829e5847cfeb4a9ee6ff27fa910721a','91a879b82fd47c797079981373eedd1c0af0deb6f453925873c1622056a0d9d2','d2f1d9fb6dddc902c87074647a00843bf1230a2445254f2b22dd5df865990f40','19d10f7d90ab9a49b939a0d116408e767a8a385c7061ceecdf149c820028e80c','c6c611b9183578f1afa2eedc70bdc78af38b6757f47173e28eb206b508d98024']::text[]))
  OR d.source_sha256 IS DISTINCT FROM t.input_versions->>'source_sha256'
  OR d.source_sha256 IS DISTINCT FROM encode(sha256(convert_to(d.content_text,'UTF8')),'hex')
  OR d.content_bytes IS DISTINCT FROM octet_length(d.content_text)
  OR d.content_bytes>1000000 OR d.historical_received_at IS NOT NULL
  OR d.source_published_at IS NOT NULL OR d.source_provider IS DISTINCT FROM 'Massive connected tool'
 THEN RAISE EXCEPTION 'CTX14_CONNECTOR_SOURCE_PROOF_MISMATCH';END IF;
 e:=d.content_text::jsonb;k:=e->>'kind';
 IF k NOT IN('CONTRACT','CALENDAR','SESSION','MINUTE') OR k IS NULL
  OR e->>'transport' IS DISTINCT FROM 'CONNECTED_TOOL_CSV_NOT_ORIGINAL_WIRE'
  OR e->>'provider' IS DISTINCT FROM 'Massive'
  OR e->>'historical_received_at' IS NOT NULL OR e->>'source_published_at' IS NOT NULL
  OR e->>'strict_historical_eligible' IS DISTINCT FROM 'false'
  OR jsonb_typeof(e->'records') IS DISTINCT FROM 'array'
  OR e->>'connector_text_sha256' IS DISTINCT FROM encode(sha256(convert_to(e->>'connector_text','UTF8')),'hex')
 THEN RAISE EXCEPTION 'CTX14_CONNECTOR_ENVELOPE_INVALID';END IF;
 IF d.retrieved_at IS DISTINCT FROM (e->>'received_at')::timestamptz THEN RAISE EXCEPTION 'CTX14_CONNECTOR_RECEIPT_METADATA_MISMATCH';END IF;
 n:=jsonb_array_length(e->'records');
 IF n<1 OR n>1500 OR n IS DISTINCT FROM (e->>'record_count')::integer
  OR n IS DISTINCT FROM (t.payload->>'record_count')::integer
 THEN RAISE EXCEPTION 'CTX14_CONNECTOR_RECORD_BOUND';END IF;
 PERFORM population_top3000_202608_202609_v1.d02_daily_resource_guard_v2();
 IF t.stage='ACQUIRE' THEN
  out_sha:=d.source_sha256;ins:=CASE WHEN EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_commit WHERE commit_key='ctx14-connector-v2:ACQUIRE:'||d.source_id) THEN 0 ELSE 1 END;
 ELSIF t.stage='NORMALIZE' THEN
  FOR r IN SELECT value FROM jsonb_array_elements(e->'records') LOOP
   symbol:=nullif(r->>'ticker','');
   root:=coalesce(nullif(r->>'product_code',''),substring(symbol from '^([A-Z0-9]+)[HMUZ][0-9]$'));
   IF root IS NULL OR root NOT IN('ES','MES','NQ','MNQ','RTY','M2K','YM','MYM')
    OR (symbol IS NOT NULL AND symbol !~ ('^'||root||'[HMUZ][0-9]$'))
   THEN RAISE EXCEPTION 'CTX14_CONNECTOR_ROOT_OR_CONTRACT_INVALID';END IF;
   dt:=CASE WHEN k='CONTRACT' THEN (r->>'date')::date ELSE (r->>'session_end_date')::date END;
   IF dt IS NULL OR dt<DATE '2026-07-31' OR dt>DATE '2026-09-30'
   THEN RAISE EXCEPTION 'CTX14_CONNECTOR_DATE_OUT_OF_SCOPE';END IF;
   IF k='CONTRACT' THEN
    IF nullif(r->>'first_trade_date','') IS NULL OR nullif(r->>'last_trade_date','') IS NULL
     OR (r->>'first_trade_date')::date>(r->>'last_trade_date')::date OR r->>'type' IS DISTINCT FROM 'single'
    THEN RAISE EXCEPTION 'CTX14_CONTRACT_LIFE_INVALID';END IF;
   ELSIF k='CALENDAR' THEN
    IF nullif(r->>'event','') IS NULL OR nullif(r->>'timestamp','') IS NULL
      OR (r->>'timestamp')::timestamptz < TIMESTAMPTZ '2026-07-29 00:00Z'
      OR (r->>'timestamp')::timestamptz >= TIMESTAMPTZ '2026-10-02 00:00Z'
    THEN RAISE EXCEPTION 'CTX14_CALENDAR_EVENT_INVALID';END IF;
   ELSE
    IF symbol IS NULL OR nullif(r->>'window_start','') IS NULL
      OR nullif(r->>'open','') IS NULL OR nullif(r->>'high','') IS NULL
      OR nullif(r->>'low','') IS NULL OR nullif(r->>'close','') IS NULL
      OR nullif(r->>'volume','') IS NULL OR nullif(r->>'transactions','') IS NULL
      OR (r->>'volume')::bigint<0 OR (r->>'transactions')::bigint<0
      OR lower(coalesce(r->>'open','')) IN('nan','infinity','-infinity')
      OR lower(coalesce(r->>'high','')) IN('nan','infinity','-infinity')
      OR lower(coalesce(r->>'low','')) IN('nan','infinity','-infinity')
      OR lower(coalesce(r->>'close','')) IN('nan','infinity','-infinity')
      OR (r->>'low')::numeric>least((r->>'open')::numeric,(r->>'close')::numeric)
      OR (r->>'high')::numeric<greatest((r->>'open')::numeric,(r->>'close')::numeric)
      OR (r->>'low')::numeric>(r->>'high')::numeric
      OR (k='SESSION' AND nullif(r->>'settlement_price','') IS NULL)
      OR (k='MINUTE' AND nullif(r->>'settlement_price','') IS NOT NULL)
    THEN RAISE EXCEPTION 'CTX14_BAR_STRUCTURE_INVALID';END IF;
    IF k='SESSION' AND (lower(r->>'settlement_price') IN('nan','infinity','-infinity') OR (r->>'settlement_price')::numeric IS NULL) THEN RAISE EXCEPTION 'CTX14_SETTLEMENT_NOT_FINITE_NUMERIC';END IF;
    ns:=(r->>'window_start')::bigint;
    IF ns<1785369600000000000 OR ns>=1790899200000000000 THEN RAISE EXCEPTION 'CTX14_BAR_TIME_OUT_OF_SCOPE';END IF;
   END IF;
  END LOOP;
  INSERT INTO population_top3000_202608_202609_v1.ctx14_connector_record_v2
    (source_id,source_ordinal,record_kind,root,contract_symbol,source_date,source_start_ns,source_record,record_sha256,source_sha256,first_attempt_id)
  SELECT d.source_id,x.ordinality::integer,k,
    coalesce(nullif(x.value->>'product_code',''),substring(x.value->>'ticker' from '^([A-Z0-9]+)[HMUZ][0-9]$')),
    nullif(x.value->>'ticker',''),
    CASE WHEN k='CONTRACT' THEN (x.value->>'date')::date ELSE (x.value->>'session_end_date')::date END,
    nullif(x.value->>'window_start','')::bigint,
    x.value,encode(sha256(convert_to(x.value::text,'UTF8')),'hex'),d.source_sha256,p_attempt
  FROM jsonb_array_elements(e->'records') WITH ORDINALITY x(value,ordinality)
  ON CONFLICT(source_id,source_ordinal) DO NOTHING;
  GET DIAGNOSTICS ins=ROW_COUNT;
  SELECT count(*) INTO bad FROM jsonb_array_elements(e->'records') WITH ORDINALITY x(value,ordinality)
    LEFT JOIN population_top3000_202608_202609_v1.ctx14_connector_record_v2 z
      ON z.source_id=d.source_id AND z.source_ordinal=x.ordinality
    WHERE z.source_record IS DISTINCT FROM x.value
      OR z.record_kind IS DISTINCT FROM k
      OR z.source_sha256 IS DISTINCT FROM d.source_sha256
      OR z.record_sha256 IS DISTINCT FROM encode(sha256(convert_to(x.value::text,'UTF8')),'hex');
  IF bad<>0 THEN RAISE EXCEPTION 'CTX14_CONNECTOR_IMMUTABLE_CONFLICT';END IF;
  SELECT encode(sha256(convert_to(string_agg(record_sha256,E'\n' ORDER BY source_ordinal),'UTF8')),'hex') INTO out_sha
    FROM population_top3000_202608_202609_v1.ctx14_connector_record_v2 WHERE source_id=d.source_id;
 ELSIF t.stage='DERIVE' THEN
  SELECT count(*),encode(sha256(convert_to(string_agg(record_sha256,E'\n' ORDER BY source_ordinal),'UTF8')),'hex')
    INTO bad,out_sha FROM population_top3000_202608_202609_v1.ctx14_connector_record_v2 WHERE source_id=d.source_id;
  IF bad<>n THEN RAISE EXCEPTION 'CTX14_CONNECTOR_DERIVE_COUNT_MISMATCH';END IF;
  SELECT count(*) INTO bad FROM population_top3000_202608_202609_v1.ctx14_connector_record_v2 z
    WHERE source_id=d.source_id AND (source_sha256 IS DISTINCT FROM d.source_sha256
     OR record_sha256 IS DISTINCT FROM encode(sha256(convert_to(source_record::text,'UTF8')),'hex'));
  IF bad<>0 THEN RAISE EXCEPTION 'CTX14_CONNECTOR_DERIVE_HASH_MISMATCH';END IF;
  SELECT count(*) INTO bad FROM jsonb_array_elements(e->'records') WITH ORDINALITY x(value,ordinality)
    LEFT JOIN population_top3000_202608_202609_v1.ctx14_connector_record_v2 z
      ON z.source_id=d.source_id AND z.source_ordinal=x.ordinality
    WHERE z.source_record IS DISTINCT FROM x.value
      OR z.record_kind IS DISTINCT FROM k
      OR z.root IS DISTINCT FROM coalesce(nullif(x.value->>'product_code',''),substring(x.value->>'ticker' from '^([A-Z0-9]+)[HMUZ][0-9]$'))
      OR z.contract_symbol IS DISTINCT FROM nullif(x.value->>'ticker','')
      OR z.source_date IS DISTINCT FROM CASE WHEN k='CONTRACT' THEN (x.value->>'date')::date ELSE (x.value->>'session_end_date')::date END
      OR z.source_start_ns IS DISTINCT FROM nullif(x.value->>'window_start','')::bigint;
  IF bad<>0 THEN RAISE EXCEPTION 'CTX14_CONNECTOR_DERIVE_SOURCE_MISMATCH';END IF;
 ELSIF t.stage IN('VALIDATE','REPAIR') THEN
  RETURN jsonb_build_object('state','BLOCKED','blocker_code','CTX14_SOURCE_PARTITION_INDEPENDENT_ACCEPTANCE_REQUIRED',
    'resumption_condition',jsonb_build_object('source_id',d.source_id,'source_sha256',d.source_sha256,
      'record_kind',k,'source_records',n,'next_action','Independent verification of pinned connector extraction and source definitions; integrate full census/calendar/minutes/settlement/publication/causal-roll requirement, preserving existing retained run.',
      'source_completeness_certified',false,'publication_supported_reconstruction_qualified',false,
      'strict_original_receipt_qualified',false,'partial_source_is_not_full_family',true));
 ELSE RAISE EXCEPTION 'CTX14_CONNECTOR_STAGE_INVALID';END IF;
 PERFORM population_top3000_202608_202609_v1.d02_daily_resource_guard_v2();
 receipt:=jsonb_build_object('commit_key','ctx14-connector-v2:'||t.stage||':'||d.source_id,
  'output_sha256',out_sha,'new_committed_units',ins,'immutable_output_ids',jsonb_build_array(d.source_id),
  'input_versions',t.input_versions,'source_records',n,'record_kind',k,
  'source_representation','CONNECTED_TOOL_CSV_NOT_ORIGINAL_WIRE','source_complete',false,
  'historical_received_at',NULL,'strict_historical_eligible',false,'original_required_units_unchanged',true);
 RETURN jsonb_build_object('state','COMPLETE','commit_receipt',receipt);
END $f$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.ctx14_source_acquire_v2(p_attempt uuid,p_token text)
RETURNS jsonb LANGUAGE plpgsql SET search_path TO pg_catalog AS $f$
DECLARE tid uuid;k text;
BEGIN
 tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);
 SELECT payload->>'kind' INTO k FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=tid;
 IF k='CTX14_CONNECTOR_SOURCE_V2' THEN RETURN population_top3000_202608_202609_v1.ctx14_connector_stage_v2(p_attempt,p_token); END IF;
 RETURN population_top3000_202608_202609_v1.ctx14_required_source_v1(p_attempt,p_token);
END $f$;
CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.ctx14_source_normalize_v2(p_attempt uuid,p_token text)
RETURNS jsonb LANGUAGE plpgsql SET search_path TO pg_catalog AS $f$
DECLARE tid uuid;k text;
BEGIN
 tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);
 SELECT payload->>'kind' INTO k FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=tid;
 IF k='CTX14_CONNECTOR_SOURCE_V2' THEN RETURN population_top3000_202608_202609_v1.ctx14_connector_stage_v2(p_attempt,p_token); END IF;
 RETURN population_top3000_202608_202609_v1.ctx14_normalize_v1(p_attempt,p_token);
END $f$;
CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.ctx14_source_derive_v2(p_attempt uuid,p_token text)
RETURNS jsonb LANGUAGE plpgsql SET search_path TO pg_catalog AS $f$
DECLARE tid uuid;k text;
BEGIN
 tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);
 SELECT payload->>'kind' INTO k FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=tid;
 IF k='CTX14_CONNECTOR_SOURCE_V2' THEN RETURN population_top3000_202608_202609_v1.ctx14_connector_stage_v2(p_attempt,p_token); END IF;
 RETURN population_top3000_202608_202609_v1.ctx14_derive_v1(p_attempt,p_token);
END $f$;
CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.ctx14_source_validate_v2(p_attempt uuid,p_token text)
RETURNS jsonb LANGUAGE plpgsql SET search_path TO pg_catalog AS $f$
DECLARE tid uuid;k text;
BEGIN
 tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);
 SELECT payload->>'kind' INTO k FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=tid;
 IF k='CTX14_CONNECTOR_SOURCE_V2' THEN RETURN population_top3000_202608_202609_v1.ctx14_connector_stage_v2(p_attempt,p_token); END IF;
 RETURN population_top3000_202608_202609_v1.ctx14_quality_hold_v1(p_attempt,p_token);
END $f$;
REVOKE ALL ON FUNCTION population_top3000_202608_202609_v1.ctx14_connector_stage_v2(uuid,text),
 population_top3000_202608_202609_v1.ctx14_source_acquire_v2(uuid,text),
 population_top3000_202608_202609_v1.ctx14_source_normalize_v2(uuid,text),
 population_top3000_202608_202609_v1.ctx14_source_derive_v2(uuid,text),
 population_top3000_202608_202609_v1.ctx14_source_validate_v2(uuid,text) FROM PUBLIC,anon,authenticated;

