BEGIN;SET LOCAL statement_timeout='15s';SET LOCAL lock_timeout='500ms';SELECT pg_advisory_xact_lock(2026083000261022);DO $g$ BEGIN IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.acquisition_control WHERE owner_id='top3000-recovery-20261023-root-4b1314bcb5c2' AND owner_fence=2 AND stop_generation=0 AND NOT stopped AND hold_reason IS NULL AND admitted_until>clock_timestamp()) OR EXISTS(SELECT 1 FROM cron.job WHERE jobid=596 AND active) OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE status='LEASED') OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_provider_lease WHERE released_at IS NULL) OR to_regclass('population_top3000_202608_202609_v1.archive_source_proof_v1') IS NOT NULL THEN RAISE EXCEPTION 'ARCHIVE_INITIAL_SCHEMA_REQUIRES_PAUSED_DRAINED_OWNER_AND_ABSENT_RELATIONS';END IF;END $g$;
-- Root-only installation. No task, provider request, quality-gate change or publication admission.
-- D02/S02 verified source archives use immutable, bounded row references. Source-only consumers
-- must use the explicit archive reader; the historical as-of interface is unchanged.
CREATE TABLE IF NOT EXISTS population_top3000_202608_202609_v1.archive_source_proof_v1 (
 family_id text NOT NULL CHECK(family_id IN('D02','S02')),
 unit_ordinal integer NOT NULL CHECK(unit_ordinal BETWEEN 1 AND 135),
 unit_key text NOT NULL,
 proof jsonb NOT NULL CHECK(jsonb_typeof(proof)='object'),
 proof_sha256 text NOT NULL CHECK(proof_sha256=encode(sha256(convert_to(proof::text,'UTF8')),'hex')),
 audit_event_id bigint NOT NULL REFERENCES population_top3000_202608_202609_v1.pipeline_event(event_id),
 audit_payload_sha256 text NOT NULL CHECK(audit_payload_sha256~'^[0-9a-f]{64}$'),
 PRIMARY KEY(family_id,unit_ordinal),UNIQUE(family_id,unit_key),UNIQUE(proof_sha256)
);
CREATE TABLE IF NOT EXISTS population_top3000_202608_202609_v1.archive_unit_snapshot_v1 (
 snapshot_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
 proof_sha256 text NOT NULL UNIQUE REFERENCES population_top3000_202608_202609_v1.archive_source_proof_v1(proof_sha256),
 manifest jsonb NOT NULL,
 manifest_sha256 text NOT NULL CHECK(manifest_sha256=encode(sha256(convert_to(manifest::text,'UTF8')),'hex')),
 built_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE IF NOT EXISTS population_top3000_202608_202609_v1.archive_batch_snapshot_v1 (
 snapshot_id uuid NOT NULL REFERENCES population_top3000_202608_202609_v1.archive_unit_snapshot_v1(snapshot_id),
 batch_number integer NOT NULL CHECK(batch_number BETWEEN 1 AND 50),
 first_source_row integer NOT NULL CHECK(first_source_row BETWEEN 1 AND 50000),
 last_source_row integer NOT NULL CHECK(last_source_row BETWEEN first_source_row AND first_source_row+999),
 row_count integer NOT NULL CHECK(row_count BETWEEN 1 AND 1000),
 rows_sha256 text NOT NULL CHECK(rows_sha256~'^[0-9a-f]{64}$'),
 PRIMARY KEY(snapshot_id,batch_number)
);
CREATE TABLE IF NOT EXISTS population_top3000_202608_202609_v1.archive_publication_member_v1 (
 publication_id uuid NOT NULL REFERENCES population_top3000_202608_202609_v1.pipeline_publication(publication_id),
 family_id text NOT NULL CHECK(family_id IN('D02','S02')),
 unit_ordinal integer NOT NULL,
 snapshot_id uuid NOT NULL REFERENCES population_top3000_202608_202609_v1.archive_unit_snapshot_v1(snapshot_id),
 PRIMARY KEY(publication_id,family_id,unit_ordinal)
);
CREATE TABLE IF NOT EXISTS population_top3000_202608_202609_v1.archive_delivery_batch_v1 (
 publication_id uuid NOT NULL REFERENCES population_top3000_202608_202609_v1.pipeline_publication(publication_id),
 snapshot_id uuid NOT NULL,
 batch_number integer NOT NULL,
 rows_sha256 text NOT NULL CHECK(rows_sha256~'^[0-9a-f]{64}$'),
 row_count integer NOT NULL CHECK(row_count BETWEEN 1 AND 1000),
 checked_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(publication_id,snapshot_id,batch_number),
 FOREIGN KEY(snapshot_id,batch_number) REFERENCES population_top3000_202608_202609_v1.archive_batch_snapshot_v1(snapshot_id,batch_number)
);

CREATE TABLE IF NOT EXISTS population_top3000_202608_202609_v1.archive_task_terminal_v1 (
 task_id uuid PRIMARY KEY REFERENCES population_top3000_202608_202609_v1.pipeline_task(task_id),
 stage text NOT NULL CHECK(stage IN('PUBLISH','DELIVER')),
 input_versions jsonb NOT NULL,
 receipt jsonb NOT NULL,
 receipt_sha256 text NOT NULL CHECK(receipt_sha256=encode(sha256(convert_to(receipt::text,'UTF8')),'hex')),
 recorded_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.archive_proof_v1(p_family text,p_ordinal integer)
RETURNS jsonb LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path TO 'pg_catalog' SET timezone TO 'UTC'
AS $f$
DECLARE v_proof population_top3000_202608_202609_v1.archive_source_proof_v1%rowtype;v_event jsonb;v_name text;
BEGIN
 SELECT * INTO v_proof FROM population_top3000_202608_202609_v1.archive_source_proof_v1
 WHERE family_id=p_family AND unit_ordinal=p_ordinal;
 IF NOT FOUND THEN RAISE EXCEPTION 'ARCHIVE_ACTUAL_UNIT_PROOF_REQUIRED'; END IF;
 SELECT event,details INTO v_name,v_event FROM population_top3000_202608_202609_v1.pipeline_event WHERE event_id=v_proof.audit_event_id;
 IF v_name IS DISTINCT FROM (CASE p_family WHEN 'D02' THEN 'D02_ALL_SOURCE_DATES_INDEPENDENT_RECONCILIATION_VERIFIED' ELSE 'S02_ALL_SOURCE_PAGES_INDEPENDENT_RECONCILIATION_VERIFIED' END)
  OR v_event->>'audit_payload_canonical_json_sha256' IS DISTINCT FROM v_proof.audit_payload_sha256
  OR encode(sha256(convert_to((v_event->'audit_payload')::text,'UTF8')),'hex') IS DISTINCT FROM v_proof.audit_payload_sha256
  OR v_event->>'owner_id' IS DISTINCT FROM 'top3000-recovery-20261023-root-4b1314bcb5c2'
  OR v_event->'owner_fence' IS DISTINCT FROM '2'::jsonb OR v_event->'stop_generation' IS DISTINCT FROM '0'::jsonb
  OR v_event->>'scope_sha256' IS DISTINCT FROM '8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd'
  OR v_proof.proof_sha256 IS DISTINCT FROM encode(sha256(convert_to(v_proof.proof::text,'UTF8')),'hex')
 THEN RAISE EXCEPTION 'ARCHIVE_ACTUAL_AUDIT_BINDING_CHANGED'; END IF;
 RETURN v_proof.proof||jsonb_build_object('proof_sha256',v_proof.proof_sha256,'audit_event_id',v_proof.audit_event_id,
 'audit_payload_sha256',v_proof.audit_payload_sha256,'family_id',p_family,'unit_ordinal',p_ordinal,'unit_key',v_proof.unit_key);
END $f$;

-- Bounded metadata/version check. Full source bytes are rehashed during publication;
-- unchanged source xmin + manifest attributes preserve that binding during each read.
CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.archive_source_header_v1(p_proof jsonb,p_rehash boolean)
RETURNS jsonb LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path TO 'pg_catalog' SET timezone TO 'UTC'
AS $f$
DECLARE v_header jsonb;v_doc population_top3000_202608_202609_v1.source_document%rowtype;v_xmin text;
 v_day population_top3000_202608_202609_v1.d02_daily_source_v2%rowtype;v_day_xmin text;
 v_expected jsonb:=p_proof->'postcheck';v_page jsonb;
BEGIN
 SELECT d.* INTO v_doc FROM population_top3000_202608_202609_v1.source_document d
 WHERE d.source_id=p_proof->>'source_id';
 IF NOT FOUND OR v_doc.source_sha256 IS DISTINCT FROM p_proof->>'source_sha256'
  OR v_doc.content_bytes IS DISTINCT FROM (p_proof->>'source_bytes')::bigint
  OR v_doc.source_provider IS DISTINCT FROM 'Massive' OR v_doc.source_published_at IS NOT NULL
  OR v_doc.historical_received_at IS NOT NULL
 THEN RAISE EXCEPTION 'ARCHIVE_SOURCE_DOCUMENT_BINDING_CHANGED'; END IF;
 SELECT d.xmin::text INTO v_xmin FROM population_top3000_202608_202609_v1.source_document d WHERE d.source_id=p_proof->>'source_id';
 IF p_rehash AND (v_doc.content_text IS NULL OR octet_length(convert_to(v_doc.content_text,'UTF8')) IS DISTINCT FROM v_doc.content_bytes
  OR encode(sha256(convert_to(v_doc.content_text,'UTF8')),'hex') IS DISTINCT FROM v_doc.source_sha256)
 THEN RAISE EXCEPTION 'ARCHIVE_SOURCE_BODY_CHANGED'; END IF;
 IF p_proof->>'family_id'='D02' THEN
  SELECT d.* INTO v_day FROM population_top3000_202608_202609_v1.d02_daily_source_v2 d
  WHERE d.source_day_id=(v_expected->>'source_day_id')::bigint;
  IF NOT FOUND THEN RAISE EXCEPTION 'ARCHIVE_D02_SOURCE_HEADER_CHANGED'; END IF;
  SELECT d.xmin::text INTO v_day_xmin FROM population_top3000_202608_202609_v1.d02_daily_source_v2 d WHERE d.source_day_id=v_day.source_day_id;
  IF v_day.source_id IS DISTINCT FROM p_proof->>'source_id'
   OR v_day.session_date IS DISTINCT FROM (p_proof->>'session_date')::date
   OR v_day.source_sha256 IS DISTINCT FROM p_proof->>'source_sha256'
   OR v_day.response_bytes IS DISTINCT FROM (p_proof->>'source_bytes')::bigint
   OR v_day.result_count IS DISTINCT FROM (p_proof->>'source_occurrences')::integer
   OR v_day.normalized_source_records IS DISTINCT FROM v_day.result_count OR v_day.verified_source_records IS DISTINCT FROM v_day.result_count
   OR v_day.normalized_at IS NULL OR v_day.source_verified_at IS NULL
   OR v_day.input_versions IS DISTINCT FROM v_expected->'input_versions'
   OR v_day.calendar_manifest_sha256 IS DISTINCT FROM v_expected->>'calendar_manifest_sha256'
   OR v_day_xmin IS DISTINCT FROM v_expected->>'source_row_version'
   OR v_xmin IS DISTINCT FROM v_expected->>'document_row_version'
   OR v_doc.source_uri IS DISTINCT FROM 'https://api.massive.com/v2/aggs/grouped/locale/us/market/stocks/'||v_day.session_date::text||'?adjusted=false&include_otc=false'
  THEN RAISE EXCEPTION 'ARCHIVE_D02_SOURCE_HEADER_CHANGED'; END IF;
  v_header:=jsonb_build_object('source_day_id',v_day.source_day_id,'session_date',v_day.session_date,'input_versions',v_day.input_versions,
   'source_id',v_day.source_id,'source_sha256',v_day.source_sha256,'source_bytes',v_day.response_bytes,'result_count',v_day.result_count,
   'retrieved_at',v_doc.retrieved_at,'source_uri',v_doc.source_uri,'source_row_version',v_day_xmin,'document_row_version',v_xmin);
 ELSE
  WITH page_meta_base AS MATERIALIZED (
   SELECT p.archive_key,p.family_id,p.partition_key,p.page_number,p.acquire_task_id,p.attempt_id,
    p.source_id,p.source_sha256,p.request_url,p.next_url,p.wire_sha256,p.wire_bytes,p.result_count,
    p.retrieved_at,p.normalized_at,p.normalized_source_records,p.xmin::text page_row_xmin,
    d.source_id document_source_id,d.source_sha256 document_sha256,d.content_bytes document_bytes,
    d.source_uri document_uri,d.source_provider document_provider,d.retrieved_at document_retrieved_at,
    d.source_published_at document_published_at,d.historical_received_at document_historical_received_at,
    d.license_policy,d.xmin::text document_row_xmin,
    encode(sha256(convert_to(d.evidence::text,'UTF8')),'hex') document_evidence_sha256,
    pa.task_id attempt_task_id,pa.owner_fence,pa.stop_generation,pa.started_at attempt_started_at,
    pa.ended_at attempt_ended_at,pa.outcome attempt_outcome,pa.adapter_sha256,
    pl.provider lease_provider,pl.reserved_bytes,pl.received_bytes,pl.released_at,pl.outcome lease_outcome,
    pc.commit_id,pc.attempt_id commit_attempt_id,pc.task_id commit_task_id,pc.output_sha256 committed_source_sha256,
    pc.receipt source_commit_receipt,pc.new_committed_units,pc.xmin::text commit_row_xmin
   FROM population_top3000_202608_202609_v1.massive_shortinterest_page_v1 p
   LEFT JOIN population_top3000_202608_202609_v1.source_document d ON d.source_id=p.source_id
   LEFT JOIN population_top3000_202608_202609_v1.pipeline_attempt pa ON pa.attempt_id=p.attempt_id
   LEFT JOIN population_top3000_202608_202609_v1.pipeline_provider_lease pl ON pl.attempt_id=p.attempt_id
   LEFT JOIN population_top3000_202608_202609_v1.pipeline_commit pc ON pc.commit_key='massive-page:'||p.source_id||':'||p.source_sha256
   WHERE p.archive_key=p_proof->>'archive_key' AND p.page_number=(p_proof->>'page_number')::integer
  ) SELECT to_jsonb(m) INTO v_page FROM page_meta_base m;
  IF v_page IS NULL OR encode(sha256(convert_to(v_page::text,'UTF8')),'hex') IS DISTINCT FROM p_proof#>>'{unit_receipt,source_snapshot_sha256}'
  THEN RAISE EXCEPTION 'ARCHIVE_S02_SOURCE_HEADER_CHANGED'; END IF;
  v_header:=v_page;
 END IF;
 RETURN v_header;
END $f$;

-- Only fixed, indexed source relations. Each invocation can expose at most 1,000 rows.
CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.archive_current_rows_v1(p_proof jsonb,p_first integer,p_last integer)
RETURNS TABLE(source_row integer,row_value jsonb) LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path TO 'pg_catalog' SET timezone TO 'UTC' SET datestyle TO 'ISO, YMD' SET bytea_output TO 'hex'
AS $f$
BEGIN
 IF p_first IS NULL OR p_last IS NULL OR p_first<1 OR p_last<p_first OR p_last>p_first+999 OR p_last>50000
 THEN RAISE EXCEPTION 'ARCHIVE_READ_RANGE_MUST_BE_AT_MOST_1000'; END IF;
 IF p_proof->>'family_id'='D02' THEN
  RETURN QUERY SELECT b.source_row,to_jsonb(b)-'source_day_id'
  FROM population_top3000_202608_202609_v1.d02_daily_bar_v2 b
  WHERE b.source_day_id=(p_proof#>>'{postcheck,source_day_id}')::bigint AND b.source_row BETWEEN p_first AND p_last
  ORDER BY b.source_row;
 ELSIF p_proof->>'family_id'='S02' THEN
  RETURN QUERY WITH keys AS MATERIALIZED (
   SELECT r.source_row,r.unit_key FROM population_top3000_202608_202609_v1.massive_shortinterest_record_v1 r
   WHERE r.archive_key=p_proof->>'archive_key' AND r.page_number=(p_proof->>'page_number')::integer
    AND r.source_row BETWEEN p_first AND p_last
    AND NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.massive_shortinterest_record_v1 prior
     WHERE prior.archive_key=r.archive_key AND prior.unit_key=r.unit_key
      AND (prior.page_number,prior.source_row)<(r.page_number,r.source_row))
  ) SELECT k.source_row,to_jsonb(o) FROM keys k
  JOIN population_top3000_202608_202609_v1.source_observation o
   ON o.release_id='top3000_companies_202608_202609_v1' AND o.family_id='S02'
    AND o.partition_key='short-interest-settlement-20260715-20260930' AND o.unit_key=k.unit_key
  ORDER BY k.source_row;
 ELSE RAISE EXCEPTION 'ARCHIVE_SOURCE_FAMILY_NOT_SUPPORTED'; END IF;
END $f$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.archive_source_unit_check_v1(p_family text,p_ordinal integer)
RETURNS jsonb LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path TO 'pg_catalog' SET timezone TO 'UTC' SET datestyle TO 'ISO, YMD' SET bytea_output TO 'hex'
AS $f$
DECLARE v_proof jsonb;v_header jsonb;v_count bigint;v_sha text;v_xmin_sha text;v_links text;v_bad bigint;
 v_batches jsonb:='[]';v_batch jsonb;v_first integer;v_last integer;v_expected_count integer;v_batch_count bigint;
BEGIN
 v_proof:=population_top3000_202608_202609_v1.archive_proof_v1(p_family,p_ordinal);
 v_header:=population_top3000_202608_202609_v1.archive_source_header_v1(v_proof,true);
 v_expected_count:=(v_proof->>'canonical_rows')::integer;
 IF p_family='D02' THEN
  WITH typed AS MATERIALIZED (
   SELECT b.source_row,b.xmin::text row_version,
    jsonb_build_array(b.source_row,b.source_ticker,b.provider_time_ms,b.provider_ny_date,b.provider_open,b.provider_high,b.provider_low,b.provider_close,
     b.share_volume,b.trade_count,b.provider_vwap,b.missing_mask,b.invalid_mask,b.structure_mask,encode(b.raw_record_sha256,'hex')) row_value
   FROM population_top3000_202608_202609_v1.d02_daily_bar_v2 b WHERE b.source_day_id=(v_proof#>>'{postcheck,source_day_id}')::bigint
  ) SELECT count(*),encode(sha256(convert_to(coalesce(string_agg(source_row::text||':'||encode(sha256(convert_to(row_value::text,'UTF8')),'hex'),E'\n' ORDER BY source_row),''),'UTF8')),'hex'),
   encode(sha256(convert_to(coalesce(string_agg(source_row::text||':'||row_version,E'\n' ORDER BY source_row),''),'UTF8')),'hex')
  INTO v_count,v_sha,v_xmin_sha FROM typed;
  IF v_count IS DISTINCT FROM v_expected_count OR v_sha IS DISTINCT FROM v_proof#>>'{unit_receipt,typed_manifest_sha256}'
   OR v_xmin_sha IS DISTINCT FROM v_proof#>>'{postcheck,typed_row_version_manifest_sha256}'
  THEN RAISE EXCEPTION 'ARCHIVE_D02_AUDITED_TYPED_VALUES_CHANGED'; END IF;
 ELSE
  SELECT count(*),encode(sha256(convert_to(coalesce(string_agg(jsonb_build_array(r.archive_key,r.page_number,r.source_row,r.source_id,r.source_record_key,r.unit_key,r.raw_record_sha256)::text,E'\n' ORDER BY r.source_row),''),'UTF8')),'hex')
  INTO v_count,v_links FROM population_top3000_202608_202609_v1.massive_shortinterest_record_v1 r
  WHERE r.archive_key=v_proof->>'archive_key' AND r.page_number=(v_proof->>'page_number')::integer;
  IF v_count IS DISTINCT FROM (v_proof->>'source_occurrences')::integer
   OR v_links IS DISTINCT FROM v_proof#>>'{unit_receipt,expected_raw_links_manifest_sha256}'
  THEN RAISE EXCEPTION 'ARCHIVE_S02_SOURCE_OCCURRENCE_LINKS_CHANGED'; END IF;
  WITH observed AS MATERIALIZED (
   SELECT r.source_row,r.row_value o FROM population_top3000_202608_202609_v1.archive_current_rows_v1(v_proof,1,1000) r
  ) SELECT count(*),encode(sha256(convert_to(coalesce(string_agg(jsonb_build_array(o->>'unit_key',o->>'record_sha256')::text,E'\n' ORDER BY o->>'unit_key'),''),'UTF8')),'hex'),
   count(*) FILTER(WHERE o->>'record_sha256' IS DISTINCT FROM encode(sha256(convert_to(jsonb_build_object('values',o->'values_json','states',o->'field_state','lineage',o->'lineage')::text,'UTF8')),'hex')
    OR o->>'source_id' IS DISTINCT FROM v_proof->>'source_id'
    OR o->>'company_id' IS NOT NULL OR o->>'security_id' IS NOT NULL OR o->>'reporting_entity_id' IS NOT NULL
    OR o->>'event_at' IS NOT NULL OR o->>'source_published_at' IS NOT NULL OR o->>'historical_received_at' IS NOT NULL OR o->>'available_at' IS NOT NULL
    OR o->>'availability_policy' IS DISTINCT FROM 'ARCHIVED_OBSERVATION'
    OR o->>'vintage_key' IS DISTINCT FROM v_proof->>'archive_key'
    OR o->>'normalization_sha256' IS DISTINCT FROM '172aa83338c9246ddd4fad659758850312119f65a6e3e4a130b41d0218ff5552'
    OR o->>'source_record_key' IS DISTINCT FROM o#>>'{lineage,source_record_key}'
    OR o->>'ingested_at' IS NULL
    OR o->>'effective_date' IS DISTINCT FROM o#>>'{values_json,settlement_date}'
    OR o->>'time_role' IS DISTINCT FROM (CASE WHEN o#>>'{values_json,settlement_date}' IN('2026-07-15','2026-07-31') THEN 'SUPPORT' ELSE 'TARGET' END))
  INTO v_count,v_sha,v_bad FROM observed;
  IF v_count IS DISTINCT FROM v_expected_count OR v_sha IS DISTINCT FROM v_proof#>>'{unit_receipt,actual_canonical_manifest_sha256}' OR v_bad<>0
  THEN RAISE EXCEPTION 'ARCHIVE_S02_AUDITED_NORMALIZED_VALUES_CHANGED'; END IF;
 END IF;
 -- Hash references after checking the actual independent proof. No second payload copy.
 IF (v_proof->>'source_occurrences')::integer>0 THEN
  FOR v_first IN SELECT generate_series(1,(v_proof->>'source_occurrences')::integer,1000) LOOP
   v_last:=least(v_first+999,(v_proof->>'source_occurrences')::integer);
   SELECT count(*),encode(sha256(convert_to(coalesce(string_agg(r.source_row::text||':'||encode(sha256(convert_to(r.row_value::text,'UTF8')),'hex'),E'\n' ORDER BY r.source_row),''),'UTF8')),'hex')
   INTO v_batch_count,v_sha FROM population_top3000_202608_202609_v1.archive_current_rows_v1(v_proof,v_first,v_last) r;
   IF v_batch_count>0 THEN
    v_batch:=jsonb_build_object('batch_number',(v_first-1)/1000+1,'first_source_row',v_first,'last_source_row',v_last,'row_count',v_batch_count,'rows_sha256',v_sha);
    v_batches:=v_batches||jsonb_build_array(v_batch);
   END IF;
  END LOOP;
 END IF;
 IF (SELECT coalesce(sum((b->>'row_count')::bigint),0) FROM jsonb_array_elements(v_batches) b) IS DISTINCT FROM v_expected_count::bigint
 THEN RAISE EXCEPTION 'ARCHIVE_REFERENCE_BATCH_CENSUS_MISMATCH'; END IF;
 RETURN jsonb_build_object('result','PASS_CURRENT_AUDITED_SOURCE_UNIT_ONLY','family_id',p_family,'unit_ordinal',p_ordinal,
  'proof_sha256',v_proof->>'proof_sha256','audit_event_id',v_proof->'audit_event_id','audit_payload_sha256',v_proof->>'audit_payload_sha256',
  'source_id',v_proof->>'source_id','source_sha256',v_proof->>'source_sha256','source_bytes',v_proof->'source_bytes',
  'source_occurrences',v_proof->'source_occurrences','canonical_rows',v_expected_count,'source_header',v_header,'batches',v_batches,
  'source_header_sha256',encode(sha256(convert_to(v_header::text,'UTF8')),'hex'),
  'company_population_verified',false,'required_family_validation_pass',false,'historical_predictor_eligible',false,
  'independent_provider_accuracy_verified',false,'historical_receipt_proven',false);
END $f$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.archive_reference_bundle_v1()
RETURNS jsonb LANGUAGE sql STABLE SET search_path TO 'pg_catalog' AS $f$
 SELECT jsonb_object_agg(x.name,encode(sha256(convert_to(pg_get_functiondef(x.signature::regprocedure),'UTF8')),'hex'))
 ||jsonb_build_object('CATALOG',encode(sha256(convert_to(pg_get_viewdef(to_regclass('population_top3000_202608_202609_v1.population_archive_catalog_current_v1'),true),'UTF8')),'hex'))
 FROM (VALUES
 ('PROOF','population_top3000_202608_202609_v1.archive_proof_v1(text,integer)'),
 ('HEADER','population_top3000_202608_202609_v1.archive_source_header_v1(jsonb,boolean)'),
 ('ROWS','population_top3000_202608_202609_v1.archive_current_rows_v1(jsonb,integer,integer)'),
 ('CHECK','population_top3000_202608_202609_v1.archive_source_unit_check_v1(text,integer)'),
 ('CONTEXT','population_top3000_202608_202609_v1.archive_reference_context_v1(uuid,text)'),
 ('PUBLISH','population_top3000_202608_202609_v1.archive_reference_publish_v1(uuid,text)'),
 ('READ','population_top3000_202608_202609_v1.population_archive_read_batch_v1(uuid,text,integer,integer)'),
 ('DELIVER','population_top3000_202608_202609_v1.archive_reference_deliver_v1(uuid,text)')) x(name,signature);
$f$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.archive_reference_context_v1(p_attempt uuid,p_token text)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path TO 'pg_catalog'
AS $f$
DECLARE v_tid uuid;v_task population_top3000_202608_202609_v1.pipeline_task%rowtype;v_set_sha text;v_selection jsonb;v_hashes jsonb;
 v_count bigint;v_selection_count bigint;v_unique bigint;
BEGIN
 v_tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);
 SELECT * INTO v_task FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=v_tid;
 IF v_task.family_id NOT IN('D02','S02') OR v_task.stage NOT IN('PUBLISH','DELIVER')
  OR v_task.payload->>'publication_purpose' IS DISTINCT FROM 'VERIFIED_SOURCE_ARCHIVE_ONLY'
  OR v_task.partition_key IS DISTINCT FROM 'verified-source-archive-reference-v1'
  OR v_task.input_versions->>'scope_sha256' IS DISTINCT FROM '8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd'
  OR v_task.input_versions->>'archive_publication_revision' IS DISTINCT FROM 'reference-archive-20261023-v1'
  OR jsonb_typeof(v_task.payload->'proof_ordinals') IS DISTINCT FROM 'array'
 THEN RAISE EXCEPTION 'ARCHIVE_PUBLICATION_EXPLICIT_SCOPE_REQUIRED'; END IF;
 v_selection:=v_task.payload->'proof_ordinals';
 IF jsonb_array_length(v_selection)<1 OR jsonb_array_length(v_selection)>135
  OR EXISTS(SELECT 1 FROM jsonb_array_elements(v_selection) x WHERE jsonb_typeof(x) IS DISTINCT FROM 'number' OR x::text!~'^[1-9][0-9]{0,2}$')
 THEN RAISE EXCEPTION 'ARCHIVE_PUBLICATION_SELECTION_INVALID'; END IF;
 SELECT count(*),count(DISTINCT (x#>>'{}')::integer) INTO v_selection_count,v_unique FROM jsonb_array_elements(v_selection) x;
 IF v_selection_count<>v_unique OR v_selection IS DISTINCT FROM (SELECT jsonb_agg((x#>>'{}')::integer ORDER BY (x#>>'{}')::integer) FROM jsonb_array_elements(v_selection) x)
  OR EXISTS(SELECT 1 FROM jsonb_array_elements(v_selection) x WHERE NOT EXISTS(
   SELECT 1 FROM population_top3000_202608_202609_v1.archive_source_proof_v1 p WHERE p.family_id=v_task.family_id AND p.unit_ordinal=(x#>>'{}')::integer))
  OR v_task.input_versions->>'selection_sha256' IS DISTINCT FROM encode(sha256(convert_to(v_selection::text,'UTF8')),'hex')
 THEN RAISE EXCEPTION 'ARCHIVE_PUBLICATION_SELECTION_CHANGED'; END IF;
 SELECT count(*),encode(sha256(convert_to(string_agg(unit_ordinal::text||':'||proof_sha256,E'\n' ORDER BY unit_ordinal),'UTF8')),'hex')
 INTO v_count,v_set_sha FROM population_top3000_202608_202609_v1.archive_source_proof_v1 WHERE family_id=v_task.family_id;
 IF v_count IS DISTINCT FROM (CASE v_task.family_id WHEN 'D02' THEN 42::bigint ELSE 135::bigint END)
  OR v_set_sha IS DISTINCT FROM v_task.input_versions->>'all_source_proof_set_sha256'
 THEN RAISE EXCEPTION 'ARCHIVE_PUBLICATION_COMPLETE_AUDIT_PROOF_SET_CHANGED'; END IF;
 v_hashes:=population_top3000_202608_202609_v1.archive_reference_bundle_v1();
 IF v_task.input_versions->>'publication_bundle_sha256' IS DISTINCT FROM encode(sha256(convert_to(v_hashes::text,'UTF8')),'hex')
  OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_adapter a WHERE a.family_id=v_task.family_id AND a.stage=v_task.stage
   AND a.reviewed_test_evidence->'function_sha256'=v_hashes)
 THEN RAISE EXCEPTION 'ARCHIVE_PUBLICATION_REVIEWED_DEPENDENCY_CHANGED'; END IF;
 RETURN jsonb_build_object('task_id',v_tid,'family_id',v_task.family_id,'stage',v_task.stage,'selection',v_selection,
  'input_versions',v_task.input_versions,'checkpoint',v_task.checkpoint,'function_sha256',v_hashes);
END $f$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.archive_reference_publish_v1(p_attempt uuid,p_token text)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path TO 'pg_catalog' SET timezone TO 'UTC' SET lock_timeout TO '500ms'
AS $f$
DECLARE v_context jsonb;v_family text;v_selection jsonb;v_index integer;v_ordinal integer;v_unit jsonb;v_sha text;v_snapshot uuid;
 v_existing jsonb;v_batch jsonb;v_prev uuid;v_pub uuid:=gen_random_uuid();v_members jsonb;v_manifest jsonb;v_count bigint;v_total bigint;v_reply jsonb;v_new_units bigint;
BEGIN
 v_context:=population_top3000_202608_202609_v1.archive_reference_context_v1(p_attempt,p_token);
 IF v_context->>'stage' IS DISTINCT FROM 'PUBLISH' THEN RAISE EXCEPTION 'ARCHIVE_PUBLISH_STAGE_REQUIRED'; END IF;
 SELECT t.receipt INTO v_existing FROM population_top3000_202608_202609_v1.archive_task_terminal_v1 t
 WHERE t.task_id=(v_context->>'task_id')::uuid AND t.stage='PUBLISH' AND t.input_versions=v_context->'input_versions'
  AND t.receipt_sha256=encode(sha256(convert_to(t.receipt::text,'UTF8')),'hex');
 IF FOUND THEN
  IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_publication p
   WHERE p.publication_id=(v_existing->>'publication_id')::uuid AND p.manifest->'input_versions'=v_context->'input_versions'
    AND p.manifest_sha256=v_existing#>>'{commit_receipt,output_sha256}'
    AND p.manifest_sha256=encode(sha256(convert_to(p.manifest::text,'UTF8')),'hex'))
  THEN RAISE EXCEPTION 'ARCHIVE_TERMINAL_PUBLICATION_REPLAY_CONFLICT'; END IF;
  RETURN jsonb_set(v_existing,'{commit_receipt,new_committed_units}','0'::jsonb);
 ELSIF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.archive_task_terminal_v1 WHERE task_id=(v_context->>'task_id')::uuid)
 THEN RAISE EXCEPTION 'ARCHIVE_TERMINAL_TASK_REPLAY_CONFLICT'; END IF;

 v_family:=v_context->>'family_id';v_selection:=v_context->'selection';v_index:=coalesce((v_context#>>'{checkpoint,next_unit_index}')::integer,0);
 IF v_index<0 OR v_index>=jsonb_array_length(v_selection) THEN RAISE EXCEPTION 'ARCHIVE_PUBLISH_CHECKPOINT_INVALID'; END IF;
 v_ordinal:=(v_selection->>v_index)::integer;
 v_unit:=population_top3000_202608_202609_v1.archive_source_unit_check_v1(v_family,v_ordinal);
 v_sha:=encode(sha256(convert_to(v_unit::text,'UTF8')),'hex');
 INSERT INTO population_top3000_202608_202609_v1.archive_unit_snapshot_v1(proof_sha256,manifest,manifest_sha256)
 VALUES(v_unit->>'proof_sha256',v_unit,v_sha) ON CONFLICT(proof_sha256) DO NOTHING;
 GET DIAGNOSTICS v_new_units=ROW_COUNT;
 SELECT snapshot_id,manifest INTO v_snapshot,v_existing FROM population_top3000_202608_202609_v1.archive_unit_snapshot_v1 WHERE proof_sha256=v_unit->>'proof_sha256';
 IF v_existing IS DISTINCT FROM v_unit THEN RAISE EXCEPTION 'ARCHIVE_UNIT_IMMUTABLE_REPLAY_CONFLICT'; END IF;
 FOR v_batch IN SELECT value FROM jsonb_array_elements(v_unit->'batches') LOOP
  INSERT INTO population_top3000_202608_202609_v1.archive_batch_snapshot_v1(snapshot_id,batch_number,first_source_row,last_source_row,row_count,rows_sha256)
  VALUES(v_snapshot,(v_batch->>'batch_number')::integer,(v_batch->>'first_source_row')::integer,(v_batch->>'last_source_row')::integer,(v_batch->>'row_count')::integer,v_batch->>'rows_sha256')
  ON CONFLICT(snapshot_id,batch_number) DO NOTHING;
 END LOOP;
 IF (SELECT coalesce(jsonb_agg(to_jsonb(b)-'snapshot_id' ORDER BY b.batch_number),'[]'::jsonb)
  FROM population_top3000_202608_202609_v1.archive_batch_snapshot_v1 b WHERE b.snapshot_id=v_snapshot) IS DISTINCT FROM v_unit->'batches'
 THEN RAISE EXCEPTION 'ARCHIVE_BATCH_IMMUTABLE_REPLAY_CONFLICT'; END IF;
 IF v_index+1<jsonb_array_length(v_selection) THEN
  RETURN jsonb_build_object('state','CONTINUE','checkpoint',jsonb_build_object('next_unit_index',v_index+1),
   'commit_receipt',jsonb_build_object('commit_key','archive-unit:'||(v_unit->>'proof_sha256'),'output_sha256',v_sha,'new_committed_units',v_new_units,
    'immutable_output_ids',jsonb_build_array(v_snapshot),'input_versions',v_context->'input_versions'),'publication_admitted',false,'full_family_validation_pass',false);
 END IF;
 SELECT count(*),sum((s.manifest->>'canonical_rows')::bigint),jsonb_agg(jsonb_build_object('unit_ordinal',p.unit_ordinal,'unit_key',p.unit_key,
  'snapshot_id',s.snapshot_id,'snapshot_manifest_sha256',s.manifest_sha256,'proof_sha256',p.proof_sha256,'rows',s.manifest->'canonical_rows') ORDER BY p.unit_ordinal)
 INTO v_count,v_total,v_members FROM population_top3000_202608_202609_v1.archive_source_proof_v1 p
 JOIN population_top3000_202608_202609_v1.archive_unit_snapshot_v1 s ON s.proof_sha256=p.proof_sha256
 WHERE p.family_id=v_family AND v_selection @> jsonb_build_array(p.unit_ordinal);
 IF v_count<>jsonb_array_length(v_selection) THEN RAISE EXCEPTION 'ARCHIVE_PUBLICATION_VERIFIED_UNIT_SET_INCOMPLETE'; END IF;
 SELECT publication_id INTO v_prev FROM population_top3000_202608_202609_v1.pipeline_release_pointer WHERE singleton FOR UPDATE;
 v_manifest:=jsonb_build_object('scope_sha256',v_context#>>'{input_versions,scope_sha256}','family_id',v_family,
  'partition_key','verified-source-archive-reference-v1','publication_purpose','VERIFIED_SOURCE_ARCHIVE_ONLY',
  'classification','PROVISIONAL_SOURCE_EVIDENCE','source_archive_result','PASS_AUDITED_SOURCE_TRANSFORMATION_ONLY',
  'source_units',v_count,'published_source_rows',v_total,'members',v_members,'input_versions',v_context->'input_versions',
  'all_audited_source_units_included',v_count=CASE v_family WHEN 'D02' THEN 42 ELSE 135 END,
  'bounded_reader','population_top3000_202608_202609_v1.population_archive_read_batch_v1',
  'reader_checks_current_values_before_return',true,'payload_copies_created',0,
  'full_required_family_validation_pass',false,'company_population_verified',false,'company_day_or_event_completeness',NULL,
  'independent_provider_accuracy_verified',false,'historical_receipt_proven',false,'historical_predictor_eligible',false,
  'full_mission_gate',population_top3000_202608_202609_v1.pipeline_full_gate_v1(),'final_delivery',false,
  'remaining_required_work','Company cohort and instrument applicability, exact family-required fields, independent provider economic accuracy and supported temporal eligibility remain unresolved. Existing required validation and repair holds are preserved.');
 v_sha:=encode(sha256(convert_to(v_manifest::text,'UTF8')),'hex');
 INSERT INTO population_top3000_202608_202609_v1.pipeline_publication(publication_id,classification,company_manifest_sha256,instrument_manifest_sha256,manifest,manifest_sha256,previous_publication_id)
 SELECT v_pub,'PROVISIONAL_SOURCE_EVIDENCE',company_manifest_sha256,instrument_manifest_sha256,v_manifest,v_sha,v_prev
 FROM population_top3000_202608_202609_v1.pipeline_universe WHERE singleton;
 IF NOT FOUND THEN RAISE EXCEPTION 'ARCHIVE_PUBLICATION_UNIVERSE_ROW_REQUIRED'; END IF;
 -- Existing published snapshots remain reachable without copying their payloads.
 INSERT INTO population_top3000_202608_202609_v1.pipeline_publication_member(publication_id,family_id,partition_key,validation_id,expected_manifest_sha256,observed_manifest_sha256,snapshot_publication_id)
 SELECT v_pub,family_id,partition_key,validation_id,expected_manifest_sha256,observed_manifest_sha256,snapshot_publication_id
 FROM population_top3000_202608_202609_v1.pipeline_publication_member WHERE publication_id=v_prev;
 INSERT INTO population_top3000_202608_202609_v1.archive_publication_member_v1(publication_id,family_id,unit_ordinal,snapshot_id)
 SELECT v_pub,v_family,(m->>'unit_ordinal')::integer,(m->>'snapshot_id')::uuid FROM jsonb_array_elements(v_members) m;
 UPDATE population_top3000_202608_202609_v1.pipeline_release_pointer SET publication_id=v_pub,updated_at=clock_timestamp() WHERE singleton;
 v_reply:=jsonb_build_object('state','COMPLETE','publication_id',v_pub,'source_units',v_count,'published_source_rows',v_total,
  'full_required_family_validation_pass',false,'final_delivery',false,
  'commit_receipt',jsonb_build_object('commit_key','archive-publication:'||v_pub::text,'output_sha256',v_sha,'new_committed_units',1,
   'immutable_output_ids',jsonb_build_array(v_pub,v_snapshot),'input_versions',v_context->'input_versions'));
 INSERT INTO population_top3000_202608_202609_v1.archive_task_terminal_v1(task_id,stage,input_versions,receipt,receipt_sha256)
 VALUES((v_context->>'task_id')::uuid,'PUBLISH',v_context->'input_versions',v_reply,encode(sha256(convert_to(v_reply::text,'UTF8')),'hex'));
 RETURN v_reply;
END $f$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.population_archive_read_batch_v1(p_publication uuid,p_family text,p_ordinal integer,p_batch integer)
RETURNS jsonb LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path TO 'pg_catalog' SET timezone TO 'UTC' SET datestyle TO 'ISO, YMD' SET bytea_output TO 'hex'
AS $f$
DECLARE v_snapshot population_top3000_202608_202609_v1.archive_unit_snapshot_v1%rowtype;
 v_batch population_top3000_202608_202609_v1.archive_batch_snapshot_v1%rowtype;v_proof jsonb;v_header jsonb;v_rows jsonb;v_count bigint;v_sha text;
BEGIN
 SELECT s.* INTO v_snapshot FROM population_top3000_202608_202609_v1.archive_publication_member_v1 m
 JOIN population_top3000_202608_202609_v1.archive_unit_snapshot_v1 s ON s.snapshot_id=m.snapshot_id
 WHERE m.publication_id=p_publication AND m.family_id=p_family AND m.unit_ordinal=p_ordinal;
 IF NOT FOUND THEN RAISE EXCEPTION 'ARCHIVE_PUBLISHED_UNIT_REQUIRED'; END IF;
 v_proof:=population_top3000_202608_202609_v1.archive_proof_v1(p_family,p_ordinal);
 IF v_snapshot.proof_sha256 IS DISTINCT FROM v_proof->>'proof_sha256'
  OR v_snapshot.manifest_sha256 IS DISTINCT FROM encode(sha256(convert_to(v_snapshot.manifest::text,'UTF8')),'hex')
 THEN RAISE EXCEPTION 'ARCHIVE_PUBLISHED_PROOF_CHANGED'; END IF;
 v_header:=population_top3000_202608_202609_v1.archive_source_header_v1(v_proof,false);
 IF v_header IS DISTINCT FROM v_snapshot.manifest->'source_header' THEN RAISE EXCEPTION 'ARCHIVE_PUBLISHED_SOURCE_HEADER_CHANGED'; END IF;
 SELECT * INTO v_batch FROM population_top3000_202608_202609_v1.archive_batch_snapshot_v1 WHERE snapshot_id=v_snapshot.snapshot_id AND batch_number=p_batch;
 IF NOT FOUND THEN RAISE EXCEPTION 'ARCHIVE_PUBLISHED_BATCH_REQUIRED'; END IF;
 IF ((v_snapshot.manifest->'batches') @> jsonb_build_array(to_jsonb(v_batch)-'snapshot_id')) IS DISTINCT FROM true
 THEN RAISE EXCEPTION 'ARCHIVE_PUBLISHED_BATCH_PROOF_CHANGED'; END IF;
 WITH rows AS MATERIALIZED(SELECT * FROM population_top3000_202608_202609_v1.archive_current_rows_v1(v_proof,v_batch.first_source_row,v_batch.last_source_row))
 SELECT count(*),coalesce(jsonb_agg(jsonb_build_object('source_row',r.source_row,'value',r.row_value) ORDER BY r.source_row),'[]'::jsonb),
  encode(sha256(convert_to(coalesce(string_agg(r.source_row::text||':'||encode(sha256(convert_to(r.row_value::text,'UTF8')),'hex'),E'\n' ORDER BY r.source_row),''),'UTF8')),'hex')
 INTO v_count,v_rows,v_sha FROM rows r;
 IF v_count IS DISTINCT FROM v_batch.row_count::bigint OR v_sha IS DISTINCT FROM v_batch.rows_sha256
 THEN RAISE EXCEPTION 'ARCHIVE_PUBLISHED_REFERENCE_VALUES_CHANGED'; END IF;
 RETURN jsonb_build_object('result','PASS_CURRENT_PUBLISHED_SOURCE_BATCH_ONLY','publication_id',p_publication,'family_id',p_family,
  'unit_ordinal',p_ordinal,'unit_key',v_proof->>'unit_key','batch_number',p_batch,'row_count',v_count,'rows_sha256',v_sha,
  'source_header',v_header,'proof_sha256',v_proof->>'proof_sha256','audit_event_id',v_proof->'audit_event_id','rows',v_rows,
  'company_population_verified',false,'required_family_validation_pass',false,'historical_predictor_eligible',false,
  'independent_provider_accuracy_verified',false,'historical_receipt_proven',false);
END $f$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.archive_reference_deliver_v1(p_attempt uuid,p_token text)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path TO 'pg_catalog' SET timezone TO 'UTC' SET lock_timeout TO '500ms'
AS $f$
DECLARE v_context jsonb;v_pub uuid;v_publication jsonb;v_row record;v_read jsonb;v_processed integer:=0;
 v_expected bigint;v_checked bigint;v_rows bigint;v_manifest jsonb;v_sha text;v_delivery uuid:=gen_random_uuid();v_progress jsonb;v_existing jsonb;v_reply jsonb;
BEGIN
 v_context:=population_top3000_202608_202609_v1.archive_reference_context_v1(p_attempt,p_token);
 IF v_context->>'stage' IS DISTINCT FROM 'DELIVER' THEN RAISE EXCEPTION 'ARCHIVE_DELIVER_STAGE_REQUIRED'; END IF;
 SELECT t.receipt INTO v_existing FROM population_top3000_202608_202609_v1.archive_task_terminal_v1 t
 WHERE t.task_id=(v_context->>'task_id')::uuid AND t.stage='DELIVER' AND t.input_versions=v_context->'input_versions'
  AND t.receipt_sha256=encode(sha256(convert_to(t.receipt::text,'UTF8')),'hex');
 IF FOUND THEN
  IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_delivery d
   WHERE d.delivery_id=(v_existing->>'delivery_id')::uuid AND d.publication_id=(v_existing->>'publication_id')::uuid
    AND d.manifest->'input_versions'=v_context->'input_versions'
    AND d.manifest_sha256=v_existing#>>'{commit_receipt,output_sha256}'
    AND d.manifest_sha256=encode(sha256(convert_to(d.manifest::text,'UTF8')),'hex'))
  THEN RAISE EXCEPTION 'ARCHIVE_TERMINAL_DELIVERY_REPLAY_CONFLICT'; END IF;
  RETURN jsonb_set(v_existing,'{commit_receipt,new_committed_units}','0'::jsonb);
 ELSIF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.archive_task_terminal_v1 WHERE task_id=(v_context->>'task_id')::uuid)
 THEN RAISE EXCEPTION 'ARCHIVE_TERMINAL_TASK_REPLAY_CONFLICT'; END IF;

 SELECT p.publication_id,p.manifest INTO v_pub,v_publication FROM population_top3000_202608_202609_v1.pipeline_publication p
 WHERE p.manifest->>'publication_purpose'='VERIFIED_SOURCE_ARCHIVE_ONLY'
  AND p.manifest->>'family_id'=v_context->>'family_id' AND p.manifest->'input_versions'=v_context->'input_versions'
 ORDER BY p.published_at DESC,p.publication_id DESC LIMIT 1;
 IF NOT FOUND THEN RAISE EXCEPTION 'ARCHIVE_MATCHING_PUBLICATION_REQUIRED'; END IF;
 FOR v_row IN SELECT m.unit_ordinal,b.* FROM population_top3000_202608_202609_v1.archive_publication_member_v1 m
  JOIN population_top3000_202608_202609_v1.archive_batch_snapshot_v1 b ON b.snapshot_id=m.snapshot_id
  WHERE m.publication_id=v_pub AND NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.archive_delivery_batch_v1 d
   WHERE d.publication_id=v_pub AND d.snapshot_id=b.snapshot_id AND d.batch_number=b.batch_number)
  ORDER BY m.unit_ordinal,b.batch_number LIMIT 3
 LOOP
  v_read:=population_top3000_202608_202609_v1.population_archive_read_batch_v1(v_pub,v_context->>'family_id',v_row.unit_ordinal,v_row.batch_number);
  INSERT INTO population_top3000_202608_202609_v1.archive_delivery_batch_v1(publication_id,snapshot_id,batch_number,rows_sha256,row_count)
  VALUES(v_pub,v_row.snapshot_id,v_row.batch_number,v_read->>'rows_sha256',(v_read->>'row_count')::integer);
  v_processed:=v_processed+1;
 END LOOP;
 SELECT count(*) INTO v_expected FROM population_top3000_202608_202609_v1.archive_publication_member_v1 m
 JOIN population_top3000_202608_202609_v1.archive_batch_snapshot_v1 b ON b.snapshot_id=m.snapshot_id WHERE m.publication_id=v_pub;
 SELECT count(*),coalesce(sum(d.row_count),0) INTO v_checked,v_rows FROM population_top3000_202608_202609_v1.archive_delivery_batch_v1 d WHERE d.publication_id=v_pub;
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.archive_delivery_batch_v1 d
  LEFT JOIN population_top3000_202608_202609_v1.archive_batch_snapshot_v1 b ON b.snapshot_id=d.snapshot_id AND b.batch_number=d.batch_number
  LEFT JOIN population_top3000_202608_202609_v1.archive_publication_member_v1 m ON m.publication_id=d.publication_id AND m.snapshot_id=d.snapshot_id
  WHERE d.publication_id=v_pub AND (m.snapshot_id IS NULL OR d.rows_sha256 IS DISTINCT FROM b.rows_sha256 OR d.row_count IS DISTINCT FROM b.row_count))
 THEN RAISE EXCEPTION 'ARCHIVE_DELIVERY_RECEIPT_PROOF_CHANGED'; END IF;
 v_progress:=jsonb_build_object('publication_id',v_pub,'batches_checked',v_checked,'expected_batches',v_expected,'source_rows_read',v_rows);
 v_sha:=encode(sha256(convert_to(v_progress::text,'UTF8')),'hex');
 IF v_checked<v_expected THEN
  RETURN jsonb_build_object('state','CONTINUE','checkpoint',v_progress,
   'commit_receipt',jsonb_build_object('commit_key','archive-delivery-batches:'||v_pub::text||':'||v_checked::text,'output_sha256',v_sha,
    'new_committed_units',v_processed,'immutable_output_ids',jsonb_build_array(v_pub),'input_versions',v_context->'input_versions'));
 END IF;
 IF v_rows IS DISTINCT FROM (v_publication->>'published_source_rows')::bigint THEN RAISE EXCEPTION 'ARCHIVE_DELIVERY_ROW_CENSUS_MISMATCH'; END IF;
 v_manifest:=v_progress||jsonb_build_object('family_id',v_context->>'family_id','publication_purpose','VERIFIED_SOURCE_ARCHIVE_ONLY',
  'source_archive_reader_verification','PASS_ALL_PUBLISHED_BATCHES','artifact_export','PENDING_EXTERNAL_EXPORT','final_delivery',false,
  'required_family_validation_pass',false,'company_population_verified',false,'historical_predictor_eligible',false,
  'independent_provider_accuracy_verified',false,'historical_receipt_proven',false,
  'reader_function','population_top3000_202608_202609_v1.population_archive_read_batch_v1',
  'catalog_view','population_top3000_202608_202609_v1.population_archive_catalog_current_v1',
  'full_mission_gate',population_top3000_202608_202609_v1.pipeline_full_gate_v1(),'input_versions',v_context->'input_versions');
 v_sha:=encode(sha256(convert_to(v_manifest::text,'UTF8')),'hex');
 INSERT INTO population_top3000_202608_202609_v1.pipeline_delivery(delivery_id,publication_id,manifest,manifest_sha256)
 VALUES(v_delivery,v_pub,v_manifest,v_sha);
 v_reply:=jsonb_build_object('state','COMPLETE','delivery_id',v_delivery,'publication_id',v_pub,'source_rows_read',v_rows,'artifact_state','PENDING_EXPORT','final_delivery',false,
  'commit_receipt',jsonb_build_object('commit_key','archive-delivery:'||v_delivery::text,'output_sha256',v_sha,'new_committed_units',1,
   'immutable_output_ids',jsonb_build_array(v_delivery),'input_versions',v_context->'input_versions'));
 INSERT INTO population_top3000_202608_202609_v1.archive_task_terminal_v1(task_id,stage,input_versions,receipt,receipt_sha256)
 VALUES((v_context->>'task_id')::uuid,'DELIVER',v_context->'input_versions',v_reply,encode(sha256(convert_to(v_reply::text,'UTF8')),'hex'));
 RETURN v_reply;
END $f$;

-- Walk the existing immutable release lineage: later legacy publications cannot hide
-- prior archive members, and reverting the existing release pointer excludes later archives.
CREATE OR REPLACE VIEW population_top3000_202608_202609_v1.population_archive_catalog_current_v1 AS
WITH RECURSIVE chain AS (
 SELECT p.publication_id,p.previous_publication_id,p.manifest,0 depth,ARRAY[p.publication_id] seen
 FROM population_top3000_202608_202609_v1.pipeline_release_pointer r
 JOIN population_top3000_202608_202609_v1.pipeline_publication p ON p.publication_id=r.publication_id WHERE r.singleton
 UNION ALL
 SELECT p.publication_id,p.previous_publication_id,p.manifest,c.depth+1,c.seen||p.publication_id FROM chain c
 JOIN population_top3000_202608_202609_v1.pipeline_publication p ON p.publication_id=c.previous_publication_id
 WHERE c.depth<1024 AND NOT p.publication_id=ANY(c.seen)
), latest AS (
 SELECT DISTINCT ON(manifest->>'family_id') publication_id,manifest FROM chain
 WHERE manifest->>'publication_purpose'='VERIFIED_SOURCE_ARCHIVE_ONLY' AND manifest ? 'published_source_rows'
 ORDER BY manifest->>'family_id',depth
)
SELECT m.publication_id,m.family_id,m.unit_ordinal,p.unit_key,s.snapshot_id,s.manifest_sha256,
 (s.manifest->>'canonical_rows')::bigint source_rows,p.proof->>'source_id' source_id,p.proof->>'source_sha256' source_sha256,
 (p.proof->>'source_bytes')::bigint source_bytes,p.audit_event_id,
 false company_population_verified,false required_family_validation_pass,false historical_predictor_eligible,
 false independent_provider_accuracy_verified,false historical_receipt_proven
FROM latest l JOIN population_top3000_202608_202609_v1.archive_publication_member_v1 m ON m.publication_id=l.publication_id
JOIN population_top3000_202608_202609_v1.archive_unit_snapshot_v1 s ON s.snapshot_id=m.snapshot_id
JOIN population_top3000_202608_202609_v1.archive_source_proof_v1 p ON p.proof_sha256=s.proof_sha256;

DO $acl$
DECLARE v_name text;v_sig text;
BEGIN
 FOREACH v_name IN ARRAY ARRAY['archive_source_proof_v1','archive_unit_snapshot_v1','archive_batch_snapshot_v1','archive_publication_member_v1','archive_delivery_batch_v1','archive_task_terminal_v1'] LOOP
  EXECUTE format('ALTER TABLE population_top3000_202608_202609_v1.%I ENABLE ROW LEVEL SECURITY',v_name);
  EXECUTE format('REVOKE ALL ON population_top3000_202608_202609_v1.%I FROM PUBLIC,anon,authenticated',v_name);
 END LOOP;
 FOREACH v_sig IN ARRAY ARRAY['archive_proof_v1(text,integer)','archive_source_header_v1(jsonb,boolean)','archive_current_rows_v1(jsonb,integer,integer)',
 'archive_source_unit_check_v1(text,integer)','archive_reference_bundle_v1()','archive_reference_context_v1(uuid,text)',
 'archive_reference_publish_v1(uuid,text)','population_archive_read_batch_v1(uuid,text,integer,integer)','archive_reference_deliver_v1(uuid,text)'] LOOP
  EXECUTE 'REVOKE ALL ON FUNCTION population_top3000_202608_202609_v1.'||v_sig||' FROM PUBLIC,anon,authenticated';
  EXECUTE 'GRANT EXECUTE ON FUNCTION population_top3000_202608_202609_v1.'||v_sig||' TO service_role';
 END LOOP;
END $acl$;
REVOKE ALL ON population_top3000_202608_202609_v1.population_archive_catalog_current_v1 FROM PUBLIC,anon,authenticated;
GRANT SELECT ON population_top3000_202608_202609_v1.population_archive_catalog_current_v1 TO service_role;

COMMIT;
