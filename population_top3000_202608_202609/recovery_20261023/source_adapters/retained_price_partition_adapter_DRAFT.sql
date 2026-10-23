-- Definitions only: retained August Alpaca SIP/raw minute bars, one instrument and NY session per task.
-- No raw-bar copy, source mutation, provider call, dense-clock denominator, universe freeze or certification.
CREATE TABLE IF NOT EXISTS population_top3000_202608_202609_v1.retained_price_partition_v1 (
 manifest_id text PRIMARY KEY,
 family_id text NOT NULL CHECK(family_id IN('P01','P02')),
 partition_key text NOT NULL,
 input_versions jsonb NOT NULL,
 source_relation text NOT NULL CHECK(source_relation='market_fixed_20250901_20260831.alpaca_sip_bars_1m_202608'),
 source_relation_oid oid NOT NULL,
 instrument_key integer NOT NULL,
 session_date date NOT NULL CHECK(session_date BETWEEN '2026-08-01' AND '2026-08-31'),
 session_label text NOT NULL CHECK(session_label IN('regular','premarket','postmarket')),
 source_content_sha256 text NOT NULL CHECK(source_content_sha256~'^[0-9a-f]{64}$'),
 manifest_sha256 text NOT NULL CHECK(manifest_sha256~'^[0-9a-f]{64}$'),
 observed_rows bigint NOT NULL CHECK(observed_rows BETWEEN 0 AND 1440),
 first_bar timestamptz,last_bar timestamptz,
 checks jsonb NOT NULL,
 mapping_snapshot jsonb NOT NULL,
 lineage jsonb NOT NULL,
 interpretation jsonb NOT NULL,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 created_by_task uuid NOT NULL REFERENCES population_top3000_202608_202609_v1.pipeline_task(task_id),
 UNIQUE(family_id,partition_key,input_versions),
 CHECK((family_id='P01' AND session_label='regular') OR (family_id='P02' AND session_label IN('premarket','postmarket')))
);
ALTER TABLE population_top3000_202608_202609_v1.retained_price_partition_v1 ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON population_top3000_202608_202609_v1.retained_price_partition_v1 FROM PUBLIC,anon,authenticated,service_role;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.price_retained_partition_v1(p_attempt uuid,p_token text)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path TO 'pg_catalog' SET TimeZone TO 'UTC' SET lock_timeout TO '500ms'
AS $function$
DECLARE v_task_id uuid;v_task population_top3000_202608_202609_v1.pipeline_task%rowtype;
 v_cal population_top3000_202608_202609_v1.catalog_calendar%rowtype;
 v_old population_top3000_202608_202609_v1.retained_price_partition_v1%rowtype;
 v_key integer;v_date date;v_session text;v_start timestamptz;v_end timestamptz;v_session_start timestamptz;v_session_end timestamptz;
 v_map jsonb;v_aliases jsonb;v_map_sha text;v_loader_sha text;v_manifest_id text;v_manifest_sha text;v_manifest jsonb;
 v_rows bigint;v_day_rows bigint;v_nulls bigint;v_invalid bigint;v_duplicates bigint;v_off_minute bigint;v_outside_session bigint;v_missing_job bigint;
 v_first timestamptz;v_last timestamptz;v_first_loaded timestamptz;v_last_loaded timestamptz;v_jobs jsonb;v_content_sha text;
 v_checks jsonb;v_lineage jsonb;v_interpretation jsonb;v_inserted bigint:=0;v_started timestamptz:=clock_timestamp();
BEGIN
 v_task_id:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);
 SELECT * INTO v_task FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=v_task_id;
 IF v_task.family_id NOT IN('P01','P02') OR v_task.stage NOT IN('ACQUIRE','NORMALIZE')
  OR v_task.payload->>'kind' IS DISTINCT FROM 'RETAINED_ALPACA_AUGUST_SESSION' THEN RAISE EXCEPTION 'RETAINED_PRICE_SCOPE_MISMATCH';END IF;
 v_manifest_id:='retained-price:'||v_task.family_id||':'||v_task.partition_key||':'||encode(sha256(convert_to(v_task.input_versions::text,'UTF8')),'hex');
 -- A manifest is useful retained-source evidence, not sufficient observation normalization/certification.
 IF v_task.stage='NORMALIZE' THEN
  SELECT * INTO v_old FROM population_top3000_202608_202609_v1.retained_price_partition_v1 WHERE manifest_id=v_manifest_id;
  RETURN jsonb_build_object('state','BLOCKED','blocker_code',CASE WHEN v_old.manifest_id IS NULL THEN 'RETAINED_PRICE_MANIFEST_REQUIRED' ELSE 'RETAINED_PRICE_COMPANY_IDENTITY_AND_EXPECTATION_REQUIRED' END,
   'resumption_condition',jsonb_build_object('manifest_id',v_manifest_id,'required_action','Bind dated security-to-company identity and the frozen top-3000 company universe, reconcile actual source observations and declared session policy, then install reviewed normalization. Never infer expected bars from a dense minute clock.',
    'historical_first_receipt_proven',false,'automatic_retry',false,'external_fetch_admitted',false));
 END IF;
 IF (v_task.payload->>'instrument_key')!~'^[0-9]{1,9}$' OR v_task.payload->>'session_date'!~'^2026-08-[0-9]{2}$'
  OR v_task.payload->>'session_label' NOT IN('regular','premarket','postmarket') THEN RAISE EXCEPTION 'RETAINED_PRICE_PARTITION_INPUT_INVALID';END IF;
 v_key:=(v_task.payload->>'instrument_key')::integer;v_date:=(v_task.payload->>'session_date')::date;v_session:=v_task.payload->>'session_label';
 IF v_key IS NULL OR v_date IS NULL OR v_session IS NULL OR v_date NOT BETWEEN '2026-08-01' AND '2026-08-31'
  OR (v_task.family_id='P01') IS DISTINCT FROM (v_session='regular') THEN RAISE EXCEPTION 'RETAINED_PRICE_PARTITION_SCOPE_INVALID';END IF;
 SELECT * INTO v_cal FROM population_top3000_202608_202609_v1.catalog_calendar
  WHERE scope_id=v_task.payload->>'calendar_scope_id' AND session_date=v_date;
 v_loader_sha:=encode(sha256(convert_to(pg_get_functiondef('consolidation_audit.load_next_alpaca_partition()'::regprocedure),'UTF8')),'hex');
 SELECT coalesce(jsonb_agg(to_jsonb(map_rows) ORDER BY to_jsonb(map_rows)::text),'[]'::jsonb) INTO v_map
 FROM (SELECT m.instrument_key,m.symbol,m.alpaca_asset_id,m.name,m.exchange,m.asset_class,m.massive_type,m.classification_method,
   m.classification_observed_at,m.provenance FROM reference.alpaca_fixed_window_instruments_remediated_v1 m WHERE m.instrument_key=v_key ORDER BY m.symbol,m.alpaca_asset_id LIMIT 2) map_rows;
 v_aliases:='[]'::jsonb;
 IF v_task.payload->>'alias_run_id' IS NOT NULL THEN
  SELECT coalesce(jsonb_agg(to_jsonb(alias_rows) ORDER BY to_jsonb(alias_rows)::text),'[]'::jsonb) INTO v_aliases
  FROM (SELECT a.symbol,a.valid_from,a.valid_to,a.source_system_key,a.mapping_method,a.mapping_strength,a.certification_state,a.row_fingerprint
   FROM reference.equity_symbol_alias_v1 a WHERE a.run_id=(v_task.payload->>'alias_run_id')::uuid AND a.instrument_key=v_key
    AND a.valid_from<=v_date AND (a.valid_to IS NULL OR a.valid_to>=v_date) ORDER BY a.symbol,a.valid_from LIMIT 20) alias_rows;
 END IF;
 v_map:=jsonb_build_object('instrument_reference',v_map,'alias_rows',v_aliases,'alias_run_id',v_task.payload->>'alias_run_id',
  'reference_candidate_limit_reached',jsonb_array_length(v_map)=2,'alias_candidate_limit_reached',jsonb_array_length(v_aliases)=20,
  'alias_date_boundary_policy','Broad inclusive candidate lookup; effective-boundary semantics require identity review.','historical_company_identity_certified',false);
 v_map_sha:=encode(sha256(convert_to(v_map::text,'UTF8')),'hex');
 IF v_cal.session_date IS NULL OR v_task.input_versions->>'calendar_record_sha256' IS DISTINCT FROM v_cal.source_record_sha256
  OR v_task.input_versions->>'calendar_receipt_sha256' IS DISTINCT FROM v_cal.source_receipt_sha256
  OR v_task.input_versions->>'loader_definition_sha256' IS DISTINCT FROM v_loader_sha
  OR v_task.input_versions->>'mapping_snapshot_sha256' IS DISTINCT FROM v_map_sha
  OR v_task.input_versions->>'source_relation_oid' IS DISTINCT FROM 'market_fixed_20250901_20260831.alpaca_sip_bars_1m_202608'::regclass::oid::text
  OR v_task.input_versions->>'hash_codec' IS DISTINCT FROM 'PG_FLOAT8_BINARY_ROW_SHA256_V1' THEN
  RETURN jsonb_build_object('state','BLOCKED','blocker_code','RETAINED_PRICE_INPUT_VERSION_RECONCILIATION',
   'resumption_condition',jsonb_build_object('instrument_key',v_key,'session_date',v_date,'observed_mapping_sha256',v_map_sha,'observed_loader_sha256',v_loader_sha,
    'required_action','Reconcile and bind exact retained relation, mapping, calendar and loader-code hashes in a new immutable task.','automatic_retry',false));
 END IF;
 v_start:=v_date::timestamp AT TIME ZONE 'America/New_York';v_end:=(v_date+1)::timestamp AT TIME ZONE 'America/New_York';
 v_session_start:=CASE v_session WHEN 'regular' THEN v_cal.regular_open_utc WHEN 'premarket' THEN (v_date+time '04:00') AT TIME ZONE 'America/New_York' ELSE v_cal.regular_close_utc END;
 v_session_end:=CASE v_session WHEN 'regular' THEN v_cal.regular_close_utc WHEN 'premarket' THEN v_cal.regular_open_utc ELSE (v_date+time '20:00') AT TIME ZONE 'America/New_York' END;
 -- Indexed primary-key range, at most1441 rows. Hash source values without copying bars into this mission.
 WITH day_rows AS MATERIALIZED (
  SELECT b.* FROM market_fixed_20250901_20260831.alpaca_sip_bars_1m_202608 b
  WHERE b.instrument_key=v_key AND b.bar_ts>=v_start AND b.bar_ts<v_end ORDER BY b.bar_ts LIMIT 1441
 ), selected AS (
  SELECT b.*,encode(sha256(convert_to(jsonb_build_array(b.instrument_key,extract(epoch FROM b.bar_ts),b.session_label::text,
   encode(float8send(b.open),'hex'),encode(float8send(b.high),'hex'),encode(float8send(b.low),'hex'),encode(float8send(b.close),'hex'),
   b.volume,b.trade_count,encode(float8send(b.vwap),'hex'),b.loaded_by_job_id,extract(epoch FROM b.source_loaded_at))::text,'UTF8')),'hex') AS row_sha
  FROM day_rows b WHERE b.session_label::text=v_session
 ) SELECT (SELECT count(*) FROM day_rows),count(*),min(bar_ts),max(bar_ts),min(source_loaded_at),max(source_loaded_at),
  count(*) FILTER(WHERE open IS NULL OR high IS NULL OR low IS NULL OR close IS NULL OR volume IS NULL OR trade_count IS NULL OR vwap IS NULL),
  count(*) FILTER(WHERE open<=0 OR high<=0 OR low<=0 OR close<=0 OR vwap<=0 OR volume<0 OR trade_count<0 OR high<greatest(open,low,close) OR low>least(open,high,close)
   OR open::text IN('NaN','Infinity','-Infinity') OR high::text IN('NaN','Infinity','-Infinity') OR low::text IN('NaN','Infinity','-Infinity') OR close::text IN('NaN','Infinity','-Infinity') OR vwap::text IN('NaN','Infinity','-Infinity')),
  count(*)-count(DISTINCT bar_ts),count(*) FILTER(WHERE bar_ts<>date_trunc('minute',bar_ts)),
  count(*) FILTER(WHERE bar_ts<v_session_start OR bar_ts>=v_session_end),count(*) FILTER(WHERE loaded_by_job_id IS NULL),
  coalesce(jsonb_agg(DISTINCT loaded_by_job_id ORDER BY loaded_by_job_id),'[]'::jsonb),encode(sha256(convert_to(coalesce(string_agg(row_sha,E'\n' ORDER BY bar_ts),''),'UTF8')),'hex')
 INTO v_day_rows,v_rows,v_first,v_last,v_first_loaded,v_last_loaded,v_nulls,v_invalid,v_duplicates,v_off_minute,v_outside_session,v_missing_job,v_jobs,v_content_sha FROM selected;
 IF v_day_rows>1440 THEN RETURN jsonb_build_object('state','BLOCKED','blocker_code','RETAINED_PRICE_DAY_ROW_BOUND',
  'resumption_condition',jsonb_build_object('instrument_key',v_key,'session_date',v_date,'rows_at_least',v_day_rows,'required_action','Inspect non-minute or duplicate source keys before any finer partition admission.','automatic_retry',false));END IF;
 v_checks:=jsonb_build_object('critical_or_required_null_rows',v_nulls,'invalid_ohlcv_rows',v_invalid,'duplicate_timestamp_rows',v_duplicates,
  'off_minute_rows',v_off_minute,'outside_declared_session_rows',v_outside_session,'missing_source_job_rows',v_missing_job,
  'structural_state',CASE WHEN v_rows=0 THEN 'NO_RETAINED_OBSERVATIONS' WHEN v_nulls+v_invalid+v_duplicates+v_off_minute+v_outside_session+v_missing_job=0 THEN 'OBSERVED_ROWS_STRUCTURALLY_VALID' ELSE 'OBSERVED_ROWS_HAVE_ISSUES' END);
 v_lineage:=jsonb_build_object('provider','Alpaca','feed','sip','adjustment','raw','timeframe','1Min',
  'semantics_basis','Retained loader definition contains source filters; row-level original HTTP receipts have not been proven.',
  'loader_function','consolidation_audit.load_next_alpaca_partition()','loader_definition_sha256',v_loader_sha,
  'retained_source_jobs',v_jobs,'first_source_loaded_at',v_first_loaded,'last_source_loaded_at',v_last_loaded,
  'calendar_record_sha256',v_cal.source_record_sha256,'calendar_receipt_sha256',v_cal.source_receipt_sha256,
  'mapping_snapshot_sha256',v_map_sha,'source_hash_codec','PG_FLOAT8_BINARY_ROW_SHA256_V1','source_original_wire_hash',NULL,
  'original_historical_receipt_proven',false,'no_provider_request',true,'raw_bars_copied',0);
 v_interpretation:=jsonb_build_object('company_population_state','PROVISIONAL_UNRESOLVED','expected_units',NULL,'completeness_ratio',NULL,
  'dense_clock_denominator_used',false,'missing_minutes_inferred',false,'independent_price_accuracy_verified',false,
  'company_identity_certified',false,'company_market_cap_certified',false,'publication_eligible',false,
  'source_snapshot_requires_revalidation_if_changed',true,'zero_rows_mean','No retained rows in this declared partition; not proof of no trading or not-applicability.');
 v_manifest:=jsonb_build_object('source_relation','market_fixed_20250901_20260831.alpaca_sip_bars_1m_202608','source_relation_oid',v_task.input_versions->>'source_relation_oid',
  'instrument_key',v_key,'session_date',v_date,'session_label',v_session,'rows',v_rows,'first_bar',v_first,'last_bar',v_last,
  'content_sha256',v_content_sha,'checks',v_checks,'mapping',v_map,'lineage',v_lineage,'interpretation',v_interpretation);
 v_manifest_sha:=encode(sha256(convert_to(v_manifest::text,'UTF8')),'hex');
 SELECT * INTO v_old FROM population_top3000_202608_202609_v1.retained_price_partition_v1 WHERE manifest_id=v_manifest_id;
 IF FOUND AND v_old.manifest_sha256 IS DISTINCT FROM v_manifest_sha THEN
  RETURN jsonb_build_object('state','BLOCKED','blocker_code','RETAINED_PRICE_SOURCE_CHANGED',
   'resumption_condition',jsonb_build_object('manifest_id',v_manifest_id,'previous_sha256',v_old.manifest_sha256,'current_sha256',v_manifest_sha,
    'required_action','Preserve the earlier manifest and reconcile the changed retained source or lineage using a new explicit version.','automatic_retry',false));
 END IF;
 IF clock_timestamp()-v_started>interval '4500 milliseconds' THEN RAISE EXCEPTION 'RETAINED_PRICE_PARTITION_TIME_BOUND';END IF;
 INSERT INTO population_top3000_202608_202609_v1.retained_price_partition_v1
 (manifest_id,family_id,partition_key,input_versions,source_relation,source_relation_oid,instrument_key,session_date,session_label,source_content_sha256,manifest_sha256,observed_rows,first_bar,last_bar,checks,mapping_snapshot,lineage,interpretation,created_by_task)
 VALUES(v_manifest_id,v_task.family_id,v_task.partition_key,v_task.input_versions,'market_fixed_20250901_20260831.alpaca_sip_bars_1m_202608',
  'market_fixed_20250901_20260831.alpaca_sip_bars_1m_202608'::regclass::oid,v_key,v_date,v_session,v_content_sha,v_manifest_sha,v_rows,v_first,v_last,v_checks,v_map,v_lineage,v_interpretation,v_task_id)
 ON CONFLICT(manifest_id) DO NOTHING;
 GET DIAGNOSTICS v_inserted=ROW_COUNT;
 RETURN jsonb_build_object('state','COMPLETE','commit_receipt',jsonb_build_object('commit_key','retained-price-inventory:'||v_task.operation_key,
  'output_sha256',v_manifest_sha,'new_committed_units',v_inserted,'immutable_output_ids',jsonb_build_array(v_manifest_id),'input_versions',v_task.input_versions,
  'unit_kind','RETAINED_PARTITION_MANIFEST','observed_source_rows',v_rows,'raw_bars_copied',0,'publication_eligible',false,'denominator_frozen',false));
END $function$;
REVOKE ALL ON FUNCTION population_top3000_202608_202609_v1.price_retained_partition_v1(uuid,text) FROM PUBLIC,anon,authenticated;
GRANT EXECUTE ON FUNCTION population_top3000_202608_202609_v1.price_retained_partition_v1(uuid,text) TO postgres,service_role;
