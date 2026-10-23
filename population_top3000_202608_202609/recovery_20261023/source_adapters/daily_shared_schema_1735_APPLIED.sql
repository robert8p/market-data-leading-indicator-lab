BEGIN;SET LOCAL statement_timeout='15s';SET LOCAL lock_timeout='500ms';SELECT pg_advisory_xact_lock(2026083000261022);DO $g$ BEGIN IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.acquisition_control WHERE owner_id='top3000-recovery-20261023-root-4b1314bcb5c2' AND owner_fence=2 AND stop_generation=0 AND NOT stopped AND hold_reason IS NULL AND admitted_until>clock_timestamp()) OR EXISTS(SELECT 1 FROM cron.job WHERE jobid=596 AND active) OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE status='LEASED') OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_provider_lease WHERE released_at IS NULL) OR to_regclass('population_top3000_202608_202609_v1.daily_shared_run_v1') IS NOT NULL THEN RAISE EXCEPTION 'DAILY_INITIAL_SCHEMA_REQUIRES_PAUSED_DRAINED_OWNER_AND_ABSENT_RELATIONS';END IF;END $g$;
-- Pure, bounded arithmetic shared by D03/D04/D05. No source query or mutation.
-- Values are source-series candidates. Qualification is separately retained.
-- Port of eed4dc2a... with explicit 1/5/20/60 raw returns and current daily Amihud.
-- The required realized_log_vol_20d preserves the existing unannualized sample SD.
CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.daily_shared_math_v1(p_rows jsonb)
RETURNS TABLE(
 session_date date,
 raw_returns double precision[], adjusted_returns double precision[],
 realized_log_vol_20d double precision,
 annualized_log_vol_20d double precision,annualized_simple_vol_20d double precision,
 relative_volume_20d double precision,volume_z20 double precision,
 daily_amihud double precision,amihud_mean_20d double precision,
 input_coverage integer[],quality_mask integer
)
LANGUAGE plpgsql IMMUTABLE SET search_path TO 'pg_catalog' AS $f$
DECLARE n integer;
BEGIN
 IF jsonb_typeof(p_rows) IS DISTINCT FROM 'array' OR octet_length(p_rows::text)>262144
 THEN RAISE EXCEPTION 'DAILY_SHARED_MATH_BOUNDED_ARRAY_REQUIRED';END IF;
 n:=jsonb_array_length(p_rows);
 IF n NOT BETWEEN 1 AND 103 OR EXISTS(
  SELECT 1 FROM jsonb_array_elements(p_rows) WITH ORDINALITY a(v,i)
  WHERE jsonb_typeof(v) IS DISTINCT FROM 'object' OR v->>'ordinal' IS DISTINCT FROM i::text
   OR coalesce(v->>'session_date','')!~'^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
   OR jsonb_typeof(v->'is_target') IS DISTINCT FROM 'boolean'
   OR coalesce(v->>'source_count','')!~'^[0-9]{1,5}$'
   OR coalesce(v->>'quality_mask','')!~'^[0-9]{1,8}$'
   OR coalesce(v->>'unknown_actions','')!~'^[0-9]{1,4}$'
   OR (v->>'split_factor') IS NULL
   OR jsonb_typeof(v->'split_factor') IS DISTINCT FROM 'number'
 ) THEN RAISE EXCEPTION 'DAILY_SHARED_MATH_ROW_CONTRACT';END IF;
 IF EXISTS(
  SELECT 1 FROM (SELECT (v->>'session_date')::date d,
    lag((v->>'session_date')::date)OVER(ORDER BY i) prev,
    to_char((v->>'session_date')::date,'YYYY-MM-DD') canonical,v->>'session_date' original
   FROM jsonb_array_elements(p_rows)WITH ORDINALITY a(v,i)) q
  WHERE d<=prev OR canonical<>original
 ) THEN RAISE EXCEPTION 'DAILY_SHARED_MATH_CALENDAR_ORDER';END IF;
 IF EXISTS(
  SELECT 1 FROM jsonb_array_elements(p_rows) a(v)
  WHERE (v->>'split_factor')::numeric<=0 OR (v->>'split_factor')::numeric>=1e20
    OR (v->>'split_factor')::numeric<=1e-20
    OR (v->>'split_factor')::numeric::text IN('NaN','Infinity','-Infinity')
    OR EXISTS(SELECT 1 FROM unnest(ARRAY['raw_close','raw_volume','dollar_volume']) k
      WHERE v ? k AND v->k<>'null'::jsonb AND jsonb_typeof(v->k) IS DISTINCT FROM 'number')
 ) THEN RAISE EXCEPTION 'DAILY_SHARED_MATH_NUMERIC_CONTRACT';END IF;
 RETURN QUERY
 WITH raw AS MATERIALIZED(
  SELECT (v->>'ordinal')::integer ord,(v->>'session_date')::date d,
   (v->>'is_target')::boolean target,(v->>'source_count')::integer source_count,
   (v->>'raw_close')::numeric raw_c,(v->>'raw_volume')::numeric raw_v,
   (v->>'dollar_volume')::numeric raw_dv,(v->>'split_factor')::numeric factor,
   (v->>'unknown_actions')::integer unknown_actions,(v->>'quality_mask')::integer mask,
   nullif(v->>'identity_anchor','') identity_anchor,nullif(v->>'price_basis','') price_basis
  FROM jsonb_array_elements(p_rows) a(v)
 ), valid AS MATERIALIZED(
  SELECT *,CASE WHEN source_count=1 AND raw_c>0 AND raw_c BETWEEN 1e-100 AND 1e100
       AND raw_c::text NOT IN('NaN','Infinity','-Infinity') THEN raw_c::float8 END c,
   CASE WHEN source_count=1 AND(raw_v=0 OR raw_v BETWEEN 1e-100 AND 1e100)
       AND raw_v::text NOT IN('NaN','Infinity','-Infinity') THEN raw_v::float8 END v,
   CASE WHEN source_count=1 AND raw_dv>0 AND raw_dv BETWEEN 1e-100 AND 1e100
       AND raw_dv::text NOT IN('NaN','Infinity','-Infinity') THEN raw_dv::float8 END dv,
   sum(ln(factor::float8))OVER(ORDER BY ord) split_log
  FROM raw
 ), adjusted AS MATERIALIZED(
  SELECT *,CASE WHEN c>0 AND abs(split_log)<300 THEN c*exp(split_log) END ac,
   CASE WHEN v=0 AND abs(split_log)<300 THEN 0::float8
    WHEN v>0 AND abs(split_log)<300 AND abs(ln(nullif(v,0))-split_log)<=ln(1e130::float8) THEN v/exp(split_log) END av
  FROM valid
 ), one_day AS MATERIALIZED(
  SELECT *,CASE WHEN ac>0 AND lag(ac)OVER w>0 AND unknown_actions=0
      AND abs(ln(ac)-ln(lag(ac)OVER w))<=300 THEN ln(ac)-ln(lag(ac)OVER w) END lr,
   CASE WHEN ac>0 AND lag(ac)OVER w>0 AND unknown_actions=0
      AND abs(ln(ac)-ln(lag(ac)OVER w))<=300 THEN ac/lag(ac)OVER w-1 END sr,
   (ac>0 AND lag(ac)OVER w>0 AND abs(ln(ac)-ln(lag(ac)OVER w))>300)IS TRUE numeric_ratio_blocked
  FROM adjusted WINDOW w AS(ORDER BY ord)
 ), stats AS MATERIALIZED(
  SELECT *,
   ARRAY[count(c)OVER w2,count(c)OVER w6,count(c)OVER w21,count(c)OVER w61]::integer[] cc,
   ARRAY[count(ac)OVER w2,count(ac)OVER w6,count(ac)OVER w21,count(ac)OVER w61]::integer[] acn,
   ARRAY[lag(c,1)OVER wi,lag(c,5)OVER wi,lag(c,20)OVER wi,lag(c,60)OVER wi] cl,
   ARRAY[lag(ac,1)OVER wi,lag(ac,5)OVER wi,lag(ac,20)OVER wi,lag(ac,60)OVER wi] al,
   ARRAY[sum(unknown_actions)OVER e1,sum(unknown_actions)OVER e5,
     sum(unknown_actions)OVER e20,sum(unknown_actions)OVER e60]::integer[] ua,
   ARRAY[CASE WHEN min(identity_anchor)OVER w2 IS DISTINCT FROM max(identity_anchor)OVER w2 THEN 1 ELSE 0 END,
    CASE WHEN min(identity_anchor)OVER w6 IS DISTINCT FROM max(identity_anchor)OVER w6 THEN 1 ELSE 0 END,
    CASE WHEN min(identity_anchor)OVER w21 IS DISTINCT FROM max(identity_anchor)OVER w21 THEN 1 ELSE 0 END,
    CASE WHEN min(identity_anchor)OVER w61 IS DISTINCT FROM max(identity_anchor)OVER w61 THEN 1 ELSE 0 END] ib,
   (count(lr)OVER e20)::integer lc20,stddev_samp(lr)OVER e20 lsd20,stddev_samp(sr)OVER e20 ssd20,
   (count(av)OVER v20)::integer vc20,avg(av)OVER v20 vm20,stddev_samp(av)OVER v20 vsd20,
   (count(*)FILTER(WHERE sr IS NOT NULL AND dv>0)OVER e20)::integer dc20,
   CASE WHEN sr IS NOT NULL AND dv>0 THEN abs(sr)/dv END amihud1,
   avg(abs(sr)/nullif(dv,0))FILTER(WHERE sr IS NOT NULL AND dv>0)OVER e20 amihud20,
   bit_or(mask)OVER w61 inherited_mask,bool_or(numeric_ratio_blocked)OVER w61 numeric_blocked_in_window,
   min(price_basis)OVER w61 minimum_basis,max(price_basis)OVER w61 maximum_basis
  FROM one_day
  WINDOW wi AS(ORDER BY ord),
   w2 AS(ORDER BY ord ROWS BETWEEN 1 PRECEDING AND CURRENT ROW),
   w6 AS(ORDER BY ord ROWS BETWEEN 5 PRECEDING AND CURRENT ROW),
   w21 AS(ORDER BY ord ROWS BETWEEN 20 PRECEDING AND CURRENT ROW),
   w61 AS(ORDER BY ord ROWS BETWEEN 60 PRECEDING AND CURRENT ROW),
   e1 AS(ORDER BY ord ROWS BETWEEN CURRENT ROW AND CURRENT ROW),
   e5 AS(ORDER BY ord ROWS BETWEEN 4 PRECEDING AND CURRENT ROW),
   e20 AS(ORDER BY ord ROWS BETWEEN 19 PRECEDING AND CURRENT ROW),
   e60 AS(ORDER BY ord ROWS BETWEEN 59 PRECEDING AND CURRENT ROW),
   v20 AS(ORDER BY ord ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING)
 )
 SELECT d,
  ARRAY[CASE WHEN cc[1]=2 AND cl[1]>0 AND ib[1]=0 THEN c/cl[1]-1 END,
    CASE WHEN cc[2]=6 AND cl[2]>0 AND ib[2]=0 THEN c/cl[2]-1 END,
    CASE WHEN cc[3]=21 AND cl[3]>0 AND ib[3]=0 THEN c/cl[3]-1 END,
    CASE WHEN cc[4]=61 AND cl[4]>0 AND ib[4]=0 THEN c/cl[4]-1 END],
  ARRAY[CASE WHEN acn[1]=2 AND al[1]>0 AND ua[1]=0 AND ib[1]=0
      AND abs(ln(ac)-ln(al[1]))<=300 THEN ac/al[1]-1 END,
    CASE WHEN acn[2]=6 AND al[2]>0 AND ua[2]=0 AND ib[2]=0
      AND abs(ln(ac)-ln(al[2]))<=300 THEN ac/al[2]-1 END,
    CASE WHEN acn[3]=21 AND al[3]>0 AND ua[3]=0 AND ib[3]=0
      AND abs(ln(ac)-ln(al[3]))<=300 THEN ac/al[3]-1 END,
    CASE WHEN acn[4]=61 AND al[4]>0 AND ua[4]=0 AND ib[4]=0
      AND abs(ln(ac)-ln(al[4]))<=300 THEN ac/al[4]-1 END],
  CASE WHEN cc[3]=21 AND lc20=20 AND ua[3]=0 AND ib[3]=0 THEN lsd20 END,
  CASE WHEN cc[3]=21 AND lc20=20 AND ua[3]=0 AND ib[3]=0 THEN lsd20*sqrt(252::float8) END,
  CASE WHEN cc[3]=21 AND lc20=20 AND ua[3]=0 AND ib[3]=0 THEN ssd20*sqrt(252::float8) END,
  CASE WHEN av=0 AND vc20=20 AND vm20>0 AND ua[3]=0 AND ib[3]=0 THEN 0::float8
   WHEN av>0 AND vc20=20 AND vm20>0 AND ua[3]=0 AND ib[3]=0
    AND ln(nullif(av,0))-ln(nullif(vm20,0))<=300 THEN av/vm20 END,
  CASE WHEN av IS NOT NULL AND vc20=20 AND vsd20>0 AND ua[3]=0 AND ib[3]=0
    AND abs(av-vm20)<=vsd20*1e130 THEN (av-vm20)/vsd20 END,
  CASE WHEN ib[1]=0 THEN amihud1 END,
  CASE WHEN cc[3]=21 AND dc20=20 AND ua[3]=0 AND ib[3]=0 THEN amihud20 END,
  cc||ARRAY[lc20,vc20,dc20,CASE WHEN v IS NOT NULL THEN 1 ELSE 0 END],
  inherited_mask
   |CASE WHEN c IS NULL THEN 1 ELSE 0 END
   |CASE WHEN cc[4]<61 THEN 2 ELSE 0 END
   |CASE WHEN v IS NULL THEN 64 ELSE 0 END
   |CASE WHEN dv IS NULL THEN 128 ELSE 0 END
   |CASE WHEN vc20=20 AND vm20=0 THEN 256 ELSE 0 END
   |CASE WHEN vc20=20 AND vsd20=0 THEN 512 ELSE 0 END
   |CASE WHEN abs(split_log)>=300 OR acn[4]<cc[4] OR(v IS NOT NULL AND av IS NULL)
     OR numeric_blocked_in_window
     OR(source_count=1 AND raw_c>0 AND(raw_c<1e-100 OR raw_c>1e100))
     OR(source_count=1 AND raw_v>0 AND(raw_v<1e-100 OR raw_v>1e100))
     OR(source_count=1 AND raw_dv>0 AND(raw_dv<1e-100 OR raw_dv>1e100))
     OR(acn[1]=2 AND al[1]>0 AND abs(ln(ac)-ln(al[1]))>300)
     OR(acn[2]=6 AND al[2]>0 AND abs(ln(ac)-ln(al[2]))>300)
     OR(acn[3]=21 AND al[3]>0 AND abs(ln(ac)-ln(al[3]))>300)
     OR(acn[4]=61 AND al[4]>0 AND abs(ln(ac)-ln(al[4]))>300)
     OR(av>0 AND vc20=20 AND vm20>0 AND ln(nullif(av,0))-ln(nullif(vm20,0))>300)
     OR(av IS NOT NULL AND vc20=20 AND vsd20>0 AND abs(av-vm20)>vsd20*1e130)
    THEN 1024 ELSE 0 END
   |CASE WHEN ib[4]=1 THEN 2048 ELSE 0 END
   |CASE WHEN minimum_basis IS DISTINCT FROM maximum_basis THEN 4096 ELSE 0 END
  FROM stats WHERE target ORDER BY d;
END $f$;
REVOKE ALL ON FUNCTION population_top3000_202608_202609_v1.daily_shared_math_v1(jsonb) FROM PUBLIC,anon,authenticated;

-- Root-only installation draft. Install daily_shared_math_v1_DRAFT.sql first.
-- This file installs private retained-source arithmetic only; it neither queues
-- work nor changes an existing producer, budget, scope, publication or controller.
CREATE TABLE IF NOT EXISTS population_top3000_202608_202609_v1.daily_shared_run_v1(
 run_id uuid PRIMARY KEY,
 run_key text NOT NULL UNIQUE,
 input_versions jsonb NOT NULL,
 source_manifest jsonb NOT NULL,
 calendar_manifest jsonb NOT NULL,
 census_sha256 text NOT NULL CHECK(census_sha256~'^[0-9a-f]{64}$'),
 series_count integer NOT NULL CHECK(series_count BETWEEN 1 AND 20000),
 batch_size integer NOT NULL CHECK(batch_size=25),
 normalized_series integer NOT NULL DEFAULT 0,
 derived_series integer NOT NULL DEFAULT 0,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 normalized_at timestamptz,derived_at timestamptz,
 CHECK(normalized_series BETWEEN 0 AND series_count),
 CHECK(derived_series BETWEEN 0 AND normalized_series)
);
CREATE TABLE IF NOT EXISTS population_top3000_202608_202609_v1.daily_shared_series_v1(
 run_id uuid NOT NULL REFERENCES population_top3000_202608_202609_v1.daily_shared_run_v1(run_id),
 series_no integer NOT NULL CHECK(series_no BETWEEN 1 AND 20000),
 source_ticker text NOT NULL CHECK(length(source_ticker) BETWEEN 1 AND 64),
 candidate_snapshot jsonb NOT NULL,
 mapping_snapshot jsonb NOT NULL,
 input_sha256 bytea CHECK(octet_length(input_sha256)=32),
 input_rows integer,
 support_rows_present integer,
 target_rows_present integer,
 target_conflict_rows integer,
 normalized_at timestamptz,
 output_sha256 bytea CHECK(octet_length(output_sha256)=32),
 output_rows integer,
 derived_at timestamptz,
 PRIMARY KEY(run_id,series_no),UNIQUE(run_id,source_ticker),
 CHECK(input_rows IS NULL OR input_rows=103),
 CHECK(output_rows IS NULL OR output_rows=42)
);
CREATE TABLE IF NOT EXISTS population_top3000_202608_202609_v1.daily_shared_input_v1(
 run_id uuid NOT NULL,series_no integer NOT NULL,session_date date NOT NULL,
 ordinal smallint NOT NULL CHECK(ordinal BETWEEN 1 AND 103),is_target boolean NOT NULL,
 source_count smallint NOT NULL CHECK(source_count BETWEEN 0 AND 2),
 raw_close numeric,raw_volume numeric,dollar_volume numeric,
 split_factor numeric NOT NULL CHECK(split_factor>1e-20 AND split_factor<1e20),
 unknown_actions smallint NOT NULL CHECK(unknown_actions BETWEEN 0 AND 9999),
 quality_mask integer NOT NULL CHECK(quality_mask>=0),
 identity_anchor text,price_basis text NOT NULL,
 source_kind smallint NOT NULL CHECK(source_kind IN(0,1,2)),
 source_key text,
 source_record_sha256 bytea NOT NULL CHECK(octet_length(source_record_sha256)=32),
 identity_record_sha256 bytea NOT NULL CHECK(octet_length(identity_record_sha256)=32),
 action_record_sha256 bytea NOT NULL CHECK(octet_length(action_record_sha256)=32),
 input_record_sha256 bytea NOT NULL CHECK(octet_length(input_record_sha256)=32),
 PRIMARY KEY(run_id,series_no,session_date),UNIQUE(run_id,series_no,ordinal),
 FOREIGN KEY(run_id,series_no)REFERENCES population_top3000_202608_202609_v1.daily_shared_series_v1(run_id,series_no),
 CHECK((is_target AND session_date BETWEEN date '2026-08-03' AND date '2026-09-30') OR(NOT is_target AND session_date BETWEEN date '2026-05-05' AND date '2026-07-31'))
);
CREATE TABLE IF NOT EXISTS population_top3000_202608_202609_v1.daily_shared_value_v1(
 run_id uuid NOT NULL,series_no integer NOT NULL,session_date date NOT NULL,
 raw_returns double precision[] NOT NULL CHECK(cardinality(raw_returns)=4),
 adjusted_returns double precision[] NOT NULL CHECK(cardinality(adjusted_returns)=4),
 realized_log_vol_20d double precision,annualized_log_vol_20d double precision,
 annualized_simple_vol_20d double precision,relative_volume_20d double precision,volume_z20 double precision,
 daily_amihud double precision,amihud_mean_20d double precision,
 input_coverage integer[] NOT NULL CHECK(cardinality(input_coverage)=8),
 quality_mask integer NOT NULL CHECK(quality_mask>=0),
 input_series_sha256 bytea NOT NULL CHECK(octet_length(input_series_sha256)=32),
 calculator_sha256 bytea NOT NULL CHECK(octet_length(calculator_sha256)=32),
 record_sha256 bytea NOT NULL CHECK(octet_length(record_sha256)=32),
 PRIMARY KEY(run_id,series_no,session_date),
 FOREIGN KEY(run_id,series_no)REFERENCES population_top3000_202608_202609_v1.daily_shared_series_v1(run_id,series_no),
 CHECK(session_date BETWEEN date '2026-08-03' AND date '2026-09-30')
);
CREATE TABLE IF NOT EXISTS population_top3000_202608_202609_v1.daily_shared_batch_v1(
 run_id uuid NOT NULL REFERENCES population_top3000_202608_202609_v1.daily_shared_run_v1(run_id),
 stage text NOT NULL CHECK(stage IN('NORMALIZE','DERIVE')),
 first_series integer NOT NULL,last_series integer NOT NULL,
 series_count integer NOT NULL CHECK(series_count BETWEEN 1 AND 25),
 row_count integer NOT NULL,manifest_sha256 text NOT NULL CHECK(manifest_sha256~'^[0-9a-f]{64}$'),
 input_versions jsonb NOT NULL,
 committed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(run_id,stage,first_series),
 CHECK(last_series-first_series+1=series_count)
);
ALTER TABLE population_top3000_202608_202609_v1.daily_shared_run_v1 ENABLE ROW LEVEL SECURITY;
ALTER TABLE population_top3000_202608_202609_v1.daily_shared_series_v1 ENABLE ROW LEVEL SECURITY;
ALTER TABLE population_top3000_202608_202609_v1.daily_shared_input_v1 ENABLE ROW LEVEL SECURITY;
ALTER TABLE population_top3000_202608_202609_v1.daily_shared_value_v1 ENABLE ROW LEVEL SECURITY;
ALTER TABLE population_top3000_202608_202609_v1.daily_shared_batch_v1 ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON population_top3000_202608_202609_v1.daily_shared_run_v1,
 population_top3000_202608_202609_v1.daily_shared_series_v1,
 population_top3000_202608_202609_v1.daily_shared_input_v1,
 population_top3000_202608_202609_v1.daily_shared_value_v1,
 population_top3000_202608_202609_v1.daily_shared_batch_v1 FROM PUBLIC,anon,authenticated;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.daily_shared_calendar_v1()
RETURNS jsonb LANGUAGE plpgsql STABLE SET search_path TO 'pg_catalog' SET timezone TO 'UTC' SET bytea_output TO 'hex' AS $f$
DECLARE target jsonb;support jsonb;all_days jsonb;n bigint;h text;
BEGIN
 target:=population_top3000_202608_202609_v1.d02_daily_calendar_v2();
 SELECT count(*),encode(sha256(convert_to(string_agg(c.session_date::text,E'\n' ORDER BY c.session_date),'UTF8')),'hex'),
  jsonb_agg(jsonb_build_object('date',c.session_date,'open',c.regular_open,'close',c.regular_close,
   'record_sha256',encode(sha256(convert_to(to_jsonb(c)::text,'UTF8')),'hex'),'is_target',false)ORDER BY c.session_date)
 INTO n,h,support FROM market_governance.us_equity_fixed_session_calendar c
 WHERE c.session_date BETWEEN date '2026-05-05' AND date '2026-07-31';
 IF n<>61 OR h IS DISTINCT FROM '228e2ce0b3e2143746c3a527cda5b88d71d6a63a9b2ceef94bf1778efd057ace'
 THEN RAISE EXCEPTION 'DAILY_SHARED_SUPPORT_CALENDAR_CHANGED';END IF;
 SELECT support||jsonb_agg(v||jsonb_build_object('is_target',true)ORDER BY v->>'date') INTO all_days
 FROM jsonb_array_elements(target->'sessions')a(v);
 RETURN jsonb_build_object('target_calendar_manifest_sha256',target->>'calendar_manifest_sha256',
  'support_date_list_sha256',h,'sessions',all_days,'calendar_manifest_sha256',encode(sha256(convert_to(all_days::text,'UTF8')),'hex'),
  'support_session_count',61,'target_session_count',42,'total_session_count',103);
END $f$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.daily_shared_input_json_v1(p_run uuid,p_series integer)
RETURNS jsonb LANGUAGE sql STABLE SET search_path TO 'pg_catalog' AS $f$
 SELECT jsonb_agg(jsonb_build_object('ordinal',i.ordinal,'session_date',i.session_date,'is_target',i.is_target,
  'source_count',i.source_count,'raw_close',i.raw_close,'raw_volume',i.raw_volume,'dollar_volume',i.dollar_volume,
  'split_factor',i.split_factor,'unknown_actions',i.unknown_actions,'quality_mask',i.quality_mask,
  'identity_anchor',i.identity_anchor,'price_basis',i.price_basis,'source_kind',i.source_kind,'source_key',i.source_key,
  'source_record_sha256',encode(i.source_record_sha256,'hex'),'identity_record_sha256',encode(i.identity_record_sha256,'hex'),
  'action_record_sha256',encode(i.action_record_sha256,'hex'),'input_record_sha256',encode(i.input_record_sha256,'hex'))ORDER BY i.ordinal)
 FROM population_top3000_202608_202609_v1.daily_shared_input_v1 i WHERE i.run_id=p_run AND i.series_no=p_series;
$f$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.daily_shared_context_v1(p_task uuid)
RETURNS uuid LANGUAGE plpgsql STABLE SET search_path TO 'pg_catalog' AS $f$
DECLARE t population_top3000_202608_202609_v1.pipeline_task%rowtype;
 r population_top3000_202608_202609_v1.daily_shared_run_v1%rowtype;
BEGIN
 SELECT * INTO t FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=p_task;
 IF NOT FOUND OR t.family_id NOT IN('D03','D04','D05') OR t.payload->>'kind' IS DISTINCT FROM 'RETAINED_SHARED_DAILY_V1'
  OR t.input_versions->>'scope_sha256' IS DISTINCT FROM '8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd'
  OR t.input_versions->>'shared_daily_revision' IS DISTINCT FROM 'retained-shared-daily-v1'
  OR t.input_versions->>'shared_daily_bundle_sha256' IS DISTINCT FROM population_top3000_202608_202609_v1.daily_shared_bundle_sha256_v1()
  OR coalesce(t.payload->>'run_id','')!~'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
 THEN RAISE EXCEPTION 'DAILY_SHARED_SCOPE_OR_BUNDLE_NOT_ADMITTED';END IF;
 IF(SELECT count(*)FROM population_top3000_202608_202609_v1.catalog_family_contract f
  WHERE f.scope_id='corrected_company_scope_8e6ca1178f03822c' AND f.family_id IN('D03','D04','D05')
   AND f.contract_sha256=t.input_versions->>(f.family_id||'_contract_sha256')
   AND f.contract_sha256=encode(sha256(convert_to(f.contract_text,'UTF8')),'hex'))<>3
 THEN RAISE EXCEPTION 'DAILY_SHARED_FROZEN_FAMILY_CONTRACT_CHANGED';END IF;
 SELECT * INTO r FROM population_top3000_202608_202609_v1.daily_shared_run_v1 WHERE run_id=(t.payload->>'run_id')::uuid;
 IF NOT FOUND OR r.input_versions IS DISTINCT FROM t.input_versions OR t.partition_key IS DISTINCT FROM r.run_key
  OR r.calendar_manifest IS DISTINCT FROM population_top3000_202608_202609_v1.daily_shared_calendar_v1()
  OR r.source_manifest IS DISTINCT FROM population_top3000_202608_202609_v1.daily_shared_source_manifest_v1()
 THEN RAISE EXCEPTION 'DAILY_SHARED_IMMUTABLE_INPUT_CHANGED';END IF;
 IF coalesce(t.payload->>'max_series','')!~'^[0-9]{1,5}$'
  OR(t.payload->>'max_series')::integer NOT BETWEEN 1 AND r.series_count
  OR((t.payload->>'max_series')::integer<>r.series_count AND(t.payload->>'max_series')::integer%25<>0)
 THEN RAISE EXCEPTION 'DAILY_SHARED_BOUNDED_SERIES_ADMISSION_REQUIRED';END IF;
 RETURN r.run_id;
END $f$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.daily_shared_normalize_v1(p_attempt uuid,p_token text)
RETURNS jsonb LANGUAGE plpgsql SET search_path TO 'pg_catalog' AS $f$
DECLARE tid uuid;rid uuid;t population_top3000_202608_202609_v1.pipeline_task%rowtype;
 r population_top3000_202608_202609_v1.daily_shared_run_v1%rowtype;
 s population_top3000_202608_202609_v1.daily_shared_series_v1%rowtype;
 prior_n integer;last_n integer;expected jsonb;actual jsonb;h bytea;manifest text;new_rows bigint:=0;inserted_n bigint;
 support_n integer;target_n integer;conflict_n integer;part_evidence jsonb;
BEGIN
 tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);
 rid:=population_top3000_202608_202609_v1.daily_shared_context_v1(tid);
 SELECT * INTO t FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=tid;
 IF t.stage<>'NORMALIZE' THEN RAISE EXCEPTION 'DAILY_SHARED_STAGE_MISMATCH';END IF;
 PERFORM population_top3000_202608_202609_v1.d02_daily_resource_guard_v2();
 SELECT * INTO r FROM population_top3000_202608_202609_v1.daily_shared_run_v1 WHERE run_id=rid FOR UPDATE;
 IF t.family_id IN('D04','D05') THEN
  IF r.derived_at IS NULL OR r.derived_series<>r.series_count THEN
   RETURN jsonb_build_object('state','BLOCKED','blocker_code','DAILY_SHARED_CALCULATION_DEPENDENCY',
    'resumption_condition',jsonb_build_object('event','D03_SHARED_DERIVE_COMPLETED','run_id',rid,'automatic_retry',false));
  END IF;
  manifest:=encode(sha256(convert_to(jsonb_build_array(rid,t.family_id,r.series_count,r.input_versions)::text,'UTF8')),'hex');
  RETURN jsonb_build_object('state','COMPLETE','commit_receipt',jsonb_build_object('commit_key','daily-shared-link:'||rid||':'||t.family_id,
   'output_sha256',manifest,'new_committed_units',0,'immutable_output_ids',jsonb_build_array(rid),
   'input_versions',t.input_versions,'shared_rows_reused',r.series_count*42,'duplicated_values_written',false,'company_population_verified',false));
 END IF;
 IF coalesce(t.checkpoint->>'last_series','0')!~'^[0-9]{1,5}$' THEN RAISE EXCEPTION 'DAILY_SHARED_CHECKPOINT_INVALID';END IF;
 prior_n:=coalesce((t.checkpoint->>'last_series')::integer,0);last_n:=least(prior_n+r.batch_size,r.series_count);
 IF prior_n>=(t.payload->>'max_series')::integer AND prior_n<r.series_count THEN
  RETURN jsonb_build_object('state','BLOCKED','blocker_code','DAILY_SHARED_FIRST_BATCH_RESOURCE_REVIEW',
   'resumption_condition',jsonb_build_object('event','FIRST_RETAINED_BATCH_STORAGE_AND_RUNTIME_REVIEW','run_id',rid,
    'normalized_series',prior_n,'admitted_series',(t.payload->>'max_series')::integer,'remaining_source_series',r.series_count-prior_n,
    'existing_inputs_preserved',true,'automatic_retry',false));
 END IF;
 IF prior_n<0 OR prior_n>=r.series_count OR prior_n%r.batch_size<>0 OR r.normalized_series NOT IN(prior_n,last_n)
 THEN RAISE EXCEPTION 'DAILY_SHARED_NORMALIZE_CHECKPOINT_DIVERGED';END IF;
 FOR s IN SELECT * FROM population_top3000_202608_202609_v1.daily_shared_series_v1
  WHERE run_id=rid AND series_no BETWEEN prior_n+1 AND last_n ORDER BY series_no LOOP
  expected:=population_top3000_202608_202609_v1.daily_shared_capture_series_v1(rid,s.series_no);
  h:=sha256(convert_to(expected::text,'UTF8'));
  INSERT INTO population_top3000_202608_202609_v1.daily_shared_input_v1
   (run_id,series_no,session_date,ordinal,is_target,source_count,raw_close,raw_volume,dollar_volume,
    split_factor,unknown_actions,quality_mask,identity_anchor,price_basis,source_kind,source_key,
    source_record_sha256,identity_record_sha256,action_record_sha256,input_record_sha256)
  SELECT rid,s.series_no,(v->>'session_date')::date,(v->>'ordinal')::smallint,(v->>'is_target')::boolean,
   (v->>'source_count')::smallint,(v->>'raw_close')::numeric,(v->>'raw_volume')::numeric,(v->>'dollar_volume')::numeric,
   (v->>'split_factor')::numeric,(v->>'unknown_actions')::smallint,(v->>'quality_mask')::integer,
   v->>'identity_anchor',v->>'price_basis',(v->>'source_kind')::smallint,v->>'source_key',
   decode(v->>'source_record_sha256','hex'),decode(v->>'identity_record_sha256','hex'),
   decode(v->>'action_record_sha256','hex'),decode(v->>'input_record_sha256','hex')
  FROM jsonb_array_elements(expected)a(v) ON CONFLICT(run_id,series_no,session_date)DO NOTHING;
  GET DIAGNOSTICS inserted_n=ROW_COUNT;new_rows:=new_rows+inserted_n;
  actual:=population_top3000_202608_202609_v1.daily_shared_input_json_v1(rid,s.series_no);
  IF actual IS DISTINCT FROM expected OR jsonb_array_length(actual)<>103 OR(s.input_sha256 IS NOT NULL AND s.input_sha256<>h)
  THEN RAISE EXCEPTION 'DAILY_SHARED_INPUT_REPLAY_CONFLICT series=%',s.series_no;END IF;
  SELECT count(*)FILTER(WHERE NOT(v->>'is_target')::boolean AND(v->>'source_count')::integer=1),
   count(*)FILTER(WHERE(v->>'is_target')::boolean AND(v->>'source_count')::integer=1),
   count(*)FILTER(WHERE(v->>'is_target')::boolean AND(v->>'source_count')::integer>1)
  INTO support_n,target_n,conflict_n FROM jsonb_array_elements(actual)a(v);
  UPDATE population_top3000_202608_202609_v1.daily_shared_series_v1 SET input_sha256=h,input_rows=103,
   support_rows_present=support_n,target_rows_present=target_n,target_conflict_rows=conflict_n,
   normalized_at=coalesce(normalized_at,clock_timestamp()) WHERE run_id=rid AND series_no=s.series_no;
 END LOOP;
 SELECT encode(sha256(convert_to(string_agg(series_no::text||':'||encode(input_sha256,'hex'),E'\n'ORDER BY series_no),'UTF8')),'hex')
 INTO manifest FROM population_top3000_202608_202609_v1.daily_shared_series_v1
 WHERE run_id=rid AND series_no BETWEEN prior_n+1 AND last_n;
 INSERT INTO population_top3000_202608_202609_v1.daily_shared_batch_v1(run_id,stage,first_series,last_series,series_count,row_count,manifest_sha256,input_versions)
 VALUES(rid,'NORMALIZE',prior_n+1,last_n,last_n-prior_n,(last_n-prior_n)*103,manifest,t.input_versions)ON CONFLICT DO NOTHING;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.daily_shared_batch_v1 b WHERE b.run_id=rid AND b.stage='NORMALIZE'
  AND b.first_series=prior_n+1 AND b.last_series=last_n AND b.series_count=last_n-prior_n AND b.row_count=(last_n-prior_n)*103
  AND b.manifest_sha256=manifest AND b.input_versions=t.input_versions)THEN RAISE EXCEPTION 'DAILY_SHARED_BATCH_REPLAY_CONFLICT';END IF;
 IF last_n=r.series_count AND(SELECT count(*)FROM population_top3000_202608_202609_v1.daily_shared_series_v1
   WHERE run_id=rid AND normalized_at IS NOT NULL AND input_rows=103)<>r.series_count
 THEN RAISE EXCEPTION 'DAILY_SHARED_ALL_INPUT_SERIES_REQUIRED';END IF;
 UPDATE population_top3000_202608_202609_v1.daily_shared_run_v1 SET normalized_series=last_n,
  normalized_at=CASE WHEN last_n=series_count THEN coalesce(normalized_at,clock_timestamp())ELSE NULL END WHERE run_id=rid;
 part_evidence:=jsonb_build_object('basis','FROZEN_CENSUS_SOURCE_TICKER_GRID_NOT_COMPANY_DENOMINATOR','run_id',rid,
  'source_series',r.series_count,'target_sessions',42,'support_sessions',61,'source_candidate_grid_units',r.series_count*42,
  'company_membership_verified',false,'regular_session_basis_verified',false,'split_mechanics_independently_verified',false,
  'historical_receipt_observed',false,'missing_source_row_is_zero',false,'required_final_action','QUALIFY_DATED_SECURITY_AND_INPUT_SEMANTICS_THEN_RECONCILE_FROZEN_COMPANY_COHORT');
 INSERT INTO population_top3000_202608_202609_v1.pipeline_partition(family_id,partition_key,time_role,denominator_state,
  denominator_evidence,producer_revision,immutable_input_versions,publication_eligible)
 SELECT f,r.run_key,'TARGET','PROVISIONAL',part_evidence,'retained-shared-daily-v1',t.input_versions,false
 FROM unnest(ARRAY['D03','D04','D05'])f ON CONFLICT(family_id,partition_key)DO NOTHING;
 IF(SELECT count(*)FROM population_top3000_202608_202609_v1.pipeline_partition p WHERE p.family_id IN('D03','D04','D05')
  AND p.partition_key=r.run_key AND p.time_role='TARGET' AND p.denominator_state='PROVISIONAL'
  AND p.denominator_evidence=part_evidence AND p.immutable_input_versions=t.input_versions
  AND p.producer_revision='retained-shared-daily-v1' AND NOT p.publication_eligible AND p.expected_units IS NULL)<>3
 THEN RAISE EXCEPTION 'DAILY_SHARED_PARTITION_REPLAY_CONFLICT';END IF;
 PERFORM population_top3000_202608_202609_v1.d02_daily_resource_guard_v2();
 RETURN jsonb_build_object('state',CASE WHEN last_n=r.series_count THEN 'COMPLETE'ELSE 'CONTINUE'END,
  'checkpoint',jsonb_build_object('last_series',last_n),'commit_receipt',jsonb_build_object('commit_key','daily-shared-input:'||rid||':'||prior_n||':'||last_n,
   'output_sha256',manifest,'new_committed_units',new_rows,'immutable_output_ids',jsonb_build_array(rid,prior_n+1,last_n),
   'input_versions',t.input_versions,'source_series',last_n-prior_n,'source_grid_rows',(last_n-prior_n)*103,'company_population_verified',false));
END $f$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.daily_shared_source_manifest_v1()
RETURNS jsonb LANGUAGE plpgsql STABLE SET search_path TO 'pg_catalog' SET timezone TO 'UTC' SET bytea_output TO 'hex' AS $f$
DECLARE days jsonb;batches jsonb;split_pages jsonb;split_rows jsonb;definitions jsonb;n integer;cal jsonb;
BEGIN
 cal:=population_top3000_202608_202609_v1.daily_shared_calendar_v1();
 SELECT count(*),jsonb_agg(jsonb_build_object('source_day_id',d.source_day_id,'session_date',d.session_date,
  'source_id',d.source_id,'source_sha256',d.source_sha256,'response_bytes',d.response_bytes,
  'result_count',d.result_count,'source_verified_at',d.source_verified_at,
  'source_summary_sha256',encode(sha256(convert_to(d.source_summary::text,'UTF8')),'hex'),
  'input_versions',d.input_versions,'xmin',d.xmin::text)ORDER BY d.session_date)
 INTO n,days FROM population_top3000_202608_202609_v1.d02_daily_source_v2 d
 WHERE d.session_date BETWEEN date '2026-08-03' AND date '2026-09-30'
  AND d.normalized_at IS NOT NULL AND d.source_verified_at IS NOT NULL
  AND d.normalized_source_records=d.result_count AND d.verified_source_records=d.result_count
  AND d.source_summary->'source_records_reconciled'='true'::jsonb
  AND d.calendar_manifest_sha256=cal->>'target_calendar_manifest_sha256';
 IF n<>42 THEN RAISE EXCEPTION 'DAILY_SHARED_ALL42_RECONCILED_D02_SOURCES_REQUIRED';END IF;
 SELECT jsonb_agg(jsonb_build_array(b.source_day_id,b.first_source_row,b.last_source_row,b.source_rows,
  b.typed_manifest_sha256,b.parser_sha256,b.normalizer_sha256,b.verifier_sha256,b.verified_at,b.xmin::text)
  ORDER BY b.source_day_id,b.first_source_row) INTO batches
 FROM population_top3000_202608_202609_v1.d02_daily_batch_v2 b;
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.d02_daily_batch_v2 b WHERE b.verified_at IS NULL)
 THEN RAISE EXCEPTION 'DAILY_SHARED_D02_BATCH_PROOF_INCOMPLETE';END IF;
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.d02_daily_source_v2 d
  LEFT JOIN LATERAL(SELECT coalesce(sum(b.source_rows),0) rows FROM population_top3000_202608_202609_v1.d02_daily_batch_v2 b WHERE b.source_day_id=d.source_day_id)p ON true
  WHERE d.session_date BETWEEN date '2026-08-03' AND date '2026-09-30' AND p.rows<>d.result_count)
 THEN RAISE EXCEPTION 'DAILY_SHARED_D02_SOURCE_PROOF_COUNTS_CHANGED';END IF;
 SELECT jsonb_agg(jsonb_build_array(p.archive_key,p.page_number,p.source_id,p.source_sha256,p.result_count,
  p.next_url,p.normalized_source_records,p.normalized_at,p.xmin::text)ORDER BY p.archive_key,p.page_number)
 INTO split_pages FROM population_top3000_202608_202609_v1.massive_archive_page_v1 p
 WHERE p.family_id='N03' AND p.partition_key='splits-execution-202608-202609';
 IF split_pages IS NULL OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.massive_archive_page_v1 p
   WHERE p.family_id='N03' AND p.partition_key='splits-execution-202608-202609' AND p.next_url IS NULL)
  OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.massive_archive_page_v1 p
   WHERE p.family_id='N03' AND p.partition_key='splits-execution-202608-202609'
   AND(p.normalized_at IS NULL OR p.normalized_source_records IS DISTINCT FROM p.result_count))
 THEN RAISE EXCEPTION 'DAILY_SHARED_TERMINAL_NORMALIZED_SPLIT_SOURCE_REQUIRED';END IF;
 SELECT jsonb_agg(jsonb_build_array(o.unit_key,o.record_sha256,o.xmin::text)ORDER BY o.unit_key) INTO split_rows
 FROM population_top3000_202608_202609_v1.source_observation o
 WHERE o.release_id='top3000_companies_202608_202609_v1' AND o.family_id='N03' AND o.partition_key='splits-execution-202608-202609';
 SELECT jsonb_object_agg(p.oid::regprocedure::text,encode(sha256(convert_to(pg_get_functiondef(p.oid),'UTF8')),'hex'))
 INTO definitions FROM pg_proc p WHERE p.oid IN(
  'market_factors_20250901_20260731_v1.compute_daily_features_remediated_v1(integer,integer,date,date)'::regprocedure,
  'market_factors_20250901_20260731_v1.compute_daily_features_equity_patch_v3(integer,integer,date,date)'::regprocedure,
  'market_factors_20250901_20260731_v1.rebuild_daily_features_remediated_v1(integer,integer,date,date)'::regprocedure);
 RETURN jsonb_build_object('daily_source_headers',days,
  'daily_batch_proofs_sha256',encode(sha256(convert_to(coalesce(batches,'[]'::jsonb)::text,'UTF8')),'hex'),
  'split_source_pages',split_pages,'split_rows_sha256',encode(sha256(convert_to(coalesce(split_rows,'[]'::jsonb)::text,'UTF8')),'hex'),
  'reused_formula_definitions',definitions,
  'support_serving_definition_sha256',encode(sha256(convert_to(pg_get_viewdef('market_factors_20250901_20260731_v1.security_daily_features_serving_remediated_v1'::regclass,true),'UTF8')),'hex'),
  'split_ledger_definition_sha256',encode(sha256(convert_to(pg_get_viewdef('market_factors_20250901_20260731_v1.executed_split_ledger_remediated_v1'::regclass,true),'UTF8')),'hex'),
  'target_source_scope','ALL_RETAINED_D02_SOURCE_ROWS_MATCHING_FROZEN_CENSUS_TICKERS',
  'company_membership_certified',false,'regular_session_basis_certified',false,
  'original_historical_receipt_observed',false);
END $f$;

-- Returns the 103 dated input rows for one frozen census line. Source metadata
-- and true source absence are hashed, but missing bars are never turned into0.
CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.daily_shared_capture_series_v1(p_run uuid,p_series integer)
RETURNS jsonb LANGUAGE plpgsql STABLE SET search_path TO 'pg_catalog' SET timezone TO 'UTC' SET bytea_output TO 'hex' AS $f$
DECLARE r population_top3000_202608_202609_v1.daily_shared_run_v1%rowtype;
 s population_top3000_202608_202609_v1.daily_shared_series_v1%rowtype;
 key integer;anchor text;result jsonb;
BEGIN
 SELECT * INTO r FROM population_top3000_202608_202609_v1.daily_shared_run_v1 WHERE run_id=p_run;
 SELECT * INTO s FROM population_top3000_202608_202609_v1.daily_shared_series_v1 WHERE run_id=p_run AND series_no=p_series;
 IF r.run_id IS NULL OR s.run_id IS NULL THEN RAISE EXCEPTION 'DAILY_SHARED_SERIES_NOT_REGISTERED';END IF;
 key:=(s.mapping_snapshot->>'instrument_key')::integer;
 anchor:=coalesce(nullif(s.candidate_snapshot->>'share_class_figi',''),nullif(s.candidate_snapshot->>'composite_figi',''));
 WITH cal AS MATERIALIZED(
  SELECT i::integer ord,(v->>'date')::date d,(v->>'is_target')::boolean target,
   lag((v->>'date')::date)OVER(ORDER BY i) prior_d
  FROM jsonb_array_elements(r.calendar_manifest->'sessions')WITH ORDINALITY a(v,i)
 ), support AS MATERIALIZED(
  SELECT f.*,sha256(convert_to(to_jsonb(f)::text,'UTF8')) row_sha
  FROM market_factors_20250901_20260731_v1.security_daily_features_serving_remediated_v1 f
  WHERE f.instrument_key=key AND f.session_date BETWEEN date '2026-05-05' AND date '2026-07-31'
 ), states AS MATERIALIZED(
  SELECT q.*,sha256(convert_to(to_jsonb(q)::text,'UTF8')) row_sha
  FROM reference.equity_security_state_v1 q WHERE q.instrument_key=key
  AND q.valid_from<=date '2026-09-30' AND coalesce(q.valid_to,date '9999-12-31')>date '2026-05-05'
 ), old_actions AS MATERIALIZED(
  SELECT a.execution_date,a.ratio_new_per_old,a.quality_state,a.ratio_known_at,
   sha256(convert_to(to_jsonb(a)::text,'UTF8')) row_sha
  FROM market_factors_20250901_20260731_v1.executed_split_ledger_remediated_v1 a
  WHERE a.instrument_key=key AND a.execution_date BETWEEN date '2026-05-05' AND date '2026-07-31'
 ), target_actions AS MATERIALIZED(
  SELECT o.effective_date,CASE WHEN o.field_state->>'split_from'='VALUE' AND o.field_state->>'split_to'='VALUE'
   THEN(o.values_json->>'split_to')::numeric/nullif((o.values_json->>'split_from')::numeric,0)END factor,
   o.values_json->>'provider_event_id' provider_id,o.record_sha256,o.values_json->>'ticker' ticker,
   o.lineage->>'raw_record_sha256' raw_hash
  FROM population_top3000_202608_202609_v1.source_observation o
  WHERE o.release_id='top3000_companies_202608_202609_v1' AND o.family_id='N03'
  AND o.partition_key='splits-execution-202608-202609' AND o.values_json->>'ticker'=s.source_ticker
 ), action_versions AS MATERIALIZED(
  SELECT *,count(*)OVER(PARTITION BY provider_id) versions
  FROM target_actions
 ), joined AS MATERIALIZED(
  SELECT c.*,f.raw_close,f.raw_volume,f.provider_dollar_volume,f.quality_flags,f.identity_anchor support_anchor,
   f.row_sha support_sha,st.n state_n,st.identity_anchor state_anchor,st.ticker state_ticker,st.qualified state_qualified,
   st.state_hash,
   ds.source_day_id,ds.source_id,ds.source_sha256,b.n source_n,b.close_value,b.volume_value,b.dollar_value,
   b.source_row,b.rows_hash,b.bad_record,
   a.factor,a.unknown_count,a.action_hash,a.action_count
  FROM cal c
  LEFT JOIN support f ON f.session_date=c.d
  LEFT JOIN LATERAL(
   SELECT count(*)n,min(coalesce(nullif(q.share_class_figi,''),nullif(q.composite_figi,'')))identity_anchor,
    min(q.ticker)ticker,bool_and(q.mapping_strength='STRONG'
      AND q.certification_state='CERTIFIED_HISTORICAL_SECURITY_STATE_STRONG_IDENTITY'
      AND q.provenance->'date_scoped'='true'::jsonb)qualified,
    sha256(convert_to(coalesce(string_agg(encode(q.row_sha,'hex'),E'\n'ORDER BY q.state_id),''),'UTF8'))state_hash
   FROM states q WHERE q.valid_from<=c.d AND coalesce(q.valid_to,date '9999-12-31')>c.d
  )st ON true
  LEFT JOIN population_top3000_202608_202609_v1.d02_daily_source_v2 ds ON c.target AND ds.session_date=c.d
  LEFT JOIN LATERAL(
   SELECT count(*)n,min(z.source_row)source_row,
    min(CASE WHEN (z.missing_mask&35)=0 AND(z.invalid_mask&63)=0 AND z.structure_mask=0 THEN z.provider_close END)close_value,
    min(CASE WHEN (z.missing_mask&64)=0 AND(z.invalid_mask&64)=0 AND z.structure_mask=0 THEN z.share_volume END)volume_value,
    min(CASE WHEN(z.missing_mask&320)=0 AND(z.invalid_mask&320)=0 AND z.structure_mask=0
      THEN z.provider_vwap*z.share_volume END)dollar_value,
    bool_or(z.invalid_mask<>0 OR z.structure_mask<>0)bad_record,
    sha256(convert_to(coalesce(string_agg(encode(sha256(convert_to(to_jsonb(z)::text,'UTF8')),'hex'),E'\n'ORDER BY z.source_row),''),'UTF8'))rows_hash
   FROM(SELECT v.* FROM population_top3000_202608_202609_v1.d02_daily_bar_v2 v
    WHERE v.source_ticker=s.source_ticker AND v.source_day_id=ds.source_day_id ORDER BY v.source_row LIMIT 2)z
  )b ON c.target
  LEFT JOIN LATERAL(
   SELECT CASE WHEN coalesce(sum(w.log_factor),0) BETWEEN -40 AND 40 THEN exp(coalesce(sum(w.log_factor),0))::numeric ELSE 1::numeric END factor,
    (count(*)FILTER(WHERE NOT w.valid_ratio)+CASE WHEN abs(coalesce(sum(w.log_factor),0))>40 THEN 1 ELSE 0 END)::integer unknown_count,
    count(*)::integer action_count,sha256(convert_to(coalesce(string_agg(w.row_hash,E'\n'ORDER BY w.row_hash),''),'UTF8'))action_hash
   FROM(
    SELECT CASE WHEN a.ratio_new_per_old>1e-20 AND a.ratio_new_per_old<1e20 THEN ln(a.ratio_new_per_old::float8)ELSE 0 END log_factor,
     (a.ratio_new_per_old>1e-20 AND a.ratio_new_per_old<1e20 AND a.quality_state='PRIMARY_SHARE_REBASING_SEMANTICS_VERIFIED')IS TRUE valid_ratio,
     encode(a.row_sha,'hex')row_hash
    FROM old_actions a WHERE NOT c.target AND a.execution_date>coalesce(c.prior_d,c.d-1)AND a.execution_date<=c.d
    UNION ALL
    SELECT CASE WHEN a.factor>1e-20 AND a.factor<1e20 AND a.versions=1 AND a.provider_id IS NOT NULL THEN ln(a.factor::float8)ELSE 0 END,
     (a.factor>1e-20 AND a.factor<1e20 AND a.versions=1 AND a.provider_id IS NOT NULL)IS TRUE,a.record_sha256
    FROM action_versions a WHERE c.target AND a.effective_date>coalesce(c.prior_d,c.d-1)AND a.effective_date<=c.d
   )w
  )a ON true
 ), input_values AS MATERIALIZED(
  SELECT ord,d,target,
   CASE WHEN target THEN least(coalesce(source_n,0),2)::integer
    WHEN support_sha IS NOT NULL AND anchor IS NOT NULL AND support_anchor=anchor
     AND state_n=1 AND state_qualified AND state_anchor=anchor AND state_ticker=s.source_ticker
    THEN 1 ELSE 0 END source_count,
   CASE WHEN target THEN close_value WHEN raw_close>0 AND raw_close<'Infinity'::float8 THEN raw_close::numeric END close_value,
   CASE WHEN target THEN volume_value WHEN raw_volume>=0 THEN raw_volume::numeric END volume_value,
   CASE WHEN target THEN dollar_value WHEN provider_dollar_volume>0 AND provider_dollar_volume<'Infinity'::float8 THEN provider_dollar_volume::numeric END dollar_value,
   factor,unknown_count,
   32|CASE WHEN target THEN 4 ELSE 0 END
    |CASE WHEN state_n=1 AND state_qualified AND state_anchor=anchor AND state_ticker=s.source_ticker THEN 0 ELSE 8 END
    |CASE WHEN target AND action_count>0 THEN 16 ELSE 0 END
    |CASE WHEN coalesce(bad_record,false)OR coalesce(source_n,0)>1 THEN 8192 ELSE 0 END mask,
   CASE WHEN state_n=1 AND state_qualified AND state_anchor=anchor AND state_ticker=s.source_ticker THEN state_anchor END identity_anchor,
   CASE WHEN target THEN 'MASSIVE_GROUPED_DAILY_SESSION_BASIS_UNVERIFIED' ELSE 'RETAINED_REGULAR_SESSION_AGGREGATE' END basis,
   CASE WHEN target THEN 2 ELSE 1 END source_kind,
   CASE WHEN target THEN source_id||':'||coalesce(source_row::text,'ABSENT')
    WHEN support_sha IS NOT NULL THEN 'market_factors_20250901_20260731_v1.security_daily_features_serving_remediated_v1:'||key::text||':'||d::text
    ELSE 'ABSENT_RETAINED_SUPPORT:'||coalesce(key::text,'UNMAPPED')||':'||d::text END source_key,
   CASE WHEN target THEN sha256(convert_to(jsonb_build_object('source_id',source_id,'source_sha256',source_sha256,'selected_typed_rows_sha256',encode(rows_hash,'hex'),'source_count_capped_at_two',coalesce(source_n,0))::text,'UTF8'))
    ELSE coalesce(support_sha,sha256(convert_to('ABSENT_RETAINED_SUPPORT','UTF8'))) END source_hash,
   state_hash,action_hash
  FROM joined
 ), encoded AS MATERIALIZED(
  SELECT jsonb_build_object('ordinal',ord,'session_date',d,'is_target',target,'source_count',source_count,
   'raw_close',CASE WHEN source_count=1 THEN close_value END,
   'raw_volume',CASE WHEN source_count=1 THEN volume_value END,
   'dollar_volume',CASE WHEN source_count=1 THEN dollar_value END,
   'split_factor',factor,'unknown_actions',unknown_count,'quality_mask',mask,
   'identity_anchor',identity_anchor,'price_basis',basis,'source_kind',source_kind,'source_key',source_key,
   'source_record_sha256',encode(source_hash,'hex'),'identity_record_sha256',encode(state_hash,'hex'),
   'action_record_sha256',encode(action_hash,'hex')) val,ord
  FROM input_values
 )
 SELECT jsonb_agg(val||jsonb_build_object('input_record_sha256',encode(sha256(convert_to(val::text,'UTF8')),'hex'))ORDER BY ord)
 INTO result FROM encoded;
 IF jsonb_array_length(result)IS DISTINCT FROM 103 THEN RAISE EXCEPTION 'DAILY_SHARED_CAPTURE_GRID_CARDINALITY';END IF;
 RETURN result;
END $f$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.daily_shared_output_json_v1(p_run uuid,p_series integer)
RETURNS jsonb LANGUAGE sql STABLE SET search_path TO 'pg_catalog' AS $f$
 SELECT jsonb_agg((to_jsonb(v)-'run_id'-'series_no'-'input_series_sha256'-'calculator_sha256'-'record_sha256')
  ||jsonb_build_object('input_series_sha256',encode(v.input_series_sha256,'hex'),
   'calculator_sha256',encode(v.calculator_sha256,'hex'),'record_sha256',encode(v.record_sha256,'hex'))ORDER BY v.session_date)
 FROM population_top3000_202608_202609_v1.daily_shared_value_v1 v WHERE v.run_id=p_run AND v.series_no=p_series;
$f$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.daily_shared_derive_v1(p_attempt uuid,p_token text)
RETURNS jsonb LANGUAGE plpgsql SET search_path TO 'pg_catalog' AS $f$
DECLARE tid uuid;rid uuid;t population_top3000_202608_202609_v1.pipeline_task%rowtype;
 r population_top3000_202608_202609_v1.daily_shared_run_v1%rowtype;
 s population_top3000_202608_202609_v1.daily_shared_series_v1%rowtype;
 prior_n integer;last_n integer;inputs jsonb;expected jsonb;actual jsonb;h bytea;math_hash text;manifest text;
 new_rows bigint:=0;inserted_n bigint;family text;child uuid;
BEGIN
 tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);
 rid:=population_top3000_202608_202609_v1.daily_shared_context_v1(tid);
 SELECT * INTO t FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=tid;
 IF t.stage<>'DERIVE' THEN RAISE EXCEPTION 'DAILY_SHARED_STAGE_MISMATCH';END IF;
 PERFORM population_top3000_202608_202609_v1.d02_daily_resource_guard_v2();
 SELECT * INTO r FROM population_top3000_202608_202609_v1.daily_shared_run_v1 WHERE run_id=rid FOR UPDATE;
 IF r.normalized_at IS NULL OR r.normalized_series<>r.series_count THEN RAISE EXCEPTION 'DAILY_SHARED_ALL_INPUTS_REQUIRED';END IF;
 IF t.family_id IN('D04','D05') THEN
  IF r.derived_at IS NULL OR r.derived_series<>r.series_count THEN RAISE EXCEPTION 'DAILY_SHARED_D03_DERIVE_REQUIRED';END IF;
  SELECT encode(sha256(convert_to(string_agg(series_no::text||':'||encode(output_sha256,'hex'),E'\n'ORDER BY series_no),'UTF8')),'hex')
  INTO manifest FROM population_top3000_202608_202609_v1.daily_shared_series_v1 WHERE run_id=rid;
  RETURN jsonb_build_object('state','COMPLETE','commit_receipt',jsonb_build_object('commit_key','daily-shared-family:'||rid||':'||t.family_id,
   'output_sha256',manifest,'new_committed_units',0,'immutable_output_ids',jsonb_build_array(rid),
   'input_versions',t.input_versions,'shared_rows_reused',r.series_count*42,'duplicated_values_written',false,
   'current_full_output_independent_audit_pending',true,'company_population_verified',false));
 END IF;
 IF coalesce(t.checkpoint->>'last_series','0')!~'^[0-9]{1,5}$' THEN RAISE EXCEPTION 'DAILY_SHARED_CHECKPOINT_INVALID';END IF;
 prior_n:=coalesce((t.checkpoint->>'last_series')::integer,0);last_n:=least(prior_n+r.batch_size,r.series_count);
 IF prior_n<0 OR prior_n>=r.series_count OR prior_n%r.batch_size<>0 OR r.derived_series NOT IN(prior_n,last_n)
 THEN RAISE EXCEPTION 'DAILY_SHARED_DERIVE_CHECKPOINT_DIVERGED';END IF;
 math_hash:=encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.daily_shared_math_v1(jsonb)'::regprocedure),'UTF8')),'hex');
 FOR s IN SELECT * FROM population_top3000_202608_202609_v1.daily_shared_series_v1
  WHERE run_id=rid AND series_no BETWEEN prior_n+1 AND last_n ORDER BY series_no LOOP
  inputs:=population_top3000_202608_202609_v1.daily_shared_input_json_v1(rid,s.series_no);
  IF s.normalized_at IS NULL OR s.input_rows<>103 OR jsonb_array_length(inputs)IS DISTINCT FROM 103
   OR s.input_sha256 IS DISTINCT FROM sha256(convert_to(inputs::text,'UTF8'))
   OR EXISTS(SELECT 1 FROM jsonb_array_elements(inputs)a(v)
     WHERE v->>'input_record_sha256' IS DISTINCT FROM encode(sha256(convert_to((v-'input_record_sha256')::text,'UTF8')),'hex'))
  THEN RAISE EXCEPTION 'DAILY_SHARED_FROZEN_INPUT_INTEGRITY series=%',s.series_no;END IF;
  WITH calculated AS MATERIALIZED(
   SELECT m.session_date,to_jsonb(m)||jsonb_build_object('input_series_sha256',encode(s.input_sha256,'hex'),'calculator_sha256',math_hash)v
   FROM population_top3000_202608_202609_v1.daily_shared_math_v1(inputs)m
  )SELECT jsonb_agg(v||jsonb_build_object('record_sha256',encode(sha256(convert_to(v::text,'UTF8')),'hex'))ORDER BY session_date)
   INTO expected FROM calculated;
  IF jsonb_array_length(expected)IS DISTINCT FROM 42 THEN RAISE EXCEPTION 'DAILY_SHARED_MATH_OUTPUT_GRID';END IF;
  h:=sha256(convert_to(expected::text,'UTF8'));
  INSERT INTO population_top3000_202608_202609_v1.daily_shared_value_v1
   (run_id,series_no,session_date,raw_returns,adjusted_returns,realized_log_vol_20d,annualized_log_vol_20d,
    annualized_simple_vol_20d,relative_volume_20d,volume_z20,daily_amihud,amihud_mean_20d,input_coverage,quality_mask,
    input_series_sha256,calculator_sha256,record_sha256)
  SELECT rid,s.series_no,v.session_date,v.raw_returns,v.adjusted_returns,v.realized_log_vol_20d,v.annualized_log_vol_20d,
   v.annualized_simple_vol_20d,v.relative_volume_20d,v.volume_z20,v.daily_amihud,v.amihud_mean_20d,v.input_coverage,v.quality_mask,
   decode(v.input_series_sha256,'hex'),decode(v.calculator_sha256,'hex'),decode(v.record_sha256,'hex')
  FROM jsonb_to_recordset(expected)v(session_date date,raw_returns float8[],adjusted_returns float8[],realized_log_vol_20d float8,
   annualized_log_vol_20d float8,annualized_simple_vol_20d float8,relative_volume_20d float8,volume_z20 float8,
   daily_amihud float8,amihud_mean_20d float8,input_coverage integer[],quality_mask integer,
   input_series_sha256 text,calculator_sha256 text,record_sha256 text)
  ON CONFLICT(run_id,series_no,session_date)DO NOTHING;
  GET DIAGNOSTICS inserted_n=ROW_COUNT;new_rows:=new_rows+inserted_n;
  actual:=population_top3000_202608_202609_v1.daily_shared_output_json_v1(rid,s.series_no);
  IF actual IS DISTINCT FROM expected OR(s.output_sha256 IS NOT NULL AND s.output_sha256<>h)
  THEN RAISE EXCEPTION 'DAILY_SHARED_OUTPUT_REPLAY_CONFLICT series=%',s.series_no;END IF;
  UPDATE population_top3000_202608_202609_v1.daily_shared_series_v1 SET output_sha256=h,output_rows=42,
   derived_at=coalesce(derived_at,clock_timestamp())WHERE run_id=rid AND series_no=s.series_no;
 END LOOP;
 SELECT encode(sha256(convert_to(string_agg(series_no::text||':'||encode(output_sha256,'hex'),E'\n'ORDER BY series_no),'UTF8')),'hex')
 INTO manifest FROM population_top3000_202608_202609_v1.daily_shared_series_v1
 WHERE run_id=rid AND series_no BETWEEN prior_n+1 AND last_n;
 INSERT INTO population_top3000_202608_202609_v1.daily_shared_batch_v1(run_id,stage,first_series,last_series,series_count,row_count,manifest_sha256,input_versions)
 VALUES(rid,'DERIVE',prior_n+1,last_n,last_n-prior_n,(last_n-prior_n)*42,manifest,t.input_versions)ON CONFLICT DO NOTHING;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.daily_shared_batch_v1 b WHERE b.run_id=rid AND b.stage='DERIVE'
  AND b.first_series=prior_n+1 AND b.last_series=last_n AND b.series_count=last_n-prior_n AND b.row_count=(last_n-prior_n)*42
  AND b.manifest_sha256=manifest AND b.input_versions=t.input_versions)THEN RAISE EXCEPTION 'DAILY_SHARED_BATCH_REPLAY_CONFLICT';END IF;
 IF last_n=r.series_count AND(SELECT count(*)FROM population_top3000_202608_202609_v1.daily_shared_series_v1
   WHERE run_id=rid AND derived_at IS NOT NULL AND output_rows=42)<>r.series_count
 THEN RAISE EXCEPTION 'DAILY_SHARED_ALL_OUTPUT_SERIES_REQUIRED';END IF;
 UPDATE population_top3000_202608_202609_v1.daily_shared_run_v1 SET derived_series=last_n,
  derived_at=CASE WHEN last_n=series_count THEN coalesce(derived_at,clock_timestamp())ELSE NULL END WHERE run_id=rid;
 IF last_n=r.series_count THEN
  FOREACH family IN ARRAY ARRAY['D04','D05'] LOOP
   child:=population_top3000_202608_202609_v1.pipeline_enqueue_v1(family,'NORMALIZE',t.partition_key,t.payload,t.input_versions,tid);
  END LOOP;
 END IF;
 PERFORM population_top3000_202608_202609_v1.d02_daily_resource_guard_v2();
 RETURN jsonb_build_object('state',CASE WHEN last_n=r.series_count THEN 'COMPLETE'ELSE 'CONTINUE'END,
  'checkpoint',jsonb_build_object('last_series',last_n),'commit_receipt',jsonb_build_object('commit_key','daily-shared-values:'||rid||':'||prior_n||':'||last_n,
   'output_sha256',manifest,'new_committed_units',new_rows,'immutable_output_ids',jsonb_build_array(rid,prior_n+1,last_n),
   'input_versions',t.input_versions,'source_series',last_n-prior_n,'calculated_target_rows',(last_n-prior_n)*42,
   'current_full_output_independent_audit_pending',true,'company_population_verified',false));
END $f$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.daily_shared_quality_hold_v1(p_attempt uuid,p_token text)
RETURNS jsonb LANGUAGE plpgsql SET search_path TO 'pg_catalog' AS $f$
DECLARE tid uuid;rid uuid;t population_top3000_202608_202609_v1.pipeline_task%rowtype;
 r population_top3000_202608_202609_v1.daily_shared_run_v1%rowtype;
BEGIN
 tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);
 rid:=population_top3000_202608_202609_v1.daily_shared_context_v1(tid);
 SELECT * INTO t FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=tid;
 SELECT * INTO r FROM population_top3000_202608_202609_v1.daily_shared_run_v1 WHERE run_id=rid;
 IF t.stage NOT IN('VALIDATE','REPAIR')THEN RAISE EXCEPTION 'DAILY_SHARED_STAGE_MISMATCH';END IF;
 RETURN jsonb_build_object('state','BLOCKED','blocker_code','DAILY_SHARED_SECURITY_SESSION_ACTION_AND_COMPANY_QUALIFICATION_REQUIRED',
  'resumption_condition',jsonb_build_object('event','QUALIFIED_INPUT_GENERATION_AND_FROZEN_COMPANY_RECONCILIATION',
   'run_id',rid,'normalized_series',r.normalized_series,'calculated_series',r.derived_series,'source_series',r.series_count,
   'full_source_calculation_finished',r.derived_at IS NOT NULL,
   'needs',jsonb_build_array('EXACT_DATED_SECURITY_CONTINUITY_INCLUDING_SEPTEMBER','REGULAR_SESSION_CLOSE_VOLUME_DOLLAR_VOLUME_QUALIFICATION',
    'SPLIT_MECHANICS_AND_KNOWLEDGE_TIME_QUALIFICATION','EXACT3000_COMPANY_AND_SECURITY_DAY_DENOMINATOR',
    'INDEPENDENT_FULL_CURRENT_INPUT_OUTPUT_RECONCILIATION_AND_OUTLIER_CHECKS','EXPLICIT_ARCHIVED_VERSUS_ORIGINAL_RECEIPT_CONSUMER_POLICY'),
   'source_candidate_values_preserved',true,'source_archive_publication_is_separate',true,'company_population_verified',false,
   'provider_accuracy_verified',false,'historical_receipt_observed',false,'quality_promoted',false,'automatic_retry',false));
END $f$;

CREATE OR REPLACE VIEW population_top3000_202608_202609_v1.daily_shared_candidates_v1 WITH(security_invoker=true)AS
SELECT r.run_key,s.source_ticker,s.series_no,v.session_date,
 v.raw_returns[1]raw_ret_1d_candidate,v.raw_returns[2]raw_ret_5d_candidate,
 v.raw_returns[3]raw_ret_20d_candidate,v.raw_returns[4]raw_ret_60d_candidate,
 v.adjusted_returns[1]adjusted_ret_1d_candidate,v.adjusted_returns[2]adjusted_ret_5d_candidate,
 v.adjusted_returns[3]adjusted_ret_20d_candidate,v.adjusted_returns[4]adjusted_ret_60d_candidate,
 v.realized_log_vol_20d,v.annualized_log_vol_20d,v.annualized_simple_vol_20d,
 v.relative_volume_20d,v.volume_z20,v.daily_amihud,v.amihud_mean_20d,v.input_coverage,v.quality_mask,
 encode(v.input_series_sha256,'hex')input_series_sha256,encode(v.calculator_sha256,'hex')calculator_sha256,
 encode(v.record_sha256,'hex')record_sha256,
 'PROVISIONAL_RETAINED_SOURCE_SERIES_ARITHMETIC'::text quality_state,
 'log volatility: unannualized sample SD; Amihud: inverse USD'::text units,
 false company_population_verified,false regular_session_basis_verified,false historical_receipt_observed,
 false publication_eligible,false production_asof_eligible
FROM population_top3000_202608_202609_v1.daily_shared_value_v1 v
JOIN population_top3000_202608_202609_v1.daily_shared_series_v1 s USING(run_id,series_no)
JOIN population_top3000_202608_202609_v1.daily_shared_run_v1 r USING(run_id);
REVOKE ALL ON population_top3000_202608_202609_v1.daily_shared_candidates_v1 FROM PUBLIC,anon,authenticated;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.daily_shared_prepare_run_v1(p_run_key text,p_fixture_count integer DEFAULT NULL)
RETURNS jsonb LANGUAGE plpgsql SET search_path TO 'pg_catalog' SET timezone TO 'UTC' SET bytea_output TO 'hex' AS $f$
DECLARE rid uuid:=gen_random_uuid();cal jsonb;src jsonb;vers jsonb;census_hash text;census_n integer;captured_n integer;
BEGIN
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton
  AND owner_id='top3000-recovery-20261023-root-4b1314bcb5c2' AND owner_fence=2 AND stop_generation=0
  AND enabled AND NOT stopped AND hold_reason IS NULL AND admitted_until>clock_timestamp())
 THEN RAISE EXCEPTION 'DAILY_SHARED_OWNER_STOP_OR_ADMISSION_CHANGED';END IF;
 IF EXISTS(SELECT 1 FROM cron.job WHERE jobid=596 AND active)
  OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE status='LEASED')
 THEN RAISE EXCEPTION 'DAILY_SHARED_PREPARE_REQUIRES_PAUSED_UNLEASED_QUEUE';END IF;
 IF(p_fixture_count IS NULL AND p_run_key<>'retained-shared-daily-v1')
  OR(p_fixture_count IS NOT NULL AND(p_fixture_count NOT BETWEEN 1 AND 50 OR p_run_key!~'^fixture-shared-daily-v1-[0-9a-f-]{36}$'))
  OR p_run_key IS NULL THEN RAISE EXCEPTION 'DAILY_SHARED_RUN_SCOPE_INVALID';END IF;
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.daily_shared_run_v1 WHERE run_key=p_run_key)
 THEN RAISE EXCEPTION 'DAILY_SHARED_EXISTING_RUN_REQUIRES_RECONCILIATION';END IF;
 PERFORM population_top3000_202608_202609_v1.d02_daily_resource_guard_v2();
 cal:=population_top3000_202608_202609_v1.daily_shared_calendar_v1();
 src:=population_top3000_202608_202609_v1.daily_shared_source_manifest_v1();
 WITH selected AS MATERIALIZED(
  SELECT c.* FROM population_top3000_202608_202609_v1.company_candidate_v1 c
  ORDER BY CASE WHEN c.ticker IN('AAPL','MSFT','NVDA')THEN 0 ELSE 1 END,c.ticker
  LIMIT coalesce(p_fixture_count,20001)
 )SELECT count(*),encode(sha256(convert_to(string_agg(encode(sha256(convert_to(to_jsonb(c)::text,'UTF8')),'hex'),E'\n'
  ORDER BY CASE WHEN c.ticker IN('AAPL','MSFT','NVDA')THEN 0 ELSE 1 END,c.ticker),'UTF8')),'hex')INTO census_n,census_hash FROM selected c;
 IF census_n NOT BETWEEN 1 AND 20000 OR(p_fixture_count IS NOT NULL AND census_n<>p_fixture_count)
  OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.company_candidate_v1 WHERE length(ticker)NOT BETWEEN 1 AND 64)
 THEN RAISE EXCEPTION 'DAILY_SHARED_CENSUS_BOUND_OR_TICKER_INVALID';END IF;
 vers:=jsonb_build_object('scope_sha256','8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd',
  'shared_daily_revision','retained-shared-daily-v1','shared_daily_bundle_sha256',population_top3000_202608_202609_v1.daily_shared_bundle_sha256_v1(),
  'calendar_manifest_sha256',cal->>'calendar_manifest_sha256','source_manifest_sha256',encode(sha256(convert_to(src::text,'UTF8')),'hex'),
  'census_sha256',census_hash,'source_series_count',census_n,'batch_size',25,
  'D03_contract_sha256','720d1d6243eed29518e9cc4be2e1fd8c1f1ec45cc4c45400c5567320d3ecab4b',
  'D04_contract_sha256','230f350109b9b137b8dd9c722c7cefefac49b91eed733b5c2b4aa96ec43b97a8',
  'D05_contract_sha256','708d472f20dc1fa936737cf792676855c2eda377c89636fe26244c077bc10e91',
  'fixture_only',p_fixture_count IS NOT NULL);
 INSERT INTO population_top3000_202608_202609_v1.daily_shared_run_v1(run_id,run_key,input_versions,source_manifest,calendar_manifest,census_sha256,series_count,batch_size)
 VALUES(rid,p_run_key,vers,src,cal,census_hash,census_n,25);
 WITH selected AS MATERIALIZED(
  SELECT c.*,row_number()OVER(ORDER BY CASE WHEN c.ticker IN('AAPL','MSFT','NVDA')THEN 0 ELSE 1 END,c.ticker)::integer n
  FROM population_top3000_202608_202609_v1.company_candidate_v1 c
  ORDER BY CASE WHEN c.ticker IN('AAPL','MSFT','NVDA')THEN 0 ELSE 1 END,c.ticker LIMIT census_n
 ), mapped AS MATERIALIZED(
  SELECT m.symbol,count(*)n,min(m.instrument_key)instrument_key,
   encode(sha256(convert_to(string_agg(encode(sha256(convert_to(to_jsonb(m)::text,'UTF8')),'hex'),E'\n'ORDER BY m.instrument_key),'UTF8')),'hex')rows_sha256
  FROM reference.alpaca_fixed_window_instruments_remediated_v1 m JOIN selected c ON c.ticker=m.symbol
  GROUP BY m.symbol
 )
 INSERT INTO population_top3000_202608_202609_v1.daily_shared_series_v1(run_id,series_no,source_ticker,candidate_snapshot,mapping_snapshot)
 SELECT rid,c.n,c.ticker,to_jsonb(c)-'n',jsonb_build_object('source_relation','reference.alpaca_fixed_window_instruments_remediated_v1',
  'source_rows',coalesce(m.n,0),'instrument_key',CASE WHEN m.n=1 THEN m.instrument_key END,
  'source_rows_sha256',m.rows_sha256,'qualification','SYMBOL_JOIN_CANDIDATE_REQUIRES_PER_SESSION_STRONG_DATED_FIGI')
 FROM selected c LEFT JOIN mapped m ON m.symbol=c.ticker;
 GET DIAGNOSTICS captured_n=ROW_COUNT;
 IF captured_n<>census_n THEN RAISE EXCEPTION 'DAILY_SHARED_CENSUS_SNAPSHOT_COUNT';END IF;
 PERFORM population_top3000_202608_202609_v1.d02_daily_resource_guard_v2();
 RETURN jsonb_build_object('run_id',rid,'run_key',p_run_key,'input_versions',vers,'source_series',census_n,
  'maximum_input_grid_rows',census_n*103,'maximum_target_rows',census_n*42,
  'useful_tick_upper_bound',2*ceil(census_n::numeric/25)::integer+10,
  'provider_requests',0,'company_population_verified',false,'registered_tasks',0);
END $f$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.daily_shared_bundle_sha256_v1()
RETURNS text LANGUAGE sql STABLE SET search_path TO 'pg_catalog' AS $f$
 SELECT encode(sha256(convert_to(string_agg(p.oid::regprocedure::text||':'||
  encode(sha256(convert_to(pg_get_functiondef(p.oid),'UTF8')),'hex'),E'\n'ORDER BY p.oid::regprocedure::text),'UTF8')),'hex')
 FROM pg_proc p WHERE p.oid IN(
  'population_top3000_202608_202609_v1.daily_shared_math_v1(jsonb)'::regprocedure,
  'population_top3000_202608_202609_v1.daily_shared_calendar_v1()'::regprocedure,
  'population_top3000_202608_202609_v1.daily_shared_source_manifest_v1()'::regprocedure,
  'population_top3000_202608_202609_v1.daily_shared_capture_series_v1(uuid,integer)'::regprocedure,
  'population_top3000_202608_202609_v1.daily_shared_input_json_v1(uuid,integer)'::regprocedure,
  'population_top3000_202608_202609_v1.daily_shared_output_json_v1(uuid,integer)'::regprocedure,
  'population_top3000_202608_202609_v1.daily_shared_context_v1(uuid)'::regprocedure,
  'population_top3000_202608_202609_v1.daily_shared_normalize_v1(uuid,text)'::regprocedure,
  'population_top3000_202608_202609_v1.daily_shared_derive_v1(uuid,text)'::regprocedure,
  'population_top3000_202608_202609_v1.daily_shared_quality_hold_v1(uuid,text)'::regprocedure,
  'population_top3000_202608_202609_v1.daily_shared_prepare_run_v1(text,integer)'::regprocedure,
  'population_top3000_202608_202609_v1.d02_daily_calendar_v2()'::regprocedure,
  'population_top3000_202608_202609_v1.d02_daily_resource_guard_v2()'::regprocedure);
$f$;
DO $permissions$
DECLARE p record;
BEGIN
 FOR p IN SELECT oid FROM pg_proc WHERE pronamespace='population_top3000_202608_202609_v1'::regnamespace
  AND proname IN('daily_shared_math_v1','daily_shared_calendar_v1','daily_shared_source_manifest_v1','daily_shared_capture_series_v1',
    'daily_shared_input_json_v1','daily_shared_output_json_v1','daily_shared_context_v1','daily_shared_normalize_v1',
    'daily_shared_derive_v1','daily_shared_quality_hold_v1','daily_shared_prepare_run_v1','daily_shared_bundle_sha256_v1')LOOP
  EXECUTE format('REVOKE ALL ON FUNCTION %s FROM PUBLIC,anon,authenticated',p.oid::regprocedure);
 END LOOP;
END $permissions$;

COMMIT;
