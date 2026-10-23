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
