CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.daily_shared_derive_admitted_v2(p_attempt uuid,p_token text)
RETURNS jsonb LANGUAGE plpgsql SET search_path TO 'pg_catalog' AS $function$
DECLARE tid uuid;rid uuid;t population_top3000_202608_202609_v1.pipeline_task%rowtype;
 r population_top3000_202608_202609_v1.daily_shared_run_v1%rowtype;
 bound_n integer;before_bytes bigint;after_bytes bigint;started timestamptz;result jsonb;measurement jsonb;
BEGIN
 tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);
 rid:=population_top3000_202608_202609_v1.daily_shared_context_v1(tid);
 SELECT * INTO t FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=tid;
 SELECT * INTO r FROM population_top3000_202608_202609_v1.daily_shared_run_v1 WHERE run_id=rid;
 IF t.family_id<>'D03' OR t.stage<>'DERIVE' THEN RAISE EXCEPTION 'DAILY_DERIVE_ADMISSION_STAGE_MISMATCH';END IF;
 IF encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.daily_shared_derive_v1(uuid,text)'::regprocedure),'UTF8')),'hex')
  IS DISTINCT FROM 'ac51175b5801451a9cdf7efab4cc8c3f36583fe8bc81a50e8100878b969bc593' THEN RAISE EXCEPTION 'DAILY_DERIVE_UNDERLYING_CODE_CHANGED';END IF;
 IF coalesce(t.payload->>'max_derived_series','25')!~'^[0-9]{1,5}$' THEN RAISE EXCEPTION 'DAILY_DERIVE_ADMISSION_INVALID';END IF;
 bound_n:=coalesce((t.payload->>'max_derived_series')::integer,25);
 IF bound_n<1 OR bound_n>r.series_count OR(bound_n<>r.series_count AND bound_n%r.batch_size<>0) THEN RAISE EXCEPTION 'DAILY_DERIVE_ADMISSION_INVALID';END IF;
 IF (r.derived_series>=bound_n OR least(r.derived_series+r.batch_size,r.series_count)>bound_n) AND r.derived_series<r.series_count THEN
  RETURN jsonb_build_object('state','BLOCKED','blocker_code','DAILY_DERIVE_MEASURED_RESOURCE_REVIEW',
   'resumption_condition',jsonb_build_object('run_id',rid,'derived_series',r.derived_series,'admitted_derived_series',bound_n,
    'series_count_unchanged',r.series_count,'required_action','Review actual value-relation growth and duration, then admit finite remaining derive series under current owner and monetary authority; preserve original denominator and counters.',
    'current_value_relation_bytes',pg_total_relation_size('population_top3000_202608_202609_v1.daily_shared_value_v1'::regclass)));
 END IF;
 before_bytes:=pg_total_relation_size('population_top3000_202608_202609_v1.daily_shared_value_v1'::regclass);started:=clock_timestamp();
 result:=population_top3000_202608_202609_v1.daily_shared_derive_v1(p_attempt,p_token);
 after_bytes:=pg_total_relation_size('population_top3000_202608_202609_v1.daily_shared_value_v1'::regclass);
 measurement:=jsonb_build_object(
  'value_relation_bytes_before',before_bytes,'value_relation_bytes_after',after_bytes,'allocation_growth_bytes',after_bytes-before_bytes,
  'elapsed_seconds',extract(epoch from clock_timestamp()-started),'admitted_derived_series',bound_n,'full_series_denominator',r.series_count);
 IF jsonb_typeof(result) IS DISTINCT FROM 'object' THEN RAISE EXCEPTION 'DAILY_DERIVE_INVALID_UNDERLYING_RESPONSE';END IF;
 IF jsonb_typeof(result->'commit_receipt') IS DISTINCT FROM 'object' THEN RETURN result||jsonb_build_object('runtime_measurement',measurement);END IF;
 RETURN jsonb_set(result,'{commit_receipt}',(result->'commit_receipt')||jsonb_build_object('runtime_measurement',measurement));
END $function$;
REVOKE ALL ON FUNCTION population_top3000_202608_202609_v1.daily_shared_derive_admitted_v2(uuid,text) FROM PUBLIC,anon,authenticated;
