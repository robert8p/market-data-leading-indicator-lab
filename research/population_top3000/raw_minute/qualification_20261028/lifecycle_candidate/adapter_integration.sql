-- Existing ACQUIRE adapter only; all non-RAW tasks retain the exact legacy route.
CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.massive_minute_route_acquire_v2(p_attempt uuid,p_token text) RETURNS jsonb
LANGUAGE plpgsql SECURITY INVOKER SET search_path='pg_catalog' AS $fn$
DECLARE tid uuid;t population_top3000_202608_202609_v1.pipeline_task%ROWTYPE;c population_top3000_202608_202609_v1.acquisition_control%ROWTYPE;context jsonb;rid bigint;
BEGIN
tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);
SELECT * INTO STRICT t FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=tid;
IF t.payload->>'kind' IS DISTINCT FROM 'RAW_MINUTE_SOURCE_V3' THEN
 IF encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.massive_minute_route_v2(uuid,text,text)'::regprocedure),'UTF8')),'hex') IS DISTINCT FROM '4e134e18a4ca56f8ac8c1763bd9fc18a9c9b8fc524336e85a69828a53bcd7357' THEN RAISE EXCEPTION 'RAW_PRESERVED_ROUTE_CHANGED';END IF;
 RETURN population_top3000_202608_202609_v1.massive_minute_route_v2(p_attempt,p_token,'ACQUIRE');
END IF;
IF encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.raw_minute_dispatch_v3(uuid,text)'::regprocedure),'UTF8')),'hex') IS DISTINCT FROM '153d3b0cdccd1fcc477f43a5a04ad08205dc9e7ded27c08a0e66a0f74341ea1a' OR encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.raw_minute_request_context_v3(uuid,text)'::regprocedure),'UTF8')),'hex') IS DISTINCT FROM '52c0c13c3f0842e030edb1d73e7f6c0ba536e3975e3ba4e6db86d1bd7fcebe34' THEN RAISE EXCEPTION 'RAW_ADAPTER_CODE_CHANGED';END IF;
context:=population_top3000_202608_202609_v1.raw_minute_request_context_v3(p_attempt,p_token);
IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_attempt WHERE attempt_id=p_attempt AND request_id IS NOT NULL) THEN RAISE EXCEPTION 'RAW_HTTP_DISPATCH_RECONCILIATION_REQUIRED';END IF;
SELECT * INTO STRICT c FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton;
IF c.http_invocations>=c.max_http_invocations THEN RETURN jsonb_build_object('state','BLOCKED','blocker_code','HTTP_INVOCATION_CAP','resumption_condition',jsonb_build_object('required_action','Review current finite capacity; no automatic cap increase'));END IF;
SELECT net.http_post(url:='https://oxzabweahkoimtevbbny.supabase.co/functions/v1/population-top3000-raw-minute-v3',headers:='{"Content-Type":"application/json"}',body:=jsonb_build_object('attempt_id',p_attempt,'token',p_token),timeout_milliseconds:=55000) INTO rid;
UPDATE population_top3000_202608_202609_v1.acquisition_control SET http_invocations=http_invocations+1 WHERE singleton;
UPDATE population_top3000_202608_202609_v1.pipeline_attempt SET request_id=rid WHERE attempt_id=p_attempt;
RETURN jsonb_build_object('state','ASYNC','request_id',rid,'worker','population-top3000-raw-minute-v3','raw_only',true);
END $fn$;
REVOKE ALL ON FUNCTION population_top3000_202608_202609_v1.massive_minute_route_acquire_v2(uuid,text) FROM PUBLIC,anon,authenticated;
