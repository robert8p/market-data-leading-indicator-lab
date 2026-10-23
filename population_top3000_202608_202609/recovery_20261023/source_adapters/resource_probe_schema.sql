-- DRAFT: private control plane for one mission-scoped filesystem metric probe.
-- Definitions only. No request is queued and no budget or capacity is changed here.
CREATE TABLE IF NOT EXISTS population_top3000_202608_202609_v1.resource_probe_v1 (
 probe_id uuid PRIMARY KEY,
 probe_kind text NOT NULL UNIQUE CHECK(probe_kind='FILESYSTEM_METRICS'),
 owner_id text NOT NULL,
 owner_fence bigint NOT NULL,
 stop_generation bigint NOT NULL,
 scope_sha256 text NOT NULL CHECK(scope_sha256='8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd'),
 token_sha256 text NOT NULL CHECK(token_sha256~'^[0-9a-f]{64}$'),
 state text NOT NULL DEFAULT 'PENDING' CHECK(state IN('PENDING','RUNNING','SUCCEEDED','FAILED','EXPIRED')),
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 expires_at timestamptz NOT NULL,
 claimed_at timestamptz,
 claim_deadline_at timestamptz,
 finished_at timestamptz,
 request_id bigint,
 http_invocation_ordinal bigint NOT NULL CHECK(http_invocation_ordinal>0),
 encumbered_usd numeric(8,2) NOT NULL CHECK(encumbered_usd=2.01),
 billing_basis text NOT NULL CHECK(billing_basis='CONSERVATIVE_INFRA_ENVELOPE_NOT_ACTUAL_INVOICE'),
 result jsonb,
 result_sha256 text CHECK(result_sha256~'^[0-9a-f]{64}$'),
 CHECK(expires_at>created_at AND expires_at<=created_at+interval '5 minutes'),
 CHECK((state IN('RUNNING','SUCCEEDED','FAILED')) IS NOT TRUE OR claimed_at IS NOT NULL),
 CHECK((state IN('SUCCEEDED','FAILED')) IS NOT TRUE OR (finished_at IS NOT NULL AND result IS NOT NULL AND result_sha256 IS NOT NULL))
);
ALTER TABLE population_top3000_202608_202609_v1.resource_probe_v1 ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON TABLE population_top3000_202608_202609_v1.resource_probe_v1 FROM PUBLIC,anon,authenticated,service_role;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.resource_probe_claim_v1(p_probe_id uuid,p_token text)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path TO 'pg_catalog'
AS $function$
DECLARE v_probe population_top3000_202608_202609_v1.resource_probe_v1%rowtype;
 v_control population_top3000_202608_202609_v1.acquisition_control%rowtype;
 v_pipeline population_top3000_202608_202609_v1.pipeline_control%rowtype;v_hash text;
BEGIN
 IF p_probe_id IS NULL OR p_token IS NULL OR p_token!~'^[0-9a-f]{64}$' THEN RETURN '{"state":"DENIED"}';END IF;
 v_hash:=encode(sha256(convert_to(p_probe_id::text||':'||p_token,'UTF8')),'hex');
 -- Reject unknown capabilities before locking mission controls.
 SELECT * INTO v_probe FROM population_top3000_202608_202609_v1.resource_probe_v1
  WHERE probe_id=p_probe_id AND token_sha256=v_hash AND state='PENDING' AND expires_at>clock_timestamp();
 IF NOT FOUND THEN RETURN '{"state":"DENIED"}';END IF;
 SELECT * INTO v_control FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton FOR UPDATE;
 SELECT * INTO v_pipeline FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton FOR UPDATE;
 SELECT * INTO v_probe FROM population_top3000_202608_202609_v1.resource_probe_v1 WHERE probe_id=p_probe_id FOR UPDATE;
 IF v_probe.state IS DISTINCT FROM 'PENDING' OR v_probe.token_sha256 IS DISTINCT FROM v_hash OR v_probe.expires_at<=clock_timestamp()
  OR v_control.owner_id IS DISTINCT FROM v_probe.owner_id OR v_control.owner_fence IS DISTINCT FROM v_probe.owner_fence
  OR v_control.stop_generation IS DISTINCT FROM v_probe.stop_generation OR v_probe.owner_fence<>2 OR v_probe.stop_generation<>0
  OR v_control.owner_id IS DISTINCT FROM 'top3000-recovery-20261023-root-4b1314bcb5c2'
  OR v_pipeline.expected_owner_id IS DISTINCT FROM v_control.owner_id OR v_pipeline.expected_owner_fence IS DISTINCT FROM v_control.owner_fence
  OR v_pipeline.expected_stop_generation IS DISTINCT FROM v_control.stop_generation
  OR v_control.scope_sha256 IS DISTINCT FROM v_probe.scope_sha256 OR v_pipeline.scope_sha256 IS DISTINCT FROM v_probe.scope_sha256
  OR v_control.enabled IS DISTINCT FROM true OR v_control.stopped IS DISTINCT FROM false OR v_control.hold_reason IS NOT NULL
  OR v_control.admitted_until IS NULL OR v_control.admitted_until<=clock_timestamp()
  OR v_pipeline.deadline IS NULL OR v_pipeline.deadline<=clock_timestamp()
  OR v_pipeline.budget_verified_at IS NULL OR v_pipeline.budget_evidence#>>'{resource_probe_admission,allowed}' IS DISTINCT FROM 'true'
  OR v_pipeline.budget_evidence#>>'{resource_probe_admission,worker}' IS DISTINCT FROM 'population-top3000-resource-v1'
  OR v_pipeline.budget_evidence->>'provider_admissions_allowed' IS DISTINCT FROM 'false'
  OR v_pipeline.unavoidable_usd<v_probe.encumbered_usd OR v_control.http_invocations<v_probe.http_invocation_ordinal
  OR (v_pipeline.actual_usd+v_pipeline.accrued_usd+v_pipeline.unavoidable_usd+v_pipeline.remaining_uncommitted_usd+v_pipeline.repair_egress_reserve_usd+v_pipeline.reserved_request_usd<=250) IS DISTINCT FROM true
 THEN RETURN '{"state":"DENIED"}';END IF;
 UPDATE population_top3000_202608_202609_v1.resource_probe_v1
 SET state='RUNNING',claimed_at=clock_timestamp(),claim_deadline_at=least(expires_at,clock_timestamp()+interval '8 seconds')
 WHERE probe_id=p_probe_id AND state='PENDING';
 RETURN jsonb_build_object('state','CLAIMED','probe_id',p_probe_id,'max_response_bytes',2097152,'total_timeout_ms',8000);
END $function$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.resource_probe_finish_v1(p_probe_id uuid,p_token text,p_result jsonb)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path TO 'pg_catalog'
AS $function$
DECLARE v_probe population_top3000_202608_202609_v1.resource_probe_v1%rowtype;
 v_control population_top3000_202608_202609_v1.acquisition_control%rowtype;
 v_pipeline population_top3000_202608_202609_v1.pipeline_control%rowtype;
 v_hash text;v_outcome text;v_error text;v_http integer;v_elapsed integer;v_observed timestamptz;
 v_metric jsonb;v_metrics jsonb:='[]'::jsonb;v_clean jsonb;v_size numeric;v_free numeric;v_avail numeric;
BEGIN
 IF p_probe_id IS NULL OR p_token IS NULL OR p_token!~'^[0-9a-f]{64}$' THEN RETURN '{"state":"DENIED"}';END IF;
 v_hash:=encode(sha256(convert_to(p_probe_id::text||':'||p_token,'UTF8')),'hex');
 SELECT * INTO v_probe FROM population_top3000_202608_202609_v1.resource_probe_v1 WHERE probe_id=p_probe_id AND token_sha256=v_hash;
 IF NOT FOUND THEN RETURN '{"state":"DENIED"}';END IF;
 IF v_probe.state IN('SUCCEEDED','FAILED') THEN RETURN jsonb_build_object('state','ALREADY_RECORDED','outcome',v_probe.state);END IF;
 SELECT * INTO v_control FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton FOR UPDATE;
 SELECT * INTO v_pipeline FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton FOR UPDATE;
 SELECT * INTO v_probe FROM population_top3000_202608_202609_v1.resource_probe_v1 WHERE probe_id=p_probe_id FOR UPDATE;
 IF v_probe.state IS DISTINCT FROM 'RUNNING' OR v_probe.token_sha256 IS DISTINCT FROM v_hash
  OR v_probe.expires_at<=clock_timestamp() OR v_probe.claim_deadline_at IS NULL OR v_probe.claim_deadline_at<=clock_timestamp()
  OR v_control.owner_id IS DISTINCT FROM v_probe.owner_id OR v_control.owner_fence IS DISTINCT FROM v_probe.owner_fence
  OR v_control.stop_generation IS DISTINCT FROM v_probe.stop_generation
  OR v_pipeline.expected_owner_id IS DISTINCT FROM v_control.owner_id OR v_pipeline.expected_owner_fence IS DISTINCT FROM v_control.owner_fence
  OR v_pipeline.expected_stop_generation IS DISTINCT FROM v_control.stop_generation
  OR v_control.scope_sha256 IS DISTINCT FROM v_probe.scope_sha256 OR v_pipeline.scope_sha256 IS DISTINCT FROM v_probe.scope_sha256
  OR v_control.enabled IS DISTINCT FROM true OR v_control.stopped IS DISTINCT FROM false OR v_control.hold_reason IS NOT NULL
  OR v_control.admitted_until IS NULL OR v_control.admitted_until<=clock_timestamp()
 THEN RETURN '{"state":"DENIED"}';END IF;
 IF jsonb_typeof(p_result) IS DISTINCT FROM 'object' OR octet_length(p_result::text)>32768
  OR EXISTS(SELECT 1 FROM jsonb_object_keys(p_result) keys(key) WHERE key NOT IN('outcome','observed_at','metrics_http_status','source_sha256','metrics','error_code','elapsed_ms','capabilities'))
 THEN RAISE EXCEPTION 'INVALID_SANITIZED_PROBE_RESULT';END IF;
 IF jsonb_typeof(p_result->'capabilities') IS DISTINCT FROM 'object'
  OR EXISTS(SELECT 1 FROM jsonb_object_keys(p_result->'capabilities') keys(key) WHERE key NOT IN('sec_user_agent_configured','sec_contact_email_configured','massive_key_configured'))
  OR jsonb_typeof(p_result#>'{capabilities,sec_user_agent_configured}') IS DISTINCT FROM 'boolean'
  OR jsonb_typeof(p_result#>'{capabilities,sec_contact_email_configured}') IS DISTINCT FROM 'boolean'
  OR jsonb_typeof(p_result#>'{capabilities,massive_key_configured}') IS DISTINCT FROM 'boolean'
 THEN RAISE EXCEPTION 'INVALID_SANITIZED_CAPABILITY_FLAGS';END IF;
 v_outcome:=p_result->>'outcome';v_error:=p_result->>'error_code';
 IF v_outcome IS NULL OR v_outcome NOT IN('SUCCEEDED','FAILED')
  OR jsonb_typeof(p_result->'metrics') IS DISTINCT FROM 'array' OR jsonb_array_length(p_result->'metrics')>32
  OR jsonb_typeof(p_result->'elapsed_ms') IS DISTINCT FROM 'number' THEN RAISE EXCEPTION 'INVALID_SANITIZED_PROBE_RESULT';END IF;
 v_http:=(p_result->>'metrics_http_status')::integer;v_elapsed:=(p_result->>'elapsed_ms')::integer;v_observed:=(p_result->>'observed_at')::timestamptz;
 IF v_observed IS NULL OR v_observed<v_probe.created_at-interval '5 seconds' OR v_observed>clock_timestamp()+interval '5 seconds'
  OR v_elapsed NOT BETWEEN 0 AND 8000 OR (v_http IS NOT NULL AND v_http NOT BETWEEN 100 AND 599)
  OR (p_result->>'source_sha256' IS NOT NULL AND p_result->>'source_sha256'!~'^[0-9a-f]{64}$') THEN RAISE EXCEPTION 'INVALID_SANITIZED_PROBE_RESULT';END IF;
 IF v_outcome='SUCCEEDED' AND (v_http IS DISTINCT FROM 200 OR p_result->>'source_sha256' IS NULL OR v_error IS NOT NULL OR jsonb_array_length(p_result->'metrics')=0) THEN RAISE EXCEPTION 'INCOMPLETE_SUCCESSFUL_PROBE_RESULT';END IF;
 IF v_outcome='FAILED' AND (v_error IS NULL OR v_error NOT IN('METRICS_CREDENTIAL_UNAVAILABLE','METRICS_HTTP_ERROR','METRICS_RESPONSE_TOO_LARGE','METRICS_READ_TIMEOUT','METRICS_PARSE_ERROR','FILESYSTEM_METRICS_MISSING','UNEXPECTED_FAILURE') OR jsonb_array_length(p_result->'metrics')<>0) THEN RAISE EXCEPTION 'INVALID_PROBE_FAILURE_CODE';END IF;
 FOR v_metric IN SELECT value FROM jsonb_array_elements(p_result->'metrics') LOOP
  IF jsonb_typeof(v_metric) IS DISTINCT FROM 'object'
   OR EXISTS(SELECT 1 FROM jsonb_object_keys(v_metric) keys(key) WHERE key NOT IN('filesystem_id','mountpoint','available_bytes','free_bytes','size_bytes'))
   OR (v_metric->>'filesystem_id') IS NULL OR v_metric->>'filesystem_id'!~'^[0-9a-f]{64}$'
   OR jsonb_typeof(v_metric->'mountpoint') IS DISTINCT FROM 'string' OR length(v_metric->>'mountpoint')>160 OR v_metric->>'mountpoint'~'[[:cntrl:]]'
   OR jsonb_typeof(v_metric->'available_bytes') IS DISTINCT FROM 'number' OR jsonb_typeof(v_metric->'free_bytes') IS DISTINCT FROM 'number' OR jsonb_typeof(v_metric->'size_bytes') IS DISTINCT FROM 'number'
  THEN RAISE EXCEPTION 'INVALID_FILESYSTEM_METRIC';END IF;
  v_avail:=(v_metric->>'available_bytes')::numeric;v_free:=(v_metric->>'free_bytes')::numeric;v_size:=(v_metric->>'size_bytes')::numeric;
  IF v_avail<0 OR v_free<v_avail OR v_size<=0 OR v_size<v_free OR v_size>9007199254740991 OR trunc(v_avail)<>v_avail OR trunc(v_free)<>v_free OR trunc(v_size)<>v_size THEN RAISE EXCEPTION 'INVALID_FILESYSTEM_METRIC';END IF;
  v_metrics:=v_metrics||jsonb_build_array(jsonb_build_object('filesystem_id',v_metric->>'filesystem_id','mountpoint',v_metric->>'mountpoint','available_bytes',v_avail,'free_bytes',v_free,'size_bytes',v_size));
 END LOOP;
 IF (SELECT count(*) FROM jsonb_array_elements(v_metrics))<>(SELECT count(DISTINCT value->>'filesystem_id') FROM jsonb_array_elements(v_metrics)) THEN RAISE EXCEPTION 'DUPLICATE_FILESYSTEM_METRIC';END IF;
 v_clean:=jsonb_build_object('outcome',v_outcome,'observed_at',v_observed,'metrics_http_status',v_http,'source_sha256',p_result->>'source_sha256',
  'metrics',v_metrics,'error_code',v_error,'elapsed_ms',v_elapsed,'capabilities',jsonb_build_object(
   'sec_user_agent_configured',(p_result#>>'{capabilities,sec_user_agent_configured}')::boolean,
   'sec_contact_email_configured',(p_result#>>'{capabilities,sec_contact_email_configured}')::boolean,
   'massive_key_configured',(p_result#>>'{capabilities,massive_key_configured}')::boolean),'source','OWNED_PROJECT_PRIVILEGED_METRICS','provider_requests',0,'no_capacity_change',true,
  'disk_interpretation','Per-filesystem snapshot; identify the database data mount before drawing capacity conclusions.');
 UPDATE population_top3000_202608_202609_v1.resource_probe_v1 SET state=v_outcome,finished_at=clock_timestamp(),result=v_clean,
  result_sha256=encode(sha256(convert_to(v_clean::text,'UTF8')),'hex') WHERE probe_id=p_probe_id;
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details)
 VALUES('RESOURCE_PROBE_RECORDED',jsonb_build_object('probe_id',p_probe_id,'outcome',v_outcome,'owner_fence',v_probe.owner_fence,'metrics_http_status',v_http,'filesystem_count',jsonb_array_length(v_metrics),'no_capacity_change',true,'budget_encumbrance_released',false));
 RETURN jsonb_build_object('state','RECORDED','outcome',v_outcome,'probe_id',p_probe_id);
END $function$;
REVOKE ALL ON FUNCTION population_top3000_202608_202609_v1.resource_probe_claim_v1(uuid,text) FROM PUBLIC,anon,authenticated;
REVOKE ALL ON FUNCTION population_top3000_202608_202609_v1.resource_probe_finish_v1(uuid,text,jsonb) FROM PUBLIC,anon,authenticated;
GRANT EXECUTE ON FUNCTION population_top3000_202608_202609_v1.resource_probe_claim_v1(uuid,text) TO postgres,service_role;
GRANT EXECUTE ON FUNCTION population_top3000_202608_202609_v1.resource_probe_finish_v1(uuid,text,jsonb) TO postgres,service_role;
