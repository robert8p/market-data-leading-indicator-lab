-- UNINTEGRATED PREPARATION: rollback could not obtain natural quiet on 2026-10-25.
-- DO NOT EXECUTE without a current exact claim, successful PostgreSQL rollback, and distinct review.
-- Slot-13-only resource probe capability. Existing v1/v2/v3 objects and rows remain immutable.
CREATE TABLE population_top3000_202608_202609_v1.resource_probe_v4 (
 probe_id uuid PRIMARY KEY,
 probe_kind text NOT NULL DEFAULT 'FILESYSTEM_METRICS' CHECK(probe_kind='FILESYSTEM_METRICS'),
 owner_id text NOT NULL CHECK(owner_id='top3000-completion-20261024-root-5db48b32d103'),
 owner_fence bigint NOT NULL CHECK(owner_fence=3),
 stop_generation bigint NOT NULL CHECK(stop_generation=1),
 scope_sha256 text NOT NULL CHECK(scope_sha256='8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd'),
 token_sha256 text NOT NULL,
 state text NOT NULL CHECK(state IN('PENDING','RUNNING','SUCCEEDED','FAILED','EXPIRED')),
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 expires_at timestamptz NOT NULL,
 claimed_at timestamptz,
 claim_deadline_at timestamptz,
 finished_at timestamptz,
 request_id bigint,
 http_invocation_ordinal bigint NOT NULL,
 encumbered_usd numeric(14,6) NOT NULL DEFAULT 1 CHECK(encumbered_usd=1),
 billing_basis text NOT NULL DEFAULT 'UNINVOICED_RESERVATION_NOT_ACTUAL_CHARGE',
 result jsonb,
 result_sha256 text,
 probe_attempt integer NOT NULL UNIQUE CHECK(probe_attempt=13),
 max_runtime_ms integer NOT NULL CHECK(max_runtime_ms=30000),
 CHECK(expires_at>created_at AND expires_at<=created_at+interval '5 minutes')
);
ALTER TABLE population_top3000_202608_202609_v1.resource_probe_v4 ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON population_top3000_202608_202609_v1.resource_probe_v4 FROM public,anon,authenticated;

CREATE FUNCTION population_top3000_202608_202609_v1.resource_probe_claim_v4(p_probe_id uuid,p_token text)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $f$
DECLARE v population_top3000_202608_202609_v1.resource_probe_v4%rowtype;
 a population_top3000_202608_202609_v1.acquisition_control%rowtype;
 p population_top3000_202608_202609_v1.pipeline_control%rowtype;h text;
BEGIN
 IF p_token IS NULL OR p_token!~'^[0-9a-f]{64}$' THEN RETURN '{"state":"DENIED"}';END IF;
 PERFORM pg_advisory_xact_lock(2026083000261022);
 h:=encode(sha256(convert_to(p_probe_id::text||':'||p_token,'UTF8')),'hex');
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.resource_probe_v4 WHERE probe_id=p_probe_id AND token_sha256=h AND state='PENDING') THEN RETURN '{"state":"DENIED"}';END IF;
 SELECT * INTO STRICT a FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton FOR UPDATE;
 SELECT * INTO STRICT p FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton FOR UPDATE;
 SELECT * INTO STRICT v FROM population_top3000_202608_202609_v1.resource_probe_v4 WHERE probe_id=p_probe_id FOR UPDATE;
 IF v.probe_attempt IS DISTINCT FROM 13 OR v.max_runtime_ms IS DISTINCT FROM 30000
 OR v.state<>'PENDING' OR v.token_sha256 IS DISTINCT FROM h OR v.expires_at<=clock_timestamp()+interval '31 seconds'
 OR a.owner_id IS DISTINCT FROM v.owner_id OR a.owner_fence IS DISTINCT FROM v.owner_fence OR a.stop_generation IS DISTINCT FROM v.stop_generation
 OR a.scope_sha256 IS DISTINCT FROM v.scope_sha256 OR p.scope_sha256 IS DISTINCT FROM v.scope_sha256
 OR p.expected_owner_id IS DISTINCT FROM a.owner_id OR p.expected_owner_fence IS DISTINCT FROM a.owner_fence OR p.expected_stop_generation IS DISTINCT FROM a.stop_generation
 OR a.stopped IS DISTINCT FROM false OR a.enabled IS DISTINCT FROM true OR p.enabled IS DISTINCT FROM true OR a.hold_reason IS NOT NULL
 OR a.admitted_until IS NULL OR a.admitted_until<=clock_timestamp() OR p.deadline IS NULL OR p.deadline<=clock_timestamp()
 OR p.budget_evidence#>>'{monetary_policy,authority_event_id}' IS DISTINCT FROM '9528'
 OR p.shared_scheduler_verified IS DISTINCT FROM true OR p.tick_count>=p.max_ticks
 OR population_top3000_202608_202609_v1.pipeline_cost_admission_v2(p,0) IS DISTINCT FROM true
 OR (SELECT count(*) FROM population_top3000_202608_202609_v1.resource_probe_v1)<>3
 OR (SELECT count(*) FROM population_top3000_202608_202609_v1.resource_probe_v2 WHERE probe_attempt=4 AND state='SUCCEEDED')<>1
 OR (SELECT count(*) FROM population_top3000_202608_202609_v1.resource_probe_v3 WHERE probe_attempt BETWEEN 5 AND 12 AND state='SUCCEEDED')<>8
 OR (SELECT count(*) FROM population_top3000_202608_202609_v1.resource_probe_v3)<>8
 OR a.http_invocations<v.http_invocation_ordinal OR p.reserved_request_usd<v.encumbered_usd THEN RETURN '{"state":"DENIED"}';END IF;
 UPDATE population_top3000_202608_202609_v1.resource_probe_v4 SET state='RUNNING',claimed_at=clock_timestamp(),claim_deadline_at=clock_timestamp()+interval '30 seconds' WHERE probe_id=p_probe_id;
 RETURN jsonb_build_object('state','CLAIMED','probe_id',p_probe_id,'max_response_bytes',2097152,'total_timeout_ms',30000,'probe_attempt',13);
END;$f$;

CREATE FUNCTION population_top3000_202608_202609_v1.resource_probe_finish_v4(p_probe_id uuid,p_token text,p_result jsonb)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $f$
DECLARE v population_top3000_202608_202609_v1.resource_probe_v4%rowtype;
 a population_top3000_202608_202609_v1.acquisition_control%rowtype;
 p population_top3000_202608_202609_v1.pipeline_control%rowtype;
 h text;outcome text;err text;http integer;elapsed integer;observed timestamptz;
 metric jsonb;metrics jsonb:='[]'::jsonb;clean jsonb;sz numeric;fr numeric;av numeric;
BEGIN
 IF p_probe_id IS NULL OR p_token IS NULL OR p_token!~'^[0-9a-f]{64}$' THEN RETURN '{"state":"DENIED"}';END IF;
 PERFORM pg_advisory_xact_lock(2026083000261022);
 h:=encode(sha256(convert_to(p_probe_id::text||':'||p_token,'UTF8')),'hex');
 SELECT * INTO v FROM population_top3000_202608_202609_v1.resource_probe_v4 WHERE probe_id=p_probe_id AND token_sha256=h;
 IF NOT FOUND THEN RETURN '{"state":"DENIED"}';END IF;
 IF v.state IN('SUCCEEDED','FAILED') THEN RETURN jsonb_build_object('state','ALREADY_RECORDED','outcome',v.state);END IF;
 SELECT * INTO a FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton FOR UPDATE;
 SELECT * INTO p FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton FOR UPDATE;
 SELECT * INTO v FROM population_top3000_202608_202609_v1.resource_probe_v4 WHERE probe_id=p_probe_id FOR UPDATE;
 IF v.probe_attempt IS DISTINCT FROM 13 OR v.max_runtime_ms IS DISTINCT FROM 30000 OR v.state IS DISTINCT FROM 'RUNNING'
 OR v.token_sha256 IS DISTINCT FROM h OR v.expires_at<=clock_timestamp() OR v.claim_deadline_at IS NULL OR v.claim_deadline_at<=clock_timestamp()
 OR a.owner_id IS DISTINCT FROM v.owner_id OR a.owner_fence IS DISTINCT FROM v.owner_fence OR a.stop_generation IS DISTINCT FROM v.stop_generation
 OR p.expected_owner_id IS DISTINCT FROM a.owner_id OR p.expected_owner_fence IS DISTINCT FROM a.owner_fence OR p.expected_stop_generation IS DISTINCT FROM a.stop_generation
 OR a.scope_sha256 IS DISTINCT FROM v.scope_sha256 OR p.scope_sha256 IS DISTINCT FROM v.scope_sha256
 OR a.enabled IS DISTINCT FROM true OR a.stopped IS DISTINCT FROM false OR a.hold_reason IS NOT NULL
 OR p.budget_evidence#>>'{monetary_policy,authority_event_id}' IS DISTINCT FROM '9528' OR p.enabled IS DISTINCT FROM true
 OR p.shared_scheduler_verified IS DISTINCT FROM true OR p.deadline IS NULL OR p.deadline<=clock_timestamp() OR p.tick_count>=p.max_ticks
 OR a.admitted_until IS NULL OR a.admitted_until<=clock_timestamp() THEN RETURN '{"state":"DENIED"}';END IF;
 IF jsonb_typeof(p_result) IS DISTINCT FROM 'object' OR octet_length(p_result::text)>32768
 OR EXISTS(SELECT 1 FROM jsonb_object_keys(p_result) k WHERE k NOT IN('outcome','observed_at','metrics_http_status','source_sha256','metrics','error_code','elapsed_ms','capabilities'))
 THEN RAISE EXCEPTION 'INVALID_SANITIZED_PROBE_RESULT';END IF;
 IF jsonb_typeof(p_result->'capabilities') IS DISTINCT FROM 'object'
 OR EXISTS(SELECT 1 FROM jsonb_object_keys(p_result->'capabilities') k WHERE k NOT IN('sec_user_agent_configured','sec_contact_email_configured','massive_key_configured'))
 OR jsonb_typeof(p_result#>'{capabilities,sec_user_agent_configured}') IS DISTINCT FROM 'boolean'
 OR jsonb_typeof(p_result#>'{capabilities,sec_contact_email_configured}') IS DISTINCT FROM 'boolean'
 OR jsonb_typeof(p_result#>'{capabilities,massive_key_configured}') IS DISTINCT FROM 'boolean' THEN RAISE EXCEPTION 'INVALID_SANITIZED_CAPABILITY_FLAGS';END IF;
 outcome:=p_result->>'outcome';err:=p_result->>'error_code';
 IF outcome IS NULL OR outcome NOT IN('SUCCEEDED','FAILED') OR jsonb_typeof(p_result->'metrics') IS DISTINCT FROM 'array'
 OR jsonb_array_length(p_result->'metrics')>32 OR jsonb_typeof(p_result->'elapsed_ms') IS DISTINCT FROM 'number' THEN RAISE EXCEPTION 'INVALID_SANITIZED_PROBE_RESULT';END IF;
 http:=(p_result->>'metrics_http_status')::integer;elapsed:=(p_result->>'elapsed_ms')::integer;observed:=(p_result->>'observed_at')::timestamptz;
 IF observed IS NULL OR observed<v.created_at-interval '5 seconds' OR observed>clock_timestamp()+interval '5 seconds'
 OR elapsed NOT BETWEEN 0 AND v.max_runtime_ms OR (http IS NOT NULL AND http NOT BETWEEN 100 AND 599)
 OR (p_result->>'source_sha256' IS NOT NULL AND p_result->>'source_sha256'!~'^[0-9a-f]{64}$') THEN RAISE EXCEPTION 'INVALID_SANITIZED_PROBE_RESULT';END IF;
 IF outcome='SUCCEEDED' AND (http IS DISTINCT FROM 200 OR p_result->>'source_sha256' IS NULL OR err IS NOT NULL OR jsonb_array_length(p_result->'metrics')=0) THEN RAISE EXCEPTION 'INCOMPLETE_SUCCESSFUL_PROBE_RESULT';END IF;
 IF outcome='FAILED' AND (err IS NULL OR err NOT IN('METRICS_CREDENTIAL_UNAVAILABLE','METRICS_HTTP_ERROR','METRICS_RESPONSE_TOO_LARGE','METRICS_READ_TIMEOUT','METRICS_PARSE_ERROR','FILESYSTEM_METRICS_MISSING','UNEXPECTED_FAILURE') OR jsonb_array_length(p_result->'metrics')<>0) THEN RAISE EXCEPTION 'INVALID_PROBE_FAILURE_CODE';END IF;
 FOR metric IN SELECT value FROM jsonb_array_elements(p_result->'metrics') LOOP
  IF jsonb_typeof(metric) IS DISTINCT FROM 'object'
   OR EXISTS(SELECT 1 FROM jsonb_object_keys(metric) k WHERE k NOT IN('filesystem_id','mountpoint','available_bytes','free_bytes','size_bytes'))
   OR metric->>'filesystem_id' IS NULL OR metric->>'filesystem_id'!~'^[0-9a-f]{64}$'
   OR jsonb_typeof(metric->'mountpoint') IS DISTINCT FROM 'string' OR length(metric->>'mountpoint')>160 OR metric->>'mountpoint'~'[[:cntrl:]]'
   OR jsonb_typeof(metric->'available_bytes') IS DISTINCT FROM 'number' OR jsonb_typeof(metric->'free_bytes') IS DISTINCT FROM 'number' OR jsonb_typeof(metric->'size_bytes') IS DISTINCT FROM 'number'
  THEN RAISE EXCEPTION 'INVALID_FILESYSTEM_METRIC';END IF;
  av:=(metric->>'available_bytes')::numeric;fr:=(metric->>'free_bytes')::numeric;sz:=(metric->>'size_bytes')::numeric;
  IF av<0 OR fr<av OR sz<=0 OR sz<fr OR sz>9007199254740991 OR trunc(av)<>av OR trunc(fr)<>fr OR trunc(sz)<>sz THEN RAISE EXCEPTION 'INVALID_FILESYSTEM_METRIC';END IF;
  metrics:=metrics||jsonb_build_array(jsonb_build_object('filesystem_id',metric->>'filesystem_id','mountpoint',metric->>'mountpoint','available_bytes',av,'free_bytes',fr,'size_bytes',sz));
 END LOOP;
 IF (SELECT count(*) FROM jsonb_array_elements(metrics))<>(SELECT count(DISTINCT value->>'filesystem_id') FROM jsonb_array_elements(metrics)) THEN RAISE EXCEPTION 'DUPLICATE_FILESYSTEM_METRIC';END IF;
 clean:=jsonb_build_object('outcome',outcome,'observed_at',observed,'metrics_http_status',http,'source_sha256',p_result->>'source_sha256','metrics',metrics,'error_code',err,'elapsed_ms',elapsed,
  'capabilities',jsonb_build_object('sec_user_agent_configured',(p_result#>>'{capabilities,sec_user_agent_configured}')::boolean,'sec_contact_email_configured',(p_result#>>'{capabilities,sec_contact_email_configured}')::boolean,'massive_key_configured',(p_result#>>'{capabilities,massive_key_configured}')::boolean),
  'source','OWNED_PROJECT_PRIVILEGED_METRICS','provider_requests',0,'no_capacity_change',true,'disk_interpretation','Per-filesystem snapshot; identify the database data mount before drawing capacity conclusions.');
 UPDATE population_top3000_202608_202609_v1.resource_probe_v4 SET state=outcome,finished_at=clock_timestamp(),result=clean,result_sha256=encode(sha256(convert_to(clean::text,'UTF8')),'hex') WHERE probe_id=p_probe_id;
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES('RESOURCE_PROBE_V4_RECORDED',jsonb_build_object('probe_id',p_probe_id,'outcome',outcome,'owner_fence',v.owner_fence,'metrics_http_status',http,'filesystem_count',jsonb_array_length(metrics),'probe_attempt',13,'no_capacity_change',true,'budget_encumbrance_released',false));
 RETURN jsonb_build_object('state','RECORDED','outcome',outcome,'probe_id',p_probe_id);
END;$f$;

REVOKE ALL ON FUNCTION population_top3000_202608_202609_v1.resource_probe_claim_v4(uuid,text),population_top3000_202608_202609_v1.resource_probe_finish_v4(uuid,text,jsonb) FROM public,anon,authenticated;
