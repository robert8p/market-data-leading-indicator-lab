-- Install only; root separately admits one deployment-verified probe. No provider calls here.
CREATE TABLE population_top3000_202608_202609_v1.ctx02_s3_head_probe_v1 (
 probe_id uuid PRIMARY KEY,probe_key text NOT NULL UNIQUE CHECK(probe_key='ctx02-s3-head-20260803-v1'),
 owner_id text NOT NULL,owner_fence bigint NOT NULL,stop_generation bigint NOT NULL,scope_sha256 text NOT NULL,
 token_sha256 text NOT NULL,created_at timestamptz NOT NULL DEFAULT clock_timestamp(),expires_at timestamptz NOT NULL,
 state text NOT NULL CHECK(state IN('PENDING','RUNNING','RECORDED','UNKNOWN_EFFECT')),claim_deadline timestamptz,
 request_id bigint,finished_at timestamptz,result jsonb,result_sha256 text,
 CHECK(expires_at>created_at AND expires_at<=created_at+interval '5 minutes')
);
ALTER TABLE population_top3000_202608_202609_v1.ctx02_s3_head_probe_v1 ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON population_top3000_202608_202609_v1.ctx02_s3_head_probe_v1 FROM public,anon,authenticated;
CREATE FUNCTION population_top3000_202608_202609_v1.ctx02_probe_control_assert_v1()
RETURNS void LANGUAGE plpgsql SET search_path=pg_catalog AS $f$
DECLARE a population_top3000_202608_202609_v1.acquisition_control%rowtype;p population_top3000_202608_202609_v1.pipeline_control%rowtype;
BEGIN
 SELECT * INTO STRICT a FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton FOR UPDATE;
 SELECT * INTO STRICT p FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton FOR UPDATE;
 IF a.owner_id IS DISTINCT FROM 'top3000-completion-20261024-root-5db48b32d103' OR a.owner_fence IS DISTINCT FROM 3 OR a.stop_generation IS DISTINCT FROM 1 OR a.stopped IS DISTINCT FROM false
 OR a.enabled IS DISTINCT FROM true OR p.enabled IS DISTINCT FROM true OR a.hold_reason IS NOT NULL
 OR a.admitted_until IS NULL OR a.admitted_until<=clock_timestamp() OR p.deadline IS NULL OR p.deadline<=clock_timestamp()
 OR p.expected_owner_id IS DISTINCT FROM a.owner_id OR p.expected_owner_fence IS DISTINCT FROM a.owner_fence OR p.expected_stop_generation IS DISTINCT FROM a.stop_generation
 OR a.scope_sha256 IS DISTINCT FROM '8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd' OR p.scope_sha256 IS DISTINCT FROM a.scope_sha256
 OR p.shared_scheduler_verified IS DISTINCT FROM true OR p.tick_count>=p.max_ticks OR population_top3000_202608_202609_v1.pipeline_cost_admission_v2(p,0) IS DISTINCT FROM true
 THEN RAISE EXCEPTION 'CTX02_PROBE_CURRENT_CONTROLLER_REQUIRED';END IF;
END; $f$;
CREATE FUNCTION population_top3000_202608_202609_v1.ctx02_s3_head_claim_v1(p_id uuid,p_token text)
RETURNS jsonb LANGUAGE plpgsql SET search_path=pg_catalog AS $f$
DECLARE v population_top3000_202608_202609_v1.ctx02_s3_head_probe_v1%rowtype;
BEGIN
 IF p_token IS NULL OR p_token!~'^[0-9a-f]{64}$' OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.ctx02_s3_head_probe_v1 WHERE probe_id=p_id AND token_sha256=encode(sha256(convert_to(p_id::text||':'||p_token,'UTF8')),'hex') AND state='PENDING') THEN RETURN '{"state":"DENIED"}';END IF;
 PERFORM population_top3000_202608_202609_v1.ctx02_probe_control_assert_v1();
 SELECT * INTO STRICT v FROM population_top3000_202608_202609_v1.ctx02_s3_head_probe_v1 WHERE probe_id=p_id FOR UPDATE;
 IF v.scope_sha256 IS DISTINCT FROM '8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd' OR v.state<>'PENDING' OR v.expires_at<=clock_timestamp()+interval '45 seconds' OR v.owner_id IS DISTINCT FROM 'top3000-completion-20261024-root-5db48b32d103' OR v.owner_fence<>3 OR v.stop_generation<>1
 OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.connector_probe_lease_v1 WHERE request_key=v.probe_key AND released_at IS NULL AND owner_id=v.owner_id AND owner_fence=v.owner_fence AND stop_generation=v.stop_generation)
 THEN RETURN '{"state":"DENIED"}';END IF;
 UPDATE population_top3000_202608_202609_v1.ctx02_s3_head_probe_v1 SET state='RUNNING',claim_deadline=clock_timestamp()+interval '45 seconds' WHERE probe_id=p_id;
 RETURN jsonb_build_object('state','CLAIMED','target','https://files.massive.com/flatfiles/us_options_opra/day_aggs_v1/2026/08/2026-08-03.csv.gz','method','HEAD','max_provider_requests',1,'max_object_download_bytes',0);
END; $f$;
CREATE FUNCTION population_top3000_202608_202609_v1.ctx02_s3_head_finish_v1(p_id uuid,p_token text,p_result jsonb)
RETURNS jsonb LANGUAGE plpgsql SET search_path=pg_catalog AS $f$
DECLARE v population_top3000_202608_202609_v1.ctx02_s3_head_probe_v1%rowtype;l population_top3000_202608_202609_v1.connector_probe_lease_v1%rowtype;
 kind text;status integer;h text;body text;source_id text;header record;receipt jsonb;clean jsonb;rr jsonb;
BEGIN
 IF p_token IS NULL OR p_token!~'^[0-9a-f]{64}$' THEN RAISE EXCEPTION 'CTX02_PROBE_TOKEN_REJECTED';END IF;
 PERFORM population_top3000_202608_202609_v1.ctx02_probe_control_assert_v1();
 SELECT * INTO STRICT v FROM population_top3000_202608_202609_v1.ctx02_s3_head_probe_v1 WHERE probe_id=p_id FOR UPDATE;
 IF v.scope_sha256 IS DISTINCT FROM '8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd' OR v.token_sha256 IS DISTINCT FROM encode(sha256(convert_to(p_id::text||':'||p_token,'UTF8')),'hex') OR v.owner_fence<>3 OR v.stop_generation<>1 OR v.owner_id IS DISTINCT FROM 'top3000-completion-20261024-root-5db48b32d103' THEN RAISE EXCEPTION 'CTX02_PROBE_TOKEN_REJECTED';END IF;
 IF v.state IN('RECORDED','UNKNOWN_EFFECT') THEN
 IF v.result->'worker_result' IS DISTINCT FROM p_result THEN RAISE EXCEPTION 'CTX02_PROBE_IMMUTABLE_RESULT_CONFLICT';END IF;
 RETURN jsonb_build_object('state','ALREADY_RECORDED','source_id','ctx02-s3-head:'||p_id);END IF;
 IF v.state<>'RUNNING' OR v.expires_at<=clock_timestamp() OR v.claim_deadline<=clock_timestamp() THEN RAISE EXCEPTION 'CTX02_PROBE_EXPIRED_OR_UNCLAIMED';END IF;
 IF jsonb_typeof(p_result) IS DISTINCT FROM 'object' OR octet_length(p_result::text)>4096
 OR EXISTS(SELECT 1 FROM jsonb_object_keys(p_result) k WHERE k NOT IN('kind','credential_presence','fetch_invocations','object_bytes_downloaded','http_status','headers','observed_at','elapsed_ms'))
 OR jsonb_typeof(p_result->'credential_presence') IS DISTINCT FROM 'object'
 OR (SELECT count(*) FROM jsonb_object_keys(p_result->'credential_presence'))<>2
 OR jsonb_typeof(p_result#>'{credential_presence,access_key_present}') IS DISTINCT FROM 'boolean'
 OR jsonb_typeof(p_result#>'{credential_presence,secret_key_present}') IS DISTINCT FROM 'boolean'
 OR p_result->>'object_bytes_downloaded' IS DISTINCT FROM '0' THEN RAISE EXCEPTION 'CTX02_PROBE_INVALID_SANITIZED_RESULT';END IF;
 kind:=p_result->>'kind';
 IF kind IS NULL OR kind NOT IN('HEAD_RESPONSE','MISSING_CREDENTIALS','CONFIRMED_NOT_DISPATCHED','UNKNOWN_READ_OUTCOME') THEN RAISE EXCEPTION 'CTX02_PROBE_INVALID_RESULT_KIND';END IF;
 IF kind IN('MISSING_CREDENTIALS','CONFIRMED_NOT_DISPATCHED') AND p_result->>'fetch_invocations' IS DISTINCT FROM '0' OR kind IN('HEAD_RESPONSE','UNKNOWN_READ_OUTCOME') AND p_result->>'fetch_invocations' IS DISTINCT FROM '1' THEN RAISE EXCEPTION 'CTX02_PROBE_FETCH_COUNT_MISMATCH';END IF;
 IF kind='MISSING_CREDENTIALS' AND p_result#>>'{credential_presence,access_key_present}'='true' AND p_result#>>'{credential_presence,secret_key_present}'='true' THEN RAISE EXCEPTION 'CTX02_PROBE_MISSING_CREDENTIAL_CONTRADICTION';END IF;
 IF kind='HEAD_RESPONSE' THEN
  IF p_result#>>'{credential_presence,access_key_present}' IS DISTINCT FROM 'true' OR p_result#>>'{credential_presence,secret_key_present}' IS DISTINCT FROM 'true'
   OR p_result->>'http_status' IS NULL OR p_result->>'http_status' !~ '^[1-5][0-9]{2}$'
   OR jsonb_typeof(p_result->'headers') IS DISTINCT FROM 'object' OR (SELECT count(*) FROM jsonb_object_keys(p_result->'headers'))<>4
   OR EXISTS(SELECT 1 FROM jsonb_object_keys(p_result->'headers') k WHERE k NOT IN('content-length','content-type','etag','last-modified'))
   OR p_result->>'observed_at' IS NULL OR (p_result->>'observed_at')::timestamptz<v.created_at OR (p_result->>'observed_at')::timestamptz>clock_timestamp()+interval '5 seconds'
   OR p_result->>'elapsed_ms' IS NULL OR p_result->>'elapsed_ms' !~ '^[0-9]{1,5}$' OR (p_result->>'elapsed_ms')::integer>45000
   THEN RAISE EXCEPTION 'CTX02_PROBE_HEAD_RESPONSE_INVALID';END IF;
  FOR header IN SELECT * FROM jsonb_each(p_result->'headers') LOOP
   IF jsonb_typeof(header.value) NOT IN('string','null') OR length(header.value#>>'{}')>256 OR (header.value#>>'{}')~'[[:cntrl:]]' THEN RAISE EXCEPTION 'CTX02_PROBE_HEADER_INVALID';END IF;
  END LOOP;
  IF p_result#>>'{headers,content-length}' IS NOT NULL AND p_result#>>'{headers,content-length}' !~ '^[0-9]{1,18}$' THEN RAISE EXCEPTION 'CTX02_PROBE_LENGTH_INVALID';END IF;
  status:=(p_result->>'http_status')::integer;
 ELSE
  IF p_result ?| ARRAY['http_status','headers','observed_at','elapsed_ms'] THEN RAISE EXCEPTION 'CTX02_PROBE_NONRESPONSE_METADATA_REJECTED';END IF;
 END IF;
 clean:=jsonb_build_object('worker_result',p_result,'request_method','HEAD','request_url','https://files.massive.com/flatfiles/us_options_opra/day_aggs_v1/2026/08/2026-08-03.csv.gz','source_representation','SANITIZED_CURRENT_HEAD_METADATA_NOT_OBJECT_CONTENT','original_historical_receipt',false,'full_census_verified',false,'provider_response_body_retained',false,'automatic_retry',false,'recorded_at',clock_timestamp(),'required_successor',CASE WHEN kind='HEAD_RESPONSE' AND status=200 THEN 'BOUNDED_ARCHIVE_GET_PARSE_THROUGHPUT_AND_INDEPENDENT_CONTRACT_CENSUS' ELSE 'RESOLVE_EXACT_S3_CAPABILITY_OR_BOUNDED_ALTERNATE_ARCHIVE_LICENSE' END);
 body:=clean::text;h:=encode(sha256(convert_to(body,'UTF8')),'hex');source_id:='ctx02-s3-head:'||p_id;
 SELECT * INTO STRICT l FROM population_top3000_202608_202609_v1.connector_probe_lease_v1 WHERE request_key=v.probe_key;
 IF octet_length(body)>l.reserved_bytes THEN RAISE EXCEPTION 'CTX02_PROBE_METADATA_BOUND';END IF;
 INSERT INTO population_top3000_202608_202609_v1.source_document(source_id,source_sha256,source_uri,source_provider,retrieved_at,content_text,content_bytes,evidence,license_policy)
 VALUES(source_id,h,clean->>'request_url','Massive S3 HEAD feasibility',clock_timestamp(),body,octet_length(body),jsonb_build_object('current_receipt_only',true,'provider_object_contents_retained',false,'scope_sha256',v.scope_sha256),'PRIVATE_MISSION_FEASIBILITY_METADATA');
 UPDATE population_top3000_202608_202609_v1.acquisition_control SET stored_bytes=stored_bytes+octet_length(body) WHERE singleton;
 IF kind<>'UNKNOWN_READ_OUTCOME' THEN
  receipt:=jsonb_build_object('state',CASE WHEN kind<>'HEAD_RESPONSE' THEN 'CONFIRMED_NOT_DISPATCHED' WHEN status>=400 THEN 'HTTP_ERROR' ELSE 'SUCCESS' END,'request_sha256',l.request_sha256,'response_sha256',h,'response_bytes',octet_length(body),'physical_http_count','UNKNOWN_CONNECTOR_INTERNALS','worker_fetch_invocations',(p_result->>'fetch_invocations')::integer,'response_representation','SANITIZED_HEAD_METADATA','http_status',status,'original_historical_receipt',false);
  rr:=population_top3000_202608_202609_v1.connector_probe_reconcile_v1(v.owner_id,v.owner_fence,v.stop_generation,v.probe_key,receipt,CASE WHEN status=429 THEN 60 ELSE 15 END);
 END IF;
 UPDATE population_top3000_202608_202609_v1.ctx02_s3_head_probe_v1 SET state=CASE WHEN kind='UNKNOWN_READ_OUTCOME' THEN 'UNKNOWN_EFFECT' ELSE 'RECORDED' END,finished_at=clock_timestamp(),result=clean,result_sha256=h WHERE probe_id=p_id;
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES('CTX02_S3_HEAD_FEASIBILITY_RECORDED_V1',jsonb_build_object('probe_id',p_id,'source_id',source_id,'source_sha256',h,'kind',kind,'http_status',status,'owner_id',v.owner_id,'owner_fence',v.owner_fence,'stop_generation',v.stop_generation,'unresolved_provider_effect',kind='UNKNOWN_READ_OUTCOME','successor',clean->>'required_successor'));
 PERFORM population_top3000_202608_202609_v1.d02_daily_resource_guard_v2();
 RETURN jsonb_build_object('state','RECORDED','kind',kind,'source_id',source_id,'source_sha256',h);
END; $f$;
REVOKE ALL ON FUNCTION population_top3000_202608_202609_v1.ctx02_probe_control_assert_v1(),population_top3000_202608_202609_v1.ctx02_s3_head_claim_v1(uuid,text),population_top3000_202608_202609_v1.ctx02_s3_head_finish_v1(uuid,text,jsonb) FROM public,anon,authenticated;
