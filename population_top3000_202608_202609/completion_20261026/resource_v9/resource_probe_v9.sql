-- REUSABLE FINITE 18..41. OLD1..17 IMMUTABLE. No HTTP, provider call, source body or capacity change when installing this code.
CREATE TABLE population_top3000_202608_202609_v1.resource_probe_v9 (
 probe_id uuid PRIMARY KEY,
 clearing_id text NOT NULL,
 authorization_review_event_id bigint NOT NULL UNIQUE,
 authorization_receipt jsonb NOT NULL,
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
 probe_attempt integer NOT NULL UNIQUE CHECK(probe_attempt BETWEEN 18 AND 41),
 max_runtime_ms integer NOT NULL CHECK(max_runtime_ms=30000),
 CHECK(expires_at>created_at AND expires_at<=created_at+interval '5 minutes')
);
ALTER TABLE population_top3000_202608_202609_v1.resource_probe_v9 ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON population_top3000_202608_202609_v1.resource_probe_v9 FROM public,anon,authenticated,service_role;

CREATE FUNCTION population_top3000_202608_202609_v1.resource_probe_claim_v9(p_probe_id uuid,p_token text)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $f$
DECLARE v population_top3000_202608_202609_v1.resource_probe_v9%rowtype;
 a population_top3000_202608_202609_v1.acquisition_control%rowtype;
 p population_top3000_202608_202609_v1.pipeline_control%rowtype;h text;
BEGIN
 IF p_token IS NULL OR p_token!~'^[0-9a-f]{64}$' THEN RETURN '{"state":"DENIED"}';END IF;
 PERFORM pg_advisory_xact_lock(2026083000261022);
 h:=encode(sha256(convert_to(p_probe_id::text||':'||p_token,'UTF8')),'hex');
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.resource_probe_v9 WHERE probe_id=p_probe_id AND token_sha256=h AND state='PENDING') THEN RETURN '{"state":"DENIED"}';END IF;
 SELECT * INTO STRICT a FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton FOR UPDATE;
 SELECT * INTO STRICT p FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton FOR UPDATE;
 SELECT * INTO STRICT v FROM population_top3000_202608_202609_v1.resource_probe_v9 WHERE probe_id=p_probe_id FOR UPDATE;
 IF v.probe_attempt NOT BETWEEN 18 AND 41 OR v.max_runtime_ms IS DISTINCT FROM 30000
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
 
 OR (SELECT count(*) FROM population_top3000_202608_202609_v1.resource_probe_v4)<>1
 OR (SELECT count(*) FROM population_top3000_202608_202609_v1.resource_probe_v4 WHERE probe_attempt=13 AND state='SUCCEEDED' AND request_id=12425 AND result_sha256='967ca4e0a9026dabb941a49fc559c6e8f4f63336d6c0ab9ac17226ac91f81c47' AND result_sha256=encode(sha256(convert_to(result::text,'UTF8')),'hex'))<>1
 OR (SELECT encode(sha256(convert_to(coalesce(jsonb_agg(to_jsonb(x) ORDER BY probe_attempt),'[]'::jsonb)::text,'UTF8')),'hex') FROM population_top3000_202608_202609_v1.resource_probe_v1 x) IS DISTINCT FROM 'ef116b47c37ebfb4f8c994749ea5f0cc23aa347aa73080b7147cd82d4a60e5f5'
 OR (SELECT encode(sha256(convert_to(coalesce(jsonb_agg(to_jsonb(x) ORDER BY probe_attempt),'[]'::jsonb)::text,'UTF8')),'hex') FROM population_top3000_202608_202609_v1.resource_probe_v2 x) IS DISTINCT FROM '3d6cdd01a13e29ca608b99e22a456a0be93f38c404bde121be980da160f9a545'
 OR (SELECT encode(sha256(convert_to(coalesce(jsonb_agg(to_jsonb(x) ORDER BY probe_attempt),'[]'::jsonb)::text,'UTF8')),'hex') FROM population_top3000_202608_202609_v1.resource_probe_v3 x) IS DISTINCT FROM '6aa015a9036da5ce400a023492dab8a3e30f8faee5b32f7877f8ccaa3d69df1d'
 OR (SELECT encode(sha256(convert_to(coalesce(jsonb_agg(to_jsonb(x) ORDER BY probe_attempt),'[]'::jsonb)::text,'UTF8')),'hex') FROM population_top3000_202608_202609_v1.resource_probe_v4 x) IS DISTINCT FROM 'ad0330cff82c1edbf8434b83b2b8ce4f9f2e7069d431bcd258d771629ebedd6b'
 OR (SELECT count(*) FROM population_top3000_202608_202609_v1.resource_probe_v5)<>1
 OR (SELECT count(*) FROM population_top3000_202608_202609_v1.resource_probe_v5 WHERE probe_attempt=14 AND state='SUCCEEDED' AND request_id=13182 AND result_sha256='860bf6839156ca33b8475e9b7cd6732e949686e3f897f673b8e5905b46ffd110' AND result_sha256=encode(sha256(convert_to(result::text,'UTF8')),'hex'))<>1
 OR (SELECT encode(sha256(convert_to(coalesce(jsonb_agg(to_jsonb(x) ORDER BY probe_attempt),'[]'::jsonb)::text,'UTF8')),'hex') FROM population_top3000_202608_202609_v1.resource_probe_v5 x) IS DISTINCT FROM '6962ca2e67f4f32a53fc17bed86968dc6ffa0695afce940f80e441c241d0427e'
 OR (SELECT count(*) FROM population_top3000_202608_202609_v1.resource_probe_v6)<>1
 OR (SELECT count(*) FROM population_top3000_202608_202609_v1.resource_probe_v6 WHERE probe_attempt=15 AND state='SUCCEEDED' AND request_id=13578 AND result_sha256='697d789e6c2b0d9143fe2db78958b0547757a1e019f6d56391691447b9eb99f6' AND result_sha256=encode(sha256(convert_to(result::text,'UTF8')),'hex'))<>1
 OR (SELECT encode(sha256(convert_to(coalesce(jsonb_agg(to_jsonb(x) ORDER BY probe_attempt),'[]'::jsonb)::text,'UTF8')),'hex') FROM population_top3000_202608_202609_v1.resource_probe_v6 x) IS DISTINCT FROM 'a28e9c88b853d56610dafb602ee7a3dd31ef204ec1ba9915493dda31eb0f35a1'
 OR (SELECT count(*) FROM population_top3000_202608_202609_v1.resource_probe_v7)<>1
 OR (SELECT count(*) FROM population_top3000_202608_202609_v1.resource_probe_v7 WHERE probe_attempt=16 AND state='SUCCEEDED' AND request_id=13757 AND result_sha256='541bed13a4cbcb9187b88a02a96dce28b5d94bc86538ddc08018e65519d5aa53' AND result_sha256=encode(sha256(convert_to(result::text,'UTF8')),'hex'))<>1
 OR (SELECT encode(sha256(convert_to(coalesce(jsonb_agg(to_jsonb(x) ORDER BY probe_attempt),'[]'::jsonb)::text,'UTF8')),'hex') FROM population_top3000_202608_202609_v1.resource_probe_v7 x) IS DISTINCT FROM 'b51f812028e3418e276978b4751f5e85078f8cf90f50e7c752037785cf38324f'
 OR (SELECT encode(sha256(convert_to(coalesce(jsonb_agg(to_jsonb(x)ORDER BY probe_attempt),'[]'::jsonb)::text,'UTF8')),'hex')FROM population_top3000_202608_202609_v1.resource_probe_v8 x)IS DISTINCT FROM'417ea224defbed02ffe929aebd31a93e55582025cd1ab0154c3883b216b3648c'
 OR (EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_event rv WHERE rv.event_id=v.authorization_review_event_id AND rv.event='RESOURCE_V9_MEASUREMENT_AUTHORIZATION_REVIEW'AND rv.details=v.authorization_receipt AND rv.details->>'approved'='true'AND rv.details->>'actual_rollback_passed'='true'AND(rv.details->>'not_after')::timestamptz>clock_timestamp()AND rv.details->>'measurement_id'=v.probe_id::text AND rv.details->'global_code_manifest'=population_top3000_202608_202609_v1.population_recovery_manifest_v1()AND rv.details->>'worker_sha256'='8f68459a785ccffffe6584441c84ea42d775da68a37187076bc35dccf2868bc1'AND rv.details->>'edge_function_slug'='population-top3000-resource-v9'AND rv.details->>'edge_status'='ACTIVE'AND rv.details->'edge_verify_jwt'='false'::jsonb AND rv.details->>'owner_id'=v.owner_id AND rv.details->>'owner_fence'=v.owner_fence::text AND rv.details->>'stop_generation'=v.stop_generation::text AND rv.details->>'scope_sha256'=v.scope_sha256 AND rv.details->>'authority_event_id'='81090'AND rv.details->>'maximum_http_invocations'='1'AND rv.details->>'new_provider_requests'='0'AND rv.details->>'maximum_source_bytes'='0'AND nullif(rv.details->>'producer','')IS NOT NULL AND nullif(rv.details->>'reviewer','')IS NOT NULL AND rv.details->>'producer'IS DISTINCT FROM rv.details->>'reviewer'
AND rv.details->'sql_code_manifest'=(SELECT jsonb_object_agg(fn,encode(sha256(convert_to(pg_get_functiondef(fn::regprocedure),'UTF8')),'hex'))FROM(VALUES('population_top3000_202608_202609_v1.resource_probe_claim_v9(uuid,text)'),('population_top3000_202608_202609_v1.resource_probe_finish_v9(uuid,text,jsonb)'),('population_top3000_202608_202609_v1.resource_probe_dispatch_v9(text,bigint,bigint,text,bigint,uuid)'))f(fn))
AND EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.continuation_handler_v6 h WHERE h.kind='CTX02_PAGE_ADMISSION'AND h.enabled AND h.function_sha256=encode(sha256(convert_to(pg_get_functiondef(h.function_oid),'UTF8')),'hex')AND rv.details->>'handler_function_sha256'=h.function_sha256 AND rv.details->>'handler_bounds_sha256'=encode(sha256(convert_to(h.bounds::text,'UTF8')),'hex'))
AND EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.continuation_clear_queue_v6 q WHERE q.clearing_id=v.clearing_id AND q.family_id='RESOURCE'AND q.status='CLAIMED'AND q.owner_id=v.owner_id AND q.owner_fence=v.owner_fence AND q.stop_generation=v.stop_generation AND q.scope_sha256=v.scope_sha256 AND q.claim_expires_at>clock_timestamp()AND rv.details->>'native_input_sha256'=q.input_sha256 AND q.input_sha256=encode(sha256(convert_to(q.input_manifest::text,'UTF8')),'hex')AND rv.details->>'native_token_sha256'=q.claim_token_sha256 AND(rv.details->>'native_claimed_at')::timestamptz=q.claimed_at AND(rv.details->>'native_expires_at')::timestamptz=q.claim_expires_at)
AND EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_event nc WHERE nc.event_id=(rv.details->>'native_claim_event_id')::bigint AND nc.event='ENGINEERING_CLEARING_CLAIMED_V6'AND nc.details->>'clearing_id'=v.clearing_id AND nc.details->>'input_sha256'=rv.details->>'native_input_sha256')
AND EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_event c JOIN population_top3000_202608_202609_v1.pipeline_event ass ON ass.event_id=(c.details->>'assignment_event_id')::bigint WHERE c.event_id=(rv.details->>'measurement_claim_event_id')::bigint AND c.event='ENGINEERING_ASSIGNMENT_CLAIMED_V5'AND ass.event='ENGINEERING_IMPLEMENTATION_ASSIGNMENT_V5'AND ass.details->>'maximum_http_invocations'='1'AND ass.details->>'new_provider_requests'='0'AND ass.details->>'maximum_source_bytes'='0'AND c.details->>'assignment_event_id'=rv.details->>'measurement_assignment_event_id'AND c.details->>'assignment_details_sha256'=rv.details->>'measurement_assignment_details_sha256'AND encode(sha256(convert_to(ass.details::text,'UTF8')),'hex')=c.details->>'assignment_details_sha256'AND c.details->>'executor'='automation:6adc2c96db348191bca7cf4eacfdac32'AND c.details->>'owner_id'=v.owner_id AND c.details->>'owner_fence'=v.owner_fence::text AND c.details->>'stop_generation'=v.stop_generation::text AND c.details->>'scope_sha256'=v.scope_sha256 AND(c.details->>'expires_at')::timestamptz>clock_timestamp()AND(ass.details->>'not_after')::timestamptz>clock_timestamp()AND NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_event done WHERE done.event IN('ENGINEERING_ASSIGNMENT_COMPLETED_V5','ENGINEERING_ASSIGNMENT_EFFECTS_RECONCILED_RELEASE_V5')AND done.details->>'claim_event_id'=c.event_id::text AND done.details->>'effects_known'='true')))
AND p.budget_evidence#>>'{execution_continuation_policy,authority_event_id}'='81090')IS DISTINCT FROM true
 OR a.http_invocations<v.http_invocation_ordinal OR p.reserved_request_usd<v.encumbered_usd THEN RETURN '{"state":"DENIED"}';END IF;
 UPDATE population_top3000_202608_202609_v1.resource_probe_v9 SET state='RUNNING',claimed_at=clock_timestamp(),claim_deadline_at=clock_timestamp()+interval '30 seconds' WHERE probe_id=p_probe_id;
 RETURN jsonb_build_object('state','CLAIMED','probe_id',p_probe_id,'max_response_bytes',2097152,'total_timeout_ms',30000,'probe_attempt',v.probe_attempt);
END;$f$;

CREATE FUNCTION population_top3000_202608_202609_v1.resource_probe_finish_v9(p_probe_id uuid,p_token text,p_result jsonb)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $f$
DECLARE v population_top3000_202608_202609_v1.resource_probe_v9%rowtype;
 a population_top3000_202608_202609_v1.acquisition_control%rowtype;
 p population_top3000_202608_202609_v1.pipeline_control%rowtype;
 h text;outcome text;err text;http integer;elapsed integer;observed timestamptz;
 metric jsonb;metrics jsonb:='[]'::jsonb;clean jsonb;sz numeric;fr numeric;av numeric;
BEGIN
 IF p_probe_id IS NULL OR p_token IS NULL OR p_token!~'^[0-9a-f]{64}$' THEN RETURN '{"state":"DENIED"}';END IF;
 PERFORM pg_advisory_xact_lock(2026083000261022);
 h:=encode(sha256(convert_to(p_probe_id::text||':'||p_token,'UTF8')),'hex');
 SELECT * INTO v FROM population_top3000_202608_202609_v1.resource_probe_v9 WHERE probe_id=p_probe_id AND token_sha256=h;
 IF NOT FOUND THEN RETURN '{"state":"DENIED"}';END IF;

 SELECT * INTO a FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton FOR UPDATE;
 SELECT * INTO p FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton FOR UPDATE;
 SELECT * INTO v FROM population_top3000_202608_202609_v1.resource_probe_v9 WHERE probe_id=p_probe_id FOR UPDATE;
 IF v.probe_attempt NOT BETWEEN 18 AND 41 OR v.max_runtime_ms IS DISTINCT FROM 30000 OR v.state NOT IN('RUNNING','SUCCEEDED','FAILED')
 OR v.token_sha256 IS DISTINCT FROM h OR(v.state='RUNNING'AND(v.expires_at<=clock_timestamp() OR v.claim_deadline_at IS NULL OR v.claim_deadline_at<=clock_timestamp())) OR (EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_event rv WHERE rv.event_id=v.authorization_review_event_id AND rv.event='RESOURCE_V9_MEASUREMENT_AUTHORIZATION_REVIEW'AND rv.details=v.authorization_receipt AND rv.details->>'approved'='true'AND rv.details->>'actual_rollback_passed'='true'AND(rv.details->>'not_after')::timestamptz>clock_timestamp()AND rv.details->>'measurement_id'=v.probe_id::text AND rv.details->'global_code_manifest'=population_top3000_202608_202609_v1.population_recovery_manifest_v1()AND rv.details->>'worker_sha256'='8f68459a785ccffffe6584441c84ea42d775da68a37187076bc35dccf2868bc1'AND rv.details->>'edge_function_slug'='population-top3000-resource-v9'AND rv.details->>'edge_status'='ACTIVE'AND rv.details->'edge_verify_jwt'='false'::jsonb AND rv.details->>'owner_id'=v.owner_id AND rv.details->>'owner_fence'=v.owner_fence::text AND rv.details->>'stop_generation'=v.stop_generation::text AND rv.details->>'scope_sha256'=v.scope_sha256 AND rv.details->>'authority_event_id'='81090'AND rv.details->>'maximum_http_invocations'='1'AND rv.details->>'new_provider_requests'='0'AND rv.details->>'maximum_source_bytes'='0'AND nullif(rv.details->>'producer','')IS NOT NULL AND nullif(rv.details->>'reviewer','')IS NOT NULL AND rv.details->>'producer'IS DISTINCT FROM rv.details->>'reviewer'
AND rv.details->'sql_code_manifest'=(SELECT jsonb_object_agg(fn,encode(sha256(convert_to(pg_get_functiondef(fn::regprocedure),'UTF8')),'hex'))FROM(VALUES('population_top3000_202608_202609_v1.resource_probe_claim_v9(uuid,text)'),('population_top3000_202608_202609_v1.resource_probe_finish_v9(uuid,text,jsonb)'),('population_top3000_202608_202609_v1.resource_probe_dispatch_v9(text,bigint,bigint,text,bigint,uuid)'))f(fn))
AND EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.continuation_handler_v6 h WHERE h.kind='CTX02_PAGE_ADMISSION'AND h.enabled AND h.function_sha256=encode(sha256(convert_to(pg_get_functiondef(h.function_oid),'UTF8')),'hex')AND rv.details->>'handler_function_sha256'=h.function_sha256 AND rv.details->>'handler_bounds_sha256'=encode(sha256(convert_to(h.bounds::text,'UTF8')),'hex'))
AND EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.continuation_clear_queue_v6 q WHERE q.clearing_id=v.clearing_id AND q.family_id='RESOURCE'AND q.status='CLAIMED'AND q.owner_id=v.owner_id AND q.owner_fence=v.owner_fence AND q.stop_generation=v.stop_generation AND q.scope_sha256=v.scope_sha256 AND q.claim_expires_at>clock_timestamp()AND rv.details->>'native_input_sha256'=q.input_sha256 AND q.input_sha256=encode(sha256(convert_to(q.input_manifest::text,'UTF8')),'hex')AND rv.details->>'native_token_sha256'=q.claim_token_sha256 AND(rv.details->>'native_claimed_at')::timestamptz=q.claimed_at AND(rv.details->>'native_expires_at')::timestamptz=q.claim_expires_at)
AND EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_event nc WHERE nc.event_id=(rv.details->>'native_claim_event_id')::bigint AND nc.event='ENGINEERING_CLEARING_CLAIMED_V6'AND nc.details->>'clearing_id'=v.clearing_id AND nc.details->>'input_sha256'=rv.details->>'native_input_sha256')
AND EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_event c JOIN population_top3000_202608_202609_v1.pipeline_event ass ON ass.event_id=(c.details->>'assignment_event_id')::bigint WHERE c.event_id=(rv.details->>'measurement_claim_event_id')::bigint AND c.event='ENGINEERING_ASSIGNMENT_CLAIMED_V5'AND ass.event='ENGINEERING_IMPLEMENTATION_ASSIGNMENT_V5'AND ass.details->>'maximum_http_invocations'='1'AND ass.details->>'new_provider_requests'='0'AND ass.details->>'maximum_source_bytes'='0'AND c.details->>'assignment_event_id'=rv.details->>'measurement_assignment_event_id'AND c.details->>'assignment_details_sha256'=rv.details->>'measurement_assignment_details_sha256'AND encode(sha256(convert_to(ass.details::text,'UTF8')),'hex')=c.details->>'assignment_details_sha256'AND c.details->>'executor'='automation:6adc2c96db348191bca7cf4eacfdac32'AND c.details->>'owner_id'=v.owner_id AND c.details->>'owner_fence'=v.owner_fence::text AND c.details->>'stop_generation'=v.stop_generation::text AND c.details->>'scope_sha256'=v.scope_sha256 AND(c.details->>'expires_at')::timestamptz>clock_timestamp()AND(ass.details->>'not_after')::timestamptz>clock_timestamp()AND NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_event done WHERE done.event IN('ENGINEERING_ASSIGNMENT_COMPLETED_V5','ENGINEERING_ASSIGNMENT_EFFECTS_RECONCILED_RELEASE_V5')AND done.details->>'claim_event_id'=c.event_id::text AND done.details->>'effects_known'='true')))
AND p.budget_evidence#>>'{execution_continuation_policy,authority_event_id}'='81090')IS DISTINCT FROM true
 OR a.owner_id IS DISTINCT FROM v.owner_id OR a.owner_fence IS DISTINCT FROM v.owner_fence OR a.stop_generation IS DISTINCT FROM v.stop_generation
 OR p.expected_owner_id IS DISTINCT FROM a.owner_id OR p.expected_owner_fence IS DISTINCT FROM a.owner_fence OR p.expected_stop_generation IS DISTINCT FROM a.stop_generation
 OR a.scope_sha256 IS DISTINCT FROM v.scope_sha256 OR p.scope_sha256 IS DISTINCT FROM v.scope_sha256
 OR a.enabled IS DISTINCT FROM true OR a.stopped IS DISTINCT FROM false OR a.hold_reason IS NOT NULL
 OR p.budget_evidence#>>'{monetary_policy,authority_event_id}' IS DISTINCT FROM '9528' OR p.enabled IS DISTINCT FROM true
 OR p.shared_scheduler_verified IS DISTINCT FROM true OR p.deadline IS NULL OR p.deadline<=clock_timestamp() OR p.tick_count>=p.max_ticks
 OR a.admitted_until IS NULL OR a.admitted_until<=clock_timestamp()
 OR (SELECT count(*) FROM population_top3000_202608_202609_v1.resource_probe_v4)<>1
 OR (SELECT count(*) FROM population_top3000_202608_202609_v1.resource_probe_v4 WHERE probe_attempt=13 AND state='SUCCEEDED' AND request_id=12425 AND result_sha256='967ca4e0a9026dabb941a49fc559c6e8f4f63336d6c0ab9ac17226ac91f81c47' AND result_sha256=encode(sha256(convert_to(result::text,'UTF8')),'hex'))<>1
 OR (SELECT encode(sha256(convert_to(coalesce(jsonb_agg(to_jsonb(x) ORDER BY probe_attempt),'[]'::jsonb)::text,'UTF8')),'hex') FROM population_top3000_202608_202609_v1.resource_probe_v1 x) IS DISTINCT FROM 'ef116b47c37ebfb4f8c994749ea5f0cc23aa347aa73080b7147cd82d4a60e5f5'
 OR (SELECT encode(sha256(convert_to(coalesce(jsonb_agg(to_jsonb(x) ORDER BY probe_attempt),'[]'::jsonb)::text,'UTF8')),'hex') FROM population_top3000_202608_202609_v1.resource_probe_v2 x) IS DISTINCT FROM '3d6cdd01a13e29ca608b99e22a456a0be93f38c404bde121be980da160f9a545'
 OR (SELECT encode(sha256(convert_to(coalesce(jsonb_agg(to_jsonb(x) ORDER BY probe_attempt),'[]'::jsonb)::text,'UTF8')),'hex') FROM population_top3000_202608_202609_v1.resource_probe_v3 x) IS DISTINCT FROM '6aa015a9036da5ce400a023492dab8a3e30f8faee5b32f7877f8ccaa3d69df1d'
 OR (SELECT encode(sha256(convert_to(coalesce(jsonb_agg(to_jsonb(x) ORDER BY probe_attempt),'[]'::jsonb)::text,'UTF8')),'hex') FROM population_top3000_202608_202609_v1.resource_probe_v4 x) IS DISTINCT FROM 'ad0330cff82c1edbf8434b83b2b8ce4f9f2e7069d431bcd258d771629ebedd6b'
 OR (SELECT count(*) FROM population_top3000_202608_202609_v1.resource_probe_v5)<>1
 OR (SELECT count(*) FROM population_top3000_202608_202609_v1.resource_probe_v5 WHERE probe_attempt=14 AND state='SUCCEEDED' AND request_id=13182 AND result_sha256='860bf6839156ca33b8475e9b7cd6732e949686e3f897f673b8e5905b46ffd110' AND result_sha256=encode(sha256(convert_to(result::text,'UTF8')),'hex'))<>1
 OR (SELECT encode(sha256(convert_to(coalesce(jsonb_agg(to_jsonb(x) ORDER BY probe_attempt),'[]'::jsonb)::text,'UTF8')),'hex') FROM population_top3000_202608_202609_v1.resource_probe_v5 x) IS DISTINCT FROM '6962ca2e67f4f32a53fc17bed86968dc6ffa0695afce940f80e441c241d0427e'
 OR (SELECT count(*) FROM population_top3000_202608_202609_v1.resource_probe_v6)<>1
 OR (SELECT count(*) FROM population_top3000_202608_202609_v1.resource_probe_v6 WHERE probe_attempt=15 AND state='SUCCEEDED' AND request_id=13578 AND result_sha256='697d789e6c2b0d9143fe2db78958b0547757a1e019f6d56391691447b9eb99f6' AND result_sha256=encode(sha256(convert_to(result::text,'UTF8')),'hex'))<>1
 OR (SELECT encode(sha256(convert_to(coalesce(jsonb_agg(to_jsonb(x) ORDER BY probe_attempt),'[]'::jsonb)::text,'UTF8')),'hex') FROM population_top3000_202608_202609_v1.resource_probe_v6 x) IS DISTINCT FROM 'a28e9c88b853d56610dafb602ee7a3dd31ef204ec1ba9915493dda31eb0f35a1'
 OR (SELECT count(*) FROM population_top3000_202608_202609_v1.resource_probe_v7)<>1
 OR (SELECT count(*) FROM population_top3000_202608_202609_v1.resource_probe_v7 WHERE probe_attempt=16 AND state='SUCCEEDED' AND request_id=13757 AND result_sha256='541bed13a4cbcb9187b88a02a96dce28b5d94bc86538ddc08018e65519d5aa53' AND result_sha256=encode(sha256(convert_to(result::text,'UTF8')),'hex'))<>1
 OR (SELECT encode(sha256(convert_to(coalesce(jsonb_agg(to_jsonb(x) ORDER BY probe_attempt),'[]'::jsonb)::text,'UTF8')),'hex') FROM population_top3000_202608_202609_v1.resource_probe_v7 x) IS DISTINCT FROM 'b51f812028e3418e276978b4751f5e85078f8cf90f50e7c752037785cf38324f'
 OR (SELECT encode(sha256(convert_to(coalesce(jsonb_agg(to_jsonb(x)ORDER BY probe_attempt),'[]'::jsonb)::text,'UTF8')),'hex')FROM population_top3000_202608_202609_v1.resource_probe_v8 x)IS DISTINCT FROM'417ea224defbed02ffe929aebd31a93e55582025cd1ab0154c3883b216b3648c'
 THEN RETURN '{"state":"DENIED"}';END IF;
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
 IF v.state IN('SUCCEEDED','FAILED')THEN IF v.result IS DISTINCT FROM clean THEN RAISE EXCEPTION 'RESOURCE_V9_DIFFERENT_RESULT_REPLAY';END IF;RETURN jsonb_build_object('state','ALREADY_RECORDED','outcome',v.state,'probe_id',p_probe_id);END IF;
 UPDATE population_top3000_202608_202609_v1.resource_probe_v9 SET state=outcome,finished_at=clock_timestamp(),result=clean,result_sha256=encode(sha256(convert_to(clean::text,'UTF8')),'hex') WHERE probe_id=p_probe_id;
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES('RESOURCE_PROBE_V9_RECORDED',jsonb_build_object('probe_id',p_probe_id,'outcome',outcome,'owner_fence',v.owner_fence,'metrics_http_status',http,'filesystem_count',jsonb_array_length(metrics),'probe_attempt',v.probe_attempt,'no_capacity_change',true,'budget_encumbrance_released',false));
 RETURN jsonb_build_object('state','RECORDED','outcome',outcome,'probe_id',p_probe_id);
END;$f$;

REVOKE ALL ON FUNCTION population_top3000_202608_202609_v1.resource_probe_claim_v9(uuid,text),population_top3000_202608_202609_v1.resource_probe_finish_v9(uuid,text,jsonb) FROM public,anon,authenticated,service_role;
