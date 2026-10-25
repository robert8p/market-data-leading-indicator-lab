-- Exact47214 implementation. No source admission or worker activation is granted.
CREATE TABLE population_top3000_202608_202609_v1.ov4_pin(id boolean PRIMARY KEY DEFAULT true CHECK(id),worker_sha text NOT NULL CHECK(worker_sha~'^[0-9a-f]{64}$'),assignment_id bigint NOT NULL,claim_id bigint NOT NULL,input_sha text NOT NULL CHECK(input_sha='c6a21603b1015a3548de6e41b30ccdeb31c81b566f330e581d9ffc0a138cd57f'));
CREATE TABLE population_top3000_202608_202609_v1.ov4_request(k text PRIMARY KEY CHECK(k~'^[0-9a-f]{64}$'),ordinal int NOT NULL UNIQUE CHECK(ordinal BETWEEN 1 AND 200),r jsonb NOT NULL,case_refs jsonb NOT NULL CHECK(jsonb_typeof(case_refs)='array' AND jsonb_array_length(case_refs)>0),request_sha text NOT NULL CHECK(request_sha=encode(sha256(convert_to(r::text,'UTF8')),'hex')));
CREATE TABLE population_top3000_202608_202609_v1.ov4_cap(k text PRIMARY KEY REFERENCES population_top3000_202608_202609_v1.ov4_request,attempt_id uuid NOT NULL UNIQUE REFERENCES population_top3000_202608_202609_v1.pipeline_attempt,admission bigint NOT NULL,token_sha text NOT NULL,worker_sha text NOT NULL,issued_at timestamptz NOT NULL DEFAULT clock_timestamp());
CREATE TABLE population_top3000_202608_202609_v1.ov4_spool(k text PRIMARY KEY REFERENCES population_top3000_202608_202609_v1.ov4_cap,body bytea NOT NULL CHECK(octet_length(body)<=1048576),sha text NOT NULL CHECK(sha=encode(sha256(body),'hex')),status int NOT NULL CHECK(status BETWEEN 100 AND 599),receipt jsonb NOT NULL CHECK(octet_length(receipt::text)<=8192),received_at timestamptz NOT NULL DEFAULT clock_timestamp());
CREATE TABLE population_top3000_202608_202609_v1.ov4_spool_budget(id boolean PRIMARY KEY DEFAULT true CHECK(id),bytes bigint NOT NULL DEFAULT 0 CHECK(bytes BETWEEN 0 AND 33554432));
INSERT INTO population_top3000_202608_202609_v1.ov4_spool_budget VALUES(true,0);
CREATE TABLE population_top3000_202608_202609_v1.ov4_point(k text PRIMARY KEY REFERENCES population_top3000_202608_202609_v1.ov4_spool,point jsonb NOT NULL,sha text NOT NULL CHECK(sha=encode(sha256(convert_to(point::text,'UTF8')),'hex')));
CREATE TABLE population_top3000_202608_202609_v1.ov4_batch(admission bigint PRIMARY KEY,task_id uuid NOT NULL UNIQUE REFERENCES population_top3000_202608_202609_v1.pipeline_task,checkpoint int NOT NULL DEFAULT 1 CHECK(checkpoint BETWEEN 1 AND 201),wake_token text NOT NULL DEFAULT gen_random_uuid()::text||gen_random_uuid()::text,dispatches int NOT NULL DEFAULT 0 CHECK(dispatches BETWEEN 0 AND 100),wake_ordinal int NOT NULL DEFAULT 0,last_request_id bigint);
CREATE FUNCTION population_top3000_202608_202609_v1.ov4_bundle() RETURNS text LANGUAGE sql STABLE SET search_path='pg_catalog' AS $$SELECT encode(sha256(convert_to(string_agg(proname||':'||pg_get_functiondef(oid),E'\n' ORDER BY proname),'UTF8')),'hex') FROM pg_proc WHERE pronamespace='population_top3000_202608_202609_v1'::regnamespace AND proname LIKE 'ov4_%'$$;
CREATE FUNCTION population_top3000_202608_202609_v1.ov4_guard(aid bigint) RETURNS jsonb LANGUAGE plpgsql SET search_path='pg_catalog' AS $$
DECLARE c jsonb;d jsonb;rv jsonb;pin record;BEGIN
 c:=population_top3000_202608_202609_v1.continuation_guard_v6('top3000-completion-20261024-root-5db48b32d103',3,1);
 SELECT * INTO STRICT pin FROM population_top3000_202608_202609_v1.ov4_pin WHERE id;
 SELECT details INTO d FROM population_top3000_202608_202609_v1.pipeline_event WHERE event_id=aid AND event='REF01_OVERVIEW_FINITE_SOURCE_ADMITTED_V4';
 SELECT details INTO rv FROM population_top3000_202608_202609_v1.pipeline_event WHERE event_id=(d->>'review_event_id')::bigint AND event='REF01_OVERVIEW_FINITE_SOURCE_REVIEW_V4';
 IF d IS NULL OR rv IS NULL OR rv->>'approved' IS DISTINCT FROM 'true' OR rv->>'producer' IS NOT DISTINCT FROM rv->>'reviewer' OR nullif(rv->>'reviewer','') IS NULL OR rv->>'proposal_sha256' IS DISTINCT FROM encode(sha256(convert_to((d-'review_event_id')::text,'UTF8')),'hex') OR d->>'worker_sha256' IS DISTINCT FROM pin.worker_sha OR d->>'bundle_sha256' IS DISTINCT FROM population_top3000_202608_202609_v1.ov4_bundle() OR d->>'request_manifest_sha256' IS DISTINCT FROM '13dafeef5c83990b6c73f8c8d9a7edcbfd81bff22b2f3e268e38d26d5c863bef' OR d->>'input_manifest_sha256' IS DISTINCT FROM pin.input_sha OR d->>'owner_id' IS DISTINCT FROM c->>'owner_id' OR d->>'owner_fence' IS DISTINCT FROM '3' OR d->>'stop_generation' IS DISTINCT FROM '1' OR d->>'scope_sha256' IS DISTINCT FROM c->>'scope_sha256' OR d->>'not_after' IS NULL OR (d->>'not_after')::timestamptz<=clock_timestamp() OR d->>'max_provider_calls' IS DISTINCT FROM '100' OR d->>'max_http_calls' IS DISTINCT FROM '200' OR d->>'max_source_bytes' IS DISTINCT FROM '33554432' OR d->>'max_relation_bytes' IS DISTINCT FROM '33554432' OR d->>'automatic_retry' IS DISTINCT FROM 'false' THEN RAISE EXCEPTION 'OV4_REVIEWED_FINITE_ADMISSION_REQUIRED';END IF;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_event e WHERE event_id=pin.claim_id AND event='ENGINEERING_ASSIGNMENT_CLAIMED_V5' AND details->>'assignment_event_id'=pin.assignment_id::text AND details->>'executor'='automation:6adc2c96db348191bca7cf4eacfdac32') THEN RAISE EXCEPTION 'OV4_ENGINEERING_PROVENANCE_REQUIRED';END IF;
 IF (SELECT sum(pg_total_relation_size(oid)) FROM pg_class WHERE relnamespace='population_top3000_202608_202609_v1'::regnamespace AND relname LIKE 'ov4_%')>33554432 THEN RAISE EXCEPTION 'OV4_RELATION_BOUND';END IF;
 IF (SELECT budget_evidence#>>'{cohort_selection_policy,policy_sha256}' FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton) IS DISTINCT FROM '3e49d661fcdb8533e5ce88a85689b41e9dde71eeae2dc8578c9e31f9ae1738bb' THEN RAISE EXCEPTION 'OV4_POLICY_CHANGED';END IF;
 RETURN d;
END;$$;
CREATE FUNCTION population_top3000_202608_202609_v1.ov4_seed(worker_sha text) RETURNS jsonb LANGUAGE plpgsql SET search_path='pg_catalog' AS $$
DECLARE a jsonb;c jsonb;BEGIN
 PERFORM population_top3000_202608_202609_v1.continuation_guard_v6('top3000-completion-20261024-root-5db48b32d103',3,1);
 SELECT details INTO STRICT a FROM population_top3000_202608_202609_v1.pipeline_event WHERE event_id=47214 AND event='ENGINEERING_IMPLEMENTATION_ASSIGNMENT_V5';
 SELECT details INTO STRICT c FROM population_top3000_202608_202609_v1.pipeline_event WHERE event_id=48427 AND event='ENGINEERING_ASSIGNMENT_CLAIMED_V5';
 IF encode(sha256(convert_to(a::text,'UTF8')),'hex')<>'5afcba653b1462a4e8d16a06ab6e9b5307f4717120418271ae15fb3650d9dfb0' OR c->>'assignment_event_id'<>'47214' OR (c->>'expires_at')::timestamptz<=clock_timestamp() OR worker_sha!~'^[0-9a-f]{64}$' THEN RAISE EXCEPTION 'OV4_CURRENT_EXACT_CLAIM_REQUIRED';END IF;
 IF encode(sha256(convert_to((a#>'{input_manifest,exact_request_manifest}')::text,'UTF8')),'hex')<>'13dafeef5c83990b6c73f8c8d9a7edcbfd81bff22b2f3e268e38d26d5c863bef' THEN RAISE EXCEPTION 'OV4_MANIFEST_CHANGED';END IF;
 INSERT INTO population_top3000_202608_202609_v1.ov4_pin VALUES(true,worker_sha,47214,48427,a->>'input_manifest_sha256');
 INSERT INTO population_top3000_202608_202609_v1.ov4_request SELECT encode(sha256(convert_to(v::text,'UTF8')),'hex'),n,v,(SELECT jsonb_agg(cp) FROM jsonb_array_elements(a#>'{input_manifest,private_case_manifest}') q(cp) JOIN population_top3000_202608_202609_v1.ref01_practical_instrument_point_v3 ap ON ap.content->>'company_id'=cp->>'company_id' AND ap.ticker=v->>'ticker' AND ap.point_date=DATE '2026-08-03' AND ap.eligible AND ap.cohort_sha256='2dc0f766befed6de6b052221c3c87482d507ba1bad931cbb98654eb3af899bec' AND ap.record_sha256=cp->>'expected_anchor_sha' AND ap.record_sha256=encode(sha256(convert_to(ap.content::text,'UTF8')),'hex') WHERE cp->>'point_date'=v->>'point_date'),encode(sha256(convert_to(v::text,'UTF8')),'hex') FROM jsonb_array_elements(a#>'{input_manifest,exact_request_manifest}') WITH ORDINALITY t(v,n);
 IF (SELECT count(*) FROM population_top3000_202608_202609_v1.ref01_practical_instrument_point_v3 WHERE point_date=DATE '2026-08-03' AND eligible AND cohort_sha256='2dc0f766befed6de6b052221c3c87482d507ba1bad931cbb98654eb3af899bec')<>3000 THEN RAISE EXCEPTION 'OV4_FROZEN_3000_DENOMINATOR_REQUIRED';END IF;
 IF (SELECT sum(jsonb_array_length(case_refs)) FROM population_top3000_202608_202609_v1.ov4_request)<>200 THEN RAISE EXCEPTION 'OV4_EXACT_200_CASE_ANCHOR_BINDINGS_REQUIRED';END IF;
 IF (SELECT count(*) FROM population_top3000_202608_202609_v1.ov4_request)<>200 OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.ov4_request WHERE r->>'exact_url'<>'https://api.massive.com/v3/reference/tickers/'||(r->>'ticker')||'?date='||(r->>'point_date') OR r->>'ticker'!~'^[A-Za-z0-9._-]{1,64}$') THEN RAISE EXCEPTION 'OV4_200_EXACT_REQUESTS_REQUIRED';END IF;
 RETURN '{"state":"SEEDED_200_UNADMITTED","http":0,"provider":0}'::jsonb;
END;$$;
CREATE FUNCTION population_top3000_202608_202609_v1.ov4_preflight(aid bigint) RETURNS jsonb LANGUAGE plpgsql STABLE SET search_path='pg_catalog' AS $$
DECLARE b record;r record;BEGIN
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.acquisition_control a CROSS JOIN population_top3000_202608_202609_v1.pipeline_control p WHERE a.singleton AND p.singleton AND a.enabled AND NOT a.stopped AND a.hold_reason IS NULL AND p.enabled AND a.owner_id='top3000-completion-20261024-root-5db48b32d103' AND a.owner_fence=3 AND a.stop_generation=1 AND a.admitted_until>clock_timestamp() AND p.deadline>clock_timestamp() AND p.tick_count<p.max_ticks) THEN RETURN '{"state":"QUIET_CONTROL_WAIT"}'::jsonb;END IF;
 SELECT * INTO b FROM population_top3000_202608_202609_v1.ov4_batch WHERE admission=aid;
 IF FOUND AND b.checkpoint=201 THEN RETURN '{"state":"COMPLETE"}'::jsonb;END IF;
 SELECT * INTO r FROM population_top3000_202608_202609_v1.ov4_request WHERE ordinal=coalesce(b.checkpoint,1);
 IF r.k IS NULL THEN RETURN '{"state":"UNSEEDED"}'::jsonb;END IF;
 RETURN jsonb_build_object('state',CASE WHEN EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.ov4_spool WHERE k=r.k) THEN 'BODY_RETAINED' WHEN EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.ov4_cap WHERE k=r.k) THEN 'EXISTING_ATTEMPT_HELD_NO_REISSUE' ELSE 'ELIGIBLE' END,'request_key',r.k);
END;$$;
CREATE FUNCTION population_top3000_202608_202609_v1.ov4_claim(aid bigint,wsha text) RETURNS jsonb LANGUAGE plpgsql SET search_path='pg_catalog' AS $$
DECLARE d jsonb;a record;p record;b record;r record;tid uuid;at uuid;tok text;pin record;taskrow record;ex bigint;BEGIN
 IF NOT pg_try_advisory_xact_lock(2026083000261022) THEN RETURN '{"state":"BUSY"}'::jsonb;END IF;
 d:=population_top3000_202608_202609_v1.ov4_guard(aid); -- Controls are always first.
 SELECT * INTO STRICT pin FROM population_top3000_202608_202609_v1.ov4_pin WHERE id;
 IF wsha IS DISTINCT FROM pin.worker_sha THEN RAISE EXCEPTION 'OV4_DEPLOYED_CODE_CHANGED';END IF;
 SELECT * INTO STRICT a FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton;
 SELECT * INTO STRICT p FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton;
 IF a.next_provider_at>clock_timestamp() OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_provider_lease WHERE released_at IS NULL) OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.connector_probe_lease_v1 WHERE released_at IS NULL) OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.acquisition_job WHERE status='LEASED' AND lease_until>clock_timestamp()) THEN RETURN '{"state":"QUIET_SHARED_PROVIDER_WAIT"}'::jsonb;END IF;
 SELECT * INTO b FROM population_top3000_202608_202609_v1.ov4_batch WHERE admission=aid FOR UPDATE;
 IF NOT FOUND THEN RAISE EXCEPTION 'OV4_START_REQUIRED';END IF;
 IF b.checkpoint=201 THEN RETURN '{"state":"COMPLETE"}'::jsonb;END IF;
 SELECT * INTO STRICT r FROM population_top3000_202608_202609_v1.ov4_request WHERE ordinal=b.checkpoint;
 IF EXISTS(SELECT 1 FROM jsonb_array_elements(r.case_refs) q(cp) WHERE NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.ref01_practical_instrument_point_v3 ap WHERE ap.content->>'company_id'=cp->>'company_id' AND ap.ticker=r.r->>'ticker' AND ap.point_date=DATE '2026-08-03' AND ap.eligible AND ap.cohort_sha256='2dc0f766befed6de6b052221c3c87482d507ba1bad931cbb98654eb3af899bec' AND ap.record_sha256=cp->>'expected_anchor_sha' AND ap.record_sha256=encode(sha256(convert_to(ap.content::text,'UTF8')),'hex'))) THEN RAISE EXCEPTION 'OV4_STALE_COMPANY_ANCHOR_PIN';END IF;
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.ov4_cap WHERE k=r.k) THEN RETURN jsonb_build_object('state','EXISTING_ATTEMPT_HELD_NO_REISSUE','request_key',r.k);END IF;
 IF (SELECT sum(pg_total_relation_size(oid)) FROM pg_class WHERE relnamespace='population_top3000_202608_202609_v1'::regnamespace AND relname LIKE 'ov4_%')+3145728>33554432 THEN RETURN '{"state":"FINITE_RELATION_WAIT"}'::jsonb;END IF;
 IF a.provider_requests>=a.max_provider_requests OR a.http_invocations>=a.max_http_invocations OR a.stored_bytes+a.reserved_bytes+1048576>a.max_source_bytes OR (SELECT count(*) FROM population_top3000_202608_202609_v1.ov4_cap WHERE admission=aid)>=100 OR ((SELECT bytes FROM population_top3000_202608_202609_v1.ov4_spool_budget WHERE id)+(SELECT coalesce(sum(l.reserved_bytes),0) FROM population_top3000_202608_202609_v1.pipeline_provider_lease l JOIN population_top3000_202608_202609_v1.ov4_cap c USING(attempt_id) WHERE l.released_at IS NULL)+1048576)>33554432 OR (SELECT sum(pg_total_relation_size(oid)) FROM pg_class WHERE relnamespace='population_top3000_202608_202609_v1'::regnamespace AND relkind IN('r','m'))+3145728>=(p.budget_evidence#>>'{resource_limits,max_relation_bytes}')::bigint THEN RETURN '{"state":"FINITE_RESOURCE_WAIT"}'::jsonb;END IF;
 SELECT * INTO STRICT taskrow FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=b.task_id FOR UPDATE;
 IF taskrow.status<>'BLOCKED' OR taskrow.executions>=taskrow.max_executions OR b.last_request_id IS NULL THEN RAISE EXCEPTION 'OV4_NATIVE_TASK_EXECUTION_OR_DISPATCH_REQUIRED';END IF;
 ex:=taskrow.executions+1;
 at:=gen_random_uuid();tok:=gen_random_uuid()::text||gen_random_uuid()::text;
 INSERT INTO population_top3000_202608_202609_v1.pipeline_attempt(attempt_id,task_id,execution_number,token_sha256,owner_fence,stop_generation,adapter_sha256,request_id) VALUES(at,b.task_id,ex,encode(sha256(convert_to(tok,'UTF8')),'hex'),3,1,encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.ov4_claim(bigint,text)'::regprocedure),'UTF8')),'hex'),b.last_request_id);
 INSERT INTO population_top3000_202608_202609_v1.ov4_cap VALUES(r.k,at,aid,encode(sha256(convert_to(tok,'UTF8')),'hex'),wsha,clock_timestamp());
 INSERT INTO population_top3000_202608_202609_v1.pipeline_provider_lease(attempt_id,provider,reserved_bytes,reserved_usd) VALUES(at,'Massive',1048576,0);
 UPDATE population_top3000_202608_202609_v1.pipeline_task SET status='LEASED',attempt_id=at,lease_until=(d->>'not_after')::timestamptz,executions=ex,owner_fence=3,stop_generation=1,started_at=coalesce(started_at,clock_timestamp()) WHERE task_id=b.task_id;
 UPDATE population_top3000_202608_202609_v1.acquisition_control SET provider_requests=provider_requests+1,http_invocations=http_invocations+1,reserved_bytes=reserved_bytes+1048576,next_provider_at=clock_timestamp()+interval '15 seconds' WHERE singleton;
 RETURN jsonb_build_object('state','CLAIMED','token',tok,'request',r.r||jsonb_build_object('request_key',r.k,'request_manifest_sha256',d->>'request_manifest_sha256','scope_sha256',d->>'scope_sha256','request_state','ADMITTED'));
END;$$;
-- Independent committed RPC: no control, task, attempt or provider-lease lock.
-- Capability permits only retaining the already reserved response. It cannot issue HTTP,
-- release reservations, normalize, advance a task or qualify facts.
CREATE FUNCTION population_top3000_202608_202609_v1.ov4_spool_save(k0 text,tok text,receipt text,wsha text) RETURNS jsonb LANGUAGE plpgsql SET search_path='pg_catalog' AS $$
DECLARE c record;j jsonb;b bytea;h text;n int;old record;BEGIN
 SELECT * INTO STRICT c FROM population_top3000_202608_202609_v1.ov4_cap WHERE k=k0;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.acquisition_control a CROSS JOIN population_top3000_202608_202609_v1.pipeline_control p CROSS JOIN population_top3000_202608_202609_v1.ov4_pin pin WHERE a.singleton AND p.singleton AND pin.id AND a.owner_id='top3000-completion-20261024-root-5db48b32d103' AND a.owner_fence=3 AND a.stop_generation=1 AND a.enabled AND NOT a.stopped AND a.hold_reason IS NULL AND p.enabled AND p.expected_owner_id=a.owner_id AND p.expected_owner_fence=3 AND p.expected_stop_generation=1 AND a.scope_sha256=p.scope_sha256 AND a.scope_sha256='8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd' AND p.budget_evidence#>>'{cohort_selection_policy,policy_sha256}'='3e49d661fcdb8533e5ce88a85689b41e9dde71eeae2dc8578c9e31f9ae1738bb' AND pin.worker_sha=wsha AND pin.worker_sha=c.worker_sha) THEN RAISE EXCEPTION 'OV4_SPOOL_CURRENT_OWNER_STOP_POLICY_CODE_REQUIRED';END IF;
 IF c.token_sha IS DISTINCT FROM encode(sha256(convert_to(tok,'UTF8')),'hex') OR c.worker_sha IS DISTINCT FROM wsha THEN RAISE EXCEPTION 'OV4_SPOOL_CAPABILITY_REJECTED';END IF;
 IF octet_length(receipt)>1500000 THEN RAISE EXCEPTION 'OV4_RECEIPT_BOUND';END IF;j:=receipt::jsonb;b:=decode(j->>'body_base64','base64');n:=octet_length(b);h:=encode(sha256(b),'hex');
 IF n>1048576 OR j->>'sha256' IS DISTINCT FROM h OR j->>'bytes' IS DISTINCT FROM n::text OR j->>'physical_http_count' IS DISTINCT FROM '1' OR j->>'representation' IS DISTINCT FROM 'FETCH_RESPONSE_BODY_BYTES_NOT_ORIGINAL_HTTP_WIRE' THEN RAISE EXCEPTION 'OV4_SOURCE_HASH_OR_BOUND_REJECTED';END IF;
 PERFORM 1 FROM population_top3000_202608_202609_v1.ov4_spool_budget WHERE id FOR UPDATE;
 SELECT * INTO old FROM population_top3000_202608_202609_v1.ov4_spool WHERE k=k0;
 IF FOUND THEN IF old.body IS DISTINCT FROM b OR old.receipt IS DISTINCT FROM j-'body_base64' THEN RAISE EXCEPTION 'OV4_IMMUTABLE_SPOOL_CONFLICT';END IF;RETURN jsonb_build_object('state','BODY_ALREADY_RETAINED','sha256',h);END IF;
 UPDATE population_top3000_202608_202609_v1.ov4_spool_budget SET bytes=bytes+n WHERE id AND bytes+n<=33554432;IF NOT FOUND THEN RAISE EXCEPTION 'OV4_SPOOL_CUMULATIVE_BOUND';END IF;
 INSERT INTO population_top3000_202608_202609_v1.ov4_spool VALUES(k0,b,h,(j->>'http_status')::int,j-'body_base64',clock_timestamp());
 RETURN jsonb_build_object('state','BODY_RETAINED','sha256',h,'original_historical_receipt_proven',false);
END;$$;
CREATE FUNCTION population_top3000_202608_202609_v1.ov4_consume(k0 text,aid bigint) RETURNS jsonb LANGUAGE plpgsql SET search_path='pg_catalog' AS $$
DECLARE d jsonb;c record;s record;r record;a record;t record;b record;l record;doc jsonb;v jsonb;h text;BEGIN
 d:=population_top3000_202608_202609_v1.ov4_guard(aid);
 SELECT * INTO STRICT c FROM population_top3000_202608_202609_v1.ov4_cap WHERE k=k0 AND admission=aid;
 SELECT * INTO STRICT a FROM population_top3000_202608_202609_v1.pipeline_attempt WHERE attempt_id=c.attempt_id FOR UPDATE;
 IF a.outcome='COMPLETE' AND EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.ov4_point p JOIN population_top3000_202608_202609_v1.pipeline_commit m ON m.attempt_id=a.attempt_id AND m.output_sha256=p.sha WHERE p.k=k0 AND p.sha=encode(sha256(convert_to(p.point::text,'UTF8')),'hex')) THEN RETURN jsonb_build_object('state','POINT_ALREADY_RETAINED','request_key',k0);END IF;
 SELECT * INTO STRICT t FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=a.task_id FOR UPDATE;
 IF a.adapter_sha256 IS DISTINCT FROM encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.ov4_claim(bigint,text)'::regprocedure),'UTF8')),'hex') OR a.owner_fence<>3 OR a.stop_generation<>1 OR t.attempt_id IS DISTINCT FROM a.attempt_id OR a.execution_number IS DISTINCT FROM t.executions OR t.lease_until IS NULL OR t.lease_until<=clock_timestamp() OR a.request_id IS NULL OR c.worker_sha IS DISTINCT FROM d->>'worker_sha256' THEN RAISE EXCEPTION 'OV4_ATTEMPT_TASK_CODE_BINDING';END IF;
 SELECT * INTO STRICT s FROM population_top3000_202608_202609_v1.ov4_spool WHERE k=k0;
 SELECT * INTO STRICT r FROM population_top3000_202608_202609_v1.ov4_request WHERE k=k0;
 IF s.sha IS DISTINCT FROM encode(sha256(s.body),'hex') THEN RAISE EXCEPTION 'OV4_SOURCE_CHANGED';END IF;
 IF a.outcome='COMPLETE' THEN RETURN jsonb_build_object('state','POINT_ALREADY_RETAINED','request_key',k0);END IF;
 IF a.outcome<>'RUNNING' OR t.status<>'LEASED' THEN RAISE EXCEPTION 'OV4_ATTEMPT_NOT_RUNNING';END IF;
 IF s.status<>200 THEN RETURN '{"state":"BODY_RETAINED_HTTP_QUALITY_HOLD","automatic_retry":false}'::jsonb;END IF;
 BEGIN doc:=convert_from(s.body,'UTF8')::jsonb;EXCEPTION WHEN OTHERS THEN RETURN '{"state":"BODY_RETAINED_ENCODING_JSON_HOLD","automatic_retry":false}'::jsonb;END;
 IF doc->>'status' IS DISTINCT FROM 'OK' OR jsonb_typeof(doc->'results') IS DISTINCT FROM 'object' OR doc#>>'{results,ticker}' IS DISTINCT FROM r.r->>'ticker' OR nullif(doc->>'next_url','') IS NOT NULL THEN RETURN '{"state":"BODY_RETAINED_IDENTITY_HOLD","automatic_retry":false}'::jsonb;END IF;
 v:=jsonb_build_object('request',r.r,'case_refs',r.case_refs,'source_record',doc->'results','source_sha256',s.sha,'request_sha256',r.request_sha,'evidence_semantics','DATE_QUALIFIED_VENDOR_OVERVIEW_POINT_NOT_INTERVAL_V4','original_historical_receipt_proven',false,'intervals_qualified',false,'family_accepted',false);h:=encode(sha256(convert_to(v::text,'UTF8')),'hex');
 INSERT INTO population_top3000_202608_202609_v1.ov4_point VALUES(k0,v,h);
 SELECT * INTO STRICT l FROM population_top3000_202608_202609_v1.pipeline_provider_lease WHERE attempt_id=a.attempt_id AND released_at IS NULL FOR UPDATE;
 UPDATE population_top3000_202608_202609_v1.acquisition_control SET reserved_bytes=reserved_bytes-l.reserved_bytes,stored_bytes=stored_bytes+octet_length(s.body) WHERE singleton AND reserved_bytes>=l.reserved_bytes;IF NOT FOUND THEN RAISE EXCEPTION 'OV4_COUNTER_RESERVATION_MISMATCH';END IF;
 UPDATE population_top3000_202608_202609_v1.pipeline_provider_lease SET released_at=clock_timestamp(),received_bytes=octet_length(s.body),outcome='NATIVE_SINGLE_HTTP_BODY_RETAINED' WHERE attempt_id=a.attempt_id;
 INSERT INTO population_top3000_202608_202609_v1.pipeline_commit(attempt_id,task_id,commit_key,output_sha256,receipt,new_committed_units) VALUES(a.attempt_id,a.task_id,'ov4-point:'||k0,h,jsonb_build_object('execution_binding','REVIEWED_PRIVATE_NATIVE_FUNCTION','worker_sha256',c.worker_sha,'source_sha256',s.sha,'point_only',true),1);
 UPDATE population_top3000_202608_202609_v1.pipeline_attempt SET outcome='COMPLETE',ended_at=clock_timestamp() WHERE attempt_id=a.attempt_id;
 UPDATE population_top3000_202608_202609_v1.ov4_batch SET checkpoint=checkpoint+1 WHERE admission=aid RETURNING * INTO b;
 UPDATE population_top3000_202608_202609_v1.pipeline_task SET status=CASE WHEN b.checkpoint=201 OR t.executions>=t.max_executions THEN 'COMPLETE' ELSE 'BLOCKED' END,blocker_code=CASE WHEN b.checkpoint=201 OR t.executions>=t.max_executions THEN NULL ELSE 'OV4_PRIVATE_NATIVE_CONTINUATION' END,resumption_condition=jsonb_build_object('route','ov4_claim','admission',aid,'next_ordinal',b.checkpoint),checkpoint=jsonb_build_object('next_ordinal',b.checkpoint),useful_committed_units=useful_committed_units+1,last_useful_progress_at=clock_timestamp(),completed_at=CASE WHEN b.checkpoint=201 OR t.executions>=t.max_executions THEN clock_timestamp() ELSE NULL END WHERE task_id=a.task_id;
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,task_id,attempt_id,details) VALUES('REF01_OVERVIEW_CONTINUATION_REQUIRED_V4',a.task_id,a.attempt_id,jsonb_build_object('admission',aid,'checkpoint',b.checkpoint,'existing_executor','automation:6adc2c96db348191bca7cf4eacfdac32','route','ov4_claim','no_automatic_http_retry',true));
 PERFORM population_top3000_202608_202609_v1.ov4_dispatch(aid);
 RETURN jsonb_build_object('state','POINT_RETAINED','request_key',k0,'record_sha256',h,'next_ordinal',b.checkpoint,'family_accepted',false);
END;$$;
CREATE FUNCTION population_top3000_202608_202609_v1.ov4_unknown(k0 text,tok text,why text) RETURNS jsonb LANGUAGE plpgsql SET search_path='pg_catalog' AS $$
DECLARE c record;BEGIN
 SELECT * INTO STRICT c FROM population_top3000_202608_202609_v1.ov4_cap WHERE k=k0;
 IF c.token_sha IS DISTINCT FROM encode(sha256(convert_to(tok,'UTF8')),'hex') THEN RAISE EXCEPTION 'OV4_CAPABILITY_REQUIRED';END IF;
 -- No lease release or retry; uncertain external evidence remains separate.
 RETURN jsonb_build_object('state','UNKNOWN_EXTERNAL_OUTCOME_HELD','request_key',k0,'automatic_retry',false,'lease_released',false,'reason',left(why,128));
END;$$;

CREATE FUNCTION population_top3000_202608_202609_v1.ov4_dispatch(aid bigint) RETURNS jsonb LANGUAGE plpgsql SET search_path='pg_catalog' AS $$
DECLARE d jsonb;b record;rid bigint;BEGIN
 d:=population_top3000_202608_202609_v1.ov4_guard(aid);
 SELECT * INTO STRICT b FROM population_top3000_202608_202609_v1.ov4_batch WHERE admission=aid FOR UPDATE;
 IF b.checkpoint=201 OR b.wake_ordinal>=b.checkpoint OR b.dispatches>=100 OR (SELECT count(*) FROM population_top3000_202608_202609_v1.ov4_cap WHERE admission=aid)>=100 THEN RETURN '{"state":"FINITE_TRANCHE_COMPLETE_OR_HELD","automatic_retry":false}'::jsonb;END IF;
 IF (SELECT http_invocations>=max_http_invocations FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton) THEN RETURN '{"state":"HTTP_CAP_HELD"}'::jsonb;END IF;
 SELECT net.http_post(url:='https://oxzabweahkoimtevbbny.supabase.co/functions/v1/population-top3000-overview-v4',headers:='{"Content-Type":"application/json"}'::jsonb,body:=jsonb_build_object('admission_event_id',aid,'wake_token',b.wake_token),timeout_milliseconds:=55000) INTO rid;
 UPDATE population_top3000_202608_202609_v1.ov4_batch SET dispatches=dispatches+1,wake_ordinal=checkpoint,last_request_id=rid WHERE admission=aid;
 UPDATE population_top3000_202608_202609_v1.acquisition_control SET http_invocations=http_invocations+1 WHERE singleton;
 RETURN jsonb_build_object('state','ASYNC','request_id',rid,'dispatch_http',1,'provider_http',0);
END;$$;
CREATE FUNCTION population_top3000_202608_202609_v1.ov4_wake(aid bigint,tok text,wsha text) RETURNS jsonb LANGUAGE plpgsql SET search_path='pg_catalog' AS $$
DECLARE b record;BEGIN
 SELECT * INTO STRICT b FROM population_top3000_202608_202609_v1.ov4_batch WHERE admission=aid;
 IF b.wake_token IS DISTINCT FROM tok OR (SELECT worker_sha FROM population_top3000_202608_202609_v1.ov4_pin WHERE id) IS DISTINCT FROM wsha THEN RAISE EXCEPTION 'OV4_WAKE_AUTH_OR_CODE_REJECTED';END IF;
 RETURN population_top3000_202608_202609_v1.ov4_claim(aid,wsha);
END;$$;
CREATE FUNCTION population_top3000_202608_202609_v1.ov4_start(aid bigint) RETURNS jsonb LANGUAGE plpgsql SET search_path='pg_catalog' AS $$
DECLARE d jsonb;tid uuid;n int;BEGIN
 d:=population_top3000_202608_202609_v1.ov4_guard(aid);
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.ov4_batch WHERE admission=aid) THEN RETURN '{"state":"ALREADY_STARTED_NO_DUPLICATE_WAKE"}'::jsonb;END IF;
 SELECT coalesce(min(r.ordinal),201) INTO n FROM population_top3000_202608_202609_v1.ov4_request r WHERE NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.ov4_point p WHERE p.k=r.k);
 tid:=gen_random_uuid();
 INSERT INTO population_top3000_202608_202609_v1.pipeline_task(task_id,operation_key,family_id,stage,partition_key,payload,input_versions,status,max_executions,blocker_code,resumption_condition) VALUES(tid,'ov4:'||aid,'REF01','ACQUIRE','ov4-private-native:'||aid,jsonb_build_object('execution_binding','REVIEWED_PRIVATE_NATIVE_FUNCTION','source_admission',aid),jsonb_build_object('manifest',d->>'request_manifest_sha256','worker',d->>'worker_sha256','policy','3e49d661fcdb8533e5ce88a85689b41e9dde71eeae2dc8578c9e31f9ae1738bb'),'BLOCKED',100,'OV4_PRIVATE_NATIVE_ROUTE','{"route":"ov4_wake"}');
 INSERT INTO population_top3000_202608_202609_v1.ov4_batch(admission,task_id,checkpoint) VALUES(aid,tid,n);
 RETURN population_top3000_202608_202609_v1.ov4_dispatch(aid);
END;$$;


CREATE FUNCTION population_top3000_202608_202609_v1.ov4_auth(aid bigint,tok text,wsha text) RETURNS boolean LANGUAGE sql STABLE SET search_path='pg_catalog' AS $$SELECT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.ov4_batch b CROSS JOIN population_top3000_202608_202609_v1.ov4_pin p WHERE b.admission=aid AND b.wake_token=tok AND p.id AND p.worker_sha=wsha)$$;


CREATE FUNCTION population_top3000_202608_202609_v1.ov4_defer(aid bigint,tok text,wsha text) RETURNS jsonb LANGUAGE plpgsql SET search_path='pg_catalog' AS $$
DECLARE b record;BEGIN
 PERFORM population_top3000_202608_202609_v1.ov4_guard(aid);
 SELECT * INTO STRICT b FROM population_top3000_202608_202609_v1.ov4_batch WHERE admission=aid FOR UPDATE;
 IF b.wake_token IS DISTINCT FROM tok OR NOT population_top3000_202608_202609_v1.ov4_auth(aid,tok,wsha) OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.ov4_cap c JOIN population_top3000_202608_202609_v1.ov4_request r ON r.k=c.k WHERE r.ordinal=b.checkpoint) THEN RAISE EXCEPTION 'OV4_ONLY_PROVEN_NO_HTTP_WAIT_MAY_DEFER';END IF;
 -- No provider capability was issued. A bounded new internal wake is safe; each wake
 -- still consumes one of100 dispatches and the global HTTP budget. Source retries forbidden.
 UPDATE population_top3000_202608_202609_v1.ov4_batch SET wake_ordinal=0 WHERE admission=aid;
 RETURN population_top3000_202608_202609_v1.ov4_dispatch(aid);
END;$$;


CREATE FUNCTION population_top3000_202608_202609_v1.ov4_immutable() RETURNS trigger LANGUAGE plpgsql SET search_path='pg_catalog' AS $$BEGIN RAISE EXCEPTION 'OV4_APPEND_ONLY_IMMUTABLE_EVIDENCE';END;$$;
CREATE TRIGGER ov4_request_immutable BEFORE UPDATE OR DELETE ON population_top3000_202608_202609_v1.ov4_request FOR EACH ROW EXECUTE FUNCTION population_top3000_202608_202609_v1.ov4_immutable();
CREATE TRIGGER ov4_cap_immutable BEFORE UPDATE OR DELETE ON population_top3000_202608_202609_v1.ov4_cap FOR EACH ROW EXECUTE FUNCTION population_top3000_202608_202609_v1.ov4_immutable();
CREATE TRIGGER ov4_spool_immutable BEFORE UPDATE OR DELETE ON population_top3000_202608_202609_v1.ov4_spool FOR EACH ROW EXECUTE FUNCTION population_top3000_202608_202609_v1.ov4_immutable();
CREATE TRIGGER ov4_point_immutable BEFORE UPDATE OR DELETE ON population_top3000_202608_202609_v1.ov4_point FOR EACH ROW EXECUTE FUNCTION population_top3000_202608_202609_v1.ov4_immutable();
CREATE TRIGGER ov4_pin_immutable BEFORE UPDATE OR DELETE ON population_top3000_202608_202609_v1.ov4_pin FOR EACH ROW EXECUTE FUNCTION population_top3000_202608_202609_v1.ov4_immutable();

DO $$DECLARE r record;BEGIN FOR r IN SELECT oid::regclass rel FROM pg_class WHERE relnamespace='population_top3000_202608_202609_v1'::regnamespace AND relname LIKE 'ov4_%' AND relkind='r' LOOP EXECUTE format('ALTER TABLE %s ENABLE ROW LEVEL SECURITY',r.rel);EXECUTE format('REVOKE ALL ON %s FROM PUBLIC,anon,authenticated',r.rel);END LOOP;FOR r IN SELECT oid::regprocedure fn FROM pg_proc WHERE pronamespace='population_top3000_202608_202609_v1'::regnamespace AND proname LIKE 'ov4_%' LOOP EXECUTE format('REVOKE ALL ON FUNCTION %s FROM PUBLIC,anon,authenticated',r.fn);END LOOP;END;$$;
