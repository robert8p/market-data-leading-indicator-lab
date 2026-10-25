CREATE TABLE population_top3000_202608_202609_v1.ref01_receipt_spool_policy_v11(
 singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),enabled boolean NOT NULL DEFAULT false,
 worker_build text NOT NULL CHECK(worker_build='REF01_PRIVATE_SPOOL_V11'),worker_sha256 text NOT NULL CHECK(worker_sha256~'^[0-9a-f]{64}$'),
 review_event_id bigint NOT NULL REFERENCES population_top3000_202608_202609_v1.pipeline_event(event_id),
 maximum_bodies integer NOT NULL CHECK(maximum_bodies=1),maximum_body_bytes bigint NOT NULL CHECK(maximum_body_bytes=8388608),maximum_relation_bytes bigint NOT NULL CHECK(maximum_relation_bytes=33554432));
CREATE TABLE population_top3000_202608_202609_v1.ref01_receipt_spool_v11(
 attempt_id uuid PRIMARY KEY REFERENCES population_top3000_202608_202609_v1.pipeline_attempt(attempt_id),task_id uuid NOT NULL,
 token_sha256 text NOT NULL,receipt_sha256 text NOT NULL,source_sha256 text NOT NULL,body_bytes bigint NOT NULL CHECK(body_bytes BETWEEN 1 AND 8388608),
 worker_sha256 text NOT NULL,retained_at timestamptz NOT NULL DEFAULT clock_timestamp(),receipt jsonb NOT NULL,lineage jsonb NOT NULL,
 CHECK(receipt_sha256=encode(sha256(convert_to(receipt::text,'UTF8')),'hex')),
 CHECK(source_sha256=encode(sha256(convert_to(receipt->>'content_text','UTF8')),'hex')),
 CHECK(body_bytes=octet_length(receipt->>'content_text')));
ALTER TABLE population_top3000_202608_202609_v1.ref01_receipt_spool_policy_v11 ENABLE ROW LEVEL SECURITY;
ALTER TABLE population_top3000_202608_202609_v1.ref01_receipt_spool_v11 ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON population_top3000_202608_202609_v1.ref01_receipt_spool_policy_v11,population_top3000_202608_202609_v1.ref01_receipt_spool_v11 FROM PUBLIC,anon,authenticated;
CREATE FUNCTION population_top3000_202608_202609_v1.ref01_spool_immutable_v11() RETURNS trigger LANGUAGE plpgsql SET search_path='pg_catalog' AS $f$ BEGIN RAISE EXCEPTION 'REF01_V11_IMMUTABLE_RECEIPT';END $f$;
CREATE TRIGGER ref01_spool_immutable_v11 BEFORE UPDATE OR DELETE ON population_top3000_202608_202609_v1.ref01_receipt_spool_v11 FOR EACH ROW EXECUTE FUNCTION population_top3000_202608_202609_v1.ref01_spool_immutable_v11();

-- Read-only snapshot authentication deliberately does not call the existing FOR UPDATE control guards.
-- Retaining a completed, already-reserved response does not finish work, release billing or admit a request.
CREATE FUNCTION population_top3000_202608_202609_v1.ref01_spool_context_v11(p_attempt uuid,p_token text,p_build text)
RETURNS jsonb LANGUAGE plpgsql SET search_path='pg_catalog' AS $f$
DECLARE a record;t record;ac record;pc record;b record;pol record;r record;adm record;plan record;prior record;n integer;url text;k text;
BEGIN
SELECT * INTO STRICT ac FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton;
SELECT * INTO STRICT pc FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton;
IF ac.owner_id IS DISTINCT FROM 'top3000-completion-20261024-root-5db48b32d103' OR ac.owner_fence<>3 OR ac.stop_generation<>1 OR NOT ac.enabled OR ac.stopped OR ac.hold_reason IS NOT NULL OR NOT pc.enabled OR ac.admitted_until IS NULL OR ac.admitted_until<=clock_timestamp() OR pc.deadline IS NULL OR pc.deadline<=clock_timestamp() OR pc.expected_owner_id IS DISTINCT FROM ac.owner_id OR pc.expected_owner_fence IS DISTINCT FROM ac.owner_fence OR pc.expected_stop_generation IS DISTINCT FROM ac.stop_generation OR ac.scope_sha256 IS DISTINCT FROM '8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd' OR pc.scope_sha256 IS DISTINCT FROM ac.scope_sha256 THEN RAISE EXCEPTION 'REF01_V11_OWNER_STOP_OR_ADMISSION';END IF;
SELECT * INTO STRICT pol FROM population_top3000_202608_202609_v1.ref01_receipt_spool_policy_v11 WHERE singleton;
IF NOT pol.enabled OR p_build IS DISTINCT FROM pol.worker_build OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_event WHERE event_id=pol.review_event_id AND event='REF01_SPOOL_V11_DEPLOYMENT_REVIEW' AND details->>'approved'='true' AND details->>'worker_sha256'=pol.worker_sha256 AND details->>'owner_id'=ac.owner_id AND details->>'owner_fence'='3' AND details->>'stop_generation'='1' AND details->>'scope_sha256'=ac.scope_sha256 AND nullif(details->>'producer','') IS NOT NULL AND nullif(details->>'reviewer','') IS NOT NULL AND details->>'producer' IS DISTINCT FROM details->>'reviewer') THEN RAISE EXCEPTION 'REF01_V11_REVIEWED_WORKER_REQUIRED';END IF;
SELECT * INTO STRICT a FROM population_top3000_202608_202609_v1.pipeline_attempt WHERE attempt_id=p_attempt;
IF p_token IS NULL OR a.token_sha256 IS DISTINCT FROM encode(sha256(convert_to(p_token,'UTF8')),'hex') OR a.owner_fence<>ac.owner_fence OR a.stop_generation<>ac.stop_generation OR a.outcome IS DISTINCT FROM 'RUNNING' OR a.request_id IS NULL THEN RAISE EXCEPTION 'REF01_V11_ATTEMPT_AUTH_REQUIRED';END IF;
SELECT * INTO STRICT t FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=a.task_id;
IF t.task_id='15a87469-6335-4bf6-b883-6c86315418e7' OR t.family_id IS DISTINCT FROM 'REF01' OR t.stage IS DISTINCT FROM 'ACQUIRE' OR t.payload->>'kind' IS DISTINCT FROM 'REF01_CENSUS_SOURCE_V1' OR t.status IS DISTINCT FROM 'LEASED' OR t.attempt_id IS DISTINCT FROM p_attempt OR t.lease_until IS NULL OR t.lease_until<=clock_timestamp() OR t.owner_fence<>ac.owner_fence OR t.stop_generation<>ac.stop_generation THEN RAISE EXCEPTION 'REF01_V11_CURRENT_NEW_ATTEMPT_REQUIRED';END IF;
SELECT * INTO STRICT b FROM population_top3000_202608_202609_v1.pipeline_adapter WHERE family_id=t.family_id AND stage=t.stage;
IF NOT b.enabled OR a.adapter_sha256 IS DISTINCT FROM b.function_sha256 OR b.function_sha256 IS DISTINCT FROM encode(sha256(convert_to(pg_get_functiondef(b.function_oid),'UTF8')),'hex') THEN RAISE EXCEPTION 'REF01_V11_ADAPTER_CODE_CHANGED';END IF;
SELECT * INTO STRICT r FROM population_top3000_202608_202609_v1.ref01_date_request_v4 WHERE request_key=t.payload->>'request_key';
SELECT * INTO STRICT adm FROM population_top3000_202608_202609_v1.ref01_date_admission_v4 WHERE request_key=r.request_key;
SELECT p.* INTO STRICT plan FROM population_top3000_202608_202609_v1.ref01_date_plan_v9 p JOIN population_top3000_202608_202609_v1.ref01_date_plan_request_v9 pr USING(plan_id) WHERE pr.request_key=r.request_key AND pr.manifest_sha256=r.manifest_sha256 AND pr.input_versions_sha256=encode(sha256(convert_to(r.input_versions::text,'UTF8')),'hex');
IF NOT plan.enabled OR plan.content_sha256 IS DISTINCT FROM encode(sha256(convert_to(plan.content::text,'UTF8')),'hex') OR plan.code_bundle_sha256 IS DISTINCT FROM population_top3000_202608_202609_v1.ref01_date_bundle_v4() OR t.input_versions IS DISTINCT FROM r.input_versions OR r.manifest_sha256 IS DISTINCT FROM encode(sha256(convert_to(r.manifest::text,'UTF8')),'hex') OR t.payload->>'request_manifest_sha256' IS DISTINCT FROM r.manifest_sha256 OR t.input_versions->>'date_plan_id' IS DISTINCT FROM 'ref01-date-fullperiod-v9' OR t.input_versions->>'scope_sha256' IS DISTINCT FROM ac.scope_sha256 OR t.input_versions->>'policy_sha256' IS DISTINCT FROM '3e49d661fcdb8533e5ce88a85689b41e9dde71eeae2dc8578c9e31f9ae1738bb' OR t.input_versions->>'cohort_sha256' IS DISTINCT FROM '2dc0f766befed6de6b052221c3c87482d507ba1bad931cbb98654eb3af899bec' OR adm.owner_id IS DISTINCT FROM ac.owner_id OR adm.owner_fence<>ac.owner_fence OR adm.stop_generation<>ac.stop_generation OR adm.expires_at<=clock_timestamp() THEN RAISE EXCEPTION 'REF01_V11_EXACT_SOURCE_ADMISSION_REQUIRED';END IF;
n:=coalesce((t.checkpoint->>'next_page')::integer,1);k:='ref01-date-archive-v4:'||encode(sha256(convert_to(r.request_key||E'\n'||r.input_versions::text,'UTF8')),'hex');
IF n>adm.admitted_page_ceiling OR n>(r.manifest->>'max_pages')::integer OR n<1 THEN RAISE EXCEPTION 'REF01_V11_PAGE_CAP';END IF;
IF n=1 THEN url:=r.manifest->>'first_url';ELSE SELECT * INTO STRICT prior FROM population_top3000_202608_202609_v1.ref01_date_page_v4 WHERE archive_key=k AND page_number=n-1;url:=prior.next_url;IF url IS NULL OR prior.source_sha256 IS DISTINCT FROM t.checkpoint->>'last_source_sha256' OR prior.source_id IS DISTINCT FROM t.checkpoint->>'last_source_id' THEN RAISE EXCEPTION 'REF01_V11_PRIOR_PAGE_CHANGED';END IF;END IF;
IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_provider_lease WHERE attempt_id=p_attempt AND provider='Massive' AND released_at IS NULL AND reserved_bytes=8388608) THEN RAISE EXCEPTION 'REF01_V11_EXISTING_RESERVATION_REQUIRED';END IF;
RETURN jsonb_build_object('task_id',t.task_id,'token_sha256',a.token_sha256,'attempt_id',p_attempt,'execution_number',a.execution_number,'request_id',a.request_id,'adapter_sha256',a.adapter_sha256,'worker_sha256',pol.worker_sha256,'owner_id',ac.owner_id,'owner_fence',ac.owner_fence,'stop_generation',ac.stop_generation,'scope_sha256',ac.scope_sha256,'admitted_until',ac.admitted_until,'original_deadline',pc.deadline,'source_admission',to_jsonb(adm),'plan_id',plan.plan_id,'plan_sha256',plan.content_sha256,'plan_bundle_sha256',plan.code_bundle_sha256,'request_key',r.request_key,'request_manifest_sha256',r.manifest_sha256,'input_versions',t.input_versions,'archive_key',k,'page_number',n,'request_url',url,'attempt_started_at',a.started_at);
END $f$;

CREATE FUNCTION population_top3000_202608_202609_v1.ref01_spool_peek_v11(p_attempt uuid,p_token text,p_build text)
RETURNS jsonb LANGUAGE plpgsql SET search_path='pg_catalog' AS $f$
DECLARE s record;c jsonb;
BEGIN
-- A missing receipt does not authenticate or admit a provider request; existing CLAIM remains mandatory.
SELECT * INTO s FROM population_top3000_202608_202609_v1.ref01_receipt_spool_v11 WHERE attempt_id=p_attempt;
IF NOT FOUND THEN
IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.ref01_receipt_spool_policy_v11 p JOIN population_top3000_202608_202609_v1.pipeline_event e ON e.event_id=p.review_event_id WHERE p.singleton AND p.enabled AND p.worker_build=p_build AND e.event='REF01_SPOOL_V11_DEPLOYMENT_REVIEW' AND e.details->>'approved'='true' AND e.details->>'worker_sha256'=p.worker_sha256 AND nullif(e.details->>'producer','') IS NOT NULL AND nullif(e.details->>'reviewer','') IS NOT NULL AND e.details->>'producer' IS DISTINCT FROM e.details->>'reviewer') OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.ref01_receipt_spool_v11) THEN RAISE EXCEPTION 'REF01_V11_FIRST_ONE_REVIEW_REQUIRED_BEFORE_FETCH';END IF;
RETURN jsonb_build_object('state','NO_RETAINED_RECEIPT');END IF;
c:=population_top3000_202608_202609_v1.ref01_spool_context_v11(p_attempt,p_token,p_build);
IF s.token_sha256 IS DISTINCT FROM c->>'token_sha256' OR s.worker_sha256 IS DISTINCT FROM c->>'worker_sha256' OR s.task_id::text IS DISTINCT FROM c->>'task_id' OR s.receipt_sha256 IS DISTINCT FROM encode(sha256(convert_to(s.receipt::text,'UTF8')),'hex') THEN RAISE EXCEPTION 'REF01_V11_RETAINED_RECEIPT_CONFLICT';END IF;
RETURN jsonb_build_object('state','RETAINED_RECEIPT','receipt',s.receipt,'receipt_sha256',s.receipt_sha256,'source_sha256',s.source_sha256);
END $f$;

CREATE FUNCTION population_top3000_202608_202609_v1.ref01_spool_put_v11(p_attempt uuid,p_token text,p_build text,p_page jsonb)
RETURNS jsonb LANGUAGE plpgsql SET search_path='pg_catalog' AS $f$
DECLARE c jsonb;s record;txt text;h text;bytes bigint;rh text;before_bytes bigint;
BEGIN
IF NOT pg_try_advisory_xact_lock(2026102500000011) THEN RAISE EXCEPTION 'REF01_V11_SPOOL_BUSY';END IF;
c:=population_top3000_202608_202609_v1.ref01_spool_context_v11(p_attempt,p_token,p_build);
txt:=p_page->>'content_text';bytes:=octet_length(txt);h:=encode(sha256(convert_to(txt,'UTF8')),'hex');rh:=encode(sha256(convert_to(p_page::text,'UTF8')),'hex');
IF jsonb_typeof(p_page) IS DISTINCT FROM 'object' OR txt IS NULL OR bytes NOT BETWEEN 1 AND 8388608 OR p_page->>'source_sha256' IS DISTINCT FROM h OR p_page->>'wire_sha256' IS DISTINCT FROM h OR p_page->>'wire_bytes' IS DISTINCT FROM bytes::text OR p_page->>'archive_key' IS DISTINCT FROM c->>'archive_key' OR p_page->>'request_manifest_sha256' IS DISTINCT FROM c->>'request_manifest_sha256' OR p_page->>'page_number' IS DISTINCT FROM c->>'page_number' OR p_page->>'request_url' IS DISTINCT FROM c->>'request_url' OR p_page->>'http_status' IS DISTINCT FROM '200' OR p_page->>'retrieved_at' IS NULL OR (p_page->>'retrieved_at')::timestamptz<(c->>'attempt_started_at')::timestamptz-interval '30seconds' OR (p_page->>'retrieved_at')::timestamptz>clock_timestamp()+interval '30seconds' OR coalesce((p_page->>'elapsed_ms')::bigint,-1) NOT BETWEEN 0 AND 45000 THEN RAISE EXCEPTION 'REF01_V11_RECEIPT_BINDING_REJECTED';END IF;
IF txt~* '\"(apiKey|api_key|access_token|authorization)\"[[:space:]]*:' OR txt~* '[?&](apiKey|api_key|access_token)=' THEN RAISE EXCEPTION 'REF01_V11_CREDENTIAL_FIELD_REJECTED';END IF;
SELECT * INTO s FROM population_top3000_202608_202609_v1.ref01_receipt_spool_v11 WHERE attempt_id=p_attempt;
IF FOUND THEN IF s.receipt_sha256 IS DISTINCT FROM rh OR s.receipt IS DISTINCT FROM p_page OR (s.lineage-ARRAY['admitted_until','source_admission']) IS DISTINCT FROM (c-ARRAY['admitted_until','source_admission']) THEN RAISE EXCEPTION 'REF01_V11_IMMUTABLE_CONFLICT';END IF;RETURN jsonb_build_object('state','DURABLY_RETAINED','receipt_sha256',rh,'source_sha256',h,'idempotent_replay',true);END IF;
IF (SELECT count(*) FROM population_top3000_202608_202609_v1.ref01_receipt_spool_v11)>=1 THEN RAISE EXCEPTION 'REF01_V11_FIRST_ONE_QUALIFICATION_BOUND';END IF;
before_bytes:=pg_total_relation_size('population_top3000_202608_202609_v1.ref01_receipt_spool_v11'::regclass);
INSERT INTO population_top3000_202608_202609_v1.ref01_receipt_spool_v11(attempt_id,task_id,token_sha256,receipt_sha256,source_sha256,body_bytes,worker_sha256,receipt,lineage) VALUES(p_attempt,(c->>'task_id')::uuid,c->>'token_sha256',rh,h,bytes,c->>'worker_sha256',p_page,c);
IF pg_total_relation_size('population_top3000_202608_202609_v1.ref01_receipt_spool_v11'::regclass)+pg_total_relation_size('population_top3000_202608_202609_v1.ref01_receipt_spool_policy_v11'::regclass)>33554432 THEN RAISE EXCEPTION 'REF01_V11_CUMULATIVE_RELATION_BOUND';END IF;
RETURN jsonb_build_object('state','DURABLY_RETAINED','receipt_sha256',rh,'source_sha256',h,'idempotent_replay',false);
END $f$;
REVOKE ALL ON FUNCTION population_top3000_202608_202609_v1.ref01_spool_immutable_v11(),population_top3000_202608_202609_v1.ref01_spool_context_v11(uuid,text,text),population_top3000_202608_202609_v1.ref01_spool_peek_v11(uuid,text,text),population_top3000_202608_202609_v1.ref01_spool_put_v11(uuid,text,text,jsonb) FROM PUBLIC,anon,authenticated;
