CREATE TABLE population_top3000_202608_202609_v1.raw_minute_page_v3(
 request_sha256 text NOT NULL REFERENCES population_top3000_202608_202609_v1.raw_minute_request_v3,
 page_number integer NOT NULL CHECK(page_number BETWEEN 1 AND 4),
 attempt_id uuid NOT NULL UNIQUE REFERENCES population_top3000_202608_202609_v1.pipeline_attempt,
 request_url text NOT NULL, content_text text NOT NULL,
 stored_sha256 text NOT NULL CHECK(stored_sha256=encode(sha256(convert_to(content_text,'UTF8')),'hex')),
 wire_sha256 text NOT NULL CHECK(wire_sha256 ~ '^[0-9a-f]{64}$'),
 wire_bytes bigint NOT NULL CHECK(wire_bytes BETWEEN 1 AND 8388608),
 validation jsonb NOT NULL, input_versions jsonb NOT NULL,
 PRIMARY KEY(request_sha256,page_number));
CREATE TABLE population_top3000_202608_202609_v1.raw_minute_reservation_v3(
 attempt_id uuid PRIMARY KEY REFERENCES population_top3000_202608_202609_v1.pipeline_attempt,
 request_sha256 text NOT NULL REFERENCES population_top3000_202608_202609_v1.raw_minute_request_v3,
 page_number integer NOT NULL CHECK(page_number BETWEEN 1 AND 4),
 request_url text NOT NULL, prior_chain_sha256 text NOT NULL,
 receipt jsonb NOT NULL, created_at timestamptz NOT NULL DEFAULT clock_timestamp());
ALTER TABLE population_top3000_202608_202609_v1.raw_minute_page_v3 ENABLE ROW LEVEL SECURITY;
ALTER TABLE population_top3000_202608_202609_v1.raw_minute_reservation_v3 ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON TABLE population_top3000_202608_202609_v1.raw_minute_page_v3,population_top3000_202608_202609_v1.raw_minute_reservation_v3 FROM PUBLIC,anon,authenticated;
CREATE FUNCTION population_top3000_202608_202609_v1.raw_minute_claim_v3(p_attempt uuid,p_token text) RETURNS jsonb
LANGUAGE plpgsql SECURITY INVOKER SET search_path='pg_catalog' AS $fn$
DECLARE c jsonb;m jsonb;t population_top3000_202608_202609_v1.pipeline_task%ROWTYPE;p record;v jsonb;chain jsonb:='[]';url text;n integer:=0;used bigint:=0;r jsonb;receipt jsonb;binding jsonb;expected_digest text;
BEGIN
IF encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.raw_minute_request_context_v3(uuid,text)'::regprocedure),'UTF8')),'hex') IS DISTINCT FROM '52c0c13c3f0842e030edb1d73e7f6c0ba536e3975e3ba4e6db86d1bd7fcebe34' OR encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.raw_minute_page_validate_v3(text,text,text)'::regprocedure),'UTF8')),'hex') IS DISTINCT FROM '5399e4f6597d0ae0ed5ecdf41dd16d7e0d2f65c1d315e48614b686ba0b26db65' OR encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.pipeline_provider_reserve_v1(uuid,text,text,bigint,numeric)'::regprocedure),'UTF8')),'hex') IS DISTINCT FROM '061748ff40061fba15df24e8e594d4411b989c48b91f41178ac99ccb049a59a8' THEN RAISE EXCEPTION 'RAW_CLAIM_HELPER_CHANGED';END IF;
c:=population_top3000_202608_202609_v1.raw_minute_request_context_v3(p_attempt,p_token);m:=c->'manifest';
SELECT * INTO STRICT t FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=(c->>'task_id')::uuid;
IF t.lease_until<=clock_timestamp()+interval '2seconds' THEN RAISE EXCEPTION 'RAW_CLAIM_LEASE_TOO_SHORT';END IF;
IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_provider_lease l JOIN population_top3000_202608_202609_v1.pipeline_attempt a USING(attempt_id) WHERE a.task_id=t.task_id AND NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.raw_minute_page_v3 z WHERE z.attempt_id=a.attempt_id)) OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.raw_minute_reservation_v3 WHERE attempt_id=p_attempt) THEN RAISE EXCEPTION 'RAW_PRIOR_EFFECT_RECONCILIATION_REQUIRED';END IF;
SELECT jsonb_build_object('manifest_text',q.manifest_text,'manifest_sha256',q.manifest_text_sha256,'source_admission_event_id',q.source_admission_event_id,'source_admission_sha256',q.source_admission_sha256) INTO binding FROM population_top3000_202608_202609_v1.raw_minute_request_v3 q WHERE q.request_sha256=c->>'request_sha256';
url:=c->>'first_url';
FOR p IN SELECT * FROM population_top3000_202608_202609_v1.raw_minute_page_v3 WHERE request_sha256=c->>'request_sha256' ORDER BY page_number LOOP
 n:=n+1;
 v:=population_top3000_202608_202609_v1.raw_minute_page_validate_v3(c->>'original_manifest_text',p.content_text,p.stored_sha256);
 IF p.page_number<>n OR p.request_url IS DISTINCT FROM url OR p.input_versions IS DISTINCT FROM t.input_versions OR p.validation IS DISTINCT FROM v OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_provider_lease l JOIN population_top3000_202608_202609_v1.pipeline_attempt a USING(attempt_id) WHERE l.attempt_id=p.attempt_id AND a.task_id=t.task_id AND l.released_at IS NOT NULL AND l.received_bytes=p.wire_bytes) THEN RAISE EXCEPTION 'RAW_PRIOR_PAGE_CHAIN_CONFLICT';END IF;
 expected_digest:=encode(sha256(convert_to(jsonb_build_object('page',p.submission,'reservation',p.commit_receipt->'reservation','input_versions',t.input_versions,'prior_chain',chain,'source_binding',binding)::text,'UTF8')),'hex');
 IF p.commit_receipt->'source_binding' IS DISTINCT FROM binding OR p.commit_receipt->>'output_sha256' IS DISTINCT FROM expected_digest OR p.submission->>'content_text' IS DISTINCT FROM p.content_text OR p.submission->>'wire_sha256' IS DISTINCT FROM p.wire_sha256 OR p.submission->>'wire_bytes' IS DISTINCT FROM p.wire_bytes::text OR p.submission->>'source_sha256' IS DISTINCT FROM p.stored_sha256 OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_commit k WHERE k.attempt_id=p.attempt_id AND k.task_id=t.task_id AND k.receipt=p.commit_receipt AND k.output_sha256=expected_digest AND k.commit_key='raw-page-v3:'||p.request_sha256||':'||p.page_number) THEN RAISE EXCEPTION 'RAW_PRIOR_PAGE_DIGEST_CONFLICT';END IF;
 IF v->>'next_url'=url OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.raw_minute_page_v3 z WHERE z.request_sha256=p.request_sha256 AND z.page_number<=n AND z.request_url=v->>'next_url') THEN RAISE EXCEPTION 'RAW_CURSOR_CYCLE';END IF;
 chain:=chain||jsonb_build_array(to_jsonb(p));used:=used+p.wire_bytes;url:=v->>'next_url';
END LOOP;
IF coalesce(t.checkpoint->>'last_page','0') IS DISTINCT FROM n::text THEN RAISE EXCEPTION 'RAW_CHECKPOINT_CONFLICT';END IF;
IF url IS NULL THEN RAISE EXCEPTION 'RAW_TERMINAL_REQUIRES_VERIFICATION';END IF;
IF n>=(c->>'maximum_pages')::integer OR used+(m->>'max_response_bytes')::bigint>(c->>'maximum_total_bytes')::bigint THEN RAISE EXCEPTION 'RAW_MEASURED_CONTINUATION_REQUIRED';END IF;
r:=population_top3000_202608_202609_v1.pipeline_provider_reserve_v1(p_attempt,p_token,'Massive',(m->>'max_response_bytes')::bigint,0);
IF r->>'state' IS DISTINCT FROM 'RESERVED' THEN RETURN r||jsonb_build_object('provider_requested',false);END IF;
receipt:=jsonb_build_object('manifest_sha256',c->>'request_sha256','request_url',url,'page_number',n+1,'attempt_id',p_attempt,'reserved_bytes',(m->>'max_response_bytes')::bigint,'expiry_epoch',extract(epoch FROM least(t.lease_until,(c->>'not_after')::timestamptz)),'reserved',true,'owner_fence',3,'stop_generation',1);
INSERT INTO population_top3000_202608_202609_v1.raw_minute_reservation_v3(attempt_id,request_sha256,page_number,request_url,prior_chain_sha256,receipt) VALUES(p_attempt,c->>'request_sha256',n+1,url,encode(sha256(convert_to(chain::text,'UTF8')),'hex'),receipt);
RETURN jsonb_build_object('state','RESERVED','manifest_text',c->>'original_manifest_text','reservation',receipt,'provider_requested',false,'source_stage_only',true);
END $fn$;
REVOKE ALL ON FUNCTION population_top3000_202608_202609_v1.raw_minute_claim_v3(uuid,text) FROM PUBLIC,anon,authenticated;
