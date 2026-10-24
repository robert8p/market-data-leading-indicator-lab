-- Root integration only. Exactly-once reconciliation of already observed external logical source attempts.
-- provider_requests is cumulative logical/source attempts. http_invocations retains its existing Edge-invocation meaning.
CREATE TABLE IF NOT EXISTS population_top3000_202608_202609_v1.external_logical_source_call_v1(
 logical_call_id text PRIMARY KEY, worker_id text NOT NULL,provider text NOT NULL,
 metadata jsonb NOT NULL,metadata_sha256 text NOT NULL,logical_counter_delta integer NOT NULL CHECK(logical_counter_delta IN(0,1)),
 first_batch_id text NOT NULL,owner_fence bigint NOT NULL,stop_generation bigint NOT NULL,recorded_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 CHECK(metadata_sha256=encode(sha256(convert_to(metadata::text,'UTF8')),'hex')));
CREATE TABLE IF NOT EXISTS population_top3000_202608_202609_v1.external_logical_source_batch_v1(
 batch_id text PRIMARY KEY,batch_manifest jsonb NOT NULL,batch_sha256 text NOT NULL,
 new_logical_rows integer NOT NULL,new_provider_requests bigint NOT NULL,recorded_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 CHECK(batch_sha256=encode(sha256(convert_to(batch_manifest::text,'UTF8')),'hex')));
ALTER TABLE population_top3000_202608_202609_v1.external_logical_source_call_v1 ENABLE ROW LEVEL SECURITY;
ALTER TABLE population_top3000_202608_202609_v1.external_logical_source_batch_v1 ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON population_top3000_202608_202609_v1.external_logical_source_call_v1,population_top3000_202608_202609_v1.external_logical_source_batch_v1 FROM PUBLIC,anon,authenticated;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.external_logical_source_reconcile_v1(
 p_owner text,p_fence bigint,p_stop bigint,p_helper_sha256 text,p_batch jsonb)
RETURNS jsonb LANGUAGE plpgsql SET search_path TO pg_catalog AS $function$
DECLARE a population_top3000_202608_202609_v1.acquisition_control%rowtype;p population_top3000_202608_202609_v1.pipeline_control%rowtype;
 old population_top3000_202608_202609_v1.external_logical_source_call_v1%rowtype;b population_top3000_202608_202609_v1.external_logical_source_batch_v1%rowtype;
 r jsonb;bid text;bh text;key text;rh text;mode text;delta integer;n integer:=0;added bigint:=0;found_rows integer;seeded integer:=0;
BEGIN
 PERFORM pg_advisory_xact_lock(2026083000261022);
 SELECT * INTO a FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton FOR UPDATE;
 IF NOT FOUND THEN RAISE EXCEPTION 'EXTERNAL_LOGICAL_CONTROL_MISSING';END IF;
 SELECT * INTO p FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton FOR UPDATE;
 IF NOT FOUND OR a.owner_id IS DISTINCT FROM p_owner OR a.owner_fence IS DISTINCT FROM p_fence OR a.stop_generation IS DISTINCT FROM p_stop
 OR a.owner_id IS DISTINCT FROM p.expected_owner_id OR a.owner_fence IS DISTINCT FROM p.expected_owner_fence OR a.stop_generation IS DISTINCT FROM p.expected_stop_generation
 OR a.enabled IS DISTINCT FROM true OR a.stopped IS DISTINCT FROM false OR a.hold_reason IS NOT NULL
 OR a.admitted_until IS NULL OR a.admitted_until<=clock_timestamp() OR p.enabled IS DISTINCT FROM true OR p.shared_scheduler_verified IS DISTINCT FROM true
 OR p.deadline IS NULL OR p.deadline<=clock_timestamp() OR a.scope_sha256 IS DISTINCT FROM '8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd' OR p.scope_sha256 IS DISTINCT FROM a.scope_sha256
 THEN RAISE EXCEPTION 'EXTERNAL_LOGICAL_OWNER_STOP_OR_ADMISSION_CHANGED';END IF;
 IF p_helper_sha256 IS DISTINCT FROM encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.external_logical_source_reconcile_v1(text,bigint,bigint,text,jsonb)'::regprocedure),'UTF8')),'hex')
 THEN RAISE EXCEPTION 'EXTERNAL_LOGICAL_HELPER_HASH_CHANGED';END IF;
 PERFORM population_top3000_202608_202609_v1.d02_daily_resource_guard_v2();
 bid:=p_batch->>'batch_id';bh:=encode(sha256(convert_to(p_batch::text,'UTF8')),'hex');
 IF jsonb_typeof(p_batch) IS DISTINCT FROM 'object' OR octet_length(p_batch::text)>2097152
 OR bid IS NULL OR length(bid) NOT BETWEEN 1 AND 200 OR bid!~'^[A-Za-z0-9:_.-]+$'
 OR p_batch->>'scope_sha256' IS DISTINCT FROM a.scope_sha256
 OR p_batch->>'source_artifact_sha256' IS NULL OR p_batch->>'source_artifact_sha256'!~'^[0-9a-f]{64}$'
 OR p_batch->>'source_artifact_path' IS NULL OR length(p_batch->>'source_artifact_path') NOT BETWEEN 1 AND 512
 OR jsonb_typeof(p_batch->'rows') IS DISTINCT FROM 'array' OR jsonb_array_length(p_batch->'rows') NOT BETWEEN 1 AND 100
 OR p_batch->>'accounting_basis' IS DISTINCT FROM 'EXTERNAL_LOGICAL_SOURCE_ATTEMPTS_NOT_PHYSICAL_WIRE_COUNT'
 THEN RAISE EXCEPTION 'EXTERNAL_LOGICAL_BATCH_INVALID';END IF;
 IF (SELECT count(*)<>count(DISTINCT value->>'logical_call_id') FROM jsonb_array_elements(p_batch->'rows')) THEN RAISE EXCEPTION 'EXTERNAL_LOGICAL_DUPLICATE_BATCH_KEYS';END IF;
 SELECT * INTO b FROM population_top3000_202608_202609_v1.external_logical_source_batch_v1 WHERE batch_id=bid;
 IF FOUND THEN
  IF b.batch_manifest IS DISTINCT FROM p_batch OR b.batch_sha256 IS DISTINCT FROM bh THEN RAISE EXCEPTION 'EXTERNAL_LOGICAL_BATCH_REPLAY_CONFLICT';END IF;
  IF EXISTS(SELECT 1 FROM jsonb_array_elements(p_batch->'rows')x LEFT JOIN population_top3000_202608_202609_v1.external_logical_source_call_v1 c ON c.logical_call_id=x.value->>'logical_call_id'
   WHERE c.metadata IS DISTINCT FROM x.value OR c.metadata_sha256 IS DISTINCT FROM encode(sha256(convert_to(x.value::text,'UTF8')),'hex')) THEN RAISE EXCEPTION 'EXTERNAL_LOGICAL_PRIOR_BATCH_ROW_CHANGED';END IF;
  RETURN jsonb_build_object('state','REPLAY','batch_id',bid,'new_logical_rows',0,'provider_requests_delta',0,'http_invocations_delta',0,'stored_bytes_delta',0,'costs_or_commitments_changed',false);
 END IF;
 FOR r IN SELECT value FROM jsonb_array_elements(p_batch->'rows') LOOP
  key:=r->>'logical_call_id';mode:=r->>'counter_mode';rh:=encode(sha256(convert_to(r::text,'UTF8')),'hex');
  IF jsonb_typeof(r) IS DISTINCT FROM 'object' OR octet_length(r::text)>65536 OR key IS NULL OR length(key) NOT BETWEEN 1 AND 240 OR key!~'^[A-Za-z0-9:_.-]+$'
  OR r->>'worker_id' IS NULL OR r->>'worker_id' NOT IN('financial04','context06','prices03','company02','acceptance07','options05','runtime01')
  OR r->>'provider' IS NULL OR length(r->>'provider') NOT BETWEEN 1 AND 100
  OR r->'logical_source_attempts' IS DISTINCT FROM '1'::jsonb
  OR r->>'operation_kind' IS NULL OR r->>'operation_kind' NOT IN('DIRECT_SOURCE_HTTP_CLIENT_CALL','CONNECTED_SOURCE_TOOL_CALL')
  OR mode IS NULL OR mode NOT IN('COUNT_NEW_LOGICAL_SOURCE_ATTEMPT','SEED_ALREADY_COUNTED_CRD')
  OR jsonb_typeof(r->'original_receipt') IS DISTINCT FROM 'object'
  OR r->>'physical_wire_count_exact' IS DISTINCT FROM 'false'
  OR r->>'invoice_cost_usd' IS NOT NULL OR r->>'cost_receipt_observed' IS DISTINCT FROM 'false'
  THEN RAISE EXCEPTION 'EXTERNAL_LOGICAL_ROW_INVALID';END IF;
  IF mode='SEED_ALREADY_COUNTED_CRD' OR key='financial04-http-v1:83e21c5e7009e14013bcbc78e1fbbee4c48d8e999b8eaebbb0bea31517d9ee8d' THEN
   IF mode IS DISTINCT FROM 'SEED_ALREADY_COUNTED_CRD' OR key IS DISTINCT FROM 'financial04-http-v1:83e21c5e7009e14013bcbc78e1fbbee4c48d8e999b8eaebbb0bea31517d9ee8d'
   OR r->>'worker_id' IS DISTINCT FROM 'financial04' OR r->>'provider' IS DISTINCT FROM 'SEC'
   OR r->>'request_url' IS DISTINCT FROM 'https://www.sec.gov/Archives/edgar/data/25475/000119312526330643/crda-20260630_htm.xml'
   OR r->>'source_sha256' IS DISTINCT FROM '7a9176789eaf515caa6a53c8c803645744b3d0d2d8c9c1f08a0a254bf872630c'
   OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.source_document WHERE source_id='financial-sec-original-crd-20260630:7a9176789eaf515caa6a53c8c803645744b3d0d2d8c9c1f08a0a254bf872630c' AND source_sha256=r->>'source_sha256' AND content_bytes=3875138 AND source_provider='SEC')
   THEN RAISE EXCEPTION 'EXTERNAL_LOGICAL_ALREADY_COUNTED_PROOF_INVALID';END IF;
   delta:=0;
  ELSE delta:=1;END IF;
  SELECT * INTO old FROM population_top3000_202608_202609_v1.external_logical_source_call_v1 WHERE logical_call_id=key;
  IF FOUND THEN
   IF old.metadata IS DISTINCT FROM r OR old.metadata_sha256 IS DISTINCT FROM rh OR old.logical_counter_delta IS DISTINCT FROM delta THEN RAISE EXCEPTION 'EXTERNAL_LOGICAL_IMMUTABLE_CALL_CONFLICT';END IF;
  ELSE
   IF a.max_provider_requests IS NULL OR a.provider_requests IS NULL OR a.provider_requests+added+delta>a.max_provider_requests THEN RAISE EXCEPTION 'EXTERNAL_LOGICAL_PROVIDER_REQUEST_BOUND';END IF;
   INSERT INTO population_top3000_202608_202609_v1.external_logical_source_call_v1(logical_call_id,worker_id,provider,metadata,metadata_sha256,logical_counter_delta,first_batch_id,owner_fence,stop_generation)
   VALUES(key,r->>'worker_id',r->>'provider',r,rh,delta,bid,p_fence,p_stop);
   n:=n+1;added:=added+delta;seeded:=seeded+CASE WHEN delta=0 THEN 1 ELSE 0 END;
  END IF;
 END LOOP;
 UPDATE population_top3000_202608_202609_v1.acquisition_control SET provider_requests=provider_requests+added,updated_at=clock_timestamp() WHERE singleton;
 INSERT INTO population_top3000_202608_202609_v1.external_logical_source_batch_v1(batch_id,batch_manifest,batch_sha256,new_logical_rows,new_provider_requests)VALUES(bid,p_batch,bh,n,added);
 PERFORM population_top3000_202608_202609_v1.d02_daily_resource_guard_v2();
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details)VALUES('EXTERNAL_LOGICAL_SOURCE_REQUESTS_RECONCILED_V1',jsonb_build_object('batch_id',bid,'batch_sha256',bh,'source_artifact_sha256',p_batch->>'source_artifact_sha256','new_logical_rows',n,'provider_requests_delta',added,'already_counted_seed_rows',seeded,
 'provider_requests_basis','CUMULATIVE_LOGICAL_SOURCE_ATTEMPTS','http_invocations_basis','EXISTING_EDGE_INVOCATIONS_UNCHANGED','exact_physical_wire_count_claimed',false,'http_invocations_delta',0,'stored_bytes_delta',0,'costs_or_commitments_changed',false,'owner_fence',p_fence,'stop_generation',p_stop));
 RETURN jsonb_build_object('state','RECONCILED','batch_id',bid,'new_logical_rows',n,'provider_requests_delta',added,'already_counted_seed_rows',seeded,'http_invocations_delta',0,'stored_bytes_delta',0,'costs_or_commitments_changed',false);
END; $function$;
REVOKE ALL ON FUNCTION population_top3000_202608_202609_v1.external_logical_source_reconcile_v1(text,bigint,bigint,text,jsonb) FROM PUBLIC,anon,authenticated;
