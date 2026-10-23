BEGIN;
SET LOCAL statement_timeout='10s';
SET LOCAL lock_timeout='2s';
SET LOCAL standard_conforming_strings=on;
SELECT pg_advisory_xact_lock(2026083000261022);
-- Root inserts SET LOCAL top3000.ref01_chunk_ordinal, ref01_chunk_text,
-- ref01_chunk_sha256 after BEGIN using the exact original serialized JSON, not JSON reformatting.
DO $insert$ DECLARE c population_top3000_202608_202609_v1.acquisition_control%rowtype;
p population_top3000_202608_202609_v1.pipeline_control%rowtype;
v_ordinal integer;v_text text;v_sha text;v_bytes integer;v_sum bigint;v_new integer;
BEGIN
SELECT * INTO c FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton FOR UPDATE;
SELECT * INTO p FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton FOR UPDATE;
IF c.owner_id IS DISTINCT FROM 'top3000-recovery-20261023-root-4b1314bcb5c2' OR c.owner_fence IS DISTINCT FROM 2 OR c.stop_generation IS DISTINCT FROM 0
 OR p.expected_owner_id IS DISTINCT FROM c.owner_id OR p.expected_owner_fence IS DISTINCT FROM c.owner_fence OR p.expected_stop_generation IS DISTINCT FROM c.stop_generation
 OR c.scope_sha256 IS DISTINCT FROM '8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd' OR p.scope_sha256 IS DISTINCT FROM c.scope_sha256
 OR c.enabled IS DISTINCT FROM true OR c.stopped IS DISTINCT FROM false OR c.admitted_until IS NULL OR c.admitted_until<=clock_timestamp()
 OR p.enabled IS DISTINCT FROM true OR p.shared_scheduler_verified IS DISTINCT FROM true OR p.deadline IS NULL OR p.deadline<=clock_timestamp()
 OR c.hold_reason IS DISTINCT FROM 'ROOT_CONNECTOR_RESERVATION:e3ce9307-f9a7-4795-911a-1967ea7c710f' OR c.reserved_bytes<8388608
 OR NOT EXISTS(SELECT 1 FROM cron.job WHERE jobid=596 AND active=false)
 OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE status='LEASED')
 OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_provider_lease WHERE released_at IS NULL)
 OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.acquisition_job WHERE status='LEASED')
 THEN RAISE EXCEPTION 'DHC_EXACT_OWNER_HOLD_AND_PAUSED_DRAINED_RUNTIME_REQUIRED';END IF;
IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_event e WHERE e.event='ROOT_REF01_CONNECTOR_RESERVED_V1'
 AND e.details->>'reservation_id'='e3ce9307-f9a7-4795-911a-1967ea7c710f'
 AND e.details->>'manifest_sha256'='7c6c572fb8e4fed3f4e5136d5928a71a3d819d61fbe0697793086da70b55c07f'
 AND e.details->>'route_id'='10' AND e.details->>'reserved_bytes'='8388608'
 AND e.details->>'owner_fence'='2' AND e.details->>'stop_generation'='0'
 AND e.details#>>'{route,route}'='/v1/reference/sec/filings/0001075415-26-000030/files/dhc-20260630.htm')

 THEN RAISE EXCEPTION 'DHC_EXISTING_RESERVATION_REQUIRED';END IF;
v_ordinal:=current_setting('top3000.ref01_chunk_ordinal')::integer;
v_text:=current_setting('top3000.ref01_chunk_text');v_sha:=current_setting('top3000.ref01_chunk_sha256');v_bytes:=octet_length(v_text);
IF v_ordinal NOT BETWEEN 1 AND 28 OR v_bytes IS DISTINCT FROM (CASE WHEN v_ordinal=28 THEN 120523 ELSE 150000 END)
 OR v_sha IS DISTINCT FROM encode(sha256(convert_to(v_text,'UTF8')),'hex') THEN RAISE EXCEPTION 'DHC_CHUNK_HASH_SIZE_OR_ORDINAL';END IF;
IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.source_document WHERE source_id='ref01-massive-connector:e3ce9307-f9a7-4795-911a-1967ea7c710f')
 THEN RAISE EXCEPTION 'DHC_ALREADY_RETAINED_RECONCILE_NO_CHUNK_REPLAY';END IF;
INSERT INTO population_top3000_202608_202609_v1.ref01_connector_response_chunk_v1(reservation_id,ordinal,chunk_text,chunk_sha256,chunk_bytes)
 VALUES('e3ce9307-f9a7-4795-911a-1967ea7c710f',v_ordinal,v_text,v_sha,v_bytes) ON CONFLICT(reservation_id,ordinal) DO NOTHING;
GET DIAGNOSTICS v_new=ROW_COUNT;
IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.ref01_connector_response_chunk_v1 q
 WHERE q.reservation_id='e3ce9307-f9a7-4795-911a-1967ea7c710f' AND q.ordinal=v_ordinal
 AND q.chunk_text=v_text AND q.chunk_sha256=v_sha AND q.chunk_bytes=v_bytes) THEN RAISE EXCEPTION 'DHC_IMMUTABLE_CHUNK_REPLAY_CONFLICT';END IF;
SELECT coalesce(sum(chunk_bytes),0) INTO v_sum FROM population_top3000_202608_202609_v1.ref01_connector_response_chunk_v1
 WHERE reservation_id='e3ce9307-f9a7-4795-911a-1967ea7c710f';
IF v_sum>4170523 OR v_sum+4170523>8388608 THEN RAISE EXCEPTION 'DHC_STAGING_AND_FINAL_COPY_RESERVATION_BOUND';END IF;
IF (SELECT coalesce(sum(pg_total_relation_size(r.oid)),0) FROM pg_class r JOIN pg_namespace n ON n.oid=r.relnamespace
 WHERE n.nspname='population_top3000_202608_202609_v1' AND r.relkind IN('r','m'))>
 coalesce(nullif(p.budget_evidence#>>'{resource_limits,max_relation_bytes}','')::bigint,0) THEN RAISE EXCEPTION 'DHC_STAGING_RELATION_CAP';END IF;
PERFORM set_config('top3000.ref01_chunk_new_rows',v_new::text,true);
END $insert$;
SELECT current_setting('top3000.ref01_chunk_ordinal')::integer ordinal,
 current_setting('top3000.ref01_chunk_new_rows')::integer new_rows,count(*) staged_chunks,sum(chunk_bytes) staged_bytes
FROM population_top3000_202608_202609_v1.ref01_connector_response_chunk_v1
WHERE reservation_id='e3ce9307-f9a7-4795-911a-1967ea7c710f';
COMMIT;
