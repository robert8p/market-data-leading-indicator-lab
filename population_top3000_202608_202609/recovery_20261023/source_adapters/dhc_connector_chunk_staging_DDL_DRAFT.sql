BEGIN;
SET LOCAL statement_timeout='10s';
SET LOCAL lock_timeout='2s';
SET LOCAL standard_conforming_strings=on;
SELECT pg_advisory_xact_lock(2026083000261022);
DO $guard$ DECLARE c population_top3000_202608_202609_v1.acquisition_control%rowtype;
p population_top3000_202608_202609_v1.pipeline_control%rowtype;
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
END $guard$;
CREATE TABLE IF NOT EXISTS population_top3000_202608_202609_v1.ref01_connector_response_chunk_v1(
 reservation_id uuid NOT NULL CHECK(reservation_id='e3ce9307-f9a7-4795-911a-1967ea7c710f'::uuid),
 ordinal integer NOT NULL CHECK(ordinal BETWEEN 1 AND 28),
 chunk_text text NOT NULL,
 chunk_sha256 text NOT NULL CHECK(chunk_sha256 ~ '^[0-9a-f]{64}$'),
 chunk_bytes integer NOT NULL CHECK(chunk_bytes BETWEEN 1 AND 150000),
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(reservation_id,ordinal),
 CHECK(chunk_bytes=octet_length(chunk_text)),
 CHECK(chunk_sha256=encode(sha256(convert_to(chunk_text,'UTF8')),'hex'))
);
REVOKE ALL ON population_top3000_202608_202609_v1.ref01_connector_response_chunk_v1 FROM PUBLIC,anon,authenticated,service_role;
ALTER TABLE population_top3000_202608_202609_v1.ref01_connector_response_chunk_v1 ENABLE ROW LEVEL SECURITY;
-- Existing8MiB reservation covers staging; no stored/request counters change here.
COMMIT;
