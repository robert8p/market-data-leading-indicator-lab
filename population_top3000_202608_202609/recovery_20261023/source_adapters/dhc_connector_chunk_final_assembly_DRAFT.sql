-- ROOT ONLY: splice this DO block AFTER BEGIN/advisory lock in the existing corrected v2 retain script,
-- BEFORE its DO $retain$ block. Keep the existing observed_at/tool_name/outcome/html_json_path GUCs.
-- Do not COMMIT between assembly, the existing retain block and the guarded DELETE below.
DO $assemble$ DECLARE c population_top3000_202608_202609_v1.acquisition_control%rowtype;
p population_top3000_202608_202609_v1.pipeline_control%rowtype;
v_content text;v_count bigint;v_bytes bigint;v_bad bigint;v_first integer;v_last integer;v_json jsonb;v_html text;
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
SELECT count(*),sum(q.chunk_bytes),min(q.ordinal),max(q.ordinal),
 count(*) FILTER(WHERE q.chunk_bytes IS DISTINCT FROM octet_length(q.chunk_text)
 OR q.chunk_sha256 IS DISTINCT FROM encode(sha256(convert_to(q.chunk_text,'UTF8')),'hex')),
 string_agg(q.chunk_text,'' ORDER BY q.ordinal)
INTO v_count,v_bytes,v_first,v_last,v_bad,v_content
FROM population_top3000_202608_202609_v1.ref01_connector_response_chunk_v1 q
WHERE q.reservation_id='e3ce9307-f9a7-4795-911a-1967ea7c710f';
IF v_count IS DISTINCT FROM 28::bigint OR v_first IS DISTINCT FROM 1 OR v_last IS DISTINCT FROM 28 OR v_bad IS DISTINCT FROM 0::bigint
 OR v_bytes IS DISTINCT FROM 4170523::bigint OR octet_length(v_content) IS DISTINCT FROM 4170523
 OR encode(sha256(convert_to(v_content,'UTF8')),'hex') IS DISTINCT FROM 'f1566bf70fb00afdb1f53620899960f50593c23aa15a50cbdee74ed2110ff856'
 THEN RAISE EXCEPTION 'DHC_ORDERED_ASSEMBLY_HASH_OR_CENSUS';END IF;
v_json:=v_content::jsonb;v_html:=v_json#>>'{structuredContent,result}';
IF v_json->'isError' IS DISTINCT FROM 'false'::jsonb OR jsonb_typeof(v_json#>'{structuredContent,result}') IS DISTINCT FROM 'string'
 OR v_html IS DISTINCT FROM v_json#>>'{content,0,text}' OR octet_length(v_html) IS DISTINCT FROM 2021465
 OR encode(sha256(convert_to(v_html,'UTF8')),'hex') IS DISTINCT FROM '997651f0cda678bc41d6930ac63f4ac6cf91bbaad5aba43b1426f8485fb4389a'
 THEN RAISE EXCEPTION 'DHC_EXACT_TWO_HTML_COPIES_REQUIRED';END IF;
PERFORM set_config('top3000.ref01_reservation_id','e3ce9307-f9a7-4795-911a-1967ea7c710f',true);
PERFORM set_config('top3000.ref01_content_base64',encode(convert_to(v_content,'UTF8'),'base64'),true);
PERFORM set_config('top3000.ref01_content_sha256','f1566bf70fb00afdb1f53620899960f50593c23aa15a50cbdee74ed2110ff856',true);
PERFORM set_config('top3000.ref01_html_json_path','["structuredContent","result"]',true);
END $assemble$;

-- APPEND THE EXISTING CORRECTED v2 DO $retain$ BLOCK HERE, then this deletion before COMMIT.
-- It performs the only stored_bytes accounting and release of this exact8MiB reservation.
-- The retain block must accept the real XML declaration/comments prefix (root already corrected it).
/* DELETE_BLOCK_START
DO $clear_staging$ DECLARE v_deleted integer;
BEGIN
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.source_document d
 WHERE d.source_id='ref01-massive-connector:e3ce9307-f9a7-4795-911a-1967ea7c710f'
 AND d.source_sha256='f1566bf70fb00afdb1f53620899960f50593c23aa15a50cbdee74ed2110ff856'
 AND d.content_bytes=4170523 AND encode(sha256(convert_to(d.content_text,'UTF8')),'hex')=d.source_sha256
 AND d.evidence#>>'{html_extraction,utf8_sha256}'='997651f0cda678bc41d6930ac63f4ac6cf91bbaad5aba43b1426f8485fb4389a'
 AND d.evidence#>>'{html_extraction,utf8_bytes}'='2021465')
 OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_event e
 WHERE e.event='ROOT_REF01_CONNECTOR_RETAINED_V1' AND e.details->>'reservation_id'='e3ce9307-f9a7-4795-911a-1967ea7c710f'
 AND e.details->>'source_sha256'='f1566bf70fb00afdb1f53620899960f50593c23aa15a50cbdee74ed2110ff856'
 AND e.details->>'outcome_class'='SUCCESS') THEN RAISE EXCEPTION 'DHC_SOURCE_NOT_COMMITTED_IN_CURRENT_TRANSACTION';END IF;
 DELETE FROM population_top3000_202608_202609_v1.ref01_connector_response_chunk_v1
 WHERE reservation_id='e3ce9307-f9a7-4795-911a-1967ea7c710f';
 GET DIAGNOSTICS v_deleted=ROW_COUNT;
 IF v_deleted<>28 THEN RAISE EXCEPTION 'DHC_STAGING_DELETE_CENSUS';END IF;
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES('ROOT_REF01_CONNECTOR_CHUNKS_ASSEMBLED_V1',
 jsonb_build_object('reservation_id','e3ce9307-f9a7-4795-911a-1967ea7c710f','chunks',28,'bytes',4170523,
 'source_sha256','f1566bf70fb00afdb1f53620899960f50593c23aa15a50cbdee74ed2110ff856',
 'provider_calls',0,'stored_byte_accounting','ONCE_BY_EXISTING_RETAIN_BLOCK','staging_deleted_atomically',true));
END $clear_staging$;
DELETE_BLOCK_END */
