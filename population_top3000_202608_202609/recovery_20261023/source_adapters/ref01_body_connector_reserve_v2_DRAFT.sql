-- ROOT ONLY v2 DRAFT. Exactly two already-observed filing filenames; no new worker.
-- Preserve 8 prior operations + 2 bodies <=10 total and complete retained representations <=24 MiB.
-- A retention cap is not a transport streaming abort; unresolved results are never silently retried.
-- Uses no new worker, RPC, adapter binding, fake pipeline attempt or automatic retry.
BEGIN;
SET LOCAL statement_timeout='10s';
SET LOCAL lock_timeout='2s';
SELECT pg_advisory_xact_lock(2026083000261022);
-- Root sets top3000.ref01_route_id to 9 (CRD) then 10 (DHC), one transaction/call at a time.
DO $reserve$
DECLARE
 c population_top3000_202608_202609_v1.acquisition_control%rowtype;
 p population_top3000_202608_202609_v1.pipeline_control%rowtype;
 m jsonb;route jsonb;route_id integer;reservation_id uuid;reservation jsonb;prior_receipt jsonb;
 relation_bytes bigint;limit_bytes bigint;budget_total numeric;changed integer;
manifest_text constant text:=$manifest$[{"route_id":9,"family_id":"REF01","case":"CRD","accession":"0001193125-26-330643","expected_cik":"0000025475","kind":"filing_document_body","method":"GET","provider":"Massive","route":"/v1/reference/sec/filings/0001193125-26-330643/files/crda-20260630.htm","parameters":{},"observed_filename":"crda-20260630.htm","provider_filelist_bytes":4812375,"canonical_sec_url":"https://www.sec.gov/Archives/edgar/data/25475/000119312526330643/crda-20260630.htm","metadata_anchor":{"route_id":1,"source_id":"ref01-massive-connector:2d4e9adf-4f48-4957-8f81-82f5341ba97a","source_sha256":"f3cde4b06fc290ae57c000d4e44826f1b397e2c36f09ae3dac00b9001effcbc5","content_bytes":1449},"filelist_anchor":{"route_id":2,"source_id":"ref01-massive-connector:dd2499a5-dfb5-4f02-b005-dedd6c3c7cf7","source_sha256":"e8ed0a06b012509c3e575eedf727e671c2c190d579baf23a5268f71f5ce9ffc7","content_bytes":553},"maximum_connector_calls":1,"maximum_retained_bytes":16777216,"aggregate_wave_maximum_retained_bytes":25165824,"maximum_wave_connector_operations":10,"original_wire_byte_identity_claimed":false},{"route_id":10,"family_id":"REF01","case":"DHC","accession":"0001075415-26-000030","expected_cik":"0001075415","kind":"filing_document_body","method":"GET","provider":"Massive","route":"/v1/reference/sec/filings/0001075415-26-000030/files/dhc-20260630.htm","parameters":{},"observed_filename":"dhc-20260630.htm","provider_filelist_bytes":2021465,"canonical_sec_url":"https://www.sec.gov/Archives/edgar/data/1075415/000107541526000030/dhc-20260630.htm","metadata_anchor":{"route_id":3,"source_id":"ref01-massive-connector:649201bd-b1e3-4187-b67c-3584362756a4","source_sha256":"0e884840a1e6cd29f385cd33f53afa8a5055d8e825dc34ba00374f83217467da","content_bytes":1501},"filelist_anchor":{"route_id":4,"source_id":"ref01-massive-connector:6b6e7182-b756-45a4-adee-3dfe292c482c","source_sha256":"71067d6a4907d7e42a778bda75cd22fdda6d954a6dee15acd9eee4290a556ab6","content_bytes":551},"maximum_connector_calls":1,"maximum_retained_bytes":8388608,"aggregate_wave_maximum_retained_bytes":25165824,"maximum_wave_connector_operations":10,"original_wire_byte_identity_claimed":false}]$manifest$;
metadata_anchors constant text:=$anchors$[{"route_id":1,"source_id":"ref01-massive-connector:2d4e9adf-4f48-4957-8f81-82f5341ba97a","source_sha256":"f3cde4b06fc290ae57c000d4e44826f1b397e2c36f09ae3dac00b9001effcbc5","content_bytes":1449},{"route_id":2,"source_id":"ref01-massive-connector:dd2499a5-dfb5-4f02-b005-dedd6c3c7cf7","source_sha256":"e8ed0a06b012509c3e575eedf727e671c2c190d579baf23a5268f71f5ce9ffc7","content_bytes":553},{"route_id":3,"source_id":"ref01-massive-connector:649201bd-b1e3-4187-b67c-3584362756a4","source_sha256":"0e884840a1e6cd29f385cd33f53afa8a5055d8e825dc34ba00374f83217467da","content_bytes":1501},{"route_id":4,"source_id":"ref01-massive-connector:6b6e7182-b756-45a4-adee-3dfe292c482c","source_sha256":"71067d6a4907d7e42a778bda75cd22fdda6d954a6dee15acd9eee4290a556ab6","content_bytes":551},{"route_id":5,"source_id":"ref01-massive-connector:13c44092-9497-467c-a2cf-f0920922dcd5","source_sha256":"b7360bd56c2984909fcdd99314de5a86d5b0a0553034ca86ba5fe47a7ea32c89","content_bytes":1337},{"route_id":6,"source_id":"ref01-massive-connector:499ad68e-518d-4f8c-81ed-f0f39edf481e","source_sha256":"2deffab097ef74b4e0a0cba2d07ded7f8d9c4ac1d7fe583236bff1adaaf50e48","content_bytes":575},{"route_id":7,"source_id":"ref01-massive-connector:a8fb8e52-230f-4db3-8556-5079a284ce0e","source_sha256":"e7a77f22ca8f6f13c69498e7d2c22aaf50767c7c22b759f96751a9eae953a2c4","content_bytes":1449},{"route_id":8,"source_id":"ref01-massive-connector:76cca03d-7a77-4bd7-bdca-01470b6aed43","source_sha256":"bdde2adb70f3b20711b6848aa99388470492d011b036c1cfba687c9555c1b1f3","content_bytes":581}]$anchors$;
metadata_count bigint;metadata_bytes bigint;body_bytes bigint;prior_body_requests bigint;retention_cap bigint;wave_remaining bigint;
BEGIN
SELECT * INTO c FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton FOR UPDATE;
SELECT * INTO p FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton FOR UPDATE;
IF c.owner_id IS DISTINCT FROM 'top3000-recovery-20261023-root-4b1314bcb5c2' OR c.owner_fence IS DISTINCT FROM 2 OR c.stop_generation IS DISTINCT FROM 0
 OR p.expected_owner_id IS DISTINCT FROM c.owner_id OR p.expected_owner_fence IS DISTINCT FROM c.owner_fence OR p.expected_stop_generation IS DISTINCT FROM c.stop_generation
 OR c.scope_sha256 IS DISTINCT FROM '8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd' OR p.scope_sha256 IS DISTINCT FROM c.scope_sha256
 OR NOT c.enabled OR c.stopped OR c.admitted_until IS NULL OR c.admitted_until<=clock_timestamp() OR NOT p.enabled OR NOT p.shared_scheduler_verified OR p.deadline IS NULL OR p.deadline<=clock_timestamp()
 THEN RAISE EXCEPTION 'CURRENT_ROOT_OWNER_SCOPE_STOP_ADMISSION_REQUIRED';END IF;
IF NOT EXISTS(SELECT 1 FROM cron.job WHERE jobid=596 AND active=false) THEN RAISE EXCEPTION 'CRON_596_MUST_BE_PAUSED';END IF;
IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE status='LEASED')
 OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_provider_lease WHERE released_at IS NULL)
 OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.acquisition_job WHERE status='LEASED')
 THEN RAISE EXCEPTION 'NO_TASK_OR_PROVIDER_LEASE_MAY_OVERLAP_MANUAL_CONNECTOR';END IF;
IF c.hold_reason IS NOT NULL THEN RAISE EXCEPTION 'EXISTING_CONTROL_HOLD_MUST_BE_RECONCILED';END IF;
IF encode(sha256(convert_to(manifest_text,'UTF8')),'hex')<>'7c6c572fb8e4fed3f4e5136d5928a71a3d819d61fbe0697793086da70b55c07f' THEN RAISE EXCEPTION 'MANIFEST_BYTES_CHANGED';END IF;
m:=manifest_text::jsonb;
IF coalesce(current_setting('top3000.ref01_route_id',true),'')!~'^(9|10)$' THEN RAISE EXCEPTION 'EXACT_ONE_OF_TWO_OBSERVED_BODY_ROUTES_REQUIRED';END IF;
route_id:=current_setting('top3000.ref01_route_id')::integer;route:=m->(route_id-9);
IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.catalog_family_contract WHERE family_id='REF01') THEN RAISE EXCEPTION 'REF01_SCOPE_CONTRACT_REQUIRED';END IF;
IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_event e WHERE e.event='ROOT_REF01_CONNECTOR_RESERVED_V1'
 AND e.details->>'manifest_sha256'='7c6c572fb8e4fed3f4e5136d5928a71a3d819d61fbe0697793086da70b55c07f' AND e.details->>'route_id'=route_id::text)
 THEN RAISE EXCEPTION 'ROUTE_ALREADY_RESERVED_NO_AUTOMATIC_REISSUE';END IF;
IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_event e WHERE e.event='ROOT_REF01_CONNECTOR_RESERVED_V1'
 AND NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_event closed WHERE closed.event='ROOT_REF01_CONNECTOR_RETAINED_V1'
  AND closed.details->>'reservation_id'=e.details->>'reservation_id')) THEN RAISE EXCEPTION 'EARLIER_MANUAL_CONNECTOR_OUTCOME_UNRESOLVED';END IF;
-- The first eight actual responses are immutable dependencies, not just expected source names.
SELECT count(*),coalesce(sum(d.content_bytes),0) INTO metadata_count,metadata_bytes
FROM jsonb_to_recordset(metadata_anchors::jsonb) AS a(source_id text,source_sha256 text,content_bytes bigint,route_id integer)
JOIN population_top3000_202608_202609_v1.source_document d ON d.source_id=a.source_id AND d.source_sha256=a.source_sha256
 AND d.content_bytes=a.content_bytes AND octet_length(d.content_text)=a.content_bytes
 AND encode(sha256(convert_to(d.content_text,'UTF8')),'hex')=a.source_sha256
WHERE EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_event e
 WHERE e.event='ROOT_REF01_CONNECTOR_RETAINED_V1' AND e.details->>'manifest_sha256'='1ebab0c5574d665162f5ee13b98a7f8632fca153b95798d500851f00b35170b9'
 AND e.details->>'source_id'=a.source_id AND e.details->>'source_sha256'=a.source_sha256 AND e.details->>'outcome_class'='SUCCESS');
IF metadata_count IS DISTINCT FROM 8::bigint OR metadata_bytes IS DISTINCT FROM 7996::bigint
 OR (SELECT count(*) FROM population_top3000_202608_202609_v1.pipeline_event e WHERE e.event='ROOT_REF01_CONNECTOR_RESERVED_V1' AND e.details->>'manifest_sha256'='1ebab0c5574d665162f5ee13b98a7f8632fca153b95798d500851f00b35170b9')<>8
 THEN RAISE EXCEPTION 'EXACT_EIGHT_METADATA_RECEIPTS_REQUIRED';END IF;
SELECT count(*) INTO prior_body_requests FROM population_top3000_202608_202609_v1.pipeline_event e
 WHERE e.event='ROOT_REF01_CONNECTOR_RESERVED_V1' AND e.details->>'manifest_sha256'='7c6c572fb8e4fed3f4e5136d5928a71a3d819d61fbe0697793086da70b55c07f';
SELECT coalesce(sum((e.details->>'retained_bytes')::bigint),0) INTO body_bytes FROM population_top3000_202608_202609_v1.pipeline_event e
 WHERE e.event='ROOT_REF01_CONNECTOR_RETAINED_V1' AND e.details->>'manifest_sha256'='7c6c572fb8e4fed3f4e5136d5928a71a3d819d61fbe0697793086da70b55c07f';
IF prior_body_requests IS DISTINCT FROM (route_id-9)::bigint THEN RAISE EXCEPTION 'BODY_WAVE_SEQUENCE_OR_REQUEST_LIMIT';END IF;
wave_remaining:=25165824-metadata_bytes-body_bytes;
retention_cap:=least((route->>'maximum_retained_bytes')::bigint,wave_remaining);
IF retention_cap<(route->>'provider_filelist_bytes')::bigint OR retention_cap<=0 THEN RAISE EXCEPTION 'AGGREGATE_24_MIB_WAVE_HEADROOM_REQUIRED';END IF;
IF p.budget_verified_at IS NULL OR coalesce((p.budget_evidence->>'provider_admissions_allowed')::boolean,false) IS NOT TRUE
 OR (coalesce(p.budget_evidence->'allowed_providers','[]'::jsonb) ? 'Massive') IS NOT TRUE THEN RAISE EXCEPTION 'CURRENT_MASSIVE_ADMISSION_REQUIRED';END IF;
limit_bytes:=nullif(p.budget_evidence#>>'{resource_limits,max_relation_bytes}','')::bigint;
SELECT coalesce(sum(pg_total_relation_size(r.oid)),0) INTO relation_bytes FROM pg_class r JOIN pg_namespace n ON n.oid=r.relnamespace
 WHERE n.nspname='population_top3000_202608_202609_v1' AND r.relkind IN('r','m');
IF limit_bytes IS NULL OR relation_bytes>=limit_bytes THEN RAISE EXCEPTION 'MISSION_RELATION_CAP';END IF;
budget_total:=p.actual_usd+p.accrued_usd+p.unavoidable_usd+p.remaining_uncommitted_usd+p.repair_egress_reserve_usd+p.reserved_request_usd;
IF budget_total IS NULL OR budget_total>250 OR c.provider_requests>=c.max_provider_requests OR c.http_invocations>=c.max_http_invocations
 OR c.stored_bytes+c.reserved_bytes+retention_cap>c.max_source_bytes THEN RAISE EXCEPTION 'BUDGET_REQUEST_OR_RAW_CAP';END IF;
IF c.next_provider_at>clock_timestamp() THEN RAISE EXCEPTION 'PROVIDER_PACING_WINDOW_NOT_READY';END IF;
reservation_id:=gen_random_uuid();
reservation:=jsonb_build_object('reservation_id',reservation_id,'manifest_sha256','7c6c572fb8e4fed3f4e5136d5928a71a3d819d61fbe0697793086da70b55c07f','route_id',route_id,'route',route,
 'controller',c.owner_id,'owner_fence',c.owner_fence,'stop_generation',c.stop_generation,'scope_sha256',c.scope_sha256,
 'reserved_at',clock_timestamp(),'review_after',clock_timestamp()+interval '5 minutes','reserved_bytes',retention_cap,'aggregate_wave_maximum_retained_bytes',25165824,'aggregate_wave_previously_retained_bytes',metadata_bytes+body_bytes,'aggregate_wave_remaining_before_reservation',wave_remaining,'prior_metadata_manifest_sha256','1ebab0c5574d665162f5ee13b98a7f8632fca153b95798d500851f00b35170b9','content_fetch_admission_version',2,'reserved_incremental_usd',0,
 'mode','ROOT_DIRECT_CONNECTOR_NO_PIPELINE_ADAPTER_EXECUTED','source_representation','CONNECTOR_REPRESENTATION_NOT_WIRE',
 'provider_counter_increment',1,'http_counter_increment',1,'http_counter_meaning','CONSERVATIVE_CONNECTOR_OPERATION_ALLOWANCE_NOT_OBSERVED_EDGE_INVOCATION',
 'wire_request_count_observed',false,'connector_internal_retries_observed',false,'automatic_retry',false,'automatic_pagination',false,
 'automatic_normalization_or_publication',false,'content_fetch_admitted',true,'invoice_reconciled',false,
 'response_byte_limit_kind','RETAINED_REPRESENTATION_CAP; connector transport cap not independently controllable',
 'max_relation_bytes',limit_bytes,'budget_total_at_reservation',budget_total);
UPDATE population_top3000_202608_202609_v1.acquisition_control SET provider_requests=provider_requests+1,http_invocations=http_invocations+1,
 reserved_bytes=reserved_bytes+retention_cap,next_provider_at=clock_timestamp()+interval '500 milliseconds',
 hold_reason='ROOT_CONNECTOR_RESERVATION:'||reservation_id::text,updated_at=clock_timestamp() WHERE singleton;
INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES('ROOT_REF01_CONNECTOR_RESERVED_V1',reservation);
PERFORM set_config('top3000.ref01_reservation_id',reservation_id::text,true);
END $reserve$;
SELECT details AS reservation FROM population_top3000_202608_202609_v1.pipeline_event
 WHERE event='ROOT_REF01_CONNECTOR_RESERVED_V1' AND details->>'reservation_id'=current_setting('top3000.ref01_reservation_id') ORDER BY at DESC LIMIT 1;
COMMIT;
