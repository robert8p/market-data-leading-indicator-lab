-- ROOT ONLY DRAFT. Do not execute before controller review.
-- Uses no new worker, RPC, adapter binding, fake pipeline attempt or automatic retry.
BEGIN;
SET LOCAL statement_timeout='10s';
SET LOCAL lock_timeout='2s';
SELECT pg_advisory_xact_lock(2026083000261022);
-- Root sets top3000.ref01_route_id to one integer1..8 after BEGIN.
DO $reserve$
DECLARE
 c population_top3000_202608_202609_v1.acquisition_control%rowtype;
 p population_top3000_202608_202609_v1.pipeline_control%rowtype;
 m jsonb;route jsonb;route_id integer;reservation_id uuid;reservation jsonb;prior_receipt jsonb;
 relation_bytes bigint;limit_bytes bigint;budget_total numeric;changed integer;
manifest_text constant text:=$manifest$[{"route_id":1,"family_id":"REF01","case":"CRD","accession":"0001193125-26-330643","expected_cik":"0000025475","kind":"metadata","method":"GET","provider":"Massive","route":"/v1/reference/sec/filings/0001193125-26-330643","parameters":{},"maximum_connector_calls":1,"maximum_retained_bytes":8388608},{"route_id":2,"family_id":"REF01","case":"CRD","accession":"0001193125-26-330643","expected_cik":"0000025475","kind":"files_sequence1","method":"GET","provider":"Massive","route":"/v1/reference/sec/filings/0001193125-26-330643/files","parameters":{"sequence":1,"sort":"sequence","order":"asc","limit":10},"maximum_connector_calls":1,"maximum_retained_bytes":8388608},{"route_id":3,"family_id":"REF01","case":"DHC","accession":"0001075415-26-000030","expected_cik":"0001075415","kind":"metadata","method":"GET","provider":"Massive","route":"/v1/reference/sec/filings/0001075415-26-000030","parameters":{},"maximum_connector_calls":1,"maximum_retained_bytes":8388608},{"route_id":4,"family_id":"REF01","case":"DHC","accession":"0001075415-26-000030","expected_cik":"0001075415","kind":"files_sequence1","method":"GET","provider":"Massive","route":"/v1/reference/sec/filings/0001075415-26-000030/files","parameters":{"sequence":1,"sort":"sequence","order":"asc","limit":10},"maximum_connector_calls":1,"maximum_retained_bytes":8388608},{"route_id":5,"family_id":"REF01","case":"LIBERTY_LIVE","accession":"0001104659-26-092020","expected_cik":"0002078416","kind":"metadata","method":"GET","provider":"Massive","route":"/v1/reference/sec/filings/0001104659-26-092020","parameters":{},"maximum_connector_calls":1,"maximum_retained_bytes":8388608},{"route_id":6,"family_id":"REF01","case":"LIBERTY_LIVE","accession":"0001104659-26-092020","expected_cik":"0002078416","kind":"files_sequence1","method":"GET","provider":"Massive","route":"/v1/reference/sec/filings/0001104659-26-092020/files","parameters":{"sequence":1,"sort":"sequence","order":"asc","limit":10},"maximum_connector_calls":1,"maximum_retained_bytes":8388608},{"route_id":7,"family_id":"REF01","case":"FORMULA_ONE","accession":"0001104659-26-091894","expected_cik":"0001560385","kind":"metadata","method":"GET","provider":"Massive","route":"/v1/reference/sec/filings/0001104659-26-091894","parameters":{},"maximum_connector_calls":1,"maximum_retained_bytes":8388608},{"route_id":8,"family_id":"REF01","case":"FORMULA_ONE","accession":"0001104659-26-091894","expected_cik":"0001560385","kind":"files_sequence1","method":"GET","provider":"Massive","route":"/v1/reference/sec/filings/0001104659-26-091894/files","parameters":{"sequence":1,"sort":"sequence","order":"asc","limit":10},"maximum_connector_calls":1,"maximum_retained_bytes":8388608}]$manifest$;
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
IF encode(sha256(convert_to(manifest_text,'UTF8')),'hex')<>'1ebab0c5574d665162f5ee13b98a7f8632fca153b95798d500851f00b35170b9' THEN RAISE EXCEPTION 'MANIFEST_BYTES_CHANGED';END IF;
m:=manifest_text::jsonb;
IF coalesce(current_setting('top3000.ref01_route_id',true),'')!~'^[1-8]$' THEN RAISE EXCEPTION 'EXACT_ONE_OF_EIGHT_ROUTES_REQUIRED';END IF;
route_id:=current_setting('top3000.ref01_route_id')::integer;route:=m->(route_id-1);
IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.catalog_family_contract WHERE family_id='REF01') THEN RAISE EXCEPTION 'REF01_SCOPE_CONTRACT_REQUIRED';END IF;
IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_event e WHERE e.event='ROOT_REF01_CONNECTOR_RESERVED_V1'
 AND e.details->>'manifest_sha256'='1ebab0c5574d665162f5ee13b98a7f8632fca153b95798d500851f00b35170b9' AND e.details->>'route_id'=route_id::text)
 THEN RAISE EXCEPTION 'ROUTE_ALREADY_RESERVED_NO_AUTOMATIC_REISSUE';END IF;
IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_event e WHERE e.event='ROOT_REF01_CONNECTOR_RESERVED_V1'
 AND NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_event closed WHERE closed.event='ROOT_REF01_CONNECTOR_RETAINED_V1'
  AND closed.details->>'reservation_id'=e.details->>'reservation_id')) THEN RAISE EXCEPTION 'EARLIER_MANUAL_CONNECTOR_OUTCOME_UNRESOLVED';END IF;
IF p.budget_verified_at IS NULL OR coalesce((p.budget_evidence->>'provider_admissions_allowed')::boolean,false) IS NOT TRUE
 OR (coalesce(p.budget_evidence->'allowed_providers','[]'::jsonb) ? 'Massive') IS NOT TRUE THEN RAISE EXCEPTION 'CURRENT_MASSIVE_ADMISSION_REQUIRED';END IF;
limit_bytes:=nullif(p.budget_evidence#>>'{resource_limits,max_relation_bytes}','')::bigint;
SELECT coalesce(sum(pg_total_relation_size(r.oid)),0) INTO relation_bytes FROM pg_class r JOIN pg_namespace n ON n.oid=r.relnamespace
 WHERE n.nspname='population_top3000_202608_202609_v1' AND r.relkind IN('r','m');
IF limit_bytes IS NULL OR relation_bytes>=limit_bytes THEN RAISE EXCEPTION 'MISSION_RELATION_CAP';END IF;
budget_total:=p.actual_usd+p.accrued_usd+p.unavoidable_usd+p.remaining_uncommitted_usd+p.repair_egress_reserve_usd+p.reserved_request_usd;
IF budget_total IS NULL OR budget_total>250 OR c.provider_requests>=c.max_provider_requests OR c.http_invocations>=c.max_http_invocations
 OR c.stored_bytes+c.reserved_bytes+8388608>c.max_source_bytes THEN RAISE EXCEPTION 'BUDGET_REQUEST_OR_RAW_CAP';END IF;
IF c.next_provider_at>clock_timestamp() THEN RAISE EXCEPTION 'PROVIDER_PACING_WINDOW_NOT_READY';END IF;
reservation_id:=gen_random_uuid();
reservation:=jsonb_build_object('reservation_id',reservation_id,'manifest_sha256','1ebab0c5574d665162f5ee13b98a7f8632fca153b95798d500851f00b35170b9','route_id',route_id,'route',route,
 'controller',c.owner_id,'owner_fence',c.owner_fence,'stop_generation',c.stop_generation,'scope_sha256',c.scope_sha256,
 'reserved_at',clock_timestamp(),'review_after',clock_timestamp()+interval '5 minutes','reserved_bytes',8388608,'reserved_incremental_usd',0,
 'mode','ROOT_DIRECT_CONNECTOR_NO_PIPELINE_ADAPTER_EXECUTED','source_representation','CONNECTOR_REPRESENTATION_NOT_WIRE',
 'provider_counter_increment',1,'http_counter_increment',1,'http_counter_meaning','CONSERVATIVE_CONNECTOR_OPERATION_ALLOWANCE_NOT_OBSERVED_EDGE_INVOCATION',
 'wire_request_count_observed',false,'connector_internal_retries_observed',false,'automatic_retry',false,'automatic_pagination',false,
 'automatic_normalization_or_publication',false,'content_fetch_admitted',false,'invoice_reconciled',false,
 'response_byte_limit_kind','RETAINED_REPRESENTATION_CAP; connector transport cap not independently controllable',
 'max_relation_bytes',limit_bytes,'budget_total_at_reservation',budget_total);
UPDATE population_top3000_202608_202609_v1.acquisition_control SET provider_requests=provider_requests+1,http_invocations=http_invocations+1,
 reserved_bytes=reserved_bytes+8388608,next_provider_at=clock_timestamp()+interval '500 milliseconds',
 hold_reason='ROOT_CONNECTOR_RESERVATION:'||reservation_id::text,updated_at=clock_timestamp() WHERE singleton;
INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES('ROOT_REF01_CONNECTOR_RESERVED_V1',reservation);
PERFORM set_config('top3000.ref01_reservation_id',reservation_id::text,true);
END $reserve$;
SELECT details AS reservation FROM population_top3000_202608_202609_v1.pipeline_event
 WHERE event='ROOT_REF01_CONNECTOR_RESERVED_V1' AND details->>'reservation_id'=current_setting('top3000.ref01_reservation_id') ORDER BY at DESC LIMIT 1;
COMMIT;
