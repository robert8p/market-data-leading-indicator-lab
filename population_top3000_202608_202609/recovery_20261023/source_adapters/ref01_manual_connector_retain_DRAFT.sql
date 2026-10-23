-- ROOT ONLY DRAFT. Do not execute before controller review.
-- Uses no new worker, RPC, adapter binding, fake pipeline attempt or automatic retry.
BEGIN;
SET LOCAL statement_timeout='10s';
SET LOCAL lock_timeout='2s';
SELECT pg_advisory_xact_lock(2026083000261022);
-- Root sets reservation_id, content_base64, content_sha256, observed_at, tool_name,
-- and outcome_class ('SUCCESS' or 'DEFINITIVE_FAILURE') as top3000.ref01_* GUCs after BEGIN.
-- content_base64 encodes the unchanged complete JSON serialization of the returned connector value.
-- Do not pass a rendered/truncated tool-output excerpt or a reformatted jsonb rendering.
DO $retain$
DECLARE
 c population_top3000_202608_202609_v1.acquisition_control%rowtype;
 p population_top3000_202608_202609_v1.pipeline_control%rowtype;
 m jsonb;route jsonb;route_id integer;reservation_id uuid;reservation jsonb;prior_receipt jsonb;
 relation_bytes bigint;limit_bytes bigint;budget_total numeric;changed integer;
raw_bytes bytea;content text;h text;sid text;observed_at timestamptz;tool_name text;outcome_class text;v_evidence jsonb;receipt jsonb;fresh integer;
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
IF coalesce(current_setting('top3000.ref01_reservation_id',true),'')!~'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN RAISE EXCEPTION 'RESERVATION_ID_REQUIRED';END IF;
reservation_id:=current_setting('top3000.ref01_reservation_id')::uuid;
SELECT details INTO reservation FROM population_top3000_202608_202609_v1.pipeline_event WHERE event='ROOT_REF01_CONNECTOR_RESERVED_V1' AND details->>'reservation_id'=reservation_id::text ORDER BY at DESC LIMIT 1;
IF reservation IS NULL OR reservation->>'manifest_sha256'<>'1ebab0c5574d665162f5ee13b98a7f8632fca153b95798d500851f00b35170b9' OR reservation->>'controller' IS DISTINCT FROM c.owner_id
 OR reservation->>'owner_fence' IS DISTINCT FROM c.owner_fence::text OR reservation->>'stop_generation' IS DISTINCT FROM c.stop_generation::text
 OR reservation->>'scope_sha256' IS DISTINCT FROM c.scope_sha256 THEN RAISE EXCEPTION 'RESERVATION_OWNER_OR_MANIFEST_MISMATCH';END IF;
route:=reservation->'route';
raw_bytes:=decode(current_setting('top3000.ref01_content_base64'),'base64');content:=convert_from(raw_bytes,'UTF8');
h:=encode(sha256(raw_bytes),'hex');
IF octet_length(raw_bytes) NOT BETWEEN 1 AND 8388608 OR h IS DISTINCT FROM current_setting('top3000.ref01_content_sha256')
 OR jsonb_typeof(content::jsonb) NOT IN('object','array','string') THEN RAISE EXCEPTION 'COMPLETE_REPRESENTATION_HASH_OR_BOUND_INVALID';END IF;
observed_at:=current_setting('top3000.ref01_observed_at')::timestamptz;tool_name:=current_setting('top3000.ref01_tool_name');outcome_class:=current_setting('top3000.ref01_outcome_class');
IF observed_at<(reservation->>'reserved_at')::timestamptz-interval '5 seconds' OR observed_at>clock_timestamp()+interval '30 seconds'
 OR tool_name IS DISTINCT FROM 'mcp__codex_apps__massive_call_api' OR outcome_class IS NULL OR outcome_class NOT IN('SUCCESS','DEFINITIVE_FAILURE')
 OR (outcome_class='SUCCESS' AND content::jsonb->'isError'='true'::jsonb) THEN RAISE EXCEPTION 'OBSERVED_RESPONSE_METADATA_INVALID';END IF;
sid:='ref01-massive-connector:'||reservation_id::text;
SELECT details INTO prior_receipt FROM population_top3000_202608_202609_v1.pipeline_event WHERE event='ROOT_REF01_CONNECTOR_RETAINED_V1' AND details->>'reservation_id'=reservation_id::text ORDER BY at DESC LIMIT 1;
IF prior_receipt IS NOT NULL THEN
 IF prior_receipt->>'source_sha256' IS DISTINCT FROM h OR prior_receipt->>'outcome_class' IS DISTINCT FROM outcome_class
  OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.source_document d WHERE d.source_id=sid AND d.source_sha256=h AND d.content_text=content AND d.content_bytes=octet_length(raw_bytes) AND d.retrieved_at=observed_at AND d.evidence->>'connector_tool_name'=tool_name AND d.evidence->'request'=route AND d.evidence->>'outcome_class'=outcome_class AND d.source_provider='Massive' AND d.source_published_at IS NULL AND d.historical_received_at IS NULL)
 THEN RAISE EXCEPTION 'EXISTING_CONNECTOR_RECEIPT_CONFLICT';END IF;
 PERFORM set_config('top3000.ref01_source_id',sid,true);RETURN;
END IF;
IF c.hold_reason IS DISTINCT FROM 'ROOT_CONNECTOR_RESERVATION:'||reservation_id::text OR c.reserved_bytes<(reservation->>'reserved_bytes')::bigint THEN RAISE EXCEPTION 'EXACT_MANUAL_RESERVATION_HOLD_REQUIRED';END IF;
v_evidence:=jsonb_build_object('mode','ROOT_DIRECT_CONNECTOR_NO_PIPELINE_ADAPTER_EXECUTED','representation','CONNECTOR_REPRESENTATION_NOT_WIRE',
 'hash_representation','EXACT_UTF8_SERIALIZATION_OF_COMPLETE_RETURNED_CONNECTOR_VALUE','reservation_id',reservation_id,'manifest_sha256','1ebab0c5574d665162f5ee13b98a7f8632fca153b95798d500851f00b35170b9',
 'request',route,'connector_tool_name',tool_name,'outcome_class',outcome_class,'connector_response_observed_at',observed_at,
 'original_wire_bytes_retained',false,'original_wire_hash_verified',false,'provider_http_status_observed',false,'provider_numeric_lexemes_verified',false,
 'connector_internal_retries_observed',false,'current_receipt_is_not_historical_availability',true,'company_mapping_applied',false,
 'expected_cik_is_review_target_not_verified_fact',true,'accession_identity_independently_verified',false,'source_document_content_fetched',false,
 'automatic_normalization_or_publication',false,'full_invoice_reconciled',false);
INSERT INTO population_top3000_202608_202609_v1.source_document
 (source_id,source_sha256,source_uri,source_provider,retrieved_at,source_published_at,historical_received_at,content_text,content_bytes,evidence,license_policy)
VALUES(sid,h,'https://api.massive.com'||(route->>'route')||CASE WHEN route->>'kind'='files_sequence1' THEN '?sequence=1&sort=sequence&order=asc&limit=10' ELSE '' END,
 'Massive',observed_at,NULL,NULL,content,octet_length(raw_bytes),v_evidence,'EXISTING_MASSIVE_SUBSCRIPTION_CONNECTOR_RESPONSE_INTERNAL_USE')
ON CONFLICT(source_id) DO NOTHING;
GET DIAGNOSTICS fresh=ROW_COUNT;
IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.source_document d WHERE d.source_id=sid AND d.source_sha256=h AND d.content_text=content
 AND d.content_bytes=octet_length(raw_bytes) AND d.evidence=v_evidence AND d.retrieved_at=observed_at AND d.source_provider='Massive' AND d.source_uri='https://api.massive.com'||(route->>'route')||CASE WHEN route->>'kind'='files_sequence1' THEN '?sequence=1&sort=sequence&order=asc&limit=10' ELSE '' END AND d.license_policy='EXISTING_MASSIVE_SUBSCRIPTION_CONNECTOR_RESPONSE_INTERNAL_USE' AND d.source_published_at IS NULL AND d.historical_received_at IS NULL)
 THEN RAISE EXCEPTION 'IMMUTABLE_CONNECTOR_SOURCE_CONFLICT';END IF;
limit_bytes:=nullif(p.budget_evidence#>>'{resource_limits,max_relation_bytes}','')::bigint;
SELECT coalesce(sum(pg_total_relation_size(r.oid)),0) INTO relation_bytes FROM pg_class r JOIN pg_namespace n ON n.oid=r.relnamespace
 WHERE n.nspname='population_top3000_202608_202609_v1' AND r.relkind IN('r','m');
IF limit_bytes IS NULL OR relation_bytes>limit_bytes THEN RAISE EXCEPTION 'MISSION_POST_RETAIN_RELATION_CAP';END IF;
UPDATE population_top3000_202608_202609_v1.acquisition_control SET reserved_bytes=reserved_bytes-(reservation->>'reserved_bytes')::bigint,
 stored_bytes=stored_bytes+octet_length(raw_bytes),hold_reason=NULL,last_useful_progress_at=clock_timestamp(),updated_at=clock_timestamp() WHERE singleton;
UPDATE population_top3000_202608_202609_v1.pipeline_control SET last_useful_progress_at=clock_timestamp() WHERE singleton;
receipt:=jsonb_build_object('reservation_id',reservation_id,'manifest_sha256','1ebab0c5574d665162f5ee13b98a7f8632fca153b95798d500851f00b35170b9','route_id',route->'route_id','route',route,
 'source_id',sid,'source_sha256',h,'retained_bytes',octet_length(raw_bytes),'new_source_documents',fresh,'outcome_class',outcome_class,
 'source_representation','CONNECTOR_REPRESENTATION_NOT_WIRE','manual_hold_released',true,'cron_resumed',false,'automatic_retry',false,
 'adapter_executed',false,'company_mapping_applied',false,'publication_admitted',false,'incremental_provider_charge_under_existing_entitlement',0,'full_invoice_reconciled',false);
INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES('ROOT_REF01_CONNECTOR_RETAINED_V1',receipt);
PERFORM set_config('top3000.ref01_source_id',sid,true);
END $retain$;
SELECT d.source_id,d.source_sha256,d.content_bytes,d.retrieved_at,d.evidence,clock_timestamp() verified_at
FROM population_top3000_202608_202609_v1.source_document d WHERE d.source_id=current_setting('top3000.ref01_source_id')
 AND d.source_sha256=encode(sha256(convert_to(d.content_text,'UTF8')),'hex') AND d.content_bytes=octet_length(d.content_text);
COMMIT;
