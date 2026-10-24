-- Private, bounded import of exact connector-returned text under a root-admitted metadata manifest.
-- No remote requests; source-request counters are reconciled independently by the controller.
CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.connector_source_import_v1(
 p_owner text,p_fence bigint,p_stop bigint,p_manifest_event bigint,p_source_id text,p_body text)
RETURNS jsonb LANGUAGE plpgsql SET search_path TO pg_catalog AS $f$
DECLARE c population_top3000_202608_202609_v1.acquisition_control%rowtype;
 p population_top3000_202608_202609_v1.pipeline_control%rowtype;
 existing population_top3000_202608_202609_v1.source_document%rowtype;
 manifest jsonb;spec jsonb;ev jsonb;h text;b bigint;n integer;relation_limit bigint;relation_bytes bigint;authority_id text;started timestamptz:=clock_timestamp();
BEGIN
 PERFORM pg_advisory_xact_lock(2026083000261022);
 SELECT * INTO c FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton FOR UPDATE;
 IF NOT FOUND THEN RAISE EXCEPTION 'CONNECTOR_IMPORT_CONTROL_MISSING';END IF;
 SELECT * INTO p FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton FOR UPDATE;
 IF NOT FOUND OR p_owner IS NULL OR p_fence IS NULL OR p_stop IS NULL
 OR c.owner_id IS DISTINCT FROM p_owner OR c.owner_fence IS DISTINCT FROM p_fence OR c.stop_generation IS DISTINCT FROM p_stop
 OR p.expected_owner_id IS DISTINCT FROM p_owner OR p.expected_owner_fence IS DISTINCT FROM p_fence OR p.expected_stop_generation IS DISTINCT FROM p_stop
 OR c.stopped IS DISTINCT FROM false OR c.enabled IS DISTINCT FROM true OR p.enabled IS DISTINCT FROM true
 OR c.hold_reason IS NOT NULL OR c.admitted_until IS NULL OR c.admitted_until<=clock_timestamp()
 OR c.scope_sha256 IS DISTINCT FROM '8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd'
 OR p.scope_sha256 IS DISTINCT FROM c.scope_sha256 OR p.shared_scheduler_verified IS DISTINCT FROM true
 OR p.deadline IS NULL OR p.deadline<=clock_timestamp() THEN RAISE EXCEPTION 'CONNECTOR_IMPORT_CURRENT_CONTROLLER_REQUIRED';END IF;
 authority_id:=p.budget_evidence#>>'{monetary_policy,authority_event_id}';
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_event e WHERE e.event_id::text=authority_id AND e.event='USER_AUTHORITY_RESUMED_20261024' AND e.details->>'owner_id'=p_owner AND e.details->>'owner_fence'=p_fence::text AND e.details->>'stop_generation'=p_stop::text AND e.details->>'scope_sha256'=c.scope_sha256) THEN RAISE EXCEPTION 'CONNECTOR_IMPORT_CURRENT_AUTHORITY_REQUIRED';END IF;
 relation_limit:=(p.budget_evidence#>>'{resource_limits,max_relation_bytes}')::bigint;
 SELECT coalesce(sum(pg_total_relation_size(r.oid)),0)::bigint INTO relation_bytes FROM pg_class r JOIN pg_namespace n ON n.oid=r.relnamespace WHERE n.nspname='population_top3000_202608_202609_v1' AND r.relkind IN('r','m');
 IF relation_limit IS NULL OR relation_limit<=0 OR relation_bytes>relation_limit THEN RAISE EXCEPTION 'CONNECTOR_IMPORT_RELATION_BOUND';END IF;
 SELECT details INTO manifest FROM population_top3000_202608_202609_v1.pipeline_event
 WHERE event_id=p_manifest_event AND event='CONNECTOR_SOURCE_MANIFEST_ADMITTED_V1'
 AND details->>'owner_id'=p_owner AND details->>'owner_fence'=p_fence::text AND details->>'stop_generation'=p_stop::text
 AND details->>'scope_sha256'=c.scope_sha256 AND details->>'authority_event_id'=authority_id;
 IF manifest IS NULL OR jsonb_typeof(manifest->'documents') IS DISTINCT FROM 'array'
 OR jsonb_array_length(manifest->'documents') NOT BETWEEN 1 AND 100
 OR manifest->>'source_completeness_verified' IS DISTINCT FROM 'false'
 OR (manifest->>'independent_review_sha256'~'^[0-9a-f]{64}$') IS DISTINCT FROM true
 THEN RAISE EXCEPTION 'CONNECTOR_IMPORT_REVIEWED_MANIFEST_REQUIRED';END IF;
 SELECT count(*) INTO n FROM jsonb_array_elements(manifest->'documents')r WHERE r->>'source_id'=p_source_id;
 IF n<>1 THEN RAISE EXCEPTION 'CONNECTOR_IMPORT_EXACT_SOURCE_REQUIRED';END IF;
 SELECT r INTO spec FROM jsonb_array_elements(manifest->'documents')r WHERE r->>'source_id'=p_source_id;
 b:=octet_length(p_body);h:=encode(sha256(convert_to(p_body,'UTF8')),'hex');
 IF b IS NULL OR b NOT BETWEEN 1 AND 524288 OR b IS DISTINCT FROM (spec->>'content_bytes')::bigint OR h IS DISTINCT FROM spec->>'source_sha256'
 OR spec->>'source_provider' IS NULL OR length(spec->>'source_provider') NOT BETWEEN 1 AND 128
 OR spec->>'source_uri' IS NULL OR spec->>'source_uri' NOT LIKE 'connector:%' OR length(spec->>'source_uri') NOT BETWEEN 1 AND 2048
 OR spec->'source_published_at' IS DISTINCT FROM 'null'::jsonb OR spec->'historical_received_at' IS DISTINCT FROM 'null'::jsonb
 OR jsonb_typeof(spec->'evidence') IS DISTINCT FROM 'object' OR spec#>>'{evidence,body_role}' IS DISTINCT FROM 'EXACT_CONNECTOR_RETURNED_TEXT_NOT_PROVIDER_HTTP_WIRE'
 OR spec#>>'{evidence,provider_original_receipt_proven}' IS DISTINCT FROM 'false'
 OR spec#>>'{evidence,company_membership_or_rank_decision_made}' IS DISTINCT FROM 'false'
 OR (spec->>'retrieved_at')::timestamptz>clock_timestamp() OR spec->>'retrieved_at' IS NULL
 THEN RAISE EXCEPTION 'CONNECTOR_IMPORT_EXACT_TEXT_OR_PROVENANCE_REQUIRED';END IF;
 ev:=spec->'evidence'||jsonb_build_object('scope_sha256',c.scope_sha256,'manifest_event_id',p_manifest_event,
 'owner_id',p_owner,'owner_fence',p_fence,'stop_generation',p_stop,'authority_event_id',authority_id,'import_revision','connector-source-import-v1',
 'source_complete',false,'strict_original_receipt_qualified',false,'source_publication_inferred',false,
 'source_request_counters_changed_by_import',false,'provider_requests_during_import',0,
 'actual_cost_invoice_reconciled',false,'current_database_transfer_at',clock_timestamp());
 SELECT * INTO existing FROM population_top3000_202608_202609_v1.source_document WHERE source_id=p_source_id;
 IF FOUND THEN
  IF existing.source_sha256 IS DISTINCT FROM h OR existing.content_bytes IS DISTINCT FROM b OR existing.content_text IS DISTINCT FROM p_body
  OR existing.source_uri IS DISTINCT FROM spec->>'source_uri' OR existing.source_provider IS DISTINCT FROM spec->>'source_provider'
  OR existing.retrieved_at IS DISTINCT FROM (spec->>'retrieved_at')::timestamptz
  OR existing.source_published_at IS NOT NULL OR existing.historical_received_at IS NOT NULL
  OR existing.evidence-'current_database_transfer_at' IS DISTINCT FROM ev-'current_database_transfer_at'
  THEN RAISE EXCEPTION 'CONNECTOR_IMPORT_IMMUTABLE_REPLAY_CONFLICT';END IF;
  RETURN jsonb_build_object('state','REUSED','source_id',p_source_id,'sha256',h,'new_bytes',0,'provider_requests',0);
 END IF;
 IF c.stored_bytes+c.reserved_bytes+b>c.max_source_bytes THEN RAISE EXCEPTION 'CONNECTOR_IMPORT_FINITE_STORAGE_BOUND';END IF;
 INSERT INTO population_top3000_202608_202609_v1.source_document(source_id,source_sha256,source_uri,source_provider,retrieved_at,source_published_at,historical_received_at,content_text,content_bytes,evidence,license_policy)
 VALUES(p_source_id,h,spec->>'source_uri',spec->>'source_provider',(spec->>'retrieved_at')::timestamptz,NULL,NULL,p_body,b,ev,
 'PRIVATE_MISSION_EVIDENCE_CONNECTED_ACCESS_NO_REDISTRIBUTION_RIGHTS_INFERRED');
 UPDATE population_top3000_202608_202609_v1.acquisition_control SET stored_bytes=stored_bytes+b,last_useful_progress_at=clock_timestamp(),updated_at=clock_timestamp() WHERE singleton;
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES('CONNECTOR_EXACT_TEXT_SOURCE_COMMITTED_V1',
 jsonb_build_object('source_id',p_source_id,'sha256',h,'manifest_event_id',p_manifest_event,'new_bytes',b,'owner_fence',p_fence,'stop_generation',p_stop,
 'provider_requests_during_import',0,'source_request_counters_preserved',true,'source_completeness_verified',false,'original_historical_receipt_proven',false));
 SELECT coalesce(sum(pg_total_relation_size(r.oid)),0)::bigint INTO relation_bytes FROM pg_class r JOIN pg_namespace n ON n.oid=r.relnamespace WHERE n.nspname='population_top3000_202608_202609_v1' AND r.relkind IN('r','m');
 IF relation_bytes>relation_limit THEN RAISE EXCEPTION 'CONNECTOR_IMPORT_RELATION_BOUND';END IF;
 IF clock_timestamp()-started>interval '5 seconds' THEN RAISE EXCEPTION 'CONNECTOR_IMPORT_TIME_BOUND';END IF;
 RETURN jsonb_build_object('state','SOURCE_COMMITTED','source_id',p_source_id,'sha256',h,'new_bytes',b,'provider_requests',0);
END $f$;
REVOKE ALL ON FUNCTION population_top3000_202608_202609_v1.connector_source_import_v1(text,bigint,bigint,bigint,text,text) FROM PUBLIC,anon,authenticated;
