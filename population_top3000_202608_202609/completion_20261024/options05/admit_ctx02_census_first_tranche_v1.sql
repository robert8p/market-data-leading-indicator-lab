-- Root only, after actual reviewed installation/rollback/deployment/registration.
-- Initial197 native request allowance plus3 retained options reservations =200; the third reservation was a zero-fetch S3 credential probe.
-- The immutable100000-page technical ceiling is not a source denominator. Source completion requires terminal cursor and independent population/lifecycle evidence.
BEGIN;
SET LOCAL lock_timeout='15s';SET LOCAL statement_timeout='25s';
DO $admit$
DECLARE a population_top3000_202608_202609_v1.acquisition_control%rowtype;p population_top3000_202608_202609_v1.pipeline_control%rowtype;
 m jsonb;v jsonb;h text;k text;tid uuid;admission jsonb;wake jsonb;registered bigint;
BEGIN
 PERFORM pg_advisory_xact_lock(2026083000261022);
 SELECT * INTO STRICT a FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton FOR UPDATE;
 SELECT * INTO STRICT p FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton FOR UPDATE;
 IF a.owner_id IS DISTINCT FROM 'top3000-completion-20261024-root-5db48b32d103' OR a.owner_fence IS DISTINCT FROM 3 OR a.stop_generation IS DISTINCT FROM 1 THEN RAISE EXCEPTION 'CTX02_CENSUS_ROOT_HANDOVER_CHANGED';END IF;
 SELECT event_id INTO registered FROM population_top3000_202608_202609_v1.pipeline_event WHERE event='CTX02_CENSUS_V1_REGISTERED'
 AND details->>'owner_id'=a.owner_id AND details->>'owner_fence'=a.owner_fence::text AND details->>'stop_generation'=a.stop_generation::text
 AND details->>'handler_file_sha256'='1c9032bb84616059d62a052316c8277f21ba0600d3dedc5f3cf5184661283f23' ORDER BY event_id DESC LIMIT 1;
 IF registered IS NULL THEN RAISE EXCEPTION 'CTX02_CENSUS_ACTUAL_REGISTRATION_REQUIRED';END IF;
 IF (SELECT count(*) FROM population_top3000_202608_202609_v1.connector_probe_lease_v1 WHERE lane='options')<>3
 OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.connector_probe_lease_v1 WHERE lane='options' AND (released_at IS NULL OR (((request_key IN('options-census-20260803-page0001-v1','options-census-20260803-page0002-v1') AND receipt->>'state'='SUCCESS') OR (request_key='ctx02-s3-head-20260803-v1' AND receipt->>'state'='CONFIRMED_NOT_DISPATCHED')) IS DISTINCT FROM true)))
 THEN RAISE EXCEPTION 'CTX02_CENSUS_RECONCILE_CURRENT200_CALL_ALLOWANCE';END IF;
 m:=jsonb_build_object('as_of','2026-08-03','expired',false,'population','ALL_PROVIDER_HISTORICAL_REFERENCE_NO_COHORT_FILTER',
 'first_url','https://api.massive.com/v3/reference/options/contracts?as_of=2026-08-03&expired=false&limit=1000&sort=ticker&order=asc',
 'max_response_bytes',1048576,'max_runtime_ms',45000,'max_pages',100000,'scope_role','FULL_ASOF_REFERENCE_SOURCE_FIRST_DATE_COHORT_JOIN_PENDING','source_denominator',NULL);
 h:=encode(sha256(convert_to(m::text,'UTF8')),'hex');k:='ctx02-census-v1:'||h;
 v:=jsonb_build_object('scope_sha256',a.scope_sha256,'CTX02_contract_sha256','474b7aced2e7e97a4b0af5e81f264f93008967669dc4968017b4b48cc1a4b3af',
 'ctx02_census_bundle_sha256',population_top3000_202608_202609_v1.ctx02_census_bundle_v1(),'parser_bundle_sha256',population_top3000_202608_202609_v1.ctx02_parser_bundle_v1());
 INSERT INTO population_top3000_202608_202609_v1.ctx02_census_request_v1(request_key,manifest,manifest_sha256,input_versions)VALUES(k,m,h,v) ON CONFLICT(request_key) DO NOTHING;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.ctx02_census_request_v1 r WHERE r.request_key=k AND r.manifest=m AND r.manifest_sha256=h AND r.input_versions=v) THEN RAISE EXCEPTION 'CTX02_CENSUS_IMMUTABLE_REQUEST_CONFLICT';END IF;
 admission:=population_top3000_202608_202609_v1.ctx02_census_admit_v1(a.owner_id,a.owner_fence,a.stop_generation,k,197,197,least(a.admitted_until,p.deadline,clock_timestamp()+interval '6 hours')-interval '1 second','Continue full historical options reference census with197 additional logical calls after2 reconciled connector pages and1 retained zero-fetch S3 reservation; preserve nonterminal cursor and all scope requirements');
 tid:=population_top3000_202608_202609_v1.pipeline_enqueue_v1('CTX02','ACQUIRE',k,jsonb_build_object('kind','CTX02_CENSUS_SOURCE_V1','request_key',k,'request_manifest_sha256',h),v,NULL);
 wake:=population_top3000_202608_202609_v1.pipeline_wake_v1(a.owner_id,a.owner_fence,'Reviewed native options census admitted with per-page processing successors and finite197-request tranche');
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details)VALUES('CTX02_CENSUS_FIRST_NATIVE_TRANCHE_ENQUEUED_V1',jsonb_build_object('owner_id',a.owner_id,'owner_fence',a.owner_fence,'stop_generation',a.stop_generation,'task_id',tid,'request_key',k,'manifest_sha256',h,'admission',admission,'registration_event_id',registered,'direct_connector_calls_preserved',2,'zero_fetch_s3_reservation_preserved',1,'prior_options_reservations_preserved',3,'source_denominator_known',false,'population_accepted',false,'wake_receipt',wake));
END;$admit$;
COMMIT;
