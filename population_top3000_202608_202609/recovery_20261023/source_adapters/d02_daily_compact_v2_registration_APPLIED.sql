-- Root-only registration after actual rollback and deployed invalid-token evidence.
-- Captures the current repaired v1 adapters. No global wake, no budget increase.
-- Seeds retained Aug3 + ONE new Aug4 day, both parked until root explicitly releases them.
BEGIN;
SET LOCAL lock_timeout='3s';SET LOCAL statement_timeout='10s';
SELECT pg_advisory_xact_lock(2026083000261022);
DO $admit$
DECLARE bundle text;cal jsonb;vers jsonb;test_evidence jsonb;deploy_evidence jsonb;wrapper_hashes jsonb;legacy_hashes jsonb;ad record;new_fn regprocedure;tid uuid;reuse_tid uuid;
BEGIN
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton AND owner_id='top3000-recovery-20261023-root-4b1314bcb5c2' AND owner_fence=2 AND stop_generation=0 AND enabled AND NOT stopped AND hold_reason IS NULL AND admitted_until>clock_timestamp()) THEN RAISE EXCEPTION 'OWNER_STOP_OR_ADMISSION_CHANGED';END IF;
 IF EXISTS(SELECT 1 FROM cron.job WHERE jobid=596 AND active) THEN RAISE EXCEPTION 'CRON_596_MUST_BE_INACTIVE_DURING_V2_TRANSFER_TEST';END IF;
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE family_id='D02' AND status='LEASED') THEN RAISE EXCEPTION 'D02_LEASE_MUST_FINISH_BEFORE_WRAPPER_TRANSFER';END IF;
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_provider_lease l JOIN population_top3000_202608_202609_v1.pipeline_attempt a USING(attempt_id) JOIN population_top3000_202608_202609_v1.pipeline_task t USING(task_id) WHERE t.family_id='D02' AND l.released_at IS NULL) THEN RAISE EXCEPTION 'D02_PROVIDER_EFFECT_REQUIRES_RECONCILIATION';END IF;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton AND shared_scheduler_verified AND budget_evidence->>'provider_admissions_allowed'='true' AND budget_evidence->'allowed_providers' ? 'Massive') THEN RAISE EXCEPTION 'CURRENT_MASSIVE_RESOURCE_ADMISSION_REQUIRED';END IF;
 PERFORM population_top3000_202608_202609_v1.d02_daily_resource_guard_v2();
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.d02_daily_source_v2) OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE family_id='D02' AND payload->>'kind'='MASSIVE_GROUPED_DAILY_V2') THEN RAISE EXCEPTION 'V2_ALREADY_STARTED_RECONCILE_PRESERVED_PROGRESS';END IF;
 bundle:=population_top3000_202608_202609_v1.d02_daily_bundle_sha256_v2();cal:=population_top3000_202608_202609_v1.d02_daily_calendar_v2();
 SELECT jsonb_object_agg(stage,encode(sha256(convert_to(pg_get_functiondef(('population_top3000_202608_202609_v1.d02_daily_route_'||lower(stage)||'_v2(uuid,text)')::regprocedure),'UTF8')),'hex')) INTO wrapper_hashes FROM (VALUES('ACQUIRE'),('NORMALIZE'),('DERIVE'),('VALIDATE'),('REPAIR')) x(stage);
 SELECT jsonb_object_agg(stage,function_sha256) INTO legacy_hashes FROM population_top3000_202608_202609_v1.pipeline_adapter WHERE family_id='D02' AND stage IN('ACQUIRE','NORMALIZE','DERIVE','VALIDATE','REPAIR');
 IF (SELECT count(*) FROM jsonb_object_keys(legacy_hashes))<>5 THEN RAISE EXCEPTION 'ALL_CURRENT_LEGACY_STAGE_ADAPTERS_REQUIRED';END IF;
 SELECT details INTO test_evidence FROM population_top3000_202608_202609_v1.pipeline_event WHERE event='D02_DAILY_V2_ROLLBACK_VERIFIED' AND details->>'result'='PASS' AND details->>'daily_bundle_sha256'=bundle AND details->'tested_function_sha256'=wrapper_hashes AND details->'preserved_legacy_function_sha256'=legacy_hashes AND details->>'live_provider_requested'='false' AND details->>'provider_accuracy_verified'='false' ORDER BY at DESC LIMIT 1;
 IF test_evidence IS NULL THEN RAISE EXCEPTION 'ACTUAL_CURRENT_V2_AND_PRESERVED_LEGACY_ROLLBACK_EVIDENCE_REQUIRED';END IF;
 SELECT details INTO deploy_evidence FROM population_top3000_202608_202609_v1.pipeline_event WHERE event='D02_DAILY_V2_WORKER_DEPLOYED_VERIFIED' AND details->>'result'='PASS' AND details->>'slug'='population-top3000-daily-v2' AND details->>'worker_sha256'='ec2933dbfa3f92e48179d45a024c97e9731dbc9a3f5909bd9999fe2a0ed120f9' AND details->>'invalid_token_http_status'='403' AND details->>'provider_requests_during_auth_probe'='0' ORDER BY at DESC LIMIT 1;
 IF deploy_evidence IS NULL THEN RAISE EXCEPTION 'CURRENT_V2_WORKER_DEPLOYMENT_AND_AUTH_EVIDENCE_REQUIRED';END IF;
 IF position('population-top3000-daily-v2' IN pg_get_functiondef('population_top3000_202608_202609_v1.pipeline_dispatch_edge_v1(uuid,text,text)'::regprocedure))=0 THEN RAISE EXCEPTION 'ROOT_REVIEWED_V2_EDGE_ALLOWLIST_REQUIRED';END IF;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.d02_grouped_archive_v1 WHERE source_id='d02-massive-grouped:4b94dfcf6c9e0582557862aa631bf1480a12dbc433ed0a53f2b3b844c6db2925:source' AND source_sha256='787163eb9f6ffb408898c8a133c6ca63c822577187793d25c94b436f639bb97b' AND result_count=12402 AND response_bytes=1355997) THEN RAISE EXCEPTION 'ORIGINAL_AUG3_SOURCE_RECEIPT_REQUIRED';END IF;
 FOR ad IN SELECT * FROM population_top3000_202608_202609_v1.pipeline_adapter WHERE family_id='D02' AND stage IN('ACQUIRE','NORMALIZE','DERIVE','VALIDATE','REPAIR') LOOP
  IF NOT ad.enabled OR ad.function_sha256 IS DISTINCT FROM encode(sha256(convert_to(pg_get_functiondef(ad.function_oid),'UTF8')),'hex') OR ad.producer_revision='massive-daily-42-v2' THEN RAISE EXCEPTION 'CURRENT_LEGACY_ADAPTER_NOT_VERIFIED_OR_ALREADY_ROUTED';END IF;
  INSERT INTO population_top3000_202608_202609_v1.d02_daily_legacy_adapter_v2(stage,function_oid,function_sha256,registration_evidence)
  VALUES(ad.stage,ad.function_oid,ad.function_sha256,jsonb_build_object('original_adapter',to_jsonb(ad),'rollback_test',test_evidence,'preserved_without_function_mutation',true)) ON CONFLICT(stage) DO NOTHING;
  IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.d02_daily_legacy_adapter_v2 WHERE stage=ad.stage AND function_oid=ad.function_oid AND function_sha256=ad.function_sha256) THEN RAISE EXCEPTION 'LEGACY_REGISTRY_CONFLICT';END IF;
  new_fn:=('population_top3000_202608_202609_v1.d02_daily_route_'||lower(ad.stage)||'_v2(uuid,text)')::regprocedure;
  UPDATE population_top3000_202608_202609_v1.pipeline_adapter SET function_oid=new_fn::oid,function_sha256=wrapper_hashes->>ad.stage,
   reviewed_test_evidence=jsonb_build_object('actual_sql_test',test_evidence,'worker_deployment',deploy_evidence,'legacy_adapter_captured',to_jsonb(ad),'admission','RETAINED_AUG3_PLUS_ONE_NEW_AUG4_SOURCE_PENDING_CONTROLLER_RELEASE','company_population_verified',false),
   producer_revision='massive-daily-42-v2',expected_batch_seconds=45 WHERE family_id='D02' AND stage=ad.stage;
 END LOOP;
 vers:=jsonb_build_object('scope_sha256','8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd','source_contract_revision','massive-daily-42-v2','daily_bundle_sha256',bundle,'calendar_manifest_sha256',cal->>'calendar_manifest_sha256','retained_aug3_source_sha256','787163eb9f6ffb408898c8a133c6ca63c822577187793d25c94b436f639bb97b');
 reuse_tid:=population_top3000_202608_202609_v1.pipeline_enqueue_v1('D02','ACQUIRE','daily-source-20260803-raw-no-otc-v2',jsonb_build_object('kind','MASSIVE_GROUPED_DAILY_V2','trading_date','2026-08-03','source_mode','REUSE_RETAINED_V1','retained_source_id','d02-massive-grouped:4b94dfcf6c9e0582557862aa631bf1480a12dbc433ed0a53f2b3b844c6db2925:source'),vers);
 tid:=population_top3000_202608_202609_v1.pipeline_enqueue_v1('D02','ACQUIRE','daily-source-20260804-raw-no-otc-v2','{"kind":"MASSIVE_GROUPED_DAILY_V2","trading_date":"2026-08-04","source_mode":"FETCH"}',vers);
 UPDATE population_top3000_202608_202609_v1.pipeline_task SET not_before='infinity' WHERE task_id IN(reuse_tid,tid);
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(task_id,event,details) VALUES(tid,'D02_DAILY_V2_FIRST_WAVE_REGISTERED',jsonb_build_object('owner_fence',2,'stop_generation',0,'daily_bundle_sha256',bundle,'worker_sha256','ec2933dbfa3f92e48179d45a024c97e9731dbc9a3f5909bd9999fe2a0ed120f9','calendar_manifest_sha256',cal->>'calendar_manifest_sha256','frozen_source_dates',42,'registered_dates',jsonb_build_array('2026-08-03','2026-08-04'),'maximum_new_provider_requests',1,'retained_aug3_source_reused',true,'legacy_adapters_preserved',legacy_hashes,'original_one_day_source_and_rows_preserved',true,'compact_batch_rows',1000,'max_response_bytes',8388608,'max_runtime_ms',45000,'budget_increased',false,'requires_controller_release',true,'company_population_verified',false));
END $admit$;
COMMIT;
