-- Root-only first-page admission after real rollback and deployed-worker authentication probes.
-- Does not purchase, extend a budget/deadline, or certify company completeness.
-- Required evidence events must describe actually observed results; do not manufacture PASS records.
BEGIN;
SET LOCAL lock_timeout='3s';
SET LOCAL statement_timeout='10s';
SELECT pg_advisory_xact_lock(2026083000261022);
DO $admit$
DECLARE test_evidence jsonb;deploy_evidence jsonb;current_functions jsonb;ad record;tid uuid;h text;
BEGIN
IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton AND owner_id='top3000-recovery-20261023-root-4b1314bcb5c2' AND owner_fence=2 AND stop_generation=0 AND NOT stopped AND enabled AND hold_reason IS NULL AND admitted_until>clock_timestamp()) THEN RAISE EXCEPTION 'OWNER_STOP_OR_ADMISSION_CHANGED';END IF;
IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton AND enabled AND shared_scheduler_verified AND budget_evidence->>'provider_admissions_allowed'='true' AND budget_evidence->'allowed_providers' ? 'Massive') THEN RAISE EXCEPTION 'CURRENT_MASSIVE_BUDGET_ADMISSION_REQUIRED';END IF;
IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE family_id='N03') OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.source_observation WHERE family_id='N03') THEN RAISE EXCEPTION 'N03_ALREADY_STARTED_RECONCILE_PRESERVED_PROGRESS';END IF;
SELECT details INTO test_evidence FROM population_top3000_202608_202609_v1.pipeline_event WHERE event='MASSIVE_ACTION_N03_ROLLBACK_VERIFIED' AND details->>'result'='PASS' AND details->>'provider_accuracy_verified'='false' AND details->>'live_provider_requested'='false' ORDER BY at DESC LIMIT 1;
current_functions:=(SELECT jsonb_object_agg(fn,encode(sha256(convert_to(pg_get_functiondef(('population_top3000_202608_202609_v1.'||fn)::regprocedure),'UTF8')),'hex')) FROM (VALUES ('massive_archive_key_v1(text,text,jsonb)'),('massive_actions_context_v1(uuid)'),('massive_actions_acquire_v1(uuid,text)'),('massive_actions_claim_v1(uuid,text)'),('massive_actions_commit_v1(uuid,text,jsonb)'),('massive_actions_failure_v1(uuid,text,jsonb)'),('massive_actions_date_v1(text)'),('massive_actions_normalize_v1(uuid,text)'),('massive_actions_derive_v1(uuid,text)'),('pipeline_validate_v1(uuid,text)')) h(fn));
IF test_evidence IS NULL OR test_evidence->'tested_function_sha256' IS DISTINCT FROM current_functions THEN RAISE EXCEPTION 'REAL_ROLLBACK_EVIDENCE_FOR_CURRENT_FUNCTIONS_REQUIRED';END IF;
SELECT details INTO deploy_evidence FROM population_top3000_202608_202609_v1.pipeline_event WHERE event='MASSIVE_ACTION_N03_WORKER_DEPLOYED_VERIFIED' AND details->>'result'='PASS' AND details->>'slug'='population-top3000-actions-v1' AND details->>'worker_sha256'='f4cc10331a4217f36887521ce3d65d479f32e38315764380562c5d261a9cc757' AND details->>'invalid_token_http_status'='403' AND details->>'provider_requests_during_auth_probe'='0' ORDER BY at DESC LIMIT 1;
IF deploy_evidence IS NULL THEN RAISE EXCEPTION 'DEPLOYED_WORKER_HASH_AND_NO_FETCH_AUTH_PROBE_REQUIRED';END IF;
IF position('population-top3000-actions-v1' in pg_get_functiondef('population_top3000_202608_202609_v1.pipeline_dispatch_edge_v1(uuid,text,text)'::regprocedure))=0 THEN RAISE EXCEPTION 'ROOT_REVIEWED_EDGE_SLUG_ALLOWLIST_REQUIRED';END IF;
INSERT INTO population_top3000_202608_202609_v1.pipeline_family_binding(family_id,expected_relation,observed_relation,expected_definition_sha256,observed_definition_sha256,binding_version,requires_company_universe,expected_partition_count,partition_manifest_evidence,partition_manifest_frozen)
VALUES('N03','population_top3000_202608_202609_v1.expected_observation'::regclass,'population_top3000_202608_202609_v1.source_observation'::regclass,
population_top3000_202608_202609_v1.pipeline_relation_hash_v1('population_top3000_202608_202609_v1.expected_observation'::regclass),population_top3000_202608_202609_v1.pipeline_relation_hash_v1('population_top3000_202608_202609_v1.source_observation'::regclass),
'n03-provisional-source-archive-20261023',true,1,'{"partitions":["splits-execution-202608-202609"],"independent_action_denominator":false,"company_membership_verified":false}',false)
ON CONFLICT(family_id) DO NOTHING;
IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_family_binding WHERE family_id='N03' AND expected_relation='population_top3000_202608_202609_v1.expected_observation'::regclass AND observed_relation='population_top3000_202608_202609_v1.source_observation'::regclass AND expected_definition_sha256=population_top3000_202608_202609_v1.pipeline_relation_hash_v1('population_top3000_202608_202609_v1.expected_observation'::regclass) AND observed_definition_sha256=population_top3000_202608_202609_v1.pipeline_relation_hash_v1('population_top3000_202608_202609_v1.source_observation'::regclass) AND binding_version='n03-provisional-source-archive-20261023' AND requires_company_universe AND expected_partition_count=1 AND NOT partition_manifest_frozen) THEN RAISE EXCEPTION 'EXISTING_N03_BINDING_CONFLICT';END IF;
FOR ad IN SELECT * FROM (VALUES ('ACQUIRE','massive_actions_acquire_v1(uuid,text)','PROVIDER'),
('NORMALIZE','massive_actions_normalize_v1(uuid,text)','DATABASE'),
('DERIVE','massive_actions_derive_v1(uuid,text)','DATABASE'),
('VALIDATE','pipeline_validate_v1(uuid,text)','DATABASE')) x(stage,fn,resource) LOOP
 h:=encode(sha256(convert_to(pg_get_functiondef(('population_top3000_202608_202609_v1.'||ad.fn)::regprocedure),'UTF8')),'hex');
 INSERT INTO population_top3000_202608_202609_v1.pipeline_adapter(family_id,stage,function_oid,function_sha256,resource_class,provider,enabled,reviewed_test_evidence,producer_revision,expected_batch_seconds)
 VALUES('N03',ad.stage,('population_top3000_202608_202609_v1.'||ad.fn)::regprocedure::oid,h,ad.resource,CASE WHEN ad.resource='PROVIDER' THEN 'Massive' ELSE NULL END,true,
 jsonb_build_object('sql_test',test_evidence,'worker_deployment',deploy_evidence,'provider_accuracy_verified',false,'admission','ONE_SOURCE_PAGE_PENDING_LIVE_REVIEW'),'massive-actions-20261023-v1',45)
 ON CONFLICT(family_id,stage) DO NOTHING;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_adapter WHERE family_id='N03' AND stage=ad.stage AND function_oid=('population_top3000_202608_202609_v1.'||ad.fn)::regprocedure::oid AND function_sha256=h AND resource_class=ad.resource AND enabled AND provider IS NOT DISTINCT FROM (CASE WHEN ad.resource='PROVIDER' THEN 'Massive' ELSE NULL END)) THEN RAISE EXCEPTION 'EXISTING_N03_ADAPTER_CONFLICT stage=%',ad.stage;END IF;
END LOOP;
tid:=population_top3000_202608_202609_v1.pipeline_enqueue_v1('N03','ACQUIRE','splits-execution-202608-202609',
'{"max_pages":1,"first_page_review_required":true,"provider_accuracy_verified":false}',
'{"scope_sha256":"8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd","archive_contract_revision":"massive-actions-20261023-v1"}');
INSERT INTO population_top3000_202608_202609_v1.pipeline_event(task_id,event,details) VALUES(tid,'MASSIVE_N03_FIRST_PAGE_ADMITTED',jsonb_build_object('owner_fence',2,'provider','Massive','max_pages',1,'max_response_bytes',8388608,'worker_sha256','f4cc10331a4217f36887521ce3d65d479f32e38315764380562c5d261a9cc757','cohort_certified',false,'independent_event_denominator_verified',false,'quality_requirement','LIVE_PAGE_REVIEW_BEFORE_CAP_INCREASE','no_new_subscription',true));
PERFORM population_top3000_202608_202609_v1.pipeline_wake_v1('top3000-recovery-20261023-root-4b1314bcb5c2',2,'TESTED_MASSIVE_N03_ONE_PAGE_ADMITTED');
END $admit$;
COMMIT;
