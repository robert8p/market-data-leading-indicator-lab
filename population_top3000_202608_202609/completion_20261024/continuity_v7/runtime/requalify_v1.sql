-- Root supplies one temp row recovery_qualification_metadata_v6(review_event_id bigint,manifest_sha256 text) from distinct review of exact current manifest.
-- No work, ownership, stop, cost, or cumulative counter is reset.
BEGIN;
SET LOCAL statement_timeout='30s';
SET LOCAL lock_timeout='15s';

CREATE TEMP TABLE recovery_qualification_metadata_v6(review_event_id bigint,manifest_sha256 text);
DO $r$ DECLARE e bigint; BEGIN
 PERFORM population_top3000_202608_202609_v1.continuation_guard_v6('top3000-completion-20261024-root-5db48b32d103',3,1);
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES('RECOVERY_MANIFEST_INDEPENDENT_REVIEW_V1',$j${"assignment_event_id": 23928, "claim_event_id": 24969, "reviewer": "/root/sequencing_preparation/runtime_reviewer", "producer": "/root/sequencing_preparation", "result": "APPROVED_EXACT_POST_INTEGRATION_RECOVERY_MANIFEST", "owner_id": "top3000-completion-20261024-root-5db48b32d103", "owner_fence": 3, "stop_generation": 1, "scope_sha256": "8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd", "authority_event_id": 9528, "manifest": {"functions": "95446fc84bcbdca9c66d978ae9812e3911708b4a67abf738c909a6ce95d894ec", "adapters": "9e10617b7a9e086acd976e7f07758a1f8400c341b5138b988c379402b42937e8"}, "manifest_sha256": "e51f6929df2221dc1911c5d1647c0ceedf3da916008fa56c4482b41ebcee330a", "canonical_manifest_hash_independently_recomputed_in_postgresql": true, "actual_rollback_receipt_sha256": "502f54890db4098244020d10722add37dd37b01fff00bc8f3b9cb6d8db67c205", "reviewed_code_and_tests": {"f05_retained_candidate_v1.sql": "b3fd3eb15db7ee9ce30e434f03c6372da001689aada2918c066c8168a8dd62cb", "dependency_wake_v1.sql": "d7138444166bb65447d6374c32b3151f77dc104f26e794e78a0ca639fa4d18fe", "continuation_dispatch_v6.sql": "bc33f98b24972fb33d115cc79763ea1f126da823035d4f59646ea100ba6e908d", "successor_assignments_v1.json": "ccaa8d356981589cda6d4d85389b44f55b2aa25991699cb832e3bd3654a2f812", "register_successors_v1.sql": "47bc6c68485000a565d88d99e04c64149eacf9c3198a99f8a974a05cdd193503", "continuation_claim_v6.sql": "521409d30036e9ec11dff3a804e1ebca0bbe23928dd449b6f5a9f31df8bcde33", "integration_header_v1.sql": "dea3e23d381398158eaf28cf6a5abdf40d338e178e607d60b102c1fc4626b094", "rollback_body_v1.sql": "278eca514546358d8246ca976a6eed77c278d1bb306fb2225545d0652abfc249", "register_runtime_v1.sql": "6cf774f3b452926a1cca10f14aeb97b5a541eeb127ecceedbb8277498563710d", "fairness_rollback_snippet_v1.sql": "810462fabbdb151b3d5e07fba875f41377aab4c1c75d27e5e22dc0fa6f19cf45", "build_runtime_bundle.py": "39210e101b72ed507b5f5d953723ea9cb082d92bf479681162ab9c0893114305"}, "independent_cleanup": {"checked_at": "2026-10-24T12:42:35.399004Z", "candidate_table_absent": true, "watch_table_absent": true, "F05_tasks": 0, "synthetic_tasks": 0, "synthetic_queues": 0}, "conditions": ["Requalify only if actual resulting manifest equals exact reviewed manifest", "Preserve recovery counters and original deadline", "Fresh current guarded authority and exact claim at integration", "Require production12 candidate commit and held validation successor before useful execution claim"], "limitations": ["No live CTX06 qualification", "No independent economic source accuracy or family acceptance", "Producer executed rollback; reviewer independently inspected receipt, unchanged productioncode hashes, fixture-only change and database cleanup", "Integration not performed by reviewer"], "approved": true, "code_manifest": {"functions": "95446fc84bcbdca9c66d978ae9812e3911708b4a67abf738c909a6ce95d894ec", "adapters": "9e10617b7a9e086acd976e7f07758a1f8400c341b5138b988c379402b42937e8"}, "reviewed_manifest_sha256": "e51f6929df2221dc1911c5d1647c0ceedf3da916008fa56c4482b41ebcee330a", "code_commit": "5a2a97c1146b1225f7385e424832a6f1840f9f3c", "integration_event_id": 25103}$j$::jsonb) RETURNING event_id INTO e;
 INSERT INTO recovery_qualification_metadata_v6 VALUES(e,'e51f6929df2221dc1911c5d1647c0ceedf3da916008fa56c4482b41ebcee330a');END;$r$;

DO $qualify$
DECLARE a population_top3000_202608_202609_v1.acquisition_control%rowtype;
 p population_top3000_202608_202609_v1.pipeline_control%rowtype;
 manifest jsonb;prior_id bigint;eng_id bigint;existing_restarts integer;new_id bigint;
 expected_manifest_sha text;review_event_id bigint;
BEGIN
 SELECT m.manifest_sha256,m.review_event_id INTO STRICT expected_manifest_sha,review_event_id FROM recovery_qualification_metadata_v6 m;
 PERFORM pg_advisory_xact_lock(2026083000261022);
 SELECT * INTO STRICT a FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton FOR UPDATE;
 SELECT * INTO STRICT p FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton FOR UPDATE;
 IF a.owner_id IS DISTINCT FROM 'top3000-completion-20261024-root-5db48b32d103' OR a.owner_fence IS DISTINCT FROM 3 OR a.stop_generation IS DISTINCT FROM 1 OR a.stopped IS DISTINCT FROM false
  OR p.expected_owner_id IS DISTINCT FROM a.owner_id OR p.expected_owner_fence IS DISTINCT FROM a.owner_fence OR p.expected_stop_generation IS DISTINCT FROM a.stop_generation
  OR a.enabled IS DISTINCT FROM true OR p.enabled IS DISTINCT FROM true OR a.hold_reason IS NOT NULL
  OR a.admitted_until IS NULL OR a.admitted_until<=clock_timestamp() OR p.deadline IS NULL OR p.deadline<=clock_timestamp()
  OR p.tick_count>=p.max_ticks OR p.shared_scheduler_verified IS DISTINCT FROM true
  OR a.scope_sha256 IS DISTINCT FROM '8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd' OR p.scope_sha256 IS DISTINCT FROM a.scope_sha256
  OR population_top3000_202608_202609_v1.pipeline_cost_admission_v2(p,0) IS DISTINCT FROM true
  OR p.budget_evidence#>>'{monetary_policy,authority_event_id}' IS DISTINCT FROM '9528'
 THEN RAISE EXCEPTION 'RECOVERY_QUALIFICATION_CURRENT_AUTHORITY_REQUIRED';END IF;
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE status='LEASED')
  OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_attempt WHERE outcome='RUNNING')
  OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_provider_lease WHERE released_at IS NULL)
  OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.connector_probe_lease_v1 WHERE released_at IS NULL)
 THEN RAISE EXCEPTION 'RECOVERY_QUALIFICATION_ACTIVE_OPERATION';END IF;
 IF NOT EXISTS(SELECT 1 FROM cron.job WHERE jobid=596 AND database=current_database() AND btrim(command)='select population_top3000_202608_202609_v1.pipeline_tick_v1();')
  OR NOT EXISTS(SELECT 1 FROM cron.job WHERE jobid=597 AND jobname='top3000-qualified-recovery-v1' AND database=current_database()
   AND command=$command$BEGIN; SET LOCAL statement_timeout='8s'; SET LOCAL lock_timeout='2s'; SELECT population_top3000_202608_202609_v1.population_recovery_request_v1(); COMMIT;$command$)
 THEN RAISE EXCEPTION 'RECOVERY_QUALIFICATION_SCHEDULER_BINDING_CHANGED';END IF;
 manifest:=population_top3000_202608_202609_v1.population_recovery_manifest_v1();
 IF expected_manifest_sha!~'^[0-9a-f]{64}$' OR encode(sha256(convert_to(manifest::text,'UTF8')),'hex') IS DISTINCT FROM expected_manifest_sha
 THEN RAISE EXCEPTION 'RECOVERY_QUALIFICATION_REVIEWED_MANIFEST_REQUIRED';END IF;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_event e WHERE e.event_id=review_event_id AND e.details->>'approved'='true' AND e.details->>'reviewed_manifest_sha256'=expected_manifest_sha AND e.details->'code_manifest'=manifest AND e.details->>'owner_id'=a.owner_id AND e.details->>'owner_fence'=a.owner_fence::text AND e.details->>'stop_generation'=a.stop_generation::text AND e.details->>'scope_sha256'=a.scope_sha256 AND nullif(e.details->>'reviewer','') IS NOT NULL AND e.details->>'reviewer' IS DISTINCT FROM e.details->>'producer') THEN RAISE EXCEPTION 'RECOVERY_QUALIFICATION_DISTINCT_CURRENT_REVIEW_REQUIRED';END IF;
 IF position('connector_probe_lease_v1' in pg_get_functiondef('population_top3000_202608_202609_v1.population_recovery_request_v1(text,text)'::regprocedure))=0
  OR position('q.details->>''authority_event_id''' in pg_get_functiondef('population_top3000_202608_202609_v1.population_recovery_request_v1(text,text)'::regprocedure))=0
 THEN RAISE EXCEPTION 'RECOVERY_QUALIFICATION_SHARED_LIMITER_REVISION_REQUIRED';END IF;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_event WHERE event='CONNECTOR_LIMITER_ACTUAL_TEST_V1'
  AND details->>'owner_id'=a.owner_id AND details->>'owner_fence'=a.owner_fence::text AND details->>'stop_generation'=a.stop_generation::text
  AND details->>'code_file_sha256'='3f2957df0c94fa01478ab3b8119812a2958a414bdb335fd2e233291650673d8f'
  AND details#>>'{runtime_receipt,result}'='PASS_SHARED_CONNECTOR_LIMITER_ROLLBACK'
  AND details#>>'{runtime_receipt,actual_provider_calls}'='0' AND details#>>'{runtime_receipt,test_mutations_rolled_back}'='true')
 THEN RAISE EXCEPTION 'RECOVERY_QUALIFICATION_CURRENT_LIMITER_TEST_REQUIRED';END IF;
 SELECT event_id INTO prior_id FROM population_top3000_202608_202609_v1.pipeline_event WHERE event='RECOVERY_DELEGATION_QUALIFIED_V1' ORDER BY event_id DESC LIMIT 1;
 SELECT max(event_id) INTO eng_id FROM population_top3000_202608_202609_v1.pipeline_event WHERE event='ROOT_SCOPED_IMPLEMENTATION_ACTIVE';
 SELECT count(*) INTO existing_restarts FROM population_top3000_202608_202609_v1.pipeline_event e
 JOIN population_top3000_202608_202609_v1.pipeline_event q ON q.event_id::text=e.details->>'policy_event_id'
 WHERE e.event='RECOVERY_WAKE_COMPLETED_V1' AND q.details->>'authority_event_id'='9528';
 IF existing_restarts>=3 THEN RAISE EXCEPTION 'RECOVERY_CUMULATIVE_RESTART_ALLOWANCE_EXHAUSTED';END IF;
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES('RECOVERY_DELEGATION_QUALIFIED_V1',jsonb_build_object(
  'actor','automation:6adc2c96db348191bca7cf4eacfdac32','authority_event_id',9528,'owner_id',a.owner_id,'owner_fence',a.owner_fence,'stop_generation',a.stop_generation,
  'scope_sha256',a.scope_sha256,'qualified_at',clock_timestamp(),'expires_at',least(a.admitted_until,p.deadline),
  'max_restarts',3,'restarts_already_used_under_authority',existing_restarts,'restart_allowance_is_cumulative_per_authority',true,'cooldown_minutes',10,
  'pipeline_job_id',596,'engineering_event_id',coalesce(eng_id,0),'superseded_policy_event_id',prior_id,'code_manifest',manifest,'reviewed_manifest_sha256',expected_manifest_sha,'independent_review_event_id',review_event_id,
  'qualification_reason','Current reviewed authority/runtime/family adapter manifest; shared connector operations count as active; repeated manifest qualification preserves authority-wide restart count.',
  'source_or_provider_calls_authorised',false,'runtime_owner_preserved_not_impersonated',true,'quality_stop_budget_and_runtime_limits_preserved',true,
  'retained_prior_pure_decision_test_count',25,'prior_test_policy_event_id',9219,'reviewed_shared_limiter_rollback_required',true)) RETURNING event_id INTO new_id;
 PERFORM cron.alter_job(597,schedule=>'* * * * *',active=>true);
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES('RECOVERY_SCHEDULER_REQUALIFIED_20261024',jsonb_build_object('policy_event_id',new_id,'authority_event_id',9528,'job_id',597,'owner_id',a.owner_id,'owner_fence',a.owner_fence,'stop_generation',a.stop_generation,'restarts_already_used',existing_restarts,'counter_or_deadline_changes',false));
END; $qualify$;
SELECT population_top3000_202608_202609_v1.population_recovery_request_v1('qualification-readback-'||(SELECT max(event_id)::text FROM population_top3000_202608_202609_v1.pipeline_event WHERE event='RECOVERY_DELEGATION_QUALIFIED_V1'),'Post-integration current manifest qualification proof') AS result;
COMMIT;
