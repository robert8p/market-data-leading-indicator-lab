-- Root supplies one temp row recovery_qualification_metadata_v6(review_event_id bigint,manifest_sha256 text) from distinct review of exact current manifest.
-- No work, ownership, stop, cost, or cumulative counter is reset.
BEGIN;
SET LOCAL statement_timeout='30s';
SET LOCAL lock_timeout='15s';

CREATE TEMP TABLE recovery_qualification_metadata_v6(review_event_id bigint,manifest_sha256 text);
DO $r$ DECLARE e bigint; BEGIN
 PERFORM population_top3000_202608_202609_v1.continuation_guard_v6('top3000-completion-20261024-root-5db48b32d103',3,1);
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES('RECOVERY_MANIFEST_INDEPENDENT_REVIEW_V1',$review${"approved": true, "producer": "root/continuation_implementation", "reviewer": "root/continuation_design_review", "owner_id": "top3000-completion-20261024-root-5db48b32d103", "owner_fence": 3, "stop_generation": 1, "scope_sha256": "8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd", "authority_event_id": 9528, "assignment_event_id": 27678, "claim_event_id": 27679, "integration_event_id": 28761, "code_review_event_id": 28759, "runtime_proof_event_id": 28760, "code_commit": "59227a7824298fb6ba6e09d7f1a87ddbac0bde8e", "checked_at": "2026-10-24T15:09:53.10475+00:00", "code_manifest": {"adapters": "caccf5f74fc2e567f594b5b6ab7d17fb98083082cce5d630121ece4d53adc40a", "functions": "bc32123e568252cf2289a4bb0687190715f6112705a656ce5e31a2fbcc364798"}, "reviewed_manifest_sha256": "e4783af679fadb0bf70c66bdd9c213a3d1579d0d6395942ec074a82482243fc6", "installed_function_sha256": {"continuation_dispatch_v6": "79bf0f1f700d1256025f3e5df3a485085a3acc01ee63b66e8c59a9ad1d677f2c", "ctx02_clear_page_admission_v6": "7b68f7313f94151d9c8a5590c2b80a5fd74a6c784e5c9a5f4b6c19ceee38252e"}, "handler_review_event_id": 28759, "canonical_policy_sha256": "b38e09dc71a4dcaef996a8b062dee353eae67a44d8f5e13b2362b0857ba09f51", "exact_request_policy_count": 3, "current_controls_enabled_unstopped": true, "finite_runtime_limits_preserved": {"source_bytes": 1000000000, "provider_requests": 10000, "http_invocations": 10055, "dispatch_calls": 21600, "ticks": 26000, "deadline": "2026-10-25T22:36:17.687194+00:00", "admitted_until": "2026-10-24T17:16:21.505883+00:00"}, "review_scope": "Independently queried installed helper and dispatcher hashes and complete recovery manifest after integration; exact code-approved postimages and reviewed policy registration match. Reuse unchanged component evidence. This qualifies the current code manifest only; real scheduled source and normalization useful output proof remains separate.", "automatic_next_tranche_useful_output_verified": false, "family_accepted": false, "full_window_complete": false, "production_writes_by_reviewer": 0}$review$::jsonb) RETURNING event_id INTO e;
 INSERT INTO recovery_qualification_metadata_v6 VALUES(e,'e4783af679fadb0bf70c66bdd9c213a3d1579d0d6395942ec074a82482243fc6');END;$r$;

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
