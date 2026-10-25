BEGIN;
SET LOCAL lock_timeout='1s';
SET LOCAL statement_timeout='25s';
DO $capture$
DECLARE before_state jsonb;after_state jsonb;planned jsonb;fixture_id bigint;start_id bigint;proof_event bigint;
BEGIN
 IF NOT pg_try_advisory_xact_lock(2026083000261022) THEN RAISE EXCEPTION 'NATURAL_QUIET_ROLLBACK_CONTROLLER_BUSY';END IF;
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE status='LEASED')
 OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_attempt WHERE outcome='RUNNING')
 OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_provider_lease WHERE released_at IS NULL)
 OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.connector_probe_lease_v1 WHERE released_at IS NULL)
 THEN RAISE EXCEPTION 'NATURAL_QUIET_ROLLBACK_REQUIRED';END IF;
 SELECT jsonb_build_object('a',to_jsonb(a),'p',to_jsonb(p),'manifest',population_top3000_202608_202609_v1.population_recovery_manifest_v1(),
  'latest_policy',(SELECT max(event_id) FROM population_top3000_202608_202609_v1.pipeline_event WHERE event='REF01_DATE_DISJOINT_TURN_POLICY_V10'),
  'latest_recovery',(SELECT max(event_id) FROM population_top3000_202608_202609_v1.pipeline_event WHERE event='RECOVERY_DELEGATION_QUALIFIED_V1'),
  'latest_event',(SELECT max(event_id) FROM population_top3000_202608_202609_v1.pipeline_event)) INTO before_state
 FROM population_top3000_202608_202609_v1.acquisition_control a CROSS JOIN population_top3000_202608_202609_v1.pipeline_control p WHERE a.singleton AND p.singleton;
 start_id:=(before_state->>'latest_event')::bigint;
 BEGIN
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES('ENGINEERING_INDEPENDENT_ADMISSION_REVIEW_V5',jsonb_build_object(
 'approved',true,'fixture_only',true,'approval_scope','EXACT_INNER_ROLLBACK_ONLY_NO_DURABLE_ADMISSION',
 'assignment_event_id',45187,'claim_event_id',46640,'producer','delegated-agent:/root/ctx06_code_review',
 'reviewer','automation:6adc2c96db348191bca7cf4eacfdac32','owner_id','top3000-completion-20261024-root-5db48b32d103','owner_fence',3,'stop_generation',1,
 'authority_event_id',9528,'scope_sha256','8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd',
 'prior_policy_event_id',45078,'readmission_assignment_ids',jsonb_build_array(44820,45186),'same_bounds_only',true,'installed_time_route_only',true,
 'code_manifest',population_top3000_202608_202609_v1.population_recovery_manifest_v1())) RETURNING event_id INTO fixture_id;
 EXECUTE replace($exact_script$
-- Exact append-only policy/time/readmission unit 45187; no code or scheduler changes.
-- Fill ONLY review_event_id after the primary independently approves this exact file.
DO $renew$
DECLARE
 review_event_id bigint:=0;
 review jsonb;ctl jsonb;pol jsonb;prior_recovery jsonb;manifest jsonb;old_a jsonb;new_a jsonb;
 old_policy_id bigint:=45078;prior_recovery_id bigint:=45081;renewal_id bigint;policy_id bigint;recovery_id bigint;
 horizon timestamptz;route_result jsonb;source_result jsonb;target record;body jsonb;new_assignment_id bigint;
 successor_ids jsonb:='[]'::jsonb;held_task_sha text;held_attempt_sha text;held_lease_sha text;restarts integer;
BEGIN
 IF NOT pg_try_advisory_xact_lock(2026083000261022) THEN RAISE EXCEPTION 'NATURAL_QUIET_RENEWAL_CONTROLLER_BUSY';END IF;
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE status='LEASED')
 OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_attempt WHERE outcome='RUNNING')
 OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_provider_lease WHERE released_at IS NULL)
 OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.connector_probe_lease_v1 WHERE released_at IS NULL)
 THEN RAISE EXCEPTION 'NATURAL_QUIET_RENEWAL_REQUIRED';END IF;
 PERFORM 1 FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton FOR UPDATE;
 PERFORM 1 FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton FOR UPDATE;
 ctl:=population_top3000_202608_202609_v1.continuation_guard_v6('top3000-completion-20261024-root-5db48b32d103',3,1);
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_event e WHERE event_id=46640
  AND event='ENGINEERING_ASSIGNMENT_CLAIMED_V5' AND details->>'assignment_event_id'='45187'
  AND details->>'executor'='automation:6adc2c96db348191bca7cf4eacfdac32'
  AND (details->>'expires_at')::timestamptz>clock_timestamp())
 OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_event WHERE details->>'claim_event_id'='46640'
  AND event IN('ENGINEERING_ASSIGNMENT_COMPLETED_V5','ENGINEERING_ASSIGNMENT_EFFECTS_RECONCILED_RELEASE_V5'))
 THEN RAISE EXCEPTION 'EXACT_ACTIVE_45187_CLAIM_REQUIRED';END IF;
 SELECT details INTO STRICT review FROM population_top3000_202608_202609_v1.pipeline_event WHERE event_id=review_event_id
  AND event='ENGINEERING_INDEPENDENT_ADMISSION_REVIEW_V5';
 IF review->>'approved' IS DISTINCT FROM 'true' OR review->>'assignment_event_id' IS DISTINCT FROM '45187'
 OR review->>'claim_event_id' IS DISTINCT FROM '46640'
 OR review->>'producer' IS DISTINCT FROM 'delegated-agent:/root/ctx06_code_review'
 OR review->>'reviewer' IS DISTINCT FROM 'automation:6adc2c96db348191bca7cf4eacfdac32'
 OR review->>'producer' IS NOT DISTINCT FROM review->>'reviewer'
 OR review->>'scope_sha256' IS DISTINCT FROM ctl->>'scope_sha256'
 OR review->>'owner_id' IS DISTINCT FROM ctl->>'owner_id' OR review->>'owner_fence' IS DISTINCT FROM '3'
 OR review->>'stop_generation' IS DISTINCT FROM '1' OR review->>'authority_event_id' IS DISTINCT FROM '9528'
 OR review->>'prior_policy_event_id' IS DISTINCT FROM '45078'
 OR review->'readmission_assignment_ids' IS DISTINCT FROM '[44820,45186]'::jsonb
 OR review->>'same_bounds_only' IS DISTINCT FROM 'true'
 OR review->>'installed_time_route_only' IS DISTINCT FROM 'true'
 THEN RAISE EXCEPTION 'EXACT_DISTINCT_RENEWAL_REVIEW_REQUIRED';END IF;
 manifest:=population_top3000_202608_202609_v1.population_recovery_manifest_v1();
 IF manifest IS DISTINCT FROM '{"adapters":"caccf5f74fc2e567f594b5b6ab7d17fb98083082cce5d630121ece4d53adc40a","functions":"6164796bb2868d14d7e4a90b4e0c6f918bd10bb2086bee4a7d9d0ae63914277a"}'::jsonb
 OR review->'code_manifest' IS DISTINCT FROM manifest
 OR (SELECT encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.continuation_dispatch_v6(text,bigint,bigint)'::regprocedure),'UTF8')),'hex')) IS DISTINCT FROM '79bf0f1f700d1256025f3e5df3a485085a3acc01ee63b66e8c59a9ad1d677f2c'
 THEN RAISE EXCEPTION 'RENEWAL_EXACT_CURRENT_CODE_REQUIRED';END IF;
 SELECT event_id,details INTO STRICT old_policy_id,pol FROM population_top3000_202608_202609_v1.pipeline_event
 WHERE event='REF01_DATE_DISJOINT_TURN_POLICY_V10' AND details->>'plan_id'='ref01-date-fullperiod-v9' AND details->>'approved'='true' ORDER BY event_id DESC LIMIT 1;
 IF old_policy_id<>45078 OR pol->'allowed_dates' IS DISTINCT FROM '["2026-09-23","2026-09-24","2026-09-25","2026-09-26","2026-09-27","2026-09-28","2026-09-29","2026-09-30"]'::jsonb
 OR pol->>'max_provider_calls' IS DISTINCT FROM '1200' OR pol->>'max_http_invocations' IS DISTINCT FROM '1200'
 OR pol->>'max_source_bytes' IS DISTINCT FROM '300000000' OR pol->>'max_relation_bytes' IS DISTINCT FROM '3000000000'
 OR pol->>'fixture_only' IS DISTINCT FROM 'false' THEN RAISE EXCEPTION 'PINNED_SAME_BOUNDS_POLICY_REQUIRED';END IF;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.ref01_date_plan_v9 p
  WHERE p.plan_id='ref01-date-fullperiod-v9' AND p.enabled
  AND p.content_sha256='f6596e461a84315e6cadfc11ec8bf1ef52cd0d5b3011d4df0353d5976370c319'
  AND p.content_sha256=encode(sha256(convert_to(p.content::text,'UTF8')),'hex')
  AND p.content_sha256=pol->>'plan_sha256' AND p.code_bundle_sha256=population_top3000_202608_202609_v1.ref01_date_bundle_v4()
  AND p.content->>'owner_id'=ctl->>'owner_id' AND p.content->>'owner_fence'='3' AND p.content->>'stop_generation'='1'
  AND p.content->>'max_provider_calls'=pol->>'max_provider_calls' AND p.content->>'max_http_invocations'=pol->>'max_http_invocations'
  AND p.content->>'max_source_bytes'=pol->>'max_source_bytes' AND p.content->>'max_relation_bytes'=pol->>'max_relation_bytes')
 OR pol->>'function_sha256' IS DISTINCT FROM 'f0bb40c0a3b6e9886087bef901995618ec179c8638f8099437caf75e2ade236d'
 OR pol->>'wrapper_sha256' IS DISTINCT FROM '569c03a6a0e17779830df3ac41b978ad1c62a7238f5a508616369c0bb017d69e'
 OR pol->>'old_impl_sha256' IS DISTINCT FROM 'ca43dbdd5a984456e6f71104c291989a27a48ab85f03a2952c1fccea88902791'
 THEN RAISE EXCEPTION 'PINNED_LIVE_SOURCE_PLAN_AND_CODE_REQUIRED';END IF;
 SELECT encode(sha256(convert_to(to_jsonb(t)::text,'UTF8')),'hex') INTO held_task_sha FROM population_top3000_202608_202609_v1.pipeline_task t WHERE task_id='15a87469-6335-4bf6-b883-6c86315418e7';
 SELECT encode(sha256(convert_to(to_jsonb(a)::text,'UTF8')),'hex') INTO held_attempt_sha FROM population_top3000_202608_202609_v1.pipeline_attempt a WHERE attempt_id='45f6e6a5-3886-4b33-b9dc-00851c1a6b04';
 SELECT encode(sha256(convert_to(to_jsonb(l)::text,'UTF8')),'hex') INTO held_lease_sha FROM population_top3000_202608_202609_v1.pipeline_provider_lease l WHERE attempt_id='45f6e6a5-3886-4b33-b9dc-00851c1a6b04';
 IF held_task_sha IS DISTINCT FROM pol->>'deferred_task_sha256' OR held_attempt_sha IS DISTINCT FROM pol->>'deferred_attempt_sha256'
 OR held_lease_sha IS DISTINCT FROM pol->>'deferred_lease_sha256' THEN RAISE EXCEPTION 'ORIGINAL_UNCERTAIN_EVIDENCE_CHANGED';END IF;
 SELECT to_jsonb(a) INTO STRICT old_a FROM population_top3000_202608_202609_v1.acquisition_control a WHERE singleton;
 IF (old_a->>'admitted_until')::timestamptz<='2026-10-25T00:47:31.833862Z'::timestamptz THEN
  IF (old_a->>'admitted_until')::timestamptz>=clock_timestamp()+interval '30minutes' THEN RAISE EXCEPTION 'NATURAL_RENEWAL_NOT_YET_ELIGIBLE';END IF;
  IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE status='LEASED')
  OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_attempt WHERE outcome='RUNNING')
  OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_provider_lease WHERE released_at IS NULL)
  OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.connector_probe_lease_v1 WHERE released_at IS NULL)
  THEN RAISE EXCEPTION 'NATURAL_QUIET_RENEWAL_REQUIRED';END IF;
  route_result:=population_top3000_202608_202609_v1.continuation_dispatch_v6('top3000-completion-20261024-root-5db48b32d103',3,1);
 END IF;
 SELECT admitted_until,to_jsonb(a) INTO horizon,new_a FROM population_top3000_202608_202609_v1.acquisition_control a WHERE singleton;
 SELECT event_id INTO renewal_id FROM population_top3000_202608_202609_v1.pipeline_event
 WHERE event='CONTINUATION_TIME_ADMISSION_REVIEWED_V6' AND details->>'owner_id'=ctl->>'owner_id'
 AND details->>'owner_fence'='3' AND details->>'stop_generation'='1'
 AND (details->>'new_admitted_until')::timestamptz>=horizon AND (details->>'new_admitted_until')::timestamptz-horizon<interval '1second'
 ORDER BY event_id DESC LIMIT 1;
 IF horizon<='2026-10-25T00:47:31.833862Z'::timestamptz OR renewal_id IS NULL
 OR horizon>(SELECT deadline FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton)
 OR horizon>clock_timestamp()+interval '3hours' THEN RAISE EXCEPTION 'FRESH_FINITE_INSTALLED_TIME_ADMISSION_REQUIRED';END IF;
 IF (new_a-'admitted_until'-'updated_at'-'next_action'-'next_stage') IS DISTINCT FROM (old_a-'admitted_until'-'updated_at'-'next_action'-'next_stage')
 THEN RAISE EXCEPTION 'RENEWAL_CHANGED_ACQUISITION_COUNTERS_OR_LIMITS';END IF;
 -- Same source permissions and immutable held task pins; horizon is the actual renewed operational horizon.
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details)
 VALUES('REF01_DATE_DISJOINT_TURN_POLICY_V10',pol||jsonb_build_object('producer','delegated-agent:/root/ctx06_code_review',
 'reviewer','automation:6adc2c96db348191bca7cf4eacfdac32','not_after',horizon,'prior_not_after',pol->'not_after',
 'superseded_policy_event_id',45078,'assignment_event_id',45187,'claim_event_id',46640,
 'independent_review_event_id',review_event_id,'operational_admission_review_event_id',renewal_id,
 'renewal_only',true,'source_limits_unchanged',true,'uncertain_task_retry_or_release_authorized',false)) RETURNING event_id INTO policy_id;
 -- Readmit unclaimed implementation assignments by append-only clone; private manifests never leave the database.
 FOR target IN SELECT event_id,details,encode(sha256(convert_to(details::text,'UTF8')),'hex') sha FROM population_top3000_202608_202609_v1.pipeline_event WHERE event_id IN(44820,45186) ORDER BY event_id LOOP
  IF (target.event_id=44820 AND target.sha IS DISTINCT FROM '6c58d0fe3ec279669c643030426a4d5e38c5d82ba2aafaafbedc28a5191c7798')
  OR (target.event_id=45186 AND target.sha IS DISTINCT FROM '3e3607444a735317f024a84b14a0327d5c605670b67e7ed4c122a69127da129e')
  OR target.details->>'maximum_source_bytes' IS DISTINCT FROM '0' OR target.details->>'maximum_http_invocations' IS DISTINCT FROM '0'
  OR target.details->>'new_provider_requests' IS DISTINCT FROM '0'
  THEN RAISE EXCEPTION 'EXACT_ZERO_SOURCE_READMISSION_PREIMAGE_REQUIRED';END IF;
  IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_event c WHERE c.event='ENGINEERING_ASSIGNMENT_CLAIMED_V5'
   AND (c.details->>'assignment_event_id'=target.event_id::text OR c.details->>'exclusive_component'=target.details->>'exclusive_component')
   AND NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_event x WHERE x.event IN('ENGINEERING_ASSIGNMENT_COMPLETED_V5','ENGINEERING_ASSIGNMENT_EFFECTS_RECONCILED_RELEASE_V5')
    AND x.details->>'claim_event_id'=c.event_id::text AND x.details->>'effects_known'='true'))
  OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_event WHERE event='ENGINEERING_ASSIGNMENT_COMPLETED_V5' AND details->>'assignment_event_id'=target.event_id::text)
  THEN RAISE EXCEPTION 'READMISSION_TARGET_HAS_ACTIVE_OR_COMPLETED_EFFECTS %',target.event_id;END IF;
  body:=target.details||jsonb_build_object('assignment_id',target.details->>'assignment_id'||':time-successor-20261025-'||renewal_id,
   'not_after',horizon,'prior_not_after',target.details->'not_after','readmission_of_event_id',target.event_id,
   'parent_assignment_event_id',target.event_id,'readmission_review_event_id',review_event_id,
   'operational_admission_review_event_id',renewal_id,'readmission_executor','automation:6adc2c96db348191bca7cf4eacfdac32',
   'assignment_admission_explicit',true,'same_bounds_and_input_manifest_preserved',true,'prior_expiry_and_bound_history_preserved',true);
  INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES('ENGINEERING_IMPLEMENTATION_ASSIGNMENT_V5',body) RETURNING event_id INTO new_assignment_id;
  successor_ids:=successor_ids||jsonb_build_array(new_assignment_id);
 END LOOP;
 SELECT event_id,details INTO STRICT prior_recovery_id,prior_recovery FROM population_top3000_202608_202609_v1.pipeline_event WHERE event='RECOVERY_DELEGATION_QUALIFIED_V1' ORDER BY event_id DESC LIMIT 1;
 IF prior_recovery_id<>45081 OR prior_recovery->'code_manifest' IS DISTINCT FROM manifest
 OR prior_recovery->>'max_restarts' IS DISTINCT FROM '3' OR prior_recovery->>'cooldown_minutes' IS DISTINCT FROM '10'
 THEN RAISE EXCEPTION 'RECOVERY_CURRENT_EXACT_PRIOR_POLICY_REQUIRED';END IF;
 SELECT count(*) INTO restarts FROM population_top3000_202608_202609_v1.pipeline_event e
 LEFT JOIN population_top3000_202608_202609_v1.pipeline_event q ON q.event_id::text=e.details->>'policy_event_id'
 WHERE e.event='RECOVERY_WAKE_COMPLETED_V1' AND (e.details->>'policy_event_id'=prior_recovery_id::text OR q.details->>'authority_event_id'='9528');
 IF restarts<>0 THEN RAISE EXCEPTION 'RECOVERY_RESTART_HISTORY_ADVANCED_REVIEW_REQUIRED';END IF;
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details)
 VALUES('RECOVERY_DELEGATION_QUALIFIED_V1',prior_recovery||jsonb_build_object('actor','automation:6adc2c96db348191bca7cf4eacfdac32',
 'not_after',horizon,'expires_at',horizon,'requalified_at',clock_timestamp(),'superseded_policy_event_id',prior_recovery_id,
 'independent_review_event_id',review_event_id,'operational_admission_review_event_id',renewal_id,
 'qualification_reason','Exact independently reviewed same-manifest finite time renewal; cumulative restart/progress clocks, original deadline and uncertain source hold preserved',
 'policy_time_readmission_event_id',policy_id,'restarts_already_used_under_authority',restarts,
 'source_or_provider_calls_authorised',false)) RETURNING event_id INTO recovery_id;
 WITH dates AS(SELECT x::date as_of FROM jsonb_array_elements_text(pol->'allowed_dates')x),
 status AS(SELECT d.as_of,q.task_id,q.status,q.checkpoint->>'next_page' next_page,
  q.status='COMPLETE' AND EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.ref01_date_page_v4 pg
   JOIN population_top3000_202608_202609_v1.source_document ds USING(source_id)
   WHERE pg.acquire_task_id=q.task_id AND pg.partition_key=pr.request_key AND pg.next_url IS NULL
   AND ds.source_sha256=pg.source_sha256 AND ds.source_sha256=encode(sha256(convert_to(ds.content_text,'UTF8')),'hex')
   AND ds.content_bytes=octet_length(ds.content_text) AND ds.content_text::jsonb->>'next_url' IS NULL
   AND jsonb_array_length(ds.content_text::jsonb->'results')=pg.result_count) terminal_source_verified
  FROM dates d LEFT JOIN population_top3000_202608_202609_v1.ref01_date_plan_request_v9 pr ON pr.plan_id='ref01-date-fullperiod-v9' AND pr.as_of=d.as_of
  LEFT JOIN population_top3000_202608_202609_v1.pipeline_task q ON q.family_id='REF01' AND q.stage='ACQUIRE' AND q.payload->>'request_key'=pr.request_key)
 SELECT jsonb_build_object('state',CASE WHEN count(*)=8 AND bool_and(coalesce(terminal_source_verified,false)) THEN 'ELIGIBLE_DATES_TERMINAL_WITH_DEFERRED_HOLD' ELSE 'EXISTING_FINITE_DISJOINT_SOURCE_WORK_READ_ONLY_STATUS' END,
  'dates',jsonb_agg(to_jsonb(status) ORDER BY as_of),'deferred_date','2026-09-22','deferred_task_id','15a87469-6335-4bf6-b883-6c86315418e7','family_accepted',false,'full_instrument_history_accepted',false)
 INTO source_result FROM status;
 IF population_top3000_202608_202609_v1.population_recovery_manifest_v1() IS DISTINCT FROM manifest
 OR (SELECT encode(sha256(convert_to(to_jsonb(t)::text,'UTF8')),'hex') FROM population_top3000_202608_202609_v1.pipeline_task t WHERE task_id='15a87469-6335-4bf6-b883-6c86315418e7') IS DISTINCT FROM held_task_sha
 OR (SELECT encode(sha256(convert_to(to_jsonb(a)::text,'UTF8')),'hex') FROM population_top3000_202608_202609_v1.pipeline_attempt a WHERE attempt_id='45f6e6a5-3886-4b33-b9dc-00851c1a6b04') IS DISTINCT FROM held_attempt_sha
 OR (SELECT encode(sha256(convert_to(to_jsonb(l)::text,'UTF8')),'hex') FROM population_top3000_202608_202609_v1.pipeline_provider_lease l WHERE attempt_id='45f6e6a5-3886-4b33-b9dc-00851c1a6b04') IS DISTINCT FROM held_lease_sha
 THEN RAISE EXCEPTION 'RENEWAL_CHANGED_CODE_OR_UNCERTAIN_EVIDENCE';END IF;
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES('ENGINEERING_POLICY_TIME_READMISSION_EFFECT_V5',ctl||jsonb_build_object(
 'actor','automation:6adc2c96db348191bca7cf4eacfdac32','assignment_event_id',45187,'claim_event_id',46640,
 'independent_review_event_id',review_event_id,'operational_admission_review_event_id',renewal_id,
 'prior_policy_event_id',45078,'policy_successor_event_id',policy_id,'recovery_successor_event_id',recovery_id,
 'engineering_successor_event_ids',successor_ids,'new_not_after',horizon,'installed_route_result',route_result,'source_plan_readback',source_result,
 'source_calls',0,'code_changed',false,'held_evidence_preserved',true,'finite_limits_and_all_counter_history_preserved',true,
 'full_period_instrument_evidence_verified',false,'family_accepted',false));
END $renew$;
$exact_script$,'review_event_id bigint:=0;','review_event_id bigint:='||fixture_id::text||';');
 SELECT details INTO STRICT planned FROM population_top3000_202608_202609_v1.pipeline_event WHERE event='ENGINEERING_POLICY_TIME_READMISSION_EFFECT_V5'
  AND event_id>start_id AND details->>'claim_event_id'='46640' ORDER BY event_id DESC LIMIT 1;
 IF planned->'held_evidence_preserved' IS DISTINCT FROM 'true'::jsonb OR planned->'source_calls' IS DISTINCT FROM '0'::jsonb
 OR jsonb_array_length(planned->'engineering_successor_event_ids')<>2 THEN RAISE EXCEPTION 'EXACT_RENEWAL_ROLLBACK_EFFECT_PROOF_MISSING';END IF;
 RAISE EXCEPTION USING ERRCODE='Z1011',MESSAGE='EXPECTED_V10_TIME_READMISSION_ROLLBACK';
 EXCEPTION WHEN SQLSTATE 'Z1011' THEN NULL;END;
 SELECT jsonb_build_object('a',to_jsonb(a),'p',to_jsonb(p),'manifest',population_top3000_202608_202609_v1.population_recovery_manifest_v1(),
  'latest_policy',(SELECT max(event_id) FROM population_top3000_202608_202609_v1.pipeline_event WHERE event='REF01_DATE_DISJOINT_TURN_POLICY_V10'),
  'latest_recovery',(SELECT max(event_id) FROM population_top3000_202608_202609_v1.pipeline_event WHERE event='RECOVERY_DELEGATION_QUALIFIED_V1'),
  'latest_event',(SELECT max(event_id) FROM population_top3000_202608_202609_v1.pipeline_event)) INTO after_state
 FROM population_top3000_202608_202609_v1.acquisition_control a CROSS JOIN population_top3000_202608_202609_v1.pipeline_control p WHERE a.singleton AND p.singleton;
 IF before_state IS DISTINCT FROM after_state THEN RAISE EXCEPTION 'ROLLBACK_CHANGED_LIVE_CONTROLS_COUNTERS_CODE_OR_POLICY';END IF;
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES('ENGINEERING_RUNTIME_ROLLBACK_PROOF_V5',jsonb_build_object(
 'assignment_event_id',45187,'claim_event_id',46640,'producer','delegated-agent:/root/ctx06_code_review','fixture_only',true,
 'result','PASS_EXACT_INSTALLED_TIME_ROUTE_SAME_BOUNDS_POLICY_AND_ASSIGNMENT_READMISSION_ROLLBACK','rolled_back',true,
 'before_code_manifest',before_state->'manifest','after_code_manifest',after_state->'manifest','controls_counters_code_and_prior_records_restored',true,
 'source_calls',0,'new_http_or_provider_permissions',0,'actual_admission_renewed_by_test',false,
 'exact_sql_sha256','aec643bba0b30dd244632064e096d1d6130dcdbd826094e836abdd21639484d8','planned_uncommitted_effect',planned,
 'limitations',jsonb_build_array('Inner fixture review and all policy/readmission/recovery/effect rows were rolled back','Rolled-back event IDs are test observations, never durable authority','Actual finite admission and append-only effects require distinct final approval and execution'))) RETURNING event_id INTO proof_event;
END $capture$;
COMMIT;
