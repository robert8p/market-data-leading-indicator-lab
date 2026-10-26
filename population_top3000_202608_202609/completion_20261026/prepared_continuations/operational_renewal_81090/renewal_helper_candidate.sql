-- UNINTEGRATED CANDIDATE. No policy event is created or granted by this file.
-- Requires fresh exact implementation assignment, actual PostgreSQL rollback, distinct
-- code/runtime review, exact-preimage installation and resulting recovery requalification.
CREATE FUNCTION population_top3000_202608_202609_v1.continuation_time_renewal_81090_v7(p_owner text,p_fence bigint,p_stop bigint)
RETURNS jsonb LANGUAGE plpgsql SET search_path='pg_catalog' AS $function$
DECLARE
 a population_top3000_202608_202609_v1.acquisition_control%rowtype;
 p population_top3000_202608_202609_v1.pipeline_control%rowtype;
 ctl jsonb;auth jsonb;policy jsonb;proposal jsonb;review jsonb;manifest jsonb;
 policy_id bigint;review_id bigint;legacy_count bigint;generation_count bigint;
 prior jsonb;prior_event_id bigint;now_at timestamptz;new_until timestamptz;used bigint;
 before_a jsonb;before_p jsonb;after_a jsonb;after_p jsonb;renewal_id bigint;
 generation constant text:='POST_DEADLINE_AUTHORITY_81090_RENEWAL_V7';
BEGIN
 -- Legacy's fourteen existing events and its original sixteen-event bound remain untouched.
 SELECT count(*) INTO legacy_count FROM population_top3000_202608_202609_v1.pipeline_event
 WHERE event='CONTINUATION_TIME_ADMISSION_REVIEWED_V6';
 IF legacy_count<16 THEN RETURN jsonb_build_object('state','LEGACY_ALLOWANCE_REMAINS','legacy_count',legacy_count);END IF;
 IF legacy_count<>16 THEN RAISE EXCEPTION 'TIME81090_UNEXPECTED_LEGACY_HISTORY';END IF;
 SELECT * INTO STRICT a FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton;
 SELECT * INTO STRICT p FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton;
 now_at:=clock_timestamp();
 IF a.admitted_until>=now_at+interval '30minutes' OR p.deadline<=a.admitted_until
 THEN RETURN jsonb_build_object('state','NOT_DUE');END IF;
 -- Ordinary busy work defers a renewal; it never pauses workers or clears leases.
 IF NOT pg_try_advisory_xact_lock(2026083000261022) THEN RETURN '{"state":"DEFERRED_QUIET_LOCK"}'::jsonb;END IF;
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE status='LEASED')
 OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_attempt WHERE outcome='RUNNING')
 OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_provider_lease WHERE released_at IS NULL)
 OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.connector_probe_lease_v1 WHERE released_at IS NULL)
 THEN RETURN '{"state":"DEFERRED_ACTIVE_WORK"}'::jsonb;END IF;
 ctl:=population_top3000_202608_202609_v1.continuation_guard_v6(p_owner,p_fence,p_stop);
 SELECT * INTO STRICT a FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton;
 SELECT * INTO STRICT p FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton;
 now_at:=clock_timestamp();
 IF a.admitted_until>=now_at+interval '30minutes' OR p.deadline<=a.admitted_until
 THEN RETURN '{"state":"NOT_DUE_AFTER_LOCK"}'::jsonb;END IF;
 SELECT details INTO STRICT auth FROM population_top3000_202608_202609_v1.pipeline_event
 WHERE event_id=81090 AND event='USER_POST_DEADLINE_CONTINUATION_AUTHORITY_V1';
 IF encode(sha256(convert_to(auth::text,'UTF8')),'hex') IS DISTINCT FROM '9889d852f25984b4c9069875d76ff2f8e77dc0b8702cb7122618de5cdd0c2c5b'
 OR auth->>'owner_id' IS DISTINCT FROM p_owner OR auth->>'owner_fence' IS DISTINCT FROM p_fence::text
 OR auth->>'stop_generation' IS DISTINCT FROM p_stop::text OR auth->>'scope_sha256' IS DISTINCT FROM a.scope_sha256
 OR (auth->>'operational_execution_horizon')::timestamptz IS DISTINCT FROM p.deadline
 THEN RAISE EXCEPTION 'TIME81090_CURRENT_AUTHORITY_REQUIRED';END IF;
 -- Append-only policy is admitted separately after genuine independent code/runtime review.
 SELECT event_id,details INTO policy_id,policy FROM population_top3000_202608_202609_v1.pipeline_event
 WHERE event='CONTINUATION_TIME_GENERATION_POLICY_V7' AND details->>'generation_id'=generation
 ORDER BY event_id DESC LIMIT 1;
 proposal:=policy->'proposal';review_id:=(policy->>'review_event_id')::bigint;
 SELECT details INTO review FROM population_top3000_202608_202609_v1.pipeline_event
 WHERE event_id=review_id AND event='CONTINUATION_TIME_GENERATION_REVIEW_V7';
 SELECT jsonb_object_agg(name,encode(sha256(convert_to(pg_get_functiondef(name::regprocedure),'UTF8')),'hex'))
 INTO manifest FROM (VALUES
 ('population_top3000_202608_202609_v1.continuation_dispatch_impl_pre81090_v7(text,bigint,bigint)'),
 ('population_top3000_202608_202609_v1.continuation_time_renewal_81090_v7(text,bigint,bigint)'),
 ('population_top3000_202608_202609_v1.continuation_dispatch_v6(text,bigint,bigint)')) f(name);
 IF policy IS NULL OR review IS NULL OR policy->>'approved' IS DISTINCT FROM 'true'
 OR proposal->>'generation_id' IS DISTINCT FROM generation OR proposal->>'time_authority_event_id' IS DISTINCT FROM '81090'
 OR proposal->>'authority_sha256' IS DISTINCT FROM '9889d852f25984b4c9069875d76ff2f8e77dc0b8702cb7122618de5cdd0c2c5b'
 OR proposal->>'owner_id' IS DISTINCT FROM p_owner OR proposal->>'owner_fence' IS DISTINCT FROM p_fence::text
 OR proposal->>'stop_generation' IS DISTINCT FROM p_stop::text OR proposal->>'scope_sha256' IS DISTINCT FROM a.scope_sha256
 OR (proposal->>'operational_execution_horizon')::timestamptz IS DISTINCT FROM p.deadline
 OR proposal->>'maximum_new_renewals' IS DISTINCT FROM '20' OR proposal->>'tranche_minutes' IS DISTINCT FROM '180'
 OR proposal->>'trigger_minutes' IS DISTINCT FROM '30' OR proposal->'code_manifest' IS DISTINCT FROM manifest
 OR review->>'approved' IS DISTINCT FROM 'true' OR review->>'actual_rollback_passed' IS DISTINCT FROM 'true'
 OR review->>'proposal_sha256' IS DISTINCT FROM encode(sha256(convert_to(proposal::text,'UTF8')),'hex')
 OR review->'code_manifest' IS DISTINCT FROM manifest OR nullif(review->>'producer','') IS NULL
 OR nullif(review->>'reviewer','') IS NULL OR review->>'producer' IS NOT DISTINCT FROM review->>'reviewer'
 THEN RAISE EXCEPTION 'TIME81090_EXACT_REVIEWED_GENERATION_POLICY_REQUIRED';END IF;
 SELECT count(*) INTO generation_count FROM population_top3000_202608_202609_v1.pipeline_event
 WHERE event='CONTINUATION_TIME_ADMISSION_REVIEWED_V7' AND details->>'generation_id'=generation;
 IF generation_count>=20 THEN RAISE EXCEPTION 'TIME81090_FINITE_GENERATION_EXHAUSTED';END IF;
 IF generation_count=0 THEN
  SELECT event_id,details INTO STRICT prior_event_id,prior FROM population_top3000_202608_202609_v1.pipeline_event
  WHERE event='CONTINUATION_TIME_ADMISSION_REVIEWED_V6' ORDER BY event_id DESC LIMIT 1;
 ELSE
  SELECT event_id,details INTO STRICT prior_event_id,prior FROM population_top3000_202608_202609_v1.pipeline_event
  WHERE event='CONTINUATION_TIME_ADMISSION_REVIEWED_V7' AND details->>'generation_id'=generation ORDER BY event_id DESC LIMIT 1;
  IF prior->>'renewal_ordinal' IS DISTINCT FROM generation_count::text THEN RAISE EXCEPTION 'TIME81090_GENERATION_CHAIN_REQUIRED';END IF;
 END IF;
 IF (prior->>'new_admitted_until')::timestamptz IS DISTINCT FROM a.admitted_until
 OR prior->>'owner_id' IS DISTINCT FROM p_owner OR prior->>'owner_fence' IS DISTINCT FROM p_fence::text
 OR prior->>'stop_generation' IS DISTINCT FROM p_stop::text OR prior->>'scope_sha256' IS DISTINCT FROM a.scope_sha256
 THEN RAISE EXCEPTION 'TIME81090_CURRENT_ADMISSION_CHAIN_REQUIRED';END IF;
 SELECT coalesce(sum(pg_total_relation_size(oid)),0) INTO used FROM pg_class
 WHERE relnamespace='population_top3000_202608_202609_v1'::regnamespace AND relkind IN('r','m');
 IF (p.budget_evidence#>>'{resource_limits,max_relation_bytes}') IS NULL
 OR used>=(p.budget_evidence#>>'{resource_limits,max_relation_bytes}')::bigint
 OR a.stored_bytes+a.reserved_bytes>=a.max_source_bytes OR a.provider_requests>=a.max_provider_requests
 OR a.http_invocations>=a.max_http_invocations OR a.dispatch_calls>=a.max_dispatch_calls OR p.tick_count>=p.max_ticks
 THEN RETURN '{"state":"DEFERRED_CURRENT_FINITE_RESOURCE_LIMIT"}'::jsonb;END IF;
 new_until:=least(p.deadline,now_at+interval '3hours');
 IF new_until<=a.admitted_until THEN RETURN '{"state":"NO_TIME_INCREMENT"}'::jsonb;END IF;
 before_a:=to_jsonb(a);before_p:=to_jsonb(p);
 UPDATE population_top3000_202608_202609_v1.acquisition_control SET admitted_until=new_until
 WHERE singleton AND admitted_until=a.admitted_until;
 IF NOT FOUND THEN RAISE EXCEPTION 'TIME81090_SINGLE_CURRENT_CONTROL_REQUIRED';END IF;
 SELECT to_jsonb(x) INTO after_a FROM population_top3000_202608_202609_v1.acquisition_control x WHERE singleton;
 SELECT to_jsonb(x) INTO after_p FROM population_top3000_202608_202609_v1.pipeline_control x WHERE singleton;
 IF before_a-'admitted_until' IS DISTINCT FROM after_a-'admitted_until' OR before_p IS DISTINCT FROM after_p
 THEN RAISE EXCEPTION 'TIME81090_COUNTER_CLOCK_CAP_OR_HORIZON_CHANGED';END IF;
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details)
 VALUES('CONTINUATION_TIME_ADMISSION_REVIEWED_V7',ctl||jsonb_build_object(
 'generation_id',generation,'time_readmission_authority_event_id',81090,'policy_event_id',policy_id,'independent_review_event_id',review_id,
 'renewal_ordinal',generation_count+1,'maximum_new_renewals',20,'legacy_renewal_count_preserved',legacy_count,
 'prior_admission_event_id',prior_event_id,'old_admitted_until',a.admitted_until,'new_admitted_until',new_until,
 'original_delivery_target',auth->>'original_delivery_target','operational_execution_horizon',p.deadline,
 'mission_relation_bytes',used,'source_bytes',a.stored_bytes,'cumulative_counters_preserved',true,
 'call_storage_tick_caps_unchanged',true,'progress_restart_clocks_preserved',true,'exact_v5_policies_recovery_not_renewed',true,
 'source_http_admission_granted',false,'data_acceptance',false)) RETURNING event_id INTO renewal_id;
 RETURN jsonb_build_object('state','RENEWED_81090_GENERATION_V7','renewal_event_id',renewal_id,'not_after',new_until);
END;$function$;
REVOKE ALL ON FUNCTION population_top3000_202608_202609_v1.continuation_time_renewal_81090_v7(text,bigint,bigint) FROM PUBLIC,anon,authenticated;
REVOKE ALL ON FUNCTION population_top3000_202608_202609_v1.continuation_dispatch_impl_pre81090_v7(text,bigint,bigint) FROM PUBLIC,anon,authenticated;

-- Existing caller's signature, return states and exact old implementation are retained.
CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.continuation_dispatch_v6(p_owner text,p_fence bigint,p_stop bigint)
RETURNS jsonb LANGUAGE plpgsql SET search_path='pg_catalog' AS $function$
DECLARE ans jsonb;
BEGIN
 ans:=population_top3000_202608_202609_v1.continuation_dispatch_impl_pre81090_v7(p_owner,p_fence,p_stop);
 IF ans->>'state'='CLEARING_WORK_DISPATCHED' THEN
  PERFORM population_top3000_202608_202609_v1.continuation_time_renewal_81090_v7(p_owner,p_fence,p_stop);
 END IF;
 RETURN ans;
END;$function$;
