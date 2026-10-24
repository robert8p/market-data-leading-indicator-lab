-- Runner: BEGIN; install candidate definitions in same transaction, substitute exact reviewed manifest below; execute; ROLLBACK.
-- Do not execute without enclosing transaction and final ROLLBACK. No HTTP.
SET LOCAL lock_timeout='15s'; SET LOCAL statement_timeout='60s';
CREATE TEMP TABLE f06_bridge_runtime_result(result jsonb) ON COMMIT DROP;
DO $test$
DECLARE m jsonb:=$manifest$__MANIFEST_JSON__$manifest$::jsonb;
 own text:='top3000-completion-20261024-root-5db48b32d103';h text;r jsonb;r2 jsonb;p jsonb;rev bigint;
 before_count jsonb;after_count jsonb;guards jsonb:='{}';tid uuid;did uuid;vid uuid;aid uuid;tok text:=repeat('b',64);fh text;stage_name text;
 t population_top3000_202608_202609_v1.pipeline_task%rowtype;n integer;successors jsonb;
BEGIN
 PERFORM population_top3000_202608_202609_v1.continuation_guard_v6(own,3,1);
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE family_id='F06' AND status='LEASED') THEN RAISE EXCEPTION 'F06_BRIDGE_OVERLAPPING_LEASE';END IF;
 SELECT jsonb_build_array(a.provider_requests,a.http_invocations,a.stored_bytes,a.reserved_bytes,p.actual_usd,p.accrued_usd,p.reserved_request_usd,p.unavoidable_usd,a.stop_generation,a.owner_fence,p.deadline,a.admitted_until) INTO before_count FROM population_top3000_202608_202609_v1.acquisition_control a CROSS JOIN population_top3000_202608_202609_v1.pipeline_control p;
 h:=encode(sha256(convert_to(m::text,'UTF8')),'hex');
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES('ENGINEERING_F06_BRIDGE_INDEPENDENT_REVIEW_V6',jsonb_build_object('manifest_sha256',h,'assignment_event_id',19696,'approved',true,'reviewer','f06_review','producer','f06_evidence','review_artifact_sha256','f48bd69dc806b5e91f5c021dfda12680669998a61284ef2ad196d1aba1c41bc0','transactional_test_only',true)) RETURNING event_id INTO rev;
 p:=jsonb_build_object('manifest',m,'claim_event_id',22646,'review_event_id',rev);
 BEGIN PERFORM population_top3000_202608_202609_v1.f06_ten_case_bridge_admit_v6(own,2,1,p);RAISE EXCEPTION 'BAD_FENCE_ACCEPTED';EXCEPTION WHEN raise_exception THEN IF SQLERRM='CONTINUATION_OWNER_STOP_SCOPE_CHANGED' THEN guards:=guards||'{"owner_change":true}'::jsonb;ELSE RAISE;END IF;END;
 BEGIN UPDATE population_top3000_202608_202609_v1.acquisition_control SET stopped=true WHERE singleton;PERFORM population_top3000_202608_202609_v1.f06_ten_case_bridge_admit_v6(own,3,1,p);RAISE EXCEPTION 'STOP_ACCEPTED';EXCEPTION WHEN raise_exception THEN IF SQLERRM='CONTINUATION_USER_OR_CONTROL_HOLD' THEN guards:=guards||'{"user_stop":true}'::jsonb;ELSE RAISE;END IF;END;
 BEGIN PERFORM population_top3000_202608_202609_v1.f06_ten_case_bridge_manifest_v6(jsonb_set(m,'{cases,0,pin,expected_parent_sha256}',to_jsonb(repeat('0',64))));RAISE EXCEPTION 'BAD_PARENT_ACCEPTED';EXCEPTION WHEN raise_exception THEN IF SQLERRM='F06_BRIDGE_CASE_CONTRACT' THEN guards:=guards||'{"stale_parent":true}'::jsonb;ELSE RAISE;END IF;END;
 BEGIN UPDATE population_top3000_202608_202609_v1.ref01_adjudication_head_v3 SET record_sha256=repeat('0',64) WHERE run_key='ref01-company-adjudication-v3-20261023' AND case_key=m#>>'{cases,0,case_key}';PERFORM population_top3000_202608_202609_v1.f06_ten_case_bridge_manifest_v6(m);RAISE EXCEPTION 'CHANGED_LIVE_PARENT_ACCEPTED';EXCEPTION WHEN raise_exception THEN IF SQLERRM='F06_BRIDGE_PARENT_CHANGED' THEN guards:=guards||'{"live_parent_changed":true}'::jsonb;ELSE RAISE;END IF;WHEN foreign_key_violation OR check_violation THEN guards:=guards||'{"live_parent_changed":true,"parent_constraint_rejected_before_helper":true}'::jsonb;END;
 BEGIN PERFORM population_top3000_202608_202609_v1.f06_ten_case_bridge_manifest_v6(jsonb_set(m,'{cases,0,retained_inspection}',jsonb_build_object('too_large',repeat('X',131073))));RAISE EXCEPTION 'BYTE_CAP_ACCEPTED';EXCEPTION WHEN raise_exception THEN IF SQLERRM='F06_BRIDGE_CASE_CONTRACT' THEN guards:=guards||'{"byte_cap":true}'::jsonb;ELSE RAISE;END IF;END;
 BEGIN PERFORM population_top3000_202608_202609_v1.f06_ten_case_bridge_manifest_v6(jsonb_set(m,'{rank_accepted}','true'));RAISE EXCEPTION 'FALSE_ACCEPTANCE_ALLOWED';EXCEPTION WHEN raise_exception THEN IF SQLERRM='F06_BRIDGE_MANIFEST_CONTRACT' THEN guards:=guards||'{"false_acceptance":true}'::jsonb;ELSE RAISE;END IF;END;
 BEGIN PERFORM population_top3000_202608_202609_v1.f06_ten_case_bridge_admit_v6(own,3,1,jsonb_set(p,'{manifest,cases,0,retained_inspection}',jsonb_build_object('tampered',true)));RAISE EXCEPTION 'UNREVIEWED_MANIFEST_ALLOWED';EXCEPTION WHEN raise_exception THEN IF SQLERRM='F06_BRIDGE_EXACT_INDEPENDENT_REVIEW_REQUIRED' THEN guards:=guards||'{"review_hash":true}'::jsonb;ELSE RAISE;END IF;END;
 r:=population_top3000_202608_202609_v1.f06_ten_case_bridge_admit_v6(own,3,1,p);
 IF r->>'new_components' IS DISTINCT FROM '10' OR r->>'pipeline_commit' IS DISTINCT FROM 'false' THEN RAISE EXCEPTION 'EXACT_DIRECT_COMPONENTS_REQUIRED';END IF;
 r2:=population_top3000_202608_202609_v1.f06_ten_case_bridge_admit_v6(own,3,1,p);
 IF r2->>'new_components' IS DISTINCT FROM '0' OR r2->>'output_sha256' IS DISTINCT FROM r->>'output_sha256' OR r2->>'component_event_id' IS DISTINCT FROM r->>'component_event_id' THEN RAISE EXCEPTION 'COMPONENT_REPLAY_CHANGED';END IF;
 BEGIN UPDATE population_top3000_202608_202609_v1.f06_ten_case_bridge_component_v6 SET record=record||'{"changed":true}'::jsonb,record_sha256=encode(sha256(convert_to((record||'{"changed":true}'::jsonb)::text,'UTF8')),'hex') WHERE manifest_sha256=h;PERFORM population_top3000_202608_202609_v1.f06_ten_case_bridge_source_successors_v6(own,3,1,h);RAISE EXCEPTION 'TAMPER_ACCEPTED';EXCEPTION WHEN raise_exception THEN IF SQLERRM='F06_BRIDGE_VALIDATION_MISMATCH' THEN guards:=guards||'{"rehash_tamper":true}'::jsonb;ELSE RAISE;END IF;END;
 successors:=population_top3000_202608_202609_v1.f06_ten_case_bridge_source_successors_v6(own,3,1,h);
 IF jsonb_array_length(successors)<>10 OR population_top3000_202608_202609_v1.f06_ten_case_bridge_source_successors_v6(own,3,1,h) IS DISTINCT FROM successors THEN RAISE EXCEPTION 'EXACT_SUCCESSOR_REPLAY_REQUIRED';END IF;
 SELECT count(*) INTO n FROM population_top3000_202608_202609_v1.f06_ten_case_bridge_component_v6 WHERE manifest_sha256=h;
 IF n<>10 THEN RAISE EXCEPTION 'EXACT_TEN_DIRECT_COMPONENTS_REQUIRED';END IF;
 SELECT jsonb_build_array(a.provider_requests,a.http_invocations,a.stored_bytes,a.reserved_bytes,p.actual_usd,p.accrued_usd,p.reserved_request_usd,p.unavoidable_usd,a.stop_generation,a.owner_fence,p.deadline,a.admitted_until) INTO after_count FROM population_top3000_202608_202609_v1.acquisition_control a CROSS JOIN population_top3000_202608_202609_v1.pipeline_control p;
 IF before_count IS DISTINCT FROM after_count THEN RAISE EXCEPTION 'COUNTERS_OR_CLOCKS_CHANGED';END IF;
 INSERT INTO f06_bridge_runtime_result VALUES(jsonb_build_object('result','PASS_F06_TEN_CASE_BRIDGE_ROLLBACK','guards',guards,'new_components',10,'replay_new_components',0,'pipeline_commit',false,'source_successor_assignment_ids',successors,'output_sha256',r->>'output_sha256','counter_clock_preservation',true,'family_accepted',false,'actual_http',0));
END;$test$;
SELECT result FROM f06_bridge_runtime_result;
-- Caller MUST ROLLBACK.
