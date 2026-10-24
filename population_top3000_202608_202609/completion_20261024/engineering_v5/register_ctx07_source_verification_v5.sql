-- Run inside current-controller transaction after reviewed DDL. No HTTP. Existing recovery requalification is not delegated.
DO $register$
DECLARE a population_top3000_202608_202609_v1.acquisition_control%rowtype;p population_top3000_202608_202609_v1.pipeline_control%rowtype;
 claim jsonb;assignment jsonb;review jsonb;test jsonb;q record;fn regprocedure;h text;iv jsonb;tid uuid;
BEGIN
 PERFORM pg_advisory_xact_lock(2026083000261022);
 SELECT * INTO STRICT a FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton FOR UPDATE;
 SELECT * INTO STRICT p FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton FOR UPDATE;
 IF a.owner_id IS DISTINCT FROM 'top3000-completion-20261024-root-5db48b32d103' OR a.owner_fence IS DISTINCT FROM 3 OR a.stop_generation IS DISTINCT FROM 1
 OR a.enabled IS DISTINCT FROM true OR a.stopped IS DISTINCT FROM false OR a.hold_reason IS NOT NULL OR p.enabled IS DISTINCT FROM true
 OR p.expected_owner_id IS DISTINCT FROM a.owner_id OR p.expected_owner_fence IS DISTINCT FROM 3 OR p.expected_stop_generation IS DISTINCT FROM 1
 OR a.scope_sha256 IS DISTINCT FROM '8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd' OR p.scope_sha256 IS DISTINCT FROM a.scope_sha256
 OR a.admitted_until IS NULL OR a.admitted_until<=clock_timestamp() OR p.deadline IS NULL OR p.deadline<=clock_timestamp() OR p.tick_count>=p.max_ticks
 OR p.shared_scheduler_verified IS DISTINCT FROM true OR p.budget_evidence#>>'{monetary_policy,authority_event_id}' IS DISTINCT FROM '9528'
 OR population_top3000_202608_202609_v1.pipeline_cost_admission_v2(p,0) IS DISTINCT FROM true THEN RAISE EXCEPTION 'CTX07_REGISTRATION_AUTHORITY';END IF;
 SELECT details INTO STRICT assignment FROM population_top3000_202608_202609_v1.pipeline_event WHERE event_id=14379 AND event='ENGINEERING_IMPLEMENTATION_ASSIGNMENT_V5';
 SELECT details INTO STRICT claim FROM population_top3000_202608_202609_v1.pipeline_event WHERE event_id=14425 AND event='ENGINEERING_ASSIGNMENT_CLAIMED_V5';
 IF encode(sha256(convert_to(assignment::text,'UTF8')),'hex') IS DISTINCT FROM 'a8d5dd57dec51601da17f16296f20e2a9db5ff4d20cfa68e337be44d2c30cb41'
 OR assignment->>'root_will_not_concurrently_modify' IS DISTINCT FROM 'true' OR (assignment->>'not_after')::timestamptz<=clock_timestamp()
 OR claim->>'assignment_event_id' IS DISTINCT FROM '14379' OR claim->>'executor' IS DISTINCT FROM 'automation:6adc2c96db348191bca7cf4eacfdac32'
 OR (claim->>'expires_at')::timestamptz<=clock_timestamp() THEN RAISE EXCEPTION 'CTX07_EXACT_CLAIM_REQUIRED';END IF;
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_event WHERE event_id>14425 AND event IN('ENGINEERING_ASSIGNMENT_COMPLETED_V5','ENGINEERING_ASSIGNMENT_EFFECTS_RECONCILED_RELEASE_V5') AND details->>'claim_event_id'='14425') THEN RAISE EXCEPTION 'CTX07_CLAIM_CLOSED';END IF;
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE family_id='CTX07' AND status='LEASED') THEN RAISE EXCEPTION 'CTX07_ACTIVE_ATTEMPT';END IF;
 SELECT details INTO test FROM population_top3000_202608_202609_v1.pipeline_event WHERE event='CTX07_V5_ROLLBACK_ACCEPTED' AND details->>'claim_event_id'='14425' ORDER BY event_id DESC LIMIT 1;
 SELECT details INTO review FROM population_top3000_202608_202609_v1.pipeline_event WHERE event='ENGINEERING_INDEPENDENT_REVIEW_V5' AND details->>'claim_event_id'='14425' ORDER BY event_id DESC LIMIT 1;
 IF test->>'result' IS DISTINCT FROM 'PASS_CTX07_ACTUAL_ROLLBACK' OR test->>'production_changes_rolled_back' IS DISTINCT FROM 'true'
 OR review->>'approved' IS DISTINCT FROM 'true' OR nullif(review->>'reviewer','') IS NULL OR review->>'reviewer'=review->>'producer'
 OR test->>'implementation_sha256' IS DISTINCT FROM review->>'implementation_sha256'
 OR test->>'helper_sha256' IS DISTINCT FROM encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.ctx07_source_verification_stage_v5(uuid,text)'::regprocedure),'UTF8')),'hex')
 OR test->>'projection_sha256' IS DISTINCT FROM encode(sha256(convert_to(pg_get_viewdef('population_top3000_202608_202609_v1.ctx07_source_verification_projection_v5'::regclass,true),'UTF8')),'hex') THEN RAISE EXCEPTION 'CTX07_INDEPENDENT_REVIEW_AND_RUNTIME_REQUIRED';END IF;
 FOR q IN SELECT * FROM (VALUES
 ('ACQUIRE','bd53e1a3f6f423bd5e2cb85dec7db04393d9f2f1eb624e373a100a650100e982'),
 ('NORMALIZE','c8e64a22850a4d27201a9cb27fd5c2f02987e9a73bf93ee7d99c3475f1ca593d'),
 ('DERIVE','ae9f1de0cf909e642d48aec21463612e48ad998f3499802ff2eb60db0dfc6024'),
 ('VALIDATE','3c139ee0eb3a10f37c79dc7fe6827d5450cbc1403b44e1dee3a5aada652e360d')) x(stage,preimage) LOOP
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_adapter WHERE family_id='CTX07' AND stage=q.stage AND function_sha256=q.preimage AND function_sha256=encode(sha256(convert_to(pg_get_functiondef(function_oid),'UTF8')),'hex')) THEN RAISE EXCEPTION 'CTX07_PREIMAGE_CHANGED';END IF;
 fn:=('population_top3000_202608_202609_v1.ctx07_source_verification_'||lower(q.stage)||'_v5(uuid,text)')::regprocedure;
 h:=encode(sha256(convert_to(pg_get_functiondef(fn),'UTF8')),'hex');
 IF test#>>ARRAY['adapter_hashes',q.stage] IS DISTINCT FROM h THEN RAISE EXCEPTION 'CTX07_WRAPPER_NOT_RUNTIME_PINNED';END IF;
 UPDATE population_top3000_202608_202609_v1.pipeline_adapter SET function_oid=fn,function_sha256=h,
 reviewed_test_evidence=reviewed_test_evidence||jsonb_build_object('ctx07_v5',test),producer_revision='ctx07-source-evidence-v5'
 WHERE family_id='CTX07' AND stage=q.stage;
 END LOOP;
 iv:=jsonb_build_object('scope_sha256',a.scope_sha256,'assignment_event_id',14379,'calendar_sha256','b7efa4d83ac8ee9adcd5cdefe6e3dba6b3cff9c5ea0cca1e55a794ee69e11f1a','helper_sha256',test->>'helper_sha256','projection_sha256',test->>'projection_sha256');
 tid:=population_top3000_202608_202609_v1.pipeline_enqueue_v1('CTX07','ACQUIRE','ctx07-retained-numerical-vintage-evidence-v5','{"kind":"CTX07_SOURCE_VERIFICATION_V5"}',iv);
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES('ENGINEERING_INTEGRATED_V5',jsonb_build_object('assignment_event_id',14379,'claim_event_id',14425,'assignment_id','ctx07-retained-numerical-vintage-evidence-v5','owner_id',a.owner_id,'owner_fence',3,'stop_generation',1,'task_id',tid,'input_versions',iv,'review',review,'runtime_test',test,'holds_released',0,'family_accepted',false));
END;$register$;
