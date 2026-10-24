-- ROOT ONLY, reviewed current-hash installation and actual rollback must precede.
BEGIN; SET LOCAL lock_timeout='15s';SET LOCAL statement_timeout='45s';
DO $register$
DECLARE a population_top3000_202608_202609_v1.acquisition_control%rowtype;p population_top3000_202608_202609_v1.pipeline_control%rowtype;q record;fn regprocedure;h text;bundle text;review jsonb;actual jsonb;iv jsonb;k text;tid uuid;assignment jsonb;ae bigint;reqs jsonb:='[]';
BEGIN
 PERFORM pg_advisory_xact_lock(2026083000261022);
 SELECT * INTO STRICT a FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton FOR UPDATE;
 SELECT * INTO STRICT p FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton FOR UPDATE;
 IF a.owner_id IS DISTINCT FROM 'top3000-completion-20261024-root-5db48b32d103' OR a.owner_fence IS DISTINCT FROM 3 OR a.stop_generation IS DISTINCT FROM 1 OR a.stopped IS DISTINCT FROM false OR a.enabled IS DISTINCT FROM true OR p.enabled IS DISTINCT FROM true OR a.hold_reason IS NOT NULL
 OR a.admitted_until IS NULL OR a.admitted_until<=clock_timestamp() OR p.deadline IS NULL OR p.deadline<=clock_timestamp()
 OR p.expected_owner_id IS DISTINCT FROM a.owner_id OR p.expected_owner_fence IS DISTINCT FROM a.owner_fence OR p.expected_stop_generation IS DISTINCT FROM a.stop_generation
 OR a.scope_sha256 IS DISTINCT FROM '8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd' OR p.scope_sha256 IS DISTINCT FROM a.scope_sha256
 OR p.budget_evidence#>>'{monetary_policy,authority_event_id}' IS DISTINCT FROM '9528' THEN RAISE EXCEPTION 'CTX07_V6_AUTHORITY';END IF;
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE family_id='CTX07' AND status='LEASED') THEN RAISE EXCEPTION 'CTX07_V6_LIVE_ATTEMPT';END IF;
 SELECT event_id,details INTO ae,assignment FROM population_top3000_202608_202609_v1.pipeline_event WHERE event='ENGINEERING_IMPLEMENTATION_ASSIGNMENT_V5' AND details->>'assignment_id'='ctx07-original-source-recovery-numerical-vintage-v6' ORDER BY event_id DESC LIMIT 1;
 IF assignment IS NULL OR assignment->>'owner_id' IS DISTINCT FROM a.owner_id OR assignment->>'owner_fence' IS DISTINCT FROM a.owner_fence::text OR assignment->>'stop_generation' IS DISTINCT FROM a.stop_generation::text OR (assignment->>'not_after')::timestamptz<=clock_timestamp() OR assignment#>>'{provider_call_admission,new_provider_requests}' IS DISTINCT FROM '64' THEN RAISE EXCEPTION 'CTX07_V6_EXPLICIT_SOURCE_ASSIGNMENT';END IF;
 bundle:=population_top3000_202608_202609_v1.ctx07_recovery_bundle_v6();
 SELECT details INTO review FROM population_top3000_202608_202609_v1.pipeline_event WHERE event='CTX07_V6_INDEPENDENT_REVIEW' AND details->>'bundle_sha256'=bundle ORDER BY event_id DESC LIMIT 1;
 SELECT details INTO actual FROM population_top3000_202608_202609_v1.pipeline_event WHERE event='CTX07_V6_ACTUAL_ROLLBACK_PASS' AND details->>'bundle_sha256'=bundle ORDER BY event_id DESC LIMIT 1;
 IF review->>'approved' IS DISTINCT FROM 'true' OR review->>'producer' IS NOT DISTINCT FROM review->>'reviewer' OR actual->>'result' IS DISTINCT FROM 'PASS_CTX07_V6_SYNTHETIC_ROLLBACK' OR actual->>'production_changes_rolled_back' IS DISTINCT FROM 'true' THEN RAISE EXCEPTION 'CTX07_V6_REVIEW_AND_RUNTIME_PROOF';END IF;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_event WHERE event='CTX07_V6_WORKER_REVIEWED_DEPLOYED' AND details->>'bundle_sha256'=bundle AND details->>'worker_sha256'=review->>'worker_sha256') THEN RAISE EXCEPTION 'CTX07_V6_WORKER_DEPLOYMENT_PROOF';END IF;
 FOR q IN SELECT * FROM (VALUES
 ('ACQUIRE','7213ebc0ca0d7c6e5e35340fcf54407fd2f60fb314e5157d09ac8b9ddb56f5e4'),
 ('NORMALIZE','3a911e5d4a079af732d177cdc3cd9f9f1bb4d6a497b2b85829194a97bb6a813b'),
 ('DERIVE','143ee0b5661a6c82cf0bba80c2e5038cf6e166682c8204ac06809d4e0233bf5a'),
 ('VALIDATE','4a827cee3d9ff611a4845a6179b54e17e6745fb266ad29d1162e904a997fc968'))x(stage,sha) LOOP
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_adapter WHERE family_id='CTX07' AND stage=q.stage AND function_sha256=q.sha AND function_sha256=encode(sha256(convert_to(pg_get_functiondef(function_oid),'UTF8')),'hex')) THEN RAISE EXCEPTION 'CTX07_V6_PREIMAGE_CHANGED';END IF;
 fn:=('population_top3000_202608_202609_v1.ctx07_recovery_'||lower(q.stage)||'_v6(uuid,text)')::regprocedure;h:=encode(sha256(convert_to(pg_get_functiondef(fn),'UTF8')),'hex');
 UPDATE population_top3000_202608_202609_v1.pipeline_adapter SET function_oid=fn,function_sha256=h,producer_revision='ctx07-recovery-v6',reviewed_test_evidence=coalesce(reviewed_test_evidence,'{}'::jsonb)||jsonb_build_object('ctx07_v6',review,'runtime_test',actual) WHERE family_id='CTX07' AND stage=q.stage;
 END LOOP;
 -- Adds only official-source provider names under existing uncapped authority; no counter or deadline changes.
 UPDATE population_top3000_202608_202609_v1.pipeline_control SET budget_evidence=jsonb_set(budget_evidence,'{allowed_providers}',(SELECT jsonb_agg(DISTINCT x) FROM jsonb_array_elements(budget_evidence->'allowed_providers'||'["BLS","BEA","Census"]'::jsonb)x)) WHERE singleton;
 iv:=jsonb_build_object('scope_sha256',a.scope_sha256,'bundle_sha256',bundle,'assignment_event_id',ae,
 'observations_sha256',(SELECT encode(sha256(convert_to(string_agg(to_jsonb(o)::text,E'\n' ORDER BY observation_id),'UTF8')),'hex') FROM population_top3000_202608_202609_v1.macro_observations o),
 'dictionary_sha256',(SELECT encode(sha256(convert_to(string_agg(payload_sha256,E'\n' ORDER BY item_key),'UTF8')),'hex') FROM population_top3000_202608_202609_v1.catalog_dictionary WHERE family_id='CTX07' AND scope_id='corrected_company_scope_8e6ca1178f03822c'));
 IF (SELECT count(*) FROM population_top3000_202608_202609_v1.macro_observations)<>79 THEN RAISE EXCEPTION 'CTX07_V6_POPULATION_CHANGED';END IF;
 FOR q IN SELECT source_id,source_url,raw_sha256,CASE WHEN source_url LIKE 'https://www.bls.gov/%' THEN 'BLS' WHEN source_url LIKE 'https://www.bea.gov/%' THEN 'BEA' ELSE 'Census' END provider FROM population_top3000_202608_202609_v1.macro_source_registry WHERE source_url ~ '^https://(www[.]bls[.]gov|www[.]bea[.]gov|www2?[.]census[.]gov)/' ORDER BY CASE WHEN source_id='empsit_202607' THEN 0 ELSE 1 END,source_id LOOP
 k:='ctx07-recovery-v6:'||q.source_id||':'||coalesce(q.raw_sha256,'missing');
 INSERT INTO population_top3000_202608_202609_v1.ctx07_recovery_request_v6 VALUES(k,q.source_id,q.source_url,q.raw_sha256,q.provider,iv,least(clock_timestamp()+interval '30minutes',a.admitted_until,p.deadline,(assignment->>'not_after')::timestamptz),8388608) ON CONFLICT DO NOTHING;
 tid:=population_top3000_202608_202609_v1.pipeline_enqueue_v1('CTX07','ACQUIRE',k,jsonb_build_object('kind','CTX07_SOURCE_RECOVERY_V6','request_key',k),iv);
 IF q.source_id='empsit_202607' THEN
 UPDATE population_top3000_202608_202609_v1.pipeline_task SET status='BLOCKED',blocker_code='CTX07_V6_RETAINED_BODY_IMPORT_PENDING',resumption_condition=jsonb_build_object('required_action','IMPORT_EXACT_ALREADY_FETCHED_BODY','sha256',q.raw_sha256,'new_fetch_forbidden',true) WHERE task_id=tid AND status='READY' AND executions=0;
 END IF;
 reqs:=reqs||jsonb_build_array(jsonb_build_object('request_key',k,'task_id',tid));
 END LOOP;
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES('CTX07_V6_REGISTERED',jsonb_build_object('assignment_event_id',ae,'bundle_sha256',bundle,'tasks',reqs,'source_scope','existing28source registry; government sources runnable; private publisher rights unresolved','old79retained_units_preserved',true,'strict_original_receipt_qualified',false,'family_accepted',false,'request_cap',64,'byte_cap',67108864,'owner_id',a.owner_id,'owner_fence',3,'stop_generation',1));
 PERFORM population_top3000_202608_202609_v1.pipeline_wake_v1(a.owner_id,a.owner_fence,'CTX07 exact original-source recovery and automatic numerical verification registered');
END;$register$;
COMMIT;
