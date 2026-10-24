-- Run after installing schema/worker/admit/continue/wrapper/bundle/seed in a transaction; caller ROLLBACKs.
DO $f$
DECLARE pl population_top3000_202608_202609_v1.ref01_date_plan_v9%rowtype; before_ctl jsonb;after_ctl jsonb;result jsonb;rej boolean:=false;x record;tested int:=0;
BEGIN
 SELECT * INTO STRICT pl FROM population_top3000_202608_202609_v1.ref01_date_plan_v9 WHERE enabled;
 IF pl.code_bundle_sha256<>population_top3000_202608_202609_v1.ref01_date_bundle_v4() OR pl.content_sha256<>encode(sha256(convert_to(pl.content::text,'UTF8')),'hex') THEN RAISE EXCEPTION 'TEST_PLAN_BUNDLE';END IF;
 IF jsonb_array_length(pl.content->'dates')<>60 OR EXISTS(SELECT 1 FROM generate_series(DATE '2026-08-01',DATE '2026-09-30',INTERVAL '1day') d WHERE d::date<>DATE '2026-08-03' AND NOT(pl.content->'dates' ? d::date::text))
 OR pl.content->'dates' ? '2026-08-03' THEN RAISE EXCEPTION 'TEST_FULL_DATES';END IF;
 IF (SELECT count(*) FROM population_top3000_202608_202609_v1.ref01_date_plan_request_v9 WHERE plan_id=pl.plan_id)<>4 THEN RAISE EXCEPTION 'TEST_EXACT_LEGACY_MAPPING';END IF;
 FOR x IN SELECT t.task_id FROM population_top3000_202608_202609_v1.pipeline_task t WHERE t.family_id='REF01' AND t.stage='NORMALIZE' AND t.payload->>'kind'='REF01_CENSUS_SOURCE_V1' AND t.payload ? 'source_page_number' LOOP
  result:=population_top3000_202608_202609_v1.ref01_date_context_v4(x.task_id);
  IF result->>'state'<>'READY' THEN RAISE EXCEPTION 'TEST_RETAINED_CONTEXT';END IF;tested:=tested+1;
 END LOOP;
 IF tested<40 THEN RAISE EXCEPTION 'TEST_TOO_FEW_RETAINED_CONTEXTS';END IF;
 SELECT jsonb_build_object('provider',provider_requests,'http',http_invocations,'stored',stored_bytes,'reserved',reserved_bytes) INTO before_ctl FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton;
 result:=population_top3000_202608_202609_v1.ref01_date_continue_v9('top3000-completion-20261024-root-5db48b32d103',3,1);
 SELECT jsonb_build_object('provider',provider_requests,'http',http_invocations,'stored',stored_bytes,'reserved',reserved_bytes) INTO after_ctl FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton;
 IF before_ctl IS DISTINCT FROM after_ctl THEN RAISE EXCEPTION 'TEST_MAINTENANCE_COUNTER_CHANGE';END IF;
 BEGIN
  PERFORM population_top3000_202608_202609_v1.ref01_date_continue_v9('wrong-owner',3,1);
 EXCEPTION WHEN OTHERS THEN IF SQLERRM<>'CONTINUATION_OWNER_STOP_SCOPE_CHANGED' THEN RAISE;END IF;rej:=true;END;
 IF NOT rej THEN RAISE EXCEPTION 'TEST_OWNER_NOT_REJECTED';END IF;rej:=false;
 BEGIN
  UPDATE population_top3000_202608_202609_v1.acquisition_control SET stopped=true WHERE singleton;
  PERFORM population_top3000_202608_202609_v1.ref01_date_continue_v9('top3000-completion-20261024-root-5db48b32d103',3,1);
 EXCEPTION WHEN OTHERS THEN IF SQLERRM<>'CONTINUATION_USER_OR_CONTROL_HOLD' THEN RAISE;END IF;rej:=true;END;
 IF NOT rej OR (SELECT stopped FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton) THEN RAISE EXCEPTION 'TEST_STOP_NOT_REJECTED_OR_FIXTURE_NOT_ROLLED_BACK';END IF;
 RAISE NOTICE 'PASS basic plan date scope, % exact retained contexts, no HTTP/counter effects, owner and stop rejection',tested;
END;$f$;
