DO $r$
DECLARE r record;a population_top3000_202608_202609_v1.acquisition_control%rowtype;used bigint;
BEGIN
 PERFORM population_top3000_202608_202609_v1.continuation_guard_v6('top3000-completion-20261024-root-5db48b32d103',3,1);
 SELECT * INTO STRICT a FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton;
 SELECT count(*) INTO used FROM population_top3000_202608_202609_v1.pipeline_attempt x JOIN population_top3000_202608_202609_v1.pipeline_task t USING(task_id) WHERE t.payload->>'kind'='REF01_CENSUS_SOURCE_V1' AND x.request_id IS NOT NULL;
 IF used>56 OR a.max_provider_requests-a.provider_requests<56-used OR a.max_http_invocations-a.http_invocations<56-used OR a.max_source_bytes-a.stored_bytes-a.reserved_bytes<16000000 THEN RAISE EXCEPTION 'REMAINING_SOURCE_HEADROOM_REQUIRED';END IF;
 FOR r IN SELECT request_key FROM population_top3000_202608_202609_v1.ref01_date_request_v4 WHERE manifest->>'as_of' IN('2026-08-04','2026-08-05') LOOP
  PERFORM population_top3000_202608_202609_v1.ref01_date_admit_v4(a.owner_id,a.owner_fence,a.stop_generation,r.request_key,14,14,least(clock_timestamp()+interval '25minutes',a.admitted_until),'Provider-wait repair: renew remaining original Aug4/Aug5 window; preserve exact56 cumulative HTTP, per-date14 calls/pages, originals, counters and original deadline.');
 END LOOP;
END;$r$;
