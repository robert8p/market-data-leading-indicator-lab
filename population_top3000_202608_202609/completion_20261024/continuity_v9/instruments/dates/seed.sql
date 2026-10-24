DO $f$
DECLARE p jsonb;dates jsonb;iv jsonb;
BEGIN
 PERFORM population_top3000_202608_202609_v1.continuation_guard_v6('top3000-completion-20261024-root-5db48b32d103',3,1);
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.ref01_date_plan_v9) THEN RAISE EXCEPTION 'REF01_DATE_PLAN_ALREADY_EXISTS';END IF;
 SELECT jsonb_agg(d::date::text ORDER BY d) INTO dates FROM generate_series(DATE '2026-08-01',DATE '2026-09-30',INTERVAL '1day') d WHERE d::date<>DATE '2026-08-03';
 iv:=jsonb_build_object('scope_sha256','8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd','policy_sha256','3e49d661fcdb8533e5ce88a85689b41e9dde71eeae2dc8578c9e31f9ae1738bb','cohort_sha256','2dc0f766befed6de6b052221c3c87482d507ba1bad931cbb98654eb3af899bec','membership_acceptance_sha256','20b9dac759857e9b510ccaeca33e4a94d8c748d588fb0cc27740cc66c389b5f9','assignment_event_id',28605,'engineering_successor_event_id',28605);
 p:=jsonb_build_object('revision','REF01_FULL_PERIOD_DATED_SOURCE_CONTINUATION_V9','owner_id','top3000-completion-20261024-root-5db48b32d103','owner_fence',3,'stop_generation',1,'authority_event_id',9528,'assignment_event_id',28605,'root_claim_event_id',29634,'source_admission_basis','New independently reviewed fullperiod finite plan under current user authority9528 and rootclaim29634;1200 cumulativecalls supersede inherited144-call assignment allowance only after current root records this plan admission.',
 'dates',dates,'retained_aug3_anchor','27c81878e9ea5ce6eb05ead025f5c9aaae11108af50ba59ecd65b3402c877c86','input_versions',iv,
 'max_provider_calls',1200,'max_http_invocations',1200,'max_source_bytes',300000000,'max_relation_bytes',3000000000,
 'initial_page_tranche',14,'renewal_page_tranche',10,'max_pages_per_date',24,'expected_date_count',60,
 'measurement',jsonb_build_object('retained_pages',40,'retained_rows',38096,'retained_wire_bytes',11268945,'date_relation_bytes',60907520,'dated_relation_bytes',21127168,'measurement_utc','2026-10-24T15:24:00Z'),
 'planning_basis','40 retained pages measured; 840 pages expected across 60 date censuses, <=1200 finite cumulative requests. 300MB source and 3GB combined relation plan ceiling (relation+2*raw+16MiB); 40-page measured~82MB relations scales to~1.72GB across840pages plus~473MB source allowance and16MiB margin, about2.21GB forecast;3GB includes~36percent margin; global limits remain binding and trigger deduplicated executable clearing work. Every date includes all active provider stocks; no inactive history or family acceptance inferred.');
 INSERT INTO population_top3000_202608_202609_v1.ref01_date_plan_v9(plan_id,content,content_sha256,code_bundle_sha256,enabled)
 VALUES('ref01-date-fullperiod-v9',p,encode(sha256(convert_to(p::text,'UTF8')),'hex'),population_top3000_202608_202609_v1.ref01_date_bundle_v4(),true);
 INSERT INTO population_top3000_202608_202609_v1.ref01_date_plan_request_v9(plan_id,as_of,request_key,manifest_sha256,input_versions_sha256)
 SELECT 'ref01-date-fullperiod-v9',(manifest->>'as_of')::date,request_key,manifest_sha256,encode(sha256(convert_to(input_versions::text,'UTF8')),'hex')
 FROM population_top3000_202608_202609_v1.ref01_date_request_v4
 WHERE (manifest->>'as_of')::date IN(DATE '2026-08-01',DATE '2026-08-02',DATE '2026-08-04',DATE '2026-08-05');
 IF (SELECT count(*) FROM population_top3000_202608_202609_v1.ref01_date_plan_request_v9)<>4 THEN RAISE EXCEPTION 'EXACT_LEGACY_REQUEST_POPULATION_REQUIRED';END IF;
END;$f$;
