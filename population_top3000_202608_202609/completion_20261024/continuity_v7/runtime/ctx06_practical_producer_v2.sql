
CREATE TABLE population_top3000_202608_202609_v1.ctx06_practical_release_v2(
 release_sha256 text PRIMARY KEY CHECK(release_sha256 ~ '^[0-9a-f]{64}$'),
 cohort_sha256 text NOT NULL, acceptance_sha256 text NOT NULL,
 policy_sha256 text NOT NULL, daily_run_id uuid NOT NULL,
 daily_input_versions_sha256 text NOT NULL,
 independent_review_event_id bigint NOT NULL REFERENCES population_top3000_202608_202609_v1.pipeline_event(event_id),
 content jsonb NOT NULL, created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 CHECK(release_sha256=encode(sha256(convert_to(content::text,'UTF8')),'hex'))
);
ALTER TABLE population_top3000_202608_202609_v1.ctx06_practical_release_v2 ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON population_top3000_202608_202609_v1.ctx06_practical_release_v2 FROM PUBLIC,anon,authenticated;

CREATE FUNCTION population_top3000_202608_202609_v1.ctx06_practical_dependencies_v2(p_daily uuid,p_versions jsonb)
RETURNS jsonb LANGUAGE plpgsql STABLE SET search_path='pg_catalog' SET "TimeZone"='UTC' AS $fn$
DECLARE m record;s record;d record;r record;e jsonb;n integer;bad integer;ready boolean:=false;why text:='QUALIFIED_FULL_PERIOD_RELEASE_REQUIRED';u jsonb;
BEGIN
 IF p_versions->>'policy_sha256' IS DISTINCT FROM '3e49d661fcdb8533e5ce88a85689b41e9dde71eeae2dc8578c9e31f9ae1738bb'
 OR p_versions->>'cohort_sha256' IS DISTINCT FROM '2dc0f766befed6de6b052221c3c87482d507ba1bad931cbb98654eb3af899bec'
 OR p_versions->>'membership_acceptance_sha256' IS DISTINCT FROM '20b9dac759857e9b510ccaeca33e4a94d8c748d588fb0cc27740cc66c389b5f9'
 OR p_versions->>'scope_sha256' IS DISTINCT FROM '8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd'
 THEN RAISE EXCEPTION 'CTX06_V2_STALE_POLICY_OR_COHORT';END IF;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.acquisition_control a CROSS JOIN population_top3000_202608_202609_v1.pipeline_control c
 WHERE a.singleton AND c.singleton AND a.owner_id='top3000-completion-20261024-root-5db48b32d103' AND a.owner_fence=3 AND a.stop_generation=1
 AND a.enabled AND NOT a.stopped AND a.hold_reason IS NULL AND c.enabled AND a.admitted_until>clock_timestamp() AND c.deadline>clock_timestamp()
 AND c.expected_owner_id=a.owner_id AND c.expected_owner_fence=a.owner_fence AND c.expected_stop_generation=a.stop_generation
 AND a.scope_sha256=p_versions->>'scope_sha256' AND c.scope_sha256=a.scope_sha256
 AND c.budget_evidence#>>'{cohort_selection_policy,policy_sha256}'=p_versions->>'policy_sha256')
 THEN RAISE EXCEPTION 'CTX06_V2_CURRENT_OWNER_STOP_REQUIRED';END IF;
 SELECT * INTO s FROM population_top3000_202608_202609_v1.ref01_practical_snapshot_v1 WHERE snapshot_sha256=p_versions->>'cohort_sha256';
 SELECT * INTO m FROM population_top3000_202608_202609_v1.ref01_practical_membership_v1 WHERE snapshot_sha256=p_versions->>'cohort_sha256' AND acceptance_sha256=p_versions->>'membership_acceptance_sha256';
 IF s.snapshot_sha256 IS NULL OR m.acceptance_sha256 IS NULL OR s.policy_sha256 IS DISTINCT FROM p_versions->>'policy_sha256'
 OR encode(sha256(convert_to(s.content::text,'UTF8')),'hex') IS DISTINCT FROM s.snapshot_sha256
 OR encode(sha256(convert_to(m.content::text,'UTF8')),'hex') IS DISTINCT FROM m.acceptance_sha256
 OR s.content->>'selected_company_count' IS DISTINCT FROM '3000' OR jsonb_array_length(s.content->'companies')<>3000
 THEN RAISE EXCEPTION 'CTX06_V2_ACCEPTED_SNAPSHOT_CHANGED';END IF;
 SELECT count(DISTINCT x->>'company_id'),count(DISTINCT (x->>'selection_rank')::integer) INTO n,bad FROM jsonb_array_elements(s.content->'companies')x;
 IF n<>3000 OR bad<>3000 OR EXISTS(SELECT 1 FROM jsonb_array_elements(s.content->'companies')x WHERE (x->>'selection_rank')::integer NOT BETWEEN 1 AND 3000)
 THEN RAISE EXCEPTION 'CTX06_V2_SELECTED_COMPANY_GRID_INVALID';END IF;
 SELECT * INTO d FROM population_top3000_202608_202609_v1.daily_shared_run_v1 WHERE run_id=p_daily;
 IF d.run_id IS NULL OR d.normalized_at IS NULL OR d.derived_at IS NULL OR d.normalized_series<>d.series_count OR d.derived_series<>d.series_count
 OR encode(sha256(convert_to(d.input_versions::text,'UTF8')),'hex') IS DISTINCT FROM p_versions->>'daily_input_versions_sha256'
 OR d.calendar_manifest IS DISTINCT FROM population_top3000_202608_202609_v1.daily_shared_calendar_v1()
 THEN RAISE EXCEPTION 'CTX06_V2_DAILY_SOURCE_CHANGED';END IF;
 SELECT * INTO r FROM population_top3000_202608_202609_v1.ctx06_practical_release_v2 WHERE release_sha256=p_versions->>'instrument_manifest_sha256';
 IF r.release_sha256 IS NOT NULL THEN
  IF r.cohort_sha256 IS DISTINCT FROM s.snapshot_sha256 OR r.acceptance_sha256 IS DISTINCT FROM m.acceptance_sha256
  OR r.policy_sha256 IS DISTINCT FROM s.policy_sha256 OR r.daily_run_id IS DISTINCT FROM p_daily
  OR r.daily_input_versions_sha256 IS DISTINCT FROM p_versions->>'daily_input_versions_sha256'
  OR r.release_sha256 IS DISTINCT FROM encode(sha256(convert_to(r.content::text,'UTF8')),'hex') THEN RAISE EXCEPTION 'CTX06_V2_RELEASE_BINDING_CHANGED';END IF;
  SELECT details INTO e FROM population_top3000_202608_202609_v1.pipeline_event WHERE event_id=r.independent_review_event_id AND event='CTX06_DATED_RELEASE_INDEPENDENT_REVIEW_V2';
  ready:=(e->>'approved'='true' AND e->>'full_period_instrument_evidence_verified'='true'
   AND e->>'instrument_manifest_sha256'=r.release_sha256 AND e->>'cohort_sha256'=r.cohort_sha256
   AND e->>'membership_acceptance_sha256'=r.acceptance_sha256 AND e->>'policy_sha256'=r.policy_sha256
   AND e->>'daily_run_id'=p_daily::text AND e->>'daily_input_versions_sha256'=r.daily_input_versions_sha256
   AND e->>'scope_sha256'=p_versions->>'scope_sha256'
   AND e->>'owner_id'='top3000-completion-20261024-root-5db48b32d103' AND e->>'owner_fence'='3' AND e->>'stop_generation'='1'
   AND nullif(e->>'producer','') IS NOT NULL AND nullif(e->>'reviewer','') IS NOT NULL AND e->>'producer' IS DISTINCT FROM e->>'reviewer'
   AND e->>'fixture_only'='false' AND r.content->>'fixture_only'='false'
   AND r.content->>'window_start'='2026-08-01' AND r.content->>'window_end'='2026-09-30'
   AND r.content->>'evidence_semantics'='QUALIFIED_DATED_INSTRUMENT_INTERVALS_WITH_EXPLICIT_LIFECYCLE_COVERAGE_V2'
   AND r.content->>'full_period_verified'='true' AND jsonb_typeof(r.content->'instruments')='array'
   AND jsonb_typeof(r.content->'company_coverage')='array') IS TRUE;
  IF ready THEN
   n:=jsonb_array_length(r.content->'instruments');
   IF n NOT BETWEEN 3000 AND 50000 OR jsonb_array_length(r.content->'company_coverage')<>3000 THEN RAISE EXCEPTION 'CTX06_V2_RELEASE_POPULATION_INVALID';END IF;
   IF (SELECT count(DISTINCT x->>'company_id') FROM jsonb_array_elements(r.content->'company_coverage')x)<>3000
   OR EXISTS(SELECT 1 FROM jsonb_array_elements(r.content->'company_coverage')x WHERE coalesce(x->>'status','') NOT IN('QUALIFIED_INTERVALS','VERIFIED_NO_ELIGIBLE_LISTING')
    OR jsonb_typeof(x->'source_refs') IS DISTINCT FROM 'array' OR jsonb_array_length(x->'source_refs')=0
    OR NOT EXISTS(SELECT 1 FROM jsonb_array_elements(s.content->'companies')c WHERE c->>'company_id'=x->>'company_id'))
   OR EXISTS(SELECT 1 FROM jsonb_array_elements(r.content->'instruments')i WHERE i->>'company_id' IS NULL OR nullif(i->>'security_id','') IS NULL
    OR nullif(i->>'listing_episode_id','') IS NULL OR nullif(i->>'ticker','') IS NULL OR i->>'listing_start' IS NULL
    OR jsonb_typeof(i->'eligible_common_equity') IS DISTINCT FROM 'boolean' OR coalesce(i->>'security_type','') NOT IN('CS','ADRC')
    OR (i->>'listing_end' IS NOT NULL AND (i->>'listing_end')::timestamptz<=(i->>'listing_start')::timestamptz)
    OR jsonb_typeof(i#>'{links_evidence,source_refs}') IS DISTINCT FROM 'array' OR jsonb_array_length(i#>'{links_evidence,source_refs}')=0
    OR NOT EXISTS(SELECT 1 FROM jsonb_array_elements(s.content->'companies')c WHERE c->>'company_id'=i->>'company_id'))
   THEN RAISE EXCEPTION 'CTX06_V2_INTERVAL_OR_COVERAGE_INVALID';END IF;
   IF EXISTS(SELECT 1 FROM jsonb_array_elements(r.content->'company_coverage')x WHERE
    (x->>'status'='QUALIFIED_INTERVALS' AND NOT EXISTS(SELECT 1 FROM jsonb_array_elements(r.content->'instruments')i WHERE i->>'company_id'=x->>'company_id'))
    OR (x->>'status'='VERIFIED_NO_ELIGIBLE_LISTING' AND EXISTS(SELECT 1 FROM jsonb_array_elements(r.content->'instruments')i WHERE i->>'company_id'=x->>'company_id' AND i->'eligible_common_equity'='true'::jsonb AND (i->>'listing_start')::timestamptz<TIMESTAMPTZ '2026-10-01 00:00Z' AND (i->>'listing_end' IS NULL OR (i->>'listing_end')::timestamptz>TIMESTAMPTZ '2026-08-01 00:00Z'))))
   THEN RAISE EXCEPTION 'CTX06_V2_COVERAGE_INSTRUMENT_CONFLICT';END IF;
   IF (SELECT count(DISTINCT (i->>'company_id',i->>'security_id',i->>'listing_episode_id')) FROM jsonb_array_elements(r.content->'instruments')i)<>n
   THEN RAISE EXCEPTION 'CTX06_V2_DUPLICATE_LISTING_IDENTITY';END IF;
   why:=NULL;
  ELSE why:='INDEPENDENT_FULL_PERIOD_RELEASE_REVIEW_REQUIRED';END IF;
 END IF;
 u:=jsonb_build_object('status','ACCEPTED_PRACTICAL_COHORT_SOURCE_CALCULATION_ONLY','company_count',3000,
 'company_manifest_sha256',s.snapshot_sha256,'instrument_manifest_sha256',r.release_sha256,
 'instrument_count',CASE WHEN ready THEN jsonb_array_length(r.content->'instruments') ELSE NULL END,
 'policy_sha256',s.policy_sha256,'membership_acceptance_sha256',m.acceptance_sha256);
 RETURN jsonb_build_object('ready',ready,'blocker',why,'universe',u,'calendar',d.calendar_manifest,
 'daily_input_versions',d.input_versions,'daily_series_count',d.series_count,'company_denominator',3000,
 'full_period_instrument_evidence_verified',ready,'family_accepted',false);
END $fn$;

CREATE FUNCTION population_top3000_202608_202609_v1.ctx06_practical_companies_v2(p_release text)
RETURNS TABLE(company_ordinal integer,company_id text,company_record jsonb,instruments jsonb,snapshot_sha256 bytea)
LANGUAGE sql STABLE SET search_path='pg_catalog' AS $fn$
 WITH rel AS MATERIALIZED(SELECT * FROM population_top3000_202608_202609_v1.ctx06_practical_release_v2 WHERE release_sha256=p_release),
 ins AS MATERIALIZED(SELECT i->>'company_id' id,jsonb_agg(i ORDER BY i->>'security_id' COLLATE "C",i->>'listing_episode_id' COLLATE "C") js FROM rel CROSS JOIN LATERAL jsonb_array_elements(content->'instruments')i GROUP BY i->>'company_id'),
 co AS MATERIALIZED(SELECT c FROM rel JOIN population_top3000_202608_202609_v1.ref01_practical_snapshot_v1 s ON s.snapshot_sha256=rel.cohort_sha256 CROSS JOIN LATERAL jsonb_array_elements(s.content->'companies')c)
 SELECT (c->>'selection_rank')::integer,c->>'company_id',c,coalesce(js,'[]'::jsonb),sha256(convert_to(jsonb_build_object('company_record',c,'instruments',coalesce(js,'[]'::jsonb))::text,'UTF8')) FROM co LEFT JOIN ins ON id=c->>'company_id';
$fn$;

CREATE FUNCTION population_top3000_202608_202609_v1.ctx06_practical_cohort_sha_v2(p_release text) RETURNS bytea
LANGUAGE sql STABLE SET search_path='pg_catalog' AS $fn$
 SELECT CASE WHEN count(*)=3000 AND count(DISTINCT company_ordinal)=3000 AND min(company_ordinal)=1 AND max(company_ordinal)=3000
 THEN sha256(convert_to(string_agg(company_ordinal||':'||encode(snapshot_sha256,'hex'),E'\n' ORDER BY company_ordinal),'UTF8')) END
 FROM population_top3000_202608_202609_v1.ctx06_practical_companies_v2(p_release);
$fn$;
CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.ctx06_practical_prepare_v2(p_key text, p_daily uuid, p_versions jsonb)
 RETURNS uuid
 LANGUAGE plpgsql
 SET search_path TO 'pg_catalog'
 SET "TimeZone" TO 'UTC'
AS $function$
DECLARE v_dep jsonb;v_rid uuid:=gen_random_uuid();v_n integer;v_in integer;v_bad integer;v_hash bytea;v_sources bytea;v_existing uuid;
BEGIN
 v_dep:=population_top3000_202608_202609_v1.ctx06_practical_dependencies_v2(p_daily,p_versions);
 IF v_dep->'ready'IS DISTINCT FROM 'true'::jsonb THEN RAISE EXCEPTION 'CTX06_FROZEN_COHORT_AND_COMPLETE_D03_REQUIRED';END IF;
 IF p_key IS DISTINCT FROM 'ctx06-practical-v2:'||encode(sha256(convert_to(p_versions::text,'UTF8')),'hex') OR p_versions->>'ctx06_bundle_sha256'IS DISTINCT FROM population_top3000_202608_202609_v1.ctx06_practical_bundle_sha_v2()
 OR p_versions->>'daily_input_versions_sha256'IS DISTINCT FROM encode(sha256(convert_to((v_dep->'daily_input_versions')::text,'UTF8')),'hex')
 OR v_dep->'calendar'IS DISTINCT FROM population_top3000_202608_202609_v1.daily_shared_calendar_v1()
 THEN RAISE EXCEPTION 'CTX06_PREPARE_SCOPE_OR_CODE_MISMATCH';END IF;
 SELECT r.run_id INTO v_existing FROM population_top3000_202608_202609_v1.ctx06_breadth_run_v1 r WHERE r.run_key=p_key;
 IF v_existing IS NOT NULL THEN
  IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.ctx06_breadth_run_v1 r WHERE r.run_id=v_existing
    AND r.input_versions=p_versions AND r.daily_run_id=p_daily AND r.universe_snapshot=v_dep->'universe' AND r.calendar_snapshot=v_dep->'calendar')
  THEN RAISE EXCEPTION 'CTX06_PREPARE_REPLAY_CONFLICT';END IF;
  RETURN v_existing;
 END IF;
 PERFORM population_top3000_202608_202609_v1.d02_daily_resource_guard_v2();
 SELECT count(*) INTO v_in FROM population_top3000_202608_202609_v1.ctx06_practical_release_v2 r CROSS JOIN LATERAL jsonb_array_elements(r.content->'instruments')i WHERE r.release_sha256=p_versions->>'instrument_manifest_sha256';
 IF v_in NOT BETWEEN 3000 AND 50000 THEN RAISE EXCEPTION 'CTX06_V2_INSTRUMENT_COUNT_INVALID';END IF;
 SELECT sha256(convert_to(string_agg(jsonb_build_array(s.series_no,s.source_ticker,encode(s.input_sha256,'hex'),encode(s.output_sha256,'hex'),
  s.candidate_snapshot,s.mapping_snapshot)::text,E'\n'ORDER BY s.series_no),'UTF8')) INTO v_sources
 FROM population_top3000_202608_202609_v1.daily_shared_series_v1 s WHERE s.run_id=p_daily;
 IF(SELECT count(*)FROM population_top3000_202608_202609_v1.daily_shared_series_v1 s WHERE s.run_id=p_daily AND s.derived_at IS NOT NULL
  AND s.output_rows=42 AND s.input_rows=103 AND s.input_sha256 IS NOT NULL AND s.output_sha256 IS NOT NULL)
  IS DISTINCT FROM(v_dep->>'daily_series_count')::integer THEN RAISE EXCEPTION 'CTX06_ALL_SOURCE_SERIES_TERMINAL_REQUIRED';END IF;
 INSERT INTO population_top3000_202608_202609_v1.ctx06_breadth_run_v1(run_id,run_key,daily_run_id,input_versions,universe_snapshot,calendar_snapshot,
  company_snapshot_sha256,source_series_manifest_sha256,company_count,instrument_count)
 VALUES(v_rid,p_key,p_daily,p_versions,v_dep->'universe',v_dep->'calendar',sha256(''::bytea),v_sources,3000,v_in);
 INSERT INTO population_top3000_202608_202609_v1.ctx06_breadth_company_v1(run_id,company_ordinal,company_id,company_record,instruments,snapshot_sha256) SELECT v_rid,c.* FROM population_top3000_202608_202609_v1.ctx06_practical_companies_v2(p_versions->>'instrument_manifest_sha256')c;
 SELECT sha256(convert_to(string_agg(c.company_ordinal||':'||encode(c.snapshot_sha256,'hex'),E'\n'ORDER BY c.company_ordinal),'UTF8'))INTO v_hash
 FROM population_top3000_202608_202609_v1.ctx06_breadth_company_v1 c WHERE c.run_id=v_rid;
 UPDATE population_top3000_202608_202609_v1.ctx06_breadth_run_v1 SET company_snapshot_sha256=v_hash WHERE run_id=v_rid;
 PERFORM population_top3000_202608_202609_v1.d02_daily_resource_guard_v2();RETURN v_rid;
END $function$
;
CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.ctx06_practical_context_v2(p_task uuid)
 RETURNS uuid
 LANGUAGE plpgsql
 STABLE
 SET search_path TO 'pg_catalog'
 SET "TimeZone" TO 'UTC'
AS $function$
DECLARE v_t population_top3000_202608_202609_v1.pipeline_task%rowtype;
 v_r population_top3000_202608_202609_v1.ctx06_breadth_run_v1%rowtype;v_d jsonb;
BEGIN
 SELECT * INTO STRICT v_t FROM population_top3000_202608_202609_v1.pipeline_task t WHERE t.task_id=p_task;
 IF v_t.family_id IS DISTINCT FROM 'CTX06' OR v_t.payload->>'kind'IS DISTINCT FROM 'CTX06_PRACTICAL_COMPANY_BREADTH_V2'
 OR v_t.partition_key IS DISTINCT FROM 'ctx06-practical-v2:'||encode(sha256(convert_to(v_t.input_versions::text,'UTF8')),'hex')
 OR v_t.input_versions->>'scope_sha256'IS DISTINCT FROM '8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd'
 OR v_t.input_versions->>'ctx06_contract_sha256'IS DISTINCT FROM '0b754b05d4998bb4c1008d35f7db9aa72de5a0e33d0fb06748a56454ea598d21'
 OR v_t.input_versions->>'ctx06_bundle_sha256'IS DISTINCT FROM population_top3000_202608_202609_v1.ctx06_practical_bundle_sha_v2()
 OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.catalog_family_contract c WHERE c.family_id='CTX06'
  AND c.scope_id='corrected_company_scope_8e6ca1178f03822c' AND c.contract_sha256=v_t.input_versions->>'ctx06_contract_sha256')
 THEN RAISE EXCEPTION 'CTX06_TASK_SCOPE_OR_CODE_MISMATCH';END IF;
 SELECT * INTO v_r FROM population_top3000_202608_202609_v1.ctx06_breadth_run_v1 r WHERE r.run_key=v_t.partition_key;
 v_d:=population_top3000_202608_202609_v1.ctx06_practical_dependencies_v2((v_t.payload->>'daily_run_id')::uuid,v_t.input_versions);
 IF v_t.stage='ACQUIRE' AND v_r.run_id IS NULL THEN RETURN NULL;END IF;
 IF v_r.run_id IS NULL OR v_r.input_versions IS DISTINCT FROM v_t.input_versions OR v_r.daily_run_id IS DISTINCT FROM(v_t.payload->>'daily_run_id')::uuid
 OR v_d->'ready'IS DISTINCT FROM 'true'::jsonb OR v_d->'universe'IS DISTINCT FROM v_r.universe_snapshot
 OR v_d->'calendar'IS DISTINCT FROM v_r.calendar_snapshot
 OR encode(sha256(convert_to((v_d->'daily_input_versions')::text,'UTF8')),'hex')IS DISTINCT FROM v_t.input_versions->>'daily_input_versions_sha256'
 OR v_r.company_snapshot_sha256 IS DISTINCT FROM population_top3000_202608_202609_v1.ctx06_practical_cohort_sha_v2(v_t.input_versions->>'instrument_manifest_sha256')
 OR v_r.company_snapshot_sha256 IS DISTINCT FROM(SELECT sha256(convert_to(string_agg(c.company_ordinal||':'||encode(c.snapshot_sha256,'hex'),E'\n'ORDER BY c.company_ordinal),'UTF8'))
  FROM population_top3000_202608_202609_v1.ctx06_breadth_company_v1 c WHERE c.run_id=v_r.run_id)
 THEN RAISE EXCEPTION 'CTX06_FROZEN_DEPENDENCY_CHANGED';END IF;
 RETURN v_r.run_id;
END $function$
;
CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.ctx06_practical_normalize_v2(p_attempt uuid, p_token text)
 RETURNS jsonb
 LANGUAGE plpgsql
 SET search_path TO 'pg_catalog'
 SET "TimeZone" TO 'UTC'
AS $function$
DECLARE v_tid uuid;v_rid uuid;v_t population_top3000_202608_202609_v1.pipeline_task%rowtype;
 v_r population_top3000_202608_202609_v1.ctx06_breadth_run_v1%rowtype;v_first integer;v_last integer;v_cap integer;v_n integer;v_h bytea;v_c record;v_now jsonb;v_s record;v_seen integer:=0;
BEGIN
 v_tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);v_rid:=population_top3000_202608_202609_v1.ctx06_practical_context_v2(v_tid);
 SELECT * INTO v_t FROM population_top3000_202608_202609_v1.pipeline_task t WHERE t.task_id=v_tid;
 SELECT * INTO v_r FROM population_top3000_202608_202609_v1.ctx06_breadth_run_v1 r WHERE r.run_id=v_rid FOR UPDATE;
 IF v_t.stage IS DISTINCT FROM 'NORMALIZE' OR v_r.normalized_at IS NOT NULL
 OR coalesce((v_t.checkpoint->>'last_company')::integer,0)<>v_r.normalized_companies THEN RAISE EXCEPTION 'CTX06_NORMALIZE_CHECKPOINT_MISMATCH';END IF;
 IF coalesce(v_t.payload->>'max_companies','')!~'^[0-9]{1,4}$' THEN RAISE EXCEPTION 'CTX06_BOUNDED_COMPANY_ADMISSION_REQUIRED';END IF;
 v_cap:=(v_t.payload->>'max_companies')::integer;
 IF v_cap NOT BETWEEN 100 AND 3000 OR v_cap%100<>0 THEN RAISE EXCEPTION 'CTX06_BOUNDED_COMPANY_ADMISSION_REQUIRED';END IF;
 IF v_r.normalized_companies>=v_cap THEN RETURN jsonb_build_object('state','BLOCKED','blocker_code','CTX06_BATCH_RESOURCE_REVIEW',
  'resumption_condition',jsonb_build_object('committed_companies',v_r.normalized_companies,'required_total',3000,
   'next_action','Measure the first real 100-company/4,200-vote batch, then admit finite continuation with the same source and cohort manifests.'));END IF;
 PERFORM population_top3000_202608_202609_v1.d02_daily_resource_guard_v2();
 v_first:=v_r.normalized_companies+1;v_last:=least(v_r.normalized_companies+100,3000,v_cap);
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.ctx06_breadth_company_v1 c LEFT JOIN population_top3000_202608_202609_v1.ctx06_practical_companies_v2(v_t.input_versions->>'instrument_manifest_sha256')n USING(company_ordinal) WHERE c.run_id=v_rid AND c.company_ordinal BETWEEN v_first AND v_last AND c.snapshot_sha256 IS DISTINCT FROM n.snapshot_sha256) THEN RAISE EXCEPTION 'CTX06_V2_COMPANY_OR_INSTRUMENT_CHANGED';END IF;
 FOR v_s IN SELECT DISTINCT s.series_no FROM population_top3000_202608_202609_v1.ctx06_breadth_company_v1 c
  CROSS JOIN LATERAL jsonb_array_elements(c.instruments)q(j)
  JOIN population_top3000_202608_202609_v1.daily_shared_series_v1 s ON s.run_id=v_r.daily_run_id AND s.source_ticker=q.j->>'ticker'
  WHERE c.run_id=v_rid AND c.company_ordinal BETWEEN v_first AND v_last ORDER BY s.series_no LIMIT 1001 LOOP
  v_seen:=v_seen+1;IF v_seen>1000 THEN RAISE EXCEPTION 'CTX06_SOURCE_SERIES_BATCH_BOUND_EXCEEDED';END IF;
  PERFORM population_top3000_202608_202609_v1.ctx06_verify_series_v1(v_r.daily_run_id,v_s.series_no);
 END LOOP;
 WITH vals AS MATERIALIZED(
  SELECT population_top3000_202608_202609_v1.ctx06_build_vote_v1(v_rid,c.company_ordinal,(d.j->>'date')::date)j
  FROM population_top3000_202608_202609_v1.ctx06_breadth_company_v1 c
  CROSS JOIN LATERAL jsonb_array_elements(v_r.calendar_snapshot->'sessions')d(j)
  WHERE c.run_id=v_rid AND c.company_ordinal BETWEEN v_first AND v_last AND d.j->'is_target'='true'::jsonb
 ), ins AS(
  INSERT INTO population_top3000_202608_202609_v1.ctx06_company_vote_v1(run_id,company_ordinal,session_date,company_id,security_id,listing_episode_id,
   source_series_no,return_1d,missing_reason,aggregation_rule,source_quality_mask,source_reference,record_sha256)
  SELECT v_rid,(j->>'company_ordinal')::integer,(j->>'session_date')::date,j->>'company_id',j->>'security_id',j->>'listing_episode_id',
   (j->>'source_series_no')::integer,(j->>'return_1d')::float8,j->>'missing_reason',j->>'aggregation_rule',(j->>'source_quality_mask')::integer,j->'source_reference',decode(j->>'record_sha256','hex')
  FROM vals RETURNING *
 )SELECT count(*),sha256(convert_to(string_agg(company_ordinal||':'||session_date||':'||encode(record_sha256,'hex'),E'\n'ORDER BY company_ordinal,session_date),'UTF8'))INTO v_n,v_h FROM ins;
 IF v_n<>42*(v_last-v_first+1)THEN RAISE EXCEPTION 'CTX06_EXACT_COMPANY_SESSION_GRID_REQUIRED';END IF;
 INSERT INTO population_top3000_202608_202609_v1.ctx06_breadth_batch_v1(run_id,first_company,last_company,row_count,manifest_sha256,input_versions,first_attempt_id)
 VALUES(v_rid,v_first,v_last,v_n,v_h,v_t.input_versions,p_attempt);
 UPDATE population_top3000_202608_202609_v1.ctx06_breadth_run_v1 SET normalized_companies=v_last,
  normalized_at=CASE WHEN v_last=3000 THEN clock_timestamp()END WHERE run_id=v_rid;
 PERFORM population_top3000_202608_202609_v1.d02_daily_resource_guard_v2();
 RETURN jsonb_build_object('state',CASE WHEN v_last=3000 THEN 'COMPLETE' ELSE 'CONTINUE'END,'checkpoint',jsonb_build_object('last_company',v_last),
  'commit_receipt',jsonb_build_object('commit_key','ctx06-votes:'||v_rid||':'||v_first||':'||v_last,
   'input_versions',v_t.input_versions,'immutable_output_ids',jsonb_build_array(v_rid||':'||v_first||':'||v_last),
   'output_sha256',encode(v_h,'hex'),'new_committed_units',v_n,'unresolved_votes_remain_in_cohort_denominator',true,'source_accuracy_verified',false));
END $function$
;
CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.ctx06_practical_derive_v2(p_attempt uuid, p_token text)
 RETURNS jsonb
 LANGUAGE plpgsql
 SET search_path TO 'pg_catalog'
 SET "TimeZone" TO 'UTC'
AS $function$
DECLARE v_tid uuid;v_rid uuid;v_t population_top3000_202608_202609_v1.pipeline_task%rowtype;
 v_r population_top3000_202608_202609_v1.ctx06_breadth_run_v1%rowtype;v_date date;v_n integer;v_ord integer;v_bad integer;v_votes jsonb;v_metrics jsonb;v_h bytea;v_rec jsonb;
BEGIN
 v_tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);v_rid:=population_top3000_202608_202609_v1.ctx06_practical_context_v2(v_tid);
 SELECT * INTO v_t FROM population_top3000_202608_202609_v1.pipeline_task t WHERE t.task_id=v_tid;
 SELECT * INTO v_r FROM population_top3000_202608_202609_v1.ctx06_breadth_run_v1 r WHERE r.run_id=v_rid FOR UPDATE;
 IF v_t.stage IS DISTINCT FROM 'DERIVE' OR v_r.normalized_at IS NULL OR v_r.normalized_companies<>3000 OR v_r.derived_at IS NOT NULL
  OR coalesce((v_t.checkpoint->>'last_session')::integer,0)<>v_r.derived_sessions THEN RAISE EXCEPTION 'CTX06_DERIVE_CHECKPOINT_MISMATCH';END IF;
 v_ord:=v_r.derived_sessions+1;
 SELECT(d.j->>'date')::date INTO v_date FROM jsonb_array_elements(v_r.calendar_snapshot->'sessions')d(j)
 WHERE d.j->'is_target'='true'::jsonb ORDER BY d.j->>'date' LIMIT 1 OFFSET v_r.derived_sessions;
 IF v_date IS NULL THEN RAISE EXCEPTION 'CTX06_FROZEN_CALENDAR_GRID_MISMATCH';END IF;
 PERFORM population_top3000_202608_202609_v1.d02_daily_resource_guard_v2();
 -- Reconstruct this day's vote from its current bound input. All 3,000 companies
 -- are checked; source absence is a reason, never a fabricated zero return.
 WITH actual AS MATERIALIZED(
  SELECT v.*,((to_jsonb(v)-'run_id'-'record_sha256')||jsonb_build_object('record_sha256',encode(v.record_sha256,'hex')))j
  FROM population_top3000_202608_202609_v1.ctx06_company_vote_v1 v WHERE v.run_id=v_rid AND v.session_date=v_date
 ), comparison AS MATERIALIZED(
  SELECT c.company_ordinal,a.j,population_top3000_202608_202609_v1.ctx06_build_vote_v1(v_rid,c.company_ordinal,v_date)e
  FROM population_top3000_202608_202609_v1.ctx06_breadth_company_v1 c LEFT JOIN actual a USING(company_ordinal)
  WHERE c.run_id=v_rid
 )SELECT count(*),count(*)FILTER(WHERE j IS DISTINCT FROM e),jsonb_agg(j ORDER BY company_ordinal),
  sha256(convert_to(string_agg(e->>'record_sha256',E'\n'ORDER BY company_ordinal),'UTF8'))
 INTO v_n,v_bad,v_votes,v_h FROM comparison;
 IF v_n<>3000 OR v_bad<>0 OR(SELECT count(*)FROM population_top3000_202608_202609_v1.ctx06_company_vote_v1 v WHERE v.run_id=v_rid AND v.session_date=v_date)<>3000
 THEN RAISE EXCEPTION 'CTX06_FULL_CURRENT_COMPANY_VOTE_RECONCILIATION_FAILED';END IF;
 v_metrics:=population_top3000_202608_202609_v1.ctx06_math_v1(v_votes,3000);
 v_rec:=v_metrics||jsonb_build_object('session_date',v_date,'cohort_id',v_r.universe_snapshot->>'company_manifest_sha256',
  'company_aggregation_rule','SOLE_ACTIVE_ELIGIBLE_SECURITY_OR_REVIEWED_REPRESENTATIVE_V1',
  'instrument_manifest_sha256',v_r.universe_snapshot->>'instrument_manifest_sha256','source_run_id',v_r.daily_run_id,
  'vote_manifest_sha256',encode(v_h,'hex'),'quality_state','PROVISIONAL_COMPANY_COHORT_SOURCE_ARITHMETIC',
  'source_publication_at',NULL,'historical_received_at',NULL,'available_at',NULL,
  'consumer_policy','ARCHIVED_SOURCE_CALCULATION_WITH_EXPLICIT_MISSING_COMPANY_COVERAGE');
 INSERT INTO population_top3000_202608_202609_v1.ctx06_breadth_session_v1(run_id,session_date,session_ordinal,breadth,vote_manifest_sha256,record_sha256,first_attempt_id)
 VALUES(v_rid,v_date,v_ord,v_rec,v_h,sha256(convert_to(v_rec::text,'UTF8')),p_attempt);
 UPDATE population_top3000_202608_202609_v1.ctx06_breadth_run_v1 SET derived_sessions=v_ord,
  derived_at=CASE WHEN v_ord=42 THEN clock_timestamp()END WHERE run_id=v_rid;
 PERFORM population_top3000_202608_202609_v1.d02_daily_resource_guard_v2();
 RETURN jsonb_build_object('state',CASE WHEN v_ord=42 THEN 'COMPLETE'ELSE 'CONTINUE'END,'checkpoint',jsonb_build_object('last_session',v_ord),
  'commit_receipt',jsonb_build_object('commit_key','ctx06-breadth:'||v_rid||':'||v_date,'input_versions',v_t.input_versions,
   'immutable_output_ids',jsonb_build_array(v_rid||':'||v_date),'output_sha256',encode(sha256(convert_to(v_rec::text,'UTF8')),'hex'),
   'new_committed_units',1,'company_votes_reconciled',3000,'valid_company_count',v_metrics->'valid_company_count',
   'missing_company_count',v_metrics->'missing_company_count','source_accuracy_verified',false));
END $function$
;
CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.ctx06_practical_acquire_v2(p_attempt uuid, p_token text)
 RETURNS jsonb
 LANGUAGE plpgsql
 SET search_path TO 'pg_catalog'
AS $function$
DECLARE v_tid uuid;v_t population_top3000_202608_202609_v1.pipeline_task%rowtype;v_d jsonb;v_rid uuid;
BEGIN
 v_tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);
 PERFORM population_top3000_202608_202609_v1.ctx06_practical_context_v2(v_tid);
 SELECT * INTO v_t FROM population_top3000_202608_202609_v1.pipeline_task t WHERE t.task_id=v_tid;
 IF v_t.stage IS DISTINCT FROM 'ACQUIRE' THEN RAISE EXCEPTION 'CTX06_ACQUIRE_ROUTE_MISMATCH';END IF;
 v_d:=population_top3000_202608_202609_v1.ctx06_practical_dependencies_v2((v_t.payload->>'daily_run_id')::uuid,v_t.input_versions);
 IF v_d->'ready'IS DISTINCT FROM 'true'::jsonb THEN
  RETURN jsonb_build_object('state','BLOCKED','blocker_code','CTX06_PRACTICAL_DEPENDENCIES_REQUIRED_V2','resumption_condition',v_d||jsonb_build_object('required_action','Bind a terminal independently reviewed full-period release and real first100/4200 proof; use a newly hash-pinned task when the release becomes available.','automatic_retry',false));
 END IF;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_event e
  WHERE e.event='CTX06_PRACTICAL_FIRST_BATCH_VERIFIED_V2' AND nullif(e.details->>'producer','') IS NOT NULL AND nullif(e.details->>'reviewer','') IS NOT NULL AND e.details->>'producer' IS DISTINCT FROM e.details->>'reviewer' AND e.details->>'fixture_only'='false' AND e.details->>'adapter_sha256'=encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.ctx06_practical_acquire_v2(uuid,text)'::regprocedure),'UTF8')),'hex')
   AND e.details->>'result'='PASS_ACTUAL_FROZEN_COHORT_FIRST100'
   AND e.details->>'ctx06_bundle_sha256'=v_t.input_versions->>'ctx06_bundle_sha256'
   AND e.details->>'cohort_sha256'=v_t.input_versions->>'cohort_sha256' AND e.details->>'policy_sha256'=v_t.input_versions->>'policy_sha256' AND e.details->>'membership_acceptance_sha256'=v_t.input_versions->>'membership_acceptance_sha256' AND e.details->>'full_period_instrument_evidence_verified'='true'
   AND e.details->>'instrument_manifest_sha256'=v_d#>>'{universe,instrument_manifest_sha256}'
   AND e.details->>'daily_run_id'=v_t.payload->>'daily_run_id'
   AND e.details->>'daily_input_versions_sha256'=v_t.input_versions->>'daily_input_versions_sha256'
   AND e.details->>'company_count'='100'AND e.details->>'vote_rows'='4200'
   AND e.details->'source_and_membership_reconciled'='true'::jsonb
   AND e.details->'all_existing_sources_unchanged'='true'::jsonb AND e.details->'rolled_back'='true'::jsonb)
 THEN RETURN jsonb_build_object('state','BLOCKED','blocker_code','CTX06_PRACTICAL_FIRST_REAL_BATCH_GATE_V2',
    'resumption_condition',jsonb_build_object('company_manifest_sha256',v_d#>>'{universe,company_manifest_sha256}',
   'daily_run_id',v_t.payload->>'daily_run_id','required_action','Root must execute and independently reconcile the first actual 100-company/4,200-vote rollback batch under this exact code, then register the reviewed producer adapters.',
   'untested_normalize_dispatch_allowed',false,'rechecks_used',v_t.recheck_count,'maximum_rechecks',v_t.max_rechecks));END IF;
 v_rid:=population_top3000_202608_202609_v1.ctx06_practical_prepare_v2(v_t.partition_key,(v_t.payload->>'daily_run_id')::uuid,v_t.input_versions);
 RETURN jsonb_build_object('state','COMPLETE','commit_receipt',jsonb_build_object('commit_key','ctx06-census:'||v_rid,
  'input_versions',v_t.input_versions,'immutable_output_ids',jsonb_build_array(v_rid),
  'output_sha256',(SELECT encode(r.company_snapshot_sha256,'hex')FROM population_top3000_202608_202609_v1.ctx06_breadth_run_v1 r WHERE r.run_id=v_rid),
  'new_committed_units',3000,'company_votes_expected',126000,'provider_calls',0,'source_accuracy_verified',false));
END $function$
;
CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.ctx06_practical_quality_hold_v2(p_attempt uuid, p_token text)
 RETURNS jsonb
 LANGUAGE plpgsql
 SET search_path TO 'pg_catalog'
AS $function$
DECLARE v_tid uuid;v_rid uuid;v_t population_top3000_202608_202609_v1.pipeline_task%rowtype;v_r population_top3000_202608_202609_v1.ctx06_breadth_run_v1%rowtype;
BEGIN
 v_tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);v_rid:=population_top3000_202608_202609_v1.ctx06_practical_context_v2(v_tid);
 SELECT * INTO v_t FROM population_top3000_202608_202609_v1.pipeline_task t WHERE t.task_id=v_tid;
 SELECT * INTO v_r FROM population_top3000_202608_202609_v1.ctx06_breadth_run_v1 r WHERE r.run_id=v_rid;
 IF v_t.stage NOT IN('VALIDATE','REPAIR')OR v_r.derived_at IS NULL OR v_r.derived_sessions<>42 THEN RAISE EXCEPTION 'CTX06_QUALITY_ROUTE_MISMATCH';END IF;
 RETURN jsonb_build_object('state','BLOCKED','blocker_code','CTX06_SOURCE_SEMANTICS_AND_INDEPENDENT_BREADTH_REVIEW_REQUIRED',
  'resumption_condition',jsonb_build_object('run_id',v_rid,'company_count',3000,'calculated_sessions',42,
   'requirements',jsonb_build_array('D03_REGULAR_CLOSE_AND_ACTION_SEMANTICS','EXACT_DATED_MULTILINE_REPRESENTATIVE_RESOLUTION',
    'INDEPENDENT_ALL_SESSION_MEMBERSHIP_AND_VOTE_RECONCILIATION','CRITICAL_OUTLIER_AND_ACTION_BOUNDARY_CHECKS',
    'ARCHIVED_SOURCE_VERSUS_STRICT_AVAILABILITY_POLICY_AND_CONSUMER_READBACK'),
   'complete_company_input_coverage_claimed',false,'provider_accuracy_verified',false,'quality_promoted',false,'automatic_retry',false));
END $function$

;
CREATE FUNCTION population_top3000_202608_202609_v1.ctx06_practical_bundle_sha_v2() RETURNS text LANGUAGE sql STABLE SET search_path='pg_catalog' AS $fn$ SELECT encode(sha256(convert_to(string_agg(p.oid::regprocedure::text||':'||encode(sha256(convert_to(pg_get_functiondef(p.oid),'UTF8')),'hex'),E'\n' ORDER BY p.oid::regprocedure::text),'UTF8')),'hex') FROM pg_proc p WHERE p.pronamespace='population_top3000_202608_202609_v1'::regnamespace AND p.proname IN ('ctx06_practical_dependencies_v2','ctx06_practical_companies_v2','ctx06_practical_cohort_sha_v2','ctx06_practical_prepare_v2','ctx06_practical_context_v2','ctx06_practical_normalize_v2','ctx06_practical_derive_v2','ctx06_practical_acquire_v2','ctx06_practical_quality_hold_v2','ctx06_math_v1','ctx06_select_security_v1','ctx06_build_vote_v1','ctx06_verify_series_v1','daily_shared_math_v1','daily_shared_calendar_v1','daily_shared_input_json_v1','daily_shared_output_json_v1','d02_daily_resource_guard_v2'); $fn$;
DO $acl$ DECLARE r record;BEGIN FOR r IN SELECT oid::regprocedure::text sig FROM pg_proc WHERE pronamespace='population_top3000_202608_202609_v1'::regnamespace AND proname LIKE 'ctx06_practical_%v2' LOOP EXECUTE 'REVOKE ALL ON FUNCTION '||r.sig||' FROM PUBLIC,anon,authenticated';END LOOP;END $acl$;
