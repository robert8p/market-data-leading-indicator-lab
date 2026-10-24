CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.ref01_date_admit_v4(p_owner text, p_fence bigint, p_stop bigint, p_request text, p_page_ceiling integer, p_call_ceiling integer, p_expires timestamp with time zone, p_reason text)
 RETURNS jsonb
 LANGUAGE plpgsql
 SET search_path TO 'pg_catalog'
AS $function$
DECLARE a population_top3000_202608_202609_v1.acquisition_control%rowtype;
 p population_top3000_202608_202609_v1.pipeline_control%rowtype;
 old population_top3000_202608_202609_v1.ref01_date_admission_v4%rowtype;r jsonb; page_limit int;
BEGIN
 PERFORM pg_advisory_xact_lock(2026083000261022);
 SELECT * INTO STRICT a FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton FOR UPDATE;
 SELECT * INTO STRICT p FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton FOR UPDATE;
 IF p_owner IS NULL OR p_fence IS NULL OR p_stop IS NULL OR a.owner_id IS DISTINCT FROM p_owner OR a.owner_fence IS DISTINCT FROM p_fence OR a.stop_generation IS DISTINCT FROM p_stop
 OR a.stopped IS DISTINCT FROM false OR a.enabled IS DISTINCT FROM true OR p.enabled IS DISTINCT FROM true OR a.hold_reason IS NOT NULL
 OR a.admitted_until IS NULL OR a.admitted_until<=clock_timestamp() OR p.deadline IS NULL OR p.deadline<=clock_timestamp()
 OR p.expected_owner_id IS DISTINCT FROM a.owner_id OR p.expected_owner_fence IS DISTINCT FROM a.owner_fence OR p.expected_stop_generation IS DISTINCT FROM a.stop_generation
 OR a.scope_sha256 IS DISTINCT FROM '8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd' OR p.scope_sha256 IS DISTINCT FROM a.scope_sha256
 OR p.shared_scheduler_verified IS DISTINCT FROM true OR population_top3000_202608_202609_v1.pipeline_cost_admission_v2(p,0) IS DISTINCT FROM true THEN RAISE EXCEPTION 'REF01_CENSUS_CURRENT_CONTROLLER_ADMISSION_REQUIRED';END IF;
 PERFORM population_top3000_202608_202609_v1.d02_daily_resource_guard_v2();
 SELECT least((prq.manifest->>'max_pages')::integer,(pl.content->>'max_pages_per_date')::integer) INTO STRICT page_limit
 FROM population_top3000_202608_202609_v1.ref01_date_plan_v9 pl
 JOIN population_top3000_202608_202609_v1.ref01_date_plan_request_v9 pr USING(plan_id)
 JOIN population_top3000_202608_202609_v1.ref01_date_request_v4 prq USING(request_key)
 WHERE pr.request_key=p_request AND pl.enabled
 AND pl.content_sha256=encode(sha256(convert_to(pl.content::text,'UTF8')),'hex')
 AND pl.code_bundle_sha256=population_top3000_202608_202609_v1.ref01_date_bundle_v4();
 SELECT * INTO old FROM population_top3000_202608_202609_v1.ref01_date_admission_v4 WHERE request_key=p_request FOR UPDATE;
 IF p_page_ceiling IS NULL OR p_call_ceiling IS NULL OR p_page_ceiling NOT BETWEEN greatest(1,coalesce(old.admitted_page_ceiling,0)) AND least(100000,page_limit)
 OR p_call_ceiling NOT BETWEEN greatest(1,coalesce(old.max_provider_calls,0)) AND least(100000,page_limit)
 OR p_expires IS NULL OR p_expires<=clock_timestamp() OR p_expires>least(a.admitted_until,p.deadline,clock_timestamp()+interval '30 minutes')
 OR p_reason IS NULL OR length(p_reason) NOT BETWEEN 10 AND 1000
 OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.ref01_date_request_v4 x WHERE x.request_key=p_request AND x.manifest_sha256=encode(sha256(convert_to(x.manifest::text,'UTF8')),'hex') AND (x.manifest->>'max_pages')::integer>=p_page_ceiling)
 THEN RAISE EXCEPTION 'REF01_CENSUS_FINITE_TRANCHE_INVALID';END IF;
 INSERT INTO population_top3000_202608_202609_v1.ref01_date_admission_v4(request_key,owner_id,owner_fence,stop_generation,admitted_page_ceiling,max_provider_calls,expires_at)
 VALUES(p_request,p_owner,p_fence,p_stop,p_page_ceiling,p_call_ceiling,p_expires)
 ON CONFLICT(request_key) DO UPDATE SET owner_id=excluded.owner_id,owner_fence=excluded.owner_fence,stop_generation=excluded.stop_generation,admitted_page_ceiling=excluded.admitted_page_ceiling,max_provider_calls=excluded.max_provider_calls,expires_at=excluded.expires_at,updated_at=clock_timestamp();
 r:=jsonb_build_object('request_key',p_request,'owner_id',p_owner,'owner_fence',p_fence,'stop_generation',p_stop,'prior',to_jsonb(old),'admitted_page_ceiling',p_page_ceiling,'max_provider_calls',p_call_ceiling,'expires_at',p_expires,'reason',p_reason,'source_denominator_changed',false,'cumulative_counters_preserved',true);
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details)VALUES('REF01_CENSUS_TRANCHE_ADMITTED_V1',r);
 PERFORM population_top3000_202608_202609_v1.d02_daily_resource_guard_v2();
 RETURN r;
END;$function$

