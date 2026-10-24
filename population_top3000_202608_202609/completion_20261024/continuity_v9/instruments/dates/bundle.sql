CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.ref01_date_bundle_v4()
 RETURNS text
 LANGUAGE sql
 STABLE
 SET search_path TO 'pg_catalog'
AS $function$ SELECT encode(sha256(convert_to(string_agg(pg_get_functiondef(oid),E'\n' ORDER BY proname),'UTF8')),'hex') FROM pg_proc WHERE pronamespace='population_top3000_202608_202609_v1'::regnamespace AND proname IN('ref01_date_context_v4','ref01_date_claim_v4','ref01_date_commit_v4','ref01_date_failure_v4','ref01_date_acquire_v4','ref01_date_process_v4','ref01_date_admit_v4','ref01_date_continue_v9','ref01_date_continue_impl_v9','ref01_date_bundle_v4'); $function$

