CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.record_verification_v1()
 RETURNS jsonb
 LANGUAGE plpgsql
 SET search_path TO 'pg_catalog'
AS $function$
declare r jsonb;v uuid;
begin r:=population_top3000_202608_202609_v1.verify_census_v1();insert into population_top3000_202608_202609_v1.acquisition_verification(verifier_version,result)values('raw-page-independent-v1',r)returning verification_id into v;return jsonb_build_object('verification_id',v,'result',r);end $function$
