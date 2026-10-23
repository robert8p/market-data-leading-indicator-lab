CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.assert_attempt_v1(p_attempt uuid, p_token text)
 RETURNS text
 LANGUAGE plpgsql
 SET search_path TO 'pg_catalog'
AS $function$
declare c population_top3000_202608_202609_v1.acquisition_control%rowtype;a population_top3000_202608_202609_v1.acquisition_attempt%rowtype;j population_top3000_202608_202609_v1.acquisition_job%rowtype;
begin
select * into c from population_top3000_202608_202609_v1.acquisition_control where singleton for update;
if not found then raise exception 'CONTROL_MISSING';end if;
select * into a from population_top3000_202608_202609_v1.acquisition_attempt where attempt_id=p_attempt for update;
if a.attempt_id is null or a.token_sha256 is distinct from encode(sha256(convert_to(p_token,'UTF8')),'hex') then raise exception 'AUTHENTICATION_FAILED';end if;
select * into j from population_top3000_202608_202609_v1.acquisition_job where job_id=a.job_id for update;
if not c.enabled or c.stopped or c.hold_reason is not null or c.admitted_until<=clock_timestamp() then raise exception 'MISSION_STOPPED_OR_HELD';end if;
if j.status is distinct from 'LEASED' or j.attempt_id is distinct from p_attempt or j.lease_until is null or j.lease_until<=clock_timestamp() then raise exception 'LEASE_LOST';end if;
if a.owner_fence is distinct from c.owner_fence or a.stop_generation is distinct from c.stop_generation or j.owner_fence is distinct from c.owner_fence or j.stop_generation is distinct from c.stop_generation then raise exception 'STALE_FENCE';end if;
return j.job_id;
end $function$
