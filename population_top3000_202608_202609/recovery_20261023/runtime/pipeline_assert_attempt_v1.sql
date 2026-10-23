CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt uuid, p_token text)
 RETURNS uuid
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'pg_catalog'
AS $function$
declare c population_top3000_202608_202609_v1.acquisition_control%rowtype;
p population_top3000_202608_202609_v1.pipeline_control%rowtype;
a population_top3000_202608_202609_v1.pipeline_attempt%rowtype;
t population_top3000_202608_202609_v1.pipeline_task%rowtype;
b population_top3000_202608_202609_v1.pipeline_adapter%rowtype;
bound_hash text;
begin
select * into c from population_top3000_202608_202609_v1.acquisition_control where singleton for update;
if not found then raise exception 'CONTROL_MISSING';end if;
select * into p from population_top3000_202608_202609_v1.pipeline_control where singleton for update;
if not found then raise exception 'PIPELINE_CONTROL_MISSING';end if;
if c.owner_id is distinct from p.expected_owner_id or c.owner_fence is distinct from p.expected_owner_fence or c.stop_generation is distinct from p.expected_stop_generation then raise exception 'CONTROLLER_OWNERSHIP_CHANGED';end if;
if c.scope_sha256 is distinct from '8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd' or p.scope_sha256 is distinct from c.scope_sha256 then raise exception 'SCOPE_MISMATCH';end if;
if not c.enabled or c.stopped or c.hold_reason is not null or c.admitted_until<=clock_timestamp() or not p.enabled or not p.shared_scheduler_verified or (p.deadline is not null and p.deadline<=clock_timestamp()) then raise exception 'MISSION_HELD_OR_ADMISSION_EXPIRED';end if;
select * into a from population_top3000_202608_202609_v1.pipeline_attempt where attempt_id=p_attempt for update;
if not found or p_token is null or a.token_sha256 is distinct from encode(sha256(convert_to(p_token,'UTF8')),'hex') then raise exception 'ATTEMPT_AUTH_FAILED';end if;
select * into t from population_top3000_202608_202609_v1.pipeline_task where task_id=a.task_id for update;
if t.status is distinct from 'LEASED' or t.attempt_id is distinct from p_attempt or t.lease_until is null or t.lease_until<=clock_timestamp() then raise exception 'LEASE_LOST';end if;
if a.owner_fence is distinct from c.owner_fence or a.stop_generation is distinct from c.stop_generation or t.owner_fence is distinct from c.owner_fence or t.stop_generation is distinct from c.stop_generation then raise exception 'STALE_OWNER_OR_STOP';end if;
select * into b from population_top3000_202608_202609_v1.pipeline_adapter where family_id=t.family_id and stage=t.stage;
if not found or not b.enabled then raise exception 'ADAPTER_MISSING_OR_DISABLED';end if;
select encode(sha256(convert_to(pg_get_functiondef(b.function_oid),'UTF8')),'hex') into bound_hash;
if bound_hash is distinct from b.function_sha256 or a.adapter_sha256 is distinct from b.function_sha256 then raise exception 'ADAPTER_CODE_CHANGED';end if;
return t.task_id;
end $function$
