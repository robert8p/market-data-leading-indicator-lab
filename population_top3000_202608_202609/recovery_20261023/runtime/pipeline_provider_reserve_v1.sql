CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.pipeline_provider_reserve_v1(p_attempt uuid, p_token text, p_provider text, p_max_bytes bigint, p_max_incremental_usd numeric)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'pg_catalog'
AS $function$
declare tid uuid;c population_top3000_202608_202609_v1.acquisition_control%rowtype;p population_top3000_202608_202609_v1.pipeline_control%rowtype;
begin
tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);
if p_provider is null or p_max_bytes is null or p_max_bytes<=0 or p_max_bytes>33554432 or p_max_incremental_usd is null or p_max_incremental_usd<0 then raise exception 'RESERVATION_INVALID';end if;
select * into c from population_top3000_202608_202609_v1.acquisition_control where singleton;
select * into p from population_top3000_202608_202609_v1.pipeline_control where singleton;
if exists(select 1 from population_top3000_202608_202609_v1.pipeline_provider_lease where attempt_id=p_attempt) then raise exception 'REQUEST_ALREADY_RESERVED_FOR_ATTEMPT';end if;
if c.next_provider_at>clock_timestamp() then return jsonb_build_object('state','WAIT','not_before',c.next_provider_at);end if;
if exists(select 1 from population_top3000_202608_202609_v1.pipeline_provider_lease where released_at is null)
 or exists(select 1 from population_top3000_202608_202609_v1.acquisition_job where status='LEASED' and lease_until>clock_timestamp()) then return jsonb_build_object('state','WAIT','not_before',clock_timestamp()+interval '5 seconds');end if;
if p.budget_verified_at is null or p.budget_evidence is null or coalesce((p.budget_evidence->>'provider_admissions_allowed')::boolean,false) is not true then return '{"state":"BUDGET_HOLD","reason":"CURRENT_MISSION_BILLING_BASELINE_REQUIRED"}';end if;
if c.provider_requests>=c.max_provider_requests or c.stored_bytes+c.reserved_bytes+p_max_bytes>c.max_source_bytes or p.actual_usd+p.accrued_usd+p.unavoidable_usd+p.remaining_uncommitted_usd+p.repair_egress_reserve_usd+p.reserved_request_usd+p_max_incremental_usd>250 then return '{"state":"BUDGET_HOLD","reason":"CONSERVATIVE_TOTAL_OR_RESOURCE_LIMIT"}';end if;
insert into population_top3000_202608_202609_v1.pipeline_provider_lease(attempt_id,provider,reserved_bytes,reserved_usd)values(p_attempt,p_provider,p_max_bytes,p_max_incremental_usd);
update population_top3000_202608_202609_v1.acquisition_control set provider_requests=provider_requests+1,reserved_bytes=reserved_bytes+p_max_bytes,next_provider_at=clock_timestamp()+interval '500 milliseconds' where singleton;
update population_top3000_202608_202609_v1.pipeline_control set reserved_request_usd=reserved_request_usd+p_max_incremental_usd where singleton;
return jsonb_build_object('state','RESERVED','max_bytes',p_max_bytes,'max_incremental_usd',p_max_incremental_usd);
end $function$
