CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.pipeline_provider_release_v1(p_attempt uuid, p_token text, p_received_bytes bigint, p_actual_usd numeric, p_retry_after_seconds integer DEFAULT 0)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'pg_catalog'
AS $function$
declare tid uuid;l population_top3000_202608_202609_v1.pipeline_provider_lease%rowtype;
begin
tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);
select * into l from population_top3000_202608_202609_v1.pipeline_provider_lease where attempt_id=p_attempt for update;
if not found then raise exception 'RESERVATION_MISSING';end if;
if l.released_at is not null then return '{"state":"ALREADY_RELEASED"}';end if;
if p_received_bytes is null or p_received_bytes<0 or p_received_bytes>l.reserved_bytes or p_actual_usd is null or p_actual_usd<0 or p_actual_usd>l.reserved_usd or p_retry_after_seconds is null or p_retry_after_seconds<0 then raise exception 'ACTUAL_EXCEEDS_ENFORCEABLE_RESERVATION';end if;
update population_top3000_202608_202609_v1.acquisition_control set reserved_bytes=reserved_bytes-l.reserved_bytes,stored_bytes=stored_bytes+p_received_bytes,next_provider_at=greatest(next_provider_at,clock_timestamp()+make_interval(secs=>p_retry_after_seconds)) where singleton;
update population_top3000_202608_202609_v1.pipeline_control set reserved_request_usd=reserved_request_usd-l.reserved_usd,accrued_usd=accrued_usd+p_actual_usd where singleton;
update population_top3000_202608_202609_v1.pipeline_provider_lease set released_at=clock_timestamp(),received_bytes=p_received_bytes,actual_usd=p_actual_usd,outcome=case when p_retry_after_seconds>0 then 'PROVIDER_WAIT' else 'RECEIVED' end where attempt_id=p_attempt;
return '{"state":"RELEASED"}';
end $function$
