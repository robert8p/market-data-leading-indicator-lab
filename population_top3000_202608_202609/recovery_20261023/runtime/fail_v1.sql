CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.fail_v1(p_attempt uuid, p_token text, p_error text, p_retry boolean, p_delay integer, p_evidence jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 SET search_path TO 'pg_catalog'
AS $function$
declare jid text;j population_top3000_202608_202609_v1.acquisition_job%rowtype;reserved bigint;n integer;retry boolean;
begin
-- Completed jobs cannot be overwritten by uncertain ACK/HTTP outcomes.
if exists(select 1 from population_top3000_202608_202609_v1.acquisition_attempt attempt_row join population_top3000_202608_202609_v1.acquisition_job job_row on job_row.job_id=attempt_row.job_id where attempt_row.attempt_id=p_attempt and attempt_row.token_sha256=encode(sha256(convert_to(p_token,'UTF8')),'hex') and job_row.status='COMPLETE') then return '{"state":"ALREADY_COMMITTED"}';end if;
jid:=population_top3000_202608_202609_v1.assert_attempt_v1(p_attempt,p_token);
select * into j from population_top3000_202608_202609_v1.acquisition_job where job_id=jid;
n:=case when j.last_error=p_error then j.consecutive_error_count+1 else 1 end;
retry:=p_retry and n<2 and j.attempts<j.max_attempts;
select reserved_bytes into reserved from population_top3000_202608_202609_v1.acquisition_attempt where attempt_id=p_attempt;
update population_top3000_202608_202609_v1.acquisition_control set reserved_bytes=greatest(0,reserved_bytes-reserved),hold_reason=case when p_error in('PROVIDER_HTTP_401','PROVIDER_HTTP_403','PROVIDER_CREDENTIAL_MISSING','DATABASE_AUTHENTICATION','DATABASE_PERMISSION') then p_error else hold_reason end,updated_at=clock_timestamp() where singleton;
if p_error='PROVIDER_HTTP_429' then update population_top3000_202608_202609_v1.acquisition_control set next_provider_at=greatest(next_provider_at,clock_timestamp()+make_interval(secs=>greatest(1,p_delay))) where singleton;end if;
update population_top3000_202608_202609_v1.acquisition_attempt set reserved_bytes=0,status='FAILED',result=jsonb_build_object('error',p_error,'retry',retry,'evidence',p_evidence) where attempt_id=p_attempt;
update population_top3000_202608_202609_v1.acquisition_job set status=case when retry then 'READY' else 'BLOCKED' end,last_error=p_error,consecutive_error_count=n,not_before=clock_timestamp()+make_interval(secs=>greatest(1,p_delay)),lease_until=null,updated_at=clock_timestamp() where job_id=jid;
insert into population_top3000_202608_202609_v1.acquisition_event(job_id,attempt_id,event,details)values(jid,p_attempt,'FAILED',jsonb_build_object('error',p_error,'retry',retry,'same_error_count',n,'evidence',p_evidence));
return jsonb_build_object('state',case when retry then 'RETRY_SCHEDULED' else 'BLOCKED' end,'error',p_error);
end $function$
