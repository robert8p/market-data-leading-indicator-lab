CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.dispatch_v1()
 RETURNS jsonb
 LANGUAGE plpgsql
 SET search_path TO 'pg_catalog'
AS $function$
declare c population_top3000_202608_202609_v1.acquisition_control%rowtype;
j population_top3000_202608_202609_v1.acquisition_job%rowtype;a uuid;token text;rid bigint;recovered bigint;
begin
if not pg_try_advisory_xact_lock(2026083000261022) then return '{"state":"BUSY"}';end if;
select * into c from population_top3000_202608_202609_v1.acquisition_control where singleton for update;
if not found then return '{"state":"CONTROL_MISSING"}';end if;
if not c.enabled or c.stopped or c.hold_reason is not null then return jsonb_build_object('state','HOLD','reason',c.hold_reason);end if;
if c.admitted_until<=clock_timestamp() then return '{"state":"ADMISSION_EXPIRED"}';end if;
if c.dispatch_calls>=c.max_dispatch_calls or c.http_invocations>=c.max_http_invocations then return '{"state":"INVOCATION_LIMIT"}';end if;
update population_top3000_202608_202609_v1.acquisition_control set dispatch_calls=dispatch_calls+1 where singleton;
-- Reconcile only expired attempts. Retained raw outputs are never deleted or redownloaded.
with expired as (
 update population_top3000_202608_202609_v1.acquisition_attempt attempt_row set status='EXPIRED'
 from population_top3000_202608_202609_v1.acquisition_job job_row
 where job_row.status='LEASED' and job_row.lease_until<=clock_timestamp() and attempt_row.attempt_id=job_row.attempt_id and attempt_row.status in('DISPATCHED','CLAIMED','SOURCE_SAVED')
 returning attempt_row.reserved_bytes
) select coalesce(sum(reserved_bytes),0) into recovered from expired;
update population_top3000_202608_202609_v1.acquisition_control set reserved_bytes=greatest(0,reserved_bytes-recovered) where singleton;
insert into population_top3000_202608_202609_v1.acquisition_event(job_id,attempt_id,event,details)
select job_id,attempt_id,'LEASE_EXPIRED',jsonb_build_object('checkpoint',checkpoint,'attempts',attempts) from population_top3000_202608_202609_v1.acquisition_job where status='LEASED' and lease_until<=clock_timestamp();
update population_top3000_202608_202609_v1.acquisition_job set status=case when attempts<max_attempts and (case when last_error='LEASE_EXPIRED' then consecutive_error_count+1 else 1 end)<2 then 'READY' else 'BLOCKED' end,consecutive_error_count=case when last_error='LEASE_EXPIRED' then consecutive_error_count+1 else 1 end,last_error='LEASE_EXPIRED',lease_until=null,updated_at=clock_timestamp() where status='LEASED' and lease_until<=clock_timestamp();
if (select count(*) from population_top3000_202608_202609_v1.acquisition_job where status='LEASED' and lease_until>clock_timestamp())>=2 then return '{"state":"CONCURRENCY_LIMIT"}';end if;
select * into j from population_top3000_202608_202609_v1.acquisition_job
where status='READY' and attempts<max_attempts and not_before<=clock_timestamp()
and (c.next_provider_at<=clock_timestamp() or exists(select 1 from population_top3000_202608_202609_v1.acquisition_raw cached where cached.job_id=acquisition_job.job_id))
order by case when kind='CENSUS' then 0 else 1 end,page_number nulls last,job_id for update skip locked limit 1;
if not found then
 if exists(select 1 from population_top3000_202608_202609_v1.acquisition_job where status in('READY','LEASED')) then return '{"state":"BOUNDED_WAIT_FOR_READY_JOB"}';end if;
 update population_top3000_202608_202609_v1.acquisition_control set next_stage='BLOCKED_COMPANY_RECONCILIATION',next_action='Controller must reconcile company identities, class/ADR cap semantics, independent sources and cutoff; retain all blocked source jobs. Acquisition idle is not mission completion.' where singleton;
 return '{"state":"BLOCKED_COMPANY_RECONCILIATION"}';
end if;
a:=gen_random_uuid();token:=replace(gen_random_uuid()::text,'-','')||replace(gen_random_uuid()::text,'-','');
update population_top3000_202608_202609_v1.acquisition_job set status='LEASED',attempts=attempts+1,attempt_id=a,lease_until=clock_timestamp()+interval '90 seconds',owner_fence=c.owner_fence,stop_generation=c.stop_generation,updated_at=clock_timestamp() where job_id=j.job_id;
insert into population_top3000_202608_202609_v1.acquisition_attempt(attempt_id,job_id,attempt_number,token_sha256,owner_fence,stop_generation)
values(a,j.job_id,j.attempts+1,encode(sha256(convert_to(token,'UTF8')),'hex'),c.owner_fence,c.stop_generation);
select net.http_post(url:='https://oxzabweahkoimtevbbny.supabase.co/functions/v1/population-top3000-source-v1',headers:='{"Content-Type":"application/json"}'::jsonb,body:=jsonb_build_object('attempt_id',a,'token',token),timeout_milliseconds:=55000) into rid;
update population_top3000_202608_202609_v1.acquisition_attempt set request_id=rid where attempt_id=a;
update population_top3000_202608_202609_v1.acquisition_control set http_invocations=http_invocations+1 where singleton;
insert into population_top3000_202608_202609_v1.acquisition_event(job_id,attempt_id,event,details) values(j.job_id,a,'DISPATCHED',jsonb_build_object('request_id',rid,'fence',c.owner_fence,'stop_generation',c.stop_generation));
return jsonb_build_object('state','DISPATCHED','job_id',j.job_id,'attempt_id',a,'request_id',rid);
end $function$
