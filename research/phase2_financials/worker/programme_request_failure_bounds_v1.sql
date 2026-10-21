CREATE OR REPLACE FUNCTION research_eq10_fundamental_pit_20261020.financial_worker_dispatch_tick_v1()
 RETURNS bigint
 LANGUAGE plpgsql
 SET search_path TO 'pg_catalog'
AS $function$
declare rid bigint;t text;c research_eq10_fundamental_pit_20261020.programme_control_v1%rowtype; has_scope boolean; failures int; failure_ids jsonb; failure_key text;
begin
if not pg_try_advisory_xact_lock(2102026104) then return null;end if;
update research_eq10_fundamental_pit_20261020.financial_worker_dispatch_v1 d set response_status=r.status_code,response_summary=jsonb_build_object('status_code',r.status_code,'content',r.content,'timed_out',r.timed_out,'error_msg',r.error_msg)
from net._http_response r where r.id=d.request_id and d.response_summary is null;
perform 1 from research_eq10_fundamental_pit_20261020.financial_worker_control_v1 where lane='financials' for update;
select * into c from research_eq10_fundamental_pit_20261020.programme_control_v1 where singleton for update;
if not c.desired_run or c.observed_state in('STOPPED_USER','COMPLETE') or exists(select 1 from research_eq10_fundamental_pit_20261020.financial_worker_control_v1 where lane='financials' and stopped) then return null;end if;
if c.stage not in('A','B') then return null;end if;
perform research_eq10_fundamental_pit_20261020.programme_maintain_prep_v1();
select * into c from research_eq10_fundamental_pit_20261020.programme_control_v1 where singleton;
if not c.desired_run then return null;end if;
with recent as(
select d.request_id,d.response_status,d.response_summary,research_eq10_fundamental_pit_20261020.programme_response_json_v1(d.response_summary->>'content') body
from research_eq10_fundamental_pit_20261020.financial_worker_dispatch_v1 d where d.request_id>coalesce((c.checkpoint->>'prep_dispatch_after')::bigint,0) and d.response_summary is not null order by d.request_id desc limit 2
), classified as(select *,coalesce(body->>'error_class',case when response_status in(401,403) then 'AUTHENTICATION' when (response_summary->>'timed_out')::boolean then 'UNCERTAIN_TRANSPORT_TIMEOUT' else 'HTTP_'||coalesce(response_status::text,'UNKNOWN') end) k from recent where body->>'state'='REQUEST_FAILED' or (response_status>=400 and coalesce(body->>'state','')<>'INTERRUPTED_FOR_RECOVERY_TEST') or (response_status is null and response_summary->>'error_msg' is not null))
select count(*),jsonb_agg(request_id),min(k) into failures,failure_ids,failure_key from classified having count(distinct k)=1;
if failures=2 then
 insert into research_eq10_fundamental_pit_20261020.execution_evidence_v4(evidence_id,evidence) values('programme_request_failure_hold_'||to_char(clock_timestamp(),'YYYYMMDDHH24MISSUS'),jsonb_build_object('request_ids',failure_ids,'class',failure_key,'recovery','Two matching request-level failures; reconcile uncertain commits and change approach before retry','automatic_stop_override',false));
 update research_eq10_fundamental_pit_20261020.programme_control_v1 set desired_run=false,observed_state='BLOCKED_EXECUTOR_REQUEST_FAILURE',next_action='Diagnose two matching request-level failures; preserve checkpoints, reconcile outcome, and materially change failed approach.',checkpoint=checkpoint||jsonb_build_object('recovery_state','BOUNDED_REQUEST_RECOVERY_HOLD','request_failure_class',failure_key),updated_at=clock_timestamp() where singleton;
 update research_eq10_fundamental_pit_20261020.financial_worker_control_v1 set stopped=true,stop_reason='PROGRAMME_BOUNDED_REQUEST_FAILURE_HOLD',updated_at=clock_timestamp() where lane='financials';
 perform cron.alter_job((select jobid from cron.job where jobname='phase2-financial-worker-v1'),active:=false);
 return null;
end if;

if not coalesce((c.gates->>'retained_prep_resources')::boolean,false) or (c.resources->>'prep_valid_until')::timestamptz<=clock_timestamp()
or (c.resources->>'prep_dispatch_used')::int>=(c.resources->>'prep_dispatch_limit')::int
or (c.resources->>'prep_result_bytes_used')::bigint>=(c.resources->>'prep_result_bytes_limit')::bigint then
 update research_eq10_fundamental_pit_20261020.programme_control_v1 set desired_run=false,observed_state='BLOCKED_RESOURCE_RECHECK',next_action='Recheck actual allowances before further bounded admission.',updated_at=clock_timestamp() where singleton;
 update research_eq10_fundamental_pit_20261020.financial_worker_control_v1 set stopped=true,stop_reason='PROGRAMME_RESOURCE_CAP_OR_EXPIRY',updated_at=clock_timestamp() where lane='financials';
 perform cron.alter_job((select jobid from cron.job where jobname='phase2-financial-worker-v1'),active:=false);
 return null;
end if;
perform research_eq10_fundamental_pit_20261020.financial_worker_supervise_v3();
if exists(select 1 from pg_stat_activity where pid<>pg_backend_pid() and state='active' and backend_type='client backend' and clock_timestamp()-query_start>interval '2 seconds') then return null;end if;
select exists(select 1 from research_eq10_fundamental_pit_20261020.financial_worker_jobs_v1 j where research_eq10_fundamental_pit_20261020.programme_job_allowed_v1(j.job_id) and j.status in('PENDING','RETRY','LEASED')) into has_scope;
if not has_scope then
 update research_eq10_fundamental_pit_20261020.programme_control_v1 set observed_state='BLOCKED_SCOPED_ITEMS',next_action='Diagnose blocked scoped jobs; bounded recovery exhausted. Do not admit old backlog.',updated_at=clock_timestamp() where singleton;
 return null;
end if;
if exists(select 1 from research_eq10_fundamental_pit_20261020.financial_worker_jobs_v1 where status='LEASED' and lease_expires_at>clock_timestamp()) then return null;end if;
if not exists(select 1 from research_eq10_fundamental_pit_20261020.financial_worker_jobs_v1 j where research_eq10_fundamental_pit_20261020.programme_job_allowed_v1(j.job_id) and (j.status in('PENDING','RETRY') or(j.status='LEASED' and j.lease_expires_at<=clock_timestamp())) and j.next_attempt_at<=clock_timestamp()) then return null;end if;
update research_eq10_fundamental_pit_20261020.programme_control_v1 set resources=jsonb_set(resources,'{prep_dispatch_used}',to_jsonb((resources->>'prep_dispatch_used')::int+1)),updated_at=clock_timestamp() where singleton;
select decrypted_secret into t from vault.decrypted_secrets where name='phase2_financial_worker_token_v1';
rid:=net.http_post(url:='https://oxzabweahkoimtevbbny.supabase.co/functions/v1/phase2-financial-worker-v1',headers:=jsonb_build_object('Content-Type','application/json','Authorization','Bearer '||t),body:='{"scheduler":"pg_cron_phase2_v1"}',timeout_milliseconds:=55000);
insert into research_eq10_fundamental_pit_20261020.financial_worker_dispatch_v1(request_id,scheduler_job)values(rid,'phase2-financial-worker-v1');
return rid;
end $function$
