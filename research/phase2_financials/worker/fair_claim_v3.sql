alter table research_eq10_fundamental_pit_20261020.financial_worker_control_v1 add column if not exists claim_sequence bigint not null default 0;
CREATE OR REPLACE FUNCTION research_eq10_fundamental_pit_20261020.financial_worker_claim_v2(p_execution text, p_version text, p_allow_page boolean)
 RETURNS jsonb
 LANGUAGE plpgsql
 SET search_path TO 'pg_catalog', 'research_eq10_fundamental_pit_20261020'
AS $function$
declare ctl research_eq10_fundamental_pit_20261020.financial_worker_control_v1%rowtype;j research_eq10_fundamental_pit_20261020.financial_worker_jobs_v1%rowtype;expired boolean;
begin
if not pg_try_advisory_xact_lock(2102026102) then return jsonb_build_object('state','BUSY');end if;
select * into ctl from research_eq10_fundamental_pit_20261020.financial_worker_control_v1 where lane='financials' for update;
update research_eq10_fundamental_pit_20261020.financial_worker_control_v1 set last_tick_at=clock_timestamp(),last_execution_id=p_execution,code_version=p_version where lane='financials';
if ctl.stopped then return jsonb_build_object('state','STOPPED');end if;
if exists(select 1 from research_eq10_fundamental_pit_20261020.financial_worker_jobs_v1 where status='LEASED' and lease_expires_at>clock_timestamp()) then return jsonb_build_object('state','LEASE_ACTIVE');end if;
update research_eq10_fundamental_pit_20261020.financial_worker_jobs_v1 set status='BLOCKED',last_error_class='RETRY_EXHAUSTED',updated_at=clock_timestamp() where status in('RETRY','LEASED') and attempts>=max_attempts and coalesce(lease_expires_at,clock_timestamp())<=clock_timestamp();
select * into j from research_eq10_fundamental_pit_20261020.financial_worker_jobs_v1 where
(status in('PENDING','RETRY') or(status='LEASED' and lease_expires_at<=clock_timestamp())) and attempts<max_attempts and next_attempt_at<=clock_timestamp()
and (p_allow_page or job_kind<>'METADATA_PAGE') and(acceptance_job or(ctl.mode='BULK' and ctl.acceptance_passed and ctl.bulk_enabled))
order by acceptance_job desc,case when ctl.claim_sequence%5=4 and job_kind='STATE_BUILD' then 1 else 0 end desc,priority desc,created_at,job_id for update skip locked limit 1;
if not found then return jsonb_build_object('state','IDLE');end if;
expired:=j.status='LEASED';
update research_eq10_fundamental_pit_20261020.financial_worker_control_v1 set claim_sequence=claim_sequence+1 where lane='financials';
update research_eq10_fundamental_pit_20261020.financial_worker_jobs_v1 set status='LEASED',attempts=attempts+1,lease_token=gen_random_uuid(),lease_owner=p_execution,lease_expires_at=clock_timestamp()+interval '90 seconds',updated_at=clock_timestamp() where job_id=j.job_id returning * into j;
insert into research_eq10_fundamental_pit_20261020.financial_worker_events_v1(job_id,event,execution_id,lease_token,details)values(j.job_id,case when expired then 'EXPIRED_LEASE_RECOVERED' else 'CLAIMED' end,p_execution,j.lease_token,jsonb_build_object('attempt',j.attempts,'checkpoint',j.checkpoint,'kind',j.job_kind,'version',p_version));
return jsonb_build_object('state','CLAIMED','job',to_jsonb(j),'max_units',ctl.max_units_per_request);
end $function$
;