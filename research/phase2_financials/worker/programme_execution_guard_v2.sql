
create function research_eq10_fundamental_pit_20261020.programme_assert_job_v1(p_job text,p_token uuid,p_fence bigint,p_stop bigint)
returns boolean language plpgsql set search_path=pg_catalog as $f$
declare c research_eq10_fundamental_pit_20261020.programme_control_v1%rowtype;j research_eq10_fundamental_pit_20261020.financial_worker_jobs_v1%rowtype; stopped_now boolean;
begin
select stopped into stopped_now from research_eq10_fundamental_pit_20261020.financial_worker_control_v1 where lane='financials' for share;
select * into c from research_eq10_fundamental_pit_20261020.programme_control_v1 where singleton for share;
select * into j from research_eq10_fundamental_pit_20261020.financial_worker_jobs_v1 where job_id=p_job for update;
if stopped_now is distinct from false or j.job_id is null or j.status<>'LEASED' or j.lease_token is distinct from p_token or j.lease_expires_at<=clock_timestamp() then raise exception 'LEASE_LOST_OR_STOPPED' using errcode='P0001';end if;
if not research_eq10_fundamental_pit_20261020.programme_job_allowed_v1(p_job) then raise exception 'PROGRAMME_JOB_OUT_OF_SCOPE_OR_GATED' using errcode='P0001';end if;
if c.owner_fence is distinct from p_fence or c.stop_generation is distinct from p_stop or (j.job_payload->>'programme_owner_fence')::bigint is distinct from p_fence or (j.job_payload->>'programme_stop_generation')::bigint is distinct from p_stop then raise exception 'PROGRAMME_STALE_FENCE' using errcode='P0001';end if;
return true;
end $f$;
revoke all on function research_eq10_fundamental_pit_20261020.programme_assert_job_v1(text,uuid,bigint,bigint) from public,anon,authenticated;
create function research_eq10_fundamental_pit_20261020.programme_provider_request_reserve_v1(p_job text,p_token uuid,p_fence bigint,p_stop bigint,p_max_bytes bigint)
returns boolean language plpgsql set search_path=pg_catalog as $f$
declare c research_eq10_fundamental_pit_20261020.programme_control_v1%rowtype;
begin
perform research_eq10_fundamental_pit_20261020.programme_assert_job_v1(p_job,p_token,p_fence,p_stop);
select * into c from research_eq10_fundamental_pit_20261020.programme_control_v1 where singleton for update;
if p_max_bytes<=0 or coalesce((c.resources->>'new_provider_requests_used')::bigint,0)+1>coalesce((c.resources->>'new_provider_requests_limit')::bigint,0) or coalesce((c.resources->>'new_source_bytes_reserved')::bigint,0)+p_max_bytes>coalesce((c.resources->>'new_source_bytes_limit')::bigint,0) then raise exception 'PROGRAMME_RESOURCE_LIMIT' using errcode='P0001';end if;
update research_eq10_fundamental_pit_20261020.programme_control_v1 set resources=resources||jsonb_build_object('new_provider_requests_used',coalesce((resources->>'new_provider_requests_used')::bigint,0)+1,'new_source_bytes_reserved',coalesce((resources->>'new_source_bytes_reserved')::bigint,0)+p_max_bytes),updated_at=clock_timestamp() where singleton;
return true;
end $f$;
revoke all on function research_eq10_fundamental_pit_20261020.programme_provider_request_reserve_v1(text,uuid,bigint,bigint,bigint) from public,anon,authenticated;

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
perform 1 from research_eq10_fundamental_pit_20261020.programme_control_v1 where singleton for share;
if not exists(select 1 from research_eq10_fundamental_pit_20261020.programme_control_v1 where singleton and desired_run and observed_state not in ('STOPPED_USER','COMPLETE')) then return jsonb_build_object('state','PROGRAMME_GATED'); end if;
if exists(select 1 from research_eq10_fundamental_pit_20261020.financial_worker_jobs_v1 where status='LEASED' and lease_expires_at>clock_timestamp()) then return jsonb_build_object('state','LEASE_ACTIVE');end if;
update research_eq10_fundamental_pit_20261020.financial_worker_jobs_v1 set status='BLOCKED',last_error_class='RETRY_EXHAUSTED',updated_at=clock_timestamp() where research_eq10_fundamental_pit_20261020.programme_job_allowed_v1(job_id) and status in('RETRY','LEASED') and attempts>=max_attempts and coalesce(lease_expires_at,clock_timestamp())<=clock_timestamp();
select * into j from research_eq10_fundamental_pit_20261020.financial_worker_jobs_v1 where
(status in('PENDING','RETRY') or(status='LEASED' and lease_expires_at<=clock_timestamp())) and attempts<max_attempts and next_attempt_at<=clock_timestamp()
and research_eq10_fundamental_pit_20261020.programme_job_allowed_v1(job_id)
and (p_allow_page or job_kind<>'METADATA_PAGE') and(acceptance_job or(ctl.mode='BULK' and ctl.acceptance_passed and ctl.bulk_enabled))
order by acceptance_job desc,case when ctl.claim_sequence%5=4 and job_kind='STATE_BUILD' then 1 else 0 end desc,priority desc,created_at,job_id for update skip locked limit 1;
if not found then return jsonb_build_object('state','IDLE');end if;
expired:=j.status='LEASED';
update research_eq10_fundamental_pit_20261020.financial_worker_control_v1 set claim_sequence=claim_sequence+1 where lane='financials';
update research_eq10_fundamental_pit_20261020.financial_worker_jobs_v1 set status='LEASED',attempts=attempts+1,lease_token=gen_random_uuid(),lease_owner=p_execution,job_payload=job_payload||jsonb_build_object('programme_owner_fence',(select owner_fence from research_eq10_fundamental_pit_20261020.programme_control_v1 where singleton),'programme_stop_generation',(select stop_generation from research_eq10_fundamental_pit_20261020.programme_control_v1 where singleton)),lease_expires_at=clock_timestamp()+interval '90 seconds',updated_at=clock_timestamp() where job_id=j.job_id returning * into j;
insert into research_eq10_fundamental_pit_20261020.financial_worker_events_v1(job_id,event,execution_id,lease_token,details)values(j.job_id,case when expired then 'EXPIRED_LEASE_RECOVERED' else 'CLAIMED' end,p_execution,j.lease_token,jsonb_build_object('attempt',j.attempts,'checkpoint',j.checkpoint,'kind',j.job_kind,'version',p_version));
return jsonb_build_object('state','CLAIMED','job',to_jsonb(j),'max_units',ctl.max_units_per_request);
end $function$
