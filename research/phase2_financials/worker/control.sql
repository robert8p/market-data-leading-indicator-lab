
create table research_eq10_fundamental_pit_20261020.financial_worker_control_v1(
 lane text primary key check(lane='financials'), stopped boolean not null default true,
 mode text not null default 'ACCEPTANCE_ONLY' check(mode in('ACCEPTANCE_ONLY','BULK')),
 acceptance_passed boolean not null default false, bulk_enabled boolean not null default false,
 last_tick_at timestamptz,last_execution_id text,code_version text,stop_reason text,
 updated_at timestamptz not null default clock_timestamp());
insert into research_eq10_fundamental_pit_20261020.financial_worker_control_v1(lane) values('financials');
create table research_eq10_fundamental_pit_20261020.financial_worker_jobs_v1(
 job_id text primary key,accession_number text not null, file_id text not null,
 acceptance_job boolean not null default false,status text not null default 'PENDING'
 check(status in('PENDING','LEASED','RETRY','COMPLETE','BLOCKED','EXCLUDED')),
 checkpoint text not null default 'PENDING', attempts integer not null default 0,max_attempts integer not null default 5,
 lease_token uuid,lease_owner text,lease_expires_at timestamptz,next_attempt_at timestamptz not null default now(),
 interrupt_after_document boolean not null default false,interruption_done boolean not null default false,
 last_error_class text,last_error text,retrieval_count integer not null default 0,
 result jsonb,created_at timestamptz not null default now(),updated_at timestamptz not null default now(),completed_at timestamptz,
 unique(accession_number,file_id));
create table research_eq10_fundamental_pit_20261020.financial_worker_events_v1(
 event_id bigint generated always as identity primary key,job_id text,event text not null,
 execution_id text,lease_token uuid,details jsonb not null default '{}',occurred_at timestamptz not null default clock_timestamp());
create table research_eq10_fundamental_pit_20261020.financial_worker_dispatch_v1(
 request_id bigint primary key,dispatched_at timestamptz not null default clock_timestamp(),scheduler_job text not null,
 response_status int,response_summary jsonb);
create index financial_worker_jobs_v1_ready on research_eq10_fundamental_pit_20261020.financial_worker_jobs_v1(status,next_attempt_at);
create function research_eq10_fundamental_pit_20261020.financial_worker_claim_v1(p_execution text,p_version text)
returns jsonb language plpgsql security invoker set search_path=pg_catalog,research_eq10_fundamental_pit_20261020 as $fn$
declare ctl research_eq10_fundamental_pit_20261020.financial_worker_control_v1%rowtype;
 j research_eq10_fundamental_pit_20261020.financial_worker_jobs_v1%rowtype; was_expired boolean;
begin
if not pg_try_advisory_xact_lock(2102026102) then return jsonb_build_object('state','BUSY');end if;
select * into ctl from research_eq10_fundamental_pit_20261020.financial_worker_control_v1 where lane='financials' for update;
update research_eq10_fundamental_pit_20261020.financial_worker_control_v1 set last_tick_at=clock_timestamp(),last_execution_id=p_execution,code_version=p_version where lane='financials';
if ctl.stopped then return jsonb_build_object('state','STOPPED');end if;
if exists(select 1 from research_eq10_fundamental_pit_20261020.financial_worker_jobs_v1 where status='LEASED' and lease_expires_at>clock_timestamp()) then return jsonb_build_object('state','LEASE_ACTIVE');end if;
update research_eq10_fundamental_pit_20261020.financial_worker_jobs_v1 set status='BLOCKED',last_error_class='RETRY_EXHAUSTED',updated_at=clock_timestamp() where status in('RETRY','LEASED') and attempts>=max_attempts and coalesce(lease_expires_at,clock_timestamp())<=clock_timestamp();
select * into j from research_eq10_fundamental_pit_20261020.financial_worker_jobs_v1
 where (status in('PENDING','RETRY') or (status='LEASED' and lease_expires_at<=clock_timestamp()))
 and attempts<max_attempts and next_attempt_at<=clock_timestamp()
 and (acceptance_job or (ctl.mode='BULK' and ctl.acceptance_passed and ctl.bulk_enabled))
 order by acceptance_job desc,created_at,job_id for update skip locked limit 1;
if not found then return jsonb_build_object('state','IDLE');end if;
was_expired=j.status='LEASED';
update research_eq10_fundamental_pit_20261020.financial_worker_jobs_v1 set
 status='LEASED',attempts=attempts+1,lease_token=gen_random_uuid(),lease_owner=p_execution,
 lease_expires_at=clock_timestamp()+interval '90 seconds',updated_at=clock_timestamp()
 where job_id=j.job_id returning * into j;
insert into research_eq10_fundamental_pit_20261020.financial_worker_events_v1(job_id,event,execution_id,lease_token,details)
values(j.job_id,case when was_expired then 'EXPIRED_LEASE_RECOVERED' else 'CLAIMED' end,p_execution,j.lease_token,jsonb_build_object('attempt',j.attempts,'checkpoint',j.checkpoint,'code_version',p_version));
return jsonb_build_object('state','CLAIMED','job',to_jsonb(j));
end $fn$;
create function research_eq10_fundamental_pit_20261020.financial_worker_dispatch_tick_v1()
returns bigint language plpgsql security invoker set search_path=pg_catalog,research_eq10_fundamental_pit_20261020 as $fn$
declare rid bigint; t text;
begin
if not exists(select 1 from research_eq10_fundamental_pit_20261020.financial_worker_control_v1 where lane='financials' and not stopped) then return null;end if;
select decrypted_secret into t from vault.decrypted_secrets where name='phase2_financial_worker_token_v1';
rid:=net.http_post(url:='https://oxzabweahkoimtevbbny.supabase.co/functions/v1/phase2-financial-worker-v1',headers:=jsonb_build_object('Content-Type','application/json','Authorization','Bearer '||t),body:='{"scheduler":"pg_cron_phase2_v1"}',timeout_milliseconds:=55000);
insert into research_eq10_fundamental_pit_20261020.financial_worker_dispatch_v1(request_id,scheduler_job)values(rid,'phase2-financial-worker-v1');
return rid;
end $fn$;
alter table research_eq10_fundamental_pit_20261020.financial_worker_control_v1 enable row level security;
alter table research_eq10_fundamental_pit_20261020.financial_worker_jobs_v1 enable row level security;
alter table research_eq10_fundamental_pit_20261020.financial_worker_events_v1 enable row level security;
alter table research_eq10_fundamental_pit_20261020.financial_worker_dispatch_v1 enable row level security;
revoke all on research_eq10_fundamental_pit_20261020.financial_worker_control_v1,research_eq10_fundamental_pit_20261020.financial_worker_jobs_v1,research_eq10_fundamental_pit_20261020.financial_worker_events_v1,research_eq10_fundamental_pit_20261020.financial_worker_dispatch_v1 from public,anon,authenticated;
revoke all on function research_eq10_fundamental_pit_20261020.financial_worker_claim_v1(text,text),research_eq10_fundamental_pit_20261020.financial_worker_dispatch_tick_v1() from public,anon,authenticated;
