create table if not exists research_eq10_fundamental_pit_20261020.financial_worker_supervision_v3(
 lane text primary key check(lane='financials'),
 observed_progress_at timestamptz,
 progress_anchor_at timestamptz not null default clock_timestamp(),
 ticks_without_progress integer not null default 0,
 diagnostic_count integer not null default 0,
 last_checked_at timestamptz,
 last_diagnostic_at timestamptz,
 state text not null default 'INITIALIZED',
 details jsonb not null default '{}'::jsonb
);
alter table research_eq10_fundamental_pit_20261020.financial_worker_supervision_v3 enable row level security;
revoke all on research_eq10_fundamental_pit_20261020.financial_worker_supervision_v3 from public,anon,authenticated;

create or replace function research_eq10_fundamental_pit_20261020.financial_worker_supervise_v3()
returns jsonb language plpgsql security invoker
set search_path=pg_catalog,research_eq10_fundamental_pit_20261020 as $fn$
declare c research_eq10_fundamental_pit_20261020.financial_worker_control_v1%rowtype;
 s research_eq10_fundamental_pit_20261020.financial_worker_supervision_v3%rowtype;
 t timestamptz:=clock_timestamp(); due_jobs int; active_jobs int; expired_jobs int; retry_at timestamptz; state_name text; detail jsonb;
begin
 if not pg_try_advisory_xact_lock(2102026103) then return '{"state":"SUPERVISOR_BUSY"}'; end if;
 select * into c from research_eq10_fundamental_pit_20261020.financial_worker_control_v1 where lane='financials';
 if c.stopped then return jsonb_build_object('state','STOPPED','reason',c.stop_reason); end if;
 insert into research_eq10_fundamental_pit_20261020.financial_worker_supervision_v3(lane,observed_progress_at,progress_anchor_at)
 values('financials',c.last_useful_progress_at,greatest(c.updated_at,c.last_useful_progress_at,t)) on conflict do nothing;
 select * into s from research_eq10_fundamental_pit_20261020.financial_worker_supervision_v3 where lane='financials' for update;
 if c.last_useful_progress_at is distinct from s.observed_progress_at or c.updated_at>s.progress_anchor_at then
  update research_eq10_fundamental_pit_20261020.financial_worker_supervision_v3 set observed_progress_at=c.last_useful_progress_at,progress_anchor_at=greatest(c.updated_at,c.last_useful_progress_at),ticks_without_progress=0,last_checked_at=t,state='USEFUL_PROGRESS',details=jsonb_build_object('last_useful_progress_at',c.last_useful_progress_at) where lane='financials';
  return jsonb_build_object('state','USEFUL_PROGRESS','last_useful_progress_at',c.last_useful_progress_at);
 end if;
 update research_eq10_fundamental_pit_20261020.financial_worker_supervision_v3 set ticks_without_progress=ticks_without_progress+1,last_checked_at=t where lane='financials' returning * into s;
 if t-s.progress_anchor_at<interval '5 minutes' and s.ticks_without_progress<10 then return jsonb_build_object('state','WITHIN_STAGE_BUDGET','ticks_without_progress',s.ticks_without_progress); end if;
 select count(*) filter(where status in('PENDING','RETRY') and next_attempt_at<=t and attempts<max_attempts),count(*) filter(where status='LEASED' and lease_expires_at>t),count(*) filter(where status='LEASED' and lease_expires_at<=t),min(next_attempt_at) filter(where status='RETRY' and next_attempt_at>t)
 into due_jobs,active_jobs,expired_jobs,retry_at from research_eq10_fundamental_pit_20261020.financial_worker_jobs_v1;
 if active_jobs>0 then state_name:='ACTIVE_LEASE_NO_PROGRESS';
 elsif expired_jobs>0 then state_name:='EXPIRED_LEASE_RECOVERY_DUE';
 elsif due_jobs>0 then state_name:='RUNNABLE_WORK_NO_PROGRESS';
 elsif retry_at is not null then state_name:='WAITING_RETRY_DEADLINE';
 else state_name:='NO_RUNNABLE_WORK_RECONCILIATION_REQUIRED'; end if;
 detail:=jsonb_build_object('state',state_name,'due_jobs',due_jobs,'active_jobs',active_jobs,'expired_jobs',expired_jobs,'next_retry_at',retry_at,'last_useful_progress_at',c.last_useful_progress_at,'ticks_without_progress',s.ticks_without_progress,'progress_anchor_at',s.progress_anchor_at,'recovery_owner','same pg_cron job 595 and existing fenced claim','automatic_stop_override',false);
 if s.last_diagnostic_at is null or t-s.last_diagnostic_at>=interval '5 minutes' or s.state<>state_name then
  insert into research_eq10_fundamental_pit_20261020.financial_worker_events_v1(event,details) values('NO_USEFUL_PROGRESS_DIAGNOSTIC',detail);
  update research_eq10_fundamental_pit_20261020.financial_worker_supervision_v3 set diagnostic_count=diagnostic_count+1,last_diagnostic_at=t where lane='financials';
 end if;
 update research_eq10_fundamental_pit_20261020.financial_worker_supervision_v3 set state=state_name,details=detail where lane='financials';
 return detail;
end $fn$;
revoke all on function research_eq10_fundamental_pit_20261020.financial_worker_supervise_v3() from public,anon,authenticated;

CREATE OR REPLACE FUNCTION research_eq10_fundamental_pit_20261020.financial_worker_dispatch_tick_v1()
 RETURNS bigint
 LANGUAGE plpgsql
 SET search_path TO 'pg_catalog', 'research_eq10_fundamental_pit_20261020'
AS $function$
declare rid bigint; t text;
begin
update research_eq10_fundamental_pit_20261020.financial_worker_dispatch_v1 d
set response_status=r.status_code,response_summary=jsonb_build_object('status_code',r.status_code,'content',r.content,'timed_out',r.timed_out,'error_msg',r.error_msg)
from net._http_response r where r.id=d.request_id and d.response_summary is null;
if not exists(select 1 from research_eq10_fundamental_pit_20261020.financial_worker_control_v1 where lane='financials' and not stopped) then return null;end if;
perform research_eq10_fundamental_pit_20261020.financial_worker_supervise_v3();
select decrypted_secret into t from vault.decrypted_secrets where name='phase2_financial_worker_token_v1';
rid:=net.http_post(url:='https://oxzabweahkoimtevbbny.supabase.co/functions/v1/phase2-financial-worker-v1',headers:=jsonb_build_object('Content-Type','application/json','Authorization','Bearer '||t),body:='{"scheduler":"pg_cron_phase2_v1"}',timeout_milliseconds:=55000);
insert into research_eq10_fundamental_pit_20261020.financial_worker_dispatch_v1(request_id,scheduler_job)values(rid,'phase2-financial-worker-v1');
return rid;
end $function$;
CREATE OR REPLACE FUNCTION research_eq10_fundamental_pit_20261020.financial_worker_refill_v2()
 RETURNS jsonb
 LANGUAGE plpgsql
 SET search_path TO 'pg_catalog', 'research_eq10_fundamental_pit_20261020'
AS $function$
declare ctl research_eq10_fundamental_pit_20261020.financial_worker_control_v1%rowtype; slots int; nd int:=0;nl int:=0;
begin
if not pg_try_advisory_xact_lock(2102026102) then return '{"state":"BUSY"}';end if;
select * into ctl from research_eq10_fundamental_pit_20261020.financial_worker_control_v1 where lane='financials' for update;
if ctl.stopped or not ctl.feeder_enabled or not ctl.bulk_enabled or not ctl.acceptance_passed or ctl.mode<>'BULK' then return '{"state":"GATED"}';end if;
select greatest(0,ctl.max_queued_jobs-count(*)) into slots from research_eq10_fundamental_pit_20261020.financial_worker_jobs_v1 where status in('PENDING','RETRY','LEASED') and job_kind<>'METADATA_PAGE';
with candidate as(
select m.accession_number,substring(m.raw_payload->>'main_file_url' from '/files/([A-Za-z0-9_.-]+)$') file_id,m.filing_date
from research_eq10_fundamental_pit_20261020.financial_filing_metadata_v1 m where m.form_type in('10-K','10-Q','10-K/A','10-Q/A')
and m.filing_date between '2023-01-01' and '2026-07-31'
and m.raw_payload->>'main_file_url' ~ ('^https://api[.]massive[.]com/v1/reference/sec/filings/'||m.accession_number||'/files/[A-Za-z0-9_.-]+[.]html?$')
and not exists(select 1 from research_eq10_fundamental_pit_20261020.financial_worker_jobs_v1 j where j.accession_number=m.accession_number and j.job_kind='DOCUMENT')
and not exists(select 1 from research_eq10_fundamental_pit_20261020.financial_documents_v1 d where d.accession_number=m.accession_number and exists(select 1 from research_eq10_fundamental_pit_20261020.financial_fact_v1 f where f.accession_number=d.accession_number and f.file_id=d.file_id))
order by m.filing_date desc,m.accession_number limit greatest(0,slots-case when ctl.metadata_terminal then 1 else 0 end))
insert into research_eq10_fundamental_pit_20261020.financial_worker_jobs_v1(job_id,accession_number,file_id,job_kind,priority,job_payload)
select 'document:'||accession_number||':'||file_id,accession_number,file_id,'DOCUMENT',10,jsonb_build_object('source','verified_massive_metadata','filing_date',filing_date) from candidate on conflict do nothing;
get diagnostics nd=row_count;
slots:=slots-nd;
if ctl.metadata_terminal and slots>0 then
with candidate as(
select i.accession_number,min(i.filing_date) filing_date from research_eq10_fundamental_pit_20261020.financial_filing_index_v1 i
where not exists(select 1 from research_eq10_fundamental_pit_20261020.financial_filing_metadata_v1 m where m.accession_number=i.accession_number)
and not exists(select 1 from research_eq10_fundamental_pit_20261020.financial_worker_jobs_v1 j where j.accession_number=i.accession_number and j.job_kind='FILING_LOOKUP')
group by i.accession_number order by min(i.filing_date) desc,i.accession_number limit slots)
insert into research_eq10_fundamental_pit_20261020.financial_worker_jobs_v1(job_id,accession_number,file_id,job_kind,priority)
select 'lookup:'||accession_number,accession_number,'__metadata__','FILING_LOOKUP',11 from candidate on conflict do nothing;
get diagnostics nl=row_count;
end if;
if nd+nl>0 then insert into research_eq10_fundamental_pit_20261020.financial_worker_events_v1(event,details)values('QUEUE_REFILLED',jsonb_build_object('documents',nd,'lookups',nl));end if;
return jsonb_build_object('state','FED','documents',nd,'lookups',nl);
end $function$
;
