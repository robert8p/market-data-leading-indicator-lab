alter table research_eq10_fundamental_pit_20261020.financial_worker_jobs_v1 drop constraint financial_worker_jobs_v1_job_kind_check;
alter table research_eq10_fundamental_pit_20261020.financial_worker_jobs_v1 add constraint financial_worker_jobs_v1_job_kind_check check(job_kind in('DOCUMENT','METADATA_PAGE','FILING_LOOKUP','VALIDATION','STATE_BUILD'));
create or replace function research_eq10_fundamental_pit_20261020.financial_enqueue_company_state_v3(p_accession text default null)
returns integer language plpgsql security invoker set search_path=pg_catalog,research_eq10_fundamental_pit_20261020 as $fn$
declare n int;
begin
insert into research_eq10_fundamental_pit_20261020.financial_worker_jobs_v1(job_id,accession_number,file_id,job_kind,priority,job_payload)
select 'state:'||entity_cik,entity_cik,'__company_states_v3__','STATE_BUILD',1,jsonb_build_object('entity_cik',entity_cik,'state_after',null,'input_revision',encode(sha256(convert_to(string_agg(metric_id||':'||value::text,'|' order by metric_id),'UTF8')),'hex'))
from research_eq10_fundamental_pit_20261020.financial_metrics_v1 where p_accession is null or entity_cik in(select entity_cik from research_eq10_fundamental_pit_20261020.financial_metrics_v1 where accession_number=p_accession) group by entity_cik
on conflict(job_id) do update set
 status=case when financial_worker_jobs_v1.status in('COMPLETE','PENDING') then 'PENDING' else financial_worker_jobs_v1.status end,
 attempts=case when financial_worker_jobs_v1.status='COMPLETE' then 0 else financial_worker_jobs_v1.attempts end,
 job_payload=case when financial_worker_jobs_v1.status in('COMPLETE','PENDING') then financial_worker_jobs_v1.job_payload||excluded.job_payload else financial_worker_jobs_v1.job_payload end,
 next_attempt_at=case when financial_worker_jobs_v1.status='COMPLETE' then clock_timestamp() else financial_worker_jobs_v1.next_attempt_at end,
 updated_at=clock_timestamp()
where financial_worker_jobs_v1.job_payload->>'input_revision' is distinct from excluded.job_payload->>'input_revision';
get diagnostics n=row_count;
return n;
end $fn$;
revoke all on function research_eq10_fundamental_pit_20261020.financial_enqueue_company_state_v3(text) from public,anon,authenticated;

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
select greatest(0,ctl.max_queued_jobs-count(*)) into slots from research_eq10_fundamental_pit_20261020.financial_worker_jobs_v1 where status in('PENDING','RETRY','LEASED') and job_kind in('DOCUMENT','FILING_LOOKUP');
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
