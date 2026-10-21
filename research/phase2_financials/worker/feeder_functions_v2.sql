
create function research_eq10_fundamental_pit_20261020.financial_worker_claim_v2(p_execution text,p_version text,p_allow_page boolean)
returns jsonb language plpgsql security invoker set search_path=pg_catalog,research_eq10_fundamental_pit_20261020 as $fn$
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
order by acceptance_job desc,priority desc,created_at,job_id for update skip locked limit 1;
if not found then return jsonb_build_object('state','IDLE');end if;
expired:=j.status='LEASED';
update research_eq10_fundamental_pit_20261020.financial_worker_jobs_v1 set status='LEASED',attempts=attempts+1,lease_token=gen_random_uuid(),lease_owner=p_execution,lease_expires_at=clock_timestamp()+interval '90 seconds',updated_at=clock_timestamp() where job_id=j.job_id returning * into j;
insert into research_eq10_fundamental_pit_20261020.financial_worker_events_v1(job_id,event,execution_id,lease_token,details)values(j.job_id,case when expired then 'EXPIRED_LEASE_RECOVERED' else 'CLAIMED' end,p_execution,j.lease_token,jsonb_build_object('attempt',j.attempts,'checkpoint',j.checkpoint,'kind',j.job_kind,'version',p_version));
return jsonb_build_object('state','CLAIMED','job',to_jsonb(j),'max_units',ctl.max_units_per_request);
end $fn$;

create function research_eq10_fundamental_pit_20261020.financial_worker_refill_v2()
returns jsonb language plpgsql security invoker set search_path=pg_catalog,research_eq10_fundamental_pit_20261020 as $fn$
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
order by m.filing_date desc,m.accession_number limit slots)
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
select 'lookup:'||accession_number,accession_number,'__metadata__','FILING_LOOKUP',5 from candidate on conflict do nothing;
get diagnostics nl=row_count;
end if;
if nd+nl>0 then insert into research_eq10_fundamental_pit_20261020.financial_worker_events_v1(event,details)values('QUEUE_REFILLED',jsonb_build_object('documents',nd,'lookups',nl));end if;
return jsonb_build_object('state','FED','documents',nd,'lookups',nl);
end $fn$;

create function research_eq10_fundamental_pit_20261020.financial_worker_import_page_v2(p_job text,p_token uuid)
returns jsonb language plpgsql security invoker set search_path=pg_catalog,research_eq10_fundamental_pit_20261020 as $fn$
declare j research_eq10_fundamental_pit_20261020.financial_worker_jobs_v1%rowtype;r research_eq10_fundamental_pit_20261020.financial_metadata_pages_raw_v2%rowtype;
x jsonb;body jsonb;total int;valid int:=0;rejected int:=0;ord int:=0;nextseq int;nextkey text;result jsonb;reason text;
begin
select a.* into j from research_eq10_fundamental_pit_20261020.financial_worker_jobs_v1 a cross join research_eq10_fundamental_pit_20261020.financial_worker_control_v1 c where a.job_id=p_job and a.lease_token=p_token and a.status='LEASED' and a.lease_expires_at>clock_timestamp() and c.lane='financials' and not c.stopped for update of a;
if not found then raise exception 'LEASE_LOST_OR_STOPPED';end if;
select * into r from research_eq10_fundamental_pit_20261020.financial_metadata_pages_raw_v2 where page_key=j.job_payload->>'page_key';
if not found then raise exception 'PAGE_CHECKPOINT_MISSING';end if;
if r.response_sha256<>encode(sha256(convert_to(r.raw_response,'UTF8')),'hex') then raise exception 'PAGE_HASH_MISMATCH';end if;
body:=r.raw_response::jsonb;
if jsonb_typeof(body->'results')<>'array' then raise exception 'INVALID_PAGE_RESULTS';end if;
total:=jsonb_array_length(body->'results');
if total>1000 then raise exception 'PAGE_LIMIT_EXCEEDED';end if;
insert into research_eq10_fundamental_pit_20261020.financial_source_pages_v1(page_key,route,request_params,expected_rows,imported_rows,next_params,response_summary,collected_at)
values(r.page_key,r.route,r.request_params,total,0,r.next_params,jsonb_build_object('raw_table','financial_metadata_pages_raw_v2','sha256',r.response_sha256,'provider','Massive')::text,r.received_at) on conflict do nothing;
for x in select value from jsonb_array_elements(body->'results') loop
ord:=ord+1;
begin
if x->>'accession_number' is null or x->>'acceptance_datetime' is null or x->>'period_of_report_date' is null or jsonb_typeof(x->'entities')<>'array' or jsonb_array_length(x->'entities')=0 then raise exception 'MISSING_REQUIRED_METADATA';end if;
perform research_eq10_fundamental_pit_20261020.financial_worker_save_metadata_v1(x);
update research_eq10_fundamental_pit_20261020.financial_filing_metadata_v1 set source_page_key=coalesce(source_page_key,r.page_key) where accession_number=x->>'accession_number';
valid:=valid+1;
exception when others then
get stacked diagnostics reason=message_text;
insert into research_eq10_fundamental_pit_20261020.financial_ingest_issues_v2(issue_id,job_id,accession_number,reason,classification,evidence)
values(r.page_key||':'||ord,p_job,x->>'accession_number','METADATA_ROW_INVALID_OR_CONFLICT','EXCLUDED',jsonb_build_object('ordinal',ord,'sqlstate',SQLSTATE,'detail',left(reason,240),'raw_page',r.page_key)) on conflict do nothing;
rejected:=rejected+1;
end;
end loop;
update research_eq10_fundamental_pit_20261020.financial_source_pages_v1 set imported_rows=valid,next_params=r.next_params,checkpointed_at=clock_timestamp(),response_summary=jsonb_build_object('raw_table','financial_metadata_pages_raw_v2','sha256',r.response_sha256,'processed',total,'imported',valid,'rejected',rejected)::text where page_key=r.page_key;
update research_eq10_fundamental_pit_20261020.financial_metadata_pages_raw_v2 set imported_at=clock_timestamp() where page_key=r.page_key;
if r.next_params is not null then
nextseq:=(j.job_payload->>'sequence')::int+1;nextkey:='metadata_20230101_20260731_'||lpad(nextseq::text,4,'0');
insert into research_eq10_fundamental_pit_20261020.financial_worker_jobs_v1(job_id,accession_number,file_id,job_kind,priority,job_payload)
values('page:'||nextkey,'METADATA',nextkey,'METADATA_PAGE',100,jsonb_build_object('page_key',nextkey,'sequence',nextseq,'params',r.next_params)) on conflict do nothing;
else update research_eq10_fundamental_pit_20261020.financial_worker_control_v1 set metadata_terminal=true where lane='financials';end if;
result:=jsonb_build_object('page_key',r.page_key,'processed',total,'imported',valid,'rejected',rejected,'terminal',r.next_params is null,'sha256',r.response_sha256);
update research_eq10_fundamental_pit_20261020.financial_worker_jobs_v1 set status='COMPLETE',checkpoint='VERIFIED_PERSISTED',result=result,completed_at=clock_timestamp(),updated_at=clock_timestamp(),lease_expires_at=null,last_error_class=null,last_error=null where job_id=p_job;
update research_eq10_fundamental_pit_20261020.financial_worker_control_v1 set last_useful_progress_at=clock_timestamp() where lane='financials';
insert into research_eq10_fundamental_pit_20261020.financial_worker_events_v1(job_id,event,execution_id,lease_token,details)values(p_job,'METADATA_PAGE_COMPLETE',j.lease_owner,p_token,result);
return result;
end $fn$;
revoke all on function research_eq10_fundamental_pit_20261020.financial_worker_claim_v2(text,text,boolean),research_eq10_fundamental_pit_20261020.financial_worker_refill_v2(),research_eq10_fundamental_pit_20261020.financial_worker_import_page_v2(text,uuid) from public,anon,authenticated;

create or replace function research_eq10_fundamental_pit_20261020.financial_worker_import_page_v2(p_job text,p_token uuid)
returns jsonb language plpgsql security invoker set search_path=pg_catalog,research_eq10_fundamental_pit_20261020 as $fn$
declare j research_eq10_fundamental_pit_20261020.financial_worker_jobs_v1%rowtype;r research_eq10_fundamental_pit_20261020.financial_metadata_pages_raw_v2%rowtype;
x jsonb;body jsonb;total int;valid int:=0;rejected int:=0;ord int:=0;nextseq int;nextkey text;out_result jsonb;reason text;
begin
select a.* into j from research_eq10_fundamental_pit_20261020.financial_worker_jobs_v1 a cross join research_eq10_fundamental_pit_20261020.financial_worker_control_v1 c where a.job_id=p_job and a.lease_token=p_token and a.status='LEASED' and a.lease_expires_at>clock_timestamp() and c.lane='financials' and not c.stopped for update of a;
if not found then raise exception 'LEASE_LOST_OR_STOPPED';end if;
select * into r from research_eq10_fundamental_pit_20261020.financial_metadata_pages_raw_v2 where page_key=j.job_payload->>'page_key';
if not found then raise exception 'PAGE_CHECKPOINT_MISSING';end if;
if r.response_sha256<>encode(sha256(convert_to(r.raw_response,'UTF8')),'hex') then raise exception 'PAGE_HASH_MISMATCH';end if;
body:=r.raw_response::jsonb;
if jsonb_typeof(body->'results')<>'array' then raise exception 'INVALID_PAGE_RESULTS';end if;
total:=jsonb_array_length(body->'results');
if total>1000 then raise exception 'PAGE_LIMIT_EXCEEDED';end if;
insert into research_eq10_fundamental_pit_20261020.financial_source_pages_v1(page_key,route,request_params,expected_rows,imported_rows,next_params,response_summary,collected_at)
values(r.page_key,r.route,r.request_params,total,0,r.next_params,jsonb_build_object('raw_table','financial_metadata_pages_raw_v2','sha256',r.response_sha256,'provider','Massive')::text,r.received_at) on conflict do nothing;
for x in select value from jsonb_array_elements(body->'results') loop
ord:=ord+1;
begin
if x->>'accession_number' is null or x->>'acceptance_datetime' is null or x->>'period_of_report_date' is null or jsonb_typeof(x->'entities')<>'array' or jsonb_array_length(x->'entities')=0 then raise exception 'MISSING_REQUIRED_METADATA';end if;
perform research_eq10_fundamental_pit_20261020.financial_worker_save_metadata_v1(x);
update research_eq10_fundamental_pit_20261020.financial_filing_metadata_v1 set source_page_key=coalesce(source_page_key,r.page_key) where accession_number=x->>'accession_number';
valid:=valid+1;
exception when others then
get stacked diagnostics reason=message_text;
insert into research_eq10_fundamental_pit_20261020.financial_ingest_issues_v2(issue_id,job_id,accession_number,reason,classification,evidence)
values(r.page_key||':'||ord,p_job,x->>'accession_number','METADATA_ROW_INVALID_OR_CONFLICT','EXCLUDED',jsonb_build_object('ordinal',ord,'sqlstate',SQLSTATE,'detail',left(reason,240),'raw_page',r.page_key)) on conflict do nothing;
rejected:=rejected+1;
end;
end loop;
update research_eq10_fundamental_pit_20261020.financial_source_pages_v1 set imported_rows=valid,next_params=r.next_params,checkpointed_at=clock_timestamp(),response_summary=jsonb_build_object('raw_table','financial_metadata_pages_raw_v2','sha256',r.response_sha256,'processed',total,'imported',valid,'rejected',rejected)::text where page_key=r.page_key;
update research_eq10_fundamental_pit_20261020.financial_metadata_pages_raw_v2 set imported_at=clock_timestamp() where page_key=r.page_key;
if r.next_params is not null then
nextseq:=(j.job_payload->>'sequence')::int+1;nextkey:='metadata_20230101_20260731_'||lpad(nextseq::text,4,'0');
insert into research_eq10_fundamental_pit_20261020.financial_worker_jobs_v1(job_id,accession_number,file_id,job_kind,priority,job_payload)
values('page:'||nextkey,'METADATA',nextkey,'METADATA_PAGE',100,jsonb_build_object('page_key',nextkey,'sequence',nextseq,'params',r.next_params)) on conflict do nothing;
else update research_eq10_fundamental_pit_20261020.financial_worker_control_v1 set metadata_terminal=true where lane='financials';end if;
out_result:=jsonb_build_object('page_key',r.page_key,'processed',total,'imported',valid,'rejected',rejected,'terminal',r.next_params is null,'sha256',r.response_sha256);
update research_eq10_fundamental_pit_20261020.financial_worker_jobs_v1 set status='COMPLETE',checkpoint='VERIFIED_PERSISTED',result=out_result,completed_at=clock_timestamp(),updated_at=clock_timestamp(),lease_expires_at=null,last_error_class=null,last_error=null where job_id=p_job;
update research_eq10_fundamental_pit_20261020.financial_worker_control_v1 set last_useful_progress_at=clock_timestamp() where lane='financials';
insert into research_eq10_fundamental_pit_20261020.financial_worker_events_v1(job_id,event,execution_id,lease_token,details)values(p_job,'METADATA_PAGE_COMPLETE',j.lease_owner,p_token,out_result);
return out_result;
end $fn$;
