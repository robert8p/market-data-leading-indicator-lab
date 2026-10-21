create table research_eq10_fundamental_pit_20261020.financial_metric_audit_v3(
accession_number text not null,file_id text not null,audit_version text not null,
document_sha256 text not null,metric text not null,source_entries bigint not null,
retained_values bigint not null,missingness_classification text not null,
audited_at timestamptz not null default clock_timestamp(),
primary key(accession_number,file_id,audit_version,metric),
foreign key(accession_number,file_id) references research_eq10_fundamental_pit_20261020.financial_document_registry_v3(accession_number,file_id));
alter table research_eq10_fundamental_pit_20261020.financial_metric_audit_v3 enable row level security;
revoke all on research_eq10_fundamental_pit_20261020.financial_metric_audit_v3 from public,anon,authenticated;
create function research_eq10_fundamental_pit_20261020.financial_audit_document_v3(p_accession text,p_file text)
returns integer language plpgsql security invoker set search_path=pg_catalog,research_eq10_fundamental_pit_20261020 as $fn$
declare n int;
begin
with doc as(
select d.document_sha256,m.report_date,m.filing_date,m.form_type,a.classification,a.effective_available_at,
coalesce(j.status='COMPLETE',exists(select 1 from research_eq10_fundamental_pit_20261020.financial_fact_v1 f where f.accession_number=p_accession and f.file_id=p_file)) complete,j.last_error_class
from research_eq10_fundamental_pit_20261020.financial_document_registry_v3 d join research_eq10_fundamental_pit_20261020.financial_filing_metadata_v1 m using(accession_number)
join research_eq10_fundamental_pit_20261020.financial_availability_review_v2 a using(accession_number)
left join research_eq10_fundamental_pit_20261020.financial_worker_jobs_v1 j on j.accession_number=d.accession_number and j.file_id=d.file_id and j.job_kind='DOCUMENT'
where d.accession_number=p_accession and d.file_id=p_file
), mapped as(
select k.metric,count(*) entries,count(*)filter(where f.extraction_state='PARSED') parsed,
count(*)filter(where f.extraction_state='NIL') nil,
count(*)filter(where f.extraction_state='FORMAT_UNSUPPORTED') unsupported,
count(*)filter(where f.extraction_state='PARSED' and f.dimension_count=0) consolidated,
count(*)filter(where f.extraction_state='PARSED' and f.dimension_count=0 and(f.period_end=d.report_date or(k.metric='shares_outstanding' and f.period_end between d.report_date and d.filing_date))) current_consolidated
from research_eq10_fundamental_pit_20261020.financial_fact_v1 f join research_eq10_fundamental_pit_20261020.financial_metric_dictionary_v1 k using(concept) cross join doc d
where f.accession_number=p_accession and f.file_id=p_file group by k.metric
),retained as(
select metric,count(*) n from research_eq10_fundamental_pit_20261020.financial_metrics_v1 where accession_number=p_accession and file_id=p_file group by metric
)
insert into research_eq10_fundamental_pit_20261020.financial_metric_audit_v3(accession_number,file_id,audit_version,document_sha256,metric,source_entries,retained_values,missingness_classification)
select p_accession,p_file,'metric_audit_v3.0.0',d.document_sha256,k.metric,coalesce(s.entries,0),coalesce(r.n,0),
case when not d.complete then coalesce(d.last_error_class,'EXTRACTION_PENDING')
 when coalesce(r.n,0)>0 then 'PRESENT'
 when d.form_type in('10-K/A','10-Q/A') then 'AMENDMENT_NORMALIZATION_HELD'
 when d.effective_available_at is null then d.classification
 when s.entries is null then 'SOURCE_CONCEPT_ABSENT_OR_UNMAPPED'
 when s.parsed=0 and s.unsupported>0 then 'FORMAT_UNSUPPORTED'
 when s.parsed=0 and s.nil>0 then 'NIL'
 when s.consolidated=0 then 'DIMENSIONED_ONLY'
 when s.current_consolidated=0 then 'NO_CURRENT_CONSOLIDATED_PERIOD'
 else 'UNIT_CONFLICT_OR_OTHER_NORMALIZATION_GUARD' end
from doc d cross join(select distinct metric from research_eq10_fundamental_pit_20261020.financial_metric_dictionary_v1) k
left join mapped s using(metric) left join retained r using(metric)
on conflict(accession_number,file_id,audit_version,metric) do update set
source_entries=excluded.source_entries,retained_values=excluded.retained_values,missingness_classification=excluded.missingness_classification,audited_at=clock_timestamp()
where(financial_metric_audit_v3.source_entries,financial_metric_audit_v3.retained_values,financial_metric_audit_v3.missingness_classification) is distinct from(excluded.source_entries,excluded.retained_values,excluded.missingness_classification);
get diagnostics n=row_count;return n;
end $fn$;
create function research_eq10_fundamental_pit_20261020.financial_audit_pending_documents_v3(p_limit int default 4)
returns integer language plpgsql security invoker set search_path=pg_catalog,research_eq10_fundamental_pit_20261020 as $fn$
declare r record;n int:=0;changed int;
begin
if p_limit not between 1 and 4 then raise exception 'INVALID_AUDIT_BATCH_BOUND';end if;
for r in
select d.accession_number,d.file_id from research_eq10_fundamental_pit_20261020.financial_document_registry_v3 d
where not exists(select 1 from research_eq10_fundamental_pit_20261020.financial_metric_audit_v3 a where a.accession_number=d.accession_number and a.file_id=d.file_id and a.audit_version='metric_audit_v3.0.0')
and not exists(select 1 from research_eq10_fundamental_pit_20261020.financial_worker_jobs_v1 j where j.accession_number=d.accession_number and j.file_id=d.file_id and j.job_kind='DOCUMENT' and j.status not in('COMPLETE','BLOCKED'))
order by d.registered_at,d.accession_number,d.file_id limit p_limit
loop changed:=research_eq10_fundamental_pit_20261020.financial_audit_document_v3(r.accession_number,r.file_id);n:=n+changed;end loop;
if n>0 then update research_eq10_fundamental_pit_20261020.financial_worker_control_v1 set last_useful_progress_at=clock_timestamp() where lane='financials';end if;
return n;
end $fn$;
revoke all on function research_eq10_fundamental_pit_20261020.financial_audit_document_v3(text,text),research_eq10_fundamental_pit_20261020.financial_audit_pending_documents_v3(int) from public,anon,authenticated;