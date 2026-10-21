create view research_eq10_fundamental_pit_20261020.financial_worker_progress_v2 with(security_invoker=true) as
select clock_timestamp() observed_at,to_jsonb(c) control,
(select jsonb_agg(to_jsonb(q)) from(select job_kind,status,count(*) jobs from research_eq10_fundamental_pit_20261020.financial_worker_jobs_v1 group by 1,2 order by 1,2)q) jobs,
(select count(*) from research_eq10_fundamental_pit_20261020.financial_filing_metadata_v1) metadata_records,
(select count(*) from research_eq10_fundamental_pit_20261020.financial_documents_v1) stored_documents,
(select count(*) from research_eq10_fundamental_pit_20261020.financial_fact_v1) fact_entries,
(select count(*) from research_eq10_fundamental_pit_20261020.financial_metrics_v1) normalized_metrics,
(select jsonb_agg(to_jsonb(q)) from(select last_error_class,count(*) jobs from research_eq10_fundamental_pit_20261020.financial_worker_jobs_v1 where status='BLOCKED' group by 1)q) blocked_reasons,
(select jsonb_agg(to_jsonb(q)) from(select page_key,expected_rows,imported_rows,next_params,checkpointed_at from research_eq10_fundamental_pit_20261020.financial_source_pages_v1 where route='/v1/reference/sec/filings' order by checkpointed_at desc limit 1)q) metadata_checkpoint
from research_eq10_fundamental_pit_20261020.financial_worker_control_v1 c;

create view research_eq10_fundamental_pit_20261020.financial_population_status_v2 with(security_invoker=true) as
with idx as(select accession_number,min(filing_date) filing_date,min(form_type) form_type,count(distinct cik) indexed_issuers from research_eq10_fundamental_pit_20261020.financial_filing_index_v1 group by accession_number),
jobs as(select accession_number,
bool_or(job_kind='DOCUMENT' and status='COMPLETE') extracted,
bool_or(job_kind='DOCUMENT' and status='BLOCKED') document_blocked,
bool_or(job_kind='FILING_LOOKUP' and last_error_class='PROVIDER_HTTP_404') detail_404,
bool_or(job_kind='FILING_LOOKUP' and status='BLOCKED') lookup_blocked,
bool_or(status in('PENDING','RETRY','LEASED')) queued,
string_agg(distinct last_error_class,',') errors from research_eq10_fundamental_pit_20261020.financial_worker_jobs_v1 group by accession_number)
select i.*,m.cik metadata_primary_cik,a.classification availability_classification,a.effective_available_at,
case when coalesce(j.extracted,false) or exists(select 1 from research_eq10_fundamental_pit_20261020.financial_documents_v1 d where d.accession_number=i.accession_number and exists(select 1 from research_eq10_fundamental_pit_20261020.financial_fact_v1 f where f.accession_number=d.accession_number and f.file_id=d.file_id)) then 'EXTRACTED'
when j.document_blocked then 'DOCUMENT_BLOCKED' when j.detail_404 then 'UNAVAILABLE_MASSIVE_DETAIL_404'
when j.lookup_blocked then 'LOOKUP_BLOCKED' when j.queued then 'QUEUED'
when m.accession_number is null then 'METADATA_NOT_YET_COLLECTED'
when m.raw_payload->>'main_file_url' is null then 'METADATA_PRIMARY_UNAVAILABLE'
else 'METADATA_READY_UNQUEUED' end processing_classification,j.errors
from idx i left join research_eq10_fundamental_pit_20261020.financial_filing_metadata_v1 m using(accession_number)
left join research_eq10_fundamental_pit_20261020.financial_availability_review_v2 a using(accession_number)
left join jobs j using(accession_number);

create view research_eq10_fundamental_pit_20261020.financial_metric_missingness_v2 with(security_invoker=true) as
with docs as(select d.accession_number,d.file_id,m.report_date,m.filing_date,m.form_type,a.classification,a.effective_available_at from research_eq10_fundamental_pit_20261020.financial_documents_v1 d join research_eq10_fundamental_pit_20261020.financial_filing_metadata_v1 m using(accession_number) join research_eq10_fundamental_pit_20261020.financial_availability_review_v2 a using(accession_number)),
mapped as(select f.accession_number,f.file_id,k.metric,count(*) source_entries,
count(*) filter(where f.extraction_state='PARSED') parsed_entries,
count(*) filter(where f.extraction_state='NIL') nil_entries,
count(*) filter(where f.extraction_state='FORMAT_UNSUPPORTED') unsupported_entries,
count(*) filter(where f.extraction_state='PARSED' and f.dimension_count=0) consolidated_entries,
count(*) filter(where f.extraction_state='PARSED' and f.dimension_count=0 and(f.period_end=m.report_date or(k.metric='shares_outstanding' and f.period_end between m.report_date and m.filing_date))) current_consolidated_entries
from research_eq10_fundamental_pit_20261020.financial_fact_v1 f join research_eq10_fundamental_pit_20261020.financial_metric_dictionary_v1 k using(concept) join research_eq10_fundamental_pit_20261020.financial_filing_metadata_v1 m using(accession_number) group by 1,2,3),
retained as(select accession_number,file_id,metric,count(*) n from research_eq10_fundamental_pit_20261020.financial_metrics_v1 group by 1,2,3)
select d.accession_number,d.file_id,k.metric,coalesce(s.source_entries,0) source_entries,coalesce(r.n,0) retained_values,
case when coalesce(r.n,0)>0 then 'PRESENT'
when d.form_type in('10-K/A','10-Q/A') then 'AMENDMENT_NORMALIZATION_HELD'
when d.effective_available_at is null then d.classification
when s.source_entries is null then 'SOURCE_CONCEPT_ABSENT_OR_UNMAPPED'
when s.parsed_entries=0 and s.unsupported_entries>0 then 'FORMAT_UNSUPPORTED'
when s.parsed_entries=0 and s.nil_entries>0 then 'NIL'
when s.consolidated_entries=0 then 'DIMENSIONED_ONLY'
when s.current_consolidated_entries=0 then 'NO_CURRENT_CONSOLIDATED_PERIOD'
else 'UNIT_CONFLICT_OR_OTHER_NORMALIZATION_GUARD' end missingness_classification
from docs d cross join(select distinct metric from research_eq10_fundamental_pit_20261020.financial_metric_dictionary_v1)k
left join mapped s on s.accession_number=d.accession_number and s.file_id=d.file_id and s.metric=k.metric
left join retained r on r.accession_number=d.accession_number and r.file_id=d.file_id and r.metric=k.metric;
revoke all on research_eq10_fundamental_pit_20261020.financial_worker_progress_v2,research_eq10_fundamental_pit_20261020.financial_population_status_v2,research_eq10_fundamental_pit_20261020.financial_metric_missingness_v2 from public,anon,authenticated;
