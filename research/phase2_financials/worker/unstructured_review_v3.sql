create table research_eq10_fundamental_pit_20261020.financial_document_review_v3(
accession_number text not null,file_id text not null,document_sha256 text not null,
review_version text not null,classification text not null,evidence jsonb not null,
reviewed_at timestamptz not null default clock_timestamp(),
primary key(accession_number,file_id,document_sha256,review_version),
foreign key(accession_number,file_id) references research_eq10_fundamental_pit_20261020.financial_document_registry_v3(accession_number,file_id));
alter table research_eq10_fundamental_pit_20261020.financial_document_review_v3 enable row level security;
revoke all on research_eq10_fundamental_pit_20261020.financial_document_review_v3 from public,anon,authenticated;
create function research_eq10_fundamental_pit_20261020.financial_review_unstructured_documents_v3(p_limit int default 4)
returns integer language plpgsql security invoker set search_path=pg_catalog,research_eq10_fundamental_pit_20261020 as $fn$
declare n int;
begin
if p_limit not between 1 and 10 then raise exception 'INVALID_REVIEW_BATCH_BOUND';end if;
with candidates as materialized(
select d.*,m.raw_payload->'entities' entities from research_eq10_fundamental_pit_20261020.financial_documents_v1 d join research_eq10_fundamental_pit_20261020.financial_worker_jobs_v1 j using(accession_number,file_id) join research_eq10_fundamental_pit_20261020.financial_filing_metadata_v1 m using(accession_number)
where j.job_kind='DOCUMENT' and j.status='BLOCKED' and j.last_error_class='NO_SUPPORTED_INLINE_XBRL'
and not exists(select 1 from research_eq10_fundamental_pit_20261020.financial_document_review_v3 r where r.accession_number=d.accession_number and r.file_id=d.file_id and r.document_sha256=d.document_sha256 and r.review_version='html_source_review_v3.0.0')
order by j.created_at limit p_limit
),texts as(
select *,regexp_replace(regexp_replace(raw_document,'<[^>]*>',' ','g'),'[[:space:]]+',' ','g') plain from candidates
)
insert into research_eq10_fundamental_pit_20261020.financial_document_review_v3(accession_number,file_id,document_sha256,review_version,classification,evidence)
select accession_number,file_id,document_sha256,'html_source_review_v3.0.0',
case when exists(select 1 from jsonb_array_elements(entities)e where e#>>'{company_data,sic}'='6189') and plain~*'asset.backed' then 'CONDITIONAL_NON_EQUITY_ABS_CANDIDATE' else 'UNSTRUCTURED_FINANCIAL_EXTRACTION_REQUIRED' end,
jsonb_build_object('provider',provider,'route',route,'source_document_sha256',document_sha256,'issuer_entities',entities,'source_bytes',octet_length(raw_document),'inline_xbrl_present',position('inlineXBRL' in raw_document)>0,'general_instruction_j',plain~*'General Instruction J','asset_backed_text',substring(plain from '(?i).{0,80}asset.backed.{0,180}'),'financial_statement_text',substring(plain from '(?i).{0,40}(?:Balance Sheets|Statements of Income|Statements of Cash Flows).{0,160}'),'classification_limit','Candidate review only; does not establish historical security-universe exclusion or financial-value absence. No values invented and no original records removed.')
from texts on conflict do nothing;
get diagnostics n=row_count;return n;
end $fn$;
revoke all on function research_eq10_fundamental_pit_20261020.financial_review_unstructured_documents_v3(int) from public,anon,authenticated;