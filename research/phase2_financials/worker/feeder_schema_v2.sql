alter table research_eq10_fundamental_pit_20261020.financial_worker_jobs_v1
 add column job_kind text not null default 'DOCUMENT' check(job_kind in('DOCUMENT','METADATA_PAGE','FILING_LOOKUP')),
 add column job_payload jsonb not null default '{}',
 add column priority integer not null default 0;
alter table research_eq10_fundamental_pit_20261020.financial_worker_control_v1
 add column feeder_enabled boolean not null default false,
 add column metadata_terminal boolean not null default false,
 add column max_units_per_request integer not null default 4 check(max_units_per_request between 1 and 8),
 add column last_useful_progress_at timestamptz,
 add column max_queued_jobs integer not null default 64 check(max_queued_jobs between 1 and 1000);
create table research_eq10_fundamental_pit_20261020.financial_metadata_pages_raw_v2(
 page_key text primary key,route text not null,request_params jsonb not null,
 raw_response text not null,response_sha256 text not null,provider_evidence jsonb not null,
 next_params jsonb,received_at timestamptz not null,imported_at timestamptz);
create table research_eq10_fundamental_pit_20261020.financial_ingest_issues_v2(
 issue_id text primary key,job_id text,accession_number text,reason text not null,
 classification text not null,evidence jsonb not null,created_at timestamptz not null default clock_timestamp());
alter table research_eq10_fundamental_pit_20261020.financial_metadata_pages_raw_v2 enable row level security;
alter table research_eq10_fundamental_pit_20261020.financial_ingest_issues_v2 enable row level security;
revoke all on research_eq10_fundamental_pit_20261020.financial_metadata_pages_raw_v2,research_eq10_fundamental_pit_20261020.financial_ingest_issues_v2 from public,anon,authenticated;
create index financial_worker_jobs_v1_kind_status on research_eq10_fundamental_pit_20261020.financial_worker_jobs_v1(job_kind,status,next_attempt_at);
create index financial_metadata_v1_feed on research_eq10_fundamental_pit_20261020.financial_filing_metadata_v1(filing_date desc,accession_number);
insert into research_eq10_fundamental_pit_20261020.financial_evidence_v1(evidence_id,evidence_type,payload)
values('persistent_feeder_20261021_v2','IMPLEMENTATION_CHECKPOINT','{"stage":"SCHEMA_INSTALLED","base_worker_version":2,"base_commit":"d0da25d1e7dcf614b17d9f54c8b854236c2257de","scope":"Resume metadata page 27, feed verified document jobs, retain one scheduler","bulk_enabled":false}');
