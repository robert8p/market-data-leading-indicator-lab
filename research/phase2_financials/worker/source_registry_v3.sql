
set lock_timeout='1500ms';
set statement_timeout='15s';
create table research_eq10_fundamental_pit_20261020.financial_document_registry_v3(
 accession_number text not null,file_id text not null,provider text not null check(provider='Massive'),route text not null,
 document_sha256 text not null,storage_kind text not null check(storage_kind in('INLINE_TEXT','CHUNKED')),
 capture_id uuid references research_eq10_fundamental_pit_20261020.financial_stream_capture_v3(capture_id),
 registered_at timestamptz not null default clock_timestamp(),primary key(accession_number,file_id),
 check((storage_kind='CHUNKED' and capture_id is not null) or(storage_kind='INLINE_TEXT' and capture_id is null))
);
alter table research_eq10_fundamental_pit_20261020.financial_document_registry_v3 enable row level security;
revoke all on research_eq10_fundamental_pit_20261020.financial_document_registry_v3 from public,anon,authenticated;
create function research_eq10_fundamental_pit_20261020.financial_register_inline_document_v3()
returns trigger language plpgsql security invoker set search_path=pg_catalog,research_eq10_fundamental_pit_20261020 as $fn$
begin
insert into research_eq10_fundamental_pit_20261020.financial_document_registry_v3(accession_number,file_id,provider,route,document_sha256,storage_kind)
values(new.accession_number,new.file_id,new.provider,new.route,new.document_sha256,'INLINE_TEXT') on conflict do nothing;
if not exists(select 1 from research_eq10_fundamental_pit_20261020.financial_document_registry_v3 r where r.accession_number=new.accession_number and r.file_id=new.file_id and r.document_sha256=new.document_sha256) then raise exception 'SOURCE_VERSION_CONFLICT';end if;
return new;
end $fn$;
revoke all on function research_eq10_fundamental_pit_20261020.financial_register_inline_document_v3() from public,anon,authenticated;
create trigger financial_register_inline_document_v3 after insert on research_eq10_fundamental_pit_20261020.financial_documents_v1 for each row execute function research_eq10_fundamental_pit_20261020.financial_register_inline_document_v3();
insert into research_eq10_fundamental_pit_20261020.financial_document_registry_v3(accession_number,file_id,provider,route,document_sha256,storage_kind)
select accession_number,file_id,provider,route,document_sha256,'INLINE_TEXT' from research_eq10_fundamental_pit_20261020.financial_documents_v1;
create function research_eq10_fundamental_pit_20261020.financial_register_stream_capture_v3(p_capture uuid)
returns void language plpgsql security invoker set search_path=pg_catalog,research_eq10_fundamental_pit_20261020 as $fn$
declare c research_eq10_fundamental_pit_20261020.financial_stream_capture_v3%rowtype;
begin
select * into strict c from research_eq10_fundamental_pit_20261020.financial_stream_capture_v3 where capture_id=p_capture and status='COMPLETE';
insert into research_eq10_fundamental_pit_20261020.financial_document_registry_v3(accession_number,file_id,provider,route,document_sha256,storage_kind,capture_id)
values(c.accession_number,c.file_id,c.provider,c.route,c.document_sha256,'CHUNKED',p_capture) on conflict do nothing;
if not exists(select 1 from research_eq10_fundamental_pit_20261020.financial_document_registry_v3 r where r.accession_number=c.accession_number and r.file_id=c.file_id and r.document_sha256=c.document_sha256) then raise exception 'SOURCE_VERSION_CONFLICT';end if;
end $fn$;
revoke all on function research_eq10_fundamental_pit_20261020.financial_register_stream_capture_v3(uuid) from public,anon,authenticated;
alter table research_eq10_fundamental_pit_20261020.financial_fact_v1 add constraint financial_fact_document_registry_v3_fkey foreign key(accession_number,file_id) references research_eq10_fundamental_pit_20261020.financial_document_registry_v3(accession_number,file_id) not valid;
alter table research_eq10_fundamental_pit_20261020.financial_fact_v1 drop constraint financial_fact_v1_accession_number_file_id_fkey;

ALTER TABLE research_eq10_fundamental_pit_20261020.financial_fact_v1 VALIDATE CONSTRAINT financial_fact_document_registry_v3_fkey;
