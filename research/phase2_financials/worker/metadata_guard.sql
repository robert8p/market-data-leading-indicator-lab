
create function research_eq10_fundamental_pit_20261020.financial_worker_save_metadata_v1(x jsonb) returns void language plpgsql security invoker set search_path=pg_catalog,research_eq10_fundamental_pit_20261020 as $fn$
declare a text:=x->>'accession_number';
begin
if a!~'^[0-9]{10}-[0-9]{2}-[0-9]{6}$' or x->>'filing_date'!~'^[0-9]{8}$' or x->>'acceptance_datetime'!~'^[0-9]{14}$' then raise exception 'Invalid provider filing metadata';end if;
if exists(select 1 from research_eq10_fundamental_pit_20261020.financial_filing_metadata_v1 m where m.accession_number=a and
(m.form_type is distinct from x->>'type' or m.filing_date is distinct from to_date(x->>'filing_date','YYYYMMDD') or m.acceptance_local is distinct from x->>'acceptance_datetime' or m.cik is distinct from x#>>'{entities,0,company_data,cik}')) then raise exception 'Provider metadata conflicts with retained source';end if;
insert into research_eq10_fundamental_pit_20261020.financial_filing_metadata_v1(accession_number,cik,form_type,filing_date,report_date,acceptance_local,accepted_at,availability_bound,availability_classification,raw_payload,source_sha256,collected_at,reasons)
values(a,x#>>'{entities,0,company_data,cik}',x->>'type',to_date(x->>'filing_date','YYYYMMDD'),to_date(x->>'period_of_report_date','YYYYMMDD'),x->>'acceptance_datetime',
(to_date(left(x->>'acceptance_datetime',8),'YYYYMMDD')::text||' '||substring(x->>'acceptance_datetime',9,2)||':'||substring(x->>'acceptance_datetime',11,2)||':'||substring(x->>'acceptance_datetime',13,2))::timestamp at time zone 'America/New_York',
(to_date(x->>'filing_date','YYYYMMDD')+1)::timestamp at time zone 'America/New_York','CONDITIONAL_NEXT_FILING_DAY_BOUND',x,encode(sha256(convert_to(x::text,'UTF8')),'hex'),clock_timestamp(),ARRAY['HISTORICAL_PROVIDER_RECEIPT_UNPROVEN','DISSEMINATION_TIME_UNPROVEN']) on conflict do nothing;
end $fn$;
revoke all on function research_eq10_fundamental_pit_20261020.financial_worker_save_metadata_v1(jsonb) from public,anon,authenticated;
insert into research_eq10_fundamental_pit_20261020.financial_evidence_v1(evidence_id,evidence_type,payload)
select 'worker_json_encoding_original','INGESTION_CORRECTION_AUDIT',jsonb_build_object('documents',(select jsonb_agg(jsonb_build_object('accession',accession_number,'provider_metadata',provider_metadata)) from research_eq10_fundamental_pit_20261020.financial_documents_v1 where jsonb_typeof(provider_metadata)='string'),'events',(select jsonb_agg(to_jsonb(e)) from research_eq10_fundamental_pit_20261020.financial_worker_events_v1 e where jsonb_typeof(details)='string'),'evidence',(select jsonb_agg(to_jsonb(e)) from research_eq10_fundamental_pit_20261020.financial_evidence_v1 e where jsonb_typeof(payload)='string'));
update research_eq10_fundamental_pit_20261020.financial_documents_v1 set provider_metadata=(provider_metadata#>>'{}')::jsonb where jsonb_typeof(provider_metadata)='string';
update research_eq10_fundamental_pit_20261020.financial_worker_events_v1 set details=(details#>>'{}')::jsonb where jsonb_typeof(details)='string';
update research_eq10_fundamental_pit_20261020.financial_evidence_v1 set payload=(payload#>>'{}')::jsonb where jsonb_typeof(payload)='string';
