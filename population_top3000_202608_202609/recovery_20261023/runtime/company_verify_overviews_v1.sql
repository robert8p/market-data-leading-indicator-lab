CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.company_verify_overviews_v1()
 RETURNS jsonb
 LANGUAGE sql
 STABLE
 SET search_path TO 'pg_catalog'
AS $function$
with expected as (
 select r.ticker from population_top3000_202608_202609_v1.reference_row r
 where r.security_type in('CS','ADRC','OS','NYRS','GDR','OTHER') or r.security_type is null
), checked as (
 select e.ticker,o.source_job_id,o.snapshot_date,o.raw_record,a.sha256,a.raw_text,
 case when a.raw_text is null then null else a.raw_text::jsonb end doc
 from expected e left join population_top3000_202608_202609_v1.ticker_overview o on o.ticker=e.ticker and o.snapshot_date='2026-08-03'
 left join population_top3000_202608_202609_v1.acquisition_raw a on a.job_id=o.source_job_id
), counts as (
 select count(*) expected_overviews,count(source_job_id) committed_overviews,
 count(*) filter(where raw_text is null) missing_raw,
 count(*) filter(where sha256 is distinct from encode(pg_catalog.sha256(convert_to(raw_text,'UTF8')),'hex')) hash_mismatches,
 count(*) filter(where raw_record is distinct from doc->'results') normalized_mismatches,
 count(*) filter(where doc->'results'->>'ticker' is distinct from ticker) ticker_mismatches,
 count(*) filter(where doc->>'status' is distinct from 'OK') status_mismatches,
 encode(pg_catalog.sha256(convert_to(string_agg(ticker||':'||coalesce(sha256,'MISSING'),E'\n' order by ticker),'UTF8')),'hex') source_manifest_sha256
 from checked
)
select to_jsonb(counts)||jsonb_build_object('verifier','company-overview-independent-v1','passed',
 expected_overviews=committed_overviews and missing_raw=0 and hash_mismatches=0 and normalized_mismatches=0 and ticker_mismatches=0 and status_mismatches=0,
 'certifies_company_universe',false,'historical_receipt_inferred',false) from counts
$function$
