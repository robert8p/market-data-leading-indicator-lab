CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.verify_census_v1()
 RETURNS jsonb
 LANGUAGE sql
 STABLE
 SET search_path TO 'pg_catalog'
AS $function$
with pages as (
 select j.job_id,j.page_number,j.status,j.params,r.raw_text::jsonb body,r.sha256,r.source_bytes,r.evidence,
 encode(sha256(convert_to(r.raw_text,'UTF8')),'hex') as recomputed_hash
 from population_top3000_202608_202609_v1.acquisition_job j left join population_top3000_202608_202609_v1.acquisition_raw r using(job_id) where j.kind='CENSUS'
), counts as (
 select count(*) pages,count(*) filter(where status='COMPLETE') complete_pages,
 count(*) filter(where sha256 is distinct from recomputed_hash) bad_hash,
 coalesce(sum(jsonb_array_length(body->'results')),0) raw_rows,
 count(*) filter(where nullif(body->>'next_url','') is null and body is not null and status='COMPLETE') terminal_pages,
 count(*)-count(distinct page_number) duplicate_page_numbers,
 min(page_number) first_page,max(page_number) last_page,
 count(*) filter(where body is null) missing_source_pages from pages
), records as (
 select count(*) normalized_rows,count(distinct ticker) unique_tickers,
 count(*)-count(distinct ticker) duplicate_ticker_rows,
 count(*) filter(where ticker is null or security_type is null) missing_identity_fields,
 count(*) filter(where security_type in('CS','ADRC','OS','NYRS','GDR','OTHER') or security_type is null) candidate_security_rows
 from population_top3000_202608_202609_v1.reference_row
), links as (
 select count(*) mismatches from population_top3000_202608_202609_v1.reference_row r join pages p on p.job_id=r.page_job_id where r.raw_record is distinct from p.body->'results'->(r.ordinal-1) or r.ticker is distinct from r.raw_record->>'ticker' or r.security_type is distinct from r.raw_record->>'type' or r.ordinal<1 or r.ordinal>jsonb_array_length(p.body->'results')
), chain as (
 select count(*) mismatches from pages p left join pages successor on successor.page_number=p.page_number+1
 where (nullif(p.evidence->>'next_cursor','') is not null and (successor.job_id is null or successor.params->>'cursor' is distinct from p.evidence->>'next_cursor'))
 or(nullif(p.body->>'next_url','') is not null and nullif(p.evidence->>'next_cursor','') is null)
 or(nullif(p.body->>'next_url','') is null and successor.job_id is not null)
 or population_top3000_202608_202609_v1.query_cursor_v1(p.body->>'next_url') is distinct from nullif(p.evidence->>'next_cursor','')
)
select jsonb_build_object('verification','INDEPENDENT_RAW_PAGE_RECONCILIATION_V1','observed_at',now(),'pages',to_jsonb(c),'records',to_jsonb(r),'raw_record_mismatches',l.mismatches,'page_chain_mismatches',ch.mismatches,
'census_page_reconciliation_pass',c.complete_pages=c.pages and c.pages>0 and c.first_page=1 and c.last_page=c.pages and c.terminal_pages=1 and c.bad_hash=0 and c.missing_source_pages=0 and c.duplicate_page_numbers=0 and c.raw_rows=r.normalized_rows and r.duplicate_ticker_rows=0 and l.mismatches=0 and ch.mismatches=0,
'type_counts',(select jsonb_object_agg(coalesce(security_type,'MISSING'),n) from(select security_type,count(*) n from population_top3000_202608_202609_v1.reference_row group by security_type)t),
'company_universe_certified',false,'limitations','Verifies retained provider page structure and normalization only. Company identities, full eligible-company census, caps, ADR/class semantics, independent source comparison and top3000 cutoff remain unresolved.') from counts c cross join records r cross join links l cross join chain ch;
$function$
