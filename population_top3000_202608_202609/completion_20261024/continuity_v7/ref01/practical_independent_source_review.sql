with eligible as materialized (
select s.* from population_top3000_202608_202609_v1.ref01_security_decision_v2 s
join population_top3000_202608_202609_v1.ref01_adjudication_head_v3 h on h.case_key=s.case_key and h.run_key='ref01-company-adjudication-v3-20261023'
join population_top3000_202608_202609_v1.ref01_adjudication_case_v3 c on c.run_key=h.run_key and c.case_key=h.case_key and c.revision_key=h.revision_key
where s.run_key='ref01-company-reconciliation-v2-20261023' and s.eligibility_state='SOURCE_SUPPORTED_COMMON' and s.company_identity_state='SOURCE_SUPPORTED_ISSUER'
and s.security_identity_state='DATED_NATIVE_IDENTIFIERS' and s.projection->>'native_type' in ('CS','ADRC') and s.projection->>'primary_exchange' in ('XNYS','XNAS','XASE','ARCX','BATS','IEXG')
and s.projection->>'historical_active'='true' and s.projection->>'rank_date'='2026-08-03' and s.case_key='CIK:'||(s.projection->>'native_cik') and s.projection->>'native_cik' ~ '^[0-9]{10}$'
and (s.projection->>'vendor_company_cap_usd')::numeric>0 and c.decision in ('ELIGIBLE','REVIEW_REQUIRED')
and not exists(select 1 from jsonb_array_elements_text(c.content->'unresolved_codes') u where u not in ('ADR_GLOBAL_CAP_AND_RATIO_RECONCILIATION','RANK_CLOSE_UNAVAILABLE_OR_AMBIGUOUS','MISSING_DATED_COMPANY_CAP','MISSING_COMPANY_EQUIVALENT_SHARES'))
), companies as (select case_key,max((projection->>'vendor_company_cap_usd')::numeric) cap from eligible group by 1 order by cap desc,case_key collate "C" limit 3000),
selected as materialized (select s.* from eligible s join companies using(case_key)),
raws as materialized(select a.job_id,a.sha256,a.evidence,a.received_at,a.raw_text,encode(sha256(convert_to(a.raw_text,'UTF8')),'hex') actual_sha from population_top3000_202608_202609_v1.acquisition_raw a
where a.job_id in(select projection->>'census_job_id' from selected union select projection->>'overview_job_id' from selected)),
census as materialized(select r.job_id,x.value rec,x.ordinality from raws r cross join lateral jsonb_array_elements(r.raw_text::jsonb->'results') with ordinality x where r.job_id like 'census:%'),
observed as materialized(select s.*, cr.rec census_record,ov.raw_text::jsonb->'results' overview_record,
crr.actual_sha census_sha,ov.actual_sha overview_sha,crr.sha256 retained_census_sha,ov.sha256 retained_overview_sha,
crr.evidence#>>'{request_params,date}' census_date,ov.evidence#>>'{request_params,date}' overview_date
from selected s left join census cr on cr.job_id=s.projection->>'census_job_id' and cr.ordinality=(s.projection->>'census_ordinal')::int
left join raws crr on crr.job_id=cr.job_id left join raws ov on ov.job_id=s.projection->>'overview_job_id')
select now() checked_at,(select count(*) from companies) companies,count(*) instruments,
(select count(*) from raws) source_bodies_rehashed,
count(*) filter(where census_sha is distinct from retained_census_sha or overview_sha is distinct from retained_overview_sha) body_hash_mismatches,
count(*) filter(where encode(sha256(convert_to(census_record::text,'UTF8')),'hex') is distinct from projection->>'census_record_sha256') census_record_mismatches,
count(*) filter(where encode(sha256(convert_to(overview_record::text,'UTF8')),'hex') is distinct from projection->>'overview_record_sha256') overview_record_mismatches,
count(*) filter(where census_record->>'ticker' is distinct from ticker or overview_record->>'ticker' is distinct from ticker
or census_record->>'cik' is distinct from projection->>'native_cik' or overview_record->>'cik' is distinct from projection->>'native_cik'
or census_record->>'type' is distinct from projection->>'native_type' or overview_record->>'type' is distinct from projection->>'native_type'
or census_record->>'primary_exchange' is distinct from projection->>'primary_exchange' or overview_record->>'primary_exchange' is distinct from projection->>'primary_exchange'
or census_record->>'active' is distinct from 'true' or overview_record->>'active' is distinct from 'true') identity_listing_field_mismatches,
count(*) filter(where (overview_record->>'market_cap')::numeric is distinct from (projection->>'vendor_company_cap_usd')::numeric) proxy_field_mismatches,
count(*) filter(where census_date is distinct from '2026-08-03' or overview_date is distinct from '2026-08-03') source_request_date_mismatches
from observed;
