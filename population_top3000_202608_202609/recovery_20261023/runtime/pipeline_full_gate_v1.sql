CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.pipeline_full_gate_v1()
 RETURNS jsonb
 LANGUAGE sql
 STABLE
 SET search_path TO 'pg_catalog'
AS $function$
with facts as(select
 (select count(*) from population_top3000_202608_202609_v1.catalog_family_contract) contract_count,
 (select count(*) from population_top3000_202608_202609_v1.pipeline_company) company_count,
 (select count(*) from population_top3000_202608_202609_v1.pipeline_instrument) instrument_count,
 (select count(*) from population_top3000_202608_202609_v1.pipeline_exception where resolved_at is null) unresolved_exceptions,
 (select count(*) from population_top3000_202608_202609_v1.pipeline_task where status='BLOCKED') blocked_tasks,
 not exists(select 1 from population_top3000_202608_202609_v1.catalog_family_contract c left join population_top3000_202608_202609_v1.pipeline_family_binding b using(family_id)
 where b.family_id is null or not b.partition_manifest_frozen or b.expected_partition_count is null or b.partition_manifest_sha256 is null
 or b.expected_partition_count<>(select count(*) from population_top3000_202608_202609_v1.pipeline_partition p where p.family_id=c.family_id)
 or exists(select 1 from population_top3000_202608_202609_v1.pipeline_partition p left join population_top3000_202608_202609_v1.pipeline_validation v on v.validation_id=p.last_validation_id where p.family_id=c.family_id and (p.denominator_state<>'FROZEN' or not p.publication_eligible or v.result is distinct from 'PASS'))) all_families_pass,
 exists(select 1 from population_top3000_202608_202609_v1.pipeline_universe where singleton and status='FROZEN' and company_count=3000 and unresolved_cutoff_cases=0 and unresolved_instrument_cases=0 and company_manifest_sha256 is not null and instrument_manifest_sha256 is not null and verification_evidence is not null) universe_pass)
select to_jsonb(facts)||jsonb_build_object('full_gate_pass',contract_count=24 and company_count=3000 and instrument_count>=3000 and unresolved_exceptions=0 and blocked_tasks=0 and all_families_pass and universe_pass) from facts;
$function$
