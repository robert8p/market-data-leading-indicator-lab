CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.pipeline_measure_partition_v1(p_family text, p_partition text)
 RETURNS jsonb
 LANGUAGE plpgsql
 STABLE
 SET search_path TO 'pg_catalog'
AS $function$
declare b jsonb;m jsonb;q text;
begin
 b:=population_top3000_202608_202609_v1.pipeline_bound_relations_v1(p_family);
 if b->>'state'<>'BOUND' then return b;end if;
 q:=format($sql$
 with e as materialized(select x.*,to_jsonb(x) whole from %s x where release_id='top3000_companies_202608_202609_v1' and family_id=$1 and partition_key=$2),
 o as materialized(select x.*,to_jsonb(x) whole from %s x where release_id='top3000_companies_202608_202609_v1' and family_id=$1 and partition_key=$2),
 source_integrity as materialized(
  select d.source_id,(d.content_text is null or d.source_sha256=encode(sha256(convert_to(d.content_text,'UTF8')),'hex')) integrity_ok
  from population_top3000_202608_202609_v1.source_document d
  join (select distinct source_id from o) source_keys on source_keys.source_id=d.source_id
 ),
 fields as(select e.unit_key,f.name,o.unit_key observed_key,o.values_json->f.name val,o.field_state->>f.name state,o.lineage,
  o.availability_policy,o.historical_received_at,
  coalesce((e.applicability_evidence->>'strict_receipt_required')::boolean,false) strict_required
  from e cross join lateral unnest(e.required_fields) f(name) left join o using(unit_key)),
 unit_counts as(select (select count(*) from e) expected_units,(select count(*) from o) received_units,
  (select count(*)-count(distinct unit_key) from e) duplicate_expected_units,(select count(*)-count(distinct unit_key) from o) duplicate_observed_units,
  (select count(*) from e left join o using(unit_key) where o.unit_key is null) missing_units,
  (select count(*) from o left join e using(unit_key) where e.unit_key is null) unexpected_units,
  (select encode(sha256(convert_to(coalesce(string_agg(whole::text,E'\n' order by unit_key),''),'UTF8')),'hex') from e) expected_manifest_sha256,
  (select encode(sha256(convert_to(coalesce(string_agg(whole::text,E'\n' order by unit_key),''),'UTF8')),'hex') from o) observed_manifest_sha256,
  (select count(*) from e where cardinality(required_fields)=0 or time_role not in('TARGET','SUPPORT') or not exists(select 1 from population_top3000_202608_202609_v1.source_document d where d.source_id=e.expectation_source_id))+
  (select count(*) from o where jsonb_typeof(values_json) is distinct from 'object' or jsonb_typeof(field_state) is distinct from 'object'
    or record_sha256 is distinct from encode(sha256(convert_to(jsonb_build_object('values',values_json,'states',field_state,'lineage',lineage)::text,'UTF8')),'hex')
    or not exists(select 1 from source_integrity d where d.source_id=o.source_id and d.integrity_ok)) structural_failures,
  (select count(*) from e join o using(unit_key) where e.company_id is distinct from o.company_id or e.security_id is distinct from o.security_id or e.reporting_entity_id is distinct from o.reporting_entity_id or e.time_role is distinct from o.time_role) membership_failures,
  (select count(*) from o where availability_policy not in('ARCHIVED_OBSERVATION','PUBLICATION_SUPPORTED_RECONSTRUCTION','ORIGINAL_RECEIPT_OBSERVED','CONSERVATIVE_ASSUMPTION','UNKNOWN')
   or (availability_policy='ORIGINAL_RECEIPT_OBSERVED' and (historical_received_at is null or available_at is null or available_at<historical_received_at))
   or (availability_policy='PUBLICATION_SUPPORTED_RECONSTRUCTION' and (source_published_at is null or available_at is null or available_at<source_published_at))
   or (available_at is not null and source_published_at is not null and available_at<source_published_at)
   or (historical_received_at is not null and historical_received_at>ingested_at)) temporal_failures),
 field_counts as(select count(*) required_fields,
  count(*) filter(where observed_key is not null and state in('VERIFIED_VALUE','VALUE','PRESENT') and val is not null and val<>'null'::jsonb) verified_value_fields,
  count(*) filter(where observed_key is not null and state='NOT_APPLICABLE' and coalesce(lineage->'field_evidence'->name->>'reason','')<>'' and lineage->'field_evidence'->name ? 'source_id') not_applicable_fields,
  count(*) filter(where observed_key is not null and state='PUBLISHED_MISSING' and lineage->'field_evidence'->name ? 'source_id') published_missing_fields,
  count(*) filter(where state='UNAVAILABLE' or (strict_required and historical_received_at is null)) unavailable_fields,
  count(*) filter(where state='CONFLICTING') conflicting_fields,count(*) filter(where state='QUARANTINED') quarantined_fields,
  count(*) filter(where observed_key is null or state is null or state not in('VERIFIED_VALUE','VALUE','PRESENT','NOT_APPLICABLE','PUBLISHED_MISSING','UNAVAILABLE','CONFLICTING','QUARANTINED')
   or (state in('VERIFIED_VALUE','VALUE','PRESENT') and (val is null or val='null'::jsonb))
   or (state='NOT_APPLICABLE' and not(coalesce(lineage->'field_evidence'->name->>'reason','')<>'' and coalesce(lineage->'field_evidence'->name ? 'source_id',false)))
   or (state='PUBLISHED_MISSING' and not coalesce(lineage->'field_evidence'->name ? 'source_id',false))) missing_required_fields,
  count(*) filter(where observed_key is not null and historical_received_at is not null and availability_policy='ORIGINAL_RECEIPT_OBSERVED' and state in('VERIFIED_VALUE','VALUE','PRESENT') and val is not null and val<>'null'::jsonb) strict_receipt_fields from fields)
 select to_jsonb(unit_counts)||to_jsonb(field_counts)||jsonb_build_object('state','MEASURED') from unit_counts cross join field_counts
 $sql$,b->>'expected',b->>'observed');
 execute q into m using p_family,p_partition;
 return m;
end $function$
;
