CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.population_read_asof_v1(p_asof timestamp with time zone, p_strict_receipt boolean DEFAULT false)
 RETURNS TABLE(publication_id uuid, family_id text, partition_key text, unit_key text, observation jsonb)
 LANGUAGE sql
 STABLE
 SET search_path TO 'pg_catalog'
AS $function$
select v.publication_id,v.family_id,v.partition_key,v.unit_key,v.observation
from population_top3000_202608_202609_v1.population_source_evidence_current_v1 v
where p_asof is not null and v.validation_state='PASS' and v.observation->>'available_at' is not null and (v.observation->>'available_at')::timestamptz<=p_asof
and v.observation->>'availability_policy' in('PUBLICATION_SUPPORTED_RECONSTRUCTION','ORIGINAL_RECEIPT_OBSERVED')
and not exists(select 1 from jsonb_each_text(v.observation->'field_state') f where f.value in('UNAVAILABLE','CONFLICTING','QUARANTINED'))
and (not p_strict_receipt or (v.observation->>'availability_policy'='ORIGINAL_RECEIPT_OBSERVED' and v.observation->>'historical_received_at' is not null and (v.observation->>'historical_received_at')::timestamptz<=p_asof));
$function$
