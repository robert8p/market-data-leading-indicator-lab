CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.macro_staging_verify_v1()
 RETURNS jsonb
 LANGUAGE sql
 STABLE
 SET search_path TO 'pg_catalog'
AS $function$
select jsonb_build_object('observations',count(*),'source_registry',(select count(*) from population_top3000_202608_202609_v1.macro_source_registry),
 'manifest_sha256',encode(sha256(convert_to(string_agg(observation_id||':'||payload_sha256||E'\n','' order by observation_id),'UTF8')),'hex'),
 'bad_payload_hashes',count(*) filter(where payload_sha256<>encode(sha256(convert_to(payload_text,'UTF8')),'hex') or payload<>payload_text::jsonb),
 'bad_source_hashes',(select count(*) from population_top3000_202608_202609_v1.macro_source_registry where payload_sha256<>encode(sha256(convert_to(payload_text,'UTF8')),'hex') or payload<>payload_text::jsonb),
 'invented_receipts',count(*) filter(where actual_historical_receipt is not null),
 'typed_headline_mismatches',count(*) filter(where record_role in('headline','revision') and
  (actual is distinct from (payload->>'actual')::numeric or released_at is distinct from (payload->>'released_at')::timestamptz or reference_period is distinct from (payload->>'reference_period')::timestamptz or factor_name is distinct from payload->>'factor_name')),
 'bad_time_order',count(*) filter(where available_at<released_at),
 'publication_reconstruction_only',true,'denominator_frozen',false)
from population_top3000_202608_202609_v1.macro_observations;
$function$
