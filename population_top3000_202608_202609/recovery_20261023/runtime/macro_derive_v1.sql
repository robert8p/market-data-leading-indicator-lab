CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.macro_derive_v1(p_attempt uuid, p_token text)
 RETURNS jsonb
 LANGUAGE plpgsql
 SET search_path TO 'pg_catalog'
AS $function$
declare tid uuid;t population_top3000_202608_202609_v1.pipeline_task%rowtype;h text;n integer;
begin
 tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);select * into t from population_top3000_202608_202609_v1.pipeline_task where task_id=tid;
 select count(*),encode(sha256(convert_to(string_agg(unit_key||':'||record_sha256||E'\n','' order by unit_key),'UTF8')),'hex') into n,h from population_top3000_202608_202609_v1.source_observation where family_id='CTX07' and partition_key=t.partition_key;
 if n=0 then raise exception 'MACRO_SOURCE_POPULATION_MISSING';end if;
 -- Prior-release values were computed as strictly earlier headlines by the normalizer.
 -- Consensus/surprise are outside the existing official-headline dictionary and stay null.
 return jsonb_build_object('state','COMPLETE','commit_receipt',jsonb_build_object('commit_key','macro-derive:'||t.partition_key||':'||h,'input_versions',t.input_versions,'immutable_output_ids',jsonb_build_array('CTX07:'||t.partition_key),'output_sha256',h,'new_committed_units',0,'derivation','Earlier-publication prior print already materialized; no research transformations'));
end $function$
