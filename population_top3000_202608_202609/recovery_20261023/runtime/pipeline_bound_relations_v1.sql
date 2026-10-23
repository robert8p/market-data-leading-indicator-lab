CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.pipeline_bound_relations_v1(p_family text)
 RETURNS jsonb
 LANGUAGE plpgsql
 STABLE
 SET search_path TO 'pg_catalog'
AS $function$
declare b population_top3000_202608_202609_v1.pipeline_family_binding%rowtype;e text;o text;
begin
 select * into b from population_top3000_202608_202609_v1.pipeline_family_binding where family_id=p_family;
 if not found then return jsonb_build_object('state','BINDING_MISSING');end if;
 select format('%I.%I',n.nspname,c.relname) into e from pg_class c join pg_namespace n on n.oid=c.relnamespace where c.oid=b.expected_relation and n.nspname='population_top3000_202608_202609_v1' and c.relkind in('r','p','v','m');
 select format('%I.%I',n.nspname,c.relname) into o from pg_class c join pg_namespace n on n.oid=c.relnamespace where c.oid=b.observed_relation and n.nspname='population_top3000_202608_202609_v1' and c.relkind in('r','p','v','m');
 if e is null or o is null then return jsonb_build_object('state','BOUND_RELATION_OUTSIDE_MISSION');end if;
 if population_top3000_202608_202609_v1.pipeline_relation_hash_v1(b.expected_relation) is distinct from b.expected_definition_sha256 or population_top3000_202608_202609_v1.pipeline_relation_hash_v1(b.observed_relation) is distinct from b.observed_definition_sha256 then return jsonb_build_object('state','BOUND_DEFINITION_CHANGED');end if;
 return jsonb_build_object('state','BOUND','expected',e,'observed',o,'binding_version',b.binding_version);
end $function$
