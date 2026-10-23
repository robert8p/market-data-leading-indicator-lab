CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.pipeline_relation_hash_v1(p_relation oid)
 RETURNS text
 LANGUAGE sql
 STABLE
 SET search_path TO 'pg_catalog'
AS $function$
select encode(sha256(convert_to(jsonb_build_object('namespace',n.nspname,'name',c.relname,'kind',c.relkind,
 'columns',(select jsonb_agg(jsonb_build_array(a.attnum,a.attname,format_type(a.atttypid,a.atttypmod),a.attnotnull) order by a.attnum) from pg_attribute a where a.attrelid=c.oid and a.attnum>0 and not a.attisdropped),
 'view',case when c.relkind in('v','m') then pg_get_viewdef(c.oid,true) else null end)::text,'UTF8')),'hex')
from pg_class c join pg_namespace n on n.oid=c.relnamespace where c.oid=p_relation;
$function$
