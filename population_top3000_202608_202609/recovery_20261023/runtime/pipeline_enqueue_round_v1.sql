CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.pipeline_enqueue_round_v1(p_family text, p_stage text, p_partition text, p_payload jsonb, p_input_versions jsonb, p_round integer, p_dependency uuid)
 RETURNS uuid
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'pg_catalog'
AS $function$
declare tid uuid;k text;
begin
 if p_round is null or p_round<1 or p_round>2 or p_stage<>'VALIDATE' then raise exception 'BOUNDED_REVERIFICATION_ONLY';end if;
 if not exists(select 1 from population_top3000_202608_202609_v1.catalog_family_contract where family_id=p_family) then raise exception 'OUT_OF_SCOPE_FAMILY';end if;
 if p_partition is null or jsonb_typeof(p_input_versions) is distinct from 'object' then raise exception 'TASK_SCOPE_INVALID';end if;
 k:=p_family||':'||p_stage||':'||p_partition||':'||encode(sha256(convert_to(p_input_versions::text,'UTF8')),'hex')||':quality-round:'||p_round::text;
 insert into population_top3000_202608_202609_v1.pipeline_task(operation_key,family_id,stage,partition_key,payload,input_versions,repair_round)
 values(k,p_family,p_stage,p_partition,p_payload,p_input_versions,p_round)on conflict(operation_key)do nothing;
 select task_id into tid from population_top3000_202608_202609_v1.pipeline_task where operation_key=k;
 if p_dependency is not null then insert into population_top3000_202608_202609_v1.pipeline_dependency(task_id,prerequisite_task_id)values(tid,p_dependency)on conflict do nothing;end if;
 return tid;
end $function$
