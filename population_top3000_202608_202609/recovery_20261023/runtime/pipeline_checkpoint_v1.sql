CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.pipeline_checkpoint_v1(p_attempt uuid, p_token text, p_checkpoint jsonb, p_commit_receipt jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'pg_catalog'
AS $function$
declare tid uuid;fresh boolean;
begin
tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);
if jsonb_typeof(p_checkpoint) is distinct from 'object' or octet_length(p_checkpoint::text)>65536 then raise exception 'CHECKPOINT_INVALID';end if;
fresh:=population_top3000_202608_202609_v1.pipeline_record_commit_v1(p_attempt,p_token,p_commit_receipt);
if not fresh or (p_commit_receipt->>'new_committed_units')::bigint<=0 then raise exception 'LEASE_RENEWAL_REQUIRES_NEW_USEFUL_COMMIT';end if;
update population_top3000_202608_202609_v1.pipeline_task set checkpoint=p_checkpoint,lease_until=least(clock_timestamp()+interval '90 seconds',(select admitted_until from population_top3000_202608_202609_v1.acquisition_control where singleton)) where task_id=tid;
return jsonb_build_object('state','CHECKPOINT_COMMITTED','task_id',tid);
end $function$
