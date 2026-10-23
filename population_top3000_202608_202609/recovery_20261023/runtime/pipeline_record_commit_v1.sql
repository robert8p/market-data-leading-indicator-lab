CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.pipeline_record_commit_v1(p_attempt uuid, p_token text, p_receipt jsonb)
 RETURNS boolean
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'pg_catalog'
AS $function$
declare tid uuid;k text;s text;n bigint;old_s text;
begin
tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);
k:=p_receipt->>'commit_key';s:=p_receipt->>'output_sha256';
if k is null or length(k)>1024 or s is null or s!~'^[0-9a-f]{64}$' or jsonb_typeof(p_receipt->'new_committed_units') is distinct from 'number' or not(p_receipt ? 'immutable_output_ids') or not(p_receipt ? 'input_versions') then raise exception 'COMMIT_RECEIPT_INVALID';end if;
n:=(p_receipt->>'new_committed_units')::bigint;if n<0 then raise exception 'COMMIT_UNITS_INVALID';end if;
if (select input_versions from population_top3000_202608_202609_v1.pipeline_task where task_id=tid) is distinct from p_receipt->'input_versions' then raise exception 'COMMIT_INPUT_VERSION_MISMATCH';end if;
select output_sha256 into old_s from population_top3000_202608_202609_v1.pipeline_commit where commit_key=k;
if found then if old_s is distinct from s then raise exception 'IMMUTABLE_COMMIT_CONFLICT';end if;return false;end if;
insert into population_top3000_202608_202609_v1.pipeline_commit(attempt_id,task_id,commit_key,output_sha256,receipt,new_committed_units) values(p_attempt,tid,k,s,p_receipt,n);
update population_top3000_202608_202609_v1.pipeline_task set useful_committed_units=useful_committed_units+n,last_useful_progress_at=case when n>0 then clock_timestamp() else last_useful_progress_at end where task_id=tid;
if n>0 then
 update population_top3000_202608_202609_v1.pipeline_control set last_useful_progress_at=clock_timestamp() where singleton;
 update population_top3000_202608_202609_v1.acquisition_control set last_useful_progress_at=clock_timestamp(),updated_at=clock_timestamp() where singleton;
end if;
return true;
end $function$
