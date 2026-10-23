CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.pipeline_finish_v1(p_attempt uuid, p_token text, p_receipt jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'pg_catalog'
AS $function$
declare tid uuid;t population_top3000_202608_202609_v1.pipeline_task%rowtype;state text;next_stage text;next_id uuid;fresh boolean;at_time timestamptz;
 a population_top3000_202608_202609_v1.pipeline_attempt%rowtype;part jsonb;v population_top3000_202608_202609_v1.pipeline_validation%rowtype;
begin
 select * into a from population_top3000_202608_202609_v1.pipeline_attempt where attempt_id=p_attempt;
 if a.token_sha256=encode(sha256(convert_to(p_token,'UTF8')),'hex') and a.outcome in('COMPLETE','CONTINUE','BLOCKED') then return jsonb_build_object('state','ALREADY_RECORDED','outcome',a.outcome);end if;
 tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);
 select * into t from population_top3000_202608_202609_v1.pipeline_task where task_id=tid;
 state:=p_receipt->>'state';
 if state is null or state not in('COMPLETE','CONTINUE','BLOCKED') then raise exception 'INVALID_FINISH_STATE';end if;
 if exists(select 1 from population_top3000_202608_202609_v1.pipeline_provider_lease where attempt_id=p_attempt and released_at is null) then raise exception 'PROVIDER_RESERVATION_MUST_BE_RECONCILED';end if;
 if state='BLOCKED' then
  if nullif(p_receipt->>'blocker_code','') is null or jsonb_typeof(p_receipt->'resumption_condition') is distinct from 'object' then raise exception 'SPECIFIC_BLOCKER_REQUIRED';end if;
  update population_top3000_202608_202609_v1.pipeline_task set status='BLOCKED',blocker_code=p_receipt->>'blocker_code',resumption_condition=p_receipt->'resumption_condition',blocker_until=nullif(p_receipt->>'recheck_at','')::timestamptz,lease_until=null where task_id=tid;
 else
  fresh:=population_top3000_202608_202609_v1.pipeline_record_commit_v1(p_attempt,p_token,p_receipt->'commit_receipt');
  if state='CONTINUE' then
   if jsonb_typeof(p_receipt->'checkpoint') is distinct from 'object' then raise exception 'CONTINUATION_CHECKPOINT_REQUIRED';end if;
   at_time:=coalesce(nullif(p_receipt->>'not_before','')::timestamptz,clock_timestamp());
   if not fresh and (p_receipt->'checkpoint') is not distinct from t.checkpoint and at_time<=clock_timestamp() then raise exception 'CONTINUATION_WITHOUT_PROGRESS_OR_WAIT';end if;
   update population_top3000_202608_202609_v1.pipeline_task set status='READY',checkpoint=p_receipt->'checkpoint',not_before=at_time,lease_until=null,same_failure_count=0 where task_id=tid;
  else
   if t.stage='PLAN' then
    if jsonb_typeof(p_receipt->'partitions') is distinct from 'array' or jsonb_array_length(p_receipt->'partitions')=0 or jsonb_array_length(p_receipt->'partitions')>1000 then raise exception 'PLAN_REQUIRES_BOUNDED_REAL_PARTITIONS';end if;
    for part in select value from jsonb_array_elements(p_receipt->'partitions')loop
     next_id:=population_top3000_202608_202609_v1.pipeline_enqueue_v1(t.family_id,'ACQUIRE',part->>'partition_key',coalesce(part->'payload','{}'),part->'input_versions',tid);
    end loop;
   elsif t.stage='VALIDATE' then
    select x.* into v from population_top3000_202608_202609_v1.pipeline_validation x join population_top3000_202608_202609_v1.pipeline_partition q on q.last_validation_id=x.validation_id
    where q.family_id=t.family_id and q.partition_key=t.partition_key and x.input_versions=t.input_versions and x.validation_id=(p_receipt->>'validation_id')::uuid;
    if not found or v.result is distinct from p_receipt->>'quality_result' then raise exception 'PERSISTED_VALIDATION_RECEIPT_REQUIRED';end if;
    next_stage:=case when v.result='PASS' or t.repair_round>=1 then 'PUBLISH' else 'REPAIR' end;
   elsif t.stage='PUBLISH' then next_stage:='DELIVER';
   elsif t.stage='REPAIR' then
    if t.repair_round>=2 then raise exception 'REPAIR_ROUND_LIMIT';end if;
    next_id:=population_top3000_202608_202609_v1.pipeline_enqueue_round_v1(t.family_id,'VALIDATE',t.partition_key,t.payload,t.input_versions,t.repair_round+1,tid);
   elsif t.stage='ACQUIRE' then next_stage:='NORMALIZE';
   elsif t.stage='NORMALIZE' then next_stage:='DERIVE';
   elsif t.stage='DERIVE' then next_stage:='VALIDATE';
   end if;
   update population_top3000_202608_202609_v1.pipeline_task set status='COMPLETE',completed_at=clock_timestamp(),lease_until=null,same_failure_count=0 where task_id=tid;
   if next_stage is not null then
    next_id:=population_top3000_202608_202609_v1.pipeline_enqueue_v1(t.family_id,next_stage,t.partition_key,t.payload,t.input_versions,tid);
    update population_top3000_202608_202609_v1.pipeline_task set repair_round=greatest(repair_round,t.repair_round) where task_id=next_id;
   end if;
  end if;
 end if;
 update population_top3000_202608_202609_v1.pipeline_attempt set outcome=state,ended_at=clock_timestamp() where attempt_id=p_attempt;
 insert into population_top3000_202608_202609_v1.pipeline_event(task_id,attempt_id,event,details)values(tid,p_attempt,state,(p_receipt-'commit_receipt')||jsonb_build_object('successor_task_id',next_id));
 return jsonb_build_object('state',state,'task_id',tid,'successor_task_id',next_id);
end $function$
