CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.pipeline_fail_v1(p_attempt uuid, p_token text, p_class text, p_fingerprint text, p_details jsonb, p_retry_after_seconds integer DEFAULT 60)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'pg_catalog'
AS $function$
declare tid uuid;t population_top3000_202608_202609_v1.pipeline_task%rowtype;same integer;retry boolean;
begin
tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);
select * into t from population_top3000_202608_202609_v1.pipeline_task where task_id=tid;
if p_class is null or p_class not in('RATE_LIMIT','LOCK','TRANSPORT_READ','UNCERTAIN_EFFECT','AUTHENTICATION','OWNERSHIP','CODE_INTEGRITY','PARSER','SOURCE_ABSENT','QUALITY','BUDGET','LEASE_EXPIRED') or nullif(p_fingerprint,'') is null or p_retry_after_seconds is null or p_retry_after_seconds<0 then raise exception 'DIAGNOSED_FAILURE_REQUIRED';end if;
same:=case when t.last_failure_fingerprint=p_fingerprint then t.same_failure_count+1 else 1 end;
retry:=p_class in('RATE_LIMIT','LOCK','TRANSPORT_READ') and same<2 and t.cumulative_failures+1<t.max_failures;
if exists(select 1 from population_top3000_202608_202609_v1.pipeline_provider_lease where attempt_id=p_attempt and released_at is null) then retry:=false;p_class:='UNCERTAIN_EFFECT';end if;
if p_class='RATE_LIMIT' then update population_top3000_202608_202609_v1.acquisition_control set next_provider_at=greatest(next_provider_at,clock_timestamp()+make_interval(secs=>p_retry_after_seconds)) where singleton;end if;
update population_top3000_202608_202609_v1.pipeline_task set status=case when retry then 'READY' else 'BLOCKED' end,cumulative_failures=cumulative_failures+1,same_failure_count=same,last_failure_class=p_class,last_failure_fingerprint=p_fingerprint,lease_until=case when p_class='UNCERTAIN_EFFECT' and exists(select 1 from population_top3000_202608_202609_v1.pipeline_provider_lease pl where pl.attempt_id=p_attempt and pl.released_at is null) then lease_until else null end,not_before=clock_timestamp()+make_interval(secs=>p_retry_after_seconds),blocker_code=case when retry then null else p_class end,resumption_condition=case when retry then null else jsonb_build_object('required_action',case p_class when 'UNCERTAIN_EFFECT' then 'Reconcile immutable output and billing receipts before retry.' when 'SOURCE_ABSENT' then 'Supply historical source or bounded reviewed alternate-source adapter.' when 'QUALITY' then 'Deploy diagnosed targeted repair with new input revision and reverify.' else 'Diagnose cause and apply reviewed changed approach; do not reset counters.'end,'details',p_details)end where task_id=tid;
update population_top3000_202608_202609_v1.pipeline_attempt set outcome=case when retry then 'RETRY_SCHEDULED' else 'BLOCKED' end,ended_at=clock_timestamp() where attempt_id=p_attempt;
insert into population_top3000_202608_202609_v1.pipeline_event(task_id,attempt_id,event,details)values(tid,p_attempt,'FAILURE',jsonb_build_object('class',p_class,'fingerprint',p_fingerprint,'same_count',same,'retry',retry,'details',p_details));
return jsonb_build_object('state',case when retry then 'RETRY_SCHEDULED' else 'BLOCKED' end);
end $function$
