CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.pipeline_tick_v1()
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'pg_catalog'
AS $function$
declare c population_top3000_202608_202609_v1.acquisition_control%rowtype;p population_top3000_202608_202609_v1.pipeline_control%rowtype;
 t population_top3000_202608_202609_v1.pipeline_task%rowtype;b population_top3000_202608_202609_v1.pipeline_adapter%rowtype;
 x record;a uuid;tok text;fn text;receipt jsonb;result jsonb;err text;errstate text;h text;wake timestamptz;ready_count bigint;hold_state text;
begin
 if not pg_try_advisory_xact_lock(2026083000261022) then return '{"state":"BUSY"}';end if;
 select * into c from population_top3000_202608_202609_v1.acquisition_control where singleton for update;
 select * into p from population_top3000_202608_202609_v1.pipeline_control where singleton for update;
 -- Never alter an unrelated scheduler job if the mission binding has changed.
 if not exists(select 1 from cron.job where jobid=596 and database=current_database()
  and btrim(command)='select population_top3000_202608_202609_v1.pipeline_tick_v1();') then
  return '{"state":"SCHEDULER_BINDING_MISMATCH"}';
 end if;
 if p.singleton is null or c.singleton is null then hold_state:='CONTROL_MISSING';
 elsif c.owner_id is distinct from p.expected_owner_id or c.owner_fence is distinct from p.expected_owner_fence or c.stop_generation is distinct from p.expected_stop_generation then hold_state:='OWNER_OR_STOP_CHANGED';
 elsif c.scope_sha256 is distinct from '8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd' or p.scope_sha256 is distinct from c.scope_sha256 then hold_state:='SCOPE_MISMATCH';
 elsif c.stopped is distinct from false or c.enabled is distinct from true or c.hold_reason is not null or p.enabled is distinct from true then hold_state:='HELD';
 end if;
 if hold_state is not null then
  -- Disarm this bound mission job without changing either owner's state, holds or fences.
  if exists(select 1 from cron.job where jobid=596 and active) then
   perform cron.alter_job(596,active=>false);
   insert into population_top3000_202608_202609_v1.pipeline_event(event,details) values('HOLD_DISARMED',
    jsonb_build_object('reason',hold_state,'owner_id',c.owner_id,'owner_fence',c.owner_fence,'stop_generation',c.stop_generation,'expected_owner_id',p.expected_owner_id,'expected_owner_fence',p.expected_owner_fence,'expected_stop_generation',p.expected_stop_generation));
  end if;
  return jsonb_build_object('state',hold_state);
 end if;
 if not p.shared_scheduler_verified then
  insert into population_top3000_202608_202609_v1.pipeline_event(event,details) values('SCHEDULER_PROBE',jsonb_build_object('backend_pid',pg_backend_pid(),'application_name',current_setting('application_name'),'database_user',current_user,'owner_id',c.owner_id,'owner_fence',c.owner_fence,'stop_generation',c.stop_generation,'no_task_executed',true));
  perform cron.alter_job(596,active=>false);
  return '{"state":"SCHEDULER_PROBE_RECORDED_AWAITING_EVIDENCE_REVIEW"}';
 end if;
 if population_top3000_202608_202609_v1.pipeline_cost_admission_v2(p,0) is distinct from true then
  if exists(select 1 from cron.job where jobid=596 and active) then
   perform cron.alter_job(596,active=>false);
   insert into population_top3000_202608_202609_v1.pipeline_event(event,details) values('HOLD_DISARMED',jsonb_build_object('reason','BUDGET_HOLD','owner_fence',c.owner_fence));
  end if;
  return '{"state":"BUDGET_HOLD"}';
 end if;
 if c.admitted_until is null or c.admitted_until<=clock_timestamp() or p.deadline is null or p.deadline<=clock_timestamp() or p.tick_count>=p.max_ticks then
  perform cron.alter_job(596,active=>false);
  insert into population_top3000_202608_202609_v1.pipeline_event(event,details) values('ADMISSION_EXPIRED_OR_TICK_LIMIT',jsonb_build_object('admitted_until',c.admitted_until,'deadline',p.deadline,'ticks',p.tick_count));
  return '{"state":"ADMISSION_EXPIRED_OR_TICK_LIMIT"}';end if;
 -- Bounded blocker clearing remains distinct from data acceptance; no user/access/unknown-effect hold is cleared.
 perform population_top3000_202608_202609_v1.continuation_dispatch_v6(c.owner_id,c.owner_fence,c.stop_generation);
 -- Advance the reviewed finite instrument date plan; exact source errors stay scoped.
 perform population_top3000_202608_202609_v1.ref01_date_continue_v9(c.owner_id,c.owner_fence,c.stop_generation);
 if p.next_wakeup_at>clock_timestamp() then return jsonb_build_object('state','SCHEDULED_WAIT','not_before',p.next_wakeup_at);end if;
 update population_top3000_202608_202609_v1.pipeline_control set tick_count=tick_count+1 where singleton;
 -- Expired source effects remain blocked. Uninvoiced provider reservations remain commitments, never inferred actual charges.
 for x in select q.*,exists(select 1 from population_top3000_202608_202609_v1.pipeline_provider_lease pl where pl.attempt_id=q.attempt_id and pl.released_at is null) as uncertain_provider_effect from population_top3000_202608_202609_v1.pipeline_task q where q.status='LEASED' and q.lease_until<=clock_timestamp() for update of q loop
  update population_top3000_202608_202609_v1.acquisition_control set reserved_bytes=greatest(0,reserved_bytes-coalesce((select sum(reserved_bytes) from population_top3000_202608_202609_v1.pipeline_provider_lease where attempt_id=x.attempt_id and released_at is null),0)) where singleton;
  if x.uncertain_provider_effect then
  if encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.pipeline_provider_expiry_accounting_v2(uuid)'::regprocedure),'UTF8')),'hex') is distinct from 'ace964d10158e91bbdcfa658dd7583961a9e8557993266ed1d2cfcb1ac041eeb' then raise exception 'EXPIRY_ACCOUNTING_HELPER_CHANGED';end if;
  perform population_top3000_202608_202609_v1.pipeline_provider_expiry_accounting_v2(x.attempt_id);
  end if;
  update population_top3000_202608_202609_v1.pipeline_attempt set outcome=case when x.uncertain_provider_effect then 'BLOCKED' else 'LEASE_EXPIRED_RECONCILED' end,ended_at=clock_timestamp() where attempt_id=x.attempt_id and outcome='RUNNING';
  update population_top3000_202608_202609_v1.pipeline_task set status=case when not x.uncertain_provider_effect and cumulative_failures+1<max_failures and (last_failure_class is distinct from 'LEASE_EXPIRED' or same_failure_count<1) then 'READY' else 'BLOCKED' end,
   cumulative_failures=cumulative_failures+1,same_failure_count=case when last_failure_class='LEASE_EXPIRED' then same_failure_count+1 else 1 end,last_failure_class=case when x.uncertain_provider_effect then 'UNCERTAIN_EFFECT' else 'LEASE_EXPIRED' end,last_failure_fingerprint=case when x.uncertain_provider_effect then 'LEASE_EXPIRED_UNCERTAIN_PROVIDER' else 'LEASE_EXPIRED' end,
   blocker_code=case when not x.uncertain_provider_effect and cumulative_failures+1<max_failures and (last_failure_class is distinct from 'LEASE_EXPIRED' or same_failure_count<1) then null else case when x.uncertain_provider_effect then 'UNCERTAIN_EFFECT' else 'LEASE_EXPIRED' end end,
   resumption_condition=case when not x.uncertain_provider_effect and cumulative_failures+1<max_failures and (last_failure_class is distinct from 'LEASE_EXPIRED' or same_failure_count<1) then null else jsonb_build_object('required_action',case when x.uncertain_provider_effect then 'Reconcile immutable output and actual billing receipts before any retry; expired provider reservation remains an uninvoiced commitment and no retry is admitted.' else 'Inspect retained source/commit hashes and worker response, apply changed approach before retry' end) end,
   lease_until=null,not_before=greatest(clock_timestamp()+interval '30 seconds',c.next_provider_at) where task_id=x.task_id;
  insert into population_top3000_202608_202609_v1.pipeline_event(task_id,attempt_id,event,details)values(x.task_id,x.attempt_id,'EXPIRED_RECONCILED',jsonb_build_object('checkpoint',x.checkpoint,'retained_commits',(select count(*) from population_top3000_202608_202609_v1.pipeline_commit where task_id=x.task_id)));
 end loop;
 -- A failed external attempt remains blocked; only its expired reservation is released.
 -- pipeline_fail_v1 preserves the original lease deadline when effects are uncertain.
 for x in select q.* from population_top3000_202608_202609_v1.pipeline_task q
  where q.status='BLOCKED' and q.blocker_code='UNCERTAIN_EFFECT'
   and q.lease_until is not null and q.lease_until<=clock_timestamp()
   and exists(select 1 from population_top3000_202608_202609_v1.pipeline_provider_lease pl where pl.attempt_id=q.attempt_id and pl.released_at is null)
  for update of q loop
  update population_top3000_202608_202609_v1.acquisition_control set reserved_bytes=reserved_bytes-coalesce((select sum(pl.reserved_bytes) from population_top3000_202608_202609_v1.pipeline_provider_lease pl where pl.attempt_id=x.attempt_id and pl.released_at is null),0) where singleton;
  if encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.pipeline_provider_expiry_accounting_v2(uuid)'::regprocedure),'UTF8')),'hex') is distinct from 'ace964d10158e91bbdcfa658dd7583961a9e8557993266ed1d2cfcb1ac041eeb' then raise exception 'EXPIRY_ACCOUNTING_HELPER_CHANGED';end if;
  perform population_top3000_202608_202609_v1.pipeline_provider_expiry_accounting_v2(x.attempt_id);
  update population_top3000_202608_202609_v1.pipeline_task set lease_until=null,
   resumption_condition=coalesce(resumption_condition,'{}'::jsonb)||jsonb_build_object('required_action','Reconcile immutable output and actual billing receipts before any retry. Operational lease ended after its original deadline; the uninvoiced monetary reservation remains retained and the task remains blocked.','operational_lease_released_at',clock_timestamp(),'billing_basis','UNINVOICED_RESERVATION_NOT_ACTUAL_CHARGE') where task_id=x.task_id;
  insert into population_top3000_202608_202609_v1.pipeline_event(task_id,attempt_id,event,details) values(x.task_id,x.attempt_id,'UNCERTAIN_RESERVATION_RECONCILED',jsonb_build_object('original_deadline',x.lease_until,'task_remains_blocked',true,'retry_admitted',false));
 end loop;
 -- Exact known no-HTTP provider waits are scheduling waits, not evidence retries.
 -- Old exhausted waits recover only at a free provider slot; all counters remain intact.
 for x in update population_top3000_202608_202609_v1.pipeline_task q
  set status='READY',blocker_until=null,blocker_code=null,resumption_condition=null
  where q.family_id='REF01' and q.stage='ACQUIRE' and q.payload->>'kind'='REF01_CENSUS_SOURCE_V1'
   and q.status='BLOCKED' and q.blocker_code='DEPENDENCY_PENDING'
   and q.resumption_condition->>'event'='PROVIDER_SLOT_AVAILABLE'
   and q.blocker_until<=clock_timestamp() and c.next_provider_at<=clock_timestamp()
   and q.owner_fence=c.owner_fence and q.stop_generation=c.stop_generation
   and q.executions<q.max_executions and q.cumulative_failures<q.max_failures
   and q.last_failure_class is distinct from 'UNCERTAIN_EFFECT'
   and not exists(select 1 from population_top3000_202608_202609_v1.pipeline_provider_lease where released_at is null)
   and exists(select 1 from population_top3000_202608_202609_v1.pipeline_attempt a where a.attempt_id=q.attempt_id and a.outcome='BLOCKED' and a.request_id is null)
   and not exists(select 1 from population_top3000_202608_202609_v1.pipeline_attempt a where a.task_id=q.task_id and a.outcome='RUNNING')
   and not exists(select 1 from population_top3000_202608_202609_v1.pipeline_provider_lease pl where pl.attempt_id=q.attempt_id)
  returning q.task_id,q.attempt_id,q.recheck_count,q.executions,q.checkpoint loop
  insert into population_top3000_202608_202609_v1.pipeline_event(event,task_id,attempt_id,details)
  values('REF01_PROVIDER_WAIT_RESUMED_V9',x.task_id,x.attempt_id,jsonb_build_object('rechecks_preserved',x.recheck_count,'executions_preserved',x.executions,'checkpoint_preserved',x.checkpoint,'basis','KNOWN_NO_HTTP_WAIT_CURRENT_FREE_PROVIDER_SLOT','family_accepted',false));
 end loop;
 -- Only explicitly timed temporary conditions are rechecked. Auth, budget, code and user holds never auto-release.
 update population_top3000_202608_202609_v1.pipeline_task q set status='READY',recheck_count=recheck_count+1,blocker_until=null,blocker_code=null,resumption_condition=null
 where q.status='BLOCKED' and q.blocker_until<=clock_timestamp() and q.recheck_count<q.max_rechecks
 and q.blocker_code in('DEPENDENCY_PENDING','RETAINED_IMPORT_PENDING','RETAINED_SEC_SOURCE_IMPORT_INCOMPLETE','ADAPTER_NOT_INSTALLED')
 and (q.family_id='REF01' and q.stage='ACQUIRE' and q.payload->>'kind'='REF01_CENSUS_SOURCE_V1' and q.resumption_condition->>'event'='PROVIDER_SLOT_AVAILABLE') IS NOT TRUE
 and exists(select 1 from population_top3000_202608_202609_v1.pipeline_adapter adapter_rows where adapter_rows.family_id=q.family_id and adapter_rows.stage=q.stage and adapter_rows.enabled);
 -- Executions are bounded independently of failure count. Do not leave exhausted work READY.
 for x in update population_top3000_202608_202609_v1.pipeline_task q
  set status='BLOCKED',blocker_code='EXECUTION_LIMIT',blocker_until=null,
   resumption_condition=jsonb_build_object('required_action','Review retained checkpoint, immutable outputs and measured batch size; admit an explicitly bounded changed approach without resetting counters.','executions',q.executions,'max_executions',q.max_executions)
  where q.status='READY' and q.executions>=q.max_executions
  returning q.task_id,q.attempt_id,q.executions,q.max_executions loop
  insert into population_top3000_202608_202609_v1.pipeline_event(task_id,attempt_id,event,details) values(x.task_id,x.attempt_id,'EXECUTION_LIMIT_BLOCKED',jsonb_build_object('executions',x.executions,'max_executions',x.max_executions));
 end loop;
 if (select count(*) from population_top3000_202608_202609_v1.pipeline_task where status='LEASED' and lease_until>clock_timestamp())>=p.max_active_tasks then return '{"state":"CONCURRENCY_LIMIT"}';end if;
 -- Least-recently-dispatched ready family first: independent bounded lanes cannot starve behind fresh cases.
 -- Aggregate retained attempts once per tick; preserve all attempt/cost/execution history and existing locks.
 -- At most four REF01 non-validation turns before a ready validation turn.
 -- History comes only from durable attempts; no task dates, counts, holds or validators are rewritten.
 with ref01_recent as materialized (
  select z.stage from population_top3000_202608_202609_v1.pipeline_attempt v
  join population_top3000_202608_202609_v1.pipeline_task z on z.task_id=v.task_id
  where z.family_id='REF01' order by v.started_at desc,v.attempt_id desc limit 4
 ), ref01_turn as materialized (
  select count(*)=4 and bool_and(stage<>'VALIDATE') as validation_due from ref01_recent
 ), family_dispatch as materialized (
  select z.family_id,max(v.started_at) as last_dispatched_at
  from population_top3000_202608_202609_v1.pipeline_attempt v
  join population_top3000_202608_202609_v1.pipeline_task z on z.task_id=v.task_id
  group by z.family_id
 )
 select q.* into t from population_top3000_202608_202609_v1.pipeline_task q
 left join population_top3000_202608_202609_v1.pipeline_adapter adapter_rows on adapter_rows.family_id=q.family_id and adapter_rows.stage=q.stage
 left join family_dispatch fair on fair.family_id=q.family_id
 cross join ref01_turn
 where q.status='READY' and q.not_before<=clock_timestamp() and q.executions<q.max_executions
 and not exists(select 1 from population_top3000_202608_202609_v1.pipeline_dependency d join population_top3000_202608_202609_v1.pipeline_task z on z.task_id=d.prerequisite_task_id where d.task_id=q.task_id and z.status<>'COMPLETE')
 and ((adapter_rows.resource_class is distinct from 'PROVIDER' and (q.family_id='REF01' and q.stage='ACQUIRE' and q.payload->>'kind'='REF01_CENSUS_SOURCE_V1') IS NOT TRUE) or (c.next_provider_at<=clock_timestamp() and not exists(select 1 from population_top3000_202608_202609_v1.pipeline_provider_lease where released_at is null)))
 order by (adapter_rows.enabled is true) desc, coalesce(fair.last_dispatched_at,'-infinity'::timestamptz),
 case when q.family_id='REF01' then
  case when ref01_turn.validation_due then case when q.stage='VALIDATE' then 0 else 1 end
   else case when q.stage='VALIDATE' then 1 else 0 end end
  else 0 end,
 case when q.family_id='REF01' and q.stage='REPAIR' then 0 else 1 end,
 case when q.family_id in('P01','P02') and q.payload->>'kind'='RETAINED_ALPACA_AUGUST_SESSION' then 20 else 0 end, q.executions,q.created_at,q.task_id for update of q skip locked limit 1;
 if not found then
  select min(q.not_before) into wake from population_top3000_202608_202609_v1.pipeline_task q where status='READY' and not_before>clock_timestamp();
  if exists(select 1 from population_top3000_202608_202609_v1.pipeline_task where status='LEASED') then wake:=least(coalesce(wake,'infinity'),clock_timestamp()+interval '5 seconds');
  elsif exists(select 1 from population_top3000_202608_202609_v1.pipeline_task where status='READY') then wake:=least(coalesce(wake,'infinity'),greatest(c.next_provider_at,clock_timestamp()+interval '30 seconds'));
  else wake:=least(coalesce(wake,'infinity'),clock_timestamp()+interval '5 minutes');end if;
  update population_top3000_202608_202609_v1.pipeline_control set next_wakeup_at=wake,idle_rechecks=idle_rechecks+1,wake_reason='WAIT_FOR_READY_TASK_OR_EVIDENCE' where singleton;
  if not exists(select 1 from population_top3000_202608_202609_v1.pipeline_task where status in('READY','LEASED')) and p.idle_rechecks>=p.max_idle_rechecks and not exists(select 1 from population_top3000_202608_202609_v1.continuation_clear_queue_v6 where status in('READY','NEEDS_REVIEW','CLAIMED','WAITING_OUTPUT') and not_after>clock_timestamp()) then
   update population_top3000_202608_202609_v1.pipeline_control set enabled=false,wake_reason='BOUNDED_WAIT_EXHAUSTED_REQUIRES_NEW_EVIDENCE' where singleton;
   perform cron.alter_job(596,schedule=>'*/5 * * * *',active=>false);
   insert into population_top3000_202608_202609_v1.pipeline_event(event,details)values('BOUNDED_WAIT_DISARMED',jsonb_build_object('resumption','New authorised evidence/adapters admitted by controller; preserve attempts and counters'));
  elsif not exists(select 1 from population_top3000_202608_202609_v1.pipeline_task where status='LEASED') then
   if exists(select 1 from population_top3000_202608_202609_v1.pipeline_task where status='READY') then perform cron.alter_job(596,schedule=>'2 seconds');
   else perform cron.alter_job(596,schedule=>'17 seconds');end if;end if;
  return jsonb_build_object('state','BOUNDED_WAIT','not_before',wake);
 end if;
 select * into b from population_top3000_202608_202609_v1.pipeline_adapter where family_id=t.family_id and stage=t.stage;
 if not found or not b.enabled then
  update population_top3000_202608_202609_v1.pipeline_task set status='BLOCKED',blocker_code='ADAPTER_NOT_INSTALLED',resumption_condition=jsonb_build_object('family',t.family_id,'stage',t.stage,'required_action','Install reviewed scoped adapter'),blocker_until=clock_timestamp()+interval '5 minutes' where task_id=t.task_id;
  return jsonb_build_object('state','ADAPTER_NOT_INSTALLED','task_id',t.task_id);
 end if;
 h:=encode(sha256(convert_to(pg_get_functiondef(b.function_oid),'UTF8')),'hex');
 if h is distinct from b.function_sha256 then
  update population_top3000_202608_202609_v1.pipeline_task set status='BLOCKED',blocker_code='CODE_INTEGRITY',resumption_condition=jsonb_build_object('required_action','Reconcile changed adapter definition and tests; bind reviewed revision') where task_id=t.task_id;return '{"state":"CODE_INTEGRITY_HOLD"}';
 end if;
 a:=gen_random_uuid();tok:=replace(gen_random_uuid()::text,'-','')||replace(gen_random_uuid()::text,'-','');
 update population_top3000_202608_202609_v1.pipeline_task set status='LEASED',attempt_id=a,lease_until=least(clock_timestamp()+interval '90 seconds',c.admitted_until),owner_fence=c.owner_fence,stop_generation=c.stop_generation,executions=executions+1,started_at=coalesce(started_at,clock_timestamp()) where task_id=t.task_id;
 insert into population_top3000_202608_202609_v1.pipeline_attempt(attempt_id,task_id,execution_number,token_sha256,owner_fence,stop_generation,adapter_sha256) values(a,t.task_id,t.executions+1,encode(sha256(convert_to(tok,'UTF8')),'hex'),c.owner_fence,c.stop_generation,b.function_sha256);
 update population_top3000_202608_202609_v1.pipeline_control set next_wakeup_at=clock_timestamp(),idle_rechecks=0,wake_reason=null where singleton;
 perform cron.alter_job(596,schedule=>'2 seconds');
 update population_top3000_202608_202609_v1.acquisition_control set next_stage='PIPELINE_EXECUTION',next_action=t.family_id||'/'||t.stage||'/'||t.partition_key where singleton;
 select format('%I.%I',n.nspname,r.proname) into fn from pg_proc r join pg_namespace n on n.oid=r.pronamespace where r.oid=b.function_oid and r.pronargs=2 and r.proargtypes[0]='uuid'::regtype and r.proargtypes[1]='text'::regtype;
 begin
  if fn is null then raise exception 'INVALID_ADAPTER_SIGNATURE';end if;
  execute format('select %s($1,$2)',fn) into receipt using a,tok;
  if receipt->>'state'='ASYNC' then result:=receipt;
  elsif (select outcome from population_top3000_202608_202609_v1.pipeline_attempt where attempt_id=a)<>'RUNNING' then result:=receipt;
  else result:=population_top3000_202608_202609_v1.pipeline_finish_v1(a,tok,receipt);end if;
 exception
 when query_canceled then
  -- The adapter/finish subtransaction has rolled back. Preserve the outer attempt and failure receipt.
  -- Non-DATABASE cancellation retains its existing external-effect handling.
  if b.resource_class is distinct from 'DATABASE' then raise;end if;
  result:=population_top3000_202608_202609_v1.pipeline_record_database_cancel_v1(a,tok);
 when others then
  get stacked diagnostics err=MESSAGE_TEXT,errstate=RETURNED_SQLSTATE;
  result:=population_top3000_202608_202609_v1.pipeline_fail_v1(a,tok,case when errstate in('55P03','40P01') then 'LOCK' else 'PARSER' end,errstate||':'||left(err,150),jsonb_build_object('sqlstate',errstate,'message',left(err,500)),60);
 end;
 insert into population_top3000_202608_202609_v1.pipeline_event(task_id,attempt_id,event,details)values(t.task_id,a,'DISPATCHED',jsonb_build_object('family',t.family_id,'stage',t.stage,'partition',t.partition_key,'resource',b.resource_class,'result',result));
 return jsonb_build_object('state','DISPATCHED','task_id',t.task_id,'attempt_id',a,'result',result);
end $function$

