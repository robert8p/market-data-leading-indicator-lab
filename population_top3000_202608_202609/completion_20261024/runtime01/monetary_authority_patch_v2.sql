CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.pipeline_cost_admission_v2(
 p_control population_top3000_202608_202609_v1.pipeline_control,
 p_incremental_usd numeric DEFAULT 0)
RETURNS boolean LANGUAGE plpgsql STABLE SET search_path TO 'pg_catalog' AS $function$
DECLARE m jsonb; c population_top3000_202608_202609_v1.acquisition_control%rowtype;
 total numeric; e population_top3000_202608_202609_v1.pipeline_event%rowtype;
BEGIN
 IF p_control.budget_verified_at IS NULL OR jsonb_typeof(p_control.budget_evidence) IS DISTINCT FROM 'object'
  OR p_incremental_usd IS NULL OR p_incremental_usd<0 OR p_incremental_usd::text IN ('NaN','Infinity','-Infinity') THEN RETURN false; END IF;
 IF EXISTS (SELECT 1 FROM unnest(ARRAY[p_control.actual_usd,p_control.accrued_usd,p_control.unavoidable_usd,
 p_control.remaining_uncommitted_usd,p_control.repair_egress_reserve_usd,p_control.reserved_request_usd]) AS v(amount)
 WHERE amount IS NULL OR amount<0 OR amount::text IN ('NaN','Infinity','-Infinity')) THEN RETURN false; END IF;
 total:=p_control.actual_usd+p_control.accrued_usd+p_control.unavoidable_usd+p_control.remaining_uncommitted_usd
  +p_control.repair_egress_reserve_usd+p_control.reserved_request_usd+p_incremental_usd;
 m:=p_control.budget_evidence->'monetary_policy';
 IF m->>'mode'='UNCAPPED_AUTHORIZED' THEN
  IF coalesce(m->>'authority_event_id','') !~ '^[0-9]{1,18}$' THEN RETURN false; END IF;
  SELECT * INTO c FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton;
  IF NOT FOUND OR c.owner_id IS DISTINCT FROM p_control.expected_owner_id OR c.owner_fence IS DISTINCT FROM p_control.expected_owner_fence
   OR c.stop_generation IS DISTINCT FROM p_control.expected_stop_generation OR c.scope_sha256 IS DISTINCT FROM p_control.scope_sha256
   OR m->>'owner_id' IS DISTINCT FROM c.owner_id OR m->>'owner_fence' IS DISTINCT FROM c.owner_fence::text
   OR m->>'stop_generation' IS DISTINCT FROM c.stop_generation::text OR m->>'scope_sha256' IS DISTINCT FROM c.scope_sha256 THEN RETURN false; END IF;
  SELECT * INTO e FROM population_top3000_202608_202609_v1.pipeline_event WHERE event_id=(m->>'authority_event_id')::bigint;
  RETURN FOUND AND e.event='USER_AUTHORITY_RESUMED_20261024' AND e.details->>'monetary_cap_removed'='true'
   AND e.details->>'owner_id'=c.owner_id AND e.details->>'owner_fence'=c.owner_fence::text
   AND e.details->>'stop_generation'=c.stop_generation::text AND e.details->>'scope_sha256'=c.scope_sha256;
 END IF;
 IF m IS NOT NULL AND m<>'null'::jsonb AND m->>'mode' IS DISTINCT FROM 'CAPPED' THEN RETURN false; END IF;
 RETURN p_control.budget_cap_usd IS NOT NULL AND p_control.budget_cap_usd>=0
  AND p_control.budget_cap_usd::text NOT IN ('NaN','Infinity','-Infinity') AND total<=p_control.budget_cap_usd;
END $function$;
REVOKE ALL ON FUNCTION population_top3000_202608_202609_v1.pipeline_cost_admission_v2(population_top3000_202608_202609_v1.pipeline_control,numeric) FROM PUBLIC,anon,authenticated;
COMMENT ON COLUMN population_top3000_202608_202609_v1.pipeline_control.budget_cap_usd IS 'Historical capped-policy amount. Not an active monetary ceiling when versioned UNCAPPED_AUTHORIZED authority is valid.';
ALTER TABLE population_top3000_202608_202609_v1.pipeline_control DROP CONSTRAINT pipeline_control_check;
ALTER TABLE population_top3000_202608_202609_v1.pipeline_control ADD CONSTRAINT pipeline_control_check CHECK(
 coalesce((budget_evidence#>>'{monetary_policy,mode}')='UNCAPPED_AUTHORIZED',false) OR
 actual_usd+accrued_usd+unavoidable_usd+remaining_uncommitted_usd+repair_egress_reserve_usd+reserved_request_usd<=budget_cap_usd);


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
 if p.next_wakeup_at>clock_timestamp() then return jsonb_build_object('state','SCHEDULED_WAIT','not_before',p.next_wakeup_at);end if;
 update population_top3000_202608_202609_v1.pipeline_control set tick_count=tick_count+1 where singleton;
 -- Expired effects are reconciled by immutable receipts. A reserved provider charge is conservatively accrued.
 for x in select q.*,exists(select 1 from population_top3000_202608_202609_v1.pipeline_provider_lease pl where pl.attempt_id=q.attempt_id and pl.released_at is null) as uncertain_provider_effect from population_top3000_202608_202609_v1.pipeline_task q where q.status='LEASED' and q.lease_until<=clock_timestamp() for update of q loop
  update population_top3000_202608_202609_v1.pipeline_control set accrued_usd=accrued_usd+coalesce((select sum(reserved_usd) from population_top3000_202608_202609_v1.pipeline_provider_lease where attempt_id=x.attempt_id and released_at is null),0),reserved_request_usd=reserved_request_usd-coalesce((select sum(reserved_usd) from population_top3000_202608_202609_v1.pipeline_provider_lease where attempt_id=x.attempt_id and released_at is null),0) where singleton;
  update population_top3000_202608_202609_v1.acquisition_control set reserved_bytes=greatest(0,reserved_bytes-coalesce((select sum(reserved_bytes) from population_top3000_202608_202609_v1.pipeline_provider_lease where attempt_id=x.attempt_id and released_at is null),0)) where singleton;
  update population_top3000_202608_202609_v1.pipeline_provider_lease set released_at=clock_timestamp(),actual_usd=reserved_usd,outcome='EXPIRED_CONSERVATIVELY_ACCRUED' where attempt_id=x.attempt_id and released_at is null;
  update population_top3000_202608_202609_v1.pipeline_attempt set outcome=case when x.uncertain_provider_effect then 'BLOCKED' else 'LEASE_EXPIRED_RECONCILED' end,ended_at=clock_timestamp() where attempt_id=x.attempt_id and outcome='RUNNING';
  update population_top3000_202608_202609_v1.pipeline_task set status=case when not x.uncertain_provider_effect and cumulative_failures+1<max_failures and (last_failure_class is distinct from 'LEASE_EXPIRED' or same_failure_count<1) then 'READY' else 'BLOCKED' end,
   cumulative_failures=cumulative_failures+1,same_failure_count=case when last_failure_class='LEASE_EXPIRED' then same_failure_count+1 else 1 end,last_failure_class=case when x.uncertain_provider_effect then 'UNCERTAIN_EFFECT' else 'LEASE_EXPIRED' end,last_failure_fingerprint=case when x.uncertain_provider_effect then 'LEASE_EXPIRED_UNCERTAIN_PROVIDER' else 'LEASE_EXPIRED' end,
   blocker_code=case when not x.uncertain_provider_effect and cumulative_failures+1<max_failures and (last_failure_class is distinct from 'LEASE_EXPIRED' or same_failure_count<1) then null else case when x.uncertain_provider_effect then 'UNCERTAIN_EFFECT' else 'LEASE_EXPIRED' end end,
   resumption_condition=case when not x.uncertain_provider_effect and cumulative_failures+1<max_failures and (last_failure_class is distinct from 'LEASE_EXPIRED' or same_failure_count<1) then null else jsonb_build_object('required_action',case when x.uncertain_provider_effect then 'Reconcile immutable output and actual billing receipts before any retry; expired provider reservation was conservatively accrued and no retry is admitted.' else 'Inspect retained source/commit hashes and worker response, apply changed approach before retry' end) end,
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
  update population_top3000_202608_202609_v1.pipeline_control set
   accrued_usd=accrued_usd+coalesce((select sum(pl.reserved_usd) from population_top3000_202608_202609_v1.pipeline_provider_lease pl where pl.attempt_id=x.attempt_id and pl.released_at is null),0),
   reserved_request_usd=reserved_request_usd-coalesce((select sum(pl.reserved_usd) from population_top3000_202608_202609_v1.pipeline_provider_lease pl where pl.attempt_id=x.attempt_id and pl.released_at is null),0) where singleton;
  update population_top3000_202608_202609_v1.acquisition_control set reserved_bytes=reserved_bytes-coalesce((select sum(pl.reserved_bytes) from population_top3000_202608_202609_v1.pipeline_provider_lease pl where pl.attempt_id=x.attempt_id and pl.released_at is null),0) where singleton;
  update population_top3000_202608_202609_v1.pipeline_provider_lease set released_at=clock_timestamp(),actual_usd=reserved_usd,outcome='UNCERTAIN_EFFECT_CONSERVATIVELY_ACCRUED' where attempt_id=x.attempt_id and released_at is null;
  update population_top3000_202608_202609_v1.pipeline_task set lease_until=null,
   resumption_condition=coalesce(resumption_condition,'{}'::jsonb)||jsonb_build_object('required_action','Reconcile immutable output and actual billing receipts before any retry. Reservation was conservatively accrued after its original deadline; task remains blocked.','reservation_reconciled_at',clock_timestamp()) where task_id=x.task_id;
  insert into population_top3000_202608_202609_v1.pipeline_event(task_id,attempt_id,event,details) values(x.task_id,x.attempt_id,'UNCERTAIN_RESERVATION_RECONCILED',jsonb_build_object('original_deadline',x.lease_until,'task_remains_blocked',true,'retry_admitted',false));
 end loop;
 -- Only explicitly timed temporary conditions are rechecked. Auth, budget, code and user holds never auto-release.
 update population_top3000_202608_202609_v1.pipeline_task q set status='READY',recheck_count=recheck_count+1,blocker_until=null,blocker_code=null,resumption_condition=null
 where q.status='BLOCKED' and q.blocker_until<=clock_timestamp() and q.recheck_count<q.max_rechecks
 and q.blocker_code in('DEPENDENCY_PENDING','RETAINED_IMPORT_PENDING','RETAINED_SEC_SOURCE_IMPORT_INCOMPLETE','ADAPTER_NOT_INSTALLED')
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
 select q.* into t from population_top3000_202608_202609_v1.pipeline_task q
 left join population_top3000_202608_202609_v1.pipeline_adapter adapter_rows on adapter_rows.family_id=q.family_id and adapter_rows.stage=q.stage
 where q.status='READY' and q.not_before<=clock_timestamp() and q.executions<q.max_executions
 and not exists(select 1 from population_top3000_202608_202609_v1.pipeline_dependency d join population_top3000_202608_202609_v1.pipeline_task z on z.task_id=d.prerequisite_task_id where d.task_id=q.task_id and z.status<>'COMPLETE')
 and (adapter_rows.resource_class is distinct from 'PROVIDER' or (c.next_provider_at<=clock_timestamp() and not exists(select 1 from population_top3000_202608_202609_v1.pipeline_provider_lease where released_at is null)))
 order by (adapter_rows.enabled is true) desc, case when q.family_id in('P01','P02') and q.payload->>'kind'='RETAINED_ALPACA_AUGUST_SESSION' then 20 else 0 end, q.executions,q.created_at,q.task_id for update of q skip locked limit 1;
 if not found then
  select min(q.not_before) into wake from population_top3000_202608_202609_v1.pipeline_task q where status='READY' and not_before>clock_timestamp();
  if exists(select 1 from population_top3000_202608_202609_v1.pipeline_task where status='LEASED') then wake:=least(coalesce(wake,'infinity'),clock_timestamp()+interval '5 seconds');
  elsif exists(select 1 from population_top3000_202608_202609_v1.pipeline_task where status='READY') then wake:=least(coalesce(wake,'infinity'),greatest(c.next_provider_at,clock_timestamp()+interval '30 seconds'));
  else wake:=least(coalesce(wake,'infinity'),clock_timestamp()+interval '5 minutes');end if;
  update population_top3000_202608_202609_v1.pipeline_control set next_wakeup_at=wake,idle_rechecks=idle_rechecks+1,wake_reason='WAIT_FOR_READY_TASK_OR_EVIDENCE' where singleton;
  if not exists(select 1 from population_top3000_202608_202609_v1.pipeline_task where status in('READY','LEASED')) and p.idle_rechecks>=p.max_idle_rechecks then
   update population_top3000_202608_202609_v1.pipeline_control set enabled=false,wake_reason='BOUNDED_WAIT_EXHAUSTED_REQUIRES_NEW_EVIDENCE' where singleton;
   perform cron.alter_job(596,schedule=>'*/5 * * * *',active=>false);
   insert into population_top3000_202608_202609_v1.pipeline_event(event,details)values('BOUNDED_WAIT_DISARMED',jsonb_build_object('resumption','New authorised evidence/adapters admitted by controller; preserve attempts and counters'));
  elsif not exists(select 1 from population_top3000_202608_202609_v1.pipeline_task where status='LEASED') then perform cron.alter_job(596,schedule=>'*/1 * * * *');end if;
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
end $function$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.pipeline_wake_v1(p_owner text, p_fence bigint, p_reason text)
 RETURNS jsonb
 LANGUAGE plpgsql
 SET search_path TO 'pg_catalog'
AS $function$
declare c population_top3000_202608_202609_v1.acquisition_control%rowtype;
 p population_top3000_202608_202609_v1.pipeline_control%rowtype;
begin
 perform pg_advisory_xact_lock(2026083000261022);
 select * into c from population_top3000_202608_202609_v1.acquisition_control where singleton for update;
 select * into p from population_top3000_202608_202609_v1.pipeline_control where singleton for update;
 if c.singleton is null or p.singleton is null then raise exception 'CONTROL_MISSING';end if;
 if p_owner is null or p_fence is null or c.owner_id is distinct from p_owner or c.owner_fence is distinct from p_fence
  or p.expected_owner_id is distinct from c.owner_id or p.expected_owner_fence is distinct from c.owner_fence or p.expected_stop_generation is distinct from c.stop_generation then raise exception 'OWNER_OR_STOP_CHANGED';end if;
 if c.scope_sha256 is distinct from '8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd' or p.scope_sha256 is distinct from c.scope_sha256 then raise exception 'SCOPE_MISMATCH';end if;
 if c.stopped is distinct from false or c.enabled is distinct from true or c.hold_reason is not null
  or c.admitted_until is null or c.admitted_until<=clock_timestamp() then raise exception 'OWNER_STOP_OR_ADMISSION';end if;
 if p.deadline is null or p.deadline<=clock_timestamp() or p.tick_count>=p.max_ticks then raise exception 'DEADLINE_OR_TICK_LIMIT';end if;
 if p.shared_scheduler_verified is distinct from true then raise exception 'SCHEDULER_EVIDENCE_REQUIRED';end if;
 if population_top3000_202608_202609_v1.pipeline_cost_admission_v2(p,0) is distinct from true then raise exception 'BUDGET_HOLD';end if;
 if nullif(btrim(p_reason),'') is null then raise exception 'WAKE_REASON_REQUIRED';end if;
 if not exists(select 1 from cron.job where jobid=596 and database=current_database()
  and btrim(command)='select population_top3000_202608_202609_v1.pipeline_tick_v1();') then raise exception 'SCHEDULER_BINDING_MISMATCH';end if;
 -- This operation admits a wake, never reconciles ownership or clears a user/budget hold.
 update population_top3000_202608_202609_v1.pipeline_control set enabled=true,next_wakeup_at=clock_timestamp(),idle_rechecks=0,wake_reason=p_reason where singleton;
 perform cron.alter_job(596,schedule=>'2 seconds',active=>true);
 insert into population_top3000_202608_202609_v1.pipeline_event(event,details)values('WAKE_ADMITTED',jsonb_build_object('owner',p_owner,'fence',p_fence,'stop_generation',c.stop_generation,'scope_sha256',c.scope_sha256,'reason',p_reason));
 return '{"state":"SCHEDULED"}';
end $function$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.population_recovery_request_v1(p_request_key text DEFAULT NULL::text, p_reason text DEFAULT 'scheduled-recovery-check'::text)
 RETURNS jsonb
 LANGUAGE plpgsql
 SET search_path TO 'pg_catalog'
AS $function$
DECLARE c population_top3000_202608_202609_v1.acquisition_control%rowtype; p population_top3000_202608_202609_v1.pipeline_control%rowtype; auth jsonb; auth_id bigint; result jsonb; previous jsonb; state text; facts jsonb; manifest jsonb; j record; eng jsonb; eng_id bigint; pause jsonb; restart_count integer; last_restart timestamptz; ready_count integer; database_only boolean; bytes bigint; lim bigint; wake jsonb;
BEGIN
 IF length(coalesce(p_request_key,''))>160 OR length(coalesce(p_reason,''))>1000 THEN RAISE EXCEPTION 'RECOVERY_ARGUMENT_TOO_LONG';END IF;
 IF NOT pg_try_advisory_xact_lock(2026083000261022) THEN RETURN jsonb_build_object('state','BUSY','writes',false);END IF;
 IF p_request_key IS NOT NULL THEN SELECT e.details->'result' INTO previous FROM population_top3000_202608_202609_v1.pipeline_event e WHERE e.event='RECOVERY_REQUEST_RESULT_V1' AND e.details->>'request_key'=p_request_key ORDER BY e.event_id DESC LIMIT 1; IF FOUND THEN RETURN previous||jsonb_build_object('idempotent_replay',true);END IF;END IF;
 SELECT * INTO c FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton FOR UPDATE;
 SELECT * INTO p FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton FOR UPDATE;
 SELECT e.event_id,e.details INTO auth_id,auth FROM population_top3000_202608_202609_v1.pipeline_event e WHERE e.event='RECOVERY_DELEGATION_QUALIFIED_V1' ORDER BY e.event_id DESC LIMIT 1;
 SELECT jobid,active INTO j FROM cron.job WHERE jobid=(auth->>'pipeline_job_id')::bigint AND database=current_database() AND btrim(command)='select population_top3000_202608_202609_v1.pipeline_tick_v1();';
 SELECT e.details INTO pause FROM population_top3000_202608_202609_v1.pipeline_event e WHERE e.event='RECOVERY_MAINTENANCE_PAUSE_V1' ORDER BY e.event_id DESC LIMIT 1;
 SELECT e.event_id,e.details INTO eng_id,eng FROM population_top3000_202608_202609_v1.pipeline_event e WHERE e.event='ROOT_SCOPED_IMPLEMENTATION_ACTIVE' ORDER BY e.event_id DESC LIMIT 1;
 SELECT count(*),max(e.at) INTO restart_count,last_restart FROM population_top3000_202608_202609_v1.pipeline_event e WHERE e.event='RECOVERY_WAKE_COMPLETED_V1' AND e.details->>'policy_event_id'=auth_id::text;
 SELECT count(*),coalesce(bool_and(a.enabled AND a.resource_class='DATABASE' AND a.function_sha256=encode(sha256(convert_to(pg_get_functiondef(a.function_oid),'UTF8')),'hex') AND a.reviewed_test_evidence IS NOT NULL AND a.reviewed_test_evidence<>'{}'::jsonb),false) INTO ready_count,database_only FROM population_top3000_202608_202609_v1.pipeline_task t LEFT JOIN population_top3000_202608_202609_v1.pipeline_adapter a USING(family_id,stage) WHERE t.status='READY' AND t.not_before<=clock_timestamp() AND t.executions<t.max_executions AND NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_dependency d JOIN population_top3000_202608_202609_v1.pipeline_task z ON z.task_id=d.prerequisite_task_id WHERE d.task_id=t.task_id AND z.status<>'COMPLETE');
 manifest:=population_top3000_202608_202609_v1.population_recovery_manifest_v1();
 lim:=(p.budget_evidence#>>'{resource_limits,max_relation_bytes}')::bigint;
 SELECT coalesce(sum(pg_total_relation_size(t.oid)),0)::bigint INTO bytes FROM pg_class t JOIN pg_namespace n ON n.oid=t.relnamespace WHERE n.nspname='population_top3000_202608_202609_v1' AND t.relkind IN('r','m');
 facts:=jsonb_build_object('qualified',auth IS NOT NULL,'owner_stop_matches',c.owner_id IS NOT DISTINCT FROM auth->>'owner_id' AND c.owner_fence IS NOT DISTINCT FROM (auth->>'owner_fence')::bigint AND c.stop_generation IS NOT DISTINCT FROM (auth->>'stop_generation')::bigint AND p.expected_owner_id IS NOT DISTINCT FROM c.owner_id AND p.expected_owner_fence IS NOT DISTINCT FROM c.owner_fence AND p.expected_stop_generation IS NOT DISTINCT FROM c.stop_generation,'scope_matches',c.scope_sha256 IS NOT DISTINCT FROM auth->>'scope_sha256' AND p.scope_sha256 IS NOT DISTINCT FROM c.scope_sha256,'stopped',c.stopped,'controls_enabled',c.enabled AND p.enabled AND c.hold_reason IS NULL,'admission_valid',(auth->>'expires_at')::timestamptz>clock_timestamp() AND c.admitted_until>clock_timestamp() AND p.deadline>clock_timestamp(),'ticks_available',p.tick_count<p.max_ticks,'budget_scheduler_valid',p.shared_scheduler_verified AND population_top3000_202608_202609_v1.pipeline_cost_admission_v2(p,0),'scheduler_binding_valid',j.jobid IS NOT NULL,'scheduler_active',j.active,'maintenance_wait',coalesce((pause->>'resume_after')::timestamptz>clock_timestamp(),false),'engineering_active',coalesce(eng_id>coalesce((auth->>'engineering_event_id')::bigint,0) AND (eng->>'next_checkpoint_due_at' IS NULL OR (eng->>'next_checkpoint_due_at')::timestamptz>clock_timestamp()),false),'active_operations',EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE status='LEASED') OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_attempt WHERE outcome='RUNNING') OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_provider_lease WHERE released_at IS NULL),'competing_session',EXISTS(SELECT 1 FROM pg_stat_activity sa WHERE sa.pid<>pg_backend_pid() AND sa.state<>'idle' AND sa.query ILIKE '%population_top3000_202608_202609_v1%'),'code_matches',manifest IS NOT DISTINCT FROM auth->'code_manifest','resources_valid',lim>0 AND bytes<lim,'runnable_count',ready_count,'database_only_reviewed',database_only,'restart_allowance',restart_count<least(coalesce((auth->>'max_restarts')::integer,0),3),'cooldown_and_progress',last_restart IS NULL OR (last_restart<=clock_timestamp()-interval '10 minutes' AND p.last_useful_progress_at>last_restart));
 state:=population_top3000_202608_202609_v1.population_recovery_decision_v1(facts);
 IF state='ELIGIBLE_FOR_GUARDED_WAKE' THEN
  wake:=population_top3000_202608_202609_v1.pipeline_wake_v1(c.owner_id,c.owner_fence,'Qualified delegated recovery: '||coalesce(p_reason,'scheduled check'));state:='RESTORED';
  INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES('RECOVERY_WAKE_COMPLETED_V1',jsonb_build_object('policy_event_id',auth_id,'actor','qualified_recovery_delegate','runtime_owner',c.owner_id,'owner_fence',c.owner_fence,'stop_generation',c.stop_generation,'request_key',p_request_key,'reason',p_reason,'prior_last_useful_progress',p.last_useful_progress_at,'ready_tasks',ready_count,'result',wake,'quality_gates_and_limits_unchanged',true));
 END IF;
 result:=jsonb_build_object('state',state,'checked_at',clock_timestamp(),'policy_event_id',auth_id,'owner_fence',c.owner_fence,'last_useful_progress',p.last_useful_progress_at,'restart_performed',state='RESTORED','checks',facts);
 IF state IN('AUTHORISATION_EXPIRED','USER_STOP','OWNER_OR_STOP_CHANGED') THEN FOR j IN SELECT jobid FROM cron.job WHERE jobname='top3000-qualified-recovery-v1' AND database=current_database() AND command LIKE '%population_top3000_202608_202609_v1.population_recovery_request_v1%' AND active LOOP PERFORM cron.alter_job(j.jobid,active=>false);END LOOP;END IF;
 IF p_request_key IS NOT NULL THEN INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES('RECOVERY_REQUEST_RESULT_V1',jsonb_build_object('request_key',p_request_key,'reason',p_reason,'result',result));
 ELSIF state IS DISTINCT FROM (SELECT e.details->>'state' FROM population_top3000_202608_202609_v1.pipeline_event e WHERE e.event='RECOVERY_CHECK_STATUS_V1' ORDER BY e.event_id DESC LIMIT 1) THEN INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES('RECOVERY_CHECK_STATUS_V1',result);END IF;
 RETURN result;
END $function$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.pipeline_provider_reserve_v1(p_attempt uuid, p_token text, p_provider text, p_max_bytes bigint, p_max_incremental_usd numeric)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'pg_catalog'
AS $function$
declare tid uuid;c population_top3000_202608_202609_v1.acquisition_control%rowtype;p population_top3000_202608_202609_v1.pipeline_control%rowtype;
begin
tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);
if p_provider is null or p_max_bytes is null or p_max_bytes<=0 or p_max_bytes>33554432 or p_max_incremental_usd is null or p_max_incremental_usd<0 then raise exception 'RESERVATION_INVALID';end if;
select * into c from population_top3000_202608_202609_v1.acquisition_control where singleton;
select * into p from population_top3000_202608_202609_v1.pipeline_control where singleton;
if exists(select 1 from population_top3000_202608_202609_v1.pipeline_provider_lease where attempt_id=p_attempt) then raise exception 'REQUEST_ALREADY_RESERVED_FOR_ATTEMPT';end if;
if c.next_provider_at>clock_timestamp() then return jsonb_build_object('state','WAIT','not_before',c.next_provider_at);end if;
if exists(select 1 from population_top3000_202608_202609_v1.pipeline_provider_lease where released_at is null)
 or exists(select 1 from population_top3000_202608_202609_v1.acquisition_job where status='LEASED' and lease_until>clock_timestamp()) then return jsonb_build_object('state','WAIT','not_before',clock_timestamp()+interval '5 seconds');end if;
if p.budget_verified_at is null or p.budget_evidence is null or coalesce((p.budget_evidence->>'provider_admissions_allowed')::boolean,false) is not true then return '{"state":"BUDGET_HOLD","reason":"CURRENT_MISSION_BILLING_BASELINE_REQUIRED"}';end if;
if (coalesce(p.budget_evidence->'allowed_providers','[]'::jsonb) ? p_provider) is not true then
 return jsonb_build_object('state','BUDGET_HOLD','reason','PROVIDER_NOT_IN_CURRENT_ADMISSION');
end if;
if p_max_bytes>8388608 then return jsonb_build_object('state','BUDGET_HOLD','reason','PER_RESPONSE_BYTE_LIMIT');end if;
if nullif(p.budget_evidence#>>'{resource_limits,max_relation_bytes}','') is null
 or (select coalesce(sum(pg_total_relation_size(r.oid)),0) from pg_class r join pg_namespace n on n.oid=r.relnamespace
 where n.nspname='population_top3000_202608_202609_v1' and r.relkind in('r','m')) >= (p.budget_evidence#>>'{resource_limits,max_relation_bytes}')::bigint
then return jsonb_build_object('state','BUDGET_HOLD','reason','MISSION_RELATION_STORAGE_BOUND');end if;
if c.provider_requests>=c.max_provider_requests or c.stored_bytes+c.reserved_bytes+p_max_bytes>c.max_source_bytes or population_top3000_202608_202609_v1.pipeline_cost_admission_v2(p,p_max_incremental_usd) is distinct from true then return '{"state":"BUDGET_HOLD","reason":"CONSERVATIVE_TOTAL_OR_RESOURCE_LIMIT"}';end if;
insert into population_top3000_202608_202609_v1.pipeline_provider_lease(attempt_id,provider,reserved_bytes,reserved_usd)values(p_attempt,p_provider,p_max_bytes,p_max_incremental_usd);
update population_top3000_202608_202609_v1.acquisition_control set provider_requests=provider_requests+1,reserved_bytes=reserved_bytes+p_max_bytes,next_provider_at=clock_timestamp()+interval '500 milliseconds' where singleton;
update population_top3000_202608_202609_v1.pipeline_control set reserved_request_usd=reserved_request_usd+p_max_incremental_usd where singleton;
return jsonb_build_object('state','RESERVED','max_bytes',p_max_bytes,'max_incremental_usd',p_max_incremental_usd);
end $function$;