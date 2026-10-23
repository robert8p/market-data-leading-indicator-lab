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
 if p.budget_verified_at is null or jsonb_typeof(p.budget_evidence) is distinct from 'object'
  or (p.actual_usd+p.accrued_usd+p.unavoidable_usd+p.remaining_uncommitted_usd+p.repair_egress_reserve_usd+p.reserved_request_usd<=p.budget_cap_usd) is distinct from true then raise exception 'BUDGET_HOLD';end if;
 if nullif(btrim(p_reason),'') is null then raise exception 'WAKE_REASON_REQUIRED';end if;
 if not exists(select 1 from cron.job where jobid=596 and database=current_database()
  and btrim(command)='select population_top3000_202608_202609_v1.pipeline_tick_v1();') then raise exception 'SCHEDULER_BINDING_MISMATCH';end if;
 -- This operation admits a wake, never reconciles ownership or clears a user/budget hold.
 update population_top3000_202608_202609_v1.pipeline_control set enabled=true,next_wakeup_at=clock_timestamp(),idle_rechecks=0,wake_reason=p_reason where singleton;
 perform cron.alter_job(596,schedule=>'2 seconds',active=>true);
 insert into population_top3000_202608_202609_v1.pipeline_event(event,details)values('WAKE_ADMITTED',jsonb_build_object('owner',p_owner,'fence',p_fence,'stop_generation',c.stop_generation,'scope_sha256',c.scope_sha256,'reason',p_reason));
 return '{"state":"SCHEDULED"}';
end $function$
