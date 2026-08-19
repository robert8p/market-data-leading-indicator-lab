-- Separate task planning from compute dispatch. Cross-venue statistical tasks may
-- exist while the primary DB is busy, but a research worker cannot claim them
-- until B-001 and MDM have reached terminal/ready states.
create table if not exists research_hub.experiment_dispatch_controls(
 run_id uuid primary key references research_hub.experiment_runs(run_id) on delete cascade,
 dispatch_enabled boolean not null default false,
 dispatch_class text not null default 'shared_primary_db',
 reason text,required_job_keys text[] not null default array[]::text[],last_evaluated_at timestamptz,
 metadata jsonb not null default '{}'::jsonb,created_at timestamptz not null default now(),updated_at timestamptz not null default now()
);
revoke all on research_hub.experiment_dispatch_controls from public,anon,authenticated;

create or replace function research_hub.register_experiment_dispatch_control_v1()
returns trigger language plpgsql security invoker set search_path=research_hub,pg_temp as $$
begin
 if new.run_key='RH-CV-SYNC-V1-20260812' then
  insert into research_hub.experiment_dispatch_controls(run_id,dispatch_enabled,dispatch_class,reason,required_job_keys,metadata)
  values(new.run_id,false,'shared_primary_db','Held while B-001 and MDM primary-database workloads are non-terminal.',array['B001-24M-REPLICATION','MDM-30D-COLLECTION'],jsonb_build_object('automatic_release',true,'user_action_required',false))
  on conflict(run_id) do update set required_job_keys=excluded.required_job_keys,dispatch_class=excluded.dispatch_class,metadata=excluded.metadata,updated_at=now();
 end if;
 return new;
end $$;
drop trigger if exists trg_register_experiment_dispatch_control_v1 on research_hub.experiment_runs;
create trigger trg_register_experiment_dispatch_control_v1 after insert or update of run_key on research_hub.experiment_runs for each row execute function research_hub.register_experiment_dispatch_control_v1();

create or replace function research_hub.refresh_experiment_dispatch_controls_v1()
returns bigint language plpgsql security invoker set search_path=research_hub,pg_temp as $$
declare v_changed bigint:=0;
begin
 with status_eval as(
  select c.run_id,c.required_job_keys,cardinality(c.required_job_keys) required_n,count(j.job_key) matched_n,
   bool_and(j.current_state ~* '^(completed|finalized|rejected|ready|terminal)') filter(where j.job_key is not null) all_terminal
  from research_hub.experiment_dispatch_controls c
  left join lateral unnest(c.required_job_keys) req(job_key) on true
  left join research_hub.program_jobs j on j.job_key=req.job_key
  group by c.run_id,c.required_job_keys
 ),desired as(select run_id,(required_n=0 or (matched_n=required_n and coalesce(all_terminal,false))) should_enable from status_eval),
 u as(update research_hub.experiment_dispatch_controls c set dispatch_enabled=d.should_enable,reason=case when d.should_enable then 'All required primary-database workloads are terminal/ready; dispatch released automatically.' else 'Held until all required primary-database workloads are terminal/ready.' end,last_evaluated_at=now(),updated_at=now() from desired d where c.run_id=d.run_id and (c.dispatch_enabled is distinct from d.should_enable or c.last_evaluated_at is null) returning 1)
 select count(*) into v_changed from u;
 return v_changed;
end $$;

create or replace function research_hub.claim_dispatchable_experiment_task_v1(p_worker_id text)
returns research_hub.experiment_tasks language plpgsql security invoker set search_path=research_hub,pg_temp as $$
declare claimed research_hub.experiment_tasks%rowtype;
begin
 with next_task as(
  select t.task_id from research_hub.experiment_tasks t
  left join research_hub.experiment_dispatch_controls c on c.run_id=t.run_id
  where t.status='queued' and coalesce(c.dispatch_enabled,true)
  order by t.priority,t.task_id for update of t skip locked limit 1
 )
 update research_hub.experiment_tasks t set status='running',claimed_by=p_worker_id,attempts=t.attempts+1,started_at=coalesce(t.started_at,now()),heartbeat_at=now(),updated_at=now() from next_task n where t.task_id=n.task_id returning t.* into claimed;
 return claimed;
end $$;

do $$ declare v_jobid bigint; begin
 select jobid into v_jobid from cron.job where jobname='research_hub_dispatch_control_refresh_v1' limit 1;
 if v_jobid is null then perform cron.schedule('research_hub_dispatch_control_refresh_v1','*/5 * * * *',$cmd$select research_hub.refresh_experiment_dispatch_controls_v1();$cmd$); end if;
end $$;