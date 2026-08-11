-- Durable, horizontally claimable research-task control plane.
-- The ingestion worker should not execute exhaustive discovery. Dedicated research
-- workers claim these tasks with SKIP LOCKED and write atomic results back into the
-- existing experiment_tests registry; run-level FDR/validation is finalized only
-- after all tasks complete.

create table if not exists research_hub.experiment_tasks (
    task_id bigserial primary key,
    run_id uuid not null references research_hub.experiment_runs(run_id) on delete cascade,
    task_key text not null,
    task_type text not null default 'feature_screen',
    payload jsonb not null default '{}'::jsonb,
    status text not null default 'queued',
    priority integer not null default 100,
    attempts integer not null default 0,
    claimed_by text,
    heartbeat_at timestamptz,
    last_error text,
    result_summary jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    started_at timestamptz,
    completed_at timestamptz,
    updated_at timestamptz not null default now(),
    unique(run_id,task_key),
    check(status in ('queued','running','completed','failed','cancelled'))
);

create index if not exists idx_rh_experiment_tasks_claim
    on research_hub.experiment_tasks(status,priority,task_id)
    where status='queued';
create index if not exists idx_rh_experiment_tasks_running
    on research_hub.experiment_tasks(heartbeat_at)
    where status='running';

create or replace function research_hub.plan_feature_screen_tasks(p_run_id uuid)
returns bigint
language plpgsql
security invoker
set search_path=research_hub,pg_temp
as $$
declare
    r research_hub.experiment_runs%rowtype;
    inserted_count bigint;
begin
    select * into r from research_hub.experiment_runs where run_id=p_run_id;
    if not found then raise exception 'Unknown experiment run %',p_run_id; end if;
    if r.feature_set_key is null then raise exception 'Run % has no feature set',r.run_key; end if;
    if exists (
        select 1 from research_hub.feature_sets fs
        where fs.feature_set_key=r.feature_set_key
          and fs.point_in_time_verified is distinct from true
    ) then raise exception 'Feature set % is not point-in-time verified',r.feature_set_key; end if;

    insert into research_hub.experiment_tasks(run_id,task_key,task_type,payload)
    select p_run_id,
           'feature:'||f.feature_key,
           'feature_screen',
           jsonb_build_object(
             'feature_key',f.feature_key,
             'feature_set_key',r.feature_set_key,
             'outcome_set_key',r.outcome_set_key,
             'discovery_start',r.discovery_start,
             'discovery_end',r.discovery_end,
             'validation_start',r.validation_start,
             'validation_end',r.validation_end,
             'tail_quantiles',coalesce(r.config->'tail_quantiles','[0.02,0.05,0.10,0.20]'::jsonb),
             'round_trip_cost_bps',coalesce(r.config->'round_trip_cost_bps','0'::jsonb),
             'holdout_accessed',false
           )
    from research_hub.feature_sets fs
    cross join lateral unnest(fs.feature_keys) as f(feature_key)
    where fs.feature_set_key=r.feature_set_key
    on conflict(run_id,task_key) do nothing;

    get diagnostics inserted_count=row_count;
    update research_hub.experiment_runs
       set status=case when status='planned' then 'tasks_planned' else status end,
           config=config||jsonb_build_object('execution_mode','chunked_tasks','holdout_accessed',false),
           updated_at=now()
     where run_id=p_run_id;
    return inserted_count;
end $$;

create or replace function research_hub.claim_experiment_task(p_worker_id text)
returns research_hub.experiment_tasks
language plpgsql
security invoker
set search_path=research_hub,pg_temp
as $$
declare
    claimed research_hub.experiment_tasks%rowtype;
begin
    with next_task as (
        select task_id
        from research_hub.experiment_tasks
        where status='queued'
        order by priority,task_id
        for update skip locked
        limit 1
    )
    update research_hub.experiment_tasks t
       set status='running',
           claimed_by=p_worker_id,
           attempts=t.attempts+1,
           started_at=coalesce(t.started_at,now()),
           heartbeat_at=now(),
           updated_at=now()
      from next_task n
     where t.task_id=n.task_id
    returning t.* into claimed;
    return claimed;
end $$;

create or replace function research_hub.heartbeat_experiment_task(p_task_id bigint,p_worker_id text)
returns boolean
language sql
security invoker
set search_path=research_hub,pg_temp
as $$
with u as (
  update research_hub.experiment_tasks
     set heartbeat_at=now(),updated_at=now()
   where task_id=p_task_id and status='running' and claimed_by=p_worker_id
   returning 1
)
select exists(select 1 from u)
$$;

create or replace function research_hub.complete_experiment_task(p_task_id bigint,p_worker_id text,p_result_summary jsonb default '{}'::jsonb)
returns boolean
language sql
security invoker
set search_path=research_hub,pg_temp
as $$
with u as (
  update research_hub.experiment_tasks
     set status='completed',result_summary=coalesce(p_result_summary,'{}'::jsonb),completed_at=now(),heartbeat_at=now(),updated_at=now()
   where task_id=p_task_id and status='running' and claimed_by=p_worker_id
   returning 1
)
select exists(select 1 from u)
$$;

create or replace function research_hub.fail_experiment_task(p_task_id bigint,p_worker_id text,p_error text,p_retry boolean default true)
returns boolean
language sql
security invoker
set search_path=research_hub,pg_temp
as $$
with u as (
  update research_hub.experiment_tasks
     set status=case when p_retry then 'queued' else 'failed' end,
         claimed_by=case when p_retry then null else claimed_by end,
         heartbeat_at=case when p_retry then null else now() end,
         last_error=left(p_error,4000),
         completed_at=case when p_retry then null else now() end,
         updated_at=now()
   where task_id=p_task_id and status='running' and claimed_by=p_worker_id
   returning 1
)
select exists(select 1 from u)
$$;

create or replace function research_hub.reclaim_stale_experiment_tasks(p_stale_after interval default interval '10 minutes')
returns bigint
language plpgsql
security invoker
set search_path=research_hub,pg_temp
as $$
declare n bigint;
begin
  update research_hub.experiment_tasks
     set status='queued',claimed_by=null,heartbeat_at=null,
         last_error=coalesce(last_error,'')||case when last_error is null or last_error='' then '' else E'\n' end||'Reclaimed after stale worker heartbeat',
         updated_at=now()
   where status='running' and heartbeat_at < now()-p_stale_after;
  get diagnostics n=row_count;
  return n;
end $$;

create or replace view research_hub.experiment_task_progress with (security_invoker=true) as
select r.run_id,r.run_key,r.name,r.status as run_status,
       count(t.task_id)::bigint task_count,
       count(t.task_id) filter(where t.status='queued')::bigint queued,
       count(t.task_id) filter(where t.status='running')::bigint running,
       count(t.task_id) filter(where t.status='completed')::bigint completed,
       count(t.task_id) filter(where t.status='failed')::bigint failed,
       max(t.heartbeat_at) last_heartbeat
from research_hub.experiment_runs r
left join research_hub.experiment_tasks t using(run_id)
group by r.run_id,r.run_key,r.name,r.status;

comment on table research_hub.experiment_tasks is 'Durable research work queue for dedicated horizontally scalable discovery workers; holdout evaluation is intentionally excluded from task payloads.';
