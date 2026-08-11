alter table public.collection_partitions
    add column if not exists infrastructure_retry_count integer not null default 0,
    add column if not exists checkpoint_reset_count integer not null default 0,
    add column if not exists last_checkpoint_validated_at timestamptz,
    add column if not exists last_checkpoint_validation text;

create table if not exists research.collection_failure_ledger (
    id bigint generated always as identity primary key,
    partition_id uuid not null references public.collection_partitions(id) on delete cascade,
    run_id uuid not null references public.collection_runs(id) on delete cascade,
    provider text not null,
    data_type text not null,
    provider_symbol text,
    failure_class text not null check (failure_class in ('infrastructure','provider','data_or_logic','checkpoint')),
    error_code text,
    error_message text,
    retryable boolean not null default false,
    retry_scheduled boolean not null default false,
    occurred_at timestamptz not null default now()
);

create index if not exists collection_failure_ledger_run_idx
    on research.collection_failure_ledger(run_id, occurred_at desc);
create index if not exists collection_failure_ledger_partition_idx
    on research.collection_failure_ledger(partition_id, occurred_at desc);

create or replace view research.collection_operational_progress as
select
    cp.run_id,
    cp.provider,
    cp.data_type,
    count(*)::bigint as total_workload,
    count(*) filter (where cp.status in ('completed','completed_empty','skipped'))::bigint as completed_workload,
    count(*) filter (where cp.status='failed')::bigint as failed_workload,
    count(*) filter (where cp.status='retry_wait')::bigint as retry_wait_workload,
    count(*) filter (where cp.status='running')::bigint as running_workload,
    count(*) filter (where cp.status in ('queued','retry_wait','running'))::bigint as remaining_workload,
    count(*) filter (where cp.infrastructure_retry_count>0 or cp.attempts>1)::bigint as retried_workload,
    coalesce(sum(cp.infrastructure_retry_count),0)::bigint as infrastructure_retry_events,
    coalesce(sum(cp.checkpoint_reset_count),0)::bigint as checkpoint_reset_events,
    (array_agg(cp.provider_symbol order by cp.locked_at desc nulls last, cp.updated_at desc)
        filter (where cp.status='running'))[1] as current_item,
    (array_agg(cp.provider_symbol order by cp.updated_at desc)
        filter (where cp.status in ('completed','completed_empty')))[1] as last_successful_item,
    max(cp.updated_at) filter (where cp.status in ('completed','completed_empty')) as last_success_at,
    max(cp.updated_at) as last_activity_at
from public.collection_partitions cp
group by cp.run_id, cp.provider, cp.data_type;
