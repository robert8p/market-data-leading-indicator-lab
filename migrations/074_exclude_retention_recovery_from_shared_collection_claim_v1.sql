create or replace function public.claim_collection_partition(p_worker_id text)
returns setof public.collection_partitions
language plpgsql
as $$
begin
    return query
    with candidate as (
        select cp.id
        from public.collection_partitions cp
        join public.collection_runs cr on cr.id = cp.run_id
        where cp.status in ('queued','retry_wait')
          and (cp.not_before is null or cp.not_before <= now())
          and cr.status in ('queued','running')
          and not (
              cp.provider='binance_futures'
              and cp.data_type='crypto_derivatives'
              and coalesce((cp.cursor->>'retention_sensitive_recovery')::boolean,false)=true
          )
        order by cp.priority desc, cp.created_at, cp.id
        for update of cp skip locked
        limit 1
    )
    update public.collection_partitions cp
       set status='running',
           locked_by=p_worker_id,
           locked_at=now(),
           heartbeat_at=now(),
           attempts=attempts+1,
           updated_at=now()
      from candidate
     where cp.id=candidate.id
    returning cp.*;
end;
$$;

-- Retention-sensitive Binance derivatives partitions are owned exclusively by
-- the hardened Supabase Edge recovery lane. Shared collection workers must not
-- consume them because the generic Binance history path is capped at 500 rows
-- and previously produced sparse false-complete partitions.
