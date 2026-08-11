create extension if not exists pg_cron;

-- On a fresh deployment these are ordinary indexes. On the live production
-- database they were created concurrently before this migration was merged,
-- so the IF NOT EXISTS checks do not block the active collector.
create index if not exists collection_partition_tick_readiness_idx
    on public.collection_partitions(run_id,instrument_id,data_type,status,start_ts,end_ts)
    where data_type in ('trades','quotes');

create index if not exists capture_windows_progressive_aggregation_idx
    on public.capture_windows(run_id,instrument_id,window_start,window_end)
    where provider='alpaca' and planned=true;

create or replace function research_hub.schedule_ready_equity_microstructure(
    p_run_id uuid default null,
    p_batch_limit integer default 10
)
returns integer
language plpgsql
security invoker
set search_path=pg_catalog,public,research_hub,pg_temp
as $$
declare
    v_active integer:=0;
    v_slots integer:=0;
    v_inserted integer:=0;
begin
    if p_batch_limit<1 or p_batch_limit>500 then
        raise exception 'p_batch_limit must be between 1 and 500';
    end if;

    select count(*) into v_active
    from public.collection_partitions p
    join public.collection_runs r on r.id=p.run_id
    where p.data_type='equity_microstructure_aggregate'
      and p.status in ('queued','retry_wait','running')
      and r.stage in ('enrichment','aggregation')
      and r.status not in ('cancelled','failed')
      and (p_run_id is null or p.run_id=p_run_id);

    v_slots:=greatest(p_batch_limit-v_active,0);
    if v_slots=0 then
        return 0;
    end if;

    with eligible_runs as (
        select r.id,r.created_at
        from public.collection_runs r
        where r.stage='enrichment'
          and r.status not in ('cancelled','failed')
          and (p_run_id is null or r.id=p_run_id)
    ), tick_state as (
        select p.run_id,p.instrument_id,
               bool_or(p.data_type='trades') as has_trades,
               bool_or(p.data_type='quotes') as has_quotes,
               count(*) filter(where p.status not in ('completed','completed_empty')) as nonterminal
        from public.collection_partitions p
        join eligible_runs er on er.id=p.run_id
        where p.provider='alpaca' and p.data_type in ('trades','quotes')
        group by p.run_id,p.instrument_id
    ), ready_instruments as (
        select run_id,instrument_id
        from tick_state
        where has_trades and has_quotes and nonterminal=0
    ), raw_windows as (
        select cw.run_id,cw.instrument_id,cw.provider_symbol,cw.window_start,cw.window_end,
               er.created_at as run_created_at,
               max(cw.window_end) over(
                   partition by cw.run_id,cw.instrument_id
                   order by cw.window_start,cw.window_end
                   rows between unbounded preceding and 1 preceding
               ) as prior_max_end
        from public.capture_windows cw
        join eligible_runs er on er.id=cw.run_id
        join ready_instruments ri using(run_id,instrument_id)
        where cw.provider='alpaca' and cw.planned=true
    ), marked as (
        select *,case when prior_max_end is null or window_start>prior_max_end then 1 else 0 end as new_group
        from raw_windows
    ), grouped as (
        select *,sum(new_group) over(
            partition by run_id,instrument_id
            order by window_start,window_end
            rows unbounded preceding
        ) as grp
        from marked
    ), merged as (
        select run_id,instrument_id,max(provider_symbol) as provider_symbol,
               min(window_start) as window_start,max(window_end) as window_end,
               count(*) as source_window_count,min(run_created_at) as run_created_at
        from grouped
        group by run_id,instrument_id,grp
    ), ready as (
        select m.*
        from merged m
        where not exists (
            select 1 from public.collection_partitions a
            where a.run_id=m.run_id
              and a.provider='miner'
              and a.instrument_id=m.instrument_id
              and a.data_type='equity_microstructure_aggregate'
              and a.start_ts=m.window_start and a.end_ts=m.window_end
        )
        order by m.run_created_at,m.window_start,m.instrument_id
        limit v_slots
    ), ins as (
        insert into public.collection_partitions(
            run_id,provider,instrument_id,provider_symbol,data_type,start_ts,end_ts,
            status,priority,max_attempts,cursor
        )
        select run_id,'miner',instrument_id,provider_symbol,'equity_microstructure_aggregate',
               window_start,window_end,'queued',5100,8,
               jsonb_build_object(
                   'source','progressive_admitted_capture_windows',
                   'source_window_count',source_window_count,
                   'scheduled_by','research_hub_progressive_v1'
               )
        from ready
        on conflict do nothing
        returning 1
    )
    select count(*)::integer into v_inserted from ins;

    return v_inserted;
end;
$$;

revoke all on function research_hub.schedule_ready_equity_microstructure(uuid,integer) from public,anon,authenticated;

create or replace function research_hub.prevent_premature_microstructure_ready()
returns trigger
language plpgsql
security invoker
set search_path=pg_catalog,public,pg_temp
as $$
begin
    if new.stage='ready' and old.stage is distinct from 'ready'
       and exists (
           select 1 from public.collection_partitions p
           where p.run_id=new.id
             and p.data_type='equity_microstructure_aggregate'
             and p.status in ('queued','retry_wait','running')
       ) then
        new.stage:='aggregation';
        new.enhancement_completed_at:=null;
    end if;
    return new;
end;
$$;

revoke all on function research_hub.prevent_premature_microstructure_ready() from public,anon,authenticated;

drop trigger if exists trg_prevent_premature_microstructure_ready on public.collection_runs;
create trigger trg_prevent_premature_microstructure_ready
before update of stage on public.collection_runs
for each row execute function research_hub.prevent_premature_microstructure_ready();

insert into research_hub.datasets(
    dataset_key,store_key,schema_name,relation_name,asset_class,provider,frequency,grain,
    ts_column,instrument_column,observable_at_column,is_raw,point_in_time_safe,status,metadata
)
values(
    'primary.equity_microstructure_1m','market_data_primary','public','equity_microstructure_1m',
    'equity','alpaca','1m','instrument-minute','ts','instrument_id','ts',false,true,'available',
    '{"role":"research_ready_microstructure","source":"selected SIP trade/quote capture windows","progressive_materialization":true}'::jsonb
)
on conflict(dataset_key) do update set
    status=excluded.status,metadata=excluded.metadata,point_in_time_safe=excluded.point_in_time_safe,updated_at=now();

select cron.schedule(
    'progressive-equity-microstructure',
    '*/5 * * * *',
    $$select research_hub.schedule_ready_equity_microstructure(null,10);$$
);

comment on function research_hub.schedule_ready_equity_microstructure(uuid,integer) is
'Idempotently schedules merged Alpaca microstructure windows once all SIP trade and quote partitions for the instrument are complete. Uses a bounded active queue so aggregation progresses without waiting for unrelated context enrichment.';
