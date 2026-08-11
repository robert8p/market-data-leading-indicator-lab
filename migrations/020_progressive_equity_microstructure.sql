create extension if not exists pg_cron;

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
    v_runs uuid[]:='{}'::uuid[];
    v_run uuid;
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
    ), raw_windows as (
        select cw.run_id,cw.instrument_id,cw.provider_symbol,cw.id as capture_window_id,
               cw.window_start,cw.window_end,er.created_at as run_created_at,
               max(cw.window_end) over(
                   partition by cw.run_id,cw.instrument_id
                   order by cw.window_start,cw.window_end
                   rows between unbounded preceding and 1 preceding
               ) as prior_max_end
        from public.capture_windows cw
        join eligible_runs er on er.id=cw.run_id
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
        where exists (
            select 1 from public.collection_partitions p
            where p.run_id=m.run_id and p.instrument_id=m.instrument_id
              and p.data_type='trades'
              and p.start_ts<m.window_end and p.end_ts>m.window_start
        )
          and exists (
            select 1 from public.collection_partitions p
            where p.run_id=m.run_id and p.instrument_id=m.instrument_id
              and p.data_type='quotes'
              and p.start_ts<m.window_end and p.end_ts>m.window_start
        )
          and not exists (
            select 1 from public.collection_partitions p
            where p.run_id=m.run_id and p.instrument_id=m.instrument_id
              and p.data_type in ('trades','quotes')
              and p.start_ts<m.window_end and p.end_ts>m.window_start
              and p.status not in ('completed','completed_empty')
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
        returning run_id
    )
    select count(*)::integer,coalesce(array_agg(distinct run_id),'{}'::uuid[])
    into v_inserted,v_runs
    from ins;

    foreach v_run in array v_runs loop
        perform public.refresh_collection_run_counts(v_run);
    end loop;

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
'Idempotently schedules merged Alpaca microstructure windows as soon as all overlapping SIP trade and quote partitions are complete. Uses a bounded active queue so aggregation can progress without waiting for unrelated context enrichment.';
