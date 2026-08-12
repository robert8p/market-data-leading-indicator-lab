create or replace function research_hub.schedule_ready_equity_microstructure_v2(
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
    if v_slots=0 then return 0; end if;

    with eligible_runs as (
        select r.id,r.created_at
        from public.collection_runs r
        where r.stage in ('enrichment','aggregation')
          and r.status not in ('cancelled','failed')
          and (p_run_id is null or r.id=p_run_id)
    ), tick_state as (
        select p.run_id,p.instrument_id,
               bool_or(p.data_type='trades') has_trades,
               bool_or(p.data_type='quotes') has_quotes,
               count(*) filter(where p.status not in ('completed','completed_empty')) nonterminal
        from public.collection_partitions p
        join eligible_runs er on er.id=p.run_id
        where p.provider='alpaca' and p.data_type in ('trades','quotes')
        group by p.run_id,p.instrument_id
    ), ready_instruments as (
        select run_id,instrument_id from tick_state
        where has_trades and has_quotes and nonterminal=0
    ), raw_windows as (
        select cw.run_id,cw.instrument_id,cw.provider_symbol,cw.window_start,cw.window_end,
               er.created_at run_created_at,
               max(cw.window_end) over(
                   partition by cw.run_id,cw.instrument_id
                   order by cw.window_start,cw.window_end
                   rows between unbounded preceding and 1 preceding
               ) prior_max_end
        from public.capture_windows cw
        join eligible_runs er on er.id=cw.run_id
        join ready_instruments ri using(run_id,instrument_id)
        where cw.provider='alpaca' and cw.planned=true
    ), marked as (
        select *,case when prior_max_end is null or window_start>prior_max_end then 1 else 0 end new_group
        from raw_windows
    ), grouped as (
        select *,sum(new_group) over(
            partition by run_id,instrument_id order by window_start,window_end rows unbounded preceding
        ) grp
        from marked
    ), merged as (
        select run_id,instrument_id,max(provider_symbol) provider_symbol,
               min(window_start) window_start,max(window_end) window_end,
               count(*) source_window_count,min(run_created_at) run_created_at
        from grouped
        group by run_id,instrument_id,grp
    ), ready as (
        select m.*
        from merged m
        where not exists (
            select 1 from public.collection_partitions a
            where a.run_id=m.run_id and a.provider='miner'
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
                   'scheduled_by','research_hub_progressive_v2'
               )
        from ready
        on conflict do nothing
        returning 1
    )
    select count(*)::integer into v_inserted from ins;

    return v_inserted;
end;
$$;

revoke all on function research_hub.schedule_ready_equity_microstructure_v2(uuid,integer)
from public,anon,authenticated;

do $$
declare v_jobid bigint;
begin
    select jobid into v_jobid from cron.job
    where jobname='progressive-equity-microstructure' limit 1;
    if v_jobid is not null then perform cron.unschedule(v_jobid); end if;
    perform cron.schedule(
        'progressive-equity-microstructure',
        '*/5 * * * *',
        $cmd$select research_hub.schedule_ready_equity_microstructure_v2(null,10);$cmd$
    );
end $$;
