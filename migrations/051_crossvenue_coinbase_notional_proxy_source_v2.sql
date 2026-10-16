-- Coinbase synchronized quote_volume is absent across the 20 cross-venue symbols.
-- Preserve the raw field as null; derive an explicitly labelled point-in-time
-- notional-volume proxy from Coinbase base volume * OHLC4. Supersede v1.1 before
-- any statistical task execution and repair the source in bounded resumable batches.

alter table public.crypto_research_crossvenue_1m
  add column if not exists c_notional_volume_proxy double precision;

create table if not exists research_hub.source_repair_checkpoints(
  repair_key text not null,
  partition_key text not null,
  cursor_ts timestamptz,
  status text not null default 'queued',
  failure_attempts integer not null default 0,
  rows_scanned bigint not null default 0,
  rows_updated bigint not null default 0,
  last_error text,
  metadata jsonb not null default '{}'::jsonb,
  updated_at timestamptz not null default now(),
  primary key(repair_key,partition_key),
  check(status in ('queued','running','completed','failed')),
  check(failure_attempts>=0)
);

insert into research_hub.source_repair_checkpoints(repair_key,partition_key,status,metadata)
select 'coinbase_usd_notional_proxy_v1',partition_key,'queued',
       jsonb_build_object('formula','base_volume * OHLC4','provider','coinbase','quote_asset','USD','point_in_time_safe',true,'outcome_accessed',false)
from research_hub.feature_materialization_checkpoints
where feature_set_key='crypto.crossvenue.sync.v1' and status='completed'
on conflict(repair_key,partition_key) do nothing;

create or replace function research_hub.backfill_coinbase_notional_proxy_batch_v1(p_symbol text,p_after timestamptz,p_limit integer default 1000)
returns jsonb
language plpgsql
set search_path=research_hub,public,pg_temp
as $$
declare v_symbol text:=upper(btrim(p_symbol)); v_instrument uuid; v_venue text; v_scanned bigint:=0; v_updated bigint:=0; v_max_ts timestamptz;
begin
  if v_symbol is null or v_symbol='' then raise exception 'symbol required'; end if;
  select i.id,v.venue_symbol into v_instrument,v_venue
  from public.crypto_venue_symbols v
  join public.instruments i on i.provider='coinbase' and i.provider_symbol=v.venue_symbol
  where v.provider='coinbase' and v.canonical_symbol=v_symbol and v.quote_asset='USD' and v.tradable=true
  order by v.priority desc,v.venue_symbol limit 1;
  if v_instrument is null then raise exception 'no Coinbase USD instrument for %',v_symbol; end if;

  with candidates as materialized (
    select cv.ctid row_ctid,cv.ts,cv.c_close
    from public.crypto_research_crossvenue_1m cv
    where cv.symbol=v_symbol and (p_after is null or cv.ts>p_after)
    order by cv.ts
    limit greatest(1,least(coalesce(p_limit,1000),10000))
  ), stats as (
    select count(*) n,max(ts) mx from candidates
  ), matches as materialized (
    select c.row_ctid,c.ts,c.c_close,
           cb.close raw_close,
           case when cb.volume is not null and cb.volume>=0 and cb.open>0 and cb.high>0 and cb.low>0 and cb.close>0
                then cb.volume*((cb.open+cb.high+cb.low+cb.close)/4.0) end as proxy
    from candidates c
    join public.market_bars_1m_coinbase cb
      on cb.provider='coinbase' and cb.instrument_id=v_instrument and cb.ts=c.ts
  ), upd as (
    update public.crypto_research_crossvenue_1m cv
       set c_notional_volume_proxy=m.proxy
      from matches m
     where cv.ctid=m.row_ctid
       and m.proxy is not null
       and abs(m.raw_close-m.c_close)<=greatest(1e-8,abs(m.c_close)*1e-9)
    returning 1
  )
  select s.n,s.mx,(select count(*) from upd) into v_scanned,v_max_ts,v_updated from stats s;

  return jsonb_build_object('symbol',v_symbol,'venue_symbol',v_venue,'scanned',v_scanned,'updated',v_updated,'max_ts',v_max_ts,'outcome_accessed',false,'formula','base_volume * OHLC4');
end $$;

create or replace function research_hub.process_next_coinbase_notional_proxy_batch_v1(p_limit integer default 2500)
returns jsonb
language plpgsql
set search_path=research_hub,pg_temp
as $$
declare v_symbol text; v_cursor timestamptz; v_result jsonb; v_scanned bigint; v_updated bigint; v_max timestamptz;
begin
  if not pg_try_advisory_xact_lock(hashtext('rh-cv-coinbase-volume-proxy-v1')::bigint) then return jsonb_build_object('status','busy'); end if;

  select partition_key,cursor_ts into v_symbol,v_cursor
  from research_hub.source_repair_checkpoints
  where repair_key='coinbase_usd_notional_proxy_v1' and (status in ('queued','running') or (status='failed' and failure_attempts<4))
  order by case status when 'running' then 0 when 'queued' then 1 else 2 end,coalesce(cursor_ts,'-infinity'::timestamptz),partition_key
  limit 1 for update skip locked;
  if v_symbol is null then return jsonb_build_object('status','idle'); end if;

  update research_hub.source_repair_checkpoints set status='running',last_error=null,updated_at=now()
  where repair_key='coinbase_usd_notional_proxy_v1' and partition_key=v_symbol;

  begin
    v_result:=research_hub.backfill_coinbase_notional_proxy_batch_v1(v_symbol,v_cursor,p_limit);
    v_scanned:=coalesce((v_result->>'scanned')::bigint,0);
    v_updated:=coalesce((v_result->>'updated')::bigint,0);
    v_max:=nullif(v_result->>'max_ts','')::timestamptz;
    if v_scanned=0 then
      update research_hub.source_repair_checkpoints
         set status='completed',metadata=metadata||jsonb_build_object('completed_at',now(),'last_batch',v_result),updated_at=now()
       where repair_key='coinbase_usd_notional_proxy_v1' and partition_key=v_symbol;
      return jsonb_build_object('status','completed_partition','symbol',v_symbol,'result',v_result);
    else
      update research_hub.source_repair_checkpoints
         set status='running',cursor_ts=v_max,rows_scanned=rows_scanned+v_scanned,rows_updated=rows_updated+v_updated,
             metadata=metadata||jsonb_build_object('last_batch',v_result),updated_at=now()
       where repair_key='coinbase_usd_notional_proxy_v1' and partition_key=v_symbol;
      return jsonb_build_object('status','batch_completed','symbol',v_symbol,'result',v_result);
    end if;
  exception when others then
    update research_hub.source_repair_checkpoints
       set status='failed',failure_attempts=failure_attempts+1,last_error=sqlerrm,updated_at=now()
     where repair_key='coinbase_usd_notional_proxy_v1' and partition_key=v_symbol;
    return jsonb_build_object('status','failed','symbol',v_symbol,'error',sqlerrm);
  end;
end $$;

-- Supersede v1.1 only if no statistical task has executed.
update research_hub.experiment_runs
set status='superseded_pre_screen',completed_at=coalesce(completed_at,now()),
    latest_result=coalesce(latest_result,'{}'::jsonb)||jsonb_build_object('superseded_before_task_execution',true,'superseded_reason','Coinbase native quote volume absent; v2 uses explicitly labelled point-in-time notional-volume proxy','task_results_viewed',false,'holdout_accessed',false),
    updated_at=now()
where run_key='RH-CV-SYNC-V1-20260812'
  and not exists(select 1 from research_hub.experiment_tasks t where t.run_id=research_hub.experiment_runs.run_id and t.status in ('running','completed'));

delete from research_hub.experiment_tasks
where run_id=(select run_id from research_hub.experiment_runs where run_key='RH-CV-SYNC-V1-20260812')
  and status='queued';

update research_hub.experiment_dispatch_controls
set dispatch_enabled=false,reason='Permanently held: v1.1 superseded before screening by explicit Coinbase notional-volume proxy v2.',
    metadata=metadata||jsonb_build_object('permanent_hold',true,'superseded_pre_screen',true),updated_at=now()
where run_id=(select run_id from research_hub.experiment_runs where run_key='RH-CV-SYNC-V1-20260812');

-- Pause v1 events and clear any outcome-blind compatibility rows; v2 recreates
-- structural events from its corrected predictor representation.
do $$ begin
  if exists(select 1 from cron.job where jobname='research_hub_crossvenue_events_v1') then
    perform cron.unschedule((select jobid from cron.job where jobname='research_hub_crossvenue_events_v1' limit 1));
  end if;
end $$;

delete from research_hub.derived_events where source_dataset_key='derived.crypto_crossvenue_features_v1' and event_key like 'cv.%';
update research_hub.event_materialization_checkpoints
set status='queued',row_count=null,last_error=null,metadata=metadata||jsonb_build_object('paused_for_v2_source_repair',true),updated_at=now()
where event_family_key='crypto.crossvenue.structural.v1';

do $$ begin
  if exists(select 1 from cron.job where jobname='research_hub_coinbase_notional_proxy_v1') then
    perform cron.unschedule((select jobid from cron.job where jobname='research_hub_coinbase_notional_proxy_v1' limit 1));
  end if;
  perform cron.schedule('research_hub_coinbase_notional_proxy_v1','*/2 * * * *','select research_hub.process_next_coinbase_notional_proxy_batch_v1(2500);');
end $$;

insert into research_hub.research_findings(finding_key,finding_type,title,statement,status,evidence,source_run_keys,reusable,propagation_targets)
values('FIND-CV-COINBASE-VOLUME-20260812','data_quality','Coinbase synchronized quote volume is absent','Across all 20 canonical Binance/Coinbase synchronized symbols, Coinbase quote-volume is absent even though raw Coinbase bars contain base volume. The v1.1 cross-venue family was superseded before any screen ran. v2 uses an explicitly labelled point-in-time Coinbase notional-volume proxy derived as base_volume × OHLC4; native quote volume is not imputed or mislabeled.','active_repair',jsonb_build_object('v1_run','RH-CV-SYNC-V1-20260812','v1_tasks_executed',0,'holdout_accessed',false,'proxy_formula','base_volume * OHLC4','outcome_accessed',false),array['RH-CV-SYNC-V1-20260812'],true,array['ARCH-PRO','XAL','METHOD'])
on conflict(finding_key) do update set statement=excluded.statement,status=excluded.status,evidence=excluded.evidence,updated_at=now();