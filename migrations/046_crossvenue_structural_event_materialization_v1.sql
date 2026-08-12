-- Outcome-blind, idempotent structural-event materialisation for the cross-venue
-- feature family. This is retained as the v1 compatibility layer; v2 later
-- repoints the definitions/materialiser before predictive screening.

create unique index if not exists derived_events_natural_uq
on research_hub.derived_events(event_key,event_version,asset_key,event_ts);

create table if not exists research_hub.event_materialization_checkpoints(
  event_family_key text not null,
  partition_key text not null,
  status text not null default 'queued',
  attempts integer not null default 0,
  row_count bigint,
  last_error text,
  metadata jsonb not null default '{}'::jsonb,
  updated_at timestamptz not null default now(),
  primary key(event_family_key,partition_key),
  check(status in ('queued','running','completed','failed')),
  check(attempts>=0)
);

insert into research_hub.event_materialization_checkpoints(event_family_key,partition_key,status,metadata)
select 'crypto.crossvenue.structural.v1', partition_key, 'queued',
       jsonb_build_object('feature_set_key','crypto.crossvenue.sync.v1','definition_version','crypto-crossvenue-sync-v1.1','outcome_accessed',false)
from research_hub.feature_materialization_checkpoints
where feature_set_key='crypto.crossvenue.sync.v1' and status='completed'
on conflict(event_family_key,partition_key) do nothing;

create or replace function research_hub.materialize_crypto_crossvenue_events_symbol_v1(p_symbol text)
returns jsonb
language plpgsql
set search_path=research_hub,pg_temp
as $$
declare v_deleted bigint; v_inserted bigint; v_symbol text:=upper(btrim(p_symbol));
begin
  if v_symbol is null or v_symbol='' then raise exception 'symbol required'; end if;

  delete from research_hub.derived_events
   where source_dataset_key='derived.crypto_crossvenue_features_v1'
     and asset_key=v_symbol
     and event_key in ('cv.binance_lead_sign_v1','cv.coinbase_lead_sign_v1','cv.binance_volume_confirms_v1','cv.coinbase_volume_confirms_v1','cv.gap_flip_v1','cv.gap_acceleration_v1');
  get diagnostics v_deleted=row_count;

  with base as (
    select v_symbol as symbol, decision_ts, observable_at,
           (features->>'cv.return_gap_1m_bc')::double precision as gap1,
           (features->>'cv.return_gap_lag1m_bc')::double precision as lag1,
           (features->>'cv.volume_shock_gap_5m_bc')::double precision as vol_gap
      from research_hub.feature_rows
     where feature_set_key='crypto.crossvenue.sync.v1'
       and instrument_key='cv:'||v_symbol
  ), events as (
    select 'cv.binance_lead_sign_v1'::text event_key,symbol,decision_ts,observable_at,jsonb_build_object('gap1',gap1) event_features from base where gap1>0
    union all
    select 'cv.coinbase_lead_sign_v1',symbol,decision_ts,observable_at,jsonb_build_object('gap1',gap1) from base where gap1<0
    union all
    select 'cv.binance_volume_confirms_v1',symbol,decision_ts,observable_at,jsonb_build_object('gap1',gap1,'volume_shock_gap_5m_bc',vol_gap) from base where gap1>0 and vol_gap>0
    union all
    select 'cv.coinbase_volume_confirms_v1',symbol,decision_ts,observable_at,jsonb_build_object('gap1',gap1,'volume_shock_gap_5m_bc',vol_gap) from base where gap1<0 and vol_gap<0
    union all
    select 'cv.gap_flip_v1',symbol,decision_ts,observable_at,jsonb_build_object('gap1',gap1,'lag1',lag1) from base where gap1 is not null and lag1 is not null and gap1*lag1<0
    union all
    select 'cv.gap_acceleration_v1',symbol,decision_ts,observable_at,jsonb_build_object('gap1',gap1,'lag1',lag1,'abs_acceleration',abs(gap1)-abs(lag1))
      from base where gap1 is not null and lag1 is not null and sign(gap1)=sign(lag1) and gap1<>0 and abs(gap1)>abs(lag1)
  )
  insert into research_hub.derived_events(event_key,event_version,asset_key,event_ts,observed_at,available_at,source_dataset_key,event_features,quality,provenance)
  select e.event_key,1,e.symbol,e.decision_ts,e.observable_at,e.observable_at,
         'derived.crypto_crossvenue_features_v1',e.event_features,
         jsonb_build_object('point_in_time_safe',true,'feature_definition_version','crypto-crossvenue-sync-v1.1','adaptive_reuse',true,'outcome_accessed',false),
         jsonb_build_object('feature_set_key','crypto.crossvenue.sync.v1','source_instrument_key','cv:'||e.symbol,'source_decision_ts',e.decision_ts,'materializer','materialize_crypto_crossvenue_events_symbol_v1')
    from events e
  on conflict(event_key,event_version,asset_key,event_ts) do update
    set observed_at=excluded.observed_at,available_at=excluded.available_at,event_features=excluded.event_features,quality=excluded.quality,provenance=excluded.provenance;
  get diagnostics v_inserted=row_count;

  return jsonb_build_object('symbol',v_symbol,'deleted_prior_rows',v_deleted,'event_rows',v_inserted,'outcome_accessed',false);
end $$;

create or replace function research_hub.process_next_crypto_crossvenue_event_partition_v1()
returns jsonb
language plpgsql
set search_path=research_hub,pg_temp
as $$
declare v_symbol text; v_result jsonb; v_attempts int;
begin
  if not pg_try_advisory_xact_lock(hashtext('rh-crossvenue-events-v1')::bigint) then
    return jsonb_build_object('status','busy');
  end if;

  select partition_key,attempts into v_symbol,v_attempts
    from research_hub.event_materialization_checkpoints
   where event_family_key='crypto.crossvenue.structural.v1'
     and (status='queued' or (status='failed' and attempts<4))
   order by attempts,partition_key
   limit 1
   for update skip locked;

  if v_symbol is null then return jsonb_build_object('status','idle'); end if;

  update research_hub.event_materialization_checkpoints
     set status='running',attempts=attempts+1,last_error=null,updated_at=now()
   where event_family_key='crypto.crossvenue.structural.v1' and partition_key=v_symbol;

  begin
    v_result:=research_hub.materialize_crypto_crossvenue_events_symbol_v1(v_symbol);
    update research_hub.event_materialization_checkpoints
       set status='completed',row_count=(v_result->>'event_rows')::bigint,
           metadata=metadata||jsonb_build_object('result',v_result,'completed_at',now(),'outcome_accessed',false),updated_at=now()
     where event_family_key='crypto.crossvenue.structural.v1' and partition_key=v_symbol;
    return jsonb_build_object('status','completed','result',v_result);
  exception when others then
    update research_hub.event_materialization_checkpoints
       set status='failed',last_error=sqlerrm,updated_at=now()
     where event_family_key='crypto.crossvenue.structural.v1' and partition_key=v_symbol;
    return jsonb_build_object('status','failed','symbol',v_symbol,'error',sqlerrm);
  end;
end $$;

do $$
begin
  if exists(select 1 from cron.job where jobname='research_hub_crossvenue_events_v1') then
    perform cron.unschedule((select jobid from cron.job where jobname='research_hub_crossvenue_events_v1' limit 1));
  end if;
  perform cron.schedule('research_hub_crossvenue_events_v1','*/10 * * * *','select research_hub.process_next_crypto_crossvenue_event_partition_v1();');
end $$;