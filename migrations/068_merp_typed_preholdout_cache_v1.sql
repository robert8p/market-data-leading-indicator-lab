-- Compact, reconstructible pre-holdout cache for the 20 frozen MERP survivors.
-- The survivor family uses five same-asset instruments and eight feature columns.
-- Source is the frozen typed 15m intermediate recorded in the MERP feature-set provenance.
-- This migration never reads or materialises rows on/after the 2026-03-01 sealed holdout boundary.

create unlogged table if not exists research_hub.merp_preholdout_cache_v1(
    instrument_key text not null,
    decision_ts timestamptz not null,
    ret15 double precision,
    ret30 double precision,
    ret60 double precision,
    ret240 double precision,
    ret_accel15 double precision,
    range15 double precision,
    log_quote_volume double precision,
    log_trade_count double precision,
    gross_3600 double precision,
    gross_14400 double precision,
    source_hash text,
    primary key(instrument_key,decision_ts)
);
revoke all on table research_hub.merp_preholdout_cache_v1 from public,anon,authenticated;

create or replace function research_hub.merp_cache_feature_value_v1(
    p_feature text,
    p_row research_hub.merp_preholdout_cache_v1
)
returns double precision
language sql
immutable
set search_path=pg_catalog,research_hub
as $$
select case p_feature
    when 'cr.ret15' then (p_row).ret15
    when 'cr.ret30' then (p_row).ret30
    when 'cr.ret60' then (p_row).ret60
    when 'cr.ret240' then (p_row).ret240
    when 'cr.ret_accel15' then (p_row).ret_accel15
    when 'cr.range15' then (p_row).range15
    when 'cr.log_quote_volume' then (p_row).log_quote_volume
    when 'cr.log_trade_count' then (p_row).log_trade_count
    else null
end
$$;

create or replace function research_hub.merp_cache_gross_value_v1(
    p_horizon integer,
    p_row research_hub.merp_preholdout_cache_v1
)
returns double precision
language sql
immutable
set search_path=pg_catalog,research_hub
as $$
select case p_horizon
    when 3600 then (p_row).gross_3600
    when 14400 then (p_row).gross_14400
    else null
end
$$;

revoke all on function research_hub.merp_cache_feature_value_v1(text,research_hub.merp_preholdout_cache_v1) from public,anon,authenticated;
revoke all on function research_hub.merp_cache_gross_value_v1(integer,research_hub.merp_preholdout_cache_v1) from public,anon,authenticated;

create or replace function research_hub.materialize_merp_preholdout_cache_symbol_v1(p_instrument text)
returns bigint
language plpgsql
security invoker
set search_path=pg_catalog,public,research_hub,pg_temp
as $$
declare
    v_rows bigint;
    v_run constant uuid := '7d9ba848-87ef-42a7-a25a-6971d44aee9d'::uuid;
begin
    if p_instrument not in ('1000SATSUSDT','API3USDT','PHAUSDT','PSGUSDT','UMAUSDT') then
        raise exception 'Instrument outside frozen MERP survivor cache universe';
    end if;

    insert into research_hub.merp_preholdout_cache_v1(
        instrument_key,decision_ts,ret15,ret30,ret60,ret240,ret_accel15,range15,
        log_quote_volume,log_trade_count,gross_3600,gross_14400,source_hash
    )
    select
        f.symbol,
        f.signal_ts,
        f.ret15,f.ret30,f.ret60,f.ret240,f.ret_accel15,f.range15,
        ln(1+greatest(coalesce(f.quote_volume,0),0)),
        ln(1+greatest(coalesce(f.trade_count,0),0)),
        case when e.open>0 and x60.open>0 then x60.open/e.open-1 end,
        case when e.open>0 and x240.open>0 then x240.open/e.open-1 end,
        md5(concat_ws('|',f.symbol,f.bucket_start::text,f.open::text,f.high::text,
                      f.low::text,f.close::text,f.quote_volume::text,f.trade_count::text))
    from public.crypto_b001_replication_features f
    join public.crypto_b001_replication_features e
      on e.run_id=v_run and e.symbol=f.symbol and e.bucket_start=f.signal_ts
    left join public.crypto_b001_replication_features x60
      on x60.run_id=v_run and x60.symbol=f.symbol and x60.bucket_start=f.signal_ts+interval '1 hour'
    left join public.crypto_b001_replication_features x240
      on x240.run_id=v_run and x240.symbol=f.symbol and x240.bucket_start=f.signal_ts+interval '4 hours'
    where f.run_id=v_run
      and f.symbol=p_instrument
      and f.signal_ts>='2024-12-06'::timestamptz
      and f.signal_ts<'2026-03-01'::timestamptz
    on conflict(instrument_key,decision_ts) do update set
        ret15=excluded.ret15,ret30=excluded.ret30,ret60=excluded.ret60,ret240=excluded.ret240,
        ret_accel15=excluded.ret_accel15,range15=excluded.range15,
        log_quote_volume=excluded.log_quote_volume,log_trade_count=excluded.log_trade_count,
        gross_3600=excluded.gross_3600,gross_14400=excluded.gross_14400,
        source_hash=excluded.source_hash;
    get diagnostics v_rows=row_count;
    return v_rows;
end;
$$;
revoke all on function research_hub.materialize_merp_preholdout_cache_symbol_v1(text) from public,anon,authenticated;
