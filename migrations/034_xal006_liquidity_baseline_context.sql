create or replace function research.xal006_attach_liquidity_context()
returns trigger
language plpgsql
set search_path = research, public, pg_temp
as $$
declare
  v_entry jsonb;
  v_exit jsonb;
begin
  if new.candidate_id <> 'XAL-006' then
    return new;
  end if;

  if new.entry_captured_at is not null
     and (old.entry_captured_at is distinct from new.entry_captured_at
          or coalesce(new.data_quality,'{}'::jsonb)->'entry_liquidity_baseline' is null) then
    select jsonb_build_object(
      'source','public.crypto_microstructure_1s',
      'provider','binance_spot',
      'market_type','spot',
      'canonical_symbol','DOGS',
      'window_minutes',60,
      'rows',count(*),
      'coverage_start',min(ts),
      'coverage_end',max(ts),
      'median_spread_bps',percentile_cont(0.5) within group(order by ((ask_price-bid_price)/nullif((ask_price+bid_price)/2.0,0)*10000.0)),
      'p90_spread_bps',percentile_cont(0.9) within group(order by ((ask_price-bid_price)/nullif((ask_price+bid_price)/2.0,0)*10000.0)),
      'median_bid_depth_quote',percentile_cont(0.5) within group(order by (bid_depth*((bid_price+ask_price)/2.0))),
      'median_ask_depth_quote',percentile_cont(0.5) within group(order by (ask_depth*((bid_price+ask_price)/2.0)))
    ) into v_entry
    from public.crypto_microstructure_1s
    where canonical_symbol='DOGS'
      and provider='binance_spot'
      and market_type='spot'
      and ts >= new.scheduled_entry_ts - interval '60 minutes'
      and ts < new.scheduled_entry_ts
      and bid_price > 0 and ask_price >= bid_price;

    new.data_quality := coalesce(new.data_quality,'{}'::jsonb)
      || jsonb_build_object('entry_liquidity_baseline',coalesce(v_entry,'{}'::jsonb));
  end if;

  if new.exit_captured_at is not null
     and (old.exit_captured_at is distinct from new.exit_captured_at
          or coalesce(new.data_quality,'{}'::jsonb)->'exit_liquidity_baseline' is null) then
    select jsonb_build_object(
      'source','public.crypto_microstructure_1s',
      'provider','binance_spot',
      'market_type','spot',
      'canonical_symbol','DOGS',
      'window_minutes',60,
      'rows',count(*),
      'coverage_start',min(ts),
      'coverage_end',max(ts),
      'median_spread_bps',percentile_cont(0.5) within group(order by ((ask_price-bid_price)/nullif((ask_price+bid_price)/2.0,0)*10000.0)),
      'p90_spread_bps',percentile_cont(0.9) within group(order by ((ask_price-bid_price)/nullif((ask_price+bid_price)/2.0,0)*10000.0)),
      'median_bid_depth_quote',percentile_cont(0.5) within group(order by (bid_depth*((bid_price+ask_price)/2.0))),
      'median_ask_depth_quote',percentile_cont(0.5) within group(order by (ask_depth*((bid_price+ask_price)/2.0)))
    ) into v_exit
    from public.crypto_microstructure_1s
    where canonical_symbol='DOGS'
      and provider='binance_spot'
      and market_type='spot'
      and ts >= new.scheduled_exit_ts - interval '60 minutes'
      and ts < new.scheduled_exit_ts
      and bid_price > 0 and ask_price >= bid_price;

    new.data_quality := coalesce(new.data_quality,'{}'::jsonb)
      || jsonb_build_object('exit_liquidity_baseline',coalesce(v_exit,'{}'::jsonb));
  end if;

  return new;
end;
$$;

drop trigger if exists trg_xal006_liquidity_context on research.xal_live_signals;
create trigger trg_xal006_liquidity_context
before update on research.xal_live_signals
for each row
execute function research.xal006_attach_liquidity_context();
