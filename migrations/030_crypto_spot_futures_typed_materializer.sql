drop function if exists research_hub.refresh_crypto_spot_futures15m_v1(uuid,timestamptz,timestamptz);

create or replace function research_hub.refresh_crypto_spot_futures15m_typed_v1(
    p_spot_run_id uuid,
    p_start timestamptz,
    p_end timestamptz
)
returns jsonb
language plpgsql
security invoker
set search_path=pg_catalog,public,research_hub,pg_temp
as $$
declare
    v_features bigint:=0;
    v_outcomes bigint:=0;
begin
    if not pg_try_advisory_xact_lock(hashtext('rh-crypto-spot-futures15m-typed-v1')::bigint) then
        return jsonb_build_object('status','busy','start',p_start,'end',p_end);
    end if;
    if p_start>=p_end then
        raise exception 'p_start must be before p_end';
    end if;

    with funding_events as materialized (
        select
            canonical_symbol,
            ts funding_ts,
            funding_rate,
            lag(funding_rate) over(partition by canonical_symbol order by ts) prev_funding_rate,
            lead(ts) over(partition by canonical_symbol order by ts) next_funding_ts
        from public.crypto_derivatives_metrics
        where provider='binance_futures'
          and interval='funding'
          and ts>=p_start-interval '8 days'
          and ts<p_end
    ), raw as materialized (
        select
            d.canonical_symbol,
            s.symbol,
            s.bucket_start,
            s.signal_ts decision_ts,
            s.open spot_open,s.high spot_high,s.low spot_low,s.close spot_close,
            s.quote_volume,s.trade_count,s.taker_buy_quote_volume,s.final_5m_return,
            s.close_vs_vwap,s.high_to_close_rejection,
            case when d.venue_symbol like '1000%USDT' and d.canonical_symbol<>'1000SATS'
                then d.mark_price/1000.0 else d.mark_price end mark_price,
            case when d.venue_symbol like '1000%USDT' and d.canonical_symbol<>'1000SATS'
                then nullif((d.metadata->>'mark_open')::double precision,0)/1000.0
                else nullif((d.metadata->>'mark_open')::double precision,0) end mark_open,
            case when d.venue_symbol like '1000%USDT' and d.canonical_symbol<>'1000SATS'
                then (d.metadata->>'mark_high')::double precision/1000.0
                else (d.metadata->>'mark_high')::double precision end mark_high,
            case when d.venue_symbol like '1000%USDT' and d.canonical_symbol<>'1000SATS'
                then (d.metadata->>'mark_low')::double precision/1000.0
                else (d.metadata->>'mark_low')::double precision end mark_low,
            case when s.close>0 then (
                (case when d.venue_symbol like '1000%USDT' and d.canonical_symbol<>'1000SATS'
                    then d.mark_price/1000.0 else d.mark_price end)/s.close-1.0
            )*10000.0 end basis_bps,
            case when s.quote_volume>0 then s.taker_buy_quote_volume/s.quote_volume end taker_buy_share,
            ln(1.0+greatest(coalesce(s.quote_volume,0),0)) log_quote_volume,
            ln(1.0+greatest(coalesce(s.trade_count,0)::double precision,0)) log_trade_count,
            f.funding_ts,f.funding_rate,f.prev_funding_rate
        from public.crypto_derivatives_metrics d
        join public.crypto_b001_replication_15m s
          on s.run_id=p_spot_run_id
         and s.symbol=d.canonical_symbol||'USDT'
         and s.bucket_start=d.ts
        left join funding_events f
          on f.canonical_symbol=d.canonical_symbol
         and s.signal_ts>=f.funding_ts
         and (f.next_funding_ts is null or s.signal_ts<f.next_funding_ts)
        where d.provider='binance_futures'
          and d.interval='15m'
          and d.ts>=p_start-interval '7 days 15 minutes'
          and d.ts<p_end
    ), w as (
        select r.*,
            lag(bucket_start,1) over(partition by canonical_symbol order by bucket_start) ts_lag1,
            lag(bucket_start,4) over(partition by canonical_symbol order by bucket_start) ts_lag4,
            lag(bucket_start,16) over(partition by canonical_symbol order by bucket_start) ts_lag16,
            lag(spot_close,1) over(partition by canonical_symbol order by bucket_start) spot_lag1,
            lag(spot_close,4) over(partition by canonical_symbol order by bucket_start) spot_lag4,
            lag(spot_close,16) over(partition by canonical_symbol order by bucket_start) spot_lag16,
            lag(mark_price,1) over(partition by canonical_symbol order by bucket_start) mark_lag1,
            lag(basis_bps,1) over(partition by canonical_symbol order by bucket_start) basis_lag1,
            lag(basis_bps,4) over(partition by canonical_symbol order by bucket_start) basis_lag4,
            count(basis_bps) over(partition by canonical_symbol order by bucket_start rows between 95 preceding and current row) basis_n96,
            avg(basis_bps) over(partition by canonical_symbol order by bucket_start rows between 95 preceding and current row) basis_avg96,
            stddev_samp(basis_bps) over(partition by canonical_symbol order by bucket_start rows between 95 preceding and current row) basis_sd96,
            count(basis_bps) over(partition by canonical_symbol order by bucket_start rows between 671 preceding and current row) basis_n672,
            avg(basis_bps) over(partition by canonical_symbol order by bucket_start rows between 671 preceding and current row) basis_avg672,
            stddev_samp(basis_bps) over(partition by canonical_symbol order by bucket_start rows between 671 preceding and current row) basis_sd672,
            count(log_quote_volume) over(partition by canonical_symbol order by bucket_start rows between 95 preceding and current row) volume_n96,
            avg(log_quote_volume) over(partition by canonical_symbol order by bucket_start rows between 95 preceding and current row) volume_avg96,
            stddev_samp(log_quote_volume) over(partition by canonical_symbol order by bucket_start rows between 95 preceding and current row) volume_sd96
        from raw r
    ), ins as (
        insert into research_hub.crypto_spot_futures15m_features_v1(
            instrument_key,canonical_symbol,spot_symbol,decision_ts,source_bucket_start,funding_observed_at,
            basis_bps,abs_basis_bps,basis_change_15m_bps,basis_change_1h_bps,basis_z_1d,basis_z_7d,
            mark_return_15m_bps,mark_spot_divergence_15m_bps,abs_mark_spot_divergence_15m_bps,
            mark_intrabar_return_bps,mark_range_bps,funding_rate_bps,abs_funding_rate_bps,funding_change_bps,
            funding_age_hours,spot_return_15m_bps,spot_return_1h_bps,spot_return_4h_bps,
            spot_final_5m_return_bps,spot_close_vs_vwap_bps,spot_high_to_close_rejection_bps,spot_range_bps,
            spot_taker_buy_share,spot_taker_imbalance,spot_log_quote_volume,spot_log_trade_count,spot_quote_volume_z_1d,
            source_hash,metadata,updated_at
        )
        select
            'BINANCE:'||w.symbol,w.canonical_symbol,w.symbol,w.decision_ts,w.bucket_start,w.funding_ts,
            w.basis_bps,abs(w.basis_bps),
            case when w.ts_lag1=w.bucket_start-interval '15 minutes' then w.basis_bps-w.basis_lag1 end,
            case when w.ts_lag4=w.bucket_start-interval '1 hour' then w.basis_bps-w.basis_lag4 end,
            case when w.basis_n96=96 and w.basis_sd96>0 then (w.basis_bps-w.basis_avg96)/w.basis_sd96 end,
            case when w.basis_n672=672 and w.basis_sd672>0 then (w.basis_bps-w.basis_avg672)/w.basis_sd672 end,
            case when w.ts_lag1=w.bucket_start-interval '15 minutes' and w.mark_lag1>0 then (w.mark_price/w.mark_lag1-1.0)*10000.0 end,
            case when w.ts_lag1=w.bucket_start-interval '15 minutes' and w.mark_lag1>0 and w.spot_lag1>0
                then ((w.mark_price/w.mark_lag1)-(w.spot_close/w.spot_lag1))*10000.0 end,
            case when w.ts_lag1=w.bucket_start-interval '15 minutes' and w.mark_lag1>0 and w.spot_lag1>0
                then abs(((w.mark_price/w.mark_lag1)-(w.spot_close/w.spot_lag1))*10000.0) end,
            case when w.mark_open>0 then (w.mark_price/w.mark_open-1.0)*10000.0 end,
            case when w.mark_price>0 and w.mark_high is not null and w.mark_low is not null
                then (w.mark_high-w.mark_low)/w.mark_price*10000.0 end,
            w.funding_rate*10000.0,
            abs(w.funding_rate*10000.0),
            case when w.funding_rate is not null and w.prev_funding_rate is not null
                then (w.funding_rate-w.prev_funding_rate)*10000.0 end,
            case when w.funding_ts is not null then extract(epoch from (w.decision_ts-w.funding_ts))/3600.0 end,
            case when w.ts_lag1=w.bucket_start-interval '15 minutes' and w.spot_lag1>0
                then (w.spot_close/w.spot_lag1-1.0)*10000.0 end,
            case when w.ts_lag4=w.bucket_start-interval '1 hour' and w.spot_lag4>0
                then (w.spot_close/w.spot_lag4-1.0)*10000.0 end,
            case when w.ts_lag16=w.bucket_start-interval '4 hours' and w.spot_lag16>0
                then (w.spot_close/w.spot_lag16-1.0)*10000.0 end,
            w.final_5m_return*10000.0,
            w.close_vs_vwap*10000.0,
            w.high_to_close_rejection*10000.0,
            case when w.spot_close>0 then (w.spot_high-w.spot_low)/w.spot_close*10000.0 end,
            w.taker_buy_share,
            case when w.taker_buy_share is not null then 2.0*w.taker_buy_share-1.0 end,
            w.log_quote_volume,
            w.log_trade_count,
            case when w.volume_n96=96 and w.volume_sd96>0
                then (w.log_quote_volume-w.volume_avg96)/w.volume_sd96 end,
            md5(concat_ws('|',w.symbol,w.bucket_start::text,w.spot_close::text,w.mark_price::text,coalesce(w.funding_rate,0)::text)),
            jsonb_strip_nulls(jsonb_build_object(
                'contract_multiplier_normalized',true,
                'funding_source_ts',w.funding_ts,
                'source_frequency','15m'
            )),
            now()
        from w
        where w.decision_ts>=p_start and w.decision_ts<p_end
        on conflict(instrument_key,decision_ts) do update set
            canonical_symbol=excluded.canonical_symbol,
            spot_symbol=excluded.spot_symbol,
            source_bucket_start=excluded.source_bucket_start,
            funding_observed_at=excluded.funding_observed_at,
            basis_bps=excluded.basis_bps,
            abs_basis_bps=excluded.abs_basis_bps,
            basis_change_15m_bps=excluded.basis_change_15m_bps,
            basis_change_1h_bps=excluded.basis_change_1h_bps,
            basis_z_1d=excluded.basis_z_1d,
            basis_z_7d=excluded.basis_z_7d,
            mark_return_15m_bps=excluded.mark_return_15m_bps,
            mark_spot_divergence_15m_bps=excluded.mark_spot_divergence_15m_bps,
            abs_mark_spot_divergence_15m_bps=excluded.abs_mark_spot_divergence_15m_bps,
            mark_intrabar_return_bps=excluded.mark_intrabar_return_bps,
            mark_range_bps=excluded.mark_range_bps,
            funding_rate_bps=excluded.funding_rate_bps,
            abs_funding_rate_bps=excluded.abs_funding_rate_bps,
            funding_change_bps=excluded.funding_change_bps,
            funding_age_hours=excluded.funding_age_hours,
            spot_return_15m_bps=excluded.spot_return_15m_bps,
            spot_return_1h_bps=excluded.spot_return_1h_bps,
            spot_return_4h_bps=excluded.spot_return_4h_bps,
            spot_final_5m_return_bps=excluded.spot_final_5m_return_bps,
            spot_close_vs_vwap_bps=excluded.spot_close_vs_vwap_bps,
            spot_high_to_close_rejection_bps=excluded.spot_high_to_close_rejection_bps,
            spot_range_bps=excluded.spot_range_bps,
            spot_taker_buy_share=excluded.spot_taker_buy_share,
            spot_taker_imbalance=excluded.spot_taker_imbalance,
            spot_log_quote_volume=excluded.spot_log_quote_volume,
            spot_log_trade_count=excluded.spot_log_trade_count,
            spot_quote_volume_z_1d=excluded.spot_quote_volume_z_1d,
            source_hash=excluded.source_hash,
            metadata=excluded.metadata,
            updated_at=now()
        returning 1
    )
    select count(*) into v_features from ins;

    with base as materialized (
        select s.symbol,s.signal_ts decision_ts,s.bucket_start,s.close entry_close
        from public.crypto_derivatives_metrics d
        join public.crypto_b001_replication_15m s
          on s.run_id=p_spot_run_id
         and s.symbol=d.canonical_symbol||'USDT'
         and s.bucket_start=d.ts
        where d.provider='binance_futures'
          and d.interval='15m'
          and s.signal_ts>=p_start
          and s.signal_ts<p_end
          and d.ts>=p_start-interval '15 minutes'
          and d.ts<p_end
    ), horizons(horizon_seconds) as (
        values(900),(3600),(14400),(86400)
    ), pairs as (
        select b.symbol,b.decision_ts,h.horizon_seconds,b.entry_close,x.signal_ts exit_ts,x.close exit_close
        from base b
        cross join horizons h
        join public.crypto_b001_replication_15m x
          on x.run_id=p_spot_run_id
         and x.symbol=b.symbol
         and x.bucket_start=b.bucket_start+make_interval(secs=>h.horizon_seconds)
    ), ins as (
        insert into research_hub.crypto_spot_futures15m_outcomes_v1(
            instrument_key,decision_ts,horizon_seconds,entry_ts,exit_ts,gross_return,metadata,updated_at
        )
        select
            'BINANCE:'||p.symbol,p.decision_ts,p.horizon_seconds,p.decision_ts,p.exit_ts,
            p.exit_close/p.entry_close-1.0,
            '{"price_basis":"15m_close","round_trip_cost_applied":false}'::jsonb,
            now()
        from pairs p
        where p.entry_close>0 and p.exit_close>0
        on conflict(instrument_key,decision_ts,horizon_seconds) do update set
            entry_ts=excluded.entry_ts,
            exit_ts=excluded.exit_ts,
            gross_return=excluded.gross_return,
            metadata=excluded.metadata,
            updated_at=now()
        returning 1
    )
    select count(*) into v_outcomes from ins;

    return jsonb_build_object(
        'status','completed',
        'start',p_start,
        'end',p_end,
        'feature_rows_upserted',v_features,
        'outcome_rows_upserted',v_outcomes
    );
end;
$$;

revoke all on function research_hub.refresh_crypto_spot_futures15m_typed_v1(uuid,timestamptz,timestamptz)
    from public,anon,authenticated;

comment on function research_hub.refresh_crypto_spot_futures15m_typed_v1(uuid,timestamptz,timestamptz) is
'Bounded typed materializer for the 26-symbol Binance spot/perpetual 15m research panel. Normalizes 1000-token futures contracts, computes funding state as-of decision time, uses only closed/current-prior bars for features, writes exact-horizon future spot returns separately, and prevents concurrent duplicate runs with an advisory lock.';