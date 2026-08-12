create or replace function research_hub.refresh_equity_microstructure_sequence_v1(
    p_start timestamptz,
    p_end timestamptz
)
returns jsonb
language plpgsql
security invoker
set search_path=pg_catalog,public,research_hub,pg_temp
as $$
declare v_rows bigint:=0;
begin
    if p_end<=p_start then raise exception 'p_end must be after p_start'; end if;

    with raw as (
        select m.*,i.provider_symbol,('ALPACA:'||i.provider_symbol) instrument_key,
               coalesce(m.last_bid_size,0)+coalesce(m.last_ask_size,0) last_depth,
               case when coalesce(m.last_bid_size,0)+coalesce(m.last_ask_size,0)>0
                    then (coalesce(m.last_bid_size,0)-coalesce(m.last_ask_size,0))
                         / nullif(coalesce(m.last_bid_size,0)+coalesce(m.last_ask_size,0),0)::double precision end quote_imbalance,
               case when coalesce(m.buy_volume,0)+coalesce(m.sell_volume,0)>0
                    then (coalesce(m.buy_volume,0)-coalesce(m.sell_volume,0))
                         / nullif(coalesce(m.buy_volume,0)+coalesce(m.sell_volume,0),0)::double precision end trade_imbalance,
               case when m.last_bid_price>0 and m.last_ask_price>=m.last_bid_price
                          and coalesce(m.last_bid_size,0)+coalesce(m.last_ask_size,0)>0
                    then ((((m.last_ask_price*coalesce(m.last_bid_size,0)
                             +m.last_bid_price*coalesce(m.last_ask_size,0))
                           / nullif(coalesce(m.last_bid_size,0)+coalesce(m.last_ask_size,0),0)::double precision)
                          / ((m.last_bid_price+m.last_ask_price)/2.0))-1.0)*10000.0 end microprice_dislocation_bps
        from public.equity_microstructure_1m m
        join public.instruments i on i.id=m.instrument_id
        where m.provider='alpaca'
          and m.ts>=p_start-interval '6 minutes'
          and m.ts<p_end
    ), history as (
        select r.*,
               lag(r.ts,1) over w prev_ts,
               lag(r.ts,5) over w ts_lag5,
               lag(r.trade_imbalance,1) over w prev_trade_imbalance,
               lag(r.quote_imbalance,1) over w prev_quote_imbalance,
               avg(r.avg_spread_bps) over(partition by r.instrument_id order by r.ts rows between 5 preceding and 1 preceding) spread_prev5,
               avg(r.last_depth) over(partition by r.instrument_id order by r.ts rows between 5 preceding and 1 preceding) depth_prev5,
               avg(r.quote_count::double precision) over(partition by r.instrument_id order by r.ts rows between 5 preceding and 1 preceding) quote_count_prev5,
               avg(r.trade_count::double precision) over(partition by r.instrument_id order by r.ts rows between 5 preceding and 1 preceding) trade_count_prev5,
               count(*) over(partition by r.instrument_id order by r.ts rows between 5 preceding and 1 preceding) prev5_rows
        from raw r
        window w as (partition by r.instrument_id order by r.ts)
    ), ratios as (
        select h.*,
               case when h.prev5_rows=5 and h.ts_lag5=h.ts-interval '5 minutes'
                    then ln(1.0+greatest(coalesce(h.avg_spread_bps,0),0))-ln(1.0+greatest(coalesce(h.spread_prev5,0),0)) end spread_log_ratio_prev5,
               case when h.prev5_rows=5 and h.ts_lag5=h.ts-interval '5 minutes'
                    then ln(1.0+greatest(coalesce(h.last_depth,0),0))-ln(1.0+greatest(coalesce(h.depth_prev5,0),0)) end depth_log_ratio_prev5,
               case when h.prev5_rows=5 and h.ts_lag5=h.ts-interval '5 minutes'
                    then ln(1.0+greatest(coalesce(h.quote_count,0),0))-ln(1.0+greatest(coalesce(h.quote_count_prev5,0),0)) end quote_rate_log_ratio_prev5,
               case when h.prev5_rows=5 and h.ts_lag5=h.ts-interval '5 minutes'
                    then ln(1.0+greatest(coalesce(h.trade_count,0),0))-ln(1.0+greatest(coalesce(h.trade_count_prev5,0),0)) end trade_rate_log_ratio_prev5,
               case when h.prev_ts=h.ts-interval '1 minute' and h.trade_imbalance is not null and h.prev_trade_imbalance is not null
                    then h.trade_imbalance-h.prev_trade_imbalance end trade_imbalance_delta_1m,
               case when h.prev_ts=h.ts-interval '1 minute' and h.quote_imbalance is not null and h.prev_quote_imbalance is not null
                    then h.quote_imbalance-h.prev_quote_imbalance end quote_imbalance_delta_1m
        from history h
    ), states as (
        select r.*,
               case when r.spread_log_ratio_prev5 is not null and r.depth_log_ratio_prev5 is not null
                    then r.spread_log_ratio_prev5-r.depth_log_ratio_prev5 end liquidity_withdrawal_score
        from ratios r
    ), final as (
        select s.*,lag(s.liquidity_withdrawal_score,1) over(partition by s.instrument_id order by s.ts) prev_withdrawal_score
        from states s
    ), ins as (
        insert into research_hub.feature_rows(
            feature_set_key,instrument_key,decision_ts,observable_at,features,
            source_dataset_key,source_row_hash,quality
        )
        select 'equity.sip.microsequence.v1',f.instrument_key,
               f.ts+interval '1 minute',f.ts+interval '1 minute',
               jsonb_strip_nulls(jsonb_build_object(
                   'seq.spread_log_ratio_prev5',f.spread_log_ratio_prev5,
                   'seq.depth_log_ratio_prev5',f.depth_log_ratio_prev5,
                   'seq.quote_rate_log_ratio_prev5',f.quote_rate_log_ratio_prev5,
                   'seq.trade_rate_log_ratio_prev5',f.trade_rate_log_ratio_prev5,
                   'seq.trade_imbalance_delta_1m',f.trade_imbalance_delta_1m,
                   'seq.quote_imbalance_delta_1m',f.quote_imbalance_delta_1m,
                   'seq.microprice_dislocation_bps',f.microprice_dislocation_bps,
                   'seq.liquidity_withdrawal_score',f.liquidity_withdrawal_score,
                   'seq.flow_quote_agreement',case when f.trade_imbalance is not null and f.quote_imbalance is not null then f.trade_imbalance*f.quote_imbalance end,
                   'seq.pressure_disagreement',case when f.trade_imbalance is not null and f.quote_imbalance is not null then abs(f.trade_imbalance-f.quote_imbalance) end,
                   'seq.microprice_flow_alignment',case when f.microprice_dislocation_bps is not null and f.trade_imbalance is not null then f.microprice_dislocation_bps*f.trade_imbalance end,
                   'seq.prior_quote_to_trade',case when f.prev_ts=f.ts-interval '1 minute' and f.prev_quote_imbalance is not null and f.trade_imbalance is not null then f.prev_quote_imbalance*f.trade_imbalance end,
                   'seq.prior_trade_to_quote',case when f.prev_ts=f.ts-interval '1 minute' and f.prev_trade_imbalance is not null and f.quote_imbalance is not null then f.prev_trade_imbalance*f.quote_imbalance end,
                   'seq.prior_withdrawal_to_flow',case when f.prev_ts=f.ts-interval '1 minute' and f.prev_withdrawal_score is not null and f.trade_imbalance is not null then f.prev_withdrawal_score*f.trade_imbalance end,
                   'seq.burst_withdrawal',case when f.liquidity_withdrawal_score is not null and f.trade_rate_log_ratio_prev5 is not null then f.liquidity_withdrawal_score+f.trade_rate_log_ratio_prev5 end,
                   'seq.quote_burst_withdrawal',case when f.liquidity_withdrawal_score is not null and f.quote_rate_log_ratio_prev5 is not null then f.liquidity_withdrawal_score+f.quote_rate_log_ratio_prev5 end
               )),
               'primary.equity_microstructure_1m',
               md5(concat_ws('|',f.instrument_id::text,f.ts::text,'microsequence-v1')),
               jsonb_build_object('source_provider','alpaca','adapter_version','microsequence-v1')
        from final f
        where f.ts>=p_start and f.ts<p_end
        on conflict(feature_set_key,instrument_key,decision_ts) do update set
            observable_at=excluded.observable_at,
            features=excluded.features,
            source_dataset_key=excluded.source_dataset_key,
            source_row_hash=excluded.source_row_hash,
            quality=excluded.quality
        returning 1
    )
    select count(*) into v_rows from ins;

    return jsonb_build_object(
        'feature_set_key','equity.sip.microsequence.v1',
        'start',p_start,'end',p_end,
        'feature_rows_upserted',v_rows,
        'holdout_outcomes_accessed',false
    );
end;
$$;

revoke all on function research_hub.refresh_equity_microstructure_sequence_v1(timestamptz,timestamptz)
from public,anon,authenticated;
