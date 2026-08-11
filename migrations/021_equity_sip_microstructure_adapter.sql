insert into research_hub.feature_definitions(
    feature_key,dataset_key,feature_name,feature_family,value_type,source_expression,
    decision_time_rule,observable_at_rule,lookback_seconds,uses_future_data,enabled,metadata
)
values
('micro.avg_spread_bps','primary.equity_microstructure_1m','Average quoted spread','liquidity','double precision','avg_spread_bps','source minute end','source minute end',60,false,true,'{"version":"v1"}'),
('micro.last_spread_bps','primary.equity_microstructure_1m','Closing quoted spread','liquidity','double precision','(last_ask-last_bid)/mid*10000','source minute end','source minute end',60,false,true,'{"version":"v1"}'),
('micro.spread_range_bps','primary.equity_microstructure_1m','Intraminute spread range','liquidity','double precision','max_spread_bps-min_spread_bps','source minute end','source minute end',60,false,true,'{"version":"v1"}'),
('micro.trade_imbalance','primary.equity_microstructure_1m','Signed trade-volume imbalance','order_flow','double precision','(buy_volume-sell_volume)/(buy_volume+sell_volume)','source minute end','source minute end',60,false,true,'{"version":"v1"}'),
('micro.buy_trade_share','primary.equity_microstructure_1m','Buy-classified trade share','order_flow','double precision','buy_trade_count/(buy_trade_count+sell_trade_count)','source minute end','source minute end',60,false,true,'{"version":"v1"}'),
('micro.unknown_volume_share','primary.equity_microstructure_1m','Unclassified volume share','quality','double precision','unknown_volume/total_volume','source minute end','source minute end',60,false,true,'{"version":"v1"}'),
('micro.last_quote_size_imbalance','primary.equity_microstructure_1m','Closing L1 size imbalance','order_book','double precision','(last_bid_size-last_ask_size)/(last_bid_size+last_ask_size)','source minute end','source minute end',60,false,true,'{"version":"v1"}'),
('micro.avg_quote_size_imbalance','primary.equity_microstructure_1m','Average L1 size imbalance','order_book','double precision','(avg_bid_size-avg_ask_size)/(avg_bid_size+avg_ask_size)','source minute end','source minute end',60,false,true,'{"version":"v1"}'),
('micro.vwap_mid_bps','primary.equity_microstructure_1m','VWAP deviation from closing midpoint','price_pressure','double precision','(vwap-mid)/mid*10000','source minute end','source minute end',60,false,true,'{"version":"v1"}'),
('micro.last_trade_mid_bps','primary.equity_microstructure_1m','Last trade deviation from closing midpoint','price_pressure','double precision','(last_trade_price-mid)/mid*10000','source minute end','source minute end',60,false,true,'{"version":"v1"}'),
('micro.intraminute_return_bps','primary.equity_microstructure_1m','First-to-last trade return','price_path','double precision','(last_trade_price/first_trade_price-1)*10000','source minute end','source minute end',60,false,true,'{"version":"v1"}'),
('micro.range_bps','primary.equity_microstructure_1m','Intraminute trade range','volatility','double precision','(high_trade_price-low_trade_price)/mid*10000','source minute end','source minute end',60,false,true,'{"version":"v1"}'),
('micro.log_total_notional','primary.equity_microstructure_1m','Log traded notional','activity','double precision','ln(1+total_notional)','source minute end','source minute end',60,false,true,'{"version":"v1"}'),
('micro.log_total_volume','primary.equity_microstructure_1m','Log traded volume','activity','double precision','ln(1+total_volume)','source minute end','source minute end',60,false,true,'{"version":"v1"}'),
('micro.log_trade_count','primary.equity_microstructure_1m','Log trade count','activity','double precision','ln(1+trade_count)','source minute end','source minute end',60,false,true,'{"version":"v1"}'),
('micro.log_quote_count','primary.equity_microstructure_1m','Log quote update count','activity','double precision','ln(1+quote_count)','source minute end','source minute end',60,false,true,'{"version":"v1"}'),
('micro.mid_return_1m_bps','primary.equity_microstructure_1m','One-minute midpoint return','price_path','double precision','(mid/lag(mid,1)-1)*10000 for consecutive minutes','source minute end','source minute end',120,false,true,'{"version":"v1"}'),
('micro.spread_change_1m_bps','primary.equity_microstructure_1m','One-minute average-spread change','liquidity','double precision','avg_spread_bps-lag(avg_spread_bps,1) for consecutive minutes','source minute end','source minute end',120,false,true,'{"version":"v1"}'),
('micro.trade_imbalance_5m','primary.equity_microstructure_1m','Five-minute trade-volume imbalance','order_flow','double precision','rolling 5-minute (buy-sell)/(buy+sell)','source minute end','source minute end',300,false,true,'{"version":"v1"}'),
('micro.spread_5m_mean_bps','primary.equity_microstructure_1m','Five-minute mean quoted spread','liquidity','double precision','rolling 5-minute avg(avg_spread_bps)','source minute end','source minute end',300,false,true,'{"version":"v1"}'),
('micro.mid_return_5m_bps','primary.equity_microstructure_1m','Five-minute midpoint return','price_path','double precision','(mid/lag(mid,5)-1)*10000 for consecutive minutes','source minute end','source minute end',360,false,true,'{"version":"v1"}'),
('micro.quote_size_imbalance_5m_mean','primary.equity_microstructure_1m','Five-minute mean L1 size imbalance','order_book','double precision','rolling 5-minute mean quote-size imbalance','source minute end','source minute end',300,false,true,'{"version":"v1"}')
on conflict(feature_key) do update set
    dataset_key=excluded.dataset_key,
    feature_name=excluded.feature_name,
    feature_family=excluded.feature_family,
    value_type=excluded.value_type,
    source_expression=excluded.source_expression,
    decision_time_rule=excluded.decision_time_rule,
    observable_at_rule=excluded.observable_at_rule,
    lookback_seconds=excluded.lookback_seconds,
    uses_future_data=false,
    enabled=true,
    metadata=excluded.metadata,
    updated_at=now();

insert into research_hub.feature_sets(
    feature_set_key,description,decision_grain,source_dataset_keys,feature_keys,
    materialization_schema,materialization_relation,point_in_time_verified,verification_notes,metadata
)
values(
    'equity.sip.microstructure.v1',
    'Point-in-time one-minute Alpaca SIP microstructure features for admitted anomaly and deterministic baseline capture windows.',
    'instrument-minute',
    array['primary.equity_microstructure_1m'],
    array[
        'micro.avg_spread_bps','micro.last_spread_bps','micro.spread_range_bps','micro.trade_imbalance',
        'micro.buy_trade_share','micro.unknown_volume_share','micro.last_quote_size_imbalance',
        'micro.avg_quote_size_imbalance','micro.vwap_mid_bps','micro.last_trade_mid_bps',
        'micro.intraminute_return_bps','micro.range_bps','micro.log_total_notional','micro.log_total_volume',
        'micro.log_trade_count','micro.log_quote_count','micro.mid_return_1m_bps','micro.spread_change_1m_bps',
        'micro.trade_imbalance_5m','micro.spread_5m_mean_bps','micro.mid_return_5m_bps',
        'micro.quote_size_imbalance_5m_mean'
    ],
    'research_hub','feature_rows',true,
    'Minute t aggregate features use decision_ts=observable_at=t+60s. Rolling features use only current/prior source minutes and require timestamp continuity.',
    '{"adapter":"equity_sip_microstructure_v1","selection_bias":"capture-window sample; quality.selection_class preserves anomaly vs deterministic baseline provenance"}'::jsonb
)
on conflict(feature_set_key) do update set
    description=excluded.description,
    decision_grain=excluded.decision_grain,
    source_dataset_keys=excluded.source_dataset_keys,
    feature_keys=excluded.feature_keys,
    point_in_time_verified=excluded.point_in_time_verified,
    verification_notes=excluded.verification_notes,
    metadata=excluded.metadata,
    updated_at=now();

insert into research_hub.outcome_definitions(
    outcome_key,outcome_name,target_asset_class,horizon_seconds,entry_rule,exit_rule,
    cost_model_key,enabled,metadata
)
values
('equity.fwd_mid_60s','Forward midpoint return 1m','equity',60,'closing midpoint known at decision time','exact future minute closing midpoint at +60s','experiment_engine_round_trip_bps',true,'{"price_basis":"quote_mid","cost_application":"experiment_engine"}'),
('equity.fwd_mid_180s','Forward midpoint return 3m','equity',180,'closing midpoint known at decision time','exact future minute closing midpoint at +180s','experiment_engine_round_trip_bps',true,'{"price_basis":"quote_mid","cost_application":"experiment_engine"}'),
('equity.fwd_mid_300s','Forward midpoint return 5m','equity',300,'closing midpoint known at decision time','exact future minute closing midpoint at +300s','experiment_engine_round_trip_bps',true,'{"price_basis":"quote_mid","cost_application":"experiment_engine"}'),
('equity.fwd_mid_900s','Forward midpoint return 15m','equity',900,'closing midpoint known at decision time','exact future minute closing midpoint at +900s','experiment_engine_round_trip_bps',true,'{"price_basis":"quote_mid","cost_application":"experiment_engine"}'),
('equity.fwd_mid_1800s','Forward midpoint return 30m','equity',1800,'closing midpoint known at decision time','exact future minute closing midpoint at +1800s','experiment_engine_round_trip_bps',true,'{"price_basis":"quote_mid","cost_application":"experiment_engine"}'),
('equity.fwd_mid_3600s','Forward midpoint return 60m','equity',3600,'closing midpoint known at decision time','exact future minute closing midpoint at +3600s','experiment_engine_round_trip_bps',true,'{"price_basis":"quote_mid","cost_application":"experiment_engine"}')
on conflict(outcome_key) do update set
    outcome_name=excluded.outcome_name,
    target_asset_class=excluded.target_asset_class,
    horizon_seconds=excluded.horizon_seconds,
    entry_rule=excluded.entry_rule,
    exit_rule=excluded.exit_rule,
    cost_model_key=excluded.cost_model_key,
    enabled=true,
    metadata=excluded.metadata,
    updated_at=now();

insert into research_hub.outcome_sets(
    outcome_set_key,description,outcome_keys,materialization_schema,materialization_relation,metadata
)
values(
    'equity.sip.mid_returns.v1',
    'Exact-horizon same-instrument quote-midpoint returns from admitted Alpaca SIP capture windows; costs are applied only by the experiment engine.',
    array['equity.fwd_mid_60s','equity.fwd_mid_180s','equity.fwd_mid_300s','equity.fwd_mid_900s','equity.fwd_mid_1800s','equity.fwd_mid_3600s'],
    'research_hub','outcome_rows',
    '{"adapter":"equity_sip_microstructure_v1","holdout_materialized_but_not_read_by_discovery":true}'::jsonb
)
on conflict(outcome_set_key) do update set
    description=excluded.description,
    outcome_keys=excluded.outcome_keys,
    metadata=excluded.metadata,
    updated_at=now();

create or replace function research_hub.refresh_equity_microstructure_adapter_v1()
returns jsonb
language plpgsql
security invoker
set search_path=pg_catalog,public,research_hub,pg_temp
as $$
declare
    v_feature_rows bigint:=0;
    v_outcome_rows bigint:=0;
begin
    with raw as (
        select m.*,i.provider_symbol,('ALPACA:'||i.provider_symbol) instrument_key,
            case when m.last_bid_price is not null and m.last_ask_price is not null and m.last_bid_price>0 and m.last_ask_price>=m.last_bid_price
                then (m.last_bid_price+m.last_ask_price)/2.0 end mid,
            case when coalesce(m.last_bid_size,0)+coalesce(m.last_ask_size,0)>0
                then (coalesce(m.last_bid_size,0)-coalesce(m.last_ask_size,0))/nullif(coalesce(m.last_bid_size,0)+coalesce(m.last_ask_size,0),0)::double precision end last_quote_imbalance,
            case when coalesce(m.avg_bid_size,0)+coalesce(m.avg_ask_size,0)>0
                then (coalesce(m.avg_bid_size,0)-coalesce(m.avg_ask_size,0))/nullif(coalesce(m.avg_bid_size,0)+coalesce(m.avg_ask_size,0),0)::double precision end avg_quote_imbalance
        from public.equity_microstructure_1m m
        join public.instruments i on i.id=m.instrument_id
        where m.provider='alpaca'
    ), calc as (
        select r.*,
            lag(r.ts) over(partition by r.instrument_id order by r.ts) prev_ts,
            lag(r.mid) over(partition by r.instrument_id order by r.ts) prev_mid,
            lag(r.avg_spread_bps) over(partition by r.instrument_id order by r.ts) prev_spread,
            lag(r.ts,4) over(partition by r.instrument_id order by r.ts) ts_lag4,
            lag(r.ts,5) over(partition by r.instrument_id order by r.ts) ts_lag5,
            lag(r.mid,5) over(partition by r.instrument_id order by r.ts) mid_lag5,
            count(*) over(partition by r.instrument_id order by r.ts rows between 4 preceding and current row) rows5,
            sum(coalesce(r.buy_volume,0)) over(partition by r.instrument_id order by r.ts rows between 4 preceding and current row) buy_volume5,
            sum(coalesce(r.sell_volume,0)) over(partition by r.instrument_id order by r.ts rows between 4 preceding and current row) sell_volume5,
            avg(r.avg_spread_bps) over(partition by r.instrument_id order by r.ts rows between 4 preceding and current row) spread5,
            avg(r.avg_quote_imbalance) over(partition by r.instrument_id order by r.ts rows between 4 preceding and current row) quote_imbalance5
        from raw r
    ), enriched as (
        select c.*,cw.selection_class,cw.trigger_kind,cw.run_id capture_run_id
        from calc c
        left join lateral (
            select w.selection_class,w.trigger_kind,w.run_id
            from public.capture_windows w
            where w.instrument_id=c.instrument_id
              and w.provider='alpaca'
              and w.planned=true
              and c.ts>=w.window_start
              and c.ts<w.window_end
            order by w.trigger_ts desc
            limit 1
        ) cw on true
    ), ins as (
        insert into research_hub.feature_rows(
            feature_set_key,instrument_key,decision_ts,observable_at,features,
            source_dataset_key,source_row_hash,quality
        )
        select
            'equity.sip.microstructure.v1',e.instrument_key,e.ts+interval '1 minute',e.ts+interval '1 minute',
            jsonb_strip_nulls(jsonb_build_object(
                'micro.avg_spread_bps',e.avg_spread_bps,
                'micro.last_spread_bps',case when e.mid>0 then (e.last_ask_price-e.last_bid_price)/e.mid*10000.0 end,
                'micro.spread_range_bps',case when e.max_spread_bps is not null and e.min_spread_bps is not null then e.max_spread_bps-e.min_spread_bps end,
                'micro.trade_imbalance',case when coalesce(e.buy_volume,0)+coalesce(e.sell_volume,0)>0 then (coalesce(e.buy_volume,0)-coalesce(e.sell_volume,0))/nullif(coalesce(e.buy_volume,0)+coalesce(e.sell_volume,0),0)::double precision end,
                'micro.buy_trade_share',case when coalesce(e.buy_trade_count,0)+coalesce(e.sell_trade_count,0)>0 then coalesce(e.buy_trade_count,0)::double precision/nullif(coalesce(e.buy_trade_count,0)+coalesce(e.sell_trade_count,0),0)::double precision end,
                'micro.unknown_volume_share',case when coalesce(e.total_volume,0)>0 then coalesce(e.unknown_volume,0)::double precision/e.total_volume::double precision end,
                'micro.last_quote_size_imbalance',e.last_quote_imbalance,
                'micro.avg_quote_size_imbalance',e.avg_quote_imbalance,
                'micro.vwap_mid_bps',case when e.mid>0 and e.vwap is not null then (e.vwap/e.mid-1.0)*10000.0 end,
                'micro.last_trade_mid_bps',case when e.mid>0 and e.last_trade_price is not null then (e.last_trade_price/e.mid-1.0)*10000.0 end,
                'micro.intraminute_return_bps',case when e.first_trade_price>0 and e.last_trade_price is not null then (e.last_trade_price/e.first_trade_price-1.0)*10000.0 end,
                'micro.range_bps',case when e.mid>0 and e.high_trade_price is not null and e.low_trade_price is not null then (e.high_trade_price-e.low_trade_price)/e.mid*10000.0 end,
                'micro.log_total_notional',ln(1.0+greatest(coalesce(e.total_notional,0)::double precision,0.0)),
                'micro.log_total_volume',ln(1.0+greatest(coalesce(e.total_volume,0)::double precision,0.0)),
                'micro.log_trade_count',ln(1.0+greatest(coalesce(e.trade_count,0)::double precision,0.0)),
                'micro.log_quote_count',ln(1.0+greatest(coalesce(e.quote_count,0)::double precision,0.0)),
                'micro.mid_return_1m_bps',case when e.prev_ts=e.ts-interval '1 minute' and e.prev_mid>0 and e.mid>0 then (e.mid/e.prev_mid-1.0)*10000.0 end,
                'micro.spread_change_1m_bps',case when e.prev_ts=e.ts-interval '1 minute' and e.prev_spread is not null and e.avg_spread_bps is not null then e.avg_spread_bps-e.prev_spread end,
                'micro.trade_imbalance_5m',case when e.rows5=5 and e.ts_lag4=e.ts-interval '4 minutes' and coalesce(e.buy_volume5,0)+coalesce(e.sell_volume5,0)>0 then (coalesce(e.buy_volume5,0)-coalesce(e.sell_volume5,0))/nullif(coalesce(e.buy_volume5,0)+coalesce(e.sell_volume5,0),0)::double precision end,
                'micro.spread_5m_mean_bps',case when e.rows5=5 and e.ts_lag4=e.ts-interval '4 minutes' then e.spread5 end,
                'micro.mid_return_5m_bps',case when e.ts_lag5=e.ts-interval '5 minutes' and e.mid_lag5>0 and e.mid>0 then (e.mid/e.mid_lag5-1.0)*10000.0 end,
                'micro.quote_size_imbalance_5m_mean',case when e.rows5=5 and e.ts_lag4=e.ts-interval '4 minutes' then e.quote_imbalance5 end
            )),
            'primary.equity_microstructure_1m',
            md5(concat_ws('|',e.instrument_id::text,e.ts::text,coalesce(e.trade_count,0)::text,coalesce(e.quote_count,0)::text,coalesce(e.total_notional,0)::text)),
            jsonb_strip_nulls(jsonb_build_object(
                'selection_class',e.selection_class,'trigger_kind',e.trigger_kind,
                'capture_run_id',e.capture_run_id,'source_provider','alpaca',
                'source_frequency','1m','adapter_version','v1'
            ))
        from enriched e
        on conflict(feature_set_key,instrument_key,decision_ts) do update set
            observable_at=excluded.observable_at,
            features=excluded.features,
            source_dataset_key=excluded.source_dataset_key,
            source_row_hash=excluded.source_row_hash,
            quality=excluded.quality
        returning 1
    )
    select count(*) into v_feature_rows from ins;

    with base as (
        select m.instrument_id,('ALPACA:'||i.provider_symbol) instrument_key,m.ts,
            m.ts+interval '1 minute' decision_ts,
            (m.last_bid_price+m.last_ask_price)/2.0 entry_mid
        from public.equity_microstructure_1m m
        join public.instruments i on i.id=m.instrument_id
        where m.provider='alpaca'
          and m.last_bid_price is not null
          and m.last_ask_price is not null
          and m.last_bid_price>0
          and m.last_ask_price>=m.last_bid_price
    ), horizons(horizon_seconds) as (
        values (60),(180),(300),(900),(1800),(3600)
    ), pairs as (
        select b.instrument_key,b.decision_ts,h.horizon_seconds,b.decision_ts entry_ts,
            x.ts+interval '1 minute' exit_ts,b.entry_mid,
            (x.last_bid_price+x.last_ask_price)/2.0 exit_mid
        from base b
        cross join horizons h
        join public.equity_microstructure_1m x
          on x.instrument_id=b.instrument_id
         and x.provider='alpaca'
         and x.ts=b.ts+make_interval(secs=>h.horizon_seconds)
         and x.last_bid_price is not null
         and x.last_ask_price is not null
         and x.last_bid_price>0
         and x.last_ask_price>=x.last_bid_price
    ), ins as (
        insert into research_hub.outcome_rows(
            outcome_set_key,instrument_key,decision_ts,horizon_seconds,entry_ts,exit_ts,
            gross_return,net_return,max_favourable_excursion,max_adverse_excursion,
            realised_volatility,metadata
        )
        select
            'equity.sip.mid_returns.v1',p.instrument_key,p.decision_ts,p.horizon_seconds,
            p.entry_ts,p.exit_ts,p.exit_mid/p.entry_mid-1.0,null,null,null,null,
            jsonb_build_object('price_basis','quote_mid','round_trip_cost_applied',false,'adapter_version','v1')
        from pairs p
        where p.entry_mid>0 and p.exit_mid>0
        on conflict(outcome_set_key,instrument_key,decision_ts,horizon_seconds) do update set
            entry_ts=excluded.entry_ts,
            exit_ts=excluded.exit_ts,
            gross_return=excluded.gross_return,
            net_return=excluded.net_return,
            max_favourable_excursion=excluded.max_favourable_excursion,
            max_adverse_excursion=excluded.max_adverse_excursion,
            realised_volatility=excluded.realised_volatility,
            metadata=excluded.metadata
        returning 1
    )
    select count(*) into v_outcome_rows from ins;

    return jsonb_build_object(
        'feature_set_key','equity.sip.microstructure.v1',
        'feature_rows_upserted',v_feature_rows,
        'outcome_set_key','equity.sip.mid_returns.v1',
        'outcome_rows_upserted',v_outcome_rows
    );
end;
$$;

revoke all on function research_hub.refresh_equity_microstructure_adapter_v1() from public,anon,authenticated;

comment on function research_hub.refresh_equity_microstructure_adapter_v1() is
'Idempotently converts research-ready Alpaca SIP one-minute microstructure into point-in-time Research Hub feature rows and exact-horizon same-instrument quote-midpoint outcomes.';
