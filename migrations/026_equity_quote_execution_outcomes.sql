insert into research_hub.outcome_definitions(
    outcome_key,outcome_name,target_asset_class,horizon_seconds,entry_rule,exit_rule,cost_model_key,enabled,metadata
)
values
('equity.quote_exec_60s','Quoted executable return 1m','equity',60,'long enters at closing ask; short enters at closing bid','long exits at future closing bid; short exits at future closing ask','quoted_spread_plus_experiment_slippage',true,'{"long_field":"gross_return","short_field":"metadata.short_quote_return","price_basis":"closing_l1_quote"}'),
('equity.quote_exec_180s','Quoted executable return 3m','equity',180,'long enters at closing ask; short enters at closing bid','long exits at future closing bid; short exits at future closing ask','quoted_spread_plus_experiment_slippage',true,'{"long_field":"gross_return","short_field":"metadata.short_quote_return","price_basis":"closing_l1_quote"}'),
('equity.quote_exec_300s','Quoted executable return 5m','equity',300,'long enters at closing ask; short enters at closing bid','long exits at future closing bid; short exits at future closing ask','quoted_spread_plus_experiment_slippage',true,'{"long_field":"gross_return","short_field":"metadata.short_quote_return","price_basis":"closing_l1_quote"}'),
('equity.quote_exec_900s','Quoted executable return 15m','equity',900,'long enters at closing ask; short enters at closing bid','long exits at future closing bid; short exits at future closing ask','quoted_spread_plus_experiment_slippage',true,'{"long_field":"gross_return","short_field":"metadata.short_quote_return","price_basis":"closing_l1_quote"}'),
('equity.quote_exec_1800s','Quoted executable return 30m','equity',1800,'long enters at closing ask; short enters at closing bid','long exits at future closing bid; short exits at future closing ask','quoted_spread_plus_experiment_slippage',true,'{"long_field":"gross_return","short_field":"metadata.short_quote_return","price_basis":"closing_l1_quote"}'),
('equity.quote_exec_3600s','Quoted executable return 60m','equity',3600,'long enters at closing ask; short enters at closing bid','long exits at future closing bid; short exits at future closing ask','quoted_spread_plus_experiment_slippage',true,'{"long_field":"gross_return","short_field":"metadata.short_quote_return","price_basis":"closing_l1_quote"}')
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
    'equity.sip.quote_exec.v1',
    'Same-instrument quoted execution returns: long ask-to-bid in gross_return and short bid-to-ask in metadata.short_quote_return. Spread crossing is therefore intrinsic; experiments may add slippage/fees.',
    array['equity.quote_exec_60s','equity.quote_exec_180s','equity.quote_exec_300s','equity.quote_exec_900s','equity.quote_exec_1800s','equity.quote_exec_3600s'],
    'research_hub','outcome_rows',
    '{"adapter":"equity_sip_quote_exec_v1","spread_crossing_included":true,"slippage_and_fees_included":false,"short_borrow_included":false}'::jsonb
)
on conflict(outcome_set_key) do update set
    description=excluded.description,
    outcome_keys=excluded.outcome_keys,
    metadata=excluded.metadata,
    updated_at=now();

create or replace function research_hub.refresh_equity_quote_execution_outcomes_v1()
returns jsonb
language plpgsql
security invoker
set search_path=pg_catalog,public,research_hub,pg_temp
as $$
declare
    v_rows bigint:=0;
begin
    with base as (
        select
            m.instrument_id,
            ('ALPACA:'||i.provider_symbol) instrument_key,
            m.ts,
            m.ts+interval '1 minute' decision_ts,
            m.last_bid_price entry_bid,
            m.last_ask_price entry_ask
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
        select
            b.instrument_key,b.decision_ts,h.horizon_seconds,b.decision_ts entry_ts,
            x.ts+interval '1 minute' exit_ts,b.entry_bid,b.entry_ask,
            x.last_bid_price exit_bid,x.last_ask_price exit_ask
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
            'equity.sip.quote_exec.v1',p.instrument_key,p.decision_ts,p.horizon_seconds,
            p.entry_ts,p.exit_ts,p.exit_bid/p.entry_ask-1.0,null,null,null,null,
            jsonb_build_object(
                'short_quote_return',p.entry_bid/p.exit_ask-1.0,
                'entry_bid',p.entry_bid,
                'entry_ask',p.entry_ask,
                'exit_bid',p.exit_bid,
                'exit_ask',p.exit_ask,
                'entry_spread_bps',(p.entry_ask-p.entry_bid)/((p.entry_ask+p.entry_bid)/2.0)*10000.0,
                'exit_spread_bps',(p.exit_ask-p.exit_bid)/((p.exit_ask+p.exit_bid)/2.0)*10000.0,
                'spread_crossing_included',true,
                'extra_slippage_included',false,
                'adapter_version','v1'
            )
        from pairs p
        where p.entry_ask>0 and p.entry_bid>0 and p.exit_ask>0 and p.exit_bid>0
        on conflict(outcome_set_key,instrument_key,decision_ts,horizon_seconds) do update set
            entry_ts=excluded.entry_ts,
            exit_ts=excluded.exit_ts,
            gross_return=excluded.gross_return,
            net_return=excluded.net_return,
            metadata=excluded.metadata
        returning 1
    )
    select count(*) into v_rows from ins;

    return jsonb_build_object(
        'outcome_set_key','equity.sip.quote_exec.v1',
        'rows_upserted',v_rows
    );
end;
$$;

revoke all on function research_hub.refresh_equity_quote_execution_outcomes_v1() from public,anon,authenticated;

comment on function research_hub.refresh_equity_quote_execution_outcomes_v1() is
'Materialises same-instrument quote-crossing returns. Long uses entry ask to future bid; short uses entry bid to future ask. Extra slippage/fees and short borrow are left to later execution modelling.';
