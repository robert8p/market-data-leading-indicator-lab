create table if not exists research_hub.psg_forward_execution_audit_v1(
    decision_ts timestamptz not null,
    notional_usdt integer not null,
    strategy_version text not null default 'merp.psg.short4h.nonoverlap.v1',
    entry_snapshot_at timestamptz,
    exit_snapshot_at timestamptz,
    entry_snapshot_distance_seconds double precision,
    exit_snapshot_distance_seconds double precision,
    entry_bid double precision,
    entry_sell_slippage_bps double precision,
    entry_sell_vwap double precision,
    exit_ask double precision,
    exit_buy_slippage_bps double precision,
    exit_buy_vwap double precision,
    gross_execution_short_return double precision,
    net_after_20bps_exchange_fees double precision,
    borrow_cost_included boolean not null default false,
    status text not null,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    primary key(decision_ts,notional_usdt)
);
create index if not exists psg_forward_execution_audit_status_idx
    on research_hub.psg_forward_execution_audit_v1(status,decision_ts desc);
revoke all on table research_hub.psg_forward_execution_audit_v1 from public,anon,authenticated;

create or replace function research_hub.refresh_psg_forward_execution_audit_v1()
returns jsonb
language plpgsql
security invoker
set search_path=pg_catalog,research_hub,public,pg_temp
as $$
declare
    s record;
    v_notional integer;
    e research_hub.psg_execution_microstructure_v1%rowtype;
    x research_hub.psg_execution_microstructure_v1%rowtype;
    v_entry_slip double precision;
    v_exit_slip double precision;
    v_entry_vwap double precision;
    v_exit_vwap double precision;
    v_gross double precision;
    v_status text;
    v_rows integer:=0;
    v_complete integer:=0;
    v_missing integer:=0;
    v_summary jsonb;
begin
    for s in
        select decision_ts,entry_open,exit_open_4h,outcome_finalized_at
        from research_hub.psg_forward_signal_v1
        where accepted_nonoverlap=true
          and outcome_finalized_at is not null
        order by decision_ts
    loop
        foreach v_notional in array array[500,1000,2000]
        loop
            select * into e
            from research_hub.psg_execution_microstructure_v1 m
            where m.observed_at between s.decision_ts-interval '3 minutes' and s.decision_ts+interval '3 minutes'
            order by abs(extract(epoch from (m.observed_at-s.decision_ts)))
            limit 1;

            select * into x
            from research_hub.psg_execution_microstructure_v1 m
            where m.observed_at between s.decision_ts+interval '4 hours'-interval '3 minutes'
                                      and s.decision_ts+interval '4 hours'+interval '3 minutes'
            order by abs(extract(epoch from (m.observed_at-(s.decision_ts+interval '4 hours'))))
            limit 1;

            v_entry_slip:=case v_notional
                when 500 then e.sell_slippage_500_bps
                when 1000 then e.sell_slippage_1000_bps
                when 2000 then e.sell_slippage_2000_bps
                else null end;
            v_exit_slip:=case v_notional
                when 500 then x.buy_slippage_500_bps
                when 1000 then x.buy_slippage_1000_bps
                when 2000 then x.buy_slippage_2000_bps
                else null end;

            if e.observed_at is null then
                v_status:='ENTRY_ORDERBOOK_SNAPSHOT_MISSING';
                v_entry_vwap:=null; v_exit_vwap:=null; v_gross:=null;
                v_missing:=v_missing+1;
            elsif x.observed_at is null then
                v_status:='EXIT_ORDERBOOK_SNAPSHOT_MISSING';
                v_entry_vwap:=case when e.bid_price>0 and v_entry_slip is not null then e.bid_price*(1-v_entry_slip/10000.0) end;
                v_exit_vwap:=null; v_gross:=null;
                v_missing:=v_missing+1;
            elsif v_entry_slip is null or v_exit_slip is null then
                v_status:='DEPTH_INSUFFICIENT_FOR_NOTIONAL';
                v_entry_vwap:=null; v_exit_vwap:=null; v_gross:=null;
                v_missing:=v_missing+1;
            else
                v_entry_vwap:=e.bid_price*(1-v_entry_slip/10000.0);
                v_exit_vwap:=x.ask_price*(1+v_exit_slip/10000.0);
                v_gross:=case when v_entry_vwap>0 then (v_entry_vwap-v_exit_vwap)/v_entry_vwap end;
                v_status:='COMPLETE_REAL_ORDERBOOK_EXECUTION';
                v_complete:=v_complete+1;
            end if;

            insert into research_hub.psg_forward_execution_audit_v1(
                decision_ts,notional_usdt,entry_snapshot_at,exit_snapshot_at,
                entry_snapshot_distance_seconds,exit_snapshot_distance_seconds,
                entry_bid,entry_sell_slippage_bps,entry_sell_vwap,
                exit_ask,exit_buy_slippage_bps,exit_buy_vwap,
                gross_execution_short_return,net_after_20bps_exchange_fees,
                borrow_cost_included,status,metadata,updated_at
            ) values(
                s.decision_ts,v_notional,e.observed_at,x.observed_at,
                case when e.observed_at is not null then abs(extract(epoch from (e.observed_at-s.decision_ts))) end,
                case when x.observed_at is not null then abs(extract(epoch from (x.observed_at-(s.decision_ts+interval '4 hours')))) end,
                e.bid_price,v_entry_slip,v_entry_vwap,
                x.ask_price,v_exit_slip,v_exit_vwap,
                v_gross,case when v_gross is not null then v_gross-0.002 end,
                false,v_status,
                jsonb_build_object(
                    'signal_version','merp.psg.short4h.nonoverlap.v1',
                    'signal_rule','PSGUSDT completed 15m trade_count >= 847',
                    'entry_execution','sell into observed Binance PSGUSDT bids',
                    'exit_execution','buy from observed Binance PSGUSDT asks after exactly 4h',
                    'fee_assumption_bps_round_trip',20,
                    'borrow_cost_pending_account_specific_api',true,
                    'no_signal_retuning',true
                ),now()
            )
            on conflict(decision_ts,notional_usdt) do update set
                entry_snapshot_at=excluded.entry_snapshot_at,exit_snapshot_at=excluded.exit_snapshot_at,
                entry_snapshot_distance_seconds=excluded.entry_snapshot_distance_seconds,
                exit_snapshot_distance_seconds=excluded.exit_snapshot_distance_seconds,
                entry_bid=excluded.entry_bid,entry_sell_slippage_bps=excluded.entry_sell_slippage_bps,
                entry_sell_vwap=excluded.entry_sell_vwap,exit_ask=excluded.exit_ask,
                exit_buy_slippage_bps=excluded.exit_buy_slippage_bps,exit_buy_vwap=excluded.exit_buy_vwap,
                gross_execution_short_return=excluded.gross_execution_short_return,
                net_after_20bps_exchange_fees=excluded.net_after_20bps_exchange_fees,
                borrow_cost_included=false,status=excluded.status,metadata=excluded.metadata,updated_at=now();
            v_rows:=v_rows+1;
        end loop;
    end loop;

    select jsonb_build_object(
        'audited_rows',count(*),
        'signals',count(distinct decision_ts),
        'complete_rows',count(*) filter(where status='COMPLETE_REAL_ORDERBOOK_EXECUTION'),
        'missing_or_insufficient_rows',count(*) filter(where status<>'COMPLETE_REAL_ORDERBOOK_EXECUTION'),
        'complete_signals',count(distinct decision_ts) filter(where status='COMPLETE_REAL_ORDERBOOK_EXECUTION'),
        'mean_net20_500',avg(net_after_20bps_exchange_fees) filter(where notional_usdt=500 and status='COMPLETE_REAL_ORDERBOOK_EXECUTION'),
        'mean_net20_1000',avg(net_after_20bps_exchange_fees) filter(where notional_usdt=1000 and status='COMPLETE_REAL_ORDERBOOK_EXECUTION'),
        'mean_net20_2000',avg(net_after_20bps_exchange_fees) filter(where notional_usdt=2000 and status='COMPLETE_REAL_ORDERBOOK_EXECUTION'),
        'borrow_cost_included',false,
        'deployment_ready',false
    ) into v_summary
    from research_hub.psg_forward_execution_audit_v1;

    update research_hub.program_jobs
    set latest_result=coalesce(latest_result,'{}'::jsonb)||jsonb_build_object('prospective_real_orderbook_execution',v_summary),
        next_automatic_action='Continue frozen PSG signal, signal-time order-book and four-hour exit-order-book collection. Borrow cost and compliant short access remain separate hard gates.',
        intervention_required=false,exact_intervention=null,updated_at=now()
    where job_key='EXEC-MERP-PSG-V1';

    return v_summary||jsonb_build_object('rows_touched_this_run',v_rows,'complete_evaluations_this_run',v_complete,'missing_evaluations_this_run',v_missing);
end;
$$;
revoke all on function research_hub.refresh_psg_forward_execution_audit_v1() from public,anon,authenticated;

update research_hub.merp_psg_execution_validation_v1
set signal_definition=signal_definition||jsonb_build_object(
        'equivalent_integer_trigger','trade_count >= 847 on the completed 15-minute PSGUSDT bar',
        'integer_equivalence_verified',true
    ),
    execution_definition=execution_definition||jsonb_build_object(
        'prospective_orderbook_notionals_usdt',jsonb_build_array(500,1000,2000),
        'signal_time_snapshot_tolerance_seconds',180,
        'exit_time_snapshot_tolerance_seconds',180,
        'borrow_cost_separate_hard_gate',true
    ),updated_at=now()
where candidate_id='RH-1F6255D317EE';

do $do$
begin
    if exists(select 1 from cron.job where jobname='research_hub_psg_forward_execution_audit_v1') then
        perform cron.unschedule((select jobid from cron.job where jobname='research_hub_psg_forward_execution_audit_v1' limit 1));
    end if;
    perform cron.schedule(
        'research_hub_psg_forward_execution_audit_v1','8,23,38,53 * * * *',
        'select research_hub.refresh_psg_forward_execution_audit_v1();'
    );
end $do$;
