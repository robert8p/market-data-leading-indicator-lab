-- Reconstructible typed panel engine. No statistical screening occurs here.
-- Feature and outcome data remain physically separate and every source timestamp is checked.

create or replace function research_hub.materialize_crypto_positioning15m_symbol_v1(p_symbol text)
returns jsonb
language plpgsql
security invoker
set search_path=pg_catalog,public,research_hub,pg_temp
as $$
declare v_feature_rows bigint:=0; v_outcome_rows bigint:=0; v_source_hash text;
begin
    if not exists(
        select 1 from research_hub.binance_deriv_recovery_quality_v1 d
        join research_hub.binance_spot15m_positioning_quality_v1 s using(canonical_symbol)
        where d.run_id='1d57032e-20fa-4d23-b066-14cc659b13e2'::uuid
          and d.canonical_symbol=p_symbol and d.data_quality_pass=true and s.quality_pass=true
    ) then raise exception 'Source quality gates not passed for %',p_symbol; end if;

    delete from research_hub.crypto_positioning15m_features_v1 where canonical_symbol=p_symbol;
    delete from research_hub.crypto_positioning15m_outcomes_v1 where canonical_symbol=p_symbol;

    insert into research_hub.crypto_positioning15m_features_v1(
        canonical_symbol,decision_ts,spot_bucket_start,observable_at,
        spot_ret15,spot_ret60,spot_ret240,spot_log_quote_volume,spot_log_trade_count,
        log_oi_value,oi_chg_5m,oi_chg_15m,oi_chg_60m,
        global_ls_log,global_ls_chg_15m,global_ls_chg_60m,
        top_account_ls_log,top_account_ls_chg_15m,top_position_ls_log,top_position_ls_chg_15m,
        account_global_div,position_global_div,taker_log_ratio,taker_imbalance,taker_chg_15m,taker_chg_60m,
        ret15_x_oi15,ret60_x_oi60,taker_x_oi15,position_div_x_ret15,
        price_up_oi_up,price_down_oi_up,price_up_oi_down,price_down_oi_down,crowded_long_buying,crowded_long_selling,
        oi_metric_ts,taker_metric_ts,source_hash,quality,updated_at
    )
    with base as (
        select s.*,
               date_bin(interval '5 minutes',s.signal_ts-interval '1 minute','1970-01-01 00:00:00+00'::timestamptz) oi_ts,
               date_bin(interval '5 minutes',s.signal_ts-interval '6 minutes','1970-01-01 00:00:00+00'::timestamptz) taker_ts
        from research_hub.binance_spot15m_positioning_v1 s
        where s.canonical_symbol=p_symbol
          and s.signal_ts>='2026-07-14 00:00+00'
          and s.signal_ts<='2026-08-12 10:15+00'
    ), calc as (
        select b.canonical_symbol,b.signal_ts decision_ts,b.bucket_start,b.signal_ts observable_at,
            b.close/nullif(b.open,0)-1 spot_ret15,
            b.close/nullif(sp4.close,0)-1 spot_ret60,
            b.close/nullif(sp16.close,0)-1 spot_ret240,
            ln(1+greatest(coalesce(b.quote_volume,0),0)) spot_log_quote_volume,
            ln(1+greatest(coalesce(b.trade_count,0),0)) spot_log_trade_count,
            ln(d0.open_interest_value) log_oi_value,
            ln(d0.open_interest_value/d5.open_interest_value) oi_chg_5m,
            ln(d0.open_interest_value/d15.open_interest_value) oi_chg_15m,
            ln(d0.open_interest_value/d60.open_interest_value) oi_chg_60m,
            ln(d0.global_long_short_ratio) global_ls_log,
            ln(d0.global_long_short_ratio/d15.global_long_short_ratio) global_ls_chg_15m,
            ln(d0.global_long_short_ratio/d60.global_long_short_ratio) global_ls_chg_60m,
            ln(d0.top_account_long_short_ratio) top_account_ls_log,
            ln(d0.top_account_long_short_ratio/d15.top_account_long_short_ratio) top_account_ls_chg_15m,
            ln(d0.top_position_long_short_ratio) top_position_ls_log,
            ln(d0.top_position_long_short_ratio/d15.top_position_long_short_ratio) top_position_ls_chg_15m,
            ln(d0.top_account_long_short_ratio/d0.global_long_short_ratio) account_global_div,
            ln(d0.top_position_long_short_ratio/d0.global_long_short_ratio) position_global_div,
            ln(t0.taker_buy_sell_ratio) taker_log_ratio,
            (t0.taker_buy_sell_ratio-1)/(t0.taker_buy_sell_ratio+1) taker_imbalance,
            ln(t0.taker_buy_sell_ratio/t15.taker_buy_sell_ratio) taker_chg_15m,
            ln(t0.taker_buy_sell_ratio/t60.taker_buy_sell_ratio) taker_chg_60m,
            d0.ts oi_metric_ts,t0.ts taker_metric_ts,
            md5(concat_ws('|',b.canonical_symbol,b.bucket_start::text,b.open::text,b.close::text,
                d0.ts::text,d0.open_interest_value::text,d0.global_long_short_ratio::text,
                d0.top_account_long_short_ratio::text,d0.top_position_long_short_ratio::text,
                t0.ts::text,t0.taker_buy_sell_ratio::text)) source_hash
        from base b
        join research_hub.binance_spot15m_positioning_v1 sp4 on sp4.canonical_symbol=b.canonical_symbol and sp4.bucket_start=b.bucket_start-interval '1 hour'
        join research_hub.binance_spot15m_positioning_v1 sp16 on sp16.canonical_symbol=b.canonical_symbol and sp16.bucket_start=b.bucket_start-interval '4 hours'
        join public.crypto_derivatives_metrics d0 on d0.provider='binance_futures' and d0.interval='5m' and d0.canonical_symbol=b.canonical_symbol and d0.ts=b.oi_ts
        join public.crypto_derivatives_metrics d5 on d5.provider='binance_futures' and d5.interval='5m' and d5.canonical_symbol=b.canonical_symbol and d5.ts=b.oi_ts-interval '5 minutes'
        join public.crypto_derivatives_metrics d15 on d15.provider='binance_futures' and d15.interval='5m' and d15.canonical_symbol=b.canonical_symbol and d15.ts=b.oi_ts-interval '15 minutes'
        join public.crypto_derivatives_metrics d60 on d60.provider='binance_futures' and d60.interval='5m' and d60.canonical_symbol=b.canonical_symbol and d60.ts=b.oi_ts-interval '1 hour'
        join public.crypto_derivatives_metrics t0 on t0.provider='binance_futures' and t0.interval='5m' and t0.canonical_symbol=b.canonical_symbol and t0.ts=b.taker_ts
        join public.crypto_derivatives_metrics t15 on t15.provider='binance_futures' and t15.interval='5m' and t15.canonical_symbol=b.canonical_symbol and t15.ts=b.taker_ts-interval '15 minutes'
        join public.crypto_derivatives_metrics t60 on t60.provider='binance_futures' and t60.interval='5m' and t60.canonical_symbol=b.canonical_symbol and t60.ts=b.taker_ts-interval '1 hour'
        where d0.open_interest_value>0 and d5.open_interest_value>0 and d15.open_interest_value>0 and d60.open_interest_value>0
          and d0.global_long_short_ratio>0 and d15.global_long_short_ratio>0 and d60.global_long_short_ratio>0
          and d0.top_account_long_short_ratio>0 and d15.top_account_long_short_ratio>0
          and d0.top_position_long_short_ratio>0 and d15.top_position_long_short_ratio>0
          and t0.taker_buy_sell_ratio>0 and t15.taker_buy_sell_ratio>0 and t60.taker_buy_sell_ratio>0
          and d0.ts+interval '60 seconds'<=b.signal_ts
          and t0.ts+interval '6 minutes'<=b.signal_ts
    )
    select canonical_symbol,decision_ts,bucket_start,observable_at,
        spot_ret15,spot_ret60,spot_ret240,spot_log_quote_volume,spot_log_trade_count,
        log_oi_value,oi_chg_5m,oi_chg_15m,oi_chg_60m,
        global_ls_log,global_ls_chg_15m,global_ls_chg_60m,
        top_account_ls_log,top_account_ls_chg_15m,top_position_ls_log,top_position_ls_chg_15m,
        account_global_div,position_global_div,taker_log_ratio,taker_imbalance,taker_chg_15m,taker_chg_60m,
        spot_ret15*oi_chg_15m,spot_ret60*oi_chg_60m,taker_imbalance*oi_chg_15m,position_global_div*spot_ret15,
        spot_ret15>0 and oi_chg_15m>0,spot_ret15<0 and oi_chg_15m>0,
        spot_ret15>0 and oi_chg_15m<0,spot_ret15<0 and oi_chg_15m<0,
        position_global_div>0 and taker_imbalance>0,position_global_div>0 and taker_imbalance<0,
        oi_metric_ts,taker_metric_ts,source_hash,
        jsonb_build_object(
          'observability_contract','binance-usdm-observability-v1',
          'oi_observable_at',oi_metric_ts+interval '60 seconds',
          'taker_observable_at',taker_metric_ts+interval '6 minutes',
          'historical_window_role','discovery_validation_only','future_replication_required',true
        ),now()
    from calc;
    get diagnostics v_feature_rows=row_count;

    insert into research_hub.crypto_positioning15m_outcomes_v1(
        canonical_symbol,decision_ts,entry_open,exit_open_900s,exit_open_3600s,exit_open_14400s,
        gross_return_900s,gross_return_3600s,gross_return_14400s,
        max_favourable_excursion_14400s,max_adverse_excursion_14400s,source_hash,metadata,updated_at
    )
    select f.canonical_symbol,f.decision_ts,e.open,x15.open,x60.open,x240.open,
        x15.open/nullif(e.open,0)-1,x60.open/nullif(e.open,0)-1,x240.open/nullif(e.open,0)-1,
        path.max_high/nullif(e.open,0)-1,path.min_low/nullif(e.open,0)-1,
        md5(concat_ws('|',f.canonical_symbol,f.decision_ts::text,e.open::text,x15.open::text,x60.open::text,x240.open::text,path.max_high::text,path.min_low::text)),
        jsonb_build_object('entry_rule','next bar open at decision_ts','exit_rule','exact future bar open','costs_applied_in_experiment',true,'historical_window_role','discovery_validation_only','future_replication_required',true),now()
    from research_hub.crypto_positioning15m_features_v1 f
    join research_hub.binance_spot15m_positioning_v1 e on e.canonical_symbol=f.canonical_symbol and e.bucket_start=f.decision_ts
    join research_hub.binance_spot15m_positioning_v1 x15 on x15.canonical_symbol=f.canonical_symbol and x15.bucket_start=f.decision_ts+interval '15 minutes'
    join research_hub.binance_spot15m_positioning_v1 x60 on x60.canonical_symbol=f.canonical_symbol and x60.bucket_start=f.decision_ts+interval '1 hour'
    join research_hub.binance_spot15m_positioning_v1 x240 on x240.canonical_symbol=f.canonical_symbol and x240.bucket_start=f.decision_ts+interval '4 hours'
    join lateral (
        select max(p.high) max_high,min(p.low) min_low,count(*) bars
        from research_hub.binance_spot15m_positioning_v1 p
        where p.canonical_symbol=f.canonical_symbol and p.bucket_start>=f.decision_ts and p.bucket_start<f.decision_ts+interval '4 hours'
    ) path on path.bars=16
    where f.canonical_symbol=p_symbol and e.open>0;
    get diagnostics v_outcome_rows=row_count;

    select md5(concat_ws('|',p_symbol,v_feature_rows::text,v_outcome_rows::text,
        coalesce(min(decision_ts)::text,''),coalesce(max(decision_ts)::text,'')))
    into v_source_hash from research_hub.crypto_positioning15m_features_v1 where canonical_symbol=p_symbol;

    return jsonb_build_object('status','materialized','canonical_symbol',p_symbol,'feature_rows',v_feature_rows,
        'outcome_rows',v_outcome_rows,'partition_hash',v_source_hash,'holdout_accessed',false);
end;
$$;
revoke all on function research_hub.materialize_crypto_positioning15m_symbol_v1(text) from public,anon,authenticated;

create or replace function research_hub.finalize_crypto_positioning15m_symbol_z_v1(p_symbol text)
returns bigint
language plpgsql
security invoker
set search_path=pg_catalog,research_hub,pg_temp
as $$
declare v_rows bigint;
begin
    with s as (
        select canonical_symbol,decision_ts,count(*) over w n,
            avg(oi_chg_15m) over w m_oi15,stddev_samp(oi_chg_15m) over w sd_oi15,
            avg(oi_chg_60m) over w m_oi60,stddev_samp(oi_chg_60m) over w sd_oi60,
            avg(global_ls_log) over w m_gl,stddev_samp(global_ls_log) over w sd_gl,
            avg(top_position_ls_log) over w m_tp,stddev_samp(top_position_ls_log) over w sd_tp,
            avg(position_global_div) over w m_pd,stddev_samp(position_global_div) over w sd_pd,
            avg(taker_imbalance) over w m_ti,stddev_samp(taker_imbalance) over w sd_ti,
            avg(spot_ret15) over w m_sr,stddev_samp(spot_ret15) over w sd_sr,
            avg(spot_log_quote_volume) over w m_qv,stddev_samp(spot_log_quote_volume) over w sd_qv
        from research_hub.crypto_positioning15m_features_v1
        where canonical_symbol=p_symbol
        window w as (partition by canonical_symbol order by decision_ts rows between 95 preceding and current row)
    )
    update research_hub.crypto_positioning15m_features_v1 f set
        oi_chg15_z_1d=case when s.n>=24 and s.sd_oi15>0 then (f.oi_chg_15m-s.m_oi15)/s.sd_oi15 end,
        oi_chg60_z_1d=case when s.n>=24 and s.sd_oi60>0 then (f.oi_chg_60m-s.m_oi60)/s.sd_oi60 end,
        global_ls_z_1d=case when s.n>=24 and s.sd_gl>0 then (f.global_ls_log-s.m_gl)/s.sd_gl end,
        top_position_ls_z_1d=case when s.n>=24 and s.sd_tp>0 then (f.top_position_ls_log-s.m_tp)/s.sd_tp end,
        position_div_z_1d=case when s.n>=24 and s.sd_pd>0 then (f.position_global_div-s.m_pd)/s.sd_pd end,
        taker_imbalance_z_1d=case when s.n>=24 and s.sd_ti>0 then (f.taker_imbalance-s.m_ti)/s.sd_ti end,
        spot_ret15_z_1d=case when s.n>=24 and s.sd_sr>0 then (f.spot_ret15-s.m_sr)/s.sd_sr end,
        spot_log_quote_volume_z_1d=case when s.n>=24 and s.sd_qv>0 then (f.spot_log_quote_volume-s.m_qv)/s.sd_qv end,
        updated_at=now()
    from s where f.canonical_symbol=s.canonical_symbol and f.decision_ts=s.decision_ts;
    get diagnostics v_rows=row_count; return v_rows;
end;
$$;
revoke all on function research_hub.finalize_crypto_positioning15m_symbol_z_v1(text) from public,anon,authenticated;

create or replace function research_hub.crypto_positioning15m_work_complete_z_v1()
returns trigger language plpgsql security invoker
set search_path=pg_catalog,research_hub,pg_temp
as $$
begin
    if new.status='completed' and old.status is distinct from 'completed' then
        perform research_hub.finalize_crypto_positioning15m_symbol_z_v1(new.canonical_symbol);
        new.metadata:=coalesce(new.metadata,'{}'::jsonb)||jsonb_build_object('rolling_z_finalized',true,'rolling_window_bars',96,'minimum_window_bars',24);
    end if;
    return new;
end;
$$;
drop trigger if exists crypto_positioning15m_work_complete_z_v1 on research_hub.crypto_positioning15m_work_v1;
create trigger crypto_positioning15m_work_complete_z_v1 before update of status on research_hub.crypto_positioning15m_work_v1
for each row execute function research_hub.crypto_positioning15m_work_complete_z_v1();

create or replace function research_hub.finalize_crypto_positioning15m_rank_day_v1(p_date date)
returns bigint language plpgsql security invoker
set search_path=pg_catalog,research_hub,pg_temp
as $$
declare v_rows bigint;
begin
    with r as (
        select canonical_symbol,decision_ts,
            percent_rank() over(partition by decision_ts order by oi_chg_15m) r_oi,
            percent_rank() over(partition by decision_ts order by global_ls_log) r_global,
            percent_rank() over(partition by decision_ts order by top_position_ls_log) r_top,
            percent_rank() over(partition by decision_ts order by taker_imbalance) r_taker,
            percent_rank() over(partition by decision_ts order by spot_ret15) r_ret
        from research_hub.crypto_positioning15m_features_v1
        where decision_ts>=p_date::timestamptz and decision_ts<(p_date+1)::timestamptz
    )
    update research_hub.crypto_positioning15m_features_v1 f set
        cs_oi_chg15_rank=r.r_oi,cs_global_ls_rank=r.r_global,cs_top_position_rank=r.r_top,
        cs_taker_imbalance_rank=r.r_taker,cs_spot_ret15_rank=r.r_ret,updated_at=now()
    from r where f.canonical_symbol=r.canonical_symbol and f.decision_ts=r.decision_ts;
    get diagnostics v_rows=row_count; return v_rows;
end;
$$;
revoke all on function research_hub.finalize_crypto_positioning15m_rank_day_v1(date) from public,anon,authenticated;

create or replace function research_hub.refresh_crypto_positioning15m_work_v1()
returns jsonb language plpgsql security invoker
set search_path=pg_catalog,public,research_hub,pg_temp
as $$
declare v_deriv_pass bigint; v_spot_terminal bigint; v_spot_pass bigint; v_work_total bigint; v_work_done bigint;
        v_rank_total bigint; v_rank_done bigint; v_feature_rows bigint; v_outcome_rows bigint; v_state text;
begin
    select count(*) into v_deriv_pass from research_hub.binance_deriv_recovery_quality_v1
    where run_id='1d57032e-20fa-4d23-b066-14cc659b13e2'::uuid and data_quality_pass=true;
    select count(*) into v_spot_terminal from research_hub.binance_spot15m_positioning_work_v1
    where status in ('completed','completed_empty','failed');
    select count(*) into v_spot_pass from research_hub.binance_spot15m_positioning_quality_v1 where quality_pass=true;

    insert into research_hub.crypto_positioning15m_work_v1(canonical_symbol,status,metadata)
    select d.canonical_symbol,'queued',jsonb_build_object('derivatives_quality_pass',true,'spot_quality_pass',true,'historical_window_role','discovery_validation_only','future_replication_required',true)
    from research_hub.binance_deriv_recovery_quality_v1 d
    join research_hub.binance_spot15m_positioning_quality_v1 s using(canonical_symbol)
    where d.run_id='1d57032e-20fa-4d23-b066-14cc659b13e2'::uuid and d.data_quality_pass=true and s.quality_pass=true
    on conflict(canonical_symbol) do update set
        metadata=research_hub.crypto_positioning15m_work_v1.metadata||excluded.metadata,
        status=case when research_hub.crypto_positioning15m_work_v1.status='waiting_source_quality' then 'queued' else research_hub.crypto_positioning15m_work_v1.status end,
        updated_at=now();

    select count(*),count(*) filter(where status='completed') into v_work_total,v_work_done from research_hub.crypto_positioning15m_work_v1;
    select coalesce(sum(feature_rows),0),coalesce(sum(outcome_rows),0) into v_feature_rows,v_outcome_rows from research_hub.crypto_positioning15m_work_v1;
    select count(*),count(*) filter(where status='completed') into v_rank_total,v_rank_done from research_hub.crypto_positioning15m_rank_work_v1;

    v_state:=case
        when v_spot_terminal<v_deriv_pass then 'waiting_for_spot_recovery_quality'
        when v_spot_pass<v_deriv_pass then 'waiting_for_all_spot_quality_audits'
        when v_work_done<v_work_total then 'materializing_typed_positioning_panel'
        when v_rank_total=0 then 'ready_to_plan_cross_sectional_rank_work'
        when v_rank_done<v_rank_total then 'finalizing_cross_sectional_ranks'
        else 'ready_for_frozen_experiment_manifest' end;

    update research_hub.program_jobs set current_state=v_state,
        progress_current=v_work_done+v_rank_done,
        progress_total=v_work_total+case when v_rank_total>0 then v_rank_total else 30 end,
        completion_pct=case when v_work_total+greatest(v_rank_total,30)>0 then 100.0*(v_work_done+v_rank_done)/(v_work_total+greatest(v_rank_total,30)) else 0 end,
        latest_result=jsonb_build_object(
          'derivatives_quality_pass_symbols',v_deriv_pass,'spot_recovery_terminal_symbols',v_spot_terminal,
          'spot_quality_pass_symbols',v_spot_pass,'materialization_symbols_total',v_work_total,
          'materialization_symbols_completed',v_work_done,'feature_rows',v_feature_rows,'outcome_rows',v_outcome_rows,
          'rank_days_total',v_rank_total,'rank_days_completed',v_rank_done,'historical_holdout_available',false,'future_replication_required',true),
        retry_state='automatic bounded point-in-time materialization',
        next_automatic_action=case
          when v_state like 'waiting%' then 'Continue public spot recovery and audits; no credentials required.'
          when v_state='materializing_typed_positioning_panel' then 'Materialise one quality-passing symbol per clean compute slot.'
          when v_state='ready_to_plan_cross_sectional_rank_work' then 'Create frozen UTC-date rank tasks.'
          when v_state='finalizing_cross_sectional_ranks' then 'Finalize one cross-sectional date per clean compute slot.'
          else 'Execute only the frozen experiment manifest.' end,
        intervention_required=false,exact_intervention=null,updated_at=now()
    where job_key='FEATURE-CRYPTO-POSITIONING-V1';

    return jsonb_build_object('state',v_state,'derivatives_quality_pass',v_deriv_pass,'spot_terminal',v_spot_terminal,
      'spot_quality_pass',v_spot_pass,'work_total',v_work_total,'work_done',v_work_done,
      'rank_total',v_rank_total,'rank_done',v_rank_done,'feature_rows',v_feature_rows,'outcome_rows',v_outcome_rows);
end;
$$;
revoke all on function research_hub.refresh_crypto_positioning15m_work_v1() from public,anon,authenticated;

create or replace function research_hub.crypto_positioning_compute_pressure_v1()
returns jsonb language sql stable set search_path=pg_catalog,pg_temp
as $$
with busy as (
    select pid,now()-query_start elapsed,left(query,160) q from pg_stat_activity
    where pid<>pg_backend_pid() and state='active' and now()-query_start>interval '45 seconds'
      and query not ilike '%refresh_crypto_positioning15m_work_v1%'
      and query not ilike '%advance_crypto_positioning15m_v1%'
)
select jsonb_build_object('busy',exists(select 1 from busy),'active_heavy_sessions',coalesce((select jsonb_agg(jsonb_build_object('pid',pid,'elapsed_seconds',extract(epoch from elapsed),'query',q)) from busy),'[]'::jsonb))
$$;
revoke all on function research_hub.crypto_positioning_compute_pressure_v1() from public,anon,authenticated;

create or replace function research_hub.advance_crypto_positioning15m_v1()
returns jsonb language plpgsql security invoker
set search_path=pg_catalog,public,research_hub,pg_temp
as $$
declare v_state jsonb; v_pressure jsonb; v_symbol text; v_date date; v_result jsonb; v_rows bigint; v_total bigint; v_done bigint;
begin
    if not pg_try_advisory_xact_lock(hashtext('research_hub.advance_crypto_positioning15m_v1')) then return jsonb_build_object('status','busy_advisory_lock'); end if;
    v_state:=research_hub.refresh_crypto_positioning15m_work_v1();
    if v_state->>'state' in ('waiting_for_spot_recovery_quality','waiting_for_all_spot_quality_audits') then return jsonb_build_object('status',v_state->>'state','state',v_state); end if;
    v_pressure:=research_hub.crypto_positioning_compute_pressure_v1();
    if coalesce((v_pressure->>'busy')::boolean,false) then return jsonb_build_object('status','waiting_for_clean_compute_slot','pressure',v_pressure,'state',v_state); end if;

    select canonical_symbol into v_symbol from research_hub.crypto_positioning15m_work_v1
    where status in ('queued','retry_wait') and attempts<max_attempts order by priority desc,canonical_symbol for update skip locked limit 1;
    if v_symbol is not null then
        update research_hub.crypto_positioning15m_work_v1 set status='running',attempts=attempts+1,started_at=now(),last_error=null,updated_at=now() where canonical_symbol=v_symbol;
        begin
            v_result:=research_hub.materialize_crypto_positioning15m_symbol_v1(v_symbol);
            update research_hub.crypto_positioning15m_work_v1 set status='completed',feature_rows=(v_result->>'feature_rows')::bigint,
                outcome_rows=(v_result->>'outcome_rows')::bigint,completed_at=now(),metadata=metadata||jsonb_build_object('partition_hash',v_result->>'partition_hash'),updated_at=now()
            where canonical_symbol=v_symbol;
            perform research_hub.refresh_crypto_positioning15m_work_v1();
            return jsonb_build_object('status','symbol_completed','canonical_symbol',v_symbol,'result',v_result);
        exception when others then
            update research_hub.crypto_positioning15m_work_v1 set status=case when attempts>=max_attempts then 'failed' else 'retry_wait' end,last_error=left(sqlerrm,4000),updated_at=now() where canonical_symbol=v_symbol;
            return jsonb_build_object('status','symbol_failed','canonical_symbol',v_symbol,'error',sqlerrm);
        end;
    end if;

    select count(*),count(*) filter(where status='completed') into v_total,v_done from research_hub.crypto_positioning15m_work_v1;
    if v_total>0 and v_done=v_total and not exists(select 1 from research_hub.crypto_positioning15m_rank_work_v1) then
        insert into research_hub.crypto_positioning15m_rank_work_v1(decision_date)
        select d::date from generate_series('2026-07-14'::date,'2026-08-12'::date,interval '1 day') d on conflict do nothing;
    end if;

    select decision_date into v_date from research_hub.crypto_positioning15m_rank_work_v1
    where status in ('queued','retry_wait') and attempts<max_attempts order by decision_date for update skip locked limit 1;
    if v_date is not null then
        update research_hub.crypto_positioning15m_rank_work_v1 set status='running',attempts=attempts+1,started_at=now(),last_error=null,updated_at=now() where decision_date=v_date;
        begin
            v_rows:=research_hub.finalize_crypto_positioning15m_rank_day_v1(v_date);
            update research_hub.crypto_positioning15m_rank_work_v1 set status='completed',rows_updated=v_rows,completed_at=now(),updated_at=now() where decision_date=v_date;
            perform research_hub.refresh_crypto_positioning15m_work_v1();
            return jsonb_build_object('status','rank_day_completed','decision_date',v_date,'rows_updated',v_rows);
        exception when others then
            update research_hub.crypto_positioning15m_rank_work_v1 set status=case when attempts>=max_attempts then 'failed' else 'retry_wait' end,last_error=left(sqlerrm,4000),updated_at=now() where decision_date=v_date;
            return jsonb_build_object('status','rank_day_failed','decision_date',v_date,'error',sqlerrm);
        end;
    end if;

    update research_hub.crypto_positioning15m_control_v1 set cross_sectional_ranks_finalized=true,updated_at=now()
    where singleton=true and not exists(select 1 from research_hub.crypto_positioning15m_rank_work_v1 where status<>'completed');
    v_state:=research_hub.refresh_crypto_positioning15m_work_v1();
    return jsonb_build_object('status',v_state->>'state','state',v_state);
end;
$$;
revoke all on function research_hub.advance_crypto_positioning15m_v1() from public,anon,authenticated;

select research_hub.refresh_crypto_positioning15m_work_v1();

do $do$
begin
    if exists(select 1 from cron.job where jobname='research_hub_crypto_positioning15m_v1') then perform cron.unschedule((select jobid from cron.job where jobname='research_hub_crypto_positioning15m_v1' limit 1)); end if;
    perform cron.schedule('research_hub_crypto_positioning15m_v1','* * * * *','select research_hub.advance_crypto_positioning15m_v1();');
end $do$;
