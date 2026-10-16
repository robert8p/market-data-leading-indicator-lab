create table if not exists research_hub.psg_execution_microstructure_v1(
    observed_at timestamptz primary key,
    venue text not null default 'binance_spot',
    venue_symbol text not null default 'PSGUSDT',
    bid_price double precision not null,
    ask_price double precision not null,
    bid_qty double precision,
    ask_qty double precision,
    mid_price double precision not null,
    spread_bps double precision not null,
    bid_top_notional_usdt double precision,
    ask_top_notional_usdt double precision,
    sell_slippage_500_bps double precision,
    buy_slippage_500_bps double precision,
    sell_slippage_1000_bps double precision,
    buy_slippage_1000_bps double precision,
    sell_slippage_2000_bps double precision,
    buy_slippage_2000_bps double precision,
    sell_slippage_5000_bps double precision,
    buy_slippage_5000_bps double precision,
    depth_levels integer,
    source text not null default 'binance_public_rest',
    metadata jsonb not null default '{}'::jsonb,
    inserted_at timestamptz not null default now()
);
create index if not exists psg_exec_microstructure_ts_desc_idx on research_hub.psg_execution_microstructure_v1(observed_at desc);
revoke all on table research_hub.psg_execution_microstructure_v1 from public,anon,authenticated;

create table if not exists research_hub.execution_monitor_leases(
    lane_key text primary key,
    next_allowed_at timestamptz not null default now(),
    last_claimed_at timestamptz,
    claims bigint not null default 0,
    updated_at timestamptz not null default now()
);
insert into research_hub.execution_monitor_leases(lane_key,next_allowed_at)
values('psg_execution_microstructure_v1',now()) on conflict(lane_key) do nothing;
revoke all on table research_hub.execution_monitor_leases from public,anon,authenticated;

create or replace function public.claim_psg_execution_microstructure_v1()
returns jsonb language plpgsql security definer
set search_path=pg_catalog,research_hub,public,pg_temp
as $$
declare v_lease research_hub.execution_monitor_leases%rowtype;
begin
 select * into v_lease from research_hub.execution_monitor_leases where lane_key='psg_execution_microstructure_v1' for update;
 if v_lease.next_allowed_at>now() then return jsonb_build_object('status','rate_limited','next_allowed_at',v_lease.next_allowed_at); end if;
 update research_hub.execution_monitor_leases set next_allowed_at=now()+interval '10 minutes',last_claimed_at=now(),claims=claims+1,updated_at=now() where lane_key='psg_execution_microstructure_v1';
 return jsonb_build_object('status','claimed','claimed_at',now());
end;$$;
revoke all on function public.claim_psg_execution_microstructure_v1() from public,anon,authenticated;
grant execute on function public.claim_psg_execution_microstructure_v1() to service_role;

create or replace function public.insert_psg_execution_microstructure_v1(
 p_observed_at timestamptz,p_bid_price double precision,p_ask_price double precision,p_bid_qty double precision,p_ask_qty double precision,
 p_spread_bps double precision,p_bid_top_notional double precision,p_ask_top_notional double precision,
 p_sell500 double precision,p_buy500 double precision,p_sell1000 double precision,p_buy1000 double precision,
 p_sell2000 double precision,p_buy2000 double precision,p_sell5000 double precision,p_buy5000 double precision,p_depth_levels integer,p_metadata jsonb
)
returns jsonb language plpgsql security definer
set search_path=pg_catalog,research_hub,public,pg_temp
as $$
declare v_mid double precision;
begin
 v_mid:=(p_bid_price+p_ask_price)/2.0;
 insert into research_hub.psg_execution_microstructure_v1(
  observed_at,bid_price,ask_price,bid_qty,ask_qty,mid_price,spread_bps,bid_top_notional_usdt,ask_top_notional_usdt,
  sell_slippage_500_bps,buy_slippage_500_bps,sell_slippage_1000_bps,buy_slippage_1000_bps,
  sell_slippage_2000_bps,buy_slippage_2000_bps,sell_slippage_5000_bps,buy_slippage_5000_bps,depth_levels,metadata
 ) values(
  p_observed_at,p_bid_price,p_ask_price,p_bid_qty,p_ask_qty,v_mid,p_spread_bps,p_bid_top_notional,p_ask_top_notional,
  p_sell500,p_buy500,p_sell1000,p_buy1000,p_sell2000,p_buy2000,p_sell5000,p_buy5000,p_depth_levels,coalesce(p_metadata,'{}'::jsonb)
 ) on conflict(observed_at) do nothing;
 return jsonb_build_object('status','inserted','observed_at',p_observed_at,'spread_bps',p_spread_bps);
end;$$;
revoke all on function public.insert_psg_execution_microstructure_v1(timestamptz,double precision,double precision,double precision,double precision,double precision,double precision,double precision,double precision,double precision,double precision,double precision,double precision,double precision,double precision,double precision,integer,jsonb) from public,anon,authenticated;
grant execute on function public.insert_psg_execution_microstructure_v1(timestamptz,double precision,double precision,double precision,double precision,double precision,double precision,double precision,double precision,double precision,double precision,double precision,double precision,double precision,double precision,double precision,integer,jsonb) to service_role;

create or replace function research_hub.invoke_psg_execution_microstructure_v1()
returns jsonb language plpgsql security invoker
set search_path=pg_catalog,research_hub,public,net,pg_temp
as $$
declare v_request_id bigint;
begin
 select net.http_post(url:='https://oxzabweahkoimtevbbny.supabase.co/functions/v1/psg-execution-microstructure-v1',body:='{}'::jsonb,headers:=jsonb_build_object('content-type','application/json'),timeout_milliseconds:=30000) into v_request_id;
 return jsonb_build_object('status','requested','request_id',v_request_id);
end;$$;
revoke all on function research_hub.invoke_psg_execution_microstructure_v1() from public,anon,authenticated;

create or replace function research_hub.refresh_psg_execution_microstructure_v1()
returns jsonb language plpgsql security invoker
set search_path=pg_catalog,research_hub,public,pg_temp
as $$
declare
 v_n bigint; v_first timestamptz; v_last timestamptz; v_days double precision;
 v_med_spread double precision; v_p95_spread double precision;
 v_med_sell500 double precision; v_p95_sell500 double precision; v_med_buy500 double precision; v_p95_buy500 double precision;
 v_med_sell1000 double precision; v_p95_sell1000 double precision; v_med_buy1000 double precision; v_p95_buy1000 double precision; v_result jsonb;
begin
 select count(*),min(observed_at),max(observed_at),percentile_cont(0.5) within group(order by spread_bps),percentile_cont(0.95) within group(order by spread_bps),percentile_cont(0.5) within group(order by sell_slippage_500_bps),percentile_cont(0.95) within group(order by sell_slippage_500_bps),percentile_cont(0.5) within group(order by buy_slippage_500_bps),percentile_cont(0.95) within group(order by buy_slippage_500_bps),percentile_cont(0.5) within group(order by sell_slippage_1000_bps),percentile_cont(0.95) within group(order by sell_slippage_1000_bps),percentile_cont(0.5) within group(order by buy_slippage_1000_bps),percentile_cont(0.95) within group(order by buy_slippage_1000_bps)
 into v_n,v_first,v_last,v_med_spread,v_p95_spread,v_med_sell500,v_p95_sell500,v_med_buy500,v_p95_buy500,v_med_sell1000,v_p95_sell1000,v_med_buy1000,v_p95_buy1000 from research_hub.psg_execution_microstructure_v1;
 v_days:=case when v_first is not null and v_last is not null then extract(epoch from (v_last-v_first))/86400.0 else 0 end;
 v_result:=jsonb_build_object('snapshots',v_n,'first_observed_at',v_first,'last_observed_at',v_last,'coverage_days',v_days,'median_spread_bps',v_med_spread,'p95_spread_bps',v_p95_spread,'median_sell500_slippage_bps',v_med_sell500,'p95_sell500_slippage_bps',v_p95_sell500,'median_buy500_slippage_bps',v_med_buy500,'p95_buy500_slippage_bps',v_p95_buy500,'median_sell1000_slippage_bps',v_med_sell1000,'p95_sell1000_slippage_bps',v_p95_sell1000,'median_buy1000_slippage_bps',v_med_buy1000,'p95_buy1000_slippage_bps',v_p95_buy1000,'minimum_prospective_evidence_before_microstructure_decision',jsonb_build_object('coverage_days',7,'snapshots',500),'microstructure_decision_ready',v_days>=7 and v_n>=500);
 update research_hub.program_jobs set latest_result=coalesce(latest_result,'{}'::jsonb)||jsonb_build_object('prospective_microstructure',v_result),progress_current=least(v_n,500),progress_total=500,completion_pct=least(100.0,100.0*v_n/500.0),latest_successful_checkpoint=v_last,current_state=case when v_days>=7 and v_n>=500 then 'microstructure_evidence_ready_short_access_still_required' else 'accumulating_prospective_microstructure_short_access_blocked' end,next_automatic_action=case when v_days>=7 and v_n>=500 then 'Evaluate the frozen PSG strategy against the prospective spread/depth distribution and verify UK-accessible borrow. Do not retune signal.' else 'Continue 15-minute PSGUSDT public spread/depth capture and verify a UK-accessible borrow route. Do not retune signal or reuse holdout.' end,intervention_required=false,exact_intervention=null,updated_at=now() where job_key='EXEC-MERP-PSG-V1';
 return v_result;
end;$$;
revoke all on function research_hub.refresh_psg_execution_microstructure_v1() from public,anon,authenticated;

do $do$
begin
 if exists(select 1 from cron.job where jobname='research_hub_psg_execution_microstructure_v1') then perform cron.unschedule((select jobid from cron.job where jobname='research_hub_psg_execution_microstructure_v1' limit 1)); end if;
 perform cron.schedule('research_hub_psg_execution_microstructure_v1','*/15 * * * *','select research_hub.invoke_psg_execution_microstructure_v1();');
 if exists(select 1 from cron.job where jobname='research_hub_psg_execution_microstructure_refresh_v1') then perform cron.unschedule((select jobid from cron.job where jobname='research_hub_psg_execution_microstructure_refresh_v1' limit 1)); end if;
 perform cron.schedule('research_hub_psg_execution_microstructure_refresh_v1','5,20,35,50 * * * *','select research_hub.refresh_psg_execution_microstructure_v1();');
end $do$;
