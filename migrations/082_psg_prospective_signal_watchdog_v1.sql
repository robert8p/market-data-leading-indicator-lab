create table if not exists research_hub.psg_forward_signal_v1(
    decision_ts timestamptz primary key,
    signal_bar_start timestamptz not null,
    signal_bar_end timestamptz not null,
    trade_count bigint not null,
    log_trade_count double precision not null,
    threshold double precision not null default 6.7428806357919,
    triggered boolean not null,
    accepted_nonoverlap boolean not null default false,
    entry_open double precision,
    entry_observed_at timestamptz,
    exit_open_4h double precision,
    gross_short_return double precision,
    net_return_20bps double precision,
    net_return_100bps double precision,
    outcome_finalized_at timestamptz,
    metadata jsonb not null default '{}'::jsonb,
    inserted_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);
create index if not exists psg_forward_signal_trigger_idx on research_hub.psg_forward_signal_v1(accepted_nonoverlap,decision_ts desc);
revoke all on table research_hub.psg_forward_signal_v1 from public,anon,authenticated;

insert into research_hub.execution_monitor_leases(lane_key,next_allowed_at)
values('psg_forward_signal_v1',now()) on conflict(lane_key) do nothing;

create or replace function public.claim_psg_forward_signal_v1()
returns jsonb language plpgsql security definer
set search_path=pg_catalog,research_hub,public,pg_temp
as $$
declare v_lease research_hub.execution_monitor_leases%rowtype;
begin
 select * into v_lease from research_hub.execution_monitor_leases where lane_key='psg_forward_signal_v1' for update;
 if v_lease.next_allowed_at>now() then return jsonb_build_object('status','rate_limited','next_allowed_at',v_lease.next_allowed_at); end if;
 update research_hub.execution_monitor_leases set next_allowed_at=now()+interval '10 minutes',last_claimed_at=now(),claims=claims+1,updated_at=now() where lane_key='psg_forward_signal_v1';
 return jsonb_build_object('status','claimed','claimed_at',now());
end;$$;
revoke all on function public.claim_psg_forward_signal_v1() from public,anon,authenticated;
grant execute on function public.claim_psg_forward_signal_v1() to service_role;

create or replace function public.upsert_psg_forward_bar_v1(p_decision_ts timestamptz,p_signal_bar_start timestamptz,p_signal_bar_end timestamptz,p_trade_count bigint,p_entry_open double precision,p_metadata jsonb)
returns jsonb language plpgsql security definer
set search_path=pg_catalog,research_hub,public,pg_temp
as $$
declare v_log double precision; v_trigger boolean; v_accept boolean; v_existing boolean;
begin
 v_log:=ln(1+greatest(p_trade_count,0)); v_trigger:=v_log>=6.7428806357919;
 select exists(select 1 from research_hub.psg_forward_signal_v1 where decision_ts=p_decision_ts) into v_existing;
 if v_existing then return jsonb_build_object('status','exists','decision_ts',p_decision_ts); end if;
 v_accept:=v_trigger and not exists(select 1 from research_hub.psg_forward_signal_v1 where accepted_nonoverlap=true and decision_ts>p_decision_ts-interval '4 hours' and decision_ts<p_decision_ts);
 insert into research_hub.psg_forward_signal_v1(decision_ts,signal_bar_start,signal_bar_end,trade_count,log_trade_count,triggered,accepted_nonoverlap,entry_open,entry_observed_at,metadata)
 values(p_decision_ts,p_signal_bar_start,p_signal_bar_end,p_trade_count,v_log,v_trigger,v_accept,p_entry_open,now(),coalesce(p_metadata,'{}'::jsonb));
 return jsonb_build_object('status','inserted','decision_ts',p_decision_ts,'triggered',v_trigger,'accepted_nonoverlap',v_accept,'log_trade_count',v_log);
end;$$;
revoke all on function public.upsert_psg_forward_bar_v1(timestamptz,timestamptz,timestamptz,bigint,double precision,jsonb) from public,anon,authenticated;
grant execute on function public.upsert_psg_forward_bar_v1(timestamptz,timestamptz,timestamptz,bigint,double precision,jsonb) to service_role;

create or replace function public.get_psg_forward_pending_v1()
returns jsonb language sql security definer
set search_path=pg_catalog,research_hub,public,pg_temp
as $$
 select coalesce(jsonb_agg(jsonb_build_object('decision_ts',decision_ts,'entry_open',entry_open) order by decision_ts),'[]'::jsonb)
 from (select decision_ts,entry_open from research_hub.psg_forward_signal_v1 where accepted_nonoverlap=true and outcome_finalized_at is null and entry_open>0 and decision_ts<=now()-interval '4 hours' order by decision_ts limit 20) q;
$$;
revoke all on function public.get_psg_forward_pending_v1() from public,anon,authenticated;
grant execute on function public.get_psg_forward_pending_v1() to service_role;

create or replace function public.finalize_psg_forward_v1(p_decision_ts timestamptz,p_exit_open double precision,p_metadata jsonb)
returns jsonb language plpgsql security definer
set search_path=pg_catalog,research_hub,public,pg_temp
as $$
declare v_entry double precision; v_gross double precision;
begin
 select entry_open into v_entry from research_hub.psg_forward_signal_v1 where decision_ts=p_decision_ts and accepted_nonoverlap=true for update;
 if v_entry is null or v_entry<=0 or p_exit_open<=0 then raise exception 'Invalid PSG forward entry/exit for %',p_decision_ts; end if;
 v_gross:=-1.0*((p_exit_open/v_entry)-1.0);
 update research_hub.psg_forward_signal_v1 set exit_open_4h=p_exit_open,gross_short_return=v_gross,net_return_20bps=v_gross-0.002,net_return_100bps=v_gross-0.010,outcome_finalized_at=now(),metadata=coalesce(metadata,'{}'::jsonb)||coalesce(p_metadata,'{}'::jsonb),updated_at=now() where decision_ts=p_decision_ts;
 return jsonb_build_object('status','finalized','decision_ts',p_decision_ts,'gross_short_return',v_gross,'net20',v_gross-0.002,'net100',v_gross-0.010);
end;$$;
revoke all on function public.finalize_psg_forward_v1(timestamptz,double precision,jsonb) from public,anon,authenticated;
grant execute on function public.finalize_psg_forward_v1(timestamptz,double precision,jsonb) to service_role;

create or replace function research_hub.invoke_psg_forward_signal_v1()
returns jsonb language plpgsql security invoker
set search_path=pg_catalog,research_hub,public,net,pg_temp
as $$
declare v_request_id bigint;
begin
 select net.http_post(url:='https://oxzabweahkoimtevbbny.supabase.co/functions/v1/psg-forward-signal-v1',body:='{}'::jsonb,headers:=jsonb_build_object('content-type','application/json'),timeout_milliseconds:=30000) into v_request_id;
 return jsonb_build_object('status','requested','request_id',v_request_id);
end;$$;
revoke all on function research_hub.invoke_psg_forward_signal_v1() from public,anon,authenticated;

create or replace function research_hub.refresh_psg_forward_signal_v1()
returns jsonb language plpgsql security invoker
set search_path=pg_catalog,research_hub,public,pg_temp
as $$
declare v_bars bigint; v_triggered bigint; v_accepted bigint; v_finalized bigint; v_mean20 double precision; v_pf20 double precision; v_mean100 double precision; v_pf100 double precision; v_first timestamptz; v_last timestamptz; v_result jsonb;
begin
 select count(*),count(*) filter(where triggered),count(*) filter(where accepted_nonoverlap),count(*) filter(where accepted_nonoverlap and outcome_finalized_at is not null),avg(net_return_20bps) filter(where accepted_nonoverlap and outcome_finalized_at is not null),sum(greatest(net_return_20bps,0)) filter(where accepted_nonoverlap and outcome_finalized_at is not null)/nullif(abs(sum(least(net_return_20bps,0)) filter(where accepted_nonoverlap and outcome_finalized_at is not null)),0),avg(net_return_100bps) filter(where accepted_nonoverlap and outcome_finalized_at is not null),sum(greatest(net_return_100bps,0)) filter(where accepted_nonoverlap and outcome_finalized_at is not null)/nullif(abs(sum(least(net_return_100bps,0)) filter(where accepted_nonoverlap and outcome_finalized_at is not null)),0),min(decision_ts),max(decision_ts)
 into v_bars,v_triggered,v_accepted,v_finalized,v_mean20,v_pf20,v_mean100,v_pf100,v_first,v_last from research_hub.psg_forward_signal_v1;
 v_result:=jsonb_build_object('observed_15m_bars',v_bars,'raw_triggers',v_triggered,'accepted_nonoverlap_triggers',v_accepted,'finalized_forward_trades',v_finalized,'mean20',v_mean20,'pf20',v_pf20,'mean100',v_mean100,'pf100',v_pf100,'first_decision_ts',v_first,'last_decision_ts',v_last,'signal_version','merp.psg.short4h.nonoverlap.v1','retuning_allowed',false);
 update research_hub.program_jobs set latest_result=coalesce(latest_result,'{}'::jsonb)||jsonb_build_object('prospective_signal_replication',v_result),updated_at=now() where job_key='EXEC-MERP-PSG-V1';
 return v_result;
end;$$;
revoke all on function research_hub.refresh_psg_forward_signal_v1() from public,anon,authenticated;

do $do$
begin
 if exists(select 1 from cron.job where jobname='research_hub_psg_forward_signal_v1') then perform cron.unschedule((select jobid from cron.job where jobname='research_hub_psg_forward_signal_v1' limit 1)); end if;
 perform cron.schedule('research_hub_psg_forward_signal_v1','1,16,31,46 * * * *','select research_hub.invoke_psg_forward_signal_v1();');
 if exists(select 1 from cron.job where jobname='research_hub_psg_forward_signal_refresh_v1') then perform cron.unschedule((select jobid from cron.job where jobname='research_hub_psg_forward_signal_refresh_v1' limit 1)); end if;
 perform cron.schedule('research_hub_psg_forward_signal_refresh_v1','6,21,36,51 * * * *','select research_hub.refresh_psg_forward_signal_v1();');
end $do$;
