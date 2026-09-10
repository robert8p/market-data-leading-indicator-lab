create extension if not exists pg_net with schema extensions;

create or replace function research_hub.invoke_binance_derivatives_recovery_edge_v1()
returns jsonb
language plpgsql
security invoker
set search_path=pg_catalog,public,research_hub,net,pg_temp
as $$
declare
    v_progress jsonb;
    v_request_id bigint;
    v_jobid bigint;
begin
    v_progress:=research_hub.refresh_binance_deriv_recovery_program_v1();
    if coalesce((v_progress->>'total')::bigint,0)>0
       and coalesce((v_progress->>'completed')::bigint,0)+coalesce((v_progress->>'failed')::bigint,0)>=coalesce((v_progress->>'total')::bigint,0) then
        select jobid into v_jobid from cron.job where jobname='research_hub_binance_deriv_recovery_edge_v1' limit 1;
        if v_jobid is not null then perform cron.unschedule(v_jobid); end if;
        return jsonb_build_object('status','terminal_unscheduled','progress',v_progress);
    end if;
    if exists(
        select 1 from public.collection_partitions
        where run_id='0f335c0d-1a11-473a-aa48-58111fac20f0'::uuid
          and provider='binance_futures' and data_type='crypto_derivatives'
          and status='running' and locked_by='supabase-edge:binance-deriv-v1'
    ) then
        return jsonb_build_object('status','busy','progress',v_progress);
    end if;
    select net.http_post(
        url:='https://oxzabweahkoimtevbbny.supabase.co/functions/v1/binance-derivatives-recovery-v1',
        body:='{}'::jsonb,
        headers:=jsonb_build_object('content-type','application/json'),
        timeout_milliseconds:=120000
    ) into v_request_id;
    return jsonb_build_object('status','requested','request_id',v_request_id,'progress',v_progress);
end;
$$;

revoke all on function research_hub.invoke_binance_derivatives_recovery_edge_v1() from public,anon,authenticated;

do $$
begin
    if exists(select 1 from cron.job where jobname='research_hub_binance_deriv_recovery_progress_v1') then
        perform cron.unschedule((select jobid from cron.job where jobname='research_hub_binance_deriv_recovery_progress_v1' limit 1));
    end if;
    perform cron.schedule(
        'research_hub_binance_deriv_recovery_progress_v1',
        '*/5 * * * *',
        'select research_hub.refresh_binance_deriv_recovery_program_v1();'
    );

    if exists(select 1 from cron.job where jobname='research_hub_binance_deriv_recovery_edge_v1') then
        perform cron.unschedule((select jobid from cron.job where jobname='research_hub_binance_deriv_recovery_edge_v1' limit 1));
    end if;
    perform cron.schedule(
        'research_hub_binance_deriv_recovery_edge_v1',
        '* * * * *',
        'select research_hub.invoke_binance_derivatives_recovery_edge_v1();'
    );
end $$;