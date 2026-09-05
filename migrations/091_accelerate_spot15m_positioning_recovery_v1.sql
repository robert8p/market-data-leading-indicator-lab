-- Safely raise the public Binance 15m recovery lane to three bounded concurrent symbols.
-- Claim selection remains serialized, each symbol is independently checkpointed, and no credentials are required.

create or replace function public.claim_binance_spot15m_positioning_v1()
returns jsonb
language plpgsql
security definer
set search_path=pg_catalog,public,research_hub,pg_temp
as $$
declare
    v_work research_hub.binance_spot15m_positioning_work_v1%rowtype;
    v_running integer;
begin
    perform pg_advisory_xact_lock(hashtext('public.claim_binance_spot15m_positioning_v1'));
    perform research_hub.refresh_binance_spot15m_positioning_work_v1();
    select count(*) into v_running
    from research_hub.binance_spot15m_positioning_work_v1
    where status='running';
    if v_running>=3 then
        return jsonb_build_object('status','busy','running',v_running,'max_concurrency',3);
    end if;
    with candidate as (
        select wt.canonical_symbol
        from research_hub.binance_spot15m_positioning_work_v1 wt
        where wt.status in ('queued','retry_wait') and wt.attempts<wt.max_attempts
        order by wt.priority desc,wt.canonical_symbol
        for update of wt skip locked
        limit 1
    )
    update research_hub.binance_spot15m_positioning_work_v1 wt
    set status='running',attempts=wt.attempts+1,locked_at=now(),last_error=null,updated_at=now()
    from candidate c
    where wt.canonical_symbol=c.canonical_symbol
    returning wt.* into v_work;

    if v_work.canonical_symbol is null then
        return jsonb_build_object('status','no_work');
    end if;
    return jsonb_build_object(
        'status','claimed','canonical_symbol',v_work.canonical_symbol,'venue_symbol',v_work.venue_symbol,
        'start_ts',v_work.start_ts,'end_ts',v_work.end_ts,
        'attempts',v_work.attempts,'max_attempts',v_work.max_attempts,'max_concurrency',3
    );
end;
$$;
revoke all on function public.claim_binance_spot15m_positioning_v1() from public,anon,authenticated;
grant execute on function public.claim_binance_spot15m_positioning_v1() to service_role;

create or replace function research_hub.invoke_binance_spot15m_positioning_v1()
returns jsonb
language plpgsql
security invoker
set search_path=pg_catalog,research_hub,net,pg_temp
as $$
declare
    v_request_id bigint;
    v_ids jsonb:='[]'::jsonb;
    i integer;
begin
    for i in 1..3 loop
        select net.http_post(
            url:='https://oxzabweahkoimtevbbny.supabase.co/functions/v1/binance-spot15m-positioning-recovery-v1',
            body:='{}'::jsonb,
            headers:=jsonb_build_object('content-type','application/json'),
            timeout_milliseconds:=120000
        ) into v_request_id;
        v_ids:=v_ids||jsonb_build_array(v_request_id);
    end loop;
    return jsonb_build_object('status','requested','request_ids',v_ids,'max_concurrency',3);
end;
$$;
revoke all on function research_hub.invoke_binance_spot15m_positioning_v1() from public,anon,authenticated;

update research_hub.program_jobs
set metadata=metadata||jsonb_build_object(
        'max_concurrency',3,
        'concurrency_rationale','Each symbol uses at most three public 1000-row 15m kline pages. Three bounded concurrent symbols remains a low-rate public workload.',
        'credentials_required',false
    ),
    retry_state='automatic bounded public-API recovery; up to three symbols concurrently',
    updated_at=now()
where job_key='SOURCE-BINANCE-SPOT15M-POSITIONING-V1';
