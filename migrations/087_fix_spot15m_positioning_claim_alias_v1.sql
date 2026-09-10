create or replace function public.claim_binance_spot15m_positioning_v1()
returns jsonb
language plpgsql
security definer
set search_path=pg_catalog,public,research_hub,pg_temp
as $$
declare
    v_work research_hub.binance_spot15m_positioning_work_v1%rowtype;
    v_lease research_hub.spot15m_positioning_recovery_lease_v1%rowtype;
begin
    perform research_hub.refresh_binance_spot15m_positioning_work_v1();
    select * into v_lease
    from research_hub.spot15m_positioning_recovery_lease_v1
    where singleton=true
    for update;
    if v_lease.next_allowed_at>now() then
        return jsonb_build_object('status','rate_limited','next_allowed_at',v_lease.next_allowed_at);
    end if;
    if exists(select 1 from research_hub.binance_spot15m_positioning_work_v1 where status='running') then
        return jsonb_build_object('status','busy');
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

    if v_work.canonical_symbol is null then return jsonb_build_object('status','no_work'); end if;
    update research_hub.spot15m_positioning_recovery_lease_v1
    set next_allowed_at=now()+interval '45 seconds',last_claimed_at=now(),claims=claims+1,updated_at=now()
    where singleton=true;
    return jsonb_build_object(
        'status','claimed','canonical_symbol',v_work.canonical_symbol,'venue_symbol',v_work.venue_symbol,
        'start_ts',v_work.start_ts,'end_ts',v_work.end_ts,
        'attempts',v_work.attempts,'max_attempts',v_work.max_attempts
    );
end;
$$;
revoke all on function public.claim_binance_spot15m_positioning_v1() from public,anon,authenticated;
grant execute on function public.claim_binance_spot15m_positioning_v1() to service_role;
