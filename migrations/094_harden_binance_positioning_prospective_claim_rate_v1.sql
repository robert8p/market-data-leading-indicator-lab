create table if not exists research_hub.binance_positioning_prospective_lease_v1(
    singleton boolean primary key default true check(singleton),
    next_allowed_at timestamptz not null default now(),
    last_claimed_at timestamptz,
    claims bigint not null default 0,
    updated_at timestamptz not null default now()
);
insert into research_hub.binance_positioning_prospective_lease_v1(singleton)
values(true) on conflict(singleton) do nothing;
revoke all on table research_hub.binance_positioning_prospective_lease_v1 from public,anon,authenticated;

create or replace function public.claim_binance_positioning_prospective_batch_v1(p_limit integer default 8)
returns jsonb
language plpgsql
security definer
set search_path=pg_catalog,public,research_hub,pg_temp
as $$
declare
    v_batch_id uuid;
    v_rows jsonb;
    v_count integer;
    v_end timestamptz;
    v_active_batches integer;
    v_lease research_hub.binance_positioning_prospective_lease_v1%rowtype;
begin
    perform pg_advisory_xact_lock(hashtext('public.claim_binance_positioning_prospective_batch_v1'));
    perform research_hub.refresh_binance_positioning_prospective_work_v1();
    if now()<'2026-08-13 00:00:00+00' then
        return jsonb_build_object('status','waiting_definition_boundary','start_at','2026-08-13T00:00:00Z');
    end if;

    select * into v_lease
    from research_hub.binance_positioning_prospective_lease_v1
    where singleton=true
    for update;
    if v_lease.next_allowed_at>now() then
        return jsonb_build_object('status','rate_limited','next_allowed_at',v_lease.next_allowed_at);
    end if;

    update research_hub.binance_positioning_prospective_batches_v1
    set status='stale_reclaimed',completed_at=now(),updated_at=now()
    where status='running' and requested_at<now()-interval '10 minutes';

    select count(*) into v_active_batches
    from research_hub.binance_positioning_prospective_batches_v1
    where status='running' and requested_at>=now()-interval '10 minutes';
    if v_active_batches>=2 then
        return jsonb_build_object('status','busy','active_batches',v_active_batches,'max_active_batches',2);
    end if;

    v_end:=date_bin(interval '5 minutes',now()-interval '1 minute','1970-01-01 00:00:00+00');
    if v_end<='2026-08-13 00:00:00+00' then
        return jsonb_build_object('status','waiting_first_complete_five_minute_period','window_end',v_end);
    end if;

    update research_hub.binance_positioning_prospective_lease_v1
    set next_allowed_at=now()+interval '30 seconds',last_claimed_at=now(),claims=claims+1,updated_at=now()
    where singleton=true;

    insert into research_hub.binance_positioning_prospective_batches_v1(status)
    values('claimed') returning batch_id into v_batch_id;

    with candidate as (
        select w.canonical_symbol
        from research_hub.binance_positioning_prospective_work_v1 w
        where w.status in ('queued','retry_wait') and w.next_due_at<=now()
        order by w.next_due_at,w.priority desc,w.canonical_symbol
        for update of w skip locked
        limit greatest(1,least(coalesce(p_limit,8),12))
    ), upd as (
        update research_hub.binance_positioning_prospective_work_v1 w
        set status='running',locked_at=now(),attempts=attempts+1,last_error=null,updated_at=now()
        from candidate c where w.canonical_symbol=c.canonical_symbol
        returning w.*
    )
    select coalesce(jsonb_agg(jsonb_build_object(
        'canonical_symbol',canonical_symbol,'spot_symbol',spot_symbol,'futures_symbol',futures_symbol,
        'start_ts',greatest('2026-08-13 00:00:00+00'::timestamptz,
             coalesce(last_window_end-interval '45 minutes','2026-08-13 00:00:00+00'::timestamptz)),
        'end_ts',v_end,'attempts',attempts,'batch_id',v_batch_id
    ) order by canonical_symbol),'[]'::jsonb),count(*)
    into v_rows,v_count from upd;

    update research_hub.binance_positioning_prospective_batches_v1
    set symbols_claimed=v_count,
        status=case when v_count=0 then 'no_work' else 'running' end,
        result=jsonb_build_object(
            'claimed_symbols',v_rows,'window_end',v_end,
            'rate_limit_seconds',30,'max_active_batches',2
        ),
        updated_at=now()
    where batch_id=v_batch_id;

    return jsonb_build_object(
        'status',case when v_count=0 then 'no_work' else 'claimed' end,
        'batch_id',v_batch_id,'symbols',v_rows,'symbol_count',v_count,'window_end',v_end,
        'rate_limit_seconds',30,'max_active_batches',2
    );
end;
$$;
revoke all on function public.claim_binance_positioning_prospective_batch_v1(integer) from public,anon,authenticated;
grant execute on function public.claim_binance_positioning_prospective_batch_v1(integer) to service_role;

update research_hub.program_jobs
set metadata=metadata||jsonb_build_object(
        'claim_rate_limit_seconds',30,
        'max_active_batches',2,
        'public_edge_abuse_guard',true
    ),
    updated_at=now()
where job_key='SOURCE-BINANCE-POSITIONING-PROSPECTIVE-V1';
