create table if not exists research_hub.binance_spot15m_positioning_v1(
    canonical_symbol text not null,
    venue_symbol text not null,
    bucket_start timestamptz not null,
    signal_ts timestamptz not null,
    open double precision not null,
    high double precision not null,
    low double precision not null,
    close double precision not null,
    volume double precision,
    quote_volume double precision,
    trade_count bigint,
    taker_buy_base_volume double precision,
    taker_buy_quote_volume double precision,
    source text not null default 'binance_public_rest_15m_positioning_recovery_v1',
    source_close_time_ms bigint,
    inserted_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    primary key(canonical_symbol,bucket_start)
);
create index if not exists binance_spot15m_positioning_ts_idx
    on research_hub.binance_spot15m_positioning_v1(bucket_start,canonical_symbol);
revoke all on table research_hub.binance_spot15m_positioning_v1 from public,anon,authenticated;

create table if not exists research_hub.binance_spot15m_positioning_work_v1(
    canonical_symbol text primary key,
    venue_symbol text not null,
    quality_partition_id uuid not null,
    start_ts timestamptz not null,
    end_ts timestamptz not null,
    status text not null default 'queued',
    priority integer not null default 100,
    attempts integer not null default 0,
    max_attempts integer not null default 8,
    rows_written bigint not null default 0,
    coverage_start timestamptz,
    coverage_end timestamptz,
    last_error text,
    locked_at timestamptz,
    completed_at timestamptz,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);
create index if not exists binance_spot15m_positioning_work_status_idx
    on research_hub.binance_spot15m_positioning_work_v1(status,priority desc,canonical_symbol);
revoke all on table research_hub.binance_spot15m_positioning_work_v1 from public,anon,authenticated;

create table if not exists research_hub.binance_spot15m_positioning_quality_v1(
    canonical_symbol text primary key,
    expected_rows bigint not null,
    actual_rows bigint not null,
    grid_fraction double precision not null,
    gaps_gt_15m bigint not null,
    max_gap_minutes double precision,
    coverage_start timestamptz,
    coverage_end timestamptz,
    quality_pass boolean not null,
    status text not null,
    audited_at timestamptz not null default now()
);
revoke all on table research_hub.binance_spot15m_positioning_quality_v1 from public,anon,authenticated;

create table if not exists research_hub.spot15m_positioning_recovery_lease_v1(
    singleton boolean primary key default true check(singleton),
    next_allowed_at timestamptz not null default now(),
    last_claimed_at timestamptz,
    claims bigint not null default 0,
    updated_at timestamptz not null default now()
);
insert into research_hub.spot15m_positioning_recovery_lease_v1(singleton)
values(true) on conflict(singleton) do nothing;
revoke all on table research_hub.spot15m_positioning_recovery_lease_v1 from public,anon,authenticated;

create or replace function research_hub.refresh_binance_spot15m_positioning_work_v1()
returns jsonb
language plpgsql
security invoker
set search_path=pg_catalog,public,research_hub,pg_temp
as $$
declare v_inserted integer:=0; v_total bigint; v_terminal bigint; v_rows bigint;
begin
    insert into research_hub.binance_spot15m_positioning_work_v1(
        canonical_symbol,venue_symbol,quality_partition_id,start_ts,end_ts,status,priority,metadata
    )
    select
        q.canonical_symbol,
        i.provider_symbol,
        q.partition_id,
        date_trunc('minute',q.coverage_start)-interval '1 hour',
        q.coverage_end+interval '4 hours 15 minutes',
        'queued',100,
        jsonb_build_object(
            'source_derivatives_run_id',q.run_id,
            'derivatives_recovery_version',q.recovery_version,
            'purpose','complete spot outcome window for recovered OI/positioning research',
            'public_api_credentials_required',false,
            'promotion_requires_post_definition_replication',true
        )
    from research_hub.binance_deriv_recovery_quality_v1 q
    join lateral (
        select provider_symbol
        from public.instruments i
        where i.provider='binance' and i.asset_class='crypto_spot'
          and i.preferred=true and i.tradable=true
          and i.canonical_symbol=q.canonical_symbol
        order by i.priority desc,i.id
        limit 1
    ) i on true
    where q.run_id='1d57032e-20fa-4d23-b066-14cc659b13e2'::uuid
      and q.data_quality_pass=true
      and q.recovery_version='36h-subwindow-v3-full-partition'
    on conflict(canonical_symbol) do update set
        venue_symbol=excluded.venue_symbol,
        quality_partition_id=excluded.quality_partition_id,
        start_ts=least(research_hub.binance_spot15m_positioning_work_v1.start_ts,excluded.start_ts),
        end_ts=greatest(research_hub.binance_spot15m_positioning_work_v1.end_ts,excluded.end_ts),
        metadata=research_hub.binance_spot15m_positioning_work_v1.metadata||excluded.metadata,
        updated_at=now()
    where research_hub.binance_spot15m_positioning_work_v1.status not in ('running');
    get diagnostics v_inserted=row_count;

    select count(*),count(*) filter(where status in ('completed','completed_empty','failed')),
           coalesce(sum(rows_written),0)
    into v_total,v_terminal,v_rows
    from research_hub.binance_spot15m_positioning_work_v1;

    insert into research_hub.program_jobs(
        job_key,exact_name,purpose,store_key,source_schema,source_table,source_id,job_kind,
        current_state,started_at,progress_current,progress_total,completion_pct,latest_result,
        retry_state,next_automatic_action,intervention_required,exact_intervention,frozen_rule,holdout_sensitive,metadata
    ) values(
        'SOURCE-BINANCE-SPOT15M-POSITIONING-V1',
        'Recover Binance spot 15m bars for derivatives-positioning research',
        'Complete the public spot-price and future-outcome window corresponding to the retention-limited Binance OI/positioning recovery without requiring credentials.',
        'market_data_primary','research_hub','binance_spot15m_positioning_v1',
        'binance-public-spot15m-positioning-v1','data_recovery',
        case when v_total>0 and v_terminal=v_total then 'completed_quality_audit_required' else 'running_public_spot15m_recovery' end,
        now(),v_terminal,v_total,case when v_total>0 then 100.0*v_terminal/v_total else 0 end,
        jsonb_build_object('symbols_total',v_total,'symbols_terminal',v_terminal,'rows_written',v_rows,'credentials_required',false),
        'automatic bounded public-API recovery',
        case when v_total>0 and v_terminal=v_total then 'Run the frozen grid-continuity audit, then release only quality-passing symbols to the typed positioning panel.' else 'Continue one bounded symbol recovery per lane claim; no API key or Rob action required.' end,
        false,null,true,true,
        jsonb_build_object('public_endpoint','/api/v3/klines','interval','15m','limit_per_request',1000,'post_definition_replication_required',true)
    )
    on conflict(job_key) do update set
        current_state=excluded.current_state,progress_current=excluded.progress_current,
        progress_total=excluded.progress_total,completion_pct=excluded.completion_pct,
        latest_result=excluded.latest_result,retry_state=excluded.retry_state,
        next_automatic_action=excluded.next_automatic_action,intervention_required=false,
        exact_intervention=null,metadata=research_hub.program_jobs.metadata||excluded.metadata,updated_at=now();

    return jsonb_build_object('work_rows_touched',v_inserted,'symbols_total',v_total,'symbols_terminal',v_terminal,'rows_written',v_rows);
end;
$$;
revoke all on function research_hub.refresh_binance_spot15m_positioning_work_v1() from public,anon,authenticated;

create or replace function public.claim_binance_spot15m_positioning_v1()
returns jsonb
language plpgsql
security definer
set search_path=pg_catalog,public,research_hub,pg_temp
as $$
declare w research_hub.binance_spot15m_positioning_work_v1%rowtype; l research_hub.spot15m_positioning_recovery_lease_v1%rowtype;
begin
    perform research_hub.refresh_binance_spot15m_positioning_work_v1();
    select * into l from research_hub.spot15m_positioning_recovery_lease_v1 where singleton=true for update;
    if l.next_allowed_at>now() then return jsonb_build_object('status','rate_limited','next_allowed_at',l.next_allowed_at); end if;
    if exists(select 1 from research_hub.binance_spot15m_positioning_work_v1 where status='running') then
        return jsonb_build_object('status','busy');
    end if;
    with c as (
        select canonical_symbol
        from research_hub.binance_spot15m_positioning_work_v1
        where status in ('queued','retry_wait') and attempts<max_attempts
        order by priority desc,canonical_symbol
        for update skip locked limit 1
    )
    update research_hub.binance_spot15m_positioning_work_v1 w
    set status='running',attempts=attempts+1,locked_at=now(),last_error=null,updated_at=now()
    from c where w.canonical_symbol=c.canonical_symbol returning w.* into w;
    if w.canonical_symbol is null then return jsonb_build_object('status','no_work'); end if;
    update research_hub.spot15m_positioning_recovery_lease_v1
    set next_allowed_at=now()+interval '45 seconds',last_claimed_at=now(),claims=claims+1,updated_at=now()
    where singleton=true;
    return jsonb_build_object('status','claimed','canonical_symbol',w.canonical_symbol,'venue_symbol',w.venue_symbol,'start_ts',w.start_ts,'end_ts',w.end_ts,'attempts',w.attempts,'max_attempts',w.max_attempts);
end;
$$;
revoke all on function public.claim_binance_spot15m_positioning_v1() from public,anon,authenticated;
grant execute on function public.claim_binance_spot15m_positioning_v1() to service_role;

create or replace function public.upsert_binance_spot15m_positioning_v1(p_rows jsonb)
returns jsonb
language plpgsql
security definer
set search_path=pg_catalog,public,research_hub,pg_temp
as $$
declare v_rows bigint;
begin
    insert into research_hub.binance_spot15m_positioning_v1(
        canonical_symbol,venue_symbol,bucket_start,signal_ts,open,high,low,close,
        volume,quote_volume,trade_count,taker_buy_base_volume,taker_buy_quote_volume,
        source_close_time_ms,updated_at
    )
    select r.canonical_symbol,r.venue_symbol,to_timestamp(r.open_time_ms/1000.0),
           to_timestamp(r.open_time_ms/1000.0)+interval '15 minutes',
           r.open,r.high,r.low,r.close,r.volume,r.quote_volume,r.trade_count,
           r.taker_buy_base_volume,r.taker_buy_quote_volume,r.close_time_ms,now()
    from jsonb_to_recordset(p_rows) as r(
        canonical_symbol text,venue_symbol text,open_time_ms bigint,close_time_ms bigint,
        open double precision,high double precision,low double precision,close double precision,
        volume double precision,quote_volume double precision,trade_count bigint,
        taker_buy_base_volume double precision,taker_buy_quote_volume double precision
    )
    on conflict(canonical_symbol,bucket_start) do update set
        venue_symbol=excluded.venue_symbol,signal_ts=excluded.signal_ts,open=excluded.open,
        high=excluded.high,low=excluded.low,close=excluded.close,volume=excluded.volume,
        quote_volume=excluded.quote_volume,trade_count=excluded.trade_count,
        taker_buy_base_volume=excluded.taker_buy_base_volume,
        taker_buy_quote_volume=excluded.taker_buy_quote_volume,
        source_close_time_ms=excluded.source_close_time_ms,updated_at=now();
    get diagnostics v_rows=row_count;
    return jsonb_build_object('status','upserted','rows',v_rows);
end;
$$;
revoke all on function public.upsert_binance_spot15m_positioning_v1(jsonb) from public,anon,authenticated;
grant execute on function public.upsert_binance_spot15m_positioning_v1(jsonb) to service_role;

create or replace function public.checkpoint_binance_spot15m_positioning_v1(
    p_canonical_symbol text,p_complete boolean,p_rows_written bigint,
    p_coverage_start timestamptz,p_coverage_end timestamptz,p_error text default null
)
returns jsonb
language plpgsql
security definer
set search_path=pg_catalog,public,research_hub,pg_temp
as $$
declare w research_hub.binance_spot15m_positioning_work_v1%rowtype; v_status text;
begin
    select * into w from research_hub.binance_spot15m_positioning_work_v1 where canonical_symbol=p_canonical_symbol for update;
    if w.canonical_symbol is null then raise exception 'Unknown positioning spot recovery symbol %',p_canonical_symbol; end if;
    if p_error is not null then
        v_status:=case when w.attempts>=w.max_attempts then 'failed' else 'retry_wait' end;
        update research_hub.binance_spot15m_positioning_work_v1
        set status=v_status,last_error=left(p_error,4000),locked_at=null,updated_at=now()
        where canonical_symbol=p_canonical_symbol;
    else
        v_status:=case when p_complete and coalesce(p_rows_written,0)=0 then 'completed_empty' when p_complete then 'completed' else 'queued' end;
        update research_hub.binance_spot15m_positioning_work_v1
        set status=v_status,rows_written=greatest(coalesce(p_rows_written,0),0),
            coverage_start=p_coverage_start,coverage_end=p_coverage_end,locked_at=null,
            completed_at=case when p_complete then now() else null end,last_error=null,updated_at=now()
        where canonical_symbol=p_canonical_symbol;
    end if;
    perform research_hub.refresh_binance_spot15m_positioning_work_v1();
    return jsonb_build_object('status',v_status,'canonical_symbol',p_canonical_symbol,'rows_written',p_rows_written);
end;
$$;
revoke all on function public.checkpoint_binance_spot15m_positioning_v1(text,boolean,bigint,timestamptz,timestamptz,text) from public,anon,authenticated;
grant execute on function public.checkpoint_binance_spot15m_positioning_v1(text,boolean,bigint,timestamptz,timestamptz,text) to service_role;

create or replace function research_hub.audit_binance_spot15m_positioning_v1(p_limit integer default 50)
returns jsonb
language plpgsql
security invoker
set search_path=pg_catalog,public,research_hub,pg_temp
as $$
declare w record; v_expected bigint; v_actual bigint; v_start timestamptz; v_end timestamptz; v_gaps bigint; v_max_gap double precision; v_pass boolean; v_done integer:=0; v_passed integer:=0;
begin
    for w in
        select x.*
        from research_hub.binance_spot15m_positioning_work_v1 x
        left join research_hub.binance_spot15m_positioning_quality_v1 q using(canonical_symbol)
        where x.status in ('completed','completed_empty')
          and (q.canonical_symbol is null or q.audited_at<x.updated_at)
        order by x.updated_at
        limit greatest(1,least(coalesce(p_limit,50),100))
    loop
        with b as (
            select bucket_start,lag(bucket_start) over(order by bucket_start) prev_ts
            from research_hub.binance_spot15m_positioning_v1
            where canonical_symbol=w.canonical_symbol and bucket_start>=w.start_ts and bucket_start<w.end_ts
        )
        select count(*),min(bucket_start),max(bucket_start),
               count(*) filter(where prev_ts is not null and bucket_start-prev_ts>interval '15 minutes'),
               max(extract(epoch from (bucket_start-prev_ts))/60.0) filter(where prev_ts is not null)
        into v_actual,v_start,v_end,v_gaps,v_max_gap from b;
        v_expected:=greatest(0,ceil(extract(epoch from (w.end_ts-w.start_ts))/900.0)::bigint);
        v_pass:=v_expected>0 and v_actual::double precision/v_expected>=0.99 and coalesce(v_max_gap,0)<=30;
        insert into research_hub.binance_spot15m_positioning_quality_v1(
            canonical_symbol,expected_rows,actual_rows,grid_fraction,gaps_gt_15m,max_gap_minutes,
            coverage_start,coverage_end,quality_pass,status,audited_at
        ) values(
            w.canonical_symbol,v_expected,v_actual,case when v_expected>0 then v_actual::double precision/v_expected else 0 end,
            coalesce(v_gaps,0),v_max_gap,v_start,v_end,v_pass,
            case when v_pass then 'quality_pass' when v_actual=0 then 'no_spot_history' else 'quality_fail_review' end,now()
        ) on conflict(canonical_symbol) do update set
            expected_rows=excluded.expected_rows,actual_rows=excluded.actual_rows,grid_fraction=excluded.grid_fraction,
            gaps_gt_15m=excluded.gaps_gt_15m,max_gap_minutes=excluded.max_gap_minutes,
            coverage_start=excluded.coverage_start,coverage_end=excluded.coverage_end,
            quality_pass=excluded.quality_pass,status=excluded.status,audited_at=now();
        v_done:=v_done+1; if v_pass then v_passed:=v_passed+1; end if;
    end loop;
    return jsonb_build_object('audited',v_done,'quality_pass',v_passed);
end;
$$;
revoke all on function research_hub.audit_binance_spot15m_positioning_v1(integer) from public,anon,authenticated;

create or replace function research_hub.invoke_binance_spot15m_positioning_v1()
returns jsonb
language plpgsql
security invoker
set search_path=pg_catalog,research_hub,net,pg_temp
as $$
declare v_request_id bigint;
begin
    select net.http_post(
        url:='https://oxzabweahkoimtevbbny.supabase.co/functions/v1/binance-spot15m-positioning-recovery-v1',
        body:='{}'::jsonb,headers:=jsonb_build_object('content-type','application/json'),
        timeout_milliseconds:=120000
    ) into v_request_id;
    return jsonb_build_object('status','requested','request_id',v_request_id);
end;
$$;
revoke all on function research_hub.invoke_binance_spot15m_positioning_v1() from public,anon,authenticated;

select research_hub.refresh_binance_spot15m_positioning_work_v1();

do $do$
begin
    if exists(select 1 from cron.job where jobname='research_hub_binance_spot15m_positioning_v1') then
        perform cron.unschedule((select jobid from cron.job where jobname='research_hub_binance_spot15m_positioning_v1' limit 1));
    end if;
    perform cron.schedule('research_hub_binance_spot15m_positioning_v1','* * * * *','select research_hub.invoke_binance_spot15m_positioning_v1();');
    if exists(select 1 from cron.job where jobname='research_hub_binance_spot15m_positioning_quality_v1') then
        perform cron.unschedule((select jobid from cron.job where jobname='research_hub_binance_spot15m_positioning_quality_v1' limit 1));
    end if;
    perform cron.schedule('research_hub_binance_spot15m_positioning_quality_v1','*/5 * * * *','select research_hub.audit_binance_spot15m_positioning_v1(50);');
end $do$;
