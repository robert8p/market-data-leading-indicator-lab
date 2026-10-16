-- Post-definition public source for genuine future replication of crypto.positioning15m.v1.
-- No API key is used. Bounded eight-symbol batches retrieve public spot bars and
-- futures OI/global/top positioning/taker ratios with a 45-minute recovery overlap.

create table if not exists research_hub.binance_positioning_prospective_work_v1(
    canonical_symbol text primary key,
    spot_symbol text not null,
    futures_symbol text not null,
    status text not null default 'queued',
    priority integer not null default 100,
    next_due_at timestamptz not null default '2026-08-13 00:00:00+00',
    last_window_end timestamptz,
    last_success_at timestamptz,
    locked_at timestamptz,
    attempts bigint not null default 0,
    consecutive_errors integer not null default 0,
    derivative_rows_written bigint not null default 0,
    spot_rows_written bigint not null default 0,
    last_error text,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);
create index if not exists binance_positioning_prospective_due_idx
    on research_hub.binance_positioning_prospective_work_v1(status,next_due_at,priority desc,canonical_symbol);
revoke all on table research_hub.binance_positioning_prospective_work_v1 from public,anon,authenticated;

create table if not exists research_hub.binance_positioning_prospective_batches_v1(
    batch_id uuid primary key default gen_random_uuid(),
    requested_at timestamptz not null default now(),
    completed_at timestamptz,
    symbols_claimed integer not null default 0,
    symbols_succeeded integer not null default 0,
    symbols_failed integer not null default 0,
    derivative_rows_written bigint not null default 0,
    spot_rows_written bigint not null default 0,
    status text not null default 'requested',
    result jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);
create index if not exists binance_positioning_prospective_batches_requested_idx
    on research_hub.binance_positioning_prospective_batches_v1(requested_at desc);
revoke all on table research_hub.binance_positioning_prospective_batches_v1 from public,anon,authenticated;

create or replace function research_hub.refresh_binance_positioning_prospective_work_v1()
returns jsonb
language plpgsql
security invoker
set search_path=pg_catalog,public,research_hub,pg_temp
as $$
declare
    v_touched integer:=0;
    v_total bigint;
    v_active bigint;
    v_recent bigint;
    v_errors bigint;
    v_deriv_rows bigint;
    v_spot_rows bigint;
    v_state text;
begin
    insert into research_hub.binance_positioning_prospective_work_v1(
        canonical_symbol,spot_symbol,futures_symbol,status,priority,next_due_at,metadata
    )
    select q.canonical_symbol,
           (select v.venue_symbol from public.crypto_venue_symbols v
             where v.provider='binance_spot' and v.market_type='spot'
               and v.canonical_symbol=q.canonical_symbol and v.tradable=true
             order by v.priority desc limit 1),
           (select v.venue_symbol from public.crypto_venue_symbols v
             where v.provider='binance_futures' and v.market_type='perpetual'
               and v.canonical_symbol=q.canonical_symbol and v.tradable=true
             order by v.priority desc limit 1),
           'queued',100,'2026-08-13 00:00:00+00',
           jsonb_build_object(
               'definition_frozen_before_collection',true,
               'future_replication_start','2026-08-13T00:00:00Z',
               'public_api_credentials_required',false,
               'observability_contract','binance-usdm-observability-v1',
               'recovery_overlap_minutes',45,
               'source_family','crypto.positioning15m.v1'
           )
    from research_hub.binance_deriv_recovery_quality_v1 q
    where q.run_id='1d57032e-20fa-4d23-b066-14cc659b13e2'::uuid
      and q.data_quality_pass=true
      and exists(select 1 from public.crypto_venue_symbols v
                 where v.provider='binance_spot' and v.market_type='spot'
                   and v.canonical_symbol=q.canonical_symbol and v.tradable=true)
      and exists(select 1 from public.crypto_venue_symbols v
                 where v.provider='binance_futures' and v.market_type='perpetual'
                   and v.canonical_symbol=q.canonical_symbol and v.tradable=true)
    on conflict(canonical_symbol) do update set
        spot_symbol=excluded.spot_symbol,
        futures_symbol=excluded.futures_symbol,
        metadata=research_hub.binance_positioning_prospective_work_v1.metadata||excluded.metadata,
        updated_at=now()
    where research_hub.binance_positioning_prospective_work_v1.status<>'inactive_unavailable';
    get diagnostics v_touched=row_count;

    update research_hub.binance_positioning_prospective_work_v1
    set status='retry_wait',locked_at=null,next_due_at=now(),
        last_error=coalesce(last_error,'')||' | stale running claim reclaimed',updated_at=now()
    where status='running' and locked_at<now()-interval '10 minutes';

    select count(*),count(*) filter(where status<>'inactive_unavailable'),
           count(*) filter(where last_success_at>now()-interval '45 minutes'),
           count(*) filter(where consecutive_errors>0 and status<>'inactive_unavailable'),
           coalesce(sum(derivative_rows_written),0),coalesce(sum(spot_rows_written),0)
    into v_total,v_active,v_recent,v_errors,v_deriv_rows,v_spot_rows
    from research_hub.binance_positioning_prospective_work_v1;

    v_state:=case
        when now()<'2026-08-13 00:00:00+00' then 'waiting_definition_boundary'
        when v_active=0 then 'no_active_symbols'
        when v_recent=0 then 'starting_prospective_collection'
        when v_recent<v_active*0.8 then 'prospective_collection_degraded_or_warming'
        else 'running_prospective_positioning_collection' end;

    insert into research_hub.program_jobs(
        job_key,exact_name,purpose,store_key,source_schema,source_table,source_id,job_kind,
        current_state,started_at,progress_current,progress_total,completion_pct,latest_result,
        retry_state,next_automatic_action,intervention_required,exact_intervention,frozen_rule,holdout_sensitive,metadata
    ) values(
        'SOURCE-BINANCE-POSITIONING-PROSPECTIVE-V1',
        'Prospective Binance OI/positioning/taker and spot source v1',
        'Collect post-definition public Binance spot bars and futures OI/global/top positioning/taker ratios for genuine future replication of the frozen positioning family.',
        'market_data_primary','public','crypto_derivatives_metrics','binance-positioning-prospective-v1','prospective_data_collection',
        v_state,now(),v_recent,v_active,case when v_active>0 then 100.0*v_recent/v_active else 0 end,
        jsonb_build_object(
            'symbols_total',v_total,'symbols_active',v_active,'symbols_recent_45m',v_recent,
            'symbols_with_errors',v_errors,'derivative_rows_written',v_deriv_rows,
            'spot_rows_written',v_spot_rows,'future_replication_start','2026-08-13T00:00:00Z',
            'credentials_required',false
        ),
        'automatic bounded public collection with 45-minute overlap and stale-claim recovery',
        case when now()<'2026-08-13 00:00:00+00'
             then 'Wait for the frozen post-definition boundary. No API key or Rob action required.'
             else 'Continue bounded public spot and positioning collection. Preserve provider timestamps and retrieval provenance; do not retune the frozen manifest.' end,
        false,null,true,true,
        jsonb_build_object(
            'batch_symbols',8,'target_refresh_minutes',30,
            'public_endpoints',jsonb_build_array(
                '/api/v3/klines','/futures/data/openInterestHist',
                '/futures/data/globalLongShortAccountRatio','/futures/data/topLongShortAccountRatio',
                '/futures/data/topLongShortPositionRatio','/futures/data/takerlongshortRatio'
            ),
            'no_api_key_required',true,'definition_precedes_collection',true,
            'promotion_role','future replication only'
        )
    )
    on conflict(job_key) do update set
        current_state=excluded.current_state,
        progress_current=excluded.progress_current,
        progress_total=excluded.progress_total,
        completion_pct=excluded.completion_pct,
        latest_result=excluded.latest_result,
        retry_state=excluded.retry_state,
        next_automatic_action=excluded.next_automatic_action,
        intervention_required=false,
        exact_intervention=null,
        metadata=research_hub.program_jobs.metadata||excluded.metadata,
        updated_at=now();

    return jsonb_build_object(
        'work_rows_touched',v_touched,'state',v_state,'symbols_total',v_total,
        'symbols_active',v_active,'symbols_recent_45m',v_recent,'symbols_with_errors',v_errors,
        'derivative_rows_written',v_deriv_rows,'spot_rows_written',v_spot_rows
    );
end;
$$;
revoke all on function research_hub.refresh_binance_positioning_prospective_work_v1() from public,anon,authenticated;

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
begin
    perform pg_advisory_xact_lock(hashtext('public.claim_binance_positioning_prospective_batch_v1'));
    perform research_hub.refresh_binance_positioning_prospective_work_v1();
    if now()<'2026-08-13 00:00:00+00' then
        return jsonb_build_object('status','waiting_definition_boundary','start_at','2026-08-13T00:00:00Z');
    end if;

    v_end:=date_bin(interval '5 minutes',now()-interval '1 minute','1970-01-01 00:00:00+00');
    if v_end<='2026-08-13 00:00:00+00' then
        return jsonb_build_object('status','waiting_first_complete_five_minute_period','window_end',v_end);
    end if;

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
        from candidate c
        where w.canonical_symbol=c.canonical_symbol
        returning w.*
    )
    select coalesce(jsonb_agg(jsonb_build_object(
        'canonical_symbol',canonical_symbol,
        'spot_symbol',spot_symbol,
        'futures_symbol',futures_symbol,
        'start_ts',greatest('2026-08-13 00:00:00+00'::timestamptz,
             coalesce(last_window_end-interval '45 minutes','2026-08-13 00:00:00+00'::timestamptz)),
        'end_ts',v_end,
        'attempts',attempts,
        'batch_id',v_batch_id
    ) order by canonical_symbol),'[]'::jsonb),count(*)
    into v_rows,v_count
    from upd;

    update research_hub.binance_positioning_prospective_batches_v1
    set symbols_claimed=v_count,
        status=case when v_count=0 then 'no_work' else 'running' end,
        result=jsonb_build_object('claimed_symbols',v_rows,'window_end',v_end),
        updated_at=now()
    where batch_id=v_batch_id;

    return jsonb_build_object(
        'status',case when v_count=0 then 'no_work' else 'claimed' end,
        'batch_id',v_batch_id,'symbols',v_rows,'symbol_count',v_count,'window_end',v_end
    );
end;
$$;
revoke all on function public.claim_binance_positioning_prospective_batch_v1(integer) from public,anon,authenticated;
grant execute on function public.claim_binance_positioning_prospective_batch_v1(integer) to service_role;

create or replace function public.checkpoint_binance_positioning_prospective_v1(
    p_batch_id uuid,
    p_canonical_symbol text,
    p_window_end timestamptz,
    p_derivative_rows bigint,
    p_spot_rows bigint,
    p_error text default null,
    p_terminal boolean default false
)
returns jsonb
language plpgsql
security definer
set search_path=pg_catalog,public,research_hub,pg_temp
as $$
declare
    v_status text;
    v_success boolean:=p_error is null;
begin
    if v_success then
        v_status:='queued';
        update research_hub.binance_positioning_prospective_work_v1
        set status=v_status,
            last_window_end=greatest(coalesce(last_window_end,'2026-08-13 00:00:00+00'),p_window_end),
            last_success_at=now(),
            next_due_at=now()+interval '20 minutes',
            locked_at=null,
            consecutive_errors=0,
            derivative_rows_written=derivative_rows_written+greatest(coalesce(p_derivative_rows,0),0),
            spot_rows_written=spot_rows_written+greatest(coalesce(p_spot_rows,0),0),
            last_error=null,
            updated_at=now()
        where canonical_symbol=p_canonical_symbol;
    else
        v_status:=case when p_terminal then 'inactive_unavailable' else 'retry_wait' end;
        update research_hub.binance_positioning_prospective_work_v1
        set status=v_status,
            next_due_at=case when p_terminal then now()+interval '365 days' else now()+interval '5 minutes' end,
            locked_at=null,
            consecutive_errors=consecutive_errors+1,
            last_error=left(p_error,4000),
            updated_at=now()
        where canonical_symbol=p_canonical_symbol;
    end if;

    update research_hub.binance_positioning_prospective_batches_v1
    set symbols_succeeded=symbols_succeeded+case when v_success then 1 else 0 end,
        symbols_failed=symbols_failed+case when v_success then 0 else 1 end,
        derivative_rows_written=derivative_rows_written+greatest(coalesce(p_derivative_rows,0),0),
        spot_rows_written=spot_rows_written+greatest(coalesce(p_spot_rows,0),0),
        updated_at=now()
    where batch_id=p_batch_id;

    update research_hub.binance_positioning_prospective_batches_v1 b
    set status=case when b.symbols_succeeded+b.symbols_failed>=b.symbols_claimed then 'completed' else b.status end,
        completed_at=case when b.symbols_succeeded+b.symbols_failed>=b.symbols_claimed then now() else b.completed_at end,
        updated_at=now()
    where batch_id=p_batch_id;

    perform research_hub.refresh_binance_positioning_prospective_work_v1();
    return jsonb_build_object(
        'status',v_status,'canonical_symbol',p_canonical_symbol,
        'derivative_rows',p_derivative_rows,'spot_rows',p_spot_rows,'terminal',p_terminal
    );
end;
$$;
revoke all on function public.checkpoint_binance_positioning_prospective_v1(uuid,text,timestamptz,bigint,bigint,text,boolean) from public,anon,authenticated;
grant execute on function public.checkpoint_binance_positioning_prospective_v1(uuid,text,timestamptz,bigint,bigint,text,boolean) to service_role;

create or replace function public.upsert_binance_positioning_prospective_spot_v1(p_rows jsonb)
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
        source,source_close_time_ms,updated_at
    )
    select r.canonical_symbol,r.venue_symbol,to_timestamp(r.open_time_ms/1000.0),
           to_timestamp(r.open_time_ms/1000.0)+interval '15 minutes',
           r.open,r.high,r.low,r.close,r.volume,r.quote_volume,r.trade_count,
           r.taker_buy_base_volume,r.taker_buy_quote_volume,
           'binance_public_prospective_positioning_v1',r.close_time_ms,now()
    from jsonb_to_recordset(p_rows) as r(
        canonical_symbol text,venue_symbol text,open_time_ms bigint,close_time_ms bigint,
        open double precision,high double precision,low double precision,close double precision,
        volume double precision,quote_volume double precision,trade_count bigint,
        taker_buy_base_volume double precision,taker_buy_quote_volume double precision
    )
    on conflict(canonical_symbol,bucket_start) do update set
        venue_symbol=excluded.venue_symbol,
        signal_ts=excluded.signal_ts,
        open=excluded.open,
        high=excluded.high,
        low=excluded.low,
        close=excluded.close,
        volume=excluded.volume,
        quote_volume=excluded.quote_volume,
        trade_count=excluded.trade_count,
        taker_buy_base_volume=excluded.taker_buy_base_volume,
        taker_buy_quote_volume=excluded.taker_buy_quote_volume,
        source=case when research_hub.binance_spot15m_positioning_v1.bucket_start>='2026-08-13 00:00:00+00'
                    then excluded.source else research_hub.binance_spot15m_positioning_v1.source end,
        source_close_time_ms=excluded.source_close_time_ms,
        updated_at=now();
    get diagnostics v_rows=row_count;
    return jsonb_build_object('status','upserted','rows',v_rows,'source','binance_public_prospective_positioning_v1');
end;
$$;
revoke all on function public.upsert_binance_positioning_prospective_spot_v1(jsonb) from public,anon,authenticated;
grant execute on function public.upsert_binance_positioning_prospective_spot_v1(jsonb) to service_role;

create or replace function research_hub.invoke_binance_positioning_prospective_v1()
returns jsonb
language plpgsql
security invoker
set search_path=pg_catalog,research_hub,net,pg_temp
as $$
declare v_request_id bigint;
begin
    select net.http_post(
        url:='https://oxzabweahkoimtevbbny.supabase.co/functions/v1/binance-positioning-prospective-v1',
        body:='{}'::jsonb,
        headers:=jsonb_build_object('content-type','application/json'),
        timeout_milliseconds:=120000
    ) into v_request_id;
    return jsonb_build_object('status','requested','request_id',v_request_id);
end;
$$;
revoke all on function research_hub.invoke_binance_positioning_prospective_v1() from public,anon,authenticated;

select research_hub.refresh_binance_positioning_prospective_work_v1();

do $do$
begin
    if exists(select 1 from cron.job where jobname='research_hub_binance_positioning_prospective_v1') then
        perform cron.unschedule((select jobid from cron.job where jobname='research_hub_binance_positioning_prospective_v1' limit 1));
    end if;
    perform cron.schedule(
        'research_hub_binance_positioning_prospective_v1','* * * * *',
        'select research_hub.invoke_binance_positioning_prospective_v1();'
    );
    if exists(select 1 from cron.job where jobname='research_hub_binance_positioning_prospective_refresh_v1') then
        perform cron.unschedule((select jobid from cron.job where jobname='research_hub_binance_positioning_prospective_refresh_v1' limit 1));
    end if;
    perform cron.schedule(
        'research_hub_binance_positioning_prospective_refresh_v1','*/5 * * * *',
        'select research_hub.refresh_binance_positioning_prospective_work_v1();'
    );
end $do$;
