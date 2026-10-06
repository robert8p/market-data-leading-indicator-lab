-- Distinguish a later first public trading bar from internal data gaps.
-- Binance spot klines are not treated as retention-limited: late-start paths remain
-- explicitly truncated, are never backfilled, and require complete continuous coverage
-- through the frozen end plus at least seven effective days.

alter table research_hub.binance_spot15m_positioning_quality_v1
    add column if not exists requested_expected_rows bigint,
    add column if not exists coverage_start_truncated boolean not null default false,
    add column if not exists coverage_end_complete boolean not null default false,
    add column if not exists effective_coverage_days double precision;

create or replace function research_hub.audit_binance_spot15m_positioning_v1(p_limit integer default 50)
returns jsonb
language plpgsql
security invoker
set search_path=pg_catalog,public,research_hub,pg_temp
as $$
declare
    w record;
    v_requested_expected bigint;
    v_effective_expected bigint;
    v_actual bigint;
    v_start timestamptz;
    v_end timestamptz;
    v_gaps bigint;
    v_max_gap double precision;
    v_effective_days double precision;
    v_start_truncated boolean;
    v_end_complete boolean;
    v_pass boolean;
    v_done integer:=0;
    v_passed integer:=0;
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
            where canonical_symbol=w.canonical_symbol
              and bucket_start>=w.start_ts and bucket_start<w.end_ts
        )
        select count(*),min(bucket_start),max(bucket_start),
               count(*) filter(where prev_ts is not null and bucket_start-prev_ts>interval '15 minutes'),
               max(extract(epoch from (bucket_start-prev_ts))/60.0) filter(where prev_ts is not null)
        into v_actual,v_start,v_end,v_gaps,v_max_gap
        from b;

        v_requested_expected:=greatest(0,ceil(extract(epoch from (w.end_ts-w.start_ts))/900.0)::bigint);
        v_effective_expected:=case
            when v_start is not null and v_end is not null
            then floor(extract(epoch from ((v_end+interval '15 minutes')-v_start))/900.0)::bigint
            else 0 end;
        v_effective_days:=case
            when v_start is not null and v_end is not null
            then extract(epoch from ((v_end+interval '15 minutes')-v_start))/86400.0
            else 0 end;
        v_start_truncated:=v_start is not null and v_start>w.start_ts+interval '15 minutes';
        v_end_complete:=v_end is not null and v_end+interval '15 minutes'>=w.end_ts-interval '15 minutes';

        v_pass:=v_effective_expected>0
            and v_actual::double precision/v_effective_expected>=0.99
            and coalesce(v_max_gap,0)<=30
            and v_end_complete
            and v_effective_days>=7;

        insert into research_hub.binance_spot15m_positioning_quality_v1(
            canonical_symbol,expected_rows,actual_rows,grid_fraction,gaps_gt_15m,max_gap_minutes,
            coverage_start,coverage_end,quality_pass,status,audited_at,
            requested_expected_rows,coverage_start_truncated,coverage_end_complete,effective_coverage_days
        ) values(
            w.canonical_symbol,v_effective_expected,v_actual,
            case when v_effective_expected>0 then v_actual::double precision/v_effective_expected else 0 end,
            coalesce(v_gaps,0),v_max_gap,v_start,
            case when v_end is not null then v_end+interval '15 minutes' end,
            v_pass,
            case
                when v_pass and v_start_truncated then 'quality_pass_later_public_history_start'
                when v_pass then 'quality_pass'
                when v_actual=0 then 'no_spot_history'
                else 'quality_fail_review' end,
            now(),v_requested_expected,v_start_truncated,v_end_complete,v_effective_days
        )
        on conflict(canonical_symbol) do update set
            expected_rows=excluded.expected_rows,actual_rows=excluded.actual_rows,
            grid_fraction=excluded.grid_fraction,gaps_gt_15m=excluded.gaps_gt_15m,
            max_gap_minutes=excluded.max_gap_minutes,coverage_start=excluded.coverage_start,
            coverage_end=excluded.coverage_end,quality_pass=excluded.quality_pass,
            status=excluded.status,requested_expected_rows=excluded.requested_expected_rows,
            coverage_start_truncated=excluded.coverage_start_truncated,
            coverage_end_complete=excluded.coverage_end_complete,
            effective_coverage_days=excluded.effective_coverage_days,audited_at=now();
        v_done:=v_done+1;
        if v_pass then v_passed:=v_passed+1; end if;
    end loop;

    return jsonb_build_object(
        'audited',v_done,'quality_pass',v_passed,
        'listing_or_public_history_start_truncation_is_not_imputed',true
    );
end;
$$;
revoke all on function research_hub.audit_binance_spot15m_positioning_v1(integer) from public,anon,authenticated;

update research_hub.binance_spot15m_positioning_work_v1
set updated_at=now()
where canonical_symbol in (
    select canonical_symbol
    from research_hub.binance_spot15m_positioning_quality_v1
    where not quality_pass and actual_rows>0 and gaps_gt_15m=0
);

select research_hub.audit_binance_spot15m_positioning_v1(100);
