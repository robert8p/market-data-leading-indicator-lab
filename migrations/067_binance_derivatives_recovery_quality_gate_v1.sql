create table if not exists research_hub.binance_deriv_recovery_quality_v1(
    partition_id uuid primary key,
    run_id uuid not null,
    canonical_symbol text not null,
    recovery_version text,
    status text not null,
    row_count bigint not null default 0,
    coverage_start timestamptz,
    coverage_end timestamptz,
    coverage_days double precision,
    expected_5m_rows bigint,
    oi_rows bigint,
    global_ratio_rows bigint,
    top_account_rows bigint,
    top_position_rows bigint,
    taker_rows bigint,
    oi_grid_fraction double precision,
    min_metric_grid_fraction double precision,
    gaps_gt_5m bigint,
    max_gap_minutes double precision,
    zero_contract boolean not null default false,
    continuity_pass boolean not null default false,
    metric_completeness_pass boolean not null default false,
    data_quality_pass boolean not null default false,
    research_admission_status text not null default 'not_evaluated',
    details jsonb not null default '{}'::jsonb,
    partition_updated_at timestamptz,
    audited_at timestamptz not null default now()
);
revoke all on table research_hub.binance_deriv_recovery_quality_v1 from public,anon,authenticated;

create or replace function research_hub.audit_binance_deriv_recovery_partition_v1(p_partition_id uuid)
returns jsonb
language plpgsql
security invoker
set search_path=pg_catalog,public,research_hub,pg_temp
as $$
declare
    v_cp public.collection_partitions%rowtype;
    v_cov_start timestamptz; v_cov_end timestamptz;
    v_expected bigint:=0; v_oi bigint:=0; v_global bigint:=0; v_top_account bigint:=0;
    v_top_position bigint:=0; v_taker bigint:=0; v_gaps bigint:=0; v_max_gap double precision:=null;
    v_oi_fraction double precision:=0; v_min_fraction double precision:=0; v_days double precision:=0;
    v_continuity boolean:=false; v_metrics boolean:=false; v_quality boolean:=false; v_zero boolean:=false;
    v_admission text;
begin
    select * into v_cp
    from public.collection_partitions
    where id=p_partition_id and provider='binance_futures' and data_type='crypto_derivatives';
    if v_cp.id is null then raise exception 'Unknown Binance derivatives recovery partition %',p_partition_id; end if;
    if v_cp.status not in ('completed','completed_empty') then
        return jsonb_build_object('status','not_terminal','partition_id',p_partition_id,'partition_status',v_cp.status);
    end if;
    if coalesce(v_cp.cursor->>'recovery_window_version','')<>'36h-subwindow-v3-full-partition' then
        return jsonb_build_object('status','wrong_recovery_version','partition_id',p_partition_id,'version',v_cp.cursor->>'recovery_window_version');
    end if;

    v_zero:=coalesce(v_cp.row_count,0)=0;
    if not v_zero then
        with base as materialized (
            select ts,open_interest,global_long_short_ratio,top_account_long_short_ratio,
                   top_position_long_short_ratio,taker_buy_sell_ratio
            from public.crypto_derivatives_metrics
            where provider='binance_futures'
              and canonical_symbol=v_cp.provider_symbol
              and interval='5m'
              and ts>=v_cp.start_ts and ts<v_cp.end_ts
              and metadata->>'source'='binance_rest_retention_recovery_v1'
        ), oi as (
            select ts,lag(ts) over(order by ts) prev_ts from base where open_interest is not null
        ), oa as (
            select count(*)::bigint n,min(ts) min_ts,max(ts) max_ts,
                   count(*) filter(where prev_ts is not null and ts-prev_ts>interval '5 minutes')::bigint gaps,
                   max(extract(epoch from (ts-prev_ts))/60.0) filter(where prev_ts is not null) max_gap
            from oi
        ), ca as (
            select count(*) filter(where open_interest is not null)::bigint oi_n,
                   count(*) filter(where global_long_short_ratio is not null)::bigint global_n,
                   count(*) filter(where top_account_long_short_ratio is not null)::bigint top_account_n,
                   count(*) filter(where top_position_long_short_ratio is not null)::bigint top_position_n,
                   count(*) filter(where taker_buy_sell_ratio is not null)::bigint taker_n
            from base
        )
        select oa.min_ts,oa.max_ts,ca.oi_n,ca.global_n,ca.top_account_n,ca.top_position_n,ca.taker_n,oa.gaps,oa.max_gap
        into v_cov_start,v_cov_end,v_oi,v_global,v_top_account,v_top_position,v_taker,v_gaps,v_max_gap
        from oa cross join ca;

        if v_cov_start is not null and v_cov_end is not null then
            v_expected:=floor(extract(epoch from (v_cov_end-v_cov_start))/300.0)::bigint+1;
            v_days:=extract(epoch from (v_cov_end-v_cov_start))/86400.0;
        end if;
        if v_expected>0 then
            v_oi_fraction:=v_oi::double precision/v_expected;
            v_min_fraction:=least(v_oi,v_global,v_top_account,v_top_position,v_taker)::double precision/v_expected;
        end if;
        v_continuity:=v_expected>=2 and v_oi_fraction>=0.995 and coalesce(v_max_gap,0)<=10.0;
        v_metrics:=v_expected>=2 and v_min_fraction>=0.99;
        v_quality:=v_continuity and v_metrics;
    end if;

    v_admission:=case
        when v_zero then 'contract_unavailable_or_no_retained_history'
        when v_quality then 'quality_pass_pending_panel_gate'
        else 'quality_fail_recovery_review'
    end;

    insert into research_hub.binance_deriv_recovery_quality_v1(
        partition_id,run_id,canonical_symbol,recovery_version,status,row_count,coverage_start,coverage_end,
        coverage_days,expected_5m_rows,oi_rows,global_ratio_rows,top_account_rows,top_position_rows,taker_rows,
        oi_grid_fraction,min_metric_grid_fraction,gaps_gt_5m,max_gap_minutes,zero_contract,continuity_pass,
        metric_completeness_pass,data_quality_pass,research_admission_status,details,partition_updated_at,audited_at
    ) values(
        v_cp.id,v_cp.run_id,v_cp.provider_symbol,v_cp.cursor->>'recovery_window_version',v_cp.status,coalesce(v_cp.row_count,0),
        v_cov_start,v_cov_end,v_days,v_expected,v_oi,v_global,v_top_account,v_top_position,v_taker,
        v_oi_fraction,v_min_fraction,v_gaps,v_max_gap,v_zero,v_continuity,v_metrics,v_quality,v_admission,
        jsonb_build_object(
            'thresholds',jsonb_build_object('oi_grid_fraction_min',0.995,'min_metric_grid_fraction_min',0.99,'max_gap_minutes',10),
            'research_promotion',false,'panel_gate_required',true
        ),v_cp.updated_at,now()
    )
    on conflict(partition_id) do update set
        run_id=excluded.run_id,canonical_symbol=excluded.canonical_symbol,recovery_version=excluded.recovery_version,
        status=excluded.status,row_count=excluded.row_count,coverage_start=excluded.coverage_start,coverage_end=excluded.coverage_end,
        coverage_days=excluded.coverage_days,expected_5m_rows=excluded.expected_5m_rows,oi_rows=excluded.oi_rows,
        global_ratio_rows=excluded.global_ratio_rows,top_account_rows=excluded.top_account_rows,
        top_position_rows=excluded.top_position_rows,taker_rows=excluded.taker_rows,
        oi_grid_fraction=excluded.oi_grid_fraction,min_metric_grid_fraction=excluded.min_metric_grid_fraction,
        gaps_gt_5m=excluded.gaps_gt_5m,max_gap_minutes=excluded.max_gap_minutes,zero_contract=excluded.zero_contract,
        continuity_pass=excluded.continuity_pass,metric_completeness_pass=excluded.metric_completeness_pass,
        data_quality_pass=excluded.data_quality_pass,research_admission_status=excluded.research_admission_status,
        details=excluded.details,partition_updated_at=excluded.partition_updated_at,audited_at=now();

    return jsonb_build_object(
        'status','audited','partition_id',v_cp.id,'symbol',v_cp.provider_symbol,'row_count',v_cp.row_count,
        'coverage_days',v_days,'expected_5m_rows',v_expected,'oi_rows',v_oi,'oi_grid_fraction',v_oi_fraction,
        'min_metric_grid_fraction',v_min_fraction,'gaps_gt_5m',v_gaps,'max_gap_minutes',v_max_gap,
        'data_quality_pass',v_quality,'research_admission_status',v_admission
    );
end;
$$;
revoke all on function research_hub.audit_binance_deriv_recovery_partition_v1(uuid) from public,anon,authenticated;

create or replace function research_hub.audit_ready_binance_deriv_recovery_v1(p_limit integer default 20)
returns jsonb
language plpgsql
security invoker
set search_path=pg_catalog,public,research_hub,pg_temp
as $$
declare
    v_current_run uuid; v_id uuid; v_done integer:=0; v_pass integer:=0; v_zero integer:=0; v_result jsonb;
begin
    select id into v_current_run
    from public.collection_runs
    where name like 'Binance Derivatives Metrics Recovery 30D %' and status<>'cancelled'
    order by created_at desc limit 1;
    if v_current_run is null then return jsonb_build_object('status','no_current_run'); end if;

    for v_id in
        select cp.id
        from public.collection_partitions cp
        left join research_hub.binance_deriv_recovery_quality_v1 q on q.partition_id=cp.id
        where cp.run_id=v_current_run
          and cp.provider='binance_futures' and cp.data_type='crypto_derivatives'
          and cp.status in ('completed','completed_empty')
          and coalesce(cp.cursor->>'recovery_window_version','')='36h-subwindow-v3-full-partition'
          and (q.partition_id is null or q.partition_updated_at is distinct from cp.updated_at)
        order by cp.updated_at
        limit greatest(1,least(coalesce(p_limit,20),50))
    loop
        v_result:=research_hub.audit_binance_deriv_recovery_partition_v1(v_id);
        v_done:=v_done+1;
        if coalesce((v_result->>'data_quality_pass')::boolean,false) then v_pass:=v_pass+1; end if;
        if v_result->>'research_admission_status'='contract_unavailable_or_no_retained_history' then v_zero:=v_zero+1; end if;
    end loop;

    return jsonb_build_object('status','completed','current_run_id',v_current_run,'audited',v_done,
        'quality_pass',v_pass,'zero_contract_or_no_history',v_zero);
end;
$$;
revoke all on function research_hub.audit_ready_binance_deriv_recovery_v1(integer) from public,anon,authenticated;

DO $$
BEGIN
    if exists(select 1 from cron.job where jobname='research_hub_binance_deriv_recovery_quality_v1') then
        perform cron.unschedule((select jobid from cron.job where jobname='research_hub_binance_deriv_recovery_quality_v1' limit 1));
    end if;
    perform cron.schedule(
        'research_hub_binance_deriv_recovery_quality_v1',
        '*/5 * * * *',
        'select research_hub.audit_ready_binance_deriv_recovery_v1(20);'
    );
END $$;
