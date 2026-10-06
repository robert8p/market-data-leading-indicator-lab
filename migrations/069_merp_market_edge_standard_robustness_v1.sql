-- Frozen pre-holdout market_edge_standard_v1 battery for MERP-CR-20260811-001.
-- Applies identically to the 20 candidate-ledger survivors. No holdout rows are read.
-- Gates are frozen before evaluation:
--   exact 20/50/100bp cost PF; UTC-date clustered LCB; deterministic 3-day block bootstrap;
--   four chronological validation folds; top-5%-event and top-three-day removal;
--   +/-7-day time-shift placebos; discovery-only neighboring tail quantiles;
--   conservative 15m quote-volume/trade-count execution proxy.

create table if not exists research_hub.merp_standard_robustness_v1(
    candidate_id text primary key,
    run_id uuid not null,
    status text not null,
    cost_pass boolean not null default false,
    dependence_pass boolean not null default false,
    fold_pass boolean not null default false,
    concentration_pass boolean not null default false,
    placebo_pass boolean not null default false,
    perturbation_pass boolean not null default false,
    execution_proxy_pass boolean not null default false,
    overall_preholdout_pass boolean not null default false,
    metrics jsonb not null default '{}'::jsonb,
    definition_hash text,
    holdout_accessed boolean not null default false,
    evaluated_at timestamptz not null default now()
);
revoke all on table research_hub.merp_standard_robustness_v1 from public,anon,authenticated;

create or replace function research_hub.run_merp_candidate_robustness_v1(p_candidate_id text)
returns jsonb
language plpgsql
security invoker
set search_path=pg_catalog,public,research_hub,pg_temp
as $$
declare
    c research_hub.candidate_ledger%rowtype;
    fd jsonb;
    v_feature text; v_source text; v_target text; v_tail text;
    v_threshold double precision; v_q double precision; v_horizon integer; v_dir integer;
    v_ds timestamptz; v_de timestamptz; v_vs timestamptz; v_ve timestamptz;
    v_n bigint; v_mean20 double precision; v_mean50 double precision; v_mean100 double precision;
    v_pf20 double precision; v_pf50 double precision; v_pf100 double precision;
    v_days integer; v_daily_mean20 double precision; v_daily_sd20 double precision; v_daily_lcb20 double precision;
    v_daily_mean100 double precision; v_daily_sd100 double precision; v_daily_lcb100 double precision;
    v_boot_lcb20 double precision; v_boot_lcb100 double precision;
    v_fold_pos20 integer; v_fold_pos100 integer; v_fold_min20 double precision; v_fold_min100 double precision;
    v_trim_mean20 double precision; v_drop3day_mean20 double precision; v_p95 double precision;
    v_placebo_p_n bigint; v_placebo_m_n bigint; v_placebo_p_mean double precision; v_placebo_m_mean double precision;
    v_q_lo double precision; v_q_hi double precision; v_thr_lo double precision; v_thr_hi double precision;
    v_pert_lo_n bigint; v_pert_hi_n bigint; v_pert_lo_mean20 double precision; v_pert_hi_mean20 double precision;
    v_pert_lo_mean100 double precision; v_pert_hi_mean100 double precision;
    v_p10_qv double precision; v_p10_trades double precision;
    v_cost boolean; v_dep boolean; v_fold boolean; v_conc boolean; v_placebo boolean; v_pert boolean; v_exec boolean; v_overall boolean;
    v_metrics jsonb; v_status text;
begin
    select * into c
    from research_hub.candidate_ledger
    where candidate_id=p_candidate_id
      and run_id='56558e2a-60a1-4ca2-b3eb-fa59250cc390'::uuid;
    if c.candidate_id is null then raise exception 'Unknown MERP candidate %',p_candidate_id; end if;
    if c.status not in ('STANDARD_ROBUSTNESS_REQUIRED','PREHOLDOUT_STANDARD_PASSED','REJECTED_PRE_HOLDOUT_ROBUSTNESS') then
        raise exception 'Candidate % is not in frozen robustness family: %',p_candidate_id,c.status;
    end if;

    fd:=c.frozen_definition;
    v_feature:=fd->>'feature_key';
    v_source:=fd->>'source_instrument';
    v_target:=fd->>'target_instrument';
    if v_source<>v_target then raise exception 'MERP v1 cache runner supports frozen same-asset survivors only'; end if;
    v_tail:=upper(fd->>'tail');
    v_threshold:=(fd->>'threshold')::double precision;
    v_q:=(fd->>'tail_quantile')::double precision;
    v_horizon:=(fd->>'horizon_seconds')::integer;
    v_dir:=(fd->>'trade_direction')::integer;
    v_ds:=(fd->'threshold_learning_period'->>0)::timestamptz;
    v_de:=(fd->'threshold_learning_period'->>1)::timestamptz;
    v_vs:=(fd->'validation_period'->>0)::timestamptz;
    v_ve:=(fd->'validation_period'->>1)::timestamptz;
    if v_ve>'2026-03-01'::timestamptz then raise exception 'Robustness validation boundary exceeds sealed holdout'; end if;
    if not exists(
        select 1 from research_hub.merp_preholdout_cache_v1
        where instrument_key=v_source and decision_ts>=v_ds and decision_ts<v_ve
    ) then raise exception 'Typed MERP preholdout cache not materialized for %',v_source; end if;

    drop table if exists pg_temp.merp_ev_tmp;
    create temp table merp_ev_tmp(
        decision_ts timestamptz primary key,feature_value double precision,gross_dir double precision,
        net20 double precision,net50 double precision,net100 double precision,
        quote_volume double precision,trade_count double precision
    ) on commit drop;

    insert into merp_ev_tmp
    select
        b.decision_ts,
        research_hub.merp_cache_feature_value_v1(v_feature,b),
        research_hub.merp_cache_gross_value_v1(v_horizon,b)*v_dir,
        research_hub.merp_cache_gross_value_v1(v_horizon,b)*v_dir-0.002,
        research_hub.merp_cache_gross_value_v1(v_horizon,b)*v_dir-0.005,
        research_hub.merp_cache_gross_value_v1(v_horizon,b)*v_dir-0.010,
        exp(coalesce(b.log_quote_volume,0))-1,
        exp(coalesce(b.log_trade_count,0))-1
    from research_hub.merp_preholdout_cache_v1 b
    where b.instrument_key=v_source
      and b.decision_ts>=v_vs and b.decision_ts<v_ve
      and research_hub.merp_cache_gross_value_v1(v_horizon,b) is not null
      and research_hub.merp_cache_feature_value_v1(v_feature,b) is not null
      and case when v_tail='HIGH'
          then research_hub.merp_cache_feature_value_v1(v_feature,b)>=v_threshold
          else research_hub.merp_cache_feature_value_v1(v_feature,b)<=v_threshold end;

    select count(*),avg(net20),avg(net50),avg(net100),
           sum(greatest(net20,0))/nullif(abs(sum(least(net20,0))),0),
           sum(greatest(net50,0))/nullif(abs(sum(least(net50,0))),0),
           sum(greatest(net100,0))/nullif(abs(sum(least(net100,0))),0),
           percentile_cont(0.1) within group(order by quote_volume),
           percentile_cont(0.1) within group(order by trade_count)
    into v_n,v_mean20,v_mean50,v_mean100,v_pf20,v_pf50,v_pf100,v_p10_qv,v_p10_trades
    from merp_ev_tmp;
    if v_n<20 then raise exception 'Too few validation events for robustness: %',v_n; end if;

    drop table if exists pg_temp.merp_daily_tmp;
    create temp table merp_daily_tmp as
    select decision_ts::date d,avg(net20) mean20,avg(net100) mean100,sum(net20) pnl20,count(*) n,
           row_number() over(order by decision_ts::date)::integer rn
    from merp_ev_tmp group by decision_ts::date order by decision_ts::date;

    select count(*),avg(mean20),stddev_samp(mean20),avg(mean100),stddev_samp(mean100)
    into v_days,v_daily_mean20,v_daily_sd20,v_daily_mean100,v_daily_sd100
    from merp_daily_tmp;
    v_daily_lcb20:=v_daily_mean20-1.96*coalesce(v_daily_sd20,0)/sqrt(greatest(v_days,1));
    v_daily_lcb100:=v_daily_mean100-1.96*coalesce(v_daily_sd100,0)/sqrt(greatest(v_days,1));

    with reps as (
        select r,b,
               1+(('x'||substr(md5(p_candidate_id||'|'||r||'|'||b),1,8))::bit(32)::bigint % greatest(v_days-2,1)) start_rn
        from generate_series(1,500) r
        cross join generate_series(1,greatest(1,ceil(v_days/3.0)::int)) b
    ), sampled as (
        select r,d.mean20,d.mean100
        from reps
        cross join lateral generate_series(0,2) j
        join merp_daily_tmp d on d.rn=least(reps.start_rn+j,v_days)
    ), bm as (
        select r,avg(mean20) b20,avg(mean100) b100 from sampled group by r
    )
    select percentile_cont(0.05) within group(order by b20),
           percentile_cont(0.05) within group(order by b100)
    into v_boot_lcb20,v_boot_lcb100 from bm;

    with f as (
        select least(4,1+floor(4.0*extract(epoch from (decision_ts-v_vs))/nullif(extract(epoch from (v_ve-v_vs)),0))::int) fold,
               net20,net100
        from merp_ev_tmp
    ), a as (
        select fold,avg(net20) m20,avg(net100) m100 from f group by fold
    )
    select count(*) filter(where m20>0),count(*) filter(where m100>0),min(m20),min(m100)
    into v_fold_pos20,v_fold_pos100,v_fold_min20,v_fold_min100 from a;

    select percentile_cont(0.95) within group(order by net20) into v_p95 from merp_ev_tmp;
    select avg(net20) into v_trim_mean20 from merp_ev_tmp where net20<=v_p95;
    with td as (
        select decision_ts::date d,sum(net20) pnl from merp_ev_tmp group by 1 order by pnl desc limit 3
    )
    select avg(e.net20) into v_drop3day_mean20
    from merp_ev_tmp e where not exists(select 1 from td where td.d=e.decision_ts::date);

    select count(*),avg(research_hub.merp_cache_gross_value_v1(v_horizon,p)*v_dir-0.002)
    into v_placebo_p_n,v_placebo_p_mean
    from merp_ev_tmp e
    join research_hub.merp_preholdout_cache_v1 p
      on p.instrument_key=v_target and p.decision_ts=e.decision_ts+interval '7 days'
    where e.decision_ts+interval '7 days'<v_ve
      and research_hub.merp_cache_gross_value_v1(v_horizon,p) is not null;

    select count(*),avg(research_hub.merp_cache_gross_value_v1(v_horizon,p)*v_dir-0.002)
    into v_placebo_m_n,v_placebo_m_mean
    from merp_ev_tmp e
    join research_hub.merp_preholdout_cache_v1 p
      on p.instrument_key=v_target and p.decision_ts=e.decision_ts-interval '7 days'
    where e.decision_ts-interval '7 days'>=v_vs
      and research_hub.merp_cache_gross_value_v1(v_horizon,p) is not null;

    v_q_lo:=greatest(0.001,v_q*0.8);
    v_q_hi:=least(0.20,v_q*1.2);
    if v_tail='HIGH' then
        select percentile_cont(1-v_q_lo) within group(order by research_hub.merp_cache_feature_value_v1(v_feature,b)),
               percentile_cont(1-v_q_hi) within group(order by research_hub.merp_cache_feature_value_v1(v_feature,b))
        into v_thr_lo,v_thr_hi
        from research_hub.merp_preholdout_cache_v1 b
        where b.instrument_key=v_source and b.decision_ts>=v_ds and b.decision_ts<v_de
          and research_hub.merp_cache_feature_value_v1(v_feature,b) is not null;
    else
        select percentile_cont(v_q_lo) within group(order by research_hub.merp_cache_feature_value_v1(v_feature,b)),
               percentile_cont(v_q_hi) within group(order by research_hub.merp_cache_feature_value_v1(v_feature,b))
        into v_thr_lo,v_thr_hi
        from research_hub.merp_preholdout_cache_v1 b
        where b.instrument_key=v_source and b.decision_ts>=v_ds and b.decision_ts<v_de
          and research_hub.merp_cache_feature_value_v1(v_feature,b) is not null;
    end if;

    with allv as (
        select research_hub.merp_cache_feature_value_v1(v_feature,b) fv,
               research_hub.merp_cache_gross_value_v1(v_horizon,b)*v_dir gr
        from research_hub.merp_preholdout_cache_v1 b
        where b.instrument_key=v_source and b.decision_ts>=v_vs and b.decision_ts<v_ve
          and research_hub.merp_cache_feature_value_v1(v_feature,b) is not null
          and research_hub.merp_cache_gross_value_v1(v_horizon,b) is not null
    )
    select
        count(*) filter(where case when v_tail='HIGH' then fv>=v_thr_lo else fv<=v_thr_lo end),
        avg(gr-0.002) filter(where case when v_tail='HIGH' then fv>=v_thr_lo else fv<=v_thr_lo end),
        avg(gr-0.010) filter(where case when v_tail='HIGH' then fv>=v_thr_lo else fv<=v_thr_lo end),
        count(*) filter(where case when v_tail='HIGH' then fv>=v_thr_hi else fv<=v_thr_hi end),
        avg(gr-0.002) filter(where case when v_tail='HIGH' then fv>=v_thr_hi else fv<=v_thr_hi end),
        avg(gr-0.010) filter(where case when v_tail='HIGH' then fv>=v_thr_hi else fv<=v_thr_hi end)
    into v_pert_lo_n,v_pert_lo_mean20,v_pert_lo_mean100,v_pert_hi_n,v_pert_hi_mean20,v_pert_hi_mean100
    from allv;

    v_cost:=coalesce(v_mean100,0)>0 and coalesce(v_pf20,0)>=1 and coalesce(v_pf50,0)>=1 and coalesce(v_pf100,0)>=1;
    v_dep:=coalesce(v_daily_lcb20,-1)>0 and coalesce(v_boot_lcb20,-1)>0;
    v_fold:=coalesce(v_fold_pos20,0)=4 and coalesce(v_fold_pos100,0)>=3;
    v_conc:=coalesce(v_trim_mean20,-1)>0 and coalesce(v_drop3day_mean20,-1)>0;
    v_placebo:=coalesce(v_placebo_p_n,0)>=0.8*v_n and coalesce(v_placebo_m_n,0)>=0.8*v_n
               and coalesce(v_placebo_p_mean,0)<0.5*v_mean20 and coalesce(v_placebo_m_mean,0)<0.5*v_mean20;
    v_pert:=coalesce(v_pert_lo_mean20,-1)>0 and coalesce(v_pert_hi_mean20,-1)>0;
    v_exec:=coalesce(v_p10_qv,0)>=25000 and coalesce(v_p10_trades,0)>=50;
    v_overall:=v_cost and v_dep and v_fold and v_conc and v_placebo and v_pert and v_exec;
    v_status:=case when v_overall then 'PREHOLDOUT_STANDARD_PASSED' else 'REJECTED_PRE_HOLDOUT_ROBUSTNESS' end;

    v_metrics:=jsonb_build_object(
        'validation',jsonb_build_object('n',v_n,'mean20',v_mean20,'mean50',v_mean50,'mean100',v_mean100,'pf20',v_pf20,'pf50',v_pf50,'pf100',v_pf100),
        'dependence',jsonb_build_object('utc_days',v_days,'daily_mean20',v_daily_mean20,'daily_lcb95_20',v_daily_lcb20,
            'daily_mean100',v_daily_mean100,'daily_lcb95_100',v_daily_lcb100,
            'block3d_boot_lcb05_20',v_boot_lcb20,'block3d_boot_lcb05_100',v_boot_lcb100,'bootstrap_reps',500),
        'folds',jsonb_build_object('positive20',v_fold_pos20,'positive100',v_fold_pos100,'min_mean20',v_fold_min20,'min_mean100',v_fold_min100),
        'concentration',jsonb_build_object('p95_net20',v_p95,'mean20_without_top5pct_events',v_trim_mean20,'mean20_without_top3_pnl_days',v_drop3day_mean20),
        'placebo',jsonb_build_object('plus7_n',v_placebo_p_n,'plus7_mean20',v_placebo_p_mean,'minus7_n',v_placebo_m_n,'minus7_mean20',v_placebo_m_mean),
        'perturbation',jsonb_build_object('q_low',v_q_lo,'threshold_low',v_thr_lo,'n_low',v_pert_lo_n,'mean20_low',v_pert_lo_mean20,
            'mean100_low',v_pert_lo_mean100,'q_high',v_q_hi,'threshold_high',v_thr_hi,'n_high',v_pert_hi_n,
            'mean20_high',v_pert_hi_mean20,'mean100_high',v_pert_hi_mean100),
        'execution_proxy',jsonb_build_object('p10_15m_quote_volume_usdt',v_p10_qv,'p10_15m_trade_count',v_p10_trades,
            'cost_stress_to_100bps',true,'real_spread_book_replication_still_required',true),
        'gates',jsonb_build_object('cost',v_cost,'dependence',v_dep,'fold',v_fold,'concentration',v_conc,
            'placebo',v_placebo,'perturbation',v_pert,'execution_proxy',v_exec),
        'holdout_accessed',false,
        'definition','market_edge_standard_v1_frozen_20260812'
    );

    insert into research_hub.merp_standard_robustness_v1(
        candidate_id,run_id,status,cost_pass,dependence_pass,fold_pass,concentration_pass,
        placebo_pass,perturbation_pass,execution_proxy_pass,overall_preholdout_pass,
        metrics,definition_hash,holdout_accessed,evaluated_at
    ) values(
        c.candidate_id,c.run_id,v_status,v_cost,v_dep,v_fold,v_conc,v_placebo,v_pert,v_exec,v_overall,
        v_metrics,c.definition_hash,false,now()
    ) on conflict(candidate_id) do update set
        status=excluded.status,cost_pass=excluded.cost_pass,dependence_pass=excluded.dependence_pass,
        fold_pass=excluded.fold_pass,concentration_pass=excluded.concentration_pass,placebo_pass=excluded.placebo_pass,
        perturbation_pass=excluded.perturbation_pass,execution_proxy_pass=excluded.execution_proxy_pass,
        overall_preholdout_pass=excluded.overall_preholdout_pass,metrics=excluded.metrics,
        definition_hash=excluded.definition_hash,holdout_accessed=false,evaluated_at=now();

    update research_hub.candidate_ledger
    set status=v_status,
        confidence=case when v_overall
            then 'Passed frozen pre-holdout market_edge_standard_v1 robustness battery; holdout remains sealed.'
            else 'Rejected by frozen pre-holdout market_edge_standard_v1 robustness battery.' end,
        next_test=case when v_overall
            then 'Eligible for governed holdout-opening decision only after candidate-family deduplication and execution-replication plan are frozen. Do not open holdout automatically.'
            else 'Do not open holdout for this candidate. Preserve rejection and failure-gate evidence.' end,
        metrics=coalesce(metrics,'{}'::jsonb)||jsonb_build_object('market_edge_standard_v1_robustness',v_metrics),
        last_tested_at=now(),updated_at=now()
    where candidate_id=c.candidate_id;

    return jsonb_build_object('candidate_id',c.candidate_id,'status',v_status,
        'overall_preholdout_pass',v_overall,'gates',v_metrics->'gates',
        'validation',v_metrics->'validation','dependence',v_metrics->'dependence');
end;
$$;
revoke all on function research_hub.run_merp_candidate_robustness_v1(text) from public,anon,authenticated;
