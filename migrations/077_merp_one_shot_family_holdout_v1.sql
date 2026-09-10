-- One-shot untouched holdout evaluation for MERP-CR-20260811-001.
-- Eligibility was frozen before holdout access: at most one representative per
-- instrument/direction/horizon family, selected only from pre-holdout evidence.
-- No parameter fitting, threshold selection, placebo search, or family reselection is allowed here.

create table if not exists research_hub.merp_family_holdout_v1(
    candidate_id text primary key,
    family_key text not null,
    status text not null,
    n bigint not null,
    mean20 double precision,
    mean100 double precision,
    pf20 double precision,
    pf100 double precision,
    utc_days integer,
    daily_lcb95_20 double precision,
    block3d_boot_lcb05_20 double precision,
    folds_positive20 integer,
    folds_positive100 integer,
    mean20_without_top5pct_events double precision,
    mean20_without_top3_pnl_days double precision,
    p10_15m_quote_volume_usdt double precision,
    p10_15m_trade_count double precision,
    holdout_pass boolean not null default false,
    metrics jsonb not null default '{}'::jsonb,
    definition_hash text not null,
    evaluated_at timestamptz not null default now()
);
revoke all on table research_hub.merp_family_holdout_v1 from public,anon,authenticated;

create or replace function research_hub.validate_merp_family_holdout_readiness_v1()
returns jsonb
language plpgsql
security invoker
set search_path=pg_catalog,public,research_hub,pg_temp
as $$
declare
    v_run constant uuid := '56558e2a-60a1-4ca2-b3eb-fa59250cc390'::uuid;
    v_reps integer; v_bad integer; v_opened timestamptz; v_sealed boolean; v_existing integer;
begin
    select holdout_opened_at,holdout_sealed into v_opened,v_sealed
    from research_hub.experiment_runs where run_id=v_run;
    if v_sealed is distinct from true or v_opened is not null then
        return jsonb_build_object('ready',false,'reason','experiment_holdout_not_sealed_or_already_opened','holdout_opened_at',v_opened,'holdout_sealed',v_sealed);
    end if;
    if exists(select 1 from research_hub.ai_experiment_registry where run_key='MERP-CR-20260811-001' and (holdout_sealed is distinct from true or holdout_opened_at is not null)) then
        return jsonb_build_object('ready',false,'reason','registry_holdout_not_sealed_or_already_opened');
    end if;
    select count(*) into v_reps from research_hub.merp_candidate_family_v1 where representative=true and family_status='family_representative_preholdout';
    select count(*) into v_bad
    from research_hub.merp_candidate_family_v1 f
    left join research_hub.merp_standard_robustness_v1 r on r.candidate_id=f.candidate_id
    where f.representative=true and (r.overall_preholdout_pass is distinct from true or r.holdout_accessed=true);
    select count(*) into v_existing from research_hub.merp_family_holdout_v1;
    return jsonb_build_object(
        'ready',v_reps=4 and v_bad=0 and v_existing=0,
        'representatives',v_reps,'invalid_representatives',v_bad,'existing_holdout_results',v_existing,
        'expected_representatives',4,'holdout_opened_at',v_opened,'holdout_sealed',v_sealed,
        'terminal_gate','n>=30; mean20>0; pf20>1; mean100>0; pf100>1; daily_lcb95_20>0; block3d_lcb05_20>0; 4/4 positive 20bp folds; >=3/4 positive 100bp folds; top5pct/top3day removal positive; p10 quote volume>=25k; p10 trade_count>=50'
    );
end;
$$;
revoke all on function research_hub.validate_merp_family_holdout_readiness_v1() from public,anon,authenticated;

create or replace function research_hub.run_merp_family_holdout_once_v1()
returns jsonb
language plpgsql
security invoker
set search_path=pg_catalog,public,research_hub,pg_temp
as $$
declare
    v_run constant uuid := '56558e2a-60a1-4ca2-b3eb-fa59250cc390'::uuid;
    v_source_run constant uuid := '7d9ba848-87ef-42a7-a25a-6971d44aee9d'::uuid;
    v_hs constant timestamptz := '2026-03-01 00:00:00+00'::timestamptz;
    v_he constant timestamptz := '2026-07-29 00:00:00+00'::timestamptz;
    v_ready jsonb; c record; fd jsonb;
    v_feature text; v_tail text; v_source text; v_target text; v_threshold double precision; v_horizon integer; v_dir integer;
    v_n bigint; v_mean20 double precision; v_mean100 double precision; v_pf20 double precision; v_pf100 double precision;
    v_days integer; v_daily_mean20 double precision; v_daily_sd20 double precision; v_daily_lcb20 double precision; v_boot_lcb20 double precision;
    v_fold20 integer; v_fold100 integer; v_trim20 double precision; v_drop3day20 double precision; v_p95 double precision;
    v_p10_qv double precision; v_p10_trades double precision; v_pass boolean; v_status text; v_passes integer:=0; v_total integer:=0;
    v_hash text := md5('MERP-CR-20260811-001|family-holdout-v1|n30|cost20-100|daily-block3d|fold4-3|concentration|qv25k|trades50');
begin
    -- Serialize and fail closed. No holdout read occurs before these assertions pass.
    perform 1 from research_hub.experiment_runs where run_id=v_run for update;
    v_ready:=research_hub.validate_merp_family_holdout_readiness_v1();
    if coalesce((v_ready->>'ready')::boolean,false) is distinct from true then
        raise exception 'MERP holdout not ready: %',v_ready;
    end if;

    create temp table if not exists merp_hold_ev_tmp(
        decision_ts timestamptz primary key,
        net20 double precision,net100 double precision,quote_volume double precision,trade_count double precision
    ) on commit drop;
    create temp table if not exists merp_hold_daily_tmp(
        d date primary key,mean20 double precision,pnl20 double precision,rn integer
    ) on commit drop;

    for c in
        select f.candidate_id,f.family_key,l.frozen_definition
        from research_hub.merp_candidate_family_v1 f
        join research_hub.candidate_ledger l on l.candidate_id=f.candidate_id and l.run_id=v_run
        join research_hub.merp_standard_robustness_v1 r on r.candidate_id=f.candidate_id and r.overall_preholdout_pass=true
        where f.representative=true and f.family_status='family_representative_preholdout'
        order by f.family_key
    loop
        v_total:=v_total+1;
        fd:=c.frozen_definition;
        v_feature:=fd->>'feature_key'; v_tail:=upper(fd->>'tail');
        v_source:=fd->>'source_instrument'; v_target:=fd->>'target_instrument';
        v_threshold:=(fd->>'threshold')::double precision; v_horizon:=(fd->>'horizon_seconds')::integer; v_dir:=(fd->>'trade_direction')::integer;
        if v_source<>v_target then raise exception 'Holdout v1 only permits frozen same-asset representatives'; end if;
        if v_horizon not in (3600,14400) then raise exception 'Unexpected frozen horizon %',v_horizon; end if;

        truncate merp_hold_ev_tmp;
        insert into merp_hold_ev_tmp(decision_ts,net20,net100,quote_volume,trade_count)
        select f.signal_ts,
               ((x.open/e.open)-1)*v_dir-0.002,
               ((x.open/e.open)-1)*v_dir-0.010,
               f.quote_volume::double precision,
               f.trade_count::double precision
        from public.crypto_b001_replication_features f
        join public.crypto_b001_replication_features e
          on e.run_id=v_source_run and e.symbol=f.symbol and e.bucket_start=f.signal_ts
        join public.crypto_b001_replication_features x
          on x.run_id=v_source_run and x.symbol=f.symbol and x.bucket_start=f.signal_ts+make_interval(secs=>v_horizon)
        cross join lateral (
            select case v_feature
                when 'cr.ret15' then f.ret15
                when 'cr.ret30' then f.ret30
                when 'cr.ret60' then f.ret60
                when 'cr.ret240' then f.ret240
                when 'cr.ret_accel15' then f.ret_accel15
                when 'cr.range15' then f.range15
                when 'cr.log_quote_volume' then ln(1+greatest(coalesce(f.quote_volume,0),0))
                when 'cr.log_trade_count' then ln(1+greatest(coalesce(f.trade_count,0),0))
                else null end as feature_value
        ) z
        where f.run_id=v_source_run and f.symbol=v_source
          and f.signal_ts>=v_hs and f.signal_ts<v_he
          and e.open>0 and x.open>0 and z.feature_value is not null
          and case when v_tail='HIGH' then z.feature_value>=v_threshold else z.feature_value<=v_threshold end;

        select count(*),avg(net20),avg(net100),
               sum(greatest(net20,0))/nullif(abs(sum(least(net20,0))),0),
               sum(greatest(net100,0))/nullif(abs(sum(least(net100,0))),0),
               percentile_cont(0.1) within group(order by quote_volume),
               percentile_cont(0.1) within group(order by trade_count)
        into v_n,v_mean20,v_mean100,v_pf20,v_pf100,v_p10_qv,v_p10_trades
        from merp_hold_ev_tmp;

        truncate merp_hold_daily_tmp;
        insert into merp_hold_daily_tmp(d,mean20,pnl20,rn)
        select d,mean20,pnl20,row_number() over(order by d)::integer
        from (
            select decision_ts::date d,avg(net20) mean20,sum(net20) pnl20
            from merp_hold_ev_tmp group by 1
        ) q order by d;
        select count(*),avg(mean20),stddev_samp(mean20) into v_days,v_daily_mean20,v_daily_sd20 from merp_hold_daily_tmp;
        v_daily_lcb20:=case when v_days>0 then v_daily_mean20-1.96*coalesce(v_daily_sd20,0)/sqrt(v_days) end;

        with reps as (
            select r,b,1+(('x'||substr(md5(c.candidate_id||'|holdout|'||r||'|'||b),1,8))::bit(32)::bigint % greatest(v_days-2,1)) start_rn
            from generate_series(1,500) r cross join generate_series(1,greatest(1,ceil(v_days/3.0)::int)) b
        ), sampled as (
            select r,d.mean20 from reps cross join lateral generate_series(0,2) j
            join merp_hold_daily_tmp d on d.rn=least(reps.start_rn+j,v_days)
        ), bm as (select r,avg(mean20) b20 from sampled group by r)
        select percentile_cont(0.05) within group(order by b20) into v_boot_lcb20 from bm;

        with f as (
            select least(4,1+floor(4.0*extract(epoch from (decision_ts-v_hs))/nullif(extract(epoch from (v_he-v_hs)),0))::int) fold,net20,net100
            from merp_hold_ev_tmp
        ), a as (select fold,avg(net20) m20,avg(net100) m100 from f group by fold)
        select count(*) filter(where m20>0),count(*) filter(where m100>0) into v_fold20,v_fold100 from a;

        select percentile_cont(0.95) within group(order by net20) into v_p95 from merp_hold_ev_tmp;
        select avg(net20) into v_trim20 from merp_hold_ev_tmp where net20<=v_p95;
        with td as (select decision_ts::date d,sum(net20) pnl from merp_hold_ev_tmp group by 1 order by pnl desc limit 3)
        select avg(e.net20) into v_drop3day20 from merp_hold_ev_tmp e where not exists(select 1 from td where td.d=e.decision_ts::date);

        v_pass:=coalesce(v_n,0)>=30
            and coalesce(v_mean20,0)>0 and coalesce(v_pf20,0)>1
            and coalesce(v_mean100,0)>0 and coalesce(v_pf100,0)>1
            and coalesce(v_daily_lcb20,-1)>0 and coalesce(v_boot_lcb20,-1)>0
            and coalesce(v_fold20,0)=4 and coalesce(v_fold100,0)>=3
            and coalesce(v_trim20,-1)>0 and coalesce(v_drop3day20,-1)>0
            and coalesce(v_p10_qv,0)>=25000 and coalesce(v_p10_trades,0)>=50;
        v_status:=case when v_pass then 'HOLDOUT_PASSED_EXECUTION_VALIDATION_REQUIRED' else 'REJECTED_UNTOUCHED_HOLDOUT' end;
        if v_pass then v_passes:=v_passes+1; end if;

        insert into research_hub.merp_family_holdout_v1(
            candidate_id,family_key,status,n,mean20,mean100,pf20,pf100,utc_days,daily_lcb95_20,block3d_boot_lcb05_20,
            folds_positive20,folds_positive100,mean20_without_top5pct_events,mean20_without_top3_pnl_days,
            p10_15m_quote_volume_usdt,p10_15m_trade_count,holdout_pass,metrics,definition_hash,evaluated_at
        ) values(
            c.candidate_id,c.family_key,v_status,coalesce(v_n,0),v_mean20,v_mean100,v_pf20,v_pf100,v_days,v_daily_lcb20,v_boot_lcb20,
            v_fold20,v_fold100,v_trim20,v_drop3day20,v_p10_qv,v_p10_trades,v_pass,
            jsonb_build_object('holdout_start',v_hs,'holdout_end',v_he,'costs_bps',jsonb_build_array(20,100),
                'selection_frozen_preholdout',true,'parameter_retuning',false,'family_reselection',false,
                'real_spread_book_replication_still_required',true),v_hash,now()
        );
    end loop;

    if v_total<>4 then raise exception 'Expected four family representatives, evaluated %',v_total; end if;

    update research_hub.candidate_ledger l
       set status=h.status,
           confidence=case when h.holdout_pass then 'Passed untouched family-level holdout; real spread/book execution validation still required' else 'Rejected on untouched family-level holdout' end,
           next_test=case when h.holdout_pass then 'Run instrument-specific real spread/order-book/shortability execution validation without retuning the signal.' else 'No retuning or repeat use of the opened holdout.' end,
           metrics=coalesce(l.metrics,'{}'::jsonb)||jsonb_build_object('untouched_holdout',h.metrics||jsonb_build_object('n',h.n,'mean20',h.mean20,'mean100',h.mean100,'pf20',h.pf20,'pf100',h.pf100,'pass',h.holdout_pass)),
           last_tested_at=now(),updated_at=now()
      from research_hub.merp_family_holdout_v1 h where h.candidate_id=l.candidate_id and l.run_id=v_run;

    update research_hub.merp_candidate_family_v1 f
       set family_status=case when h.holdout_pass then 'family_representative_holdout_passed' else 'family_representative_holdout_rejected' end
      from research_hub.merp_family_holdout_v1 h where h.candidate_id=f.candidate_id and f.representative=true;

    update research_hub.experiment_runs
       set holdout_sealed=false,holdout_opened_at=now(),
           latest_result=coalesce(latest_result,'{}'::jsonb)||jsonb_build_object('holdout_accessed',true,'family_representatives_tested',v_total,'family_representatives_passed',v_passes,'holdout_policy','one-shot-family-holdout-v1'),
           config=coalesce(config,'{}'::jsonb)||jsonb_build_object('holdout_accessed',true,'holdout_policy','one-shot-family-holdout-v1'),updated_at=now()
     where run_id=v_run;
    update research_hub.ai_experiment_registry
       set holdout_sealed=false,holdout_opened_at=now(),
           latest_result=coalesce(latest_result,'{}'::jsonb)||jsonb_build_object('holdout_accessed',true,'family_representatives_tested',v_total,'family_representatives_passed',v_passes,'holdout_policy','one-shot-family-holdout-v1'),
           config=coalesce(config,'{}'::jsonb)||jsonb_build_object('holdout_accessed',true,'holdout_policy','one-shot-family-holdout-v1'),updated_at=now()
     where run_key='MERP-CR-20260811-001';
    update research_hub.program_jobs
       set current_state=case when v_passes>0 then 'untouched_holdout_complete_execution_validation_required' else 'untouched_holdout_complete_no_survivor' end,
           latest_result=coalesce(latest_result,'{}'::jsonb)||jsonb_build_object('holdout_accessed',true,'family_representatives_tested',v_total,'family_representatives_passed',v_passes,'holdout_policy','one-shot-family-holdout-v1'),
           retry_state='terminal one-shot holdout; no retuning or repeat holdout use',
           next_automatic_action=case when v_passes>0 then 'Advance only holdout-passing family representatives to real spread/order-book/shortability execution validation. Do not retune signal definitions or reuse holdout.' else 'Preserve campaign as terminal no-survivor result; continue independent blank-canvas discovery.' end,
           intervention_required=false,exact_intervention=null,updated_at=now()
     where job_key='MERP-CR-20260811-001';

    return jsonb_build_object('status','untouched_holdout_complete','representatives_tested',v_total,'representatives_passed',v_passes,'definition_hash',v_hash,'holdout_start',v_hs,'holdout_end',v_he);
end;
$$;
revoke all on function research_hub.run_merp_family_holdout_once_v1() from public,anon,authenticated;
