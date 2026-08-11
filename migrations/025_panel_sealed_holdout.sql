create or replace function research_hub.evaluate_frozen_panel_holdout(p_run_id uuid)
returns jsonb
language plpgsql
security invoker
set search_path=pg_catalog,research_hub,pg_temp
as $$
declare
    r research_hub.experiment_runs%rowtype;
    v_min_holdout integer;
    v_min_hit double precision;
    v_max_wlr double precision;
    v_evaluated bigint:=0;
    v_passed bigint:=0;
begin
    select * into r
    from research_hub.experiment_runs
    where run_id=p_run_id
    for update;

    if not found then
        raise exception 'Unknown research_hub experiment run %',p_run_id;
    end if;
    if r.holdout_start is null or r.holdout_end is null then
        raise exception 'Run % has no sealed holdout window',r.run_key;
    end if;
    if coalesce((r.config->>'holdout_accessed')::boolean,false) then
        raise exception 'Holdout has already been accessed for run %',r.run_key;
    end if;
    if exists(
        select 1
        from research_hub.candidate_ledger c
        where c.run_id=p_run_id
          and c.status='DEPENDENCE_REVIEW_REQUIRED'
    ) then
        raise exception 'Dependence review must be completed before holdout access for run %',r.run_key;
    end if;

    v_min_holdout:=coalesce((r.config->>'minimum_holdout_events')::integer,50);
    v_min_hit:=coalesce((r.config->>'minimum_hit_rate')::double precision,0.500001);
    v_max_wlr:=case when r.config ? 'maximum_worst_loss_ratio'
        then (r.config->>'maximum_worst_loss_ratio')::double precision else null end;

    create temporary table tmp_panel_holdout(
        candidate_id text primary key,
        n bigint,
        mean_net double precision,
        median_net double precision,
        hit_rate_net double precision,
        profit_factor_net double precision,
        worst_net double precision,
        avg_winner_net double precision,
        avg_loser_net double precision,
        worst_loss_ratio double precision,
        passed boolean
    ) on commit drop;

    insert into tmp_panel_holdout
    with candidates as(
        select
            c.candidate_id,
            c.frozen_definition->>'feature_key' feature_key,
            c.frozen_definition->>'tail' tail,
            (c.frozen_definition->>'threshold')::double precision threshold,
            (c.frozen_definition->>'horizon_seconds')::integer horizon_seconds,
            (c.frozen_definition->>'trade_direction')::integer trade_direction,
            coalesce((c.frozen_definition->>'round_trip_cost_bps')::double precision,0)/10000.0 cost,
            nullif(c.frozen_definition->>'selection_class','') selection_class
        from research_hub.candidate_ledger c
        where c.run_id=p_run_id
          and c.status='HOLDOUT_READY'
          and c.frozen_definition->>'engine'='research_hub_panel_v1'
          and coalesce(c.metrics #>> '{dependence_robustness,passed}','false')='true'
    ), events as(
        select c.*,fr.instrument_key,fr.decision_ts
        from candidates c
        join research_hub.feature_rows fr
          on fr.feature_set_key=r.feature_set_key
         and fr.decision_ts>=r.holdout_start
         and fr.decision_ts<r.holdout_end
         and fr.features ? c.feature_key
         and jsonb_typeof(fr.features->c.feature_key)='number'
         and (c.selection_class is null or fr.quality->>'selection_class'=c.selection_class)
        where (c.tail='LOW' and (fr.features->>c.feature_key)::double precision<=c.threshold)
           or (c.tail='HIGH' and (fr.features->>c.feature_key)::double precision>=c.threshold)
    ), scored as(
        select
            e.candidate_id,
            e.trade_direction*o.gross_return-e.cost net_return
        from events e
        join research_hub.outcome_rows o
          on o.outcome_set_key=r.outcome_set_key
         and o.instrument_key=e.instrument_key
         and o.decision_ts=e.decision_ts
         and o.horizon_seconds=e.horizon_seconds
         and o.gross_return is not null
    ), metrics as(
        select
            candidate_id,
            count(*)::bigint n,
            avg(net_return) mean_net,
            percentile_cont(0.5) within group(order by net_return) median_net,
            avg((net_return>0)::integer::double precision) hit_rate_net,
            case when abs(sum(net_return) filter(where net_return<0))>0
                then sum(net_return) filter(where net_return>0)/abs(sum(net_return) filter(where net_return<0)) end profit_factor_net,
            min(net_return) worst_net,
            avg(net_return) filter(where net_return>0) avg_winner_net,
            avg(net_return) filter(where net_return<0) avg_loser_net
        from scored
        group by candidate_id
    )
    select
        m.candidate_id,
        m.n,
        m.mean_net,
        m.median_net,
        m.hit_rate_net,
        m.profit_factor_net,
        m.worst_net,
        m.avg_winner_net,
        m.avg_loser_net,
        case
            when m.worst_net>=0 then 0.0
            when m.avg_winner_net>0 then abs(m.worst_net)/m.avg_winner_net
        end worst_loss_ratio,
        (
            m.n>=v_min_holdout
            and m.mean_net>0
            and m.hit_rate_net>=v_min_hit
            and (
                v_max_wlr is null
                or coalesce(
                    case
                        when m.worst_net>=0 then 0.0
                        when m.avg_winner_net>0 then abs(m.worst_net)/m.avg_winner_net
                    end,
                    1e100
                )<=v_max_wlr
            )
        ) passed
    from metrics m;

    update research_hub.candidate_ledger c
    set metrics=coalesce(c.metrics,'{}'::jsonb)||jsonb_build_object(
            'holdout',jsonb_build_object(
                'n',h.n,
                'mean_net',h.mean_net,
                'median_net',h.median_net,
                'hit_rate_net',h.hit_rate_net,
                'profit_factor_net',h.profit_factor_net,
                'worst_net',h.worst_net,
                'avg_winner_net',h.avg_winner_net,
                'avg_loser_net',h.avg_loser_net,
                'worst_loss_ratio',h.worst_loss_ratio,
                'passed',h.passed
            )
        ),
        frozen_definition=c.frozen_definition||jsonb_build_object('holdout_accessed',true),
        status=case when h.passed then 'HOLDOUT_PASSED' else 'REJECTED_HOLDOUT' end,
        confidence=case when h.passed then 'Holdout passed' else 'Rejected' end,
        next_test=case
            when h.passed then 'Run execution realism, regime stability and independent replication before promotion.'
            else 'Reject or redesign only in a new experiment; do not alter this frozen candidate.'
        end,
        updated_at=now()
    from tmp_panel_holdout h
    where c.run_id=p_run_id
      and c.candidate_id=h.candidate_id;

    update research_hub.experiment_tests t
    set holdout_positive=h.passed
    from research_hub.candidate_ledger c
    join tmp_panel_holdout h on h.candidate_id=c.candidate_id
    where t.run_id=p_run_id
      and c.run_id=p_run_id
      and t.feature_key=c.frozen_definition->>'feature_key'
      and t.horizon_seconds=(c.frozen_definition->>'horizon_seconds')::integer
      and split_part(t.slice_key,'_',1)=c.frozen_definition->>'tail';

    select count(*) into v_evaluated from tmp_panel_holdout;
    select count(*) into v_passed from tmp_panel_holdout where passed;

    update research_hub.experiment_runs
    set status='holdout_evaluated',
        config=config||jsonb_build_object(
            'holdout_accessed',true,
            'holdout_accessed_at',now()
        ),
        updated_at=now()
    where run_id=p_run_id;

    return jsonb_build_object(
        'run_id',p_run_id,
        'evaluated',v_evaluated,
        'passed',v_passed,
        'holdout_accessed',true
    );
end;
$$;

revoke all on function research_hub.evaluate_frozen_panel_holdout(uuid) from public,anon,authenticated;

comment on function research_hub.evaluate_frozen_panel_holdout(uuid) is
'One-way sealed holdout evaluator for frozen panel candidates that passed dependence review. Re-entry is blocked after first holdout access.';
