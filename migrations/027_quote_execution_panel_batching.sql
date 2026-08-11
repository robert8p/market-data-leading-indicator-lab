drop function if exists research_hub.run_pooled_quote_execution_tail_screen(uuid);

create or replace function research_hub.prepare_quote_execution_tail_screen(p_run_id uuid)
returns jsonb
language plpgsql
security invoker
set search_path=pg_catalog,research_hub,pg_temp
as $$
declare
    r research_hub.experiment_runs%rowtype;
begin
    select * into r
    from research_hub.experiment_runs
    where run_id=p_run_id
    for update;

    if not found then
        raise exception 'Unknown research_hub experiment run %',p_run_id;
    end if;
    if r.feature_set_key is null or r.outcome_set_key is null then
        raise exception 'Run % must define feature and outcome sets',r.run_key;
    end if;
    if r.discovery_start is null or r.discovery_end is null or r.validation_start is null or r.validation_end is null then
        raise exception 'Run % must define discovery and validation windows',r.run_key;
    end if;
    if r.validation_start<r.discovery_end then
        raise exception 'Validation must not overlap discovery for run %',r.run_key;
    end if;
    if exists(
        select 1
        from jsonb_array_elements_text(coalesce(r.config->'tail_quantiles','[0.02,0.05,0.10,0.20]'::jsonb)) q(x)
        where q.x::double precision<=0 or q.x::double precision>=0.5
    ) then
        raise exception 'All tail_quantiles must be between 0 and 0.5';
    end if;

    delete from research_hub.experiment_tests where run_id=p_run_id;
    delete from research_hub.candidate_ledger where run_id=p_run_id;

    update research_hub.experiment_runs
    set status='screening_batches',
        started_at=coalesce(started_at,now()),
        completed_at=null,
        search_space_tests=null,
        config=(config-'last_error')||jsonb_build_object(
            'holdout_accessed',false,
            'engine','research_hub_quote_exec_panel_v1',
            'batch_mode',true
        ),
        updated_at=now()
    where run_id=p_run_id;

    return jsonb_build_object(
        'run_id',p_run_id,
        'status','screening_batches',
        'holdout_accessed',false
    );
end;
$$;

create or replace function research_hub.run_quote_execution_feature_batch(
    p_run_id uuid,
    p_feature_keys text[]
)
returns jsonb
language plpgsql
security invoker
set search_path=pg_catalog,research_hub,pg_temp
as $$
declare
    r research_hub.experiment_runs%rowtype;
    v_extra_cost double precision;
    v_min_events integer;
    v_min_validation integer;
    v_min_hit double precision;
    v_max_wlr double precision;
    v_selection_class text;
    v_inserted bigint:=0;
begin
    if p_feature_keys is null or cardinality(p_feature_keys)=0 then
        raise exception 'p_feature_keys cannot be empty';
    end if;

    select * into r
    from research_hub.experiment_runs
    where run_id=p_run_id;

    if not found then
        raise exception 'Unknown research_hub experiment run %',p_run_id;
    end if;
    if r.status not in ('screening_batches','running') then
        raise exception 'Run % is not prepared for batched screening (status=%)',r.run_key,r.status;
    end if;

    v_extra_cost:=coalesce((r.config->>'extra_round_trip_cost_bps')::double precision,0)/10000.0;
    v_min_events:=coalesce((r.config->>'minimum_discovery_events')::integer,150);
    v_min_validation:=coalesce((r.config->>'minimum_validation_events')::integer,greatest(50,v_min_events/3));
    v_min_hit:=coalesce((r.config->>'minimum_hit_rate')::double precision,0.500001);
    v_max_wlr:=case when r.config ? 'maximum_worst_loss_ratio'
        then (r.config->>'maximum_worst_loss_ratio')::double precision else null end;
    v_selection_class:=nullif(r.config->>'selection_class','');

    delete from research_hub.experiment_tests
    where run_id=p_run_id
      and feature_key=any(p_feature_keys);

    create temporary table tmp_quote_batch_metrics(
        phase text not null,
        position_side text not null,
        feature_key text not null,
        tail text not null,
        tail_q double precision not null,
        threshold double precision not null,
        horizon_seconds integer not null,
        n bigint not null,
        mean_gross double precision,
        mean_net double precision,
        median_net double precision,
        hit_rate_net double precision,
        profit_factor_net double precision,
        worst_net double precision,
        avg_winner_net double precision,
        avg_loser_net double precision,
        worst_loss_ratio double precision,
        sd_net double precision,
        primary key(phase,position_side,feature_key,tail,tail_q,horizon_seconds)
    ) on commit drop;

    insert into tmp_quote_batch_metrics
    with feature_keys as(
        select distinct unnest(p_feature_keys) feature_key
    ), base as(
        select
            fr.instrument_key,
            fr.decision_ts,
            f.feature_key,
            (fr.features->>f.feature_key)::double precision feature_value,
            case
                when fr.decision_ts>=r.discovery_start and fr.decision_ts<r.discovery_end then 'discovery'
                when fr.decision_ts>=r.validation_start and fr.decision_ts<r.validation_end then 'validation'
            end phase
        from research_hub.feature_rows fr
        cross join feature_keys f
        where fr.feature_set_key=r.feature_set_key
          and fr.decision_ts>=r.discovery_start
          and fr.decision_ts<r.validation_end
          and fr.features ? f.feature_key
          and jsonb_typeof(fr.features->f.feature_key)='number'
          and (v_selection_class is null or fr.quality->>'selection_class'=v_selection_class)
    ), quantiles as(
        select distinct x::double precision tail_q
        from jsonb_array_elements_text(coalesce(r.config->'tail_quantiles','[0.02,0.05,0.10,0.20]'::jsonb)) q(x)
    ), thresholds as(
        select
            b.feature_key,
            q.tail_q,
            percentile_cont(q.tail_q) within group(order by b.feature_value) low_cut,
            percentile_cont(1.0-q.tail_q) within group(order by b.feature_value) high_cut
        from base b
        cross join quantiles q
        where b.phase='discovery'
        group by b.feature_key,q.tail_q
    ), events as(
        select
            b.instrument_key,b.decision_ts,b.feature_key,b.phase,x.tail,t.tail_q,x.threshold
        from base b
        join thresholds t using(feature_key)
        cross join lateral(values('LOW'::text,t.low_cut),('HIGH'::text,t.high_cut)) x(tail,threshold)
        where b.phase is not null
          and ((x.tail='LOW' and b.feature_value<=x.threshold)
            or (x.tail='HIGH' and b.feature_value>=x.threshold))
    ), event_outcomes as(
        select
            e.*,o.horizon_seconds,s.position_side,s.side_return
        from events e
        join research_hub.outcome_rows o
          on o.outcome_set_key=r.outcome_set_key
         and o.instrument_key=e.instrument_key
         and o.decision_ts=e.decision_ts
        cross join lateral(values
            ('LONG'::text,o.gross_return),
            ('SHORT'::text,case when o.metadata ? 'short_quote_return'
                then (o.metadata->>'short_quote_return')::double precision end)
        ) s(position_side,side_return)
        where s.side_return is not null
    ), eligible as(
        select
            position_side,feature_key,tail,tail_q,threshold,horizon_seconds,count(*) discovery_n
        from event_outcomes
        where phase='discovery'
        group by position_side,feature_key,tail,tail_q,threshold,horizon_seconds
        having count(*)>=v_min_events
    ), scored as(
        select
            e.phase,e.position_side,e.feature_key,e.tail,e.tail_q,k.threshold,e.horizon_seconds,
            e.side_return quoted_return,
            e.side_return-v_extra_cost net_return
        from event_outcomes e
        join eligible k using(position_side,feature_key,tail,tail_q,horizon_seconds)
    )
    select
        phase,position_side,feature_key,tail,tail_q,threshold,horizon_seconds,
        count(*)::bigint,
        avg(quoted_return),
        avg(net_return),
        percentile_cont(0.5) within group(order by net_return),
        avg((net_return>0)::integer::double precision),
        case when abs(sum(net_return) filter(where net_return<0))>0
            then sum(net_return) filter(where net_return>0)/abs(sum(net_return) filter(where net_return<0)) end,
        min(net_return),
        avg(net_return) filter(where net_return>0),
        avg(net_return) filter(where net_return<0),
        case when min(net_return)>=0 then 0.0
            when (avg(net_return) filter(where net_return>0))>0
            then abs(min(net_return))/(avg(net_return) filter(where net_return>0)) end,
        stddev_samp(net_return)
    from scored
    group by phase,position_side,feature_key,tail,tail_q,threshold,horizon_seconds;

    insert into research_hub.experiment_tests(
        run_id,feature_key,outcome_key,source_instrument,target_instrument,slice_key,horizon_seconds,
        n,mean_gross,mean_net,median_net,hit_rate_net,profit_factor_net,worst_net,
        avg_winner_net,avg_loser_net,worst_loss_ratio,effect_size,validation_positive,
        validation_n,validation_mean_net,validation_median_net,validation_hit_rate_net,
        validation_profit_factor_net,validation_worst_net,validation_avg_winner_net,
        validation_avg_loser_net,validation_worst_loss_ratio,metadata
    )
    select
        p_run_id,
        d.feature_key,
        lower(d.position_side)||'_horizon_'||d.horizon_seconds,
        coalesce(r.config->>'panel_label','EQUITY_SIP_QUOTE_EXEC_PANEL'),
        'SELF',
        d.position_side||'_'||d.tail||'_Q'||to_char(d.tail_q,'FM0.000'),
        d.horizon_seconds,
        d.n,d.mean_gross,d.mean_net,d.median_net,d.hit_rate_net,d.profit_factor_net,
        d.worst_net,d.avg_winner_net,d.avg_loser_net,d.worst_loss_ratio,
        case when d.sd_net is not null and d.sd_net>0 then d.mean_net/d.sd_net end,
        (
            coalesce(v.mean_net,-1e100)>0
            and coalesce(v.n,0)>=v_min_validation
            and coalesce(v.hit_rate_net,0)>=v_min_hit
            and (v_max_wlr is null or coalesce(v.worst_loss_ratio,1e100)<=v_max_wlr)
        ),
        v.n,v.mean_net,v.median_net,v.hit_rate_net,v.profit_factor_net,
        v.worst_net,v.avg_winner_net,v.avg_loser_net,v.worst_loss_ratio,
        jsonb_build_object(
            'engine','research_hub_quote_exec_panel_v1',
            'position_side',d.position_side,
            'selection_class',v_selection_class,
            'threshold',d.threshold,
            'tail_quantile',d.tail_q,
            'extra_round_trip_cost_bps',v_extra_cost*10000.0,
            'spread_crossing_included',true,
            'short_borrow_included',false,
            'discovery',jsonb_build_object(
                'n',d.n,'mean_net',d.mean_net,'median_net',d.median_net,
                'hit_rate_net',d.hit_rate_net,'profit_factor_net',d.profit_factor_net,
                'worst_net',d.worst_net,'avg_winner_net',d.avg_winner_net,
                'avg_loser_net',d.avg_loser_net,'worst_loss_ratio',d.worst_loss_ratio
            ),
            'validation',case when v.n is null then null else jsonb_build_object(
                'n',v.n,'mean_net',v.mean_net,'median_net',v.median_net,
                'hit_rate_net',v.hit_rate_net,'profit_factor_net',v.profit_factor_net,
                'worst_net',v.worst_net,'avg_winner_net',v.avg_winner_net,
                'avg_loser_net',v.avg_loser_net,'worst_loss_ratio',v.worst_loss_ratio
            ) end,
            'promotion_constraints',jsonb_build_object(
                'minimum_hit_rate',v_min_hit,
                'maximum_worst_loss_ratio',v_max_wlr
            ),
            'holdout_accessed',false
        )
    from tmp_quote_batch_metrics d
    left join tmp_quote_batch_metrics v
      on v.phase='validation'
     and v.position_side=d.position_side
     and v.feature_key=d.feature_key
     and v.tail=d.tail
     and v.tail_q=d.tail_q
     and v.horizon_seconds=d.horizon_seconds
    where d.phase='discovery';

    get diagnostics v_inserted=row_count;

    update research_hub.experiment_runs
    set updated_at=now()
    where run_id=p_run_id;

    return jsonb_build_object(
        'run_id',p_run_id,
        'feature_keys',to_jsonb(p_feature_keys),
        'tests_inserted',v_inserted,
        'holdout_accessed',false
    );
end;
$$;

create or replace function research_hub.finalize_quote_execution_tail_screen(p_run_id uuid)
returns jsonb
language plpgsql
security invoker
set search_path=pg_catalog,research_hub,pg_temp
as $$
declare
    r research_hub.experiment_runs%rowtype;
    v_fdr double precision;
    v_min_hit double precision;
    v_max_wlr double precision;
    v_selection_class text;
    v_tests bigint;
    v_candidates bigint;
begin
    select * into r
    from research_hub.experiment_runs
    where run_id=p_run_id
    for update;

    if not found then
        raise exception 'Unknown research_hub experiment run %',p_run_id;
    end if;
    if r.status not in ('screening_batches','running') then
        raise exception 'Run % cannot be finalized from status %',r.run_key,r.status;
    end if;

    v_fdr:=coalesce((r.config->>'fdr_q')::double precision,0.05);
    v_min_hit:=coalesce((r.config->>'minimum_hit_rate')::double precision,0.500001);
    v_max_wlr:=case when r.config ? 'maximum_worst_loss_ratio'
        then (r.config->>'maximum_worst_loss_ratio')::double precision else null end;
    v_selection_class:=nullif(r.config->>'selection_class','');

    with family_tests as(
        select test_id,p_value
        from research_hub.experiment_tests
        where run_id=p_run_id and p_value is not null
        union all
        select t.test_id,t.p_value
        from research_hub.experiment_tests t
        where t.p_value is not null
          and t.run_id in (
              select x::uuid
              from jsonb_array_elements_text(coalesce(r.config->'multiplicity_parent_run_ids','[]'::jsonb)) q(x)
          )
    ), ranked as(
        select
            test_id,p_value,
            row_number() over(order by p_value,test_id) rn,
            count(*) over() m
        from family_tests
    ), raw_q as(
        select test_id,rn,least(1.0,p_value*m::double precision/rn::double precision) raw_q
        from ranked
    ), adjusted as(
        select
            test_id,
            least(1.0,min(raw_q) over(order by rn desc rows between unbounded preceding and current row)) q_value
        from raw_q
    )
    update research_hub.experiment_tests t
    set q_value=a.q_value
    from adjusted a
    where t.run_id=p_run_id
      and t.test_id=a.test_id;

    with ordered as(
        select
            test_id,
            lag(mean_net) over(partition by run_id,feature_key,slice_key order by horizon_seconds) prev_mean,
            lead(mean_net) over(partition by run_id,feature_key,slice_key order by horizon_seconds) next_mean
        from research_hub.experiment_tests
        where run_id=p_run_id
    )
    update research_hub.experiment_tests t
    set adjacent_horizon_positive=(coalesce(o.prev_mean,0)>0 or coalesce(o.next_mean,0)>0)
    from ordered o
    where t.test_id=o.test_id;

    delete from research_hub.candidate_ledger where run_id=p_run_id;

    insert into research_hub.candidate_ledger(
        candidate_id,run_id,status,descriptive_name,frozen_definition,metrics,
        confidence,next_test,frozen_at
    )
    select
        'RH-'||upper(substr(md5(r.run_key||'|'||t.feature_key||'|'||t.slice_key||'|'||t.horizon_seconds),1,12)),
        p_run_id,
        'FROZEN_VALIDATION_PASSED',
        coalesce(r.config->>'panel_label','EQUITY_SIP_QUOTE_EXEC_PANEL')||' '||t.slice_key||' '||t.feature_key||' -> SELF @ '||t.horizon_seconds||'s',
        jsonb_build_object(
            'engine','research_hub_quote_exec_panel_v1',
            'feature_set_key',r.feature_set_key,
            'outcome_set_key',r.outcome_set_key,
            'panel_label',coalesce(r.config->>'panel_label','EQUITY_SIP_QUOTE_EXEC_PANEL'),
            'selection_class',v_selection_class,
            'feature_key',t.feature_key,
            'position_side',t.metadata->>'position_side',
            'tail',case when t.slice_key like '%_HIGH_%' then 'HIGH' else 'LOW' end,
            'tail_quantile',t.metadata->'tail_quantile',
            'threshold',t.metadata->'threshold',
            'target_scope','SELF',
            'horizon_seconds',t.horizon_seconds,
            'extra_round_trip_cost_bps',t.metadata->'extra_round_trip_cost_bps',
            'spread_crossing_included',true,
            'threshold_learning_period',jsonb_build_array(r.discovery_start,r.discovery_end),
            'validation_period',jsonb_build_array(r.validation_start,r.validation_end),
            'holdout_accessed',false
        ),
        jsonb_build_object(
            'discovery',t.metadata->'discovery',
            'validation',t.metadata->'validation',
            'q_value',t.q_value,
            'effect_size',t.effect_size
        ),
        case when t.q_value<=v_fdr/10.0 then 'Strong' else 'Candidate' end,
        'Run dependence-aware robustness on frozen quoted-execution discovery/validation evidence before sealed holdout.',
        now()
    from research_hub.experiment_tests t
    where t.run_id=p_run_id
      and t.q_value is not null
      and t.q_value<=v_fdr
      and t.mean_net>0
      and t.validation_positive is true
      and t.adjacent_horizon_positive is true
      and t.hit_rate_net>=v_min_hit
      and (v_max_wlr is null or coalesce(t.worst_loss_ratio,1e100)<=v_max_wlr)
    on conflict(candidate_id) do update set
        status=excluded.status,
        frozen_definition=excluded.frozen_definition,
        metrics=excluded.metrics,
        confidence=excluded.confidence,
        next_test=excluded.next_test,
        frozen_at=excluded.frozen_at,
        updated_at=now();

    select count(*) into v_tests
    from research_hub.experiment_tests
    where run_id=p_run_id;

    select count(*) into v_candidates
    from research_hub.candidate_ledger
    where run_id=p_run_id;

    update research_hub.experiment_runs
    set status='validation_complete_candidates_frozen',
        search_space_tests=v_tests,
        completed_at=now(),
        updated_at=now(),
        config=config||jsonb_build_object(
            'holdout_accessed',false,
            'engine','research_hub_quote_exec_panel_v1',
            'same_instrument_only',true,
            'spread_crossing_included',true,
            'finalized',true
        )
    where run_id=p_run_id;

    return jsonb_build_object(
        'run_id',p_run_id,
        'tests',v_tests,
        'candidates',v_candidates,
        'holdout_accessed',false
    );
end;
$$;

revoke all on function research_hub.prepare_quote_execution_tail_screen(uuid) from public,anon,authenticated;
revoke all on function research_hub.run_quote_execution_feature_batch(uuid,text[]) from public,anon,authenticated;
revoke all on function research_hub.finalize_quote_execution_tail_screen(uuid) from public,anon,authenticated;

comment on function research_hub.run_quote_execution_feature_batch(uuid,text[]) is
'Idempotently screens a bounded feature batch against both LONG and SHORT quote-crossing returns using discovery-only thresholds and unchanged validation rules; holdout is never read.';
