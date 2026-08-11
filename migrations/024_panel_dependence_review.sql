create or replace function research_hub.review_panel_candidate_dependence(p_run_id uuid)
returns jsonb
language plpgsql
security invoker
set search_path=pg_catalog,research_hub,pg_temp
as $$
declare
    r research_hub.experiment_runs%rowtype;
    v_min_clusters integer;
    v_p_limit double precision;
    v_reviewed bigint:=0;
    v_passed bigint:=0;
begin
    select * into r
    from research_hub.experiment_runs
    where run_id=p_run_id;

    if not found then
        raise exception 'Unknown research_hub experiment run %',p_run_id;
    end if;

    v_min_clusters:=coalesce((r.config->>'minimum_dependence_clusters')::integer,8);
    v_p_limit:=coalesce((r.config->>'dependence_p_value')::double precision,0.05);

    create temporary table tmp_panel_dependence(
        candidate_id text primary key,
        discovery_clusters bigint,
        discovery_cluster_mean double precision,
        discovery_cluster_sd double precision,
        discovery_cluster_p double precision,
        validation_clusters bigint,
        validation_cluster_mean double precision,
        validation_positive_cluster_rate double precision,
        passed boolean
    ) on commit drop;

    insert into tmp_panel_dependence
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
          and c.frozen_definition->>'engine'='research_hub_panel_v1'
    ), event_rows as(
        select
            c.*,
            fr.instrument_key,
            fr.decision_ts,
            case
                when fr.decision_ts>=r.discovery_start and fr.decision_ts<r.discovery_end then 'discovery'
                when fr.decision_ts>=r.validation_start and fr.decision_ts<r.validation_end then 'validation'
            end phase
        from candidates c
        join research_hub.feature_rows fr
          on fr.feature_set_key=r.feature_set_key
         and fr.decision_ts>=r.discovery_start
         and fr.decision_ts<r.validation_end
         and fr.features ? c.feature_key
         and jsonb_typeof(fr.features->c.feature_key)='number'
         and (c.selection_class is null or fr.quality->>'selection_class'=c.selection_class)
        where (c.tail='LOW' and (fr.features->>c.feature_key)::double precision<=c.threshold)
           or (c.tail='HIGH' and (fr.features->>c.feature_key)::double precision>=c.threshold)
    ), scored as(
        select
            e.candidate_id,
            e.phase,
            e.instrument_key,
            e.trade_direction*o.gross_return-e.cost net_return
        from event_rows e
        join research_hub.outcome_rows o
          on o.outcome_set_key=r.outcome_set_key
         and o.instrument_key=e.instrument_key
         and o.decision_ts=e.decision_ts
         and o.horizon_seconds=e.horizon_seconds
         and o.gross_return is not null
        where e.phase is not null
    ), clusters as(
        select candidate_id,phase,instrument_key,avg(net_return) cluster_mean
        from scored
        group by candidate_id,phase,instrument_key
    ), discovery as(
        select
            candidate_id,
            count(*)::bigint clusters,
            avg(cluster_mean) cluster_mean,
            stddev_samp(cluster_mean) cluster_sd
        from clusters
        where phase='discovery'
        group by candidate_id
    ), validation as(
        select
            candidate_id,
            count(*)::bigint clusters,
            avg(cluster_mean) cluster_mean,
            avg((cluster_mean>0)::integer::double precision) positive_cluster_rate
        from clusters
        where phase='validation'
        group by candidate_id
    )
    select
        c.candidate_id,
        d.clusters,
        d.cluster_mean,
        d.cluster_sd,
        research_hub.positive_edge_pvalue(d.cluster_mean,d.cluster_sd,d.clusters),
        v.clusters,
        v.cluster_mean,
        v.positive_cluster_rate,
        (
            coalesce(d.clusters,0)>=v_min_clusters
            and coalesce(v.clusters,0)>=v_min_clusters
            and coalesce(d.cluster_mean,-1e100)>0
            and coalesce(v.cluster_mean,-1e100)>0
            and coalesce(research_hub.positive_edge_pvalue(d.cluster_mean,d.cluster_sd,d.clusters),1.0)<=v_p_limit
        )
    from candidates c
    left join discovery d using(candidate_id)
    left join validation v using(candidate_id);

    update research_hub.candidate_ledger c
    set metrics=coalesce(c.metrics,'{}'::jsonb)||jsonb_build_object(
            'dependence_robustness',jsonb_build_object(
                'method','instrument_cluster_means',
                'passed',d.passed,
                'minimum_clusters',v_min_clusters,
                'one_sided_p_limit',v_p_limit,
                'discovery_clusters',d.discovery_clusters,
                'discovery_cluster_mean',d.discovery_cluster_mean,
                'discovery_cluster_sd',d.discovery_cluster_sd,
                'discovery_cluster_p',d.discovery_cluster_p,
                'validation_clusters',d.validation_clusters,
                'validation_cluster_mean',d.validation_cluster_mean,
                'validation_positive_cluster_rate',d.validation_positive_cluster_rate
            )
        ),
        status=case when d.passed then 'HOLDOUT_READY' else 'REJECTED_DEPENDENCE' end,
        confidence=case when d.passed then c.confidence else 'Rejected' end,
        next_test=case
            when d.passed then 'Run explicit sealed panel holdout evaluator without changing the frozen definition.'
            else 'Do not access holdout; dependence-aware discovery/validation evidence did not pass.'
        end,
        updated_at=now()
    from tmp_panel_dependence d
    where c.run_id=p_run_id
      and c.candidate_id=d.candidate_id;

    select count(*) into v_reviewed from tmp_panel_dependence;
    select count(*) into v_passed from tmp_panel_dependence where passed;

    return jsonb_build_object(
        'run_id',p_run_id,
        'reviewed',v_reviewed,
        'passed',v_passed,
        'holdout_accessed',false,
        'method','instrument_cluster_means'
    );
end;
$$;

revoke all on function research_hub.review_panel_candidate_dependence(uuid) from public,anon,authenticated;

comment on function research_hub.review_panel_candidate_dependence(uuid) is
'Performs an instrument-cluster dependence review using frozen discovery/validation definitions only. Holdout remains untouched.';
