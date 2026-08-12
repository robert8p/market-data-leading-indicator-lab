create table if not exists research_hub.crypto_spot_futures15m_screen_tasks_v1(
    task_id bigserial primary key,
    run_id uuid not null references research_hub.experiment_runs(run_id) on delete cascade,
    feature_key text not null,
    status text not null default 'queued' check(status in ('queued','running','completed','failed','cancelled')),
    attempts integer not null default 0,
    claimed_at timestamptz,
    heartbeat_at timestamptz,
    last_error text,
    result_summary jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    completed_at timestamptz,
    updated_at timestamptz not null default now(),
    unique(run_id,feature_key)
);

create index if not exists crypto_spot_futures15m_screen_tasks_claim_idx
    on research_hub.crypto_spot_futures15m_screen_tasks_v1(status,task_id)
    where status='queued';

revoke all on research_hub.crypto_spot_futures15m_screen_tasks_v1 from public,anon,authenticated;

insert into research_hub.experiment_runs(
    run_key,name,status,feature_set_key,outcome_set_key,
    discovery_start,discovery_end,validation_start,validation_end,holdout_start,holdout_end,
    config,code_version
)
values(
    'crypto_spot_futures15m_tail_v1_202510_202606',
    'Crypto spot/futures 15m typed tail discovery v1',
    'awaiting_materialization',
    'crypto.spot_futures15m.v1','crypto.binance_spot15m_returns.v1',
    '2025-10-01 00:00:00+00','2026-04-01 00:00:00+00',
    '2026-04-01 00:00:00+00','2026-06-01 00:00:00+00',
    '2026-06-01 00:00:00+00','2026-07-01 00:00:00+00',
    '{"tail_quantiles":[0.02,0.05,0.10,0.20],"round_trip_cost_bps":20,"minimum_discovery_events":500,"minimum_validation_events":200,"minimum_discovery_clusters":60,"minimum_validation_clusters":30,"minimum_hit_rate":0.500001,"maximum_worst_loss_ratio":0.10,"fdr_q":0.05,"pvalue_method":"instrument_day_cluster_means","execution_replication_required":true,"holdout_accessed":false}'::jsonb,
    'crypto-spot-futures15m-typed-screen-v1'
)
on conflict(run_key) do nothing;

create or replace function research_hub.prepare_crypto_spot_futures15m_screen_v1()
returns jsonb
language plpgsql
security invoker
set search_path=pg_catalog,research_hub,pg_temp
as $$
declare
    v_ready jsonb;
    v_run_id uuid;
    v_inserted bigint:=0;
begin
    v_ready:=research_hub.crypto_spot_futures15m_materialization_status_v1();
    if coalesce((v_ready->>'discovery_ready')::boolean,false) is not true
       or coalesce((v_ready->>'validation_ready')::boolean,false) is not true then
        return jsonb_build_object('status','not_ready','materialization',v_ready);
    end if;
    if coalesce((v_ready#>>'{sealed_holdout,untouched}')::boolean,false) is not true then
        raise exception 'Sealed June holdout is not untouched';
    end if;

    select run_id into v_run_id
    from research_hub.experiment_runs
    where run_key='crypto_spot_futures15m_tail_v1_202510_202606';
    if v_run_id is null then raise exception 'Crypto spot/futures experiment run is missing'; end if;

    insert into research_hub.crypto_spot_futures15m_screen_tasks_v1(run_id,feature_key,status)
    select v_run_id,x,'queued'
    from unnest((select feature_keys from research_hub.feature_sets where feature_set_key='crypto.spot_futures15m.v1')) x
    on conflict(run_id,feature_key) do nothing;
    get diagnostics v_inserted=row_count;

    update research_hub.experiment_runs
    set status='screening_tasks_planned',started_at=coalesce(started_at,now()),completed_at=null,
        config=config||jsonb_build_object('materialization_verified_at',now(),'holdout_accessed',false),updated_at=now()
    where run_id=v_run_id
      and status not in ('validation_complete_candidates_frozen','holdout_evaluated');

    return jsonb_build_object('status','prepared','run_id',v_run_id,'new_tasks',v_inserted,'materialization',v_ready);
end;
$$;

create or replace function research_hub.run_crypto_spot_futures15m_feature_screen_v1(
    p_run_id uuid,
    p_feature_key text
)
returns jsonb
language plpgsql
security invoker
set search_path=pg_catalog,research_hub,pg_temp
as $$
declare
    r research_hub.experiment_runs%rowtype;
    v_column text;
    v_cost double precision;
    v_min_events integer;
    v_min_validation integer;
    v_min_clusters integer;
    v_min_validation_clusters integer;
    v_min_hit double precision;
    v_max_wlr double precision;
    v_inserted bigint:=0;
    v_sql text;
begin
    select * into r from research_hub.experiment_runs where run_id=p_run_id;
    if not found then raise exception 'Unknown experiment run %',p_run_id; end if;
    if r.feature_set_key<>'crypto.spot_futures15m.v1' or r.outcome_set_key<>'crypto.binance_spot15m_returns.v1' then
        raise exception 'Run % is not the typed crypto spot/futures experiment',r.run_key;
    end if;
    if not (p_feature_key=any((select feature_keys from research_hub.feature_sets where feature_set_key=r.feature_set_key))) then
        raise exception 'Feature % is not in feature set %',p_feature_key,r.feature_set_key;
    end if;
    if left(p_feature_key,3)<>'cf.' then raise exception 'Unexpected feature key %',p_feature_key; end if;
    v_column:=substr(p_feature_key,4);
    if not exists(
        select 1 from information_schema.columns
        where table_schema='research_hub' and table_name='crypto_spot_futures15m_features_v1' and column_name=v_column
    ) then raise exception 'No typed feature column for %',p_feature_key; end if;

    v_cost:=coalesce((r.config->>'round_trip_cost_bps')::double precision,0)/10000.0;
    v_min_events:=coalesce((r.config->>'minimum_discovery_events')::integer,500);
    v_min_validation:=coalesce((r.config->>'minimum_validation_events')::integer,200);
    v_min_clusters:=coalesce((r.config->>'minimum_discovery_clusters')::integer,60);
    v_min_validation_clusters:=coalesce((r.config->>'minimum_validation_clusters')::integer,30);
    v_min_hit:=coalesce((r.config->>'minimum_hit_rate')::double precision,0.500001);
    v_max_wlr:=case when r.config ? 'maximum_worst_loss_ratio' then (r.config->>'maximum_worst_loss_ratio')::double precision else null end;

    delete from research_hub.experiment_tests where run_id=p_run_id and feature_key=p_feature_key;

    create temporary table tmp_crypto_sf_metrics(
        phase text not null,tail text not null,tail_q double precision not null,threshold double precision not null,
        horizon_seconds integer not null,trade_direction integer not null,n bigint not null,
        mean_gross double precision,mean_net double precision,median_net double precision,hit_rate_net double precision,
        profit_factor_net double precision,worst_net double precision,avg_winner_net double precision,avg_loser_net double precision,
        worst_loss_ratio double precision,cluster_n bigint,cluster_mean double precision,cluster_sd double precision,cluster_p double precision,
        primary key(phase,tail,tail_q,horizon_seconds)
    ) on commit drop;

    v_sql:=format($sql$
        insert into tmp_crypto_sf_metrics
        with base as(
            select f.instrument_key,f.decision_ts,f.%1$I feature_value,
                   case when f.decision_ts>=$1 and f.decision_ts<$2 then 'discovery'
                        when f.decision_ts>=$3 and f.decision_ts<$4 then 'validation' end phase
            from research_hub.crypto_spot_futures15m_features_v1 f
            where f.decision_ts>=$1 and f.decision_ts<$4 and f.%1$I is not null
        ), quantiles as(
            select distinct x::double precision tail_q
            from jsonb_array_elements_text($5::jsonb) q(x)
        ), thresholds as(
            select q.tail_q,
                   percentile_cont(q.tail_q) within group(order by b.feature_value) low_cut,
                   percentile_cont(1.0-q.tail_q) within group(order by b.feature_value) high_cut
            from base b cross join quantiles q
            where b.phase='discovery'
            group by q.tail_q
        ), events as(
            select b.instrument_key,b.decision_ts,b.phase,x.tail,t.tail_q,x.threshold
            from base b cross join thresholds t
            cross join lateral(values('LOW'::text,t.low_cut),('HIGH'::text,t.high_cut)) x(tail,threshold)
            where (x.tail='LOW' and b.feature_value<=x.threshold)
               or (x.tail='HIGH' and b.feature_value>=x.threshold)
        ), event_outcomes as(
            select e.*,o.horizon_seconds,o.gross_return
            from events e
            join research_hub.crypto_spot_futures15m_outcomes_v1 o
              on o.instrument_key=e.instrument_key and o.decision_ts=e.decision_ts
        ), directions as(
            select tail,tail_q,threshold,horizon_seconds,
                   case when avg(gross_return)>=0 then 1 else -1 end trade_direction,
                   count(*) discovery_n
            from event_outcomes
            where phase='discovery'
            group by tail,tail_q,threshold,horizon_seconds
            having count(*)>=$6
        ), scored as(
            select e.phase,e.instrument_key,e.decision_ts,e.tail,e.tail_q,d.threshold,e.horizon_seconds,d.trade_direction,
                   d.trade_direction*e.gross_return directed_gross,
                   d.trade_direction*e.gross_return-$7 net_return
            from event_outcomes e
            join directions d using(tail,tail_q,horizon_seconds)
        ), event_metrics as(
            select phase,tail,tail_q,threshold,horizon_seconds,trade_direction,count(*)::bigint n,
                   avg(directed_gross) mean_gross,avg(net_return) mean_net,
                   percentile_cont(0.5) within group(order by net_return) median_net,
                   avg((net_return>0)::integer::double precision) hit_rate_net,
                   case when abs(sum(net_return) filter(where net_return<0))>0
                        then sum(net_return) filter(where net_return>0)/abs(sum(net_return) filter(where net_return<0)) end profit_factor_net,
                   min(net_return) worst_net,
                   avg(net_return) filter(where net_return>0) avg_winner_net,
                   avg(net_return) filter(where net_return<0) avg_loser_net,
                   case when min(net_return)>=0 then 0.0
                        when (avg(net_return) filter(where net_return>0))>0
                        then abs(min(net_return))/(avg(net_return) filter(where net_return>0)) end worst_loss_ratio
            from scored
            group by phase,tail,tail_q,threshold,horizon_seconds,trade_direction
        ), cluster_means as(
            select phase,tail,tail_q,threshold,horizon_seconds,trade_direction,
                   instrument_key,decision_ts::date cluster_date,avg(net_return) cluster_return
            from scored
            group by phase,tail,tail_q,threshold,horizon_seconds,trade_direction,instrument_key,decision_ts::date
        ), cluster_metrics as(
            select phase,tail,tail_q,threshold,horizon_seconds,trade_direction,count(*)::bigint cluster_n,
                   avg(cluster_return) cluster_mean,stddev_samp(cluster_return) cluster_sd
            from cluster_means
            group by phase,tail,tail_q,threshold,horizon_seconds,trade_direction
        )
        select e.phase,e.tail,e.tail_q,e.threshold,e.horizon_seconds,e.trade_direction,e.n,
               e.mean_gross,e.mean_net,e.median_net,e.hit_rate_net,e.profit_factor_net,e.worst_net,
               e.avg_winner_net,e.avg_loser_net,e.worst_loss_ratio,c.cluster_n,c.cluster_mean,c.cluster_sd,
               case when e.phase='discovery' then research_hub.positive_edge_pvalue(c.cluster_mean,c.cluster_sd,c.cluster_n) end
        from event_metrics e
        join cluster_metrics c using(phase,tail,tail_q,threshold,horizon_seconds,trade_direction)
    $sql$,v_column);

    execute v_sql using r.discovery_start,r.discovery_end,r.validation_start,r.validation_end,
        coalesce(r.config->'tail_quantiles','[0.02,0.05,0.10,0.20]'::jsonb),v_min_events,v_cost;

    insert into research_hub.experiment_tests(
        run_id,feature_key,outcome_key,source_instrument,target_instrument,slice_key,horizon_seconds,
        n,mean_gross,mean_net,median_net,hit_rate_net,profit_factor_net,worst_net,
        p_value,q_value,effect_size,adjacent_horizon_positive,validation_positive,
        avg_winner_net,avg_loser_net,worst_loss_ratio,
        validation_n,validation_mean_net,validation_median_net,validation_hit_rate_net,
        validation_profit_factor_net,validation_worst_net,validation_avg_winner_net,
        validation_avg_loser_net,validation_worst_loss_ratio,metadata
    )
    select p_run_id,p_feature_key,'horizon_'||d.horizon_seconds,'BINANCE_SPOT_FUTURES_26','SELF',
           d.tail||'_Q'||to_char(d.tail_q,'FM0.000'),d.horizon_seconds,
           d.n,d.mean_gross,d.mean_net,d.median_net,d.hit_rate_net,d.profit_factor_net,d.worst_net,
           d.cluster_p,null,null,false,
           (coalesce(v.mean_net,-1e100)>0 and coalesce(v.n,0)>=v_min_validation
             and coalesce(v.hit_rate_net,0)>=v_min_hit
             and (v_max_wlr is null or coalesce(v.worst_loss_ratio,1e100)<=v_max_wlr)
             and coalesce(v.cluster_n,0)>=v_min_validation_clusters and coalesce(v.cluster_mean,-1e100)>0),
           d.avg_winner_net,d.avg_loser_net,d.worst_loss_ratio,
           v.n,v.mean_net,v.median_net,v.hit_rate_net,v.profit_factor_net,v.worst_net,
           v.avg_winner_net,v.avg_loser_net,v.worst_loss_ratio,
           jsonb_build_object(
             'engine','crypto_spot_futures15m_typed_tail_v1','typed_column',v_column,
             'threshold',d.threshold,'tail_quantile',d.tail_q,'trade_direction',d.trade_direction,
             'round_trip_cost_bps',v_cost*10000.0,'pvalue_method','instrument_day_cluster_means',
             'discovery_clusters',jsonb_build_object('n',d.cluster_n,'mean',d.cluster_mean,'sd',d.cluster_sd,'p_value',d.cluster_p),
             'validation_clusters',case when v.cluster_n is null then null else jsonb_build_object('n',v.cluster_n,'mean',v.cluster_mean,'sd',v.cluster_sd) end,
             'minimum_discovery_clusters',v_min_clusters,'minimum_validation_clusters',v_min_validation_clusters,
             'execution_replication_required',true,'holdout_accessed',false
           )
    from tmp_crypto_sf_metrics d
    left join tmp_crypto_sf_metrics v
      on v.phase='validation' and v.tail=d.tail and v.tail_q=d.tail_q and v.horizon_seconds=d.horizon_seconds
    where d.phase='discovery' and d.cluster_n>=v_min_clusters;
    get diagnostics v_inserted=row_count;

    return jsonb_build_object('status','completed','run_id',p_run_id,'feature_key',p_feature_key,'tests_inserted',v_inserted,'holdout_accessed',false);
end;
$$;

create or replace function research_hub.finalize_crypto_spot_futures15m_screen_v1(p_run_id uuid)
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
    v_tests bigint;
    v_candidates bigint;
begin
    select * into r from research_hub.experiment_runs where run_id=p_run_id for update;
    if not found then raise exception 'Unknown experiment run %',p_run_id; end if;
    if coalesce((r.config->>'holdout_accessed')::boolean,false) then raise exception 'Holdout already accessed'; end if;
    v_fdr:=coalesce((r.config->>'fdr_q')::double precision,0.05);
    v_min_hit:=coalesce((r.config->>'minimum_hit_rate')::double precision,0.500001);
    v_max_wlr:=case when r.config ? 'maximum_worst_loss_ratio' then (r.config->>'maximum_worst_loss_ratio')::double precision else null end;

    with ranked as(
        select test_id,p_value,row_number() over(order by p_value,test_id) rn,count(*) over() m
        from research_hub.experiment_tests where run_id=p_run_id and p_value is not null
    ), raw_q as(
        select test_id,rn,least(1.0,p_value*m::double precision/rn::double precision) raw_q from ranked
    ), adjusted as(
        select test_id,least(1.0,min(raw_q) over(order by rn desc rows between unbounded preceding and current row)) q_value from raw_q
    )
    update research_hub.experiment_tests t set q_value=a.q_value from adjusted a where t.test_id=a.test_id;

    with ordered as(
        select test_id,
               lag(mean_net) over(partition by run_id,feature_key,slice_key order by horizon_seconds) prev_mean,
               lead(mean_net) over(partition by run_id,feature_key,slice_key order by horizon_seconds) next_mean
        from research_hub.experiment_tests where run_id=p_run_id
    )
    update research_hub.experiment_tests t
    set adjacent_horizon_positive=(coalesce(o.prev_mean,0)>0 or coalesce(o.next_mean,0)>0)
    from ordered o where t.test_id=o.test_id;

    delete from research_hub.candidate_ledger where run_id=p_run_id;
    insert into research_hub.candidate_ledger(
        candidate_id,run_id,status,descriptive_name,frozen_definition,metrics,confidence,next_test,frozen_at
    )
    select 'RH-CF-'||upper(substr(md5(r.run_key||'|'||t.feature_key||'|'||t.slice_key||'|'||t.horizon_seconds),1,12)),
           p_run_id,'FROZEN_VALIDATION_PASSED',
           t.slice_key||' '||t.feature_key||' -> SELF @ '||t.horizon_seconds||'s',
           jsonb_build_object(
             'engine','crypto_spot_futures15m_typed_tail_v1','feature_set_key',r.feature_set_key,'outcome_set_key',r.outcome_set_key,
             'feature_key',t.feature_key,'typed_column',t.metadata->>'typed_column','tail',split_part(t.slice_key,'_',1),
             'tail_quantile',t.metadata->'tail_quantile','threshold',t.metadata->'threshold','horizon_seconds',t.horizon_seconds,
             'trade_direction',t.metadata->'trade_direction','round_trip_cost_bps',t.metadata->'round_trip_cost_bps',
             'threshold_learning_period',jsonb_build_array(r.discovery_start,r.discovery_end),
             'validation_period',jsonb_build_array(r.validation_start,r.validation_end),
             'holdout_period',jsonb_build_array(r.holdout_start,r.holdout_end),
             'execution_replication_required',true,'holdout_accessed',false
           ),
           jsonb_build_object(
             'discovery',jsonb_build_object('n',t.n,'mean_net',t.mean_net,'median_net',t.median_net,'hit_rate_net',t.hit_rate_net,'worst_net',t.worst_net,'avg_winner_net',t.avg_winner_net,'worst_loss_ratio',t.worst_loss_ratio),
             'validation',jsonb_build_object('n',t.validation_n,'mean_net',t.validation_mean_net,'median_net',t.validation_median_net,'hit_rate_net',t.validation_hit_rate_net,'worst_net',t.validation_worst_net,'avg_winner_net',t.validation_avg_winner_net,'worst_loss_ratio',t.validation_worst_loss_ratio),
             'q_value',t.q_value,'cluster_statistics',jsonb_build_object('discovery',t.metadata->'discovery_clusters','validation',t.metadata->'validation_clusters')
           ),
           case when t.q_value<=v_fdr/10.0 then 'Strong screening candidate' else 'Screening candidate' end,
           'Run block-bootstrap / regime robustness and 1-second execution-mechanism replication before any sealed-holdout evaluation.',now()
    from research_hub.experiment_tests t
    where t.run_id=p_run_id and t.q_value is not null and t.q_value<=v_fdr
      and t.mean_net>0 and t.validation_positive is true and t.adjacent_horizon_positive is true
      and t.hit_rate_net>=v_min_hit and (v_max_wlr is null or coalesce(t.worst_loss_ratio,1e100)<=v_max_wlr)
    on conflict(candidate_id) do update set
      status=excluded.status,frozen_definition=excluded.frozen_definition,metrics=excluded.metrics,
      confidence=excluded.confidence,next_test=excluded.next_test,frozen_at=excluded.frozen_at,updated_at=now();

    select count(*) into v_tests from research_hub.experiment_tests where run_id=p_run_id;
    select count(*) into v_candidates from research_hub.candidate_ledger where run_id=p_run_id;
    update research_hub.experiment_runs
    set status='validation_complete_candidates_frozen',search_space_tests=v_tests,completed_at=now(),updated_at=now(),
        config=config||jsonb_build_object('holdout_accessed',false,'screen_finalized_at',now(),'pvalue_method','instrument_day_cluster_means')
    where run_id=p_run_id;
    return jsonb_build_object('status','finalized','run_id',p_run_id,'tests',v_tests,'candidates',v_candidates,'holdout_accessed',false);
end;
$$;

create or replace function research_hub.process_next_crypto_spot_futures15m_screen_task_v1()
returns jsonb
language plpgsql
security invoker
set search_path=pg_catalog,research_hub,pg_temp
as $$
declare
    v_ready jsonb;
    v_run_id uuid;
    v_task_id bigint;
    v_feature_key text;
    v_attempts integer;
    v_result jsonb;
    v_remaining bigint;
    v_failed bigint;
begin
    if not pg_try_advisory_xact_lock(hashtext('rh-crypto-spot-futures15m-screen-v1')::bigint) then
        return jsonb_build_object('status','busy');
    end if;
    v_ready:=research_hub.crypto_spot_futures15m_materialization_status_v1();
    if coalesce((v_ready->>'discovery_ready')::boolean,false) is not true
       or coalesce((v_ready->>'validation_ready')::boolean,false) is not true then
        return jsonb_build_object('status','not_ready','materialization',v_ready);
    end if;
    if coalesce((v_ready#>>'{sealed_holdout,untouched}')::boolean,false) is not true then
        raise exception 'Sealed June holdout is not untouched';
    end if;
    perform research_hub.prepare_crypto_spot_futures15m_screen_v1();

    select run_id into v_run_id from research_hub.experiment_runs
    where run_key='crypto_spot_futures15m_tail_v1_202510_202606';

    select task_id,feature_key,attempts into v_task_id,v_feature_key,v_attempts
    from research_hub.crypto_spot_futures15m_screen_tasks_v1
    where run_id=v_run_id and status='queued'
    order by task_id limit 1 for update skip locked;

    if v_task_id is not null then
        update research_hub.crypto_spot_futures15m_screen_tasks_v1
        set status='running',attempts=attempts+1,claimed_at=now(),heartbeat_at=now(),last_error=null,updated_at=now()
        where task_id=v_task_id;
        begin
            v_result:=research_hub.run_crypto_spot_futures15m_feature_screen_v1(v_run_id,v_feature_key);
            update research_hub.crypto_spot_futures15m_screen_tasks_v1
            set status='completed',result_summary=v_result,heartbeat_at=now(),completed_at=now(),updated_at=now()
            where task_id=v_task_id;
            return jsonb_build_object('status','completed_feature','feature_key',v_feature_key,'result',v_result,'holdout_accessed',false);
        exception when others then
            update research_hub.crypto_spot_futures15m_screen_tasks_v1
            set status=case when v_attempts+1>=3 then 'failed' else 'queued' end,
                last_error=left(sqlerrm,4000),heartbeat_at=now(),updated_at=now()
            where task_id=v_task_id;
            return jsonb_build_object('status',case when v_attempts+1>=3 then 'failed' else 'retry_queued' end,'feature_key',v_feature_key,'error',sqlerrm,'holdout_accessed',false);
        end;
    end if;

    select count(*) into v_remaining from research_hub.crypto_spot_futures15m_screen_tasks_v1
    where run_id=v_run_id and status in ('queued','running');
    select count(*) into v_failed from research_hub.crypto_spot_futures15m_screen_tasks_v1
    where run_id=v_run_id and status='failed';
    if v_failed>0 then
        update research_hub.experiment_runs set status='screening_failed',updated_at=now() where run_id=v_run_id;
        return jsonb_build_object('status','screening_failed','failed_tasks',v_failed,'holdout_accessed',false);
    end if;
    if v_remaining=0 then
        return research_hub.finalize_crypto_spot_futures15m_screen_v1(v_run_id);
    end if;
    return jsonb_build_object('status','waiting','remaining',v_remaining,'holdout_accessed',false);
end;
$$;

create or replace function research_hub.crypto_spot_futures15m_screen_status_v1()
returns jsonb
language sql
security invoker
stable
set search_path=pg_catalog,research_hub,pg_temp
as $$
with r as(
    select * from research_hub.experiment_runs where run_key='crypto_spot_futures15m_tail_v1_202510_202606'
), t as(
    select count(*) total,
           count(*) filter(where status='queued') queued,
           count(*) filter(where status='running') running,
           count(*) filter(where status='completed') completed,
           count(*) filter(where status='failed') failed
    from research_hub.crypto_spot_futures15m_screen_tasks_v1
    where run_id=(select run_id from r)
)
select jsonb_build_object(
    'run_id',(select run_id from r),'run_status',(select status from r),
    'materialization',research_hub.crypto_spot_futures15m_materialization_status_v1(),
    'tasks',to_jsonb(t),'tests',(select count(*) from research_hub.experiment_tests where run_id=(select run_id from r)),
    'candidates',(select count(*) from research_hub.candidate_ledger where run_id=(select run_id from r)),
    'holdout_accessed',coalesce(((select config from r)->>'holdout_accessed')::boolean,false)
) from t;
$$;

revoke all on function research_hub.prepare_crypto_spot_futures15m_screen_v1() from public,anon,authenticated;
revoke all on function research_hub.run_crypto_spot_futures15m_feature_screen_v1(uuid,text) from public,anon,authenticated;
revoke all on function research_hub.finalize_crypto_spot_futures15m_screen_v1(uuid) from public,anon,authenticated;
revoke all on function research_hub.process_next_crypto_spot_futures15m_screen_task_v1() from public,anon,authenticated;
revoke all on function research_hub.crypto_spot_futures15m_screen_status_v1() from public,anon,authenticated;

do $$
begin
    if exists(select 1 from cron.job where jobname='research_hub_crypto_spot_futures15m_screen_v1') then
        perform cron.unschedule((select jobid from cron.job where jobname='research_hub_crypto_spot_futures15m_screen_v1' limit 1));
    end if;
    perform cron.schedule(
        'research_hub_crypto_spot_futures15m_screen_v1',
        '7,27,47 * * * *',
        'set work_mem=''96MB''; set statement_timeout=''8min''; select research_hub.process_next_crypto_spot_futures15m_screen_task_v1();'
    );
end $$;