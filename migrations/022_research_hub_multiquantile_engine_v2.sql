-- Research Hub multi-quantile discovery engine v2.
-- Thresholds and direction are learned only in discovery. Validation is frozen.
-- Holdout is never read by this function.

alter table research_hub.experiment_tests add column if not exists avg_winner_net double precision;
alter table research_hub.experiment_tests add column if not exists avg_loser_net double precision;
alter table research_hub.experiment_tests add column if not exists worst_loss_ratio double precision;
alter table research_hub.experiment_tests add column if not exists validation_n bigint;
alter table research_hub.experiment_tests add column if not exists validation_mean_net double precision;
alter table research_hub.experiment_tests add column if not exists validation_median_net double precision;
alter table research_hub.experiment_tests add column if not exists validation_hit_rate_net double precision;
alter table research_hub.experiment_tests add column if not exists validation_profit_factor_net double precision;
alter table research_hub.experiment_tests add column if not exists validation_worst_net double precision;
alter table research_hub.experiment_tests add column if not exists validation_avg_winner_net double precision;
alter table research_hub.experiment_tests add column if not exists validation_avg_loser_net double precision;
alter table research_hub.experiment_tests add column if not exists validation_worst_loss_ratio double precision;

create or replace function research_hub.run_multiquantile_tail_screen(p_run_id uuid)
returns jsonb language plpgsql security invoker set search_path=research_hub,pg_temp as $$
declare
    r research_hub.experiment_runs%rowtype;
    v_cost double precision;
    v_min_events integer;
    v_min_validation integer;
    v_fdr double precision;
    v_min_hit double precision;
    v_max_wlr double precision;
    v_tests bigint;
    v_candidates bigint;
begin
    select * into r from research_hub.experiment_runs where run_id=p_run_id for update;
    if not found then raise exception 'Unknown research_hub experiment run %',p_run_id; end if;
    if r.feature_set_key is null or r.outcome_set_key is null then raise exception 'Run % must define feature and outcome sets',r.run_key; end if;
    if r.discovery_start is null or r.discovery_end is null or r.validation_start is null or r.validation_end is null then raise exception 'Run % must define discovery and validation windows',r.run_key; end if;
    if r.validation_start<r.discovery_end then raise exception 'Validation must not overlap discovery for run %',r.run_key; end if;

    v_cost:=coalesce((r.config->>'round_trip_cost_bps')::double precision,0)/10000.0;
    v_min_events:=coalesce((r.config->>'minimum_discovery_events')::integer,100);
    v_min_validation:=coalesce((r.config->>'minimum_validation_events')::integer,greatest(30,v_min_events/3));
    v_fdr:=coalesce((r.config->>'fdr_q')::double precision,0.05);
    v_min_hit:=coalesce((r.config->>'minimum_hit_rate')::double precision,0.0);
    v_max_wlr:=case when r.config ? 'maximum_worst_loss_ratio' then (r.config->>'maximum_worst_loss_ratio')::double precision else null end;

    if exists (
        select 1 from jsonb_array_elements_text(coalesce(r.config->'tail_quantiles','[0.02,0.05,0.10,0.20]'::jsonb)) q(x)
        where q.x::double precision<=0 or q.x::double precision>=0.5
    ) then raise exception 'All tail_quantiles must be between 0 and 0.5'; end if;

    update research_hub.experiment_runs set status='running',started_at=coalesce(started_at,now()),updated_at=now() where run_id=p_run_id;
    delete from research_hub.experiment_tests where run_id=p_run_id;
    delete from research_hub.candidate_ledger where run_id=p_run_id;

    create temporary table tmp_rh_mq_metrics(
        phase text not null,source_instrument text not null,feature_key text not null,tail text not null,tail_q double precision not null,threshold double precision not null,
        target_instrument text not null,horizon_seconds integer not null,trade_direction integer not null,n bigint not null,
        mean_gross double precision,mean_net double precision,median_net double precision,hit_rate_net double precision,
        profit_factor_net double precision,worst_net double precision,avg_winner_net double precision,avg_loser_net double precision,worst_loss_ratio double precision,sd_net double precision,
        primary key(phase,source_instrument,feature_key,tail,tail_q,target_instrument,horizon_seconds)
    ) on commit drop;

    insert into tmp_rh_mq_metrics
    with base as(
        select fr.instrument_key source_instrument,nullif(fr.quality->>'legacy_run_id','') scope_key,fr.decision_ts,j.key feature_key,
               (j.value#>>'{}')::double precision feature_value,
               case when fr.decision_ts>=r.discovery_start and fr.decision_ts<r.discovery_end then 'discovery'
                    when fr.decision_ts>=r.validation_start and fr.decision_ts<r.validation_end then 'validation' end phase
        from research_hub.feature_rows fr cross join lateral jsonb_each(fr.features) j(key,value)
        where fr.feature_set_key=r.feature_set_key and fr.decision_ts>=r.discovery_start and fr.decision_ts<r.validation_end and jsonb_typeof(j.value)='number'
    ), quantiles as(
        select distinct x::double precision tail_q
        from jsonb_array_elements_text(coalesce(r.config->'tail_quantiles','[0.02,0.05,0.10,0.20]'::jsonb)) q(x)
    ), thresholds as(
        select b.source_instrument,b.scope_key,b.feature_key,q.tail_q,
               percentile_cont(q.tail_q) within group(order by b.feature_value) low_cut,
               percentile_cont(1.0-q.tail_q) within group(order by b.feature_value) high_cut
        from base b cross join quantiles q where b.phase='discovery'
        group by b.source_instrument,b.scope_key,b.feature_key,q.tail_q
    ), events as(
        select b.source_instrument,b.scope_key,b.decision_ts,b.feature_key,b.phase,x.tail,t.tail_q,x.threshold
        from base b join thresholds t on t.source_instrument=b.source_instrument and t.feature_key=b.feature_key and t.scope_key is not distinct from b.scope_key
        cross join lateral(values('LOW'::text,t.low_cut),('HIGH'::text,t.high_cut)) x(tail,threshold)
        where b.phase is not null and ((x.tail='LOW' and b.feature_value<=x.threshold) or (x.tail='HIGH' and b.feature_value>=x.threshold))
    ), event_outcomes as(
        select e.*,o.instrument_key target_instrument,o.horizon_seconds,o.gross_return
        from events e join research_hub.outcome_rows o on o.outcome_set_key=r.outcome_set_key and o.decision_ts=e.decision_ts and o.gross_return is not null
         and (e.scope_key is null or coalesce(o.metadata->>'legacy_run_id','')=e.scope_key)
    ), directions as(
        select source_instrument,feature_key,tail,tail_q,threshold,target_instrument,horizon_seconds,
               case when avg(gross_return)>=0 then 1 else -1 end trade_direction,count(*) discovery_n
        from event_outcomes where phase='discovery'
        group by source_instrument,feature_key,tail,tail_q,threshold,target_instrument,horizon_seconds having count(*)>=v_min_events
    ), scored as(
        select e.phase,e.source_instrument,e.feature_key,e.tail,e.tail_q,d.threshold,e.target_instrument,e.horizon_seconds,d.trade_direction,
               d.trade_direction*e.gross_return directed_gross,d.trade_direction*e.gross_return-v_cost net_return
        from event_outcomes e join directions d using(source_instrument,feature_key,tail,tail_q,target_instrument,horizon_seconds)
    )
    select phase,source_instrument,feature_key,tail,tail_q,threshold,target_instrument,horizon_seconds,trade_direction,count(*)::bigint,
           avg(directed_gross),avg(net_return),percentile_cont(0.5) within group(order by net_return),avg((net_return>0)::integer::double precision),
           case when abs(sum(net_return) filter(where net_return<0))>0 then sum(net_return) filter(where net_return>0)/abs(sum(net_return) filter(where net_return<0)) end,
           min(net_return),avg(net_return) filter(where net_return>0),avg(net_return) filter(where net_return<0),
           case when min(net_return)>=0 then 0.0 when (avg(net_return) filter(where net_return>0))>0 then abs(min(net_return))/(avg(net_return) filter(where net_return>0)) end,
           stddev_samp(net_return)
    from scored group by phase,source_instrument,feature_key,tail,tail_q,threshold,target_instrument,horizon_seconds,trade_direction;

    insert into research_hub.experiment_tests
    (run_id,feature_key,outcome_key,source_instrument,target_instrument,slice_key,horizon_seconds,n,mean_gross,mean_net,median_net,hit_rate_net,profit_factor_net,worst_net,avg_winner_net,avg_loser_net,worst_loss_ratio,effect_size,validation_positive,validation_n,validation_mean_net,validation_median_net,validation_hit_rate_net,validation_profit_factor_net,validation_worst_net,validation_avg_winner_net,validation_avg_loser_net,validation_worst_loss_ratio,metadata)
    select p_run_id,d.feature_key,'horizon_'||d.horizon_seconds,d.source_instrument,d.target_instrument,d.tail||'_Q'||to_char(d.tail_q,'FM0.000'),d.horizon_seconds,d.n,d.mean_gross,d.mean_net,d.median_net,d.hit_rate_net,d.profit_factor_net,d.worst_net,d.avg_winner_net,d.avg_loser_net,d.worst_loss_ratio,
           case when d.sd_net is not null and d.sd_net>0 then d.mean_net/d.sd_net end,
           (coalesce(v.mean_net,-1e100)>0 and coalesce(v.n,0)>=v_min_validation and coalesce(v.hit_rate_net,0)>=v_min_hit and (v_max_wlr is null or coalesce(v.worst_loss_ratio,1e100)<=v_max_wlr)),
           v.n,v.mean_net,v.median_net,v.hit_rate_net,v.profit_factor_net,v.worst_net,v.avg_winner_net,v.avg_loser_net,v.worst_loss_ratio,
           jsonb_build_object('threshold',d.threshold,'tail_quantile',d.tail_q,'trade_direction',d.trade_direction,'round_trip_cost_bps',v_cost*10000.0,
             'discovery',jsonb_build_object('n',d.n,'mean_net',d.mean_net,'median_net',d.median_net,'hit_rate_net',d.hit_rate_net,'profit_factor_net',d.profit_factor_net,'worst_net',d.worst_net,'avg_winner_net',d.avg_winner_net,'avg_loser_net',d.avg_loser_net,'worst_loss_ratio',d.worst_loss_ratio),
             'validation',case when v.n is null then null else jsonb_build_object('n',v.n,'mean_net',v.mean_net,'median_net',v.median_net,'hit_rate_net',v.hit_rate_net,'profit_factor_net',v.profit_factor_net,'worst_net',v.worst_net,'avg_winner_net',v.avg_winner_net,'avg_loser_net',v.avg_loser_net,'worst_loss_ratio',v.worst_loss_ratio) end,
             'promotion_constraints',jsonb_build_object('minimum_hit_rate',v_min_hit,'maximum_worst_loss_ratio',v_max_wlr),'holdout_accessed',false)
    from tmp_rh_mq_metrics d left join tmp_rh_mq_metrics v on v.phase='validation' and v.source_instrument=d.source_instrument and v.feature_key=d.feature_key and v.tail=d.tail and v.tail_q=d.tail_q and v.target_instrument=d.target_instrument and v.horizon_seconds=d.horizon_seconds
    where d.phase='discovery';

    with ranked as(
        select test_id,p_value,row_number() over(order by p_value,test_id) rn,count(*) over() m from research_hub.experiment_tests where run_id=p_run_id and p_value is not null
    ),raw_q as(select test_id,rn,least(1.0,p_value*m::double precision/rn::double precision) raw_q from ranked),
    adjusted as(select test_id,least(1.0,min(raw_q) over(order by rn desc rows between unbounded preceding and current row)) q_value from raw_q)
    update research_hub.experiment_tests t set q_value=a.q_value from adjusted a where t.test_id=a.test_id;

    with ordered as(
        select test_id,lag(mean_net) over(partition by run_id,source_instrument,target_instrument,feature_key,slice_key order by horizon_seconds) prev_mean,
               lead(mean_net) over(partition by run_id,source_instrument,target_instrument,feature_key,slice_key order by horizon_seconds) next_mean
        from research_hub.experiment_tests where run_id=p_run_id
    ) update research_hub.experiment_tests t set adjacent_horizon_positive=(coalesce(o.prev_mean,0)>0 or coalesce(o.next_mean,0)>0) from ordered o where t.test_id=o.test_id;

    insert into research_hub.candidate_ledger(candidate_id,run_id,status,descriptive_name,frozen_definition,metrics,confidence,next_test,frozen_at)
    select 'RH-'||upper(substr(md5(r.run_key||'|'||t.source_instrument||'|'||t.feature_key||'|'||t.slice_key||'|'||t.target_instrument||'|'||t.horizon_seconds),1,12)),p_run_id,
           'FROZEN_VALIDATION_PASSED',t.source_instrument||' '||t.slice_key||' '||t.feature_key||' -> '||t.target_instrument||' @ '||t.horizon_seconds||'s',
           jsonb_build_object('engine','research_hub_multiquantile_v2','feature_set_key',r.feature_set_key,'outcome_set_key',r.outcome_set_key,'source_instrument',t.source_instrument,'feature_key',t.feature_key,'tail',split_part(t.slice_key,'_',1),'tail_quantile',t.metadata->'tail_quantile','threshold',t.metadata->'threshold','target_instrument',t.target_instrument,'horizon_seconds',t.horizon_seconds,'trade_direction',t.metadata->'trade_direction','round_trip_cost_bps',t.metadata->'round_trip_cost_bps','threshold_learning_period',jsonb_build_array(r.discovery_start,r.discovery_end),'validation_period',jsonb_build_array(r.validation_start,r.validation_end),'holdout_accessed',false),
           jsonb_build_object('discovery',t.metadata->'discovery','validation',t.metadata->'validation','q_value',t.q_value,'effect_size',t.effect_size),
           case when t.q_value<=v_fdr/10.0 then 'Strong' else 'Candidate' end,
           case when r.holdout_start is not null and r.holdout_end is not null then 'Run explicit sealed-holdout evaluation without changing the frozen definition.' else 'Define an untouched holdout before further promotion.' end,now()
    from research_hub.experiment_tests t where t.run_id=p_run_id and t.q_value is not null and t.q_value<=v_fdr and t.mean_net>0 and t.validation_positive is true and t.adjacent_horizon_positive is true and t.hit_rate_net>=v_min_hit and (v_max_wlr is null or coalesce(t.worst_loss_ratio,1e100)<=v_max_wlr)
    on conflict(candidate_id) do update set status=excluded.status,frozen_definition=excluded.frozen_definition,metrics=excluded.metrics,confidence=excluded.confidence,next_test=excluded.next_test,frozen_at=excluded.frozen_at,updated_at=now();

    select count(*) into v_tests from research_hub.experiment_tests where run_id=p_run_id;
    select count(*) into v_candidates from research_hub.candidate_ledger where run_id=p_run_id;
    update research_hub.experiment_runs set status='validation_complete_candidates_frozen',search_space_tests=v_tests,completed_at=now(),updated_at=now(),config=config||jsonb_build_object('holdout_accessed',false,'engine','research_hub_multiquantile_v2') where run_id=p_run_id;
    return jsonb_build_object('run_id',p_run_id,'tests',v_tests,'candidates',v_candidates,'holdout_accessed',false);
exception when others then
    update research_hub.experiment_runs set status='failed',updated_at=now(),config=config||jsonb_build_object('last_error',sqlerrm) where run_id=p_run_id;
    return jsonb_build_object('run_id',p_run_id,'status','failed','error',sqlerrm);
end $$;

create or replace view research_hub.experiment_leaderboard with (security_invoker=true) as
select r.run_key,r.name,r.status as run_status,r.feature_set_key,r.outcome_set_key,t.test_id,t.feature_key,t.source_instrument,t.target_instrument,t.slice_key,t.horizon_seconds,t.n,t.mean_net,t.median_net,t.hit_rate_net,t.profit_factor_net,t.worst_net,t.avg_winner_net,t.avg_loser_net,t.worst_loss_ratio,t.effect_size,t.p_value,t.q_value,t.adjacent_horizon_positive,t.validation_positive,t.validation_n,t.validation_mean_net,t.validation_median_net,t.validation_hit_rate_net,t.validation_profit_factor_net,t.validation_worst_net,t.validation_avg_winner_net,t.validation_avg_loser_net,t.validation_worst_loss_ratio,t.holdout_positive,t.metadata
from research_hub.experiment_tests t join research_hub.experiment_runs r using(run_id);

comment on function research_hub.run_multiquantile_tail_screen(uuid) is 'Screens multiple discovery-learned tail quantiles in one globally FDR-corrected search; validates frozen rules without reading holdout. Optional promotion constraints are configured per run.';
comment on view research_hub.experiment_leaderboard is 'Cross-run discovery/validation leaderboard including winner/loss asymmetry metrics. Holdout remains separate.';
