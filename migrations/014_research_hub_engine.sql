create or replace function research_hub.positive_edge_pvalue_from_effect(p_effect_size double precision, p_n bigint)
returns double precision language sql immutable strict as $$
select case when p_n<=1 then null else least(1.0,greatest(0.0,0.5*erfc((p_effect_size*sqrt(p_n::double precision))/sqrt(2.0)))) end
$$;

create or replace function research_hub.enforce_positive_edge_test_pvalue()
returns trigger language plpgsql security invoker set search_path=research_hub,pg_temp as $$
begin
    if new.effect_size is not null and new.n is not null and new.n>1 then
        new.p_value:=research_hub.positive_edge_pvalue_from_effect(new.effect_size,new.n);
    end if;
    return new;
end $$;

drop trigger if exists trg_research_hub_positive_edge_pvalue on research_hub.experiment_tests;
create trigger trg_research_hub_positive_edge_pvalue before insert or update of effect_size,n,p_value on research_hub.experiment_tests
for each row execute function research_hub.enforce_positive_edge_test_pvalue();

create or replace function research_hub.run_univariate_tail_screen(p_run_id uuid)
returns jsonb language plpgsql security invoker set search_path=research_hub,pg_temp as $$
declare
    r research_hub.experiment_runs%rowtype;
    v_low_q double precision; v_high_q double precision; v_cost double precision;
    v_min_events integer; v_min_validation integer; v_fdr double precision;
    v_tests bigint; v_candidates bigint;
begin
    select * into r from research_hub.experiment_runs where run_id=p_run_id for update;
    if not found then raise exception 'Unknown research_hub experiment run %',p_run_id; end if;
    if r.feature_set_key is null or r.outcome_set_key is null then raise exception 'Run % must define feature and outcome sets',r.run_key; end if;
    if r.discovery_start is null or r.discovery_end is null or r.validation_start is null or r.validation_end is null then raise exception 'Run % must define discovery and validation windows',r.run_key; end if;
    if r.validation_start<r.discovery_end then raise exception 'Validation must not overlap discovery for run %',r.run_key; end if;

    v_low_q:=coalesce((r.config->>'low_tail_quantile')::double precision,0.05);
    v_high_q:=coalesce((r.config->>'high_tail_quantile')::double precision,0.95);
    v_cost:=coalesce((r.config->>'round_trip_cost_bps')::double precision,0)/10000.0;
    v_min_events:=coalesce((r.config->>'minimum_discovery_events')::integer,100);
    v_min_validation:=coalesce((r.config->>'minimum_validation_events')::integer,greatest(30,v_min_events/3));
    v_fdr:=coalesce((r.config->>'fdr_q')::double precision,0.05);

    update research_hub.experiment_runs set status='running',started_at=coalesce(started_at,now()),updated_at=now() where run_id=p_run_id;
    delete from research_hub.experiment_tests where run_id=p_run_id;
    delete from research_hub.candidate_ledger where run_id=p_run_id;

    create temporary table tmp_rh_metrics(
        phase text not null,source_instrument text not null,feature_key text not null,tail text not null,threshold double precision not null,
        target_instrument text not null,horizon_seconds integer not null,trade_direction integer not null,n bigint not null,
        mean_gross double precision,mean_net double precision,median_net double precision,hit_rate_net double precision,
        profit_factor_net double precision,worst_net double precision,sd_net double precision,
        primary key(phase,source_instrument,feature_key,tail,target_instrument,horizon_seconds)
    ) on commit drop;

    insert into tmp_rh_metrics
    with base as(
        select fr.instrument_key source_instrument,nullif(fr.quality->>'legacy_run_id','') scope_key,fr.decision_ts,j.key feature_key,
               (j.value#>>'{}')::double precision feature_value,
               case when fr.decision_ts>=r.discovery_start and fr.decision_ts<r.discovery_end then 'discovery'
                    when fr.decision_ts>=r.validation_start and fr.decision_ts<r.validation_end then 'validation' end phase
        from research_hub.feature_rows fr cross join lateral jsonb_each(fr.features) j(key,value)
        where fr.feature_set_key=r.feature_set_key and fr.decision_ts>=r.discovery_start and fr.decision_ts<r.validation_end and jsonb_typeof(j.value)='number'
    ), thresholds as(
        select source_instrument,scope_key,feature_key,
               percentile_cont(v_low_q) within group(order by feature_value) low_cut,
               percentile_cont(v_high_q) within group(order by feature_value) high_cut
        from base where phase='discovery' group by source_instrument,scope_key,feature_key
    ), events as(
        select b.source_instrument,b.scope_key,b.decision_ts,b.feature_key,b.phase,x.tail,x.threshold
        from base b join thresholds t on t.source_instrument=b.source_instrument and t.feature_key=b.feature_key and t.scope_key is not distinct from b.scope_key
        cross join lateral(values('LOW'::text,t.low_cut),('HIGH'::text,t.high_cut)) x(tail,threshold)
        where b.phase is not null and ((x.tail='LOW' and b.feature_value<=x.threshold) or (x.tail='HIGH' and b.feature_value>=x.threshold))
    ), event_outcomes as(
        select e.*,o.instrument_key target_instrument,o.horizon_seconds,o.gross_return
        from events e join research_hub.outcome_rows o on o.outcome_set_key=r.outcome_set_key and o.decision_ts=e.decision_ts and o.gross_return is not null
         and (e.scope_key is null or coalesce(o.metadata->>'legacy_run_id','')=e.scope_key)
    ), directions as(
        select source_instrument,feature_key,tail,threshold,target_instrument,horizon_seconds,
               case when avg(gross_return)>=0 then 1 else -1 end trade_direction,count(*) discovery_n
        from event_outcomes where phase='discovery'
        group by source_instrument,feature_key,tail,threshold,target_instrument,horizon_seconds having count(*)>=v_min_events
    ), scored as(
        select e.phase,e.source_instrument,e.feature_key,e.tail,d.threshold,e.target_instrument,e.horizon_seconds,d.trade_direction,
               d.trade_direction*e.gross_return directed_gross,d.trade_direction*e.gross_return-v_cost net_return
        from event_outcomes e join directions d using(source_instrument,feature_key,tail,target_instrument,horizon_seconds)
    )
    select phase,source_instrument,feature_key,tail,threshold,target_instrument,horizon_seconds,trade_direction,count(*)::bigint,
           avg(directed_gross),avg(net_return),percentile_cont(0.5) within group(order by net_return),avg((net_return>0)::integer::double precision),
           case when abs(sum(net_return) filter(where net_return<0))>0 then sum(net_return) filter(where net_return>0)/abs(sum(net_return) filter(where net_return<0)) end,
           min(net_return),stddev_samp(net_return)
    from scored group by phase,source_instrument,feature_key,tail,threshold,target_instrument,horizon_seconds,trade_direction;

    insert into research_hub.experiment_tests
    (run_id,feature_key,outcome_key,source_instrument,target_instrument,slice_key,horizon_seconds,n,mean_gross,mean_net,median_net,hit_rate_net,profit_factor_net,worst_net,effect_size,validation_positive,metadata)
    select p_run_id,d.feature_key,'horizon_'||d.horizon_seconds,d.source_instrument,d.target_instrument,d.tail,d.horizon_seconds,d.n,d.mean_gross,d.mean_net,d.median_net,d.hit_rate_net,d.profit_factor_net,d.worst_net,
           case when d.sd_net is not null and d.sd_net>0 then d.mean_net/d.sd_net end,
           (coalesce(v.mean_net,-1e100)>0 and coalesce(v.n,0)>=v_min_validation),
           jsonb_build_object('threshold',d.threshold,'trade_direction',d.trade_direction,'round_trip_cost_bps',v_cost*10000.0,
             'discovery',jsonb_build_object('n',d.n,'mean_net',d.mean_net,'median_net',d.median_net,'hit_rate_net',d.hit_rate_net,'profit_factor_net',d.profit_factor_net,'worst_net',d.worst_net),
             'validation',case when v.n is null then null else jsonb_build_object('n',v.n,'mean_net',v.mean_net,'median_net',v.median_net,'hit_rate_net',v.hit_rate_net,'profit_factor_net',v.profit_factor_net,'worst_net',v.worst_net) end,
             'holdout_accessed',false)
    from tmp_rh_metrics d left join tmp_rh_metrics v on v.phase='validation' and v.source_instrument=d.source_instrument and v.feature_key=d.feature_key and v.tail=d.tail and v.target_instrument=d.target_instrument and v.horizon_seconds=d.horizon_seconds
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
           jsonb_build_object('feature_set_key',r.feature_set_key,'outcome_set_key',r.outcome_set_key,'source_instrument',t.source_instrument,'feature_key',t.feature_key,'tail',t.slice_key,'threshold',t.metadata->'threshold','target_instrument',t.target_instrument,'horizon_seconds',t.horizon_seconds,'trade_direction',t.metadata->'trade_direction','round_trip_cost_bps',t.metadata->'round_trip_cost_bps','threshold_learning_period',jsonb_build_array(r.discovery_start,r.discovery_end),'validation_period',jsonb_build_array(r.validation_start,r.validation_end),'holdout_accessed',false),
           jsonb_build_object('discovery',t.metadata->'discovery','validation',t.metadata->'validation','q_value',t.q_value,'effect_size',t.effect_size),
           case when t.q_value<=v_fdr/10.0 then 'Strong' else 'Candidate' end,
           case when r.holdout_start is not null and r.holdout_end is not null then 'Run explicit sealed-holdout evaluation without changing the frozen definition.' else 'Define an untouched holdout before further promotion.' end,now()
    from research_hub.experiment_tests t where t.run_id=p_run_id and t.q_value<=v_fdr and t.mean_net>0 and t.validation_positive is true and t.adjacent_horizon_positive is true
    on conflict(candidate_id) do update set status=excluded.status,frozen_definition=excluded.frozen_definition,metrics=excluded.metrics,confidence=excluded.confidence,next_test=excluded.next_test,frozen_at=excluded.frozen_at,updated_at=now();

    select count(*) into v_tests from research_hub.experiment_tests where run_id=p_run_id;
    select count(*) into v_candidates from research_hub.candidate_ledger where run_id=p_run_id;
    update research_hub.experiment_runs set status='validation_complete_candidates_frozen',search_space_tests=v_tests,completed_at=now(),updated_at=now(),config=config||jsonb_build_object('holdout_accessed',false,'engine','research_hub_univariate_tail_v1') where run_id=p_run_id;
    return jsonb_build_object('run_id',p_run_id,'tests',v_tests,'candidates',v_candidates,'holdout_accessed',false);
exception when others then
    update research_hub.experiment_runs set status='failed',updated_at=now(),config=config||jsonb_build_object('last_error',sqlerrm) where run_id=p_run_id;
    return jsonb_build_object('run_id',p_run_id,'status','failed','error',sqlerrm);
end $$;

create or replace function research_hub.evaluate_frozen_holdout(p_run_id uuid)
returns jsonb language plpgsql security invoker set search_path=research_hub,pg_temp as $$
declare r research_hub.experiment_runs%rowtype; c record; h record; v_count integer:=0; v_cost double precision; v_threshold double precision; v_direction integer;
begin
    select * into r from research_hub.experiment_runs where run_id=p_run_id for update;
    if not found then raise exception 'Unknown experiment run %',p_run_id; end if;
    if r.holdout_start is null or r.holdout_end is null then raise exception 'Run % has no sealed holdout window',r.run_key; end if;
    if r.status not in('validation_complete_candidates_frozen','holdout_complete') then raise exception 'Run % is not frozen; status=%',r.run_key,r.status; end if;
    for c in select * from research_hub.candidate_ledger where run_id=p_run_id loop
        v_cost:=coalesce((c.frozen_definition->>'round_trip_cost_bps')::double precision,0)/10000.0;
        v_threshold:=(c.frozen_definition->>'threshold')::double precision; v_direction:=(c.frozen_definition->>'trade_direction')::integer;
        select count(*)::bigint,avg(v_direction*o.gross_return-v_cost),percentile_cont(0.5) within group(order by v_direction*o.gross_return-v_cost),
               avg(((v_direction*o.gross_return-v_cost)>0)::integer::double precision),min(v_direction*o.gross_return-v_cost),
               avg(v_direction*o.gross_return-v_cost) filter(where (v_direction*o.gross_return-v_cost)>0)
        into h
        from research_hub.feature_rows fr join research_hub.outcome_rows o on o.outcome_set_key=(c.frozen_definition->>'outcome_set_key') and o.instrument_key=(c.frozen_definition->>'target_instrument') and o.decision_ts=fr.decision_ts and o.horizon_seconds=(c.frozen_definition->>'horizon_seconds')::integer and o.gross_return is not null
        where fr.feature_set_key=(c.frozen_definition->>'feature_set_key') and fr.instrument_key=(c.frozen_definition->>'source_instrument') and fr.decision_ts>=r.holdout_start and fr.decision_ts<r.holdout_end
          and jsonb_typeof(fr.features->(c.frozen_definition->>'feature_key'))='number'
          and (((c.frozen_definition->>'tail')='LOW' and (fr.features->>(c.frozen_definition->>'feature_key'))::double precision<=v_threshold) or ((c.frozen_definition->>'tail')='HIGH' and (fr.features->>(c.frozen_definition->>'feature_key'))::double precision>=v_threshold));
        update research_hub.candidate_ledger set metrics=metrics||jsonb_build_object('holdout',jsonb_build_object('n',h.count,'mean_net',h.avg,'median_net',h.percentile_cont,'hit_rate_net',h.avg_1,'worst_net',h.min,'avg_winner_net',h.avg_2)),status=case when coalesce(h.avg,-1e100)>0 then 'HOLDOUT_POSITIVE' else 'HOLDOUT_FAILED' end,updated_at=now() where candidate_id=c.candidate_id;
        v_count:=v_count+1;
    end loop;
    update research_hub.experiment_runs set status='holdout_complete',completed_at=now(),updated_at=now(),config=config||jsonb_build_object('holdout_accessed',true) where run_id=p_run_id;
    return jsonb_build_object('run_id',p_run_id,'candidates_evaluated',v_count,'holdout_accessed',true);
end $$;

comment on function research_hub.run_univariate_tail_screen(uuid) is 'Learns thresholds and direction in discovery, applies unchanged rules in validation, freezes candidates, and never reads holdout.';
comment on function research_hub.evaluate_frozen_holdout(uuid) is 'Explicit one-way holdout evaluator for already frozen Research Hub candidates.';
