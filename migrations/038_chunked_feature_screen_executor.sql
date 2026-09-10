-- Resumable feature-by-feature screen. Each feature commits independently;
-- global BH-FDR and candidate freezing occur only after all tasks complete.

create or replace function research_hub.plan_feature_screen_tasks(p_run_id uuid)
returns bigint language plpgsql set search_path=research_hub,pg_temp as $$
declare r research_hub.experiment_runs%rowtype; inserted_count bigint;
begin
 select * into r from research_hub.experiment_runs where run_id=p_run_id;
 if not found then raise exception 'Unknown experiment run %',p_run_id; end if;
 if r.feature_set_key is null then raise exception 'Run % has no feature set',r.run_key; end if;
 if exists(select 1 from research_hub.feature_sets fs where fs.feature_set_key=r.feature_set_key and fs.point_in_time_verified is distinct from true) then
   raise exception 'Feature set % is not point-in-time verified',r.feature_set_key;
 end if;
 insert into research_hub.experiment_tasks(run_id,task_key,task_type,payload)
 select p_run_id,'feature:'||f.feature_key,'feature_screen',
   jsonb_build_object('feature_key',f.feature_key,'feature_set_key',r.feature_set_key,'outcome_set_key',r.outcome_set_key,
     'discovery_start',r.discovery_start,'discovery_end',r.discovery_end,'validation_start',r.validation_start,'validation_end',r.validation_end,
     'tail_quantiles',coalesce(r.config->'tail_quantiles','[0.02,0.05,0.10,0.20]'::jsonb),
     'round_trip_cost_bps',coalesce(r.config->'round_trip_cost_bps','0'::jsonb),'holdout_accessed',false)
 from research_hub.feature_sets fs cross join lateral unnest(fs.feature_keys) f(feature_key)
 where fs.feature_set_key=r.feature_set_key
 on conflict(run_id,task_key) do nothing;
 get diagnostics inserted_count=row_count;
 update research_hub.experiment_runs
 set status=case when status='planned' then 'tasks_planned' else status end,
     config=config||jsonb_build_object('execution_mode','chunked_tasks','holdout_accessed',false),updated_at=now()
 where run_id=p_run_id;
 return inserted_count;
end $$;

create or replace function research_hub.run_feature_screen_task(p_task_id bigint,p_worker_id text)
returns jsonb language plpgsql set search_path=research_hub,pg_temp as $$
declare
 v_task research_hub.experiment_tasks%rowtype; v_run research_hub.experiment_runs%rowtype;
 v_feature text; v_cost double precision; v_min_events integer; v_min_validation integer;
 v_min_hit double precision; v_max_wlr double precision; v_tests bigint;
begin
 select * into v_task from research_hub.experiment_tasks where task_id=p_task_id for update;
 if not found then raise exception 'Unknown experiment task %',p_task_id; end if;
 if v_task.status='completed' then return jsonb_build_object('task_id',p_task_id,'status','already_completed'); end if;
 if v_task.status in ('queued','failed') then
   update research_hub.experiment_tasks set status='running',claimed_by=p_worker_id,attempts=attempts+1,
     started_at=coalesce(started_at,now()),heartbeat_at=now(),completed_at=null,last_error=null,updated_at=now() where task_id=p_task_id;
   v_task.status:='running'; v_task.claimed_by:=p_worker_id;
 end if;
 if v_task.status<>'running' or v_task.claimed_by is distinct from p_worker_id then raise exception 'Task % is not claimed by worker %',p_task_id,p_worker_id; end if;
 select * into v_run from research_hub.experiment_runs where run_id=v_task.run_id;
 if not found then raise exception 'Missing experiment run for task %',p_task_id; end if;
 v_feature:=v_task.payload->>'feature_key';
 if v_feature is null then raise exception 'Task % has no feature_key',p_task_id; end if;
 v_cost:=coalesce((v_run.config->>'round_trip_cost_bps')::double precision,0)/10000.0;
 v_min_events:=coalesce((v_run.config->>'minimum_discovery_events')::integer,100);
 v_min_validation:=coalesce((v_run.config->>'minimum_validation_events')::integer,greatest(30,v_min_events/3));
 v_min_hit:=coalesce((v_run.config->>'minimum_hit_rate')::double precision,0.0);
 v_max_wlr:=case when v_run.config ? 'maximum_worst_loss_ratio' then (v_run.config->>'maximum_worst_loss_ratio')::double precision else null end;
 delete from research_hub.experiment_tests et where et.run_id=v_task.run_id and et.feature_key=v_feature;

 create temporary table tmp_rh_task_metrics(
  phase text not null,source_instrument text not null,tail text not null,tail_q double precision not null,threshold double precision not null,
  target_instrument text not null,horizon_seconds integer not null,trade_direction integer not null,n bigint not null,
  mean_gross double precision,mean_net double precision,median_net double precision,hit_rate_net double precision,profit_factor_net double precision,
  worst_net double precision,avg_winner_net double precision,avg_loser_net double precision,worst_loss_ratio double precision,sd_net double precision,
  primary key(phase,source_instrument,tail,tail_q,target_instrument,horizon_seconds)
 ) on commit drop;

 insert into tmp_rh_task_metrics
 with base as(
   select fr.instrument_key source_instrument,nullif(fr.quality->>'legacy_run_id','') scope_key,fr.decision_ts,
     (fr.features->>v_feature)::double precision feature_value,
     case when fr.decision_ts>=v_run.discovery_start and fr.decision_ts<v_run.discovery_end then 'discovery'
          when fr.decision_ts>=v_run.validation_start and fr.decision_ts<v_run.validation_end then 'validation' end phase
   from research_hub.feature_rows fr
   where fr.feature_set_key=v_run.feature_set_key and fr.features ? v_feature
     and fr.decision_ts>=v_run.discovery_start and fr.decision_ts<v_run.validation_end
 ), quantiles as(
   select distinct x::double precision tail_q
   from jsonb_array_elements_text(coalesce(v_run.config->'tail_quantiles','[0.02,0.05,0.10,0.20]'::jsonb)) q(x)
 ), thresholds as(
   select b.source_instrument,b.scope_key,q.tail_q,
     percentile_cont(q.tail_q) within group(order by b.feature_value) low_cut,
     percentile_cont(1.0-q.tail_q) within group(order by b.feature_value) high_cut
   from base b cross join quantiles q where b.phase='discovery'
   group by b.source_instrument,b.scope_key,q.tail_q
 ), events as(
   select b.source_instrument,b.scope_key,b.decision_ts,b.phase,x.tail,th.tail_q,x.threshold
   from base b join thresholds th on th.source_instrument=b.source_instrument and th.scope_key is not distinct from b.scope_key
   cross join lateral(values('LOW'::text,th.low_cut),('HIGH'::text,th.high_cut)) x(tail,threshold)
   where b.phase is not null and ((x.tail='LOW' and b.feature_value<=x.threshold) or (x.tail='HIGH' and b.feature_value>=x.threshold))
 ), event_outcomes as(
   select e.source_instrument,e.scope_key,e.decision_ts,e.phase,e.tail,e.tail_q,e.threshold,
     o.instrument_key target_instrument,o.horizon_seconds,o.gross_return
   from events e join research_hub.outcome_rows o
     on o.outcome_set_key=v_run.outcome_set_key and o.decision_ts=e.decision_ts and o.gross_return is not null
    and (e.scope_key is null or coalesce(o.metadata->>'legacy_run_id','')=e.scope_key)
 ), directions as(
   select eo.source_instrument,eo.tail,eo.tail_q,eo.threshold,eo.target_instrument,eo.horizon_seconds,
     case when avg(eo.gross_return)>=0 then 1 else -1 end trade_direction,count(*) discovery_n
   from event_outcomes eo where eo.phase='discovery'
   group by eo.source_instrument,eo.tail,eo.tail_q,eo.threshold,eo.target_instrument,eo.horizon_seconds having count(*)>=v_min_events
 ), scored as(
   select eo.phase,eo.source_instrument,eo.tail,eo.tail_q,dir.threshold,eo.target_instrument,eo.horizon_seconds,dir.trade_direction,
     dir.trade_direction*eo.gross_return directed_gross,dir.trade_direction*eo.gross_return-v_cost net_return
   from event_outcomes eo join directions dir
     on dir.source_instrument=eo.source_instrument and dir.tail=eo.tail and dir.tail_q=eo.tail_q
    and dir.target_instrument=eo.target_instrument and dir.horizon_seconds=eo.horizon_seconds
 )
 select sc.phase,sc.source_instrument,sc.tail,sc.tail_q,sc.threshold,sc.target_instrument,sc.horizon_seconds,sc.trade_direction,count(*)::bigint,
   avg(sc.directed_gross),avg(sc.net_return),percentile_cont(0.5) within group(order by sc.net_return),avg((sc.net_return>0)::integer::double precision),
   case when abs(sum(sc.net_return) filter(where sc.net_return<0))>0 then sum(sc.net_return) filter(where sc.net_return>0)/abs(sum(sc.net_return) filter(where sc.net_return<0)) end,
   min(sc.net_return),avg(sc.net_return) filter(where sc.net_return>0),avg(sc.net_return) filter(where sc.net_return<0),
   case when min(sc.net_return)>=0 then 0.0 when (avg(sc.net_return) filter(where sc.net_return>0))>0 then abs(min(sc.net_return))/(avg(sc.net_return) filter(where sc.net_return>0)) end,
   stddev_samp(sc.net_return)
 from scored sc group by sc.phase,sc.source_instrument,sc.tail,sc.tail_q,sc.threshold,sc.target_instrument,sc.horizon_seconds,sc.trade_direction;

 insert into research_hub.experiment_tests
 (run_id,feature_key,outcome_key,source_instrument,target_instrument,slice_key,horizon_seconds,n,mean_gross,mean_net,median_net,hit_rate_net,profit_factor_net,worst_net,avg_winner_net,avg_loser_net,worst_loss_ratio,effect_size,validation_positive,validation_n,validation_mean_net,validation_median_net,validation_hit_rate_net,validation_profit_factor_net,validation_worst_net,validation_avg_winner_net,validation_avg_loser_net,validation_worst_loss_ratio,metadata)
 select v_task.run_id,v_feature,'horizon_'||d.horizon_seconds,d.source_instrument,d.target_instrument,
   d.tail||'_Q'||to_char(d.tail_q,'FM0.000'),d.horizon_seconds,d.n,d.mean_gross,d.mean_net,d.median_net,d.hit_rate_net,d.profit_factor_net,
   d.worst_net,d.avg_winner_net,d.avg_loser_net,d.worst_loss_ratio,case when d.sd_net is not null and d.sd_net>0 then d.mean_net/d.sd_net end,
   (coalesce(val.mean_net,-1e100)>0 and coalesce(val.n,0)>=v_min_validation and coalesce(val.hit_rate_net,0)>=v_min_hit
      and (v_max_wlr is null or coalesce(val.worst_loss_ratio,1e100)<=v_max_wlr)),
   val.n,val.mean_net,val.median_net,val.hit_rate_net,val.profit_factor_net,val.worst_net,val.avg_winner_net,val.avg_loser_net,val.worst_loss_ratio,
   jsonb_build_object('engine','research_hub_chunked_feature_v1','task_id',p_task_id,'threshold',d.threshold,'tail_quantile',d.tail_q,
     'trade_direction',d.trade_direction,'round_trip_cost_bps',v_cost*10000.0,
     'discovery',jsonb_build_object('n',d.n,'mean_net',d.mean_net,'median_net',d.median_net,'hit_rate_net',d.hit_rate_net,'profit_factor_net',d.profit_factor_net,'worst_net',d.worst_net,'avg_winner_net',d.avg_winner_net,'avg_loser_net',d.avg_loser_net,'worst_loss_ratio',d.worst_loss_ratio),
     'validation',case when val.n is null then null else jsonb_build_object('n',val.n,'mean_net',val.mean_net,'median_net',val.median_net,'hit_rate_net',val.hit_rate_net,'profit_factor_net',val.profit_factor_net,'worst_net',val.worst_net,'avg_winner_net',val.avg_winner_net,'avg_loser_net',val.avg_loser_net,'worst_loss_ratio',val.worst_loss_ratio) end,
     'promotion_constraints',jsonb_build_object('minimum_hit_rate',v_min_hit,'maximum_worst_loss_ratio',v_max_wlr),'holdout_accessed',false)
 from tmp_rh_task_metrics d left join tmp_rh_task_metrics val
   on val.phase='validation' and val.source_instrument=d.source_instrument and val.tail=d.tail and val.tail_q=d.tail_q
  and val.target_instrument=d.target_instrument and val.horizon_seconds=d.horizon_seconds
 where d.phase='discovery';

 select count(*) into v_tests from research_hub.experiment_tests et where et.run_id=v_task.run_id and et.feature_key=v_feature;
 update research_hub.experiment_tasks set status='completed',result_summary=jsonb_build_object('tests',v_tests,'feature_key',v_feature,'holdout_accessed',false),
   completed_at=now(),heartbeat_at=now(),updated_at=now() where task_id=p_task_id;
 return jsonb_build_object('task_id',p_task_id,'status','completed','feature_key',v_feature,'tests',v_tests,'holdout_accessed',false);
exception when others then
 update research_hub.experiment_tasks set status='failed',last_error=left(sqlerrm,4000),completed_at=now(),updated_at=now() where task_id=p_task_id;
 return jsonb_build_object('task_id',p_task_id,'status','failed','error',sqlerrm,'holdout_accessed',false);
end $$;

create or replace function research_hub.finalize_chunked_screen(p_run_id uuid)
returns jsonb language plpgsql set search_path=research_hub,pg_temp as $$
declare r research_hub.experiment_runs%rowtype; v_fdr double precision; v_tests bigint; v_candidates bigint;
begin
 select * into r from research_hub.experiment_runs where run_id=p_run_id for update;
 if not found then raise exception 'Unknown experiment run %',p_run_id; end if;
 if exists(select 1 from research_hub.experiment_tasks where run_id=p_run_id and status<>'completed') then
   raise exception 'Run % still has incomplete research tasks',r.run_key;
 end if;
 v_fdr:=coalesce((r.config->>'fdr_q')::double precision,0.05);
 with ranked as(
   select test_id,p_value,row_number() over(order by p_value,test_id) rn,count(*) over() m
   from research_hub.experiment_tests where run_id=p_run_id and p_value is not null
 ),raw_q as(select test_id,rn,least(1.0,p_value*m::double precision/rn::double precision) raw_q from ranked),
 adjusted as(select test_id,least(1.0,min(raw_q) over(order by rn desc rows between unbounded preceding and current row)) q_value from raw_q)
 update research_hub.experiment_tests t set q_value=a.q_value from adjusted a where t.test_id=a.test_id;
 with ordered as(
   select test_id,lag(mean_net) over(partition by run_id,source_instrument,target_instrument,feature_key,slice_key order by horizon_seconds) prev_mean,
     lead(mean_net) over(partition by run_id,source_instrument,target_instrument,feature_key,slice_key order by horizon_seconds) next_mean
   from research_hub.experiment_tests where run_id=p_run_id
 ) update research_hub.experiment_tests t set adjacent_horizon_positive=(coalesce(o.prev_mean,0)>0 or coalesce(o.next_mean,0)>0)
   from ordered o where t.test_id=o.test_id;
 delete from research_hub.candidate_ledger where run_id=p_run_id;
 insert into research_hub.candidate_ledger(candidate_id,run_id,status,descriptive_name,frozen_definition,metrics,confidence,next_test,frozen_at)
 select 'RH-'||upper(substr(md5(r.run_key||'|'||t.source_instrument||'|'||t.feature_key||'|'||t.slice_key||'|'||t.target_instrument||'|'||t.horizon_seconds),1,12)),
   p_run_id,'FROZEN_VALIDATION_PASSED',t.source_instrument||' '||t.slice_key||' '||t.feature_key||' -> '||t.target_instrument||' @ '||t.horizon_seconds||'s',
   jsonb_build_object('engine','research_hub_chunked_feature_v1','feature_set_key',r.feature_set_key,'outcome_set_key',r.outcome_set_key,'source_instrument',t.source_instrument,'feature_key',t.feature_key,'tail',split_part(t.slice_key,'_',1),'tail_quantile',t.metadata->'tail_quantile','threshold',t.metadata->'threshold','target_instrument',t.target_instrument,'horizon_seconds',t.horizon_seconds,'trade_direction',t.metadata->'trade_direction','round_trip_cost_bps',t.metadata->'round_trip_cost_bps','threshold_learning_period',jsonb_build_array(r.discovery_start,r.discovery_end),'validation_period',jsonb_build_array(r.validation_start,r.validation_end),'holdout_accessed',false),
   jsonb_build_object('discovery',t.metadata->'discovery','validation',t.metadata->'validation','q_value',t.q_value,'effect_size',t.effect_size),
   case when t.q_value<=v_fdr/10.0 then 'Strong screening result' else 'Screening result' end,
   'Run dependence-aware robustness before any sealed-holdout evaluation.',now()
 from research_hub.experiment_tests t
 where t.run_id=p_run_id and t.q_value is not null and t.q_value<=v_fdr and t.mean_net>0
   and t.validation_positive is true and t.adjacent_horizon_positive is true
 on conflict(candidate_id) do update set status=excluded.status,frozen_definition=excluded.frozen_definition,metrics=excluded.metrics,
   confidence=excluded.confidence,next_test=excluded.next_test,frozen_at=excluded.frozen_at,updated_at=now();
 select count(*) into v_tests from research_hub.experiment_tests where run_id=p_run_id;
 select count(*) into v_candidates from research_hub.candidate_ledger where run_id=p_run_id;
 update research_hub.experiment_runs
 set status='screening_complete_dependence_review_required',search_space_tests=v_tests,completed_at=now(),updated_at=now(),
     config=config||jsonb_build_object('holdout_accessed',false,'engine','research_hub_chunked_feature_v1') where run_id=p_run_id;
 return jsonb_build_object('run_id',p_run_id,'tests',v_tests,'candidates',v_candidates,'holdout_accessed',false,'next_gate','dependence_robustness');
end $$;
