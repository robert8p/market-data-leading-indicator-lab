create or replace function research_hub.evaluate_frozen_holdout(p_run_id uuid)
returns jsonb
language plpgsql
security invoker
set search_path=research_hub,pg_temp
as $$
declare
    r research_hub.experiment_runs%rowtype;
    c record;
    h record;
    v_count integer:=0;
    v_cost double precision;
    v_threshold double precision;
    v_direction integer;
begin
    select * into r from research_hub.experiment_runs where run_id=p_run_id for update;
    if not found then raise exception 'Unknown experiment run %',p_run_id; end if;
    if r.holdout_start is null or r.holdout_end is null then raise exception 'Run % has no sealed holdout window',r.run_key; end if;
    if r.status not in('validation_complete_candidates_frozen','holdout_complete') then raise exception 'Run % is not in a frozen state; status=%',r.run_key,r.status; end if;

    for c in select * from research_hub.candidate_ledger where run_id=p_run_id loop
        v_cost:=coalesce((c.frozen_definition->>'round_trip_cost_bps')::double precision,0)/10000.0;
        v_threshold:=(c.frozen_definition->>'threshold')::double precision;
        v_direction:=(c.frozen_definition->>'trade_direction')::integer;

        select count(*)::bigint as n,
               avg(v_direction*o.gross_return-v_cost) as mean_net,
               percentile_cont(0.5) within group(order by v_direction*o.gross_return-v_cost) as median_net,
               avg(((v_direction*o.gross_return-v_cost)>0)::integer::double precision) as hit_rate_net,
               case when abs(sum(v_direction*o.gross_return-v_cost) filter(where (v_direction*o.gross_return-v_cost)<0))>0
                    then sum(v_direction*o.gross_return-v_cost) filter(where (v_direction*o.gross_return-v_cost)>0)
                         /abs(sum(v_direction*o.gross_return-v_cost) filter(where (v_direction*o.gross_return-v_cost)<0)) end as profit_factor_net,
               min(v_direction*o.gross_return-v_cost) as worst_net,
               avg(v_direction*o.gross_return-v_cost) filter(where (v_direction*o.gross_return-v_cost)>0) as avg_winner_net
        into h
        from research_hub.feature_rows fr
        join research_hub.outcome_rows o
          on o.outcome_set_key=(c.frozen_definition->>'outcome_set_key')
         and o.instrument_key=(c.frozen_definition->>'target_instrument')
         and o.decision_ts=fr.decision_ts
         and o.horizon_seconds=(c.frozen_definition->>'horizon_seconds')::integer
         and o.gross_return is not null
         and (nullif(fr.quality->>'legacy_run_id','') is null or coalesce(o.metadata->>'legacy_run_id','')=fr.quality->>'legacy_run_id')
        where fr.feature_set_key=(c.frozen_definition->>'feature_set_key')
          and fr.instrument_key=(c.frozen_definition->>'source_instrument')
          and fr.decision_ts>=r.holdout_start and fr.decision_ts<r.holdout_end
          and jsonb_typeof(fr.features->(c.frozen_definition->>'feature_key'))='number'
          and (((c.frozen_definition->>'tail')='LOW' and (fr.features->>(c.frozen_definition->>'feature_key'))::double precision<=v_threshold)
            or ((c.frozen_definition->>'tail')='HIGH' and (fr.features->>(c.frozen_definition->>'feature_key'))::double precision>=v_threshold));

        update research_hub.candidate_ledger
        set metrics=metrics||jsonb_build_object('holdout',jsonb_build_object(
                'n',h.n,'mean_net',h.mean_net,'median_net',h.median_net,'hit_rate_net',h.hit_rate_net,
                'profit_factor_net',h.profit_factor_net,'worst_net',h.worst_net,'avg_winner_net',h.avg_winner_net,
                'worst_loss_ratio',case when h.avg_winner_net is not null and h.avg_winner_net>0 then abs(least(coalesce(h.worst_net,0),0))/h.avg_winner_net end)),
            status=case when coalesce(h.mean_net,-1e100)>0 then 'HOLDOUT_POSITIVE' else 'HOLDOUT_FAILED' end,
            next_test=case when coalesce(h.mean_net,-1e100)>0 then 'Proceed to execution-realism and robustness audit without retuning.' else 'Reject frozen candidate; do not retune on holdout.' end,
            updated_at=now()
        where candidate_id=c.candidate_id;

        update research_hub.experiment_tests
        set holdout_positive=(coalesce(h.mean_net,-1e100)>0),
            metadata=metadata||jsonb_build_object('holdout',jsonb_build_object('n',h.n,'mean_net',h.mean_net,'median_net',h.median_net,'hit_rate_net',h.hit_rate_net,'profit_factor_net',h.profit_factor_net,'worst_net',h.worst_net),'holdout_accessed',true)
        where run_id=p_run_id
          and source_instrument=(c.frozen_definition->>'source_instrument')
          and feature_key=(c.frozen_definition->>'feature_key')
          and slice_key=(c.frozen_definition->>'tail')
          and target_instrument=(c.frozen_definition->>'target_instrument')
          and horizon_seconds=(c.frozen_definition->>'horizon_seconds')::integer;
        v_count:=v_count+1;
    end loop;

    update research_hub.experiment_runs set status='holdout_complete',completed_at=now(),updated_at=now(),config=config||jsonb_build_object('holdout_accessed',true) where run_id=p_run_id;
    return jsonb_build_object('run_id',p_run_id,'candidates_evaluated',v_count,'holdout_accessed',true);
end $$;
