-- Per-run FDR does not pay for repeatedly mining the same sample after learning
-- from earlier searches. Surface cumulative adaptive reuse and require genuinely
-- unused replication before promotion when data have been repeatedly mined.
create or replace view research_hub.ai_adaptive_search_exposure with (security_invoker=true) as
select feature_set_key,outcome_set_key,discovery_start,discovery_end,validation_start,validation_end,
       count(*) experiment_runs,
       sum(coalesce(search_space_tests,0)) cumulative_recorded_tests,
       count(*) filter(where status not in ('planned','cancelled')) executed_runs,
       min(created_at) first_run_at,max(created_at) latest_run_at,
       case when count(*) filter(where status not in ('planned','cancelled'))>=3 then 'high_adaptive_reuse'
            when count(*) filter(where status not in ('planned','cancelled'))=2 then 'reused'
            else 'single_campaign' end reuse_class,
       (count(*) filter(where status not in ('planned','cancelled'))>=2) independent_replication_required
from research_hub.experiment_runs
group by feature_set_key,outcome_set_key,discovery_start,discovery_end,validation_start,validation_end;

update research_hub.operating_policies
set policy=policy||jsonb_build_object(
 'adaptive_data_reuse','Track repeated mining of the same feature/outcome/split across campaigns. If the same discovery/validation data are adaptively reused in two or more executed campaigns, promotion requires genuinely unused temporal or external replication even when per-run FDR and validation pass.',
 'researcher_overfitting_control','Treat prior chat findings, failed searches and human/model insight as information leakage into subsequent campaigns on the same sample; record cumulative search exposure and do not relabel recycled data as untouched.'
),updated_at=now()
where policy_key='market_edge_core_v1';
