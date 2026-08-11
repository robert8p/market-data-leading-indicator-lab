-- Conservative control-plane readiness view for automated research.
-- A dataset is eligible only when it is non-empty, explicitly point-in-time safe,
-- has no open warning/critical DQ issues, and any external adapter is ready.

create or replace view research_hub.dataset_readiness with (security_invoker=true) as
with issue_counts as (
    select dataset_key,
           count(*) filter (where resolved_at is null) as open_issues,
           count(*) filter (where resolved_at is null and severity='critical') as critical_issues,
           count(*) filter (where resolved_at is null and severity='warning') as warning_issues,
           count(*) filter (where resolved_at is null and severity='info') as info_issues,
           array_agg(distinct issue_type order by issue_type) filter (where resolved_at is null) as open_issue_types
    from research_hub.data_quality_issues
    group by dataset_key
)
select d.*,
       coalesce(i.open_issues,0)::bigint as open_issues,
       coalesce(i.critical_issues,0)::bigint as critical_issues,
       coalesce(i.warning_issues,0)::bigint as warning_issues,
       coalesce(i.info_issues,0)::bigint as info_issues,
       coalesce(i.open_issue_types,array[]::text[]) as open_issue_types,
       s.status as sync_status,
       s.last_source_ts,
       s.last_row_count as checkpoint_row_count,
       s.last_error as sync_error,
       case
         when coalesce(d.row_estimate,0)<=0 then 'unavailable_empty'
         when coalesce(i.critical_issues,0)>0 then 'blocked_quality'
         when d.point_in_time_safe is false then 'blocked_point_in_time'
         when d.point_in_time_safe is null then 'point_in_time_review'
         when coalesce(i.warning_issues,0)>0 then 'quality_warning'
         when d.store_key<>'market_data_primary' and coalesce(s.status,'') not in ('ready','synced','active') then 'adapter_required'
         when d.status ilike '%control%' or d.status ilike '%reference%' then 'control_or_reference'
         else 'research_ready'
       end as readiness_status,
       case
         when coalesce(d.row_estimate,0)<=0 then false
         when coalesce(i.critical_issues,0)>0 then false
         when d.point_in_time_safe is distinct from true then false
         when coalesce(i.warning_issues,0)>0 then false
         when d.store_key<>'market_data_primary' and coalesce(s.status,'') not in ('ready','synced','active') then false
         else true
       end as eligible_for_automated_predictor_search
from research_hub.dataset_inventory d
left join issue_counts i using(dataset_key)
left join research_hub.sync_checkpoints s using(dataset_key);

comment on view research_hub.dataset_readiness is
'Control-plane readiness view combining non-empty availability, point-in-time safety, open data-quality issues, and federation/sync state. eligible_for_automated_predictor_search is deliberately conservative.';
