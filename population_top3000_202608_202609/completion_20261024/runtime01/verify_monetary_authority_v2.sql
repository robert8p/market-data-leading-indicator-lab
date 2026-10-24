-- Read-only verification after controller installs patch and current authority.
WITH baseline AS (
 SELECT to_jsonb(p) AS j FROM population_top3000_202608_202609_v1.pipeline_control p WHERE singleton
), fixtures(name,j,incremental,expected) AS (
 SELECT 'current_explicit_authority',j,0::numeric,true FROM baseline UNION ALL
 SELECT 'no_replacement_monetary_ceiling',j,1000000::numeric,true FROM baseline UNION ALL
 SELECT 'stale_controller_rejected',j||'{"expected_owner_id":"stale-review-fixture"}',0,false FROM baseline UNION ALL
 SELECT 'stale_fence_rejected',j||'{"expected_owner_fence":-1}',0,false FROM baseline UNION ALL
 SELECT 'stale_stop_rejected',j||'{"expected_stop_generation":-1}',0,false FROM baseline UNION ALL
 SELECT 'stale_scope_rejected',j||'{"scope_sha256":"stale-review-fixture"}',0,false FROM baseline UNION ALL
 SELECT 'missing_authority_event_rejected',jsonb_set(j,'{budget_evidence,monetary_policy,authority_event_id}','"0"'),0,false FROM baseline UNION ALL
 SELECT 'malformed_authority_event_rejected',jsonb_set(j,'{budget_evidence,monetary_policy,authority_event_id}','"bad"'),0,false FROM baseline UNION ALL
 SELECT 'negative_increment_rejected',j,-1,false FROM baseline UNION ALL
 SELECT 'infinite_increment_rejected',j,'Infinity'::numeric,false FROM baseline UNION ALL
 SELECT 'nan_increment_rejected',j,'NaN'::numeric,false FROM baseline UNION ALL
 SELECT 'missing_current_policy_mode_rejected',jsonb_set(j,'{budget_evidence,monetary_policy,mode}','"UNKNOWN"'),0,false FROM baseline UNION ALL
 SELECT 'negative_cost_counter_rejected',j||'{"actual_usd":-1}',0,false FROM baseline UNION ALL
 SELECT 'legacy_cap_still_supported',jsonb_set(j,'{budget_evidence}',(j->'budget_evidence')-'monetary_policy')||'{"actual_usd":100,"accrued_usd":0,"unavoidable_usd":0,"remaining_uncommitted_usd":0,"repair_egress_reserve_usd":0,"reserved_request_usd":0,"budget_cap_usd":250}',150,true FROM baseline UNION ALL
 SELECT 'legacy_cap_still_enforced',jsonb_set(j,'{budget_evidence}',(j->'budget_evidence')-'monetary_policy')||'{"actual_usd":100,"accrued_usd":0,"unavoidable_usd":0,"remaining_uncommitted_usd":0,"repair_egress_reserve_usd":0,"reserved_request_usd":0,"budget_cap_usd":250}',151,false FROM baseline
), results AS (
 SELECT name, expected,population_top3000_202608_202609_v1.pipeline_cost_admission_v2(jsonb_populate_record(NULL::population_top3000_202608_202609_v1.pipeline_control,j),incremental) AS actual FROM fixtures
)
SELECT name,expected,actual,actual IS NOT DISTINCT FROM expected AS pass FROM results ORDER BY name;
