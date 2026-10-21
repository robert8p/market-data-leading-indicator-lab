create or replace function research_eq10_fundamental_pit_20261020.financial_company_state_asof_v3(p_cik text,p_cutoff timestamptz)
returns jsonb language sql stable security invoker set search_path=pg_catalog,research_eq10_fundamental_pit_20261020 as $fn$
with latest as(
select * from research_eq10_fundamental_pit_20261020.financial_company_states_v3 where entity_cik=lpad(p_cik,10,'0') and asof_at<=p_cutoff order by asof_at desc,computed_at desc,input_sha256 desc limit 1
), expected as(
select encode(sha256(convert_to(coalesce(string_agg(metric_id||':'||value::text,'|' order by metric_id),''),'UTF8')),'hex') sha
from research_eq10_fundamental_pit_20261020.financial_metrics_v1 where entity_cik=lpad(p_cik,10,'0') and available_at<=p_cutoff and period_end<=(p_cutoff at time zone 'America/New_York')::date
)
select case when latest.input_sha256=expected.sha then
 payload||jsonb_build_object('requested_cutoff',p_cutoff,'state_age_seconds',extract(epoch from p_cutoff-asof_at),'research_computed_at',computed_at,'retrieval_status','CURRENT_FOR_RETAINED_INPUTS','source_population_complete',false)
else jsonb_build_object('entity_cik',lpad(p_cik,10,'0'),'requested_cutoff',p_cutoff,'classification','EXCLUDED_STATE_REBUILD_REQUIRED','retrieval_status','STATE_REBUILD_REQUIRED','latest_state_asof',asof_at,'persisted_input_sha256',input_sha256,'current_input_sha256',expected.sha,'features','[]'::jsonb,'source_population_complete',false) end
from latest cross join expected;
$fn$;
revoke all on function research_eq10_fundamental_pit_20261020.financial_company_state_asof_v3(text,timestamptz) from public,anon,authenticated;