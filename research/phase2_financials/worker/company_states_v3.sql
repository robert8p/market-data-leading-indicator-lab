create index if not exists financial_metrics_company_cutoff_v3_idx on research_eq10_fundamental_pit_20261020.financial_metrics_v1(entity_cik,available_at,metric,period_end);
create table research_eq10_fundamental_pit_20261020.financial_company_states_v3(
 entity_cik text not null,asof_at timestamptz not null,calculation_version text not null,
 input_sha256 text not null,payload jsonb not null,computed_at timestamptz not null default clock_timestamp(),
 primary key(entity_cik,asof_at,calculation_version,input_sha256)
);
alter table research_eq10_fundamental_pit_20261020.financial_company_states_v3 enable row level security;
revoke all on research_eq10_fundamental_pit_20261020.financial_company_states_v3 from public,anon,authenticated;

create function research_eq10_fundamental_pit_20261020.financial_calculate_state_v3(p_cik text,p_cutoff timestamptz)
returns jsonb language sql stable security invoker set search_path=pg_catalog,research_eq10_fundamental_pit_20261020 as $fn$
with raw as materialized(
 select *,dense_rank()over(partition by metric,unit,period_start,period_end order by available_at desc) version_rank
 from research_eq10_fundamental_pit_20261020.financial_metrics_v1
 where entity_cik=lpad(p_cik,10,'0') and available_at<=p_cutoff and period_end<=(p_cutoff at time zone 'America/New_York')::date
), m as materialized(
 select metric,unit,period_start,period_end,period_kind,min(value) value,max(available_at) available_at,
 jsonb_agg(metric_id order by metric_id) inputs
 from raw where version_rank=1 group by 1,2,3,4,5 having count(distinct value)=1
), flows as materialized(
 select * from m where metric in('revenue','net_income','operating_income','gross_profit','operating_cash_flow','capex','free_cash_flow','cost_of_revenue')
), q0 as(
 select metric,unit,period_start,period_end,value,available_at,inputs,1 priority,'reported original quarter'::text method from flows where period_kind='QUARTER'
 union all
 select a.metric,a.unit,b.period_end+1,a.period_end,a.value-b.value,greatest(a.available_at,b.available_at),a.inputs||b.inputs,2,
 'current cumulative value minus earlier original cumulative value; may include cumulative revisions'::text
 from flows a join flows b on b.metric=a.metric and b.unit=a.unit and b.period_start=a.period_start
 and a.period_end-b.period_end between 70 and 110
 and ((a.period_kind='HALF_YEAR_YTD' and b.period_kind='QUARTER') or(a.period_kind='NINE_MONTH_YTD' and b.period_kind='HALF_YEAR_YTD') or(a.period_kind='ANNUAL' and b.period_kind='NINE_MONTH_YTD'))
), qr as(
 select *,dense_rank()over(partition by metric,unit,period_end order by priority,available_at desc) preference from q0
), q as materialized(
 select metric,unit,min(period_start) period_start,period_end,min(value) value,max(available_at) available_at,
 jsonb_agg(inputs) inputs,string_agg(distinct method,'; ') method
 from qr where preference=1 group by 1,2,4 having count(distinct(value,period_start))=1
), t0 as(
 select a.metric,a.unit,d.period_start,a.period_end,a.value+b.value+c.value+d.value value,
 greatest(a.available_at,b.available_at,c.available_at,d.available_at) available_at,
 jsonb_build_array(a.inputs,b.inputs,c.inputs,d.inputs) inputs,
 'sum of four contiguous quarters; implied quarters may contain cumulative revisions'::text method,2 priority
 from q a join q b on b.metric=a.metric and b.unit=a.unit and b.period_end=a.period_start-1
 join q c on c.metric=a.metric and c.unit=a.unit and c.period_end=b.period_start-1
 join q d on d.metric=a.metric and d.unit=a.unit and d.period_end=c.period_start-1
 where a.period_end-d.period_start+1 in(364,365,366,371)
 union all
 select metric,unit,period_start,period_end,value,available_at,inputs,'reported annual duration; TTM at that fiscal year end',1 from flows where period_kind='ANNUAL' and period_end-period_start+1 in(364,365,366,371)
), t as materialized(
 select distinct on(metric,unit) * from t0 order by metric,unit,period_end desc,priority,available_at desc
), yoy as materialized(
 select a.metric,a.unit,a.period_start,a.period_end,a.period_kind,a.value current_value,b.value prior_value,
 (a.value-b.value)/nullif(abs(b.value),0) value,greatest(a.available_at,b.available_at) available_at,a.inputs||b.inputs inputs
 from m a join lateral(select b.* from m b where b.metric=a.metric and b.unit=a.unit and b.period_kind=a.period_kind
 and a.period_end-b.period_end between 350 and 380
 and abs((a.period_end-a.period_start)-(b.period_end-b.period_start))<=14
 order by b.period_end desc,b.available_at desc limit 1)b on true
 where a.period_kind in('QUARTER','ANNUAL','HALF_YEAR_YTD','NINE_MONTH_YTD')
), latest_m as(
 select distinct on(metric,unit,period_kind) * from m order by metric,unit,period_kind,period_end desc,available_at desc
), latest_yoy as(
 select distinct on(metric,unit,period_kind) * from yoy order by metric,unit,period_kind,period_end desc,available_at desc
), features as(
 select metric feature,value,unit,period_start,period_end,period_kind,available_at,inputs,'existing normalized metric; input metric IDs retain the exact original or derived formula'::text formula from latest_m
 union all
 select 'ttm_'||metric,value,unit,period_start,period_end,'TTM',available_at,inputs,method from t
 union all
 select metric||'_yoy_'||lower(period_kind),value,'ratio',period_start,period_end,period_kind,available_at,inputs,'(current - comparable prior year) / abs(prior year); undefined at zero base' from latest_yoy where value is not null
 union all
 select a.metric||'_yoy_acceleration_quarter',a.value-b.value,'ratio_change',a.period_start,a.period_end,'QUARTER',greatest(a.available_at,b.available_at),jsonb_build_array(a.inputs,b.inputs),'current quarter YoY rate minus previous contiguous quarter YoY rate'
 from latest_yoy a join yoy b on b.metric=a.metric and b.unit=a.unit and b.period_kind='QUARTER' and b.period_end=a.period_start-1
 where a.period_kind='QUARTER' and a.value is not null and b.value is not null
 union all
 select metric||'_negative_to_positive_yoy_'||lower(period_kind),case when prior_value<0 and current_value>0 then 1 else 0 end,'binary',period_start,period_end,period_kind,available_at,inputs,'prior comparable year negative and current value positive'
 from latest_yoy where metric in('net_income','operating_income','operating_cash_flow','free_cash_flow')
 union all
 select 'shares_outstanding_change',a.value-b.value,'shares',b.period_end,a.period_end,'INSTANT_SEQUENCE',greatest(a.available_at,b.available_at),a.inputs||b.inputs,'latest cover shares minus prior cover shares; class continuity remains conditional'
 from latest_m a join lateral(select b.* from m b where b.metric='shares_outstanding' and b.unit=a.unit and b.period_end<a.period_end order by b.period_end desc,b.available_at desc limit 1)b on true where a.metric='shares_outstanding'
 union all
 select 'shares_outstanding_change_ratio',(a.value-b.value)/b.value,'ratio',b.period_end,a.period_end,'INSTANT_SEQUENCE',greatest(a.available_at,b.available_at),a.inputs||b.inputs,'(latest cover shares - prior cover shares) / prior cover shares; class continuity conditional'
 from latest_m a join lateral(select b.* from m b where b.metric='shares_outstanding' and b.unit=a.unit and b.period_end<a.period_end order by b.period_end desc,b.available_at desc limit 1)b on true where a.metric='shares_outstanding' and b.value>0
 union all
 select 'ttm_'||p.metric,a.value/b.value,'ratio',a.period_start,a.period_end,'TTM',greatest(a.available_at,b.available_at),jsonb_build_array(a.inputs,b.inputs),p.a||' TTM / revenue TTM'
 from(values('gross_margin','gross_profit'),('operating_margin','operating_income'),('net_margin','net_income'))p(metric,a)
 join t a on a.metric=p.a join t b on b.metric='revenue' and b.unit=a.unit and b.period_start=a.period_start and b.period_end=a.period_end where b.value>0
)
select jsonb_build_object('entity_cik',lpad(p_cik,10,'0'),'asof_at',p_cutoff,'calculation_version','financial_state_v3.0.0','classification','CONDITIONAL',
 'input_metric_count',(select count(*) from raw),'input_sha256',(select encode(sha256(convert_to(coalesce(string_agg(metric_id||':'||value::text,'|' order by metric_id),''),'UTF8')),'hex') from raw),
 'conflicted_metric_periods',(select count(*) from(select metric,unit,period_start,period_end from raw where version_rank=1 group by 1,2,3,4 having count(distinct value)>1)x),
 'features',coalesce((select jsonb_agg(to_jsonb(f)||jsonb_build_object('classification','CONDITIONAL','period_age_days',(p_cutoff at time zone 'America/New_York')::date-f.period_end) order by feature,period_kind,unit) from features f),'[]'::jsonb),
 'missingness',jsonb_build_object('zero_base_yoy',(select count(*) from latest_yoy where value is null),'ttm_flow_metrics',(select count(*) from t),'expected_supported_flow_metrics',8),
 'safeguards',jsonb_build_object('all_inputs_available_by_cutoff',true,'later_comparatives_backdated',false,'amendments_backdated',false,'forecasts_used',false,'quarter_difference_revision_risk','CONDITIONAL','historical_receipt_certified',false,'share_changes_split_adjusted',false));
$fn$;
revoke all on function research_eq10_fundamental_pit_20261020.financial_calculate_state_v3(text,timestamptz) from public,anon,authenticated;

create function research_eq10_fundamental_pit_20261020.financial_build_company_states_v3(p_cik text,p_after timestamptz,p_limit int default 10)
returns jsonb language plpgsql security invoker set search_path=pg_catalog,research_eq10_fundamental_pit_20261020 as $fn$
declare cutoff timestamptz;state jsonb;n int:=0;last_cutoff timestamptz:=p_after;remaining boolean;
begin
 if p_limit not between 1 and 10 then raise exception 'INVALID_STATE_BATCH_BOUND';end if;
 for cutoff in
 with cutoffs as(select distinct available_at as at from research_eq10_fundamental_pit_20261020.financial_metrics_v1 where entity_cik=lpad(p_cik,10,'0') and available_at between '2025-08-31T16:00:00Z' and '2026-08-01T03:59:59.999999Z' union select '2025-08-31T16:00:00Z'::timestamptz)
 select at from cutoffs where p_after is null or at>p_after order by at limit p_limit
 loop
  state:=research_eq10_fundamental_pit_20261020.financial_calculate_state_v3(p_cik,cutoff);
  insert into research_eq10_fundamental_pit_20261020.financial_company_states_v3(entity_cik,asof_at,calculation_version,input_sha256,payload) values(lpad(p_cik,10,'0'),cutoff,state->>'calculation_version',state->>'input_sha256',state) on conflict do nothing;
  n:=n+1;last_cutoff:=cutoff;
 end loop;
 select exists(select 1 from research_eq10_fundamental_pit_20261020.financial_metrics_v1 where entity_cik=lpad(p_cik,10,'0') and available_at>last_cutoff and available_at<='2026-08-01T03:59:59.999999Z') into remaining;
 return jsonb_build_object('states_checked',n,'last_cutoff',last_cutoff,'remaining',remaining,'entity_cik',lpad(p_cik,10,'0'),'classification','CONDITIONAL');
end $fn$;
revoke all on function research_eq10_fundamental_pit_20261020.financial_build_company_states_v3(text,timestamptz,int) from public,anon,authenticated;

create function research_eq10_fundamental_pit_20261020.financial_company_state_asof_v3(p_cik text,p_cutoff timestamptz)
returns jsonb language sql stable security invoker set search_path=pg_catalog,research_eq10_fundamental_pit_20261020 as $fn$
select payload||jsonb_build_object('requested_cutoff',p_cutoff,'state_age_seconds',extract(epoch from p_cutoff-asof_at),'research_computed_at',computed_at)
from research_eq10_fundamental_pit_20261020.financial_company_states_v3
where entity_cik=lpad(p_cik,10,'0') and asof_at<=p_cutoff
order by asof_at desc,computed_at desc,input_sha256 desc limit 1;
$fn$;
revoke all on function research_eq10_fundamental_pit_20261020.financial_company_state_asof_v3(text,timestamptz) from public,anon,authenticated;
