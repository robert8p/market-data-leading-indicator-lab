
create table research_eq10_fundamental_pit_20261020.financial_metric_dictionary_v1(metric text not null,concept text primary key,priority integer not null,expected_unit text not null,definition text not null);
insert into research_eq10_fundamental_pit_20261020.financial_metric_dictionary_v1 values
('revenue','us-gaap:RevenueFromContractWithCustomerExcludingAssessedTax',1,'currency','Reported revenue excluding assessed tax'),
('revenue','us-gaap:Revenues',2,'currency','Reported total revenues'),
('revenue','us-gaap:SalesRevenueNet',3,'currency','Net sales revenue'),
('net_income','us-gaap:NetIncomeLoss',1,'currency','Reported net income or loss'),
('operating_income','us-gaap:OperatingIncomeLoss',1,'currency','Reported operating income or loss'),
('gross_profit','us-gaap:GrossProfit',1,'currency','Reported gross profit'),
('cost_of_revenue','us-gaap:CostOfRevenue',1,'currency','Reported cost of revenue'),
('cost_of_revenue','us-gaap:CostOfGoodsAndServicesSold',2,'currency','Reported goods/services cost'),
('eps_basic','us-gaap:EarningsPerShareBasic',1,'currency_per_share','Basic EPS; never a share count'),
('eps_diluted','us-gaap:EarningsPerShareDiluted',1,'currency_per_share','Diluted EPS; never a share count'),
('operating_cash_flow','us-gaap:NetCashProvidedByUsedInOperatingActivities',1,'currency','Operating cash flow for exact reporting duration'),
('capex','us-gaap:PaymentsToAcquirePropertyPlantAndEquipment',1,'currency','Cash payments for property plant and equipment, positive cash-outflow convention'),
('cash','us-gaap:CashAndCashEquivalentsAtCarryingValue',1,'currency','Cash and cash equivalents at instant'),
('assets','us-gaap:Assets',1,'currency','Total assets'),
('liabilities','us-gaap:Liabilities',1,'currency','Total liabilities'),
('equity','us-gaap:StockholdersEquity',1,'currency','Parent stockholders equity'),
('equity_including_noncontrolling','us-gaap:StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest',1,'currency','Equity including noncontrolling interests'),
('current_assets','us-gaap:AssetsCurrent',1,'currency','Current assets'),
('current_liabilities','us-gaap:LiabilitiesCurrent',1,'currency','Current liabilities'),
('debt_current','us-gaap:LongTermDebtCurrent',1,'currency','Current maturities of long-term debt'),
('debt_noncurrent','us-gaap:LongTermDebtNoncurrent',1,'currency','Noncurrent long-term debt'),
('debt_total_reported','us-gaap:LongTermDebt',1,'currency','Reported long-term debt; do not silently combine with overlapping debt components'),
('short_term_borrowings','us-gaap:ShortTermBorrowings',1,'currency','Reported short-term borrowings'),
('shares_outstanding','dei:EntityCommonStockSharesOutstanding',1,'shares','Actual common shares at dated cover-page context; issuer-level unless class dimension proven'),
('common_shares_outstanding','us-gaap:CommonStockSharesOutstanding',1,'shares','Reported common shares at instant'),
('weighted_shares_basic','us-gaap:WeightedAverageNumberOfSharesOutstandingBasic',1,'shares','Weighted-average basic shares over duration; prohibited as market-cap share count'),
('weighted_shares_diluted','us-gaap:WeightedAverageNumberOfDilutedSharesOutstanding',1,'shares','Weighted-average diluted shares over duration; prohibited as market-cap share count');
create table research_eq10_fundamental_pit_20261020.financial_metrics_v1(
 metric_id text primary key,accession_number text not null,file_id text not null,entity_cik text not null,
 metric text not null,value numeric not null,unit text not null,period_start date,period_end date not null,
 period_kind text not null,report_date date not null,available_at timestamptz not null,
 classification text not null check(classification in('CONDITIONAL','CERTIFIED')),
 formula text not null,input_lineage jsonb not null,research_available_at timestamptz not null default clock_timestamp());
create index financial_metrics_v1_asof on research_eq10_fundamental_pit_20261020.financial_metrics_v1(entity_cik,available_at,metric,period_kind);
alter table research_eq10_fundamental_pit_20261020.financial_metric_dictionary_v1 enable row level security;
alter table research_eq10_fundamental_pit_20261020.financial_metrics_v1 enable row level security;
revoke all on research_eq10_fundamental_pit_20261020.financial_metric_dictionary_v1,research_eq10_fundamental_pit_20261020.financial_metrics_v1 from public,anon,authenticated;
create function research_eq10_fundamental_pit_20261020.financial_reconstruct_metrics_v1(p_accession text,p_file text)
returns jsonb language plpgsql security invoker set search_path=pg_catalog,research_eq10_fundamental_pit_20261020 as $fn$
declare n bigint;
begin
with candidates as(
 select f.*,d.metric,d.priority,d.expected_unit,m.report_date,m.availability_bound,
 case when f.period_kind='INSTANT' then 'INSTANT' when f.period_end-f.period_start+1 between 70 and 110 then 'QUARTER'
 when f.period_end-f.period_start+1 between 150 and 210 then 'HALF_YEAR_YTD'
 when f.period_end-f.period_start+1 between 240 and 305 then 'NINE_MONTH_YTD'
 when f.period_end-f.period_start+1 between 330 and 400 then 'ANNUAL' else 'TRANSITION' end normalized_period,
 nullif((xpath('string(//x:measure)',f.unit_xml,ARRAY[ARRAY['x','http://www.xbrl.org/2003/instance']]))[1]::text,'') measure,
 nullif((xpath('string(//x:unitNumerator/x:measure)',f.unit_xml,ARRAY[ARRAY['x','http://www.xbrl.org/2003/instance']]))[1]::text,'') numerator,
 nullif((xpath('string(//x:unitDenominator/x:measure)',f.unit_xml,ARRAY[ARRAY['x','http://www.xbrl.org/2003/instance']]))[1]::text,'') denominator,
 min(d.priority) over(partition by d.metric,f.entity_cik,f.period_start,f.period_end) best_priority
 from research_eq10_fundamental_pit_20261020.financial_fact_v1 f
 join research_eq10_fundamental_pit_20261020.financial_metric_dictionary_v1 d using(concept)
 join research_eq10_fundamental_pit_20261020.financial_filing_metadata_v1 m using(accession_number)
 where f.accession_number=p_accession and f.file_id=p_file and f.extraction_state='PARSED' and f.dimension_count=0
 and m.form_type in('10-K','10-Q') and m.availability_bound is not null
 and (f.period_end=m.report_date or (d.metric='shares_outstanding' and f.period_end between m.report_date and m.filing_date))
 and (f.period_start is null or f.period_start<=f.period_end)
 and exists(select 1 from jsonb_array_elements(m.raw_payload->'entities') e where lpad(e#>>'{company_data,cik}',10,'0')=lpad(f.entity_cik,10,'0'))
), grouped as(
 select lpad(entity_cik,10,'0') cik,metric,period_start,period_end,normalized_period,report_date,availability_bound,
 case when expected_unit='currency_per_share' then split_part(numerator,':',2)||'/shares' when expected_unit='currency' then split_part(measure,':',2) else 'shares' end unit,
 min(value) value,count(distinct value) value_variants,
 jsonb_agg(jsonb_build_object('accession',accession_number,'file_id',file_id,'ordinal',ordinal,'concept',concept,'fact_sha256',fact_sha256,'context_id',context_id,'scale',source_scale)) lineage
 from candidates where priority=best_priority and
 ((expected_unit='currency' and measure ~ '^iso4217:[A-Z]{3}$' and numerator is null)
 or(expected_unit='shares' and measure='xbrli:shares' and numerator is null)
 or(expected_unit='currency_per_share' and numerator ~ '^iso4217:[A-Z]{3}$' and denominator='xbrli:shares'))
 group by 1,2,3,4,5,6,7,8
)
insert into research_eq10_fundamental_pit_20261020.financial_metrics_v1(metric_id,accession_number,file_id,entity_cik,metric,value,unit,period_start,period_end,period_kind,report_date,available_at,classification,formula,input_lineage)
select encode(sha256(convert_to(concat_ws('|',p_accession,p_file,cik,metric,period_start,period_end,unit),'UTF8')),'hex'),p_accession,p_file,cik,metric,value,unit,period_start,period_end,normalized_period,report_date,availability_bound,'CONDITIONAL','original filing fact * 10^scale * sign; consolidated context; current report or cover-share instant',lineage from grouped where value_variants=1 on conflict do nothing;
with pairs(metric,a,b,operation) as(values
('operating_margin','operating_income','revenue','DIVIDE'),('gross_margin','gross_profit','revenue','DIVIDE'),('net_margin','net_income','revenue','DIVIDE'),
('current_ratio','current_assets','current_liabilities','DIVIDE'),('cash_ratio','cash','current_liabilities','DIVIDE'),
('liabilities_to_assets','liabilities','assets','DIVIDE'),('equity_to_assets','equity','assets','DIVIDE'),
('free_cash_flow','operating_cash_flow','capex','SUBTRACT'))
insert into research_eq10_fundamental_pit_20261020.financial_metrics_v1(metric_id,accession_number,file_id,entity_cik,metric,value,unit,period_start,period_end,period_kind,report_date,available_at,classification,formula,input_lineage)
select encode(sha256(convert_to(a.metric_id||b.metric_id||p.metric,'UTF8')),'hex'),a.accession_number,a.file_id,a.entity_cik,p.metric,
case when p.operation='DIVIDE' then a.value/nullif(b.value,0) else a.value-b.value end,
case when p.operation='DIVIDE' then 'ratio' else a.unit end,a.period_start,a.period_end,a.period_kind,a.report_date,greatest(a.available_at,b.available_at),'CONDITIONAL',
p.a||case when p.operation='DIVIDE' then ' / ' else ' - ' end||p.b,jsonb_build_array(a.metric_id,b.metric_id)
from pairs p join research_eq10_fundamental_pit_20261020.financial_metrics_v1 a on a.metric=p.a
join research_eq10_fundamental_pit_20261020.financial_metrics_v1 b on b.metric=p.b and b.accession_number=a.accession_number and b.file_id=a.file_id and b.entity_cik=a.entity_cik and b.period_start is not distinct from a.period_start and b.period_end=a.period_end and b.unit=a.unit
where a.accession_number=p_accession and a.file_id=p_file and (p.operation<>'DIVIDE' or b.value>0) and (p.operation<>'SUBTRACT' or b.value>=0) on conflict do nothing;
select count(*) into n from research_eq10_fundamental_pit_20261020.financial_metrics_v1 where accession_number=p_accession and file_id=p_file;
return jsonb_build_object('metrics',n,'comparative_values_backdated',false,'amendments_backdated',false,'receipt_certified',false);
end $fn$;
create function research_eq10_fundamental_pit_20261020.financial_metrics_asof_v1(p_cik text,p_cutoff timestamptz,p_allow_conditional boolean default false,p_research_cutoff timestamptz default now())
returns jsonb language sql stable security invoker set search_path=pg_catalog,research_eq10_fundamental_pit_20261020 as $fn$
select coalesce(jsonb_agg(to_jsonb(r)),'[]'::jsonb) from(
 select distinct on(metric,period_kind) * from research_eq10_fundamental_pit_20261020.financial_metrics_v1
 where entity_cik=p_cik and available_at<=p_cutoff and research_available_at<=p_research_cutoff
 and p_cutoff>='2025-09-01 00:00:00 America/New_York'::timestamptz and p_cutoff<'2026-08-01 00:00:00 America/New_York'::timestamptz
 and(classification='CERTIFIED' or p_allow_conditional)
 order by metric,period_kind,report_date desc,available_at desc,metric_id
)r $fn$;
revoke all on function research_eq10_fundamental_pit_20261020.financial_reconstruct_metrics_v1(text,text),research_eq10_fundamental_pit_20261020.financial_metrics_asof_v1(text,timestamptz,boolean,timestamptz) from public,anon,authenticated;
