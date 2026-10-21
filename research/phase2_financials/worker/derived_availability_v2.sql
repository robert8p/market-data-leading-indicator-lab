CREATE OR REPLACE FUNCTION research_eq10_fundamental_pit_20261020.financial_reconstruct_metrics_v1(p_accession text, p_file text)
 RETURNS jsonb
 LANGUAGE plpgsql
 SET search_path TO 'pg_catalog', 'research_eq10_fundamental_pit_20261020'
AS $function$
declare n bigint;
begin
with candidates as(
 select f.*,d.metric,d.priority,d.expected_unit,m.report_date,av.effective_available_at as availability_bound,
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
 join research_eq10_fundamental_pit_20261020.financial_availability_review_v2 av using(accession_number)
 where f.accession_number=p_accession and f.file_id=p_file and f.extraction_state='PARSED' and f.dimension_count=0
 and m.form_type in('10-K','10-Q') and av.effective_available_at is not null
 and (f.period_end=m.report_date or (d.metric='shares_outstanding' and f.period_end between m.report_date and m.filing_date))
 and (f.period_start is null or f.period_start<=f.period_end)
 and exists(select 1 from jsonb_array_elements(m.raw_payload->'entities') e where lpad(e#>>'{company_data,cik}',10,'0')=lpad(f.entity_cik,10,'0'))
), grouped as(
 select lpad(entity_cik,10,'0') cik,metric,period_start,period_end,normalized_period,report_date,availability_bound,
 case when expected_unit='currency_per_share' then split_part(numerator,':',2)||'/shares' when expected_unit='currency' then split_part(measure,':',2) else 'shares' end unit,
 min(value) value,count(distinct value) value_variants,
 jsonb_agg(jsonb_build_object('accession',accession_number,'file_id',file_id,'ordinal',ordinal,'concept',concept,'fact_sha256',fact_sha256,'context_id',context_id,'scale',source_scale,'availability_method','filing_acceptance_review_v2')) lineage
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
return jsonb_build_object('metrics',n,'comparative_values_backdated',false,'amendments_backdated',false,'receipt_certified',false,'availability_review',(select to_jsonb(a) from research_eq10_fundamental_pit_20261020.financial_availability_review_v2 a where a.accession_number=p_accession));
end $function$;
revoke all on function research_eq10_fundamental_pit_20261020.financial_reconstruct_metrics_v1(text,text) from public,anon,authenticated;