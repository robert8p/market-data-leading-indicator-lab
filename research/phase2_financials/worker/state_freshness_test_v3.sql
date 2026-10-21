begin;
select set_config('statement_timeout','12000',true);
insert into research_eq10_fundamental_pit_20261020.financial_metrics_v1(metric_id,accession_number,file_id,entity_cik,metric,value,unit,period_start,period_end,period_kind,report_date,available_at,classification,formula,input_lineage)
select 'SYNTHETIC_ROLLBACK_STALE_GUARD',accession_number,file_id,entity_cik,metric,value+1,unit,period_start,period_end,period_kind,report_date,'2025-08-31T16:00Z','CONDITIONAL','SYNTHETIC_UNCOMMITTED_TEST',input_lineage from research_eq10_fundamental_pit_20261020.financial_metrics_v1 where entity_cik='0000320193' and metric='revenue' limit 1;
select research_eq10_fundamental_pit_20261020.financial_company_state_asof_v3('0000320193','2025-08-31T16:00Z')->>'retrieval_status' stale_guard_status,jsonb_array_length(research_eq10_fundamental_pit_20261020.financial_company_state_asof_v3('0000320193','2025-08-31T16:00Z')->'features') exposed_stale_features;
rollback;