-- persistent_worker_asof_boundaries
with x as(select
 research_eq10_fundamental_pit_20261020.financial_metrics_asof_v1('0001826470','2025-12-06 04:59:59.999999+00',true) before,
 research_eq10_fundamental_pit_20261020.financial_metrics_asof_v1('0001826470','2025-12-06 05:00:00+00',true) after,
 research_eq10_fundamental_pit_20261020.financial_metrics_asof_v1('0001826470','2025-12-06 05:00:00+00') strict,
 research_eq10_fundamental_pit_20261020.financial_metrics_asof_v1('0001826470','2025-12-06 05:00:00+00',true,'2026-10-21 00:00:00+00') before_research)
select (select count(*) from jsonb_array_elements(before) e where e->>'accession_number'='0001193125-25-309620') future_statement_before,
(select count(*) from jsonb_array_elements(after) e where e->>'accession_number'='0001193125-25-309620') statement_at_bound,
jsonb_array_length(strict) strict_rows,jsonb_array_length(before_research) before_research_rows from x;

-- persistent_worker_numerical_reconciliation
with g as(select accession_number,period_start,period_end,unit,jsonb_object_agg(metric,value) vals from research_eq10_fundamental_pit_20261020.financial_metrics_v1 where unit='USD' group by 1,2,3,4)
select accession_number,period_start,period_end,
case when vals?'assets' and vals?'liabilities' then (vals->>'assets')::numeric-(vals->>'liabilities')::numeric-coalesce((vals->>'equity_including_noncontrolling')::numeric,(vals->>'equity')::numeric) end balance_residual,
case when vals?'gross_profit' then (vals->>'revenue')::numeric-(vals->>'cost_of_revenue')::numeric-(vals->>'gross_profit')::numeric end gross_profit_residual,
case when vals?'free_cash_flow' then (vals->>'operating_cash_flow')::numeric-(vals->>'capex')::numeric-(vals->>'free_cash_flow')::numeric end free_cash_flow_residual from g order by 1,2 nulls first;

-- persistent_worker_source_integrity
select d.accession_number,d.file_id,d.provider,octet_length(d.raw_document) bytes,
d.document_sha256=encode(sha256(convert_to(d.raw_document,'UTF8')),'hex') hash_matches,
d.document_sha256=d.provider_metadata->>'raw_bytes_sha256' raw_bytes_hash_matches,
d.route='/v1/reference/sec/filings/'||d.accession_number||'/files/'||d.file_id authorized_route,
(select count(*) from research_eq10_fundamental_pit_20261020.financial_fact_v1 f where f.accession_number=d.accession_number and f.file_id=d.file_id) facts,
(select count(*) from research_eq10_fundamental_pit_20261020.financial_metrics_v1 m where m.accession_number=d.accession_number and m.file_id=d.file_id) metrics
from research_eq10_fundamental_pit_20261020.financial_documents_v1 d where d.accession_number in('0001826470-25-000015','0001193125-25-309620') order by 1;

-- persistent_worker_dst_availability
select accession_number,acceptance_local,accepted_at,availability_bound,availability_classification,historical_provider_receipt_at,
case accession_number when '0001826470-25-000015' then accepted_at='2025-08-29 20:05:48+00'::timestamptz and availability_bound='2025-08-30 04:00:00+00'::timestamptz when '0001193125-25-309620' then accepted_at='2025-12-05 21:05:48+00'::timestamptz and availability_bound='2025-12-06 05:00:00+00'::timestamptz end independent_expected_utc_matches
from research_eq10_fundamental_pit_20261020.financial_filing_metadata_v1 where accession_number in('0001826470-25-000015','0001193125-25-309620') order by 1;

-- persistent_worker_lineage_integrity
with rows as(select m.*,jsonb_array_elements(input_lineage) i from research_eq10_fundamental_pit_20261020.financial_metrics_v1 m)
select count(*) lineage_entries,count(*) filter(where case when jsonb_typeof(i)='object' then not exists(select 1 from research_eq10_fundamental_pit_20261020.financial_fact_v1 f where f.accession_number=i->>'accession' and f.file_id=i->>'file_id' and f.ordinal=(i->>'ordinal')::int and f.fact_sha256=i->>'fact_sha256' and f.dimension_count=0 and f.extraction_state='PARSED' and lpad(f.entity_cik,10,'0')=rows.entity_cik) when jsonb_typeof(i)='string' then not exists(select 1 from research_eq10_fundamental_pit_20261020.financial_metrics_v1 a where a.metric_id=i#>>'{}' and a.available_at<=rows.available_at and a.accession_number=rows.accession_number) else true end) broken_lineage,
count(*) filter(where classification<>'CONDITIONAL') nonconditional from rows;

-- persistent_worker_recovery
select j.job_id,j.status,j.attempts,j.retrieval_count,j.interruption_done,
count(*) filter(where e.event='INTENTIONAL_WORKER_INTERRUPTION') interruptions,
count(*) filter(where e.event='EXPIRED_LEASE_RECOVERED') recoveries,
count(*) filter(where e.event='DOCUMENT_REUSED') document_reuses,
count(distinct e.details->>'isolate') distinct_isolates,
count(*) filter(where e.event='BATCH_COMPLETE') completions
from research_eq10_fundamental_pit_20261020.financial_worker_jobs_v1 j join research_eq10_fundamental_pit_20261020.financial_worker_events_v1 e using(job_id)
group by j.job_id,j.status,j.attempts,j.retrieval_count,j.interruption_done order by j.job_id;

-- persistent_worker_ratio_reconciliation
with expected as(select d.metric_id,d.value,p.a,p.b from research_eq10_fundamental_pit_20261020.financial_metrics_v1 d join (values('operating_margin','operating_income','revenue'),('gross_margin','gross_profit','revenue'),('net_margin','net_income','revenue'),('current_ratio','current_assets','current_liabilities'),('cash_ratio','cash','current_liabilities'),('liabilities_to_assets','liabilities','assets'),('equity_to_assets','equity','assets')) p(metric,a,b) on p.metric=d.metric), vals as(select e.*,m1.value numerator,m2.value denominator from expected e join research_eq10_fundamental_pit_20261020.financial_metrics_v1 d using(metric_id) join research_eq10_fundamental_pit_20261020.financial_metrics_v1 m1 on m1.metric=e.a and m1.accession_number=d.accession_number and m1.period_start is not distinct from d.period_start and m1.period_end=d.period_end join research_eq10_fundamental_pit_20261020.financial_metrics_v1 m2 on m2.metric=e.b and m2.accession_number=d.accession_number and m2.period_start is not distinct from d.period_start and m2.period_end=d.period_end) select count(*) tested,count(*) filter(where abs(value-numerator/denominator)>1e-15) mismatches from vals;

-- persistent_worker_private_access
select (select count(*) from pg_class c join pg_namespace n on n.oid=c.relnamespace where n.nspname='research_eq10_fundamental_pit_20261020' and c.relname like 'financial_%' and c.relkind='r' and(not c.relrowsecurity or has_table_privilege('anon',c.oid,'SELECT,INSERT,UPDATE,DELETE') or has_table_privilege('authenticated',c.oid,'SELECT,INSERT,UPDATE,DELETE'))) exposed_tables,(select count(*) from pg_proc p join pg_namespace n on n.oid=p.pronamespace where n.nspname='research_eq10_fundamental_pit_20261020' and (p.proname like 'financial_%' or p.proname like 'extract_financial_%') and(has_function_privilege('anon',p.oid,'EXECUTE') or has_function_privilege('authenticated',p.oid,'EXECUTE'))) exposed_functions;

-- Stop and bulk guard test rolls back
begin;
do $test$
declare result jsonb;
begin
update research_eq10_fundamental_pit_20261020.financial_worker_control_v1 set stopped=true where lane='financials';
result:=research_eq10_fundamental_pit_20261020.financial_worker_claim_v1('rollback-stop-test','test');
if result->>'state'<>'STOPPED' then raise exception 'Stop gate failed: %',result;end if;
update research_eq10_fundamental_pit_20261020.financial_worker_control_v1 set stopped=false where lane='financials';
insert into research_eq10_fundamental_pit_20261020.financial_worker_jobs_v1(job_id,accession_number,file_id,acceptance_job) values('ROLLBACK_ONLY_BULK_GATE_TEST','0000000000-00-000000','test.htm',false);
result:=research_eq10_fundamental_pit_20261020.financial_worker_claim_v1('rollback-bulk-test','test');
if result->>'state'<>'IDLE' then raise exception 'Bulk gate failed: %',result;end if;
if exists(select 1 from research_eq10_fundamental_pit_20261020.financial_worker_events_v1 e join research_eq10_fundamental_pit_20261020.financial_worker_jobs_v1 j using(job_id) where e.event='INTENTIONAL_WORKER_INTERRUPTION' and j.lease_token=e.lease_token and j.status='LEASED') then raise exception 'Expired lease still writable';end if;
end $test$;
rollback;
-- Idempotency test rolls back
begin; do $test$ declare nf bigint; nm bigint; begin
select count(*) into nf from research_eq10_fundamental_pit_20261020.financial_fact_v1;
select count(*) into nm from research_eq10_fundamental_pit_20261020.financial_metrics_v1;
perform research_eq10_fundamental_pit_20261020.extract_financial_document_v1('0001193125-25-309620','woof-20251101.htm');
perform research_eq10_fundamental_pit_20261020.financial_reconstruct_metrics_v1('0001193125-25-309620','woof-20251101.htm');
if nf<>(select count(*) from research_eq10_fundamental_pit_20261020.financial_fact_v1) or nm<>(select count(*) from research_eq10_fundamental_pit_20261020.financial_metrics_v1) then raise exception 'Idempotency failed';end if;
end $test$;rollback;