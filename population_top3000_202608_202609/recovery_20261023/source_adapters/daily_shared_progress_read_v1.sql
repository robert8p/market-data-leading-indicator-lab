-- Bounded current readback: new compact tables/metadata only, no source bodies.
BEGIN READ ONLY;
SET LOCAL statement_timeout='8s';
SELECT jsonb_build_object('observed_at',clock_timestamp(),
 'runs',(SELECT coalesce(jsonb_agg(to_jsonb(r)-'source_manifest'-'calendar_manifest'ORDER BY r.run_key),'[]')FROM population_top3000_202608_202609_v1.daily_shared_run_v1 r),
 'tasks',(SELECT coalesce(jsonb_agg(jsonb_build_object('task_id',t.task_id,'family',t.family_id,'stage',t.stage,'status',t.status,
   'checkpoint',t.checkpoint,'admitted_series',t.payload->'max_series','blocker_code',t.blocker_code,'executions',t.executions,'not_before',t.not_before)ORDER BY t.family_id,t.stage),'[]')
  FROM population_top3000_202608_202609_v1.pipeline_task t WHERE t.family_id IN('D03','D04','D05')),
 'batch_totals',(SELECT coalesce(jsonb_agg(to_jsonb(q)),'[]')FROM(SELECT b.run_id,b.stage,count(*)batches,sum(b.series_count)series,sum(b.row_count)rows,max(b.committed_at)latest_commit
  FROM population_top3000_202608_202609_v1.daily_shared_batch_v1 b GROUP BY b.run_id,b.stage)q),
 'reference_series',(SELECT coalesce(jsonb_agg(to_jsonb(s)-'candidate_snapshot'-'mapping_snapshot'ORDER BY s.run_id,s.series_no),'[]')
  FROM population_top3000_202608_202609_v1.daily_shared_series_v1 s WHERE s.source_ticker IN('AAPL','MSFT','NVDA')),
 'new_relation_sizes',(SELECT jsonb_object_agg(c.relname,pg_total_relation_size(c.oid))FROM pg_class c
  WHERE c.relnamespace='population_top3000_202608_202609_v1'::regnamespace AND c.relkind='r' AND c.relname LIKE 'daily_shared_%'),
 'mission_relation_bytes',(SELECT sum(pg_total_relation_size(c.oid))FROM pg_class c WHERE c.relnamespace='population_top3000_202608_202609_v1'::regnamespace AND c.relkind='r'),
 'tick_budget',(SELECT jsonb_build_object('tick_count',p.tick_count,'max_ticks',p.max_ticks,'deadline',p.deadline,'resource_limits',p.budget_evidence->'resource_limits')FROM population_top3000_202608_202609_v1.pipeline_control p WHERE p.singleton),
 'calculated_series_are_company_completeness',false,'provider_accuracy_verified',false
)AS daily_shared_progress;
ROLLBACK;
