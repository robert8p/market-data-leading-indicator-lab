-- Read-only useful-output observation. Registration/READY/heartbeat is never completion.
SELECT clock_timestamp() observed_at,r.request_key,r.manifest->>'as_of' as_of,
 (SELECT jsonb_agg(jsonb_build_object('page_number',p.page_number,'source_id',p.source_id,'source_sha256',p.source_sha256,'retained_at',p.retrieved_at,'source_records',p.result_count,'normalized_records',p.normalized_source_records,'normalized_at',p.normalized_at,'terminal',p.next_url IS NULL) ORDER BY p.page_number)
  FROM population_top3000_202608_202609_v1.ctx02_census_page_v1 p WHERE p.partition_key=r.request_key) pages,
 (SELECT jsonb_agg(jsonb_build_object('task_id',t.task_id,'stage',t.stage,'status',t.status,'blocker_code',t.blocker_code,'source_page_number',t.payload->>'source_page_number','checkpoint',t.checkpoint,'executions',t.executions,'cumulative_failures',t.cumulative_failures) ORDER BY t.created_at)
  FROM population_top3000_202608_202609_v1.pipeline_task t WHERE t.family_id='CTX02' AND t.payload->>'request_key'=r.request_key) tasks,
 (SELECT jsonb_agg(jsonb_build_object('commit_id',c.commit_id,'committed_at',c.committed_at,'task_id',c.task_id,'stage',t.stage,'new_units',c.new_committed_units,'output_sha256',c.receipt->>'output_sha256','page_valid_units',c.receipt->>'page_valid_units','page_quarantined_units',c.receipt->>'page_quarantined_units','required_scope_accepted',c.receipt->>'required_scope_accepted') ORDER BY c.committed_at)
  FROM population_top3000_202608_202609_v1.pipeline_commit c JOIN population_top3000_202608_202609_v1.pipeline_task t USING(task_id) WHERE t.family_id='CTX02' AND t.payload->>'request_key'=r.request_key) commits
FROM population_top3000_202608_202609_v1.ctx02_census_request_v1 r WHERE r.manifest->>'route_revision'='ctx02-fullwindow-route-v6' ORDER BY as_of;
