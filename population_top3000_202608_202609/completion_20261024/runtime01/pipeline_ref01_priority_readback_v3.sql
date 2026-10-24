-- Read-only actual post-deployment proof: root records this result after real cron dispatches.
WITH installed AS (
 SELECT event_id,at,details FROM population_top3000_202608_202609_v1.pipeline_event
 WHERE event='REF01_BOUNDED_PRODUCTIVE_PRIORITY_INSTALLED_V3' AND details->>'owner_fence'='3' AND details->>'stop_generation'='1'
 ORDER BY event_id DESC LIMIT 1
), dispatched AS (
 SELECT e.event_id,e.at,e.task_id,e.attempt_id,e.details FROM population_top3000_202608_202609_v1.pipeline_event e,installed i
 WHERE e.event_id>i.event_id AND e.event='DISPATCHED' AND e.details->>'family'='REF01'
 ORDER BY e.event_id LIMIT 12
)
SELECT clock_timestamp() observed_at,
 encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.pipeline_tick_v1()'::regprocedure),'UTF8')),'hex') current_tick_sha256,
 (SELECT to_jsonb(i) FROM installed i) installation,
 (SELECT jsonb_agg(jsonb_build_object('event_id',d.event_id,'at',d.at,'task_id',d.task_id,'attempt_id',d.attempt_id,'stage',d.details->>'stage','result',d.details->'result',
 'task_status',t.status,'successor',jsonb_build_object('task_id',s.task_id,'stage',s.stage,'status',s.status,'repair_round',s.repair_round),'immutable_commits',(SELECT jsonb_agg(jsonb_build_object('commit_key',to_jsonb(c)->'commit_key','output_sha256',to_jsonb(c)->'output_sha256','new_committed_units',to_jsonb(c)->'new_committed_units')) FROM population_top3000_202608_202609_v1.pipeline_commit c WHERE c.task_id=d.task_id)) ORDER BY d.event_id)
 FROM dispatched d JOIN population_top3000_202608_202609_v1.pipeline_task t ON t.task_id=d.task_id
 LEFT JOIN population_top3000_202608_202609_v1.pipeline_task s ON s.task_id=nullif(d.details#>>'{result,successor_task_id}','')::uuid) actual_ref01_dispatches,
 (SELECT jsonb_build_object('tick_count',tick_count,'max_ticks',max_ticks,'last_useful_progress_at',last_useful_progress_at,'expected_owner',expected_owner_id,'fence',expected_owner_fence,'stop_generation',expected_stop_generation)
 FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton) controls;
