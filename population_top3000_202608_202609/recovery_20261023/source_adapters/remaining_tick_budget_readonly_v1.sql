-- ROOT READ ONLY. One MVCC snapshot; no bodies, source-row scans, provider calls or cap changes.
-- Estimate is conditional on successful retained-data processing to the existing quality holds.
BEGIN TRANSACTION READ ONLY;
SET LOCAL statement_timeout='8s';
SET LOCAL lock_timeout='1s';
WITH task_meta AS MATERIALIZED (
 SELECT t.task_id,t.family_id,t.stage,t.partition_key,t.payload->>'kind' AS kind,t.input_versions,
  t.status,t.checkpoint,t.executions,t.max_executions,t.blocker_code,t.blocker_until,
  t.recheck_count,t.max_rechecks,t.not_before,t.lease_until,t.repair_round,t.created_at
 FROM population_top3000_202608_202609_v1.pipeline_task t
), d02_state AS MATERIALIZED (
 SELECT ds.session_date,ds.result_count,ds.normalized_source_records,ds.verified_source_records,
  ds.normalized_at,ds.source_verified_at,aq.partition_key,aq.status AS acquire_status,
  nt.task_id AS normalize_task_id,nt.status AS normalize_status,nt.executions AS normalize_executions,
  nt.max_executions AS normalize_max_executions,
  CASE WHEN coalesce(nt.checkpoint->>'last_source_row','0') ~ '^[0-9]{1,6}$'
   THEN coalesce(nt.checkpoint->>'last_source_row','0')::bigint END AS normalize_checkpoint,
  dt.task_id AS derive_task_id,dt.status AS derive_status,dt.executions AS derive_executions,
  dt.max_executions AS derive_max_executions,
  CASE WHEN coalesce(dt.checkpoint->>'last_verified_row','0') ~ '^[0-9]{1,6}$'
   THEN coalesce(dt.checkpoint->>'last_verified_row','0')::bigint END AS derive_checkpoint,
  vt.task_id AS validate_task_id,vt.status AS validate_status,vt.executions AS validate_executions
 FROM population_top3000_202608_202609_v1.d02_daily_source_v2 ds
 JOIN task_meta aq ON aq.task_id=ds.acquire_task_id
 LEFT JOIN LATERAL (SELECT tm.* FROM task_meta tm WHERE tm.family_id='D02'
  AND tm.kind='MASSIVE_GROUPED_DAILY_V2' AND tm.partition_key=aq.partition_key
  AND tm.input_versions=ds.input_versions AND tm.stage='NORMALIZE'
  ORDER BY tm.repair_round DESC,tm.created_at DESC,tm.task_id DESC LIMIT 1) nt ON true
 LEFT JOIN LATERAL (SELECT tm.* FROM task_meta tm WHERE tm.family_id='D02'
  AND tm.kind='MASSIVE_GROUPED_DAILY_V2' AND tm.partition_key=aq.partition_key
  AND tm.input_versions=ds.input_versions AND tm.stage='DERIVE'
  ORDER BY tm.repair_round DESC,tm.created_at DESC,tm.task_id DESC LIMIT 1) dt ON true
 LEFT JOIN LATERAL (SELECT tm.* FROM task_meta tm WHERE tm.family_id='D02'
  AND tm.kind='MASSIVE_GROUPED_DAILY_V2' AND tm.partition_key=aq.partition_key
  AND tm.input_versions=ds.input_versions AND tm.stage='VALIDATE'
  ORDER BY tm.repair_round DESC,tm.created_at DESC,tm.task_id DESC LIMIT 1) vt ON true
), d02_residual AS MATERIALIZED (
 SELECT d.*,
  CASE WHEN d.normalize_status='COMPLETE' THEN 0
   WHEN d.normalize_checkpoint BETWEEN 0 AND d.result_count
    AND (d.normalize_checkpoint=0 OR d.normalize_checkpoint%1000=0)
   THEN greatest(1,(d.result_count-d.normalize_checkpoint+999)/1000) END AS normalize_calls_remaining,
  CASE WHEN d.derive_status='COMPLETE' THEN 0
   WHEN d.derive_checkpoint BETWEEN 0 AND d.result_count
    AND (d.derive_checkpoint=0 OR d.derive_checkpoint%1000=0)
   THEN greatest(1,(d.result_count-d.derive_checkpoint+999)/1000) END AS derive_calls_remaining,
  CASE WHEN d.validate_executions>0 AND d.validate_status IN('COMPLETE','BLOCKED') THEN 0 ELSE 1 END AS quality_assessment_calls_upper,
  (d.acquire_status='COMPLETE'
   AND d.normalize_task_id IS NOT NULL
   AND (d.normalize_status IS DISTINCT FROM 'COMPLETE' OR (d.normalized_at IS NOT NULL AND d.normalized_source_records=d.result_count))
   AND (d.derive_status IS DISTINCT FROM 'COMPLETE' OR (d.source_verified_at IS NOT NULL AND d.verified_source_records=d.result_count))
   AND (d.normalize_status IS DISTINCT FROM 'COMPLETE' OR d.derive_task_id IS NOT NULL)
   AND (d.derive_status IS DISTINCT FROM 'COMPLETE' OR d.validate_task_id IS NOT NULL)
  ) IS TRUE AS metadata_and_handoffs_consistent
 FROM d02_state d
), s02_archives AS MATERIALIZED (
 SELECT sp.archive_key,sp.partition_key,sp.acquire_task_id,count(*) AS pages,
  max(sp.page_number) AS highest_page,max(sp.page_number) FILTER(WHERE sp.next_url IS NULL) AS terminal_page,
  count(*) FILTER(WHERE sp.next_url IS NULL) AS terminal_pages,
  count(*) FILTER(WHERE sp.normalized_at IS NOT NULL AND sp.normalized_source_records=sp.result_count) AS normalized_pages,
  sum(sp.result_count) AS source_occurrences
 FROM population_top3000_202608_202609_v1.massive_shortinterest_page_v1 sp
 WHERE sp.family_id='S02' AND sp.partition_key='short-interest-settlement-20260715-20260930'
 GROUP BY sp.archive_key,sp.partition_key,sp.acquire_task_id
), s02_state AS MATERIALIZED (
 SELECT sa.*,aq.status AS acquire_status,
  nt.task_id AS normalize_task_id,nt.status AS normalize_status,nt.executions AS normalize_executions,
  nt.max_executions AS normalize_max_executions,
  CASE WHEN coalesce(nt.checkpoint->>'last_normalized_page','0') ~ '^[0-9]{1,4}$'
   THEN coalesce(nt.checkpoint->>'last_normalized_page','0')::bigint END AS normalize_checkpoint,
  dt.task_id AS derive_task_id,dt.status AS derive_status,dt.executions AS derive_executions,
  dt.max_executions AS derive_max_executions,
  CASE WHEN coalesce(dt.checkpoint->>'last_derived_page','0') ~ '^[0-9]{1,4}$'
   THEN coalesce(dt.checkpoint->>'last_derived_page','0')::bigint END AS derive_checkpoint,
  vt.task_id AS validate_task_id,vt.status AS validate_status,vt.executions AS validate_executions,
  rt.task_id AS repair_task_id,rt.status AS repair_status,rt.executions AS repair_executions
 FROM s02_archives sa JOIN task_meta aq ON aq.task_id=sa.acquire_task_id
 LEFT JOIN LATERAL (SELECT tm.* FROM task_meta tm WHERE tm.family_id='S02' AND tm.partition_key=sa.partition_key
  AND tm.input_versions=aq.input_versions AND tm.stage='NORMALIZE'
  ORDER BY tm.repair_round DESC,tm.created_at DESC,tm.task_id DESC LIMIT 1) nt ON true
 LEFT JOIN LATERAL (SELECT tm.* FROM task_meta tm WHERE tm.family_id='S02' AND tm.partition_key=sa.partition_key
  AND tm.input_versions=aq.input_versions AND tm.stage='DERIVE'
  ORDER BY tm.repair_round DESC,tm.created_at DESC,tm.task_id DESC LIMIT 1) dt ON true
 LEFT JOIN LATERAL (SELECT tm.* FROM task_meta tm WHERE tm.family_id='S02' AND tm.partition_key=sa.partition_key
  AND tm.input_versions=aq.input_versions AND tm.stage='VALIDATE'
  ORDER BY tm.repair_round DESC,tm.created_at DESC,tm.task_id DESC LIMIT 1) vt ON true
 LEFT JOIN LATERAL (SELECT tm.* FROM task_meta tm WHERE tm.family_id='S02' AND tm.partition_key=sa.partition_key
  AND tm.input_versions=aq.input_versions AND tm.stage='REPAIR'
  ORDER BY tm.repair_round DESC,tm.created_at DESC,tm.task_id DESC LIMIT 1) rt ON true
), s02_residual AS MATERIALIZED (
 SELECT s.*,
  CASE WHEN s.normalize_status='COMPLETE' THEN 0
   WHEN s.normalize_checkpoint BETWEEN 0 AND s.terminal_page-1 THEN s.terminal_page-s.normalize_checkpoint END AS normalize_calls_remaining,
  CASE WHEN s.derive_status='COMPLETE' THEN 0
   WHEN s.derive_checkpoint BETWEEN 0 AND s.terminal_page-1 THEN s.terminal_page-s.derive_checkpoint END AS derive_calls_remaining,
  CASE WHEN s.validate_executions>0 AND s.validate_status IN('COMPLETE','BLOCKED') THEN 0 ELSE 1 END
   + CASE WHEN s.validate_status='BLOCKED' AND s.validate_executions>0 THEN 0
     WHEN s.repair_executions>0 AND s.repair_status IN('COMPLETE','BLOCKED') THEN 0 ELSE 1 END AS quality_assessment_calls_upper,
  (s.acquire_status='COMPLETE' AND s.terminal_pages=1 AND s.pages=s.terminal_page AND s.highest_page=s.terminal_page
   AND s.normalize_task_id IS NOT NULL
   AND (s.normalize_status IS DISTINCT FROM 'COMPLETE' OR (s.normalized_pages=s.pages AND s.derive_task_id IS NOT NULL))
   AND (s.derive_status IS DISTINCT FROM 'COMPLETE' OR s.validate_task_id IS NOT NULL)
  ) IS TRUE AS metadata_and_handoffs_consistent
 FROM s02_state s
), sample_assessments AS MATERIALIZED (
 SELECT tm.* FROM task_meta tm WHERE tm.family_id IN('P01','P02') AND tm.stage='NORMALIZE'
  AND tm.kind='RETAINED_ALPACA_AUGUST_SESSION'
), modeled_active_task_ids AS MATERIALIZED (
 SELECT tm.task_id FROM task_meta tm WHERE
  (tm.family_id='D02' AND tm.kind='MASSIVE_GROUPED_DAILY_V2' AND tm.stage IN('NORMALIZE','DERIVE','VALIDATE'))
  OR (tm.family_id='S02' AND tm.partition_key='short-interest-settlement-20260715-20260930'
   AND tm.stage IN('NORMALIZE','DERIVE','VALIDATE','REPAIR'))
  OR tm.task_id IN(SELECT sa.task_id FROM sample_assessments sa)
), task_extras AS (
 SELECT count(*) FILTER(WHERE tm.status='LEASED') AS leased_tasks,
  count(*) FILTER(WHERE tm.status='READY' AND tm.not_before>clock_timestamp()) AS future_ready_tasks,
  count(*) FILTER(WHERE tm.status IN('READY','LEASED') AND tm.executions>=tm.max_executions) AS active_tasks_at_execution_cap,
  count(*) FILTER(WHERE tm.status IN('READY','LEASED') AND NOT EXISTS(
   SELECT 1 FROM modeled_active_task_ids m WHERE m.task_id=tm.task_id)) AS unmodeled_active_tasks,
  count(*) FILTER(WHERE tm.status='BLOCKED' AND tm.blocker_until IS NOT NULL
   AND tm.recheck_count<tm.max_rechecks
   AND tm.blocker_code IN('DEPENDENCY_PENDING','RETAINED_IMPORT_PENDING','RETAINED_SEC_SOURCE_IMPORT_INCOMPLETE','ADAPTER_NOT_INSTALLED')
   AND EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_adapter ad
    WHERE ad.family_id=tm.family_id AND ad.stage=tm.stage AND ad.enabled)) AS eligible_timed_recheck_tasks,
  coalesce(sum(greatest(0,tm.max_rechecks-tm.recheck_count)) FILTER(WHERE tm.status='BLOCKED'
   AND tm.blocker_until IS NOT NULL AND tm.recheck_count<tm.max_rechecks
   AND tm.blocker_code IN('DEPENDENCY_PENDING','RETAINED_IMPORT_PENDING','RETAINED_SEC_SOURCE_IMPORT_INCOMPLETE','ADAPTER_NOT_INSTALLED')
   AND EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_adapter ad
    WHERE ad.family_id=tm.family_id AND ad.stage=tm.stage AND ad.enabled)),0) AS configured_timed_recheck_calls_upper
 FROM task_meta tm
), d02_totals AS (
 SELECT count(*) AS source_dates,sum(d.result_count) AS source_occurrences,
  coalesce(sum(d.normalize_calls_remaining),0) AS normalize_calls_remaining,
  coalesce(sum(d.derive_calls_remaining),0) AS derive_calls_remaining,
  coalesce(sum(d.quality_assessment_calls_upper),0) AS quality_assessment_calls_upper,
  count(*) FILTER(WHERE NOT d.metadata_and_handoffs_consistent
   OR d.normalize_calls_remaining IS NULL OR d.derive_calls_remaining IS NULL) AS invalid_or_unproven_partitions,
  count(*) FILTER(WHERE d.normalize_status IN('BLOCKED','CANCELLED','FAILED') OR d.derive_status IN('BLOCKED','CANCELLED','FAILED')) AS data_stage_holds
 FROM d02_residual d
), s02_totals AS (
 SELECT count(*) AS archives,coalesce(sum(s.pages),0) AS retained_pages,
  coalesce(sum(s.normalize_calls_remaining),0) AS normalize_calls_remaining,
  coalesce(sum(s.derive_calls_remaining),0) AS derive_calls_remaining,
  coalesce(sum(s.quality_assessment_calls_upper),0) AS quality_assessment_calls_upper,
  count(*) FILTER(WHERE NOT s.metadata_and_handoffs_consistent
   OR s.normalize_calls_remaining IS NULL OR s.derive_calls_remaining IS NULL) AS invalid_or_unproven_archives,
  count(*) FILTER(WHERE s.normalize_status IN('BLOCKED','CANCELLED','FAILED') OR s.derive_status IN('BLOCKED','CANCELLED','FAILED')) AS data_stage_holds
 FROM s02_residual s
), sample_totals AS (
 SELECT count(*) AS sample_tasks,count(*) FILTER(WHERE sa.status IN('READY','LEASED')) AS assessment_calls_remaining,
  count(*) FILTER(WHERE sa.status='BLOCKED') AS already_assessed_and_held,
  count(*) FILTER(WHERE sa.status NOT IN('READY','LEASED','BLOCKED','COMPLETE','CANCELLED')) AS unexpected_statuses
 FROM sample_assessments sa
), cost AS (
 SELECT d.normalize_calls_remaining+d.derive_calls_remaining+d.quality_assessment_calls_upper
   +s.normalize_calls_remaining+s.derive_calls_remaining+s.quality_assessment_calls_upper
   +q.assessment_calls_remaining AS once_through_adapter_calls_upper,
  d.source_dates=42 AND d.invalid_or_unproven_partitions=0 AND d.data_stage_holds=0
   AND s.archives=1 AND s.invalid_or_unproven_archives=0 AND s.data_stage_holds=0
   AND q.unexpected_statuses=0 AS modeled_metadata_consistent
 FROM d02_totals d CROSS JOIN s02_totals s CROSS JOIN sample_totals q
)
SELECT jsonb_build_object(
 'observed_at',clock_timestamp(),'estimate_class','SUCCESSFUL_RETAINED_WORK_TO_EXISTING_EXPLICIT_HOLDS_ONLY',
 'control',jsonb_build_object('owner_id',ac.owner_id,'owner_fence',ac.owner_fence,'stop_generation',ac.stop_generation,
  'tick_count',pc.tick_count,'max_ticks',pc.max_ticks,'ticks_remaining',greatest(0,pc.max_ticks-pc.tick_count),
  'pipeline_enabled',pc.enabled,'hold_reason',ac.hold_reason,'next_wakeup_at',pc.next_wakeup_at,
  'idle_rechecks',pc.idle_rechecks,'max_idle_rechecks',pc.max_idle_rechecks,'max_active_tasks',pc.max_active_tasks,
  'scheduler',(SELECT jsonb_build_object('active',j.active,'schedule',j.schedule) FROM cron.job j WHERE j.jobid=596)),
 'd02',to_jsonb(d),'s02',to_jsonb(s),'sample_assessments',to_jsonb(q),'other_tick_exposure',to_jsonb(ex),
 'once_through_adapter_calls_upper',co.once_through_adapter_calls_upper,
 'post_work_idle_ticks_upper',CASE WHEN co.once_through_adapter_calls_upper>0 THEN pc.max_idle_rechecks+1 ELSE greatest(0,pc.max_idle_rechecks-pc.idle_rechecks+1) END,
 'conditional_total_ticks_upper',CASE WHEN co.modeled_metadata_consistent AND ex.unmodeled_active_tasks=0
  AND ex.leased_tasks=0 AND ex.future_ready_tasks=0 AND ex.active_tasks_at_execution_cap=0
  AND ex.eligible_timed_recheck_tasks=0
  THEN co.once_through_adapter_calls_upper+ex.configured_timed_recheck_calls_upper
   +CASE WHEN co.once_through_adapter_calls_upper>0 THEN pc.max_idle_rechecks+1 ELSE greatest(0,pc.max_idle_rechecks-pc.idle_rechecks+1) END END,
 'conditional_slack_after_work_and_idle',CASE WHEN co.modeled_metadata_consistent AND ex.unmodeled_active_tasks=0
  AND ex.leased_tasks=0 AND ex.future_ready_tasks=0 AND ex.active_tasks_at_execution_cap=0
  AND ex.eligible_timed_recheck_tasks=0
  THEN pc.max_ticks-pc.tick_count-co.once_through_adapter_calls_upper-ex.configured_timed_recheck_calls_upper
   -CASE WHEN co.once_through_adapter_calls_upper>0 THEN pc.max_idle_rechecks+1 ELSE greatest(0,pc.max_idle_rechecks-pc.idle_rechecks+1) END END,
 'modeled_metadata_consistent',co.modeled_metadata_consistent,
 'd02_partitions',(SELECT coalesce(jsonb_agg(to_jsonb(dr) ORDER BY dr.session_date),'[]'::jsonb) FROM d02_residual dr),
 's02_archives',(SELECT coalesce(jsonb_agg(to_jsonb(sr) ORDER BY sr.archive_key),'[]'::jsonb) FROM s02_residual sr),
 'active_task_groups',(SELECT coalesce(jsonb_agg(to_jsonb(g) ORDER BY g.family_id,g.stage,g.status),'[]'::jsonb)
  FROM (SELECT tm.family_id,tm.stage,tm.status,tm.kind,count(*) AS tasks,sum(tm.executions) AS executions
   FROM task_meta tm WHERE tm.status IN('READY','LEASED') GROUP BY tm.family_id,tm.stage,tm.status,tm.kind) g),
 'function_hashes',(SELECT jsonb_object_agg(f.signature,encode(sha256(convert_to(pg_get_functiondef(to_regprocedure(f.signature)),'UTF8')),'hex'))
  FROM (VALUES
   ('population_top3000_202608_202609_v1.pipeline_tick_v1()'),
   ('population_top3000_202608_202609_v1.pipeline_finish_v1(uuid,text,jsonb)'),
   ('population_top3000_202608_202609_v1.d02_daily_normalize_v2(uuid,text)'),
   ('population_top3000_202608_202609_v1.d02_daily_derive_v2(uuid,text)'),
   ('population_top3000_202608_202609_v1.d02_daily_quality_hold_v2(uuid,text)'),
   ('population_top3000_202608_202609_v1.massive_shortinterest_normalize_v1(uuid,text)'),
   ('population_top3000_202608_202609_v1.massive_shortinterest_derive_v1(uuid,text)'),
   ('population_top3000_202608_202609_v1.massive_shortinterest_repair_hold_v1(uuid,text)'),
   ('population_top3000_202608_202609_v1.price_retained_partition_v1(uuid,text)')
  ) f(signature)),
 'qualification','One successful adapter call per useful tick. D02 counts exact remaining1000-row batches, S02 exact remaining pages, sample assessments one call each; final batches finish without extra terminal calls. Includes at most one D02 quality hold per date and at most S02 VALIDATE plus REPAIR. The conditional bound excludes new admissions, failed/retried calls, new timed waits and source or task repairs; it is not a bound for finishing the entire24-family mission. A NULL bound requires inspection of the returned anomaly/exposure fields. No cap or cadence change is proposed by this query.'
) AS remaining_tick_budget
FROM population_top3000_202608_202609_v1.acquisition_control ac
JOIN population_top3000_202608_202609_v1.pipeline_control pc USING(singleton)
CROSS JOIN d02_totals d CROSS JOIN s02_totals s CROSS JOIN sample_totals q CROSS JOIN task_extras ex CROSS JOIN cost co
WHERE ac.singleton;
COMMIT;
