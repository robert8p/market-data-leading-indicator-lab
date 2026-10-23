-- Root-only minimal priority change. No enable/wake, limits, holds, counters or task changes.
-- Existing eligible noninventory work precedes this bounded retained-price inventory.
BEGIN;
SET LOCAL statement_timeout='5s';
SET LOCAL lock_timeout='500ms';
SELECT cron.alter_job(596,active:=false);
SELECT pg_advisory_xact_lock(2026083000261022);
DO $patch$
DECLARE v_before text;v_after text;
 v_old text:='order by (adapter_rows.enabled is true) desc, q.executions,q.created_at,q.task_id for update of q skip locked limit 1;';
 v_new text:='order by (adapter_rows.enabled is true) desc, case when q.family_id in(''P01'',''P02'') and q.payload->>''kind''=''RETAINED_ALPACA_AUGUST_SESSION'' then 20 else 0 end, q.executions,q.created_at,q.task_id for update of q skip locked limit 1;';
BEGIN
 PERFORM 1 FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton FOR UPDATE;
 PERFORM 1 FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton FOR UPDATE;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.acquisition_control a
  JOIN population_top3000_202608_202609_v1.pipeline_control p ON p.singleton=a.singleton
  WHERE a.singleton AND a.owner_id='top3000-recovery-20261023-root-4b1314bcb5c2' AND a.owner_fence=2 AND a.stop_generation=0
   AND a.owner_id=p.expected_owner_id AND a.owner_fence=p.expected_owner_fence AND a.stop_generation=p.expected_stop_generation
   AND a.scope_sha256=p.scope_sha256 AND a.scope_sha256='8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd'
   AND NOT a.stopped AND a.enabled AND a.hold_reason IS NULL AND a.admitted_until>clock_timestamp())
 THEN RAISE EXCEPTION 'RETAINED_PRICE_PRIORITY_OWNER_OR_ADMISSION_CHANGED';END IF;
 IF EXISTS(SELECT 1 FROM cron.job WHERE jobid=596 AND active)
  OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE status='LEASED')
  OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_provider_lease WHERE released_at IS NULL)
 THEN RAISE EXCEPTION 'PAUSE_AND_RECONCILE_BEFORE_PRIORITY_PATCH';END IF;
 v_before:=pg_get_functiondef('population_top3000_202608_202609_v1.pipeline_tick_v1()'::regprocedure);
 IF position(v_new in v_before)>0 THEN RETURN;END IF;
 IF (length(v_before)-length(replace(v_before,v_old,'')))/length(v_old)<>1 THEN RAISE EXCEPTION 'CURRENT_DISPATCH_ORDER_REQUIRES_REVIEW';END IF;
 v_after:=replace(v_before,v_old,v_new);
 EXECUTE v_after;
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details)
 VALUES('RETAINED_PRICE_LOWER_PRIORITY_DISPATCH_INSTALLED',jsonb_build_object('owner_fence',2,'stop_generation',0,
  'previous_function_sha256',encode(sha256(convert_to(v_before,'UTF8')),'hex'),
  'function_sha256',encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.pipeline_tick_v1()'::regprocedure),'UTF8')),'hex'),
  'inventory_effective_priority',20,'other_eligible_work_effective_priority',0,'changed_only_selection_order',true));
END $patch$;
SELECT cron.alter_job(596,active:=true);
COMMIT;
