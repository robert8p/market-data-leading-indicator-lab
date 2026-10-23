BEGIN;
SET LOCAL statement_timeout='8s'; SET LOCAL lock_timeout='500ms';
DO $pause$ BEGIN
 IF NOT pg_try_advisory_xact_lock(2026083000261022) THEN RAISE EXCEPTION 'ROOT_CONTROLLER_BUSY'; END IF;
 PERFORM 1 FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton FOR UPDATE;
 PERFORM 1 FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton FOR UPDATE;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.acquisition_control a JOIN population_top3000_202608_202609_v1.pipeline_control p USING(singleton)
 WHERE a.singleton AND a.owner_id='top3000-recovery-20261023-root-4b1314bcb5c2' AND a.owner_fence=2 AND a.stop_generation=0
 AND p.expected_owner_id=a.owner_id AND p.expected_owner_fence=a.owner_fence AND p.expected_stop_generation=a.stop_generation
 AND a.scope_sha256='8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd' AND p.scope_sha256=a.scope_sha256
 AND a.hold_reason IS NULL AND a.reserved_bytes=0 AND NOT a.stopped AND a.admitted_until>clock_timestamp() AND p.deadline>clock_timestamp())
 THEN RAISE EXCEPTION 'ROOT_OWNER_SCOPE_STOP_OR_RESERVATION_CHANGED'; END IF;
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE status='LEASED')
 OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_provider_lease WHERE released_at IS NULL)
 THEN RAISE EXCEPTION 'ROOT_LEASES_MUST_DRAIN'; END IF;
 IF NOT EXISTS(SELECT 1 FROM cron.job WHERE jobid=596 AND database=current_database() AND btrim(command)='select population_top3000_202608_202609_v1.pipeline_tick_v1();')
 THEN RAISE EXCEPTION 'ROOT_BOUND_CRON_CHANGED'; END IF;
 PERFORM cron.alter_job(596,active:=false);
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details)
 VALUES('ROOT_S02_VALIDATION_RECOVERY_PAUSED',jsonb_build_object('owner_id','top3000-recovery-20261023-root-4b1314bcb5c2','owner_fence',2,'stop_generation',0,'reason','Replace canceled generic S02 validation with reviewed bounded metadata hold; all completed source data and historical attempts preserved.','provider_calls',0,'scope_changed',false));
END $pause$;
SELECT clock_timestamp() observed_at,(SELECT active FROM cron.job WHERE jobid=596) scheduler_active,
(SELECT count(*) FROM population_top3000_202608_202609_v1.pipeline_task WHERE status='READY') ready_tasks,
(SELECT count(*) FROM population_top3000_202608_202609_v1.pipeline_task WHERE status='LEASED') leased_tasks;
COMMIT;
