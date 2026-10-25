BEGIN;
SET LOCAL lock_timeout='15s';
SET LOCAL statement_timeout='45s';
DO $quiet$
DECLARE deadline timestamptz:=clock_timestamp()+interval '45 seconds';locked boolean:=false;
BEGIN
 LOOP
  locked:=pg_try_advisory_xact_lock(2026083000261022);
  EXIT WHEN locked OR clock_timestamp()>=deadline;
  PERFORM pg_sleep(0.25);
 END LOOP;
 IF NOT locked THEN RAISE EXCEPTION 'RESOURCE_V4_BUSY_RETRY_LATER';END IF;
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE status='LEASED')
 OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_attempt WHERE ended_at IS NULL)
 OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_provider_lease WHERE released_at IS NULL)
 OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.connector_probe_lease_v1 WHERE released_at IS NULL)
 THEN RAISE EXCEPTION 'RESOURCE_V4_ACTIVE_WORK_RETRY_LATER';END IF;
END;$quiet$;
