-- Existing sole runtime owner and current reviewed code are mandatory.
-- A replay cannot extend the pause. Independent recovery revalidates all guards.
CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.population_maintenance_pause_v1(p_operation_key text,p_reason text,p_seconds integer DEFAULT 300) RETURNS jsonb LANGUAGE plpgsql SET search_path TO pg_catalog AS $fn$
DECLARE auth jsonb;c population_top3000_202608_202609_v1.acquisition_control%rowtype;p population_top3000_202608_202609_v1.pipeline_control%rowtype; prior jsonb; result jsonb;
BEGIN
 IF nullif(btrim(p_operation_key),'') IS NULL OR length(p_operation_key)>160 OR nullif(btrim(p_reason),'') IS NULL OR length(p_reason)>1000 OR p_seconds IS NULL OR p_seconds NOT BETWEEN 1 AND 900 THEN RAISE EXCEPTION 'BOUNDED_MAINTENANCE_ARGUMENT_REQUIRED';END IF;
 IF NOT pg_try_advisory_xact_lock(2026083000261022) THEN RETURN jsonb_build_object('state','BUSY','writes',false);END IF;
 SELECT e.details INTO prior FROM population_top3000_202608_202609_v1.pipeline_event e WHERE e.event='RECOVERY_MAINTENANCE_PAUSE_V1' AND e.details->>'operation_key'=p_operation_key ORDER BY e.event_id DESC LIMIT 1;
 IF FOUND THEN RETURN prior||jsonb_build_object('idempotent_replay',true);END IF;
 SELECT * INTO c FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton FOR UPDATE; SELECT * INTO p FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton FOR UPDATE;
 SELECT e.details INTO auth FROM population_top3000_202608_202609_v1.pipeline_event e WHERE e.event='RECOVERY_DELEGATION_QUALIFIED_V1' ORDER BY e.event_id DESC LIMIT 1;
 IF auth IS NULL OR c.owner_id IS DISTINCT FROM auth->>'owner_id' OR c.owner_fence IS DISTINCT FROM (auth->>'owner_fence')::bigint OR c.stop_generation IS DISTINCT FROM (auth->>'stop_generation')::bigint OR p.expected_owner_id IS DISTINCT FROM c.owner_id OR p.expected_owner_fence IS DISTINCT FROM c.owner_fence OR p.expected_stop_generation IS DISTINCT FROM c.stop_generation OR c.scope_sha256 IS DISTINCT FROM auth->>'scope_sha256' OR p.scope_sha256 IS DISTINCT FROM c.scope_sha256 OR c.stopped OR c.hold_reason IS NOT NULL OR NOT c.enabled OR NOT p.enabled OR c.admitted_until<=clock_timestamp() OR (auth->>'expires_at')::timestamptz<=clock_timestamp() THEN RAISE EXCEPTION 'QUALIFIED_CURRENT_DELEGATION_REQUIRED';END IF;
 IF population_top3000_202608_202609_v1.population_recovery_manifest_v1() IS DISTINCT FROM auth->'code_manifest' THEN RAISE EXCEPTION 'CODE_CHANGED_REQUALIFY_BEFORE_MAINTENANCE';END IF;
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE status='LEASED') OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_provider_lease WHERE released_at IS NULL) THEN RAISE EXCEPTION 'DRAINED_QUEUE_REQUIRED';END IF;
 IF NOT EXISTS(SELECT 1 FROM cron.job WHERE jobid=(auth->>'pipeline_job_id')::bigint AND database=current_database() AND btrim(command)='select population_top3000_202608_202609_v1.pipeline_tick_v1();') THEN RAISE EXCEPTION 'SCHEDULER_BINDING_CHANGED';END IF;
 result:=jsonb_build_object('operation_key',p_operation_key,'reason',p_reason,'state','BOUNDED_MAINTENANCE','owner_fence',c.owner_fence,'paused_at',clock_timestamp(),'resume_after',least(clock_timestamp()+make_interval(secs=>p_seconds),c.admitted_until,p.deadline),'code_manifest',auth->'code_manifest','automatic_resume_requires_unchanged_tested_code',true);
 PERFORM cron.alter_job((auth->>'pipeline_job_id')::bigint,active=>false);
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES('RECOVERY_MAINTENANCE_PAUSE_V1',result);
 RETURN result;
END $fn$;
REVOKE ALL ON FUNCTION population_top3000_202608_202609_v1.population_maintenance_pause_v1(text,text,integer) FROM PUBLIC,anon,authenticated;
