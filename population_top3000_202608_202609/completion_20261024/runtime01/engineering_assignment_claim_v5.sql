-- Exact event claim protocol. Executor fills only assignment_event_id, assignment_details_sha256 and automation_run_id.
-- This script does not change ownership, task states, source data, counters, admission or deadlines.
BEGIN;
SET LOCAL lock_timeout='15s'; SET LOCAL statement_timeout='30s';
DO $claim$
DECLARE
 assignment_event_id bigint:=0;
 assignment_details_sha256 text:='REPLACE_WITH_PINNED_ASSIGNMENT_DETAILS_SHA256';
 automation_run_id text:='REPLACE_WITH_ACTUAL_AUTOMATION_RUN_ID';
 a population_top3000_202608_202609_v1.acquisition_control%rowtype;
 p population_top3000_202608_202609_v1.pipeline_control%rowtype;
 task jsonb;ev bigint;prior jsonb;until_at timestamptz;
BEGIN
 IF assignment_event_id<=0 OR assignment_details_sha256!~'^[0-9a-f]{64}$'
 OR automation_run_id='REPLACE_WITH_ACTUAL_AUTOMATION_RUN_ID' OR length(automation_run_id) NOT BETWEEN 8 AND 128
 THEN RAISE EXCEPTION 'ENGINEERING_CLAIM_EXACT_ASSIGNMENT_AND_RUN_REQUIRED';END IF;
 PERFORM pg_advisory_xact_lock(2026083000261022);
 SELECT * INTO STRICT a FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton FOR UPDATE;
 SELECT * INTO STRICT p FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton FOR UPDATE;
 IF a.owner_id IS DISTINCT FROM 'top3000-completion-20261024-root-5db48b32d103' OR a.owner_fence IS DISTINCT FROM 3 OR a.stop_generation IS DISTINCT FROM 1
 OR a.enabled IS DISTINCT FROM true OR a.stopped IS DISTINCT FROM false OR a.hold_reason IS NOT NULL OR p.enabled IS DISTINCT FROM true
 OR p.expected_owner_id IS DISTINCT FROM a.owner_id OR p.expected_owner_fence IS DISTINCT FROM a.owner_fence OR p.expected_stop_generation IS DISTINCT FROM a.stop_generation
 OR a.scope_sha256 IS DISTINCT FROM '8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd' OR p.scope_sha256 IS DISTINCT FROM a.scope_sha256
 OR a.admitted_until IS NULL OR a.admitted_until<=clock_timestamp() OR p.deadline IS NULL OR p.deadline<=clock_timestamp()
 OR p.tick_count>=p.max_ticks OR p.shared_scheduler_verified IS DISTINCT FROM true
 OR p.budget_evidence#>>'{monetary_policy,authority_event_id}' IS DISTINCT FROM '9528'
 OR population_top3000_202608_202609_v1.pipeline_cost_admission_v2(p,0) IS DISTINCT FROM true
 THEN RAISE EXCEPTION 'ENGINEERING_CLAIM_CURRENT_AUTHORITY_REQUIRED';END IF;
 SELECT details INTO STRICT task FROM population_top3000_202608_202609_v1.pipeline_event
 WHERE event_id=assignment_event_id AND event='ENGINEERING_IMPLEMENTATION_ASSIGNMENT_V5';
 IF encode(sha256(convert_to(task::text,'UTF8')),'hex') IS DISTINCT FROM assignment_details_sha256
 OR task->>'owner_id' IS DISTINCT FROM a.owner_id OR task->>'owner_fence' IS DISTINCT FROM a.owner_fence::text
 OR task->>'stop_generation' IS DISTINCT FROM a.stop_generation::text OR task->>'scope_sha256' IS DISTINCT FROM a.scope_sha256
 OR task->>'authority_event_id' IS DISTINCT FROM '9528'
 OR task->>'assigned_executor' IS DISTINCT FROM 'automation:6adc2c96db348191bca7cf4eacfdac32'
 OR task->>'root_will_not_concurrently_modify' IS DISTINCT FROM 'true'
 OR nullif(task->>'assignment_id','') IS NULL OR nullif(task->>'exclusive_component','') IS NULL
 OR task->>'not_after' IS NULL OR (task->>'not_after')::timestamptz<=clock_timestamp()
 OR (task->>'maximum_elapsed_minutes')::integer NOT BETWEEN 1 AND 45
 OR (task->>'maximum_tool_calls')::integer NOT BETWEEN 1 AND 100
 OR task->>'maximum_elapsed_minutes' IS NULL OR task->>'maximum_tool_calls' IS NULL
 THEN RAISE EXCEPTION 'ENGINEERING_CLAIM_EXACT_DELEGATION_REQUIRED';END IF;
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_event e
 WHERE e.event='ENGINEERING_ASSIGNMENT_COMPLETED_V5' AND e.details->>'assignment_event_id'=assignment_event_id::text)
 THEN RAISE EXCEPTION 'ENGINEERING_ASSIGNMENT_ALREADY_COMPLETED';END IF;
 SELECT to_jsonb(e) INTO prior FROM population_top3000_202608_202609_v1.pipeline_event e
 WHERE e.event='ENGINEERING_ASSIGNMENT_CLAIMED_V5'
 AND (e.details->>'assignment_event_id'=assignment_event_id::text OR e.details->>'exclusive_component'=task->>'exclusive_component')
 AND NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_event done
  WHERE done.event IN('ENGINEERING_ASSIGNMENT_COMPLETED_V5','ENGINEERING_ASSIGNMENT_EFFECTS_RECONCILED_RELEASE_V5')
  AND done.details->>'claim_event_id'=e.event_id::text AND done.details->>'effects_known'='true')
 ORDER BY e.event_id DESC LIMIT 1;
 IF prior IS NOT NULL THEN
  IF prior#>>'{details,automation_run_id}'=automation_run_id AND prior#>>'{details,assignment_details_sha256}'=assignment_details_sha256
   AND prior#>>'{details,assignment_event_id}'=assignment_event_id::text
   AND (prior#>>'{details,expires_at}')::timestamptz>clock_timestamp()
  THEN RETURN; END IF;
  RAISE EXCEPTION 'ENGINEERING_ASSIGNMENT_ACTIVE_OR_UNRECONCILED_CLAIM %',prior->>'event_id';
 END IF;
 until_at:=least(clock_timestamp()+make_interval(mins=>(task->>'maximum_elapsed_minutes')::integer),
  (task->>'not_after')::timestamptz,a.admitted_until,p.deadline);
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details)
 VALUES('ENGINEERING_ASSIGNMENT_CLAIMED_V5',jsonb_build_object(
  'assignment_event_id',assignment_event_id,'assignment_id',task->>'assignment_id','assignment_details_sha256',assignment_details_sha256,
  'exclusive_component',task->>'exclusive_component','automation_run_id',automation_run_id,
  'executor','automation:6adc2c96db348191bca7cf4eacfdac32','owner_id',a.owner_id,'owner_fence',a.owner_fence,'stop_generation',a.stop_generation,
  'scope_sha256',a.scope_sha256,'authority_event_id',9528,'claimed_at',clock_timestamp(),'expires_at',until_at,
  'claim_scope','ONE_EXPLICIT_IMPLEMENTATION_ASSIGNMENT','expiry_is_not_effect_reconciliation',true,
  'no_owner_or_counter_or_deadline_change',true,'no_source_calls_performed',true)) RETURNING event_id INTO ev;
END;
$claim$;
SELECT event_id,at,details FROM population_top3000_202608_202609_v1.pipeline_event
WHERE event='ENGINEERING_ASSIGNMENT_CLAIMED_V5' ORDER BY event_id DESC LIMIT 1;
COMMIT;
