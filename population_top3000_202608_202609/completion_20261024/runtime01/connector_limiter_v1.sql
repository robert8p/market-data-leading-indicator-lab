DO $preimage$ BEGIN
 IF encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.pipeline_provider_reserve_v1(uuid,text,text,bigint,numeric)'::regprocedure),'UTF8')),'hex') IS DISTINCT FROM '0a3561ae96116daecfc4c8d663044912eb934dd2370724ed539df892de7407be'
 OR encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.population_recovery_request_v1(text,text)'::regprocedure),'UTF8')),'hex') IS DISTINCT FROM 'c483ec93fe5467624ad434b7b6368e2f12346770dbe9130c0fa5d5a5ab828b98'
 THEN RAISE EXCEPTION 'CONNECTOR_LIMITER_RUNTIME_PREIMAGE_CHANGED';END IF;
END $preimage$;
DO $controller_guard$ BEGIN
 IF NOT pg_try_advisory_xact_lock(2026083000261022) THEN RAISE EXCEPTION 'CONNECTOR_INSTALL_CONTROLLER_BUSY';END IF;
 PERFORM 1 FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton FOR UPDATE;
 PERFORM 1 FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton FOR UPDATE;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.acquisition_control a JOIN population_top3000_202608_202609_v1.pipeline_control p ON p.singleton=a.singleton
 WHERE a.owner_id='top3000-completion-20261024-root-5db48b32d103' AND a.owner_fence=3 AND a.stop_generation=1 AND NOT a.stopped
 AND p.expected_owner_id=a.owner_id AND p.expected_owner_fence=a.owner_fence AND p.expected_stop_generation=a.stop_generation
 AND population_top3000_202608_202609_v1.pipeline_cost_admission_v2(p,0)) THEN RAISE EXCEPTION 'CONNECTOR_INSTALL_CURRENT_CONTROLLER_REQUIRED';END IF;
END $controller_guard$;
-- Private mission-scoped direct connector lease. Root executes reservations serially.
-- Counts logical calls. Connector-internal physical HTTP count remains unknown.
CREATE TABLE population_top3000_202608_202609_v1.connector_probe_lease_v1 (
 request_key text PRIMARY KEY CHECK(length(request_key) BETWEEN 1 AND 160),
 owner_id text NOT NULL,owner_fence bigint NOT NULL,stop_generation bigint NOT NULL,
 lane text NOT NULL CHECK(lane IN('REF01','financial','prices','options','context')),
 provider text NOT NULL CHECK(provider='Massive'),request_sha256 text NOT NULL CHECK(request_sha256~'^[0-9a-f]{64}$'),
 reserved_bytes bigint NOT NULL CHECK(reserved_bytes BETWEEN 1 AND 8388608),
 reserved_usd numeric(14,6) NOT NULL CHECK(reserved_usd>=0),
 reserved_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 released_at timestamptz,receipt jsonb,
 CHECK((released_at IS NULL AND receipt IS NULL) OR (released_at IS NOT NULL AND jsonb_typeof(receipt)='object'))
);
ALTER TABLE population_top3000_202608_202609_v1.connector_probe_lease_v1 ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON population_top3000_202608_202609_v1.connector_probe_lease_v1 FROM public,anon,authenticated;

CREATE FUNCTION population_top3000_202608_202609_v1.connector_probe_reserve_v1(
 p_owner text,p_fence bigint,p_stop bigint,p_key text,p_lane text,p_request_sha256 text,p_max_bytes bigint,p_incremental_usd numeric)
RETURNS jsonb LANGUAGE plpgsql SET search_path=pg_catalog AS $f$
DECLARE a population_top3000_202608_202609_v1.acquisition_control%rowtype;
 p population_top3000_202608_202609_v1.pipeline_control%rowtype;
 old population_top3000_202608_202609_v1.connector_probe_lease_v1%rowtype;
 lane_limit integer;lane_used integer;relation_bytes bigint;
BEGIN
 IF NOT pg_try_advisory_xact_lock(2026083000261022) THEN RETURN jsonb_build_object('state','BUSY');END IF;
 SELECT * INTO STRICT a FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton FOR UPDATE;
 SELECT * INTO STRICT p FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton FOR UPDATE;
 IF p_owner IS NULL OR p_fence IS NULL OR p_stop IS NULL OR a.owner_id IS DISTINCT FROM p_owner OR a.owner_fence IS DISTINCT FROM p_fence
  OR a.stop_generation IS DISTINCT FROM p_stop OR a.stopped IS DISTINCT FROM false
  OR p.expected_owner_id IS DISTINCT FROM a.owner_id OR p.expected_owner_fence IS DISTINCT FROM a.owner_fence OR p.expected_stop_generation IS DISTINCT FROM a.stop_generation
  OR a.enabled IS DISTINCT FROM true OR p.enabled IS DISTINCT FROM true OR a.hold_reason IS NOT NULL
  OR a.admitted_until IS NULL OR a.admitted_until<=clock_timestamp() OR p.deadline IS NULL OR p.deadline<=clock_timestamp()
  OR p.shared_scheduler_verified IS DISTINCT FROM true OR p.tick_count>=p.max_ticks
  OR a.scope_sha256 IS DISTINCT FROM '8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd' OR p.scope_sha256 IS DISTINCT FROM a.scope_sha256
 THEN RAISE EXCEPTION 'CONNECTOR_CONTROLLER_STOP_SCOPE_ADMISSION_REJECTED';END IF;
 IF p_key IS NULL OR length(p_key) NOT BETWEEN 1 AND 160 OR p_lane IS NULL OR p_lane NOT IN('REF01','financial','prices','options','context')
  OR p_request_sha256 IS NULL OR p_request_sha256 !~ '^[0-9a-f]{64}$'
  OR p_max_bytes IS NULL OR p_max_bytes NOT BETWEEN 1 AND 8388608
  OR population_top3000_202608_202609_v1.pipeline_cost_admission_v2(p,p_incremental_usd) IS DISTINCT FROM true
  OR p_incremental_usd>=100000000 OR round(p_incremental_usd,6) IS DISTINCT FROM p_incremental_usd
  OR (p.budget_evidence->'allowed_providers' ? 'Massive') IS DISTINCT FROM true
  OR (p.budget_evidence->>'provider_admissions_allowed')::boolean IS DISTINCT FROM true
 THEN RAISE EXCEPTION 'CONNECTOR_ARGUMENT_OR_COST_ADMISSION_REJECTED';END IF;
 SELECT * INTO old FROM population_top3000_202608_202609_v1.connector_probe_lease_v1 WHERE request_key=p_key;
 IF FOUND THEN
  IF old.request_sha256 IS DISTINCT FROM p_request_sha256 OR old.lane IS DISTINCT FROM p_lane
   OR old.owner_id IS DISTINCT FROM p_owner OR old.owner_fence IS DISTINCT FROM p_fence OR old.stop_generation IS DISTINCT FROM p_stop
   OR old.reserved_bytes IS DISTINCT FROM p_max_bytes OR old.reserved_usd IS DISTINCT FROM p_incremental_usd
  THEN RAISE EXCEPTION 'CONNECTOR_REQUEST_KEY_CONFLICT';END IF;
  RETURN jsonb_build_object('state',CASE WHEN old.released_at IS NULL THEN 'ALREADY_RESERVED_DO_NOT_REISSUE' ELSE 'ALREADY_RECONCILED_DO_NOT_REISSUE' END,'request_key',p_key,'receipt',old.receipt);
 END IF;
 IF a.next_provider_at>clock_timestamp() THEN RETURN jsonb_build_object('state','WAIT','not_before',a.next_provider_at);END IF;
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.connector_probe_lease_v1 WHERE released_at IS NULL)
  OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_provider_lease WHERE released_at IS NULL)
  OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.acquisition_job WHERE status='LEASED' AND lease_until>clock_timestamp())
 THEN RETURN jsonb_build_object('state','WAIT_UNRECONCILED_PROVIDER_OPERATION');END IF;
 lane_limit:=CASE p_lane WHEN 'REF01' THEN 800 WHEN 'financial' THEN 800 WHEN 'prices' THEN 100 WHEN 'options' THEN 200 WHEN 'context' THEN 100 END;
 SELECT count(*) INTO lane_used FROM population_top3000_202608_202609_v1.connector_probe_lease_v1 WHERE lane=p_lane;
 SELECT coalesce(sum(pg_total_relation_size(r.oid)),0) INTO relation_bytes FROM pg_class r JOIN pg_namespace n ON n.oid=r.relnamespace WHERE n.nspname='population_top3000_202608_202609_v1' AND r.relkind IN('r','m');
 IF lane_used>=lane_limit OR a.provider_requests>=a.max_provider_requests OR a.stored_bytes+a.reserved_bytes+p_max_bytes>a.max_source_bytes
  OR nullif(p.budget_evidence#>>'{resource_limits,max_relation_bytes}','') IS NULL
  OR relation_bytes>=(p.budget_evidence#>>'{resource_limits,max_relation_bytes}')::bigint
 THEN RETURN jsonb_build_object('state','FINITE_RESOURCE_BOUND');END IF;
 INSERT INTO population_top3000_202608_202609_v1.connector_probe_lease_v1(request_key,owner_id,owner_fence,stop_generation,lane,provider,request_sha256,reserved_bytes,reserved_usd)
 VALUES(p_key,p_owner,p_fence,p_stop,p_lane,'Massive',p_request_sha256,p_max_bytes,p_incremental_usd);
 UPDATE population_top3000_202608_202609_v1.acquisition_control SET provider_requests=provider_requests+1,reserved_bytes=reserved_bytes+p_max_bytes,next_provider_at=clock_timestamp()+interval '15 seconds',updated_at=clock_timestamp() WHERE singleton;
 UPDATE population_top3000_202608_202609_v1.pipeline_control SET reserved_request_usd=reserved_request_usd+p_incremental_usd WHERE singleton;
 RETURN jsonb_build_object('state','RESERVED','request_key',p_key,'request_sha256',p_request_sha256,'provider','Massive','max_bytes',p_max_bytes,'logical_provider_calls_reserved',1,'physical_http_count','UNKNOWN_CONNECTOR_INTERNALS','automatic_retry',false);
END $f$;

CREATE FUNCTION population_top3000_202608_202609_v1.connector_probe_reconcile_v1(
 p_owner text,p_fence bigint,p_stop bigint,p_key text,p_receipt jsonb,p_cooldown_seconds integer DEFAULT 15)
RETURNS jsonb LANGUAGE plpgsql SET search_path=pg_catalog AS $f$
DECLARE a population_top3000_202608_202609_v1.acquisition_control%rowtype;
 p population_top3000_202608_202609_v1.pipeline_control%rowtype;
 l population_top3000_202608_202609_v1.connector_probe_lease_v1%rowtype;v_receipt jsonb;
BEGIN
 SELECT * INTO STRICT a FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton FOR UPDATE;
 SELECT * INTO STRICT p FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton FOR UPDATE;
 SELECT * INTO STRICT l FROM population_top3000_202608_202609_v1.connector_probe_lease_v1 WHERE request_key=p_key FOR UPDATE;
 IF p_owner IS NULL OR p_fence IS NULL OR p_stop IS NULL OR a.owner_id IS DISTINCT FROM p_owner OR a.owner_fence IS DISTINCT FROM p_fence OR a.stop_generation IS DISTINCT FROM p_stop
  OR p.expected_owner_id IS DISTINCT FROM a.owner_id OR p.expected_owner_fence IS DISTINCT FROM a.owner_fence OR p.expected_stop_generation IS DISTINCT FROM a.stop_generation
  OR l.owner_id IS DISTINCT FROM p_owner OR l.owner_fence IS DISTINCT FROM p_fence OR l.stop_generation IS DISTINCT FROM p_stop
 THEN RAISE EXCEPTION 'CONNECTOR_RECONCILE_OWNER_STOP_CHANGED';END IF;
 IF jsonb_typeof(p_receipt) IS DISTINCT FROM 'object' OR p_receipt->>'state' IS NULL OR p_receipt->>'state' NOT IN('SUCCESS','HTTP_ERROR','CONFIRMED_NOT_DISPATCHED')
  OR p_receipt->>'request_sha256' IS DISTINCT FROM l.request_sha256
  OR p_receipt->>'physical_http_count' IS DISTINCT FROM 'UNKNOWN_CONNECTOR_INTERNALS'
  OR p_cooldown_seconds IS NULL OR p_cooldown_seconds NOT BETWEEN 15 AND 3600
  OR p_receipt->>'response_sha256' IS NULL OR p_receipt->>'response_sha256' !~ '^[0-9a-f]{64}$'
  OR p_receipt->>'response_bytes' IS NULL OR p_receipt->>'response_bytes' !~ '^[0-9]{1,8}$'
  OR octet_length(p_receipt::text)>16384
 THEN RAISE EXCEPTION 'CONNECTOR_RECEIPT_INVALID_OR_UNKNOWN_EFFECT';END IF;
 IF (p_receipt->>'response_bytes')::bigint>l.reserved_bytes THEN RAISE EXCEPTION 'CONNECTOR_RESPONSE_EXCEEDED_RESERVATION';END IF;
 IF p_receipt->>'state'='HTTP_ERROR' AND (p_receipt->>'http_status' IS NULL OR p_receipt->>'http_status' !~ '^[45][0-9]{2}$') THEN RAISE EXCEPTION 'CONNECTOR_HTTP_STATUS_REQUIRED';END IF;
 IF p_receipt->>'http_status'='429' AND p_cooldown_seconds<60 THEN RAISE EXCEPTION 'CONNECTOR_RATE_LIMIT_COOLDOWN_REQUIRED';END IF;
 v_receipt:=p_receipt||jsonb_build_object('incremental_cost_state','UNINVOICED_RESERVATION_RETAINED','reserved_incremental_usd',l.reserved_usd,'invoice_reconciled',false);
 IF l.released_at IS NOT NULL THEN
  IF l.receipt IS DISTINCT FROM v_receipt THEN RAISE EXCEPTION 'CONNECTOR_RECEIPT_CONFLICT';END IF;
  RETURN jsonb_build_object('state','ALREADY_RECONCILED','request_key',p_key);
 END IF;
 IF a.reserved_bytes<l.reserved_bytes OR p.reserved_request_usd<l.reserved_usd THEN RAISE EXCEPTION 'CONNECTOR_RESERVATION_COUNTER_MISMATCH';END IF;
 UPDATE population_top3000_202608_202609_v1.connector_probe_lease_v1 SET released_at=clock_timestamp(),receipt=v_receipt WHERE request_key=p_key;
 UPDATE population_top3000_202608_202609_v1.acquisition_control SET reserved_bytes=reserved_bytes-l.reserved_bytes,next_provider_at=greatest(next_provider_at,clock_timestamp()+make_interval(secs=>p_cooldown_seconds)),updated_at=clock_timestamp() WHERE singleton;
 -- Keep the conservative reservation outstanding until explicit invoice/charge reconciliation.
 -- Releasing the transport lease does not turn a maximum allowance into incurred cost.
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES('CONNECTOR_PROBE_RECONCILED_V1',jsonb_build_object('request_key',p_key,'lane',l.lane,'provider',l.provider,'owner_id',p_owner,'owner_fence',p_fence,'stop_generation',p_stop,'receipt',v_receipt,'invoice_reconciled',false));
 RETURN jsonb_build_object('state','RECONCILED','request_key',p_key,'next_provider_at',(SELECT next_provider_at FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton),'provider_counter_preserved',true);
END $f$;
REVOKE ALL ON FUNCTION population_top3000_202608_202609_v1.connector_probe_reserve_v1(text,bigint,bigint,text,text,text,bigint,numeric),population_top3000_202608_202609_v1.connector_probe_reconcile_v1(text,bigint,bigint,text,jsonb,integer) FROM public,anon,authenticated;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.pipeline_provider_reserve_v1(p_attempt uuid, p_token text, p_provider text, p_max_bytes bigint, p_max_incremental_usd numeric)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'pg_catalog'
AS $function$
declare tid uuid;c population_top3000_202608_202609_v1.acquisition_control%rowtype;p population_top3000_202608_202609_v1.pipeline_control%rowtype;
begin
tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);
if p_provider is null or p_max_bytes is null or p_max_bytes<=0 or p_max_bytes>33554432 or p_max_incremental_usd is null or p_max_incremental_usd<0 then raise exception 'RESERVATION_INVALID';end if;
select * into c from population_top3000_202608_202609_v1.acquisition_control where singleton;
select * into p from population_top3000_202608_202609_v1.pipeline_control where singleton;
if exists(select 1 from population_top3000_202608_202609_v1.pipeline_provider_lease where attempt_id=p_attempt) then raise exception 'REQUEST_ALREADY_RESERVED_FOR_ATTEMPT';end if;
if c.next_provider_at>clock_timestamp() then return jsonb_build_object('state','WAIT','not_before',c.next_provider_at);end if;
if exists(select 1 from population_top3000_202608_202609_v1.connector_probe_lease_v1 where released_at is null)
 or exists(select 1 from population_top3000_202608_202609_v1.pipeline_provider_lease where released_at is null)
 or exists(select 1 from population_top3000_202608_202609_v1.acquisition_job where status='LEASED' and lease_until>clock_timestamp()) then return jsonb_build_object('state','WAIT','not_before',clock_timestamp()+interval '5 seconds');end if;
if p.budget_verified_at is null or p.budget_evidence is null or coalesce((p.budget_evidence->>'provider_admissions_allowed')::boolean,false) is not true then return '{"state":"BUDGET_HOLD","reason":"CURRENT_MISSION_BILLING_BASELINE_REQUIRED"}';end if;
if (coalesce(p.budget_evidence->'allowed_providers','[]'::jsonb) ? p_provider) is not true then
 return jsonb_build_object('state','BUDGET_HOLD','reason','PROVIDER_NOT_IN_CURRENT_ADMISSION');
end if;
if p_max_bytes>8388608 then return jsonb_build_object('state','BUDGET_HOLD','reason','PER_RESPONSE_BYTE_LIMIT');end if;
if nullif(p.budget_evidence#>>'{resource_limits,max_relation_bytes}','') is null
 or (select coalesce(sum(pg_total_relation_size(r.oid)),0) from pg_class r join pg_namespace n on n.oid=r.relnamespace
 where n.nspname='population_top3000_202608_202609_v1' and r.relkind in('r','m')) >= (p.budget_evidence#>>'{resource_limits,max_relation_bytes}')::bigint
then return jsonb_build_object('state','BUDGET_HOLD','reason','MISSION_RELATION_STORAGE_BOUND');end if;
if c.provider_requests>=c.max_provider_requests or c.stored_bytes+c.reserved_bytes+p_max_bytes>c.max_source_bytes or population_top3000_202608_202609_v1.pipeline_cost_admission_v2(p,p_max_incremental_usd) is distinct from true then return '{"state":"BUDGET_HOLD","reason":"CONSERVATIVE_TOTAL_OR_RESOURCE_LIMIT"}';end if;
insert into population_top3000_202608_202609_v1.pipeline_provider_lease(attempt_id,provider,reserved_bytes,reserved_usd)values(p_attempt,p_provider,p_max_bytes,p_max_incremental_usd);
update population_top3000_202608_202609_v1.acquisition_control set provider_requests=provider_requests+1,reserved_bytes=reserved_bytes+p_max_bytes,next_provider_at=clock_timestamp()+case when p_provider='Massive' then interval '15 seconds' else interval '500 milliseconds' end where singleton;
update population_top3000_202608_202609_v1.pipeline_control set reserved_request_usd=reserved_request_usd+p_max_incremental_usd where singleton;
return jsonb_build_object('state','RESERVED','max_bytes',p_max_bytes,'max_incremental_usd',p_max_incremental_usd);
end $function$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.population_recovery_request_v1(p_request_key text DEFAULT NULL::text, p_reason text DEFAULT 'scheduled-recovery-check'::text)
 RETURNS jsonb
 LANGUAGE plpgsql
 SET search_path TO 'pg_catalog'
AS $function$
DECLARE c population_top3000_202608_202609_v1.acquisition_control%rowtype; p population_top3000_202608_202609_v1.pipeline_control%rowtype; auth jsonb; auth_id bigint; result jsonb; previous jsonb; state text; facts jsonb; manifest jsonb; j record; eng jsonb; eng_id bigint; pause jsonb; restart_count integer; last_restart timestamptz; ready_count integer; database_only boolean; bytes bigint; lim bigint; wake jsonb;
BEGIN
 IF length(coalesce(p_request_key,''))>160 OR length(coalesce(p_reason,''))>1000 THEN RAISE EXCEPTION 'RECOVERY_ARGUMENT_TOO_LONG';END IF;
 IF NOT pg_try_advisory_xact_lock(2026083000261022) THEN RETURN jsonb_build_object('state','BUSY','writes',false);END IF;
 IF p_request_key IS NOT NULL THEN SELECT e.details->'result' INTO previous FROM population_top3000_202608_202609_v1.pipeline_event e WHERE e.event='RECOVERY_REQUEST_RESULT_V1' AND e.details->>'request_key'=p_request_key ORDER BY e.event_id DESC LIMIT 1; IF FOUND THEN RETURN previous||jsonb_build_object('idempotent_replay',true);END IF;END IF;
 SELECT * INTO c FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton FOR UPDATE;
 SELECT * INTO p FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton FOR UPDATE;
 SELECT e.event_id,e.details INTO auth_id,auth FROM population_top3000_202608_202609_v1.pipeline_event e WHERE e.event='RECOVERY_DELEGATION_QUALIFIED_V1' ORDER BY e.event_id DESC LIMIT 1;
 SELECT jobid,active INTO j FROM cron.job WHERE jobid=(auth->>'pipeline_job_id')::bigint AND database=current_database() AND btrim(command)='select population_top3000_202608_202609_v1.pipeline_tick_v1();';
 SELECT e.details INTO pause FROM population_top3000_202608_202609_v1.pipeline_event e WHERE e.event='RECOVERY_MAINTENANCE_PAUSE_V1' ORDER BY e.event_id DESC LIMIT 1;
 SELECT e.event_id,e.details INTO eng_id,eng FROM population_top3000_202608_202609_v1.pipeline_event e WHERE e.event='ROOT_SCOPED_IMPLEMENTATION_ACTIVE' ORDER BY e.event_id DESC LIMIT 1;
 SELECT count(*),max(e.at) INTO restart_count,last_restart FROM population_top3000_202608_202609_v1.pipeline_event e LEFT JOIN population_top3000_202608_202609_v1.pipeline_event q ON q.event_id::text=e.details->>'policy_event_id' WHERE e.event='RECOVERY_WAKE_COMPLETED_V1' AND (e.details->>'policy_event_id'=auth_id::text OR (auth->>'authority_event_id' IS NOT NULL AND q.details->>'authority_event_id'=auth->>'authority_event_id'));
 SELECT count(*),coalesce(bool_and(a.enabled AND a.resource_class='DATABASE' AND a.function_sha256=encode(sha256(convert_to(pg_get_functiondef(a.function_oid),'UTF8')),'hex') AND a.reviewed_test_evidence IS NOT NULL AND a.reviewed_test_evidence<>'{}'::jsonb),false) INTO ready_count,database_only FROM population_top3000_202608_202609_v1.pipeline_task t LEFT JOIN population_top3000_202608_202609_v1.pipeline_adapter a USING(family_id,stage) WHERE t.status='READY' AND t.not_before<=clock_timestamp() AND t.executions<t.max_executions AND NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_dependency d JOIN population_top3000_202608_202609_v1.pipeline_task z ON z.task_id=d.prerequisite_task_id WHERE d.task_id=t.task_id AND z.status<>'COMPLETE');
 manifest:=population_top3000_202608_202609_v1.population_recovery_manifest_v1();
 lim:=(p.budget_evidence#>>'{resource_limits,max_relation_bytes}')::bigint;
 SELECT coalesce(sum(pg_total_relation_size(t.oid)),0)::bigint INTO bytes FROM pg_class t JOIN pg_namespace n ON n.oid=t.relnamespace WHERE n.nspname='population_top3000_202608_202609_v1' AND t.relkind IN('r','m');
 facts:=jsonb_build_object('qualified',auth IS NOT NULL,'owner_stop_matches',c.owner_id IS NOT DISTINCT FROM auth->>'owner_id' AND c.owner_fence IS NOT DISTINCT FROM (auth->>'owner_fence')::bigint AND c.stop_generation IS NOT DISTINCT FROM (auth->>'stop_generation')::bigint AND p.expected_owner_id IS NOT DISTINCT FROM c.owner_id AND p.expected_owner_fence IS NOT DISTINCT FROM c.owner_fence AND p.expected_stop_generation IS NOT DISTINCT FROM c.stop_generation,'scope_matches',c.scope_sha256 IS NOT DISTINCT FROM auth->>'scope_sha256' AND p.scope_sha256 IS NOT DISTINCT FROM c.scope_sha256,'stopped',c.stopped,'controls_enabled',c.enabled AND p.enabled AND c.hold_reason IS NULL,'admission_valid',(auth->>'expires_at')::timestamptz>clock_timestamp() AND c.admitted_until>clock_timestamp() AND p.deadline>clock_timestamp(),'ticks_available',p.tick_count<p.max_ticks,'budget_scheduler_valid',p.shared_scheduler_verified AND population_top3000_202608_202609_v1.pipeline_cost_admission_v2(p,0),'scheduler_binding_valid',j.jobid IS NOT NULL,'scheduler_active',j.active,'maintenance_wait',coalesce((pause->>'resume_after')::timestamptz>clock_timestamp(),false),'engineering_active',coalesce(eng_id>coalesce((auth->>'engineering_event_id')::bigint,0) AND (eng->>'next_checkpoint_due_at' IS NULL OR (eng->>'next_checkpoint_due_at')::timestamptz>clock_timestamp()),false),'active_operations',EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.connector_probe_lease_v1 WHERE released_at IS NULL) OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE status='LEASED') OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_attempt WHERE outcome='RUNNING') OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_provider_lease WHERE released_at IS NULL),'competing_session',EXISTS(SELECT 1 FROM pg_stat_activity sa WHERE sa.pid<>pg_backend_pid() AND sa.state<>'idle' AND sa.query ILIKE '%population_top3000_202608_202609_v1%'),'code_matches',manifest IS NOT DISTINCT FROM auth->'code_manifest','resources_valid',lim>0 AND bytes<lim,'runnable_count',ready_count,'database_only_reviewed',database_only,'restart_allowance',restart_count<least(coalesce((auth->>'max_restarts')::integer,0),3),'cooldown_and_progress',last_restart IS NULL OR (last_restart<=clock_timestamp()-interval '10 minutes' AND p.last_useful_progress_at>last_restart));
 state:=population_top3000_202608_202609_v1.population_recovery_decision_v1(facts);
 IF state='ELIGIBLE_FOR_GUARDED_WAKE' THEN
  wake:=population_top3000_202608_202609_v1.pipeline_wake_v1(c.owner_id,c.owner_fence,'Qualified delegated recovery: '||coalesce(p_reason,'scheduled check'));state:='RESTORED';
  INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES('RECOVERY_WAKE_COMPLETED_V1',jsonb_build_object('policy_event_id',auth_id,'actor','qualified_recovery_delegate','runtime_owner',c.owner_id,'owner_fence',c.owner_fence,'stop_generation',c.stop_generation,'request_key',p_request_key,'reason',p_reason,'prior_last_useful_progress',p.last_useful_progress_at,'ready_tasks',ready_count,'result',wake,'quality_gates_and_limits_unchanged',true));
 END IF;
 result:=jsonb_build_object('state',state,'checked_at',clock_timestamp(),'policy_event_id',auth_id,'owner_fence',c.owner_fence,'last_useful_progress',p.last_useful_progress_at,'restart_performed',state='RESTORED','checks',facts);
 IF state IN('AUTHORISATION_EXPIRED','USER_STOP','OWNER_OR_STOP_CHANGED') THEN FOR j IN SELECT jobid FROM cron.job WHERE jobname='top3000-qualified-recovery-v1' AND database=current_database() AND command LIKE '%population_top3000_202608_202609_v1.population_recovery_request_v1%' AND active LOOP PERFORM cron.alter_job(j.jobid,active=>false);END LOOP;END IF;
 IF p_request_key IS NOT NULL THEN INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES('RECOVERY_REQUEST_RESULT_V1',jsonb_build_object('request_key',p_request_key,'reason',p_reason,'result',result));
 ELSIF state IS DISTINCT FROM (SELECT e.details->>'state' FROM population_top3000_202608_202609_v1.pipeline_event e WHERE e.event='RECOVERY_CHECK_STATUS_V1' ORDER BY e.event_id DESC LIMIT 1) THEN INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES('RECOVERY_CHECK_STATUS_V1',result);END IF;
 RETURN result;
END $function$;

