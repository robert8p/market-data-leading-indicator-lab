-- Scoped acquisition wrapper; the exact v6 helper bundle and original source inputs stay unchanged.
CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.ctx07_recovery_acquire_v7(p_attempt uuid,p_token text)
RETURNS jsonb LANGUAGE plpgsql SET search_path TO pg_catalog AS $f$
DECLARE tid uuid;t population_top3000_202608_202609_v1.pipeline_task%rowtype;c jsonb;n bigint;admission jsonb;BEGIN
 PERFORM pg_advisory_xact_lock(2026083000261022);
 tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);
 SELECT * INTO STRICT t FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=tid;
 IF t.payload->>'kind' IS DISTINCT FROM 'CTX07_SOURCE_RECOVERY_V6' THEN RETURN population_top3000_202608_202609_v1.ctx07_recovery_acquire_v6(p_attempt,p_token);END IF;
 c:=population_top3000_202608_202609_v1.ctx07_recovery_context_v6(tid);
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.ctx07_recovery_body_v6 WHERE request_key=t.partition_key) THEN RETURN population_top3000_202608_202609_v1.ctx07_recovery_acquire_v6(p_attempt,p_token);END IF;
 SELECT count(*) INTO n FROM population_top3000_202608_202609_v1.pipeline_attempt a JOIN population_top3000_202608_202609_v1.pipeline_task q USING(task_id) WHERE q.payload->>'kind'='CTX07_SOURCE_RECOVERY_V6' AND a.request_id IS NOT NULL;
 SELECT details INTO admission FROM population_top3000_202608_202609_v1.pipeline_event WHERE event='CTX07_V7_EDGE_ADMISSION' AND details->>'owner_id'='top3000-completion-20261024-root-5db48b32d103' AND details->>'owner_fence'='3' AND details->>'stop_generation'='1' AND details->>'scope_sha256'='8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd' AND details->>'maximum_cumulative_edge_invocations'='114' ORDER BY event_id DESC LIMIT 1;
 IF n>=114 OR admission IS NULL OR nullif(admission->>'not_after','') IS NULL OR (admission->>'not_after')::timestamptz<=clock_timestamp() THEN
 RETURN jsonb_build_object('state','BLOCKED','blocker_code','CTX07_V6_EDGE_ADMISSION_REVIEW','resumption_condition',jsonb_build_object('required_action','Measured independent edge-only admission review; preserve cumulative114cap and original64sourcecap','cumulative_edge_invocations',n,'maximum_cumulative_edge_invocations',114,'new_provider_request_allowed',false));END IF;
 RETURN population_top3000_202608_202609_v1.ctx07_recovery_acquire_v6(p_attempt,p_token);
END;$f$;
REVOKE ALL ON FUNCTION population_top3000_202608_202609_v1.ctx07_recovery_acquire_v7(uuid,text) FROM PUBLIC,anon,authenticated;
