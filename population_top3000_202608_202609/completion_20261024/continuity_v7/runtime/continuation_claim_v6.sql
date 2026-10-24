CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.continuation_claim_v6(p_owner text, p_fence bigint, p_stop bigint, p_executor text, p_run_id text)
 RETURNS jsonb
 LANGUAGE plpgsql
 SET search_path TO 'pg_catalog'
AS $function$
DECLARE ctl jsonb;q population_top3000_202608_202609_v1.continuation_clear_queue_v6%rowtype;tok text;ev bigint;
BEGIN
 ctl:=population_top3000_202608_202609_v1.continuation_guard_v6(p_owner,p_fence,p_stop);
 IF p_executor IS DISTINCT FROM 'automation:6adc2c96db348191bca7cf4eacfdac32' OR length(coalesce(p_run_id,'')) NOT BETWEEN 8 AND 128 THEN RAISE EXCEPTION 'CONTINUATION_EXECUTOR_REQUIRED';END IF;

 -- Unexecuted READY snapshots can become obsolete as independent work advances. They are explicitly superseded, never counted complete.
 UPDATE population_top3000_202608_202609_v1.continuation_clear_queue_v6 taskpin SET status='SUPERSEDED',last_receipt=jsonb_build_object('effects_known',true,'reason','PINNED_SOURCE_TASK_CHANGED_BEFORE_CLAIM','implementation_executed',false),updated_at=clock_timestamp()
 WHERE taskpin.status IN('READY','NEEDS_REVIEW') AND taskpin.kind<>'CLAIM_RECONCILIATION' AND EXISTS(
 SELECT 1 FROM jsonb_array_elements(taskpin.input_manifest) m LEFT JOIN population_top3000_202608_202609_v1.pipeline_task t ON t.task_id=(m->>'task_id')::uuid
 WHERE m ? 'task_id' AND (t.task_id IS NULL OR t.status IS DISTINCT FROM 'BLOCKED' OR t.blocker_code IS DISTINCT FROM taskpin.blocker_code
 OR m->'input_versions' IS DISTINCT FROM t.input_versions OR m->'checkpoint' IS DISTINCT FROM t.checkpoint
 OR m->>'payload_sha256' IS DISTINCT FROM encode(sha256(convert_to(t.payload::text,'UTF8')),'hex')
 OR m->>'executions' IS DISTINCT FROM t.executions::text OR m->>'cumulative_failures' IS DISTINCT FROM t.cumulative_failures::text));
 -- Unmatched expired queue claims remain narrow; never block unrelated families.
 SELECT x.* INTO q FROM population_top3000_202608_202609_v1.continuation_clear_queue_v6 x WHERE status IN('READY','NEEDS_REVIEW') AND not_after>clock_timestamp() AND attempts<max_attempts
 AND owner_id=p_owner AND owner_fence=p_fence AND stop_generation=p_stop
 AND NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.continuation_clear_queue_v6 y WHERE y.family_id=x.family_id AND y.status='CLAIMED')
 AND NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task t WHERE t.family_id=x.family_id AND t.status='LEASED')
 AND NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_event e WHERE e.event='ENGINEERING_ASSIGNMENT_CLAIMED_V5' AND e.details->>'owner_id'=p_owner AND e.details->>'exclusive_component' LIKE '%'||x.family_id||'%' AND NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_event done WHERE done.event IN('ENGINEERING_ASSIGNMENT_COMPLETED_V5','ENGINEERING_ASSIGNMENT_EFFECTS_RECONCILED_RELEASE_V5') AND done.details->>'claim_event_id'=e.event_id::text AND done.details->>'effects_known'='true'))

 ORDER BY CASE x.kind WHEN 'OPERATIONAL_REVIEW' THEN 0 ELSE 1 END,
 coalesce((SELECT max(hist.claimed_at) FROM population_top3000_202608_202609_v1.continuation_clear_queue_v6 hist WHERE hist.family_id=x.family_id),'-infinity'::timestamptz),
 x.created_at,x.clearing_id FOR UPDATE SKIP LOCKED LIMIT 1;
 IF NOT FOUND THEN RETURN jsonb_build_object('state','NO_ELIGIBLE_CLEARING_ASSIGNMENT','external_groups',(SELECT count(*) FROM population_top3000_202608_202609_v1.continuation_clear_queue_v6 WHERE status='BLOCKED_EXTERNAL'),'unreconciled_claims',(SELECT count(*) FROM population_top3000_202608_202609_v1.continuation_clear_queue_v6 WHERE status='CLAIMED'));END IF;
 tok:=replace(gen_random_uuid()::text,'-','')||replace(gen_random_uuid()::text,'-','');
 UPDATE population_top3000_202608_202609_v1.continuation_clear_queue_v6 SET status='CLAIMED',attempts=attempts+1,claim_token_sha256=encode(sha256(convert_to(tok,'UTF8')),'hex'),claimed_at=clock_timestamp(),claim_expires_at=least(clock_timestamp()+interval '30minutes',not_after),updated_at=clock_timestamp() WHERE clearing_id=q.clearing_id;
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES('ENGINEERING_CLEARING_CLAIMED_V6',ctl||jsonb_build_object('clearing_id',q.clearing_id,'assignment_event_id',q.assignment_event_id,'executor',p_executor,'run_id',p_run_id,'input_sha256',q.input_sha256,'claim_expires_at',least(clock_timestamp()+interval '30minutes',q.not_after))) RETURNING event_id INTO ev;
 RETURN jsonb_build_object('state','CLAIMED','clearing_id',q.clearing_id,'claim_event_id',ev,'token',tok,'assignment',to_jsonb(q),'claim_expires_at',least(clock_timestamp()+interval '30minutes',q.not_after));
END;$function$
