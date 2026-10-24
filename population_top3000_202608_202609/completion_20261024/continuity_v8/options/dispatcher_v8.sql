CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.continuation_dispatch_v6(p_owner text, p_fence bigint, p_stop bigint)
 RETURNS jsonb
 LANGUAGE plpgsql
 SET search_path TO 'pg_catalog'
AS $function$
DECLARE ctl jsonb;q record;manifest jsonb;h text;k text;v_kind text;ev bigint;inserted integer:=0;external_count integer:=0;changed integer;last_progress timestamptz;pending integer;handler population_top3000_202608_202609_v1.continuation_handler_v6%rowtype;ans jsonb;fn text;src uuid;cm record;ac population_top3000_202608_202609_v1.acquisition_control%rowtype;pc population_top3000_202608_202609_v1.pipeline_control%rowtype;usage_bytes bigint;policy_generation text;
BEGIN
 ctl:=population_top3000_202608_202609_v1.continuation_guard_v6(p_owner,p_fence,p_stop);

 PERFORM population_top3000_202608_202609_v1.sequencing_dependency_wake_v1(p_owner,p_fence,p_stop);
 -- At most one full classification pass per minute, including when scheduler cadence is two seconds.
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_event WHERE event='CONTINUATION_DISPATCH_PASS_V6' AND at>clock_timestamp()-interval '60seconds') THEN RETURN '{"state":"BOUNDED_RECHECK_NOT_DUE"}'::jsonb;END IF;
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES('CONTINUATION_DISPATCH_PASS_V6',ctl);
 -- Renew operational time admission before expiry. This does not raise call/storage/tick limits or the original deadline.
 SELECT * INTO ac FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton;SELECT * INTO pc FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton;
 IF ac.admitted_until<clock_timestamp()+interval '30minutes' AND pc.deadline>ac.admitted_until
 AND (SELECT count(*) FROM population_top3000_202608_202609_v1.pipeline_event WHERE event='CONTINUATION_TIME_ADMISSION_REVIEWED_V6')<16 THEN
 SELECT coalesce(sum(pg_total_relation_size(c.oid)),0) INTO usage_bytes FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='population_top3000_202608_202609_v1' AND c.relkind IN('r','m');
 IF usage_bytes < (pc.budget_evidence#>>'{resource_limits,max_relation_bytes}')::bigint
 AND ac.stored_bytes+ac.reserved_bytes<ac.max_source_bytes
 AND ac.provider_requests<ac.max_provider_requests AND ac.http_invocations<ac.max_http_invocations
 AND NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_provider_lease WHERE released_at IS NULL)
 AND NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.connector_probe_lease_v1 WHERE released_at IS NULL)
 AND NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE status='LEASED')
 THEN
 UPDATE population_top3000_202608_202609_v1.acquisition_control SET admitted_until=least(pc.deadline,clock_timestamp()+interval '3hours') WHERE singleton;
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES('CONTINUATION_TIME_ADMISSION_REVIEWED_V6',ctl||jsonb_build_object('old_admitted_until',ac.admitted_until,'new_admitted_until',least(pc.deadline,clock_timestamp()+interval '3hours'),'original_deadline',pc.deadline,'mission_relation_bytes',usage_bytes,'source_bytes',ac.stored_bytes,'cumulative_counters_preserved',true,'call_storage_tick_caps_unchanged',true,'reviewed_policy','CURRENT_USER_CONTINUITY_AUTHORITY_WITH_FINITE3H_TRANCHE_MAX16'));
 ctl:=population_top3000_202608_202609_v1.continuation_guard_v6(p_owner,p_fence,p_stop);
 END IF;END IF;

 -- Re-admit only unclaimed clearing work to the newly reviewed operational horizon, preserving every prior deadline and attempt.
 FOR q IN SELECT * FROM population_top3000_202608_202609_v1.continuation_clear_queue_v6 WHERE status IN('READY','NEEDS_REVIEW') AND not_after<(ctl->>'not_after')::timestamptz AND not_after<=clock_timestamp()+interval '30minutes' ORDER BY created_at LIMIT 96 FOR UPDATE LOOP
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES('ENGINEERING_CLEARING_ADMISSION_RENEWED_V6',ctl||jsonb_build_object('clearing_id',q.clearing_id,'original_assignment_event_id',q.assignment_event_id,'prior_not_after',q.not_after,'new_not_after',ctl->>'not_after','input_sha256',q.input_sha256,'attempts_preserved',q.attempts,'claimed_rows_excluded',true)) RETURNING event_id INTO ev;
 UPDATE population_top3000_202608_202609_v1.continuation_clear_queue_v6 SET not_after=(ctl->>'not_after')::timestamptz,last_receipt=coalesce(last_receipt,'{}'::jsonb)||jsonb_build_object('admission_renewal_event_id',ev),updated_at=clock_timestamp() WHERE clearing_id=q.clearing_id;
 END LOOP;
 -- An admitted/requeued task is not yet cleared. Require its real source commit and automatic normalization successor.
 FOR q IN SELECT * FROM population_top3000_202608_202609_v1.continuation_clear_queue_v6 WHERE status='WAITING_OUTPUT' AND kind='OPERATIONAL_REVIEW' ORDER BY created_at LIMIT 4 LOOP
  src:=(q.last_receipt->>'source_task_id')::uuid;
  SELECT c.commit_id,c.committed_at,p.source_id,p.page_number,p.partition_key INTO cm FROM population_top3000_202608_202609_v1.pipeline_commit c JOIN population_top3000_202608_202609_v1.ctx02_census_page_v1 p ON p.acquire_task_id=c.task_id AND c.receipt->'immutable_output_ids' @> jsonb_build_array(p.source_id) WHERE c.task_id=src AND c.committed_at>q.claimed_at AND c.new_committed_units>0 AND p.page_number=(q.last_receipt->>'next_page')::integer ORDER BY c.committed_at LIMIT 1;
  IF FOUND AND EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task t WHERE t.family_id=q.family_id AND t.stage='NORMALIZE' AND t.partition_key=cm.partition_key||':page:'||cm.page_number::text AND t.payload->>'source_page_number'=cm.page_number::text AND t.created_at>q.claimed_at) THEN
   UPDATE population_top3000_202608_202609_v1.continuation_clear_queue_v6 SET status='COMPLETE',last_receipt=last_receipt||jsonb_build_object('useful_commit_id',cm.commit_id,'useful_committed_at',cm.committed_at,'useful_output_observed',true),updated_at=clock_timestamp() WHERE clearing_id=q.clearing_id;
   INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES('CONTINUATION_OPERATIONAL_USEFUL_OUTPUT_V6',ctl||jsonb_build_object('clearing_id',q.clearing_id,'source_task_id',src,'commit_id',cm.commit_id,'normalized_successor_exists',true));
  END IF;
 END LOOP;
 -- At most96 durable clearing groups, at most16 newly dispatched in one tick. Full source task denominator is never edited.
 FOR q IN SELECT t.family_id,t.blocker_code, population_top3000_202608_202609_v1.continuation_classify_v6(t.family_id,min(t.stage),t.blocker_code) kind,
 jsonb_agg(jsonb_build_object('task_id',t.task_id,'stage',t.stage,'partition_key',t.partition_key,'payload_sha256',encode(sha256(convert_to(t.payload::text,'UTF8')),'hex'),
 'input_versions',t.input_versions,'checkpoint',t.checkpoint,'attempt_id',t.attempt_id,'executions',t.executions,'cumulative_failures',t.cumulative_failures)
 ORDER BY t.task_id) manifest
 FROM population_top3000_202608_202609_v1.pipeline_task t WHERE t.status='BLOCKED' GROUP BY t.family_id,t.blocker_code,CASE WHEN t.family_id='CTX02' AND t.stage='ACQUIRE' AND t.blocker_code='CTX02_CENSUS_PAGE_CAP_REVIEW' THEN t.task_id ELSE NULL END ORDER BY t.family_id,t.blocker_code LIMIT 96 LOOP
  v_kind:=q.kind;manifest:=q.manifest;h:=encode(sha256(convert_to(manifest::text,'UTF8')),'hex');
  -- Reviewed policy generation gives an exact failed/old admission a new immutable clearing identity.
  policy_generation:='';
  IF q.family_id='CTX02' AND q.blocker_code='CTX02_CENSUS_PAGE_CAP_REVIEW' AND jsonb_array_length(manifest)=1 THEN
   SELECT encode(sha256(convert_to(ch.bounds::text,'UTF8')),'hex') INTO policy_generation
   FROM population_top3000_202608_202609_v1.continuation_handler_v6 ch
   WHERE ch.kind='CTX02_PAGE_ADMISSION' AND ch.enabled AND ch.bounds->>'policy_revision'='CTX02_MEASURED_REQUEST_CONTINUATION_V8'
   AND ch.bounds->'request_policies' ? (manifest->0->>'task_id');
   policy_generation:=coalesce(policy_generation,'');
  END IF;
  k:=encode(sha256(convert_to(p_owner||':'||p_fence||':'||p_stop||':'||q.family_id||':'||coalesce(q.blocker_code,'')||':'||h||CASE WHEN policy_generation<>'' THEN ':'||policy_generation ELSE '' END,'UTF8')),'hex');
  IF policy_generation<>'' THEN
   -- Never supersede claimed/unknown effects or reset attempt history. A new reviewed data policy is an explicit new generation.
   UPDATE population_top3000_202608_202609_v1.continuation_clear_queue_v6 old
   SET status='SUPERSEDED',last_receipt=coalesce(old.last_receipt,'{}'::jsonb)||jsonb_build_object('effects_known',true,'reason','REVIEWED_EXACT_REQUEST_POLICY_GENERATION_ADVANCED','successor_clearing_id',k,'successor_policy_sha256',policy_generation,'prior_attempts_preserved',old.attempts),updated_at=clock_timestamp()
   WHERE old.family_id='CTX02' AND old.blocker_code='CTX02_CENSUS_PAGE_CAP_REVIEW' AND old.clearing_id<>k
   AND jsonb_array_length(old.input_manifest)=1 AND old.input_manifest->0->>'task_id'=manifest->0->>'task_id'
   AND ((old.status='READY' AND old.attempts=0) OR (old.status='NEEDS_REVIEW' AND old.last_receipt->>'failed_handler_transaction_rolled_back'='true' AND old.last_receipt->>'effects_known'='true' AND old.last_receipt->>'policy_sha256' IS DISTINCT FROM policy_generation));
  END IF;

  UPDATE population_top3000_202608_202609_v1.continuation_clear_queue_v6 SET status='SUPERSEDED',last_receipt=coalesce(last_receipt,'{}'::jsonb)||jsonb_build_object('effects_known',true,'reason','UNCLAIMED_BLOCKER_MANIFEST_ADVANCED','successor_input_sha256',h,'implementation_executed',false),updated_at=clock_timestamp()
  WHERE status IN('READY','NEEDS_REVIEW') AND attempts=0 AND family_id=q.family_id AND blocker_code=q.blocker_code AND input_sha256<>h AND (q.family_id<>'CTX02' OR q.blocker_code<>'CTX02_CENSUS_PAGE_CAP_REVIEW' OR (jsonb_array_length(input_manifest)=1 AND input_manifest->0->>'task_id'=q.manifest->0->>'task_id'));
  IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.continuation_clear_queue_v6 WHERE clearing_id=k) THEN CONTINUE;END IF;
  IF (SELECT count(*) FROM population_top3000_202608_202609_v1.continuation_clear_queue_v6 WHERE status NOT IN('COMPLETE','SUPERSEDED'))>=96 OR inserted>=16 THEN EXIT;END IF;
  INSERT INTO population_top3000_202608_202609_v1.continuation_clear_queue_v6(clearing_id,kind,family_id,blocker_code,input_manifest,input_sha256,status,owner_id,owner_fence,stop_generation,scope_sha256,not_after,last_receipt)
  VALUES(k,v_kind,q.family_id,coalesce(q.blocker_code,'UNSPECIFIED'),manifest,h,CASE WHEN v_kind='EXTERNAL_CONDITION' THEN 'BLOCKED_EXTERNAL' ELSE 'READY' END,
  p_owner,p_fence,p_stop,ctl->>'scope_sha256',(ctl->>'not_after')::timestamptz,
  CASE WHEN v_kind='EXTERNAL_CONDITION' THEN jsonb_build_object('external_condition',q.blocker_code,'retry_allowed',false,'affected_tasks',jsonb_array_length(manifest)) END) ON CONFLICT DO NOTHING;
  GET DIAGNOSTICS changed=ROW_COUNT; IF changed=0 THEN CONTINUE;END IF;
  INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES('ENGINEERING_CLEARING_ASSIGNMENT_V6',jsonb_build_object(
    'assignment_id','clearing-v6:'||k,'clearing_id',k,'kind',v_kind,'family_id',q.family_id,'blocker_code',q.blocker_code,
    'owner_id',p_owner,'owner_fence',p_fence,'stop_generation',p_stop,'scope_sha256',ctl->>'scope_sha256','authority_event_id',9528,
    'assigned_executor','automation:6adc2c96db348191bca7cf4eacfdac32','input_sha256',h,'input_manifest',manifest,
    'exclusive_component',v_kind||':'||q.family_id,'root_will_not_concurrently_modify',true,'not_after',ctl->>'not_after',
    'allowed_repository_path','population_top3000_202608_202609/completion_20261024/continuity_v6/'||lower(q.family_id),
    'maximum_elapsed_minutes',30,'maximum_tool_calls',80,'maximum_repair_iterations',2,'maximum_production_integrations',1,
    'new_provider_requests',CASE WHEN v_kind='SOURCE_RECOVERY' THEN 32 ELSE 0 END,'maximum_source_bytes',CASE WHEN v_kind='SOURCE_RECOVERY' THEN 16777216 ELSE 0 END,
    'request_admission_required',true,'producer_role','Delegated distinct clearing executor','independent_reviewer_role','Distinct reviewer; producer cannot self-certify',
    'execution_recipe',CASE v_kind WHEN 'OPERATIONAL_REVIEW' THEN 'Measure current counters, retained hash chain, source bytes and storage; independently review finite next tranche; apply guarded admission and exact blocked acquisition requeue preserving checkpoint/counters; prove useful commit and normalization successor.'
      WHEN 'SOURCE_RECOVERY' THEN 'Read pinned source requirements; reuse exact original retained bodies first; reserve finite requests under current controls; acquire authoritative originals into private mission storage; implement or invoke reviewed numerical/vintage verification; dispatch independent review successor. Preserve original-receipt limitations.'
      WHEN 'COMPANY_RECONCILIATION' THEN 'Route to Prompt02 company producer only; collect exact company/cutoff/listing source facts from Prompt04; reconcile immutable candidate decisions with independent review; no other executor may decide membership/rank.'
      WHEN 'INDEPENDENT_VERIFICATION' THEN 'Dispatch distinct reviewer for pinned numerical/population/temporal output; run meaningful exact-scope tests; retain evidence holds; return review receipt to root for acceptance; never self-certify.'
      WHEN 'IMPLEMENTATION_REPAIR' THEN 'Inspect exact failure and pinned inputs; produce smallest scoped correction with synthetic+rollback tests; independent review before guarded installation; requeue only named defect hold; prove useful commit and successor.'
      ELSE 'Record exact external condition; do not retry or release source task.' END,
    'expected_useful_commit','Immutable output/evidence commit for pinned affected task or reviewed clearing receipt; READY/enabled/heartbeat is insufficient',
    'expected_successor','Existing pipeline automatic successor or explicitly dispatched bounded independent-review assignment',
    'protected_invariants',jsonb_build_array('Preserve owner/fence/stop and original deadline','Preserve all source rows, denominators, costs, attempts and failures','No unrelated scanner/trading/D06 generation','Keep provider bytes private','No family acceptance or serving promotion')))
  RETURNING event_id INTO ev;
  UPDATE population_top3000_202608_202609_v1.continuation_clear_queue_v6 SET assignment_event_id=ev WHERE clearing_id=k;
  inserted:=inserted+1;IF v_kind='EXTERNAL_CONDITION' THEN external_count:=external_count+1;END IF;
 END LOOP;

 -- Exactly one registered, independently reviewed operational helper per pass. No generic source-task hold clearing.
 SELECT * INTO handler FROM population_top3000_202608_202609_v1.continuation_handler_v6 WHERE kind='CTX02_PAGE_ADMISSION' AND enabled;
 IF FOUND THEN
 IF handler.function_sha256 IS DISTINCT FROM encode(sha256(convert_to(pg_get_functiondef(handler.function_oid),'UTF8')),'hex') THEN RAISE EXCEPTION 'CONTINUATION_OPERATIONAL_HANDLER_CHANGED';END IF;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_event e WHERE e.event_id=handler.review_event_id AND e.details->>'approved'='true' AND e.details->>'handler_function_sha256'=handler.function_sha256 AND e.details->>'owner_id'=p_owner AND e.details->>'owner_fence'=p_fence::text AND e.details->>'stop_generation'=p_stop::text AND e.details->>'scope_sha256'=ctl->>'scope_sha256' AND nullif(e.details->>'reviewer','') IS NOT NULL AND nullif(e.details->>'producer','') IS NOT NULL AND e.details->>'reviewer' IS DISTINCT FROM e.details->>'producer' AND e.details->>'handler_bounds_sha256'=encode(sha256(convert_to(handler.bounds::text,'UTF8')),'hex')) THEN RAISE EXCEPTION 'CONTINUATION_OPERATIONAL_REVIEW_REQUIRED';END IF;
 SELECT * INTO q FROM population_top3000_202608_202609_v1.continuation_clear_queue_v6 WHERE status='READY' AND kind='OPERATIONAL_REVIEW' AND family_id='CTX02' AND blocker_code='CTX02_CENSUS_PAGE_CAP_REVIEW' AND jsonb_array_length(input_manifest)=1 AND handler.bounds->'request_policies' ? (input_manifest->0->>'task_id') AND not_after>clock_timestamp() AND attempts<max_attempts
 AND NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.continuation_clear_queue_v6 other WHERE other.status IN('CLAIMED','WAITING_OUTPUT') AND other.clearing_id<>continuation_clear_queue_v6.clearing_id AND jsonb_array_length(other.input_manifest)=1 AND other.input_manifest->0->>'task_id'=continuation_clear_queue_v6.input_manifest->0->>'task_id')
 ORDER BY created_at,clearing_id LIMIT 1 FOR UPDATE;
 IF FOUND AND NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE status='LEASED')
 AND NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_provider_lease WHERE released_at IS NULL)
 AND NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.connector_probe_lease_v1 WHERE released_at IS NULL) THEN
 src:=(q.input_manifest->0->>'task_id')::uuid;
 IF jsonb_array_length(q.input_manifest)<>1 THEN RAISE EXCEPTION 'CONTINUATION_EXACT_SOURCE_TASK_REQUIRED';END IF;
 SELECT format('%I.%I',n.nspname,p.proname) INTO fn FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE p.oid=handler.function_oid;
 BEGIN
 EXECUTE format('select %s($1,$2,$3,$4)',fn) INTO ans USING p_owner,p_fence,p_stop,jsonb_build_object('source_task_id',src,'clearing_id',q.clearing_id,'bounds',handler.bounds);
 IF ans->>'state' IS DISTINCT FROM 'CLEARED' THEN RAISE EXCEPTION 'CONTINUATION_HANDLER_NOT_CLEARED';END IF;
 UPDATE population_top3000_202608_202609_v1.continuation_clear_queue_v6 SET status='WAITING_OUTPUT',attempts=attempts+1,claimed_at=clock_timestamp(),last_receipt=ans||jsonb_build_object('independent_review_event_id',handler.review_event_id,'policy_sha256',encode(sha256(convert_to(handler.bounds::text,'UTF8')),'hex')),updated_at=clock_timestamp() WHERE clearing_id=q.clearing_id;
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES('CONTINUATION_OPERATIONAL_REQUEUED_V6',ctl||jsonb_build_object('clearing_id',q.clearing_id,'receipt',ans,'useful_output_observed',false));
 EXCEPTION WHEN OTHERS THEN
 UPDATE population_top3000_202608_202609_v1.continuation_clear_queue_v6 SET status='NEEDS_REVIEW',attempts=attempts+1,last_receipt=jsonb_build_object('recoverable_condition',left(SQLERRM,400),'execution_recipe','Measure resource/counter/lease evidence; use finite reviewed admission or exact defect correction, then explicit guarded requeue; do not bypass the failed guard','effects_known',true,'failed_handler_transaction_rolled_back',true,'policy_sha256',encode(sha256(convert_to(handler.bounds::text,'UTF8')),'hex')),updated_at=clock_timestamp() WHERE clearing_id=q.clearing_id;
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES('CONTINUATION_OPERATIONAL_REVIEW_HELD_V6',ctl||jsonb_build_object('clearing_id',q.clearing_id,'error',left(SQLERRM,400),'source_task_unchanged',true,'policy_sha256',encode(sha256(convert_to(handler.bounds::text,'UTF8')),'hex'),'required_successor',CASE WHEN SQLERRM='OPTIONS_V8_MEASURED_RESOURCE_REVIEW_REQUIRED' THEN 'RESOURCE_CAPACITY_PROBE_AND_MEASURED_ADMISSION' ELSE 'EXACT_GUARDED_DEFECT_OR_EVIDENCE_REVIEW' END));
 END;
 END IF;END IF;

 -- A separately reviewed CTX07 helper materializes exact-source parser and independent-review successors without source downloads.
 SELECT * INTO handler FROM population_top3000_202608_202609_v1.continuation_handler_v6 WHERE kind='CTX07_SOURCE_SUCCESSORS' AND enabled;
 IF FOUND THEN
  IF handler.function_sha256 IS DISTINCT FROM encode(sha256(convert_to(pg_get_functiondef(handler.function_oid),'UTF8')),'hex') THEN RAISE EXCEPTION 'CONTINUATION_SOURCE_SUCCESSOR_HANDLER_CHANGED';END IF;
  IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_event e WHERE e.event_id=handler.review_event_id AND e.details->>'approved'='true' AND e.details->>'handler_function_sha256'=handler.function_sha256 AND e.details->>'owner_id'=p_owner AND e.details->>'owner_fence'=p_fence::text AND e.details->>'stop_generation'=p_stop::text AND e.details->>'scope_sha256'=ctl->>'scope_sha256' AND nullif(e.details->>'producer','') IS NOT NULL AND nullif(e.details->>'reviewer','') IS NOT NULL AND e.details->>'reviewer' IS DISTINCT FROM e.details->>'producer') THEN RAISE EXCEPTION 'CONTINUATION_SOURCE_SUCCESSOR_REVIEW_REQUIRED';END IF;
  IF (SELECT count(*) FROM population_top3000_202608_202609_v1.pipeline_event e WHERE e.event='CONTINUATION_SOURCE_SUCCESSOR_HANDLER_FAILED_V7' AND e.details->>'handler_sha256'=handler.function_sha256)<3 THEN
   SELECT format('%I.%I',n.nspname,p.proname) INTO fn FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE p.oid=handler.function_oid;
   BEGIN
    EXECUTE format('select %s($1,$2,$3)',fn) INTO ans USING p_owner,p_fence,p_stop;
   EXCEPTION WHEN OTHERS THEN
    INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES('CONTINUATION_SOURCE_SUCCESSOR_HANDLER_FAILED_V7',ctl||jsonb_build_object('handler_sha256',handler.function_sha256,'error',left(SQLERRM,400),'effects_known',true,'helper_mutations_rolled_back',true,'attempt_limit',3,'existing_source_clearing_assignment_remains_eligible',true));
   END;
  END IF;
 END IF;
 -- Expired engineering claims are executable effect-reconciliation work, never automatic lease stealing.
 FOR q IN SELECT e.event_id,e.details FROM population_top3000_202608_202609_v1.pipeline_event e WHERE e.event='ENGINEERING_ASSIGNMENT_CLAIMED_V5'
  AND e.details->>'owner_id'=p_owner AND e.details->>'owner_fence'=p_fence::text AND e.details->>'stop_generation'=p_stop::text
  AND (e.details->>'expires_at')::timestamptz<=clock_timestamp()
  AND NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_event r WHERE r.event IN('ENGINEERING_ASSIGNMENT_COMPLETED_V5','ENGINEERING_ASSIGNMENT_EFFECTS_RECONCILED_RELEASE_V5') AND r.details->>'claim_event_id'=e.event_id::text AND r.details->>'effects_known'='true')
  ORDER BY e.event_id LIMIT 4 LOOP
  manifest:=jsonb_build_array(jsonb_build_object('claim_event_id',q.event_id,'claim_details_sha256',encode(sha256(convert_to(q.details::text,'UTF8')),'hex')));h:=encode(sha256(convert_to(manifest::text,'UTF8')),'hex');k:='claim:'||q.event_id;
  IF (SELECT count(*) FROM population_top3000_202608_202609_v1.continuation_clear_queue_v6 WHERE status NOT IN('COMPLETE','SUPERSEDED'))>=96 THEN EXIT;END IF;
  INSERT INTO population_top3000_202608_202609_v1.continuation_clear_queue_v6(clearing_id,kind,family_id,blocker_code,input_manifest,input_sha256,status,owner_id,owner_fence,stop_generation,scope_sha256,not_after)
  VALUES(k,'CLAIM_RECONCILIATION','ENGINEERING','EXPIRED_CLAIM_EFFECTS_UNRECONCILED',manifest,h,'READY',p_owner,p_fence,p_stop,ctl->>'scope_sha256',(ctl->>'not_after')::timestamptz) ON CONFLICT DO NOTHING;
 END LOOP;

 -- Expired executor claims require effect reconciliation. They never prevent a disjoint family claim.
 FOR q IN SELECT * FROM population_top3000_202608_202609_v1.continuation_clear_queue_v6 WHERE status='CLAIMED' AND claim_expires_at<=clock_timestamp() AND kind<>'CLAIM_RECONCILIATION' ORDER BY claimed_at LIMIT 4 LOOP
 manifest:=jsonb_build_array(jsonb_build_object('clearing_claim_id',q.clearing_id,'claimed_at',q.claimed_at,'input_sha256',q.input_sha256,'required_action','Reconcile immutable Git/deployment/database/provider effects; release only under current controller with exact evidence; unknown effects stay held'));
 h:=encode(sha256(convert_to(manifest::text,'UTF8')),'hex');k:='clear-claim:'||q.clearing_id;
 IF (SELECT count(*) FROM population_top3000_202608_202609_v1.continuation_clear_queue_v6 WHERE status NOT IN('COMPLETE','SUPERSEDED'))<96 THEN
 INSERT INTO population_top3000_202608_202609_v1.continuation_clear_queue_v6(clearing_id,kind,family_id,blocker_code,input_manifest,input_sha256,status,owner_id,owner_fence,stop_generation,scope_sha256,not_after)
 VALUES(k,'CLAIM_RECONCILIATION','ENGINEERING','EXPIRED_V6_CLAIM_EFFECTS_UNRECONCILED',manifest,h,'READY',p_owner,p_fence,p_stop,ctl->>'scope_sha256',(ctl->>'not_after')::timestamptz) ON CONFLICT DO NOTHING;END IF;
 END LOOP;
 -- Proactive capacity review has a concrete executable measurement task before an admitted source tranche hits shared limits.
 SELECT coalesce(sum(pg_total_relation_size(c.oid)),0) INTO usage_bytes FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='population_top3000_202608_202609_v1' AND c.relkind IN('r','m');
 IF usage_bytes>(pc.budget_evidence#>>'{resource_limits,max_relation_bytes}')::bigint*0.88 OR ac.stored_bytes+ac.reserved_bytes>ac.max_source_bytes*0.88 OR ac.provider_requests>ac.max_provider_requests*0.88
 OR ac.http_invocations>ac.max_http_invocations*0.88 OR pc.tick_count>pc.max_ticks*0.88 OR ac.dispatch_calls>ac.max_dispatch_calls*0.88
 OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.continuation_clear_queue_v6 cq WHERE cq.kind='OPERATIONAL_REVIEW' AND cq.family_id='CTX02' AND cq.status='NEEDS_REVIEW' AND cq.last_receipt->>'effects_known'='true' AND cq.last_receipt->>'failed_handler_transaction_rolled_back'='true' AND cq.last_receipt->>'recoverable_condition'='OPTIONS_V8_MEASURED_RESOURCE_REVIEW_REQUIRED')
 OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.continuation_handler_v6 ch WHERE ch.kind='CTX02_PAGE_ADMISSION' AND ch.enabled AND ch.bounds->>'policy_revision'='CTX02_MEASURED_REQUEST_CONTINUATION_V8'
 AND (usage_bytes>(ch.bounds#>>'{global_ceiling,relation_bytes}')::bigint*0.88 OR ac.stored_bytes+ac.reserved_bytes>(ch.bounds#>>'{global_ceiling,source_bytes}')::bigint*0.88 OR ac.provider_requests>(ch.bounds#>>'{global_ceiling,provider_requests}')::bigint*0.88 OR ac.http_invocations>(ch.bounds#>>'{global_ceiling,http_invocations}')::bigint*0.88)) THEN
 manifest:=jsonb_build_array(jsonb_build_object('resource_limits',pc.budget_evidence->'resource_limits','source_limit',ac.max_source_bytes,'provider_limit',ac.max_provider_requests,'original_deadline',pc.deadline,'max_ticks',pc.max_ticks,'max_dispatch_calls',ac.max_dispatch_calls,'max_http_invocations',ac.max_http_invocations,'options_policy_sha256',(SELECT encode(sha256(convert_to(bounds::text,'UTF8')),'hex') FROM population_top3000_202608_202609_v1.continuation_handler_v6 WHERE kind='CTX02_PAGE_ADMISSION'),'options_reviewed_global_ceilings',(SELECT bounds->'global_ceiling' FROM population_top3000_202608_202609_v1.continuation_handler_v6 WHERE kind='CTX02_PAGE_ADMISSION')));
 h:=encode(sha256(convert_to(manifest::text,'UTF8')),'hex');k:='resource:'||h;
 IF (SELECT count(*) FROM population_top3000_202608_202609_v1.continuation_clear_queue_v6 WHERE status NOT IN('COMPLETE','SUPERSEDED'))<96 THEN
 INSERT INTO population_top3000_202608_202609_v1.continuation_clear_queue_v6(clearing_id,kind,family_id,blocker_code,input_manifest,input_sha256,status,owner_id,owner_fence,stop_generation,scope_sha256,not_after,last_receipt)
 VALUES(k,'OPERATIONAL_REVIEW','RESOURCE','RESOURCE_CAPACITY_PROBE_AND_MEASURED_ADMISSION',manifest,h,'READY',p_owner,p_fence,p_stop,ctl->>'scope_sha256',(ctl->>'not_after')::timestamptz,
 jsonb_build_object('measured_relation_bytes',usage_bytes,'source_bytes',ac.stored_bytes,'reserved_bytes',ac.reserved_bytes,'http_invocations',ac.http_invocations,'tick_count',pc.tick_count,'dispatch_calls',ac.dispatch_calls,'execution_recipe','Inspect exact reviewed options policy and live ceilings; use a newly independently reviewed policy data generation to consume increased headroom without a code patch or resetting failed clearing attempts. Inspect existing resource probe and use a separately versioned finite slot for fresh read-only project capacity; calculate finite new source/relation/call allowance with at least20GB other-work headroom; distinct reviewer and counter-preservation rollback before controller admission. Do not reset original probe slots, counters or deadline.')) ON CONFLICT DO NOTHING;END IF;END IF;
 -- Every special clearing row is discoverable as a runnable assignment event, including effect-reconciliation and resource review.
 FOR q IN SELECT * FROM population_top3000_202608_202609_v1.continuation_clear_queue_v6 WHERE assignment_event_id IS NULL ORDER BY created_at LIMIT 8 LOOP
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES('ENGINEERING_CLEARING_ASSIGNMENT_V6',ctl||jsonb_build_object('assignment_id','clearing-v6:'||q.clearing_id,'clearing_id',q.clearing_id,'kind',q.kind,'family_id',q.family_id,'blocker_code',q.blocker_code,'input_manifest',q.input_manifest,'input_sha256',q.input_sha256,'assigned_executor',q.assigned_executor,'maximum_elapsed_minutes',30,'maximum_tool_calls',80,'maximum_repair_iterations',2,'maximum_production_integrations',1,'producer_role','Distinct clearing executor','independent_reviewer_role','Distinct reviewer','exclusive_component',q.kind||':'||q.family_id,'root_will_not_concurrently_modify',true,'execution_recipe',coalesce(q.last_receipt->>'execution_recipe',q.input_manifest->0->>'required_action','Reconcile exact claim effects using immutable Git/deployment/data receipts; current controller alone records release without data acceptance'),'expected_useful_commit','Pinned effect reconciliation or measured operational admission evidence; not an enabled scheduler','expected_successor','Exact authorized implementation or data task under existing mission','new_provider_requests',0,'new_http_invocations',CASE WHEN q.family_id='RESOURCE' THEN 1 ELSE 0 END,'maximum_source_bytes',0,'preserve_counters_deadline_and_holds',true)) RETURNING event_id INTO ev;
 UPDATE population_top3000_202608_202609_v1.continuation_clear_queue_v6 SET assignment_event_id=ev WHERE clearing_id=q.clearing_id;
 END LOOP;
 last_progress:=(ctl->>'last_useful_progress_at')::timestamptz;
 IF last_progress IS NULL OR last_progress<clock_timestamp()-interval '10minutes' THEN
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_event WHERE event='CONTINUATION_NO_USEFUL_PROGRESS_V6' AND at>clock_timestamp()-interval '10minutes') THEN
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES('CONTINUATION_NO_USEFUL_PROGRESS_V6',ctl||jsonb_build_object('scheduler_is_not_progress',true,'ready_clearing_groups',(SELECT count(*) FROM population_top3000_202608_202609_v1.continuation_clear_queue_v6 WHERE status='READY'),'ready_data_tasks',(SELECT count(*) FROM population_top3000_202608_202609_v1.pipeline_task WHERE status='READY'),'active_attempts',(SELECT count(*) FROM population_top3000_202608_202609_v1.pipeline_task WHERE status='LEASED')));END IF;END IF;
 RETURN jsonb_build_object('state','CLEARING_WORK_DISPATCHED','new_groups',inserted,'new_external_groups',external_count,'ready_groups',(SELECT count(*) FROM population_top3000_202608_202609_v1.continuation_clear_queue_v6 WHERE status='READY'));
END;$function$;

REVOKE ALL ON FUNCTION population_top3000_202608_202609_v1.continuation_dispatch_v6(text,bigint,bigint) FROM PUBLIC,anon,authenticated;
