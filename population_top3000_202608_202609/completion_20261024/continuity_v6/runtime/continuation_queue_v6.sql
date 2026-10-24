-- Durable clearing queue; private control plane. No source bytes, provider request or dataset acceptance here.
CREATE TABLE population_top3000_202608_202609_v1.continuation_clear_queue_v6(
 clearing_id text PRIMARY KEY, kind text NOT NULL CHECK(kind IN('OPERATIONAL_REVIEW','SOURCE_RECOVERY','COMPANY_RECONCILIATION','INDEPENDENT_VERIFICATION','IMPLEMENTATION_REPAIR','CLAIM_RECONCILIATION','EXTERNAL_CONDITION')),
 family_id text NOT NULL, blocker_code text NOT NULL, input_manifest jsonb NOT NULL, input_sha256 text NOT NULL,
 status text NOT NULL CHECK(status IN('READY','CLAIMED','WAITING_OUTPUT','BLOCKED_EXTERNAL','NEEDS_REVIEW','SUPERSEDED','COMPLETE')),
 owner_id text NOT NULL, owner_fence bigint NOT NULL, stop_generation bigint NOT NULL, scope_sha256 text NOT NULL,
 assigned_executor text NOT NULL DEFAULT 'automation:6adc2c96db348191bca7cf4eacfdac32',
 not_after timestamptz NOT NULL, max_attempts integer NOT NULL DEFAULT 3 CHECK(max_attempts BETWEEN 1 AND 3),
 attempts integer NOT NULL DEFAULT 0 CHECK(attempts>=0), claim_token_sha256 text, claimed_at timestamptz, claim_expires_at timestamptz,
 assignment_event_id bigint, last_receipt jsonb, created_at timestamptz NOT NULL DEFAULT clock_timestamp(),updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 CHECK(input_sha256=encode(sha256(convert_to(input_manifest::text,'UTF8')),'hex')));
CREATE INDEX continuation_queue_pending_v6 ON population_top3000_202608_202609_v1.continuation_clear_queue_v6(status,created_at);
CREATE TABLE population_top3000_202608_202609_v1.continuation_handler_v6(kind text PRIMARY KEY, function_oid regprocedure NOT NULL,function_sha256 text NOT NULL,
 review_event_id bigint NOT NULL, bounds jsonb NOT NULL, enabled boolean NOT NULL DEFAULT false);
REVOKE ALL ON population_top3000_202608_202609_v1.continuation_clear_queue_v6,population_top3000_202608_202609_v1.continuation_handler_v6 FROM PUBLIC,anon,authenticated;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.continuation_guard_v6(p_owner text,p_fence bigint,p_stop bigint)
RETURNS jsonb LANGUAGE plpgsql SET search_path=pg_catalog AS $f$
DECLARE a population_top3000_202608_202609_v1.acquisition_control%rowtype;p population_top3000_202608_202609_v1.pipeline_control%rowtype;
BEGIN
 PERFORM pg_advisory_xact_lock(2026083000261022);
 SELECT * INTO STRICT a FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton FOR UPDATE;
 SELECT * INTO STRICT p FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton FOR UPDATE;
 IF p_owner IS DISTINCT FROM 'top3000-completion-20261024-root-5db48b32d103' OR p_fence IS DISTINCT FROM 3 OR p_stop IS DISTINCT FROM 1
 OR a.owner_id IS DISTINCT FROM p_owner OR a.owner_fence IS DISTINCT FROM p_fence OR a.stop_generation IS DISTINCT FROM p_stop
 OR p.expected_owner_id IS DISTINCT FROM a.owner_id OR p.expected_owner_fence IS DISTINCT FROM a.owner_fence OR p.expected_stop_generation IS DISTINCT FROM a.stop_generation
 OR a.scope_sha256 IS DISTINCT FROM '8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd' OR p.scope_sha256 IS DISTINCT FROM a.scope_sha256
 THEN RAISE EXCEPTION 'CONTINUATION_OWNER_STOP_SCOPE_CHANGED';END IF;
 IF a.enabled IS DISTINCT FROM true OR a.stopped IS DISTINCT FROM false OR a.hold_reason IS NOT NULL OR p.enabled IS DISTINCT FROM true
 THEN RAISE EXCEPTION 'CONTINUATION_USER_OR_CONTROL_HOLD';END IF;
 IF a.admitted_until IS NULL OR a.admitted_until<=clock_timestamp() OR p.deadline IS NULL OR p.deadline<=clock_timestamp()
 OR p.tick_count>=p.max_ticks OR a.dispatch_calls>=a.max_dispatch_calls OR p.shared_scheduler_verified IS DISTINCT FROM true
 OR p.budget_evidence#>>'{monetary_policy,authority_event_id}' IS DISTINCT FROM '9528'
 OR population_top3000_202608_202609_v1.pipeline_cost_admission_v2(p,0) IS DISTINCT FROM true THEN RAISE EXCEPTION 'CONTINUATION_ADMISSION_REQUIRED';END IF;
 RETURN jsonb_build_object('owner_id',a.owner_id,'owner_fence',a.owner_fence,'stop_generation',a.stop_generation,'scope_sha256',a.scope_sha256,
 'not_after',least(a.admitted_until,p.deadline),'last_useful_progress_at',p.last_useful_progress_at,'authority_event_id',9528);
END;$f$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.continuation_classify_v6(p_family text,p_stage text,p_blocker text)
RETURNS text LANGUAGE sql IMMUTABLE SET search_path=pg_catalog AS $f$
 SELECT CASE
 WHEN coalesce(p_blocker,'') ~ '(USER_STOP|OWNER_OR_STOP|AUTH|DENIED|ACCESS_DENIAL|FORBIDDEN|MISSING_PROVIDER_KEY|UNCERTAIN|UNKNOWN_HTTP|UNKNOWN_EFFECT|UNAVAILABLE_HISTORY|CREDENTIAL)' THEN 'EXTERNAL_CONDITION'
 WHEN coalesce(p_blocker,'') ~ '(PAGE_CAP|CALL_CAP|REQUEST_CAP|STORAGE|RELATION_CAP|ADMISSION|EXECUTION_LIMIT|RESPONSE_CAP)' THEN 'OPERATIONAL_REVIEW'
 WHEN coalesce(p_blocker,'') ~ '(PARSER|CODE_INTEGRITY|ADAPTER|IMPLEMENTATION|ENGINEERING_REGISTRATION)' THEN 'IMPLEMENTATION_REPAIR'
 WHEN p_family='CTX07' AND coalesce(p_blocker,'') ~ 'SOURCE' THEN 'SOURCE_RECOVERY'
 WHEN p_stage='ACQUIRE' AND coalesce(p_blocker,'') ~ '(SOURCE|DOCUMENT)' THEN 'SOURCE_RECOVERY'
 WHEN p_family='REF01' OR coalesce(p_blocker,'') ~ '(COMPANY|CUTOFF|IDENTITY|DENOMINATOR|COHORT)' THEN 'COMPANY_RECONCILIATION'
 WHEN p_stage='VALIDATE' OR coalesce(p_blocker,'') ~ '(INDEPENDENT|NUMERICAL|TEMPORAL|PUBLICATION|LIFECYCLE|ACCURACY|QUALITY)' THEN 'INDEPENDENT_VERIFICATION'
 ELSE 'IMPLEMENTATION_REPAIR' END;
$f$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.continuation_dispatch_v6(p_owner text,p_fence bigint,p_stop bigint)
RETURNS jsonb LANGUAGE plpgsql SET search_path=pg_catalog AS $f$
DECLARE ctl jsonb;q record;manifest jsonb;h text;k text;v_kind text;ev bigint;inserted integer:=0;external_count integer:=0;changed integer;last_progress timestamptz;pending integer;handler population_top3000_202608_202609_v1.continuation_handler_v6%rowtype;ans jsonb;fn text;src uuid;cm record;ac population_top3000_202608_202609_v1.acquisition_control%rowtype;pc population_top3000_202608_202609_v1.pipeline_control%rowtype;usage_bytes bigint;
BEGIN
 ctl:=population_top3000_202608_202609_v1.continuation_guard_v6(p_owner,p_fence,p_stop);

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
 FROM population_top3000_202608_202609_v1.pipeline_task t WHERE t.status='BLOCKED' GROUP BY t.family_id,t.blocker_code ORDER BY t.family_id,t.blocker_code LIMIT 96 LOOP
  v_kind:=q.kind;manifest:=q.manifest;h:=encode(sha256(convert_to(manifest::text,'UTF8')),'hex');
  k:=encode(sha256(convert_to(p_owner||':'||p_fence||':'||p_stop||':'||q.family_id||':'||coalesce(q.blocker_code,'')||':'||h,'UTF8')),'hex');

  UPDATE population_top3000_202608_202609_v1.continuation_clear_queue_v6 SET status='SUPERSEDED',last_receipt=coalesce(last_receipt,'{}'::jsonb)||jsonb_build_object('effects_known',true,'reason','UNCLAIMED_BLOCKER_MANIFEST_ADVANCED','successor_input_sha256',h,'implementation_executed',false),updated_at=clock_timestamp()
  WHERE status IN('READY','NEEDS_REVIEW') AND attempts=0 AND family_id=q.family_id AND blocker_code=q.blocker_code AND input_sha256<>h;
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
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_event e WHERE e.event_id=handler.review_event_id AND e.details->>'approved'='true' AND e.details->>'handler_function_sha256'=handler.function_sha256 AND e.details->>'owner_id'=p_owner AND e.details->>'owner_fence'=p_fence::text AND e.details->>'stop_generation'=p_stop::text AND e.details->>'scope_sha256'=ctl->>'scope_sha256' AND nullif(e.details->>'reviewer','') IS NOT NULL AND e.details->>'reviewer' IS DISTINCT FROM e.details->>'producer') THEN RAISE EXCEPTION 'CONTINUATION_OPERATIONAL_REVIEW_REQUIRED';END IF;
 SELECT * INTO q FROM population_top3000_202608_202609_v1.continuation_clear_queue_v6 WHERE status='READY' AND kind='OPERATIONAL_REVIEW' AND family_id='CTX02' AND blocker_code='CTX02_CENSUS_PAGE_CAP_REVIEW' AND not_after>clock_timestamp() AND attempts<max_attempts ORDER BY created_at LIMIT 1 FOR UPDATE;
 IF FOUND THEN
 src:=(q.input_manifest->0->>'task_id')::uuid;
 IF jsonb_array_length(q.input_manifest)<>1 THEN RAISE EXCEPTION 'CONTINUATION_EXACT_SOURCE_TASK_REQUIRED';END IF;
 SELECT format('%I.%I',n.nspname,p.proname) INTO fn FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE p.oid=handler.function_oid;
 BEGIN
 EXECUTE format('select %s($1,$2,$3,$4)',fn) INTO ans USING p_owner,p_fence,p_stop,jsonb_build_object('source_task_id',src,'clearing_id',q.clearing_id,'bounds',handler.bounds);
 IF ans->>'state' IS DISTINCT FROM 'CLEARED' THEN RAISE EXCEPTION 'CONTINUATION_HANDLER_NOT_CLEARED';END IF;
 UPDATE population_top3000_202608_202609_v1.continuation_clear_queue_v6 SET status='WAITING_OUTPUT',attempts=attempts+1,claimed_at=clock_timestamp(),last_receipt=ans||jsonb_build_object('independent_review_event_id',handler.review_event_id),updated_at=clock_timestamp() WHERE clearing_id=q.clearing_id;
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES('CONTINUATION_OPERATIONAL_REQUEUED_V6',ctl||jsonb_build_object('clearing_id',q.clearing_id,'receipt',ans,'useful_output_observed',false));
 EXCEPTION WHEN OTHERS THEN
 UPDATE population_top3000_202608_202609_v1.continuation_clear_queue_v6 SET status='NEEDS_REVIEW',attempts=attempts+1,last_receipt=jsonb_build_object('recoverable_condition',left(SQLERRM,400),'execution_recipe','Measure resource/counter/lease evidence; use finite reviewed admission or exact defect correction, then explicit guarded requeue; do not bypass the failed guard','effects_known',true,'failed_handler_transaction_rolled_back',true),updated_at=clock_timestamp() WHERE clearing_id=q.clearing_id;
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES('CONTINUATION_OPERATIONAL_REVIEW_HELD_V6',ctl||jsonb_build_object('clearing_id',q.clearing_id,'error',left(SQLERRM,400),'source_task_unchanged',true));
 END;
 END IF;END IF;
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
 IF usage_bytes>(pc.budget_evidence#>>'{resource_limits,max_relation_bytes}')::bigint*0.88 OR ac.stored_bytes>ac.max_source_bytes*0.88 OR ac.provider_requests>ac.max_provider_requests*0.88 THEN
 manifest:=jsonb_build_array(jsonb_build_object('resource_limits',pc.budget_evidence->'resource_limits','source_limit',ac.max_source_bytes,'provider_limit',ac.max_provider_requests,'original_deadline',pc.deadline));
 h:=encode(sha256(convert_to(manifest::text,'UTF8')),'hex');k:='resource:'||h;
 IF (SELECT count(*) FROM population_top3000_202608_202609_v1.continuation_clear_queue_v6 WHERE status NOT IN('COMPLETE','SUPERSEDED'))<96 THEN
 INSERT INTO population_top3000_202608_202609_v1.continuation_clear_queue_v6(clearing_id,kind,family_id,blocker_code,input_manifest,input_sha256,status,owner_id,owner_fence,stop_generation,scope_sha256,not_after,last_receipt)
 VALUES(k,'OPERATIONAL_REVIEW','RESOURCE','RESOURCE_CAPACITY_PROBE_AND_MEASURED_ADMISSION',manifest,h,'READY',p_owner,p_fence,p_stop,ctl->>'scope_sha256',(ctl->>'not_after')::timestamptz,
 jsonb_build_object('measured_relation_bytes',usage_bytes,'source_bytes',ac.stored_bytes,'execution_recipe','Inspect existing resource probe and use a separately versioned finite slot for fresh read-only project capacity; calculate finite new source/relation/call allowance with at least20GB other-work headroom; distinct reviewer and counter-preservation rollback before controller admission. Do not reset original probe slots, counters or deadline.')) ON CONFLICT DO NOTHING;END IF;END IF;
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
END;$f$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.continuation_claim_v6(p_owner text,p_fence bigint,p_stop bigint,p_executor text,p_run_id text)
RETURNS jsonb LANGUAGE plpgsql SET search_path=pg_catalog AS $f$
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

 ORDER BY CASE kind WHEN 'OPERATIONAL_REVIEW' THEN 0 WHEN 'SOURCE_RECOVERY' THEN 1 WHEN 'CLAIM_RECONCILIATION' THEN 2 ELSE 3 END,created_at,clearing_id FOR UPDATE SKIP LOCKED LIMIT 1;
 IF NOT FOUND THEN RETURN jsonb_build_object('state','NO_ELIGIBLE_CLEARING_ASSIGNMENT','external_groups',(SELECT count(*) FROM population_top3000_202608_202609_v1.continuation_clear_queue_v6 WHERE status='BLOCKED_EXTERNAL'),'unreconciled_claims',(SELECT count(*) FROM population_top3000_202608_202609_v1.continuation_clear_queue_v6 WHERE status='CLAIMED'));END IF;
 tok:=replace(gen_random_uuid()::text,'-','')||replace(gen_random_uuid()::text,'-','');
 UPDATE population_top3000_202608_202609_v1.continuation_clear_queue_v6 SET status='CLAIMED',attempts=attempts+1,claim_token_sha256=encode(sha256(convert_to(tok,'UTF8')),'hex'),claimed_at=clock_timestamp(),claim_expires_at=least(clock_timestamp()+interval '30minutes',not_after),updated_at=clock_timestamp() WHERE clearing_id=q.clearing_id;
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES('ENGINEERING_CLEARING_CLAIMED_V6',ctl||jsonb_build_object('clearing_id',q.clearing_id,'assignment_event_id',q.assignment_event_id,'executor',p_executor,'run_id',p_run_id,'input_sha256',q.input_sha256,'claim_expires_at',least(clock_timestamp()+interval '30minutes',q.not_after))) RETURNING event_id INTO ev;
 RETURN jsonb_build_object('state','CLAIMED','clearing_id',q.clearing_id,'claim_event_id',ev,'token',tok,'assignment',to_jsonb(q),'claim_expires_at',least(clock_timestamp()+interval '30minutes',q.not_after));
END;$f$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.continuation_finish_v6(p_owner text,p_fence bigint,p_stop bigint,p_id text,p_token text,p_receipt jsonb)
RETURNS jsonb LANGUAGE plpgsql SET search_path=pg_catalog AS $f$
DECLARE ctl jsonb;q population_top3000_202608_202609_v1.continuation_clear_queue_v6%rowtype;st text;ev bigint;
BEGIN
 ctl:=population_top3000_202608_202609_v1.continuation_guard_v6(p_owner,p_fence,p_stop);
 SELECT * INTO STRICT q FROM population_top3000_202608_202609_v1.continuation_clear_queue_v6 WHERE clearing_id=p_id FOR UPDATE;
 IF q.status IS DISTINCT FROM 'CLAIMED' OR q.owner_id IS DISTINCT FROM p_owner OR q.owner_fence IS DISTINCT FROM p_fence OR q.stop_generation IS DISTINCT FROM p_stop
 OR q.claim_token_sha256 IS DISTINCT FROM encode(sha256(convert_to(p_token,'UTF8')),'hex') OR q.claim_expires_at<=clock_timestamp() THEN RAISE EXCEPTION 'CONTINUATION_CLAIM_REQUIRED';END IF;
 IF jsonb_typeof(p_receipt) IS DISTINCT FROM 'object' OR p_receipt->>'effects_known' IS DISTINCT FROM 'true' THEN RAISE EXCEPTION 'CONTINUATION_EFFECTS_RECONCILIATION_REQUIRED';END IF;
 st:=p_receipt->>'state';
 IF st='COMPLETE' AND (q.kind='CLAIM_RECONCILIATION' OR q.family_id='RESOURCE') THEN
  IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_event e WHERE e.event_id=(p_receipt->>'independent_review_event_id')::bigint AND e.details->>'approved'='true' AND e.details->>'clearing_id'=q.clearing_id AND e.details->>'input_sha256'=q.input_sha256 AND nullif(e.details->>'reviewer','') IS NOT NULL AND e.details->>'reviewer' IS DISTINCT FROM e.details->>'producer')
  OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_event e WHERE e.event_id=(p_receipt->>'useful_event_id')::bigint AND e.details->>'effects_known'='true' AND e.details->>'owner_id'=p_owner AND e.details->>'owner_fence'=p_fence::text AND e.details->>'stop_generation'=p_stop::text AND e.details->>'clearing_id'=q.clearing_id AND ((q.kind='CLAIM_RECONCILIATION' AND ((e.event='ENGINEERING_ASSIGNMENT_EFFECTS_RECONCILED_RELEASE_V5' AND e.details->>'claim_event_id'=q.input_manifest->0->>'claim_event_id') OR (e.event='ENGINEERING_CLEARING_EFFECTS_RECONCILED_RELEASE_V6' AND e.details->>'clearing_claim_id'=q.input_manifest->0->>'clearing_claim_id'))) OR (q.family_id='RESOURCE' AND e.event='CONTINUATION_MEASURED_ADMISSION_V6' AND e.details->>'cumulative_counters_preserved'='true' AND e.details->>'original_deadline_preserved'='true')))
  OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_event ue WHERE ue.event_id=(p_receipt->>'useful_event_id')::bigint AND (
   EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task t WHERE t.task_id=(p_receipt->>'successor_task_id')::uuid AND (ue.details->>'successor_task_id'=t.task_id::text OR (t.input_versions->>'parent_clearing_id'=q.clearing_id AND t.input_versions->>'clearing_input_sha256'=q.input_sha256)))
   OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.continuation_clear_queue_v6 n WHERE n.clearing_id=p_receipt->>'successor_clearing_id' AND n.status IN('READY','CLAIMED','WAITING_OUTPUT') AND (ue.details->>'successor_clearing_id'=n.clearing_id OR EXISTS(SELECT 1 FROM jsonb_array_elements(n.input_manifest) child WHERE child->>'parent_clearing_id'=q.clearing_id AND child->>'clearing_input_sha256'=q.input_sha256)))))
  THEN RAISE EXCEPTION 'CONTINUATION_REVIEW_EFFECT_RECEIPT_SUCCESSOR_REQUIRED';END IF;
 ELSIF st='COMPLETE' THEN
  IF p_receipt->>'independent_review_event_id' !~ '^[0-9]+$' OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_event WHERE event_id=(p_receipt->>'independent_review_event_id')::bigint AND details->>'approved'='true' AND details->>'clearing_id'=q.clearing_id AND details->>'input_sha256'=q.input_sha256 AND details->>'producer' IS DISTINCT FROM details->>'reviewer' AND nullif(details->>'reviewer','') IS NOT NULL)
  OR p_receipt->>'useful_commit_id' IS NULL OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_commit WHERE commit_id=(p_receipt->>'useful_commit_id')::uuid AND new_committed_units>0 AND (task_id IN(SELECT (x->>'task_id')::uuid FROM jsonb_array_elements(q.input_manifest)x WHERE x ? 'task_id') OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task ct WHERE ct.task_id=population_top3000_202608_202609_v1.pipeline_commit.task_id AND ct.input_versions->>'clearing_id'=q.clearing_id AND ct.input_versions->>'clearing_input_sha256'=q.input_sha256)))
  OR p_receipt->>'successor_task_id' IS NULL OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task succ JOIN population_top3000_202608_202609_v1.pipeline_commit c ON c.commit_id=(p_receipt->>'useful_commit_id')::uuid JOIN population_top3000_202608_202609_v1.pipeline_task parent ON parent.task_id=c.task_id WHERE succ.task_id=(p_receipt->>'successor_task_id')::uuid AND succ.family_id=parent.family_id AND succ.input_versions=parent.input_versions AND ((succ.partition_key=parent.partition_key AND succ.stage=CASE parent.stage WHEN 'ACQUIRE' THEN 'NORMALIZE' WHEN 'NORMALIZE' THEN 'DERIVE' WHEN 'DERIVE' THEN 'VALIDATE' END AND succ.created_at>=c.committed_at) OR (parent.family_id='CTX02' AND parent.stage='ACQUIRE' AND succ.stage='NORMALIZE' AND EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.ctx02_census_page_v1 page WHERE page.acquire_task_id=parent.task_id AND c.receipt->'immutable_output_ids' @> jsonb_build_array(page.source_id) AND succ.partition_key=parent.partition_key||':page:'||page.page_number::text AND succ.payload->>'source_page_number'=page.page_number::text))))
  THEN RAISE EXCEPTION 'CONTINUATION_REVIEW_COMMIT_SUCCESSOR_REQUIRED';END IF;
 ELSIF st='BLOCKED_EXTERNAL' THEN
  IF length(coalesce(p_receipt->>'external_condition',''))<12 OR p_receipt->>'bounded_alternatives_evidence' IS NULL THEN RAISE EXCEPTION 'CONTINUATION_PRECISE_EXTERNAL_CONDITION_REQUIRED';END IF;
 ELSE RAISE EXCEPTION 'CONTINUATION_RECEIPT_STATE_INVALID';END IF;
 UPDATE population_top3000_202608_202609_v1.continuation_clear_queue_v6 SET status=st,last_receipt=p_receipt,updated_at=clock_timestamp() WHERE clearing_id=p_id;
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES('ENGINEERING_CLEARING_RESULT_V6',ctl||jsonb_build_object('clearing_id',p_id,'receipt',p_receipt,'original_task_holds_changed',false)) RETURNING event_id INTO ev;
 RETURN jsonb_build_object('state',st,'receipt_event_id',ev);
END;$f$;
REVOKE ALL ON FUNCTION population_top3000_202608_202609_v1.continuation_guard_v6(text,bigint,bigint),population_top3000_202608_202609_v1.continuation_classify_v6(text,text,text),population_top3000_202608_202609_v1.continuation_dispatch_v6(text,bigint,bigint),population_top3000_202608_202609_v1.continuation_claim_v6(text,bigint,bigint,text,text),population_top3000_202608_202609_v1.continuation_finish_v6(text,bigint,bigint,text,text,jsonb) FROM PUBLIC,anon,authenticated;
