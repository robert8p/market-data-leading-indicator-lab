CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.ref01_date_continue_v9(p_owner text,p_fence bigint,p_stop bigint)
RETURNS jsonb LANGUAGE plpgsql SET search_path=pg_catalog AS $f$
DECLARE ctl jsonb; result jsonb; err text; errstate text; manifest jsonb;h text;integrity_fault boolean;
BEGIN
 -- Owner, user stop, scope and global admission rejection are outside fault isolation.
 ctl:=population_top3000_202608_202609_v1.continuation_guard_v6(p_owner,p_fence,p_stop);
 BEGIN
  result:=population_top3000_202608_202609_v1.ref01_date_continue_impl_v9(p_owner,p_fence,p_stop);
  RETURN result;
 EXCEPTION WHEN OTHERS THEN
  GET STACKED DIAGNOSTICS err=MESSAGE_TEXT,errstate=RETURNED_SQLSTATE;
  -- The failed implementation subtransaction has no committed effects. Hold only its source lane.
  manifest:=jsonb_build_array(jsonb_build_object('plan_ids',(SELECT jsonb_agg(plan_id ORDER BY plan_id) FROM population_top3000_202608_202609_v1.ref01_date_plan_v9 WHERE enabled),
   'error_state',errstate,'error',left(err,500),'installed_bundle',population_top3000_202608_202609_v1.ref01_date_bundle_v4()));
  h:=encode(sha256(convert_to(manifest::text,'UTF8')),'hex');
  SELECT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.ref01_date_plan_v9 p WHERE p.enabled AND
   (p.code_bundle_sha256 IS DISTINCT FROM population_top3000_202608_202609_v1.ref01_date_bundle_v4()
    OR p.content_sha256 IS DISTINCT FROM encode(sha256(convert_to(p.content::text,'UTF8')),'hex')
    OR p.content->>'owner_id' IS DISTINCT FROM p_owner OR p.content->>'owner_fence' IS DISTINCT FROM p_fence::text OR p.content->>'stop_generation' IS DISTINCT FROM p_stop::text)) INTO integrity_fault;
  UPDATE population_top3000_202608_202609_v1.ref01_date_plan_v9 SET enabled=false,last_blocker='IMPLEMENTATION_REPAIR',updated_at=clock_timestamp() WHERE enabled;
  UPDATE population_top3000_202608_202609_v1.pipeline_task SET status='BLOCKED',blocker_code=CASE WHEN integrity_fault THEN 'CODE_INTEGRITY' ELSE 'REF01_DATE_PLAN_IMPLEMENTATION_HOLD_V9' END,blocker_until=NULL,
   resumption_condition=jsonb_build_object('clearing_id','ref01-date-code:'||h,'required_action','Repair and independently verify exact dated-source plan; preserve source hashes, attempts and counters')
   WHERE family_id='REF01' AND payload->>'kind'='REF01_CENSUS_SOURCE_V1' AND (stage='ACQUIRE' OR integrity_fault) AND status='READY';
  INSERT INTO population_top3000_202608_202609_v1.continuation_clear_queue_v6(clearing_id,kind,family_id,blocker_code,input_manifest,input_sha256,status,owner_id,owner_fence,stop_generation,scope_sha256,not_after,last_receipt)
  VALUES('ref01-date-code:'||h,'IMPLEMENTATION_REPAIR','REF01','REF01_DATE_PLAN_IMPLEMENTATION_HOLD_V9',manifest,h,'READY',p_owner,p_fence,p_stop,ctl->>'scope_sha256',(ctl->>'not_after')::timestamptz,
   jsonb_build_object('effects_known',true,'failed_handler_transaction_rolled_back',true,'execution_recipe','Reconcile exact dated source plan code/hash or malformed-state failure. Implement narrowly reviewed correction; prove rollback tests, retain source originals and counts, then re-enable exact plan and only named implementation holds. Independent mission lanes continue.')) ON CONFLICT DO NOTHING;
  INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details)
   SELECT 'REF01_DATE_PLAN_FAILURE_ISOLATED_V9',ctl||jsonb_build_object('clearing_id','ref01-date-code:'||h,'error',left(err,500),'error_state',errstate,'failed_subtransaction_rolled_back',true,'family_accepted',false)
   WHERE NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_event WHERE event='REF01_DATE_PLAN_FAILURE_ISOLATED_V9' AND details->>'clearing_id'='ref01-date-code:'||h);
  RETURN jsonb_build_object('state','REF01_PLAN_HELD_REPAIR_DISPATCHED','clearing_id','ref01-date-code:'||h);
 END;
END;$f$;
REVOKE ALL ON FUNCTION population_top3000_202608_202609_v1.ref01_date_continue_v9(text,bigint,bigint) FROM PUBLIC,anon,authenticated;
