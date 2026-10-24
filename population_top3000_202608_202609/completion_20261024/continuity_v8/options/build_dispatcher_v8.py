from pathlib import Path
p=Path('continuity_v7/options_auto/dispatcher_v7.sql').read_text()
p=p.replace('usage_bytes bigint;','usage_bytes bigint;policy_generation text;')
needle="  k:=encode(sha256(convert_to(p_owner||':'||p_fence||':'||p_stop||':'||q.family_id||':'||coalesce(q.blocker_code,'')||':'||h,'UTF8')),'hex');"
replacement="""  -- Reviewed policy generation gives an exact failed/old admission a new immutable clearing identity.
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
  END IF;"""
assert needle in p
p=p.replace(needle,replacement)
needle="AND nullif(e.details->>'reviewer','') IS NOT NULL AND e.details->>'reviewer' IS DISTINCT FROM e.details->>'producer') THEN RAISE EXCEPTION 'CONTINUATION_OPERATIONAL_REVIEW_REQUIRED';END IF;"
replacement="AND nullif(e.details->>'reviewer','') IS NOT NULL AND nullif(e.details->>'producer','') IS NOT NULL AND e.details->>'reviewer' IS DISTINCT FROM e.details->>'producer' AND e.details->>'handler_bounds_sha256'=encode(sha256(convert_to(handler.bounds::text,'UTF8')),'hex')) THEN RAISE EXCEPTION 'CONTINUATION_OPERATIONAL_REVIEW_REQUIRED';END IF;"
assert needle in p
p=p.replace(needle,replacement)
p=p.replace("AND input_manifest->0->>'task_id'='d377d980-5dbd-4276-a2c9-d5638c60b668' AND not_after>clock_timestamp() AND attempts<max_attempts ORDER BY created_at LIMIT 1 FOR UPDATE;", "AND handler.bounds->'request_policies' ? (input_manifest->0->>'task_id') AND not_after>clock_timestamp() AND attempts<max_attempts\n AND NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.continuation_clear_queue_v6 other WHERE other.status IN('CLAIMED','WAITING_OUTPUT') AND other.clearing_id<>continuation_clear_queue_v6.clearing_id AND jsonb_array_length(other.input_manifest)=1 AND other.input_manifest->0->>'task_id'=continuation_clear_queue_v6.input_manifest->0->>'task_id')\n ORDER BY created_at,clearing_id LIMIT 1 FOR UPDATE;")
p=p.replace("'independent_review_event_id',handler.review_event_id)", "'independent_review_event_id',handler.review_event_id,'policy_sha256',encode(sha256(convert_to(handler.bounds::text,'UTF8')),'hex'))")
p=p.replace("'failed_handler_transaction_rolled_back',true),updated_at", "'failed_handler_transaction_rolled_back',true,'policy_sha256',encode(sha256(convert_to(handler.bounds::text,'UTF8')),'hex')),updated_at")
# The READY group's assignment is still exact-engineering recovery when a real resource bound blocks it.
old="'source_task_unchanged',true));"
new="'source_task_unchanged',true,'policy_sha256',encode(sha256(convert_to(handler.bounds::text,'UTF8')),'hex'),'required_successor',CASE WHEN SQLERRM='OPTIONS_V8_MEASURED_RESOURCE_REVIEW_REQUIRED' THEN 'RESOURCE_CAPACITY_PROBE_AND_MEASURED_ADMISSION' ELSE 'EXACT_GUARDED_DEFECT_OR_EVIDENCE_REVIEW' END));"
assert old in p
p=p.replace(old,new)
Path('continuity_v8/options/dispatcher_v8.sql').write_text(p+'\nREVOKE ALL ON FUNCTION population_top3000_202608_202609_v1.continuation_dispatch_v6(text,bigint,bigint) FROM PUBLIC,anon,authenticated;\n')
