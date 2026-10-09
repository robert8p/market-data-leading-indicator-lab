CREATE INDEX IF NOT EXISTS remediation_noncrypto_ready_20261009_idx ON market_governance.remediation_source_batch_v1(run_id,source_type,priority DESC,batch_id)
 WHERE provider='massive' AND status IN('QUEUED','RETRY') AND (request_json->>'noncrypto_scope'='LISTED_EQUITY_GAPS_20261009_V1' OR request_json->>'required_parser_version'='massive_native_warmup_reference_page_20261008_v1');
CREATE INDEX IF NOT EXISTS remediation_noncrypto_completed_20261009_idx ON market_governance.remediation_source_batch_v1(run_id,completed_at DESC)
 WHERE provider='massive' AND status='COMPLETE' AND (request_json->>'noncrypto_scope'='LISTED_EQUITY_GAPS_20261009_V1' OR request_json->>'required_parser_version'='massive_native_warmup_reference_page_20261008_v1');
CREATE OR REPLACE FUNCTION public.market_data_remediation_noncrypto_claim_v1(p_request jsonb)
RETURNS jsonb LANGUAGE plpgsql SET search_path='pg_catalog' SET statement_timeout='30s' SET lock_timeout='5s' AS $$
DECLARE rid text=p_request->>'run_id'; wid text=p_request->>'worker_id';
 r market_governance.remediation_run_v1%rowtype;
 q market_governance.remediation_source_batch_v1%rowtype; g jsonb; cfg jsonb; n integer; last_type text; chosen_type text;
 base jsonb=jsonb_build_object('version','noncrypto_equity_handoff_20261009_v1','run_id','market_data_remediation_20261008_v1');
BEGIN
 IF rid IS DISTINCT FROM 'market_data_remediation_20261008_v1'
 OR p_request->>'version' IS DISTINCT FROM 'noncrypto_equity_handoff_20261009_v1'
 OR coalesce(wid,'') NOT LIKE 'remediation:%' OR length(wid)>160 THEN RAISE EXCEPTION 'noncrypto_request_invalid';END IF;
 IF NOT pg_try_advisory_xact_lock(hashtextextended(rid||':source_claim',0)) THEN RETURN base||jsonb_build_object('status','idle','reason','source_claim_busy');END IF;
 SELECT * INTO STRICT r FROM market_governance.remediation_run_v1 WHERE run_id=rid;
 cfg=r.protections->'noncrypto_runner_v1';
 IF cfg->>'enabled' IS DISTINCT FROM 'true' OR cfg->>'version' IS DISTINCT FROM 'noncrypto_equity_handoff_20261009_v1'
 OR r.protections#>>'{noncrypto_priority_v1,active}' IS DISTINCT FROM 'true' THEN
  RETURN base||jsonb_build_object('status','idle','reason','noncrypto_route_disabled');END IF;
 g=market_governance.remediation_worker_gate_v1(rid);
 IF g->>'ready' IS DISTINCT FROM 'true' THEN RETURN base||g||jsonb_build_object('status','idle');END IF;
 UPDATE market_governance.remediation_source_batch_v1 b SET status=CASE WHEN attempts>=3 THEN 'FAILED' ELSE 'RETRY' END,
 attempt_history=attempt_history||jsonb_build_array(jsonb_build_object('attempt',attempts,'status','LEASE_EXPIRED','source_id',current_source_id,'validation',validation,'completed_at',clock_timestamp())),
 last_error=jsonb_build_object('code','expired_noncrypto_worker_lease'),worker_id=null,lease_token=null,lease_expires_at=null,updated_at=clock_timestamp()
 WHERE run_id=rid AND status='RUNNING' AND lease_expires_at<clock_timestamp() AND market_governance.remediation_noncrypto_task_v1(to_jsonb(b));
 IF EXISTS(SELECT 1 FROM market_governance.remediation_source_batch_v1 WHERE run_id=rid AND status='RUNNING' AND lease_expires_at>=clock_timestamp()) THEN
  RETURN base||jsonb_build_object('status','idle','reason','existing_source_lease');END IF;
 SELECT b.source_type INTO last_type FROM market_governance.remediation_source_batch_v1 b
 WHERE b.run_id=rid AND b.provider='massive' AND b.status='COMPLETE'
 AND (b.request_json->>'noncrypto_scope'='LISTED_EQUITY_GAPS_20261009_V1'
 OR b.request_json->>'required_parser_version'='massive_native_warmup_reference_page_20261008_v1')
 ORDER BY b.completed_at DESC LIMIT 1;
 FOR chosen_type IN SELECT unnest(CASE WHEN last_type='massive_reference_tickers' THEN ARRAY['massive_candles','massive_reference_tickers'] ELSE ARRAY['massive_reference_tickers','massive_candles'] END) LOOP
  SELECT b.* INTO q FROM market_governance.remediation_source_batch_v1 b
  WHERE b.run_id=rid AND b.provider='massive' AND b.status IN('QUEUED','RETRY') AND b.attempts<3 AND b.not_before<=clock_timestamp()
  AND b.source_type=chosen_type
  AND (b.request_json->>'noncrypto_scope'='LISTED_EQUITY_GAPS_20261009_V1'
  OR b.request_json->>'required_parser_version'='massive_native_warmup_reference_page_20261008_v1')
  AND CASE WHEN b.source_type='massive_reference_tickers' THEN p_request#>>'{source_capabilities,massive_warmup_reference}'=b.request_json->>'required_parser_version'
  ELSE p_request#>>'{source_capabilities,massive_candles}'=b.request_json->>'required_parser_version' END
  ORDER BY b.priority DESC,b.batch_id FOR UPDATE OF b SKIP LOCKED LIMIT 1;
  EXIT WHEN FOUND;
 END LOOP;
 IF q.batch_id IS NULL THEN RETURN base||jsonb_build_object('status','idle','reason','no_eligible_noncrypto_tasks_pending_or_dependency_blocked','full_remediation_complete',false);END IF;
 IF NOT market_governance.remediation_noncrypto_task_v1(to_jsonb(q)) THEN RAISE EXCEPTION 'noncrypto_exact_task_contract_invalid';END IF;
 UPDATE market_governance.remediation_source_batch_v1 SET status='RUNNING',attempts=attempts+1,worker_id=wid,
 lease_token=gen_random_uuid(),lease_expires_at=clock_timestamp()+interval '3 minutes',heartbeat_at=clock_timestamp(),
 current_source_id=null,acquired_count=0,valid_count=0,invalid_count=0,duplicate_count=0,outside_count=0,
 validation='{}',completed_at=null,updated_at=clock_timestamp(),last_error=null WHERE batch_id=q.batch_id RETURNING * INTO q;
 RETURN base||g||jsonb_build_object('status','claimed','batch',to_jsonb(q));
END $$;
