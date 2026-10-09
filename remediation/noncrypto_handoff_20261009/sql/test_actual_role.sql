BEGIN;
SET LOCAL statement_timeout='30s';
SET LOCAL lock_timeout='10s';
UPDATE market_governance.remediation_run_v1 SET protections=jsonb_set(protections,'{noncrypto_runner_v1}','{"enabled":true,"version":"noncrypto_equity_handoff_20261009_v1"}') WHERE run_id='market_data_remediation_20261008_v1';
SET LOCAL ROLE service_role;
DO $$ DECLARE r jsonb;b jsonb;h jsonb;req jsonb='{"run_id":"market_data_remediation_20261008_v1","worker_id":"remediation:rollback-validation","version":"noncrypto_equity_handoff_20261009_v1","source_capabilities":{"massive_warmup_reference":"massive_native_warmup_reference_page_20261008_v1","massive_candles":"ohlc_source_rules_20261008_v3_native_symbol_quote_contract"}}'; BEGIN
 r=public.market_data_remediation_noncrypto_claim_v1(req);b=r->'batch';
 IF r->>'status' IS DISTINCT FROM 'claimed' OR market_governance.remediation_noncrypto_task_v1(b) IS DISTINCT FROM true THEN RAISE EXCEPTION 'claim_failed:%',r;END IF;
 h=jsonb_build_object('run_id',req->>'run_id','worker_id',req->>'worker_id','batch_id',b->'batch_id','lease_token',b->>'lease_token');
 IF public.market_data_remediation_heartbeat_v1(h)->>'renewed' IS DISTINCT FROM 'true' THEN RAISE EXCEPTION 'heartbeat_failed';END IF;
 IF public.market_data_remediation_noncrypto_claim_v1(req)->>'reason' IS DISTINCT FROM 'existing_source_lease' THEN RAISE EXCEPTION 'lease_failed';END IF;
 IF public.market_data_remediation_claim_v1(req)->>'reason' IS DISTINCT FROM 'legacy_source_route_paused_use_noncrypto_route' THEN RAISE EXCEPTION 'legacy_crypto_guard_failed';END IF;
 IF market_governance.remediation_noncrypto_task_v1(b||'{"provider":"coinbase","source_type":"coinbase_candles","symbol":"BTC-USD"}') THEN RAISE EXCEPTION 'crypto_eligibility_failed';END IF;
 IF has_function_privilege('anon','public.market_data_remediation_noncrypto_claim_v1(jsonb)','EXECUTE') OR has_function_privilege('authenticated','public.market_data_remediation_noncrypto_claim_v1(jsonb)','EXECUTE') THEN RAISE EXCEPTION 'public_grants_failed';END IF;
END $$;
RESET ROLE;
UPDATE market_governance.remediation_run_v1 SET protections=jsonb_set(protections,'{user_stop,active}','true') WHERE run_id='market_data_remediation_20261008_v1';
SET LOCAL ROLE service_role;
DO $$ DECLARE r jsonb;BEGIN
 r=public.market_data_remediation_noncrypto_claim_v1('{"run_id":"market_data_remediation_20261008_v1","worker_id":"remediation:rollback-validation","version":"noncrypto_equity_handoff_20261009_v1"}');
 IF r->>'ready' IS DISTINCT FROM 'false' THEN RAISE EXCEPTION 'stop_guard_failed:%',r;END IF;
END $$;
SELECT 'PASS: actual service-role price claim, heartbeat, exclusive lease, legacy/crypto rejection, access grants and user stop; all rolled back' result;
ROLLBACK;
