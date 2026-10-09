BEGIN;
SET LOCAL lock_timeout='20s';
UPDATE market_governance.remediation_run_v1 SET protections=jsonb_set(protections,'{expedite_v2}','{"acquisition_overlap_enabled":true}') WHERE run_id='market_data_remediation_20261008_v1';
UPDATE market_governance.remediation_source_batch_v1 SET status='RUNNING',lease_expires_at=clock_timestamp()+interval '3 minutes' WHERE batch_id=(SELECT batch_id FROM market_governance.remediation_source_batch_v1 WHERE run_id='market_data_remediation_20261008_v1' AND provider='massive' AND source_type='massive_candles' AND status IN ('QUEUED','RETRY') AND (request_json->>'noncrypto_scope'='LISTED_EQUITY_GAPS_20261009_V1' OR request_json->>'required_parser_version'='massive_native_warmup_reference_page_20261008_v1') ORDER BY priority DESC,batch_id LIMIT 1);
SET LOCAL ROLE service_role;
DO $test$
DECLARE q jsonb;payload jsonb='{"run_id":"market_data_remediation_20261008_v1","worker_id":"compact-11111111-1111-1111-1111-111111111111","version":"compact_feature_finite_runner_20261008_v1","manifest_sha256":"db596062a61de56b36863ab2415772863e1764885b32356f4de89611703eccad","generation_id":"compact_full_20261008_v1","action":"status"}';
BEGIN
q=public.market_data_remediation_compact_feature_step_v1(payload);
IF q->>'status' NOT IN ('READY','LEASE_HELD') THEN RAISE EXCEPTION 'eligible_source_overlap_test_failed:%',q;END IF;
IF public.market_data_remediation_pipeline_v1('{"run_id":"market_data_remediation_20261008_v1","version":"remediation_sequential_handoff_20261008_v1","worker_id":"pipeline-11111111-1111-1111-1111-111111111111"}')->>'noncrypto_overlap_enabled' IS DISTINCT FROM 'true' THEN RAISE EXCEPTION 'pipeline_flag_failed';END IF;
END $test$;
RESET ROLE;
UPDATE market_governance.remediation_run_v1 SET protections=jsonb_set(protections,'{expedite_v2}','{}') WHERE run_id='market_data_remediation_20261008_v1';
SET LOCAL ROLE service_role;
DO $test$
DECLARE q jsonb;
BEGIN
q=public.market_data_remediation_compact_feature_step_v1('{"run_id":"market_data_remediation_20261008_v1","worker_id":"compact-11111111-1111-1111-1111-111111111111","version":"compact_feature_finite_runner_20261008_v1","manifest_sha256":"db596062a61de56b36863ab2415772863e1764885b32356f4de89611703eccad","generation_id":"compact_full_20261008_v1","action":"status"}');
IF q->>'reason' IS DISTINCT FROM 'exclusive_materialization_lane_required' THEN RAISE EXCEPTION 'disabled_overlap_not_fail_closed:%',q;END IF;
END $test$;
ROLLBACK;
SELECT jsonb_build_object('actual_role_overlap_permitted',true,'missing_control_fail_closed',true,'rolled_back',true) validation;
