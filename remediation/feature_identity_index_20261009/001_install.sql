-- Project oxzabweahkoimtevbbny / run market_data_remediation_20261008_v1
-- User authorization: "Perform the next useful optimisation", 2026-10-09.
-- Add only a physical access path; feature functions, rows, manifests, grants and controls are unchanged.
BEGIN;
SET LOCAL lock_timeout='2s';
SET LOCAL statement_timeout='30s';
DO $guard$
BEGIN
 IF NOT EXISTS (SELECT 1 FROM market_governance.remediation_run_v1
  WHERE run_id='market_data_remediation_20261008_v1' AND project_ref='oxzabweahkoimtevbbny'
  AND phase IN(3,4) AND status='IN_PROGRESS'
  AND protections#>>'{user_stop,active}'='false'
  AND protections#>>'{crypto_pause,active}'='true')
 THEN RAISE EXCEPTION 'active_authorized_noncrypto_run_required'; END IF;
END $guard$;
CREATE INDEX remediation_change_v1_native_variant_run_idx
 ON market_governance.remediation_change_v1 (run_id)
 WHERE operation='EXACT_DATE_NATIVE_TRADING_VARIANT_CORRECTION';
INSERT INTO market_governance.remediation_change_v1
 (change_id,run_id,finding_ids,relation_name,row_identity,before_row,after_row,operation,evidence,validation)
VALUES
 ('OPS:FEATURE_IDENTITY_INDEX:20261009_V1','market_data_remediation_20261008_v1',ARRAY['RT-07','IP-06'],
 'market_governance.remediation_change_v1',
 jsonb_build_object('index','market_governance.remediation_change_v1_native_variant_run_idx'),
 jsonb_build_object('access_path','240 repeated full ledger scans','profile_total_ms',19162.870,'ledger_scan_loops',240,'ledger_scan_total_ms',9256.08,'ledger_shared_hits',1006800),
 jsonb_build_object('index','market_governance.remediation_change_v1_native_variant_run_idx','predicate','operation = EXACT_DATE_NATIVE_TRADING_VARIANT_CORRECTION','status','INSTALLED_PENDING_VERIFICATION'),
 'FEATURE_IDENTITY_LOOKUP_PARTIAL_INDEX_INSTALLED',
 jsonb_build_object('authorization','Perform the next useful optimisation','profile_batch',8540,'profile_sessions',240,'profile_retained_minutes',32881,'source','bounded read-only EXPLAIN ANALYZE of original feature computation','baseline_output_sha256','29382a9bb4c1ab68a42dc05a360503ab0836f1cc4b909ece87fc3ca7bbeceff8'),
 jsonb_build_object('feature_logic_changed',false,'frozen_data_changed',false,'worker_restart',false,'plan_instance_subscription_changed',false,'full_remediation_complete',false));
COMMIT;
