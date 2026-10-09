-- Rollback only this physical index. No data/function/control rollback is needed.
DROP INDEX CONCURRENTLY IF EXISTS market_governance.remediation_change_v1_native_variant_run_idx;
