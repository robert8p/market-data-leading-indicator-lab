# Feature identity lookup optimization — 9 October 2026

Project: `oxzabweahkoimtevbbny`. Run: `market_data_remediation_20261008_v1`.

## Deployed change

Installed at 2026-10-09 07:11:18 UTC (08:11:18 Europe/London).
Added `market_governance.remediation_change_v1_native_variant_run_idx`, a 16 KiB partial index on `run_id` for `operation='EXACT_DATE_NATIVE_TRADING_VARIANT_CORRECTION'`.

The inner feature SQL repeatedly scanned the growing governance ledger to retrieve nine exact-date identity corrections. A 240-session batch performed 240 full scans, removing about 27,267 unrelated rows per scan. The index lets the same unmodified query fetch those nine rows directly.

## Measurement

Same bounded EXPLAIN ANALYZE query, 32 MB work_mem, batch 8540:
- Before: 19,162.870 ms.
- After: 8,302.019 ms.
- Elapsed reduction: 56.68%.
- Ledger access: 1,006,800 shared-buffer hits reduced to 960.
- Ledger scan: 38.567 ms per loop reduced to 0.014 ms per loop.
- No temporary-disk writes in either measured plan.

Normal worker observation through 07:12:48 UTC:
- 40 preceding batches averaged 13.840 seconds.
- First 11 batches after deployment averaged 7.300 seconds.
- Mean source-minute counts differed (33,255 before; 30,375 after).
- Source-minute processing per execution second increased from 2,403 to 4,161.
- All 11 new batch checks passed; no new error ledger entries.
These are short-window observations under live background load, not a guaranteed full-run speedup or completion ETA.

## Correctness and operational scope

Recomputed all 240 sessions / 32,881 retained minute rows from the profiled batch. Complete row comparison (excluding the new computation timestamp only), source hashes and output hashes had zero mismatches. The nine identity-correction rows retained their exact fingerprint. All five checked feature/builder/validation/RPC definitions retained their hashes. The worker remained running with the same worker ID and manifest.

No feature formulas, protected data, original manifest, grants, RLS, purchase, plan, instance count, subscription or allocated filesystem changes. No worker restart. Crypto pause remains active. Adding 16 KiB of index storage is usage within the existing allocation, not a paid capacity increase.

Security advisor check found only the existing table's informational RLS-without-policy condition in this scope; no pre-change advisor snapshot was taken, so this is not a global security delta audit. See https://supabase.com/docs/guides/database/database-linter?lint=0008_rls_enabled_no_policy .

## Reproduction and rollback

- `001_install.sql`: applied migration, scoped guards and installation ledger entry. Do not replay into the already-indexed project.
- `profile.sql`: original bounded read-only EXPLAIN ANALYZE query.
- `verify_parity.sql`: read-only complete row comparison; expected 240 rows and zero mismatches.
- `evidence/baseline_plan.json`, `evidence/indexed_plan.json`: full plans.
- `evidence/baseline_parity.json`, `evidence/verification.json`: before/after identity, function and output fingerprints, live checkpoint evidence.
- `rollback.sql`: remove only the physical index if a regression is independently verified. Do not run as part of ordinary verification.

This optimization is complete. Full market-data remediation, coverage reconciliation and consumer promotion remain incomplete.
