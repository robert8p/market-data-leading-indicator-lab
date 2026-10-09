# Non-crypto remediation handoff repair — 9 October 2026

The crypto pause disabled all source claims, and the only background handoff led to Binance/Coinbase after the feature build. The equity priority list had no executable source route. A second defect was the absence of the required historical warmup parser in the deployed worker.

This repair adds a separately controlled, service-role-only non-crypto claim route; retains the global legacy source pause; blocks crypto claims, commits and heartbeats during the crypto pause; and interleaves source work with existing feature batches on the existing Render worker. An indexed selector alternates historical-reference and listed-price work without scanning the large crypto queue. No new service, subscription, instance, plan or schedule is created.

The queue includes the already approved 61 historical reference dates and 45,493 new native-symbol price tasks covering 102,802 affected listing-days (102,794 missing retained rollups and eight guarded alias cases). Price requests group consecutive affected sessions within a calendar month. Native case is preserved; raw artifacts, actual receipt, source hashes and exact affected-day evidence remain private. Empty responses do not establish no trades, irrecoverability or completed remediation. Neither task completion nor staging population constitutes canonical promotion.

Validation: 22 Python tests including original pipeline regressions; rolled-back service-role claims/heartbeats, exclusive lease, crypto rejection, stop and grants checks. Queue reconciliation verified every task's non-crypto contract and the fixed window. Cryptographic source receipts and live deployment must also be verified before recording operational success.

Reproduction: apply the SQL files in numbered order, then run sql/test_actual_role.sql (rolls back) and the Python tests. Database controls remain disabled until the verified code is deployed. The canonical project is oxzabweahkoimtevbbny and run is market_data_remediation_20261008_v1. Preserve raw/frozen data and all unrelated stopped work.

Still outstanding: independent source/identity reconciliation, historical availability classification, consumer cutover, canonical promotion, and other non-crypto family-specific remediation. The feature materialization and acquisition route cannot independently close those findings.
