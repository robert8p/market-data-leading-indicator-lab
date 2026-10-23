# Retained daily feature production: D03, D04 and D05

This package creates an executable database-only calculation lane for all retained census ticker lines. It computes the three feature families once into compact, shared tables. It makes no source request and changes no existing producer, controller, budget, scheduler, universe or publication policy.

## Root execution order

1. Keep cron 596 paused and require no leased tasks or unreconciled provider leases. Verify the current owner / fence 2 / stop 0 and existing finite admission. This package never renews an allowance.
2. Execute `daily_shared_math_rollback_test_v1.sql`. The script installs the pure function within its transaction, checks eight independent numerical cases with 336 target rows, checks future causality and three extreme-input conditions, and rolls back. Persist the actual returned receipt under `DAILY_SHARED_MATH_FIXTURES_VERIFIED`. It must say `PASS_INDEPENDENT_NUMERIC_FIXTURES` and bind the current PostgreSQL function-definition hash. The local Python receipt is reference generation evidence, not SQL execution evidence.
3. Install `daily_shared_math_v1_DRAFT.sql`, then `daily_shared_pipeline_v1_DRAFT.sql`. The latter creates private tables/functions/view only. Both have revoked PUBLIC/anon/authenticated access; the tables have RLS enabled.
4. Execute `daily_shared_pipeline_rollback_test_v1.sql`. It uses actual retained data for 26 frozen census lines, explicitly including AAPL, MSFT and NVDA. It executes 25 + 1 normalization and derivation batches, confirms 2,678 input-grid rows and 1,092 target rows, tests the first 25 stop, exact replay and tamper rejection, and follows D04/D05 to their actual current qualification holds. All test changes roll back. Persist the actual result under `DAILY_SHARED_RETAINED26_ROLLBACK_VERIFIED` only if it returns `PASS_ACTUAL_RETAINED26_ROLLBACK`.
5. Review actual batch times and storage. Execute `daily_shared_registration_v1_DRAFT.sql` only after both matching actual receipts exist. It snapshots the existing census, installs 12 DATABASE adapters, and parks one D03 NORMALIZE task at infinity with `max_series=25`. It performs no provider call. Root may release this one task under the current finite allowance. It commits 25 series, then reaches the explicit first-batch resource-review stop without losing its checkpoint.
6. Use `daily_shared_progress_read_v1.sql` and the actual fixture timings to assess the first live batch. Root may persist `DAILY_SHARED_FIRST25_RESOURCE_REVIEW_VERIFIED` with the concrete fields required by the expansion SQL. The receipt must bind the run/bundle and actual 2,575 rows, give a conservative projected additional relation-size bound, a maximum measured batch time no greater than 20 seconds, and the requested final census-series limit. It must preserve the false company-certification flag.
7. Execute `daily_shared_expand_after_first25_v1_DRAFT.sql`. It requires the actual first 25 checkpoint and the existing remaining tick/storage/deadline allowance. It raises only this task's `max_series`, preserves the checkpoint and counters, and parks it pending root release. The full calculation then runs in batches of 25 with a live relation-size guard before and after each batch. D03 completion automatically queues D04/D05, which link to the same computed rows without duplicating values.
8. After calculation, perform independent full current input/output reconciliation and resolve the dated security, session, action, company-cohort and consumer-time requirements listed in the actual quality hold. This intermediary is not completion of the mission. Qualification, full publication and delivery remain subsequent controller work; the present package does not fabricate those outcomes.

The two mutation scripts deliberately require no prior D03/D04/D05 registration. If such work appears, reconcile it instead of overwriting it. A failed actual gate remains evidence; fix its cause and retain both failed and corrected receipts.

## Retained inputs and exact scope

The frozen input grid has 103 sessions: 61 retained support sessions from 5 May through 31 July 2026 and 42 target sessions from 3 August through 30 September 2026. The support date-list hash is pinned. The target calendar is the already frozen mission calendar. Missing source rows remain explicit absent grid rows; no missing daily row becomes a zero-volume observation.

The candidate census is preserved as source ticker lines, including lines whose company eligibility is under review. This protects against the known common-REIT exclusion risk. It does not convert13,044 ticker lines into 13,044 companies, freeze the requested 3,000-company cohort, or make unsupported symbol continuity assumptions.

Support reuse requires all of the following on each support date: exactly one candidate instrument mapping, matching share-class/composite FIGI anchor, exactly one valid dated STRONG security state, the retained certified historical state marker, date-scoped provenance, and the same ticker. A current symbol lookup alone is insufficient. The actual reference samples satisfy this test for the 61 support sessions. Their retained identity state ends 1 September; no September identity is extrapolated.

Target prices use the 42 independently reconciled D02 raw-source archives. Selection binds both the exact source day and ticker, with a limit of two to expose duplicates. Duplicate/conflicting occurrences are quarantined; the calculator does not pick an arbitrary close. Source counts, masks, exact selected-row hashes, source-header versions, frozen calendar metadata and split-record hashes are retained. The price series is explicitly a candidate until session semantics and dated security continuity are qualified.

May–July split inputs reuse the retained split ledger. August–September split candidates reuse the retained N03 event rows. Provider-ID conflicts, absent/invalid ratios and unknown mechanics remain marked. The present data do not establish exact original publication/receipt timestamps, so every consumer eligibility flag remains false.

## Formula semantics

| Family | Shared calculated values | Required guards |
|---|---|---|
| D03 | Raw and split-adjusted 1/5/20/60-session simple returns | Full scheduled window; no missing close skipped; no known identity break; adjusted series additionally needs all valid adjusted prices and no unknown intervening action. |
| D04 | Unannualized sample standard deviation of 20 adjusted log returns; separately named annualized log/simple volatility; relative volume and volume z score | Prior 20 volume observations exclude the current day; zero/missing denominators produce unavailable values with explicit flags. |
| D05 | Daily absolute adjusted simple return divided by positive dollar volume; 20-session mean | Dollar volume uses the retained provider dollar value or target VWAP×share volume. No close×volume fallback. All 20 valid inputs are required for the mean. |

The required `realized_log_vol_20d` follows the installed retained implementation's unannualized sample SD. Annualized variants remain separately named. The old retained functions have a hard pre-August cutoff and are preserved; this package ports their relevant arithmetic into a bounded pure function instead of modifying old releases.

Extreme raw-price, adjusted-ratio and variance inputs are bounded. An adjusted interval that becomes invalid cannot be skipped merely because its endpoints later become valid. Flag 1024 records this condition. The independent numerical fixture checks ordinary windows, splits, missing rows, zero volume, constant volume, unknown actions, identity breaks, future causality and extreme ranges.

## Capacity and task bounds

For 13,044 retained census lines, the fixed maximum is 1,343,532 input-grid rows and 547,848 target feature rows. The three feature families share those 547,848 rows. There are 522 NORMALIZE batches and 522 DERIVE batches; the initial planning bound is at most 1,054 useful adapter executions including family handoffs and the first 25 review stop. Other mission work and scheduler overhead require separate allowance.

The provisional additional relation-size planning bound is 1 GiB. This is not a measured guarantee: root must use the actual 26 fixture and first 25 live measurements before expansion. Every batch rechecks the current admitted mission relation-size cap; the scripts never increase it. The saved source manifest covers 42 headers, roughly 546 compact batch proofs and 297 retained split observations, rather than rehashing all 525,999 price records on every tick.

## Current evidence level

Local reference generation and peer code inspection can support root review. Only the controller's actual returned SQL receipts establish that the installed functions and retained data passed the stated gates. Successful arithmetic/source reconciliation does not independently verify provider prices, establish the exact company denominator, prove regular-session semantics, or recreate an original historical receipt that was never observed.
