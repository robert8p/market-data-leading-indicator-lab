# S02 all-page independent audit: bounded V3 execution

This is a read-only execution package for the sole controller. No source fetch, production parser, persistent mutation, timeout increase, provider-accuracy certificate or company-completeness certificate is included.

## Why the execution boundary changed

The original complete plan and combined V2 plan exceeded the existing 10-second tool/statement boundary. Root isolated staging at 2.233 seconds, canonical selection at 6.933 seconds, and observed membership at 8.782 seconds. The original expected-membership query exceeded 10 seconds. Its plan used a correlated membership lookup for every expected row. The V3 expected query performs the same distinct-unit membership comparison with a set join; root measured it at 1.963 seconds for 134,931 rows, with zero missing or unexpected units. Those diagnostic receipts are not reused as the final audit's before snapshot.

The V3 package gives each expensive part its own read-only transaction and result. Every part and assembly keeps statement_timeout=10s, lock_timeout=500ms, work_mem=16MB and an explicit UTC/ISO session. Connection continuity is unnecessary.

The first V3 observed query timed out when paired with expected. Root's EXPLAIN showed two materialized intermediates and two sorts, with an underestimated observed population. The final observed template removes those intermediates and hashes directly from the scoped observation table joined to the distinct canonical keys; it computes the canonical count once. Root's solo execution passed in 7.447 seconds at 16:19:06 UTC, with 134,931 observed units, zero missing/unexpected, and exactly the earlier `fb14ce912070f64d45c54c0832e306d96d7325a8a1bdd7ba0a6f5a351107c50a` manifest. Run observed alone both before and after the page audit. Its promoted template is byte-identical to the measured flattened query, SHA `fe1fa438650ec08ca83aa76eef42637fd87a6defcbb2799d22078a32be41cd2c`; no index, capacity or timeout change is included.

The flattened after query later exceeded 10 seconds because it still used ordered random heap access with a stale 8,863-row estimate for the actual 134,931 observations. Root inspected a materially different local plan using `SET LOCAL enable_indexscan=off`; it uses a fixed-PK bitmap heap scan and retains exactly the same selected values, ordering codec and membership calculation. That solo query passed in 3.638 seconds at 16:26:10.623864 UTC, with the same `fb14ce…` manifest. The final canonical observed template now includes only this local planner setting, SHA `551d4bdb50f544f7bbde0e2bbea6e56375458188bd5b1604a4e03be0fe283ebf` (1,796 bytes). The setting expires at transaction end; no persistent index, statistics, capacity or timeout change is included. Earlier failed attempts and successful diagnostic receipts remain preserved.

Root's complete actual final audit passed at 16:26:37.799301 UTC: 135 pages, 134,931 source occurrences/canonical/observed versions, zero duplicates, all 14 global gates and all four final gates zero, unchanged plan `dea0885076bbcf45986e6b1aa64fab44d2c98b2f31f69e252a904a8ed426a4a4`, page-receipt manifest `700e912f0a6728e0625ac320e9b5c794fff4b667fbf3e9de572e7e0dc27bebe4`. The exact final result is `PASS_SOURCE_TRANSFORMATION_ONLY`. `s02_independent_all135_actual_1626.json` is the actual result, not a local synthetic test. This closes the retained-source transformation audit; it does not mark the S02 company/publication dataset or full mission complete.

## Exact execution order

1. Keep source and producer tables stable during the audit. Run the four standalone files freshly: `s02_independent_staging_part_v3.sql`, `s02_independent_canonical_part_v3.sql`, `s02_independent_observed_part_v3.sql`, and `s02_independent_expected_part_v3.sql`. Preserve every complete returned object and its query hash.
2. Inside `s02_independent_plan_assemble_v3.sql`, after BEGIN and SET statements but before its WITH query, bind those complete objects using SET LOCAL `top3000.s02_partitioned_plan_staging`, `top3000.s02_partitioned_plan_canonical`, `top3000.s02_partitioned_plan_observed`, and `top3000.s02_partitioned_plan_expected`. Use properly escaped SQL literals or collision-checked dollar quoting. Do not expect another call's local settings to persist.
3. Save the complete resulting plan row. Require 135 pages, 134,931 source occurrences, the expected canonical/observed counts and every global mismatch equal to zero before starting pages. This result is a plan, not an audit PASS.
4. Run one actual page performance check under the unchanged 10-second bound. The page query is the unchanged `s02_independent_source_page_reconciliation.sql`; bind `top3000.s02_readback_page` and `top3000.s02_plan_sha256` inside its transaction. If it passes within bounds, run all 135 unique page checks. A valid first page from this exact fresh plan can count toward the 135; a diagnostic made with another plan cannot.
5. Only after all page calls finish, run all four standalone part files again. Preserve all four fresh after objects.
6. Run `s02_independent_global_assemble_v3.sql` with the four after-part objects and with `top3000.s02_plan_before` and `top3000.s02_page_receipts` set inside its transaction. The former is the compact before-plan object described below; the latter is the complete array of all 135 receipts.
7. Preserve the complete final row, every before/after object, all page receipts, executed SQL/template hashes and the complete before plan. Accept only `PASS_SOURCE_TRANSFORMATION_ONLY` with all four final gates zero and every global mismatch zero. Runtime or SQL failures remain failures and must not be converted into PASS.

## Compact before-plan fields

The final call does not need the large `pages_to_verify` array. Retain exactly these required fields from the full before plan:

- `check_revision`
- `plan_sha256`
- `source_pages`
- `source_occurrences`
- `global_mismatches`
- `observed_at`
- `preparation_method`
- `preparation_parts_manifest`
- `preparation_parts_latest_at`

Keeping `preparation_parts_earliest_at` and canonical/observed counts is harmless but not necessary for the final input gate. Preserve the full original plan separately.

## Integrity and result compatibility

The plan hash's original source/header/helper/row-version codec is unchanged. Staging retains exact raw-record links and xmin versions. Canonical selection retains the first `(page_number, source_row)` for every unit. Observed and expected membership retain the same fixed release/family/partition filters and hashes. Under the existing unique composite primary keys, missing units equal canonical distinct units minus actual units plus unexpected units.

The four supplied preparation objects have exact revision/archive/qualification checks. Every aggregate count must be explicitly present as a nonnegative integer within bigint range; null or missing global/final mismatch values reject the audit. Their complete content, excluding only `observed_at`, is hashed. Before and after content hashes must match. Before parts must be newer than producer completion; their latest time must precede the before plan; the plan must precede each page start. Every after-part observation must follow every page finish. Root also executes after calls sequentially after the page wave so their transaction snapshots are fresh. There is no claim that receipts continuously certify future table state.

The existing final revision is retained: `S02_ALL135_RETAINED_SOURCE_CANONICAL_RECONCILIATION_V1`. The existing four final gates remain `global_mismatch_fields`, `plan_before_changed`, `receipt_coverage`, and `receipt_content_or_page_binding`. A new global gate `preparation_parts_binding` is added; every existing global and page gate remains. The result adds preparation method and part-manifest/timestamp metadata.

All provider-accuracy, company-completeness, historical-availability and publication-admission flags remain false. Scheduled FINRA publication dates are not precise intraday availability. Full retained-source transformation reconciliation is a necessary source audit, not completion of the full 24-family population mission.
