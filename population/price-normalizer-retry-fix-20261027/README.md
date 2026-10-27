# Price-normalizer retry defects: patched and tested

## Scope and current status

The user's 27 October request was to fix the two defects found in independent review 93373 of proposed all-session August normalizer candidate 89183. The review was correct and is preserved. The defect repair is implemented in new immutable candidate **94953**, with regression SQL **95014** and actual producer test receipt **95120**. It has **not** been deployed or independently approved for deployment.

## Defects corrected

1. A retry previously accepted an altered stored VWAP while returning COMPLETE with zero new rows. The new code compares every stored observation with its freshly derived immutable source representation: all values, timestamps, company/security identity, row/version hashes and full lineage. Floating-point fields also receive bitwise comparisons. A conflicting output raises `NORMALIZED_OUTPUT_CONFLICT` (or the retained narrower version/count guard), without silently overwriting the conflicting record.
2. A retry previously accepted a fabricated `expected_minute_bars=390` even though the required state was unknown/NULL. The new code compares the entire expectation record, including the NULL minute denominator, observed count, field list, company/date/session, qualification metadata and normalizer identity. A conflict raises `NORMALIZATION_EXPECTATION_CONFLICT`.

The partition manifest is locked during normalization to serialize calls for the same manifest. Valid retries remain idempotent, and legitimate partial-output recovery remains supported.

## Actual PostgreSQL verification

The unchanged original 24 checks were rerun successfully first. The expanded suite then completed at **2026-10-27T03:40:36.838975Z**:

| Check group | Passed |
|---|---:|
| Original regression and native-handoff checks | 24 |
| Adversarial persisted-output/expectation retries | 84 |
| Valid retries and partial-output recovery | 6 |
| Total distinct checks in the expanded suite | **114** |

The suite used **945 retained source bars**, comprising 318 premarket, 390 regular-session and 237 postmarket bars, and compared **6,615 numerical fields**. Both review counterexamples were rejected in every session. The 84 adversarial cases cover 28 mutations in each session, including all price/volume/trade-count values, identity, date/session, hashes, lineage and expectation metadata. Each rejection was checked not to silently rewrite the corrupt fixture. Native completion and automatic DERIVE successor behavior were exercised.

All tests were actual database rollback tests, not a static-only assessment. The test transaction was rolled back. Fresh readback at **03:42:52 GMT** confirmed:

- Zero remaining fixture tasks.
- **13,406** existing retained observation rows preserved.
- Proposed normalizer v4 absent from installed functions.
- Candidate event 94953 and test proof 95120 retained.
- No new source requests or production adapter activation.

An initial readmission-fixture schema-name typo failed at compilation before effects. It is not counted as a passed check. The first code patch passed both test suites without a further code repair.

## Exact reproducibility pins

- Original candidate source SHA-256: `e813dd085b2b93a170a93123e507d7aeb7f76db3b76e67c6780c9e98a9e78a8e`
- Repaired complete candidate source SHA-256: `2ea06221f85c42d06640fe948321525f742becba1060e214a1fec8eb0bab1a9f`
- Repaired PostgreSQL definition SHA-256: `223bdc2dbfbce5ef188cc4b04edb9b485e726d10a2ac513566cd145380abd73b`
- Expanded test SQL SHA-256: `930ecebbd103bcf839aa4166d959c9275ac5d1853c45108e60cdd7d1016572c9`

The complete candidate and test bodies are stored in the mission's private immutable event records 94953 and 95014. `retry_checks.sql` preserves the new code fragment; `verify_repair_rollback.sql` is the pinned reproduction harness. Neither file authorizes deployment.

## Independent review and operational restriction

Producer test results are not independent re-approval. A subsequent combined SQL transaction to record the foreground claim's completion/release, register an independent re-review/integration successor and supersede the old assignment was **rejected by OpenAI safety checks**. Readback established zero corresponding completion/release rows and zero new successor assignments. Claim 94867 must not be force-released or treated as safely reconciled merely because its time limit expires.

The rejected write was not retried, split, disguised, delegated or rerouted. The existing automation was notified of the exact restriction and asked to perform a separate **read-only** examination of the saved patch and test evidence, without reproducing the denied state transitions or deploying. An immediate run was requested; that request is not proof of a completed review.

Compatible all-session downstream processing/routing, genuine independent approval and legitimate resolution of the blocked workflow update remain required before activation. This bug fix does not certify September coverage, the full cohort, source economic accuracy, point-in-time history or either complete price family. Other admitted data work is unaffected.
