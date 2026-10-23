# Compact D02 daily source lane — root installation notes

## Status and purpose

This is a local implementation draft for the existing US top-3,000-company population mission. It archives one unadjusted, non-OTC Massive grouped-daily response for each of the 42 frozen August–September sessions. August 3 reuses its already retained response. The original one-day D02 functions, raw document, generic observations, expected observations and family binding are preserved.

The worker regression suite passed locally. The SQL fixture must still be executed by root against the actual database; no live SQL test, deployment or provider call is claimed by this draft. Root owns all external mutations and budget admission.

The compact table holds source occurrences and exact PostgreSQL numeric values. It does not turn this all-market source into a certified company-day dataset. Company identity, the final company universe, eligible share classes, absence rules, corporate actions, daily close semantics and historical availability remain explicit downstream requirements.

## Files

- `d02_daily_compact_v2_schema_DRAFT.sql`: private source manifests, typed occurrences and batch proofs; fixed calendar and source contracts; acquisition helpers; bounded normalization/reconciliation; explicit quality hold; compatibility wrappers.
- `population-top3000-daily-v2_DRAFT.ts`: isolated Edge worker. Slug `population-top3000-daily-v2`; original `population-top3000-price-v1` is unchanged.
- `d02_daily_v2_worker_local_test.mjs`: actual worker-handler tests using fake DB/network, including all 42 admitted dates.
- `d02_daily_compact_v2_rollback_test_DRAFT.sql`: actual SQL test with synthetic edge cases, the actual preserved August 3 body, and a legacy delegation check. Everything rolls back.
- `d02_daily_compact_v2_registration_DRAFT.sql`: root-only first-wave registration, requiring actual current SQL and worker-auth evidence. Both new tasks are parked.
- `d02_daily_compact_v2_first_readback.sql`: bounded one-result source/runtime/storage readback.
- `d02_daily_compact_v2_expand_after_review_DRAFT.sql`: register the remaining 40 dates only after root records a real source/storage/runtime review. Those tasks are also parked.
- `d02_daily_v2_calendar_contract.json`: fixed date-list contract.

## Installation and first-wave sequence

1. Preserve root ownership/fence 2/stop generation 0. Ensure cron 596 is inactive and no D02 lease remains. Keep the current one-day repair changes and completed data.
2. Install the schema draft. It does not enable runtime, register adapters, enqueue tasks or call a provider.
3. Execute the rollback fixture with its 55-second statement timeout. It requires the current five original D02 stage adapters and the completed original NORMALIZE task. It captures those adapters within the rollback transaction and calls the new wrappers with real fences/tokens. It performs no Edge dispatch.
4. The fixture must return `result=PASS` and `receipt_event=D02_DAILY_V2_ROLLBACK_VERIFIED`. Root records the actual returned result as event details. Expected typed occurrences are 13,405: the 1,003-row synthetic source plus all 12,402 actual August 3 occurrences. The empty synthetic response adds no typed bar. The source-date manifests contain actual August 3 plus synthetic August 4 and August 5, all rolled back.
5. Inspect `actual_aug3_bounded_benchmark`: 13 normalization and 13 reconciliation batches, including the final current-hash verification of every earlier batch. Inspect the returned full typed manifest. Each individual batch remains capped at 1,000 rows; the terminal check scans at most 50,000 typed rows once, without repeated raw-page parsing per row.
6. Add only the new slug to the dispatcher allowlist, deploy the exact worker with the existing custom-token contract, and verify an invalid-token request returns HTTP 403 with zero provider-request increment. Record event `D02_DAILY_V2_WORKER_DEPLOYED_VERIFIED` with `result=PASS`, `slug=population-top3000-daily-v2`, exact `worker_sha256`, `invalid_token_http_status=403`, and `provider_requests_during_auth_probe=0`.
7. Execute first-wave registration. It requires the SQL evidence to match both the current new wrapper hashes and the captured original adapter hashes. It preserves the existing family binding and global scheduler state. It parks a retained August 3 ACQUIRE task and one fresh August 4 ACQUIRE task. Root releases these only when current runtime/resource admission is ready.
8. Complete those two real dates through the bounded stages. VALIDATE intentionally stops at `D02_V2_COMPANY_DAY_DENOMINATOR_AND_CLOSE_SEMANTICS_REQUIRED`. Source-stage completion is useful retained population; it is not final company completeness or an accuracy certification.
9. Use the first-wave readback to review exact bytes, records, batch runtimes, flags, conflicts and measured storage. If source integrity/runtime pass and projected storage fits the unchanged admission, record `D02_DAILY_V2_INITIAL_SOURCE_AND_STORAGE_REVIEWED` with actual evidence plus these required fields: `result=PASS`, current `daily_bundle_sha256`, current `calendar_manifest_sha256`, `first_two_sources_reconciled=true`, `projected_storage_within_current_admission=true`, `bounded_batch_runtime_verified=true`, `company_population_verified=false`.
10. The expansion draft then parks the remaining 40 exact frozen dates. Root can release them under the current provider request, HTTP, raw byte, relation byte, cost and deadline limits. It does not increase any budget or add a subscription.

## Actual rollback coverage

The synthetic response includes duplicates, changed versions of the same ticker, exact small decimal prices and volume above JavaScript’s safe integer range, a malformed long ticker, a wrong-date timestamp, inconsistent OHLC, explicit zero volume/trades, missing fields, and invalid negative/fractional counts. It retains all 1,003 source occurrences. Exact replay commits zero new rows; a forged typed value is rejected. A changed earlier batch after its proof is rejected by terminal reconciliation. A newly tightened current relation cap is enforced.

The actual August 3 response is processed through all 26 bounded calls. Its raw source remains in its original `source_document`; only a new source-day reference and typed rows are produced inside the rollback transaction. There is no request or storage-counter charge for reusing the existing body.

For legacy compatibility, the fixture temporarily leases the completed original NORMALIZE task with its original inputs and checkpoint. Direct invocation of the captured function and invocation through the wrapper must both reach the specific existing `D02_GROUPED_NORMALIZE_CHECKPOINT_MISMATCH` guard. The entire temporary lease branch rolls back. This proves preserved delegation and replay protection; it is not a new normalization success or a repair of the original stage.

The empty response is retained as an empty source. It creates no price/volume row. A partial HTTP 200 classified as a parser failure must retain an `UNCERTAIN_EFFECT` lease; it cannot silently release and retry.

## Source, scale and limits

- Exact source request: `https://api.massive.com/v2/aggs/grouped/locale/us/market/stocks/YYYY-MM-DD?adjusted=false&include_otc=false`.
- Provider is case-sensitive `Massive`. Existing key aliases only. No new paid add-on or entitlement is assumed.
- Frozen date-list hash: `188cc82e93e2c3446d9fba86cec682b996913997f56a2263d3fd680475998368`. The full calendar evidence manifest is calculated from the actual frozen calendar and pinned in task input versions.
- Exactly one provider request per admitted date and attempt; no pagination for this endpoint. A continuation is rejected and preserved as an unresolved source-contract issue, never truncated.
- Maximum source body 8 MiB, worker runtime 45 seconds, response 50,000 records, normalizer/reconciler batch 1,000 occurrences.
- Original August 3 source: 12,402 records, 1,355,997 bytes, SHA256 `787163eb9f6ffb408898c8a133c6ca63c822577187793d25c94b436f639bb97b`.
- At that observed daily scale, all 42 source dates would be approximately 520,884 raw occurrences and 56,951,874 raw bytes. The 41 fresh dates would add approximately 55,595,877 raw bytes. These are planning estimates, not a storage admission or a guarantee that every day has the same size. Typed-table/index growth must be measured in the first wave.
- Current mission relation limits are read from the actual admission, before and after mutations. They are not hardcoded to a new cap.

## Data meaning and preservation

`d02_daily_provisional_v2` exposes typed source occurrences and provenance privately. Invalid/missing fields remain flags plus nulls or the reported numeric values; the exact original record is recoverable from source body, ordinal and record hash. A missing source record is never converted to a zero-volume bar. Duplicates and conflicting versions remain source occurrences and are counted separately.

The provider aggregate timestamp is kept as milliseconds and a New York date candidate. Its position within the aggregate window remains unverified for this daily endpoint and is never labeled publication time. No company/security identifier, historical receipt timestamp, availability timestamp or publication eligibility is invented.

Mask bits are ticker 1, timestamp 2, open 4, high 8, low 16, close 32, share volume 64, trade count 128 and VWAP 256. `missing_mask` marks absent/null fields; `invalid_mask` marks malformed/range-invalid values; `structure_mask` bit 1 marks OHLC inconsistency and bit 2 marks a provider New York date inconsistent with the requested date. Optional fields can be missing without implying that a source record was absent. The quality hold determines downstream acceptability under the eventual family contract.

The original one-day adapter functions are never redefined. Their current OIDs and hashes are captured during tested registration and checked before every legacy delegation. Existing tasks retain their original inputs and checkpoints. A later change to an original function requires explicit reconciliation of the captured adapter rather than silently changing its meaning.
