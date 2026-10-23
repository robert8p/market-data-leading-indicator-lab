# N02 dividend date-union adapter

The N02 draft uses two explicit, independently checkpointed queries on the documented **deprecated** `/v3/reference/dividends` endpoint. The first requests ex-dividend dates from 1 August through 30 September 2026; the second requests declaration dates in that same inclusive date range. Root verified that this endpoint documents both filter dimensions. The newer `/stocks/v1/dividends` page did not document a declaration-date request filter, so this implementation does not invent one. If the older endpoint is unavailable or not entitled, its known HTTP failure blocks for controller review.

## Ready files and evidence

- `massive_actions_n02_schema_DRAFT.sql`: separate N02 tables and functions; leaves existing N03 helpers untouched. Three new authenticated routing functions can forward N03 or N02 requests to their respective fenced helpers.
- `massive_actions_n02_rollback_test_DRAFT.sql`: real SQL fixture test within a rollback transaction. It must pass before admission. It requests no provider or Edge work.
- `population-top3000-actions-n02-expansion_DRAFT.ts`: proposed replacement for the existing `population-top3000-actions-v1` deployment. It supports N03 and exactly the two N02 route/partition pairs.
- `massive_actions_n02_expansion_local_test.mjs`: passed `PASS_LOCAL_N03_UNCHANGED_AND_N02_TWO_FIXED_ROUTES_EXACT_VALUES_EMPTY_PAGE_RULES_AUTH_REPLAY_AND_UNKNOWN_COMMIT` in the local fake-database/fake-network harness. These tests establish implementation behavior, not source accuracy.
- `massive_actions_n02_registration_DRAFT.sql`: first-page admission, one page per stream, under the unchanged shared resource controls.
- `massive_actions_n02_first_page_readback.sql`: bounded first-page integrity and union readback.

The combined worker SHA256 is `8011d85be03db0410a60e97a503744637af7d5904dead6fdd0e76fb7296a82bb`. The original N03 source remains unchanged at `f4cc10331a4217f36887521ce3d65d479f32e38315764380562c5d261a9cc757`.

## What the union does

Every source page and every returned record occurrence is retained. NORMALIZE verifies the stored response against its page manifest, then indexes at most 1,000 records per dispatch. It does not create provisional dividend observations yet.

DERIVE waits until **both** source streams have terminal, contiguous and fully normalized page chains. It then processes one retained page per dispatch. An identical event version returned several times, or returned in both date queries, is assigned once. Canonical assignment prefers the ex-date stream, then its earliest page and row. This ordering does not depend on which DERIVE task runs first. All source occurrences remain available in `massive_dividend_record_v1`; observations contain their occurrence count, a provenance-manifest hash and the exact lookup key.

Different record versions sharing a provider ID remain separate and are marked conflicting across the entire union. The source's optional fields remain unavailable when absent. Invalid dates, invalid numeric amounts, unknown dividend types and values outside the queried date dimension are retained and quarantined. An in-window declaration whose ex-date falls in October is correctly retained as part of the declaration window. Its future ex-date is not changed to fit the August–September period.

The source's date-only declaration does not establish an exact publication timestamp. Company/security identity, exact effective time, historical availability and original historical receipts remain unverified/null. No price adjustment is applied. Expected rows are a provisional deduplicated provider enumeration; they do not certify an independently established all-company dividend denominator. Both family partitions require the final company universe and remain unfrozen.

## Root sequence

1. Install the separate schema draft after review. No adapter registration or source task is created by this file. Existing N03 helper definitions are not replaced.
2. Run the rollback test when there are no live task/provider leases. It covers two source pages, eight retained record occurrences, six unique event versions, an identical event repeated within and across streams, two conflicting versions of the same ID, optional fields, exact large numeric values, a future ex-date attached to an in-window declaration, and a deliberately quarantined out-of-query result. It also proves the first stream cannot produce a premature partial union and an incorrect helper bundle cannot reserve a request. Declaration derivation runs first to verify deterministic canonical assignment.
3. Record only the actually observed PASS under `MASSIVE_ACTION_N02_ROLLBACK_VERIFIED`, including the returned `dividends_bundle_sha256`, `tested_function_sha256`, `provider_accuracy_verified: false` and `live_provider_requested: false`.
4. When no actions worker attempt is active, deploy the combined worker to the existing `population-top3000-actions-v1` slug. Preserve its existing environment and custom-token controls. Run the existing invalid-token probe and verify HTTP 403 with no provider-request counter increase. Record the observed deployed hash, local N03 regression PASS and probe under `MASSIVE_ACTION_N02_COMBINED_WORKER_DEPLOYED_VERIFIED` using the fields required by the registration SQL. This is an audited compatible worker update, not a claim that earlier N03 data passed a new source-accuracy check.
5. Run registration. It creates only two ACQUIRE tasks with `max_pages: 1` each, so the first reviewed wave can consume at most two provider requests. The existing shared 200-request additional allowance, 250 MB raw cap, relation cap, US$250 ceiling and mission deadline remain unchanged. There is no new subscription or add-on.
6. Inspect both first-page receipts. If either has a cursor, that stream blocks at its page cap. Review the actual source fields, resource usage and cursor chain before admitting a bounded cap increase on the **existing** task. Preserve its checkpoint and input versions. Do not reset page numbering or recreate an archive to work around a cap.
7. After both streams finish, inspect union counts and quarantine/conflict states. Final company mapping, independent event-census reconciliation, source accuracy samples and exact publication-time evidence remain required before certification.

The helper bundle hash in `input_versions.dividends_bundle_sha256` covers the N02 helper definitions, routing wrappers and the reused source-key/date helpers. A changed helper bundle blocks the old task before a provider request. Root must reconcile any intentional revision without silently resetting a retained archive.

The acquisition mechanics retain the established N03 safeguards: fixed host and routes, one request per attempt, no redirects or automatic retries, 8 MiB response bound, 45-second worker budget, exact numeric lexeme preservation, credential-redacted cursor storage, original application-response-body hash, rehashing before normalization, actual retained-byte accounting and unresolved reservations for unknown transport effects. The word `wire` in the hash field refers to response body bytes delivered by `fetch`, which may already be decompressed by the runtime.

No child performed a deployment, database mutation, live provider call or budget change.
