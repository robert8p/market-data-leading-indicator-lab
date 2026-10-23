# N03 first-page source archive: root execution notes

This bundle implements **N03 splits only**. N01 news and N02 dividends are not admitted by these files. It uses the existing Massive subscription, the existing mission budget gate, the unchanged company scope and the existing deadline. It does not freeze a company universe, certify a denominator, invent source publication timestamps or apply current cumulative adjustment factors to historical prices.

## Files and observed status

| File | Purpose | Current evidence |
|---|---|---|
| `population-top3000-actions-v1.ts` | Bounded Edge source-page worker | Local fake-network tests passed; root must verify deployment |
| `massive_actions_n03_schema_DRAFT.sql` | Private archive-page table and fenced acquire/claim/commit/failure/normalize/derive functions | Draft for root installation; child performed no database writes |
| `massive_actions_n03_rollback_test_DRAFT.sql` | Real SQL fixture test, entirely rolled back | Must run and pass on the current deployed schema; no provider calls |
| `massive_actions_n03_local_test.mjs` | Local authentication, URL, numeric preservation, size, transport and JSON-driver tests | Passed `PASS_LOCAL_N03_AUTH_REPLAY_SCOPE_CURSOR_HASH_PRECISION_SIZE_CLASSIFIED_FAILURES_AND_JSON_DRIVER_BOUNDARY` |
| `massive_actions_n03_registration_DRAFT.sql` | Guarded one-page production admission | Requires actual root-recorded SQL and deployed-worker evidence |
| `massive_actions_n03_first_page_readback.sql` | Bounded retained-source integrity and shape readback | Run after the first real source page |

## Root execution sequence

1. Inspect and install the schema draft. It creates no tasks and enables no provider activity. If any earlier version of the page table exists, inspect its shape first; the latest definition requires a `source_sha256` manifest column. All new functions and the table are private to the control path; do not add public API grants.
2. Run the rollback test while no task/provider leases are active. The transaction takes the dispatcher advisory lock, temporarily moves unrelated eligible tasks aside, creates clearly labelled synthetic source fixtures, tests the fenced lifecycle, then rolls everything back. A returned test PASS is engineering evidence only; `provider_accuracy_verified` and `live_provider_requested` remain false. Preserve the full returned `tested_function_sha256` map.
3. Deploy the exact worker as `population-top3000-actions-v1` with `verify_jwt=false`. The worker itself requires the current 64-character pipeline attempt token, project URL, owner fence, stop generation, admitted resource budget and lease before any provider request. Existing environment variables are sufficient: `SUPABASE_URL`, `SUPABASE_DB_URL`, and one of `MASSIVE_API_KEY`, `MASSIVE_KEY`, `POLYGON_API_KEY`, `POLYGON_KEY`. Do not print them or create new credentials.
4. Perform a single invalid-token authentication probe against the deployed slug. Require HTTP 403 and independently unchanged provider-request counters. Add only this slug to the existing dispatcher allowlist, preserving its other checks and routes.
5. Record evidence only after observing it: `MASSIVE_ACTION_N03_ROLLBACK_VERIFIED` with `result: PASS`, `provider_accuracy_verified: false`, `live_provider_requested: false` and the returned `tested_function_sha256`; `MASSIVE_ACTION_N03_WORKER_DEPLOYED_VERIFIED` with `result: PASS`, `slug`, exact `worker_sha256`, `invalid_token_http_status: 403`, and `provider_requests_during_auth_probe: 0`. The registration checks these fields against the current functions and expected worker hash.
6. Run the registration. It seeds one ACQUIRE partition with `max_pages: 1`, registers ACQUIRE/NORMALIZE/DERIVE/VALIDATE and wakes the existing scheduler. It does not increase budget, request caps, storage caps or the mission deadline. If the first page has a cursor, the next attempt blocks at `MASSIVE_PAGE_CAP_REVIEW` without fetching it. If the first page is terminal, normalization and provisional validation may proceed.
7. Inspect the real page with the readback query. Review actual optional fields, query dates, result count, cursor integrity, duplicated IDs and explicit unavailable/quarantined fields. This is not independent source accuracy verification. Independently verify action mechanics for an appropriate risk-focused sample before asserting accuracy, and reconcile company/instrument identity and effective dates against the final universe.
8. After first-page review, a root-recorded bounded admission may increase the existing task's `payload.max_pages` and clear only its specific page-cap blocker. Preserve its input versions, archive key, cursor, checkpoint and request counters. Never replace the task or reset page numbering. The reviewed cap must fit the shared remaining 200-request allowance and storage headroom. Do not automatically make the theoretical 1,000-page code ceiling the operational cap.

## Important semantics

- Initial request: `/stocks/v1/splits`, `execution_date.gte=2026-08-01`, `execution_date.lte=2026-09-30`, `sort=execution_date.asc`, `limit=1000`. Continuations are accepted only on the same fixed origin/route and are canonicalized to a credential-free cursor. There is exactly one fetch per reserved attempt and no worker retries.
- The original response body bytes delivered by `fetch` are hashed before redaction. These bytes may already be HTTP-decompressed by the runtime; `wire_sha256` therefore names the original application response body, not TLS packets or compressed wire framing. The stored text differs only in the top-level `next_url` value. Its SHA and byte count are calculated independently. Result values, integer lexemes and whitespace are preserved.
- Successful release charges the exact retained bytes. Unsaved error responses charge zero retained bytes. An uncertain fetch or stream effect leaves the reservation unresolved, with the existing expiry reconciler responsible for conservative accounting. A known HTTP error blocks for controller review; it does not silently retry or consume another request.
- Each retained page and its continuation checkpoint commit atomically. A crash after fetch but before an acknowledged commit never causes a worker-level refetch. Root reconciles any unknown effect using task/attempt/page/lease evidence.
- Normalization verifies the document SHA, bytes and manifest again. Exact repeated records deduplicate; different versions of the same source ID remain visible and become `CONFLICTING`. Missing optional fields are retained as unavailable. Invalid/out-of-window values are quarantined, not silently discarded.
- Observation company/security IDs, exact effective timestamps, source publication and historical availability remain null until separately established. Returned provider event keys form a **provisional source enumeration**, not an independent all-company event denominator. The family binding requires the company universe and is not frozen. Generic validation must not certify this stage as complete or accurate.
- `historical_adjustment_factor` is retained only inside the raw event. It is a current cumulative provider factor and is not applied to a causal price series by this adapter.

No deployment, registration, live provider request or database mutation was performed by the source-adapter child.
