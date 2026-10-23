# N01 bounded news archive — local draft for root review

## Review status

Prepared locally only. No remote SQL, deployment, provider request, new purchase or source-lane activation was performed by this agent. Existing N02/N03 workers and adapters were not edited.

Local worker tests passed:

`PASS_LOCAL_N01_AUTH_REPLAY_SCOPE_CURSOR_HASH_PRECISION_SIZE_CLASSIFIED_FAILURES_AND_JSON_DRIVER_BOUNDARY`

Current worker SHA-256:

`ba2457e6ecccec9223381b01778d41caf2d94ad75d287f1f36e6a06b4640aee3`

The database rollback test is a **draft for root execution**. A local PostgreSQL runtime was unavailable; no database-test PASS is claimed here. The registration file refuses activation without actual rollback evidence matching current function hashes and an actual deployed-worker authentication probe matching this worker hash.

## Files

- `massive_news_n01_schema_DRAFT.sql` — separate N01 page and occurrence manifests, explicit version conflicts, scope/claim/save/failure handlers, normalization, derivation and helper bundle hash.
- `population-top3000-news-v1_DRAFT.ts` — standalone proposed Edge slug `population-top3000-news-v1`, pinned `postgres@3.4.5`, body capability authentication through the fenced database claim before provider credentials or fetch.
- `massive_news_n01_local_test.mjs` — fully mocked DB/provider tests; no external traffic.
- `massive_news_n01_rollback_test_DRAFT.sql` — root-only synthetic two-page SQL test, fully rolled back.
- `massive_news_n01_registration_DRAFT.sql` — root-only one-page admission, dependent on genuine test/deploy evidence.
- `massive_news_n01_first_page_readback.sql` — read-only first-page envelope, date-window, byte/hash and task-state inspection without returning whole article metadata.

## Source contract

Root verified the official provider documentation at:

https://massive.com/docs/rest/stocks/news

Fixed first request:

`https://api.massive.com/v2/reference/news?published_utc.gte=2026-08-01T04%3A00%3A00Z&published_utc.lt=2026-10-01T04%3A00%3A00Z&order=asc&sort=published_utc&limit=1000`

This covers publication timestamps from 1 August 2026 midnight New York time inclusive to 1 October 2026 midnight New York time exclusive. Continuations are accepted only on the exact HTTPS provider origin and news path, with a retained canonical cursor. Fixed optional query parameters must match the original query. Credential parameters are removed from the retained top-level `next_url`; the original wire hash/byte count and the separately stored text hash/byte count remain distinct.

No article URLs are followed. Title, author, publisher, tickers, description, keywords and insights are retained only as provider API metadata. Missing optional metadata is explicit `UNAVAILABLE`. Any provider insights remain attributed source metadata; no sentiment model or new research is run.

The documentation allows optional envelope `status`, `count` and `results`. This initial adapter accepts an absent status, and if present requires `OK`. Count, if present, must be an integer matching the results length. It deliberately requires an explicit results array: an absent `results` field is blocked for review, never interpreted as an empty news census. Root should inspect the first actual envelope before extending the page cap.

## Controls and budget semantics

- One claimed provider request per fenced attempt; duplicate claims rejected by the shared provider lease.
- Initial operational cap **1 page**. Hard implementation ceiling **100 pages**. Neither represents permission to exceed the shared provider/HTTP/source/storage controls.
- Each response is at most **8,388,608 bytes**; fetch timeout at most **25 seconds** and total worker deadline **45 seconds**.
- Existing provider reserve/release, owner fence, stop generation, task token, attempt lease and registered function hash checks remain in force.
- Task input versions also bind the complete N01 helper bundle hash. Unreviewed helper changes invalidate existing tasks.
- The current mission **2,000,000,000-byte relation limit** is checked before and after every normalization batch, and after derivation. Missing/invalid cap fails closed. A failed post-insert check rolls back that batch; PostgreSQL physical allocation from an aborted transaction may persist, so root must review resource failures before any renewed admission.
- Current retained-source, HTTP, provider-count, deadline and budget limits are unchanged. The worker passes incremental provider charge `0` only inside root's existing purchased-source admission and infrastructure encumbrance. It makes no claim that invoices are zero or reconciled.
- Fetch/stream/receipt outcomes with unknown effects retain their reservation for controller reconciliation. Definitive HTTP errors are blocked without automatic retry and release zero retained bytes.
- No raw provider credential, full request URL containing a key, SQL error text or environment value is returned by the Edge handler.

## Data quality and time semantics

Raw response numeric lexemes are never reserialized through JavaScript numbers. JSON parsing is used for validation only; the retained text changes solely at a top-level `next_url` value. Duplicate JSON keys at every depth are rejected, because converting them to JSONB would otherwise silently choose a value. UTF-8 BOM input is rejected rather than silently removed.

Each source page is immutable by archive key/page, URI, content and wire hashes, byte counts, record count and cursor. Normalization rechecks the retained content, source provider, URI, current retrieval time, wire metadata and source hash.

Each page/row occurrence is retained separately. Identical provider-ID/content versions normalize once, with deterministic first-occurrence provenance. The full list of occurrences remains in `massive_news_record_link_v1`. Different contents under the same provider ID remain separate versions and are recorded explicitly in `massive_news_version_conflict_v1`; no arbitrary canonical article is selected.

Full replay checks compare observation values, field states, lineage, hash, source IDs, timestamps, identity nulls and expected-row evidence against the source-derived expected values. Merely recomputing a hash over a tampered row cannot make it pass.

`published_utc` is retained exactly as provider text and parsed only with an explicit RFC3339 timezone. Invalid/missing timestamps and records outside the requested publication window remain explicit quarantined/unavailable observations. Parsed PostgreSQL timestamps have microsecond precision: excess fractional digits are truncated before casting so a timestamp immediately before a window boundary cannot round across it. Exact provider fractional digits remain in the raw field and `values_json`. Leap-second values are preserved but quarantined rather than normalized silently.

The provider publication timestamp populates `source_published_at` and the news publication event time, with lineage explicitly saying that independent publisher accuracy was not checked. `available_at` is conservatively the current API retrieval time. `historical_received_at` remains null. No October retrieval is claimed to establish August/September historical receipt.

Company, security and reporting-entity mappings remain null. Generic validation must retain a provisional, publication-ineligible partition with an unresolved company/news denominator. A complete provider page chain does not establish all news for the final 3,000 companies.

## Suggested root sequence

1. Review and install the isolated SQL draft while retaining existing completed mission work.
2. Run the root-only rollback test when no task/provider lease is active. The fixture uses two pages, nine occurrences and eight distinct content versions. It tests exact numbers, provider ID conflicts, full replay tamper checks, explicit time bounds including submicrosecond timestamps on both sides of the requested window, resource guard behavior, dispatcher progression and unknown-effect retention. The forged-value fixture updates the forged value and its matching hash atomically so the immediate row checksum constraint remains satisfied and the independent replay guard is actually exercised. Preserve the actual returned function-hash evidence.
3. Add only `population-top3000-news-v1` to the reviewed dispatch slug allowlist, deploy the exact worker, and verify an invalid attempt/token causes no provider request.
4. Record only actually observed evidence in the events required by the registration draft; then run registration. It admits one page, does not extend any budget/deadline, and does not certify coverage.
5. Inspect the first actual page with the readback query. Confirm optional envelope semantics, source date bounds, pagination, retained hashes and the actual stored record shape.
6. If the page has a continuation, explicitly admit a bounded increase of the existing task's operational page cap without changing archive input versions or discarding the retained page. The current hard ceiling is 100 pages; preserve at least the FINRA/action allocation within the shared 200-request acquisition admission. Do not manufacture an archive terminal state when a cap is reached.
7. Once a real terminal page exists, normalize one page per task attempt, derive version/duplicate receipts, and run generic validation. Treat all-source coverage and top-company completeness as separate unresolved quality questions.
