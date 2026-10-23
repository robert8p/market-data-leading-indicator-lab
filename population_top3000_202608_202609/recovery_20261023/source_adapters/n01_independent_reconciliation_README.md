# Independent N01 retained-source reconciliation

This read-only verification covers the completed 11-page, 10,282-record archive `massive-archive:N01:e9d1bdec926cc865b7ae140a6a475726898a9f35fa9b1cd5c41608a6909c2576`. It never calls the production parser or normalizer and does not make source requests. Each query uses a 10-second statement timeout and a 500ms lock timeout. The global plan reads metadata and hashes only; each page query reads and parses at most one retained source body containing at most 1,000 records.

## Run sequence

1. Run `n01_independent_reconciliation_plan.sql`. Preserve its single JSON result. Require every `global_mismatches` value to be zero. The result intentionally says `PLAN_ONLY_REQUIRES_ALL_PAGE_CHECKS`.
2. For each of the 11 listed pages, insert scoped `SET LOCAL top3000.n01_readback_archive` and `top3000.n01_readback_page` settings immediately after `BEGIN READ ONLY` in `n01_independent_source_page_reconciliation.sql`. Run independently and retain each actual result. Every result must say `PASS_PAGE_SOURCE_TRANSFORMATION_ONLY`, with zero mismatch counts. A timeout is an unresolved check; do not relabel it as PASS or raise the deadline without diagnosis.
3. Insert the original plan result as `SET LOCAL top3000.n01_plan_before` and the array of all 11 observed page receipts as `SET LOCAL top3000.n01_page_receipts` immediately after BEGIN in `n01_independent_reconciliation_global.sql`. This query recomputes the metadata plan. It requires an unchanged plan hash, exact page coverage, zero global/page mismatches, the same helper-definition hashes, and page raw-occurrence manifests matching the current global plan. The only successful final result is `PASS_SOURCE_TRANSFORMATION_ONLY`.

Escape SQL string literals by doubling single quotes, or use a correctly delimited SQL literal. Supply receipt JSON objects themselves, not an outer tool-response wrapper. All SET LOCAL settings affect only the read-only transaction; there are no table writes or new database functions.

## What is compared

- Exact retained UTF8 hashes/byte counts, document/provider/licence/evidence metadata, optional envelope fields, results count and page cursor chain.
- Every raw array occurrence and content-version identity against the occurrence-link table, including missing and unexpected links.
- First page/row canonical provenance for each distinct provider-id/content version, raw JSON values and every required/optional field state.
- RFC3339 instants reconstructed independently from components, integer microseconds and explicit UTC offsets. Eight embedded cases cover fractional timestamps on both scope boundaries, an offset representation, an invalid calendar date, absent timezone and a leap second. Exact provider strings remain unchanged in values/raw JSON.
- Canonical source metadata, source-published/event timestamp, New York effective date and current first-retrieval availability; historical receipt remains NULL.
- Every expected field, expected-source reference, applicability claim and record hash; company/security/reporting entity stay unresolved.
- All provider-id/version conflicts with exact sorted hashes, count, evidence and conflict-record hash.
- No missing or unexpected canonical/expected rows, provisional denominator and publication-ineligible partition. The final before/after plan detects changes to the compared metadata, row hashes, expectations and conflicts during the verification window.

## Limits

A successful result establishes exact transformation and internal provenance reconciliation of the retained provider archive. It does not prove that Massive's news archive includes every relevant article, that the publisher timestamp or article contents are independently accurate, or that all records belong to the requested 3,000 companies. Original wire bytes were not retained; their stored hash/length receipt is checked, while only the credential-redacted retained UTF8 body is independently rehashed. Current retrieval availability is never described as an observed historical receipt. No article bodies, additional research or sentiment generation are involved.

The verifier is a new independent query implementation. It has not been executed against the database by this child; root must retain the actual plan, each page result, final result and execution SQL hashes.
