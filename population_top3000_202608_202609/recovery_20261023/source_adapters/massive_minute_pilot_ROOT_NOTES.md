# Massive minute fallback: one AAPL New York day

This is a local implementation handoff to the sole controller. No provider calls, database mutations, deployment, or spend were performed by this agent. Local worker tests passed with fake database/network implementations. Actual PostgreSQL execution and live source entitlement remain root gates.

## Fixed source and scope

- Family P01; partition `massive-minute-AAPL-20260908-v1`; payload kind `MASSIVE_MINUTE_PILOT_V1`.
- Symbol AAPL; New York date 2026-09-08; interval `[2026-09-08T04:00:00Z, 2026-09-09T04:00:00Z)`.
- First URL: `https://api.massive.com/v2/aggs/ticker/AAPL/range/1/minute/1788840000000/1788926399999?adjusted=false&sort=asc&limit=50000`.
- Source contract is the root-opened official custom-bars documentation, `https://massive.com/docs/rest/stocks/aggregates/custom-bars`, source ref `turn53view0` in the root thread. The aggregate timestamp is the window start; qualifying trades determine whether a bar exists; all ET sessions are requested. There is no SIP-feed claim.
- Isolated new Edge slug `population-top3000-minute-v1`; no original price, daily, Alpaca, or actions worker is replaced.
- Initial admission is one page. A nonterminal response is retained with its cursor and then blocks for controller review. Supported lifetime bounds are 100 pages and 50,000 total raw occurrences. These bounds do not increase the shared mission request, byte, relation, or dollar limits.

## Root execution sequence

1. Keep cron596 paused, ownership fence2 / stop generation0 valid, and reconcile outstanding task/provider effects. Use the existing Massive entitlement and current resource admission.
2. Install `massive_minute_pilot_schema_DRAFT.sql`. It creates only private minute-lane objects. It does not register adapters, fetch data, enable cron, or change P02.
3. Execute `massive_minute_pilot_rollback_test_DRAFT.sql` on the actual database. Its transaction rolls back every change and makes no HTTP/Edge call. Record the returned PASS receipt as `MASSIVE_MINUTE_PILOT_ROLLBACK_VERIFIED` only if the real execution returns it. Retain failure receipts if the test fails; never substitute local inspection for actual execution.
4. Deploy `population-top3000-minute-v1_DRAFT.ts` to the new slug. It uses the existing DB URL/project binding and the already configured Massive key alias contract. Do not expose or change key values.
5. Root adds only this slug to the dispatch allowlist after review. Perform an invalid-token probe; require HTTP403 and zero provider requests. Record `MASSIVE_MINUTE_WORKER_DEPLOYED_VERIFIED` with the exact worker hash and probe evidence.
6. Run `massive_minute_pilot_registration_DRAFT.sql`. It requires the matching current function/worker test evidence, captures four current P01 adapter functions and hashes, and installs compatibility wrappers. It parks one AAPL task with `not_before='infinity'`. No global wake or budget increase is performed.
7. Root releases only that one task after checking current admission/headroom and resumes the controlled scheduler as appropriate. The exact provider reserve is `Massive`; the mixed P01 ACQUIRE adapter has provider NULL because each preserved handler reserves its own exact provider.
8. Run `massive_minute_pilot_first_readback.sql`. Review actual HTTP entitlement, source bytes/hash, result counts, checkpoint, timestamps, source flags, total storage, and runtime before expanding anything. If the response is nonterminal, review the stored first page before raising its page cap; no automatic truncation or fresh archive reset is allowed.

## Preservation and provenance

The four current P01 functions are captured by OID and definition hash, rather than redefined. This preserves the existing Alpaca bundle and retained-source checks. Only known legacy payload kinds are delegated. P02 adapter definitions, bindings, tasks, and source manifests are compared unchanged in the rollback fixture. No original family binding is replaced.

Raw UTF-8 JSON is retained exactly except for a canonical, credential-redacted continuation URL. The original wire hash/bytes and retained-body hash/bytes are separate. Numeric lexemes survive JavaScript parsing because the worker edits only the continuation-URL span; PostgreSQL parses exact JSON numerics. Successful accounting records retained bytes; wire bytes remain source evidence. One fenced reserve authorizes one fetch. Unknown fetch, partial HTTP200, or unknown commit effects never cause an automatic retry; the lease stays available for root reconciliation.

The compact raw table retains every occurrence by page and source ordinal. Normalization and independent raw reparsing run in 1,000-row batches. Each batch has an exact typed manifest; terminal reconciliation rechecks all current typed manifests, source counts, function hashes, and source chain metadata. DERIVE also requires the source manifest frozen by NORMALIZE, preventing a changed earlier page from silently changing the accepted chain.

## Quality and applicability

Missing and invalid fields have explicit masks. Explicit zero volume/count is distinct from missing/null. Prices are numeric and are not rounded through JavaScript doubles. Invalid OHLC, negative volume, fractional trade counts, absent/invalid timestamps, out-of-window timestamps and unaligned timestamps remain quarantined evidence. Duplicate and conflicting timestamp occurrences remain traceable rather than silently overwritten.

An omitted `results` field is accepted only with explicit numeric `resultsCount=0`; it yields zero observations, not a synthetic zero-volume bar. Missing bars are never filled from a dense clock. Publication time and historical receipt are unknown and remain NULL. The private provisional view contains no company/security assignment, no SIP certification, and no publication eligibility. VALIDATE blocks until company identity, expected trade/session populations, independent price/corporate-action accuracy and availability evidence are resolved. Source-chain completion is not company completeness.

## Verification included

The local worker suite exercises actual handler routing with fake DB/fetch: fixed origin/path/window, auth-before-fetch, no duplicate claim fetch, exact numeric lexemes, duplicate JSON keys at all depths, next-URL filtering, optional explicit-empty results, finite sizes/counts/runtime, typed `tx.json`, unknown commit handling and safe error responses. It passed before handoff and root independently reran it.

The actual SQL rollback fixture exercises two pages with 1,003+7 source occurrences, three normalize and three reparse batches, identical replay, typed-row tampering, a jointly revised source document/page manifest, a late modified already verified batch, retrieval metadata tampering, current relation-size caps, a hyphenated cursor, the one-page cap, empty source semantics, partial200 uncertain outcomes, exact retained-byte accounting, timestamp boundary cases, high precision and explicit-zero/missing distinctions. It calls an existing retained P01 NORMALIZE task both directly and through the wrapper and requires the same exact identity/expectation blocker. These are runtime tests to be executed by root, not claims that database testing already passed.

The manifest records exact file hashes. Root should execute these exact files, preserve the real receipts and make any targeted correction with updated evidence.
