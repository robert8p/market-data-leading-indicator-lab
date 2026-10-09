# Expediting steps 2 and 3 — 9 October 2026

Project: oxzabweahkoimtevbbny. Run: market_data_remediation_20261008_v1.

## Measured performance decision

Seven idempotent probes ran from the existing Render worker, using independent transports for the parallel pair. Every result matched the retained feature output hash. Serial total: 17.710621 seconds. The concurrent pair genuinely overlapped and took 20.694714 seconds wall time (16.85% slower). Combined batch: 17.081002 seconds (3.56% improvement, too small on this measurement to justify changing the sealed manifest). Splitting the second batch took 10.586113 versus 10.377196 seconds. Earlier 32MB/128MB and JIT probes showed no reliable gain. Retain one feature execution lane, original batch boundaries and 32MB work_mem. Acquisition remains independent.

## Durable handoffs

The deployed maintenance thread alternates scoped source and feature validation on the existing service. Source completion atomically enqueues a job, including source revisions; completed backlog is reconciled separately. Leases, retained compressed/raw hashes, exact native/typed values, dated identities, current source versions and stop controls are checked before commit. Restarts replay committed jobs; failed/empty/identity-excluded rows retain explicit recoverable dispositions.

Qualified supplemental listed prices publish through public.market_data_listed_prices_as_of_remediated_v2. Warmup native reference dates publish through public.market_data_warmup_native_listings_as_of_v2 only after all pages, retained rows and pagination reconcile. The full compact feature route public.market_data_shared_features_canonical_v2 stays closed until all 11,289 manifest batches independently validate and full populations reconcile. Every release creates a remediation_change_v1 record. Default historical mode respects actual availability; archival proxies require explicit opt-in.

This automation does not establish full remediation completion. Unresolved identities and empty-response absence checks remain open. Warmup identity/price-derived rebuilds, supplemental-price feature generation and protected research consumer migration are separate work; the original/frozen generations remain intact. Crypto remains paused.

## Verification and reproduction

36 Python tests cover transport whitelisting, true concurrent probe invocation, hash/size corruption, bounded decompression, retry idempotence, lost leases, stops and idle release checks, plus existing acquisition/feature controls. Rollback-only SQL verified actual service-role access, stop/scope gates, feature reconciliation, no premature release, positive promotion and release branches, strict versus archival visibility, tampered raw sources, empty responses and known alias exclusions. Positive success branches use hypothetical resolved states inside a rolled-back transaction and do not constitute live data promotion.

Pinned raw fixtures: source batch IDs 837322, 854994, 880194, 880195. Fetch only these retained response artifacts through the approved project-scoped MCP; render the SQL templates with tests/render_retained_source_sql.py. Retained provider data and credentials are excluded from this repository.

SQL files preserve applied changes in dependency order. Apply only outstanding migrations, never replay CREATE TABLE or CREATE TRIGGER statements into the existing project. Runtime probe records are durable and are not rerun once measured.
