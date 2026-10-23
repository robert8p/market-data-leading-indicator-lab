# S02 independent retained-source and canonical reconciliation

## Scope

This read-only draft verifies all **135 retained pages / 134,931 source occurrences** in `massive-archive:S02:c1ed3ed6aa5bad110f122c2459ba27f6daa14a7ac484363d4915c0fc9ddb1f88`. The source window covers settlement dates July15–September30,2026; July records support the August/September mission. The requested population remains 3,000 companies, whose membership and report denominator are unresolved. A terminal provider page chain does not establish that company denominator.

The controller's earlier actual raw-only receipt `s02_all135_source_raw_independent_actual.json` reports zero hash/byte, count, cursor and date/shape mismatches at14:41:50.513806Z, source manifest `b1632ed886746c9c8266ee42bd8e31e6a37cddbc0262dafded6334f406cdea1a`. It is a preserved prior result, not a claim these new canonical queries were executed.

## Controller execution sequence

1. Wait for the admitted S02 NORMALIZE and DERIVE tasks to finish. Run `s02_independent_reconciliation_plan.sql`; retain its single result and require every `global_mismatches` value to be zero. The plan intentionally returns `PLAN_ONLY_REQUIRES_ALL135_PAGE_CHECKS`.
2. For each page1–135, insert `SET LOCAL top3000.s02_readback_page='<page>';` and `SET LOCAL top3000.s02_plan_sha256='<original plan_sha256>';` immediately after BEGIN in `s02_independent_source_page_reconciliation.sql`. Retain each actual receipt. Every page must return `PASS_PAGE_SOURCE_TRANSFORMATION_ONLY` with all mismatch counts zero.
3. Insert the original plan object into `top3000.s02_plan_before` and the array of all135 actual page receipts into `top3000.s02_page_receipts` in the global query. It recomputes the plan, requires an unchanged fingerprint, exact page membership and successful matching receipts, and returns only `PASS_SOURCE_TRANSFORMATION_ONLY` on success.

Each SQL statement has a **10-second timeout**,500ms lock timeout and16MiB work-memory limit. Each page query rehashes/parses one retained body of at most1,000 records. The metadata plan uses indexed source keys and row-version fingerprints, without loading all source bodies or canonical JSON. Each page performs bounded exact raw/staging, canonical and expectation comparisons. If a query times out, preserve the failure and diagnose its plan; do not silently raise the deadline or relabel an unexecuted page as passed. Separate transactions permit resumable receipts; a changed final plan invalidates this combined pass.

## Independent checks

- Exact retained UTF8 source SHA/length and full document evidence; source status,count,request/cursor identity and reserved provider-attempt linkage. Original wire bytes are not retained and cannot be independently rehashed.
- Every source occurrence, raw JSON, business key and content-version key, with missing/unexpected staging rows. Every canonical version uses the first page/row for an identical record; all occurrences contribute to its exact provenance hash. Distinct content versions for one business key remain conflicting.
- Set-based expected fields and states using core PostgreSQL date/numeric parsing. No production parser, normalizer, calendar, context or bundle helper is executed. Eight embedded literal cases cover zero/missing/string numeric data,fractional and negative short interest,invalid dates,provider999.99 values,and exact numbers beyond binary-float precision.
- A literal six-date controller-verified FINRA schedule extract is compared independently with output dates. Scheduled dates remain DATE values; they are never manufactured into publication instants. September30 settlement has scheduled October9 publication. `event_at`,observed publication,historical receipt,and available-at remain NULL;company/security/reporting-entity IDs remain unresolved.
- Exact values,all11 field states,lineage,record hash,expected-field list,expectation provenance,source references and provisional publication-ineligible partition. Global set comparisons reject missing or unexpected canonical/expected rows.
- Before/after fingerprints include source/staging/observation/expectation row `xmin` versions as well as declared hashes. Any committed update or replacement during the audit changes the fingerprint even if an application hash was left unchanged. This is a short-lived same-database concurrency check,not a durable identity or cross-database hash.

## Limits and authority

These queries test faithful transformation and retained provenance. They do not establish provider/exchange accuracy,all-company report coverage,source entitlement beyond the existing admission,or strict historical usability. Provider days-to-cover is preserved without recomputing or endorsing its rounding/censoring semantics. The verifier uses independently written set-based logic but shares PostgreSQL JSONB/date/numeric/SHA primitives with the producer; that shared canonicalization is disclosed. The source calendar derives from the controller's already verified official schedule extract; this local task made no new external calendar request.

Root alone executes and retains actual SQL/receipts and performs durable Git/database checkpointing. No helper functions,tables,publication routes,budget limits,leases or worker states are changed by this draft. Supabase/data-quality guidance informed the review; the skill's fixed historical-scope executable review helper was not run or relabeled as a live operational certificate. The current explicitly authorized August/September2026 scope and $250 cap remain authoritative. **Database execution: NOT_RUN by this child. Certification: NOT_ISSUED.**
