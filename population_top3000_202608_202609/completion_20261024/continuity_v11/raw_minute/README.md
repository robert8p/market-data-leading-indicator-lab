# Manifest-driven raw minute acquisition core

## Status at 27 October 2026

Tested implementation candidate, **not deployed**. This source-only work does not install or activate the restricted price normalizer and does not certify prices, corporate actions, identities, historical receipts or an entire family.

The original `raw_minute_core.py` at commit `824a28e8b4a4f3de7427927d15c1fb1bedd62bca` has SHA-256 `6ac4e72daa9ce5892aa80faaea60a25e73572e384c65c55f06326acc0ff510c6` and Git blob `53194a47a68b9231bd0405795eb95a141d6dcc87`. Apply `raw_minute_core_v2.patch` from commit `d8d70f6a17b144e33cb38a52ee1275afe5e11833` to obtain the current tested core SHA-256 `70edf2b54de06a851f9bc5999ac97519c873abcbf02419fa78251973dd6c3ae0`.

Version 2 fixes two pre-deployment findings: unredacted exceptions from a failed failure-receipt callback, and an untyped extreme-Decimal parsing exception. Unknown callback-write outcomes remain unresolved, with no automatic retry or false lease-release claim.

**34 producer tests passed locally**, including all 6,000 private request manifests. No independent reviewer, network test or native runtime PASS is claimed by those tests. Full code, tests and the private manifest are preserved in the mission's private saved package; durable candidate event **132410** and hardening event **133138** identify exact artifacts. The current package is 323,043 bytes, SHA-256 `15089c18f5b12ef42d157a3b1fba7b68924e10ca93573259f5b8b6e0c68c3923`.

## Scope and preserved semantics

The worklist is 3,000 accepted anchor symbols x two months, August and September 2026: 6,000 bounded requests. Sorted symbol/month request keys match the current database reconciliation at 13:11:34 UTC, SHA-256 `c386ad2ed1458163c287116f7bf891195edc09f83be7d33f653b09bde4571de4`.

This is **not** the complete historical share-class/listing-lifecycle frame. Extra securities and ticker changes remain required discoveries. A provider response does not establish company identity or historical eligibility. The private cohort manifest must not be committed to public Git.

The core validates source scope and canonical hashes; preserves raw numeric lexemes; retains separate original/stored byte hashes; strips credential-bearing next URLs; rejects cross-origin/path changes, duplicate JSON keys, invalid control types and nonfinite JSON; records rather than conceals timestamp anomalies; and does not infer zero trades or complete coverage from an empty response. It permits one actual current reservation, one HTTP attempt and one commit attempt, preserving uncertainty after external or commit failures.

## Required native integration

Assignment **132411** covers genuine distinct review and completion of the persistent raw-only transport/adapter. It must enforce actual current source reservations, secure credentials, bounded streaming/deadlines, ownership/stop controls, replay equality, pagination, cumulative resource/rate accounting and private access.

The generic native ACQUIRE completion path automatically creates NORMALIZE. Do **not** connect this raw-only source route to the restricted normalizer or an incompatible handler. Require an explicitly reviewed source-only lifecycle, actual native positive/negative tests, separate measured source admission and real retained-page output before claiming production execution. A Python core is not automatically a Deno Edge deployment.

Use useful measured tranches and automatic permitted successors. The four-page ceiling is an operating bound, not permission to call a nonterminal request complete. Do not extrapolate nominal call counts into bytes, complete coverage or an ETA. All 24 data families remain required.
