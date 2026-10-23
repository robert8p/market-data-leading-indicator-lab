# Independent D02 verification for the 42 frozen source dates

This package is read-only and local-only. It makes no provider call, installs no database objects, and changes no source, typed row, task, quality state, or control. Root executes the SQL. Actual SQL validation is a separate gate from the local synthetic receipt tests.

## What it proves

For each frozen session date, the verifier reads the exact retained unadjusted, non-OTC Massive daily aggregate response, independently projects its source occurrences, and compares them with the compact typed primary-key range. It checks every represented value, date, missing/invalid/structural mask, canonical source-row hash, source ordinal, missing row and extra row. Source bytes, source SHA, provider, fixed date URL, wire metadata, retrieval time, license policy and acquisition/task linkage are checked directly. No production parser, number decoder, calendar helper or validation routine is invoked.

The 42-date calendar is independently read from the frozen catalog, with its exact date-list SHA, record/receipt hashes, qualifications and timestamp ordering checked. Source metadata, proof metadata and their PostgreSQL row versions are fingerprinted before and after the run. Each date also has a second bounded typed-value and row-version postcheck, with a fresh exact source body hash. A changed value, changed row version, changed source/proof manifest, incomplete date, missing receipt or wrong date blocks the final report.

The resulting claim is **independent implementation reconciliation of the retained source**, covering exactly 42 source dates. It does not establish independent provider price accuracy, regular-session close semantics, historical public availability, a frozen top-3,000 company universe, or company-day completeness. All such claims remain false or unknown in the output.

## Files

- `d02_independent_verify_one_date_TEMPLATE.sql`: full direct raw-to-typed comparison for one fixed date, one source response, at most 50,000 occurrences and 8MiB raw body.
- `d02_independent_postcheck_one_date_TEMPLATE.sql`: one-date current typed manifest and row-version recheck, plus fresh raw-source SHA/bytes; no raw JSON parsing.
- `d02_independent_manifest_snapshot.sql`: inexpensive 42-date calendar/source/proof manifest; no typed-bar scan. It includes active/incomplete data-stage counts so the final run cannot certify an in-progress queue.
- `d02_independent_queries/`: 42 rendered verify queries and 42 rendered postchecks, with no placeholders.
- `d02_independent_query_plan.json`: exact dates, paths and file hashes for the 84 per-date statements.
- `d02_independent_collect.py`: local receipt aggregation, with no database/network capability.
- `d02_independent_collect_test.py`: synthetic receipt-gate regression, including stale data and missing-date rejection.

Every SQL file uses a read-only transaction, a 10-second statement timeout, a 1-second lock timeout and UTC output. No large raw body is returned to chat; receipts contain hashes, counts, flags and at most 10 row mismatch examples. Raw JSON is parsed once per date and never rehashed inside a per-record loop.

## Root execution sequence

1. First execute the rendered `verify_20260803.sql` against the actual database and inspect the result and duration. The previous root first-wave receipt contains 12,402 Aug3 and 12,456 Aug4 occurrences, with no typed differences. It also records three explicit zero-volume Aug4 records; these must remain distinct from missing volume or optional missing fields. No need to wait for all dates merely to check that the query compiles and performs safely.
2. For the final complete-source run, wait until all 42 daily source dates have finished ACQUIRE, NORMALIZE and DERIVE. Other mission families may continue. Do not mutate D02 sources or typed rows during the readback. The manifest requires zero incomplete D02 data-stage tasks and all 42 producer-completed source dates; VALIDATE may remain blocked for legitimate company/accuracy requirements.
3. Run `d02_independent_manifest_snapshot.sql`; save its single JSON result as `manifest_before.json` in a root-owned receipt directory. Save the exact tool query receipt separately as usual.
4. Execute all 42 rendered `verify_YYYYMMDD.sql` queries in bounded batches. Serial or at most two read queries at once avoids burdening the shared database. Save their single JSON results as `verify_YYYYMMDD.json`.
5. Execute all 42 rendered `postcheck_YYYYMMDD.sql` queries and save results as `postcheck_YYYYMMDD.json`. This second pass rechecks current source hashes, typed manifests and row versions after the first comparisons. It does not call a provider or repeat source acquisition.
6. Run the same global manifest query again and save `manifest_after.json`.
7. Run the local aggregator:

   ```bash
   python /workspace/scratch/4b1314bcb5c2/work/top3000_recovery_20261023/source_adapters/d02_independent_collect.py /absolute/path/to/root_receipts
   ```

   Exit0 and `PASS_SOURCE_ARCHIVE_RECONCILIATION_ONLY` require every gate to pass. Exit2 preserves a report of incomplete or changed evidence; it is not a reason to change data or relax checks automatically.

The collector accepts the direct JSON result, a single-element PostgreSQL row array containing the named result, or a simple JSON `result` wrapper. It deliberately rejects unparsed narrative/tool error text. It checks timestamps in the order global-before ≤ date-verify ≤ date-postcheck ≤ global-after, all 42 exact dates, source/typed counts, stable row versions and stable metadata/proof fingerprints.

## Exact semantics

PostgreSQL numeric values are compared directly. The document SHA covers exact stored UTF-8 JSON bytes; each row's canonical hash covers that JSONB record. The typed manifest codec is SHA256 of newline-separated `source_row:SHA256(JSONB array of all mapped typed fields)` in source order. Its field order is source row, ticker, provider milliseconds, New York date, open, high, low, close, volume, trade count, VWAP, missing mask, invalid mask, structure mask and canonical raw-row SHA. This codec is separate from the production batch manifest.

The independent date calculation uses epoch whole seconds plus milliseconds, rather than invoking the producer's date conversion helper. Missing/invalid masks are reconstructed from raw field presence and declared numeric/domain rules. A source zero is retained as zero; null/absent values remain absent. An empty response has zero occurrences, not a fabricated zero-volume bar. Duplicate source occurrences remain separate by ordinal. A fully reconciled source file with missing, invalid or structurally flagged source records remains explicitly flagged and does not gain provider-accuracy certification.

## Local verification status

The collector regression passed one complete synthetic 42-date case and eleven rejection cases: missing postcheck, value mismatch, changed typed hash, changed row version, changed global manifest, incomplete data stage, false company claim, missing source date, wrong receipt order, changed raw bytes and duplicate date manifest. The positive case includes three explicit zero-volume observations with optional missing fields and an empty final date. These are tests of receipt aggregation, not source data verification.

Root's actual per-date SQL result/performance and completed 42-date receipts remain required. All files are drafts for controller review; no certification event or source mutation is generated by this package.
