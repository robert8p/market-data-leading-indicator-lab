# Foreground execution repairs — 26 October 2026

This is a safe aggregate receipt, not a final family acceptance. No provider originals, contact values, credentials or private case payloads are included.

## 1. Withdrawn exact-ranking work retired

Production event **84024**, `PRACTICAL_POLICY_OBSOLETE_RANKING_WORK_SUPERSEDED_V1`, applies the user's practical-cohort amendment **23086** and accepted membership **24407**.

- 341 old REF01 exact-cutoff VALIDATE tasks changed from BLOCKED to CANCELLED with an explicit superseded-requirement disposition.
- The exact unclaimed recovery group changed from READY to SUPERSEDED.
- No task was falsely marked COMPLETE, no source fact was modified, and no data-quality or family-acceptance flag was promoted.
- Zero downstream dependencies and zero open attempts were confirmed before the change.
- The original task/queue rows are preserved privately in the database receipt.
- A transaction rollback test passed before the production commit. The first test failed before mutations because it used an incorrect attempt-column name; that failed test is not claimed as a pass.
- Postcommit readback: 341 cancelled, zero still blocked under this exact cutoff blocker, zero completed_at values.
- Mixed identity/listing requirements and all remaining financial-data requirements were deliberately preserved.

## 2. Completed options page work reconciled without redundant replay

Production event **84051**, recorded **2026-10-26T09:07:01.051235Z**, is `CTX02_REGISTERED_CHAINS_PROCESSING_ACCOUNTING_RECONCILED_V1`.

Three registered reference chains contain:

| Pages | Raw source occurrences | Stored normalized rows | Quarantined rows |
|---:|---:|---:|---:|
| 1,996 | 1,995,714 | 1,995,352 | 362 |
| 1,991 | 1,990,163 | 1,989,650 | 513 |
| 2,017 | 2,016,581 | 2,016,490 | 91 |
| **6,004** | **6,002,458** | **6,001,492** | **966** |

Every page already had one completed NORMALIZE and one completed DERIVE task, matching output hashes/input versions and an existing blocked independent VALIDATE task. All first URLs and terminal contiguous cursor chains matched. Stored row counts were checked over all pages in five bounded batches, with zero count discrepancies. The 220 legacy V2 receipts lack the later page_valid_units field; missing fields were not invented or treated as asserted values.

The three redundant whole-chain NORMALIZE tasks, which failed before doing useful work on the old PL/pgSQL alias collision, were replaced by this versioned accounting reconciliation and set to CANCELLED. Their unclaimed recovery group was superseded. Event **84052** explicitly supersedes the unclaimed repair assignment **83437** for these three tasks.

This avoids re-running 6,004 NORMALIZE plus 6,004 DERIVE chunks that had already completed. It does **not** repair the old general terminal-parser function. New requests using that old terminal path still require a separately reviewed implementation if applicable.

All **966 quarantine rows** and all **6,004 independent population/lifecycle validation tasks** remain. This is page-chain and processing-accounting evidence only: not independent recomputation of every raw value, not economic source accuracy, not unique-contract counts, not daily OHLCV, not full-window lifecycle or point-in-time qualification, and not family acceptance.

A guarded rollback test passed, followed by the actual production commit. Controls, counters, attempts and old source evidence were preserved. Postcommit readback found exactly the three redundant parser tasks cancelled.

## 3. Real two-backend concurrency method proved

The SEC caller's earlier qualification attempt **84027** did not achieve overlapping sessions because connector calls ran serially. A bounded native pg_cron writer plus foreground observer produced actual simultaneous backend evidence:

- Registration **84064** at **09:16:56.665956Z**.
- Observer proof **84065** at **09:17:01.681610Z**.
- Writer rollback proof **84066** at **09:17:04.605626Z**.
- Writer PID **729654** and observer PID **729652** are distinct.
- READY-phase observation **09:16:58.584800–09:16:58.601387Z**: fixture invisible, actual pipeline_tick_v1 returned BUSY, same writer lock held before and after.
- LEASED-phase observation **09:17:01.621163–09:17:01.631152Z**: fixture invisible, actual pipeline_tick_v1 returned BUSY, same writer lock held before and after.
- The real native enqueue function was exercised in an entirely rolled-back fixture transaction. The fixture state transition was simulated, not a real admitted provider attempt.
- The temporary job self-unscheduled before testing. Follow-up verified zero test jobs, zero fixture rows and zero test barrier locks. Existing jobs 596 and 597 remained active on unchanged schedules.
- Zero provider calls, zero HTTP calls, no new source permission, no production caller integration, and no changes to the acquisition/pipeline controls.

### Exact qualification boundary

This proves the available two-session test mechanism, actual scheduler exclusion and uncommitted-row visibility behavior. It does **not** invoke or certify the proposed SEC-specific begin/finish functions. No independent reviewer approval is fabricated. The existing executor must apply genuinely independent review to these observations and the exact preserved PG21 candidate proof, and perform any candidate-specific residual race cases before authorizing deployment/source activity.

The actual native enqueue preimage was `c50b4821aa43720dc4a1ea563eca1934b8f9e70fb24cadc050ccc08e0943b3e3`; scheduler preimage was `c8df19b574c748d5494b0200b981cbd338864c6ea57e7e2240dce14aff5fc2d1`. The executed writer-command hash is `13b8c1c00310b132612d25c6bbceec41032927638b6c50521afa6559413d6a8d`.

## 4. Concurrent work actually completed by the existing executor

- Futures transformation-only numerical export: saved **84008**, completed **84009**, released **84010**, at **08:42:17Z**; 2,652 rows, not full futures certification.
- June wage revision reader: independently verified **84040**, completed **84041**, released **84042**, at **09:02:07Z**.
- SEC native caller: assignment **83900** was released **84029** after partial qualification; it was not installed and no SEC source acquisition is claimed.

## Remaining mission state

The practical 3,000-company membership remains fixed. Full-period instrument/lifecycle evidence, daily options data, minute coverage, financial sources and final family qualification remain required. These repairs do not imply that all 24 families are running or accepted. No new budget/access approval is required for the already-authorized scoped continuation.
