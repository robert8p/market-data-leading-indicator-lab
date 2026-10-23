# Remaining scheduler tick budget — root read-only handoff

The diagnostic calculates the remaining successful retained-source work to its existing explicit quality holds. It does not estimate completion of the entire 24-family company population mission. No cap, cadence, task, provider or data mutation is requested.

## Execute and interpret

Run `remaining_tick_budget_readonly_v1.sql` as one read-only transaction. It contains one result SELECT, an eight-second statement timeout and a one-second lock timeout. It reads task/control/adapter metadata, the 42 D02 source headers and S02 page headers. It neither reads raw source bodies nor scans typed observation rows.

Use `conditional_total_ticks_upper` and `conditional_slack_after_work_and_idle` only when they are non-null and the listed live function hashes still correspond to the reviewed stage contracts. A positive slack means the currently modeled retained work fits under the unchanged 3,000-tick ceiling, conditional on successful execution without new admissions, new failures, repair work, timed waits or unexpected task changes. A null result is an instruction to inspect the anomaly/exposure fields, not permission to treat the omitted work as zero. A negative slack establishes that the modeled work does not fit as currently scheduled.

The JSON contains the actual control counter and remaining cap, aggregate and per-partition D02/S02 residual calls, sample assessment counts, other active task groups, timed recheck exposure, and nine current function-definition hashes. The read query itself and observer reads consume no scheduler ticks.

## Counted work

| Lane | Remaining successful adapter calls | Terminal behavior |
|---|---|---|
| D02 NORMALIZE | For each unfinished date, `max(1, ceil((result_count - last_source_row) / 1000))` | The final batch completes inline. An explicitly empty source still needs one call. |
| D02 DERIVE | For each unfinished date, `max(1, ceil((result_count - last_verified_row) / 1000))` | The final batch completes inline. A not-yet-created successor starts at zero. |
| D02 quality assessment | At most one call for each date not already assessed into its explicit hold | The reviewed v2 quality handler returns BLOCKED; no automatic downstream work. |
| S02 NORMALIZE | Terminal source page minus `last_normalized_page` | One retained page per call, final page completes inline. |
| S02 DERIVE | Terminal source page minus `last_derived_page` | One retained page per call, final page completes inline. |
| S02 quality assessment | At most VALIDATE plus one REPAIR call when both remain | Generic validation may already block before REPAIR. The reviewed repair handler explicitly holds. |
| Retained P01/P02 sample assessments | One call per remaining READY or LEASED NORMALIZE task of kind `RETAINED_ALPACA_AUGUST_SESSION` | Each call assesses its existing manifest and returns a durable identity/expectation hold. It creates no downstream task. |

The checkpoint, rather than just a source progress counter, governs the exact remaining batch count. A source write may already have happened while the final task receipt remains unreconciled. Already completed stages count as zero only with the associated header/handoff consistency checks. A missing expected successor or an invalid checkpoint makes the overall bound unavailable.

For the reviewed queue, each useful scheduler tick invokes at most one adapter. Advisory-lock BUSY exits and scheduled waits occur before the counter increments. Concurrency waits, missing-adapter/code checks and due idle checks can occur after it increments. The diagnostic therefore refuses an aggregate bound when there are active leases, future READY waits, unmodeled active tasks, exhausted active execution caps or eligible timed automatic rechecks. It reports their counts for controller inspection.

After useful work, a purely idle queue takes at most `max_idle_rechecks + 1` further counted checks before disarming: the function compares the pre-increment idle counter. If no useful work remains, the allowance is `max(0, max_idle_rechecks - idle_rechecks + 1)`. The query includes that allowance. Read-only verification outside the scheduler does not add to this allowance.

## Old snapshot only: why the current read matters

The retained snapshot at **2026-10-23 15:11:39.753403 UTC** had tick count 1,726 of 3,000, leaving 1,274. It showed two completed compact D02 dates and 40 remaining dates; 176,141 source rows still required normalization and 501,141 required derivation. Without per-date counts, the inequality `sum(ceil(r_i/1000)) <= ceil(sum(r_i)/1000) + 39` gives conservative bounds of 216 normalization calls and 541 derivation calls. Add 40 D02 quality assessments, at most 222 S02 calls (85 normalization + 135 derivation + 2 quality), and 208 retained-price assessments: **at most 1,227 adapter calls**, leaving **47 ticks before idle and other overhead**.

That calculation uses old aggregate evidence and is intentionally loose. It is not the current residual workload and does not prove the present cap adequate. The read-only query removes the aggregate-rounding uncertainty by inspecting each actual checkpoint and result count.

## Practical controller decision

If the actual result has non-null positive slack, preserve the existing limits and let the admitted retained work reach its explicit holds. Include the available slack in the controller record and rerun this same bounded read only if new admissions, failures or scheduling behavior alter the premise. If the bound is null, resolve its specific reported exposure before making a capacity conclusion. If it is negative, preserve all completed work and have the sole controller reconcile the remaining finite scope and resource admission; this handoff grants no authority to raise limits or skip source verification.

Company membership, company coverage, independent vendor accuracy and strict historical publication quality remain unresolved by this scheduling calculation. A completed source batch or a durable assessment hold is not a certified dataset.

## Verification status

Local inspection confirmed the one-adapter dispatch cardinality, batch/page sizes, inline terminal returns, explicit quality holds and idle-counter ordering in the saved definitions. Actual SQL compilation, runtime and current database counts must be supplied by root. This agent performed no external calls or mutations.
