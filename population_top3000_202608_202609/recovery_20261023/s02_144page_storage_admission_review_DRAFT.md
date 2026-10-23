# S02 continuation and bounded logical-storage review

**Recommend S02 maximum 144 pages and a 4,000,000,000-byte mission relation ceiling. Keep the 250,000,000-byte raw cap and existing provider/HTTP ceilings unchanged.** This is a local recommendation for the sole controller; no admission or database change was performed by this reviewer.

## Evidence and request envelope

At the root's 14:28 readback, 128 pages and 128,000 source records were retained. Page 128 has a continuation cursor; its last record is PMTU for 30 September. Root independently verified hashes and date scope for pages 1, 24, 48, 64, 96 and 128. These samples support a bounded continuation; they do not establish a terminal archive or complete company population. Root's monotonic source-date review suggests approximately ten further pages, so a 144-page ceiling supplies 16 additional opportunities without presuming success.

The actual counters are 5,924 provider requests against 6,039 and 5,939 HTTP invocations against 6,055: 115 and 116 remain. Up to 16 additional S02 pages plus 41 remaining D02 source dates use 57, leaving 58 provider and 59 HTTP opportunities for a separately admitted minute pilot, identity reads or explicitly reconciled outcomes. **No request-ceiling increase is necessary.** Unknown-effect requests are not automatically retried.

## Why the 2GB relation ceiling is too tight

Current mission tables and indexes occupy 1,623,269,376 bytes, leaving 376,730,624 bytes at 2GB. S02's current source JSON is compact—about 118KB per 1,000 records—but NORMALIZE creates a provenance occurrence table with three indexes. DERIVE creates generic source observations, repeated JSON lineage/hashes and three indexes, plus expected observations and their primary index. A source-byte multiplier alone would substantially understate this expansion.

For an all-distinct 144,000-record archive, a conservative planning allowance of 8–12KiB per source record gives approximately 1.18–1.77GB additional table/index allocation. This places S02 alone at approximately 2.80–3.39GB of mission relations. This is a structural planning range, **not a measured PostgreSQL size or a guaranteed per-record bound**. Actual first committed NORMALIZE and DERIVE batch deltas will provide a better forecast without separate source requests or a capacity probe.

For D02, 42 days at the actual August 3 count of 12,402 rows yield 520,884 compact rows. At a 512-byte planning allowance per typed row including indexes, that is about 267MB. Sixteen S02 pages at 118,468 bytes plus 41 D02 files at 1,355,997 bytes add 57.5MB logical raw content. Together, the high S02 scenario, compact daily rows, those raw files and a 100MB operational allowance project to approximately 3.82GB. A 4GB mission guard therefore gives useful bounded room for these lanes. It does not authorize automatic expansion beyond 4GB or certify that all possible source variants will fit.

Logical raw content projects from 118,562,998 to176,054,363 bytes, below the unchanged 250MB cap. Preserve one active task, the 8MiB response cap and before/after-batch relation checks. A failed postbatch resource check can leave allocated physical pages even though data changes roll back; stop and review after the first such failure instead of retrying repeatedly.

## Budget and provisioned disk

Changing the SQL mission-size guard does not provision additional Supabase disk or change compute. Keep the monetary envelope **$76.03 encumbered  + $50 repair/egress reserve  + $123.97 uncertainty/unallocated  = $250**. The existing $10 recovery infrastructure allowance covers the already admitted request envelope; no extra money is allocated here. Full invoices remain unreconciled, and no zero-spend or unused-quota claim is made.

The 12:02 actual resource probe found 792,631,238,656 filesystem bytes and 175,632,646,144 bytes available, approximately 77.8% used. The additional 2GB of logical allowance is approximately 1.14% of that earlier available space; the inferred margin to the documented 90% expansion trigger was approximately 96.4GB. This is strong headroom evidence for a narrow logical increase, although it is an earlier shared-project observation and excludes later unrelated writes and WAL.

The already encumbered $50 storage contingency exceeds one full month of the documented maximum 200GB expansion step at either listed gp3 pricing ($25) or io2 pricing ($39). This is a conservative contingency, not evidence of the project's disk type, an incurred expansion, an invoice, or permission to purchase new capacity. Continuing storage charges after mission closure and expansion caused by other project activity require the existing resource/closure review; a mission SQL guard cannot cap account-wide future invoices.

Official references: [compute and disk pricing](https://supabase.com/docs/guides/platform/compute-and-disk), [disk usage and automatic growth](https://supabase.com/docs/guides/platform/database-size), [Edge invocation rounding](https://supabase.com/docs/guides/platform/manage-your-usage/edge-function-invocations), [egress pricing](https://supabase.com/docs/guides/platform/manage-your-usage/egress).

## Root admission conditions

Lock and verify the current owner, fence, scope, stop generation and counters before the change. Preserve all existing checkpoints and unknown-effect leases. Treat 144 as a hard review boundary if the archive is still nonterminal. Record first committed NORMALIZE/DERIVE batch sizes and timings as ordinary processing evidence, then continue while the unchanged guards permit it. Keep company membership and exact publication availability unresolved until their independent gates are met; a terminal page chain is not company completeness.
