# Bounded request extension review

**Recommendation:** admit at most 200 further requests to the existing entitled sources and 200 further Edge invocations from the counters locked at the moment of admission. Keep the existing 250,000,000-byte raw-source cap and 2,000,000,000-byte mission-relation cap unchanged. This is a local recommendation for the sole controller, not an executed admission.

At the root-reported provider counter of 5,796, a ceiling of **5,996** gives exactly 200 further calls. Adding 200 to the old ceiling of 5,899 would instead leave 303 future calls. The HTTP ceiling should likewise be the actual locked invocation counter plus 200.

A conservative request allocation is 128 S02 pages, 41 remaining D02 source dates and 20 identity reads: 189 calls, leaving 11 for specifically reconciled failed/unknown outcomes. Existing unknown-effect leases remain held; there is no automatic retry authority. The S02 page cap does not guarantee a terminal page or company-population completeness.

## Storage remains bounded

Root reports approximately 103.6 MB of retained raw content and 1.6 GB of mission tables/indexes. The actual first S02 page is approximately 118 KB, so 128 similarly sized pages project to 15.1 MB. The retained August 3 D02 source is 1,355,997 bytes; 41 comparable new dates project to 55.6 MB. Together the two lanes project to approximately **174.3 MB total logical raw content**, within the existing 250 MB limit. These are size estimates, not hard upper bounds on future responses.

Keep the per-response 8 MiB guard, single active task, raw reservation accounting and post-normalization relation guard. The relation cap must stop work if typed rows, generic observations, indexes or dead-row allocation approach 2 GB. Root can assess a separate storage extension after measuring the compact D02 pilot and S02 normalization. No disk, compute, IOPS, service or subscription change is included here.

## The existing $10 infrastructure encumbrance is sufficient for this envelope

Supabase bills Edge invocations in whole one-million packages at **$2 per package**, rounding upward. Therefore the relevant marginal bound is **$2 per billing cycle**, not the misleading proportional estimate of $0.0004. Reserve **$4** in case the current admission crosses one billing-cycle boundary. This does not assume that the organization's included quota is unused. [Official invocation pricing](https://supabase.com/docs/guides/platform/manage-your-usage/edge-function-invocations).

Allow a conservative **20 GB of billable traffic**, including serialization/protocol copies under these fixed workers and excluding bulk exports. At the official uncached egress rate of **$0.09/GB**, that is **$1.80**. The combined operation-based allowance is **$5.80**, leaving **$4.20** inside the already encumbered $10 for this recovery infrastructure. [Official egress pricing](https://supabase.com/docs/guides/platform/manage-your-usage/egress).

The existing Massive endpoints must remain within the previously documented prepaid entitlement. A missing entitlement or provider payment response blocks the request; it never permits an upgrade or paid add-on. Existing fixed project compute is not a new per-query purchase. [Official compute and disk model](https://supabase.com/docs/guides/platform/compute-and-disk).

Keep monetary controls unchanged: **$76.03 encumbered + $50 repair/egress reserve + $123.97 uncertainty/unallocated = $250**. The $5.80 allowance is inside the existing $10 component, not an additional allocation. Keep `full_mission_invoice_reconciled=false`. Recorded zero actual/accrued fields do not prove a zero invoice.

The earlier actual disk probe left substantial provisioned headroom. This request extension does not enlarge the mission's already admitted storage caps. Nevertheless, mission relation size excludes shared WAL and unrelated project work; it is not a global-disk guarantee. Supabase documents automatic growth at 90% disk usage and warns that WAL contributes to usage. [Official database/disk explanation](https://supabase.com/docs/guides/platform/database-size).
