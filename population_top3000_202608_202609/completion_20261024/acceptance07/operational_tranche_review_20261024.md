# Independent operational tranche review

Reviewer: top3000-completion-20261024-acceptance07. Producer: runtime01. Controller alone applies.

Result: ACCEPT for controlled integration of `runtime01/operational_tranche_v3.sql`, SHA256 `815f342a97b5e7e4c4171dec63ab865702a28a26b9f8d23f34fdc15fdb419cfe`.

Independent read-only live evidence at 2026-10-24 03:47:36.448297Z confirms owner root-5db48b32d103, fence 3, stop 1, not stopped, cost authority admitted, original ceilings 6039 provider requests / 6055 HTTP invocations / 250000000 source bytes. Observed cumulative counters were 5986 / 6002 / 187522771 bytes and zero reserved bytes. Source-byte CHECK independently read as 0..250000000. Current bounds remain one active task, 4000000000 relation bytes and 8388608 per response. Admission expiry 2026-10-24 12:15:00Z and original mission deadline 2026-10-25 22:36:17.687194Z.

Code obtains the same mission advisory lock and row locks, checks exact owner/fence/stop/scope/admission and effective monetary authority, validates preimage ceilings, makes finite +2000 provider/HTTP and +250000000 byte allocations, and snapshots all listed cumulative counters, money fields and deadline/admission values for exact post-update equality. DDL and updates are transactional. Replay validates effective authority and final ceilings before returning; it issues no provider requests or deployment.

This review is static plus read-only preimage verification, not a claim that root applied the migration or tested replay. Root must retain its application receipt and verify resulting limits/counters.

Nonblocking policy discrepancy separately escalated: current resource_limits still contains legacy `no_paid_model_calls:true` and `no_capacity_change:true`. The user's changed authority authorizes these when necessary. Controller should supersede any operative prohibition, preserving the historical original and finite resource/provider bounds. The tranche's statement that it performed no capacity change is accurate. Direct-connector probes remain subject to the shared provider limiter and separate reconciled usage ledger; this patch does not implement that limiter.
