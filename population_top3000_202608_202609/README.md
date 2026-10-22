# Isolated population acquisition lane

Prepared for parent review; nothing here has been deployed. Scope is preliminary historical reference census and dated ticker overviews only. This does not certify the 3,000-company membership or complete any family.

Deployment order: review migration; apply to new private schema; deploy `population-top3000-source-v1` with `index.ts` and `deno.json`; read deployed source back; test disabled and stale-token rejection; enable one census job with interrupt test; invoke dispatcher; install mission-only 1-second cron after cost/resource admission; verify independently persisted page, interrupted lease recovery and next-page progression. Enablement and cron creation are deliberately absent from this draft.

The migration defaults the lane to disabled. It creates a new mission record and one census job. Parent must reconcile its existing mission record and owner ID before applying. Do not overwrite another owner. Parent should shorten the initial admission window or budget if its authoritative admission requires that. User prompt SHA-256 is `8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd`.

Limits: 2 active leases, at most 2 starts/second, 10,000 provider requests, 12,000 HTTP invocations including failed dispatches, 21,600 dispatcher calls, 6-hour initial admission, 250,000,000 source bytes retained or reserved, 8MiB per response, 90-second leases, 4 attempts/job, same failure twice causes a hold. Paid compute/storage/egress admission and billing reconciliation belong to parent; these request/byte bounds alone are not a dollar meter. Retry-After is preserved even beyond admission: no early retry is permitted.

Historical census is unfiltered by security type and preserves every response row. At terminal pagination, overview jobs are queued for `CS`, `ADRC`, `OS`, `NYRS`, `GDR`, `OTHER` and missing types as unresolved candidates, with all type codes separately reported by the verifier. Missing or unsupported types remain an explicit classification gap. Company-level identities, capitalisation semantics and multi-class/ADR reconciliation remain downstream work. No ranking or member selection is performed.

Source truth is raw UTF-8 JSON plus SHA-256, request parameters, provider request ID/HTTP date, ingestion time and code version. Publication/receipt timestamps are not inferred from current ingestion. All dated requests use 2026-08-03, and empty/inconsistent source replies are held.

`verify_census_v1()` is a separate read-only verifier over raw pages and normalized rows. It does not reuse producer flags to establish source counts, and does not certify historical company completeness or membership. Parent must run it in a separately observed tool call and preserve the result. `record_verification_v1()` persists its output separately.

Target later work: parent must implement/reconcile the remaining 24-family pipeline. Do not describe this lane as the full durable mission or claim background progress from deployment alone.
