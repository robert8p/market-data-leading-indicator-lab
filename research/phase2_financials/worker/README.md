# Phase 2 persistent financial worker

The dedicated Supabase Edge Function phase2-financial-worker-v1 is invoked by pg_cron job phase2-financial-worker-v1 in project oxzabweahkoimtevbbny. It has separate compute from all Render scanner services. Source credential values remain in existing Edge Function environment secrets; invocation token remains in Supabase Vault. Source contains only its SHA-256 authenticator.

Only acceptance jobs are currently enabled. Bulk work requires mode=BULK, acceptance_passed=true and bulk_enabled=true in research_eq10_fundamental_pit_20261020.financial_worker_control_v1. None of those flags is automatically changed by a worker. Existing indexing/extraction checkpoints are retained.

One document per invocation; one database connection; one active lease across the lane; 90-second lease; at most five attempts; exponential retry delay; HTTP 401/403 or missing provider credentials stop the lane. 429 and 5xx are retryable; unsupported document structures and identity conflicts are blocked with explicit reasons. Eight-MiB documents are handed off as requiring a larger-memory lane, never silently treated as absent.

Every write and completion is fenced by its lease token. The stop control is checked before every atomic persistence stage. A timeout after document commit resumes from the retained document hash without fetching it again. Extraction and completion commit atomically. The intentional acceptance failpoint terminates the first worker request after document persistence, without acknowledging the lease; the scheduler must expire and reclaim it.

To stop: set stopped=true in financial_worker_control_v1; optionally deactivate only cron job phase2-financial-worker-v1. To resume a recoverable blocked job, inspect evidence and restore it to RETRY only after the cause is fixed; do not reset attempts or existing checkpoints blindly. Re-enable stopped=false. Expired leases are recovered automatically; do not manually create a second controller.

Observe financial_worker_jobs_v1, financial_worker_events_v1, financial_worker_dispatch_v1 and cron.job_run_details. net._http_response is transient; final verification evidence is copied to financial_verification_v1. The checkpoint from earlier chat processing is financial_source_pages_v1: completed index pages must not be restarted; resume metadata collection from the latest retained next_params after the execution acceptance gate.

Financial classifications remain CONDITIONAL: original accession-bound document facts, conservative next-filing-day availability in America/New_York; original historical provider receipt and exact dissemination remain unproven. As-of reads default to certified-only and require explicit conditional opt-in. Weighted-average EPS denominators are distinct from shares outstanding; dimensional share classes require separate resolution before market-cap use.

This branch is isolated. Do not merge into or deploy the scanner branch as a side effect of this research worker.


## Acceptance results — 2026-10-21 UTC

Persistent execution acceptance PASS; whole Phase 2 reconstruction PARTIAL.
Supabase function phase2-financial-worker-v1, version 2, code phase2-persistent-v1.0.1.
Cron job 595 is active on */5 * * * *; mode ACCEPTANCE_ONLY; acceptance_passed=true; bulk_enabled=false.
Two real Massive original 10-Q documents completed. August WOOF deliberately returned HTTP 503 after the durable document checkpoint, left its 90-second lease unacknowledged, and recovered on the next eligible independent scheduled execution. Two attempts, one retrieval, different Edge isolates; extraction and metrics persisted. This was a request abort, not an operating-system kill.
December WOOF completed on the first scheduled attempt. Independent lxml/Decimal checks compared every parsed numeric fact (554 + 599), with zero discrepancies. Source hash, lineage, numerical reconciliation, cutoff boundary, research receipt cutoff, strict-mode exclusion, DST, idempotency, stop and bulk gates passed.
The old metadata JSON double encoding was corrected with original representations retained in financial_evidence_v1 worker_json_encoding_original. Deployed source retrieved and matched this branch.
No scanner or Render configuration changed. This is a shared database with bounded low-concurrency queries, not an isolated database server.
No bulk jobs are queued. Existing collection checkpoints must not be restarted.

## Operations

Automatic request crash recovery: wait for the 90-second lease to expire; next five-minute tick reclaims the same job and reuses any saved document. Retryable HTTP 429/5xx/timeouts receive backoff; at most five attempts. Deterministic errors go to BLOCKED. Credential failures stop the lane. No automatic guesses or alternate provider.
Stop: update research_eq10_fundamental_pit_20261020.financial_worker_control_v1 set stopped=true,stop_reason='OPERATOR_STOP',updated_at=clock_timestamp() where lane='financials';
Resume after resolving the cause: set stopped=false and clear stop_reason. Leave bulk_enabled=false until the next operation is explicitly implemented.
Disable scheduler as an additional emergency control: select cron.alter_job(job_id:=595,active:=false).
Do not clear jobs, facts, documents, source pages, or leases to resume. For a blocked job, record the remediation in financial_worker_events_v1 and requeue only that job after resolving its error; preserve saved evidence and completion history.
Deployment updates must specify import_map_path="deno.json", entrypoint_path="index.ts", and include both files. Native JWT verification is false because this service uses a random Vault-held bearer token whose SHA-256 is checked in the worker. Never log the bearer token or provider keys.
Reprovisioning requires a newly generated token in Vault phase2_financial_worker_token_v1 and the matching hash in the deployed source; the repository does not contain the token.

## Next operation

Continue Phase 2 source discovery in a durable page-job type from financial_source_pages_v1 page metadata_20230101_20260731_0026 and its next_params cursor. The current executor consumes document jobs; automatic metadata pagination/queue feeding is not implemented. The completed financial index (10 pages, 92,995 raw rows, 90,402 accessions) must be reused.
Reconcile 11 metadata accessions absent from the normalized index, and 64,410 indexed accessions without metadata. Queue bounded document jobs from verified originals, including annual and amendment acceptance tests, then permit controlled bulk by setting all three mode/acceptance/bulk gates appropriately. Full historical equity/security eligibility, amendment availability, annual transitions, TTM/growth and dimensional share-class resolution remain incomplete.
