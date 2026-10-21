# Phase 2 persistent financial worker — current state

As of 2026-10-21 04:35 Europe/London, verified external bulk execution is active.
Function phase2-financial-worker-v1, deployed version 7, code phase2-persistent-v2.1.2.
One existing Cron controller (job 595) runs every 30 seconds. One active lease; at most four sequential units per request, no more than one metadata page per request; queue target 64 is backpressure, not a population cap.
Bulk, feeder and acceptance gates are enabled. Stop remains false.

The prior completed index and pages 1–26 were reused. Page 27 resumed, was deliberately interrupted after raw-page persistence, and recovered with one download and two attempts. Its 1,000 source rows and hashes reconcile. New pages enqueue their successors atomically. The feeder replenishes document jobs; it skips completed extraction. When metadata reaches its terminal cursor it also creates lookup jobs for indexed accessions absent from metadata. Each unavailable lookup is classified individually.

Acceptance includes an independent 987-fact Apple annual numeric/context/unit/dimension comparison with zero discrepancies; annual accounting reconciliations; source-page integrity; lease fencing; stop and bulk gates; and completed independent batches on the final deployed source.
See execution_state.json and acceptance_feeder_20261021.json. Authoritative live state is in the private research schema, especially financial_worker_progress_v2, financial_population_status_v2 and financial_metric_missingness_v2.

Availability correction: financial_availability_review_v2 preserves the original metadata while computing a conservative bound no earlier than acceptance. Thirteen original bounds preceded acceptance; 734 records lacked acceptance. The former now use the later next-day bound; the latter are excluded from normalized metrics. No previously published metric depended on these records. financial_reconstruct_metrics_v1 uses the corrected view; downstream users must use financial_metrics_asof_v1 or the effective review, not raw availability_bound alone. All retained metrics remain CONDITIONAL.

Resource recovery: a 24 MiB whole-buffer attempt exceeded Edge memory (request 49, HTTP 546, observed Memory shutdown, 294,990,659 bytes). It was rolled back. Only the Southern Company large retry actually failed at that higher bound; two other large retries were held without attempting them. The stable worker limits each response to 8 MiB and retains explicit holds. A streaming large-document path is still engineering work; these are not provider-data absence or permanent universe exclusions. Ordinary ingestion resumed and remains active. No Render/scanner changes or paid-service creation occurred.

New source files:
- feeder_schema_v2.sql and feeder_functions_v2.sql
- availability_safeguard_v2.sql and derived_availability_v2.sql
- progress_missingness_v2.sql
- independent_numeric_check.py
- execution_state.json and acceptance_feeder_20261021.json

Safety/recovery: stop with financial_worker_control_v1.stopped=true. Keep completed rows and raw evidence. Expired request leases recover automatically; retryable failures have bounded backoff and five attempts. Non-retryable failures remain visible. Native JWT validation is disabled only because the worker verifies a Vault-held random bearer token by hash before database use. Do not expose tokens or provider keys.

Full Phase 2 is PARTIAL. Metadata collection and ordinary document reconstruction continue independently. Large-document streaming, original historical receipt/version evidence, amendments/restatements, complete historical security eligibility, growth/TTM and sequence-pattern factors remain unfinished. No Phase 3 work has begun.

## Earlier acceptance snapshot — retained history

The following text describes the earlier idle acceptance configuration and is superseded by the current state above.

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
