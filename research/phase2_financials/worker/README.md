# Phase 2 persistent financial worker

The dedicated Supabase Edge Function phase2-financial-worker-v1 is invoked by pg_cron job phase2-financial-worker-v1 in project oxzabweahkoimtevbbny. It has separate compute from all Render scanner services. Source credential values remain in existing Edge Function environment secrets; invocation token remains in Supabase Vault. Source contains only its SHA-256 authenticator.

Only acceptance jobs are currently enabled. Bulk work requires mode=BULK, acceptance_passed=true and bulk_enabled=true in research_eq10_fundamental_pit_20261020.financial_worker_control_v1. None of those flags is automatically changed by a worker. Existing indexing/extraction checkpoints are retained.

One document per invocation; one database connection; one active lease across the lane; 90-second lease; at most five attempts; exponential retry delay; HTTP 401/403 or missing provider credentials stop the lane. 429 and 5xx are retryable; unsupported document structures and identity conflicts are blocked with explicit reasons. Eight-MiB documents are handed off as requiring a larger-memory lane, never silently treated as absent.

Every write and completion is fenced by its lease token. The stop control is checked before every atomic persistence stage. A timeout after document commit resumes from the retained document hash without fetching it again. Extraction and completion commit atomically. The intentional acceptance failpoint terminates the first worker request after document persistence, without acknowledging the lease; the scheduler must expire and reclaim it.

To stop: set stopped=true in financial_worker_control_v1; optionally deactivate only cron job phase2-financial-worker-v1. To resume a recoverable blocked job, inspect evidence and restore it to RETRY only after the cause is fixed; do not reset attempts or existing checkpoints blindly. Re-enable stopped=false. Expired leases are recovered automatically; do not manually create a second controller.

Observe financial_worker_jobs_v1, financial_worker_events_v1, financial_worker_dispatch_v1 and cron.job_run_details. net._http_response is transient; final verification evidence is copied to financial_verification_v1. The checkpoint from earlier chat processing is financial_source_pages_v1: completed index pages must not be restarted; resume metadata collection from the latest retained next_params after the execution acceptance gate.

Financial classifications remain CONDITIONAL: original accession-bound document facts, conservative next-filing-day availability in America/New_York; original historical provider receipt and exact dissemination remain unproven. As-of reads default to certified-only and require explicit conditional opt-in. Weighted-average EPS denominators are distinct from shares outstanding; dimensional share classes require separate resolution before market-cap use.

This branch is isolated. Do not merge into or deploy the scanner branch as a side effect of this research worker.
