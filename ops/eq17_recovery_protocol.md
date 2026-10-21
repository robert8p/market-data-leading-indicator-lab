# EQ17 correction controller — recovery protocol

**Authoritative checkpoint:** [eq17_execution_state.json](./eq17_execution_state.json)  
**Validation command in this repository:** \`python scripts/eq17_checkpoint_verify.py\`  
**Scope:** Existing private EQ17 scanner; no live trading, new subscriptions, changed rule thresholds or relaxed evidence contracts.

This file is a durable *restart procedure*, not a claim that ChatGPT continues operating after a turn. GitHub CI validates repository evidence on future pushes; it does not automatically deploy the scanner, perform background corrections, or certify a live market session.

## Reconciliation on every new turn

1. Read the checkpoint and validate its stages A–H, states, recorded release and rollback branch.
2. Fetch the **current** \`eq17-private-scanner-20261015\` GitHub head and inspect changes since the checkpoint. Do not overwrite work from a concurrent controller.
3. Inspect Render service \`srv-d9jm320u01pc73fhincg\` in workspace \`tea-d562ma4hg0os73aejnfg\`: latest and in-flight deployments, failure events, health results and recent child-exit classifications. AutoDeploy is disabled, so a Git commit does not imply a deployment.
4. Read Supabase project \`oxzabweahkoimtevbbny\` (private schema \`live_eq17\`) to confirm the scanner owner lease, recent heartbeat, latest runtime release and unchanged \`live:paused\` setting.
5. Separate collection/heartbeat activity from an active *engineering implementation*. No running chat-level engineering controller may be inferred from source polls or an old \`corrective:controller\` record. If an operation is marked \`in_progress\`, check actual commit/deploy outcomes before retrying.
6. Resume the checkpoint's **first unverified executable** step. If it needs a future market session, mark it \`blocked\` with the precise date/session evidence required; do not manufacture acceptance.

## Immutable identities and rollback

- Verified original protected scanner release: \`7b4727c990213d44dfc0532f3f939c36706423a3b33362c8dc2f0b654bff6c26\`.
- Protected baseline branch: \`eq17-rollback-baseline-20261021\` pinned to \`d29a2d25153eff60bc72c216ab66b0aa69adfe17\`. This branch is a **rollback reference**, not a second deploy controller.
- Latest successful diagnostic-hardening deployment is recorded in JSON. Always recheck Render; do not assume its SHA remains current.
- If a new deployment fails, reconcile any partially successful action, restore the known-good GitHub code, manually deploy only after confirming the previous Render deployment, and verify the private HTTP endpoint, owner, source polling and paused entries. Retain all existing evidence.

## Verification gates

- **Packaging:** top-level private artifact checksum, \`manifest.json\`, \`research_groups.json\`, five group manifest references and changed source-file checksums must all agree. Broken or missing manifests fail before child startup.
- **Subprocess:** a crash produces only allowlisted reason codes; sanitized rows are written to \`live_eq17.operational_events\` as \`eq17_child_exit\` / \`ops\` with bounded, idempotent retry. Do not store raw stderr, secrets or database URLs. An audit probe marked \`eq17_child_exit_probe\` / \`test\` is synthetic, **not a real crash**.
- **Readiness:** confirm the private HTTP health path and both child processes, then verify the new runtime's matching database lease and recent heartbeat. Transient HTTP 503 responses during zero-downtime handover are expected until ownership is transferred; do not claim success until the new instance returns HTTP 200 and has the matching lease.
- **Regression:** the deployed image passed eight manifest-integrity and six subprocess tests. Additional checkpoint-validation tests run separately. Do not substitute syntax-only tests for the 14 actual regression cases.
- **Timing:** only genuine current-release regular-session publication records, measured from observed completed bar end through result publication, can certify the maximum **45 seconds**. The existing 10-run full-universe SIP capacity replay was 28.78–32.91 seconds (mean 30.97s); it is **not** live-session acceptance.
- **Acceptance:** do not activate entries, dismiss source/identity holds, change TP/SL assumptions or relabel replay records as actual live evidence.

## Read-only database checks

\`\`\`sql
SELECT now() AS verified_at,
       l.owner,
       to_timestamp(l.expires) AS lease_expires,
       l.expires > extract(epoch FROM now()) AS active_lease,
       s.payload::jsonb->>'owner' = l.owner AS owner_heartbeat_matches,
       to_timestamp((s.payload::jsonb->>'heartbeat')::numeric) AS heartbeat_at
FROM live_eq17.owner_lease l
JOIN live_eq17.state s ON s.key='live:owner'
WHERE l.name='scanner';

SELECT kind, scope, to_timestamp(max(received)) AS latest_evidence
FROM live_eq17.evidence
WHERE kind IN ('evaluation_summary','live_session_acceptance',
               'source_poll','latency_acceptance','closed_session_sip_check')
GROUP BY kind,scope;

SELECT id, to_timestamp(created) AS observed_at, scope, payload
FROM live_eq17.operational_events
WHERE kind='eq17_child_exit' ORDER BY created DESC LIMIT 10;
\`\`\`

These queries establish actual observations, not a command to repair anything. Do not expose raw operational payloads publicly.

## Next market-session dependency

The last fully verified deployment occurred after the 20 October 2026 US equity market close. The next scheduled regular US equity session is Wednesday **21 October 2026**, 09:30–16:00 New York time (14:30–21:00 London BST); verify the current exchange calendar and actual close before treating the session as complete. Require observed source cutoffs, publication receipts, full 90-rule evaluation, current-release identity and live acceptance for each applicable minute. Do not declare 45-second acceptance if any observed required minute fails or if an essential source is unavailable.

If the next session is unobserved, record \`PARTIAL — LIVE-SESSION EVIDENCE PENDING\`, identify the next exact check and leave the scanner's safety controls unchanged.

## Checkpoint mutation discipline

After every independently verified change, update the JSON operations, deploy and rollback evidence, last verified commit, test outcome, blockers and exact next action; commit the update to the scanner branch. Only one operation may be \`in_progress\`. A connector denial is not an engineering failure: record the action and rejection, reconcile whether it partially completed, then select a supported alternative. Do not retry unchanged rejected tool calls.
