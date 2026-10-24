# Independent runtime-policy review

Reviewer: top3000-completion-20261024-acceptance07. Producer: /root/runtime_01. Scope: static independent review, before live installation. No production writes performed by reviewer.

Reviewed patch SHA-256: `0352ddbc41cb5921ff9be9de7962a95ca1ab4a28c3162690775a528c19a1b388`.

Disposition: ACCEPT FOR CONTROLLER INTEGRATION AND LIVE VERIFICATION.

The four changed callers replace only monetary-cap admission expressions. Owner/fence/stop/scope, scheduler binding, resource, provider, retry, tick and finite-lifetime guards remain. The new helper rejects absent/malformed authority, stale owner/fence/stop/scope, invalid/negative/nonfinite increments and counters; explicit uncapped authority must bind the durable resumption event. Legacy capped mode remains supported. The aggregate budget constraint is relaxed only for explicit uncapped policy mode; the executable helper additionally checks the durable authority event. No cost/request/byte/tick counter is reset. Historical $250 amount remains as historical field, with explanatory column comment. No new PUBLIC execution grant is introduced by the helper.

The patch file and all four preimage hashes were independently verified locally. The supplied verification SQL covers current authority, no substitute monetary ceiling, stale identifiers, missing/malformed authority, negative/nonfinite values and both allowed/rejected legacy capped cases. Those are acceptance tests to execute after installation; static review is not a claim they have passed live.

Controller integration conditions: compare live definitions against the four pinned preimages; apply atomically; record current owner/stop authority; execute the supplied read-only verification matrix; verify stopped/stale-owner behavior at existing callers; refresh the recovery delegation's exact code manifest under the new owner so old manifest evidence cannot silently qualify changed runtime code. Existing resource/provider admissions remain independently bounded and may need measured revision.

Known nonblocking limitation: legacy resource_probe_claim_v1 is deliberately excluded because its prior attempt3 ceiling is exhausted. It must not be silently reset or used as evidence that the current uncapped policy failed. A separately admitted successor probe is needed if fresh capacity evidence is required.

Canonicalization correction: the producer's captured preimage files contain one additional final newline relative to pg_get_functiondef. The initial local-file hashes were valid file hashes, but not direct database-definition hashes; the controller's hash guard correctly aborted that first comparison. patch_manifest.json now records both local_preimage_file_sha256 and canonical preimage_sha256, removing exactly that extra newline. Patch SHA remains unchanged. Compare canonical hashes to live definitions; never relax the preimage equality guard.
