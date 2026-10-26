# SEC qualification: actual two-backend method

## Actual result, not a proposal

Database events 84064, 84065 and 84066 record a successful native scheduler/advisory-lock/MVCC experiment on 26 October 2026. This method solved the prior inability to obtain overlapping SQL sessions through serialized connector calls.

The temporary job was 598, named `top3000-concurrency-probe-20261026-v1`. It removed itself in a committed transaction before the experiment, then used a separate transaction with a 10-second statement bound. It acquired the existing mission advisory lock using `pg_try_advisory_xact_lock`, without pausing or changing the existing jobs. The observer ran concurrently in the foreground in a transaction bounded to 15 seconds, with a 12-second loop bound.

The writer used actual `pipeline_enqueue_v1` to create a rollback-only F03/ACQUIRE fixture. It signalled phase 1 through a transaction-level two-key advisory barrier, held READY for three seconds, changed only the fixture's status to LEASED, signalled phase 2 through another barrier and held for three seconds. An intentional caught `ZX001` exception rolled back the fixture subtransaction. No genuine provider attempt was created. The outer writer checked that the fixture was absent and the acquisition/pipeline controls were unchanged, then committed only its factual test receipt.

The observer detected the barrier through `pg_locks`, required a different backend PID, verified that same PID held the global mission gate, checked that no fixture row was visible, invoked actual `pipeline_tick_v1`, required a BUSY result, and verified that the same writer barrier was still held after the call. Both READY and LEASED phases were observed.

| Phase | Writer PID | Observer PID | Observation interval UTC | Visible fixture rows | Real tick result |
|---|---:|---:|---|---:|---|
| READY, uncommitted | 729654 | 729652 | 09:16:58.584800–09:16:58.601387 | 0 | BUSY |
| LEASED, uncommitted | 729654 | 729652 | 09:17:01.621163–09:17:01.631152 | 0 | BUSY |

Writer rollback receipt at 09:17:04.605626 UTC confirms zero retained fixture rows, no native attempt, zero HTTP/provider calls and unchanged control rows. A separate follow-up read confirmed zero test jobs and barrier locks, and original jobs 596/597 still active with their previous schedules.

## Exact native preimages exercised

- `pipeline_tick_v1()`: `c8df19b574c748d5494b0200b981cbd338864c6ea57e7e2240dce14aff5fc2d1`
- `pipeline_enqueue_v1(text,text,text,jsonb,jsonb,uuid)`: `c50b4821aa43720dc4a1ea563eca1934b8f9e70fb24cadc050ccc08e0943b3e3`
- Actual writer-command SHA-256: `13b8c1c00310b132612d25c6bbceec41032927638b6c50521afa6559413d6a8d`

## Qualification boundary and continuation

The proposed SEC-specific begin/finish functions were NOT installed or invoked in this test. This result establishes the test mechanism and the exact native gate/MVCC behaviors above. It is not a substitute for genuine independent review of the candidate caller and its transaction boundaries, required candidate-specific negative cases, gateway/contact configuration, official Edge compilation, or resource/source admission.

Consume the already-preserved PG21 candidate evidence in private event 84027 rather than redoing every passed test. A distinct reviewer must determine the residual risk against the exact current candidate. Where candidate-specific contention remains necessary, use this proven method with a new bounded test identity and call the candidate inside the rollback-only writer transaction. Never claim that the previous native-only test invoked the candidate.

Do not repeat an unchanged serialized pair of connector calls and label it concurrency. Do not reuse the already-closed test job or backdate its evidence. Do not leave a repeating test job, pause production schedules, force an active lease or obtain provider data during zero-source qualification.
