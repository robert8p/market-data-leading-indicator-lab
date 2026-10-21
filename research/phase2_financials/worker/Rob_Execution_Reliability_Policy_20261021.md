# Rob — Execution Reliability Policy and Stall Analysis

**Prepared:** 21 October 2026, Europe/London  
**Purpose:** Explain recurring work stalls and preserve concrete operating rules for future authorised work.  
**Review scope:** Historical handovers and retrieved conversation records. This is not a fresh inspection of every live GitHub, Render or Supabase operation.  
**Memory status:** The available Personal Context capability retrieves context but exposes no supported memory-write action. This document preserves the policy; it does not establish that global ChatGPT memory was updated or that future conversations will load it automatically.

## 1. Assessment

The strongest recurring problem is a gap between an instruction to continue and a verified mechanism that actually continues. Several scanner and research handovers describe completed or interrupted conversational execution while the application, collection service or monitoring remained active. Those forms of activity did not mean that outstanding implementation was advancing.

There are also documented cases where a persistent worker existed and failed. The October 4 review identifies faulty control-budget and clock handling, repeated supervisor disablement with an unknown cause, and a controller whose scope ended at one development wave. The October 5 report records completed source reconstruction followed by a blocked preparation stage. The diagnosis therefore cannot be reduced to “there was never a worker.”

The recurring failure modes are:

1. Foreground implementation ended without a continuing executor.
2. Runtime or monitoring activity was mistaken for progress on the requested correction.
3. A real worker failed at a control, resource, deployment or stage-transition boundary.
4. Connector or platform controls prevented a necessary action.
5. Feasible engineering, unfinished collection and unavailable historical evidence were not kept clearly separate.
6. Instructions, checkpoints and successful pilots did not consistently translate into whole-mission execution.

Rob had already provided clear continuation and recovery instructions. A stronger version of “keep going” would repeat an existing preference. The useful change is to make execution mode, ownership, useful progress, failure handling and closure objectively checkable.

## 2. Evidence and causal findings

### A. Conversational execution ended while the scanner continued

The 17 October EQ17 Phase 1 handover states that the earlier corrective turn had ended and no continuing implementation, test or deployment operation was found within the systems it inspected. It separately records one durable scanner owner and distinguishes later evaluations and monitoring from corrective implementation.

The handover identifies the last completed corrective release and its verification window. It also says the read-only phase itself passed while the full scanner mission remained incomplete.

**Implication:** A healthy application process, current lease or successful monitoring run cannot demonstrate that engineering work is still proceeding. Each mission needs its own execution evidence.

**Confidence:** Strong historical evidence from a saved handover; not a claim about present live state. [S1]

### B. Permission and execution state were conflated

The 6 October EQ20 status records the state `AUTHORIZED_IDLE_NO_WORK_IN_FLIGHT`. It explicitly distinguishes an ACTIVE permission flag from an active background worker. It also records that user approval already existed, but a database constraint and later platform refusals prevented the requested recovery.

**Implication:** “Approved,” “active,” “deployed” and “running” answer different questions. Granting broader permission again does not necessarily repair a connection or resolve a platform refusal.

**Confidence:** Strong historical evidence. The detailed platform-refusal rationale was not available, and must remain unknown. [S2]

### C. Some persistent workers had real control and supervision defects

The 4 October critical-path review identifies an optional heartbeat that could exceed a parent control allowance and terminate a useful child. It also identifies a clock-accounting defect that caused acquisition deadlines to be missed. The supervisor became disabled repeatedly while the mission was unfinished; the underlying disablement cause was not established.

The same review records that W10 was a finite development wave and its deployed controller ended at W10 completion. Successful execution of that wave could not by itself fulfil the larger research objective.

**Implication:** Persistence is necessary for long jobs but insufficient. Control overhead must fit the stage budget, terminal results must be retained, supervision must itself be observable, and every authorised stage transition must have an executable owner.

**Confidence:** Strong historical evidence for the recorded defects and scope boundary. No inference that these exact defects remain present today. [S3]

### D. Useful output was achieved, then the next stage failed

The 5 October remediation report records 7,590 corrected source records, 3,024 issuer admissions and 907 immutable export parts. It then records preparation failures, `CHILD_EXIT_NO_RECEIPT`, and zero completed discovery fits.

A separate export problem was diagnosed as excessive memory use and repaired with file-backed handling. By contrast, the preparation process's exit −9 did not identify its cause: the report explicitly left the historical cause unknown.

**Implication:** Distinguish successful upstream work from the currently blocked stage. Preserve completed output and diagnose the exact downstream failure. Do not restart the source collection or label every killed process an out-of-memory event.

**Confidence:** Strong historical evidence; reported measurements are specific to those attempts. [S4]

### E. Recovery tests had narrower coverage than general autonomy

The S04 report records a real executor interruption and successful recovery by another process. It retained one logical effect, two checkpoints and one independently verified output.

It also expressly states that the conversation remained alive, and genuine chat loss, observer loss and VM loss were not tested in that stage. The tested workflow was subsequently disarmed.

**Implication:** A passed recovery test establishes only the failure mode, deployment and scope actually exercised. It does not establish that every subsequent mission has a continuously operating recovery mechanism.

**Confidence:** Strong historical evidence. This review did not inspect every later qualification stage, so it makes no claim that these cases were never subsequently tested. [S5]

### F. Bulk-operation failures and temporary execution interrupted collection

The 20 October short-interest handover records a database import rejection with protocol error −32600. The returned text said permission was missing, but provided no more specific rationale and did not identify automatic approval review as the source.

The handover records a reconciled checkpoint of 150 pages and 148,507 OTC reference rows. No collection process remained running at that recorded checkpoint. These counts are historical; later conversation summaries report subsequent progress.

**Implication:** Record the exact failed action and known committed outcome. Reconcile before replaying any uncertain write. Use smaller operations only where the same operation remains authorised and the evidence supports a size or reliability problem; reducing batch size is not permission to bypass a denial.

**Confidence:** Strong historical evidence for that checkpoint. No current OTC count is asserted. [S6]

### G. Evidence limits were mixed with unfinished, recoverable work

The frozen short-interest release contains 556,025 reports, classified as 247,864 conditional and 308,161 excluded, with zero certified and zero unprocessed reports within that frozen population.

The later handover discovered an omitted provider category: the reference capture used the stocks market filter, while OTC securities had a separate filter. The resulting OTC recovery was unfinished. Thus “zero unprocessed in the frozen release” did not establish completion of the broader recovery.

Separately, original historical value versions and provider receipt/availability evidence remained unavailable through inspected routes. Repeated transformations of current data cannot reconstruct those missing historical facts.

**Implication:** Keep four states distinct: completed processing of a defined population; incomplete acquisition or reconciliation; evidence routes genuinely exhausted; external historical evidence unavailable. Close evidence-limited work only after feasible scoped work has been handled or explicitly blocked.

**Confidence:** Strong historical evidence. Recoverable OTC omissions must not be relabelled as irreducible evidence absence. [S6]

### H. The anti-stall instructions already existed

The 13 October restart prompt required checkpointed bounded batches, reconciliation before write retries, one controller, truthful active-chat versus persistent-worker reporting, and a change of approach after two materially identical failures.

It also required recovery after ten relevant tool calls or five minutes of active execution without new durable stage evidence, evaluated at tool boundaries, with a longer bounded deadline justified in advance where needed. It said that a successful pilot was not the end of the authorised scope.

**Implication:** Repeating the instruction is insufficient. Future execution must visibly apply it through state, progress evidence, recovery decisions and truthful closure.

**Confidence:** Strong evidence that the instructions were present. It does not identify which internal assistant decision caused each later lapse. [S7]

## 3. Additional contributors and limits

Many related chats and phases create a risk of fragmented authority, stale handovers, conflicting revisions and duplicate implementation ownership. Retrieved 21 October conversation records also report discovery of a concurrent GitHub/Render change. This is a reason to verify ownership and revision before mutation, not proof that a duplicate live controller exists now.

Status requests may also displace implementation if treated as a replacement task. They should be answered briefly while preserving the active objective. Repeated status messages alone do not prove this happened in every case.

Large analysis scopes increase execution demands. They justify bounded persistent jobs and resource planning, but do not establish that model choice, the user's phone, or a general outage caused the recorded stalls.

Testing and acceptance can legitimately expose new defects. The correction is finite, relevant acceptance and reuse of valid evidence, not skipping required tests. Evidence must remain bound to the exact tested code, input and environment.

This review cannot certify the current state of all missions, determine the hidden cause of every terminated turn, or guarantee that future infrastructure failures will never occur.

## 4. Standing operating policy

Apply these rules to multi-step implementation, research, remediation and data-processing work. Keep simple, low-impact requests lightweight. Current explicit instructions, stop directives, task scope and access controls always prevail.

### 1. Preserve the active objective

Treat status questions, clarifications and context compression as steering of the current task unless Rob clearly stops or replaces it. Answer a status question briefly and resume feasible authorised work in the active turn.

Do not substitute diagnosis, a plan, a prompt or a handover for requested implementation. Continue through agreed verification and delivery. Do not ask again for permission that already covers the action.

### 2. Reconcile before resuming

Read the authoritative checkpoint and inspect relevant current operations before changing shared state. Identify the operation, scope, cancellation state, owner, last verified result, in-flight actions and exact next step.

Reuse completed indexing, extraction, corrections and evidence. Resolve uncertain write outcomes before retrying. Do not infer abandonment solely from an old heartbeat.

### 3. Maintain one owner for shared execution

Use one controlling owner for a mission and serialize changes to shared production targets. Parallelise independent investigation, preparation or isolated work when useful.

Use isolated branches, schemas or bounded lanes where needed. Reconcile expected revisions before committing or deploying. Do not replace a healthy owner or launch a competing controller.

### 4. Report execution mode accurately

Distinguish:

| Execution mode | Required meaning |
|---|---|
| Active in this turn | The assistant is currently carrying out the work; continuation after the turn is not established. |
| Verified external execution | An authorised external run exists, with its owner, operation identifier, scope and durable progress independently checked. |
| Waiting for an external event | A named event or evidence arrival is required; identify any real observer and next check. |
| Blocked | A specific dependency or control prevents the next action; independent feasible work should continue. |
| Stopped | A stop or cancellation is effective; outstanding dispatched effects may still require reconciliation. |
| Complete | The agreed scope and acceptance have been satisfied. |

If execution state cannot be established, say it is unknown and perform bounded reconciliation. A plan, prompt, lease heartbeat, healthy scanner or monitor is not evidence that the requested correction is progressing.

### 5. Establish durable execution when needed

For work intended to continue beyond a turn, use existing authorised infrastructure and the fastest reliable route within the applicable cost and access constraints.

Require bounded batches, durable checkpoints, idempotent effects, exclusive ownership with stale-writer protection, error classification, bounded retries, stop controls and retained progress evidence.

Prove one real input-to-persisted-output batch and the relevant interruption/recovery path before scaling or claiming independence. Verify the continuing run or scheduling mechanism, that the recovery supervisor is armed, and which failures the proof actually covers. An executor-restart test is not proof of observer or host recovery. A successful pilot does not complete the remaining authorised population.

Keep heavy reconstruction away from the live scanner's critical processing and memory resources unless isolation and capacity have been established.

### 6. Measure useful progress

Count unique verified results: completed and validated records, persisted batches, tested changes, deployed revisions or acceptance items closed.

Keep expected, discovered, collected and processed populations distinct. Keep acquisition, extraction, validation, persistence, publication and independent verification separate. Do not reset a stall clock with polling, logs, repeated tests of unchanged evidence or heartbeat activity.

For active execution, use five minutes or ten relevant tool calls without new durable stage evidence, whichever comes first, as a default diagnostic trigger at tool boundaries. Record a justified longer stage deadline before a legitimately long operation. This is a recovery trigger, not a blanket cancellation timer or a new ceiling on total authorised work.

### 7. Recover through evidence

After two materially identical failures, diagnose and materially change the approach rather than repeating it.

Classify transient transport, rate limits, resource limits, lock conflicts, code/input problems, expired or stale ownership, permissions, authentication, scope/stop, unavailable evidence and uncertain outcomes separately.

For an uncertain mutation, reconcile authoritative receipts or committed effects before replay. Follow retry timing where applicable. A permission rejection does not authorise another route around the same control.

Record attempts and unresolved effects across resumptions. Continue independent items when one item is blocked.

### 8. Make errors actionable

Retain the business-operation result as well as transport status. Capture bounded subprocess failure details and the failure stage.

Do not turn a generic timeout or exit code into a specific diagnosis without evidence. Verify the exact deployed revision and real consumer behaviour; a passed local test or successful deployment request alone does not prove recovery.

### 9. Separate completion, evidence and waiting

Define acceptance criteria early and maintain a finite list of remaining dependencies.

Distinguish engineering acceptance from historical certification, live-session acceptance and source availability. Record unavailable historical evidence and what exact external evidence could resolve it.

Avoid reopening exhausted routes without new information. Do not classify incomplete collection as evidence-unavailable. Preserve frozen releases and admit verified additions through separately versioned outputs.

A legitimate market-session wait or an explicitly completed phase boundary is not automatically a stall. Continue other authorised work and identify what will actually trigger the next step.

### 10. Preserve current constraints

Respect existing stops, holds, provider restrictions, cost limits and no-live-trading requirements. Do not import an old task's budget or permissions into a different task.

Use the currently authorised task's infrastructure and access routes. Broad approval does not override a platform denial. If a real block remains, identify the action, target, known reason, outcome and exact missing prerequisite; do not invent a denial reason.

This policy does not restart stopped missions.

### 11. End with a verifiable disposition

At an intentional handover or turn boundary, preserve:

- Completed scope and the last independently verified result.
- Exact outstanding work and its population or acceptance item.
- The real execution mode and any external operation/owner.
- Last useful progress time and proof reference.
- Known failures, uncertain actions and their reconciliation.
- Exact next action and any user-only prerequisite.

Do not claim that work, monitoring or recovery continues after the turn without verified external evidence. Where useful authorised work remains and is feasible, keep executing rather than ending voluntarily with another plan.

### 12. Keep preferences separate from operational state

Long-term memory should hold these behavioural preferences. GitHub and database records should hold volatile operation status, code revisions, counts, ownership, attempts and receipts.

Do not store passwords, credentials or speculative root causes in memory. Do not imply that writing a document automatically updates global memory or that a saved rule implements infrastructure.

## 5. Minimal checkpoint for substantial missions

Reuse an existing record where available; do not create competing ledgers.

| Field | Purpose |
|---|---|
| Operation ID, scope and acceptance | Identify exactly what must be completed. |
| Current authority and stop state | Avoid reviving stopped or out-of-scope work. |
| Owner and relevant lease/fence | Prevent competing writers. |
| Recovery supervisor state and tested failure modes | Establish whether recovery is armed and what it can recover from. |
| Input and code revision | Bind results to the correct source and implementation. |
| Stage and execution mode | Distinguish allowed, running, waiting and stopped work. |
| Stage-specific completed counts | Make durable progress visible without mixing units. |
| Last useful result and time | Detect lack of progress. |
| In-flight action and outcome certainty | Prevent unsafe replay after interruption. |
| Failure class and attempts | Support bounded, informed recovery. |
| Deployment and rollback evidence | Distinguish code preparation from production change. |
| Remaining blocker and exact next action | Enable another authorised executor to resume. |

The record is evidence and coordination state. It is not itself a worker.

## 6. Concise memory entry prepared for a memory-capable session

> Rob expects authorised implementation and analysis missions to continue through verified completion without repeated requests to proceed. Status questions and context compression do not replace the active objective. Reconcile authoritative checkpoints, ownership, cancellation and uncertain writes before resuming; preserve completed work and use one controller for shared execution. Distinguish active-turn work, verified external execution, waiting, blocked, stopped and complete. Never infer engineering progress from a running scanner, heartbeat, monitoring task or approval flag. Long work needs an authorised persistent mechanism with bounded idempotent batches, checkpoints, exclusive ownership, stop controls, classified retries and verified real-batch/recovery evidence. After two materially identical failures, diagnose and change approach; use five minutes/ten relevant calls without durable progress as a default recovery trigger at tool boundaries, allowing a justified longer deadline for genuinely long stages. Reconcile uncertain writes before replay and never bypass denials. Continue independent work when one item is blocked. Separate unfinished collection, engineering completion, live acceptance and unavailable historical evidence; preserve frozen releases and stop repeating exhausted evidence routes. Reuse valid verification and finish the authorised scope after pilots. Protect scanner resources and retain task-specific providers, spending limits, holds and stops. End with verified results, real execution state, checkpoint, exact remaining work and next action. Store preferences in memory and transient run facts in project records. Claim a memory update or background execution only when actually verified.

**Application limit:** This entry is prepared and preserved here. No global-memory write was available or performed in this review. The policy can be used in the current conversation and retrieved for future work, but automatic loading in another conversation has not been verified.

## 7. What this achieves and what remains

This review identifies the recurring mechanisms, preserves a specific operating policy, and states the limits of the evidence and memory capability.

Applying the policy can reduce premature endings, blind retries, repeated investigation, ambiguous status and accidental restart. Eliminating unattended execution gaps additionally requires each long mission to have a working, authorised executor and recovery path with current evidence. The memory policy cannot substitute for that implementation.

No claim is made that any currently running scanner, reconstruction worker, connector or supervisor was repaired by this historical review.

## Source register

These were retrieved and read during this review. They are historical saved records, not fresh live-system measurements.

- **[S1] EQ17_Phase_01_Handover.md**, 17 October 2026. Sections: Authority and execution state; Automation and controls; Operations and reused validation. File ID: file_00000000c61481f5b0dafed90f84482e.
- **[S2] EQ20_Phase2_Current_Status_20261006.md**, 6 October 2026. Sections: Current blocker; Actual execution state. File ID: file_00000000ca4081f590ab6f563312f05e.
- **[S3] ASTRA_EQ20_Critical_Path_Review_20261004.md**, 4 October 2026. Sections: Assessment; Immediate fault; Additional fault; Clock release verification. First 100 returned lines read. File ID: file_00000000f24c82109a1630a43b0dff90.
- **[S4] ASTRA_EQ20_Remediation_Report_20261005.md**, 5 October 2026. Sections: What has actually advanced; Deployed code; Current critical path. First 70 returned lines read. File ID: file_00000000bfa881f5a2b3c347e7e1e3c3.
- **[S5] ASTRA_S04_Report.md**, 12 October 2026. Sections: Assessment; Actual execution and evidence; What was implemented and exercised. First 100 returned lines read. File ID: file_00000000270c822f8ed0901759499dba.
- **[S6] Fundamental_Factor_Phase_01_Short_Interest_Handover_20261020.md**, version 2, updated 20 October 2026. Sections: Latest execution checkpoint; Remaining executable work; Prior Phase 1 completion review; Historical certification limitations. First 180 returned lines read. File ID: file_00000000624481f496fa3ad20e78eee7.
- **[S7] Equity_Remediation_Activity_1_Restart_Prompt_20261013.txt**, 13 October 2026. Sections: Reconcile; Prove one complete live repair cycle; Enforce no-progress recovery; Finish Activity 1 only. File ID: file_00000000db248243b7fa919a8e1e75a3.

Retrieved conversation summaries through 21 October provided supplementary context for recent scanner and financial-reconstruction concerns. They were not used to assert a fresh live execution state.

