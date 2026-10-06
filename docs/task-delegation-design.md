# Task delivery and execution

Task describes work delivered to an Actor. `Goal` sends a message to a Goal; `Agent` starts an internal Agent; `Delegate` selects an external executor. Both execution variants have one persistent Task owner at `/tasks/<sha256(source, requestId)>`. There is no separate business Run or Delegation owner.

## Admission and conversation

`TaskMessage` carries stable request identity, source, creation time, causal budget and optional frozen public Context evidence. Signals retain an exact occurrence before delivery; Goal tool identities derive from input and tool-call identity. Receivers validate source authority and reject changed identity reuse.

TaskActor commits instructions/evidence as `task.admission` in Pi, then saves business metadata, Pi references and the receipt before acknowledging. `task.input` retains follow-up instructions and their original receipts. Startup recovers Pi admissions whose Actor handoff was interrupted, using the same input-admission transition as live delivery so completed or failed work reactivates consistently. The first input owns the initial Pi reference and receipt; admission contains only source, reply Goal, executor, causal budget and optional publication action. Actor state does not duplicate message bodies or confirmation IDs already owned by ApprovalQueue.

A Task retains its identity across follow-ups and completed-work reactivation. Each instruction has its own request identity and delivery status. Exact retries return their original receipts even after completion. Rejected, cancelled and uncertain Tasks do not accept new work that would bypass reconciliation.

## Execution and follow-up

Internal execution uses a dedicated Pi conversation and AgentRunner. Execution is asynchronous to both the Task mailbox and the main Goal conversation. Busy instructions use native Pi steering when its runner is active; otherwise they remain pending for the next invocation. Completed Task follow-ups use the same retained working context.

External execution freezes executor policy and requires confirmation through ApprovalQueue. Only a matching persisted decision permits submission. The adapter owns busy follow-up behavior: Codex steers an active turn and starts a new turn in the same thread after completion; Pi retains context through its execution conversation. The current Doubao adapter explicitly rejects follow-up delivery because it has no implemented continuation API.

Task tracks pending, sending, accepted, completed, rejected and unknown inputs independently of provider rounds. Generation checks discard superseded observations. All external input paths share one submission operation; acceptance saves the returned handle and input status together. A provider round ending does not complete inputs still awaiting delivery.

`task.result` stores outcome text and its covered input references in Pi before the Actor commits terminal state, result reference and optional publication. Recovery can finish this handoff without executing again. Feedback goes to the reply Goal with a deterministic identity and retries missing acknowledgement within the owning Scope. Terminal replay requires no configured executor.

## Uncertainty and approvals

Sending markers precede external I/O. An interrupted submission or answer is uncertain and is never automatically sent again. Explicit `ResumeTask` carries request identity and expected revision. It observes the original execution, optionally uses read-only `lookupSubmission`, and resumes only an authoritative resumable failure. Resumption markers prevent another request ID from bypassing an unknown external outcome.

ApprovalQueue owns confirmation, permission and information requests. Resolve persists validated answers before acknowledgement; owners acknowledge delivery. Task persists answer sending/sent/unknown markers by request identity. Follow-up execution cannot discard an answer acknowledgement; execution generation only determines whether that response may advance the current round. Queue revocation tombstones prevent stale requests reopening. Progress and results appear in Goal conversations rather than a second notification store.

`InspectTask` returns instructions, follow-ups, outcomes, available native tool records, source references and decision requests. Provider handles and metadata remain private. External providers own their private transcripts; Aster retains its messages and returned results in Pi.

## Publication and lifetime

An explicit `PublishResult` action freezes destination, sending identity and result for separate human approval. Execution confirmation does not authorize publication. Task saves authorization and a sending marker before invoking ChannelWrites. Published, rejected and unknown delivery remain separate from Task completion; recovery does not resend uncertain operations.

Infrastructure Layers own external adapters and their release. SDK boundaries forward cancellation without equating local interruption with remote cancellation. Ending a Goal revokes unstarted Tasks; submitted work stays with its existing owner. Capture handoff and shutdown drain remain durable.

Tests use fake transports, real Actor mailboxes, Deferred and temporary Pi stores. They cover responsive follow-up, reactivation, admission/result handoff recovery, stale completions, approval authority, uncertain delivery and exact publication grants.
