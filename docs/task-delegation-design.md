# Task delivery and delegation

A Task sends work to an Actor. A Goal Task is a text message addressed to a Goal. A Delegate Task contains an executor name, prepared instructions/material, an explicit replyTo Goal and an optional publication action. Signals execute Tasks on a Context condition or a schedule; Goal tools use the same delivery path.

## Admission

`TaskMessage` carries requestId, source, task, createdAt, causal budget and optional frozen public Context evidence. Signal occurrences retain the complete envelope before delivery. Goal tool identities derive from the input and tool call. Retry never changes creation time, instructions or evidence.

Goal Tasks use `SubmitInput(TaskMessage)`. Delegate Tasks use `StartTask` and create `/runs/<sha256(source, requestId)>` under `/user/runs`. Targets validate ownership and retain an exact-input receipt before acknowledgement. Signal deliveries must match their committed occurrence. The Run also verifies its executor and reply Goal before admission.

The Run stores its admitted input, executor policy, status, approval IDs, optional resumptions, outcome and publication. The policy is frozen before confirmation and is combined with the admitted instructions and evidence for both confirmation and submission. There is no model-driven Task preparation or readiness stage.

## Execution

```text
awaiting-confirmation → ready → submitting → running / waiting_input
                    ↘ rejected               ↓
                                  completed / failed / cancelled / uncertain
```

Only a matching persisted ApprovalQueue decision can move an awaiting-confirmation Run forward. Sending a bare `ApprovalResolved` command cannot grant permission. Delegate owns external session/run handles and all provider metadata. Stable Run admission is separate from external submission.

Delegation persists submission intent before calling the executor and the returned handle before reporting it. A missing handle after interruption is uncertain, not permission to submit again. `lookupSubmission` is an optional read-only reconciliation capability. A found handle is saved; missing, failed or unsupported lookup leaves uncertainty visible.

Authoritative failure, cancellation and completion remain distinct from transport uncertainty. Terminal outcomes are committed before notifying Run and replayed after restart without requiring an executor. Run forwards feedback asynchronously to its replyTo Goal using a deterministic receipt identity. A missing Goal acknowledgement retries the same message in a scoped Effect. Durable terminal state supports replay after restart. Goal receipt deduplication prevents duplicate conversation inputs.

## Approvals and explicit resumption

ApprovalQueue owns pending human confirmation, permission and information requests. Resolve commits a validated answer before acknowledging the caller. Repeating the same resolved answer returns success; replacing it fails. The queue redelivers resolved answers until the owning Actor acknowledges them. Revocation tombstones prevent stale enqueue from reopening withdrawn requests. General progress and results appear in Goal conversations, not in a second notification system.

`ResumeRun` targets a failed or uncertain Run with requestId and expectedRevision. Run commits the exact input and receipt before handing it to Delegation. Delegation observes the original handle and resumes only a reported resumable failure. It saves a resume marker before external I/O. An interrupted or ambiguous resume remains unknown; a new request ID cannot bypass that marker. Pending confirmation and completed execution cannot be resumed.

`InspectDelegation` exposes status, result, requests and source references through the application API. Provider metadata and native messages remain private.

## Publication

A Delegate Task can request `PublishResult` with an exact Channel path and sending identity. Completion freezes the result and action into the Run's writeback record in the same commit. Publication requires a separate approval of destination, identity and content; Task confirmation does not authorize publication.

The Run verifies the committed approval, retains authorization and saves `sending` before calling `ChannelWrites` through `pipeToSelf`. Outcomes are `published`, `rejected` or `unknown`, separate from Task completion. Recovery never resends a `sending` or `unknown` operation. Adapters own credentials and verify the retained exact request and grant before submission.

## Resource ownership

ExternalAgents and ChannelWrites are Effect capabilities; infrastructure Layers acquire and release adapters. Native Promise boundaries forward cancellation and use tagged errors. Local interruption does not prove external cancellation. Ending a Goal cancels work that has not begun, while submitted execution remains under its Run/Delegation. Capture handoff and shutdown drain remain durable.

Tests use fake transports, real Actor mailboxes, Deferred and TestClock. They verify admission order, duplicate receipts, approval authority, Goal feedback, recovery, uncertain submission and publication without calling external services.
