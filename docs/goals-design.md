# Goals

A Goal coordinates three parallel capabilities:

```text
Goal
├── Pi conversation — reasoning, user input and feedback
├── Tasks           — asynchronous external work
└── Signals         — conditions and timers

Context change → System One Gate → Goal Agent Gate → Pi conversation
User input ─────────────────────────────────────────→ Pi conversation
Task feedback / Signal occurrence ─────────────────→ Pi conversation
```

The conversation can request a Task or configure a Signal through tools. Their owners accept and persist those commands. Returning an assistant response does not stop a Task or a Signal. Goal End interrupts its conversation, revokes Task executions that have not started, and deactivates its Signals. Already-submitted external work remains owned by the existing Run and Delegation; interruption does not prove cancellation.

## Conversation

Each Goal has one durable Pi session/conversation, identified by its slug. Pi owns the transcript, model/tool rounds, tool-call recovery, queued submissions and native compaction. The existing agent adapter uses a Pi Document for exchange identities and committed result references. Goal does not recreate these mechanisms as an evaluation engine.

`goals/conversation.ts` supplies the stable assistant policy, current Context/memory reads, `update_goal`, `start_task` and `set_signal`. Assistant text ends a conversation turn naturally. There is no `finish_turn`, plan result, next-step union, result-application phase or automatic continuation protocol.

`update_goal` asks the Goal mailbox to change business progress. The mailbox verifies the invocation generation and completion evidence. Reads use public Context views; private credentials, execution handles and native tool transcripts remain excluded. Task and Signal commands go to their peer owners and return durable receipts. Unknown mutation outcomes stop further model work through the existing Pi tool fence.

## Input and recovery

Goal creation commits a deterministic initial input. Runtime activation is an in-memory gate; restarting does not manufacture another initial pursuit.

`SubmitInput` validates producer authority and commits the input and exact request receipt before acknowledging. One delivery at a time enters Pi with `requestId = inputId`; later inputs remain pending. Input status records only the cross-store handoff: pending, running, completed, failed, unknown or ignored. Context changes additionally retain the Agent Gate decision. There is no frozen Context catalogue or second evaluation journal.

The Goal mailbox is the only writer of Goal state. Asynchronous work returns through `pipeToSelf`, guarded by a generation. On restart, unfinished delivery reuses the same Pi identity and reconciles the existing exchange. Known failures can receive an explicit `RetryTurn`, which creates one successor input. Unknown outcomes block later delivery until reconciled; they never authorize a replacement external submission.

The retained business state is:

- Goal identity, description, optional completion criteria, status and progress.
- Accepted inputs with their delivery status, response and producer causality.
- Exact request receipts for idempotent producer retries.
- Business notification outbox and the public-history count.

Task execution state lives in `/runs/...`; Signal state lives in `/signals/...`; conversation execution state lives in Pi. Goal stores none of their duplicated lifecycle state. Business history is a public input feed, not Pi's transcript. The Timeline reads accepted inputs and conversation responses directly.

## Gates

The Context reaction owner first runs the existing System One relevance screen. Only admitted evidence reaches the Goal. Before entering the persistent conversation, `goals/gate.ts` runs a separate read-only Goal Agent Gate. An unrelated change is recorded as ignored and never sent to the conversation. Gate failure is visible and may be explicitly retried.

User input, Task feedback and already-owned Signal occurrences do not pass through generic Context screening. Feedback carries its original causal budget; starting a new conversation turn does not replenish it. Exhausted automatic feedback remains visible without invoking Pi.

## Tasks and Signals

Goals use the same `StartTask` command and `TaskDeliveryInput` as Personal. The shared Run root persists an exact admission and receipt, performs readiness/confirmation, and owns the external Delegation. Task identity is derived from source and tool-call identity. Goal has no separate Task CRUD list, revision model, reservation journal or Task proposal batch. Its work panel reads the independent Run Contexts.

Signals accept direct commands in their own mailbox, validate owner/revision/timing, and commit definition and receipt together. They retain occurrences until the Goal acknowledges acceptance. A Goal Signal observes conditions or schedules and sends evidence; it does not independently execute the Goal's external Task. Goal has no Signal proposal or delivery outbox. Startup reattaches subscriptions from Signal records and Task feedback from Run records.

## Scope

This implementation does not migrate historical Goal states, evaluation records or planning protocols. Existing runtime data and credentials are not changed by the rewrite. Verification uses fake models/transports and temporary Pi storage.
