# Goals

A Goal coordinates three parallel capabilities:

```text
Goal
├── Pi conversation — reasoning, user input and feedback
├── Tasks           — messages to Goal or Delegate Actors
└── Signals         — conditions and timers

Context change → System One Gate → Goal Agent Gate → Pi conversation
User input ─────────────────────────────────────────→ Pi conversation
Task messages / execution feedback ─────────────────→ Pi conversation
```

The conversation can request a Task or configure a Signal through tools. Their owners accept and persist those commands. Returning an assistant response does not stop a Task or a Signal. Goal End interrupts its conversation, revokes Task executions that have not started, and deactivates its Signals. Already-submitted external work remains owned by the existing Run and Delegation; interruption does not prove cancellation.

## Conversation

Each Goal has one durable Pi session/conversation, identified by its slug. Pi owns the transcript, model/tool rounds, tool-call recovery, queued submissions and native compaction. The existing agent adapter uses a Pi Document for exchange identities and committed result references. Goal does not recreate these mechanisms as an evaluation engine.

`goals/conversation.ts` supplies the stable assistant policy, current Context/memory reads, `update_goal`, `start_task` and `set_signal`. Assistant text ends a conversation turn naturally. There is no `finish_turn`, plan result, next-step union, result-application phase or automatic continuation protocol.

`update_goal` asks the Goal mailbox to change business progress. The mailbox verifies the invocation generation and completion evidence. Reads use public Context views; private credentials, execution handles and native tool transcripts remain excluded. Task and Signal commands go to their peer owners and return durable receipts. Unknown mutation outcomes stop further model work through the existing Pi tool fence.

## Input and recovery

Configured Goal creation commits a deterministic initial input. The built-in `/goals/personal` starts with an empty input queue and otherwise uses the ordinary GoalActor. Runtime activation is an in-memory gate; restarting does not manufacture another initial pursuit.

`SubmitInput` validates producer authority and commits the input and exact request receipt before acknowledging. One delivery at a time enters Pi with `requestId = inputId`; later inputs remain pending. Input status records only the cross-store handoff: pending, running, completed, failed, unknown or ignored. Context changes additionally retain the Agent Gate decision. There is no frozen Context catalogue or second evaluation journal.

The Goal mailbox is the only writer of Goal state. Asynchronous work returns through `pipeToSelf`, guarded by a generation. On restart, unfinished delivery reuses the same Pi identity and reconciles the existing exchange. Known failures can receive an explicit `RetryTurn`, which creates one successor input. Unknown outcomes block later delivery until reconciled; they never authorize a replacement external submission.

The persisted Goal state has five fields:

- `definition`: identity, title, description and optional completion criteria. Startup replaces the complete definition with current configuration, including removed optional fields, while preserving work.
- `status`: active or completed.
- `summary`: the single current business progress summary.
- `inputs`: accepted inputs, Context gate decisions, delivery status, responses, errors, per-input causal budgets and history projection markers.
- `receipts`: request ID, normalized command SHA-256 fingerprint and original commit receipt. Decoding removes fields outside the command contract; canonical JSON sorts object keys and preserves array order, so retries tolerate object key order without accepting changed content.

There is no top-level causal chain, error cache, duplicate progress field, completion-origin flag or history count. Execution feedback inherits causality from its admitted Run. The public view derives the latest settled error from inputs; history queries provide totals. Receipts and input admission remain atomic, and recovery retains each input's gate decision and history projection marker.

Task execution state lives in `/runs/...`; Signal state lives in `/signals/...`; conversation execution state lives in Pi. Goal stores none of their duplicated lifecycle state. Business history is a public input feed, not Pi's transcript. The Timeline reads accepted inputs and conversation responses directly.

## Gates

The Context reaction owner first runs the existing System One relevance screen. Only admitted evidence reaches the Goal. Before entering the persistent conversation, `goals/gate.ts` runs a separate read-only Goal Agent Gate. An unrelated change is recorded as ignored and never sent to the conversation. Gate failure is visible and may be explicitly retried.

User input, Task feedback and Task messages do not pass through generic Context screening. Feedback carries its original causal budget; starting a new conversation turn does not replenish it. Exhausted automatic feedback remains visible without invoking Pi.

## Tasks and Signals

Goals send `TaskMessage` through a shared dispatcher. A Goal Task delivers text directly; a Delegate Task includes prepared instructions, executor and replyTo Goal. The Run root saves admission and receipt before confirmation and owns external Delegation. Task identity derives from source and tool-call identity. Its work panel reads independent Run Contexts, including Signal Tasks replying to this Goal.

Signals accept direct commands, validate owner/revision/timing and commit the complete trigger, Task and receipt. Both Context and schedule triggers freeze a Task occurrence before dispatch. Receivers acknowledge durable admission. Goal has no Signal proposal batch, subscription facade or notification outbox. Startup restores Signal owners before registering Goals and reattaches Run feedback after receivers exist.

## Scope

This implementation does not migrate historical Goal states, evaluation records or planning protocols. Existing runtime data and credentials are not changed by the rewrite. Verification uses fake models/transports and temporary Pi storage.
