# Task delivery and execution

Task describes work delivered to an Actor. `Goal` sends a message to a Goal; `Agent` starts an internal Agent; `Delegate` selects an external executor. Both execution variants have one persistent Task owner at `/tasks/<sha256(source, requestId)>`. There is no separate business Run or Delegation owner.

## Ownership and organization

TaskActor constructs Actor-local TaskState and TaskExecution services in its Behavior scope. The mailbox is the only caller that changes Task business state. Long execution uses scoped Fibers and returns through `pipeToSelf`, with generation checks for late results. Goal conversations and other Task mailboxes remain responsive.

```text
tasks/
  actor.ts                  # Mailbox scheduling, scoped workers, feedback and handoffs
  root.ts                   # Child registration, routing and watch
  protocol.ts               # Commands and asynchronous results
  delivery.ts               # Cross-Actor delivery, Goal attachment and feedback
  view.ts                   # Public projection, inspection and evidence capture
  state/
    snapshot.ts             # Business state, inputs, outcomes and work contracts
    model.ts                # accept, start, settle, cancel and Pi handoff recovery
    admission.ts            # Task identity, frozen Signal authority and envelopes
    store.ts                # Committed Ref and ContextRegistry persistence
  execution/
    service.ts              # run, send, cancel; internal/external orchestration
    checkpoint.ts           # Private Pi executor journal
    contracts.ts            # Executor capabilities/errors
    agent.ts                # Internal Agent invocation
    external.ts             # External confirmation and executor prompts
publications/
  actor.ts                  # Independent publication state, approval and transport
  contracts.ts              # Channel write capability and errors
```

TaskState has four business operations:

- `accept(input)` validates authority and identity, saves the Pi message and commits its reference and receipt before acknowledging.
- `start` selects pending work and records execution ownership.
- `settle(outcome)` saves a Pi result and updates only the inputs explicitly covered by that outcome. Later inputs remain pending.
- `cancel(reason)` revokes work that can safely stop without a provider cancellation. Running work first needs TaskExecution confirmation, then settles as cancelled.

The snapshot contains admission metadata, lifecycle status, input references and receipts, a round identity, the latest result reference and outstanding requests. Its statuses are `ready`, `running`, `waiting_input`, `completed`, `failed`, `cancelled` and `uncertain`. Input references are pending or completed; their transport delivery phases do not belong to the business snapshot. The private Store commits through ContextRegistry before updating its Ref, with an uninterruptible commit-to-Ref handoff.

TaskExecution exposes `run(work)`, `send(input)` and `cancel()`. It owns internal invocation, native steering, external submission, follow-up, responses, polling and recovery. Its private Pi `task.execution` checkpoints retain frozen executor policy, confirmation, provider handles, delivery markers and retained outcomes. A Semaphore serializes executor transitions and submissions outside the Task mailbox. Provider handles, prompts and transport phases never enter TaskSnapshot or public Context projections.

TasksRootActor registers and watches retained children without awaiting their recovery. A supervised root restart reuses existing children. Each child mailbox queues commands during startup; a slow or failed Task does not block its siblings or Goal registration. Supervision owns retries and terminal child failures reach the root through watch. Goal startup repairs its Task references, but does not wake or recover Task execution.

## Admission and messages

`TaskMessage` carries stable request identity, source, creation time, causal budget and optional frozen public Context evidence. Signals retain an exact occurrence before delivery; Goal tool identities derive from input and tool-call identity. Receivers validate source authority and reject changed identity reuse.

TaskState commits instructions/evidence as `task.admission` in Pi before saving the initial snapshot. Later `task.input` entries contain a typed Message, Answer, Check or Retry and its original receipt. Recovery finishes interrupted Pi-to-snapshot handoffs without losing accepted work. Pi `task.result` entries retain outcomes and their covered input identities. The Actor snapshot stores references, not message bodies.

After admission, delivery attaches the Task path to the source Goal, when present, and the reply Goal. Attachment is idempotent and commits before the tool returns success. An interrupted attachment retains the accepted Task identity; Goal restoration repairs missing references from Task metadata.

A Task retains its identity across follow-ups and completed-work reactivation. Exact command replay returns its original receipt. Cancelled and uncertain Tasks cannot accept a new message that bypasses cancellation or reconciliation. `Input` routes follow-up messages to an existing Task; `StartTask` creates or deduplicates initial admission.

## Execution and follow-up

Internal execution uses a retained Pi conversation and AgentRunner. Busy instructions use native Pi steering while the runner accepts it; otherwise they remain pending for another invocation. Lightweight Context and memory tools use the shared implementations in `tools/`. The Goal remains the user-facing speaker and Task tool records stay in execution details.

External execution freezes executor policy and requires confirmation through ApprovalQueue. Only a matching persisted decision permits submission. The adapter owns follow-up behavior: Codex steers an active turn and starts a later turn in the same thread; Pi retains its execution conversation. Doubao rejects unsupported follow-up delivery explicitly.

Executor delivery markers precede external I/O. A returned handle and accepted delivery marker commit together. Polling wakes when the checkpoint changes and ignores observations from a superseded revision. A provider round ending cannot complete inputs not covered by that execution. Inputs arriving during the transition to waiting are scheduled rather than stranded.

Task outcomes commit to Pi before business settlement. Retained outcomes can complete interrupted handoffs without invoking the executor again. Feedback uses a stable identity and retries missing Goal acknowledgement within the owner's Scope. Initial confirmation belongs to ApprovalQueue and does not emit repeated Goal feedback; execution outcomes and requests for further information do. Terminal Task replay requires no configured executor.

## Recovery, cancellation and approvals

`CheckTask` observes the original execution. External checking uses status or read-only `lookupSubmission`; it does not call submit or resume. Internal checking reconciles the original native request identity. `RetryTask` is a distinct command accepted only for known failed work: external resumption requires an authoritative resumable failure, and a definitely rejected initial submission can be explicitly submitted again. Internal retry uses a new request identity and the failed instruction in the retained conversation. Both commands carry request identity and expected revision; exact retries preserve the original receipt.

Unknown submission, follow-up, response or resume outcomes never authorize automatic resubmission. An older provider handle cannot prove that a later input was delivered. Task remains uncertain when the adapter cannot reconcile that input.

ApprovalQueue owns confirmation, permission and information requests. Resolve commits the validated answer before acknowledgement; the execution owner acknowledges delivery after persisting acceptance. Interrupted response delivery retains a sending/unknown marker and is not automatically repeated. Cancelled unstarted work revokes outstanding confirmation requests.

Local interruption does not prove external cancellation. Internal cancellation interrupts the scoped invocation; external cancellation requires a provider capability that confirms the operation stopped. An unsupported cancellation leaves the Task running. Ending a Goal cancels unstarted work; already-submitted work retains its Task owner.

## Publication and inspection

An explicit `PublishResult` action requests a separate handoff to PublicationsActor at `/publications`. That owner freezes one publication per Task from its committed result and original action. Later Task instructions cannot silently replace reviewed content. Execution confirmation never authorizes publication.

PublicationsActor owns its own snapshot and Pi journal, approval, authorization, sending marker and transport callback. Pi submission intent commits before transport I/O; recovery reconciles Pi-to-Context handoffs and never repeats sending/unknown operations. Published, rejected and unknown publication outcomes do not change Task completion. ChannelWrites and infrastructure Layers own transport capabilities and resource release.

`InspectTask` joins the Task snapshot, Pi messages, executor checkpoint, approvals and publication record. It returns instructions, follow-ups, outcomes, available tool records and source references without private provider metadata. The web Task detail subscribes to Task, approval and publication invalidations. External providers retain ownership of their private transcripts.

Tests use fake transports, real Actor mailboxes, Deferred and temporary Pi stores. They cover responsive follow-up, reactivation, handoff recovery, late completions, confirmation authority, separate check/retry semantics, cancellation confirmation and publication recovery. No historical-data compatibility path is provided.
