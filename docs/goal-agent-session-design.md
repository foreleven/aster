# Goal Agent Session, Evaluation, and Timeline design

Implementation update (2026-10-04): [Goal command redesign](goal-command-redesign.md) is the authoritative implemented protocol. This document retains the original session-boundary rationale. The implementation reuses `GoalReasoner.plan` and `Agent.make({ durable })`, without a separate session Actor/Layer. `GoalStarted` is persisted only on first activation; `RetryTurn` explicitly retries known failure with identical input IDs. Unknown outcomes require session reconciliation. Task/Signal writes use `finish_turn` proposals, and native transcript/compaction belong entirely to Agent Session. See the redesign's compatibility procedure for legacy pending requests without frozen snapshots.

This document is the target design for the boundary between a GoalActor, its durable Agent session, and the Goal Timeline. It completes the decisions recorded in ADR 0042 through ADR 0044 and records the first `pi-durable` implementation boundary.

## 1. Domain topology

```text
Goal
└── Goal Agent Session (one per Goal)
    └── Primary Conversation (one long-lived transcript scope)
        ├── Goal Inputs (ordered business inputs)
        ├── Agent Runs (one per Evaluation Group)
        ├── native assistant/tool transcript
        └── compaction checkpoints

Goal Evaluation Group
├── ordered Goal Inputs
├── one Agent Run
├── one Evaluation Result
├── Conclusion items
├── Goal Task changes
└── Goal-owned Signal changes
```

The Goal is the business owner. The Goal Agent Session is the durable model-execution owner. A Session is not a Goal, a Conversation is not a Run, and a Run is not an external Delegation. A Timeline is a structured projection of Goal records and references to execution records; it is not a parser over the model transcript.

There is exactly one active Agent Run per Goal. Inputs received during a Run are persisted and queued for the next Evaluation Group. They do not steer or mutate the active model context.

## 2. Input contract

Every accepted input is wrapped in a stable envelope before it is scheduled:

```ts
type GoalInputEnvelope = {
  inputId: string;
  goalId: string;
  ordinal: number;
  receivedAt: string;
  dedupeKey: string;
  evaluationId?: string;
  payload: GoalEvaluationInput;
};

type GoalEvaluationInput =
  | {
      _tag: "GoalIntent";
      intentId: string;
      source: {
        contextPath: string;
        actorPath: string;
        name: string;
        kind: "lark-chat";
      };
      content: {
        summary: string;
        summaryRevision: string;
        summaryFingerprint: string;
      };
      relevance: {
        score: number; // inclusive 0..1
        rationale: string;
        screeningRecordId: string;
        threshold: number;
        policyVersion: string;
      };
      createdAt: string;
    }
  | { _tag: "UserInput"; text: string; createdAt: string }
  | {
      _tag: "SignalOccurrence";
      occurrenceId: string;
      signalPath: string;
      evidence: string;
      occurredAt: string;
    }
  | {
      _tag: "ExecutionFeedback";
      runPath: string;
      taskId?: string;
      status: string;
      terminal: boolean;
      text: string;
      occurredAt: string;
    }
  | { _tag: "Startup"; reason: string; createdAt: string }
  | { _tag: "Retry"; retryOf: string; createdAt: string };
```

`GoalIntent` is the only Chat-derived input. It contains the persisted post-summary Chat content, source identity, score, rationale, and screening record identity. It never copies the raw pending message batch. `UserInput`, `SignalOccurrence`, `ExecutionFeedback`, `Startup`, and `Retry` remain distinct variants; a generic string such as `Evaluate.reason` is not a substitute for them.

The stable `intentId` is scoped to one Goal, one source Chat, and one `summaryRevision`. A repeated delivery reuses the same identity. A screening retry receives its own `screeningRecordId`, but a successful admission for an already admitted revision does not create another GoalIntent or retract the first one.

## 3. Evaluation Group and state machine

An Evaluation Group is created when a GoalActor takes a fixed ordered batch of inputs. It owns one `evaluationId`, one Agent Run identity, and all output references. The batch is closed before the model call starts.

The persisted record keeps independent phases so a UI never confuses handoff state with model state or external execution:

```ts
type EvaluationGroup = {
  evaluationId: string;
  goalId: string;
  inputIds: readonly string[];
  runId?: string;
  retryOf?: string;
  handoff: "pending" | "submitted" | "reconciliation_required" | "failed";
  run: "queued" | "running" | "result_ready" | "failed" | "cancelled";
  application: "pending" | "applying" | "applied" | "partially_applied" | "failed";
  createdAt: string;
  startedAt?: string;
  finishedAt?: string;
  error?: { code: string; message: string };
};
```

The user-facing group status is derived as follows:

| Condition                                                          | Timeline status           | Meaning                                                                |
| ------------------------------------------------------------------ | ------------------------- | ---------------------------------------------------------------------- |
| Handoff has not been accepted by Pi                                | `pending`                 | The Goal accepted the input; recovery can retry the same request.      |
| Handoff accepted or Run is active                                  | `running`                 | Pi owns an active or queued Agent Run.                                 |
| Submission outcome is unknown                                      | `reconciliation_required` | Query the stable request identity before resubmitting.                 |
| Run or result validation failed                                    | `failed`                  | The inputs and partial transcript remain available.                    |
| Result applied and all required operations settled                 | `completed`               | Planning finished; external executions may still be running.           |
| Local result applied but a cross-Actor operation is pending/failed | `partially_applied`       | The group is recoverable and shows the unsettled operation explicitly. |

Automatic recovery retries only known transient handoff failures, with a bounded backoff. It never resubmits an unknown request. A user Retry creates a new Evaluation Group with `retryOf` and the same input identities; it does not overwrite the original group.

## 4. Evaluation Result

The Agent Run returns one structured result. It may contain several output kinds at once:

```ts
type EvaluationResult = {
  resultId: string;
  evaluationId: string;
  runId: string;
  disposition: "advance" | "no_change" | "ignored";
  conclusion?: {
    text: string;
    evidenceRefs: readonly string[];
  };
  taskChanges: readonly TaskChange[];
  signalChanges: readonly SignalChange[];
  createdAt: string;
};
```

`ignored` is the durable way for a Goal Agent to explain that an admitted Intent does not justify Goal work after deeper reasoning. It remains visible with its score and rationale; it is never silently deleted or reclassified as an unrelated screening result.

GoalActor validates the complete result before applying its own state. Goal-owned summary, progress, Task changes, and Timeline references are committed as one mailbox-owned state transition. SignalRoot operations are separate durable operations with `evaluationId` and an operation idempotency key. They may settle later and move the group through `partially_applied`; no cross-store rollback is assumed.

An Evaluation Result is not an execution authorization. Creating or updating a Goal Task can still require preparation and user confirmation under the existing execution policy. External Delegation and Run status remain authoritative in their own Contexts.

## 5. Timeline projection

The Timeline API exposes groups rather than a flat transcript:

```ts
type TimelineEvaluationGroup = {
  evaluationId: string;
  status:
    | "pending"
    | "running"
    | "reconciliation_required"
    | "failed"
    | "partially_applied"
    | "completed";
  trigger: { kind: string; createdAt: string };
  retryOf?: string;
  inputs: readonly TimelineInput[];
  agentRun?: {
    runId: string;
    model?: string;
    startedAt?: string;
    finishedAt?: string;
    transcriptRef: string;
  };
  outputs: readonly TimelineOutput[];
  executionRefs: readonly string[];
};

type TimelineInput =
  | {
      kind: "chat-intent";
      inputId: string;
      chat: { actorPath: string; name: string };
      summary: string;
      score: number;
      rationale: string;
      screeningRecordId: string;
      summaryRevision: string;
    }
  | { kind: "user-input"; inputId: string; text: string }
  | { kind: "signal-occurrence"; inputId: string; signalPath: string; evidence: string }
  | { kind: "execution-feedback"; inputId: string; runPath: string; text: string; status: string }
  | { kind: "startup" | "retry"; inputId: string; detail: string };

type TimelineOutput =
  | { kind: "conclusion"; id: string; text: string; evidenceRefs: readonly string[] }
  | { kind: "task-change"; id: string; taskId: string; operation: string; status: string }
  | { kind: "signal-change"; id: string; signalPath: string; operation: string; status: string }
  | { kind: "handoff"; id: string; status: string; error?: string };
```

The UI should show a group header with trigger, status, timestamps, and retry links; expandable input cards with Chat name, summary, score, rationale, and source path; a collapsed Agent Run/transcript link; and separate output cards for conclusions, Task changes, and Signal changes. Execution results are separate cards linked by `evaluationId` and `taskId`; they do not rewrite the planning card.

## 6. Storage and ownership

The logical stores are separate even when a deployment co-locates their files:

| Store                     | Authoritative data                                                                          | Writer                                        |
| ------------------------- | ------------------------------------------------------------------------------------------- | --------------------------------------------- |
| Goal state                | Definition, summary, progress, Goal Tasks, group cursors                                    | GoalActor mailbox                             |
| Goal input log            | Accepted `GoalInputEnvelope` records and dedupe keys                                        | GoalActor mailbox                             |
| Evaluation log            | Group phases, result references, output application events                                  | GoalActor mailbox plus handoff reconciliation |
| Pi Session                | Immutable Conversation Entries, native transcript, Agent Run, durable generation/tool tasks | Pi Session owner                              |
| Screening dataset         | Pairwise System One scores, rationales, normalized input snapshots, model/policy metadata   | Screening adapter                             |
| Signal/Execution Contexts | Signal definitions, occurrences, Delegations, external status/outcomes                      | Their Actors                                  |
| Timeline                  | Read projection over Goal and execution records                                             | Projection/read model                         |

The current `GoalHistory` JSONL store is a transitional Goal input and compatibility feed. The target contract keeps structured Goal records separate from native Agent entries. New Timeline membership must be written as structured records and must not be inferred from the Pi transcript or old natural-language event messages.

Persist ordering for one input is:

```text
GoalActor: input + Evaluation Group(pending)
  → Goal handoff record(requestId)
  → Pi inbox dedupe(requestId) + Conversation Entry
  → Pi Agent Run transcript/result
  → GoalActor result application
  → SignalRoot/Task operations and execution links
```

The first and third stores cannot share a transaction. The protocol is therefore at-least-once delivery with stable identities and idempotent application, not cross-store exactly-once semantics.

## 7. Relationship to Agent messages

Each logical input is recorded in the Pi Conversation as an immutable Entry with two views:

```ts
type ConversationEntry = {
  entryId: string;
  evaluationId?: string;
  inputId?: string;
  data: unknown; // GoalInput, checkpoint, result reference, or protocol metadata
  model?: AgentMessage; // the bounded model representation, when applicable
};
```

`data` is the audit and Timeline source. `model` is the representation admitted to the model context. For a Chat Intent, the model view includes the Chat Summary, score, rationale, source name/path, and relevant references. Raw pending Lark messages stay in the Chat Context. Native assistant messages, tool calls, and tool results remain intact in Pi and preserve their associations and ordering.

The current Goal Summary is domain state, not an automatically generated transcript message. A context-building adapter supplies the current Summary, selected complete Conversation Entries, instructions, tool declarations, and retrieved evidence to each Run. Pi compaction is a context-size operation; it does not silently replace or mutate the Goal Summary. A Goal Summary update is written by GoalActor from an accepted Evaluation Result or an explicit summary operation.

When the context budget is exceeded, compact only complete tool exchanges, persist the checkpoint before advancing the working boundary, and retain references that let an Agent recover older evidence. A failed compaction leaves the previous boundary and working window intact.

## 8. Pi durable fit and adapter boundary

The researched `pi-durable` capabilities fit the target in these areas:

- Session, Conversation, immutable Entry, mutable Document, and durable Task provide the needed transcript, summary, inbox, and run primitives.
- A session mutation line can atomically commit Pi Entries, task records, and Documents within the Pi store.
- Durable input, request-id dedupe, recovery, compaction, watch/view, SQLite, and JSONL support the handoff and replay protocol.
- The effect-sandwich pattern supports intent persistence before an external side effect and outcome persistence afterward.

The following limits must remain explicit:

- A Pi store is owned by one process; it does not provide automatic multi-process locking, cluster coordination, or cross-store transactions.
- The effect sandwich does not make external side effects exactly once. Tool operations need idempotency keys and reconciliation, especially for SignalRoot changes and external submissions.
- Aster pins `@earendil-works/pi-agent-core`, `@earendil-works/pi-ai`, and `@earendil-works/pi-durable` to npm latest `1.0.0`. `@aster/agent` now opens one JSONL Conversation per Goal, resumes unfinished work, and uses stable request IDs for deduplicated handoff.

Introduce an Aster `GoalAgentSession` port in the Agent adapter layer. Core depends on this capability, not on Pi classes. The port needs operations equivalent to:

```ts
open(goalId): Effect<SessionRef, SessionError>
appendInput(session, envelope): Effect<AppendReceipt, SessionError>
startRun(session, evaluation): Effect<RunReceipt, SessionError>
readResult(session, evaluationId): Effect<Option<EvaluationResult>, SessionError>
watchRun(session, evaluationId): Stream<RunEvent, SessionError>
```

The first implementation keeps `GoalHistory` as the Goal input queue and compatibility feed. GoalActor persists each accepted input before scheduling evaluation; the Agent adapter submits only the committed batch to the durable Conversation. Pi owns native transcript and tool-task persistence, while GoalActor remains the business-state single writer. No GoalActor imports Pi storage or calls `runPromise` internally.

## 9. Failure, retry, and recovery rules

| Failure point                                 | Durable fact                                               | Recovery                                                                        |
| --------------------------------------------- | ---------------------------------------------------------- | ------------------------------------------------------------------------------- |
| Before Goal input commit                      | No accepted input                                          | Caller may retry using its source dedupe key.                                   |
| After Goal commit, before Pi handoff          | Input and pending group exist                              | GoalActor recovery submits the same request id.                                 |
| Unknown Pi submission result                  | Request may have been accepted                             | Reconcile by request id; never blind-resubmit.                                  |
| Agent Run failure                             | Partial native transcript and failed group exist           | Explicit `RetryTurn` creates a new group with `retryOf` and the same input IDs. |
| Pi result persisted, GoalActor callback lost  | Result exists under evaluation id                          | GoalActor reads and applies the result idempotently.                            |
| Goal application partially reaches SignalRoot | Local Goal result is applied; operation receipt is durable | Retry the operation id, then show `partially_applied` until settled.            |
| Process restart during compaction             | Previous summary/boundary remain valid                     | Resume only from the last durable checkpoint.                                   |

Agent Run retries never reuse a model call identity after a known failure. They reuse the logical input identities, create a new Run and Evaluation Group, and preserve the failed transcript for evaluation. A retry budget and backoff belong to the handoff/recovery service, not to Timeline rendering.

## 10. Observability and evaluation data

Screening records remain a separate append-only dataset. Every GoalIntent and Agent Run is joinable without copying raw messages into operational logs:

```text
screeningRecordId → intentId → evaluationId → runId → output ids → task/signal/execution refs
```

Operational events retain only stable identifiers, status transitions, model/policy version, latency, token usage, error category, and counts of inputs/outputs. Full prompts, Chat messages, credentials, and tool payloads stay in their authorized durable stores. The evaluation dataset may retain the normalized screening snapshot and the bounded rationale required for score calibration.

Useful later labels include `admission_correct`, `ignored_after_reasoning`, `task_useful`, `signal_useful`, and user correction events. These labels should be appended as evaluation annotations rather than rewriting the original score or rationale.

## 11. Implementation sequence and acceptance checks

1. Add schemas for GoalInput, EvaluationGroup, EvaluationResult, and Timeline projection, with stable IDs and explicit statuses.
2. Replace free-form Goal evaluation reasons at the ingress boundary with typed GoalInput envelopes; keep a compatibility mapping for startup and legacy commands.
3. Add the GoalActor-owned input/evaluation log and the idempotent handoff state machine using a fake GoalAgentSession port.
4. Persist and replay structured Run results before changing Goal state. Add tests for a crash between every two adjacent steps in the ordering above.
5. Validate the Pi durable adapter's storage recovery, single-writer ownership, compaction behavior, and request-id dedupe locally.
6. Build the Timeline projection and UI cards from structured records; keep the native transcript as a linked detail view.
7. Migrate or mirror the current GoalHistory transcript and remove the compatibility path only after recovery and pagination tests pass.

The minimum acceptance scenarios are: duplicate Chat summary revisions create one Intent; two Goals independently receive the same Chat when both scores pass; a pending handoff survives restart; an unknown submission is reconciled without a duplicate Run; a completed Pi result is applied after a lost callback; one result can produce conclusion, Task, and Signal changes; a false-positive Intent remains visible as `ignored`; execution results link back without changing planning history; and screening records can be exported for calibration without raw message leakage.
