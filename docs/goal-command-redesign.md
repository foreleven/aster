# Goal command and Agent Run redesign

Status: implemented (2026-10-04). Existing persisted evaluation identifiers and historical results remain readable; user runtime data is not rewritten.

## Objective

Make a Goal an active pursuit of a user-defined outcome. On first activation it starts useful research and work, using the available read-only tools. It preserves progress across inputs and restarts, asks for essential decisions when needed, and applies structured results through its owning mailbox.

The design follows the deep-module principles in `codebase-design`: a small caller-facing interface with scheduling, durable handoff, result application and recovery behind it. It preserves ADR 0042, ADR 0044 and ADR 0045: isolated durable Agent sessions, results persisted before application, and a business Timeline independent of the native transcript.

The vocabulary `Turn` below means one existing Agent Run and its immutable admitted input batch. It does not mean one provider HTTP request or an external Delegation. Existing evaluation identifiers remain stable during rollout.

## Ownership

| Module                | Owns                                                                                                                           | Does not own                                         |
| --------------------- | ------------------------------------------------------------------------------------------------------------------------------ | ---------------------------------------------------- |
| Goal                  | Accepted inputs, turn admissions, summary, Goal Tasks, completion, output intents and business Timeline                        | Native model transcript or browser/process transport |
| Agent Session         | Native conversation, model/tool loop, transcript checkpoints, compaction, durable structured result and request reconciliation | Goal business-state mutation or execution approval   |
| Signal                | Conditions, schedules, occurrences and idempotent application of Goal-owned Signal operations                                  | Goal planning or completion                          |
| Task Run              | Preparation, confirmation, Delegation and authoritative execution status                                                       | Goal summary                                         |
| Context query adapter | Supported read-only queries and their evidence snapshots                                                                       | Goal decisions                                       |

Goal remains the single writer for Goal business state. Agent Session is the existing durable Agent capability, not a new mandatory child Actor or another host-configured Layer. Core supplies tools and decodes the structured result; `packages/agent` remains domain-neutral and does not import Goal types. Do not add a separate Actor for each callback just to reduce the parent union.

## Caller-facing requests

Only these four requests belong to the exported Goal request protocol. Each request carries a stable `requestId` and an explicit typed `replyTo`. Acceptance means durable acceptance, not completion of Agent work.

| Request               | Contract                                                                                                                                                                                                                   |
| --------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `SubmitInput`         | Validate and durably accept one typed delivery, return its stable receipt, and schedule eligible work. Same identity and same content returns the original receipt; different content conflicts.                           |
| `End`                 | Persist user-ended status, close new work admission, retire local inference, revoke pending execution approvals, and persist Signal deactivation intents. Retain history and already accepted external execution outcomes. |
| `RetryTurn`           | Explicitly retry a definitively failed Agent Run with a new turn identity, `retryOf`, and the same original input IDs. Unknown outcomes require reconciliation first.                                                      |
| `RetrySignalDelivery` | Retry a particular persisted Signal operation using its original operation identity and revision-bound recovery request. This does not rerun the Agent.                                                                    |

`SubmitInput` preserves five distinct input variants:

- `UserInput`: direct user instructions, with a stable request identity.
- `PersonalMessage`: user-authorized forwarding by Personal, retaining its operation identity, expected Goal revision and causal chain.
- `GoalIntent`: screened external evidence with source, revision and relevance provenance.
- `SignalOccurrence`: a deduplicated occurrence ID and its supporting evidence.
- `ExecutionFeedback`: Task Run status, execution identity and causal chain. Intermediate progress updates the execution projection; terminal outcomes may schedule a new turn.

This unifies admission, not trust. Use a closed, schema-backed delivery union with variant-specific validation and receipt semantics. Do not flatten it to `{ source: string, text: string }`. Public HTTP/RPC handlers construct only the input kinds they are authorized to submit; clients cannot select an internal producer identity. Personal, screening, Signal and Run ingress retain their existing provenance checks and deduplication rules. A completed Goal rejects new actionable input but still records/reconciles feedback from work it already owns.

An input accepted while a turn is running is queued for the next turn. It does not mutate the frozen input or interrupt the current turn. Explicit `End` can cancel local work. Mid-turn user steering is a separate future feature.

## Runtime control

Keep a separate, runtime-only protocol:

| Command      | Contract                                                                                                                                                                                 |
| ------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `Activate`   | After integrations are ready, enable scheduling; create the initial pursuit input once for a new Goal, or recover existing work for an existing Goal. Repeated activation is idempotent. |
| `AwaitReady` | Acknowledge successful recovery and activation/admission readiness. It does not wait for a model response or for every output operation to settle.                                       |

Runtime selects and activates integrations before activating Goal work. Recovery that fails must fail readiness rather than returning a success handshake. User input received during recovery can be durably accepted but cannot start inference until recovery and activation have both succeeded.

The initial input is a persisted `GoalStarted` record with a deterministic identity scoped to the Goal identity and pursuit generation. Runtime restart does not create another `GoalStarted`. A future explicit reopen would need a new pursuit generation; this proposal does not add reopening.

When there is no pending turn, new input, persisted continuation or due Signal, restart does not invoke the model. Continuous monitoring is driven by Signals, not by a synthetic startup message on every process boot.

## Private mailbox protocol

Four private messages coordinate work. These are not exported to business callers:

| Message                 | Purpose                                                                                                                                                                           |
| ----------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `RunNext`               | A coalesced wakeup with no free-form `reason`. If active, activated, recovered and idle, admit the next eligible batch and start one Agent Run.                                   |
| `TurnSettled`           | Return a typed Agent Run outcome to the mailbox with `turnId`, behavior generation and stable result identity. Validate and apply once, or record a failure/unknown outcome.      |
| `SignalDeliverySettled` | Settle one persisted output operation using its operation ID and dispatch generation.                                                                                             |
| `RecoverySettled`       | Return session reconciliation and Signal-subscription restoration results for the current behavior generation. It only releases recovery gating; it cannot finish a running turn. |

The exported request protocol therefore has four variants. Including runtime control and private messages, the implementation has ten variants. The reduction comes from ownership changes, not from hiding seventeen commands inside an untyped `Internal` payload.

No detached Promises or mutable completion flags are introduced. Production schemas use `Schema.TaggedStruct`, explicit typed `ReplyTo`, tagged errors, `Match` dispatch and scoped Effect work. Use `context.pipeToSelf` for asynchronous work. All late callbacks must match both the stable turn/operation identity and the current behavior/dispatch generation before changing state.

## Admission and the first Agent invocation

`RunNext` performs these mailbox-owned steps:

1. Check Goal status, activation/recovery gates and active turn.
2. Select an ordered, bounded batch of unconsumed inputs, then check the causal budget of that actual batch. Reserve room for the earliest pending user instruction so an exhausted older continuation cannot starve new instructions. Preserve ordinal order within the batch; an excluded input cannot supply authority. A single oversized input remains admissible so it cannot block the queue. Distinguish initial pursuit, new inputs and an explicit continuation from their typed records.
3. Freeze the exact Goal definition, selected input IDs and payloads, business summary, Task/Signal read model, initial public Context snapshot and any historical range required by the handoff. Store the content itself or immutable references that are guaranteed to remain resolvable; mutable paths alone are insufficient.
4. Persist the turn admission and request identity before calling the Agent Session.
5. Start the scoped session operation and pipe its outcome back as `TurnSettled`.

This removes `FreezeEvaluation`: the worker starts only after a complete admission exists. Slow transcript loading and compaction belong to Agent Session and must not block the Goal mailbox. Startup and pending-input normalization do not need a separate model call.

The model receives the actual Goal as its task and the admitted inputs as the new conversation material. The first input is an instruction to begin pursuing the Goal, not `"Start Goal evaluation"` and not an evidence-only wrapper around that sentence. User instructions retain their role; external intents, Signal evidence and execution reports remain explicitly untrusted evidence. Existing summary and history provide continuity without redefining the user's authority.

For the travel example, the first run discovers the Xiaohongshu and Ctrip catalogues, queries both, compares destinations, and returns useful provisional recommendations. Unknown departure city or dates limit route-specific fares; they do not prevent destination research. Successful research findings belong in the result even when no Task or Signal is created.

## Agent tools

The Agent directly uses read-only Context queries, memory and paginated history. Task and Signal discovery reads the frozen business read model admitted for this turn, including deleted entries needed for ID-specific inspection. Live external query results are timestamped and retained in the durable tool transcript; they are not claimed to be part of the initial frozen snapshot.

All proposed Goal business changes are submitted through one final result tool, `finish_turn`. Remove direct Task/Signal mutation tools and the generic `Tool` mailbox path after updating their current callers. Result application revalidates current revisions, Goal status and execution constraints; frozen reads do not authorize overwriting newer state.

Removing `Tool` also removes `SignalEdited`. This is an interface change with a concrete invariant: inference reads evidence and proposes changes; the Goal mailbox validates and commits the complete proposal. Read-only research does not need an external Task or execution confirmation.

## Turn result and continuation

A structured result carries stable turn/result identity, findings, source references, the current summary, Task/Signal proposals, disposition and one next-step choice:

| Next step      | Meaning                                                                                                                                                                      |
| -------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `Continue`     | More useful work can be done now. Include a concrete objective and a link to the preceding outcome; Goal validates and persists one continuation input before scheduling it. |
| `WaitForInput` | A specific essential user decision or missing fact blocks the next useful work. Include the questions alongside the findings already obtained.                               |
| `WaitForEvent` | Await an existing execution, approval, source change or explicitly configured Signal. Keep the relevant stable references. This does not create a timer implicitly.          |
| `Complete`     | The configured completion criteria are satisfied with evidence. Ongoing Goals without completion criteria remain active until the user ends them.                            |

Model this as a closed tagged union. Do not add an independent `completed` boolean that can contradict the next step. Preserve `advance`, `no_change` and `ignored` semantics for external evidence; ignored/no-change results cannot introduce mutations, completion or self-triggered continuation.

`Continue` is a proposal, not an unconditional loop. Reuse the existing causal chain limits and per-root admission limits. Each continuation consumes budget and preserves the original root identity. Coalesce it with pending inputs where appropriate; never reset the budget by inventing a new startup input. Reject duplicate continuation identities and visibly explain exhausted budgets. Missing optional preferences alone do not justify waiting if meaningful relevant research remains possible.

Session persists the structured result before notifying Goal. Goal validates the entire result, then atomically commits its summary, Task changes, input consumption, business Timeline links, continuation/wait state and output intents. Only afterward does it dispatch cross-Actor operations or materialize execution reservations. Partial Signal delivery remains separately recoverable and does not discard already committed findings.

## Persistence and recovery

Keep these concepts separate in storage and the UI:

- Goal status: active or completed, with completion origin such as user-ended or criteria satisfied.
- Agent Run outcome: admitted, running, result available, known failure or reconciliation required.
- Result application: pending, locally applied, partially applied or fully settled.
- Next-step state: continue, waiting for input, waiting for an event or complete.

A completed Agent Run does not imply a completed Goal or completed external execution. A failed Signal delivery does not imply that the Agent must rerun.

Recovery inspects the saved turn identity and session receipt/result. If a result exists, apply/replay it without another model invocation. If the session confirms that it never accepted the request, the same frozen admission can be submitted under the same idempotency identity. If the run is active, reconnect or observe it. If the outcome is unknown, retain `reconciliation_required`; never create a replacement run automatically. A user `RetryTurn` is available only after a known failure and retains the original input membership.

The Goal business journal remains the Timeline authority. Agent Session owns native transcript and compaction; neither free-form transcript parsing nor duplicated transcript callbacks may reconstruct business state. Historical business records remain readable by the Agent as evidence. Removing `Transcript` and `Compacted` requires the session module to own any missing durable input-window compaction behavior first; it is not permission to drop old conversation content. Tests use a fake durable session interface instead of keeping a separate production non-durable Goal workflow.

`End` persists closure before replying and fences subsequent local callbacks. Closing a local Fiber is not evidence that a provider run or Delegation was cancelled. Retain late outcomes for reconciliation/history, but do not apply new business proposals after closure. Already-running external executions remain governed by their Run owners. Signal deactivation is a durable output operation whose final status remains visible after End acceptance.

## Mapping from current commands

| Current command                                               | Proposed destination                                                                         |
| ------------------------------------------------------------- | -------------------------------------------------------------------------------------------- |
| `UserMessage`, `Deliver`, `Intent`, `Occurrence`, `Execution` | `SubmitInput`, preserving five typed payloads and their distinct admission policies          |
| `End`                                                         | `End` with durable closure and output intents                                                |
| `RetrySignal`                                                 | `RetrySignalDelivery`                                                                        |
| `Evaluate`                                                    | Runtime `Activate` plus private `RunNext`; trigger meaning is retained in typed inputs       |
| `Planned`                                                     | Private `TurnSettled`                                                                        |
| `Ready`                                                       | Runtime `AwaitReady` with a defined recovery contract                                        |
| `FreezeEvaluation`                                            | Removed after freezing the admission before session submission                               |
| `Transcript`, `Compacted`                                     | Removed from Goal after Agent Session owns durable transcript/window handling                |
| `Tool`, `SignalEdited`                                        | Removed after reads use the admitted read model and all writes use the final result          |
| `SignalDelivered`                                             | Private `SignalDeliverySettled`                                                              |
| `Reconciled`                                                  | Private `RecoverySettled` for recovery; deactivation receipts use output delivery settlement |

`RetryTurn` is an explicit caller-facing replacement for using arbitrary `Evaluate.reason` strings to retry failed work. Root routing (`Route`) stays an implementation detail of the Goal runtime and is not added to the Goal business protocol.

## Rollout and verification

1. Separate exported requests, runtime control and private mailbox messages, using temporary ingress adapters for existing callers. Preserve current receipts, version checks and deduplication identities.
2. Introduce idempotent activation and typed initial/continuation inputs. Update first-run prompt construction. Keep transport startup free of unsolicited polling.
3. Freeze complete turn admissions in the mailbox. Exercise the existing durable session protocol; add any necessary inspection/checkpoint capability through the existing Agent module.
4. Switch Task/Signal reads to the frozen read model and remove direct mutation callers. Route all changes through validated turn results.
5. Remove Goal transcript/compaction callbacks only after session-owned history continuity is verified. Introduce explicit next-step results and bounded continuation.
6. Update API and Timeline projections while preserving existing persisted identifiers and original records. Use versioned decoding or an explicit offline migration where required. Do not rewrite, delete or restart user data as part of this design review. Historical completed results remain historical and are not relabeled or rerun.

Use real Goal/Actor runtime tests with injected sessions, query adapters, `Deferred` and `TestClock`. Verify first activation starts once; empty restart does not rerun; requests arriving during recovery/running are durably queued; source identities cannot be spoofed through public ingress; receipt replay detects payload conflicts; frozen input survives restart; a persisted result with a lost callback is applied once; unknown provider outcomes block retry; late generations cannot apply; End preserves uncertain external outcomes; Signal retries do not rerun the model; compaction preserves native conversation and source references; and continuation consumes causal budget while providing useful research before optional clarification.

## Alternatives considered

Keeping all existing behavior and renaming `Evaluate` to `Work` would improve wording but leave ownership and duplicate mutation paths unchanged. Collapsing every callback into an opaque `Internal` or `Event` command would hide the complexity from types while retaining it in handlers. Moving every concern to another Actor would multiply lifecycle and delivery protocols. The proposed seam keeps one Goal business owner and reuses the existing durable Agent Session, removing only responsibilities that have a clear existing owner elsewhere.

## Implemented readiness and compatibility

`AwaitReady` defaults to `stage: "activated"`. Runtime may explicitly await `stage: "restored"` to make delivery receivers available before integrations activate; this does not enable inference. Root readiness aggregation runs in scoped background work and returns through its mailbox, so waiting cannot block routing or `Initialize`. Failed recovery returns `Failed`. A root-scoped activation gate also covers supervised child restarts.

Fresh state has `activated: false` and creates one deterministic `GoalStarted` on activation, including when input already arrived during restoration. A missing marker identifies historical state and is adopted without inventing another startup. Known turn failures leave later unassigned inputs eligible; uncertain results retain their pending identity and block replacement work.

The session adapter inspects durable exchange and native submission records during reconciliation. Saved results can replay across prompt/catalogue upgrades. An exchange that never reached provider execution may resume only its original frozen admission; a previously accepted uncertain provider request cannot issue a fresh provider call during reconciliation.

Legacy pending requests without a complete frozen snapshot use `replayOnly` inspection. A saved result is returned without inference. A missing session receipt is reported as a known failure without submitting anything. An unsettled receipt remains uncertain and blocks work. Legacy failed records without immutable input IDs reject `RetryTurn` before acceptance.

For an unsettled legacy receipt, stop the application, back up the Goal Context/history and its Pi session directory, and restore the matching application version to reconcile that original request under its original catalogue. Preserve its identity and receipt. Restart the upgraded version after a saved result is available. Do not delete the receipt, fabricate a frozen snapshot from current state, or manually clear unknown state to force another provider call. New instructions after a confirmed missing receipt are fresh requests; they do not rewrite the historical turn.
