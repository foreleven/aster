# Goals and work IM exploration

## Goal Agent Session boundary (accepted, 2026-10-02)

Each Goal owns one isolated durable Pi Session with one primary Conversation. The Conversation is the long-lived transcript scope; successive Agent Runs are planning exchanges within it. A Goal is therefore not a global Pi Session, and the Goal Timeline is not a projection of the raw Agent transcript.

GoalActor remains the single writer for Goal business state, Goal Inputs, Evaluation Groups, and the public Goal History/Timeline records. Pi Session owns the native Agent transcript, Agent Run lifecycle, and durable model/tool execution tasks. GoalActor accepts and persists a Goal Input before handing it to the Pi Conversation through a durable, retryable handoff. The handoff carries stable Goal/Input/Evaluation identities and is idempotent; it does not assume a cross-store atomic transaction.

The handoff may be temporarily pending or failed after the Goal Input has been accepted. Recovery retries the handoff without creating another Input or Evaluation identity. Agent transcript entries and tool records are not parsed to reconstruct Timeline membership; structured Goal records remain authoritative for that projection.

## Goal Input handoff and recovery (accepted, 2026-10-02)

Acceptance of a Goal Input is visible immediately in the Timeline pending-input queue. Admission closes an ordered input batch and creates its Evaluation Group with `pending` handoff status. Inputs arriving during that run remain in the queue for a later group. A successful idempotent handoff moves the group to `running`; a completed Agent Run moves it to `completed`, while a known planning failure moves it to `failed` and retains the partial transcript and all inputs. A lost or unknown submission result is `reconciliation_required`: recovery looks up the stable request identity before any resubmission.

Pi unavailability may leave an accepted group pending and is recoverable after restart. Automatic recovery may retry a handoff with the same request identity. A user-initiated Retry never overwrites the original group: it creates a new Evaluation Group with `retryOf` pointing to the failed or reconciliation-resolved group and reuses the same durable input identities. These states describe planning handoff and Agent processing only; they do not describe the status of an external Task or Delegation.

## Agent Run result ownership (accepted, 2026-10-02)

The Pi Session persists each Agent Run's native transcript and structured Evaluation Result under the stable Evaluation Group identity. The result includes the planning conclusion and declarative proposals for Goal Task or Goal-owned Signal changes. GoalActor receives the result through its mailbox, checks the evaluation identity and generation, and remains the only writer that applies Goal Summary, Goal Task, Goal-owned Signal association, and Timeline state. A restart may replay an already-persisted result by request identity; it must not start another model call for the same Evaluation Group.

The Pi Session cannot mutate Goal state directly, and a callback alone is not the durable record of a result. External Delegation state remains owned by its execution Context and cannot be rewritten by an Agent Run result.

Task result application is implemented through `GoalPlan.taskChanges`. The model has Task read tools and proposes ordered create/update/delete/execute operations in `submit_plan`; it has no Task mutation tools. The Goal mailbox validates the whole batch against current Task and Run revisions before committing the result, Task state, summary and execution reservations together. A rejected batch retains its result/error without partial Task writes. Run identities derive from the Evaluation and operation index. Creation and cancellation occur after commit and recover from the retained Task state; unstarted obsolete executions lose confirmation, while started executions retain their frozen input. Signal result application uses `GoalPlan.signalChanges` with at most 16 proposals and one change per target. Task and Signal proposals are validated together before any write. The same Goal commit retains the result, Task state and frozen Signal outbox commands. Each command has a stable Evaluation-derived identity, full definition, owner, expected Context revision and causal budget. The Signal mailbox commits its definition and receipt atomically; exact replay returns the original receipt before checking later revisions. Delivery markers and bounded attempts survive restart. Evaluations remain `partially_applied` while any Signal operation is pending, unknown or rejected, and become `completed` only after all receipts are saved. Public projections expose operation references/status/receipts without frozen causal evidence. Reattachment of an unchanged Signal preserves its Context revision.

The complete target object model, phase state machine, Timeline projection, storage ownership, Agent message mapping, Pi durable adapter boundary, and recovery rules are specified in [Goal Agent Session, Evaluation, and Timeline design](./goal-agent-session-design.md). The current implementation remains transitional until the typed input/evaluation protocol and session port are introduced.

## Chat changes and Goal relevance (accepted design, 2026-10-01)

Ordinary incremental Lark chat processing has two distinct System One decisions. First, the current rolling Chat Summary and pending changed messages determine whether summarization is needed. If needed, summarization produces a new rolling summary that is persisted in the chat's Context State. A resulting Context State change then enters Goal relevance screening. Each source Context and candidate Goal pair receives a separate System One request containing the current Chat Summary and only that Goal's title, description, and current Goal Summary. The response scores that pair independently; it does not select or rank Goals against one another. Multiple Goals may independently qualify, or none may qualify. The score means the degree to which the Chat Summary contains evidence relevant to that Goal; importance and urgency remain Goal-planning concerns.

Every pairwise relevance decision must emit a structured evaluation record for later calibration and error analysis. The relevance score is a continuous number in the inclusive range `0..1`; it expresses only how strongly the Chat Summary is relevant to that Goal. The initial admission threshold is `0.7`, recorded with its policy version; missing or invalid scores fail closed. The record must identify the source Context, Goal, summary/version fingerprints, score, admission result, threshold/policy version, System One model, request correlation, latency, and typed failure when applicable. Evaluation records are persisted in a separate local append-only JSONL dataset. Each record retains the complete normalized screening input snapshot—Chat Summary, Goal title, Goal description, and current Goal Summary—alongside fingerprints. It does not retain the raw message batch, credentials, or unrelated Contexts. Normal operational logs must not copy raw chat messages or credentials and should retain only a record identifier and outcome summary.

When a pair passes admission, the delivery to the Goal carries the `score` and bounded `rationale` with the source Context identity. These fields are durable Goal activity data and are displayed by the Goal UI so a user can understand why the Context entered the Goal. An unrelated or failed screening decision is never delivered to that Goal. The current `Evaluate` command carries only a free-form reason, so the delivery contract and Timeline projection still need implementation.

The current `Evaluate.reason` is only a caller-generated trigger label: startup, user input, Signal occurrence, execution result, compaction retry, or `Context changed: {path}`. It is not produced by System One and does not contain the Chat Summary or screening result; queued labels are concatenated by the evaluation scheduler before being passed to the Goal Agent. A passing relevance assessment must instead deliver a structured `GoalIntent` carrying the screening result. Other trigger types may retain separate structured trigger metadata and must not be represented as GoalIntent merely because they start evaluation.

For a Lark Chat change, `GoalIntent` is the meaningful input delivered to the GoalActor. It contains the source Chat actor path, Chat name, the persisted post-summary content, the independent relevance score, the bounded screening rationale, the summary revision/fingerprint, the screening record identity, and its creation time. The content is the Chat Summary produced by the first System One decision and summarizer; raw pending messages remain owned by the Chat Context and are not copied into the GoalIntent. The GoalActor persists the GoalIntent and invokes the Goal Agent with it and the current Goal state. The resulting evaluation may record a conclusion, create or update a Goal Task, create or update a Goal-owned Signal, or produce more than one of these in one planning round.

One Goal Agent evaluation is therefore an ordered input/output exchange: it consumes one or more admitted GoalIntents plus current Goal state, and may emit a conclusion together with Task and Signal changes. Each emitted change remains owned by its existing Goal operation and is represented separately in the Goal activity projection; the evaluation itself is not forced into one mutually exclusive result kind.

Accepted batching policy (2026-10-02): while a Goal Agent evaluation is running, arriving GoalIntents remain pending in reception order. After that evaluation finishes, the pending Intents are supplied as an ordered list to the next evaluation rather than starting one invocation per arrival. Each Intent retains its own identity, source content, score, and rationale; batching does not concatenate them into a reason string or collapse different source revisions. The running evaluation's input batch remains fixed. The next batch must respect the existing context budget, retaining any overflow for subsequent evaluations.

Accepted Timeline grouping (2026-10-02): one Goal Evaluation forms one Timeline group linking its ordered input Intents to its recorded outputs. Each input shows the Chat name, summary content, score, and rationale, with its source path available for inspection. Outputs appear as distinct items within the group: text conclusions, Goal Task changes, and Goal-owned Signal changes. The group shows the evaluation's processing, completed, or failed status. Evaluation completion describes the planning exchange; it does not mean an external task execution has finished. Group membership must be explicit rather than inferred from neighboring timestamps or free-form text. A later execution result is a separate Timeline item linked to both the Task and the Evaluation that proposed it; it does not rewrite the original planning outcome.

If a Goal Agent call fails, the failed Evaluation Group and its input Intents remain durable. A retry creates a new Evaluation Group with `retryOf` pointing to the failed group; it does not overwrite the failure, discard the Intents, or create duplicate Intent identities. Retry admission and limits remain to be specified separately from the Timeline shape.

Accepted Intent identity (2026-10-02): one Goal, one source Chat, and one persisted `summaryRevision` identify one `GoalIntent`; implementations may derive `intentId` as a stable hash of those values. Repeated delivery or screening of the same revision reuses that Intent identity and never creates a duplicate Timeline input. Screening attempts remain separately auditable, including retries after an unknown or failed decision; the policy for a conflicting later score is still open.

For a stable `summaryRevision`, the first successful screening admission is authoritative for routing. Later screenings of that same revision may be recorded for calibration but do not retract, replace, or re-route an already admitted Intent. A failed or unknown screening attempt has no admission result and may be retried before an Intent is created.

A Context change screened as unrelated stays outside that Goal's History and Feed; screening diagnostics belong to the global runtime. This is an admission boundary, not merely a Timeline display filter. The previously suggested additional include/exclude scope fields have not been adopted.

The current `relevantGoals` implementation sends separate pairwise scores with the source Chat Summary and the Goal title, description and current summary. Policy `goal-relevance-v3` requires an evidenced link to the specific Goal outcome or responsibility before scoring impact. For project-specific Goals, shared terminology, owners, urgency and overdue bugs do not establish project identity; an unverified link scores at most three on the zero-indexed `0..9` rubric. Explicit dependencies and established aliases can qualify without repeating the Goal name. The Goal Agent independently checks externally routed evidence during its existing evaluation and uses `ignored` with no mutations when none of those inputs has a verified link. Mixed batches may advance using only verified relevant evidence. This prompt policy reduces false-positive propagation without another model call; it is not a deterministic semantic guarantee. An already admitted Intent and its native transcript remain durable, including an ignored evaluation. Recovered summary stages and explicit flushes may resume or perform summarization without repeating the first decision.

## Goal titles

Goal definitions accept an optional nonblank `title` alongside the required `description`. The title is display metadata; the description remains the detailed reasoning instruction. The public Goal state stores the effective title (configured title or description fallback). Startup refreshes that field, including for existing Goal records, without resetting tasks, history or completion status. Older records without a title remain readable, and the UI falls back to description, then slug.

## Goal confirmation and conversation continuity (implemented, 2026-09-30)

The Goal owns a `tasks` collection of persistent Goal Tasks. Each Goal Task supplies the stable identity for one tracked piece of work; no separate work-item layer is introduced. Its Agent receives tools to inspect, create, and modify those tasks. Goal-generated Signals can associate with the work they monitor, and execution records associate with the corresponding Goal Task. For example, evidence from two chats about the same version-impact analysis updates the same Goal Task instead of creating duplicate delegations. A Signal occurrence first returns to the Goal Agent for assessment. Only when the Agent proposes execution is a concrete execution proposal prepared for user confirmation; execution results feed back into that Goal Task.

The Goal Agent receives tools to create, inspect, modify, and delete both its Goal Tasks and its Goal-generated Signals. This includes editing task descriptions, plans, and evidence, and marking a Goal Task complete. After task completion, Signals may continue monitoring for subsequent changes; completing a task does not itself require ending the associated monitoring. Execution states such as running, succeeded, and failed come from actual runtime records and cannot be overwritten by the planning Agent. The existing prepared `Task` payload represents instructions and input for one execution; it is distinct from the persistent Goal Task. Exact tool contracts remain implementation details. Subsequent monitoring occurrences are assessed by the Goal Agent using the existing task operations.

Accepted deletion policy: tasks and Signals are logically deleted, with their history retained. Deleting a Goal Task revokes its pending execution confirmations and prevents new executions of that deleted task; already-running executions continue to completion and retain their results. Late preparation or approval responses must not launch a deleted task. Deleting a Signal stops its monitoring. Task deletion and Signal deletion are separate operations with no automatic cascade between them. Default active listings omit deleted entries, while history remains accessible.

Scope constraint: Goal Tasks form a flat collection. Do not introduce task-to-task links, dependencies, parent/child tasks, or predecessor/successor relationships in this iteration. The proposed linked-follow-up-task behavior is not adopted. The Agent uses the existing create/update operations and Goal conversation to handle new information; there is no separate relationship-management workflow. This constraint concerns relationships between tasks and does not remove their Goal ownership or references to actual execution records.

Goal-generated Signals default to `confirm`: after a concrete Task is prepared and passes readiness checks, execution must wait for the user's confirmation. This supersedes the earlier auto-mode decision and is enforced for Goal execution proposals.

Accepted rollout scope: the user will manually delete old local data and validate from a fresh state. Migration of existing Goal state, custom messages, Signals, pending Runs, and approvals is out of scope for this iteration. Do not add automatic cleanup or delete user data as part of implementation. Normal persistence and restart recovery for data created by the new implementation remain required.

The requested Goal representation includes a current summary and a persistent conversation made of standard Agent messages. Each Goal Agent invocation receives a bounded working window of that conversation as its actual `messages` input, alongside the current summary, rather than serializing conversation history inside a new user prompt. The repository's shared Agent contract already accepts and returns `AgentMessage[]` from pi. Complete persistence does not imply passing the entire history to every invocation.

Persist the full native Agent transcript, including assistant tool calls and their corresponding tool results (such as Context reads, memory searches, and memory expansions), as well as user and assistant conversation messages. Preserve tool-call/result associations and ordering; do not reduce each planning round to its final progress text. The full Goal History resides in local files, while the in-memory conversational working set is summary plus a bounded messages window. Compaction controls Agent context size without removing the original history. It must preserve complete conversational units rather than arbitrarily cutting through a tool exchange; exact budget defaults and configuration names remain to be specified.

Goal progression may produce only a recorded milestone, conclusion, or important observation when the available evidence does not justify creating or executing a task. These entries belong in messages and the complete Goal History, alongside user exchanges, task decisions, Signal changes, and execution feedback. Together they provide a continuous Goal Feed; a planning round does not need to produce a Task to be useful. The feed must remain browsable beyond the in-memory messages window. Its presentation and history access API remain to be designed, and its display does not replace the native Agent transcript.

Accepted context policy: use a configurable token budget covering the summary, selected messages, instructions, tool definitions, and retrieved evidence, with room reserved for generated responses and tool interactions. When needed, fold an older complete message prefix into the summary, retaining source references and an explicit covered-history boundary, and keep recent complete exchanges. Persist the new summary and boundary before advancing the working window. Paginated history lookup tools allow the Agent to recover original evidence within the same budget; current task and execution facts remain authoritative in their own records. Large individual tool results and mid-invocation growth also require bounding, not merely a message-count cap at invocation start.

Recommended failure behavior for implementing this policy: if summary generation or persistence fails, retain the previous summary, history boundary, and working messages. Do not silently discard unsummarized history to fit the budget. If the existing context cannot fit, defer the planning invocation and surface a retryable compaction error. History arrivals after the selected compaction boundary remain outside that compaction and must not be lost when its result is saved.

After each successful planning round, update the Goal Summary to explicitly cover existing outcomes, tasks currently executing, tasks awaiting the user's confirmation, and unresolved questions or work. Cite the associated task/run records. This gives subsequent planning a view of prior and outstanding work as well as project progress, helping it avoid proposing the same work again. Exact execution status continues to come from durable execution records; Goal Task completion is maintained explicitly through task tools, and the summary must not independently redefine either status.

When the same work item already has a task awaiting confirmation or currently executing, reuse its Goal Task and retain newly arrived evidence rather than creating another equivalent task. A task awaiting confirmation may be revised, but confirmation must apply to its latest version; an approval of a superseded version must not authorize the revision. For an executing task, record supplemental information locally and wait for its result before deciding whether follow-up work is needed. This does not implicitly authorize sending a new instruction into the running external session. Goal Task identity links the work across Signals and source Contexts; the matching rules for newly discovered work remain to be designed.

Before this implementation, the code stored `state.progress` and custom Goal events (`type`, `text`, `at`, `references`). That adapter embedded the last forty events as JSON in a new user message and discarded the native transcript. The new adapter supplies native messages, persists generated messages before tool execution proceeds, and records runtime evidence explicitly. Existing persisted messages are not migrated under the accepted fresh-state rollout.

This work is part of the ongoing [Doubao task quality and continuity design](task-delegation-design.md), including readable task submission, executor default instructions, memory recall/result capture, and duplicate-work prevention.

### Implementation and validation

Implemented in core Goal/Signal actors and tools, the Agent adapter, integration history storage, Doubao submission, memory capture, and the local API/web client. Full Goal history lives at `~/.aster/goals/{slug}/history.jsonl`; the existing Context files hold the working window and summary. Defaults are `contextTokens: 200000` and `reserveTokens: 8192`. Durable planning uses Pi's native token accounting and compacts at 191808 tokens by default, subject to the model context window. GoalHistory working windows and isolated-run guards retain conservative UTF-8 byte accounting. The feed endpoint paginates full history; task state and execution links appear in the Goal inspector.

Validation: workspace build and typecheck pass; 117 backend tests and 6 browser tests pass. Coverage includes revision-bound confirmation, deletion during preparation/running execution, native transcript hooks, history compaction and recovery, one-time/Cron restart scheduling, stale timers, durable outcome-memory retries, and feed pagination. Validation uses controlled adapters; no live Doubao task was submitted, and no old user data was deleted.

The architectural inspection below describes the pre-change implementation. Later historical sections retain earlier decisions for context; the implemented decisions at the top supersede conflicting auto-execution and transcript policies.

### Architecture review and accepted Signal-to-Goal routing

Accepted: Goal-owned Signal occurrences go to the Goal Agent rather than directly creating an external execution. The Agent may record an observation or conclusion in messages without creating a task, update an existing task, or propose a new execution subject to confirmation. This supersedes the earlier Goal Signal trigger-to-execution policy. The implementation recommendations below support this boundary; exact protocols and lifecycle details remain open.

The current application source-change entry point (`apps/local/src/cli.ts`) screens relevant Goals and directly evaluates only independent Signals. Goal-owned Signals are evaluated by `makeGoalRuntime.reconcile` against the planning round's evidence after the Goal Agent submits its complete desired Signal set. Therefore the current Goal loop is source change → Goal planning → Signal reconciliation/evaluation → Signal Run → preparation/readiness/confirmation or automatic delegation → execution feedback → Goal planning. It is not two concurrent direct detection paths for the same Goal-owned Signal.

There is no persistent Goal Task in this implementation. `Task` is only the prepared instructions/input payload attached to a Run. The per-Signal source fingerprint prevents replay of an identical source snapshot, but does not merge equivalent work discovered across chats or exclude an existing pending/running execution. Reconciliation re-evaluates evidence even when Signal definitions are unchanged. Goal Agent history is reduced to custom events, and replacing the entire desired Signal list makes omission equivalent to deactivation. These are structural continuity gaps, beyond task prompt readability.

Proposed responsibility split:

- Goal Context is the durable, inspectable representation of the user's objective, current summary, native Agent messages, tasks, and associated Signals. GoalActor serializes commands and validates changes; Context itself is not an autonomous planner.
- Goal Agent is invoked to reason over this state, evidence, and recalled memory. It manages Goal Tasks and Goal-owned Signals through scoped tools. It does not own a second authoritative task store or overwrite execution facts.
- Goal Task tracks a concrete outcome across planning rounds and executions. Its business completion and an individual execution's outcome remain distinct. The existing prepared Task payload should be named explicitly as an execution input in the new design to avoid confusing these two responsibilities; exact naming is still a proposal.
- Goal-owned Signal monitors a condition and reports an occurrence with evidence to the Goal. The accepted routing is that this occurrence does not independently create an external execution. Goal planning decides whether to update existing work, propose an execution, or merely record the observation. Independent user-authored Signals retain their existing direct execution workflow; this change is scoped to Goal-owned Signals.
- Execution records own the frozen instructions/evidence revision, confirmation, delegation session, progress, and outcome. All Goal execution proposals use one admission path that checks current task/revision and active execution before preparation and dispatch. Runtime enforces confirmation; task CRUD tools cannot bypass it.

In this proposal, user input and relevant source changes still permit discovery of new work; Signals provide targeted continued monitoring, including after a task completes. Related source events and Signal occurrences should be coalesced into one Goal evaluation. Re-reading evidence, updating the Goal summary, and saving the same Signal must not count as new occurrences. A genuinely new occurrence may cause further work, but the Agent consults existing tasks and execution outcomes first. Stable occurrence identity and durable consumption are needed alongside semantic task matching.

Task and Signal mutations go through their owning actors and return confirmed persisted results to the Agent. Signal actors may remain under SignalsRoot: domain ownership by a Goal does not require actor-parent ownership. Tool calls/results belong in the native transcript; external execution notifications must be represented as explicit runtime evidence without inventing assistant tool-call/result pairs. During an Agent invocation, incoming changes are queued and stale revisions cannot overwrite newer task or execution state. Run outcomes are durably recorded before notifying the Goal and capturing outcome memory; memory is a recall index, not the execution-status authority.

The Signal-to-Goal routing and deletion policies are accepted. The implementation now exposes scoped task/Signal tools, stores full history separately from working Context messages, and enforces revision-bound confirmation and logical deletion. Task-to-task relationship modeling and old-data migration are explicitly excluded from this iteration.

Status: the first implementation now exists. It includes Goal actors, generated Signals under SignalsRoot, real Doubao delegation and recovery, public Goal conversations, a separate HTTP/SSE web client, user IM polling, pi 1.0.0 reasoning, and JSON/JSONL persistence. `pi-durable` 1.0.0 is installed for the next Goal Agent Session adapter step; the current wrapper still uses the compatible `pi-agent-core` runtime path. The “Verified starting point” below records the pre-implementation baseline. See `apps/local/README.md` for the implemented configuration and API, and current validation limitations.

### Signal timer objects (implemented)

Accepted scope: Signal supports a serializable object for constructing its timer, with one-time absolute timestamps and Cron schedules implemented using Effect. On restart, missed occurrences are coalesced into one overdue wake-up rather than replayed individually. This adds timer-driven Goal assessment to the previously accepted source/user/execution triggers.

Concrete configuration shape for implementation:

```ts
type SignalSchedule =
  { type: "once"; at: string } | { type: "cron"; expression: string; timeZone: string };

// Signal.schedule examples:
// { type: "once", at: "2026-10-01T20:00:00+08:00" }
// { type: "cron", expression: "0 20 * * *", timeZone: "Asia/Shanghai" }
```

Store this data object on the Signal; construct the Effect Schedule only at runtime. Validate `at` as an absolute timestamp with an explicit offset or UTC designation, and validate Cron expressions and time zones before saving. Natural-language conditions remain separate from `schedule`; an optional structured `notBefore` is a lower-bound guard, not an active timer. Agent Signal tools accept the same serializable configuration. Timer updates invalidate the old schedule revision; deletion stops scheduling. First-version scheduling does not expose arbitrary Effect combinators or fixed/spaced interval configurations.

The installed Effect version is `4.0.0-rc.117`. Its local source and a runtime probe confirm:

- `Schedule.duration(delay)` with `Effect.schedule` runs once after the delay. An absolute one-time deadline requires computing the remaining delay from a persisted timestamp, then checking the deadline again on delivery.
- `Schedule.spaced` waits after the previous action completes; `Schedule.fixed` aligns to a regular cadence and does not replay every missed interval. If the scheduled action merely enqueues an Actor command, completion means enqueue completion, not completion of downstream Agent work.
- `Schedule.cron(expression, timeZone)` supports calendar scheduling, and `Cron.next` computes the next occurrence. A probe at `2026-09-30T19:30:00+08:00` with `0 20 * * *` and `Asia/Shanghai` produced a 30-minute delay and a next occurrence of `2026-09-30T12:00:00.000Z`.
- `Effect.repeat` executes once immediately before consulting the schedule. `Effect.schedule` consults the schedule first. A one-shot zero-duration probe executed once with `schedule` and twice with `repeat`; use the former when the first invocation must wait.

Recommended design: represent temporal constraints separately from natural-language conditions. A `notBefore` constraint is a runtime gate on all occurrence paths, while a one-shot timestamp or cron rule can actively wake a Signal without new source data. These are distinct behaviors. Timers deliver commands to SignalActor, which validates active/deleted state, current schedule revision, due time, and occurrence identity before notifying GoalActor. Timers do not directly invoke the external Agent or bypass confirmation. Actor-scoped asynchronous waiting can use the existing `pipeToSelf` mechanism without blocking its mailbox; reconfiguration needs cancellation or revision checks for old timer messages.

Schedule is an in-process timing policy, not durable scheduling storage. Persist serializable timing configuration and progress/occurrences, reconstruct timers after restart, and make Goal notification consumption idempotent. Apply the accepted policy of one coalesced overdue wake-up on restart rather than replaying every missed interval. A completed one-time timer must not fire again after restart; recurring schedules resume at their next future occurrence after overdue handling. The runtime persists deadlines and occurrence delivery, invalidates stale timer revisions, and restores timers using Effect.

## Intent

Add Goals to the local Context and Signal system, explore functionality inspired by the user's reference to Meta Muse app, and validate the resulting behavior against the user's work IM. The exact reference product and the desired subset of its behavior have not yet been verified; the candidates below are proposals for this system, not claims about that product.

## Verified starting point

- Root Actors are registered in code. Public Contexts expose `path`, fixed `description`, `state`, and `messages`.
- Lark currently supplies account and mail Contexts. Its configuration parser accepts `/mail` as its child; there is no work IM ingestion implementation yet.
- The common Context processor publishes eligible source changes for evaluation, but the current gate and extraction pipeline decode email data and use email-specific prompts.
- Signals are user-authored standing definitions. The Agent currently returns triggered Signal slugs, and every Trigger command creates a new Signal Run. The wider design's choice between updating an existing run and creating a new one is not implemented.
- A Signal Run prints proposed delegation. Readiness, interactive confirmation, execution, and result collection are not implemented in this slice.
- Memory capture and compact search followed by selective expansion are implemented. Source activity is captured after a Signal triggers; ordinary Context updates do not automatically write memory.
- Application Contexts and runs currently remain in memory. Durable Actor primitives exist, but persistence of these domain Contexts has not been wired into the local slice.

Sources: `apps/local/src/config.ts`, `processing.ts`, `detect.ts`, `signals.ts`, `packages/integrations/src/lark/index.ts`, and the existing core design.

## Design areas covered

1. **Goal meaning and ownership.** Define what desired outcome a Goal represents, how it differs from a standing Signal, and who may create or change it. Determine whether a Goal is a Context after its responsibilities are clear.
2. **Evidence and progress.** Determine how activity across chats, emails, and existing runs affects a Goal, how conclusions cite their sources, and what constitutes completion or a blocker.
3. **Work IM understanding.** Evaluate chat context rather than isolated messages. Decide how to accumulate enough context, when to reevaluate, and how to distinguish an update to existing work from newly discovered work.
4. **Continued pursuit.** Determine whether and when a Goal leads to proposed actions or delegation, how results affect progress, and whether follow-up is needed without a new source message. This does not reopen the deferred steer design by default.
5. **User feedback and validation.** Make the evidence and resulting proposals inspectable, and let the user correct irrelevant, repeated, or mistaken conclusions. Define the first useful work IM validation outcome before committing to an interface.
6. **Continuity.** Decide what must survive restart to make multi-day Goal tracking meaningful, including observed source position, progress, and associations with existing work.

The accepted decisions for these areas are recorded below. Existing constraints, including excluding Signal Run messages from generic Signal discovery, remain in force unless explicitly revised.

## Accepted distinction

A Goal describes a desired outcome or ongoing responsibility pursued across multiple conversations, observations, and actions. A Signal describes when specified work should be delegated. A finite Goal has completion criteria; an ongoing Goal has no automatic completion point and remains active until the user ends it.

For example, a finite Goal could be “complete the review of project X this week.” New IM discussions may provide evidence of outstanding decisions, blockers, or completion. The system relates those changes to the same Goal and identifies useful next steps through the accepted auto execution flow. The user's actual first validation Goal is the ongoing responsibility recorded below.

## Accepted creation policy

The first version uses explicitly user-created Goals through YAML configuration. An Agent uses work IM evidence to update progress, identify blockers, and propose next steps for those Goals. Automatic discovery and adoption of new Goals from conversations are deferred. Mentioning a possible future task in a chat does not itself create a Goal.

## Accepted relationship boundary

Signals may be related to Goals or operate independently. Adding Goals does not require every Signal to belong to a Goal. Both user-authored Signals and Goal-derived Signals express a trigger condition, task, target Agent, and execution mode.

## Accepted Goal-driven Signal generation

Keep Goals distinct from Signals. An Agent uses a Goal and the current state to generate and adjust Signals that advance that Goal. As circumstances change, it can reconsider which Signals are useful and need adjustment. The user preferred this direction over making per-run relevance classification the primary relationship between Goals and Signals; that earlier proposal is not adopted.

This brings automatic Signal generation and adjustment into scope specifically for explicitly user-created Goals. General discovery of new Goals remains deferred. The accepted association, retirement, persistence, activation, and evaluation policies are recorded below; exact protocols remain implementation work.

## Accepted first-version activation and execution policy

The first version supports `auto` for Goal-derived Signals. Generated or adjusted Signals participate in normal evaluation, and an executable triggering proceeds to delegation automatically. A confirmation workflow for Goal-derived Signals is outside this first version. This narrows the earlier configurable-mode proposal; it does not remove the existing mode field from independently user-authored Signals. The current print-only implementation is unchanged until implementation begins.

## Accepted Goal evaluation triggers

Evaluate a Goal when it is created, using available Contexts and recalled memory to derive initial Signals. Subsequently, relevant source Context state changes can cause reevaluation: Jev screens state changes for relevance before an Agent reads the Contexts, updates progress, and adjusts Signals. An associated execution result is a third evaluation trigger: the Goal records the result and invokes an Agent to assess progress and whether its Signals need adjustment. This is explicit feedback to the interested Goal, not generic Signal discovery from run messages. Timer-driven follow-up without new activity remains undecided.

## Accepted execution and message-stream scope

This iteration must perform real Agent delegation instead of stopping at printed proposals. It must also provide a message stream for each Goal. The current implementation still prints proposals; this section specifies the next implementation's scope.

Execution progress and results from a Goal-derived Signal are delivered directly to its originating Goal through business notifications and recorded in that Goal's message stream. Execution results also cause Goal reevaluation. Users can converse with a Goal through this stream as described below. Real execution does not by itself reopen the deferred steer behavior or permit arbitrary Signal-to-Signal triggering. Agent selection, result collection, and the stream's delivery interface need to be specified.

## Accepted Goal message-stream semantics

The first version supports a continuous, bidirectional Goal conversation containing user input, Agent progress explanations, planning decisions, and execution outcome summaries, with references to source Contexts and associated runs. It uses the existing Context `messages` concept, with current progress in `state`. Detailed execution records remain available through their execution Contexts.

A user message is delivered to the GoalActor, recorded in the conversation, and triggers reevaluation. For example, the user can add a constraint or change a priority. Together with creation, relevant Context changes, and execution results, user messages form the four accepted evaluation triggers. Already-running delegated work finishes using its original input; the new message informs subsequent planning and does not introduce steer behavior. The accepted interaction interface is the replaceable local web client and public backend API described below.

## Actor architecture

The ownership boundary is accepted: Goals are managed by GoalActors, while all SignalActors remain under SignalsRoot. GoalActors watch associated Signals, receive business notifications of progress and results, and reevaluate after execution results. Detailed notification protocols remain to be specified.

Each Goal would be a Context owned by a GoalActor, for example `/goals/{slug}`, exposing the established `state` and `messages` representation. A Goals root would create and supervise these actors from explicit configuration. The GoalActor would own its desired outcome, progress, blockers, and recorded decisions, and coordinate planning through ordinary Agent invocations. An Actor's lifetime is independent of a model invocation; a Goal need not keep a model process running continuously.

Source integrations would continue to maintain their own Context actors. Common state-change processing and Jev screening would route relevant state changes into Goal evaluation and the existing Signal evaluation path. A chat remains a coherent Context, rather than each IM message directly becoming a Signal. Goal planning reads available Contexts and memory, then returns proposed progress updates and Signal changes to the GoalActor for application.

SignalsRoot creates and supervises all SignalActors, including those generated to advance Goals. A GoalActor requests Signal creation through SignalsRoot and watches associated Signal actors; it does not spawn them as its own children. User-authored and Goal-derived Signals therefore share the existing management and execution path. The previously proposed hierarchy with SignalActors under GoalActors is not adopted.

The current runtime's `ActorContext.watch` reports actor termination through `Terminated`; it does not publish domain state changes, run progress, or execution results. A separate business subscription delivers Signal and execution updates to the originating GoalActor, while retaining existing lifecycle-watch semantics. A triggered Signal creates its own run and delegates execution through a Delegation Context. Progress and outcomes return to that run and notify the originating GoalActor to update its message stream. An execution result also triggers Agent reevaluation of Goal progress and possible Signal adjustments. This feedback route does not make run messages eligible for arbitrary Signal discovery.

Long Agent operations would run asynchronously, with completion returned through `pipeToSelf` or domain messages. Goal state changes remain serialized through its mailbox. Incoming Commands and the outward-visible `messages` history remain distinct: the actor records meaningful updates and evidence references rather than exposing its raw mailbox. User conversation is an additional input. Messages arriving during an Agent operation can be retained for subsequent evaluation without introducing steer behavior.

The Goal message stream exposes its public history through the replaceable web client and backend API. Detailed source conversations stay in source Contexts, and detailed execution records stay in their execution Contexts; Goal messages summarize relevant progress and link to that evidence. This iteration uses local `state.json` and `messages.jsonl` files for Actor persistence as specified below. The existing SQLite Actor backend remains available, but is not the selected storage for this iteration.

Stopping a GoalActor does not structurally stop associated SignalActors, because they have a different parent. Goal completion explicitly deactivates its generated Signals through SignalsRoot, as specified below. Goal paths and subscription details remain to be specified.

## Accepted initial evaluation of Goal-derived Signals

Goal planning can discover actionable work in the current Contexts that it has just read. A newly derived Signal is immediately evaluated against that planning input through the normal Signal evaluation and execution path. If its condition is satisfied and the run is executable, it proceeds automatically without waiting for another external change. This is a bounded exception to the existing future-facing default for new Signals, not a scan of all historical Contexts. Avoiding repeated work across planning rounds remains a lifecycle topic to resolve; creation alone is not proof that a Signal has triggered.

## Accepted first-version Signal association

All Signals remain managed by SignalsRoot, but the first version does not reuse Signals across Goals. Each generated Signal serves only its originating Goal, and its progress and results return directly to that Goal. Independently user-configured Signals have no associated Goal. During later planning rounds, a Goal adjusts its own existing Signals rather than creating duplicate definitions for the same ongoing work.

This replaces the previously accepted cross-Goal reuse proposal. Reuse would require additional rules for result relevance, authority to change a shared definition, and retirement. The first version chooses a direct association instead; an extra Jev pass to route shared outcomes is therefore unnecessary. Jev screening of external Context changes remains part of the design.

## Accepted Signals policy after Goal completion

A completed Goal requests deactivation of its generated Signals through SignalsRoot, preventing further runs. Already-started delegations finish and report their results to the Goal message stream. Deactivation stops new triggering; it does not require immediately stopping the SignalActor and its executing descendants. How completion is determined remains a separate decision. Whether later information can reopen a completed Goal has not been decided.

## Accepted Goal completion decision

Completion criteria are optional. For a finite Goal, the user describes the desired outcome and completion criteria; the evaluating Agent automatically determines completion from those criteria and available Context and execution-result evidence. It records its conclusion and supporting references in the Goal message stream, then invokes the accepted deactivation policy. A successful delegation alone does not establish that the whole Goal has been achieved. Goals without completion criteria remain active until the user ends them.

Accepted domain terms will be added to `CONTEXT.md` as they are resolved. Architectural decisions will be recorded only when an actual trade-off has been decided.

## Accepted reasoning-model and execution separation

Add named model configurations under `config.models` for Goal-internal reasoning. Each configured model has a `name`. Goal assessment, planning, and completion decisions use a configured model rather than requiring the previously proposed Codex planning backend. The Goal's resulting delegated work is handled by Doubao, with progress and results returned through the existing Signal Run and Goal feedback design.

The implemented code still uses Codex CLI for current email Signal evaluation and Context description generation, and only prints delegation targets. These facts describe the starting point, not the agreed new Goal reasoning backend. The exact model API protocols, tool access, model-selection fields, and Doubao execution adapter contract remain to be specified.

## Accepted global Goal model selection

Goal reasoning-model selection belongs to global `config`; individual Goal definitions do not select a model. `config.models` holds named model entries, and `config.goals.model` references the entry used by all Goals. The `config.goals` object leaves room for future global Goal settings. This replaces both the proposed per-Goal `model` reference and the flat `config.goalModel` spelling. Endpoint and credential settings belong to the named model entry; their exact schema remains to be specified.

## Accepted use of pi core

Use pi core for Goal-internal Agent operation instead of implementing an OpenAI-compatible-only loop. The integration uses pi-mono's Agent core (`@earendil-works/pi-agent-core` 1.0.0) and its `@earendil-works/pi-ai` 1.0.0 model layer. The durable session package (`@earendil-works/pi-durable` 1.0.0) is installed for the Goal Agent Session adapter. Goal-derived task execution remains delegated to Doubao through Signal Runs.

The upstream Agent core documentation describes model/tool-call loops, custom application messages, and streaming message and tool-execution events. Its model layer supplies provider adaptation. These capabilities fit the existing Goal conversation and named-model configuration design. This choice does not reopen steer behavior.

Reference: [pi Agent core documentation](https://github.com/badlogic/pi-mono/blob/main/packages/agent/README.md). At inspection time the main-branch documentation uses the `@earendil-works` package scope, while npm also exposes the older `@mariozechner/pi-agent-core` and `@mariozechner/pi-ai` releases; implementation must use documentation matching the chosen release rather than mix the APIs.

## Proposed Goal integration boundary

GoalActor remains the owner of Goal state and durable public messages. The pi Agent handles reasoning, tool execution, and streaming within an Actor-scoped operation; asynchronous callbacks must return state-changing outcomes through the actor protocol rather than mutate Goal state directly. The exact mapping from pi messages/events to the Goal conversation remains to be specified.

## Accepted Goal Agent tool capabilities

The first tool set lets the Goal's pi Agent read Contexts, search and selectively expand memory, create or adjust its Goal-derived Signals through SignalsRoot, and report progress or completion to GoalActor. Writes are handled by the corresponding Actor rather than direct mutation of another Actor's state. Actual delegated work follows the Signal Run path to Doubao. These capabilities are accepted; exact tool names and input/output schemas remain to be specified.

## Accepted replaceable Goal conversation interface

The first version includes a minimal local web interface: a Goal list with current status, a selected Goal's conversation and task progress/results, and an input for user messages. Goal creation remains YAML-based as already agreed.

The user expects to replace this UI freely, so it must be independently implemented as a replaceable client. Goal, Signal, planning, and delegation behavior belong to the backend and must not depend on a browser page being open. The frontend consumes a public application API and message stream, rather than importing Actor, Effect, or pi runtime implementations or accessing persistence files directly. Replacing the frontend must not require rewriting those business modules. Backend operations should remain usable by another client through the same contract.

An HTTP API with server-sent events is the current implementation recommendation for reads, user messages, and live updates. Exact transport schemas, frontend technology, and package layout remain to be specified; the independence boundary is accepted.

## Accepted work IM observation scope

Validate against all IM conversations accessible to the configured work account, rather than a manually selected set of chat IDs. The `/lark/im` integration discovers and observes those conversations. Each chat forms a Context, and evaluation uses its conversation context rather than treating each incoming message as an independent Signal. Goal and Signal relevance is determined by the system using the agreed Jev and Agent evaluation flow. The earlier chat-ID allowlist proposal is not adopted.

This decision defines conversation coverage. Initial recent-history loading and incremental updates are accepted below; the exact history window and event-ingestion mechanics remain to be specified.

## Accepted initial IM context

Load recent conversation history for each discovered chat as its initial Context, then maintain the conversation incrementally from new messages. Initial history supplies interpretation context rather than automatically treating every historical message as a new event. A Goal's accepted initial planning and immediate evaluation of newly derived Signals can still use these available Contexts. The exact history window and access to older messages are not yet specified.

## Accepted local-file persistence

Use local files for the first Goal iteration's business Actor persistence:

```text
~/.aster/actors/{path}/state.json
~/.aster/actors/{path}/messages.jsonl
```

`state.json` stores current structured state. `messages.jsonl` stores ordered domain Messages as newline-delimited JSON, one complete Message per line, appended in order. These are domain history records, not persisted mailbox Commands. Each Actor remains responsible for serializing its writes. The JSONL format replaces the initially proposed `messages.json` array.

The proposed path mapping uses the public Context path under the storage root, for example `/goals/project-review` maps to `~/.aster/actors/goals/project-review/`. Runtime-only actor hierarchy details should not leak into public paths; the precise mapping and file-recovery mechanics will be addressed during implementation.

Persist Goal state and messages, Goal-derived Signal definitions and activation state, and Signal Run and Delegation records through this file-based approach. A restart can recover progress and known execution state; this does not itself establish that an external Agent task can be resumed. The existing SQLite Actor backend remains available, but the SQLite proposal for this iteration is replaced by these local files. The agentmemory service's own persistent storage is a separate concern.

## First real validation Goal

The user is the frontend TL for Knowledge Engine and wants to continuously follow Knowledge Engine project progress in that role. This is the selected validation scenario. It expresses an ongoing responsibility rather than a one-off deliverable with a natural completion point. Its primary delegated output is analysis of important project changes, as agreed below.

## Accepted ongoing Goals

Goals also represent ongoing responsibilities. Completion criteria are optional: a Goal without completion criteria remains active until the user explicitly ends it. Completing an individual delegation or observing the completion of a project updates progress and the message stream without automatically completing the ongoing Goal. Finite Goals retain the accepted evidence-based automatic completion policy.

## Accepted TL Goal delegated output

Important project changes are occasions to delegate a concise analysis to Doubao, delivered in the Goal conversation. The analysis explains what changed, its impact on frontend delivery, dependencies or blockers, matters requiring the TL's attention or intervention, recommended next steps, and references to the underlying messages or documents. This is the first version's primary output for the validation Goal. The Goal remains an ongoing responsibility; delivery of a single analysis does not complete it.

## Implementation follow-through

The next implementation should connect the agreed flow end to end: initialize recent history for all accessible work chats; evaluate relevant changes against the ongoing Goal; derive or adjust its own Signals; evaluate and execute eligible runs through Doubao; and return progress and evidence-backed analyses to the Goal conversation. User replies must feed the next planning round, and restarting the local application must recover persisted Goal, Signal, and execution records.

Implementation details to settle against the actual libraries and integrations include the pi release and provider configuration mapping, Actor tool and notification schemas, the file-store recovery mechanism, the Doubao submission and result-query protocol, Lark ingress and history defaults, and the independent web API/stream schema. These do not imply additional product features. Timer-driven reevaluation is now implemented as described above. Automatic reopening of a completed finite Goal remains outside scope.

## First implementation verification

The monorepo suite passed 61 tests (Actor 28, Context 6, memory 6, local application 21), covering Goal-to-Signal-to-delegation feedback, completion/deactivation, messages arriving during reasoning, restart polling without resubmission, IM pagination/bootstrap/deduplication, HTTP input routing and origin checks, and file recovery. Goal model tool calls were verified against MiniMax CN; Context screening and execution readiness were verified against the configured Laya System One service. A bounded synthetic frontend-risk task was submitted to DoubaoWork and its final response collected successfully. Lark user chat/message response shapes were verified live without publishing chat contents.

The independent frontend builds successfully. Visual/interaction inspection remains unverified because IAB was unavailable and Chrome/inventory computer-use calls timed out; Image Gen also returned 404, so there is no generated concept/screenshot comparison. Existing user-owned Signals and memory processes were left running; the new config takes effect when that process is restarted.

## State-driven screening and rolling chat summaries (design update)

System One screening is triggered by changes to an Actor's public Context state, not by arbitrary Context updates. Message appends and compaction alone do not trigger screening. IM chats first roll a new-message batch into their previous summary; the summary is part of state. Only after storing that summary may covered messages be removed, retaining any messages received in the meantime. Changed summary state then enters Goal relevance screening and Goal reasoning.

This updates the external Context-change trigger. Explicit user-to-Goal commands and execution-result feedback remain separately described inputs; they are not generic Message-change subscriptions. See [IM summary design](im-summary-design.md) for the current discussion and pending routing decisions.

## Effect lifecycle and history safeguards (2026-09-30)

A behavior restart preserves its child Actors. Goal, Signal, and IM restoration reuse existing children; a recovering Run reattaches to its Delegation and receives the recorded result without submitting another external task. Repeated initialization does not replace an existing Run. Signal configuration accepts definition fields only, so a stale configuration snapshot cannot overwrite current deadlines or delivery progress. Timer evidence excludes occurrence history and other delivery bookkeeping.

Each Goal assessment has a unique generation. Agent tool, transcript, compaction, and completion messages carry that generation; a restarted or ended assessment cannot mutate the current one. Promise callbacks bridge through the captured Effect context and the assessment's cancellation signal. Direct application tool requests remain possible without an assessment generation. Expected validation errors use `GoalToolError`; infrastructure defects propagate to Actor supervision.

`GoalHistory.append`, `read`, and `count` return Effects. The file adapter uses asynchronous I/O and a per-Goal semaphore, awaiting durable writes before releasing ownership even during interruption. On first access it rebuilds a sequence-to-byte-offset index from `~/.aster/goals/{slug}/history.jsonl`; later page reads seek directly to the selected records. The index retains offsets rather than message bodies and is rebuilt after an I/O failure. Incomplete trailing writes are truncated during recovery; committed corruption remains an error. This retains the existing JSONL format and assumes the application's single-writer store lock.

Codex RPC cancellation stops local waiting and prevents subsequent RPC stages. A request already sent can still have an unknown external outcome and is never automatically replayed because of cancellation. Mail response decoding reports a typed failure to the polling retry path instead of restarting the mailbox Actor.

## Implemented structured Timeline API

`GetGoalTimeline` projects durable business inputs and Evaluation records, without reading native transcript text. `GoalInput.payload` distinguishes UserInput, PersonalMessage, GoalIntent, SignalOccurrence, ExecutionFeedback and Startup. Input acceptance commits its stable identity with the corresponding receipt or domain update before acknowledgement. GoalHistory is an idempotent model-view projection keyed by input ID. Each admitted Evaluation stores its ordered input IDs; its history boundary ends at the last selected input. Admission bounds a batch by the Goal context budget and leaves overflow pending.

The API returns groups, unassigned pending inputs, total group count and a stable ordinal cursor. Task outputs retain their applied titles, operations and reserved Run paths. Signal outputs reflect durable delivery receipts, including unknown/rejected outcomes. A result marked ignored or no_change cannot mutate Tasks or Signals or complete the Goal. An external execution feedback input references its Run, Task and originating Evaluation; applying a plan is distinct from completing that Run.

The web Timeline reads this API, groups inputs/conclusion/outputs, exposes Intent source/score/rationale, and refreshes every loaded page after invalidation or reconnect because delivery status can change in older groups. Notes filters only user inputs. Raw History remains in Inspector; Agent run details currently expose stable session/request identifiers, not a native transcript reader. Composer retries retain their request ID while the draft remains mounted. Timeline pending-input pagination and native transcript inspection remain separate follow-up work. No existing-data migration or history-text fallback is implemented.

`RetryGoalSignal` authorizes replay of a single unknown Signal delivery using its frozen operation ID and the last observed attempt count. GoalActor commits the retry request and acknowledgement receipt before dispatch. The attempt count never resets; each new operator authorization can extend the automatic three-attempt ceiling by one. Replaying the same authorization returns its stored receipt, while changed payload reuse or a stale observed attempt count conflicts. Known receiver rejection requires a corrected plan, not replay with an altered revision. Browser retry identity survives in-app navigation and SSE reconnect for the lifetime of the client registry.
