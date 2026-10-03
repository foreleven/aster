# Unified external Agent tasks

## Pi execution authority (implemented, 2026-10-03)

The Pi executor uses the native read tool with a host-owned `SandboxManager`. The initial policy exposes only two immutable virtual files, `/task/input.md` and `/task/instructions.md`, reconstructed from the admitted Task. It has no host filesystem, process, network or credential capability. File mutations and shell requests fail at the ExecutionEnv boundary. This permits evidence analysis without granting arbitrary host execution; installed native tool code remains trusted infrastructure.

Admission persists the environment policy ID with the model and tool catalogue. Pending recovery and each environment acquisition verify the same policy before tool execution. Result replay does not reacquire the environment or invoke the model. Task confirmation grants no additional capabilities; external writeback still requires an explicit action and its own domain policy or approval. A future execution backend must enforce that authority rather than forwarding it as prompt text.

## Doubao task quality and continuity (implemented, 2026-09-30)

Requested improvements: make submitted task text readable; introduce configurable default instructions under `agents.doubao`, including read-only behavior and confirmation for actions such as sending messages; and prevent repeated similar work by improving task context, memory recall, and retention of execution outcomes. Permission boundaries and Goal task continuity are recorded below and in the Goal design; exact configuration schema and rendering format remain implementation details.

Accepted permission boundary: default read-only work may create reports and drafts in the task's dedicated local workspace. Modifying existing files, updating external documents, sending messages, and comparable writes require individual confirmation. Apply this boundary during both Task preparation and construction of the Doubao default prompt. Implemented in preparation, readable confirmation content, and executor prompt construction.

Goal-owned Signal occurrences return to the Goal Agent for assessment and may produce only an observation. When the Goal proposes an execution, the default is `confirm`, requiring user confirmation of the prepared Task before delegation. The agreed Goal behavior, including flat task management, native Agent messages, full local history with a bounded summary/messages working context, and logical deletion, is recorded in [Goals and work IM exploration](goals-design.md). Confirming a Task does not automatically waive the separate confirmation requirement for external writes. The user will manually remove old data for fresh validation; migration is out of scope.

Before this change, `taskPrompt` submitted a short preamble followed by compact `JSON.stringify(task)`. Doubao uses `AskOnRisk`, but there is no configurable per-executor default prompt. Task preparation exposes `memory_search`, `memory_expand`, and Context discovery tools and instructs the model to use relevant evidence; it does not enforce that recall or prior-work inspection occurs before submitting a prepared Task. This does not establish whether any particular live invocation actually read memory.

Execution results are persisted in Delegation and Signal Run history and reported to the Goal, but there is no separate terminal-result memory capture. Signal Run capture uses the same session ID as the run path and is deduplicated after its first capture, so subsequent completion updates do not refresh that memory. Capture failures are logged without durable retry. Existing Signal deduplication hashes the source Context per Signal; changed source snapshots or distinct Signals can still lead to semantically similar work. These are code-level gaps, not a verified causal trace of a specific duplicate task.

Implementation now renders Chinese Markdown task sections and applies top-level `agents.doubao.prompt` during preparation and submission. Preparation performs prior-memory search and expands available candidates before generating instructions, includes Goal/current-work evidence, and exposes Context tools for further inspection. Trigger and terminal-outcome captures use distinct identities; pending captures and completion IDs survive restart, and failed writes retry. The default prompt works with the existing Doubao `AskOnRisk` approval bridge; it is an executor instruction, not a new operating-system sandbox. Controlled integration tests cover prompt/session behavior; live external delegation was not exercised for this change.

The gap descriptions and concrete run traces above/below document the pre-change investigation.

### Inspection of current Goal tasks

The local `knowledge-engine` Goal asks for concise analysis of project progress and frontend implications. Its generated Signal `knowledge-engine--ke-adapter-2-23-1-marketing-agent-impact` produced three persisted runs. In Beijing time, run `67766362-8027-4764-ad59-fa40295c70ce` triggered at 2026-09-29 23:58:15 from the product/engineering chat; run `79ae1795-96c1-47d9-95d4-bd704fa66fb8` triggered about six seconds later from the stability-weekly chat. Both prepared tasks address the same version-adaptation issue. Different source paths bypass the per-Signal source fingerprint check, and the code does not exclude work already preparing or running for the same business issue.

Run `7c52bf8d-cdf5-47c2-a711-03f3d6368a87` triggered at 2026-09-30 10:25:25 after the product/engineering source gained another message. Its saved `when` is `2026-09-30T20:00:00+08:00`, demonstrating that this string did not enforce a not-before time. Signal conditions are model-interpreted text, not an enforced schedule. The run later recorded an uncertain outcome after external cancellation.

Goal reconciliation evaluates its desired Signals against each cited source Context, even when those Signals are unchanged. Generated Signals are assigned Doubao and auto mode in code. Task preparation then expands the selected Signal into instructions and evidence before System One readiness and external submission. The first task explicitly instructs contacting colleagues and arranging a sync; its persisted executor result claims that a group message was sent. The external send has not been independently verified here. This shows authorization expansion in the generated task despite the Goal's analysis-oriented wording.

The later task's sources reference both earlier execution records, so the evidence does not support saying all task construction lacked prior context. References alone do not prove which tools were called. The confirmed gap is lack of an enforced comparison against ongoing/completed work and lack of result-memory refresh, rather than simply absence of memory tools.

Status: implemented in core, Actor runtime, integrations and the local application; provider limitations and validation are documented below.

## Accepted responsibilities

The internal Agent builds a concrete Task after a Signal triggers, using the Signal, relevant Contexts and memory when needed. Signal extraction and fixed Context description generation move from Codex to the internal Agent.

Codex and Doubao are external Task executors behind a common delegation contract. Their adapters translate the Task into platform calls and collect execution progress/results. They do not independently interpret a Signal or decide the business task. Signal remains the standing definition; Task describes the work for one triggering; Delegation represents its execution.

## Accepted Task content

The first Task model contains `instructions` and `input`. Instructions describe concrete work, constraints and expected output. Input carries the material needed for execution and its source references, assembled by the internal Agent from relevant Contexts and memory. External executors do not need access to this system's Context or memory interfaces in the first version.

Executor selection belongs to delegation configuration. Platform session IDs, polling and completion markers belong to adapters rather than the business Task content. The accepted schema is:

```ts
interface Task {
  instructions: string;
  input: Array<{
    content: string;
    sources: string[];
  }>;
}
```

Each input item contains prepared facts or necessary original text, with source references such as Context paths, message URLs or memory references. References support attribution; external executors need not dereference them. Task instructions and input evidence remain distinct.

## Accepted preparation and execution order

A Signal triggering creates its Run Context. The internal Agent then constructs a Task and records it in the Run's ordered messages. System One evaluates execution readiness using the Signal, the prepared Task and executor capabilities. An executable Run in auto mode proceeds to delegation; confirm mode waits for the user's confirmation of that concrete Task. Delegation executes that same Task.

Task is recorded as a Run message in the first version, not managed by a separate Task Actor. This replaces the previous readiness check before Run creation. Failed preparation or readiness is recorded and stops automatic advancement without external execution.

## Accepted external executor contract

All external executors expose `submit(task)`, `status(session)`, `resume(session)`, `wait(session)` and `respond(session, request, response)`. Submit returns an external session handle, resume reconnects to or continues that same session, and wait returns an execution status snapshot, including TaskResult on completion or pending requests when input is needed. Methods are asynchronous; transport errors are recorded by the owning Actor.

The handle explicitly includes the external Agent's real `sessionId`, optional `runId`, and optional adapter-owned metadata. Delegation Context persists the executor identity and handle. A local process ID or internal Delegation ID is not a substitute for the external session ID. Output-file locations and completion markers remain adapter details, not Task content.

## Accepted session recovery

On application restart, DelegationActor checks the original external session status, resumes only when needed, and then waits for its result. The adapter reconnects if the session is still running, continues execution if interrupted, and returns the existing result if already completed. Recovery means continuing the task in its external session, not merely querying results. The adapter must not repeat completed work or automatically create a replacement session.

If the external session is missing or cannot be resumed, record a recovery failure while retaining the Task and sessionId. Do not create another session automatically. Provider recovery constraints are documented below.

## Accepted result contract

The first successful TaskResult is `{ text: string }`, containing the final answer extracted by the adapter. Core records it in Delegation and Signal Run messages and reports it to the associated Goal when present. Execution failures are recorded separately from successful results. The first version does not standardize external conversation transcripts or intermediate progress events.

## Accepted internal model selection

`config.agent.model` references a named entry in `config.models` and selects the internal Agent model for Signal extraction, fixed Context description generation and Task preparation. Goal reasoning keeps `config.goals.model`; Lark chat summarization keeps its integration-owned `summary.model` selection.

```yaml
config:
  agent:
    model: internal-model
```

## Accepted preparation failure handling

If Task preparation fails or System One rejects readiness, record the reason in Signal Run messages and stop automatic advancement for that run. Do not call the external executor or enter an automatic retry loop in the first version.

## External status before recovery

External execution can outlive the Aster process. Recovery must inspect the original external execution before attempting to continue it: wait if still running, collect the result if completed, and resume only if interrupted. The user supplied the run-specific Doubao query `doubao sessions status <sessionId> --run <runId>`. The adapter therefore needs to retain the external run ID alongside the session ID when available, rather than treating application-level running status as task status.

After the user upgraded the CLI, `sessions status` is recognized. The supplied session/run lookup returns “Doubao conversation was not found”, including with explicit `--app work`; no live run status has been verified. Inspection of the installed CLI shows statuses `running`, `completed`, `failed`, `cancelled`, `waiting_input` and `unknown`, with `conversationId`, `runId` and a final `reply.text` only when completed. The CLI also provides `sessions wait`; its timeout does not cancel the external task. Application-level `status` is not execution status. External input requests use the internal approval queue described below.

## Accepted external input handling

When an external executor reports `waiting_input`, record its question or input request in Signal Run messages and wait for the user. The internal Agent must not answer on the user's behalf. Retain the original session/run identifiers. This is a waiting state, not a successful TaskResult or a failed task.

## Accepted status contract

The common executor interface includes `status(session)`, returning a state of `running`, `completed`, `waiting_input`, `failed`, `cancelled` or `unknown`, with the applicable result, input request or error information. Each adapter maps its platform's state to this contract.

Recovery checks status first. Running work is awaited; completed work yields its result; input requests are recorded for the user. Resume is attempted only when interruption has been established. Unknown status does not authorize continuing or resubmitting. A failed status may carry `resumable: true` when the adapter has established interruption. Cancellation and unknown state do not authorize automatic resumption.

## Accepted centralized approval handling

The system provides a unified internal approval queue. Requests are handled internally and the resulting decision or supplied input is delivered back to the Actor responsible for the invocation. The responsible Actor then forwards the response to the matching external session/run through its adapter and continues tracking execution. Users are not required to switch to the external application's UI as the primary flow.

The same queue handles both pre-execution confirmation for Signals configured with `mode: confirm` and requests for permission or missing information during external execution. The request type is retained. Pre-execution decisions return to SignalRunActor; execution-time responses return to DelegationActor. These owning Actors decide how work continues.

The queue owns approval request handling and result routing; the invocation Actor owns Agent execution. DelegationActor owns external execution; approval IDs and destination paths correlate delivery through the shared queue. External `waiting_input` can represent a question or a permission request, so the queue must retain that distinction rather than treating every request as a yes/no decision.

## Accepted approval queue Context

A single persistent `/approvals` Context is owned by ApprovalQueueActor. Its state contains current approval entries (awaiting user input, resolved awaiting delivery acknowledgement, acknowledged); its messages record requests, user responses and acknowledgements. Each entry retains its unique request ID, normalized absolute destination Actor path and related Run/Delegation paths. Recovery loads the queue and continues delivery of resolved entries not yet acknowledged.

Approval activity does not trigger generic System One discovery. The existing `defineContext` option `signalSource` is opt-in; the approval Context leaves it false. `makeContextProcessor` gates both Goal relevance evaluation and Signal screening through this option, stateChanged and per-update evaluate. The Goal runtime's evidence evaluation also skips Contexts without signalSource. Approval changes still persist and notify subscribers. Delivery of an approval response is an explicit Actor command, not a new Signal trigger.

## Implementation scope

External Task executions are managed by DelegationActor. Internal pi reasoning stays in the existing business workflow and uses `pipeToSelf` for Task preparation; it is not modeled as a second external execution Actor. A submission interrupted before its handle is durably recorded becomes uncertain and is not automatically submitted again. Task preparation failure or failed readiness stays recorded without an automatic retry loop.

Approval request IDs correlate durable responses. The receiver persists its response before acknowledging the queue. External response submission is tracked separately as received/sending/sent/uncertain; an ambiguous send is retained for inspection, not blindly repeated. The local API provides GET /api/approvals and POST /api/approvals/respond; the web client presents confirmations, permissions and questions and links to their execution records.

Signal recovery re-creates actors for pending confirmations and executions. Legacy Run records without a prepared Task are marked for manual inspection instead of inventing a Task and re-submitting it.

## CLI capability inspection before this change

Local CLI help confirms `codex exec resume <SESSION_ID> [PROMPT]` and JSONL event output. The existing Codex runner uses `--ephemeral`, which must be removed for persistent resumable tasks. Doubao help exposes `sessions read <conversation-id>` and `sessions send <conversation-id> <message>`; continuing in the same conversation is available. Detecting running/interrupted/completed states and implementing safe reconnection still require verification. These help checks did not execute any external task.

## Actor reference inspection before this change

Current ActorRef contains runtime tell/ask methods and an incarnation ID. ActorRefImpl closes over a live ActorCell; ActorSystem indexes cells by incarnation and its private find also checks reference identity. ReplyTo validates a live reference, not a serialized address. Ask creates an ephemeral deferred reply target. A newly spawned ActorCell receives a new incarnation, so persisting an old ref does not reconstruct a usable reply target after process restart. Supervision restart within the same cell differs from a process restart.

ContextRegistry restores public records, not running actors or a Context-path-to-ActorRef registry. Before this change ActorSystem exposed no public path resolver. Existing Signal recovery explicitly spawns Run/Delegation actors again and passes fresh replyTo references. The implemented recovery filter now includes approval-waiting states.

Accepted direction: retain a serializable stable destination and approval request ID, resolve/rebind to the recovered invocation Actor, retain resolved approval results until acknowledged, and deduplicate receipt by approval ID. Actor runtime paths and public Context paths are not interchangeable: a Delegation has a public /delegations/{id} Context while its runtime Actor is a child of the Signal Run. Recovery must reconstruct the Actor, not merely load its Context. The implemented durable address is a normalized runtime path; queue delivery is acknowledged by request ID and target path.

## Akka selection reference and accepted local API

Akka Classic exposes `context.actorSelection(path)` (rather than `context.select`) and `ActorSelection.resolveOne` to obtain a current ActorRef. Absolute and relative paths select locations; ActorRef identifies a specific incarnation. Selection neither creates a missing Actor nor supplies durable delivery. Official reference: https://doc.akka.io/libraries/akka-core/current/actors.html#identifying-actors-via-actor-selection .

Accepted for this local runtime: `context.select(path)` and `system.select(path)` return a path-based selection that can resolve the currently registered ActorRef. Initially support exact local absolute and relative paths, without wildcards, remote addressing or automatic actor creation. Persist the normalized absolute actor path, not the runtime selection object. Missing destinations must be observable so the approval queue can retain pending delivery; acknowledgement/deduplication remain the approval protocol's responsibility. `selection.resolve()` resolves the current live reference and explicitly reports a missing destination. A successful `tell()` is not acknowledgement that the approval was handled.

## Approval response capability inspection

Installed Doubao CLI status parsing distinguishes pending `input` from `approval` requests and includes messageId/blockId, optional threadId, and input clarifyId or approval scene/items. Session ID alone is insufficient to target a native response. The inspected CLI help and command dispatcher expose status/wait/send but no dedicated operation to answer those pending controls. Sending an ordinary chat message must not be assumed equivalent to a native approval response.

Accepted adapter extension: `respond(session, request, response)` targets the exact pending external request. ApprovalQueueActor returns a correlated user response to the invocation Actor; that Actor invokes the adapter. Response delivery failure retains the pending delivery/error state rather than claiming external execution resumed. Implemented provider mappings and their verification limits are documented below.

## Agreed execution and approval flow

Signal triggering creates a Run; the internal Agent prepares and records its Task; System One checks readiness; confirm mode submits a pre-execution request to the shared approval queue. After approval or in auto mode, the invocation Actor submits the Task to the selected executor and persists its session/run identity. External input requests enter the same queue. Resolved requests return to the responsible Actor by a persisted address and request ID. That Actor forwards execution-time responses with `respond` and continues tracking the same external execution. Internal delivery acknowledgement and external response delivery are distinct stages; neither proves that the Task has completed.

The unified contract consists of submit, status, resume, wait and respond. Business success remains TaskResult `{ text }`. The adapters, native approval bridge and local UI/API implement this flow; verification limits are listed below.

## Provider validation and limitations

The Codex adapter uses the generated local app-server protocol: thread/start, thread/read, thread/resume, turn/start, native command/file/permission approvals and requestUserInput responses. It no longer uses ephemeral `codex exec`. A remembered process ID is only additional recovery evidence, never a replacement for the real thread/turn IDs; a still-live or unidentifiable old process prevents automatic concurrent resumption. Shutdown records active turns before terminating its app-server; an interrupted turn can resume only with matching shutdown evidence. User cancellation is preserved.

The inspected Doubao CLI main branch (0.12.0) explicitly does not submit approvals. The integration therefore supplies a native response bridge, following observed Work 2.31.6 module and communication contracts. It handles local interaction.ask answers, single-command allow/reject, and pre-tool safety decisions; it rejects unsupported native controls and does not grant broader persistent permissions. Before responding it queries the original session/run and checks the exact pending request. Read-only probing confirmed the current app exposes the required bridge/API functions. Actual user approvals were not issued during validation; controlled tests cover protocol mapping.

Automated tests cover path resolution across ActorSystem recreation, durable queue delivery, confirmation recovery without rebuilding the Task, execution readiness ordering, external response correlation, failure handling, Codex session/approval/result recovery with a protocol fixture, and Doubao native mapping. Existing Goal, Signal, memory and IM flows remain covered by the full suite.

## Pi durable executor

An optional `agents.pi.model` registers Pi behind the same ExternalAgent contract. TaskPreparation remains responsible for constructing the exact Task from the Signal definition and source snapshot. Delegation now passes a stable `requestId` derived from its Context path; existing executors may ignore this optional admission metadata. Pi commits task, owned conversation, configuration and accepted history atomically, and rejects changed content under an existing request identity. Its returned sessionId/runId are real Pi conversation/task IDs; adapter metadata identifies the owner shard and request.

Status and wait read durable outcomes. A completed execution can be replayed after restart without another model request; safe tool intents may resume, while unsafe interrupted/error outcomes are projected as unknown and block subsequent tool calls. Execution result/history is stored before core applies it to Delegation and Signal Run. When a submission handle was not acknowledged, Delegation now uses optional ExternalAgent.lookupSubmission to query the original Task/request identity. Pi looks up retained admission without creating or resuming work. Found handles commit before Run notification and ordinary observation continues; missing, failed or unsupported lookup remains uncertain. No generic external submit is automatically retried. Explicit user resumeRun admission remains separate work.

The configured initial Pi executor provides evidence-only analysis without tools. It does not support interactive input or approval requests, and rejects responses it cannot deliver. Approval-aware real tools, controlled execution environments, ownerless Personal/Goal runtime migration remain unfinished; shared Pi Context/runtime ownership and offline backend migration are implemented. Temporary-store tests cover the real Delegation mailbox path with a fake model; no live external service is exercised.

## Admission inspection and reconciliation

A Delegation without an acknowledged execution handle can ask a capable adapter to look up its stable submission identity. This is a read-only query against durable admission, not another submit. Pi matches the original prompt/instructions and request ID against its task record and validates the single owned conversation. Lookup uses the retained model/catalogue, so a completed admission remains inspectable under a newer configuration. New submit still rejects changed configuration under the same identity, and reopening pending execution still requires its original catalogue/model.

On found admission, Delegation persists its session and clears the superseded uncertainty before notifying Run. Run clears its old uncertain display outcome when the submitted handle arrives. The ordinary status/wait path then observes and, where the existing runtime contract permits, continues already-admitted pending work according to replay policy. Lookup itself never starts the Harness, restarts failed execution or replaces an unsafe unknown outcome. Missing or unavailable admission stays uncertain; unsupported adapters keep the existing conservative behavior.

Personal's inspectDelegation command is independent of executor observation: it reads the committed business record in the Personal mailbox and returns a schema-defined projection. Its RPC and replay-safe model tool omit provider/session metadata, raw transcript and native frames. The Delegation page consumes this projection and refreshes after path changes or SSE reconnection. General Context access redaction and external unsafe-tool outcome reconciliation remain broader requirements.

## Independent Personal Task admission

Personal may submit a prepared Task directly through `StartPersonalTask`. This creates a Run under `/runs/personal--<sha256(requestId)>`, owned by the runtime’s `/user/runs` root. It does not create a Signal or schedule. Existing Goal and Signal Run ownership remains separate. The shared `PreparedTask` contract is the exact execution input; TaskPreparation.prepare must not rewrite it.

Personal persists its command in the outbox before delivery. The receiving Run validates the target identity and executor, and creates its frozen Task, source/causation, checking state and exact-input receipt in one expectedRevision-zero commit. Only then does it acknowledge admission. Exact retries return that receipt before revision checking; payload reuse conflicts. Recovery resumes the retained readiness/confirmation/delegation phase, and completed retries never start another execution. Readiness runs before a mandatory confirmation of that concrete Task. A model can propose the Task in its structured reply but cannot approve it.

## Explicit Run resumption

`ResumePersonalRun` carries separate Personal and Run revisions, request ID, causation ID and an existing Run path. It accepts failed or uncertain executions with a frozen Task. It does not restart task preparation, replace a session, bypass pending confirmation, or reopen completed/cancelled/rejected work. Goal Task validity is checked at admission. The `/runs` root routes the command to the original Personal, Signal or Goal child and never creates a replacement owner for an unavailable path.

Personal queues the command before acknowledging. The Run commits exact-input admission and a pending handoff with its receipt before sending to Delegation. Repeated requests reconcile the same handoff; collisions and stale new requests conflict. Delegation likewise admits the exact command before provider observation. Creating a child for manual resumption suppresses its ordinary recovery path until this admission, while already-admitted unfinished commands recover on restart.

Delegation first uses retained completion/cancellation or queries the original external session. Only a provider-reported resumable failure permits resume. A `resuming` marker commits before the external call. The returned session handle (including a changed run ID) and successful marker commit together. Resume failure or a crash in that interval leaves an unknown outcome; recovery can observe but cannot repeat the call, including with a new request ID. A different returned session ID is rejected as uncertainty and the original handle is retained. A later authoritative running/completed observation reconciles unknown markers. Durable completion replays after parent outcome loss without consulting the provider.

Run admission receipts, Delegation execution status and Task completion remain separate. UI controls preserve uncertain source admission identity across navigation/reconnect, expose handoff state, and read business resumption status through Delegation inspection. Browser-only identity does not yet survive a full document reload. Ordinary pre-existing automatic recovery paths are still subject to the broader execution-reconciliation audit.

## Personal approval requests

`RequestPersonalApproval` takes Personal/ApprovalQueue revisions, a source Context revision, the existing approval ID, and request/causation identity. Personal saves the command through its durable outbox. A structured Personal reply can propose `approvalRequests`; reply, cursor and intents commit together. Approval decisions remain explicit user operations.

ApprovalQueue derives the entry from committed domain state. A Run must be awaiting confirmation for the concrete frozen Task, with a valid Goal Task reference when present. A Delegation must be waiting for an unanswered request from its current session. Queue prompts, input options and Actor destinations come from those records rather than caller input. A request cannot invent a new permission, reuse an answered/obsolete execution request, reopen a resolved/revoked entry, or grant standing authority.

The queue validates source and queue revisions and atomically commits the entry with its exact-input command receipt. A matching existing pending entry remains unchanged; only the new command receipt is added. Exact retry returns the retained receipt even after later domain progress or revocation. Approval admission snapshots a demand; source state may progress independently, and the execution owner revalidates the response in its mailbox. Revocation is durable even if it arrives before enqueue, so a delayed request cannot recreate a withdrawn demand after restart.

The API requests approval of existing work. Preparation, generation of external questions and permission policy remain owned by Run, Delegation and their execution services. This endpoint never executes work or decides the approval itself.
