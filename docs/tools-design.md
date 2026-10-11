# Agent tools and Context retrieval

Status: implemented. Updated on 2026-10-11 against baseline `b7ff77b`. This replaces the previous tool design and implements the tool-specific refinement of the separate core architecture review. Broader core business-state and lifecycle changes remain separate work.

## Intent and ownership

The primary Goal Agent communicates with the user and coordinates work. Internal Tasks discover sources, inspect their commands and retrieve evidence. Model arguments describe requested work; code supplies ownership, feedback destinations, stable operation identities and execution policy.

Retain explicit catalogues, Effect-native execution, serialized owner-local state methods, commit-before-acknowledgement, native transcript persistence, cancellation and uncertain-outcome reconciliation. Remove model-facing Actor envelopes, `Task.Goal`, handwritten domain schema copies, tool-side Actor routing and the `queryTool` / `commandTool` split. No compatibility aliases or historical-data migration are required.

Tools adapt domain capabilities. Domains own validation, routing, accepted state, receipts and recovery. Integrations own their source commands and provider boundaries. The agent package owns SDK schema conversion, streaming response parsing, captured Effect Context and callback lifetime. The Actor package remains domain-neutral.

## Observations motivating the redesign

The travel Goal received directory entries for `/apps/ctrip` and `/apps/xiaohongshu`, and its native system entry declared `start_task`. One reply announced two Tasks without calling a tool. A later turn made ten `start_task` calls; all parsed arguments were `{}` and failed native validation before Task admission. The model's explanations about missing data or XML serialization are generated text, not diagnostic evidence.

Two local reproductions expose independent adapter concerns:

- The installed Pi 1.1.0 Anthropic adapter correctly assembles JSON deltas, but overwrites complete `content_block_start.input` with `{}` at block end when no JSON delta follows. The production turn has no retained raw SSE, so this does not establish its exact wire-level cause.
- A command tool failing with `ApplicationError(kind="unavailable")` becomes an ordinary Pi tool error. A synthetic model retried with a new call ID and executed it twice. Throwing a typed error or marking a tool non-replayable does not stop a fresh model call.

Shallow schemas, explicit capability descriptions, correct provider parsing and a terminal invocation failure path are separate requirements. Prompt changes alone cannot verify the transport or recovery behavior.

## Context definitions visible to the primary Agent

The Goal system instructions include a sanitized Configured Contexts directory containing only public `path` and `description`:

```json
[
  {
    "path": "/apps",
    "description": "Connected applications for read-only research and travel planning."
  },
  {
    "path": "/apps/ctrip",
    "description": "Find destinations, accommodation and transport; prices and availability are snapshots."
  },
  {
    "path": "/apps/xiaohongshu",
    "description": "Find travel experiences and recommendations; inspect original notes and comments."
  }
]
```

Reuse the captured configuration and existing `ContextDirectory` acquisition boundary. Traverse public Context path/description nodes, including direct child and `children` mappings. Never inject the complete `contexts` configuration tree, adapter settings, credentials, implementation code or private owner state. Descriptions guide research and confer no external-action authority.

The current reader includes explicitly described configured entries. An entry using only an adapter-default description may be absent; omission does not prove that a source is disabled or inaccessible. Prefer explicit source descriptions. Task discovery is the fallback, without introducing a new configuration manifest or integration-specific logic in core.

The primary Agent does not receive Context or Memory retrieval tools. Its directory is sufficient to choose likely source paths. Full command schemas, dynamic entries and live availability belong to Task discovery. Goals may start before integrations activate: a configured source must not be presented as already connected or already queried.

| Information                         | Authority                              | Consumer                                         |
| ----------------------------------- | -------------------------------------- | ------------------------------------------------ |
| Configured paths/descriptions       | Captured configuration metadata        | Goal prompt and Task routing hints               |
| Active paths and command catalogues | Scoped owner registration              | Internal Task discovery                          |
| Command arguments/results/text      | Owning domain command schemas          | Internal Task execution                          |
| Committed Context text              | DurableContext and owner serializer    | Explicit application reads and supplied evidence |
| Retained query evidence             | Caller-owned native conversation entry | Task paging and exact replay                     |

A directory, capability definition and query result are different things. Generic model tools never expose arbitrary state dumps, raw snapshots, pending Context messages or unfiltered native transcripts.

## Task input and `contextPaths`

`start_task` starts internal Agent work only. Its model input is a plain record:

| Field          | Contract                                                                                                            |
| -------------- | ------------------------------------------------------------------------------------------------------------------- |
| `title`        | Required non-blank short title describing the work for Task lists and UI.                                           |
| `instructions` | Required non-empty work request, known user constraints and desired result.                                         |
| `contextPaths` | Optional canonical public Context paths, normalized to a unique list; defaults to `[]`.                             |
| `evidence`     | Optional `{ content, sources }` records of supplied material; defaults to `[]`. Missing `sources` defaults to `[]`. |

The title is retained in Task admission and snapshot metadata. Task list projections read it without loading instruction bodies; inspection exposes it alongside instructions; follow-ups, retries and recovery preserve the original title. It does not determine Task identity.

Path hints are part of the contract but need not be non-empty. Requiring a known path for every Task encourages invented paths. Tasks without hints discover capabilities themselves.

```json
{
  "title": "Research Spring Festival family travel",
  "instructions": "Research initial Spring Festival travel options from Nanjing for two adults and one child. Compare candidate directions and identify what depends on the child's age or budget. Do not assume either preference.",
  "contextPaths": ["/apps/ctrip", "/apps/xiaohongshu"]
}
```

Path-hint semantics:

- Validate public path syntax and remove duplicates at admission. Do not perform source reads or require an active route to accept work.
- Hints identify sources to investigate first. They are neither authorization nor an access allowlist; discovery may find other relevant read-only sources.
- A root such as `/apps` may be only a directory. Discover its descendants; do not invent a command for the root. Describe a known child directly when appropriate.
- Persist hints as structured Task admission data. Recovery and follow-up must not parse them out of instructions or depend on the original Goal invocation being alive.
- Supply hints and their known public directory descriptions to the Task prompt. Copy only selected metadata, not raw configuration. Label supplied evidence as data, not instructions or authority.
- Keep `contextPaths` separate from `evidence.sources`: a suggested source is not proof of retrieval or a citation.
- Missing/retired routes produce retrieval limitations. They do not imply that facts do not exist or block independent investigation through other sources.

`delegate_task` accepts `executor`, `title`, `instructions` and optional `evidence`. External executors do not automatically receive Aster's internal Context tools. Retrieve connected evidence with an internal Task before passing it to an external executor when needed; a Context path alone does not grant external-process access.

## Owner and invocation injection

Reuse `CurrentContextPath` for canonical owner identity and `GoalState` for serialized local Goal operations. Add only a small invocation binding for accepted input identity, timestamp and the existing propagation budget. Derive stable operation identity once from owner path, accepted input identity and tool-call identity, retaining existing durable identity semantics. Remove per-tool `origin` callbacks and repeated Goal/path factory bindings.

For Goal tools, code binds Task source and asynchronous feedback destination to the current Goal. A Signal is the source of its action, while feedback returns to its owning Goal. An internal Task path must not be blindly used as a Goal feedback destination. Creation/coordination tools are available only in the appropriate Goal catalogue.

The model never constructs `replyTo`, `source`, `requestId`, timestamps, budgets, private Actor paths, cancellation handles or generation tokens. Durable records store stable identifiers, never live ActorRefs. Mailbox protocols still retain explicit typed `ReplyTo<Response>`; automatic feedback binding does not add an implicit sender model to Actor runtime.

The agent adapter captures the caller's Effect Context and retires callbacks with the invocation Scope. Owner-local operations call GoalState directly and never ask their own mailbox. Cross-owner domain capabilities own addressing and receipts. Tools neither select `/user/...` Actors nor cast dynamic refs. No global invocation singleton, universal metadata envelope or new receipt database is introduced.

## Complete tool catalogue

Inputs use Effect Schema and normalize optional fields once. Names below are the target interface; receipts and selected business responses exclude private execution state.

| Invocation            | Tool                       | Model input                                         | Behavior                                                                                  |
| --------------------- | -------------------------- | --------------------------------------------------- | ----------------------------------------------------------------------------------------- |
| Goal                  | `goal_current`             | `{}`                                                | Current public Goal understanding and described internal/external execution capabilities. |
| Goal                  | `update_summary`           | `{ summary }`                                       | Persist through GoalState before acknowledgement; cannot complete or archive a Goal.      |
| Goal                  | `task_list`                | `{}`                                                | Current public Goal Tasks, including completed work that can continue.                    |
| Goal                  | `start_task`               | `{ title, instructions, contextPaths?, evidence? }` | Admit internal work; return its Task path and receipt.                                    |
| Goal                  | `delegate_task`            | `{ executor, title, instructions, evidence? }`      | Admit external work with the existing confirmation workflow.                              |
| Goal                  | `task_send`                | `{ taskPath, text }`                                | Ownership-validated follow-up to the existing Task; completed work may continue.          |
| Goal                  | `send_goal_message`        | `{ goalPath, text }`                                | Independent durable Goal input; no TaskActor or Task reference.                           |
| Goal                  | `signal_list`              | `{}`                                                | Owner's public rules, statuses and observed definition versions.                          |
| Goal                  | `create_signal`            | `{ id, trigger, action }`                           | Create an owner-scoped rule; conflicting creation is rejected.                            |
| Goal                  | `update_signal`            | `{ id, version, trigger, action }`                  | Edit using the observed definition version.                                               |
| Goal                  | `control_signal`           | `{ id, version, operation }`                        | `pause`, `resume` or `delete`, preserving domain pending-action rules.                    |
| Internal Task         | `list_contexts`            | `{ parent?, offset? }`                              | Page active capability paths/descriptions.                                                |
| Internal Task         | `describe_context`         | `{ path }`                                          | Read supported command names, descriptions and argument schemas.                          |
| Internal Task         | `query_context`            | `{ path, command, args? }`                          | Validate and invoke a read-only command; retain evidence before returning the first page. |
| Internal Task         | `read_query_result`        | `{ resultId, offset }`                              | Read another retained text page without repeating the source query.                       |
| Internal Task         | `memory_search`            | `{ query }`                                         | Compact recall candidates; expand useful matches before relying on them.                  |
| Internal Task         | `memory_expand`            | `{ items: [{ obsId, sessionId? }] }`                | Original selected memory evidence and references.                                         |
| Goal or Internal Task | `memory_save`              | `{ content, type?, concepts, files }`               | Durable admission of a concise fact, preference, decision or finding.                     |
| Context gate          | `submit_context_relevance` | `{ relevant, reason }`                              | Private structured gate result; no retrieval or mutation tools.                           |

Goal and Task catalogues are independently importable. Internal Tasks do not recursively start Tasks, mutate Goal summaries or create Signals. No Agent tool archives a Goal or retries uncertain submissions. External executor tools remain executor-specific. The current source-query allowlists do not authorize travel booking, posting or account changes.

Signal triggers preserve Context conditions and timezone-qualified once/cron schedules. Use descriptive model-facing `type` values, not mailbox envelopes. Actions explicitly select `start_task`, `delegate_task` or `send_goal_message` with the corresponding work/message input. Code binds feedback ownership. Small semantic trigger/action unions remain appropriate; do not copy a nested create/update/control union and mixed Actor Task protocol into one tool. Frozen occurrences retain exact source, action and evidence; rule edits never rewrite admitted occurrences.

The normalized Signal records use these shallow variants:

| Record               | Fields                                                                                        |
| -------------------- | --------------------------------------------------------------------------------------------- |
| Context trigger      | `{ type: "context", when }`                                                                   |
| One-time trigger     | `{ type: "once", at }`, where `at` is an ISO timestamp with a timezone offset.                |
| Recurring trigger    | `{ type: "cron", expression, timeZone }`, with a validated cron expression and IANA timezone. |
| Internal Task action | `{ type: "start_task", title, instructions, contextPaths?, evidence? }`                       |
| External Task action | `{ type: "delegate_task", executor, title, instructions, evidence? }`                         |
| Goal message action  | `{ type: "send_goal_message", goalPath, text }`                                               |

`id` is a local rule slug; code derives the current Goal namespace. `version` is the observed positive definition version, not a Context revision. Missing versions cannot become blind updates. Creation, editing and controls remain separate operations. Signal execution retains manual pause/deletion, user-only Goal archival and the accepted skip-missed-times policy from the core design.

Successful Task creation reports `status: "accepted"`, `taskPath` and the owner receipt. It does not imply completion or provider submission. Goal-message acceptance returns its receipt without a synthetic Task path. Machine identifiers remain in tool results/execution details; user replies explain the work naturally.

## Context discovery and command definitions

`list_contexts` returns `{ items: [{ path, description }], total, nextOffset }`, sorted by public path, twenty items per page. `parent` defaults to `/`, `offset` to zero. A non-root parent selects descendants, not its stored state. Pages reflect live registration at each call and are not a frozen catalogue snapshot while owners activate/retire.

`describe_context` returns `{ path, description, commands: [{ command, description, arguments }] }`. Argument JSON Schema comes from the owner's Effect Schema, including required fields, enums and limits. There is no duplicated tool-side command catalogue or fallback `state` command. Missing routes report unavailable; an empty command set does not permit an invented command.

`ContextCommand.Class` declarations remain the shared source for owner registration, discovery metadata, dynamic validation and typed Actor dispatch. Registration belongs to the owner's Scope. A discovered chat/email/Task is not necessarily a capability entry point: its collection can provide the supported list/read commands.

A Task may reuse a known command definition without repeatedly listing/describing it. Execution always checks the current registered schema. Configured source metadata alone cannot replace this validation.

## Context query execution

`query_context` accepts a public path, supported command name and named arguments; `args` defaults to `{}`. Retain the existing scalar argument contract: strings, numbers and booleans. Arbitrary nested JSON requires a concrete owning-query requirement; shell text is never accepted.

Decode the outer input, then validate against the selected command's argument Schema with excess-property rejection. Unsupported commands/fields produce sanitized field locations and expected types or constraints. Never echo rejected credentials, private commands or arbitrary backend exception text. Validate success and declared errors against the owner's schemas; malformed handler responses remain defects rather than empty results.

Integrations own Ctrip flags, signed Xiaohongshu URL requirements, Lark mailbox filters and other provider details. Generic core tools do not reproduce them. Source reads do not alter synchronization cursors or pending Context messages and do not authorize booking or sending.

Example after inspecting `/apps/xiaohongshu`:

```json
{
  "path": "/apps/xiaohongshu",
  "command": "search",
  "args": {
    "query": "Spring Festival family travel from Nanjing",
    "limit": 10
  }
}
```

Preserve source timestamps, prices/availability snapshot semantics and provider coverage limits. An unavailable route, an unpublished holiday announcement or an incomplete provider page is not proof that a fact or travel option does not exist.

## Query evidence and pagination

Format each successful validated result using the owner's declared text formatter, then append the complete evidence to the caller's Pi conversation before returning `resultId`. The native entry retains the original validated input and canonical encoded command input, one complete text result, and observation time. Command defaults/transforms are applied before execution and canonical comparison. Pi remains the only query-result store; neither a result database nor an invocation-local Map becomes another authority.

Pages return `{ resultId, path, content, totalCharacters, nextOffset }`. Preserve the existing 2,000-character page size and 1,000,000-character result limit. Reject oversized evidence with a request to narrow the source query. Tool presentation consumes a typed page and does not synchronously decode unknown output inside the generic wrapper.

`read_query_result` binds the current Task owner in code and checks entry kind. Reject negative, non-integer or beyond-end offsets; completion uses `nextOffset: null`. Empty evidence remains a valid result. Copying an integer ID cannot expose another Task's conversation. Exact original or canonical input replays committed evidence without a live route. While the route is active, other valid representations normalize before identity comparison, so omitted and explicitly defaulted fields share the same observation. After route retirement, a representation different from both retained forms returns a conflict requesting the exact original or canonical input; no JavaScript decoder or schema cache is persisted. Changed canonical arguments under the same identity fail.

Source pagination and evidence pagination are distinct:

- A provider cursor inside evidence requires another `query_context` call with that documented cursor to retrieve more source records.
- `read_query_result` reads more characters from the response already retained. It never fetches another provider page.

Retained pages remain readable after route retirement or model compaction. New query identities can observe new source facts while old pages remain the same observation. AppState does not gain a second query-result history.

## Query resources and cancellation

Replace ContextsActor with one runtime-owned scoped query execution capability shared by model tools and application queries. Reuse owner registration and the existing four slow-query limit; providers can impose stricter limits. Discovery and retained-page reads do not occupy remote-query slots. Capacity exhaustion and duplicate in-flight identities are distinct known failures.

Each business query still reaches its owning Actor command where that is the owner boundary. Caller interruption, timeout or owner-route retirement cancels that request and releases capacity. Closing the root Scope retires workers. Use scoped Fibers and Effect cancellation, not detached Promises, timer registries or a forwarding Actor.

Memory search/expand move to scoped recall with the existing four-operation limit. MemoryActor owns durable capture, retry and observation. Recall cancellation must not change admitted capture drain semantics.

A remote read completed before evidence persistence can be read again after a failed handoff: there is no exactly-once remote-read promise. Once evidence commits, replay/paging reuse it even if acknowledgement was lost.

## Schema and error boundaries

Model-facing operation inputs use Effect Schema beside their owning domain operations. Reuse work, evidence, path, schedule and query constraints. These records intentionally differ from internal Actor commands by omitting execution metadata. Delete handwritten TypeBox copies of Task and Signal contracts.

The agent package converts supported Effect Schemas into SDK JSON Schema at the native boundary and passes decoded inputs to Effects. Runtime decoding enforces constraints that JSON Schema cannot express. Test actual provider payloads and SDK validation, including unions, nested records, optional fields and empty-argument tools. Do not weaken domain validation to accommodate a broken parser.

Use one thin tool definition adapter for result encoding and known error feedback. It does not own addressing, permission policy, capacity or automatic retries. Delete `queryTool`, `commandTool` and read/write modes.

| Outcome                                                                            | Behavior                                                                                                                                                               |
| ---------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Known invalid input, conflict, failed query or missing source                      | Return `isError: true`, stable code and sanitized explanation/field metadata. The model may correct input or choose another source.                                    |
| Mutation definitely rejected before acceptance                                     | Return a known rejection. Destination unavailability before dispatch is not automatically unknown acceptance.                                                          |
| Mutation dispatched but receipt missing, or accepted work with unconfirmed handoff | Preserve typed uncertainty and original identity, stop the reasoning invocation and reconcile the original operation. Never automatically resubmit with a new call ID. |
| Defect                                                                             | Preserve failure and the owning supervision path.                                                                                                                      |
| Interruption                                                                       | Preserve cancellation and drain applicable admitted work; never claim that local cancellation proves remote cancellation.                                              |

Domain operations distinguish definite failure from unknown acceptance; `unavailable` alone cannot express both. The agent callback boundary needs an explicit typed terminal-failure contract without core imports. Freeze the callback fence before awaiting durable I/O, retain terminal native evidence by original input identity, then notify/abort Pi. Reject further callbacks and preserve `outcome: "unknown"` plus the operation identity through Goal/native recovery. Do not use `Effect.die` to represent expected uncertainty. Concurrent work already admitted retains its independent owner and requires reconciliation.

GoalState retains the uncertain operation ID and checks existing source-qualified Task, Signal and Goal-message receipts on restoration and subsequent mailbox wakes. An absent receipt never proves rejection. A positive receipt permits trusted `AgentConversations.resolveFailure` to append native resolution evidence without configuring a model, repair Task attachment and settle the aborted dialogue with an acceptance notice. Queued user inputs and Task feedback then resume. The original native request remains aborted rather than resubmitted; unsupported or unavailable resolution stays unknown. No Agent tool exposes this resolution capability.

Native `replay` is separate from input/error handling and reflects the actual recovery contract. Retained queries/reads can be replay-safe because identity and ownership are checked. Mutations default to non-replayable unless exact replay is explicitly supported and tested. Read/write labels do not enforce permission or define replay safety; unsafe recovery protection does not prevent a new model call ID.

| Tool family                                        | Initial replay declaration                                                                                                               |
| -------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------- |
| Goal/Task/Signal reads and Context catalogue reads | Safe to run again; they may observe a newer committed state.                                                                             |
| `query_context` and `read_query_result`            | Safe with owner-bound identity and retained evidence checks; committed query replay returns its original observation.                    |
| Memory search/expand                               | Safe as read-only operations; an interrupted uncommitted read may observe a newer source.                                                |
| Summary, Task, Goal-message and Signal mutations   | Non-replayable by default. Enable safe replay only when the owner operation's exact identity/receipt contract is implemented and tested. |
| Private relevance result                           | Safe within its read-only gate; it does not mutate business state.                                                                       |

Readable content and typed `details` present the same selected business response; they are not separate stores. Private transcript frames, provider handles and receipts do not become routine public Goal text.

## Prompt and provider requirements

`goals/prompt.ts` assembles the structured Goal conversation policy: role and responsibilities, input handling, next-action decisions, capability use, evidence/authorization, and communication, followed by sanitized Goal, language and configured Context metadata. Task completion is evaluated against the original purpose before answering, continuing the same work or starting distinct necessary work.

Successful internal turns may return no text when an update warrants no notification. GoalState still durably settles these inputs without manufacturing a public reply. Empty user turns retain their visible fallback; typed execution failures, unknown outcomes and automatic-feedback budget exhaustion retain their existing handling. Prompt wording guides model choices; local tests verify assembly and settlement rather than claiming to measure model behavior.

The Goal prompt defines internal Task retrieval responsibilities; the `start_task` description names the discovery, command-query, retained-page and Memory tools. Missing fresh facts in the Goal conversation are a reason to retrieve evidence, not to declare a source unavailable.

Execute creation/follow-up tools before claiming accepted work. Only a successful receipt supports acknowledgement; a natural-language plan creates no Task. Finish submitting independent work needed for the current plan, then end the turn without polling. Feedback arrives through the existing durable handoff, and the Goal communicates findings, failures and critical questions in its own voice.

Proceed with useful read-only investigation when known constraints suffice. Ask questions that materially change the next action and continue independent work while awaiting answers. Do not invent age, budget, dates, destination or authority. Greetings and questions answerable from supplied evidence need no Task.

Task input includes instructions, source hints, selected metadata and explicitly labelled evidence. Inspect capabilities before declaring retrieval unavailable, expand useful source records and return citations and material coverage limits. A retained text page ending does not exhaust a provider cursor.

Fix provider parsing independently: preserve complete initial tool input without JSON deltas, correctly assemble deltas when present and report malformed input as a protocol/validation failure. Do not silently replace it with `{}` or add a generic retry loop. Raw wire capture is not enabled by default; diagnostics exclude credentials and private source content. The production fix must use a verified dependency version or a narrow private adapter, not edits to generated `node_modules` files.

## Complete target file responsibilities

| File                                                        | Responsibility                                                                                                                          |
| ----------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------- |
| `packages/core/src/tools/define.ts`                         | Typed descriptors, result presentation and known tool-error feedback; no read/write modes.                                              |
| `packages/core/src/tools/invocation.ts`                     | Small invocation metadata binding and stable identity derivation using injected ownership.                                              |
| `packages/core/src/tools/goal/catalogue.ts`                 | Assemble Goal tools without importing Task retrieval implementations. The private gate tool stays with Goal execution.                  |
| `packages/core/src/tools/task/catalogue.ts`                 | Assemble Task retrieval tools without loading the Goal coordination catalogue.                                                          |
| `packages/core/src/tools/goal/state.ts`                     | Current Goal and summary tools over local GoalState.                                                                                    |
| `packages/core/src/tools/goal/tasks.ts`                     | Task list, internal creation, external delegation and follow-up adapters.                                                               |
| `packages/core/src/tools/goal/messages.ts`                  | Independent `send_goal_message` adapter.                                                                                                |
| `packages/core/src/tools/goal/signals.ts`                   | Owner rule listing, creation, versioned edits and controls.                                                                             |
| `packages/core/src/tools/task/contexts.ts`                  | Discovery, command definitions, queries and typed retained-page presentation.                                                           |
| `packages/core/src/tools/task/memory.ts`                    | Scoped recall search/expand adapters.                                                                                                   |
| `packages/core/src/config/context-directory.ts`             | Captured public configured paths/descriptions, independent of live readiness.                                                           |
| `packages/core/src/tasks/contracts.ts`                      | Concrete work, evidence, structured path hints and admission contracts; no Task.Goal.                                                   |
| `packages/core/src/tasks/operations.ts`                     | Task addressing, ownership, admission/attachment, follow-up and uncertain receipts.                                                     |
| `packages/core/src/tasks/delivery.ts`                       | Private durable Task admission/attachment, feedback and existing receipt lookup used by domain operations.                              |
| `packages/core/src/tasks/execution/agent.ts`                | Model input with instructions, hints, selected directory metadata and evidence.                                                         |
| `packages/core/src/goals/messages.ts`                       | Independent Goal-message identity, validation and durable destination receipt.                                                          |
| `packages/core/src/goals/message-contracts.ts`              | Independent message delivery/model input schemas without loading delivery handlers or Signal storage.                                   |
| `packages/core/src/goals/reconciliation.ts`                 | Source-qualified positive receipt lookup across existing domain authorities; no submission, universal receipt store or model execution. |
| `packages/core/src/signals/contracts.ts`                    | Trigger/action/rule and normalized operation input schemas.                                                                             |
| `packages/core/src/signals/operations.ts`                   | Versioned domain changes and source/feedback ownership.                                                                                 |
| `packages/core/src/queries/commands.ts`                     | Discoverable owner command definitions, schemas and text formatting contracts.                                                          |
| `packages/core/src/queries/registry.ts`                     | Scoped registration, discovery and dynamic command validation.                                                                          |
| `packages/core/src/queries/execution.ts`                    | Shared query capacity, execution, cancellation and evidence handoff.                                                                    |
| `packages/core/src/queries/evidence.ts`                     | Native evidence identity, persistence, observation time, owner validation and paging.                                                   |
| `packages/core/src/queries/contracts.ts`                    | Query, catalogue, page and framework error schemas.                                                                                     |
| `packages/core/src/memory/recall.ts`                        | Scoped, bounded backend-independent search/expand and cancellation.                                                                     |
| `packages/core/src/runtime/addressing.ts`                   | Private typed cross-owner routing used by domain operations, never raw selectors exposed to tools.                                      |
| `packages/agent/src/shared/effect-tools.ts`                 | Effect Schema-to-SDK conversion/validation and native callback adaptation.                                                              |
| `packages/agent/src/shared/callbacks.ts`                    | Captured Context, invocation Scope, cancellation, defect propagation and terminal-failure latch.                                        |
| `packages/agent/src/shared/contracts.ts`                    | Backend-neutral failure/outcome contract including uncertain invocation failure.                                                        |
| `packages/agent/src/shared/tool-validation.ts`              | Schema-owned field/constraint feedback with rejected values and arbitrary keys masked.                                                  |
| `packages/agent/src/harness/failures.ts`                    | Native terminal failure and resolution identities/schemas, independent of core domain policy.                                           |
| `packages/agent/src/harness/runtime.ts`                     | Durable tool/provider registration and native result encoding.                                                                          |
| `packages/agent/src/models.ts` and private provider adapter | Resolved registration and verified provider compatibility/tool-input parsing.                                                           |

Delete `tools/actors.ts`, `tools/task/schema.ts`, `tools/signal/schema.ts`, the shared `tools/catalogues.ts` barrel and old one-file-per-tool adapters after their responsibilities move. Each invocation imports its own catalogue directly. Remove ContextsActor and Memory read commands when scoped query/recall services land. Do not create a public Layer for each internal helper. These grouped tool-family files refine the earlier core-review organization using `goals/tools.ts`, `tasks/tools.ts` and `memory/tools.ts`; other confirmed core boundaries remain unchanged.

## Implementation sequence and acceptance checks

1. Add local provider fixtures and correct tool-input parsing: complete initial input, split deltas, mixed text/tools, parallel calls, malformed JSON and legitimate empty-argument tools.
2. Implement typed terminal invocation failure. Prove that unknown mutation acceptance prevents new callback IDs, preserves identity and remains unknown through native/Goal recovery. Known query failures still permit independent research.
3. Introduce shallow Effect Schema inputs and injected ownership. Separate internal Tasks, external delegation and Goal messages while preserving receipts, confirmation, attachment and feedback.
4. Persist Context hints and include selected metadata in Task input. Clarify Goal/Task capabilities and action-before-claim behavior.
5. Replace read mailboxes with shared scoped query/recall, preserving owner commands, strict decoding, limits, cancellation and evidence paging before removing old paths.
6. Replace nested Signal tool envelopes with explicit versioned inputs/actions. Delete obsolete schemas/helpers and align maintained docs/exports.

Use `node:test`, real isolated Actors, temporary/in-memory Pi stores and fake models/transports. Verify:

- Goal metadata handles both nesting styles without exposing secrets. Missing static metadata remains discoverable by a Task; startup does not require live routes.
- Model fields omit feedback/identity/protocol metadata. Concurrent Goals and Tasks retain separate owner and callback bindings.
- Hints survive recovery, remain distinct from citations and do not impose an implicit access restriction.
- A real internal Task describes and queries synthetic `/apps` sources, retains evidence before acknowledgement and returns feedback to its bound Goal.
- Goal messages create no TaskActor. External execution retains approval and uncertain-submission recovery.
- Known errors are model-visible, uncertainty blocks fresh call IDs, and defects/interruption cannot become success.
- Query replay checks normalized input, owner and entry kind. Pages work after route retirement/compaction and never repeat source reads. Cover empty results, offsets, size limits and provider cursors.
- Capacity releases on interruption, timeout and owner retirement. Metadata stays responsive; duplicate in-flight identities do not duplicate backend reads.
- SDK wire schemas retain required fields and runtime-only validation remains authoritative.

For implementation, run `pnpm build`, focused agent/core tests, `pnpm test`, `pnpm check` and affected Effect diagnostics. Run browser tests if application contracts or UI change. Do not invoke real models, executors or messaging services or modify runtime data/credentials for verification. Implementation verification includes local fake transport suites, type checks, Effect diagnostics and browser tests for the Signal action projection.
