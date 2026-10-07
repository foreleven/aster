# Goal conversation design

Status: implemented. This document records the confirmed product boundaries. See [Goals](goals-design.md) and [Tasks](task-delegation-design.md) for the current contracts and recovery behavior.

## Confirmed direction

The Goal conversation should feel like a natural exchange between the user and the assistant. Tool calls, their arguments and raw results must not appear in this message list. See the Goal Conversation term in [the glossary](../CONTEXT.md).

Context changes remain internal evidence after relevance screening. The Goal decides whether to produce a useful natural-language assistant update; raw Context summaries are not automatically displayed as assistant statements. This selects option B from the interview. The visible speaker is distinct from the role used to encode evidence for the model.

Task feedback is interpreted by the Goal and expressed in its natural conversational voice. Completion, failure or inability to proceed, and requests for user input or decisions require a visible response. Important findings and material plan changes may warrant an update at the Goal's discretion. Routine acceptance, startup and execution steps do not each produce a separate message, avoiding duplicate acknowledgements. Raw tool records remain in Task execution details rather than the primary conversation.

Task is the existing domain concept that carries evidence retrieval and sustained work. It is not restricted to external Agent execution: an internal Agent may execute a Task in its own working conversation. A separate persistent work concept named Run should not be introduced alongside Task.

A completed Task accepts later instructions concerning the same work and becomes active again, retaining its identity and working context. Distinct work creates a new Task. Completion describes fulfillment of the currently accepted work, not permanent closure of the Task. For example, adding a regional breakdown to an already completed report continues its original Task. Reactivation starts further execution without erasing earlier outcomes; it does not imply resuming an already completed external provider run.

Follow-up instructions for an executing Task should guide its ongoing work at the next supported opportunity. The primary conversation acknowledges durable acceptance and remains responsive. For internal execution, Pi's `whenBusy: "steer"` provides the intended tool-round boundary rather than waiting for the whole execution to finish. Receiving a follow-up does not itself cancel an in-flight tool operation or undo completed effects.

External Agents own the handling of follow-up instructions while busy. Their implementations decide whether to steer current work or queue further execution; core does not implement an executor capability matrix or a fallback scheduling policy. Task owns durable input acceptance, delivery tracking and incorporation of execution feedback. The external Agent and its adapter own delivery into the provider's conversation and its execution scheduling. Acknowledging an instruction does not claim it has already taken effect, and core must not infer completion of all accepted Task work from a provider round ending alone.

All Aster-owned Goal and Task messages use pi-durable as their authoritative storage. This includes user input, assistant replies, internal Context evidence, Task instructions and follow-ups, execution feedback and available tool records. There is no separate GoalHistory message store or second public-chat message store. This decision concerns the Goal and Task conversations discussed here; it does not require changing an external Agent's private storage or migrating integration source archives as part of this refactor.

## Conversation and Task split

The primary Goal conversation handles simple exchanges and routes sustained work through Tasks. Follow-up input concerning existing work returns to that Task; unrelated work may create another Task. The primary conversation remains the user-facing speaker, while Tasks retain their working context and tool activity. Pending user input has priority over ready background inputs. Context screening runs in a separate bounded read-only slot, so a slow gate does not reserve the main conversation; an executing or recovering main turn remains serial and is not preempted. Internal Agent execution and external delegation are execution choices for the same Task concept.

The primary Agent has only seven coordination tools: `goal_current`, `update_summary`, `task_list`, `start_task`, `task_send`, `signal_list` and `set_signal`. Context discovery, Context/integration queries and memory retrieval are available only in internal Task execution. This boundary is enforced by the tool catalogue rather than by estimating how long a query will take. Asking for an existing Task's status can be handled in the primary conversation; looking up today's emails or performing an additional analysis is routed to a Task. After durable admission the Goal acknowledges the work and ends its turn; it does not poll or wait for completion. Dialogue based on available conversation requires no Task and no mandatory preliminary tool call. All tool interactions remain outside the public message list.

Task owners use `/tasks/<sha256(source, requestId)>`. Internal execution uses a dedicated retained Pi conversation; external executor handles remain private TaskExecution checkpoints in Pi. Pi execution tasks are SDK mechanisms and do not introduce another Aster work entity.

## Message storage and presentation

The implementation gives the Goal primary conversation and each Task their own Pi conversation identity. For external execution, the Task conversation retains Aster's instructions, follow-ups and returned messages; the provider still owns its private session. A separate internal model invocation is not required merely to store an external Agent's feedback.

Pi supports custom entries and write-only submissions, so retaining evidence or execution feedback need not trigger a model call. Input origin and public visibility must be explicit metadata or entry kinds. Filtering only by model role is insufficient: the current adapter represents internal evidence as user-role input, and intermediate assistant text is not necessarily a user-facing reply. The public Goal list projects actual user messages and the Goal's selected conversational replies. Tool records and internal evidence remain accessible through internal/detail views without being copied into another store.

Selected reply entries can be identified by durable references in Pi rather than copying their text into Goal state. `goal.input` and `goal.reply` distinguish admitted evidence from selected public text. Task entries use `task.admission`, `task.input` and `task.result`. Use committed order and stable request/entry identities for replay and deduplication. Pi compaction and reset retain older entries in storage; full-history pagination must read retained entries, not just the current model context or active transcript view.

The separate GoalHistory interface, memory/file implementations and runtime wiring have been removed. The implementation replaces duplicated Goal input payloads, response strings and Task message bodies in Actor persistence with Pi conversation, submission or entry references as appropriate. Actor state continues to own business state and delivery/recovery bookkeeping; Pi owns message content. Goal business mutations go through the Actor-local GoalState service, shared by command handlers and local tools; Pi remains the message store.

Message admission must commit to Pi before acceptance is acknowledged. Pi and Actor storage do not share an atomic transaction: use stable identities and a recoverable handoff so a crash between message admission and Actor-state updates neither loses work nor repeats an accepted execution. The shared scoped Pi writer supports message admission while a Task executes. SDK operations and writer ownership stay in packages/agent, with Effect capabilities consumed by core. Durable Agent construction requires the injected AgentConversations service; it never opens a standalone writer.

## Implementation

`packages/agent/src/conversations.ts` owns shared Pi writers and retained entry access. Goal admission and presentation live in `goals/state/inputs.ts`, `goals/actor.ts` and `goals/view.ts`. `tasks/actor.ts` schedules execution and follow-up through its mailbox, `tasks/state/` owns the committed business model and Pi handoffs, and `tasks/execution/` invokes internal and external executors. `tasks/view.ts` provides execution details. External adapters implement continuation through `ExternalAgent.followUp`. The web Goal Timeline renders public dialogue, with Task and approval detail separate.

The implementation replaces the old contracts without migration or compatibility aliases. Runtime data and credentials are not changed by development or validation.

### Delivery and recovery requirements

- Use one stable Task identity across follow-ups and reactivation, and a distinct stable request identity for each accepted input. Exact retries return the existing acceptance; changed reuse fails.
- Commit message admission in Pi before acknowledgement. Recover the Actor handoff from committed entry/submission identities when a crash separates those commits. Neither an Actor receipt alone nor a transient callback substitutes for the stored input.
- Record gate results and public-reply selection against stable input/entry references. Replaying a completed invocation must not rerun its model call or duplicate its public reply.
- Keep model-context compaction independent of retained chat history. A read or UI reconnect must not start an Agent execution.
- Receiving input during Task execution must not block behind the whole model invocation. Model execution results return through the Actor mailbox with generation checks. Local Goal tools use the injected GoalState business methods, which serialize state changes without holding a writer during model execution.
- A Task with accepted, unsettled follow-ups is not complete merely because one execution round ended. Preserve the input coverage reported by each execution so late completion cannot overwrite newer work.
- Keep Task execution independent of the main conversation's current turn. Ending a turn does not stop its Tasks; changing this lifecycle must not accidentally inherit ordinary blocking subagent ownership.

### Validation

Use fake models and external executors with temporary Pi storage. Cover main-conversation responsiveness, internal Task execution, busy follow-up, completed-Task reactivation, concurrent independent Tasks, Context gating, public-message filtering, compaction history reads and restart recovery. Inject crashes at message admission, Actor handoff and result delivery boundaries. Retain approval and unknown external outcome tests.

Run the affected package tests during implementation, then `pnpm build`, `pnpm check`, cross-package `pnpm test`, `pnpm test:web`, and Effect diagnostics for affected packages. No real external Agent, model or messaging service is needed for verification. Update the Goal, Task, runtime and agent design documents to describe the final replacement.
