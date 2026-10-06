# Goals

A Goal owns a natural conversation, business understanding and routing to Tasks and Signals. The default personal assistant is the ordinary `/goals/personal` Goal and starts idle.

```text
Context change → System One → Context-only Goal gate → Goal conversation
User input / Task feedback ─────────────────────────→ Goal conversation
Goal conversation → lightweight tools
                  → Task → internal Agent or external executor
                  → Signal → scheduled or Context-triggered Task
```

## Conversation and messages

Simple exchanges and lightweight Context/memory reads run in the main Agent. Sustained work uses a Task. `start_task` creates work; `task_send` sends instructions to an existing Task, including completed work that should continue. A new topic only needs a Task when it requires sustained work. A Goal turn ending does not stop its Tasks.

`AgentConversations` owns one Pi conversation per Goal. Pi is the only message store: `goal.input` retains accepted user input, internal evidence and feedback; `goal.reply` retains selected public replies. Native Pi entries retain model and tool activity. There is no GoalHistory service or separate public-chat store. Compaction changes model context, not retained message history.

The public Timeline projects actual user inputs and selected assistant replies, with stable Pi entry cursors. It excludes Context envelopes, tool calls/results and intermediate tool-round narration. Context evidence may produce a useful conversational update, but is not itself a public assistant statement. Task completion, failure, blockage and decision requests require visible communication. Empty model replies and exhausted automatic-feedback budgets receive a conservative visible notice for required communication.

Goal conversations and Context gates log provider thinking blocks, text and tool-call names/IDs/arguments at Info level after each model response, before tool execution. Logs carry `goalPath`, `phase`, `model` and the conversation `inputId` or gate `intentId`. These are response-level diagnostics, not token streaming or a second message store. Thinking signatures are excluded; providers without thinking blocks only produce text/tool-call logs. Settled Pi replay does not emit fresh response logs.

## State and recovery

GoalSnapshot contains `definition`, `status`, `summary`, `tasks`, `inputs` and `receipts`. Input records hold Pi references, input kind, delivery status, Context gate decision, causality, retry reference and error. Array order records admission order; timestamps come from Pi entries. Message bodies and response text are not duplicated in Actor state. Receipts retain normalized command fingerprints and the original acceptance revision.

Admission commits the message and receipt identity to Pi before the Actor saves its references and acknowledges. Startup recovers admitted entries before creating the configured initial pursuit. Exact retries return their existing receipts; changed reuse fails. One input enters the main conversation at a time. Later inputs are durably accepted while execution is in progress. Pending user inputs take priority over ready background inputs; each group retains admission order. An already-started or recovering Pi exchange keeps its turn.

GoalActor creates one scoped GoalState service in `started`, after Context registration. The Actor mailbox queues Commands until restoration completes. GoalsRootActor registers and watches its children without awaiting their startup. Each child queues its own inputs during recovery, so one slow or failing Goal does not block its siblings. Supervision owns retries; terminal failures reach the root as `Terminated` and are logged. Runtime owns the integration readiness gate that delays execution, while restored Goals can already admit inputs. No Goal readiness or activation Commands are needed. Commands and local Agent tools share that same instance. GoalState serializes complete business transitions with a Semaphore, including validation, Pi handoff and snapshot persistence. A private Ref contains the committed GoalSnapshot; Store commits through ContextRegistry before updating the Ref, and the commit-to-Ref handoff drains despite interruption. ContextRegistry continues to publish durable Context changes; it is not a second business writer. Agent execution runs outside the writer and returns through `pipeToSelf` with generation checks. Interrupted work reconciles the original Pi request. Known failed inputs support explicit `RetryTurn`; unknown outcomes block later inputs until reconciled. Reply selection commits to Pi before the input settles in Actor state, so completed native exchanges can replay without another model call.

`update_summary` changes only the business summary. It resolves the current GoalState from the invocation Effect Context and cannot complete a Goal. There is no self-ask or summary command/reply protocol. End interrupts local conversation execution, deactivates owned Signals and cancels Tasks that have not started. Submitted work retains its Task owner; local interruption never proves an external cancellation.

## Context gate

System One independently matches every eligible Goal and Context Signal. A Goal then applies a separate read-only Agent gate only to Context evidence. Ignored evidence does not enter the persistent model conversation or public chat. User input, direct Task messages and execution feedback bypass this second gate.

Each Goal has one read-only screening slot independent of its single main-conversation slot. Screening can run while the conversation is busy, and user turns can run while screening is busy. Context inputs stay pending until their gate decision commits; relevant inputs then compete for the main slot rather than starting a conversation directly from the gate callback. This bounds concurrency to one gate plus one conversation per Goal.

Gate work belongs to the Actor Behavior scope and has its own generation and cancellation signal. End or completion cancels it and ignores pending inputs; late results cannot change an ended Goal. An interrupted gate remains pending and can be screened again after restart without treating a read-only check as an uncertain Pi delivery. Recorded gate decisions are reused. Gate failures settle only their own input and do not release the main-conversation slot.

Execution feedback carries Task path, status at emission and text; Goal resolves the original causal budget from the Task. Automatic feedback retains its original causal budget. A new conversation turn does not replenish it. Exhausted feedback can produce a visible notice without invoking another model.

## Implementation organization

```text
goals/
  actor.ts                 # Goal lifecycle and execution scheduling
  root.ts                  # Goal registration, routing and readiness
  protocol.ts              # Public requests, replies and internal mailbox messages
  agent.ts                 # Conversation, Context gate, prompts and response logging
  view.ts                  # Public Context projection and Pi conversation timeline
  state/
    model.ts               # Actor-local GoalState service and business transitions
    snapshot.ts            # Serializable snapshot and input-reference schemas
    store.ts               # Private persistence adapter and committed snapshot Ref
    admission.ts           # Request validation, deduplication and admission
    inputs.ts              # Pi input references, resolution and recovery
  screening/
    decision.ts            # System One relevance scoring and screening audit contracts
    intent.ts              # Frozen Context-change delivery envelopes
```

`actor.ts` constructs the `state/model.ts` Layer once in its Behavior Scope. Every command and local tool shares that instance. The state model owns business rules and serialization, while its private Store owns the committed Ref and persistence. Input schemas live in `state/snapshot.ts`, so decoding snapshots does not load input persistence operations.

`agent.ts` owns both model invocations: the primary conversation and the read-only Context gate. Their prompts, response logs and input-message rendering stay together. `screening/` owns the earlier System One decision and frozen delivery; this is distinct from the Agent's second gate.

`protocol.ts` contains public requests and internal result/scheduling messages. Callers import contracts from it and root registration from `root.ts`; `actor.ts` does not forward either module. The package entrypoint exposes public capabilities directly from their defining modules, without compatibility barrels or duplicate exports. The built-in personal Goal is a configuration default beside GoalSettings in `config/settings.ts`.

`view.ts` groups the public Context projection and the Pi-backed conversation timeline. Shared tools remain in `../tools/`: local Goal tools resolve GoalState, while cross-Actor operations use CurrentActors. Native transcript persistence and SDK callback retirement remain in `@aster/agent`.

Activation, readiness waiters and conversation/gate execution handles remain mailbox-owned Refs. Model invocation never holds the state writer, so new inputs can be accepted while an Agent is running. Local tools may briefly wait for an in-flight durable admission. AgentRunner aborts callbacks when their invocation finishes or is cancelled; GoalState rejects writes after its Layer closes. Ending the Goal and summary mutation share the same writer, so a summary either commits before End or observes the inactive state and fails. This refactor does not add a new pause/resume protocol: the existing End/completed lifecycle remains separate pending work.

The snapshot Ref is a committed mirror, not a second persistence system. Registry description initialization can update Context metadata independently using revision checks; all Goal business fields go through GoalState. Only durable Context publication drives external change consumers, so a SubscriptionRef would add a redundant notification channel. Pi remains the sole message store, and interrupted Pi-to-snapshot handoffs retain their existing recovery path.

## Related work

Task state lives at `/tasks/<identity>`, Signal state at `/signals/<slug>`, and execution transcripts in Pi. GoalSnapshot.tasks retains unique stable paths for Tasks created by or replying to the Goal, including completed Tasks that may continue. Creation acknowledges its caller only after Task admission and Goal attachment commit. Repeated attachment is idempotent; startup rebuilds the list from Task admission metadata if the cross-Actor handoff was interrupted, and feedback can also repair a missing reference. `goal_current` exposes these references; `task_list` resolves their current public Task views. Task lifecycle and message content remain owned by TaskActor and Pi. Direct messages to another Goal do not create a TaskActor or a synthetic Task reference. Signals freeze exact Task occurrences before delivery; receivers acknowledge durable admission. Runtime restores Task and Signal owners before activating Goals and producers.

See [conversation design](goal-conversation-design.md), [Tasks](task-delegation-design.md) and [runtime](runtime-design.md). There is no historical-data migration or compatibility protocol.
