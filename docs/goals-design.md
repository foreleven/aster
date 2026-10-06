# Goals

A Goal owns a natural conversation, business progress and routing to Tasks and Signals. The default personal assistant is the ordinary `/goals/personal` Goal and starts idle.

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

## State and recovery

GoalState contains `definition`, `status`, `summary`, `inputs` and `receipts`. Input records hold Pi references, input kind, delivery status, Context gate decision, causality, retry reference and error. Array order records admission order; timestamps come from Pi entries. Message bodies and response text are not duplicated in Actor state. Receipts retain normalized command fingerprints and the original acceptance revision.

Admission commits the message and receipt identity to Pi before the Actor saves its references and acknowledges. Startup recovers admitted entries before creating the configured initial pursuit. Exact retries return their existing receipts; changed reuse fails. One input enters the main conversation at a time. Later inputs are durably accepted while execution is in progress.

The Actor mailbox remains the single business-state writer. Agent work returns through `pipeToSelf` with generation checks. Interrupted work reconciles the original Pi request. Known failed inputs support explicit `RetryTurn`; unknown outcomes block later inputs until reconciled. Reply selection commits to Pi before the input settles in Actor state, so completed native exchanges can replay without another model call.

`update_goal` changes the business summary and optionally completes a Goal with evidence. End interrupts local conversation execution, deactivates owned Signals and cancels Tasks that have not started. Submitted work retains its Task owner; local interruption never proves an external cancellation.

## Context gate

System One independently matches every eligible Goal and Context Signal. A Goal then applies a separate read-only Agent gate only to Context evidence. Ignored evidence does not enter the persistent model conversation or public chat. User input, direct Task messages and execution feedback bypass this second gate.

Execution feedback carries Task path, status at emission and text; Goal resolves the original causal budget from the Task. Automatic feedback retains its original causal budget. A new conversation turn does not replenish it. Exhausted feedback can produce a visible notice without invoking another model.

## Related work

Task state lives at `/tasks/<identity>`, Signal state at `/signals/<slug>`, and execution transcripts in Pi. The Goal stores no duplicate Task or Signal lifecycle. Signals freeze exact Task occurrences before delivery; receivers acknowledge durable admission. Runtime restores Task and Signal owners before activating Goals and producers.

See [conversation design](goal-conversation-design.md), [Tasks](task-delegation-design.md) and [runtime](runtime-design.md). There is no historical-data migration or compatibility protocol.
