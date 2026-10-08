# Agent execution

The package exposes two independent Effect capabilities:

| Import                 | Capability                        | Input and result                               | Dependencies               |
| ---------------------- | --------------------------------- | ---------------------------------------------- | -------------------------- |
| `@aster/agent/agent`   | `AgentRunner.run`, `Agent.make`   | Supplied transcript → generated messages       | Models                     |
| `@aster/agent/harness` | `DurableHarness.withConversation` | Native submission → persisted assistant answer | Models, AgentConversations |

Ordinary execution creates a fresh Pi Agent per invocation. Durable execution retains one Harness root per owner path, such as `/goals/personal` or `/tasks/<id>`. The package root exports shared tools, errors, message types and model configuration. Business prompts, input identities and evidence formatting belong to callers.

## Durable conversation API

```ts
const text =
  yield *
  DurableHarness.use((harness) =>
    harness.withConversation(
      { name: "reasoning-model", owner: "/goals/personal", instructions: "Analyze evidence." },
      (conversation) =>
        Effect.gen(function* () {
          const submission = yield* conversation.submit({
            requestId: "input-1",
            content: "Evidence…",
          });
          const answer = yield* submission.wait;
          return answer?.content;
        }),
    ),
  );
```

`withConversation` captures the caller's Effect Context and owns model/tool callbacks for its scope. A per-owner permit prevents two scopes from replacing each other's implementations. Short business message transactions remain independent of this permit. Tools and response observers return Effects, preserve their service requirements, and propagate defects through native SDK error recovery.

Both execution paths use `FiberSet.makeRuntimePromise` to bridge Effect callbacks into Pi. Callback fibers close before the SDK worker is interrupted, so SDK idle cleanup cannot wait on live callbacks. A separate defect notification preserves fatal failures even when Pi recovers a rejected tool Promise; ordinary typed tool errors retain Pi's recovery behavior.

The conversation exposes `submit`, `submission(requestId)` and `abort`. Submission handles expose `status` and `wait`. `submission` returns an Option without submitting or scheduling work. `submit` reuses an existing native receipt before admitting a new input. Pi's native submission table owns admission, queueing, execution status and answer references. Existing request keys remain `["aster.agent.input", requestId, "initial"]`. Callers must validate stable input identity: native deduplication does not compare changed payloads.

`wait` resumes queued or placed work through Pi, then reads the referenced assistant entry directly. Terminal status reads do not resume scheduling. An unanswered input maps to `AgentError` with `outcome: "failed"`; transport/storage failures remain unknown. Missing native answer references fail explicitly. A terminating tool round returns the native assistant tool-call entry; tool results remain available through conversation history. The adapter does not reconstruct, merge or sort a generated-message transcript.

Cancellation of an individual wait only cancels observation. `conversation.abort` explicitly aborts native work. When an owning scope fails or is interrupted, it aborts and drains the work it scheduled before releasing its callback environment. Successful scopes wait until their scheduled work is idle. Admission commits drain before scope cleanup; retired handles cannot admit new work. A cleanup failure quarantines the writer. Process crashes and native Harness close retain checkpoints according to Pi's recovery rules; an explicit abort instead persists an aborted outcome.

Model and extension implementations are installed once per scope. Pending submissions retain saved conversation configuration. Idle roots receive the supplied configuration for subsequent work. `contextBudget` validates and caps the model window; native blocking compaction handles transcript retention, with background compaction disabled for scoped execution.

## Native recovery and business ownership

Pi controls generation checkpoints, safe/unsafe tool replay, steering queues and compaction. Interrupted safe tools can replay; unsafe tools receive Pi's interrupted result without re-executing that call. There is no adapter-wide unknown-result fence or special `reconcile` mode that blocks every placed input. Operation-specific idempotency and unknown external outcomes belong to business tools and external executor adapters.

Goal submits its persisted input ID and uses the native answer to finish its business reply handoff. Task records delivery intent in its execution journal before submitting `whenBusy: "steer"`, and records native acceptance afterward. It seals steering admission, waits for each submission covered by the round, then commits its result. Recovery reuses those request identities. There is no application exchange ledger or separate steering document in the agent package.

`AgentConversations` owns scoped storage writers and business message operations: `append`, `find`, `read`, `get` and `tools`. Entries and their request index commit atomically. Message admission validates changed identity reuse. Known references use native entry lookup; history projections scan entries. The native driver is package-private. Execution configuration and callback ownership belong to DurableHarness.

`src/agent/` contains ordinary execution, `src/harness/` the native durable adapter and storage ownership, and `src/shared/` Effect callback/tool adaptation. Test implementations inject `AgentRunner.make(execute)` or `DurableHarness.make(openConversation)` independently. Production durable calls have no ordinary `AgentResult.messages` contract.

## Models and ordinary execution

Models supports MiniMax, Anthropic and OpenAI chat-completions providers. `Models.configured` reads typed `config.models` through Effect ConfigProvider. `Models.layer(entries)` supports explicit composition and tests. Credential references resolve through the captured provider's secrets namespace only when a model is selected, and remain redacted until the SDK boundary.

Ordinary Agent execution retains native message/tool types and supports one bounded result-tool correction. Terminal provider errors become typed AgentError failures with generated messages. Interruption aborts the ordinary Pi Agent and waits for it to become idle. `onResponse` observes model responses before their tool round; it is diagnostic, not a persistence acknowledgement.

AsterRuntime assembles both execution capabilities. Goal screening uses AgentRunner, Goal conversations and internal Tasks use DurableHarness, and Lark owns its ordinary Agent summarization flow.

## Local storage ownership

`AgentConversations.layer(root)` receives a resolved conversation directory from the host. The local host places it under `<config.durable.root>/conversations` and holds the existing root store lock for the entire runtime lifetime. Pi requires one process per storage; the agent package does not acquire a second filesystem lock or expose lease diagnostics.

Effect Scope closes each Harness before host ownership ends. A retired or quarantined writer rejects further access. A failed close is a defect, so the host retains its root lock until process exit when drain is uncertain. JSONL opens explicitly request fsync.
