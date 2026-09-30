# Shared Agent capability and integration-owned summarization

Status: implemented and verified.

## Accepted ownership direction

- Move chat summarization into `packages/lark-integration`. Its prompts, rolling-summary inputs, output validation, and summary-specific tools belong to Lark integration.
- Introduce a separate `packages/agent` package providing reusable Agent capabilities. Model-provider wiring and generic Agent execution belong here rather than in the app or Lark-specific code.
- The application supplies configured models. Integrations depend on the Agent capability, not application configuration types or application modules.
- The chat summarizer creates an Agent through `Agent.make({ name, tools })`, then supplies messages when calling `agent.run({ messages })`.

## Previous coupling

`apps/local/src/chat-summarizer.ts` previously owned Lark summary prompts and tools and imports the Lark summary contracts. `apps/local/src/model-agent.ts` wrapped pi and imported the app's ModelConfig. The application previously selected `config.goals.model` for both Goal planning and chat summarization, then injected ChatSummarizer directly. Moving only the files would leave model selection coupled to Goals; the new dependency boundary must address that explicitly.

## Accepted model name and execution boundary

`name` selects `config.models[].name`; it is not an Agent-role identifier and does not introduce a separate role-to-model mapping.

Construction and execution are separate:

```ts
const agent = yield * Agent.make({ name: "summary-model", tools });
const result = yield * agent.run({ messages });
```

Each run starts an independent conversation using its supplied messages. The Agent does not implicitly retain messages across runs. Chat continuity belongs to the Context's persisted rolling summary, which the Lark summarizer includes in the next run's input. Provider-specific conversation state must not leak from one run to another. The run result envelope is accepted below; message and tool types follow pi as accepted below.

## Accepted model provision

The app constructs a Models Layer from its configured models and provides it to ActorSystem. `Agent.make` resolves the selected model from that Effect service. Lark integration only supplies the configured model name and its own tools/messages; it does not load YAML, read app configuration types, or construct provider clients. The accepted provision shape is `ActorSystem.make().pipe(ActorSystem.provide(Models.layer(config.models)))`; `Agent.make` resolves its model by name from this environment.

## Accepted Lark model selection

Lark integration consumes its own model selection at `contexts./lark.children./im.config.summary.model`. This value references `config.models[].name` and is passed to `Agent.make` as `name`. Chat summarization no longer selects its model through `config.goals.model`.

```yaml
contexts:
  /lark:
    children:
      /im:
        config:
          summary:
            model: summary-model
```

## Accepted run result boundary

`agent.run({ messages: input })` returns `{ messages }`, containing the messages produced during that run: model replies, tool calls, and tool results. The returned list does not repeat the supplied input history.

The shared Agent package does not return business-specific types such as ChatSummary or GoalPlan. Lark defines and validates its `save_summary` tool result and obtains the summary from that run's tool-result messages. This keeps result ownership in the integration rather than adding business protocols to the generic Agent package.

## Accepted thin pi wrapper

The Agent package wraps pi rather than introducing a separate Agent protocol. It re-exports pi message and tool types for callers. Tools keep pi's native parameter schema, execution signature, and result conventions; a new Effect Schema-based tool protocol is not part of this design.

`Agent.make` and `agent.run` return Effects. Effect interruption aborts the underlying pi run through `abort()`. Integrations remain responsible for their own business prompts and output validation.

## Verified pi failure behavior

The installed pi runtime can finish its prompt Promise after encoding a model/runtime failure in an assistant message with `stopReason: "error"` or `"aborted"` and `errorMessage`. A resolved Promise alone therefore does not establish successful execution. The accepted Effect mapping is recorded below. Ordinary tool-error messages are distinct because pi can let the model recover from them within a run.

## Accepted failure and cancellation behavior

- Normal completion returns `{ messages }` containing only the current run's generated messages.
- A terminal model/runtime failure becomes an Effect failure carrying the error and the run's generated messages; a resolved pi Promise does not by itself count as success.
- Ordinary tool execution errors retain pi's native behavior so the model can correct them within the same run. A tool error is not automatically a terminal Agent failure.
- Effect interruption calls pi's `abort()` and remains an interruption rather than being converted into a successful result or ordinary failure.

The Lark summarizer keeps pending source messages when Agent execution fails or when no valid summary tool result is available. Failure mapping does not move business-result validation into the generic Agent package.

## Accepted migration scope and implementation

Both chat summarization and Goal reasoning use the shared Agent package. Lark owns `chat-summarizer.ts`, including the ChatSummarizer service Layer supplied by LarkIntegration. The Goal reasoner remains in the app and consumes the shared Agent interface. The app's previous `model-agent.ts` and `chat-summarizer.ts` source files are removed. Signal extraction and Context description initialization continue using Codex CLI; Doubao delegation is unchanged.

The app provides `Models.layer(config.models)` before integration and Goal Layers. Configuration uses the shared ModelConfig schema. Existing Promise-based domain interfaces capture the supplied Models service and run the Effect Agent with the caller's AbortSignal; timeouts are applied by each business adapter. Both summary and Goal outputs are validated from native pi tool-result `details`, without closure-based result state.

Each run creates a fresh pi instance. The wrapper ensures supplied tools are declared on a leading system message, preserves native message/tool types, maps terminal errors, and aborts/waits for the underlying run during interruption.

Verification: build and typecheck pass. The full suite passes 70 tests, including five Agent tests for isolation, terminal failure transcripts, recoverable tool errors/tool declarations, interruption, and unknown model selection. A synthetic live run completed Lark summarization, System One Goal matching and Goal planning through the new package; it did not install Signals or delegate an external task. Earlier System One relevance variability remains a separate known issue.

## Subsequent core/infrastructure split

Goal contracts and actors now belong to core. Goal reasoning uses Agent inside core, and the app assembles the runtime with concrete integrations. The Agent package remains the thin pi wrapper; Lark still owns chat summarization. See the core design's package ownership section for the current dependency direction.

Goal reasoning now lives in core/goals and uses the Agent package. Integrations contains only provider/transport adapters; it no longer owns a Goal runtime or business planning rules. The app assembles core runtime services with concrete adapters.

## Bounded Context discovery and structured completion

Goal planning and internal reasoning no longer embed every persisted Context description in the initial model input. They receive a count/root catalogue and `search_contexts` (20 bounded description snippets per page) plus `read_context` (12,000 JSON characters per page). The full snapshot stays local, accessible by path; pagination offsets preserve access to complete records. This prevents thousands of historical chats from exhausting the model context before it can submit a result.

`Agent.make({ resultTool })` requires a successful result-tool message. An ordinary prose completion gets at most one correction in the same conversation. Provider errors, interruption and `length` truncation fail explicitly; truncation reports token counts and is not blindly retried. Goal plans use `submit_plan`; internal extraction, description and Task preparation use `submit_result`. No free-form text is accepted as a structured plan, and this correction does not resubmit external Tasks.

Context stream errors are isolated inside the per-change handler. A failed description or evaluation is logged with its path; subsequent changes remain subscribed. Cancellation continues to propagate. A failed item itself is not automatically replayed.
