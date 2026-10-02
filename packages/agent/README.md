# Agent

A thin Effect wrapper around pi 1.0.0. Message and tool types are re-exported from pi; integrations own prompts, tools and business-result validation. `pi-durable` 1.0.0 is available for the Goal Agent Session adapter, while the current wrapper still uses the compatible `pi-agent-core` runtime path.

```ts
import { Agent, Models } from "@aster/agent";
import { Effect } from "effect";

const program = Effect.gen(function* () {
  const agent = yield* Agent.make({ name: "summary-model", tools: [] });
  return yield* agent.run({
    messages: [
      { role: "system", content: "Summarize the supplied evidence.", timestamp: Date.now() },
      { role: "user", content: [{ type: "text", text: "Evidence…" }], timestamp: Date.now() },
    ],
  });
});

// The app supplies its named model configuration once.
// Effect.runPromise(program.pipe(Effect.provide(Models.layer(config.models))))
// ActorSystem.make().pipe(ActorSystem.provide(Models.layer(config.models)))
```

`Agent.make` resolves `name` through the Models Effect service. Every `run` creates a fresh pi instance, including concurrent runs on the same handle. Returned `{ messages }` contains only generated messages, not the supplied input. Business outputs can be returned in a native pi tool's `details`, with `terminate: true` when appropriate, then read from the resulting `toolResult` message.

A terminal pi error becomes `AgentError` with the generated messages. Ordinary tool failures remain in pi's loop and can be corrected by the model. Effect interruption calls `abort()` and waits for pi to become idle; it remains interruption rather than becoming an ordinary failure. Callers apply `Effect.timeout` for their own execution limits.

Models supports the existing MiniMax, Anthropic, and OpenAI chat-completions providers, with configured URL, model ID and credentials. Environment credential references are resolved when a model is selected. Neither this package nor its callers add a second message or tool protocol.

`Models.configured` reads the typed `config.models` array through Effect ConfigProvider. `Models.layer(entries)` remains available for explicit composition and tests. Both capture the provider during acquisition; exact `${ENV_VAR}` credentials resolve through its `secrets` namespace when a model is selected. The shared `secretConfig` retains values as `Redacted` until the SDK boundary. Unused model credentials are not required, and prompt text is never interpolated.
