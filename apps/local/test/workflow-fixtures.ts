import { AgentRunner } from "@aster/agent";
import { MemoryRecall, SystemOneClient } from "@aster/core";
import { ConfigProvider, Effect, Layer } from "effect";

export const personalDisabled = Layer.mergeAll(
  ConfigProvider.layer(ConfigProvider.fromUnknown({})),
  Layer.succeed(
    AgentRunner,
    AgentRunner.make(() => Effect.die("No model expected")),
  ),
);
export const taskExecutionLayer = Layer.mergeAll(
  ConfigProvider.layer(ConfigProvider.fromUnknown({ config: { agent: { model: "test" } } })),
  Layer.succeed(MemoryRecall, {
    search: () => Effect.succeed({ results: [] }),
    expand: () => Effect.succeed({ results: [] }),
  }),
  Layer.succeed(SystemOneClient, {
    systemOne: () => Effect.succeed({ answers: { executable: { type: "choice", choice: "yes" } } }),
  }),
  Layer.succeed(
    AgentRunner,
    AgentRunner.make(() =>
      Effect.succeed({
        messages: [
          {
            role: "toolResult",
            toolCallId: "result",
            toolName: "submit_result",
            content: [],
            details: { instructions: "Review the evidence", input: [] },
            isError: false,
            timestamp: 0,
          },
        ],
      }),
    ),
  ),
);
