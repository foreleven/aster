import { Agent as PiAgent, type AgentMessage } from "@earendil-works/pi-agent-core";
import { toToolDeclaration } from "@earendil-works/pi-ai";
import { Effect } from "effect";
import { Models } from "../models.js";
import {
  AgentError,
  type AgentMessages,
  type AgentOptionsBase,
  type AgentResult,
} from "../shared/contracts.js";

export interface Agent {
  readonly run: (input: AgentMessages) => Effect.Effect<AgentResult, AgentError>;
}
export interface AgentOptions extends AgentOptionsBase {
  readonly resultTool?: string;
}

const make = Effect.fn("Agent.make")(function* (
  options: AgentOptions,
): Effect.fn.Return<Agent, AgentError, Models> {
  const models = yield* Models;
  const resolved = yield* models.resolve(options.name);
  const tools = [...(options.tools ?? [])];
  if (options.resultTool && !tools.some((tool) => tool.name === options.resultTool))
    return yield* Effect.fail(new AgentError(`Unknown result tool: ${options.resultTool}`));
  return {
    run: ({ messages }) =>
      Effect.suspend(() => {
        const generated: AgentMessage[] = [];
        return Effect.acquireUseRelease(
          Effect.try({
            try: () => {
              // A new pi instance per run also isolates concurrent invocations of this handle.
              const transcript = structuredClone([...messages]);
              const first = transcript[0];
              // pi takes tool declarations from an existing leading system message.
              if (first?.role === "system")
                first.toolsAdded = [
                  ...new Map(
                    [...(first.toolsAdded ?? []), ...tools.map(toToolDeclaration)].map((tool) => [
                      tool.name,
                      tool,
                    ]),
                  ).values(),
                ];
              const pi = new PiAgent({
                initialState: { model: resolved.model, tools, messages: transcript },
                streamFn: resolved.stream,
                getApiKey: resolved.getApiKey,
              });
              pi.subscribe(async (event, signal) => {
                if (event.type === "message_end") {
                  generated.push(event.message);
                  if (event.message.role === "assistant")
                    await options.onResponse?.(event.message, signal);
                }
              });
              return pi;
            },
            catch: (cause) => new AgentError("Agent initialization failed", [], { cause }),
          }),
          (pi) =>
            Effect.tryPromise({
              try: async (signal) => {
                const abort = () => pi.abort();
                signal.addEventListener("abort", abort, { once: true });
                try {
                  await pi.continue();
                  const checkTerminal = () => {
                    const terminal = generated.findLast((message) => message.role === "assistant");
                    if (
                      terminal?.role === "assistant" &&
                      ["error", "aborted", "length"].includes(terminal.stopReason)
                    ) {
                      throw new AgentError(
                        terminal.errorMessage ||
                          `Agent stopped: ${terminal.stopReason}; inputTokens=${terminal.usage.input + terminal.usage.cacheRead}, outputTokens=${terminal.usage.output}`,
                        [...generated],
                      );
                    }
                  };
                  const hasResult = () =>
                    generated.some(
                      (message) =>
                        message.role === "toolResult" &&
                        message.toolName === options.resultTool &&
                        !message.isError,
                    );
                  checkTerminal();
                  if (options.resultTool && !hasResult()) {
                    // One correction within this reasoning conversation, never an external Task resubmission.
                    await pi.prompt(
                      `The required structured result is missing. Submit it using ${options.resultTool}. Do not substitute prose. Correct any tool validation errors before submitting.`,
                    );
                    checkTerminal();
                    if (!hasResult())
                      throw new AgentError(
                        `Agent did not submit ${options.resultTool} after one correction`,
                        [...generated],
                      );
                  }
                  return { messages: [...generated] };
                } finally {
                  signal.removeEventListener("abort", abort);
                }
              },
              catch: (cause) =>
                cause instanceof AgentError
                  ? cause
                  : new AgentError(
                      cause instanceof Error ? cause.message : String(cause),
                      [...generated],
                      { cause },
                    ),
            }),
          (pi) =>
            Effect.promise(async () => {
              pi.abort();
              await pi.waitForIdle();
            }),
        );
      }),
  };
});

export const Agent = { make };
