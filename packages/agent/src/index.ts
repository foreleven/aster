import {
  Agent as PiAgent,
  type AgentMessage,
  type AgentTool,
  type StreamFn,
} from "@earendil-works/pi-agent-core";
import {
  createModels,
  toToolDeclaration,
  type Model,
  type Api,
  type AssistantMessage,
} from "@earendil-works/pi-ai";
import { minimaxProvider } from "@earendil-works/pi-ai/providers/minimax";
import { anthropicProvider } from "@earendil-works/pi-ai/providers/anthropic";
import { openaiProvider } from "@earendil-works/pi-ai/providers/openai";
import { Config, ConfigProvider, Context, Effect, Layer, Redacted, Schema } from "effect";
import { withAgentCallbacks } from "./agent-callbacks.js";
import { secretConfig } from "./configuration.js";
import { AgentConversations } from "./conversations.js";
export { AgentConversations, ConversationError, ConversationEntry } from "./conversations.js";
import { PiStorageLease } from "./pi-storage-lease.js";
export { PiStorageLease, PiStorageLeaseError } from "./pi-storage-lease.js";
import {
  durableDirectory,
  runDurableAgent,
  DurableCloseFailure,
  DurableContextBudget,
  type DurableRunOptions,
} from "./durable.js";
import { DurableAgentFailure } from "./durable-error.js";
export { secretConfig } from "./configuration.js";
export { rejectedToolResult } from "./durable-tools.js";
import { nativeInvocation, type AgentRequest } from "./effect-tools.js";
export type { EffectTool, AgentRequest } from "./effect-tools.js";

export type {
  AgentMessage,
  AgentTool,
  AgentToolResult,
  StreamFn,
} from "@earendil-works/pi-agent-core";
export { Type } from "@earendil-works/pi-ai";
export type { TSchema, Message, AssistantMessage, ToolResultMessage } from "@earendil-works/pi-ai";

export const ModelConfig = Schema.Struct({
  name: Schema.String,
  provider: Schema.Literals(["minimax", "anthropic", "openai"]),
  model: Schema.String,
  url: Schema.String,
  apiKey: Schema.String,
});
export type ModelConfig = typeof ModelConfig.Type;

export class AgentError extends Error {
  readonly _tag = "AgentError";
  readonly outcome?: "failed" | "unknown";
  constructor(
    message: string,
    readonly messages: readonly AgentMessage[] = [],
    options?: ErrorOptions & { readonly outcome?: "failed" | "unknown" },
  ) {
    super(message, options);
    this.outcome = options?.outcome;
  }
}
export interface ResolvedModel {
  readonly model: Model<Api>;
  readonly stream: StreamFn;
  readonly getApiKey: () => string;
}
export class Models extends Context.Service<
  Models,
  {
    readonly resolve: (name: string) => Effect.Effect<ResolvedModel, AgentError>;
  }
>()("agent/Models") {
  static layer(configs: readonly ModelConfig[]) {
    return Layer.effect(
      Models,
      Effect.gen(function* () {
        const provider = yield* ConfigProvider.ConfigProvider;
        return yield* Effect.try({
          try: () => {
            const entries = new Map(configs.map((config) => [config.name, { ...config }]));
            if (entries.size !== configs.length) throw new AgentError("Model names must be unique");
            const models = createModels();
            models.setProvider(minimaxProvider());
            models.setProvider(anthropicProvider());
            models.setProvider(openaiProvider());
            return Models.of({
              resolve: (name) =>
                Effect.gen(function* () {
                  const config = entries.get(name);
                  if (!config) return yield* Effect.fail(new AgentError(`Unknown model: ${name}`));
                  const key = yield* secretConfig(config.apiKey, provider).pipe(
                    Effect.mapError(
                      (cause) => new AgentError("Model credential unavailable", [], { cause }),
                    ),
                  );
                  return yield* Effect.try({
                    try: () => {
                      return {
                        model: {
                          id: config.model,
                          name: config.name,
                          provider: config.provider,
                          api:
                            config.provider === "openai"
                              ? "openai-completions"
                              : "anthropic-messages",
                          baseUrl: config.url,
                          reasoning: false,
                          input: ["text"],
                          contextWindow: 200_000,
                          maxTokens: 8192,
                          cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
                        } satisfies Model<Api>,
                        stream: models.streamSimple.bind(models),
                        getApiKey: () => Redacted.value(key),
                      };
                    },
                    catch: (cause) =>
                      cause instanceof AgentError
                        ? cause
                        : new AgentError("Model resolution failed", [], { cause }),
                  });
                }),
            });
          },
          catch: (cause) =>
            cause instanceof AgentError
              ? cause
              : new AgentError("Models initialization failed", [], { cause }),
        });
      }),
    );
  }

  static readonly configured = Layer.unwrap(
    Config.schema(Schema.Array(ModelConfig), ["config", "models"]).pipe(
      Config.withDefault([]),
      Effect.map((configs) => Models.layer(configs)),
    ),
  );
}

export interface Agent {
  readonly run: (input: {
    readonly messages: readonly AgentMessage[];
  }) => Effect.Effect<{ readonly messages: readonly AgentMessage[] }, AgentError>;
}
export const Agent = {
  make: (options: {
    readonly name: string;
    readonly tools?: readonly AgentTool[];
    readonly resultTool?: string;
    readonly onMessage?: (message: AgentMessage) => Promise<void> | void;
    /** Live provider responses before tool execution; observational, not a durable acknowledgement. */
    readonly onResponse?: (message: AssistantMessage, signal?: AbortSignal) => Promise<void> | void;
    readonly transformContext?: (
      messages: AgentMessage[],
      signal?: AbortSignal,
    ) => Promise<AgentMessage[]>;
    readonly durable?: DurableRunOptions;
  }): Effect.Effect<Agent, AgentError, Models> =>
    Effect.gen(function* () {
      const models = yield* Models;
      const conversations = yield* Effect.serviceOption(AgentConversations);
      const resolved = yield* models.resolve(options.name);
      const requestedBudget = yield* Schema.decodeUnknownEffect(
        Schema.optional(DurableContextBudget),
      )(options.durable?.contextBudget).pipe(
        Effect.mapError((cause) => new AgentError("Invalid durable context budget", [], { cause })),
      );
      const contextBudget =
        requestedBudget === undefined
          ? undefined
          : {
              ...requestedBudget,
              contextTokens: Math.min(requestedBudget.contextTokens, resolved.model.contextWindow),
            };
      if (contextBudget && contextBudget.reserveTokens >= contextBudget.contextTokens)
        return yield* Effect.fail(
          new AgentError("Durable output reserve must fit within the provider model window"),
        );
      const tools = [...(options.tools ?? [])];
      if (options.resultTool && !tools.some((tool) => tool.name === options.resultTool))
        return yield* Effect.fail(new AgentError(`Unknown result tool: ${options.resultTool}`));
      return {
        run: ({ messages }) =>
          Effect.suspend(() => {
            const durable = options.durable;
            if (durable && conversations._tag === "Some")
              return conversations.value
                .driver(`/${durable.owner ?? "goals"}/${durable.sessionId}`)
                .pipe(
                  Effect.mapError(
                    (cause) =>
                      new AgentError("Conversation unavailable", [], { cause, outcome: "unknown" }),
                  ),
                  Effect.flatMap((driver) =>
                    driver
                      .exclusive(
                        Effect.acquireUseRelease(
                          Effect.sync(() => {
                            const controller = new AbortController();
                            const task = runDurableAgent({
                              resolved,
                              messages,
                              tools,
                              resultTool: options.resultTool,
                              onMessage: options.onMessage,
                              onResponse: options.onResponse,
                              transformContext: options.transformContext,
                              durable: { ...durable, contextBudget },
                              signal: controller.signal,
                              driver,
                            });
                            return { controller, task };
                          }),
                          ({ controller, task }) =>
                            Effect.tryPromise({
                              try: (signal) => {
                                const abort = () => controller.abort();
                                signal.addEventListener("abort", abort, { once: true });
                                return task.finally(() =>
                                  signal.removeEventListener("abort", abort),
                                );
                              },
                              catch: (cause) =>
                                new AgentError(
                                  cause instanceof Error ? cause.message : String(cause),
                                  [],
                                  {
                                    cause,
                                    outcome:
                                      cause instanceof DurableAgentFailure ? "failed" : "unknown",
                                  },
                                ),
                            }),
                          ({ controller, task }) =>
                            Effect.gen(function* () {
                              controller.abort();
                              const failed = yield* Effect.promise(() =>
                                task.then(
                                  () => false,
                                  (cause) => cause instanceof DurableCloseFailure,
                                ),
                              );
                              if (failed) yield* driver.quarantine;
                            }),
                        ),
                      )
                      .pipe(
                        Effect.catchTag("ConversationError", (cause) =>
                          Effect.fail(
                            new AgentError("Conversation unavailable", [], {
                              cause,
                              outcome: "unknown",
                            }),
                          ),
                        ),
                      ),
                  ),
                );
            if (durable)
              return Effect.scoped(
                Effect.gen(function* () {
                  const lease = yield* PiStorageLease.acquire(
                    durableDirectory(durable),
                    JSON.stringify([durable.owner ?? "goals", durable.sessionId]),
                  ).pipe(
                    Effect.mapError(
                      (cause) =>
                        new AgentError(
                          "Pi storage is owned by another process or unavailable",
                          [],
                          { cause, outcome: "unknown" },
                        ),
                    ),
                  );
                  return yield* Effect.acquireUseRelease(
                    Effect.sync(() => {
                      const controller = new AbortController();
                      const task = runDurableAgent({
                        resolved,
                        messages,
                        tools,
                        resultTool: options.resultTool,
                        onMessage: options.onMessage,
                        onResponse: options.onResponse,
                        transformContext: options.transformContext,
                        durable: {
                          ...durable,
                          contextBudget,
                          storageDirectory: lease.identity.directory,
                        },
                        signal: controller.signal,
                      });
                      return { controller, task };
                    }),
                    ({ task, controller }) =>
                      Effect.tryPromise({
                        try: (signal) => {
                          const abort = () => controller.abort();
                          signal.addEventListener("abort", abort, { once: true });
                          return task.finally(() => signal.removeEventListener("abort", abort));
                        },
                        catch: (cause) =>
                          cause instanceof AgentError
                            ? cause
                            : new AgentError(
                                cause instanceof Error ? cause.message : String(cause),
                                [],
                                {
                                  cause,
                                  outcome:
                                    cause instanceof DurableAgentFailure ? "failed" : "unknown",
                                },
                              ),
                      }),
                    ({ controller, task }) =>
                      Effect.gen(function* () {
                        controller.abort();
                        // The SDK wait can cancel before its owner closes. Join the complete
                        // invocation (including finally) before releasing this Effect's scope.
                        // Its failure is already delivered by the use phase; this only joins.
                        const closeFailed = yield* Effect.promise(() =>
                          task.then(
                            () => false,
                            (cause) => cause instanceof DurableCloseFailure,
                          ),
                        );
                        if (closeFailed) yield* lease.quarantine;
                      }),
                  );
                }),
              );
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
                        [...(first.toolsAdded ?? []), ...tools.map(toToolDeclaration)].map(
                          (tool) => [tool.name, tool],
                        ),
                      ).values(),
                    ];
                  const pi = new PiAgent({
                    initialState: { model: resolved.model, tools, messages: transcript },
                    streamFn: resolved.stream,
                    getApiKey: resolved.getApiKey,
                    transformContext: options.transformContext,
                  });
                  pi.subscribe(async (event) => {
                    if (event.type === "message_end") {
                      generated.push(event.message);
                      if (event.message.role === "assistant")
                        await options.onResponse?.(event.message);
                      await options.onMessage?.(event.message);
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
                        const terminal = generated.findLast(
                          (message) => message.role === "assistant",
                        );
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
    }),
};

export {
  PiDurableAgentRuntime,
  PiExecutionOwner,
  PiRuntimeError,
  PiExecutionHandle,
  PiExecutionResult,
  type PiExecutionStatus,
  type PiDurableRuntime,
  type PiExecutionEnvironment,
} from "./pi-runtime.js";

export type AgentInvocation = Parameters<typeof Agent.make>[0] & Parameters<Agent["run"]>[0];
export type AgentResult = Effect.Success<ReturnType<Agent["run"]>>;

/** Shared execution capability; each invocation owns a fresh Agent and its callback scope. */
export class AgentRunner extends Context.Service<
  AgentRunner,
  {
    readonly run: <E, R>(request: AgentRequest<E, R>) => Effect.Effect<AgentResult, AgentError, R>;
  }
>()("agent/Runner") {
  static readonly make = (
    execute: (invocation: AgentInvocation) => Effect.Effect<AgentResult, AgentError>,
  ): AgentRunner["Service"] => ({
    run: <E, R>(request: AgentRequest<E, R>) =>
      withAgentCallbacks<AgentResult, AgentError, R>((invoke) =>
        execute(nativeInvocation(request, invoke)),
      ),
  });

  static readonly layer = Layer.effect(
    AgentRunner,
    Effect.gen(function* () {
      const models = yield* Models;
      const conversations = yield* AgentConversations;
      return AgentRunner.make(
        Effect.fn("AgentRunner.run")(function* ({ messages, ...options }) {
          const agent = yield* Agent.make(options).pipe(
            Effect.provideService(Models, models),
            Effect.provideService(AgentConversations, conversations),
          );
          return yield* agent.run({ messages });
        }),
      );
    }),
  );
}
