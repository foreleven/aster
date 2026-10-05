import { AgentRunner, AgentError, type AgentInvocation, type AgentResult } from "@aster/agent";
import { ConfigProvider, Effect, Layer, Option, Schema } from "effect";
import { PersonalMessage, PersonalState, type PublicContext } from "@aster/api-contracts";
import {
  MemoryRecall,
  SystemOneClient,
  DecisionError,
  GoalSettings,
  GoalSignals,
  GoalHistoryStore,
  makeMemoryGoalHistory,
  type TaskExecution,
  type PersonalReasoner,
  type ContextRecord,
} from "../src/index.js";

export const emptyRecall = Layer.succeed(MemoryRecall, {
  search: () => Effect.succeed({ results: [] }),
  expand: () => Effect.succeed({ results: [] }),
});
export const reasoningConfig = ConfigProvider.layerAdd(
  ConfigProvider.fromUnknown({
    config: { agent: { model: "test" } },
  }),
);
export const agentResult = (toolName: string, details: unknown): AgentResult => ({
  messages: [
    {
      role: "toolResult",
      toolCallId: "test",
      toolName,
      details: Schema.decodeUnknownSync(Schema.Json)(details),
      content: [],
      isError: false,
      timestamp: 0,
    },
  ],
});
const agentFailure = (cause: Error) => new AgentError(cause.message, [], { cause });

/** Each fake handles one model protocol and delegates other invocations to the preceding fake. */
export const modelReplyLayer = (
  resultTool: string | undefined,
  execute: (invocation: AgentInvocation) => Effect.Effect<AgentResult, AgentError>,
) =>
  Layer.effect(
    AgentRunner,
    Effect.gen(function* () {
      const previous = yield* Effect.serviceOption(AgentRunner);
      return AgentRunner.of({
        run: (invocation) => {
          // The fallback also owns the callback scope of the invocation it executes.
          return AgentRunner.make((options) =>
            options.resultTool === resultTool
              ? execute(options)
              : Option.isSome(previous)
                ? previous.value.run(() => Effect.succeed(options))
                : Effect.die(new Error(`Unexpected model invocation: ${options.resultTool}`)),
          ).run(invocation);
        },
      });
    }),
  );

// The SDK tool boundary is exercised by the fake model, just like a real model tool call.
const callTool = (input: AgentInvocation, name: string, args: object) =>
  Effect.tryPromise({
    try: (signal) => input.tools!.find((tool) => tool.name === name)!.execute("test", args, signal),
    catch: (cause) => new AgentError(String(cause), [], { cause }),
  }).pipe(
    Effect.map((result) => {
      const content = result.content.find((part) => part.type === "text");
      return JSON.parse(content?.type === "text" ? content.text : "null");
    }),
  );
const readPages = (input: AgentInvocation, name: string, args: object = {}) =>
  Effect.gen(function* () {
    let text = "";
    let offset: number | null = 0;
    while (offset !== null) {
      const page: { content: string; items: PublicContext[]; nextOffset: number | null } =
        yield* callTool(input, name, { ...args, offset });
      if (page.nextOffset === undefined)
        return yield* Effect.die(new Error(`Bad page from ${name}: ${JSON.stringify(page)}`));
      text += page.content;
      offset = page.nextOffset;
    }
    return JSON.parse(text);
  });
const readContexts = (input: AgentInvocation) =>
  Effect.gen(function* () {
    const records: Record<string, ContextRecord> = {};
    let offset: number | null = 0;
    while (offset !== null) {
      const page: { content: string; items: PublicContext[]; nextOffset: number | null } =
        yield* callTool(input, "search_contexts", { query: "", offset });
      for (const item of page.items)
        records[item.path] = yield* readPages(input, "read_context", { path: item.path });
      offset = page.nextOffset;
    }
    return records;
  });

export interface TaskScenario {
  readonly prepare: TaskExecution["buildExecutionInput"];
  readonly ready: TaskExecution["checkReadiness"];
}
export const taskExecutionLayer = (scenario: TaskScenario) =>
  Layer.mergeAll(
    reasoningConfig,
    emptyRecall,
    modelReplyLayer("submit_result", (input) =>
      Effect.gen(function* () {
        const user = input.messages.find((message) => message.role === "user")!;
        const text =
          typeof user.content === "string"
            ? user.content
            : user.content.map((part) => (part.type === "text" ? part.text : "")).join("");
        const evidence = JSON.parse(
          text.split("\n").find((line) => line.startsWith('{"signal":'))!,
        );
        const contexts = yield* readContexts(input);
        const result = yield* scenario
          .prepare(evidence.signal, evidence.source, contexts)
          .pipe(Effect.mapError(agentFailure));
        return agentResult("submit_result", result);
      }),
    ),
    Layer.succeed(SystemOneClient, {
      systemOne: (input) =>
        Effect.gen(function* () {
          const { signal, context, task } = JSON.parse(input.state as string);
          const ready = yield* scenario
            .ready(signal, context, task)
            .pipe(Effect.mapError((cause) => new DecisionError({ message: cause.message, cause })));
          return { answers: { executable: { type: "choice", choice: ready ? "yes" : "no" } } };
        }),
    }),
  );
export const personalDisabled = Layer.mergeAll(
  reasoningConfig,
  Layer.effect(
    AgentRunner,
    Effect.serviceOption(AgentRunner).pipe(
      Effect.map((previous) =>
        Option.getOrElse(previous, () => AgentRunner.make(() => Effect.die("No model expected"))),
      ),
    ),
  ),
);
export const personalReasoningLayer = (scenario: PersonalReasoner) =>
  scenario.enabled
    ? Layer.mergeAll(
        ConfigProvider.layerAdd(
          ConfigProvider.fromUnknown({ config: { personal: { model: "test" } } }),
          { asPrimary: true },
        ),
        modelReplyLayer("submit_reply", (input) =>
          Effect.gen(function* () {
            const read = (path: string) =>
              readPages(input, "read_context", { path }).pipe(
                Effect.orDie,
              ) as Effect.Effect<PublicContext>;
            const personal = yield* read("/personal");
            const state = Schema.decodeUnknownSync(PersonalState)(personal.state);
            const messages = Schema.decodeUnknownSync(Schema.Array(PersonalMessage))(
              personal.messages,
            );
            const message = messages.find(
              (message) =>
                message.requestId === state.pendingRequestIds[0] && message.target === "/personal",
            )!;
            const result = yield* scenario
              .run(
                message,
                {
                  read,
                  list: Effect.gen(function* () {
                    const records: PublicContext[] = [];
                    let offset: number | null = 0;
                    while (offset !== null) {
                      const page: {
                        content: string;
                        items: PublicContext[];
                        nextOffset: number | null;
                      } = yield* callTool(input, "list_contexts", { offset }).pipe(Effect.orDie);
                      records.push(...page.items);
                      offset = page.nextOffset;
                    }
                    return records;
                  }),
                  executors: callTool(input, "list_executors", {}).pipe(Effect.orDie),
                  inspectDelegation: (path) =>
                    callTool(input, "inspect_delegation", { path }).pipe(Effect.orDie),
                },
                JSON.parse(input.durable!.requestId)[1],
              )
              .pipe(Effect.mapError(agentFailure));
            return agentResult("submit_reply", result);
          }),
        ),
      )
    : personalDisabled;

export interface GoalScenario {
  readonly definitions: GoalSettings["Service"]["definitions"];
  readonly reasoner: {
    readonly plan: (input: {
      current: ContextRecord;
      messages: readonly import("@aster/agent").AgentMessage[];
    }) => Effect.Effect<
      { progress: string; completed: boolean; evidence: readonly string[] },
      Error
    >;
  };
  readonly signals: GoalSignals["Service"]["signals"];
  readonly reconcile: GoalSignals["Service"]["reconcile"];
  readonly deactivate: GoalSignals["Service"]["deactivate"];
  readonly history?: GoalHistoryStore["Service"];
  readonly contextTokens?: number;
  readonly reserveTokens?: number;
}
export const goalWorkflowLayer = (scenario: GoalScenario) =>
  Layer.mergeAll(
    emptyRecall,
    Layer.succeed(GoalSettings, {
      definitions: scenario.definitions,
      reasoning: {
        model: "test",
        contextTokens: scenario.contextTokens,
        reserveTokens: scenario.reserveTokens,
      },
    }),
    Layer.succeed(GoalSignals, {
      ...scenario,
      applySignal: () => Effect.die("Unexpected Signal mutation"),
    }),
    Layer.succeed(GoalHistoryStore, scenario.history ?? makeMemoryGoalHistory()),
    modelReplyLayer("submit_relevance", () =>
      Effect.succeed(
        agentResult("submit_relevance", { relevant: true, reason: "Relevant test evidence" }),
      ),
    ),
    modelReplyLayer(undefined, (input) =>
      Effect.gen(function* () {
        const current = yield* callTool(input, "goal_current", {});
        const response = yield* scenario.reasoner
          .plan({
            current: {
              path: `/goals/${current.goal.slug}`,
              description: current.goal.description,
              state: current.state,
              messages: [],
            },
            messages: input.messages.filter((message) => message.role !== "system"),
          })
          .pipe(Effect.mapError(agentFailure));
        yield* callTool(input, "update_goal", {
          progress: response.progress,
          completed:
            response.completed && !!current.goal.completionCriteria && !!response.evidence.length,
          evidence: response.evidence,
        });
        return { messages: [] };
      }),
    ),
  );
