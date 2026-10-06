import {
  AgentRunner,
  Type,
  rejectedToolResult,
  type AgentTool,
  type AgentMessage,
  type TSchema,
} from "@aster/agent";
import { Effect, Clock } from "effect";
import { createHash } from "node:crypto";
import type { ApplicationError } from "@aster/api-contracts";
import type { GoalDefinition } from "../config/schema.js";
import type { ContextRegistry } from "../context/registry.js";
import type { MemoryRecall } from "../memory/contracts.js";
import type { ContextQueries } from "../context/queries.js";
import { contextTools } from "../reasoning/context-tools.js";
import { contextQueryTools } from "../reasoning/context-query-tools.js";
import type { TaskMessage } from "@aster/api-contracts";
import type { GoalSignalInput } from "../signals/goal-command.js";
import type { ResolvedGoalInput } from "./inputs.js";
import { inputMessage } from "./inputs.js";
import { goalAgentPrompt } from "./agent-prompt.js";

const output = (value: unknown) => ({
  content: [{ type: "text" as const, text: JSON.stringify(value) }],
  details: value,
});
const tool = <T extends TSchema>(value: AgentTool<T>) => value;
export interface GoalConversation {
  readonly goal: GoalDefinition;
  readonly model: string;
  readonly input: ResolvedGoalInput;
  readonly registry: ContextRegistry["Service"];
  readonly memory: MemoryRecall["Service"];
  readonly queries?: ContextQueries["Service"];
  readonly executors: readonly string[];
  readonly contextTokens?: number;
  readonly reserveTokens?: number;
  readonly reconcile: boolean;
  readonly update: (
    progress: string,
    completed: boolean,
    evidence: readonly string[],
  ) => Effect.Effect<unknown, ApplicationError>;
  readonly startTask: (input: TaskMessage) => Effect.Effect<unknown, ApplicationError>;
  readonly followupTask: (
    input: import("@aster/api-contracts").FollowupTaskInput,
  ) => Effect.Effect<unknown, ApplicationError>;
  readonly signal: (input: GoalSignalInput) => Effect.Effect<unknown, ApplicationError>;
}

/** One durable Pi conversation per Goal. Tools command peer owners; no plan application phase. */
export const runGoalConversation = Effect.fn("Goal.conversation")(function* (
  options: GoalConversation,
) {
  const runner = yield* AgentRunner;
  return yield* runner.run((invoke) =>
    Effect.gen(function* () {
      const { goal, input, registry } = options;
      const source = `/goals/${goal.slug}`;
      const identity = (callId: string) =>
        createHash("sha256")
          .update(JSON.stringify([source, input.inputId, callId]))
          .digest("hex");
      const causal = {
        rootRequestId: input.causal.rootRequestId,
        remainingAgentTurns: Math.max(0, input.causal.remainingAgentTurns - 1),
      };
      const mutation = (effect: Effect.Effect<unknown, ApplicationError>, signal?: AbortSignal) =>
        invoke(
          effect.pipe(
            Effect.map(output),
            Effect.catchTag("ApplicationError", (error) =>
              error.kind === "unavailable"
                ? Effect.fail(error)
                : Effect.succeed(rejectedToolResult(error.message)),
            ),
          ),
          signal,
        );
      const actorTask = Type.Union([
        Type.Object({
          _tag: Type.Literal("Goal"),
          target: Type.String({ pattern: "^/goals/[a-z0-9][a-z0-9-]*$" }),
          text: Type.String({ minLength: 1 }),
        }),
        Type.Object({
          _tag: Type.Literal("Agent"),
          replyTo: Type.String(),
          task: Type.Object({
            instructions: Type.String({ minLength: 1 }),
            input: Type.Array(
              Type.Object({ content: Type.String(), sources: Type.Array(Type.String()) }),
            ),
          }),
        }),
        Type.Object({
          _tag: Type.Literal("Delegate"),
          agent: Type.String(),
          replyTo: Type.String({ pattern: "^/goals/[a-z0-9][a-z0-9-]*$" }),
          task: Type.Object({
            instructions: Type.String({ minLength: 1 }),
            input: Type.Array(
              Type.Object({ content: Type.String(), sources: Type.Array(Type.String()) }),
            ),
          }),
        }),
      ]);
      const signalDefinition = Type.Object({
        trigger: Type.Union([
          Type.Object({ _tag: Type.Literal("Context"), when: Type.String({ minLength: 1 }) }),
          Type.Object({
            _tag: Type.Literal("Schedule"),
            schedule: Type.Union([
              Type.Object({ type: Type.Literal("once"), at: Type.String() }),
              Type.Object({
                type: Type.Literal("cron"),
                expression: Type.String(),
                timeZone: Type.String(),
              }),
            ]),
          }),
        ]),
        task: actorTask,
      });
      const reads = contextTools(registry.reader.snapshot(), 4000);
      const tools = [
        tool({
          ...reads[0],
          execute: (...args: Parameters<(typeof reads)[0]["execute"]>) =>
            contextTools(registry.reader.snapshot(), 4000)[0].execute(...args),
        }),
        tool({
          ...reads[1],
          execute: (...args: Parameters<(typeof reads)[1]["execute"]>) =>
            contextTools(registry.reader.snapshot(), 4000)[1].execute(...args),
        }),
        ...contextQueryTools(options.queries, invoke),
        tool({
          name: "goal_current",
          replay: "safe",
          label: "Read Goal",
          description: "Read current Goal state and available Task executors.",
          parameters: Type.Object({}),
          execute: async () =>
            output({
              goal,
              state: registry.views.project(registry.get(source)!).state,
              executors: options.executors,
            }),
        }),
        tool({
          name: "task_list",
          replay: "safe",
          label: "Read Tasks",
          description: "Read independent asynchronous Tasks started by this Goal.",
          parameters: Type.Object({}),
          execute: async () =>
            output(
              Object.values(registry.reader.snapshot()).filter(
                (record) =>
                  record.path.startsWith("/tasks/") &&
                  ((record.state as { sourcePath?: string }).sourcePath === source ||
                    (record.state as { replyTo?: string }).replyTo === source),
              ),
            ),
        }),
        tool({
          name: "signal_list",
          replay: "safe",
          label: "Read Signals",
          description: "Read Goal signals and timers, including their current revisions.",
          parameters: Type.Object({}),
          execute: async () =>
            output(
              Object.values(registry.reader.snapshot()).filter(
                (record) =>
                  record.path.startsWith("/signals/") &&
                  (record.state as { goal?: string }).goal === goal.slug,
              ),
            ),
        }),
        tool({
          name: "memory_search",
          replay: "safe",
          label: "Search memory",
          description: "Find recalled evidence.",
          parameters: Type.Object({ query: Type.String() }),
          execute: async (_id, args, signal) =>
            output(await invoke(options.memory.search(args.query), signal)),
        }),
        tool({
          name: "memory_expand",
          replay: "safe",
          label: "Expand memory",
          description: "Read original recalled evidence.",
          parameters: Type.Object({
            items: Type.Array(
              Type.Object({ obsId: Type.String(), sessionId: Type.Optional(Type.String()) }),
            ),
          }),
          execute: async (_id, args, signal) =>
            output(await invoke(options.memory.expand(args.items), signal)),
        }),
        tool({
          name: "update_goal",
          replay: "safe",
          label: "Update Goal",
          description:
            "Record a concise business summary. Complete only with evidence satisfying configured completion criteria.",
          parameters: Type.Object({
            progress: Type.String({ minLength: 1, maxLength: 6000 }),
            completed: Type.Boolean(),
            evidence: Type.Array(Type.String()),
          }),
          execute: (_id, args, signal) =>
            mutation(options.update(args.progress, args.completed, args.evidence), signal),
        }),
        tool({
          name: "start_task",
          replay: "never",
          label: "Start asynchronous Task",
          description:
            "Start sustained work with the internal Agent, send a message to a Goal, or delegate externally. Delegate tasks require user confirmation and reply to the specified Goal. Keep the same request identity on unknown outcomes.",
          parameters: Type.Object({ task: actorTask }),
          execute: (id, args, signal) => {
            const requestId = identity(id);
            return mutation(
              options.startTask({
                ...args,
                requestId,
                source,
                createdAt: input.receivedAt,
                causal,
              }),
              signal,
            );
          },
        }),
        tool({
          name: "task_send",
          replay: "never",
          label: "Continue Task",
          description:
            "Send a correction or follow-up to an existing Task. Completed Tasks can continue the same work.",
          parameters: Type.Object({ target: Type.String(), text: Type.String({ minLength: 1 }) }),
          execute: (id, args, signal) =>
            mutation(options.followupTask({ ...args, source, requestId: identity(id) }), signal),
        }),
        tool({
          name: "set_signal",
          replay: "never",
          label: "Manage Signal",
          description:
            "Create, change or delete a Goal signal or timer. It executes its configured Task when triggered. Read the current revision before changing an existing Signal.",
          parameters: Type.Object({
            id: Type.String({ pattern: "^[a-z0-9][a-z0-9-]*$" }),
            change: Type.Union([
              Type.Object({ operation: Type.Literal("create"), definition: signalDefinition }),
              Type.Object({
                operation: Type.Literal("update"),
                revision: Type.Integer(),
                definition: signalDefinition,
              }),
              Type.Object({ operation: Type.Literal("delete"), revision: Type.Integer() }),
            ]),
          }),
          execute: (id, args, signal) =>
            mutation(
              options.signal({
                requestId: identity(id),
                source,
                target: `/signals/${args.id.startsWith(`${goal.slug}--`) ? args.id : `${goal.slug}--${args.id}`}`,
                change: args.change,
                causal,
              }),
              signal,
            ),
        }),
      ];
      return {
        name: options.model,
        tools,
        durable: {
          sessionId: goal.slug,
          requestId: input.inputId,
          reconcile: options.reconcile,
          catalogueId: "aster.goal.conversation.v1",
          contextBudget: {
            contextTokens: options.contextTokens ?? 200000,
            reserveTokens: options.reserveTokens ?? 8192,
          },
        },
        messages: [
          {
            role: "system" as const,
            timestamp: yield* Clock.currentTimeMillis,
            content: goalAgentPrompt,
          },
          inputMessage(input),
        ],
      };
    }),
  );
});
export const conversationText = (messages: readonly AgentMessage[]) => {
  const last = messages.findLast((message) => message.role === "assistant");
  return last?.role === "assistant" && !last.content.some((block) => block.type === "toolCall")
    ? last.content.flatMap((block) => (block.type === "text" ? [block.text] : [])).join("\n\n")
    : "";
};
