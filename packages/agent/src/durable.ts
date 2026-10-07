import { DurableSteers } from "./durable-steers.js";
import { isJsonValue, type Context as ChordContext, type JsonValue } from "@earendil-works/chord";
import { BACKGROUND_CONTEXT, withAbortSignal } from "@earendil-works/chord/context";
import {
  createModels,
  createProvider,
  lazyStream,
  type ProviderStreams,
  type SimpleStreamOptions,
  type AssistantMessage,
} from "@earendil-works/pi-ai";
import {
  defineExtension,
  defineTool,
  GenerationTask,
  CompactionTask,
  type Harness,
  type EntryId,
  hook,
  type Cursor,
  type EntryRecord,
  type EntryQuery,
  type Extension,
  type ToolRegistration,
} from "@earendil-works/pi-durable";
import { createHash } from "node:crypto";
import { Schema } from "effect";
import { admitExchange, completeExchange, lookupExchange } from "./durable-exchange.js";
import { DurableAgentFailure } from "./durable-error.js";
import { entriesFor, fenceTools, generationFence, hasUnknownToolOutcome } from "./durable-tools.js";
import type { AgentMessage, AgentTool, AgentToolResult } from "@earendil-works/pi-agent-core";
import type { ConversationDriver } from "./conversations.js";
import type { ResolvedModel } from "./index.js";

const durableContext = (signal?: AbortSignal): ChordContext =>
  signal ? withAbortSignal(signal, BACKGROUND_CONTEXT) : BACKGROUND_CONTEXT;

const jsonDetails = (value: unknown): JsonValue | undefined => {
  if (value === undefined) return undefined;
  return isJsonValue(value) ? value : undefined;
};

const toolResult = (
  result: AgentToolResult,
  api: { output: (chunk: string | Uint8Array) => void },
) => {
  for (const item of result.content) if (item.type === "text") api.output(item.text);
  const details = jsonDetails(result.details);
  return {
    content: result.content,
    ...(details === undefined ? {} : { details }),
    ...(result.isError === undefined ? {} : { isError: result.isError }),
    ...(result.usage === undefined ? {} : { usage: result.usage }),
    ...(result.terminate ? { control: { terminate: true as const } } : {}),
  };
};

export const durableTool = (tool: AgentTool): ToolRegistration =>
  defineTool({
    name: tool.name,
    description: tool.description,
    parameters: tool.parameters,
    replay: tool.replay === "safe" ? "safe" : "unsafe",
    prepareArguments: tool.prepareArguments,
    execute: async (args, api, context) => {
      const result = await tool.execute(api.callId, args, context.abortSignal, (partial) => {
        for (const item of partial.content) if (item.type === "text") api.output(item.text);
      });
      return toolResult(result, api);
    },
  });

export const durableModels = (
  resolved: ResolvedModel,
  beforeModel?: (signal?: AbortSignal) => Promise<void>,
  models = createModels(),
) => {
  const providerStreams: ProviderStreams = {
    stream: (model, context, options) =>
      lazyStream(model, async () => {
        await beforeModel?.(options?.signal);
        return resolved.stream(resolved.model, context, options);
      }),
    streamSimple: (model, context, options?: SimpleStreamOptions) =>
      lazyStream(model, async () => {
        await beforeModel?.(options?.signal);
        return resolved.stream(resolved.model, context, options);
      }),
  };
  models.setProvider(
    createProvider({
      id: resolved.model.provider,
      name: resolved.model.provider,
      models: [resolved.model],
      auth: {
        apiKey: {
          name: "Aster model credential",
          resolve: async () => ({ auth: { apiKey: resolved.getApiKey() } }),
        },
      },
      api: providerStreams,
    }),
  );
  return models;
};

const inputText = (messages: readonly AgentMessage[]) =>
  JSON.stringify({
    evidence: messages.filter((message) => message.role !== "system"),
    instruction:
      "Process this newly committed input according to your instructions. Use the available tools and submit any required structured result.",
  });

export const DurableContextBudget = Schema.Struct({
  contextTokens: Schema.Int.check(Schema.isGreaterThan(0)),
  reserveTokens: Schema.Int.check(Schema.isGreaterThan(0)),
}).check(
  Schema.makeFilter((budget) => budget.reserveTokens < budget.contextTokens, {
    expected: "Output reserve smaller than the effective model context window",
  }),
);

export interface DurableRunOptions {
  /** Bump when tool behavior change. */
  readonly catalogueId?: string;
  /** Recover saved work without issuing a new provider request for an accepted exchange. */
  readonly reconcile?: boolean;
  readonly owner?: "goals" | "tasks";
  readonly sessionId: string;
  readonly requestId: string;
  /** Token budget for native compaction, capped to the provider model window. */
  readonly contextBudget?: typeof DurableContextBudget.Type;
}

export class DurableCloseFailure extends Error {
  constructor(cause: unknown) {
    super("Pi writer shutdown is uncertain; storage ownership retained", { cause });
  }
}
const abortDurableConversation = async (harness: Harness) => {
  try {
    const conversation = await harness.root(BACKGROUND_CONTEXT);
    await conversation.abort(BACKGROUND_CONTEXT);
    await conversation.waitForIdle(BACKGROUND_CONTEXT);
  } catch (cause) {
    throw new DurableCloseFailure(cause);
  }
};

export const runDurableAgent = async (input: {
  readonly resolved: ResolvedModel;
  readonly messages: readonly AgentMessage[];
  readonly tools: readonly AgentTool[];
  readonly onResponse?: (message: AssistantMessage, signal?: AbortSignal) => Promise<void> | void;
  readonly durable: DurableRunOptions;
  readonly signal?: AbortSignal;
  readonly driver: ConversationDriver;
}) => {
  const context = durableContext(input.signal);
  const budget = input.durable.contextBudget;
  const resolved = budget
    ? { ...input.resolved, model: { ...input.resolved.model, contextWindow: budget.contextTokens } }
    : input.resolved;
  const { harness, registry, models, settings } = input.driver;
  const unsafeTools = new Set(
    input.tools.filter((tool) => tool.replay !== "safe").map((tool) => tool.name),
  );
  const fence = generationFence(() => harness, unsafeTools);
  const extension: Extension = defineExtension({
    name: `aster-${input.durable.owner === "tasks" ? "task" : "goal"}-tools:${input.durable.sessionId}`,
    tools: fenceTools(input.tools.map(durableTool), unsafeTools),
    hooks: [
      hook(GenerationTask, {
        beforeRequest: fence.beforeRequest,
        afterResponse: (message, _api, context) => input.onResponse?.(message, context.abortSignal),
      }),
      hook(CompactionTask, { beforeCompact: fence.beforeCompact }),
    ],
  });
  registry.install(extension);
  let observingAccepted = false;
  let providerBlocked = false;
  let completed = false;
  durableModels(
    resolved,
    async (signal) => {
      if (observingAccepted) {
        providerBlocked = true;
        throw new Error(
          "Accepted Agent outcome is uncertain; reconciliation cannot issue another provider request",
        );
      }
      await fence.beforeModel(signal);
    },
    models,
  );
  // Native blocking compaction remains owned by this invocation.
  settings.compaction = budget
    ? {
        enabled: true,
        reserveTokens: budget.reserveTokens,
        keepRecentTokens: Math.floor((budget.contextTokens - budget.reserveTokens) / 2),
        backgroundTokens: 0,
      }
    : undefined;
  try {
    const conversation = await harness.root(context, {
      agent: {
        model: { provider: input.resolved.model.provider, modelId: input.resolved.model.id },
        extensions: [extension],
      },
    });
    const system = input.messages.find((message) => message.role === "system");
    const instructions = system
      ? typeof system.content === "string"
        ? system.content
        : JSON.stringify(system.content)
      : "";
    const content = inputText(input.messages);
    // Pi deduplicates request IDs by submission type only. Aster additionally
    // freezes the complete input/configuration before any scheduler is resumed.
    const existing = input.durable.reconcile
      ? await lookupExchange(
          harness,
          JSON.stringify([input.durable.owner ?? "goals", input.durable.sessionId]),
          input.durable.requestId,
          context,
        )
      : undefined;
    const submitted =
      existing &&
      (await harness.commit(
        (tx) =>
          tx.submissionByRequest(
            conversation.id,
            JSON.stringify(["aster.agent.input", input.durable.requestId, "initial"]),
          ),
        context,
      ));
    observingAccepted =
      submitted !== undefined && submitted !== null && submitted.status !== "queued";
    const admission =
      existing?.resultEntries !== null && existing?.resultEntries !== undefined
        ? { entryIds: existing.resultEntries, error: existing.error }
        : await admitExchange(
            harness,
            {
              conversationId: conversation.id,
              identity: JSON.stringify([input.durable.owner ?? "goals", input.durable.sessionId]),
              requestId: input.durable.requestId,
              input: JSON.stringify({
                content,
                instructions,
                catalogueId: input.durable.catalogueId ?? "aster.agent.v2",
                contextBudget: budget,
                configuration: createHash("sha256")
                  .update(
                    JSON.stringify({
                      model: input.resolved.model,
                      tools: input.tools.map(({ name, description, parameters, replay }) => ({
                        name,
                        description,
                        parameters,
                        replay,
                      })),
                    }),
                  )
                  .digest("hex"),
              }),
              instructions,
              model: { provider: input.resolved.model.provider, modelId: input.resolved.model.id },
              extension,
            },
            context,
          );
    if (admission.error !== null) throw new DurableAgentFailure({ message: admission.error });
    const resultMessages = (ids: readonly number[]) =>
      harness.commit(async (tx) => {
        const messages: AgentMessage[] = [];
        for (const id of ids) {
          const entry = await tx.entry(id as EntryId);
          if (entry?.conversationId !== conversation.id)
            throw new Error("Durable result references missing transcript entries");
          messages.push(
            ...(entry.model ?? []).filter(
              (message) => message.role !== "user" && message.role !== "system",
            ),
          );
        }
        return messages;
      }, context);
    if (admission.entryIds !== null) {
      const messages = await resultMessages(admission.entryIds);
      completed = true;
      return { messages };
    }
    const assertKnownOutcome = async () => {
      const unknown = await harness.commit(
        async (tx) => hasUnknownToolOutcome(await entriesFor(tx, conversation.id), unsafeTools),
        context,
      );
      if (unknown)
        throw new Error(
          "Tool outcome is unknown; reconcile the retained operation before further Agent work.",
        );
    };
    await assertKnownOutcome();
    const submission = await conversation.submit(
      {
        type: "input",
        content,
        requestId: JSON.stringify(["aster.agent.input", input.durable.requestId, "initial"]),
        whenBusy: "followUp",
      },
      context,
    );
    const readEntries = async (query: Omit<EntryQuery, "conversationId">) => {
      const entries: EntryRecord[] = [];
      let cursor: Cursor | undefined;
      do {
        const page = await conversation.entries(query, 100, cursor, context);
        entries.push(...page.items);
        cursor = page.next;
      } while (cursor);
      return entries.reverse();
    };
    const collect = async (currentSubmission: Awaited<ReturnType<typeof conversation.submit>>) => {
      const settled = await currentSubmission.wait(context);
      if (providerBlocked)
        throw new Error("Agent outcome requires reconciliation; no provider request was retried");
      await assertKnownOutcome();
      if (settled.status === "unanswered") {
        const detail = typeof settled.detail === "string" ? `: ${settled.detail}` : "";
        const error = `Durable agent submission failed: ${settled.reason}${detail}`;
        await completeExchange(
          harness,
          {
            conversationId: conversation.id,
            requestId: input.durable.requestId,
            entryIds: [],
            error,
          },
          context,
        );
        throw new DurableAgentFailure({ message: error });
      }
      const entries = await readEntries({ minEntryId: settled.entry, maxEntryId: settled.answer });
      if (
        !entries.some((entry) => entry.id === settled.entry) ||
        !entries.some((entry) => entry.id === settled.answer)
      )
        throw new Error("Durable submission entry is missing from the transcript");
      const resultEntryIds = entries
        .filter((entry) => entry.id !== settled.entry && entry.model?.length)
        .map((entry) => Number(entry.id));
      // With terminate:true, Pi's answer is the assistant tool-call entry, not
      // the result. Include only that final round, stopping at the next input
      // or assistant so a replay never consumes another run's results.
      const answer = entries.find((entry) => entry.id === settled.answer);
      const calls = new Set(
        answer?.model?.flatMap((message) =>
          message.role === "assistant"
            ? message.content.flatMap((part) => (part.type === "toolCall" ? [part.id] : []))
            : [],
        ),
      );
      if (calls.size) {
        const tail = await readEntries({ minEntryId: settled.answer });
        for (const { message, entryId } of tail
          .filter((entry) => entry.id !== settled.answer)
          .flatMap((entry) =>
            (entry.model ?? []).map((message) => ({ message, entryId: Number(entry.id) })),
          )) {
          if (message.role === "user" || message.role === "assistant") break;
          if (message.role === "toolResult" && calls.delete(message.toolCallId)) {
            resultEntryIds.push(entryId);
          }
          if (!calls.size) break;
        }
        if (calls.size)
          throw new Error("Durable terminal tool results are missing from the transcript");
      }
      return resultEntryIds;
    };
    const steering: Array<Promise<Awaited<ReturnType<typeof conversation.submit>>>> = [];
    const admitSteering = (requestId: string, text: string, steerContext: typeof context) => {
      const pending = (async () => {
        await harness.commit(async (tx) => {
          const doc = await tx.doc(DurableSteers, conversation.id);
          const existing = doc.inputs.find((item) => item.requestId === requestId);
          if (existing && (existing.text !== text || existing.parent !== input.durable.requestId))
            throw new Error("Steering identity conflicts with its admitted input");
          if (!existing) {
            doc.inputs.push({ requestId, text, parent: input.durable.requestId });
            await tx.appendEntry(conversation.id, {
              kind: "app.aster.message",
              data: {
                requestId: `steer:${requestId}`,
                kind: "task.steer",
                data: { requestId, parent: input.durable.requestId },
                at: new Date().toISOString(),
              },
            });
          }
        }, steerContext);
        return conversation.submit(
          { type: "input", content: text, requestId: `steer:${requestId}`, whenBusy: "steer" },
          steerContext,
        );
      })();
      steering.push(pending);
      return pending;
    };
    const retainedSteers = await harness.commit(
      async (tx) =>
        (await tx.doc(DurableSteers, conversation.id)).inputs.filter(
          (item) => item.parent === input.durable.requestId,
        ),
      context,
    );
    for (const item of retainedSteers) admitSteering(item.requestId, item.text, context);
    input.driver.steering = admitSteering;
    const resultEntryIds = await collect(submission);
    // Closing admission is synchronous with observing an empty batch. A follow-up
    // admitted during collection is included, even if Pi started another generation.
    for (let index = 0; index < steering.length; index++) {
      const next = await collect(await steering[index]!);
      resultEntryIds.push(...next);
    }
    delete input.driver.steering;
    const ids = [...new Set(resultEntryIds)].sort((a, b) => a - b);
    const messages = await resultMessages(ids);
    await completeExchange(
      harness,
      {
        conversationId: conversation.id,
        requestId: input.durable.requestId,
        entryIds: ids,
      },
      context,
    );
    completed = true;
    return { messages };
  } finally {
    delete input.driver.steering;
    // Shutdown uncertainty takes precedence over an ordinary run failure: the
    // enclosing Effect must quarantine ownership even if a result was committed.
    if (!completed) await abortDurableConversation(harness);
  }
};
