import { BACKGROUND_CONTEXT, withAbortSignal } from "@earendil-works/chord/context";
import type { Context as NativeContext } from "@earendil-works/chord";
import {
  AssistantEntry,
  defineExtension,
  GenerationTask,
  hook,
  type Submission,
} from "@earendil-works/pi-durable";
import { createEditTool, createReadTool, createWriteTool } from "@earendil-works/pi-durable/tools";
import { Effect, Exit, Option, Ref, Schema } from "effect";
import { AgentModels } from "../models.js";
import { AgentError } from "../shared/contracts.js";
import { AgentConversations, conversationDriver } from "./conversations.js";
import {
  DurableContextBudget,
  type HarnessConversation,
  type HarnessOptions,
  type HarnessSubmission,
} from "./session-contracts.js";
import { durableTool } from "../shared/durable-tool.js";

const nativeRequestId = (id: string) => JSON.stringify(["aster.agent.input", id, "initial"]);
const native = <A>(operation: (context: NativeContext) => Promise<A>) =>
  Effect.tryPromise({
    try: (signal) => operation(withAbortSignal(signal, BACKGROUND_CONTEXT)),
    catch: (cause) =>
      new AgentError("Agent session operation failed", [], { cause, outcome: "unknown" }),
  });

/** Thin pi-durable session adapter. Pi owns checkpoint, replay and recovery. */
export const openSession = Effect.fn("AgentSession.open")(function* (options: HarnessOptions) {
  const models = yield* AgentModels;
  const model = yield* models.resolve(options.name);
  const budget = yield* Schema.decodeUnknownEffect(Schema.optional(DurableContextBudget))(
    options.contextBudget,
  ).pipe(
    Effect.mapError((cause) => new AgentError("Invalid durable context budget", [], { cause })),
  );
  if (budget && budget.reserveTokens >= Math.min(budget.contextTokens, model.contextWindow))
    return yield* Effect.fail(
      new AgentError("Durable output reserve must fit within the provider model window"),
    );
  const conversations = yield* AgentConversations;
  const driver = yield* conversations[conversationDriver](options.owner).pipe(
    Effect.mapError((cause) => new AgentError(cause.message, [], { cause, outcome: "unknown" })),
  );
  yield* driver.acquire.pipe(
    Effect.mapError((cause) => new AgentError(cause.message, [], { cause })),
  );
  const root = yield* native((context) => driver.harness.root(context));
  const closed = yield* Ref.make(false);
  const access = <A>(operation: (context: NativeContext) => Promise<A>) =>
    Ref.get(closed).pipe(
      Effect.flatMap((retired) =>
        retired ? Effect.fail(new AgentError("Agent session scope is closed")) : native(operation),
      ),
    );
  yield* Effect.addFinalizer((exit) =>
    Effect.gen(function* () {
      yield* Ref.set(closed, true);
      yield* native((context) =>
        Exit.isSuccess(exit) ? root.waitForIdle(context) : root.abort(context),
      ).pipe(Effect.orDie);
    }),
  );
  const extension = defineExtension({
    name: options.extensionName ?? `aster-tools:${options.owner}`,
    tools: (options.tools ?? []).map(durableTool),
    hooks: [
      hook(GenerationTask, {
        afterResponse: (message, _api, context) =>
          options.onResponse?.(message, context.abortSignal),
      }),
    ],
  });
  const workspace = defineExtension({
    name: "aster-workspace-tools",
    tools: [createReadTool(), createWriteTool(), createEditTool()],
  });
  const workspaceEnabled =
    options.workspaceId !== undefined && driver.workspaceEnvironment !== undefined;
  yield* Effect.sync(() => {
    for (const provider of models.models.getProviders()) driver.models.setProvider(provider);
    driver.registry.install(extension);
    if (workspaceEnabled) driver.registry.install(workspace);
    driver.settings.compaction = budget
      ? {
          enabled: true,
          reserveTokens: budget.reserveTokens,
          keepRecentTokens: Math.floor(
            (Math.min(budget.contextTokens, model.contextWindow) - budget.reserveTokens) / 2,
          ),
          backgroundTokens: 0,
        }
      : undefined;
  });
  const pending = yield* native((context) => driver.harness.inspect(context));
  if (!pending.submissions.some((record) => record.conversationId === root.id))
    yield* native((context) =>
      root.configure(
        {
          model: { provider: model.provider, modelId: model.id },
          instructions: options.instructions,
          extensions: workspaceEnabled ? [extension, workspace] : [extension],
          cwd: options.workspaceId ?? null,
        },
        context,
      ),
    );
  const wrap = (submission: Submission): HarnessSubmission => ({
    status: access((context) => submission.status(context)),
    wait: Effect.gen(function* () {
      const record = yield* access((context) => submission.wait(context));
      if (record.status === "unanswered")
        return yield* Effect.fail(
          new AgentError(
            `Durable submission failed: ${record.reason}${record.detail === undefined ? "" : `: ${typeof record.detail === "string" ? record.detail : JSON.stringify(record.detail)}`}`,
            [],
            { outcome: "failed" },
          ),
        );
      if (record.type !== "input" || record.status !== "done")
        return yield* Effect.fail(new AgentError("Expected a settled input submission"));
      const entry = yield* access((context) =>
        root.commit((tx) => tx.entry(AssistantEntry, record.answer), context),
      );
      if (!entry) return yield* Effect.fail(new AgentError("Native answer entry is missing"));
      return entry.model?.find((message) => message.role === "assistant");
    }),
  });
  return {
    submission: (requestId) =>
      access(async (context) => {
        const record = await root.commit(
          (tx) => tx.submissionByRequest(root.id, nativeRequestId(requestId)),
          context,
        );
        if (!record) return Option.none();
        const submission = await driver.harness.submission(record.id, context);
        return submission ? Option.some(wrap(submission)) : Option.none();
      }),
    submit: (input) =>
      access(async (context) =>
        wrap(
          await root.submit(
            { ...input, type: "input", requestId: nativeRequestId(input.requestId) },
            context,
          ),
        ),
      ).pipe(Effect.uninterruptible),
    abort: access((context) => root.abort(context)),
  } satisfies HarnessConversation;
});
