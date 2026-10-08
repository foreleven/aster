import { BACKGROUND_CONTEXT, withAbortSignal } from "@earendil-works/chord/context";
import type { Context as NativeContext } from "@earendil-works/chord";
import {
  AgentDoc,
  AssistantEntry,
  defineExtension,
  GenerationTask,
  hook,
  type Submission,
} from "@earendil-works/pi-durable";
import { Effect, Exit, Option, Ref, Schema, Semaphore } from "effect";
import { Models } from "../models.js";
import { AgentError } from "../shared/contracts.js";
import { AgentConversations, conversationDriver } from "./conversations.js";
import {
  DurableContextBudget,
  type HarnessConversation,
  type HarnessOptions,
  type HarnessSubmission,
} from "./contracts.js";
import { durableModels, durableTool } from "./runtime.js";

// Keep existing native request identities when reopening a retained root.
const nativeRequestId = (id: string) => JSON.stringify(["aster.agent.input", id, "initial"]);
const native = <A>(operation: (context: NativeContext) => Promise<A>) =>
  Effect.tryPromise({
    try: (signal) => operation(withAbortSignal(signal, BACKGROUND_CONTEXT)),
    catch: (cause) =>
      new AgentError(cause instanceof Error ? cause.message : String(cause), [], {
        cause,
        outcome: "unknown",
      }),
  });

/** The scope owns code and callbacks; Pi owns submissions and scheduling. */
export const openConversation = Effect.fn("DurableHarness.open")(function* (
  options: HarnessOptions,
) {
  const models = yield* Models;
  const resolved = yield* models.resolve(options.name);
  const budget = yield* Schema.decodeUnknownEffect(Schema.optional(DurableContextBudget))(
    options.contextBudget,
  ).pipe(
    Effect.mapError((cause) => new AgentError("Invalid durable context budget", [], { cause })),
  );
  const contextTokens = Math.min(
    budget?.contextTokens ?? resolved.model.contextWindow,
    resolved.model.contextWindow,
  );
  if (budget && budget.reserveTokens >= contextTokens)
    return yield* Effect.fail(
      new AgentError("Durable output reserve must fit within the provider model window"),
    );
  const conversations = yield* AgentConversations;
  const driver = yield* conversations[conversationDriver](options.owner).pipe(
    Effect.mapError((cause) => new AgentError(cause.message, [], { cause, outcome: "unknown" })),
  );
  yield* driver.acquire.pipe(
    Effect.mapError((cause) => new AgentError(cause.message, [], { cause, outcome: "unknown" })),
  );
  const { harness } = driver;
  const root = yield* native((context) => harness.root(context));
  const closed = yield* Ref.make(false);
  const scheduled = yield* Ref.make(false);
  const admission = yield* Semaphore.make(1);
  const access = <A>(operation: (context: NativeContext) => Promise<A>) =>
    Ref.get(closed).pipe(
      Effect.flatMap((retired) =>
        retired ? Effect.fail(new AgentError("Conversation scope is closed")) : native(operation),
      ),
    );
  const abort = access((context) => root.abort(context));
  yield* Effect.addFinalizer((exit) =>
    Effect.gen(function* () {
      yield* Ref.set(closed, true);
      if (!(yield* Ref.get(scheduled))) return;
      // A cancelled wait is only an observation. Leaving its owning scope must
      // drain native work before the Effect callback environment can be released.
      yield* (
        Exit.isSuccess(exit)
          ? native((context) => root.waitForIdle(context))
          : native((context) => root.abort(context))
      ).pipe(
        Effect.tapError(() => driver.quarantine),
        Effect.orDie,
      );
    }).pipe(admission.withPermit),
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
  yield* Effect.sync(() => {
    driver.registry.install(extension);
    durableModels(
      { ...resolved, model: { ...resolved.model, contextWindow: contextTokens } },
      driver.models,
    );
    driver.settings.compaction = budget
      ? {
          enabled: true,
          reserveTokens: budget.reserveTokens,
          keepRecentTokens: Math.floor((contextTokens - budget.reserveTokens) / 2),
          backgroundTokens: 0,
        }
      : undefined;
  });
  // Restore implementations before resuming; pending work retains its saved configuration.
  const pending = yield* native((context) => harness.inspect(context));
  if (pending.submissions.some((record) => record.conversationId === root.id)) {
    const saved = yield* native((context) => harness.snapshot(AgentDoc, root.id, context));
    if (
      saved?.model?.provider !== resolved.model.provider ||
      saved.model.modelId !== resolved.model.id ||
      !Array.isArray(saved.extensions) ||
      !saved.extensions.includes(extension.name)
    )
      return yield* Effect.fail(
        new AgentError("Pending native work requires its saved model and extension", [], {
          outcome: "unknown",
        }),
      );
  } else
    yield* native((context) =>
      root.configure(
        {
          model: { provider: resolved.model.provider, modelId: resolved.model.id },
          instructions: options.instructions,
          extensions: [extension],
        },
        context,
      ),
    );
  const wrap = (submission: Submission): HarnessSubmission => ({
    status: access((context) => submission.status(context)),
    wait: Effect.gen(function* () {
      const current = yield* access((context) => submission.status(context));
      const record =
        current.status === "done" || current.status === "unanswered"
          ? current
          : yield* Ref.set(scheduled, true).pipe(
              Effect.andThen(access((context) => submission.wait(context))),
            );
      if (record.status === "unanswered")
        return yield* Effect.fail(
          new AgentError(
            `Durable submission failed: ${record.reason}${typeof record.detail === "string" ? `: ${record.detail}` : ""}`,
            [],
            { outcome: "failed" },
          ),
        );
      if (record.type !== "input" || record.status !== "done")
        return yield* Effect.fail(new AgentError("Expected a settled native input submission"));
      const entry = yield* access((context) =>
        root.commit((tx) => tx.entry(AssistantEntry, record.answer), context),
      );
      if (!entry)
        return yield* Effect.fail(
          new AgentError("Native answer entry is missing", [], { outcome: "unknown" }),
        );
      return entry.model?.find((message) => message.role === "assistant");
    }),
  });
  const submission = (requestId: string) =>
    access(async (context) => {
      const record = await harness.commit(
        (tx) => tx.submissionByRequest(root.id, nativeRequestId(requestId)),
        context,
      );
      if (!record) return Option.none<HarnessSubmission>();
      const handle = await harness.submission(record.id, context);
      if (!handle) throw new Error("Native submission is missing");
      return Option.some(wrap(handle));
    });
  return {
    submission,
    submit: (input) =>
      Effect.gen(function* () {
        const previous = yield* submission(input.requestId);
        if (Option.isSome(previous)) return previous.value;
        yield* Ref.set(scheduled, true);
        // Admission must finish its durable handoff before interruption is observed.
        return wrap(
          yield* access((context) =>
            root.submit(
              { ...input, type: "input", requestId: nativeRequestId(input.requestId) },
              context,
            ),
          ),
        );
      }).pipe(admission.withPermit, Effect.uninterruptible),
    abort,
  } satisfies HarnessConversation;
});
