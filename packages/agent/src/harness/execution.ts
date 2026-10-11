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
import { createEditTool, createReadTool, createWriteTool } from "@earendil-works/pi-durable/tools";
import { Effect, Exit, Option, Ref, Schema, Semaphore } from "effect";
import { Models } from "../models.js";
import { AgentError, ToolInvocationFailure } from "../shared/contracts.js";
import { AgentConversations, conversationDriver } from "./conversations.js";
import {
  DurableContextBudget,
  type HarnessConversation,
  type HarnessOptions,
  type HarnessSubmission,
} from "./contracts.js";
import { durableModels, durableTool } from "./runtime.js";
import {
  failureRequestId,
  resolutionRequestId,
  FailureEvidence,
  FailureResolution,
} from "./failures.js";

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
  const unsettled = yield* Ref.make<ReadonlySet<string>>(new Set());
  const retainFailure = Effect.fn("DurableHarness.retainFailure")(function* (
    failure: ToolInvocationFailure,
  ) {
    for (const requestId of yield* Ref.get(unsettled)) {
      const current = yield* native((context) =>
        harness.commit(
          (tx) => tx.submissionByRequest(root.id, nativeRequestId(requestId)),
          context,
        ),
      );
      // A previously accepted input may have settled without this scope observing its wait.
      if (!current || current.status === "done" || current.status === "unanswered") continue;
      yield* conversations
        .append(options.owner, failureRequestId(requestId), "agent.invocation.failure", {
          requestId,
          message: failure.message,
          outcome: failure.outcome,
          ...(failure.operationId === undefined ? {} : { operationId: failure.operationId }),
        })
        .pipe(
          Effect.mapError(
            (cause) =>
              new AgentError("Cannot retain terminal tool failure", [], {
                cause,
                outcome: "unknown",
                operationId: failure.operationId,
              }),
          ),
          Effect.tapError(() => driver.quarantine),
        );
    }
  });
  yield* options.registerTerminalHandler?.(retainFailure) ?? Effect.void;
  const readFailure = Effect.fnUntraced(function* (requestId: string) {
    const saved = yield* conversations
      .find(options.owner, failureRequestId(requestId))
      .pipe(
        Effect.mapError(
          (cause) =>
            new AgentError("Cannot read terminal tool failure", [], { cause, outcome: "unknown" }),
        ),
      );
    if (Option.isNone(saved)) return Option.none<typeof FailureEvidence.Type>();
    if (saved.value.kind !== "agent.invocation.failure")
      return yield* Effect.fail(
        new AgentError("Terminal tool failure entry kind mismatch", [], { outcome: "unknown" }),
      );
    const failure = yield* Schema.decodeUnknownEffect(FailureEvidence)(saved.value.data).pipe(
      Effect.mapError(
        (cause) =>
          new AgentError("Invalid terminal tool failure entry", [], { cause, outcome: "unknown" }),
      ),
    );
    if (failure.requestId !== requestId)
      return yield* Effect.fail(
        new AgentError("Terminal tool failure identity mismatch", [], { outcome: "unknown" }),
      );
    return Option.some(failure);
  });
  const failureEvidence = Effect.fnUntraced(function* (requestId: string) {
    const marker = yield* readFailure(requestId);
    if (Option.isNone(marker)) return;
    const failure = marker.value;
    const resolved = yield* conversations.find(options.owner, resolutionRequestId(requestId)).pipe(
      Effect.mapError(
        (cause) =>
          new AgentError("Cannot read terminal failure resolution", [], {
            cause,
            outcome: "unknown",
            operationId: failure.operationId,
          }),
      ),
    );
    if (Option.isNone(resolved))
      return yield* Effect.fail(
        new AgentError(failure.message, [], {
          outcome: failure.outcome,
          operationId: failure.operationId,
        }),
      );
    if (resolved.value.kind !== "agent.invocation.failure.resolved")
      return yield* Effect.fail(
        new AgentError("Terminal failure resolution entry kind mismatch", [], {
          outcome: "unknown",
          operationId: failure.operationId,
        }),
      );
    const resolution = yield* Schema.decodeUnknownEffect(FailureResolution)(
      resolved.value.data,
    ).pipe(
      Effect.mapError(
        (cause) =>
          new AgentError("Invalid terminal failure resolution entry", [], {
            cause,
            outcome: "unknown",
            operationId: failure.operationId,
          }),
      ),
    );
    if (resolution.requestId !== requestId || resolution.operationId !== failure.operationId)
      return yield* Effect.fail(
        new AgentError("Terminal failure resolution identity mismatch", [], {
          outcome: "unknown",
          operationId: failure.operationId,
        }),
      );
    return yield* Effect.fail(
      new AgentError(
        "Original tool outcome was reconciled; its reasoning invocation remains aborted",
        [],
        {
          outcome: "failed",
          operationId: failure.operationId,
        },
      ),
    );
  });
  const resolveFailure = Effect.fn("DurableHarness.resolveFailure")(
    function* (requestId: string, operationId: string) {
      if (yield* Ref.get(closed))
        return yield* Effect.fail(new AgentError("Conversation scope is closed"));
      yield* conversations.resolveFailure(options.owner, requestId, operationId).pipe(
        Effect.mapError(
          (cause) =>
            new AgentError(cause.message, [], {
              cause,
              outcome: cause.kind === "unavailable" ? "unknown" : "failed",
              operationId,
            }),
        ),
      );
    },
    admission.withPermit,
    Effect.uninterruptible,
  );
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
  const workspaceExtension = defineExtension({
    name: "aster-workspace-tools",
    tools: [createReadTool(), createWriteTool(), createEditTool()],
  });
  const workspaceEnabled =
    options.workspaceId !== undefined && driver.workspaceEnvironment !== undefined;
  yield* Effect.sync(() => {
    driver.registry.install(extension);
    if (workspaceEnabled) driver.registry.install(workspaceExtension);
    durableModels(
      { ...resolved, model: { ...resolved.model, contextWindow: contextTokens } },
      driver.models,
      options.invocationSignal,
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
      !saved.extensions.includes(extension.name) ||
      (workspaceEnabled && !saved.extensions.includes(workspaceExtension.name))
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
          extensions: workspaceEnabled ? [extension, workspaceExtension] : [extension],
          cwd: options.workspaceId,
        },
        context,
      ),
    );
  const wrap = (submission: Submission, requestId: string): HarnessSubmission => ({
    status: access((context) => submission.status(context)),
    wait: Effect.gen(function* () {
      yield* failureEvidence(requestId);
      yield* Ref.update(unsettled, (current) => new Set([...current, requestId]));
      const current = yield* access((context) => submission.status(context));
      const record =
        current.status === "done" || current.status === "unanswered"
          ? current
          : yield* Ref.set(scheduled, true).pipe(
              Effect.andThen(access((context) => submission.wait(context))),
            );
      yield* Ref.update(
        unsettled,
        (current) => new Set([...current].filter((id) => id !== requestId)),
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
      return Option.some(wrap(handle, requestId));
    });
  return {
    submission,
    submit: (input) =>
      Effect.gen(function* () {
        yield* Ref.update(unsettled, (current) => new Set([...current, input.requestId]));
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
          input.requestId,
        );
      }).pipe(admission.withPermit, Effect.uninterruptible),
    abort,
    resolveFailure,
  } satisfies HarnessConversation;
});
