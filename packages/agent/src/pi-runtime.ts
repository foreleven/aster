import { isDeepStrictEqual } from "node:util";
import { Data, Effect, Exit, Match, Option, Schema, Semaphore } from "effect";
import { BACKGROUND_CONTEXT, withAbortSignal } from "@earendil-works/chord/context";
import type { Context as ChordContext } from "@earendil-works/chord";
import {
  configure,
  createRegistry,
  defineDoc,
  defineExtension,
  defineTask,
  Harness,
  GenerationTask,
  CompactionTask,
  hook,
  type ConversationId,
  type Cursor,
  type Storage,
  type Session,
  type TaskRecord,
  type Tx,
  type ToolRegistration,
} from "@earendil-works/pi-durable";
import type { ExecutionEnv } from "@earendil-works/pi-durable/env";
import type { AgentTool } from "@earendil-works/pi-agent-core";
import type { ResolvedModel } from "./index.js";
import { durableModels, durableTool } from "./durable.js";
import { entriesFor, hasUnknownToolOutcome, fenceTools, generationFence } from "./durable-tools.js";

export class PiRuntimeError extends Data.TaggedError("PiRuntimeError")<{
  readonly operation:
    "open" | "submit" | "status" | "wait" | "resume" | "close" | "context" | "recover" | "lookup";
  readonly cause: unknown;
}> {}

const AdmissionRequest = Schema.Struct({
  requestId: Schema.NonEmptyString,
  prompt: Schema.NonEmptyString,
  instructions: Schema.String,
});
const Input = Schema.Struct({
  ...AdmissionRequest.fields,
  catalogueId: Schema.NonEmptyString,
  environmentPolicyId: Schema.NonEmptyString,
  model: Schema.Struct({ provider: Schema.NonEmptyString, modelId: Schema.NonEmptyString }),
});
/** Host-owned native capability boundary. Credentials and domain commands never
 * belong to this input; only the already admitted task evidence is supplied. */
export interface PiExecutionEnvironment {
  readonly policyId: string;
  readonly open: (
    input: typeof AdmissionRequest.Type & { readonly namespace: string },
    context: ChordContext,
  ) => Promise<ExecutionEnv>;
}
export const PiExecutionHandle = Schema.Struct({
  sessionId: Schema.NonEmptyString,
  runId: Schema.NonEmptyString,
});
export type PiExecutionHandle = typeof PiExecutionHandle.Type;
export const PiExecutionResult = Schema.Union([
  Schema.Struct({ state: Schema.Literal("completed"), text: Schema.String }),
  Schema.Struct({
    state: Schema.Literals(["failed", "unknown", "cancelled"]),
    error: Schema.String,
  }),
]);
export type PiExecutionResult = typeof PiExecutionResult.Type;
export type PiExecutionStatus = PiExecutionResult | { readonly state: "running" };
export const PiExecutionOwner = defineDoc<{ ownerId: string }>({
  kind: "app.aster.execution.owner",
  version: 1,
  scope: "session",
  initial: () => ({ ownerId: "" }),
});

/** SDK callback boundary. Task and transcript writes remain on Pi's transaction
 * line; domain Actors receive only handles and decoded business outcomes. */
const openDriver = async (
  storage: Storage,
  options: {
    readonly ownerId: string;
    readonly resolved: ResolvedModel;
    readonly catalogueId: string;
    readonly tools: readonly AgentTool[];
    readonly nativeTools?: readonly ToolRegistration[];
    readonly environment?: PiExecutionEnvironment;
    readonly validateSession?: (session: Session, context: ChordContext) => Promise<void>;
  },
  context: ChordContext,
  ownHarness: (harness: Harness) => void,
) => {
  const registered = [...options.tools.map(durableTool), ...(options.nativeTools ?? [])];
  const unsafeTools = new Set(
    registered.filter((tool) => tool.replay !== "safe").map((tool) => tool.name),
  );
  const tools = fenceTools(registered, unsafeTools);
  const environmentPolicyId = options.environment?.policyId ?? "aster.no-environment.v1";
  const fence = generationFence(() => harness, unsafeTools);
  const execution = defineTask<typeof Input.Type, { phase: "execute" }, PiExecutionResult>({
    name: "app.aster.execution",
    version: 1,
    initial: () => ({ phase: "execute" }),
    phases: {
      execute: async (task, runtime, context) => {
        const input = Schema.decodeUnknownSync(Input)(task.input);
        let child: ConversationId | undefined;
        await runtime.commit(async (tx) => {
          const found = (await tx.scanConversations({ ownerTaskId: task.id }, 2)).items;
          if (found.length !== 1) throw new Error("Execution must own exactly one conversation");
          child = found[0].id;
          return undefined;
        }, context);
        if (child === undefined) throw new Error("Execution conversation missing");
        const conversation = await runtime.conversation(child, context);
        if (!conversation) throw new Error("Execution conversation missing");
        const submission = await conversation.submit(
          {
            type: "input",
            content: input.prompt,
            requestId: input.requestId,
            whenBusy: "reject",
          },
          context,
        );
        const settled = await submission.wait(context);
        await conversation.waitForIdle(context);
        await runtime.commit(async (tx) => {
          const entries = await entriesFor(tx, conversation.id);
          const messages = entries.flatMap((entry) => entry.model ?? []);
          const answer = messages
            .filter((message) => message.role === "assistant")
            .find((message) => message.stopReason === "stop");
          const result: PiExecutionResult = Match.value({
            unknown: hasUnknownToolOutcome(entries, unsafeTools),
            settled,
          }).pipe(
            Match.when({ unknown: true }, () => ({
              state: "unknown" as const,
              error: "An unsafe tool has an uncertain outcome; reconciliation is required.",
            })),
            Match.when({ settled: { status: "unanswered" } }, ({ settled }) => ({
              state: "failed" as const,
              error: `Pi execution ended without an answer: ${settled.reason}`,
            })),
            Match.orElse(() =>
              answer
                ? {
                    state: "completed" as const,
                    text: answer.content
                      .flatMap((part) => (part.type === "text" ? [part.text] : []))
                      .join("\n"),
                  }
                : { state: "failed" as const, error: "Pi execution returned no final answer" },
            ),
          );
          await tx.appendEntry(task.conversationId, {
            kind: "app.aster.execution.result",
            data: {
              mappingVersion: 1,
              taskId: task.id,
              conversationId: conversation.id,
              requestId: input.requestId,
              result,
            },
          });
          return { status: "terminal", outcome: { status: "completed", result } };
        }, context);
      },
    },
    abort: async (_task, runtime, context) => {
      await runtime.commit(
        () => ({
          status: "terminal",
          outcome: { status: "aborted", reason: "Execution cancelled" },
        }),
        context,
      );
    },
  });
  const extension = defineExtension({
    name: `aster-execution:${options.catalogueId}`,
    tools,
    tasks: [execution],
    hooks: [
      hook(GenerationTask, {
        beforeRequest: fence.beforeRequest,
      }),
      hook(CompactionTask, { beforeCompact: fence.beforeCompact }),
    ],
  });
  const registry = createRegistry();
  registry.install(extension);
  const harness: Harness = await Harness.open(
    storage,
    {
      models: durableModels(options.resolved, fence.beforeModel),
      registry,
      env: options.environment
        ? async (target, context) => {
            const input = await harness.commit(async (tx) => {
              const conversation = await tx.conversation(target.conversationId);
              const task = conversation?.owner && (await tx.task(conversation.owner.taskId));
              if (!task || task.kind !== execution.definition.name)
                throw new Error("Execution environment requires an admitted Task owner");
              const input = Schema.decodeUnknownSync(Input)(task.input);
              if (input.environmentPolicyId !== environmentPolicyId)
                throw new Error("Execution environment policy changed after admission");
              return input;
            }, context);
            return options.environment!.open(
              {
                requestId: input.requestId,
                prompt: input.prompt,
                instructions: input.instructions,
                namespace: JSON.stringify([options.ownerId, target.conversationId]),
              },
              context,
            );
          }
        : undefined,
    },
    context,
  );
  ownHarness(harness);
  await options.validateSession?.(harness, context);
  const ownerId = Schema.decodeUnknownSync(Schema.NonEmptyString)(options.ownerId);
  Schema.decodeUnknownSync(Schema.NonEmptyString)(options.catalogueId);
  Schema.decodeUnknownSync(Schema.NonEmptyString)(environmentPolicyId);
  await harness.commit(async (tx) => {
    const existingTasks = await tx.scanTasks({}, 1);
    const identity = await tx.doc(PiExecutionOwner);
    const stored = Schema.decodeUnknownSync(Schema.Struct({ ownerId: Schema.String }))(identity);
    if (
      (stored.ownerId === "" && existingTasks.items.length > 0) ||
      (stored.ownerId !== "" && stored.ownerId !== ownerId)
    )
      throw new Error("Pi execution storage belongs to another owner");
    identity.ownerId = ownerId;
  }, context);
  await harness.commit(async (tx) => {
    let cursor: Cursor | undefined;
    do {
      const page = await tx.scanTasks({ kind: execution.definition.name }, 100, cursor);
      for (const task of page.items) {
        if (task.state.status === "terminal") continue;
        const input = Schema.decodeUnknownSync(Input)(task.input);
        if (
          input.catalogueId !== options.catalogueId ||
          input.environmentPolicyId !== environmentPolicyId ||
          input.model.provider !== options.resolved.model.provider ||
          input.model.modelId !== options.resolved.model.id
        )
          throw new Error(
            "Pending Pi execution requires its original model, tool catalogue and environment policy",
          );
      }
      cursor = page.next;
    } while (cursor);
  }, context);
  const find = async (tx: Tx, handle: PiExecutionHandle) => {
    let cursor: Cursor | undefined;
    do {
      const page = await tx.scanTasks({ kind: execution.definition.name }, 100, cursor);
      const task = page.items.find((item) => String(item.id) === handle.runId);
      if (task) {
        const child = (await tx.scanConversations({ ownerTaskId: task.id }, 2)).items;
        if (child.length !== 1 || String(child[0].id) !== handle.sessionId)
          throw new Error("Pi execution handle does not match its owned conversation");
        return task;
      }
      cursor = page.next;
    } while (cursor);
    throw new Error("Pi execution was not found; no replacement was created");
  };
  const project = (task: TaskRecord<unknown, unknown, unknown>): PiExecutionStatus => {
    if (task.state.status !== "terminal") return { state: "running" };
    return Match.value(task.state.outcome).pipe(
      Match.when({ status: "completed" }, ({ result }) =>
        Schema.decodeUnknownSync(PiExecutionResult)(result),
      ),
      Match.when({ status: "failed" }, ({ error }) => ({
        state: "failed" as const,
        error: error.message,
      })),
      Match.when({ status: "aborted" }, () => ({
        state: "cancelled" as const,
        error: "Execution cancelled",
      })),
      Match.when({ status: "faulted" }, ({ error }) => ({
        state: "unknown" as const,
        error: error.message,
      })),
      Match.when({ status: "orphaned" }, ({ reason }) => ({
        state: "unknown" as const,
        error: reason,
      })),
      Match.exhaustive,
    );
  };
  const executionInput = (request: { requestId: string; prompt: string; instructions: string }) =>
    Schema.decodeUnknownSync(Input)({
      ...request,
      catalogueId: options.catalogueId,
      environmentPolicyId,
      model: { provider: options.resolved.model.provider, modelId: options.resolved.model.id },
    });
  const findAdmission = async (
    tx: Tx,
    input: typeof Input.Type | typeof AdmissionRequest.Type,
  ): Promise<PiExecutionHandle | undefined> => {
    let cursor: Cursor | undefined;
    do {
      const page = await tx.scanTasks({ kind: execution.definition.name }, 100, cursor);
      const existing = page.items.find(
        (task) => Schema.decodeUnknownSync(Input)(task.input).requestId === input.requestId,
      );
      if (existing) {
        const saved =
          "model" in input
            ? existing.input
            : Schema.decodeUnknownSync(AdmissionRequest)(existing.input);
        if (!isDeepStrictEqual(saved, input))
          throw new Error("Pi execution request ID was reused with different input");
        const child = (await tx.scanConversations({ ownerTaskId: existing.id }, 2)).items;
        if (child.length !== 1) throw new Error("Pi execution mapping is invalid");
        return { sessionId: String(child[0].id), runId: String(existing.id) };
      }
      cursor = page.next;
    } while (cursor);
    return undefined;
  };
  return {
    session: harness,
    // Admission inspection neither creates work nor resumes the Harness.
    lookup: async (
      request: { requestId: string; prompt: string; instructions: string },
      context: ChordContext,
    ) =>
      Option.fromUndefinedOr(
        await harness.commit(
          (tx) => findAdmission(tx, Schema.decodeUnknownSync(AdmissionRequest)(request)),
          context,
        ),
      ),
    submit: async (
      request: { requestId: string; prompt: string; instructions: string },
      context: ChordContext,
    ) => {
      const input = executionInput(request);
      const root = await harness.root(context);
      const handle = await root.commit(async (tx) => {
        const existing = await findAdmission(tx, input);
        if (existing) return existing;
        const taskId = await tx.createTask(execution, input, {
          ownership: { kind: "conversation" },
        });
        const child = await tx.createConversation({ ownership: { kind: "task", taskId } });
        await configure(tx, child.id, {
          model: input.model,
          instructions: input.instructions,
          extensions: [extension],
        });
        await tx.appendEntry(root.id, {
          kind: "app.aster.execution.accepted",
          data: {
            mappingVersion: 1,
            requestId: input.requestId,
            taskId,
            conversationId: child.id,
            input,
          },
        });
        return { sessionId: String(child.id), runId: String(taskId) };
      }, context);
      harness.resume();
      return handle;
    },
    status: async (handle: PiExecutionHandle, context: ChordContext) =>
      project(await harness.commit((tx) => find(tx, handle), context)),
    resume: async (handle: PiExecutionHandle, context: ChordContext) => {
      await harness.commit((tx) => find(tx, handle), context);
      harness.resume();
      return handle;
    },
    wait: async (handle: PiExecutionHandle, context: ChordContext) => {
      const task = await harness.commit((tx) => find(tx, handle), context);
      return project(await harness.waitForTask(task.id, context));
    },
  };
};

/** Scope owns one replaceable Harness. Short mutations and reopening are
 * serialized; long-running observers never hold the owner permit. Reopening
 * drains the previous Harness before any new storage handle is acquired. */
const make = Effect.fn("PiDurableAgentRuntime.make")(function* (options: {
  readonly ownerId: string;
  readonly openStorage: Effect.Effect<Storage, PiRuntimeError>;
  /** Retain host ownership if SDK close cannot prove its writer drained. */
  readonly onCloseFailure?: Effect.Effect<void>;
  readonly resolved: ResolvedModel;
  readonly catalogueId: string;
  readonly tools?: readonly AgentTool[];
  readonly nativeTools?: readonly ToolRegistration[];
  readonly environment?: PiExecutionEnvironment;
  readonly validateSession?: (session: Session, context: ChordContext) => Promise<void>;
}) {
  type Driver = Awaited<ReturnType<typeof openDriver>>;
  const close = (resource: { storage: Storage; harness?: Harness }) =>
    Effect.tryPromise({
      try: () =>
        resource.harness
          ? resource.harness.close(BACKGROUND_CONTEXT)
          : resource.storage.close(BACKGROUND_CONTEXT),
      catch: (cause) => new PiRuntimeError({ operation: "close", cause }),
    }).pipe(Effect.tapError(() => options.onCloseFailure ?? Effect.void));
  const acquire = Effect.gen(function* () {
    const storage = yield* options.openStorage;
    const resource: { storage: Storage; harness?: Harness } = { storage };
    const driver = yield* Effect.tryPromise({
      try: (signal) =>
        openDriver(
          storage,
          { ...options, tools: options.tools ?? [] },
          withAbortSignal(signal, BACKGROUND_CONTEXT),
          (harness) => {
            resource.harness = harness;
          },
        ),
      catch: (cause) => new PiRuntimeError({ operation: "open", cause }),
    }).pipe(
      Effect.onExit((exit) =>
        Exit.isFailure(exit) ? close(resource).pipe(Effect.orDie) : Effect.void,
      ),
    );
    return { ...resource, driver };
  }).pipe(Effect.uninterruptible);
  type Resource = Effect.Success<typeof acquire>;
  const writer = yield* Semaphore.make(1);
  const owner = yield* Effect.acquireRelease(
    Effect.map(
      acquire,
      (current): { current: Resource | undefined; closed: boolean; recovering: boolean } => ({
        current,
        closed: false,
        recovering: false,
      }),
    ),
    (owner) =>
      writer.withPermit(
        Effect.gen(function* () {
          owner.closed = true;
          if (owner.current) yield* close(owner.current).pipe(Effect.orDie);
        }),
      ),
  );
  const current = Effect.suspend(() =>
    owner.current && !owner.closed && !owner.recovering
      ? Effect.succeed(owner.current.driver)
      : Effect.fail(
          new PiRuntimeError({
            operation: "open",
            cause: "Pi owner is closed or requires recovery",
          }),
        ),
  );
  const call = <A>(
    operation: PiRuntimeError["operation"],
    run: (driver: Driver, context: ChordContext) => Promise<A>,
  ) =>
    current.pipe(
      Effect.flatMap((driver) =>
        Effect.tryPromise({
          try: (signal) => run(driver, withAbortSignal(signal, BACKGROUND_CONTEXT)),
          catch: (cause) => new PiRuntimeError({ operation, cause }),
        }),
      ),
    );
  const mutate = <A>(
    operation: PiRuntimeError["operation"],
    run: (driver: Driver, context: ChordContext) => Promise<A>,
  ) => writer.withPermit(call(operation, run).pipe(Effect.uninterruptible));
  const recover = writer.withPermit(
    Effect.gen(function* () {
      if (owner.closed)
        return yield* new PiRuntimeError({ operation: "recover", cause: "Pi owner is closed" });
      owner.recovering = true;
      if (owner.current) {
        yield* close(owner.current);
        owner.current = undefined;
      }
      owner.current = yield* acquire;
      owner.recovering = false;
    }).pipe(Effect.uninterruptible),
  );
  return {
    ownerId: options.ownerId,
    /** Infrastructure-only bridge; core receives DurableContext/ExternalAgent ports. */
    withSession: <A>(run: (session: Session, context: ChordContext) => Promise<A>) =>
      mutate("context", (driver, context) => run(driver.session, context)),
    recover,
    submit: (request: { requestId: string; prompt: string; instructions: string }) =>
      mutate("submit", (driver, context) => driver.submit(request, context)),
    lookup: (request: { requestId: string; prompt: string; instructions: string }) =>
      mutate("lookup", (driver, context) => driver.lookup(request, context)),
    status: (handle: PiExecutionHandle) =>
      mutate("status", (driver, context) => driver.status(handle, context)),
    resume: (handle: PiExecutionHandle) =>
      mutate("resume", (driver, context) => driver.resume(handle, context)),
    // Session.close cancels observers of the retired generation; callers reconcile
    // using their durable handle. Never restart an observer by resubmitting work.
    wait: (handle: PiExecutionHandle) =>
      call("wait", (driver, context) => driver.wait(handle, context)),
  };
});

export type PiDurableRuntime = Effect.Success<ReturnType<typeof make>>;
export const PiDurableAgentRuntime = { make };
