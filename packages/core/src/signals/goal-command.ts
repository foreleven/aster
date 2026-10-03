import { isDeepStrictEqual } from "node:util";
import {
  ApplicationError,
  CausalChain,
  CommandReceipt,
  RetryGoalSignalInput,
} from "@aster/api-contracts";
import { Effect, Schema } from "effect";
import { SignalDefinition, validateSignalTime } from "../config/schema.js";
import type { ContextRegistry } from "../context/registry.js";

/** Frozen full definition published by a Goal result, never a model-owned command envelope. */
export const GoalSignalInput = Schema.Struct({
  requestId: CommandReceipt.fields.requestId,
  evaluationId: Schema.NonEmptyString,
  source: Schema.String.check(Schema.isPattern(/^\/goals\/[a-z0-9][a-z0-9-]*$/)),
  target: Schema.String.check(Schema.isPattern(/^\/signals\/[a-z0-9][a-z0-9-]*$/)),
  expectedRevision: CommandReceipt.fields.revision,
  operation: Schema.Literals(["create", "update", "delete"]),
  definition: SignalDefinition,
  causal: CausalChain,
  createdAt: Schema.NonEmptyString,
}).check(
  Schema.makeFilter(
    (input) => {
      const goal = input.source.slice("/goals/".length);
      return (
        input.target === `/signals/${input.definition.slug}` &&
        input.definition.slug.startsWith(`${goal}--`) &&
        input.definition.mode === "confirm" &&
        input.definition.action === undefined &&
        Number.isFinite(Date.parse(input.createdAt))
      );
    },
    { expected: "A Goal-owned, confirmation-required Signal operation with a valid timestamp" },
  ),
);
export type GoalSignalInput = typeof GoalSignalInput.Type;

export const GoalSignalReceipt = Schema.Struct({
  input: GoalSignalInput,
  receipt: CommandReceipt,
}).check(
  Schema.makeFilter(
    ({ input, receipt }) =>
      input.requestId === receipt.requestId && receipt.revision === input.expectedRevision + 1,
    { expected: "Receipt for the published Signal operation" },
  ),
);
const RetryReceipt = Schema.Struct({ input: RetryGoalSignalInput, receipt: CommandReceipt });
export const signalAttemptLimit = (operation: {
  readonly retries?: readonly (typeof RetryReceipt.Type)[];
}) => Math.max(3, ...(operation.retries ?? []).map((retry) => retry.input.expectedAttempts + 1));

export const GoalSignalOperation = Schema.Struct({
  input: GoalSignalInput,
  status: Schema.Literals(["pending", "sending", "unknown", "delivered", "rejected"]),
  attempts: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  retries: Schema.optional(Schema.Array(RetryReceipt)),
  receipt: Schema.optional(CommandReceipt),
  error: Schema.optional(Schema.String),
}).check(
  Schema.makeFilter(
    (operation) => {
      if (operation.attempts > signalAttemptLimit(operation)) return false;
      const retries = operation.retries ?? [];
      if (new Set(retries.map((retry) => retry.input.requestId)).size !== retries.length)
        return false;
      if (
        retries.some(
          (retry) =>
            retry.input.slug !== operation.input.source.slice("/goals/".length) ||
            retry.input.operationId !== operation.input.requestId ||
            retry.receipt.requestId !== retry.input.requestId ||
            retry.input.expectedAttempts > operation.attempts,
        )
      )
        return false;
      if (operation.status === "delivered")
        return (
          operation.receipt?.requestId === operation.input.requestId &&
          operation.receipt.revision === operation.input.expectedRevision + 1 &&
          operation.attempts > 0
        );
      return (
        operation.receipt === undefined &&
        (operation.status === "pending"
          ? operation.attempts < signalAttemptLimit(operation)
          : operation.attempts > 0)
      );
    },
    { expected: "Bounded Signal delivery with a matching committed receipt" },
  ),
);
export type GoalSignalOperation = typeof GoalSignalOperation.Type;

const Ownership = Schema.Struct({
  goal: Schema.optional(Schema.String),
  owner: Schema.optional(Schema.String),
  revision: Schema.optional(Schema.Number),
  goalCommandReceipts: Schema.optional(Schema.Array(GoalSignalReceipt)),
});
const Publisher = Schema.Struct({
  status: Schema.Literals(["active", "completed"]),
  tasks: Schema.Array(Schema.Struct({ id: Schema.String, status: Schema.String })),
  signalOutbox: Schema.Array(GoalSignalOperation),
});

/** Receiver mailbox only: definition, schedule revision and receipt commit together. */
export const applyGoalSignal = Effect.fn("Signal.applyGoalCommand")(function* (options: {
  readonly registry: ContextRegistry["Service"];
  readonly path: string;
  readonly raw: GoalSignalInput;
  readonly configured: readonly SignalDefinition[];
  readonly agents: readonly string[];
  readonly nextDue: (definition: SignalDefinition) => number | undefined;
}): Effect.fn.Return<CommandReceipt, ApplicationError> {
  const input = yield* Schema.decodeUnknownEffect(GoalSignalInput)(options.raw).pipe(
    Effect.mapError(
      () => new ApplicationError({ kind: "invalid-input", message: "Invalid Goal Signal command" }),
    ),
  );
  if (input.target !== options.path)
    return yield* new ApplicationError({
      kind: "invalid-input",
      message: "Signal command addressed to another owner",
    });
  const current = options.registry.get(options.path);
  const state = current && Schema.decodeUnknownSync(Ownership)(current.state);
  const prior = state?.goalCommandReceipts?.find(
    (record) => record.input.requestId === input.requestId,
  );
  if (prior) {
    if (!isDeepStrictEqual(prior.input, input))
      return yield* new ApplicationError({
        kind: "conflict",
        message: "Signal request ID belongs to another command",
      });
    return prior.receipt;
  }
  const goal = input.source.slice("/goals/".length);
  if (
    options.configured.some((definition) => definition.slug === input.definition.slug) ||
    (current && (state?.goal !== goal || state.owner !== undefined))
  )
    return yield* new ApplicationError({
      kind: "conflict",
      message: "Signal belongs to another owner",
    });
  if ((input.operation === "create") === !!current)
    return yield* new ApplicationError({
      kind: "conflict",
      message: "Signal operation does not match current existence",
    });
  const source = options.registry.get(input.source);
  const publisher = Schema.decodeUnknownResult(Publisher)(source?.state);
  if (
    publisher._tag === "Failure" ||
    !publisher.success.signalOutbox.some((operation) => isDeepStrictEqual(operation.input, input))
  )
    return yield* new ApplicationError({
      kind: "invalid-input",
      message: "Signal command was not published by its Goal",
    });
  if (publisher.success.status !== "active" && input.operation !== "delete")
    return yield* new ApplicationError({ kind: "conflict", message: "Goal has ended" });
  if (
    input.operation !== "delete" &&
    input.definition.taskId &&
    !publisher.success.tasks.some(
      (task) => task.id === input.definition.taskId && task.status !== "deleted",
    )
  )
    return yield* new ApplicationError({
      kind: "conflict",
      message: "Signal references a missing or deleted Goal Task",
    });
  if (input.operation !== "delete" && !options.agents.includes(input.definition.agent))
    return yield* new ApplicationError({
      kind: "invalid-input",
      message: "Signal executor is not configured",
    });
  if (!input.definition.when.trim() || !input.definition.task.trim())
    return yield* new ApplicationError({
      kind: "invalid-input",
      message: "Signal condition and task must not be empty",
    });
  const nextDue = yield* Effect.try({
    try: () => {
      validateSignalTime(input.definition);
      return input.operation === "delete" ? undefined : options.nextDue(input.definition);
    },
    catch: () => new ApplicationError({ kind: "invalid-input", message: "Invalid Signal timing" }),
  });
  const receipt = { requestId: input.requestId, revision: (current?.revision ?? 0) + 1 };
  const next: Record<string, unknown> = {
    ...current?.state,
    ...input.definition,
    schedule: input.definition.schedule,
    notBefore: input.definition.notBefore,
    taskId: input.definition.taskId,
    goal,
    causal: input.causal,
    active: input.operation !== "delete",
    deleted: input.operation === "delete",
    revision: (state?.revision ?? 0) + 1,
    nextDue,
    timerDone: false,
    goalCommandReceipts: [...(state?.goalCommandReceipts ?? []), { input, receipt }],
  };
  for (const key of ["schedule", "notBefore", "taskId", "nextDue"])
    if (next[key] === undefined) delete next[key];
  yield* options.registry
    .commit(
      {
        path: options.path,
        description: current?.description ?? `Goal Signal: ${input.definition.slug}`,
        state: next,
        messages: current?.messages ?? [],
      },
      { expectedRevision: input.expectedRevision },
    )
    .pipe(
      Effect.catchTag("ContextConflict", () =>
        Effect.fail(
          new ApplicationError({
            kind: "conflict",
            message: "Signal changed after Goal result admission",
          }),
        ),
      ),
      Effect.catchTag("ContextValidationError", Effect.die),
      Effect.catchTag("ContextCommitError", Effect.die),
    );
  return receipt;
});
