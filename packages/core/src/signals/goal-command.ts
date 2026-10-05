import { isDeepStrictEqual } from "node:util";
import { ApplicationError, CausalChain, CommandReceipt } from "@aster/api-contracts";
import { Effect, Match, Schema } from "effect";
import { SignalDefinition, validateSignalTime } from "../config/schema.js";
import type { ContextRegistry } from "../context/registry.js";

const SignalPatch = Schema.Struct({
  trigger: SignalDefinition.fields.trigger,
  task: SignalDefinition.fields.task,
});
export const GoalSignalChange = Schema.Union([
  Schema.Struct({ operation: Schema.Literal("create"), definition: SignalPatch }),
  Schema.Struct({
    operation: Schema.Literal("update"),
    revision: Schema.Int,
    definition: SignalPatch,
  }),
  Schema.Struct({ operation: Schema.Literal("delete"), revision: Schema.Int }),
]);
/** Tool-call identity and arguments are replayed by Pi; the Signal mailbox owns the receipt. */
export const GoalSignalInput = Schema.Struct({
  requestId: Schema.NonEmptyString,
  source: Schema.String.check(Schema.isPattern(/^\/goals\/[a-z0-9][a-z0-9-]*$/)),
  target: Schema.String.check(Schema.isPattern(/^\/signals\/[a-z0-9][a-z0-9-]*$/)),
  change: GoalSignalChange,
  causal: CausalChain,
}).check(
  Schema.makeFilter(
    (input) =>
      input.target
        .slice("/signals/".length)
        .startsWith(`${input.source.slice("/goals/".length)}--`),
    { expected: "Signal belongs to the requesting Goal" },
  ),
);
export type GoalSignalInput = typeof GoalSignalInput.Type;
export const GoalSignalReceipt = Schema.Struct({ input: GoalSignalInput, receipt: CommandReceipt });
const Ownership = Schema.Struct({
  goal: Schema.optional(Schema.String),
  revision: Schema.optional(Schema.Number),
  deleted: Schema.optional(Schema.Boolean),
  goalCommandReceipts: Schema.optional(Schema.Array(GoalSignalReceipt)),
});

/** Shared Signal owner validates, commits and acknowledges each direct command once. */
export const applyGoalSignal = Effect.fn("Signal.applyGoalCommand")(function* (options: {
  registry: ContextRegistry["Service"];
  path: string;
  raw: GoalSignalInput;
  configured: readonly SignalDefinition[];
  nextDue: (definition: SignalDefinition) => number | undefined;
}): Effect.fn.Return<CommandReceipt, ApplicationError> {
  const invalid = (message: string) => new ApplicationError({ kind: "invalid-input", message });
  const conflict = (message: string) => new ApplicationError({ kind: "conflict", message });
  const input = yield* Schema.decodeUnknownEffect(GoalSignalInput)(options.raw).pipe(
    Effect.mapError(() => invalid("Invalid Goal Signal command")),
  );
  if (input.target !== options.path) return yield* invalid("Signal command targets another owner");
  const current = options.registry.get(options.path);
  const state = current && Schema.decodeUnknownSync(Ownership)(current.state);
  const prior = state?.goalCommandReceipts?.find(
    (record) => record.input.requestId === input.requestId,
  );
  if (prior) {
    if (!isDeepStrictEqual(prior.input, input))
      return yield* conflict("Signal request ID belongs to another command");
    return prior.receipt;
  }
  const goal = input.source.slice("/goals/".length);
  const slug = input.target.slice("/signals/".length);
  if (
    options.configured.some((definition) => definition.slug === slug) ||
    (current && state?.goal !== goal)
  )
    return yield* conflict("Signal belongs to another owner");
  const owner = options.registry.get(input.source)?.state as { status?: string } | undefined;
  if (owner?.status !== "active") return yield* conflict("Goal has ended or is missing");
  const change = input.change;
  if (change.operation === "create" ? !!current : !current || state?.deleted)
    return yield* conflict("Signal operation does not match current existence");
  if (change.operation !== "create" && change.revision !== state?.revision)
    return yield* conflict("Signal revision changed; read it again");
  const patch = Match.value(change).pipe(
    Match.when({ operation: "delete" }, () => ({})),
    Match.orElse((value) => value.definition),
  );
  const raw = { ...current?.state, ...patch, slug };
  const definition = yield* Schema.decodeUnknownEffect(SignalDefinition)(raw).pipe(
    Effect.mapError(() => invalid("Invalid Signal definition")),
  );
  const nextDue = yield* Effect.try({
    try: () => {
      if (definition.trigger._tag === "Schedule") validateSignalTime(definition.trigger);
      return change.operation === "delete" ? undefined : options.nextDue(definition);
    },
    catch: () => invalid("Invalid Signal timing"),
  });
  const receipt = { requestId: input.requestId, revision: (current?.revision ?? 0) + 1 };
  const next = {
    ...current?.state,
    ...definition,
    goal,
    causal: input.causal,
    active: change.operation !== "delete",
    occurrences:
      (current?.state as { occurrences?: readonly unknown[] } | undefined)?.occurrences ?? [],
    deleted: change.operation === "delete",
    revision: (state?.revision ?? 0) + 1,
    nextDue,
    timerDone: false,
    goalCommandReceipts: [...(state?.goalCommandReceipts ?? []), { input, receipt }],
  };
  yield* options.registry
    .commit(
      {
        path: options.path,
        description: current?.description ?? `Goal Signal: ${slug}`,
        state: next,
        messages: current?.messages ?? [],
      },
      { expectedRevision: current?.revision ?? 0 },
    )
    .pipe(
      Effect.catchTag("ContextConflict", () =>
        Effect.fail(conflict("Signal changed during command admission")),
      ),
      Effect.catchTags({ ContextValidationError: Effect.die, ContextCommitError: Effect.die }),
    );
  return receipt;
});
